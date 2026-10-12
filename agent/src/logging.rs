//! Structured guest logs: one `vmon-log:{json}` line per event, which the VMM
//! captures into the sandbox `console.log` and the node parses. Used by PID-1
//! init, the GC4 loop, and envd.
//!
//! Lines go to the `vmon.log` virtio-console port once the VMM has announced
//! it (one VM exit per line), else to `/dev/console`, a UART that costs one
//! port-IO exit per byte (~400 per envd command).

use std::{
	fs::{self, File, OpenOptions},
	io::Write,
	path::Path,
	sync::{
		LazyLock, Mutex,
		atomic::{AtomicU64, Ordering},
		mpsc::{self, SyncSender, TrySendError},
	},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Value};

/// Line prefix the node uses to tell structured logs from console noise.
pub const LOG_PREFIX: &str = "vmon-log:";

/// Severity of a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
	Debug,
	Info,
	Warn,
	Error,
}

impl Level {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Debug => "debug",
			Self::Info => "info",
			Self::Warn => "warn",
			Self::Error => "error",
		}
	}
}

/// sysfs view of virtio-console ports: `<dev>/name` per `/dev/<dev>`.
const VIRTIO_PORTS: &str = "/sys/class/virtio-ports";
/// Minimum gap between looks for the log port while it is missing (early boot,
/// a VMM without it, or another process holding it open).
const PORT_RETRY: Duration = Duration::from_secs(1);

/// Where [`write_line`] sends lines.
struct Sink {
	port:        Option<File>,
	last_lookup: Option<Instant>,
	console:     Option<File>,
}

impl Sink {
	/// The log port, looking it up again at most every [`PORT_RETRY`].
	fn port(&mut self) -> Option<&mut File> {
		if self.port.is_none() && self.last_lookup.is_none_or(|at| at.elapsed() >= PORT_RETRY) {
			self.last_lookup = Some(Instant::now());
			self.port = open_log_port();
		}
		self.port.as_mut()
	}
}

static SINK: LazyLock<Mutex<Sink>> = LazyLock::new(|| {
	Mutex::new(Sink {
		port:        None,
		last_lookup: None,
		console:     OpenOptions::new().write(true).open("/dev/console").ok(),
	})
});

/// Opens the virtio-console port named [`crate::proto::LOG_PORT_NAME`].
fn open_log_port() -> Option<File> {
	let path = fs::read_dir(VIRTIO_PORTS)
		.ok()?
		.flatten()
		.find_map(|entry| {
			let name = fs::read_to_string(entry.path().join("name")).ok()?;
			(name.trim_end() == crate::proto::LOG_PORT_NAME)
				.then(|| Path::new("/dev").join(entry.file_name()))
		})?;
	OpenOptions::new().write(true).open(path).ok()
}

/// Format one log line (without trailing newline). `fields` must be a JSON
/// object (other values are stored under `data`); `ts`, `level`, `msg` win
/// over same-named fields.
pub fn format_line(ts_ms: u128, level: Level, msg: &str, fields: Value) -> String {
	let mut map = match fields {
		Value::Object(map) => map,
		Value::Null => Map::new(),
		other => Map::from_iter([("data".to_owned(), other)]),
	};
	map.insert("ts".into(), Value::from(u64::try_from(ts_ms).unwrap_or(u64::MAX)));
	map.insert("level".into(), Value::from(level.as_str()));
	map.insert("msg".into(), Value::from(msg));
	format!("{LOG_PREFIX}{}", Value::Object(map))
}

/// Lines [`emit_queued`] buffers before it starts dropping.
const QUEUE_DEPTH: usize = 1024;

/// Background console writer behind [`emit_queued`].
struct Queue {
	tx:      SyncSender<String>,
	/// Lines dropped because the queue was full, not yet reported.
	dropped: AtomicU64,
}

static QUEUE: LazyLock<Queue> = LazyLock::new(|| {
	let (tx, rx) = mpsc::sync_channel::<String>(QUEUE_DEPTH);
	// A failed spawn drops `rx`; `enqueue` then reports `Disconnected` and
	// `emit_queued` writes synchronously.
	let _ = std::thread::Builder::new()
		.name("vmon-log".into())
		.spawn(move || {
			for line in rx {
				write_line(&line);
			}
		});
	Queue { tx, dropped: AtomicU64::new(0) }
});

fn timestamped_line(level: Level, msg: &str, fields: Value) -> String {
	let ts = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis();
	let mut line = format_line(ts, level, msg, fields);
	line.push('\n');
	line
}

/// Emit a structured log line to the log port or `/dev/console` (stderr if
/// neither is available), blocking until it was accepted.
pub fn emit(level: Level, msg: &str, fields: Value) {
	write_line(&timestamped_line(level, msg, fields));
}

/// Like [`emit`], but never blocks: a background writer drains the queue.
///
/// High-rate lines (envd process events) must stay off request paths, and the
/// `/dev/console` fallback drains at one VM exit per byte. When the queue is
/// full the line is dropped and the drop count is logged with the next line
/// that fits.
pub fn emit_queued(level: Level, msg: &str, fields: Value) {
	let queue = &*QUEUE;
	if let Err(line) = enqueue(&queue.tx, &queue.dropped, timestamped_line(level, msg, fields)) {
		write_line(&line);
	}
}

/// Queues `line`, first reporting earlier drops. Returns the line when the
/// writer is gone so the caller can write it synchronously.
fn enqueue(tx: &SyncSender<String>, dropped: &AtomicU64, line: String) -> Result<(), String> {
	let lost = dropped.swap(0, Ordering::Relaxed);
	if lost > 0 {
		let notice = timestamped_line(
			Level::Warn,
			"log queue full, lines dropped",
			serde_json::json!({"dropped": lost}),
		);
		if tx.try_send(notice).is_err() {
			dropped.fetch_add(lost, Ordering::Relaxed);
		}
	}
	match tx.try_send(line) {
		Ok(()) => Ok(()),
		Err(TrySendError::Full(_)) => {
			dropped.fetch_add(1, Ordering::Relaxed);
			Ok(())
		},
		Err(TrySendError::Disconnected(line)) => Err(line),
	}
}

fn write_line(line: &str) {
	let mut sink = SINK
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	if let Some(port) = sink.port() {
		if port.write_all(line.as_bytes()).is_ok() {
			return;
		}
		// Port gone (device reset): fall back and look again later.
		sink.port = None;
	}
	if let Some(console) = sink.console.as_mut()
		&& console.write_all(line.as_bytes()).is_ok()
	{
		return;
	}
	let _ = std::io::stderr().write_all(line.as_bytes());
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn full_queue_drops_and_reports_then_disconnect_returns_line() {
		let (tx, rx) = mpsc::sync_channel(2);
		let dropped = AtomicU64::new(0);
		for line in ["a", "b", "c"] {
			assert_eq!(enqueue(&tx, &dropped, line.into()), Ok(()));
		}
		assert_eq!(dropped.load(Ordering::Relaxed), 1);
		assert_eq!((rx.recv().unwrap(), rx.recv().unwrap()), ("a".into(), "b".into()));
		assert_eq!(enqueue(&tx, &dropped, "d".into()), Ok(()));
		let notice: Value = serde_json::from_str(
			rx.recv()
				.unwrap()
				.trim_end()
				.strip_prefix(LOG_PREFIX)
				.unwrap(),
		)
		.unwrap();
		assert_eq!((notice["level"].as_str(), notice["dropped"].as_u64()), (Some("warn"), Some(1)));
		assert_eq!(rx.recv().unwrap(), "d");
		drop(rx);
		assert_eq!(enqueue(&tx, &dropped, "e".into()), Err("e".into()));
	}

	#[test]
	fn reserved_keys_override_fields_and_scalars_are_wrapped() {
		let line = format_line(5, Level::Warn, "m", json!({"msg": "spoof", "port": 1}));
		let parsed: Value = serde_json::from_str(line.strip_prefix(LOG_PREFIX).unwrap()).unwrap();
		assert_eq!(parsed, json!({"ts": 5, "level": "warn", "msg": "m", "port": 1}));
		let line = format_line(1, Level::Info, "x", json!(3));
		assert!(line.contains("\"data\":3"));
		assert!(!line.contains('\n'));
	}
}
