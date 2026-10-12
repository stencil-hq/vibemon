//! Build log: in-memory entries for `GET /builds/{id}?logsOffset` mirrored as
//! JSON lines into `builds/<id>/build.log` so status survives node restarts.

use std::{
	io::Write,
	path::{Path, PathBuf},
	sync::Arc,
};

use parking_lot::Mutex;

use crate::{api::BuildLogEntry, artifacts::now_ms};

/// Longest single log message kept (longer lines are truncated).
const MAX_MESSAGE: usize = 16 * 1024;

/// Append-only build log shared by the pipeline and the status endpoint.
#[derive(Clone, Debug)]
pub struct BuildLog {
	inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
	entries: Vec<BuildLogEntry>,
	file:    Option<std::fs::File>,
	path:    PathBuf,
}

impl BuildLog {
	/// Creates (truncating) the log file at `path`.
	pub fn create(path: &Path) -> Self {
		let file = std::fs::File::create(path)
			.inspect_err(|error| tracing::warn!(%error, path = %path.display(), "build log file"))
			.ok();
		Self {
			inner: Arc::new(Mutex::new(Inner { entries: Vec::new(), file, path: path.to_owned() })),
		}
	}

	/// Loads a finished build's log from disk (unparsable lines are skipped).
	pub fn load(path: &Path) -> Option<Self> {
		let text = std::fs::read_to_string(path).ok()?;
		let entries = text
			.lines()
			.filter_map(|line| serde_json::from_str(line).ok())
			.collect();
		Some(Self {
			inner: Arc::new(Mutex::new(Inner { entries, file: None, path: path.to_owned() })),
		})
	}

	/// Appends an entry with the current timestamp.
	pub fn push(&self, level: &str, step: Option<&str>, message: impl Into<String>) {
		let mut message: String = message.into();
		if message.len() > MAX_MESSAGE {
			let mut cut = MAX_MESSAGE;
			while !message.is_char_boundary(cut) {
				cut -= 1;
			}
			message.truncate(cut);
		}
		let entry = BuildLogEntry {
			timestamp_ms: now_ms(),
			level: level.to_owned(),
			step: step.map(str::to_owned),
			message,
		};
		let mut inner = self.inner.lock();
		if let Some(file) = inner.file.as_mut()
			&& let Ok(mut line) = serde_json::to_vec(&entry)
		{
			line.push(b'\n');
			if let Err(error) = file.write_all(&line) {
				tracing::warn!(%error, "build log write failed");
				inner.file = None;
			}
		}
		inner.entries.push(entry);
	}

	/// `info`-level entry.
	pub fn info(&self, step: Option<&str>, message: impl Into<String>) {
		self.push("info", step, message);
	}

	/// `error`-level entry.
	pub fn error(&self, step: Option<&str>, message: impl Into<String>) {
		self.push("error", step, message);
	}

	/// Entries from `offset` on, plus the total count.
	pub fn since(&self, offset: u64) -> (Vec<BuildLogEntry>, u64) {
		let inner = self.inner.lock();
		let start = usize::try_from(offset)
			.unwrap_or(usize::MAX)
			.min(inner.entries.len());
		(inner.entries[start..].to_vec(), inner.entries.len() as u64)
	}

	/// Path of the backing file.
	pub fn path(&self) -> PathBuf {
		self.inner.lock().path.clone()
	}
}

/// Splits streamed process output into log lines, carrying partial lines
/// across chunks (and `\r` progress updates).
#[derive(Debug, Default)]
pub struct LineSplitter {
	pending: Vec<u8>,
}

impl LineSplitter {
	/// Feeds a chunk; returns the completed lines.
	pub fn feed(&mut self, chunk: &[u8]) -> Vec<String> {
		let mut lines = Vec::new();
		for &byte in chunk {
			if byte == b'\n' || byte == b'\r' {
				if !self.pending.is_empty() {
					lines.push(String::from_utf8_lossy(&self.pending).into_owned());
					self.pending.clear();
				}
			} else {
				self.pending.push(byte);
				if self.pending.len() >= MAX_MESSAGE {
					lines.push(String::from_utf8_lossy(&self.pending).into_owned());
					self.pending.clear();
				}
			}
		}
		lines
	}

	/// Returns the trailing partial line, if any.
	pub fn finish(&mut self) -> Option<String> {
		(!self.pending.is_empty())
			.then(|| String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn offsets_page_and_reload_from_disk() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("build.log");
		let log = BuildLog::create(&path);
		log.info(Some("base"), "one");
		log.error(None, "two");
		let (entries, total) = log.since(1);
		assert_eq!((entries.len(), total), (1, 2));
		assert_eq!(entries[0].message, "two");
		assert_eq!(log.since(5).0.len(), 0, "offset past the end is empty, not an error");
		let reloaded = BuildLog::load(&path).unwrap();
		let (entries, total) = reloaded.since(0);
		assert_eq!(total, 2);
		assert_eq!(entries[0].step.as_deref(), Some("base"));
		assert_eq!(entries[1].level, "error");
	}

	#[test]
	fn splitter_joins_partial_lines_and_drops_blank_ones() {
		let mut splitter = LineSplitter::default();
		assert_eq!(splitter.feed(b"hel"), Vec::<String>::new());
		assert_eq!(splitter.feed(b"lo\nwor"), ["hello"]);
		assert_eq!(splitter.feed(b"ld\r\n\n50%\r"), ["world", "50%"]);
		assert_eq!(splitter.feed(b"tail"), Vec::<String>::new());
		assert_eq!(splitter.finish().as_deref(), Some("tail"));
		assert_eq!(splitter.finish(), None);
	}
}
