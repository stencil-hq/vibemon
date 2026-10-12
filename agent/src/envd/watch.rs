//! Directory watching for `WatchDir` (stream) and
//! `CreateWatcher`/`GetWatcherEvents`/`RemoveWatcher` (polling): inotify with
//! fsnotify's op mapping (one event per op bit, order CREATE, RENAME, CHMOD,
//! WRITE, REMOVE), optional recursion, names relative to the watched dir,
//! and the network-mount guard.

use std::{
	collections::{HashMap, VecDeque},
	ffi::CString,
	io,
	os::{
		fd::{AsRawFd, FromRawFd, OwnedFd},
		unix::ffi::OsStrExt,
	},
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use parking_lot::Mutex;
use tokio::io::unix::AsyncFd;

use super::{
	connect::{RpcError, StreamSink},
	entry::entry_info,
	filesystem::io_rpc_error,
	pb::filesystem::{self as pb, EventType, FilesystemEvent, watch_dir_response as wd},
	users::NameCache,
};

const WATCH_MASK: u32 = libc::IN_MOVED_TO
	| libc::IN_MOVED_FROM
	| libc::IN_CREATE
	| libc::IN_ATTRIB
	| libc::IN_MODIFY
	| libc::IN_MOVE_SELF
	| libc::IN_DELETE
	| libc::IN_DELETE_SELF;
/// Polling watchers keep at most this many undrained events (oldest dropped).
const MAX_BUFFERED_EVENTS: usize = 65_536;

/// Filesystem magic numbers envd treats as network mounts.
const NETWORK_FS: [i64; 5] = [
	0x6969,      // NFS
	0x517b,      // SMB
	0xff53_4d42, // CIFS
	0xfe53_4d42, // SMB2
	0x6573_5546, // FUSE (incl. virtio-fs)
];

/// fsnotify op bits of an inotify mask, in envd's emission order.
pub fn event_types(mask: u32) -> Vec<EventType> {
	let mut out = Vec::new();
	if mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0 {
		out.push(EventType::Create);
	}
	if mask & (libc::IN_MOVE_SELF | libc::IN_MOVED_FROM) != 0 {
		out.push(EventType::Rename);
	}
	if mask & libc::IN_ATTRIB != 0 {
		out.push(EventType::Chmod);
	}
	if mask & libc::IN_MODIFY != 0 {
		out.push(EventType::Write);
	}
	if mask & (libc::IN_DELETE | libc::IN_DELETE_SELF) != 0 {
		out.push(EventType::Remove);
	}
	out
}

/// An armed inotify watch over a directory (optionally its subtree).
pub struct Watcher {
	fd:            AsyncFd<OwnedFd>,
	root:          PathBuf,
	recursive:     bool,
	include_entry: bool,
	/// wd → directory relative to `root` ("" = root).
	dirs:          HashMap<i32, PathBuf>,
	buf:           Vec<u8>,
}

impl Watcher {
	/// Validate `path` and arm the watch (envd: missing → `not_found`, not a
	/// directory or an unflagged network mount → `invalid_argument`).
	pub fn new(
		path: &Path,
		recursive: bool,
		include_entry: bool,
		allow_network_mounts: bool,
	) -> Result<Self, RpcError> {
		let meta = std::fs::metadata(path).map_err(|e| io_rpc_error(path, &e))?;
		if !meta.is_dir() {
			return Err(RpcError::invalid(format!("path {} not a directory", path.display())));
		}
		if !allow_network_mounts && is_network_mount(path) {
			return Err(RpcError::invalid(format!(
				"path {} is on a network mount; set allowNetworkMounts to watch it",
				path.display()
			)));
		}
		// SAFETY: inotify_init1 returns a new fd or -1.
		let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
		if raw < 0 {
			return Err(RpcError::internal(format!("inotify: {}", io::Error::last_os_error())));
		}
		// SAFETY: fresh fd we own.
		let owned = unsafe { OwnedFd::from_raw_fd(raw) };
		let fd = AsyncFd::new(owned).map_err(|e| RpcError::internal(format!("inotify: {e}")))?;
		let mut w = Self {
			fd,
			root: path.to_owned(),
			recursive,
			include_entry,
			dirs: HashMap::new(),
			buf: vec![0u8; 64 << 10],
		};
		w.add(Path::new(""))
			.map_err(|e| RpcError::internal(format!("error watching {}: {e}", path.display())))?;
		Ok(w)
	}

	/// Watch `rel` (and, when recursive, every directory below it).
	fn add(&mut self, rel: &Path) -> io::Result<()> {
		let abs = self.root.join(rel);
		let c = CString::new(abs.as_os_str().as_bytes())
			.map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
		// SAFETY: valid fd and NUL-terminated path.
		let wd = unsafe {
			libc::inotify_add_watch(
				self.fd.get_ref().as_raw_fd(),
				c.as_ptr(),
				WATCH_MASK | libc::IN_ONLYDIR,
			)
		};
		if wd < 0 {
			return Err(io::Error::last_os_error());
		}
		self.dirs.insert(wd, rel.to_owned());
		if self.recursive {
			let mut subdirs: Vec<_> = std::fs::read_dir(&abs)?
				.filter_map(Result::ok)
				.filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
				.map(|e| rel.join(e.file_name()))
				.collect();
			subdirs.sort();
			for sub in subdirs {
				// Directories that vanish or are unreadable are skipped.
				let _ = self.add(&sub);
			}
		}
		Ok(())
	}

	/// Wait for the next batch of events.
	pub async fn next_batch(&mut self) -> io::Result<Vec<FilesystemEvent>> {
		loop {
			let n = {
				let mut guard = self.fd.readable().await?;
				let buf = &mut self.buf;
				match guard.try_io(|fd| {
					// SAFETY: reading into an owned buffer.
					let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
					if n < 0 {
						Err(io::Error::last_os_error())
					} else {
						Ok(n.cast_unsigned())
					}
				}) {
					Ok(r) => r?,
					Err(_would_block) => continue,
				}
			};
			let raw: Vec<(i32, u32, Vec<u8>)> = parse_events(&self.buf[..n]);
			let events = self.translate(raw);
			if !events.is_empty() {
				return Ok(events);
			}
		}
	}

	fn translate(&mut self, raw: Vec<(i32, u32, Vec<u8>)>) -> Vec<FilesystemEvent> {
		let mut names = NameCache::default();
		let mut out = Vec::new();
		for (wd, mask, name) in raw {
			if mask & libc::IN_IGNORED != 0 {
				self.dirs.remove(&wd);
				continue;
			}
			let Some(dir) = self.dirs.get(&wd).cloned() else {
				continue;
			};
			let is_self = name.is_empty();
			if is_self && !dir.as_os_str().is_empty() {
				// The parent watch already reported this subdirectory.
				continue;
			}
			let rel = if is_self {
				PathBuf::from(".")
			} else {
				dir.join(std::ffi::OsStr::from_bytes(&name))
			};
			if self.recursive
				&& mask & libc::IN_ISDIR != 0
				&& mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0
			{
				let _ = self.add(&rel);
			}
			for ty in event_types(mask) {
				let entry = (self.include_entry
					&& matches!(ty, EventType::Create | EventType::Write | EventType::Chmod))
				.then(|| entry_info(&self.root.join(&rel), &mut names).ok())
				.flatten();
				out.push(FilesystemEvent {
					name: rel.to_string_lossy().into_owned(),
					r#type: ty as i32,
					entry,
				});
			}
		}
		out
	}
}

/// Split a buffer of `struct inotify_event` records into (wd, mask, name).
fn parse_events(buf: &[u8]) -> Vec<(i32, u32, Vec<u8>)> {
	const HDR: usize = 16;
	let mut out = Vec::new();
	let mut off = 0;
	while off + HDR <= buf.len() {
		let word = |i: usize| [buf[off + i], buf[off + i + 1], buf[off + i + 2], buf[off + i + 3]];
		let wd = i32::from_ne_bytes(word(0));
		let mask = u32::from_ne_bytes(word(4));
		let len = u32::from_ne_bytes(word(12)) as usize;
		let end = (off + HDR + len).min(buf.len());
		let name = &buf[off + HDR..end];
		let name = name.split(|&b| b == 0).next().unwrap_or(&[]).to_vec();
		out.push((wd, mask, name));
		off += HDR + len;
	}
	out
}

fn is_network_mount(path: &Path) -> bool {
	let Ok(c) = CString::new(path.as_os_str().as_bytes()) else {
		return false;
	};
	// SAFETY: zeroed statfs is a valid out-parameter.
	let mut st: libc::statfs = unsafe { std::mem::zeroed() };
	// SAFETY: valid path and out-pointer.
	if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
		return false;
	}
	#[allow(
		clippy::useless_conversion,
		clippy::unnecessary_cast,
		reason = "f_type width differs per libc"
	)]
	let ty = i64::from(st.f_type as i64) & 0xffff_ffff;
	NETWORK_FS.contains(&ty)
}

/// Stream a `WatchDir`: `start{}`, then events and keepalives until the
/// client leaves.
pub async fn stream(
	mut watcher: Watcher,
	sink: &StreamSink,
	keepalive: Duration,
) -> Result<(), RpcError> {
	let msg = |e: wd::Event| pb::WatchDirResponse { event: Some(e) };
	if sink
		.send(&msg(wd::Event::Start(wd::StartEvent {})))
		.await
		.is_err()
	{
		return Ok(());
	}
	let mut next_ping = tokio::time::Instant::now() + keepalive;
	loop {
		tokio::select! {
			batch = watcher.next_batch() => {
				let batch = batch.map_err(|e| RpcError::internal(format!("watcher error: {e}")))?;
				for event in batch {
					if sink.send(&msg(wd::Event::Filesystem(event))).await.is_err() {
						return Ok(());
					}
				}
				next_ping = tokio::time::Instant::now() + keepalive;
			},
			() = tokio::time::sleep_until(next_ping) => {
				if sink.send(&msg(wd::Event::Keepalive(wd::KeepAlive {}))).await.is_err() {
					return Ok(());
				}
				next_ping = tokio::time::Instant::now() + keepalive;
			},
			() = sink.closed() => return Ok(()),
		}
	}
}

struct Polled {
	events: Mutex<(VecDeque<FilesystemEvent>, Option<String>)>,
	task:   tokio::task::JoinHandle<()>,
}

/// Polling watchers by id.
#[derive(Default)]
pub struct Registry {
	watchers: Mutex<HashMap<String, Arc<Polled>>>,
}

impl Registry {
	/// Start buffering events of `watcher`; returns its id.
	pub fn create(&self, mut watcher: Watcher) -> String {
		let id = random_id();
		let slot = Arc::new_cyclic(|weak: &std::sync::Weak<Polled>| {
			let weak = weak.clone();
			let task = tokio::spawn(async move {
				loop {
					let batch = watcher.next_batch().await;
					let Some(p) = weak.upgrade() else { return };
					let mut guard = p.events.lock();
					match batch {
						Ok(events) => {
							guard.0.extend(events);
							while guard.0.len() > MAX_BUFFERED_EVENTS {
								guard.0.pop_front();
							}
						},
						Err(e) => {
							guard.1 = Some(format!("watcher error: {e}"));
							return;
						},
					}
				}
			});
			Polled { events: Mutex::new((VecDeque::new(), None)), task }
		});
		self.watchers.lock().insert(id.clone(), slot);
		id
	}

	/// Drain buffered events (a watcher failure is returned as the error).
	pub fn drain(&self, id: &str) -> Result<Vec<FilesystemEvent>, RpcError> {
		let w = self
			.watchers
			.lock()
			.get(id)
			.cloned()
			.ok_or_else(|| RpcError::not_found(format!("watcher with id {id} not found")))?;
		let mut guard = w.events.lock();
		if let Some(err) = guard.1.take() {
			return Err(RpcError::internal(err));
		}
		Ok(guard.0.drain(..).collect())
	}

	/// Stop and forget a watcher.
	pub fn remove(&self, id: &str) -> Result<(), RpcError> {
		let w = self
			.watchers
			.lock()
			.remove(id)
			.ok_or_else(|| RpcError::not_found(format!("watcher with id {id} not found")))?;
		w.task.abort();
		Ok(())
	}
}

fn random_id() -> String {
	let mut bytes = [0u8; 16];
	// SAFETY: getrandom fills the given buffer.
	let n = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
	if n != 16 {
		let t = super::time::now_ns().to_ne_bytes();
		bytes.copy_from_slice(&t);
	}
	bytes.iter().fold(String::with_capacity(32), |mut s, b| {
		let _ = std::fmt::Write::write_fmt(&mut s, format_args!("{b:02x}"));
		s
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn op_bits_split_in_envd_order() {
		assert_eq!(event_types(libc::IN_CREATE), [EventType::Create]);
		assert_eq!(event_types(libc::IN_MOVED_TO), [EventType::Create]);
		assert_eq!(event_types(libc::IN_MOVED_FROM), [EventType::Rename]);
		assert_eq!(
			event_types(libc::IN_DELETE | libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_CREATE),
			[EventType::Create, EventType::Chmod, EventType::Write, EventType::Remove]
		);
		let ignored = event_types(libc::IN_IGNORED);
		assert!(ignored.is_empty(), "unexpected IN_IGNORED events: {ignored:?}");
	}

	fn tmp(name: &str) -> PathBuf {
		let p = std::env::temp_dir().join(format!("envd-watch-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&p);
		std::fs::create_dir_all(&p).unwrap();
		p
	}

	async fn collect(w: &mut Watcher, want: usize) -> Vec<(String, EventType)> {
		let mut out = Vec::new();
		while out.len() < want {
			let batch = tokio::time::timeout(Duration::from_secs(5), w.next_batch())
				.await
				.unwrap()
				.unwrap();
			out.extend(
				batch
					.into_iter()
					.map(|e| (e.name, EventType::try_from(e.r#type).unwrap())),
			);
		}
		out
	}

	#[tokio::test]
	async fn non_recursive_reports_direct_children_only() {
		let root = tmp("flat");
		std::fs::create_dir(root.join("sub")).unwrap();
		let mut w = Watcher::new(&root, false, true, false).unwrap();
		std::fs::write(root.join("sub/nested"), "x").unwrap();
		std::fs::write(root.join("a.txt"), "x").unwrap();
		let events = collect(&mut w, 2).await;
		assert_eq!(events[0], ("a.txt".into(), EventType::Create));
		assert_eq!(events[1], ("a.txt".into(), EventType::Write));
		assert!(events.iter().all(|(n, _)| !n.contains("nested")));
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[tokio::test]
	async fn recursive_follows_new_directories_with_relative_names() {
		let root = tmp("rec");
		let mut w = Watcher::new(&root, true, false, false).unwrap();
		std::fs::create_dir(root.join("d")).unwrap();
		let first = collect(&mut w, 1).await;
		assert_eq!(first[0], ("d".into(), EventType::Create));
		std::fs::write(root.join("d/f"), "x").unwrap();
		std::fs::remove_file(root.join("d/f")).unwrap();
		let events = collect(&mut w, 3).await;
		assert_eq!(events[0], ("d/f".into(), EventType::Create));
		assert_eq!(events.last().unwrap(), &("d/f".into(), EventType::Remove));
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn validation_codes() {
		let root = tmp("val");
		std::fs::write(root.join("f"), "").unwrap();
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		rt.block_on(async {
			assert_eq!(
				Watcher::new(&root.join("missing"), false, false, false)
					.err()
					.unwrap()
					.code,
				super::super::connect::Code::NotFound
			);
			assert_eq!(
				Watcher::new(&root.join("f"), false, false, false)
					.err()
					.unwrap()
					.code,
				super::super::connect::Code::InvalidArgument
			);
		});
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[tokio::test]
	async fn polling_registry_drains_and_removes() {
		let root = tmp("poll");
		let reg = Registry::default();
		let id = reg.create(Watcher::new(&root, false, false, false).unwrap());
		std::fs::write(root.join("x"), "").unwrap();
		let mut got = Vec::new();
		for _ in 0..100 {
			got.extend(reg.drain(&id).unwrap());
			if !got.is_empty() {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		assert_eq!(got[0].name, "x");
		// Drained events are not delivered twice.
		assert!(
			reg.drain(&id)
				.unwrap()
				.iter()
				.all(|e| e.r#type != EventType::Create as i32)
		);
		reg.remove(&id).unwrap();
		assert_eq!(reg.remove(&id).unwrap_err().code, super::super::connect::Code::NotFound);
		assert_eq!(reg.drain(&id).unwrap_err().code, super::super::connect::Code::NotFound);
		std::fs::remove_dir_all(&root).unwrap();
	}
}
