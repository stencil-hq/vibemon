#[cfg(target_os = "linux")]
fn main() {
	if let Err(err) = linux_agent::run() {
		eprintln!("vmon-agent: {err}");
		std::process::exit(1);
	}
}

#[cfg(not(target_os = "linux"))]
fn main() {
	eprintln!("vmon-agent is only supported on Linux guests");
	std::process::exit(1);
}

#[cfg(target_os = "linux")]
mod linux_agent {
	use std::{
		collections::{HashMap, HashSet},
		ffi::CString,
		fs::{self, File, OpenOptions},
		io::{self, Read, Write},
		net::{Ipv4Addr, TcpStream, ToSocketAddrs},
		os::unix::{
			ffi::OsStrExt,
			fs::{MetadataExt, OpenOptionsExt},
			io::{AsRawFd, FromRawFd},
			process::{CommandExt, ExitStatusExt},
		},
		process::{Command, Stdio},
		str::FromStr,
		sync::{Arc, Mutex, MutexGuard},
		thread,
		time::{Duration, SystemTime, UNIX_EPOCH},
	};

	use serde::Deserialize;
	use serde_json::{Value, json};
	use vmon_agent::proto::{self, Frame};

	type SharedWriter = Arc<Mutex<File>>;
	type Sessions = Arc<Mutex<HashMap<u32, Session>>>;
	type PendingWrites = Arc<Mutex<HashMap<u32, PendingWrite>>>;
	type PtySessions = Arc<Mutex<HashMap<String, PtySessionState>>>;

	struct PtySessionState {
		pid:        i32,
		master:     File,
		cols:       u16,
		rows:       u16,
		exec:       Option<String>,
		created_at: u64,
		binding:    Option<proto::PtyBinding>,
		scrollback: proto::Scrollback,
		exit:       Option<(i32, SystemTime)>,
	}

	const HVC0: &str = "/dev/hvc0";
	const CONSOLE: &str = "/dev/console";
	const DEFAULT_IFACE: &str = "eth0";
	const STREAM_CHUNK: usize = 64 * 1024;

	#[repr(C)]
	#[allow(clippy::struct_field_names, reason = "netlink kernel ABI field names")]
	struct IfInfoMsg {
		ifi_family: u8,
		ifi_pad:    u8,
		ifi_type:   u16,
		ifi_index:  i32,
		ifi_flags:  u32,
		ifi_change: u32,
	}

	#[repr(C)]
	#[allow(clippy::struct_field_names, reason = "netlink kernel ABI field names")]
	struct IfAddrMsg {
		ifa_family:    u8,
		ifa_prefixlen: u8,
		ifa_flags:     u8,
		ifa_scope:     u8,
		ifa_index:     u32,
	}

	#[repr(C)]
	#[allow(clippy::struct_field_names, reason = "netlink kernel ABI field names")]
	struct RtMsg {
		rtm_family:   u8,
		rtm_dst_len:  u8,
		rtm_src_len:  u8,
		rtm_tos:      u8,
		rtm_table:    u8,
		rtm_protocol: u8,
		rtm_scope:    u8,
		rtm_type:     u8,
		rtm_flags:    u32,
	}

	#[repr(C)]
	struct RtAttr {
		rta_len:  u16,
		rta_type: u16,
	}

	struct Session {
		pid:        i32,
		stdin:      Option<std::process::ChildStdin>,
		tty_master: Option<File>,
	}

	struct PendingWrite {
		file:  File,
		bytes: u64,
	}

	pub fn run() -> Result<(), String> {
		setup_pid1()?;
		set_child_subreaper()?;

		let cmdline = read_cmdline();
		match cmdline.get("vmon.agent").map(String::as_str) {
			Some("run") => run_one_shot(&cmdline),
			_ => serve(),
		}
	}

	/// Put the virtio-console RPC device into raw mode.
	///
	/// `/dev/hvc0` is a tty, so it comes up in canonical line-discipline mode:
	/// reads block until a newline and the discipline rewrites CR/LF and
	/// intercepts control bytes. The agent protocol is length-prefixed binary,
	/// so without raw mode `read_frame` stalls and frames are corrupted. A
	/// non-tty backend (`ENOTTY`) needs no change.
	fn set_hvc_raw(device: &File) -> Result<(), String> {
		let fd = device.as_raw_fd();
		// SAFETY: `fd` is a valid open file descriptor owned by `device`, and
		// `termios` is fully initialized by `tcgetattr` before use.
		unsafe {
			let mut termios: libc::termios = std::mem::zeroed();
			if libc::tcgetattr(fd, &mut termios) != 0 {
				let err = io::Error::last_os_error();
				if err.raw_os_error() == Some(libc::ENOTTY) {
					return Ok(());
				}
				return Err(format!("tcgetattr {HVC0}: {err}"));
			}
			libc::cfmakeraw(&mut termios);
			if libc::tcsetattr(fd, libc::TCSANOW, &termios) != 0 {
				return Err(format!("tcsetattr {HVC0}: {}", io::Error::last_os_error()));
			}
			// Discard anything the tty buffered (and line-discipline-mangled) before
			// raw mode took effect — kernel/boot console noise on hvc0, or canonical
			// CR/LF rewrites — so the first `read_frame` starts on a clean boundary.
			// The host only sends real frames in response to later client actions,
			// so flushing the startup buffer never drops a genuine request.
			if libc::tcflush(fd, libc::TCIFLUSH) != 0 {
				return Err(format!("tcflush {HVC0}: {}", io::Error::last_os_error()));
			}
		}
		Ok(())
	}

	fn serve() -> Result<(), String> {
		let device = OpenOptions::new()
			.read(true)
			.write(true)
			.open(HVC0)
			.map_err(|err| format!("open {HVC0}: {err}"))?;
		set_hvc_raw(&device)?;
		// Announce readiness with one complete alignment marker. The host bridge
		// discards this marker before accepting clients, so no partial startup
		// frame can leave a newly connected reader out of alignment.
		{
			let mut out = &device;
			out.write_all(&proto::SYNC)
				.map_err(|err| format!("write ready marker to {HVC0}: {err}"))?;
			out.flush()
				.map_err(|err| format!("flush ready marker to {HVC0}: {err}"))?;
		}
		let mut reader = device
			.try_clone()
			.map_err(|err| format!("clone {HVC0}: {err}"))?;
		let writer = Arc::new(Mutex::new(device));
		let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
		let pty_sessions: PtySessions = Arc::new(Mutex::new(HashMap::new()));
		let pending_writes: PendingWrites = Arc::new(Mutex::new(HashMap::new()));
		spawn_orphan_reaper(sessions.clone(), pty_sessions.clone());

		let agent =
			Agent { writer, sessions, pty_sessions, pending_writes, epoch: Arc::new(Mutex::new(0)) };

		// Discard pre-protocol bytes (guest-kernel console noise on hvc0) up to the
		// host's SYNC marker so the frame loop starts on a clean boundary.
		if !proto::resync_to_marker(&mut reader).map_err(|err| format!("resync {HVC0}: {err}"))? {
			return Ok(());
		}

		loop {
			match proto::read_frame(&mut reader) {
				Ok(Some(frame)) => agent.handle_frame(frame),
				Ok(None) => return Ok(()),
				Err(err) => return Err(format!("read frame: {err}")),
			}
		}
	}

	struct Agent {
		writer:         SharedWriter,
		sessions:       Sessions,
		pty_sessions:   PtySessions,
		pending_writes: PendingWrites,
		epoch:          Arc<Mutex<u64>>,
	}

	impl Agent {
		fn handle_frame(&self, frame: Frame) {
			match frame.ty {
				proto::FRAME_REQ => self.handle_request(frame.id, &frame.payload),
				proto::FRAME_STDIN => self.handle_stdin(frame.id, &frame.payload),
				proto::FRAME_KILL => self.handle_kill(frame.id, &frame.payload),
				other => self.send_error(frame.id, format!("unknown frame type {other}")),
			}
		}

		fn handle_request(&self, id: u32, payload: &[u8]) {
			let request: Value = match serde_json::from_slice(payload) {
				Ok(request) => request,
				Err(err) => {
					self.send_error(id, format!("bad request json: {err}"));
					return;
				},
			};

			let Some(op) = request.get("op").and_then(Value::as_str) else {
				self.send_error(id, "missing op");
				return;
			};

			match op {
				proto::OP_HELLO => self.hello(id, &request),
				"ping" => self.send_resp(
					id,
					json!({
						 "ok": true,
						 "version": env!("CARGO_PKG_VERSION"),
						 "arch": std::env::consts::ARCH,
					}),
				),
				"activity" => match activity_sample() {
					Ok(sample) => self.send_resp(id, sample),
					Err(error) => self.send_error(id, format!("reading guest activity: {error}")),
				},
				"exec" => self.start_exec(id, &request),
				"pty_open" => self.pty_open(id, &request),
				"pty_attach" => self.pty_attach(id, &request),
				"pty_detach" => self.pty_detach(id, &request),
				"pty_resize" => self.pty_resize(id, &request),
				"pty_list" => self.pty_list(id),
				"pty_close" => self.pty_close(id, &request),
				"pty_exec" => self.pty_exec(id, &request),
				"fs_read" => self.fs_read(id, &request),
				"fs_sync" => {
					// SAFETY: `sync` has no pointer arguments and flushes every mounted filesystem.
					unsafe { libc::sync() };
					self.send_resp(id, json!({"ok": true}));
				},
				"fs_write" => self.fs_write(id, &request),
				"fs_list" => self.fs_list(id, &request),
				"fs_stat" => self.fs_stat(id, &request),
				"fs_mkdir" => self.fs_mkdir(id, &request),
				"fs_remove" => self.fs_remove(id, &request),
				"net_config" => self.net_config(id, &request),
				"resize" => self.resize(id, &request),
				"tcp_probe" => self.tcp_probe(id, &request),
				"mount" => self.mount(id, &request),
				"mount_overlay" => self.mount_overlay(id, &request),
				_ => self.send_error(id, "unknown op"),
			}
		}

		fn hello(&self, id: u32, request: &Value) {
			let Some(epoch) = request
				.get("epoch")
				.and_then(Value::as_u64)
				.filter(|value| *value != 0)
			else {
				self.send_error(id, "hello requires a non-zero epoch");
				return;
			};
			*lock(&self.epoch) = epoch;
			for session in lock(&self.pty_sessions).values_mut() {
				session.binding = None;
			}
			self.send_resp(id, json!({ "ok": true, "epoch": epoch }));
		}

		fn pty_open(&self, id: u32, request: &Value) {
			if lock(&self.pty_sessions).len() >= 128 {
				self.send_error(id, "session cap reached");
				return;
			}
			let session_id = match request
				.get("session_id")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
			{
				Some(value) => value.to_owned(),
				None => match fs::read_to_string("/proc/sys/kernel/random/uuid") {
					Ok(value) => value.trim().to_owned(),
					Err(_) => format!("pty-{}-{}", std::process::id(), id),
				},
			};
			if lock(&self.pty_sessions).contains_key(&session_id) {
				self.send_error(id, "session already exists");
				return;
			}
			let cols = request
				.get("cols")
				.and_then(Value::as_u64)
				.unwrap_or(80)
				.clamp(1, u16::MAX as u64) as u16;
			let rows = request
				.get("rows")
				.and_then(Value::as_u64)
				.unwrap_or(24)
				.clamp(1, u16::MAX as u64) as u16;
			let exec = request
				.get("exec")
				.and_then(Value::as_str)
				.map(str::to_owned);
			let mut master_fd = -1;
			let mut slave_fd = -1;
			let winsize =
				libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
			// SAFETY: openpty initializes both descriptors and only reads winsize.
			if unsafe {
				libc::openpty(
					&mut master_fd,
					&mut slave_fd,
					std::ptr::null_mut(),
					std::ptr::null(),
					&winsize,
				)
			} < 0
			{
				self.send_error(id, format!("openpty: {}", io::Error::last_os_error()));
				return;
			}
			if let Err(error) = set_cloexec(master_fd).and_then(|()| set_cloexec(slave_fd)) {
				close_fd(master_fd);
				close_fd(slave_fd);
				self.send_error(id, format!("pty cloexec: {error}"));
				return;
			}
			let mut command = Command::new("sh");
			if let Some(script) = &exec {
				command.args(["-lc", script]);
			} else {
				command.arg("-l");
			}
			if let Some(workdir) = request.get("workdir").and_then(Value::as_str) {
				command.current_dir(workdir);
			}
			if let Some(env) = request.get("env") {
				match env_object(env) {
					Ok(values) => command.envs(values),
					Err(error) => {
						close_fd(master_fd);
						close_fd(slave_fd);
						self.send_error(id, error);
						return;
					},
				};
			}
			// SAFETY: child-only closure uses async-signal-safe descriptor/session
			// syscalls.
			unsafe {
				command.pre_exec(move || {
					if libc::setsid() < 0 || libc::ioctl(slave_fd, libc::TIOCSCTTY, 0) < 0 {
						return Err(io::Error::last_os_error());
					}
					for target in 0..=2 {
						if libc::dup2(slave_fd, target) < 0 {
							return Err(io::Error::last_os_error());
						}
					}
					if slave_fd > 2 {
						libc::close(slave_fd);
					}
					libc::close(master_fd);
					Ok(())
				});
			}
			let mut child = match command.spawn() {
				Ok(child) => child,
				Err(error) => {
					close_fd(master_fd);
					close_fd(slave_fd);
					self.send_error(id, format!("spawn: {error}"));
					return;
				},
			};
			close_fd(slave_fd);
			// SAFETY: master_fd is uniquely owned after successful openpty.
			let master = unsafe { File::from_raw_fd(master_fd) };
			let reader = match master.try_clone() {
				Ok(reader) => reader,
				Err(error) => {
					let _ = signal_process_group(child.id() as i32, libc::SIGKILL);
					self.send_error(id, format!("pty clone: {error}"));
					return;
				},
			};
			let pid = child.id() as i32;
			let created_at = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.unwrap_or_default()
				.as_millis() as u64;
			let binding = proto::PtyBinding { epoch: *lock(&self.epoch), frame_id: id };
			lock(&self.pty_sessions).insert(session_id.clone(), PtySessionState {
				pid,
				master,
				cols,
				rows,
				exec,
				created_at,
				binding: Some(binding),
				scrollback: proto::Scrollback::new(proto::PTY_SCROLLBACK_LIMIT),
				exit: None,
			});
			self.send_resp(
				id,
				pty_meta(&session_id, lock(&self.pty_sessions).get(&session_id).unwrap()),
			);
			let sessions = self.pty_sessions.clone();
			let writer = self.writer.clone();
			let epoch = self.epoch.clone();
			thread::spawn(move || {
				let mut reader = reader;
				let mut buffer = vec![0u8; STREAM_CHUNK];
				loop {
					match reader.read(&mut buffer) {
						Ok(0) | Err(_) => break,
						Ok(count) => {
							if !publish_pty_output(
								&sessions,
								&writer,
								&epoch,
								&session_id,
								&buffer[..count],
							) {
								return;
							}
						},
					}
				}
				let status = child.wait().ok();
				let code = status.and_then(|status| status.code()).unwrap_or(-1);
				let binding = {
					let mut all = lock(&sessions);
					let Some(session) = all.get_mut(&session_id) else {
						return;
					};
					session.exit = Some((code, SystemTime::now()));
					session.binding.take()
				};
				if let Some(binding) = binding
					&& binding.epoch == *lock(&epoch)
				{
					send_json(&writer, proto::FRAME_EXIT, binding.frame_id, &json!({ "code": code }));
				}
			});
		}

		fn pty_attach(&self, id: u32, request: &Value) {
			let Some(session_id) = request.get("session_id").and_then(Value::as_str) else {
				self.send_error(id, "missing session_id");
				return;
			};
			let epoch = *lock(&self.epoch);
			let mut sessions = lock(&self.pty_sessions);
			let Some(session) = sessions.get_mut(session_id) else {
				self.send_error(id, "unknown pty session");
				return;
			};
			if let Some(cols) = request.get("cols").and_then(Value::as_u64) {
				session.cols = cols.clamp(1, u16::MAX as u64) as u16;
			}
			if let Some(rows) = request.get("rows").and_then(Value::as_u64) {
				session.rows = rows.clamp(1, u16::MAX as u64) as u16;
			}
			let winsize = libc::winsize {
				ws_row:    session.rows,
				ws_col:    session.cols,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			// SAFETY: the session owns a live PTY master and ioctl only reads winsize.
			unsafe {
				libc::ioctl(session.master.as_raw_fd(), libc::TIOCSWINSZ, &winsize);
			}
			let binding = proto::PtyBinding { epoch, frame_id: id };
			session.binding = Some(binding);
			let replay = session.scrollback.snapshot();
			let exit_code = session.exit.as_ref().map(|exit| exit.0);
			let meta = pty_meta(session_id, session);

			// Keep the session lock until the complete initial sequence is on the
			// wire. The output pump cannot publish live bytes ahead of replay.
			let result = {
				let mut writer = lock(&self.writer);
				write_pty_attach_frames(&mut *writer, id, &meta, &replay, exit_code)
			};
			if (result.is_err() || exit_code.is_some()) && session.binding == Some(binding) {
				session.binding = None;
			}
		}

		fn pty_detach(&self, id: u32, request: &Value) {
			let session_id = request.get("session_id").and_then(Value::as_str);
			let binding = proto::PtyBinding { epoch: *lock(&self.epoch), frame_id: id };
			for (name, session) in lock(&self.pty_sessions).iter_mut() {
				if session_id.is_none_or(|wanted| wanted == name) && session.binding == Some(binding) {
					session.binding = None;
				}
			}
			self.send_resp(id, json!({ "ok": true }));
		}

		fn pty_resize(&self, id: u32, request: &Value) {
			let Some(session_id) = request.get("session_id").and_then(Value::as_str) else {
				self.send_error(id, "missing session_id");
				return;
			};
			let mut sessions = lock(&self.pty_sessions);
			let Some(session) = sessions.get_mut(session_id) else {
				self.send_error(id, "unknown pty session");
				return;
			};
			if let Some(cols) = request.get("cols").and_then(Value::as_u64) {
				session.cols = cols.clamp(1, u16::MAX as u64) as u16;
			}
			if let Some(rows) = request.get("rows").and_then(Value::as_u64) {
				session.rows = rows.clamp(1, u16::MAX as u64) as u16;
			}
			let ws = libc::winsize {
				ws_row:    session.rows,
				ws_col:    session.cols,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			// SAFETY: the session owns a live PTY master and ioctl only reads winsize.
			let rc = unsafe { libc::ioctl(session.master.as_raw_fd(), libc::TIOCSWINSZ, &ws) };
			drop(sessions);
			if rc < 0 {
				self.send_error(id, format!("pty resize: {}", io::Error::last_os_error()));
			} else {
				self.send_resp(id, json!({ "ok": true }));
			}
		}

		fn pty_list(&self, id: u32) {
			let mut sessions = lock(&self.pty_sessions);
			sessions.retain(|_, session| {
				session.exit.is_none_or(|(_, exited_at)| {
					exited_at.elapsed().unwrap_or_default() < Duration::from_mins(1)
				})
			});
			let mut list = sessions
				.iter()
				.map(|(name, session)| pty_meta(name, session))
				.collect::<Vec<_>>();
			list.sort_by_key(|session| {
				session
					.get("created_at_unix_millis")
					.and_then(Value::as_u64)
					.unwrap_or_default()
			});
			self.send_resp(id, json!({ "ok": true, "sessions": list }));
		}

		fn pty_close(&self, id: u32, request: &Value) {
			let Some(session_id) = request.get("session_id").and_then(Value::as_str) else {
				self.send_error(id, "missing session_id");
				return;
			};
			let Some(session) = lock(&self.pty_sessions).remove(session_id) else {
				self.send_error(id, "unknown pty session");
				return;
			};
			let exit_code = session.exit.map(|exit| exit.0);
			let _ = signal_process_group(session.pid, libc::SIGKILL);
			if let Some(binding) = session.binding
				&& binding.epoch == *lock(&self.epoch)
			{
				send_json(
					&self.writer,
					proto::FRAME_EXIT,
					binding.frame_id,
					&json!({ "code": exit_code.unwrap_or(-libc::SIGKILL) }),
				);
			}
			self.send_resp(
				id,
				json!({
					"ok": true,
					"session_id": session_id,
					"exit_code": exit_code,
				}),
			);
		}

		fn pty_exec(&self, id: u32, request: &Value) {
			let Some(session_id) = request.get("session_id").and_then(Value::as_str) else {
				self.send_error(id, "missing session_id");
				return;
			};
			let Some(script) = request.get("command").and_then(Value::as_str) else {
				self.send_error(id, "missing command");
				return;
			};
			let pid = match lock(&self.pty_sessions).get(session_id) {
				Some(session) if session.exit.is_none() => session.pid,
				Some(_) => {
					self.send_error(id, "pty session has exited");
					return;
				},
				None => {
					self.send_error(id, "unknown pty session");
					return;
				},
			};
			let cwd = fs::read_link(format!("/proc/{pid}/cwd")).ok();
			let environ = fs::read(format!("/proc/{pid}/environ")).unwrap_or_default();
			let mut command = Command::new("sh");
			command
				.args(["-c", script])
				.stdin(Stdio::null())
				.stdout(Stdio::piped())
				.stderr(Stdio::piped());
			if let Some(cwd) = cwd {
				command.current_dir(cwd);
			}
			command.env_clear();
			for entry in environ
				.split(|byte| *byte == 0)
				.filter(|entry| !entry.is_empty())
			{
				if let Some(index) = entry.iter().position(|byte| *byte == b'=') {
					command.env(
						std::ffi::OsStr::from_bytes(&entry[..index]),
						std::ffi::OsStr::from_bytes(&entry[index + 1..]),
					);
				}
			}
			// SAFETY: setsid is async-signal-safe and isolates timeout termination.
			unsafe {
				command.pre_exec(|| {
					if libc::setsid() < 0 {
						Err(io::Error::last_os_error())
					} else {
						Ok(())
					}
				});
			}
			let mut child = match command.spawn() {
				Ok(child) => child,
				Err(error) => {
					self.send_error(id, format!("pty_exec spawn: {error}"));
					return;
				},
			};
			let stdout = child.stdout.take().unwrap();
			let stderr = child.stderr.take().unwrap();
			let stdout_reader =
				thread::spawn(move || read_bounded(stdout, proto::PTY_SCROLLBACK_LIMIT));
			let stderr_reader =
				thread::spawn(move || read_bounded(stderr, proto::PTY_SCROLLBACK_LIMIT));
			let timeout = Duration::from_millis(
				request
					.get("timeout_ms")
					.and_then(Value::as_u64)
					.unwrap_or(60_000),
			);
			let deadline = std::time::Instant::now() + timeout;
			let status = loop {
				match child.try_wait() {
					Ok(Some(status)) => break Ok(status),
					Ok(None) if std::time::Instant::now() < deadline => {
						thread::sleep(Duration::from_millis(10));
					},
					Ok(None) => {
						let _ = signal_process_group(child.id() as i32, libc::SIGKILL);
						break child.wait();
					},
					Err(error) => break Err(error),
				}
			};
			let stdout = stdout_reader.join().unwrap_or_default();
			let stderr = stderr_reader.join().unwrap_or_default();
			match status {
				Ok(status) => self.send_resp(
					id,
					json!({
						"ok": true,
						"stdout": stdout,
						"stderr": stderr,
						"code": status.code().unwrap_or(-1),
					}),
				),
				Err(error) => self.send_error(id, format!("pty_exec wait: {error}")),
			}
		}

		fn start_exec(&self, id: u32, request: &Value) {
			let tty = request.get("tty").and_then(Value::as_bool).unwrap_or(false);

			let cmd = match string_array(request, "cmd") {
				Ok(cmd) if !cmd.is_empty() => cmd,
				Ok(_) => {
					self.send_error(id, "cmd must not be empty");
					return;
				},
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let mut command = Command::new(&cmd[0]);
			command.args(&cmd[1..]);
			if let Some(cwd) = request.get("cwd").and_then(Value::as_str) {
				command.current_dir(cwd);
			}
			command.env_clear();
			if let Some(env) = request.get("env") {
				match env_object(env) {
					Ok(env) => {
						for (key, value) in env {
							command.env(key, value);
						}
					},
					Err(err) => {
						self.send_error(id, err);
						return;
					},
				}
			}

			// A tty session runs the child as a session leader whose controlling
			// terminal is the pty slave bound to fd 0/1/2; the parent keeps the
			// master for stdin writes and resize. Non-tty sessions keep separate
			// piped stdin/stdout/stderr exactly as before.
			let mut pty_fds: Option<(libc::c_int, libc::c_int)> = None;
			if tty {
				let mut master_fd: libc::c_int = -1;
				let mut slave_fd: libc::c_int = -1;
				// SAFETY: `openpty` initializes the two out-pointers on success;
				// name, termios, and winsize are optional and intentionally null.
				let rc = unsafe {
					libc::openpty(
						&mut master_fd,
						&mut slave_fd,
						std::ptr::null_mut(),
						std::ptr::null(),
						std::ptr::null(),
					)
				};
				if rc < 0 {
					self.send_error(id, format!("openpty: {}", io::Error::last_os_error()));
					return;
				}
				if let Err(err) = set_cloexec(master_fd) {
					close_fd(master_fd);
					close_fd(slave_fd);
					self.send_error(id, format!("openpty master cloexec: {err}"));
					return;
				}
				if let Err(err) = set_cloexec(slave_fd) {
					close_fd(master_fd);
					close_fd(slave_fd);
					self.send_error(id, format!("openpty slave cloexec: {err}"));
					return;
				}
				pty_fds = Some((master_fd, slave_fd));
				// SAFETY: `pre_exec` runs in the child after fork and before
				// exec. The closure only invokes async-signal-safe libc calls to
				// create a new session, attach the controlling tty, duplicate the
				// pty slave onto stdio, and close inherited pty descriptors.
				unsafe {
					command.pre_exec(move || {
						if libc::setsid() < 0 {
							return Err(io::Error::last_os_error());
						}
						if libc::ioctl(slave_fd, libc::TIOCSCTTY, 0 as libc::c_int) < 0 {
							return Err(io::Error::last_os_error());
						}
						if libc::dup2(slave_fd, 0) < 0
							|| libc::dup2(slave_fd, 1) < 0
							|| libc::dup2(slave_fd, 2) < 0
						{
							return Err(io::Error::last_os_error());
						}
						if slave_fd > 2 {
							libc::close(slave_fd);
						}
						libc::close(master_fd);
						Ok(())
					});
				}
			} else {
				command
					.stdin(Stdio::piped())
					.stdout(Stdio::piped())
					.stderr(Stdio::piped());
				// SAFETY: `pre_exec` runs in the child after fork and before
				// exec. `setsid` is async-signal-safe and isolates the process
				// group used by later kill requests.
				unsafe {
					command.pre_exec(|| {
						if libc::setsid() < 0 {
							Err(io::Error::last_os_error())
						} else {
							Ok(())
						}
					});
				}
			}

			let mut child = match command.spawn() {
				Ok(child) => child,
				Err(err) => {
					if let Some((master_fd, slave_fd)) = pty_fds {
						close_fd(master_fd);
						close_fd(slave_fd);
					}
					self.send_error(id, format!("spawn: {err}"));
					return;
				},
			};

			let pid = child.id() as i32;

			let (stdin, tty_master, joins) = if let Some((master_fd, slave_fd)) = pty_fds {
				// The child holds its own slave reference now; drop the parent's.
				close_fd(slave_fd);
				// SAFETY: `master_fd` came from `openpty`, is still owned by this
				// process, and has not been wrapped in a `File` before this point.
				let master = unsafe { File::from_raw_fd(master_fd) };
				let reader = match master.try_clone() {
					Ok(reader) => reader,
					Err(err) => {
						let _ = signal_process_group(pid, libc::SIGKILL);
						self.send_error(id, format!("pty clone: {err}"));
						return;
					},
				};
				// Under a tty, stdout and stderr are merged onto the master.
				let join = spawn_stream_thread(self.writer.clone(), id, proto::FRAME_STDOUT, reader);
				(None, Some(master), vec![join])
			} else {
				let stdin = child.stdin.take();
				let stdout = child.stdout.take();
				let stderr = child.stderr.take();
				let mut joins = Vec::new();
				if let Some(stream) = stdout {
					joins.push(spawn_stream_thread(
						self.writer.clone(),
						id,
						proto::FRAME_STDOUT,
						stream,
					));
				}
				if let Some(stream) = stderr {
					joins.push(spawn_stream_thread(
						self.writer.clone(),
						id,
						proto::FRAME_STDERR,
						stream,
					));
				}
				(stdin, None, joins)
			};

			lock(&self.sessions).insert(id, Session { pid, stdin, tty_master });

			let sessions = self.sessions.clone();
			let writer = self.writer.clone();
			thread::spawn(move || {
				let status = child.wait();
				let removed = lock(&sessions).remove(&id);
				drop(removed);

				for join in joins {
					let _ = join.join();
				}

				let payload = match status {
					Ok(status) => {
						let signal = status.signal();
						let code = status
							.code()
							.unwrap_or_else(|| signal.map_or(-1, |sig| -sig));
						json!({
							 "code": code,
							 "signal": signal,
						})
					},
					Err(err) => json!({
						 "code": -1,
						 "signal": Value::Null,
						 "error": err.to_string(),
					}),
				};
				send_json(&writer, proto::FRAME_EXIT, id, &payload);
			});
		}

		fn handle_stdin(&self, id: u32, payload: &[u8]) {
			if self.handle_pending_write_stdin(id, payload) {
				return;
			}
			let epoch = *lock(&self.epoch);
			let mut pty_sessions = lock(&self.pty_sessions);
			if let Some(session) = pty_sessions
				.values_mut()
				.find(|session| session.binding == Some(proto::PtyBinding { epoch, frame_id: id }))
			{
				if payload.is_empty() {
					session.binding = None;
				} else if let Err(error) = session.master.write_all(payload) {
					drop(pty_sessions);
					self.send_error(id, format!("pty input: {error}"));
				}
				return;
			}
			drop(pty_sessions);

			let mut sessions = lock(&self.sessions);
			let Some(session) = sessions.get_mut(&id) else {
				drop(sessions);
				// An empty payload is an EOF / close-stdin signal. A quick command
				// can exit and have its session removed before the client's EOF
				// arrives, so closing a gone session's stdin is a benign no-op,
				// not an error.
				if !payload.is_empty() {
					self.send_error(id, "unknown stdin session");
				}
				return;
			};

			if session.tty_master.is_some() {
				// PTYs cannot be half-closed like pipes. An empty stdin frame is
				// the protocol EOF marker, so send the terminal EOF character
				// instead of dropping the master fd (the reader clone would keep
				// the pty open and leave shells waiting forever).
				let input: &[u8] = if payload.is_empty() { b"\x04" } else { payload };
				if let Some(master) = session.tty_master.as_mut()
					&& let Err(err) = master.write_all(input)
				{
					if payload.is_empty() {
						return;
					}
					drop(sessions);
					self.send_error(id, format!("tty stdin write: {err}"));
				}
				return;
			}

			if payload.is_empty() {
				session.stdin.take();
				return;
			}

			let Some(stdin) = session.stdin.as_mut() else {
				drop(sessions);
				self.send_error(id, "stdin closed");
				return;
			};

			if let Err(err) = stdin.write_all(payload) {
				drop(sessions);
				self.send_error(id, format!("stdin write: {err}"));
			}
		}

		fn handle_pending_write_stdin(&self, id: u32, payload: &[u8]) -> bool {
			let mut writes = lock(&self.pending_writes);
			let Some(write) = writes.get_mut(&id) else {
				return false;
			};

			if payload.is_empty() {
				let Some(mut finished) = writes.remove(&id) else {
					return true;
				};
				let result = finished.file.flush().map(|()| finished.bytes);
				drop(writes);
				match result {
					Ok(bytes) => self.send_resp(id, json!({"ok": true, "size": bytes})),
					Err(err) => self.send_io_error(id, format!("fs_write flush: {err}"), &err),
				}
				return true;
			}

			match write.file.write_all(payload) {
				Ok(()) => write.bytes += payload.len() as u64,
				Err(err) => {
					writes.remove(&id);
					drop(writes);
					self.send_io_error(id, format!("fs_write: {err}"), &err);
				},
			}
			true
		}

		fn handle_kill(&self, id: u32, payload: &[u8]) {
			let signal = if payload.is_empty() {
				libc::SIGTERM
			} else {
				match serde_json::from_slice::<Value>(payload)
					.ok()
					.and_then(|value| value.get("signal").and_then(Value::as_i64))
				{
					Some(signal) if signal > 0 && signal <= i32::MAX as i64 => signal as i32,
					_ => libc::SIGTERM,
				}
			};

			let pid = { lock(&self.sessions).get(&id).map(|session| session.pid) };
			let Some(pid) = pid else {
				self.send_error(id, "unknown kill session");
				return;
			};

			if let Err(err) = signal_process_group(pid, signal) {
				self.send_error(id, format!("kill: {err}"));
			}
		}

		fn fs_read(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let mut file = match File::open(&path) {
				Ok(file) => file,
				Err(err) => {
					self.send_io_error(id, format!("fs_read {path}: {err}"), &err);
					return;
				},
			};

			let mut total = 0u64;
			let mut buf = vec![0u8; STREAM_CHUNK];
			loop {
				match file.read(&mut buf) {
					Ok(0) => break,
					Ok(n) => {
						total += n as u64;
						if send_frame(&self.writer, proto::FRAME_STDOUT, id, &buf[..n]).is_err() {
							return;
						}
					},
					Err(err) => {
						self.send_io_error(id, format!("fs_read {path}: {err}"), &err);
						return;
					},
				}
			}
			let _ = send_frame(&self.writer, proto::FRAME_STDOUT, id, &[]);
			self.send_resp(id, json!({"ok": true, "size": total}));
		}

		fn fs_write(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let mode = request.get("mode").and_then(Value::as_u64).unwrap_or(0o644) as u32;

			let file = match OpenOptions::new()
				.create(true)
				.truncate(true)
				.write(true)
				.mode(mode)
				.open(&path)
			{
				Ok(file) => file,
				Err(err) => {
					self.send_io_error(id, format!("fs_write {path}: {err}"), &err);
					return;
				},
			};

			lock(&self.pending_writes).insert(id, PendingWrite { file, bytes: 0 });
		}

		fn fs_list(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			let entries = match fs::read_dir(&path) {
				Ok(entries) => entries,
				Err(err) => {
					self.send_io_error(id, format!("fs_list {path}: {err}"), &err);
					return;
				},
			};

			let mut result = Vec::new();
			for entry in entries {
				let entry = match entry {
					Ok(entry) => entry,
					Err(err) => {
						self.send_io_error(id, format!("fs_list {path}: {err}"), &err);
						return;
					},
				};
				let metadata = match entry.metadata() {
					Ok(metadata) => metadata,
					Err(err) => {
						self.send_io_error(id, format!("fs_list metadata: {err}"), &err);
						return;
					},
				};
				let name = entry.file_name().to_string_lossy().into_owned();
				result.push(json!({
					 "name": name,
					 "type": file_type(&metadata),
					 "size": metadata.len(),
					 "mode": metadata.mode(),
					 "mtime": metadata.mtime(),
				}));
			}

			self.send_resp(id, json!({"ok": true, "entries": result}));
		}

		fn fs_stat(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};

			match fs::symlink_metadata(&path) {
				Ok(metadata) => self.send_resp(
					id,
					json!({
						 "ok": true,
						 "type": file_type(&metadata),
						 "size": metadata.len(),
						 "mode": metadata.mode(),
						 "mtime": metadata.mtime(),
					}),
				),
				Err(err) => self.send_io_error(id, format!("fs_stat {path}: {err}"), &err),
			}
		}

		fn fs_mkdir(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let parents = request
				.get("parents")
				.and_then(Value::as_bool)
				.unwrap_or(true);
			let result = if parents {
				fs::create_dir_all(&path)
			} else {
				fs::create_dir(&path)
			};
			match result {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_io_error(id, format!("fs_mkdir {path}: {err}"), &err),
			}
		}

		fn fs_remove(&self, id: u32, request: &Value) {
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let recursive = request
				.get("recursive")
				.and_then(Value::as_bool)
				.unwrap_or(false);
			let metadata = match fs::symlink_metadata(&path) {
				Ok(metadata) => metadata,
				Err(err) => {
					self.send_io_error(id, format!("fs_remove {path}: {err}"), &err);
					return;
				},
			};
			let result = if recursive && metadata.is_dir() {
				fs::remove_dir_all(&path)
			} else if metadata.is_dir() {
				fs::remove_dir(&path)
			} else {
				fs::remove_file(&path)
			};
			match result {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_io_error(id, format!("fs_remove {path}: {err}"), &err),
			}
		}

		fn net_config(&self, id: u32, request: &Value) {
			let ip = match string_field(request, "ip") {
				Ok(ip) => ip,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let prefix = match request.get("prefix").and_then(Value::as_u64) {
				Some(prefix) if prefix <= 32 => prefix as u8,
				_ => {
					self.send_error(id, "prefix must be 0..=32");
					return;
				},
			};
			let gw = match string_field(request, "gw") {
				Ok(gw) => gw,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let dns = match request.get("dns") {
				Some(Value::Array(_)) => match string_array(request, "dns") {
					Ok(dns) => dns,
					Err(err) => {
						self.send_error(id, err);
						return;
					},
				},
				_ => Vec::new(),
			};

			match apply_net_config(DEFAULT_IFACE, &ip, prefix, &gw, &dns) {
				Ok(()) => self.send_resp(id, json!({"ok": true})),
				Err(err) => self.send_error(id, format!("net_config: {err}")),
			}
		}

		fn resize(&self, id: u32, request: &Value) {
			let rows = match request.get("rows").and_then(Value::as_u64) {
				Some(rows) if rows > 0 && u16::try_from(rows).is_ok() => rows as u16,
				_ => {
					self.send_error(id, "rows must be 1..=u16::MAX");
					return;
				},
			};
			let cols = match request.get("cols").and_then(Value::as_u64) {
				Some(cols) if cols > 0 && u16::try_from(cols).is_ok() => cols as u16,
				_ => {
					self.send_error(id, "cols must be 1..=u16::MAX");
					return;
				},
			};

			let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
			let rc = {
				let sessions = lock(&self.sessions);
				let Some(session) = sessions.get(&id) else {
					drop(sessions);
					self.send_error(id, "unknown resize session");
					return;
				};
				let Some(master) = session.tty_master.as_ref() else {
					drop(sessions);
					self.send_error(id, "session has no tty");
					return;
				};
				// SAFETY: `master` stays alive while the sessions lock is held;
				// TIOCSWINSZ reads the provided winsize and updates the pty.
				unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &ws) }
			};
			if rc < 0 {
				self.send_error(id, format!("resize: {}", io::Error::last_os_error()));
			} else {
				self.send_resp(id, json!({"ok": true}));
			}
		}

		fn tcp_probe(&self, id: u32, request: &Value) {
			let port = match request.get("port").and_then(Value::as_u64) {
				Some(port) if port >= 1 && u16::try_from(port).is_ok() => port as u16,
				_ => {
					self.send_error(id, "port must be 1..=65535");
					return;
				},
			};
			let host = request
				.get("host")
				.and_then(Value::as_str)
				.unwrap_or("127.0.0.1");

			let addr = match format!("{host}:{port}").to_socket_addrs() {
				Ok(mut addrs) => {
					if let Some(addr) = addrs.next() {
						addr
					} else {
						self.send_error(id, format!("tcp_probe: cannot resolve {host}:{port}"));
						return;
					}
				},
				Err(err) => {
					self.send_error(id, format!("tcp_probe resolve {host}:{port}: {err}"));
					return;
				},
			};

			// A refused or timed-out connection is a valid `connected: false`,
			// not an error: a closed port is exactly what a readiness probe asks.
			let connected = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok();
			self.send_resp(id, json!({"ok": true, "connected": connected}));
		}

		fn mount(&self, id: u32, request: &Value) {
			let tag = match string_field(request, "tag") {
				Ok(tag) => tag,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let fstype = request
				.get("fstype")
				.and_then(Value::as_str)
				.unwrap_or("virtiofs");
			let ro = request.get("ro").and_then(Value::as_bool).unwrap_or(false);

			if let Err(err) = fs::create_dir_all(&path) {
				self.send_error(id, format!("mount mkdir {path}: {err}"));
				return;
			}

			let Ok(source) = CString::new(tag.as_str()) else {
				self.send_error(id, "tag contains an interior nul byte");
				return;
			};
			let Ok(target) = CString::new(path.as_str()) else {
				self.send_error(id, "path contains an interior nul byte");
				return;
			};
			let Ok(fs_type) = CString::new(fstype) else {
				self.send_error(id, "fstype contains an interior nul byte");
				return;
			};
			let flags = if ro { libc::MS_RDONLY } else { 0 };

			// SAFETY: all three C strings are nul-terminated and live across the
			// call; data is null (virtiofs takes no mount options here).
			let rc = unsafe {
				libc::mount(source.as_ptr(), target.as_ptr(), fs_type.as_ptr(), flags, std::ptr::null())
			};
			if rc != 0 {
				self.send_error(id, format!("mount {tag} -> {path}: {}", io::Error::last_os_error()));
				return;
			}
			self.send_resp(id, json!({"ok": true}));
		}

		fn mount_overlay(&self, id: u32, request: &Value) {
			let tag = match string_field(request, "tag") {
				Ok(tag) => tag,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let path = match string_field(request, "path") {
				Ok(path) => path,
				Err(err) => {
					self.send_error(id, err);
					return;
				},
			};
			let base = format!("/var/lib/vmon/s3/{tag}");
			let lower = format!("{base}/lower");
			let upper = format!("{base}/upper");
			let work = format!("{base}/work");

			for directory in [&lower, &upper, &work, &path] {
				if let Err(err) = fs::create_dir_all(directory) {
					self.send_error(id, format!("mount_overlay mkdir {directory}: {err}"));
					return;
				}
			}

			let Ok(source) = CString::new(tag.as_str()) else {
				self.send_error(id, "mount_overlay tag contains an interior nul byte");
				return;
			};
			let Ok(lower_target) = CString::new(lower.as_str()) else {
				self.send_error(id, "mount_overlay lower path contains an interior nul byte");
				return;
			};
			let Ok(virtiofs) = CString::new("virtiofs") else {
				self.send_error(id, "mount_overlay virtiofs type contains an interior nul byte");
				return;
			};

			// SAFETY: the source, target, and filesystem type C strings are
			// nul-terminated and live across the call; virtiofs takes no data.
			let rc = unsafe {
				libc::mount(
					source.as_ptr(),
					lower_target.as_ptr(),
					virtiofs.as_ptr(),
					libc::MS_RDONLY,
					std::ptr::null(),
				)
			};
			if rc != 0 {
				self.send_error(
					id,
					format!("mount_overlay lower {tag} -> {lower}: {}", io::Error::last_os_error()),
				);
				return;
			}

			let Ok(overlay_source) = CString::new("overlay") else {
				self.send_error(id, "mount_overlay overlay source contains an interior nul byte");
				return;
			};
			let Ok(target) = CString::new(path.as_str()) else {
				self.send_error(id, "mount_overlay path contains an interior nul byte");
				return;
			};
			let Ok(overlay_type) = CString::new("overlay") else {
				self.send_error(id, "mount_overlay overlay type contains an interior nul byte");
				return;
			};
			let options = format!("lowerdir={lower},upperdir={upper},workdir={work}");
			let Ok(data) = CString::new(options) else {
				self.send_error(id, "mount_overlay options contain an interior nul byte");
				return;
			};

			// SAFETY: source, target, filesystem type, and data are
			// nul-terminated C strings that remain live across the call; data
			// contains the overlayfs lowerdir, upperdir, and workdir options.
			let rc = unsafe {
				libc::mount(
					overlay_source.as_ptr(),
					target.as_ptr(),
					overlay_type.as_ptr(),
					0,
					data.as_ptr().cast(),
				)
			};
			if rc != 0 {
				self.send_error(
					id,
					format!("mount_overlay overlay {path}: {}", io::Error::last_os_error()),
				);
				return;
			}

			self.send_resp(id, json!({"ok": true}));
		}

		fn send_resp(&self, id: u32, value: Value) {
			send_json(&self.writer, proto::FRAME_RESP, id, &value);
		}

		fn send_error(&self, id: u32, error: impl Into<String>) {
			self.send_resp(id, json!({"ok": false, "error": error.into()}));
		}

		fn send_io_error(&self, id: u32, error: impl Into<String>, source: &io::Error) {
			let error = error.into();
			let response = match source.raw_os_error() {
				Some(errno) => json!({"ok": false, "error": error, "errno": errno}),
				None => json!({"ok": false, "error": error}),
			};
			self.send_resp(id, response);
		}
	}

	fn activity_sample() -> io::Result<Value> {
		let stat = fs::read_to_string("/proc/stat")?;
		let cpu_ticks = stat
			.lines()
			.next()
			.filter(|line| line.starts_with("cpu "))
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing aggregate CPU row"))?
			.split_ascii_whitespace()
			.skip(1)
			.enumerate()
			.filter(|(index, _)| matches!(index, 0 | 1 | 2 | 5 | 6 | 7))
			.filter_map(|(_, value)| value.parse::<u64>().ok())
			.fold(0_u64, u64::saturating_add);
		let disk_sectors = fs::read_to_string("/proc/diskstats")?
			.lines()
			.filter_map(|line| {
				let mut fields = line.split_ascii_whitespace();
				let read = fields.nth(5)?.parse::<u64>().ok()?;
				let written = fields.nth(3)?.parse::<u64>().ok()?;
				Some(read.saturating_add(written))
			})
			.fold(0_u64, u64::saturating_add);
		let network_bytes = fs::read_to_string("/proc/net/dev")?
			.lines()
			.filter_map(|line| line.split_once(':').map(|(_, counters)| counters))
			.filter_map(|counters| {
				let mut fields = counters.split_ascii_whitespace();
				let received = fields.next()?.parse::<u64>().ok()?;
				let sent = fields.nth(7)?.parse::<u64>().ok()?;
				Some(received.saturating_add(sent))
			})
			.fold(0_u64, u64::saturating_add);
		Ok(json!({
			"ok": true,
			"cpu_ticks": cpu_ticks,
			"disk_sectors": disk_sectors,
			"network_bytes": network_bytes,
		}))
	}

	fn spawn_stream_thread<R>(
		writer: SharedWriter,
		id: u32,
		ty: u8,
		mut stream: R,
	) -> thread::JoinHandle<()>
	where
		R: Read + Send + 'static,
	{
		thread::spawn(move || {
			let mut buf = vec![0u8; STREAM_CHUNK];
			loop {
				match stream.read(&mut buf) {
					Ok(0) => break,
					Ok(n) => {
						if send_frame(&writer, ty, id, &buf[..n]).is_err() {
							return;
						}
					},
					Err(err) if err.kind() == io::ErrorKind::Interrupted => {},
					Err(_) => break,
				}
			}
			let _ = send_frame(&writer, ty, id, &[]);
		})
	}

	fn send_json(writer: &SharedWriter, ty: u8, id: u32, value: &Value) {
		if let Ok(payload) = serde_json::to_vec(value) {
			let _ = send_frame(writer, ty, id, &payload);
		}
	}

	fn send_frame(writer: &SharedWriter, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
		let mut guard = lock(writer);
		proto::write_frame(&mut *guard, ty, id, payload)?;
		guard.flush()
	}

	fn write_json_frame<W: Write>(writer: &mut W, ty: u8, id: u32, value: &Value) -> io::Result<()> {
		let payload = serde_json::to_vec(value)
			.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
		proto::write_frame(writer, ty, id, &payload)
	}

	fn write_pty_attach_frames<W: Write>(
		writer: &mut W,
		id: u32,
		meta: &Value,
		replay: &[u8],
		exit_code: Option<i32>,
	) -> io::Result<()> {
		write_json_frame(writer, proto::FRAME_RESP, id, meta)?;
		if !replay.is_empty() {
			proto::write_frame(writer, proto::FRAME_STDOUT, id, replay)?;
		}
		if let Some(code) = exit_code {
			write_json_frame(writer, proto::FRAME_EXIT, id, &json!({ "code": code }))?;
		}
		writer.flush()
	}

	fn publish_pty_output(
		sessions: &PtySessions,
		writer: &SharedWriter,
		epoch: &Arc<Mutex<u64>>,
		session_id: &str,
		output: &[u8],
	) -> bool {
		let binding = {
			let mut all = lock(sessions);
			let Some(session) = all.get_mut(session_id) else {
				return false;
			};
			session.scrollback.push(output);
			session.binding
		};
		if let Some(binding) = binding
			&& binding.epoch == *lock(epoch)
			&& send_frame(writer, proto::FRAME_STDOUT, binding.frame_id, output).is_err()
		{
			// The host connection died mid-write; unbind so the next attach on a
			// fresh epoch rebinds cleanly.
			if let Some(session) = lock(sessions).get_mut(session_id)
				&& session.binding == Some(binding)
			{
				session.binding = None;
			}
		}
		true
	}

	fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
		match mutex.lock() {
			Ok(guard) => guard,
			Err(poisoned) => poisoned.into_inner(),
		}
	}

	fn close_fd(fd: libc::c_int) {
		if fd < 0 {
			return;
		}
		// SAFETY: `fd` is an owned raw file descriptor on all call paths that
		// reach here. Cleanup ignores close errors because there is no recovery.
		let _ = unsafe { libc::close(fd) };
	}

	fn set_cloexec(fd: libc::c_int) -> io::Result<()> {
		// SAFETY: `fd` is an open descriptor; F_GETFD reads descriptor flags and
		// does not dereference any pointers.
		let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
		if flags < 0 {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: `fd` is an open descriptor; F_SETFD only updates descriptor
		// flags. FD_CLOEXEC prevents pty fds leaking into unrelated execs.
		let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
		if rc < 0 {
			Err(io::Error::last_os_error())
		} else {
			Ok(())
		}
	}

	fn signal_process_group(pid: i32, signal: i32) -> io::Result<()> {
		// SAFETY: `kill` with a negative pid targets the process group created
		// for this exec session by `setsid`; no pointers are involved.
		let rc = unsafe { libc::kill(-pid, signal) };
		if rc < 0 {
			Err(io::Error::last_os_error())
		} else {
			Ok(())
		}
	}

	fn string_field(value: &Value, field: &str) -> Result<String, String> {
		value
			.get(field)
			.and_then(Value::as_str)
			.map(ToOwned::to_owned)
			.ok_or_else(|| format!("{field} must be a string"))
	}

	fn string_array(value: &Value, field: &str) -> Result<Vec<String>, String> {
		let Some(items) = value.get(field).and_then(Value::as_array) else {
			return Err(format!("{field} must be an array of strings"));
		};
		items
			.iter()
			.map(|item| {
				item
					.as_str()
					.map(ToOwned::to_owned)
					.ok_or_else(|| format!("{field} must be an array of strings"))
			})
			.collect()
	}

	fn env_object(value: &Value) -> Result<Vec<(String, String)>, String> {
		let Some(object) = value.as_object() else {
			return Err("env must be an object".to_string());
		};
		object
			.iter()
			.map(|(key, value)| {
				value
					.as_str()
					.map(|value| (key.clone(), value.to_string()))
					.ok_or_else(|| "env values must be strings".to_string())
			})
			.collect()
	}

	fn pty_meta(session_id: &str, session: &PtySessionState) -> Value {
		json!({
			"session_id": session_id,
			"running": session.exit.is_none(),
			"exit_code": session.exit.as_ref().map(|exit| exit.0),
			"cols": session.cols,
			"rows": session.rows,
			"exec": session.exec.as_deref(),
			"created_at_unix_millis": session.created_at,
			"attached_count": u8::from(session.binding.is_some()),
		})
	}

	fn read_bounded<R: Read>(mut reader: R, limit: usize) -> Vec<u8> {
		let mut output = Vec::new();
		let mut buffer = vec![0u8; STREAM_CHUNK];
		loop {
			match reader.read(&mut buffer) {
				Ok(0) | Err(_) => return output,
				Ok(count) => {
					let remaining = limit.saturating_sub(output.len());
					output.extend_from_slice(&buffer[..count.min(remaining)]);
				},
			}
		}
	}
	fn file_type(metadata: &fs::Metadata) -> &'static str {
		let ty = metadata.file_type();
		if ty.is_file() {
			"file"
		} else if ty.is_dir() {
			"dir"
		} else if ty.is_symlink() {
			"symlink"
		} else {
			"other"
		}
	}

	fn spawn_orphan_reaper(sessions: Sessions, pty_sessions: PtySessions) {
		thread::spawn(move || {
			loop {
				reap_orphans(&sessions, &pty_sessions);
				thread::sleep(std::time::Duration::from_millis(100));
			}
		});
	}

	/// Reap reparented orphan/grandchild processes without stealing the exit
	/// status of any tracked exec-session leader.
	///
	/// As PID1 with `PR_SET_CHILD_SUBREAPER`, every descendant whose parent
	/// dies is reparented to us, so reaping must happen on every poll — not
	/// only when no sessions are active — or zombies accumulate while any exec
	/// is running.
	///
	/// Invariant: a session leader's zombie is owned by that session's waiter
	/// thread (`child.wait()` in `start_exec`), which is authoritative for
	/// emitting the EXIT frame with the real status. We must therefore never
	/// consume a leader's zombie here. To stay race-free we *peek* with
	/// `WNOWAIT` (which leaves the child reapable) instead of an unconditional
	/// `waitpid(-1)`: a tracked leader pid is left untouched for its own
	/// thread, while an untracked orphan is reaped (status discarded) with a
	/// targeted `waitpid(pid)`.
	///
	/// This is exempt from the snapshot/exit race because `start_exec` removes
	/// a session from the map only *after* `child.wait()` has reaped its
	/// leader: while a leader is a live zombie it is always present in the map
	/// (so we skip it), and once it leaves the map it has already been reaped
	/// (so `waitid` can no longer observe it). A leader's status is thus never
	/// double-reaped, regardless of how the 100ms poll interleaves with exits.
	fn reap_orphans(sessions: &Sessions, pty_sessions: &PtySessions) {
		// Snapshot tracked leader pids, then drop the locks before any wait syscall.
		let mut leaders: HashSet<i32> = lock(sessions).values().map(|session| session.pid).collect();
		leaders.extend(lock(pty_sessions).values().map(|session| session.pid));

		loop {
			// SAFETY: an all-zero `siginfo_t` is a valid initial buffer for
			// `waitid`, which fully initializes the fields it reports.
			let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
			// SAFETY: `info` is a valid writable pointer, and WNOWAIT ensures
			// tracked session leaders remain reapable by their waiter threads.
			let rc = unsafe {
				libc::waitid(libc::P_ALL, 0, &mut info, libc::WEXITED | libc::WNOHANG | libc::WNOWAIT)
			};
			// rc == -1: no children left (ECHILD) or interrupted — retry next poll.
			if rc == -1 {
				break;
			}
			// SAFETY: `waitid` returned success and `info` has been initialized.
			// WNOHANG with nothing waitable leaves si_pid == 0.
			let pid = unsafe { info.si_pid() };
			if pid == 0 {
				break;
			}
			if leaders.contains(&pid) {
				// Tracked leader: leave the zombie for its owning session
				// thread. WNOWAIT did not consume it, so a re-peek would just
				// return this same front-of-list child again; stop scanning
				// this poll. The blocked `child.wait()` reaps it within
				// microseconds, and any orphan behind it is collected on the
				// next 100ms tick.
				break;
			}
			// Untracked orphan reparented to us: reap it and discard the status.
			let mut status = 0;
			// SAFETY: `pid` came from `waitid` and is still waitable because the
			// earlier call used WNOWAIT.
			let _ = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
		}
	}

	fn setup_pid1() -> Result<(), String> {
		// SAFETY: `getpid` has no preconditions and touches no Rust memory.
		if unsafe { libc::getpid() } != 1 {
			return Ok(());
		}
		fs::create_dir_all("/proc").map_err(|err| format!("create /proc: {err}"))?;
		fs::create_dir_all("/sys").map_err(|err| format!("create /sys: {err}"))?;
		fs::create_dir_all("/dev").map_err(|err| format!("create /dev: {err}"))?;
		mount_fs("proc", "/proc", "proc")?;
		mount_fs("sysfs", "/sys", "sysfs")?;
		mount_fs("devtmpfs", "/dev", "devtmpfs")?;
		fs::create_dir_all("/dev/pts").map_err(|err| format!("create /dev/pts: {err}"))?;
		mount_fs("devpts", "/dev/pts", "devpts")?;
		Ok(())
	}

	fn mount_fs(source: &str, target: &str, fstype: &str) -> Result<(), String> {
		let source = CString::new(source).map_err(|err| err.to_string())?;
		let target = CString::new(target).map_err(|err| err.to_string())?;
		let fstype = CString::new(fstype).map_err(|err| err.to_string())?;
		// SAFETY: all C strings are nul-terminated and live across the call;
		// data is null because these pseudo-filesystems need no mount options.
		let rc = unsafe {
			libc::mount(source.as_ptr(), target.as_ptr(), fstype.as_ptr(), 0, std::ptr::null())
		};
		if rc == 0 {
			return Ok(());
		}
		let err = io::Error::last_os_error();
		if err.raw_os_error() == Some(libc::EBUSY) {
			Ok(())
		} else {
			Err(format!("mount {target:?}: {err}"))
		}
	}

	fn set_child_subreaper() -> Result<(), String> {
		// SAFETY: PR_SET_CHILD_SUBREAPER takes integer arguments only; enabling
		// it for the agent lets PID1 reap orphaned descendants.
		let rc = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
		if rc == 0 {
			Ok(())
		} else {
			Err(format!("prctl PR_SET_CHILD_SUBREAPER: {}", io::Error::last_os_error()))
		}
	}

	fn read_cmdline() -> HashMap<String, String> {
		fs::read_to_string("/proc/cmdline")
			.unwrap_or_default()
			.split_whitespace()
			.map(|part| match part.split_once('=') {
				Some((key, value)) => (key.to_string(), value.to_string()),
				None => (part.to_string(), "1".to_string()),
			})
			.collect()
	}

	#[derive(Deserialize)]
	struct Entry {
		cmd: Vec<String>,
		cwd: Option<String>,
		env: Option<HashMap<String, String>>,
	}

	fn run_one_shot(cmdline: &HashMap<String, String>) -> Result<(), String> {
		let encoded = cmdline
			.get("vmon.entry")
			.ok_or_else(|| "vmon.entry missing".to_string())?;
		let bytes = decode_base64(encoded)?;
		let entry: Entry =
			serde_json::from_slice(&bytes).map_err(|err| format!("entry json: {err}"))?;
		if entry.cmd.is_empty() {
			return Err("entry cmd must not be empty".to_string());
		}

		let console = OpenOptions::new()
			.read(true)
			.write(true)
			.open(CONSOLE)
			.map_err(|err| format!("open {CONSOLE}: {err}"))?;

		let mut command = Command::new(&entry.cmd[0]);
		command.args(&entry.cmd[1..]);
		if let Some(cwd) = entry.cwd {
			command.current_dir(cwd);
		}
		command.env_clear();
		if let Some(env) = entry.env {
			command.envs(env);
		}
		command
			.stdin(Stdio::from(
				console
					.try_clone()
					.map_err(|err| format!("clone console stdin: {err}"))?,
			))
			.stdout(Stdio::from(
				console
					.try_clone()
					.map_err(|err| format!("clone console stdout: {err}"))?,
			))
			.stderr(Stdio::from(console));

		let mut child = command
			.spawn()
			.map_err(|err| format!("spawn entry: {err}"))?;
		let _ = child.wait().map_err(|err| format!("wait entry: {err}"))?;
		reboot_guest()
	}

	fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
		let mut out = Vec::with_capacity(input.len() * 3 / 4);
		let mut buffer = 0u32;
		let mut bits = 0u8;

		for byte in input.bytes() {
			let value = match byte {
				b'A'..=b'Z' => byte - b'A',
				b'a'..=b'z' => byte - b'a' + 26,
				b'0'..=b'9' => byte - b'0' + 52,
				b'+' | b'-' => 62,
				b'/' | b'_' => 63,
				b'=' => break,
				b'\n' | b'\r' | b'\t' | b' ' => continue,
				_ => return Err(format!("invalid base64 byte 0x{byte:02x}")),
			} as u32;
			buffer = (buffer << 6) | value;
			bits += 6;
			if bits >= 8 {
				bits -= 8;
				out.push(((buffer >> bits) & 0xff) as u8);
			}
		}
		Ok(out)
	}

	fn reboot_guest() -> Result<(), String> {
		// SAFETY: `sync` and `reboot` take no Rust pointers; RB_AUTOBOOT asks
		// the kernel to restart the guest after pending writes are flushed.
		unsafe {
			libc::sync();
			if libc::reboot(libc::RB_AUTOBOOT) == 0 {
				Ok(())
			} else {
				Err(format!("reboot: {}", io::Error::last_os_error()))
			}
		}
	}

	fn apply_net_config(
		iface: &str,
		ip: &str,
		prefix: u8,
		gw: &str,
		dns: &[String],
	) -> Result<(), String> {
		let ip = Ipv4Addr::from_str(ip).map_err(|err| format!("ip: {err}"))?;
		let gw = Ipv4Addr::from_str(gw).map_err(|err| format!("gw: {err}"))?;
		let ifname = CString::new(iface).map_err(|err| err.to_string())?;
		// SAFETY: `ifname` is a nul-terminated string that lives across the call.
		let ifindex = unsafe { libc::if_nametoindex(ifname.as_ptr()) };
		if ifindex == 0 {
			return Err(format!("if_nametoindex {iface}: {}", io::Error::last_os_error()));
		}

		set_link_up(ifindex)?;
		add_ipv4_addr(ifindex, ip, prefix)?;
		add_default_route(ifindex, gw)?;
		write_resolv_conf(dns)?;
		Ok(())
	}

	fn write_resolv_conf(dns: &[String]) -> Result<(), String> {
		if dns.is_empty() {
			return Ok(());
		}
		let mut data = String::new();
		for server in dns {
			data.push_str("nameserver ");
			data.push_str(server);
			data.push('\n');
		}
		fs::write("/etc/resolv.conf", data).map_err(|err| format!("write /etc/resolv.conf: {err}"))
	}

	fn set_link_up(ifindex: u32) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = IfInfoMsg {
			ifi_family: libc::AF_UNSPEC as u8,
			ifi_pad:    0,
			ifi_type:   0,
			ifi_index:  ifindex as i32,
			ifi_flags:  libc::IFF_UP as u32,
			ifi_change: libc::IFF_UP as u32,
		};
		push_struct(&mut payload, &msg);
		netlink_request(libc::RTM_NEWLINK, (libc::NLM_F_REQUEST | libc::NLM_F_ACK) as u16, &payload)
			.map_err(|err| format!("link up: {err}"))
	}

	fn add_ipv4_addr(ifindex: u32, ip: Ipv4Addr, prefix: u8) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = IfAddrMsg {
			ifa_family:    libc::AF_INET as u8,
			ifa_prefixlen: prefix,
			ifa_flags:     0,
			ifa_scope:     libc::RT_SCOPE_UNIVERSE,
			ifa_index:     ifindex,
		};
		push_struct(&mut payload, &msg);
		push_attr(&mut payload, libc::IFA_LOCAL, &ip.octets());
		push_attr(&mut payload, libc::IFA_ADDRESS, &ip.octets());
		netlink_request(
			libc::RTM_NEWADDR,
			(libc::NLM_F_REQUEST | libc::NLM_F_ACK | libc::NLM_F_CREATE | libc::NLM_F_REPLACE) as u16,
			&payload,
		)
		.map_err(|err| format!("addr add: {err}"))
	}

	fn add_default_route(ifindex: u32, gw: Ipv4Addr) -> Result<(), String> {
		let mut payload = Vec::new();
		let msg = RtMsg {
			rtm_family:   libc::AF_INET as u8,
			rtm_dst_len:  0,
			rtm_src_len:  0,
			rtm_tos:      0,
			rtm_table:    libc::RT_TABLE_MAIN,
			rtm_protocol: libc::RTPROT_BOOT,
			rtm_scope:    libc::RT_SCOPE_UNIVERSE,
			rtm_type:     libc::RTN_UNICAST,
			rtm_flags:    0,
		};
		push_struct(&mut payload, &msg);
		push_attr(&mut payload, libc::RTA_GATEWAY, &gw.octets());
		push_attr(&mut payload, libc::RTA_OIF, &ifindex.to_ne_bytes());
		match netlink_request(
			libc::RTM_NEWROUTE,
			(libc::NLM_F_REQUEST | libc::NLM_F_ACK | libc::NLM_F_CREATE | libc::NLM_F_REPLACE) as u16,
			&payload,
		) {
			Ok(()) => Ok(()),
			Err(err) if err.raw_os_error() == Some(libc::EEXIST) => Ok(()),
			Err(err) => Err(format!("route add: {err}")),
		}
	}

	fn netlink_request(message_type: u16, flags: u16, payload: &[u8]) -> io::Result<()> {
		// SAFETY: `socket` takes integer arguments only and returns a new fd on
		// success, which this function closes before returning.
		let fd = unsafe {
			libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_ROUTE)
		};
		if fd < 0 {
			return Err(io::Error::last_os_error());
		}

		let result = (|| {
			let mut request =
				Vec::with_capacity(std::mem::size_of::<libc::nlmsghdr>() + payload.len());
			let header = libc::nlmsghdr {
				nlmsg_len:   (std::mem::size_of::<libc::nlmsghdr>() + payload.len()) as u32,
				nlmsg_type:  message_type,
				nlmsg_flags: flags,
				nlmsg_seq:   1,
				nlmsg_pid:   0,
			};
			push_struct(&mut request, &header);
			request.extend_from_slice(payload);

			// SAFETY: zero is a valid sockaddr_nl baseline; fields are filled below.
			let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
			addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
			addr.nl_pid = 0;
			addr.nl_groups = 0;
			// SAFETY: `fd` is an open netlink socket; request and addr point to
			// initialized buffers that live for the duration of the syscall.
			let sent = unsafe {
				libc::sendto(
					fd,
					request.as_ptr().cast(),
					request.len(),
					0,
					(&addr as *const libc::sockaddr_nl).cast(),
					std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
				)
			};
			if sent < 0 {
				return Err(io::Error::last_os_error());
			}

			let mut ack = [0u8; 4096];
			// SAFETY: `ack` is a valid writable buffer for `recv`.
			let len = unsafe { libc::recv(fd, ack.as_mut_ptr().cast(), ack.len(), 0) };
			if len < 0 {
				return Err(io::Error::last_os_error());
			}
			parse_netlink_ack(&ack[..len as usize])
		})();

		// SAFETY: `fd` is owned by this function and is closed exactly once here.
		let close_result = unsafe { libc::close(fd) };
		if result.is_ok() && close_result < 0 {
			Err(io::Error::last_os_error())
		} else {
			result
		}
	}

	fn parse_netlink_ack(buf: &[u8]) -> io::Result<()> {
		let header_len = std::mem::size_of::<libc::nlmsghdr>();
		if buf.len() < header_len {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short netlink ack"));
		}
		// SAFETY: the length check above guarantees a complete header; unaligned
		// reads are required because the header starts in a byte slice.
		let header = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast::<libc::nlmsghdr>()) };
		if header.nlmsg_type != libc::NLMSG_ERROR as u16 {
			return Ok(());
		}
		if buf.len() < header_len + std::mem::size_of::<libc::nlmsgerr>() {
			return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short netlink error"));
		}
		// SAFETY: the length check above guarantees a complete nlmsgerr; unaligned
		// reads are required because the payload starts in a byte slice.
		let err =
			unsafe { std::ptr::read_unaligned(buf[header_len..].as_ptr().cast::<libc::nlmsgerr>()) };
		if err.error == 0 {
			Ok(())
		} else {
			Err(io::Error::from_raw_os_error(-err.error))
		}
	}

	fn push_struct<T>(buf: &mut Vec<u8>, value: &T) {
		// SAFETY: callers pass fully initialized repr(C)/libc netlink structs
		// whose byte representation is sent directly to the kernel.
		let bytes = unsafe {
			std::slice::from_raw_parts((value as *const T).cast::<u8>(), std::mem::size_of::<T>())
		};
		buf.extend_from_slice(bytes);
	}

	fn push_attr(buf: &mut Vec<u8>, attr_type: u16, data: &[u8]) {
		let len = std::mem::size_of::<RtAttr>() + data.len();
		let attr = RtAttr { rta_len: len as u16, rta_type: attr_type };
		push_struct(buf, &attr);
		buf.extend_from_slice(data);
		while !buf.len().is_multiple_of(4) {
			buf.push(0);
		}
	}

	#[cfg(test)]
	mod tests {
		use std::{
			os::{fd::OwnedFd, unix::net::UnixStream},
			sync::{Barrier, TryLockError},
		};

		use super::*;

		fn pty_agent(exit_code: Option<i32>) -> (Arc<Agent>, PtySessions, UnixStream) {
			let (writer, reader) = UnixStream::pair().unwrap();
			let writer = File::from(OwnedFd::from(writer));
			let sessions = Arc::new(Mutex::new(HashMap::new()));
			let mut scrollback = proto::Scrollback::new(proto::PTY_SCROLLBACK_LIMIT);
			scrollback.push(b"replay");
			lock(&sessions).insert("session".to_owned(), PtySessionState {
				pid: 1,
				master: File::open("/dev/null").unwrap(),
				cols: 80,
				rows: 24,
				exec: None,
				created_at: 1,
				binding: None,
				scrollback,
				exit: exit_code.map(|code| (code, SystemTime::now())),
			});
			let agent = Arc::new(Agent {
				writer:         Arc::new(Mutex::new(writer)),
				sessions:       Arc::new(Mutex::new(HashMap::new())),
				pty_sessions:   Arc::clone(&sessions),
				pending_writes: Arc::new(Mutex::new(HashMap::new())),
				epoch:          Arc::new(Mutex::new(7)),
			});
			(agent, sessions, reader)
		}

		#[test]
		fn pty_attach_replays_before_retained_exit() {
			let (agent, sessions, mut reader) = pty_agent(Some(23));

			agent.pty_attach(41, &json!({ "session_id": "session" }));

			let meta = proto::read_frame(&mut reader).unwrap().unwrap();
			let replay = proto::read_frame(&mut reader).unwrap().unwrap();
			let exit = proto::read_frame(&mut reader).unwrap().unwrap();
			assert_eq!(meta.ty, proto::FRAME_RESP);
			assert_eq!(replay, Frame {
				ty:      proto::FRAME_STDOUT,
				id:      41,
				payload: b"replay".to_vec(),
			});
			assert_eq!(exit.ty, proto::FRAME_EXIT);
			assert_eq!(serde_json::from_slice::<Value>(&exit.payload).unwrap(), json!({ "code": 23 }));
			assert!(lock(&sessions)["session"].binding.is_none());
		}

		#[test]
		fn pty_attach_blocks_live_output_until_replay_is_published() {
			let (agent, sessions, mut reader) = pty_agent(None);
			let writer_guard = lock(&agent.writer);
			let barrier = Arc::new(Barrier::new(2));
			let attaching = {
				let agent = Arc::clone(&agent);
				let barrier = Arc::clone(&barrier);
				thread::spawn(move || {
					barrier.wait();
					agent.pty_attach(42, &json!({ "session_id": "session" }));
				})
			};
			barrier.wait();
			thread::sleep(Duration::from_millis(20));
			assert!(matches!(sessions.try_lock(), Err(TryLockError::WouldBlock)));

			let publishing = {
				let sessions = Arc::clone(&sessions);
				let writer = Arc::clone(&agent.writer);
				let epoch = Arc::clone(&agent.epoch);
				thread::spawn(move || {
					assert!(publish_pty_output(&sessions, &writer, &epoch, "session", b"live"));
				})
			};
			drop(writer_guard);
			attaching.join().unwrap();
			publishing.join().unwrap();

			let meta = proto::read_frame(&mut reader).unwrap().unwrap();
			let replay = proto::read_frame(&mut reader).unwrap().unwrap();
			let live = proto::read_frame(&mut reader).unwrap().unwrap();
			assert_eq!(meta.ty, proto::FRAME_RESP);
			assert_eq!(replay, Frame {
				ty:      proto::FRAME_STDOUT,
				id:      42,
				payload: b"replay".to_vec(),
			});
			assert_eq!(live, Frame {
				ty:      proto::FRAME_STDOUT,
				id:      42,
				payload: b"live".to_vec(),
			});
		}
	}
}
