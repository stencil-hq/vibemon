//! `process.Process` service: spawning (pipes or PTY, user switch, env,
//! cwd, cgroup, lifetime deadline), the process registry with 30 s retention
//! of exited processes for late `Connect`, output fan-out to attached
//! streams, input/resize/signal/close-stdin, and the Start/Connect stream
//! loops with keepalives.

use std::{
	collections::HashMap,
	io,
	os::{
		fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
		unix::process::ExitStatusExt,
	},
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt, unix::AsyncFd},
	process::ChildStdin,
	sync::{mpsc, watch},
};

use super::{
	cgroups::Kind as CgroupKind,
	connect::{Code, RpcError, StreamSink},
	paths,
	pb::process::{
		self as pb, ProcessConfig, ProcessEvent, ProcessInfo, process_event as ev,
		process_input::Input, process_selector::Selector,
	},
	state::EnvdState,
	users::User,
};

/// How long an exited process stays connectable.
pub const EXIT_RETENTION: Duration = Duration::from_secs(30);
/// Read chunk for pipes (envd: ≤ 32 KiB).
const PIPE_CHUNK: usize = 32 << 10;
/// Read chunk for PTYs (envd: ≤ 16 KiB).
const PTY_CHUNK: usize = 16 << 10;
/// After the child exits, keep draining output until it is idle this long
/// (catches buffered output; stops waiting for background grandchildren
/// that inherited the pipes).
const DRAIN_IDLE: Duration = Duration::from_millis(20);
/// Per-subscriber event queue depth (backpressure on the process beyond it).
const SUBSCRIBER_DEPTH: usize = 64;
/// Tag whose processes run in the `system` cgroup.
const SYSTEM_TAG: &str = "_system";

/// One event fanned out to attached streams.
#[derive(Clone, Debug)]
pub enum Event {
	Data(ev::data_event::Output),
	End(ev::EndEvent),
}

enum Io {
	Pty(Arc<AsyncFd<OwnedFd>>),
	Pipes(tokio::sync::Mutex<Option<ChildStdin>>),
}

struct Fanout {
	subscribers: Vec<mpsc::Sender<Event>>,
	end:         Option<ev::EndEvent>,
}

/// A tracked process.
pub struct Proc {
	pub pid:    u32,
	pub tag:    Option<String>,
	pub config: ProcessConfig,
	io:         Io,
	pidfd:      Option<OwnedFd>,
	fanout:     Mutex<Fanout>,
}

/// Result of attaching to a process.
pub enum Subscription {
	Live(mpsc::Receiver<Event>),
	Ended(ev::EndEvent),
}

impl Proc {
	/// Attach a new listener (or get the retained end event).
	pub fn subscribe(&self) -> Subscription {
		let mut f = self.fanout.lock();
		if let Some(end) = &f.end {
			return Subscription::Ended(end.clone());
		}
		let (tx, rx) = mpsc::channel(SUBSCRIBER_DEPTH);
		f.subscribers.push(tx);
		Subscription::Live(rx)
	}

	async fn publish(&self, event: Event) {
		let subs: Vec<_> = {
			let mut f = self.fanout.lock();
			f.subscribers.retain(|s| !s.is_closed());
			f.subscribers.clone()
		};
		for s in subs {
			let _ = s.send(event.clone()).await;
		}
	}

	async fn finish(&self, end: ev::EndEvent) {
		let subs = {
			let mut f = self.fanout.lock();
			f.end = Some(end.clone());
			std::mem::take(&mut f.subscribers)
		};
		for s in subs {
			let _ = s.send(Event::End(end.clone())).await;
		}
	}

	fn has_ended(&self) -> bool {
		self.fanout.lock().end.is_some()
	}

	/// Deliver `sig` to the process itself (not its group), via pidfd so a
	/// recycled pid is never hit.
	pub fn signal(&self, sig: libc::c_int) -> io::Result<()> {
		if self.has_ended() {
			return Err(io::Error::from_raw_os_error(libc::ESRCH));
		}
		let rc = match &self.pidfd {
			// SAFETY: raw syscall with a valid pidfd and null siginfo.
			Some(fd) => unsafe {
				libc::syscall(
					libc::SYS_pidfd_send_signal,
					fd.as_raw_fd(),
					sig,
					std::ptr::null::<libc::siginfo_t>(),
					0,
				)
			},
			// SAFETY: plain kill(2).
			None => libc::c_long::from(unsafe { libc::kill(self.pid.cast_signed(), sig) }),
		};
		if rc == 0 {
			Ok(())
		} else {
			Err(io::Error::last_os_error())
		}
	}

	/// Write to stdin (pipe processes).
	pub async fn write_stdin(&self, data: &[u8]) -> Result<(), RpcError> {
		match &self.io {
			Io::Pipes(stdin) => {
				let mut guard = stdin.lock().await;
				let pipe = guard
					.as_mut()
					.ok_or_else(|| RpcError::internal("stdin is not enabled or was closed"))?;
				pipe
					.write_all(data)
					.await
					.map_err(|e| RpcError::internal(format!("error writing to stdin: {e}")))
			},
			Io::Pty(_) => Err(RpcError::internal("process has a pty; send pty input instead")),
		}
	}

	/// Write to the PTY master (PTY processes).
	pub async fn write_pty(&self, data: &[u8]) -> Result<(), RpcError> {
		let Io::Pty(master) = &self.io else {
			return Err(RpcError::internal("process does not have a pty"));
		};
		let mut rest = data;
		while !rest.is_empty() {
			let mut guard = master
				.writable()
				.await
				.map_err(|e| RpcError::internal(format!("error writing to pty: {e}")))?;
			match guard.try_io(|fd| {
				// SAFETY: writing from a valid slice to an owned fd.
				let n = unsafe { libc::write(fd.as_raw_fd(), rest.as_ptr().cast(), rest.len()) };
				if n < 0 {
					Err(io::Error::last_os_error())
				} else {
					Ok(n.cast_unsigned())
				}
			}) {
				Ok(Ok(n)) => rest = &rest[n..],
				Ok(Err(e)) => return Err(RpcError::internal(format!("error writing to pty: {e}"))),
				Err(_would_block) => {},
			}
		}
		Ok(())
	}

	/// Close stdin (EOF). PTYs have no separate stdin.
	pub async fn close_stdin(&self) -> Result<(), RpcError> {
		match &self.io {
			Io::Pipes(stdin) => {
				let pipe = stdin.lock().await.take();
				if let Some(mut pipe) = pipe {
					let _ = pipe.shutdown().await;
				}
				Ok(())
			},
			Io::Pty(_) => Err(RpcError::new(
				Code::Unknown,
				"cannot close stdin for a pty process; send Ctrl+D (0x04) instead",
			)),
		}
	}

	/// Resize the PTY.
	pub fn resize(&self, cols: u32, rows: u32) -> Result<(), RpcError> {
		let Io::Pty(master) = &self.io else {
			return Err(RpcError::internal("process does not have a pty"));
		};
		set_winsize(master.get_ref().as_raw_fd(), cols, rows)
			.map_err(|e| RpcError::internal(format!("error resizing tty: {e}")))
	}

	fn info(&self) -> ProcessInfo {
		ProcessInfo { config: Some(self.config.clone()), pid: self.pid, tag: self.tag.clone() }
	}
}

#[derive(Default)]
struct Tables {
	live:   HashMap<u32, Arc<Proc>>,
	exited: Vec<(Instant, Arc<Proc>)>,
}

/// Live and recently exited processes.
#[derive(Default)]
pub struct Registry {
	tables: Mutex<Tables>,
}

impl Registry {
	/// Live processes (envd `List`).
	pub fn list(&self) -> Vec<ProcessInfo> {
		let t = self.tables.lock();
		let mut out: Vec<_> = t.live.values().map(|p| p.info()).collect();
		out.sort_by_key(|p| p.pid);
		out
	}

	/// Find a live process by selector.
	pub fn find_live(&self, selector: Option<&pb::ProcessSelector>) -> Result<Arc<Proc>, RpcError> {
		let t = self.tables.lock();
		find_in(t.live.values(), selector)
	}

	/// Find a live or recently exited process (for `Connect`).
	pub fn find_connectable(
		&self,
		selector: Option<&pb::ProcessSelector>,
	) -> Result<Arc<Proc>, RpcError> {
		let mut t = self.tables.lock();
		t.exited.retain(|(at, _)| at.elapsed() < EXIT_RETENTION);
		find_in(t.live.values(), selector)
			.or_else(|_| find_in(t.exited.iter().map(|(_, p)| p), selector))
	}

	fn insert(&self, proc: Arc<Proc>) {
		self.tables.lock().live.insert(proc.pid, proc);
	}

	fn retire(&self, pid: u32) {
		let mut t = self.tables.lock();
		t.exited.retain(|(at, _)| at.elapsed() < EXIT_RETENTION);
		if let Some(p) = t.live.remove(&pid) {
			t.exited.push((Instant::now(), p));
		}
	}
}

fn find_in<'a>(
	mut procs: impl Iterator<Item = &'a Arc<Proc>>,
	selector: Option<&pb::ProcessSelector>,
) -> Result<Arc<Proc>, RpcError> {
	match selector.and_then(|s| s.selector.as_ref()) {
		Some(Selector::Pid(pid)) => procs
			.find(|p| p.pid == *pid)
			.cloned()
			.ok_or_else(|| RpcError::not_found(format!("process with pid {pid} not found"))),
		Some(Selector::Tag(tag)) => procs
			.find(|p| p.tag.as_deref() == Some(tag.as_str()))
			.cloned()
			.ok_or_else(|| RpcError::not_found(format!("process with tag {tag} not found"))),
		None => Err(RpcError::invalid("process selector (pid or tag) is required")),
	}
}

/// Go `syscall.Signal.String()` names (what envd's `status` strings use).
pub fn signal_name(sig: i32) -> String {
	let name = match sig {
		1 => "hangup",
		2 => "interrupt",
		3 => "quit",
		4 => "illegal instruction",
		5 => "trace/breakpoint trap",
		6 => "aborted",
		7 => "bus error",
		8 => "floating point exception",
		9 => "killed",
		10 => "user defined signal 1",
		11 => "segmentation fault",
		12 => "user defined signal 2",
		13 => "broken pipe",
		14 => "alarm clock",
		15 => "terminated",
		16 => "stack fault",
		17 => "child exited",
		18 => "continued",
		19 => "stopped (signal)",
		20 => "stopped",
		21 => "stopped (tty input)",
		22 => "stopped (tty output)",
		23 => "urgent I/O condition",
		24 => "CPU time limit exceeded",
		25 => "file size limit exceeded",
		26 => "virtual timer expired",
		27 => "profiling timer expired",
		28 => "window changed",
		29 => "I/O possible",
		30 => "power failure",
		31 => "bad system call",
		n => return format!("signal {n}"),
	};
	name.to_owned()
}

/// envd `end` event for an exit status (Go `ProcessState` semantics:
/// signalled → `exited=false`, `exit_code=-1`, `status="signal: killed"`;
/// `error` = status text for any unsuccessful exit).
pub fn end_event(status: std::process::ExitStatus) -> ev::EndEvent {
	let (exited, exit_code, text) = match (status.code(), status.signal()) {
		(Some(code), _) => (true, code, format!("exit status {code}")),
		(None, Some(sig)) => {
			let core = if status.core_dumped() {
				" (core dumped)"
			} else {
				""
			};
			(false, -1, format!("signal: {}{core}", signal_name(sig)))
		},
		(None, None) => (false, -1, "unknown exit status".to_owned()),
	};
	let error = (!status.success()).then(|| text.clone());
	ev::EndEvent { exit_code, exited, status: text, error }
}

/// Upstream envd `userCommand()`: cmd and args joined by spaces.
fn user_command(config: &ProcessConfig) -> String {
	std::iter::once(config.cmd.as_str())
		.chain(config.args.iter().map(String::as_str))
		.collect::<Vec<_>>()
		.join(" ")
}

/// The `process_start` sandbox log line (upstream envd `process` logger).
fn start_log(pid: u32, config: &ProcessConfig) -> (String, serde_json::Value) {
	(
		format!("Process with pid {pid} started"),
		serde_json::json!({
			"logger": "process",
			"event_type": "process_start",
			"pid": pid,
			"command": user_command(config),
		}),
	)
}

/// The `process_end` sandbox log line (upstream envd `process` logger).
fn end_log(pid: u32, end: &ev::EndEvent) -> (String, serde_json::Value) {
	(
		format!("Process with pid {pid} ended"),
		serde_json::json!({
			"logger": "process",
			"event_type": "process_end",
			"pid": pid,
			"process_result": {
				"exit_code": end.exit_code,
				"exited": end.exited,
				"status": end.status,
				"error": end.error,
			},
		}),
	)
}

/// Everything `spawn` needs besides the request.
pub struct SpawnContext<'a> {
	pub user:     &'a User,
	/// Process lifetime (`Connect-Timeout-Ms` on `Start`).
	pub lifetime: Option<Duration>,
}

/// Validate, build, and start a process; registers it and starts its I/O
/// pumps and reaper task. Returns the process and a live subscription taken
/// before any output could be produced.
pub fn spawn(
	state: &Arc<EnvdState>,
	req: pb::StartRequest,
	ctx: &SpawnContext<'_>,
) -> Result<(Arc<Proc>, mpsc::Receiver<Event>), RpcError> {
	let config = req
		.process
		.ok_or_else(|| RpcError::invalid("process config is required"))?;
	if config.cmd.is_empty() {
		return Err(RpcError::invalid("process cmd is required"));
	}
	let user = ctx.user;
	let workdir = state.workdir();
	let cwd = match config.cwd.as_deref() {
		Some(cwd) => {
			let p = paths::resolve(cwd, &user.home, workdir.as_deref()).map_err(RpcError::invalid)?;
			if !p.is_dir() {
				return Err(RpcError::invalid(format!("cwd '{}' does not exist", p.display())));
			}
			p
		},
		None => default_cwd(user, workdir.as_deref()),
	};

	let mut env: Vec<(String, String)> = vec![
		("PATH".into(), state.path_env.clone()),
		("HOME".into(), user.home.to_string_lossy().into_owned()),
		("USER".into(), user.name.clone()),
		("LOGNAME".into(), user.name.clone()),
		("SHELL".into(), user.shell.clone()),
	];
	env.extend(state.default_env());
	env.extend(config.envs.iter().map(|(k, v)| (k.clone(), v.clone())));

	let mut cmd = tokio::process::Command::new(&config.cmd);
	cmd.args(&config.args)
		.env_clear()
		.current_dir(&cwd)
		.kill_on_drop(false);
	for (k, v) in &env {
		cmd.env(k, v);
	}

	let pty = req.pty.as_ref().map(|p| p.size.unwrap_or_default());
	let master = if let Some(size) = pty {
		let (m, slave) = open_pty(size.cols, size.rows)
			.map_err(|e| RpcError::internal(format!("error creating pty: {e}")))?;
		let dup = |fd: &OwnedFd| {
			fd.try_clone()
				.map_err(|e| RpcError::internal(format!("error creating pty: {e}")))
		};
		cmd.stdin(dup(&slave)?).stdout(dup(&slave)?).stderr(slave);
		Some(m)
	} else {
		let stdin = if req.stdin.unwrap_or(true) {
			std::process::Stdio::piped()
		} else {
			std::process::Stdio::null()
		};
		cmd.stdin(stdin)
			.stdout(std::process::Stdio::piped())
			.stderr(std::process::Stdio::piped());
		None
	};

	let kind = if req.tag.as_deref() == Some(SYSTEM_TAG) {
		CgroupKind::System
	} else if pty.is_some() {
		CgroupKind::Pty
	} else {
		CgroupKind::User
	};
	let creds = Credentials::for_user(user);
	let child_setup =
		ChildSetup { pty: pty.is_some(), cgroup_procs: state.cgroups.procs_fd(kind), creds };
	// SAFETY: the hook only performs async-signal-safe syscalls on data
	// prepared before fork (no allocation, no locks).
	unsafe {
		cmd.pre_exec(move || child_setup.apply());
	}

	let guard = crate::reaper::spawn_guard();
	let mut child = cmd.spawn().map_err(|e| {
		let code = match e.kind() {
			io::ErrorKind::NotFound => Code::NotFound,
			io::ErrorKind::PermissionDenied => Code::PermissionDenied,
			_ => Code::Internal,
		};
		RpcError::new(code, format!("error starting process '{}': {e}", config.cmd))
	})?;
	let Some(pid) = child.id() else {
		return Err(RpcError::internal("process exited before it could be tracked"));
	};
	guard.track(pid.cast_signed());
	drop(guard);
	drop(cmd); // closes the parent's copies of the PTY slave

	let pidfd = pidfd_open(pid);
	let io = match &master {
		Some(m) => Io::Pty(m.clone()),
		None => Io::Pipes(tokio::sync::Mutex::new(child.stdin.take())),
	};
	let proc = Arc::new(Proc {
		pid,
		tag: req.tag,
		config,
		io,
		pidfd,
		fanout: Mutex::new(Fanout { subscribers: Vec::new(), end: None }),
	});
	let Subscription::Live(rx) = proc.subscribe() else {
		unreachable!("a just-spawned process has no end event");
	};
	state.processes.insert(proc.clone());
	let (msg, fields) = start_log(pid, &proc.config);
	crate::logging::emit_queued(crate::logging::Level::Info, &msg, fields);

	let (exited_tx, exited_rx) = watch::channel(false);
	let mut pumps = Vec::new();
	if let Some(m) = master {
		pumps.push(tokio::spawn(pump_pty(m, proc.clone(), exited_rx)));
	} else {
		if let Some(out) = child.stdout.take() {
			pumps.push(tokio::spawn(pump_pipe(out, Stream::Stdout, proc.clone(), exited_rx.clone())));
		}
		if let Some(err) = child.stderr.take() {
			pumps.push(tokio::spawn(pump_pipe(err, Stream::Stderr, proc.clone(), exited_rx)));
		}
	}

	if let Some(lifetime) = ctx.lifetime {
		let p = proc.clone();
		tokio::spawn(async move {
			tokio::time::sleep(lifetime).await;
			let _ = p.signal(libc::SIGKILL);
		});
	}

	let st = state.clone();
	let p = proc.clone();
	tokio::spawn(async move {
		let status = child.wait().await;
		crate::reaper::untrack(pid.cast_signed());
		let _ = exited_tx.send(true);
		for pump in pumps {
			let _ = pump.await;
		}
		let end = match status {
			Ok(status) => end_event(status),
			Err(e) => ev::EndEvent {
				exit_code: -1,
				exited:    false,
				status:    "unknown".into(),
				error:     Some(format!("wait: {e}")),
			},
		};
		let (msg, fields) = end_log(pid, &end);
		crate::logging::emit_queued(crate::logging::Level::Info, &msg, fields);
		p.finish(end).await;
		st.processes.retire(pid);
	});
	Ok((proc, rx))
}

fn default_cwd(user: &User, workdir: Option<&str>) -> PathBuf {
	if let Some(w) = workdir
		&& let Ok(p) = paths::resolve(w, &user.home, None)
		&& p.is_dir()
	{
		return p;
	}
	if user.home.is_dir() {
		user.home.clone()
	} else {
		PathBuf::from("/")
	}
}

#[derive(Clone, Copy)]
enum Stream {
	Stdout,
	Stderr,
}

async fn pump_pipe(
	mut reader: impl AsyncRead + Unpin,
	which: Stream,
	proc: Arc<Proc>,
	mut exited: watch::Receiver<bool>,
) {
	let mut buf = vec![0u8; PIPE_CHUNK];
	let mut draining = *exited.borrow();
	loop {
		let read = if draining {
			match tokio::time::timeout(DRAIN_IDLE, reader.read(&mut buf)).await {
				Ok(r) => r,
				Err(_idle) => return,
			}
		} else {
			tokio::select! {
				r = reader.read(&mut buf) => r,
				_ = exited.changed() => { draining = true; continue; }
			}
		};
		match read {
			Ok(0) | Err(_) => return,
			Ok(n) => {
				let data = buf[..n].to_vec();
				let out = match which {
					Stream::Stdout => ev::data_event::Output::Stdout(data),
					Stream::Stderr => ev::data_event::Output::Stderr(data),
				};
				proc.publish(Event::Data(out)).await;
			},
		}
	}
}

async fn read_pty(master: &AsyncFd<OwnedFd>, buf: &mut [u8]) -> io::Result<usize> {
	loop {
		let mut guard = master.readable().await?;
		match guard.try_io(|fd| {
			// SAFETY: reading into a valid buffer from an owned fd.
			let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
			if n < 0 {
				Err(io::Error::last_os_error())
			} else {
				Ok(n.cast_unsigned())
			}
		}) {
			Ok(r) => return r,
			Err(_would_block) => {},
		}
	}
}

async fn pump_pty(
	master: Arc<AsyncFd<OwnedFd>>,
	proc: Arc<Proc>,
	mut exited: watch::Receiver<bool>,
) {
	let mut buf = vec![0u8; PTY_CHUNK];
	let mut draining = *exited.borrow();
	loop {
		let read = if draining {
			match tokio::time::timeout(DRAIN_IDLE, read_pty(&master, &mut buf)).await {
				Ok(r) => r,
				Err(_idle) => return,
			}
		} else {
			tokio::select! {
				r = read_pty(&master, &mut buf) => r,
				_ = exited.changed() => { draining = true; continue; }
			}
		};
		// EIO = every slave fd closed (the PTY equivalent of EOF).
		match read {
			Ok(0) | Err(_) => return,
			Ok(n) => {
				proc
					.publish(Event::Data(ev::data_event::Output::Pty(buf[..n].to_vec())))
					.await;
			},
		}
	}
}

/// Open a PTY pair (both ends close-on-exec), sized `cols`×`rows`
/// (0 → 80×24); the master is non-blocking and registered with tokio.
fn open_pty(cols: u32, rows: u32) -> io::Result<(Arc<AsyncFd<OwnedFd>>, OwnedFd)> {
	// SAFETY: posix_openpt returns a new fd or -1.
	let master = unsafe {
		libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC | libc::O_NONBLOCK)
	};
	if master < 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: master is a freshly opened fd we own.
	let master = unsafe { OwnedFd::from_raw_fd(master) };
	// SAFETY: grantpt/unlockpt on a valid pty master fd.
	if unsafe { libc::grantpt(master.as_raw_fd()) != 0 || libc::unlockpt(master.as_raw_fd()) != 0 } {
		return Err(io::Error::last_os_error());
	}
	let mut name = [0 as libc::c_char; 128];
	// SAFETY: name is a valid buffer of the given length.
	let rc = unsafe { libc::ptsname_r(master.as_raw_fd(), name.as_mut_ptr(), name.len()) };
	if rc != 0 {
		return Err(io::Error::from_raw_os_error(rc));
	}
	// SAFETY: ptsname_r NUL-terminates on success.
	let slave =
		unsafe { libc::open(name.as_ptr(), libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
	if slave < 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: slave is a freshly opened fd we own.
	let slave = unsafe { OwnedFd::from_raw_fd(slave) };
	let (cols, rows) = if cols == 0 || rows == 0 {
		(80, 24)
	} else {
		(cols, rows)
	};
	set_winsize(master.as_raw_fd(), cols, rows)?;
	Ok((Arc::new(AsyncFd::new(master)?), slave))
}

fn set_winsize(fd: RawFd, cols: u32, rows: u32) -> io::Result<()> {
	let ws = libc::winsize {
		ws_row:    u16::try_from(rows).unwrap_or(u16::MAX),
		ws_col:    u16::try_from(cols).unwrap_or(u16::MAX),
		ws_xpixel: 0,
		ws_ypixel: 0,
	};
	// SAFETY: TIOCSWINSZ reads a winsize from a valid pointer.
	if unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws) } == 0 {
		Ok(())
	} else {
		Err(io::Error::last_os_error())
	}
}

fn pidfd_open(pid: u32) -> Option<OwnedFd> {
	// SAFETY: raw pidfd_open(2); returns a new fd or -1.
	let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
	let fd = RawFd::try_from(fd).ok().filter(|&fd| fd >= 0)?;
	// SAFETY: a non-negative return is a new fd we own.
	Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Credentials to switch to in the child, prepared before fork.
struct Credentials {
	uid:    libc::uid_t,
	gid:    libc::gid_t,
	groups: Vec<libc::gid_t>,
}

impl Credentials {
	fn for_user(user: &User) -> Self {
		Self { uid: user.uid, gid: user.gid, groups: user.groups.clone() }
	}
}

struct ChildSetup {
	pty:          bool,
	cgroup_procs: Option<RawFd>,
	creds:        Credentials,
}

const OOM_SCORE_ADJ: &std::ffi::CStr = c"/proc/self/oom_score_adj";

impl ChildSetup {
	/// Runs in the forked child before exec: only async-signal-safe calls.
	fn apply(&self) -> io::Result<()> {
		// SAFETY: plain syscalls on fds/buffers owned by this child.
		unsafe {
			if self.pty {
				if libc::setsid() < 0 {
					return Err(io::Error::last_os_error());
				}
				if libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
					return Err(io::Error::last_os_error());
				}
			}
			// envd makes user processes the preferred OOM victims.
			let fd = libc::open(OOM_SCORE_ADJ.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
			if fd >= 0 {
				libc::write(fd, b"100".as_ptr().cast(), 3);
				libc::close(fd);
			}
			if let Some(procs) = self.cgroup_procs {
				libc::write(procs, b"0".as_ptr().cast(), 1);
			}
			let c = &self.creds;
			if libc::geteuid() == 0 {
				if libc::setgroups(c.groups.len(), c.groups.as_ptr()) != 0
					|| libc::setgid(c.gid) != 0
					|| libc::setuid(c.uid) != 0
				{
					return Err(io::Error::last_os_error());
				}
			} else if libc::geteuid() != c.uid {
				return Err(io::Error::from_raw_os_error(libc::EPERM));
			}
		}
		Ok(())
	}
}

/// Forward process events to a Start/Connect stream: `start{pid}`, then
/// data/keepalives, then exactly one `end`. Returns when the end was sent or
/// the client left (the process keeps running either way).
pub async fn forward<R>(
	pid: u32,
	sub: Subscription,
	sink: &StreamSink,
	keepalive: Duration,
	wrap: impl Fn(ProcessEvent) -> R + Send + Sync,
) -> Result<(), RpcError>
where
	R: prost::Message + serde::Serialize + Send,
{
	let send = |e: ev::Event| wrap(ProcessEvent { event: Some(e) });
	if sink
		.send(&send(ev::Event::Start(ev::StartEvent { pid })))
		.await
		.is_err()
	{
		return Ok(());
	}
	let mut rx = match sub {
		Subscription::Ended(end) => {
			let _ = sink.send(&send(ev::Event::End(end))).await;
			return Ok(());
		},
		Subscription::Live(rx) => rx,
	};
	let mut next_ping = tokio::time::Instant::now() + keepalive;
	loop {
		tokio::select! {
			event = rx.recv() => match event {
				Some(Event::Data(output)) => {
					let data = ev::Event::Data(ev::DataEvent { output: Some(output) });
					if sink.send(&send(data)).await.is_err() {
						return Ok(());
					}
					next_ping = tokio::time::Instant::now() + keepalive;
				},
				Some(Event::End(end)) => {
					let _ = sink.send(&send(ev::Event::End(end))).await;
					return Ok(());
				},
				None => return Err(RpcError::internal("process event stream closed without an end event")),
			},
			() = tokio::time::sleep_until(next_ping) => {
				if sink.send(&send(ev::Event::Keepalive(ev::KeepAlive {}))).await.is_err() {
					return Ok(());
				}
				next_ping = tokio::time::Instant::now() + keepalive;
			},
			() = sink.closed() => return Ok(()),
		}
	}
}

/// Map an envd `Signal` enum value to a POSIX signal (only TERM/KILL).
pub fn posix_signal(value: i32) -> Result<libc::c_int, RpcError> {
	match pb::Signal::try_from(value) {
		Ok(pb::Signal::Sigterm) => Ok(libc::SIGTERM),
		Ok(pb::Signal::Sigkill) => Ok(libc::SIGKILL),
		_ => Err(RpcError::new(Code::Unimplemented, format!("invalid signal: {value}"))),
	}
}

/// Write `input` to the process's stdin or PTY as the oneof selects.
pub async fn send_input(proc: &Proc, input: Option<pb::ProcessInput>) -> Result<(), RpcError> {
	match input.and_then(|i| i.input) {
		Some(Input::Stdin(data)) => proc.write_stdin(&data).await,
		Some(Input::Pty(data)) => proc.write_pty(&data).await,
		None => Ok(()),
	}
}

#[cfg(test)]
mod tests {
	use std::os::unix::process::ExitStatusExt;

	use super::*;

	#[test]
	fn end_event_matches_go_process_state() {
		let ok = end_event(std::process::ExitStatus::from_raw(0));
		assert_eq!(
			(ok.exit_code, ok.exited, ok.status.as_str(), ok.error),
			(0, true, "exit status 0", None)
		);
		let fail = end_event(std::process::ExitStatus::from_raw(3 << 8));
		assert_eq!(fail.exit_code, 3);
		assert_eq!(fail.error.as_deref(), Some("exit status 3"));
		let killed = end_event(std::process::ExitStatus::from_raw(libc::SIGKILL));
		assert_eq!((killed.exit_code, killed.exited), (-1, false));
		assert_eq!(killed.status, "signal: killed");
		assert_eq!(killed.error.as_deref(), Some("signal: killed"));
		let core = end_event(std::process::ExitStatus::from_raw(libc::SIGSEGV | 0x80));
		assert_eq!(core.status, "signal: segmentation fault (core dumped)");
		assert_eq!(signal_name(64), "signal 64");
	}

	#[test]
	fn lifecycle_logs_match_upstream_process_logger() {
		let config = ProcessConfig {
			cmd: "/bin/bash".into(),
			args: vec!["-l".into(), "-c".into(), "echo hi".into()],
			..Default::default()
		};
		let (msg, fields) = start_log(42, &config);
		assert_eq!(msg, "Process with pid 42 started");
		assert_eq!(
			fields,
			serde_json::json!({"logger": "process", "event_type": "process_start", "pid": 42, "command": "/bin/bash -l -c echo hi"})
		);
		let (msg, fields) = end_log(42, &end_event(std::process::ExitStatus::from_raw(3 << 8)));
		assert_eq!(msg, "Process with pid 42 ended");
		assert_eq!(fields["event_type"], "process_end");
		assert_eq!(fields["process_result"]["exit_code"], 3);
		assert_eq!(fields["process_result"]["error"], "exit status 3");
	}

	#[test]
	fn only_term_and_kill_are_accepted() {
		assert_eq!(posix_signal(15).unwrap(), libc::SIGTERM);
		assert_eq!(posix_signal(9).unwrap(), libc::SIGKILL);
		assert_eq!(posix_signal(0).unwrap_err().code, Code::Unimplemented);
		assert_eq!(posix_signal(2).unwrap_err().code, Code::Unimplemented);
	}
}
