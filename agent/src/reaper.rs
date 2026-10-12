//! PID-1 child reaping that never steals an owned child's exit status.
//!
//! The agent (as PID 1 / child subreaper) inherits every orphan; those must be
//! reaped. Children whose owner waits for them itself (exec sessions, PTYs,
//! envd processes) are *tracked*: owners hold [`spawn_guard`] across the
//! spawn, [`SpawnGuard::track`] the pid, and [`untrack`] it after their own
//! wait. [`reap_untracked`] holds the exclusive side of the same lock while it
//! scans, so a freshly spawned child can never be reaped before it is tracked.

use std::{
	collections::HashSet,
	fs,
	sync::{LazyLock, Mutex, RwLock, RwLockReadGuard},
};

static SPAWN_LOCK: RwLock<()> = RwLock::new(());
static TRACKED: LazyLock<Mutex<HashSet<i32>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Held by a spawner between `spawn()` and [`SpawnGuard::track`].
pub struct SpawnGuard {
	_guard: RwLockReadGuard<'static, ()>,
}

impl SpawnGuard {
	/// Mark `pid` as owned by the caller: the reaper will leave it alone.
	pub fn track(&self, pid: i32) {
		tracked().insert(pid);
	}
}

/// Take before spawning a child you will wait for yourself (exec, envd).
pub fn spawn_guard() -> SpawnGuard {
	SpawnGuard {
		_guard: SPAWN_LOCK
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner),
	}
}

/// Release `pid` after the owner's wait has reaped it.
pub fn untrack(pid: i32) {
	tracked().remove(&pid);
}

fn tracked() -> std::sync::MutexGuard<'static, HashSet<i32>> {
	TRACKED
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Reap every exited, untracked child; returns how many were reaped. Called
/// periodically by the PID-1 loop.
pub fn reap_untracked() -> usize {
	let _exclusive = SPAWN_LOCK
		.write()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	let owned = tracked().clone();
	let mut reaped = 0;
	loop {
		// SAFETY: an all-zero `siginfo_t` is a valid buffer for `waitid`.
		let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
		// SAFETY: `info` is writable; WNOWAIT leaves the child waitable.
		let rc = unsafe {
			libc::waitid(libc::P_ALL, 0, &mut info, libc::WEXITED | libc::WNOHANG | libc::WNOWAIT)
		};
		if rc == -1 {
			return reaped;
		}
		// SAFETY: `waitid` succeeded, so `info` is initialized.
		let pid = unsafe { info.si_pid() };
		if pid == 0 {
			return reaped;
		}
		if !owned.contains(&pid) {
			reap(pid);
			reaped += 1;
			continue;
		}
		// The first waitable child is owned; its owner reaps it. Find the other
		// exited children through /proc so they are not blocked behind it.
		for zombie in zombie_children() {
			if !owned.contains(&zombie) {
				reap(zombie);
				reaped += 1;
			}
		}
		return reaped;
	}
}

fn reap(pid: i32) {
	let mut status = 0;
	// SAFETY: `pid` is an exited child; WNOHANG never blocks.
	let _ = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
}

/// Pids of our zombie children, from `/proc/<pid>/stat`.
fn zombie_children() -> Vec<i32> {
	// SAFETY: `getpid` has no preconditions.
	let me = unsafe { libc::getpid() };
	let Ok(dir) = fs::read_dir("/proc") else {
		return Vec::new();
	};
	dir.filter_map(Result::ok)
		.filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
		.filter(|pid| {
			fs::read_to_string(format!("/proc/{pid}/stat"))
				.ok()
				.and_then(|stat| parse_state_ppid(&stat))
				.is_some_and(|(state, ppid)| state == 'Z' && ppid == me)
		})
		.collect()
}

/// Extract `(state, ppid)` from a `/proc/<pid>/stat` line (comm may contain
/// spaces and parentheses, so parse after the last `)`).
fn parse_state_ppid(stat: &str) -> Option<(char, i32)> {
	let rest = &stat[stat.rfind(')')? + 1..];
	let mut fields = rest.split_ascii_whitespace();
	let state = fields.next()?.chars().next()?;
	let ppid = fields.next()?.parse().ok()?;
	Some((state, ppid))
}

#[cfg(test)]
mod tests {
	use std::process::Command;

	use super::*;

	#[test]
	fn parses_stat_with_hostile_comm() {
		assert_eq!(parse_state_ppid("42 (a) b) (c) Z 1 42 42 0"), Some(('Z', 1)));
		assert_eq!(parse_state_ppid("garbage"), None);
	}

	#[test]
	fn tracked_child_status_survives_reaper() {
		let guard = spawn_guard();
		let mut tracked_child = Command::new("sh").args(["-c", "exit 7"]).spawn().unwrap();
		guard.track(tracked_child.id() as i32);
		drop(guard);
		let mut orphan = Command::new("true").spawn().unwrap();
		std::thread::sleep(std::time::Duration::from_millis(200));
		assert!(reap_untracked() >= 1);
		// The untracked child was reaped by us, so its own wait fails…
		assert!(orphan.try_wait().is_err());
		// …while the tracked one still reports its real status.
		assert_eq!(tracked_child.wait().unwrap().code(), Some(7));
		untrack(tracked_child.id() as i32);
	}
}
