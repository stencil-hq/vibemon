//! Shared envd state: `/init`-controlled defaults (user, workdir, env vars,
//! access token, sandbox identity), the process and watcher registries, the
//! cgroup manager, and pause-hook locks. One `Arc<EnvdState>` per server.

use std::{collections::BTreeMap, path::Path};

use parking_lot::{Mutex, RwLock};

use super::{
	cgroups::Cgroups,
	metrics::CpuSampler,
	process::Registry as Processes,
	users::{self, User},
	watch::Registry as Watchers,
};

/// Directory holding the `.E2B_*` identity files.
pub const RUN_DIR: &str = "/run/e2b";

/// Values `/init` sets and later requests consume.
#[derive(Debug, Default)]
pub struct Defaults {
	/// Default user (`/init.defaultUser`, else `root`).
	pub user:           String,
	/// Default working directory (`/init.defaultWorkdir`).
	pub workdir:        Option<String>,
	/// `/init.envVars` (replaced wholesale by each applied `/init`).
	pub init_env:       BTreeMap<String, String>,
	/// envd access token; `None` = open access.
	pub access_token:   Option<String>,
	/// `E2B_SANDBOX_ID`.
	pub sandbox_id:     Option<String>,
	/// `E2B_TEMPLATE_ID`.
	pub template_id:    Option<String>,
	/// `E2B_EVENTS_ADDRESS` (from `/init.hyperloopIP`).
	pub events_address: Option<String>,
	/// Newest applied `/init.timestamp` (unix ns): older replays are ignored.
	pub last_init_ns:   Option<i128>,
}

/// Why a username could not be used.
#[derive(Debug)]
pub enum UserError {
	/// No such account.
	Unknown(String),
	/// The passwd lookup itself failed.
	Lookup(std::io::Error),
}

impl std::fmt::Display for UserError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Unknown(name) => write!(f, "invalid username: '{name}'"),
			Self::Lookup(e) => write!(f, "user lookup failed: {e}"),
		}
	}
}

/// Process-wide envd state.
pub struct EnvdState {
	/// Template env vars from the agent (lowest precedence after the base set).
	pub template_env: Vec<(String, String)>,
	/// `PATH` given to spawned processes before template/request overrides.
	pub path_env:     String,
	/// `/init`-controlled defaults.
	pub defaults:     RwLock<Defaults>,
	/// Live and recently exited processes.
	pub processes:    Processes,
	/// Polling watchers (`CreateWatcher`).
	pub watchers:     Watchers,
	/// Workload cgroups (no-op when cgroup v2 is unavailable).
	pub cgroups:      Cgroups,
	/// Serializes `/freeze`, `/unfreeze`, `/fsfreeze`, `/fsthaw`, `/init` thaw.
	pub freeze_lock:  tokio::sync::Mutex<()>,
	/// Serializes `/init` handlers.
	pub init_lock:    tokio::sync::Mutex<()>,
	/// Previous `/proc/stat` sample for `cpu_used_pct`.
	pub cpu:          Mutex<CpuSampler>,
	/// Last `/init.cpuCount` target and how many attempts reaching it took.
	pub cpu_target:   Mutex<(u32, u32)>,
}

impl EnvdState {
	/// Env vars every process gets after `PATH/HOME/USER/LOGNAME/SHELL` and
	/// before request `envs`: template env, `E2B_*` identity, `/init` env.
	/// Also the body of `GET /envs`.
	pub fn default_env(&self) -> BTreeMap<String, String> {
		let d = self.defaults.read();
		let mut env: BTreeMap<String, String> = self.template_env.iter().cloned().collect();
		env.insert("E2B_SANDBOX".into(), "true".into());
		if let Some(id) = &d.sandbox_id {
			env.insert("E2B_SANDBOX_ID".into(), id.clone());
		}
		if let Some(id) = &d.template_id {
			env.insert("E2B_TEMPLATE_ID".into(), id.clone());
		}
		if let Some(addr) = &d.events_address {
			env.insert("E2B_EVENTS_ADDRESS".into(), addr.clone());
		}
		env.extend(d.init_env.iter().map(|(k, v)| (k.clone(), v.clone())));
		env
	}

	/// Resolve the request user: explicit name, else the default user.
	pub fn resolve_user(&self, name: Option<&str>) -> Result<User, UserError> {
		let name = match name {
			Some(n) if !n.is_empty() => n.to_owned(),
			_ => self.defaults.read().user.clone(),
		};
		match users::lookup(&name) {
			Ok(Some(user)) => Ok(user),
			Ok(None) => Err(UserError::Unknown(name)),
			Err(e) => Err(UserError::Lookup(e)),
		}
	}

	/// Default workdir snapshot.
	pub fn workdir(&self) -> Option<String> {
		self.defaults.read().workdir.clone()
	}

	/// Current access token snapshot.
	pub fn access_token(&self) -> Option<String> {
		self.defaults.read().access_token.clone()
	}
}

/// Write `/run/e2b/<name>` (best effort: identity files are informational).
pub fn write_run_file(name: &str, value: &str) {
	let dir = Path::new(RUN_DIR);
	if std::fs::create_dir_all(dir).is_ok() {
		let _ = std::fs::write(dir.join(name), value);
	}
}
