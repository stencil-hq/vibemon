//! E2B envd-compatible server running inside the guest (TCP 49983).
//!
//! Serves the Connect v1 `process.Process` / `filesystem.Filesystem` services
//! and envd's REST surface (`/health`, `/metrics`, `/init`, `/envs`,
//! `/files`, pause hooks) so the unmodified E2B SDKs can drive the sandbox.
//! Started by the agent's init path via [`spawn`] and, for testing, as
//! `vmon-agent envd [--port N]` via [`run_standalone`].

/// envd version this server reports (`X-Envd-Version` on `/init`; the node
/// records it as the sandbox's `envdVersion`).
///
/// 0.6.6 is the highest upstream release whose complete client- and
/// orchestrator-visible feature set is implemented here: every SDK gate up to
/// 0.6.4 (recursive watch, stdin control, default user, `CloseStdin`,
/// octet-stream + gzip upload, file metadata, watch entry info, network-mount
/// watch flag), cgroup `/freeze` (0.6.3), `/collapse` (0.6.5) and
/// `/fsfreeze`/`/fsthaw` (0.6.6). The next server gate, live `/upgrade`
/// (0.6.12), is not implemented.
pub const ENVD_VERSION: &str = "0.6.6";

/// Returns [`ENVD_VERSION`]; for callers that prefer a function.
#[must_use]
pub const fn envd_version() -> &'static str {
	ENVD_VERSION
}

/// Default envd TCP port (the SDKs hard-code it).
pub const DEFAULT_PORT: u16 = 49983;

#[cfg(target_os = "linux")]
mod auth;
#[cfg(target_os = "linux")]
mod cgroups;
#[cfg(target_os = "linux")]
mod connect;
#[cfg(all(test, target_os = "linux"))]
mod e2e_tests;
#[cfg(target_os = "linux")]
mod entry;
#[cfg(target_os = "linux")]
mod files;
#[cfg(target_os = "linux")]
mod filesystem;
#[cfg(target_os = "linux")]
mod http;
#[cfg(target_os = "linux")]
mod init;
#[cfg(target_os = "linux")]
mod metrics;
#[cfg(target_os = "linux")]
mod paths;
#[cfg(target_os = "linux")]
mod pause;
#[cfg(target_os = "linux")]
mod pb;
#[cfg(target_os = "linux")]
mod process;
#[cfg(target_os = "linux")]
mod server;
#[cfg(target_os = "linux")]
mod state;
#[cfg(target_os = "linux")]
mod time;
#[cfg(target_os = "linux")]
mod users;
#[cfg(target_os = "linux")]
mod watch;

#[cfg(target_os = "linux")]
pub use server::{EnvdConfig, EnvdHandle, run_standalone, spawn};
