//! Engine: single owner of the sandbox registry and all VM lifecycle logic.
//!
//! [`EngineApi`] is the seam the v1 API layer talks through (and API tests
//! fake); [`Engine`] is the real implementation, a method-for-method port of
//! `python/vmon/core.py` on top of [`spawn::MicroVm`], [`agent::AgentConn`],
//! the image pipeline, and host networking. The engine is synchronous; the
//! async API layer reaches it via `tokio::task::spawn_blocking`.

pub mod agent;
pub mod control;
pub(crate) mod diskdelta;
mod facade;
pub mod pty;
pub mod s3proxy;
pub mod spawn;
pub(crate) mod staging_gc;
pub mod vpc;

use std::collections::HashMap;

pub use facade::Engine;
pub(crate) use facade::{LifecycleCaptureGuard, ReplicaExport};
use futures_util::future::BoxFuture;
use serde_json::{Map, Value};

use crate::{
	EngineError,
	error::Result,
	models::{ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
	security::credentials::{Credential, CredentialMetadata},
};
/// Mesh-routing handoff for a portable restore. The staged epoch is
/// deliberately non-serving until the VM is ready and `PostgreSQL` finalizes
/// it.
pub(crate) trait OwnershipHandoff: Send + Sync {
	/// Return this runtime's current development-mode owner, if the sandbox is
	/// mesh-managed. Production lifecycle code reads `PostgreSQL` directly.
	fn current_owner(&self, _sid: &str) -> Result<Option<(String, i64)>> {
		Ok(None)
	}
	fn begin_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	fn commit_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	fn abort_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Acquire fresh quorum writable-volume votes for a staged portable
	/// restore. Implementations remove archived lease metadata from `params`;
	/// callers must persist only these returned epoch-fenced values.
	fn acquire_restore_volume_leases<'a>(
		&'a self,
		sid: &'a str,
		owner: &'a str,
		epoch: i64,
		params: &'a mut Map<String, Value>,
	) -> BoxFuture<'a, Result<Vec<Value>>>;
	fn persist_restore_volume_leases<'a>(
		&'a self,
		sid: &'a str,
		leases: &'a [Value],
	) -> BoxFuture<'a, Result<()>>;
	fn release_restore_volume_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, Result<()>>;
	fn release_source_volume_leases<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<()>>;
	/// Fence and durably mark the exact portable recovery point before the
	/// source VM is torn down. While prepared the sandbox is non-serving.
	fn prepare_suspend(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		point: &str,
		generation: u64,
	) -> Result<()>;
	/// Commit the previously prepared suspension only after the source's local
	/// registry has durably become suspended.
	fn commit_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Cancel an uncommitted suspend marker while the paused source is resumed.
	fn abort_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Fence a rollback's immutable target and its portable safety point before
	/// the existing candidate is destructively replaced.
	fn prepare_rollback(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		target: &str,
		safety: &str,
		operation_generation: u64,
		checkpoint_generation: u64,
	) -> Result<()>;
	/// Atomically publish authoritative Running metadata and clear the exact
	/// rollback marker only after the replacement is ready.
	fn commit_rollback(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Clear only an uncommitted rollback marker after the original/safety
	/// candidate has been restored.
	fn abort_rollback(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Route the instance away and durably tombstone this exact owner before
	/// its local VM can be destroyed.
	fn begin_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
	/// Delete authoritative ownership/idempotency/replica rows only after the
	/// local candidate is confirmed gone. A failed commit leaves the tombstone
	/// for mesh maintenance replay.
	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()>;
}

/// A non-interactive or streaming exec request (shared by `POST /exec`, the
/// exec WebSocket, and the shell gateway; §2.4 of the migration contract).
#[derive(Clone, Debug, Default)]
pub struct ExecRequest {
	pub cmd:     Vec<String>,
	pub tty:     bool,
	pub env:     Option<HashMap<String, String>>,
	pub workdir: Option<String>,
	/// Seconds; `POST /exec` callers are additionally capped server-side (60s).
	pub timeout: Option<f64>,
}

/// Captured result of a non-interactive exec.
#[derive(Clone, Debug)]
pub struct ExecCapture {
	pub exit:   i64,
	pub stdout: Vec<u8>,
	pub stderr: Vec<u8>,
}

/// Exit of a streamed exec: `{exit, signal}` per the WS frame protocol.
#[derive(Clone, Copy, Debug)]
pub struct ExecExit {
	pub code:   i64,
	pub signal: Option<i64>,
}

/// Control half of a live exec stream: stdin, resize, kill.
pub trait ExecControl: Send {
	fn write_stdin(&mut self, data: &[u8]) -> Result<()>;
	fn close_stdin(&mut self) -> Result<()>;
	fn resize(&mut self, rows: u16, cols: u16) -> Result<()>;
	fn kill(&mut self, signal: i32) -> Result<()>;
}

/// A live streaming exec: channel receivers work from both sync and async
/// (flume) consumers. Streams close on EOF; `exit` yields exactly one value.
pub struct ExecStream {
	pub control: Box<dyn ExecControl>,
	pub stdout:  flume::Receiver<Vec<u8>>,
	pub stderr:  flume::Receiver<Vec<u8>>,
	pub exit:    flume::Receiver<ExecExit>,
}

/// A started shell session (`WS /v1/shell`): the resolved sandbox name, the
/// interactive stream, and whether the sandbox is ephemeral (cleaned up on
/// disconnect via [`EngineApi::shell_cleanup`]).
pub struct ShellSession {
	pub name:      String,
	pub stream:    ExecStream,
	pub ephemeral: bool,
}

/// Immutable rolling recovery point retained for one sandbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPoint {
	/// Stable recovery-point name.
	pub name:                   String,
	/// `disk` or `checkpoint`.
	pub kind:                   String,
	/// Capture time.
	pub created_at_unix_millis: u64,
	/// Encrypted archive size.
	pub size_bytes:             u64,
}

/// The engine surface the v1 API layer consumes. Sync and blocking: the API
/// layer wraps calls in `spawn_blocking`. Views are `serde_json::Value`
/// objects with the canonical `record.view()` keys.
pub trait EngineApi: Send + Sync + 'static {
	// -- sandboxes --------------------------------------------------------
	fn create(&self, params: SandboxCreate) -> Result<Value>;
	fn list(&self, tags: Option<HashMap<String, String>>) -> Result<Vec<Value>>;
	/// Return cached sandbox views without runtime probes for orchestration
	/// heartbeats.
	fn orchestration_inventory(&self) -> Result<Vec<Value>> {
		self.list(None)
	}
	fn get(&self, id: &str) -> Result<Value>;
	fn stop(&self, id: &str) -> Result<Value>;
	/// Stop a sandbox while preserving a caller-owned process return code.
	fn stop_with_returncode(&self, id: &str, returncode: Option<i64>) -> Result<Value> {
		let _ = returncode;
		self.stop(id)
	}
	/// Stop + delete state (today's `/remove`; `DELETE /v1/sandboxes/{id}`).
	fn remove(&self, id: &str) -> Result<Value>;
	fn terminate(&self, id: &str, reason: &str) -> Result<Value>;
	fn pause(&self, id: &str) -> Result<Value>;
	fn resume(&self, id: &str) -> Result<Value>;
	fn resize(
		&self,
		id: &str,
		cpus: Option<u32>,
		memory_mib: Option<u32>,
		disk_mb: Option<u64>,
	) -> Result<Value> {
		let _ = (id, cpus, memory_mib, disk_mb);
		Err(EngineError::unsupported("sandbox resizing is unavailable"))
	}
	/// Atomically claim an expired durable suspended marker and install only its
	/// non-serving local projection. Callers must invoke this solely after
	/// owner routing proves the recorded owner unavailable, then call `resume`
	/// to perform the ordinary fenced materialization.
	fn adopt_suspended_marker(&self, _id: &str) -> Result<()> {
		Err(EngineError::unsupported("durable suspended-marker adoption is unavailable"))
	}
	/// Durably checkpoint and stop a sandbox while preserving its identity.
	fn suspend(&self, id: &str) -> Result<Value>;
	/// Return retained rolling recovery points for one sandbox.
	fn history(&self, id: &str) -> Result<Vec<RecoveryPoint>>;
	/// Restore a sandbox identity to an immutable recovery point.
	fn rollback(&self, id: &str, recovery_point: &str) -> Result<Value>;
	/// Returns `{"deadline_unix": …}`.
	fn extend(&self, id: &str, secs: u64) -> Result<Value>;
	/// Replace and rearm one sandbox's network-idle suspension policy.
	fn set_idle_timeout(&self, id: &str, secs: f64) -> Result<Value> {
		let _ = (id, secs);
		Err(EngineError::unsupported("runtime idle-timeout updates are unavailable"))
	}
	fn metrics(&self, id: &str) -> Result<Value>;

	// -- console / logs ---------------------------------------------------
	/// Full console log contents (non-follow `GET /logs`).
	fn logs(&self, id: &str) -> Result<Vec<u8>>;
	/// Follow the console log: receiver of appended chunks; closes when the
	/// VM reaches a terminal state. Also backs `WS /{id}/attach`.
	fn logs_follow(&self, id: &str) -> Result<flume::Receiver<Vec<u8>>>;

	// -- exec / shell -----------------------------------------------------
	/// Non-interactive exec with captured output (server-capped timeout).
	fn exec_capture(&self, id: &str, req: ExecRequest) -> Result<ExecCapture>;
	/// Streaming exec for the WS protocol (§2.4).
	fn exec_stream(&self, id: &str, req: ExecRequest) -> Result<ExecStream>;
	/// Open and attach to a persistent guest PTY session.
	fn pty_open(&self, _id: &str, _params: Value) -> Result<pty::PtyStream> {
		Err(EngineError::unsupported("persistent PTYs are unavailable"))
	}
	/// Reattach to a persistent guest PTY session.
	fn pty_attach(&self, _id: &str, _params: Value) -> Result<pty::PtyStream> {
		Err(EngineError::unsupported("persistent PTYs are unavailable"))
	}
	fn pty_list(&self, _id: &str) -> Result<Vec<Value>> {
		Err(EngineError::unsupported("persistent PTYs are unavailable"))
	}
	fn pty_close(&self, _id: &str, _session_id: &str) -> Result<Value> {
		Err(EngineError::unsupported("persistent PTYs are unavailable"))
	}
	fn pty_exec(
		&self,
		_id: &str,
		_session_id: &str,
		_command: &str,
		_timeout: f64,
	) -> Result<ExecCapture> {
		Err(EngineError::unsupported("persistent PTYs are unavailable"))
	}
	/// Boot-or-attach an interactive shell (`WS /v1/shell` semantics; params
	/// are the loose query/first-frame shell parameters).
	fn shell_start(&self, params: Value) -> Result<ShellSession>;
	/// Tear down an ephemeral shell sandbox after its WS disconnects.
	fn shell_cleanup(&self, name: &str);

	// -- guest filesystem -------------------------------------------------
	fn file_read(&self, id: &str, path: &str) -> Result<Vec<u8>>;
	fn file_write(&self, id: &str, path: &str, data: &[u8]) -> Result<()>;
	fn file_delete(&self, id: &str, path: &str, recursive: bool) -> Result<()>;
	fn file_list(&self, id: &str, path: &str) -> Result<Value>;
	fn file_stat(&self, id: &str, path: &str) -> Result<Value>;

	// -- networking -------------------------------------------------------
	fn network_get(&self, id: &str) -> Result<Value>;
	fn network_set(&self, id: &str, policy: NetworkBody) -> Result<Value>;
	/// Returns `{"tunnels": {...}, "connect_token": "…"}`.
	fn tunnels(&self, id: &str) -> Result<Value>;
	/// Host target `(ip, port)` for the tunnel HTTP/WS proxy.
	fn tunnel_target(&self, id: &str, port: u16) -> Result<(String, u16)>;
	fn vpc_create(
		&self,
		_tenant: &str,
		_name: Option<&str>,
		_cidr: Option<&str>,
	) -> Result<vpc::Vpc> {
		Err(EngineError::unsupported("VPCs are unavailable"))
	}
	fn vpc_list(&self, _tenant: &str) -> Result<Vec<vpc::Vpc>> {
		Err(EngineError::unsupported("VPCs are unavailable"))
	}
	fn vpc_delete(&self, _tenant: &str, _id: &str) -> Result<()> {
		Err(EngineError::unsupported("VPCs are unavailable"))
	}

	// -- snapshots --------------------------------------------------------
	/// Machine snapshot -> `{"snapshot": name, "dir": path}`.
	fn snapshot(&self, id: &str, name: Option<String>, stop: bool) -> Result<Value>;
	/// Filesystem snapshot -> `{"image": name}`.
	fn snapshot_fs(&self, id: &str, name: Option<String>) -> Result<Value>;
	fn snapshots(&self) -> Result<Vec<String>>;
	/// Restore a named snapshot into a fresh sandbox -> view (201).
	fn restore(&self, snapshot: &str, body: RestoreBody) -> Result<Value>;
	/// Delete an immutable named snapshot.
	fn snapshot_delete(&self, snapshot: &str) -> Result<()>;
	/// Fork `CoW` clones -> `{"clones": [<canonical sandbox view>, ...]}`.
	fn fork(&self, snapshot: &str, body: ForkBody) -> Result<Value>;

	// -- volumes / pools (server-owned) ------------------------------------
	fn volume_list(&self) -> Result<Vec<String>>;
	fn volume_create(&self, name: &str) -> Result<()>;
	fn volume_delete(&self, name: &str) -> Result<()>;
	/// `{ref: {ready, hits, misses, size}}`.
	fn pool_list(&self) -> Result<Value>;
	fn pool_set(&self, reference: &str, body: PoolPutBody) -> Result<Value>;
	fn pool_delete(&self, reference: &str) -> Result<()>;

	// -- credentials ------------------------------------------------------
	/// List non-secret credential metadata for one authenticated tenant.
	fn credential_list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>>;
	/// Encrypt or rotate one tenant-scoped credential.
	fn credential_put(
		&self,
		tenant: &str,
		key_id: &str,
		credential: Credential,
	) -> Result<CredentialMetadata>;
	/// Revoke one tenant-scoped credential.
	fn credential_delete(&self, tenant: &str, name: &str) -> Result<()>;

	// -- system -----------------------------------------------------------
	/// `{version, platform, arch, backend, capabilities}`.
	fn info(&self) -> Result<Value>;
	/// Subscribe to lifecycle events for the SSE stream (`{event, ts, …}`).
	fn subscribe_events(&self) -> flume::Receiver<Value>;
	/// Prometheus text exposition for `GET /metrics`.
	fn prometheus_metrics(&self) -> String;
	/// Mesh migrate (Phase 4); `unsupported` until the mesh port lands.
	fn migrate(&self, id: &str, target: &str) -> Result<Value>;
}
