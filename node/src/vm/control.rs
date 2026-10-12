//! Client for the VMM control socket (`--api-sock`): newline-delimited JSON,
//! a banner line, then `{"id","method","params"}` →
//! `{"id","ok","result"|"error"}`.
//!
//! The VMM closes idle connections after its 5 s read timeout, so every call
//! opens a fresh connection (a Unix socket connect is microseconds).

use std::{
	path::{Path, PathBuf},
	sync::atomic::{AtomicU64, Ordering},
	time::Duration,
};

use anyhow::{Context, anyhow, bail};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
	net::UnixStream,
};

/// Timeout for quick control methods.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// Snapshots write all guest memory; the VMM itself gives up after 60 s.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_mins(2);

/// Control-socket client for one VMM.
#[derive(Debug)]
pub struct ControlClient {
	path:    PathBuf,
	next_id: AtomicU64,
}

impl ControlClient {
	/// Client for the socket at `path` (no connection is made yet).
	pub const fn new(path: PathBuf) -> Self {
		Self { path, next_id: AtomicU64::new(1) }
	}

	/// Socket path.
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Sends one method call and returns its `result`.
	pub async fn call(
		&self,
		method: &str,
		params: Value,
		timeout: Duration,
	) -> anyhow::Result<Value> {
		tokio::time::timeout(timeout, self.call_inner(method, params))
			.await
			.map_err(|_| anyhow!("vmm {method}: no reply within {timeout:?}"))?
	}

	async fn call_inner(&self, method: &str, params: Value) -> anyhow::Result<Value> {
		let stream = UnixStream::connect(&self.path)
			.await
			.with_context(|| format!("connecting vmm control socket {}", self.path.display()))?;
		let (read, mut write) = stream.into_split();
		let mut lines = BufReader::new(read).lines();
		let banner = lines
			.next_line()
			.await?
			.ok_or_else(|| anyhow!("vmm closed the control socket before its banner"))?;
		let banner: Value = serde_json::from_str(&banner).context("malformed vmm banner")?;
		if banner.get("api").and_then(Value::as_u64) != Some(1) {
			bail!("unsupported vmm control api: {banner}");
		}
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		let mut line = serde_json::to_vec(&json!({ "id": id, "method": method, "params": params }))?;
		line.push(b'\n');
		write.write_all(&line).await?;
		let reply = lines
			.next_line()
			.await?
			.ok_or_else(|| anyhow!("vmm closed the control socket during {method}"))?;
		let reply: Value = serde_json::from_str(&reply).context("malformed vmm reply")?;
		if reply.get("ok").and_then(Value::as_bool) == Some(true) {
			return Ok(reply.get("result").cloned().unwrap_or(Value::Null));
		}
		let error = reply.get("error").cloned().unwrap_or(Value::Null);
		let code = error
			.get("code")
			.and_then(Value::as_str)
			.unwrap_or("internal");
		let message = error
			.get("message")
			.and_then(Value::as_str)
			.unwrap_or("vmm request failed");
		bail!("vmm {method}: {code}: {message}")
	}

	/// Liveness probe.
	pub async fn ping(&self) -> anyhow::Result<()> {
		self
			.call("ping", json!({}), CONTROL_TIMEOUT)
			.await
			.map(drop)
	}

	/// `{cpus, mem_mib, state, devices, deadline_unix, uptime_ms}`.
	pub async fn info(&self) -> anyhow::Result<Value> {
		self.call("info", json!({}), CONTROL_TIMEOUT).await
	}

	/// Parks every vCPU and quiesces devices (idempotent). A failed pause
	/// kills the VM.
	pub async fn pause(&self) -> anyhow::Result<()> {
		self
			.call("pause", json!({}), CONTROL_TIMEOUT)
			.await
			.map(drop)
	}

	/// Resumes a paused VM (idempotent).
	pub async fn resume(&self) -> anyhow::Result<()> {
		self
			.call("resume", json!({}), CONTROL_TIMEOUT)
			.await
			.map(drop)
	}

	/// Writes a full snapshot to `<snapshot-root>/<name>`; the VM must be
	/// paused.
	pub async fn snapshot(&self, name: &str) -> anyhow::Result<()> {
		self
			.call("snapshot", json!({ "name": name }), SNAPSHOT_TIMEOUT)
			.await
			.map(drop)
	}

	/// `snapshot` storing only the pages that differ from the sibling snapshot
	/// `base` (same snapshot root).
	pub async fn snapshot_delta(&self, name: &str, base: &str) -> anyhow::Result<()> {
		self
			.call("snapshot", json!({ "name": name, "base": base }), SNAPSHOT_TIMEOUT)
			.await
			.map(drop)
	}

	/// Asks the VMM to exit.
	pub async fn quit(&self) -> anyhow::Result<()> {
		self
			.call("quit", json!({}), CONTROL_TIMEOUT)
			.await
			.map(drop)
	}

	/// VMM counters and gauges (boot/fork durations, exits, …).
	pub async fn metrics(&self) -> anyhow::Result<Value> {
		self.call("metrics", json!({}), CONTROL_TIMEOUT).await
	}

	/// Sets the hard deadline to now + `secs`; returns the new deadline (unix
	/// s).
	pub async fn extend(&self, secs: u64) -> anyhow::Result<i64> {
		let result = self
			.call("extend", json!({ "secs": secs }), CONTROL_TIMEOUT)
			.await?;
		result
			.get("deadline_unix")
			.and_then(Value::as_i64)
			.ok_or_else(|| anyhow!("vmm extend returned no deadline: {result}"))
	}
}
