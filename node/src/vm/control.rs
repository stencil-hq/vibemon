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
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
	net::UnixStream,
};

/// Timeout for quick control methods, and for sending any request.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// Error reply of a VMM control method (`{"code","message"}`).
#[derive(Debug, thiserror::Error)]
#[error("vmm {method}: {code}: {message}")]
pub struct VmmError {
	pub method:  String,
	/// Stable VMM code (`busy`, `not_paused`, `nested_virtualization_active`,
	/// `internal`, …).
	pub code:    String,
	pub message: String,
}

impl VmmError {
	/// Whether `error` (any context chain) is a VMM reply with `code`.
	pub fn has_code(error: &anyhow::Error, code: &str) -> bool {
		error.downcast_ref::<Self>().is_some_and(|e| e.code == code)
	}
}

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
		tokio::time::timeout(timeout, async {
			let lines = self.send(method, params).await?;
			Self::reply(lines, method).await
		})
		.await
		.map_err(|_| anyhow!("vmm {method}: no reply within {timeout:?}"))?
	}

	/// [`Self::call`] for methods whose duration is host IO (a snapshot writes
	/// and fsyncs guest RAM: minutes on a contended disk): sending is bounded
	/// by [`CONTROL_TIMEOUT`], the reply has no deadline. The VMM answers a
	/// request it could not start within its own start timeout with `busy`,
	/// one it started when it finishes, and its exit closes the connection.
	async fn call_to_completion(&self, method: &str, params: Value) -> anyhow::Result<Value> {
		let lines = tokio::time::timeout(CONTROL_TIMEOUT, self.send(method, params))
			.await
			.map_err(|_| anyhow!("vmm {method}: not sent within {CONTROL_TIMEOUT:?}"))??;
		Self::reply(lines, method).await
	}

	/// Connects, checks the banner, and sends one request.
	async fn send(
		&self,
		method: &str,
		params: Value,
	) -> anyhow::Result<Lines<BufReader<UnixStream>>> {
		let stream = UnixStream::connect(&self.path)
			.await
			.with_context(|| format!("connecting vmm control socket {}", self.path.display()))?;
		let mut lines = BufReader::new(stream).lines();
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
		lines.get_mut().get_mut().write_all(&line).await?;
		Ok(lines)
	}

	/// Reads the reply of the request [`Self::send`] sent.
	async fn reply(mut lines: Lines<BufReader<UnixStream>>, method: &str) -> anyhow::Result<Value> {
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
		Err(
			VmmError {
				method:  method.to_owned(),
				code:    code.to_owned(),
				message: message.to_owned(),
			}
			.into(),
		)
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
	/// paused. Waits as long as the VMM writes it.
	pub async fn snapshot(&self, name: &str) -> anyhow::Result<()> {
		self
			.call_to_completion("snapshot", json!({ "name": name }))
			.await
			.map(drop)
	}

	/// `snapshot` storing only the pages that differ from the sibling snapshot
	/// `base` (same snapshot root).
	pub async fn snapshot_delta(&self, name: &str, base: &str) -> anyhow::Result<()> {
		self
			.call_to_completion("snapshot", json!({ "name": name, "base": base }))
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

#[cfg(test)]
mod tests {
	use tokio::net::UnixListener;

	use super::*;

	/// Fake VMM control socket: banner, one request, then `reply` after
	/// `delay` (`None` closes the connection instead, as an exiting VMM does).
	fn fake_vmm(listener: UnixListener, delay: Duration, reply: Option<Value>) {
		tokio::spawn(async move {
			let (stream, _) = listener.accept().await.unwrap();
			let (read, mut write) = stream.into_split();
			write
				.write_all(b"{\"vmm\":\"test\",\"api\":1}\n")
				.await
				.unwrap();
			let mut lines = BufReader::new(read).lines();
			let request: Value =
				serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
			assert_eq!(request["method"], "snapshot");
			tokio::time::sleep(delay).await;
			if let Some(mut reply) = reply {
				reply["id"] = request["id"].clone();
				let mut line = serde_json::to_vec(&reply).unwrap();
				line.push(b'\n');
				write.write_all(&line).await.unwrap();
			}
		});
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_waits_as_long_as_the_vmm_writes_it() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("api.sock");
		// Guest RAM written and fsynced on a contended disk.
		fake_vmm(
			UnixListener::bind(&path).unwrap(),
			Duration::from_mins(10),
			Some(json!({ "ok": true, "result": {} })),
		);
		ControlClient::new(path)
			.snapshot("s")
			.await
			.expect("snapshot");
	}

	#[tokio::test(start_paused = true)]
	async fn snapshot_fails_when_the_vmm_exits_mid_write() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("api.sock");
		fake_vmm(UnixListener::bind(&path).unwrap(), Duration::from_secs(1), None);
		let error = ControlClient::new(path)
			.snapshot("s")
			.await
			.expect_err("the VMM is gone");
		assert!(
			error
				.to_string()
				.contains("closed the control socket during snapshot"),
			"{error}"
		);
	}

	#[tokio::test]
	async fn snapshot_refusals_keep_the_vmm_code() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("api.sock");
		fake_vmm(
			UnixListener::bind(&path).unwrap(),
			Duration::ZERO,
			Some(json!({ "ok": false, "error": {
				"code": "nested_virtualization_active", "message": "the guest runs VMs",
			}})),
		);
		let error = ControlClient::new(path)
			.snapshot("s")
			.await
			.expect_err("refused")
			.context("capturing");
		assert!(VmmError::has_code(&error, "nested_virtualization_active"), "{error:#}");
		assert!(!VmmError::has_code(&error, "internal"));
		assert_eq!(
			format!("{error:#}"),
			"capturing: vmm snapshot: nested_virtualization_active: the guest runs VMs"
		);
	}
}
