//! Minimal VMM control-socket client for `set_net_policy` (newline JSON:
//! banner line, then `{"id","method","params"}` → `{"id","ok",…}`).

use std::{path::Path, time::Duration};

use anyhow::{Context as _, bail};
use serde_json::{Value, json};
use tokio::{
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
	net::UnixStream,
};

use super::policy::VmmFilter;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Replace the live user-net filter of the VMM behind `api_sock`.
pub async fn set_net_policy(api_sock: &Path, filter: &VmmFilter) -> anyhow::Result<()> {
	tokio::time::timeout(TIMEOUT, call(api_sock, filter))
		.await
		.with_context(|| format!("set_net_policy on {} timed out", api_sock.display()))?
}

async fn call(api_sock: &Path, filter: &VmmFilter) -> anyhow::Result<()> {
	let stream = UnixStream::connect(api_sock)
		.await
		.with_context(|| format!("connecting to {}", api_sock.display()))?;
	let (read, mut write) = stream.into_split();
	let mut lines = BufReader::new(read).lines();
	lines
		.next_line()
		.await?
		.context("VMM control socket closed before its banner")?;
	let mut request = serde_json::to_vec(
		&json!({"id": 1, "method": "set_net_policy", "params": {"policy": filter}}),
	)?;
	request.push(b'\n');
	write.write_all(&request).await?;
	let line = lines
		.next_line()
		.await?
		.context("VMM control socket closed before replying")?;
	let reply: Value = serde_json::from_str(&line).context("parsing VMM control reply")?;
	if reply.get("ok").and_then(Value::as_bool) != Some(true) {
		bail!("set_net_policy failed: {}", reply.get("error").unwrap_or(&Value::Null));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use tokio::net::UnixListener;

	use super::*;

	#[tokio::test]
	async fn sends_policy_and_surfaces_vmm_errors() {
		let dir = tempfile::tempdir().expect("tmp");
		let sock = dir.path().join("api.sock");
		let listener = UnixListener::bind(&sock).expect("bind");
		let server = tokio::spawn(async move {
			let mut seen = Vec::new();
			for ok in [true, false] {
				let (stream, _) = listener.accept().await.expect("accept");
				let (read, mut write) = stream.into_split();
				write
					.write_all(b"{\"vmm\":\"t\",\"api\":1}\n")
					.await
					.expect("banner");
				let line = BufReader::new(read)
					.lines()
					.next_line()
					.await
					.expect("read")
					.expect("line");
				seen.push(serde_json::from_str::<Value>(&line).expect("json"));
				let reply = if ok {
					"{\"id\":1,\"ok\":true,\"result\":{}}\n"
				} else {
					"{\"id\":1,\"ok\":false,\"error\":{\"code\":\"bad_request\",\"message\":\"no \
					 filter\"}}\n"
				};
				write.write_all(reply.as_bytes()).await.expect("reply");
			}
			seen
		});
		let filter = VmmFilter {
			blocked: vec!["10.0.0.0/8".into()],
			exempt:  vec![],
			allow:   vec!["1.1.1.1/32".into()],
			deny:    vec!["0.0.0.0/0".into()],
			domains: vec![],
			dns:     false,
		};
		set_net_policy(&sock, &filter).await.expect("ok reply");
		let err = set_net_policy(&sock, &filter)
			.await
			.expect_err("error reply");
		assert!(err.to_string().contains("no filter"), "{err}");
		let seen = server.await.expect("join");
		assert_eq!(seen[0]["method"], "set_net_policy");
		assert_eq!(seen[0]["params"]["policy"]["allow"][0], "1.1.1.1/32");
	}
}
