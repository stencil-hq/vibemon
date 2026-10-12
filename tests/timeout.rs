mod common;

use std::{fs, time::Duration};

use serde_json::{Value, json};

#[test]
fn timeout_secs_self_terminates_with_status_json() {
	if !common::require_hv() {
		return;
	}

	// Volume write round-trips require the agent/e2e path and are covered by the
	// Python e2e suite; this legacy initramfs harness only needs lifecycle
	// smoke.
	let dir = common::test_dir("timeout");
	let sock = dir.join("api.sock");
	let mut args = common::base_args("soak");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--timeout-secs".into());
	args.push("2".into());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);

	let Some((status, output)) = vm.wait_for_exit(Duration::from_secs(10)) else {
		let output = vm.output();
		vm.kill();
		panic!("vmon did not exit within 10s; output:\n{output}");
	};

	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);

	let status_path = dir.join("status.json");
	let status_json = fs::read_to_string(&status_path)
		.unwrap_or_else(|e| panic!("reading {}: {e}", status_path.display()));
	let status: Value = serde_json::from_str(&status_json)
		.unwrap_or_else(|e| panic!("parsing {}: {e}; body={status_json:?}", status_path.display()));

	assert_eq!(status.get("state").and_then(Value::as_str), Some("finished"));
	assert_eq!(status.get("reason").and_then(Value::as_str), Some("timeout"));
	assert_eq!(status.get("vmm_returncode").and_then(Value::as_i64), Some(124));
}

/// `extend` control verb: re-arming the deadline defers the `--timeout-secs`
/// self-kill, and the eventual shutdown is not attributed to a timeout.
#[test]
fn extend_defers_timeout_self_kill() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("timeout-extend");
	let sock = dir.join("api.sock");
	let mut args = common::base_args("soak");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--timeout-secs".into());
	args.push("3".into());

	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);

	// Extend as soon as the control socket accepts (well inside the original
	// 3s window, which starts at VMM launch — a slow boot must not eat it),
	// then wait for the guest to come up.
	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	let result = control.request("extend", json!({"secs": 60}));
	vm.wait_for("SOAK_READY", Duration::from_mins(1));
	assert!(
		result
			.get("deadline_unix")
			.and_then(Value::as_u64)
			.is_some(),
		"extend did not report the new deadline: {result}"
	);

	// Outlive the original 3s deadline by a comfortable margin. The control
	// server drops idle connections after 5s, so drop ours and reconnect for
	// the quit (clients speak one command per connection anyway).
	drop(control);
	std::thread::sleep(Duration::from_secs(5));
	assert!(vm.has_exited().is_none(), "vmon self-killed despite extend; output:\n{}", vm.output());

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "vmon exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);

	let status_path = dir.join("status.json");
	if let Ok(body) = fs::read_to_string(&status_path) {
		let status: Value = serde_json::from_str(&body)
			.unwrap_or_else(|e| panic!("parsing {}: {e}; body={body:?}", status_path.display()));
		assert_ne!(
			status.get("reason").and_then(Value::as_str),
			Some("timeout"),
			"shutdown after extend was attributed to a timeout: {status}"
		);
	}
}
