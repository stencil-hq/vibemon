mod common;

use std::{path::PathBuf, time::Duration};

use serde_json::json;

#[test]
fn restore_resumes_past_snapshot_marker() {
	if !common::require_hv() {
		return;
	}

	let snap = create_snapshot("restore", &[]);
	let args = vec!["--restore".to_string(), snap.display().to_string()];
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "SNAPSHOT_AFTER_RESTORE", Duration::from_secs(90));

	assert!(
		output.contains("SNAPSHOT_AFTER_RESTORE") || output.contains("SNAPSHOT_RESTORED"),
		"restore marker missing:\n{output}"
	);
	common::assert_no_panic(&output);
}

#[test]
fn fork_from_count_two_starts_two_clones() {
	if !common::require_hv() {
		return;
	}
	// The supervisor only exits once every forked clone self-terminates, but the
	// x86_64 test guest cannot power off under vmm (no ACPI shutdown — its
	// kernel halts instead of exiting), so the supervisor would never return.
	// Fork reaping is exercised on aarch64 (PSCI poweroff) and by the Python
	// e2e `t_fork`, which covers fork on x86_64 too.
	if cfg!(target_arch = "x86_64") {
		return;
	}

	let snap = create_snapshot("fork", &[]);
	let args = vec![
		"--fork-from".to_string(),
		snap.display().to_string(),
		"--count".to_string(),
		"2".to_string(),
	];
	let refs = common::as_refs(&args);
	let mut parent = common::spawn_vmm(&refs);
	let (status, output) = parent.wait(Duration::from_mins(2));

	assert!(status.success(), "fork parent exited with {status}; output:\n{output}");
	// Each clone's guest serial output is byte-interleaved on the shared parent
	// stdout, so per-guest restore markers can be mangled past exact matching
	// when clones boot in lockstep (observed under nested KVM). Verify instead
	// that two clones were launched and the fork supervisor waited for all of
	// them to exit cleanly; single-clone resume-past-snapshot correctness is
	// covered by `restore_resumes_past_snapshot_marker`.
	let clones = output.matches("fork child").count();
	assert!(clones >= 2, "expected two forked clones, saw {clones}:\n{output}");
	common::assert_no_panic(&output);
}

fn create_snapshot(name: &str, extra_args: &[&str]) -> PathBuf {
	let dir = common::test_dir(&format!("snapshot-{name}"));
	let sock = dir.join("control.sock");
	let snap_name = "snapshot";
	let snap = dir.join(snap_name);
	let mut args = common::base_args("snapshot");
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	for arg in extra_args {
		args.push((*arg).to_string());
	}
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("pause"), "OK");
	assert_eq!(control.command(&format!("snapshot {snap_name}")), "OK");
	assert_eq!(control.command("quit"), "OK");

	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
	common::assert_snapshot_written(&snap);
	common::assert_no_panic(&output);
	snap
}

#[test]
fn delta_snapshot_restores_and_is_smaller() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("snapshot-delta");
	let sock = dir.join("control.sock");
	let base_name = "base";
	let delta_name = "delta";
	let base = dir.join(base_name);
	let delta = dir.join(delta_name);

	let cmdline = common::cmdline_with("snapshot", &[("vmon.snapshot_after_base_hold", "30")]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.push("--api-sock".into());
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));

	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("pause"), "OK");
	control.request("snapshot", json!({"name": base_name}));
	assert_eq!(control.command("resume"), "OK");
	vm.wait_for("SNAPSHOT_AFTER_BASE", Duration::from_mins(1));

	assert_eq!(control.command("pause"), "OK");
	control.request("snapshot", json!({"name": delta_name, "base": base_name}));
	assert_eq!(control.command("quit"), "OK");

	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
	common::assert_snapshot_written(&base);
	common::assert_snapshot_written(&delta);
	common::assert_no_panic(&output);

	let base_len = common::current_memory_file_len(&base);
	let delta_len = common::current_memory_file_len(&delta);
	assert!(delta_len < base_len, "delta memory file {delta_len} not smaller than base {base_len}");

	let restore = common::test_dir("snapshot-delta-restore");
	let mut restore_args = common::base_args("snapshot");
	restore_args.push("--restore".into());
	restore_args.push(delta.display().to_string());
	restore_args.push("--snapshot-root".into());
	restore_args.push(restore.display().to_string());
	let refs = common::as_refs(&restore_args);
	let output = common::boot_capture(&refs, "SNAPSHOT_AFTER_RESTORE", Duration::from_secs(90));
	assert!(
		output.contains("SNAPSHOT_AFTER_RESTORE") || output.contains("SNAPSHOT_RESTORED"),
		"restore marker missing:\n{output}"
	);
	common::assert_no_panic(&output);
}

/// A fork of a snapshot captured long ago continues the guest's monotonic
/// clock from the capture: the time between snapshot and fork must not show up
/// as guest time (on HVF the restore used to re-apply the raw vtimer offset,
/// jumping `CLOCK_MONOTONIC` by the whole downtime and firing every guest
/// timeout at once), and timers must keep firing on every vCPU of a multi-vCPU
/// fork (a vCPU-numbering mismatch used to hand per-vCPU interrupt state to
/// the wrong vCPU and wedge the guest).
#[test]
fn fork_of_old_multi_vcpu_snapshot_keeps_guest_time_and_timers() {
	if !common::require_hv() {
		return;
	}
	// The guest prints SNAPSHOT_READY, sleeps GUEST_SLEEP of guest time, then
	// prints SNAPSHOT_RESTORED. The snapshot lands early in that sleep and the
	// fork starts DOWNTIME later, so the sleep must still have most of
	// GUEST_SLEEP left after the fork; a counter that counted the downtime ends
	// it at once.
	const GUEST_SLEEP: u64 = 10;
	const DOWNTIME: Duration = Duration::from_secs(15);

	let dir = common::test_dir("snapshot-clock");
	let sock = dir.join("control.sock");
	let cmdline =
		common::cmdline_with("snapshot", &[("vmon.snapshot_delay", &GUEST_SLEEP.to_string())]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.extend(["--cpus", "4", "--api-sock"].map(String::from));
	args.push(sock.display().to_string());
	args.push("--snapshot-root".into());
	args.push(dir.display().to_string());
	let refs = common::as_refs(&args);
	let mut vm = common::spawn_vmm(&refs);
	vm.wait_for("SNAPSHOT_READY", Duration::from_mins(1));
	let mut control = common::ControlClient::connect(&sock, Duration::from_secs(10));
	assert_eq!(control.command("pause"), "OK");
	assert_eq!(control.command("snapshot snapshot"), "OK");
	assert_eq!(control.command("quit"), "OK");
	let (status, output) = vm.wait(Duration::from_secs(30));
	assert!(status.success(), "snapshot source exited with {status}; output:\n{output}");
	let snap = dir.join("snapshot");
	common::assert_snapshot_written(&snap);
	std::thread::sleep(DOWNTIME);

	let fork_args = ["--fork-from".to_string(), snap.display().to_string()];
	let refs = common::as_refs(&fork_args);
	let started = std::time::Instant::now();
	let mut fork = common::spawn_vmm(&refs);
	let output = fork.wait_for("SNAPSHOT_RESTORED", Duration::from_secs(GUEST_SLEEP * 3));
	let slept = started.elapsed();
	assert!(
		slept >= Duration::from_secs(GUEST_SLEEP / 2),
		"guest sleep ended {slept:?} after the fork: the {DOWNTIME:?} between snapshot and fork \
		 leaked into guest time:\n{output}"
	);
	// The x86_64 test guest cannot power off under vmm (no ACPI shutdown; its
	// kernel halts instead), so the fork never exits on its own there.
	if cfg!(target_arch = "x86_64") {
		fork.kill();
		common::assert_no_panic(&output);
		return;
	}
	let (status, output) = fork.wait(Duration::from_secs(30));
	assert!(status.success(), "fork exited with {status}; output:\n{output}");
	common::assert_no_panic(&output);
}

#[test]
fn rng_device_survives_snapshot_restore() {
	if !common::require_hv() {
		return;
	}

	// Snapshot a VM carrying the virtio-rng device, then restore it (the device
	// is reconstructed from the snapshot's backend hint, so no flag is needed on
	// restore) and confirm it resumes past the snapshot marker. This exercises
	// rng device-state capture, restore validation, and the BackendHint::Rng
	// reconstruct + reactivate path.
	let snap = create_snapshot("rng", &["--rng"]);
	let args = vec!["--restore".to_string(), snap.display().to_string()];
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "SNAPSHOT_AFTER_RESTORE", Duration::from_secs(90));

	assert!(
		output.contains("SNAPSHOT_AFTER_RESTORE") || output.contains("SNAPSHOT_RESTORED"),
		"restore marker missing:\n{output}"
	);
	common::assert_no_panic(&output);
}
