//! The in-guest job gate: the runner's `ACTIONS_RUNNER_HOOK_JOB_STARTED`
//! script.
//!
//! JIT runners are repository-level, and GitHub hands a runner any queued
//! job whose labels match, so a job vmon refused (a public repository's run
//! by an actor not on the allow list) could start on a runner provisioned for
//! an allowed job. The runner start command writes this script (read-only)
//! before it execs `run.sh`; the runner runs it before the job's first step
//! and waits for it. A failing hook would not stop the job, so a denial ends
//! the VM instead (`STOP`) and never returns. It fails closed: wrong repository
//! → deny; public repository → deny unless public repositories are on and
//! either anyone may run or both `GITHUB_ACTOR` and `GITHUB_TRIGGERING_ACTOR`
//! (default `GITHUB_ACTOR`) are allowed (case-insensitive). Private
//! repositories only check the repository.
//!
//! The start command also points the runner at the node's CI cache: the
//! guest's default gateway (the node side of the sandbox's TAP) serves
//! GitHub's results service API on [`ci_cache::PORT`]; when it does not
//! answer its ping (a node without the cache), GitHub's results service is
//! used as before.

use vmon_node::ci_cache;

use crate::store::ci::PublicPolicy;

/// Path of the hook script in the `ci` image (outside `_work`).
pub const HOOK_PATH: &str = "/home/user/actions-runner/.vmon/job-started.sh";
/// The guest's IPv4 routing table (default gateway lookup).
const ROUTE_FILE: &str = "/proc/net/route";

/// `value` as one single-quoted shell word.
fn quote(value: &str) -> String {
	format!("'{}'", value.replace('\'', r"'\''"))
}

/// What a denying hook runs after its error line. A failing hook does not
/// stop the job (the runner still runs `always()`/`failure()` steps and
/// action pre-scripts), but the worker waits for the hook, so nothing of the
/// job runs while it is alive: it ends the VM (a guest reboot is a VM exit
/// on vmon), else kills the runner processes, and never returns. `exit 1`
/// after it is the last resort.
const STOP: &str = r#"sync
sudo -n /usr/sbin/reboot -ff >/dev/null 2>&1 || sudo -n /sbin/reboot -f >/dev/null 2>&1
echo b | sudo -n tee /proc/sysrq-trigger >/dev/null 2>&1
for p in /proc/[0-9]*; do
	case "$(cat "$p/comm" 2>/dev/null)" in
	Runner.Worker|Runner.Listener|run.sh|run-helper.sh) kill -KILL "${p#/proc/}" 2>/dev/null ;;
	esac
done
while :; do sleep 3600; done"#;

/// The hook script of a runner serving `repo`.
pub fn job_started_hook(repo: &str, private: bool, policy: &PublicPolicy) -> String {
	hook(repo, private, policy, STOP)
}

/// [`job_started_hook`] with `stop` run on denial (tests).
fn hook(repo: &str, private: bool, policy: &PublicPolicy, stop: &str) -> String {
	let mode = if private {
		"private"
	} else if !policy.enabled {
		"closed"
	} else if policy.anyone {
		"anyone"
	} else {
		"users"
	};
	let users: Vec<String> = policy
		.users
		.iter()
		.map(|user| user.to_lowercase())
		.collect();
	format!(
		r#"#!/bin/sh
# vmon CI job gate (ACTIONS_RUNNER_HOOK_JOB_STARTED): fails the job before its
# first step unless vmon allows it on this runner.
repo={repo}
mode={mode}
users={users}
stop={stop}
lower() {{ printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }}
deny() {{ echo "::error::vmon CI refused this job: $1"; eval "$stop"; exit 1; }}
got=$(lower "${{GITHUB_REPOSITORY:-}}")
[ "$got" = "$repo" ] || deny "this runner serves $repo, not ${{GITHUB_REPOSITORY:-an unknown repository}}"
case "$mode" in
private|anyone) exit 0 ;;
closed) deny "$repo is public and public repositories are off for its account" ;;
esac
actor=$(lower "${{GITHUB_ACTOR:-}}")
trig=$(lower "${{GITHUB_TRIGGERING_ACTOR:-${{GITHUB_ACTOR:-}}}}")
for who in "$actor" "$trig"; do
	case " $users " in
	*" $who "*) [ -n "$who" ] || deny "$repo is public and the run names no actor" ;;
	*) deny "$repo is public and @$who may not run jobs on vmon (actor @$actor, triggered by @$trig)" ;;
	esac
done
exit 0
"#,
		repo = quote(&repo.to_lowercase()),
		mode = quote(mode),
		users = quote(&users.join(" ")),
		stop = quote(stop),
	)
}

/// `sh` commands exporting `ACTIONS_RESULTS_URL` (POSIX `sh` and `awk`;
/// `set -e` safe): the node's CI cache at the default gateway read from
/// `route_file` when its ping answers, else GitHub's results service. The
/// gateway is the little-endian hex third field of the route whose
/// destination and mask are `00000000`.
fn results_url(route_file: &str) -> String {
	let port = ci_cache::PORT;
	format!(
		"u={upstream}; g=$(awk '$2 == \"00000000\" && $8 == \"00000000\" && length($3) == 8 {{ \
		 print \"0x\" substr($3, 7, 2), \"0x\" substr($3, 5, 2), \"0x\" substr($3, 3, 2), \"0x\" \
		 substr($3, 1, 2); exit }}' {route} 2>/dev/null) || g=; if [ -n \"$g\" ]; then g=$(printf \
		 '%d.%d.%d.%d' $g 2>/dev/null) || g=; fi; if [ -n \"$g\" ] && curl -fsS -m 3 -o /dev/null \
		 \"http://$g:{port}{ping}\"; then u=\"http://$g:{port}/\"; else echo \"vmon: no CI cache \
		 at the default gateway; using GitHub's results service\" >&2; fi; export \
		 ACTIONS_RESULTS_URL=\"$u\"",
		upstream = quote(ci_cache::DEFAULT_UPSTREAM),
		route = quote(route_file),
		ping = ci_cache::PING_PATH,
	)
}

/// `sh -c` start command of the runner.
///
/// Writes the hook from `$<gate_env>`, points the runner at the CI cache
/// ([`results_url`]), then execs `run.sh` with the JIT config from
/// `$<jit_env>` (both unset first, so neither reaches the jobs'
/// environment). Any failure stops before the runner starts.
pub fn start_command(jit_env: &str, gate_env: &str) -> String {
	format!(
		"set -e; h={HOOK_PATH}; c=\"${jit_env}\"; unset {jit_env}; mkdir -p \"${{h%/*}}\"; rm -f \
		 \"$h\"; printf '%s' \"${gate_env}\" > \"$h\"; chmod 555 \"$h\"; unset {gate_env}; export \
		 ACTIONS_RUNNER_HOOK_JOB_STARTED=\"$h\"; {results}; exec ./run.sh --jitconfig \"$c\"",
		results = results_url(ROUTE_FILE),
	)
}

#[cfg(test)]
mod tests {
	use std::process::Command;

	use super::*;

	/// The hook with a stop that only reports itself.
	fn test_hook(repo: &str, private: bool, policy: &PublicPolicy) -> String {
		hook(repo, private, policy, "echo STOPPED")
	}

	/// Runs `hook` with the GitHub variables `env`: `(exit 0?, stdout)`.
	fn run(hook: &str, env: &[(&str, &str)]) -> (bool, String) {
		let output = Command::new("sh")
			.arg("-c")
			.arg(hook)
			.env_clear()
			.env("PATH", "/usr/bin:/bin")
			.envs(env.iter().copied())
			.output()
			.unwrap();
		(output.status.success(), String::from_utf8_lossy(&output.stdout).into_owned())
	}

	fn policy(enabled: bool, anyone: bool, users: &[&str]) -> PublicPolicy {
		PublicPolicy { enabled, anyone, users: users.iter().map(|u| (*u).to_owned()).collect() }
	}

	#[test]
	fn public_repositories_need_both_actors_on_the_list() {
		let hook = test_hook("Acme/Site", false, &policy(true, false, &["Alice", "bob"]));
		let env = |actor, trig| {
			vec![
				("GITHUB_REPOSITORY", "acme/site"),
				("GITHUB_ACTOR", actor),
				("GITHUB_TRIGGERING_ACTOR", trig),
			]
		};
		assert_eq!(run(&hook, &env("alice", "BOB")), (true, String::new()));
		let (ok, out) = run(&hook, &env("mallory", "alice"));
		assert!(!ok && out.starts_with("::error::") && out.contains("@mallory"), "{out}");
		assert!(out.ends_with("STOPPED\n"), "a denial stops the runner: {out}");
		let (ok, out) = run(&hook, &env("alice", "mallory"));
		assert!(!ok && out.contains("triggered by @mallory"), "{out}");
		// Without a triggering actor the actor counts twice.
		assert!(run(&hook, &env("alice", "")).0);
		assert!(!run(&hook, &env("", "")).0);
		let (ok, out) = run(&hook, &[("GITHUB_REPOSITORY", "acme/app"), ("GITHUB_ACTOR", "alice")]);
		assert!(!ok && out.contains("serves acme/site, not acme/app"), "{out}");
		assert!(!run(&hook, &[("GITHUB_ACTOR", "alice")]).0, "no repository");
	}

	#[test]
	fn private_and_open_policies_only_check_the_repository() {
		let private = test_hook("acme/app", true, &policy(false, false, &[]));
		assert!(run(&private, &[("GITHUB_REPOSITORY", "ACME/app"), ("GITHUB_ACTOR", "x")]).0);
		assert!(!run(&private, &[("GITHUB_REPOSITORY", "acme/other")]).0);
		let anyone = test_hook("acme/site", false, &policy(true, true, &[]));
		assert!(run(&anyone, &[("GITHUB_REPOSITORY", "acme/site"), ("GITHUB_ACTOR", "x")]).0);
		let closed = test_hook("acme/site", false, &policy(false, true, &["alice"]));
		let (ok, out) =
			run(&closed, &[("GITHUB_REPOSITORY", "acme/site"), ("GITHUB_ACTOR", "alice")]);
		assert!(!ok && out.contains("public repositories are off"), "{out}");
	}

	#[test]
	fn the_real_stop_parses_and_is_baked_in() {
		let hook = job_started_hook("acme/site", false, &policy(true, false, &["alice"]));
		assert!(hook.contains("reboot -ff") && hook.contains("Runner.Worker|Runner.Listener"));
		let status = Command::new("sh")
			.arg("-n")
			.arg("-c")
			.arg(&hook)
			.status()
			.unwrap();
		assert!(status.success(), "the hook is valid sh");
		let status = Command::new("sh")
			.arg("-n")
			.arg("-c")
			.arg(STOP)
			.status()
			.unwrap();
		assert!(status.success(), "the stop is valid sh");
	}

	#[test]
	fn the_start_command_is_valid_sh() {
		let command = start_command("VMON_CI_JITCONFIG", "VMON_CI_GATE");
		assert!(command.contains("export ACTIONS_RESULTS_URL="), "{command}");
		assert!(command.ends_with("exec ./run.sh --jitconfig \"$c\""), "{command}");
		let status = Command::new("sh")
			.arg("-n")
			.arg("-c")
			.arg(&command)
			.status()
			.unwrap();
		assert!(status.success(), "the start command is valid sh");
	}

	/// Runs [`results_url`] under `set -e` against a route table `routes`
	/// with a stub `curl` exiting `curl_exit`: `(ACTIONS_RESULTS_URL, the
	/// stub's arguments)`.
	fn results_with(routes: Option<&str>, curl_exit: u8) -> (String, String) {
		use std::os::unix::fs::PermissionsExt;

		let dir = tempfile::tempdir().unwrap();
		let route = dir.path().join("route");
		if let Some(routes) = routes {
			std::fs::write(&route, routes).unwrap();
		}
		let log = dir.path().join("curl.log");
		let bin = dir.path().join("bin");
		std::fs::create_dir(&bin).unwrap();
		let curl = bin.join("curl");
		std::fs::write(
			&curl,
			format!("#!/bin/sh\nprintf '%s ' \"$@\" > '{}'\nexit {curl_exit}\n", log.display()),
		)
		.unwrap();
		std::fs::set_permissions(&curl, std::fs::Permissions::from_mode(0o755)).unwrap();
		let script = format!(
			"set -e; {}; printf '%s' \"$ACTIONS_RESULTS_URL\"",
			results_url(route.to_str().unwrap())
		);
		let output = Command::new("sh")
			.arg("-c")
			.arg(&script)
			.env_clear()
			.env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
			.output()
			.unwrap();
		assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
		let url = String::from_utf8_lossy(&output.stdout).into_owned();
		(url, std::fs::read_to_string(&log).unwrap_or_default())
	}

	/// `/proc/net/route` of a guest on 192.168.2.0/24 with gateway
	/// 192.168.2.1 (little-endian hex).
	const ROUTES: &str = concat!(
		"Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n",
		"eth0\t0002A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0\n",
		"eth0\t00000000\t0102A8C0\t0003\t0\t0\t0\t00000000\t0\t0\t0\n",
	);

	#[test]
	fn the_runner_uses_the_cache_at_the_default_gateway() {
		let (url, curl) = results_with(Some(ROUTES), 0);
		assert_eq!(url, "http://192.168.2.1:7948/");
		let ping = "http://192.168.2.1:7948/_vmon/ping";
		assert!(curl.contains("-m 3") && curl.contains(ping), "{curl}");
	}

	#[test]
	fn the_runner_falls_back_to_github_without_the_cache() {
		let (url, curl) = results_with(Some(ROUTES), 7);
		assert_eq!(url, ci_cache::DEFAULT_UPSTREAM, "the ping failed");
		assert!(curl.contains("192.168.2.1"), "{curl}");
		let (url, curl) = results_with(None, 0);
		assert_eq!(url, ci_cache::DEFAULT_UPSTREAM, "no route table");
		assert!(curl.is_empty(), "no probe without a gateway: {curl}");
		let no_default = ROUTES.lines().take(2).collect::<Vec<_>>().join("\n");
		assert_eq!(results_with(Some(&no_default), 0).0, ci_cache::DEFAULT_UPSTREAM);
	}

	#[test]
	fn quoting_keeps_odd_values_inert() {
		let hook = test_hook("a'b/$(touch x)", false, &policy(true, false, &["o'neil"]));
		let (ok, _) = run(&hook, &[
			("GITHUB_REPOSITORY", "a'b/$(touch x)"),
			("GITHUB_ACTOR", "o'neil"),
			("GITHUB_TRIGGERING_ACTOR", "O'Neil"),
		]);
		assert!(ok);
	}
}
