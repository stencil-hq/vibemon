//! Linux host side of TAP networking: create the per-sandbox TAP, address it,
//! and install NAT/forwarding rules plus the lease's connection mark (ported
//! from vmond `net.rs`). Egress policy (FORWARD filter jump, REDIRECT to the
//! egress proxy, INPUT rules) belongs to `crate::egress`.

use std::{
	fmt::Write as _,
	fs,
	io::{self, Write as _},
	process::{Command, Stdio},
};

use anyhow::{Context, bail};

use super::TapLease;

/// Linux capability number of `CAP_NET_ADMIN`.
const CAP_NET_ADMIN: u32 = 12;

/// Whether this process may create TAPs and edit iptables (root or
/// `CAP_NET_ADMIN`).
pub(super) fn has_net_admin() -> bool {
	// SAFETY: geteuid has no preconditions.
	if unsafe { libc::geteuid() } == 0 {
		return true;
	}
	let Ok(status) = fs::read_to_string("/proc/self/status") else {
		return false;
	};
	status
		.lines()
		.find_map(|line| line.strip_prefix("CapEff:"))
		.and_then(|bits| u64::from_str_radix(bits.trim(), 16).ok())
		.is_some_and(|mask| mask & (1 << CAP_NET_ADMIN) != 0)
}

fn run(program: &str, args: &[&str]) -> anyhow::Result<()> {
	let output = Command::new(program)
		.args(args)
		.output()
		.with_context(|| format!("running {program}"))?;
	if output.status.success() {
		return Ok(());
	}
	bail!("{program} {} failed: {}", args.join(" "), String::from_utf8_lossy(&output.stderr).trim())
}

fn succeeds(program: &str, args: &[&str]) -> bool {
	Command::new(program)
		.args(args)
		.output()
		.is_ok_and(|output| output.status.success())
}

fn comment(tap: &str, kind: &str) -> String {
	format!("vmon:{tap}:{kind}")
}

/// iptables rule specs (without the `-A/-I/-D/-C` verb) for one lease:
/// `(table, chain, spec)`.
pub(super) fn rules(lease: &TapLease) -> Vec<(&'static str, &'static str, Vec<String>)> {
	let network = format!("{}/{}", lease.network, lease.prefix);
	let guest = format!("{}/32", lease.guest_ip);
	let mark = lease.conntrack_mark().to_string();
	let to_strings = |items: &[&str]| {
		items
			.iter()
			.map(|item| (*item).to_owned())
			.collect::<Vec<_>>()
	};
	vec![
		(
			"nat",
			"POSTROUTING",
			to_strings(&[
				"-s",
				&network,
				"!",
				"-d",
				&network,
				"-m",
				"comment",
				"--comment",
				&comment(&lease.tap, "masq"),
				"-j",
				"MASQUERADE",
			]),
		),
		(
			"filter",
			"FORWARD",
			to_strings(&[
				"-o",
				&lease.tap,
				"-d",
				&guest,
				"-m",
				"conntrack",
				"--ctstate",
				"ESTABLISHED,RELATED",
				"-m",
				"comment",
				"--comment",
				&comment(&lease.tap, "return"),
				"-j",
				"ACCEPT",
			]),
		),
		(
			"filter",
			"FORWARD",
			to_strings(&[
				"-i",
				&lease.tap,
				"-s",
				&guest,
				"-m",
				"comment",
				"--comment",
				&comment(&lease.tap, "out"),
				"-j",
				"ACCEPT",
			]),
		),
		// Tags every guest flow (forwarded or redirected to the egress proxy)
		// so `conntrack::flush_mark` can drop them when the lease is reused.
		(
			"mangle",
			"PREROUTING",
			to_strings(&[
				"-i",
				&lease.tap,
				"-m",
				"comment",
				"--comment",
				&comment(&lease.tap, "mark"),
				"-j",
				"CONNMARK",
				"--set-mark",
				&mark,
			]),
		),
	]
}

/// Creates and addresses the TAP and installs its NAT/forward rules.
/// `owner_uid` lets a privilege-dropped VMM open the TAP.
pub(super) fn setup(lease: &TapLease, owner_uid: Option<u32>) -> anyhow::Result<()> {
	if !succeeds("ip", &["link", "show", "dev", &lease.tap]) {
		let mut args = vec!["tuntap", "add", "dev", lease.tap.as_str(), "mode", "tap"];
		let uid = owner_uid.map(|uid| uid.to_string());
		if let Some(uid) = &uid {
			args.extend(["user", uid.as_str()]);
		}
		run("ip", &args)?;
	}
	let host_cidr = format!("{}/{}", lease.host_ip, lease.prefix);
	run("ip", &["addr", "replace", &host_cidr, "dev", &lease.tap])?;
	run("ip", &["link", "set", "dev", &lease.tap, "up"])?;
	// IPv6 on the TAP would bypass the IPv4-only policy.
	let _ = fs::write(format!("/proc/sys/net/ipv6/conf/{}/disable_ipv6", lease.tap), "1");
	fs::write("/proc/sys/net/ipv4/ip_forward", "1").context("enabling net.ipv4.ip_forward")?;
	// Append the rules not present yet: one listing per table, one transaction.
	let mut script = String::new();
	for (table, rules) in by_table(lease) {
		let listing = listing(table)?;
		let missing: Vec<String> = rules
			.iter()
			.filter(|(chain, spec)| !listing.lines().any(|line| is_rule(line, chain, spec)))
			.map(|(chain, spec)| format!("-A {chain} {}", spec.join(" ")))
			.collect();
		if !missing.is_empty() {
			let _ = write!(script, "*{table}\n{}\nCOMMIT\n", missing.join("\n"));
		}
	}
	if !script.is_empty() {
		run_with_input("iptables-restore", &["-w", "--noflush"], &script)?;
	}
	Ok(())
}

/// Removes the lease's rules (duplicates a crashed setup left included) and
/// deletes the TAP. Idempotent.
pub(super) fn teardown(lease: &TapLease) -> io::Result<()> {
	let mut script = String::new();
	for (table, rules) in by_table(lease) {
		let listing = listing(table).map_err(io::Error::other)?;
		let deletes: Vec<String> = listing
			.lines()
			.filter(|line| rules.iter().any(|(chain, spec)| is_rule(line, chain, spec)))
			.map(|line| format!("-D{}", unquote(line).trim_start_matches("-A")))
			.collect();
		if !deletes.is_empty() {
			let _ = write!(script, "*{table}\n{}\nCOMMIT\n", deletes.join("\n"));
		}
	}
	if !script.is_empty() {
		run_with_input("iptables-restore", &["-w", "--noflush"], &script)
			.map_err(io::Error::other)?;
	}
	if !succeeds("ip", &["link", "del", "dev", &lease.tap])
		&& succeeds("ip", &["link", "show", "dev", &lease.tap])
	{
		return Err(io::Error::other(format!("deleting tap {}", lease.tap)));
	}
	Ok(())
}

/// Lease indexes of the host's `vmon<N>` interfaces.
pub(super) fn existing() -> Vec<u32> {
	let Ok(entries) = fs::read_dir("/sys/class/net") else {
		return Vec::new();
	};
	entries
		.filter_map(Result::ok)
		.filter_map(|entry| {
			let name = entry.file_name();
			let digits = name.to_str()?.strip_prefix("vmon")?;
			digits
				.bytes()
				.all(|b| b.is_ascii_digit())
				.then(|| digits.parse().ok())
				.flatten()
		})
		.collect()
}

/// One table's rules: `(table, [(chain, spec)])`.
type TableRules = (&'static str, Vec<(&'static str, Vec<String>)>);

/// The lease's rules grouped by table.
fn by_table(lease: &TapLease) -> Vec<TableRules> {
	let mut out: Vec<TableRules> = Vec::new();
	for (table, chain, spec) in rules(lease) {
		match out.iter_mut().find(|(t, _)| *t == table) {
			Some((_, rules)) => rules.push((chain, spec)),
			None => out.push((table, vec![(chain, spec)])),
		}
	}
	out
}

/// `iptables -S` of one table.
fn listing(table: &str) -> anyhow::Result<String> {
	let output = Command::new("iptables")
		.args(["-w", "-t", table, "-S"])
		.output()
		.context("running iptables")?;
	if !output.status.success() {
		bail!("iptables -t {table} -S: {}", String::from_utf8_lossy(&output.stderr).trim());
	}
	Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// `iptables -S` prints quoted comments; restore input and specs use bare
/// tokens (ours contain no spaces).
fn unquote(line: &str) -> String {
	line.replace('"', "")
}

/// Whether an `iptables -S` line is `-A <chain>` carrying `spec`'s comment
/// (comments are unique per lease and rule).
fn is_rule(line: &str, chain: &str, spec: &[String]) -> bool {
	let line = unquote(line);
	let mut tokens = line.split_whitespace();
	let comment = spec
		.windows(2)
		.find(|w| w[0] == "--comment")
		.map(|w| w[1].as_str());
	tokens.next() == Some("-A")
		&& tokens.next() == Some(chain)
		&& comment.is_some_and(|comment| {
			line
				.split_whitespace()
				.collect::<Vec<_>>()
				.windows(2)
				.any(|w| w == ["--comment", comment])
		})
}

/// Runs `program args` with `input` on stdin.
fn run_with_input(program: &str, args: &[&str], input: &str) -> anyhow::Result<()> {
	let mut child = Command::new(program)
		.args(args)
		.stdin(Stdio::piped())
		.stdout(Stdio::null())
		.stderr(Stdio::piped())
		.spawn()
		.with_context(|| format!("running {program}"))?;
	child
		.stdin
		.take()
		.context("stdin")?
		.write_all(input.as_bytes())?;
	let output = child.wait_with_output()?;
	if !output.status.success() {
		bail!("{program}: {}", String::from_utf8_lossy(&output.stderr).trim());
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn listed_rules_match_by_chain_and_lease_comment() {
		let spec: Vec<String> = [
			"-s",
			"172.20.0.4/30",
			"-m",
			"comment",
			"--comment",
			"vmon:vmon1:masq",
			"-j",
			"MASQUERADE",
		]
		.iter()
		.map(ToString::to_string)
		.collect();
		let ours =
			"-A POSTROUTING -s 172.20.0.4/30 -m comment --comment \"vmon:vmon1:masq\" -j MASQUERADE";
		assert!(is_rule(ours, "POSTROUTING", &spec));
		assert!(!is_rule(ours, "FORWARD", &spec));
		let other_lease = ours.replace("vmon1", "vmon10");
		assert!(!is_rule(&other_lease, "POSTROUTING", &spec));
		assert_eq!(
			format!("-D{}", unquote(ours).trim_start_matches("-A")),
			"-D POSTROUTING -s 172.20.0.4/30 -m comment --comment vmon:vmon1:masq -j MASQUERADE"
		);
	}
}
