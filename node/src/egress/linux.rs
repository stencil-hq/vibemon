//! Linux transparent egress: the per-node proxy listener (`SO_ORIGINAL_DST`)
//! and the per-TAP iptables plumbing.
//!
//! Per TAP the node installs (all tagged `-m comment --comment vmon-egress`;
//! pooled TAPs get them when they are created, with closed policy chains, so
//! attaching a sandbox only swaps the chain contents):
//! - nat `PREROUTING -i <tap> -p tcp -j REDIRECT --to-ports <proxy>`: every
//!   guest TCP flow reaches the proxy, which recovers the original destination
//!   and identifies the sandbox by source IP;
//! - filter `INPUT -i <tap>`: ACCEPT tcp to the proxy port, DROP the rest (the
//!   guest never reaches host services);
//! - filter `FORWARD -i <tap> -j vmon-e-<tap>`: UDP/ICMP CIDR policy (`vmon-e-`
//!   = always-denied ranges with node exemptions, then `vmon-u-` = user allow →
//!   RETURN, user deny → DROP). Closed (a single DROP) while no sandbox is
//!   attached; attach inserts the policy above that DROP (add-only), detach
//!   resets them to closed, live updates replace them, each atomically with one
//!   `iptables-restore --noflush`;
//! - ip6tables `INPUT`/`FORWARD -i <tap> -j DROP` (TAP networking is IPv4).

use std::{
	collections::HashMap,
	io::{self, Write as _},
	mem,
	net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
	os::fd::AsRawFd,
	process::{Command, Stdio},
	sync::Arc,
};

use anyhow::{Context as _, bail};
use parking_lot::RwLock;
use tokio::net::TcpListener;

use super::{
	policy::{Cidr, Compiled},
	proxy::{self, Shared},
};

const COMMENT: &str = "vmon-egress";

/// The node's transparent proxy listener and its guest-IP → policy table.
pub struct Proxy {
	port:   u16,
	guests: RwLock<HashMap<Ipv4Addr, Arc<Compiled>>>,
}

impl Proxy {
	/// Bind `0.0.0.0:port` (0 = ephemeral) and serve redirected flows.
	pub async fn start(port: u16, shared: Shared) -> anyhow::Result<Arc<Self>> {
		let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))
			.await
			.with_context(|| format!("binding egress proxy on port {port}"))?;
		let local = listener.local_addr()?;
		let proxy = Arc::new(Self { port: local.port(), guests: RwLock::new(HashMap::new()) });
		let shared = Arc::new(shared);
		let weak = Arc::downgrade(&proxy);
		tokio::spawn(async move {
			loop {
				let (stream, peer) = match listener.accept().await {
					Ok(accepted) => accepted,
					Err(e) => {
						tracing::warn!("egress proxy accept failed: {e}");
						tokio::time::sleep(std::time::Duration::from_millis(50)).await;
						continue;
					},
				};
				let Some(proxy) = weak.upgrade() else { return };
				let IpAddr::V4(guest) = peer.ip().to_canonical() else {
					continue;
				};
				let Some(policy) = proxy.guests.read().get(&guest).cloned() else {
					tracing::debug!(%peer, "egress connection from unknown guest");
					continue;
				};
				let orig = match original_dst(&stream) {
					Ok(orig) if orig.port() != proxy.port || orig.ip() != local.ip() => orig,
					Ok(_) => continue,
					Err(e) => {
						tracing::debug!(%peer, "SO_ORIGINAL_DST failed: {e}");
						continue;
					},
				};
				let _ = stream.set_nodelay(true);
				let shared = shared.clone();
				tokio::spawn(async move {
					if let Err(e) = proxy::handle(stream, orig, policy, &shared).await {
						tracing::debug!(%peer, %orig, "egress flow ended: {e}");
					}
				});
			}
		});
		Ok(proxy)
	}

	/// Listening port (REDIRECT target).
	pub const fn port(&self) -> u16 {
		self.port
	}

	/// Install or replace the policy for flows from `guest`.
	pub fn set(&self, guest: Ipv4Addr, policy: Arc<Compiled>) {
		self.guests.write().insert(guest, policy);
	}

	/// Stop serving flows from `guest`.
	pub fn remove(&self, guest: Ipv4Addr) {
		self.guests.write().remove(&guest);
	}
}

/// Original destination of a `REDIRECT`ed IPv4 connection.
pub fn original_dst(stream: &impl AsRawFd) -> io::Result<SocketAddr> {
	// SAFETY: sockaddr_in is plain old data; all-zero is a valid value.
	let mut addr: libc::sockaddr_in = unsafe { mem::zeroed() };
	let mut len = libc::socklen_t::try_from(mem::size_of::<libc::sockaddr_in>())
		.expect("sockaddr_in size fits socklen_t");
	// SAFETY: the fd is a live socket owned by `stream`; `addr`/`len` point
	// to a writable sockaddr_in of the advertised size.
	let rc = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_IP,
			libc::SO_ORIGINAL_DST,
			(&raw mut addr).cast(),
			&raw mut len,
		)
	};
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(SocketAddr::V4(SocketAddrV4::new(
		Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
		u16::from_be(addr.sin_port),
	)))
}

fn entry_chain(tap: &str) -> String {
	format!("vmon-e-{tap}")
}

fn user_chain(tap: &str) -> String {
	format!("vmon-u-{tap}")
}

fn v4(cidrs: &[Cidr]) -> impl Iterator<Item = String> + '_ {
	cidrs
		.iter()
		.map(ToString::to_string)
		.filter(|cidr| !cidr.contains(':'))
}

/// Rule specs (after `-A <chain>`) of `policy`'s entry and user chains. The
/// entry chain always ends in `-g <user chain>`, so rules after it are
/// unreachable.
fn policy_specs(tap: &str, policy: &Compiled) -> (Vec<String>, Vec<String>) {
	let user = user_chain(tap);
	let entry = std::iter::once("-p tcp -j DROP".to_owned())
		.chain(v4(policy.internal_cidrs()).map(|cidr| format!("-d {cidr} -g {user}")))
		.chain(v4(policy.blocked_cidrs()).map(|cidr| format!("-d {cidr} -j DROP")))
		.chain(std::iter::once(format!("-g {user}")))
		.collect();
	let user = v4(policy.allow_cidrs())
		.map(|cidr| format!("-d {cidr} -j RETURN"))
		.chain(v4(policy.deny_cidrs()).map(|cidr| format!("-d {cidr} -j DROP")))
		.collect();
	(entry, user)
}

/// `iptables-restore --noflush` input that (re)creates both policy chains.
pub fn filter_restore(tap: &str, policy: &Compiled) -> String {
	use std::fmt::Write as _;
	let (entry, user) = (entry_chain(tap), user_chain(tap));
	let (entry_specs, user_specs) = policy_specs(tap, policy);
	let mut out = format!("*filter\n:{entry} - [0:0]\n:{user} - [0:0]\n");
	for spec in entry_specs {
		let _ = writeln!(out, "-A {entry} {spec}");
	}
	for spec in user_specs {
		let _ = writeln!(out, "-A {user} {spec}");
	}
	out.push_str("COMMIT\n");
	out
}

/// `iptables-restore --noflush` input that opens closed chains
/// ([`closed_restore`]) for `policy` by only adding rules: the entry rules go
/// above the closed `DROP` (unreachable behind the final `-g <user chain>`),
/// the user rules into the empty user chain. Commits that delete rules are
/// slow (7–15 ms on a nested-KVM host against ~1.5 ms add-only), so attach
/// only adds and [`close_filter`] pays the deletion after detach.
fn open_restore(tap: &str, policy: &Compiled) -> String {
	use std::fmt::Write as _;
	let (entry, user) = (entry_chain(tap), user_chain(tap));
	let (entry_specs, user_specs) = policy_specs(tap, policy);
	let mut out = String::from("*filter\n");
	for (position, spec) in entry_specs.iter().enumerate() {
		let _ = writeln!(out, "-I {entry} {} {spec}", position + 1);
	}
	for spec in user_specs {
		let _ = writeln!(out, "-A {user} {spec}");
	}
	out.push_str("COMMIT\n");
	out
}

/// `iptables-restore --noflush` input that (re)creates both policy chains
/// closed: everything the guest forwards is dropped until a sandbox attaches.
fn closed_restore(tap: &str) -> String {
	let (entry, user) = (entry_chain(tap), user_chain(tap));
	format!("*filter\n:{entry} - [0:0]\n:{user} - [0:0]\n-A {entry} -j DROP\nCOMMIT\n")
}

/// One iptables invocation: binary plus arguments.
type Rule = (&'static str, Vec<String>);

fn args(parts: &[&str]) -> Vec<String> {
	parts.iter().map(ToString::to_string).collect()
}

/// Jump/redirect rules, in insertion order (`-I`, so the last inserted ends
/// up first: ACCEPT to the proxy lands above the INPUT DROP).
pub fn hook_rules(tap: &str, port: u16) -> Vec<Rule> {
	let port = port.to_string();
	let entry = entry_chain(tap);
	vec![
		(
			"iptables",
			args(&[
				"-t",
				"nat",
				"PREROUTING",
				"-i",
				tap,
				"-p",
				"tcp",
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				"REDIRECT",
				"--to-ports",
				&port,
			]),
		),
		(
			"iptables",
			args(&[
				"-t",
				"filter",
				"INPUT",
				"-i",
				tap,
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				"DROP",
			]),
		),
		(
			"iptables",
			args(&[
				"-t",
				"filter",
				"INPUT",
				"-i",
				tap,
				"-p",
				"tcp",
				"--dport",
				&port,
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				"ACCEPT",
			]),
		),
		(
			"iptables",
			args(&[
				"-t",
				"filter",
				"FORWARD",
				"-i",
				tap,
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				&entry,
			]),
		),
		(
			"ip6tables",
			args(&[
				"-t",
				"filter",
				"INPUT",
				"-i",
				tap,
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				"DROP",
			]),
		),
		(
			"ip6tables",
			args(&[
				"-t",
				"filter",
				"FORWARD",
				"-i",
				tap,
				"-m",
				"comment",
				"--comment",
				COMMENT,
				"-j",
				"DROP",
			]),
		),
	]
}

/// Built-in chains that may hold this module's per-TAP hooks.
const HOOK_CHAINS: [(&str, &str, &str); 5] = [
	("iptables", "nat", "PREROUTING"),
	("iptables", "filter", "INPUT"),
	("iptables", "filter", "FORWARD"),
	("ip6tables", "filter", "INPUT"),
	("ip6tables", "filter", "FORWARD"),
];

/// `-D` argument lists (after `-w -t <table>`) deleting every hook for `tap`
/// in an `iptables -S <chain>` listing, whatever proxy port it targets (a
/// previous node process may have used another ephemeral port).
fn hook_deletes(listing: &str, tap: &str) -> Vec<Vec<String>> {
	listing
		.lines()
		.filter_map(|line| {
			let tokens: Vec<&str> = line.split_whitespace().collect();
			let ours = tokens.first() == Some(&"-A")
				&& tokens.windows(2).any(|w| w == ["-i", tap])
				&& tokens
					.windows(2)
					.any(|w| w[0] == "--comment" && w[1].trim_matches('"') == COMMENT);
			ours.then(|| {
				std::iter::once("-D".to_string())
					.chain(tokens[1..].iter().map(|t| t.trim_matches('"').to_string()))
					.collect()
			})
		})
		.collect()
}

/// Feeds `script` to `<bin> -w --noflush` (`iptables-restore` or
/// `ip6tables-restore`): one process and one atomic transaction. Blocking.
fn restore(bin: &str, script: &str) -> anyhow::Result<()> {
	let mut child = Command::new(bin)
		.args(["-w", "--noflush"])
		.stdin(Stdio::piped())
		.stdout(Stdio::null())
		.stderr(Stdio::piped())
		.spawn()
		.with_context(|| format!("running {bin}"))?;
	let mut stdin = child.stdin.take().with_context(|| format!("{bin} stdin"))?;
	stdin.write_all(script.as_bytes())?;
	drop(stdin);
	let output = child.wait_with_output()?;
	if !output.status.success() {
		bail!("{bin}: {}", String::from_utf8_lossy(&output.stderr).trim());
	}
	Ok(())
}

/// Runs blocking iptables work on the blocking pool.
async fn blocking<T: Send + 'static>(
	work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
	tokio::task::spawn_blocking(work)
		.await
		.context("iptables task panicked")?
}

/// Atomically (re)write the TAP's UDP/ICMP policy chains: one
/// `iptables-restore`, the hooks stay as they are.
pub async fn apply_filter(tap: &str, policy: &Compiled) -> anyhow::Result<()> {
	let (tap, script) = (tap.to_owned(), filter_restore(tap, policy));
	blocking(move || {
		restore("iptables-restore", &script).with_context(|| format!("policy chains of {tap}"))
	})
	.await
}

/// Atomically open the TAP's closed policy chains ([`install_hooks`],
/// [`close_filter`]) for `policy`: one add-only `iptables-restore`.
pub async fn open_filter(tap: &str, policy: &Compiled) -> anyhow::Result<()> {
	let (tap, script) = (tap.to_owned(), open_restore(tap, policy));
	blocking(move || {
		restore("iptables-restore", &script)
			.with_context(|| format!("opening policy chains of {tap}"))
	})
	.await
}

/// Atomically reset the TAP's policy chains to closed (drop everything the
/// guest forwards) once its sandbox detached: one `iptables-restore`.
pub async fn close_filter(tap: &str) -> anyhow::Result<()> {
	let (tap, script) = (tap.to_owned(), closed_restore(tap));
	blocking(move || {
		restore("iptables-restore", &script)
			.with_context(|| format!("closing policy chains of {tap}"))
	})
	.await
}

/// Install the hooks and closed policy chains for a TAP no sandbox uses yet
/// (removing stale ones first); attaching then only needs
/// [`apply_filter`].
pub async fn install_hooks(tap: &str, port: u16) -> anyhow::Result<()> {
	let (tap, chains) = (tap.to_owned(), closed_restore(tap));
	blocking(move || install_with(&tap, port, &chains)).await
}

/// Install the hooks and `policy`'s chains for `tap` (removing stale ones
/// first).
pub async fn install(tap: &str, port: u16, policy: &Compiled) -> anyhow::Result<()> {
	let (tap, chains) = (tap.to_owned(), filter_restore(tap, policy));
	blocking(move || install_with(&tap, port, &chains)).await
}

/// [`remove`], then one `iptables-restore` transaction for the IPv4 policy
/// chains (`chains`, a complete `*filter` section) and hooks, one
/// `ip6tables-restore` for the IPv6 drops (best effort). Blocking.
fn install_with(tap: &str, port: u16, chains: &str) -> anyhow::Result<()> {
	use std::fmt::Write as _;
	remove(tap);
	let (mut filter4, mut nat4, mut filter6) = (String::new(), String::new(), String::new());
	for (bin, rule) in hook_rules(tap, port) {
		// `rule` = `-t <table> <chain> <spec…>`; `-I` keeps the insertion order.
		let section = match (bin, rule[1].as_str()) {
			("iptables", "nat") => &mut nat4,
			("iptables", _) => &mut filter4,
			_ => &mut filter6,
		};
		let _ = writeln!(section, "-I {}", rule[2..].join(" "));
	}
	let chains = chains.strip_suffix("COMMIT\n").unwrap_or(chains);
	restore("iptables-restore", &format!("{chains}{filter4}COMMIT\n*nat\n{nat4}COMMIT\n"))
		.with_context(|| format!("installing egress rules of {tap}"))?;
	if let Err(e) = restore("ip6tables-restore", &format!("*filter\n{filter6}COMMIT\n")) {
		tracing::warn!("egress: {e:#}; IPv6 isolation of {tap} skipped");
	}
	Ok(())
}

/// Tables that may hold this module's per-TAP hooks.
const HOOK_TABLES: [(&str, &str); 3] =
	[("iptables", "nat"), ("iptables", "filter"), ("ip6tables", "filter")];

/// Remove every hook (any proxy port, duplicates included) and chain for
/// `tap`; missing rules are ignored. One listing per table, then one
/// transaction per binary. Blocking; runs only off the request path (TAP
/// teardown, stale-rule cleanup before an install).
pub fn remove(tap: &str) {
	let listings: Vec<_> = HOOK_TABLES
		.iter()
		.filter_map(|&(bin, table)| {
			let output = Command::new(bin)
				.args(["-w", "-t", table, "-S"])
				.stdin(Stdio::null())
				.output()
				.ok()?;
			Some((bin, table, String::from_utf8_lossy(&output.stdout).into_owned()))
		})
		.collect();
	for bin in ["iptables", "ip6tables"] {
		let script = removal_script(
			tap,
			listings
				.iter()
				.filter(|(b, ..)| *b == bin)
				.map(|(_, table, listing)| (*table, listing.as_str())),
		);
		if !script.is_empty()
			&& let Err(e) = restore(&format!("{bin}-restore"), &script)
		{
			tracing::warn!("egress cleanup of {tap}: {e:#}");
		}
	}
}

/// `*-restore` input deleting `tap`'s hooks from the built-in chains of each
/// listed table and flushing/deleting its policy chains (only what the
/// `-S` listings show, so the transaction cannot fail on a missing rule).
fn removal_script<'a>(tap: &str, listings: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
	use std::fmt::Write as _;
	let chains = [entry_chain(tap), user_chain(tap)];
	let mut out = String::new();
	for (table, listing) in listings {
		let mut body: Vec<String> = hook_deletes(listing, tap)
			.into_iter()
			.filter(|delete| {
				HOOK_CHAINS
					.iter()
					.any(|(_, t, chain)| *t == table && delete[1] == *chain)
			})
			.map(|delete| delete.join(" "))
			.collect();
		if table == "filter" {
			let present: Vec<&String> = chains
				.iter()
				.filter(|chain| listing.lines().any(|line| line == format!("-N {chain}")))
				.collect();
			body.extend(present.iter().map(|chain| format!("-F {chain}")));
			body.extend(present.iter().map(|chain| format!("-X {chain}")));
		}
		if !body.is_empty() {
			let _ = write!(out, "*{table}\n{}\nCOMMIT\n", body.join("\n"));
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use tokio::{io::AsyncWriteExt, process::Command};

	use super::*;
	use crate::api::NetworkPolicy;

	fn compiled(allow: &[&str], deny: &[&str], internal: &[&str]) -> Compiled {
		let policy = NetworkPolicy {
			allow_out: allow.iter().map(ToString::to_string).collect(),
			deny_out: deny.iter().map(ToString::to_string).collect(),
			..NetworkPolicy::default()
		};
		let internal: Vec<Cidr> = internal.iter().map(|c| c.parse().expect("cidr")).collect();
		Compiled::compile(&policy, &internal).expect("policy")
	}

	#[test]
	fn restore_orders_exemptions_before_blocks_before_user_rules() {
		let text = filter_restore(
			"tap7",
			&compiled(&["1.2.3.0/24", "2001:db8::/32", "x.io"], &["0.0.0.0/0"], &["10.9.0.0/16"]),
		);
		let lines: Vec<&str> = text.lines().collect();
		let pos = |needle: &str| {
			lines
				.iter()
				.position(|l| *l == needle)
				.unwrap_or_else(|| panic!("missing {needle}\n{text}"))
		};
		assert!(
			pos("-A vmon-e-tap7 -d 10.9.0.0/16 -g vmon-u-tap7")
				< pos("-A vmon-e-tap7 -d 10.0.0.0/8 -j DROP")
		);
		assert!(pos("-A vmon-e-tap7 -d 10.0.0.0/8 -j DROP") < pos("-A vmon-e-tap7 -g vmon-u-tap7"));
		assert!(
			pos("-A vmon-u-tap7 -d 1.2.3.0/24 -j RETURN") < pos("-A vmon-u-tap7 -d 0.0.0.0/0 -j DROP")
		);
		// Domains imply the default nameserver; IPv6 entries are skipped.
		pos("-A vmon-u-tap7 -d 8.8.8.8/32 -j RETURN");
		assert!(!text.contains("2001:db8"));
		assert!(!text.contains("::1"));
		assert!(text.ends_with("COMMIT\n"));
	}

	#[test]
	fn hook_deletes_match_any_port_but_only_our_tap_and_comment() {
		let listing = "-P INPUT ACCEPT\n-A INPUT -i tap1 -p tcp -m tcp --dport 40001 -m comment \
		               --comment vmon-egress -j ACCEPT\n-A INPUT -i tap1 -p tcp -m tcp --dport \
		               39999 -m comment --comment \"vmon-egress\" -j ACCEPT\n-A INPUT -i tap10 -m \
		               comment --comment vmon-egress -j DROP\n-A INPUT -i tap1 -m comment --comment \
		               other -j DROP\n-A INPUT -i tap1 -m comment --comment vmon-egress -j DROP\n";
		let deletes = hook_deletes(listing, "tap1");
		assert_eq!(deletes.len(), 3, "{deletes:?}");
		assert!(deletes.iter().all(|d| d[0] == "-D" && d[1] == "INPUT"));
		assert!(deletes[1].contains(&"39999".to_string()));
		assert!(deletes[1].contains(&"vmon-egress".to_string()), "quotes stripped");
		assert!(deletes.iter().all(|d| !d.contains(&"other".to_string())));
	}
	#[test]
	fn removal_deletes_listed_hooks_and_chains_in_one_transaction_per_table() {
		let filter = "-P INPUT ACCEPT\n-N vmon-e-tap1\n-N vmon-u-tap1\n-N vmon-e-tap10\n-A INPUT -i \
		              tap1 -m comment --comment vmon-egress -j DROP\n-A FORWARD -i tap1 -m comment \
		              --comment \"vmon-egress\" -j vmon-e-tap1\n-A FORWARD -i tap10 -m comment \
		              --comment vmon-egress -j vmon-e-tap10\n-A vmon-e-tap1 -p tcp -j DROP\n";
		let nat = "-P PREROUTING ACCEPT\n";
		let script = removal_script("tap1", [("nat", nat), ("filter", filter)]);
		assert_eq!(
			script,
			"*filter\n-D INPUT -i tap1 -m comment --comment vmon-egress -j DROP\n-D FORWARD -i tap1 \
			 -m comment --comment vmon-egress -j vmon-e-tap1\n-F vmon-e-tap1\n-F vmon-u-tap1\n-X \
			 vmon-e-tap1\n-X vmon-u-tap1\nCOMMIT\n"
		);
		let script = removal_script("tap2", [("nat", nat), ("filter", filter)]);
		assert!(script.is_empty(), "unexpected removal script: {script:?}");
	}

	#[test]
	fn open_inserts_the_policy_above_the_closed_drop_without_deleting() {
		let policy = compiled(&["1.2.3.0/24"], &["0.0.0.0/0"], &[]);
		let open = open_restore("vmon3", &policy);
		let full = filter_restore("vmon3", &policy);
		assert!(
			open
				.lines()
				.all(|l| !l.starts_with(':') && !l.starts_with("-D")),
			"{open}"
		);
		// Same rules, same order: entry rules at positions 1.., user rules
		// appended.
		let normalize = |line: &str| {
			let tokens: Vec<&str> = line.split_whitespace().collect();
			match tokens.as_slice() {
				["-I", chain, position, rest @ ..] if position.parse::<u32>().is_ok() => {
					format!("-A {chain} {}", rest.join(" "))
				},
				_ => line.to_owned(),
			}
		};
		let rules = |text: &str| -> Vec<String> {
			text
				.lines()
				.filter(|l| l.starts_with('-'))
				.map(normalize)
				.collect()
		};
		assert_eq!(rules(&open), rules(&full));
		assert!(open.contains("-I vmon-e-vmon3 1 -p tcp -j DROP\n"));
		let last_entry = open
			.lines()
			.rfind(|l| l.starts_with("-I vmon-e-vmon3"))
			.expect("entry rules");
		assert!(last_entry.ends_with("-g vmon-u-vmon3"), "{last_entry}");
	}

	#[test]
	fn closed_chains_drop_everything() {
		assert_eq!(
			closed_restore("vmon3"),
			"*filter\n:vmon-e-vmon3 - [0:0]\n:vmon-u-vmon3 - [0:0]\n-A vmon-e-vmon3 -j DROP\nCOMMIT\n"
		);
	}

	#[test]
	fn hooks_accept_proxy_port_above_input_drop() {
		let rules = hook_rules("tap0", 15001);
		let drop = rules
			.iter()
			.position(|(bin, r)| {
				*bin == "iptables"
					&& r.contains(&"INPUT".to_string())
					&& r.ends_with(&["DROP".to_string()])
			})
			.expect("drop");
		let accept = rules
			.iter()
			.position(|(_, r)| r.contains(&"15001".to_string()) && r.contains(&"ACCEPT".to_string()))
			.expect("accept");
		// Rules are inserted with -I at the head: later inserts end up first.
		assert!(accept > drop);
		assert_eq!(rules[0].1[..3], ["-t", "nat", "PREROUTING"]);
	}

	/// Privileged end-to-end check (run as root on Linux with
	/// `VMON_EGRESS_NETNS_TEST=1`): a veth "tap" into a guest netns, real
	/// REDIRECT + `SO_ORIGINAL_DST`, CIDR allow/deny and live update.
	#[tokio::test]
	async fn netns_redirect_end_to_end() {
		if std::env::var_os("VMON_EGRESS_NETNS_TEST").is_none() {
			return;
		}
		let sh = |cmd: String| async move {
			let out = Command::new("sh")
				.arg("-c")
				.arg(&cmd)
				.output()
				.await
				.expect("sh");
			(
				out.status.success(),
				String::from_utf8_lossy(&out.stdout).into_owned()
					+ &String::from_utf8_lossy(&out.stderr),
			)
		};
		let (tap, guest_ns) = ("vmonetap0", "vmonegst");
		/// Removes the test's links, namespace, and rules even when an
		/// assertion fails.
		struct Cleanup {
			tap: &'static str,
			ns:  &'static str,
		}
		impl Drop for Cleanup {
			fn drop(&mut self) {
				let output = |bin: &str, args: &[String]| {
					std::process::Command::new(bin).args(args).output().ok()
				};
				let run =
					|bin: &str, args: &[String]| output(bin, args).is_some_and(|o| o.status.success());
				for (bin, table, chain) in HOOK_CHAINS {
					let Some(listing) = output(bin, &args(&["-w", "-t", table, "-S", chain])) else {
						continue;
					};
					for delete in hook_deletes(&String::from_utf8_lossy(&listing.stdout), self.tap) {
						let mut argv = args(&["-w", "-t", table]);
						argv.extend(delete);
						run(bin, &argv);
					}
				}
				for chain in [entry_chain(self.tap), user_chain(self.tap)] {
					run("iptables", &args(&["-w", "-F", &chain]));
					run("iptables", &args(&["-w", "-X", &chain]));
				}
				run("ip", &args(&["netns", "del", self.ns]));
				run("ip", &args(&["link", "del", self.tap]));
				run("ip", &args(&["link", "del", "vmoneorg"]));
			}
		}
		drop(Cleanup { tap, ns: guest_ns });
		let _cleanup = Cleanup { tap, ns: guest_ns };
		for cmd in [
			format!("ip netns add {guest_ns}"),
			format!("ip link add {tap} type veth peer name eth0 netns {guest_ns}"),
			format!("ip addr add 172.31.250.1/30 dev {tap} && ip link set {tap} up"),
			format!(
				"ip -n {guest_ns} addr add 172.31.250.2/30 dev eth0 && ip -n {guest_ns} link set eth0 \
				 up && ip -n {guest_ns} link set lo up"
			),
			format!("ip -n {guest_ns} route add default via 172.31.250.1"),
			// A hook left by a previous node process on another proxy port.
			format!(
				"iptables -w -I INPUT -i {tap} -p tcp --dport 1 -m comment --comment {COMMENT} -j \
				 ACCEPT"
			),
			// Two "public" origins on a dummy interface of the host.
			"ip link add vmoneorg type dummy && ip addr add 198.51.100.1/32 dev vmoneorg && ip addr \
			 add 203.0.113.1/32 dev vmoneorg && ip link set vmoneorg up"
				.to_string(),
		] {
			let (ok, out) = sh(cmd.clone()).await;
			assert!(ok, "{cmd}: {out}");
		}
		for ip in ["198.51.100.1", "203.0.113.1"] {
			let listener = TcpListener::bind((ip, 8080)).await.expect("origin bind");
			tokio::spawn(async move {
				while let Ok((mut s, _)) = listener.accept().await {
					let _ = s.write_all(b"origin-ok\n").await;
				}
			});
		}
		let proxy = Proxy::start(0, Shared::new(None).expect("shared"))
			.await
			.expect("proxy");
		let guest: Ipv4Addr = "172.31.250.2".parse().expect("ip");
		let deny = compiled(&[], &["198.51.100.0/24"], &[]);
		proxy.set(guest, Arc::new(deny.clone()));
		// Pooled path: hooks with closed chains first, then the add-only open.
		install_hooks(tap, proxy.port())
			.await
			.expect("install hooks");
		let (_, closed) = sh(format!("iptables -w -S {}", entry_chain(tap))).await;
		assert!(closed.contains("-j DROP"), "closed chains expected:\n{closed}");
		open_filter(tap, &deny).await.expect("attach policy");
		let (_, open) = sh(format!("iptables -w -S {}", entry_chain(tap))).await;
		let goto = open.find(&format!("-g {}", user_chain(tap))).expect("goto");
		assert!(
			open[goto..]
				.lines()
				.nth(1)
				.is_some_and(|l| l.ends_with("-j DROP")),
			"{open}"
		);
		let (_, input) = sh("iptables -w -S INPUT".to_string()).await;
		assert!(
			!input.contains("--dport 1 "),
			"stale hook from another port survived install:\n{input}"
		);
		let fetch = |ip: &'static str| {
			sh(format!(
				"ip netns exec {guest_ns} sh -c 'exec 3<>/dev/tcp/{ip}/8080 2>/dev/null && timeout 3 \
				 cat <&3' 2>&1 || ip netns exec {guest_ns} timeout 3 nc -w 2 {ip} 8080 </dev/null"
			))
		};
		let (_, out) = fetch("203.0.113.1").await;
		assert!(
			out.contains("origin-ok"),
			"allowed origin must be reachable through the proxy: {out}"
		);
		let (_, out) = fetch("198.51.100.1").await;
		assert!(!out.contains("origin-ok"), "denied origin must be unreachable: {out}");
		// Live update flips the decision.
		let flipped = compiled(&[], &["203.0.113.0/24"], &[]);
		proxy.set(guest, Arc::new(flipped.clone()));
		apply_filter(tap, &flipped).await.expect("update");
		let (_, out) = fetch("198.51.100.1").await;
		assert!(out.contains("origin-ok"), "after update: {out}");
		let (_, out) = fetch("203.0.113.1").await;
		assert!(!out.contains("origin-ok"), "after update: {out}");
		// Detach closes the chains again; the next attach opens them.
		close_filter(tap).await.expect("close");
		let (_, closed) = sh(format!("iptables -w -S {}", entry_chain(tap))).await;
		assert_eq!(closed.lines().filter(|l| l.starts_with("-A")).count(), 1, "{closed}");
		open_filter(tap, &flipped).await.expect("reopen");
		// The guest cannot reach host services on the tap address.
		let (_, out) = sh(format!(
			"ip netns exec {guest_ns} timeout 3 nc -w 2 172.31.250.1 22 </dev/null; echo rc=$?"
		))
		.await;
		assert!(!out.contains("SSH"), "{out}");
		// Domain allow under deny-all over real REDIRECT (needs internet and a
		// working host resolver; `VMON_EGRESS_DOMAIN`, e.g. www.google.com).
		// The guest's chosen IP is a bogus public address: a domain match is
		// re-dialed by name on the host, so it must still work.
		if let Ok(domain) = std::env::var("VMON_EGRESS_DOMAIN") {
			let domains = compiled(&[domain.as_str()], &["0.0.0.0/0"], &[]);
			proxy.set(guest, Arc::new(domains.clone()));
			apply_filter(tap, &domains).await.expect("update");
			let curl = |scheme: &str, host: &str| {
				let port = if scheme == "https" { 443 } else { 80 };
				sh(format!(
					"ip netns exec {guest_ns} curl -sS -m 10 -o /dev/null -w 'code=%{{http_code}}' \
					 --resolve {host}:{port}:198.18.7.7 {scheme}://{host}/ 2>&1"
				))
			};
			for scheme in ["https", "http"] {
				let (_, out) = curl(scheme, &domain).await;
				assert!(
					out.contains("code=2") || out.contains("code=3"),
					"{scheme}://{domain} must pass: {out}"
				);
				let (_, out) = curl(scheme, "example.org").await;
				assert!(out.contains("code=000"), "{scheme}://example.org must be denied: {out}");
			}
		}
		remove(tap);
		let (_, rules) = sh("iptables-save".to_string()).await;
		assert!(!rules.contains(tap), "rules left behind:\n{rules}");
	}
}
