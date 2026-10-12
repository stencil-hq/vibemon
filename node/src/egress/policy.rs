//! E2B egress policy engine (ARCHITECTURE.md §8.4).
//!
//! Pure and platform-independent: the driver calls [`validate`]/[`normalize`]
//! on API input, the Linux transparent proxy calls [`Compiled::decide`] per
//! connection, and macOS compiles the same policy into the VMM user-net
//! filter document ([`Compiled::vmm_filter`]).
//!
//! Precedence (E2B `tcpfirewall.isEgressAllowed` plus the always-denied
//! ranges): allowed domain (HTTP `Host` on :80, TLS SNI on :443; the flow is
//! re-dialed by hostname and the resolved IP must not be private) → private /
//! link-local destination (always denied unless the node exempts it) →
//! allowed CIDR → denied CIDR → default allow.

use std::{
	collections::BTreeMap,
	fmt,
	net::{IpAddr, Ipv4Addr, Ipv6Addr},
	str::FromStr,
};

use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde::{Deserialize, Serialize};

use crate::api::{EgressProxy, NetworkPolicy, NetworkRule};

/// The only accepted "block everything" deny entry (E2B
/// `AllInternetTrafficCIDR`).
pub const ALL_TRAFFIC: &str = "0.0.0.0/0";

/// Resolver implicitly allowed when `allowOut` contains domains (E2B
/// `DefaultNameserver`).
pub const DEFAULT_NAMESERVER: Ipv4Addr = Ipv4Addr::new(8, 8, 8, 8);

/// Destinations sandboxes can never reach unless the node exempts them (E2B
/// `DeniedSandboxCIDRs`).
pub const ALWAYS_DENIED: [&str; 9] = [
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"::1/128",
	"fc00::/7",
	"fe80::/10",
];

const MAX_PROXY_CREDENTIAL_BYTES: usize = 255;
const MAX_SERVER_NAME_BYTES: usize = 253;
const MAX_CA_CERT_BYTES: usize = 8192;
const MAX_DOMAIN_BYTES: usize = 253;

/// An IPv4 or IPv6 network; a bare address parses as a host route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cidr {
	addr:   IpAddr,
	prefix: u8,
}

impl Cidr {
	/// Network containing exactly `ip`.
	pub const fn host(ip: IpAddr) -> Self {
		let prefix = if ip.is_ipv4() { 32 } else { 128 };
		Self { addr: ip, prefix }
	}

	/// Whether `ip` (IPv4-mapped IPv6 is treated as IPv4) lies in this network.
	pub fn contains(&self, ip: IpAddr) -> bool {
		match (self.addr, ip.to_canonical()) {
			(IpAddr::V4(net), IpAddr::V4(ip)) => {
				let mask = u32::MAX
					.checked_shl(32 - u32::from(self.prefix))
					.unwrap_or(0);
				u32::from(net) & mask == u32::from(ip) & mask
			},
			(IpAddr::V6(net), IpAddr::V6(ip)) => {
				let mask = u128::MAX
					.checked_shl(128 - u32::from(self.prefix))
					.unwrap_or(0);
				u128::from(net) & mask == u128::from(ip) & mask
			},
			_ => false,
		}
	}

	/// Whether the written network address is `0.0.0.0` / `::`.
	const fn is_unspecified(&self) -> bool {
		self.addr.is_unspecified()
	}
}

impl FromStr for Cidr {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, ()> {
		let Some((addr, prefix)) = s.split_once('/') else {
			return s.parse::<IpAddr>().map(Self::host).map_err(|_| ());
		};
		let addr = addr.parse::<IpAddr>().map_err(|_| ())?;
		if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
			return Err(());
		}
		let prefix = prefix.parse::<u8>().map_err(|_| ())?;
		let max = if addr.is_ipv4() { 32 } else { 128 };
		if prefix > max {
			return Err(());
		}
		Ok(Self { addr, prefix })
	}
}

impl fmt::Display for Cidr {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}/{}", self.addr, self.prefix)
	}
}

/// An `allowOut` / `rules` domain pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainPattern {
	/// `*`: every hostname.
	Any,
	/// `example.com` (lowercase).
	Exact(String),
	/// `*.example.com`: strict subdomains of the stored suffix (lowercase,
	/// without the `*.`); the apex does not match.
	Subdomains(String),
}

impl DomainPattern {
	/// Parse a pattern; `None` when it is neither `*`, a DNS name, nor `*.` +
	/// DNS name.
	pub fn parse(pattern: &str) -> Option<Self> {
		if pattern == "*" {
			return Some(Self::Any);
		}
		if let Some(suffix) = pattern.strip_prefix("*.") {
			return is_dns_name(suffix).then(|| Self::Subdomains(suffix.to_ascii_lowercase()));
		}
		is_dns_name(pattern).then(|| Self::Exact(pattern.to_ascii_lowercase()))
	}

	/// Case-insensitive match of a hostname taken from SNI / `Host` / a DNS
	/// question.
	pub fn matches(&self, host: &str) -> bool {
		if host.is_empty() || host.starts_with('.') {
			return false;
		}
		match self {
			Self::Any => true,
			Self::Exact(name) => host.eq_ignore_ascii_case(name),
			Self::Subdomains(suffix) => {
				host.len() > suffix.len() + 1
					&& host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
					&& host[host.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
			},
		}
	}

	/// Ordering key: exact names beat longer wildcards beat shorter ones beat
	/// `*`.
	const fn specificity(&self) -> (u8, usize) {
		match self {
			Self::Exact(name) => (2, name.len()),
			Self::Subdomains(suffix) => (1, suffix.len()),
			Self::Any => (0, 0),
		}
	}
}

impl fmt::Display for DomainPattern {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Any => f.write_str("*"),
			Self::Exact(name) => f.write_str(name),
			Self::Subdomains(suffix) => write!(f, "*.{suffix}"),
		}
	}
}

/// Hostname validity used for patterns: 1..=253 bytes of LDH/underscore
/// labels (1..=63 bytes, no leading/trailing hyphen), optional trailing dot
/// rejected so matching stays unambiguous.
fn is_dns_name(name: &str) -> bool {
	if name.is_empty() || name.len() > MAX_DOMAIN_BYTES || name.ends_with('.') {
		return false;
	}
	name.split('.').all(|label| {
		!label.is_empty()
			&& label.len() <= 63
			&& !label.starts_with('-')
			&& !label.ends_with('-')
			&& label
				.bytes()
				.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	})
}

/// One `allowOut` entry after parsing.
enum AllowEntry {
	Cidr(Cidr),
	Domain(DomainPattern),
}

fn parse_allow(entry: &str) -> Result<AllowEntry, String> {
	if let Ok(cidr) = entry.parse::<Cidr>() {
		return Ok(AllowEntry::Cidr(cidr));
	}
	if entry.contains('/') || entry.parse::<IpAddr>().is_ok() {
		return Err(format!("invalid allowed CIDR {entry}"));
	}
	DomainPattern::parse(entry)
		.map(AllowEntry::Domain)
		.ok_or_else(|| format!("invalid allowed domain {entry}"))
}

/// E2B `IsSpecifiedIPOrCIDR`: IP or CIDR whose address is not unspecified,
/// except the literal `0.0.0.0/0`.
fn parse_deny(entry: &str) -> Result<Cidr, String> {
	let cidr = entry
		.parse::<Cidr>()
		.map_err(|()| format!("invalid denied CIDR {entry}"))?;
	if cidr.is_unspecified() && entry != ALL_TRAFFIC {
		return Err(format!(
			"invalid denied CIDR {entry}: only {ALL_TRAFFIC} may use an unspecified address"
		));
	}
	Ok(cidr)
}

fn parse_rule_key(key: &str) -> Result<DomainPattern, String> {
	match DomainPattern::parse(key) {
		Some(DomainPattern::Any) | None => Err(format!(
			"invalid rules domain {key}: expected a DNS name or a single leading wildcard like \
			 *.example.com"
		)),
		Some(pattern) => Ok(pattern),
	}
}

fn validate_rules(rules: &BTreeMap<String, Vec<NetworkRule>>) -> Result<(), String> {
	for (key, list) in rules {
		parse_rule_key(key)?;
		for rule in list {
			for (name, value) in &rule.headers {
				http::HeaderName::from_bytes(name.as_bytes())
					.map_err(|_| format!("invalid header name {name:?} in rules for {key}"))?;
				http::HeaderValue::from_str(value)
					.map_err(|_| format!("invalid value for header {name} in rules for {key}"))?;
			}
		}
	}
	Ok(())
}

/// Split `host:port` / `[v6]:port` into its parts.
pub fn split_host_port(address: &str) -> Option<(&str, u16)> {
	let (host, port) = address.rsplit_once(':')?;
	let host = host
		.strip_prefix('[')
		.and_then(|h| h.strip_suffix(']'))
		.unwrap_or(host);
	if host.is_empty() || host.contains(':') && host.parse::<Ipv6Addr>().is_err() {
		return None;
	}
	let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;
	Some((host, port))
}

fn validate_proxy(proxy: &EgressProxy) -> Result<(), String> {
	if split_host_port(&proxy.address).is_none() {
		return Err(format!("invalid egressProxy.address {:?}: expected host:port", proxy.address));
	}
	for (field, value) in [("username", &proxy.username), ("password", &proxy.password)] {
		if value
			.as_ref()
			.is_some_and(|value| value.len() > MAX_PROXY_CREDENTIAL_BYTES)
		{
			return Err(format!("egressProxy.{field} exceeds {MAX_PROXY_CREDENTIAL_BYTES} bytes"));
		}
	}
	if proxy.password.is_some() && proxy.username.is_none() {
		return Err("egressProxy.password requires egressProxy.username".to_string());
	}
	let Some(tls) = &proxy.tls else {
		return Ok(());
	};
	if !tls.enabled {
		if tls.server_name.is_some() || tls.ca_cert.is_some() {
			return Err("egressProxy.tls fields require tls.enabled=true".to_string());
		}
		return Ok(());
	}
	if let Some(name) = &tls.server_name
		&& (name.is_empty() || name.len() > MAX_SERVER_NAME_BYTES)
	{
		return Err(format!("egressProxy.tls.serverName must be 1..={MAX_SERVER_NAME_BYTES} bytes"));
	}
	if let Some(pem) = &tls.ca_cert {
		if pem.len() > MAX_CA_CERT_BYTES {
			return Err(format!("egressProxy.tls.caCert exceeds {MAX_CA_CERT_BYTES} bytes"));
		}
		parse_pem_certs(pem).map_err(|e| format!("invalid egressProxy.tls.caCert: {e}"))?;
	}
	Ok(())
}

/// Parse one or more PEM certificates; errors when none are present.
pub fn parse_pem_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
	let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| e.to_string())?;
	if certs.is_empty() {
		return Err("no PEM certificate found".to_string());
	}
	Ok(certs)
}

/// Validate an already-folded policy (`allow_internet_access=false` →
/// `deny_out=["0.0.0.0/0"]`) against E2B's create/update rules. Called by
/// the driver before persisting and by the node before enforcing.
pub fn validate(policy: &NetworkPolicy) -> Result<(), String> {
	for entry in &policy.deny_out {
		parse_deny(entry)?;
	}
	let mut has_domains = false;
	for entry in &policy.allow_out {
		has_domains |= matches!(parse_allow(entry)?, AllowEntry::Domain(_));
	}
	if has_domains && !policy.deny_out.iter().any(|entry| entry == ALL_TRAFFIC) {
		return Err(format!(
			"allowOut domains require denyOut to contain {ALL_TRAFFIC} (block all other traffic)"
		));
	}
	validate_rules(&policy.rules)?;
	policy.egress_proxy.as_ref().map_or(Ok(()), validate_proxy)
}

/// Validate, then canonicalize the policy for storage.
///
/// Bare IPs become `/32` / `/128`, domain patterns and rule keys are
/// lowercased, duplicate allow/deny entries are dropped (first occurrence
/// kept), and rule lists of keys that collide after lowercasing are
/// concatenated.
pub fn normalize(policy: NetworkPolicy) -> Result<NetworkPolicy, String> {
	validate(&policy)?;
	let mut allow_out = Vec::with_capacity(policy.allow_out.len());
	for entry in &policy.allow_out {
		let entry = match parse_allow(entry)? {
			AllowEntry::Cidr(cidr) => cidr.to_string(),
			AllowEntry::Domain(pattern) => pattern.to_string(),
		};
		if !allow_out.contains(&entry) {
			allow_out.push(entry);
		}
	}
	let mut deny_out = Vec::with_capacity(policy.deny_out.len());
	for entry in &policy.deny_out {
		let entry = parse_deny(entry)?.to_string();
		if !deny_out.contains(&entry) {
			deny_out.push(entry);
		}
	}
	let mut rules: BTreeMap<String, Vec<NetworkRule>> = BTreeMap::new();
	for (key, list) in policy.rules {
		rules
			.entry(parse_rule_key(&key)?.to_string())
			.or_default()
			.extend(list);
	}
	Ok(NetworkPolicy { allow_out, deny_out, egress_proxy: policy.egress_proxy, rules })
}

/// Parse operator-configured internal CIDRs exempt from [`ALWAYS_DENIED`].
pub fn parse_internal(entries: &[String]) -> Result<Vec<Cidr>, String> {
	entries
		.iter()
		.map(|entry| {
			entry
				.parse::<Cidr>()
				.map_err(|()| format!("invalid internal CIDR {entry}"))
		})
		.collect()
}

/// Outcome of [`Compiled::decide`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
	/// Allowed by a domain rule: re-dial by hostname; a resolved address must
	/// pass [`Compiled::resolved_ip_allowed`].
	Domain,
	/// Allowed by CIDR or by default: dial the original destination.
	Allow,
	/// Blocked.
	Deny,
}

/// A policy compiled for enforcement on one node.
#[derive(Clone, Debug)]
pub struct Compiled {
	domains:  Vec<DomainPattern>,
	allow:    Vec<Cidr>,
	deny:     Vec<Cidr>,
	blocked:  Vec<Cidr>,
	internal: Vec<Cidr>,
	/// Header rules, most specific pattern first.
	rules:    Vec<(DomainPattern, Vec<(String, String)>)>,
	proxy:    Option<EgressProxy>,
}

impl Compiled {
	/// Compile a policy; `internal` are node-exempted private ranges. Adds
	/// [`DEFAULT_NAMESERVER`] to the allowed CIDRs when domains are present.
	pub fn compile(policy: &NetworkPolicy, internal: &[Cidr]) -> Result<Self, String> {
		validate(policy)?;
		let mut domains = Vec::new();
		let mut allow = Vec::new();
		for entry in &policy.allow_out {
			match parse_allow(entry)? {
				AllowEntry::Cidr(cidr) => allow.push(cidr),
				AllowEntry::Domain(pattern) => domains.push(pattern),
			}
		}
		if !domains.is_empty() {
			allow.push(Cidr::host(IpAddr::V4(DEFAULT_NAMESERVER)));
		}
		let deny = policy
			.deny_out
			.iter()
			.map(|entry| parse_deny(entry))
			.collect::<Result<_, _>>()?;
		let blocked = ALWAYS_DENIED
			.iter()
			.map(|entry| {
				entry
					.parse::<Cidr>()
					.expect("ALWAYS_DENIED entries are valid CIDRs")
			})
			.collect();
		let mut rules = Vec::with_capacity(policy.rules.len());
		for (key, list) in &policy.rules {
			let mut headers: Vec<(String, String)> = Vec::new();
			for (name, value) in list.iter().flat_map(|rule| &rule.headers) {
				headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
				headers.push((name.clone(), value.clone()));
			}
			rules.push((parse_rule_key(key)?, headers));
		}
		rules.sort_by_key(|(pattern, _)| std::cmp::Reverse(pattern.specificity()));
		Ok(Self {
			domains,
			allow,
			deny,
			blocked,
			internal: internal.to_vec(),
			rules,
			proxy: policy.egress_proxy.clone(),
		})
	}

	/// Whether `ip` is a private/link-local destination the node does not
	/// exempt.
	pub fn is_blocked(&self, ip: IpAddr) -> bool {
		self.blocked.iter().any(|cidr| cidr.contains(ip))
			&& !self.internal.iter().any(|cidr| cidr.contains(ip))
	}

	/// Whether `host` matches an allowed domain pattern.
	pub fn domain_allowed(&self, host: &str) -> bool {
		self.domains.iter().any(|pattern| pattern.matches(host))
	}

	/// Decide a TCP flow to `ip` whose sniffed hostname (only from :80 Host /
	/// :443 SNI) is `host`.
	pub fn decide(&self, host: Option<&str>, ip: IpAddr) -> Decision {
		if host.is_some_and(|host| self.domain_allowed(host)) {
			return Decision::Domain;
		}
		if self.is_blocked(ip) {
			return Decision::Deny;
		}
		if self.allow.iter().any(|cidr| cidr.contains(ip)) {
			return Decision::Allow;
		}
		if self.deny.iter().any(|cidr| cidr.contains(ip)) {
			return Decision::Deny;
		}
		Decision::Allow
	}

	/// Check for an address a domain-allowed hostname resolved to.
	pub fn resolved_ip_allowed(&self, ip: IpAddr) -> bool {
		!self.is_blocked(ip)
	}

	/// Whether hostname sniffing can change any decision or transform.
	pub const fn wants_hostname(&self) -> bool {
		!self.domains.is_empty() || !self.rules.is_empty() || self.proxy.is_some()
	}

	/// Header overrides for HTTPS requests to `host`, most specific rule key
	/// wins.
	pub fn rules_for(&self, host: &str) -> Option<&[(String, String)]> {
		self
			.rules
			.iter()
			.find(|(pattern, _)| pattern.matches(host))
			.map(|(_, headers)| headers.as_slice())
	}

	/// SOCKS5 upstream for allowed flows, if configured.
	pub const fn egress_proxy(&self) -> Option<&EgressProxy> {
		self.proxy.as_ref()
	}

	/// Allowed CIDRs including the implicit nameserver (UDP/ICMP filter input).
	pub fn allow_cidrs(&self) -> &[Cidr] {
		&self.allow
	}

	/// Denied CIDRs (UDP/ICMP filter input).
	pub fn deny_cidrs(&self) -> &[Cidr] {
		&self.deny
	}

	/// Always-denied ranges (UDP/ICMP filter input).
	pub fn blocked_cidrs(&self) -> &[Cidr] {
		&self.blocked
	}

	/// Node-exempted internal ranges.
	pub fn internal_cidrs(&self) -> &[Cidr] {
		&self.internal
	}

	/// The document passed to `vmon vmm --user-net-policy` / `set_net_policy`.
	pub fn vmm_filter(&self) -> VmmFilter {
		let strings = |cidrs: &[Cidr]| cidrs.iter().map(ToString::to_string).collect();
		VmmFilter {
			blocked: strings(&self.blocked),
			exempt:  strings(&self.internal),
			allow:   strings(&self.allow),
			deny:    strings(&self.deny),
			domains: self.domains.iter().map(ToString::to_string).collect(),
			dns:     self.decide(None, IpAddr::V4(DEFAULT_NAMESERVER)) != Decision::Deny,
		}
	}
}

/// VMM user-net filter document (schema owned by
/// `vmm/src/virtio/net/user/filter.rs`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmmFilter {
	/// Always-denied destinations unless in `exempt`.
	pub blocked: Vec<String>,
	pub exempt:  Vec<String>,
	pub allow:   Vec<String>,
	pub deny:    Vec<String>,
	/// Domain patterns learned from virtual-DNS answers, admitted on TCP 80/443.
	pub domains: Vec<String>,
	/// Whether the slirp virtual DNS (10.0.2.3:53) is reachable.
	pub dns:     bool,
}

#[cfg(test)]
mod tests {
	use super::*;

	fn policy(allow: &[&str], deny: &[&str]) -> NetworkPolicy {
		NetworkPolicy {
			allow_out: allow.iter().map(ToString::to_string).collect(),
			deny_out: deny.iter().map(ToString::to_string).collect(),
			..NetworkPolicy::default()
		}
	}

	fn compiled(allow: &[&str], deny: &[&str]) -> Compiled {
		Compiled::compile(&policy(allow, deny), &[]).expect("valid policy")
	}

	fn ip(s: &str) -> IpAddr {
		s.parse().expect("ip")
	}

	#[test]
	fn precedence_table() {
		let p = compiled(&["1.1.1.0/24", "api.example.com", "*.cdn.net"], &["0.0.0.0/0"]);
		// Allowed CIDR beats the deny-all.
		assert_eq!(p.decide(None, ip("1.1.1.9")), Decision::Allow);
		// Denied by the deny-all.
		assert_eq!(p.decide(None, ip("9.9.9.9")), Decision::Deny);
		// Domain match beats deny and ignores the original IP (even private).
		assert_eq!(p.decide(Some("API.Example.com"), ip("9.9.9.9")), Decision::Domain);
		assert_eq!(p.decide(Some("api.example.com"), ip("10.0.0.1")), Decision::Domain);
		// Non-matching hostname falls through to CIDR rules.
		assert_eq!(p.decide(Some("evil.com"), ip("1.1.1.1")), Decision::Allow);
		assert_eq!(p.decide(Some("evil.com"), ip("9.9.9.9")), Decision::Deny);
		// Domains imply the default nameserver.
		assert_eq!(p.decide(None, IpAddr::V4(DEFAULT_NAMESERVER)), Decision::Allow);
		// Default allow without deny rules.
		assert_eq!(compiled(&[], &[]).decide(None, ip("9.9.9.9")), Decision::Allow);
		// Deny without allow.
		let p = compiled(&[], &["9.9.0.0/16"]);
		assert_eq!(p.decide(None, ip("9.9.1.1")), Decision::Deny);
		assert_eq!(p.decide(None, ip("9.8.1.1")), Decision::Allow);
		assert_eq!(p.decide(None, IpAddr::V4(DEFAULT_NAMESERVER)), Decision::Allow);
	}

	#[test]
	fn private_ranges_are_always_denied_unless_exempt() {
		let p = compiled(&["10.0.0.0/8", "0.0.0.0/0"], &[]);
		for target in [
			"10.1.2.3",
			"100.64.0.1",
			"127.0.0.1",
			"169.254.169.254",
			"172.31.255.255",
			"192.168.1.1",
			"::1",
			"fd00::1",
			"fe80::1",
			"::ffff:10.0.0.1",
		] {
			assert_eq!(p.decide(None, ip(target)), Decision::Deny, "{target}");
		}
		for target in ["172.32.0.1", "100.128.0.1", "11.0.0.1", "2001:db8::1"] {
			assert_eq!(p.decide(None, ip(target)), Decision::Allow, "{target}");
		}
		let exempt = Compiled::compile(&policy(&[], &[]), &["10.9.0.0/16".parse().expect("cidr")])
			.expect("policy");
		assert_eq!(exempt.decide(None, ip("10.9.1.1")), Decision::Allow);
		assert_eq!(exempt.decide(None, ip("10.8.1.1")), Decision::Deny);
		// An exempted range is still subject to user deny rules.
		let exempt =
			Compiled::compile(&policy(&[], &["0.0.0.0/0"]), &["10.9.0.0/16".parse().expect("cidr")])
				.expect("policy");
		assert_eq!(exempt.decide(None, ip("10.9.1.1")), Decision::Deny);
		// A domain flow re-dialed by name must not land on a private address.
		assert!(!p.resolved_ip_allowed(ip("192.168.0.10")));
		assert!(p.resolved_ip_allowed(ip("93.184.216.34")));
	}

	#[test]
	fn ipv6_cidrs() {
		let p = compiled(&["2001:db8::/32"], &["2000::/3"]);
		assert_eq!(p.decide(None, ip("2001:db8:1::1")), Decision::Allow);
		assert_eq!(p.decide(None, ip("2a00::1")), Decision::Deny);
		assert_eq!(p.decide(None, ip("1.2.3.4")), Decision::Allow);
		assert!("::/0".parse::<Cidr>().expect("cidr").contains(ip("::2")));
		assert!(
			!"::/0"
				.parse::<Cidr>()
				.expect("cidr")
				.contains(ip("1.2.3.4"))
		);
	}

	#[test]
	fn wildcard_matching() {
		let sub = DomainPattern::parse("*.Example.com").expect("pattern");
		assert!(sub.matches("a.example.com"));
		assert!(sub.matches("A.B.EXAMPLE.COM"));
		assert!(!sub.matches("example.com"));
		assert!(!sub.matches("badexample.com"));
		assert!(!sub.matches(".example.com"));
		let exact = DomainPattern::parse("example.com").expect("pattern");
		assert!(exact.matches("EXAMPLE.com"));
		assert!(!exact.matches("a.example.com"));
		assert!(DomainPattern::Any.matches("anything.io"));
		assert!(!DomainPattern::Any.matches(""));
		assert_eq!(DomainPattern::parse("*.*.example.com"), None);
		assert_eq!(DomainPattern::parse("exa mple.com"), None);
		assert_eq!(DomainPattern::parse("example.com."), None);
	}

	#[test]
	fn validation_errors() {
		let err = |p: NetworkPolicy| validate(&p).expect_err("must be rejected");
		assert!(err(policy(&[], &["example.com"])).contains("invalid denied CIDR"));
		assert!(err(policy(&[], &["0.0.0.0"])).contains("invalid denied CIDR"));
		assert!(err(policy(&[], &["0.0.0.0/8"])).contains("invalid denied CIDR"));
		assert!(err(policy(&[], &["1.2.3.4/33"])).contains("invalid denied CIDR"));
		assert!(err(policy(&["example.com"], &[])).contains("0.0.0.0/0"));
		assert!(err(policy(&["example.com"], &["1.0.0.0/8"])).contains("0.0.0.0/0"));
		assert!(err(policy(&["1.2.3.4/40"], &[])).contains("invalid allowed CIDR"));
		assert!(err(policy(&["bad domain"], &[])).contains("invalid allowed domain"));
		validate(&policy(&["example.com", "*.x.io", "*", "1.2.3.4"], &["0.0.0.0/0", "5.6.7.8"]))
			.expect("valid");

		let mut p = policy(&[], &[]);
		p.rules
			.insert("*".to_string(), vec![NetworkRule::default()]);
		assert!(err(p.clone()).contains("invalid rules domain"));
		p.rules.clear();
		p.rules.insert("api.x.io".to_string(), vec![NetworkRule {
			headers: BTreeMap::from([("bad header".to_string(), "v".to_string())]),
		}]);
		assert!(err(p.clone()).contains("invalid header name"));
		p.rules.clear();
		p.rules.insert("api.x.io".to_string(), vec![NetworkRule {
			headers: BTreeMap::from([("x-a".to_string(), "v\r\nInjected: 1".to_string())]),
		}]);
		assert!(err(p).contains("invalid value"));

		let proxy = |address: &str, tls: Option<crate::api::EgressProxyTls>| NetworkPolicy {
			egress_proxy: Some(EgressProxy {
				address: address.to_string(),
				username: None,
				password: None,
				tls,
			}),
			..NetworkPolicy::default()
		};
		assert!(err(proxy("proxy.example.com", None)).contains("host:port"));
		assert!(err(proxy("proxy.example.com:0", None)).contains("host:port"));
		validate(&proxy("[2001:db8::1]:1080", None)).expect("v6 proxy");
		let tls = |enabled: bool, server_name: Option<&str>, ca_cert: Option<&str>| {
			Some(crate::api::EgressProxyTls {
				enabled,
				server_name: server_name.map(ToString::to_string),
				ca_cert: ca_cert.map(ToString::to_string),
			})
		};
		assert!(err(proxy("p:1080", tls(false, Some("p"), None))).contains("tls.enabled"));
		assert!(err(proxy("p:1080", tls(true, None, Some("not pem")))).contains("caCert"));
		validate(&proxy("p:1080", tls(true, Some("p.internal"), None))).expect("tls proxy");
	}

	#[test]
	fn normalize_canonicalizes() {
		let mut p = policy(&["8.8.4.4", "Example.COM", "8.8.4.4/32", "2001:db8::1"], &[
			"0.0.0.0/0",
			"1.2.3.4",
		]);
		p.rules
			.insert("API.x.io".to_string(), vec![NetworkRule::default()]);
		p.rules
			.insert("api.x.io".to_string(), vec![NetworkRule::default()]);
		let n = normalize(p).expect("valid");
		assert_eq!(n.allow_out, ["8.8.4.4/32", "example.com", "2001:db8::1/128"]);
		assert_eq!(n.deny_out, ["0.0.0.0/0", "1.2.3.4/32"]);
		assert_eq!(n.rules.len(), 1);
		assert_eq!(n.rules["api.x.io"].len(), 2);
	}

	#[test]
	fn rules_prefer_most_specific_key_and_later_headers_override() {
		let mut p = policy(&[], &[]);
		let rule = |pairs: &[(&str, &str)]| NetworkRule {
			headers: pairs
				.iter()
				.map(|(k, v)| ((*k).to_string(), (*v).to_string()))
				.collect(),
		};
		p.rules
			.insert("*.x.io".to_string(), vec![rule(&[("x-scope", "wild")])]);
		p.rules
			.insert("*.api.x.io".to_string(), vec![rule(&[("x-scope", "api")])]);
		p.rules.insert("v1.api.x.io".to_string(), vec![
			rule(&[("X-Scope", "first")]),
			rule(&[("x-scope", "exact")]),
		]);
		let c = Compiled::compile(&p, &[]).expect("policy");
		assert_eq!(
			c.rules_for("v1.api.x.io"),
			Some(&[("x-scope".to_string(), "exact".to_string())][..])
		);
		assert_eq!(c.rules_for("v2.api.x.io").expect("rule")[0].1, "api");
		assert_eq!(c.rules_for("www.x.io").expect("rule")[0].1, "wild");
		assert_eq!(c.rules_for("x.io"), None);
	}

	#[test]
	fn vmm_filter_reflects_dns_reachability() {
		assert!(compiled(&[], &[]).vmm_filter().dns);
		assert!(!compiled(&[], &["0.0.0.0/0"]).vmm_filter().dns);
		let f = compiled(&["example.com"], &["0.0.0.0/0"]).vmm_filter();
		assert!(f.dns);
		assert_eq!(f.domains, ["example.com"]);
		assert!(f.allow.contains(&"8.8.8.8/32".to_string()));
		assert_eq!(f.blocked.len(), ALWAYS_DENIED.len());
	}

	#[test]
	fn vmm_filter_wire_format_matches_vmm_schema() {
		// `vmm::virtio::net::user::filter::PolicyDoc` denies unknown fields;
		// keep these keys in lockstep with it.
		let json = serde_json::to_value(compiled(&["1.2.3.4"], &[]).vmm_filter()).expect("json");
		let mut keys: Vec<&str> = json
			.as_object()
			.expect("object")
			.keys()
			.map(String::as_str)
			.collect();
		keys.sort_unstable();
		assert_eq!(keys, ["allow", "blocked", "deny", "dns", "domains", "exempt"]);
		assert_eq!(json["allow"], serde_json::json!(["1.2.3.4/32"]));
	}
}
