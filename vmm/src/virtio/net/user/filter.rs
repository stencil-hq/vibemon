//! Egress policy filter for user-mode networking (`--user-net-policy`).
//!
//! Applied to every guest→slirp frame. A new flow is admitted by the first
//! matching rule:
//! 1. DHCP to the slirp server, and the slirp DNS (`10.0.2.3:53`) when `dns` is
//!    set;
//! 2. TCP to ports 80/443 of an address learned from a slirp-DNS answer for a
//!    name matching `domains` (unless it is in `blocked` and not `exempt`);
//! 3. `blocked` (private/link-local ranges, including the slirp gateway that
//!    reaches host loopback) → drop, unless the address is `exempt`;
//! 4. `allow` → pass; 5. `deny` → drop; 6. pass.
//!
//! TCP is filtered on the SYN; later segments of a flow admitted under any
//! earlier policy keep passing, so live updates only affect new connections.
//! UDP and ICMP are judged per packet by destination. Learned addresses
//! expire with their DNS TTL (clamped) and are only honored while a current
//! `domains` pattern still matches the name they were learned for. Answers
//! from other resolvers (including DNS over HTTPS) never unlock domains.
//! Both tables are bounded. The document is produced by the node
//! (`vmon_node::egress::policy::VmmFilter`); the filter is not part of the
//! snapshot, the node passes it again on restore/fork.

use std::{
	collections::{HashMap, HashSet},
	net::{IpAddr, Ipv4Addr, Ipv6Addr},
	time::{Duration, Instant},
};

use serde::Deserialize;

use crate::result::{Result, err};

const ETH_HEADER: usize = 14;
const ETHERTYPE_ARP: u16 = 0x0806;
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_IPV6: u16 = 0x86dd;
const PROTO_TCP: u8 = 6;
const PROTO_UDP: u8 = 17;
const TCP_SYN: u8 = 0x02;
const TCP_RST: u8 = 0x04;
const TCP_ACK: u8 = 0x10;
const DNS_PORT: u16 = 53;
const DHCP_CLIENT_PORT: u16 = 68;
const DHCP_SERVER_PORT: u16 = 67;
const SLIRP_DNS: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 3);
const SLIRP_HOST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 2);
const SLIRP_BROADCAST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 255);
/// Most entries kept in the TCP flow table; idle flows are evicted first.
const MAX_FLOWS: usize = 16_384;
/// Flows idle this long are forgotten (a later segment is re-judged).
const FLOW_IDLE: Duration = Duration::from_hours(2);
/// Most DNS-learned addresses kept; the soonest-expiring are evicted first.
const MAX_LEARNED: usize = 4096;
const MIN_LEARNED_TTL: Duration = Duration::from_mins(1);
const MAX_LEARNED_TTL: Duration = Duration::from_hours(1);

/// JSON document accepted by `--user-net-policy` and `set_net_policy`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDoc {
	blocked: Vec<String>,
	exempt:  Vec<String>,
	allow:   Vec<String>,
	deny:    Vec<String>,
	domains: Vec<String>,
	dns:     bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cidr {
	addr:   IpAddr,
	prefix: u8,
}

impl Cidr {
	fn parse(s: &str) -> Result<Self> {
		let invalid = || err(format!("invalid CIDR {s:?} in user-net policy"));
		let (addr, prefix) = s.split_once('/').ok_or_else(invalid)?;
		let addr: IpAddr = addr.parse().map_err(|_| invalid())?;
		let prefix: u8 = prefix.parse().map_err(|_| invalid())?;
		if prefix > if addr.is_ipv4() { 32 } else { 128 } {
			return Err(invalid());
		}
		Ok(Self { addr, prefix })
	}

	fn contains(&self, ip: IpAddr) -> bool {
		match (self.addr, ip) {
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pattern {
	Any,
	Exact(String),
	/// Strict subdomains of the stored lowercase suffix.
	Subdomains(String),
}

impl Pattern {
	fn parse(s: &str) -> Result<Self> {
		let valid = |name: &str| {
			!name.is_empty()
				&& name.len() <= 253
				&& name.split('.').all(|label| {
					!label.is_empty()
						&& label
							.bytes()
							.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
				})
		};
		if s == "*" {
			return Ok(Self::Any);
		}
		if let Some(suffix) = s.strip_prefix("*.")
			&& valid(suffix)
		{
			return Ok(Self::Subdomains(suffix.to_ascii_lowercase()));
		}
		if valid(s) {
			return Ok(Self::Exact(s.to_ascii_lowercase()));
		}
		Err(err(format!("invalid domain pattern {s:?} in user-net policy")))
	}

	/// `name` is lowercase without a trailing dot.
	fn matches(&self, name: &str) -> bool {
		match self {
			Self::Any => !name.is_empty(),
			Self::Exact(exact) => name == exact,
			Self::Subdomains(suffix) => {
				name.len() > suffix.len() + 1
					&& name.ends_with(suffix.as_str())
					&& name.as_bytes()[name.len() - suffix.len() - 1] == b'.'
			},
		}
	}
}

/// A validated policy document.
#[derive(Clone, Debug)]
pub struct Policy {
	blocked: Vec<Cidr>,
	exempt:  Vec<Cidr>,
	allow:   Vec<Cidr>,
	deny:    Vec<Cidr>,
	domains: Vec<Pattern>,
	dns:     bool,
}

impl Policy {
	/// Parse and validate a policy document.
	pub fn from_json(json: &[u8]) -> Result<Self> {
		let doc: PolicyDoc =
			serde_json::from_slice(json).map_err(|e| err(format!("parsing user-net policy: {e}")))?;
		Self::from_doc(doc)
	}

	/// Parse a policy carried inside a control request.
	pub fn from_value(value: serde_json::Value) -> Result<Self> {
		let doc: PolicyDoc =
			serde_json::from_value(value).map_err(|e| err(format!("parsing user-net policy: {e}")))?;
		Self::from_doc(doc)
	}

	fn from_doc(doc: PolicyDoc) -> Result<Self> {
		let cidrs = |list: &[String]| {
			list
				.iter()
				.map(|s| Cidr::parse(s))
				.collect::<Result<Vec<_>>>()
		};
		Ok(Self {
			blocked: cidrs(&doc.blocked)?,
			exempt:  cidrs(&doc.exempt)?,
			allow:   cidrs(&doc.allow)?,
			deny:    cidrs(&doc.deny)?,
			domains: doc
				.domains
				.iter()
				.map(|s| Pattern::parse(s))
				.collect::<Result<_>>()?,
			dns:     doc.dns,
		})
	}

	fn is_blocked(&self, ip: IpAddr) -> bool {
		self.blocked.iter().any(|c| c.contains(ip)) && !self.exempt.iter().any(|c| c.contains(ip))
	}

	fn domain_matches(&self, name: &str) -> bool {
		self.domains.iter().any(|p| p.matches(name))
	}

	/// CIDR verdict for a destination (rules 3–6).
	fn cidr_allows(&self, ip: IpAddr) -> bool {
		if self.is_blocked(ip) {
			return false;
		}
		if self.allow.iter().any(|c| c.contains(ip)) {
			return true;
		}
		!self.deny.iter().any(|c| c.contains(ip))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FlowKey {
	src:   IpAddr,
	sport: u16,
	dst:   IpAddr,
	dport: u16,
}

/// Live filter state shared by the TX path and the slirp RX path.
#[derive(Debug)]
pub struct Filter {
	policy:  Policy,
	flows:   HashMap<FlowKey, Instant>,
	learned: HashMap<IpAddr, (Instant, String)>,
}

/// Parsed L3/L4 view of one Ethernet frame.
struct Packet {
	src:      IpAddr,
	dst:      IpAddr,
	protocol: u8,
	/// `(source port, destination port, TCP flags)` when the L4 header is
	/// present (first fragment).
	ports:    Option<(u16, u16, u8)>,
	payload:  usize,
}

fn parse(frame: &[u8]) -> Option<Packet> {
	if frame.len() < ETH_HEADER {
		return None;
	}
	let l3 = &frame[ETH_HEADER..];
	let (src, dst, protocol, l4, first_fragment) = match u16::from_be_bytes([frame[12], frame[13]]) {
		ETHERTYPE_IPV4 => {
			if l3.len() < 20 || l3[0] >> 4 != 4 {
				return None;
			}
			let ihl = usize::from(l3[0] & 0x0f) * 4;
			if ihl < 20 || l3.len() < ihl {
				return None;
			}
			let fragment_offset = u16::from_be_bytes([l3[6], l3[7]]) & 0x1fff;
			let src = IpAddr::V4(Ipv4Addr::new(l3[12], l3[13], l3[14], l3[15]));
			let dst = IpAddr::V4(Ipv4Addr::new(l3[16], l3[17], l3[18], l3[19]));
			(src, dst, l3[9], ETH_HEADER + ihl, fragment_offset == 0)
		},
		ETHERTYPE_IPV6 => {
			if l3.len() < 40 || l3[0] >> 4 != 6 {
				return None;
			}
			let mut src = [0u8; 16];
			let mut dst = [0u8; 16];
			src.copy_from_slice(&l3[8..24]);
			dst.copy_from_slice(&l3[24..40]);
			// Extension headers are not walked: such packets are judged by
			// destination only, like non-first fragments.
			(
				IpAddr::V6(Ipv6Addr::from(src)),
				IpAddr::V6(Ipv6Addr::from(dst)),
				l3[6],
				ETH_HEADER + 40,
				true,
			)
		},
		_ => return None,
	};
	let l4_bytes = &frame[l4.min(frame.len())..];
	let ports = match protocol {
		PROTO_TCP if first_fragment && l4_bytes.len() >= 14 => Some((
			u16::from_be_bytes([l4_bytes[0], l4_bytes[1]]),
			u16::from_be_bytes([l4_bytes[2], l4_bytes[3]]),
			l4_bytes[13],
		)),
		PROTO_UDP if first_fragment && l4_bytes.len() >= 8 => Some((
			u16::from_be_bytes([l4_bytes[0], l4_bytes[1]]),
			u16::from_be_bytes([l4_bytes[2], l4_bytes[3]]),
			0,
		)),
		_ => None,
	};
	Some(Packet {
		src,
		dst,
		protocol,
		ports,
		payload: l4 + if protocol == PROTO_UDP { 8 } else { 0 },
	})
}

impl Filter {
	/// Start filtering with `policy` and empty flow/DNS tables.
	pub fn new(policy: Policy) -> Self {
		Self { policy, flows: HashMap::new(), learned: HashMap::new() }
	}

	/// Replace the policy; established TCP flows and learned addresses are
	/// kept (learned ones still need a matching pattern of the new policy).
	pub fn set_policy(&mut self, policy: Policy) {
		self.policy = policy;
	}

	/// Whether a guest→host frame may enter slirp.
	pub fn admit(&mut self, frame: &[u8], now: Instant) -> bool {
		if frame.len() >= ETH_HEADER && u16::from_be_bytes([frame[12], frame[13]]) == ETHERTYPE_ARP {
			return true;
		}
		let Some(packet) = parse(frame) else {
			return false;
		};
		if self.slirp_service(&packet) {
			return true;
		}
		match (packet.protocol, packet.ports) {
			(PROTO_TCP, Some((sport, dport, flags))) => {
				let key = FlowKey { src: packet.src, sport, dst: packet.dst, dport };
				let new_connection = flags & TCP_SYN != 0 && flags & TCP_ACK == 0;
				if !new_connection && let Some(seen) = self.flows.get_mut(&key) {
					*seen = now;
					if flags & TCP_RST != 0 {
						self.flows.remove(&key);
					}
					return true;
				}
				let allowed =
					self.learned_allows(packet.dst, dport, now) || self.policy.cidr_allows(packet.dst);
				if allowed && flags & TCP_RST == 0 {
					self.remember_flow(key, now);
				}
				allowed
			},
			_ => self.policy.cidr_allows(packet.dst),
		}
	}

	/// DHCP and (when enabled) the slirp resolver.
	fn slirp_service(&self, packet: &Packet) -> bool {
		let IpAddr::V4(dst) = packet.dst else {
			return false;
		};
		match (packet.protocol, packet.ports) {
			(PROTO_UDP, Some((DHCP_CLIENT_PORT, DHCP_SERVER_PORT, _))) => {
				dst == Ipv4Addr::BROADCAST || dst == SLIRP_BROADCAST || dst == SLIRP_HOST
			},
			(PROTO_UDP | PROTO_TCP, Some((_, DNS_PORT, _))) => self.policy.dns && dst == SLIRP_DNS,
			_ => false,
		}
	}

	fn learned_allows(&self, ip: IpAddr, port: u16, now: Instant) -> bool {
		matches!(port, 80 | 443)
			&& !self.policy.is_blocked(ip)
			&& self
				.learned
				.get(&ip)
				.is_some_and(|(expires, name)| *expires > now && self.policy.domain_matches(name))
	}

	fn remember_flow(&mut self, key: FlowKey, now: Instant) {
		if self.flows.len() >= MAX_FLOWS && !self.flows.contains_key(&key) {
			self
				.flows
				.retain(|_, seen| now.duration_since(*seen) < FLOW_IDLE);
			if self.flows.len() >= MAX_FLOWS
				&& let Some(oldest) = self
					.flows
					.iter()
					.min_by_key(|(_, seen)| **seen)
					.map(|(k, _)| *k)
			{
				self.flows.remove(&oldest);
			}
		}
		self.flows.insert(key, now);
	}

	/// Learn addresses from a slirp→guest frame carrying a slirp-DNS answer.
	pub fn observe(&mut self, frame: &[u8], now: Instant) {
		if self.policy.domains.is_empty() {
			return;
		}
		let Some(packet) = parse(frame) else {
			return;
		};
		if packet.protocol != PROTO_UDP
			|| packet.src != IpAddr::V4(SLIRP_DNS)
			|| !matches!(packet.ports, Some((DNS_PORT, _, _)))
		{
			return;
		}
		let Some(answer) = frame.get(packet.payload..) else {
			return;
		};
		for (ip, name, ttl) in dns_answers(answer, |name| self.policy.domain_matches(name)) {
			let expires =
				now + Duration::from_secs(u64::from(ttl)).clamp(MIN_LEARNED_TTL, MAX_LEARNED_TTL);
			self.learn(ip, name, expires, now);
		}
	}

	fn learn(&mut self, ip: IpAddr, name: String, expires: Instant, now: Instant) {
		if self.learned.len() >= MAX_LEARNED && !self.learned.contains_key(&ip) {
			self.learned.retain(|_, (expiry, _)| *expiry > now);
			if self.learned.len() >= MAX_LEARNED
				&& let Some(soonest) = self
					.learned
					.iter()
					.min_by_key(|(_, (expiry, _))| *expiry)
					.map(|(ip, _)| *ip)
			{
				self.learned.remove(&soonest);
			}
		}
		self.learned.insert(ip, (expires, name));
	}
}

/// Byte reader for DNS messages with bounds-checked access.
struct Dns<'a> {
	msg: &'a [u8],
	pos: usize,
}

impl Dns<'_> {
	fn u16(&mut self) -> Option<u16> {
		let bytes = self.msg.get(self.pos..self.pos + 2)?;
		self.pos += 2;
		Some(u16::from_be_bytes([bytes[0], bytes[1]]))
	}

	fn u32(&mut self) -> Option<u32> {
		let bytes = self.msg.get(self.pos..self.pos + 4)?;
		self.pos += 4;
		Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
	}

	/// Read a (possibly compressed) name at the cursor, lowercased.
	fn name(&mut self) -> Option<String> {
		let (name, end) = read_name(self.msg, self.pos)?;
		self.pos = end;
		Some(name)
	}
}

/// Decode the name at `pos`; returns it and the offset after its in-place
/// encoding. Compression pointers are followed with a hop limit.
fn read_name(msg: &[u8], mut pos: usize) -> Option<(String, usize)> {
	let mut name = String::new();
	let mut end = None;
	for _ in 0..128 {
		let len = *msg.get(pos)?;
		match len >> 6 {
			0b00 if len == 0 => {
				return Some((name, end.unwrap_or(pos + 1)));
			},
			0b00 => {
				let label = msg.get(pos + 1..pos + 1 + usize::from(len))?;
				if !name.is_empty() {
					name.push('.');
				}
				name.extend(label.iter().map(|b| char::from(b.to_ascii_lowercase())));
				if name.len() > 255 {
					return None;
				}
				pos += 1 + usize::from(len);
			},
			0b11 => {
				let low = *msg.get(pos + 1)?;
				end.get_or_insert(pos + 2);
				pos = usize::from(u16::from_be_bytes([len & 0x3f, low]));
			},
			_ => return None,
		}
	}
	None
}

/// `(address, question name, ttl)` for A/AAAA answers whose owner is a
/// queried name accepted by `wanted`, or a CNAME target reached from one.
fn dns_answers(msg: &[u8], wanted: impl Fn(&str) -> bool) -> Vec<(IpAddr, String, u32)> {
	const TYPE_A: u16 = 1;
	const TYPE_CNAME: u16 = 5;
	const TYPE_AAAA: u16 = 28;
	const CLASS_IN: u16 = 1;
	let mut out = Vec::new();
	let mut dns = Dns { msg, pos: 0 };
	let Some((_, flags, qdcount, ancount)) =
		(|| Some((dns.u16()?, dns.u16()?, dns.u16()?, dns.u16()?)))()
	else {
		return out;
	};
	// Response bit set, RCODE 0.
	if flags & 0x8000 == 0 || flags & 0x000f != 0 {
		return out;
	}
	dns.pos = 12;
	// owner name → question name it answers for.
	let mut chain: HashMap<String, String> = HashMap::new();
	for _ in 0..qdcount {
		let Some(question) = dns.name() else {
			return out;
		};
		if dns.u16().is_none() || dns.u16().is_none() {
			return out;
		}
		if wanted(&question) {
			chain.insert(question.clone(), question);
		}
	}
	let mut seen = HashSet::new();
	for _ in 0..ancount {
		let Some(owner) = dns.name() else { return out };
		let (Some(rtype), Some(class), Some(ttl), Some(rdlen)) =
			(dns.u16(), dns.u16(), dns.u32(), dns.u16())
		else {
			return out;
		};
		let start = dns.pos;
		let Some(rdata) = msg.get(start..start + usize::from(rdlen)) else {
			return out;
		};
		dns.pos = start + usize::from(rdlen);
		let Some(question) = chain.get(&owner).cloned() else {
			continue;
		};
		if class != CLASS_IN {
			continue;
		}
		let ip = match (rtype, rdata.len()) {
			(TYPE_A, 4) => IpAddr::V4(Ipv4Addr::new(rdata[0], rdata[1], rdata[2], rdata[3])),
			(TYPE_AAAA, 16) => {
				let mut octets = [0u8; 16];
				octets.copy_from_slice(rdata);
				IpAddr::V6(Ipv6Addr::from(octets))
			},
			(TYPE_CNAME, _) => {
				if let Some((target, _)) = read_name(msg, start) {
					chain.insert(target, question);
				}
				continue;
			},
			_ => continue,
		};
		if seen.insert(ip) {
			out.push((ip, question, ttl));
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	const GUEST: [u8; 4] = [10, 0, 2, 15];

	fn policy(json: &str) -> Policy {
		Policy::from_json(json.as_bytes()).expect("valid policy")
	}

	/// E2B-style policy: always-denied ranges, deny-all, allow 1.1.1.0/24
	/// and `*.example.com`, DNS reachable.
	fn restrictive() -> Policy {
		policy(
			r#"{"blocked":["10.0.0.0/8","127.0.0.0/8","169.254.0.0/16","172.16.0.0/12","192.168.0.0/16","::1/128","fc00::/7","fe80::/10"],
			"exempt":[],"allow":["1.1.1.0/24","8.8.8.8/32","2606:4700::/32"],"deny":["0.0.0.0/0"],"domains":["*.example.com"],"dns":true}"#,
		)
	}

	fn eth(ethertype: u16, l3: &[u8]) -> Vec<u8> {
		let mut frame = vec![0u8; 12];
		frame.extend_from_slice(&ethertype.to_be_bytes());
		frame.extend_from_slice(l3);
		frame
	}

	fn l4(protocol: u8, sport: u16, dport: u16, flags: u8, payload: &[u8]) -> Vec<u8> {
		let mut out = Vec::new();
		out.extend_from_slice(&sport.to_be_bytes());
		out.extend_from_slice(&dport.to_be_bytes());
		if protocol == PROTO_TCP {
			out.extend_from_slice(&[0; 8]);
			out.extend_from_slice(&[0x50, flags, 0xff, 0xff, 0, 0, 0, 0]);
		} else {
			out.extend_from_slice(&u16::try_from(8 + payload.len()).expect("len").to_be_bytes());
			out.extend_from_slice(&[0, 0]);
		}
		out.extend_from_slice(payload);
		out
	}

	fn v4(
		protocol: u8,
		src: [u8; 4],
		dst: [u8; 4],
		sport: u16,
		dport: u16,
		flags: u8,
		payload: &[u8],
	) -> Vec<u8> {
		let body = l4(protocol, sport, dport, flags, payload);
		let mut ip = vec![0x45, 0, 0, 0, 0, 0, 0, 0, 64, protocol, 0, 0];
		ip.extend_from_slice(&src);
		ip.extend_from_slice(&dst);
		ip.extend_from_slice(&body);
		eth(ETHERTYPE_IPV4, &ip)
	}

	fn tcp(dst: [u8; 4], dport: u16, flags: u8) -> Vec<u8> {
		v4(PROTO_TCP, GUEST, dst, 40000, dport, flags, &[])
	}

	fn v6(protocol: u8, dst: &str, dport: u16, flags: u8) -> Vec<u8> {
		let body = l4(protocol, 40000, dport, flags, &[]);
		let mut ip = vec![0x60, 0, 0, 0];
		ip.extend_from_slice(&u16::try_from(body.len()).expect("len").to_be_bytes());
		ip.extend_from_slice(&[protocol, 64]);
		ip.extend_from_slice(&"fec0::15".parse::<Ipv6Addr>().expect("ip").octets());
		ip.extend_from_slice(&dst.parse::<Ipv6Addr>().expect("ip").octets());
		ip.extend_from_slice(&body);
		eth(ETHERTYPE_IPV6, &ip)
	}

	/// A slirp-DNS response for `question` with CNAME/A/AAAA answers.
	fn dns_response(question: &str, answers: &[(&str, u16, Vec<u8>, u32)], rcode: u16) -> Vec<u8> {
		let encode = |name: &str| {
			let mut out = Vec::new();
			for label in name.split('.') {
				out.push(u8::try_from(label.len()).expect("label"));
				out.extend_from_slice(label.as_bytes());
			}
			out.push(0);
			out
		};
		let mut msg = vec![0x12, 0x34];
		msg.extend_from_slice(&(0x8180 | rcode).to_be_bytes());
		msg.extend_from_slice(&[0, 1]);
		msg.extend_from_slice(&u16::try_from(answers.len()).expect("len").to_be_bytes());
		msg.extend_from_slice(&[0, 0, 0, 0]);
		msg.extend_from_slice(&encode(question));
		msg.extend_from_slice(&[0, 1, 0, 1]);
		for (owner, rtype, rdata, ttl) in answers {
			if *owner == question {
				msg.extend_from_slice(&[0xc0, 12]); // pointer to the question name
			} else {
				msg.extend_from_slice(&encode(owner));
			}
			msg.extend_from_slice(&rtype.to_be_bytes());
			msg.extend_from_slice(&[0, 1]);
			msg.extend_from_slice(&ttl.to_be_bytes());
			msg.extend_from_slice(&u16::try_from(rdata.len()).expect("len").to_be_bytes());
			msg.extend_from_slice(rdata);
		}
		v4(PROTO_UDP, SLIRP_DNS.octets(), GUEST, DNS_PORT, 33333, 0, &msg)
	}

	fn name(n: &str) -> Vec<u8> {
		let mut out = Vec::new();
		for label in n.split('.') {
			out.push(u8::try_from(label.len()).expect("label"));
			out.extend_from_slice(label.as_bytes());
		}
		out.push(0);
		out
	}

	#[test]
	fn cidr_rules_on_new_tcp_and_udp() {
		let mut f = Filter::new(restrictive());
		let now = Instant::now();
		assert!(f.admit(&tcp([1, 1, 1, 1], 443, TCP_SYN), now), "allowed CIDR");
		assert!(!f.admit(&tcp([9, 9, 9, 9], 443, TCP_SYN), now), "deny-all");
		assert!(
			!f.admit(&v4(PROTO_UDP, GUEST, [9, 9, 9, 9], 5000, 53, 0, b"q"), now),
			"UDP to a denied resolver"
		);
		assert!(
			f.admit(&v4(PROTO_UDP, GUEST, [8, 8, 8, 8], 5000, 53, 0, b"q"), now),
			"UDP to allowed resolver"
		);
		assert!(
			f.admit(&v4(PROTO_UDP, GUEST, SLIRP_DNS.octets(), 5000, 53, 0, b"q"), now),
			"slirp DNS"
		);
		assert!(f.admit(&v4(PROTO_UDP, [0; 4], [255; 4], 68, 67, 0, b"dhcp"), now), "DHCP");
		// The slirp gateway reaches host loopback: blocked as 10/8.
		assert!(!f.admit(&tcp(SLIRP_HOST.octets(), 22, TCP_SYN), now));
		assert!(!f.admit(&tcp(SLIRP_DNS.octets(), 80, TCP_SYN), now), "only port 53 of the resolver");
		// ARP passes; garbage does not.
		assert!(f.admit(&eth(ETHERTYPE_ARP, &[0; 28]), now));
		assert!(!f.admit(&[0u8; 10], now));
		assert!(!f.admit(&eth(0x1234, &[0; 40]), now));
	}

	#[test]
	fn allow_beats_deny_and_blocked_beats_allow_unless_exempt() {
		let p = policy(
			r#"{"blocked":["10.0.0.0/8"],"exempt":["10.9.0.0/16"],"allow":["10.0.0.0/8","5.5.5.0/24"],"deny":["5.0.0.0/8"],"domains":[],"dns":false}"#,
		);
		let mut f = Filter::new(p);
		let now = Instant::now();
		assert!(f.admit(&tcp([5, 5, 5, 5], 80, TCP_SYN), now));
		assert!(!f.admit(&tcp([5, 6, 5, 5], 80, TCP_SYN), now));
		assert!(!f.admit(&tcp([10, 1, 1, 1], 80, TCP_SYN), now), "allow cannot open a blocked range");
		assert!(f.admit(&tcp([10, 9, 1, 1], 80, TCP_SYN), now), "exempt range");
		assert!(
			!f.admit(&v4(PROTO_UDP, GUEST, SLIRP_DNS.octets(), 5000, 53, 0, b"q"), now),
			"dns disabled"
		);
	}

	#[test]
	fn established_flows_survive_policy_updates() {
		let mut f = Filter::new(policy(
			r#"{"blocked":[],"exempt":[],"allow":[],"deny":[],"domains":[],"dns":true}"#,
		));
		let now = Instant::now();
		assert!(f.admit(&tcp([9, 9, 9, 9], 443, TCP_SYN), now));
		f.set_policy(restrictive());
		assert!(f.admit(&tcp([9, 9, 9, 9], 443, TCP_ACK), now), "established flow continues");
		assert!(
			!f.admit(&v4(PROTO_TCP, GUEST, [9, 9, 9, 9], 40001, 443, TCP_SYN, &[]), now),
			"new flow denied"
		);
		assert!(
			!f.admit(&v4(PROTO_TCP, GUEST, [9, 9, 9, 9], 40001, 443, TCP_ACK, &[]), now),
			"unknown non-SYN judged"
		);
		// A reused 4-tuple SYN is re-judged under the new policy.
		assert!(!f.admit(&tcp([9, 9, 9, 9], 443, TCP_SYN), now));
		// RST ends the flow.
		assert!(f.admit(&tcp([9, 9, 9, 9], 443, TCP_RST | TCP_ACK), now));
		assert!(!f.admit(&tcp([9, 9, 9, 9], 443, TCP_ACK), now));
	}

	#[test]
	fn ipv6_packets() {
		let mut f = Filter::new(restrictive());
		let now = Instant::now();
		assert!(f.admit(&v6(PROTO_TCP, "2606:4700::1111", 443, TCP_SYN), now));
		assert!(f.admit(&v6(PROTO_UDP, "2a00::1", 53, 0), now), "deny-all is IPv4-only");
		assert!(!f.admit(&v6(PROTO_TCP, "::1", 22, TCP_SYN), now));
		assert!(!f.admit(&v6(PROTO_UDP, "fd00::1", 53, 0), now));
		assert!(!f.admit(&v6(PROTO_TCP, "fe80::1", 80, TCP_SYN), now));
	}

	#[test]
	fn dns_answers_unlock_web_ports_with_ttl_and_revocation() {
		let mut f = Filter::new(restrictive());
		let now = Instant::now();
		let cdn = [93, 184, 216, 34];
		let v6addr = "2001:db8::34".parse::<Ipv6Addr>().expect("ip");
		assert!(!f.admit(&tcp(cdn, 443, TCP_SYN), now));
		f.observe(
			&dns_response(
				"API.example.com",
				&[
					("API.example.com", 5, name("edge.cdn.net"), 300),
					("edge.cdn.net", 1, cdn.to_vec(), 120),
					("edge.cdn.net", 28, v6addr.octets().to_vec(), 120),
					("other.net", 1, vec![6, 6, 6, 6], 120),
				],
				0,
			),
			now,
		);
		assert!(f.admit(&tcp(cdn, 443, TCP_SYN), now), "learned via CNAME chain");
		assert!(f.admit(&v4(PROTO_TCP, GUEST, cdn, 40002, 80, TCP_SYN, &[]), now));
		assert!(!f.admit(&v4(PROTO_TCP, GUEST, cdn, 40003, 22, TCP_SYN, &[]), now), "web ports only");
		assert!(!f.admit(&v4(PROTO_UDP, GUEST, cdn, 40004, 443, 0, b"quic"), now), "TCP only");
		assert!(f.admit(&v6(PROTO_TCP, "2001:db8::34", 443, TCP_SYN), now));
		assert!(!f.admit(&tcp([6, 6, 6, 6], 443, TCP_SYN), now), "unrelated owner not learned");
		// TTL expiry (clamped to ≥ 60 s).
		let later = now + Duration::from_secs(121);
		assert!(!f.admit(&v4(PROTO_TCP, GUEST, cdn, 40005, 443, TCP_SYN, &[]), later));
		// Revocation by live update even before expiry.
		f.observe(
			&dns_response("www.example.com", &[("www.example.com", 1, cdn.to_vec(), 600)], 0),
			now,
		);
		f.set_policy(policy(r#"{"blocked":[],"exempt":[],"allow":[],"deny":["0.0.0.0/0"],"domains":["other.org"],"dns":true}"#));
		assert!(!f.admit(&v4(PROTO_TCP, GUEST, cdn, 40006, 443, TCP_SYN, &[]), now));
	}

	#[test]
	fn dns_snooping_ignores_untrusted_sources_and_bad_answers() {
		let mut f = Filter::new(restrictive());
		let now = Instant::now();
		// Non-matching question.
		f.observe(&dns_response("evil.com", &[("evil.com", 1, vec![7, 7, 7, 7], 60)], 0), now);
		assert!(!f.admit(&tcp([7, 7, 7, 7], 443, TCP_SYN), now));
		// NXDOMAIN-coded response.
		f.observe(
			&dns_response("a.example.com", &[("a.example.com", 1, vec![7, 7, 7, 8], 60)], 3),
			now,
		);
		assert!(!f.admit(&tcp([7, 7, 7, 8], 443, TCP_SYN), now));
		// A matching answer from a resolver other than slirp's.
		let mut spoofed =
			dns_response("a.example.com", &[("a.example.com", 1, vec![7, 7, 7, 9], 60)], 0);
		spoofed[ETH_HEADER + 12..ETH_HEADER + 16].copy_from_slice(&[8, 8, 8, 8]);
		f.observe(&spoofed, now);
		assert!(!f.admit(&tcp([7, 7, 7, 9], 443, TCP_SYN), now));
		// An answer pointing a matching name at a blocked range.
		f.observe(
			&dns_response("b.example.com", &[("b.example.com", 1, vec![192, 168, 1, 1], 60)], 0),
			now,
		);
		assert!(!f.admit(&tcp([192, 168, 1, 1], 443, TCP_SYN), now));
		// Truncated message and a compression loop do not panic or learn.
		let mut truncated =
			dns_response("c.example.com", &[("c.example.com", 1, vec![7, 7, 7, 10], 60)], 0);
		truncated.truncate(truncated.len() - 2);
		f.observe(&truncated, now);
		assert!(!f.admit(&tcp([7, 7, 7, 10], 443, TCP_SYN), now));
		assert_eq!(read_name(&[0xc0, 0x00], 0), None);
	}

	#[test]
	fn tables_are_bounded() {
		let mut f = Filter::new(restrictive());
		let now = Instant::now();
		for i in 0..MAX_LEARNED + 10 {
			let ip = IpAddr::V4(Ipv4Addr::from(0x5d00_0000 + u32::try_from(i).expect("i")));
			f.learn(ip, "x.example.com".into(), now + Duration::from_mins(1), now);
		}
		assert_eq!(f.learned.len(), MAX_LEARNED);
		let mut f = Filter::new(policy(
			r#"{"blocked":[],"exempt":[],"allow":[],"deny":[],"domains":[],"dns":true}"#,
		));
		for i in 0..MAX_FLOWS + 10 {
			let sport = u16::try_from(i % 60_000).expect("port");
			let dst = [1, 1, (i / 60_000) as u8, 1];
			assert!(f.admit(
				&v4(PROTO_TCP, GUEST, dst, sport, 443, TCP_SYN, &[]),
				now + Duration::from_micros(i as u64)
			));
		}
		assert_eq!(f.flows.len(), MAX_FLOWS);
	}

	#[test]
	fn rejects_malformed_documents() {
		assert!(Policy::from_json(b"{}").is_err());
		assert!(
			Policy::from_json(
				br#"{"blocked":["10/8"],"exempt":[],"allow":[],"deny":[],"domains":[],"dns":true}"#
			)
			.is_err()
		);
		assert!(Policy::from_json(br#"{"blocked":[],"exempt":[],"allow":["1.2.3.4/33"],"deny":[],"domains":[],"dns":true}"#).is_err());
		assert!(
			Policy::from_json(
				br#"{"blocked":[],"exempt":[],"allow":[],"deny":[],"domains":["bad name"],"dns":true}"#
			)
			.is_err()
		);
		assert!(
			Policy::from_json(
				br#"{"blocked":[],"exempt":[],"allow":[],"deny":[],"domains":[],"dns":true,"extra":1}"#
			)
			.is_err()
		);
	}
}
