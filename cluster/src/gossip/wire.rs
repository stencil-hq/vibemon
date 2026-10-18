//! Gossip wire format: HMAC-authenticated UDP packets and TCP push-pull frames.
//!
//! Every UDP datagram is `body ‖ tag` where `tag = HMAC-SHA256(key, body)` and
//! `key = HKDF-SHA256(secret, info = "vmon-gossip")`. A body is
//! `"VG2" ‖ message ‖ count:u8 ‖ updates…`; TCP frames are `len:u32be ‖ body ‖
//! tag` with a push-pull body. An update is `state:u8 ‖ id:u64 ‖
//! incarnation:u64 ‖ n:u8 ‖ n × addr ‖ meta_version:u64 ‖ has_meta:u8 ‖
//! [len:u32 ‖ meta]` with `1 ≤ n ≤ 16` advertised addresses; an address is
//! `4 ‖ ipv4 ‖ port:u16` or `6 ‖ ipv6 ‖ port:u16`. All integers are
//! big-endian. Decoding never panics: truncated, oversized, or trailing bytes
//! reject the whole packet.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::MemberState;
use crate::meta::NodeId;

/// Protocol magic + version prefix of every body. `VG1` (one address per
/// member) is rejected like any other foreign packet.
const MAGIC: [u8; 3] = *b"VG2";
/// HMAC-SHA256 tag length appended to every packet and frame.
pub(super) const TAG_LEN: usize = 32;
/// Upper bound for one UDP datagram including the tag (fits a 1500-byte MTU).
pub(super) const MAX_PACKET: usize = 1400;
/// Upper bound for one TCP push-pull frame (body + tag).
pub(super) const MAX_FRAME: usize = 32 << 20;
/// Most addresses one member advertises.
pub(super) const MAX_ADDRS: usize = 16;
const ADDR_V4_LEN: usize = 1 + 4 + 2;
const ADDR_V6_LEN: usize = 1 + 16 + 2;
/// Longest message header: a `PingReq` naming an IPv6 address.
const MAX_MESSAGE_LEN: usize = MAGIC.len() + 1 + 4 + 8 + ADDR_V6_LEN;
/// Longest update apart from its metadata bytes: [`MAX_ADDRS`] IPv6 addresses.
const MAX_UPDATE_OVERHEAD: usize = 1 + 8 + 8 + 1 + MAX_ADDRS * ADDR_V6_LEN + 8 + 1 + 4;
/// Metadata larger than this is not inlined into UDP updates; receivers fetch
/// it with a push-pull exchange instead. Any update inlining at most this much
/// fits every packet next to the longest message header.
pub(super) const INLINE_META_MAX: usize =
	MAX_PACKET - TAG_LEN - MAX_MESSAGE_LEN - 1 - MAX_UPDATE_OVERHEAD;

const MSG_PING: u8 = 1;
const MSG_ACK: u8 = 2;
const MSG_PING_REQ: u8 = 3;
const MSG_INDIRECT_ACK: u8 = 4;
const MSG_GOSSIP: u8 = 5;
const MSG_PUSH_PULL: u8 = 0x10;

type HmacSha256 = Hmac<Sha256>;

/// Packet authenticator keyed from the cluster secret.
pub(super) struct Auth {
	mac: HmacSha256,
}

impl Auth {
	/// Derives the gossip key from the shared cluster secret.
	pub(super) fn new(secret: &[u8]) -> Self {
		let mut key = [0u8; 32];
		Hkdf::<Sha256>::new(None, secret)
			.expand(b"vmon-gossip", &mut key)
			.expect("32 bytes is a valid HKDF-SHA256 output length");
		Self { mac: HmacSha256::new_from_slice(&key).expect("HMAC accepts any key length") }
	}

	/// Appends the authentication tag over the whole buffer.
	pub(super) fn seal(&self, buf: &mut Vec<u8>) {
		let mut mac = self.mac.clone();
		mac.update(buf);
		buf.extend_from_slice(&mac.finalize().into_bytes());
	}

	/// Verifies the trailing tag (constant time) and returns the body.
	pub(super) fn open<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
		let split = buf.len().checked_sub(TAG_LEN)?;
		let (body, tag) = buf.split_at(split);
		let mut mac = self.mac.clone();
		mac.update(body);
		mac.verify_slice(tag).ok()?;
		Some(body)
	}
}

/// Failure-detector message carried by a UDP packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Message {
	/// Direct probe; only the member whose id is `target` answers.
	Ping { seq: u32, target: NodeId },
	/// Answer to a `Ping` with the same `seq`.
	Ack { seq: u32 },
	/// Ask the receiver to probe `target` at `addr` on the sender's behalf.
	PingReq { seq: u32, target: NodeId, addr: SocketAddr },
	/// Relayed success of a `PingReq` with the requester's `seq`.
	IndirectAck { seq: u32 },
	/// Pure dissemination packet (only piggybacked updates).
	Gossip,
}

/// One member's state as disseminated by gossip and push-pull.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Update {
	pub id:           NodeId,
	/// Every address the member advertises, in its own preference order
	/// (`1..=MAX_ADDRS` entries).
	pub addrs:        Vec<SocketAddr>,
	pub state:        MemberState,
	pub incarnation:  u64,
	pub meta_version: u64,
	/// `None` when the metadata was too large to inline (receiver fetches it).
	pub meta:         Option<Vec<u8>>,
}

/// Decoded TCP push-pull body: the sender's full member table.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PushPull {
	pub from:    NodeId,
	pub members: Vec<Update>,
}

/// Starts a UDP packet body with the header and `msg`; the caller appends the
/// update count, the updates, and the tag.
pub(super) fn encode_message(msg: Message) -> Vec<u8> {
	let mut buf = Vec::with_capacity(MAX_PACKET);
	buf.extend_from_slice(&MAGIC);
	match msg {
		Message::Ping { seq, target } => {
			buf.push(MSG_PING);
			buf.extend_from_slice(&seq.to_be_bytes());
			buf.extend_from_slice(&target.0.to_be_bytes());
		},
		Message::Ack { seq } => {
			buf.push(MSG_ACK);
			buf.extend_from_slice(&seq.to_be_bytes());
		},
		Message::PingReq { seq, target, addr } => {
			buf.push(MSG_PING_REQ);
			buf.extend_from_slice(&seq.to_be_bytes());
			buf.extend_from_slice(&target.0.to_be_bytes());
			put_addr(&mut buf, addr);
		},
		Message::IndirectAck { seq } => {
			buf.push(MSG_INDIRECT_ACK);
			buf.extend_from_slice(&seq.to_be_bytes());
		},
		Message::Gossip => buf.push(MSG_GOSSIP),
	}
	buf
}

/// Appends one encoded update to `buf`.
pub(super) fn encode_update(update: &Update, buf: &mut Vec<u8>) {
	buf.push(state_code(update.state));
	buf.extend_from_slice(&update.id.0.to_be_bytes());
	buf.extend_from_slice(&update.incarnation.to_be_bytes());
	debug_assert!((1..=MAX_ADDRS).contains(&update.addrs.len()), "advertised address count");
	buf.push(u8::try_from(update.addrs.len()).expect("at most MAX_ADDRS addresses"));
	for addr in &update.addrs {
		put_addr(buf, *addr);
	}
	buf.extend_from_slice(&update.meta_version.to_be_bytes());
	match &update.meta {
		Some(meta) => {
			buf.push(1);
			let len = u32::try_from(meta.len()).expect("member metadata is far below 4 GiB");
			buf.extend_from_slice(&len.to_be_bytes());
			buf.extend_from_slice(meta);
		},
		None => buf.push(0),
	}
}

/// Decodes an authenticated UDP body into its message and piggybacked updates.
pub(super) fn decode_packet(body: &[u8]) -> Option<(Message, Vec<Update>)> {
	let mut r = Reader { buf: body };
	if r.take(MAGIC.len())? != MAGIC {
		return None;
	}
	let msg = match r.u8()? {
		MSG_PING => Message::Ping { seq: r.u32()?, target: r.node_id()? },
		MSG_ACK => Message::Ack { seq: r.u32()? },
		MSG_PING_REQ => {
			Message::PingReq { seq: r.u32()?, target: r.node_id()?, addr: r.addr()? }
		},
		MSG_INDIRECT_ACK => Message::IndirectAck { seq: r.u32()? },
		MSG_GOSSIP => Message::Gossip,
		_ => return None,
	};
	let count = r.u8()?;
	let mut updates = Vec::with_capacity(usize::from(count).min(r.buf.len() / MIN_UPDATE_LEN));
	for _ in 0..count {
		updates.push(r.update()?);
	}
	r.buf.is_empty().then_some((msg, updates))
}

/// Encodes a push-pull body carrying `members` (metadata always inlined).
pub(super) fn encode_push_pull(from: NodeId, members: &[Update]) -> Vec<u8> {
	let mut buf = Vec::with_capacity(64 + members.len() * 128);
	buf.extend_from_slice(&MAGIC);
	buf.push(MSG_PUSH_PULL);
	buf.extend_from_slice(&from.0.to_be_bytes());
	let count = u32::try_from(members.len()).expect("member count fits u32");
	buf.extend_from_slice(&count.to_be_bytes());
	for member in members {
		encode_update(member, &mut buf);
	}
	buf
}

/// Decodes an authenticated push-pull body.
pub(super) fn decode_push_pull(body: &[u8]) -> Option<PushPull> {
	let mut r = Reader { buf: body };
	if r.take(MAGIC.len())? != MAGIC || r.u8()? != MSG_PUSH_PULL {
		return None;
	}
	let from = r.node_id()?;
	let count = usize::try_from(r.u32()?).ok()?;
	let mut members = Vec::with_capacity(count.min(r.buf.len() / MIN_UPDATE_LEN));
	for _ in 0..count {
		members.push(r.update()?);
	}
	r.buf.is_empty().then_some(PushPull { from, members })
}

/// Smallest possible encoded update (one IPv4 address, no metadata).
const MIN_UPDATE_LEN: usize = 1 + 8 + 8 + 1 + ADDR_V4_LEN + 8 + 1;

const fn state_code(state: MemberState) -> u8 {
	match state {
		MemberState::Alive => 0,
		MemberState::Suspect => 1,
		MemberState::Dead => 2,
		MemberState::Left => 3,
	}
}

fn put_addr(buf: &mut Vec<u8>, addr: SocketAddr) {
	match addr.ip() {
		IpAddr::V4(ip) => {
			buf.push(4);
			buf.extend_from_slice(&ip.octets());
		},
		IpAddr::V6(ip) => {
			buf.push(6);
			buf.extend_from_slice(&ip.octets());
		},
	}
	buf.extend_from_slice(&addr.port().to_be_bytes());
}

/// Bounds-checked cursor over an untrusted body.
struct Reader<'a> {
	buf: &'a [u8],
}

impl<'a> Reader<'a> {
	const fn take(&mut self, n: usize) -> Option<&'a [u8]> {
		if self.buf.len() < n {
			return None;
		}
		let (head, rest) = self.buf.split_at(n);
		self.buf = rest;
		Some(head)
	}

	fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
		self.take(N)?.try_into().ok()
	}

	fn u8(&mut self) -> Option<u8> {
		Some(self.array::<1>()?[0])
	}

	fn u32(&mut self) -> Option<u32> {
		Some(u32::from_be_bytes(self.array()?))
	}

	fn u64(&mut self) -> Option<u64> {
		Some(u64::from_be_bytes(self.array()?))
	}

	fn node_id(&mut self) -> Option<NodeId> {
		Some(NodeId(self.u64()?))
	}

	fn addr(&mut self) -> Option<SocketAddr> {
		let ip = match self.u8()? {
			4 => IpAddr::V4(Ipv4Addr::from(self.array::<4>()?)),
			6 => IpAddr::V6(Ipv6Addr::from(self.array::<16>()?)),
			_ => return None,
		};
		Some(SocketAddr::new(ip, u16::from_be_bytes(self.array()?)))
	}

	/// `count:u8` (`1..=MAX_ADDRS`) addresses.
	fn addrs(&mut self) -> Option<Vec<SocketAddr>> {
		let count = usize::from(self.u8()?);
		if !(1..=MAX_ADDRS).contains(&count) {
			return None;
		}
		(0..count).map(|_| self.addr()).collect()
	}

	fn update(&mut self) -> Option<Update> {
		let state = match self.u8()? {
			0 => MemberState::Alive,
			1 => MemberState::Suspect,
			2 => MemberState::Dead,
			3 => MemberState::Left,
			_ => return None,
		};
		let id = self.node_id()?;
		let incarnation = self.u64()?;
		let addrs = self.addrs()?;
		let meta_version = self.u64()?;
		let meta = match self.u8()? {
			0 => None,
			1 => {
				let len = usize::try_from(self.u32()?).ok()?;
				Some(self.take(len)?.to_vec())
			},
			_ => return None,
		};
		Some(Update { id, addrs, state, incarnation, meta_version, meta })
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sample_update(meta: Option<Vec<u8>>) -> Update {
		Update {
			id: NodeId(0xdead_beef),
			addrs: [
				"[2a01:4f9:4b:1a08::2]:7946",
				"10.200.108.18:7946",
				"[::1]:1",
				"100.97.175.76:7946",
			]
			.iter()
			.map(|addr| addr.parse().unwrap())
			.collect(),
			state: MemberState::Suspect,
			incarnation: 42,
			meta_version: 7,
			meta,
		}
	}

	fn sample_packet() -> Vec<u8> {
		let mut buf = encode_message(Message::PingReq {
			seq:    9,
			target: NodeId(3),
			addr:   "10.0.0.1:7946".parse().unwrap(),
		});
		buf.push(2);
		encode_update(&sample_update(Some(b"{\"a\":1}".to_vec())), &mut buf);
		encode_update(&sample_update(None), &mut buf);
		buf
	}

	#[test]
	fn packet_round_trips() {
		let (msg, updates) = decode_packet(&sample_packet()).unwrap();
		assert_eq!(msg, Message::PingReq {
			seq:    9,
			target: NodeId(3),
			addr:   "10.0.0.1:7946".parse().unwrap(),
		});
		assert_eq!(updates, vec![sample_update(Some(b"{\"a\":1}".to_vec())), sample_update(None)]);
		let mut single = sample_update(None);
		single.addrs.truncate(1);
		let mut buf = encode_message(Message::Gossip);
		buf.push(1);
		encode_update(&single, &mut buf);
		assert_eq!(decode_packet(&buf).unwrap(), (Message::Gossip, vec![single]));
	}

	/// A `Gossip` packet with one update carrying `addrs` under the advertised
	/// count `count`.
	fn packet_with_addr_count(addrs: &[SocketAddr], count: u8) -> Vec<u8> {
		let mut buf = encode_message(Message::Gossip);
		buf.push(1);
		buf.push(state_code(MemberState::Alive));
		buf.extend_from_slice(&7u64.to_be_bytes());
		buf.extend_from_slice(&1u64.to_be_bytes());
		buf.push(count);
		for addr in addrs {
			put_addr(&mut buf, *addr);
		}
		buf.extend_from_slice(&1u64.to_be_bytes());
		buf.push(0);
		buf
	}

	#[test]
	fn address_count_must_be_between_one_and_max() {
		let addrs: Vec<SocketAddr> = (0..=MAX_ADDRS)
			.map(|i| SocketAddr::from(([10, 0, 0, u8::try_from(i).unwrap()], 7946)))
			.collect();
		let count = |n: usize| u8::try_from(n).unwrap();
		let (_, updates) = decode_packet(&packet_with_addr_count(&addrs[..1], 1)).unwrap();
		assert_eq!(updates[0].addrs, addrs[..1]);
		let (_, updates) =
			decode_packet(&packet_with_addr_count(&addrs[..MAX_ADDRS], count(MAX_ADDRS))).unwrap();
		assert_eq!(updates[0].addrs, addrs[..MAX_ADDRS]);
		assert!(decode_packet(&packet_with_addr_count(&[], 0)).is_none());
		assert!(decode_packet(&packet_with_addr_count(&addrs, count(MAX_ADDRS + 1))).is_none());
		// The count must match the addresses that follow.
		assert!(decode_packet(&packet_with_addr_count(&addrs[..2], 3)).is_none());
		assert!(decode_packet(&packet_with_addr_count(&addrs[..3], 2)).is_none());
	}

	#[test]
	fn longest_inline_update_fits_the_longest_packet() {
		let update = Update {
			id:           NodeId(u64::MAX),
			addrs:        (0..MAX_ADDRS)
				.map(|i| {
					SocketAddr::new(
						IpAddr::V6(Ipv6Addr::from(u128::MAX - u128::try_from(i).unwrap())),
						65535,
					)
				})
				.collect(),
			state:        MemberState::Alive,
			incarnation:  u64::MAX,
			meta_version: u64::MAX,
			meta:         Some(vec![b'x'; INLINE_META_MAX]),
		};
		let mut buf = encode_message(Message::PingReq {
			seq:    u32::MAX,
			target: NodeId(u64::MAX),
			addr:   "[2a01:4f9:4b:1a08::2]:7946".parse().unwrap(),
		});
		assert_eq!(buf.len(), MAX_MESSAGE_LEN);
		buf.push(1);
		encode_update(&update, &mut buf);
		assert_eq!(buf.len() + TAG_LEN, MAX_PACKET);
		assert_eq!(decode_packet(&buf).unwrap().1, vec![update]);
	}

	#[test]
	fn every_truncation_and_extension_is_rejected() {
		let packet = sample_packet();
		for len in 0..packet.len() {
			assert!(decode_packet(&packet[..len]).is_none(), "prefix of {len} bytes accepted");
		}
		let mut longer = packet;
		longer.push(0);
		assert!(decode_packet(&longer).is_none());
	}

	#[test]
	fn push_pull_round_trips_and_rejects_packet_bodies() {
		let members = vec![sample_update(Some(vec![1; 3000])), sample_update(None)];
		let body = encode_push_pull(NodeId(5), &members);
		assert_eq!(decode_push_pull(&body).unwrap(), PushPull { from: NodeId(5), members });
		assert!(decode_push_pull(&sample_packet()).is_none());
		assert!(decode_packet(&body).is_none());
	}

	#[test]
	fn auth_rejects_foreign_key_and_tampering() {
		let ours = Auth::new(b"secret-a");
		let theirs = Auth::new(b"secret-b");
		let mut sealed = sample_packet();
		ours.seal(&mut sealed);
		assert_eq!(ours.open(&sealed).unwrap(), sample_packet().as_slice());
		assert!(theirs.open(&sealed).is_none());
		let last = sealed.len() - 1;
		sealed[last] ^= 1;
		assert!(ours.open(&sealed).is_none());
		assert!(ours.open(&sealed[..TAG_LEN - 1]).is_none());
	}

	#[test]
	fn random_and_bit_flipped_bodies_never_panic() {
		let packet = sample_packet();
		let pp = encode_push_pull(NodeId(1), &[sample_update(Some(vec![7; 40]))]);
		for round in 0..20_000usize {
			let len = rand::random_range(0..200usize);
			let noise: Vec<u8> = (0..len).map(|_| rand::random()).collect();
			let _ = decode_packet(&noise);
			let _ = decode_push_pull(&noise);
			let mut flipped = if round % 2 == 0 {
				packet.clone()
			} else {
				pp.clone()
			};
			let idx = rand::random_range(0..flipped.len());
			flipped[idx] = rand::random();
			let _ = decode_packet(&flipped);
			let _ = decode_push_pull(&flipped);
		}
	}
}
