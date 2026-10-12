//! Member table: SWIM merge rules, suspicion/reaping timers, and the
//! piggyback broadcast queue. Pure state (no I/O) so precedence rules are unit
//! tested directly.

use std::{
	collections::HashMap,
	net::SocketAddr,
	time::{Duration, Instant},
};

use rand::seq::{IteratorRandom, SliceRandom};

use super::{
	Member, MemberEvent, MemberState,
	wire::{self, Update},
};
use crate::meta::NodeId;

/// Where an update came from; push-pull state is merged more conservatively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Origin {
	/// Piggybacked on a UDP packet or produced locally.
	Gossip,
	/// Full-state exchange over TCP: a remote `Dead` is downgraded to `Suspect`
	/// so the member gets a chance to refute (this is what heals partitions).
	PushPull,
}

/// Result of merging one update.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Outcome {
	/// Event to publish to subscribers.
	pub event:      Option<MemberEvent>,
	/// The update announced newer metadata without inlining it; fetch it from
	/// this address with a push-pull exchange.
	pub fetch_meta: Option<SocketAddr>,
}

struct Entry {
	member: Member,
	/// When `member.state` last changed (drives suspicion and reaping).
	since:  Instant,
}

struct Broadcast {
	id:        NodeId,
	bytes:     Vec<u8>,
	transmits: u32,
	/// Monotonic enqueue order; newer wins ties on `transmits`.
	order:     u64,
}

/// Membership table of one gossip instance (includes the local member).
pub(super) struct Table {
	local:       NodeId,
	members:     HashMap<NodeId, Entry>,
	queue:       Vec<Broadcast>,
	order:       u64,
	probe_ring:  Vec<NodeId>,
	probe_index: usize,
}

impl Table {
	/// Creates a table holding only the local member, queued for broadcast.
	pub(super) fn new(local: Member, now: Instant) -> Self {
		let id = local.id;
		let mut table = Self {
			local:       id,
			members:     HashMap::new(),
			queue:       Vec::new(),
			order:       0,
			probe_ring:  Vec::new(),
			probe_index: 0,
		};
		table
			.members
			.insert(id, Entry { member: local, since: now });
		table.enqueue(id);
		table
	}

	/// Snapshot of every known member, including the local one.
	pub(super) fn members(&self) -> Vec<Member> {
		self.members.values().map(|e| e.member.clone()).collect()
	}

	/// Snapshot of one member.
	pub(super) fn get(&self, id: NodeId) -> Option<Member> {
		self.members.get(&id).map(|e| e.member.clone())
	}

	/// The local member.
	#[cfg(test)]
	pub(super) fn local(&self) -> &Member {
		&self.members[&self.local].member
	}

	/// Number of members not known to be gone (Alive or Suspect), self included.
	pub(super) fn live_count(&self) -> usize {
		self
			.members
			.values()
			.filter(|e| is_live(e.member.state))
			.count()
	}

	/// `scale × max(1, ⌈log10(n + 1)⌉)` for the current live member count;
	/// used for both the retransmit limit and the suspicion timeout.
	pub(super) fn log_scale(&self, scale: u32) -> u32 {
		// ⌈log10(n + 1)⌉ equals the decimal digit count of n for n ≥ 1.
		let mut digits = 0u32;
		let mut rest = self.live_count();
		while rest > 0 {
			digits += 1;
			rest /= 10;
		}
		scale.saturating_mul(digits.max(1))
	}

	/// Peers (excluding self) in one of `states`, chosen at random, at most `n`.
	pub(super) fn random_peers(
		&self,
		states: &[MemberState],
		exclude: Option<NodeId>,
		n: usize,
	) -> Vec<(NodeId, SocketAddr)> {
		self
			.members
			.values()
			.filter(|e| e.member.id != self.local && Some(e.member.id) != exclude)
			.filter(|e| states.contains(&e.member.state))
			.map(|e| (e.member.id, e.member.addr))
			.sample(&mut rand::rng(), n)
	}

	/// Next probe target in a shuffled round-robin over live peers; returns the
	/// id, address, and incarnation probed (a failed probe suspects exactly it).
	pub(super) fn next_probe_target(&mut self) -> Option<(NodeId, SocketAddr, u64)> {
		for _ in 0..2 {
			while let Some(&id) = self.probe_ring.get(self.probe_index) {
				self.probe_index += 1;
				if let Some(e) = self.members.get(&id)
					&& is_live(e.member.state)
				{
					return Some((id, e.member.addr, e.member.incarnation));
				}
			}
			self.probe_ring = self
				.members
				.values()
				.filter(|e| e.member.id != self.local && is_live(e.member.state))
				.map(|e| e.member.id)
				.collect();
			self.probe_ring.shuffle(&mut rand::rng());
			self.probe_index = 0;
		}
		None
	}

	/// Full state for a push-pull exchange (metadata always inlined).
	pub(super) fn snapshot(&self) -> Vec<Update> {
		self
			.members
			.values()
			.map(|e| update_of(&e.member, true))
			.collect()
	}

	/// Merges one update following SWIM precedence:
	/// - `Alive` wins only with a strictly higher incarnation (and may move the
	///   member to a new address, e.g. after a restart);
	/// - `Suspect` wins over `Alive` at an equal or higher incarnation;
	/// - `Dead`/`Left` win over live states at an equal or higher incarnation;
	/// - metadata is independent: a higher `meta_version` wins.
	///
	/// Updates about the local member trigger refutation (incarnation bump).
	pub(super) fn apply(&mut self, mut update: Update, origin: Origin, now: Instant) -> Outcome {
		if origin == Origin::PushPull && update.state == MemberState::Dead {
			update.state = MemberState::Suspect;
		}
		if update.id == self.local {
			self.refute(&update);
			return Outcome::default();
		}
		let Some(entry) = self.members.get_mut(&update.id) else {
			return self.insert(update, now);
		};
		let member = &mut entry.member;
		let before_state = member.state;
		let before_addr = member.addr;
		let before_incarnation = member.incarnation;
		let newer = update.incarnation > member.incarnation;
		let at_least = update.incarnation >= member.incarnation;
		match update.state {
			MemberState::Alive if newer => {
				member.state = MemberState::Alive;
				member.addr = update.addr;
				member.incarnation = update.incarnation;
			},
			MemberState::Suspect
				if (at_least && before_state == MemberState::Alive)
					|| (newer && before_state == MemberState::Suspect) =>
			{
				member.state = MemberState::Suspect;
				member.incarnation = update.incarnation;
			},
			MemberState::Dead if at_least && is_live(before_state) => {
				member.state = MemberState::Dead;
				member.incarnation = update.incarnation;
			},
			MemberState::Left if at_least && before_state != MemberState::Left => {
				member.state = MemberState::Left;
				member.incarnation = update.incarnation;
			},
			_ => {},
		}
		let mut fetch_meta = None;
		let mut meta_changed = false;
		if update.meta_version > member.meta_version {
			match update.meta {
				Some(meta) => {
					member.meta = meta;
					member.meta_version = update.meta_version;
					meta_changed = true;
				},
				None if is_live(member.state) => fetch_meta = Some(member.addr),
				None => {},
			}
		}
		if member.state != before_state {
			entry.since = now;
		}
		let member = &entry.member;
		let visible = member.state != before_state || member.addr != before_addr || meta_changed;
		let event = visible.then(|| match (before_state, member.state) {
			(MemberState::Dead | MemberState::Left, MemberState::Alive | MemberState::Suspect) => {
				MemberEvent::Joined(member.clone())
			},
			(_, MemberState::Dead) if before_state != MemberState::Dead => {
				MemberEvent::Failed(member.clone())
			},
			(_, MemberState::Left) if before_state != MemberState::Left => {
				MemberEvent::Left(member.clone())
			},
			_ => MemberEvent::Updated(member.clone()),
		});
		if visible || member.incarnation != before_incarnation {
			self.enqueue(update.id);
		}
		Outcome { event, fetch_meta }
	}

	/// Marks `id` suspect after a failed probe of `incarnation`.
	pub(super) fn suspect(&mut self, id: NodeId, incarnation: u64, now: Instant) -> Outcome {
		let Some(member) = self.members.get(&id).map(|e| &e.member) else {
			return Outcome::default();
		};
		let update = Update {
			id,
			addr: member.addr,
			state: MemberState::Suspect,
			incarnation,
			meta_version: 0,
			meta: None,
		};
		self.apply(update, Origin::Gossip, now)
	}

	/// Replaces local metadata with a bumped version; returns the new local
	/// member.
	pub(super) fn set_local_meta(&mut self, meta: Vec<u8>) -> Member {
		let local = &mut self.entry_mut(self.local).member;
		local.meta_version += 1;
		local.meta = meta;
		let member = local.clone();
		self.enqueue(self.local);
		member
	}

	/// Marks the local member `Left` with a bumped incarnation and queues it.
	pub(super) fn leave(&mut self, now: Instant) {
		let entry = self.entry_mut(self.local);
		entry.member.state = MemberState::Left;
		entry.member.incarnation += 1;
		entry.since = now;
		self.enqueue(self.local);
	}

	/// Whether the local member's latest update is still being disseminated.
	pub(super) fn local_broadcast_pending(&self) -> bool {
		self.queue.iter().any(|b| b.id == self.local)
	}

	/// Whether any update is waiting to be disseminated.
	pub(super) const fn has_broadcasts(&self) -> bool {
		!self.queue.is_empty()
	}

	/// Expires suspicions older than `suspicion_timeout` into `Dead` and reaps
	/// `Dead`/`Left` members older than `reap_after`.
	pub(super) fn expire(
		&mut self,
		now: Instant,
		suspicion_timeout: Duration,
		reap_after: Duration,
	) -> Vec<MemberEvent> {
		let mut events = Vec::new();
		let mut changed = Vec::new();
		self.members.retain(|&id, entry| {
			if id == self.local {
				return true;
			}
			let age = now.saturating_duration_since(entry.since);
			match entry.member.state {
				MemberState::Suspect if age >= suspicion_timeout => {
					entry.member.state = MemberState::Dead;
					entry.since = now;
					events.push(MemberEvent::Failed(entry.member.clone()));
					changed.push(id);
					true
				},
				MemberState::Dead | MemberState::Left => age < reap_after,
				_ => true,
			}
		});
		let members = &self.members;
		self.queue.retain(|b| members.contains_key(&b.id));
		for id in changed {
			self.enqueue(id);
		}
		events
	}

	/// Pops piggyback updates for one packet: least-transmitted (then newest)
	/// first, at most `budget` bytes and 255 updates. Each returned update
	/// counts one transmit; updates reaching `limit` transmits are dropped.
	pub(super) fn take_broadcasts(&mut self, budget: usize, limit: u32) -> (u8, Vec<u8>) {
		self
			.queue
			.sort_by(|a, b| a.transmits.cmp(&b.transmits).then(b.order.cmp(&a.order)));
		let mut out = Vec::new();
		let mut count = 0u8;
		for broadcast in &mut self.queue {
			if count == u8::MAX {
				break;
			}
			if out.len() + broadcast.bytes.len() > budget {
				continue;
			}
			out.extend_from_slice(&broadcast.bytes);
			broadcast.transmits += 1;
			count += 1;
		}
		self.queue.retain(|b| b.transmits < limit);
		(count, out)
	}

	fn insert(&mut self, update: Update, now: Instant) -> Outcome {
		if !is_live(update.state) {
			return Outcome::default();
		}
		let fetch_meta = (update.meta.is_none() && update.meta_version > 0).then_some(update.addr);
		let (meta_version, meta) = match update.meta {
			Some(meta) => (update.meta_version, meta),
			None => (0, Vec::new()),
		};
		let member = Member {
			id: update.id,
			addr: update.addr,
			state: update.state,
			incarnation: update.incarnation,
			meta_version,
			meta,
		};
		self
			.members
			.insert(update.id, Entry { member: member.clone(), since: now });
		self.enqueue(update.id);
		Outcome { event: Some(MemberEvent::Joined(member)), fetch_meta }
	}

	/// Refutes stale or hostile claims about the local member by bumping the
	/// incarnation (state/address) or `meta_version` (metadata) above them.
	fn refute(&mut self, update: &Update) {
		let local = &mut self.entry_mut(self.local).member;
		if local.state == MemberState::Left {
			return;
		}
		let mut bumped = false;
		let contradicts = match update.state {
			MemberState::Alive => {
				update.incarnation > local.incarnation
					|| (update.incarnation == local.incarnation && update.addr != local.addr)
			},
			_ => update.incarnation >= local.incarnation,
		};
		if contradicts {
			local.incarnation = update.incarnation + 1;
			bumped = true;
		}
		let foreign_meta = update.meta_version > local.meta_version
			|| (update.meta_version == local.meta_version
				&& update.meta.as_ref().is_some_and(|m| *m != local.meta));
		if foreign_meta {
			local.meta_version = update.meta_version + 1;
			bumped = true;
		}
		if bumped {
			self.enqueue(self.local);
		}
	}

	fn entry_mut(&mut self, id: NodeId) -> &mut Entry {
		self.members.get_mut(&id).expect("member present")
	}

	/// Queues the current state of `id`, replacing any older queued update.
	fn enqueue(&mut self, id: NodeId) {
		let Some(entry) = self.members.get(&id) else {
			return;
		};
		let mut bytes = Vec::new();
		wire::encode_update(&update_of(&entry.member, false), &mut bytes);
		self.order += 1;
		self.queue.retain(|b| b.id != id);
		self
			.queue
			.push(Broadcast { id, bytes, transmits: 0, order: self.order });
	}
}

const fn is_live(state: MemberState) -> bool {
	matches!(state, MemberState::Alive | MemberState::Suspect)
}

/// Builds the wire update for `member`; UDP updates inline metadata only for
/// `Alive` members whose metadata fits [`wire::INLINE_META_MAX`].
fn update_of(member: &Member, always_inline: bool) -> Update {
	let inline = always_inline
		|| (member.state == MemberState::Alive && member.meta.len() <= wire::INLINE_META_MAX);
	Update {
		id:           member.id,
		addr:         member.addr,
		state:        member.state,
		incarnation:  member.incarnation,
		meta_version: member.meta_version,
		meta:         inline.then(|| member.meta.clone()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const LOCAL: NodeId = NodeId(1);
	const PEER: NodeId = NodeId(2);

	fn addr(port: u16) -> SocketAddr {
		SocketAddr::from(([127, 0, 0, 1], port))
	}

	fn member(id: NodeId, port: u16) -> Member {
		Member {
			id,
			addr: addr(port),
			state: MemberState::Alive,
			incarnation: 10,
			meta_version: 5,
			meta: b"meta".to_vec(),
		}
	}

	fn update(state: MemberState, incarnation: u64) -> Update {
		Update {
			id: PEER,
			addr: addr(2),
			state,
			incarnation,
			meta_version: 5,
			meta: Some(b"meta".to_vec()),
		}
	}

	/// Table with LOCAL and PEER (Alive, incarnation 10), queue drained.
	fn table() -> (Table, Instant) {
		let now = Instant::now();
		let mut t = Table::new(member(LOCAL, 1), now);
		let out = t.apply(update(MemberState::Alive, 10), Origin::Gossip, now);
		assert!(matches!(out.event, Some(MemberEvent::Joined(_))));
		t.queue.clear();
		(t, now)
	}

	fn peer(t: &Table) -> Member {
		t.get(PEER).unwrap()
	}

	#[test]
	fn stale_or_equal_alive_is_ignored() {
		let (mut t, now) = table();
		t.apply(update(MemberState::Suspect, 10), Origin::Gossip, now);
		let out = t.apply(update(MemberState::Alive, 10), Origin::Gossip, now);
		assert_eq!(out, Outcome::default());
		assert_eq!(peer(&t).state, MemberState::Suspect);
		let out = t.apply(update(MemberState::Alive, 11), Origin::Gossip, now);
		assert!(matches!(&out.event, Some(MemberEvent::Updated(m)) if m.state == MemberState::Alive));
		assert_eq!(peer(&t).incarnation, 11);
	}

	#[test]
	fn suspect_dead_left_precedence() {
		let (mut t, now) = table();
		assert_eq!(t.apply(update(MemberState::Suspect, 9), Origin::Gossip, now), Outcome::default());
		assert_eq!(peer(&t).state, MemberState::Alive);
		t.apply(update(MemberState::Suspect, 10), Origin::Gossip, now);
		assert_eq!(peer(&t).state, MemberState::Suspect);
		let out = t.apply(update(MemberState::Dead, 10), Origin::Gossip, now);
		assert!(matches!(out.event, Some(MemberEvent::Failed(_))));
		// Suspect never resurrects a dead member, even at a higher incarnation.
		t.apply(update(MemberState::Suspect, 12), Origin::Gossip, now);
		assert_eq!(peer(&t).state, MemberState::Dead);
		let out = t.apply(update(MemberState::Left, 10), Origin::Gossip, now);
		assert!(matches!(out.event, Some(MemberEvent::Left(_))));
		let out = t.apply(update(MemberState::Alive, 11), Origin::Gossip, now);
		assert!(matches!(out.event, Some(MemberEvent::Joined(_))));
	}

	#[test]
	fn push_pull_dead_downgrades_to_suspect() {
		let (mut t, now) = table();
		let out = t.apply(update(MemberState::Dead, 10), Origin::PushPull, now);
		assert!(matches!(out.event, Some(MemberEvent::Updated(_))));
		assert_eq!(peer(&t).state, MemberState::Suspect);
	}

	#[test]
	fn unknown_dead_or_left_is_not_inserted() {
		let now = Instant::now();
		let mut t = Table::new(member(LOCAL, 1), now);
		t.apply(update(MemberState::Dead, 3), Origin::Gossip, now);
		t.apply(update(MemberState::Left, 3), Origin::Gossip, now);
		assert!(t.get(PEER).is_none());
	}

	#[test]
	fn restart_with_new_addr_replaces_entry_only_with_higher_incarnation() {
		let (mut t, now) = table();
		let mut moved = update(MemberState::Alive, 10);
		moved.addr = addr(99);
		t.apply(moved.clone(), Origin::Gossip, now);
		assert_eq!(peer(&t).addr, addr(2));
		moved.incarnation = 11;
		let out = t.apply(moved, Origin::Gossip, now);
		assert!(matches!(&out.event, Some(MemberEvent::Updated(m)) if m.addr == addr(99)));
	}

	#[test]
	fn local_member_refutes_suspicion_and_foreign_meta() {
		let (mut t, now) = table();
		let mut claim = update(MemberState::Suspect, 10);
		claim.id = LOCAL;
		claim.meta_version = 9;
		t.apply(claim, Origin::Gossip, now);
		let local = t.local().clone();
		assert_eq!(local.state, MemberState::Alive);
		assert_eq!(local.incarnation, 11);
		assert_eq!(local.meta_version, 10);
		assert_eq!(local.meta, b"meta");
		assert!(t.local_broadcast_pending());
		// A stale suspicion does not bump again.
		let mut stale = update(MemberState::Dead, 10);
		stale.id = LOCAL;
		stale.meta_version = 1;
		t.queue.clear();
		t.apply(stale, Origin::Gossip, now);
		assert_eq!(t.local().incarnation, 11);
		assert!(!t.local_broadcast_pending());
	}

	#[test]
	fn newer_meta_wins_and_missing_bytes_request_fetch() {
		let (mut t, now) = table();
		let mut upd = update(MemberState::Alive, 10);
		upd.meta_version = 4;
		upd.meta = Some(b"old".to_vec());
		assert_eq!(t.apply(upd.clone(), Origin::Gossip, now), Outcome::default());
		upd.meta_version = 6;
		upd.meta = None;
		let out = t.apply(upd.clone(), Origin::Gossip, now);
		assert_eq!(out, Outcome { event: None, fetch_meta: Some(addr(2)) });
		assert_eq!(peer(&t).meta_version, 5);
		upd.meta = Some(b"new".to_vec());
		let out = t.apply(upd, Origin::Gossip, now);
		assert!(matches!(&out.event, Some(MemberEvent::Updated(m)) if m.meta == b"new"));
	}

	#[test]
	fn suspicion_expires_and_dead_members_are_reaped() {
		let (mut t, now) = table();
		t.suspect(PEER, 10, now);
		let timeout = Duration::from_secs(5);
		let reap = Duration::from_mins(1);
		assert_eq!(t.expire(now + Duration::from_secs(4), timeout, reap), Vec::new());
		let events = t.expire(now + timeout, timeout, reap);
		assert!(matches!(events.as_slice(), [MemberEvent::Failed(m)] if m.id == PEER));
		assert_eq!(t.expire(now + timeout + Duration::from_secs(59), timeout, reap), Vec::new());
		assert!(t.get(PEER).is_some());
		t.expire(now + timeout + reap, timeout, reap);
		assert!(t.get(PEER).is_none());
		assert!(t.get(LOCAL).is_some());
	}

	#[test]
	fn failed_probe_of_old_incarnation_does_not_suspect_refuted_member() {
		let (mut t, now) = table();
		t.apply(update(MemberState::Alive, 12), Origin::Gossip, now);
		t.suspect(PEER, 10, now);
		assert_eq!(peer(&t).state, MemberState::Alive);
	}

	#[test]
	fn broadcasts_prefer_least_transmitted_fit_budget_and_retire() {
		let now = Instant::now();
		let mut t = Table::new(member(LOCAL, 1), now);
		for id in 2..6 {
			let mut upd = update(MemberState::Alive, 1);
			upd.id = NodeId(id);
			t.apply(upd, Origin::Gossip, now);
		}
		let one = t.queue[0].bytes.len();
		// Budget for two updates: the newest untransmitted ones go first.
		let (count, bytes) = t.take_broadcasts(one * 2 + 1, 3);
		assert_eq!((count, bytes.len()), (2, one * 2));
		let first: Vec<u64> = t
			.queue
			.iter()
			.filter(|b| b.transmits == 1)
			.map(|b| b.id.0)
			.collect();
		assert_eq!(first.len(), 2);
		assert!(first.contains(&5) && first.contains(&4));
		let (count, _) = t.take_broadcasts(one * 3, 3);
		assert_eq!(count, 3);
		assert!(t.queue.iter().all(|b| b.transmits == 1));
		// Every update reaches the limit after enough packets and is retired.
		for _ in 0..3 {
			t.take_broadcasts(usize::MAX, 3);
		}
		assert!(!t.has_broadcasts());
		// Nothing fits a too-small budget.
		t.set_local_meta(vec![0; 10]);
		assert_eq!(t.take_broadcasts(10, 3), (0, Vec::new()));
	}

	#[test]
	fn large_meta_is_not_inlined_in_udp_updates() {
		let mut m = member(LOCAL, 1);
		m.meta = vec![b'x'; wire::INLINE_META_MAX + 1];
		assert!(update_of(&m, false).meta.is_none());
		assert!(update_of(&m, true).meta.is_some());
		m.meta.truncate(wire::INLINE_META_MAX);
		assert!(update_of(&m, false).meta.is_some());
		m.state = MemberState::Suspect;
		assert!(update_of(&m, false).meta.is_none());
	}

	#[test]
	fn log_scale_matches_ceil_log10() {
		let now = Instant::now();
		let mut t = Table::new(member(LOCAL, 1), now);
		// n = 1 live → ⌈log10(2)⌉ = 1.
		assert_eq!(t.log_scale(4), 4);
		for id in 2..=9 {
			let mut upd = update(MemberState::Alive, 1);
			upd.id = NodeId(id);
			t.apply(upd, Origin::Gossip, now);
		}
		// n = 9 → ⌈log10(10)⌉ = 1.
		assert_eq!(t.log_scale(4), 4);
		let mut upd = update(MemberState::Alive, 1);
		upd.id = NodeId(10);
		t.apply(upd, Origin::Gossip, now);
		// n = 10 → ⌈log10(11)⌉ = 2.
		assert_eq!(t.log_scale(4), 8);
	}
}
