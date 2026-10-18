//! SWIM-style gossip memberlist (ARCHITECTURE.md §5.2).
//!
//! Every vibemon process runs one [`Gossip`] instance. Failure detection uses
//! UDP probes (`Ping`/`Ack`, then `PingReq`/`IndirectAck` through `k` helpers);
//! member updates piggyback on every packet, plus periodic `Gossip` packets to
//! a few random peers while updates are pending. A TCP push-pull full-state
//! exchange on the same port runs on join, periodically with a random alive
//! member (anti-entropy), with a random dead member (partition healing), and on
//! demand to fetch metadata too large to inline in a UDP packet. Every packet
//! and frame carries an HMAC-SHA256 tag keyed by `HKDF(secret, "vmon-gossip")`;
//! anything else is dropped and counted ([`Gossip::dropped_packets`]).
//!
//! Members advertise every address they are reachable at. Each process ranks a
//! peer's addresses for itself ([`crate::addrs::rank`]: LAN first) and reaches
//! it through one of them, its route ([`Member::addr`]); probes fail over to
//! another address when the route stops answering and move back to a
//! better-ranked one once it answers again.

mod table;
#[cfg(test)]
mod tests;
mod wire;

use std::{
	collections::{HashMap, HashSet},
	fmt, io,
	net::{IpAddr, SocketAddr},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
	},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream, UdpSocket},
	sync::{broadcast, mpsc},
	task::JoinHandle,
	time::{Interval, MissedTickBehavior},
};
use tracing::{debug, trace};

use self::{
	table::{Origin, Table},
	wire::{Message, PushPull, Update},
};
use crate::{addrs, meta::NodeId};

/// Capacity of the [`Gossip::subscribe`] channel; slow subscribers lag.
const EVENT_CAPACITY: usize = 1024;

/// Gossip settings. [`GossipConfig::new`] fills production timings; tests
/// shorten them.
#[derive(Clone)]
pub struct GossipConfig {
	/// Identity of this process.
	pub id:                 NodeId,
	/// UDP + TCP listen address. Port 0 binds an ephemeral port (same for both).
	pub bind:               SocketAddr,
	/// Addresses peers may reach us at, in preference order (at most 16,
	/// duplicates dropped). Port 0 → the bound port. Empty → the bind address
	/// when its IP is specified, else every [`addrs::advertisable`] address of
	/// the bind family, re-read every `refresh_interval`.
	pub advertise:          Vec<SocketAddr>,
	/// Shared cluster secret; the packet key is `HKDF(secret, "vmon-gossip")`.
	pub secret:             Vec<u8>,
	/// Period between failure-detector probes (one member per period).
	pub probe_interval:     Duration,
	/// Wait for a direct `Ack` before asking helpers for indirect probes.
	pub ack_timeout:        Duration,
	/// Number of helpers asked to probe an unresponsive member (`k`).
	pub indirect_probes:    usize,
	/// Suspicion timeout = `suspicion_mult × max(1, ⌈log10(n+1)⌉) ×
	/// probe_interval`, after which a suspect is declared `Dead`.
	pub suspicion_mult:     u32,
	/// Each update is piggybacked `retransmit_mult × max(1, ⌈log10(n+1)⌉)`
	/// times before it is retired.
	pub retransmit_mult:    u32,
	/// Period of dedicated `Gossip` packets while updates are pending; also the
	/// tick for suspicion expiry and reaping.
	pub gossip_interval:    Duration,
	/// Number of random peers each `Gossip` round sends to.
	pub gossip_fanout:      usize,
	/// Period of the anti-entropy push-pull with a random member.
	pub push_pull_interval: Duration,
	/// Deadline for one whole TCP push-pull exchange.
	pub tcp_timeout:        Duration,
	/// `Dead`/`Left` members are removed from the table after this long.
	pub reap_after:         Duration,
	/// Period of re-reading this host's interfaces: peers' addresses are
	/// re-ranked, and an automatic `advertise` list is re-advertised when it
	/// changed.
	pub refresh_interval:   Duration,
}

impl GossipConfig {
	/// Builds a config with production timings (probe 1 s, ack 500 ms, k=3,
	/// suspicion ×5, retransmit ×4, gossip 200 ms × 3, push-pull 30 s, TCP
	/// 10 s, reap 1 h, interface refresh 30 s).
	pub const fn new(
		id: NodeId,
		bind: SocketAddr,
		advertise: Vec<SocketAddr>,
		secret: Vec<u8>,
	) -> Self {
		Self {
			id,
			bind,
			advertise,
			secret,
			probe_interval: Duration::from_secs(1),
			ack_timeout: Duration::from_millis(500),
			indirect_probes: 3,
			suspicion_mult: 5,
			retransmit_mult: 4,
			gossip_interval: Duration::from_millis(200),
			gossip_fanout: 3,
			push_pull_interval: Duration::from_secs(30),
			tcp_timeout: Duration::from_secs(10),
			reap_after: Duration::from_hours(1),
			refresh_interval: Duration::from_secs(30),
		}
	}
}

impl fmt::Debug for GossipConfig {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("GossipConfig")
			.field("id", &self.id)
			.field("bind", &self.bind)
			.field("advertise", &self.advertise)
			.field("secret", &"<redacted>")
			.field("probe_interval", &self.probe_interval)
			.field("ack_timeout", &self.ack_timeout)
			.field("indirect_probes", &self.indirect_probes)
			.field("suspicion_mult", &self.suspicion_mult)
			.field("retransmit_mult", &self.retransmit_mult)
			.field("gossip_interval", &self.gossip_interval)
			.field("gossip_fanout", &self.gossip_fanout)
			.field("push_pull_interval", &self.push_pull_interval)
			.field("tcp_timeout", &self.tcp_timeout)
			.field("reap_after", &self.reap_after)
			.field("refresh_interval", &self.refresh_interval)
			.finish()
	}
}

/// Liveness of a member as seen by the local failure detector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberState {
	/// Answering probes.
	Alive,
	/// Missed a probe round; becomes `Dead` unless it refutes in time.
	Suspect,
	/// Suspicion expired without refutation (crash or partition).
	Dead,
	/// Left gracefully via [`Gossip::leave`].
	Left,
}

/// One cluster member as known locally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
	/// Stable process identity.
	pub id:           NodeId,
	/// Address this process currently reaches the member at (UDP + TCP), its
	/// route: one of `addrs`, picked by the local failure detector. For the
	/// local member, its first advertised address.
	pub addr:         SocketAddr,
	/// Every address the member advertises, best-ranked first for this host
	/// ([`addrs::rank`]; equal ranks keep the member's order). The local
	/// member lists its own in advertised order.
	pub addrs:        Vec<SocketAddr>,
	/// Local view of liveness.
	pub state:        MemberState,
	/// SWIM incarnation; bumped by the member to refute suspicion or to leave.
	pub incarnation:  u64,
	/// Version of `meta`; bumped by every [`Gossip::set_meta`].
	pub meta_version: u64,
	/// Opaque metadata (vibemon encodes `MemberMeta` JSON). Empty until known.
	pub meta:         Vec<u8>,
}

/// Membership change published to [`Gossip::subscribe`] receivers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberEvent {
	/// A new member appeared, or a `Dead`/`Left` member came back.
	Joined(Member),
	/// Metadata, advertised addresses, route, or `Alive`↔`Suspect` state
	/// changed (also emitted for the local member on [`Gossip::set_meta`] and
	/// [`Gossip::set_addrs`]).
	Updated(Member),
	/// A member was declared `Dead`.
	Failed(Member),
	/// A member left gracefully.
	Left(Member),
}

impl MemberEvent {
	/// The member snapshot carried by any event variant.
	pub const fn member(&self) -> &Member {
		match self {
			Self::Joined(m) | Self::Updated(m) | Self::Failed(m) | Self::Left(m) => m,
		}
	}
}

/// Handle to a running gossip instance; cheap to clone. Dropping the last
/// handle stops all background tasks without announcing `Left` (crash-like);
/// call [`Gossip::leave`] for a graceful exit.
#[derive(Clone)]
pub struct Gossip {
	inner: Arc<Inner>,
	tasks: Arc<Tasks>,
}

impl fmt::Debug for Gossip {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Gossip")
			.field("id", &self.inner.cfg.id)
			.field("addrs", &self.inner.table.lock().local().addrs)
			.finish_non_exhaustive()
	}
}

impl Gossip {
	/// Binds UDP + TCP on `cfg.bind`, starts the background tasks, and returns
	/// a handle whose only member is the local one (with `meta`). Called once
	/// per process by the cluster facade.
	pub async fn start(cfg: GossipConfig, meta: Vec<u8>) -> io::Result<Self> {
		let (udp, listener) = bind_pair(cfg.bind).await?;
		let bound = udp.local_addr()?;
		let cfg = GossipConfig { bind: bound, ..cfg };
		let auto_advertise = cfg.advertise.is_empty() && bound.ip().is_unspecified();
		let advertise = if auto_advertise {
			advertisable(bound)
		} else if cfg.advertise.is_empty() {
			vec![bound]
		} else {
			resolve_advertise(&cfg.advertise, bound)?
		};
		// Wall-clock start values make a restarted member (same id) outrank its
		// previous life immediately; refutation covers clock regressions.
		let epoch_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(1, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
		debug!(id = %cfg.id, bind = %bound, ?advertise, "gossip starting");
		let local = Member {
			id: cfg.id,
			addr: advertise[0],
			addrs: advertise,
			state: MemberState::Alive,
			incarnation: epoch_ms,
			meta_version: epoch_ms,
			meta,
		};
		let inner = Arc::new(Inner {
			auth: wire::Auth::new(&cfg.secret),
			table: Mutex::new(Table::new(local, addrs::interfaces(), Instant::now())),
			cfg,
			auto_advertise,
			udp,
			waiters: Mutex::new(HashMap::new()),
			seq: AtomicU32::new(rand::random()),
			events: broadcast::channel(EVENT_CAPACITY).0,
			dropped: AtomicU64::new(0),
			fetching: Mutex::new(HashSet::new()),
			verifying: Mutex::new(HashSet::new()),
			stopped: AtomicBool::new(false),
			#[cfg(test)]
			blocked: Mutex::new(HashSet::new()),
		});
		let handles = vec![
			tokio::spawn(Arc::clone(&inner).recv_loop()),
			tokio::spawn(Arc::clone(&inner).accept_loop(listener)),
			tokio::spawn(Arc::clone(&inner).probe_loop()),
			tokio::spawn(Arc::clone(&inner).gossip_loop()),
			tokio::spawn(Arc::clone(&inner).push_pull_loop()),
			tokio::spawn(Arc::clone(&inner).refresh_loop()),
		];
		Ok(Self { inner, tasks: Arc::new(Tasks(Mutex::new(handles))) })
	}

	/// Push-pulls with every seed (`host:port`, DNS resolved, concurrently;
	/// addresses of this instance are skipped) and returns how many seeds were
	/// contacted. `Ok(0)` for an empty list or self-only seeds; an error when
	/// at least one foreign seed was given and none could be contacted.
	pub async fn join(&self, seeds: &[String]) -> io::Result<usize> {
		if self.inner.stopped.load(Ordering::SeqCst) {
			return Err(io::Error::new(io::ErrorKind::NotConnected, "gossip instance has left"));
		}
		let results =
			futures::future::join_all(seeds.iter().map(|seed| self.inner.join_seed(seed))).await;
		let mut contacted = 0;
		let mut last_err = None;
		for result in results {
			match result {
				Ok(true) => contacted += 1,
				Ok(false) => {},
				Err(err) => last_err = Some(err),
			}
		}
		match last_err {
			Some(err) if contacted == 0 => Err(err),
			_ => Ok(contacted),
		}
	}

	/// First advertised address of this instance (what seeds lists use).
	pub fn local_addr(&self) -> SocketAddr {
		self.inner.table.lock().local().addr
	}

	/// All known members, including the local one and not-yet-reaped
	/// `Dead`/`Left` members.
	pub fn members(&self) -> Vec<Member> {
		self.inner.table.lock().members()
	}

	/// One member by id (the local id returns the local member).
	pub fn member(&self, id: NodeId) -> Option<Member> {
		self.inner.table.lock().get(id)
	}

	/// Replaces the local metadata, bumping `meta_version`, and disseminates
	/// it. Callers rate-limit (≥ 1 s apart).
	pub fn set_meta(&self, meta: Vec<u8>) {
		let member = self.inner.table.lock().set_local_meta(meta);
		self.inner.publish(MemberEvent::Updated(member));
	}

	/// Advertises `addrs` from now on (preference order, at most 16, port 0 →
	/// the bound port) with a bumped incarnation and disseminates them; peers
	/// keep their route to this member while it is still advertised. No-op when
	/// unchanged. With an automatic advertise list (wildcard bind, empty
	/// `advertise`), the next interface refresh replaces a differing list.
	pub fn set_addrs(&self, addrs: Vec<SocketAddr>) -> io::Result<()> {
		let addrs = resolve_advertise(&addrs, self.inner.cfg.bind)?;
		self.inner.set_local_addrs(addrs);
		Ok(())
	}

	/// Subscribes to membership events from now on.
	pub fn subscribe(&self) -> broadcast::Receiver<MemberEvent> {
		self.inner.events.subscribe()
	}

	/// Number of packets/frames dropped for failed authentication or decoding.
	pub fn dropped_packets(&self) -> u64 {
		self.inner.dropped.load(Ordering::Relaxed)
	}

	/// Announces `Left` (bumped incarnation), waits until the announcement has
	/// been disseminated or `timeout` elapses, then stops all tasks. Idempotent.
	pub async fn leave(&self, timeout: Duration) {
		if self.inner.stopped.swap(true, Ordering::SeqCst) {
			return;
		}
		self.inner.table.lock().leave(Instant::now());
		self.inner.gossip_round();
		let deadline = tokio::time::Instant::now() + timeout;
		loop {
			let pending = {
				let table = self.inner.table.lock();
				table.local_broadcast_pending() && table.live_count() > 0
			};
			let now = tokio::time::Instant::now();
			if !pending || now >= deadline {
				break;
			}
			tokio::time::sleep(self.inner.cfg.gossip_interval.min(deadline - now)).await;
		}
		self.tasks.abort_all();
		debug!(id = %self.inner.cfg.id, "gossip left");
	}
}

/// Background task handles; aborted on `leave` or when the last handle drops.
struct Tasks(Mutex<Vec<JoinHandle<()>>>);

impl Tasks {
	fn abort_all(&self) {
		for handle in self.0.lock().drain(..) {
			handle.abort();
		}
	}
}

impl Drop for Tasks {
	fn drop(&mut self) {
		self.abort_all();
	}
}

/// Pending acknowledgement for a sequence number.
enum Waiter {
	/// One of our own pings, signalled by `Ack` or `IndirectAck`: reports
	/// `addr` on `tx`, the address pinged directly (`None`: the `PingReq`s of
	/// a probe round).
	Probe {
		tx:       mpsc::UnboundedSender<Option<SocketAddr>>,
		addr:     Option<SocketAddr>,
		deadline: Instant,
	},
	/// A probe we run for `requester`'s `PingReq{seq}`.
	Relay { requester: SocketAddr, seq: u32, deadline: Instant },
}

impl Waiter {
	const fn deadline(&self) -> Instant {
		match self {
			Self::Probe { deadline, .. } | Self::Relay { deadline, .. } => *deadline,
		}
	}
}

/// Acknowledgements gathered by one probe (or route verification) of a
/// member.
struct ProbeRound {
	tx:       mpsc::UnboundedSender<Option<SocketAddr>>,
	rx:       mpsc::UnboundedReceiver<Option<SocketAddr>>,
	/// The member's addresses, best-ranked first.
	addrs:    Vec<SocketAddr>,
	/// Sequence numbers of the round's pings (dropped from the waiters at the
	/// end).
	seqs:     Vec<u32>,
	/// Index in `addrs` of the best-ranked address that acked directly.
	best:     Option<usize>,
	/// A `PingReq` helper reached the member.
	indirect: bool,
}

impl ProbeRound {
	fn new(addrs: Vec<SocketAddr>) -> Self {
		let (tx, rx) = mpsc::unbounded_channel();
		Self { tx, rx, addrs, seqs: Vec::new(), best: None, indirect: false }
	}

	/// Records acks until `done` holds or `within` elapses.
	async fn wait(&mut self, within: Duration, done: fn(&Self) -> bool) {
		let deadline = tokio::time::Instant::now() + within;
		while !done(self) {
			let Ok(Some(ack)) = tokio::time::timeout_at(deadline, self.rx.recv()).await else {
				return;
			};
			match ack {
				Some(addr) => {
					if let Some(index) = self.addrs.iter().position(|a| *a == addr) {
						self.best = Some(self.best.map_or(index, |best| best.min(index)));
					}
				},
				None => self.indirect = true,
			}
		}
	}
}

struct Inner {
	cfg:            GossipConfig,
	/// `advertise` follows the interfaces (wildcard bind, no configured list).
	auto_advertise: bool,
	auth:           wire::Auth,
	udp:            UdpSocket,
	table:          Mutex<Table>,
	waiters:        Mutex<HashMap<u32, Waiter>>,
	seq:            AtomicU32,
	events:         broadcast::Sender<MemberEvent>,
	dropped:        AtomicU64,
	/// Members with an in-flight metadata fetch (deduplicates fetches).
	fetching:       Mutex<HashSet<NodeId>>,
	/// Members with an in-flight route verification.
	verifying:      Mutex<HashSet<NodeId>>,
	/// Set by `leave`; blocks joins, new fetches, and route verifications.
	stopped:        AtomicBool,
	/// Test-only partition: destinations we refuse to send to or connect to.
	#[cfg(test)]
	blocked:        Mutex<HashSet<SocketAddr>>,
}

impl Inner {
	fn next_seq(&self) -> u32 {
		self.seq.fetch_add(1, Ordering::Relaxed)
	}

	#[cfg(test)]
	fn is_blocked(&self, addr: SocketAddr) -> bool {
		self.blocked.lock().contains(&addr)
	}

	#[cfg(not(test))]
	#[allow(clippy::unused_self, reason = "test builds consult a partition set")]
	const fn is_blocked(&self, _addr: SocketAddr) -> bool {
		false
	}

	/// Sends `msg` to `to`, piggybacking as many pending updates as fit.
	fn send(&self, msg: Message, to: SocketAddr) {
		self.send_packet(msg, to, true);
	}

	/// Sends `msg` to `to`, with pending updates only if `piggyback`: extra
	/// probe pings must not spend the updates' retransmit budget.
	fn send_packet(&self, msg: Message, to: SocketAddr, piggyback: bool) {
		if self.is_blocked(to) {
			return;
		}
		let mut buf = wire::encode_message(msg);
		if piggyback {
			let budget = wire::MAX_PACKET.saturating_sub(buf.len() + 1 + wire::TAG_LEN);
			let (count, updates) = {
				let mut table = self.table.lock();
				let limit = table.log_scale(self.cfg.retransmit_mult);
				table.take_broadcasts(budget, limit)
			};
			buf.push(count);
			buf.extend_from_slice(&updates);
		} else {
			buf.push(0);
		}
		self.auth.seal(&mut buf);
		// A dual-stack socket reaches IPv4 peers at their v4-mapped address.
		let dest = match to {
			SocketAddr::V4(v4) if self.cfg.bind.is_ipv6() => {
				SocketAddr::new(IpAddr::V6(v4.ip().to_ipv6_mapped()), v4.port())
			},
			_ => to,
		};
		if let Err(err) = self.udp.try_send_to(&buf, dest) {
			trace!(%to, %err, "gossip send failed");
		}
	}

	fn publish(&self, event: MemberEvent) {
		let member = event.member();
		debug!(id = %member.id, addr = %member.addr, state = ?member.state, ?event, "gossip member event");
		// No receivers is fine.
		let _ = self.events.send(event);
	}

	/// Advertises `addrs` for the local member and publishes the change.
	fn set_local_addrs(&self, addrs: Vec<SocketAddr>) {
		let member = self.table.lock().set_local_addrs(addrs);
		if let Some(member) = member {
			self.publish(MemberEvent::Updated(member));
		}
	}

	/// Routes traffic for `id` through `addr` and publishes a change.
	fn set_route(&self, id: NodeId, addr: SocketAddr) {
		let event = self.table.lock().set_route(id, addr);
		if let Some(event) = event {
			self.publish(event);
		}
	}

	/// Applies updates and publishes resulting events, metadata fetches, and
	/// route verifications.
	fn merge(self: &Arc<Self>, updates: Vec<Update>, origin: Origin) {
		let now = Instant::now();
		let outcomes: Vec<_> = {
			let mut table = self.table.lock();
			updates
				.into_iter()
				.map(|update| (update.id, table.apply(update, origin, now)))
				.collect()
		};
		for (id, outcome) in outcomes {
			if let Some(event) = outcome.event {
				self.publish(event);
			}
			if outcome.verify_route {
				self.verify_route(id);
			}
			if outcome.fetch_meta {
				self.fetch_meta(id);
			}
		}
	}

	/// Pulls metadata that was too large to inline via push-pull with its owner.
	fn fetch_meta(self: &Arc<Self>, id: NodeId) {
		if self.stopped.load(Ordering::SeqCst) || !self.fetching.lock().insert(id) {
			return;
		}
		let inner = Arc::clone(self);
		tokio::spawn(async move {
			if let Err(err) = inner.push_pull_member(id).await {
				debug!(%id, %err, "gossip metadata fetch failed");
			}
			inner.fetching.lock().remove(&id);
		});
	}

	/// Pings every address of a member whose route is unverified (new, back,
	/// or re-addressed) and routes through the best-ranked one answering
	/// within `ack_timeout`. Until then, and when nothing answers, the route
	/// stays the best-ranked address; probes take over from there.
	fn verify_route(self: &Arc<Self>, id: NodeId) {
		if self.stopped.load(Ordering::SeqCst) || !self.verifying.lock().insert(id) {
			return;
		}
		let inner = Arc::clone(self);
		tokio::spawn(async move {
			let target = inner.table.lock().target(id);
			if let Some(target) = target {
				let mut round = ProbeRound::new(target.addrs);
				for index in 0..round.addrs.len() {
					inner.ping(&mut round, id, index, false);
				}
				round
					.wait(inner.cfg.ack_timeout, |round| round.best == Some(0))
					.await;
				inner.finish(&round);
				if let Some(best) = round.best {
					inner.set_route(id, round.addrs[best]);
				}
			}
			inner.verifying.lock().remove(&id);
		});
	}

	fn handle_packet(self: &Arc<Self>, buf: &[u8], from: SocketAddr) {
		let Some((msg, updates)) = self.auth.open(buf).and_then(wire::decode_packet) else {
			self.dropped.fetch_add(1, Ordering::Relaxed);
			trace!(%from, len = buf.len(), "dropped unauthenticated or malformed gossip packet");
			return;
		};
		self.merge(updates, Origin::Gossip);
		match msg {
			Message::Ping { seq, target } => {
				if target == self.cfg.id {
					self.send(Message::Ack { seq }, from);
				}
			},
			Message::Ack { seq } | Message::IndirectAck { seq } => self.resolve(seq),
			Message::PingReq { seq, target, addr } => {
				// The requester's route failed; ours may differ and work.
				let route = self.table.lock().route(target);
				self.relay(from, seq, target, addr, true);
				if let Some(route) = route.filter(|route| *route != addr) {
					self.relay(from, seq, target, route, false);
				}
			},
			Message::Gossip => {},
		}
	}

	/// Pings `target` at `addr` on behalf of `requester`'s `PingReq{seq}`.
	fn relay(
		&self,
		requester: SocketAddr,
		seq: u32,
		target: NodeId,
		addr: SocketAddr,
		piggyback: bool,
	) {
		let relay_seq = self.next_seq();
		let deadline = Instant::now() + self.cfg.probe_interval;
		self
			.waiters
			.lock()
			.insert(relay_seq, Waiter::Relay { requester, seq, deadline });
		self.send_packet(Message::Ping { seq: relay_seq, target }, addr, piggyback);
	}

	fn resolve(&self, seq: u32) {
		let waiter = self.waiters.lock().remove(&seq);
		match waiter {
			Some(Waiter::Probe { tx, addr, .. }) => {
				let _ = tx.send(addr);
			},
			Some(Waiter::Relay { requester, seq, .. }) => {
				self.send(Message::IndirectAck { seq }, requester);
			},
			None => {},
		}
	}

	/// Pings `target` at its address `round.addrs[index]` as part of `round`
	/// (`piggyback`: only the route ping carries updates).
	fn ping(&self, round: &mut ProbeRound, target: NodeId, index: usize, piggyback: bool) {
		let addr = round.addrs[index];
		let seq = self.next_seq();
		round.seqs.push(seq);
		let deadline = self.probe_deadline();
		let waiter = Waiter::Probe { tx: round.tx.clone(), addr: Some(addr), deadline };
		self.waiters.lock().insert(seq, waiter);
		self.send_packet(Message::Ping { seq, target }, addr, piggyback);
	}

	/// Forgets the pending acks of a finished round.
	fn finish(&self, round: &ProbeRound) {
		let mut waiters = self.waiters.lock();
		for seq in &round.seqs {
			waiters.remove(seq);
		}
	}

	/// Waiter expiry for probe pings (backstop for `finish`).
	fn probe_deadline(&self) -> Instant {
		Instant::now() + self.cfg.probe_interval + self.cfg.ack_timeout * 2
	}

	/// One SWIM probe round of the next member (ARCHITECTURE.md §5.2):
	/// 1. ping its route (piggybacking updates) and every better-ranked address,
	///    and wait up to `ack_timeout`;
	/// 2. without a direct ack, ping its remaining addresses and ask `k` helpers
	///    to probe the route, and wait the rest of the probe interval for a
	///    direct ack.
	///
	/// A direct ack moves the route to the best-ranked address that answered;
	/// an indirect ack only confirms liveness; silence suspects exactly the
	/// probed incarnation.
	async fn probe(self: &Arc<Self>) {
		let Some(target) = self.table.lock().next_probe_target() else {
			return;
		};
		let route = target
			.addrs
			.iter()
			.position(|addr| *addr == target.route)
			.unwrap_or(0);
		let mut round = ProbeRound::new(target.addrs);
		self.ping(&mut round, target.id, route, true);
		for index in 0..route {
			self.ping(&mut round, target.id, index, false);
		}
		round
			.wait(self.cfg.ack_timeout, |round| round.best == Some(0))
			.await;
		if round.best.is_none() {
			for index in route + 1..round.addrs.len() {
				self.ping(&mut round, target.id, index, false);
			}
			let helpers = self.table.lock().random_peers(
				&[MemberState::Alive],
				Some(target.id),
				self.cfg.indirect_probes,
			);
			if !helpers.is_empty() {
				let seq = self.next_seq();
				round.seqs.push(seq);
				let deadline = self.probe_deadline();
				let waiter = Waiter::Probe { tx: round.tx.clone(), addr: None, deadline };
				self.waiters.lock().insert(seq, waiter);
				let msg = Message::PingReq { seq, target: target.id, addr: target.route };
				for (_, helper) in helpers {
					self.send(msg, helper);
				}
			}
			let rest = self
				.cfg
				.probe_interval
				.saturating_sub(self.cfg.ack_timeout)
				.max(self.cfg.ack_timeout);
			round.wait(rest, |round| round.best.is_some()).await;
		}
		self.finish(&round);
		if let Some(best) = round.best {
			self.set_route(target.id, round.addrs[best]);
		} else if !round.indirect {
			debug!(id = %target.id, route = %target.route, "gossip probe failed");
			let outcome = self
				.table
				.lock()
				.suspect(target.id, target.incarnation, Instant::now());
			if let Some(event) = outcome.event {
				self.publish(event);
			}
		}
	}

	/// Expires suspicions, reaps gone members, and drops stale ack waiters.
	fn maintain(&self) {
		let now = Instant::now();
		let events = {
			let mut table = self.table.lock();
			let suspicion = self
				.cfg
				.probe_interval
				.saturating_mul(table.log_scale(self.cfg.suspicion_mult));
			table.expire(now, suspicion, self.cfg.reap_after)
		};
		for event in events {
			self.publish(event);
		}
		self
			.waiters
			.lock()
			.retain(|_, waiter| waiter.deadline() > now);
	}

	/// Sends pending updates to `gossip_fanout` random live peers.
	fn gossip_round(&self) {
		let targets = {
			let table = self.table.lock();
			if !table.has_broadcasts() {
				return;
			}
			table.random_peers(
				&[MemberState::Alive, MemberState::Suspect],
				None,
				self.cfg.gossip_fanout,
			)
		};
		for (_, addr) in targets {
			self.send(Message::Gossip, addr);
		}
	}

	/// Resolves one seed and push-pulls with its first reachable foreign
	/// address. `Ok(false)` when it only resolves to this instance.
	async fn join_seed(self: &Arc<Self>, seed: &str) -> io::Result<bool> {
		let own = self.table.lock().local().addrs.clone();
		let addrs: Vec<SocketAddr> = tokio::net::lookup_host(seed)
			.await
			.map_err(|err| io::Error::new(err.kind(), format!("gossip seed {seed}: {err}")))?
			.filter(|addr| !own.contains(addr) && *addr != self.cfg.bind)
			.collect();
		let mut last_err = None;
		for addr in addrs {
			match self.push_pull(addr).await {
				Ok(_) => return Ok(true),
				Err(err) => {
					last_err =
						Some(io::Error::new(err.kind(), format!("gossip seed {seed} ({addr}): {err}")));
				},
			}
		}
		last_err.map_or(Ok(false), Err)
	}

	/// Client side of a full-state exchange with `addr`; returns the id of the
	/// member that answered.
	async fn push_pull(self: &Arc<Self>, addr: SocketAddr) -> io::Result<NodeId> {
		if self.is_blocked(addr) {
			return Err(io::Error::new(io::ErrorKind::ConnectionRefused, "blocked by test partition"));
		}
		let exchange = async {
			let mut stream = TcpStream::connect(addr).await?;
			stream.set_nodelay(true)?;
			self.write_state(&mut stream).await?;
			let remote = self.read_state(&mut stream).await?;
			trace!(from = %remote.from, members = remote.members.len(), "gossip push-pull reply");
			self.merge(remote.members, Origin::PushPull);
			Ok(remote.from)
		};
		tokio::time::timeout(self.cfg.tcp_timeout, exchange)
			.await
			.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "gossip push-pull timed out"))?
	}

	/// Push-pull with member `id`, dialing its addresses in
	/// [`Table::dial_order`]; the first one that answers as `id` becomes its
	/// route.
	async fn push_pull_member(self: &Arc<Self>, id: NodeId) -> io::Result<()> {
		let addrs = self.table.lock().dial_order(id);
		let mut last_err = None;
		for addr in addrs {
			match self.push_pull(addr).await {
				Ok(from) if from == id => {
					self.set_route(id, addr);
					return Ok(());
				},
				Ok(from) => last_err = Some(io::Error::other(format!("{addr} answered as {from}"))),
				Err(err) => last_err = Some(io::Error::new(err.kind(), format!("{addr}: {err}"))),
			}
		}
		Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown member")))
	}

	/// Server side: merge the peer's state first (so refutations are already
	/// reflected), then reply with ours.
	async fn serve_push_pull(self: Arc<Self>, mut stream: TcpStream) {
		let exchange = async {
			let remote = self.read_state(&mut stream).await?;
			self.merge(remote.members, Origin::PushPull);
			self.write_state(&mut stream).await
		};
		match tokio::time::timeout(self.cfg.tcp_timeout, exchange).await {
			Ok(Ok(())) => {},
			Ok(Err(err)) => debug!(%err, "gossip push-pull serve failed"),
			Err(_) => debug!("gossip push-pull serve timed out"),
		}
	}

	async fn write_state(&self, stream: &mut TcpStream) -> io::Result<()> {
		let mut body = wire::encode_push_pull(self.cfg.id, &self.table.lock().snapshot());
		self.auth.seal(&mut body);
		let len = u32::try_from(body.len())
			.ok()
			.filter(|_| body.len() <= wire::MAX_FRAME)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "gossip state too large"))?;
		stream.write_all(&len.to_be_bytes()).await?;
		stream.write_all(&body).await?;
		stream.flush().await
	}

	async fn read_state(&self, stream: &mut TcpStream) -> io::Result<PushPull> {
		let len = usize::try_from(stream.read_u32().await?).unwrap_or(usize::MAX);
		if !(wire::TAG_LEN..=wire::MAX_FRAME).contains(&len) {
			self.dropped.fetch_add(1, Ordering::Relaxed);
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"gossip frame length out of range",
			));
		}
		let mut buf = vec![0u8; len];
		stream.read_exact(&mut buf).await?;
		self
			.auth
			.open(&buf)
			.and_then(wire::decode_push_pull)
			.ok_or_else(|| {
				self.dropped.fetch_add(1, Ordering::Relaxed);
				io::Error::new(
					io::ErrorKind::InvalidData,
					"unauthenticated or malformed push-pull frame",
				)
			})
	}

	async fn recv_loop(self: Arc<Self>) {
		let mut buf = vec![0u8; 65536];
		loop {
			match self.udp.recv_from(&mut buf).await {
				Ok((len, from)) => self.handle_packet(&buf[..len], from),
				Err(err) => {
					// ICMP-induced resets (Windows) and transient errors: keep
					// serving.
					trace!(%err, "gossip udp receive error");
					tokio::time::sleep(Duration::from_millis(10)).await;
				},
			}
		}
	}

	async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
		loop {
			match listener.accept().await {
				Ok((stream, _)) => {
					let _ = stream.set_nodelay(true);
					tokio::spawn(Arc::clone(&self).serve_push_pull(stream));
				},
				Err(err) => {
					debug!(%err, "gossip tcp accept error");
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
			}
		}
	}

	async fn probe_loop(self: Arc<Self>) {
		let mut tick = ticker(self.cfg.probe_interval);
		loop {
			tick.tick().await;
			self.probe().await;
		}
	}

	async fn gossip_loop(self: Arc<Self>) {
		let mut tick = ticker(self.cfg.gossip_interval);
		loop {
			tick.tick().await;
			self.maintain();
			self.gossip_round();
		}
	}

	/// Anti-entropy with a random alive member (through its route) plus a
	/// reconnect attempt with a random dead one through each of its addresses
	/// (heals partitions once the network does).
	async fn push_pull_loop(self: Arc<Self>) {
		let mut tick = ticker(self.cfg.push_pull_interval);
		loop {
			tick.tick().await;
			let (alive, dead) = {
				let table = self.table.lock();
				(
					table.random_peers(&[MemberState::Alive], None, 1),
					table.random_peers(&[MemberState::Dead], None, 1),
				)
			};
			for (id, addr) in alive {
				if let Err(err) = self.push_pull(addr).await {
					debug!(%id, %addr, %err, "gossip push-pull failed");
				}
			}
			for (id, _) in dead {
				if let Err(err) = self.push_pull_member(id).await {
					debug!(%id, %err, "gossip push-pull with dead member failed");
				}
			}
		}
	}

	/// Re-reads this host's interfaces every `refresh_interval`: re-ranks
	/// peers' addresses and re-advertises an automatic address list that
	/// changed.
	async fn refresh_loop(self: Arc<Self>) {
		let mut tick = ticker(self.cfg.refresh_interval);
		loop {
			tick.tick().await;
			let interfaces = addrs::interfaces();
			let events = self.table.lock().set_interfaces(interfaces);
			for event in events {
				self.publish(event);
			}
			if self.auto_advertise {
				self.set_local_addrs(advertisable(self.cfg.bind));
			}
		}
	}
}

/// Interval whose first tick fires one period from now; late ticks are delayed.
fn ticker(period: Duration) -> Interval {
	let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
	interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
	interval
}

/// Binds UDP and TCP on the same port; for port 0 retries until both bind.
async fn bind_pair(bind: SocketAddr) -> io::Result<(UdpSocket, TcpListener)> {
	if bind.port() != 0 {
		return Ok((UdpSocket::bind(bind).await?, TcpListener::bind(bind).await?));
	}
	for _ in 0..64 {
		let udp = UdpSocket::bind(bind).await?;
		let port = udp.local_addr()?.port();
		match TcpListener::bind(SocketAddr::new(bind.ip(), port)).await {
			Ok(listener) => return Ok((udp, listener)),
			Err(err) if err.kind() == io::ErrorKind::AddrInUse => {},
			Err(err) => return Err(err),
		}
	}
	Err(io::Error::new(io::ErrorKind::AddrInUse, "no port free for both gossip UDP and TCP"))
}

/// The first [`wire::MAX_ADDRS`] [`addrs::advertisable`] IPs of a wildcard
/// `bound` address, at its port.
fn advertisable(bound: SocketAddr) -> Vec<SocketAddr> {
	addrs::advertisable(bound.ip())
		.into_iter()
		.take(wire::MAX_ADDRS)
		.map(|ip| SocketAddr::new(ip, bound.port()))
		.collect()
}

/// Validates an explicit advertise list: fills zero ports from the bound
/// address and drops duplicates.
fn resolve_advertise(advertise: &[SocketAddr], bound: SocketAddr) -> io::Result<Vec<SocketAddr>> {
	let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidInput, msg);
	let mut out = Vec::with_capacity(advertise.len());
	for addr in advertise {
		if addr.ip().is_unspecified() {
			return Err(invalid(format!("gossip advertise address {addr} has no routable IP")));
		}
		let port = if addr.port() == 0 {
			bound.port()
		} else {
			addr.port()
		};
		let addr = SocketAddr::new(addr.ip(), port);
		if !out.contains(&addr) {
			out.push(addr);
		}
	}
	if !(1..=wire::MAX_ADDRS).contains(&out.len()) {
		return Err(invalid(format!(
			"gossip advertises 1 to {} addresses, not {}",
			wire::MAX_ADDRS,
			out.len()
		)));
	}
	Ok(out)
}
