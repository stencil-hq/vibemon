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

mod table;
#[cfg(test)]
mod tests;
mod wire;

use std::{
	collections::{HashMap, HashSet},
	fmt, io,
	net::SocketAddr,
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
	sync::{broadcast, oneshot},
	task::JoinHandle,
	time::{Interval, MissedTickBehavior},
};
use tracing::{debug, trace};

use self::{
	table::{Origin, Table},
	wire::{Message, PushPull, Update},
};
use crate::meta::NodeId;

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
	/// Address peers use to reach us. Port 0 → the bound port; an unspecified
	/// IP → the bind IP (which then must be specified).
	pub advertise:          SocketAddr,
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
}

impl GossipConfig {
	/// Builds a config with production timings (probe 1 s, ack 500 ms, k=3,
	/// suspicion ×5, retransmit ×4, gossip 200 ms × 3, push-pull 30 s, TCP
	/// 10 s, reap 1 h).
	pub const fn new(id: NodeId, bind: SocketAddr, advertise: SocketAddr, secret: Vec<u8>) -> Self {
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
	/// Advertised gossip address (UDP + TCP).
	pub addr:         SocketAddr,
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
	/// Metadata, address, or `Alive`↔`Suspect` state changed (also emitted for
	/// the local member on [`Gossip::set_meta`]).
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
			.field("addr", &self.inner.cfg.advertise)
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
		let advertise = resolve_advertise(cfg.advertise, bound)?;
		let cfg = GossipConfig { bind: bound, advertise, ..cfg };
		// Wall-clock start values make a restarted member (same id) outrank its
		// previous life immediately; refutation covers clock regressions.
		let epoch_ms = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(1, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
		let local = Member {
			id: cfg.id,
			addr: advertise,
			state: MemberState::Alive,
			incarnation: epoch_ms,
			meta_version: epoch_ms,
			meta,
		};
		let inner = Arc::new(Inner {
			auth: wire::Auth::new(&cfg.secret),
			table: Mutex::new(Table::new(local, Instant::now())),
			cfg,
			udp,
			waiters: Mutex::new(HashMap::new()),
			seq: AtomicU32::new(rand::random()),
			events: broadcast::channel(EVENT_CAPACITY).0,
			dropped: AtomicU64::new(0),
			fetching: Mutex::new(HashSet::new()),
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
		];
		debug!(id = %inner.cfg.id, bind = %bound, advertise = %advertise, "gossip started");
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

	/// Advertised address of this instance (what peers and seeds lists use).
	pub fn local_addr(&self) -> SocketAddr {
		self.inner.cfg.advertise
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
	/// Our own probe; signalled by `Ack` or `IndirectAck`.
	Probe { tx: oneshot::Sender<()>, deadline: Instant },
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

struct Inner {
	cfg:      GossipConfig,
	auth:     wire::Auth,
	udp:      UdpSocket,
	table:    Mutex<Table>,
	waiters:  Mutex<HashMap<u32, Waiter>>,
	seq:      AtomicU32,
	events:   broadcast::Sender<MemberEvent>,
	dropped:  AtomicU64,
	/// Addresses with an in-flight metadata fetch (deduplicates fetches).
	fetching: Mutex<HashSet<SocketAddr>>,
	/// Set by `leave`; blocks joins and new fetches.
	stopped:  AtomicBool,
	/// Test-only partition: destinations we refuse to send to or connect to.
	#[cfg(test)]
	blocked:  Mutex<HashSet<SocketAddr>>,
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
		if self.is_blocked(to) {
			return;
		}
		let mut buf = wire::encode_message(msg);
		let budget = wire::MAX_PACKET.saturating_sub(buf.len() + 1 + wire::TAG_LEN);
		let (count, updates) = {
			let mut table = self.table.lock();
			let limit = table.log_scale(self.cfg.retransmit_mult);
			table.take_broadcasts(budget, limit)
		};
		buf.push(count);
		buf.extend_from_slice(&updates);
		self.auth.seal(&mut buf);
		if let Err(err) = self.udp.try_send_to(&buf, to) {
			trace!(%to, %err, "gossip send failed");
		}
	}

	fn publish(&self, event: MemberEvent) {
		let member = event.member();
		debug!(id = %member.id, addr = %member.addr, state = ?member.state, ?event, "gossip member event");
		// No receivers is fine.
		let _ = self.events.send(event);
	}

	/// Applies updates and publishes resulting events / metadata fetches.
	fn merge(self: &Arc<Self>, updates: Vec<Update>, origin: Origin) {
		let now = Instant::now();
		let outcomes: Vec<_> = {
			let mut table = self.table.lock();
			updates
				.into_iter()
				.map(|update| table.apply(update, origin, now))
				.collect()
		};
		for outcome in outcomes {
			if let Some(event) = outcome.event {
				self.publish(event);
			}
			if let Some(addr) = outcome.fetch_meta {
				self.fetch_meta(addr);
			}
		}
	}

	/// Pulls metadata that was too large to inline via push-pull with its owner.
	fn fetch_meta(self: &Arc<Self>, addr: SocketAddr) {
		if self.stopped.load(Ordering::SeqCst) || !self.fetching.lock().insert(addr) {
			return;
		}
		let inner = Arc::clone(self);
		tokio::spawn(async move {
			if let Err(err) = inner.push_pull(addr).await {
				debug!(%addr, %err, "gossip metadata fetch failed");
			}
			inner.fetching.lock().remove(&addr);
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
				let relay_seq = self.next_seq();
				let deadline = Instant::now() + self.cfg.probe_interval;
				self
					.waiters
					.lock()
					.insert(relay_seq, Waiter::Relay { requester: from, seq, deadline });
				self.send(Message::Ping { seq: relay_seq, target }, addr);
			},
			Message::Gossip => {},
		}
	}

	fn resolve(&self, seq: u32) {
		let waiter = self.waiters.lock().remove(&seq);
		match waiter {
			Some(Waiter::Probe { tx, .. }) => {
				let _ = tx.send(());
			},
			Some(Waiter::Relay { requester, seq, .. }) => {
				self.send(Message::IndirectAck { seq }, requester);
			},
			None => {},
		}
	}

	/// One SWIM probe round: direct ping, then `k` indirect probes, then
	/// suspicion of exactly the probed incarnation.
	async fn probe(self: &Arc<Self>) {
		let Some((target, addr, incarnation)) = self.table.lock().next_probe_target() else {
			return;
		};
		let seq = self.next_seq();
		let (tx, mut rx) = oneshot::channel();
		let deadline = Instant::now() + self.cfg.probe_interval + self.cfg.ack_timeout * 2;
		self
			.waiters
			.lock()
			.insert(seq, Waiter::Probe { tx, deadline });
		self.send(Message::Ping { seq, target }, addr);
		if matches!(tokio::time::timeout(self.cfg.ack_timeout, &mut rx).await, Ok(Ok(()))) {
			return;
		}
		let helpers = self.table.lock().random_peers(
			&[MemberState::Alive],
			Some(target),
			self.cfg.indirect_probes,
		);
		for (_, helper) in helpers {
			self.send(Message::PingReq { seq, target, addr }, helper);
		}
		let rest = self
			.cfg
			.probe_interval
			.saturating_sub(self.cfg.ack_timeout)
			.max(self.cfg.ack_timeout);
		if matches!(tokio::time::timeout(rest, &mut rx).await, Ok(Ok(()))) {
			return;
		}
		self.waiters.lock().remove(&seq);
		debug!(%target, %addr, "gossip probe failed");
		let outcome = self
			.table
			.lock()
			.suspect(target, incarnation, Instant::now());
		if let Some(event) = outcome.event {
			self.publish(event);
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
		let addrs: Vec<SocketAddr> = tokio::net::lookup_host(seed)
			.await
			.map_err(|err| io::Error::new(err.kind(), format!("gossip seed {seed}: {err}")))?
			.filter(|addr| *addr != self.cfg.advertise && *addr != self.cfg.bind)
			.collect();
		let mut last_err = None;
		for addr in addrs {
			match self.push_pull(addr).await {
				Ok(()) => return Ok(true),
				Err(err) => {
					last_err =
						Some(io::Error::new(err.kind(), format!("gossip seed {seed} ({addr}): {err}")));
				},
			}
		}
		last_err.map_or(Ok(false), Err)
	}

	/// Client side of a full-state exchange with `addr`.
	async fn push_pull(self: &Arc<Self>, addr: SocketAddr) -> io::Result<()> {
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
			Ok(())
		};
		tokio::time::timeout(self.cfg.tcp_timeout, exchange)
			.await
			.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "gossip push-pull timed out"))?
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

	/// Anti-entropy with a random alive member plus a reconnect attempt with a
	/// random dead one (heals partitions once the network does).
	async fn push_pull_loop(self: Arc<Self>) {
		let mut tick = ticker(self.cfg.push_pull_interval);
		loop {
			tick.tick().await;
			let targets = {
				let table = self.table.lock();
				let mut targets = table.random_peers(&[MemberState::Alive], None, 1);
				targets.extend(table.random_peers(&[MemberState::Dead], None, 1));
				targets
			};
			for (id, addr) in targets {
				if let Err(err) = self.push_pull(addr).await {
					debug!(%id, %addr, %err, "gossip push-pull failed");
				}
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

/// Fills a zero advertise port / unspecified advertise IP from the bound
/// address.
fn resolve_advertise(advertise: SocketAddr, bound: SocketAddr) -> io::Result<SocketAddr> {
	let port = if advertise.port() == 0 {
		bound.port()
	} else {
		advertise.port()
	};
	let ip = if !advertise.ip().is_unspecified() {
		advertise.ip()
	} else if !bound.ip().is_unspecified() {
		bound.ip()
	} else {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"gossip advertise address must have a routable IP when binding a wildcard address",
		));
	};
	Ok(SocketAddr::new(ip, port))
}
