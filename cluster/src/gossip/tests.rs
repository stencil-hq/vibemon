//! In-process gossip clusters on loopback (ephemeral ports, fast timings).

use std::{net::Ipv4Addr, ops::Range, time::Duration};

use tokio::{io::AsyncWriteExt, net::UdpSocket, sync::broadcast::error::RecvError};

use super::*;

const SECRET: &[u8] = b"test-cluster-secret";
const PROBE: Duration = Duration::from_millis(100);
/// `suspicion_mult (3) × probe_interval` for clusters under 10 members.
const SUSPICION: Duration = Duration::from_millis(300);

fn fast_config(id: u64, secret: &[u8]) -> GossipConfig {
	let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
	GossipConfig {
		probe_interval: PROBE,
		ack_timeout: Duration::from_millis(40),
		suspicion_mult: 3,
		gossip_interval: Duration::from_millis(30),
		tcp_timeout: Duration::from_secs(2),
		..GossipConfig::new(NodeId(id), any, Vec::new(), secret.to_vec())
	}
}

fn meta_of(id: u64) -> Vec<u8> {
	format!("meta-{id}").into_bytes()
}

fn id_of(g: &Gossip) -> NodeId {
	g.inner.cfg.id
}

fn state_of(g: &Gossip, id: NodeId) -> Option<MemberState> {
	g.member(id).map(|m| m.state)
}

fn all_alive(nodes: &[Gossip]) -> bool {
	nodes.iter().all(|g| {
		nodes
			.iter()
			.all(|other| state_of(g, id_of(other)) == Some(MemberState::Alive))
	})
}

async fn eventually(within: Duration, mut check: impl FnMut() -> bool) -> bool {
	let deadline = Instant::now() + within;
	loop {
		if check() {
			return true;
		}
		if Instant::now() >= deadline {
			return false;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

/// First event matching `pred`, or `None` after `within`.
async fn wait_event(
	rx: &mut broadcast::Receiver<MemberEvent>,
	within: Duration,
	pred: impl Fn(&MemberEvent) -> bool,
) -> Option<MemberEvent> {
	let deadline = tokio::time::Instant::now() + within;
	loop {
		match tokio::time::timeout_at(deadline, rx.recv()).await {
			Ok(Ok(event)) if pred(&event) => return Some(event),
			Ok(Ok(_) | Err(RecvError::Lagged(_))) => {},
			Ok(Err(RecvError::Closed)) | Err(_) => return None,
		}
	}
}

/// Starts members `ids`, joins all of them through the first, and waits until
/// everyone sees everyone `Alive`.
async fn cluster_with(ids: Range<u64>, tweak: impl Fn(&mut GossipConfig)) -> Vec<Gossip> {
	let mut nodes = Vec::new();
	for id in ids {
		let mut cfg = fast_config(id, SECRET);
		tweak(&mut cfg);
		nodes.push(Gossip::start(cfg, meta_of(id)).await.unwrap());
	}
	let seed = nodes[0].local_addr().to_string();
	for node in &nodes[1..] {
		assert_eq!(node.join(std::slice::from_ref(&seed)).await.unwrap(), 1);
	}
	assert!(
		eventually(Duration::from_secs(5), || all_alive(&nodes)).await,
		"cluster did not converge"
	);
	nodes
}

async fn cluster(ids: Range<u64>) -> Vec<Gossip> {
	cluster_with(ids, |_| {}).await
}

/// Whether the host has IPv6 loopback, which the multi-address tests need: a
/// member bound to `[::]` reached at both `[::1]` and `127.0.0.1`.
fn dual_stack() -> bool {
	let ok = std::net::UdpSocket::bind("[::1]:0").is_ok();
	if !ok {
		eprintln!("skipped: no IPv6 loopback");
	}
	ok
}

/// Binds `[::]` and advertises `[::1]` (preferred) and `127.0.0.1`: two
/// addresses of one socket, so blocking one leaves the other working.
fn dual(cfg: &mut GossipConfig) {
	cfg.bind = "[::]:0".parse().unwrap();
	cfg.advertise = vec!["[::1]:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap()];
}

fn dual_config(id: u64) -> GossipConfig {
	let mut cfg = fast_config(id, SECRET);
	dual(&mut cfg);
	cfg
}

/// The `127.0.0.1` address of a member started with [`dual`].
fn v4_of(g: &Gossip) -> SocketAddr {
	SocketAddr::from((Ipv4Addr::LOCALHOST, g.local_addr().port()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_members_converge_through_one_seed() {
	let nodes = cluster(1..6).await;
	for g in &nodes {
		assert_eq!(g.members().len(), 5);
		for other in &nodes {
			let m = g.member(id_of(other)).unwrap();
			assert_eq!(m.addr, other.local_addr());
			assert_eq!(m.meta, meta_of(id_of(other).0));
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_edge_cases() {
	let g = Gossip::start(fast_config(7, SECRET), Vec::new())
		.await
		.unwrap();
	assert_eq!(g.join(&[]).await.unwrap(), 0);
	// Seeds that resolve only to ourselves are skipped, not errors.
	assert_eq!(g.join(&[g.local_addr().to_string()]).await.unwrap(), 0);
	let closed = std::net::TcpListener::bind("127.0.0.1:0")
		.unwrap()
		.local_addr()
		.unwrap();
	assert!(g.join(&[closed.to_string()]).await.is_err());
	assert!(
		g.join(&["no-such-host.invalid:7946".to_owned()])
			.await
			.is_err()
	);
	// One reachable seed is enough even if another fails.
	let peer = Gossip::start(fast_config(8, SECRET), Vec::new())
		.await
		.unwrap();
	let seeds = [closed.to_string(), peer.local_addr().to_string()];
	assert_eq!(g.join(&seeds).await.unwrap(), 1);
	assert_eq!(state_of(&g, NodeId(8)), Some(MemberState::Alive));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_member_is_declared_dead_within_bound() {
	let mut nodes = cluster(10..14).await;
	let mut rx = nodes[0].subscribe();
	let victim = nodes.pop().unwrap();
	let victim_id = id_of(&victim);
	drop(victim);
	// Bound: probed within 3 rounds + one failed probe round + suspicion +
	// slack.
	let bound = PROBE * 4 + SUSPICION + Duration::from_millis(800);
	let event = wait_event(&mut rx, bound, |e| {
		e.member().id == victim_id && matches!(e, MemberEvent::Failed(_) | MemberEvent::Left(_))
	})
	.await;
	assert!(matches!(event, Some(MemberEvent::Failed(_))), "got {event:?}");
	assert!(
		eventually(Duration::from_secs(1), || nodes
			.iter()
			.all(|g| state_of(g, victim_id) == Some(MemberState::Dead)))
		.await
	);
	assert!(all_alive(&nodes));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn falsely_suspected_member_refutes_and_stays_alive() {
	let nodes = cluster(20..23).await;
	let target = nodes[2].local_addr();
	let id = id_of(&nodes[2]);
	let incarnation = nodes[0].member(id).unwrap().incarnation;
	let mut rx = nodes[0].subscribe();
	let accusation = Update {
		id,
		addrs: vec![target],
		state: MemberState::Suspect,
		incarnation,
		meta_version: 0,
		meta: None,
	};
	nodes[0].inner.merge(vec![accusation], Origin::Gossip);
	assert_eq!(state_of(&nodes[0], id), Some(MemberState::Suspect));
	assert!(
		eventually(Duration::from_secs(2), || nodes.iter().all(|g| {
			g.member(id)
				.is_some_and(|m| m.state == MemberState::Alive && m.incarnation > incarnation)
		}))
		.await,
		"refutation did not spread"
	);
	tokio::time::sleep(SUSPICION * 3).await;
	assert!(all_alive(&nodes));
	let failed = wait_event(&mut rx, Duration::from_millis(1), |e| {
		e.member().id == id && matches!(e, MemberEvent::Failed(_))
	})
	.await;
	assert_eq!(failed, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn meta_updates_propagate_inline_and_fetched() {
	let nodes = cluster(30..34).await;
	let mut rx = nodes[3].subscribe();
	let before = nodes[3].member(id_of(&nodes[1])).unwrap().meta_version;
	let big = vec![b'x'; 3000];
	nodes[1].set_meta(b"small".to_vec());
	nodes[2].set_meta(big.clone());
	let (small_id, big_id) = (id_of(&nodes[1]), id_of(&nodes[2]));
	assert!(
		eventually(Duration::from_secs(3), || nodes.iter().all(|g| {
			g.member(small_id).unwrap().meta == b"small" && g.member(big_id).unwrap().meta == big
		}))
		.await,
		"metadata did not propagate"
	);
	assert!(nodes[3].member(small_id).unwrap().meta_version > before);
	let updated = wait_event(
		&mut rx,
		Duration::from_secs(1),
		|e| matches!(e, MemberEvent::Updated(m) if m.id == small_id && m.meta == b"small"),
	)
	.await;
	assert!(updated.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn packets_with_foreign_secret_are_ignored() {
	let nodes = cluster(40..42).await;
	let rogue = Gossip::start(fast_config(49, b"another-secret"), Vec::new())
		.await
		.unwrap();
	assert!(
		rogue
			.join(&[nodes[0].local_addr().to_string()])
			.await
			.is_err()
	);
	assert!(nodes[0].dropped_packets() >= 1, "rejected push-pull frame not counted");
	// Make the rogue probe and gossip to node 0 over UDP.
	let before = nodes[0].dropped_packets();
	let known = Update {
		id:           id_of(&nodes[0]),
		addrs:        vec![nodes[0].local_addr()],
		state:        MemberState::Alive,
		incarnation:  1,
		meta_version: 0,
		meta:         None,
	};
	rogue.inner.merge(vec![known], Origin::Gossip);
	assert!(eventually(Duration::from_secs(2), || nodes[0].dropped_packets() >= before + 3).await);
	tokio::time::sleep(SUSPICION).await;
	for g in &nodes {
		assert_eq!(g.members().len(), 2);
		assert!(g.member(NodeId(49)).is_none());
	}
	assert!(all_alive(&nodes));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_leave_yields_left_not_failed() {
	let nodes = cluster(50..53).await;
	let mut receivers = [nodes[0].subscribe(), nodes[1].subscribe()];
	let leaver = &nodes[2];
	let id = id_of(leaver);
	let started = Instant::now();
	leaver.leave(Duration::from_secs(3)).await;
	assert!(started.elapsed() < Duration::from_secs(1), "leave waited for its timeout");
	for rx in &mut receivers {
		let event = wait_event(rx, Duration::from_secs(1), |e| {
			e.member().id == id && matches!(e, MemberEvent::Failed(_) | MemberEvent::Left(_))
		})
		.await;
		assert!(matches!(event, Some(MemberEvent::Left(_))), "got {event:?}");
	}
	tokio::time::sleep(SUSPICION * 3).await;
	for g in &nodes[..2] {
		assert_eq!(state_of(g, id), Some(MemberState::Left));
	}
	for rx in &mut receivers {
		let failed =
			wait_event(rx, Duration::from_millis(1), |e| matches!(e, MemberEvent::Failed(_))).await;
		assert_eq!(failed, None);
	}
	assert!(
		leaver
			.join(&[nodes[0].local_addr().to_string()])
			.await
			.is_err()
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_pull_merges_separately_joined_groups() {
	let a = cluster(60..62).await;
	let b = cluster(70..72).await;
	assert_eq!(a[0].members().len(), 2);
	assert_eq!(a[1].join(&[b[0].local_addr().to_string()]).await.unwrap(), 1);
	let all: Vec<Gossip> = a.iter().chain(&b).cloned().collect();
	assert!(eventually(Duration::from_secs(3), || all_alive(&all)).await, "groups did not merge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_heals_after_members_were_declared_dead() {
	let nodes =
		cluster_with(80..84, |cfg| cfg.push_pull_interval = Duration::from_millis(300)).await;
	let (left, right) = nodes.split_at(2);
	let block = |from: &[Gossip], to: &[Gossip]| {
		for g in from {
			g.inner
				.blocked
				.lock()
				.extend(to.iter().map(Gossip::local_addr));
		}
	};
	block(left, right);
	block(right, left);
	let split = |a: &[Gossip], b: &[Gossip]| {
		a.iter().all(|g| {
			b.iter()
				.all(|o| state_of(g, id_of(o)) == Some(MemberState::Dead))
		})
	};
	assert!(
		eventually(Duration::from_secs(5), || split(left, right) && split(right, left)).await,
		"partition not detected"
	);
	for g in &nodes {
		g.inner.blocked.lock().clear();
	}
	assert!(
		eventually(Duration::from_secs(10), || all_alive(&nodes)).await,
		"partition did not heal"
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_member_replaces_old_address() {
	let mut nodes = cluster(100..103).await;
	let old = nodes.pop().unwrap();
	let old_addr = old.local_addr();
	drop(old);
	let mut cfg = fast_config(102, SECRET);
	cfg.bind = "0.0.0.0:0".parse().unwrap();
	cfg.advertise = vec!["127.0.0.1:0".parse().unwrap()];
	let reborn = Gossip::start(cfg, b"second-life".to_vec()).await.unwrap();
	assert_eq!(reborn.local_addr().ip(), std::net::Ipv4Addr::LOCALHOST);
	assert_ne!(reborn.local_addr(), old_addr);
	assert_eq!(
		reborn
			.join(&[nodes[0].local_addr().to_string()])
			.await
			.unwrap(),
		1
	);
	nodes.push(reborn.clone());
	assert!(
		eventually(Duration::from_secs(3), || nodes.iter().all(|g| {
			g.member(NodeId(102)).is_some_and(|m| {
				m.state == MemberState::Alive
					&& m.addr == reborn.local_addr()
					&& m.meta == b"second-life"
			})
		}))
		.await,
		"restarted member not adopted"
	);
	assert!(all_alive(&nodes));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_route_fails_over_and_moves_back_once_unblocked() {
	if !dual_stack() {
		return;
	}
	let nodes = cluster_with(110..113, dual).await;
	let (prober, target) = (&nodes[0], &nodes[2]);
	let id = id_of(target);
	let (v6, v4) = (target.local_addr(), v4_of(target));
	for g in &nodes[..2] {
		let m = g.member(id).unwrap();
		assert_eq!((m.addr, m.addrs), (v6, vec![v6, v4]));
	}
	let mut rx = prober.subscribe();
	prober.inner.blocked.lock().insert(v6);
	let switched = wait_event(
		&mut rx,
		Duration::from_secs(2),
		|e| matches!(e, MemberEvent::Updated(m) if m.id == id && m.addr == v4),
	)
	.await;
	assert!(switched.is_some(), "route did not fail over");
	tokio::time::sleep(SUSPICION * 3).await;
	let doubted = wait_event(&mut rx, Duration::from_millis(1), |e| {
		e.member().id == id && e.member().state != MemberState::Alive
	})
	.await;
	assert_eq!(doubted, None, "a member reachable at another address was suspected");
	let m = prober.member(id).unwrap();
	assert_eq!((m.state, m.addr), (MemberState::Alive, v4));
	prober.inner.blocked.lock().clear();
	assert!(
		eventually(Duration::from_secs(2), || prober.member(id).is_some_and(|m| m.addr == v6)).await,
		"route did not move back to the better-ranked address"
	);
	assert!(all_alive(&nodes));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joining_member_route_is_verified_before_any_probe() {
	if !dual_stack() {
		return;
	}
	let mut cfg = dual_config(120);
	// No probe rounds: only the join-time verification can move the route.
	cfg.probe_interval = Duration::from_secs(3600);
	let observer = Gossip::start(cfg, meta_of(120)).await.unwrap();
	let joiner = Gossip::start(dual_config(121), meta_of(121)).await.unwrap();
	let (v6, v4) = (joiner.local_addr(), v4_of(&joiner));
	observer.inner.blocked.lock().insert(v6);
	let mut rx = observer.subscribe();
	assert_eq!(
		joiner
			.join(&[observer.local_addr().to_string()])
			.await
			.unwrap(),
		1
	);
	let joined = wait_event(&mut rx, Duration::from_secs(1), |e| e.member().id == NodeId(121)).await;
	assert!(
		matches!(&joined, Some(MemberEvent::Joined(m)) if m.addr == v6),
		"a new member starts at its best-ranked address: {joined:?}"
	);
	let routed = wait_event(
		&mut rx,
		Duration::from_secs(1),
		|e| matches!(e, MemberEvent::Updated(m) if m.id == NodeId(121) && m.addr == v4),
	)
	.await;
	assert!(routed.is_some(), "the join did not verify the route");
	assert_eq!(state_of(&observer, NodeId(121)), Some(MemberState::Alive));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_addrs_propagates_and_routes_follow_the_advertised_list() {
	if !dual_stack() {
		return;
	}
	let nodes = cluster_with(130..133, |cfg| {
		cfg.bind = "[::]:0".parse().unwrap();
		cfg.advertise = vec!["[::1]:0".parse().unwrap()];
	})
	.await;
	let mover = &nodes[2];
	let id = id_of(mover);
	let (v6, v4) = (mover.local_addr(), v4_of(mover));
	let before = nodes[0].member(id).unwrap().incarnation;
	let mut rx = nodes[0].subscribe();
	mover
		.set_addrs(vec!["[::1]:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap()])
		.unwrap();
	let peers = &nodes[..2];
	assert!(
		eventually(Duration::from_secs(2), || peers.iter().all(|g| {
			g.member(id)
				.is_some_and(|m| m.addrs == [v6, v4] && m.addr == v6 && m.incarnation > before)
		}))
		.await,
		"new addresses did not propagate"
	);
	let updated = wait_event(
		&mut rx,
		Duration::from_secs(1),
		|e| matches!(e, MemberEvent::Updated(m) if m.id == id),
	)
	.await;
	assert!(updated.is_some());
	// Dropping the route moves peers to the address still advertised.
	mover.set_addrs(vec![v4]).unwrap();
	assert_eq!(mover.local_addr(), v4);
	assert!(
		eventually(Duration::from_secs(2), || peers.iter().all(|g| g
			.member(id)
			.is_some_and(|m| m.addrs == [v4] && m.addr == v4)))
		.await,
		"route did not follow the advertised addresses"
	);
	let incarnation = mover.member(id).unwrap().incarnation;
	mover
		.set_addrs(vec!["127.0.0.1:0".parse().unwrap()])
		.unwrap();
	assert_eq!(mover.member(id).unwrap().incarnation, incarnation, "unchanged list is a no-op");
	assert!(mover.set_addrs(Vec::new()).is_err());
	assert!(mover.set_addrs(vec!["0.0.0.0:0".parse().unwrap()]).is_err());
	let too_many = (1..=17)
		.map(|i| SocketAddr::from(([10, 0, 0, i], 1)))
		.collect();
	assert!(mover.set_addrs(too_many).is_err());
	tokio::time::sleep(SUSPICION).await;
	assert!(all_alive(&nodes));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_req_helper_also_tries_its_own_route() {
	if !dual_stack() {
		return;
	}
	let nodes = cluster_with(140..143, dual).await;
	let (prober, helper, target) = (&nodes[0], &nodes[1], &nodes[2]);
	let id = id_of(target);
	let (v6, v4) = (target.local_addr(), v4_of(target));
	// The helper reaches the target only at its second address…
	helper.inner.blocked.lock().insert(v6);
	assert!(
		eventually(Duration::from_secs(2), || helper.member(id).is_some_and(|m| m.addr == v4)).await
	);
	// …and the prober not at all: its probes ask the helper to ping [::1],
	// which only works because the helper also pings its own route.
	let before = prober.member(id).unwrap().incarnation;
	let mut rx = prober.subscribe();
	prober.inner.blocked.lock().extend([v6, v4]);
	tokio::time::sleep(SUSPICION * 3).await;
	let doubted = wait_event(&mut rx, Duration::from_millis(1), |e| {
		e.member().id == id && e.member().state != MemberState::Alive
	})
	.await;
	assert_eq!(doubted, None, "indirect probes failed");
	let m = prober.member(id).unwrap();
	assert_eq!((m.state, m.incarnation, m.addr), (MemberState::Alive, before, v6));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_traffic_is_dropped_without_disrupting_the_member() {
	let nodes = cluster(90..92).await;
	let target = nodes[0].local_addr();
	let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
	let auth = wire::Auth::new(SECRET);
	for round in 0..600usize {
		let len = rand::random_range(0..1500usize);
		let mut noise: Vec<u8> = (0..len).map(|_| rand::random()).collect();
		if round % 2 == 0 {
			// Authenticated garbage exercises the decoder, not just the MAC check.
			if round % 4 == 0 {
				noise.splice(0..0, *b"VG2");
			}
			auth.seal(&mut noise);
		}
		sock.send_to(&noise, target).await.unwrap();
		if round % 50 == 0 {
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	}
	for frame in [vec![0xff; 4], vec![0, 0, 0, 40, 1, 2, 3], {
		let mut body = b"VG2\x10garbage".to_vec();
		auth.seal(&mut body);
		let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
		frame.extend(body);
		frame
	}] {
		let mut stream = TcpStream::connect(target).await.unwrap();
		stream.write_all(&frame).await.unwrap();
		drop(stream);
	}
	assert!(eventually(Duration::from_secs(2), || nodes[0].dropped_packets() >= 100).await);
	assert!(nodes[0].tasks.0.lock().iter().all(|h| !h.is_finished()), "a gossip task died");
	nodes[0].set_meta(b"still-here".to_vec());
	assert!(
		eventually(Duration::from_secs(2), || {
			nodes[1]
				.member(id_of(&nodes[0]))
				.is_some_and(|m| m.meta == b"still-here")
		})
		.await
	);
	tokio::time::sleep(SUSPICION).await;
	assert!(all_alive(&nodes));
}
