//! In-process cluster tests: real gossip, raft over cluster HTTP, on-disk
//! storage.

use std::{
	collections::BTreeSet,
	future::Future,
	path::{Path, PathBuf},
	time::Duration,
};

use openraft::async_runtime::WatchReceiver;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	task::JoinHandle,
};

use super::{Compare, Kv, KvError, Op, Range, Txn, WatchEvent};
use crate::{Cluster, ClusterConfig, MemberMeta, NodeId, Roles};

const DRIVER: Roles = Roles { node: false, driver: true };
const NODE: Roles = Roles { node: true, driver: false };
const WAIT: Duration = Duration::from_secs(30);

struct Proc {
	cluster: Cluster,
	server:  JoinHandle<()>,
	url:     String,
}

impl Proc {
	async fn start(id: u64, dir: &Path, roles: Roles, seeds: &[&Self], expect: usize) -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = format!("http://{}", listener.local_addr().unwrap());
		let cfg = ClusterConfig {
			id: NodeId(id),
			data_dir: dir.join(format!("{id:x}")),
			roles,
			gossip_bind: "127.0.0.1:0".parse().unwrap(),
			gossip_advertise: "127.0.0.1:0".parse().unwrap(),
			cluster_url: url.clone(),
			join: seeds
				.iter()
				.map(|p| p.cluster.gossip.local_addr().to_string())
				.collect(),
			secret: b"test-secret".to_vec(),
			bootstrap_expect: expect,
			raft_reap_after: Duration::from_mins(10),
		};
		let cluster =
			Cluster::start(cfg, MemberMeta { version: "test".into(), ..MemberMeta::default() })
				.await
				.unwrap();
		let router = cluster.router();
		let server = tokio::spawn(async move {
			axum::serve(listener, router).await.unwrap();
		});
		Self { cluster, server, url }
	}

	async fn kill(self) {
		self.cluster.shutdown().await;
		self.server.abort();
		let _ = self.server.await;
	}

	fn id(&self) -> NodeId {
		self.cluster.local_id()
	}

	fn cluster_id(&self) -> Option<String> {
		self
			.cluster
			.kv
			.local()
			.and_then(|local| local.state().cluster_id())
	}

	fn voters(&self) -> BTreeSet<NodeId> {
		let local = self.cluster.kv.local().unwrap();
		let metrics = local.raft().metrics().borrow_watched().clone();
		metrics.membership_config.membership().voter_ids().collect()
	}

	/// Builds a snapshot and purges the log behind it, so followers/restarts
	/// must use the snapshot.
	async fn snapshot_and_purge(&self) {
		let raft = self.cluster.kv.local().unwrap().raft().clone();
		let applied = raft.metrics().borrow_watched().last_applied.unwrap().index;
		raft.trigger().snapshot().await.unwrap();
		eventually("snapshot", || async {
			raft
				.metrics()
				.borrow_watched()
				.snapshot
				.is_some_and(|s| s.index >= applied)
				.then_some(())
		})
		.await;
		raft.trigger().purge_log(applied).await.unwrap();
		eventually("purge", || async {
			raft
				.metrics()
				.borrow_watched()
				.purged
				.is_some_and(|p| p.index >= applied)
				.then_some(())
		})
		.await;
	}

	fn leader(&self) -> Option<NodeId> {
		*self.cluster.leader().borrow()
	}
}

/// Polls `check` until it yields `Some` or [`WAIT`] passes.
async fn eventually<T, F, Fut>(what: &str, mut check: F) -> T
where
	F: FnMut() -> Fut,
	Fut: Future<Output = Option<T>>,
{
	let deadline = tokio::time::Instant::now() + WAIT;
	loop {
		if let Some(value) = check().await {
			return value;
		}
		assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

async fn leader_of(procs: &[&Proc]) -> NodeId {
	eventually("a common leader", || async {
		let leader = procs[0].leader()?;
		let agreed =
			procs.iter().all(|p| p.leader() == Some(leader)) && procs.iter().any(|p| p.id() == leader);
		agreed.then_some(leader)
	})
	.await
}

fn dir() -> (tempfile::TempDir, PathBuf) {
	let tmp = tempfile::tempdir().unwrap();
	let path = tmp.path().to_owned();
	(tmp, path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_single_driver_bootstraps_and_serves_reads_writes_watch_and_ttl() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0xa, &dir, DRIVER, &[], 1).await;
	assert_eq!(leader_of(&[&a]).await, a.id());
	assert!(a.cluster.is_leader());

	let mut watch = a.cluster.kv.watch("w/");
	let rev = a
		.cluster
		.kv
		.put("w/one", b"1".to_vec(), None)
		.await
		.unwrap();
	a.cluster
		.kv
		.put("other", b"x".to_vec(), None)
		.await
		.unwrap();
	let entry = a.cluster.kv.get_consistent("w/one").await.unwrap().unwrap();
	assert_eq!(
		(entry.value.as_slice(), entry.mod_rev, entry.create_rev),
		(b"1".as_slice(), rev, rev)
	);
	assert!(matches!(watch.recv().await, Some(WatchEvent::Put(e)) if e.key == "w/one"));

	// The cluster id is recorded in the state machine and published through
	// gossip.
	let cluster_id = eventually("cluster id", || async { a.cluster_id() }).await;
	eventually("cluster id in gossip", || async {
		let (_, meta) = a
			.cluster
			.members()
			.into_iter()
			.find(|(m, _)| m.id == a.id())?;
		(meta.raft?.cluster_id? == cluster_id).then_some(())
	})
	.await;

	// TTL: the leader's expiry tick removes the key and notifies watchers.
	a.cluster
		.kv
		.put("w/lease", b"l".to_vec(), Some(Duration::from_millis(300)))
		.await
		.unwrap();
	assert!(a.cluster.kv.get("w/lease").await.unwrap().is_some());
	assert!(matches!(watch.recv().await, Some(WatchEvent::Put(e)) if e.key == "w/lease"));
	let deleted = tokio::time::timeout(Duration::from_secs(5), watch.recv())
		.await
		.unwrap();
	assert!(matches!(deleted, Some(WatchEvent::Delete { key, .. }) if key == "w/lease"));
	assert!(a.cluster.kv.get("w/lease").await.unwrap().is_none());
	assert!(
		a.cluster.kv.get("w/one").await.unwrap().is_some(),
		"keys without TTL survive expiry ticks"
	);

	assert!(a.cluster.kv.delete("w/one").await.unwrap());
	assert!(!a.cluster.kv.delete("w/one").await.unwrap());
	assert_eq!(a.cluster.kv.list("").await.unwrap().len(), 1);
	a.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_driver_restart_recovers_committed_data() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0xb, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	for i in 0..20 {
		a.cluster
			.kv
			.put(&format!("k/{i}"), vec![i], None)
			.await
			.unwrap();
	}
	a.cluster.kv.delete("k/3").await.unwrap();
	a.snapshot_and_purge().await;
	a.cluster.kv.put("k/tail", Vec::new(), None).await.unwrap();
	a.cluster.kv.delete("k/tail").await.unwrap();
	let cluster_id = eventually("cluster id", || async { a.cluster_id() }).await;
	a.kill().await;

	let a = Proc::start(0xb, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	let entries = eventually("replayed state", || async {
		let list = a.cluster.kv.list("k/").await.ok()?;
		(list.len() == 19).then_some(list)
	})
	.await;
	assert!(entries.iter().all(|e| e.key != "k/3"));
	assert_eq!(a.cluster.kv.get("k/7").await.unwrap().unwrap().value, vec![7]);
	assert_eq!(a.cluster_id(), Some(cluster_id), "restart must not re-bootstrap");
	let rev = a.cluster.kv.put("k/new", Vec::new(), None).await.unwrap();
	assert!(rev > a.cluster.kv.get("k/19").await.unwrap().unwrap().mod_rev);
	a.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_node_only_process_goes_through_driver_and_admission_never_exceeds_limit() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x10, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	let n = Proc::start(0x11, &dir, NODE, &[&a], 1).await;
	assert_eq!(n.leader(), None, "node-only processes have no raft leader");
	assert!(n.cluster.kv.watch("").recv().await.is_none(), "watch is drivers-only");

	let kv = n.cluster.kv.clone();
	let rev =
		eventually("node-only write", || async { kv.put("n/x", b"v".to_vec(), None).await.ok() })
			.await;
	assert_eq!(kv.get("n/x").await.unwrap().unwrap().mod_rev, rev);
	assert_eq!(kv.get_consistent("n/x").await.unwrap().unwrap().value, b"v");
	assert_eq!(a.cluster.kv.get("n/x").await.unwrap().unwrap().mod_rev, rev);
	let guarded = Txn {
		compare: vec![Compare::ModRev { key: "n/x".into(), rev: rev + 100 }],
		success: vec![Op::Delete { key: "n/x".into() }],
		failure: vec![Op::Put { key: "n/failed".into(), value: Vec::new(), ttl_ms: None }],
	};
	assert!(!kv.txn(guarded).await.unwrap().succeeded);
	assert!(kv.get("n/failed").await.unwrap().is_some());
	assert_eq!(kv.list("n/").await.unwrap().len(), 2);

	// 30 concurrent admissions from both processes against a limit of 7.
	let mut tasks = Vec::new();
	for i in 0..30 {
		let kv = if i % 2 == 0 {
			n.cluster.kv.clone()
		} else {
			a.cluster.kv.clone()
		};
		tasks.push(tokio::spawn(async move {
			let txn = Txn {
				compare: vec![Compare::CountBelow { prefix: "team/t1/sbx/".into(), limit: 7 }],
				success: vec![Op::Put {
					key:    format!("team/t1/sbx/{i}"),
					value:  Vec::new(),
					ttl_ms: None,
				}],
				failure: Vec::new(),
			};
			kv.txn(txn).await.unwrap().succeeded
		}));
	}
	let mut admitted = 0;
	for task in tasks {
		admitted += usize::from(task.await.unwrap());
	}
	assert_eq!(admitted, 7);
	assert_eq!(a.cluster.kv.list("team/t1/sbx/").await.unwrap().len(), 7);

	// With the only driver gone the node-only client reports the KV as
	// unavailable.
	a.kill().await;
	let err = kv.get("n/x").await.unwrap_err();
	assert!(matches!(err, KvError::Unavailable(_)), "{err:?}");
	n.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_driver_that_sees_existing_cluster_joins_instead_of_bootstrapping() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x20, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	let cluster_id = eventually("cluster id in gossip", || async {
		let (_, meta) = a
			.cluster
			.members()
			.into_iter()
			.find(|(m, _)| m.id == a.id())?;
		meta.raft?.cluster_id
	})
	.await;
	a.cluster
		.kv
		.put("before", b"1".to_vec(), None)
		.await
		.unwrap();
	// The log behind the snapshot is gone, so b can only catch up via snapshot
	// install.
	a.snapshot_and_purge().await;

	// Smaller id and expect=1: it would bootstrap on its own if it ignored the
	// existing cluster.
	let b = Proc::start(0x1, &dir, DRIVER, &[&a], 1).await;
	eventually("b promoted to voter", || async {
		(a.voters() == BTreeSet::from([a.id(), b.id()])).then_some(())
	})
	.await;
	assert_eq!(eventually("b applies cluster id", || async { b.cluster_id() }).await, cluster_id);
	assert_eq!(leader_of(&[&a, &b]).await, a.id());
	assert_eq!(b.cluster.kv.get("before").await.unwrap().unwrap().value, b"1");
	b.kill().await;
	a.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn kv_three_drivers_bootstrap_add_late_voter_and_survive_leader_loss() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x30, &dir, DRIVER, &[], 3).await;
	tokio::time::sleep(Duration::from_secs(2)).await;
	assert!(
		a.cluster_id().is_none() && a.leader().is_none(),
		"expect=3 must wait for three drivers"
	);
	let b = Proc::start(0x31, &dir, DRIVER, &[&a], 3).await;
	let c = Proc::start(0x32, &dir, DRIVER, &[&a], 3).await;
	let procs = [&a, &b, &c];
	leader_of(&procs).await;
	let cluster_id = eventually("one cluster id", || async {
		let id = a.cluster_id()?;
		procs
			.iter()
			.all(|p| p.cluster_id().as_deref() == Some(id.as_str()))
			.then_some(id)
	})
	.await;
	let three = BTreeSet::from([a.id(), b.id(), c.id()]);
	assert!(procs.iter().all(|p| p.voters() == three));

	// Writes from every driver (followers forward to the leader).
	for (i, p) in procs.iter().enumerate() {
		for j in 0..10 {
			p.cluster
				.kv
				.put(&format!("data/{i}/{j}"), vec![1], None)
				.await
				.unwrap();
		}
	}

	// A late fourth driver becomes a voter of the same cluster.
	let d = Proc::start(0x33, &dir, DRIVER, &[&b], 3).await;
	let four: BTreeSet<NodeId> = three.iter().copied().chain([d.id()]).collect();
	eventually("d promoted", || async { (a.voters() == four && d.voters() == four).then_some(()) })
		.await;
	assert_eq!(d.cluster_id(), Some(cluster_id.clone()));

	// Kill the leader: the rest elect a new one and keep every committed write.
	let leader = leader_of(&[&a, &b, &c, &d]).await;
	let mut all = vec![a, b, c, d];
	let index = all.iter().position(|p| p.id() == leader).unwrap();
	all.remove(index).kill().await;
	let rest: Vec<&Proc> = all.iter().collect();
	let new_leader = leader_of(&rest).await;
	assert_ne!(new_leader, leader);
	for p in &rest {
		p.cluster
			.kv
			.put(&format!("after/{}", p.id()), vec![2], None)
			.await
			.unwrap();
	}
	for p in &rest {
		let data = p.cluster.kv.get_consistent("data/2/9").await.unwrap();
		assert!(data.is_some(), "committed write lost on {}", p.id());
	}
	let survivor = rest[0];
	eventually("all writes visible", || async {
		let data = survivor.cluster.kv.list("data/").await.ok()?;
		let after = survivor.cluster.kv.list("after/").await.ok()?;
		(data.len() == 30 && after.len() == 3).then_some(())
	})
	.await;
	assert_eq!(survivor.cluster_id(), Some(cluster_id));
	for p in all {
		p.kill().await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn kv_sync_makes_follower_reads_see_every_acknowledged_write() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x40, &dir, DRIVER, &[], 3).await;
	let b = Proc::start(0x41, &dir, DRIVER, &[&a], 3).await;
	let c = Proc::start(0x42, &dir, DRIVER, &[&a], 3).await;
	let procs = [&a, &b, &c];
	let leader = leader_of(&procs).await;
	eventually("every driver applied state", || async {
		procs.iter().all(|p| p.cluster_id().is_some()).then_some(())
	})
	.await;
	let followers: Vec<&Proc> = procs.iter().copied().filter(|p| p.id() != leader).collect();

	// Write through one driver, read right away on a follower after `sync`.
	for i in 0..60 {
		let writer = procs[i % 3];
		let reader = followers[i % 2];
		let key = format!("seen/{i:03}");
		writer.cluster.kv.put(&key, vec![1], None).await.unwrap();
		reader.cluster.kv.sync().await.unwrap();
		assert!(reader.cluster.kv.get(&key).await.unwrap().is_some(), "{key} not visible after sync");
	}

	// Range pages over HTTP from a handle outside the cluster.
	let outside = Kv::connect(vec![followers[0].url.clone()], b"test-secret").unwrap();
	outside.put("seen/zzz", vec![2], None).await.unwrap();
	let page = Range {
		prefix:  "seen/".into(),
		after:   Some("seen/057".into()),
		limit:   Some(2),
		reverse: false,
	};
	let keys: Vec<String> = outside
		.range(&page)
		.await
		.unwrap()
		.into_iter()
		.map(|e| e.key)
		.collect();
	assert_eq!(keys, ["seen/058", "seen/059"]);
	let last = Range { limit: Some(1), reverse: true, after: None, ..page };
	assert_eq!(outside.range(&last).await.unwrap()[0].key, "seen/zzz");
	let wrong = Kv::connect(vec![followers[0].url.clone()], b"wrong").unwrap();
	assert!(matches!(wrong.get("seen/000").await, Err(KvError::Rejected(_))));

	for p in [a, b, c] {
		p.kill().await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn kv_restarted_former_leader_never_claims_leadership_while_another_leads() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x40, &dir, DRIVER, &[], 3).await;
	let b = Proc::start(0x41, &dir, DRIVER, &[&a], 3).await;
	let c = Proc::start(0x42, &dir, DRIVER, &[&a], 3).await;
	let old = leader_of(&[&a, &b, &c]).await;
	let mut all = vec![a, b, c];
	all[0]
		.cluster
		.kv
		.put("before", b"1".to_vec(), None)
		.await
		.unwrap();

	let index = all.iter().position(|p| p.id() == old).unwrap();
	all.remove(index).kill().await;
	let new_leader = leader_of(&[&all[0], &all[1]]).await;
	assert_ne!(new_leader, old);
	all[0]
		.cluster
		.kv
		.put("during", b"2".to_vec(), None)
		.await
		.unwrap();

	// Restart the former leader with its persisted (committed) vote.
	let restarted = Proc::start(old.0, &dir, DRIVER, &[&all[0]], 3).await;
	let mut leader_rx = restarted.cluster.leader();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
	while tokio::time::Instant::now() < deadline {
		assert!(!restarted.cluster.is_leader(), "deposed leader reported is_leader() after restart");
		assert_ne!(
			*leader_rx.borrow_and_update(),
			Some(old),
			"deposed leader in its own leader() watch"
		);
		let (_, meta) = restarted
			.cluster
			.members()
			.into_iter()
			.find(|(m, _)| m.id == old)
			.unwrap();
		assert_ne!(
			meta.raft.and_then(|r| r.leader),
			Some(old),
			"deposed leader advertised itself in gossip"
		);
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
	all.push(restarted);
	let rest: Vec<&Proc> = all.iter().collect();
	assert_eq!(leader_of(&rest).await, new_leader);
	let restarted = &all[2];
	eventually("restarted node catches up", || async {
		restarted
			.cluster
			.kv
			.get("during")
			.await
			.ok()
			.flatten()
			.map(|_| ())
	})
	.await;
	for p in all {
		p.kill().await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_concurrent_txns_each_get_their_own_result() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x60, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	a.cluster
		.kv
		.put("taken", b"x".to_vec(), None)
		.await
		.unwrap();

	// Even callers claim fresh keys; odd ones a taken key, so their compare
	// fails. Batched log appends mix both; every caller gets its own outcome.
	let calls: Vec<_> = (0..200_u32)
		.map(|i| {
			let kv = a.cluster.kv.clone();
			tokio::spawn(async move {
				let key = if i % 2 == 0 {
					format!("q/{i}")
				} else {
					"taken".to_owned()
				};
				let txn = Txn {
					compare: vec![Compare::Missing(key.clone())],
					success: vec![Op::Put {
						key:    key.clone(),
						value:  i.to_be_bytes().to_vec(),
						ttl_ms: None,
					}],
					failure: Vec::new(),
				};
				(i, key, kv.txn(txn).await.unwrap())
			})
		})
		.collect();
	let mut revisions = BTreeSet::new();
	for call in calls {
		let (i, key, result) = call.await.unwrap();
		assert_eq!(result.succeeded, i % 2 == 0, "{key}");
		if result.succeeded {
			let entry = a.cluster.kv.get(&key).await.unwrap().unwrap();
			assert_eq!((entry.mod_rev, entry.value), (result.revision, i.to_be_bytes().to_vec()));
			assert!(revisions.insert(result.revision), "revision {} reported twice", result.revision);
		}
	}
	a.kill().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_resp_requires_auth_and_maps_set_get_del_exists_onto_the_kv() {
	let (_tmp, dir) = dir();
	let a = Proc::start(0x50, &dir, DRIVER, &[], 1).await;
	leader_of(&[&a]).await;
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	let cluster = a.cluster.clone();
	let server =
		tokio::spawn(async move { cluster.serve_resp(listener, std::future::pending()).await });

	// One pipelined write; QUIT closes the connection after the last reply.
	let requests: [(&[u8], &str); 17] = [
		(b"GET k\r\n", "-NOAUTH Authentication required.\r\n"),
		(b"AUTH wrong\r\n", "-WRONGPASS invalid username-password pair or user is disabled.\r\n"),
		(b"AUTH default test-secret\r\n", "+OK\r\n"),
		(b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nv 1\r\n\r\n$2\r\nNX\r\n", "+OK\r\n"),
		(b"SET k w NX\r\n", "$-1\r\n"),
		(b"SET missing x XX\r\n", "$-1\r\n"),
		(b"GET k\r\n", "$5\r\nv 1\r\n\r\n"),
		(b"SET k w XX\r\n", "+OK\r\n"),
		(b"GET k\r\n", "$1\r\nw\r\n"),
		// A malformed key fails the whole DEL before any key is deleted.
		(b"*3\r\n$3\r\nDEL\r\n$1\r\nk\r\n$1\r\n\xff\r\n", "-ERR key is not valid UTF-8\r\n"),
		(b"GET k\r\n", "$1\r\nw\r\n"),
		(b"SET lease l PX 60000\r\n", "+OK\r\n"),
		(b"EXISTS k missing k\r\n", ":2\r\n"),
		(b"DEL k missing\r\n", ":1\r\n"),
		(b"GET k\r\n", "$-1\r\n"),
		(b"SET k\r\n", "-ERR wrong number of arguments for 'set' command\r\n"),
		(b"QUIT\r\n", "+OK\r\n"),
	];
	let mut stream = TcpStream::connect(addr).await.unwrap();
	let pipeline: Vec<u8> = requests
		.iter()
		.flat_map(|(request, _)| *request)
		.copied()
		.collect();
	stream.write_all(&pipeline).await.unwrap();
	let mut replies = String::new();
	stream.read_to_string(&mut replies).await.unwrap();
	let expected: String = requests.iter().map(|(_, reply)| *reply).collect();
	assert_eq!(replies, expected);

	let lease = a.cluster.kv.get_consistent("lease").await.unwrap().unwrap();
	assert!(lease.expires_at_ms.is_some(), "PX sets a TTL");
	server.abort();
	a.kill().await;
}
