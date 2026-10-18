//! Test fixture: a [`Store`] on a private single-voter raft cluster.

use std::time::Duration;

use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeId, Roles};

use super::Store;

/// Cluster secret of [`start_cluster`] clusters.
pub const SECRET: &str = "test-secret";

/// A store on its own one-driver cluster (temp data dir, no listeners
/// needed: a single voter commits locally).
pub struct TestStore {
	pub store:   Store,
	pub cluster: Cluster,
	_dir:        tempfile::TempDir,
}

impl TestStore {
	/// Starts the cluster and waits until it leads with applied state.
	pub async fn start() -> Self {
		let dir = tempfile::tempdir().expect("temp dir");
		let cluster = start_cluster(&dir, Roles { node: false, driver: true }).await;
		Self { store: Store::new(cluster.kv.clone()), cluster, _dir: dir }
	}

	/// Stops the cluster's background tasks.
	pub async fn finish(self) {
		self.cluster.shutdown().await;
	}
}

/// Starts a single-member cluster with `roles` under `dir` and, when it is a
/// driver, waits for its raft leadership.
pub async fn start_cluster(dir: &tempfile::TempDir, roles: Roles) -> Cluster {
	let cluster = Cluster::start(
		ClusterConfig {
			id: NodeId::random(),
			data_dir: dir.path().join("cluster"),
			roles,
			gossip_bind: "127.0.0.1:0".parse().expect("socket addr"),
			advertise: None,
			cluster_port: 1,
			join: Vec::new(),
			secret: SECRET.as_bytes().to_vec(),
			bootstrap_expect: 1,
			raft_reap_after: Duration::from_mins(10),
		},
		MemberMeta { version: "test".into(), ..MemberMeta::default() },
	)
	.await
	.expect("start test cluster");
	if roles.driver {
		let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
		while !cluster.is_leader() {
			assert!(tokio::time::Instant::now() < deadline, "test cluster never led");
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	}
	cluster
}
