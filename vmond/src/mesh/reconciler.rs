//! Background orphan reconciliation and replication loops.
//!
//! Port of `python/vmon/server/reconciler.py`.  This module intentionally keeps
//! the neighboring state/lease/record/replica implementations behind narrow
//! traits so the route/state agents can wire their concrete stores without this
//! file editing them.  The ordering, quorum checks, HA tiers, worker keys, and
//! peer wire payloads mirror the Python implementation.

use std::{
	collections::{HashMap, HashSet},
	path::{Path, PathBuf},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use parking_lot::Mutex;
use reqwest::Client;
use serde_json::{Map, Value, json};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{error, info, warn};

use crate::{EngineError, Result, config::ServeConfig, mesh::lease::LeaseRecord};

#[derive(Clone, Debug)]
pub struct ReconcileConfig {
	pub replicate_sec:         f64,
	pub replicas:              usize,
	pub replicate_concurrency: usize,
	pub restore_quorum:        Option<bool>,
	pub outbound_token:        String,
}

impl ReconcileConfig {
	pub fn from_serve_config(
		config: &ServeConfig,
		mesh_enabled: bool,
		outbound_token: String,
	) -> Self {
		Self {
			replicate_sec: config.effective_replicate_sec(mesh_enabled),
			replicas: config.replicas,
			replicate_concurrency: config.replicate_concurrency,
			restore_quorum: config.restore_quorum,
			outbound_token,
		}
	}

	pub fn restore_quorum_enabled(&self, expected_members: usize) -> bool {
		self.restore_quorum.unwrap_or(expected_members >= 3)
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRecord {
	pub sid:             String,
	pub params:          Value,
	pub owner:           String,
	pub epoch:           u64,
	pub idempotency_key: String,
	pub ha:              String,
	pub restart_policy:  String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRecord {
	pub sid:                   String,
	pub digest:                String,
	pub object_key:            String,
	pub source_node:           String,
	pub source_epoch:          u64,
	pub checkpoint_generation: u64,
	pub snapshot_dir:          PathBuf,
	pub params:                Value,
	pub needs_secrets:         bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicatePreparation {
	pub digest:                String,
	pub checkpoint_generation: u64,
	pub snapshot_dir:          PathBuf,
	pub params:                Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxRecord {
	pub name:   String,
	pub detail: Map<String, Value>,
}

/// The exact locally adopted ownership generation of a running VM. Lease
/// renewal is forbidden for a generation that this node has not adopted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOwnerEpoch {
	pub sid:   String,
	pub owner: String,
	pub epoch: u64,
}

pub trait MeshReconcileState: Send + Sync {
	fn enabled(&self) -> bool;
	fn interval(&self) -> f64;
	fn node_id(&self) -> &str;
	fn advertise(&self) -> String;
	fn create_timeout(&self) -> Duration;
	fn expected_members(&self) -> usize;
	fn live_member_ids(&self) -> Vec<String>;
	fn peer_url(&self, node_id: &str) -> Option<String>;
	fn is_peer_healthy(&self, node_id: &str) -> bool;
	fn owner_of(&self, sid: &str) -> Result<Option<String>>;
	fn restore_owner(&self, sid: &str, exclude: &HashSet<String>) -> Option<String>;
	fn restore_quorum_met(&self, confirmations: usize) -> bool;
	fn quorum_needed(&self) -> usize;
	fn drain_orphans(&self) -> Vec<(String, String)>;
	fn requeue_orphan(&self, sid: &str, dead: &str);
	/// Durable suspension is non-serving by contract and must not be repaired
	/// by the generic HA/orphan loop.
	fn is_nonserving_lifecycle(&self, _sid: &str) -> Result<bool> {
		Ok(false)
	}
	/// A durable local owner survived restart but has no local VM candidate.
	/// This is proof of local loss, so recovery must not wait on a health
	/// probe of this same process.
	fn local_candidate_missing(&self, _sid: &str) -> Result<bool> {
		Ok(false)
	}
	/// Claim the next epoch before a restore starts. This must atomically make
	/// `owner` authoritative for the returned generation.
	fn claim_restore_epoch(&self, sid: &str, owner: &str) -> Result<u64>;
	/// Claim only the ownership lineage examined for this restore. The default
	/// preserves in-memory test stores; durable stores must CAS owner+epoch.
	fn claim_restore_epoch_observed(
		&self,
		sid: &str,
		owner: &str,
		_previous: &CreateRecord,
		_pending: &Value,
	) -> Result<u64> {
		self.claim_restore_epoch(sid, owner)
	}
	/// Return true only while the exact claimed generation is authoritative.
	fn owns_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<bool>;
	/// Finalize a restore only if its exact ownership generation still holds.
	fn finalize_restore_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<()>;
	/// Renew the exact claimed generation while a candidate is being restored.
	/// `false` means the PostgreSQL-clock lease has expired or was superseded.
	/// Implementations without a durable lease retain the conservative exact
	/// ownership check for compatibility.
	fn renew_restore_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<bool> {
		self.owns_epoch(sid, owner, epoch)
	}
	/// Return a failed claim to its observed prior lineage only when this exact
	/// candidate still owns it. A stale claimant must never undo a winner.
	fn abort_restore_epoch(
		&self,
		_sid: &str,
		_owner: &str,
		_epoch: u64,
		_previous_owner: &str,
		_previous_epoch: u64,
	) -> Result<()> {
		Ok(())
	}
	/// Make the recovered runtime parameters durable under the exact unexpired
	/// claim before routing may serve the candidate.
	fn commit_restore_epoch(
		&self,
		sid: &str,
		owner: &str,
		epoch: u64,
		_params: &Value,
		_ha: &str,
		_restart_policy: &str,
	) -> Result<()> {
		self.finalize_restore_epoch(sid, owner, epoch)
	}
	/// Resolve a potentially ambiguous restore commit without relying on the
	/// response path. Durable implementations inspect the exact owner epoch.
	fn restore_commit_applied(&self, _sid: &str, _owner: &str, _epoch: u64) -> Result<bool> {
		Ok(false)
	}
	fn worker_begin(&self, key: &str) -> bool;
	fn worker_end(&self, key: &str);
	fn note_event(&self, event: &str);
	fn replica_targets(&self, sid: &str, replicas: usize) -> Vec<String>;
	fn fenced_local_ids(&self, owned_ids: &[String]) -> Result<Vec<String>>;
	/// Report whether this node currently holds an in-flight target-migration
	/// guard for `sid`.
	///
	/// Such a candidate is deliberately paused and its ownership record is
	/// mid-transfer, so a transiently stale owner view must never fence it —
	/// tearing it down would discard the adopted runtime and the process-local
	/// secrets that only exist in memory.
	fn migration_target_in_flight(&self, _sid: &str) -> bool {
		false
	}
	/// Return the exact ownership generations local routing adopted for these
	/// running VMs. Missing entries must be fenced, never rebound implicitly.
	fn local_owner_epochs(&self, owned_ids: &[String]) -> Result<Vec<LocalOwnerEpoch>>;
	/// Renew only these exact locally adopted owner/epoch tuples; return VMs
	/// that lost authority or could not be verified before their deadline.
	fn renew_owner_leases(&self, local: &[LocalOwnerEpoch]) -> Result<Vec<String>>;
	fn forget_local_owner(&self, sid: &str);
}

pub trait ReconcileEngine: Send + Sync {
	fn owned_ids(&self) -> Result<Vec<String>>;
	fn get(&self, sid: &str) -> Result<SandboxRecord>;
	fn remove(&self, sid: &str) -> Result<()>;
	fn replicate_prepare<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<ReplicatePreparation>>;
	fn replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()>;
	fn restore_from_template<'a>(
		&'a self,
		params: &'a Value,
		snapshot_dir: &'a Path,
		quorum_ok: bool,
	) -> BoxFuture<'a, Result<SandboxRecord>>;
	/// Activate an exact paused restore candidate while its pending ownership
	/// marker still fences routing. Implementations must be idempotent so a
	/// restart can finish activation before the serving commit.
	fn activate_candidate<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<()>>;
	fn record_volume_leases(&self, sid: &str, leases: &[LeaseRecord]) -> Result<()>;
	fn record_idempotency(&self, sid: &str, key: &str) -> Result<()>;
	fn set_ha_metadata(&self, sid: &str, ha: &str, restart_policy: &str) -> Result<()>;
	fn create_from_record(&self, params: &Value) -> Result<SandboxRecord>;
	fn run_from_record(&self, params: &Value) -> Result<Value>;
	fn restore_from_record(&self, params: &Value) -> Result<Value>;
	/// Install a launch/readiness cancellation signal for an uncommitted
	/// restore. Implementations may use a no-op when no cancellable runtime is
	/// available.
	fn begin_restore_cancellation(&self, _sid: &str) {}
	fn cancel_restore(&self, _sid: &str) {}
	fn end_restore_cancellation(&self, _sid: &str) {}
	/// Report only a live, locally managed restore candidate. Registry
	/// presence alone is insufficient and implementations must fail closed.
	fn candidate_is_running(&self, _sid: &str) -> bool {
		false
	}
}

pub trait CreateRecordStore: Send + Sync {
	fn get(&self, sid: &str) -> Result<Option<CreateRecord>>;
	fn reconcile_records(&self) -> Result<()>;
}

pub trait ReplicaStore: Send + Sync {
	fn get<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<Option<ReplicaRecord>>>;
	fn holds(&self, sid: &str) -> Result<bool>;
	fn secrets_ready(&self, sid: &str) -> Result<bool>;
	fn drop_replica(&self, record: &ReplicaRecord) -> Result<()>;
	/// Derive and bind the durable shared-object identity to the active export.
	/// Non-production local replicas deliberately bind an empty identity.
	fn publication_key(&self, _sid: &str, _prep: &ReplicatePreparation) -> Result<String> {
		Ok(String::new())
	}
	/// Pin the local materialized cache root while this replica is consumed.
	/// Implementations without a local cache may return no guard.
	fn acquire_cache_root(&self, _record: &ReplicaRecord) -> Option<Box<dyn Send>> {
		None
	}
}

pub trait LeaseManager: Send + Sync {
	/// Renew every local writable-volume lease by strict majority, stopping
	/// writers that miss their renew deadline.
	fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>>;
	/// Acquire strict-majority leases for all writable volumes in `params`.
	fn acquire_writable_volume_leases<'a>(
		&'a self,
		params: &'a Value,
		epoch: u64,
	) -> BoxFuture<'a, Result<Vec<LeaseRecord>>>;
	/// Best-effort release for already-granted lease records.
	fn release_leases<'a>(&'a self, leases: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>>;
	/// Release every lease recorded for a sandbox and clear its metadata.
	fn release_record_volume_leases<'a>(
		&'a self,
		record: &'a SandboxRecord,
	) -> BoxFuture<'a, Result<()>>;
}

pub struct Reconciler<'a> {
	pub config:           ReconcileConfig,
	pub mesh:             &'a dyn MeshReconcileState,
	pub engine:           &'a dyn ReconcileEngine,
	pub records:          &'a dyn CreateRecordStore,
	pub replicas:         &'a dyn ReplicaStore,
	pub leases:           &'a dyn LeaseManager,
	pub client:           &'a Client,
	pub checkpoint_times: Mutex<HashMap<String, f64>>,
}

/// A restore claim stays non-serving until its candidate, writable-volume
/// leases, durable record, and local routing have all crossed the commit
/// boundary. Every failure after `claim_restore_epoch` must consume this
/// guard, rather than leaking a live candidate or detaching side effects.
struct ClaimGuard<'r, 'a> {
	reconciler:     &'r Reconciler<'a>,
	sid:            String,
	owner:          String,
	epoch:          u64,
	previous_owner: String,
	previous_epoch: u64,
	lease_records:  Vec<LeaseRecord>,
	restored:       Option<SandboxRecord>,
	committed:      bool,
}

impl<'r, 'a> ClaimGuard<'r, 'a> {
	fn new(
		reconciler: &'r Reconciler<'a>,
		sid: &str,
		owner: &str,
		epoch: u64,
		previous: &CreateRecord,
	) -> Self {
		reconciler.engine.begin_restore_cancellation(sid);
		Self {
			reconciler,
			sid: sid.to_owned(),
			owner: owner.to_owned(),
			epoch,
			previous_owner: previous.owner.clone(),
			previous_epoch: previous.epoch,
			lease_records: Vec::new(),
			restored: None,
			committed: false,
		}
	}

	/// Tear down before releasing the exact claim. If teardown cannot be
	/// confirmed, retain the claim and fence local routing: another claimant
	/// must not race a possibly-running candidate.
	async fn abort(&self) {
		if self.committed {
			return;
		}
		self.reconciler.engine.cancel_restore(&self.sid);
		self.reconciler.mesh.forget_local_owner(&self.sid);
		let record = self
			.restored
			.clone()
			.or_else(|| self.reconciler.engine.get(&self.sid).ok());
		let torn_down = match record {
			Some(record) => {
				if self.reconciler.engine.remove(&record.name).is_err() {
					false
				} else {
					let _ = self
						.reconciler
						.leases
						.release_record_volume_leases(&record)
						.await;
					true
				}
			},
			None => true,
		};
		let _ = self
			.reconciler
			.leases
			.release_leases(&self.lease_records)
			.await;
		if torn_down {
			let _ = self.reconciler.mesh.abort_restore_epoch(
				&self.sid,
				&self.owner,
				self.epoch,
				&self.previous_owner,
				self.previous_epoch,
			);
		}
		self.reconciler.engine.end_restore_cancellation(&self.sid);
	}
}

impl<'a> Reconciler<'a> {
	pub fn new(
		config: ReconcileConfig,
		mesh: &'a dyn MeshReconcileState,
		engine: &'a dyn ReconcileEngine,
		records: &'a dyn CreateRecordStore,
		replicas: &'a dyn ReplicaStore,
		leases: &'a dyn LeaseManager,
		client: &'a Client,
	) -> Self {
		Self {
			config,
			mesh,
			engine,
			records,
			replicas,
			leases,
			client,
			checkpoint_times: Mutex::new(HashMap::new()),
		}
	}

	/// One replication cadence.  The caller owns `last` so `replicate_forever`
	/// can suppress redundant transfers across sleeps without storing the digest
	/// in durable state.
	pub async fn replicate_once(&self, last: &mut HashMap<String, String>) -> Result<()> {
		if !self.mesh.enabled() {
			return Ok(());
		}
		let owned_ids = self.engine.owned_ids()?;
		let owned = owned_ids.iter().cloned().collect::<HashSet<_>>();
		self.prune_replication_bookkeeping(last, &owned);
		for sid in owned_ids {
			self.replicate_sid(&sid, last).await;
		}
		Ok(())
	}

	/// Clears cadence bookkeeping for deterministic module tests without
	/// altering production scheduling or checkpoint state.
	#[cfg(test)]
	pub(crate) fn reset_replication_cadence_for_test(&self, sid: &str) {
		self.checkpoint_times.lock().remove(sid);
	}

	/// Renew and fence owner leases without waiting for migration, orphan, or
	/// replication work. Runtime should schedule this independently at a cadence
	/// shorter than the owner lease TTL.
	pub async fn renew_owner_leases_once(&self) -> Result<()> {
		if !self.mesh.enabled() {
			return Ok(());
		}
		let owned_ids = self.engine.owned_ids()?;
		let local = self.mesh.local_owner_epochs(&owned_ids)?;
		let mut fence = self.mesh.fenced_local_ids(&owned_ids)?;
		fence.extend(self.mesh.renew_owner_leases(&local)?);
		fence.sort();
		fence.dedup();
		fence.retain(|sid| !self.mesh.migration_target_in_flight(sid));
		for sid in fence {
			self.fence_local(&sid).await;
		}
		Ok(())
	}

	/// One HA reconciliation cadence: renew leases, reconcile durable records,
	/// drain orphan queue, then fence local VMs superseded by higher-epoch
	/// owners.
	pub async fn ha_reconcile_once(&self) -> Result<()> {
		if !self.mesh.enabled() {
			return Ok(());
		}
		if let Err(err) = self.leases.renew_writable_volume_leases_once().await {
			error!("failed to renew writable-volume leases: {err}");
		}
		if let Err(err) = self.records.reconcile_records() {
			error!("failed to reconcile create records: {err}");
		}
		for (sid, dead) in self.mesh.drain_orphans() {
			self.reconcile_orphan(&sid, &dead).await;
		}
		let owned_ids = self.engine.owned_ids()?;
		let local = self.mesh.local_owner_epochs(&owned_ids)?;
		let mut fence = self.mesh.fenced_local_ids(&owned_ids)?;
		match self.mesh.renew_owner_leases(&local) {
			Ok(expired_or_fenced) => fence.extend(expired_or_fenced),
			Err(err) => {
				error!("failed to renew owner leases: {err}");
				// Runtime tracks each locally held deadline and must return
				// every expired/unverifiable VM on the next successful check.
			},
		}
		fence.sort();
		fence.dedup();
		fence.retain(|sid| !self.mesh.migration_target_in_flight(sid));
		for sid in fence {
			self.fence_local(&sid).await;
		}
		Ok(())
	}

	async fn replicate_sid(&self, sid: &str, last: &mut HashMap<String, String>) {
		let mut digest = String::new();
		let mut snapshot_dir = PathBuf::new();
		let result: Result<()> = async {
			let Some(owner_record) = self.records.get(sid)? else {
				return Err(EngineError::not_found(format!(
					"cannot replicate {sid} without an authoritative ownership record"
				)));
			};
			if record_restore_pending(&owner_record) {
				last.remove(sid);
				self.checkpoint_times.lock().remove(sid);
				return Ok(());
			}
			if owner_record.owner != self.mesh.node_id() {
				return Err(EngineError::invalid(format!(
					"cannot replicate {sid}: owner {} is not this node",
					owner_record.owner
				)));
			}
			if owner_record.ha == "off" {
				last.remove(sid);
				self.checkpoint_times.lock().remove(sid);
				return Ok(());
			}
			let now = unix_time();
			{
				let mut checkpoint_times = self.checkpoint_times.lock();
				if let Some(last_attempt) = checkpoint_times.get(sid)
					&& now - *last_attempt < self.config.replicate_sec.max(1.0)
				{
					return Ok(());
				}
				checkpoint_times.insert(sid.to_owned(), now);
			}
			let prep = match self.engine.replicate_prepare(sid).await {
				Ok(prep) => prep,
				Err(err) => {
					warn!("replicate skip {sid}: {err}");
					return Ok(());
				},
			};
			// A matching content digest does not identify the checkpoint
			// lifecycle. Its generation and launch parameters may have changed,
			// and peers must durably observe that newer metadata even when their
			// immutable object upload is a no-op.
			let object_key = self.replicas.publication_key(sid, &prep)?;
			digest.clone_from(&prep.digest);
			snapshot_dir.clone_from(&prep.snapshot_dir);
			let targets = self.mesh.replica_targets(sid, self.config.replicas);
			info!("replicate {sid} digest={} targets={targets:?}", digest_prefix(&prep.digest));
			if targets.is_empty() {
				return Ok(());
			}
			let summary = self
				.push_replica_to_targets(
					targets,
					&prep,
					&object_key,
					&owner_record.owner,
					owner_record.epoch,
				)
				.await;
			if summary.any {
				self.mesh.note_event("replication");
				self
					.checkpoint_times
					.lock()
					.insert(sid.to_owned(), unix_time());
			}
			if summary.all {
				last.insert(sid.to_owned(), prep.digest);
			}
			Ok(())
		}
		.await;
		if let Err(err) = result {
			error!("replication cycle failed for {sid}: {err}");
		}
		if !digest.is_empty()
			&& !snapshot_dir.as_os_str().is_empty()
			&& let Err(err) = self.engine.replicate_cleanup(&digest, &snapshot_dir)
		{
			warn!("replication cleanup failed for {sid}: {err}");
		}
	}

	async fn push_replica_to_targets(
		&self,
		targets: Vec<String>,
		prep: &ReplicatePreparation,
		object_key: &str,
		source_node: &str,
		source_epoch: u64,
	) -> PushSummary {
		let concurrency = self.config.replicate_concurrency.max(1);
		let semaphore = std::sync::Arc::new(Semaphore::new(concurrency));
		let mut futures = FuturesUnordered::new();
		for peer_id in targets {
			futures.push(push_replica(
				self.client,
				self.mesh,
				semaphore.clone(),
				peer_id,
				object_key.to_owned(),
				prep.digest.clone(),
				prep.checkpoint_generation,
				source_node.to_owned(),
				source_epoch,
				prep.params.clone(),
				self.config.outbound_token.clone(),
			));
		}
		let mut any = false;
		let mut all = true;
		while let Some(ok) = futures.next().await {
			any |= ok;
			all &= ok;
		}
		PushSummary { any, all }
	}

	fn prune_replication_bookkeeping(
		&self,
		last: &mut HashMap<String, String>,
		owned: &HashSet<String>,
	) {
		let mut checkpoint_times = self.checkpoint_times.lock();
		let stale = last
			.keys()
			.chain(checkpoint_times.keys())
			.filter(|sid| !owned.contains(*sid))
			.cloned()
			.collect::<HashSet<_>>();
		for sid in stale {
			last.remove(&sid);
			checkpoint_times.remove(&sid);
		}
	}

	async fn reconcile_orphan(&self, sid: &str, dead: &str) {
		let result = async {
			let create_record = self.records.get(sid)?;
			let restore_pending = create_record.as_ref().is_some_and(record_restore_pending);
			let local_candidate_missing = self.mesh.local_candidate_missing(sid)?;
			if !restore_pending
				&& !local_candidate_missing
				&& let Some(owner) = self.mesh.owner_of(sid)?
				&& owner != dead
				&& (owner == self.mesh.node_id() || self.mesh.is_peer_healthy(&owner))
			{
				return Ok(false);
			}
			if !restore_pending && !local_candidate_missing && self.mesh.is_peer_healthy(dead) {
				warn!("deferring restore of {sid}: prior owner {dead} is still reachable");
				return Ok(true);
			}
			if !restore_pending && self.mesh.is_nonserving_lifecycle(sid)? {
				return Ok(false);
			}
			let ha = create_record
				.as_ref()
				.map_or("async", |record| record.ha.as_str());
			let can_rerun = create_record.as_ref().is_some_and(record_can_rerun);
			if ha == "off" {
				return Ok(false);
			}
			let allow_checkpoint = matches!(ha, "async" | "async+rerun");
			let replica = if allow_checkpoint {
				self.replicas.get(sid).await?
			} else {
				None
			};
			let replica_held = replica.is_some();
			let replica_needs_secrets = replica.as_ref().is_some_and(|record| record.needs_secrets);
			let replica_secrets_ready =
				replica_held && (!replica_needs_secrets || self.replicas.secrets_ready(sid)?);
			if replica_needs_secrets && !replica_secrets_ready {
				warn!("cannot auto-restore {sid}: replica secrets unavailable on this node");
				return Ok(true);
			}
			if !replica_needs_secrets {
				let exclude = if local_candidate_missing {
					HashSet::new()
				} else {
					HashSet::from([dead.to_owned()])
				};
				let retrying_local_claim = restore_pending
					&& create_record
						.as_ref()
						.is_some_and(|record| record.owner == self.mesh.node_id());
				if !retrying_local_claim
					&& self.mesh.restore_owner(sid, &exclude).as_deref() != Some(self.mesh.node_id())
				{
					return Ok(true);
				}
			}
			let restore_quorum = self
				.config
				.restore_quorum_enabled(self.mesh.expected_members());
			if restore_quorum && !local_candidate_missing && !self.restore_quorum_confirmed(dead).await
			{
				warn!(
					"deferring restore of {sid}: quorum not met ({}/{})",
					self.last_quorum_confirmations(dead).await,
					self.mesh.quorum_needed()
				);
				return Ok(true);
			}
			if replica_secrets_ready {
				return self
					.restore_from_replica(sid, dead, create_record.as_ref(), restore_quorum)
					.await;
			}
			if can_rerun && self.rerun_from_record(sid, dead).await? {
				return Ok(false);
			}
			if !replica_held {
				warn!("cannot auto-restore {sid}: no local replica (will retry)");
			}
			Ok(true)
		}
		.await;
		let retry = match result {
			Ok(should_retry) => should_retry,
			Err(err) => {
				error!("failed to restore orphaned sandbox {sid} from {dead}: {err}");
				true
			},
		};
		if retry {
			self.mesh.requeue_orphan(sid, dead);
		}
	}

	async fn restore_quorum_confirmed(&self, dead: &str) -> bool {
		let confirmations = self.last_quorum_confirmations(dead).await;
		self.mesh.restore_quorum_met(confirmations)
	}

	async fn last_quorum_confirmations(&self, dead: &str) -> usize {
		let mut confirmations = 1;
		for member_id in self.mesh.live_member_ids() {
			if member_id == self.mesh.node_id() {
				continue;
			}
			let Some(url) = self.mesh.peer_url(&member_id) else {
				continue;
			};
			let target = format!("{}/v1/mesh/reachable/{dead}", url.trim_end_matches('/'));
			let Ok(response) = self
				.client
				.get(target)
				.bearer_auth(&self.config.outbound_token)
				.header("X-Vmon-Mesh-Hop", "1")
				.timeout(self.mesh.create_timeout())
				.send()
				.await
			else {
				continue;
			};
			let Ok(payload) = response.json::<Value>().await else {
				continue;
			};
			if payload.get("reachable").and_then(Value::as_bool) == Some(false) {
				confirmations += 1;
			}
		}
		confirmations
	}

	async fn restore_from_replica(
		&self,
		sid: &str,
		dead: &str,
		create_record: Option<&CreateRecord>,
		restore_quorum: bool,
	) -> Result<bool> {
		let key = format!("restore:{sid}");
		if !self.mesh.worker_begin(&key) {
			return Ok(true);
		}
		let result = async {
			let rec = self
				.replicas
				.get(sid)
				.await?
				.ok_or_else(|| EngineError::not_found("no local replica"))?;
			let _cache_root = self.replicas.acquire_cache_root(&rec);
			let Some(lineage) = create_record else {
				self.replicas.drop_replica(&rec)?;
				return Err(EngineError::invalid(format!(
					"restore of {sid} has no authoritative ownership lineage"
				)));
			};
			let pending = replica_restore_pending(&rec, lineage);
			let valid_lineage = if lineage
				.params
				.as_object()
				.is_some_and(|params| params.contains_key("_mesh_restore_pending"))
			{
				replica_matches_restore_pending(&rec, &pending)
			} else {
				replica_lineage_matches(&rec, lineage)
			};
			if !valid_lineage {
				self.replicas.drop_replica(&rec)?;
				return Err(EngineError::invalid(format!(
					"restore of {sid} rejected replica inconsistent with its durable restore intent"
				)));
			}
			let owner = self.mesh.node_id();
			let epoch = self
				.mesh
				.claim_restore_epoch_observed(sid, owner, lineage, &pending)?;
			let mut claim = ClaimGuard::new(self, sid, owner, epoch, lineage);
			// A crash after candidate launch but before the serving commit can
			// leave the exact VM paused under this pending claim. Re-validate the
			// claim and finish activation without launching a second VM.
			if self.engine.candidate_is_running(sid) {
				let restored = match self.engine.get(sid) {
					Ok(restored) if restored.name == sid => restored,
					Ok(_) | Err(_) => {
						claim.abort().await;
						return Err(EngineError::busy(format!(
							"restore of {sid} has an unverifiable retained candidate"
						)));
					},
				};
				claim.restored = Some(restored.clone());
				let retained = async {
					if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
						return Err(EngineError::busy(format!(
							"restore of {sid} lost ownership epoch {epoch} before retained lease recovery"
						)));
					}
					claim.lease_records = self
						.leases
						.acquire_writable_volume_leases(&rec.params, epoch)
						.await?;
					self
						.engine
						.record_volume_leases(&restored.name, &claim.lease_records)?;
					if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
						return Err(EngineError::busy(format!(
							"restore of {sid} lost ownership epoch {epoch} before retained activation"
						)));
					}
					self
						.engine
						.set_ha_metadata(&restored.name, &lineage.ha, &lineage.restart_policy)?;
					self.engine.activate_candidate(sid).await?;
					if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
						return Err(EngineError::busy(format!(
							"restore of {sid} lost ownership epoch {epoch} after retained activation"
						)));
					}
					Ok::<(), EngineError>(())
				}
				.await;
				if let Err(error) = retained {
					claim.abort().await;
					return Err(error);
				}
				if let Err(error) = self.mesh.commit_restore_epoch(
					sid,
					owner,
					epoch,
					&rec.params,
					&lineage.ha,
					&lineage.restart_policy,
				) && !self.mesh.restore_commit_applied(sid, owner, epoch)?
				{
					claim.committed = true;
					self.engine.end_restore_cancellation(sid);
					return Err(error);
				}
				claim.committed = true;
				self.engine.end_restore_cancellation(sid);
				if let Err(error) = self.replicas.drop_replica(&rec) {
					warn!(sid, %error, "restored replica remains cached after retained commit");
				}
				self.mesh.note_event("restore");
				return Ok(false);
			}
			let attempt = async {
				claim.lease_records = self
					.leases
					.acquire_writable_volume_leases(&rec.params, epoch)
					.await?;
				if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
					return Err(EngineError::busy(format!(
						"restore of {sid} lost ownership epoch {epoch} before launch"
					)));
				}
				let launch =
					self
						.engine
						.restore_from_template(&rec.params, &rec.snapshot_dir, restore_quorum);
				tokio::pin!(launch);
				let mut renewal = tokio::time::interval(Duration::from_millis(250));
				renewal.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
				let mut lease_lost = false;
				let restored = loop {
					tokio::select! {
						result = &mut launch => break result?,
						_ = renewal.tick(), if !lease_lost => {
							if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
								lease_lost = true;
								self.engine.cancel_restore(sid);
							}
						},
					}
				};
				if lease_lost {
					return Err(EngineError::busy(format!(
						"restore of {sid} lost ownership epoch {epoch} during launch"
					)));
				}
				claim.restored = Some(restored.clone());
				if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
					return Err(EngineError::busy(format!(
						"restore of {sid} lost ownership epoch {epoch} after launch"
					)));
				}
				self
					.engine
					.set_ha_metadata(&restored.name, &lineage.ha, &lineage.restart_policy)?;
				self
					.engine
					.record_volume_leases(&restored.name, &claim.lease_records)?;
				self.engine.activate_candidate(sid).await?;
				if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
					return Err(EngineError::busy(format!(
						"restore of {sid} lost ownership epoch {epoch} after activation"
					)));
				}
				// A commit error may be a response-path failure after PostgreSQL
				// applied the exact CAS. Retry the idempotent operation once;
				// if its outcome remains ambiguous, retain the fenced candidate
				// rather than tearing down a potentially serving VM.
				if let Err(first_error) = self.mesh.commit_restore_epoch(
					sid,
					owner,
					epoch,
					&rec.params,
					&lineage.ha,
					&lineage.restart_policy,
				) && self
					.mesh
					.commit_restore_epoch(
						sid,
						owner,
						epoch,
						&rec.params,
						&lineage.ha,
						&lineage.restart_policy,
					)
					.is_err() && !self.mesh.restore_commit_applied(sid, owner, epoch)?
				{
					claim.committed = true;
					self.engine.end_restore_cancellation(sid);
					return Err(first_error);
				}
				claim.committed = true;
				self.engine.end_restore_cancellation(sid);
				Ok::<_, EngineError>(())
			}
			.await;
			if let Err(error) = attempt {
				claim.abort().await;
				return Err(error);
			}
			if let Err(error) = self.replicas.drop_replica(&rec) {
				warn!(sid, %error, "restored replica remains cached after serving commit");
			}
			self.mesh.note_event("restore");
			info!("restored orphaned sandbox {sid} from replica after {dead} died");
			Ok(false)
		}
		.await;
		self.mesh.worker_end(&key);
		result
	}

	#[cfg(test)]
	/// Shared best-effort cleanup used by focused failure tests and callers
	/// that have not yet obtained an exact [`ClaimGuard`].
	async fn cleanup_failed_restore(
		&self,
		sid: &str,
		restored: Option<&SandboxRecord>,
		lease_records: &[LeaseRecord],
	) {
		let record = restored.cloned().or_else(|| self.engine.get(sid).ok());
		if let Some(record) = record {
			let _ = self.engine.remove(&record.name);
			let _ = self.leases.release_record_volume_leases(&record).await;
		}
		let _ = self.leases.release_leases(lease_records).await;
	}

	async fn rerun_from_record(&self, sid: &str, dead: &str) -> Result<bool> {
		let Some(record) = self.records.get(sid)? else {
			return Ok(false);
		};
		if !record_can_rerun(&record) {
			return Ok(false);
		}
		let key = format!("rerun:{sid}");
		if !self.mesh.worker_begin(&key) {
			return Ok(true);
		}
		let result = async {
			let pending = rerun_restore_pending(&record);
			if pending.get("kind").and_then(Value::as_str) != Some("rerun") {
				return Err(EngineError::invalid(format!(
					"rerun of {sid} found an incompatible durable restore intent"
				)));
			}
			let owner = self.mesh.node_id();
			let epoch = self
				.mesh
				.claim_restore_epoch_observed(sid, owner, &record, &pending)?;
			let mut claim = ClaimGuard::new(self, sid, owner, epoch, &record);
			let attempt = async {
				if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
					return Err(EngineError::busy(format!(
						"rerun of {sid} lost ownership epoch {epoch} before launch"
					)));
				}
				let mut params = params_object(record.params.clone())?;
				let kind = params
					.remove("_kind")
					.and_then(|value| value.as_str().map(str::to_owned))
					.unwrap_or_else(|| "sandbox".to_owned());
				params.insert("name".to_owned(), Value::String(sid.to_owned()));
				params.insert("ha".to_owned(), Value::String(record.ha.clone()));
				params
					.insert("restart_policy".to_owned(), Value::String(record.restart_policy.clone()));
				if kind == "run" || kind == "restore" {
					params.insert("detach".to_owned(), Value::Bool(true));
				}
				let params = Value::Object(params);
				let created_sid = match kind.as_str() {
					"run" => created_name_from_value(&self.engine.run_from_record(&params)?, sid),
					"restore" => {
						created_name_from_value(&self.engine.restore_from_record(&params)?, sid)
					},
					_ => self.engine.create_from_record(&params)?.name,
				};
				claim.restored = self.engine.get(&created_sid).ok();
				if !self.mesh.renew_restore_epoch(sid, owner, epoch)? {
					return Err(EngineError::busy(format!(
						"rerun of {sid} lost ownership epoch {epoch} after launch"
					)));
				}
				self
					.engine
					.set_ha_metadata(&created_sid, &record.ha, &record.restart_policy)?;
				self
					.engine
					.record_idempotency(&created_sid, &record.idempotency_key)?;
				self.mesh.commit_restore_epoch(
					sid,
					owner,
					epoch,
					&record.params,
					&record.ha,
					&record.restart_policy,
				)?;
				claim.committed = true;
				self.engine.end_restore_cancellation(sid);
				Ok::<_, EngineError>(true)
			}
			.await;
			if let Err(error) = attempt {
				claim.abort().await;
				return Err(error);
			}
			self.mesh.note_event("restore");
			info!("re-ran orphaned sandbox {sid} from create record after {dead} died");
			Ok(true)
		}
		.await;
		self.mesh.worker_end(&key);
		result
	}

	async fn fence_local(&self, sid: &str) {
		let result: Result<()> = async {
			let record = self.engine.get(sid)?;
			self.engine.remove(sid)?;
			self.leases.release_record_volume_leases(&record).await?;
			self.mesh.forget_local_owner(sid);
			self.mesh.note_event("fence");
			warn!("fenced {sid}: superseded by a higher-epoch owner");
			Ok(())
		}
		.await;
		if let Err(err) = result {
			error!("failed to fence superseded sandbox {sid}: {err}");
		}
	}
}

#[derive(Clone, Copy, Debug)]
struct PushSummary {
	any: bool,
	all: bool,
}

pub async fn replicate_forever(ctx: &Reconciler<'_>) {
	let mut last = HashMap::new();
	if ctx.config.replicate_sec <= 0.0 {
		return;
	}
	loop {
		sleep(Duration::from_secs_f64(ctx.config.replicate_sec)).await;
		if let Err(err) = ctx.replicate_once(&mut last).await {
			error!("replication pass failed: {err}");
		}
	}
}

pub async fn ha_reconcile_forever(ctx: &Reconciler<'_>) {
	loop {
		sleep(Duration::from_secs_f64(ctx.mesh.interval().max(1.0))).await;
		if let Err(err) = ctx.ha_reconcile_once().await {
			error!("HA reconciliation pass failed: {err}");
		}
	}
}

async fn push_replica(
	client: &Client,
	mesh: &dyn MeshReconcileState,
	semaphore: std::sync::Arc<Semaphore>,
	peer_id: String,
	object_key: String,
	digest: String,
	checkpoint_generation: u64,
	source_node: String,
	source_epoch: u64,
	params: Value,
	outbound_token: String,
) -> bool {
	let Some(url) = mesh.peer_url(&peer_id) else {
		return false;
	};
	let Ok(_permit) = semaphore.acquire_owned().await else {
		return false;
	};
	let target = format!("{}/v1/mesh/replica/receive", url.trim_end_matches('/'));
	let body = json!({
		"object_key": object_key,
		"digest": digest,
		"source_url": mesh.advertise(),
		"source_node": source_node,
		"source_epoch": source_epoch,
		"checkpoint_generation": checkpoint_generation,
		"params": params,
	});
	let result = client
		.post(target)
		.bearer_auth(outbound_token)
		.header("X-Vmon-Mesh-Hop", "1")
		.timeout(mesh.create_timeout())
		.json(&body)
		.send()
		.await
		.and_then(reqwest::Response::error_for_status);
	if let Err(err) = result {
		error!("failed to replicate sandbox to peer {peer_id}: {err}");
		false
	} else {
		true
	}
}

fn replica_lineage_matches(replica: &ReplicaRecord, owner: &CreateRecord) -> bool {
	replica.source_node == owner.owner && replica.source_epoch == owner.epoch
}

fn replica_restore_pending(replica: &ReplicaRecord, owner: &CreateRecord) -> Value {
	let mut pending = Map::new();
	pending.insert("kind".to_owned(), Value::String("replica".to_owned()));
	pending.insert("source_owner".to_owned(), Value::String(replica.source_node.clone()));
	pending.insert("source_epoch".to_owned(), Value::from(replica.source_epoch));
	pending.insert("digest".to_owned(), Value::String(replica.digest.clone()));
	pending.insert("checkpoint_generation".to_owned(), Value::from(replica.checkpoint_generation));
	if owner.params.get("_mesh_restore_pending").is_some() {
		return owner.params["_mesh_restore_pending"].clone();
	}
	Value::Object(pending)
}

fn rerun_restore_pending(owner: &CreateRecord) -> Value {
	if let Some(pending) = owner.params.get("_mesh_restore_pending") {
		return pending.clone();
	}
	let mut pending = Map::new();
	pending.insert("kind".to_owned(), Value::String("rerun".to_owned()));
	pending.insert("source_owner".to_owned(), Value::String(owner.owner.clone()));
	pending.insert("source_epoch".to_owned(), Value::from(owner.epoch));
	Value::Object(pending)
}

fn replica_matches_restore_pending(replica: &ReplicaRecord, pending: &Value) -> bool {
	pending.get("kind").and_then(Value::as_str) == Some("replica")
		&& pending.get("source_owner").and_then(Value::as_str) == Some(&replica.source_node)
		&& pending.get("source_epoch").and_then(Value::as_u64) == Some(replica.source_epoch)
		&& pending.get("digest").and_then(Value::as_str) == Some(&replica.digest)
		&& pending.get("checkpoint_generation").and_then(Value::as_u64)
			== Some(replica.checkpoint_generation)
}

fn record_restore_pending(record: &CreateRecord) -> bool {
	record
		.params
		.as_object()
		.is_some_and(|params| params.contains_key("_mesh_restore_pending"))
}

fn record_can_rerun(record: &CreateRecord) -> bool {
	record.restart_policy == "rerun" || record.ha.contains("rerun")
}

fn params_object(value: Value) -> Result<Map<String, Value>> {
	match value {
		Value::Object(map) => Ok(map),
		_ => Err(EngineError::invalid("record params must be an object")),
	}
}

fn created_name_from_value(value: &Value, fallback: &str) -> String {
	value
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
		.unwrap_or(fallback)
		.to_owned()
}

fn digest_prefix(digest: &str) -> &str {
	digest.get(..12).unwrap_or(digest)
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	};

	use super::*;

	struct Mesh {
		suspended:       bool,
		retain_owner:    bool,
		commit_failures: AtomicUsize,
		commits:         AtomicUsize,
		fenced:          Vec<String>,
		migrating:       bool,
	}

	impl MeshReconcileState for Mesh {
		fn enabled(&self) -> bool {
			true
		}

		fn interval(&self) -> f64 {
			1.0
		}

		fn node_id(&self) -> &'static str {
			"node-b"
		}

		fn advertise(&self) -> String {
			String::new()
		}

		fn create_timeout(&self) -> Duration {
			Duration::from_secs(1)
		}

		fn expected_members(&self) -> usize {
			1
		}

		fn live_member_ids(&self) -> Vec<String> {
			vec![]
		}

		fn peer_url(&self, _: &str) -> Option<String> {
			None
		}

		fn is_peer_healthy(&self, _: &str) -> bool {
			false
		}

		fn owner_of(&self, _: &str) -> Result<Option<String>> {
			Ok(None)
		}

		fn restore_owner(&self, _: &str, _: &HashSet<String>) -> Option<String> {
			None
		}

		fn restore_quorum_met(&self, _: usize) -> bool {
			true
		}

		fn quorum_needed(&self) -> usize {
			1
		}

		fn drain_orphans(&self) -> Vec<(String, String)> {
			vec![]
		}

		fn is_nonserving_lifecycle(&self, _: &str) -> Result<bool> {
			Ok(self.suspended)
		}

		fn requeue_orphan(&self, _: &str, _: &str) {}

		fn claim_restore_epoch(&self, _: &str, _: &str) -> Result<u64> {
			Ok(1)
		}

		fn owns_epoch(&self, _: &str, _: &str, _: u64) -> Result<bool> {
			Ok(self.retain_owner)
		}

		fn finalize_restore_epoch(&self, _: &str, _: &str, _: u64) -> Result<()> {
			Ok(())
		}

		fn commit_restore_epoch(
			&self,
			_: &str,
			_: &str,
			_: u64,
			_: &Value,
			_: &str,
			_: &str,
		) -> Result<()> {
			self.commits.fetch_add(1, Ordering::SeqCst);
			if self
				.commit_failures
				.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |failures| failures.checked_sub(1))
				.is_ok()
			{
				return Err(EngineError::engine("ambiguous commit response"));
			}
			Ok(())
		}

		fn worker_begin(&self, _: &str) -> bool {
			true
		}

		fn worker_end(&self, _: &str) {}

		fn note_event(&self, _: &str) {}

		fn replica_targets(&self, _: &str, _: usize) -> Vec<String> {
			vec![]
		}

		fn fenced_local_ids(&self, _: &[String]) -> Result<Vec<String>> {
			Ok(self.fenced.clone())
		}

		fn migration_target_in_flight(&self, _: &str) -> bool {
			self.migrating
		}

		fn local_owner_epochs(&self, _: &[String]) -> Result<Vec<LocalOwnerEpoch>> {
			Ok(vec![])
		}

		fn renew_owner_leases(&self, _: &[LocalOwnerEpoch]) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn forget_local_owner(&self, _: &str) {}
	}

	struct SecretMesh {
		node:   &'static str,
		claims: Arc<AtomicUsize>,
		winner: Arc<AtomicBool>,
	}
	impl MeshReconcileState for SecretMesh {
		fn enabled(&self) -> bool {
			true
		}

		fn interval(&self) -> f64 {
			1.0
		}

		fn node_id(&self) -> &str {
			self.node
		}

		fn advertise(&self) -> String {
			String::new()
		}

		fn create_timeout(&self) -> Duration {
			Duration::from_secs(1)
		}

		fn expected_members(&self) -> usize {
			1
		}

		fn live_member_ids(&self) -> Vec<String> {
			vec![]
		}

		fn peer_url(&self, _: &str) -> Option<String> {
			None
		}

		fn is_peer_healthy(&self, _: &str) -> bool {
			false
		}

		fn owner_of(&self, _: &str) -> Result<Option<String>> {
			Ok(None)
		}

		fn restore_owner(&self, _: &str, _: &HashSet<String>) -> Option<String> {
			Some("node-b".to_owned())
		}

		fn restore_quorum_met(&self, _: usize) -> bool {
			true
		}

		fn quorum_needed(&self) -> usize {
			1
		}

		fn drain_orphans(&self) -> Vec<(String, String)> {
			vec![]
		}

		fn requeue_orphan(&self, _: &str, _: &str) {}

		fn claim_restore_epoch(&self, _: &str, _: &str) -> Result<u64> {
			self.claims.fetch_add(1, Ordering::SeqCst);
			if self.winner.swap(true, Ordering::SeqCst) {
				Err(EngineError::busy("restore claim fenced"))
			} else {
				Ok(7)
			}
		}

		fn owns_epoch(&self, _: &str, _: &str, _: u64) -> Result<bool> {
			Ok(true)
		}

		fn finalize_restore_epoch(&self, _: &str, _: &str, _: u64) -> Result<()> {
			Ok(())
		}

		fn worker_begin(&self, _: &str) -> bool {
			true
		}

		fn worker_end(&self, _: &str) {}

		fn note_event(&self, _: &str) {}

		fn replica_targets(&self, _: &str, _: usize) -> Vec<String> {
			vec![]
		}

		fn fenced_local_ids(&self, _: &[String]) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn local_owner_epochs(&self, _: &[String]) -> Result<Vec<LocalOwnerEpoch>> {
			Ok(vec![])
		}

		fn renew_owner_leases(&self, _: &[LocalOwnerEpoch]) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn forget_local_owner(&self, _: &str) {}
	}

	struct Engine {
		record:              SandboxRecord,
		removes:             AtomicUsize,
		restores:            AtomicUsize,
		activations:         AtomicUsize,
		activation_failures: AtomicUsize,
		candidate_running:   AtomicBool,
		lease_records:       AtomicUsize,
	}

	impl ReconcileEngine for Engine {
		fn owned_ids(&self) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn get(&self, _: &str) -> Result<SandboxRecord> {
			Ok(self.record.clone())
		}

		fn remove(&self, _: &str) -> Result<()> {
			self.removes.fetch_add(1, Ordering::SeqCst);
			self.candidate_running.store(false, Ordering::SeqCst);
			Ok(())
		}

		fn replicate_prepare<'a>(
			&'a self,
			_: &'a str,
		) -> BoxFuture<'a, Result<ReplicatePreparation>> {
			Box::pin(async { Err(EngineError::engine("unused")) })
		}

		fn replicate_cleanup(&self, _: &str, _: &Path) -> Result<()> {
			Ok(())
		}

		fn restore_from_template<'a>(
			&'a self,
			_: &'a Value,
			_: &'a Path,
			_: bool,
		) -> BoxFuture<'a, Result<SandboxRecord>> {
			self.restores.fetch_add(1, Ordering::SeqCst);
			self.candidate_running.store(true, Ordering::SeqCst);
			Box::pin(async move { Ok(self.record.clone()) })
		}

		fn activate_candidate<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<()>> {
			self.activations.fetch_add(1, Ordering::SeqCst);
			let fail = self
				.activation_failures
				.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |failures| failures.checked_sub(1))
				.is_ok();
			Box::pin(async move {
				if fail {
					Err(EngineError::engine("activation failed"))
				} else {
					Ok(())
				}
			})
		}

		fn record_volume_leases(&self, _: &str, _: &[LeaseRecord]) -> Result<()> {
			self.lease_records.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn record_idempotency(&self, _: &str, _: &str) -> Result<()> {
			Ok(())
		}

		fn set_ha_metadata(&self, _: &str, _: &str, _: &str) -> Result<()> {
			Ok(())
		}

		fn create_from_record(&self, _: &Value) -> Result<SandboxRecord> {
			Ok(self.record.clone())
		}

		fn run_from_record(&self, _: &Value) -> Result<Value> {
			Ok(Value::Null)
		}

		fn restore_from_record(&self, _: &Value) -> Result<Value> {
			Ok(Value::Null)
		}

		fn candidate_is_running(&self, _: &str) -> bool {
			self.candidate_running.load(Ordering::SeqCst)
		}
	}

	struct Records;
	impl CreateRecordStore for Records {
		fn get(&self, _: &str) -> Result<Option<CreateRecord>> {
			Ok(Some(CreateRecord {
				sid:             "sandbox".to_owned(),
				params:          Value::Object(Map::new()),
				owner:           "node-a".to_owned(),
				epoch:           6,
				idempotency_key: "restore-test".to_owned(),
				ha:              "async".to_owned(),
				restart_policy:  "none".to_owned(),
			}))
		}

		fn reconcile_records(&self) -> Result<()> {
			Ok(())
		}
	}

	struct Replicas {
		held: bool,
	}
	impl ReplicaStore for Replicas {
		fn get<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<ReplicaRecord>>> {
			Box::pin(async {
				Ok(Some(ReplicaRecord {
					sid:                   "sandbox".to_owned(),
					digest:                "digest".to_owned(),
					object_key:            "replicas/sandbox/digest".to_owned(),
					source_node:           "node-a".to_owned(),
					source_epoch:          6,
					checkpoint_generation: 1,
					snapshot_dir:          PathBuf::from("/checkpoint"),
					params:                Value::Object(Map::new()),
					needs_secrets:         false,
				}))
			})
		}

		fn holds(&self, _: &str) -> Result<bool> {
			Ok(self.held)
		}

		fn secrets_ready(&self, _: &str) -> Result<bool> {
			Ok(self.held)
		}

		fn drop_replica(&self, _: &ReplicaRecord) -> Result<()> {
			Ok(())
		}
	}

	struct SecretReplicas {
		ready: bool,
	}
	impl ReplicaStore for SecretReplicas {
		fn get<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<ReplicaRecord>>> {
			Box::pin(async {
				Ok(Some(ReplicaRecord {
					sid:                   "sandbox".to_owned(),
					digest:                "secret-digest".to_owned(),
					object_key:            "replicas/sandbox/secret-digest".to_owned(),
					source_node:           "node-a".to_owned(),
					source_epoch:          6,
					checkpoint_generation: 1,
					snapshot_dir:          PathBuf::from("/checkpoint"),
					params:                Value::Object(Map::new()),
					needs_secrets:         true,
				}))
			})
		}

		fn holds(&self, _: &str) -> Result<bool> {
			Ok(true)
		}

		fn secrets_ready(&self, _: &str) -> Result<bool> {
			Ok(self.ready)
		}

		fn drop_replica(&self, _: &ReplicaRecord) -> Result<()> {
			Ok(())
		}
	}

	struct Leases {
		acquisitions:     AtomicUsize,
		record_releases:  AtomicUsize,
		granted_releases: AtomicUsize,
	}

	impl LeaseManager for Leases {
		fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>> {
			Box::pin(async { Ok(()) })
		}

		fn acquire_writable_volume_leases<'a>(
			&'a self,
			_: &'a Value,
			_: u64,
		) -> BoxFuture<'a, Result<Vec<LeaseRecord>>> {
			self.acquisitions.fetch_add(1, Ordering::SeqCst);
			Box::pin(async { Ok(vec![]) })
		}

		fn release_leases<'a>(&'a self, _: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>> {
			self.granted_releases.fetch_add(1, Ordering::SeqCst);
			Box::pin(async { Ok(()) })
		}

		fn release_record_volume_leases<'a>(
			&'a self,
			_: &'a SandboxRecord,
		) -> BoxFuture<'a, Result<()>> {
			self.record_releases.fetch_add(1, Ordering::SeqCst);
			Box::pin(async { Ok(()) })
		}
	}

	struct LossMesh {
		renewals: AtomicUsize,
		aborts:   AtomicUsize,
		commits:  AtomicUsize,
	}

	impl MeshReconcileState for LossMesh {
		fn enabled(&self) -> bool {
			true
		}

		fn interval(&self) -> f64 {
			1.0
		}

		fn node_id(&self) -> &'static str {
			"node-b"
		}

		fn advertise(&self) -> String {
			String::new()
		}

		fn create_timeout(&self) -> Duration {
			Duration::from_secs(1)
		}

		fn expected_members(&self) -> usize {
			1
		}

		fn live_member_ids(&self) -> Vec<String> {
			vec![]
		}

		fn peer_url(&self, _: &str) -> Option<String> {
			None
		}

		fn is_peer_healthy(&self, _: &str) -> bool {
			false
		}

		fn owner_of(&self, _: &str) -> Result<Option<String>> {
			Ok(None)
		}

		fn restore_owner(&self, _: &str, _: &HashSet<String>) -> Option<String> {
			None
		}

		fn restore_quorum_met(&self, _: usize) -> bool {
			true
		}

		fn quorum_needed(&self) -> usize {
			1
		}

		fn drain_orphans(&self) -> Vec<(String, String)> {
			vec![]
		}

		fn requeue_orphan(&self, _: &str, _: &str) {}

		fn claim_restore_epoch(&self, _: &str, _: &str) -> Result<u64> {
			Ok(7)
		}

		fn owns_epoch(&self, _: &str, _: &str, _: u64) -> Result<bool> {
			Ok(true)
		}

		fn finalize_restore_epoch(&self, _: &str, _: &str, _: u64) -> Result<()> {
			Ok(())
		}

		fn renew_restore_epoch(&self, _: &str, _: &str, _: u64) -> Result<bool> {
			Ok(self.renewals.fetch_add(1, Ordering::SeqCst) == 0)
		}

		fn abort_restore_epoch(&self, _: &str, _: &str, _: u64, _: &str, _: u64) -> Result<()> {
			self.aborts.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn commit_restore_epoch(
			&self,
			_: &str,
			_: &str,
			_: u64,
			_: &Value,
			_: &str,
			_: &str,
		) -> Result<()> {
			self.commits.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn worker_begin(&self, _: &str) -> bool {
			true
		}

		fn worker_end(&self, _: &str) {}

		fn note_event(&self, _: &str) {}

		fn replica_targets(&self, _: &str, _: usize) -> Vec<String> {
			vec![]
		}

		fn fenced_local_ids(&self, _: &[String]) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn local_owner_epochs(&self, _: &[String]) -> Result<Vec<LocalOwnerEpoch>> {
			Ok(vec![])
		}

		fn renew_owner_leases(&self, _: &[LocalOwnerEpoch]) -> Result<Vec<String>> {
			Ok(vec![])
		}

		fn forget_local_owner(&self, _: &str) {}
	}

	fn test_config() -> ReconcileConfig {
		ReconcileConfig {
			replicate_sec:         1.0,
			replicas:              1,
			replicate_concurrency: 1,
			restore_quorum:        None,
			outbound_token:        String::new(),
		}
	}

	fn test_engine() -> Engine {
		Engine {
			record:              SandboxRecord { name: "sandbox".to_owned(), detail: Map::new() },
			removes:             AtomicUsize::new(0),
			restores:            AtomicUsize::new(0),
			activations:         AtomicUsize::new(0),
			activation_failures: AtomicUsize::new(0),
			lease_records:       AtomicUsize::new(0),
			candidate_running:   AtomicBool::new(false),
		}
	}

	fn test_leases() -> Leases {
		Leases {
			acquisitions:     AtomicUsize::new(0),
			record_releases:  AtomicUsize::new(0),
			granted_releases: AtomicUsize::new(0),
		}
	}

	#[tokio::test]
	async fn secret_ready_holder_bypasses_unready_hrw_winner() {
		let claims = Arc::new(AtomicUsize::new(0));
		let winner = Arc::new(AtomicBool::new(false));
		let records = Records;
		let client = Client::new();
		let unready_mesh =
			SecretMesh { node: "node-b", claims: Arc::clone(&claims), winner: Arc::clone(&winner) };
		let unready_engine = test_engine();
		let unready_replicas = SecretReplicas { ready: false };
		let unready_leases = test_leases();
		Reconciler::new(
			test_config(),
			&unready_mesh,
			&unready_engine,
			&records,
			&unready_replicas,
			&unready_leases,
			&client,
		)
		.reconcile_orphan("sandbox", "node-a")
		.await;
		assert_eq!(claims.load(Ordering::SeqCst), 0, "unready HRW winner must not claim");
		assert_eq!(unready_engine.restores.load(Ordering::SeqCst), 0);

		let ready_mesh = SecretMesh { node: "node-c", claims: Arc::clone(&claims), winner };
		let ready_engine = test_engine();
		let ready_replicas = SecretReplicas { ready: true };
		let ready_leases = test_leases();
		Reconciler::new(
			test_config(),
			&ready_mesh,
			&ready_engine,
			&records,
			&ready_replicas,
			&ready_leases,
			&client,
		)
		.reconcile_orphan("sandbox", "node-a")
		.await;
		assert_eq!(claims.load(Ordering::SeqCst), 1);
		assert_eq!(ready_engine.restores.load(Ordering::SeqCst), 1, "ready holder must restore");
	}

	#[tokio::test]
	async fn two_secret_ready_holders_only_launch_the_cas_winner() {
		let claims = Arc::new(AtomicUsize::new(0));
		let winner = Arc::new(AtomicBool::new(false));
		let records = Records;
		let client = Client::new();
		let first_mesh =
			SecretMesh { node: "node-b", claims: Arc::clone(&claims), winner: Arc::clone(&winner) };
		let first_engine = test_engine();
		let first_replicas = SecretReplicas { ready: true };
		let first_leases = test_leases();
		Reconciler::new(
			test_config(),
			&first_mesh,
			&first_engine,
			&records,
			&first_replicas,
			&first_leases,
			&client,
		)
		.reconcile_orphan("sandbox", "node-a")
		.await;

		let second_mesh = SecretMesh { node: "node-c", claims: Arc::clone(&claims), winner };
		let second_engine = test_engine();
		let second_replicas = SecretReplicas { ready: true };
		let second_leases = test_leases();
		Reconciler::new(
			test_config(),
			&second_mesh,
			&second_engine,
			&records,
			&second_replicas,
			&second_leases,
			&client,
		)
		.reconcile_orphan("sandbox", "node-a")
		.await;
		assert_eq!(claims.load(Ordering::SeqCst), 2, "both ready holders may contend");
		assert_eq!(first_engine.restores.load(Ordering::SeqCst), 1);
		assert_eq!(second_engine.restores.load(Ordering::SeqCst), 0, "fenced loser must not launch");
	}

	/// Drive one lease-renewal cadence against a sandbox the mesh reports as
	/// superseded, returning how many times it was torn down.
	async fn fence_removals_for_superseded_sandbox(migrating: bool) -> usize {
		let mesh = Mesh {
			suspended: false,
			retain_owner: false,
			commit_failures: AtomicUsize::new(0),
			commits: AtomicUsize::new(0),
			fenced: vec!["sandbox".to_owned()],
			migrating,
		};
		let engine = test_engine();
		let records = Records;
		let replicas = Replicas { held: false };
		let leases = test_leases();
		let client = Client::new();
		Reconciler::new(test_config(), &mesh, &engine, &records, &replicas, &leases, &client)
			.renew_owner_leases_once()
			.await
			.unwrap();
		engine.removes.load(Ordering::SeqCst)
	}

	/// A stale owner view is expected mid-handoff: the record moves to the
	/// target before its candidate is activated. Fencing then would delete the
	/// adopted runtime along with the process-local secrets that only exist in
	/// memory, so the migration guard must outrank the fence.
	#[tokio::test]
	async fn in_flight_migration_target_is_never_fenced_by_lease_reconciliation() {
		assert_eq!(
			fence_removals_for_superseded_sandbox(true).await,
			0,
			"an in-flight migration target must survive"
		);
		assert_eq!(
			fence_removals_for_superseded_sandbox(false).await,
			1,
			"a genuinely superseded sandbox must still be fenced"
		);
	}

	#[tokio::test]
	async fn fence_loss_cleanup_removes_vm_and_releases_every_lease() {
		let mesh = Mesh {
			suspended:       false,
			retain_owner:    false,
			commit_failures: AtomicUsize::new(0),
			commits:         AtomicUsize::new(0),
			fenced:          Vec::new(),
			migrating:       false,
		};
		let engine = test_engine();
		let records = Records;
		let replicas = Replicas { held: false };
		let leases = test_leases();
		let client = Client::new();
		let reconciler = Reconciler::new(
			ReconcileConfig {
				replicate_sec:         1.0,
				replicas:              1,
				replicate_concurrency: 1,
				restore_quorum:        None,
				outbound_token:        String::new(),
			},
			&mesh,
			&engine,
			&records,
			&replicas,
			&leases,
			&client,
		);
		let lease = LeaseRecord::new("data", "node-b", 1, 1.0, 1.0).unwrap();

		reconciler
			.cleanup_failed_restore("sandbox", None, &[lease])
			.await;

		assert_eq!(engine.removes.load(Ordering::SeqCst), 1);
		assert_eq!(leases.record_releases.load(Ordering::SeqCst), 1);
		assert_eq!(leases.granted_releases.load(Ordering::SeqCst), 1);
	}

	#[tokio::test]
	async fn suspended_orphan_never_launches_replica_restore() {
		let mesh = Mesh {
			suspended:       true,
			retain_owner:    false,
			commit_failures: AtomicUsize::new(0),
			commits:         AtomicUsize::new(0),
			fenced:          Vec::new(),
			migrating:       false,
		};
		let engine = test_engine();
		let records = Records;
		let replicas = Replicas { held: true };
		let leases = test_leases();
		let client = Client::new();
		let reconciler = Reconciler::new(
			ReconcileConfig {
				replicate_sec:         1.0,
				replicas:              1,
				replicate_concurrency: 1,
				restore_quorum:        None,
				outbound_token:        String::new(),
			},
			&mesh,
			&engine,
			&records,
			&replicas,
			&leases,
			&client,
		);

		reconciler.reconcile_orphan("sandbox", "node-a").await;

		assert_eq!(engine.restores.load(Ordering::SeqCst), 0);
	}

	#[tokio::test]
	async fn ambiguous_commit_reconcile_reuses_retained_candidate_without_relaunching() {
		let mesh = Mesh {
			suspended:       false,
			retain_owner:    true,
			commit_failures: AtomicUsize::new(2),
			commits:         AtomicUsize::new(0),
			fenced:          Vec::new(),
			migrating:       false,
		};
		let engine = test_engine();
		let records = Records;
		let replicas = Replicas { held: false };
		let leases = test_leases();
		let client = Client::new();
		let reconciler =
			Reconciler::new(test_config(), &mesh, &engine, &records, &replicas, &leases, &client);
		let lineage = records.get("sandbox").unwrap().unwrap();

		assert!(
			reconciler
				.restore_from_replica("sandbox", "node-a", Some(&lineage), true)
				.await
				.is_err()
		);
		assert_eq!(engine.restores.load(Ordering::SeqCst), 1);

		assert!(
			!reconciler
				.restore_from_replica("sandbox", "node-a", Some(&lineage), true)
				.await
				.unwrap()
		);
		assert_eq!(engine.restores.load(Ordering::SeqCst), 1);
		assert_eq!(engine.removes.load(Ordering::SeqCst), 0);
		assert_eq!(leases.acquisitions.load(Ordering::SeqCst), 2);
		assert_eq!(engine.lease_records.load(Ordering::SeqCst), 2);
	}

	#[tokio::test]
	async fn activation_failure_aborts_before_commit_and_retries_from_pending() {
		let mesh = Mesh {
			suspended:       false,
			retain_owner:    true,
			commit_failures: AtomicUsize::new(0),
			commits:         AtomicUsize::new(0),
			fenced:          Vec::new(),
			migrating:       false,
		};
		let engine = test_engine();
		engine.activation_failures.store(1, Ordering::SeqCst);
		let records = Records;
		let replicas = Replicas { held: false };
		let leases = test_leases();
		let client = Client::new();
		let reconciler =
			Reconciler::new(test_config(), &mesh, &engine, &records, &replicas, &leases, &client);
		let lineage = records.get("sandbox").unwrap().unwrap();

		assert!(
			reconciler
				.restore_from_replica("sandbox", "node-a", Some(&lineage), true)
				.await
				.is_err()
		);
		assert_eq!(mesh.commits.load(Ordering::SeqCst), 0);
		assert_eq!(engine.removes.load(Ordering::SeqCst), 1);
		assert!(!engine.candidate_running.load(Ordering::SeqCst));

		assert!(
			!reconciler
				.restore_from_replica("sandbox", "node-a", Some(&lineage), true)
				.await
				.unwrap()
		);
		assert_eq!(mesh.commits.load(Ordering::SeqCst), 1);
		assert_eq!(engine.restores.load(Ordering::SeqCst), 2);
		assert_eq!(engine.activations.load(Ordering::SeqCst), 2);
	}

	#[test]
	fn replica_from_a_epoch_one_is_rejected_after_b_epoch_two_is_authoritative() {
		let replica = ReplicaRecord {
			sid:                   "sandbox".to_owned(),
			digest:                "digest-a".to_owned(),
			object_key:            "replicas/sandbox/digest-a".to_owned(),
			source_node:           "node-a".to_owned(),
			source_epoch:          1,
			checkpoint_generation: 9,
			snapshot_dir:          PathBuf::from("/checkpoint"),
			params:                Value::Object(Map::new()),
			needs_secrets:         false,
		};
		let authoritative = CreateRecord {
			sid:             "sandbox".to_owned(),
			params:          Value::Object(Map::new()),
			owner:           "node-b".to_owned(),
			epoch:           2,
			idempotency_key: String::new(),
			ha:              "async".to_owned(),
			restart_policy:  "none".to_owned(),
		};

		assert!(
			!replica_lineage_matches(&replica, &authoritative),
			"claimant C must reject A/1 after B/2 became authoritative"
		);
	}

	#[tokio::test]
	async fn slow_restore_renewal_loss_tears_candidate_down_without_commit() {
		let mesh = LossMesh {
			renewals: AtomicUsize::new(0),
			aborts:   AtomicUsize::new(0),
			commits:  AtomicUsize::new(0),
		};
		let engine = test_engine();
		let records = Records;
		let replicas = Replicas { held: false };
		let leases = test_leases();
		let client = Client::new();
		let reconciler = Reconciler::new(
			ReconcileConfig {
				replicate_sec:         1.0,
				replicas:              1,
				replicate_concurrency: 1,
				restore_quorum:        None,
				outbound_token:        String::new(),
			},
			&mesh,
			&engine,
			&records,
			&replicas,
			&leases,
			&client,
		);
		let lineage = records.get("sandbox").unwrap().unwrap();

		assert!(
			reconciler
				.restore_from_replica("sandbox", "node-a", Some(&lineage), true)
				.await
				.is_err()
		);
		assert_eq!(mesh.commits.load(Ordering::SeqCst), 0);
		assert_eq!(mesh.aborts.load(Ordering::SeqCst), 1);
		assert_eq!(engine.removes.load(Ordering::SeqCst), 1);
		assert_eq!(leases.record_releases.load(Ordering::SeqCst), 1);
	}
}
