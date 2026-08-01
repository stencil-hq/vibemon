use std::{
	collections::{HashMap, HashSet, VecDeque},
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	time::Duration,
};

use axum::{Json, Router, extract::State, routing::post};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use super::{
	gossip::{JsonObject, MeshError, MeshResult},
	reconciler::{
		CreateRecord, CreateRecordStore, LeaseManager as ReconcileLeaseManager, LocalOwnerEpoch,
		MeshReconcileState, ReconcileConfig, ReconcileEngine, Reconciler,
		ReplicaRecord as ReconcileReplicaRecord, ReplicaStore as ReconcileReplicaStore,
		ReplicatePreparation, SandboxRecord,
	},
	replica::ReplicaRecord,
	routes::{
		CreateRecordWire, MeshEngine, MeshLeaseManager, MeshRecordStore, MeshTemplateTransfer,
		MetadataPull, MigratePrepareWire, MigrationCleanupWire, commit_target_migration,
		persist_target_migration_leases,
	},
	runtime::{
		DeleteTombstoneEngine, DeleteTombstoneStore, MeshRuntime, begin_target_migration,
		converge_delete_tombstones, copy_verify_or_rollback, install_replica_cache,
		refresh_replica_metadata,
	},
};
use crate::{EngineError, Result, mesh::lease::LeaseRecord};

fn unsupported<T>() -> MeshResult<T> {
	Err(MeshError::new("unexpected test fake call"))
}

struct EngineFake {
	candidate_present:        AtomicBool,
	adopt_fails:              bool,
	activate_fails:           AtomicBool,
	adopts:                   AtomicUsize,
	activations:              AtomicUsize,
	recorded_leases:          Mutex<Vec<Vec<Value>>>,
	persisted_intents:        Mutex<Vec<MigrationCleanupWire>>,
	record_lease_fails:       AtomicBool,
	teardowns:                Mutex<Vec<(String, i64)>>,
	teardown_fails:           std::sync::atomic::AtomicBool,
	aborts:                   AtomicUsize,
	abort_fails:              AtomicBool,
	cleanup_persist_fails:    AtomicBool,
	events:                   Arc<Mutex<Vec<&'static str>>>,
	portable_history_deletes: Mutex<Vec<(String, String, i64)>>,
	cleanup_removed:          Mutex<Vec<String>>,
	cleanup_remove_calls:     AtomicUsize,
	cleanup_remove_fails:     AtomicBool,
}

impl EngineFake {
	fn new(running: bool, adopt_fails: bool) -> Self {
		Self {
			candidate_present: AtomicBool::new(running),
			adopt_fails,
			activate_fails: AtomicBool::new(false),
			adopts: AtomicUsize::new(0),
			activations: AtomicUsize::new(0),
			recorded_leases: Mutex::new(Vec::new()),
			persisted_intents: Mutex::new(Vec::new()),
			record_lease_fails: AtomicBool::new(false),
			teardowns: Mutex::new(Vec::new()),
			teardown_fails: std::sync::atomic::AtomicBool::new(false),
			aborts: AtomicUsize::new(0),
			abort_fails: AtomicBool::new(false),
			cleanup_persist_fails: AtomicBool::new(false),
			events: Arc::new(Mutex::new(Vec::new())),
			portable_history_deletes: Mutex::new(Vec::new()),
			cleanup_removed: Mutex::new(Vec::new()),
			cleanup_remove_calls: AtomicUsize::new(0),
			cleanup_remove_fails: AtomicBool::new(false),
		}
	}
}
struct TransferFake;

impl MeshTemplateTransfer for TransferFake {
	fn pull_template<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::invalid("unexpected template pull")) })
	}

	fn pull_checkpoint<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::invalid("unexpected checkpoint pull")) })
	}

	fn pull_snapshot<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_sid: String,
		_object_key: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::invalid("unexpected snapshot pull")) })
	}

	fn pull_template_metadata<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>> {
		Box::pin(async { Err(MeshError::invalid("unexpected template metadata pull")) })
	}
}

impl MeshEngine for EngineFake {
	fn owned_ids(&self) -> MeshResult<Vec<String>> {
		unsupported()
	}

	fn list_views(&self) -> MeshResult<Vec<Value>> {
		unsupported()
	}

	fn has_sandbox(&self, _sid: &str) -> bool {
		self.candidate_present.load(Ordering::SeqCst)
	}

	fn get_view(&self, _sid: &str) -> MeshResult<Value> {
		unsupported()
	}

	fn checkpoint_age_sec(&self, _sid: &str) -> Option<f64> {
		None
	}

	fn find_by_idempotency_key(&self, _key: &str) -> MeshResult<Option<Value>> {
		unsupported()
	}

	fn record_idempotency(&self, _sid: &str, _key: &str) -> MeshResult<()> {
		unsupported()
	}

	fn record_volume_leases(&self, _sid: &str, leases: Vec<Value>) -> MeshResult<()> {
		self.events.lock().push("persist_leases");
		if self.record_lease_fails.load(Ordering::SeqCst) {
			return Err(MeshError::new("persisting leases failed"));
		}
		self.recorded_leases.lock().push(leases);
		Ok(())
	}

	fn stage_migration_delta(
		&self,
		_sid: String,
		verified_cache_path: String,
		_base_dir: String,
	) -> BoxFuture<'_, MeshResult<String>> {
		Box::pin(async move { Ok(verified_cache_path) })
	}

	fn create_sandbox(&self, _params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		Box::pin(async { unsupported() })
	}

	fn run_detached(&self, _params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		Box::pin(async { unsupported() })
	}

	fn restore_detached(&self, _params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		Box::pin(async { unsupported() })
	}

	fn restore_from_template(
		&self,
		_params: JsonObject,
		_template_dir: String,
		_quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>> {
		Box::pin(async { unsupported() })
	}

	fn migrate_precopy(&self, _sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		Box::pin(async { unsupported() })
	}

	fn migrate_finalize(
		&self,
		_sid: String,
		_base_dir: String,
		_cleanup: MigrationCleanupWire,
	) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		Box::pin(async { unsupported() })
	}

	fn checkpoint_discard(
		&self,
		_snapshot_dir: String,
		_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { unsupported() })
	}

	fn migrate_abort(
		&self,
		_sid: String,
		_base_digest: String,
		_delta_dir: String,
		_delta_digest: String,
		_params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>> {
		self.events.lock().push("abort");
		self.aborts.fetch_add(1, Ordering::SeqCst);
		let fails = self.abort_fails.load(Ordering::SeqCst);
		Box::pin(async move {
			if fails {
				Err(MeshError::new("abort failed"))
			} else {
				Ok(())
			}
		})
	}

	fn migrate_commit(
		&self,
		_sid: String,
		_base_dir: String,
		_base_digest: String,
		_delta_dir: String,
		_delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { unsupported() })
	}

	fn migrate_adopt_target(
		&self,
		_sid: String,
		_delta_dir: String,
		_params: JsonObject,
	) -> BoxFuture<'_, MeshResult<Value>> {
		self.events.lock().push("adopt_paused");
		self.adopts.fetch_add(1, Ordering::SeqCst);
		self.candidate_present.store(true, Ordering::SeqCst);
		let fails = self.adopt_fails;
		Box::pin(async move {
			if fails {
				Err(MeshError::new("adopt failed"))
			} else {
				Ok(json!({}))
			}
		})
	}

	fn migrate_activate_target(
		&self,
		_sid: String,
		_expected_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		self.events.lock().push("activate");
		self.activations.fetch_add(1, Ordering::SeqCst);
		let fails = self.activate_fails.load(Ordering::SeqCst);
		Box::pin(async move {
			if fails {
				Err(MeshError::new("activation failed"))
			} else {
				Ok(())
			}
		})
	}

	fn teardown_candidate(&self, sid: String, expected_epoch: i64) -> BoxFuture<'_, MeshResult<()>> {
		self.candidate_present.store(false, Ordering::SeqCst);
		self.teardowns.lock().push((sid, expected_epoch));
		self.events.lock().push("teardown");
		let fails = self.teardown_fails.load(Ordering::SeqCst);
		Box::pin(async move {
			if fails {
				Err(MeshError::new("teardown failed"))
			} else {
				Ok(())
			}
		})
	}

	fn delete_portable_history(
		&self,
		sid: String,
		owner: String,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		self
			.portable_history_deletes
			.lock()
			.push((sid, owner, epoch));
		Box::pin(async { Ok(()) })
	}

	fn migration_cleanup_persist(&self, cleanup: MigrationCleanupWire) -> MeshResult<()> {
		self.events.lock().push("journal");
		if self.cleanup_persist_fails.load(Ordering::SeqCst) {
			return Err(MeshError::new("journal persist failed"));
		}
		self.persisted_intents.lock().push(cleanup);
		Ok(())
	}

	fn migration_cleanup_remove(&self, sid: String) -> MeshResult<()> {
		self.events.lock().push("cleanup_remove");
		self.cleanup_remove_calls.fetch_add(1, Ordering::SeqCst);
		if self.cleanup_remove_fails.load(Ordering::SeqCst) {
			return Err(MeshError::new("journal remove failed"));
		}
		self.cleanup_removed.lock().push(sid);
		Ok(())
	}
}

struct LeaseFake {
	acquired: Vec<Value>,
	released: Mutex<Vec<Vec<Value>>>,
	events:   Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl MeshLeaseManager for LeaseFake {
	fn vote_grant(
		&self,
		_volume: String,
		_holder_node: String,
		_epoch: i64,
		_ttl: f64,
	) -> MeshResult<JsonObject> {
		unsupported()
	}

	fn vote_renew(
		&self,
		_volume: String,
		_holder_node: String,
		_epoch: i64,
		_ttl: f64,
	) -> MeshResult<JsonObject> {
		unsupported()
	}

	fn vote_release(
		&self,
		_volume: String,
		_holder_node: String,
		_epoch: i64,
	) -> MeshResult<JsonObject> {
		unsupported()
	}

	fn acquire_writable_volume_leases(
		&self,
		_params: JsonObject,
		_epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>> {
		let acquired = self.acquired.clone();
		Box::pin(async move { Ok(acquired) })
	}

	fn release_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>> {
		if let Some(events) = &self.events {
			events.lock().push("release");
		}
		self.released.lock().push(leases);
		Box::pin(async { Ok(()) })
	}

	fn release_record_volume_leases(&self, _sid: String) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { unsupported() })
	}
}

#[derive(Default)]
struct RecordFake(Mutex<Vec<CreateRecordWire>>);

impl MeshRecordStore for RecordFake {
	fn get(&self, _sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		unsupported()
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		self.0.lock().push(record.clone());
		Ok(record)
	}

	fn commit_delete(&self, _sid: &str, _owner: &str, _epoch: i64) -> MeshResult<()> {
		unsupported()
	}

	fn list(&self) -> MeshResult<Vec<CreateRecordWire>> {
		Ok(self.0.lock().clone())
	}

	fn notify_source_migration_claim<'a>(
		&'a self,
		_record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}
}

struct AbortRecordFake {
	current:        Mutex<CreateRecordWire>,
	fresh:          CreateRecordWire,
	abort_calls:    AtomicUsize,
	cas_updates:    AtomicUsize,
	complete_calls: AtomicUsize,
	events:         Arc<Mutex<Vec<&'static str>>>,
}

impl MeshRecordStore for AbortRecordFake {
	fn get(&self, _sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		Ok(Some(self.current.lock().clone()))
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		*self.current.lock() = record.clone();
		Ok(record)
	}

	fn commit_delete(&self, _sid: &str, _owner: &str, _epoch: i64) -> MeshResult<()> {
		unsupported()
	}

	fn list(&self) -> MeshResult<Vec<CreateRecordWire>> {
		Ok(vec![self.current.lock().clone()])
	}

	fn abort_migration_handoff(
		&self,
		_sid: &str,
		_source_owner: &str,
		_source_epoch: i64,
		_target_owner: &str,
		_token: &str,
	) -> MeshResult<Option<CreateRecordWire>> {
		let call = self.abort_calls.fetch_add(1, Ordering::SeqCst);
		if call == 0 {
			self.events.lock().push("cas_abort");
			self.cas_updates.fetch_add(1, Ordering::SeqCst);
			*self.current.lock() = self.fresh.clone();
		} else {
			self.events.lock().push("aborted_disposition");
		}
		Ok(Some(self.fresh.clone()))
	}

	fn complete_migration_abort(
		&self,
		_sid: &str,
		_token: &str,
		_owner: &str,
		_fresh_epoch: i64,
	) -> MeshResult<CreateRecordWire> {
		self.events.lock().push("complete_abort");
		self.complete_calls.fetch_add(1, Ordering::SeqCst);
		Ok(self.fresh.clone())
	}

	fn notify_source_migration_claim<'a>(
		&'a self,
		_record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>> {
		Box::pin(async { unsupported() })
	}
}

struct ExactRecordFake {
	record:      Mutex<Option<CreateRecordWire>>,
	fail_update: bool,
	fail_notify: bool,
	events:      Arc<Mutex<Vec<&'static str>>>,
}

impl MeshRecordStore for ExactRecordFake {
	fn get(&self, _sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		Ok(self.record.lock().clone())
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		*self.record.lock() = Some(record.clone());
		Ok(record)
	}

	fn commit_delete(&self, _sid: &str, _owner: &str, _epoch: i64) -> MeshResult<()> {
		unsupported()
	}

	fn list(&self) -> MeshResult<Vec<CreateRecordWire>> {
		Ok(self.record.lock().clone().into_iter().collect())
	}

	fn update_exact(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		self.events.lock().push("durable_commit");
		if self.fail_update {
			Err(MeshError::new("marker CAS failed"))
		} else {
			*self.record.lock() = Some(record.clone());
			Ok(record)
		}
	}

	fn claim_expected_with_intent(
		&self,
		_expected_owner: String,
		_expected_epoch: i64,
		record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		self.events.lock().push("intent");
		*self.record.lock() = Some(record.clone());
		Ok(record)
	}

	fn notify_source_migration_claim<'a>(
		&'a self,
		_record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>> {
		self.events.lock().push("confirm_source");
		let fail = self.fail_notify;
		Box::pin(async move {
			if fail {
				Err(MeshError::unreachable("source unavailable"))
			} else {
				Ok(())
			}
		})
	}
}

fn pending_record(delta_dir: &std::path::Path) -> CreateRecordWire {
	let mut params = Map::new();
	params.insert("volume".to_owned(), json!("data"));

	params.insert("_mesh_migration_delta_dir".to_owned(), json!(delta_dir));
	params.insert("_mesh_migration_id".to_owned(), json!("migration-1"));
	CreateRecordWire {
		sid: "sandbox-1".to_owned(),
		params,
		owner: "target".to_owned(),
		epoch: 7,
		idempotency_key: "exact-token".to_owned(),
		ha: "mesh".to_owned(),
		restart_policy: "always".to_owned(),
		created_at: 0.0,
	}
}

#[test]
fn shared_target_guard_fences_replay_until_the_receive_finishes() {
	let active = Arc::new(Mutex::new(HashSet::new()));
	let receive = begin_target_migration(&active, "sandbox-1").unwrap();
	assert!(begin_target_migration(&active, "sandbox-1").is_err());
	drop(receive);
	assert!(begin_target_migration(&active, "sandbox-1").is_ok());
}

fn write_checkpoint(dir: &std::path::Path, memory: &str) {
	std::fs::create_dir_all(dir).unwrap();
	std::fs::write(dir.join("agent-ready.json"), "{}").unwrap();
	std::fs::write(dir.join("memory.bin"), memory).unwrap();
}

#[tokio::test]
async fn releases_exact_acquired_lease_when_target_adoption_fails() {
	let delta = tempfile::tempdir().unwrap();
	let exact = vec![json!({"volume": "data", "epoch": 7, "token": "exact"})];
	let engine = EngineFake::new(false, true);
	let leases =
		LeaseFake { acquired: exact.clone(), released: Mutex::new(Vec::new()), events: None };
	let pending = pending_record(delta.path());
	let records = RecordFake(Mutex::new(vec![pending.clone()]));
	let transfer = TransferFake;
	let client = reqwest::Client::new();

	assert!(
		MeshRuntime::converge_migration_intent(
			&engine, &leases, &records, &transfer, &client, "token", pending,
		)
		.await
		.is_err()
	);
	assert_eq!(*leases.released.lock(), vec![exact]);
	assert_eq!(*engine.teardowns.lock(), vec![("sandbox-1".to_owned(), 7)]);
	let persisted = records.0.lock();
	assert_eq!(persisted.len(), 1);
	assert_eq!(persisted[0].params.get("_mesh_migration_committed"), None);
}

#[tokio::test]
async fn pending_exact_token_with_running_vm_commits_without_second_adopt() {
	let delta = tempfile::tempdir().unwrap();
	let exact = vec![json!({"volume": "data", "epoch": 7, "token": "exact"})];
	let engine = EngineFake::new(true, false);
	let leases =
		LeaseFake { acquired: exact.clone(), released: Mutex::new(Vec::new()), events: None };
	let records = RecordFake::default();
	let transfer = TransferFake;
	let client = reqwest::Client::new();

	MeshRuntime::converge_migration_intent(
		&engine,
		&leases,
		&records,
		&transfer,
		&client,
		"token",
		pending_record(delta.path()),
	)
	.await
	.unwrap();
	assert_eq!(engine.adopts.load(Ordering::SeqCst), 0);
	assert_eq!(*engine.recorded_leases.lock(), vec![exact]);
	let committed = records.0.lock();
	assert_eq!(committed.len(), 1);
	assert_eq!(committed[0].params.get("_mesh_migration_committed"), Some(&Value::Bool(true)));
}

#[tokio::test]
async fn migration_target_activates_only_after_leases_and_durable_commit() {
	let delta = tempfile::tempdir().unwrap();
	let engine = EngineFake::new(false, false);
	let records = ExactRecordFake {
		record:      Mutex::new(Some(pending_record(delta.path()))),
		fail_update: false,
		fail_notify: false,
		events:      engine.events.clone(),
	};
	let leases = LeaseFake {
		acquired: vec![json!({"volume": "data", "epoch": 7, "token": "exact"})],
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let transfer = TransferFake;
	let client = reqwest::Client::new();

	MeshRuntime::converge_migration_intent(
		&engine,
		&leases,
		&records,
		&transfer,
		&client,
		"token",
		pending_record(delta.path()),
	)
	.await
	.unwrap();

	assert_eq!(*engine.events.lock(), vec![
		"confirm_source",
		"adopt_paused",
		"persist_leases",
		"durable_commit",
		"activate"
	]);
}

#[tokio::test]
async fn source_confirmation_failure_keeps_target_inactive() {
	let delta = tempfile::tempdir().unwrap();
	let engine = EngineFake::new(false, false);
	let records = ExactRecordFake {
		record:      Mutex::new(Some(pending_record(delta.path()))),
		fail_update: false,
		fail_notify: true,
		events:      engine.events.clone(),
	};
	let leases = LeaseFake {
		acquired: Vec::new(),
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let transfer = TransferFake;
	let client = reqwest::Client::new();

	assert!(
		MeshRuntime::converge_migration_intent(
			&engine,
			&leases,
			&records,
			&transfer,
			&client,
			"token",
			pending_record(delta.path()),
		)
		.await
		.is_err()
	);
	assert_eq!(*engine.events.lock(), vec!["confirm_source"]);
	assert_eq!(engine.adopts.load(Ordering::SeqCst), 0);
	assert_eq!(engine.activations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn migration_target_commit_failure_never_activates_and_releases_paused_candidate() {
	let delta = tempfile::tempdir().unwrap();
	let engine = EngineFake::new(false, false);
	let records = ExactRecordFake {
		record:      Mutex::new(Some(pending_record(delta.path()))),
		fail_update: true,
		fail_notify: false,
		events:      engine.events.clone(),
	};
	let exact = vec![json!({"volume": "data", "epoch": 7, "token": "exact"})];
	let leases = LeaseFake {
		acquired: exact.clone(),
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let transfer = TransferFake;
	let client = reqwest::Client::new();

	assert!(
		MeshRuntime::converge_migration_intent(
			&engine,
			&leases,
			&records,
			&transfer,
			&client,
			"token",
			pending_record(delta.path()),
		)
		.await
		.is_err()
	);

	assert_eq!(*leases.released.lock(), vec![exact]);
	assert_eq!(*engine.events.lock(), vec![
		"confirm_source",
		"adopt_paused",
		"persist_leases",
		"durable_commit",
		"teardown",
		"release"
	]);
}

#[tokio::test]
async fn committed_migration_retries_activation_without_rolling_back_ownership() {
	let delta = tempfile::tempdir().unwrap();
	let engine = EngineFake::new(false, false);
	engine.activate_fails.store(true, Ordering::SeqCst);
	let records = ExactRecordFake {
		record:      Mutex::new(Some(pending_record(delta.path()))),
		fail_update: false,
		fail_notify: false,
		events:      engine.events.clone(),
	};
	let leases = LeaseFake {
		acquired: vec![json!({"volume": "data", "epoch": 7, "token": "exact"})],
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let transfer = TransferFake;
	let client = reqwest::Client::new();
	let pending = pending_record(delta.path());

	assert!(
		MeshRuntime::converge_migration_intent(
			&engine, &leases, &records, &transfer, &client, "token", pending,
		)
		.await
		.is_err()
	);
	let committed = records.get("sandbox-1").unwrap().unwrap();
	assert_eq!(committed.params.get("_mesh_migration_committed"), Some(&Value::Bool(true)));
	assert!(engine.teardowns.lock().is_empty());
	assert!(leases.released.lock().is_empty());

	engine.activate_fails.store(false, Ordering::SeqCst);
	MeshRuntime::converge_migration_intent(
		&engine, &leases, &records, &transfer, &client, "token", committed,
	)
	.await
	.unwrap();

	assert_eq!(engine.activations.load(Ordering::SeqCst), 2);
	assert_eq!(*engine.events.lock(), vec![
		"confirm_source",
		"adopt_paused",
		"persist_leases",
		"durable_commit",
		"activate",
		"confirm_source",
		"activate"
	]);
}

#[tokio::test]
async fn source_migration_intent_exists_before_receive_or_status_resolution() {
	let engine = EngineFake::new(false, false);
	let intent = MigrationCleanupWire {
		sid:            "sandbox-1".to_owned(),
		base_dir:       "/base".to_owned(),
		base_digest:    "base-digest".to_owned(),
		delta_dir:      "/delta".to_owned(),
		delta_digest:   "delta-digest".to_owned(),
		target_url:     "http://target".to_owned(),
		migration_id:   "migration-1".to_owned(),
		epoch:          7,
		schema_version: 1,
		source_owner:   "source".to_owned(),
		source_epoch:   6,
		target_owner:   "target".to_owned(),
		target_epoch:   7,
		token:          "exact-token".to_owned(),
		phase:          "prepared".to_owned(),
	};

	let resolved = MeshRuntime::persist_before_migration_receive(&engine, intent, || async {
		let intents = engine.persisted_intents.lock();
		assert_eq!(intents.len(), 1);
		assert_eq!(intents[0].migration_id, "migration-1");
		Ok(json!({"committed": true}))
	})
	.await
	.unwrap();
	assert_eq!(resolved, json!({"committed": true}));
}

#[tokio::test]
async fn failed_final_replica_verification_rolls_back_copied_object() {
	let copied = Arc::new(AtomicBool::new(false));
	let rolled_back = Arc::new(AtomicBool::new(false));
	let copy_state = Arc::clone(&copied);
	let verify_state = Arc::clone(&copied);
	let rollback_state = Arc::clone(&rolled_back);

	let error = copy_verify_or_rollback(
		"replicas/final",
		async move {
			copy_state.store(true, Ordering::SeqCst);
			Ok(())
		},
		async move {
			assert!(verify_state.load(Ordering::SeqCst));
			Err(EngineError::invalid("final digest mismatch"))
		},
		async move {
			rollback_state.store(true, Ordering::SeqCst);
			Ok(())
		},
	)
	.await
	.unwrap_err();

	assert_eq!(error.message, "final digest mismatch");
	assert!(rolled_back.load(Ordering::SeqCst));
}

#[test]
fn corrupt_replica_cache_is_rejected_removed_and_replaced_with_verified_content() {
	let temp = tempfile::tempdir().unwrap();
	let cache = temp.path().join("cache");
	let snapshot = "checkpoint";
	write_checkpoint(&cache.join(snapshot), "corrupt");
	let source = tempfile::tempdir().unwrap();
	write_checkpoint(source.path(), "verified");
	let digest = crate::image::cas::snapshot_digest(source.path()).unwrap();

	let installed = install_replica_cache(&cache, snapshot, &digest, |staging| {
		std::fs::create_dir_all(staging).unwrap();
		std::fs::rename(source.path(), staging.join(snapshot)).unwrap();
		Ok(())
	})
	.unwrap();
	assert_eq!(crate::image::cas::snapshot_digest(&installed).unwrap(), digest);
	assert_eq!(std::fs::read_to_string(installed.join("memory.bin")).unwrap(), "verified");
}

#[test]
fn interrupted_partial_replica_extraction_is_never_trusted_and_retry_installs_cache() {
	let temp = tempfile::tempdir().unwrap();
	let cache = temp.path().join("cache");
	let snapshot = "checkpoint";
	let source = tempfile::tempdir().unwrap();
	write_checkpoint(source.path(), "verified");
	let digest = crate::image::cas::snapshot_digest(source.path()).unwrap();

	assert!(
		install_replica_cache(&cache, snapshot, &digest, |staging| {
			write_checkpoint(&staging.join(snapshot), "partial");
			Err(crate::EngineError::engine("interrupted extraction"))
		})
		.is_err()
	);
	assert!(!cache.exists());
	let installed = install_replica_cache(&cache, snapshot, &digest, |staging| {
		std::fs::create_dir_all(staging).unwrap();
		std::fs::rename(source.path(), staging.join(snapshot)).unwrap();
		Ok(())
	})
	.unwrap();
	assert_eq!(crate::image::cas::snapshot_digest(&installed).unwrap(), digest);
}

#[test]
fn verified_replica_bytes_reuse_new_authoritative_metadata_without_stale_secrets() {
	let mut authoritative_params = Map::new();
	authoritative_params.insert("lineage".to_owned(), json!("new"));
	authoritative_params.insert("secrets".to_owned(), json!({"token": "authoritative-secret"}));
	let shared = ReplicaRecord::new_fenced(
		"sandbox-1",
		"digest-1",
		"source-b",
		12,
		34,
		"/authoritative/checkpoint",
		authoritative_params.clone(),
		true,
	)
	.unwrap();
	let mut current_secrets = Map::new();
	current_secrets.insert("secrets".to_owned(), json!({"token": "resolved-secret"}));

	let refreshed = refresh_replica_metadata(
		shared.clone(),
		Some("/cache/verified-checkpoint"),
		Some(&current_secrets),
	);
	assert_eq!(refreshed.snapshot_dir, "/cache/verified-checkpoint");
	assert_eq!(refreshed.source_node, "source-b");
	assert_eq!(refreshed.source_epoch, 12);
	assert_eq!(refreshed.checkpoint_generation, 34);
	let mut expected_params = authoritative_params.clone();
	expected_params.insert("secrets".to_owned(), json!({"token": "resolved-secret"}));
	assert_eq!(refreshed.params, expected_params);

	let unresolved = refresh_replica_metadata(shared, Some("/cache/verified-checkpoint"), None);
	assert_eq!(unresolved.snapshot_dir, "/cache/verified-checkpoint");
	assert_eq!(unresolved.params, authoritative_params);
	assert_ne!(unresolved.params.get("secrets"), Some(&json!({"token": "resolved-secret"})));
}

struct CadenceMesh {
	peer_url: String,
}

impl MeshReconcileState for CadenceMesh {
	fn enabled(&self) -> bool {
		true
	}

	fn interval(&self) -> f64 {
		1.0
	}

	fn node_id(&self) -> &'static str {
		"node-a"
	}

	fn advertise(&self) -> String {
		"http://node-a".to_owned()
	}

	fn create_timeout(&self) -> Duration {
		Duration::from_secs(1)
	}

	fn expected_members(&self) -> usize {
		2
	}

	fn live_member_ids(&self) -> Vec<String> {
		vec!["node-b".to_owned()]
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		(node_id == "node-b").then(|| self.peer_url.clone())
	}

	fn is_peer_healthy(&self, _node_id: &str) -> bool {
		true
	}

	fn owner_of(&self, _sid: &str) -> Result<Option<String>> {
		Ok(None)
	}

	fn restore_owner(&self, _sid: &str, _exclude: &HashSet<String>) -> Option<String> {
		None
	}

	fn restore_quorum_met(&self, _confirmations: usize) -> bool {
		true
	}

	fn quorum_needed(&self) -> usize {
		1
	}

	fn drain_orphans(&self) -> Vec<(String, String)> {
		Vec::new()
	}

	fn requeue_orphan(&self, _sid: &str, _dead: &str) {}

	fn claim_restore_epoch(&self, _sid: &str, _owner: &str) -> Result<u64> {
		Ok(1)
	}

	fn owns_epoch(&self, _sid: &str, _owner: &str, _epoch: u64) -> Result<bool> {
		Ok(true)
	}

	fn finalize_restore_epoch(&self, _sid: &str, _owner: &str, _epoch: u64) -> Result<()> {
		Ok(())
	}

	fn worker_begin(&self, _key: &str) -> bool {
		true
	}

	fn worker_end(&self, _key: &str) {}

	fn note_event(&self, _event: &str) {}

	fn replica_targets(&self, _sid: &str, _replicas: usize) -> Vec<String> {
		vec!["node-b".to_owned()]
	}

	fn fenced_local_ids(&self, _owned_ids: &[String]) -> Result<Vec<String>> {
		Ok(Vec::new())
	}

	fn local_owner_epochs(&self, _owned_ids: &[String]) -> Result<Vec<LocalOwnerEpoch>> {
		Ok(Vec::new())
	}

	fn renew_owner_leases(&self, _local: &[LocalOwnerEpoch]) -> Result<Vec<String>> {
		Ok(Vec::new())
	}

	fn forget_local_owner(&self, _sid: &str) {}
}

struct CadenceEngine(Mutex<VecDeque<ReplicatePreparation>>);

impl ReconcileEngine for CadenceEngine {
	fn owned_ids(&self) -> Result<Vec<String>> {
		Ok(vec!["sandbox-1".to_owned()])
	}

	fn get(&self, _sid: &str) -> Result<SandboxRecord> {
		Err(EngineError::engine("unused"))
	}

	fn remove(&self, _sid: &str) -> Result<()> {
		Err(EngineError::engine("unused"))
	}

	fn replicate_prepare<'a>(
		&'a self,
		_sid: &'a str,
	) -> BoxFuture<'a, Result<ReplicatePreparation>> {
		let prepared = self
			.0
			.lock()
			.pop_front()
			.ok_or_else(|| EngineError::engine("missing preparation"));
		Box::pin(async move { prepared })
	}

	fn replicate_cleanup(&self, _digest: &str, _snapshot_dir: &Path) -> Result<()> {
		Ok(())
	}

	fn restore_from_template<'a>(
		&'a self,
		_params: &'a Value,
		_snapshot_dir: &'a Path,
		_quorum_ok: bool,
	) -> BoxFuture<'a, Result<SandboxRecord>> {
		Box::pin(async { Err(EngineError::engine("unused")) })
	}

	fn activate_candidate<'a>(&'a self, _sid: &'a str) -> BoxFuture<'a, Result<()>> {
		Box::pin(async { Err(EngineError::engine("unused")) })
	}

	fn record_volume_leases(&self, _sid: &str, _leases: &[LeaseRecord]) -> Result<()> {
		Ok(())
	}

	fn record_idempotency(&self, _sid: &str, _key: &str) -> Result<()> {
		Ok(())
	}

	fn set_ha_metadata(&self, _sid: &str, _ha: &str, _restart_policy: &str) -> Result<()> {
		Ok(())
	}

	fn create_from_record(&self, _params: &Value) -> Result<SandboxRecord> {
		Err(EngineError::engine("unused"))
	}

	fn run_from_record(&self, _params: &Value) -> Result<Value> {
		Err(EngineError::engine("unused"))
	}

	fn restore_from_record(&self, _params: &Value) -> Result<Value> {
		Err(EngineError::engine("unused"))
	}
}

struct CadenceRecords;
impl CreateRecordStore for CadenceRecords {
	fn get(&self, _sid: &str) -> Result<Option<CreateRecord>> {
		Ok(Some(CreateRecord {
			sid:             "sandbox-1".to_owned(),
			params:          json!({}),
			owner:           "node-a".to_owned(),
			epoch:           9,
			idempotency_key: String::new(),
			ha:              "async".to_owned(),
			restart_policy:  "none".to_owned(),
		}))
	}

	fn reconcile_records(&self) -> Result<()> {
		Ok(())
	}
}

struct CadenceReplicas;
impl ReconcileReplicaStore for CadenceReplicas {
	fn get<'a>(&'a self, _sid: &'a str) -> BoxFuture<'a, Result<Option<ReconcileReplicaRecord>>> {
		Box::pin(async { Ok(None) })
	}

	fn holds(&self, _sid: &str) -> Result<bool> {
		Ok(false)
	}

	fn secrets_ready(&self, _sid: &str) -> Result<bool> {
		Ok(false)
	}

	fn drop_replica(&self, _record: &ReconcileReplicaRecord) -> Result<()> {
		Ok(())
	}
}

struct CadenceLeases;
impl ReconcileLeaseManager for CadenceLeases {
	fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn acquire_writable_volume_leases<'a>(
		&'a self,
		_params: &'a Value,
		_epoch: u64,
	) -> BoxFuture<'a, Result<Vec<LeaseRecord>>> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn release_leases<'a>(&'a self, _leases: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn release_record_volume_leases<'a>(
		&'a self,
		_record: &'a SandboxRecord,
	) -> BoxFuture<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}
}

async fn capture_replica_publish(
	State(received): State<Arc<Mutex<Vec<Value>>>>,
	Json(body): Json<Value>,
) {
	received.lock().push(body);
}

#[tokio::test]
async fn newer_replication_generation_with_same_digest_publishes_updated_metadata() {
	let received = Arc::new(Mutex::new(Vec::new()));
	let app = Router::new()
		.route("/v1/mesh/replica/receive", post(capture_replica_publish))
		.with_state(received.clone());
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
	let mesh = CadenceMesh { peer_url: format!("http://{address}") };
	let engine = CadenceEngine(Mutex::new(VecDeque::from([
		ReplicatePreparation {
			digest:                "same-digest".to_owned(),
			checkpoint_generation: 1,
			snapshot_dir:          PathBuf::from("/checkpoint/one"),
			params:                json!({"revision": 1}),
		},
		ReplicatePreparation {
			digest:                "same-digest".to_owned(),
			checkpoint_generation: 2,
			snapshot_dir:          PathBuf::from("/checkpoint/two"),
			params:                json!({"revision": 2}),
		},
	])));
	let records = CadenceRecords;
	let replicas = CadenceReplicas;
	let leases = CadenceLeases;
	let client = reqwest::Client::new();
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
	let mut last = HashMap::new();
	reconciler.replicate_once(&mut last).await.unwrap();
	reconciler.reset_replication_cadence_for_test("sandbox-1");
	reconciler.replicate_once(&mut last).await.unwrap();
	server.abort();

	let published = received.lock();
	assert_eq!(published.len(), 2);
	assert_eq!(published[0]["digest"], "same-digest");
	assert_eq!(published[1]["digest"], "same-digest");
	assert_eq!(published[1]["checkpoint_generation"], 2);
	assert_eq!(published[1]["params"]["revision"], 2);
}

#[tokio::test]
async fn target_migration_persists_intent_then_exact_commit_marker_before_ack() {
	let delta = tempfile::tempdir().unwrap();
	let events = Arc::new(Mutex::new(Vec::new()));
	let engine = EngineFake::new(false, false);
	let records = ExactRecordFake {
		record:      Mutex::new(None),
		fail_update: false,
		fail_notify: false,
		events:      events.clone(),
	};
	let leases = LeaseFake {
		acquired: Vec::new(),
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let mut record = pending_record(delta.path());
	records
		.claim_expected_with_intent("source".to_owned(), 6, record.clone())
		.unwrap();
	assert_eq!(
		records
			.get(&record.sid)
			.unwrap()
			.unwrap()
			.params
			.get("_mesh_migration_committed"),
		None
	);

	commit_target_migration(
		&records,
		&engine,
		&leases,
		record.sid.clone(),
		record.epoch,
		&mut record,
		vec![json!({"volume": "data", "token": "exact"})],
	)
	.await
	.unwrap();

	let committed = records.get(&record.sid).unwrap().unwrap();
	assert_eq!(committed.owner, "target");
	assert_eq!(committed.epoch, 7);
	assert_eq!(committed.idempotency_key, "exact-token");
	assert_eq!(committed.params.get("_mesh_migration_committed"), Some(&Value::Bool(true)));
	assert_eq!(*events.lock(), vec!["intent", "durable_commit"]);
}

#[tokio::test]
async fn marker_cas_failure_tears_target_down_before_releasing_leases_without_false_commit() {
	let delta = tempfile::tempdir().unwrap();
	let engine = EngineFake::new(false, false);
	let records = ExactRecordFake {
		record:      Mutex::new(None),
		fail_update: true,
		fail_notify: false,
		events:      engine.events.clone(),
	};
	let exact = vec![json!({"volume": "data", "token": "exact"})];
	let leases = LeaseFake {
		acquired: exact.clone(),
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};
	let mut record = pending_record(delta.path());
	records
		.claim_expected_with_intent("source".to_owned(), 6, record.clone())
		.unwrap();

	assert!(
		commit_target_migration(
			&records,
			&engine,
			&leases,
			record.sid.clone(),
			record.epoch,
			&mut record,
			exact.clone(),
		)
		.await
		.is_err()
	);
	assert_eq!(*engine.teardowns.lock(), vec![("sandbox-1".to_owned(), 7)]);
	assert_eq!(*leases.released.lock(), vec![exact]);
	assert_eq!(*engine.events.lock(), vec!["intent", "durable_commit", "teardown", "release"]);
	let persisted = records.get("sandbox-1").unwrap().unwrap();
	assert_eq!(persisted.params.get("_mesh_migration_committed"), None);
}

#[tokio::test]
async fn failed_target_lease_persistence_tears_down_and_releases_without_committing() {
	let engine = EngineFake::new(false, false);
	engine.record_lease_fails.store(true, Ordering::SeqCst);
	let exact = vec![json!({"volume": "data", "token": "exact"})];
	let leases = LeaseFake {
		acquired: exact.clone(),
		released: Mutex::new(Vec::new()),
		events:   Some(engine.events.clone()),
	};

	let error =
		persist_target_migration_leases(&engine, &leases, "sandbox-1".to_owned(), 7, exact.clone())
			.await
			.unwrap_err();

	assert_eq!(error.message, "persisting leases failed");
	assert!(engine.recorded_leases.lock().is_empty());
	assert_eq!(*engine.teardowns.lock(), vec![("sandbox-1".to_owned(), 7)]);
	assert_eq!(*leases.released.lock(), vec![exact]);
	assert_eq!(*engine.events.lock(), vec!["persist_leases", "teardown", "release"]);
}

struct TombstoneStore {
	records: Mutex<Vec<CreateRecordWire>>,
	events:  Arc<Mutex<Vec<&'static str>>>,
}

impl DeleteTombstoneStore for TombstoneStore {
	fn list_deleting(&self) -> MeshResult<Vec<CreateRecordWire>> {
		Ok(self.records.lock().clone())
	}

	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> MeshResult<()> {
		self.events.lock().push("commit");
		self
			.records
			.lock()
			.retain(|record| record.sid != sid || record.owner != owner || record.epoch != epoch);
		Ok(())
	}
}

struct TombstoneEngine {
	teardown_fails:   AtomicBool,
	events:           Arc<Mutex<Vec<&'static str>>>,
	history_attempts: AtomicUsize,
}

impl DeleteTombstoneEngine for TombstoneEngine {
	fn teardown_candidate(
		&self,
		_sid: String,
		_expected_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		self.events.lock().push("teardown");
		let fails = self.teardown_fails.load(Ordering::SeqCst);
		Box::pin(async move {
			if fails {
				Err(MeshError::new("teardown failed"))
			} else {
				Ok(())
			}
		})
	}

	fn delete_portable_history(
		&self,
		_sid: String,
		_owner: String,
		_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		self.events.lock().push("history");
		self.history_attempts.fetch_add(1, Ordering::SeqCst);
		Box::pin(async { Ok(()) })
	}
}

#[tokio::test]
async fn deleting_tombstone_replays_fail_closed_then_commits_after_ordered_retry() {
	let delta = tempfile::tempdir().unwrap();
	let tombstone = pending_record(delta.path());
	let events = Arc::new(Mutex::new(Vec::new()));
	let store =
		TombstoneStore { records: Mutex::new(vec![tombstone.clone()]), events: events.clone() };
	let engine = TombstoneEngine {
		teardown_fails:   AtomicBool::new(true),
		events:           events.clone(),
		history_attempts: AtomicUsize::new(0),
	};
	let fences = Mutex::new(Vec::new());

	converge_delete_tombstones(&store, &engine, |record| {
		events.lock().push("fence");
		fences.lock().push((record.sid.clone(), record.epoch));
	})
	.await
	.unwrap();
	assert_eq!(*fences.lock(), vec![("sandbox-1".to_owned(), 7)]);
	assert_eq!(*events.lock(), vec!["fence", "teardown"]);
	{
		let retained = store.records.lock();
		assert_eq!(retained.len(), 1);
		assert_eq!(retained[0].sid, tombstone.sid);
		assert_eq!(retained[0].epoch, tombstone.epoch);
	}
	assert_eq!(engine.history_attempts.load(Ordering::SeqCst), 0);

	engine.teardown_fails.store(false, Ordering::SeqCst);
	converge_delete_tombstones(&store, &engine, |record| {
		events.lock().push("fence");
		fences.lock().push((record.sid.clone(), record.epoch));
	})
	.await
	.unwrap();
	assert!(store.records.lock().is_empty());
	assert_eq!(engine.history_attempts.load(Ordering::SeqCst), 1);
	assert_eq!(*events.lock(), vec!["fence", "teardown", "fence", "teardown", "history", "commit"],);
}

#[tokio::test]
async fn source_abort_replay_retries_failed_restore_without_second_handoff_cas() {
	let mut source_params = Map::new();
	source_params.insert("volume".to_owned(), json!("data"));
	let source = CreateRecordWire {
		sid:             "sandbox-1".to_owned(),
		params:          source_params,
		owner:           "source".to_owned(),
		epoch:           8,
		idempotency_key: "exact-token".to_owned(),
		ha:              "mesh".to_owned(),
		restart_policy:  "always".to_owned(),
		created_at:      0.0,
	};
	let mut restored = source.clone();
	restored.epoch = 9;
	let engine = EngineFake::new(false, false);
	engine.abort_fails.store(true, Ordering::SeqCst);
	let records = AbortRecordFake {
		current:        Mutex::new(source.clone()),
		fresh:          restored.clone(),
		abort_calls:    AtomicUsize::new(0),
		cas_updates:    AtomicUsize::new(0),
		complete_calls: AtomicUsize::new(0),
		events:         engine.events.clone(),
	};
	let cleanup = MigrationCleanupWire {
		sid:            source.sid.clone(),
		base_dir:       "/base".to_owned(),
		base_digest:    "base-digest".to_owned(),
		delta_dir:      "/delta".to_owned(),
		delta_digest:   "delta-digest".to_owned(),
		target_url:     "http://target".to_owned(),
		migration_id:   "migration-1".to_owned(),
		epoch:          8,
		schema_version: 1,
		source_owner:   "source".to_owned(),
		source_epoch:   8,
		target_owner:   "target".to_owned(),
		target_epoch:   9,
		token:          "exact-token".to_owned(),
		phase:          String::new(),
	};

	assert!(
		MeshRuntime::replay_source_migration_abort(&records, &engine, &cleanup)
			.await
			.is_err()
	);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 1);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 1);
	assert!(engine.cleanup_removed.lock().is_empty());
	let retry = engine.persisted_intents.lock().last().cloned().unwrap();
	assert_eq!(retry.phase, "restore_pending");

	engine.abort_fails.store(false, Ordering::SeqCst);
	let returned = MeshRuntime::replay_source_migration_abort(&records, &engine, &retry)
		.await
		.unwrap();
	assert_eq!(returned.owner, "source");
	assert_eq!(returned.epoch, 9);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 2);
	assert_eq!(records.cas_updates.load(Ordering::SeqCst), 1);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 2);
	assert_eq!(*engine.cleanup_removed.lock(), vec!["sandbox-1".to_owned()]);
	assert_eq!(*engine.events.lock(), vec![
		"journal",
		"cas_abort",
		"journal",
		"abort",
		"aborted_disposition",
		"abort",
		"journal",
		"complete_abort",
		"cleanup_remove",
	],);
}

#[test]
fn unreachable_or_pending_target_status_never_allows_source_abort_replay() {
	let unreachable = MeshError::unreachable("target unavailable");
	let pending = json!({"state": "pending", "committed": false});
	let committed = json!({"state": "committed", "committed": true});
	let absent = json!({"state": "absent", "committed": false});
	assert!(!MeshRuntime::source_migration_abort_allowed(Err(&unreachable)));
	assert!(!MeshRuntime::source_migration_abort_allowed(Ok(pending.as_object().unwrap())));
	assert!(!MeshRuntime::source_migration_abort_allowed(Ok(committed.as_object().unwrap())));
	assert!(MeshRuntime::source_migration_abort_allowed(Ok(absent.as_object().unwrap())));
}

#[tokio::test]
async fn abort_replay_recovers_after_crash_between_cas_and_restore_journal() {
	let mut params = Map::new();
	params.insert("volume".to_owned(), json!("data"));
	let source = CreateRecordWire {
		sid: "sandbox-1".to_owned(),
		params,
		owner: "source".to_owned(),
		epoch: 8,
		idempotency_key: "exact-token".to_owned(),
		ha: "mesh".to_owned(),
		restart_policy: "always".to_owned(),
		created_at: 0.0,
	};
	let mut fresh = source.clone();
	fresh.epoch = 9;
	let engine = EngineFake::new(false, false);
	engine.cleanup_persist_fails.store(true, Ordering::SeqCst);
	let records = AbortRecordFake {
		current:        Mutex::new(source.clone()),
		fresh:          fresh.clone(),
		abort_calls:    AtomicUsize::new(0),
		cas_updates:    AtomicUsize::new(0),
		complete_calls: AtomicUsize::new(0),
		events:         engine.events.clone(),
	};
	let cleanup = MigrationCleanupWire {
		sid:            source.sid.clone(),
		base_dir:       "/base".to_owned(),
		base_digest:    "base-digest".to_owned(),
		delta_dir:      "/delta".to_owned(),
		delta_digest:   "delta-digest".to_owned(),
		target_url:     "http://target".to_owned(),
		migration_id:   "migration-1".to_owned(),
		epoch:          8,
		schema_version: 1,
		source_owner:   "source".to_owned(),
		source_epoch:   8,
		target_owner:   "target".to_owned(),
		target_epoch:   9,
		token:          "exact-token".to_owned(),
		phase:          "abort_pending".to_owned(),
	};

	assert!(
		MeshRuntime::replay_source_migration_abort(&records, &engine, &cleanup)
			.await
			.is_err()
	);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 1);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 0);
	assert!(engine.cleanup_removed.lock().is_empty());

	engine.cleanup_persist_fails.store(false, Ordering::SeqCst);
	let restored = MeshRuntime::replay_source_migration_abort(&records, &engine, &cleanup)
		.await
		.unwrap();
	assert_eq!(restored.owner, "source");
	assert_eq!(restored.epoch, 9);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 2);
	assert_eq!(records.cas_updates.load(Ordering::SeqCst), 1);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 1);
	assert_eq!(*engine.cleanup_removed.lock(), vec!["sandbox-1".to_owned()]);
}

#[tokio::test]
async fn complete_pending_replay_finishes_after_crash_before_journal_removal() {
	let mut params = Map::new();
	params.insert("volume".to_owned(), json!("data"));
	let source = CreateRecordWire {
		sid: "sandbox-1".to_owned(),
		params,
		owner: "source".to_owned(),
		epoch: 8,
		idempotency_key: "exact-token".to_owned(),
		ha: "mesh".to_owned(),
		restart_policy: "always".to_owned(),
		created_at: 0.0,
	};
	let mut fresh = source.clone();
	fresh.epoch = 9;
	let engine = EngineFake::new(false, false);
	engine.cleanup_remove_fails.store(true, Ordering::SeqCst);
	let records = AbortRecordFake {
		current:        Mutex::new(source.clone()),
		fresh:          fresh.clone(),
		abort_calls:    AtomicUsize::new(0),
		cas_updates:    AtomicUsize::new(0),
		complete_calls: AtomicUsize::new(0),
		events:         engine.events.clone(),
	};
	let cleanup = MigrationCleanupWire {
		sid:            source.sid.clone(),
		base_dir:       "/base".to_owned(),
		base_digest:    "base-digest".to_owned(),
		delta_dir:      "/delta".to_owned(),
		delta_digest:   "delta-digest".to_owned(),
		target_url:     "http://target".to_owned(),
		migration_id:   "migration-1".to_owned(),
		epoch:          8,
		schema_version: 1,
		source_owner:   "source".to_owned(),
		source_epoch:   8,
		target_owner:   "target".to_owned(),
		target_epoch:   9,
		token:          "exact-token".to_owned(),
		phase:          String::new(),
	};

	assert!(
		MeshRuntime::replay_source_migration_abort(&records, &engine, &cleanup)
			.await
			.is_err()
	);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 1);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 1);
	assert_eq!(records.complete_calls.load(Ordering::SeqCst), 1);
	let retry = engine.persisted_intents.lock().last().cloned().unwrap();
	assert_eq!(retry.phase, "complete_pending");
	assert_eq!(retry.source_epoch, 9);

	engine.cleanup_remove_fails.store(false, Ordering::SeqCst);
	let completed = MeshRuntime::replay_source_migration_abort(&records, &engine, &retry)
		.await
		.unwrap();
	assert_eq!(completed.owner, "source");
	assert_eq!(completed.epoch, 9);
	assert_eq!(engine.aborts.load(Ordering::SeqCst), 1);
	assert_eq!(records.abort_calls.load(Ordering::SeqCst), 1);
	assert_eq!(records.complete_calls.load(Ordering::SeqCst), 2);
	assert_eq!(*engine.cleanup_removed.lock(), vec!["sandbox-1".to_owned()]);
}
