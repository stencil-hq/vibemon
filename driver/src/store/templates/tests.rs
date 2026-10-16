//! MySQL-gated tests (`VMON_MYSQL_TEST_URL`) of the template/build store.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use vmon_cluster::NodeId;
use vmon_node::api::{ArtifactInfo, BuildReason};

use super::*;
use crate::{
	state::TeamLimits,
	store::{
		builds::{BuildStart, BuildStatus, LogLevel, NewLog},
		testdb::TestDb,
	},
};

fn tags(list: &[&str]) -> Vec<String> {
	list.iter().map(ToString::to_string).collect()
}

async fn register(
	store: &Store,
	team: &str,
	ns: &str,
	alias: &str,
	tag_list: &[String],
	limit: u32,
) -> (RegisterOutcome, String) {
	let build_id = uuid::Uuid::new_v4().to_string();
	let new_id = crate::builds::names::new_template_id();
	let outcome = store
		.register_build(&RegisterBuild {
			team_id: team,
			namespace: ns,
			alias,
			new_template_id: &new_id,
			build_id: &build_id,
			tags: tag_list,
			vcpus: 2,
			memory_mb: 1024,
			disk_mb: 4096,
			min_free_disk_mb: None,
			max_concurrent_builds: limit,
		})
		.await
		.unwrap();
	(outcome, build_id)
}

fn template_id(outcome: &RegisterOutcome) -> String {
	match outcome {
		RegisterOutcome::Registered { template_id, .. } => template_id.clone(),
		other => panic!("expected a registration, got {other:?}"),
	}
}

fn artifact(build_id: &str) -> ArtifactInfo {
	ArtifactInfo {
		build_id:        build_id.into(),
		node_id:         NodeId(7),
		arch:            "x86_64".into(),
		backend:         "kvm".into(),
		cpu_baseline:    "v3".into(),
		has_snapshot:    true,
		bytes:           1,
		envd_version:    "0.6.4".into(),
		vcpus:           4,
		memory_mb:       2048,
		disk_mb:         8192,
		default_user:    "user".into(),
		default_workdir: None,
		env:             BTreeMap::new(),
	}
}

#[tokio::test]
async fn register_reuses_templates_supersedes_waiting_builds_and_enforces_limits() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();

	let (first, first_build) =
		register(store, &team.id, &team.slug, "tpl", &tags(&["v1"]), 10).await;
	let tid = template_id(&first);
	let (second, second_build) =
		register(store, &team.id, &team.slug, "tpl", &tags(&["v1", "latest"]), 10).await;
	assert_eq!(template_id(&second), tid, "same alias reuses the template");
	let superseded = store.build(&first_build).await.unwrap().unwrap();
	assert_eq!(superseded.status(), BuildStatus::Error);
	assert_eq!(superseded.reason.unwrap().0, crate::store::builds::superseded_reason());
	assert_eq!(store.build(&second_build).await.unwrap().unwrap().status(), BuildStatus::Waiting);

	// A disjoint tag set does not supersede; the limit counts waiting builds.
	let (third, _) = register(store, &team.id, &team.slug, "tpl", &tags(&["v2"]), 10).await;
	assert_eq!(template_id(&third), tid);
	assert_eq!(store.build(&second_build).await.unwrap().unwrap().status(), BuildStatus::Waiting);
	let (limited, _) = register(store, &team.id, &team.slug, "fresh", &tags(&["default"]), 2).await;
	assert_eq!(limited, RegisterOutcome::ConcurrencyLimit { limit: 2 });
	assert!(
		store
			.template_by_alias(&team.slug, "fresh")
			.await
			.unwrap()
			.is_none(),
		"rolled back"
	);

	// A snapshot owning the alias blocks template registration.
	let snap_build = uuid::Uuid::new_v4().to_string();
	let snap = store
		.register_snapshot(&RegisterSnapshot {
			team_id:           &team.id,
			kind:              TemplateKind::Snapshot,
			source_sandbox_id: "isbx",
			node_id:           "00000000000000d1",
			name:              Some((&team.slug, "snap")),
			tag:               "default",
			new_template_id:   &crate::builds::names::new_template_id(),
			build_id:          &snap_build,
			vcpus:             2,
			memory_mb:         512,
			disk_mb:           1024,
			envd_version:      "0.6.4",
		})
		.await
		.unwrap();
	assert!(matches!(snap, SnapshotOutcome::Registered { .. }));
	let (taken, _) = register(store, &team.id, &team.slug, "snap", &tags(&["default"]), 10).await;
	assert_eq!(taken, RegisterOutcome::AliasTaken);
	db.drop_db().await;
}

#[tokio::test]
async fn ready_builds_get_their_tags_and_terminal_states_are_final() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (outcome, build_id) =
		register(store, &team.id, &team.slug, "tpl", &tags(&["default", "v1"]), 10).await;
	let tid = template_id(&outcome);
	let spec = serde_json::json!({"fromImage": "e2bdev/base"});
	let start = BuildStart {
		node_id:          "0000000000000007",
		arch:             "x86_64",
		spec:             &spec,
		template_version: "2.1.0",
		start_cmd:        None,
		ready_cmd:        None,
	};
	assert!(store.mark_build_started(&build_id, &start).await.unwrap());
	assert!(
		!store.mark_build_started(&build_id, &start).await.unwrap(),
		"only waiting builds start"
	);
	assert_eq!(store.template(&tid).await.unwrap().unwrap().build_count, 1);
	assert!(store.tag_build(&tid, "default").await.unwrap().is_none(), "tags move only once ready");

	assert!(
		store
			.finish_build_ready(&build_id, &artifact(&build_id))
			.await
			.unwrap()
	);
	assert_eq!(store.tag_build(&tid, "v1").await.unwrap().as_deref(), Some(build_id.as_str()));
	let ready = store.build(&build_id).await.unwrap().unwrap();
	assert_eq!(
		(ready.vcpus, ready.memory_mb, ready.envd_version.as_deref()),
		(4, 2048, Some("0.6.4"))
	);
	let reason = BuildReason { message: "late".into(), step: None };
	assert!(!store.fail_build(&build_id, &reason).await.unwrap(), "ready is terminal");
	assert!(
		!store
			.finish_build_ready(&build_id, &artifact(&build_id))
			.await
			.unwrap()
	);

	// Soft delete frees the alias and tags; the name then makes a new template.
	assert!(store.soft_delete_template(&tid).await.unwrap().is_some());
	assert!(store.soft_delete_template(&tid).await.unwrap().is_none());
	assert!(store.template(&tid).await.unwrap().is_none());
	assert!(store.template_tags(&tid).await.unwrap().is_empty());
	let (again, _) = register(store, &team.id, &team.slug, "tpl", &tags(&["default"]), 10).await;
	assert_ne!(template_id(&again), tid);
	assert_eq!(
		store
			.builds_pending_gc(10)
			.await
			.unwrap()
			.iter()
			.map(|b| b.id.clone())
			.collect::<Vec<_>>(),
		[build_id]
	);
	db.drop_db().await;
}

#[tokio::test]
async fn building_builds_are_scoped_to_shared_tags() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (first, a) = register(store, &team.id, &team.slug, "tpl", &tags(&["a", "shared"]), 10).await;
	let tid = template_id(&first);
	let (_, b) = register(store, &team.id, &team.slug, "tpl", &tags(&["b"]), 10).await;
	let spec = serde_json::json!({"fromImage": "e2bdev/base"});
	let start = BuildStart {
		node_id:          "0000000000000007",
		arch:             "x86_64",
		spec:             &spec,
		template_version: "2.1.0",
		start_cmd:        None,
		ready_cmd:        None,
	};
	for id in [&a, &b] {
		assert!(store.mark_build_started(id, &start).await.unwrap());
	}
	let ids = |rows: Vec<crate::store::builds::BuildRow>| {
		rows.into_iter().map(|r| r.id).collect::<Vec<_>>()
	};
	assert_eq!(
		ids(
			store
				.template_building_builds(&tid, &tags(&["b"]))
				.await
				.unwrap()
		),
		std::slice::from_ref(&b)
	);
	assert_eq!(
		ids(
			store
				.template_building_builds(&tid, &tags(&["shared", "x"]))
				.await
				.unwrap()
		),
		[a]
	);
	assert!(
		store
			.template_building_builds(&tid, &tags(&["c"]))
			.await
			.unwrap()
			.is_empty()
	);
	db.drop_db().await;
}

#[tokio::test]
async fn log_pages_filter_by_level_and_appends_are_idempotent() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (_, build_id) = register(store, &team.id, &team.slug, "tpl", &tags(&["default"]), 10).await;
	let at = |s| Utc.timestamp_opt(1_767_000_000 + s, 0).unwrap();
	let logs: Vec<NewLog> =
		[LogLevel::Info, LogLevel::Debug, LogLevel::Error, LogLevel::Warn, LogLevel::Info]
			.into_iter()
			.enumerate()
			.map(|(i, level)| NewLog {
				ts: at(i as i64),
				level,
				step: Some("1".into()),
				message: format!("line {i}"),
			})
			.collect();
	store
		.append_build_logs(&build_id, 0, &logs[..3])
		.await
		.unwrap();
	store
		.append_build_logs(&build_id, 2, &logs[2..])
		.await
		.unwrap();
	assert_eq!(store.build_log_count(&build_id).await.unwrap(), 5);

	let all = store
		.build_logs_page(&build_id, LogLevel::Debug, 0, 100)
		.await
		.unwrap();
	assert_eq!(all.iter().map(|l| l.seq).collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
	let warn = store
		.build_logs_page(&build_id, LogLevel::Warn, 0, 100)
		.await
		.unwrap();
	assert_eq!(warn.iter().map(|l| l.message.as_str()).collect::<Vec<_>>(), ["line 2", "line 3"]);
	let second_page = store
		.build_logs_page(&build_id, LogLevel::Debug, 3, 1)
		.await
		.unwrap();
	assert_eq!(second_page[0].message, "line 3");
	assert!(
		store
			.build_logs_page(&build_id, LogLevel::Debug, 5, 100)
			.await
			.unwrap()
			.is_empty(),
		"end page is empty"
	);

	let back = store
		.build_logs_by_time(
			&build_id,
			at(3),
			crate::store::builds::LogDirection::Backward,
			LogLevel::Debug,
			2,
		)
		.await
		.unwrap();
	assert_eq!(back.iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 2]);
	let fwd = store
		.build_logs_by_time(
			&build_id,
			at(3),
			crate::store::builds::LogDirection::Forward,
			LogLevel::Info,
			10,
		)
		.await
		.unwrap();
	assert_eq!(fwd.iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 4]);
	db.drop_db().await;
}

#[tokio::test]
async fn template_listing_paginates_newest_first() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let mut ids = Vec::new();
	for alias in ["a", "b", "c"] {
		let (outcome, _) =
			register(store, &team.id, &team.slug, alias, &tags(&["default"]), 10).await;
		ids.push(template_id(&outcome));
		tokio::time::sleep(std::time::Duration::from_millis(5)).await;
	}
	let filter = TemplateFilter::default();
	let first = store
		.list_templates(&team.id, TemplateKind::Template, &filter, None, Some(2))
		.await
		.unwrap();
	assert_eq!(first.iter().map(|t| t.id.clone()).collect::<Vec<_>>(), [
		ids[2].clone(),
		ids[1].clone()
	]);
	let cursor = Cursor {
		created_at_us: crate::store::micros(first[1].created_at),
		id:            first[1].id.clone(),
	};
	let rest = store
		.list_templates(&team.id, TemplateKind::Template, &filter, Some(&cursor), Some(2))
		.await
		.unwrap();
	assert_eq!(rest.iter().map(|t| t.id.clone()).collect::<Vec<_>>(), [ids[0].clone()]);
	let ready_only = TemplateFilter { ready_only: true, ..TemplateFilter::default() };
	assert!(
		store
			.list_templates(&team.id, TemplateKind::Template, &ready_only, None, None)
			.await
			.unwrap()
			.is_empty()
	);
	db.drop_db().await;
}

#[tokio::test]
async fn only_public_system_templates_hold_bare_aliases() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let bare = async |alias: &str| {
		store
			.template_by_alias("", alias)
			.await
			.unwrap()
			.map(|t| t.id)
	};
	let acme = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let system = store
		.create_team("System", TeamLimits::default())
		.await
		.unwrap();
	assert!(store.set_team_system(&system.id, true).await.unwrap());
	let squat = template_id(
		&register(store, &acme.id, &acme.slug, "base", &[], 10)
			.await
			.0,
	);
	let base = template_id(
		&register(store, &system.id, &system.slug, "base", &[], 10)
			.await
			.0,
	);

	// An ordinary team's publish leaves the bare alias free.
	assert!(store.set_template_public(&squat, true).await.unwrap());
	assert_eq!(bare("base").await, None);
	// A system team's publish claims it; unpublishing releases it.
	assert!(store.set_template_public(&base, true).await.unwrap());
	assert_eq!(bare("base").await, Some(base.clone()));
	assert!(store.set_template_public(&base, false).await.unwrap());
	assert_eq!(bare("base").await, None);

	// Team flag changes follow the team's public templates.
	assert!(store.set_template_public(&base, true).await.unwrap());
	assert!(store.set_team_system(&system.id, false).await.unwrap());
	assert_eq!(bare("base").await, None);
	assert!(!store.team_by_id(&system.id).await.unwrap().unwrap().system);
	assert!(store.set_team_system(&system.id, true).await.unwrap());
	assert_eq!(bare("base").await, Some(base.clone()));
	assert!(store.team_by_id(&system.id).await.unwrap().unwrap().system);
	db.drop_db().await;
}

#[tokio::test]
async fn concurrent_registrations_of_new_aliases_all_succeed() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	// Fresh aliases in the same index gap used to deadlock (MySQL 1213):
	// both transactions hold the gap lock of their `FOR UPDATE` probe and
	// then insert into it.
	for round in 0..5 {
		let aliases: Vec<String> = (0..4).map(|i| format!("tpl-{round}-{i}")).collect();
		let outcomes = futures::future::join_all(
			aliases
				.iter()
				.map(|alias| register(store, &team.id, &team.slug, alias, &[], 100)),
		)
		.await;
		for (outcome, _) in outcomes {
			template_id(&outcome);
		}
	}
	db.drop_db().await;
}

/// Waits until a statement on the test database blocks on a row lock.
async fn wait_for_lock_wait(store: &Store) {
	for _ in 0..500 {
		let waiting: i64 = sqlx::query_scalar(
			"SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN \
			 performance_schema.data_locks l ON l.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID WHERE \
			 l.OBJECT_SCHEMA = DATABASE()",
		)
		.fetch_one(store.pool())
		.await
		.unwrap();
		if waiting > 0 {
			return;
		}
		tokio::time::sleep(std::time::Duration::from_millis(10)).await;
	}
	panic!("no statement started waiting for a lock");
}

/// Asserts that no other transaction holds a lock on build `id`.
async fn assert_build_unlocked(store: &Store, id: &str) {
	let mut probe = store.pool().begin().await.unwrap();
	sqlx::query("SELECT id FROM builds WHERE id = ? FOR UPDATE NOWAIT")
		.bind(id)
		.fetch_one(&mut *probe)
		.await
		.expect("the build row is not locked while the template lock is awaited");
	probe.rollback().await.unwrap();
}

#[tokio::test]
async fn deadlocked_template_delete_is_rerun() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (outcome, build_id) = register(store, &team.id, &team.slug, "tpl", &tags(&["v1"]), 10).await;
	let tid = template_id(&outcome);

	// Another transaction locks the build, then the template: the reverse of
	// the delete's order. Its inserted log lines make it the heavier one, so
	// InnoDB picks the delete as the deadlock victim.
	let mut other = store.pool().begin().await.unwrap();
	sqlx::query(
		"INSERT INTO build_logs (build_id, seq, ts, level, message) WITH RECURSIVE n (i) AS (SELECT \
		 0 UNION ALL SELECT i + 1 FROM n WHERE i < 199) SELECT ?, i, UTC_TIMESTAMP(3), 'info', \
		 'weight' FROM n",
	)
	.bind(&build_id)
	.execute(&mut *other)
	.await
	.unwrap();
	sqlx::query("UPDATE builds SET updated_at = UTC_TIMESTAMP(3) WHERE id = ?")
		.bind(&build_id)
		.execute(&mut *other)
		.await
		.unwrap();
	let delete = tokio::spawn({
		let store = store.clone();
		let tid = tid.clone();
		async move { store.soft_delete_template(&tid).await }
	});
	// The delete holds the template and waits for the build.
	wait_for_lock_wait(store).await;
	sqlx::query("UPDATE templates SET updated_at = UTC_TIMESTAMP(3) WHERE id = ?")
		.bind(&tid)
		.execute(&mut *other)
		.await
		.expect("the delete, not this transaction, is the deadlock victim");
	other.commit().await.unwrap();

	let deleted = delete
		.await
		.unwrap()
		.expect("the deadlocked delete is rerun");
	assert!(deleted.is_some());
	assert!(store.template(&tid).await.unwrap().is_none());
	assert_eq!(store.build(&build_id).await.unwrap().unwrap().status(), BuildStatus::Error);
	db.drop_db().await;
}

#[tokio::test]
async fn build_transitions_lock_the_template_before_the_build() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (outcome, build_id) = register(store, &team.id, &team.slug, "tpl", &tags(&["v1"]), 10).await;
	let tid = template_id(&outcome);
	let lock_template = async || {
		let mut holder = store.pool().begin().await.unwrap();
		sqlx::query("SELECT id FROM templates WHERE id = ? FOR UPDATE")
			.bind(&tid)
			.fetch_one(&mut *holder)
			.await
			.unwrap();
		holder
	};

	// Template deletes and registrations lock the template, then its builds;
	// a transition locking the build first closes a deadlock cycle with them.
	let holder = lock_template().await;
	let started = tokio::spawn({
		let store = store.clone();
		let build_id = build_id.clone();
		async move {
			let spec = serde_json::json!({"fromImage": "e2bdev/base"});
			let start = BuildStart {
				node_id:          "0000000000000007",
				arch:             "x86_64",
				spec:             &spec,
				template_version: "2.1.0",
				start_cmd:        None,
				ready_cmd:        None,
			};
			store.mark_build_started(&build_id, &start).await
		}
	});
	wait_for_lock_wait(store).await;
	assert_build_unlocked(store, &build_id).await;
	holder.commit().await.unwrap();
	assert!(started.await.unwrap().unwrap());

	let holder = lock_template().await;
	let finished = tokio::spawn({
		let store = store.clone();
		let build_id = build_id.clone();
		async move {
			store
				.finish_build_ready(&build_id, &artifact(&build_id))
				.await
		}
	});
	wait_for_lock_wait(store).await;
	assert_build_unlocked(store, &build_id).await;
	holder.commit().await.unwrap();
	assert!(finished.await.unwrap().unwrap());
	assert_eq!(store.tag_build(&tid, "v1").await.unwrap().as_deref(), Some(build_id.as_str()));
	db.drop_db().await;
}

#[tokio::test]
async fn fork_purge_leaves_other_templates_unlocked() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (outcome, _) = register(store, &team.id, &team.slug, "tpl", &tags(&["v1"]), 10).await;
	let live = template_id(&outcome);
	let fork_id = crate::builds::names::new_template_id();
	let fork_build = uuid::Uuid::new_v4().to_string();
	store
		.register_snapshot(&RegisterSnapshot {
			team_id:           &team.id,
			kind:              TemplateKind::Fork,
			source_sandbox_id: "isbx",
			node_id:           "00000000000000d1",
			name:              None,
			tag:               "default",
			new_template_id:   &fork_id,
			build_id:          &fork_build,
			vcpus:             2,
			memory_mb:         512,
			disk_mb:           1024,
			envd_version:      "0.6.4",
		})
		.await
		.unwrap();
	assert!(
		store
			.soft_delete_template(&fork_id)
			.await
			.unwrap()
			.is_some()
	);
	assert_eq!(store.purge_collected_forks().await.unwrap(), 0, "artifacts not collected yet");
	store.mark_artifacts_collected(&fork_build).await.unwrap();

	// A transaction holding an unrelated template (a spawn count bump, a
	// build registration) must not stall the purge, nor the purge it.
	let mut holder = store.pool().begin().await.unwrap();
	sqlx::query("SELECT id FROM templates WHERE id = ? FOR UPDATE")
		.bind(&live)
		.fetch_one(&mut *holder)
		.await
		.unwrap();
	let purged =
		tokio::time::timeout(std::time::Duration::from_secs(5), store.purge_collected_forks())
			.await
			.expect("the purge does not wait for unrelated templates")
			.unwrap();
	holder.rollback().await.unwrap();
	assert_eq!(purged, 1);
	assert!(store.build(&fork_build).await.unwrap().is_none(), "builds cascade");
	db.drop_db().await;
}
