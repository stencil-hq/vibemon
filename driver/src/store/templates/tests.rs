//! Template/build store tests on a private one-voter cluster.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use vmon_cluster::NodeId;
use vmon_node::api::{ArtifactInfo, BuildReason};

use super::*;
use crate::{
	state::TeamLimits,
	store::{
		builds::{BuildStart, BuildStatus, LogDirection, LogLevel, NewLog, superseded_reason},
		testing::TestStore,
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

fn start(spec: &serde_json::Value) -> BuildStart<'_> {
	BuildStart {
		node_id: "0000000000000007",
		arch: "x86_64",
		spec,
		template_version: "2.1.0",
		start_cmd: None,
		ready_cmd: None,
	}
}

fn snapshot<'a>(
	team_id: &'a str,
	kind: TemplateKind,
	name: Option<(&'a str, &'a str)>,
	new_template_id: &'a str,
	build_id: &'a str,
) -> RegisterSnapshot<'a> {
	RegisterSnapshot {
		team_id,
		kind,
		source_sandbox_id: "isbx",
		node_id: "00000000000000d1",
		name,
		tag: "default",
		new_template_id,
		build_id,
		vcpus: 2,
		memory_mb: 512,
		disk_mb: 1024,
		envd_version: "0.6.4",
	}
}

#[tokio::test]
async fn register_reuses_templates_supersedes_waiting_builds_and_enforces_limits() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();

	let (first, first_build) =
		register(store, &team.id, &team.slug, "tpl", &tags(&["v1"]), 10).await;
	let tid = template_id(&first);
	let (second, second_build) =
		register(store, &team.id, &team.slug, "TPL", &tags(&["v1", "latest"]), 10).await;
	assert_eq!(template_id(&second), tid, "the alias (case-insensitive) reuses the template");
	let superseded = store.build(&first_build).await.unwrap().unwrap();
	assert_eq!(superseded.status(), BuildStatus::Error);
	assert_eq!(superseded.reason.unwrap(), superseded_reason());
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
	// Superseded builds free their slot before the limit is checked.
	let (replacing, _) = register(store, &team.id, &team.slug, "tpl", &tags(&["v2"]), 2).await;
	assert_eq!(template_id(&replacing), tid);
	assert_eq!(store.team_unfinished_builds(&team.id).await.unwrap().len(), 2);

	// A snapshot owning the alias blocks template registration.
	let snap_build = uuid::Uuid::new_v4().to_string();
	let snap_template = crate::builds::names::new_template_id();
	let snap = store
		.register_snapshot(&snapshot(
			&team.id,
			TemplateKind::Snapshot,
			Some((&team.slug, "snap")),
			&snap_template,
			&snap_build,
		))
		.await
		.unwrap();
	assert_eq!(snap, SnapshotOutcome::Registered { template_id: snap_template.clone() });
	assert_eq!(
		store
			.template(&snap_template)
			.await
			.unwrap()
			.unwrap()
			.build_count,
		1
	);
	let (taken, _) = register(store, &team.id, &team.slug, "snap", &tags(&["default"]), 10).await;
	assert_eq!(taken, RegisterOutcome::AliasTaken);
	db.finish().await;
}

#[tokio::test]
async fn ready_builds_get_their_tags_and_terminal_states_are_final() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (outcome, build_id) =
		register(store, &team.id, &team.slug, "tpl", &tags(&["default", "v1"]), 10).await;
	let tid = template_id(&outcome);
	let spec = serde_json::json!({"fromImage": "e2bdev/base"});
	assert!(
		store
			.mark_build_started(&build_id, &start(&spec))
			.await
			.unwrap()
	);
	assert!(
		!store
			.mark_build_started(&build_id, &start(&spec))
			.await
			.unwrap(),
		"only waiting builds start"
	);
	assert_eq!(store.building_build_ids().await.unwrap(), std::slice::from_ref(&build_id));
	assert_eq!(store.template(&tid).await.unwrap().unwrap().build_count, 1);
	assert!(store.tag_build(&tid, "default").await.unwrap().is_none(), "tags move only once ready");
	assert!(store.mark_build_running(&build_id).await.unwrap());
	assert!(!store.mark_build_running(&build_id).await.unwrap(), "recorded once");

	assert!(
		store
			.finish_build_ready(&build_id, &artifact(&build_id))
			.await
			.unwrap()
	);
	assert_eq!(store.tag_build(&tid, "v1").await.unwrap().as_deref(), Some(build_id.as_str()));
	assert_eq!(store.building_build_ids().await.unwrap(), Vec::<String>::new());
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

	// A second build is building when the template goes: the delete fails it
	// and returns it for cancellation.
	let (_, building) = register(store, &team.id, &team.slug, "tpl", &tags(&["v2"]), 10).await;
	assert!(
		store
			.mark_build_started(&building, &start(&spec))
			.await
			.unwrap()
	);

	// Soft delete frees the alias and tags; the name then makes a new template.
	let cancelled = store.soft_delete_template(&tid).await.unwrap().unwrap();
	assert_eq!(cancelled.iter().map(|b| b.id.as_str()).collect::<Vec<_>>(), [building.as_str()]);
	assert_eq!(store.build(&building).await.unwrap().unwrap().status(), BuildStatus::Error);
	assert!(store.soft_delete_template(&tid).await.unwrap().is_none());
	assert!(store.template(&tid).await.unwrap().is_none());
	assert_eq!(store.template_tags(&tid).await.unwrap().len(), 0);
	assert_eq!(
		store
			.aliases_of(std::slice::from_ref(&tid))
			.await
			.unwrap()
			.len(),
		0
	);
	let (again, _) = register(store, &team.id, &team.slug, "tpl", &tags(&["default"]), 10).await;
	assert_ne!(template_id(&again), tid);
	let mut pending: Vec<String> = store
		.builds_pending_gc(10)
		.await
		.unwrap()
		.into_iter()
		.map(|b| b.id)
		.collect();
	pending.sort();
	let mut expected = vec![build_id.clone(), building.clone()];
	expected.sort();
	assert_eq!(pending, expected);
	store.mark_artifacts_collected(&build_id).await.unwrap();
	let pending = store.builds_pending_gc(10).await.unwrap();
	assert_eq!(pending.iter().map(|b| b.id.clone()).collect::<Vec<_>>(), [building]);
	db.finish().await;
}

#[tokio::test]
async fn building_builds_are_scoped_to_shared_tags() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let (first, a) = register(store, &team.id, &team.slug, "tpl", &tags(&["a", "shared"]), 10).await;
	let tid = template_id(&first);
	let (_, b) = register(store, &team.id, &team.slug, "tpl", &tags(&["b"]), 10).await;
	let spec = serde_json::json!({"fromImage": "e2bdev/base"});
	for id in [&a, &b] {
		assert!(store.mark_build_started(id, &start(&spec)).await.unwrap());
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
	db.finish().await;
}

#[tokio::test]
async fn template_builds_paginate_newest_first_and_latest_builds_follow_status() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let mut builds = Vec::new();
	let mut tid = String::new();
	for tag in ["a", "b", "c"] {
		let (outcome, build) = register(store, &team.id, &team.slug, "tpl", &tags(&[tag]), 10).await;
		tid = template_id(&outcome);
		builds.push(build);
		tokio::time::sleep(std::time::Duration::from_millis(5)).await;
	}
	let ids = std::slice::from_ref(&tid);
	let first = store.template_builds(&tid, None, Some(2)).await.unwrap();
	assert_eq!(first.iter().map(|b| b.id.clone()).collect::<Vec<_>>(), [
		builds[2].clone(),
		builds[1].clone()
	]);
	let cursor = Cursor {
		created_at_us: crate::store::micros(first[1].created_at),
		id:            first[1].id.clone(),
	};
	let rest = store
		.template_builds(&tid, Some(&cursor), Some(2))
		.await
		.unwrap();
	assert_eq!(rest.iter().map(|b| b.id.clone()).collect::<Vec<_>>(), [builds[0].clone()]);

	assert_eq!(store.latest_builds(ids).await.unwrap()[0].id, builds[2]);
	assert!(store.latest_ready_builds(ids).await.unwrap().is_empty());
	assert!(
		store
			.finish_build_ready(&builds[0], &artifact(&builds[0]))
			.await
			.unwrap()
	);
	assert_eq!(store.latest_ready_builds(ids).await.unwrap()[0].id, builds[0]);
	let ready_only = TemplateFilter { ready_only: true, ..TemplateFilter::default() };
	let listed = store
		.list_templates(&team.id, TemplateKind::Template, &ready_only, None, None)
		.await
		.unwrap();
	assert_eq!(listed.iter().map(|t| t.id.clone()).collect::<Vec<_>>(), [tid.clone()]);
	db.finish().await;
}

#[tokio::test]
async fn log_pages_filter_by_level_and_appends_are_idempotent() {
	let db = TestStore::start().await;
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
	let mut redelivered = logs[2..].to_vec();
	redelivered[0].message = "replaced".into();
	store
		.append_build_logs(&build_id, 2, &redelivered)
		.await
		.unwrap();
	assert_eq!(store.build_log_count(&build_id).await.unwrap(), 5);

	let all = store
		.build_logs_page(&build_id, LogLevel::Debug, 0, 100)
		.await
		.unwrap();
	assert_eq!(all.iter().map(|l| l.seq).collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
	assert_eq!(all[2].message, "line 2", "stored lines stay untouched");
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
		.build_logs_by_time(&build_id, at(3), LogDirection::Backward, LogLevel::Debug, 2)
		.await
		.unwrap();
	assert_eq!(back.iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 2]);
	let fwd = store
		.build_logs_by_time(&build_id, at(3), LogDirection::Forward, LogLevel::Info, 10)
		.await
		.unwrap();
	assert_eq!(fwd.iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 4]);
	let warn_back = store
		.build_logs_by_time(&build_id, at(4), LogDirection::Backward, LogLevel::Warn, 10)
		.await
		.unwrap();
	assert_eq!(warn_back.iter().map(|l| l.seq).collect::<Vec<_>>(), [3, 2]);
	db.finish().await;
}

#[tokio::test]
async fn template_listing_paginates_newest_first() {
	let db = TestStore::start().await;
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

	// Snapshot listings filter by source sandbox and bare alias.
	let snap_template = crate::builds::names::new_template_id();
	let snap_build = uuid::Uuid::new_v4().to_string();
	store
		.register_snapshot(&snapshot(
			&team.id,
			TemplateKind::Snapshot,
			Some((&team.slug, "Snap")),
			&snap_template,
			&snap_build,
		))
		.await
		.unwrap();
	let list = async |filter: TemplateFilter<'_>| {
		store
			.list_templates(&team.id, TemplateKind::Snapshot, &filter, None, None)
			.await
			.unwrap()
			.into_iter()
			.map(|t| t.id)
			.collect::<Vec<_>>()
	};
	let by_alias = TemplateFilter { alias: Some("snap"), ..TemplateFilter::default() };
	assert_eq!(list(by_alias).await, std::slice::from_ref(&snap_template));
	let other_alias = TemplateFilter { alias: Some("a"), ..TemplateFilter::default() };
	assert_eq!(list(other_alias).await.len(), 0);
	let by_sandbox = TemplateFilter { source_sandbox_id: Some("isbx"), ..TemplateFilter::default() };
	assert_eq!(list(by_sandbox).await, [snap_template]);
	let other_sandbox = TemplateFilter { source_sandbox_id: Some("x"), ..TemplateFilter::default() };
	assert_eq!(list(other_sandbox).await.len(), 0);
	db.finish().await;
}

#[tokio::test]
async fn only_public_system_templates_hold_bare_aliases() {
	let db = TestStore::start().await;
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
	let names: Vec<String> = store
		.aliases_of(std::slice::from_ref(&base))
		.await
		.unwrap()
		.iter()
		.map(AliasRow::name)
		.collect();
	assert_eq!(names, [format!("{}/base", system.slug), "base".into()]);
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
	db.finish().await;
}

#[tokio::test]
async fn concurrent_registrations_of_new_aliases_all_succeed() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	// Concurrent registrations of one team conflict on its unfinished-build
	// set; each loser reruns until it commits.
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
	assert_eq!(store.team_unfinished_builds(&team.id).await.unwrap().len(), 20);
	db.finish().await;
}

#[tokio::test]
async fn concurrent_registrations_respect_the_team_limit() {
	let db = TestStore::start().await;
	let store = &db.store;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	let aliases: Vec<String> = (0..6).map(|i| format!("tpl-{i}")).collect();
	let outcomes = futures::future::join_all(
		aliases
			.iter()
			.map(|alias| register(store, &team.id, &team.slug, alias, &[], 3)),
	)
	.await;
	let admitted = outcomes
		.iter()
		.filter(|(outcome, _)| matches!(outcome, RegisterOutcome::Registered { .. }))
		.count();
	assert_eq!(admitted, 3);
	assert_eq!(store.team_unfinished_builds(&team.id).await.unwrap().len(), 3);
	db.finish().await;
}

#[tokio::test]
async fn fork_purge_waits_for_collected_artifacts_and_cascades() {
	let db = TestStore::start().await;
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
		.register_snapshot(&snapshot(&team.id, TemplateKind::Fork, None, &fork_id, &fork_build))
		.await
		.unwrap();
	let line = NewLog {
		ts:      Utc::now(),
		level:   LogLevel::Info,
		step:    None,
		message: "captured".into(),
	};
	store
		.append_build_logs(&fork_build, 0, &[line])
		.await
		.unwrap();
	let future = Utc::now() + chrono::TimeDelta::minutes(1);
	assert_eq!(
		store.unretired_fork_templates(future).await.unwrap(),
		std::slice::from_ref(&fork_id)
	);
	assert!(
		store
			.soft_delete_template(&fork_id)
			.await
			.unwrap()
			.is_some()
	);
	assert_eq!(store.unretired_fork_templates(future).await.unwrap(), Vec::<String>::new());
	assert_eq!(store.purge_collected_forks().await.unwrap(), 0, "artifacts not collected yet");
	store.mark_artifacts_collected(&fork_build).await.unwrap();
	assert_eq!(store.purge_collected_forks().await.unwrap(), 1);
	assert_eq!(store.purge_collected_forks().await.unwrap(), 0);
	assert!(store.build(&fork_build).await.unwrap().is_none(), "builds cascade");
	assert_eq!(store.build_log_count(&fork_build).await.unwrap(), 0, "build logs cascade");
	assert!(store.template(&live).await.unwrap().is_some(), "other templates stay");
	db.finish().await;
}
