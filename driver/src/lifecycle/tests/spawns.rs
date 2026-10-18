//! Template spawn counts are written off the create path.

use super::*;

async fn template(h: &Harness) -> crate::store::templates::TemplateRow {
	h.store.template(&h.template_id).await.unwrap().unwrap()
}

/// A create only counts its spawn in memory (concurrent creates of one
/// template never contend on its record); the writer adds it afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_count_spawns_off_the_create_path() {
	let h = harness().await;
	let (status, created) = h.create().await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	assert_eq!(template(&h).await.spawn_count, 0);

	assert!(builds::flush_spawns(&h.state).await);
	assert_eq!(template(&h).await.spawn_count, 1);
	h.finish().await;
}

/// A batch written late (another driver's, or one retried after a failure)
/// adds its count but never moves `last_spawned_at` back.
#[tokio::test]
async fn late_spawn_batches_keep_the_latest_spawn_time() {
	use chrono::SubsecRound as _;

	let h = harness().await;
	let later = chrono::Utc::now().trunc_subsecs(3);
	let earlier = later - chrono::TimeDelta::seconds(30);
	h.store
		.add_template_spawns(&h.template_id, 2, later)
		.await
		.unwrap();
	h.store
		.add_template_spawns(&h.template_id, 3, earlier)
		.await
		.unwrap();
	let stored = template(&h).await;
	assert_eq!(stored.spawn_count, 5);
	assert_eq!(stored.last_spawned_at, Some(later));
	h.finish().await;
}
