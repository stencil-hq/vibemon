//! Template spawn counts are written off the create path.

use super::*;

async fn spawn_count(h: &Harness) -> i64 {
	sqlx::query_scalar("SELECT spawn_count FROM templates WHERE id = ?")
		.bind(&h.template_id)
		.fetch_one(h.db.store.pool())
		.await
		.unwrap()
}

/// A create does not wait for the template row (which concurrent creates of
/// one template used to queue on, one durable `UPDATE` each): with the row
/// locked by another transaction it still starts at once, and its spawn is
/// counted when the row is free again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creates_do_not_wait_for_the_template_row() {
	let Some(h) = harness().await else {
		return;
	};
	builds::spawn_spawn_writer(h.state.clone());
	let mut holder = h.db.store.pool().begin().await.unwrap();
	sqlx::query("SELECT id FROM templates WHERE id = ? FOR UPDATE")
		.bind(&h.template_id)
		.fetch_one(&mut *holder)
		.await
		.unwrap();

	let (status, created) = tokio::time::timeout(Duration::from_secs(10), h.create())
		.await
		.expect("the create waited for the locked template row");
	assert_eq!(status, StatusCode::CREATED, "{created}");
	assert_eq!(spawn_count(&h).await, 0);

	holder.rollback().await.unwrap();
	eventually("the spawn count", || async { spawn_count(&h).await == 1 }).await;
	h.finish().await;
}

/// A batch written late (another driver's, or one retried after a failure)
/// adds its count but never moves `last_spawned_at` back.
#[tokio::test]
async fn late_spawn_batches_keep_the_latest_spawn_time() {
	use chrono::SubsecRound as _;

	let Some(h) = harness().await else {
		return;
	};
	let later = chrono::Utc::now().trunc_subsecs(3);
	let earlier = later - chrono::TimeDelta::seconds(30);
	h.db
		.store
		.add_template_spawns(&h.template_id, 2, later)
		.await
		.unwrap();
	h.db
		.store
		.add_template_spawns(&h.template_id, 3, earlier)
		.await
		.unwrap();
	let (count, last): (i64, Option<chrono::DateTime<chrono::Utc>>) =
		sqlx::query_as("SELECT spawn_count, last_spawned_at FROM templates WHERE id = ?")
			.bind(&h.template_id)
			.fetch_one(h.db.store.pool())
			.await
			.unwrap();
	assert_eq!(count, 5);
	assert_eq!(last, Some(later));
	h.finish().await;
}
