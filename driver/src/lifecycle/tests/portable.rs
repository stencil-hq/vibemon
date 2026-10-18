//! Portable paused sandboxes (object store) and sandbox self-keepalive with
//! the envd access token.

use vmon_cluster::kv::{Compare, Op, Txn};
use vmon_node::paused_store::{self, Remote};

use super::*;
use crate::lifecycle::{self as lifecycle, KillReason};

async fn create(h: &Harness) -> String {
	let (status, created) = h.create().await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	created["sandboxID"].as_str().unwrap().to_owned()
}

/// Makes the (fake) node advertise an object store, after `change`.
async fn object_store_node(h: &Harness, change: impl FnOnce(&mut NodeCapacity)) {
	h.cluster.update_meta(|meta| {
		let cap = meta.node.as_mut().unwrap();
		if !cap.features.iter().any(|f| f == paused_store::FEATURE) {
			cap.features.push(paused_store::FEATURE.into());
		}
		change(cap);
	});
	eventually("object-store capability in gossip", || async {
		h.state.node_members().iter().any(|(id, meta)| {
			*id == NODE
				&& meta.node.as_ref().is_some_and(|cap| {
					cap.features
						.iter()
						.any(|feature| feature == paused_store::FEATURE)
				})
		})
	})
	.await;
}

/// A paused sandbox of an object-store node with a published generation.
async fn portable_paused(h: &Harness) -> (String, Remote) {
	object_store_node(h, |_| {}).await;
	let id = create(h).await;
	let (status, body, _) = h
		.api
		.call("POST", &format!("/sandboxes/{id}/pause"), Some(json!({})))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
	let owner = h
		.cluster
		.kv
		.get_consistent(&paused_store::owner_key(&id))
		.await
		.unwrap()
		.expect("the pause names its publisher");
	assert_eq!(serde_json::from_slice::<NodeId>(&owner.value).unwrap(), NODE);
	let remote = Remote {
		url:          "s3://fake-paused-store".into(),
		manifest:     format!("paused/{id}/{}/manifest.json", uuid::Uuid::new_v4()),
		arch:         "x86_64".into(),
		backend:      "kvm".into(),
		cpu_baseline: "v3".into(),
		bytes:        1,
	};
	h.cluster
		.kv
		.put(&paused_store::key(&id), serde_json::to_vec(&remote).unwrap(), None)
		.await
		.unwrap();
	(id, remote)
}

/// The paused sandbox's holder disappears (row names a dead node; the
/// fake node forgets it).
async fn lose_paused_owner(h: &Harness, id: &str) {
	let dead = NodeId(0xdead).to_string();
	h.store
		.edit_sandbox_for_test(id, |row| row.node_id.clone_from(&dead))
		.await;
	h.fake.lock().sandboxes.remove(id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_only_pauses_write_no_portable_keys() {
	let h = harness().await;
	let id = create(&h).await;
	lifecycle::pause(&h.state, Some(&h.team.id), &id, true)
		.await
		.unwrap();
	lifecycle::resume(&h.state, &h.team, &id, 600_000, false)
		.await
		.unwrap();
	let request = h.fake.lock().resumes.last().unwrap().1.clone();
	assert!(request.remote.is_none() && request.meta.is_none());
	assert!(
		request.artifact_sources.is_empty(),
		"unexpected artifact sources: {:?}",
		request.artifact_sources
	);
	lifecycle::kill(&h.state, Some(&h.team.id), &id, KillReason::Request)
		.await
		.unwrap();
	assert!(
		h.cluster
			.kv
			.list(&format!("paused/{id}/"))
			.await
			.unwrap()
			.is_empty(),
		"without an object store the lifecycle writes no paused/ keys"
	);
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_resume_prefers_the_live_owner_and_offers_the_record() {
	let h = harness().await;
	let (id, remote) = portable_paused(&h).await;
	let (status, body, _) = h
		.api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let request = h.fake.lock().resumes.last().unwrap().1.clone();
	assert!(!request.cold_boot);
	assert_eq!(request.remote.as_ref(), Some(&remote), "the holder may have lost its copy");
	let meta = request.meta.expect("metadata for adoption");
	assert_eq!((meta.sandbox_id.as_str(), meta.team_id.as_str()), (id.as_str(), h.team.id.as_str()));
	assert!(request.artifact_sources.is_empty(), "no relocation");
	assert_eq!(
		get_route(&h.cluster.kv, &id)
			.await
			.unwrap()
			.unwrap()
			.node_id,
		NODE
	);
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_resume_relocates_from_a_dead_owner() {
	let h = harness().await;
	let (id, remote) = portable_paused(&h).await;
	lose_paused_owner(&h, &id).await;
	lifecycle::resume(&h.state, &h.team, &id, 600_000, false)
		.await
		.unwrap();
	let request = h.fake.lock().resumes.last().unwrap().1.clone();
	assert!(!request.cold_boot);
	assert_eq!(request.remote.as_ref(), Some(&remote));
	let meta = request.meta.unwrap();
	assert_eq!(meta.envd_access_token, h.state.keys.envd_token(&id));
	assert!(meta.pid.is_none() && meta.tap.is_none());
	assert!(!request.artifact_sources.is_empty(), "the template holder is offered");
	let owner = h
		.cluster
		.kv
		.get_consistent(&paused_store::owner_key(&id))
		.await
		.unwrap()
		.unwrap();
	assert_eq!(serde_json::from_slice::<NodeId>(&owner.value).unwrap(), NODE);
	assert_eq!(
		get_route(&h.cluster.kv, &id)
			.await
			.unwrap()
			.unwrap()
			.node_id,
		NODE
	);
	assert_eq!(h.store.sandbox(&id).await.unwrap().unwrap().node_id, NODE.to_string());
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_resume_cold_boots_on_backend_or_cpu_mismatch_but_never_changes_arch() {
	for mismatch in ["backend", "cpu", "arch"] {
		let h = harness().await;
		let (id, _) = portable_paused(&h).await;
		lose_paused_owner(&h, &id).await;
		let resumes = h.fake.lock().resumes.len();
		object_store_node(&h, |cap| match mismatch {
			"backend" => cap.backend = "hvf".into(),
			"cpu" => cap.cpu_baseline = "v2".into(),
			_ => cap.arch = "aarch64".into(),
		})
		.await;
		eventually("changed capability in gossip", || async {
			h.state.node_members().iter().any(|(_, meta)| {
				meta.node.as_ref().is_some_and(|cap| {
					cap.backend == "hvf" || cap.cpu_baseline == "v2" || cap.arch == "aarch64"
				})
			})
		})
		.await;
		let result = lifecycle::resume(&h.state, &h.team, &id, 600_000, false).await;
		if mismatch == "arch" {
			assert_eq!(result.unwrap_err().status, StatusCode::SERVICE_UNAVAILABLE);
			assert_eq!(h.fake.lock().resumes.len(), resumes);
			assert!(get_route(&h.cluster.kv, &id).await.unwrap().is_none());
		} else {
			result.unwrap();
			assert!(h.fake.lock().resumes.last().unwrap().1.cold_boot, "{mismatch}");
		}
		h.finish().await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_resume_needs_an_object_store_node_and_keeps_volume_pins() {
	let h = harness().await;
	let (id, _) = portable_paused(&h).await;
	lose_paused_owner(&h, &id).await;
	h.cluster.update_meta(|meta| {
		meta
			.node
			.as_mut()
			.unwrap()
			.features
			.retain(|f| f != paused_store::FEATURE);
	});
	eventually("object store gone from gossip", || async {
		h.state.node_members().iter().all(|(_, meta)| {
			meta
				.node
				.as_ref()
				.is_none_or(|cap| cap.features.iter().all(|f| f != paused_store::FEATURE))
		})
	})
	.await;
	// The record stays authoritative even when no live node can import it.
	let error = lifecycle::resume(&h.state, &h.team, &id, 600_000, false)
		.await
		.unwrap_err();
	assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE, "no importing node");

	object_store_node(&h, |_| {}).await;
	let dead = NodeId(0xdead).to_string();
	let volume = h
		.store
		.insert_volume(&h.team.id, "pinned", Some(&dead), false)
		.await
		.unwrap();
	let mounts: Vec<vmon_node::api::VolumeMount> = serde_json::from_value(serde_json::json!([
		{"volumeId": volume.id, "name": volume.name, "path": "/data"}
	]))
	.unwrap();
	h.store
		.edit_sandbox_for_test(&id, |row| row.volume_mounts.clone_from(&mounts))
		.await;
	let resumes = h.fake.lock().resumes.len();
	let error = lifecycle::resume(&h.state, &h.team, &id, 600_000, false)
		.await
		.unwrap_err();
	assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE, "a dead volume pin never migrates");
	assert_eq!(h.fake.lock().resumes.len(), resumes);
	assert!(get_route(&h.cluster.kv, &id).await.unwrap().is_none());
	assert_eq!(h.store.sandbox(&id).await.unwrap().unwrap().node_id, dead);
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_fences_late_uploads_and_keeps_the_record_until_cleanup_succeeds() {
	let h = harness().await;
	let (id, mut remote) = portable_paused(&h).await;
	lose_paused_owner(&h, &id).await;
	// An unusable store fails the object cleanup.
	remote.url = "invalid://paused-store".into();
	let key = paused_store::key(&id);
	h.cluster
		.kv
		.put(&key, serde_json::to_vec(&remote).unwrap(), None)
		.await
		.unwrap();
	assert!(
		lifecycle::kill(&h.state, Some(&h.team.id), &id, KillReason::Request)
			.await
			.is_err()
	);
	assert!(
		h.cluster.kv.get_consistent(&key).await.unwrap().is_some(),
		"a failed cleanup keeps the record for the retry"
	);
	let fence = paused_store::deleted_key(&id);
	assert!(h.cluster.kv.get_consistent(&fence).await.unwrap().is_some());
	// The node-side publication transaction of a late upload now fails.
	let late = h
		.cluster
		.kv
		.txn(Txn {
			compare: vec![Compare::Missing(fence), Compare::Missing(key.clone())],
			success: vec![Op::Put {
				key:    key.clone(),
				value:  serde_json::to_vec(&remote).unwrap(),
				ttl_ms: None,
			}],
			..Txn::default()
		})
		.await
		.unwrap();
	assert!(!late.succeeded);
	assert_eq!(
		lifecycle::resume(&h.state, &h.team, &id, 600_000, false)
			.await
			.unwrap_err()
			.status,
		StatusCode::NOT_FOUND,
		"a fenced sandbox never resumes"
	);
	// Once the objects are gone the retry needs no live holder.
	h.cluster.kv.delete(&key).await.unwrap();
	lifecycle::kill(&h.state, Some(&h.team.id), &id, KillReason::Request)
		.await
		.unwrap();
	assert_eq!(
		h.store.sandbox(&id).await.unwrap().unwrap().state(),
		crate::store::sandboxes::SandboxState::Killed
	);
	assert!(
		h.cluster
			.kv
			.get_consistent(&paused_store::owner_key(&id))
			.await
			.unwrap()
			.is_none()
	);
	h.finish().await;
}

/// `X-Access-Token: <envdAccessToken>` authenticates the detail, timeout,
/// and refresh routes of its own sandbox and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn envd_token_keeps_only_its_own_sandbox_alive() {
	let h = harness().await;
	let (api, state, fake) = (&h.api, &h.state, &h.fake);
	let api_key = crate::admin::create_team_api_key(&h.store, &state.keys, &h.team.id, "keepalive")
		.await
		.unwrap();
	let id = create(&h).await;
	let other = create(&h).await;
	let token = state.keys.envd_token(&id);
	let headers = [("x-access-token", token.as_str())];
	let end = |id: String| async move {
		get_route(&state.cluster.kv, &id)
			.await
			.unwrap()
			.unwrap()
			.end_at_ms
	};

	let (status, detail, _) = api
		.call_with_headers("GET", &format!("/sandboxes/{id}"), &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK, "{detail}");
	assert_eq!(
		(detail["sandboxID"].as_str(), detail["state"].as_str()),
		(Some(id.as_str()), Some("running"))
	);

	let before = end(id.clone()).await;
	let (status, body, _) = api
		.call_with_headers(
			"POST",
			&format!("/sandboxes/{id}/timeout"),
			&headers,
			Some(json!({"timeout": 1200})),
		)
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
	let after_timeout = end(id.clone()).await;
	assert!(after_timeout > before);
	assert!(
		fake.lock().deadlines.contains(&(id.clone(), after_timeout)),
		"the node got the deadline"
	);
	let (status, body, _) = api
		.call_with_headers(
			"POST",
			&format!("/sandboxes/{id}/refreshes"),
			&headers,
			Some(json!({"duration": 1800})),
		)
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
	assert!(end(id.clone()).await > after_timeout);

	// Never another sandbox, even of the same team.
	let other_before = end(other.clone()).await;
	for (method, suffix, body) in [
		("GET", "", None),
		("POST", "/timeout", Some(json!({"timeout": 1200}))),
		("POST", "/refreshes", Some(json!({"duration": 1800}))),
	] {
		let (status, body, _) = api
			.call_with_headers(method, &format!("/sandboxes/{other}{suffix}"), &headers, body)
			.await;
		assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {suffix}: {body}");
	}
	assert_eq!(end(other.clone()).await, other_before);

	// No other route or method accepts the sandbox credential.
	for (method, uri, body) in [
		("POST", "/sandboxes".to_owned(), Some(json!({"templateID": "my-tpl"}))),
		("POST", "/v2/sandboxes".to_owned(), Some(json!({"templateID": "my-tpl"}))),
		("GET", "/sandboxes".to_owned(), None),
		("GET", "/v2/sandboxes".to_owned(), None),
		("GET", "/sandboxes/metrics".to_owned(), None),
		("DELETE", format!("/sandboxes/{id}"), None),
		("POST", format!("/sandboxes/{id}/pause"), Some(json!({}))),
		("POST", format!("/sandboxes/{id}/resume"), Some(json!({}))),
		("POST", format!("/sandboxes/{id}/connect"), Some(json!({"timeout": 600}))),
		("POST", format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 600}))),
		("POST", format!("/sandboxes/{id}/snapshots"), Some(json!({"name": "keepalive"}))),
		("POST", format!("/sandboxes/{id}/fork"), Some(json!({}))),
		("PUT", format!("/sandboxes/{id}/network"), Some(json!({}))),
		("GET", format!("/sandboxes/{id}/logs"), None),
		("GET", format!("/v2/sandboxes/{id}/logs"), None),
		("GET", format!("/sandboxes/{id}/metrics"), None),
		("GET", "/snapshots".to_owned(), None),
		("GET", "/api-keys".to_owned(), None),
	] {
		let (status, body, _) = api.call_with_headers(method, &uri, &headers, body).await;
		assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}: {body}");
	}

	// Wrong tokens reveal nothing about whether the sandbox exists.
	for target in [id.as_str(), "iunknown00000000000000"] {
		let (status, body, _) = api
			.call_with_headers(
				"GET",
				&format!("/sandboxes/{target}"),
				&[("x-access-token", "wrong")],
				None,
			)
			.await;
		assert_eq!(status, StatusCode::UNAUTHORIZED);
		assert_eq!(body["message"], "Invalid or expired access token");
	}
	let unknown = "iunknown00000000000000";
	let (status, ..) = api
		.call_with_headers(
			"GET",
			&format!("/sandboxes/{unknown}"),
			&[("x-access-token", &state.keys.envd_token(unknown))],
			None,
		)
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// Team credentials keep precedence over the new header.
	let (status, ..) = api
		.call_with_headers(
			"GET",
			&format!("/sandboxes/{id}"),
			&[("x-api-key", &api_key), ("x-access-token", "wrong")],
			None,
		)
		.await;
	assert_eq!(status, StatusCode::OK);
	let (status, ..) = api
		.call_with_headers(
			"GET",
			&format!("/sandboxes/{id}"),
			&[("x-api-key", "malformed"), ("x-access-token", &token)],
			None,
		)
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// Both credentials clamp to the team's per-execution maximum.
	let route = get_route(&state.cluster.kv, &id).await.unwrap().unwrap();
	let max_s = u64::from(h.team.limits.max_length_hours) * 3600;
	let cap = route.started_at_ms + max_s * 1000;
	for credentials in [&[("x-api-key", api_key.as_str())][..], &headers[..]] {
		let (status, body, _) = api
			.call_with_headers(
				"POST",
				&format!("/sandboxes/{id}/timeout"),
				credentials,
				Some(json!({"timeout": max_s + 1})),
			)
			.await;
		assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
		assert_eq!(end(id.clone()).await, cap);
	}

	// Paused: detail works, the timeout is refused like with team auth.
	lifecycle::pause(state, Some(&h.team.id), &id, true)
		.await
		.unwrap();
	let (status, paused, _) = api
		.call_with_headers("GET", &format!("/sandboxes/{id}"), &headers, None)
		.await;
	assert_eq!((status, paused["state"].as_str()), (StatusCode::OK, Some("paused")));
	let (status, ..) = api
		.call_with_headers(
			"POST",
			&format!("/sandboxes/{id}/timeout"),
			&headers,
			Some(json!({"timeout": 600})),
		)
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND, "timeout still refuses paused sandboxes");

	// Killed: the token is dead.
	h.kill(&id).await;
	for (method, suffix, body) in [
		("GET", "", None),
		("POST", "/timeout", Some(json!({"timeout": 600}))),
		("POST", "/refreshes", Some(json!({"duration": 600}))),
	] {
		let (status, body, _) = api
			.call_with_headers(method, &format!("/sandboxes/{id}{suffix}"), &headers, body)
			.await;
		assert_eq!(status, StatusCode::UNAUTHORIZED, "killed {method} {suffix}: {body}");
	}
	h.finish().await;
}
