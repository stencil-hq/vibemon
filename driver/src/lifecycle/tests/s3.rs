//! `vmonS3Mounts` through the API: validation, what the node receives, what
//! the store keeps, credential re-resolution on resume, and object-store
//! volume placement.

use vmon_node::api::{S3Mount, S3MountCredentials, S3MountMode};

use super::*;

fn keys(id: &str, secret: &str) -> S3MountCredentials {
	S3MountCredentials {
		access_key_id:     id.into(),
		secret_access_key: secret.into(),
		session_token:     None,
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_mounts_reach_the_node_and_never_leak_credentials() {
	let h = harness().await;
	let Harness { store, fake, state, api, team, .. } = &h;
	let secret_value = |key: &str, secret: &str| {
		state.keys.encrypt(
			json!({"accessKeyId": key, "secretAccessKey": secret})
				.to_string()
				.as_bytes(),
		)
	};
	let secret = store
		.create_secret(
			&team.id,
			"bucket-keys",
			&secret_value("AKSECRET1", "SECRETVALUE1"),
			&BTreeMap::new(),
		)
		.await
		.unwrap();

	let body = |mounts: Value| json!({"templateID": "my-tpl", "vmonS3Mounts": mounts});
	for (bad, why) in [
		(json!([{"path": "/data", "uri": "s3://b", "mode": "rw"}]), "mode"),
		(json!([{"path": "data", "uri": "s3://b"}]), "relative path"),
		(json!([{"path": "/d", "uri": "s3://b"}, {"path": "/d/x", "uri": "s3://c"}]), "overlap"),
		(json!([{"path": "/d", "uri": "s3://b", "credentialsSecret": "missing"}]), "secret"),
		(json!([{"path": "/d", "uri": "s3://b", "secretKey": "x"}]), "unknown field"),
	] {
		let (status, error, _) = api.call("POST", "/v2/sandboxes", Some(body(bad))).await;
		assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {error}");
	}

	let mounts = json!([
		{"path": "/data", "uri": "s3://user-bucket/models", "mode": "writeback",
		 "endpoint": "https://minio.example.com", "region": "eu-west-1",
		 "credentials": {"accessKeyId": "AKINLINE", "secretAccessKey": "TOPSECRET"}},
		{"path": "/public", "uri": "s3://open-data", "credentialsSecret": "bucket-keys"},
		{"path": "/anon", "uri": "https://storage.example.com/b/p", "mode": "overlay"},
	]);
	let (status, created, _) = api.call("POST", "/v2/sandboxes", Some(body(mounts))).await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	let id = created["sandboxID"].as_str().unwrap().to_owned();

	let sent = fake.lock().creates.last().unwrap().s3_mounts.clone();
	let expected = vec![
		S3Mount {
			path:        "/data".into(),
			uri:         "s3://user-bucket/models".into(),
			mode:        S3MountMode::Writeback,
			endpoint:    Some("https://minio.example.com".into()),
			region:      Some("eu-west-1".into()),
			credentials: Some(keys("AKINLINE", "TOPSECRET")),
		},
		S3Mount {
			path:        "/public".into(),
			uri:         "s3://open-data".into(),
			mode:        S3MountMode::Ro,
			endpoint:    None,
			region:      None,
			credentials: Some(keys("AKSECRET1", "SECRETVALUE1")),
		},
		S3Mount {
			path:        "/anon".into(),
			uri:         "https://storage.example.com/b/p".into(),
			mode:        S3MountMode::Overlay,
			endpoint:    None,
			region:      None,
			credentials: None,
		},
	];
	assert_eq!(sent, expected);

	// The store keeps no plaintext secret; the API never returns one.
	let row = store.sandbox(&id).await.unwrap().unwrap();
	let records = serde_json::to_string(&row.s3_mounts).unwrap();
	let stored = serde_json::to_string(&row).unwrap();
	assert!(records.contains("bucket-keys") && records.contains("user-bucket"), "{records}");
	for leak in ["TOPSECRET", "AKINLINE", "SECRETVALUE1"] {
		assert!(!stored.contains(leak), "{stored}");
		assert!(!String::from_utf8_lossy(row.s3_mounts_enc.as_deref().unwrap()).contains(leak));
	}
	let (_, detail, _) = api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert!(!detail.to_string().contains("TOPSECRET"), "{detail}");

	// Pause, rotate the secret, resume: the node gets the inline keys back
	// and the secret's new value.
	let (status, ..) = api
		.call("POST", &format!("/sandboxes/{id}/pause"), Some(json!({})))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	store
		.update_secret(&team.id, &secret.id, &secret_value("AKSECRET2", "SECRETVALUE2"), None)
		.await
		.unwrap()
		.unwrap();
	let (status, body, _) = api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 60})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let resumed = fake.lock().resumes.last().unwrap().1.s3_mounts.clone();
	let mut rotated = expected;
	rotated[1].credentials = Some(keys("AKSECRET2", "SECRETVALUE2"));
	assert_eq!(resumed, rotated);
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_store_volumes_need_an_object_store_driver() {
	let h = harness().await;
	let Harness { store, fake, api, team, .. } = &h;
	// A volume created by an object-store driver: no owning node.
	let volume = store
		.insert_volume(&team.id, "shared", None, true)
		.await
		.unwrap();
	assert!(volume.object_store && volume.node_id.is_none());
	let request =
		json!({"templateID": "my-tpl", "volumeMounts": [{"name": "shared", "path": "/mnt/shared"}]});
	// This driver has no object store: it cannot serve the volume.
	let (status, body, _) = api
		.call("POST", "/v2/sandboxes", Some(request.clone()))
		.await;
	assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
	let creates = fake.lock().creates.clone();
	assert!(creates.is_empty(), "unexpected creates: {creates:?}");
	h.finish().await;
}
