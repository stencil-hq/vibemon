//! GPU sandboxes: the request precedence (metadata over the template's
//! `VMON_GPUS`), placement on free GPUs, and filesystem-only pauses and
//! snapshots with cold-boot resumes.

use vmon_cluster::GpuInfo;

use super::*;

/// Makes the (fake) node publish `gpus`.
async fn publish_gpus(h: &Harness, gpus: Vec<GpuInfo>) {
	h.cluster
		.update_meta(|meta| meta.node.as_mut().unwrap().gpus.clone_from(&gpus));
	eventually("GPUs in gossip", || async {
		h.state
			.node_members()
			.iter()
			.any(|(id, meta)| *id == NODE && meta.node.as_ref().is_some_and(|cap| cap.gpus == gpus))
	})
	.await;
}

fn gpu(sandbox: Option<&str>) -> GpuInfo {
	GpuInfo {
		address: "0000:52:00.0".into(),
		model:   "GB205 [GeForce RTX 5070]".into(),
		sandbox: sandbox.map(str::to_owned),
		problem: String::new(),
	}
}

/// Registers the ready template `gpu-tpl`, whose environment asks for one GPU.
async fn gpu_template(h: &Harness) {
	let build_id = uuid::Uuid::new_v4().to_string();
	let registered = h
		.store
		.register_build(&RegisterBuild {
			team_id:               &h.team.id,
			namespace:             &h.team.slug,
			alias:                 "gpu-tpl",
			new_template_id:       "tpl0000000000000000g",
			build_id:              &build_id,
			tags:                  &["default".to_owned()],
			vcpus:                 2,
			memory_mb:             512,
			disk_mb:               2048,
			min_free_disk_mb:      None,
			max_concurrent_builds: 10,
		})
		.await
		.unwrap();
	assert!(matches!(registered, RegisterOutcome::Registered { .. }), "{registered:?}");
	let mut info = artifact(&build_id);
	info.env.insert("VMON_GPUS".into(), "1".into());
	assert!(h.store.finish_build_ready(&build_id, &info).await.unwrap());
	h.cluster
		.kv
		.put(&art_key(&build_id, NODE), serde_json::to_vec(&info).unwrap(), None)
		.await
		.unwrap();
}

async fn create_with(h: &Harness, body: Value) -> (StatusCode, Value) {
	let (status, body, _) = h.api.call("POST", "/v2/sandboxes", Some(body)).await;
	(status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gpu_requests_follow_metadata_then_template_env_and_need_free_gpus() {
	let h = harness().await;
	gpu_template(&h).await;

	// Invalid counts are the request's fault.
	let (status, body) =
		create_with(&h, json!({"templateID": "my-tpl", "metadata": {"vmon.gpus": "two"}})).await;
	assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
	assert!(body["message"].as_str().unwrap().contains("vmon.gpus"), "{body}");

	// No node has a GPU at all: incompatible, not merely busy.
	let (status, body) = create_with(&h, json!({"templateID": "gpu-tpl"})).await;
	assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
	assert_eq!(body["error_code"], "sandbox_no_compatible_node");

	// Its only GPU is held by another sandbox: no capacity right now.
	publish_gpus(&h, vec![gpu(Some("iother"))]).await;
	let (status, body) = create_with(&h, json!({"templateID": "gpu-tpl"})).await;
	assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
	assert_eq!(body["error_code"], "sandbox_capacity_unavailable");

	// Metadata opts the GPU template out; CPU-only creates ignore GPUs.
	let (status, body) =
		create_with(&h, json!({"templateID": "gpu-tpl", "metadata": {"vmon.gpus": "0"}})).await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let cpu_only = body["sandboxID"].as_str().unwrap().to_owned();
	{
		let fake = h.fake.lock();
		let req = fake.creates.last().unwrap();
		assert_eq!((req.gpus, req.cold_boot), (0, false));
	}
	h.kill(&cpu_only).await;

	// The template default applies once the GPU is free; the create cold-boots.
	publish_gpus(&h, vec![gpu(None)]).await;
	let (status, body) = create_with(&h, json!({"templateID": "gpu-tpl"})).await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let id = body["sandboxID"].as_str().unwrap().to_owned();
	{
		let fake = h.fake.lock();
		let req = fake.creates.last().unwrap();
		assert_eq!((req.gpus, req.cold_boot), (1, true));
	}
	assert_eq!(h.store.sandbox(&id).await.unwrap().unwrap().gpus, 1);
	let (status, listed, _) = h
		.api
		.call("GET", &format!("/admin/sandboxes?id={id}"), None)
		.await;
	assert_eq!(status, StatusCode::OK, "{listed}");
	assert_eq!(listed[0]["gpus"], 1, "{listed}");
	h.kill(&id).await;

	// Metadata overrides a CPU-only template.
	let (status, body) =
		create_with(&h, json!({"templateID": "my-tpl", "metadata": {"vmon.gpus": "1"}})).await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	assert_eq!(h.fake.lock().creates.last().unwrap().gpus, 1);
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gpu_sandboxes_pause_and_snapshot_filesystem_only_and_resume_cold() {
	let h = harness().await;
	publish_gpus(&h, vec![gpu(None)]).await;
	let gpus = json!({"vmon.gpus": "1"});

	// A memory pause request degrades to a filesystem-only pause.
	let (status, body) =
		create_with(&h, json!({"templateID": "my-tpl", "metadata": gpus.clone()})).await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let id = body["sandboxID"].as_str().unwrap().to_owned();
	let (status, body, _) = h
		.api
		.call("POST", &format!("/sandboxes/{id}/snapshots"), Some(json!({"name": "gpu-snap"})))
		.await;
	assert!(status.is_success(), "{status} {body}");
	assert_eq!(h.fake.lock().snapshot_memory, vec![false]);
	let (status, ..) = h
		.api
		.call("POST", &format!("/sandboxes/{id}/pause"), Some(json!({})))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	assert_eq!(h.fake.lock().pauses, vec![(id.clone(), false)]);

	// Resuming cold-boots and re-reserves the GPUs, even when asked to keep
	// memory.
	let (status, body, _) = h
		.api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	{
		let fake = h.fake.lock();
		let (resumed, req) = fake.resumes.last().unwrap();
		assert_eq!(resumed, &id);
		assert_eq!((req.gpus, req.cold_boot), (1, true));
	}
	h.kill(&id).await;

	// autoPause keeps working (filesystem-only), and autoResume stays allowed.
	let (status, body) = create_with(
		&h,
		json!({"templateID": "my-tpl", "timeout": 1, "autoPause": true,
			"autoResume": {"enabled": true}, "metadata": gpus}),
	)
	.await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	let id = body["sandboxID"].as_str().unwrap().to_owned();
	eventually("filesystem-only auto-pause", || async {
		h.fake.lock().pauses.last() == Some(&(id.clone(), false))
	})
	.await;
	let row = h.store.sandbox(&id).await.unwrap().unwrap();
	assert!(row.auto_pause_memory && row.auto_resume, "requested flags are kept as asked");
	h.finish().await;
}
