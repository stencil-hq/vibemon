use std::{
	fs::File,
	io::{Read, Seek, SeekFrom, Write},
};

use vmon_cloud::s3::{PART_BYTES, fake::FakeS3};
use vmon_cluster::NodeId;

use super::*;

fn host() -> HostIdentity {
	HostIdentity {
		node_id:      NodeId(1),
		arch:         "aarch64".into(),
		backend:      "hvf".into(),
		cpu_baseline: "test".into(),
	}
}

fn meta() -> SandboxMeta {
	serde_json::from_value(serde_json::json!({
		"sandboxId": "isandbox1", "teamId": "team1", "templateId": "template1",
		"buildId": "build1", "state": "paused", "vcpus": 1, "memoryMb": 128,
		"diskMb": 64, "startedAtMs": 1, "endAtMs": 2, "envdVersion": "test",
		"envdAccessToken": "secret-token",
		"network": {"allowOut": [], "denyOut": [], "egressProxy": null, "rules": {}},
		"volumeMounts": [], "pid": null, "tap": null, "endedAtMs": null
	}))
	.unwrap()
}

fn snapshot_dir(dir: &Path, memory: usize, fill: u8) {
	fs::create_dir_all(dir).unwrap();
	fs::write(dir.join("current-generation"), b"1").unwrap();
	fs::write(dir.join("vmstate.1.bin"), vec![fill ^ 0xff; 4096]).unwrap();
	let mut file = File::create(dir.join("memory.1.bin")).unwrap();
	file.set_len(memory as u64 * 4).unwrap();
	file.write_all(&vec![fill; memory]).unwrap();
}

/// `paused/<id>/` with a sparse raw disk, a snapshot, and optionally its own
/// (non-template) RAM base.
fn paused_dir(root: &Path, memory: usize, own_base: bool) {
	fs::create_dir_all(root).unwrap();
	let mut rootfs = File::create(root.join("rootfs.img")).unwrap();
	rootfs.set_len(64 << 20).unwrap();
	rootfs.write_all(&[0xaa; 4096]).unwrap();
	rootfs.seek(SeekFrom::Start(32 << 20)).unwrap();
	rootfs.write_all(&[0x55; 8192]).unwrap();
	rootfs.sync_all().unwrap();
	snapshot_dir(&root.join("snapshot"), memory, 0x76);
	if own_base {
		snapshot_dir(&root.join("base"), 8192, 0x42);
	}
}

fn input<'a>(
	paused: &'a Path,
	staging: &'a Path,
	meta: &'a SandboxMeta,
	host: &'a HostIdentity,
) -> UploadInput<'a> {
	UploadInput {
		id: "isandbox1",
		paused_dir: paused,
		staging,
		meta,
		boot_init: "/usr/bin/envd",
		host,
		template_snapshot: None,
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_fetch_and_delete_roundtrip() {
	let fake = FakeS3::start().await;
	let store = Store::new(fake.client("bucket", "store"));
	let dir = tempfile::tempdir().unwrap();
	let paused = dir.path().join("paused");
	let staging = dir.path().join("paused.upload");
	paused_dir(&paused, PART_BYTES + (1 << 20), true);
	let (meta, host) = (meta(), host());
	let remote = store
		.upload(&input(&paused, &staging, &meta, &host), &Cancellation::new())
		.await
		.unwrap();
	assert!(!staging.exists(), "staging is removed");
	assert_eq!(remote.url, store.s3().location().url());
	let generation = store.generation_prefix(&remote).unwrap();
	assert!(generation.starts_with("paused/isandbox1/"));
	let manifest = store.manifest(&remote).await.unwrap();
	assert!(manifest.has_snapshot && manifest.ram_base_build.is_none());
	assert_eq!(manifest.disk_size, 64 << 20);
	let paths: Vec<_> = manifest.files.iter().map(|f| f.path.as_str()).collect();
	assert_eq!(paths, [
		"meta.json",
		"boot-init.json",
		"rootfs.blk",
		"rootfs.blk.json",
		"snapshot/current-generation",
		"snapshot/memory.1.bin",
		"snapshot/vmstate.1.bin",
		"base/current-generation",
		"base/memory.1.bin",
		"base/vmstate.1.bin",
	]);
	let image = &manifest.files[2];
	assert_eq!(image.extents, [[0, image.size]], "the block image is dense");
	let objects = fake.objects();
	let object = |rel: &str| &objects[&format!("bucket/store/{generation}/{rel}")];
	let uploaded: serde_json::Value = serde_json::from_slice(&object("meta.json").data).unwrap();
	assert_eq!(uploaded["envdAccessToken"], "", "the token never leaves the node");
	assert_eq!(object("boot-init.json").data, "\"/usr/bin/envd\"");
	let memory = &manifest.files[5];
	assert!(memory.extents.iter().map(|e| e[1]).sum::<u64>() < memory.size, "memory stays sparse");
	assert_eq!(
		objects.values().map(|o| o.data.len() as u64).sum::<u64>(),
		remote.bytes + object("manifest.json").data.len() as u64
	);
	let last_put = fake
		.requests()
		.into_iter()
		.rfind(|r| r.method == axum::http::Method::PUT && !r.query.contains_key("partNumber"))
		.unwrap();
	assert!(last_put.path.ends_with("/manifest.json"), "the manifest is written last");

	let target = tempfile::tempdir().unwrap();
	let full = target.path().join("full");
	store.fetch(&remote, &full, |_| true).await.unwrap();
	let mut disk = File::open(full.join("rootfs.img")).unwrap();
	assert_eq!(disk.metadata().unwrap().len(), 64 << 20);
	let mut block = [0; 4096];
	disk.read_exact(&mut block).unwrap();
	assert_eq!(block, [0xaa; 4096]);
	disk.seek(SeekFrom::Start(32 << 20)).unwrap();
	disk.read_exact(&mut block).unwrap();
	assert_eq!(block, [0x55; 4096]);
	assert!(!full.join("rootfs.blk").exists() && !full.join("rootfs.blk.json").exists());
	for rel in ["snapshot/memory.1.bin", "snapshot/vmstate.1.bin", "base/memory.1.bin"] {
		assert_eq!(fs::read(full.join(rel)).unwrap(), fs::read(paused.join(rel)).unwrap(), "{rel}");
	}
	let partial = target.path().join("partial");
	store
		.fetch(&remote, &partial, |path| !path.starts_with("snapshot/") && !path.starts_with("base/"))
		.await
		.unwrap();
	assert!(partial.join("rootfs.img").is_file() && !partial.join("snapshot").exists());
	assert!(store.fetch(&remote, &partial, |_| true).await.is_err(), "never merges into state");

	let newer = store
		.upload(&input(&paused, &staging, &meta, &host), &Cancellation::new())
		.await
		.unwrap();
	assert_ne!(newer.manifest, remote.manifest);
	store.delete(&remote).await.unwrap();
	store.delete(&remote).await.unwrap();
	assert!(fake.keys().iter().all(|key| !key.contains(&generation)));
	store.delete(&newer).await.unwrap();
	assert_eq!(fake.keys(), [] as [std::string::String; 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn template_ram_base_is_linked_not_uploaded() {
	let fake = FakeS3::start().await;
	let store = Store::new(fake.client("bucket", ""));
	let dir = tempfile::tempdir().unwrap();
	let template = dir.path().join("template");
	snapshot_dir(&template, 8192, 0x11);
	let paused = dir.path().join("paused");
	paused_dir(&paused, 8192, false);
	fs::create_dir(paused.join("base")).unwrap();
	for name in ["current-generation", "memory.1.bin", "vmstate.1.bin"] {
		fs::hard_link(template.join(name), paused.join("base").join(name)).unwrap();
	}
	let (meta, host) = (meta(), host());
	let staging = dir.path().join("staging");
	let mut upload = input(&paused, &staging, &meta, &host);
	upload.template_snapshot = Some(&template);
	let remote = store.upload(&upload, &Cancellation::new()).await.unwrap();
	let manifest = store.manifest(&remote).await.unwrap();
	assert_eq!(manifest.ram_base_build.as_deref(), Some("build1"));
	assert!(manifest.files.iter().all(|f| !f.path.starts_with("base/")));
	// A copy (not the template's inode) is the sandbox's own base: uploaded.
	fs::remove_file(paused.join("base/memory.1.bin")).unwrap();
	fs::copy(template.join("memory.1.bin"), paused.join("base/memory.1.bin")).unwrap();
	let remote = store.upload(&upload, &Cancellation::new()).await.unwrap();
	let manifest = store.manifest(&remote).await.unwrap();
	assert!(manifest.ram_base_build.is_none());
	assert!(manifest.files.iter().any(|f| f.path == "base/memory.1.bin"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_and_cancelled_uploads_leave_nothing() {
	let dir = tempfile::tempdir().unwrap();
	let paused = dir.path().join("paused");
	let staging = dir.path().join("staging");
	paused_dir(&paused, PART_BYTES + (1 << 20), false);
	let (meta, host) = (meta(), host());
	for mode in ["before", "part", "manifest", "error"] {
		let fake = FakeS3::start().await;
		let store = Store::new(fake.client("bucket", ""));
		let cancel = Cancellation::new();
		let hook_cancel = cancel.clone();
		match mode {
			"before" => cancel.cancel(),
			"part" => fake.set_hook(move |request| {
				if request.query.contains_key("partNumber") {
					hook_cancel.cancel();
				}
				None
			}),
			"manifest" => fake.set_hook(move |request| {
				if request.path.ends_with("/manifest.json") {
					hook_cancel.cancel();
				}
				None
			}),
			_ => fake.set_hook(|request| {
				request
					.path
					.ends_with("/vmstate.1.bin")
					.then_some(axum::http::StatusCode::BAD_REQUEST)
			}),
		}
		let result = store
			.upload(&input(&paused, &staging, &meta, &host), &cancel)
			.await;
		assert!(result.is_err(), "{mode}");
		fake.clear_hook();
		for _ in 0..100 {
			if fake.open_uploads() == 0 {
				break;
			}
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
		assert!(fake.keys().is_empty(), "{mode}: {:?}", fake.keys());
		assert_eq!(fake.open_uploads(), 0, "{mode}");
		assert!(!staging.exists(), "{mode}");
		if mode == "before" {
			assert!(fake.requests().is_empty());
		}
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_objects_and_bad_manifests_are_rejected() {
	let fake = FakeS3::start().await;
	let store = Store::new(fake.client("bucket", "p"));
	let dir = tempfile::tempdir().unwrap();
	let paused = dir.path().join("paused");
	paused_dir(&paused, 8192, false);
	let (meta, host) = (meta(), host());
	let staging = dir.path().join("staging");
	let remote = store
		.upload(&input(&paused, &staging, &meta, &host), &Cancellation::new())
		.await
		.unwrap();
	let generation = store.generation_prefix(&remote).unwrap();
	let key = format!("bucket/p/{generation}/snapshot/vmstate.1.bin");
	let original = fake.objects()[&key].data.clone();
	let target = tempfile::tempdir().unwrap();
	for truncated in [false, true] {
		let mut corrupt = original.to_vec();
		if truncated {
			corrupt.pop();
		} else {
			corrupt[0] ^= 0xff;
		}
		fake.insert(&key, corrupt);
		let dest = target.path().join("paused");
		assert!(store.fetch(&remote, &dest, |_| true).await.is_err());
		assert!(!dest.exists());
		assert_eq!(fs::read_dir(target.path()).unwrap().count(), 0, "stage removed");
	}
	let mut wrong = remote.clone();
	wrong.cpu_baseline = "other".into();
	assert!(store.manifest(&wrong).await.is_err());
	let mut elsewhere = remote.clone();
	elsewhere.url = "s3://other/bucket".into();
	assert!(store.manifest(&elsewhere).await.is_err());
	for manifest in ["paused/isandbox1/not-a-uuid/manifest.json", "paused/../x/y/manifest.json"] {
		let bad = Remote { manifest: manifest.into(), ..remote.clone() };
		assert!(store.manifest(&bad).await.is_err());
	}
	let mut manifest = store.manifest(&remote).await.unwrap();
	manifest.files[4].path = "snapshot/../../escape".into();
	fake.insert(&format!("bucket/p/{}", remote.manifest), serde_json::to_vec(&manifest).unwrap());
	assert!(store.manifest(&remote).await.is_err());
}
