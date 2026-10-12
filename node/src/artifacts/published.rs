//! Builds published in the operator object store (ARCHITECTURE.md §8.5).
//!
//! `builds/<buildID>/` mirrors what a node serves to peers: every manifest
//! file as packed sparse extents under its relative path, and the
//! [`ArtifactManifest`] as `manifest.json`, written last. A node that lacks a
//! build fetches it from there when no live peer serves it, with the same
//! verification as a peer fetch; a published manifest makes the build
//! independent of any node.

use std::pin::Pin;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use vmon_cloud::s3::{PutOptions, S3Client};

use super::{ArtifactMeta, ArtifactStore, MANIFEST, safe_relative, sparse};
use crate::api::ArtifactManifest;

/// Bytes of one fetched artifact file.
pub(super) type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// Largest accepted published manifest.
const MANIFEST_LIMIT: usize = 8 << 20;

/// Object key of `rel` of a published build.
pub fn object_key(build_id: &str, rel: &str) -> String {
	format!("builds/{build_id}/{rel}")
}

impl ArtifactStore {
	/// Uploads a committed local build to the object store unless it is
	/// already published there. Returns whether it is published.
	pub async fn publish(&self, build_id: &str) -> Result<bool> {
		let Some(s3) = &self.inner.object_store else {
			return Ok(false);
		};
		let manifest_key = object_key(build_id, MANIFEST);
		if s3.exists(&manifest_key).await? {
			return Ok(true);
		}
		let Some(manifest) = self.manifest(build_id).await? else {
			bail!("build {build_id} is not local");
		};
		let dir = self.build_dir(build_id)?;
		let started = std::time::Instant::now();
		let mut bytes = 0u64;
		for file in &manifest.files {
			let size = file.extents.iter().map(|extent| extent[1]).sum::<u64>();
			let body =
				sparse::read_extents(dir.join(safe_relative(&file.path)?), file.extents.clone());
			let key = object_key(build_id, &file.path);
			s3.upload_stream(&key, size, body, &PutOptions::default())
				.await
				.with_context(|| format!("uploading {key}"))?;
			bytes += size;
		}
		s3.put(&manifest_key, Bytes::from(serde_json::to_vec(&manifest)?), &PutOptions::default())
			.await
			.with_context(|| format!("uploading {manifest_key}"))?;
		tracing::info!(
			build_id,
			bytes,
			secs = started.elapsed().as_secs_f64(),
			"published build artifacts to the object store"
		);
		Ok(true)
	}

	/// Fetches a published build from the object store.
	pub(super) async fn fetch_published(
		&self,
		build_id: &str,
		s3: &S3Client,
	) -> Result<ArtifactMeta> {
		let body = s3
			.get_bytes(&object_key(build_id, MANIFEST), MANIFEST_LIMIT)
			.await
			.with_context(|| format!("reading the published manifest of {build_id}"))?;
		let manifest: ArtifactManifest = serde_json::from_slice(&body)?;
		if manifest.info.build_id != build_id {
			bail!("published manifest describes build {}", manifest.info.build_id);
		}
		let meta = self
			.install_fetched(build_id, &manifest, |path| {
				let (s3, key) = (s3.clone(), object_key(build_id, path));
				async move {
					let object = s3
						.get(&key)
						.await
						.with_context(|| format!("fetching {key}"))?;
					let stream: ByteStream = Box::pin(object.body.map_err(anyhow::Error::from));
					Ok(stream)
				}
			})
			.await?;
		tracing::info!(build_id, "fetched build artifacts from the object store");
		Ok(meta)
	}
}

/// Removes a published build (template deletion / artifact GC).
pub async fn delete_published(s3: &S3Client, build_id: &str) -> Result<u64> {
	if build_id.is_empty() || build_id.contains('/') {
		bail!("invalid build id {build_id:?}");
	}
	Ok(s3.delete_prefix(&format!("builds/{build_id}/")).await?)
}

#[cfg(test)]
mod tests {
	use vmon_cloud::s3::fake::FakeS3;

	use super::*;
	use crate::artifacts::tests::{committed_build, host_with};

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn published_builds_are_fetched_without_any_peer() {
		let fake = FakeS3::start().await;
		let s3 = fake.client("bucket", "store");
		let (holder_dir, holder) = committed_build("b1", host_with("hvf"), Some(s3.clone())).await;
		assert!(holder.publish("b1").await.unwrap());
		let keys = fake.keys();
		assert!(keys.iter().any(|k| k.ends_with("builds/b1/manifest.json")));
		assert!(keys.iter().any(|k| k.ends_with("builds/b1/rootfs.ext4")));
		let uploads = fake.requests().len();
		assert!(holder.publish("b1").await.unwrap(), "idempotent");
		assert_eq!(fake.requests().len(), uploads + 1, "a published build is only probed");

		// Another node with no peers at all installs it from the store.
		let other_dir = tempfile::tempdir().unwrap();
		let other =
			ArtifactStore::new(other_dir.path(), host_with("hvf"), "s".into(), reqwest::Client::new())
				.unwrap()
				.with_object_store(Some(s3.clone()));
		let meta = other.fetch("b1", &[]).await.unwrap();
		assert!(meta.info.has_snapshot, "compatible snapshot fetched");
		assert_eq!(
			std::fs::read(other_dir.path().join("builds/b1/rootfs.ext4")).unwrap(),
			std::fs::read(holder_dir.path().join("builds/b1/rootfs.ext4")).unwrap()
		);
		// An incompatible host takes the rootfs only.
		let third_dir = tempfile::tempdir().unwrap();
		let third =
			ArtifactStore::new(third_dir.path(), host_with("kvm"), "s".into(), reqwest::Client::new())
				.unwrap()
				.with_object_store(Some(s3.clone()));
		assert!(!third.fetch("b1", &[]).await.unwrap().info.has_snapshot);
		// Corruption is rejected and nothing is installed.
		let key = keys
			.iter()
			.find(|k| k.ends_with("builds/b1/rootfs.ext4"))
			.unwrap();
		fake.insert(key, "corrupt");
		let fourth_dir = tempfile::tempdir().unwrap();
		let fourth = ArtifactStore::new(
			fourth_dir.path(),
			host_with("hvf"),
			"s".into(),
			reqwest::Client::new(),
		)
		.unwrap()
		.with_object_store(Some(s3.clone()));
		assert!(fourth.fetch("b1", &[]).await.is_err());
		assert!(fourth.local_meta("b1").is_none());
		assert!(delete_published(&s3, "b1").await.unwrap() >= 3);
		assert_eq!(fake.keys(), [] as [std::string::String; 0]);
		assert!(fourth.fetch("b1", &[]).await.is_err(), "deleted builds are gone");
	}
}
