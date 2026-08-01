//! Disk-only recovery artifact format.
//!
//! A disk recovery point intentionally contains only a coherent root block
//! image.  It is not a VM snapshot: it never contains RAM, CPU, transport, or
//! device state and is restored by cold-booting with `rootfs.img`.

use std::{
	fs::{self, OpenOptions},
	io::Write,
	path::{Path, PathBuf},
	sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::{
	result::{Result, err},
	virtio::DiskCapture,
};

pub const DISK_DIR: &str = "disk";
pub const ROOTFS_IMAGE: &str = "rootfs.img";
pub const MANIFEST: &str = "manifest.json";
const VERSION: u32 = 1;
static NEXT_STAGING: AtomicU64 = AtomicU64::new(0);

/// The publicly consumable description of a disk-only recovery artifact.
///
/// `rootfs` is relative to this manifest's parent; callers must not use an
/// untrusted absolute path from it.
#[derive(Debug, Deserialize, Serialize)]
pub struct DiskManifest {
	pub version: u32,
	pub rootfs:  String,
	pub bytes:   u64,
	pub capture: String,
	pub boot:    String,
}

impl DiskManifest {
	fn from_capture(capture: DiskCapture) -> Self {
		Self {
			version: VERSION,
			rootfs:  ROOTFS_IMAGE.to_owned(),
			bytes:   capture.bytes,
			capture: capture.method.as_str().to_owned(),
			boot:    "cold".to_owned(),
		}
	}
}

/// Atomically publish a disk-only point below `snapshot_dir`.
///
/// The caller supplies the clone operation while the virtio-block path is
/// quiesced.  A failed clone removes its private temporary directory and never
/// replaces an existing published point.
pub fn write_disk_point<F>(snapshot_dir: &Path, capture: F) -> Result<DiskManifest>
where
	F: FnOnce(&Path) -> Result<DiskCapture>,
{
	fs::create_dir_all(snapshot_dir)
		.map_err(|e| err(format!("creating disk snapshot parent {}: {e}", snapshot_dir.display())))?;
	let final_dir = snapshot_dir.join(DISK_DIR);
	if final_dir.exists() {
		return Err(err(format!("disk recovery artifact {} already exists", final_dir.display())));
	}

	let temporary = temporary_dir(snapshot_dir);
	fs::create_dir(&temporary).map_err(|e| {
		err(format!("creating disk snapshot staging dir {}: {e}", temporary.display()))
	})?;
	let result = (|| {
		let image = temporary.join(ROOTFS_IMAGE);
		let capture = capture(&image)?;
		let manifest = DiskManifest::from_capture(capture);
		write_manifest(&temporary.join(MANIFEST), &manifest)?;
		sync_dir(&temporary)?;
		fs::rename(&temporary, &final_dir).map_err(|e| {
			err(format!("publishing disk recovery artifact {}: {e}", final_dir.display()))
		})?;
		sync_dir(snapshot_dir)?;
		Ok(manifest)
	})();
	if result.is_err() {
		let _ = fs::remove_dir_all(&temporary);
	}
	result
}

/// Remove a point published by this process after its worker thaw failed.
/// The caller only invokes this after it created this unique point, so no
/// existing recovery point is ever overwritten or removed.
pub fn remove_disk_point(snapshot_dir: &Path) {
	let _ = fs::remove_dir_all(snapshot_dir.join(DISK_DIR));
}

fn temporary_dir(parent: &Path) -> PathBuf {
	loop {
		let sequence = NEXT_STAGING.fetch_add(1, Ordering::Relaxed);
		let candidate = parent.join(format!(".{DISK_DIR}.{}.{}.tmp", std::process::id(), sequence));
		if !candidate.exists() {
			return candidate;
		}
	}
}

fn write_manifest(path: &Path, manifest: &DiskManifest) -> Result<()> {
	let bytes = serde_json::to_vec(manifest)
		.map_err(|e| err(format!("serializing disk recovery manifest: {e}")))?;
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)
		.map_err(|e| err(format!("creating disk recovery manifest {}: {e}", path.display())))?;
	file
		.write_all(&bytes)
		.map_err(|e| err(format!("writing disk recovery manifest {}: {e}", path.display())))?;
	file
		.write_all(b"\n")
		.map_err(|e| err(format!("terminating disk recovery manifest {}: {e}", path.display())))?;
	file
		.sync_all()
		.map_err(|e| err(format!("syncing disk recovery manifest {}: {e}", path.display())))
}

#[cfg_attr(
	not(unix),
	allow(clippy::unnecessary_wraps, reason = "Windows has no portable directory fsync")
)]
fn sync_dir(path: &Path) -> Result<()> {
	#[cfg(unix)]
	{
		fs::File::open(path)
			.and_then(|file| file.sync_all())
			.map_err(|e| err(format!("syncing directory {}: {e}", path.display())))?;
	}
	#[cfg(not(unix))]
	let _ = path;
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{
		path::{Path, PathBuf},
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::*;
	use crate::virtio::{DiskCapture, DiskCaptureMethod};

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	struct TestDir {
		path: PathBuf,
	}

	impl TestDir {
		fn new() -> Self {
			let path = std::env::temp_dir().join(format!(
				"vmon-disk-snapshot-test-{}-{}",
				std::process::id(),
				NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
			));
			fs::create_dir(&path).unwrap();
			Self { path }
		}

		fn path(&self) -> &Path {
			&self.path
		}
	}

	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = fs::remove_dir_all(&self.path);
		}
	}

	#[test]
	fn point_contains_only_disk_artifact_and_manifest() {
		let tmp = TestDir::new();
		let point = tmp.path().join("point");
		let manifest = write_disk_point(&point, |image| {
			fs::write(image, b"before-boundary")?;
			Ok(DiskCapture { bytes: 15, method: DiskCaptureMethod::SparseCopy })
		})
		.unwrap();

		assert_eq!(manifest.rootfs, ROOTFS_IMAGE);
		assert_eq!(manifest.boot, "cold");
		let disk = point.join(DISK_DIR);
		assert!(disk.join(ROOTFS_IMAGE).is_file());
		assert!(disk.join(MANIFEST).is_file());
		assert!(!disk.join("memory.1.bin").exists());
		assert!(!disk.join("vmstate.1.bin").exists());
		assert_eq!(fs::read_dir(&disk).unwrap().count(), 2);
	}

	#[test]
	fn failed_capture_leaves_no_published_or_staging_artifact() {
		let tmp = TestDir::new();
		let point = tmp.path().join("point");
		let error = write_disk_point(&point, |_| Err(err("injected clone failure"))).unwrap_err();
		assert!(error.to_string().contains("injected clone failure"));
		assert!(!point.join(DISK_DIR).exists());
		assert_eq!(fs::read_dir(&point).unwrap().count(), 0);
	}

	#[test]
	fn point_excludes_post_boundary_writes() {
		let tmp = TestDir::new();
		let source = tmp.path().join("source.img");
		let point = tmp.path().join("point");
		let mut source_file = fs::File::create(&source).unwrap();
		source_file.write_all(b"before").unwrap();
		source_file.sync_all().unwrap();
		write_disk_point(&point, |image| {
			fs::copy(&source, image)?;
			Ok(DiskCapture { bytes: 6, method: DiskCaptureMethod::SparseCopy })
		})
		.unwrap();
		fs::write(&source, b"after!").unwrap();
		assert_eq!(fs::read(point.join(DISK_DIR).join(ROOTFS_IMAGE)).unwrap(), b"before");
	}
}
