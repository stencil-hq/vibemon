//! One S3-backed guest mount: answers the remote virtio-fs proxy protocol
//! (`vmm::remotefs::proto`) for its tag according to its mode.

use std::{sync::Arc, time::Duration};

use vmm::remotefs::proto;

use super::{
	bucket::{Attrs, Bucket, FsError, FsResult, Kind, Stat, check_path, parent, unix_now},
	stage::{SYNC_BUDGET, Stage},
};
use crate::api::S3MountMode;

/// Metadata TTL of read-only and overlay mounts (matches the device's 5 s
/// entry/attribute timeout).
pub const RO_TTL: Duration = Duration::from_secs(5);
/// Metadata TTL of write-back mounts: other sandboxes may change objects.
pub const RW_TTL: Duration = Duration::from_secs(1);
/// Permissions shown for objects without POSIX metadata in writable views.
const DEFAULT_DIR_PERM: u32 = 0o777;
const DEFAULT_FILE_PERM: u32 = 0o666;

/// A mounted prefix and its mode.
#[derive(Debug)]
pub struct Mount {
	bucket: Arc<Bucket>,
	mode:   S3MountMode,
	stage:  Option<Arc<Stage>>,
}

impl Mount {
	/// A mount of `bucket`; write-back mounts stage under `stage_dir`.
	///
	/// # Errors
	///
	/// Fails when the staging directory cannot be created.
	pub fn new(
		bucket: Arc<Bucket>,
		mode: S3MountMode,
		stage_dir: &std::path::Path,
	) -> std::io::Result<Self> {
		let stage = match mode {
			S3MountMode::Writeback => {
				Some(Arc::new(Stage::new(Arc::clone(&bucket), stage_dir.to_owned())?))
			},
			S3MountMode::Ro | S3MountMode::Overlay => None,
		};
		Ok(Self { bucket, mode, stage })
	}

	/// The mount's mode.
	pub const fn mode(&self) -> S3MountMode {
		self.mode
	}

	/// Uploads every dirty staged file and waits (pause, kill).
	///
	/// # Errors
	///
	/// Returns the first upload failure.
	pub async fn sync_all(&self) -> FsResult<()> {
		match &self.stage {
			Some(stage) => stage.sync_all().await,
			None => Ok(()),
		}
	}

	fn writable(&self) -> FsResult<&Arc<Stage>> {
		self.stage.as_ref().ok_or(FsError::ReadOnly)
	}

	/// Wire attributes of `stat` under this mount's presentation policy:
	/// `ro` keeps the device defaults (`0555`/`0444`, root); `overlay` shows
	/// everything world-writable so a guest overlay accepts any user's
	/// writes; `writeback` shows stored POSIX metadata (world-writable
	/// defaults for objects without any).
	fn present(&self, stat: &Stat) -> proto::StatReply {
		let default = if stat.kind == Kind::Dir {
			DEFAULT_DIR_PERM
		} else {
			DEFAULT_FILE_PERM
		};
		let (mode, uid, gid) = match self.mode {
			S3MountMode::Ro => (None, None, None),
			S3MountMode::Overlay => (Some(default), Some(0), Some(0)),
			S3MountMode::Writeback => (
				Some(stat.attrs.perm.unwrap_or(default)),
				Some(stat.attrs.uid.unwrap_or(0)),
				Some(stat.attrs.gid.unwrap_or(0)),
			),
		};
		proto::StatReply {
			kind: if stat.kind == Kind::Dir {
				proto::Kind::Dir
			} else {
				proto::Kind::File
			},
			size: stat.size,
			mtime: stat.mtime,
			etag: stat.etag.clone(),
			mode,
			uid,
			gid,
		}
	}

	async fn lookup(&self, path: &str) -> FsResult<Option<Stat>> {
		check_path(path)?;
		if let Some(stage) = &self.stage
			&& let Some(stat) = stage.stat(path)
		{
			return Ok(Some(stat));
		}
		// Read-only views need no user metadata: a fresh listing suffices.
		self.bucket.stat(path, self.stage.is_none()).await
	}

	async fn require(&self, path: &str) -> FsResult<Stat> {
		self.lookup(path).await?.ok_or(FsError::NotFound)
	}

	async fn require_dir(&self, path: &str) -> FsResult<()> {
		match self.lookup(path).await? {
			Some(stat) if stat.kind == Kind::Dir => Ok(()),
			Some(_) => Err(FsError::NotDir),
			None => Err(FsError::NotFound),
		}
	}

	/// `stat`.
	pub async fn stat(&self, path: &str) -> FsResult<proto::StatReply> {
		Ok(self.present(&self.require(path).await?))
	}

	/// `list`: object listing merged with staged files.
	pub async fn list(&self, path: &str) -> FsResult<proto::ListReply> {
		let listed = self.bucket.list(path).await?;
		let mut entries: Vec<proto::Entry> = listed
			.iter()
			.map(|entry| {
				let stat = Stat {
					kind:   entry.kind,
					size:   entry.size,
					mtime:  entry.mtime,
					etag:   entry.etag.clone(),
					attrs:  Attrs::default(),
					marker: false,
				};
				let reply = self.present(&stat);
				// Listings carry no user metadata: a write-back device must
				// fetch real attributes before trusting an entry.
				let unknown = self.mode == S3MountMode::Writeback;
				proto::Entry {
					name:  entry.name.clone(),
					kind:  reply.kind,
					size:  reply.size,
					mtime: reply.mtime,
					mode:  if unknown { None } else { reply.mode },
					uid:   if unknown { None } else { reply.uid },
					gid:   if unknown { None } else { reply.gid },
				}
			})
			.collect();
		if let Some(stage) = &self.stage {
			for (name, stat) in stage.children(path) {
				let reply = self.present(&stat);
				let entry = proto::Entry {
					name:  name.clone(),
					kind:  reply.kind,
					size:  reply.size,
					mtime: reply.mtime,
					mode:  reply.mode,
					uid:   reply.uid,
					gid:   reply.gid,
				};
				match entries.iter_mut().find(|listed| listed.name == name) {
					Some(listed) => *listed = entry,
					None => entries.push(entry),
				}
			}
			entries.sort_by(|left, right| left.name.cmp(&right.name));
		}
		Ok(proto::ListReply { entries })
	}

	/// `read`.
	pub async fn read(&self, path: &str, offset: u64, len: u32) -> FsResult<Vec<u8>> {
		check_path(path)?;
		if let Some(stage) = &self.stage
			&& let Some(read) = stage.read(path, offset, len).await
		{
			return read;
		}
		for attempt in 0..2 {
			let stat = self.require(path).await?;
			if stat.kind == Kind::Dir {
				return Err(FsError::IsDir);
			}
			let stat = if stat.etag.is_none() && attempt == 0 {
				// Listing-derived stats may lack an ETag on some services.
				self
					.bucket
					.stat(path, false)
					.await?
					.ok_or(FsError::NotFound)?
			} else {
				stat
			};
			match self.bucket.read(path, &stat, offset, len).await {
				Err(FsError::Stale) if attempt == 0 => self.bucket.invalidate(path),
				other => return other,
			}
		}
		Err(FsError::Stale)
	}

	/// `create`.
	pub async fn create(
		&self,
		path: &str,
		attrs: Attrs,
		excl: bool,
		trunc: bool,
	) -> FsResult<proto::StatReply> {
		let stage = self.writable()?;
		match self.lookup(path).await? {
			Some(stat) if stat.kind == Kind::Dir => Err(FsError::IsDir),
			Some(_) if excl => Err(FsError::Exists),
			Some(stat) => {
				if trunc {
					stage.truncate(path, 0).await?;
					return self.stat(path).await;
				}
				Ok(self.present(&stat))
			},
			None => {
				self.require_dir(parent(path)).await?;
				Ok(self.present(&stage.create(path, attrs)?))
			},
		}
	}

	/// `mkdir`.
	pub async fn mkdir(&self, path: &str, attrs: Attrs) -> FsResult<proto::StatReply> {
		self.writable()?;
		check_path(path)?;
		if path.is_empty() || self.lookup(path).await?.is_some() {
			return Err(FsError::Exists);
		}
		self.require_dir(parent(path)).await?;
		let attrs = Attrs { mtime: Some(unix_now()), ..attrs };
		self.bucket.put_marker(path, attrs).await?;
		let stat =
			Stat { kind: Kind::Dir, size: 0, mtime: unix_now(), etag: None, attrs, marker: true };
		self.bucket.remember(path, Some(stat.clone()));
		Ok(self.present(&stat))
	}

	/// `write`.
	pub async fn write(&self, path: &str, offset: u64, data: &[u8]) -> FsResult<proto::WriteReply> {
		check_path(path)?;
		let written = self.writable()?.write(path, offset, data).await?;
		Ok(proto::WriteReply { written })
	}

	/// `setattr`.
	pub async fn setattr(
		&self,
		path: &str,
		size: Option<u64>,
		update: Attrs,
	) -> FsResult<proto::StatReply> {
		let stage = self.writable()?;
		let stat = self.require(path).await?;
		if path.is_empty() {
			// The root is not an object: its attributes are fixed.
			return Ok(self.present(&stat));
		}
		if stat.kind == Kind::Dir {
			if size.is_some() {
				return Err(FsError::IsDir);
			}
			let attrs = stat.attrs.merge(update);
			self.bucket.put_marker(path, attrs).await?;
			let stat = Stat { attrs, marker: true, mtime: attrs.mtime.unwrap_or(stat.mtime), ..stat };
			self.bucket.remember(path, Some(stat.clone()));
			return Ok(self.present(&stat));
		}
		if let Some(size) = size {
			stage.truncate(path, size).await?;
		}
		if update != Attrs::default() && stage.set_attrs_if_dirty(path, update).is_none() {
			// Clean file: rewrite the object's metadata in place.
			let attrs = stat.attrs.merge(update);
			let attrs = Attrs { mtime: attrs.mtime.or(Some(stat.mtime)), ..attrs };
			let etag = self.bucket.set_metadata(path, attrs).await?;
			stage.metadata_rewritten(path, update, &etag);
			self.bucket.invalidate(path);
		}
		self.stat(path).await
	}

	/// `sync` (`close`, `fsync`, release).
	pub async fn sync(&self, path: &str, release: bool) -> FsResult<()> {
		let Some(stage) = &self.stage else {
			return Ok(());
		};
		if release {
			stage.release(path).await
		} else {
			stage.sync(path, Some(SYNC_BUDGET)).await
		}
	}

	/// `unlink`.
	pub async fn unlink(&self, path: &str) -> FsResult<()> {
		let stage = self.writable()?;
		let stat = self.require(path).await?;
		if stat.kind == Kind::Dir {
			return Err(FsError::IsDir);
		}
		self.bucket.ensure_marker(parent(path)).await?;
		stage.discard(path).await;
		self.bucket.client().delete(path).await?;
		self.bucket.invalidate(path);
		self.bucket.remember(path, None);
		Ok(())
	}

	/// `rmdir`.
	pub async fn rmdir(&self, path: &str) -> FsResult<()> {
		let stage = self.writable()?;
		if path.is_empty() {
			return Err(FsError::Perm);
		}
		let stat = self.require(path).await?;
		if stat.kind != Kind::Dir {
			return Err(FsError::NotDir);
		}
		if stage.has_below(path) || self.bucket.has_children(path).await? {
			return Err(FsError::NotEmpty);
		}
		self.bucket.ensure_marker(parent(path)).await?;
		self
			.bucket
			.client()
			.delete(&super::bucket::dir_prefix(path))
			.await?;
		self.bucket.invalidate(path);
		self.bucket.remember(path, None);
		Ok(())
	}

	/// `rename`: server-side copy + delete (directories key by key, up to
	/// [`super::bucket::MAX_RENAME_KEYS`]); not atomic for directories.
	pub async fn rename(&self, from: &str, to: &str, noreplace: bool) -> FsResult<()> {
		let stage = self.writable()?;
		if from.is_empty() || to.is_empty() {
			return Err(FsError::Perm);
		}
		let source = self.require(from).await?;
		let target = self.lookup(to).await?;
		self.require_dir(parent(to)).await?;
		if let Some(target) = &target {
			if noreplace {
				return Err(FsError::Exists);
			}
			match (source.kind, target.kind) {
				(Kind::File, Kind::Dir) => return Err(FsError::IsDir),
				(Kind::Dir, Kind::File) => return Err(FsError::NotDir),
				(Kind::Dir, Kind::Dir) => {
					if stage.has_below(to) || self.bucket.has_children(to).await? {
						return Err(FsError::NotEmpty);
					}
				},
				(Kind::File, Kind::File) => {},
			}
		}
		self.bucket.ensure_marker(parent(from)).await?;
		let client = self.bucket.client();
		if source.kind == Kind::File {
			stage.sync(from, None).await?;
			stage.discard(from).await;
			client.copy(from, to, None).await?;
			client.delete(from).await?;
		} else {
			stage.sync_below(from).await?;
			let keys = self
				.bucket
				.keys_below(from, super::bucket::MAX_RENAME_KEYS)
				.await?;
			if target.is_some() {
				client.delete(&super::bucket::dir_prefix(to)).await?;
			}
			let from_prefix = super::bucket::dir_prefix(from);
			for key in &keys {
				let rest = key.strip_prefix(&from_prefix).unwrap_or(key);
				client.copy(key, &format!("{to}/{rest}"), None).await?;
			}
			if !source.marker {
				// An implicit source keeps existing through its children; the
				// target gets a marker so an empty result stays a directory.
				self.bucket.put_marker(to, source.attrs).await?;
			}
			for key in &keys {
				client.delete(key).await?;
			}
		}
		self.bucket.invalidate_all();
		Ok(())
	}

	/// Answers one decoded proxy request (the tag was already routed).
	pub async fn handle(&self, request: proto::Request, data: &[u8]) -> FsResult<Reply> {
		use proto::Request as R;
		Ok(match request {
			R::Stat { path, .. } => Reply::Json(json(&self.stat(&path).await?)),
			R::List { path, .. } => Reply::Json(json(&self.list(&path).await?)),
			R::Read { path, offset, len, .. } => Reply::Data(self.read(&path, offset, len).await?),
			R::Create { path, mode, uid, gid, excl, trunc, .. } => {
				let attrs =
					Attrs { perm: Some(mode), uid: Some(uid), gid: Some(gid), mtime: None };
				Reply::Json(json(&self.create(&path, attrs, excl, trunc).await?))
			},
			R::Mkdir { path, mode, uid, gid, .. } => {
				let attrs =
					Attrs { perm: Some(mode), uid: Some(uid), gid: Some(gid), mtime: None };
				Reply::Json(json(&self.mkdir(&path, attrs).await?))
			},
			R::Write { path, offset, .. } => {
				Reply::Json(json(&self.write(&path, offset, data).await?))
			},
			R::Setattr { path, size, mode, uid, gid, mtime, .. } => {
				let update = Attrs { perm: mode, uid, gid, mtime };
				Reply::Json(json(&self.setattr(&path, size, update).await?))
			},
			R::Sync { path, release, .. } => {
				self.sync(&path, release).await?;
				Reply::Json(b"{}".to_vec())
			},
			R::Unlink { path, .. } => {
				self.unlink(&path).await?;
				Reply::Json(b"{}".to_vec())
			},
			R::Rmdir { path, .. } => {
				self.rmdir(&path).await?;
				Reply::Json(b"{}".to_vec())
			},
			R::Rename { from, to, noreplace, .. } => {
				check_path(&from)?;
				check_path(&to)?;
				self.rename(&from, &to, noreplace).await?;
				Reply::Json(b"{}".to_vec())
			},
		})
	}
}

/// A successful proxy reply.
#[derive(Debug)]
pub enum Reply {
	/// `OK_JSON` payload.
	Json(Vec<u8>),
	/// `OK_DATA` payload.
	Data(Vec<u8>),
}

fn json<T: serde::Serialize>(value: &T) -> Vec<u8> {
	serde_json::to_vec(value).unwrap_or_default()
}

/// The tag of a proxy request.
pub fn request_tag(request: &proto::Request) -> &str {
	use proto::Request as R;
	match request {
		R::Stat { tag, .. }
		| R::List { tag, .. }
		| R::Read { tag, .. }
		| R::Create { tag, .. }
		| R::Mkdir { tag, .. }
		| R::Write { tag, .. }
		| R::Setattr { tag, .. }
		| R::Sync { tag, .. }
		| R::Unlink { tag, .. }
		| R::Rmdir { tag, .. }
		| R::Rename { tag, .. } => tag,
	}
}

#[cfg(test)]
mod tests {
	use vmon_cloud::s3::fake::FakeS3;

	use super::*;

	fn mount(fake: &FakeS3, mode: S3MountMode, dir: &std::path::Path) -> Mount {
		let ttl = if mode == S3MountMode::Writeback {
			RW_TTL
		} else {
			RO_TTL
		};
		let bucket = Arc::new(Bucket::new(fake.client("bkt", "m"), ttl, 1 << 20));
		Mount::new(bucket, mode, &dir.join("stage")).unwrap()
	}

	fn user(perm: u32) -> Attrs {
		Attrs { perm: Some(perm), uid: Some(1000), gid: Some(1000), mtime: None }
	}

	#[tokio::test]
	async fn presentation_policy_per_mode() {
		let fake = FakeS3::start().await;
		fake.insert("bkt/m/f", "x");
		let dir = tempfile::tempdir().unwrap();
		let ro = mount(&fake, S3MountMode::Ro, dir.path())
			.stat("f")
			.await
			.unwrap();
		assert_eq!((ro.mode, ro.uid), (None, None));
		let overlay = mount(&fake, S3MountMode::Overlay, dir.path());
		assert_eq!(overlay.stat("").await.unwrap().mode, Some(0o777));
		assert_eq!(overlay.stat("f").await.unwrap().mode, Some(0o666));
		assert_eq!(overlay.mkdir("d", user(0o755)).await.unwrap_err(), FsError::ReadOnly);
		let rw = mount(&fake, S3MountMode::Writeback, dir.path());
		assert_eq!(rw.stat("f").await.unwrap().mode, Some(0o666));
		let listed = rw.list("").await.unwrap();
		assert_eq!(listed.entries[0].mode, None, "write-back listings defer attributes");
	}

	#[tokio::test]
	async fn writeback_posix_flow_lands_in_the_bucket() {
		let fake = FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		let m = mount(&fake, S3MountMode::Writeback, dir.path());

		m.mkdir("docs", user(0o755)).await.unwrap();
		assert!(fake.keys().contains(&"bkt/m/docs/".to_owned()));
		assert_eq!(m.mkdir("docs", user(0o755)).await.unwrap_err(), FsError::Exists);
		assert_eq!(m.mkdir("no/such", user(0o755)).await.unwrap_err(), FsError::NotFound);

		let created = m
			.create("docs/a.txt", user(0o644), true, false)
			.await
			.unwrap();
		assert_eq!((created.mode, created.uid), (Some(0o644), Some(1000)));
		assert_eq!(
			m.create("docs/a.txt", user(0o644), true, false)
				.await
				.unwrap_err(),
			FsError::Exists
		);
		m.write("docs/a.txt", 0, b"hello").await.unwrap();
		// Visible in its own listing before upload.
		let listed = m.list("docs").await.unwrap();
		assert_eq!((listed.entries[0].name.as_str(), listed.entries[0].size), ("a.txt", 5));
		m.sync("docs/a.txt", true).await.unwrap();
		assert_eq!(fake.objects()["bkt/m/docs/a.txt"].data.as_ref(), b"hello");

		// Append after release re-stages lazily.
		m.write("docs/a.txt", 5, b" world").await.unwrap();
		m.sync("docs/a.txt", false).await.unwrap();
		assert_eq!(fake.objects()["bkt/m/docs/a.txt"].data.as_ref(), b"hello world");
		m.sync("docs/a.txt", true).await.unwrap();

		// chmod of a clean file rewrites metadata in place.
		let changed = m.setattr("docs/a.txt", None, Attrs { perm: Some(0o600), ..Attrs::default() });
		assert_eq!(changed.await.unwrap().mode, Some(0o600));
		assert_eq!(fake.objects()["bkt/m/docs/a.txt"].metadata["mode"], (0o100_600).to_string());
		assert_eq!(fake.objects()["bkt/m/docs/a.txt"].data.as_ref(), b"hello world");

		let truncated = m
			.setattr("docs/a.txt", Some(5), Attrs::default())
			.await
			.unwrap();
		assert_eq!(truncated.size, 5);
		m.sync("docs/a.txt", true).await.unwrap();
		assert_eq!(fake.objects()["bkt/m/docs/a.txt"].data.as_ref(), b"hello");

		assert_eq!(m.rmdir("docs").await.unwrap_err(), FsError::NotEmpty);
		m.rename("docs", "archive", false).await.unwrap();
		let keys = fake.keys();
		assert!(keys.contains(&"bkt/m/archive/a.txt".to_owned()), "{keys:?}");
		assert!(!keys.iter().any(|key| key.starts_with("bkt/m/docs")), "{keys:?}");
		assert_eq!(m.read("archive/a.txt", 0, 64).await.unwrap(), b"hello");

		m.create("archive/b.txt", user(0o644), true, false)
			.await
			.unwrap();
		m.write("archive/b.txt", 0, b"b").await.unwrap();
		m.rename("archive/b.txt", "archive/a.txt", false)
			.await
			.unwrap();
		assert_eq!(fake.objects()["bkt/m/archive/a.txt"].data.as_ref(), b"b");
		assert_eq!(
			m.rename("archive/a.txt", "archive", false)
				.await
				.unwrap_err(),
			FsError::IsDir
		);

		m.unlink("archive/a.txt").await.unwrap();
		assert_eq!(m.stat("archive").await.unwrap().kind, proto::Kind::Dir, "parent kept");
		m.rmdir("archive").await.unwrap();
		assert!(fake.keys().is_empty(), "{:?}", fake.keys());
	}

	#[tokio::test]
	async fn deleting_the_last_child_keeps_an_implicit_parent() {
		let fake = FakeS3::start().await;
		fake.insert("bkt/m/implicit/only.txt", "x");
		let dir = tempfile::tempdir().unwrap();
		let m = mount(&fake, S3MountMode::Writeback, dir.path());
		m.unlink("implicit/only.txt").await.unwrap();
		assert_eq!(fake.keys(), ["bkt/m/implicit/"]);
		assert_eq!(m.stat("implicit").await.unwrap().kind, proto::Kind::Dir);
	}
}
