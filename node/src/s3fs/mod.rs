//! S3-backed guest filesystems (ARCHITECTURE.md §8.8).
//!
//! Every S3 mount of a sandbox — bring-your-own-bucket mounts
//! (`CreateSandbox::s3_mounts`) and E2B volumes stored in the operator
//! object store (`VolumeMount::object_store`) — is a VMM remote virtio-fs
//! device (`--remote-fs <tag>:<sock>[:rw]`) served by one per-sandbox proxy
//! on the node. Credentials stay on the host; the guest only sees FUSE.
//!
//! Modes: `ro` (read-only device, lazy ranged reads), `overlay` (the same
//! lower plus a guest overlayfs whose upper lives on the sandbox disk), and
//! `writeback` (writable device; [`stage`] persists files to the bucket).

pub mod bucket;
pub mod content;
pub mod mount;
pub mod proxy;
pub mod stage;

use std::{
	collections::HashMap,
	net::IpAddr,
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use anyhow::{Context, anyhow, bail};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use vmon_agent::proto::Op;
use vmon_cloud::{
	aws::Credentials,
	s3::{S3Auth, S3Client, S3Config},
};

use self::{
	bucket::{Bucket, DEFAULT_CACHE_BYTES},
	mount::{Mount, RO_TTL, RW_TTL},
	proxy::Proxy,
};
/// Node feature advertised when `--object-store` is configured: paused
/// sandboxes are portable and object-store volumes can be mounted.
pub use crate::paused_store::FEATURE;
use crate::{
	api::{S3Mount, S3MountMode, VolumeMount},
	gc4::{AgentClient, MountRequest},
};
/// How long a kill waits for write-back uploads before dropping them.
const DETACH_SYNC_TIMEOUT: Duration = Duration::from_mins(5);
/// File (0600, in the sandbox directory) that lets a restarted node re-serve
/// the mounts of VMMs it adopts.
const MOUNTS_FILE: &str = "s3mounts.json";

/// Key prefix of a volume in the operator object store.
pub fn volume_prefix(volume_id: &str) -> String {
	format!("volumes/{volume_id}")
}

/// One S3 mount of a sandbox, in device order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Planned {
	/// virtio-fs tag (`s3_<index>`).
	pub tag:    String,
	/// Guest mount point.
	pub path:   String,
	pub mode:   S3MountMode,
	pub source: Source,
}

/// Where a mount's objects live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
	/// A user bucket with its own credentials.
	Byo(S3Mount),
	/// `volumes/<id>/` in the operator object store.
	Volume(String),
}

/// The S3 mounts of a sandbox: user buckets first, then object-store volumes.
///
/// Tags are positional, so the same request always yields the same
/// device set (a snapshot restore needs a binding for every saved tag).
pub fn plan(s3_mounts: &[S3Mount], volumes: &[VolumeMount]) -> Vec<Planned> {
	let byo = s3_mounts
		.iter()
		.map(|mount| (mount.path.clone(), mount.mode, Source::Byo(mount.clone())));
	let vols = volumes
		.iter()
		.filter(|volume| volume.object_store)
		.map(|volume| {
			(volume.path.clone(), S3MountMode::Writeback, Source::Volume(volume.volume_id.clone()))
		});
	byo.chain(vols)
		.enumerate()
		.map(|(index, (path, mode, source))| Planned {
			tag: format!("s3_{index}"),
			path,
			mode,
			source,
		})
		.collect()
}

/// `--remote-fs` binding of one planned mount.
pub fn vmm_arg(tag: &str, sock: &Path, mode: S3MountMode) -> String {
	let suffix = match mode {
		// The overlay lower is mounted read-only in the guest; a writable
		// device lets the proxy present writable permissions (overlayfs
		// checks them) and a proxy-defined root.
		S3MountMode::Ro => "",
		S3MountMode::Overlay | S3MountMode::Writeback => ":rw",
	};
	format!("--remote-fs={tag}:{}{suffix}", sock.display())
}

/// Whether `ip` is loopback, private, link-local, CGNAT, unique-local, or
/// unspecified (never a user bucket endpoint by default: the node would be
/// a confused deputy into its own network).
fn internal_ip(ip: IpAddr) -> bool {
	ip.to_canonical().is_unspecified()
		|| ip.is_multicast()
		|| crate::egress::policy::ALWAYS_DENIED.iter().any(|cidr| {
			cidr
				.parse::<crate::egress::policy::Cidr>()
				.is_ok_and(|cidr| cidr.contains(ip))
		})
}

/// Host and port of the endpoint a bring-your-own mount talks to, when it
/// is not AWS.
fn custom_endpoint(mount: &S3Mount) -> anyhow::Result<Option<(String, u16)>> {
	let url = match &mount.endpoint {
		Some(endpoint) => endpoint.as_str(),
		None if mount.uri.starts_with("s3://") => return Ok(None),
		None => mount.uri.as_str(),
	};
	let parsed = url::Url::parse(url).with_context(|| format!("invalid S3 endpoint {url:?}"))?;
	let host = parsed
		.host_str()
		.ok_or_else(|| anyhow!("S3 endpoint {url:?} has no host"))?
		.trim_matches(['[', ']'])
		.to_owned();
	let port = parsed
		.port_or_known_default()
		.ok_or_else(|| anyhow!("S3 endpoint {url:?} has no port"))?;
	Ok(Some((host, port)))
}

/// Saved mounts of a running sandbox (restart recovery).
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedMounts {
	s3_mounts: Vec<S3Mount>,
	volumes:   Vec<VolumeMount>,
	/// Proxy socket the VMM was launched with.
	sock:      PathBuf,
}

/// Configuration of [`S3Fs`].
#[derive(Clone, Debug)]
pub struct S3FsConfig {
	/// Parent of the per-sandbox proxy socket dirs (short path).
	pub run_dir:       PathBuf,
	/// Operator object store (volumes), if configured.
	pub object_store:  Option<S3Client>,
	/// Let user mounts reach private/loopback endpoints (development).
	pub allow_private: bool,
	/// Chunk-cache budget per mount.
	pub cache_bytes:   usize,
}

impl S3FsConfig {
	/// Defaults for `run_dir` and the operator store.
	pub const fn new(run_dir: PathBuf, object_store: Option<S3Client>) -> Self {
		Self { run_dir, object_store, allow_private: false, cache_bytes: DEFAULT_CACHE_BYTES }
	}
}

/// Live proxy of one sandbox.
#[derive(Debug)]
struct Live {
	proxy: Proxy,
	dir:   PathBuf,
	tags:  Vec<String>,
}

/// The guest side of a sandbox's S3 mounts after [`S3Fs::attach`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Attached {
	/// Extra `vmon vmm` flags (`--remote-fs …`).
	pub vmm_args: Vec<String>,
	/// Mounts in device order.
	pub mounts:   Vec<Planned>,
}

impl Attached {
	/// Device tags in order.
	pub fn tags(&self) -> Vec<String> {
		self.mounts.iter().map(|mount| mount.tag.clone()).collect()
	}
}

/// Per-sandbox S3 mount proxies of this node.
#[derive(Debug)]
pub struct S3Fs {
	config:  S3FsConfig,
	proxies: Mutex<HashMap<String, Arc<Live>>>,
	next:    std::sync::atomic::AtomicU64,
}

impl S3Fs {
	/// Creates the manager (no proxies yet).
	pub fn new(config: S3FsConfig) -> Self {
		Self {
			config,
			proxies: Mutex::new(HashMap::new()),
			next: std::sync::atomic::AtomicU64::new(0),
		}
	}

	/// Whether volumes can be served from the operator object store.
	pub const fn has_object_store(&self) -> bool {
		self.config.object_store.is_some()
	}

	fn sock_dir(&self, sandbox_id: &str) -> PathBuf {
		self.config.run_dir.join(format!("{sandbox_id}-s3"))
	}

	async fn client(&self, planned: &Planned) -> anyhow::Result<S3Client> {
		match &planned.source {
			Source::Volume(volume_id) => self
				.config
				.object_store
				.as_ref()
				.ok_or_else(|| {
					anyhow!("volume {volume_id} lives in the object store, which this node lacks")
				})?
				.with_prefix(&volume_prefix(volume_id))
				.map_err(|error| anyhow!("volume {volume_id}: {error}")),
			Source::Byo(mount) => {
				if let Some((host, port)) = custom_endpoint(mount)?
					&& !self.config.allow_private
				{
					let addrs = tokio::net::lookup_host((host.as_str(), port))
						.await
						.with_context(|| format!("resolving S3 endpoint {host}"))?;
					for addr in addrs {
						if internal_ip(addr.ip()) {
							bail!(
								"S3 endpoint {host} resolves to the internal address {} (not allowed for \
								 sandbox mounts)",
								addr.ip()
							);
						}
					}
				}
				let auth = match &mount.credentials {
					Some(credentials) => S3Auth::Explicit(Credentials::permanent(
						credentials.access_key_id.clone(),
						credentials.secret_access_key.clone(),
						credentials.session_token.clone(),
					)),
					// Never the node's own identity: that would let users read
					// whatever the operator's role can.
					None => S3Auth::Anonymous,
				};
				let config = S3Config {
					location: mount.uri.clone(),
					region: mount.region.clone(),
					endpoint: mount.endpoint.clone(),
					auth,
					allow_http: true,
					addressing: None,
				};
				S3Client::connect(config)
					.await
					.with_context(|| format!("S3 mount {}", mount.path))
			},
		}
	}

	/// Starts the proxy of `sandbox_id` for its S3 mounts (none → `None`).
	/// Write-back staging lives under `sandbox_dir/s3fs/<tag>/`; the mount
	/// list (with credentials) is saved 0600 in `sandbox_dir` so a restarted
	/// node can re-serve adopted VMMs.
	///
	/// # Errors
	///
	/// Fails on invalid mounts, rejected endpoints, or socket errors.
	pub async fn attach(
		&self,
		sandbox_id: &str,
		sandbox_dir: &Path,
		s3_mounts: &[S3Mount],
		volumes: &[VolumeMount],
	) -> anyhow::Result<Option<Attached>> {
		self
			.attach_at(sandbox_id, sandbox_dir, s3_mounts, volumes, None)
			.await
	}

	async fn attach_at(
		&self,
		sandbox_id: &str,
		sandbox_dir: &Path,
		s3_mounts: &[S3Mount],
		volumes: &[VolumeMount],
		sock: Option<PathBuf>,
	) -> anyhow::Result<Option<Attached>> {
		let planned = plan(s3_mounts, volumes);
		if planned.is_empty() {
			self.detach_now(sandbox_id);
			return Ok(None);
		}
		// Per-attach names: a replaced proxy removes only its own socket and
		// staging.
		let generation = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		let stage_root = sandbox_dir.join("s3fs");
		let mut mounts = HashMap::new();
		for mount in &planned {
			let client = self.client(mount).await?;
			let ttl = if mount.mode == S3MountMode::Writeback {
				RW_TTL
			} else {
				RO_TTL
			};
			let bucket = Arc::new(Bucket::new(client, ttl, self.config.cache_bytes));
			let staging = stage_root.join(format!("{generation}-{}", mount.tag));
			let mounted = Mount::new(bucket, mount.mode, &staging)
				.with_context(|| format!("staging for {}", mount.path))?;
			mounts.insert(mount.tag.clone(), Arc::new(mounted));
		}
		let dir = self.sock_dir(sandbox_id);
		crate::vm::private_dir(&dir).with_context(|| format!("creating {}", dir.display()))?;
		{
			use std::os::unix::fs::PermissionsExt;
			// Searchable by a privilege-dropped VMM; the socket is 0666.
			std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o711))?;
		}
		let sock = sock.unwrap_or_else(|| dir.join(format!("fs{generation}.sock")));
		save_mounts(sandbox_dir, s3_mounts, volumes, &sock)?;
		let proxy = Proxy::start(sock.clone(), mounts)
			.with_context(|| format!("binding S3 proxy {}", sock.display()))?;
		let tags = planned.iter().map(|mount| mount.tag.clone()).collect();
		let old = self
			.proxies
			.lock()
			.insert(sandbox_id.to_owned(), Arc::new(Live { proxy, dir, tags }));
		if let Some(old) = old {
			tracing::warn!(sandbox = sandbox_id, sock = %old.proxy.sock().display(), "replaced a live S3 proxy");
		}
		let vmm_args = planned
			.iter()
			.map(|mount| vmm_arg(&mount.tag, &sock, mount.mode))
			.collect();
		Ok(Some(Attached { vmm_args, mounts: planned }))
	}

	/// Re-serves the saved mounts of an adopted VMM after a node restart.
	///
	/// # Errors
	///
	/// Fails when saved mounts exist but cannot be served.
	pub async fn reattach(&self, sandbox_id: &str, sandbox_dir: &Path) -> anyhow::Result<()> {
		let Some(saved) = load_mounts(sandbox_dir) else {
			return Ok(());
		};
		// The VMM keeps connecting to the path it was launched with.
		self
			.attach_at(sandbox_id, sandbox_dir, &saved.s3_mounts, &saved.volumes, Some(saved.sock))
			.await
			.map(drop)
	}

	/// Mounts `attached` in a cold-booted guest.
	///
	/// # Errors
	///
	/// Fails when the agent rejects a mount.
	pub async fn mount_guest(agent: &AgentClient, attached: &Attached) -> anyhow::Result<()> {
		for mount in &attached.mounts {
			match mount.mode {
				S3MountMode::Overlay => agent
					.request(Op::MountOverlay, json!({"tag": mount.tag, "path": mount.path}))
					.await
					.map(drop),
				S3MountMode::Ro | S3MountMode::Writeback => {
					agent
						.mount(&MountRequest {
							source:    mount.tag.clone(),
							path:      mount.path.clone(),
							fstype:    "virtiofs".into(),
							read_only: mount.mode == S3MountMode::Ro,
						})
						.await
				},
			}
			.with_context(|| format!("mounting S3 {} at {}", mount.mode.as_str(), mount.path))?;
		}
		Ok(())
	}

	/// S3 device tags of a running sandbox (empty without mounts).
	pub fn tags(&self, sandbox_id: &str) -> Vec<String> {
		self
			.proxies
			.lock()
			.get(sandbox_id)
			.map(|live| live.tags.clone())
			.unwrap_or_default()
	}

	/// Uploads every pending write-back change of `sandbox_id` and waits
	/// (pause: the paused state must be complete in the bucket).
	///
	/// # Errors
	///
	/// Fails when an upload fails.
	pub async fn sync(&self, sandbox_id: &str) -> anyhow::Result<()> {
		let live = self.proxies.lock().get(sandbox_id).cloned();
		match live {
			Some(live) => live
				.proxy
				.sync_all()
				.await
				.map_err(|error| anyhow!("flushing S3 mounts: {error}")),
			None => Ok(()),
		}
	}

	/// Stops the proxy of a sandbox whose VM is gone: uploads pending
	/// write-back changes (bounded), then drops staging and the socket.
	pub async fn detach(&self, sandbox_id: &str) {
		let live = self.proxies.lock().remove(sandbox_id);
		let Some(live) = live else { return };
		match tokio::time::timeout(DETACH_SYNC_TIMEOUT, live.proxy.sync_all()).await {
			Ok(Ok(())) => {},
			Ok(Err(error)) => {
				tracing::warn!(sandbox = sandbox_id, %error, "S3 uploads failed at detach; changes lost");
			},
			Err(_) => {
				tracing::warn!(sandbox = sandbox_id, "S3 uploads timed out at detach; changes lost");
			},
		}
		let _ = std::fs::remove_dir_all(&live.dir);
	}

	/// Stops a proxy without uploading (failed boots, replaced attachments).
	pub fn detach_now(&self, sandbox_id: &str) {
		let live = self.proxies.lock().remove(sandbox_id);
		if let Some(live) = live {
			let _ = std::fs::remove_dir_all(&live.dir);
		}
	}
}

fn save_mounts(
	dir: &Path,
	s3_mounts: &[S3Mount],
	volumes: &[VolumeMount],
	sock: &Path,
) -> anyhow::Result<()> {
	use std::{io::Write, os::unix::fs::OpenOptionsExt};
	let saved = SavedMounts {
		s3_mounts: s3_mounts.to_vec(),
		volumes:   volumes
			.iter()
			.filter(|volume| volume.object_store)
			.cloned()
			.collect(),
		sock:      sock.to_owned(),
	};
	let path = dir.join(MOUNTS_FILE);
	let tmp = dir.join(format!("{MOUNTS_FILE}.tmp"));
	let mut file = std::fs::OpenOptions::new()
		.write(true)
		.create(true)
		.truncate(true)
		.mode(0o600)
		.open(&tmp)
		.with_context(|| format!("writing {}", tmp.display()))?;
	file.write_all(&serde_json::to_vec(&saved)?)?;
	file.sync_all()?;
	std::fs::rename(&tmp, &path)?;
	Ok(())
}

fn load_mounts(dir: &Path) -> Option<SavedMounts> {
	let bytes = std::fs::read(dir.join(MOUNTS_FILE)).ok()?;
	serde_json::from_slice(&bytes)
		.map_err(|error| tracing::warn!(dir = %dir.display(), %error, "unreadable saved S3 mounts"))
		.ok()
}

/// Removes the saved mount list (credentials) of an ended sandbox.
pub fn forget_mounts(sandbox_dir: &Path) {
	let _ = std::fs::remove_file(sandbox_dir.join(MOUNTS_FILE));
	let _ = std::fs::remove_dir_all(sandbox_dir.join("s3fs"));
}

/// Device tags saved beside a snapshot that has S3 mounts.
pub const SNAPSHOT_TAGS_FILE: &str = "s3-tags.json";

/// Records the S3 device tags of a snapshot captured with mounts.
///
/// # Errors
///
/// Fails when the file cannot be written.
pub fn save_snapshot_tags(snapshot_dir: &Path, tags: &[String]) -> std::io::Result<()> {
	if tags.is_empty() {
		return Ok(());
	}
	std::fs::write(snapshot_dir.join(SNAPSHOT_TAGS_FILE), serde_json::to_vec(tags)?)
}

/// S3 device tags a snapshot was captured with (empty: none).
pub fn snapshot_tags(snapshot_dir: &Path) -> Vec<String> {
	std::fs::read(snapshot_dir.join(SNAPSHOT_TAGS_FILE))
		.ok()
		.and_then(|bytes| serde_json::from_slice(&bytes).ok())
		.unwrap_or_default()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn byo(path: &str, mode: S3MountMode) -> S3Mount {
		S3Mount {
			path: path.into(),
			uri: "s3://bucket/prefix".into(),
			mode,
			endpoint: None,
			region: None,
			credentials: None,
		}
	}

	fn volume(id: &str, path: &str, object_store: bool) -> VolumeMount {
		VolumeMount { volume_id: id.into(), name: id.into(), path: path.into(), object_store }
	}

	#[test]
	fn tags_are_positional_and_skip_node_local_volumes() {
		let planned =
			plan(&[byo("/data", S3MountMode::Ro), byo("/scratch", S3MountMode::Overlay)], &[
				volume("v1", "/v1", false),
				volume("v2", "/v2", true),
			]);
		let summary: Vec<_> = planned
			.iter()
			.map(|mount| (mount.tag.as_str(), mount.path.as_str(), mount.mode))
			.collect();
		assert_eq!(summary, [
			("s3_0", "/data", S3MountMode::Ro),
			("s3_1", "/scratch", S3MountMode::Overlay),
			("s3_2", "/v2", S3MountMode::Writeback),
		]);
		assert_eq!(planned[2].source, Source::Volume("v2".into()));
	}

	#[test]
	fn vmm_args_mark_writable_devices() {
		let sock = Path::new("/run/x/fs.sock");
		assert_eq!(vmm_arg("s3_0", sock, S3MountMode::Ro), "--remote-fs=s3_0:/run/x/fs.sock");
		assert_eq!(vmm_arg("s3_1", sock, S3MountMode::Overlay), "--remote-fs=s3_1:/run/x/fs.sock:rw");
		assert_eq!(
			vmm_arg("s3_2", sock, S3MountMode::Writeback),
			"--remote-fs=s3_2:/run/x/fs.sock:rw"
		);
	}

	#[test]
	fn internal_endpoints_are_recognized() {
		for internal in
			["127.0.0.1", "10.1.2.3", "169.254.169.254", "::1", "0.0.0.0", "::ffff:10.0.0.1"]
		{
			assert!(internal_ip(internal.parse().unwrap()), "{internal}");
		}
		assert!(!internal_ip("52.216.1.1".parse().unwrap()));
		let mut mount = byo("/d", S3MountMode::Ro);
		assert_eq!(custom_endpoint(&mount).unwrap(), None);
		mount.endpoint = Some("http://127.0.0.1:19000".into());
		assert_eq!(custom_endpoint(&mount).unwrap(), Some(("127.0.0.1".into(), 19000)));
		mount.endpoint = None;
		mount.uri = "https://minio.example.com/bucket/x".into();
		assert_eq!(custom_endpoint(&mount).unwrap(), Some(("minio.example.com".into(), 443)));
	}

	#[tokio::test]
	async fn private_endpoints_are_rejected_unless_allowed() {
		let dir = tempfile::tempdir().unwrap();
		let mut mount = byo("/d", S3MountMode::Ro);
		mount.endpoint = Some("http://127.0.0.1:9".into());
		let s3fs = S3Fs::new(S3FsConfig::new(dir.path().join("run"), None));
		let error = s3fs
			.attach("sbx", dir.path(), &[mount], &[])
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("internal address"), "{error:#}");
		let error = s3fs
			.attach("sbx", dir.path(), &[], &[volume("v", "/v", true)])
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("object store"), "{error:#}");
	}

	#[tokio::test]
	async fn attach_saves_mounts_privately_and_detach_cleans_up() {
		use std::os::unix::fs::PermissionsExt;
		let fake = vmon_cloud::s3::fake::FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		let mut config = S3FsConfig::new(dir.path().join("run"), Some(fake.client("ops", "store")));
		config.allow_private = true;
		std::fs::create_dir_all(&config.run_dir).unwrap();
		let s3fs = S3Fs::new(config);
		let attached = s3fs
			.attach("sbx", dir.path(), &[], &[volume("v9", "/mnt/v", true)])
			.await
			.unwrap()
			.unwrap();
		assert_eq!(attached.tags(), ["s3_0"]);
		assert!(attached.vmm_args[0].ends_with("fs0.sock:rw"));
		let saved = dir.path().join(MOUNTS_FILE);
		assert_eq!(std::fs::metadata(&saved).unwrap().permissions().mode() & 0o777, 0o600);
		s3fs.sync("sbx").await.unwrap();
		s3fs.detach("sbx").await;
		assert!(!dir.path().join("run/sbx-s3").exists());
		forget_mounts(dir.path());
		assert!(!saved.exists());
	}
}
