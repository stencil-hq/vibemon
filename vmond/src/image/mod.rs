//! OCI -> ext4 -> boot-verified template pipeline. Port of
//! python/vmon/image.py.
#![allow(clippy::module_name_repetitions, reason = "public API mirrors the Python image module")]

pub mod assets;
pub mod build;
pub mod cas;
mod disk_image;
pub mod registry_auth;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
	collections::{BTreeMap, HashMap},
	fs::{self, File},
	hash::Hash,
	io::{Read, Seek, SeekFrom},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{Arc, LazyLock},
	time::{SystemTime, UNIX_EPOCH},
};

pub use disk_image::{PublishedRootfs, RemoteRootfs};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
	error::{EngineError, Result},
	image::registry_auth::AuthFile,
};

const IMAGE_TRANSPORT_PREFIXES: &[&str] = &[
	"docker://",
	"gs://",
	"oci:",
	"dir:",
	"docker-archive:",
	"oci-archive:",
	"containers-storage:",
];

const E2FSPROGS_DIRS: &[&str] = &[
	"/opt/homebrew/opt/e2fsprogs/sbin",
	"/usr/local/opt/e2fsprogs/sbin",
	"/opt/homebrew/sbin",
	"/usr/local/sbin",
	"/sbin",
	"/usr/sbin",
];

const EXTRA_TOOL_DIRS: &[&str] = &[
	"/opt/homebrew/bin",
	"/usr/local/bin",
	"/opt/homebrew/opt/e2fsprogs/sbin",
	"/usr/local/opt/e2fsprogs/sbin",
	"/opt/homebrew/sbin",
	"/usr/local/sbin",
	"/sbin",
	"/usr/sbin",
];

const TEMPLATE_BOOT_VERSION: u64 = 6;
const STATIC_AGENT_HINT: &str =
	"Build and bundle it with `just agent-musl`, or set VMON_AGENT=/path/to/static-agent.";
const AGENT_BUILD_TIMEOUT_SECS: u64 = 120;

/// Image execution defaults needed by sandbox commands.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageConfig {
	pub reference:  String,
	pub entrypoint: Vec<String>,
	pub cmd:        Vec<String>,
	pub env:        Vec<String>,
	pub workdir:    String,
	pub user:       String,
}

impl ImageConfig {
	/// Return the final process argv for an optional command override.
	pub fn argv(&self, override_cmd: Option<&[String]>) -> Vec<String> {
		if let Some(override_cmd) = override_cmd.filter(|cmd| !cmd.is_empty()) {
			if self.entrypoint.is_empty() {
				return override_cmd.to_vec();
			}
			let mut argv = self.entrypoint.clone();
			argv.extend_from_slice(override_cmd);
			return argv;
		}
		let mut argv = self.entrypoint.clone();
		argv.extend_from_slice(&self.cmd);
		argv
	}

	/// Return image environment entries as a key-value map.
	pub fn env_dict(&self) -> HashMap<String, String> {
		self
			.env
			.iter()
			.filter_map(|entry| {
				entry
					.split_once('=')
					.map(|(key, value)| (key.to_owned(), value.to_owned()))
			})
			.collect()
	}
}

/// A boot-verified sandbox template snapshot and its immutable base disk.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CachedTemplate {
	pub name:         String,
	pub snapshot_dir: PathBuf,
	pub rootfs:       PathBuf,
	pub spec:         ImageConfig,
	pub image_digest: String,
	pub disk_mb:      u64,
	pub memory:       u64,
	pub cpus:         u64,
	pub fs_slots:     u64,
	pub host_slot:    bool,
	pub nic_slot:     bool,
	pub tap_slot:     bool,
	pub digest:       String,
}

/// Template build inputs accepted by [`cached_template`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateRequest {
	pub image:      Option<String>,
	pub dockerfile: Option<PathBuf>,
	pub context:    PathBuf,
	pub disk_mb:    u64,
	pub timeout:    u64,
	pub memory:     u64,
	pub cpus:       u64,
	pub fs_slots:   u64,
	pub host_slot:  bool,
	pub nic_slot:   bool,
	pub tap_slot:   bool,
}

impl Default for TemplateRequest {
	fn default() -> Self {
		Self {
			image:      None,
			dockerfile: None,
			context:    PathBuf::from("."),
			disk_mb:    1024,
			timeout:    300,
			memory:     512,
			cpus:       1,
			fs_slots:   0,
			host_slot:  false,
			nic_slot:   false,
			tap_slot:   false,
		}
	}
}

/// Reserved virtio-fs volume presented to a template boot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateVolume {
	pub tag:      String,
	pub dir:      PathBuf,
	pub readonly: bool,
}

/// Complete handoff for the Engine-owned boot-verify and snapshot step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateSpec {
	pub vm_name:       String,
	pub template_name: String,
	pub template_dir:  PathBuf,
	pub rootfs_ext4:   PathBuf,
	pub snapshot_root: PathBuf,
	pub image:         String,
	pub timeout:       u64,
	pub memory:        u64,
	pub cpus:          u64,
	pub volumes:       Vec<TemplateVolume>,
	pub fs_dir:        Option<PathBuf>,
	pub user_net:      bool,
	pub tap_slot:      bool,
	pub rng:           bool,
	/// Range-addressable base used when this template comes from a GCE export.
	pub remote_rootfs: Option<RemoteRootfs>,
}

/// Engine seam for booting a prepared rootfs and snapshotting it as a template.
pub trait TemplateBooter {
	fn boot_verify_and_snapshot(&self, spec: &TemplateSpec) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageTools {
	pub skopeo: PathBuf,
	pub umoci:  PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedImage {
	reference: String,
	spec:      ImageConfig,
	digest:    String,
	source:    PreparedImageSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PreparedImageSource {
	Oci { transport_ref: String, tools: ImageTools, arch: String },
	GceDisk(Box<disk_image::PreparedDiskImage>),
}
/// Immutable OCI identity returned by the registry resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedOciImage {
	/// Caller-facing reference pinned to the selected manifest digest.
	pub reference: String,
	/// Lowercase SHA-256 manifest digest without the `sha256:` prefix.
	pub digest:    String,
	/// Selected Linux OCI platform.
	pub platform:  String,
}

/// Resolve an OCI reference for one architecture without consulting the
/// prepared-image cache. Mutable tags are deliberately inspected on every
/// call so registration observes tag movement.
pub fn resolve_oci_reference(reference: &str, arch: Option<&str>) -> Result<ResolvedOciImage> {
	let reference = parse_reference(Some(reference))?
		.ok_or_else(|| EngineError::invalid("OCI image reference is required"))?;
	if is_gce_disk_image_reference(&reference) {
		return Err(EngineError::invalid("gs:// references are GCE disk images, not OCI images"));
	}
	let tools = detect_image_tools()?;
	let arch = skopeo_arch(arch);
	let auth = registry_auth::auth_file_for(&reference);
	let stdout = run_stdout(&skopeo_inspect_digest_args(
		&tools.skopeo,
		&reference,
		&arch,
		auth.as_ref().map(AuthFile::path),
	))
	.map_err(|error| {
		EngineError::engine(format!("failed to resolve OCI image for linux/{arch}: {error}"))
	})?;
	let info: Value = serde_json::from_str(&stdout)?;
	let digest = info
		.get("Digest")
		.and_then(Value::as_str)
		.and_then(|value| value.strip_prefix("sha256:"))
		.ok_or_else(|| EngineError::engine("image source did not return a sha256 manifest digest"))?;
	if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		return Err(EngineError::engine("registry returned an invalid sha256 manifest digest"));
	}
	let digest = digest.to_ascii_lowercase();
	let pinned = if is_registry_reference(&reference) {
		let unqualified = reference.strip_prefix("docker://").unwrap_or(&reference);
		let repository = unqualified
			.split_once('@')
			.map_or(unqualified, |(repository, _)| repository);
		let last_slash = repository.rfind('/').map_or(0, |index| index + 1);
		let tag = repository[last_slash..]
			.find(':')
			.map(|index| last_slash + index);
		let repository = &repository[..tag.unwrap_or(repository.len())];
		format!("{repository}@sha256:{digest}")
	} else {
		reference
	};
	Ok(ResolvedOciImage { reference: pinned, digest, platform: format!("linux/{arch}") })
}

type PreparedImageCacheKey = (PathBuf, String, String);
static PREPARED_IMAGES: LazyLock<Mutex<HashMap<PreparedImageCacheKey, PreparedImage>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));
static PREPARED_IMAGE_INSPECTIONS: LazyLock<
	KeyedSingleflight<PreparedImageCacheKey, PreparedImage>,
> = LazyLock::new(KeyedSingleflight::default);
static TEMPLATE_MATERIALIZATIONS: LazyLock<KeyedSingleflight<PathBuf, CachedTemplate>> =
	LazyLock::new(KeyedSingleflight::default);

struct KeyedSingleflight<K, T> {
	flights: Mutex<HashMap<K, Arc<SingleflightResult<T>>>>,
}

struct SingleflightResult<T> {
	result:  Mutex<Option<Result<T>>>,
	ready:   Condvar,
	#[cfg(test)]
	waiters: AtomicUsize,
}

impl<K, T> Default for KeyedSingleflight<K, T> {
	fn default() -> Self {
		Self { flights: Mutex::new(HashMap::new()) }
	}
}

impl<K, T> KeyedSingleflight<K, T>
where
	K: Clone + Eq + Hash,
	T: Clone,
{
	fn run(&self, key: K, produce: impl FnOnce() -> Result<T>) -> Result<T> {
		let (flight, producer) = {
			let mut flights = self.flights.lock();
			if let Some(flight) = flights.get(&key) {
				(Arc::clone(flight), false)
			} else {
				let flight = Arc::new(SingleflightResult {
					result:               Mutex::new(None),
					ready:                Condvar::new(),
					#[cfg(test)]
					waiters:              AtomicUsize::new(0),
				});
				flights.insert(key.clone(), Arc::clone(&flight));
				(flight, true)
			}
		};
		if producer {
			let result = produce();
			{
				let mut completed = flight.result.lock();
				*completed = Some(result.clone());
			}
			flight.ready.notify_all();
			let mut flights = self.flights.lock();
			if flights
				.get(&key)
				.is_some_and(|current| Arc::ptr_eq(current, &flight))
			{
				flights.remove(&key);
			}
			return result;
		}
		#[cfg(test)]
		flight.waiters.fetch_add(1, Ordering::Relaxed);
		let mut completed = flight.result.lock();
		while completed.is_none() {
			flight.ready.wait(&mut completed);
		}
		completed
			.clone()
			.ok_or_else(|| EngineError::engine("singleflight completed without a result"))?
	}

	#[cfg(test)]
	fn waiter_count(&self, key: &K) -> usize {
		let flights = self.flights.lock();
		flights
			.get(key)
			.map_or(0, |flight| flight.waiters.load(Ordering::Relaxed))
	}
}

/// Normalize an optional image reference, rejecting whitespace.
pub fn parse_reference(reference: Option<&str>) -> Result<Option<String>> {
	let Some(reference) = reference else {
		return Ok(None);
	};
	let reference = reference.trim();
	if reference.is_empty() {
		return Ok(None);
	}
	if reference.chars().any(char::is_whitespace) {
		return Err(EngineError::invalid(format!(
			"image reference must not contain whitespace: {reference:?}"
		)));
	}
	Ok(Some(reference.to_owned()))
}
/// Return whether a reference names a GCE-exported disk archive in GCS.
///
/// Keeping this transport explicit prevents an opaque `gs://` URI from ever
/// reaching skopeo's OCI transport parser.
pub fn is_gce_disk_image_reference(reference: &str) -> bool {
	reference.starts_with(disk_image::GCS_PREFIX)
}

/// Publish a GCE export's root partition as a range-addressable GCS object.
///
/// Conversion is explicit because it reads the entire compressed export; normal
/// sandbox creation only consumes an artifact already published by this call.
pub fn publish_gce_rootfs(reference: &str) -> Result<PublishedRootfs> {
	let agent = ensure_agent(None)?;
	disk_image::publish(reference, &agent)
}

/// Read the lazy rootfs descriptor persisted alongside a template snapshot.
pub fn template_remote_rootfs(template_dir: &Path) -> Result<Option<RemoteRootfs>> {
	let path = template_dir.join("remote-rootfs.json");
	match fs::read(&path) {
		Ok(bytes) => serde_json::from_slice(&bytes)
			.map(Some)
			.map_err(|error| EngineError::engine(format!("invalid {}: {error}", path.display()))),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error.into()),
	}
}

/// Shared per-worker block cache for one immutable published rootfs digest.
pub fn remote_rootfs_cache_path(remote: &RemoteRootfs) -> PathBuf {
	crate::home::state_dir()
		.join("images")
		.join("remote-rootfs")
		.join(format!("{}.cache", remote.sha256))
}

#[derive(Serialize)]
struct RemoteRootfsIndex<'a> {
	version:           u32,
	compressed_size:   u64,
	uncompressed_size: u64,
	original_size:     u64,
	block_size:        u64,
	blocks:            &'a [[u64; 2]],
}

/// Persist the compact seek index separately so the VMM need not fetch a
/// second authenticated object before it can service the first block read.
pub fn ensure_remote_rootfs_index(remote: &RemoteRootfs) -> Result<PathBuf> {
	let dir = crate::home::state_dir()
		.join("images")
		.join("remote-rootfs");
	let path = dir.join(format!("{}.index.json", remote.sha256));
	let index = RemoteRootfsIndex {
		version:           3,
		compressed_size:   remote.compressed_size,
		uncompressed_size: remote.uncompressed_size,
		original_size:     remote.original_size,
		block_size:        remote.block_size,
		blocks:            &remote.blocks,
	};
	let contents = serde_json::to_vec(&index)?;
	if fs::read(&path).is_ok_and(|current| current == contents) {
		return Ok(path);
	}
	let temporary = temp_file_in(&dir, ".remote-index-")?;
	fs::write(&temporary, contents)?;
	fs::rename(&temporary, &path)?;
	Ok(path)
}

/// Mint a fresh metadata-server token for a GCS block request.
pub(crate) fn gcs_access_token() -> Result<String> {
	registry_auth::metadata_access_token().ok_or_else(|| {
		EngineError::unauthorized(
			"failed to obtain a GCE metadata-server OAuth token for the remote rootfs",
		)
	})
}

/// Return daemonless OCI image tools required by image-backed sandboxes.
pub fn detect_image_tools() -> Result<ImageTools> {
	let skopeo = find_tool("skopeo");
	let umoci = find_tool("umoci");
	let mut missing = Vec::new();
	if skopeo.is_none() {
		missing.push("skopeo");
	}
	if umoci.is_none() {
		missing.push("umoci");
	}
	if !missing.is_empty() {
		return Err(EngineError::unsupported(format!(
			"missing required image tool(s): {}",
			missing.join(", ")
		)));
	}
	Ok(ImageTools { skopeo: skopeo.unwrap_or_default(), umoci: umoci.unwrap_or_default() })
}

/// Normalize common OCI architecture names to vmon node architecture names.
pub fn normalize_oci_arch(arch: Option<&str>) -> Option<String> {
	let machine = arch?.trim().to_ascii_lowercase().replace('-', "_");
	if machine.is_empty() {
		return None;
	}
	Some(match machine.as_str() {
		"amd64" | "x64" => "x86_64".to_owned(),
		"arm64" => "aarch64".to_owned(),
		other => other.to_owned(),
	})
}

/// Return Linux architectures advertised by an image manifest, if inspectable.
pub fn manifest_arches(reference: &str) -> Option<Vec<String>> {
	let Ok(Some(reference)) = parse_reference(Some(reference)) else {
		return None;
	};
	if is_gce_disk_image_reference(&reference) {
		return None;
	}
	let skopeo = find_tool("skopeo")?;
	let transport_ref = image_transport_ref(&reference);
	let mut inspect_info = None;
	let mut digest = None;
	if let Ok(stdout) = run_stdout(&[
		path_string(&skopeo),
		"inspect".to_owned(),
		"--no-tags".to_owned(),
		transport_ref.clone(),
	]) && let Ok(info) = serde_json::from_str::<Value>(&stdout)
	{
		digest = info
			.get("Digest")
			.and_then(Value::as_str)
			.map(ToOwned::to_owned);
		inspect_info = Some(info);
	}
	if let Some(digest) = digest.as_deref()
		&& let Some(cached) = read_manifest_arch_cache(&reference, digest)
	{
		return Some(cached);
	}
	let raw =
		run_stdout(&[path_string(&skopeo), "inspect".to_owned(), "--raw".to_owned(), transport_ref])
			.ok()?;
	let arches = manifest_arches_from_raw(&raw, inspect_info.as_ref()).ok()?;
	if arches.is_empty() {
		return None;
	}
	let digest =
		digest.unwrap_or_else(|| format!("raw-{}", hex::encode(Sha256::digest(raw.as_bytes()))));
	let _ = write_manifest_arch_cache(&reference, &digest, &arches);
	Some(arches)
}

/// Reserved virtio-fs tag for a warm-volume slot.
pub fn slot_tag(i: u64) -> String {
	format!("vmon_slot{i}")
}

/// Format the rootfs cache key for an image digest, disk size, and agent
/// digest.
pub fn image_cache_key(image_digest: &str, disk_mb: u64, agent_digest: &str) -> String {
	format!("{image_digest}-{disk_mb}-a{}", digest_prefix(agent_digest, 12))
}

/// Format the immutable template snapshot name.
pub fn template_name(
	image_digest: &str,
	disk_mb: u64,
	agent_digest: &str,
	memory: u64,
	cpus: u64,
	fs_slots: u64,
	host_slot: bool,
	nic_slot: bool,
	tap_slot: bool,
) -> String {
	let mut name = format!(
		"tpl-{}-{disk_mb}-a{}-m{memory}-c{cpus}",
		digest_prefix(image_digest, 12),
		digest_prefix(agent_digest, 12)
	);
	if fs_slots != 0 {
		name.push_str("-s");
		name.push_str(&fs_slots.to_string());
	}
	if host_slot {
		name.push_str("-h");
	}
	if nic_slot {
		name.push_str("-n");
	}
	if tap_slot {
		name.push_str("-t");
	}
	name
}

fn template_vm_name(template: &str) -> String {
	let digest = hex::encode(Sha256::digest(template.as_bytes()));
	format!("_t-{}", &digest[..16])
}

/// Build or reuse an agent-capable ext4 rootfs and boot-verified template
/// snapshot.
pub fn cached_template(
	booter: &impl TemplateBooter,
	request: &TemplateRequest,
) -> Result<CachedTemplate> {
	validate_template_request(request)?;
	let prepared =
		prepare_image(request.image.as_deref(), request.dockerfile.as_deref(), &request.context)?;
	let agent = ensure_agent(None)?;
	let agent_digest = sha256_file(&agent)?;
	if let PreparedImageSource::GceDisk(image) = &prepared.source
		&& image.remote.agent_sha256 != agent_digest
	{
		return Err(EngineError::invalid(format!(
			"published rootfs agent does not match this vmon build; run `vmon image \
			 publish-gce-rootfs {}`",
			image.reference
		)));
	}
	let image_digest = prepared.digest.clone();
	let image_key = image_cache_key(&image_digest, request.disk_mb, &agent_digest);
	let tpl_name = template_name(
		&image_digest,
		request.disk_mb,
		&agent_digest,
		request.memory,
		request.cpus,
		request.fs_slots,
		request.host_slot,
		request.nic_slot,
		request.tap_slot,
	);
	let state_dir = crate::home::state_dir();
	let tpl_dir = state_dir.join("templates").join(&tpl_name);
	TEMPLATE_MATERIALIZATIONS.run(tpl_dir.clone(), || {
		materialize_template(
			booter,
			request,
			&prepared,
			&agent,
			&agent_digest,
			&image_digest,
			&image_key,
			&tpl_name,
			&state_dir,
			tpl_dir,
		)
	})
}

#[expect(
	clippy::too_many_arguments,
	reason = "the request and prepared image retain their established shapes"
)]
fn materialize_template(
	booter: &impl TemplateBooter,
	request: &TemplateRequest,
	prepared: &PreparedImage,
	agent: &Path,
	agent_digest: &str,
	image_digest: &str,
	image_key: &str,
	tpl_name: &str,
	state_dir: &Path,
	tpl_dir: PathBuf,
) -> Result<CachedTemplate> {
	let spec = prepared.spec.clone();
	let image_dir = state_dir.join("images").join(image_key);
	let rootfs_ext4 = image_dir.join("rootfs.ext4");
	let spec_path = image_dir.join("spec.json");
	fs::create_dir_all(&image_dir)?;
	let mut remote_rootfs = match &prepared.source {
		PreparedImageSource::GceDisk(image) => Some(image.remote.clone()),
		PreparedImageSource::Oci { .. } => None,
	};
	if let Some(remote) = &mut remote_rootfs {
		let logical_size = request
			.disk_mb
			.checked_mul(1024 * 1024)
			.ok_or_else(|| EngineError::invalid("disk_mb is too large"))?;
		if logical_size < remote.uncompressed_size {
			return Err(EngineError::invalid(format!(
				"requested disk is {} bytes but the minimized rootfs needs {} bytes",
				logical_size, remote.uncompressed_size
			)));
		}
		remote.logical_size = logical_size;
	}

	if remote_rootfs.is_none() && !rootfs_ext4.is_file() {
		let tmp = TempDir::new("vmon-image-")?;
		let tmp_ext4 = temp_file_in(&image_dir, ".rootfs.ext4.tmp-")?;
		let rootfs = tmp.path().join("rootfs");
		fs::create_dir(&rootfs)?;
		export_oci_image(prepared, &rootfs, tmp.path())?;
		inject_agent(&rootfs, agent)?;
		let build_result = mkfs_ext4(&rootfs, &tmp_ext4, request.disk_mb);
		if build_result.is_ok() {
			fs::rename(&tmp_ext4, &rootfs_ext4)?;
		}
		let _ = fs::remove_file(&tmp_ext4);
		build_result?;
	}
	fs::write(&spec_path, serde_json::to_string_pretty(&spec)?)?;

	let mut template = CachedTemplate {
		name:         tpl_name.to_owned(),
		snapshot_dir: tpl_dir.clone(),
		rootfs:       rootfs_ext4.clone(),
		spec:         spec.clone(),
		image_digest: image_digest.to_owned(),
		disk_mb:      request.disk_mb,
		memory:       request.memory,
		cpus:         request.cpus,
		fs_slots:     request.fs_slots,
		host_slot:    request.host_slot,
		nic_slot:     request.nic_slot,
		tap_slot:     request.tap_slot,
		digest:       String::new(),
	};
	let kernel = assets::default_kernel()?;
	let kernel_sha = sha256_file(&kernel)?;
	let marker = tpl_dir.join("agent-ready.json");
	let cached_remote_matches = remote_rootfs.as_ref().is_none_or(|remote| {
		template_remote_rootfs(&tpl_dir).is_ok_and(|value| value.as_ref() == Some(remote))
	});
	if snapshot_state_present(&tpl_dir)
		&& tpl_dir.join("rootfs.img").is_file()
		&& cached_remote_matches
		&& template_marker_current(
			&marker,
			&kernel_sha,
			request.memory,
			request.cpus,
			request.fs_slots,
			request.host_slot,
			request.nic_slot,
		) {
		index_cached_template(&mut template, &marker)?;
		return Ok(template);
	}
	if tpl_dir.exists() {
		fs::remove_dir_all(&tpl_dir)?;
	}
	if remote_rootfs.is_some() {
		let _ = fs::remove_file(&rootfs_ext4);
	}
	let vm_name = template_vm_name(tpl_name);
	let slot_void = state_dir.join("slot-void");
	fs::create_dir_all(&slot_void)?;
	let volumes = (0..request.fs_slots)
		.map(|i| TemplateVolume {
			tag:      slot_tag(i),
			dir:      slot_void.clone(),
			readonly: true,
		})
		.collect();
	let boot_spec = TemplateSpec {
		vm_name,
		template_name: tpl_name.to_owned(),
		template_dir: tpl_dir.clone(),
		rootfs_ext4,
		snapshot_root: state_dir.join("templates"),
		image: spec.reference.clone(),
		timeout: request.timeout,
		memory: request.memory,
		cpus: request.cpus,
		volumes,
		fs_dir: request.host_slot.then_some(slot_void),
		user_net: request.nic_slot,
		tap_slot: request.tap_slot,
		rng: true,
		remote_rootfs: remote_rootfs.clone(),
	};
	booter.boot_verify_and_snapshot(&boot_spec)?;
	fs::create_dir_all(&tpl_dir)?;
	if let Some(remote) = &remote_rootfs {
		fs::write(tpl_dir.join("remote-rootfs.json"), serde_json::to_vec_pretty(remote)?)?;
	}
	write_marker(
		&marker,
		&spec.reference,
		image_digest,
		agent_digest,
		request.disk_mb,
		&kernel_sha,
		request.fs_slots,
		request.memory,
		request.cpus,
		request.host_slot,
		request.nic_slot,
		request.tap_slot,
	)?;
	index_cached_template(&mut template, &marker)?;
	if request.dockerfile.is_some() {
		let _ = build::prune_build_layouts(Some(&prepared.reference), 3);
	}
	Ok(template)
}

/// Append `--authfile` when credentials were resolved for this registry.
fn push_authfile(args: &mut Vec<String>, authfile: Option<&Path>) {
	if let Some(path) = authfile {
		args.push("--authfile".to_owned());
		args.push(path_string(path));
	}
}

/// Return skopeo inspect --config argv.
pub fn skopeo_inspect_config_args(
	skopeo: &Path,
	reference: &str,
	arch: &str,
	authfile: Option<&Path>,
) -> Vec<String> {
	let mut args = vec![
		path_string(skopeo),
		"inspect".to_owned(),
		"--config".to_owned(),
		"--override-os".to_owned(),
		"linux".to_owned(),
		"--override-arch".to_owned(),
		skopeo_arch(Some(arch)),
	];
	push_authfile(&mut args, authfile);
	args.push(image_transport_ref(reference));
	args
}

/// Return skopeo inspect --no-tags argv for digest resolution.
pub fn skopeo_inspect_digest_args(
	skopeo: &Path,
	reference: &str,
	arch: &str,
	authfile: Option<&Path>,
) -> Vec<String> {
	let mut args = vec![
		path_string(skopeo),
		"inspect".to_owned(),
		"--no-tags".to_owned(),
		"--override-os".to_owned(),
		"linux".to_owned(),
		"--override-arch".to_owned(),
		skopeo_arch(Some(arch)),
	];
	push_authfile(&mut args, authfile);
	args.push(image_transport_ref(reference));
	args
}

/// Return skopeo copy argv for exporting an OCI image.
pub fn skopeo_copy_args(
	skopeo: &Path,
	arch: &str,
	transport_ref: &str,
	oci_dir: &Path,
	authfile: Option<&Path>,
) -> Vec<String> {
	let mut args = vec![
		path_string(skopeo),
		"copy".to_owned(),
		"--override-os".to_owned(),
		"linux".to_owned(),
		"--override-arch".to_owned(),
		skopeo_arch(Some(arch)),
	];
	push_authfile(&mut args, authfile);
	args.push(transport_ref.to_owned());
	args.push(format!("oci:{}:latest", path_string(oci_dir)));
	args
}

/// Return umoci unpack argv.
pub fn umoci_unpack_args(
	umoci: &Path,
	oci_dir: &Path,
	bundle: &Path,
	rootless: bool,
) -> Vec<String> {
	let mut args = vec![path_string(umoci), "unpack".to_owned()];
	if rootless {
		args.push("--rootless".to_owned());
	}
	args.extend([
		"--image".to_owned(),
		format!("{}:latest", path_string(oci_dir)),
		path_string(bundle),
	]);
	args
}

/// Return mkfs.ext4/mke2fs argv.
pub fn mkfs_ext4_args(mkfs: &Path, rootfs: &Path, out: &Path) -> Vec<String> {
	let mut args = vec![path_string(mkfs)];
	if mkfs.file_name().and_then(|name| name.to_str()) == Some("mke2fs") {
		args.extend(["-t".to_owned(), "ext4".to_owned()]);
	}
	args.extend([
		"-q".to_owned(),
		"-F".to_owned(),
		"-d".to_owned(),
		path_string(rootfs),
		path_string(out),
	]);
	args
}

/// Locate the static guest agent injected into image rootfs trees.
pub fn find_agent_binary(arch: Option<&str>) -> Result<PathBuf> {
	let arch = agent_arch(arch);
	if let Ok(env) = std::env::var("VMON_AGENT")
		&& !env.is_empty()
	{
		let path = expand_home(&env);
		if !path.is_file() {
			return Err(EngineError::not_found(format!(
				"VMON_AGENT points to missing file: {}",
				path.display()
			)));
		}
		if !is_static_elf(&path)? {
			return Err(EngineError::engine(format!(
				"vmon-agent must be a static (musl) binary for arbitrary guest rootfs. \
				 {STATIC_AGENT_HINT}"
			)));
		}
		return Ok(path);
	}
	let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.map_or_else(|| PathBuf::from("."), Path::to_path_buf);
	let mut candidates = vec![
		repo
			.join("target")
			.join("test-assets")
			.join(format!("vmon-agent-{arch}")),
		repo
			.join("target")
			.join(format!("{arch}-unknown-linux-musl"))
			.join("release")
			.join("vmon-agent"),
		repo.join("target").join("release").join("vmon-agent"),
		repo
			.join("target")
			.join(format!("{arch}-unknown-linux-gnu"))
			.join("release")
			.join("vmon-agent"),
	];
	if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
		let target_dir = PathBuf::from(target_dir);
		candidates.push(
			target_dir
				.join(format!("{arch}-unknown-linux-musl"))
				.join("release")
				.join("vmon-agent"),
		);
		candidates.push(target_dir.join("release").join("vmon-agent"));
	}
	if let Some(found) = find_tool("vmon-agent") {
		candidates.push(found);
	}
	let checked = candidates
		.iter()
		.map(|path| path.to_string_lossy())
		.collect::<Vec<_>>()
		.join(", ");
	let mut any_file = false;
	for candidate in &candidates {
		if candidate.is_file() {
			any_file = true;
			if is_static_elf(candidate)? {
				return Ok(candidate.clone());
			}
		}
	}
	if any_file {
		return Err(EngineError::engine(format!(
			"a vmon-agent for {arch} was found but is not a static (musl) binary required for \
			 arbitrary guest rootfs; checked: {checked}. {STATIC_AGENT_HINT}"
		)));
	}
	Err(EngineError::engine(format!(
		"vmon-agent binary for {arch} not found; checked: {checked}. {STATIC_AGENT_HINT}"
	)))
}

/// Return a usable static guest agent, building it from a checkout when
/// possible.
pub fn ensure_agent(arch: Option<&str>) -> Result<PathBuf> {
	match find_agent_binary(arch) {
		Ok(path) => Ok(path),
		Err(error) => {
			let agent_arch = agent_arch(arch);
			if build_static_agent(&agent_arch) {
				find_agent_binary(arch)
			} else {
				Err(error)
			}
		},
	}
}

fn validate_template_request(request: &TemplateRequest) -> Result<()> {
	if request.disk_mb == 0 {
		return Err(EngineError::invalid("disk_mb must be positive"));
	}
	if (request.nic_slot || request.tap_slot) && (request.fs_slots > 0 || request.host_slot) {
		return Err(EngineError::invalid("a NIC slot cannot be combined with fs_slots or host_slot"));
	}
	if request.nic_slot && request.tap_slot {
		return Err(EngineError::invalid(
			"nic_slot (macOS user-net) and tap_slot (Linux TAP) are mutually exclusive",
		));
	}
	Ok(())
}

fn prepare_image(
	reference: Option<&str>,
	dockerfile: Option<&Path>,
	context: &Path,
) -> Result<PreparedImage> {
	let built = dockerfile.is_some();
	let reference = if let Some(dockerfile) = dockerfile {
		let tag = parse_reference(reference)?.unwrap_or_else(|| "vmon-build:latest".to_owned());
		if tag.starts_with(disk_image::GCS_PREFIX) {
			return Err(EngineError::invalid(
				"a Dockerfile build cannot use a gs:// disk image reference",
			));
		}
		build::build_image(dockerfile, context, &tag, None)?
	} else {
		parse_reference(reference)?
			.ok_or_else(|| EngineError::invalid("provide an image reference"))?
	};
	if is_gce_disk_image_reference(&reference) {
		let image = disk_image::prepare(&reference)?;
		return Ok(PreparedImage {
			reference,
			spec: image.spec.clone(),
			digest: image.digest.clone(),
			source: PreparedImageSource::GceDisk(Box::new(image)),
		});
	}
	let tools = detect_image_tools()?;
	let arch = skopeo_arch(None);
	if !built && is_registry_reference(&reference) {
		let key = (crate::home::state_dir(), reference.clone(), arch.clone());
		{
			let prepared = PREPARED_IMAGES.lock();
			if let Some(image) = prepared.get(&key) {
				return Ok(image.clone());
			}
		}
		return PREPARED_IMAGE_INSPECTIONS.run(key.clone(), || {
			let prepared = PREPARED_IMAGES.lock();
			if let Some(image) = prepared.get(&key) {
				return Ok(image.clone());
			}
			drop(prepared);
			let image = inspect_prepared_image(reference, tools, arch)?;
			let mut prepared = PREPARED_IMAGES.lock();
			prepared.insert(key, image.clone());
			Ok(image)
		});
	}
	inspect_prepared_image(reference, tools, arch)
}

fn inspect_prepared_image(
	reference: String,
	tools: ImageTools,
	arch: String,
) -> Result<PreparedImage> {
	let spec = inspect_oci(&tools, &reference, &arch)?;
	let digest = image_digest_oci(&tools, &reference, &arch)?;
	Ok(PreparedImage {
		source: PreparedImageSource::Oci {
			transport_ref: image_transport_ref(&reference),
			tools,
			arch,
		},
		reference,
		spec,
		digest,
	})
}

/// Return whether a reference pulls from a registry rather than a host-local
/// transport.
pub fn is_registry_reference(reference: &str) -> bool {
	reference.starts_with("docker://")
		|| !IMAGE_TRANSPORT_PREFIXES
			.iter()
			.any(|prefix| reference.starts_with(prefix))
}

fn inspect_oci(tools: &ImageTools, reference: &str, arch: &str) -> Result<ImageConfig> {
	let auth = registry_auth::auth_file_for(reference);
	let stdout = run_stdout(&skopeo_inspect_config_args(
		&tools.skopeo,
		reference,
		arch,
		auth.as_ref().map(AuthFile::path),
	))?;
	let config: Value = serde_json::from_str(&stdout)?;
	let cfg = config
		.get("config")
		.or_else(|| config.get("Config"))
		.and_then(Value::as_object)
		.cloned()
		.unwrap_or_default();
	Ok(ImageConfig {
		reference:  reference.to_owned(),
		entrypoint: string_array(cfg.get("Entrypoint")),
		cmd:        string_array(cfg.get("Cmd")),
		env:        string_array(cfg.get("Env")),
		workdir:    cfg
			.get("WorkingDir")
			.and_then(Value::as_str)
			.unwrap_or("/")
			.to_owned(),
		user:       cfg
			.get("User")
			.and_then(Value::as_str)
			.unwrap_or_default()
			.to_owned(),
	})
}

fn image_digest_oci(tools: &ImageTools, reference: &str, arch: &str) -> Result<String> {
	let auth = registry_auth::auth_file_for(reference);
	let stdout = run_stdout(&skopeo_inspect_digest_args(
		&tools.skopeo,
		reference,
		arch,
		auth.as_ref().map(AuthFile::path),
	))?;
	let info: Value = serde_json::from_str(&stdout)?;
	let digest = info
		.get("Digest")
		.and_then(Value::as_str)
		.map_or_else(|| hex::encode(Sha256::digest(reference.as_bytes())), ToOwned::to_owned);
	Ok(sanitize_digest(&digest))
}

fn export_oci_image(image: &PreparedImage, rootfs: &Path, work: &Path) -> Result<()> {
	let PreparedImageSource::Oci { transport_ref, tools, arch } = &image.source else {
		return Err(EngineError::engine("attempted to export a non-OCI image with skopeo"));
	};
	let oci_dir = work.join("oci");
	let bundle = work.join("bundle");
	let auth = registry_auth::auth_file_for(transport_ref);
	run_inherited(&skopeo_copy_args(
		&tools.skopeo,
		arch,
		transport_ref,
		&oci_dir,
		auth.as_ref().map(AuthFile::path),
	))?;
	run_inherited(&umoci_unpack_args(&tools.umoci, &oci_dir, &bundle, !is_root()))?;
	let unpacked_rootfs = bundle.join("rootfs");
	if !unpacked_rootfs.is_dir() {
		return Err(EngineError::engine(format!(
			"image unpack did not produce a rootfs: {}",
			unpacked_rootfs.display()
		)));
	}
	if rootfs.exists() {
		if rootfs.is_dir() {
			fs::remove_dir_all(rootfs)?;
		} else {
			fs::remove_file(rootfs)?;
		}
	}
	fs::rename(unpacked_rootfs, rootfs)?;
	Ok(())
}

fn inject_agent(rootfs: &Path, agent: &Path) -> Result<()> {
	let dst_dir = rootfs.join(".vmon");
	fs::create_dir_all(&dst_dir)?;
	let dst = dst_dir.join("agent");
	fs::copy(agent, &dst)?;
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(&dst, fs::Permissions::from_mode(0o755))?;
	}
	Ok(())
}

fn find_mkfs_ext4() -> Option<PathBuf> {
	for name in ["mkfs.ext4", "mke2fs"] {
		if let Some(found) = find_tool(name) {
			return Some(found);
		}
	}
	for directory in E2FSPROGS_DIRS {
		for name in ["mkfs.ext4", "mke2fs"] {
			let candidate = Path::new(directory).join(name);
			if is_executable(&candidate) {
				return Some(candidate);
			}
		}
	}
	None
}

fn mkfs_ext4(rootfs: &Path, out: &Path, disk_mb: u64) -> Result<()> {
	let mkfs = find_mkfs_ext4().ok_or_else(|| {
		EngineError::unsupported(
			"mkfs.ext4 not found (install e2fsprogs; on macOS: `brew install e2fsprogs`)",
		)
	})?;
	let file = File::create(out)?;
	file.set_len(disk_mb * 1024 * 1024)?;
	run_inherited(&mkfs_ext4_args(&mkfs, rootfs, out))
}

fn template_marker_current(
	marker: &Path,
	kernel_sha: &str,
	memory: u64,
	cpus: u64,
	fs_slots: u64,
	host_slot: bool,
	nic_slot: bool,
) -> bool {
	let Ok(text) = fs::read_to_string(marker) else {
		return false;
	};
	let Ok(data) = serde_json::from_str::<Value>(&text) else {
		return false;
	};
	data.get("boot_version").and_then(Value::as_u64) == Some(TEMPLATE_BOOT_VERSION)
		&& data.get("kernel_sha").and_then(Value::as_str) == Some(kernel_sha)
		&& data.get("memory").and_then(Value::as_u64) == Some(memory)
		&& data.get("cpus").and_then(Value::as_u64) == Some(cpus)
		&& data.get("fs_slots").and_then(Value::as_u64).unwrap_or(0) == fs_slots
		&& data
			.get("host_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false)
			== host_slot
		&& data
			.get("nic_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false)
			== nic_slot
}

fn marker_content_digest(marker: &Path) -> Option<String> {
	let data = serde_json::from_str::<Value>(&fs::read_to_string(marker).ok()?).ok()?;
	let value = data.get("content_digest")?.as_str()?;
	if value.len() == 64
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
	{
		Some(value.to_owned())
	} else {
		None
	}
}

fn store_marker_content_digest(marker: &Path, content_digest: &str) -> Result<()> {
	let mut data = serde_json::from_str::<Value>(&fs::read_to_string(marker)?)?;
	let Value::Object(map) = &mut data else {
		return Err(EngineError::engine("agent-ready.json marker is not an object"));
	};
	map.insert("content_digest".to_owned(), Value::String(content_digest.to_owned()));
	fs::write(marker, serde_json::to_string_pretty(&data)?)?;
	Ok(())
}

fn index_cached_template(template: &mut CachedTemplate, marker: &Path) -> Result<()> {
	if let Some(cached_digest) = marker_content_digest(marker)
		&& cas::lookup(&cached_digest)? == Some(template.snapshot_dir.clone())
	{
		template.digest = cached_digest;
		return Ok(());
	}
	let cached_digest = marker_content_digest(marker);
	let content_digest = cas::index_template(&template.snapshot_dir, None)?;
	template.digest.clone_from(&content_digest);
	if Some(content_digest.as_str()) != cached_digest.as_deref() {
		store_marker_content_digest(marker, &content_digest)?;
	}
	Ok(())
}

fn write_marker(
	marker: &Path,
	image: &str,
	digest: &str,
	agent_digest: &str,
	disk_mb: u64,
	kernel_sha: &str,
	fs_slots: u64,
	memory: u64,
	cpus: u64,
	host_slot: bool,
	nic_slot: bool,
	tap_slot: bool,
) -> Result<()> {
	if let Some(parent) = marker.parent() {
		fs::create_dir_all(parent)?;
	}
	let mut data = BTreeMap::new();
	data.insert("agent_digest", Value::String(agent_digest.to_owned()));
	data.insert("boot_version", Value::from(TEMPLATE_BOOT_VERSION));
	data.insert("cpus", Value::from(cpus));
	data.insert("digest", Value::String(digest.to_owned()));
	data.insert("disk_mb", Value::from(disk_mb));
	data.insert("fs_slots", Value::from(fs_slots));
	data.insert("host_slot", Value::from(host_slot));
	data.insert("image", Value::String(image.to_owned()));
	data.insert("kernel_sha", Value::String(kernel_sha.to_owned()));
	data.insert("memory", Value::from(memory));
	data.insert("nic_slot", Value::from(nic_slot));
	data.insert("tap_slot", Value::from(tap_slot));
	fs::write(marker, serde_json::to_string_pretty(&data)?)?;
	Ok(())
}

fn snapshot_state_present(snap_dir: &Path) -> bool {
	snap_dir.join("current-generation").is_file()
}

fn image_transport_ref(reference: &str) -> String {
	if IMAGE_TRANSPORT_PREFIXES
		.iter()
		.any(|prefix| reference.starts_with(prefix))
	{
		reference.to_owned()
	} else {
		format!("docker://{reference}")
	}
}

fn skopeo_arch(arch: Option<&str>) -> String {
	let machine = arch
		.unwrap_or(std::env::consts::ARCH)
		.trim()
		.to_ascii_lowercase()
		.replace('-', "_");
	match machine.as_str() {
		"x86_64" | "amd64" | "x64" => "amd64".to_owned(),
		"aarch64" | "arm64" => "arm64".to_owned(),
		other => other.to_owned(),
	}
}

fn sanitize_digest(digest: &str) -> String {
	let digest = digest.strip_prefix("sha256:").unwrap_or(digest);
	digest
		.chars()
		.map(|ch| {
			if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-') {
				ch
			} else {
				'-'
			}
		})
		.collect()
}

fn string_array(value: Option<&Value>) -> Vec<String> {
	value
		.and_then(Value::as_array)
		.map(|items| {
			items
				.iter()
				.filter_map(Value::as_str)
				.map(ToOwned::to_owned)
				.collect()
		})
		.unwrap_or_default()
}

fn manifest_arch_cache_path(reference: &str, digest: &str) -> PathBuf {
	let ref_key = hex::encode(Sha256::digest(reference.as_bytes()));
	let digest_key = sanitize_digest(digest);
	crate::home::state_dir()
		.join("manifest-arches")
		.join(ref_key)
		.join(format!("{digest_key}.json"))
}

fn read_manifest_arch_cache(reference: &str, digest: &str) -> Option<Vec<String>> {
	let path = manifest_arch_cache_path(reference, digest);
	let data = serde_json::from_str::<Value>(&fs::read_to_string(path).ok()?).ok()?;
	let arches = data.get("arches")?.as_array()?;
	let mut out = arches
		.iter()
		.filter_map(Value::as_str)
		.filter_map(|value| normalize_oci_arch(Some(value)))
		.collect::<Vec<_>>();
	out.sort();
	out.dedup();
	if out.is_empty() { None } else { Some(out) }
}

fn write_manifest_arch_cache(reference: &str, digest: &str, arches: &[String]) -> Result<()> {
	let path = manifest_arch_cache_path(reference, digest);
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}
	let tmp = path.with_extension("tmp");
	let mut data = BTreeMap::new();
	data.insert("reference", Value::String(reference.to_owned()));
	data.insert("digest", Value::String(digest.to_owned()));
	data.insert(
		"arches",
		Value::Array(
			arches
				.iter()
				.cloned()
				.map(Value::String)
				.collect::<Vec<_>>(),
		),
	);
	fs::write(&tmp, serde_json::to_string(&data)?)?;
	fs::rename(tmp, path)?;
	Ok(())
}

fn manifest_arches_from_raw(raw: &str, inspect_info: Option<&Value>) -> Result<Vec<String>> {
	let manifest: Value = serde_json::from_str(raw)?;
	let mut arches = Vec::new();
	if let Some(manifests) = manifest.get("manifests").and_then(Value::as_array) {
		for entry in manifests {
			let Some(platform) = entry.get("platform").and_then(Value::as_object) else {
				continue;
			};
			if let Some(os_name) = platform.get("os").and_then(Value::as_str)
				&& !os_name.is_empty()
				&& !os_name.eq_ignore_ascii_case("linux")
			{
				continue;
			}
			if let Some(arch) = platform
				.get("architecture")
				.and_then(Value::as_str)
				.and_then(|arch| normalize_oci_arch(Some(arch)))
			{
				arches.push(arch);
			}
		}
		arches.sort();
		arches.dedup();
		if !arches.is_empty() {
			return Ok(arches);
		}
	}
	for value in [
		manifest.get("architecture"),
		manifest
			.get("config")
			.and_then(|config| config.get("architecture")),
		inspect_info.and_then(|info| info.get("Architecture")),
	]
	.into_iter()
	.flatten()
	{
		if let Some(arch) = value
			.as_str()
			.and_then(|arch| normalize_oci_arch(Some(arch)))
		{
			return Ok(vec![arch]);
		}
	}
	Ok(Vec::new())
}

fn sha256_file(path: &Path) -> Result<String> {
	let bytes = fs::read(path)?;
	Ok(hex::encode(Sha256::digest(bytes)))
}

fn run_stdout(cmd: &[String]) -> Result<String> {
	let Some((program, args)) = cmd.split_first() else {
		return Err(EngineError::engine("empty command"));
	};
	let output = Command::new(program).args(args).output()?;
	if !output.status.success() {
		let code = output
			.status
			.code()
			.map_or_else(|| "signal".to_owned(), |code| code.to_string());
		let stderr = String::from_utf8_lossy(&output.stderr);
		let detail = stderr.trim();
		let message = if detail.is_empty() {
			format!("{program} failed with exit code {code}")
		} else {
			format!("{program} failed with exit code {code}: {detail}")
		};
		return Err(EngineError::engine(message));
	}
	String::from_utf8(output.stdout).map_err(|e| EngineError::engine(e.to_string()))
}

fn run_inherited(cmd: &[String]) -> Result<()> {
	let Some((program, args)) = cmd.split_first() else {
		return Err(EngineError::engine("empty command"));
	};
	let status = Command::new(program)
		.args(args)
		.stdin(Stdio::null())
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.status()?;
	if status.success() {
		Ok(())
	} else {
		Err(EngineError::engine(format!(
			"{program} failed with exit code {}",
			status
				.code()
				.map_or_else(|| "signal".to_owned(), |code| code.to_string())
		)))
	}
}

fn agent_arch(arch: Option<&str>) -> String {
	let machine = arch.unwrap_or(std::env::consts::ARCH);
	match machine
		.trim()
		.to_ascii_lowercase()
		.replace('-', "_")
		.as_str()
	{
		"aarch64" | "arm64" => "aarch64".to_owned(),
		_ => "x86_64".to_owned(),
	}
}

fn is_static_elf(path: &Path) -> Result<bool> {
	const PT_INTERP: u32 = 3;
	let mut file = File::open(path)?;
	let mut header = [0_u8; 64];
	let n = file.read(&mut header)?;
	if n < 16 || &header[..4] != b"\x7fELF" {
		return Ok(false);
	}
	let class = header[4];
	let data = header[5];
	if !matches!(data, 1 | 2) {
		return Ok(false);
	}
	let little = data == 1;
	let (phoff, phentsize, phnum) = match class {
		1 => {
			if n < 48 {
				return Ok(false);
			}
			(
				u64::from(read_u32(&header, 28, little)),
				read_u16(&header, 42, little),
				read_u16(&header, 44, little),
			)
		},
		2 => {
			if n < 64 {
				return Ok(false);
			}
			(
				read_u64(&header, 32, little),
				read_u16(&header, 54, little),
				read_u16(&header, 56, little),
			)
		},
		_ => return Ok(false),
	};
	if phoff == 0 || phnum == 0 {
		return Ok(true);
	}
	for i in 0..phnum {
		file.seek(SeekFrom::Start(phoff + u64::from(i) * u64::from(phentsize)))?;
		let mut entry = [0_u8; 4];
		if file.read(&mut entry)? < 4 {
			break;
		}
		let p_type = if little {
			u32::from_le_bytes(entry)
		} else {
			u32::from_be_bytes(entry)
		};
		if p_type == PT_INTERP {
			return Ok(false);
		}
	}
	Ok(true)
}

fn read_u16(bytes: &[u8], offset: usize, little: bool) -> u16 {
	let value = [bytes[offset], bytes[offset + 1]];
	if little {
		u16::from_le_bytes(value)
	} else {
		u16::from_be_bytes(value)
	}
}

fn read_u32(bytes: &[u8], offset: usize, little: bool) -> u32 {
	let value = [bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]];
	if little {
		u32::from_le_bytes(value)
	} else {
		u32::from_be_bytes(value)
	}
}

fn read_u64(bytes: &[u8], offset: usize, little: bool) -> u64 {
	let value = [
		bytes[offset],
		bytes[offset + 1],
		bytes[offset + 2],
		bytes[offset + 3],
		bytes[offset + 4],
		bytes[offset + 5],
		bytes[offset + 6],
		bytes[offset + 7],
	];
	if little {
		u64::from_le_bytes(value)
	} else {
		u64::from_be_bytes(value)
	}
}

fn musl_target_installed(target: &str) -> bool {
	let Some(rustup) = find_tool("rustup") else {
		return false;
	};
	let Ok(output) = Command::new(rustup)
		.args(["target", "list", "--installed"])
		.stdout(Stdio::piped())
		.stderr(Stdio::null())
		.output()
	else {
		return false;
	};
	if !output.status.success() {
		return false;
	}
	String::from_utf8_lossy(&output.stdout)
		.lines()
		.any(|line| line == target)
}

fn build_static_agent(arch: &str) -> bool {
	let Some(cargo) = find_tool("cargo") else {
		return false;
	};
	let target = format!("{arch}-unknown-linux-musl");
	if !musl_target_installed(&target) {
		return false;
	}
	let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.map_or_else(|| PathBuf::from("."), Path::to_path_buf);
	if !repo.join("Cargo.toml").is_file() {
		return false;
	}
	let cmd = if let Some(just) = find_tool("just").filter(|_| arch == agent_arch(None)) {
		vec![path_string(&just), "agent-musl".to_owned()]
	} else {
		vec![
			path_string(&cargo),
			"build".to_owned(),
			"-p".to_owned(),
			"vmon-agent".to_owned(),
			"--target".to_owned(),
			target,
			"--release".to_owned(),
		]
	};
	run_quiet_timeout(&cmd, &repo, AGENT_BUILD_TIMEOUT_SECS).is_ok()
}

fn run_quiet_timeout(cmd: &[String], cwd: &Path, timeout_secs: u64) -> Result<()> {
	let Some((program, args)) = cmd.split_first() else {
		return Err(EngineError::engine("empty command"));
	};
	let mut child = Command::new(program)
		.args(args)
		.current_dir(cwd)
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()?;
	let start = std::time::Instant::now();
	loop {
		if let Some(status) = child.try_wait()? {
			return if status.success() {
				Ok(())
			} else {
				Err(EngineError::engine(format!("{program} failed with {status}")))
			};
		}
		if start.elapsed().as_secs() >= timeout_secs {
			let _ = child.kill();
			let _ = child.wait();
			return Err(EngineError::engine(format!("{program} timed out")));
		}
		std::thread::sleep(std::time::Duration::from_millis(100));
	}
}

fn find_tool(name: &str) -> Option<PathBuf> {
	std::env::var_os("PATH")
		.into_iter()
		.flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
		.chain(EXTRA_TOOL_DIRS.iter().map(PathBuf::from))
		.map(|dir| dir.join(name))
		.find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
	if !path.is_file() {
		return false;
	}
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		path
			.metadata()
			.is_ok_and(|meta| meta.permissions().mode() & 0o111 != 0)
	}
	#[cfg(not(unix))]
	{
		true
	}
}

fn is_root() -> bool {
	#[cfg(unix)]
	{
		// SAFETY: geteuid has no preconditions and does not dereference pointers.
		unsafe { libc::geteuid() == 0 }
	}
	#[cfg(not(unix))]
	{
		false
	}
}

fn expand_home(value: &str) -> PathBuf {
	if let Some(rest) = value.strip_prefix("~/")
		&& let Some(home) = std::env::var_os("HOME")
	{
		return PathBuf::from(home).join(rest);
	}
	PathBuf::from(value)
}

fn temp_file_in(dir: &Path, prefix: &str) -> Result<PathBuf> {
	fs::create_dir_all(dir)?;
	for attempt in 0..100_u32 {
		let path = dir.join(format!("{prefix}{}-{attempt}-{}", std::process::id(), time_nanos()));
		match File::options().write(true).create_new(true).open(&path) {
			Ok(_) => return Ok(path),
			Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(e) => return Err(e.into()),
		}
	}
	Err(EngineError::engine("failed to create temporary file"))
}

fn digest_prefix(digest: &str, len: usize) -> &str {
	digest.get(..len).unwrap_or(digest)
}

fn path_string(path: &Path) -> String {
	path.to_string_lossy().into_owned()
}

fn time_nanos() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos())
}

struct TempDir {
	path: PathBuf,
}

impl TempDir {
	fn new(prefix: &str) -> Result<Self> {
		let parent = std::env::temp_dir();
		for attempt in 0..100_u32 {
			let path =
				parent.join(format!("{prefix}{}-{attempt}-{}", std::process::id(), time_nanos()));
			match fs::create_dir(&path) {
				Ok(()) => return Ok(Self { path }),
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
				Err(e) => return Err(e.into()),
			}
		}
		Err(EngineError::engine("failed to create temporary directory"))
	}

	fn path(&self) -> &Path {
		&self.path
	}
}

impl Drop for TempDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.path);
	}
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{Arc, Barrier, mpsc},
		thread,
		time::{Duration, Instant},
	};

	use super::*;

	fn wait_for_waiters(flights: &KeyedSingleflight<String, usize>, key: &str, expected: usize) {
		let deadline = Instant::now() + Duration::from_secs(1);
		while flights.waiter_count(&key.to_owned()) < expected {
			assert!(Instant::now() < deadline, "waiters did not join the in-flight request");
			thread::yield_now();
		}
	}

	#[test]
	fn same_key_singleflight_shares_one_materialization() {
		const REQUESTS: usize = 4;
		let flights = Arc::new(KeyedSingleflight::<String, usize>::default());
		let calls = Arc::new(AtomicUsize::new(0));
		let start = Arc::new(Barrier::new(REQUESTS));
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel();
		let release_rx = Arc::new(Mutex::new(release_rx));
		let mut workers = Vec::new();

		for _ in 0..REQUESTS {
			let flights = Arc::clone(&flights);
			let calls = Arc::clone(&calls);
			let start = Arc::clone(&start);
			let started_tx = started_tx.clone();
			let release_rx = Arc::clone(&release_rx);
			workers.push(thread::spawn(move || {
				start.wait();
				flights.run("same".to_owned(), || {
					calls.fetch_add(1, Ordering::Relaxed);
					started_tx.send(()).unwrap();
					release_rx.lock().recv().unwrap();
					Ok(42)
				})
			}));
		}
		drop(started_tx);

		started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
		wait_for_waiters(&flights, "same", REQUESTS - 1);
		release_tx.send(()).unwrap();
		for worker in workers {
			assert_eq!(worker.join().unwrap().unwrap(), 42);
		}
		assert_eq!(calls.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn independent_singleflight_keys_materialize_in_parallel() {
		let flights = Arc::new(KeyedSingleflight::<String, usize>::default());
		let calls = Arc::new(AtomicUsize::new(0));
		let start = Arc::new(Barrier::new(2));
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel();
		let release_rx = Arc::new(Mutex::new(release_rx));
		let mut workers = Vec::new();

		for (key, result) in [("first", 1), ("second", 2)] {
			let flights = Arc::clone(&flights);
			let calls = Arc::clone(&calls);
			let start = Arc::clone(&start);
			let started_tx = started_tx.clone();
			let release_rx = Arc::clone(&release_rx);
			workers.push(thread::spawn(move || {
				start.wait();
				flights.run(key.to_owned(), || {
					calls.fetch_add(1, Ordering::Relaxed);
					started_tx.send(key).unwrap();
					release_rx.lock().recv().unwrap();
					Ok(result)
				})
			}));
		}
		drop(started_tx);

		let mut started = vec![
			started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
			started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
		];
		started.sort_unstable();
		assert_eq!(started, ["first", "second"]);
		release_tx.send(()).unwrap();
		release_tx.send(()).unwrap();
		for worker in workers {
			worker.join().unwrap().unwrap();
		}
		assert_eq!(calls.load(Ordering::Relaxed), 2);
	}

	#[test]
	fn failed_singleflight_wakes_waiters_and_allows_retry() {
		let flights = Arc::new(KeyedSingleflight::<String, usize>::default());
		let calls = Arc::new(AtomicUsize::new(0));
		let start = Arc::new(Barrier::new(2));
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel();
		let release_rx = Arc::new(Mutex::new(release_rx));
		let mut workers = Vec::new();

		for _ in 0..2 {
			let flights = Arc::clone(&flights);
			let calls = Arc::clone(&calls);
			let start = Arc::clone(&start);
			let started_tx = started_tx.clone();
			let release_rx = Arc::clone(&release_rx);
			workers.push(thread::spawn(move || {
				start.wait();
				flights.run("failing".to_owned(), || {
					calls.fetch_add(1, Ordering::Relaxed);
					started_tx.send(()).unwrap();
					release_rx.lock().recv().unwrap();
					Err(EngineError::engine("materialization failed"))
				})
			}));
		}
		drop(started_tx);

		started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
		wait_for_waiters(&flights, "failing", 1);
		release_tx.send(()).unwrap();
		for worker in workers {
			assert_eq!(worker.join().unwrap().unwrap_err().message, "materialization failed");
		}
		assert_eq!(calls.load(Ordering::Relaxed), 1);

		assert_eq!(
			flights
				.run("failing".to_owned(), || {
					calls.fetch_add(1, Ordering::Relaxed);
					Ok(99)
				})
				.unwrap(),
			99
		);
		assert_eq!(calls.load(Ordering::Relaxed), 2);
	}

	#[test]
	fn parse_reference_rejects_empty_and_whitespace() {
		assert_eq!(parse_reference(None).unwrap(), None);
		assert_eq!(parse_reference(Some("  alpine:3.20  ")).unwrap(), Some("alpine:3.20".to_owned()));
		assert!(parse_reference(Some("bad ref")).is_err());
	}

	#[test]
	fn template_formatting_matches_python() {
		let image_digest = "abcdef1234567890";
		let agent_digest = "0011223344556677";
		assert_eq!(
			image_cache_key(image_digest, 1024, agent_digest),
			"abcdef1234567890-1024-a001122334455"
		);
		assert_eq!(
			template_name(image_digest, 1024, agent_digest, 512, 1, 0, false, false, false),
			"tpl-abcdef123456-1024-a001122334455-m512-c1"
		);
		assert_eq!(
			template_name(image_digest, 2048, agent_digest, 768, 2, 3, true, false, true),
			"tpl-abcdef123456-2048-a001122334455-m768-c2-s3-h-t"
		);
		assert_eq!(
			template_vm_name("tpl-abcdef123456-1024-a001122334455-m512-c1"),
			"_t-cac4617d1239a251"
		);
		assert_ne!(
			template_vm_name("tpl-abcdef123456-1024-a001122334455-m512-c1"),
			template_vm_name("tpl-abcdef123456-1024-a001122334455-m512-c2")
		);
		assert_eq!(slot_tag(7), "vmon_slot7");
	}

	#[test]
	fn image_tool_argv_matches_python() {
		assert_eq!(
			skopeo_inspect_config_args(Path::new("skopeo"), "alpine:latest", "aarch64", None),
			vec![
				"skopeo",
				"inspect",
				"--config",
				"--override-os",
				"linux",
				"--override-arch",
				"arm64",
				"docker://alpine:latest",
			]
		);
		assert_eq!(
			skopeo_copy_args(
				Path::new("/bin/skopeo"),
				"amd64",
				"docker://alpine",
				Path::new("/tmp/oci"),
				None
			),
			vec![
				"/bin/skopeo",
				"copy",
				"--override-os",
				"linux",
				"--override-arch",
				"amd64",
				"docker://alpine",
				"oci:/tmp/oci:latest",
			]
		);
		assert_eq!(
			umoci_unpack_args(Path::new("umoci"), Path::new("oci"), Path::new("bundle"), true),
			vec!["umoci", "unpack", "--rootless", "--image", "oci:latest", "bundle"]
		);
		assert_eq!(
			mkfs_ext4_args(Path::new("mke2fs"), Path::new("rootfs"), Path::new("out.img")),
			vec!["mke2fs", "-t", "ext4", "-q", "-F", "-d", "rootfs", "out.img"]
		);
	}
	#[test]
	fn registry_reference_detection_excludes_mutable_local_transports() {
		assert!(is_registry_reference("alpine:latest"));
		assert!(is_registry_reference("docker://registry.example/vmon:latest"));
		assert!(!is_registry_reference("oci:/tmp/layout:latest"));
		assert!(!is_registry_reference("dir:/tmp/image"));
		assert!(!is_registry_reference("containers-storage:vmon:latest"));
		assert!(!is_registry_reference("gs://sandbox-images/export.tar.gz"));
		assert!(is_gce_disk_image_reference("gs://sandbox-images/export.tar.gz"));
		assert!(!is_gce_disk_image_reference("oci:/tmp/layout:latest"));
		assert!(!is_gce_disk_image_reference("alpine:latest"));
	}

	#[test]
	fn gce_disk_references_never_reach_oci_inspection() {
		let reference = "gs://sandbox-images/export.tar.gz";
		assert_eq!(
			resolve_oci_reference(reference, None).unwrap_err().message,
			"gs:// references are GCE disk images, not OCI images"
		);
		assert_eq!(manifest_arches(reference), None);
	}

	#[test]
	fn command_failure_reports_stderr() {
		let error = run_stdout(&[
			"sh".to_owned(),
			"-c".to_owned(),
			"printf registry-unavailable >&2; exit 7".to_owned(),
		])
		.unwrap_err();
		assert!(error.message.contains("exit code 7"));
		assert!(error.message.contains("registry-unavailable"));
	}
}
