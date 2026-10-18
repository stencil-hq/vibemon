//! Host NVIDIA GPUs for sandboxes (ARCHITECTURE.md §8.2).
//!
//! Discovery scans `/sys/bus/pci/devices` for NVIDIA display functions and
//! publishes every assignable unit in `NodeCapacity::gpus`: a whole GPU (a
//! physical function without SR-IOV VFs) or an SR-IOV vGPU VF with a
//! pre-created profile. Units that sandboxes cannot use carry a `problem`.
//! [`GpuPool`] reserves units per sandbox; the VMM receives them as
//! `--vfio-gpu`, and the guest driver bundle ([`DriverBundle`]) is shared
//! read-only as virtio-fs tag [`BUNDLE_TAG`].

use std::{
	collections::{BTreeMap, HashMap},
	fs,
	io::Write,
	path::{Path, PathBuf},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vmon_cluster::GpuInfo;

use crate::server::ApiError;

/// Problem of a GPU whose VM exited without the guest unloading its driver:
/// the GSP state survives FLR (Blackwell), a bus reset drops the card off
/// the bus, so only a host reboot makes it usable again.
pub const NEEDS_RESET: &str = "needs host reboot: VM exited without releasing the GPU";
/// GPUs one sandbox may hold (the VMM's `MAX_VFIO_GPUS`).
pub const MAX_GPUS: u32 = 8;
/// virtio-fs tag of the guest driver bundle (mounted at `/run/vmon/gpu`).
pub const BUNDLE_TAG: &str = "vmon_gpu";
/// The bundle's description of itself.
const BUNDLE_MANIFEST: &str = "manifest.json";
/// Where the VMM expects `--vfio-gpu` functions.
const PCI_DEVICES: &str = "/sys/bus/pci/devices";
/// `pci.ids` locations, first match wins.
const PCI_IDS: [&str; 3] =
	["/usr/share/hwdata/pci.ids", "/usr/share/misc/pci.ids", "/usr/share/pci.ids"];
const NVIDIA_VENDOR: u32 = 0x10de;
const PCI_BASE_CLASS_DISPLAY: u32 = 0x03;
const PCI_CLASS_BRIDGE_PCI: u32 = 0x0604;
const VFIO_PCI: &str = "vfio-pci";

/// Host paths discovery reads (a fake tree in tests).
#[derive(Clone, Debug)]
pub struct HostPaths {
	/// `/sys/bus/pci/devices`.
	pub pci_devices: PathBuf,
	/// `/dev` (`iommu`, `vfio/devices/vfio<N>`).
	pub dev:         PathBuf,
	/// vfio-pci's `disable_idle_d3` module parameter.
	pub idle_d3:     PathBuf,
}

impl HostPaths {
	/// The real host.
	pub fn host() -> Self {
		Self {
			pci_devices: PathBuf::from(PCI_DEVICES),
			dev:         PathBuf::from("/dev"),
			idle_d3:     PathBuf::from("/sys/module/vfio_pci/parameters/disable_idle_d3"),
		}
	}
}

/// `manifest.json` of a guest driver bundle (`kernel/build.sh --nvidia`).
#[derive(Debug, Deserialize)]
struct BundleManifest {
	/// NVIDIA driver version.
	driver: String,
	/// `uname -r` of the guest kernel the modules were built for.
	kernel: String,
}

/// The guest NVIDIA driver bundle (`vmon-nvidia-<arch>/`) and the release
/// of the kernel the node boots, which the bundle's modules must match.
#[derive(Clone, Debug, Default)]
pub struct DriverBundle {
	/// `None` when no bundle was found.
	pub dir:      Option<PathBuf>,
	/// Release of the node's guest kernel; `None` when unknown (then any
	/// bundle is accepted and the guest agent checks).
	pub kernel:   Option<String>,
	/// Name of the bundle directory looked for, for the missing-bundle
	/// problem.
	pub searched: String,
}

impl DriverBundle {
	/// Why GPUs are unusable because of the bundle; `None` when it is fine.
	/// Re-read on every call so a bundle installed or replaced later counts.
	fn problem(&self) -> Option<String> {
		let Some(dir) = &self.dir else {
			return Some(format!(
				"no guest driver bundle ({}; pass --gpu-driver or run `just gpu-driver`)",
				self.searched
			));
		};
		let manifest = match fs::read(dir.join(BUNDLE_MANIFEST)) {
			Ok(bytes) => bytes,
			Err(error) => {
				return Some(format!(
					"no guest driver bundle ({}: {error})",
					dir.join(BUNDLE_MANIFEST).display()
				));
			},
		};
		let manifest: BundleManifest = match serde_json::from_slice(&manifest) {
			Ok(manifest) => manifest,
			Err(error) => {
				return Some(format!(
					"invalid guest driver bundle manifest in {}: {error}",
					dir.display()
				));
			},
		};
		match &self.kernel {
			Some(kernel) if *kernel != manifest.kernel => Some(format!(
				"guest driver bundle {} is built for kernel {}, the node boots {kernel}",
				manifest.driver, manifest.kernel
			)),
			_ => None,
		}
	}
}

/// Release string (`uname -r`) of a guest kernel image: from the x86 boot
/// protocol header of a bzImage, else the `Linux version …` banner of an
/// uncompressed image (arm64 `Image`).
pub fn kernel_release(image: &[u8]) -> Option<String> {
	let token = |bytes: &[u8]| {
		let end = bytes
			.iter()
			.position(|&b| b == b' ' || b == 0 || b == b'\n')
			.unwrap_or(bytes.len());
		let release = std::str::from_utf8(&bytes[..end]).ok()?;
		(!release.is_empty()).then(|| release.to_owned())
	};
	if image.get(0x202..0x206) == Some(b"HdrS") {
		let offset = usize::from(u16::from_le_bytes([*image.get(0x20e)?, *image.get(0x20f)?]));
		return token(image.get(offset + 0x200..)?);
	}
	const BANNER: &[u8] = b"Linux version ";
	let at = image
		.windows(BANNER.len())
		.position(|window| window == BANNER)?;
	token(&image[at + BANNER.len()..])
}

/// NVIDIA device names from `pci.ids` (`device id → name`).
#[derive(Clone, Debug, Default)]
pub struct Models(HashMap<u32, String>);

impl Models {
	/// Loads the first readable `pci.ids` of the host.
	fn host() -> Self {
		PCI_IDS
			.iter()
			.find_map(|path| fs::read_to_string(path).ok())
			.map_or_else(Self::default, |text| Self::parse(&text))
	}

	/// Parses the NVIDIA vendor block of a `pci.ids` file.
	pub fn parse(text: &str) -> Self {
		let mut devices = HashMap::new();
		let mut in_vendor = false;
		for line in text.lines() {
			if line.is_empty() || line.starts_with('#') {
				continue;
			}
			if !line.starts_with('\t') {
				if in_vendor {
					break;
				}
				in_vendor = line
					.split_whitespace()
					.next()
					.and_then(|id| u32::from_str_radix(id, 16).ok())
					== Some(NVIDIA_VENDOR);
				continue;
			}
			// Subsystem lines (two tabs) belong to the device above.
			if !in_vendor || line.starts_with("\t\t") {
				continue;
			}
			let line = line.trim_start_matches('\t');
			let Some((id, name)) = line.split_once(char::is_whitespace) else {
				continue;
			};
			if let Ok(id) = u32::from_str_radix(id, 16) {
				devices.insert(id, name.trim().to_owned());
			}
		}
		Self(devices)
	}

	/// Model name of NVIDIA device `device`.
	fn name(&self, device: u32) -> String {
		self
			.0
			.get(&device)
			.cloned()
			.unwrap_or_else(|| format!("NVIDIA 10de:{device:04x}"))
	}
}

fn read_trimmed(path: &Path) -> Option<String> {
	fs::read_to_string(path)
		.ok()
		.map(|text| text.trim().to_owned())
}

fn read_hex(path: &Path) -> Option<u32> {
	u32::from_str_radix(read_trimmed(path)?.strip_prefix("0x")?, 16).ok()
}

/// Driver bound to a sysfs PCI function, if any.
fn bound_driver(function: &Path) -> Option<String> {
	fs::read_link(function.join("driver"))
		.ok()?
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
}

/// Why the IOMMU group of a whole GPU is not viable: the first member other
/// than the GPU that is bound to something else than vfio-pci (bridges may
/// keep `pcieport`).
fn group_problem(function: &Path, address: &str) -> Option<String> {
	let group = function.join("iommu_group");
	let Ok(target) = fs::read_link(&group) else {
		return Some(
			"IOMMU is off or the GPU has no IOMMU group (enable intel_iommu/amd_iommu)".into(),
		);
	};
	let group_name = target
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.unwrap_or_default();
	let Ok(entries) = fs::read_dir(group.join("devices")) else {
		return Some(format!("IOMMU group {group_name} is unreadable"));
	};
	let mut members: Vec<_> = entries
		.filter_map(Result::ok)
		.map(|entry| (entry.file_name().to_string_lossy().into_owned(), entry.path()))
		.filter(|(name, _)| name != address)
		.collect();
	members.sort();
	for (name, path) in members {
		let driver = bound_driver(&path);
		let viable = match driver.as_deref() {
			None | Some(VFIO_PCI) => true,
			Some("pcieport") => {
				read_hex(&path.join("class")).is_some_and(|class| class >> 8 == PCI_CLASS_BRIDGE_PCI)
			},
			Some(_) => false,
		};
		if !viable {
			return Some(format!(
				"IOMMU group {group_name} member {name} is bound to {}, not vfio-pci",
				driver.unwrap_or_default()
			));
		}
	}
	None
}

/// Why the function has no usable VFIO cdev (`<function>/vfio-dev/vfio<N>`
/// with `/dev/vfio/devices/vfio<N>`).
fn cdev_problem(function: &Path, dev: &Path) -> Option<String> {
	let names: Vec<String> = fs::read_dir(function.join("vfio-dev"))
		.map(|entries| {
			entries
				.filter_map(Result::ok)
				.map(|entry| entry.file_name().to_string_lossy().into_owned())
				.collect()
		})
		.unwrap_or_default();
	let [name] = names.as_slice() else {
		return Some("no VFIO cdev (vfio-dev); the host kernel needs CONFIG_VFIO_DEVICE_CDEV".into());
	};
	let node = dev.join("vfio/devices").join(name);
	device_problem(&node, "VFIO cdev")
}

/// Why the VMM (spawned by this process; root VMMs open devices before
/// dropping privileges) cannot open the device node `path` read/write.
fn device_problem(path: &Path, what: &str) -> Option<String> {
	if !path.exists() {
		return Some(format!("no {what} {}", path.display()));
	}
	let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
		return Some(format!("invalid path {}", path.display()));
	};
	// SAFETY: access(2) only reads the NUL-terminated path.
	let accessible = unsafe { libc::access(c_path.as_ptr(), libc::R_OK | libc::W_OK) } == 0;
	(!accessible).then(|| {
		format!(
			"no read/write access to {} (run vmon as root or grant the node user access)",
			path.display()
		)
	})
}

/// Every assignable NVIDIA unit under `paths`, sorted by address, with its
/// first problem: not on vfio-pci, IOMMU off, group not viable, idle D3, no
/// VFIO cdev, no `/dev/iommu`, then `shared` (bundle or host problems that
/// apply to every unit). VFs skip the driver, group, and D3 checks like the
/// VMM does.
fn discover(paths: &HostPaths, models: &Models, shared: Option<&str>) -> Vec<GpuInfo> {
	let Ok(entries) = fs::read_dir(&paths.pci_devices) else {
		return Vec::new();
	};
	let mut gpus = Vec::new();
	for entry in entries.filter_map(Result::ok) {
		let address = entry.file_name().to_string_lossy().into_owned();
		let function = entry.path();
		if read_hex(&function.join("vendor")) != Some(NVIDIA_VENDOR)
			|| read_hex(&function.join("class")).map(|class| class >> 16)
				!= Some(PCI_BASE_CLASS_DISPLAY)
		{
			continue;
		}
		let vf = fs::symlink_metadata(function.join("physfn")).is_ok();
		if vf {
			// Only VFs with a pre-created vGPU profile are units.
			let profile = read_trimmed(&function.join("nvidia/current_vgpu_type"))
				.and_then(|value| value.parse::<u32>().ok());
			if profile.is_none_or(|profile| profile == 0) {
				continue;
			}
		} else if fs::read_dir(&function).is_ok_and(|entries| {
			entries
				.filter_map(Result::ok)
				.any(|entry| entry.file_name().to_string_lossy().starts_with("virtfn"))
		}) {
			// SR-IOV enabled: its VFs are the units.
			continue;
		}
		let model = read_hex(&function.join("device"))
			.map_or_else(|| "NVIDIA GPU".to_owned(), |device| models.name(device));
		let problem = (!vf)
			.then(|| match bound_driver(&function) {
				Some(driver) if driver == VFIO_PCI => {
					group_problem(&function, &address).or_else(|| idle_d3_problem(&paths.idle_d3))
				},
				Some(driver) => Some(format!("bound to {driver}, not vfio-pci")),
				None => Some("no driver bound (bind it to vfio-pci)".to_owned()),
			})
			.flatten()
			.or_else(|| cdev_problem(&function, &paths.dev))
			.or_else(|| device_problem(&paths.dev.join("iommu"), "iommufd"))
			.or_else(|| shared.map(str::to_owned))
			.unwrap_or_default();
		gpus.push(GpuInfo { address, model, sandbox: None, problem });
	}
	gpus.sort_by(|a, b| a.address.cmp(&b.address));
	gpus
}

/// vfio-pci powers idle devices down to D3hot between VMs. A Blackwell GPU
/// left in D3hot after a guest unloaded its driver fails the next open with a
/// config-read timeout that firmware-first error handling escalates to a host
/// reset (seen on an RTX 5070), so whole GPUs need `disable_idle_d3`.
fn idle_d3_problem(param: &Path) -> Option<String> {
	(read_trimmed(param).as_deref() != Some("Y")).then(|| {
		"vfio-pci idles GPUs in D3 between VMs, which can hang the host on the next VM; set \
		 vfio-pci.disable_idle_d3=1"
			.to_owned()
	})
}

/// Stable RFC 4122 (version 8) UUID of a sandbox VM, exposed to NVIDIA's
/// guest driver as the SMBIOS system UUID (`--vm-uuid`).
pub fn vm_uuid(sandbox_id: &str) -> String {
	let digest = Sha256::digest(format!("vmon-sandbox-vm:{sandbox_id}"));
	let mut bytes = [0u8; 16];
	bytes.copy_from_slice(&digest[..16]);
	bytes[6] = (bytes[6] & 0x0f) | 0x80;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	let hex = hex::encode(bytes);
	format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..])
}

/// `gpu-quarantine.json`: GPUs that need a host reboot ([`NEEDS_RESET`]),
/// valid for the host boot that wrote it, so it survives node restarts and
/// clears itself once the host reboots.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuarantineFile {
	boot_id: String,
	/// Address → sandbox whose VM left it dirty.
	gpus:    BTreeMap<String, String>,
}

/// Where quarantined GPUs persist.
#[derive(Debug)]
struct QuarantineStore {
	file:    PathBuf,
	/// `/proc/sys/kernel/random/boot_id` of this host boot.
	boot_id: String,
}

impl QuarantineStore {
	fn load(&self) -> BTreeMap<String, String> {
		fs::read(&self.file)
			.ok()
			.and_then(|bytes| serde_json::from_slice::<QuarantineFile>(&bytes).ok())
			.filter(|file| file.boot_id == self.boot_id)
			.map(|file| file.gpus)
			.unwrap_or_default()
	}

	fn save(&self, gpus: BTreeMap<String, String>) -> std::io::Result<()> {
		let tmp = self.file.with_extension("json.tmp");
		let mut file = fs::File::create(&tmp)?;
		file.write_all(&serde_json::to_vec_pretty(&QuarantineFile {
			boot_id: self.boot_id.clone(),
			gpus,
		})?)?;
		file.sync_all()?;
		fs::rename(&tmp, &self.file)
	}
}

/// Host GPUs and their reservations. Discovery reruns on every
/// [`Self::list`] and [`Self::reserve`], so binding changes count at once.
#[derive(Debug)]
pub struct GpuPool {
	/// `None`: the host cannot have GPUs (non-Linux).
	paths:      Option<HostPaths>,
	models:     Models,
	bundle:     DriverBundle,
	/// Why no unit is usable on this host regardless of its state.
	host:       Option<String>,
	/// Address → holding sandbox.
	held:       Mutex<HashMap<String, String>>,
	/// Quarantined GPUs when not persisted (address → sandbox).
	quarantine: Mutex<BTreeMap<String, String>>,
	persisted:  Option<QuarantineStore>,
}

impl GpuPool {
	/// The real host: discovery on Linux (whole-GPU passthrough needs an
	/// `x86_64` KVM host), nothing elsewhere. Quarantined GPUs persist in
	/// `<data_dir>/gpu-quarantine.json` for this host boot.
	pub fn host(bundle: DriverBundle, data_dir: &Path) -> Self {
		if !cfg!(target_os = "linux") {
			return Self::empty();
		}
		let host = (!cfg!(target_arch = "x86_64"))
			.then(|| "unsupported host: GPU passthrough needs an x86_64 Linux KVM host".to_owned());
		let pool = Self::new(HostPaths::host(), Models::host(), bundle, host);
		match read_trimmed(Path::new("/proc/sys/kernel/random/boot_id")) {
			Some(boot_id) => pool.persist_quarantine(data_dir.join("gpu-quarantine.json"), boot_id),
			None => pool,
		}
	}

	/// A pool over `paths`; `host` is a problem every unit gets. Quarantine
	/// stays in memory unless [`Self::persist_quarantine`].
	pub fn new(
		paths: HostPaths,
		models: Models,
		bundle: DriverBundle,
		host: Option<String>,
	) -> Self {
		Self {
			paths: Some(paths),
			models,
			bundle,
			host,
			held: Mutex::default(),
			quarantine: Mutex::default(),
			persisted: None,
		}
	}

	/// Keeps quarantined GPUs in `file`, valid while the host's boot id is
	/// `boot_id`.
	#[must_use]
	pub fn persist_quarantine(mut self, file: PathBuf, boot_id: String) -> Self {
		self.persisted = Some(QuarantineStore { file, boot_id });
		self
	}

	/// A pool without GPUs.
	pub fn empty() -> Self {
		Self {
			paths:      None,
			models:     Models::default(),
			bundle:     DriverBundle::default(),
			host:       None,
			held:       Mutex::default(),
			quarantine: Mutex::default(),
			persisted:  None,
		}
	}

	fn quarantined(&self) -> BTreeMap<String, String> {
		match &self.persisted {
			Some(store) => store.load(),
			None => self.quarantine.lock().clone(),
		}
	}

	/// Frees `gpus` of `sandbox` whose VM exited with the guest driver still
	/// loaded, marking them [`NEEDS_RESET`] so no sandbox gets them until the
	/// host reboots.
	pub fn quarantine(&self, sandbox: &str, gpus: &[String]) {
		if gpus.is_empty() {
			return;
		}
		tracing::error!(
			sandbox,
			gpus = ?gpus,
			"VM exited without releasing its GPUs; quarantined until the host reboots"
		);
		{
			let _held = self.held.lock();
			let add = |entries: &mut BTreeMap<String, String>| {
				for address in gpus {
					entries.insert(address.clone(), sandbox.to_owned());
				}
			};
			if let Some(store) = &self.persisted {
				let mut entries = store.load();
				add(&mut entries);
				if let Err(error) = store.save(entries) {
					tracing::warn!(file = %store.file.display(), %error, "persisting GPU quarantine failed");
				}
			} else {
				add(&mut self.quarantine.lock());
			}
		}
		self.release(sandbox, gpus);
	}

	/// Bundle directory shared with GPU sandboxes.
	pub fn bundle_dir(&self) -> Option<&Path> {
		self.bundle.dir.as_deref()
	}

	/// `--vfio-gpu` path of a unit.
	pub fn sysfs_path(&self, address: &str) -> PathBuf {
		self
			.paths
			.as_ref()
			.map_or_else(|| PathBuf::from(PCI_DEVICES), |paths| paths.pci_devices.clone())
			.join(address)
	}

	fn discover(&self) -> Vec<GpuInfo> {
		let Some(paths) = &self.paths else {
			return Vec::new();
		};
		let shared = self.bundle.problem().or_else(|| self.host.clone());
		let mut gpus = discover(paths, &self.models, shared.as_deref());
		let quarantined = self.quarantined();
		for gpu in &mut gpus {
			if quarantined.contains_key(&gpu.address) {
				NEEDS_RESET.clone_into(&mut gpu.problem);
			}
		}
		gpus
	}

	/// Every unit with its holder (capacity publishing).
	pub fn list(&self) -> Vec<GpuInfo> {
		let mut gpus = self.discover();
		let held = self.held.lock();
		for gpu in &mut gpus {
			gpu.sandbox = held.get(&gpu.address).cloned();
		}
		gpus
	}

	/// Reserves `count` free, assignable units for `sandbox` (all or none).
	/// Too few is `busy`, so the driver tries another node.
	pub fn reserve(&self, sandbox: &str, count: u32) -> Result<Vec<String>, ApiError> {
		if count == 0 {
			return Ok(Vec::new());
		}
		if count > MAX_GPUS {
			return Err(ApiError::invalid(format!(
				"at most {MAX_GPUS} GPUs per sandbox (got {count})"
			)));
		}
		let gpus = self.discover();
		let mut held = self.held.lock();
		let free: Vec<String> = gpus
			.iter()
			.filter(|gpu| gpu.problem.is_empty() && !held.contains_key(&gpu.address))
			.map(|gpu| gpu.address.clone())
			.take(count as usize)
			.collect();
		if free.len() < count as usize {
			let assignable = gpus.iter().filter(|gpu| gpu.problem.is_empty()).count();
			return Err(ApiError::busy(format!(
				"sandbox {sandbox} needs {count} GPU(s); this node has {} free of {assignable} \
				 assignable ({} found)",
				free.len(),
				gpus.len()
			)));
		}
		for address in &free {
			held.insert(address.clone(), sandbox.to_owned());
		}
		Ok(free)
	}

	/// Re-registers units a running sandbox already holds (restart
	/// recovery), whatever their current problem. Fails if another sandbox
	/// holds one of them.
	pub fn claim(&self, sandbox: &str, gpus: &[String]) -> Result<(), String> {
		let mut held = self.held.lock();
		if let Some((address, holder)) = gpus.iter().find_map(|address| {
			held
				.get(address)
				.filter(|holder| *holder != sandbox)
				.map(|holder| (address, holder))
		}) {
			return Err(format!("GPU {address} is held by sandbox {holder}"));
		}
		for address in gpus {
			held.insert(address.clone(), sandbox.to_owned());
		}
		Ok(())
	}

	/// Frees `gpus` where `sandbox` still holds them.
	pub fn release(&self, sandbox: &str, gpus: &[String]) {
		let mut held = self.held.lock();
		for address in gpus {
			if held.get(address).is_some_and(|holder| holder == sandbox) {
				held.remove(address);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::os::unix::fs::symlink;

	use super::*;

	const PCI_IDS_SAMPLE: &str = "\
# comment
10de  NVIDIA Corporation
\t2f04  GB205 [GeForce RTX 5070]
\t\t1043 8a14  Subsystem name that is not a device
\t2f80  GB205 High Definition Audio Controller
\t20b0  GA100 [A100 SXM4 40GB]
10df  Emulex Corporation
\t2f05  Not NVIDIA
";

	/// A fake `/sys` + `/dev` tree.
	struct Fake {
		root:  tempfile::TempDir,
		paths: HostPaths,
	}

	impl Fake {
		fn new() -> Self {
			let root = tempfile::tempdir().unwrap();
			let paths = HostPaths {
				pci_devices: root.path().join("sys/bus/pci/devices"),
				dev:         root.path().join("dev"),
				idle_d3:     root.path().join("disable_idle_d3"),
			};
			fs::write(&paths.idle_d3, b"Y\n").unwrap();
			fs::create_dir_all(&paths.pci_devices).unwrap();
			fs::create_dir_all(paths.dev.join("vfio/devices")).unwrap();
			fs::create_dir_all(root.path().join("sys/bus/pci/drivers")).unwrap();
			fs::write(paths.dev.join("iommu"), b"").unwrap();
			Self { root, paths }
		}

		fn function(&self, address: &str) -> PathBuf {
			self
				.root
				.path()
				.join("sys/devices/pci0000:00")
				.join(address)
		}

		/// A PCI function bound to `driver`, with its `/sys/bus/pci/devices`
		/// link.
		fn add(
			&self,
			address: &str,
			vendor: u32,
			device: u32,
			class: u32,
			driver: Option<&str>,
		) -> PathBuf {
			let function = self.function(address);
			fs::create_dir_all(&function).unwrap();
			fs::write(function.join("vendor"), format!("0x{vendor:04x}\n")).unwrap();
			fs::write(function.join("device"), format!("0x{device:04x}\n")).unwrap();
			fs::write(function.join("class"), format!("0x{class:06x}\n")).unwrap();
			if let Some(driver) = driver {
				let dir = self.root.path().join("sys/bus/pci/drivers").join(driver);
				fs::create_dir_all(&dir).unwrap();
				symlink(&dir, function.join("driver")).unwrap();
			}
			symlink(&function, self.paths.pci_devices.join(address)).unwrap();
			function
		}

		/// Puts `members` into IOMMU group `group`.
		fn group(&self, group: u32, members: &[&str]) {
			let dir = self
				.root
				.path()
				.join(format!("sys/kernel/iommu_groups/{group}"));
			fs::create_dir_all(dir.join("devices")).unwrap();
			for member in members {
				let function = self.function(member);
				symlink(&function, dir.join("devices").join(member)).unwrap();
				symlink(&dir, function.join("iommu_group")).unwrap();
			}
		}

		fn cdev(&self, address: &str, index: u32) {
			let name = format!("vfio{index}");
			fs::create_dir_all(self.function(address).join("vfio-dev").join(&name)).unwrap();
			fs::write(self.paths.dev.join("vfio/devices").join(&name), b"").unwrap();
		}

		/// A whole RTX 5070 + audio on vfio-pci in one group, with cdevs.
		fn rtx(&self, bus: u32, group: u32) -> String {
			let gpu = format!("0000:{bus:02x}:00.0");
			let audio = format!("0000:{bus:02x}:00.1");
			self.add(&gpu, 0x10de, 0x2f04, 0x03_0000, Some("vfio-pci"));
			self.add(&audio, 0x10de, 0x2f80, 0x04_0300, Some("vfio-pci"));
			self.group(group, &[&gpu, &audio]);
			self.cdev(&gpu, group * 2);
			self.cdev(&audio, group * 2 + 1);
			gpu
		}

		fn bundle(&self, kernel: &str) -> DriverBundle {
			let dir = self.root.path().join("vmon-nvidia-x86_64");
			fs::create_dir_all(&dir).unwrap();
			fs::write(
				dir.join(BUNDLE_MANIFEST),
				format!(r#"{{"driver": "615.71.09", "kernel": "{kernel}"}}"#),
			)
			.unwrap();
			DriverBundle {
				dir:      Some(dir),
				kernel:   Some("6.16.9-vmon".into()),
				searched: "vmon-nvidia-x86_64".into(),
			}
		}

		fn pool(&self) -> GpuPool {
			GpuPool::new(
				self.paths.clone(),
				Models::parse(PCI_IDS_SAMPLE),
				self.bundle("6.16.9-vmon"),
				None,
			)
		}

		fn problems(&self) -> Vec<(String, String)> {
			self
				.pool()
				.list()
				.into_iter()
				.map(|gpu| (gpu.address, gpu.problem))
				.collect()
		}
	}

	#[test]
	fn pci_ids_parse_only_nvidia_devices() {
		let models = Models::parse(PCI_IDS_SAMPLE);
		assert_eq!(models.name(0x2f04), "GB205 [GeForce RTX 5070]");
		assert_eq!(models.name(0x20b0), "GA100 [A100 SXM4 40GB]");
		// Another vendor's device id and unknown ids fall back.
		assert_eq!(models.name(0x2f05), "NVIDIA 10de:2f05");
		assert_eq!(Models::default().name(0x2f04), "NVIDIA 10de:2f04");
	}

	#[test]
	fn whole_gpu_on_vfio_is_assignable_and_audio_is_not_a_unit() {
		let fake = Fake::new();
		let gpu = fake.rtx(0x52, 15);
		// A non-NVIDIA display function and an NVIDIA non-display one.
		fake.add("0000:00:02.0", 0x8086, 0x1234, 0x03_0000, Some("i915"));
		let gpus = fake.pool().list();
		assert_eq!(gpus, [GpuInfo {
			address: gpu,
			model:   "GB205 [GeForce RTX 5070]".into(),
			sandbox: None,
			problem: String::new(),
		}]);
	}

	#[test]
	fn problems_are_reported_in_order() {
		let fake = Fake::new();
		// Host driver.
		fake.add("0000:01:00.0", 0x10de, 0x2f04, 0x03_0000, Some("nvidia"));
		// No driver.
		fake.add("0000:02:00.0", 0x10de, 0x2f04, 0x03_0000, None);
		// vfio-pci without an IOMMU group.
		fake.add("0000:03:00.0", 0x10de, 0x2f04, 0x03_0000, Some("vfio-pci"));
		// Group member on its host driver; a pcieport bridge is fine.
		fake.add("0000:04:00.0", 0x10de, 0x2f04, 0x03_0000, Some("vfio-pci"));
		fake.add("0000:04:00.1", 0x10de, 0x2f80, 0x04_0300, Some("snd_hda_intel"));
		fake.add("0000:00:04.0", 0x8086, 0x1111, 0x06_0400, Some("pcieport"));
		fake.group(4, &["0000:00:04.0", "0000:04:00.0", "0000:04:00.1"]);
		// Viable group (bridge + unbound member) but no cdev.
		fake.add("0000:05:00.0", 0x10de, 0x2f04, 0x03_0000, Some("vfio-pci"));
		fake.add("0000:05:00.1", 0x10de, 0x2f80, 0x04_0300, None);
		fake.add("0000:00:05.0", 0x8086, 0x1111, 0x06_0400, Some("pcieport"));
		fake.group(5, &["0000:00:05.0", "0000:05:00.0", "0000:05:00.1"]);
		// A pcieport that is not a bridge does not make the group viable.
		fake.add("0000:06:00.0", 0x10de, 0x2f04, 0x03_0000, Some("vfio-pci"));
		fake.add("0000:00:06.0", 0x8086, 0x2222, 0x08_8000, Some("pcieport"));
		fake.group(6, &["0000:00:06.0", "0000:06:00.0"]);
		let problems = fake.problems();
		let problem = |address: &str| {
			problems
				.iter()
				.find(|(a, _)| a == address)
				.map(|(_, p)| p.as_str())
				.unwrap()
		};
		assert_eq!(problem("0000:01:00.0"), "bound to nvidia, not vfio-pci");
		assert!(problem("0000:02:00.0").starts_with("no driver bound"));
		assert!(problem("0000:03:00.0").starts_with("IOMMU is off"));
		assert_eq!(
			problem("0000:04:00.0"),
			"IOMMU group 4 member 0000:04:00.1 is bound to snd_hda_intel, not vfio-pci"
		);
		assert!(problem("0000:05:00.0").starts_with("no VFIO cdev"), "{}", problem("0000:05:00.0"));
		assert!(problem("0000:06:00.0").contains("member 0000:00:06.0 is bound to pcieport"));
		assert_eq!(problems.len(), 6, "{problems:?}");
	}

	#[test]
	fn whole_gpus_need_vfio_idle_d3_disabled() {
		let fake = Fake::new();
		let gpu = fake.rtx(0x52, 15);
		fs::write(&fake.paths.idle_d3, b"N\n").unwrap();
		assert!(fake.problems()[0].1.contains("vfio-pci.disable_idle_d3=1"), "{:?}", fake.problems());
		fs::remove_file(&fake.paths.idle_d3).unwrap();
		assert!(fake.problems()[0].1.contains("disable_idle_d3"), "vfio-pci not loaded");
		fs::write(&fake.paths.idle_d3, b"Y\n").unwrap();
		assert_eq!(fake.problems(), [(gpu, String::new())]);
	}

	#[test]
	fn missing_iommufd_bundle_and_host_problems_follow_device_problems() {
		let fake = Fake::new();
		let gpu = fake.rtx(0x52, 15);
		fake.add("0000:01:00.0", 0x10de, 0x2f04, 0x03_0000, Some("nvidia"));

		let mut pool = fake.pool();
		pool.bundle.dir = None;
		let gpus = pool.list();
		assert!(gpus[0].problem.starts_with("bound to nvidia"), "device problems come first");
		assert!(
			gpus[1]
				.problem
				.starts_with("no guest driver bundle (vmon-nvidia-x86_64")
		);

		let pool =
			GpuPool::new(fake.paths.clone(), Models::default(), fake.bundle("6.12.8-other"), None);
		assert_eq!(
			pool.list()[1].problem,
			"guest driver bundle 615.71.09 is built for kernel 6.12.8-other, the node boots \
			 6.16.9-vmon"
		);

		let pool = GpuPool::new(
			fake.paths.clone(),
			Models::default(),
			fake.bundle("6.16.9-vmon"),
			Some("unsupported host: test".into()),
		);
		assert_eq!(pool.list()[1].problem, "unsupported host: test");

		fs::remove_file(fake.paths.dev.join("iommu")).unwrap();
		let gpus = fake.pool().list();
		assert_eq!(gpus[1].address, gpu);
		assert!(gpus[1].problem.contains("dev/iommu"), "{}", gpus[1].problem);
	}

	#[test]
	fn vgpu_vfs_with_a_profile_are_units_and_their_pf_is_not() {
		let fake = Fake::new();
		let pf = fake.add("0000:3d:00.0", 0x10de, 0x20b0, 0x03_0200, Some("nvidia"));
		for (index, profile) in [(4, "557"), (5, "0"), (6, "")] {
			let address = format!("0000:3d:00.{index}");
			let vf = fake.add(&address, 0x10de, 0x20b0, 0x03_0200, Some("nvidia"));
			symlink(&pf, vf.join("physfn")).unwrap();
			symlink(&vf, pf.join(format!("virtfn{}", index - 4))).unwrap();
			fs::create_dir_all(vf.join("nvidia")).unwrap();
			fs::write(vf.join("nvidia/current_vgpu_type"), format!("{profile}\n")).unwrap();
			fake.cdev(&address, index);
		}
		let gpus = fake.pool().list();
		assert_eq!(gpus.len(), 1, "{gpus:?}");
		assert_eq!(gpus[0].address, "0000:3d:00.4");
		assert_eq!(gpus[0].model, "GA100 [A100 SXM4 40GB]");
		// The VMM does not require vfio-pci or a viable group for VFs.
		assert_eq!(gpus[0].problem, "");
	}

	#[test]
	fn reserve_is_all_or_nothing_and_release_and_claim_track_holders() {
		let fake = Fake::new();
		let first = fake.rtx(0x52, 15);
		let second = fake.rtx(0x53, 16);
		fake.add("0000:01:00.0", 0x10de, 0x2f04, 0x03_0000, Some("nvidia"));
		let pool = fake.pool();

		assert_eq!(pool.reserve("ia", 0).unwrap(), Vec::<String>::new());
		assert_eq!(pool.reserve("ia", 9).unwrap_err().code, "invalid");
		assert_eq!(pool.reserve("ia", 1).unwrap(), [first.as_str()]);
		let busy = pool.reserve("ib", 2).unwrap_err();
		assert_eq!(busy.code, "busy");
		assert!(busy.message.contains("1 free of 2 assignable (3 found)"), "{}", busy.message);
		// The failed reservation took nothing.
		assert_eq!(pool.reserve("ib", 1).unwrap(), [second.as_str()]);
		let holders: Vec<_> = pool.list().into_iter().map(|gpu| gpu.sandbox).collect();
		assert_eq!(holders, [None, Some("ia".into()), Some("ib".into())]);

		// Releasing someone else's units is a no-op.
		pool.release("ib", std::slice::from_ref(&first));
		assert_eq!(pool.reserve("ic", 1).unwrap_err().code, "busy");
		pool.release("ia", std::slice::from_ref(&first));
		assert!(
			pool
				.list()
				.iter()
				.all(|gpu| gpu.sandbox.as_deref() != Some("ia"))
		);

		// Recovery re-claims recorded units, never another holder's.
		assert!(pool.claim("ia", std::slice::from_ref(&second)).is_err());
		pool.claim("ia", std::slice::from_ref(&first)).unwrap();
		pool.claim("ia", std::slice::from_ref(&first)).unwrap();
		assert_eq!(pool.list()[1].sandbox.as_deref(), Some("ia"));
	}

	#[test]
	fn quarantined_gpus_stay_unusable_until_the_host_reboots() {
		let fake = Fake::new();
		let gpu = fake.rtx(0x52, 15);
		let file = fake.root.path().join("gpu-quarantine.json");
		let pool = fake
			.pool()
			.persist_quarantine(file.clone(), "boot-1".into());
		assert_eq!(pool.reserve("ia", 1).unwrap(), [gpu.as_str()]);
		pool.quarantine("ia", std::slice::from_ref(&gpu));
		let listed = pool.list();
		assert_eq!(listed[0].problem, NEEDS_RESET);
		assert_eq!(listed[0].sandbox, None, "the holder is released");
		assert_eq!(pool.reserve("ib", 1).unwrap_err().code, "busy");

		// A node restart in the same host boot keeps it; a reboot clears it.
		let same_boot = fake
			.pool()
			.persist_quarantine(file.clone(), "boot-1".into());
		assert_eq!(same_boot.list()[0].problem, NEEDS_RESET);
		let rebooted = fake
			.pool()
			.persist_quarantine(file.clone(), "boot-2".into());
		assert_eq!(rebooted.list()[0].problem, "");
		assert_eq!(rebooted.reserve("ib", 1).unwrap(), [gpu.as_str()]);
		// A quarantine in the new boot replaces the stale file.
		rebooted.quarantine("ib", std::slice::from_ref(&gpu));
		assert_eq!(rebooted.list()[0].problem, NEEDS_RESET);
		let stale = fake.pool().persist_quarantine(file, "boot-1".into());
		assert_eq!(stale.list()[0].problem, "");

		// Without persistence the quarantine lives in memory.
		let memory = fake.pool();
		memory.quarantine("ic", std::slice::from_ref(&gpu));
		assert_eq!(memory.list()[0].problem, NEEDS_RESET);
	}

	#[test]
	fn kernel_release_from_bzimage_header_and_banner() {
		let mut bz = vec![0u8; 0x1000];
		bz[0x202..0x206].copy_from_slice(b"HdrS");
		bz[0x20e..0x210].copy_from_slice(&0x300u16.to_le_bytes());
		bz[0x500..0x500 + 31].copy_from_slice(b"6.16.9-vmon (b@h) #1 SMP PREEMP");
		assert_eq!(kernel_release(&bz).as_deref(), Some("6.16.9-vmon"));
		let image = b"\x00garbageLinux version 6.12.8-vmon (b@h) #1 SMP\x00";
		assert_eq!(kernel_release(image).as_deref(), Some("6.12.8-vmon"));
		assert_eq!(kernel_release(b"nothing here"), None);
	}

	#[test]
	fn vm_uuids_are_stable_versioned_and_distinct() {
		let uuid = vm_uuid("i0123456789abcdefghij");
		assert_eq!(uuid, vm_uuid("i0123456789abcdefghij"));
		assert_ne!(uuid, vm_uuid("i0123456789abcdefghik"));
		assert_eq!(uuid.len(), 36);
		assert_eq!(&uuid[14..15], "8");
		assert!(matches!(&uuid[19..20], "8" | "9" | "a" | "b"));
	}
}
