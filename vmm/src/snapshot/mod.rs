//! Snapshot on-disk format and (de)serialization.
//!
//! A snapshot is a directory whose `current-generation` manifest selects the
//! active generation's two files:
//!   - `vmstate.<gen>.bin` — postcard of the arch-neutral [`Snapshot`]
//!     envelope.
//!   - `memory.<gen>.bin`  — raw guest RAM, regions concatenated in slot order.
//!
//! The envelope is arch-neutral; the per-vCPU / machine payloads live in
//! `crate::arch::state` (selected at compile time) and are referenced here only
//! by type name, so this module compiles identically on `x86_64` and aarch64.

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::{
	fs::{self, File, OpenOptions},
	io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use virtio_queue::QueueState;
use vm_memory::{Address, GuestAddress, GuestMemory, GuestMemoryRegion, bitmap::Bitmap};
use vm_superio::serial::SerialState as VmSerialState;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

#[cfg(target_arch = "x86_64")]
use crate::layout::PCI_VIRTIO_BAR_SIZE;
use crate::{
	arch::state::{MachineState, VcpuState},
	config::{MAX_CPUS, MAX_MEM_MIB},
	hv::{Backend, current_backend},
	layout::{IRQ_BASE, IRQ_END, MMIO_DEVICE_SIZE, MMIO_MEM_SIZE, MMIO_MEM_START},
	memory::{self, GuestMemoryMmap},
	result::{Result, err},
};

/// Snapshot format version (postcard-encoded).
///
/// Bumped to 3 to add serialized libslirp state for virtio user-mode
/// networking, and to 4 because HVF vCPU state now records the frozen guest
/// counter instead of a raw vtimer offset; older snapshots are unsupported and
/// must be recaptured.
pub const SNAPSHOT_VERSION: u32 = 4;

const DELTA_PAGE_SIZE: u64 = 4096;
const DELTA_PAGE_USIZE: usize = DELTA_PAGE_SIZE as usize;
const MAX_DELTA_CHAIN_DEPTH: usize = 64;
/// Pages per parallel scan/diff task (64 MiB of RAM); rayon work-steals the
/// remainder.
const DIFF_PAGES_PER_WORKER: u64 = 16 * 1024;

const CURRENT_GENERATION_FILE: &str = "current-generation";
const MANIFEST_TMP_FILE: &str = "current-generation.tmp";
const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SNAPSHOT_NAME_BYTES: usize = 128;
const MAX_MANIFEST_BYTES: u64 = 32;
const SERIAL_FIFO_SIZE: usize = 0x40;
const VIRTQ_DESC_ELEMENT_SIZE: u64 = 16;
const VIRTQ_AVAIL_META_SIZE: u64 = 6;
const VIRTQ_AVAIL_ELEMENT_SIZE: u64 = 2;
const VIRTQ_USED_META_SIZE: u64 = 6;
const VIRTQ_USED_ELEMENT_SIZE: u64 = 8;
const VIRTQ_EVENT_ELEMENT_SIZE: u64 = 2;

/// The complete, self-contained state of a paused VM (minus guest RAM, which is
/// stored in the selected memory file).
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
	pub version:     u32,
	pub arch:        String,
	pub backend:     Backend,
	pub mem_mib:     usize,
	pub cpus:        u8,
	pub cmdline:     String,
	pub boot_mode:   String,
	pub firmware:    Option<String>,
	pub mem_regions: Vec<MemRegion>,
	pub vcpus:       Vec<VcpuState>,
	pub machine:     MachineState,
	pub serial:      SerialState,
	pub devices:     Vec<DeviceState>,
	pub delta:       Option<DeltaMemory>,
}

/// A decoded snapshot plus the memory file selected by the same manifest read.
pub struct SnapshotImage {
	snapshot:    Snapshot,
	memory_file: PathBuf,
}

impl SnapshotImage {
	pub const fn snapshot(&self) -> &Snapshot {
		&self.snapshot
	}

	pub fn memory_file(&self) -> &Path {
		&self.memory_file
	}

	pub fn into_snapshot(self) -> Snapshot {
		self.snapshot
	}
}

/// One guest-RAM slot, in KVM slot order. `file_offset` is the byte offset of
/// this region within the selected memory file (prefix sum of prior regions).
#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct MemRegion {
	pub gpa:         u64,
	pub len:         u64,
	pub file_offset: u64,
}

/// Memory-delta descriptor: this snapshot's memory file holds only the guest
/// pages that differ from the reconstructed `base` snapshot.
#[derive(Serialize, Deserialize, Clone)]
pub struct DeltaMemory {
	/// Base snapshot directory basename, resolved against the delta dir's
	/// parent.
	pub base:      String,
	/// Diff granularity in bytes (always `DELTA_PAGE_SIZE` for written
	/// snapshots).
	pub page_size: u64,
	/// One bit per full-image page, LSB-first, ascending page index. A set bit
	/// means the memory file carries that page; set-bit pages are stored
	/// back-to-back in ascending index order.
	pub changed:   Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeviceKind {
	Block,
	Net,
	Console,
	Fs,
	Rng,
	RemoteFs,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeviceTransportKind {
	Mmio,
	Pci,
}

/// Enough to re-open the backend on restore (CLI flags override these).
#[derive(Serialize, Deserialize, Clone)]
pub enum BackendHint {
	Block {
		path:      String,
		read_only: bool,
	},
	Net {
		tap: String,
		mac: [u8; 6],
	},
	/// Entitlement-free user-mode NAT networking.
	UserNet {
		mac: [u8; 6],
	},
	/// virtio-console agent channel; no host backend to reopen.
	Console,
	/// virtio-fs share; `read_only` reflects whether guest writes are rejected.
	Fs {
		tag:        String,
		shared_dir: String,
		read_only:  bool,
	},
	/// virtio-rng entropy source; no host backend to reopen.
	Rng,
	/// Remote read-only virtio-fs share served through a per-VM proxy socket.
	RemoteFs {
		tag:  String,
		sock: String,
	},
}

/// Per-device transport + queue + backend state.
#[derive(Serialize, Deserialize, Clone)]
pub struct DeviceState {
	pub kind:                   DeviceKind,
	pub transport:              DeviceTransportKind,
	pub mmio_base:              u64,
	pub gsi:                    u32,
	pub interrupt_status:       u32,
	pub device_features_select: u32,
	pub driver_features_select: u32,
	pub acked_features:         u64,
	pub status:                 u32,
	pub activated:              bool,
	pub queues:                 Vec<QueueStateSer>,
	pub backend:                BackendHint,
	pub user_net_state:         Option<Vec<u8>>,
	pub transport_pci:          Option<PciTransportStateSer>,
	pub fs:                     Option<FsStateSer>,
}

/// Serializable mirror of one virtio-pci function's transport-specific state.
#[derive(Serialize, Deserialize, Clone)]
pub struct PciTransportStateSer {
	/// Raw 256-byte PCI config space as last exposed to the guest.
	pub config_space: Vec<u8>,
	pub bar_base:     u64,
	pub bar0_probe:   bool,
	pub command:      u16,
	pub msix:         MsixStateSer,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MsixStateSer {
	pub shared_vector: u16,
	pub control:       u16,
	pub pending:       u64,
	pub table:         Vec<MsixEntrySer>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MsixEntrySer {
	pub msg_addr:    u64,
	pub msg_data:    u32,
	pub vector_ctrl: u32,
}

/// Serializable virtio-fs inode table. Paths are relative to the host shared
/// root for [`BackendHint::Fs`] and to the object prefix for
/// [`BackendHint::RemoteFs`].
#[derive(Serialize, Deserialize, Clone)]
pub struct FsStateSer {
	/// Node ID with its path relative to the backend's exported root.
	pub inodes: Vec<(u64, String)>,
	pub next:   u64,
}

/// Serializable mirror of [`vm_superio::serial::SerialState`].
#[derive(Serialize, Deserialize, Clone)]
pub struct SerialState {
	pub baud_divisor_low:         u8,
	pub baud_divisor_high:        u8,
	pub interrupt_enable:         u8,
	pub interrupt_identification: u8,
	pub line_control:             u8,
	pub line_status:              u8,
	pub modem_control:            u8,
	pub modem_status:             u8,
	pub scratch:                  u8,
	pub in_buffer:                Vec<u8>,
}

impl From<VmSerialState> for SerialState {
	fn from(s: VmSerialState) -> Self {
		Self {
			baud_divisor_low:         s.baud_divisor_low,
			baud_divisor_high:        s.baud_divisor_high,
			interrupt_enable:         s.interrupt_enable,
			interrupt_identification: s.interrupt_identification,
			line_control:             s.line_control,
			line_status:              s.line_status,
			modem_control:            s.modem_control,
			modem_status:             s.modem_status,
			scratch:                  s.scratch,
			in_buffer:                s.in_buffer,
		}
	}
}

impl From<SerialState> for VmSerialState {
	fn from(s: SerialState) -> Self {
		Self {
			baud_divisor_low:         s.baud_divisor_low,
			baud_divisor_high:        s.baud_divisor_high,
			interrupt_enable:         s.interrupt_enable,
			interrupt_identification: s.interrupt_identification,
			line_control:             s.line_control,
			line_status:              s.line_status,
			modem_control:            s.modem_control,
			modem_status:             s.modem_status,
			scratch:                  s.scratch,
			in_buffer:                s.in_buffer,
		}
	}
}

/// Field-for-field serializable mirror of [`virtio_queue::state::QueueState`].
#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct QueueStateSer {
	pub max_size:          u16,
	pub next_avail:        u16,
	pub next_used:         u16,
	pub event_idx_enabled: bool,
	pub size:              u16,
	pub ready:             bool,
	pub desc_table:        u64,
	pub avail_ring:        u64,
	pub used_ring:         u64,
}

impl From<QueueState> for QueueStateSer {
	fn from(q: QueueState) -> Self {
		Self {
			max_size:          q.max_size,
			next_avail:        q.next_avail,
			next_used:         q.next_used,
			event_idx_enabled: q.event_idx_enabled,
			size:              q.size,
			ready:             q.ready,
			desc_table:        q.desc_table,
			avail_ring:        q.avail_ring,
			used_ring:         q.used_ring,
		}
	}
}

impl From<QueueStateSer> for QueueState {
	fn from(q: QueueStateSer) -> Self {
		Self {
			max_size:          q.max_size,
			next_avail:        q.next_avail,
			next_used:         q.next_used,
			event_idx_enabled: q.event_idx_enabled,
			size:              q.size,
			ready:             q.ready,
			desc_table:        q.desc_table,
			avail_ring:        q.avail_ring,
			used_ring:         q.used_ring,
		}
	}
}

/// Architecture string baked into a snapshot; restore must match the build.
pub const fn build_arch() -> &'static str {
	if cfg!(target_arch = "x86_64") {
		"x86_64"
	} else {
		"aarch64"
	}
}

/// True if `name` is a bounded ASCII snapshot basename safe for paths and logs.
pub fn is_safe_snapshot_name(name: &str) -> bool {
	!name.is_empty()
		&& name.len() <= MAX_SNAPSHOT_NAME_BYTES
		&& !name.starts_with('.')
		&& !name.contains("..")
		&& name
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Transactionally write memory and state as a new generation, then publish it.
///
/// `guest_dirty`, when present, is the hypervisor's fetch-and-cleared
/// guest-write dirty log (one word bitmap per RAM region); combined with the
/// host-write bitmaps carried by `mem` it lets a delta dump inspect only pages
/// that could have changed since `base` was captured.
pub fn write_snapshot<F>(
	dir: &Path,
	base: Option<&str>,
	mem: &GuestMemoryMmap,
	guest_dirty: Option<Vec<Vec<u64>>>,
	build_snapshot: F,
) -> Result<()>
where
	F: FnOnce(Vec<MemRegion>, Option<DeltaMemory>) -> Snapshot,
{
	fs::create_dir_all(dir)
		.map_err(|e| err(format!("creating snapshot dir {}: {e}", dir.display())))?;

	let generation = next_generation(dir)?;
	let memory_tmp = dir.join(generation_memory_tmp_file(generation));
	let state_tmp = dir.join(generation_state_tmp_file(generation));
	let memory_file = dir.join(generation_memory_file(generation));
	let state_file = dir.join(generation_state_file(generation));

	let prepare = (|| -> Result<()> {
		let (mem_regions, delta) = match base {
			None => (dump_memory_file(&memory_tmp, mem)?, None),
			Some(b) => {
				let (r, d) = dump_delta_memory_file(&memory_tmp, dir, b, mem, guest_dirty.as_deref())?;
				(r, Some(d))
			},
		};
		let snap = build_snapshot(mem_regions, delta);
		validate_snapshot_metadata(&snap, &memory_tmp)?;
		write_state_file(&state_tmp, &snap)?;

		fs::rename(&memory_tmp, &memory_file)
			.map_err(|e| err(format!("publishing {}: {e}", memory_file.display())))?;
		fs::rename(&state_tmp, &state_file)
			.map_err(|e| err(format!("publishing {}: {e}", state_file.display())))?;
		sync_dir(dir)?;
		Ok(())
	})();

	if let Err(e) = prepare {
		cleanup_unpublished_generation(dir, generation);
		return Err(e);
	}

	publish_generation(dir, generation)?;
	Ok(())
}

/// Read and validate the manifest-selected snapshot state.
pub fn read_state(dir: &Path) -> Result<Snapshot> {
	Ok(read_snapshot(dir)?.into_snapshot())
}

/// Read and validate state together with the matching memory file path.
pub fn read_snapshot(dir: &Path) -> Result<SnapshotImage> {
	let layout = selected_layout(dir)?;
	let snapshot = read_state_file(&layout.state_file)?;
	let image = SnapshotImage { snapshot, memory_file: layout.memory_file };
	validate_snapshot(&image)?;
	Ok(image)
}

/// Read and validate snapshot metadata without requiring the memory file.
///
/// Lazy remote restore uses this when guest RAM is supplied page-by-page by a
/// page source. A delta's descriptor is validated but not applied: the page
/// source must serve the reconstructed full image (base chain plus delta),
/// addressed by full-image page index.
pub fn read_snapshot_metadata(dir: &Path) -> Result<SnapshotImage> {
	let layout = selected_layout(dir)?;
	let snapshot = read_state_file(&layout.state_file)?;
	let image = SnapshotImage { snapshot, memory_file: layout.memory_file };
	validate_snapshot_metadata_only(&image)?;
	Ok(image)
}

/// Validate a decoded snapshot image before restore/fork allocates or indexes
/// it.
pub fn validate_snapshot(image: &SnapshotImage) -> Result<()> {
	validate_snapshot_metadata(image.snapshot(), image.memory_file())
}

struct SnapshotLayout {
	state_file:  PathBuf,
	memory_file: PathBuf,
}

fn selected_layout(dir: &Path) -> Result<SnapshotLayout> {
	let generation = current_generation(dir)?
		.ok_or_else(|| err(format!("no {CURRENT_GENERATION_FILE} manifest in {}", dir.display())))?;
	Ok(SnapshotLayout {
		state_file:  dir.join(generation_state_file(generation)),
		memory_file: dir.join(generation_memory_file(generation)),
	})
}

fn next_generation(dir: &Path) -> Result<u64> {
	match current_generation(dir)? {
		Some(generation) => generation
			.checked_add(1)
			.ok_or_else(|| err("snapshot generation overflow")),
		None => Ok(1),
	}
}

fn snapshot_read_options() -> OpenOptions {
	let mut options = OpenOptions::new();
	options.read(true);
	#[cfg(unix)]
	options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
	#[cfg(windows)]
	options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
	options
}

fn snapshot_create_options() -> OpenOptions {
	let mut options = OpenOptions::new();
	options.write(true).create_new(true);
	#[cfg(unix)]
	options
		.mode(0o600)
		.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
	#[cfg(windows)]
	options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
	options
}

fn current_generation(dir: &Path) -> Result<Option<u64>> {
	let path = dir.join(CURRENT_GENERATION_FILE);
	let file = match snapshot_read_options().open(&path) {
		Ok(file) => file,
		Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(err(format!("opening {}: {error}", path.display()))),
	};
	if !file
		.metadata()
		.map_err(|error| err(format!("stat {}: {error}", path.display())))?
		.is_file()
	{
		return Err(err(format!("{} is not a regular file", path.display())));
	}
	let mut contents = String::new();
	Read::take(file, MAX_MANIFEST_BYTES + 1)
		.read_to_string(&mut contents)
		.map_err(|error| err(format!("reading {}: {error}", path.display())))?;
	if contents.len() as u64 > MAX_MANIFEST_BYTES {
		return Err(err(format!("{CURRENT_GENERATION_FILE} exceeds {MAX_MANIFEST_BYTES} bytes")));
	}
	Ok(Some(parse_generation(&contents)?))
}

fn parse_generation(contents: &str) -> Result<u64> {
	let generation = contents.trim();
	if generation.is_empty() || !generation.as_bytes().iter().all(u8::is_ascii_digit) {
		return Err(err(format!("invalid {CURRENT_GENERATION_FILE} contents {generation:?}")));
	}
	let generation = generation
		.parse::<u64>()
		.map_err(|e| err(format!("invalid {CURRENT_GENERATION_FILE}: {e}")))?;
	if generation == 0 {
		return Err(err(format!("{CURRENT_GENERATION_FILE} must be nonzero")));
	}
	Ok(generation)
}

fn publish_generation(dir: &Path, generation: u64) -> Result<()> {
	let tmp = dir.join(MANIFEST_TMP_FILE);
	let current = dir.join(CURRENT_GENERATION_FILE);
	let publish = (|| -> Result<()> {
		let mut file = create_snapshot_file(&tmp)?;
		writeln!(file, "{generation}").map_err(|e| err(format!("writing {}: {e}", tmp.display())))?;
		file
			.sync_all()
			.map_err(|e| err(format!("syncing {}: {e}", tmp.display())))?;
		drop(file);
		fs::rename(&tmp, &current)
			.map_err(|e| err(format!("publishing {}: {e}", current.display())))?;
		Ok(())
	})();
	if let Err(e) = publish {
		let _ = fs::remove_file(&tmp);
		return Err(e);
	}
	sync_dir(dir)
}

fn open_snapshot_file(path: &Path) -> Result<File> {
	let file = snapshot_read_options()
		.open(path)
		.map_err(|error| err(format!("opening {}: {error}", path.display())))?;
	if !file
		.metadata()
		.map_err(|error| err(format!("stat {}: {error}", path.display())))?
		.is_file()
	{
		return Err(err(format!("{} is not a regular file", path.display())));
	}
	Ok(file)
}

fn create_snapshot_file(path: &Path) -> Result<File> {
	snapshot_create_options()
		.open(path)
		.map_err(|error| err(format!("creating {}: {error}", path.display())))
}

fn read_state_file(path: &Path) -> Result<Snapshot> {
	let file = open_snapshot_file(path)?;
	let len = file
		.metadata()
		.map_err(|error| err(format!("stat {}: {error}", path.display())))?
		.len();
	if len > MAX_STATE_BYTES as u64 {
		return Err(err(format!(
			"snapshot state {} is {len} bytes, over {MAX_STATE_BYTES} byte limit",
			path.display()
		)));
	}
	let mut bytes = Vec::with_capacity(len as usize);
	Read::take(file, MAX_STATE_BYTES as u64 + 1)
		.read_to_end(&mut bytes)
		.map_err(|error| err(format!("reading {}: {error}", path.display())))?;
	if bytes.len() > MAX_STATE_BYTES {
		return Err(err(format!(
			"snapshot state {} grew over {MAX_STATE_BYTES} byte limit while reading",
			path.display()
		)));
	}
	let (version, _) = postcard::take_from_bytes::<u32>(&bytes)
		.map_err(|e| err(format!("decoding snapshot version: {e}")))?;
	if version > SNAPSHOT_VERSION {
		return Err(err(format!(
			"snapshot version {version} is newer than supported {SNAPSHOT_VERSION}"
		)));
	}
	if version < SNAPSHOT_VERSION {
		return Err(err(format!(
			"snapshot version {version} is older than supported {SNAPSHOT_VERSION}; recapture \
			 required"
		)));
	}
	let (snap, rest) = postcard::take_from_bytes::<Snapshot>(&bytes)
		.map_err(|e| err(format!("decoding snapshot: {e}")))?;
	if !rest.is_empty() {
		return Err(err(format!("snapshot state has {} trailing bytes", rest.len())));
	}
	validate_snapshot_header(&snap)?;
	Ok(snap)
}

fn validate_snapshot_header(snap: &Snapshot) -> Result<()> {
	if snap.version != SNAPSHOT_VERSION {
		return Err(err(format!(
			"snapshot version {} != supported {SNAPSHOT_VERSION}",
			snap.version
		)));
	}
	if snap.arch != build_arch() {
		return Err(err(format!("snapshot arch {:?} != build arch {:?}", snap.arch, build_arch())));
	}
	if snap.backend != current_backend() {
		return Err(err(format!(
			"snapshot backend {:?} != current backend {:?}; cross-hypervisor restore is unsupported",
			snap.backend,
			current_backend()
		)));
	}
	match snap.boot_mode.as_str() {
		"direct" if snap.firmware.is_none() => {},
		"direct" => {
			return Err(err("snapshot direct boot mode must not include firmware"));
		},
		"uefi" => match snap.firmware.as_deref() {
			Some(path) if !path.is_empty() => {},
			_ => return Err(err("snapshot uefi boot mode requires firmware")),
		},
		other => {
			return Err(err(format!("snapshot boot_mode {other:?} is not supported")));
		},
	}
	Ok(())
}

fn validate_snapshot_resource_caps(cpus: u8, mem_mib: usize) -> Result<()> {
	if cpus == 0 {
		return Err(err("snapshot must contain at least one vCPU"));
	}
	if cpus > MAX_CPUS {
		return Err(err(format!("snapshot cpus {cpus} exceeds launch cap {MAX_CPUS}")));
	}
	if mem_mib == 0 {
		return Err(err("snapshot memory size is zero"));
	}
	if mem_mib > MAX_MEM_MIB {
		return Err(err(format!(
			"snapshot memory size {mem_mib} MiB exceeds launch cap {MAX_MEM_MIB} MiB"
		)));
	}
	Ok(())
}

fn validate_snapshot_metadata(snap: &Snapshot, memory_file: &Path) -> Result<()> {
	validate_snapshot_header(snap)?;
	validate_snapshot_resource_caps(snap.cpus, snap.mem_mib)?;
	if snap.vcpus.len() != usize::from(snap.cpus) {
		return Err(err(format!(
			"snapshot vcpu state count {} != cpus {}",
			snap.vcpus.len(),
			snap.cpus
		)));
	}
	if snap.serial.in_buffer.len() > SERIAL_FIFO_SIZE {
		return Err(err(format!(
			"serial input FIFO length {} exceeds {SERIAL_FIFO_SIZE}",
			snap.serial.in_buffer.len()
		)));
	}
	let memory_len = snapshot_memory_len(memory_file)?;
	validate_memory_regions(snap, memory_len)?;
	validate_devices(snap)?;
	Ok(())
}

fn validate_snapshot_metadata_only(image: &SnapshotImage) -> Result<()> {
	let snap = image.snapshot();
	validate_snapshot_header(snap)?;
	validate_snapshot_resource_caps(snap.cpus, snap.mem_mib)?;
	if snap.vcpus.len() != usize::from(snap.cpus) {
		return Err(err(format!(
			"snapshot vcpu state count {} != cpus {}",
			snap.vcpus.len(),
			snap.cpus
		)));
	}
	if snap.serial.in_buffer.len() > SERIAL_FIFO_SIZE {
		return Err(err(format!(
			"serial input FIFO length {} exceeds {SERIAL_FIFO_SIZE}",
			snap.serial.in_buffer.len()
		)));
	}
	let total_ram = validate_region_layout(snap)?;
	if let Some(delta) = &snap.delta {
		validate_delta_descriptor(delta, total_ram)?;
	}
	validate_devices(snap)?;
	Ok(())
}

/// [`validate_delta_memory`] without a memory file: the bitmap is checked
/// against itself (a remote page source serves the pages).
fn validate_delta_descriptor(delta: &DeltaMemory, total_ram: u64) -> Result<()> {
	let changed = delta
		.changed
		.iter()
		.map(|byte| u64::from(byte.count_ones()))
		.sum::<u64>();
	validate_delta_memory(delta, total_ram, changed.saturating_mul(delta.page_size))
}

fn snapshot_memory_len(path: &Path) -> Result<u64> {
	Ok(open_snapshot_file(path)?
		.metadata()
		.map_err(|error| err(format!("stat snapshot memory {}: {error}", path.display())))?
		.len())
}

fn validate_memory_regions(snap: &Snapshot, memory_len: u64) -> Result<()> {
	let total_ram = validate_region_layout(snap)?;
	match &snap.delta {
		None => {
			if total_ram != memory_len {
				return Err(err(format!(
					"snapshot memory data length {total_ram} != memory file length {memory_len}"
				)));
			}
		},
		Some(d) => validate_delta_memory(d, total_ram, memory_len)?,
	}
	Ok(())
}

fn validate_region_layout(snap: &Snapshot) -> Result<u64> {
	let mem_bytes = snap
		.mem_mib
		.checked_mul(1 << 20)
		.ok_or_else(|| err("snapshot memory size overflows usize"))?;
	if mem_bytes == 0 {
		return Err(err("snapshot memory size is zero"));
	}
	let expected = memory::arrange_memory(mem_bytes);
	if snap.mem_regions.len() != expected.len() {
		return Err(err(format!(
			"snapshot memory region count {} != architecture layout count {}",
			snap.mem_regions.len(),
			expected.len()
		)));
	}

	let mut next_file_offset = 0u64;
	let mut total_ram = 0u64;
	let mut ranges = Vec::with_capacity(snap.mem_regions.len());
	for (idx, region) in snap.mem_regions.iter().enumerate() {
		let (expected_addr, expected_len) = expected[idx];
		if region.len == 0 {
			return Err(err(format!("snapshot memory region {idx} is empty")));
		}
		if region.len % DELTA_PAGE_SIZE != 0 {
			return Err(err(format!("snapshot memory region {idx} length is not page-aligned")));
		}
		let expected_len = u64::try_from(expected_len)
			.map_err(|_| err("architecture memory layout length overflows u64"))?;
		if region.gpa != expected_addr.raw_value() || region.len != expected_len {
			return Err(err(format!(
				"snapshot memory region {idx} ({:#x}, {}) does not match architecture layout ({:#x}, \
				 {})",
				region.gpa,
				region.len,
				expected_addr.raw_value(),
				expected_len
			)));
		}
		if region.file_offset != next_file_offset {
			return Err(err(format!(
				"snapshot memory region {idx} file offset {} != expected {}",
				region.file_offset, next_file_offset
			)));
		}
		let end = region.gpa.checked_add(region.len).ok_or_else(|| {
			err(format!("snapshot memory region {idx} guest address range overflows"))
		})?;
		next_file_offset = next_file_offset
			.checked_add(region.len)
			.ok_or_else(|| err(format!("snapshot memory region {idx} file offset range overflows")))?;
		total_ram = total_ram
			.checked_add(region.len)
			.ok_or_else(|| err(format!("snapshot memory region {idx} total RAM size overflows")))?;
		ranges.push((region.gpa, end));
	}
	ranges.sort_unstable_by_key(|&(start, _)| start);
	for pair in ranges.windows(2) {
		if pair[1].0 < pair[0].1 {
			return Err(err(format!("snapshot memory regions overlap at {:#x}", pair[1].0)));
		}
	}
	let mem_bytes =
		u64::try_from(mem_bytes).map_err(|_| err("snapshot memory size overflows u64"))?;
	if total_ram != mem_bytes {
		return Err(err(format!(
			"snapshot memory regions total {total_ram} bytes != configured {mem_bytes} bytes"
		)));
	}
	Ok(total_ram)
}

fn validate_delta_memory(d: &DeltaMemory, total_ram: u64, memory_len: u64) -> Result<()> {
	if d.page_size != DELTA_PAGE_SIZE {
		return Err(err(format!("snapshot delta page size {} != {DELTA_PAGE_SIZE}", d.page_size)));
	}
	if !total_ram.is_multiple_of(d.page_size) {
		return Err(err(format!("snapshot delta total RAM {total_ram} is not page-aligned")));
	}
	let total_pages = total_ram / d.page_size;
	let expected_bitmap_len = total_pages.div_ceil(8);
	if d.changed.len() as u64 != expected_bitmap_len {
		return Err(err(format!(
			"snapshot delta bitmap length {} != {expected_bitmap_len}",
			d.changed.len()
		)));
	}
	let spare_bits = (total_pages % 8) as u32;
	if spare_bits != 0 {
		let last = d.changed[d.changed.len() - 1];
		let mask = !((1u8 << spare_bits) - 1);
		if last & mask != 0 {
			return Err(err("snapshot delta bitmap has set bits past page count"));
		}
	}
	let set = d.changed.iter().map(|b| b.count_ones()).sum::<u32>() as u64;
	let expected_len = set * d.page_size;
	if memory_len != expected_len {
		return Err(err(format!(
			"snapshot delta memory file length {memory_len} != {expected_len} for {set} changed pages"
		)));
	}
	if !is_safe_snapshot_name(&d.base) {
		return Err(err(format!("snapshot delta base {:?} is not a safe basename", d.base)));
	}
	Ok(())
}

fn validate_devices(snap: &Snapshot) -> Result<()> {
	validate_device_states(&snap.devices, &snap.mem_regions)
}

fn validate_device_states(devices: &[DeviceState], mem_regions: &[MemRegion]) -> Result<()> {
	for (idx, device) in devices.iter().enumerate() {
		match device.transport {
			DeviceTransportKind::Mmio => {
				if device.transport_pci.is_some() {
					return Err(err(format!("snapshot device {idx} has PCI state on MMIO transport")));
				}
			},
			DeviceTransportKind::Pci => validate_pci_transport_state(idx, device)?,
		}
		match device.kind {
			DeviceKind::Fs | DeviceKind::RemoteFs => {
				if device.fs.is_none() {
					return Err(err(format!("snapshot virtio-fs device {idx} is missing fs state")));
				}
			},
			_ => {
				if device.fs.is_some() {
					return Err(err(format!("snapshot non-fs device {idx} carries virtio-fs state")));
				}
			},
		}
		match (&device.backend, &device.user_net_state) {
			(BackendHint::UserNet { .. }, None) => {
				return Err(err(format!("snapshot user-net device {idx} is missing libslirp state")));
			},
			(BackendHint::UserNet { .. }, Some(state)) if state.is_empty() => {
				return Err(err(format!("snapshot user-net device {idx} has empty libslirp state")));
			},
			(BackendHint::UserNet { .. }, Some(_)) => {},
			(_, Some(_)) => {
				return Err(err(format!("snapshot non-user-net device {idx} carries libslirp state")));
			},
			(_, None) => {},
		}
		let expected_queues = match (&device.kind, &device.backend) {
			(DeviceKind::Block, BackendHint::Block { .. }) => 1,
			(DeviceKind::Net, BackendHint::Net { .. } | BackendHint::UserNet { .. }) => 2,
			(DeviceKind::Console, BackendHint::Console) => {
				crate::virtio::console::queue_count(device.acked_features, device.activated)
			},
			(DeviceKind::Fs, BackendHint::Fs { .. }) => 2,
			(DeviceKind::RemoteFs, BackendHint::RemoteFs { .. }) => 2,
			(DeviceKind::Rng, BackendHint::Rng) => 1,
			(kind, _) => {
				return Err(err(format!("snapshot device {idx} backend does not match kind {kind:?}")));
			},
		};
		if device.queues.len() != expected_queues {
			return Err(err(format!(
				"snapshot device {idx} queue count {} != expected {expected_queues}",
				device.queues.len()
			)));
		}
		for (queue_idx, queue) in device.queues.iter().enumerate() {
			if queue.max_size == 0 {
				return Err(err(format!("snapshot device {idx} queue {queue_idx} has zero max size")));
			}
			if queue.size > queue.max_size {
				return Err(err(format!(
					"snapshot device {idx} queue {queue_idx} size {} exceeds max {}",
					queue.size, queue.max_size
				)));
			}
			if queue.ready && queue.size == 0 {
				return Err(err(format!(
					"snapshot device {idx} queue {queue_idx} is ready with zero size"
				)));
			}
			if queue.ready {
				validate_ready_queue_ram(idx, queue_idx, queue, mem_regions)?;
			}
		}
	}
	validate_device_addressing(devices)?;
	Ok(())
}

fn validate_pci_transport_state(idx: usize, device: &DeviceState) -> Result<()> {
	let Some(state) = &device.transport_pci else {
		return Err(err(format!("snapshot PCI device {idx} is missing PCI transport state")));
	};
	if state.config_space.len() != 256 {
		return Err(err(format!(
			"snapshot PCI device {idx} config space length {} != 256",
			state.config_space.len()
		)));
	}
	if state.msix.table.is_empty() {
		return Err(err(format!("snapshot PCI device {idx} MSI-X table is empty")));
	}
	if state.msix.table.len() > 64 {
		return Err(err(format!(
			"snapshot PCI device {idx} MSI-X table length {} exceeds pending-bit capacity",
			state.msix.table.len()
		)));
	}
	Ok(())
}

fn validate_device_addressing(devices: &[DeviceState]) -> Result<()> {
	let device_count =
		u64::try_from(devices.len()).map_err(|_| err("snapshot device count overflows u64"))?;
	let mmio_capacity = MMIO_MEM_SIZE / MMIO_DEVICE_SIZE;
	if device_count > mmio_capacity {
		return Err(err(format!(
			"snapshot device count {device_count} exceeds MMIO aperture capacity {mmio_capacity}"
		)));
	}
	let gsi_capacity = IRQ_END
		.checked_sub(IRQ_BASE)
		.ok_or_else(|| err(format!("architecture GSI range {IRQ_BASE}..{IRQ_END} is invalid")))?;
	if device_count > u64::from(gsi_capacity) {
		return Err(err(format!(
			"snapshot device count {device_count} exceeds GSI capacity {gsi_capacity}"
		)));
	}

	let aperture_end = MMIO_MEM_START
		.checked_add(MMIO_MEM_SIZE)
		.ok_or_else(|| err("architecture MMIO device aperture overflows"))?;
	let mut mmio_ranges = Vec::with_capacity(devices.len());
	let mut gsis = Vec::with_capacity(devices.len());
	for (idx, device) in devices.iter().enumerate() {
		let window_size = match device.transport {
			DeviceTransportKind::Mmio => MMIO_DEVICE_SIZE,
			DeviceTransportKind::Pci => {
				#[cfg(target_arch = "x86_64")]
				{
					PCI_VIRTIO_BAR_SIZE
				}
				#[cfg(not(target_arch = "x86_64"))]
				{
					return Err(err(format!(
						"snapshot device {idx} uses PCI transport on unsupported architecture"
					)));
				}
			},
		};
		if device.mmio_base % window_size != 0 {
			return Err(err(format!(
				"snapshot device {idx} MMIO base {:#x} is not aligned to {window_size:#x}",
				device.mmio_base
			)));
		}
		let mmio_end = device.mmio_base.checked_add(window_size).ok_or_else(|| {
			err(format!("snapshot device {idx} MMIO window overflows guest address space"))
		})?;
		if device.mmio_base < MMIO_MEM_START || mmio_end > aperture_end {
			return Err(err(format!(
				"snapshot device {idx} MMIO window {:#x}..{:#x} is outside device aperture \
				 {MMIO_MEM_START:#x}..{aperture_end:#x}",
				device.mmio_base, mmio_end
			)));
		}
		if device.gsi < IRQ_BASE || device.gsi >= IRQ_END {
			return Err(err(format!(
				"snapshot device {idx} GSI {} is outside device GSI range {IRQ_BASE}..{IRQ_END}",
				device.gsi
			)));
		}
		mmio_ranges.push((device.mmio_base, mmio_end, idx));
		gsis.push((device.gsi, idx));
	}

	mmio_ranges.sort_unstable_by_key(|&(start, ..)| start);
	for pair in mmio_ranges.windows(2) {
		let (prev_start, prev_end, prev_idx) = pair[0];
		let (start, end, idx) = pair[1];
		if start < prev_end {
			return Err(err(format!(
				"snapshot device {idx} MMIO window {start:#x}..{end:#x} overlaps device {prev_idx} \
				 window {prev_start:#x}..{prev_end:#x}"
			)));
		}
	}

	gsis.sort_unstable_by_key(|&(gsi, _)| gsi);
	for pair in gsis.windows(2) {
		let (prev_gsi, prev_idx) = pair[0];
		let (gsi, idx) = pair[1];
		if gsi == prev_gsi {
			return Err(err(format!(
				"snapshot device {idx} reuses GSI {gsi} already used by device {prev_idx}"
			)));
		}
	}
	Ok(())
}

fn validate_ready_queue_ram(
	device_idx: usize,
	queue_idx: usize,
	queue: &QueueStateSer,
	regions: &[MemRegion],
) -> Result<()> {
	let queue_size = u64::from(queue.size);
	let event_len = if queue.event_idx_enabled {
		VIRTQ_EVENT_ELEMENT_SIZE
	} else {
		0
	};
	let desc_len = VIRTQ_DESC_ELEMENT_SIZE
		.checked_mul(queue_size)
		.ok_or_else(|| err("virtqueue descriptor table size overflows"))?;
	let avail_len = VIRTQ_AVAIL_META_SIZE
		.checked_add(
			VIRTQ_AVAIL_ELEMENT_SIZE
				.checked_mul(queue_size)
				.ok_or_else(|| err("virtqueue available ring size overflows"))?,
		)
		.and_then(|len| len.checked_add(event_len))
		.ok_or_else(|| err("virtqueue available ring size overflows"))?;
	let used_len = VIRTQ_USED_META_SIZE
		.checked_add(
			VIRTQ_USED_ELEMENT_SIZE
				.checked_mul(queue_size)
				.ok_or_else(|| err("virtqueue used ring size overflows"))?,
		)
		.and_then(|len| len.checked_add(event_len))
		.ok_or_else(|| err("virtqueue used ring size overflows"))?;

	validate_queue_ram_range(
		device_idx,
		queue_idx,
		"descriptor table",
		queue.desc_table,
		desc_len,
		regions,
	)?;
	validate_queue_ram_range(
		device_idx,
		queue_idx,
		"available ring",
		queue.avail_ring,
		avail_len,
		regions,
	)?;
	validate_queue_ram_range(device_idx, queue_idx, "used ring", queue.used_ring, used_len, regions)
}

fn validate_queue_ram_range(
	device_idx: usize,
	queue_idx: usize,
	label: &str,
	start: u64,
	len: u64,
	regions: &[MemRegion],
) -> Result<()> {
	if len == 0 {
		return Err(err(format!(
			"snapshot device {device_idx} queue {queue_idx} {label} range is empty"
		)));
	}
	let end = start.checked_add(len).ok_or_else(|| {
		err(format!(
			"snapshot device {device_idx} queue {queue_idx} {label} range overflows guest address \
			 space"
		))
	})?;
	let fits = regions
		.iter()
		.any(|region| match region.gpa.checked_add(region.len) {
			Some(region_end) => start >= region.gpa && end <= region_end,
			None => false,
		});
	if !fits {
		return Err(err(format!(
			"snapshot device {device_idx} queue {queue_idx} {label} range {start:#x}..{end:#x} is \
			 outside declared guest RAM"
		)));
	}
	Ok(())
}

fn write_state_file(path: &Path, snap: &Snapshot) -> Result<()> {
	let bytes = postcard::to_allocvec(snap).map_err(|e| err(format!("encoding snapshot: {e}")))?;
	if bytes.len() > MAX_STATE_BYTES {
		return Err(err(format!(
			"encoded snapshot state is {} bytes, over {MAX_STATE_BYTES} byte limit",
			bytes.len()
		)));
	}
	let mut file = create_snapshot_file(path)?;
	file
		.write_all(&bytes)
		.map_err(|e| err(format!("writing {}: {e}", path.display())))?;
	file
		.sync_all()
		.map_err(|e| err(format!("syncing {}: {e}", path.display())))?;
	Ok(())
}

/// Build the region table for a live guest memory layout, asserting each region
/// is page-aligned (required for both full and delta snapshots).
fn memory_region_table(mem: &GuestMemoryMmap) -> Result<Vec<MemRegion>> {
	let mut regions = Vec::new();
	let mut file_offset = 0u64;
	for (idx, region) in mem.iter().enumerate() {
		let gpa = region.start_addr().raw_value();
		let len = region.len();
		if gpa % DELTA_PAGE_SIZE != 0 {
			return Err(err(format!("guest memory region {idx} GPA {gpa:#x} is not page-aligned")));
		}
		if len == 0 {
			return Err(err(format!("guest memory region {idx} at {gpa:#x} is empty")));
		}
		if len % DELTA_PAGE_SIZE != 0 {
			return Err(err(format!("guest memory region {gpa:#x} length {len} is not page-aligned")));
		}
		regions.push(MemRegion { gpa, len, file_offset });
		file_offset = file_offset
			.checked_add(len)
			.ok_or_else(|| err(format!("guest memory region {idx} file offset overflows")))?;
	}
	if regions.is_empty() {
		return Err(err("guest memory has no regions"));
	}
	Ok(regions)
}

fn region_len_usize(region: &MemRegion) -> Result<usize> {
	usize::try_from(region.len).map_err(|_| {
		err(format!("snapshot memory region @ {:#x} length {} exceeds usize", region.gpa, region.len))
	})
}

fn regions_total_len(regions: &[MemRegion]) -> Result<u64> {
	let mut total = 0u64;
	for (idx, region) in regions.iter().enumerate() {
		total = total
			.checked_add(region.len)
			.ok_or_else(|| err(format!("snapshot memory region {idx} total length overflows")))?;
	}
	Ok(total)
}

#[cfg(target_os = "windows")]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
	use std::os::windows::fs::FileExt;
	let mut written = 0;
	while written < buf.len() {
		let n = file.seek_write(&buf[written..], offset + written as u64)?;
		if n == 0 {
			return Err(std::io::ErrorKind::WriteZero.into());
		}
		written += n;
	}
	Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
	use std::os::unix::fs::FileExt;
	file.write_all_at(buf, offset)
}

/// Dump every guest-RAM region into `path` (slot order) and return the region
/// table.
///
/// All-zero pages become file holes: untouched guest RAM — usually most of a
/// fresh boot — costs neither dump time, disk space, nor transfer bytes,
/// while readers still observe the full logical image. The zero scan runs
/// across worker threads (this executes while the VM is paused), and data
/// runs are written with positioned writes, one syscall per run.
fn dump_memory_file(path: &Path, mem: &GuestMemoryMmap) -> Result<Vec<MemRegion>> {
	let file = create_snapshot_file(path)?;
	let regions = memory_region_table(mem)?;
	let mut logical = 0u64;
	for region in &regions {
		let ptr = mem
			.get_host_address(GuestAddress(region.gpa))
			.map_err(|e| err(format!("host address for {gpa:#x}: {e}", gpa = region.gpa)))?;
		let len = region_len_usize(region)?;
		// SAFETY: `get_host_address` returned a pointer into `mem` for this
		// region; `memory_region_table` supplied the region length, and `mem`
		// outlives the read-only slice.
		let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
		let nonzero = scan_nonzero_pages(slice);
		let pages = len / DELTA_PAGE_USIZE + usize::from(!len.is_multiple_of(DELTA_PAGE_USIZE));
		let mut page = 0usize;
		while page < pages {
			let bit = |index: usize| nonzero[index / 8] & (1 << (index % 8)) != 0;
			if !bit(page) {
				page += 1;
				continue;
			}
			let start = page;
			while page < pages && bit(page) {
				page += 1;
			}
			let byte_start = start * DELTA_PAGE_USIZE;
			let byte_end = len.min(page * DELTA_PAGE_USIZE);
			write_all_at(&file, &slice[byte_start..byte_end], logical + byte_start as u64)
				.map_err(|e| err(format!("writing region {gpa:#x}: {e}", gpa = region.gpa)))?;
		}
		logical += len as u64;
	}
	// Materialize the trailing hole so the logical length matches guest RAM.
	file
		.set_len(logical)
		.map_err(|e| err(format!("sizing {}: {e}", path.display())))?;
	file
		.sync_all()
		.map_err(|e| err(format!("syncing {}: {e}", path.display())))?;
	Ok(regions)
}

/// LSB-first bitmap of pages in `slice` containing any nonzero byte, scanned
/// across the rayon pool. Each output byte is accumulated in a register and
/// stored once; pages derive from the byte index.
fn scan_nonzero_pages(slice: &[u8]) -> Vec<u8> {
	let pages =
		slice.len() / DELTA_PAGE_USIZE + usize::from(!slice.len().is_multiple_of(DELTA_PAGE_USIZE));
	let mut bitmap = vec![0u8; pages.div_ceil(8)];
	bitmap
		.par_chunks_mut((DIFF_PAGES_PER_WORKER / 8) as usize)
		.enumerate()
		.for_each(|(chunk_index, bits)| {
			let first_page = chunk_index * (DIFF_PAGES_PER_WORKER as usize);
			for (byte_index, out) in bits.iter_mut().enumerate() {
				let page0 = first_page + byte_index * 8;
				let mut value = 0u8;
				for bit in 0..8u32 {
					let from = (page0 + bit as usize) * DELTA_PAGE_USIZE;
					if from >= slice.len() {
						break;
					}
					let to = slice.len().min(from + DELTA_PAGE_USIZE);
					if !memory::is_zero(&slice[from..to]) {
						value |= 1 << bit;
					}
				}
				*out = value;
			}
		});
	bitmap
}

/// Read-only mmap of a whole base snapshot memory file.
struct FileMap {
	map: memmap2::Mmap,
}

impl FileMap {
	/// Map `path` read-only, requiring it to be exactly `expected_len` bytes.
	fn open(path: &Path, expected_len: u64) -> Result<Self> {
		let file = open_snapshot_file(path)?;
		let len = file
			.metadata()
			.map_err(|e| err(format!("stat {}: {e}", path.display())))?
			.len();
		if len != expected_len {
			return Err(err(format!(
				"base memory file {} is {len} bytes, expected {expected_len}",
				path.display()
			)));
		}
		if len == 0 {
			return Err(err(format!("base memory file {} is empty", path.display())));
		}
		// SAFETY: the file remains unchanged for the lifetime of this private,
		// read-only mapping; snapshot generations are immutable after
		// publication.
		let map = unsafe { memmap2::MmapOptions::new().map(&file) }
			.map_err(|e| err(format!("mapping {}: {e}", path.display())))?;
		Ok(Self { map })
	}
}

/// Base-snapshot RAM used for diffing: a zero-copy map of a full base's memory
/// file, or a scratch reconstruction when the base is itself a delta chain.
enum BaseView {
	Mapped(FileMap),
	Scratch(GuestMemoryMmap),
}

/// One region's live/base page pointers plus its global page window. Pointers
/// are carried as `usize` so worker threads can capture them.
struct RegionDiff {
	base_ptr:   usize,
	live_ptr:   usize,
	pages:      u64,
	first_page: u64,
}

fn region_diff_specs(
	base: &BaseView,
	live: &GuestMemoryMmap,
	regions: &[MemRegion],
) -> Result<Vec<RegionDiff>> {
	let mut specs = Vec::with_capacity(regions.len());
	let mut first_page = 0u64;
	for region in regions {
		let live_ptr = live
			.get_host_address(GuestAddress(region.gpa))
			.map_err(|e| err(format!("host address for live {gpa:#x}: {e}", gpa = region.gpa)))?
			as usize;
		let base_ptr = match base {
			// In-bounds: FileMap::open checked the file covers the full region
			// table, and region file offsets are prefix sums of region lengths.
			BaseView::Mapped(map) => {
				let offset = usize::try_from(region.file_offset)
					.map_err(|_| err("region file offset exceeds usize"))?;
				map.map.as_ptr() as usize + offset
			},
			BaseView::Scratch(mem) => mem
				.get_host_address(GuestAddress(region.gpa))
				.map_err(|e| err(format!("host address for base {gpa:#x}: {e}", gpa = region.gpa)))?
				as usize,
		};
		specs.push(RegionDiff {
			base_ptr,
			live_ptr,
			pages: region.len / DELTA_PAGE_SIZE,
			first_page,
		});
		first_page += region.len / DELTA_PAGE_SIZE;
	}
	Ok(specs)
}

/// Dump a delta memory file relative to `base` and return the full region table
/// plus the delta descriptor. With a `guest_dirty` log, only pages flagged by
/// the hypervisor or by the host-write bitmap are inspected; otherwise every
/// page is compared across worker threads.
fn dump_delta_memory_file(
	path: &Path,
	delta_dir: &Path,
	base: &str,
	live: &GuestMemoryMmap,
	guest_dirty: Option<&[Vec<u64>]>,
) -> Result<(Vec<MemRegion>, DeltaMemory)> {
	let live_regions = memory_region_table(live)?;
	let base_dir = base_dir_of(delta_dir, base)?;
	let base_image = read_snapshot(&base_dir)?;
	let total_ram = regions_total_len(&live_regions)?;
	let base_view = if base_image.snapshot().delta.is_none() {
		if !same_layout(&base_image.snapshot().mem_regions, &live_regions) {
			return Err(err("delta base RAM layout differs from live memory"));
		}
		BaseView::Mapped(FileMap::open(base_image.memory_file(), total_ram)?)
	} else {
		let total_bytes =
			usize::try_from(total_ram).map_err(|_| err("guest RAM size overflows usize"))?;
		let scratch = memory::create_guest_memory(total_bytes)?;
		load_layer(&base_dir, &base_image, &scratch, &live_regions, 1)?;
		BaseView::Scratch(scratch)
	};
	let specs = region_diff_specs(&base_view, live, &live_regions)?;
	// RAM forked from this very base file only diverges in private copies.
	#[cfg(target_os = "linux")]
	let private_log = match (guest_dirty, &base_view) {
		(None, BaseView::Mapped(_)) => {
			private_copy_log(live, &live_regions, base_image.memory_file())
		},
		_ => None,
	};
	#[cfg(not(target_os = "linux"))]
	let private_log: Option<Vec<Vec<u64>>> = None;

	let mut file = create_snapshot_file(path)?;
	let changed = match guest_dirty.or(private_log.as_deref()) {
		Some(log) => diff_dirty_pages(&specs, live, log, &mut file)?,
		None => diff_pages_parallel(&specs, &mut file)?,
	};
	file
		.sync_all()
		.map_err(|e| err(format!("syncing {}: {e}", path.display())))?;
	Ok((live_regions, DeltaMemory { base: base.to_owned(), page_size: DELTA_PAGE_SIZE, changed }))
}

/// Pages of `live` that may differ from `base_memory` when guest RAM is a
/// `MAP_PRIVATE` mapping of that same file at the snapshot layout (a fork or
/// lazy restore of it): a page that was never written still reads the file's
/// bytes, so only private copies (present or swapped anonymous pages, per
/// `/proc/self/pagemap`) can differ. Returns one word bitmap of host pages per
/// region — the shape of the hypervisor dirty log — or `None` when RAM is not
/// such a mapping or [`memory::pagemap`] is unavailable, leaving the caller to
/// compare every page. Lazily populated 2 MiB RAM restored from the same file
/// reports its written huge pages instead ([`memory::lazy_written_log`]).
#[cfg(target_os = "linux")]
fn private_copy_log(
	live: &GuestMemoryMmap,
	regions: &[MemRegion],
	base_memory: &Path,
) -> Option<Vec<Vec<u64>>> {
	use std::os::unix::fs::{FileExt, MetadataExt};

	const PM_PRESENT: u64 = 1 << 63;
	const PM_SWAP: u64 = 1 << 62;
	const PM_FILE: u64 = 1 << 61;
	const ENTRY_BYTES: usize = size_of::<u64>();

	if let Some(log) = memory::lazy_written_log(live, base_memory) {
		return Some(log);
	}
	let base = fs::metadata(base_memory).ok()?;
	let host_page = host_page_size().ok()?;
	let pagemap = memory::pagemap()?;
	let mut log = Vec::with_capacity(regions.len());
	let mut entries = Vec::new();
	for (region, mapped) in regions.iter().zip(live.iter()) {
		let backing = mapped.file_offset()?;
		let file = backing.file().metadata().ok()?;
		if mapped.flags() & libc::MAP_PRIVATE == 0
			|| file.dev() != base.dev()
			|| file.ino() != base.ino()
			|| backing.start() != region.file_offset
		{
			return None;
		}
		let pages = usize::try_from(region.len.div_ceil(host_page)).ok()?;
		entries.clear();
		entries.resize(pages * ENTRY_BYTES, 0u8);
		let first_page = u64::try_from(mapped.as_ptr().addr()).ok()? / host_page;
		pagemap
			.read_exact_at(&mut entries, first_page.checked_mul(ENTRY_BYTES as u64)?)
			.ok()?;
		let mut words = vec![0u64; pages.div_ceil(64)];
		for (page, entry) in entries.as_chunks::<ENTRY_BYTES>().0.iter().enumerate() {
			let entry = u64::from_ne_bytes(*entry);
			if entry & (PM_PRESENT | PM_SWAP) != 0 && entry & PM_FILE == 0 {
				words[page / 64] |= 1 << (page % 64);
			}
		}
		log.push(words);
	}
	Some(log)
}

fn load_memory_file(path: &Path, mem: &GuestMemoryMmap, regions: &[MemRegion]) -> Result<()> {
	ensure_guest_memory_layout(mem, regions, "snapshot restore destination")?;
	let expected_len = regions_total_len(regions)?;
	let memory_len = snapshot_memory_len(path)?;
	if memory_len != expected_len {
		return Err(err(format!(
			"snapshot memory data length {expected_len} != memory file length {memory_len}"
		)));
	}
	let mut file = open_snapshot_file(path)?;
	for r in regions {
		file
			.seek(SeekFrom::Start(r.file_offset))
			.map_err(|e| err(format!("seeking to region {:#x}: {e}", r.gpa)))?;
		let ptr = mem
			.get_host_address(GuestAddress(r.gpa))
			.map_err(|e| err(format!("host address for {:#x}: {e}", r.gpa)))?;
		let len = region_len_usize(r)?;
		// SAFETY: `get_host_address` returned a pointer into the destination
		// guest mapping, which was checked above to match `regions`; the mutable
		// slice is used only for this file read.
		let dst = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
		file
			.read_exact(dst)
			.map_err(|e| err(format!("reading region {:#x}: {e}", r.gpa)))?;
	}
	Ok(())
}

/// Compare live RAM against the base view across the rayon pool, then stream
/// every changed live page to `out` in ascending page order. Returns the
/// LSB-first changed-page bitmap.
fn diff_pages_parallel(specs: &[RegionDiff], out: &mut File) -> Result<Vec<u8>> {
	let total_pages: u64 = specs.iter().map(|spec| spec.pages).sum();
	let mut bitmap = vec![0u8; total_pages.div_ceil(8) as usize];
	// One task per DIFF_PAGES_PER_WORKER pages; rayon work-steals the rest.
	bitmap
		.par_chunks_mut((DIFF_PAGES_PER_WORKER / 8) as usize)
		.enumerate()
		.for_each(|(chunk_index, bits)| {
			let first_page = chunk_index as u64 * DIFF_PAGES_PER_WORKER;
			diff_bitmap_chunk(specs, first_page, total_pages, bits);
		});
	write_changed_pages(specs, &bitmap, out)?;
	Ok(bitmap)
}

/// Fill `bits` (whose bit 0 is global page `first_page`) with the changed
/// pages it covers. Each output byte is accumulated in a register and stored
/// once — pages derive from the byte index, so there is no per-page bitmap
/// indexing. Infallible: all pointers were validated by
/// [`region_diff_specs`] and the VM is paused.
fn diff_bitmap_chunk(specs: &[RegionDiff], first_page: u64, total_pages: u64, bits: &mut [u8]) {
	// Pages ascend, so the owning spec advances monotonically.
	let mut spec_index = specs.partition_point(|spec| spec.first_page + spec.pages <= first_page);
	for (byte_index, out) in bits.iter_mut().enumerate() {
		let page0 = first_page + byte_index as u64 * 8;
		let mut value = 0u8;
		for bit in 0..8u32 {
			let page = page0 + u64::from(bit);
			if page >= total_pages {
				break;
			}
			while spec_index < specs.len()
				&& specs[spec_index].first_page + specs[spec_index].pages <= page
			{
				spec_index += 1;
			}
			let Some(spec) = specs.get(spec_index) else {
				break;
			};
			let offset = ((page - spec.first_page) * DELTA_PAGE_SIZE) as usize;
			// SAFETY: `page` lies inside this region's page window, so both
			// pointers address one full mapped page; base is an immutable
			// mapping and live RAM is quiesced (VM paused) for the dump.
			let (base_page, live_page) = unsafe {
				(
					std::slice::from_raw_parts((spec.base_ptr + offset) as *const u8, DELTA_PAGE_USIZE),
					std::slice::from_raw_parts((spec.live_ptr + offset) as *const u8, DELTA_PAGE_USIZE),
				)
			};
			if base_page != live_page {
				value |= 1 << bit;
			}
		}
		*out = value;
	}
}

/// Stream every bitmap-flagged live page to `out` in ascending page order.
fn write_changed_pages(specs: &[RegionDiff], bitmap: &[u8], out: &mut File) -> Result<()> {
	let mut writer = BufWriter::with_capacity(1 << 20, out);
	for spec in specs {
		for page in 0..spec.pages {
			let global = spec.first_page + page;
			if bitmap[(global / 8) as usize] & (1 << (global % 8)) == 0 {
				continue;
			}
			let offset = (page * DELTA_PAGE_SIZE) as usize;
			// SAFETY: the page lies inside this region's live mapping (see
			// `region_diff_specs`); the VM is paused for the dump.
			let live_page = unsafe {
				std::slice::from_raw_parts((spec.live_ptr + offset) as *const u8, DELTA_PAGE_USIZE)
			};
			writer
				.write_all(live_page)
				.map_err(|e| err(format!("writing delta page {global}: {e}")))?;
		}
	}
	writer
		.flush()
		.map_err(|e| err(format!("flushing delta pages: {e}")))?;
	Ok(())
}

#[cfg(unix)]
fn host_page_size() -> Result<u64> {
	// SAFETY: sysconf(_SC_PAGESIZE) has no memory-safety preconditions.
	u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
		.map_err(|_| err("host page size is not positive"))
}

#[cfg(target_os = "windows")]
#[allow(
	clippy::unnecessary_wraps,
	reason = "the shared snapshot callsite also handles Unix page-size failures"
)]
fn host_page_size() -> Result<u64> {
	use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};
	let mut info = SYSTEM_INFO::default();
	// SAFETY: `info` points to writable storage for the duration of the call.
	unsafe { GetSystemInfo(&mut info) };
	Ok(u64::from(info.dwPageSize))
}

/// Write only pages flagged dirty since the base was captured: the union of
/// the hypervisor's guest-write log and the host-write bitmap that raw DMA
/// paths mark. Every candidate is verified against the base so rewrites of
/// identical bytes do not inflate the delta; unflagged pages are skipped
/// entirely, making the dump O(dirty set) instead of O(guest RAM).
fn diff_dirty_pages(
	specs: &[RegionDiff],
	live: &GuestMemoryMmap,
	guest_log: &[Vec<u64>],
	out: &mut File,
) -> Result<Vec<u8>> {
	if guest_log.len() != specs.len() {
		return Err(err(format!(
			"guest dirty log covers {} slots for {} memory regions",
			guest_log.len(),
			specs.len()
		)));
	}
	let host_page = host_page_size()?;
	if host_page < DELTA_PAGE_SIZE || !host_page.is_multiple_of(DELTA_PAGE_SIZE) {
		return Err(err(format!("host page size {host_page} incompatible with delta pages")));
	}
	let total_pages: u64 = specs.iter().map(|spec| spec.pages).sum();
	let mut bitmap = vec![0u8; total_pages.div_ceil(8) as usize];
	let mut writer = BufWriter::with_capacity(1 << 20, out);
	for ((spec, log), region) in specs.iter().zip(guest_log).zip(live.iter()) {
		let host_pages = (spec.pages * DELTA_PAGE_SIZE).div_ceil(host_page);
		if (log.len() as u64) < host_pages.div_ceil(64) {
			return Err(err(format!(
				"guest dirty log holds {} words for {host_pages} host pages",
				log.len()
			)));
		}
		let host_bits: &vm_memory::mmap::MmapRegion<vm_memory::bitmap::AtomicBitmap> = region;
		let host_bitmap = host_bits.bitmap();
		for page in 0..spec.pages {
			let byte_offset = page * DELTA_PAGE_SIZE;
			let host_index = byte_offset / host_page;
			let guest_dirty = log[(host_index / 64) as usize] >> (host_index % 64) & 1 != 0;
			let host_dirty = host_bitmap.dirty_at(byte_offset as usize);
			if !guest_dirty && !host_dirty {
				continue;
			}
			let offset = byte_offset as usize;
			// SAFETY: the page lies inside this region's mappings (see
			// `region_diff_specs`); base is immutable and the VM is paused.
			let (base_page, live_page) = unsafe {
				(
					std::slice::from_raw_parts((spec.base_ptr + offset) as *const u8, DELTA_PAGE_USIZE),
					std::slice::from_raw_parts((spec.live_ptr + offset) as *const u8, DELTA_PAGE_USIZE),
				)
			};
			if base_page != live_page {
				let global = spec.first_page + page;
				bitmap[(global / 8) as usize] |= 1 << (global % 8);
				writer
					.write_all(live_page)
					.map_err(|e| err(format!("writing delta page {global}: {e}")))?;
			}
		}
	}
	writer
		.flush()
		.map_err(|e| err(format!("flushing delta pages: {e}")))?;
	Ok(bitmap)
}

/// Read changed pages from `memory_file` and apply them to `mem`. Pages are
/// stored back-to-back in ascending order, so the file is read sequentially,
/// one `read` per run of consecutive changed pages within a region.
fn apply_delta_pages(
	memory_file: &Path,
	mem: &GuestMemoryMmap,
	d: &DeltaMemory,
	regions: &[MemRegion],
) -> Result<()> {
	ensure_guest_memory_layout(mem, regions, "snapshot restore destination")?;
	let total_ram = regions_total_len(regions)?;
	let memory_len = snapshot_memory_len(memory_file)?;
	validate_delta_memory(d, total_ram, memory_len)?;
	let mut file = open_snapshot_file(memory_file)?;
	let changed = |page: u64| {
		usize::try_from(page / 8)
			.ok()
			.and_then(|byte| d.changed.get(byte))
			.is_some_and(|bits| bits & (1u8 << (page % 8)) != 0)
	};
	let mut first_page = 0u64;
	for region in regions {
		let pages = region.len / DELTA_PAGE_SIZE;
		let ptr = mem
			.get_host_address(GuestAddress(region.gpa))
			.map_err(|e| err(format!("host address for {:#x}: {e}", region.gpa)))?;
		let mut page = 0u64;
		while page < pages {
			if !changed(first_page + page) {
				page += 1;
				continue;
			}
			let start = page;
			while page < pages && changed(first_page + page) {
				page += 1;
			}
			let offset = usize::try_from(start * DELTA_PAGE_SIZE)
				.map_err(|_| err("delta page offset exceeds usize"))?;
			let len = usize::try_from((page - start) * DELTA_PAGE_SIZE)
				.map_err(|_| err("delta run length exceeds usize"))?;
			// SAFETY: `ptr` maps this whole region (layout checked above against
			// `regions`) and `[offset, offset + len)` lies within its `len`
			// bytes; the mutable slice is consumed by this single `read_exact`.
			let dst = unsafe { std::slice::from_raw_parts_mut(ptr.add(offset), len) };
			file.read_exact(dst).map_err(|e| {
				err(format!("reading delta pages {}..{}: {e}", first_page + start, first_page + page))
			})?;
		}
		first_page += pages;
	}
	Ok(())
}

/// Resolve a delta's `base` name to a directory path relative to the delta's
/// parent.
fn base_dir_of(dir: &Path, base: &str) -> Result<PathBuf> {
	if !is_safe_snapshot_name(base) {
		return Err(err(format!("delta base {base:?} is not a safe basename")));
	}
	let parent = dir
		.parent()
		.ok_or_else(|| err("delta snapshot dir has no parent"))?;
	Ok(parent.join(base))
}

fn same_layout(a: &[MemRegion], b: &[MemRegion]) -> bool {
	a.len() == b.len()
		&& a
			.iter()
			.zip(b.iter())
			.all(|(x, y)| x.gpa == y.gpa && x.len == y.len && x.file_offset == y.file_offset)
}

fn ensure_guest_memory_layout(
	mem: &GuestMemoryMmap,
	expected: &[MemRegion],
	context: &str,
) -> Result<()> {
	let actual = memory_region_table(mem)?;
	if !same_layout(&actual, expected) {
		return Err(err(format!("{context} RAM layout differs from snapshot metadata")));
	}
	Ok(())
}

/// Load a full snapshot's memory file into `mem`.
fn load_layer(
	dir: &Path,
	image: &SnapshotImage,
	mem: &GuestMemoryMmap,
	expected: &[MemRegion],
	depth: usize,
) -> Result<()> {
	if depth > MAX_DELTA_CHAIN_DEPTH {
		return Err(err(format!("snapshot delta chain exceeds max depth {MAX_DELTA_CHAIN_DEPTH}")));
	}
	if !same_layout(&image.snapshot().mem_regions, expected) {
		return Err(err("delta chain layer RAM layout differs from top snapshot"));
	}
	match &image.snapshot().delta {
		None => load_memory_file(image.memory_file(), mem, &image.snapshot().mem_regions),
		Some(d) => {
			let base_dir = base_dir_of(dir, &d.base)?;
			let base_image = read_snapshot(&base_dir)?;
			load_layer(&base_dir, &base_image, mem, expected, depth + 1)?;
			apply_delta_pages(image.memory_file(), mem, d, expected)
		},
	}
}

/// Reconstruct a (possibly delta) snapshot's guest RAM by loading the full
/// chain.
pub fn load_memory_chain(dir: &Path, image: &SnapshotImage, mem: &GuestMemoryMmap) -> Result<()> {
	let expected = image.snapshot().mem_regions.clone();
	ensure_guest_memory_layout(mem, &expected, "snapshot restore destination")?;
	load_layer(dir, image, mem, &expected, 0)
}

/// Map a delta snapshot's guest RAM lazily.
///
/// The deepest FULL layer's memory file becomes a `MAP_PRIVATE` mapping
/// (clean pages shared with the host page cache) and each delta layer is
/// applied on top, faulting private copies of only the changed pages, so
/// restore cost scales with the delta instead of guest RAM.
///
/// The base memory file MUST stay unmodified while the VM runs, because
/// unfaulted pages are read from it on demand (unlinking it is fine: the
/// mapping pins the inode). A later delta against the same base file finds
/// the applied pages as private copies (see `private_copy_log`).
///
/// `huge` selects lazily populated 2 MiB RAM for the base when the host
/// supports it (see `memory::create_guest_memory_private`); delta pages applied
/// on top then count as written for the next delta (`private_copy_log`).
pub fn map_memory_chain_private(
	dir: &Path,
	image: &SnapshotImage,
	huge: bool,
) -> Result<GuestMemoryMmap> {
	let expected = image.snapshot().mem_regions.clone();
	// Walk to the deepest full layer, remembering the delta layers on the way.
	let mut deltas: Vec<SnapshotImage> = Vec::new();
	let mut layer_dir = dir.to_path_buf();
	let mut layer = read_snapshot(&layer_dir)?;
	while let Some(delta) = &layer.snapshot().delta {
		if deltas.len() >= MAX_DELTA_CHAIN_DEPTH {
			return Err(err(format!(
				"snapshot delta chain exceeds max depth {MAX_DELTA_CHAIN_DEPTH}"
			)));
		}
		if !same_layout(&layer.snapshot().mem_regions, &expected) {
			return Err(err("delta chain layer RAM layout differs from top snapshot"));
		}
		layer_dir = base_dir_of(&layer_dir, &delta.base)?;
		deltas.push(layer);
		layer = read_snapshot(&layer_dir)?;
	}
	if !same_layout(&layer.snapshot().mem_regions, &expected) {
		return Err(err("delta chain base RAM layout differs from top snapshot"));
	}
	let regions: Vec<(u64, u64, u64)> = expected
		.iter()
		.map(|r| (r.gpa, r.len, r.file_offset))
		.collect();
	let mem = memory::create_guest_memory_private(layer.memory_file(), &regions, huge)?;
	for delta_layer in deltas.iter().rev() {
		let descriptor = delta_layer
			.snapshot()
			.delta
			.as_ref()
			.expect("chain layers above the base are deltas");
		apply_delta_pages(delta_layer.memory_file(), &mem, descriptor, &expected)?;
	}
	Ok(mem)
}

fn sync_dir(dir: &Path) -> Result<()> {
	let file = File::open(dir).map_err(|e| err(format!("opening {}: {e}", dir.display())))?;
	file
		.sync_all()
		.map_err(|e| err(format!("syncing {}: {e}", dir.display())))?;
	Ok(())
}

fn generation_state_file(generation: u64) -> String {
	format!("vmstate.{generation}.bin")
}

fn generation_memory_file(generation: u64) -> String {
	format!("memory.{generation}.bin")
}

fn generation_state_tmp_file(generation: u64) -> String {
	format!("vmstate.{generation}.bin.tmp")
}

fn generation_memory_tmp_file(generation: u64) -> String {
	format!("memory.{generation}.bin.tmp")
}

fn cleanup_unpublished_generation(dir: &Path, generation: u64) {
	for path in [
		dir.join(generation_memory_tmp_file(generation)),
		dir.join(generation_state_tmp_file(generation)),
		dir.join(generation_memory_file(generation)),
		dir.join(generation_state_file(generation)),
	] {
		let _ = fs::remove_file(path);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snapshot_names_are_bounded_safe_basenames() {
		for valid in ["checkpoint", "worker-1", "release_2.0"] {
			assert!(is_safe_snapshot_name(valid), "{valid}");
		}
		for invalid in [
			"",
			".hidden",
			"../escape",
			"nested/snapshot",
			r"nested\snapshot",
			"two..dots",
			"white space",
			"é",
		] {
			assert!(!is_safe_snapshot_name(invalid), "{invalid}");
		}
		assert!(!is_safe_snapshot_name(&"x".repeat(MAX_SNAPSHOT_NAME_BYTES + 1)));
	}

	fn block_device(mmio_base: u64, gsi: u32) -> DeviceState {
		DeviceState {
			kind: DeviceKind::Block,
			transport: DeviceTransportKind::Mmio,
			mmio_base,
			gsi,
			interrupt_status: 0,
			device_features_select: 0,
			driver_features_select: 0,
			acked_features: 0,
			status: 0,
			activated: false,
			queues: vec![queue(false)],
			backend: BackendHint::Block { path: "disk.img".to_string(), read_only: false },
			user_net_state: None,
			transport_pci: None,
			fs: None,
		}
	}

	fn queue(ready: bool) -> QueueStateSer {
		QueueStateSer {
			max_size: 8,
			next_avail: 0,
			next_used: 0,
			event_idx_enabled: false,
			size: 8,
			ready,
			desc_table: 0x1000,
			avail_ring: 0x2000,
			used_ring: 0x3000,
		}
	}

	#[cfg(target_arch = "x86_64")]
	fn pci_state(bar_base: u64) -> PciTransportStateSer {
		PciTransportStateSer {
			config_space: vec![0; 256],
			bar_base,
			bar0_probe: false,
			command: 0,
			msix: MsixStateSer {
				shared_vector: 0xffff,
				control:       0,
				pending:       0,
				table:         vec![MsixEntrySer { msg_addr: 0, msg_data: 0, vector_ctrl: 1 }],
			},
		}
	}

	fn serial_state() -> SerialState {
		SerialState {
			baud_divisor_low:         0,
			baud_divisor_high:        0,
			interrupt_enable:         0,
			interrupt_identification: 0,
			line_control:             0,
			line_status:              0,
			modem_control:            0,
			modem_status:             0,
			scratch:                  0,
			in_buffer:                Vec::new(),
		}
	}

	#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
	fn machine_state() -> MachineState {
		// SAFETY: zero is a valid inert value for these KVM state blobs in
		// migration-only unit tests; no ioctl consumes this synthetic state.
		unsafe { std::mem::zeroed() }
	}

	#[cfg(all(target_arch = "x86_64", target_os = "windows"))]
	fn machine_state() -> MachineState {
		MachineState {
			ioapic: crate::hv::IoApicState {
				selector:    0,
				id:          0,
				redirection: vec![1 << 16; 24],
			},
		}
	}

	#[cfg(target_arch = "aarch64")]
	fn machine_state() -> MachineState {
		#[cfg(target_os = "linux")]
		let gic = crate::arch::state::GicState::Kvm { entries: Vec::new() };
		#[cfg(target_os = "macos")]
		let gic = crate::arch::state::GicState::Hvf { blob: Vec::new() };
		MachineState { gic }
	}

	fn snapshot_with_devices(devices: Vec<DeviceState>) -> Snapshot {
		Snapshot {
			version: SNAPSHOT_VERSION,
			arch: build_arch().to_string(),
			backend: current_backend(),
			mem_mib: 0,
			cpus: 0,
			cmdline: String::new(),
			boot_mode: "direct".to_string(),
			firmware: None,
			mem_regions: Vec::new(),
			vcpus: Vec::new(),
			machine: machine_state(),
			serial: serial_state(),
			devices,
			delta: None,
		}
	}

	fn unique_temp_path(prefix: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"{prefix}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		))
	}

	#[cfg(not(target_os = "windows"))]
	fn temp_root_dir(prefix: &str) -> PathBuf {
		let path = unique_temp_path(prefix);
		fs::create_dir(&path).unwrap();
		path
	}

	fn mem_regions_for_mib(mem_mib: usize) -> Vec<MemRegion> {
		let mut file_offset = 0u64;
		memory::arrange_memory(mem_mib << 20)
			.into_iter()
			.map(|(addr, len)| {
				let len = len as u64;
				let region = MemRegion { gpa: addr.raw_value(), len, file_offset };
				file_offset += len;
				region
			})
			.collect()
	}

	#[test]
	fn rejects_snapshot_cpus_over_launch_cap() {
		let cpus = MAX_CPUS + 1;
		let err = validate_snapshot_resource_caps(cpus, 1)
			.unwrap_err()
			.to_string();
		assert!(err.contains("cpus"), "unexpected error: {err}");
		assert!(err.contains("launch cap"), "unexpected error: {err}");
		assert!(err.contains(&MAX_CPUS.to_string()), "unexpected error: {err}");
	}

	#[test]
	fn rejects_snapshot_mem_over_launch_cap() {
		let mem_mib = MAX_MEM_MIB + 1;
		let err = validate_snapshot_resource_caps(1, mem_mib)
			.unwrap_err()
			.to_string();
		assert!(err.contains("memory"), "unexpected error: {err}");
		assert!(err.contains("launch cap"), "unexpected error: {err}");
		assert!(err.contains(&MAX_MEM_MIB.to_string()), "unexpected error: {err}");
	}

	#[cfg(target_arch = "x86_64")]
	#[test]
	fn accepts_pci_device_transport_with_state() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.transport = DeviceTransportKind::Pci;
		device.transport_pci = Some(pci_state(MMIO_MEM_START));

		validate_device_states(&[device], &[]).unwrap();
	}

	#[test]
	fn rejects_virtio_fs_device_without_serialized_state() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Fs;
		device.queues = vec![queue(false), queue(false)];
		device.backend = BackendHint::Fs {
			tag:        "host".to_string(),
			shared_dir: "/srv".to_string(),
			read_only:  true,
		};

		let err = validate_device_states(&[device], &[])
			.unwrap_err()
			.to_string();
		assert!(err.contains("missing fs state"));
	}

	#[test]
	fn accepts_virtio_fs_device_with_serialized_state() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Fs;
		device.queues = vec![queue(false), queue(false)];
		device.backend = BackendHint::Fs {
			tag:        "host".to_string(),
			shared_dir: "/srv".to_string(),
			read_only:  true,
		};
		device.fs = Some(FsStateSer {
			inodes: vec![(1, ".".to_string()), (2, "file".to_string())],
			next:   3,
		});

		validate_device_states(&[device], &[]).unwrap();
	}

	#[test]
	fn rejects_backend_kind_mismatch() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.backend = BackendHint::Console;

		let err = validate_device_states(&[device], &[])
			.unwrap_err()
			.to_string();
		assert!(err.contains("backend does not match"));
	}

	#[test]
	fn accepts_rng_device_with_single_queue() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Rng;
		device.backend = BackendHint::Rng;
		// virtio-rng exposes a single requestq.
		device.queues = vec![queue(false)];

		validate_device_states(&[device], &[]).unwrap();
	}

	#[test]
	fn rejects_rng_device_with_wrong_queue_count() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Rng;
		device.backend = BackendHint::Rng;
		device.queues = vec![queue(false), queue(false)];

		let err = validate_device_states(&[device], &[])
			.unwrap_err()
			.to_string();
		assert!(err.contains("queue count"), "unexpected error: {err}");
	}

	#[test]
	fn rejects_queue_size_larger_than_max() {
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.queues[0].size = device.queues[0].max_size + 1;

		let err = validate_device_states(&[device], &[])
			.unwrap_err()
			.to_string();
		assert!(err.contains("exceeds max"));
	}

	#[test]
	fn rejects_duplicate_gsi() {
		let err = validate_device_addressing(&[
			block_device(MMIO_MEM_START, IRQ_BASE),
			block_device(MMIO_MEM_START + MMIO_DEVICE_SIZE, IRQ_BASE),
		])
		.unwrap_err()
		.to_string();
		assert!(err.contains("reuses GSI"));
	}

	#[test]
	fn rejects_overlapping_mmio_window() {
		let err = validate_device_addressing(&[
			block_device(MMIO_MEM_START, IRQ_BASE),
			block_device(MMIO_MEM_START, IRQ_BASE + 1),
		])
		.unwrap_err()
		.to_string();
		assert!(err.contains("overlaps"));
	}

	#[test]
	fn rejects_ready_queue_ring_outside_guest_ram() {
		let mut q = queue(true);
		q.used_ring = 0xfff0;
		let err = validate_ready_queue_ram(0, 0, &q, &[MemRegion {
			gpa:         0,
			len:         0x1_0000,
			file_offset: 0,
		}])
		.unwrap_err()
		.to_string();
		assert!(err.contains("used ring"));
		assert!(err.contains("outside declared guest RAM"));
	}

	#[test]
	fn rejects_too_new_snapshot_version_before_full_decode() {
		let path = std::env::temp_dir().join(format!(
			"vmon-too-new-{}-{}.bin",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		let bytes = postcard::to_allocvec(&(SNAPSHOT_VERSION + 1)).unwrap();
		fs::write(&path, bytes).unwrap();

		let err = match read_state_file(&path) {
			Ok(_) => panic!("too-new snapshot version was accepted"),
			Err(e) => e.to_string(),
		};
		let _ = fs::remove_file(&path);

		assert!(err.contains("newer than supported"));
	}

	#[cfg(unix)]
	#[test]
	fn snapshot_files_reject_symlinks_for_reads_and_writes() {
		let dir = temp_root_dir("vmon-snapshot-symlink");
		let outside = unique_temp_path("vmon-snapshot-outside");
		fs::write(&outside, b"1\n").unwrap();
		for file in [CURRENT_GENERATION_FILE, "vmstate.1.bin", "memory.1.bin"] {
			std::os::unix::fs::symlink(&outside, dir.join(file)).unwrap();
		}

		assert!(current_generation(&dir).is_err());
		assert!(read_state_file(&dir.join("vmstate.1.bin")).is_err());
		assert!(snapshot_memory_len(&dir.join("memory.1.bin")).is_err());
		let write_link = dir.join("vmstate.2.bin.tmp");
		std::os::unix::fs::symlink(&outside, &write_link).unwrap();
		assert!(create_snapshot_file(&write_link).is_err());
		assert_eq!(fs::read(&outside).unwrap(), b"1\n");

		fs::remove_dir_all(dir).unwrap();
		fs::remove_file(outside).unwrap();
	}

	#[test]
	fn pci_transport_state_survives_serde_round_trip() {
		let mut config_space = vec![0u8; 256];
		for (i, byte) in config_space.iter_mut().enumerate() {
			*byte = (i as u8) ^ 0xa5;
		}
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.transport = DeviceTransportKind::Pci;
		device.transport_pci = Some(PciTransportStateSer {
			config_space: config_space.clone(),
			bar_base:     0xc000_0000,
			bar0_probe:   true,
			command:      0x0406,
			msix:         MsixStateSer {
				shared_vector: 3,
				control:       0x8000,
				pending:       0xdead_beef_0000_0001,
				table:         vec![
					MsixEntrySer { msg_addr: 0xfee0_0000, msg_data: 0x4021, vector_ctrl: 0 },
					MsixEntrySer { msg_addr: 0xfee0_1000, msg_data: 0x4022, vector_ctrl: 1 },
				],
			},
		});

		let snap = snapshot_with_devices(vec![device]);
		let bytes = postcard::to_allocvec(&snap).unwrap();
		let (decoded, rest) = postcard::take_from_bytes::<Snapshot>(&bytes).unwrap();
		assert!(rest.is_empty(), "trailing bytes after decode: {rest:?}");

		let restored = &decoded.devices[0];
		assert_eq!(restored.transport, DeviceTransportKind::Pci);
		let pci = restored
			.transport_pci
			.as_ref()
			.expect("PCI transport state survives round-trip");
		assert_eq!(pci.config_space, config_space);
		assert_eq!(pci.bar_base, 0xc000_0000);
		assert!(pci.bar0_probe);
		assert_eq!(pci.command, 0x0406);
		assert_eq!(pci.msix.shared_vector, 3);
		assert_eq!(pci.msix.control, 0x8000);
		assert_eq!(pci.msix.pending, 0xdead_beef_0000_0001);
		assert_eq!(pci.msix.table.len(), 2);
		assert_eq!(pci.msix.table[0].msg_addr, 0xfee0_0000);
		assert_eq!(pci.msix.table[0].msg_data, 0x4021);
		assert_eq!(pci.msix.table[0].vector_ctrl, 0);
		assert_eq!(pci.msix.table[1].msg_addr, 0xfee0_1000);
		assert_eq!(pci.msix.table[1].msg_data, 0x4022);
		assert_eq!(pci.msix.table[1].vector_ctrl, 1);
	}

	#[test]
	fn user_net_state_survives_device_serde_round_trip() {
		let state = vec![0, 0, 0, 7, 0xaa, 0xbb, 0xcc];
		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Net;
		device.queues = vec![queue(false), queue(false)];
		device.backend = BackendHint::UserNet { mac: [0x02, 0, 0, 0, 0, 1] };
		device.user_net_state = Some(state.clone());

		validate_device_states(&[device.clone()], &[]).unwrap();
		let snap = snapshot_with_devices(vec![device]);
		let bytes = postcard::to_allocvec(&snap).unwrap();
		let (decoded, rest) = postcard::take_from_bytes::<Snapshot>(&bytes).unwrap();
		assert!(rest.is_empty(), "trailing bytes after decode: {rest:?}");
		assert_eq!(decoded.devices[0].user_net_state.as_deref(), Some(state.as_slice()));
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn fs_state_survives_serde_round_trip_and_restore_drops_stale() {
		let root = temp_root_dir("vmon-snap-fs");
		fs::write(root.join("kept"), b"ok").unwrap();

		let mut device = block_device(MMIO_MEM_START, IRQ_BASE);
		device.kind = DeviceKind::Fs;
		device.queues = vec![queue(false), queue(false)];
		device.backend = BackendHint::Fs {
			tag:        "host".to_string(),
			shared_dir: root.to_string_lossy().into_owned(),
			read_only:  true,
		};
		device.fs = Some(FsStateSer {
			inodes: vec![(1, ".".to_string()), (2, "kept".to_string()), (3, "missing".to_string())],
			next:   4,
		});

		// serialize -> deserialize: the inode table must survive intact.
		let snap = snapshot_with_devices(vec![device]);
		let bytes = postcard::to_allocvec(&snap).unwrap();
		let (decoded, rest) = postcard::take_from_bytes::<Snapshot>(&bytes).unwrap();
		assert!(rest.is_empty(), "trailing bytes after decode: {rest:?}");

		let fs_ser = decoded.devices[0]
			.fs
			.as_ref()
			.expect("virtio-fs state survives round-trip");
		assert_eq!(fs_ser.inodes, vec![
			(1, ".".to_string()),
			(2, "kept".to_string()),
			(3, "missing".to_string()),
		]);
		assert_eq!(fs_ser.next, 4);

		// restore against the live root: the stale "missing" nodeid is dropped,
		// resolvable nodeids survive.
		let fs = crate::virtio::fs::Fs::restore("host".to_string(), root.clone(), fs_ser, true)
			.expect("restore against tempdir root");
		let saved = fs.save();
		assert!(
			saved
				.inodes
				.iter()
				.any(|(id, path)| *id == 1 && path == "."),
			"root nodeid preserved: {:?}",
			saved.inodes
		);
		assert!(
			saved
				.inodes
				.iter()
				.any(|(id, path)| *id == 2 && path == "kept"),
			"resolvable nodeid preserved: {:?}",
			saved.inodes
		);
		assert!(
			!saved.inodes.iter().any(|(id, _)| *id == 3),
			"stale nodeid dropped: {:?}",
			saved.inodes
		);
		assert_eq!(saved.next, 4);

		fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn validate_memory_regions_rejects_full_memory_file_length_mismatch() {
		let mut snap = snapshot_with_devices(Vec::new());
		snap.mem_mib = 1;
		snap.mem_regions = mem_regions_for_mib(1);

		let err = validate_memory_regions(&snap, (1 << 20) + DELTA_PAGE_SIZE)
			.unwrap_err()
			.to_string();
		assert!(err.contains("memory file length"), "unexpected error: {err}");
	}

	#[test]
	fn load_memory_chain_rejects_destination_layout_mismatch_before_copy() {
		let mut snap = snapshot_with_devices(Vec::new());
		snap.mem_mib = 2;
		snap.mem_regions = mem_regions_for_mib(2);
		let image =
			SnapshotImage { snapshot: snap, memory_file: unique_temp_path("vmon-missing-memory") };
		let mem = memory::create_guest_memory(1 << 20).unwrap();

		let err = load_memory_chain(Path::new("."), &image, &mem)
			.unwrap_err()
			.to_string();
		assert!(err.contains("layout differs"), "unexpected error: {err}");
	}

	#[test]
	fn delta_descriptor_validates_without_memory_file_for_remote_page_in() {
		let total_ram = 1u64 << 20;
		let pages = total_ram / DELTA_PAGE_SIZE;
		let mut changed = vec![0u8; pages.div_ceil(8) as usize];
		changed[0] = 0b101;
		let mut delta = DeltaMemory { base: "base".into(), page_size: DELTA_PAGE_SIZE, changed };
		validate_delta_descriptor(&delta, total_ram).expect("consistent delta descriptor");

		delta.changed.push(0);
		let err = validate_delta_descriptor(&delta, total_ram)
			.unwrap_err()
			.to_string();
		assert!(err.contains("bitmap length"), "unexpected error: {err}");
	}

	#[test]
	fn validate_delta_memory_accepts_consistent_bitmap() {
		let d = DeltaMemory {
			base:      "base0".to_string(),
			page_size: DELTA_PAGE_SIZE,
			changed:   vec![0b0000_0011],
		};
		validate_delta_memory(&d, 8 * DELTA_PAGE_SIZE, 2 * DELTA_PAGE_SIZE).unwrap();
	}

	#[test]
	fn validate_delta_memory_rejects_wrong_memory_len() {
		let d = DeltaMemory {
			base:      "base0".to_string(),
			page_size: DELTA_PAGE_SIZE,
			changed:   vec![0b0000_0011],
		};
		let err = validate_delta_memory(&d, 8 * DELTA_PAGE_SIZE, 3 * DELTA_PAGE_SIZE)
			.unwrap_err()
			.to_string();
		assert!(err.contains("changed pages"), "unexpected error: {err}");
	}

	#[test]
	fn validate_delta_memory_rejects_bitmap_length_mismatch() {
		let d = DeltaMemory {
			base:      "base0".to_string(),
			page_size: DELTA_PAGE_SIZE,
			changed:   vec![0, 0],
		};
		let err = validate_delta_memory(&d, 8 * DELTA_PAGE_SIZE, 0)
			.unwrap_err()
			.to_string();
		assert!(err.contains("bitmap length"), "unexpected error: {err}");
	}

	#[test]
	fn validate_delta_memory_rejects_set_pad_bits() {
		let d = DeltaMemory {
			base:      "base0".to_string(),
			page_size: DELTA_PAGE_SIZE,
			changed:   vec![0b1111_0000],
		};
		let err = validate_delta_memory(&d, 4 * DELTA_PAGE_SIZE, 4 * DELTA_PAGE_SIZE)
			.unwrap_err()
			.to_string();
		assert!(err.contains("past page count"), "unexpected error: {err}");
	}

	#[test]
	fn validate_delta_memory_rejects_unsafe_base() {
		let d = DeltaMemory {
			base:      "../evil".to_string(),
			page_size: DELTA_PAGE_SIZE,
			changed:   vec![0b0000_0001],
		};
		let err = validate_delta_memory(&d, 8 * DELTA_PAGE_SIZE, DELTA_PAGE_SIZE)
			.unwrap_err()
			.to_string();
		assert!(err.contains("safe basename"), "unexpected error: {err}");
	}

	#[test]
	fn delta_diff_apply_round_trip() {
		let sz = 4 * 1024 * 1024usize;
		let base = memory::create_guest_memory(sz).unwrap();
		let live = memory::create_guest_memory(sz).unwrap();
		let restored = memory::create_guest_memory(sz).unwrap();

		let gpa = base.iter().next().unwrap().start_addr();

		// Fill base with a fixed pattern, then copy it to live and restored.
		let base_ptr = base.get_host_address(gpa).unwrap();
		// SAFETY: `base` was created with `sz` bytes, and `gpa` is the start of
		// its first region, so this pointer covers the whole guest-memory range.
		let base_slice = unsafe { std::slice::from_raw_parts_mut(base_ptr, sz) };
		for (i, b) in base_slice.iter_mut().enumerate() {
			*b = (i % 256) as u8;
		}

		let live_ptr = live.get_host_address(gpa).unwrap();
		// SAFETY: `live` was created with the same `sz` byte length, and `gpa`
		// is the start of its first region; the resulting mutable slice is the
		// only live slice to this mapping in the test.
		let live_slice = unsafe { std::slice::from_raw_parts_mut(live_ptr, sz) };
		live_slice.copy_from_slice(base_slice);

		let restored_ptr = restored.get_host_address(gpa).unwrap();
		// SAFETY: `restored` was created with the same `sz` byte length, and
		// `gpa` is the start of its first region; this mutable slice is used
		// only to seed the restored memory before delta application.
		let restored_slice = unsafe { std::slice::from_raw_parts_mut(restored_ptr, sz) };
		restored_slice.copy_from_slice(base_slice);

		// Mutate two non-adjacent pages in live.
		let page0 = DELTA_PAGE_SIZE as usize;
		let page1 = 3 * DELTA_PAGE_SIZE as usize;
		for i in 0..DELTA_PAGE_SIZE as usize {
			live_slice[page0 + i] = 0xaa;
			live_slice[page1 + i] = 0xbb;
		}

		let regions = memory_region_table(&base).unwrap();
		let tmp_path = unique_temp_path("vmon-delta-diff");
		let mut file = File::create(&tmp_path).unwrap();
		let base_view = BaseView::Scratch(base);
		let specs = region_diff_specs(&base_view, &live, &regions).unwrap();
		let bitmap = diff_pages_parallel(&specs, &mut file).unwrap();
		drop(file);

		let set = bitmap.iter().map(|b| b.count_ones()).sum::<u32>();
		assert_eq!(set, 2, "expected two changed pages");

		let d =
			DeltaMemory { base: "x".to_string(), page_size: DELTA_PAGE_SIZE, changed: bitmap };
		apply_delta_pages(&tmp_path, &restored, &d, &regions).unwrap();
		let _ = fs::remove_file(&tmp_path);

		let restored_ptr = restored.get_host_address(gpa).unwrap();
		// SAFETY: `restored` still owns the `sz` byte mapping starting at `gpa`;
		// the earlier mutable restored slice is no longer used before this
		// read-only comparison slice is created.
		let restored_slice = unsafe { std::slice::from_raw_parts(restored_ptr, sz) };
		assert_eq!(restored_slice, live_slice);
	}

	/// Contract of the dirty-window delta: flagged-but-unchanged pages are
	/// verified against the base and dropped, unflagged pages are skipped
	/// without inspection, and flags may arrive from either the guest-write
	/// log or the host-write bitmap.
	#[test]
	fn dirty_diff_writes_only_flagged_changed_pages() {
		let sz = 4 * 1024 * 1024usize;
		let base = memory::create_guest_memory(sz).unwrap();
		let live = memory::create_guest_memory(sz).unwrap();
		let gpa = base.iter().next().unwrap().start_addr();

		let base_ptr = base.get_host_address(gpa).unwrap();
		// SAFETY: `base` owns `sz` mapped bytes starting at `gpa`; this is the
		// only slice over it in the test.
		let base_slice = unsafe { std::slice::from_raw_parts_mut(base_ptr, sz) };
		for (i, b) in base_slice.iter_mut().enumerate() {
			*b = (i % 256) as u8;
		}
		let live_ptr = live.get_host_address(gpa).unwrap();
		// SAFETY: `live` owns `sz` mapped bytes starting at `gpa`; this is the
		// only mutable slice over it in the test.
		let live_slice = unsafe { std::slice::from_raw_parts_mut(live_ptr, sz) };
		live_slice.copy_from_slice(base_slice);

		let host_page = host_page_size().unwrap();
		let group = (host_page / DELTA_PAGE_SIZE).max(1) as usize;
		// Three delta pages in three distinct host pages: `changed_host`
		// (changed + host-flagged), `changed_guest` (changed + guest-flagged),
		// and `silent` (changed but never flagged -> must be skipped).
		let changed_host = group;
		let changed_guest = 2 * group;
		let silent = 3 * group;
		for page in [changed_host, changed_guest, silent] {
			live_slice[page * DELTA_PAGE_USIZE] ^= 0xff;
		}
		memory::mark_dirty(
			&live,
			GuestAddress(gpa.raw_value() + (changed_host * DELTA_PAGE_USIZE) as u64),
			DELTA_PAGE_USIZE,
		);
		// Guest log flags `changed_guest`'s host page plus an unchanged host
		// page (a false positive that verification must drop).
		let host_pages = (sz as u64).div_ceil(host_page);
		let mut log = vec![0u64; host_pages.div_ceil(64) as usize];
		for host_index in [
			(changed_guest as u64 * DELTA_PAGE_SIZE) / host_page,
			(4 * group as u64 * DELTA_PAGE_SIZE) / host_page,
		] {
			log[(host_index / 64) as usize] |= 1 << (host_index % 64);
		}

		let regions = memory_region_table(&base).unwrap();
		let base_view = BaseView::Scratch(base);
		let specs = region_diff_specs(&base_view, &live, &regions).unwrap();
		let tmp_path = unique_temp_path("vmon-dirty-diff");
		let mut file = File::create(&tmp_path).unwrap();
		let bitmap = diff_dirty_pages(&specs, &live, &[log], &mut file).unwrap();
		drop(file);
		let _ = fs::remove_file(&tmp_path);

		let bit = |page: usize| bitmap[page / 8] & (1 << (page % 8)) != 0;
		assert!(bit(changed_host), "host-flagged changed page must be captured");
		assert!(bit(changed_guest), "guest-flagged changed page must be captured");
		assert!(!bit(silent), "unflagged page is skipped by contract");
		let set = bitmap.iter().map(|b| b.count_ones()).sum::<u32>();
		assert_eq!(set, 2, "false positives must be verified away");
	}

	/// Pause of a fork: only pages written since the fork are candidates (a
	/// rewrite with identical bytes is verified away, a read is never one), and
	/// a fork of that delta still reports the applied pages, so the next pause
	/// against the same base keeps them.
	#[cfg(target_os = "linux")]
	#[test]
	fn private_fork_delta_tracks_written_pages_across_pauses() {
		use vm_memory::Bytes;

		const MIB: usize = 4;
		memory::open_pagemap();
		let regions = mem_regions_for_mib(MIB);
		let total = regions_total_len(&regions).unwrap();
		let tuples: Vec<(u64, u64, u64)> = regions
			.iter()
			.map(|r| (r.gpa, r.len, r.file_offset))
			.collect();
		let image: Vec<u8> = (0..total)
			.map(|i| u8::try_from(i / DELTA_PAGE_SIZE % 251 + 1).unwrap())
			.collect();
		let base_path = unique_temp_path("vmon-private-base");
		fs::write(&base_path, &image).unwrap();
		assert_eq!(regions.len(), 1, "{MIB} MiB of RAM is one region");
		let gpa_of = |page: u64| GuestAddress(regions[0].gpa + page * DELTA_PAGE_SIZE);
		let page_bytes = |page: u64| {
			let start = usize::try_from(page * DELTA_PAGE_SIZE).unwrap();
			image[start..start + DELTA_PAGE_USIZE].to_vec()
		};
		let read_all = |mem: &GuestMemoryMmap| {
			let mut out = vec![0u8; usize::try_from(total).unwrap()];
			for region in &regions {
				let start = usize::try_from(region.file_offset).unwrap();
				let end = start + usize::try_from(region.len).unwrap();
				mem.read_slice(&mut out[start..end], GuestAddress(region.gpa))
					.unwrap();
			}
			out
		};
		// Diffs `live` against the base the way a delta pause does and
		// returns the changed-page bitmap plus the delta memory file.
		let delta_of = |live: &GuestMemoryMmap| {
			let log = private_copy_log(live, &regions, &base_path).expect("private fork log");
			let base_view = BaseView::Mapped(FileMap::open(&base_path, total).unwrap());
			let specs = region_diff_specs(&base_view, live, &regions).unwrap();
			let path = unique_temp_path("vmon-private-delta");
			let mut file = File::create(&path).unwrap();
			let changed = diff_dirty_pages(&specs, live, &log, &mut file).unwrap();
			(changed, path)
		};
		let bit = |bitmap: &[u8], page: u64| {
			bitmap[usize::try_from(page / 8).unwrap()] & (1 << (page % 8)) != 0
		};

		let (written, rewritten, read, later) = (3u64, 7u64, 9u64, 11u64);
		let fork = memory::create_guest_memory_private(&base_path, &tuples, false).unwrap();
		let mut scratch = vec![0u8; DELTA_PAGE_USIZE];
		fork.read_slice(&mut scratch, gpa_of(read)).unwrap();
		fork
			.write_slice(&[0xee; DELTA_PAGE_USIZE], gpa_of(written))
			.unwrap();
		fork
			.write_slice(&page_bytes(rewritten), gpa_of(rewritten))
			.unwrap();

		let (changed, delta_path) = delta_of(&fork);
		assert!(bit(&changed, written), "a written page must be stored");
		assert!(!bit(&changed, rewritten), "an identical rewrite must be verified away");
		assert!(!bit(&changed, read), "a read must not be a candidate");
		assert_eq!(changed.iter().map(|b| b.count_ones()).sum::<u32>(), 1);

		// Resume: the base mapped again, the delta applied on top.
		let resumed = memory::create_guest_memory_private(&base_path, &tuples, false).unwrap();
		let delta = DeltaMemory { base: "base".into(), page_size: DELTA_PAGE_SIZE, changed };
		apply_delta_pages(&delta_path, &resumed, &delta, &regions).unwrap();
		assert_eq!(read_all(&resumed), read_all(&fork));
		resumed
			.write_slice(&[0x11; DELTA_PAGE_USIZE], gpa_of(later))
			.unwrap();

		let (changed, second_path) = delta_of(&resumed);
		assert!(bit(&changed, written), "applied delta pages must stay in the next delta");
		assert!(bit(&changed, later));
		assert_eq!(changed.iter().map(|b| b.count_ones()).sum::<u32>(), 2);

		// Anonymous RAM or a different base file is not a fork of the base.
		let anonymous = memory::create_guest_memory(MIB << 20).unwrap();
		assert!(private_copy_log(&anonymous, &regions, &base_path).is_none());
		let other_path = unique_temp_path("vmon-private-other");
		fs::write(&other_path, &image).unwrap();
		assert!(private_copy_log(&fork, &regions, &other_path).is_none());

		for path in [base_path, delta_path, second_path, other_path] {
			let _ = fs::remove_file(path);
		}
	}
}
