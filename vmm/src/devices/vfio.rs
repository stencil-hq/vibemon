//! NVIDIA SR-IOV vGPU assignment through the vendor-specific VFIO cdev API.
//!
//! The host NVIDIA vGPU manager owns profile creation. The VMM accepts only a
//! pre-created NVIDIA virtual function, opens its VFIO cdev before dropping
//! privileges, maps guest RAM through iommufd, and presents the function on the
//! guest PCI bus.

use std::{
	fs::{self, File, OpenOptions},
	io,
	os::fd::AsRawFd,
	path::{Path, PathBuf},
	sync::{Arc, Weak},
	thread::{self, JoinHandle},
};

use iommufd_ioctls::IommuFd;
use kvm_bindings::{kvm_msi, kvm_userspace_memory_region};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use tracing::{error, warn};
use vfio_bindings::bindings::vfio::{
	VFIO_PCI_BAR0_REGION_INDEX, VFIO_PCI_CONFIG_REGION_INDEX, VFIO_PCI_INTX_IRQ_INDEX,
	VFIO_PCI_MSI_IRQ_INDEX, VFIO_PCI_MSIX_IRQ_INDEX, VFIO_REGION_INFO_FLAG_MMAP,
};
use vfio_ioctls::{VfioDevice, VfioIommufd, VfioOps, VfioRegionInfoCap, VfioRegionSparseMmapArea};
use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

use crate::{
	config::MAX_VFIO_GPUS,
	devices::pci::PciFunction,
	hv::Vm,
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
};

const NVIDIA_VENDOR_ID: u16 = 0x10de;
const PCI_CONFIG_SPACE_SIZE: usize = 4096;
const PCI_COMMAND: usize = 0x04;
const PCI_STATUS: usize = 0x06;
const PCI_BAR0: usize = 0x10;
const PCI_CAPABILITY_LIST: usize = 0x34;
const PCI_COMMAND_MEMORY: u16 = 0x0002;
const PCI_STATUS_CAP_LIST: u16 = 0x0010;
const PCI_CAP_ID_MSI: u8 = 0x05;
const PCI_CAP_ID_MSIX: u8 = 0x11;
const MSI_CONTROL_ENABLE: u16 = 0x0001;
const MSI_CONTROL_MMC_MASK: u16 = 0x000e;
const MSI_CONTROL_MME_MASK: u16 = 0x0070;
const MSI_CONTROL_64BIT: u16 = 0x0080;
const MSIX_CONTROL_TABLE_SIZE: u16 = 0x07ff;
const MSIX_CONTROL_FUNCTION_MASK: u16 = 0x4000;
const MSIX_CONTROL_ENABLE: u16 = 0x8000;
const MSIX_VECTOR_MASKED: u32 = 0x0000_0001;
const MAX_GPU_VECTORS: usize = 64;
const KVM_DEVICE_SLOT_BASE: u32 = 32;
const KVM_DEVICE_SLOT_END: u32 = 256;
/// Base of the 64-bit guest BAR aperture reserved for assigned GPUs.
pub const PCI_HIGH_MMIO_START: u64 = 0x100_0000_0000;
/// Size of the 64-bit guest BAR aperture reserved for assigned GPUs.
pub const PCI_HIGH_MMIO_SIZE: u64 = 0x400_0000_0000;

/// Host resources opened while the launcher still has access to VFIO device
/// nodes.
pub struct PreopenedVfio {
	iommufd: Arc<IommuFd>,
	devices: Vec<PreopenedGpu>,
}

struct PreopenedGpu {
	sysfs_path: PathBuf,
	device:     File,
}

/// PCI BAR and KVM memory-slot allocator shared by assigned GPU functions.
pub struct VfioPciResources {
	low_next:  u64,
	low_end:   u64,
	high_next: u64,
	high_end:  u64,
	slot_next: u32,
}

impl VfioPciResources {
	/// Create an allocator after the last emulated 32-bit PCI BAR.
	pub fn new(low_next: u64, low_end: u64) -> Result<Self> {
		if low_next > low_end {
			return Err(err("VFIO PCI low MMIO aperture starts beyond its end"));
		}
		Ok(Self {
			low_next,
			low_end,
			high_next: PCI_HIGH_MMIO_START,
			high_end: PCI_HIGH_MMIO_START + PCI_HIGH_MMIO_SIZE,
			slot_next: KVM_DEVICE_SLOT_BASE,
		})
	}

	fn allocate_bar(&mut self, size: u64, is_64bit: bool) -> Result<u64> {
		if size == 0 || !size.is_power_of_two() {
			return Err(err(format!("VFIO PCI BAR size {size:#x} is not a nonzero power of two")));
		}
		let (next, end, label) = if is_64bit {
			(&mut self.high_next, self.high_end, "64-bit")
		} else {
			(&mut self.low_next, self.low_end, "32-bit")
		};
		let base = align_up(*next, size)
			.ok_or_else(|| err(format!("VFIO PCI {label} BAR allocation overflow")))?;
		let allocation_end = base
			.checked_add(size)
			.ok_or_else(|| err(format!("VFIO PCI {label} BAR end overflow")))?;
		if allocation_end > end {
			return Err(err(format!(
				"NVIDIA vGPU {label} BAR of {size:#x} bytes does not fit in the guest PCI aperture"
			)));
		}
		*next = allocation_end;
		Ok(base)
	}

	fn allocate_slot(&mut self) -> Result<u32> {
		if self.slot_next >= KVM_DEVICE_SLOT_END {
			return Err(err("NVIDIA vGPU exhausted KVM device memory slots"));
		}
		let slot = self.slot_next;
		self.slot_next += 1;
		Ok(slot)
	}
}

#[derive(Clone)]
struct PciBar {
	index:      u8,
	region:     u32,
	size:       u64,
	address:    u64,
	flags:      u32,
	is_64bit:   bool,
	probe_low:  bool,
	probe_high: bool,
}

struct BarMapping {
	slot:       u32,
	guest_addr: u64,
	_mapping:   MmapMut,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InterruptMode {
	Disabled,
	Msi { vectors: usize },
	Msix { vectors: usize },
}

#[derive(Clone, Copy)]
struct MsiCapability {
	offset:      usize,
	max_vectors: usize,
	is_64bit:    bool,
}

#[derive(Clone, Copy)]
struct MsixCapability {
	offset:       usize,
	vectors:      usize,
	table_bar:    u8,
	table_offset: u64,
	pba_bar:      u8,
	pba_offset:   u64,
}

#[derive(Clone, Copy, Default)]
struct MsixEntry {
	address: u64,
	data:    u32,
	control: u32,
}

impl MsixEntry {
	fn bytes(self) -> [u8; 16] {
		let mut bytes = [0u8; 16];
		bytes[..8].copy_from_slice(&self.address.to_le_bytes());
		bytes[8..12].copy_from_slice(&self.data.to_le_bytes());
		bytes[12..].copy_from_slice(&self.control.to_le_bytes());
		bytes
	}

	fn from_bytes(bytes: [u8; 16]) -> Self {
		Self {
			address: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
			data:    u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
			control: u32::from_le_bytes(bytes[12..].try_into().unwrap()),
		}
	}
}

struct PciState {
	config:     Vec<u8>,
	bars:       Vec<PciBar>,
	msi:        Option<MsiCapability>,
	msix:       Option<MsixCapability>,
	msix_table: Vec<MsixEntry>,
	pending:    Vec<bool>,
	interrupt:  InterruptMode,
}

struct IrqLine {
	event: EventFd,
}

/// One pre-created NVIDIA SR-IOV vGPU function assigned through VFIO.
pub struct VfioPciDevice {
	device:     VfioDevice,
	vm:         Arc<Vm>,
	state:      Mutex<PciState>,
	irq_lines:  Vec<IrqLine>,
	irq_kill:   EventFd,
	irq_thread: Mutex<Option<JoinHandle<()>>>,
	mappings:   Vec<BarMapping>,
	label:      String,
}

impl VfioPciDevice {
	/// Attach a pre-opened NVIDIA VFIO cdev and allocate its guest PCI BARs.
	fn new(
		device: File,
		sysfs_path: &Path,
		vfio_ops: Arc<dyn VfioOps>,
		vm: Arc<Vm>,
		resources: &mut VfioPciResources,
	) -> Result<Self> {
		let device = VfioDevice::new_from_fd(device, vfio_ops).map_err(|error| {
			err(format!("opening NVIDIA VFIO device {}: {error}", sysfs_path.display()))
		})?;
		device.reset();
		let _ = device.disable_irq(VFIO_PCI_INTX_IRQ_INDEX);

		let config_size = device.get_region_size(VFIO_PCI_CONFIG_REGION_INDEX);
		if config_size < PCI_CONFIG_SPACE_SIZE as u64 {
			return Err(err(format!(
				"NVIDIA VFIO device {} exposes only {config_size} bytes of PCI configuration space",
				sysfs_path.display()
			)));
		}
		let mut config = vec![0u8; PCI_CONFIG_SPACE_SIZE];
		device.region_read(VFIO_PCI_CONFIG_REGION_INDEX, &mut config, 0);
		let vendor = read_u16(&config, 0);
		if vendor != NVIDIA_VENDOR_ID {
			return Err(err(format!(
				"VFIO device {} has PCI vendor {vendor:#06x}; only NVIDIA ({NVIDIA_VENDOR_ID:#06x}) \
				 is supported",
				sysfs_path.display()
			)));
		}

		let bars = build_bars(&device, &mut config, resources)?;
		let msi = parse_msi(&device, &config)?;
		let msix = parse_msix(&device, &config)?;
		if msi.is_none() && msix.is_none() {
			return Err(err(format!(
				"NVIDIA VFIO device {} exposes neither MSI nor MSI-X",
				sysfs_path.display()
			)));
		}
		if let Some(capability) = msi {
			let control = read_u16(&config, capability.offset + 2) & !MSI_CONTROL_ENABLE;
			write_u16(&mut config, capability.offset + 2, control);
		}
		if let Some(capability) = msix {
			let control = read_u16(&config, capability.offset + 2) & !MSIX_CONTROL_ENABLE;
			write_u16(&mut config, capability.offset + 2, control);
		}
		let vector_count = msi
			.map_or(0, |capability| capability.max_vectors)
			.max(msix.map_or(0, |capability| capability.vectors));
		if vector_count > MAX_GPU_VECTORS {
			return Err(err(format!(
				"NVIDIA VFIO device {} requests {vector_count} interrupt vectors; maximum is \
				 {MAX_GPU_VECTORS}",
				sysfs_path.display()
			)));
		}
		let mut irq_lines = Vec::with_capacity(vector_count);
		for _ in 0..vector_count {
			irq_lines.push(IrqLine { event: EventFd::new(EFD_NONBLOCK)? });
		}
		let irq_kill = EventFd::new(EFD_NONBLOCK)?;

		let msix_table = msix.map_or_else(Vec::new, |capability| {
			vec![MsixEntry { control: MSIX_VECTOR_MASKED, ..MsixEntry::default() }; capability.vectors]
		});
		let pending = vec![false; vector_count];
		let label = sysfs_path
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or("nvidia-vgpu")
			.to_owned();
		let mut assigned = Self {
			device,
			vm,
			state: Mutex::new(PciState {
				config,
				bars,
				msi,
				msix,
				msix_table,
				pending,
				interrupt: InterruptMode::Disabled,
			}),
			irq_lines,
			irq_kill,
			irq_thread: Mutex::new(None),
			mappings: Vec::new(),
			label,
		};
		assigned.map_bars(resources)?;
		Ok(assigned)
	}

	fn map_bars(&mut self, resources: &mut VfioPciResources) -> Result<()> {
		let state = self.state.lock();
		for bar in &state.bars {
			let flags = self.device.get_region_flags(bar.region);
			if flags & VFIO_REGION_INFO_FLAG_MMAP == 0 {
				continue;
			}
			for area in mmap_areas(&self.device, bar, state.msix) {
				if area.size == 0 {
					continue;
				}
				let len = usize::try_from(area.size)
					.map_err(|_| err(format!("NVIDIA BAR mapping {} is too large", area.size)))?;
				let file_offset = self
					.device
					.get_region_offset(bar.region)
					.checked_add(area.offset)
					.ok_or_else(|| err("NVIDIA VFIO BAR mmap offset overflow"))?;
				// SAFETY: VFIO advertises this page-aligned subrange as
				// mmap-capable; the returned mapping is retained until its KVM
				// memory slot is removed.
				let mut mapping = unsafe {
					MmapOptions::new()
						.offset(file_offset)
						.len(len)
						.map_mut(&self.device)
				}
				.map_err(|error| {
					err(format!(
						"mapping NVIDIA VFIO BAR{} at offset {:#x}: {error}",
						bar.index, area.offset
					))
				})?;
				let guest_addr = bar
					.address
					.checked_add(area.offset)
					.ok_or_else(|| err("NVIDIA VFIO guest BAR address overflow"))?;
				let slot = resources.allocate_slot()?;
				let region = kvm_userspace_memory_region {
					slot,
					guest_phys_addr: guest_addr,
					memory_size: area.size,
					userspace_addr: mapping.as_mut_ptr() as u64,
					flags: 0,
				};
				// SAFETY: `mapping` owns `area.size` bytes at userspace_addr and is
				// kept in `self.mappings` until this slot is explicitly removed.
				unsafe { self.vm.fd.set_user_memory_region(region)? };
				self
					.mappings
					.push(BarMapping { slot, guest_addr, _mapping: mapping });
			}
		}
		Ok(())
	}

	/// Start the MSI delivery thread for this device.
	pub fn start_interrupts(self: &Arc<Self>) -> Result<()> {
		let mut thread = self.irq_thread.lock();
		if thread.is_some() {
			return Ok(());
		}
		let events = self
			.irq_lines
			.iter()
			.map(|line| line.event.try_clone())
			.collect::<io::Result<Vec<_>>>()?;
		let kill = self.irq_kill.try_clone()?;
		let device = Arc::downgrade(self);
		let name = format!("vfio-{}", self.label);
		*thread = Some(
			thread::Builder::new()
				.name(name)
				.spawn(move || interrupt_loop(device, events, kill))?,
		);
		Ok(())
	}

	/// Disable VFIO interrupts and join the MSI delivery thread.
	pub fn stop_interrupts(&self) -> Result<()> {
		let mut first_error = {
			let mut state = self.state.lock();
			let error = self.disable_interrupts(state.interrupt).err();
			state.interrupt = InterruptMode::Disabled;
			error
		};
		let _ = self.irq_kill.write(1);
		if let Some(thread) = self.irq_thread.lock().take()
			&& thread.join().is_err()
			&& first_error.is_none()
		{
			first_error = Some(err("NVIDIA VFIO interrupt thread panicked"));
		}
		first_error.map_or(Ok(()), Err)
	}

	fn handle_interrupt(&self, vector: usize) {
		let mut state = self.state.lock();
		let Some(message) = interrupt_message(&mut state, vector) else {
			return;
		};
		if let Err(cause) = self.vm.fd.signal_msi(message) {
			error!(gpu = %self.label, vector, error = %cause, "failed to inject NVIDIA vGPU MSI");
		}
	}

	fn update_interrupts(&self, state: &mut PciState) -> Result<()> {
		let desired = desired_interrupt_mode(state);
		if desired == state.interrupt {
			return Ok(());
		}
		self.disable_interrupts(state.interrupt)?;
		match desired {
			InterruptMode::Disabled => {},
			InterruptMode::Msi { vectors } => {
				let eventfds = self.irq_lines[..vectors]
					.iter()
					.map(|line| &line.event)
					.collect();
				self.device.enable_msi(eventfds)?;
			},
			InterruptMode::Msix { vectors } => {
				let eventfds = self.irq_lines[..vectors]
					.iter()
					.map(|line| &line.event)
					.collect();
				self.device.enable_msix(eventfds)?;
			},
		}
		state.interrupt = desired;
		Ok(())
	}

	fn disable_interrupts(&self, mode: InterruptMode) -> Result<()> {
		match mode {
			InterruptMode::Disabled => Ok(()),
			InterruptMode::Msi { .. } => self.device.disable_msi().map_err(Into::into),
			InterruptMode::Msix { .. } => self.device.disable_msix().map_err(Into::into),
		}
	}

	fn flush_pending(&self, state: &mut PciState) {
		if !matches!(state.interrupt, InterruptMode::Msix { .. }) {
			return;
		}
		for vector in 0..state.pending.len() {
			if !state.pending[vector] {
				continue;
			}
			let Some(message) = interrupt_message(state, vector) else {
				continue;
			};
			state.pending[vector] = false;
			if let Err(cause) = self.vm.fd.signal_msi(message) {
				state.pending[vector] = true;
				error!(gpu = %self.label, vector, error = %cause, "failed to flush NVIDIA vGPU MSI");
			}
		}
	}
}

impl PciFunction for VfioPciDevice {
	fn read_config(&self, offset: usize, data: &mut [u8]) {
		data.fill(0xff);
		let state = self.state.lock();
		let mut config = state.config.clone();
		for bar in &state.bars {
			if bar.probe_low {
				write_u32(&mut config, PCI_BAR0 + usize::from(bar.index) * 4, bar_mask_low(bar));
			}
			if bar.is_64bit && bar.probe_high {
				write_u32(&mut config, PCI_BAR0 + (usize::from(bar.index) + 1) * 4, bar_mask_high(bar));
			}
		}
		copy_from(&config, offset, data, 0xff);
	}

	fn write_config(&self, offset: usize, data: &[u8]) {
		if data.is_empty() || offset >= PCI_CONFIG_SPACE_SIZE {
			return;
		}
		let mut state = self.state.lock();
		let old_config = state.config.clone();
		overlay(&mut state.config, offset, data);

		let mut virtualized = false;
		let mut bars = state.bars.clone();
		for bar in &mut bars {
			let low = PCI_BAR0 + usize::from(bar.index) * 4;
			if touches(offset, data.len(), low, 4) {
				bar.probe_low = read_u32(&state.config, low) == u32::MAX;
				virtualized = true;
			}
			if bar.is_64bit {
				let high = low + 4;
				if touches(offset, data.len(), high, 4) {
					bar.probe_high = read_u32(&state.config, high) == u32::MAX;
					virtualized = true;
				}
			}
			write_assigned_bar(&mut state.config, bar);
		}
		state.bars = bars;

		if let Some(msi) = state.msi
			&& touches(offset, data.len(), msi.offset, msi_size(msi))
		{
			virtualized = true;
			state.config[msi.offset] = old_config[msi.offset];
			state.config[msi.offset + 1] = old_config[msi.offset + 1];
			let old_control = read_u16(&old_config, msi.offset + 2);
			let requested = read_u16(&state.config, msi.offset + 2);
			let writable = requested & (MSI_CONTROL_ENABLE | MSI_CONTROL_MME_MASK);
			let readonly = old_control & !(MSI_CONTROL_ENABLE | MSI_CONTROL_MME_MASK);
			write_u16(&mut state.config, msi.offset + 2, readonly | writable);
		}
		if let Some(msix) = state.msix
			&& touches(offset, data.len(), msix.offset, 12)
		{
			virtualized = true;
			state.config[msix.offset] = old_config[msix.offset];
			state.config[msix.offset + 1] = old_config[msix.offset + 1];
			let old_control = read_u16(&old_config, msix.offset + 2);
			let requested = read_u16(&state.config, msix.offset + 2);
			let writable = requested & (MSIX_CONTROL_ENABLE | MSIX_CONTROL_FUNCTION_MASK);
			let readonly = old_control & MSIX_CONTROL_TABLE_SIZE;
			write_u16(&mut state.config, msix.offset + 2, readonly | writable);
			state.config[msix.offset + 4..msix.offset + 12]
				.copy_from_slice(&old_config[msix.offset + 4..msix.offset + 12]);
		}

		if !virtualized {
			self
				.device
				.region_write(VFIO_PCI_CONFIG_REGION_INDEX, data, offset as u64);
			let end = offset.saturating_add(data.len()).min(PCI_CONFIG_SPACE_SIZE);
			self.device.region_read(
				VFIO_PCI_CONFIG_REGION_INDEX,
				&mut state.config[offset..end],
				offset as u64,
			);
		}
		if let Err(cause) = self.update_interrupts(&mut state) {
			error!(gpu = %self.label, error = %cause, "failed to configure NVIDIA vGPU interrupts");
			disable_interrupt_bits(&mut state);
			state.interrupt = InterruptMode::Disabled;
		}
		self.flush_pending(&mut state);
	}

	fn read_bar(&self, addr: u64, data: &mut [u8]) -> bool {
		let state = self.state.lock();
		let Some((bar, offset)) = find_bar(&state, addr, data.len()) else {
			return false;
		};
		if let Some(msix) = state.msix {
			if bar.index == msix.table_bar
				&& range_contains(msix.table_offset, msix.vectors as u64 * 16, offset, data.len())
			{
				read_msix_table(&state.msix_table, offset - msix.table_offset, data);
				return true;
			}
			let pba_size = msix.vectors.div_ceil(64) as u64 * 8;
			if bar.index == msix.pba_bar
				&& range_contains(msix.pba_offset, pba_size, offset, data.len())
			{
				read_pending(&state.pending, offset - msix.pba_offset, data);
				return true;
			}
		}
		self.device.region_read(bar.region, data, offset);
		true
	}

	fn write_bar(&self, addr: u64, data: &[u8]) -> bool {
		let mut state = self.state.lock();
		let Some((bar, offset)) = find_bar(&state, addr, data.len()) else {
			return false;
		};
		if let Some(msix) = state.msix {
			if bar.index == msix.table_bar
				&& range_contains(msix.table_offset, msix.vectors as u64 * 16, offset, data.len())
			{
				write_msix_table(&mut state.msix_table, offset - msix.table_offset, data);
				self.flush_pending(&mut state);
				return true;
			}
			let pba_size = msix.vectors.div_ceil(64) as u64 * 8;
			if bar.index == msix.pba_bar
				&& range_contains(msix.pba_offset, pba_size, offset, data.len())
			{
				return true;
			}
		}
		self.device.region_write(bar.region, data, offset);
		true
	}
}

impl Drop for VfioPciDevice {
	fn drop(&mut self) {
		if let Err(cause) = self.stop_interrupts() {
			warn!(gpu = %self.label, error = %cause, "failed to stop NVIDIA vGPU interrupts");
		}
		for mapping in self.mappings.iter().rev() {
			let region = kvm_userspace_memory_region {
				slot:            mapping.slot,
				guest_phys_addr: mapping.guest_addr,
				memory_size:     0,
				userspace_addr:  0,
				flags:           0,
			};
			// SAFETY: a zero-sized KVM region removes the slot before its mmap is
			// dropped.
			if let Err(cause) = unsafe { self.vm.fd.set_user_memory_region(region) } {
				warn!(gpu = %self.label, slot = mapping.slot, error = %cause, "failed to remove NVIDIA BAR memory slot");
			}
		}
		self.device.reset();
	}
}

/// Canonicalize and validate pre-created NVIDIA SR-IOV vGPU sysfs paths.
pub fn normalize_gpu_paths(paths: &mut [PathBuf]) -> Result<()> {
	if paths.len() > MAX_VFIO_GPUS {
		return Err(err(format!(
			"at most {MAX_VFIO_GPUS} NVIDIA vGPU functions can be assigned (got {})",
			paths.len()
		)));
	}
	let mut normalized = Vec::with_capacity(paths.len());
	for path in paths.iter() {
		if !path.is_absolute() || !path.starts_with("/sys/bus/pci/devices") {
			return Err(err(format!(
				"--vfio-gpu must name an absolute SR-IOV VF under /sys/bus/pci/devices (got {})",
				path.display()
			)));
		}
		let bdf = path
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| err(format!("invalid NVIDIA vGPU sysfs path {}", path.display())))?;
		validate_bdf(bdf)?;
		let canonical = fs::canonicalize(path)
			.map_err(|cause| err(format!("resolving NVIDIA vGPU {}: {cause}", path.display())))?;
		if fs::symlink_metadata(canonical.join("physfn")).is_err() {
			return Err(err(format!(
				"NVIDIA vGPU {} is not an SR-IOV virtual function (missing physfn)",
				path.display()
			)));
		}
		let vendor = read_sysfs_trimmed(&canonical.join("vendor"))?;
		if vendor != "0x10de" {
			return Err(err(format!(
				"VF {} has vendor {vendor}; only NVIDIA SR-IOV vGPU functions are supported",
				path.display()
			)));
		}
		let current_type = read_sysfs_trimmed(&canonical.join("nvidia/current_vgpu_type"))?;
		if current_type == "0" || current_type.parse::<u32>().is_err() {
			return Err(err(format!(
				"NVIDIA VF {} has no pre-created vGPU profile (current_vgpu_type={current_type:?})",
				path.display()
			)));
		}
		if normalized.contains(&canonical) {
			return Err(err(format!("NVIDIA vGPU {} was specified more than once", path.display())));
		}
		normalized.push(canonical);
	}
	paths.clone_from_slice(&normalized);
	Ok(())
}

/// Resolve the vendor VFIO character device associated with one NVIDIA virtual
/// function.
pub fn gpu_cdev_path(sysfs_path: &Path) -> Result<PathBuf> {
	let vfio_dev = sysfs_path.join("vfio-dev");
	let entries = fs::read_dir(&vfio_dev)
		.map_err(|cause| err(format!("reading {}: {cause}", vfio_dev.display())))?
		.collect::<io::Result<Vec<_>>>()?;
	if entries.len() != 1 || !entries[0].file_type()?.is_dir() {
		return Err(err(format!(
			"{} must contain exactly one VFIO cdev directory",
			vfio_dev.display()
		)));
	}
	Ok(Path::new("/dev/vfio/devices").join(entries[0].file_name()))
}

/// Open iommufd and all NVIDIA VFIO cdevs before the sandbox drops privileges.
pub fn preopen_gpu_devices(paths: &[PathBuf]) -> Result<Option<PreopenedVfio>> {
	if paths.is_empty() {
		return Ok(None);
	}
	let iommufd = Arc::new(
		IommuFd::new()
			.map_err(|cause| err(format!("opening /dev/iommu for NVIDIA vGPU: {cause}")))?,
	);
	let mut devices = Vec::with_capacity(paths.len());
	for sysfs_path in paths {
		let cdev = gpu_cdev_path(sysfs_path)?;
		let device = OpenOptions::new()
			.read(true)
			.write(true)
			.open(&cdev)
			.map_err(|cause| err(format!("opening NVIDIA VFIO cdev {}: {cause}", cdev.display())))?;
		devices.push(PreopenedGpu { sysfs_path: sysfs_path.clone(), device });
	}
	Ok(Some(PreopenedVfio { iommufd, devices }))
}

/// Attach every pre-opened NVIDIA vGPU and map guest RAM into their shared
/// IOAS.
pub fn attach_gpu_devices(
	preopened: PreopenedVfio,
	vm: Arc<Vm>,
	resources: &mut VfioPciResources,
) -> Result<Vec<Arc<VfioPciDevice>>> {
	let kvm_device = Arc::new(vm.create_vfio_device()?);
	let vfio_ops: Arc<dyn VfioOps> = Arc::new(
		VfioIommufd::new(preopened.iommufd, None, Some(kvm_device))
			.map_err(|cause| err(format!("creating NVIDIA VFIO IOAS: {cause}")))?,
	);
	for region in vm.memory().iter() {
		// SAFETY: guest memory is mmap-backed for the VM lifetime. Device DMA and
		// VMM accesses use volatile/atomic guest-memory operations until IOAS
		// drop.
		unsafe {
			vfio_ops.vfio_dma_map(
				region.start_addr().raw_value(),
				region.len() as usize,
				region.as_ptr(),
			)
		}
		.map_err(|cause| {
			err(format!(
				"mapping guest RAM {:#x}+{:#x} for NVIDIA vGPU DMA: {cause}",
				region.start_addr().raw_value(),
				region.len()
			))
		})?;
	}

	let mut devices = Vec::with_capacity(preopened.devices.len());
	for gpu in preopened.devices {
		devices.push(Arc::new(VfioPciDevice::new(
			gpu.device,
			&gpu.sysfs_path,
			vfio_ops.clone(),
			vm.clone(),
			resources,
		)?));
	}
	Ok(devices)
}

fn build_bars(
	device: &VfioDevice,
	config: &mut [u8],
	resources: &mut VfioPciResources,
) -> Result<Vec<PciBar>> {
	let mut bars = Vec::new();
	let mut index = 0u8;
	while index < 6 {
		let region = VFIO_PCI_BAR0_REGION_INDEX + u32::from(index);
		let size = device.get_region_size(region);
		if size == 0 {
			index += 1;
			continue;
		}
		let register = PCI_BAR0 + usize::from(index) * 4;
		let original = read_u32(config, register);
		if original & 1 != 0 {
			return Err(err(format!(
				"NVIDIA VFIO BAR{index} is port I/O; only MMIO BARs are supported"
			)));
		}
		let is_64bit = original & 0x6 == 0x4;
		if is_64bit && index == 5 {
			return Err(err("NVIDIA VFIO BAR5 claims an invalid 64-bit BAR pair"));
		}
		let address = resources.allocate_bar(size, is_64bit)?;
		let bar = PciBar {
			index,
			region,
			size,
			address,
			flags: original & 0xf,
			is_64bit,
			probe_low: false,
			probe_high: false,
		};
		write_assigned_bar(config, &bar);
		bars.push(bar);
		index += if is_64bit { 2 } else { 1 };
	}
	Ok(bars)
}

fn parse_msi(device: &VfioDevice, config: &[u8]) -> Result<Option<MsiCapability>> {
	let Some(offset) = find_capability(config, PCI_CAP_ID_MSI) else {
		return Ok(None);
	};
	let control = read_u16(config, offset + 2);
	let advertised = 1usize << usize::from((control & MSI_CONTROL_MMC_MASK) >> 1);
	let available = device
		.get_irq_info(VFIO_PCI_MSI_IRQ_INDEX)
		.map_or(0, |irq| irq.count as usize);
	if available == 0 {
		return Ok(None);
	}
	if available < advertised {
		return Err(err(format!(
			"NVIDIA VFIO MSI exposes {available} vectors but PCI config advertises {advertised}"
		)));
	}
	Ok(Some(MsiCapability {
		offset,
		max_vectors: advertised,
		is_64bit: control & MSI_CONTROL_64BIT != 0,
	}))
}

fn parse_msix(device: &VfioDevice, config: &[u8]) -> Result<Option<MsixCapability>> {
	let Some(offset) = find_capability(config, PCI_CAP_ID_MSIX) else {
		return Ok(None);
	};
	let control = read_u16(config, offset + 2);
	let advertised = usize::from(control & MSIX_CONTROL_TABLE_SIZE) + 1;
	let available = device
		.get_irq_info(VFIO_PCI_MSIX_IRQ_INDEX)
		.map_or(0, |irq| irq.count as usize);
	if available == 0 {
		return Ok(None);
	}
	if available < advertised {
		return Err(err(format!(
			"NVIDIA VFIO MSI-X exposes {available} vectors but PCI config advertises {advertised}"
		)));
	}
	let table = read_u32(config, offset + 4);
	let pba = read_u32(config, offset + 8);
	let table_bar = (table & 0x7) as u8;
	let table_offset = u64::from(table & !0x7);
	let pba_bar = (pba & 0x7) as u8;
	let pba_offset = u64::from(pba & !0x7);
	validate_msix_range(device, "table", table_bar, table_offset, u64::try_from(advertised)? * 16)?;
	validate_msix_range(
		device,
		"PBA",
		pba_bar,
		pba_offset,
		u64::try_from(advertised.div_ceil(64))? * 8,
	)?;
	Ok(Some(MsixCapability {
		offset,
		vectors: advertised,
		table_bar,
		table_offset,
		pba_bar,
		pba_offset,
	}))
}

fn validate_msix_range(
	device: &VfioDevice,
	label: &str,
	bar: u8,
	offset: u64,
	size: u64,
) -> Result<()> {
	if bar >= 6 {
		return Err(err(format!("NVIDIA MSI-X {label} uses invalid BAR{bar}")));
	}
	let bar_size = device.get_region_size(VFIO_PCI_BAR0_REGION_INDEX + u32::from(bar));
	let end = offset
		.checked_add(size)
		.ok_or_else(|| err(format!("NVIDIA MSI-X {label} range overflows")))?;
	if bar_size == 0 || end > bar_size {
		return Err(err(format!(
			"NVIDIA MSI-X {label} range {offset:#x}..{end:#x} exceeds BAR{bar} size {bar_size:#x}"
		)));
	}
	Ok(())
}

fn mmap_areas(
	device: &VfioDevice,
	bar: &PciBar,
	msix: Option<MsixCapability>,
) -> Vec<VfioRegionSparseMmapArea> {
	let mut areas = device
		.get_region_caps(bar.region)
		.into_iter()
		.find_map(|capability| match capability {
			VfioRegionInfoCap::SparseMmap(sparse) => Some(sparse.areas),
			_ => None,
		})
		.unwrap_or_else(|| vec![VfioRegionSparseMmapArea { offset: 0, size: bar.size }]);
	let Some(msix) = msix else {
		return areas;
	};
	let page_size = host_page_size();
	let mut traps = Vec::new();
	if bar.index == msix.table_bar {
		traps.push((
			align_down(msix.table_offset, page_size),
			align_up(msix.table_offset + msix.vectors as u64 * 16, page_size).unwrap_or(bar.size),
		));
	}
	if bar.index == msix.pba_bar {
		let pba_size = msix.vectors.div_ceil(64) as u64 * 8;
		traps.push((
			align_down(msix.pba_offset, page_size),
			align_up(msix.pba_offset + pba_size, page_size).unwrap_or(bar.size),
		));
	}
	for trap in traps {
		areas = areas
			.into_iter()
			.flat_map(|area| subtract_area(area, trap.0, trap.1))
			.collect();
	}
	areas
}

fn subtract_area(
	area: VfioRegionSparseMmapArea,
	trap_start: u64,
	trap_end: u64,
) -> Vec<VfioRegionSparseMmapArea> {
	let area_end = area.offset.saturating_add(area.size);
	if trap_end <= area.offset || trap_start >= area_end {
		return vec![area];
	}
	let mut result = Vec::with_capacity(2);
	if trap_start > area.offset {
		result
			.push(VfioRegionSparseMmapArea { offset: area.offset, size: trap_start - area.offset });
	}
	if trap_end < area_end {
		result.push(VfioRegionSparseMmapArea { offset: trap_end, size: area_end - trap_end });
	}
	result
}

fn interrupt_loop(device: Weak<VfioPciDevice>, events: Vec<EventFd>, kill: EventFd) {
	let mut fds = events
		.iter()
		.map(|event| libc::pollfd { fd: event.as_raw_fd(), events: libc::POLLIN, revents: 0 })
		.collect::<Vec<_>>();
	let kill_index = fds.len();
	fds.push(libc::pollfd { fd: kill.as_raw_fd(), events: libc::POLLIN, revents: 0 });
	loop {
		// SAFETY: `fds` owns initialized pollfd records for the call duration.
		let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
		if result < 0 {
			let cause = io::Error::last_os_error();
			if cause.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			error!(error = %cause, "NVIDIA vGPU interrupt poll failed");
			return;
		}
		if fds[kill_index].revents & libc::POLLIN != 0 {
			let _ = kill.read();
			return;
		}
		for (vector, event) in events.iter().enumerate() {
			let revents = fds[vector].revents;
			if revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
				error!(vector, revents, "NVIDIA vGPU interrupt eventfd failed");
				return;
			}
			if revents & libc::POLLIN == 0 {
				continue;
			}
			let count = match event.read() {
				Ok(count) => count,
				Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => continue,
				Err(cause) => {
					error!(vector, error = %cause, "reading NVIDIA vGPU interrupt failed");
					return;
				},
			};
			let Some(device) = device.upgrade() else {
				return;
			};
			for _ in 0..count {
				device.handle_interrupt(vector);
			}
		}
	}
}

fn desired_interrupt_mode(state: &PciState) -> InterruptMode {
	if let Some(msix) = state.msix {
		let control = read_u16(&state.config, msix.offset + 2);
		if control & MSIX_CONTROL_ENABLE != 0 {
			return InterruptMode::Msix { vectors: msix.vectors };
		}
	}
	if let Some(msi) = state.msi {
		let control = read_u16(&state.config, msi.offset + 2);
		if control & MSI_CONTROL_ENABLE != 0 {
			let requested = 1usize << usize::from((control & MSI_CONTROL_MME_MASK) >> 4);
			return InterruptMode::Msi { vectors: requested.min(msi.max_vectors) };
		}
	}
	InterruptMode::Disabled
}

fn interrupt_message(state: &mut PciState, vector: usize) -> Option<kvm_msi> {
	match state.interrupt {
		InterruptMode::Disabled => None,
		InterruptMode::Msi { vectors } => {
			if vector >= vectors {
				return None;
			}
			let capability = state.msi?;
			let address_low = read_u32(&state.config, capability.offset + 4);
			let (address_high, data_offset) = if capability.is_64bit {
				(read_u32(&state.config, capability.offset + 8), capability.offset + 12)
			} else {
				(0, capability.offset + 8)
			};
			let base_data = u32::from(read_u16(&state.config, data_offset));
			let vector_mask = u32::try_from(vectors - 1).ok()?;
			Some(kvm_msi {
				address_lo: address_low,
				address_hi: address_high,
				data: (base_data & !vector_mask) | u32::try_from(vector).ok()?,
				..Default::default()
			})
		},
		InterruptMode::Msix { vectors } => {
			if vector >= vectors {
				return None;
			}
			let capability = state.msix?;
			let control = read_u16(&state.config, capability.offset + 2);
			let entry = state.msix_table[vector];
			if control & MSIX_CONTROL_FUNCTION_MASK != 0 || entry.control & MSIX_VECTOR_MASKED != 0 {
				state.pending[vector] = true;
				return None;
			}
			Some(kvm_msi {
				address_lo: entry.address as u32,
				address_hi: (entry.address >> 32) as u32,
				data: entry.data,
				..Default::default()
			})
		},
	}
}

fn disable_interrupt_bits(state: &mut PciState) {
	if let Some(msi) = state.msi {
		let control = read_u16(&state.config, msi.offset + 2) & !MSI_CONTROL_ENABLE;
		write_u16(&mut state.config, msi.offset + 2, control);
	}
	if let Some(msix) = state.msix {
		let control = read_u16(&state.config, msix.offset + 2) & !MSIX_CONTROL_ENABLE;
		write_u16(&mut state.config, msix.offset + 2, control);
	}
}

fn find_bar(state: &PciState, addr: u64, len: usize) -> Option<(PciBar, u64)> {
	if read_u16(&state.config, PCI_COMMAND) & PCI_COMMAND_MEMORY == 0 {
		return None;
	}
	let len = u64::try_from(len).ok()?;
	state.bars.iter().find_map(|bar| {
		let offset = addr.checked_sub(bar.address)?;
		(offset < bar.size && len <= bar.size - offset).then_some((bar.clone(), offset))
	})
}

fn write_assigned_bar(config: &mut [u8], bar: &PciBar) {
	let register = PCI_BAR0 + usize::from(bar.index) * 4;
	write_u32(config, register, (bar.address as u32 & 0xffff_fff0) | bar.flags);
	if bar.is_64bit {
		write_u32(config, register + 4, (bar.address >> 32) as u32);
	}
}

const fn bar_mask_low(bar: &PciBar) -> u32 {
	(!(bar.size - 1) as u32 & 0xffff_fff0) | bar.flags
}

const fn bar_mask_high(bar: &PciBar) -> u32 {
	(!(bar.size - 1) >> 32) as u32
}

fn find_capability(config: &[u8], id: u8) -> Option<usize> {
	if read_u16(config, PCI_STATUS) & PCI_STATUS_CAP_LIST == 0 {
		return None;
	}
	let mut offset = usize::from(config[PCI_CAPABILITY_LIST] & !0x3);
	let mut visited = [false; 256];
	for _ in 0..48 {
		if !(0x40..=0xfc).contains(&offset) || visited[offset] {
			return None;
		}
		visited[offset] = true;
		if config[offset] == id {
			return Some(offset);
		}
		offset = usize::from(config[offset + 1] & !0x3);
		if offset == 0 {
			return None;
		}
	}
	None
}

const fn msi_size(capability: MsiCapability) -> usize {
	if capability.is_64bit { 14 } else { 10 }
}

fn read_msix_table(table: &[MsixEntry], offset: u64, data: &mut [u8]) {
	data.fill(0);
	let start = offset as usize;
	let table_size = table.len() * 16;
	for (output, byte) in data.iter_mut().zip(start..table_size) {
		*output = table[byte / 16].bytes()[byte % 16];
	}
}

fn write_msix_table(table: &mut [MsixEntry], offset: u64, data: &[u8]) {
	let start = offset as usize;
	let table_size = table.len() * 16;
	if start >= table_size || data.is_empty() {
		return;
	}
	let end = start.saturating_add(data.len()).min(table_size);
	for (entry_index, entry) in table
		.iter_mut()
		.enumerate()
		.take((end - 1) / 16 + 1)
		.skip(start / 16)
	{
		let entry_start = entry_index * 16;
		let copy_start = start.max(entry_start);
		let copy_end = end.min(entry_start + 16);
		let mut bytes = entry.bytes();
		bytes[copy_start - entry_start..copy_end - entry_start]
			.copy_from_slice(&data[copy_start - start..copy_end - start]);
		*entry = MsixEntry::from_bytes(bytes);
	}
}

fn read_pending(pending: &[bool], offset: u64, data: &mut [u8]) {
	data.fill(0);
	let start_bit = offset.saturating_mul(8) as usize;
	for (byte_index, byte) in data.iter_mut().enumerate() {
		for bit in 0..8 {
			let vector = start_bit + byte_index * 8 + bit;
			if pending.get(vector).copied().unwrap_or(false) {
				*byte |= 1 << bit;
			}
		}
	}
}

const fn range_contains(base: u64, size: u64, access: u64, len: usize) -> bool {
	let Some(access_end) = access.checked_add(len as u64) else {
		return false;
	};
	let Some(end) = base.checked_add(size) else {
		return false;
	};
	access >= base && access_end <= end
}

fn validate_bdf(value: &str) -> Result<()> {
	let Some((domain, rest)) = value.split_once(':') else {
		return Err(err(format!("invalid PCI BDF {value:?}")));
	};
	let Some((bus, rest)) = rest.split_once(':') else {
		return Err(err(format!("invalid PCI BDF {value:?}")));
	};
	let Some((slot, function)) = rest.split_once('.') else {
		return Err(err(format!("invalid PCI BDF {value:?}")));
	};
	for (part, width) in [(domain, 4), (bus, 2), (slot, 2), (function, 1)] {
		if part.len() != width || !part.bytes().all(|byte| byte.is_ascii_hexdigit()) {
			return Err(err(format!("invalid PCI BDF {value:?}")));
		}
	}
	if u8::from_str_radix(slot, 16).unwrap() >= 32 || u8::from_str_radix(function, 16).unwrap() >= 8
	{
		return Err(err(format!("invalid PCI BDF {value:?}")));
	}
	Ok(())
}

fn read_sysfs_trimmed(path: &Path) -> Result<String> {
	fs::read_to_string(path)
		.map(|value| value.trim().to_owned())
		.map_err(|cause| err(format!("reading {}: {cause}", path.display())))
}

fn host_page_size() -> u64 {
	// SAFETY: `_SC_PAGESIZE` has no preconditions and does not mutate process
	// state.
	let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
	if size > 0 { size as u64 } else { 4096 }
}

const fn align_down(value: u64, alignment: u64) -> u64 {
	value & !(alignment - 1)
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
	value
		.checked_add(alignment - 1)
		.map(|value| value & !(alignment - 1))
}

const fn touches(offset: usize, len: usize, field: usize, field_len: usize) -> bool {
	offset < field + field_len && field < offset + len
}

fn copy_from(source: &[u8], offset: usize, data: &mut [u8], fill: u8) {
	data.fill(fill);
	let Some(source) = source.get(offset..) else {
		return;
	};
	let len = source.len().min(data.len());
	data[..len].copy_from_slice(&source[..len]);
}

fn overlay(destination: &mut [u8], offset: usize, data: &[u8]) {
	let Some(destination) = destination.get_mut(offset..) else {
		return;
	};
	let len = destination.len().min(data.len());
	destination[..len].copy_from_slice(&data[..len]);
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
	u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
	u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
	bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
	bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn allocates_aligned_nonoverlapping_gpu_bar_apertures() {
		let mut resources =
			VfioPciResources::new(0xc080_0000, 0xe000_0000).expect("resource allocator");
		assert_eq!(
			resources
				.allocate_bar(0x0100_0000, false)
				.expect("first low BAR"),
			0xc100_0000
		);
		assert_eq!(
			resources
				.allocate_bar(0x0200_0000, false)
				.expect("second low BAR"),
			0xc200_0000
		);
		assert_eq!(resources.allocate_bar(0x8000_0000, true).expect("high BAR"), PCI_HIGH_MMIO_START);
		assert!(resources.allocate_bar(3, false).is_err());

		let mut exhausted =
			VfioPciResources::new(0xdfff_f000, 0xe000_0000).expect("bounded allocator");
		assert!(exhausted.allocate_bar(0x2000, false).is_err());
	}

	#[test]
	fn preserves_masked_msix_until_the_guest_unmasks_its_vector() {
		let capability = MsixCapability {
			offset:       0x50,
			vectors:      1,
			table_bar:    0,
			table_offset: 0,
			pba_bar:      0,
			pba_offset:   0x1000,
		};
		let mut config = vec![0; PCI_CONFIG_SPACE_SIZE];
		write_u16(&mut config, capability.offset + 2, MSIX_CONTROL_ENABLE);
		let mut state = PciState {
			config,
			bars: Vec::new(),
			msi: None,
			msix: Some(capability),
			msix_table: vec![MsixEntry {
				address: 0x0000_0000_fee0_0000,
				data:    0x41,
				control: MSIX_VECTOR_MASKED,
			}],
			pending: vec![false],
			interrupt: InterruptMode::Msix { vectors: 1 },
		};

		assert!(interrupt_message(&mut state, 0).is_none());
		assert!(state.pending[0]);
		state.msix_table[0].control = 0;
		let message = interrupt_message(&mut state, 0).expect("unmasked MSI-X message");
		assert_eq!(message.address_lo, 0xfee0_0000);
		assert_eq!(message.address_hi, 0);
		assert_eq!(message.data, 0x41);
	}

	#[test]
	fn excludes_trapped_msix_pages_from_sparse_bar_mappings() {
		let area = VfioRegionSparseMmapArea { offset: 0, size: 0x4000 };
		assert_eq!(subtract_area(area, 0x1000, 0x3000), [
			VfioRegionSparseMmapArea { offset: 0, size: 0x1000 },
			VfioRegionSparseMmapArea { offset: 0x3000, size: 0x1000 },
		]);
	}

	#[test]
	fn accepts_only_canonical_pci_function_addresses() {
		assert!(validate_bdf("0000:3d:00.4").is_ok());
		for invalid in ["3d:00.4", "0000:3d:20.0", "0000:3d:00.8", "0000:zz:00.0"] {
			assert!(validate_bdf(invalid).is_err(), "{invalid} should be rejected");
		}
	}
}
