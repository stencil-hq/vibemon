//! NVIDIA GPU assignment through the VFIO cdev API (`--vfio-gpu`).
//!
//! Two kinds of PCI function are accepted:
//!
//! - **Whole-GPU passthrough:** an NVIDIA display-class function bound to
//!   `vfio-pci` whose IOMMU group is viable (every other member is bound to
//!   `vfio-pci`, unbound, or a `pcieport` bridge). Companion functions such as
//!   the HDMI audio controller are never opened and are not presented to the
//!   guest.
//! - **SR-IOV vGPU:** a virtual function with a vGPU profile pre-created by the
//!   host NVIDIA vGPU manager.
//!
//! The VMM opens the VFIO cdev before dropping privileges, maps guest RAM
//! through iommufd, and presents the function as `00:<slot>.0` on the guest
//! PCI bus. Direct boot has no firmware, so the VMM assigns BARs itself: 32-bit
//! memory BARs below 4 GiB, 64-bit BARs in a dedicated aperture at
//! [`PCI_HIGH_MMIO_START`]. Configuration space is partly emulated:
//!
//! - BARs are fixed at their assigned addresses (sizing probes are emulated);
//!   I/O-port BARs and the expansion ROM read as absent.
//! - The interrupt pin reads as zero: guests must use MSI or MSI-X, which the
//!   VMM emulates and relays through KVM. `INTx` is never enabled.
//! - The Power Management capability is unlinked so guests cannot move the
//!   function into D3 (vfio-pci revokes BAR mappings in low-power states).
//! - The SR-IOV, ARI, Resizable BAR, VF Resizable BAR, and AER extended
//!   capabilities are unlinked: they control host-owned state (VF creation, BAR
//!   sizes that must match the fixed guest layout, host error handling).
//! - Everything else is forwarded to the VFIO config region, where vfio-pci
//!   applies its own write filtering.
//!
//! Reset: the function is reset through `VFIO_DEVICE_RESET` on attach and
//! detach when vfio-pci reports a function-level reset method (FLR, PM, or a
//! slot/bus reset that affects only this function). Otherwise vfio-pci itself
//! performs a bus reset once every function of the bus/slot is closed, which
//! holds because companion functions are never opened here.
//!
//! Host setup for whole GPUs: load vfio-pci with `disable_idle_d3=1`. Without
//! it vfio-pci moves an idle GPU to D3hot between VMs, and the D3hot→D0 + FLR
//! transitions around open/close have hard-reset a host with a `GeForce` RTX
//! 5070 (Blackwell). Blackwell's FLR also does not reset GSP firmware state:
//! the guest must unload nvidia.ko before the VM exits, or the next VM cannot
//! initialize the GPU ("WPR2 already up"). Never bus-reset such a GPU from the
//! host; it can drop off the bus until a power cycle.

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
/// PCI base class of display controllers (VGA, XGA, 3D).
const PCI_BASE_CLASS_DISPLAY: u32 = 0x03;
/// PCI class code (base + subclass) of PCI-to-PCI bridges.
const PCI_CLASS_BRIDGE_PCI: u32 = 0x0604;
const PCI_CONFIG_SPACE_SIZE: usize = 4096;
const PCI_LEGACY_CONFIG_SPACE_SIZE: usize = 256;
const PCI_COMMAND: usize = 0x04;
const PCI_STATUS: usize = 0x06;
const PCI_HEADER_TYPE: usize = 0x0e;
const PCI_HEADER_TYPE_MULTI_FUNCTION: u8 = 0x80;
const PCI_BAR0: usize = 0x10;
const PCI_BAR_COUNT: usize = 6;
const PCI_ROM_ADDRESS: usize = 0x30;
const PCI_CAPABILITY_LIST: usize = 0x34;
const PCI_INTERRUPT_LINE: usize = 0x3c;
const PCI_INTERRUPT_PIN: usize = 0x3d;
const PCI_COMMAND_MEMORY: u16 = 0x0002;
const PCI_STATUS_CAP_LIST: u16 = 0x0010;
const PCI_CAP_ID_PM: u8 = 0x01;
const PCI_PM_CAP_SIZE: usize = 8;
const PCI_CAP_ID_MSI: u8 = 0x05;
const PCI_CAP_ID_MSIX: u8 = 0x11;
const PCI_MSIX_CAP_SIZE: usize = 12;
const PCI_EXT_CAP_START: usize = 0x100;
const PCI_EXT_CAP_ID_AER: u16 = 0x0001;
const PCI_EXT_CAP_ID_ARI: u16 = 0x000e;
const PCI_EXT_CAP_ID_SRIOV: u16 = 0x0010;
const PCI_EXT_CAP_ID_REBAR: u16 = 0x0015;
const PCI_EXT_CAP_ID_VF_REBAR: u16 = 0x0024;
/// Standard capabilities unlinked from the guest-visible chain, with their
/// sizes.
const HIDDEN_CAPABILITIES: [(u8, usize); 1] = [(PCI_CAP_ID_PM, PCI_PM_CAP_SIZE)];
/// Extended capabilities unlinked from the guest-visible chain.
const HIDDEN_EXT_CAPABILITIES: [u16; 5] = [
	PCI_EXT_CAP_ID_AER,
	PCI_EXT_CAP_ID_ARI,
	PCI_EXT_CAP_ID_SRIOV,
	PCI_EXT_CAP_ID_REBAR,
	PCI_EXT_CAP_ID_VF_REBAR,
];
const MSI_CONTROL_ENABLE: u16 = 0x0001;
const MSI_CONTROL_MMC_MASK: u16 = 0x000e;
const MSI_CONTROL_MME_MASK: u16 = 0x0070;
const MSI_CONTROL_64BIT: u16 = 0x0080;
const MSI_CONTROL_MASKABLE: u16 = 0x0100;
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
				"VFIO GPU {label} BAR of {size:#x} bytes does not fit in the guest PCI aperture"
			)));
		}
		*next = allocation_end;
		Ok(base)
	}

	fn allocate_slot(&mut self) -> Result<u32> {
		if self.slot_next >= KVM_DEVICE_SLOT_END {
			return Err(err("VFIO GPU BARs exhausted KVM device memory slots"));
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
	/// Per-vector masking (mask and pending registers follow the data word).
	maskable:    bool,
}

impl MsiCapability {
	const fn data_offset(self) -> usize {
		self.offset + if self.is_64bit { 12 } else { 8 }
	}

	const fn mask_offset(self) -> usize {
		self.offset + if self.is_64bit { 16 } else { 12 }
	}

	const fn pending_offset(self) -> usize {
		self.mask_offset() + 4
	}

	/// Capability size in bytes, header included.
	const fn size(self) -> usize {
		match (self.is_64bit, self.maskable) {
			(false, false) => 10,
			(true, false) => 14,
			(false, true) => 20,
			(true, true) => 24,
		}
	}
}

/// How one byte of the guest-visible configuration space is backed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ConfigByte {
	/// Read from and written to the VFIO config region (vfio-pci filters it).
	Passthrough,
	/// Emulated: guest writes update the VMM's copy (then field fixups apply).
	Emulated,
	/// Emulated: guest writes are ignored.
	ReadOnly,
}

/// Per-byte backing of the guest-visible configuration space.
struct ConfigLayout {
	bytes: Vec<ConfigByte>,
}

impl ConfigLayout {
	fn new(device_size: usize) -> Self {
		let mut bytes = vec![ConfigByte::ReadOnly; PCI_CONFIG_SPACE_SIZE];
		bytes[..device_size.min(PCI_CONFIG_SPACE_SIZE)].fill(ConfigByte::Passthrough);
		Self { bytes }
	}

	fn mark(&mut self, start: usize, len: usize, kind: ConfigByte) {
		let end = start.saturating_add(len).min(PCI_CONFIG_SPACE_SIZE);
		if start < end {
			self.bytes[start..end].fill(kind);
		}
	}

	fn kind(&self, offset: usize) -> ConfigByte {
		self.bytes[offset]
	}

	/// End (exclusive, at most `end`) of the run of identically backed bytes
	/// starting at `start`.
	fn run_end(&self, start: usize, end: usize) -> usize {
		let kind = self.bytes[start];
		self.bytes[start..end]
			.iter()
			.position(|byte| *byte != kind)
			.map_or(end, |len| start + len)
	}
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

/// One NVIDIA GPU function (whole GPU or SR-IOV vGPU) assigned through VFIO.
pub struct VfioPciDevice {
	device:     VfioDevice,
	vm:         Arc<Vm>,
	layout:     ConfigLayout,
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
		if config_size < PCI_LEGACY_CONFIG_SPACE_SIZE as u64 {
			return Err(err(format!(
				"NVIDIA VFIO device {} exposes only {config_size} bytes of PCI configuration space",
				sysfs_path.display()
			)));
		}
		let config_size = usize::try_from(config_size)
			.unwrap_or(PCI_CONFIG_SPACE_SIZE)
			.min(PCI_CONFIG_SPACE_SIZE);
		let mut config = vec![0u8; PCI_CONFIG_SPACE_SIZE];
		device.region_read(VFIO_PCI_CONFIG_REGION_INDEX, &mut config[..config_size], 0);
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
		let layout = virtualize_config(&mut config, config_size, &bars, msi, msix);
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
			.unwrap_or("nvidia-gpu")
			.to_owned();
		let mut assigned = Self {
			device,
			vm,
			layout,
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
			error!(gpu = %self.label, vector, error = %cause, "failed to inject VFIO GPU MSI");
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

	/// Deliver vectors that became pending while masked and are now unmasked.
	fn flush_pending(&self, state: &mut PciState) {
		if state.interrupt == InterruptMode::Disabled {
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
				error!(gpu = %self.label, vector, error = %cause, "failed to flush VFIO GPU MSI");
			}
		}
		sync_msi_pending(state);
	}
}

impl PciFunction for VfioPciDevice {
	fn read_config(&self, offset: usize, data: &mut [u8]) {
		let state = self.state.lock();
		read_config_bytes(&self.layout, &state, offset, data, |start, bytes| {
			self
				.device
				.region_read(VFIO_PCI_CONFIG_REGION_INDEX, bytes, start as u64);
		});
	}

	fn write_config(&self, offset: usize, data: &[u8]) {
		let mut state = self.state.lock();
		write_config_bytes(&self.layout, &mut state, offset, data, |start, bytes, cache| {
			self
				.device
				.region_write(VFIO_PCI_CONFIG_REGION_INDEX, bytes, start as u64);
			self
				.device
				.region_read(VFIO_PCI_CONFIG_REGION_INDEX, cache, start as u64);
		});
		if let Err(cause) = self.update_interrupts(&mut state) {
			error!(gpu = %self.label, error = %cause, "failed to configure VFIO GPU interrupts");
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
			warn!(gpu = %self.label, error = %cause, "failed to stop VFIO GPU interrupts");
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
				warn!(gpu = %self.label, slot = mapping.slot, error = %cause, "failed to remove VFIO GPU BAR memory slot");
			}
		}
		// Best effort: a no-op when vfio-pci has no function-level reset method;
		// vfio-pci then resets the bus when the last function is closed.
		self.device.reset();
	}
}

/// Canonicalize and validate `--vfio-gpu` sysfs paths (whole GPUs bound to
/// vfio-pci or SR-IOV vGPU functions with a profile).
pub fn normalize_gpu_paths(paths: &mut [PathBuf]) -> Result<()> {
	if paths.len() > MAX_VFIO_GPUS {
		return Err(err(format!(
			"at most {MAX_VFIO_GPUS} NVIDIA GPU functions can be assigned (got {})",
			paths.len()
		)));
	}
	let mut normalized = Vec::with_capacity(paths.len());
	for path in paths.iter() {
		if !path.is_absolute() || !path.starts_with("/sys/bus/pci/devices") {
			return Err(err(format!(
				"--vfio-gpu must name an absolute PCI function under /sys/bus/pci/devices (got {})",
				path.display()
			)));
		}
		let bdf = path
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| err(format!("invalid --vfio-gpu sysfs path {}", path.display())))?;
		validate_bdf(bdf)?;
		let canonical = fs::canonicalize(path)
			.map_err(|cause| err(format!("resolving --vfio-gpu {}: {cause}", path.display())))?;
		validate_gpu_function(path, &canonical)?;
		if normalized.contains(&canonical) {
			return Err(err(format!("--vfio-gpu {} was specified more than once", path.display())));
		}
		normalized.push(canonical);
	}
	paths.clone_from_slice(&normalized);
	Ok(())
}

/// Check that the canonical sysfs directory of `path` is an assignable NVIDIA
/// GPU function: an SR-IOV vGPU VF with a pre-created profile, or a whole GPU
/// bound to vfio-pci whose IOMMU group is viable.
fn validate_gpu_function(path: &Path, canonical: &Path) -> Result<()> {
	let vendor = read_sysfs_trimmed(&canonical.join("vendor"))?;
	if parse_sysfs_hex(&vendor) != Some(u32::from(NVIDIA_VENDOR_ID)) {
		return Err(err(format!(
			"--vfio-gpu {} has PCI vendor {vendor}; only NVIDIA ({NVIDIA_VENDOR_ID:#06x}) GPUs are \
			 supported",
			path.display()
		)));
	}
	let class = read_sysfs_trimmed(&canonical.join("class"))?;
	if parse_sysfs_hex(&class).map(|class| class >> 16) != Some(PCI_BASE_CLASS_DISPLAY) {
		return Err(err(format!(
			"--vfio-gpu {} has PCI class {class}; only display controllers (class 0x03xxxx) can be \
			 assigned",
			path.display()
		)));
	}

	if fs::symlink_metadata(canonical.join("physfn")).is_ok() {
		let current_type = read_sysfs_trimmed(&canonical.join("nvidia/current_vgpu_type"))?;
		if current_type == "0" || current_type.parse::<u32>().is_err() {
			return Err(err(format!(
				"NVIDIA VF {} has no pre-created vGPU profile (current_vgpu_type={current_type:?})",
				path.display()
			)));
		}
		return Ok(());
	}

	match bound_driver(canonical)? {
		Some(driver) if driver == "vfio-pci" => {},
		driver => {
			return Err(err(format!(
				"--vfio-gpu {} is {}; bind it to vfio-pci for whole-GPU passthrough",
				path.display(),
				driver.map_or_else(
					|| "not bound to a driver".to_owned(),
					|driver| format!("bound to {driver}")
				)
			)));
		},
	}
	let group_link = canonical.join("iommu_group");
	let group = fs::read_link(&group_link).map_err(|cause| {
		err(format!(
			"--vfio-gpu {} has no IOMMU group (is the IOMMU enabled?): {cause}",
			path.display()
		))
	})?;
	let group_name = group
		.file_name()
		.map(|name| name.to_string_lossy().into_owned())
		.unwrap_or_default();
	let members = group_link.join("devices");
	let mut entries = fs::read_dir(&members)
		.map_err(|cause| err(format!("reading {}: {cause}", members.display())))?
		.collect::<io::Result<Vec<_>>>()?;
	entries.sort_by_key(fs::DirEntry::file_name);
	for entry in entries {
		let member = fs::canonicalize(entry.path())
			.map_err(|cause| err(format!("resolving {}: {cause}", entry.path().display())))?;
		if member == canonical {
			continue;
		}
		let driver = bound_driver(&member)?;
		let viable = match driver.as_deref() {
			None | Some("vfio-pci") => true,
			// The kernel lets bridges keep pcieport: it never claims their DMA.
			Some("pcieport") => read_sysfs_trimmed(&member.join("class"))
				.ok()
				.and_then(|class| parse_sysfs_hex(&class))
				.is_some_and(|class| class >> 8 == PCI_CLASS_BRIDGE_PCI),
			Some(_) => false,
		};
		if !viable {
			return Err(err(format!(
				"IOMMU group {group_name} of --vfio-gpu {} is not viable: member {} is bound to {}; \
				 bind every function in the group to vfio-pci",
				path.display(),
				entry.file_name().to_string_lossy(),
				driver.unwrap_or_default()
			)));
		}
	}
	Ok(())
}

/// Name of the driver bound to a sysfs PCI function, if any.
fn bound_driver(function: &Path) -> Result<Option<String>> {
	match fs::read_link(function.join("driver")) {
		Ok(target) => Ok(target
			.file_name()
			.map(|name| name.to_string_lossy().into_owned())),
		Err(cause) if cause.kind() == io::ErrorKind::NotFound => Ok(None),
		Err(cause) => Err(err(format!("reading {}/driver: {cause}", function.display()))),
	}
}

fn parse_sysfs_hex(value: &str) -> Option<u32> {
	u32::from_str_radix(value.strip_prefix("0x")?, 16).ok()
}

/// Resolve the VFIO character device associated with one assigned PCI
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
///
/// Also lifts `RLIMIT_MEMLOCK` while still privileged: iommufd pins all guest
/// RAM for device DMA and charges it to the locked memory of the (by then
/// unprivileged) sandbox uid, which a default limit would refuse with `ENOMEM`.
pub fn preopen_gpu_devices(paths: &[PathBuf]) -> Result<Option<PreopenedVfio>> {
	if paths.is_empty() {
		return Ok(None);
	}
	let unlimited = libc::rlimit { rlim_cur: libc::RLIM_INFINITY, rlim_max: libc::RLIM_INFINITY };
	// SAFETY: `unlimited` is a valid `rlimit` that libc copies during the call.
	if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &unlimited) } != 0 {
		warn!(
			error = %io::Error::last_os_error(),
			"cannot lift RLIMIT_MEMLOCK; VFIO DMA mapping of guest RAM may fail with ENOMEM"
		);
	}
	let iommufd = Arc::new(
		IommuFd::new().map_err(|cause| err(format!("opening /dev/iommu for VFIO GPU: {cause}")))?,
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

/// Attach every pre-opened NVIDIA GPU function and map guest RAM into their
/// shared IOAS.
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
				"mapping guest RAM {:#x}+{:#x} for VFIO GPU DMA: {cause}",
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
			// I/O-port BARs (e.g. a GeForce BAR5 for legacy VGA/VBIOS access)
			// read as absent: the guest has no port I/O window for them and
			// NVIDIA's driver only uses the memory BARs.
			index += 1;
			continue;
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
		maskable: control & MSI_CONTROL_MASKABLE != 0,
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
			error!(error = %cause, "VFIO GPU interrupt poll failed");
			return;
		}
		if fds[kill_index].revents & libc::POLLIN != 0 {
			let _ = kill.read();
			return;
		}
		for (vector, event) in events.iter().enumerate() {
			let revents = fds[vector].revents;
			if revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
				error!(vector, revents, "VFIO GPU interrupt eventfd failed");
				return;
			}
			if revents & libc::POLLIN == 0 {
				continue;
			}
			let count = match event.read() {
				Ok(count) => count,
				Err(cause) if cause.kind() == io::ErrorKind::WouldBlock => continue,
				Err(cause) => {
					error!(vector, error = %cause, "reading VFIO GPU interrupt failed");
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
			if capability.maskable
				&& read_u32(&state.config, capability.mask_offset()) & (1 << vector) != 0
			{
				state.pending[vector] = true;
				sync_msi_pending(state);
				return None;
			}
			let address_low = read_u32(&state.config, capability.offset + 4);
			let address_high = if capability.is_64bit {
				read_u32(&state.config, capability.offset + 8)
			} else {
				0
			};
			let base_data = u32::from(read_u16(&state.config, capability.data_offset()));
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
	capability_chain(config)
		.into_iter()
		.find_map(|(offset, capability)| (capability == id).then_some(offset))
}

/// Mirror pending MSI vectors into the emulated MSI Pending Bits register.
fn sync_msi_pending(state: &mut PciState) {
	let Some(capability) = state.msi.filter(|capability| capability.maskable) else {
		return;
	};
	let bits = state
		.pending
		.iter()
		.take(capability.max_vectors.min(32))
		.enumerate()
		.filter(|(_, pending)| **pending)
		.fold(0u32, |bits, (vector, _)| bits | (1 << vector));
	write_u32(&mut state.config, capability.pending_offset(), bits);
}

/// Build the guest-visible configuration space from the device's and record
/// which bytes are emulated (see the module documentation for the policy).
///
/// `config` holds the device's configuration space with BARs already assigned
/// by [`build_bars`]; `device_size` is the size of the VFIO config region.
fn virtualize_config(
	config: &mut [u8],
	device_size: usize,
	bars: &[PciBar],
	msi: Option<MsiCapability>,
	msix: Option<MsixCapability>,
) -> ConfigLayout {
	let mut layout = ConfigLayout::new(device_size);
	config[device_size.min(PCI_CONFIG_SPACE_SIZE)..].fill(0);

	// One function per guest slot, whatever the host topology.
	config[PCI_HEADER_TYPE] &= !PCI_HEADER_TYPE_MULTI_FUNCTION;
	layout.mark(PCI_HEADER_TYPE, 1, ConfigByte::ReadOnly);

	// Unassigned BAR slots (absent, I/O port) and the expansion ROM read as
	// zero.
	config[PCI_BAR0..PCI_BAR0 + PCI_BAR_COUNT * 4].fill(0);
	layout.mark(PCI_BAR0, PCI_BAR_COUNT * 4, ConfigByte::ReadOnly);
	for bar in bars {
		write_assigned_bar(config, bar);
		let len = if bar.is_64bit { 8 } else { 4 };
		layout.mark(PCI_BAR0 + usize::from(bar.index) * 4, len, ConfigByte::Emulated);
	}
	config[PCI_ROM_ADDRESS..PCI_ROM_ADDRESS + 4].fill(0);
	layout.mark(PCI_ROM_ADDRESS, 4, ConfigByte::ReadOnly);

	// INTx is never routed: guests must use MSI or MSI-X. The line register is
	// guest scratch space.
	config[PCI_INTERRUPT_LINE] = 0;
	config[PCI_INTERRUPT_PIN] = 0;
	layout.mark(PCI_INTERRUPT_LINE, 1, ConfigByte::Emulated);
	layout.mark(PCI_INTERRUPT_PIN, 1, ConfigByte::ReadOnly);

	virtualize_capabilities(config, &mut layout);
	if device_size >= PCI_CONFIG_SPACE_SIZE {
		virtualize_ext_capabilities(config, &mut layout);
	}

	if let Some(msi) = msi {
		layout.mark(msi.offset + 2, msi.size() - 2, ConfigByte::Emulated);
		if msi.maskable {
			write_u32(config, msi.pending_offset(), 0);
			layout.mark(msi.pending_offset(), 4, ConfigByte::ReadOnly);
		}
	}
	if let Some(msix) = msix {
		layout.mark(msix.offset + 2, 2, ConfigByte::Emulated);
		layout.mark(msix.offset + 4, PCI_MSIX_CAP_SIZE - 4, ConfigByte::ReadOnly);
	}
	layout
}

/// Relink the standard capability list without [`HIDDEN_CAPABILITIES`] and
/// make every capability header read-only.
fn virtualize_capabilities(config: &mut [u8], layout: &mut ConfigLayout) {
	layout.mark(PCI_CAPABILITY_LIST, 1, ConfigByte::ReadOnly);
	let capabilities = capability_chain(config);
	let mut previous = PCI_CAPABILITY_LIST;
	for (offset, id) in capabilities {
		if let Some((_, size)) = HIDDEN_CAPABILITIES.iter().find(|(hidden, _)| *hidden == id) {
			let end = (offset + size).min(PCI_LEGACY_CONFIG_SPACE_SIZE);
			config[offset..end].fill(0);
			layout.mark(offset, end - offset, ConfigByte::ReadOnly);
			continue;
		}
		config[previous] = offset as u8;
		previous = offset + 1;
		layout.mark(offset, 2, ConfigByte::ReadOnly);
	}
	config[previous] = 0;
}

/// Standard capabilities as `(offset, id)` in chain order.
fn capability_chain(config: &[u8]) -> Vec<(usize, u8)> {
	let mut chain = Vec::new();
	if read_u16(config, PCI_STATUS) & PCI_STATUS_CAP_LIST == 0 {
		return chain;
	}
	let mut offset = usize::from(config[PCI_CAPABILITY_LIST] & !0x3);
	while (0x40..=0xfc).contains(&offset) && !chain.iter().any(|(seen, _)| *seen == offset) {
		chain.push((offset, config[offset]));
		offset = usize::from(config[offset + 1] & !0x3);
	}
	chain
}

/// Relink the `PCIe` extended capability list without
/// [`HIDDEN_EXT_CAPABILITIES`] and make every extended capability header
/// read-only. A hidden capability at 0x100 is replaced by a null (ID 0) header
/// that links to the first visible one, as the list must start there.
fn virtualize_ext_capabilities(config: &mut [u8], layout: &mut ConfigLayout) {
	let mut chain: Vec<(usize, u16)> = Vec::new();
	let mut offset = PCI_EXT_CAP_START;
	while offset != 0 && !chain.iter().any(|(seen, _)| *seen == offset) {
		let header = read_u32(config, offset);
		if header == 0 || header == u32::MAX {
			break;
		}
		chain.push((offset, header as u16));
		offset = (header >> 20) as usize & !0x3;
		if !(PCI_EXT_CAP_START..=PCI_CONFIG_SPACE_SIZE - 4).contains(&offset) {
			break;
		}
	}
	let visible = chain
		.iter()
		.filter(|(_, id)| !HIDDEN_EXT_CAPABILITIES.contains(id))
		.map(|(offset, _)| *offset)
		.collect::<Vec<_>>();
	for &(offset, id) in &chain {
		if HIDDEN_EXT_CAPABILITIES.contains(&id) {
			// Capability sizes vary (SR-IOV, AER, ReBAR); hide everything up to
			// the next capability in address order.
			let end = chain
				.iter()
				.map(|(start, _)| *start)
				.filter(|start| *start > offset)
				.min()
				.unwrap_or(PCI_CONFIG_SPACE_SIZE);
			config[offset..end].fill(0);
			layout.mark(offset, end - offset, ConfigByte::ReadOnly);
		}
	}
	for (index, &offset) in visible.iter().enumerate() {
		let next = visible.get(index + 1).copied().unwrap_or(0);
		let header = read_u32(config, offset) & 0x000f_ffff;
		write_u32(config, offset, header | ((next as u32) << 20));
		layout.mark(offset, 4, ConfigByte::ReadOnly);
	}
	if chain
		.first()
		.is_some_and(|(_, id)| HIDDEN_EXT_CAPABILITIES.contains(id))
	{
		let next = visible.first().copied().unwrap_or(0);
		write_u32(config, PCI_EXT_CAP_START, (next as u32) << 20);
	}
}

/// Serve a guest configuration read: pass-through bytes come live from the
/// device (`device_read(offset, buffer)`), emulated bytes from `state.config`,
/// and BAR registers being sized return their size masks.
fn read_config_bytes(
	layout: &ConfigLayout,
	state: &PciState,
	offset: usize,
	data: &mut [u8],
	device_read: impl FnOnce(usize, &mut [u8]),
) {
	data.fill(0xff);
	if offset >= PCI_CONFIG_SPACE_SIZE {
		return;
	}
	let end = offset.saturating_add(data.len()).min(PCI_CONFIG_SPACE_SIZE);
	let data = &mut data[..end - offset];
	if (offset..end).any(|byte| layout.kind(byte) == ConfigByte::Passthrough) {
		device_read(offset, data);
	}
	for (byte, value) in (offset..end).zip(data.iter_mut()) {
		if layout.kind(byte) != ConfigByte::Passthrough {
			*value = state.config[byte];
		}
	}
	for bar in &state.bars {
		let register = PCI_BAR0 + usize::from(bar.index) * 4;
		if bar.probe_low {
			patch_field(data, offset, register, &bar_mask_low(bar).to_le_bytes());
		}
		if bar.is_64bit && bar.probe_high {
			patch_field(data, offset, register + 4, &bar_mask_high(bar).to_le_bytes());
		}
	}
}

/// Apply a guest configuration write: pass-through runs go to the device
/// (`device_write(offset, bytes, cache)` must also refresh `cache` from it),
/// emulated bytes update `state.config`, read-only bytes are dropped. BAR,
/// MSI, and MSI-X fields then get their register semantics.
fn write_config_bytes(
	layout: &ConfigLayout,
	state: &mut PciState,
	offset: usize,
	data: &[u8],
	mut device_write: impl FnMut(usize, &[u8], &mut [u8]),
) {
	if data.is_empty() || offset >= PCI_CONFIG_SPACE_SIZE {
		return;
	}
	let end = offset.saturating_add(data.len()).min(PCI_CONFIG_SPACE_SIZE);
	let len = end - offset;
	let msi_control = state
		.msi
		.map(|capability| read_u16(&state.config, capability.offset + 2));
	let msix_control = state
		.msix
		.map(|capability| read_u16(&state.config, capability.offset + 2));

	let mut start = offset;
	while start < end {
		let stop = layout.run_end(start, end);
		let bytes = &data[start - offset..stop - offset];
		match layout.kind(start) {
			ConfigByte::Passthrough => device_write(start, bytes, &mut state.config[start..stop]),
			ConfigByte::Emulated => state.config[start..stop].copy_from_slice(bytes),
			ConfigByte::ReadOnly => {},
		}
		start = stop;
	}

	let PciState { config, bars, .. } = &mut *state;
	for bar in bars.iter_mut() {
		let low = PCI_BAR0 + usize::from(bar.index) * 4;
		if touches(offset, len, low, 4) {
			bar.probe_low = read_u32(config, low) == u32::MAX;
		}
		if bar.is_64bit && touches(offset, len, low + 4, 4) {
			bar.probe_high = read_u32(config, low + 4) == u32::MAX;
		}
		// Assigned BARs are fixed: only sizing probes are honoured.
		write_assigned_bar(config, bar);
	}
	if let (Some(msi), Some(old)) = (state.msi, msi_control)
		&& touches(offset, len, msi.offset + 2, 2)
	{
		let requested = read_u16(&state.config, msi.offset + 2);
		let writable = MSI_CONTROL_ENABLE | MSI_CONTROL_MME_MASK;
		write_u16(&mut state.config, msi.offset + 2, (old & !writable) | (requested & writable));
	}
	if let (Some(msix), Some(old)) = (state.msix, msix_control)
		&& touches(offset, len, msix.offset + 2, 2)
	{
		let requested = read_u16(&state.config, msix.offset + 2);
		let writable = MSIX_CONTROL_ENABLE | MSIX_CONTROL_FUNCTION_MASK;
		write_u16(
			&mut state.config,
			msix.offset + 2,
			(old & MSIX_CONTROL_TABLE_SIZE) | (requested & writable),
		);
	}
}

/// Overwrite the part of `data` (a config access at `offset`) that overlaps
/// the register `value` located at `field`.
fn patch_field(data: &mut [u8], offset: usize, field: usize, value: &[u8]) {
	for (index, byte) in value.iter().enumerate() {
		if let Some(target) = (field + index)
			.checked_sub(offset)
			.and_then(|position| data.get_mut(position))
		{
			*target = *byte;
		}
	}
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

	const GPU_MSI: MsiCapability =
		MsiCapability { offset: 0x48, max_vectors: 16, is_64bit: true, maskable: true };
	const GPU_MSIX: MsixCapability = MsixCapability {
		offset:       0xb0,
		vectors:      9,
		table_bar:    0,
		table_offset: 0x00b9_0000,
		pba_bar:      0,
		pba_offset:   0x00ba_0000,
	};

	fn put_cap(config: &mut [u8], offset: usize, id: u8, next: usize) {
		config[offset] = id;
		config[offset + 1] = next as u8;
	}

	fn put_ext_cap(config: &mut [u8], offset: usize, id: u16, next: usize) {
		write_u32(config, offset, u32::from(id) | (1 << 16) | ((next as u32) << 20));
	}

	/// Configuration space shaped like a `GeForce` RTX 5070 behind vfio-pci.
	fn geforce_config() -> Vec<u8> {
		let mut config = vec![0u8; PCI_CONFIG_SPACE_SIZE];
		write_u16(&mut config, 0, NVIDIA_VENDOR_ID);
		write_u16(&mut config, 2, 0x2f04);
		write_u16(&mut config, PCI_COMMAND, 0x0007);
		write_u16(&mut config, PCI_STATUS, PCI_STATUS_CAP_LIST);
		config[0x0b] = 0x03;
		config[PCI_HEADER_TYPE] = 0x80;
		write_u32(&mut config, PCI_BAR0, 0xb000_0000);
		write_u32(&mut config, PCI_BAR0 + 4, 0x0000_000c);
		write_u32(&mut config, PCI_BAR0 + 8, 0x2038);
		write_u32(&mut config, PCI_BAR0 + 12, 0x0000_000c);
		write_u32(&mut config, PCI_BAR0 + 16, 0x203c);
		write_u32(&mut config, PCI_BAR0 + 20, 0x9001);
		write_u32(&mut config, PCI_ROM_ADDRESS, 0xb400_0000);
		config[PCI_CAPABILITY_LIST] = 0x40;
		config[PCI_INTERRUPT_LINE] = 0xff;
		config[PCI_INTERRUPT_PIN] = 2;
		put_cap(&mut config, 0x40, PCI_CAP_ID_PM, 0x48);
		write_u16(&mut config, 0x44, 0x0003);
		put_cap(&mut config, 0x48, PCI_CAP_ID_MSI, 0x60);
		write_u16(&mut config, 0x4a, MSI_CONTROL_64BIT | MSI_CONTROL_MASKABLE | 0x0008);
		put_cap(&mut config, 0x60, 0x10, 0x9c);
		put_cap(&mut config, 0x9c, 0x09, 0xb0);
		put_cap(&mut config, 0xb0, PCI_CAP_ID_MSIX, 0);
		write_u16(&mut config, 0xb2, 8);
		write_u32(&mut config, 0xb4, 0x00b9_0000);
		write_u32(&mut config, 0xb8, 0x00ba_0000);
		put_ext_cap(&mut config, 0x100, 0x19, 0x12c);
		put_ext_cap(&mut config, 0x12c, 0x18, 0x134);
		put_ext_cap(&mut config, 0x134, PCI_EXT_CAP_ID_REBAR, 0x1b8);
		write_u32(&mut config, 0x138, 0x0007_fc00);
		put_ext_cap(&mut config, 0x1b8, PCI_EXT_CAP_ID_AER, 0x248);
		put_ext_cap(&mut config, 0x248, PCI_EXT_CAP_ID_ARI, 0x250);
		put_ext_cap(&mut config, 0x250, PCI_EXT_CAP_ID_SRIOV, 0x2a4);
		write_u16(&mut config, 0x25e, 1);
		put_ext_cap(&mut config, 0x2a4, 0x0b, 0);
		config
	}

	fn geforce_bars() -> Vec<PciBar> {
		let bar = |index: u8, size: u64, address: u64, flags: u32| PciBar {
			index,
			region: VFIO_PCI_BAR0_REGION_INDEX + u32::from(index),
			size,
			address,
			flags,
			is_64bit: flags & 0x4 != 0,
			probe_low: false,
			probe_high: false,
		};
		vec![
			bar(0, 0x0400_0000, 0xd400_0000, 0x0),
			bar(1, 0x4_0000_0000, PCI_HIGH_MMIO_START, 0xc),
			bar(3, 0x0200_0000, PCI_HIGH_MMIO_START + 0x4_0000_0000, 0xc),
		]
	}

	fn geforce_state() -> (ConfigLayout, PciState, Vec<u8>) {
		let device = geforce_config();
		let mut config = device.clone();
		let bars = geforce_bars();
		let layout = virtualize_config(
			&mut config,
			PCI_CONFIG_SPACE_SIZE,
			&bars,
			Some(GPU_MSI),
			Some(GPU_MSIX),
		);
		let state = PciState {
			config,
			bars,
			msi: Some(GPU_MSI),
			msix: Some(GPU_MSIX),
			msix_table: vec![MsixEntry::default(); GPU_MSIX.vectors],
			pending: vec![false; GPU_MSI.max_vectors],
			interrupt: InterruptMode::Disabled,
		};
		(layout, state, device)
	}

	fn guest_read(layout: &ConfigLayout, state: &PciState, device: &[u8], offset: usize) -> u32 {
		let mut data = [0u8; 4];
		read_config_bytes(layout, state, offset, &mut data, |start, bytes| {
			bytes.copy_from_slice(&device[start..start + bytes.len()]);
		});
		u32::from_le_bytes(data)
	}

	fn guest_write(
		layout: &ConfigLayout,
		state: &mut PciState,
		device: &mut [u8],
		offset: usize,
		value: u32,
	) {
		write_config_bytes(layout, state, offset, &value.to_le_bytes(), |start, bytes, cache| {
			device[start..start + bytes.len()].copy_from_slice(bytes);
			cache.copy_from_slice(&device[start..start + cache.len()]);
		});
	}

	#[test]
	fn hides_host_owned_config_space_of_a_whole_gpu() {
		let (layout, state, device) = geforce_state();
		let read = |offset| guest_read(&layout, &state, &device, offset);

		assert_eq!((read(PCI_HEADER_TYPE & !3) >> 16) & 0xff, 0, "single-function header");
		assert_eq!(read(PCI_BAR0), 0xd400_0000);
		assert_eq!(read(PCI_BAR0 + 4), 0x0000_000c);
		assert_eq!(read(PCI_BAR0 + 8), (PCI_HIGH_MMIO_START >> 32) as u32);
		assert_eq!(read(PCI_BAR0 + 20), 0, "I/O BAR5 reads as absent");
		assert_eq!(read(PCI_ROM_ADDRESS), 0, "expansion ROM reads as absent");
		assert_eq!((read(PCI_INTERRUPT_LINE) >> 8) & 0xff, 0, "no INTx pin");

		let mut chain = Vec::new();
		let mut offset = read(PCI_CAPABILITY_LIST) as usize & 0xfc;
		while offset != 0 {
			let header = read(offset);
			chain.push((offset, header as u8));
			offset = (header >> 8) as usize & 0xfc;
		}
		assert_eq!(chain, [(0x48, PCI_CAP_ID_MSI), (0x60, 0x10), (0x9c, 0x09), (0xb0, 0x11)]);
		assert_eq!(read(0x40), 0, "PM capability body is hidden");

		let mut ext = Vec::new();
		let mut offset = PCI_EXT_CAP_START;
		while offset != 0 {
			let header = read(offset);
			ext.push((offset, header as u16));
			offset = (header >> 20) as usize;
		}
		assert_eq!(ext, [(0x100, 0x19), (0x12c, 0x18), (0x2a4, 0x0b)]);
		for hidden in [0x134, 0x138, 0x1b8, 0x248, 0x250, 0x25c] {
			assert_eq!(read(hidden), 0, "hidden extended capability byte {hidden:#x}");
		}
	}

	#[test]
	fn replaces_a_hidden_first_extended_capability_with_a_null_header() {
		let mut config = vec![0u8; PCI_CONFIG_SPACE_SIZE];
		put_ext_cap(&mut config, 0x100, PCI_EXT_CAP_ID_AER, 0x148);
		put_ext_cap(&mut config, 0x148, PCI_EXT_CAP_ID_SRIOV, 0x200);
		put_ext_cap(&mut config, 0x200, 0x18, 0);
		let mut layout = ConfigLayout::new(PCI_CONFIG_SPACE_SIZE);
		virtualize_ext_capabilities(&mut config, &mut layout);
		assert_eq!(read_u32(&config, 0x100), 0x200 << 20);
		assert_eq!(read_u32(&config, 0x200) & 0xffff, 0x18);
		assert_eq!(read_u32(&config, 0x200) >> 20, 0);
		assert_eq!(layout.kind(0x104), ConfigByte::ReadOnly);
		assert_eq!(layout.kind(0x204), ConfigByte::Passthrough);
	}

	#[test]
	fn emulates_bar_sizing_and_forwards_only_passthrough_bytes() {
		let (layout, mut state, mut device) = geforce_state();

		guest_write(&layout, &mut state, &mut device, PCI_BAR0 + 4, u32::MAX);
		guest_write(&layout, &mut state, &mut device, PCI_BAR0 + 8, u32::MAX);
		guest_write(&layout, &mut state, &mut device, PCI_BAR0 + 20, u32::MAX);
		guest_write(&layout, &mut state, &mut device, PCI_ROM_ADDRESS, u32::MAX);
		assert_eq!(guest_read(&layout, &state, &device, PCI_BAR0 + 4), 0x0000_000c);
		assert_eq!(guest_read(&layout, &state, &device, PCI_BAR0 + 8), 0xffff_fffc);
		assert_eq!(guest_read(&layout, &state, &device, PCI_BAR0 + 20), 0);
		assert_eq!(guest_read(&layout, &state, &device, PCI_ROM_ADDRESS), 0);
		assert_eq!(read_u32(&device, PCI_BAR0 + 20), 0x9001, "BAR writes never reach the device");

		guest_write(&layout, &mut state, &mut device, PCI_BAR0 + 4, 0x1234_0000);
		guest_write(&layout, &mut state, &mut device, PCI_BAR0 + 8, 0x55);
		assert_eq!(
			guest_read(&layout, &state, &device, PCI_BAR0 + 8),
			(PCI_HIGH_MMIO_START >> 32) as u32,
			"assigned BARs stay fixed"
		);

		guest_write(&layout, &mut state, &mut device, PCI_COMMAND, 0x0000_0406);
		assert_eq!(read_u16(&device, PCI_COMMAND), 0x0406, "command register passes through");
		assert_eq!(read_u16(&state.config, PCI_COMMAND), 0x0406, "cached for BAR decoding");

		let pm_control = read_u32(&device, 0x44);
		guest_write(&layout, &mut state, &mut device, 0x44, 0x0000_0003);
		assert_eq!(read_u32(&device, 0x44), pm_control, "hidden PM capability is not writable");

		guest_write(&layout, &mut state, &mut device, 0x48, 0xffff_0000 | 0x0071);
		let msi = guest_read(&layout, &state, &device, 0x48);
		assert_eq!(msi & 0xffff, 0x6005, "MSI header stays read-only");
		assert_eq!(msi >> 16, u32::from(MSI_CONTROL_64BIT | MSI_CONTROL_MASKABLE | 0x0079));
	}

	#[test]
	fn defers_masked_msi_vectors_through_the_pending_register() {
		let (layout, mut state, mut device) = geforce_state();
		guest_write(&layout, &mut state, &mut device, 0x48 + 4, 0xfee0_0000);
		guest_write(&layout, &mut state, &mut device, 0x48 + 12, 0x0040);
		guest_write(&layout, &mut state, &mut device, 0x48 + 16, 0b10);
		guest_write(
			&layout,
			&mut state,
			&mut device,
			0x48,
			(u32::from(MSI_CONTROL_ENABLE | 0x0020) << 16) | 0x6005,
		);
		state.interrupt = desired_interrupt_mode(&state);
		assert_eq!(state.interrupt, InterruptMode::Msi { vectors: 4 });

		let first = interrupt_message(&mut state, 0).expect("unmasked vector");
		assert_eq!((first.address_lo, first.data), (0xfee0_0000, 0x40));
		assert!(interrupt_message(&mut state, 1).is_none());
		assert_eq!(guest_read(&layout, &state, &device, 0x48 + 20), 0b10);

		guest_write(&layout, &mut state, &mut device, 0x48 + 16, 0);
		let second = interrupt_message(&mut state, 1).expect("unmasked vector");
		assert_eq!(second.data, 0x41);
	}

	fn fake_function(root: &Path, bdf: &str, class: &str, driver: Option<&str>) -> PathBuf {
		let function = root.join("devices").join(bdf);
		fs::create_dir_all(&function).expect("function dir");
		fs::write(function.join("vendor"), "0x10de\n").expect("vendor");
		fs::write(function.join("class"), format!("{class}\n")).expect("class");
		if let Some(driver) = driver {
			let driver_dir = root.join("drivers").join(driver);
			fs::create_dir_all(&driver_dir).expect("driver dir");
			std::os::unix::fs::symlink(&driver_dir, function.join("driver")).expect("driver link");
		}
		let group = root.join("groups/15/devices");
		fs::create_dir_all(&group).expect("group dir");
		std::os::unix::fs::symlink(&function, group.join(bdf)).expect("group member");
		std::os::unix::fs::symlink(root.join("groups/15"), function.join("iommu_group"))
			.expect("group link");
		function
	}

	fn fake_sysfs(tag: &str) -> PathBuf {
		let root = std::env::temp_dir().join(format!("vmon-vfio-{tag}-{}", std::process::id()));
		let _ = fs::remove_dir_all(&root);
		fs::create_dir_all(&root).expect("fake sysfs root");
		root
	}

	#[test]
	fn requires_a_viable_iommu_group_for_whole_gpus() {
		let root = fake_sysfs("viable");
		let gpu = fake_function(&root, "0000:52:00.0", "0x030000", Some("vfio-pci"));
		fake_function(&root, "0000:52:00.1", "0x040300", Some("vfio-pci"));
		validate_gpu_function(&gpu, &gpu).expect("viable group");
		fs::remove_dir_all(&root).expect("cleanup");

		let root = fake_sysfs("offender");
		let gpu = fake_function(&root, "0000:52:00.0", "0x030000", Some("vfio-pci"));
		fake_function(&root, "0000:52:00.1", "0x040300", Some("snd_hda_intel"));
		let error = validate_gpu_function(&gpu, &gpu)
			.expect_err("audio function on its host driver")
			.to_string();
		assert!(error.contains("IOMMU group 15"), "{error}");
		assert!(error.contains("0000:52:00.1 is bound to snd_hda_intel"), "{error}");
		fs::remove_dir_all(&root).expect("cleanup");

		let root = fake_sysfs("host-driver");
		let gpu = fake_function(&root, "0000:52:00.0", "0x030000", Some("nvidia"));
		let error = validate_gpu_function(&gpu, &gpu)
			.expect_err("GPU on its host driver")
			.to_string();
		assert!(error.contains("bound to nvidia; bind it to vfio-pci"), "{error}");
		fs::remove_dir_all(&root).expect("cleanup");

		let root = fake_sysfs("class");
		let audio = fake_function(&root, "0000:52:00.1", "0x040300", Some("vfio-pci"));
		let error = validate_gpu_function(&audio, &audio)
			.expect_err("non-display function")
			.to_string();
		assert!(error.contains("only display controllers"), "{error}");
		fs::remove_dir_all(&root).expect("cleanup");
	}
}
