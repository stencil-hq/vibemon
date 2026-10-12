//! WHP VM creation, guest-memory registration and shared backend state.

use std::sync::Arc;

use libwhp::{
	FALSE, GPARangeMapping, Partition, WHV_CAPABILITY, WHV_CAPABILITY_CODE, WHV_MAP_GPA_RANGE_FLAGS,
	WHV_PARTITION_PROPERTY, WHV_PARTITION_PROPERTY_CODE, WHV_X64_LOCAL_APIC_EMULATION_MODE,
	get_capability,
};
use parking_lot::Mutex;
use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

use super::{CpuId, IoApic, IoApicState, IrqLine, Vcpu};
use crate::{
	devices::Bus,
	layout::IO_APIC_DEFAULT_PHYS_BASE,
	memory::{GuestMemoryMmap, create_guest_memory},
	os::EventFd,
	result::{Result, err},
};

const WHP_PROCESSOR_COUNT: u32 = 64;
const CPUID_EXIT_LEAVES: &[u32] = &[0x1, 0xb, 0x1f, 0x4000_0000];

/// A userspace ioeventfd registration handled by the WHP run loop.
pub(super) struct IoEvent {
	pub addr:      u64,
	pub datamatch: Option<u64>,
	pub evt:       EventFd,
}

/// Shared ioevent table consulted by every WHP vCPU.
pub(super) type IoEvents = Arc<Mutex<Vec<IoEvent>>>;

/// Owns the WHP partition, guest RAM and shared userspace interrupt/ioevent
/// state.
pub struct Vm {
	#[allow(dead_code, reason = "dropping the mapping unmaps guest RAM from WHP")]
	mappings:        Mutex<Vec<GPARangeMapping>>,
	partition:       Partition,
	ioapic:          Arc<Mutex<IoApic>>,
	ioevents:        IoEvents,
	memory:          GuestMemoryMmap,
	supported_cpuid: CpuId,
	dirty_tracking:  bool,
}

impl Vm {
	/// Create a VM with freshly-allocated guest RAM of `mem_size` bytes.
	pub fn new(mem_size: usize) -> Result<Self> {
		Self::with_memory(create_guest_memory(mem_size)?)
	}

	/// Create a VM around already-built guest memory.
	pub fn with_memory(memory: GuestMemoryMmap) -> Result<Self> {
		ensure_hypervisor_present()?;
		ensure_local_apic_available()?;
		ensure_extended_exits_available()?;
		let dirty_tracking = dirty_tracking_available()?;

		let partition = Partition::new()?;
		set_processor_count(&partition, WHP_PROCESSOR_COUNT)?;
		set_extended_exits(&partition)?;
		partition.set_property_cpuid_exits(CPUID_EXIT_LEAVES)?;
		set_local_apic(&partition)?;
		partition.setup()?;

		let mappings = register_memory(&partition, &memory, dirty_tracking)?;
		let ioapic = Arc::new(Mutex::new(IoApic::new(partition.clone())));
		Ok(Self {
			mappings: Mutex::new(mappings),
			partition,
			ioapic,
			ioevents: Arc::new(Mutex::new(Vec::new())),
			memory,
			supported_cpuid: CpuId::host_supported(),
			dirty_tracking,
		})
	}

	/// Return the guest memory owned by this VM.
	pub const fn memory(&self) -> &GuestMemoryMmap {
		&self.memory
	}

	/// Map writable UEFI flash at its guest physical address.
	///
	/// The returned WHP mapping is retained by the VM until partition teardown;
	/// the caller must retain the backing `GuestMemoryMmap` for the same
	/// lifetime.
	pub fn map_firmware(&self, memory: &GuestMemoryMmap) -> Result<()> {
		for region in memory.iter() {
			let host = HostMem { ptr: region.as_ptr(), len: region.len() as usize };
			let mapping = self.partition.map_gpa_range(
				&host,
				region.start_addr().raw_value(),
				region.len(),
				WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagRead
					| WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagWrite
					| WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagExecute,
			)?;
			self.mappings.lock().push(mapping);
		}
		Ok(())
	}

	/// Return the host-supported CPUID template for x86 vCPU setup.
	pub const fn supported_cpuid(&self) -> &CpuId {
		&self.supported_cpuid
	}

	/// Return the userspace IOAPIC device for MMIO bus registration.
	pub fn ioapic(&self) -> Arc<Mutex<IoApic>> {
		Arc::clone(&self.ioapic)
	}

	/// Register the userspace IOAPIC on the guest MMIO bus.
	#[allow(
		clippy::unnecessary_wraps,
		reason = "device registration shares a fallible cross-hypervisor callsite"
	)]
	pub fn register_ioapic(&self, bus: &mut Bus) -> Result<()> {
		bus.register(u64::from(IO_APIC_DEFAULT_PHYS_BASE), 0x20, self.ioapic());
		Ok(())
	}

	/// Capture VM-global userspace interrupt-controller state.
	pub fn save_machine_state(&self) -> IoApicState {
		self.ioapic.lock().save_state()
	}

	/// Restore VM-global userspace interrupt-controller state.
	pub fn restore_machine_state(&self, state: &IoApicState) -> Result<()> {
		self.ioapic.lock().restore_state(state)
	}

	/// Create a backend-owned vCPU with the given index.
	pub fn create_vcpu(&self, id: u8) -> Result<Vcpu> {
		if u32::from(id) >= WHP_PROCESSOR_COUNT {
			return Err(err(format!(
				"WHP vCPU id {id} exceeds configured processor count {WHP_PROCESSOR_COUNT}"
			)));
		}
		let vp = self.partition.create_virtual_processor(u32::from(id))?;
		Ok(Vcpu::new(vp, self.ioevents.clone()))
	}

	/// Create a triggerable userspace IOAPIC interrupt line for guest GSI `gsi`.
	pub fn irq_line(&self, gsi: u32) -> Result<IrqLine> {
		if !IoApic::supports_gsi(gsi) {
			return Err(err(format!(
				"WHP IOAPIC GSI {gsi} exceeds {} implemented pins",
				IoApic::pin_count()
			)));
		}
		Ok(IrqLine::new(self.ioapic.clone(), gsi))
	}

	/// Wake `evt` when the guest writes an optional `datamatch` at MMIO `addr`.
	pub fn register_ioevent(&self, evt: &EventFd, addr: u64, datamatch: Option<u64>) -> Result<()> {
		self
			.ioevents
			.lock()
			.push(IoEvent { addr, datamatch, evt: evt.try_clone()? });
		Ok(())
	}

	/// Return whether guest writes are recorded by WHP for incremental
	/// snapshots.
	pub const fn dirty_tracking_available(&self) -> bool {
		self.dirty_tracking
	}
}

fn register_memory(
	partition: &Partition,
	memory: &GuestMemoryMmap,
	dirty_tracking: bool,
) -> Result<Vec<GPARangeMapping>> {
	let mut mappings = Vec::new();
	for region in memory.iter() {
		let host = HostMem { ptr: region.as_ptr(), len: region.len() as usize };
		let mut flags = WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagRead
			| WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagWrite
			| WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagExecute;
		if dirty_tracking {
			flags |= WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagTrackDirtyPages;
		}
		mappings.push(partition.map_gpa_range(
			&host,
			region.start_addr().raw_value(),
			region.len(),
			flags,
		)?);
	}
	Ok(mappings)
}

struct HostMem {
	ptr: *mut u8,
	len: usize,
}

impl libwhp::memory::Memory for HostMem {
	fn as_slice_mut(&mut self) -> &mut [u8] {
		// SAFETY: `ptr..ptr+len` names a live guest-memory mmap for the duration
		// of this call.
		unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
	}

	fn as_slice(&self) -> &[u8] {
		// SAFETY: `ptr..ptr+len` names a live guest-memory mmap for the duration
		// of this call.
		unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
	}

	fn as_ptr(&self) -> *const libwhp::VOID {
		self.ptr.cast()
	}

	fn get_size(&self) -> usize {
		self.len
	}
}

fn ensure_hypervisor_present() -> Result<()> {
	let capability: WHV_CAPABILITY =
		get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeHypervisorPresent)?;
	// SAFETY: the union field matches the requested capability code.
	if unsafe { capability.HypervisorPresent } == FALSE {
		return Err(err("Windows Hypervisor Platform is not present or not enabled"));
	}
	Ok(())
}

fn ensure_local_apic_available() -> Result<()> {
	let capability: WHV_CAPABILITY = get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeFeatures)?;
	// SAFETY: the union field matches the requested capability code.
	let features = unsafe { capability.Features };
	if features.LocalApicEmulation() == 0 {
		return Err(err("WHP local APIC emulation is unavailable"));
	}
	Ok(())
}

fn dirty_tracking_available() -> Result<bool> {
	let capability: WHV_CAPABILITY = get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeFeatures)?;
	// SAFETY: the union field matches the requested capability code.
	Ok(unsafe { capability.Features }.DirtyPageTracking() != 0)
}

fn ensure_extended_exits_available() -> Result<()> {
	let capability: WHV_CAPABILITY =
		get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeExtendedVmExits)?;
	// SAFETY: the union field matches the requested capability code.
	let exits = unsafe { capability.ExtendedVmExits };
	if exits.X64CpuidExit() == 0 || exits.X64MsrExit() == 0 {
		return Err(err("WHP CPUID and MSR exits are required but unavailable"));
	}
	Ok(())
}

fn set_processor_count(partition: &Partition, count: u32) -> Result<()> {
	let mut property = WHV_PARTITION_PROPERTY::default();
	property.ProcessorCount = count;
	partition.set_property(
		WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
		&property,
	)?;
	Ok(())
}

fn set_extended_exits(partition: &Partition) -> Result<()> {
	let mut property = WHV_PARTITION_PROPERTY::default();
	// SAFETY: we initialize the `ExtendedVmExits` union arm before passing it to
	// WHP.
	unsafe {
		property.ExtendedVmExits.set_X64CpuidExit(1);
		property.ExtendedVmExits.set_X64MsrExit(1);
	}
	partition.set_property(
		WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeExtendedVmExits,
		&property,
	)?;
	Ok(())
}

fn set_local_apic(partition: &Partition) -> Result<()> {
	let mut property = WHV_PARTITION_PROPERTY::default();
	property.LocalApicEmulationMode =
		WHV_X64_LOCAL_APIC_EMULATION_MODE::WHvX64LocalApicEmulationModeXApic;
	partition.set_property(
		WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeLocalApicEmulationMode,
		&property,
	)?;
	Ok(())
}
