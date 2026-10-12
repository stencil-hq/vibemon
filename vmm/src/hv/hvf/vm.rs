//! HVF VM creation, RAM registration, vCPU creation and ioevent routing.

use std::sync::{Arc, Mutex, atomic::AtomicBool};

use applevisor::prelude::{
	GicConfig, GicEnabled, SysReg as HvSysReg, Vcpu as HvVcpuApi, VcpuHandle, VirtualMachine,
	VirtualMachineConfig, VirtualMachineInstance,
};
use tracing::warn;

use crate::{
	hv::hvf::{
		clock::GuestClock,
		gic::{Gic, IrqLine},
		memory::map_guest_memory,
		psci::PsciCoordinator,
		vcpu::Vcpu,
	},
	memory::{GuestMemoryMmap, create_guest_memory},
	os::EventFd,
	result::{Result, err},
};

/// A userspace ioeventfd registration handled by the HVF run loop.
pub struct IoEvent {
	pub(crate) addr:      u64,
	pub(crate) datamatch: Option<u64>,
	pub(crate) evt:       EventFd,
}

/// Shared ioevent table consulted by every HVF vCPU.
pub type IoEvents = Arc<Mutex<Vec<IoEvent>>>;

/// Cloneable HVF VM pieces needed by the owning vCPU thread to create its vCPU.
#[allow(dead_code, reason = "part of public API platform abstraction")]
#[derive(Clone)]
pub struct VcpuFactory {
	vm:       VirtualMachineInstance<GicEnabled>,
	psci:     Arc<PsciCoordinator>,
	ioevents: IoEvents,
	clock:    Arc<GuestClock>,
}

/// Owns the HVF VM handle, guest RAM and shared backend state.
pub struct Vm {
	instance: VirtualMachineInstance<GicEnabled>,
	memory:   GuestMemoryMmap,
	ioevents: IoEvents,
	gic:      Mutex<Option<Gic>>,
	psci:     Arc<PsciCoordinator>,
	clock:    Arc<GuestClock>,
}

impl Vm {
	/// Create a VM with freshly allocated guest RAM.
	pub fn new(mem_size: usize) -> Result<Self> {
		Self::with_memory(create_guest_memory(mem_size)?)
	}

	/// Create a VM around an existing guest-memory mapping.
	pub fn with_memory(memory: GuestMemoryMmap) -> Result<Self> {
		let vm_cfg = VirtualMachineConfig::default();

		let mut gic_cfg = GicConfig::default();
		gic_cfg.set_distributor_base(crate::layout::GIC_DIST_BASE)?;
		gic_cfg.set_redistributor_base(crate::layout::GIC_REDIST_BASE)?;

		let instance = VirtualMachine::with_gic(vm_cfg, gic_cfg).map_err(|e| {
			err(format!(
				"creating HVF VM with GIC failed: {e}; on macOS this usually means the process lacks \
				 the Hypervisor entitlement or Hypervisor.framework is unavailable"
			))
		})?;
		Self::register_memory(&memory)?;
		let max_vcpus = HvVcpuApi::get_max_count().map_err(|e| {
			err(format!(
				"querying HVF vCPU limit failed: {e}; on macOS this can indicate missing Hypervisor \
				 entitlement"
			))
		})? as usize;
		Ok(Self {
			instance,
			memory,
			ioevents: Arc::new(Mutex::new(Vec::new())),
			gic: Mutex::new(None),
			psci: Arc::new(PsciCoordinator::new(max_vcpus)),
			clock: Arc::new(GuestClock::running(0)),
		})
	}

	/// Return the VM guest-memory mapping.
	pub const fn memory(&self) -> &GuestMemoryMmap {
		&self.memory
	}

	/// Register every memory region with Hypervisor.framework.
	pub fn register_memory(memory: &GuestMemoryMmap) -> Result<()> {
		map_guest_memory(memory)
	}

	/// HVF has no guest-write dirty log; live-migration deltas fall back to a
	/// full parallel page diff on this backend.
	#[allow(
		clippy::unused_self,
		clippy::unnecessary_wraps,
		reason = "mirrors the fallible KVM surface so call sites stay monomorphic"
	)]
	pub fn arm_dirty_tracking(&self) -> Result<bool> {
		Ok(false)
	}

	/// Always `None` on HVF (see [`Self::arm_dirty_tracking`]).
	#[allow(
		clippy::unused_self,
		clippy::unnecessary_wraps,
		reason = "mirrors the fallible KVM surface so call sites stay monomorphic"
	)]
	pub fn take_dirty_log(&self) -> Result<Option<Vec<Vec<u64>>>> {
		Ok(None)
	}

	/// Create a vCPU on the caller's thread and report when it is inside HVF.
	pub fn create_vcpu(&self, id: u8, hv_run: Arc<AtomicBool>) -> Result<Vcpu> {
		create_vcpu(
			&self.instance,
			self.psci.clone(),
			self.ioevents.clone(),
			self.clock.clone(),
			id,
			hv_run,
		)
	}

	/// Freeze the guest counter at the value the snapshot's vCPUs were
	/// captured at, before they are restored; each vCPU then resumes it from
	/// there (see [`GuestClock`]).
	pub fn restore_guest_clock(&self, vcpus: &[crate::arch::state::VcpuState]) -> Result<()> {
		self
			.clock
			.restore(crate::hv::hvf::state::captured_guest_counter(vcpus)?);
		Ok(())
	}

	/// Return a triggerable GIC SPI interrupt line.
	pub fn irq_line(&self, gsi: u32) -> Result<IrqLine> {
		let gic = self
			.gic
			.lock()
			.map_err(|_| err("HVF GIC mutex poisoned"))?
			.clone()
			.ok_or_else(|| err("HVF GIC has not been created"))?;
		gic.irq_line(gsi)
	}

	/// Record an ioeventfd-style MMIO doorbell handled by the vCPU run loop.
	pub fn register_ioevent(&self, evt: &EventFd, addr: u64, datamatch: Option<u64>) -> Result<()> {
		self
			.ioevents
			.lock()
			.map_err(|_| err("HVF ioevent table poisoned"))?
			.push(IoEvent { addr, datamatch, evt: evt.try_clone()? });
		Ok(())
	}

	/// Wrap the GIC created at VM creation time after all vCPUs exist.
	pub fn create_gic(&self, num_cpus: u64) -> Result<Gic> {
		let gic = Gic::new(self.instance.clone(), num_cpus);
		*self.gic.lock().map_err(|_| err("HVF GIC mutex poisoned"))? = Some(gic.clone());
		Ok(gic)
	}

	/// Return a cloneable vCPU factory for thread-local HVF vCPU creation.
	#[allow(dead_code, reason = "part of public API platform abstraction")]
	pub fn vcpu_factory(&self) -> VcpuFactory {
		VcpuFactory {
			vm:       self.instance.clone(),
			psci:     self.psci.clone(),
			ioevents: self.ioevents.clone(),
			clock:    self.clock.clone(),
		}
	}

	/// Build a closure that exits the supplied HVF vCPUs from another thread.
	#[allow(dead_code, reason = "part of public API platform abstraction")]
	pub fn vcpu_kicker(&self, handles: Vec<VcpuHandle>) -> Arc<dyn Fn() + Send + Sync> {
		let vm = self.instance.clone();
		Arc::new(move || {
			if let Err(e) = vm.vcpus_exit(&handles) {
				warn!("failed to exit HVF vCPUs: {e}");
			}
		})
	}
}

impl VcpuFactory {
	/// Create a thread-local HVF vCPU with the VM's shared PSCI/ioevent state.
	#[allow(dead_code, reason = "part of public API platform abstraction")]
	pub fn create_vcpu(&self, id: u8, hv_run: Arc<AtomicBool>) -> Result<Vcpu> {
		create_vcpu(
			&self.vm,
			self.psci.clone(),
			self.ioevents.clone(),
			self.clock.clone(),
			id,
			hv_run,
		)
	}
}

fn create_vcpu(
	vm: &VirtualMachineInstance<GicEnabled>,
	psci: Arc<PsciCoordinator>,
	ioevents: IoEvents,
	clock: Arc<GuestClock>,
	id: u8,
	hv_run: Arc<AtomicBool>,
) -> Result<Vcpu> {
	let inner = vm.vcpu_create().map_err(|e| {
		err(format!(
			"creating HVF vCPU {id} failed: {e}; on macOS this can indicate missing Hypervisor \
			 entitlement"
		))
	})?;
	let mpidr = mpidr_for_id(id);
	inner.set_sys_reg(HvSysReg::MPIDR_EL1, mpidr)?;
	psci.register_mpidr(usize::from(id), mpidr)?;
	// Every vCPU of the VM shares one guest counter from the start.
	inner.set_vtimer_offset(clock.offset())?;
	Ok(Vcpu::new(id, inner, psci, ioevents, clock, hv_run))
}
fn mpidr_for_id(id: u8) -> u64 {
	let id = u64::from(id);
	let aff0 = id & 0xf;
	let aff1 = (id >> 4) & 0xff;
	let aff2 = (id >> 12) & 0xff;
	aff0 | (aff1 << 8) | (aff2 << 16)
}
