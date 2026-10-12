//! WHP vCPU wrapper, run-loop exits and `x86_64` boot register setup.

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use libwhp::{
	E_FAIL, HRESULT, S_OK, VirtualProcessor, WHV_GUEST_PHYSICAL_ADDRESS, WHV_GUEST_VIRTUAL_ADDRESS,
	WHV_MEMORY_ACCESS_CONTEXT, WHV_REGISTER_NAME, WHV_REGISTER_VALUE, WHV_RUN_VP_EXIT_CONTEXT,
	WHV_RUN_VP_EXIT_REASON, WHV_TRANSLATE_GVA_FLAGS, WHV_TRANSLATE_GVA_RESULT_CODE,
	WHV_X64_FP_CONTROL_STATUS_REGISTER, WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_struct,
	WHV_X64_IO_PORT_ACCESS_CONTEXT, WHV_X64_PENDING_EVENT_TYPE, WHV_X64_PENDING_EXCEPTION_EVENT,
	WHV_X64_SEGMENT_REGISTER, WHV_X64_TABLE_REGISTER, WHV_X64_XMM_CONTROL_STATUS_REGISTER,
	WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_struct,
	instruction_emulator::{
		Emulator, EmulatorCallbacks, WHV_EMULATOR_IO_ACCESS_INFO, WHV_EMULATOR_MEMORY_ACCESS_INFO,
		WHV_EMULATOR_STATUS,
	},
};
use parking_lot::Mutex;
use vm_memory::{Address, Bytes, GuestAddress, GuestMemory, GuestMemoryRegion};

use super::{CpuId, vm::IoEvents};
use crate::{
	devices::Bus,
	hv::Exit,
	layout::{
		BOOT_GDT_OFFSET, BOOT_IDT_OFFSET, BOOT_STACK_POINTER, PDE_START, PDPTE_START, PML4_START,
		ZERO_PAGE_START,
	},
	memory::GuestMemoryMmap,
	result::{Result, err},
};

const LEAF_FEATURE_INFO: u32 = 0x1;
const LEAF_EXTENDED_TOPOLOGY: u32 = 0xb;
const LEAF_V2_EXTENDED_TOPOLOGY: u32 = 0x1f;
const ECX_HYPERVISOR_BIT: u32 = 1 << 31;
const BOOT_GDT_MAX: usize = 4;
const EFER_LMA: u64 = 0x400;
const EFER_LME: u64 = 0x100;
const X86_CR0_PE: u64 = 1;
const X86_CR0_MP: u64 = 1 << 1;
const X86_CR0_ET: u64 = 1 << 4;
const X86_CR0_NE: u64 = 1 << 5;
const X86_CR0_WP: u64 = 1 << 16;
const X86_CR0_AM: u64 = 1 << 18;
const X86_CR0_PG: u64 = 1 << 31;
const X86_CR0_RESET: u64 = 0x6000_0010;
const X86_CR4_PAE: u64 = 1 << 5;
const X86_CR4_OSFXSR: u64 = 1 << 9;
const X86_CR4_OSXMMEXCPT: u64 = 1 << 10;
const MSR_IA32_SYSENTER_CS: u32 = 0x174;
const MSR_IA32_SYSENTER_ESP: u32 = 0x175;
const MSR_IA32_SYSENTER_EIP: u32 = 0x176;
const MSR_IA32_TSC: u32 = 0x10;
const MSR_IA32_APICBASE: u32 = 0x1b;
const MSR_IA32_MISC_ENABLE: u32 = 0x1a0;
const MSR_IA32_MISC_ENABLE_FAST_STRING: u64 = 0x1;
const MSR_IA32_CR_PAT: u32 = 0x277;
const MSR_STAR: u32 = 0xc000_0081;
const MSR_LSTAR: u32 = 0xc000_0082;
const MSR_CSTAR: u32 = 0xc000_0083;
const MSR_SYSCALL_MASK: u32 = 0xc000_0084;
const MSR_KERNEL_GS_BASE: u32 = 0xc000_0102;
const MSR_IA32_TSC_AUX: u32 = 0xc000_0103;
const APIC_LVT0: usize = 0x350;
const APIC_LVT1: usize = 0x360;
const APIC_MODE_NMI: u32 = 0x4;
const APIC_MODE_EXTINT: u32 = 0x7;
const BOOT_CR0: u64 =
	X86_CR0_PE | X86_CR0_MP | X86_CR0_ET | X86_CR0_NE | X86_CR0_WP | X86_CR0_AM | X86_CR0_PG;
const BOOT_CR4: u64 = X86_CR4_PAE | X86_CR4_OSFXSR | X86_CR4_OSXMMEXCPT;
const WHP_DIRTY_PAGE_SIZE: u64 = 4096;

/// Cloneable WHP virtual processor with userspace emulation state.
pub struct Vcpu {
	inner:       Arc<VirtualProcessor>,
	cancel:      VcpuCancel,
	ioevents:    IoEvents,
	cpuid:       Arc<Mutex<CpuId>>,
	misc_enable: Arc<AtomicU64>,
}

#[derive(Clone)]
struct VcpuCancel {
	vp: Arc<VirtualProcessor>,
}

impl Clone for Vcpu {
	fn clone(&self) -> Self {
		Self {
			inner:       self.inner.clone(),
			cancel:      self.cancel.clone(),
			ioevents:    self.ioevents.clone(),
			cpuid:       self.cpuid.clone(),
			misc_enable: self.misc_enable.clone(),
		}
	}
}

impl Vcpu {
	/// Wrap a newly-created WHP virtual processor.
	pub(super) fn new(inner: VirtualProcessor, ioevents: IoEvents) -> Self {
		let inner = Arc::new(inner);
		let cancel = VcpuCancel { vp: inner.clone() };
		Self {
			inner,
			cancel,
			ioevents,
			cpuid: Arc::new(Mutex::new(CpuId::default())),
			misc_enable: Arc::new(AtomicU64::new(MSR_IA32_MISC_ENABLE_FAST_STRING)),
		}
	}

	/// Return the underlying WHP virtual processor for Windows-specific setup
	/// code.
	pub fn whp_vp(&self) -> &VirtualProcessor {
		self.inner.as_ref()
	}

	/// Force a running WHP vCPU to return from `WHvRunVirtualProcessor`.
	pub fn kick(&self) {
		self.cancel.cancel();
	}

	/// Run this vCPU once, emulating PIO/MMIO exits against the supplied buses.
	pub fn run_whp(&self, pio_bus: &Bus, mmio_bus: &Bus) -> Result<Exit<'static>> {
		let exit_context = self.inner.run()?;
		match exit_context.ExitReason {
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonMemoryAccess => {
				// SAFETY: `MemoryAccess` is valid for this exit reason.
				let access = unsafe { &exit_context.anon_union.MemoryAccess };
				self.emulate_mmio(&exit_context, access, pio_bus, mmio_bus)?;
				Ok(Exit::Continue)
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64IoPortAccess => {
				// SAFETY: `IoPortAccess` is valid for this exit reason.
				let access = unsafe { &exit_context.anon_union.IoPortAccess };
				self.emulate_io(&exit_context, access, pio_bus, mmio_bus)?;
				Ok(Exit::Continue)
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64Cpuid => {
				self.handle_cpuid(&exit_context)?;
				Ok(Exit::Continue)
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64MsrAccess => {
				self.handle_msr(&exit_context)?;
				Ok(Exit::Continue)
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64Halt => Ok(Exit::Hlt),
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonCanceled => Ok(Exit::Continue),
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonUnrecoverableException => Ok(Exit::Shutdown),
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonInvalidVpRegisterValue => {
				Err(err(format!("WHP rejected vCPU {} register state", self.inner.index())))
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonUnsupportedFeature => {
				// SAFETY: `UnsupportedFeature` is valid for this exit reason.
				let feature = unsafe { exit_context.anon_union.UnsupportedFeature };
				Err(err(format!(
					"WHP unsupported feature {:?} parameter {:#x}",
					feature.FeatureCode, feature.FeatureParameter
				)))
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonException => {
				// SAFETY: `VpException` is valid for this exit reason.
				let exception = unsafe { exit_context.anon_union.VpException };
				Err(err(format!(
					"WHP vCPU exception type {} error {:#x}",
					exception.ExceptionType, exception.ErrorCode
				)))
			},
			WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64InterruptWindow
			| WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64ApicEoi
			| WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonNone => Ok(Exit::Other),
		}
	}

	/// Store the per-vCPU CPUID overrides applied on top of WHP's filtered
	/// default results.
	#[allow(
		clippy::unnecessary_wraps,
		reason = "x86 boot setup uses the same fallible CPUID interface on KVM and WHP"
	)]
	pub fn set_cpuid_template(&self, base: &CpuId, cpu_id: u8) -> Result<()> {
		let mut cpuid = base.clone();
		for entry in cpuid.as_mut_slice() {
			match entry.function {
				LEAF_FEATURE_INFO => {
					entry.ebx = (entry.ebx & 0x00ff_ffff) | (u32::from(cpu_id) << 24);
					entry.ecx |= ECX_HYPERVISOR_BIT;
				},
				LEAF_EXTENDED_TOPOLOGY | LEAF_V2_EXTENDED_TOPOLOGY => entry.edx = u32::from(cpu_id),
				0x4000_0000 => set_hypervisor_vendor(entry),
				_ => {},
			}
		}
		*self.cpuid.lock() = cpuid;
		Ok(())
	}

	/// Clone the CPUID surface currently presented to this vCPU.
	pub fn cpuid_state(&self) -> CpuId {
		self.cpuid.lock().clone()
	}

	/// Replace the CPUID surface presented to this vCPU.
	pub fn set_cpuid_state(&self, cpuid: &CpuId) {
		*self.cpuid.lock() = cpuid.clone();
	}

	/// Return the userspace-emulated `IA32_MISC_ENABLE` value.
	pub(crate) fn misc_enable_state(&self) -> u64 {
		self.misc_enable.load(Ordering::SeqCst)
	}

	/// Restore the userspace-emulated `IA32_MISC_ENABLE` value.
	pub(crate) fn set_misc_enable_state(&self, value: u64) {
		self.misc_enable.store(value, Ordering::SeqCst);
	}

	/// Fetch and clear WHP's guest-write bitmap for every RAM region.
	pub(crate) fn take_dirty_log(&self, memory: &GuestMemoryMmap) -> Result<Vec<Vec<u64>>> {
		let mut logs = Vec::new();
		for region in memory.iter() {
			let pages = region.len().div_ceil(WHP_DIRTY_PAGE_SIZE);
			let bytes = u32::try_from(pages.div_ceil(64) * std::mem::size_of::<u64>() as u64)
				.map_err(|_| err("WHP dirty bitmap size overflows u32"))?;
			let start = region.start_addr().raw_value();
			let log = self
				.inner
				.query_gpa_range_dirty_bitmap(start, region.len(), bytes)?;
			self
				.inner
				.query_gpa_range_dirty_bitmap(start, region.len(), 0)?;
			logs.push(log.into_vec());
		}
		Ok(logs)
	}

	/// Clear WHP's guest-write bitmap for every RAM region.
	pub(crate) fn clear_dirty_log(&self, memory: &GuestMemoryMmap) -> Result<()> {
		for region in memory.iter() {
			self.inner.query_gpa_range_dirty_bitmap(
				region.start_addr().raw_value(),
				region.len(),
				0,
			)?;
		}
		Ok(())
	}

	/// Write the boot MSR values into the vCPU.
	pub fn setup_msrs(&self) -> Result<()> {
		self
			.misc_enable
			.store(MSR_IA32_MISC_ENABLE_FAST_STRING, Ordering::SeqCst);
		self.set_msr_register(MSR_IA32_SYSENTER_CS, 0)?;
		self.set_msr_register(MSR_IA32_SYSENTER_ESP, 0)?;
		self.set_msr_register(MSR_IA32_SYSENTER_EIP, 0)?;
		self.set_msr_register(MSR_STAR, 0)?;
		self.set_msr_register(MSR_CSTAR, 0)?;
		self.set_msr_register(MSR_LSTAR, 0)?;
		self.set_msr_register(MSR_SYSCALL_MASK, 0)?;
		self.set_msr_register(MSR_KERNEL_GS_BASE, 0)?;
		self.set_msr_register(MSR_IA32_TSC, 0)?;
		Ok(())
	}

	/// Configure LINT0 = `ExtINT` and LINT1 = NMI.
	pub fn set_lint(&self) -> Result<()> {
		let mut lapic = self.inner.get_lapic()?;
		let lvt0 = get_lapic_reg(&lapic, APIC_LVT0);
		let lvt1 = get_lapic_reg(&lapic, APIC_LVT1);
		set_lapic_reg(&mut lapic, APIC_LVT0, set_apic_delivery_mode(lvt0, APIC_MODE_EXTINT));
		set_lapic_reg(&mut lapic, APIC_LVT1, set_apic_delivery_mode(lvt1, APIC_MODE_NMI));
		self.inner.set_lapic(&lapic)?;
		Ok(())
	}

	/// Configure the x87/SSE control words to sane reset defaults.
	pub fn setup_fpu(&self) -> Result<()> {
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterFpControlStatus,
			WHV_REGISTER_NAME::WHvX64RegisterXmmControlStatus,
		];
		let mut fp = WHV_X64_FP_CONTROL_STATUS_REGISTER::default();
		fp.anon_struct =
			WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_struct { FpControl: 0x37f, ..Default::default() };
		let mut xmm = WHV_X64_XMM_CONTROL_STATUS_REGISTER::default();
		xmm.anon_struct = WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_struct {
			XmmStatusControl: 0x1f80,
			XmmStatusControlMask: 0xffff,
			..Default::default()
		};
		let mut values = [WHV_REGISTER_VALUE::default(), WHV_REGISTER_VALUE::default()];
		values[0].FpControlStatus = fp;
		values[1].XmmControlStatus = xmm;
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	/// Set the general-purpose registers for the boot entry point.
	pub fn setup_regs(&self, boot_ip: u64) -> Result<()> {
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterRflags,
			WHV_REGISTER_NAME::WHvX64RegisterRip,
			WHV_REGISTER_NAME::WHvX64RegisterRsp,
			WHV_REGISTER_NAME::WHvX64RegisterRbp,
			WHV_REGISTER_NAME::WHvX64RegisterRsi,
		];
		let values = [
			reg64(0x0000_0000_0000_0002),
			reg64(boot_ip),
			reg64(BOOT_STACK_POINTER),
			reg64(BOOT_STACK_POINTER),
			reg64(ZERO_PAGE_START),
		];
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	/// Set the architectural reset-vector general-purpose register state for
	/// UEFI.
	pub fn setup_uefi_regs(&self, entry: u64) -> Result<()> {
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterRflags,
			WHV_REGISTER_NAME::WHvX64RegisterRip,
			WHV_REGISTER_NAME::WHvX64RegisterRsp,
			WHV_REGISTER_NAME::WHvX64RegisterRbp,
			WHV_REGISTER_NAME::WHvX64RegisterRsi,
		];
		let values = [reg64(2), reg64(entry & 0xffff), reg64(0), reg64(0), reg64(0)];
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	/// Configure real-mode segment and control-register state for a UEFI reset.
	pub fn setup_uefi_sregs(&self) -> Result<()> {
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterCs,
			WHV_REGISTER_NAME::WHvX64RegisterDs,
			WHV_REGISTER_NAME::WHvX64RegisterEs,
			WHV_REGISTER_NAME::WHvX64RegisterFs,
			WHV_REGISTER_NAME::WHvX64RegisterGs,
			WHV_REGISTER_NAME::WHvX64RegisterSs,
			WHV_REGISTER_NAME::WHvX64RegisterCr0,
			WHV_REGISTER_NAME::WHvX64RegisterCr3,
			WHV_REGISTER_NAME::WHvX64RegisterCr4,
			WHV_REGISTER_NAME::WHvX64RegisterEfer,
		];
		let data = reset_segment(0, 0, 3);
		let values = [
			segment_reg(reset_segment(0xf000, 0xffff_0000, 0xb)),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			reg64(X86_CR0_RESET),
			reg64(0),
			reg64(0),
			reg64(0),
		];
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	/// Hold an application processor until the guest sends its startup IPI.
	pub fn suspend_for_startup(&self) -> Result<()> {
		self
			.inner
			.set_registers(&[WHV_REGISTER_NAME::WHvRegisterInternalActivityState], &[reg64(1)])?;
		Ok(())
	}

	/// Configure segment/control registers and install boot page tables.
	pub fn setup_sregs(&self, mem: &GuestMemoryMmap) -> Result<()> {
		write_boot_tables(mem)?;
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterGdtr,
			WHV_REGISTER_NAME::WHvX64RegisterIdtr,
			WHV_REGISTER_NAME::WHvX64RegisterCs,
			WHV_REGISTER_NAME::WHvX64RegisterDs,
			WHV_REGISTER_NAME::WHvX64RegisterEs,
			WHV_REGISTER_NAME::WHvX64RegisterFs,
			WHV_REGISTER_NAME::WHvX64RegisterGs,
			WHV_REGISTER_NAME::WHvX64RegisterSs,
			WHV_REGISTER_NAME::WHvX64RegisterTr,
			WHV_REGISTER_NAME::WHvX64RegisterCr3,
			WHV_REGISTER_NAME::WHvX64RegisterCr4,
			WHV_REGISTER_NAME::WHvX64RegisterCr0,
			WHV_REGISTER_NAME::WHvX64RegisterEfer,
		];
		let data = data_segment(2);
		let values = [
			table_reg(BOOT_GDT_OFFSET, (std::mem::size_of::<u64>() * BOOT_GDT_MAX - 1) as u16),
			table_reg(BOOT_IDT_OFFSET, (std::mem::size_of::<u64>() - 1) as u16),
			segment_reg(code_segment(1)),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			segment_reg(data),
			segment_reg(tss_segment(3)),
			reg64(PML4_START),
			reg64(BOOT_CR4),
			reg64(BOOT_CR0),
			reg64(EFER_LME | EFER_LMA),
		];
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	/// Snapshot this WHP vCPU's architectural state.
	pub fn save_state(&self, xsave_size: usize) -> Result<crate::arch::state::VcpuState> {
		crate::arch::state::save_vcpu(self, xsave_size)
	}

	/// Restore this WHP vCPU's architectural state.
	pub fn restore_state(&self, state: &crate::arch::state::VcpuState) -> Result<()> {
		crate::arch::state::restore_vcpu(self, state)
	}

	fn emulate_mmio(
		&self,
		exit_context: &WHV_RUN_VP_EXIT_CONTEXT,
		access: &WHV_MEMORY_ACCESS_CONTEXT,
		pio_bus: &Bus,
		mmio_bus: &Bus,
	) -> Result<()> {
		let emulator = Emulator::<RunCallbacks>::new()?;
		let mut callbacks =
			RunCallbacks::new(self.inner.as_ref(), pio_bus, mmio_bus, self.ioevents.clone());
		let status = emulator.try_mmio_emulation(&mut callbacks, &exit_context.VpContext, access)?;
		finish_emulation(status, callbacks.error)
	}

	fn emulate_io(
		&self,
		exit_context: &WHV_RUN_VP_EXIT_CONTEXT,
		access: &WHV_X64_IO_PORT_ACCESS_CONTEXT,
		pio_bus: &Bus,
		mmio_bus: &Bus,
	) -> Result<()> {
		let emulator = Emulator::<RunCallbacks>::new()?;
		let mut callbacks =
			RunCallbacks::new(self.inner.as_ref(), pio_bus, mmio_bus, self.ioevents.clone());
		let status = emulator.try_io_emulation(&mut callbacks, &exit_context.VpContext, access)?;
		finish_emulation(status, callbacks.error)
	}

	fn handle_cpuid(&self, exit_context: &WHV_RUN_VP_EXIT_CONTEXT) -> Result<()> {
		// SAFETY: `CpuidAccess` is valid for this exit reason.
		let access = unsafe { exit_context.anon_union.CpuidAccess };
		let function = access.Rax as u32;
		let defaults = [
			access.DefaultResultRax,
			access.DefaultResultRbx,
			access.DefaultResultRcx,
			access.DefaultResultRdx,
		];
		let entry = self.cpuid.lock().result(function, access.Rcx as u32);
		let [eax, ebx, ecx, edx] = apply_cpuid_override(defaults, function, entry);
		let names = [
			WHV_REGISTER_NAME::WHvX64RegisterRip,
			WHV_REGISTER_NAME::WHvX64RegisterRax,
			WHV_REGISTER_NAME::WHvX64RegisterRbx,
			WHV_REGISTER_NAME::WHvX64RegisterRcx,
			WHV_REGISTER_NAME::WHvX64RegisterRdx,
		];
		let values = [reg64(next_rip(exit_context)), reg64(eax), reg64(ebx), reg64(ecx), reg64(edx)];
		self.inner.set_registers(&names, &values)?;
		Ok(())
	}

	fn handle_msr(&self, exit_context: &WHV_RUN_VP_EXIT_CONTEXT) -> Result<()> {
		// SAFETY: `MsrAccess` is valid for this exit reason.
		let access = unsafe { exit_context.anon_union.MsrAccess };
		if !supports_msr(access.MsrNumber) {
			return self.inject_general_protection();
		}
		if access.AccessInfo.IsWrite() == 1 {
			let value = (access.Rdx << 32) | (access.Rax & 0xffff_ffff);
			self.set_msr_register(access.MsrNumber, value)?;
			self
				.inner
				.set_registers(&[WHV_REGISTER_NAME::WHvX64RegisterRip], &[reg64(next_rip(
					exit_context,
				))])?;
		} else {
			let value = self.get_msr_register(access.MsrNumber)?;
			let names = [
				WHV_REGISTER_NAME::WHvX64RegisterRip,
				WHV_REGISTER_NAME::WHvX64RegisterRax,
				WHV_REGISTER_NAME::WHvX64RegisterRdx,
			];
			let values =
				[reg64(next_rip(exit_context)), reg64(value & 0xffff_ffff), reg64(value >> 32)];
			self.inner.set_registers(&names, &values)?;
		}
		Ok(())
	}

	fn inject_general_protection(&self) -> Result<()> {
		let mut event = WHV_X64_PENDING_EXCEPTION_EVENT::default();
		event.set_EventPending(1);
		event.set_EventType(WHV_X64_PENDING_EVENT_TYPE::WHvX64PendingEventException as u64);
		event.set_DeliverErrorCode(1);
		event.set_Vector(13);
		event.set_ErrorCode(0);
		let mut value = WHV_REGISTER_VALUE::default();
		value.ExceptionEvent = event;
		self
			.inner
			.set_registers(&[WHV_REGISTER_NAME::WHvRegisterPendingEvent], &[value])?;
		Ok(())
	}

	fn set_msr_register(&self, msr: u32, value: u64) -> Result<()> {
		if msr == MSR_IA32_MISC_ENABLE {
			self.misc_enable.store(value, Ordering::SeqCst);
			return Ok(());
		}
		let name = msr_register_name(msr)
			.ok_or_else(|| err(format!("no WHP register mapping for MSR {msr:#x}")))?;
		self.inner.set_registers(&[name], &[reg64(value)])?;
		Ok(())
	}

	fn get_msr_register(&self, msr: u32) -> Result<u64> {
		if msr == MSR_IA32_MISC_ENABLE {
			return Ok(self.misc_enable.load(Ordering::SeqCst));
		}
		let name = msr_register_name(msr)
			.ok_or_else(|| err(format!("no WHP register mapping for MSR {msr:#x}")))?;
		let mut value = [WHV_REGISTER_VALUE::default()];
		self.inner.get_registers(&[name], &mut value)?;
		// SAFETY: the requested register is a 64-bit register.
		Ok(unsafe { value[0].Reg64 })
	}
}

impl VcpuCancel {
	fn cancel(&self) {
		let _ = self.vp.cancel_run();
	}
}

struct RunCallbacks {
	vp:       *const VirtualProcessor,
	pio_bus:  *const Bus,
	mmio_bus: *const Bus,
	ioevents: IoEvents,
	error:    Option<String>,
}

impl RunCallbacks {
	const fn new(vp: &VirtualProcessor, pio_bus: &Bus, mmio_bus: &Bus, ioevents: IoEvents) -> Self {
		Self { vp, pio_bus, mmio_bus, ioevents, error: None }
	}

	fn fail(&mut self, msg: impl Into<String>) -> HRESULT {
		self.error = Some(msg.into());
		E_FAIL
	}

	const fn vp(&self) -> &VirtualProcessor {
		// SAFETY: callbacks are invoked synchronously while `run_whp` keeps `vp`
		// alive.
		unsafe { &*self.vp }
	}

	const fn pio_bus(&self) -> &Bus {
		// SAFETY: callbacks are invoked synchronously while `run_whp` keeps
		// `pio_bus` alive.
		unsafe { &*self.pio_bus }
	}

	const fn mmio_bus(&self) -> &Bus {
		// SAFETY: callbacks are invoked synchronously while `run_whp` keeps
		// `mmio_bus` alive.
		unsafe { &*self.mmio_bus }
	}
}

impl EmulatorCallbacks for RunCallbacks {
	fn io_port(&mut self, io_access: &mut WHV_EMULATOR_IO_ACCESS_INFO) -> HRESULT {
		let len = usize::from(io_access.AccessSize).min(std::mem::size_of::<u32>());
		let port = u64::from(io_access.Port);
		if io_access.Direction == 1 {
			let bytes = io_access.Data.to_le_bytes();
			self.pio_bus().write(port, &bytes[..len]);
		} else {
			let mut bytes = [0xffu8; 4];
			if self.pio_bus().read(port, &mut bytes[..len]) {
				io_access.Data = u32::from_le_bytes(bytes);
			} else {
				io_access.Data = u32::MAX;
			}
		}
		S_OK
	}

	fn memory(&mut self, memory_access: &mut WHV_EMULATOR_MEMORY_ACCESS_INFO) -> HRESULT {
		let len = usize::from(memory_access.AccessSize).min(memory_access.Data.len());
		let addr = memory_access.GpaAddress;
		if memory_access.Direction == 1 {
			let data = &memory_access.Data[..len];
			if self.signal_ioevent(addr, data).is_err() {
				return self.fail(format!("failed to signal WHP ioevent at {addr:#x}"));
			}
			self.mmio_bus().write(addr, data);
		} else {
			let mut data = [0xffu8; 8];
			if self.mmio_bus().read(addr, &mut data[..len]) {
				memory_access.Data[..len].copy_from_slice(&data[..len]);
			} else {
				memory_access.Data[..len].fill(0xff);
			}
		}
		S_OK
	}

	fn get_virtual_processor_registers(
		&mut self,
		register_names: &[WHV_REGISTER_NAME],
		register_values: &mut [WHV_REGISTER_VALUE],
	) -> HRESULT {
		match self.vp().get_registers(register_names, register_values) {
			Ok(()) => S_OK,
			Err(e) => self.fail(format!("WHP get registers failed: {e}")),
		}
	}

	fn set_virtual_processor_registers(
		&mut self,
		register_names: &[WHV_REGISTER_NAME],
		register_values: &[WHV_REGISTER_VALUE],
	) -> HRESULT {
		match self.vp().set_registers(register_names, register_values) {
			Ok(()) => S_OK,
			Err(e) => self.fail(format!("WHP set registers failed: {e}")),
		}
	}

	fn translate_gva_page(
		&mut self,
		gva: WHV_GUEST_VIRTUAL_ADDRESS,
		translate_flags: WHV_TRANSLATE_GVA_FLAGS,
		translation_result: &mut WHV_TRANSLATE_GVA_RESULT_CODE,
		gpa: &mut WHV_GUEST_PHYSICAL_ADDRESS,
	) -> HRESULT {
		match self.vp().translate_gva(gva, translate_flags) {
			Ok((result, translated)) => {
				*translation_result = result.ResultCode;
				*gpa = translated;
				S_OK
			},
			Err(e) => self.fail(format!("WHP GVA translation failed: {e}")),
		}
	}
}

impl RunCallbacks {
	fn signal_ioevent(&self, addr: u64, data: &[u8]) -> Result<()> {
		for event in self.ioevents.lock().iter() {
			if event_matches(event.addr, event.datamatch, addr, data) {
				event.evt.write(1)?;
				break;
			}
		}
		Ok(())
	}
}

fn finish_emulation(status: WHV_EMULATOR_STATUS, callback_error: Option<String>) -> Result<()> {
	if status.EmulationSuccessful() == 1 {
		return Ok(());
	}
	if let Some(error) = callback_error {
		return Err(err(error));
	}
	Err(err(format!("WHP instruction emulation failed: {:#x}", status.AsUINT32)))
}

fn event_matches(event_addr: u64, datamatch: Option<u64>, access_addr: u64, data: &[u8]) -> bool {
	event_addr == access_addr && datamatch.is_none_or(|value| value == data_to_u64(data))
}

fn data_to_u64(data: &[u8]) -> u64 {
	let mut bytes = [0u8; 8];
	let n = data.len().min(bytes.len());
	bytes[..n].copy_from_slice(&data[..n]);
	u64::from_le_bytes(bytes)
}

fn set_hypervisor_vendor(entry: &mut super::CpuIdEntry) {
	entry.eax = 0x4000_0000;
	let vendor = *b"VibeWHP\0\0\0\0\0";
	entry.ebx = u32::from_le_bytes(vendor[0..4].try_into().unwrap());
	entry.ecx = u32::from_le_bytes(vendor[4..8].try_into().unwrap());
	entry.edx = u32::from_le_bytes(vendor[8..12].try_into().unwrap());
}

fn apply_cpuid_override(
	mut defaults: [u64; 4],
	function: u32,
	entry: Option<super::CpuIdEntry>,
) -> [u64; 4] {
	let Some(entry) = entry else {
		return defaults;
	};
	match function {
		LEAF_FEATURE_INFO => {
			defaults[1] = (defaults[1] & 0x00ff_ffff) | (u64::from(entry.ebx) & 0xff00_0000);
			defaults[2] |= u64::from(ECX_HYPERVISOR_BIT);
		},
		LEAF_EXTENDED_TOPOLOGY | LEAF_V2_EXTENDED_TOPOLOGY => {
			defaults[3] = u64::from(entry.edx);
		},
		0x4000_0000 => {
			defaults = [
				u64::from(entry.eax),
				u64::from(entry.ebx),
				u64::from(entry.ecx),
				u64::from(entry.edx),
			];
		},
		_ => {},
	}
	defaults
}

const fn supports_msr(msr: u32) -> bool {
	msr == MSR_IA32_MISC_ENABLE || msr_register_name(msr).is_some()
}

fn next_rip(exit_context: &WHV_RUN_VP_EXIT_CONTEXT) -> u64 {
	exit_context.VpContext.Rip + u64::from(exit_context.VpContext.InstructionLength())
}

fn reg64(value: u64) -> WHV_REGISTER_VALUE {
	let mut reg = WHV_REGISTER_VALUE::default();
	reg.Reg64 = value;
	reg
}

fn segment_reg(value: WHV_X64_SEGMENT_REGISTER) -> WHV_REGISTER_VALUE {
	let mut reg = WHV_REGISTER_VALUE::default();
	reg.Segment = value;
	reg
}

fn table_reg(base: u64, limit: u16) -> WHV_REGISTER_VALUE {
	let mut reg = WHV_REGISTER_VALUE::default();
	reg.Table = WHV_X64_TABLE_REGISTER { Pad: [0; 3], Limit: limit, Base: base };
	reg
}

fn reset_segment(selector: u16, base: u64, type_: u16) -> WHV_X64_SEGMENT_REGISTER {
	let mut seg = WHV_X64_SEGMENT_REGISTER {
		Base:       base,
		Limit:      0xffff,
		Selector:   selector,
		Attributes: 0,
	};
	seg.set_SegmentType(type_);
	seg.set_NonSystemSegment(1);
	seg.set_Present(1);
	seg
}

fn code_segment(index: u16) -> WHV_X64_SEGMENT_REGISTER {
	let mut seg = flat_segment(index);
	seg.set_SegmentType(11);
	seg.set_Long(1);
	seg
}

fn data_segment(index: u16) -> WHV_X64_SEGMENT_REGISTER {
	let mut seg = flat_segment(index);
	seg.set_SegmentType(3);
	seg.set_Default(1);
	seg
}

fn tss_segment(index: u16) -> WHV_X64_SEGMENT_REGISTER {
	let mut seg = WHV_X64_SEGMENT_REGISTER {
		Base:       0,
		Limit:      0xfffff,
		Selector:   index << 3,
		Attributes: 0,
	};
	seg.set_SegmentType(11);
	seg.set_Present(1);
	seg.set_Granularity(1);
	seg
}

fn flat_segment(index: u16) -> WHV_X64_SEGMENT_REGISTER {
	let mut seg = WHV_X64_SEGMENT_REGISTER {
		Base:       0,
		Limit:      0xfffff,
		Selector:   index << 3,
		Attributes: 0,
	};
	seg.set_NonSystemSegment(1);
	seg.set_Present(1);
	seg.set_Granularity(1);
	seg
}

fn write_boot_tables(mem: &GuestMemoryMmap) -> Result<()> {
	let gdt_table: [u64; BOOT_GDT_MAX] = [
		gdt_entry(0, 0, 0),
		gdt_entry(0xa09b, 0, 0xfffff),
		gdt_entry(0xc093, 0, 0xfffff),
		gdt_entry(0x808b, 0, 0xfffff),
	];
	let boot_gdt_addr = GuestAddress(BOOT_GDT_OFFSET);
	for (index, entry) in gdt_table.iter().enumerate() {
		let addr = mem
			.checked_offset(boot_gdt_addr, index * std::mem::size_of::<u64>())
			.ok_or("failed to compute GDT entry address")?;
		mem.write_obj(*entry, addr)?;
	}
	mem.write_obj(0u64, GuestAddress(BOOT_IDT_OFFSET))?;
	mem.write_obj(GuestAddress(PDPTE_START).raw_value() | 0x03, GuestAddress(PML4_START))?;
	mem.write_obj(GuestAddress(PDE_START).raw_value() | 0x03, GuestAddress(PDPTE_START))?;
	for i in 0..512u64 {
		mem.write_obj((i << 21) + 0x83u64, GuestAddress(PDE_START).unchecked_add(i * 8))?;
	}
	Ok(())
}

const fn gdt_entry(flags: u16, base: u32, limit: u32) -> u64 {
	(((base as u64) & 0xff00_0000) << 32)
		| (((flags as u64) & 0x0000_f0ff) << 40)
		| (((limit as u64) & 0x000f_0000) << 32)
		| (((base as u64) & 0x00ff_ffff) << 16)
		| ((limit as u64) & 0x0000_ffff)
}

fn get_lapic_reg(lapic: &libwhp::LapicStateRaw, reg_offset: usize) -> u32 {
	let r = &lapic.regs[reg_offset..reg_offset + 4];
	u32::from_le_bytes([r[0] as u8, r[1] as u8, r[2] as u8, r[3] as u8])
}

fn set_lapic_reg(lapic: &mut libwhp::LapicStateRaw, reg_offset: usize, value: u32) {
	let bytes = value.to_le_bytes();
	let r = &mut lapic.regs[reg_offset..reg_offset + 4];
	for (i, b) in bytes.iter().enumerate() {
		r[i] = *b as i8 as std::os::raw::c_char;
	}
}

const fn set_apic_delivery_mode(reg: u32, mode: u32) -> u32 {
	(reg & !0x700) | (mode << 8)
}

const fn msr_register_name(msr: u32) -> Option<WHV_REGISTER_NAME> {
	Some(match msr {
		MSR_IA32_SYSENTER_CS => WHV_REGISTER_NAME::WHvX64RegisterSysenterCs,
		MSR_IA32_SYSENTER_ESP => WHV_REGISTER_NAME::WHvX64RegisterSysenterEsp,
		MSR_IA32_SYSENTER_EIP => WHV_REGISTER_NAME::WHvX64RegisterSysenterEip,
		MSR_STAR => WHV_REGISTER_NAME::WHvX64RegisterStar,
		MSR_CSTAR => WHV_REGISTER_NAME::WHvX64RegisterCstar,
		MSR_LSTAR => WHV_REGISTER_NAME::WHvX64RegisterLstar,
		MSR_SYSCALL_MASK => WHV_REGISTER_NAME::WHvX64RegisterSfmask,
		MSR_KERNEL_GS_BASE => WHV_REGISTER_NAME::WHvX64RegisterKernelGsBase,
		MSR_IA32_TSC => WHV_REGISTER_NAME::WHvX64RegisterTsc,
		MSR_IA32_APICBASE => WHV_REGISTER_NAME::WHvX64RegisterApicBase,
		MSR_IA32_CR_PAT => WHV_REGISTER_NAME::WHvX64RegisterPat,
		MSR_IA32_TSC_AUX => WHV_REGISTER_NAME::WHvX64RegisterTscAux,
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ioevent_matching_uses_address_and_little_endian_datamatch() {
		assert!(event_matches(0x1000, None, 0x1000, &[]));
		assert!(event_matches(0x1000, Some(0x1234), 0x1000, &[0x34, 0x12]));
		assert!(!event_matches(0x1000, Some(0x1234), 0x1000, &[0x12, 0x34]));
		assert!(!event_matches(0x1000, None, 0x1004, &[]));
	}

	#[test]
	fn cpuid_overrides_only_per_vcpu_fields() {
		let defaults = [1, 0x0055_aa55, 0x10, 4];
		let entry = super::super::CpuIdEntry {
			ebx: 0xab00_0000,
			ecx: ECX_HYPERVISOR_BIT,
			edx: 7,
			..Default::default()
		};
		let feature = apply_cpuid_override(defaults, LEAF_FEATURE_INFO, Some(entry));
		assert_eq!(feature[0], defaults[0]);
		assert_eq!(feature[1], 0xab55_aa55);
		assert_eq!(feature[2], defaults[2] | u64::from(ECX_HYPERVISOR_BIT));
		assert_eq!(feature[3], defaults[3]);

		let topology = apply_cpuid_override(defaults, LEAF_EXTENDED_TOPOLOGY, Some(entry));
		assert_eq!(topology, [defaults[0], defaults[1], defaults[2], 7]);
		assert_eq!(apply_cpuid_override(defaults, 0x7, Some(entry)), defaults);
	}

	#[test]
	fn msr_filter_accepts_only_emulated_or_backed_registers() {
		assert!(supports_msr(MSR_IA32_MISC_ENABLE));
		assert!(supports_msr(MSR_IA32_TSC));
		assert!(!supports_msr(u32::MAX));
	}
}
