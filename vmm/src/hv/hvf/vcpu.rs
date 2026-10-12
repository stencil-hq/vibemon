//! HVF vCPU wrapper, MMIO decode/completion and PSCI handling.

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use applevisor::prelude::{ExitReason, GicIccReg, Reg, SysReg as HvSysReg};

use crate::{
	bail,
	hv::{
		Exit, SysReg,
		hvf::{
			clock::GuestClock,
			psci::{
				PSCI_AFFINITY_INFO_32, PSCI_AFFINITY_INFO_64, PSCI_CPU_OFF, PSCI_CPU_ON_32,
				PSCI_CPU_ON_64, PSCI_FEATURES, PSCI_MIGRATE_INFO_TYPE, PSCI_NOT_SUPPORTED,
				PSCI_SUCCESS, PSCI_SYSTEM_OFF, PSCI_SYSTEM_RESET, PSCI_VERSION, PSCI_VERSION_1_0,
				PsciCoordinator,
			},
			vm::IoEvents,
		},
	},
	result::{Result, err},
};

const EC_WFI_WFE: u64 = 0x01;
const EC_HVC: u64 = 0x16;
const EC_SMC: u64 = 0x17;
const EC_SYSREG: u64 = 0x18;
const EC_DATA_ABORT_LOWER_EL: u64 = 0x24;
const ISS_ISV: u64 = 1 << 24;
const ISS_SAS_SHIFT: u64 = 22;
const ISS_SRT_SHIFT: u64 = 16;
const ISS_SF: u64 = 1 << 15;
const ISS_SSE: u64 = 1 << 21;
const ISS_WNR: u64 = 1 << 6;
const ISS_SYSREG_OP0_SHIFT: u64 = 20;
const ISS_SYSREG_OP1_SHIFT: u64 = 14;
const ISS_SYSREG_CRN_SHIFT: u64 = 10;
const ISS_SYSREG_OP2_SHIFT: u64 = 17;
const ISS_SYSREG_RT_SHIFT: u64 = 5;
const ISS_SYSREG_CRM_SHIFT: u64 = 1;
const ISS_SYSREG_READ: u64 = 1;
const OSLAR_EL1: SysReg = SysReg::new(2, 0, 1, 0, 4);
const OSLSR_EL1: SysReg = SysReg::new(2, 0, 1, 1, 4);
const OSDLR_EL1: SysReg = SysReg::new(2, 0, 1, 3, 4);
const PSTATE_EL1H_MASKED: u64 = 0x3c5;
const PSCI_FUNCTION_MASK: u64 = 0xffff_ffff;
const POWERED_OFF_POLL: Duration = Duration::from_millis(100);
/// How long an idle (WFI/WFE) vCPU sleeps before re-entering the guest so the
/// virtual timer and pending device interrupts can be delivered.
const WFI_POLL: Duration = Duration::from_micros(500);

#[derive(Clone, Copy)]
enum MmioSignExtend {
	None,
	Signed32,
	Signed64,
}

#[derive(Clone, Copy)]
struct PendingMmio {
	dest:        u8,
	size:        usize,
	is_read:     bool,
	sign_extend: MmioSignExtend,
}

struct HvRunGuard<'a>(&'a AtomicBool);

impl<'a> HvRunGuard<'a> {
	fn enter(state: &'a AtomicBool) -> Self {
		state.store(true, Ordering::SeqCst);
		Self(state)
	}
}

impl Drop for HvRunGuard<'_> {
	fn drop(&mut self) {
		self.0.store(false, Ordering::SeqCst);
	}
}

/// A thread-owned HVF vCPU with userspace MMIO completion state.
pub struct Vcpu {
	id:           u8,
	inner:        applevisor::prelude::Vcpu,
	psci:         Arc<PsciCoordinator>,
	ioevents:     IoEvents,
	pending_mmio: Option<PendingMmio>,
	mmio_buf:     [u8; 8],
	/// True only while this thread is inside `hv_vcpu_run`; the pause kicker
	/// must not target a vCPU parked or running host-side emulation.
	hv_run:       Arc<AtomicBool>,
	/// The VM's guest counter, shared by every vCPU (see `freeze_vtimer`).
	clock:        Arc<GuestClock>,
}

impl Vcpu {
	/// Create a backend vCPU wrapper around an applevisor vCPU.
	pub(crate) const fn new(
		id: u8,
		inner: applevisor::prelude::Vcpu,
		psci: Arc<PsciCoordinator>,
		ioevents: IoEvents,
		clock: Arc<GuestClock>,
		hv_run: Arc<AtomicBool>,
	) -> Self {
		Self { id, inner, psci, ioevents, pending_mmio: None, mmio_buf: [0; 8], hv_run, clock }
	}

	/// Run the vCPU until HVF exits to userspace.
	pub fn run(&mut self) -> Result<Exit<'_>> {
		self.apply_pending_mmio()?;

		if !self.psci.is_powered(usize::from(self.id)) {
			let Some(start) = self
				.psci
				.wait_for_power_on_timeout(usize::from(self.id), POWERED_OFF_POLL)?
			else {
				return Ok(Exit::Continue);
			};
			self.set_pc(start.entry)?;
			self.set_gpr(0, start.context_id)?;
			self.set_pstate(PSTATE_EL1H_MASKED)?;
		}

		// Unmask the virtual timer before entering the guest so it can fire
		// again; we mask it on each VTIMER_ACTIVATED exit to break the storm,
		// and the in-kernel GIC delivers PPI 27 to the guest. The guest's own
		// CNTV_CTL governs whether the timer is actually armed.
		self.inner.set_vtimer_mask(false)?;
		{
			let _run = HvRunGuard::enter(&self.hv_run);
			self.inner.run()?;
		}
		let exit = self.inner.get_exit_info();
		match exit.reason {
			ExitReason::CANCELED => Ok(Exit::Continue),
			ExitReason::VTIMER_ACTIVATED => {
				self.inner.set_vtimer_mask(true)?;
				Ok(Exit::Continue)
			},
			ExitReason::UNKNOWN => {
				let pc = self.inner.get_reg(Reg::PC).unwrap_or(0);
				bail!("HVF: UNKNOWN exit reason at pc={pc:#x}");
			},
			ExitReason::EXCEPTION => {
				let syndrome = exit.exception.syndrome;
				let ec = (syndrome >> 26) & 0x3f;
				match ec {
					EC_DATA_ABORT_LOWER_EL => {
						self.handle_data_abort(syndrome, exit.exception.physical_address)
					},
					EC_HVC | EC_SMC => self.handle_psci(),
					EC_SYSREG => self.handle_sys_reg_trap(syndrome),
					EC_WFI_WFE => {
						// The guest is idle waiting for an interrupt. HVF traps
						// WFI/WFE and reports PC already past it, so we must let
						// real time advance: sleep briefly so the (unmasked)
						// virtual timer deadline can pass and device-asserted
						// SPIs become pending, then re-enter and HVF/the
						// in-kernel GIC delivers the interrupt. The bounded
						// sleep caps interrupt latency while idle.
						std::thread::sleep(WFI_POLL);
						Ok(Exit::Continue)
					},
					_ => {
						let pc = self.inner.get_reg(Reg::PC).unwrap_or(0);
						bail!(
							"HVF: unhandled exception EC={ec:#x} syndrome={syndrome:#x} pc={pc:#x} \
							 far={:#x}",
							exit.exception.virtual_address
						);
					},
				}
			},
		}
	}

	/// Whether the guest is inside an interrupt handler: an acknowledged
	/// interrupt whose priority has not been dropped (`ICC_AP{0,1}R0_EL1`).
	///
	/// A snapshot captured in that window restores unreliably: the in-kernel
	/// GIC sometimes never drops the restored active priority when the guest
	/// EOIs, so the vCPU stops taking device interrupts. The run loop
	/// therefore only parks a pausing vCPU outside handlers.
	pub(crate) fn mid_interrupt(&self) -> bool {
		[GicIccReg::AP0R0_EL1, GicIccReg::AP1R0_EL1]
			.into_iter()
			.any(|reg| self.inner.get_icc_reg(reg).is_ok_and(|value| value != 0))
	}

	/// Mark this vCPU as the initially powered-on boot CPU.
	pub fn init_boot(&self) -> Result<()> {
		let mpidr = self.inner.get_sys_reg(HvSysReg::MPIDR_EL1)?;
		self.psci.register_mpidr(usize::from(self.id), mpidr)?;
		self.psci.mark_booted(usize::from(self.id))
	}

	/// Mark this vCPU as powered off until a guest PSCI `CPU_ON` call.
	pub fn init_secondary(&self) -> Result<()> {
		let mpidr = self.inner.get_sys_reg(HvSysReg::MPIDR_EL1)?;
		self.psci.register_mpidr(usize::from(self.id), mpidr)?;
		self.psci.mark_off(usize::from(self.id))
	}

	/// Return whether PSCI currently considers this vCPU powered/runnable.
	pub(crate) fn is_powered(&self) -> bool {
		self.psci.is_powered(usize::from(self.id))
	}

	/// Restore this vCPU's PSCI powered/runnable state from a snapshot.
	pub(crate) fn set_powered(&self, powered: bool) -> Result<()> {
		if powered {
			self.psci.mark_booted(usize::from(self.id))
		} else {
			self.psci.mark_off(usize::from(self.id))
		}
	}

	/// Set the guest program counter.
	pub fn set_pc(&self, v: u64) -> Result<()> {
		self.inner.set_reg(Reg::PC, v)?;
		Ok(())
	}

	/// Set one aarch64 general-purpose register X0..X30.
	pub fn set_gpr(&self, idx: u8, v: u64) -> Result<()> {
		self.inner.set_reg(gpr_reg(idx)?, v)?;
		Ok(())
	}

	/// Read one aarch64 general-purpose register X0..X30.
	pub fn get_gpr(&self, idx: u8) -> Result<u64> {
		Ok(self.inner.get_reg(gpr_reg(idx)?)?)
	}

	/// Set the current PSTATE/CPSR value.
	pub fn set_pstate(&self, v: u64) -> Result<()> {
		self.inner.set_reg(Reg::CPSR, v)?;
		Ok(())
	}

	/// Read a backend-neutral aarch64 system register.
	pub fn get_sys_reg(&self, r: SysReg) -> Result<u64> {
		Ok(self.inner.get_sys_reg(sys_reg_to_hvf(r)?)?)
	}

	/// Write a backend-neutral aarch64 system register.
	#[allow(dead_code, reason = "part of public API platform abstraction")]
	pub fn set_sys_reg(&self, r: SysReg, v: u64) -> Result<()> {
		self.inner.set_sys_reg(sys_reg_to_hvf(r)?, v)?;
		Ok(())
	}

	/// Complete a userspace MMIO exit after the bus access has been handled.
	pub(crate) fn complete_mmio(&mut self) -> Result<()> {
		self.apply_pending_mmio()
	}

	/// Read the host virtual counter (`CNTVCT_EL0`) — the same timebase the HVF
	/// vtimer offset is expressed in, so deltas need no unit conversion.
	fn host_cntvct() -> u64 {
		let cnt: u64;
		// SAFETY: CNTVCT_EL0 is unconditionally readable at EL0 on aarch64.
		unsafe { std::arch::asm!("mrs {}, cntvct_el0", out(reg) cnt, options(nostack, nomem)) };
		cnt
	}

	/// Freeze the VM's guest counter as this vCPU parks for a pause. HVF keeps
	/// the virtual counter running while a vCPU is parked (unlike KVM, which
	/// stops it), so without this the guest would see the entire pause as a
	/// forward `CNTVCT` jump on resume and stall or panic seconds later.
	pub(crate) fn freeze_vtimer(&self) {
		self.clock.freeze(Self::host_cntvct());
	}

	/// Resume the VM's guest counter as this vCPU leaves a pause (or starts
	/// from a restored snapshot): apply the offset every vCPU shares, which
	/// continues the counter from where it was frozen.
	pub(crate) fn thaw_vtimer(&self) -> Result<()> {
		self
			.inner
			.set_vtimer_offset(self.clock.thaw(Self::host_cntvct()))?;
		Ok(())
	}

	/// The guest counter value the VM is frozen at while paused.
	pub(crate) fn frozen_guest_counter(&self) -> Result<u64> {
		self
			.clock
			.frozen()
			.ok_or_else(|| err("HVF vCPU state captured while the guest counter is running"))
	}

	/// Save HVF vCPU state into the backend-neutral snapshot form.
	pub fn save_state(&mut self, _xsave_size: usize) -> Result<crate::arch::state::VcpuState> {
		self.apply_pending_mmio()?;
		crate::hv::hvf::state::save_vcpu(self)
	}

	/// Restore HVF vCPU state from the backend-neutral snapshot form.
	pub fn restore_state(&self, st: &crate::arch::state::VcpuState) -> Result<()> {
		crate::hv::hvf::state::restore_vcpu(self, st)
	}

	/// Return a shareable handle that can force this vCPU out of `run`.
	#[allow(dead_code, reason = "part of public API platform abstraction")]
	pub fn handle(&self) -> applevisor::prelude::VcpuHandle {
		self.inner.get_handle()
	}

	/// Return the Hypervisor.framework vCPU id used for cross-thread force-exit.
	pub fn hv_vcpu_id(&self) -> u64 {
		self.inner.id()
	}

	/// Return the underlying applevisor vCPU for backend-local helpers.
	pub(crate) const fn inner(&self) -> &applevisor::prelude::Vcpu {
		&self.inner
	}

	fn handle_data_abort(&mut self, syndrome: u64, addr: u64) -> Result<Exit<'_>> {
		let iss = syndrome & 0x01ff_ffff;
		if iss & ISS_ISV == 0 {
			bail!("HVF data abort has ISV=0 at IPA {addr:#x}, syndrome {syndrome:#x}");
		}
		let size = 1usize << ((iss >> ISS_SAS_SHIFT) & 0x3);
		let srt = ((iss >> ISS_SRT_SHIFT) & 0x1f) as u8;
		let is_write = iss & ISS_WNR != 0;
		let sign_extend = if iss & ISS_SSE == 0 {
			MmioSignExtend::None
		} else if iss & ISS_SF == 0 {
			MmioSignExtend::Signed32
		} else {
			MmioSignExtend::Signed64
		};
		self.mmio_buf = [0; 8];

		if is_write {
			let value = if srt == 31 { 0 } else { self.get_gpr(srt)? };
			self.mmio_buf[..size].copy_from_slice(&value.to_le_bytes()[..size]);
			if self.signal_matching_ioevent(addr, size)? {
				self.advance_pc()?;
				return Ok(Exit::Continue);
			}
			self.pending_mmio = Some(PendingMmio {
				dest: srt,
				size,
				is_read: false,
				sign_extend: MmioSignExtend::None,
			});
			Ok(Exit::MmioWrite { addr, data: &self.mmio_buf[..size] })
		} else {
			self.pending_mmio = Some(PendingMmio { dest: srt, size, is_read: true, sign_extend });
			Ok(Exit::MmioRead { addr, data: &mut self.mmio_buf[..size] })
		}
	}

	fn handle_sys_reg_trap(&self, syndrome: u64) -> Result<Exit<'static>> {
		let iss = syndrome & 0x01ff_ffff;
		let reg = SysReg::new(
			((iss >> ISS_SYSREG_OP0_SHIFT) & 0x3) as u8,
			((iss >> ISS_SYSREG_OP1_SHIFT) & 0x7) as u8,
			((iss >> ISS_SYSREG_CRN_SHIFT) & 0xf) as u8,
			((iss >> ISS_SYSREG_CRM_SHIFT) & 0xf) as u8,
			((iss >> ISS_SYSREG_OP2_SHIFT) & 0x7) as u8,
		);
		let is_read = iss & ISS_SYSREG_READ != 0;
		let rt = ((iss >> ISS_SYSREG_RT_SHIFT) & 0x1f) as u8;

		// Linux clears the architectural debug OS locks during hw-breakpoint
		// setup. HVF traps those lock registers, so model them as unlocked.
		if matches!(reg, OSLAR_EL1 | OSLSR_EL1 | OSDLR_EL1) {
			if is_read && rt != 31 {
				self.set_gpr(rt, 0)?;
			}
			self.advance_pc()?;
			return Ok(Exit::Continue);
		}

		let pc = self.inner.get_reg(Reg::PC).unwrap_or(0);
		let access = if is_read { "read" } else { "write" };
		bail!(
			"HVF: unhandled system register {access} S{}_{}_C{}_C{}_{} rt={} syndrome={syndrome:#x} \
			 pc={pc:#x}",
			reg.op0,
			reg.op1,
			reg.crn,
			reg.crm,
			reg.op2,
			rt,
		);
	}

	fn handle_psci(&self) -> Result<Exit<'static>> {
		let function = self.get_gpr(0)? & PSCI_FUNCTION_MASK;
		let ret = match function {
			PSCI_VERSION => PSCI_VERSION_1_0 as i64,
			PSCI_CPU_ON_32 => {
				let target = self.get_gpr(1)? as u32 as u64;
				let entry = self.get_gpr(2)? as u32 as u64;
				let context_id = self.get_gpr(3)? as u32 as u64;
				self.psci.power_on(target, entry, context_id)
			},
			PSCI_CPU_ON_64 => {
				let target = self.get_gpr(1)?;
				let entry = self.get_gpr(2)?;
				let context_id = self.get_gpr(3)?;
				self.psci.power_on(target, entry, context_id)
			},
			PSCI_CPU_OFF => {
				self.psci.power_off(usize::from(self.id))?;
				PSCI_SUCCESS
			},
			PSCI_AFFINITY_INFO_32 => {
				let target = self.get_gpr(1)? as u32 as u64;
				self.psci.affinity_info(target) as i64
			},
			PSCI_AFFINITY_INFO_64 => {
				let target = self.get_gpr(1)?;
				self.psci.affinity_info(target) as i64
			},
			PSCI_SYSTEM_OFF => return Ok(Exit::Shutdown),
			PSCI_SYSTEM_RESET => return Ok(Exit::SystemEvent(0)),
			PSCI_FEATURES => Self::psci_features(self.get_gpr(1)? & PSCI_FUNCTION_MASK) as i64,
			PSCI_MIGRATE_INFO_TYPE => 2,
			_ => PSCI_NOT_SUPPORTED,
		};
		// Per ARMv8 the exception return address for HVC/SMC is the instruction
		// *after* the call, so HVF already reports PC past the HVC — unlike a
		// data abort (faulting-instruction PC). Do NOT advance here.
		self.set_gpr(0, ret as u64)?;
		Ok(Exit::Continue)
	}

	const fn psci_features(function: u64) -> u64 {
		match function {
			PSCI_VERSION
			| PSCI_CPU_OFF
			| PSCI_CPU_ON_32
			| PSCI_CPU_ON_64
			| PSCI_AFFINITY_INFO_32
			| PSCI_AFFINITY_INFO_64
			| PSCI_SYSTEM_OFF
			| PSCI_SYSTEM_RESET
			| PSCI_FEATURES
			| PSCI_MIGRATE_INFO_TYPE => 0,
			_ => PSCI_NOT_SUPPORTED as u64,
		}
	}

	fn signal_matching_ioevent(&self, addr: u64, size: usize) -> Result<bool> {
		let mut bytes = [0u8; 8];
		bytes[..size].copy_from_slice(&self.mmio_buf[..size]);
		let value = u64::from_le_bytes(bytes);
		let table = self
			.ioevents
			.lock()
			.map_err(|_| err("HVF ioevent table poisoned"))?;
		for event in table.iter() {
			if event.addr == addr && event.datamatch.is_none_or(|datamatch| datamatch == value) {
				event.evt.write(1)?;
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn apply_pending_mmio(&mut self) -> Result<()> {
		let Some(pending) = self.pending_mmio.take() else {
			return Ok(());
		};
		if pending.is_read && pending.dest != 31 {
			let mut bytes = [0u8; 8];
			bytes[..pending.size].copy_from_slice(&self.mmio_buf[..pending.size]);
			let value = extend_mmio_load(u64::from_le_bytes(bytes), pending.size, pending.sign_extend);
			self.set_gpr(pending.dest, value)?;
		}
		self.advance_pc()
	}

	fn advance_pc(&self) -> Result<()> {
		let pc = self.inner.get_reg(Reg::PC)?;
		self.inner.set_reg(Reg::PC, pc.wrapping_add(4))?;
		Ok(())
	}
}

const fn extend_mmio_load(value: u64, size: usize, sign_extend: MmioSignExtend) -> u64 {
	match sign_extend {
		MmioSignExtend::None => value,
		MmioSignExtend::Signed32 => sign_extend_to_u64(value, size) as u32 as u64,
		MmioSignExtend::Signed64 => sign_extend_to_u64(value, size),
	}
}

const fn sign_extend_to_u64(value: u64, size: usize) -> u64 {
	let shift = 64 - (size * 8);
	((value << shift) as i64 >> shift) as u64
}

fn gpr_reg(idx: u8) -> Result<Reg> {
	Ok(match idx {
		0 => Reg::X0,
		1 => Reg::X1,
		2 => Reg::X2,
		3 => Reg::X3,
		4 => Reg::X4,
		5 => Reg::X5,
		6 => Reg::X6,
		7 => Reg::X7,
		8 => Reg::X8,
		9 => Reg::X9,
		10 => Reg::X10,
		11 => Reg::X11,
		12 => Reg::X12,
		13 => Reg::X13,
		14 => Reg::X14,
		15 => Reg::X15,
		16 => Reg::X16,
		17 => Reg::X17,
		18 => Reg::X18,
		19 => Reg::X19,
		20 => Reg::X20,
		21 => Reg::X21,
		22 => Reg::X22,
		23 => Reg::X23,
		24 => Reg::X24,
		25 => Reg::X25,
		26 => Reg::X26,
		27 => Reg::X27,
		28 => Reg::X28,
		29 => Reg::X29,
		30 => Reg::X30,
		_ => return Err(err(format!("invalid GPR index {idx}"))),
	})
}

pub fn sys_reg_to_hvf(r: SysReg) -> Result<HvSysReg> {
	let enc = ((r.op0 as u16) << 14)
		| ((r.op1 as u16) << 11)
		| ((r.crn as u16) << 7)
		| ((r.crm as u16) << 3)
		| (r.op2 as u16);
	Ok(match enc {
		0xc005 => HvSysReg::MPIDR_EL1,
		0xc080 => HvSysReg::SCTLR_EL1,
		0xc081 => HvSysReg::ACTLR_EL1,
		0xc082 => HvSysReg::CPACR_EL1,
		0xc100 => HvSysReg::TTBR0_EL1,
		0xc101 => HvSysReg::TTBR1_EL1,
		0xc102 => HvSysReg::TCR_EL1,
		0xc108 => HvSysReg::APIAKEYLO_EL1,
		0xc109 => HvSysReg::APIAKEYHI_EL1,
		0xc10a => HvSysReg::APIBKEYLO_EL1,
		0xc10b => HvSysReg::APIBKEYHI_EL1,
		0xc110 => HvSysReg::APDAKEYLO_EL1,
		0xc111 => HvSysReg::APDAKEYHI_EL1,
		0xc112 => HvSysReg::APDBKEYLO_EL1,
		0xc113 => HvSysReg::APDBKEYHI_EL1,
		0xc118 => HvSysReg::APGAKEYLO_EL1,
		0xc119 => HvSysReg::APGAKEYHI_EL1,
		0xc200 => HvSysReg::SPSR_EL1,
		0xc201 => HvSysReg::ELR_EL1,
		0xc208 => HvSysReg::SP_EL0,
		0xc288 => HvSysReg::AFSR0_EL1,
		0xc289 => HvSysReg::AFSR1_EL1,
		0xc290 => HvSysReg::ESR_EL1,
		0xc300 => HvSysReg::FAR_EL1,
		0xc3a0 => HvSysReg::PAR_EL1,
		0xc510 => HvSysReg::MAIR_EL1,
		0xc518 => HvSysReg::AMAIR_EL1,
		0xc600 => HvSysReg::VBAR_EL1,
		0xc681 => HvSysReg::CONTEXTIDR_EL1,
		0xc684 => HvSysReg::TPIDR_EL1,
		0xc708 => HvSysReg::CNTKCTL_EL1,
		0xd000 => HvSysReg::CSSELR_EL1,
		0xde82 => HvSysReg::TPIDR_EL0,
		0xde83 => HvSysReg::TPIDRRO_EL0,
		0xdf19 => HvSysReg::CNTV_CTL_EL0,
		0xdf1a => HvSysReg::CNTV_CVAL_EL0,
		0xe208 => HvSysReg::SP_EL1,
		_ => return Err(err(format!("unsupported HVF sysreg encoding {enc:#x}"))),
	})
}
