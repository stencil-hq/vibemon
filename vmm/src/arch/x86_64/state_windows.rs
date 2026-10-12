//! Windows x86 vCPU and machine snapshot state for WHP.

use std::collections::BTreeMap;

use libwhp::{WHV_REGISTER_NAME, WHV_REGISTER_VALUE};
use serde::{Deserialize, Serialize};

use crate::{
	hv::{CpuId, IoApicState, Vcpu, Vm},
	result::{Result, err},
};

/// Return the host x86 feature surface in the canonical placement format.
pub fn cpu_baseline() -> Result<String> {
	let cpuid = CpuId::host_supported();
	let mut baseline = BTreeMap::new();
	baseline.insert("v".to_string(), serde_json::json!(1_u32));
	for entry in cpuid.as_slice() {
		match (entry.function, entry.index) {
			(0x1, 0) => {
				baseline.insert("1.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("1.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			(0x7, 0) => {
				baseline.insert("7.0.ebx".to_string(), serde_json::json!(entry.ebx));
				baseline.insert("7.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("7.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			(0xd, 0) => {
				baseline.insert("D.0.eax".to_string(), serde_json::json!(entry.eax));
				baseline.insert("D.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert(
					"xcr0".to_string(),
					serde_json::json!(u64::from(entry.eax) | (u64::from(entry.edx) << 32)),
				);
			},
			(0xd, 1) => {
				baseline.insert("D.1.eax".to_string(), serde_json::json!(entry.eax));
				baseline.insert("D.1.ecx".to_string(), serde_json::json!(entry.ecx));
			},
			(0x8000_0001, 0) => {
				baseline.insert("80000001.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("80000001.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			_ => {},
		}
	}
	Ok(serde_json::to_string(&baseline)?)
}

const REGISTERS: &[WHV_REGISTER_NAME] = &[
	WHV_REGISTER_NAME::WHvX64RegisterRax,
	WHV_REGISTER_NAME::WHvX64RegisterRcx,
	WHV_REGISTER_NAME::WHvX64RegisterRdx,
	WHV_REGISTER_NAME::WHvX64RegisterRbx,
	WHV_REGISTER_NAME::WHvX64RegisterRsp,
	WHV_REGISTER_NAME::WHvX64RegisterRbp,
	WHV_REGISTER_NAME::WHvX64RegisterRsi,
	WHV_REGISTER_NAME::WHvX64RegisterRdi,
	WHV_REGISTER_NAME::WHvX64RegisterR8,
	WHV_REGISTER_NAME::WHvX64RegisterR9,
	WHV_REGISTER_NAME::WHvX64RegisterR10,
	WHV_REGISTER_NAME::WHvX64RegisterR11,
	WHV_REGISTER_NAME::WHvX64RegisterR12,
	WHV_REGISTER_NAME::WHvX64RegisterR13,
	WHV_REGISTER_NAME::WHvX64RegisterR14,
	WHV_REGISTER_NAME::WHvX64RegisterR15,
	WHV_REGISTER_NAME::WHvX64RegisterRip,
	WHV_REGISTER_NAME::WHvX64RegisterRflags,
	WHV_REGISTER_NAME::WHvX64RegisterEs,
	WHV_REGISTER_NAME::WHvX64RegisterCs,
	WHV_REGISTER_NAME::WHvX64RegisterSs,
	WHV_REGISTER_NAME::WHvX64RegisterDs,
	WHV_REGISTER_NAME::WHvX64RegisterFs,
	WHV_REGISTER_NAME::WHvX64RegisterGs,
	WHV_REGISTER_NAME::WHvX64RegisterLdtr,
	WHV_REGISTER_NAME::WHvX64RegisterTr,
	WHV_REGISTER_NAME::WHvX64RegisterIdtr,
	WHV_REGISTER_NAME::WHvX64RegisterGdtr,
	WHV_REGISTER_NAME::WHvX64RegisterCr0,
	WHV_REGISTER_NAME::WHvX64RegisterCr2,
	WHV_REGISTER_NAME::WHvX64RegisterCr3,
	WHV_REGISTER_NAME::WHvX64RegisterCr4,
	WHV_REGISTER_NAME::WHvX64RegisterCr8,
	WHV_REGISTER_NAME::WHvX64RegisterDr0,
	WHV_REGISTER_NAME::WHvX64RegisterDr1,
	WHV_REGISTER_NAME::WHvX64RegisterDr2,
	WHV_REGISTER_NAME::WHvX64RegisterDr3,
	WHV_REGISTER_NAME::WHvX64RegisterDr6,
	WHV_REGISTER_NAME::WHvX64RegisterDr7,
	WHV_REGISTER_NAME::WHvX64RegisterXCr0,
	WHV_REGISTER_NAME::WHvX64RegisterXmm0,
	WHV_REGISTER_NAME::WHvX64RegisterXmm1,
	WHV_REGISTER_NAME::WHvX64RegisterXmm2,
	WHV_REGISTER_NAME::WHvX64RegisterXmm3,
	WHV_REGISTER_NAME::WHvX64RegisterXmm4,
	WHV_REGISTER_NAME::WHvX64RegisterXmm5,
	WHV_REGISTER_NAME::WHvX64RegisterXmm6,
	WHV_REGISTER_NAME::WHvX64RegisterXmm7,
	WHV_REGISTER_NAME::WHvX64RegisterXmm8,
	WHV_REGISTER_NAME::WHvX64RegisterXmm9,
	WHV_REGISTER_NAME::WHvX64RegisterXmm10,
	WHV_REGISTER_NAME::WHvX64RegisterXmm11,
	WHV_REGISTER_NAME::WHvX64RegisterXmm12,
	WHV_REGISTER_NAME::WHvX64RegisterXmm13,
	WHV_REGISTER_NAME::WHvX64RegisterXmm14,
	WHV_REGISTER_NAME::WHvX64RegisterXmm15,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx0,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx1,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx2,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx3,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx4,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx5,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx6,
	WHV_REGISTER_NAME::WHvX64RegisterFpMmx7,
	WHV_REGISTER_NAME::WHvX64RegisterFpControlStatus,
	WHV_REGISTER_NAME::WHvX64RegisterXmmControlStatus,
	WHV_REGISTER_NAME::WHvX64RegisterTsc,
	WHV_REGISTER_NAME::WHvX64RegisterEfer,
	WHV_REGISTER_NAME::WHvX64RegisterKernelGsBase,
	WHV_REGISTER_NAME::WHvX64RegisterApicBase,
	WHV_REGISTER_NAME::WHvX64RegisterPat,
	WHV_REGISTER_NAME::WHvX64RegisterSysenterCs,
	WHV_REGISTER_NAME::WHvX64RegisterSysenterEip,
	WHV_REGISTER_NAME::WHvX64RegisterSysenterEsp,
	WHV_REGISTER_NAME::WHvX64RegisterStar,
	WHV_REGISTER_NAME::WHvX64RegisterLstar,
	WHV_REGISTER_NAME::WHvX64RegisterCstar,
	WHV_REGISTER_NAME::WHvX64RegisterSfmask,
	WHV_REGISTER_NAME::WHvX64RegisterTscAux,
	WHV_REGISTER_NAME::WHvX64RegisterSpecCtrl,
	WHV_REGISTER_NAME::WHvRegisterPendingEvent,
	WHV_REGISTER_NAME::WHvX64RegisterDeliverabilityNotifications,
	WHV_REGISTER_NAME::WHvRegisterInternalActivityState,
];

/// Complete WHP register and LAPIC state for one vCPU.
#[derive(Serialize, Deserialize, Clone)]
pub struct VcpuState {
	pub cpuid:       CpuId,
	pub registers:   Vec<[u64; 2]>,
	pub lapic:       Vec<u8>,
	pub misc_enable: u64,
}

/// VM-global userspace IOAPIC state.
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {
	pub ioapic: IoApicState,
}

/// Capture one paused WHP vCPU.
pub fn save_vcpu(vcpu: &Vcpu, _: usize) -> Result<VcpuState> {
	let mut values = vec![WHV_REGISTER_VALUE::default(); REGISTERS.len()];
	vcpu.whp_vp().get_registers(REGISTERS, &mut values)?;
	let registers = values
		.iter()
		.map(|value| {
			// SAFETY: every WHP register value occupies the common 128-bit union
			// storage.
			let value = unsafe { value.Reg128 };
			[value.Low64, value.High64]
		})
		.collect();
	let lapic = vcpu.whp_vp().get_lapic()?;
	let lapic = lapic.regs.iter().map(|byte| *byte as u8).collect();
	Ok(VcpuState {
		cpuid: vcpu.cpuid_state(),
		registers,
		lapic,
		misc_enable: vcpu.misc_enable_state(),
	})
}

/// Restore one WHP vCPU.
pub fn restore_vcpu(vcpu: &Vcpu, state: &VcpuState) -> Result<()> {
	if state.registers.len() != REGISTERS.len() {
		return Err(err(format!(
			"WHP snapshot has {} registers, expected {}",
			state.registers.len(),
			REGISTERS.len()
		)));
	}
	if state.lapic.len() != 4096 {
		return Err(err(format!("WHP snapshot LAPIC is {} bytes, expected 4096", state.lapic.len())));
	}
	vcpu.set_cpuid_state(&state.cpuid);
	vcpu.set_misc_enable_state(state.misc_enable);
	let values: Vec<WHV_REGISTER_VALUE> = state
		.registers
		.iter()
		.map(|value| WHV_REGISTER_VALUE {
			Reg128: libwhp::WHV_UINT128 { Low64: value[0], High64: value[1] },
		})
		.collect();
	vcpu.whp_vp().set_registers(REGISTERS, &values)?;
	let mut lapic = libwhp::LapicStateRaw::default();
	for (dst, src) in lapic.regs.iter_mut().zip(&state.lapic) {
		*dst = *src as i8;
	}
	vcpu.whp_vp().set_lapic(&lapic)?;
	Ok(())
}

/// Capture VM-global WHP state.
#[allow(
	clippy::unnecessary_wraps,
	reason = "the architecture-neutral snapshot path uses one fallible machine-state interface"
)]
pub fn save_machine(vm: &Vm) -> Result<MachineState> {
	Ok(MachineState { ioapic: vm.save_machine_state() })
}

/// Restore VM-global WHP state.
pub fn restore_machine(vm: &Vm, state: &MachineState) -> Result<()> {
	vm.restore_machine_state(&state.ioapic)
}
