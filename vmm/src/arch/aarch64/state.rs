//! Backend-neutral aarch64 snapshot state types.
//!
//! Backend-specific capture and replay live under `crate::hv::{kvm,hvf}`; this
//! module is only the serialized form shared by snapshot code.

use serde::{Deserialize, Serialize};

/// Return the non-x86 restore-compatibility token.
///
/// aarch64 has a single restore class, so this never fails; it mirrors the
/// fallible `x86_64` KVM CPUID probe's signature so `main` can call it
/// uniformly.
#[allow(clippy::unnecessary_wraps, reason = "signature parity with the x86_64 KVM probe")]
pub fn cpu_baseline() -> crate::result::Result<String> {
	Ok("arch:aarch64".to_string())
}

/// Serialized per-vCPU aarch64 state, tagged by the backend that captured it.
///
/// The variant discriminates the on-disk schema, so a restore on the wrong
/// backend fails the `match` rather than silently decoding incompatible bytes.
#[derive(Serialize, Deserialize, Clone)]
pub enum VcpuState {
	/// KVM-captured state: the full `ONE_REG` set plus MP/event blobs.
	Kvm {
		/// `(reg_id, little-endian bytes)` for every `KVM_GET_REG_LIST` entry.
		regs:        Vec<(u64, Vec<u8>)>,
		/// Raw `kvm_mp_state` bytes.
		mp_state:    Vec<u8>,
		/// Raw `kvm_vcpu_events` bytes.
		vcpu_events: Vec<u8>,
	},
	/// HVF-captured state: curated GP/system/ICC registers, FP/SIMD registers,
	/// PSCI power state, and vtimer controls.
	Hvf {
		/// `(index, value)`; `0..=30` = X0..X30, `31` = PC, `32` = CPSR,
		/// `33` = FPCR, `34` = FPSR.
		regs:           Vec<(u8, u64)>,
		/// `(encoding, value)` for the curated migratable system registers.
		sys_regs:       Vec<(crate::hv::SysReg, u64)>,
		/// `(encoding, value)` for the GIC CPU-interface registers.
		icc_regs:       Vec<(u64, u64)>,
		/// `(index, value)`; `0..=31` = Q0..Q31 as little-endian 128-bit lanes.
		simd_fp_regs:   Vec<(u8, u128)>,
		/// PSCI powered/runnable state for this vCPU.
		powered:        bool,
		/// Guest virtual counter (`CNTVCT_EL0`) value the paused VM was frozen
		/// at, identical on every vCPU. A restore continues the counter from
		/// here, so time between capture and restore is not guest time.
		vtimer_counter: u64,
		/// Whether the virtual timer is masked.
		vtimer_masked:  bool,
	},
}

/// Serialized aarch64 interrupt-controller state.
#[derive(Serialize, Deserialize, Clone)]
pub enum GicState {
	/// KVM `GICv3` device attrs as `(group, attr, value)` triples.
	Kvm { entries: Vec<(u32, u64, u64)> },
	/// HVF GIC state blob returned by Hypervisor.framework.
	Hvf { blob: Vec<u8> },
}

/// VM-wide aarch64 machine state.
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {
	/// Serialized interrupt-controller state.
	pub gic: GicState,
}
