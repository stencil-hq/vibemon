//! Boot-time MSR initialization.
//!
//! This is the classic crosvm/kvmtool boot MSR set: zero the SYSENTER/SYSCALL
//! MSRs and the TSC, and enable fast-string operations in `IA32_MISC_ENABLE`
//! (some kernels fault early without it).

#[cfg(target_os = "linux")]
use kvm_bindings::{Msrs, kvm_msr_entry};

#[cfg(target_os = "linux")]
use crate::bail;
use crate::{hv::Vcpu, result::Result};

const MSR_IA32_SYSENTER_CS: u32 = 0x174;
const MSR_IA32_SYSENTER_ESP: u32 = 0x175;
const MSR_IA32_SYSENTER_EIP: u32 = 0x176;
const MSR_STAR: u32 = 0xc000_0081;
const MSR_LSTAR: u32 = 0xc000_0082;
const MSR_CSTAR: u32 = 0xc000_0083;
const MSR_SYSCALL_MASK: u32 = 0xc000_0084;
const MSR_KERNEL_GS_BASE: u32 = 0xc000_0102;
const MSR_IA32_TSC: u32 = 0x10;
const MSR_IA32_MISC_ENABLE: u32 = 0x1a0;
const MSR_IA32_MISC_ENABLE_FAST_STRING: u64 = 0x1;

// Additional MSRs captured for snapshot/restore (the migration set below). Boot
// leaves these at KVM defaults; save/restore round-trips whichever ones
// `get_msrs` reports the running guest actually enabled.
const MSR_IA32_CR_PAT: u32 = 0x277;
const MSR_IA32_TSC_AUX: u32 = 0xc000_0103;
const MSR_IA32_TSC_DEADLINE: u32 = 0x6e0;
const MSR_IA32_APICBASE: u32 = 0x1b;
const MSR_KVM_WALL_CLOCK_NEW: u32 = 0x4b56_4d00;
const MSR_KVM_SYSTEM_TIME_NEW: u32 = 0x4b56_4d01;
const MSR_KVM_ASYNC_PF_EN: u32 = 0x4b56_4d02;
const MSR_KVM_STEAL_TIME: u32 = 0x4b56_4d03;
const MSR_KVM_PV_EOI_EN: u32 = 0x4b56_4d04;
const MSR_KVM_ASYNC_PF_INT: u32 = 0x4b56_4d06;
// Control-flow Enforcement Technology (CET) and supervisor xstate. Fedora 6.x
// guests commonly run with CR4.CET set on modern Intel hosts; restoring CR4
// without these MSRs leaves exception-entry CET state at reset defaults.
const MSR_IA32_U_CET: u32 = 0x6a0;
const MSR_IA32_S_CET: u32 = 0x6a2;
const MSR_IA32_PL0_SSP: u32 = 0x6a4;
const MSR_IA32_PL1_SSP: u32 = 0x6a5;
const MSR_IA32_PL2_SSP: u32 = 0x6a6;
const MSR_IA32_PL3_SSP: u32 = 0x6a7;
const MSR_IA32_INT_SSP_TAB: u32 = 0x6a8;
const MSR_IA32_XSS: u32 = 0xda0;
// The guest kernel locks IA32_FEATURE_CONTROL with VMX enabled at boot. A
// restore without it leaves the vCPU at KVM's reset value 0, and a guest
// hypervisor's VMXON then faults (no /dev/kvm VMs in forks of a template).
// Intel only: AMD hosts do not report it, so `state::save_vcpu`'s supported-MSR
// filter drops it there.
const MSR_IA32_FEAT_CTL: u32 = 0x3a;

/// MSR indices captured for snapshot/restore (and zeroed/seeded at boot).
/// Superset; at save time this is filtered through `KVM_GET_MSR_INDEX_LIST` and
/// only entries that `get_msrs` actually returns are kept.
#[cfg(target_os = "linux")]
pub const MIGRATION_MSRS: &[u32] = &[
	MSR_IA32_SYSENTER_CS,
	MSR_IA32_SYSENTER_ESP,
	MSR_IA32_SYSENTER_EIP,
	MSR_STAR,
	MSR_CSTAR,
	MSR_LSTAR,
	MSR_SYSCALL_MASK,
	MSR_KERNEL_GS_BASE,
	MSR_IA32_TSC,
	MSR_IA32_MISC_ENABLE,
	MSR_IA32_CR_PAT,
	MSR_IA32_TSC_AUX,
	MSR_IA32_TSC_DEADLINE,
	MSR_IA32_APICBASE,
	MSR_KVM_WALL_CLOCK_NEW,
	MSR_KVM_SYSTEM_TIME_NEW,
	MSR_KVM_ASYNC_PF_EN,
	MSR_KVM_STEAL_TIME,
	MSR_KVM_PV_EOI_EN,
	MSR_KVM_ASYNC_PF_INT,
	MSR_IA32_U_CET,
	MSR_IA32_S_CET,
	MSR_IA32_PL0_SSP,
	MSR_IA32_PL1_SSP,
	MSR_IA32_PL2_SSP,
	MSR_IA32_PL3_SSP,
	MSR_IA32_INT_SSP_TAB,
	MSR_IA32_XSS,
	MSR_IA32_FEAT_CTL,
];

#[cfg(target_os = "linux")]
fn entry(index: u32, data: u64) -> kvm_msr_entry {
	kvm_msr_entry { index, data, ..Default::default() }
}

/// Write the boot MSR values into the vCPU.
pub fn setup_msrs(vcpu: &Vcpu) -> Result<()> {
	#[cfg(target_os = "linux")]
	{
		let entries = [
			entry(MSR_IA32_SYSENTER_CS, 0),
			entry(MSR_IA32_SYSENTER_ESP, 0),
			entry(MSR_IA32_SYSENTER_EIP, 0),
			entry(MSR_STAR, 0),
			entry(MSR_CSTAR, 0),
			entry(MSR_LSTAR, 0),
			entry(MSR_SYSCALL_MASK, 0),
			entry(MSR_KERNEL_GS_BASE, 0),
			entry(MSR_IA32_TSC, 0),
			entry(MSR_IA32_MISC_ENABLE, MSR_IA32_MISC_ENABLE_FAST_STRING),
		];
		let msrs = Msrs::from_entries(&entries)?;
		let written = vcpu.fd().set_msrs(&msrs)?;
		if written != entries.len() {
			bail!("set_msrs wrote {written} of {} entries", entries.len());
		}
		Ok(())
	}
	#[cfg(target_os = "windows")]
	{
		vcpu.setup_msrs()
	}
}
