//! `x86_64` vCPU and machine snapshot state.
//!
//! Captures the full migratable KVM state of a paused vCPU and of the
//! VM-global interrupt/timer devices, plus the inverse restore path. We
//! serialize the dynamically sized `Xsave` wrapper so `KVM_XSAVE2` hosts keep
//! the complete xstate image (legacy FPU/SSE, AVX, PKRU, and larger dynamic
//! components such as AMX when present), paired with `kvm_xcrs`.

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::size_of,
	sync::OnceLock,
};

use kvm_bindings::{
	CpuId, KVM_MAX_CPUID_ENTRIES, KVM_STATE_NESTED_FORMAT_SVM, KVM_STATE_NESTED_FORMAT_VMX,
	KVM_STATE_NESTED_GIF_SET, KVM_STATE_NESTED_GUEST_MODE, KVM_STATE_NESTED_RUN_PENDING, Msrs,
	Xsave, kvm_clock_data, kvm_debugregs, kvm_irqchip, kvm_lapic_state, kvm_mp_state, kvm_msr_entry,
	kvm_nested_state, kvm_pit_state2, kvm_regs, kvm_sregs, kvm_vcpu_events, kvm_xcrs, kvm_xsave,
	kvm_xsave2,
};
use kvm_ioctls::{Kvm, KvmNestedStateBuffer, VcpuFd, VmFd};
use serde::{Deserialize, Serialize};
use vmm_sys_util::fam::FamStruct;

use crate::{
	bail,
	hv::Vcpu,
	result::{Result, err},
};

/// Host KVM's advertised MSR index list, captured at VM construction while
/// /dev/kvm is still openable. [`save_vcpu`] runs on the vCPU threads after the
/// Landlock/seccomp filters engage, which deny reopening /dev/kvm — so the list
/// is cached up front via [`cache_supported_msrs`] rather than re-derived
/// there.
static SUPPORTED_MSRS: OnceLock<BTreeSet<u32>> = OnceLock::new();

/// Capture the host's supported MSR index list from an open KVM handle.
///
/// Called from the KVM-owning VM constructor before the sandbox filters engage,
/// so every VM origin — fresh boot, restore, and fork — has the list ready when
/// it is later snapshotted. Idempotent; the first successful call wins.
///
/// # Errors
/// Propagates `KVM_GET_MSR_INDEX_LIST` failures.
pub fn cache_supported_msrs(kvm: &Kvm) -> Result<()> {
	if SUPPORTED_MSRS.get().is_some() {
		return Ok(());
	}
	let set: BTreeSet<u32> = kvm
		.get_msr_index_list()?
		.as_slice()
		.iter()
		.copied()
		.collect();
	let _ = SUPPORTED_MSRS.set(set);
	Ok(())
}

/// The cached supported-MSR set, or an error if it was never primed.
///
/// # Errors
/// Errors if [`cache_supported_msrs`] did not run before the filter phase.
fn supported_msrs() -> Result<&'static BTreeSet<u32>> {
	SUPPORTED_MSRS.get().ok_or_else(|| {
		err("supported MSR index list was not cached before the sandbox filters engaged")
	})
}

/// Return the host x86 restore-compatibility feature surface as canonical JSON.
#[cfg(target_os = "linux")]
pub fn cpu_baseline() -> Result<String> {
	let cpuid = Kvm::new()?.get_supported_cpuid(KVM_MAX_CPUID_ENTRIES)?;
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
				let xcr0 = u64::from(entry.eax) | (u64::from(entry.edx) << 32);
				baseline.insert("xcr0".to_string(), serde_json::json!(xcr0));
			},
			(0xd, 1) => {
				baseline.insert("D.1.eax".to_string(), serde_json::json!(entry.eax));
				baseline.insert("D.1.ecx".to_string(), serde_json::json!(entry.ecx));
			},
			(0x80000001, 0) => {
				baseline.insert("80000001.0.ecx".to_string(), serde_json::json!(entry.ecx));
				baseline.insert("80000001.0.edx".to_string(), serde_json::json!(entry.edx));
			},
			_ => {},
		}
	}
	Ok(serde_json::to_string(&baseline)?)
}

/// Complete migratable state of one x86 vCPU.
#[derive(Serialize, Deserialize, Clone)]
pub struct VcpuState {
	pub cpuid:       CpuId,
	/// Curated migration MSRs as `(index, data)` pairs. Only the MSRs KVM
	/// actually reported are stored (see [`save_vcpu`]); an MSR the running
	/// guest never enabled must not be blindly re-asserted on restore.
	pub msrs:        Vec<(u32, u64)>,
	pub regs:        kvm_regs,
	pub sregs:       kvm_sregs,
	/// Full extended state via XSAVE2 — covers AVX-512/AMX/PKRU on CPUs whose
	/// xstate exceeds the legacy 4096-byte `kvm_xsave` (e.g. Sapphire Rapids).
	/// `Xsave` is a dynamically-sized FAM wrapper around `kvm_xsave2`; the
	/// `with-serde` feature serializes it.
	pub xsave:       Xsave,
	pub xcrs:        kvm_xcrs,
	pub debug_regs:  kvm_debugregs,
	pub lapic:       kvm_lapic_state,
	pub mp_state:    kvm_mp_state,
	pub vcpu_events: kvm_vcpu_events,
}

/// VM-global x86 device state: the master/slave PICs, the IOAPIC, the PIT and
/// the KVM paravirtual clock.
#[derive(Serialize, Deserialize, Clone)]
pub struct MachineState {
	pub pic_master: kvm_irqchip,
	pub pic_slave:  kvm_irqchip,
	pub ioapic:     kvm_irqchip,
	pub pit:        kvm_pit_state2,
	pub clock:      kvm_clock_data,
}

/// [`save_vcpu`] refusal: the guest uses hardware virtualization (it runs VMs
/// of its own), so KVM holds nested state for this vCPU (VMX operation, SVM
/// with a host save area, a running nested guest) that [`VcpuState`] does not
/// carry. A restore without it would corrupt the guest hypervisor, so the
/// snapshot is refused; the control API answers `nested_virtualization_active`.
/// The vmon guest kernel boots with `kvm.enable_virt_at_load=0`, so this
/// clears once the guest's last VM exits.
#[derive(Debug)]
pub struct NestedVirtualizationActive {
	/// What KVM reported.
	pub detail: &'static str,
}

impl std::fmt::Display for NestedVirtualizationActive {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"the guest is using hardware virtualization ({}); snapshots cannot capture nested \
			 virtualization state, stop the guest's virtual machines and retry",
			self.detail
		)
	}
}

impl std::error::Error for NestedVirtualizationActive {}

/// `EFER.SVME`: SVM enabled.
const EFER_SVME: u64 = 1 << 12;
/// `VM_HSAVE_PA`: the SVM host save area, written by a guest hypervisor when
/// it enables SVM and zeroed when it disables it.
const MSR_VM_HSAVE_PA: u32 = 0xc001_0117;

/// Why KVM's nested state for this vCPU cannot be dropped, or `None` when a
/// snapshot without it is exact.
fn nested_virtualization(vcpu: &VcpuFd, sregs: &kvm_sregs) -> Result<Option<&'static str>> {
	let mut state = KvmNestedStateBuffer::empty();
	match vcpu.nested_state(&mut state) {
		Ok(_) => {},
		// No KVM_CAP_NESTED_STATE: the host offers no nested virtualization.
		Err(e) if e.errno() == libc::EINVAL => return Ok(None),
		Err(e) => return Err(e.into()),
	}
	let hsave_pa = if u32::from(state.format) == KVM_STATE_NESTED_FORMAT_SVM {
		let mut msrs =
			Msrs::from_entries(&[kvm_msr_entry { index: MSR_VM_HSAVE_PA, ..Default::default() }])?;
		(vcpu.get_msrs(&mut msrs)? == 1).then(|| msrs.as_slice()[0].data)
	} else {
		None
	};
	Ok(nested_state_in_use(&state, sregs.efer, hsave_pa))
}

/// [`nested_virtualization`] on the raw KVM report. VMX: the vCPU is in VMX
/// operation (`VMXON`). SVM: `EFER.SVME` with a host save area (a guest
/// hypervisor enabled SVM since this VMM started), or `CLGI` in effect; an
/// unreadable `hsave_pa` counts as in use. SVME with a zero save area is what
/// a snapshot taken before `kvm.enable_virt_at_load=0` restores to: nothing
/// is lost by snapshotting it again. Both: a nested guest is running or
/// about to run, or KVM returned state beyond the header.
fn nested_state_in_use(
	state: &KvmNestedStateBuffer,
	efer: u64,
	hsave_pa: Option<u64>,
) -> Option<&'static str> {
	let flags = u32::from(state.flags);
	if flags & (KVM_STATE_NESTED_GUEST_MODE | KVM_STATE_NESTED_RUN_PENDING) != 0 {
		return Some("a nested guest is running");
	}
	if state.size as usize > size_of::<kvm_nested_state>() {
		return Some("KVM holds nested guest state");
	}
	match u32::from(state.format) {
		KVM_STATE_NESTED_FORMAT_VMX => {
			// SAFETY: the VMX format selects the `vmx` header; every bit
			// pattern of it is valid.
			let hdr = unsafe { state.hdr.vmx };
			(hdr.vmxon_pa != u64::MAX || hdr.smm.flags != 0).then_some("VMX operation is on")
		},
		KVM_STATE_NESTED_FORMAT_SVM => {
			if flags & KVM_STATE_NESTED_GIF_SET == 0 {
				Some("the SVM global interrupt flag is clear")
			} else {
				(efer & EFER_SVME != 0 && hsave_pa != Some(0)).then_some("SVM is enabled")
			}
		},
		_ => Some("unknown nested state format"),
	}
}

/// Read the complete migratable state of a (paused) vCPU.
///
/// Fails with [`NestedVirtualizationActive`] while the guest uses hardware
/// virtualization.
pub fn save_vcpu(vcpu: &Vcpu, xsave_size: usize) -> Result<VcpuState> {
	let vcpu = vcpu.fd();
	// KVM_GET_MP_STATE calls into the in-kernel APIC event acceptance path and
	// can modify vCPU/LAPIC state. Snapshot it before the register files so the
	// saved regs/sregs/LAPIC/events all describe the same architectural point.
	let mp_state = vcpu.get_mp_state()?;
	let regs = vcpu.get_regs()?;
	let sregs = vcpu.get_sregs()?;
	if let Some(detail) = nested_virtualization(vcpu, &sregs)? {
		return Err(Box::new(NestedVirtualizationActive { detail }));
	}

	// Capture the full extended state. Hosts advertising KVM_CAP_XSAVE2 can have
	// an xstate larger than the legacy 4096-byte area (AVX-512/AMX/PKRU) — use
	// KVM_GET_XSAVE2 there; otherwise fall back to the legacy KVM_GET_XSAVE.
	let xsave = if xsave_size > size_of::<kvm_xsave>() {
		let extra = (xsave_size - size_of::<kvm_xsave>())
			.div_ceil(size_of::<<kvm_xsave2 as FamStruct>::Entry>());
		let mut x = Xsave::new(extra).map_err(|e| err(format!("alloc xsave2: {e:?}")))?;
		// SAFETY: `x` is sized to the host's reported XSAVE2 length.
		unsafe { vcpu.get_xsave2(&mut x)? };
		x
	} else {
		let legacy = vcpu.get_xsave()?;
		let mut x = Xsave::new(0).map_err(|e| err(format!("alloc xsave: {e:?}")))?;
		// SAFETY: the pointer is valid; we only overwrite the embedded fixed
		// kvm_xsave.
		unsafe { (*x.as_mut_fam_struct_ptr()).xsave = legacy };
		x
	};

	let xcrs = vcpu.get_xcrs()?;
	let debug_regs = vcpu.get_debug_regs()?;
	let lapic = vcpu.get_lapic()?;
	let cpuid = vcpu.get_cpuid2(KVM_MAX_CPUID_ENTRIES)?;

	// Read only MSRs the host KVM advertises. A blind KVM_GET_MSRS over an
	// optional curated list stops at the first unsupported index, which can
	// silently drop later critical state (e.g. CET/XSS on newer Intel hosts).
	// The list is captured at VM construction (`cache_supported_msrs`) because
	// this runs on a vCPU thread under the sandbox filters, which deny reopening
	// /dev/kvm.
	let supported = supported_msrs()?;
	let probe: Vec<kvm_msr_entry> = super::msr::MIGRATION_MSRS
		.iter()
		.copied()
		.filter(|index| supported.contains(index))
		.map(|index| kvm_msr_entry { index, data: 0, ..Default::default() })
		.collect();
	let mut msrs = Msrs::from_entries(&probe)?;
	let n = vcpu.get_msrs(&mut msrs)?;
	let msrs: Vec<(u32, u64)> = msrs.as_slice()[..n]
		.iter()
		.map(|e| (e.index, e.data))
		.collect();

	// KVM_GET_VCPU_EVENTS is order-sensitive: keep it last because earlier KVM
	// GET ioctls may accept pending APIC events or otherwise adjust event state.
	let vcpu_events = vcpu.get_vcpu_events()?;

	Ok(VcpuState { cpuid, msrs, regs, sregs, xsave, xcrs, debug_regs, lapic, mp_state, vcpu_events })
}

/// Restore a vCPU from saved state. Order follows the x86 KVM migration
/// dependencies used by QEMU/Firecracker: restore register files before
/// events, LAPIC before TSC-deadline MSRs, and events last because `SET_REGS`
/// clears pending exceptions.
pub fn restore_vcpu(vcpu: &Vcpu, st: &VcpuState) -> Result<()> {
	let vcpu = vcpu.fd();
	vcpu.set_cpuid2(&st.cpuid)?;
	vcpu.set_mp_state(st.mp_state)?;
	vcpu.set_regs(&st.regs)?;
	vcpu.set_sregs(&st.sregs)?;

	// Restore the extended state before XCR0. This is the ordering used by KVM
	// migration users: KVM_SET_XSAVE copies the raw xstate image, then
	// KVM_SET_XCRS restores which components the guest has enabled.
	if st.xsave.as_slice().is_empty() {
		// SAFETY: `st.xsave` was produced by get_xsave2/get_xsave on a vCPU of
		// this same host, so its buffer matches this host's legacy XSAVE
		// layout.
		unsafe { vcpu.set_xsave(&st.xsave.as_fam_struct_ref().xsave)? };
	} else {
		// SAFETY: `st.xsave` was produced by get_xsave2/get_xsave on a vCPU of
		// this same host, so its buffer matches this host's XSAVE2 layout.
		unsafe { vcpu.set_xsave2(&st.xsave)? };
	}
	vcpu.set_xcrs(&st.xcrs)?;
	vcpu.set_debug_regs(&st.debug_regs)?;
	vcpu.set_lapic(&st.lapic)?;

	let entries: Vec<kvm_msr_entry> = st
		.msrs
		.iter()
		.map(|&(index, data)| kvm_msr_entry { index, data, ..Default::default() })
		.collect();
	let msrs = Msrs::from_entries(&entries)?;
	let n = vcpu.set_msrs(&msrs)?;
	if n < st.msrs.len() {
		bail!("set_msrs wrote {n} of {} entries", st.msrs.len());
	}

	vcpu.set_vcpu_events(&st.vcpu_events)?;
	Ok(())
}

/// Read the VM-global interrupt-controller, PIT and clock state.
pub fn save_machine(vm: &VmFd) -> Result<MachineState> {
	let mut pic_master =
		kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_PIC_MASTER, ..Default::default() };
	vm.get_irqchip(&mut pic_master)
		.map_err(|e| err(format!("snapshot get_irqchip PIC_MASTER: {e}")))?;
	let mut pic_slave =
		kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_PIC_SLAVE, ..Default::default() };
	vm.get_irqchip(&mut pic_slave)
		.map_err(|e| err(format!("snapshot get_irqchip PIC_SLAVE: {e}")))?;
	let mut ioapic = kvm_irqchip { chip_id: kvm_bindings::KVM_IRQCHIP_IOAPIC, ..Default::default() };
	vm.get_irqchip(&mut ioapic)
		.map_err(|e| err(format!("snapshot get_irqchip IOAPIC: {e}")))?;
	let pit = vm
		.get_pit2()
		.map_err(|e| err(format!("snapshot get_pit2: {e}")))?;
	let clock = vm
		.get_clock()
		.map_err(|e| err(format!("snapshot get_clock: {e}")))?;
	Ok(MachineState { pic_master, pic_slave, ioapic, pit, clock })
}

/// Restore the VM-global interrupt-controller, PIT and clock state.
pub fn restore_machine(vm: &VmFd, st: &MachineState) -> Result<()> {
	vm.set_irqchip(&st.pic_master)?;
	vm.set_irqchip(&st.pic_slave)?;
	vm.set_irqchip(&st.ioapic)?;
	vm.set_pit2(&st.pit)?;
	vm.set_clock(&st.clock)?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use kvm_bindings::kvm_vmx_nested_state_hdr;

	use super::*;

	/// A KVM report as `KVM_GET_NESTED_STATE` writes it: header only.
	fn report(format: u32, flags: u32) -> KvmNestedStateBuffer {
		let mut state = KvmNestedStateBuffer::empty();
		state.format = u16::try_from(format).unwrap();
		state.flags = u16::try_from(flags).unwrap();
		state.size = u32::try_from(size_of::<kvm_nested_state>()).unwrap();
		state
	}

	fn vmx(vmxon_pa: u64, flags: u32) -> KvmNestedStateBuffer {
		let mut state = report(KVM_STATE_NESTED_FORMAT_VMX, flags);
		state.hdr.vmx =
			kvm_vmx_nested_state_hdr { vmxon_pa, vmcs12_pa: u64::MAX, ..Default::default() };
		state
	}

	#[test]
	fn vmx_is_in_use_from_vmxon_until_vmxoff() {
		assert_eq!(nested_state_in_use(&vmx(u64::MAX, 0), 0, None), None);
		assert!(nested_state_in_use(&vmx(0x1000, 0), 0, None).is_some());
		assert!(nested_state_in_use(&vmx(0x1000, KVM_STATE_NESTED_GUEST_MODE), 0, None).is_some());
		let mut with_vmcs12 = vmx(0x1000, 0);
		with_vmcs12.size += 4096;
		assert!(nested_state_in_use(&with_vmcs12, 0, None).is_some());
	}

	#[test]
	fn svm_is_in_use_while_a_guest_hypervisor_enabled_it() {
		let idle = report(KVM_STATE_NESTED_FORMAT_SVM, KVM_STATE_NESTED_GIF_SET);
		assert_eq!(nested_state_in_use(&idle, 0, Some(0)), None);
		// SVME restored from an older snapshot without a host save area.
		assert_eq!(nested_state_in_use(&idle, EFER_SVME, Some(0)), None);
		assert!(nested_state_in_use(&idle, EFER_SVME, Some(0x7000)).is_some());
		assert!(nested_state_in_use(&idle, EFER_SVME, None).is_some());
		let clgi = report(KVM_STATE_NESTED_FORMAT_SVM, 0);
		assert!(nested_state_in_use(&clgi, 0, Some(0)).is_some());
		let running = report(
			KVM_STATE_NESTED_FORMAT_SVM,
			KVM_STATE_NESTED_GIF_SET | KVM_STATE_NESTED_GUEST_MODE,
		);
		assert!(nested_state_in_use(&running, EFER_SVME, Some(0x7000)).is_some());
	}
}
