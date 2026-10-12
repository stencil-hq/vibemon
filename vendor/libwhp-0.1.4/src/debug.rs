// Copyright 2019 CrowdStrike, Inc.
// Copyright 2019 Cloudbase Solutions
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Portions Copyright 2017 The Chromium OS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the THIRD-PARTY file.

use std::fmt;

use platform::*;
pub use win_hv_platform_defs::*;
pub use win_hv_platform_defs_internal::*;

impl fmt::Debug for WHV_RUN_VP_EXIT_CONTEXT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "ExitReason: {:?}", self.ExitReason)?;
        writeln!(fmt, "Reserved: {}", self.Reserved)?;
        writeln!(fmt, "Run context: {:?}", self.VpContext)?;
        writeln!(fmt, "Execution state: {}", self.VpContext.ExecutionState)?;

        unsafe {
            match self.ExitReason {
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonMemoryAccess => {
                    writeln!(fmt, "{:?}", self.anon_union.MemoryAccess)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64IoPortAccess => {
                    writeln!(fmt, "{:?}", self.anon_union.IoPortAccess)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64MsrAccess => {
                    writeln!(fmt, "{:?}", self.anon_union.MsrAccess)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64Cpuid => {
                    writeln!(fmt, "{:?}", self.anon_union.CpuidAccess)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonException => {
                    writeln!(fmt, "{:?}", self.anon_union.VpException)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64InterruptWindow => {
                    writeln!(fmt, "{:?}", self.anon_union.InterruptWindow)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonUnsupportedFeature => {
                    writeln!(fmt, "{:?}", self.anon_union.UnsupportedFeature)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonCanceled => {
                    writeln!(fmt, "{:?}", self.anon_union.CancelReason)?;
                }
                WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonX64ApicEoi => {
                    writeln!(fmt, "{:?}", self.anon_union.ApicEoi)?;
                }
                _ => {
                    writeln!(fmt, "unexected exit reason!")?;
                }
        }
    }

    writeln!(fmt, "")
    }
}

fn dump_instruction_bytes(fmt: &mut fmt::Formatter, bytes: &[u8]) -> fmt::Result {
    for idx in 0..bytes.len() {
        if (idx > 0) && (idx % 16 == 0) {
            writeln!(fmt, "")?;
        }
        write!(fmt, "{:02x} ", bytes[idx])?;
    }
    writeln!(fmt, "")
}

fn dump_access_info(fmt: &mut fmt::Formatter, access_info: &WHV_MEMORY_ACCESS_INFO) -> fmt::Result {
    match access_info.AccessType() {
        0 => write!(fmt, "Read")?,
        1 => write!(fmt, "Write")?,
        2 => write!(fmt, "Execute")?,
        _ => write!(fmt, "Unknown")?
    };

    if access_info.GpaUnmapped() == 1 {
        write!(fmt, " GpaUnmapped")?;
    }
    
    if access_info.GvaValid() == 1 {
        write!(fmt, " GpaValid")?;
    }

    writeln!(fmt, "")
}

impl fmt::Debug for WHV_MEMORY_ACCESS_CONTEXT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "MemoryAccess:")?;
        writeln!(fmt, "  InstructionByteCount: {}", self.InstructionByteCount)?;
        write!(fmt, "  InstructionBytes: ")?;
        dump_instruction_bytes(fmt, &self.InstructionBytes)?;
        write!(fmt, "  AccessInfo: 0x{:x} - ", self.AccessInfo.AsUINT32)?;
        dump_access_info(fmt, &self.AccessInfo)?;
        writeln!(fmt, "  Gpa: 0x{:x}", self.Gpa)?;
        writeln!(fmt, "  Gva: 0x{:x}", self.Gva)
    }
}

impl fmt::Debug for WHV_X64_IO_PORT_ACCESS_CONTEXT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "IoPortAccess:")?;

        // Context of the virtual processor
        writeln!(fmt,
            "  InstructionByteCount: 0x{:x}",
            self.InstructionByteCount
        )?;
        writeln!(fmt, "  Reserved: {:?}", self.Reserved)?;
        write!(fmt, "  InstructionBytes: ")?;
        dump_instruction_bytes(fmt, &self.InstructionBytes)?;

        // I/O port access info
        writeln!(fmt, "  AccessInfo: {:?}", self.AccessInfo)?;
        writeln!(fmt, "  PortNumber: 0x{:x}", self.PortNumber)?;
        writeln!(fmt, "  Reserved2: {:?}", self.Reserved2)?;
        writeln!(fmt,
            "  Rax: 0x{:016x} Rcx: 0x{:016x} Rsi: 0x{:016x} Rdi: 0x{:016x}",
            self.Rax, self.Rcx, self.Rsi, self.Rdi
        )?;
        writeln!(fmt, "  Ds: {:?}", self.Ds)?;
        writeln!(fmt, "  Es: {:?}", self.Es)
    }
}

impl fmt::Debug for WHV_X64_MSR_ACCESS_CONTEXT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "MsrAccess:")?;
        writeln!(fmt,
            "  MsrNumber: 0x{:x} AccessInfo: {}",
            self.MsrNumber, self.AccessInfo.AsUINT32
        )?;
        writeln!(fmt, "  Rax: 0x{:016x} Rdx: 0x{:016x}", self.Rax, self.Rdx)
    }
}

impl fmt::Debug for WHV_X64_CPUID_ACCESS_CONTEXT {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "CpuidAccess:")?;
        writeln!(fmt,
            "  Rax: {:016?} Rbx: {:016?} Rcx: {:016?} Rdx: {:016?}",
            self.Rax, self.Rbx, self.Rcx, self.Rdx
        )?;
        writeln!(fmt,
            "  DefaultResult Rax: {:016?} Rbx: {:016?} Rcx: {:016?} Rdx: {:016?}",
            self.DefaultResultRax,
            self.DefaultResultRbx,
            self.DefaultResultRcx,
            self.DefaultResultRdx
        )
    }
}

impl fmt::Debug for VirtualProcessor {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "VirtualProcessor:")?;
        writeln!(fmt, "Index: 0x{:x}", self.index())?;

        dump_gp_regs(fmt, self)?;
        dump_segment_regs(fmt, self)?;
        dump_table_regs(fmt, self)?;
        dump_control_regs(fmt, self)?;
        dump_debug_regs(fmt, self)?;
        dump_fp_regs(fmt, self)?;
        dump_msr_regs(fmt, self)?;
        dump_mtr_regs(fmt, self)?;
        dump_mtrfix_regs(fmt, self)?;
        dump_interrupt_regs(fmt, self)?;

        dump_cpu_counters(fmt, self)?;
        Ok(())
    }
}

fn dump_gp_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 18;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
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
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    writeln!(fmt, "Regs:")?;
    unsafe {
        writeln!(fmt,
            "  Rax: {:016x} Rcx: {:016x} Rdx: {:016x} Rbx: {:016x}\n\
             \x20 Rsp: {:016x} Rbp: {:016x} Rsi: {:016x} Rdi: {:016x}\n\
             \x20 R8:  {:016x} R9:  {:016x} R10: {:016x} R11: {:016x}\n\
             \x20 R12: {:016x} R13: {:016x} R14: {:016x} R15: {:016x}\n\
             \x20 Rip: {:016x} Rflags: {:016x}",
            reg_values[0].Reg64,
            reg_values[1].Reg64,
            reg_values[2].Reg64,
            reg_values[3].Reg64,
            reg_values[4].Reg64,
            reg_values[5].Reg64,
            reg_values[6].Reg64,
            reg_values[7].Reg64,
            reg_values[8].Reg64,
            reg_values[9].Reg64,
            reg_values[10].Reg64,
            reg_values[11].Reg64,
            reg_values[12].Reg64,
            reg_values[13].Reg64,
            reg_values[14].Reg64,
            reg_values[15].Reg64,
            reg_values[16].Reg64,
            reg_values[17].Reg64
        )?;
    }
    writeln!(fmt, "")
}

fn dump_segment_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 8;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterCs,
        WHV_REGISTER_NAME::WHvX64RegisterSs,
        WHV_REGISTER_NAME::WHvX64RegisterDs,
        WHV_REGISTER_NAME::WHvX64RegisterEs,
        WHV_REGISTER_NAME::WHvX64RegisterFs,
        WHV_REGISTER_NAME::WHvX64RegisterGs,
        WHV_REGISTER_NAME::WHvX64RegisterTr,
        WHV_REGISTER_NAME::WHvX64RegisterLdtr,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    writeln!(fmt, "Segment regs:")?;
    unsafe {
        writeln!(fmt,
            "  Cs: {:?}\n\
             \x20 Ss: {:?}\n\
             \x20 Ds: {:?}\n\
             \x20 Es: {:?}\n\
             \x20 Fs: {:?}\n\
             \x20 Gs: {:?}\n\
             \x20 Tr: {:?}\n\
             \x20 Ldtr: {:?}",
            reg_values[0].Segment,
            reg_values[1].Segment,
            reg_values[2].Segment,
            reg_values[3].Segment,
            reg_values[4].Segment,
            reg_values[5].Segment,
            reg_values[6].Segment,
            reg_values[7].Segment,
        )?
    }
    writeln!(fmt, "")
}

fn dump_table_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 2;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterIdtr,
        WHV_REGISTER_NAME::WHvX64RegisterGdtr,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    unsafe {
        writeln!(fmt, "Table regs:")?;
        writeln!(fmt, "Idtr = {:?}", reg_values[0].Table)?;
        writeln!(fmt, "Gdtr = {:0?}", reg_values[1].Table)?;
    }
    writeln!(fmt, "")
}

fn dump_control_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 5;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterCr0,
        WHV_REGISTER_NAME::WHvX64RegisterCr2,
        WHV_REGISTER_NAME::WHvX64RegisterCr3,
        WHV_REGISTER_NAME::WHvX64RegisterCr4,
        WHV_REGISTER_NAME::WHvX64RegisterCr8,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    let mut idx = 0;
    writeln!(fmt, "Control regs:")?;
    for v in reg_names.iter() {
        unsafe {
            writeln!(fmt, "{:?} = 0x{:x?}", v, reg_values[idx].Reg64)?;
        }
        idx += 1;
    }
    writeln!(fmt, "")
}

fn dump_debug_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 6;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterDr0,
        WHV_REGISTER_NAME::WHvX64RegisterDr1,
        WHV_REGISTER_NAME::WHvX64RegisterDr2,
        WHV_REGISTER_NAME::WHvX64RegisterDr3,
        WHV_REGISTER_NAME::WHvX64RegisterDr6,
        WHV_REGISTER_NAME::WHvX64RegisterDr7,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    unsafe {
        writeln!(fmt,
            "Debug regs:\n\
             Dr0={:016x} Dr1={:016x} Dr2={:016x} \n\
             Dr3={:016x} Dr6={:016x} Dr7={:016x}\n",
            reg_values[0].Reg64,
            reg_values[1].Reg64,
            reg_values[2].Reg64,
            reg_values[3].Reg64,
            reg_values[4].Reg64,
            reg_values[5].Reg64,
        )
    }
}

fn dump_fp_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 26;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
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
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    unsafe {
        writeln!(fmt,
            "Fp regs: \n\
             Xmm0={:016x}{:016x}  Xmm1={:016x}{:016x} \n\
             Xmm2={:016x}{:016x}  Xmm3={:016x}{:016x} \n\
             Xmm4={:016x}{:016x}  Xmm5={:016x}{:016x} \n\
             Xmm6={:016x}{:016x}  Xmm7={:016x}{:016x} \n\
             Xmm8={:016x}{:016x}  Xmm9={:016x}{:016x} \n\
             Xmm10={:016x}{:016x} Xmm11={:016x}{:016x} \n\
             Xmm12={:016x}{:016x} Xmm13={:016x}{:016x} \n\
             Xmm14={:016x}{:016x} Xmm15={:016x}{:016x} \n\
             Mmx0={:016x} Mmx1={:016x} Mmx2={:016x} \n\
             Mmx3={:016x} Mmx4={:016x} Mmx5={:016x} \n\
             Mmx6={:016x} Mmx7={:016x} \n\
             Csr={:016x} XCsr={:016x}\n",
            reg_values[0].Fp.AsUINT128.High64,
            reg_values[0].Fp.AsUINT128.Low64,
            reg_values[1].Fp.AsUINT128.High64,
            reg_values[1].Fp.AsUINT128.Low64,
            reg_values[2].Fp.AsUINT128.High64,
            reg_values[2].Fp.AsUINT128.Low64,
            reg_values[3].Fp.AsUINT128.High64,
            reg_values[3].Fp.AsUINT128.Low64,
            reg_values[4].Fp.AsUINT128.High64,
            reg_values[4].Fp.AsUINT128.Low64,
            reg_values[5].Fp.AsUINT128.High64,
            reg_values[5].Fp.AsUINT128.Low64,
            reg_values[6].Fp.AsUINT128.High64,
            reg_values[6].Fp.AsUINT128.Low64,
            reg_values[7].Fp.AsUINT128.High64,
            reg_values[7].Fp.AsUINT128.Low64,
            reg_values[8].Fp.AsUINT128.High64,
            reg_values[8].Fp.AsUINT128.Low64,
            reg_values[9].Fp.AsUINT128.High64,
            reg_values[9].Fp.AsUINT128.Low64,
            reg_values[10].Fp.AsUINT128.High64,
            reg_values[10].Fp.AsUINT128.Low64,
            reg_values[11].Fp.AsUINT128.High64,
            reg_values[11].Fp.AsUINT128.Low64,
            reg_values[12].Fp.AsUINT128.High64,
            reg_values[12].Fp.AsUINT128.Low64,
            reg_values[13].Fp.AsUINT128.High64,
            reg_values[13].Fp.AsUINT128.Low64,
            reg_values[14].Fp.AsUINT128.High64,
            reg_values[14].Fp.AsUINT128.Low64,
            reg_values[15].Fp.AsUINT128.High64,
            reg_values[15].Fp.AsUINT128.Low64,
            reg_values[16].Reg64,
            reg_values[17].Reg64,
            reg_values[18].Reg64,
            reg_values[19].Reg64,
            reg_values[20].Reg64,
            reg_values[21].Reg64,
            reg_values[22].Reg64,
            reg_values[23].Reg64,
            reg_values[24].Reg64,
            reg_values[25].Reg64,
        )
    }
}

fn dump_msr_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 12;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
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
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    let mut idx = 0;
    writeln!(fmt, "Msr regs:")?;
    for v in reg_names.iter() {
        unsafe {
            writeln!(fmt, "{:?} = 0x{:x?}", v, reg_values[idx].Reg64)?;
        }
        idx += 1;
    }
    writeln!(fmt, "")
}

fn dump_mtr_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 16;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase0,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask0,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase1,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask1,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase2,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask2,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase3,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask3,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase4,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask4,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase5,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask5,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase6,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask6,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase7,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask7,
        /*
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase8,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask8,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBase9,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMask9,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseA,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskA,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseB,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskB,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseC,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskC,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseD,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskD,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseE,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskE,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysBaseF,
                WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrPhysMaskF,
        */
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    unsafe {
        writeln!(fmt,
            "Mttr regs:\n\
             Mtrr0={:016x}, Mask0={:016x}, Mtrr1={:016x}, Mask1={:016x}\n\
             Mtrr2={:016x}, Mask2={:016x}, Mtrr3={:016x}, Mask3={:016x}\n\
             Mtrr4={:016x}, Mask4={:016x}, Mtrr5={:016x}, Mask5={:016x}\n\
             Mtrr6={:016x}, Mask6={:016x}, Mtrr7={:016x}, Mask7={:016x}\n",
            reg_values[0].Reg64,
            reg_values[1].Reg64,
            reg_values[2].Reg64,
            reg_values[3].Reg64,
            reg_values[4].Reg64,
            reg_values[5].Reg64,
            reg_values[6].Reg64,
            reg_values[7].Reg64,
            reg_values[8].Reg64,
            reg_values[9].Reg64,
            reg_values[10].Reg64,
            reg_values[11].Reg64,
            reg_values[12].Reg64,
            reg_values[13].Reg64,
            reg_values[14].Reg64,
            reg_values[15].Reg64,
        )
    }
}

fn dump_mtrfix_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 11;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix64k00000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix16k80000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix16kA0000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kC0000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kC8000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kD0000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kD8000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kE0000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kE8000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kF0000,
        WHV_REGISTER_NAME::WHvX64RegisterMsrMtrrFix4kF8000,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    unsafe {
        writeln!(fmt,
            "[00000]:{:016x}, [80000]:{:016x}, [A0000]:{:016x},\n\
             [C0000]:{:016x}, [C8000]:{:016x}, \n\
             [D0000]:{:016x}, [D8000]:{:016x}, \n\
             [E0000]:{:016x}, [E8000]:{:016x}, \n\
             [F0000]:{:016x}, [F8000]:{:016x}",
            reg_values[0].Reg64,
            reg_values[1].Reg64,
            reg_values[2].Reg64,
            reg_values[3].Reg64,
            reg_values[4].Reg64,
            reg_values[5].Reg64,
            reg_values[6].Reg64,
            reg_values[7].Reg64,
            reg_values[8].Reg64,
            reg_values[9].Reg64,
            reg_values[10].Reg64,
        )
    }
}

fn dump_interrupt_regs(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    const NUM_REGS: usize = 5;
    let reg_names: [WHV_REGISTER_NAME; NUM_REGS] = [
        WHV_REGISTER_NAME::WHvRegisterPendingInterruption,
        WHV_REGISTER_NAME::WHvRegisterInterruptState,
        WHV_REGISTER_NAME::WHvRegisterPendingEvent,
        WHV_REGISTER_NAME::WHvX64RegisterDeliverabilityNotifications,
        WHV_REGISTER_NAME::WHvRegisterInternalActivityState,
    ];
    let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS] = Default::default();

    // Get the registers as a baseline
    vp.get_registers(&reg_names, &mut reg_values).or(Err(fmt::Error))?;

    writeln!(fmt, "Interrupt regs:")?;
    let mut idx = 0;
    unsafe {
        writeln!(fmt,
            "{:?} = {}",
            reg_names[idx], reg_values[idx].PendingInterruption
        )?;
    }
    let event_type = unsafe { reg_values[idx].PendingInterruption.InterruptionType() };

    idx += 1;
    unsafe {
        writeln!(fmt, "{:?} = {}", reg_names[idx], reg_values[idx].InterruptState)?;
    }
    idx += 1;

    if event_type == WHV_X64_PENDING_EVENT_TYPE::WHvX64PendingEventException as u64 {
        unsafe {
            writeln!(fmt, "{:?} = {}", reg_names[idx], reg_values[idx].ExceptionEvent)?;
        }
    } else if event_type == WHV_X64_PENDING_EVENT_TYPE::WHvX64PendingEventExtInt as u64 {
        unsafe {
            writeln!(fmt, "{:?} = {}", reg_names[idx], reg_values[idx].ExtIntEvent)?;
        }
    } else {
        writeln!(fmt, "Unknown event type: {}", event_type)?;
    }
    idx += 1;
    unsafe {
        writeln!(fmt,
            "{:?} = {}",
            reg_names[idx], reg_values[idx].DeliverabilityNotifications
        )?;
    }
    idx += 1;
    unsafe {
        writeln!(fmt, "{:?} = {}", reg_names[idx], reg_values[idx].Reg128)?;
    }
    writeln!(fmt, "")
}

fn dump_cpu_counters(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    dump_apic_counters(fmt, vp)?;
    dump_cpu_runtime_counters(fmt, vp)?;
    dump_cpu_intercept_counters(fmt, vp)?;
    dump_cpu_event_counters(fmt, vp)
}

fn dump_apic_counters(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    let counters: WHV_PROCESSOR_COUNTERS = vp
        .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetApic)
        .or(Err(fmt::Error))?;
    unsafe {
        writeln!(fmt, "Apic counters: {:#?}\n", counters.ApicCounters)
    }
}

fn dump_cpu_runtime_counters(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    let counters: WHV_PROCESSOR_COUNTERS = vp
        .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetRuntime)
        .or(Err(fmt::Error))?;
    unsafe {
        writeln!(fmt, "CPU runtime counters: {:#?}\n", counters.RuntimeCounters)
    }
}

fn dump_cpu_intercept_counters(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    let counters: WHV_PROCESSOR_COUNTERS = vp
        .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetIntercepts)
        .or(Err(fmt::Error))?;
    unsafe {
        writeln!(fmt,
            "CPU intercept counters: {:#?}\n",
            counters.InterceptCounters
        )
    }
}

fn dump_cpu_event_counters(fmt: &mut fmt::Formatter, vp: &VirtualProcessor) -> fmt::Result {
    let counters: WHV_PROCESSOR_COUNTERS = vp
        .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetEvents)
        .or(Err(fmt::Error))?;
    unsafe {
        writeln!(fmt, "CPU event counters: {:#?}\n", counters.EventCounters)
    }
}
