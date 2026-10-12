// Copyright 2018-2019 CrowdStrike, Inc.
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

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use common::*;

pub const APIC_LVT_NB: usize = 6;
pub const NUM_REGS: usize = 8;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct LapicStateRaw {
    pub regs: [::std::os::raw::c_char; 4096usize],
}

impl Default for LapicStateRaw {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

impl ::std::fmt::Debug for LapicStateRaw {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.regs[..].fmt(fmt)
    }
}

// MMIO offsets to find registers within the APIC state
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum APIC_REG_OFFSET {
    LocalApicId = 0x020,
    LocalApicVersion = 0x030,
    TaskPriority = 0x080,       // TPR
    ProcessorPriority = 0x0a0,  // PPR
    Eoi = 0x0b0,                // EOI
    LogicalDestination = 0x0d0, // LDR
    DestinationFormat = 0x0e0,  // DFR
    SpuriousVector = 0x0f0,     // SVR

    // Interrupt Request Register (IRR) for various bits, one bit per vector
    InterruptRequest0 = 0x200, // Bits 31:0
    InterruptRequest1 = 0x210, // Bits 63:32 ...
    InterruptRequest2 = 0x220,
    InterruptRequest3 = 0x230,
    InterruptRequest4 = 0x240,
    InterruptRequest5 = 0x250,
    InterruptRequest6 = 0x260,
    InterruptRequest7 = 0x270,

    // In-Service Register (ISR) for various bits, one bit per vector
    InService0 = 0x100, // Bits 31:0
    InService1 = 0x110, // Bits 63:32 ...
    InService2 = 0x120,
    InService3 = 0x130,
    InService4 = 0x140,
    InService5 = 0x150,
    InService6 = 0x160,
    InService7 = 0x170,

    // Trigger Mode Registers (TMR) for various bits, one bit per vector
    TriggerMode0 = 0x180, // Bits 31:0
    TriggerMode1 = 0x190, // Bits 63:32 ...
    TriggerMode2 = 0x1a0,
    TriggerMode3 = 0x1b0,
    TriggerMode4 = 0x1c0,
    TriggerMode5 = 0x1d0,
    TriggerMode6 = 0x1e0,
    TriggerMode7 = 0x1f0,

    ErrorStatus = 0x280,
    LvtCmci = 0x2f0,
    InterruptCommand0 = 0x300,
    InterruptCommand1 = 0x310,

    // Local Vector Table (LVT)
    LvtTimer = 0x320,
    LvtThermalSensor = 0x330,
    LvtPerfMon = 0x340,
    LvtLint0 = 0x350,
    LvtLint1 = 0x360,
    LvtError = 0x370,
    TimerInitialCount = 0x380,
    TimerCurrentCount = 0x390,
    TimerDivideConfiguration = 0x3e0,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct XsaveArea {
    pub region: [UINT32; 1024usize],
}

impl Default for XsaveArea {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

impl ::std::fmt::Debug for XsaveArea {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        self.region[..].fmt(fmt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout_lapic_state_raw() {
        assert_eq!(
            ::std::mem::size_of::<LapicStateRaw>(),
            4096usize,
            concat!("Size of: ", stringify!(LAPICState))
        );
        assert_eq!(
            ::std::mem::align_of::<LapicStateRaw>(),
            1usize,
            concat!("Alignment of ", stringify!(LapicStateRaw))
        );
    }

    #[test]
    fn test_layout_xsave_area() {
        assert_eq!(
            ::std::mem::size_of::<XsaveArea>(),
            4096usize,
            concat!("Size of: ", stringify!(XsaveArea))
        );
        assert_eq!(
            ::std::mem::align_of::<XsaveArea>(),
            4usize,
            concat!("Alignment of ", stringify!(XsaveArea))
        );
    }
}
