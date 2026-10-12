// Copyright 2018 Cloudbase Solutions Srl
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

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use std::fmt;
use std::ops::{BitAnd, BitAndAssign, BitOrAssign, Shl, Shr};

use common::*;

pub type WHV_PARTITION_HANDLE = *mut VOID;

pub type WHV_GUEST_PHYSICAL_ADDRESS = UINT64;
pub type WHV_GUEST_VIRTUAL_ADDRESS = UINT64;

pub const WHV_E_INSUFFICIENT_BUFFER: HRESULT = -2143878399; // 0x80370301
pub const WHV_E_INVALID_PARTITION_CONFIG: HRESULT = -2143878396; // 0x80370304
pub const WHV_E_GPA_RANGE_NOT_FOUND: HRESULT = -2143878395; // 0x80370305

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_PARTITION_PROPERTY_CODE {
    WHvPartitionPropertyCodeExtendedVmExits = 0x00000001,
    WHvPartitionPropertyCodeExceptionExitBitmap = 0x00000002,
    WHvPartitionPropertyCodeSeparateSecurityDomain = 0x00000003,

    WHvPartitionPropertyCodeProcessorFeatures = 0x00001001,
    WHvPartitionPropertyCodeProcessorClFlushSize = 0x00001002,
    WHvPartitionPropertyCodeCpuidExitList = 0x00001003,
    WHvPartitionPropertyCodeCpuidResultList = 0x00001004,
    WHvPartitionPropertyCodeLocalApicEmulationMode = 0x00001005,
    WHvPartitionPropertyCodeProcessorXsaveFeatures = 0x00001006,

    WHvPartitionPropertyCodeProcessorCount = 0x00001fff,
}

impl Default for WHV_PARTITION_PROPERTY_CODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_CAPABILITY_CODE {
    // Capabilities of the API implementation
    WHvCapabilityCodeHypervisorPresent = 0x00000000,
    WHvCapabilityCodeFeatures = 0x00000001,
    WHvCapabilityCodeExtendedVmExits = 0x00000002,
    WHvCapabilityCodeExceptionExitBitmap = 0x00000003,

    // Capabilities of the system's processor
    WHvCapabilityCodeProcessorVendor = 0x00001000,
    WHvCapabilityCodeProcessorFeatures = 0x00001001,
    WHvCapabilityCodeProcessorClFlushSize = 0x00001002,
    HWvCapabilityCodeProcessorXsaveFeatures = 0x00001003,
}

impl Default for WHV_CAPABILITY_CODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_PROCESSOR_VENDOR {
    WHvProcessorVendorAmd = 0x0000,
    WHvProcessorVendorIntel = 0x0001,
    WHvProcessorVendorHygon = 0x0002,
}

impl Default for WHV_PROCESSOR_VENDOR {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_RUN_VP_EXIT_REASON {
    WHvRunVpExitReasonNone = 0x00000000,

    // Standard exits caused by operations of the virtual processor
    WHvRunVpExitReasonMemoryAccess = 0x00000001,
    WHvRunVpExitReasonX64IoPortAccess = 0x00000002,
    WHvRunVpExitReasonUnrecoverableException = 0x00000004,
    WHvRunVpExitReasonInvalidVpRegisterValue = 0x00000005,
    WHvRunVpExitReasonUnsupportedFeature = 0x00000006,
    WHvRunVpExitReasonX64InterruptWindow = 0x00000007,
    WHvRunVpExitReasonX64Halt = 0x00000008,
    WHvRunVpExitReasonX64ApicEoi = 0x00000009,

    // Additional exits that can be configured through partition properties
    WHvRunVpExitReasonX64MsrAccess = 0x00001000,
    WHvRunVpExitReasonX64Cpuid = 0x00001001,
    WHvRunVpExitReasonException = 0x00001002,

    // Exits caused by the host
    WHvRunVpExitReasonCanceled = 0x00002001,
}

impl Default for WHV_RUN_VP_EXIT_REASON {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_X64_PENDING_INTERRUPTION_TYPE {
    WHvX64PendingInterrupt = 0,
    WHvX64PendingNmi = 2,
    WHvX64PendingException = 3,
}

impl Default for WHV_X64_PENDING_INTERRUPTION_TYPE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_X64_UNSUPPORTED_FEATURE_CODE {
    WHvUnsupportedFeatureIntercept = 1,
    WHvUnsupportedFeatureTaskSwitchTss = 2,
}

impl Default for WHV_X64_UNSUPPORTED_FEATURE_CODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_RUN_VP_CANCEL_REASON {
    WhvRunVpCancelReasonUser = 0, // Execution canceled by HvCancelRunVirtualProcessor
}

impl Default for WHV_RUN_VP_CANCEL_REASON {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_REGISTER_NAME {
    // X64 General purpose registers
    WHvX64RegisterRax = 0x00000000,
    WHvX64RegisterRcx = 0x00000001,
    WHvX64RegisterRdx = 0x00000002,
    WHvX64RegisterRbx = 0x00000003,
    WHvX64RegisterRsp = 0x00000004,
    WHvX64RegisterRbp = 0x00000005,
    WHvX64RegisterRsi = 0x00000006,
    WHvX64RegisterRdi = 0x00000007,
    WHvX64RegisterR8 = 0x00000008,
    WHvX64RegisterR9 = 0x00000009,
    WHvX64RegisterR10 = 0x0000000A,
    WHvX64RegisterR11 = 0x0000000B,
    WHvX64RegisterR12 = 0x0000000C,
    WHvX64RegisterR13 = 0x0000000D,
    WHvX64RegisterR14 = 0x0000000E,
    WHvX64RegisterR15 = 0x0000000F,
    WHvX64RegisterRip = 0x00000010,
    WHvX64RegisterRflags = 0x00000011,

    // X64 Segment registers
    WHvX64RegisterEs = 0x00000012,
    WHvX64RegisterCs = 0x00000013,
    WHvX64RegisterSs = 0x00000014,
    WHvX64RegisterDs = 0x00000015,
    WHvX64RegisterFs = 0x00000016,
    WHvX64RegisterGs = 0x00000017,
    WHvX64RegisterLdtr = 0x00000018,
    WHvX64RegisterTr = 0x00000019,

    // X64 Table registers
    WHvX64RegisterIdtr = 0x0000001A,
    WHvX64RegisterGdtr = 0x0000001B,

    // X64 Control Registers
    WHvX64RegisterCr0 = 0x0000001C,
    WHvX64RegisterCr2 = 0x0000001D,
    WHvX64RegisterCr3 = 0x0000001E,
    WHvX64RegisterCr4 = 0x0000001F,
    WHvX64RegisterCr8 = 0x00000020,

    // X64 Debug Registers
    WHvX64RegisterDr0 = 0x00000021,
    WHvX64RegisterDr1 = 0x00000022,
    WHvX64RegisterDr2 = 0x00000023,
    WHvX64RegisterDr3 = 0x00000024,
    WHvX64RegisterDr6 = 0x00000025,
    WHvX64RegisterDr7 = 0x00000026,

    // X64 Extended Control Registers
    WHvX64RegisterXCr0 = 0x00000027,

    // X64 Floating Point and Vector Registers
    WHvX64RegisterXmm0 = 0x00001000,
    WHvX64RegisterXmm1 = 0x00001001,
    WHvX64RegisterXmm2 = 0x00001002,
    WHvX64RegisterXmm3 = 0x00001003,
    WHvX64RegisterXmm4 = 0x00001004,
    WHvX64RegisterXmm5 = 0x00001005,
    WHvX64RegisterXmm6 = 0x00001006,
    WHvX64RegisterXmm7 = 0x00001007,
    WHvX64RegisterXmm8 = 0x00001008,
    WHvX64RegisterXmm9 = 0x00001009,
    WHvX64RegisterXmm10 = 0x0000100A,
    WHvX64RegisterXmm11 = 0x0000100B,
    WHvX64RegisterXmm12 = 0x0000100C,
    WHvX64RegisterXmm13 = 0x0000100D,
    WHvX64RegisterXmm14 = 0x0000100E,
    WHvX64RegisterXmm15 = 0x0000100F,
    WHvX64RegisterFpMmx0 = 0x00001010,
    WHvX64RegisterFpMmx1 = 0x00001011,
    WHvX64RegisterFpMmx2 = 0x00001012,
    WHvX64RegisterFpMmx3 = 0x00001013,
    WHvX64RegisterFpMmx4 = 0x00001014,
    WHvX64RegisterFpMmx5 = 0x00001015,
    WHvX64RegisterFpMmx6 = 0x00001016,
    WHvX64RegisterFpMmx7 = 0x00001017,
    WHvX64RegisterFpControlStatus = 0x00001018,
    WHvX64RegisterXmmControlStatus = 0x00001019,

    // X64 MSRs
    WHvX64RegisterTsc = 0x00002000,
    WHvX64RegisterEfer = 0x00002001,
    WHvX64RegisterKernelGsBase = 0x00002002,
    WHvX64RegisterApicBase = 0x00002003,
    WHvX64RegisterPat = 0x00002004,
    WHvX64RegisterSysenterCs = 0x00002005,
    WHvX64RegisterSysenterEip = 0x00002006,
    WHvX64RegisterSysenterEsp = 0x00002007,
    WHvX64RegisterStar = 0x00002008,
    WHvX64RegisterLstar = 0x00002009,
    WHvX64RegisterCstar = 0x0000200A,
    WHvX64RegisterSfmask = 0x0000200B,

    WHvX64RegisterMsrMtrrCap = 0x0000200D,
    WHvX64RegisterMsrMtrrDefType = 0x0000200E,

    WHvX64RegisterMsrMtrrPhysBase0 = 0x00002010,
    WHvX64RegisterMsrMtrrPhysBase1 = 0x00002011,
    WHvX64RegisterMsrMtrrPhysBase2 = 0x00002012,
    WHvX64RegisterMsrMtrrPhysBase3 = 0x00002013,
    WHvX64RegisterMsrMtrrPhysBase4 = 0x00002014,
    WHvX64RegisterMsrMtrrPhysBase5 = 0x00002015,
    WHvX64RegisterMsrMtrrPhysBase6 = 0x00002016,
    WHvX64RegisterMsrMtrrPhysBase7 = 0x00002017,
    WHvX64RegisterMsrMtrrPhysBase8 = 0x00002018,
    WHvX64RegisterMsrMtrrPhysBase9 = 0x00002019,
    WHvX64RegisterMsrMtrrPhysBaseA = 0x0000201A,
    WHvX64RegisterMsrMtrrPhysBaseB = 0x0000201B,
    WHvX64RegisterMsrMtrrPhysBaseC = 0x0000201C,
    WHvX64RegisterMsrMtrrPhysBaseD = 0x0000201D,
    WHvX64RegisterMsrMtrrPhysBaseE = 0x0000201E,
    WHvX64RegisterMsrMtrrPhysBaseF = 0x0000201F,

    WHvX64RegisterMsrMtrrPhysMask0 = 0x00002040,
    WHvX64RegisterMsrMtrrPhysMask1 = 0x00002041,
    WHvX64RegisterMsrMtrrPhysMask2 = 0x00002042,
    WHvX64RegisterMsrMtrrPhysMask3 = 0x00002043,
    WHvX64RegisterMsrMtrrPhysMask4 = 0x00002044,
    WHvX64RegisterMsrMtrrPhysMask5 = 0x00002045,
    WHvX64RegisterMsrMtrrPhysMask6 = 0x00002046,
    WHvX64RegisterMsrMtrrPhysMask7 = 0x00002047,
    WHvX64RegisterMsrMtrrPhysMask8 = 0x00002048,
    WHvX64RegisterMsrMtrrPhysMask9 = 0x00002049,
    WHvX64RegisterMsrMtrrPhysMaskA = 0x0000204A,
    WHvX64RegisterMsrMtrrPhysMaskB = 0x0000204B,
    WHvX64RegisterMsrMtrrPhysMaskC = 0x0000204C,
    WHvX64RegisterMsrMtrrPhysMaskD = 0x0000204D,
    WHvX64RegisterMsrMtrrPhysMaskE = 0x0000204E,
    WHvX64RegisterMsrMtrrPhysMaskF = 0x0000204F,

    WHvX64RegisterMsrMtrrFix64k00000 = 0x00002070,
    WHvX64RegisterMsrMtrrFix16k80000 = 0x00002071,
    WHvX64RegisterMsrMtrrFix16kA0000 = 0x00002072,
    WHvX64RegisterMsrMtrrFix4kC0000 = 0x00002073,
    WHvX64RegisterMsrMtrrFix4kC8000 = 0x00002074,
    WHvX64RegisterMsrMtrrFix4kD0000 = 0x00002075,
    WHvX64RegisterMsrMtrrFix4kD8000 = 0x00002076,
    WHvX64RegisterMsrMtrrFix4kE0000 = 0x00002077,
    WHvX64RegisterMsrMtrrFix4kE8000 = 0x00002078,
    WHvX64RegisterMsrMtrrFix4kF0000 = 0x00002079,
    WHvX64RegisterMsrMtrrFix4kF8000 = 0x0000207A,

    WHvX64RegisterTscAux = 0x0000207B,
    WHvX64RegisterSpecCtrl = 0x00002084,
    WHvX64RegisterPredCmd = 0x00002085,

    // APIC state (also accessible via WHv(Get/Set)VirtualProcessorInterruptControllerState)
    WHvX64RegisterApicId = 0x00003002,
    WHvX64RegisterApicVersion = 0x00003003,

    // Interrupt / Event Registers
    WHvRegisterPendingInterruption = 0x80000000,
    WHvRegisterInterruptState = 0x80000001,
    WHvRegisterPendingEvent = 0x80000002,
    WHvX64RegisterDeliverabilityNotifications = 0x80000004,
    WHvRegisterInternalActivityState = 0x80000005,
}

impl Default for WHV_REGISTER_NAME {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_TRANSLATE_GVA_RESULT_CODE {
    WHvTranslateGvaResultSuccess = 0,

    // Translation failures
    WHvTranslateGvaResultPageNotPresent = 1,
    WHvTranslateGvaResultPrivilegeViolation = 2,
    WHvTranslateGvaResultInvalidPageTableFlags = 3,

    // GPA access failures
    WHvTranslateGvaResultGpaUnmapped = 4,
    WHvTranslateGvaResultGpaNoReadAccess = 5,
    WHvTranslateGvaResultGpaNoWriteAccess = 6,
    WHvTranslateGvaResultGpaIllegalOverlayAccess = 7,
    WHvTranslateGvaResultIntercept = 8,
}

impl Default for WHV_TRANSLATE_GVA_RESULT_CODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_EXCEPTION_TYPE {
    WHvX64ExceptionTypeDivideErrorFault = 0x0,
    WHvX64ExceptionTypeDebugTrapOrFault = 0x1,
    WHvX64ExceptionTypeBreakpointTrap = 0x3,
    WHvX64ExceptionTypeOverflowTrap = 0x4,
    WHvX64ExceptionTypeBoundRangeFault = 0x5,
    WHvX64ExceptionTypeInvalidOpcodeFault = 0x6,
    WHvX64ExceptionTypeDeviceNotAvailableFault = 0x7,
    WHvX64ExceptionTypeDoubleFaultAbort = 0x8,
    WHvX64ExceptionTypeInvalidTaskStateSegmentFault = 0x0A,
    WHvX64ExceptionTypeSegmentNotPresentFault = 0x0B,
    WHvX64ExceptionTypeStackFault = 0x0C,
    WHvX64ExceptionTypeGeneralProtectionFault = 0x0D,
    WHvX64ExceptionTypePageFault = 0x0E,
    WHvX64ExceptionTypeFloatingPointErrorFault = 0x10,
    WHvX64ExceptionTypeAlignmentCheckFault = 0x11,
    WHvX64ExceptionTypeMachineCheckAbort = 0x12,
    WHvX64ExceptionTypeSimdFloatingPointFault = 0x13,
}

impl Default for WHV_EXCEPTION_TYPE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_X64_LOCAL_APIC_EMULATION_MODE {
    WHvX64LocalApicEmulationModeNone = 0x0,
    WHvX64LocalApicEmulationModeXApic = 0x1,
}

impl Default for WHV_X64_LOCAL_APIC_EMULATION_MODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_MEMORY_ACCESS_TYPE {
    WHvMemoryAccessRead = 0,
    WHvMemoryAccessWrite = 1,
    WHvMemoryAccessExecute = 2,
}

impl Default for WHV_MEMORY_ACCESS_TYPE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

bitflags! {
    #[repr(C)]
    pub struct WHV_TRANSLATE_GVA_FLAGS: UINT32 {
        const WHvTranslateGvaFlagNone = 0x00000000;
        const WHvTranslateGvaFlagValidateRead = 0x00000001;
        const WHvTranslateGvaFlagValidateWrite = 0x00000002;
        const WHvTranslateGvaFlagValidateExecute = 0x00000004;
        const WHvTranslateGvaFlagPrivilegeExempt = 0x00000008;
        const WHvTranslateGvaFlagSetPageTableBits = 0x00000010;
    }
}

bitflags! {
    #[repr(C)]
    pub struct WHV_MAP_GPA_RANGE_FLAGS: UINT32  {
        const WHvMapGpaRangeFlagNone = 0x00000000;
        const WHvMapGpaRangeFlagRead = 0x00000001;
        const WHvMapGpaRangeFlagWrite = 0x00000002;
        const WHvMapGpaRangeFlagExecute = 0x00000004;
        const WHvMapGpaRangeFlagTrackDirtyPages = 0x00000008;
    }
}

#[allow(non_snake_case)]
#[derive(Copy, Clone)]
#[repr(C)]
pub union WHV_CAPABILITY {
    pub HypervisorPresent: BOOL,
    pub Features: WHV_CAPABILITY_FEATURES,
    pub ExtendedVmExits: WHV_EXTENDED_VM_EXITS,
    pub ProcessorVendor: WHV_PROCESSOR_VENDOR,
    pub ProcessorFeatures: WHV_PROCESSOR_FEATURES,
    pub ProcessorXsaveFeatures: WHV_PROCESSOR_XSAVE_FEATURES,
    pub ProcessorClFlushSize: UINT8,
    pub ExceptionExitBitmap: UINT64,
}

impl Default for WHV_CAPABILITY {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct WHV_X64_CPUID_RESULT {
    pub Function: UINT32,
    pub Reserved: [UINT32; 3],
    pub Eax: UINT32,
    pub Ebx: UINT32,
    pub Ecx: UINT32,
    pub Edx: UINT32,
}

#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_PARTITION_PROPERTY {
    pub ExtendedVmExits: WHV_EXTENDED_VM_EXITS,
    pub ProcessorFeatures: WHV_PROCESSOR_FEATURES,
    pub ProcessorXsaveFeatures: WHV_PROCESSOR_XSAVE_FEATURES,
    pub ProcessorClFlushSize: UINT8,
    pub ProcessorCount: UINT32,
    pub CpuidExitList: [UINT32; 1],
    pub CpuidResultList: [WHV_X64_CPUID_RESULT; 1],
    pub ExceptionExitBitmap: UINT64,
    pub LocalApicEmulationMode: WHV_X64_LOCAL_APIC_EMULATION_MODE,
    pub SeparateSecurityDomain: BOOL,
}

impl Default for WHV_PARTITION_PROPERTY {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[allow(non_snake_case)]
#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct WHV_CAPABILITY_FEATURES {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_CAPABILITY_FEATURES AsUINT64: UINT64 [
    PartialUnmap set_PartialUnmap[0..1],
    LocalApicEmulation set_LocalApicEmulation[1..2],
    Xsave set_Xsave[2..3],
    DirtyPageTracking set_DirtyPageTracking[3..4],
    SpeculationControl set_SpeculationControl[4..6],
    Reserved set_Reserved[5..64],
]);

#[allow(non_snake_case)]
#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct WHV_EXTENDED_VM_EXITS {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_EXTENDED_VM_EXITS AsUINT64: UINT64 [
    X64CpuidExit set_X64CpuidExit[0..1],
    X64MsrExit set_X64MsrExit[1..2],
    ExceptionExit set_ExceptionExit[2..3],
    Reserved set_Reserved[4..64],
]);

#[allow(non_snake_case)]
#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct WHV_PROCESSOR_FEATURES {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_PROCESSOR_FEATURES AsUINT64: UINT64 [
    Sse3Support set_Sse3Support[0..1],
    LahfSahfSupport set_LahfSahfSupport[1..2],
    Ssse3Support set_Ssse3Support[2..3],
    Sse4_1Support set_Sse4_1Support[3..4],
    Sse4_2Support set_Sse4_2Support[4..5],
    Sse4aSupport set_Sse4aSupport[5..6],
    XopSupport set_XopSupport[6..7],
    PopCntSupport set_PopCntSupport[7..8],
    Cmpxchg16bSupport set_Cmpxchg16bSupport[8..9],
    Altmovcr8Support set_Altmovcr8Support[9..10],
    LzcntSupport set_LzcntSupport[10..11],
    MisAlignSseSupport set_MisAlignSseSupport[11..12],
    MmxExtSupport set_MmxExtSupport[12..13],
    Amd3DNowSupport set_Amd3DNowSupport[13..14],
    ExtendedAmd3DNowSupport set_ExtendedAmd3DNowSupport[14..15],
    Page1GbSupport set_Page1GbSupport[15..16],
    AesSupport set_AesSupport[16..17],
    PclmulqdqSupport set_PclmulqdqSupport[17..18],
    PcidSupport set_PcidSupport[18..19],
    Fma4Support set_Fma4Support[19..20],
    F16CSupport set_F16CSupport[20..21],
    RdRandSupport set_RdRandSupport[21..22],
    RdWrFsGsSupport set_RdWrFsGsSupport[22..23],
    SmepSupport set_SmepSupport[23..24],
    EnhancedFastStringSupport set_EnhancedFastStringSupport[24..25],
    Bmi1Support set_Bmi1Support[25..26],
    Bmi2Support set_Bmi2Support[26..27],
    Reserved1 set_Reserved1[27..28],
    MovbeSupport set_MovbeSupport[28..29],
    Npiep1Support set_Npiep1Support[29..30],
    DepX87FPUSaveSupport set_DepX87FPUSaveSupport[30..31],
    RdSeedSupport set_RdSeedSupportp[31..32],
    AdxSupport set_AdxSupport[32..33],
    IntelPrefetchSupport set_IntelPrefetchSupport[33..34],
    SmapSupport set_SmapSupport[34..35],
    HleSupport set_HleSupport[35..36],
    RtmSupport set_RtmSupport[36..37],
    RdtscpSupport set_RdtscpSupport[37..38],
    ClflushoptSupport set_ClflushoptSupport[38..39],
    ClwbSupport set_ClwbSupport[39..40],
    ShaSupport set_ShaSupport[40..41],
    X87PointersSavedSupport set_X87PointersSavedSupport[41..42],
    InvpcidSupport set_InvpcSupport[42..43],
    IbrsSupport set_IbrsSupport[43..44],
    StibpSupport set_StibpSupport[44..45],
    IbpbSupport set_IbpbSupport[45..46],
    Reserved2 set_Reserved2[46..47],
    SsbdSupport set_SsbdSupport[47..48],
    FastShortRepMovSupport set_FastShortRepMovSupport[48..49],
    Reserved3 set_Reserved3[49..50],
    RdclNo set_RdclNo[50..51],
    IbrsAllSupport set_ibrsAllSupport[51..52],
    Reserved4 set_Reserved4[52..53],
    SsbNo set_ssbNo[53..54],
    RsbANo set_RsbANo[54..55],
    Reserved5 set_Reserved5[55..64],
]);

#[allow(non_snake_case)]
#[derive(Copy, Clone, Default)]
#[repr(C)]
pub struct WHV_PROCESSOR_XSAVE_FEATURES {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_PROCESSOR_XSAVE_FEATURES AsUINT64: UINT64 [
        XsaveSupport set_XsaveSupport[0..1],
        XsaveoptSupport set_XsaveoptSupport[1..2],
        AvxSupport set_AvxSupport[2..3],
        Avx2Support set_Avx2Support[3..4],
        FmaSupport set_FmaSupport[4..5],
        MpxSupport set_MpxSupport[5..6],
        Avx512Support set_Avx512Support[6..7],
        Avx512DQSupport set_Avx512DQSupport[7..8],
        Avx512CDSupport set_Avx512CDSupport[8..9],
        Avx512BWSupport set_Avx512BWSupport[9..10],
        Avx512VLSupport set_Avx512VLSupport[10..11],
        XsaveCompSupport set_XsaveCompSupport[11..12],
        XsaveSupervisorSupport set_XsaveSupervisorSupport[12..13],
        Xcr1Support set_Xcr1Support[13..14],
        Avx512BitalgSupport set_Avx512BitalgSupport[14..15],
        Avx512IfmaSupport set_Avx512IfmaSupport[15..16],
        Avx512VBmiSupport set_Avx512VBmiSupport[16..17],
        Avx512VBmi2Support set_Avx512VBmi2Support[17..18],
        Avx512VnniSupport set_Avx512VnniSupport[18..19],
        GfniSupport set_GfniSupport[19..20],
        VaesSupport set_VaesSupport[20..21],
        Avx512VPopcntdqSupport set_Avx512VPopcntdqSupport[21..22],
        VpclmulqdqSupport set_VpclmulqdqSupport[22..23],
        Reserved set_Reserved[23..64],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_SEGMENT_REGISTER {
    pub Base: UINT64,
    pub Limit: UINT32,
    pub Selector: UINT16,
    pub Attributes: UINT16,
}

bitfield!(WHV_X64_SEGMENT_REGISTER Attributes: UINT16 [
    SegmentType set_SegmentType[0..4],
    NonSystemSegment set_NonSystemSegment[4..5],
    DescriptorPrivilegeLevel set_DescriptorPrivilegeLevel[5..7],
    Present set_Present[7..8],
    Reserved set_Reserved[8..12],
    Available set_Available[12..13],
    Long set_Long[13..14],
    Default set_Default[14..15],
    Granularity set_Granularity[15..16],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_TABLE_REGISTER {
    pub Pad: [UINT16; 3],
    pub Limit: UINT16,
    pub Base: UINT64,
}

impl fmt::Display for WHV_X64_TABLE_REGISTER {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "(Pad: {:?} Limit: {} Base: {})",
            self.Pad, self.Limit, self.Base
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_VP_EXECUTION_STATE {
    pub AsUINT16: UINT16,
}

bitfield!(WHV_X64_VP_EXECUTION_STATE AsUINT16: UINT16[
    Cpl set_Cpl[0..2],
    Cr0Pe set_Cr0Pe[2..3],
    Cr0Am set_Cr0Am[3..4],
    EferLma set_EferLma[4..5],
    DebugActive set_DebugActive[5..6],
    InterruptionPending set_InterruptionPending[6..7],
    Reserved0 set_Reserved0[7..12],
    InterruptShadow set_InterruptShadow[12..13],
    Reserved1 set_Reserved1[13..16],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_VP_EXIT_CONTEXT {
    pub ExecutionState: WHV_X64_VP_EXECUTION_STATE,
    // Rust doesn't support bit fields so InstructionLength (4 bits) and Cr8 (4 bits)
    // are combined here
    pub InstructionLengthCr8: UINT8,
    pub Reserved: UINT8,
    pub Reserved2: UINT32,
    pub Cs: WHV_X64_SEGMENT_REGISTER,
    pub Rip: UINT64,
    pub Rflags: UINT64,
}

bitfield!(WHV_VP_EXIT_CONTEXT InstructionLengthCr8: UINT8[
    InstructionLength set_InstructionLength[0..4],
    Cr8 set_Cr8[4..8],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_MEMORY_ACCESS_INFO {
    pub AsUINT32: UINT32,
}

bitfield!(WHV_MEMORY_ACCESS_INFO  AsUINT32: UINT32[
    // WHV_MEMORY_ACCESS_TYPE
    AccessType set_AccessType[0..2],
    GpaUnmapped set_GpaUnmapped[2..3],
    GvaValid set_GvaValid[3..4],
    Reserved set_Reserved[4..32],
]);

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_MEMORY_ACCESS_CONTEXT {
    // Context of the virtual processor
    pub InstructionByteCount: UINT8,
    pub Reserved: [UINT8; 3],
    pub InstructionBytes: [UINT8; 16],

    // Memory access info
    pub AccessInfo: WHV_MEMORY_ACCESS_INFO,
    pub Gpa: WHV_GUEST_PHYSICAL_ADDRESS,
    pub Gva: WHV_GUEST_VIRTUAL_ADDRESS,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_IO_PORT_ACCESS_INFO {
    pub AsUINT32: UINT32,
}

bitfield!(WHV_X64_IO_PORT_ACCESS_INFO AsUINT32: UINT32[
    IsWrite set_IsWrite[0..1],
    AccessSize set_AccessSize[1..4],
    StringOp set_StringOp[4..5],
    RepPrefix set_RepPrefix[5..6],
    Reserved set_Reserved[6..32],
]);

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_IO_PORT_ACCESS_CONTEXT {
    // Context of the virtual processor
    pub InstructionByteCount: UINT8,
    pub Reserved: [UINT8; 3],
    pub InstructionBytes: [UINT8; 16],

    // I/O port access info
    pub AccessInfo: WHV_X64_IO_PORT_ACCESS_INFO,
    pub PortNumber: UINT16,
    pub Reserved2: [UINT16; 3],
    pub Rax: UINT64,
    pub Rcx: UINT64,
    pub Rsi: UINT64,
    pub Rdi: UINT64,
    pub Ds: WHV_X64_SEGMENT_REGISTER,
    pub Es: WHV_X64_SEGMENT_REGISTER,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_MSR_ACCESS_INFO {
    pub AsUINT32: UINT32,
}

bitfield!(WHV_X64_MSR_ACCESS_INFO AsUINT32: UINT32[
    IsWrite set_IsWrite[0..1],
    Reserved set_Reserved[1..32],
]);

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_MSR_ACCESS_CONTEXT {
    // MSR access info
    pub AccessInfo: WHV_X64_MSR_ACCESS_INFO,
    pub MsrNumber: UINT32,
    pub Rax: UINT64,
    pub Rdx: UINT64,
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_CPUID_ACCESS_CONTEXT {
    // CPUID access info
    pub Rax: UINT64,
    pub Rcx: UINT64,
    pub Rdx: UINT64,
    pub Rbx: UINT64,
    pub DefaultResultRax: UINT64,
    pub DefaultResultRcx: UINT64,
    pub DefaultResultRdx: UINT64,
    pub DefaultResultRbx: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_VP_EXCEPTION_INFO {
    pub AsUINT32: UINT32,
}

bitfield!(WHV_VP_EXCEPTION_INFO AsUINT32: UINT32[
    ErrorCodeValid set_ErrorCodeValid[0..1],
    SoftwareException set_SoftwareException[1..2],
    Reserved set_Reserved[2..32],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_VP_EXCEPTION_CONTEXT {
    pub InstructionByteCount: UINT8,
    pub Reserved: [UINT8; 3],
    pub InstructionBytes: [UINT8; 16],

    // Exception info
    pub ExceptionInfo: WHV_VP_EXCEPTION_INFO,
    // WHV_EXCEPTION_TYPE
    pub ExceptionType: UINT8,
    pub Reserved2: [UINT8; 3],
    pub ErrorCode: UINT32,
    pub ExceptionParameter: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_INTERRUPTION_DELIVERABLE_CONTEXT {
    pub DeliverableType: WHV_X64_PENDING_INTERRUPTION_TYPE,
}

// Context data for an exit caused by an APIC EOI of a level-triggered
// interrupt (WHvRunVpExitReasonX64ApicEoi)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_APIC_EOI_CONTEXT {
    pub InterruptVector: UINT32,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_UNSUPPORTED_FEATURE_CONTEXT {
    pub FeatureCode: WHV_X64_UNSUPPORTED_FEATURE_CODE,
    pub Reserved: UINT32,
    pub FeatureParameter: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_RUN_VP_CANCELED_CONTEXT {
    pub CancelReason: WHV_RUN_VP_CANCEL_REASON,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_RUN_VP_EXIT_CONTEXT_anon_union {
    pub MemoryAccess: WHV_MEMORY_ACCESS_CONTEXT,
    pub IoPortAccess: WHV_X64_IO_PORT_ACCESS_CONTEXT,
    pub MsrAccess: WHV_X64_MSR_ACCESS_CONTEXT,
    pub CpuidAccess: WHV_X64_CPUID_ACCESS_CONTEXT,
    pub VpException: WHV_VP_EXCEPTION_CONTEXT,
    pub InterruptWindow: WHV_X64_INTERRUPTION_DELIVERABLE_CONTEXT,
    pub UnsupportedFeature: WHV_X64_UNSUPPORTED_FEATURE_CONTEXT,
    pub CancelReason: WHV_RUN_VP_CANCELED_CONTEXT,
    pub ApicEoi: WHV_X64_APIC_EOI_CONTEXT,
}

impl Default for WHV_RUN_VP_EXIT_CONTEXT_anon_union {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Copy, Clone, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_RUN_VP_EXIT_CONTEXT {
    pub ExitReason: WHV_RUN_VP_EXIT_REASON,
    pub Reserved: UINT32,
    pub VpContext: WHV_VP_EXIT_CONTEXT,
    pub anon_union: WHV_RUN_VP_EXIT_CONTEXT_anon_union,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(align(16))]
#[repr(C)]
pub struct WHV_UINT128 {
    pub Low64: UINT64,
    pub High64: UINT64,
    // Original type is a union that includes also:
    // UINT32  Dword[4];
}

impl fmt::Display for WHV_UINT128 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:016x}`{:016x}", self.High64, self.Low64)
    }
}

impl Shl<usize> for WHV_UINT128 {
    type Output = Self;

    fn shl(self, rhs: usize) -> WHV_UINT128 {
        let shifted_out_of_low = self.Low64 >> (64 - rhs);
        let shifted_high = (self.High64 << rhs) | (shifted_out_of_low);
        WHV_UINT128 {
            Low64: self.Low64 << rhs,
            High64: shifted_high,
        }
    }
}

impl Shr<usize> for WHV_UINT128 {
    type Output = Self;

    fn shr(self, rhs: usize) -> WHV_UINT128 {
        let shifted_out_of_high = self.High64 << (64 - rhs);
        let shifted_low = (self.Low64 >> rhs) | shifted_out_of_high;
        WHV_UINT128 {
            Low64: shifted_low,
            High64: self.High64 >> rhs,
        }
    }
}

impl BitAnd<u64> for WHV_UINT128 {
    type Output = Self;

    // rhs is the "righ-hand side" of the expression 'a & b'
    fn bitand(self, rhs: u64) -> Self {
        WHV_UINT128 {
            Low64: self.Low64 & rhs,
            High64: self.High64,
        }
    }
}

impl BitAnd for WHV_UINT128 {
    type Output = Self;

    // rhs is the "righ-hand side" of the expression 'a & b'
    fn bitand(self, rhs: Self) -> Self {
        WHV_UINT128 {
            Low64: self.Low64 & rhs.Low64,
            High64: self.High64 & rhs.High64,
        }
    }
}

impl BitOrAssign for WHV_UINT128 {
    fn bitor_assign(&mut self, rhs: Self) {
        self.High64 |= rhs.High64;
        self.Low64 |= rhs.Low64;
    }
}

impl BitAndAssign<u64> for WHV_UINT128 {
    fn bitand_assign(&mut self, rhs: u64) {
        self.High64 = self.High64;
        self.Low64 &= rhs;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_INTERRUPT_STATE_REGISTER {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_X64_INTERRUPT_STATE_REGISTER AsUINT64: UINT64[
    InterruptShadow set_InterruptShadow[0..1],
    NmiMasked set_NmiMasked[1..2],
    Reserved set_Reserved[2..64],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_PENDING_INTERRUPTION_REGISTER {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_X64_PENDING_INTERRUPTION_REGISTER AsUINT64: UINT64[
    InterruptionPending set_InterruptionPending[0..1],
    InterruptionType set_InterruptionType[1..4],  // WHV_X64_PENDING_INTERRUPTION_TYPE
    DeliverErrorCode set_DeliverErrorCode[4..5],
    InstructionLength set_InstructionLength[5..9],
    NestedEvent set_NestedEvent[9..10],
    Reserved set_Reserved[10..16],
    InterruptionVector set_InterruptionVector[16..32],
    ErrorCode set_ErrorCode[32..64],
]);

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_X64_PENDING_EVENT_TYPE {
    WHvX64PendingEventException = 0,
    WHvX64PendingEventExtInt = 5,
}

impl Default for WHV_X64_PENDING_EVENT_TYPE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_PENDING_EXCEPTION_EVENT {
    pub AsUINT128: WHV_UINT128,
}

bitfield128!(WHV_X64_PENDING_EXCEPTION_EVENT AsUINT128: UINT64 [
        EventPending set_EventPending[0..1],
        // Must be WHvX64PendingEventException
        EventType set_EventType[1..4],
        Reserved0 set_Reserved0[4..8],

        DeliverErrorCode set_DeliverErrorCode[8..9],
        Reserved1 set_Reserved1[9..16],
        Vector set_Vector[16..32],
        ErrorCode set_ErrorCode[32..64],
        ExceptionParameter set_ExceptionParameter[64..128],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_PENDING_EXT_INT_EVENT {
    pub AsUINT128: WHV_UINT128,
}

bitfield128!(WHV_X64_PENDING_EXT_INT_EVENT AsUINT128: UINT64 [
    EventPending set_EventPending[0..1],
    EventType set_EventType[1..4],
    Reserved0 set_Reserved0[4..8],
    Vector set_Vector[8..16],
    Reserved1 set_Reserved1[16..64],

    Reserved2 set_Reserved2[64..128],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_DELIVERABILITY_NOTIFICATIONS_REGISTER {
    pub AsUINT64: UINT64,
}

bitfield!(WHV_X64_DELIVERABILITY_NOTIFICATIONS_REGISTER AsUINT64: UINT64[
    NmiNotification set_NmiNotification[0..1],
    InterruptNotification set_InterruptNotification[1..2],
    InterruptPriority set_InterruptPriority[2..6],
    Reserved set_Reserved[6..64],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_FP_REGISTER {
    pub AsUINT128: WHV_UINT128,
    // TODO: add bitfields
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_FP_CONTROL_STATUS_REGISTER_32bit_mode_anon_struct {
    pub LastFpEip: UINT32,
    pub LastFpCs: UINT16,
    pub Reserved2: UINT16,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_union {
    pub LastFpRip: UINT64,
    pub anon_struct: WHV_X64_FP_CONTROL_STATUS_REGISTER_32bit_mode_anon_struct,
}

impl Default for WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_union {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Copy, Clone, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_struct {
    pub FpControl: UINT16,
    pub FpStatus: UINT16,
    pub FpTag: UINT8,
    pub Reserved: UINT8,
    pub LastFpOp: UINT16,
    pub anon_union: WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_union,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_X64_FP_CONTROL_STATUS_REGISTER {
    pub AsUINT128: WHV_UINT128,
    pub anon_struct: WHV_X64_FP_CONTROL_STATUS_REGISTER_anon_struct,
}

impl Default for WHV_X64_FP_CONTROL_STATUS_REGISTER {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_XMM_CONTROL_STATUS_REGISTER_32bit_mode_anon_struct {
    pub LastFpDp: UINT32,
    pub LastFpDs: UINT16,
    Reserved: UINT16,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_union {
    pub LastFpRdp: UINT64,
    pub anon_struct: WHV_X64_XMM_CONTROL_STATUS_REGISTER_32bit_mode_anon_struct,
}

impl Default for WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_union {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Copy, Clone, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_struct {
    pub XmmStatusControl: UINT32,
    pub XmmStatusControlMask: UINT32,
    pub anon_union: WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_union,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_X64_XMM_CONTROL_STATUS_REGISTER {
    pub anon_struct: WHV_X64_XMM_CONTROL_STATUS_REGISTER_anon_struct,
    pub AsUINT128: WHV_UINT128,
}

impl Default for WHV_X64_XMM_CONTROL_STATUS_REGISTER {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_REGISTER_VALUE {
    pub Reg128: WHV_UINT128,
    pub Reg64: UINT64,
    pub Reg32: UINT32,
    pub Reg16: UINT16,
    pub Reg8: UINT8,
    pub Fp: WHV_X64_FP_REGISTER,
    pub FpControlStatus: WHV_X64_FP_CONTROL_STATUS_REGISTER,
    pub XmmControlStatus: WHV_X64_XMM_CONTROL_STATUS_REGISTER,
    pub Segment: WHV_X64_SEGMENT_REGISTER,
    pub Table: WHV_X64_TABLE_REGISTER,
    pub InterruptState: WHV_X64_INTERRUPT_STATE_REGISTER,
    pub PendingInterruption: WHV_X64_PENDING_INTERRUPTION_REGISTER,
    pub DeliverabilityNotifications: WHV_X64_DELIVERABILITY_NOTIFICATIONS_REGISTER,
    pub ExceptionEvent: WHV_X64_PENDING_EXCEPTION_EVENT,
    pub ExtIntEvent: WHV_X64_PENDING_EXT_INT_EVENT,
}

impl Default for WHV_REGISTER_VALUE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_TRANSLATE_GVA_RESULT {
    pub ResultCode: WHV_TRANSLATE_GVA_RESULT_CODE,
    pub Reserved: UINT32,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_INTERRUPT_TYPE {
    WHvX64InterruptTypeFixed = 0,
    WHvX64InterruptTypeLowestPriority = 1,
    WHvX64InterruptTypeNmi = 4,
    WHvX64InterruptTypeInit = 5,
    WHvX64InterruptTypeSipi = 6,
    WHvX64InterruptTypeLocalInt1 = 9,
}

impl Default for WHV_INTERRUPT_TYPE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_INTERRUPT_DESTINATION_MODE {
    WHvX64InterruptDestinationModePhysical = 0,
    WHvX64InterruptDestinationModeLogical = 1,
}

impl Default for WHV_INTERRUPT_DESTINATION_MODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_INTERRUPT_TRIGGER_MODE {
    WHvX64InterruptTriggerModeEdge = 0,
    WHvX64InterruptTriggerModeLevel = 1,
}

impl Default for WHV_INTERRUPT_TRIGGER_MODE {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_INTERRUPT_CONTROL {
    // Rust doesn't support bit fields so Type (8 bits), DestinationMode (4 bits),
    // TriggerMode (4 bits) and the remainder Reserved (48 bits) are combined here
    pub TypeDestinationModeTriggerModeReserved: UINT64,
    pub Destination: UINT32,
    pub Vector: UINT32,
}

bitfield!(WHV_INTERRUPT_CONTROL TypeDestinationModeTriggerModeReserved: UINT64 [
    InterruptType set_InterruptType[0..8],
    DestinationMode set_DestinationMode[8..12],
    TriggerMode set_TriggerMode[12..16],
    Reserved set_Reserved[16..64],
]);

// WHvGetPartitionCounters types
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_PARTITION_COUNTER_SET {
    WHvPartitionCounterSetMemory = 0,
}

impl Default for WHV_PARTITION_COUNTER_SET {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PARTITION_MEMORY_COUNTERS {
    pub Mapped4KPageCount: UINT64,
    pub Mapped2MPageCount: UINT64,
    pub Mapped1GPageCount: UINT64,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WHV_PROCESSOR_COUNTER_SET {
    WHvProcessorCounterSetRuntime = 0,
    WHvProcessorCounterSetIntercepts = 1,
    WHvProcessorCounterSetEvents = 2,
    WHvProcessorCounterSetApic = 3,
}

impl Default for WHV_PROCESSOR_COUNTER_SET {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PROCESSOR_RUNTIME_COUNTERS {
    pub TotalRuntime100ns: UINT64,
    pub HypervisorRuntime100ns: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PROCESSOR_INTERCEPT_COUNTER {
    pub Count: UINT64,
    pub Time100ns: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PROCESSOR_INTERCEPT_COUNTERS {
    pub PageInvalidations: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub ControlRegisterAccesses: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub IoInstructions: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub HaltInstructions: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub CpuidInstructions: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub MsrAccesses: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub OtherIntercepts: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub PendingInterrupts: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub EmulatedInstructions: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub DebugRegisterAccesses: WHV_PROCESSOR_INTERCEPT_COUNTER,
    pub PageFaultIntercepts: WHV_PROCESSOR_INTERCEPT_COUNTER,
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PROCESSOR_EVENT_COUNTERS {
    pub PageFaultCount: UINT64,
    pub ExceptionCount: UINT64,
    pub InterruptCount: UINT64,
}

#[derive(Debug, Copy, Clone, PartialEq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_PROCESSOR_APIC_COUNTERS {
    pub MmioAccessCount: UINT64,
    pub EoiAccessCount: UINT64,
    pub TprAccessCount: UINT64,
    pub SentIpiCount: UINT64,
    pub SelfIpiCount: UINT64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn test_data_type_sizes() {
        // Make sure all unions and structs have a size that matches the value
        // obtained with a sizeof() in C.
        assert_eq!(std::mem::size_of::<WHV_CAPABILITY>(), 8);
        assert_eq!(std::mem::size_of::<WHV_X64_CPUID_RESULT>(), 32);
        assert_eq!(std::mem::size_of::<WHV_PARTITION_PROPERTY>(), 32);
        assert_eq!(std::mem::size_of::<WHV_X64_SEGMENT_REGISTER>(), 16);
        assert_eq!(std::mem::size_of::<WHV_X64_TABLE_REGISTER>(), 16);
        assert_eq!(std::mem::size_of::<WHV_VP_EXIT_CONTEXT>(), 40);
        assert_eq!(std::mem::size_of::<WHV_MEMORY_ACCESS_CONTEXT>(), 40);
        assert_eq!(std::mem::size_of::<WHV_X64_IO_PORT_ACCESS_CONTEXT>(), 96);
        assert_eq!(std::mem::size_of::<WHV_X64_MSR_ACCESS_CONTEXT>(), 24);
        assert_eq!(std::mem::size_of::<WHV_X64_CPUID_ACCESS_CONTEXT>(), 64);
        assert_eq!(std::mem::size_of::<WHV_VP_EXCEPTION_CONTEXT>(), 40);
        assert_eq!(
            std::mem::size_of::<WHV_X64_INTERRUPTION_DELIVERABLE_CONTEXT>(),
            4
        );
        assert_eq!(
            std::mem::size_of::<WHV_X64_UNSUPPORTED_FEATURE_CONTEXT>(),
            16
        );
        assert_eq!(std::mem::size_of::<WHV_RUN_VP_CANCELED_CONTEXT>(), 4);
        assert_eq!(std::mem::size_of::<WHV_RUN_VP_EXIT_CONTEXT>(), 144);
        assert_eq!(std::mem::size_of::<WHV_UINT128>(), 16);
        assert_eq!(std::mem::size_of::<WHV_REGISTER_VALUE>(), 16);
        assert_eq!(std::mem::size_of::<WHV_TRANSLATE_GVA_RESULT>(), 8);
    }

    #[test]
    fn test_whv_uint128_shl() {
        let shifted = WHV_UINT128 {
            High64: 0x5555_4444_3333_89ab,
            Low64: 0xcdef_2222_1111_0000,
        } << 16;
        assert_eq!(shifted.High64, 0x4444_3333_89ab_cdef);
        assert_eq!(shifted.Low64, 0x2222_1111_0000_0000);
    }

    #[test]
    fn test_whv_uint128_shr() {
        let shifted = WHV_UINT128 {
            High64: 0x5555_4444_3333_89ab,
            Low64: 0xcdef_2222_1111_0000,
        } >> 16;
        assert_eq!(shifted.High64, 0x0000_5555_4444_3333);
        assert_eq!(shifted.Low64, 0x89ab_cdef_2222_1111);
    }
}
