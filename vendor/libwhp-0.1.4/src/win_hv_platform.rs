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

use common::*;
use win_hv_platform_defs::*;

#[allow(non_snake_case)]
#[link(name = "WinHvPlatform")]
extern "stdcall" {
    pub fn WHvCreatePartition(Partition: *mut WHV_PARTITION_HANDLE) -> HRESULT;
    pub fn WHvSetupPartition(Partition: WHV_PARTITION_HANDLE) -> HRESULT;
    pub fn WHvDeletePartition(Partition: WHV_PARTITION_HANDLE) -> HRESULT;
    pub fn WHvCreateVirtualProcessor(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        Flags: UINT32,
    ) -> HRESULT;
    pub fn WHvDeleteVirtualProcessor(Partition: WHV_PARTITION_HANDLE, VpIndex: UINT32) -> HRESULT;
    pub fn WHvSetPartitionProperty(
        Partition: WHV_PARTITION_HANDLE,
        PropertyCode: WHV_PARTITION_PROPERTY_CODE,
        PropertyBuffer: *const VOID,
        PropertyBufferSizeInBytes: UINT32,
    ) -> HRESULT;
    pub fn WHvGetPartitionProperty(
        Partition: WHV_PARTITION_HANDLE,
        PropertyCode: WHV_PARTITION_PROPERTY_CODE,
        PropertyBuffer: *mut VOID,
        PropertyBufferSizeInBytes: UINT32,
        WrittenSizeInBytes: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvGetCapability(
        CapabilityCode: WHV_CAPABILITY_CODE,
        CapabilityBuffer: *mut VOID,
        CapabilityBufferSizeInBytes: UINT32,
        WrittenSizeInBytes: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvCancelRunVirtualProcessor(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        Flags: UINT32,
    ) -> HRESULT;
    pub fn WHvRunVirtualProcessor(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        ExitContext: *mut VOID,
        ExitContextSizeInBytes: UINT32,
    ) -> HRESULT;
    pub fn WHvSetVirtualProcessorRegisters(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        RegisterNames: *const WHV_REGISTER_NAME,
        RegisterCount: UINT32,
        RegisterValues: *const WHV_REGISTER_VALUE,
    ) -> HRESULT;
    pub fn WHvGetVirtualProcessorRegisters(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        RegisterNames: *const WHV_REGISTER_NAME,
        RegisterCount: UINT32,
        RegisterValues: *mut WHV_REGISTER_VALUE,
    ) -> HRESULT;
    pub fn WHvMapGpaRange(
        Partition: WHV_PARTITION_HANDLE,
        SourceAddress: *const VOID,
        GuestAddress: WHV_GUEST_PHYSICAL_ADDRESS,
        SizeInBytes: UINT64,
        Flags: WHV_MAP_GPA_RANGE_FLAGS,
    ) -> HRESULT;
    pub fn WHvUnmapGpaRange(
        Partition: WHV_PARTITION_HANDLE,
        GuestAddress: WHV_GUEST_PHYSICAL_ADDRESS,
        SizeInBytes: UINT64,
    ) -> HRESULT;
    pub fn WHvTranslateGva(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        Gva: WHV_GUEST_VIRTUAL_ADDRESS,
        TranslateFlags: WHV_TRANSLATE_GVA_FLAGS,
        TranslationResult: *mut WHV_TRANSLATE_GVA_RESULT,
        Gpa: *mut WHV_GUEST_PHYSICAL_ADDRESS,
    ) -> HRESULT;
    pub fn WHvGetVirtualProcessorInterruptControllerState(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        State: *mut VOID,
        StateSize: UINT32,
        WrittenSize: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvSetVirtualProcessorInterruptControllerState(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        State: *const VOID,
        StateSize: UINT32,
    ) -> HRESULT;
    pub fn WHvRequestInterrupt(
        Partition: WHV_PARTITION_HANDLE,
        Interrupt: *const WHV_INTERRUPT_CONTROL,
        InterruptControlSize: UINT32,
    ) -> HRESULT;
    pub fn WHvQueryGpaRangeDirtyBitmap(
        Partition: WHV_PARTITION_HANDLE,
        GuestAddress: WHV_GUEST_PHYSICAL_ADDRESS,
        RangeSizeInBytes: UINT64,
        Bitmap: *mut UINT64,
        BitmapSizeInBytes: UINT32,
    ) -> HRESULT;
    pub fn WHvGetPartitionCounters(
        Partition: WHV_PARTITION_HANDLE,
        CounterSet: WHV_PARTITION_COUNTER_SET,
        Buffer: *mut VOID,
        BufferSizeInBytes: UINT32,
        BytesWritten: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvGetVirtualProcessorCounters(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        CounterSet: WHV_PROCESSOR_COUNTER_SET,
        Buffer: *mut VOID,
        BufferSizeInBytes: UINT32,
        BytesWritten: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvGetVirtualProcessorXsaveState(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        Buffer: *mut VOID,
        BufferSizeInBytes: UINT32,
        BytesWritten: *mut UINT32,
    ) -> HRESULT;
    pub fn WHvSetVirtualProcessorXsaveState(
        Partition: WHV_PARTITION_HANDLE,
        VpIndex: UINT32,
        Buffer: *const VOID,
        BufferSizeInBytes: UINT32,
    ) -> HRESULT;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    fn check_hypervisor_present() {
        let mut capability: WHV_CAPABILITY = Default::default();
        let mut written_size: UINT32 = 0;

        let result = unsafe {
            WHvGetCapability(
                WHV_CAPABILITY_CODE::WHvCapabilityCodeHypervisorPresent,
                &mut capability as *mut _ as *mut VOID,
                std::mem::size_of::<WHV_CAPABILITY>() as UINT32,
                &mut written_size,
            )
        };

        assert_eq!(result, S_OK, "WHvGetCapability failed with 0x{:X}", result);

        assert_eq!(
            std::mem::size_of::<BOOL>() as UINT32,
            written_size,
            "WrittenSizeInBytes does not match BOOL size {}",
            written_size
        );

        unsafe {
            assert_eq!(capability.HypervisorPresent, TRUE, "Hypervisor not present");
        };
    }

    fn with_partition<F>(action: F)
    where
        F: Fn(WHV_PARTITION_HANDLE),
    {
        check_hypervisor_present();

        let mut part: WHV_PARTITION_HANDLE = std::ptr::null_mut();

        let result = unsafe { WHvCreatePartition(&mut part) };
        assert_eq!(
            result, S_OK,
            "WHvCreatePartition failed with 0x{:X}",
            result
        );

        action(part);

        let result = unsafe { WHvDeletePartition(part) };
        assert_eq!(
            result, S_OK,
            "WHvDeletePartition failed with 0x{:X}",
            result
        );
    }

    fn with_vcpu<F>(part: WHV_PARTITION_HANDLE, action: F)
    where
        F: Fn(UINT32),
    {
        let vp_index = 0;
        let mut prop: WHV_PARTITION_PROPERTY = Default::default();

        let result = unsafe {
            prop.ProcessorCount = 1;

            WHvSetPartitionProperty(
                part,
                WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
                &mut prop as *mut _ as *mut VOID,
                std::mem::size_of::<WHV_PARTITION_PROPERTY>() as UINT32,
            )
        };
        assert_eq!(
            result, S_OK,
            "WHvSetPartitionProperty failed with 0x{:X}",
            result
        );

        let result = unsafe { WHvSetupPartition(part) };
        assert_eq!(result, S_OK, "WHvSetupPartition failed with 0x{:X}", result);

        let result = unsafe { WHvCreateVirtualProcessor(part, vp_index, 0) };
        assert_eq!(
            result, S_OK,
            "WHvCreateVirtualProcessor failed with 0x{:X}",
            result
        );

        action(vp_index);

        let result = unsafe { WHvDeleteVirtualProcessor(part, vp_index) };
        assert_eq!(
            result, S_OK,
            "WHvDeleteVirtualProcessor failed with 0x{:X}",
            result
        );
    }

    #[test]
    fn test_get_capability() {
        let mut capability: WHV_CAPABILITY = Default::default();
        let mut written_size: UINT32 = 0;

        let result = unsafe {
            WHvGetCapability(
                WHV_CAPABILITY_CODE::WHvCapabilityCodeFeatures,
                &mut capability as *mut _ as *mut VOID,
                std::mem::size_of::<WHV_CAPABILITY>() as UINT32,
                &mut written_size,
            )
        };

        assert_eq!(result, S_OK, "WHvGetCapability failed with 0x{:X}", result);
        assert_eq!(
            std::mem::size_of::<UINT64>() as UINT32,
            written_size,
            "WrittenSizeInBytes does not match UINT64 size {}",
            written_size
        );
    }

    #[test]
    fn test_create_delete_partition() {
        with_partition(|_part| {});
    }

    #[test]
    fn test_set_get_partition_property() {
        with_partition(|part| {
            let mut prop: WHV_PARTITION_PROPERTY = Default::default();
            let mut prop_out: WHV_PARTITION_PROPERTY = Default::default();
            let mut written_size: UINT32 = 0;

            let result = unsafe {
                prop.ProcessorCount = 1;

                WHvSetPartitionProperty(
                    part,
                    WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
                    &prop as *const _ as *const VOID,
                    std::mem::size_of::<WHV_PARTITION_PROPERTY>() as UINT32,
                )
            };

            assert_eq!(
                result, S_OK,
                "WHvSetPartitionProperty failed with 0x{:X}",
                result
            );

            let result = unsafe {
                WHvGetPartitionProperty(
                    part,
                    WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
                    &mut prop_out as *mut _ as *mut VOID,
                    std::mem::size_of::<WHV_PARTITION_PROPERTY>() as UINT32,
                    &mut written_size,
                )
            };

            unsafe {
                assert_eq!(
                    prop.ProcessorCount, prop_out.ProcessorCount,
                    "Partition property value not matching"
                );
            }

            assert_eq!(
                result, S_OK,
                "WHvGetPartitionProperty failed with 0x{:X}",
                result
            );
            assert_eq!(
                std::mem::size_of::<BOOL>() as UINT32,
                written_size,
                "WrittenSizeInBytes does not match BOOL size {}",
                written_size
            );
        });
    }

    #[test]
    fn test_setup_partition() {
        with_partition(|part| {
            let result = unsafe { WHvSetupPartition(part) };
            assert_eq!(
                result, WHV_E_INVALID_PARTITION_CONFIG,
                "WHvSetupPartition failed with 0x{:X}",
                result
            );
        });
    }

    #[test]
    fn test_create_delete_vcpu() {
        with_partition(|part| {
            with_vcpu(part, |_vp_index| {});
        });
    }

    #[test]
    fn test_set_get_vcpu_registers() {
        with_partition(|part| {
            with_vcpu(part, |vp_index| {
                const NUM_REGS: UINT32 = 1;
                const REG_VALUE: UINT64 = 11111111;
                let mut reg_names: [WHV_REGISTER_NAME; NUM_REGS as usize] = Default::default();
                let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();
                let mut reg_values_out: [WHV_REGISTER_VALUE; NUM_REGS as usize] =
                    Default::default();

                reg_names[0] = WHV_REGISTER_NAME::WHvX64RegisterRax;
                reg_values[0].Reg64 = REG_VALUE;

                let result = unsafe {
                    WHvSetVirtualProcessorRegisters(
                        part,
                        vp_index,
                        reg_names.as_ptr(),
                        NUM_REGS,
                        reg_values.as_ptr(),
                    )
                };
                assert_eq!(
                    result, S_OK,
                    "WHvSetVirtualProcessorRegisters failed with 0x{:X}",
                    result
                );

                let result = unsafe {
                    WHvGetVirtualProcessorRegisters(
                        part,
                        vp_index,
                        reg_names.as_ptr(),
                        NUM_REGS,
                        reg_values_out.as_mut_ptr(),
                    )
                };
                assert_eq!(
                    result, S_OK,
                    "WHvGetVirtualProcessorRegisters failed with 0x{:X}",
                    result
                );

                unsafe {
                    assert_eq!(
                        reg_values_out[0].Reg64, REG_VALUE,
                        "Registers values fo not match"
                    );
                }
            });
        });
    }

    #[test]
    fn test_run_vcpu() {
        with_partition(|part| {
            with_vcpu(part, |vp_index| {
                let mut exit_context: WHV_RUN_VP_EXIT_CONTEXT = Default::default();
                let exit_context_size = std::mem::size_of::<WHV_RUN_VP_EXIT_CONTEXT>() as UINT32;

                let result = unsafe {
                    WHvRunVirtualProcessor(
                        part,
                        vp_index,
                        &mut exit_context as *mut _ as *mut VOID,
                        1,
                    )
                };

                assert_eq!(
                    result, WHV_E_INSUFFICIENT_BUFFER,
                    "WHvRunVirtualProcessor failed with 0x{:X}",
                    result
                );

                exit_context = Default::default();
                let result = unsafe {
                    WHvRunVirtualProcessor(
                        part,
                        vp_index,
                        &mut exit_context as *mut _ as *mut VOID,
                        exit_context_size,
                    )
                };

                assert_eq!(
                    result, S_OK,
                    "WHvRunVirtualProcessor failed with 0x{:X}",
                    result
                );

                assert_eq!(
                    exit_context.ExitReason,
                    WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonMemoryAccess,
                    "Unexpected exit reason"
                )
            });
        });
    }

    #[test]
    fn test_cancel_run_vcpu() {
        with_partition(|part| {
            with_vcpu(part, |vp_index| {
                let result = unsafe { WHvCancelRunVirtualProcessor(part, vp_index, 0) };

                assert_eq!(
                    result, S_OK,
                    "WHvCancelRunVirtualProcessor failed with 0x{:X}",
                    result
                );
            });
        });
    }

    #[test]
    fn test_map_gpa_range() {
        with_partition(|part| {
            const SIZE: UINT64 = 1024;
            let source_address = Box::new([0; SIZE as usize]);
            let guest_address: WHV_GUEST_PHYSICAL_ADDRESS = 0;

            let result = unsafe {
                WHvMapGpaRange(
                    part,
                    source_address.as_ptr() as *const VOID,
                    guest_address,
                    SIZE,
                    WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagRead
                        | WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagWrite,
                )
            };

            // TODO(alexpilotti): modify this test to have an S_OK result.
            // This error is expected with this test setup
            assert_eq!(
                result, E_INVALIDARG,
                "WHvMapGpaRange failed with 0x{:X}",
                result
            );
        });
    }

    #[test]
    #[ignore]
    fn test_unmap_gpa_range_not_found() {
        with_partition(|part| {
            const SIZE: UINT64 = 1024;
            let guest_address: WHV_GUEST_PHYSICAL_ADDRESS = 0;

            let result = unsafe { WHvUnmapGpaRange(part, guest_address, SIZE) };

            assert_eq!(
                result, WHV_E_GPA_RANGE_NOT_FOUND,
                "WHvUnmapGpaRange failed with 0x{:X}",
                result
            );
        });
    }

    #[test]
    fn test_translate_gva() {
        with_partition(|part| {
            with_vcpu(part, |vp_index| {
                let gva: WHV_GUEST_PHYSICAL_ADDRESS = 0;
                let mut gpa: WHV_GUEST_PHYSICAL_ADDRESS = 0;
                let mut translation_result: WHV_TRANSLATE_GVA_RESULT = Default::default();

                let result = unsafe {
                    WHvTranslateGva(
                        part,
                        vp_index,
                        gva,
                        WHV_TRANSLATE_GVA_FLAGS::WHvTranslateGvaFlagValidateRead,
                        &mut translation_result,
                        &mut gpa,
                    )
                };

                assert_eq!(result, S_OK, "WHvTranslateGva failed with 0x{:X}", result);

                // This API changed, it used to return GpaUnmapped, now it runs Success.
                // So support both versions for now.
                //
                let result = translation_result.ResultCode;
                assert!(
                    result == WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultGpaUnmapped ||
                    result == WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultSuccess,
                    "Unexpected translation result code {:?}", result);

                assert_eq!(gpa, 0, "Unexpected GPA value");
            });
        });
    }
}
