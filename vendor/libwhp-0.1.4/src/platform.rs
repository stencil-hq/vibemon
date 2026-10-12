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

use common::*;
use memory::*;
use std;
use std::sync::Arc;
use win_hv_platform::*;
pub use win_hv_platform_defs::*;
pub use win_hv_platform_defs_internal::*;
pub use x86_64::{LapicStateRaw, XsaveArea};

pub fn get_capability(capability_code: WHV_CAPABILITY_CODE) -> Result<WHV_CAPABILITY, WHPError> {
    let mut capability: WHV_CAPABILITY = Default::default();
    let mut written_size: UINT32 = 0;

    check_result(unsafe {
        WHvGetCapability(
            capability_code,
            &mut capability as *mut _ as *mut VOID,
            std::mem::size_of::<WHV_CAPABILITY>() as UINT32,
            &mut written_size,
        )
    })?;
    Ok(capability)
}

pub struct PartitionHandle {
    handle: WHV_PARTITION_HANDLE,
}

// Handles can be safely shared among threads.
unsafe impl Send for PartitionHandle {}
unsafe impl Sync for PartitionHandle {}

impl PartitionHandle {
    fn handle(&self) -> WHV_PARTITION_HANDLE {
        self.handle
    }
}

impl Drop for PartitionHandle {
    fn drop(&mut self) {
        check_result(unsafe { WHvDeletePartition(self.handle) }).unwrap();
    }
}

pub struct Partition {
    partition: Arc<PartitionHandle>,
}

impl Clone for Partition {
    fn clone(&self) -> Partition {
        Partition {
            partition: self.partition.clone()
        }
    }
}

impl Partition {
    pub fn new() -> Result<Partition, WHPError> {
        let mut handle: WHV_PARTITION_HANDLE = std::ptr::null_mut();
        check_result(unsafe { WHvCreatePartition(&mut handle) })?;
        Ok(Partition {
            partition: Arc::new(PartitionHandle { handle: handle }),
        })
    }

    pub fn set_property(
        &self,
        property_code: WHV_PARTITION_PROPERTY_CODE,
        property: &WHV_PARTITION_PROPERTY,
    ) -> Result<(), WHPError> {
        self.set_property_from_buffer(
            property_code,
            property as *const _ as *const VOID,
            std::mem::size_of::<WHV_PARTITION_PROPERTY>() as UINT32,
        )?;
        Ok(())
    }

    pub fn set_property_cpuid_exits(&self, cpuids: &[UINT32]) -> Result<(), WHPError> {
        self.set_property_from_buffer(
            WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeCpuidExitList,
            cpuids.as_ptr() as *const VOID,
            (std::mem::size_of::<UINT32>() * cpuids.len()) as UINT32,
        )?;
        Ok(())
    }

    pub fn set_property_cpuid_results(
        &self,
        cpuid_results: &[WHV_X64_CPUID_RESULT],
    ) -> Result<(), WHPError> {
        self.set_property_from_buffer(
            WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeCpuidResultList,
            cpuid_results.as_ptr() as *const VOID,
            (std::mem::size_of::<WHV_X64_CPUID_RESULT>() * cpuid_results.len()) as UINT32,
        )?;
        Ok(())
    }

    fn set_property_from_buffer(
        &self,
        property_code: WHV_PARTITION_PROPERTY_CODE,
        property: *const VOID,
        size: UINT32,
    ) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvSetPartitionProperty(
                self.partition.handle(),
                property_code,
                property,
                size,
            )
        })?;
        Ok(())
    }

    pub fn get_property(
        &self,
        property_code: WHV_PARTITION_PROPERTY_CODE,
    ) -> Result<WHV_PARTITION_PROPERTY, WHPError> {
        let mut property: WHV_PARTITION_PROPERTY = Default::default();
        self.get_property_buffer(
            property_code,
            &mut property as *mut _ as *mut VOID,
            std::mem::size_of::<WHV_PARTITION_PROPERTY>() as UINT32,
        )?;
        Ok(property)
    }

    fn get_property_buffer(
        &self,
        property_code: WHV_PARTITION_PROPERTY_CODE,
        property: *mut VOID,
        size: UINT32,
    ) -> Result<UINT32, WHPError> {
        let mut written_size: UINT32 = 0;

        check_result(unsafe {
            WHvGetPartitionProperty(
                self.partition.handle(),
                property_code,
                property,
                size,
                &mut written_size,
            )
        })?;
        Ok(written_size)
    }

    pub fn setup(&self) -> Result<(), WHPError> {
        check_result(unsafe { WHvSetupPartition(self.partition.handle()) })?;
        Ok(())
    }

    pub fn create_virtual_processor(&self, index: UINT32) -> Result<VirtualProcessor, WHPError> {
        check_result(unsafe {
            WHvCreateVirtualProcessor(self.partition.handle(), index, 0)
        })?;
        Ok(VirtualProcessor {
            partition: Arc::clone(&self.partition),
            index: index,
        })
    }

    pub fn map_gpa_range<T: Memory>(
        &self,
        source_address: &T,
        guest_address: WHV_GUEST_PHYSICAL_ADDRESS,
        size: UINT64,
        flags: WHV_MAP_GPA_RANGE_FLAGS,
    ) -> Result<GPARangeMapping, WHPError> {
        check_result(unsafe {
            WHvMapGpaRange(
                self.partition.handle(),
                source_address.as_ptr(),
                guest_address,
                size,
                flags,
            )
        })?;
        Ok(GPARangeMapping {
            partition: Arc::clone(&self.partition),
            source_address: source_address.as_ptr(),
            guest_address: guest_address,
            size: size,
            flags: flags,
        })
    }

    pub fn request_interrupt(&self, interrupt: &WHV_INTERRUPT_CONTROL) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvRequestInterrupt(
                self.partition.handle(),
                interrupt,
                std::mem::size_of::<WHV_INTERRUPT_CONTROL>() as UINT32,
            )
        })?;
        Ok(())
    }
}

pub struct GPARangeMapping {
    partition: Arc<PartitionHandle>,
    source_address: *const VOID,
    guest_address: WHV_GUEST_PHYSICAL_ADDRESS,
    size: UINT64,
    flags: WHV_MAP_GPA_RANGE_FLAGS,
}

impl GPARangeMapping {
    pub fn get_source_address(&self) -> *const VOID {
        self.source_address
    }

    pub fn get_guest_address(&self) -> WHV_GUEST_PHYSICAL_ADDRESS {
        self.guest_address
    }

    pub fn get_size(&self) -> UINT64 {
        self.size
    }

    pub fn get_flags(&self) -> WHV_MAP_GPA_RANGE_FLAGS {
        self.flags
    }
}

impl Drop for GPARangeMapping {
    fn drop(&mut self) {
        let p = &self.partition;
        check_result(unsafe { WHvUnmapGpaRange(p.handle(), self.guest_address, self.size) })
            .unwrap();
    }
}

pub struct VirtualProcessor {
    partition: Arc<PartitionHandle>,
    index: UINT32,
}

impl VirtualProcessor {
    pub fn index(&self) -> UINT32 {
        return self.index;
    }

    pub fn run(&self) -> Result<WHV_RUN_VP_EXIT_CONTEXT, WHPError> {
        let mut exit_context: WHV_RUN_VP_EXIT_CONTEXT = Default::default();
        let exit_context_size = std::mem::size_of::<WHV_RUN_VP_EXIT_CONTEXT>() as UINT32;

        check_result(unsafe {
            WHvRunVirtualProcessor(
                self.partition.handle(),
                self.index,
                &mut exit_context as *mut _ as *mut VOID,
                exit_context_size,
            )
        })?;
        Ok(exit_context)
    }

    pub fn cancel_run(&self) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvCancelRunVirtualProcessor(self.partition.handle(), self.index, 0)
        })?;
        Ok(())
    }

    pub fn set_registers(
        &self,
        reg_names: &[WHV_REGISTER_NAME],
        reg_values: &[WHV_REGISTER_VALUE],
    ) -> Result<(), WHPError> {
        let num_regs = reg_names.len();

        if num_regs != reg_values.len() {
            panic!("reg_names and reg_values must have the same length")
        }

        check_result(unsafe {
            WHvSetVirtualProcessorRegisters(
                self.partition.handle(),
                self.index,
                reg_names.as_ptr(),
                num_regs as UINT32,
                reg_values.as_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn get_registers(
        &self,
        reg_names: &[WHV_REGISTER_NAME],
        reg_values: &mut [WHV_REGISTER_VALUE],
    ) -> Result<(), WHPError> {
        let num_regs = reg_names.len();

        if num_regs != reg_values.len() {
            panic!("reg_names and reg_values must have the same length")
        }

        check_result(unsafe {
            WHvGetVirtualProcessorRegisters(
                self.partition.handle(),
                self.index,
                reg_names.as_ptr(),
                num_regs as UINT32,
                reg_values.as_mut_ptr(),
            )
        })?;
        Ok(())
    }

    pub fn translate_gva(
        &self,
        gva: WHV_GUEST_VIRTUAL_ADDRESS,
        flags: WHV_TRANSLATE_GVA_FLAGS,
    ) -> Result<(WHV_TRANSLATE_GVA_RESULT, WHV_GUEST_PHYSICAL_ADDRESS), WHPError> {
        let mut gpa: WHV_GUEST_PHYSICAL_ADDRESS = 0;
        let mut translation_result: WHV_TRANSLATE_GVA_RESULT = Default::default();

        check_result(unsafe {
            WHvTranslateGva(
                self.partition.handle(),
                self.index,
                gva,
                flags,
                &mut translation_result,
                &mut gpa,
            )
        })?;
        Ok((translation_result, gpa))
    }

    pub fn query_gpa_range_dirty_bitmap(
        &self,
        gva: WHV_GUEST_PHYSICAL_ADDRESS,
        range_size_in_bytes: UINT64,
        bitmap_size_in_bytes: UINT32,
    ) -> Result<Box<[UINT64]>, WHPError> {
        let num_elem = bitmap_size_in_bytes / std::mem::size_of::<UINT64>() as UINT32;
        let mut bitmap: Box<[UINT64]> = vec![0; num_elem as usize].into_boxed_slice();

        check_result(unsafe {
            WHvQueryGpaRangeDirtyBitmap(
                self.partition.handle(),
                gva,
                range_size_in_bytes,
                bitmap.as_mut_ptr(),
                bitmap_size_in_bytes,
            )
        })?;
        Ok(bitmap)
    }

    pub fn get_lapic(&self) -> Result<LapicStateRaw, WHPError> {
        let mut state: LapicStateRaw = Default::default();
        let mut written_size: UINT32 = 0;

        check_result(unsafe {
            WHvGetVirtualProcessorInterruptControllerState(
                self.partition.handle(),
                self.index,
                &mut state as *mut _ as *mut VOID,
                std::mem::size_of::<LapicStateRaw>() as UINT32,
                &mut written_size,
            )
        })?;
        Ok(state)
    }

    pub fn set_lapic(&self, state: &LapicStateRaw) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvSetVirtualProcessorInterruptControllerState(
                self.partition.handle(),
                self.index,
                state as *const _ as *const VOID,
                std::mem::size_of::<LapicStateRaw>() as UINT32,
            )
        })?;
        Ok(())
    }

    pub fn request_interrupt(&self, interrupt: &WHV_INTERRUPT_CONTROL) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvRequestInterrupt(
                self.partition.handle(),
                interrupt,
                std::mem::size_of::<WHV_INTERRUPT_CONTROL>() as UINT32,
            )
        })?;
        Ok(())
    }

    #[allow(unreachable_patterns)] // Future-proof against new WHV_PARTITION_COUNTER_SET values
    pub fn get_partition_counters(
        &self,
        partition_counter_set: WHV_PARTITION_COUNTER_SET,
    ) -> Result<WHV_PARTITION_COUNTERS, WHPError> {
        let mut partition_counters: WHV_PARTITION_COUNTERS = Default::default();
        let mut bytes_written: UINT32 = 0;

        let buffer_size_in_bytes = match partition_counter_set {
            WHV_PARTITION_COUNTER_SET::WHvPartitionCounterSetMemory => {
                std::mem::size_of::<WHV_PARTITION_MEMORY_COUNTERS>() as UINT32
            }
            _ => panic!("Unknown partition counter set enum value"),
        };

        check_result(unsafe {
            WHvGetPartitionCounters(
                self.partition.handle(),
                partition_counter_set,
                &mut partition_counters as *mut _ as *mut VOID,
                buffer_size_in_bytes as UINT32,
                &mut bytes_written,
            )
        })?;
        Ok(partition_counters)
    }

    #[allow(unreachable_patterns)] // Future-proof against new WHV_PROCESSOR_COUNTER_SET values
    pub fn get_processor_counters(
        &self,
        processor_counter_set: WHV_PROCESSOR_COUNTER_SET,
    ) -> Result<WHV_PROCESSOR_COUNTERS, WHPError> {
        let mut processor_counters: WHV_PROCESSOR_COUNTERS = Default::default();
        let mut bytes_written: UINT32 = 0;

        let buffer_size_in_bytes = match processor_counter_set {
            WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetRuntime => {
                std::mem::size_of::<WHV_PROCESSOR_RUNTIME_COUNTERS>()
            }
            WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetIntercepts => {
                std::mem::size_of::<WHV_PROCESSOR_INTERCEPT_COUNTERS>()
            }
            WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetEvents => {
                std::mem::size_of::<WHV_PROCESSOR_EVENT_COUNTERS>()
            }
            WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetApic => {
                std::mem::size_of::<WHV_PROCESSOR_APIC_COUNTERS>()
            }
            _ => panic!("Unknown processor counter set enum value"),
        };

        check_result(unsafe {
            WHvGetVirtualProcessorCounters(
                self.partition.handle(),
                self.index,
                processor_counter_set,
                &mut processor_counters as *mut _ as *mut VOID,
                buffer_size_in_bytes as UINT32,
                &mut bytes_written,
            )
        })?;
        Ok(processor_counters)
    }

    pub fn get_xsave_state(&self) -> Result<XsaveArea, WHPError> {
        let mut xsave_area: XsaveArea = Default::default();
        let mut bytes_written: UINT32 = 0;

        check_result(unsafe {
            WHvGetVirtualProcessorXsaveState(
                self.partition.handle(),
                self.index,
                &mut xsave_area as *mut _ as *mut VOID,
                std::mem::size_of::<XsaveArea>() as UINT32,
                &mut bytes_written,
            )
        })?;
        Ok(xsave_area)
    }

    pub fn set_xsave_state(&self, xsave_area: XsaveArea) -> Result<(), WHPError> {
        check_result(unsafe {
            WHvSetVirtualProcessorXsaveState(
                self.partition.handle(),
                self.index,
                &xsave_area as *const _ as *const VOID,
                std::mem::size_of::<XsaveArea>() as UINT32,
            )
        })?;
        Ok(())
    }
}

impl Drop for VirtualProcessor {
    fn drop(&mut self) {
        check_result(unsafe {
            WHvDeleteVirtualProcessor(self.partition.handle(), self.index)
        })
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn test_create_delete_partition() {
        println!("CreateDeletePartition");
        let p: Partition = Partition::new().unwrap();
        drop(p);
    }

    #[test]
    fn test_delete_partition_panic() {
        let result = std::panic::catch_unwind(|| {
            // Create an invalid partition
            let _p = Partition {
                partition: Arc::new(PartitionHandle {
                    handle: std::ptr::null_mut(),
                }),
            };
        });
        assert!(result.is_err(), "Drop was suppoesed to panic");
    }

    #[test]
    fn test_get_capability() {
        let _capability: WHV_CAPABILITY =
            get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeHypervisorPresent).unwrap();
    }

    #[test]
    fn test_set_get_partition_property() {
        let p: Partition = Partition::new().unwrap();
        let property_code = WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount;
        let mut property: WHV_PARTITION_PROPERTY = Default::default();
        property.ProcessorCount = 1;

        p.set_property(property_code, &property).unwrap();
        let property_out = p.get_property(property_code).unwrap();

        unsafe {
            assert_eq!(
                property.ProcessorCount, property_out.ProcessorCount,
                "The property value is not matching"
            );
        }
    }

    #[test]
    fn test_set_get_partition_property_cpuid_exits() {
        let p: Partition = Partition::new().unwrap();
        let cpuids: [UINT32; 2] = [1, 2];

        // Getting this property is not supported
        assert_eq!(
            p.set_property_cpuid_exits(&cpuids).ok(),
            Some(()),
            "set_property_cpuid_exits failed"
        );
    }

    #[test]
    fn test_set_get_partition_property_cpuid_results() {
        const CPUID_EXT_HYPERVISOR: UINT32 = 1 << 31;
        let p: Partition = Partition::new().unwrap();
        let mut cpuid_results: Vec<WHV_X64_CPUID_RESULT> = Vec::new();
        let mut cpuid_result: WHV_X64_CPUID_RESULT = Default::default();
        cpuid_result.Function = 1;
        cpuid_result.Ecx = CPUID_EXT_HYPERVISOR;
        cpuid_results.push(cpuid_result);

        // Getting this property is not supported
        assert_eq!(
            p.set_property_cpuid_results(&cpuid_results).ok(),
            Some(()),
            "set_property_cpuid_results failed"
        );
    }

    #[test]
    fn test_setup_partition() {
        let p: Partition = Partition::new().unwrap();
        let mut property: WHV_PARTITION_PROPERTY = Default::default();
        property.ProcessorCount = 1;

        // Setup fails without setting at least the number of vcpus
        p.set_property(
            WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
            &property,
        )
        .unwrap();
    }

    #[test]
    fn test_setup_partition_fail() {
        let p: Partition = Partition::new().unwrap();
        match p.setup() {
            Err(e) => assert_eq!(
                e.result(),
                WHV_E_INVALID_PARTITION_CONFIG,
                "Unexpected error code"
            ),
            Ok(()) => panic!("An error was expected"),
        }
    }

    fn setup_vcpu_test(p: &mut Partition) {
        let mut property: WHV_PARTITION_PROPERTY = Default::default();
        property.ProcessorCount = 1;

        p.set_property(
            WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
            &property,
        )
        .unwrap();
        p.setup().unwrap();
    }

    #[test]
    fn test_create_delete_virtual_processor() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();
        drop(vp)
    }

    #[test]
    fn test_run_virtual_processor() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();
        let exit_context: WHV_RUN_VP_EXIT_CONTEXT = vp.run().unwrap();

        assert_eq!(
            exit_context.ExitReason,
            WHV_RUN_VP_EXIT_REASON::WHvRunVpExitReasonMemoryAccess,
            "Unexpected exit reason"
        )
    }

    #[test]
    fn test_cancel_virtual_processor() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();
        vp.cancel_run().unwrap();
    }

    #[test]
    fn test_set_get_virtual_processor_registers() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();

        const NUM_REGS: UINT32 = 1;
        const REG_VALUE: UINT64 = 11111111;
        let mut reg_names: [WHV_REGISTER_NAME; NUM_REGS as usize] = Default::default();
        let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();
        let mut reg_values_out: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();

        reg_names[0] = WHV_REGISTER_NAME::WHvX64RegisterRax;
        reg_values[0].Reg64 = REG_VALUE;

        vp.set_registers(&reg_names, &reg_values).unwrap();
        vp.get_registers(&reg_names, &mut reg_values_out).unwrap();

        unsafe {
            assert_eq!(
                reg_values_out[0].Reg64, REG_VALUE,
                "Registers values do not match"
            );
        }
    }

    #[test]
    fn test_map_gpa_range() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        const SIZE: UINT64 = 0x100000;
        let guest_address: WHV_GUEST_PHYSICAL_ADDRESS = 0;

        let mem = VirtualMemory::new(SIZE as usize).unwrap();

        let mapping = p
            .map_gpa_range(
                &mem,
                guest_address,
                SIZE,
                WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagRead,
            )
            .unwrap();

        assert_eq!(mapping.get_size(), SIZE);
        assert_eq!(mapping.get_source_address(), mem.as_ptr());
        assert_eq!(mapping.get_guest_address(), guest_address);
        assert_eq!(
            mapping.get_flags(),
            WHV_MAP_GPA_RANGE_FLAGS::WHvMapGpaRangeFlagRead
        );
    }

    #[test]
    fn test_translate_gva() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();

        let gva: WHV_GUEST_PHYSICAL_ADDRESS = 0;
        let (translation_result, gpa) = vp
            .translate_gva(
                gva,
                WHV_TRANSLATE_GVA_FLAGS::WHvTranslateGvaFlagValidateRead,
            )
            .unwrap();

        // This API changed, it used to return GpaUnmapped, now it runs Success.
        // So support both versions for now.
        //
        let result = translation_result.ResultCode;
        assert!(
            result == WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultGpaUnmapped ||
            result == WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultSuccess,
            "Unexpected translation result code {:?}", result);

        assert_eq!(gpa, 0, "Unexpected GPA value");
    }

    #[test]
    fn test_virtual_processor_index() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();

        assert_eq!(vp.index(), vp_index, "Index value not matching");
    }

    #[test]
    #[allow(unused_variables)]
    #[allow(unused_mut)]
    fn test_request_interrupt() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;
        let mut vp = p.create_virtual_processor(vp_index).unwrap();

        let mut interrupt_control: WHV_INTERRUPT_CONTROL = Default::default();
        // TriggerMode = 0 (Edge)
        // DestinationMode = 0 (Logical)
        // InterruptType = 0x0 (Fixed)
        interrupt_control.TypeDestinationModeTriggerModeReserved = 0x000;
        interrupt_control.Destination = 0;
        interrupt_control.Vector = 0x37;
        let interrupt_control_size = std::mem::size_of::<WHV_INTERRUPT_CONTROL>() as UINT32;
        match vp.request_interrupt(&interrupt_control) {
            Err(e) => println!("Error"),
            Ok(()) => println!("Success"),
        }
    }

    #[test]
    fn test_get_set_xsave_state() {
        let mut capability_features: WHV_CAPABILITY_FEATURES = Default::default();
        capability_features.AsUINT64 = 0;

        let capability: WHV_CAPABILITY =
            get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeFeatures).unwrap();
        unsafe {
            capability_features = capability.Features;
        }

        if capability_features.Xsave() != 0 {
            let mut p: Partition = Partition::new().unwrap();
            setup_vcpu_test(&mut p);

            let vp_index: UINT32 = 0;
            let vp = p.create_virtual_processor(vp_index).unwrap();

            let mut xsave_state: XsaveArea = Default::default();
            assert_eq!(xsave_state.region[7], 0);

            xsave_state = vp.get_xsave_state().unwrap();
            assert_eq!(xsave_state.region[7], 0xffff);

            vp.set_xsave_state(xsave_state).unwrap();
        }
    }

    fn initialize_apic(p: &mut Partition) -> bool {
        let capability: WHV_CAPABILITY =
            get_capability(WHV_CAPABILITY_CODE::WHvCapabilityCodeFeatures).unwrap();
        let features: WHV_CAPABILITY_FEATURES = unsafe { capability.Features };
        let mut apic_enabled = false;

        if features.LocalApicEmulation() != 0 {
            let mut property: WHV_PARTITION_PROPERTY = Default::default();

            property.LocalApicEmulationMode =
                WHV_X64_LOCAL_APIC_EMULATION_MODE::WHvX64LocalApicEmulationModeXApic;

            p.set_property(
                WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeLocalApicEmulationMode,
                &property,
            )
            .unwrap();

            apic_enabled = true;
        }

        apic_enabled
    }

    use interrupts::*;
    use x86_64::*;
    #[test]
    fn test_enable_get_set_apic() {
        let mut p: Partition = Partition::new().unwrap();

        let apic_enabled = initialize_apic(&mut p);

        let mut property: WHV_PARTITION_PROPERTY = Default::default();
        property.ProcessorCount = 1;

        p.set_property(
            WHV_PARTITION_PROPERTY_CODE::WHvPartitionPropertyCodeProcessorCount,
            &property,
        )
        .unwrap();

        p.setup().unwrap();

        let vp_index: UINT32 = 0;
        let vp = p.create_virtual_processor(vp_index).unwrap();

        if apic_enabled == true {
            let state: LapicStateRaw = vp.get_lapic().unwrap();
            let icr0 = get_lapic_reg(&state, APIC_REG_OFFSET::InterruptCommand0);
            assert_eq!(icr0, 0);

            // Uses both get_lapic and set_lapic under the hood
            set_reg_in_lapic(&vp, APIC_REG_OFFSET::InterruptCommand0, 0x40);

            let state_out: LapicStateRaw = vp.get_lapic().unwrap();
            let icr0 = get_lapic_reg(&state_out, APIC_REG_OFFSET::InterruptCommand0);
            assert_eq!(icr0, 0x40);
        }
    }

    #[test]
    fn test_get_partition_counters() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;

        let mut _vp = p.create_virtual_processor(vp_index).unwrap();

        let counters: WHV_PARTITION_COUNTERS = _vp
            .get_partition_counters(WHV_PARTITION_COUNTER_SET::WHvPartitionCounterSetMemory)
            .unwrap();
        let mem_counters = unsafe { counters.MemoryCounters };

        assert_eq!(mem_counters.Mapped4KPageCount, 0);
        assert_eq!(mem_counters.Mapped2MPageCount, 0);
        assert_eq!(mem_counters.Mapped1GPageCount, 0);
    }

    #[test]
    fn test_get_processor_counters() {
        let mut p: Partition = Partition::new().unwrap();
        setup_vcpu_test(&mut p);

        let vp_index: UINT32 = 0;

        let vp = p.create_virtual_processor(vp_index).unwrap();
        let counters: WHV_PROCESSOR_COUNTERS = vp
            .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetRuntime)
            .unwrap();
        let runtime_counters = unsafe { counters.RuntimeCounters };
        assert!(runtime_counters.TotalRuntime100ns > 0);

        let counters: WHV_PROCESSOR_COUNTERS = vp
            .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetIntercepts)
            .unwrap();
        let intercept_counters = unsafe { counters.InterceptCounters };
        assert_eq!(intercept_counters.PageInvalidations.Count, 0);

        let counters: WHV_PROCESSOR_COUNTERS = vp
            .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetEvents)
            .unwrap();
        let event_counters = unsafe { counters.EventCounters };
        assert_eq!(event_counters.PageFaultCount, 0);

        let counters: WHV_PROCESSOR_COUNTERS = vp
            .get_processor_counters(WHV_PROCESSOR_COUNTER_SET::WHvProcessorCounterSetApic)
            .unwrap();
        let apic_counters = unsafe { counters.ApicCounters };
        assert_eq!(apic_counters.SentIpiCount, 0);
    }
}
