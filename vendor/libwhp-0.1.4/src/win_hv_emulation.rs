// Copyright 2018 Cloudbase Solutions Srl
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
use win_hv_emulation_defs::*;
use win_hv_platform_defs::*;

pub type WHV_EMULATOR_IO_PORT_CALLBACK =
    extern "stdcall" fn(Context: *mut VOID, IoAccess: *mut WHV_EMULATOR_IO_ACCESS_INFO) -> HRESULT;

pub type WHV_EMULATOR_MEMORY_CALLBACK = extern "stdcall" fn(
    Context: *mut VOID,
    MemoryAccess: *mut WHV_EMULATOR_MEMORY_ACCESS_INFO,
) -> HRESULT;

pub type WHV_EMULATOR_GET_VIRTUAL_PROCESSOR_REGISTERS_CALLBACK = extern "stdcall" fn(
    Context: *mut VOID,
    RegisterNames: *const WHV_REGISTER_NAME,
    RegisterCount: UINT32,
    RegisterValues: *mut WHV_REGISTER_VALUE,
) -> HRESULT;

pub type WHV_EMULATOR_SET_VIRTUAL_PROCESSOR_REGISTERS_CALLBACK = extern "stdcall" fn(
    Context: *mut VOID,
    RegisterNames: *const WHV_REGISTER_NAME,
    RegisterCount: UINT32,
    RegisterValues: *const WHV_REGISTER_VALUE,
) -> HRESULT;

pub type WHV_EMULATOR_TRANSLATE_GVA_PAGE_CALLBACK = extern "stdcall" fn(
    Context: *mut VOID,
    Gva: WHV_GUEST_VIRTUAL_ADDRESS,
    TranslateFlags: WHV_TRANSLATE_GVA_FLAGS,
    TranslationResult: *mut WHV_TRANSLATE_GVA_RESULT_CODE,
    Gpa: *mut WHV_GUEST_PHYSICAL_ADDRESS,
) -> HRESULT;

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_EMULATOR_CALLBACKS {
    pub Size: UINT32,
    pub Reserved: UINT32,
    pub WHvEmulatorIoPortCallback: WHV_EMULATOR_IO_PORT_CALLBACK,
    pub WHvEmulatorMemoryCallback: WHV_EMULATOR_MEMORY_CALLBACK,
    pub WHvEmulatorGetVirtualProcessorRegisters:
        WHV_EMULATOR_GET_VIRTUAL_PROCESSOR_REGISTERS_CALLBACK,
    pub WHvEmulatorSetVirtualProcessorRegisters:
        WHV_EMULATOR_SET_VIRTUAL_PROCESSOR_REGISTERS_CALLBACK,
    pub WHvEmulatorTranslateGvaPage: WHV_EMULATOR_TRANSLATE_GVA_PAGE_CALLBACK,
}

#[allow(non_snake_case)]
#[link(name = "WinHvEmulation")]
extern "stdcall" {
    pub fn WHvEmulatorCreateEmulator(
        Callbacks: *const WHV_EMULATOR_CALLBACKS,
        Emulator: *mut WHV_EMULATOR_HANDLE,
    ) -> HRESULT;
    pub fn WHvEmulatorDestroyEmulator(Emulator: WHV_EMULATOR_HANDLE) -> HRESULT;
    pub fn WHvEmulatorTryIoEmulation(
        Emulator: WHV_EMULATOR_HANDLE,
        Context: *mut VOID,
        VpContext: *const WHV_VP_EXIT_CONTEXT,
        IoInstructionContext: *const WHV_X64_IO_PORT_ACCESS_CONTEXT,
        EmulatorReturnStatus: *mut WHV_EMULATOR_STATUS,
    ) -> HRESULT;
    pub fn WHvEmulatorTryMmioEmulation(
        Emulator: WHV_EMULATOR_HANDLE,
        Context: *mut VOID,
        VpContext: *const WHV_VP_EXIT_CONTEXT,
        MmioInstructionContext: *const WHV_MEMORY_ACCESS_CONTEXT,
        EmulatorReturnStatus: *mut WHV_EMULATOR_STATUS,
    ) -> HRESULT;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    extern "stdcall" fn io_port_cb(
        _context: *mut VOID,
        _io_access: *mut WHV_EMULATOR_IO_ACCESS_INFO,
    ) -> HRESULT {
        S_OK
    }

    extern "stdcall" fn memory_cb(
        _context: *mut VOID,
        _memory_access: *mut WHV_EMULATOR_MEMORY_ACCESS_INFO,
    ) -> HRESULT {
        S_OK
    }

    extern "stdcall" fn get_vp_registers_cb(
        _context: *mut VOID,
        _register_names: *const WHV_REGISTER_NAME,
        _register_count: UINT32,
        _register_values: *mut WHV_REGISTER_VALUE,
    ) -> HRESULT {
        S_OK
    }

    extern "stdcall" fn set_vp_registers_cb(
        _context: *mut VOID,
        _register_names: *const WHV_REGISTER_NAME,
        _register_count: UINT32,
        _register_values: *const WHV_REGISTER_VALUE,
    ) -> HRESULT {
        S_OK
    }

    extern "stdcall" fn translate_gva_page_cb(
        _context: *mut VOID,
        _gva: WHV_GUEST_VIRTUAL_ADDRESS,
        _translate_flags: WHV_TRANSLATE_GVA_FLAGS,
        _translation_result: *mut WHV_TRANSLATE_GVA_RESULT_CODE,
        _gpa: *mut WHV_GUEST_PHYSICAL_ADDRESS,
    ) -> HRESULT {
        S_OK
    }

    fn with_emulator<F>(action: F)
    where
        F: Fn(WHV_EMULATOR_HANDLE),
    {
        let mut emulator: WHV_EMULATOR_HANDLE = std::ptr::null_mut();

        let callbacks = WHV_EMULATOR_CALLBACKS {
            Size: std::mem::size_of::<WHV_EMULATOR_CALLBACKS>() as UINT32,
            Reserved: 0,
            WHvEmulatorIoPortCallback: io_port_cb,
            WHvEmulatorMemoryCallback: memory_cb,
            WHvEmulatorGetVirtualProcessorRegisters: get_vp_registers_cb,
            WHvEmulatorSetVirtualProcessorRegisters: set_vp_registers_cb,
            WHvEmulatorTranslateGvaPage: translate_gva_page_cb,
        };

        let result = unsafe { WHvEmulatorCreateEmulator(&callbacks, &mut emulator) };
        assert_eq!(
            result, S_OK,
            "WHvEmulatorCreateEmulator failed with 0x{:X}",
            result
        );

        action(emulator);

        let result = unsafe { WHvEmulatorDestroyEmulator(emulator) };
        assert_eq!(
            result, S_OK,
            "WHvEmulatorDestroyEmulator failed with 0x{:X}",
            result
        );
    }

    #[test]
    fn test_create_destroy_emulator() {
        with_emulator(|_emulator| {});
    }

    #[test]
    fn test_try_io_emulation() {
        with_emulator(|emulator| {
            let context = std::ptr::null_mut();
            let mut vp_context: WHV_VP_EXIT_CONTEXT = Default::default();
            let io_instruction_context: WHV_X64_IO_PORT_ACCESS_CONTEXT = Default::default();
            let mut emulator_status: WHV_EMULATOR_STATUS = Default::default();

            // Without this WHvEmulatorTryIoEmulation returns E_INVALIDARG
            vp_context.InstructionLengthCr8 = 0xF;

            let result = unsafe {
                WHvEmulatorTryIoEmulation(
                    emulator,
                    context,
                    &vp_context,
                    &io_instruction_context,
                    &mut emulator_status,
                )
            };

            assert_eq!(
                result, S_OK,
                "WHvEmulatorTryIoEmulation failed with 0x{:X}",
                result
            );
        });
    }

    #[test]
    fn test_try_mmiio_emulation() {
        with_emulator(|emulator| {
            let context = std::ptr::null_mut();
            let vp_context: WHV_VP_EXIT_CONTEXT = Default::default();
            let mmio_instruction_context: WHV_MEMORY_ACCESS_CONTEXT = Default::default();
            let mut emulator_status: WHV_EMULATOR_STATUS = Default::default();

            let result = unsafe {
                WHvEmulatorTryMmioEmulation(
                    emulator,
                    context,
                    &vp_context,
                    &mmio_instruction_context,
                    &mut emulator_status,
                )
            };

            assert_eq!(
                result, S_OK,
                "WHvEmulatorTryMmioEmulation failed with 0x{:X}",
                result
            );
        });
    }

    #[test]
    fn test_data_type_sizes() {
        // Make sure all unions and structs have a size that matches the value
        // obtained with a sizeof() in C.
        assert_eq!(std::mem::size_of::<WHV_EMULATOR_CALLBACKS>(), 48);
    }
}
