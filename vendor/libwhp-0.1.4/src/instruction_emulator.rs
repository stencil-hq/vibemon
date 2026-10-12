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

use common::*;
use std;
use win_hv_emulation::*;
pub use win_hv_emulation_defs::*;
use win_hv_platform_defs::*;

pub trait EmulatorCallbacks {
    fn io_port(
        &mut self,
        io_access: &mut WHV_EMULATOR_IO_ACCESS_INFO,
    ) -> HRESULT;
    fn memory(
        &mut self,
        memory_access: &mut WHV_EMULATOR_MEMORY_ACCESS_INFO,
    ) -> HRESULT;
    fn get_virtual_processor_registers(
        &mut self,
        register_names: &[WHV_REGISTER_NAME],
        register_values: &mut [WHV_REGISTER_VALUE],
    ) -> HRESULT;
    fn set_virtual_processor_registers(
        &mut self,
        register_names: &[WHV_REGISTER_NAME],
        register_values: &[WHV_REGISTER_VALUE],
    ) -> HRESULT;
    fn translate_gva_page(
        &mut self,
        gva: WHV_GUEST_VIRTUAL_ADDRESS,
        translate_flags: WHV_TRANSLATE_GVA_FLAGS,
        translation_result: &mut WHV_TRANSLATE_GVA_RESULT_CODE,
        gpa: &mut WHV_GUEST_PHYSICAL_ADDRESS,
    ) -> HRESULT;
}

#[repr(C)]
struct CallbacksContext<'a, T: EmulatorCallbacks + 'a> {
    emulator: &'a Emulator<T>,
    context: &'a mut T,
}

pub struct Emulator<T: EmulatorCallbacks> {
    emulator_handle: WHV_EMULATOR_HANDLE,
    dummy: std::marker::PhantomData<T>,
}

unsafe impl<T: EmulatorCallbacks> Send for Emulator<T> {}

impl<T: EmulatorCallbacks> Emulator<T> {
    pub fn new() -> Result<Self, WHPError> {

        let native_callbacks = WHV_EMULATOR_CALLBACKS {
            Size: std::mem::size_of::<WHV_EMULATOR_CALLBACKS>() as UINT32,
            Reserved: 0,
            WHvEmulatorIoPortCallback: Emulator::<T>::io_port_cb,
            WHvEmulatorMemoryCallback: Emulator::<T>::memory_cb,
            WHvEmulatorGetVirtualProcessorRegisters: Emulator::<T>::get_vp_registers_cb,
            WHvEmulatorSetVirtualProcessorRegisters: Emulator::<T>::set_vp_registers_cb,
            WHvEmulatorTranslateGvaPage: Emulator::<T>::translate_gva_page_cb,
        };

        let mut emulator_handle: WHV_EMULATOR_HANDLE = std::ptr::null_mut();
        check_result(unsafe { WHvEmulatorCreateEmulator(&native_callbacks, &mut emulator_handle) })?;
        Ok(Emulator {
            emulator_handle: emulator_handle,
            dummy: Default::default(),
        })
    }

    fn catch_unwind_hres<F: FnOnce() -> HRESULT + std::panic::UnwindSafe>(action: F) -> HRESULT {
        // Panics must not unwind across the callback boundary
        let res = std::panic::catch_unwind(action);
        match res {
            Ok(ret_value) => ret_value,
            _ => E_FAIL,
        }
    }

    extern "stdcall" fn io_port_cb(
        context: *mut VOID,
        io_access: *mut WHV_EMULATOR_IO_ACCESS_INFO,
    ) -> HRESULT {
        Emulator::<T>::catch_unwind_hres(|| {
            let cc = unsafe { &mut *(context as *mut CallbacksContext<T>) };
            T::io_port(cc.context, unsafe { &mut *io_access })
        })
    }

    extern "stdcall" fn memory_cb(
        context: *mut VOID,
        memory_access: *mut WHV_EMULATOR_MEMORY_ACCESS_INFO,
    ) -> HRESULT {
        Emulator::<T>::catch_unwind_hres(|| {
            let cc = unsafe { &mut *(context as *mut CallbacksContext<T>) };
            T::memory(cc.context, unsafe { &mut *memory_access })
        })
    }

    extern "stdcall" fn get_vp_registers_cb(
        context: *mut VOID,
        register_names: *const WHV_REGISTER_NAME,
        register_count: UINT32,
        register_values: *mut WHV_REGISTER_VALUE,
    ) -> HRESULT {
        Emulator::<T>::catch_unwind_hres(|| {
            let cc = unsafe { &mut *(context as *mut CallbacksContext<T>) };
            T::get_virtual_processor_registers(
                cc.context,
                unsafe { std::slice::from_raw_parts(register_names, register_count as usize) },
                unsafe { std::slice::from_raw_parts_mut(register_values, register_count as usize) },
            )
        })
    }

    extern "stdcall" fn set_vp_registers_cb(
        context: *mut VOID,
        register_names: *const WHV_REGISTER_NAME,
        register_count: UINT32,
        register_values: *const WHV_REGISTER_VALUE,
    ) -> HRESULT {
        Emulator::<T>::catch_unwind_hres(|| {
            let cc = unsafe { &mut *(context as *mut CallbacksContext<T>) };
            T::set_virtual_processor_registers(
                cc.context,
                unsafe { std::slice::from_raw_parts(register_names, register_count as usize) },
                unsafe { std::slice::from_raw_parts(register_values, register_count as usize) },
            )
        })
    }

    extern "stdcall" fn translate_gva_page_cb(
        context: *mut VOID,
        gva: WHV_GUEST_VIRTUAL_ADDRESS,
        translate_flags: WHV_TRANSLATE_GVA_FLAGS,
        translation_result: *mut WHV_TRANSLATE_GVA_RESULT_CODE,
        gpa: *mut WHV_GUEST_PHYSICAL_ADDRESS,
    ) -> HRESULT {
        Emulator::<T>::catch_unwind_hres(|| {
            let cc = unsafe { &mut *(context as *mut CallbacksContext<T>) };
            T::translate_gva_page(
                cc.context,
                gva,
                translate_flags,
                unsafe { &mut *translation_result },
                unsafe { &mut *gpa },
            )
        })
    }

    pub fn try_io_emulation(
        &self,
        context: &mut T,
        vp_context: &WHV_VP_EXIT_CONTEXT,
        io_instruction_context: &WHV_X64_IO_PORT_ACCESS_CONTEXT,
    ) -> Result<WHV_EMULATOR_STATUS, WHPError> {
        let mut callbacks_context = CallbacksContext {
            emulator: self,
            context: context,
        };

        let mut return_status: WHV_EMULATOR_STATUS = Default::default();
        check_result(unsafe {
            WHvEmulatorTryIoEmulation(
                self.emulator_handle,
                &mut callbacks_context as *mut _ as *mut VOID,
                vp_context,
                io_instruction_context,
                &mut return_status,
            )
        })?;
        Ok(return_status)
    }

    pub fn try_mmio_emulation<'a>(
        &self,
        context: &'a mut T,
        vp_context: &WHV_VP_EXIT_CONTEXT,
        mmio_instruction_context: &WHV_MEMORY_ACCESS_CONTEXT,
    ) -> Result<WHV_EMULATOR_STATUS, WHPError> {
        let mut callbacks_context = CallbacksContext {
            emulator: self,
            context: context,
        };

        let mut return_status: WHV_EMULATOR_STATUS = Default::default();
        check_result(unsafe {
            WHvEmulatorTryMmioEmulation(
                self.emulator_handle,
                &mut callbacks_context as *mut _ as *mut VOID,
                vp_context,
                mmio_instruction_context,
                &mut return_status,
            )
        })?;
        Ok(return_status)
    }
}

impl<T: EmulatorCallbacks> Drop for Emulator<T>
{
    fn drop(&mut self) {
        check_result(unsafe { WHvEmulatorDestroyEmulator(self.emulator_handle) }).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCallbacks<'a> {
        expected_context: &'a str,
        expected_io_access_size: UINT16,
        expected_memory_access_size: UINT8,
        expected_reg_size: UINT32,
        expected_reg_names: &'a [WHV_REGISTER_NAME],
        expected_reg_values: &'a [WHV_REGISTER_VALUE],
        returned_reg_values: &'a [WHV_REGISTER_VALUE],
        returned_gpa: WHV_GUEST_PHYSICAL_ADDRESS,
        returned_translation_result: WHV_TRANSLATE_GVA_RESULT_CODE,
    }

    impl<'a> TestCallbacks<'a> {
        fn check_context(&self) {
            assert_eq!(
                self.expected_context,
                "context",
                "Unexpected context value"
            );
        }
    }

    impl<'a> Default for TestCallbacks<'a> {
        fn default() -> TestCallbacks<'a> {
            TestCallbacks {
                expected_context: "context",
                expected_io_access_size: 0,
                expected_memory_access_size: 0,
                expected_reg_size: 0,
                expected_reg_names: &[],
                expected_reg_values: &[],
                returned_reg_values: &[],
                returned_gpa: 0,
                returned_translation_result:
                    WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultSuccess,
            }
        }
    }

    impl<'a> EmulatorCallbacks for TestCallbacks<'a> {
        fn io_port(
            &mut self,
            io_access: &mut WHV_EMULATOR_IO_ACCESS_INFO,
        ) -> HRESULT {
            self.check_context();
            assert_eq!(
                io_access.AccessSize, self.expected_io_access_size,
                "Unexpected AccessSize value"
            );
            io_access.AccessSize = !io_access.AccessSize;
            S_OK
        }
        fn memory(
            &mut self,
            memory_access: &mut WHV_EMULATOR_MEMORY_ACCESS_INFO,
        ) -> HRESULT {
            self.check_context();
            assert_eq!(
                memory_access.AccessSize, self.expected_memory_access_size,
                "Unexpected AccessSize value"
            );
            memory_access.AccessSize = !memory_access.AccessSize;
            S_OK
        }
        fn get_virtual_processor_registers(
            &mut self,
            register_names: &[WHV_REGISTER_NAME],
            register_values: &mut [WHV_REGISTER_VALUE],
        ) -> HRESULT {
            self.check_context();
            assert_eq!(
                register_names.len(),
                self.expected_reg_size as usize,
                "Unexpected register_names size"
            );
            assert_eq!(
                register_values.len(),
                self.expected_reg_size as usize,
                "Unexpected register_values size"
            );
            assert_eq!(
                register_names, self.expected_reg_names,
                "Unexpected reg names"
            );
            assert_eq!(
                register_values.len(),
                self.returned_reg_values.len(),
                "{}{}",
                "The length of returned_reg_values does not match with the length ",
                "of register_values"
            );
            register_values[..].copy_from_slice(self.returned_reg_values);
            S_OK
        }
        fn set_virtual_processor_registers(
            &mut self,
            register_names: &[WHV_REGISTER_NAME],
            register_values: &[WHV_REGISTER_VALUE],
        ) -> HRESULT {
            self.check_context();
            assert_eq!(
                register_names.len(),
                self.expected_reg_size as usize,
                "Unexpected register_names size"
            );
            assert_eq!(
                register_values.len(),
                self.expected_reg_size as usize,
                "Unexpected register_values size"
            );
            assert_eq!(
                register_names, self.expected_reg_names,
                "Unexpected reg names"
            );
            assert_eq!(
                register_values.len(),
                self.expected_reg_values.len(),
                "{}{}",
                "The length of expected_reg_values does not match with the length ",
                "of register_values"
            );
            for (ai, bi) in register_values.iter().zip(self.expected_reg_values.iter()) {
                unsafe { assert_eq!(ai.Reg128, bi.Reg128, "Unexpected reg value") };
            }
            S_OK
        }
        fn translate_gva_page(
            &mut self,
            _gva: WHV_GUEST_VIRTUAL_ADDRESS,
            _translate_flags: WHV_TRANSLATE_GVA_FLAGS,
            translation_result: &mut WHV_TRANSLATE_GVA_RESULT_CODE,
            gpa: &mut WHV_GUEST_PHYSICAL_ADDRESS,
        ) -> HRESULT {
            self.check_context();
            *gpa = self.returned_gpa;
            *translation_result = self.returned_translation_result;
            S_OK
        }
    }

    #[test]
    fn test_create_delete_emulator() {
        let e = Emulator::<TestCallbacks>::new().unwrap();
        drop(e);
    }

    #[test]
    fn test_try_io_emulation() {
        let mut vp_context: WHV_VP_EXIT_CONTEXT = Default::default();
        let io_instruction_context: WHV_X64_IO_PORT_ACCESS_CONTEXT = Default::default();

        // Without this WHvEmulatorTryIoEmulation returns E_INVALIDARG
        vp_context.InstructionLengthCr8 = 0xF;

        let mut callbacks = TestCallbacks::default();

        let e = Emulator::<TestCallbacks>::new().unwrap();
        let _return_status = e
            .try_io_emulation(
                &mut callbacks,
                &vp_context,
                &io_instruction_context,
            )
            .unwrap();
    }

    #[test]
    fn test_try_mmio_emulation() {
        let vp_context: WHV_VP_EXIT_CONTEXT = Default::default();
        let mmio_instruction_context: WHV_MEMORY_ACCESS_CONTEXT = Default::default();

        let mut callbacks = TestCallbacks::default();

        let e = Emulator::<TestCallbacks>::new().unwrap();
        let _return_status = e
            .try_mmio_emulation(
                &mut callbacks,
                &vp_context,
                &mmio_instruction_context,
            )
            .unwrap();
    }

    #[test]
    fn test_io_port_callback() {
        const EXPECTED_IO_ACCESS_SIZE: UINT16 = 1111;
        let mut callbacks = TestCallbacks {
            expected_io_access_size: EXPECTED_IO_ACCESS_SIZE,
            ..Default::default()
        };

        let mut e = Emulator::<TestCallbacks>::new().unwrap();

        let mut callbacks_context = CallbacksContext {
            emulator: &mut e,
            context: &mut callbacks,
        };

        let mut io_access: WHV_EMULATOR_IO_ACCESS_INFO = Default::default();
        io_access.AccessSize = EXPECTED_IO_ACCESS_SIZE;

        let ret = Emulator::<TestCallbacks>::io_port_cb(
            (&mut callbacks_context) as *mut _ as *mut VOID,
            &mut io_access,
        );
        assert_eq!(ret, S_OK, "Unexpected io_port_cb return value");
        assert_eq!(io_access.AccessSize, !EXPECTED_IO_ACCESS_SIZE, "Unexpected AccessSizee");
    }

    #[test]
    fn test_memory_callback() {
        const EXPECTED_MEMORY_ACCESS_SIZE: UINT8 = 111;
        let mut callbacks = TestCallbacks {
            expected_memory_access_size: EXPECTED_MEMORY_ACCESS_SIZE,
            ..Default::default()
        };

        let mut e = Emulator::<TestCallbacks>::new().unwrap();

        let mut callbacks_context = CallbacksContext {
            emulator: &mut e,
            context: &mut callbacks,
        };

        let mut mem_access: WHV_EMULATOR_MEMORY_ACCESS_INFO = Default::default();
        mem_access.AccessSize = EXPECTED_MEMORY_ACCESS_SIZE;

        let ret = Emulator::<TestCallbacks>::memory_cb(
            &mut callbacks_context as *mut _ as *mut VOID,
            &mut mem_access,
        );
        assert_eq!(ret, S_OK, "Unexpected memory_cb return value");
        assert_eq!(mem_access.AccessSize, !EXPECTED_MEMORY_ACCESS_SIZE, "Unexpected AccessSizee");
    }

    #[test]
    fn test_get_vp_registers_callback() {
        const NUM_REGS: UINT32 = 1;
        const REG_VALUE: UINT64 = 11111111;
        let mut reg_names: [WHV_REGISTER_NAME; NUM_REGS as usize] = Default::default();
        let mut returned_reg_values: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();

        reg_names[0] = WHV_REGISTER_NAME::WHvX64RegisterRax;
        returned_reg_values[0].Reg64 = REG_VALUE;

        let mut callbacks = TestCallbacks {
            expected_reg_names: &reg_names,
            expected_reg_size: NUM_REGS,
            returned_reg_values: &returned_reg_values,
            ..Default::default()
        };

        let mut e = Emulator::<TestCallbacks>::new().unwrap();

        let mut callbacks_context = CallbacksContext {
            emulator: &mut e,
            context: &mut callbacks,
        };

        let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();

        let ret = Emulator::<TestCallbacks>::get_vp_registers_cb(
            &mut callbacks_context as *mut _ as *mut VOID,
            reg_names.as_ptr(),
            NUM_REGS,
            reg_values.as_mut_ptr(),
        );
        assert_eq!(ret, S_OK, "Unexpected get_vp_registers_cb return value");
        for (ai, bi) in reg_values.iter().zip(returned_reg_values.iter()) {
            unsafe { assert_eq!(ai.Reg128, bi.Reg128, "Unexpected reg value") };
        }
    }

    #[test]
    fn test_set_vp_registers_callback() {
        const NUM_REGS: UINT32 = 1;
        const REG_VALUE: UINT64 = 11111111;
        let mut reg_names: [WHV_REGISTER_NAME; NUM_REGS as usize] = Default::default();
        let mut reg_values: [WHV_REGISTER_VALUE; NUM_REGS as usize] = Default::default();

        reg_names[0] = WHV_REGISTER_NAME::WHvX64RegisterRax;
        reg_values[0].Reg64 = REG_VALUE;

        let mut callbacks = TestCallbacks {
            expected_reg_names: &reg_names,
            expected_reg_size: NUM_REGS,
            expected_reg_values: &reg_values,
            ..Default::default()
        };

        let mut e = Emulator::<TestCallbacks>::new().unwrap();

        let mut callbacks_context = CallbacksContext {
            emulator: &mut e,
            context: &mut callbacks,
        };

        let ret = Emulator::<TestCallbacks>::set_vp_registers_cb(
            &mut callbacks_context as *mut _ as *mut VOID,
            reg_names.as_ptr(),
            NUM_REGS,
            reg_values.as_ptr(),
        );
        assert_eq!(ret, S_OK, "Unexpected set_vp_registers_cb return value");
    }

    #[test]
    fn translate_gva_page() {
        const RETURNED_GPA: WHV_GUEST_VIRTUAL_ADDRESS = 11111;
        const RETURNED_TRANSLATION_RESULT: WHV_TRANSLATE_GVA_RESULT_CODE =
            WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultGpaUnmapped;
        let mut callbacks = TestCallbacks {
            returned_gpa: RETURNED_GPA,
            returned_translation_result: RETURNED_TRANSLATION_RESULT,
            ..Default::default()
        };

        let mut e = Emulator::<TestCallbacks>::new().unwrap();

        let mut callbacks_context = CallbacksContext {
            emulator: &mut e,
            context: &mut callbacks,
        };

        let gva: WHV_GUEST_VIRTUAL_ADDRESS = 0;
        let translate_flags: WHV_TRANSLATE_GVA_FLAGS =
            WHV_TRANSLATE_GVA_FLAGS::WHvTranslateGvaFlagNone;
        let mut translation_result: WHV_TRANSLATE_GVA_RESULT_CODE =
            WHV_TRANSLATE_GVA_RESULT_CODE::WHvTranslateGvaResultSuccess;
        let mut gpa: WHV_GUEST_PHYSICAL_ADDRESS = 0;

        let ret = Emulator::<TestCallbacks>::translate_gva_page_cb(
            &mut callbacks_context as *mut _ as *mut VOID,
            gva,
            translate_flags,
            &mut translation_result,
            &mut gpa,
        );
        assert_eq!(ret, S_OK, "Unexpected translate_gva_page return value");
        assert_eq!(gpa, RETURNED_GPA, "Returned GPA does not match");
        assert_eq!(
            translation_result, RETURNED_TRANSLATION_RESULT,
            "Returned translation result does not match"
        );
    }
}
