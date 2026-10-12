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
use win_hv_platform_defs::*;

pub type WHV_EMULATOR_HANDLE = *mut VOID;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_EMULATOR_STATUS {
    pub AsUINT32: UINT32,
}

bitfield!(WHV_EMULATOR_STATUS AsUINT32: UINT32[
    EmulationSuccessful set_EmulationSuccessful[0..1],
    InternalEmulationFailure set_InternalEmulationFailure[1..2],
    IoPortCallbackFailed set_IoPortCallbackFailed[2..3],
    MemoryCallbackFailed set_MemoryCallbackFailed[3..4],
    TranslateGvaPageCallbackFailed set_TranslateGvaPageCallbackFailed[4..5],
    TranslateGvaPageCallbackGpaIsNotAligned set_TranslateGvaPageCallbackGpaIsNotAligned[5..6],
    GetVirtualProcessorRegistersCallbackFailed set_GetVirtualProcessorRegistersCallbackFailed[6..7],
    SetVirtualProcessorRegistersCallbackFailed set_SetVirtualProcessorRegistersCallbackFailed[7..8],
    InterruptCausedIntercept set_InterruptCausedIntercept[8..9],
    GuestCannotBeFaulted set_GuestCannotBeFaulted[9..10],
    Reserved set_Reserved[10..32],
]);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_EMULATOR_MEMORY_ACCESS_INFO {
    pub GpaAddress: WHV_GUEST_PHYSICAL_ADDRESS,
    pub Direction: UINT8,
    pub AccessSize: UINT8,
    pub Data: [UINT8; 8],
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_EMULATOR_IO_ACCESS_INFO {
    pub Direction: UINT8,
    pub Port: UINT16,
    pub AccessSize: UINT16,
    pub Data: UINT32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn test_data_type_sizes() {
        // Make sure all unions and structs have a size that matches the value
        // obtained with a sizeof() in C.
        assert_eq!(std::mem::size_of::<WHV_EMULATOR_MEMORY_ACCESS_INFO>(), 24);
        assert_eq!(std::mem::size_of::<WHV_EMULATOR_IO_ACCESS_INFO>(), 12);
    }
}
