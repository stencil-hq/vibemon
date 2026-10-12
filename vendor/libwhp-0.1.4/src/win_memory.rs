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

pub const MEM_COMMIT: DWORD = 0x00001000;
pub const MEM_RESERVE: DWORD = 0x00002000;
pub const MEM_RELEASE: DWORD = 0x00008000;
pub const PAGE_READWRITE: DWORD = 0x04;

#[allow(non_snake_case)]
#[link(name = "Kernel32")]
extern "stdcall" {
    pub fn VirtualAlloc(
        lpAddress: *mut VOID,
        dwSize: SIZE_T,
        flAllocationType: DWORD,
        flProtect: DWORD,
    ) -> *mut VOID;
    pub fn VirtualFree(lpAddress: *mut VOID, dwSize: SIZE_T, dwFreeType: DWORD) -> BOOL;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std;

    #[test]
    fn test_virtual_alloc_free() {
        const SIZE: SIZE_T = 0x1000;
        let addr = unsafe {
            VirtualAlloc(
                std::ptr::null_mut(),
                SIZE,
                MEM_COMMIT | MEM_RESERVE,
                PAGE_READWRITE,
            )
        };
        assert!(addr != std::ptr::null_mut(), "VirtualAlloc failed");

        let result = unsafe { VirtualFree(addr, 0, MEM_RELEASE) };
        assert!(result != 0, "VirtualFree failed");
    }
}
