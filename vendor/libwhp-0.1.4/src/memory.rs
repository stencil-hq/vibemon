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
use std;
use std::error::Error;
use std::fmt;
use win_memory::*;

pub trait Memory {
    fn as_slice_mut(&mut self) -> &mut [u8];
    fn as_slice(&self) -> &[u8];
    fn as_ptr(&self) -> *const VOID;
    fn get_size(&self) -> usize;
}

#[derive(Debug)]
pub struct MemoryError {}

impl MemoryError {
    pub fn new() -> MemoryError {
        MemoryError {}
    }
}

impl Error for MemoryError {
    fn description(&self) -> &str {
        "Memory error"
    }
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to allocate memory")
    }
}

pub struct VirtualMemory {
    address: *mut VOID,
    size: usize,
}

impl VirtualMemory {
    pub fn new(size: usize) -> Result<VirtualMemory, MemoryError> {
        let address = unsafe {
            VirtualAlloc(
                std::ptr::null_mut(),
                size as SIZE_T,
                MEM_COMMIT | MEM_RESERVE,
                PAGE_READWRITE,
            )
        };

        match address as u64 {
            0 => Err(MemoryError::new()),
            _ => Ok(VirtualMemory {
                address: address,
                size: size,
            }),
        }
    }
}

impl Memory for VirtualMemory {
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.address as *mut u8, self.size) }
    }
    
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.address as *mut u8, self.size) }
    }

    fn as_ptr(&self) -> *const VOID {
        self.address
    }

    fn get_size(&self) -> usize {
        self.size
    }
}

impl Drop for VirtualMemory {
    fn drop(&mut self) {
        unsafe { VirtualFree(self.address, 0, MEM_RELEASE) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_virtual_alloc_free() {
        const SIZE: usize = 0x1000;
        let mut mem = VirtualMemory::new(SIZE).unwrap();
        let addr = mem.address;

        assert_eq!(mem.get_size(), SIZE);
        assert_eq!(mem.as_ptr(), addr);

        let slice = mem.as_slice_mut();
        assert_eq!(slice.as_ptr() as *const VOID, addr);
        assert_eq!(slice.len(), SIZE);
    }
}
