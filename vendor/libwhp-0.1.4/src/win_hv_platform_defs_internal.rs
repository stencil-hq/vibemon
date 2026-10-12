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

use common::*;
use win_hv_platform_defs::*;

/*
 * This is our own union structure to provide a
 * generic interface to WHvGetProcessorCounters. If new set types
 * are added to that WHV_PROCESSOR_COUNTER_SET enum (and new corresponding
 * structs created), this union should also be updated to reflect the
 * new WHV_PROCESSOR_COUNTER_SET types.
 */
#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_PROCESSOR_COUNTERS {
    pub RuntimeCounters: WHV_PROCESSOR_RUNTIME_COUNTERS,
    pub InterceptCounters: WHV_PROCESSOR_INTERCEPT_COUNTERS,
    pub EventCounters: WHV_PROCESSOR_EVENT_COUNTERS,
    pub ApicCounters: WHV_PROCESSOR_APIC_COUNTERS,
}

impl Default for WHV_PROCESSOR_COUNTERS {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

/*
 * This is our own union structure to provide a
 * generic interface to WHvGetPartitionCounters. If new set types
 * are added to that WHV_PARTITION_COUNTER_SET enum (and new corresponding
 * structs created), this union should also be updated to reflect the
 * new WHV_PARTITION_COUNTER_SET types.
 */
#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_PARTITION_COUNTERS {
    pub MemoryCounters: WHV_PARTITION_MEMORY_COUNTERS,
}

impl Default for WHV_PARTITION_COUNTERS {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}

/*
 * MSI-related structures and definitions
 */
pub const MSI_DATA_VECTOR_SHIFT: UINT32 = 0;
pub const MSI_DATA_VECTOR_MASK: UINT32 = 0x000000ff;

pub const MSI_DATA_DELIVERY_MODE_SHIFT: UINT32 = 8;
pub const MSI_DATA_LEVEL_SHIFT: UINT32 = 14;
pub const MSI_DATA_TRIGGER_SHIFT: UINT32 = 15;

/*
 * Shift/mask fields for msi address
 */

pub const MSI_ADDR_DEST_MODE_SHIFT: UINT32 = 2;
pub const MSI_ADDR_REDIRECTION_SHIFT: UINT32 = 3;
pub const MSI_ADDR_DEST_ID_SHIFT: UINT32 = 12;
pub const MSI_ADDR_DEST_IDX_SHIFT: UINT32 = 4;
pub const MSI_ADDR_DEST_ID_MASK: UINT32 = 0x000ff000;

#[derive(Copy, Clone, Default)]
#[allow(non_snake_case)]
#[repr(C)]
pub struct WHV_MSI_ENTRY_anon_struct {
    pub Address: UINT32,
    pub Data: UINT32,
}

#[derive(Copy, Clone)]
#[allow(non_snake_case)]
#[repr(C)]
pub union WHV_MSI_ENTRY {
    pub AsUINT64: UINT64,
    pub anon_struct: WHV_MSI_ENTRY_anon_struct,
}

impl Default for WHV_MSI_ENTRY {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}
