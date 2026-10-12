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

extern crate libc;

#[macro_use]
mod macros;

pub mod common;
pub mod debug;
pub mod instruction_emulator;
pub mod interrupts;
pub mod platform;
mod win_hv_emulation;
mod win_hv_emulation_defs;
mod win_hv_platform;
mod win_hv_platform_defs;
mod win_hv_platform_defs_internal;
mod win_memory;
pub mod x86_64;

pub use common::*;
pub use platform::*;
pub mod memory;

#[macro_use]
extern crate bitflags;
extern crate byteorder;
