//! Apple Hypervisor.framework backend for aarch64 macOS hosts.

pub mod clock;
pub mod gic;
pub mod memory;
pub mod psci;
pub mod state;
pub mod vcpu;
pub mod vm;

pub use gic::{Gic, IrqLine};
pub use vcpu::Vcpu;
pub use vm::Vm;
