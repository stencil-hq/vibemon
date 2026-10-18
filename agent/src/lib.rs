//! Shared guest-agent pieces: the GC4 frame protocol (also used by the host
//! node), the tunnel runtime, structured console logging, and the guest-only
//! envd server, reaper, OCI unpacker, rootfs provisioner, and GPU driver
//! bring-up.

#[cfg(target_os = "linux")]
pub mod entropy;
pub mod envd;
#[cfg(target_os = "linux")]
pub mod gpu;
pub mod logging;
pub mod proto;
#[cfg(target_os = "linux")]
pub mod provision;
#[cfg(target_os = "linux")]
pub mod reaper;
pub mod tunnel;
#[cfg(target_os = "linux")]
pub mod unpack;
