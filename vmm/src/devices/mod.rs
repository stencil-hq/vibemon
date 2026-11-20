//! Device-model plumbing: a portable port/MMIO bus.

#[cfg(unix)]
pub mod ovnfs;
#[cfg(target_arch = "x86_64")]
pub mod pci;
pub mod serial;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub mod vfio;

use std::sync::Arc;

use parking_lot::Mutex;

/// A device addressable on a port-IO or MMIO bus.
///
/// `offset` is relative to the device's base address. Accesses are at most 8
/// bytes; devices read/write only the widths they care about.
pub trait BusDevice: Send {
	fn read(&mut self, offset: u64, data: &mut [u8]);
	fn write(&mut self, offset: u64, data: &[u8]);
}

struct Entry {
	base:   u64,
	len:    u64,
	device: Arc<Mutex<dyn BusDevice>>,
}

/// A flat address bus mapping ranges to devices. Immutable after setup, so it
/// is shared (`Arc`) across vCPU threads; each device is individually locked.
#[derive(Default)]
pub struct Bus {
	entries: Vec<Entry>,
}

impl Bus {
	pub fn new() -> Self {
		Self::default()
	}

	/// Map `[base, base+len)` to `device`.
	pub fn register(&mut self, base: u64, len: u64, device: Arc<Mutex<dyn BusDevice>>) {
		assert!(len > 0, "bus device range must not be empty");
		let end = base
			.checked_add(len)
			.expect("bus device range overflows u64");
		for entry in &self.entries {
			let entry_end = entry
				.base
				.checked_add(entry.len)
				.expect("registered bus device range overflows u64");
			assert!(
				end <= entry.base || base >= entry_end,
				"bus device range overlaps an existing mapping"
			);
		}
		self.entries.push(Entry { base, len, device });
	}

	fn find(&self, addr: u64, access_len: usize) -> Option<(u64, &Arc<Mutex<dyn BusDevice>>)> {
		let access_len = u64::try_from(access_len).ok()?;
		if access_len == 0 {
			return None;
		}
		self
			.entries
			.iter()
			.find(|e| {
				let Some(offset) = addr.checked_sub(e.base) else {
					return false;
				};
				offset < e.len && access_len <= e.len - offset
			})
			.map(|e| (e.base, &e.device))
	}

	/// Dispatch a read; returns false if no device owns `addr`.
	pub fn read(&self, addr: u64, data: &mut [u8]) -> bool {
		match self.find(addr, data.len()) {
			Some((base, dev)) => {
				dev.lock().read(addr - base, data);
				true
			},
			None => false,
		}
	}

	/// Dispatch a write; returns false if no device owns `addr`.
	pub fn write(&self, addr: u64, data: &[u8]) -> bool {
		match self.find(addr, data.len()) {
			Some((base, dev)) => {
				dev.lock().write(addr - base, data);
				true
			},
			None => false,
		}
	}
}
