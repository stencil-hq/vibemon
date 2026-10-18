//! Minimal SMBIOS identity table carrying the stable VM UUID for assigned
//! NVIDIA GPUs.

use vm_memory::{Bytes, GuestAddress};

use crate::{memory::GuestMemoryMmap, result::Result};

const ENTRY_POINT_ADDRESS: u64 = 0x000f_0000;
const TABLE_ADDRESS: u64 = 0x000f_0100;
const ENTRY_POINT_LEN: usize = 0x1f;

/// Publish a stable type-1 system UUID for NVIDIA's guest driver and licensing.
pub fn setup_system_uuid(memory: &GuestMemoryMmap, uuid: [u8; 16]) -> Result<()> {
	let table = build_table(uuid);
	memory.write_slice(&table, GuestAddress(TABLE_ADDRESS))?;

	let mut entry = [0u8; ENTRY_POINT_LEN];
	entry[..4].copy_from_slice(b"_SM_");
	entry[5] = ENTRY_POINT_LEN as u8;
	entry[6] = 2;
	entry[7] = 8;
	let max_structure = u16::try_from(table.len())?;
	entry[8..10].copy_from_slice(&max_structure.to_le_bytes());
	entry[16..21].copy_from_slice(b"_DMI_");
	entry[22..24].copy_from_slice(&u16::try_from(table.len())?.to_le_bytes());
	entry[24..28].copy_from_slice(&u32::try_from(TABLE_ADDRESS)?.to_le_bytes());
	entry[28..30].copy_from_slice(&2u16.to_le_bytes());
	entry[30] = 0x28;
	entry[21] = checksum(&entry[16..]);
	entry[4] = checksum(&entry);
	memory.write_slice(&entry, GuestAddress(ENTRY_POINT_ADDRESS))?;
	Ok(())
}

fn build_table(uuid: [u8; 16]) -> Vec<u8> {
	let mut table = vec![0u8; 0x1b];
	table[0] = 1;
	table[1] = 0x1b;
	table[2..4].copy_from_slice(&0x0100u16.to_le_bytes());
	table[4] = 1;
	table[5] = 2;
	table[8..24].copy_from_slice(&smbios_uuid(uuid));
	table[24] = 0x06;
	table.extend_from_slice(b"Vibemon\0vmon\0\0");

	table.extend_from_slice(&[127, 4, 0xff, 0x7f, 0, 0]);
	table
}

fn smbios_uuid(mut uuid: [u8; 16]) -> [u8; 16] {
	uuid[..4].reverse();
	uuid[4..6].reverse();
	uuid[6..8].reverse();
	uuid
}

fn checksum(bytes: &[u8]) -> u8 {
	0u8.wrapping_sub(bytes.iter().copied().fold(0u8, u8::wrapping_add))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn publishes_valid_smbios_uuid_identity() {
		let memory = crate::memory::create_guest_memory(2 << 20).expect("guest memory");
		let uuid = [
			0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
			0x88,
		];
		setup_system_uuid(&memory, uuid).expect("publish SMBIOS UUID");

		let mut entry = [0; ENTRY_POINT_LEN];
		memory
			.read_slice(&mut entry, GuestAddress(ENTRY_POINT_ADDRESS))
			.expect("read SMBIOS entry point");
		assert_eq!(&entry[..4], b"_SM_");
		assert_eq!(&entry[16..21], b"_DMI_");
		assert_eq!(entry.iter().copied().fold(0u8, u8::wrapping_add), 0);
		assert_eq!(entry[16..].iter().copied().fold(0u8, u8::wrapping_add), 0);

		let table_len = usize::from(u16::from_le_bytes([entry[22], entry[23]]));
		let mut table = vec![0; table_len];
		memory
			.read_slice(&mut table, GuestAddress(TABLE_ADDRESS))
			.expect("read SMBIOS table");
		assert_eq!(&table[..4], &[1, 0x1b, 0, 1]);
		assert_eq!(&table[8..24], &[
			0x78, 0x56, 0x34, 0x12, 0xbc, 0x9a, 0xf0, 0xde, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
			0x88,
		]);
		assert_eq!(&table[table.len() - 6..], &[127, 4, 0xff, 0x7f, 0, 0]);
	}
}
