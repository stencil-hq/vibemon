//! /30 lease allocator over the TAP address pool (`--net-pool`, default
//! `172.20.0.0/16`). Block `i` covers `base + 4i … base + 4i + 3`: host (TAP
//! side, guest gateway) = `+1`, guest = `+2`.

use std::{collections::BTreeSet, net::Ipv4Addr};

use anyhow::{bail, ensure};

/// Addresses of one /30 block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Block {
	pub index:   u32,
	pub network: Ipv4Addr,
	pub host:    Ipv4Addr,
	pub guest:   Ipv4Addr,
}

/// Prefix length of every lease.
pub const LEASE_PREFIX: u8 = 30;

/// Hands out /30 blocks round-robin so a just-released block (whose TAP
/// teardown may still be in flight) is reused as late as possible.
#[derive(Debug)]
pub struct LeaseAllocator {
	base:   u32,
	blocks: u32,
	used:   BTreeSet<u32>,
	cursor: u32,
}

impl LeaseAllocator {
	/// Parses `a.b.c.d/len` (`len` ≤ 30, no host bits set).
	pub fn new(cidr: &str) -> anyhow::Result<Self> {
		let Some((addr, len)) = cidr.split_once('/') else {
			bail!("net pool {cidr:?} must be a CIDR like 172.20.0.0/16");
		};
		let addr: Ipv4Addr = addr
			.parse()
			.map_err(|_| anyhow::anyhow!("net pool {cidr:?}: bad address"))?;
		let len: u8 = len
			.parse()
			.map_err(|_| anyhow::anyhow!("net pool {cidr:?}: bad prefix length"))?;
		ensure!(
			(8..=LEASE_PREFIX).contains(&len),
			"net pool {cidr:?}: prefix must be between /8 and /30"
		);
		let base = u32::from(addr);
		let host_bits = 32 - u32::from(len);
		ensure!(base & ((1u32 << host_bits) - 1) == 0, "net pool {cidr:?} has host bits set");
		Ok(Self { base, blocks: 1u32 << (host_bits - 2), used: BTreeSet::new(), cursor: 0 })
	}

	/// Number of /30 blocks in the pool.
	pub const fn capacity(&self) -> u32 {
		self.blocks
	}

	/// Leases currently held.
	pub fn in_use(&self) -> usize {
		self.used.len()
	}

	/// Addresses of block `index`.
	pub fn block(&self, index: u32) -> Option<Block> {
		(index < self.blocks).then(|| {
			let network = self.base + index * 4;
			Block {
				index,
				network: Ipv4Addr::from(network),
				host: Ipv4Addr::from(network + 1),
				guest: Ipv4Addr::from(network + 2),
			}
		})
	}

	/// Takes the next free block after the cursor; `None` when exhausted.
	pub fn allocate(&mut self) -> Option<Block> {
		for step in 0..self.blocks {
			let index = (self.cursor + step) % self.blocks;
			if self.used.insert(index) {
				self.cursor = (index + 1) % self.blocks;
				return self.block(index);
			}
		}
		None
	}

	/// Marks a block held by a recovered sandbox; `false` if out of range or
	/// taken.
	pub fn reserve(&mut self, index: u32) -> bool {
		index < self.blocks && self.used.insert(index)
	}

	/// Returns a block to the pool.
	pub fn release(&mut self, index: u32) {
		self.used.remove(&index);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rejects_bad_pools() {
		assert!(LeaseAllocator::new("172.20.0.0").is_err());
		assert!(LeaseAllocator::new("172.20.0.0/31").is_err(), "a /31 cannot hold a /30");
		assert!(LeaseAllocator::new("172.20.0.1/16").is_err(), "host bits set");
		assert!(LeaseAllocator::new("300.0.0.0/16").is_err());
	}

	#[test]
	fn block_addresses_follow_the_30_layout() {
		let mut pool = LeaseAllocator::new("172.20.0.0/16").unwrap();
		assert_eq!(pool.capacity(), 16_384);
		let first = pool.allocate().unwrap();
		assert_eq!(
			(first.network, first.host, first.guest),
			(
				"172.20.0.0".parse().unwrap(),
				"172.20.0.1".parse().unwrap(),
				"172.20.0.2".parse().unwrap()
			)
		);
		let last = pool.block(16_383).unwrap();
		assert_eq!(last.guest, "172.20.255.254".parse::<Ipv4Addr>().unwrap());
		assert_eq!(pool.block(16_384), None);
	}

	#[test]
	fn exhausts_then_reuses_released_blocks_round_robin() {
		let mut pool = LeaseAllocator::new("10.9.0.0/28").unwrap();
		let got: Vec<u32> = (0..4).map(|_| pool.allocate().unwrap().index).collect();
		assert_eq!(got, [0, 1, 2, 3]);
		assert_eq!(pool.allocate(), None, "pool of 4 blocks is exhausted");
		pool.release(1);
		pool.release(3);
		// Cursor sits after block 3, so block 1 comes back before 3.
		assert_eq!(pool.allocate().unwrap().index, 1);
		assert_eq!(pool.allocate().unwrap().index, 3);
		assert_eq!(pool.in_use(), 4);
	}

	#[test]
	fn reserve_blocks_allocation_and_rejects_duplicates() {
		let mut pool = LeaseAllocator::new("10.9.0.0/29").unwrap();
		assert!(pool.reserve(0));
		assert!(!pool.reserve(0), "double reservation of a recovered lease");
		assert!(!pool.reserve(2), "out of range");
		assert_eq!(pool.allocate().unwrap().index, 1);
		assert_eq!(pool.allocate(), None);
	}
}
