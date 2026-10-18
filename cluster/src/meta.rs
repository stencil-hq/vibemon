//! Typed member metadata carried inside gossip as JSON bytes.
//!
//! Gossip treats metadata as opaque bytes; every vibemon process encodes a
//! [`MemberMeta`] so nodes and drivers can discover each other's roles,
//! internal URLs, capacity, and raft state (see ARCHITECTURE.md §5.1).

use std::{fmt, net::SocketAddr, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::gossip::Member;

/// Upper bound for encoded metadata so it always fits a gossip packet.
pub const MAX_META_BYTES: usize = 4096;

/// Stable process identity; also the openraft node id.
///
/// Rendered as 16 lowercase hex digits everywhere (logs, API, KV keys).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

impl NodeId {
	/// Generates a random non-zero id for a fresh data directory.
	pub fn random() -> Self {
		loop {
			let id = rand::random::<u64>();
			if id != 0 {
				return Self(id);
			}
		}
	}
}

impl fmt::Display for NodeId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:016x}", self.0)
	}
}

impl FromStr for NodeId {
	type Err = std::num::ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		u64::from_str_radix(s, 16).map(Self)
	}
}

impl Serialize for NodeId {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.collect_str(self)
	}
}

impl<'de> Deserialize<'de> for NodeId {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let raw = String::deserialize(deserializer)?;
		raw.parse().map_err(serde::de::Error::custom)
	}
}

/// Roles a process serves. A process may serve both.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Roles {
	pub node:   bool,
	pub driver: bool,
}

/// Metadata every member publishes through gossip.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberMeta {
	pub roles:         Roles,
	/// Host name of the machine, shown instead of the id.
	pub hostname:      String,
	/// Port of the internal HTTP listener (`/cluster/v1`, `/node/v1`) on every
	/// gossip address; see [`MemberMeta::cluster_url`].
	pub cluster_port:  u16,
	/// Public base URL of a driver's API/ingress listener.
	pub public_url:    Option<String>,
	/// vmon version string.
	pub version:       String,
	pub started_at_ms: u64,
	/// Present iff `roles.node`.
	pub node:          Option<NodeCapacity>,
	/// Present iff `roles.driver`.
	pub raft:          Option<RaftStatus>,
}

/// Node capacity and placement attributes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
	/// Guest architecture served: `x86_64` or `aarch64`.
	pub arch:           String,
	/// Hypervisor backend: `kvm`, `hvf`, or `whp`.
	pub backend:        String,
	/// Host OS: `linux`, `macos`, or `windows`.
	pub os:             String,
	/// Host CPU model name (e.g. `Intel(R) Xeon(R) Gold 6438N`); display only.
	pub cpu_model:      String,
	/// Snapshot compatibility key (`vmon vmm --print-cpu-baseline`).
	pub cpu_baseline:   String,
	pub cpus:           u32,
	pub memory_mb:      u64,
	pub disk_free_mb:   u64,
	pub vcpus_used:     u32,
	pub memory_used_mb: u64,
	pub sandboxes:      u32,
	pub builds:         u32,
	/// Set by the node when told to drain; placement skips draining nodes.
	pub draining:       bool,
	/// Optional capabilities, e.g. `egress-domains`, `egress-proxy`, `net-tap`,
	/// `net-user`.
	pub features:       Vec<String>,
	/// Build IDs with local artifacts, most recent first (truncated to fit).
	pub cached_builds:  Vec<String>,
	/// GPUs found on the host, assignable or not.
	pub gpus:           Vec<GpuInfo>,
}

/// One host GPU as published in [`NodeCapacity::gpus`]; the scheduler counts
/// the free, assignable ones.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GpuInfo {
	/// PCI address (`0000:52:00.0`).
	pub address: String,
	/// Model name (`GB205 [GeForce RTX 5070]`).
	pub model:   String,
	/// Sandbox holding it; `None` when free.
	pub sandbox: Option<String>,
	/// Why sandboxes cannot use it (e.g. `bound to nvidia, not vfio-pci`);
	/// empty when assignable.
	pub problem: String,
}

impl GpuInfo {
	/// Free and assignable.
	pub const fn available(&self) -> bool {
		self.sandbox.is_none() && self.problem.is_empty()
	}
}

/// Raft view published by drivers; used for bootstrap and leader discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftStatus {
	/// UUID chosen at bootstrap; `None` until this driver joins a raft cluster.
	pub cluster_id: Option<String>,
	pub leader:     Option<NodeId>,
	pub voter:      bool,
	pub term:       u64,
}

impl MemberMeta {
	/// Internal HTTP base URL of `member` through the address this process
	/// currently reaches it at ([`Member::addr`]).
	pub fn cluster_url(&self, member: &Member) -> String {
		format!("http://{}", SocketAddr::new(member.addr.ip(), self.cluster_port))
	}

	/// Encodes to JSON, dropping `cached_builds` entries until it fits
	/// [`MAX_META_BYTES`].
	pub fn encode(&self) -> Vec<u8> {
		let mut meta = self.clone();
		loop {
			let bytes = serde_json::to_vec(&meta).expect("member metadata serializes");
			let trimmable = meta
				.node
				.as_ref()
				.is_some_and(|node| !node.cached_builds.is_empty());
			if bytes.len() <= MAX_META_BYTES || !trimmable {
				return bytes;
			}
			if let Some(node) = meta.node.as_mut() {
				let keep = node.cached_builds.len() / 2;
				node.cached_builds.truncate(keep);
			}
		}
	}

	/// Decodes metadata published by a peer; `None` for foreign or corrupt
	/// payloads.
	pub fn decode(bytes: &[u8]) -> Option<Self> {
		serde_json::from_slice(bytes).ok()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn node_id_round_trips_as_hex_string() {
		let id = NodeId(0x00ab_cdef_0123_4567);
		let json = serde_json::to_string(&id).unwrap();
		assert_eq!(json, "\"00abcdef01234567\"");
		assert_eq!(serde_json::from_str::<NodeId>(&json).unwrap(), id);
	}

	#[test]
	fn encode_trims_cached_builds_to_fit() {
		let meta = MemberMeta {
			node: Some(NodeCapacity {
				cached_builds: (0..1000).map(|i| format!("{i:036}")).collect(),
				..NodeCapacity::default()
			}),
			..MemberMeta::default()
		};
		let bytes = meta.encode();
		assert!(bytes.len() <= MAX_META_BYTES);
		let decoded = MemberMeta::decode(&bytes).unwrap();
		let kept = decoded.node.unwrap().cached_builds;
		assert!(!kept.is_empty(), "compaction kept no entries");
		assert_eq!(kept[0], format!("{:036}", 0));
	}
}
