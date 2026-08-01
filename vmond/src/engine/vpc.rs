use std::{
	collections::BTreeMap,
	fs,
	net::Ipv4Addr,
	path::{Path, PathBuf},
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{EngineError, error::Result};

const DEFAULT_CIDR: &str = "10.77.0.0/16";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Vpc {
	pub id:                     String,
	pub name:                   String,
	pub cidr:                   String,
	pub created_at_unix_millis: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredVpc {
	#[serde(default = "default_tenant")]
	owner_tenant:           String,
	id:                     String,
	name:                   String,
	cidr:                   String,
	#[serde(rename = "created_at")]
	created_at_unix_millis: u64,
	#[serde(default)]
	allocations:            BTreeMap<String, String>,
}

impl From<&StoredVpc> for Vpc {
	fn from(value: &StoredVpc) -> Self {
		Self {
			id:                     value.id.clone(),
			name:                   value.name.clone(),
			cidr:                   value.cidr.clone(),
			created_at_unix_millis: value.created_at_unix_millis,
		}
	}
}

#[derive(Default, Deserialize, Serialize)]
struct RegistryData {
	#[serde(default)]
	vpcs: Vec<StoredVpc>,
}

pub struct VpcRegistry {
	path: PathBuf,
	data: Mutex<RegistryData>,
}

impl VpcRegistry {
	pub fn open(home: &Path) -> Result<Self> {
		let path = home.join("vpcs.json");
		let data = match fs::read(&path) {
			Ok(bytes) => serde_json::from_slice(&bytes)
				.map_err(|error| EngineError::engine(format!("reading VPC registry: {error}")))?,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => RegistryData::default(),
			Err(error) => return Err(error.into()),
		};
		Ok(Self { path, data: Mutex::new(data) })
	}

	pub fn create(&self, owner_tenant: &str, name: Option<&str>, cidr: Option<&str>) -> Result<Vpc> {
		let network = Ipv4Network::parse(
			cidr
				.filter(|value| !value.is_empty())
				.unwrap_or(DEFAULT_CIDR),
		)?;
		let mut data = self.data.lock();
		if data.vpcs.iter().any(|entry| {
			Ipv4Network::parse(&entry.cidr).is_ok_and(|existing| existing.overlaps(network))
		}) {
			return Err(EngineError::invalid("VPC CIDR overlaps an existing VPC"));
		}
		let id = loop {
			let candidate = format!("vpc-{:08x}", rand::random::<u32>());
			if data.vpcs.iter().all(|entry| entry.id != candidate) {
				break candidate;
			}
		};
		let stored = StoredVpc {
			owner_tenant: owner_tenant.to_owned(),
			id,
			name: name.unwrap_or_default().to_owned(),
			cidr: network.to_string(),
			created_at_unix_millis: now_millis(),
			allocations: BTreeMap::new(),
		};
		let result = Vpc::from(&stored);
		data.vpcs.push(stored);
		self.persist(&data)?;
		Ok(result)
	}

	pub fn list(&self, owner_tenant: &str) -> Vec<Vpc> {
		self
			.data
			.lock()
			.vpcs
			.iter()
			.filter(|entry| entry.owner_tenant == owner_tenant)
			.map(Vpc::from)
			.collect()
	}

	pub fn get(&self, owner_tenant: &str, id: &str) -> Result<Vpc> {
		self
			.data
			.lock()
			.vpcs
			.iter()
			.find(|entry| entry.id == id && entry.owner_tenant == owner_tenant)
			.map(Vpc::from)
			.ok_or_else(|| EngineError::not_found(format!("unknown VPC '{id}'")))
	}

	pub fn ensure_deletable(&self, owner_tenant: &str, id: &str) -> Result<Vpc> {
		let data = self.data.lock();
		let entry = data
			.vpcs
			.iter()
			.find(|entry| entry.id == id && entry.owner_tenant == owner_tenant)
			.ok_or_else(|| EngineError::not_found(format!("unknown VPC '{id}'")))?;
		if !entry.allocations.is_empty() {
			return Err(EngineError::busy(format!("VPC '{id}' is attached to a sandbox")));
		}
		Ok(Vpc::from(entry))
	}

	pub fn delete(&self, owner_tenant: &str, id: &str) -> Result<Vpc> {
		let mut data = self.data.lock();
		let index = data
			.vpcs
			.iter()
			.position(|entry| entry.id == id && entry.owner_tenant == owner_tenant)
			.ok_or_else(|| EngineError::not_found(format!("unknown VPC '{id}'")))?;
		if !data.vpcs[index].allocations.is_empty() {
			return Err(EngineError::busy(format!("VPC '{id}' is attached to a sandbox")));
		}
		let removed = data.vpcs.remove(index);
		self.persist(&data)?;
		Ok(Vpc::from(&removed))
	}

	pub fn allocate(
		&self,
		owner_tenant: &str,
		id: &str,
		sandbox: &str,
		requested: Option<&str>,
	) -> Result<String> {
		let mut data = self.data.lock();
		let entry = data
			.vpcs
			.iter_mut()
			.find(|entry| entry.id == id && entry.owner_tenant == owner_tenant)
			.ok_or_else(|| EngineError::not_found(format!("unknown VPC '{id}'")))?;
		if let Some(existing) = entry.allocations.get(sandbox) {
			return Ok(existing.clone());
		}
		let network = Ipv4Network::parse(&entry.cidr)?;
		let address = if let Some(requested) = requested {
			let address = requested.parse::<Ipv4Addr>().map_err(|_| {
				EngineError::invalid("VPC NIC ipv4 must be a valid IPv4 address or true")
			})?;
			network.validate_guest(address)?;
			address
		} else {
			network
				.hosts()
				.find(|candidate| {
					!entry
						.allocations
						.values()
						.any(|value| value == &candidate.to_string())
				})
				.ok_or_else(|| EngineError::busy(format!("VPC '{id}' has no free IPv4 addresses")))?
		};
		if entry
			.allocations
			.values()
			.any(|value| value == &address.to_string())
		{
			return Err(EngineError::invalid(format!("IPv4 address {address} is already allocated")));
		}
		entry
			.allocations
			.insert(sandbox.to_owned(), address.to_string());
		self.persist(&data)?;
		Ok(address.to_string())
	}

	pub fn release_sandbox(&self, sandbox: &str) -> Result<()> {
		let mut data = self.data.lock();
		let mut changed = false;
		for entry in &mut data.vpcs {
			changed |= entry.allocations.remove(sandbox).is_some();
		}
		if changed {
			self.persist(&data)?;
		}
		Ok(())
	}

	pub fn gateway_and_prefix(&self, owner_tenant: &str, id: &str) -> Result<(String, u8)> {
		let vpc = self.get(owner_tenant, id)?;
		let network = Ipv4Network::parse(&vpc.cidr)?;
		Ok((Ipv4Addr::from(network.base.saturating_add(1)).to_string(), network.prefix))
	}

	fn persist(&self, data: &RegistryData) -> Result<()> {
		if let Some(parent) = self.path.parent() {
			fs::create_dir_all(parent)?;
		}
		let temporary = self
			.path
			.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4()));
		fs::write(&temporary, serde_json::to_vec_pretty(data)?)?;
		fs::rename(temporary, &self.path)?;
		Ok(())
	}
}

fn default_tenant() -> String {
	"default".to_owned()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Ipv4Network {
	base:   u32,
	prefix: u8,
}

impl Ipv4Network {
	fn parse(value: &str) -> Result<Self> {
		let (address, prefix) = value.split_once('/').ok_or_else(|| {
			EngineError::invalid("VPC CIDR must be an IPv4 network in a.b.c.d/len form")
		})?;
		let address = address.parse::<Ipv4Addr>().map_err(|_| {
			EngineError::invalid("VPC CIDR must be an IPv4 network in a.b.c.d/len form")
		})?;
		let prefix = prefix
			.parse::<u8>()
			.ok()
			.filter(|prefix| (8..=30).contains(prefix))
			.ok_or_else(|| EngineError::invalid("VPC CIDR prefix must be between 8 and 30"))?;
		let mask = u32::MAX << (32 - prefix);
		let raw = u32::from(address);
		if raw & mask != raw {
			return Err(EngineError::invalid("VPC CIDR address must be the network address"));
		}
		Ok(Self { base: raw, prefix })
	}

	const fn last(self) -> u32 {
		self.base | (u32::MAX >> self.prefix)
	}

	const fn overlaps(self, other: Self) -> bool {
		self.base <= other.last() && other.base <= self.last()
	}

	fn validate_guest(self, address: Ipv4Addr) -> Result<()> {
		let raw = u32::from(address);
		if raw <= self.base.saturating_add(1) || raw >= self.last() {
			return Err(EngineError::invalid(format!(
				"IPv4 address {address} is not a usable host in {self}"
			)));
		}
		Ok(())
	}

	fn hosts(self) -> impl Iterator<Item = Ipv4Addr> {
		(self.base.saturating_add(2)..self.last()).map(Ipv4Addr::from)
	}
}

impl std::fmt::Display for Ipv4Network {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "{}/{}", Ipv4Addr::from(self.base), self.prefix)
	}
}

fn now_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn crud_persists_and_rejects_overlaps() {
		let home = tempfile::tempdir().unwrap();
		let registry = VpcRegistry::open(home.path()).unwrap();
		let first = registry
			.create("tenant-a", Some("primary"), Some("10.80.0.0/24"))
			.unwrap();
		assert_eq!(first.name, "primary");
		assert_eq!(first.cidr, "10.80.0.0/24");
		assert!(
			registry
				.create("tenant-a", None, Some("10.80.0.128/25"))
				.is_err()
		);
		drop(registry);
		let registry = VpcRegistry::open(home.path()).unwrap();
		assert_eq!(registry.list("tenant-a"), vec![first.clone()]);
		assert_eq!(registry.delete("tenant-a", &first.id).unwrap(), first);
		assert!(registry.list("tenant-a").is_empty());
	}

	#[test]
	fn cidr_validation_rejects_invalid_networks() {
		for cidr in ["10.0.0.1/24", "10.0.0.0/7", "10.0.0.0/31", "not-a-cidr", "::1/64"] {
			assert!(Ipv4Network::parse(cidr).is_err(), "accepted {cidr}");
		}
	}

	#[test]
	fn allocations_skip_gateway_reject_duplicates_and_release() {
		let home = tempfile::tempdir().unwrap();
		let registry = VpcRegistry::open(home.path()).unwrap();
		let vpc = registry
			.create("tenant-a", None, Some("10.90.0.0/29"))
			.unwrap();
		assert_eq!(registry.allocate("tenant-a", &vpc.id, "one", None).unwrap(), "10.90.0.2");
		assert_eq!(
			registry
				.allocate("tenant-a", &vpc.id, "two", Some("10.90.0.5"))
				.unwrap(),
			"10.90.0.5"
		);
		assert!(
			registry
				.allocate("tenant-a", &vpc.id, "three", Some("10.90.0.5"))
				.is_err()
		);
		assert!(registry.delete("tenant-a", &vpc.id).is_err());
		drop(registry);
		let registry = VpcRegistry::open(home.path()).unwrap();
		assert_eq!(registry.allocate("tenant-a", &vpc.id, "one", None).unwrap(), "10.90.0.2");
		registry.release_sandbox("one").unwrap();
		registry.release_sandbox("two").unwrap();
		registry.delete("tenant-a", &vpc.id).unwrap();
	}

	#[test]
	fn tenant_scope_hides_foreign_vpcs_and_rejects_cross_tenant_attach() {
		let home = tempfile::tempdir().unwrap();
		let registry = VpcRegistry::open(home.path()).unwrap();
		let vpc = registry
			.create("tenant-a", Some("private"), Some("10.91.0.0/24"))
			.unwrap();
		assert!(registry.list("tenant-b").is_empty());
		let delete = registry.delete("tenant-b", &vpc.id).unwrap_err();
		assert_eq!(delete.code.as_str(), "not_found");
		let attach = registry
			.allocate("tenant-b", &vpc.id, "sandbox-b", None)
			.unwrap_err();
		assert_eq!(attach.code.as_str(), "not_found");
	}
}
