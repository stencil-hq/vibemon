//! Host networking: preallocated slot pool, in-memory /30 lease allocator,
//! per-create TAP setup, egress policy, and loopback tunnels.
//!
//! Port of python/vmon/net.py, redesigned so per-create networking cost does
//! not scale with create rate.
//!
//! # Fast paths
//! - `block_network=true` sandboxes without credentials or a VPC NIC never
//!   reach the default network setup path: the engine gates calls on
//!   `network_required()` (`!block_network || credentials_requested ||
//!   vpc_nic_requested`), and teardown consults `lease_for`, which returns
//!   `None` without touching any allocator or broker state.
//! - IP leases live in an in-memory allocator (`LeaseStore`) loaded once per
//!   journal path. Allocate/release mutate memory and hand a snapshot to a
//!   background writer thread; the on-disk JSON map keeps its historical format
//!   and remains the crash-recovery journal. No flock/parse happens on the
//!   create hot path.
//! - With a warm slot pool (`--net-slots N` / `VMON_NET_SLOTS`, default off),
//!   claiming networking for a sandbox with the DEFAULT policy runs zero
//!   external commands and zero broker round trips: the TAP, addresses, and
//!   base rules were installed at pool fill, and teardown recycles the slot
//!   instead of deleting it. See [`slots`].
//!
//! # When external commands run
//! - Pool fill (serve start): one broker batch per slot chunk (TAP creation +
//!   iptables base rules) plus one `nft -f -` installing the slot skeleton.
//! - Custom egress/inbound policy on a pooled slot: ONE broker round trip
//!   executing one batched `nft -f -` that fills the slot's named sets.
//!   Recycling a custom-policy slot is likewise one batched invocation.
//! - Non-pooled (pool cold or exhausted) sandboxes use the original per-create
//!   broker path: iptables rule-per-sandbox setup and exact-delete teardown,
//!   exactly as before. The sweep to nftables-everything is out of scope;
//!   iptables remains the mechanism for non-pooled TAPs.

pub mod broker;
pub mod policy;
pub mod slots;

#[cfg(target_os = "linux")]
use std::collections::btree_map::Entry;
#[cfg(target_os = "linux")]
use std::process::Command;
#[cfg(any(test, target_os = "linux"))]
use std::time::Duration;
use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt,
	fs::{self, File, OpenOptions},
	io::{Seek, SeekFrom, Write},
	net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
	path::PathBuf,
	sync::{Arc, LazyLock, mpsc},
	thread,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::{
	io,
	net::{TcpListener, TcpStream},
	task::JoinHandle as TokioJoinHandle,
};

use crate::error::{EngineError, Result};
#[cfg(target_os = "linux")]
use crate::security::CREDENTIAL_GATEWAY_PORT;

#[cfg(target_os = "linux")]
const CAP_NET_ADMIN: u32 = 12;
#[cfg(target_os = "linux")]
const IP_FORWARD: &str = "/proc/sys/net/ipv4/ip_forward";
#[cfg(target_os = "linux")]
const PROC_NET_DEV: &str = "/proc/net/dev";
#[cfg(any(test, target_os = "linux"))]
const RULE_PREFIX: &str = "vmon";
const POOL_BASE: Ipv4Addr = Ipv4Addr::new(172, 20, 0, 0);
const POOL_PREFIX: u8 = 16;
const LEASE_PREFIX: u8 = 30;
const LEASE_STEP: u32 = 1 << (32 - LEASE_PREFIX);
const POOL_SIZE: u32 = 1 << (32 - POOL_PREFIX);

/// DNS servers injected into TAP-backed guests.
pub const DEFAULT_DNS: [&str; 2] = ["1.1.1.1", "8.8.8.8"];
/// The fixed libslirp guest address.
pub const USER_NET_GUEST_IP: &str = "10.0.2.15";
/// The fixed libslirp prefix length.
pub const USER_NET_PREFIX: u8 = 24;
/// The fixed libslirp gateway address.
pub const USER_NET_GATEWAY: &str = "10.0.2.2";
/// DNS servers exposed by libslirp.
pub const USER_NET_DNS: [&str; 1] = ["10.0.2.3"];

/// Returns cumulative received and transmitted bytes on non-loopback host
/// interfaces.
///
/// Linux reads the kernel's `/proc/net/dev` counters; unsupported platforms and
/// unreadable counter files report no available counters.
#[cfg_attr(
	not(target_os = "linux"),
	allow(clippy::missing_const_for_fn, reason = "Linux reads runtime network counters.")
)]
pub(crate) fn host_network_bytes() -> Option<(u64, u64)> {
	#[cfg(target_os = "linux")]
	{
		Some(parse_proc_net_dev(&fs::read_to_string(PROC_NET_DEV).ok()?))
	}
	#[cfg(not(target_os = "linux"))]
	{
		None
	}
}

#[cfg(any(test, target_os = "linux"))]
fn parse_proc_net_dev(contents: &str) -> (u64, u64) {
	let mut ingress = 0_u64;
	let mut egress = 0_u64;

	for row in contents.lines() {
		let Some((interface, columns)) = row.rsplit_once(':') else {
			continue;
		};
		if interface.trim() == "lo" {
			continue;
		}
		let mut columns = columns.split_whitespace();
		let Some(received) = columns.next().and_then(|value| value.parse::<u64>().ok()) else {
			continue;
		};
		let Some(transmitted) = columns.nth(7).and_then(|value| value.parse::<u64>().ok()) else {
			continue;
		};
		ingress = ingress.saturating_add(received);
		egress = egress.saturating_add(transmitted);
	}

	(ingress, egress)
}

#[cfg(any(test, target_os = "linux"))]
static START_INSTANT: std::sync::LazyLock<std::time::Instant> =
	std::sync::LazyLock::new(std::time::Instant::now);

#[cfg(target_os = "linux")]
static REFRESHERS: LazyLock<Mutex<HashMap<String, DomainRefresher>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(target_os = "linux")]
static BROKER_SOCKET: LazyLock<Mutex<Option<PathBuf>>> = LazyLock::new(|| Mutex::new(None));
/// A host TAP lease and its forwarding policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TapLease {
	pub name:         String,
	pub guest_ip:     String,
	pub host_ip:      String,
	pub prefix:       u8,
	pub network:      String,
	pub egress_allow: Vec<String>,
}

impl TapLease {
	/// Build and validate a point-to-point TAP lease.
	pub fn new(
		name: &str,
		guest_ip: &str,
		host_ip: &str,
		prefix: u8,
		egress_allow: Option<&[String]>,
	) -> Result<Self> {
		if name.is_empty() || name.len() >= 16 {
			return Err(EngineError::invalid("tap name must be non-empty and shorter than 16 bytes"));
		}
		let guest = parse_ipv4(guest_ip)?;
		let host = parse_ipv4(host_ip)?;
		if prefix != LEASE_PREFIX {
			return Err(EngineError::invalid("sandbox TAP networking requires a /30 prefix"));
		}
		let network = ipv4_network(host, prefix)?;
		if !ipv4_contains(network, prefix, guest) || !ipv4_contains(network, prefix, host) {
			return Err(EngineError::invalid(format!(
				"{guest} and {host} are not both in {network}/{prefix}"
			)));
		}
		if !ipv4_contains(POOL_BASE, POOL_PREFIX, network) {
			return Err(EngineError::invalid(format!(
				"sandbox TAP network must be inside {POOL_BASE}/{POOL_PREFIX}"
			)));
		}
		let network_value = u32::from(network);
		if u32::from(host) != network_value + 1 || u32::from(guest) != network_value + 2 {
			return Err(EngineError::invalid(
				"sandbox TAP addresses must use network+1 for host and network+2 for guest",
			));
		}
		let mut allowed = Vec::new();
		for cidr in egress_allow.unwrap_or(&[]) {
			allowed.push(cidr.parse::<CidrNet>()?.to_string());
		}
		Ok(Self {
			name: name.to_owned(),
			guest_ip: guest.to_string(),
			host_ip: host.to_string(),
			prefix,
			network: format!("{network}/{prefix}"),
			egress_allow: allowed,
		})
	}

	/// The guest address as an iptables host CIDR.
	pub fn guest_cidr(&self) -> String {
		format!("{}/32", self.guest_ip)
	}

	/// The host TAP address with the TAP prefix.
	pub fn host_cidr(&self) -> String {
		format!("{}/{}", self.host_ip, self.prefix)
	}
}

/// A persisted TAP allocation for a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestConfig {
	pub tap:      String,
	pub host_ip:  String,
	pub guest_ip: String,
	pub prefix:   u8,
	pub network:  String,
	pub dns:      Vec<String>,
}

/// The subset of network config handed to the guest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestNetworkConfig {
	pub tap:      String,
	pub guest_ip: String,
	pub host_ip:  String,
	pub prefix:   u8,
	pub dns:      Vec<String>,
}

impl From<&GuestConfig> for GuestNetworkConfig {
	fn from(value: &GuestConfig) -> Self {
		Self {
			tap:      value.tap.clone(),
			guest_ip: value.guest_ip.clone(),
			host_ip:  value.host_ip.clone(),
			prefix:   value.prefix,
			dns:      value.dns.clone(),
		}
	}
}

/// A configured sandbox network and its loopback port tunnels.
pub struct SandboxNetwork {
	pub name:             String,
	pub config:           GuestConfig,
	pub tap:              TapLease,
	pub guest_config:     GuestNetworkConfig,
	tunnels:              TunnelSet,
	egress_allow:         Option<Vec<String>>,
	egress_allow_domains: Option<Vec<String>>,
	vpc_bridge:           Option<String>,
}

impl SandboxNetwork {
	/// Return guest-port to local loopback endpoint mappings.
	pub fn tunnels(&self) -> BTreeMap<u16, (String, u16)> {
		self.tunnels.mapping()
	}

	/// Permit only the fixed host credential-gateway TCP port.
	pub fn allow_credential_gateway(&self) -> Result<()> {
		broker_request(broker::Request::AllowCredential { lease: (&self.tap).into() }).map(|_| ())
	}

	/// Tear down tunnels, host policy, and the persisted lease.
	pub fn teardown(self) -> Result<()> {
		let Self {
			name,
			config: _,
			tap,
			guest_config: _,
			tunnels,
			egress_allow,
			egress_allow_domains,
			vpc_bridge,
		} = self;
		drop(tunnels);
		if let Some(bridge) = vpc_bridge {
			SystemVpcBridge.detach_tap(&bridge, &tap.name)?;
		} else {
			teardown_tap(
				&tap.name,
				Some(&tap.guest_ip),
				Some(&tap.host_ip),
				tap.prefix,
				egress_allow.as_deref(),
				egress_allow_domains.as_deref(),
			)?;
			release_guest_config(&name)?;
		}
		Ok(())
	}
}

/// A set of loopback TCP tunnels into a guest.
pub struct TunnelSet {
	mapping:          BTreeMap<u16, (String, u16)>,
	accept_tasks:     Vec<TokioJoinHandle<()>>,
	connection_tasks: Arc<Mutex<Vec<TokioJoinHandle<()>>>>,
}

impl TunnelSet {
	/// Start loopback TCP proxies for the requested guest ports.
	pub async fn new(
		guest_ip: &str,
		ports: &[u16],
		inbound_cidr_allowlist: Option<&[String]>,
	) -> Result<Self> {
		let guest_addr = parse_ip(guest_ip)?;
		let allowed_nets = Arc::new(parse_cidr_allowlist(inbound_cidr_allowlist)?);
		let mut unique = Vec::new();
		let mut seen = BTreeSet::new();
		for &port in ports {
			if port == 0 {
				return Err(EngineError::invalid("invalid TCP port 0"));
			}
			if seen.insert(port) {
				unique.push(port);
			}
		}

		let connection_tasks = Arc::new(Mutex::new(Vec::new()));
		let mut accept_tasks = Vec::new();
		let mut mapping = BTreeMap::new();
		for guest_port in unique {
			let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
			let local = listener.local_addr()?;
			mapping.insert(guest_port, (local.ip().to_string(), local.port()));
			let task_allowed = Arc::clone(&allowed_nets);
			let task_connections = Arc::clone(&connection_tasks);
			let guest = SocketAddr::new(guest_addr, guest_port);
			accept_tasks.push(tokio::spawn(async move {
				accept_tunnel(listener, guest, task_allowed, task_connections).await;
			}));
		}

		Ok(Self { mapping, accept_tasks, connection_tasks })
	}

	/// Return guest-port to local loopback endpoint mappings.
	pub fn mapping(&self) -> BTreeMap<u16, (String, u16)> {
		self.mapping.clone()
	}
}

impl Drop for TunnelSet {
	fn drop(&mut self) {
		for task in &self.accept_tasks {
			task.abort();
		}
		for task in self.connection_tasks.lock().drain(..) {
			task.abort();
		}
	}
}

/// Allocate, configure, and expose host networking for a sandbox.
pub async fn setup_sandbox_network(
	name: &str,
	ports: &[u16],
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	inbound_cidr_allowlist: Option<&[String]>,
) -> Result<SandboxNetwork> {
	let config = allocate_guest_config(name)?;
	let policy_result = policy::EgressPolicy::new(
		policy::SystemResolver,
		egress_allow_domains.unwrap_or(&[]),
		egress_allow.unwrap_or(&[]),
	)
	.and_then(|p| {
		let dns_ips = config
			.dns
			.iter()
			.map(|s| {
				s.parse::<IpAddr>()
					.map_err(|_| EngineError::invalid(format!("invalid DNS address {s}")))
			})
			.collect::<Result<Vec<IpAddr>>>()?;
		p.approved_dns(&dns_ips)?;
		Ok(())
	});
	if let Err(err) = policy_result {
		let _ = release_guest_config(name);
		return Err(err);
	}
	let tap_result = setup_tap(
		&config.tap,
		&config.guest_ip,
		&config.host_ip,
		config.prefix,
		egress_allow,
		egress_allow_domains,
		None,
	);
	let tap = match tap_result {
		Ok(tap) => tap,
		Err(err) => {
			let _ = release_guest_config(name);
			return Err(err);
		},
	};

	let tunnels = match TunnelSet::new(&config.guest_ip, ports, inbound_cidr_allowlist).await {
		Ok(tunnels) => tunnels,
		Err(err) => {
			let _ = teardown_tap(
				&config.tap,
				Some(&config.guest_ip),
				Some(&config.host_ip),
				config.prefix,
				egress_allow,
				egress_allow_domains,
			);
			let _ = release_guest_config(name);
			return Err(err);
		},
	};
	let guest_config = GuestNetworkConfig::from(&config);
	Ok(SandboxNetwork {
		name: name.to_owned(),
		config,
		tap,
		guest_config,
		tunnels,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		vpc_bridge: None,
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
	})
}

/// Host bridge operations used by routed VPC networking.
///
/// `Sync` keeps async VPC setup futures `Send`; implementations are stateless
/// command seams.
pub(crate) trait VpcBridge: Sync {
	fn ensure_bridge(&self, bridge: &str, gateway: &str, prefix: u8) -> Result<()>;
	fn attach_tap(&self, bridge: &str, tap: &str) -> Result<()>;
	fn detach_tap(&self, bridge: &str, tap: &str) -> Result<()>;
	fn delete_bridge(&self, bridge: &str) -> Result<()>;
}

pub(crate) struct SystemVpcBridge;

impl VpcBridge for SystemVpcBridge {
	fn ensure_bridge(&self, bridge: &str, gateway: &str, prefix: u8) -> Result<()> {
		system_vpc_ensure_bridge(bridge, gateway, prefix)
	}

	fn attach_tap(&self, bridge: &str, tap: &str) -> Result<()> {
		system_vpc_attach_tap(bridge, tap)
	}

	fn detach_tap(&self, _bridge: &str, tap: &str) -> Result<()> {
		system_vpc_detach_tap(tap)
	}

	fn delete_bridge(&self, bridge: &str) -> Result<()> {
		system_vpc_delete_bridge(bridge)
	}
}

pub(crate) fn vpc_bridge_name(id: &str) -> Result<String> {
	let suffix = id
		.strip_prefix("vpc-")
		.filter(|suffix| suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()))
		.ok_or_else(|| EngineError::invalid("VPC id must be a vpc-<8hex> identifier"))?;
	Ok(format!("vmonbr-{suffix}"))
}

pub(crate) fn delete_vpc_bridge(id: &str) -> Result<()> {
	SystemVpcBridge.delete_bridge(&vpc_bridge_name(id)?)
}

pub(crate) fn require_vpc_host() -> Result<()> {
	if cfg!(target_os = "linux") {
		Ok(())
	} else {
		Err(EngineError::invalid("VPCs require a Linux host"))
	}
}

pub(crate) async fn setup_vpc_network(
	name: &str,
	vpc_id: &str,
	guest_ip: &str,
	gateway: &str,
	prefix: u8,
	ports: &[u16],
	inbound_cidr_allowlist: Option<&[String]>,
) -> Result<SandboxNetwork> {
	setup_vpc_network_with_bridge(
		&SystemVpcBridge,
		name,
		vpc_id,
		guest_ip,
		gateway,
		prefix,
		ports,
		inbound_cidr_allowlist,
	)
	.await
}

async fn setup_vpc_network_with_bridge(
	bridge_ops: &dyn VpcBridge,
	name: &str,
	vpc_id: &str,
	guest_ip: &str,
	gateway: &str,
	prefix: u8,
	ports: &[u16],
	inbound_cidr_allowlist: Option<&[String]>,
) -> Result<SandboxNetwork> {
	if !cfg!(target_os = "linux") && !cfg!(test) {
		return Err(EngineError::invalid("VPCs require a Linux host"));
	}
	let bridge = vpc_bridge_name(vpc_id)?;
	let tap_name = tap_name(name);
	bridge_ops.ensure_bridge(&bridge, gateway, prefix)?;
	bridge_ops.attach_tap(&bridge, &tap_name)?;
	let tunnels = match TunnelSet::new(guest_ip, ports, inbound_cidr_allowlist).await {
		Ok(tunnels) => tunnels,
		Err(error) => {
			let _ = bridge_ops.detach_tap(&bridge, &tap_name);
			return Err(error);
		},
	};
	let dns = DEFAULT_DNS
		.iter()
		.map(|value| (*value).to_owned())
		.collect::<Vec<_>>();
	let network = format!(
		"{}/{}",
		Ipv4Addr::from(u32::from(parse_ipv4(guest_ip)?) & (u32::MAX << (32 - prefix))),
		prefix
	);
	let config = GuestConfig {
		tap: tap_name.clone(),
		host_ip: gateway.to_owned(),
		guest_ip: guest_ip.to_owned(),
		prefix,
		network,
		dns: dns.clone(),
	};
	let guest_config = GuestNetworkConfig {
		tap: tap_name.clone(),
		guest_ip: guest_ip.to_owned(),
		host_ip: gateway.to_owned(),
		prefix,
		dns,
	};
	Ok(SandboxNetwork {
		name: name.to_owned(),
		config,
		tap: TapLease {
			name: tap_name,
			guest_ip: guest_ip.to_owned(),
			host_ip: gateway.to_owned(),
			prefix,
			network: vpc_id.to_owned(),
			egress_allow: Vec::new(),
		},
		guest_config,
		tunnels,
		egress_allow: None,
		egress_allow_domains: None,
		vpc_bridge: Some(bridge),
	})
}

/// Allocate or return the persisted guest network config for a sandbox.
pub fn allocate_guest_config(name: &str) -> Result<GuestConfig> {
	allocate_guest_config_with_dns(name, &DEFAULT_DNS)
}

/// Allocate or return the persisted guest network config with custom DNS.
///
/// Warm slot pool: claims a preallocated slot purely in memory. Otherwise
/// falls back to the in-memory /30 allocator (journal-backed).
pub fn allocate_guest_config_with_dns(name: &str, dns: &[&str]) -> Result<GuestConfig> {
	if let Some(config) = slots::pool_lookup(name) {
		return Ok(config);
	}
	let store = LeaseStore::default();
	if let Some(config) = store.lease_for(name) {
		return Ok(config);
	}
	if let Some(config) = slots::pool_claim(name, dns) {
		return Ok(config);
	}
	store.allocate(name, dns)
}

/// Release a sandbox's guest network config (slot binding or persisted lease).
pub fn release_guest_config(name: &str) -> Result<()> {
	if let Some((slot, custom)) = slots::pool_release_name(name) {
		// Normal flows recycle through `teardown_tap` first; this path only
		// fires when setup failed before the TAP was touched.
		if custom {
			broker_request(broker::Request::RecycleSlot { slot })?;
		}
		return Ok(());
	}
	LeaseStore::default().release(name)
}

/// Return the lease for a sandbox without allocating one.
pub fn lease_for(name: &str) -> Option<GuestConfig> {
	slots::pool_lookup(name).or_else(|| LeaseStore::default().lease_for(name))
}

/// Return the deterministic TAP name for a sandbox name.
pub fn tap_name(name: &str) -> String {
	let mut hasher = Sha1::new();
	hasher.update(name.as_bytes());
	let digest = hasher.finalize();
	format!("tv{}", hex::encode(digest)[..10].to_owned())
}

/// Configure the externally managed privileged network broker socket.
pub fn configure_broker_socket(socket: Option<PathBuf>) {
	#[cfg(target_os = "linux")]
	{
		*BROKER_SOCKET.lock() = socket;
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = socket;
	}
}

/// Return whether a privileged network broker is configured.
#[cfg(target_os = "linux")]
pub fn has_net_admin() -> bool {
	BROKER_SOCKET.lock().is_some()
}

/// Return whether a privileged network broker is configured.
#[cfg(not(target_os = "linux"))]
pub const fn has_net_admin() -> bool {
	false
}
/// Test seam: when installed, every broker round trip is routed through this
/// handler instead of the Unix socket, letting tests count and answer them.
#[cfg(test)]
type BrokerStubFn = Box<dyn FnMut(&broker::Request) -> Result<Option<TapLease>> + Send>;

#[cfg(test)]
pub(crate) static BROKER_STUB: LazyLock<Mutex<Option<BrokerStubFn>>> =
	LazyLock::new(|| Mutex::new(None));

fn broker_request(request: broker::Request) -> Result<Option<TapLease>> {
	#[cfg(test)]
	if let Some(stub) = BROKER_STUB.lock().as_mut() {
		return stub(&request);
	}
	#[cfg(target_os = "linux")]
	{
		let socket = BROKER_SOCKET
			.lock()
			.clone()
			.ok_or_else(|| EngineError::engine("network broker is not configured"))?;
		broker::call(&socket, request)
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = request;
		Err(EngineError::unsupported("host TAP networking requires Linux"))
	}
}

#[cfg(target_os = "linux")]
fn has_net_admin_direct() -> bool {
	// SAFETY: geteuid has no preconditions and reads process credentials.
	if unsafe { libc::geteuid() } == 0 {
		return true;
	}
	let Ok(status) = fs::read_to_string("/proc/self/status") else {
		return false;
	};
	for line in status.lines() {
		if let Some(value) = line.strip_prefix("CapEff:") {
			let Some(bits) = value.split_whitespace().next() else {
				return false;
			};
			let Ok(mask) = u64::from_str_radix(bits, 16) else {
				return false;
			};
			return (mask & (1_u64 << CAP_NET_ADMIN)) != 0;
		}
	}
	false
}

#[cfg(target_os = "linux")]
fn require_net_admin() -> Result<()> {
	if has_net_admin_direct() {
		Ok(())
	} else {
		Err(EngineError::engine("network setup needs root or CAP_NET_ADMIN"))
	}
}

/// Create/configure a TAP interface and its NAT/FORWARD rules.
#[cfg(any(test, target_os = "linux"))]
pub const PROTECTED_V4_RANGES: &[&str] = &[
	"0.0.0.0/8",
	"127.0.0.0/8",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
	"224.0.0.0/4",
	"255.255.255.255/32",
	"100.64.0.0/10",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.88.99.0/24",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"240.0.0.0/4",
];

#[cfg(any(test, target_os = "linux"))]
fn protected_deny_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	range: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		range,
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("deny-range-{idx}")),
		"-j",
		"REJECT",
	])
}

#[cfg(target_os = "linux")]
fn credential_gateway_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let port = CREDENTIAL_GATEWAY_PORT.to_string();
	strings([
		action.flag(),
		"INPUT",
		"-i",
		&lease.name,
		"-s",
		&lease.guest_cidr(),
		"-d",
		&lease.host_ip,
		"-p",
		"tcp",
		"--dport",
		&port,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "credential-gateway"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(target_os = "linux")]
fn host_deny_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	strings([
		action.flag(),
		"INPUT",
		"-i",
		&lease.name,
		"-s",
		&lease.guest_cidr(),
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "host-deny"),
		"-j",
		"DROP",
	])
}

#[cfg(target_os = "linux")]
pub(crate) fn allow_credential_gateway_direct(lease: &TapLease) -> Result<()> {
	iptables_ensure(&credential_gateway_rule(IptablesAction::Insert, lease))
}

/// Configure forwarding policy for a TAP.
///
/// Pooled slots: the TAP and base rules already exist, so the default policy
/// returns without any broker round trip; a custom policy (any `egress_allow`
/// or `egress_allow_domains`, including empty deny-all lists) is applied as
/// one batched broker request that fills the slot's nftables sets.
/// Non-pooled TAPs go through the original per-create broker setup.
pub fn setup_tap(
	name: &str,
	guest_ip: &str,
	host_ip: &str,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	previous_egress_allow: Option<&[String]>,
) -> Result<TapLease> {
	if let Some((slot, already_custom)) = slots::pool_slot_for_tap(name) {
		if slot.guest_ip != guest_ip || slot.host_ip != host_ip || slot.prefix != prefix {
			return Err(EngineError::invalid("slot TAP addresses do not match its claim"));
		}
		let lease = TapLease::new(name, guest_ip, host_ip, prefix, egress_allow)?;
		let domains = clean_domains(egress_allow_domains);
		let restricted = egress_allow.is_some() || !domains.is_empty();
		// Recycle semantics replace exact-delete bookkeeping on slots.
		let _ = previous_egress_allow;
		if restricted {
			broker_request(broker::Request::ClaimSlot {
				slot,
				egress_allow: lease.egress_allow.clone(),
				egress_allow_domains: domains,
				already_custom,
			})?;
			slots::pool_set_custom(name, true);
		} else if already_custom {
			broker_request(broker::Request::RecycleSlot { slot })?;
			slots::pool_set_custom(name, false);
		}
		return Ok(lease);
	}
	let response = broker_request(broker::Request::Setup {
		name: name.to_owned(),
		guest_ip: guest_ip.to_owned(),
		host_ip: host_ip.to_owned(),
		prefix,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
		previous_egress_allow: previous_egress_allow.map(<[String]>::to_vec),
	})?;
	response.ok_or_else(|| EngineError::engine("network broker returned no TAP lease"))
}

#[cfg(target_os = "linux")]
/// Create one TAP owned by the authenticated unprivileged daemon user.
pub(crate) fn setup_tap_direct(
	name: &str,
	guest_ip: &str,
	host_ip: &str,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
	previous_egress_allow: Option<&[String]>,
	owner_uid: u32,
) -> Result<TapLease> {
	require_net_admin()?;
	let lease = TapLease::new(name, guest_ip, host_ip, prefix, egress_allow)?;

	if !ip_link_exists(name)? {
		run(
			&[
				"ip".to_owned(),
				"tuntap".to_owned(),
				"add".to_owned(),
				"dev".to_owned(),
				name.to_owned(),
				"mode".to_owned(),
				"tap".to_owned(),
				"user".to_owned(),
				owner_uid.to_string(),
			],
			true,
		)?;
	}
	run(
		&[
			"ip".to_owned(),
			"addr".to_owned(),
			"replace".to_owned(),
			lease.host_cidr(),
			"dev".to_owned(),
			name.to_owned(),
		],
		true,
	)?;
	run(&argv(["ip", "link", "set", "dev", name, "up"]), true)?;
	disable_ipv6(name)?;
	enable_ip_forward()?;

	iptables_ensure(&masq_rule(IptablesAction::Append, &lease))?;
	iptables_ensure(&return_rule(IptablesAction::Append, &lease))?;
	iptables_delete(&credential_gateway_rule(IptablesAction::Delete, &lease))?;
	iptables_delete(&host_deny_rule(IptablesAction::Delete, &lease))?;
	iptables_ensure(&host_deny_rule(IptablesAction::Insert, &lease))?;

	let domains = clean_domains(egress_allow_domains);
	let restricted = egress_allow.is_some() || !domains.is_empty();
	let prepared_domains = if restricted {
		let mut policy =
			policy::EgressPolicy::new(policy::SystemResolver, &domains, &lease.egress_allow)?;
		let now = START_INSTANT.elapsed();
		let mut rules = BTreeMap::new();
		let mut next_idx = 0;
		for domain in &domains {
			for ip in policy.resolve_allowed(domain, now)? {
				let ip = ip.to_string();
				if let Entry::Vacant(entry) = rules.entry(ip) {
					entry.insert(next_idx);
					next_idx += 1;
				}
			}
		}
		Some((rules, next_idx))
	} else {
		None
	};
	// Keep a terminal deny in place throughout every policy replacement. New
	// rules are installed around it, and unrestricted mode removes it only
	// after the protected-range denies and public allow are complete.
	iptables_ensure(&deny_egress_rule(IptablesAction::Append, &lease))?;
	let prior_domain_ips = stop_domain_refresher(name);
	iptables_delete(&unrestricted_egress_rule(IptablesAction::Delete, &lease))?;
	for (ip, idx) in &prior_domain_ips {
		iptables_delete(&domain_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			ip,
			*idx,
		))?;
	}
	for (idx, cidr) in previous_egress_allow.unwrap_or(&[]).iter().enumerate() {
		iptables_delete(&egress_cidr_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			cidr,
			idx,
		))?;
	}
	for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
		iptables_delete(&protected_deny_rule(
			IptablesAction::Delete,
			&lease.name,
			&lease.guest_cidr(),
			range,
			idx,
		))?;
	}

	if restricted {
		for (idx, cidr) in lease.egress_allow.iter().enumerate() {
			iptables_ensure(&egress_cidr_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				cidr,
				idx,
			))?;
		}
		let (rules, next_idx) = prepared_domains.expect("restricted policy was prepared");
		for (ip, idx) in &rules {
			iptables_ensure(&domain_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				ip,
				*idx,
			))?;
		}
		if !domains.is_empty() {
			start_domain_refresher(
				&lease.name,
				RefreshTarget::Iptables { guest_cidr: lease.guest_cidr() },
				domains,
				rules,
				next_idx,
				lease.egress_allow.clone(),
			)?;
		}
	} else {
		for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
			iptables_ensure(&protected_deny_rule(
				IptablesAction::Insert,
				&lease.name,
				&lease.guest_cidr(),
				range,
				idx,
			))?;
		}
		iptables_ensure(&unrestricted_egress_rule(IptablesAction::Append, &lease))?;
		iptables_delete(&deny_egress_rule(IptablesAction::Delete, &lease))?;
	}

	Ok(lease)
}

/// Remove a TAP's forwarding policy.
///
/// Pooled slots are RECYCLED: the slot returns to the free list and only a
/// custom policy costs one broker round trip (flushing the slot's dynamic set
/// elements); the TAP device and base rules stay installed for the next
/// claim. Non-pooled TAPs are deleted with their exact policy rules as
/// before.
pub fn teardown_tap(
	name: &str,
	guest_ip: Option<&str>,
	host_ip: Option<&str>,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
) -> Result<()> {
	if let Some((slot, custom)) = slots::pool_recycle_tap(name) {
		if custom {
			broker_request(broker::Request::RecycleSlot { slot })?;
		}
		return Ok(());
	}
	broker_request(broker::Request::Teardown {
		name: name.to_owned(),
		guest_ip: guest_ip.map(str::to_owned),
		host_ip: host_ip.map(str::to_owned),
		prefix,
		egress_allow: egress_allow.map(<[String]>::to_vec),
		egress_allow_domains: egress_allow_domains.map(<[String]>::to_vec),
	})
	.map(|_| ())
}

#[cfg(target_os = "linux")]
pub(crate) fn teardown_tap_direct(
	name: &str,
	guest_ip: Option<&str>,
	host_ip: Option<&str>,
	prefix: u8,
	egress_allow: Option<&[String]>,
	egress_allow_domains: Option<&[String]>,
) -> Result<()> {
	let seen_rules = stop_domain_refresher(name);
	if !has_net_admin_direct() {
		return Err(EngineError::engine("network broker needs root or CAP_NET_ADMIN"));
	}

	if let (Some(guest_ip), Some(host_ip)) = (guest_ip, host_ip) {
		let lease = TapLease::new(name, guest_ip, host_ip, prefix, egress_allow)?;
		iptables_delete(&masq_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&return_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&credential_gateway_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&host_deny_rule(IptablesAction::Delete, &lease))?;
		iptables_delete(&unrestricted_egress_rule(IptablesAction::Delete, &lease))?;
		for (idx, range) in PROTECTED_V4_RANGES.iter().enumerate() {
			iptables_delete(&protected_deny_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				range,
				idx,
			))?;
		}
		for (idx, cidr) in lease.egress_allow.iter().enumerate() {
			iptables_delete(&egress_cidr_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				cidr,
				idx,
			))?;
		}
		iptables_delete(&deny_egress_rule(IptablesAction::Delete, &lease))?;

		let mut domain_rules = seen_rules;
		if let Some(allowed_domains) = egress_allow_domains {
			let clean = clean_domains(Some(allowed_domains));
			if let Ok(mut policy) =
				policy::EgressPolicy::new(policy::SystemResolver, &clean, &lease.egress_allow)
			{
				let now = START_INSTANT.elapsed();
				for domain in &clean {
					if let Ok(ips) = policy.resolve_allowed(domain, now) {
						for ip in ips {
							let ip_str = ip.to_string();
							domain_rules.entry(ip_str).or_insert(0);
						}
					}
				}
			}
		}
		for (ip, idx) in domain_rules {
			iptables_delete(&domain_rule(
				IptablesAction::Delete,
				&lease.name,
				&lease.guest_cidr(),
				&ip,
				idx,
			))?;
		}
	}

	if ip_link_exists(name)? {
		run(&argv(["ip", "link", "del", "dev", name]), false)?;
	}
	Ok(())
}

/// Slots per `PreallocateSlots` broker frame (frames are capped at 64 KiB).
const SLOT_BATCH: usize = 200;

/// Configure the preallocated network slot pool (`0` disables pooling).
///
/// Filling runs on a background thread so serve start is not delayed; until
/// the pool is warm (or when it is exhausted), creates use the per-create
/// path unchanged. A failed fill logs a warning and leaves pooling off.
pub fn configure_slot_pool(count: usize) {
	if count == 0 {
		return;
	}
	thread::Builder::new()
		.name("vmon-net-slot-fill".to_owned())
		.spawn(move || match fill_slot_pool(count) {
			Ok(()) => tracing::info!(count, "network slot pool ready"),
			Err(error) => {
				tracing::warn!(
					%error,
					"network slot pool preallocation failed; using per-create networking"
				);
			},
		})
		.map_or_else(|error| tracing::warn!(%error, "spawning network slot fill failed"), drop);
}

/// Carve slot leases from the in-memory allocator and preallocate their TAPs
/// plus base policy through the broker in batches.
fn fill_slot_pool(count: usize) -> Result<()> {
	let store = LeaseStore::default();
	let mut specs = Vec::with_capacity(count);
	for index in 0..u32::try_from(count).map_err(|_| EngineError::invalid("net_slots too large"))? {
		let tap = slots::slot_tap_name(index);
		let config =
			store.allocate_with_tap(&slots::slot_lease_name(index), &DEFAULT_DNS, Some(&tap))?;
		specs.push(slots::SlotSpec {
			index,
			name: tap,
			guest_ip: config.guest_ip,
			host_ip: config.host_ip,
			prefix: config.prefix,
		});
	}
	// Slot reservations must reach the journal before the TAPs exist.
	store.flush()?;
	for (batch, chunk) in specs.chunks(SLOT_BATCH).enumerate() {
		broker_request(broker::Request::PreallocateSlots {
			slots: chunk.to_vec(),
			reset: batch == 0,
		})?;
	}
	slots::pool_install(specs);
	Ok(())
}

/// Broker side: create a batch of slot TAPs with base rules, then install the
/// pooled-slot nftables skeleton in one `nft -f -` invocation.
#[cfg(target_os = "linux")]
pub(crate) fn preallocate_slots_direct(
	specs: &[slots::SlotSpec],
	reset: bool,
	owner_uid: u32,
) -> Result<()> {
	for spec in specs {
		setup_tap_direct(
			&spec.name,
			&spec.guest_ip,
			&spec.host_ip,
			spec.prefix,
			None,
			None,
			None,
			owner_uid,
		)?;
	}
	run_nft(&slots::pool_init_payload(specs, reset))
}

/// Broker side: apply a custom egress policy to one pooled slot.
///
/// Resolves domain allowlists, then executes ONE `nft -f -` batch carrying
/// every element operation. Starts a set-targeted domain refresher when
/// domains are present.
#[cfg(target_os = "linux")]
pub(crate) fn claim_slot_direct(
	slot: &slots::SlotSpec,
	egress_allow: &[String],
	egress_allow_domains: &[String],
	already_custom: bool,
) -> Result<()> {
	// Policy replacement: drop any refresher from the previous claim.
	let _ = stop_domain_refresher(&slot.name);
	let mut elements = Vec::with_capacity(egress_allow.len());
	for cidr in egress_allow {
		let net = cidr.parse::<CidrNet>()?;
		if matches!(net, CidrNet::V6 { .. }) {
			return Err(EngineError::invalid("pooled slot egress allows IPv4 CIDRs only"));
		}
		elements.push(net.to_string());
	}
	let domains = clean_domains(Some(egress_allow_domains));
	let mut rules = BTreeMap::new();
	let mut next_idx = 0;
	if !domains.is_empty() {
		let mut policy = policy::EgressPolicy::new(policy::SystemResolver, &domains, egress_allow)?;
		let now = START_INSTANT.elapsed();
		for domain in &domains {
			for ip in policy.resolve_allowed(domain, now)? {
				let IpAddr::V4(ip) = ip else { continue };
				let ip = ip.to_string();
				if let Entry::Vacant(entry) = rules.entry(ip.clone()) {
					entry.insert(next_idx);
					next_idx += 1;
					elements.push(ip);
				}
			}
		}
	}
	run_nft(&slots::claim_payload(slot, &elements, already_custom))?;
	if !domains.is_empty() {
		start_domain_refresher(
			&slot.name,
			RefreshTarget::NftSet { set: slot.egress_set() },
			domains,
			rules,
			next_idx,
			egress_allow.to_vec(),
		)?;
	}
	Ok(())
}

/// Broker side: reset one pooled slot back to the preinstalled base policy.
#[cfg(target_os = "linux")]
pub(crate) fn recycle_slot_direct(slot: &slots::SlotSpec) -> Result<()> {
	let _ = stop_domain_refresher(&slot.name);
	run_nft(&slots::recycle_payload(slot))
}

/// Execute one batched nftables program via `nft -f -`.
#[cfg(target_os = "linux")]
fn run_nft(payload: &str) -> Result<()> {
	use std::process::Stdio;
	// nft -f applies the batch transactionally; concurrent nft processes can
	// race ruleset generation updates, so serialize spawn-to-wait per process.
	static NFT_LOCK: Mutex<()> = Mutex::new(());
	if payload.is_empty() {
		return Ok(());
	}
	let _guard = NFT_LOCK.lock();
	let mut child = Command::new("nft")
		.args(["-f", "-"])
		.stdin(Stdio::piped())
		.stdout(Stdio::null())
		.stderr(Stdio::piped())
		.spawn()
		.map_err(|error| EngineError::engine(format!("spawning nft failed: {error}")))?;
	child
		.stdin
		.take()
		.ok_or_else(|| EngineError::engine("nft stdin unavailable"))?
		.write_all(payload.as_bytes())?;
	let output = child.wait_with_output()?;
	if output.status.success() {
		Ok(())
	} else {
		Err(EngineError::engine(format!(
			"nft -f failed with status {}: {}",
			output.status.code().unwrap_or(-1),
			String::from_utf8_lossy(&output.stderr).trim()
		)))
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaseEntry {
	network: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	tap:     Option<String>,
}

/// Handle to the in-memory /30 lease allocator behind one journal path.
///
/// The journal (a JSON map in the historical on-disk format) is parsed ONCE
/// per path; allocate/release hit memory and enqueue a snapshot for a
/// background writer thread that rewrites the file under an exclusive flock.
/// Existing deployments keep their leases; the per-operation flock + parse is
/// gone from the hot path.
struct LeaseStore {
	path: PathBuf,
}

/// In-memory lease table plus the free list of /30 network blocks.
struct LeaseAllocator {
	leases: BTreeMap<String, LeaseEntry>,
	free:   BTreeSet<u32>,
}

impl LeaseAllocator {
	/// Parse the journal (or start empty) and derive the free-block set.
	fn load(path: &std::path::Path) -> Result<Self> {
		let leases = match fs::read_to_string(path) {
			Ok(raw) if !raw.trim().is_empty() => {
				serde_json::from_str::<BTreeMap<String, LeaseEntry>>(&raw)?
			},
			_ => BTreeMap::new(),
		};
		let mut free = (0..POOL_SIZE)
			.step_by(LEASE_STEP as usize)
			.map(|offset| u32::from(POOL_BASE) + offset)
			.collect::<BTreeSet<u32>>();
		for entry in leases.values() {
			if let Some(base) = lease_network_base(&entry.network) {
				free.remove(&base);
			}
		}
		Ok(Self { leases, free })
	}

	/// Allocate (or return) the lease for `name`; `true` when state changed.
	fn allocate(
		&mut self,
		name: &str,
		dns: &[&str],
		tap: Option<&str>,
	) -> Result<(GuestConfig, bool)> {
		if let Some(entry) = self.leases.get(name) {
			return Ok((config_from_entry(name, entry, dns)?, false));
		}
		let Some(base) = self.free.pop_first() else {
			return Err(EngineError::engine(format!(
				"no free /30 networks left in {POOL_BASE}/{POOL_PREFIX}"
			)));
		};
		let entry = LeaseEntry {
			network: format!("{}/{LEASE_PREFIX}", Ipv4Addr::from(base)),
			tap:     Some(tap.map_or_else(|| tap_name(name), str::to_owned)),
		};
		let config = config_from_entry(name, &entry, dns)?;
		self.leases.insert(name.to_owned(), entry);
		Ok((config, true))
	}

	/// Release `name`'s lease; `true` when a lease was actually removed.
	fn release(&mut self, name: &str) -> bool {
		let Some(entry) = self.leases.remove(name) else {
			return false;
		};
		if let Some(base) = lease_network_base(&entry.network) {
			self.free.insert(base);
		}
		true
	}

	fn snapshot(&self) -> Result<String> {
		let mut json = serde_json::to_string_pretty(&self.leases)?;
		json.push('\n');
		Ok(json)
	}
}

/// The base address of a stored `/30` lease network, if well-formed.
fn lease_network_base(network: &str) -> Option<u32> {
	match network.parse::<CidrNet>().ok()? {
		CidrNet::V4 { network, prefix: LEASE_PREFIX } => Some(u32::from(network)),
		_ => None,
	}
}

enum JournalMsg {
	Persist(String),
	Flush(mpsc::Sender<()>),
}

/// Loaded allocator + journal writer for one lease file.
struct LeaseState {
	alloc:  Mutex<LeaseAllocator>,
	writer: mpsc::Sender<JournalMsg>,
}

static LEASE_STATES: LazyLock<Mutex<HashMap<PathBuf, Arc<LeaseState>>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

impl LeaseStore {
	const fn new(path: PathBuf) -> Self {
		Self { path }
	}

	fn default() -> Self {
		Self::new(crate::home::state_dir().join("network").join("leases.json"))
	}

	/// Load (once) the allocator + writer thread behind this journal path.
	fn state(&self) -> Result<Arc<LeaseState>> {
		if let Some(state) = LEASE_STATES.lock().get(&self.path) {
			return Ok(Arc::clone(state));
		}
		// Load outside the map lock; racing loaders converge on one entry.
		let alloc = LeaseAllocator::load(&self.path)?;
		let mut states = LEASE_STATES.lock();
		if let Some(state) = states.get(&self.path) {
			return Ok(Arc::clone(state));
		}
		let (writer, rx) = mpsc::channel::<JournalMsg>();
		let path = self.path.clone();
		thread::Builder::new()
			.name("vmon-lease-journal".to_owned())
			.spawn(move || journal_writer(&path, &rx))?;
		let state = Arc::new(LeaseState { alloc: Mutex::new(alloc), writer });
		states.insert(self.path.clone(), Arc::clone(&state));
		Ok(state)
	}

	fn allocate(&self, name: &str, dns: &[&str]) -> Result<GuestConfig> {
		self.allocate_with_tap(name, dns, None)
	}

	fn allocate_with_tap(&self, name: &str, dns: &[&str], tap: Option<&str>) -> Result<GuestConfig> {
		let state = self.state()?;
		let mut alloc = state.alloc.lock();
		let (config, dirty) = alloc.allocate(name, dns, tap)?;
		if dirty {
			let snapshot = alloc.snapshot()?;
			drop(alloc);
			let _ = state.writer.send(JournalMsg::Persist(snapshot));
		}
		Ok(config)
	}

	fn release(&self, name: &str) -> Result<()> {
		let state = self.state()?;
		let mut alloc = state.alloc.lock();
		if alloc.release(name) {
			let snapshot = alloc.snapshot()?;
			drop(alloc);
			let _ = state.writer.send(JournalMsg::Persist(snapshot));
		}
		Ok(())
	}

	fn lease_for(&self, name: &str) -> Option<GuestConfig> {
		let state = self.state().ok()?;
		let alloc = state.alloc.lock();
		let entry = alloc.leases.get(name)?;
		config_from_entry(name, entry, &DEFAULT_DNS).ok()
	}

	/// Block until every queued journal snapshot reached disk.
	fn flush(&self) -> Result<()> {
		let state = self.state()?;
		let (ack, done) = mpsc::channel();
		state
			.writer
			.send(JournalMsg::Flush(ack))
			.map_err(|_| EngineError::engine("lease journal writer exited"))?;
		done
			.recv()
			.map_err(|_| EngineError::engine("lease journal writer exited"))
	}
}

/// Writer-thread loop: coalesce queued snapshots and rewrite the journal.
fn journal_writer(path: &std::path::Path, rx: &mpsc::Receiver<JournalMsg>) {
	while let Ok(message) = rx.recv() {
		let mut latest = None;
		let mut acks = Vec::new();
		let mut queue = Some(message);
		while let Some(message) = queue.take() {
			match message {
				JournalMsg::Persist(snapshot) => latest = Some(snapshot),
				JournalMsg::Flush(ack) => acks.push(ack),
			}
			queue = rx.try_recv().ok();
		}
		if let Some(snapshot) = latest
			&& let Err(error) = write_journal(path, &snapshot)
		{
			tracing::warn!(path = %path.display(), %error, "writing network lease journal failed");
		}
		for ack in acks {
			let _ = ack.send(());
		}
	}
}

/// Rewrite the journal file under an exclusive flock (crash-recovery format).
fn write_journal(path: &std::path::Path, snapshot: &str) -> Result<()> {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent)?;
	}
	let mut file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.truncate(false)
		.open(path)?;
	let _lock = FileLock::exclusive(&file)?;
	file.set_len(0)?;
	file.seek(SeekFrom::Start(0))?;
	file.write_all(snapshot.as_bytes())?;
	Ok(())
}

struct FileLock {
	#[cfg(unix)]
	fd: std::os::fd::RawFd,
}

impl FileLock {
	fn exclusive(file: &File) -> Result<Self> {
		#[cfg(unix)]
		{
			use std::os::fd::AsRawFd;

			let fd = file.as_raw_fd();
			flock(fd, libc::LOCK_EX)?;
			Ok(Self { fd })
		}
		#[cfg(not(unix))]
		{
			let _ = file;
			Ok(Self {})
		}
	}
}

impl Drop for FileLock {
	fn drop(&mut self) {
		#[cfg(unix)]
		{
			let _ = flock(self.fd, libc::LOCK_UN);
		}
	}
}

#[cfg(unix)]
fn flock(fd: std::os::fd::RawFd, operation: libc::c_int) -> Result<()> {
	// SAFETY: flock only observes the valid file descriptor borrowed from File.
	let rc = unsafe { libc::flock(fd, operation) };
	if rc == 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error().into())
	}
}

async fn accept_tunnel(
	listener: TcpListener,
	guest: SocketAddr,
	allowed_nets: Arc<Vec<CidrNet>>,
	connection_tasks: Arc<Mutex<Vec<TokioJoinHandle<()>>>>,
) {
	loop {
		let Ok((client, peer)) = listener.accept().await else {
			break;
		};
		let nets = Arc::clone(&allowed_nets);
		let task = tokio::spawn(async move {
			proxy_tcp(client, peer, guest, &nets).await;
		});
		connection_tasks.lock().push(task);
	}
}

async fn proxy_tcp(
	mut client: TcpStream,
	peer: SocketAddr,
	guest: SocketAddr,
	allowed_nets: &[CidrNet],
) {
	if !ip_allowed(peer.ip(), allowed_nets) {
		return;
	}
	let Ok(mut guest_stream) = TcpStream::connect(guest).await else {
		return;
	};
	let _ = io::copy_bidirectional(&mut client, &mut guest_stream).await;
}

fn parse_cidr_allowlist(allowlist: Option<&[String]>) -> Result<Vec<CidrNet>> {
	let items = allowlist.map_or_else(|| vec!["127.0.0.0/8".to_owned()], <[String]>::to_vec);
	items.iter().map(|cidr| cidr.parse()).collect()
}

fn ip_allowed(addr: IpAddr, nets: &[CidrNet]) -> bool {
	nets.iter().any(|net| net.contains(addr))
}

fn parse_ip(value: &str) -> Result<IpAddr> {
	value
		.parse::<IpAddr>()
		.map_err(|_| EngineError::invalid(format!("invalid IP address {value}")))
}

fn parse_ipv4(value: &str) -> Result<Ipv4Addr> {
	match parse_ip(value)? {
		IpAddr::V4(addr) => Ok(addr),
		IpAddr::V6(_) => Err(EngineError::invalid("sandbox TAP networking supports IPv4 only")),
	}
}

fn config_from_entry(name: &str, entry: &LeaseEntry, dns: &[&str]) -> Result<GuestConfig> {
	let cidr = entry.network.parse::<CidrNet>()?;
	let (network, prefix) = match cidr {
		CidrNet::V4 { network, prefix } => (network, prefix),
		CidrNet::V6 { .. } => {
			return Err(EngineError::invalid("sandbox TAP networking supports IPv4 only"));
		},
	};
	let host_ip = Ipv4Addr::from(u32::from(network) + 1);
	let guest_ip = Ipv4Addr::from(u32::from(network) + 2);
	Ok(GuestConfig {
		tap: entry.tap.clone().unwrap_or_else(|| tap_name(name)),
		host_ip: host_ip.to_string(),
		guest_ip: guest_ip.to_string(),
		prefix,
		network: format!("{network}/{prefix}"),
		dns: dns.iter().map(|value| (*value).to_owned()).collect(),
	})
}

fn ipv4_network(addr: Ipv4Addr, prefix: u8) -> Result<Ipv4Addr> {
	if prefix > 32 {
		return Err(EngineError::invalid(format!("invalid IPv4 prefix {prefix}")));
	}
	Ok(Ipv4Addr::from(u32::from(addr) & ipv4_mask(prefix)))
}

fn ipv4_contains(network: Ipv4Addr, prefix: u8, addr: Ipv4Addr) -> bool {
	(u32::from(addr) & ipv4_mask(prefix)) == u32::from(network)
}

const fn ipv4_mask(prefix: u8) -> u32 {
	if prefix == 0 {
		0
	} else {
		u32::MAX << (32 - prefix)
	}
}

fn ipv6_network(addr: Ipv6Addr, prefix: u8) -> Result<Ipv6Addr> {
	if prefix > 128 {
		return Err(EngineError::invalid(format!("invalid IPv6 prefix {prefix}")));
	}
	let raw = u128::from(addr);
	let mask = if prefix == 0 {
		0
	} else {
		u128::MAX << (128 - prefix)
	};
	Ok(Ipv6Addr::from(raw & mask))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CidrNet {
	V4 { network: Ipv4Addr, prefix: u8 },
	V6 { network: Ipv6Addr, prefix: u8 },
}

impl CidrNet {
	fn contains(&self, addr: IpAddr) -> bool {
		match (self, addr) {
			(Self::V4 { network, prefix }, IpAddr::V4(addr)) => ipv4_contains(*network, *prefix, addr),
			(Self::V6 { network, prefix }, IpAddr::V6(addr)) => {
				let mask = if *prefix == 0 {
					0
				} else {
					u128::MAX << (128 - *prefix)
				};
				(u128::from(addr) & mask) == u128::from(*network)
			},
			_ => false,
		}
	}
}

impl std::str::FromStr for CidrNet {
	type Err = EngineError;

	fn from_str(value: &str) -> Result<Self> {
		let value = value.trim();
		if value.is_empty() {
			return Err(EngineError::invalid("empty CIDR"));
		}
		let (addr_s, prefix_s) = value
			.split_once('/')
			.map_or((value, None), |(addr, prefix)| (addr, Some(prefix)));
		let addr = parse_ip(addr_s)?;
		match addr {
			IpAddr::V4(addr) => {
				let prefix = parse_prefix(prefix_s, 32)?;
				Ok(Self::V4 { network: ipv4_network(addr, prefix)?, prefix })
			},
			IpAddr::V6(addr) => {
				let prefix = parse_prefix(prefix_s, 128)?;
				Ok(Self::V6 { network: ipv6_network(addr, prefix)?, prefix })
			},
		}
	}
}

impl fmt::Display for CidrNet {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::V4 { network, prefix } => write!(f, "{network}/{prefix}"),
			Self::V6 { network, prefix } => write!(f, "{network}/{prefix}"),
		}
	}
}

fn parse_prefix(prefix: Option<&str>, max: u8) -> Result<u8> {
	let Some(prefix) = prefix else {
		return Ok(max);
	};
	let parsed = prefix
		.parse::<u8>()
		.map_err(|_| EngineError::invalid(format!("invalid CIDR prefix {prefix}")))?;
	if parsed > max {
		return Err(EngineError::invalid(format!("invalid CIDR prefix {prefix}")));
	}
	Ok(parsed)
}

#[cfg(any(test, target_os = "linux"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IptablesAction {
	Append,
	Delete,
	Insert,
}

#[cfg(any(test, target_os = "linux"))]
impl IptablesAction {
	const fn flag(self) -> &'static str {
		match self {
			Self::Append => "-A",
			Self::Delete => "-D",
			Self::Insert => "-I",
		}
	}
}

#[cfg(any(test, target_os = "linux"))]
fn masq_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	strings([
		"-t",
		"nat",
		action.flag(),
		"POSTROUTING",
		"-s",
		&lease.network,
		"!",
		"-d",
		&lease.network,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "masq"),
		"-j",
		"MASQUERADE",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn return_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-o",
		&lease.name,
		"-d",
		&guest_cidr,
		"-m",
		"conntrack",
		"--ctstate",
		"ESTABLISHED,RELATED",
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "return"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn unrestricted_egress_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		&lease.name,
		"-s",
		&guest_cidr,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "egress"),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn egress_cidr_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	cidr: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		cidr,
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("egress-{idx}")),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn deny_egress_rule(action: IptablesAction, lease: &TapLease) -> Vec<String> {
	let guest_cidr = lease.guest_cidr();
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		&lease.name,
		"-s",
		&guest_cidr,
		"-m",
		"comment",
		"--comment",
		&comment(&lease.name, "egress-deny"),
		"-j",
		"REJECT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn domain_rule(
	action: IptablesAction,
	name: &str,
	guest_cidr: &str,
	ip: &str,
	idx: usize,
) -> Vec<String> {
	strings([
		action.flag(),
		"FORWARD",
		"-i",
		name,
		"-s",
		guest_cidr,
		"-d",
		&format!("{ip}/32"),
		"-m",
		"comment",
		"--comment",
		&comment(name, &format!("domain-{idx}")),
		"-j",
		"ACCEPT",
	])
}

#[cfg(any(test, target_os = "linux"))]
fn comment(name: &str, tag: &str) -> String {
	format!("{RULE_PREFIX}:{name}:{tag}")
}

#[cfg(any(test, target_os = "linux"))]
fn iptables_check_form(rule: &[String]) -> Result<Vec<String>> {
	let mut converted = rule.to_vec();
	for value in &mut converted {
		if value == "-A" || value == "-D" || value == "-I" {
			"-C".clone_into(value);
			return Ok(converted);
		}
	}
	Err(EngineError::invalid(format!("iptables rule missing -A/-D/-I: {}", rule.join(" "))))
}

#[cfg(any(test, target_os = "linux"))]
fn strings<const N: usize>(items: [&str; N]) -> Vec<String> {
	items.into_iter().map(str::to_owned).collect()
}

fn clean_domains(domains: Option<&[String]>) -> Vec<String> {
	domains
		.unwrap_or(&[])
		.iter()
		.map(|domain| domain.trim())
		.filter(|domain| !domain.is_empty())
		.map(str::to_owned)
		.collect()
}

#[cfg(target_os = "linux")]
fn start_domain_refresher(
	name: &str,
	target: RefreshTarget,
	domains: Vec<String>,
	rules: BTreeMap<String, usize>,
	next_idx: usize,
	trusted: Vec<String>,
) -> Result<()> {
	let refresher = DomainRefresher::new(name, target, domains, rules, next_idx, trusted).start()?;
	REFRESHERS.lock().insert(name.to_owned(), refresher);
	Ok(())
}

#[cfg(target_os = "linux")]
fn stop_domain_refresher(name: &str) -> BTreeMap<String, usize> {
	let refresher = REFRESHERS.lock().remove(name);
	refresher.map_or_else(BTreeMap::new, DomainRefresher::stop)
}

/// Where a domain refresher applies resolved addresses.
#[cfg(any(test, target_os = "linux"))]
#[derive(Clone, Debug)]
enum RefreshTarget {
	/// Per-IP iptables FORWARD rules (non-pooled TAPs).
	Iptables { guest_cidr: String },
	/// Elements of a pooled slot's nftables egress set, updated as one
	/// batched `nft -f -` invocation per refresh cycle.
	#[allow(dead_code, reason = "reserved for egress policy expansion")]
	NftSet { set: String },
}

#[cfg(any(test, target_os = "linux"))]
struct DomainRefresher {
	name:     String,
	target:   RefreshTarget,
	domains:  Vec<String>,
	trusted:  Vec<String>,
	rules:    Arc<Mutex<BTreeMap<String, usize>>>,
	next_idx: Arc<Mutex<usize>>,
	stop_tx:  mpsc::Sender<()>,
	stop_rx:  Option<mpsc::Receiver<()>>,
	thread:   Option<thread::JoinHandle<()>>,
}

#[cfg(any(test, target_os = "linux"))]
impl DomainRefresher {
	const INTERVAL: Duration = Duration::from_mins(1);

	fn new(
		name: &str,
		target: RefreshTarget,
		domains: Vec<String>,
		rules: BTreeMap<String, usize>,
		next_idx: usize,
		trusted: Vec<String>,
	) -> Self {
		let (stop_tx, stop_rx) = mpsc::channel();
		Self {
			name: name.to_owned(),
			target,
			domains,
			trusted,
			rules: Arc::new(Mutex::new(rules)),
			next_idx: Arc::new(Mutex::new(next_idx)),
			stop_tx,
			stop_rx: Some(stop_rx),
			thread: None,
		}
	}

	fn start(mut self) -> Result<Self> {
		let Some(stop_rx) = self.stop_rx.take() else {
			return Err(EngineError::engine("domain refresher already started"));
		};
		let name = self.name.clone();
		let target = self.target.clone();
		let domains = self.domains.clone();
		let trusted = self.trusted.clone();
		let rules = Arc::clone(&self.rules);
		let next_idx = Arc::clone(&self.next_idx);
		let thread_name = format!("vmon-egress-dns-{name}");
		let thread = thread::Builder::new().name(thread_name).spawn(move || {
			#[cfg(not(target_os = "linux"))]
			let _ = &name;
			let Ok(mut policy) = policy::EgressPolicy::new(policy::SystemResolver, &domains, &trusted)
			else {
				return;
			};
			loop {
				match stop_rx.recv_timeout(Self::INTERVAL) {
					Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
					Err(mpsc::RecvTimeoutError::Timeout) => {},
				}
				let now = START_INSTANT.elapsed();
				let mut current_ips = BTreeSet::new();
				let mut refresh_failed = false;
				for domain in &domains {
					if let Ok(ips) = policy.resolve_allowed(domain, now) {
						for ip in ips {
							current_ips.insert(ip.to_string());
						}
					} else {
						refresh_failed = true;
						break;
					}
				}

				if refresh_failed {
					// Fail closed: drop every previously allowed address.
					Self::purge(&name, &target, &rules.lock().clone());
					rules.lock().clear();
					break;
				}

				let mut to_add = Vec::new();
				let mut to_remove = Vec::new();
				{
					let rules_guard = rules.lock();
					for ip in rules_guard.keys() {
						if !current_ips.contains(ip) {
							to_remove.push(ip.clone());
						}
					}
					for ip in &current_ips {
						if !rules_guard.contains_key(ip) {
							to_add.push(ip.clone());
						}
					}
				}

				match &target {
					RefreshTarget::NftSet { set } => {
						// One batched nft invocation covers adds and removes.
						if to_add.is_empty() && to_remove.is_empty() {
							continue;
						}
						#[cfg(target_os = "linux")]
						let res = run_nft(&slots::update_set_payload(set, &to_add, &to_remove));
						#[cfg(not(target_os = "linux"))]
						let res: Result<()> = {
							let _ = set;
							Ok(())
						};
						if res.is_ok() {
							let mut rules_guard = rules.lock();
							for ip in to_add {
								let idx = {
									let mut next_idx_guard = next_idx.lock();
									let current = *next_idx_guard;
									*next_idx_guard += 1;
									current
								};
								rules_guard.insert(ip, idx);
							}
							for ip in &to_remove {
								rules_guard.remove(ip);
							}
						}
					},
					RefreshTarget::Iptables { guest_cidr } => {
						#[cfg(not(target_os = "linux"))]
						let _ = guest_cidr;
						// 1. Add refreshed/new rules FIRST.
						for ip in to_add {
							let idx = {
								let mut next_idx_guard = next_idx.lock();
								let current = *next_idx_guard;
								*next_idx_guard += 1;
								current
							};
							#[cfg(target_os = "linux")]
							let res = iptables_ensure(&domain_rule(
								IptablesAction::Insert,
								&name,
								guest_cidr,
								&ip,
								idx,
							));
							#[cfg(not(target_os = "linux"))]
							let res: Result<()> = Ok(());
							if res.is_ok() {
								rules.lock().insert(ip.clone(), idx);
							}
						}
						// 2. Delete stale rules SECOND.
						for ip in to_remove {
							let idx = rules.lock().get(&ip).copied();
							if let Some(idx) = idx {
								#[cfg(not(target_os = "linux"))]
								let _ = idx;
								#[cfg(target_os = "linux")]
								let res = iptables_delete(&domain_rule(
									IptablesAction::Delete,
									&name,
									guest_cidr,
									&ip,
									idx,
								));
								#[cfg(not(target_os = "linux"))]
								let res: Result<()> = Ok(());
								if res.is_ok() {
									rules.lock().remove(&ip);
								}
							}
						}
					},
				}
			}
		})?;
		self.thread = Some(thread);
		Ok(self)
	}

	/// Remove every applied address for `target` (fail-closed cleanup).
	#[cfg_attr(
		not(target_os = "linux"),
		allow(clippy::missing_const_for_fn, reason = "Linux performs runtime firewall cleanup.")
	)]
	fn purge(name: &str, target: &RefreshTarget, applied: &BTreeMap<String, usize>) {
		#[cfg(not(target_os = "linux"))]
		let _ = (name, target, applied);
		#[cfg(target_os = "linux")]
		match target {
			RefreshTarget::NftSet { set } => {
				let remove = applied.keys().cloned().collect::<Vec<_>>();
				if !remove.is_empty() {
					let _ = run_nft(&slots::update_set_payload(set, &[], &remove));
				}
			},
			RefreshTarget::Iptables { guest_cidr } => {
				for (ip, idx) in applied {
					let _ =
						iptables_delete(&domain_rule(IptablesAction::Delete, name, guest_cidr, ip, *idx));
				}
			},
		}
	}

	fn stop(mut self) -> BTreeMap<String, usize> {
		let _ = self.stop_tx.send(());
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
		let rules = self.rules.lock().clone();
		// Slot sets are flushed by the claim/recycle payloads that follow a
		// stop; only iptables targets need exact-delete cleanup here.
		if matches!(self.target, RefreshTarget::Iptables { .. }) {
			Self::purge(&self.name, &self.target, &rules);
		}
		rules
	}
}

#[cfg(target_os = "linux")]
fn disable_ipv6(name: &str) -> Result<()> {
	let path = PathBuf::from("/proc/sys/net/ipv6/conf")
		.join(name)
		.join("disable_ipv6");
	fs::write(&path, "1\n").map_err(|error| {
		EngineError::engine(format!("disabling IPv6 on sandbox TAP {name}: {error}"))
	})
}

#[cfg(target_os = "linux")]
fn enable_ip_forward() -> Result<()> {
	let current = fs::read_to_string(IP_FORWARD)
		.map_err(|err| EngineError::engine(format!("enabling IPv4 forwarding: {err}")))?;
	if current.trim() == "1" {
		return Ok(());
	}
	fs::write(IP_FORWARD, "1\n")
		.map_err(|err| EngineError::engine(format!("enabling IPv4 forwarding: {err}")))
}

#[cfg(target_os = "linux")]
fn ip_link_exists(name: &str) -> Result<bool> {
	Ok(run(&argv(["ip", "link", "show", "dev", name]), false)?.success())
}

#[cfg(target_os = "linux")]
fn iptables_ensure(rule: &[String]) -> Result<()> {
	let check_rule = iptables_check_form(rule)?;
	if run_iptables(&check_rule, false)?.success() {
		return Ok(());
	}
	run_iptables(rule, true)?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn iptables_delete(rule: &[String]) -> Result<()> {
	while run_iptables(rule, false)?.success() {}
	Ok(())
}

#[cfg(target_os = "linux")]
fn run_iptables(rule: &[String], check: bool) -> Result<RunOutput> {
	// Concurrent iptables invocations contend on the xtables lock and fail
	// with exit 4; -w makes every ensure/check/delete wait instead.
	let mut command = Vec::with_capacity(rule.len() + 3);
	command.push("iptables".to_owned());
	command.push("-w".to_owned());
	command.push("5".to_owned());
	command.extend(rule.iter().cloned());
	run(&command, check)
}

#[cfg(target_os = "linux")]
fn argv<const N: usize>(items: [&str; N]) -> Vec<String> {
	items.into_iter().map(str::to_owned).collect()
}

#[cfg(target_os = "linux")]
struct RunOutput {
	code: i32,
}

#[cfg(target_os = "linux")]
impl RunOutput {
	const fn success(&self) -> bool {
		self.code == 0
	}
}

#[cfg(target_os = "linux")]
fn run(argv: &[String], check: bool) -> Result<RunOutput> {
	let Some(program) = argv.first() else {
		return Err(EngineError::engine("empty command"));
	};
	let output = Command::new(program)
		.args(&argv[1..])
		.output()
		.map_err(|err| {
			if err.kind() == std::io::ErrorKind::NotFound {
				EngineError::engine(format!("required command not found: {program}"))
			} else {
				EngineError::engine(err.to_string())
			}
		})?;
	let code = output.status.code().unwrap_or(-1);
	if check && !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let detail = if stderr.trim().is_empty() {
			stdout.trim()
		} else {
			stderr.trim()
		};
		return Err(EngineError::engine(format!("{} failed: {detail}", argv.join(" "))));
	}
	Ok(RunOutput { code })
}

#[cfg(target_os = "linux")]
fn system_vpc_ensure_bridge(bridge: &str, gateway: &str, prefix: u8) -> Result<()> {
	require_net_admin()?;
	if !run(&argv(["ip", "link", "show", "dev", bridge]), false)?.success() {
		run(&argv(["ip", "link", "add", "name", bridge, "type", "bridge"]), true)?;
	}
	run(
		&[
			"ip".to_owned(),
			"address".to_owned(),
			"replace".to_owned(),
			format!("{gateway}/{prefix}"),
			"dev".to_owned(),
			bridge.to_owned(),
		],
		true,
	)?;
	run(&argv(["ip", "link", "set", "dev", bridge, "up"]), true)?;
	Ok(())
}

#[cfg(not(target_os = "linux"))]
fn system_vpc_ensure_bridge(_bridge: &str, _gateway: &str, _prefix: u8) -> Result<()> {
	Err(EngineError::invalid("VPCs require a Linux host"))
}

#[cfg(target_os = "linux")]
fn system_vpc_attach_tap(bridge: &str, tap: &str) -> Result<()> {
	require_net_admin()?;
	if !run(&argv(["ip", "link", "show", "dev", tap]), false)?.success() {
		let uid = {
			// SAFETY: geteuid has no preconditions and reads process credentials.
			unsafe { libc::geteuid() }.to_string()
		};
		run(
			&[
				"ip".to_owned(),
				"tuntap".to_owned(),
				"add".to_owned(),
				"dev".to_owned(),
				tap.to_owned(),
				"mode".to_owned(),
				"tap".to_owned(),
				"user".to_owned(),
				uid,
			],
			true,
		)?;
	}
	run(
		&[
			"ip".to_owned(),
			"link".to_owned(),
			"set".to_owned(),
			"dev".to_owned(),
			tap.to_owned(),
			"master".to_owned(),
			bridge.to_owned(),
		],
		true,
	)?;
	run(&argv(["ip", "link", "set", "dev", tap, "up"]), true)?;
	Ok(())
}

#[cfg(not(target_os = "linux"))]
fn system_vpc_attach_tap(_bridge: &str, _tap: &str) -> Result<()> {
	Err(EngineError::invalid("VPCs require a Linux host"))
}

#[cfg(target_os = "linux")]
fn system_vpc_detach_tap(tap: &str) -> Result<()> {
	require_net_admin()?;
	if run(&argv(["ip", "link", "show", "dev", tap]), false)?.success() {
		run(&argv(["ip", "link", "delete", "dev", tap]), true)?;
	}
	Ok(())
}

#[cfg(not(target_os = "linux"))]
fn system_vpc_detach_tap(_tap: &str) -> Result<()> {
	Err(EngineError::invalid("VPCs require a Linux host"))
}

#[cfg(target_os = "linux")]
fn system_vpc_delete_bridge(bridge: &str) -> Result<()> {
	require_net_admin()?;
	if run(&argv(["ip", "link", "show", "dev", bridge]), false)?.success() {
		run(&argv(["ip", "link", "delete", "dev", bridge, "type", "bridge"]), true)?;
	}
	Ok(())
}

#[cfg(not(target_os = "linux"))]
fn system_vpc_delete_bridge(_bridge: &str) -> Result<()> {
	Err(EngineError::invalid("VPCs require a Linux host"))
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;

	/// Serializes tests that touch the global slot pool / broker stub.
	static TEST_ENV: Mutex<()> = Mutex::new(());

	/// Installs a broker stub that counts calls and records request kinds;
	/// uninstalls on drop even when the test panics.
	struct StubGuard {
		calls: Arc<AtomicUsize>,
		kinds: Arc<Mutex<Vec<&'static str>>>,
	}

	impl StubGuard {
		fn install() -> Self {
			let calls = Arc::new(AtomicUsize::new(0));
			let kinds = Arc::new(Mutex::new(Vec::new()));
			let stub_calls = Arc::clone(&calls);
			let stub_kinds = Arc::clone(&kinds);
			*BROKER_STUB.lock() = Some(Box::new(move |request| {
				stub_calls.fetch_add(1, Ordering::SeqCst);
				stub_kinds.lock().push(match request {
					broker::Request::Setup { .. } => "setup",
					broker::Request::AllowCredential { .. } => "allow-credential",
					broker::Request::Teardown { .. } => "teardown",
					broker::Request::PreallocateSlots { .. } => "preallocate",
					broker::Request::ClaimSlot { .. } => "claim",
					broker::Request::RecycleSlot { .. } => "recycle",
				});
				Ok(None)
			}));
			Self { calls, kinds }
		}

		fn calls(&self) -> usize {
			self.calls.load(Ordering::SeqCst)
		}
	}

	impl Drop for StubGuard {
		fn drop(&mut self) {
			*BROKER_STUB.lock() = None;
			slots::pool_reset();
		}
	}

	fn test_slot(index: u32) -> slots::SlotSpec {
		let base = u32::from(POOL_BASE) + index * LEASE_STEP;
		slots::SlotSpec {
			index,
			name: slots::slot_tap_name(index),
			guest_ip: Ipv4Addr::from(base + 2).to_string(),
			host_ip: Ipv4Addr::from(base + 1).to_string(),
			prefix: LEASE_PREFIX,
		}
	}

	#[test]
	fn slot_claim_recycle_reuses_tap_without_broker_requests() {
		let _env = TEST_ENV.lock();
		slots::pool_reset();
		let stub = StubGuard::install();
		slots::pool_install(vec![test_slot(0)]);

		// Default-policy claim: zero broker round trips end to end.
		let config = allocate_guest_config("vmon-nettest-slot-a").unwrap();
		assert_eq!(config.tap, slots::slot_tap_name(0));
		let lease =
			setup_tap(&config.tap, &config.guest_ip, &config.host_ip, config.prefix, None, None, None)
				.unwrap();
		assert_eq!(lease.name, config.tap);
		assert_eq!(lease_for("vmon-nettest-slot-a").unwrap().tap, config.tap);
		teardown_tap(
			&config.tap,
			Some(&config.guest_ip),
			Some(&config.host_ip),
			config.prefix,
			None,
			None,
		)
		.unwrap();
		release_guest_config("vmon-nettest-slot-a").unwrap();
		assert_eq!(stub.calls(), 0);

		// Recycled slot serves the next sandbox with the same TAP and lease.
		let reused = allocate_guest_config("vmon-nettest-slot-b").unwrap();
		assert_eq!(reused.tap, config.tap);
		assert_eq!(reused.guest_ip, config.guest_ip);
		assert_eq!(reused.host_ip, config.host_ip);
		assert_eq!(stub.calls(), 0);

		// A custom egress policy costs exactly one batched broker request,
		// and recycling it exactly one more.
		let allow = vec!["203.0.113.0/24".to_owned()];
		setup_tap(
			&reused.tap,
			&reused.guest_ip,
			&reused.host_ip,
			reused.prefix,
			Some(&allow),
			None,
			None,
		)
		.unwrap();
		assert_eq!(stub.calls(), 1);
		teardown_tap(
			&reused.tap,
			Some(&reused.guest_ip),
			Some(&reused.host_ip),
			reused.prefix,
			Some(&allow),
			None,
		)
		.unwrap();
		release_guest_config("vmon-nettest-slot-b").unwrap();
		assert_eq!(stub.calls(), 2);
		assert_eq!(*stub.kinds.lock(), vec!["claim", "recycle"]);
	}

	#[test]
	fn block_network_path_leaves_net_state_untouched() {
		let _env = TEST_ENV.lock();
		slots::pool_reset();
		let stub = StubGuard::install();
		slots::pool_install(vec![test_slot(0)]);
		let free_before = slots::pool_free_len();

		// The engine's spawn path performs NO net calls for block_network
		// sandboxes; teardown only consults `lease_for`, which must not
		// allocate, claim, or reach the broker.
		assert!(lease_for("vmon-nettest-blocked").is_none());

		assert_eq!(slots::pool_free_len(), free_before);
		assert_eq!(stub.calls(), 0);
	}

	#[test]
	fn in_memory_allocator_survives_journal_reload() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("network").join("leases.json");
		let store = LeaseStore::new(path.clone());

		let alpha = store.allocate("alpha", &DEFAULT_DNS).unwrap();
		let beta = store.allocate("beta", &DEFAULT_DNS).unwrap();
		assert_eq!(store.allocate("alpha", &DEFAULT_DNS).unwrap(), alpha);
		store.release("beta").unwrap();
		store.release("beta").unwrap(); // releasing twice is a no-op
		store.flush().unwrap();

		// Reload the journal from disk as a fresh allocator (daemon restart).
		let mut reloaded = LeaseAllocator::load(&path).unwrap();
		assert!(reloaded.leases.contains_key("alpha"));
		assert!(!reloaded.leases.contains_key("beta"));

		// Idempotent re-allocate: alpha keeps its block across the reload.
		let (alpha_again, dirty) = reloaded.allocate("alpha", &DEFAULT_DNS, None).unwrap();
		assert!(!dirty);
		assert_eq!(alpha_again, alpha);

		// Beta's freed block is the lowest available again.
		let (gamma, dirty) = reloaded.allocate("gamma", &DEFAULT_DNS, None).unwrap();
		assert!(dirty);
		assert_eq!(gamma.network, beta.network);
	}

	#[test]
	fn lease_allocation_persistence_release_roundtrip() {
		let dir = tempfile::tempdir().unwrap();
		let store = LeaseStore::new(dir.path().join("network").join("leases.json"));

		let first = store.allocate("alpha", &DEFAULT_DNS).unwrap();
		assert_eq!(first.network, "172.20.0.0/30");
		assert_eq!(first.host_ip, "172.20.0.1");
		assert_eq!(first.guest_ip, "172.20.0.2");
		assert_eq!(first.prefix, 30);
		assert_eq!(first.tap, tap_name("alpha"));

		let second = store.allocate("beta", &DEFAULT_DNS).unwrap();
		assert_eq!(second.network, "172.20.0.4/30");
		assert_eq!(store.allocate("alpha", &DEFAULT_DNS).unwrap(), first);
		assert_eq!(store.lease_for("alpha").as_ref(), Some(&first));

		store.flush().unwrap();
		let raw = fs::read_to_string(dir.path().join("network").join("leases.json")).unwrap();
		let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
		assert_eq!(value["alpha"]["network"], "172.20.0.0/30");
		assert_eq!(value["alpha"]["tap"], tap_name("alpha"));

		store.release("alpha").unwrap();
		assert_eq!(store.lease_for("alpha"), None);
		let reused = store.allocate("gamma", &DEFAULT_DNS).unwrap();
		assert_eq!(reused.network, "172.20.0.0/30");
		assert_eq!(store.lease_for("beta"), Some(second));
	}

	#[test]
	fn tap_name_derivation_matches_python() {
		assert_eq!(tap_name("sandbox"), "tv9ed037b849");
		assert_eq!(tap_name("vm-1"), "tv764646a060");
	}

	#[test]
	fn cidr_allowlist_matches_boundaries() {
		let cidrs =
			["192.168.1.0/24".to_owned(), "10.0.0.5/32".to_owned(), "2001:db8::/126".to_owned()];
		let nets = parse_cidr_allowlist(Some(&cidrs)).unwrap();
		assert!(ip_allowed("192.168.1.0".parse().unwrap(), &nets));
		assert!(ip_allowed("192.168.1.255".parse().unwrap(), &nets));
		assert!(!ip_allowed("192.168.2.0".parse().unwrap(), &nets));
		assert!(ip_allowed("10.0.0.5".parse().unwrap(), &nets));
		assert!(!ip_allowed("10.0.0.6".parse().unwrap(), &nets));
		assert!(ip_allowed("2001:db8::3".parse().unwrap(), &nets));
		assert!(!ip_allowed("2001:db8::4".parse().unwrap(), &nets));

		let default = parse_cidr_allowlist(None).unwrap();
		assert!(ip_allowed("127.255.255.255".parse().unwrap(), &default));
		assert!(!ip_allowed("128.0.0.1".parse().unwrap(), &default));
	}

	#[test]
	fn iptables_rule_argv_is_symmetric() {
		let allow = vec!["0.0.0.0/0".to_owned()];
		let lease = TapLease::new("tvabc123", "172.20.0.2", "172.20.0.1", 30, Some(&allow)).unwrap();

		let masq_add = masq_rule(IptablesAction::Append, &lease);
		assert_eq!(masq_add, vec![
			"-t",
			"nat",
			"-A",
			"POSTROUTING",
			"-s",
			"172.20.0.0/30",
			"!",
			"-d",
			"172.20.0.0/30",
			"-m",
			"comment",
			"--comment",
			"vmon:tvabc123:masq",
			"-j",
			"MASQUERADE",
		]);
		let mut masq_delete = masq_add.clone();
		"-D".clone_into(&mut masq_delete[2]);
		assert_eq!(masq_rule(IptablesAction::Delete, &lease), masq_delete);

		let mut return_delete = return_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut return_delete[0]);
		assert_eq!(return_rule(IptablesAction::Delete, &lease), return_delete);

		let mut unrestricted_delete = unrestricted_egress_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut unrestricted_delete[0]);
		assert_eq!(unrestricted_egress_rule(IptablesAction::Delete, &lease), unrestricted_delete);

		let mut cidr_delete =
			egress_cidr_rule(IptablesAction::Append, &lease.name, &lease.guest_cidr(), "0.0.0.0/0", 0);
		"-D".clone_into(&mut cidr_delete[0]);
		assert_eq!(
			egress_cidr_rule(IptablesAction::Delete, &lease.name, &lease.guest_cidr(), "0.0.0.0/0", 0,),
			cidr_delete
		);

		let mut deny_delete = deny_egress_rule(IptablesAction::Append, &lease);
		"-D".clone_into(&mut deny_delete[0]);
		assert_eq!(deny_egress_rule(IptablesAction::Delete, &lease), deny_delete);

		let check = iptables_check_form(&masq_add).unwrap();
		let mut expected_check = masq_add;
		"-C".clone_into(&mut expected_check[2]);
		assert_eq!(check, expected_check);

		let domain_insert =
			domain_rule(IptablesAction::Insert, &lease.name, &lease.guest_cidr(), "203.0.113.9", 0);
		assert_eq!(domain_insert[0], "-I");
		assert_eq!(iptables_check_form(&domain_insert).unwrap()[0], "-C");
		let mut domain_delete = domain_insert;
		"-D".clone_into(&mut domain_delete[0]);
		assert_eq!(
			domain_rule(IptablesAction::Delete, &lease.name, &lease.guest_cidr(), "203.0.113.9", 0,),
			domain_delete
		);
	}

	#[test]
	fn test_domain_refresher_rule_indexing_state() {
		let rules = BTreeMap::from([("1.1.1.1".to_owned(), 0), ("8.8.8.8".to_owned(), 1)]);
		let next_idx = 2;
		let refresher = DomainRefresher::new(
			"test-sb",
			RefreshTarget::Iptables { guest_cidr: "10.0.2.15/32".to_owned() },
			vec!["api.example.com".to_owned()],
			rules,
			next_idx,
			vec![],
		)
		.start()
		.unwrap();
		assert_eq!(refresher.rules.lock().get("1.1.1.1"), Some(&0));
		assert_eq!(refresher.rules.lock().get("8.8.8.8"), Some(&1));
		assert_eq!(*refresher.next_idx.lock(), 2);

		let stopped_rules = refresher.stop();
		assert_eq!(stopped_rules.get("1.1.1.1"), Some(&0));
		assert_eq!(stopped_rules.get("8.8.8.8"), Some(&1));
	}

	#[test]
	fn test_protected_deny_rule_argv() {
		let rule =
			protected_deny_rule(IptablesAction::Append, "test-sb", "10.0.2.15/32", "127.0.0.0/8", 3);
		assert_eq!(rule, vec![
			"-A".to_owned(),
			"FORWARD".to_owned(),
			"-i".to_owned(),
			"test-sb".to_owned(),
			"-s".to_owned(),
			"10.0.2.15/32".to_owned(),
			"-d".to_owned(),
			"127.0.0.0/8".to_owned(),
			"-m".to_owned(),
			"comment".to_owned(),
			"--comment".to_owned(),
			"vmon:test-sb:deny-range-3".to_owned(),
			"-j".to_owned(),
			"REJECT".to_owned(),
		]);
	}

	#[test]
	fn test_domain_rule_argv() {
		let rule = domain_rule(IptablesAction::Insert, "test-sb", "10.0.2.15/32", "1.1.1.1", 5);
		assert_eq!(rule, vec![
			"-I".to_owned(),
			"FORWARD".to_owned(),
			"-i".to_owned(),
			"test-sb".to_owned(),
			"-s".to_owned(),
			"10.0.2.15/32".to_owned(),
			"-d".to_owned(),
			"1.1.1.1/32".to_owned(),
			"-m".to_owned(),
			"comment".to_owned(),
			"--comment".to_owned(),
			"vmon:test-sb:domain-5".to_owned(),
			"-j".to_owned(),
			"ACCEPT".to_owned(),
		]);
	}
	#[test]
	fn proc_net_dev_parser_skips_loopback_and_malformed_rows() {
		let counters = parse_proc_net_dev(
			"Inter-|   Receive                                                |  Transmit\nface \
			 |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo \
			 colls carrier compressed\nlo: 999 0 0 0 0 0 0 0 999 0 0 0 0 0 0 0\neth0: \
			 18446744073709551615 0 0 0 0 0 0 0 7 0 0 0 0 0 0 0\nmalformed: 3 0 0\nwlan0: 1 0 0 0 0 \
			 0 0 0 18446744073709551615 0 0 0 0 0 0 0\nbad-value: x 0 0 0 0 0 0 0 3 0 0 0 0 0 0 0\n",
		);

		assert_eq!(counters, (u64::MAX, u64::MAX));
	}
	struct FakeVpcBridge {
		calls: Mutex<Vec<String>>,
	}

	impl VpcBridge for FakeVpcBridge {
		fn ensure_bridge(&self, bridge: &str, gateway: &str, prefix: u8) -> Result<()> {
			self
				.calls
				.lock()
				.push(format!("bridge:{bridge}:{gateway}/{prefix}"));
			Ok(())
		}

		fn attach_tap(&self, bridge: &str, tap: &str) -> Result<()> {
			self.calls.lock().push(format!("attach:{bridge}:{tap}"));
			Ok(())
		}

		fn detach_tap(&self, bridge: &str, tap: &str) -> Result<()> {
			self.calls.lock().push(format!("detach:{bridge}:{tap}"));
			Ok(())
		}

		fn delete_bridge(&self, bridge: &str) -> Result<()> {
			self.calls.lock().push(format!("delete:{bridge}"));
			Ok(())
		}
	}

	#[tokio::test]
	async fn vpc_setup_uses_isolated_bridge_seam() {
		let bridge = FakeVpcBridge { calls: Mutex::new(Vec::new()) };
		let _network = setup_vpc_network_with_bridge(
			&bridge,
			"sandbox",
			"vpc-1234abcd",
			"10.88.0.2",
			"10.88.0.1",
			24,
			&[],
			None,
		)
		.await
		.unwrap();
		assert_eq!(bridge.calls.lock().clone(), vec![
			"bridge:vmonbr-1234abcd:10.88.0.1/24".to_owned(),
			format!("attach:vmonbr-1234abcd:{}", tap_name("sandbox")),
		]);
	}
}
