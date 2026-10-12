//! Sandbox networking (ARCHITECTURE.md §8.4).
//!
//! macOS uses user-mode NAT inside the VMM; Linux uses one TAP per sandbox
//! with a /30 lease from the pool and NAT via iptables. Guest addressing is
//! pushed with GC4 `net_config` after every boot, fork, and resume (guests
//! run no DHCP client).

#[cfg(target_os = "linux")]
mod conntrack;
mod lease;
#[cfg(target_os = "linux")]
mod tap;

use std::{
	net::Ipv4Addr,
	sync::{
		Arc, OnceLock, Weak,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
};

use anyhow::{anyhow, bail};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

pub use self::lease::{Block, LEASE_PREFIX, LeaseAllocator};
use crate::{egress::Egress, gc4::NetConfig, vm::VmNet};

/// Requested networking mode (`--net`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NetMode {
	/// libslirp NAT inside the VMM (macOS, Windows).
	User,
	/// Host TAP + NAT (Linux, needs `CAP_NET_ADMIN`).
	Tap,
	/// `user` on macOS, `tap` on Linux.
	Auto,
}

/// Public resolvers handed to TAP guests (the host IP is firewalled off).
const TAP_DNS: [&str; 2] = ["8.8.8.8", "1.1.1.1"];
/// Upper bound of ready TAP slots ([`NetManager::prewarm`]).
pub const MAX_TAP_SLOTS: usize = 32;
/// Released TAPs return to the ready pool until it holds this multiple of
/// its target size ([`NetManager::prewarm`]); the refill stops at 1×.
const RECYCLE_HEADROOM: usize = 2;

/// A TAP lease; persisted in sandbox `meta.json` so restart recovery can
/// re-reserve it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TapLease {
	pub index:    u32,
	pub tap:      String,
	pub network:  Ipv4Addr,
	pub host_ip:  Ipv4Addr,
	pub guest_ip: Ipv4Addr,
	pub prefix:   u8,
	pub mac:      String,
}

impl TapLease {
	fn from_block(block: Block) -> Self {
		let [_, top, hi, lo] = (block.index + 1).to_be_bytes();
		Self {
			index:    block.index,
			tap:      format!("vmon{}", block.index),
			network:  block.network,
			host_ip:  block.host,
			guest_ip: block.guest,
			prefix:   LEASE_PREFIX,
			mac:      format!("02:76:6d:{top:02x}:{hi:02x}:{lo:02x}"),
		}
	}

	/// Connection mark of the guest's flows (`CONNMARK`, unique per lease and
	/// never 0), used to flush their conntrack entries.
	#[cfg_attr(
		not(target_os = "linux"),
		expect(dead_code, reason = "conntrack flushing is Linux-only")
	)]
	const fn conntrack_mark(&self) -> u32 {
		self.index + 1
	}
}

/// How this node networks its VMs, resolved at start.
#[derive(Debug)]
enum Backend {
	User,
	Tap {
		pool:      Mutex<LeaseAllocator>,
		owner_uid: Option<u32>,
		/// Installs and removes TAP egress hooks ([`NetManager::set_egress`]).
		egress:    OnceLock<Egress>,
		/// Set-up, unused TAPs (address, link, NAT rules, connection mark,
		/// egress hooks with closed policy chains), handed out by `acquire`
		/// and refilled in the background.
		ready:     Mutex<Vec<TapLease>>,
		/// Target size of `ready` (0 until [`NetManager::prewarm`]).
		slots:     AtomicUsize,
		/// A background refill is running.
		refilling: AtomicBool,
	},
	/// No usable backend; every acquire fails with this reason.
	Unavailable(String),
}

/// Allocates per-VM network attachments.
#[derive(Debug)]
pub struct NetManager {
	backend: Backend,
}

/// Network attachment of one VM; releasing it recycles the TAP into the ready
/// pool or tears it down; dropping it tears it down.
#[derive(Debug)]
pub struct NetLease {
	tap:        Option<TapLease>,
	manager:    Weak<NetManager>,
	/// The TAP may return to the ready pool: it was set up by this process
	/// with egress hooks (not adopted, not a build VM's).
	recyclable: bool,
}

impl NetManager {
	/// Resolves `mode` for this host. `pool` is the TAP CIDR; `owner_uid` the
	/// uid privilege-dropped VMMs run as (TAP ownership).
	pub fn new(mode: NetMode, pool: &str, owner_uid: Option<u32>) -> anyhow::Result<Arc<Self>> {
		let linux = cfg!(target_os = "linux");
		let backend = match (mode, linux) {
			(NetMode::User, true) => {
				bail!("--net user is not supported on Linux (the VMM needs a TAP); use --net tap")
			},
			(NetMode::User | NetMode::Auto, false) => Backend::User,
			(NetMode::Tap, false) => bail!("--net tap is only supported on Linux; use --net user"),
			(NetMode::Tap | NetMode::Auto, true) => {
				let pool = LeaseAllocator::new(pool)?;
				if has_net_admin() {
					Backend::Tap {
						pool: Mutex::new(pool),
						owner_uid,
						egress: OnceLock::new(),
						ready: Mutex::new(Vec::new()),
						slots: AtomicUsize::new(0),
						refilling: AtomicBool::new(false),
					}
				} else if mode == NetMode::Tap {
					bail!("--net tap needs root or CAP_NET_ADMIN to create TAP devices and NAT rules");
				} else {
					tracing::warn!(
						"no CAP_NET_ADMIN: this node cannot network sandboxes and will reject creates"
					);
					Backend::Unavailable(
						"node lacks CAP_NET_ADMIN for TAP networking (run as root or grant \
						 CAP_NET_ADMIN)"
							.into(),
					)
				}
			},
		};
		Ok(Arc::new(Self { backend }))
	}

	/// Capacity features advertised in gossip (`net-user` / `net-tap`).
	pub fn features(&self) -> Vec<String> {
		match self.backend {
			Backend::User => vec!["net-user".into()],
			Backend::Tap { .. } => vec!["net-tap".into()],
			Backend::Unavailable(_) => Vec::new(),
		}
	}

	/// Hands over the node's egress enforcement: TAPs set up from now on get
	/// its hooks with closed policy chains ([`Egress::prepare_tap`]), and TAP
	/// teardown removes them. Call once, right after egress started.
	pub fn set_egress(&self, egress: Egress) {
		if let Backend::Tap { egress: slot, .. } = &self.backend {
			let _ = slot.set(egress);
		}
	}

	fn egress(&self) -> Option<&Egress> {
		match &self.backend {
			Backend::Tap { egress, .. } => egress.get(),
			_ => None,
		}
	}

	/// Allocates and plumbs a network attachment for a sandbox VM; on Linux a
	/// ready TAP with egress hooks whose policy chains stay closed until
	/// [`Egress::attach`].
	pub async fn acquire(self: &Arc<Self>) -> anyhow::Result<NetLease> {
		match &self.backend {
			Backend::User => Ok(self.lease(None, false)),
			Backend::Unavailable(reason) => bail!("{reason}"),
			Backend::Tap { ready, .. } => {
				let slot = ready.lock().pop();
				let lease = if let Some(slot) = slot {
					self.lease(Some(slot), true)
				} else {
					let lease = self.lease(Some(self.allocate()?), true);
					self.setup(lease.tap.as_ref(), true).await?;
					lease
				};
				self.refill();
				Ok(lease)
			},
		}
	}

	/// Allocates a fresh network attachment without egress hooks (template
	/// build VMs have unrestricted egress); it never enters the ready pool.
	pub async fn acquire_unfiltered(self: &Arc<Self>) -> anyhow::Result<NetLease> {
		match &self.backend {
			Backend::User => Ok(self.lease(None, false)),
			Backend::Unavailable(reason) => bail!("{reason}"),
			Backend::Tap { .. } => {
				let lease = self.lease(Some(self.allocate()?), false);
				self.setup(lease.tap.as_ref(), false).await?;
				Ok(lease)
			},
		}
	}

	fn lease(self: &Arc<Self>, tap: Option<TapLease>, recyclable: bool) -> NetLease {
		NetLease { tap, manager: Arc::downgrade(self), recyclable }
	}

	/// Reserves the next free block of the TAP address pool.
	fn allocate(&self) -> anyhow::Result<TapLease> {
		let Backend::Tap { pool, .. } = &self.backend else {
			bail!("this node does not use TAP networking");
		};
		let block = pool
			.lock()
			.allocate()
			.ok_or_else(|| anyhow!("TAP address pool exhausted"))?;
		Ok(TapLease::from_block(block))
	}

	/// Creates the TAP with its NAT rules and connection mark and, when
	/// `hooks` and egress is known, its egress hooks with closed policy
	/// chains. On failure the caller tears the lease down.
	async fn setup(&self, lease: Option<&TapLease>, hooks: bool) -> anyhow::Result<()> {
		let (Backend::Tap { owner_uid, .. }, Some(lease)) = (&self.backend, lease) else {
			return Ok(());
		};
		setup_tap(lease.clone(), *owner_uid).await?;
		if hooks && let Some(egress) = self.egress() {
			egress.prepare_tap(&lease.tap).await?;
		}
		Ok(())
	}

	/// Keeps up to `slots` (≤ [`MAX_TAP_SLOTS`]) TAPs set up ahead of time, so
	/// creates and resumes take a ready one instead of spawning `ip`/iptables
	/// on the request path; released TAPs are recycled into the pool. First
	/// tears down `vmon<N>` TAPs no sandbox holds (idle slots of a previous
	/// process). Call once, after restart recovery re-reserved the leases of
	/// adopted sandboxes and after [`Self::set_egress`].
	pub async fn prewarm(self: &Arc<Self>, slots: usize) {
		let Backend::Tap { pool, slots: target, .. } = &self.backend else {
			return;
		};
		let stale: Vec<TapLease> = existing_taps()
			.into_iter()
			.filter_map(|index| {
				let mut pool = pool.lock();
				pool
					.reserve(index)
					.then(|| pool.block(index))
					.flatten()
					.map(TapLease::from_block)
			})
			.collect();
		if !stale.is_empty() {
			tracing::info!(count = stale.len(), "removing TAPs left by a previous node process");
			let manager = Arc::downgrade(self);
			let _ = tokio::task::spawn_blocking(move || {
				for lease in &stale {
					teardown(&manager, lease);
				}
			})
			.await;
		}
		target.store(slots.min(MAX_TAP_SLOTS), Ordering::Relaxed);
		self.refill();
	}

	/// Tears down the idle ready TAPs and stops refilling (node shutdown).
	pub async fn shutdown(self: &Arc<Self>) {
		let Backend::Tap { ready, slots, .. } = &self.backend else {
			return;
		};
		slots.store(0, Ordering::Relaxed);
		let idle = std::mem::take(&mut *ready.lock());
		let manager = Arc::downgrade(self);
		let _ = tokio::task::spawn_blocking(move || {
			for lease in &idle {
				teardown(&manager, lease);
			}
		})
		.await;
	}

	/// Starts a background refill of the ready TAPs unless one is running or
	/// the pool is full.
	fn refill(self: &Arc<Self>) {
		let Backend::Tap { ready, slots, refilling, .. } = &self.backend else {
			return;
		};
		if ready.lock().len() >= slots.load(Ordering::Relaxed)
			|| refilling.swap(true, Ordering::AcqRel)
		{
			return;
		}
		let manager = Arc::clone(self);
		tokio::spawn(async move {
			let Backend::Tap { ready, slots, refilling, .. } = &manager.backend else {
				return;
			};
			while ready.lock().len() < slots.load(Ordering::Relaxed) {
				let Ok(lease) = manager.allocate() else { break };
				let weak = Arc::downgrade(&manager);
				if let Err(error) = manager.setup(Some(&lease), true).await {
					tracing::warn!(error = format!("{error:#}"), tap = %lease.tap, "pre-creating a TAP failed");
					let _ = tokio::task::spawn_blocking(move || teardown(&weak, &lease)).await;
					break;
				}
				if !manager.make_ready(&lease, 1) {
					let _ = tokio::task::spawn_blocking(move || teardown(&weak, &lease)).await;
				}
			}
			refilling.store(false, Ordering::Release);
		});
	}

	/// Whether the ready pool holds `factor` × its target size.
	fn pool_full(&self, factor: usize) -> bool {
		match &self.backend {
			Backend::Tap { ready, slots, .. } => {
				ready.lock().len() >= factor * slots.load(Ordering::Relaxed)
			},
			_ => true,
		}
	}

	/// Puts a set-up TAP into the ready pool unless it holds `factor` × its
	/// target size; `false` when it is full.
	fn make_ready(&self, lease: &TapLease, factor: usize) -> bool {
		let Backend::Tap { ready, slots, .. } = &self.backend else {
			return false;
		};
		let mut ready = ready.lock();
		if ready.len() >= factor * slots.load(Ordering::Relaxed) {
			return false;
		}
		ready.push(lease.clone());
		true
	}

	/// Returns the TAP of an exited VM whose egress chains were reset to
	/// closed ([`Egress::release`]) to the ready pool, after flushing its
	/// guest's conntrack entries; `false` when the pool is full. Blocking.
	///
	/// The pool keeps up to [`RECYCLE_HEADROOM`] × its target: the refill
	/// already replaced the TAP when its VM started, so tearing it down now
	/// and setting one up for the next create would churn iptables (every
	/// changing commit takes milliseconds) next to the request path.
	fn recycle(&self, lease: &TapLease) -> bool {
		if self.pool_full(RECYCLE_HEADROOM) {
			return false;
		}
		flush_conntrack(lease);
		self.make_ready(lease, RECYCLE_HEADROOM)
	}

	/// Re-reserves the lease of a VM adopted during restart recovery; its TAP
	/// and rules already exist. Its TAP is torn down on release (its rules
	/// come from a previous process).
	pub fn adopt(self: &Arc<Self>, tap: Option<TapLease>) -> anyhow::Result<NetLease> {
		match (&self.backend, &tap) {
			(Backend::Tap { pool, .. }, Some(lease)) => {
				if !pool.lock().reserve(lease.index) {
					bail!("recovered TAP lease {} is already in use", lease.index);
				}
			},
			(Backend::User, None) => {},
			_ => bail!("recovered network attachment does not match this node's networking mode"),
		}
		Ok(self.lease(tap, false))
	}

	fn release(&self, lease: &TapLease) {
		if let Backend::Tap { pool, .. } = &self.backend {
			pool.lock().release(lease.index);
		}
	}
}

#[cfg(target_os = "linux")]
fn has_net_admin() -> bool {
	tap::has_net_admin()
}

#[cfg(target_os = "linux")]
fn existing_taps() -> Vec<u32> {
	tap::existing()
}

#[cfg(not(target_os = "linux"))]
const fn existing_taps() -> Vec<u32> {
	Vec::new()
}

#[cfg(not(target_os = "linux"))]
const fn has_net_admin() -> bool {
	false
}

#[cfg(target_os = "linux")]
async fn setup_tap(lease: TapLease, owner_uid: Option<u32>) -> anyhow::Result<()> {
	tokio::task::spawn_blocking(move || tap::setup(&lease, owner_uid)).await?
}

#[cfg(not(target_os = "linux"))]
#[expect(clippy::unused_async, reason = "mirrors the Linux signature")]
async fn setup_tap(_lease: TapLease, _owner_uid: Option<u32>) -> anyhow::Result<()> {
	bail!("TAP networking requires Linux")
}

/// Deletes the conntrack entries of the lease's guest flows (by connection
/// mark), so the lease's next VM inherits no NAT mappings or established
/// state. Blocking.
#[cfg(target_os = "linux")]
fn flush_conntrack(lease: &TapLease) {
	if let Err(error) = conntrack::flush_mark(lease.conntrack_mark()) {
		tracing::warn!(%error, tap = %lease.tap, "flushing the TAP's conntrack entries failed");
	}
}

#[cfg(not(target_os = "linux"))]
const fn flush_conntrack(_lease: &TapLease) {}

impl NetLease {
	/// VMM networking flags for this attachment.
	pub fn vm_net(&self) -> VmNet {
		match &self.tap {
			Some(lease) => VmNet::Tap { name: lease.tap.clone(), mac: lease.mac.clone() },
			None => VmNet::User,
		}
	}

	/// The TAP lease, if any (persisted for recovery, handed to egress).
	pub const fn tap(&self) -> Option<&TapLease> {
		self.tap.as_ref()
	}

	/// Guest `eth0` configuration to push with GC4 `net_config`.
	pub fn guest_config(&self) -> NetConfig {
		match &self.tap {
			Some(lease) => NetConfig {
				ip:     lease.guest_ip.to_string(),
				prefix: lease.prefix,
				gw:     lease.host_ip.to_string(),
				dns:    TAP_DNS.iter().map(|dns| (*dns).to_owned()).collect(),
			},
			// libslirp's fixed guest network.
			None => NetConfig {
				ip:     "10.0.2.15".into(),
				prefix: 24,
				gw:     "10.0.2.2".into(),
				dns:    vec!["10.0.2.3".into()],
			},
		}
	}

	/// Returns the TAP to the ready pool (after flushing its conntrack
	/// entries), or tears it down and frees the lease when the pool is full
	/// or the TAP is not recyclable. Call only once the VM has exited and its
	/// egress attachment is released.
	pub async fn release(mut self) {
		let Some(lease) = self.tap.take() else { return };
		let (manager, recyclable) = (self.manager.clone(), self.recyclable);
		let _ = tokio::task::spawn_blocking(move || {
			if recyclable
				&& manager
					.upgrade()
					.is_some_and(|manager| manager.recycle(&lease))
			{
				return;
			}
			teardown(&manager, &lease);
		})
		.await;
	}
}

/// Removes the TAP's egress rules, NAT rules, and device, flushes its
/// guest's conntrack entries, and frees the lease. Blocking.
fn teardown(manager: &Weak<NetManager>, lease: &TapLease) {
	let manager = manager.upgrade();
	if let Some(egress) = manager.as_ref().and_then(|manager| manager.egress()) {
		egress.remove_tap(&lease.tap);
	}
	#[cfg(target_os = "linux")]
	if let Err(error) = tap::teardown(lease) {
		tracing::warn!(%error, tap = %lease.tap, "TAP teardown failed");
	}
	flush_conntrack(lease);
	if let Some(manager) = manager {
		manager.release(lease);
	}
}

impl Drop for NetLease {
	fn drop(&mut self) {
		if let Some(lease) = self.tap.take() {
			teardown(&self.manager, &lease);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tap_lease_names_and_macs_are_unique_per_block() {
		let mut pool = LeaseAllocator::new("172.20.0.0/16").unwrap();
		let a = TapLease::from_block(pool.allocate().unwrap());
		let b = TapLease::from_block(pool.block(300).unwrap());
		assert_eq!(a.tap, "vmon0");
		assert_eq!(a.mac, "02:76:6d:00:00:01");
		assert_eq!(b.tap, "vmon300");
		assert_eq!(b.mac, "02:76:6d:00:01:2d");
		let last = TapLease::from_block(pool.block(16_383).unwrap());
		assert!(last.tap.len() <= 15, "Linux interface names are at most 15 bytes");
	}

	#[test]
	fn guest_config_matches_lease_or_slirp() {
		let manager = Arc::new(NetManager { backend: Backend::User });
		let user = manager.lease(None, false);
		assert_eq!(user.guest_config().ip, "10.0.2.15");
		assert_eq!(user.vm_net(), VmNet::User);
		let pool = LeaseAllocator::new("172.20.0.0/16").unwrap();
		let lease = TapLease::from_block(pool.block(1).unwrap());
		let tap = NetLease { tap: Some(lease), manager: Weak::new(), recyclable: false };
		let config = tap.guest_config();
		assert_eq!(
			(config.ip.as_str(), config.gw.as_str(), config.prefix),
			("172.20.0.6", "172.20.0.5", 30)
		);
		// Avoid running teardown commands for this synthetic lease.
		std::mem::forget(tap);
	}

	#[cfg(not(target_os = "linux"))]
	#[test]
	fn platform_mode_resolution() {
		assert!(NetManager::new(NetMode::Tap, "172.20.0.0/16", None).is_err());
		let auto = NetManager::new(NetMode::Auto, "172.20.0.0/16", None).unwrap();
		assert_eq!(auto.features(), ["net-user"]);
	}
}
