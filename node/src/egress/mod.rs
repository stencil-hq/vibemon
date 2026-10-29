//! Sandbox egress enforcement (ARCHITECTURE.md §8.4).
//!
//! [`Egress`] is the node runtime's single entry point:
//! - Linux (TAP networking): a transparent proxy fed by iptables `REDIRECT`
//!   implements the full E2B semantics (domains via Host/SNI, CIDR allow/deny,
//!   private ranges, SOCKS5 `egressProxy`, HTTPS header `rules` via MITM) plus
//!   a UDP/ICMP CIDR filter per TAP.
//! - macOS (`--net user`): the policy is compiled into the VMM user-net filter
//!   document ([`policy::VmmFilter`]), passed at launch with
//!   `--user-net-policy` and replaced live through the VMM control method
//!   `set_net_policy`.
//!
//! [`policy::validate`]/[`policy::normalize`] are shared with the driver.

use std::{
	collections::HashMap,
	fmt,
	net::Ipv4Addr,
	path::{Path, PathBuf},
	sync::Arc,
};

use anyhow::{Context as _, bail};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::api::NetworkPolicy;

#[cfg(target_os = "linux")]
mod linux;
pub mod mitm;
pub mod policy;
pub mod proxy;
pub mod sniff;
pub mod socks;
#[cfg(unix)]
mod vmm_control;

/// Byte stream usable as either side of a proxied flow.
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}

/// Type-erased [`Io`] (plain TCP, TLS to a proxy, replayed prefix, …).
pub type BoxedIo = Box<dyn Io>;

/// Where a node-local service (the CI cache) listens for TAP guests.
///
/// `port` on the host side of every lease of the TAP pool (`--net-pool`; /30
/// leases, host = block + 1), not only the guest's current gateway, so a
/// sandbox migrated from another node keeps reaching it at the address it
/// was told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalTarget {
	pub port: u16,
	base:     u32,
	mask:     u32,
}

impl LocalTarget {
	/// `port` on the lease host addresses of `pool` (`a.b.c.d/len`, `len`
	/// 8–30, no host bits set).
	pub fn new(port: u16, pool: &str) -> anyhow::Result<Self> {
		let (addr, len) = pool
			.split_once('/')
			.with_context(|| format!("net pool {pool:?} must be a CIDR"))?;
		let addr: Ipv4Addr = addr
			.parse()
			.with_context(|| format!("net pool {pool:?}: bad address"))?;
		let len: u32 = len
			.parse()
			.with_context(|| format!("net pool {pool:?}: bad prefix length"))?;
		anyhow::ensure!((8..=30).contains(&len), "net pool {pool:?}: prefix must be /8 to /30");
		let mask = u32::MAX << (32 - len);
		let base = u32::from(addr);
		anyhow::ensure!(base & !mask == 0, "net pool {pool:?} has host bits set");
		Ok(Self { port, base, mask })
	}

	/// Whether a guest connection to `orig` is for the service.
	pub fn matches(&self, orig: std::net::SocketAddr) -> bool {
		let std::net::SocketAddr::V4(orig) = orig else {
			return false;
		};
		let ip = u32::from(*orig.ip());
		orig.port() == self.port && ip & self.mask == self.base && (ip - self.base) % 4 == 1
	}
}

/// Node egress settings (from `vmon serve` flags).
#[derive(Clone, Debug)]
pub struct EgressConfig {
	/// Node data dir; the interception CA lives in `<data_dir>/egress/`.
	pub data_dir:       PathBuf,
	/// Operator CIDRs exempt from the always-denied private ranges.
	pub internal_allow: Vec<String>,
	/// Linux transparent proxy port (0 = ephemeral).
	pub proxy_port:     u16,
}

/// How a sandbox is connected, as provided by the node runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EgressNet {
	/// macOS user-mode NAT: live updates go to the VMM control socket.
	User { api_sock: PathBuf },
	/// Linux TAP lease.
	Tap { tap: String, host_ip: Ipv4Addr, guest_ip: Ipv4Addr },
}

/// Node-wide egress enforcement; cheap to clone.
#[derive(Clone)]
pub struct Egress {
	inner: Arc<Inner>,
}

struct Inner {
	internal: Vec<policy::Cidr>,
	attached: tokio::sync::Mutex<HashMap<String, EgressNet>>,
	#[cfg(target_os = "linux")]
	proxy:    Arc<linux::Proxy>,
	/// TAPs whose hooks target this process's proxy port (installed by
	/// [`Egress::prepare_tap`] or a full [`Egress::attach`]) and the state of
	/// their policy chains; attach and release only rewrite the chains.
	#[cfg(target_os = "linux")]
	hooked:   parking_lot::Mutex<HashMap<String, Chains>>,
	ca_pem:   Option<String>,
}

/// Policy chains of a hooked TAP.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Chains {
	/// A single DROP: no sandbox attached ([`linux::open_filter`] applies).
	Closed,
	/// A sandbox's policy.
	Open,
}

impl fmt::Debug for Egress {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Egress").finish_non_exhaustive()
	}
}

impl Egress {
	/// Start egress enforcement. On Linux this loads/creates the CA and binds
	/// the transparent proxy; elsewhere it only validates the config.
	#[cfg_attr(
		not(target_os = "linux"),
		expect(
			clippy::unused_async,
			clippy::unused_async_trait_impl,
			reason = "only the Linux proxy bind awaits"
		)
	)]
	pub async fn start(cfg: EgressConfig) -> anyhow::Result<Self> {
		let internal = policy::parse_internal(&cfg.internal_allow).map_err(anyhow::Error::msg)?;
		#[cfg(target_os = "linux")]
		{
			let ca = Arc::new(mitm::Ca::load_or_create(&cfg.data_dir.join("egress"))?);
			let ca_pem = Some(ca.pem().to_string());
			let proxy = linux::Proxy::start(cfg.proxy_port, proxy::Shared::new(Some(ca))?).await?;
			tracing::info!(port = proxy.port(), "egress proxy listening");
			Ok(Self {
				inner: Arc::new(Inner {
					internal,
					attached: tokio::sync::Mutex::default(),
					proxy,
					hooked: parking_lot::Mutex::default(),
					ca_pem,
				}),
			})
		}
		#[cfg(not(target_os = "linux"))]
		{
			let _ = (&cfg.data_dir, cfg.proxy_port);
			Ok(Self {
				inner: Arc::new(Inner {
					internal,
					attached: tokio::sync::Mutex::default(),
					ca_pem: None,
				}),
			})
		}
	}

	/// Node features contributed by egress (`MemberMeta.features`).
	pub fn features(&self) -> Vec<String> {
		if cfg!(target_os = "linux") {
			vec!["egress-domains".to_string(), "egress-proxy".to_string()]
		} else {
			vec!["egress-domains".to_string()]
		}
	}

	/// Hands TAP guests' connections to `target` (a port on any lease host
	/// address) to `service` instead of proxying them (the CI cache,
	/// [`crate::ci_cache`]), with the sandbox of the TAP they arrived on;
	/// whatever the sandbox's policy, the service decides whom it serves. Set
	/// once; a no-op without TAP networking.
	pub fn set_local_service(
		&self,
		target: LocalTarget,
		service: Arc<dyn crate::ci_cache::LocalService>,
	) {
		#[cfg(target_os = "linux")]
		self.inner.proxy.set_local(target, service);
		#[cfg(not(target_os = "linux"))]
		let _ = (target, service);
	}

	/// CA PEM the guest must trust for HTTPS `rules` (envd `/init` `caBundle`).
	pub fn ca_bundle(&self) -> Option<String> {
		self.inner.ca_pem.clone()
	}

	fn compile(&self, policy: &NetworkPolicy) -> anyhow::Result<policy::Compiled> {
		policy::Compiled::compile(policy, &self.inner.internal).map_err(anyhow::Error::msg)
	}

	/// For `--net user` launches (create, fork, resume): write the compiled
	/// VMM filter to `policy_path` and return the extra `vmon vmm` argv.
	pub fn user_net_args(
		&self,
		policy_path: &Path,
		policy: &NetworkPolicy,
	) -> anyhow::Result<Vec<String>> {
		let filter = self.compile(policy)?.vmm_filter();
		let tmp = policy_path.with_extension("tmp");
		std::fs::write(&tmp, serde_json::to_vec(&filter)?)
			.with_context(|| format!("writing {}", tmp.display()))?;
		std::fs::rename(&tmp, policy_path).with_context(|| format!("renaming {}", tmp.display()))?;
		Ok(vec!["--user-net-policy".to_string(), policy_path.display().to_string()])
	}

	/// Pre-installs the hooks and closed policy chains of a TAP no sandbox
	/// uses yet (TAP pool refill, off the request path), so [`Self::attach`]
	/// only adds the policy rules.
	#[cfg_attr(
		not(target_os = "linux"),
		expect(
			clippy::unused_async,
			clippy::unused_async_trait_impl,
			reason = "only Linux iptables setup awaits"
		)
	)]
	pub async fn prepare_tap(&self, tap: &str) -> anyhow::Result<()> {
		#[cfg(target_os = "linux")]
		{
			linux::install_hooks(tap, self.inner.proxy.port()).await?;
			self
				.inner
				.hooked
				.lock()
				.insert(tap.to_owned(), Chains::Closed);
			Ok(())
		}
		#[cfg(not(target_os = "linux"))]
		{
			let _ = tap;
			bail!("TAP egress enforcement requires a Linux node")
		}
	}

	/// Removes every egress hook and chain of a TAP that is being deleted
	/// (TAP teardown). Blocking; call off the async runtime.
	#[cfg_attr(
		not(target_os = "linux"),
		expect(clippy::missing_const_for_fn, reason = "the Linux body locks and runs iptables")
	)]
	pub fn remove_tap(&self, tap: &str) {
		#[cfg(target_os = "linux")]
		{
			self.inner.hooked.lock().remove(tap);
			linux::remove(tap);
		}
		#[cfg(not(target_os = "linux"))]
		let _ = tap;
	}

	/// Start enforcing `policy` for a running sandbox (after its VMM is up).
	/// Re-attaching an attached sandbox replaces the previous attachment.
	/// On a prepared TAP ([`Self::prepare_tap`]) this is one add-only
	/// `iptables-restore` of its policy rules; other TAPs (adopted during
	/// restart recovery) get their hooks reinstalled first.
	/// The node serializes operations per sandbox, so the attachment table is
	/// not held across rule installation (other sandboxes' attach/detach stay
	/// fast).
	pub async fn attach(
		&self,
		sandbox_id: &str,
		net: EgressNet,
		policy: &NetworkPolicy,
	) -> anyhow::Result<()> {
		let compiled = self.compile(policy)?;
		if let Some(previous) = self.take(sandbox_id).await {
			self.release(&previous).await;
		}
		match &net {
			EgressNet::User { .. } => {
				if cfg!(target_os = "linux") {
					bail!("user-mode networking is not supported on Linux nodes");
				}
			},
			#[cfg(target_os = "linux")]
			EgressNet::Tap { tap, guest_ip, .. } => {
				self.inner.proxy.set(*guest_ip, linux::Guest {
					policy:  Arc::new(compiled.clone()),
					sandbox: sandbox_id.into(),
				});
				let chains = self.inner.hooked.lock().get(tap).copied();
				let installed = match chains {
					Some(Chains::Closed) => linux::open_filter(tap, &compiled).await,
					Some(Chains::Open) => linux::apply_filter(tap, &compiled).await,
					None => linux::install(tap, self.inner.proxy.port(), &compiled).await,
				};
				if let Err(e) = installed {
					self.inner.proxy.remove(*guest_ip);
					return Err(e.context(format!("installing egress rules for {sandbox_id}")));
				}
				self.inner.hooked.lock().insert(tap.clone(), Chains::Open);
			},
			#[cfg(not(target_os = "linux"))]
			EgressNet::Tap { .. } => bail!("TAP egress enforcement requires a Linux node"),
		}
		let _ = compiled;
		self
			.inner
			.attached
			.lock()
			.await
			.insert(sandbox_id.to_string(), net);
		Ok(())
	}

	/// Replace a running sandbox's policy (`PUT /sandboxes/{id}/network`).
	/// New flows see the new policy; established flows continue.
	pub async fn update(&self, sandbox_id: &str, policy: &NetworkPolicy) -> anyhow::Result<()> {
		let compiled = self.compile(policy)?;
		let attached = self.inner.attached.lock().await;
		let net = attached
			.get(sandbox_id)
			.with_context(|| format!("sandbox {sandbox_id} has no egress attachment"))?;
		match net {
			#[cfg(unix)]
			EgressNet::User { api_sock } => {
				vmm_control::set_net_policy(api_sock, &compiled.vmm_filter()).await
			},
			#[cfg(not(unix))]
			EgressNet::User { .. } => bail!("live user-net policy updates require a Unix control socket"),
			#[cfg(target_os = "linux")]
			EgressNet::Tap { tap, guest_ip, .. } => {
				linux::apply_filter(tap, &compiled).await?;
				self.inner.proxy.set(*guest_ip, linux::Guest {
					policy:  Arc::new(compiled),
					sandbox: sandbox_id.into(),
				});
				Ok(())
			},
			#[cfg(not(target_os = "linux"))]
			EgressNet::Tap { .. } => bail!("TAP egress enforcement requires a Linux node"),
		}
	}

	/// Stop enforcing for a sandbox (pause, kill, VMM exit); idempotent.
	/// Must run before the TAP is deleted.
	pub async fn detach(&self, sandbox_id: &str) {
		if let Some(net) = self.take(sandbox_id).await {
			self.release(&net).await;
		}
	}

	/// Forgets a sandbox's attachment and returns it for [`Self::release`]:
	/// a later [`Self::attach`] of the same sandbox (resume) is then
	/// unaffected by the release of the old one. Idempotent.
	pub async fn take(&self, sandbox_id: &str) -> Option<EgressNet> {
		self.inner.attached.lock().await.remove(sandbox_id)
	}

	/// Removes the sandbox state of an attachment returned by [`Self::take`]:
	/// its proxy policy, and its policy chains are reset to closed (one
	/// `iptables-restore`; the hooks stay for the TAP's next sandbox and go
	/// with [`Self::remove_tap`]). Must run before its TAP is reused.
	#[cfg_attr(
		not(target_os = "linux"),
		expect(
			clippy::unused_async,
			clippy::unused_async_trait_impl,
			reason = "only Linux iptables cleanup awaits"
		)
	)]
	pub async fn release(&self, net: &EgressNet) {
		match net {
			EgressNet::User { .. } => {},
			#[cfg(target_os = "linux")]
			EgressNet::Tap { tap, guest_ip, .. } => {
				self.inner.proxy.remove(*guest_ip);
				let hooked = self.inner.hooked.lock().contains_key(tap);
				let closed = if hooked {
					linux::close_filter(tap).await
				} else {
					Err(anyhow::anyhow!("{tap} has no egress hooks of this node process"))
				};
				match closed {
					Ok(()) => {
						self.inner.hooked.lock().insert(tap.clone(), Chains::Closed);
					},
					Err(e) => {
						// Never hand the TAP on with this sandbox's policy: drop
						// everything; its next attach installs from scratch.
						if hooked {
							tracing::warn!("egress: {e:#}; removing the rules of {tap}");
						}
						self.inner.hooked.lock().remove(tap);
						let tap = tap.clone();
						let _ = tokio::task::spawn_blocking(move || linux::remove(&tap)).await;
					},
				}
			},
			#[cfg(not(target_os = "linux"))]
			EgressNet::Tap { .. } => {},
		}
	}
}

#[cfg(test)]
mod local_target_tests {
	use super::LocalTarget;

	#[test]
	fn lease_host_addresses_of_the_pool_match() {
		let target = LocalTarget::new(7948, "172.20.0.0/16").unwrap();
		let at = |addr: &str| target.matches(addr.parse().unwrap());
		// Host side (+1) of any /30 lease: the current or a previous gateway.
		assert!(at("172.20.0.1:7948") && at("172.20.0.125:7948") && at("172.20.255.253:7948"));
		// Network, guest, broadcast addresses; other ports; outside the pool.
		assert!(!at("172.20.0.124:7948") && !at("172.20.0.126:7948") && !at("172.20.0.127:7948"));
		assert!(!at("172.20.0.125:7949") && !at("172.21.0.1:7948") && !at("[::1]:7948"));
		assert!(LocalTarget::new(7948, "172.20.0.1/16").is_err(), "host bits");
		assert!(LocalTarget::new(7948, "172.20.0.0/31").is_err());
		assert!(LocalTarget::new(7948, "nope").is_err());
	}
}
