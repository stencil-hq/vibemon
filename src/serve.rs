//! `vmon serve`: starts the cluster layer, then the node and/or driver roles,
//! then the cluster and public listeners (ARCHITECTURE.md §1, §3).

use std::{
	fs,
	net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
	path::{Path, PathBuf},
	time::Duration,
};

use anyhow::{Context, bail};
use axum::serve::ListenerExt;
use clap::{Args, ValueEnum};
use tokio::signal::unix::{Signal, SignalKind, signal};
use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeId, RaftStatus, Roles};
use vmon_driver::{Driver, DriverConfig, TeamLimits, ingress::listener::TlsFiles};
use vmon_node::{
	CapacityOverrides, Node, NodeConfig,
	config::{NetMode, default_data_dir, default_run_dir},
};

/// `--net` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum NetArg {
	/// TAP on Linux, user-mode NAT elsewhere.
	Auto,
	/// Host TAP + NAT (Linux, needs the net-admin capability).
	Tap,
	/// User-mode NAT inside the VMM.
	User,
}

impl From<NetArg> for NetMode {
	fn from(arg: NetArg) -> Self {
		match arg {
			NetArg::Auto => Self::Auto,
			NetArg::Tap => Self::Tap,
			NetArg::User => Self::User,
		}
	}
}

/// Roles a `vmon serve` process can run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Role {
	/// Hosts microVMs.
	Node,
	/// Serves the E2B API and sandbox ingress, schedules work.
	Driver,
}

/// Flags of `vmon serve`. Every flag also reads a `VMON_*` environment
/// variable.
#[derive(Args, Debug)]
pub struct ServeArgs {
	/// Roles to run (comma separated).
	#[arg(long, env = "VMON_ROLE", value_delimiter = ',', default_value = "node,driver")]
	role:     Vec<Role>,
	/// State directory (default: /var/lib/vmon as root on Linux, else ~/.vmon).
	#[arg(long, env = "VMON_HOME")]
	data_dir: Option<PathBuf>,
	/// Directory for per-VM Unix sockets (default: <runtime dir>/vmon or
	/// /tmp/vmon-<uid>).
	#[arg(long, env = "VMON_RUN_DIR")]
	run_dir:  Option<PathBuf>,
	/// Log filter (tracing env-filter syntax).
	#[arg(long, env = "VMON_LOG", default_value = "info,openraft=warn")]
	log:      String,
	/// Emit JSON logs.
	#[arg(long, env = "VMON_LOG_JSON")]
	log_json: bool,

	/// Shared cluster secret; generated and stored in the data dir when unset.
	#[arg(long, env = "VMON_CLUSTER_SECRET", hide_env_values = true)]
	cluster_secret:    Option<String>,
	/// Internal HTTP listener (raft, KV, node API, tunnels).
	#[arg(long, env = "VMON_CLUSTER_LISTEN", default_value = "0.0.0.0:7947")]
	cluster_listen:    SocketAddr,
	/// URL peers use to reach `--cluster-listen` (default: http://<detected-ip>:<port>).
	#[arg(long, env = "VMON_CLUSTER_ADVERTISE")]
	cluster_advertise: Option<String>,
	/// Gossip listener (UDP probes + TCP push-pull).
	#[arg(long, env = "VMON_GOSSIP_LISTEN", default_value = "0.0.0.0:7946")]
	gossip_listen:     SocketAddr,
	/// Address peers use to reach `--gossip-listen`.
	#[arg(long, env = "VMON_GOSSIP_ADVERTISE")]
	gossip_advertise:  Option<SocketAddr>,
	/// Gossip seeds (`host:port`, comma separated).
	#[arg(long, env = "VMON_JOIN", value_delimiter = ',')]
	join:              Vec<String>,
	/// Number of drivers that must be visible before the raft cluster
	/// bootstraps.
	#[arg(long, env = "VMON_BOOTSTRAP_EXPECT", default_value_t = 1)]
	bootstrap_expect:  usize,
	/// How long a dead driver stays a raft voter before removal (seconds).
	#[arg(long, env = "VMON_RAFT_REAP_AFTER", default_value_t = 600)]
	raft_reap_after:   u64,

	/// Public API + sandbox ingress listener (driver).
	#[arg(long, env = "VMON_LISTEN", default_value = "0.0.0.0:3000")]
	listen:      SocketAddr,
	/// Optional HTTPS listener for the same router (driver).
	#[arg(long, env = "VMON_TLS_LISTEN", requires_all = ["tls_cert", "tls_key"])]
	tls_listen:  Option<SocketAddr>,
	/// PEM certificate chain (wildcard for `*.<domain>` plus `api.<domain>`).
	#[arg(long, env = "VMON_TLS_CERT")]
	tls_cert:    Option<PathBuf>,
	/// PEM private key.
	#[arg(long, env = "VMON_TLS_KEY")]
	tls_key:     Option<PathBuf>,
	/// Sandbox domain: sandboxes are served at `<port>-<id>.<domain>`.
	#[arg(long, env = "VMON_DOMAIN", default_value = "localhost")]
	domain:      String,
	/// Public base URL of this driver (signed upload URLs point here).
	#[arg(long, env = "VMON_PUBLIC_URL")]
	public_url:  Option<String>,
	/// Database URL (driver), e.g. `mysql://user:pass@host:3306/vmon`.
	#[arg(long, env = "VMON_MYSQL_URL", hide_env_values = true)]
	mysql_url:   Option<String>,
	/// Token accepted in `X-Admin-Token` for admin endpoints.
	#[arg(long, env = "VMON_ADMIN_TOKEN", hide_env_values = true)]
	admin_token: Option<String>,

	/// Guest kernel (default: `vmon-kernel-<arch>` next to vmon, else
	/// `target/kernel/` of a dev tree, else a pinned cloud-hypervisor kernel
	/// downloaded into the data dir that lacks netfilter/IPv6/NFS).
	#[arg(long, env = "VMON_KERNEL")]
	kernel:                  Option<PathBuf>,
	/// Static guest agent binary (default: `vmon-agent-<arch>` next to vmon).
	#[arg(long, env = "VMON_AGENT")]
	agent:                   Option<PathBuf>,
	/// Guest networking.
	#[arg(long, env = "VMON_NET", value_enum, default_value_t = NetArg::Auto)]
	net:                     NetArg,
	/// Address pool for Linux TAP leases.
	#[arg(long, env = "VMON_NET_POOL", default_value = "172.20.0.0/16")]
	net_pool:                String,
	/// Concurrent VM boots (0 = 4 × host CPUs).
	#[arg(long, env = "VMON_BOOT_CONCURRENCY", default_value_t = 0)]
	boot_concurrency:        usize,
	/// Concurrent template builds on this node (0 = 2).
	#[arg(long, env = "VMON_MAX_BUILDS", default_value_t = 0)]
	max_builds:              usize,
	/// Linux root only: uid:gid that VMM processes drop to (unset = no privilege
	/// drop).
	#[arg(long, env = "VMON_VMM_USER", value_parser = parse_uid_gid)]
	vmm_user:                Option<(u32, u32)>,
	/// CIDRs sandboxes may reach despite the always-denied private ranges (comma
	/// separated).
	#[arg(long, env = "VMON_EGRESS_INTERNAL_ALLOW", value_delimiter = ',')]
	egress_internal_allow:   Vec<String>,
	/// Docker Hub pull-through mirror tried first for anonymous `docker.io`
	/// template base images, e.g. `https://mirror.gcr.io` (falls back to
	/// Docker Hub).
	#[arg(long, env = "VMON_REGISTRY_MIRROR")]
	registry_mirror:         Option<String>,
	/// Operator object store (<s3://bucket/prefix> or
	/// <https://endpoint/bucket/prefix>): portable paused sandboxes and S3
	/// volumes. Credentials/region/endpoint from `VMON_S3_*` or the AWS
	/// environment and workload role.
	#[arg(long, env = "VMON_OBJECT_STORE")]
	object_store:            Option<String>,
	/// Resume paused sandboxes of other nodes lazily from the object store
	/// (rootfs blocks on demand; on Linux guest memory too); `false` always
	/// downloads the whole paused state first.
	#[arg(long, env = "VMON_LAZY_RESUME", default_value_t = true, action = clap::ArgAction::Set)]
	lazy_resume:             bool,
	/// Let `vmonS3Mounts` endpoints resolve to loopback/private/link-local
	/// addresses (off: the node would reach its own network for users).
	#[arg(long, env = "VMON_S3_MOUNTS_ALLOW_PRIVATE")]
	s3_mounts_allow_private: bool,
	/// Volume directory (default: <data-dir>/volumes).
	#[arg(long, env = "VMON_VOLUMES_DIR")]
	volumes_dir:             Option<PathBuf>,
	/// The volume directory is shared storage visible to every node.
	#[arg(long, env = "VMON_SHARED_VOLUMES")]
	shared_volumes:          bool,
	/// vCPUs offered to sandboxes (default: host CPUs).
	#[arg(long, env = "VMON_MAX_VCPUS")]
	max_vcpus:               Option<u32>,
	/// Memory offered to sandboxes in MiB (default: host memory).
	#[arg(long, env = "VMON_MAX_MEMORY_MB")]
	max_memory_mb:           Option<u64>,
	/// Node role on Linux: reserve this many MiB of 2 MiB hugetlb pages
	/// (`vm.nr_hugepages`) at startup for sandbox RAM restored from snapshots
	/// (create, resume, fork). 0 leaves the host setting alone.
	#[arg(long, env = "VMON_HUGEPAGE_POOL_MB", default_value_t = 0)]
	hugepage_pool_mb:        u64,
}

fn parse_uid_gid(raw: &str) -> Result<(u32, u32), String> {
	let (uid, gid) = raw.split_once(':').ok_or("expected <uid>:<gid>")?;
	let parse = |part: &str| part.parse::<u32>().map_err(|e| format!("{part:?}: {e}"));
	Ok((parse(uid)?, parse(gid)?))
}

/// Runs `vmon serve` until SIGINT/SIGTERM.
pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
	let mut signals = Signals::install()?;
	init_tracing(&args.log, args.log_json)?;
	let roles =
		Roles { node: args.role.contains(&Role::Node), driver: args.role.contains(&Role::Driver) };
	if !roles.node && !roles.driver {
		bail!("--role must include node and/or driver");
	}
	if roles.driver && args.mysql_url.is_none() {
		bail!("the driver role needs --mysql-url (VMON_MYSQL_URL)");
	}

	let data_dir = args.data_dir.clone().unwrap_or_else(default_data_dir);
	fs::create_dir_all(&data_dir).with_context(|| format!("creating {}", data_dir.display()))?;
	let id = load_or_create_node_id(&data_dir)?;
	let secret = match &args.cluster_secret {
		Some(secret) => secret.clone(),
		None => load_or_create_secret(&data_dir)?,
	};

	let advertise_ip = advertise_ip(args.cluster_listen.ip());
	let cluster_url = args.cluster_advertise.clone().unwrap_or_else(|| {
		format!("http://{}", SocketAddr::new(advertise_ip, args.cluster_listen.port()))
	});
	let gossip_advertise = args
		.gossip_advertise
		.unwrap_or_else(|| SocketAddr::new(advertise_ip, args.gossip_listen.port()));
	let public_url = args
		.public_url
		.clone()
		.unwrap_or_else(|| match args.tls_listen {
			Some(_) => format!("https://api.{}", args.domain),
			None => format!("http://{}", SocketAddr::new(advertise_ip, args.listen.port())),
		});

	let meta = MemberMeta {
		roles,
		cluster_url: cluster_url.clone(),
		public_url: roles.driver.then(|| public_url.clone()),
		version: env!("CARGO_PKG_VERSION").to_owned(),
		started_at_ms: now_ms(),
		node: None,
		raft: roles.driver.then(RaftStatus::default),
	};
	let cluster = Cluster::start(
		ClusterConfig {
			id,
			data_dir: data_dir.clone(),
			roles,
			gossip_bind: args.gossip_listen,
			gossip_advertise,
			cluster_url: cluster_url.clone(),
			join: args.join.clone(),
			secret: secret.as_bytes().to_vec(),
			bootstrap_expect: args.bootstrap_expect,
			raft_reap_after: Duration::from_secs(args.raft_reap_after),
		},
		meta,
	)
	.await
	.context("starting cluster layer")?;
	tracing::info!(%id, %cluster_url, gossip = %gossip_advertise, ?roles, "cluster layer started");

	let object_store = match args
		.object_store
		.as_deref()
		.filter(|url| !url.trim().is_empty())
	{
		Some(url) => {
			let client = vmon_cloud::s3::S3Client::from_env(url)
				.await
				.with_context(|| format!("configuring --object-store {url}"))?;
			tracing::info!(store = client.location().url(), "object store configured");
			Some(client)
		},
		None => None,
	};
	let node = if roles.node {
		#[cfg(target_os = "linux")]
		if args.hugepage_pool_mb > 0 {
			reserve_hugepages(args.hugepage_pool_mb / 2);
		}
		#[cfg(not(target_os = "linux"))]
		if args.hugepage_pool_mb > 0 {
			tracing::warn!("--hugepage-pool-mb is only supported on Linux");
		}
		let config = NodeConfig {
			data_dir:                data_dir.clone(),
			run_dir:                 args.run_dir.clone().unwrap_or_else(default_run_dir),
			kernel:                  args.kernel.clone(),
			agent:                   args.agent.clone(),
			vmon_bin:                std::env::current_exe().context("locating the vmon binary")?,
			boot_concurrency:        args.boot_concurrency,
			max_parallel_builds:     args.max_builds,
			net_mode:                args.net.into(),
			net_pool:                args.net_pool.clone(),
			volumes_dir:             args.volumes_dir.clone(),
			shared_volumes:          args.shared_volumes,
			capacity:                CapacityOverrides {
				cpus:      args.max_vcpus,
				memory_mb: args.max_memory_mb,
			},
			cluster_secret:          secret.clone(),
			vmm_user:                args.vmm_user,
			egress_internal_allow:   args.egress_internal_allow.clone(),
			registry_mirror:         args.registry_mirror.clone(),
			object_store:            object_store.clone(),
			lazy_resume:             args.lazy_resume,
			s3_mounts_allow_private: args.s3_mounts_allow_private,
		};
		Some(
			Node::start(config, cluster.clone())
				.await
				.context("starting node role")?,
		)
	} else {
		None
	};

	let driver = if let Some(mysql_url) = args.mysql_url.as_deref().filter(|_| roles.driver) {
		let config = DriverConfig {
			domain: args.domain.clone(),
			public_url,
			admin_token: args.admin_token.clone(),
			cluster_secret: secret.clone(),
			data_dir: data_dir.clone(),
			default_limits: TeamLimits::default(),
			shared_volumes: args.shared_volumes,
			object_store,
		};
		Some(
			Driver::start(config, mysql_url, cluster.clone())
				.await
				.context("starting driver role")?,
		)
	} else {
		None
	};

	let mut cluster_router = cluster.router();
	if let Some(node) = &node {
		cluster_router = cluster_router.merge(node.router());
	}
	let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
	let mut servers = tokio::task::JoinSet::new();
	servers.spawn(serve_plain(args.cluster_listen, cluster_router, shutdown_rx.clone()));
	if node.is_some() {
		tracing::info!(cluster_listen = %args.cluster_listen, "node role serving the node API");
	}
	if let Some(driver) = &driver {
		let router = driver.public_router();
		servers.spawn(vmon_driver::ingress::listener::serve(
			args.listen,
			None,
			router.clone(),
			wait_shutdown(shutdown_rx.clone()),
		));
		if let (Some(addr), Some(cert), Some(key)) = (args.tls_listen, &args.tls_cert, &args.tls_key)
		{
			let tls = TlsFiles { cert: cert.clone(), key: key.clone() };
			servers.spawn(vmon_driver::ingress::listener::serve(
				addr,
				Some(tls),
				router,
				wait_shutdown(shutdown_rx.clone()),
			));
		}
		tracing::info!(listen = %args.listen, domain = %args.domain, "driver serving the E2B API");
	}

	tokio::select! {
		() = signals.recv() => tracing::info!("shutdown requested"),
		Some(result) = servers.join_next() => {
			result.context("listener task panicked")?.context("listener failed")?;
		},
	}
	let _ = shutdown_tx.send(true);
	if let Some(node) = node {
		node.shutdown().await;
	}
	cluster.shutdown().await;
	while servers.join_next().await.is_some() {}
	Ok(())
}

/// Sets the 2 MiB hugetlb pool (`vm.nr_hugepages`) to `pages`; restored
/// sandbox RAM uses it (ARCHITECTURE.md §8) and falls back to 4 KiB pages once
/// it is exhausted. Needs root and free contiguous memory, so the kernel may
/// grant fewer pages; the outcome is logged.
#[cfg(target_os = "linux")]
fn reserve_hugepages(pages: u64) {
	const KNOB: &str = "/proc/sys/vm/nr_hugepages";
	if let Err(error) = fs::write(KNOB, pages.to_string()) {
		tracing::warn!(%error, pages, "cannot reserve 2 MiB pages for sandbox RAM");
		return;
	}
	let granted = fs::read_to_string(KNOB)
		.ok()
		.and_then(|value| value.trim().parse::<u64>().ok())
		.unwrap_or(0);
	if granted < pages {
		tracing::warn!(requested = pages, granted, "kernel granted fewer 2 MiB pages");
	} else {
		tracing::info!(pages, "reserved 2 MiB pages for sandbox RAM");
	}
}

async fn serve_plain(
	addr: SocketAddr,
	router: axum::Router,
	shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
	let listener = tokio::net::TcpListener::bind(addr)
		.await
		.with_context(|| format!("binding {addr}"))?
		// Responses go out in several writes (headers, body chunks); without
		// NODELAY each multi-write response stalls on Nagle + delayed ACK (~40 ms).
		.tap_io(|tcp| {
			let _ = tcp.set_nodelay(true);
		});
	axum::serve(listener, router)
		.with_graceful_shutdown(wait_shutdown(shutdown))
		.await?;
	Ok(())
}

async fn wait_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
	while !*shutdown.borrow() {
		if shutdown.changed().await.is_err() {
			return;
		}
	}
}

/// SIGINT/SIGTERM listeners, registered before startup so a signal that
/// arrives while roles are starting triggers a clean shutdown instead of the
/// default kill.
struct Signals {
	interrupt: Signal,
	terminate: Signal,
}

impl Signals {
	fn install() -> anyhow::Result<Self> {
		Ok(Self {
			interrupt: signal(SignalKind::interrupt()).context("installing SIGINT handler")?,
			terminate: signal(SignalKind::terminate()).context("installing SIGTERM handler")?,
		})
	}

	async fn recv(&mut self) {
		tokio::select! {
			_ = self.interrupt.recv() => {},
			_ = self.terminate.recv() => {},
		}
	}
}

fn init_tracing(filter: &str, json: bool) -> anyhow::Result<()> {
	let filter = tracing_subscriber::EnvFilter::try_new(filter).context("invalid --log filter")?;
	let builder = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr);
	let result = if json {
		builder.json().try_init()
	} else {
		builder.try_init()
	};
	result.map_err(|e| anyhow::anyhow!("initializing logging: {e}"))
}

fn load_or_create_node_id(data_dir: &Path) -> anyhow::Result<NodeId> {
	let path = data_dir.join("node-id");
	match fs::read_to_string(&path) {
		Ok(raw) => raw
			.trim()
			.parse()
			.with_context(|| format!("parsing {}", path.display())),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			let id = NodeId::random();
			write_private(&path, &id.to_string())?;
			Ok(id)
		},
		Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
	}
}

fn load_or_create_secret(data_dir: &Path) -> anyhow::Result<String> {
	let path = data_dir.join("cluster-secret");
	match fs::read_to_string(&path) {
		Ok(raw) => Ok(raw.trim().to_owned()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			let secret = hex::encode(rand::random::<[u8; 32]>());
			write_private(&path, &secret)?;
			tracing::warn!(path = %path.display(), "generated a cluster secret; share it (VMON_CLUSTER_SECRET) with every process of this cluster");
			Ok(secret)
		},
		Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
	}
}

fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
	use std::{io::Write, os::unix::fs::OpenOptionsExt};
	let mut file = fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(path)
		.with_context(|| format!("creating {}", path.display()))?;
	file.write_all(contents.as_bytes())?;
	file.sync_all()?;
	Ok(())
}

/// The bind IP when specific, else the host's outbound IPv4 (loopback when
/// offline).
fn advertise_ip(bind: IpAddr) -> IpAddr {
	if !bind.is_unspecified() {
		return bind;
	}
	UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
		.and_then(|socket| {
			socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9))?;
			socket.local_addr()
		})
		.map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), |addr| addr.ip())
}

fn now_ms() -> u64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.as_millis() as u64)
}
