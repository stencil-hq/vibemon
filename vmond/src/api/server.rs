use std::{
	collections::HashMap, fs, hash::BuildHasher, net::IpAddr, os::unix::fs::PermissionsExt,
	sync::Arc, time::Duration,
};

use tokio::{net::TcpListener, sync::broadcast};

use super::{
	routes,
	state::{ApiState, Transport, UdsPeerListener},
};
use crate::{
	EngineError, Result,
	config::{ServeConfig, resolve_serve_config},
	engine::{Engine, EngineApi},
	function::FunctionDomain,
	home::{Home, OwnerLock},
	mesh::runtime::MeshRuntime,
	net,
	orch::worker::{OrchWorker, OrchWorkerOptions, load_or_create_worker_id},
};

pub fn serve<S>(overrides: HashMap<String, String, S>) -> Result<()>
where
	S: BuildHasher,
{
	let overrides = overrides.into_iter().collect::<HashMap<_, _>>();
	init_logging();
	let config = resolve_serve_config(&overrides)?;
	validate_tcp_auth(&config)?;
	net::configure_broker_socket(config.network_broker_socket.clone());
	net::configure_slot_pool(config.net_slots);
	let home = Home::new(config.home.clone());
	let owner = OwnerLock::acquire(&home)?;
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.map_err(EngineError::from)?;
	let engine = Arc::new(Engine::new(config.clone())?);
	let mesh = MeshRuntime::new(config.clone(), home.clone(), engine.clone())?;
	runtime.block_on(mesh.verify_storage())?;
	let result = runtime.block_on(async {
		prepare_home(&home)?;
		write_pid(&home)?;
		let result = run_listeners(home.clone(), config.clone(), engine.clone(), mesh).await;
		cleanup_files(&home);
		result
	});
	engine.shutdown();
	drop(owner);
	result
}

async fn run_listeners(
	home: Home,
	config: ServeConfig,
	engine: Arc<Engine>,
	mesh: Arc<MeshRuntime>,
) -> Result<()> {
	let uds = bind_uds(&home)?;
	let tcp_listener = if tcp_enabled(&config) {
		let addr = format!("{}:{}", config.host, config.port);
		Some(TcpListener::bind(&addr).await.map_err(EngineError::from)?)
	} else {
		None
	};
	let server_uid = current_uid();
	let uds_listener = UdsPeerListener::new(uds, server_uid);
	let engine_api: Arc<dyn EngineApi> = engine.clone();
	let functions = FunctionDomain::open(home.clone(), engine_api.clone(), &config)?;
	let base_state = ApiState::new(engine_api, functions.clone(), config.clone(), Transport::Unix)
		.with_mesh(mesh.clone());
	let base_state = match start_orch_worker(&home, &config, &base_state)? {
		Some(worker) => base_state.with_orch_worker(worker),
		None => base_state,
	};
	let uds_router = routes::router(base_state.with_transport(Transport::Unix));
	let (shutdown_tx, _) = broadcast::channel::<()>(4);
	let signal_tx = shutdown_tx.clone();
	tokio::spawn(async move {
		wait_for_shutdown_signal().await;
		let _ = signal_tx.send(());
	});
	let _background_tasks = mesh.start_background(&shutdown_tx);
	let _maintenance_task = engine.start_maintenance(shutdown_tx.subscribe());
	let mut tasks = Vec::new();
	let mut uds_shutdown = shutdown_tx.subscribe();
	tasks.push(tokio::spawn(async move {
		axum::serve(uds_listener, uds_router)
			.with_graceful_shutdown(async move {
				let _ = uds_shutdown.recv().await;
			})
			.await
	}));
	if let Some(listener) = tcp_listener {
		let tcp_router = routes::router(base_state.with_transport(Transport::Tcp));
		let mut tcp_shutdown = shutdown_tx.subscribe();
		tasks.push(tokio::spawn(async move {
			axum::serve(listener, tcp_router)
				.with_graceful_shutdown(async move {
					let _ = tcp_shutdown.recv().await;
				})
				.await
		}));
	}
	let mut first_error = None;
	for task in tasks {
		match task.await {
			Ok(Ok(())) => {},
			Ok(Err(err)) => {
				if first_error.is_none() {
					first_error = Some(EngineError::from(err));
				}
			},
			Err(err) => {
				if first_error.is_none() {
					first_error = Some(EngineError::engine(format!("server task failed: {err}")));
				}
			},
		}
	}
	functions.shutdown().await;
	if let Some(err) = first_error {
		Err(err)
	} else {
		Ok(())
	}
}

/// When `orch_redis` is configured, build this node's [`OrchWorker`] and
/// start its heartbeat publisher on the current runtime.
fn start_orch_worker(
	home: &Home,
	config: &ServeConfig,
	state: &ApiState,
) -> Result<Option<Arc<OrchWorker>>> {
	let Some(redis_url) = config.orch_redis.clone() else {
		return Ok(None);
	};
	let url = match config.orch_url.clone() {
		Some(url) => url,
		None if tcp_enabled(config) => {
			crate::mesh::state::default_advertise(&config.host, config.port)
		},
		None => {
			return Err(EngineError::invalid("orch mode requires --orch-url or a TCP listener"));
		},
	};
	let wid = match config.orch_id.clone() {
		Some(wid) => wid,
		None => load_or_create_worker_id(home)?,
	};
	let caps = crate::mesh::state::probe_caps();
	let (backend, arch) = crate::mesh::state::probe_compat();
	let worker = OrchWorker::new(OrchWorkerOptions {
		redis_url,
		wid,
		url,
		arch,
		backend,
		caps: crate::orch::Resources { vcpus: caps.vcpus, mem_mib: caps.mem_mib },
		heartbeat: Duration::from_secs_f64(config.orch_heartbeat_sec),
		dead_after: Duration::from_secs_f64(config.orch_dead_after_sec),
		max_sandboxes: config.orch_max_sandboxes,
		memory_reserve_mib: config.orch_memory_reserve_mib,
	})?;
	drop(worker.spawn(state.engine.clone()));
	Ok(Some(worker))
}

/// Install the process-wide tracing subscriber for `vmon serve`.
///
/// Defaults to `info` (mesh replication and reconcile events log at
/// `info`/`warn`); `RUST_LOG` overrides. `try_init` keeps embedded/test
/// callers that already installed a subscriber working.
fn init_logging() {
	let filter = tracing_subscriber::EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
	let _ = tracing_subscriber::fmt()
		.with_env_filter(filter)
		.with_writer(std::io::stderr)
		.try_init();
}

fn validate_tcp_auth(config: &ServeConfig) -> Result<()> {
	if tcp_enabled(config)
		&& config
			.token
			.as_deref()
			.unwrap_or_default()
			.trim()
			.is_empty()
		&& !is_loopback_host(&config.host)
	{
		return Err(EngineError::invalid(
			"refusing non-loopback TCP bind without --token or VMON_API_TOKEN",
		));
	}
	if config.tls_cert.is_some() != config.tls_key.is_some() {
		return Err(EngineError::invalid("vmon serve TLS requires both tls_cert and tls_key"));
	}
	Ok(())
}

fn tcp_enabled(config: &ServeConfig) -> bool {
	!config.host.trim().is_empty()
}

fn is_loopback_host(host: &str) -> bool {
	let host = host.trim();
	if host.eq_ignore_ascii_case("localhost") {
		return true;
	}
	host.parse::<IpAddr>().is_ok_and(|addr| addr.is_loopback())
}

fn bind_uds(home: &Home) -> Result<tokio::net::UnixListener> {
	let sock = home.vmond_sock();
	match fs::remove_file(&sock) {
		Ok(()) => {},
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
		Err(err) => return Err(err.into()),
	}
	let listener = tokio::net::UnixListener::bind(&sock).map_err(EngineError::from)?;
	let mut perms = fs::metadata(&sock)?.permissions();
	perms.set_mode(0o600);
	fs::set_permissions(&sock, perms)?;
	Ok(listener)
}

fn prepare_home(home: &Home) -> Result<()> {
	fs::create_dir_all(home.root())?;
	let mut perms = fs::metadata(home.root())?.permissions();
	perms.set_mode(0o700);
	fs::set_permissions(home.root(), perms)?;
	Ok(())
}

fn write_pid(home: &Home) -> Result<()> {
	fs::write(home.vmond_pid(), format!("{}\n", std::process::id())).map_err(EngineError::from)
}

fn cleanup_files(home: &Home) {
	for path in [home.vmond_sock(), home.vmond_pid()] {
		match fs::remove_file(path) {
			Ok(()) => {},
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
			Err(err) => tracing::warn!("failed to remove server file: {err}"),
		}
	}
}

async fn wait_for_shutdown_signal() {
	#[cfg(unix)]
	{
		let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
		if let Ok(mut term) = term {
			tokio::select! {
				_ = tokio::signal::ctrl_c() => {},
				_ = term.recv() => {},
			}
			return;
		}
	}
	let _ = tokio::signal::ctrl_c().await;
}

fn current_uid() -> u32 {
	// SAFETY: `geteuid` takes no pointers and cannot violate Rust memory safety.
	unsafe { libc::geteuid() }
}
