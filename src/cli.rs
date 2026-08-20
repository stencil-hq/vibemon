use std::{
	collections::HashMap,
	fs,
	io::{self, Cursor, IsTerminal, Read, Write},
	path::{Path, PathBuf},
	process::{Command, Stdio},
	sync::{
		atomic::{AtomicBool, Ordering},
		mpsc,
	},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use prost::Message;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use vmon_proto::v1 as pb;

use crate::{
	contexts::{
		Context, ContextStore, LOCAL, normalize_server, now_secs, remote_client_from_context,
		roster_from_status,
	},
	error::{CliError, Result, err},
	transport::{ApiClient, Grpc, status_error},
};

#[derive(Clone, Debug, Default)]
pub struct TransportOptions {
	context: Option<String>,
	token:   Option<String>,
}

#[derive(Parser)]
#[command(
	name = "vmon",
	version,
	about = "Vibemon microVM CLI",
	arg_required_else_help = true,
	after_help = "Global transport options (place before the subcommand):\n  --context NAME   Use \
	              a saved remote context, or 'local' for the UDS daemon\n  --token TOKEN    Bearer \
	              token for the selected remote context"
)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
#[allow(
	clippy::large_enum_variant,
	reason = "clap parses this enum once and boxing ServeArgs would add indirection without \
	          runtime benefit"
)]
enum Commands {
	/// Boot an image as a sandbox and stream its output unless detached.
	Run(RunArgs),
	/// Package, register, and atomically activate a durable application.
	Deploy(DeployArgs),
	/// Inspect and enter durable function revisions.
	Function {
		#[command(subcommand)]
		command: FunctionCommands,
	},
	/// Publish derived artifacts for non-OCI images.
	Image {
		#[command(subcommand)]
		command: ImageCommands,
	},
	/// Inspect durable calls and their reconnectable logs.
	Call {
		#[command(subcommand)]
		command: CallCommands,
	},
	/// List sandboxes.
	Ps,
	/// Run a command inside an agent-enabled sandbox.
	Exec(ExecArgs),
	/// Run or attach to an interactive shell.
	Shell(ShellArgs),
	/// Copy a file between host and guest.
	Cp(CpArgs),
	/// Show a sandbox console log.
	Logs(LogsArgs),
	/// Stop a running sandbox but keep its record.
	Stop(NameArg),
	/// Remove a sandbox and its state.
	Rm(NameArg),
	/// Pause a running sandbox.
	Pause(NameArg),
	/// Resume a paused sandbox.
	Resume(NameArg),
	/// Durably checkpoint a sandbox and release its live VM.
	Suspend(NameArg),
	/// List retained recovery points for a sandbox.
	History(NameArg),
	/// Restore a sandbox identity to a retained recovery point.
	Rollback(RollbackArgs),
	/// Reset a sandbox timeout deadline.
	Extend(ExtendArgs),
	/// Snapshot machine state into a named template.
	Snapshot(SnapshotArgs),
	/// Restore a sandbox from a snapshot.
	Restore(RestoreArgs),
	/// Fork copy-on-write clones from a snapshot.
	Fork(ForkArgs),
	/// Show live sandbox metrics.
	Stats(NameArg),
	/// Show Prometheus metrics, or sandbox metrics when NAME is supplied.
	Metrics(MetricsArgs),
	/// Guest filesystem operations.
	Fs {
		#[command(subcommand)]
		command: FsCommands,
	},
	/// Named volume operations.
	Volume {
		#[command(subcommand)]
		command: VolumeCommands,
	},
	/// Warm pool operations.
	Pool {
		#[command(subcommand)]
		command: PoolCommands,
	},
	/// Diagnose local prerequisites.
	Doctor(DoctorArgs),
	/// Manage the local daemon process.
	Daemon {
		#[command(subcommand)]
		command: DaemonCommands,
	},
	/// Serve the gRPC control plane and web/HTTP endpoints.
	Serve(ServeArgs),
	/// Serve the horizontally-scalable sandbox scheduler (orch layer).
	Sched(SchedArgs),
	/// Benchmark sandbox launch throughput against a scheduler or worker.
	Bench(BenchArgs),
	/// Run the narrow privileged Linux network broker.
	NetBroker(NetBrokerArgs),
	/// Manage remote API contexts.
	Context {
		#[command(subcommand)]
		command: ContextCommands,
	},
	/// Mesh cluster operations.
	Mesh {
		#[command(subcommand)]
		command: MeshCommands,
	},
	/// Print a shell completion script.
	Completion(CompletionArgs),
}

#[derive(Args)]
struct DeployArgs {
	/// Python application file, optionally followed by `::qualname`.
	target: String,
}

#[derive(Subcommand)]
enum FunctionCommands {
	/// List immutable function revisions.
	Ls,
	/// Resolve a current or pinned function and open an interactive worker
	/// shell.
	Shell { reference: String },
}

fn cmd_image(command: ImageCommands) -> Result<i32> {
	match command {
		ImageCommands::PublishGceRootfs { reference } => {
			let published = vmond::image::publish_gce_rootfs(&reference)?;
			let action = if published.skipped {
				"reused"
			} else {
				"published"
			};
			println!(
				"{action} {} compressed_size={} uncompressed_size={} original_size={} sha256={} \
				 sidecar={}",
				published.object,
				published.compressed_size,
				published.uncompressed_size,
				published.original_size,
				published.sha256,
				published.sidecar
			);
			Ok(0)
		},
	}
}

#[derive(Subcommand)]
enum ImageCommands {
	/// Publish a GCE export's ext4 root as a range-addressable GCS object.
	PublishGceRootfs { reference: String },
}

#[derive(Subcommand)]
enum CallCommands {
	/// Print a durable call and its result or structured error.
	Get { id: String },
	/// Stream call logs, reconnecting from the last committed sequence.
	Logs {
		id:     String,
		#[arg(short, long)]
		follow: bool,
	},
}

#[derive(Args)]
struct RunArgs {
	/// OCI/container image reference.
	image:         Option<String>,
	/// Command to run in the sandbox.
	#[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "CMD")]
	cmd:           Vec<String>,
	/// Dockerfile to build server-side before running.
	#[arg(short = 'f', long)]
	dockerfile:    Option<String>,
	/// Dockerfile build context.
	#[arg(long = "context", default_value = ".")]
	build_context: String,
	/// Sandbox name.
	#[arg(long)]
	name:          Option<String>,
	/// Guest RAM in MiB.
	#[arg(long, default_value_t = 512)]
	mem:           u32,
	/// vCPU count.
	#[arg(long, default_value_t = 1)]
	cpus:          u32,
	/// Sandbox disk size in MiB.
	#[arg(long, default_value_t = 1024)]
	disk_mb:       u32,
	/// Create timeout in seconds.
	#[arg(long, default_value_t = 300.0)]
	timeout:       f64,
	/// Leave the sandbox running in the background.
	#[arg(short, long)]
	detach:        bool,
	/// Boot without networking.
	#[arg(long)]
	block_network: bool,
	/// Request an owner architecture.
	#[arg(long, value_enum)]
	arch:          Option<Arch>,
}

#[derive(Args)]
struct ExecArgs {
	name: String,
	/// Allocate a PTY and stream over the interactive exec protocol.
	#[arg(short = 't', long)]
	tty:  bool,
	/// Command to run.
	#[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "CMD")]
	cmd:  Vec<String>,
}

#[derive(Args)]
struct ShellArgs {
	/// Running sandbox, snapshot, or image reference.
	reference: Option<String>,
	/// One-off command. Without this, an interactive shell is started.
	#[arg(short = 'c', long = "cmd")]
	command:   Option<String>,
	/// Image for a fresh shell if REF is omitted.
	#[arg(long)]
	image:     Option<String>,
	/// Environment variable KEY=VALUE; bare KEY copies from the host.
	#[arg(short = 'e', long = "env", value_name = "KEY=VALUE")]
	env:       Vec<String>,
	#[arg(long, default_value_t = 512)]
	mem:       u32,
	#[arg(long, default_value_t = 1)]
	cpus:      u32,
	#[arg(long, default_value_t = 1024)]
	disk_mb:   u32,
	#[arg(long, default_value_t = 300.0)]
	timeout:   f64,
	/// Force PTY allocation on.
	#[arg(long = "pty", conflicts_with = "no_pty")]
	pty:       bool,
	/// Force PTY allocation off.
	#[arg(long = "no-pty")]
	no_pty:    bool,
}

#[derive(Args)]
struct CpArgs {
	src: String,
	dst: String,
}

#[derive(Args)]
struct LogsArgs {
	name:   String,
	#[arg(short, long)]
	follow: bool,
}

#[derive(Args)]
struct NameArg {
	name: String,
}

#[derive(Args)]
struct RollbackArgs {
	name:           String,
	recovery_point: String,
}

#[derive(Args)]
struct ExtendArgs {
	name: String,
	secs: u64,
}

#[derive(Args)]
struct SnapshotArgs {
	name:     String,
	snapshot: String,
	#[arg(long)]
	stop:     bool,
}

#[derive(Args)]
struct RestoreArgs {
	snapshot: String,
	#[arg(long)]
	name:     Option<String>,
	#[arg(long)]
	agent:    bool,
	#[arg(short, long)]
	detach:   bool,
	#[arg(long, value_enum)]
	arch:     Option<Arch>,
}

#[derive(Args)]
struct ForkArgs {
	snapshot: String,
	#[arg(long, default_value_t = 2)]
	count:    u32,
	#[arg(long, value_enum)]
	arch:     Option<Arch>,
}

#[derive(Args)]
struct MetricsArgs {
	name: Option<String>,
}

#[derive(Subcommand)]
enum FsCommands {
	/// List a guest directory: NAME[:PATH].
	List { reference: String },
	/// Stat a guest path: NAME[:PATH].
	Stat { reference: String },
}

#[derive(Subcommand)]
enum VolumeCommands {
	/// List named volumes.
	Ls,
	/// Remove a named volume.
	Rm { name: String },
}

#[derive(Subcommand)]
enum PoolCommands {
	/// List warm pools.
	Ls,
	/// Set or resize a warm pool.
	Set(PoolSetArgs),
	/// Delete a warm pool.
	Rm { reference: String },
}

#[derive(Args)]
struct PoolSetArgs {
	reference:     String,
	#[arg(long)]
	size:          u32,
	#[arg(long)]
	image:         Option<String>,
	#[arg(long)]
	template:      Option<String>,
	#[arg(long)]
	mem:           Option<u32>,
	#[arg(long)]
	cpus:          Option<u32>,
	#[arg(long)]
	disk_mb:       Option<u32>,
	#[arg(long)]
	block_network: bool,
}

#[derive(Args)]
struct DoctorArgs {
	#[arg(long)]
	serve:       bool,
	#[arg(long = "config")]
	config_path: Option<PathBuf>,
}

#[derive(Subcommand)]
enum DaemonCommands {
	/// Show local daemon health.
	Status,
	/// Stop the local daemon with SIGTERM.
	Stop,
}

#[derive(Args, Default)]
struct ServeArgs {
	/// Optional Python application target to deploy before serving.
	target: Option<String>,
	/// Redeploy changed function inputs while the daemon is running.
	#[arg(long, requires = "target")]
	watch: bool,
	#[arg(long = "config")]
	config_path: Option<PathBuf>,
	#[arg(long)]
	home: Option<PathBuf>,
	#[arg(long)]
	host: Option<String>,
	#[arg(long)]
	port: Option<u16>,
	#[arg(long)]
	token: Option<String>,
	#[arg(long)]
	client_token: Option<String>,
	/// JSON map from tenant bearer tokens to tenant IDs.
	#[arg(long)]
	tenant_tokens: Option<String>,
	/// JSON map from tenant IDs to customer-managed encryption key IDs.
	#[arg(long)]
	tenant_keys: Option<String>,
	#[arg(long)]
	tls_cert: Option<String>,
	#[arg(long)]
	tls_key: Option<String>,
	#[arg(long)]
	idle_timeout: Option<f64>,
	#[arg(long)]
	history_disk_sec: Option<f64>,
	#[arg(long)]
	history_checkpoint_sec: Option<f64>,
	#[arg(long)]
	history_retention: Option<usize>,
	#[arg(long)]
	history_max_age_sec: Option<f64>,
	#[arg(long)]
	template_ttl_sec: Option<f64>,
	/// Maximum uncompressed function artifact size accepted by the daemon.
	#[arg(long, value_parser = clap::value_parser!(u64).range(1..=1_125_899_906_842_624))]
	function_artifact_max_bytes: Option<u64>,
	#[arg(long)]
	replicate_sec: Option<f64>,
	#[arg(long)]
	replicas: Option<usize>,
	#[arg(long)]
	replicate_concurrency: Option<usize>,
	#[arg(long = "restore-quorum", conflicts_with = "no_restore_quorum")]
	restore_quorum: bool,
	#[arg(long = "no-restore-quorum")]
	no_restore_quorum: bool,
	#[arg(long)]
	warm_pool_size: Option<usize>,
	#[arg(long)]
	warm_images: Option<String>,
	#[arg(long)]
	mesh_heartbeat_sec: Option<f64>,
	#[arg(long)]
	mesh_reap_sec: Option<f64>,
	#[arg(long)]
	mesh_idem_ttl_sec: Option<f64>,
	#[arg(long)]
	mesh_create_timeout_sec: Option<f64>,
	#[arg(long)]
	mesh_w_warm: Option<f64>,
	#[arg(long)]
	mesh_w_free: Option<f64>,
	#[arg(long)]
	mesh_w_local: Option<f64>,
	#[arg(long)]
	mesh_w_region: Option<f64>,
	#[arg(long)]
	mesh_w_inflight: Option<f64>,
	#[arg(long)]
	orch_redis: Option<String>,
	#[arg(long)]
	orch_url: Option<String>,
	/// Host IP that per-sandbox TCP tunnels bind.
	#[arg(long)]
	tunnel_bind: Option<String>,
	#[arg(long)]
	orch_id: Option<String>,
	#[arg(long)]
	orch_heartbeat_sec: Option<f64>,
	#[arg(long)]
	orch_dead_after_sec: Option<f64>,
	#[arg(long)]
	orch_max_sandboxes: Option<u64>,
	/// Maximum concurrently booting sandboxes (0 = 4x host CPUs).
	#[arg(long)]
	boot_concurrency: Option<usize>,
}
#[derive(Args)]
struct NetBrokerArgs {
	/// Mode-0600 Unix socket served by the broker.
	#[arg(long)]
	socket:    PathBuf,
	/// UID allowed to connect to this mode-0600 socket; requires root when it
	/// differs from the broker UID.
	#[arg(long, value_parser = clap::value_parser!(u32).range(0..=4_294_967_294))]
	owner_uid: Option<u32>,
}

#[derive(Subcommand)]
enum ContextCommands {
	/// Add a context from a gateway URL.
	#[command(alias = "create")]
	Add(ContextAddArgs),
	/// List configured contexts.
	Ls,
	/// Switch the active context.
	Use { name: String },
	/// Remove a context and saved token.
	Rm { name: String },
}

#[derive(Args)]
struct ContextAddArgs {
	name:       String,
	#[arg(short, long)]
	server:     String,
	#[arg(long)]
	token:      Option<String>,
	#[arg(long)]
	save_token: bool,
	#[arg(long, default_value = "")]
	region:     String,
}

#[derive(Subcommand)]
enum MeshCommands {
	/// Initialize this node as a cluster and print a join blob.
	Setup(MeshSetupArgs),
	/// Join an existing cluster.
	Join(MeshJoinArgs),
	/// Leave the cluster.
	Leave(MeshLeaveArgs),
	/// Migrate a running sandbox to another mesh node.
	Migrate(MeshMigrateArgs),
	/// Show cluster status.
	Status,
}

#[derive(Args)]
struct MeshSetupArgs {
	#[arg(long)]
	advertise:   Option<String>,
	#[arg(long, default_value = "")]
	region:      String,
	#[arg(long)]
	max_vcpus:   Option<u32>,
	#[arg(long)]
	max_mem_mib: Option<u32>,
}

#[derive(Args)]
struct MeshJoinArgs {
	blob:      String,
	#[arg(long)]
	advertise: Option<String>,
	#[arg(long, default_value = "")]
	region:    String,
}

#[derive(Args)]
struct MeshMigrateArgs {
	name: String,
	node: String,
}

#[derive(Args)]
struct MeshLeaveArgs {
	#[arg(long)]
	drain: bool,
}

#[derive(Args)]
struct CompletionArgs {
	shell: CompletionShell,
}

#[derive(Clone, ValueEnum)]
enum Arch {
	#[value(name = "x86_64")]
	X86_64,
	Aarch64,
}

#[derive(Clone, ValueEnum)]
enum CompletionShell {
	Bash,
	Zsh,
	Fish,
}

pub fn run(raw_args: Vec<String>) -> Result<i32> {
	let (transport_options, args) = extract_transport_options(raw_args)?;
	let cli = Cli::parse_from(args);
	execute(cli.command, &transport_options)
}

fn execute(command: Commands, transport_options: &TransportOptions) -> Result<i32> {
	match command {
		Commands::Run(args) => cmd_run(args, transport_options),
		Commands::Deploy(args) => cmd_deploy(args, transport_options),
		Commands::Function { command } => cmd_function(command, transport_options),
		Commands::Image { command } => cmd_image(command),
		Commands::Call { command } => cmd_call(command, transport_options),
		Commands::Ps => cmd_ps(transport_options),
		Commands::Exec(args) => cmd_exec(args, transport_options),
		Commands::Shell(args) => cmd_shell(args, transport_options),
		Commands::Cp(args) => cmd_cp(args, transport_options),
		Commands::Logs(args) => cmd_logs(args, transport_options),
		Commands::Stop(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Stop, "stopped")
		},
		Commands::Rm(args) => cmd_rm(args, transport_options),
		Commands::Pause(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Pause, "paused")
		},
		Commands::Resume(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Resume, "resumed")
		},
		Commands::Suspend(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Suspend, "suspended")
		},
		Commands::History(args) => cmd_history(args, transport_options),
		Commands::Rollback(args) => cmd_rollback(args, transport_options),
		Commands::Extend(args) => cmd_extend(args, transport_options),
		Commands::Snapshot(args) => cmd_snapshot(args, transport_options),
		Commands::Restore(args) => cmd_restore(args, transport_options),
		Commands::Fork(args) => cmd_fork(args, transport_options),
		Commands::Stats(args) => cmd_stats(&args.name, transport_options),
		Commands::Metrics(args) => cmd_metrics(args, transport_options),
		Commands::Fs { command } => cmd_fs(command, transport_options),
		Commands::Volume { command } => cmd_volume(command, transport_options),
		Commands::Pool { command } => cmd_pool(command, transport_options),
		Commands::Doctor(args) => Ok(cmd_doctor(args)),
		Commands::Daemon { command } => cmd_daemon(command),
		Commands::Serve(args) => cmd_serve(args, transport_options),
		Commands::Sched(args) => cmd_sched(args),
		Commands::NetBroker(args) => cmd_net_broker(args),
		Commands::Context { command } => cmd_context(command),
		Commands::Bench(args) => cmd_bench(args),
		Commands::Mesh { command } => cmd_mesh(command, transport_options),
		Commands::Completion(args) => Ok(cmd_completion(args)),
	}
}

/// `vmon bench` drives production create or snapshot-fork RPCs and reports
/// throughput, latency percentiles, placement distribution, and error taxonomy.
#[derive(Args)]
struct BenchArgs {
	/// Scheduler (or worker) base URL, e.g. <http://host:8100>.
	#[arg(long)]
	server:               String,
	/// Bearer token (falls back to $`VMON_API_TOKEN`).
	#[arg(long)]
	token:                Option<String>,
	/// Total measured requests.
	#[arg(long, conflicts_with = "target_rps")]
	count:                Option<usize>,
	/// Concurrent in-flight requests in ordinary closed-loop mode.
	#[arg(long, default_value_t = 32)]
	concurrency:          usize,
	/// Client HTTP/2 connections (0 = size automatically from the load).
	#[arg(long, default_value_t = 0)]
	connections:          usize,
	/// Open-loop schedule→drain→report→cleanup repetitions.
	#[arg(long, default_value_t = 1)]
	waves:                usize,
	/// Guest image.
	#[arg(long, default_value = "alpine")]
	image:                String,
	/// Named snapshot to fork instead of creating from an image.
	#[arg(long, conflicts_with = "target_rps")]
	snapshot:             Option<String>,
	/// Guest memory in MiB.
	#[arg(long, default_value_t = 128)]
	memory:               u32,
	/// Guest vCPUs.
	#[arg(long, default_value_t = 1)]
	cpus:                 u32,
	/// Per-request deadline in seconds.
	#[arg(long, default_value_t = 120.0)]
	timeout_sec:          f64,
	/// Untimed warmup requests.
	#[arg(long, default_value_t = 4)]
	warmup:               usize,
	/// Full-scale create rate mapped onto live scheduler memory.
	#[arg(long, requires = "reference_memory_mib")]
	target_rps:           Option<f64>,
	/// Full-scale memory capacity represented by --target-rps.
	#[arg(long, visible_alias = "capacity-memory-mib", requires = "target_rps")]
	reference_memory_mib: Option<u64>,
	/// Fraction of current memory capacity available to the burst.
	#[arg(long, visible_alias = "headroom", default_value_t = 0.8)]
	memory_headroom:      f64,
	/// Keep launched sandboxes instead of removing them.
	#[arg(long)]
	keep:                 bool,
	/// Emit a machine-readable JSON summary line.
	#[arg(long)]
	json:                 bool,
}
fn cmd_bench(args: BenchArgs) -> Result<i32> {
	let scaled = match (args.target_rps, args.reference_memory_mib) {
		(Some(target_rps), Some(reference_memory_mib)) => Some(crate::bench::ScaledBurstOptions {
			target_rps,
			reference_memory_mib,
			headroom: args.memory_headroom,
		}),
		(None, None) => None,
		_ => {
			return Err(CliError::new(
				"--target-rps and --reference-memory-mib must be supplied together",
			));
		},
	};
	crate::bench::run(crate::bench::BenchOptions {
		server: args.server,
		token: args.token.or_else(|| {
			std::env::var("VMON_API_TOKEN")
				.ok()
				.filter(|value| !value.is_empty())
		}),
		count: args.count.unwrap_or(200).max(1),
		concurrency: args.concurrency.max(1),
		connections: args.connections,
		waves: args.waves.max(1),
		image: args.image,
		memory: args.memory.max(32),
		cpus: args.cpus.max(1),
		timeout: Duration::from_secs_f64(args.timeout_sec.max(1.0)),
		warmup: args.warmup,
		keep: args.keep,
		json: args.json,
		scaled,
		snapshot: args.snapshot,
	})
}

/// `vmon sched` flags: the scheduler front of the orchestration layer.
/// Workers join by running `vmon serve --orch-redis <same-redis>`.
#[derive(Args)]
struct SchedArgs {
	/// TCP bind address.
	#[arg(long, default_value = "127.0.0.1:8100")]
	listen:                 String,
	/// Redis URL shared with workers (falls back to $`VMON_ORCH_REDIS`);
	/// omitted spawns an embedded dev-only instance and prints its URL.
	#[arg(long)]
	redis:                  Option<String>,
	/// Inbound bearer token, required for non-loopback binds (falls back to
	/// $`VMON_API_TOKEN`).
	#[arg(long)]
	token:                  Option<String>,
	/// Bearer token presented to workers — their `--token` (falls back to
	/// $`VMON_WORKER_TOKEN`).
	#[arg(long)]
	worker_token:           Option<String>,
	/// Worker liveness window in seconds (match workers' --orch-dead-after-sec).
	#[arg(long, default_value_t = 5.0)]
	dead_after_sec:         f64,
	/// Deadline for one worker create attempt, in seconds.
	#[arg(long, default_value_t = 60.0)]
	create_timeout_sec:     f64,
	/// Additional placement attempts after a rejected create.
	#[arg(long, default_value_t = 3)]
	create_retries:         usize,
	/// Autoscaler: minimum worker count.
	#[arg(long, default_value_t = 0)]
	autoscale_min:          u64,
	/// Autoscaler: maximum worker count (0 disables the autoscaler).
	#[arg(long, default_value_t = 0)]
	autoscale_max:          u64,
	/// Autoscaler: target memory utilization (0, 1].
	#[arg(long, default_value_t = 0.7)]
	autoscale_target_util:  f64,
	/// Autoscaler: decision cadence in seconds.
	#[arg(long, default_value_t = 15.0)]
	autoscale_interval_sec: f64,
	/// Hook run to add workers (env: `VMON_SCALE_DESIRED/DELTA/LIVE`).
	#[arg(long)]
	scale_up_cmd:           Option<String>,
	/// Hook run after draining workers (env adds `VMON_DRAIN_WIDS`).
	#[arg(long)]
	scale_down_cmd:         Option<String>,
}

fn cmd_sched(args: SchedArgs) -> Result<i32> {
	let seconds = |value: f64| Duration::from_secs_f64(value.max(0.1));
	let autoscaler = (args.autoscale_max > 0).then(|| vmond::orch::autoscaler::AutoscalerOptions {
		min: args.autoscale_min,
		max: args.autoscale_max,
		target_util: args.autoscale_target_util.clamp(0.05, 1.0),
		interval: seconds(args.autoscale_interval_sec),
		scale_up_cmd: args.scale_up_cmd,
		scale_down_cmd: args.scale_down_cmd,
		..Default::default()
	});
	let env_fallback = |explicit: Option<String>, var: &str| {
		explicit.or_else(|| std::env::var(var).ok().filter(|value| !value.is_empty()))
	};
	vmond::orch::server::serve_scheduler(vmond::orch::server::SchedulerOptions {
		listen: args.listen,
		redis_url: env_fallback(args.redis, "VMON_ORCH_REDIS"),
		token: env_fallback(args.token, "VMON_API_TOKEN"),
		worker_token: env_fallback(args.worker_token, "VMON_WORKER_TOKEN"),
		dead_after: seconds(args.dead_after_sec),
		create_timeout: seconds(args.create_timeout_sec),
		create_retries: args.create_retries,
		autoscaler,
	})?;
	Ok(0)
}

fn cmd_net_broker(args: NetBrokerArgs) -> Result<i32> {
	vmond::net::broker::serve(&args.socket, args.owner_uid)?;
	Ok(0)
}

fn extract_transport_options(raw_args: Vec<String>) -> Result<(TransportOptions, Vec<String>)> {
	let mut out = Vec::new();
	let mut options = TransportOptions::default();
	let mut iter = raw_args.into_iter();
	if let Some(argv0) = iter.next() {
		out.push(argv0);
	}
	while let Some(arg) = iter.next() {
		if arg == "--context" {
			options.context = Some(
				iter
					.next()
					.ok_or_else(|| CliError::new("--context requires a value"))?,
			);
			continue;
		}
		if let Some(value) = arg.strip_prefix("--context=") {
			options.context = Some(value.to_owned());
			continue;
		}
		if arg == "--token" {
			options.token = Some(
				iter
					.next()
					.ok_or_else(|| CliError::new("--token requires a value"))?,
			);
			continue;
		}
		if let Some(value) = arg.strip_prefix("--token=") {
			options.token = Some(value.to_owned());
			continue;
		}
		out.push(arg);
		out.extend(iter);
		return Ok((options, out));
	}
	Ok((options, out))
}

fn client(options: &TransportOptions, autostart: bool) -> Result<ApiClient> {
	let store = ContextStore::load_default();
	let target = options.context.clone().or_else(|| store.current_name());
	let Some(name) = target.filter(|name| name != LOCAL) else {
		return Ok(ApiClient::local(autostart));
	};
	let context = store
		.get(&name)
		.ok_or_else(|| CliError::new(format!("no such context {name:?}")))?;
	let token = store.resolve_token(&name, options.token.as_deref())?;
	remote_client_from_context(context, token)
}

const INSPECTION_MAGIC: &[u8; 8] = b"VMONCLI1";
const ARTIFACT_CHUNK_SIZE: usize = 1024 * 1024;

struct InspectedFunction {
	binding: String,
	package: Vec<u8>,
	spec:    pb::FunctionSpec,
}

struct InspectedTarget {
	namespace: String,
	app:       String,
	functions: Vec<InspectedFunction>,
}

fn read_inspection_field(cursor: &mut Cursor<Vec<u8>>, label: &str) -> Result<Vec<u8>> {
	let mut size = [0_u8; 4];
	cursor
		.read_exact(&mut size)
		.map_err(|_| CliError::new(format!("inspection output truncated before {label}")))?;
	let size = u32::from_be_bytes(size) as usize;
	let mut value = vec![0; size];
	cursor
		.read_exact(&mut value)
		.map_err(|_| CliError::new(format!("inspection output truncated in {label}")))?;
	Ok(value)
}

fn inspect_target(target: &str) -> Result<InspectedTarget> {
	let path = target.split_once("::").map_or(target, |(path, _)| path);
	if !Path::new(path).is_file() {
		return err(format!("target is not an existing file: {path}"));
	}
	let python = std::env::var_os("VMON_PYTHON").unwrap_or_else(|| "python3".into());
	let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/py");
	let existing = std::env::var_os("PYTHONPATH").unwrap_or_default();
	let mut pythonpath = sdk.into_os_string();
	if !existing.is_empty() {
		pythonpath.push(":");
		pythonpath.push(existing);
	}
	let output = Command::new(python)
		.args(["-m", "vmon._cli_inspect", target])
		.env("PYTHONPATH", pythonpath)
		.stdin(Stdio::null())
		.output()
		.map_err(|error| CliError::new(format!("failed to start target inspector: {error}")))?;
	if !output.status.success() {
		let detail = String::from_utf8_lossy(&output.stderr);
		return err(detail.trim().to_owned());
	}
	let mut cursor = Cursor::new(output.stdout);
	let mut magic = [0_u8; 8];
	cursor
		.read_exact(&mut magic)
		.map_err(|_| CliError::new("target inspector returned an empty response"))?;
	if &magic != INSPECTION_MAGIC {
		return err("target inspector returned an unsupported response");
	}
	let namespace = String::from_utf8(read_inspection_field(&mut cursor, "namespace")?)
		.map_err(|error| CliError::new(format!("inspection namespace is not UTF-8: {error}")))?;
	let app = String::from_utf8(read_inspection_field(&mut cursor, "app name")?)
		.map_err(|error| CliError::new(format!("inspection app name is not UTF-8: {error}")))?;
	let mut count = [0_u8; 4];
	cursor
		.read_exact(&mut count)
		.map_err(|_| CliError::new("inspection output omitted definition count"))?;
	let mut functions = Vec::with_capacity(u32::from_be_bytes(count) as usize);
	for _ in 0..u32::from_be_bytes(count) {
		let binding = String::from_utf8(read_inspection_field(&mut cursor, "binding")?)
			.map_err(|error| CliError::new(format!("inspection binding is not UTF-8: {error}")))?;
		let package = read_inspection_field(&mut cursor, "package")?;
		let encoded = read_inspection_field(&mut cursor, "function spec")?;
		let spec = pb::FunctionSpec::decode(encoded.as_slice())
			.map_err(|error| CliError::new(format!("invalid function protobuf: {error}")))?;
		functions.push(InspectedFunction { binding, package, spec });
	}
	if cursor.position() != cursor.get_ref().len() as u64 {
		return err("target inspector returned trailing data");
	}
	Ok(InspectedTarget { namespace, app, functions })
}

fn sha256_digest(data: &[u8]) -> pb::Digest {
	pb::Digest {
		algorithm: pb::DigestAlgorithm::Sha256 as i32,
		value:     Sha256::digest(data).to_vec(),
	}
}

fn artifact_ref(data: &[u8]) -> pb::ArtifactRef {
	pb::ArtifactRef { digest: Some(sha256_digest(data)) }
}

fn upload_artifact(grpc: &Grpc, data: &[u8]) -> Result<pb::ArtifactRef> {
	let artifact = artifact_ref(data);
	let mut artifacts = grpc.artifacts();
	match grpc.block_on(artifacts.stat(artifact.clone())) {
		Ok(_) => return Ok(artifact),
		Err(status) if status.code() == tonic::Code::NotFound => {},
		Err(status) => return Err(status_error(status)),
	}
	let header = pb::PutArtifactHeader {
		expected_digest:     Some(sha256_digest(data)),
		expected_size_bytes: data.len() as u64,
		media_type_presence: Some(pb::put_artifact_header::MediaTypePresence::MediaType(
			"application/vnd.vmon.python-package".to_owned(),
		)),
		ttl_millis_presence: None,
	};
	let mut frames = Vec::with_capacity(data.len() / ARTIFACT_CHUNK_SIZE + 2);
	frames.push(pb::PutArtifactRequest {
		frame: Some(pb::put_artifact_request::Frame::Header(header)),
	});
	frames.extend(
		data
			.chunks(ARTIFACT_CHUNK_SIZE)
			.map(|chunk| pb::PutArtifactRequest {
				frame: Some(pb::put_artifact_request::Frame::Data(chunk.to_vec())),
			}),
	);
	grpc
		.block_on(artifacts.put(ReceiverStream::new({
			let (tx, rx) = tokio::sync::mpsc::channel(frames.len().max(1));
			for frame in frames {
				tx.blocking_send(frame)
					.expect("artifact receiver remains alive until RPC starts");
			}
			drop(tx);
			rx
		})))
		.map_err(status_error)?;
	Ok(artifact)
}

fn request_id(prefix: &str, bytes: &[u8]) -> String {
	format!("{prefix}-{:x}", Sha256::digest(bytes))
}

fn deploy_request_id() -> Result<String> {
	let mut bytes = [0_u8; 16];
	fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	Ok(format!(
		"cli-activate-{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:\
		 02x}{:02x}{:02x}{:02x}",
		bytes[0],
		bytes[1],
		bytes[2],
		bytes[3],
		bytes[4],
		bytes[5],
		bytes[6],
		bytes[7],
		bytes[8],
		bytes[9],
		bytes[10],
		bytes[11],
		bytes[12],
		bytes[13],
		bytes[14],
		bytes[15],
	))
}

fn activation_request(
	app: pb::AppRef,
	mut bindings: Vec<pb::AppFunctionBinding>,
	request_id: String,
) -> pb::ActivateAppRequest {
	bindings.sort_by(|left, right| left.name.cmp(&right.name));
	pb::ActivateAppRequest {
		app: Some(app),
		functions: bindings,
		expected_current_presence: None,
		request_id,
	}
}

fn deploy_target_mode(
	target: &str,
	options: &TransportOptions,
	autostart: bool,
	activate_app: bool,
) -> Result<Vec<pb::RevisionRef>> {
	let inspected = inspect_target(target)?;
	let grpc = client(options, autostart)?.grpc()?;
	let mut functions = grpc.functions();
	let mut bindings = Vec::with_capacity(inspected.functions.len());
	for inspected_function in inspected.functions {
		let source = upload_artifact(&grpc, &inspected_function.package)?;
		let mut spec = inspected_function.spec;
		spec
			.package
			.as_mut()
			.ok_or_else(|| CliError::new("inspected function omitted package"))?
			.source = Some(source);
		let encoded = spec.encode_to_vec();
		let revision = grpc
			.block_on(functions.register(pb::RegisterFunctionRequest {
				spec:              Some(spec),
				request_id:        request_id("cli-register", &encoded),
				transient_secrets: Vec::new(),
			}))
			.map_err(status_error)?
			.into_inner()
			.r#ref
			.ok_or_else(|| CliError::new("function registration omitted revision"))?;
		bindings.push(pb::AppFunctionBinding {
			name:     inspected_function.binding,
			revision: Some(revision),
		});
	}
	if !activate_app {
		let revisions = bindings
			.into_iter()
			.filter_map(|binding| binding.revision)
			.collect::<Vec<_>>();
		println!("registered {} function revision(s)", revisions.len());
		return Ok(revisions);
	}
	let app = pb::AppRef { namespace: inspected.namespace, name: inspected.app };
	let activation_request_id = deploy_request_id()?;
	let activated = grpc
		.block_on(functions.activate_app(activation_request(app, bindings, activation_request_id)))
		.map_err(status_error)?
		.into_inner();
	let revision = activated
		.r#ref
		.as_ref()
		.map_or("-", |value| value.revision_id.as_str());
	println!("deployed {} functions  app-revision={revision}", activated.functions.len());
	Ok(activated
		.functions
		.into_iter()
		.filter_map(|binding| binding.revision)
		.collect())
}

fn deploy_target(target: &str, options: &TransportOptions) -> Result<Vec<pb::RevisionRef>> {
	deploy_target_mode(target, options, true, true)
}

fn cmd_deploy(args: DeployArgs, options: &TransportOptions) -> Result<i32> {
	deploy_target(&args.target, options)?;
	Ok(0)
}

fn parse_function_ref(reference: &str) -> Result<(pb::FunctionRef, Option<String>)> {
	let (name, revision) = reference
		.split_once('@')
		.map_or((reference, None), |(name, revision)| (name, Some(revision.to_owned())));
	if name.is_empty() || revision.as_deref() == Some("") {
		return err("function reference must be NAME or NAME@REVISION");
	}
	let (namespace, name) = name
		.split_once('/')
		.map_or(("default", name), |(namespace, name)| (namespace, name));
	if namespace.is_empty() || name.is_empty() {
		return err("function reference must contain non-empty namespace and name");
	}
	Ok((pb::FunctionRef { namespace: namespace.to_owned(), name: name.to_owned() }, revision))
}

fn app_binding_reference(reference: &str) -> Option<(&str, &str)> {
	if reference.contains('@') || reference.contains('/') {
		return None;
	}
	let (app, binding) = reference.split_once('.')?;
	(!app.is_empty() && !binding.is_empty()).then_some((app, binding))
}

fn lookup_revision(grpc: &Grpc, reference: &str) -> Result<pb::FunctionRevision> {
	if let Some((app, binding)) = app_binding_reference(reference) {
		let mut functions = grpc.functions();
		let app_revision = grpc
			.block_on(functions.get_app(pb::GetAppRequest {
				app: Some(pb::AppSelector {
					selection: Some(pb::app_selector::Selection::Current(pb::AppRef {
						namespace: "default".to_owned(),
						name:      app.to_owned(),
					})),
				}),
			}))
			.map_err(status_error)?
			.into_inner();
		let revision = app_revision
			.functions
			.into_iter()
			.find(|candidate| candidate.name == binding)
			.and_then(|candidate| candidate.revision)
			.ok_or_else(|| CliError::new(format!("app {app:?} has no function {binding:?}")))?;
		return grpc
			.block_on(functions.get(pb::GetFunctionRequest {
				function: Some(pb::FunctionSelector {
					selection: Some(pb::function_selector::Selection::Pinned(revision)),
				}),
			}))
			.map(tonic::Response::into_inner)
			.map_err(status_error);
	}
	let (function, revision) = parse_function_ref(reference)?;
	let selection = revision.map_or_else(
		|| pb::function_selector::Selection::Current(function.clone()),
		|revision_id| {
			pb::function_selector::Selection::Pinned(pb::RevisionRef {
				function: Some(function.clone()),
				revision_id,
			})
		},
	);
	let mut functions = grpc.functions();
	match grpc.block_on(functions.get(pb::GetFunctionRequest {
		function: Some(pb::FunctionSelector { selection: Some(selection) }),
	})) {
		Ok(response) => Ok(response.into_inner()),
		Err(status) if status.code() == tonic::Code::NotFound && !reference.contains('@') => {
			let mut page_token = String::new();
			loop {
				let response = grpc
					.block_on(functions.list(pb::ListFunctionsRequest {
						namespace_presence: None,
						function_presence: None,
						page_size: 200,
						page_token,
					}))
					.map_err(status_error)?
					.into_inner();
				if let Some(revision) = response.revisions.into_iter().find(|revision| {
					revision
						.r#ref
						.as_ref()
						.is_some_and(|value| value.revision_id == reference)
				}) {
					return Ok(revision);
				}
				if response.next_page_token.is_empty() {
					return Err(status_error(status));
				}
				page_token = response.next_page_token;
			}
		},
		Err(status) => Err(status_error(status)),
	}
}

fn json_envelope(value: &Value) -> Result<pb::ValueEnvelope> {
	let encoded = serde_json::to_vec(value)?;
	Ok(pb::ValueEnvelope {
		schema_version:          1,
		serializer:              pb::ValueSerializer::Json as i32,
		compression:             pb::ValueCompression::None as i32,
		checksum:                Some(sha256_digest(&encoded)),
		uncompressed_size_bytes: encoded.len() as u64,
		storage:                 Some(pb::value_envelope::Storage::InlineData(encoded)),
		python_presence:         None,
		type_name_presence:      Some(pb::value_envelope::TypeNamePresence::TypeName(
			"json".to_owned(),
		)),
	})
}

fn verify_artifact_data(data: Vec<u8>, expected: Option<&pb::Digest>) -> Result<Vec<u8>> {
	if let Some(expected) = expected {
		if expected.algorithm != pb::DigestAlgorithm::Sha256 as i32 {
			return err("artifact uses an unsupported digest algorithm");
		}
		if expected.value != Sha256::digest(&data).as_slice() {
			return err("artifact digest mismatch");
		}
	}
	Ok(data)
}

fn download_artifact(grpc: &Grpc, artifact: pb::ArtifactRef) -> Result<Vec<u8>> {
	let expected = artifact.digest.clone();
	let mut artifacts = grpc.artifacts();
	let mut stream = grpc
		.block_on(
			artifacts
				.get(pb::GetArtifactRequest { artifact: Some(artifact), range_presence: None }),
		)
		.map_err(status_error)?
		.into_inner();
	let mut data = Vec::new();
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(chunk)) => {
				if chunk.offset != data.len() as u64 {
					return err("artifact download had a non-contiguous offset");
				}
				data.extend_from_slice(&chunk.data);
				if chunk.eof {
					return verify_artifact_data(data, expected.as_ref());
				}
			},
			Ok(None) => return verify_artifact_data(data, expected.as_ref()),
			Err(status) => return Err(status_error(status)),
		}
	}
}

fn envelope_json(grpc: &Grpc, envelope: &pb::ValueEnvelope) -> Result<Value> {
	if envelope.serializer != pb::ValueSerializer::Json as i32 {
		return err("call result is not portable JSON");
	}
	let stored = match envelope.storage.as_ref() {
		Some(pb::value_envelope::Storage::InlineData(data)) => data.clone(),
		Some(pb::value_envelope::Storage::Artifact(artifact)) => {
			download_artifact(grpc, artifact.clone())?
		},
		None => return err("call result omitted value storage"),
	};
	let mut data = Vec::new();
	match pb::ValueCompression::try_from(envelope.compression) {
		Ok(pb::ValueCompression::None) => data = stored,
		Ok(pb::ValueCompression::Gzip) => {
			flate2::read::GzDecoder::new(stored.as_slice()).read_to_end(&mut data)?;
		},
		Ok(pb::ValueCompression::Zstd) => {
			data = zstd::stream::decode_all(stored.as_slice())
				.map_err(|error| CliError::new(format!("invalid zstd call result: {error}")))?;
		},
		_ => return err("call result uses an unknown compression codec"),
	}
	if data.len() as u64 != envelope.uncompressed_size_bytes {
		return err("call result size does not match its envelope");
	}
	if let Some(checksum) = &envelope.checksum
		&& checksum.value != Sha256::digest(&data).as_slice()
	{
		return err("call result checksum mismatch");
	}
	serde_json::from_slice(&data).map_err(Into::into)
}

fn terminal_status(status: i32) -> bool {
	matches!(
		pb::CallStatus::try_from(status),
		Ok(pb::CallStatus::Succeeded | pb::CallStatus::Failed | pb::CallStatus::Cancelled)
	)
}

fn print_call_error(error: &pb::CallError) {
	eprintln!("{}: {}", error.code, error.message);
	for frame in &error.frames {
		eprintln!("  at {} ({}:{})", frame.function, frame.file, frame.line);
	}
}

static CALL_INTERRUPTED: AtomicBool = AtomicBool::new(false);

extern "C" fn interrupt_call(_: libc::c_int) {
	CALL_INTERRUPTED.store(true, Ordering::SeqCst);
}

fn install_call_interrupt() {
	CALL_INTERRUPTED.store(false, Ordering::SeqCst);
	// SAFETY: the handler only performs an async-signal-safe atomic store.
	unsafe {
		libc::signal(libc::SIGINT, interrupt_call as *const () as usize);
	}
}

fn outcome_exit_code(outcome: &pb::call_result::Outcome) -> i32 {
	i32::from(matches!(outcome, pb::call_result::Outcome::Error(_)))
}

fn wait_call(grpc: &Grpc, call_id: &str) -> Result<i32> {
	let mut calls = grpc.calls();
	loop {
		if CALL_INTERRUPTED.load(Ordering::SeqCst) {
			grpc
				.block_on(calls.cancel(pb::CancelCallRequest {
					call:       Some(pb::CallRef { call_id: call_id.to_owned() }),
					reason:     "client interrupted".to_owned(),
					request_id: request_id("cli-cancel", call_id.as_bytes()),
				}))
				.map_err(status_error)?;
			return Ok(130);
		}
		let record = grpc
			.block_on(calls.get(pb::CallRef { call_id: call_id.to_owned() }))
			.map_err(status_error)?
			.into_inner();
		if !terminal_status(record.status) {
			thread::sleep(Duration::from_millis(100));
			continue;
		}
		if record.status == pb::CallStatus::Succeeded as i32 {
			let result = grpc
				.block_on(calls.get_result(pb::GetCallResultRequest { call: record.r#ref, index: 0 }))
				.map_err(status_error)?
				.into_inner();
			let outcome = result
				.outcome
				.ok_or_else(|| CliError::new("call result omitted outcome"))?;
			let exit_code = outcome_exit_code(&outcome);
			match outcome {
				pb::call_result::Outcome::Value(value) => {
					println!("{}", serde_json::to_string_pretty(&envelope_json(grpc, &value)?)?);
				},
				pb::call_result::Outcome::Error(error) => print_call_error(&error),
			}
			return Ok(exit_code);
		}
		if let Some(pb::call_record::ErrorPresence::Error(error)) = record.error_presence {
			print_call_error(&error);
		}
		return Ok(if record.status == pb::CallStatus::Cancelled as i32 {
			130
		} else {
			1
		});
	}
}

fn invocation_input(values: &[Value], input_id: String) -> Result<pb::CallInput> {
	if input_id.is_empty() {
		return err("call input ID must not be empty");
	}
	let positional = values
		.iter()
		.map(json_envelope)
		.collect::<Result<Vec<_>>>()?;
	Ok(pb::CallInput {
		index: 0,
		payload: Some(pb::call_input::Payload::Arguments(pb::InvocationArguments {
			positional,
			named: HashMap::new(),
		})),
		input_id,
	})
}

fn cmd_run_function(
	target: String,
	raw_args: Vec<String>,
	options: &TransportOptions,
) -> Result<i32> {
	let values = raw_args
		.into_iter()
		.map(|value| {
			serde_json::from_str(&value)
				.map_err(|error| CliError::new(format!("invalid JSON argument {value:?}: {error}")))
		})
		.collect::<Result<Vec<Value>>>()?;
	let revisions = deploy_target_mode(&target, options, true, false)?;
	let revision = revisions
		.into_iter()
		.next()
		.ok_or_else(|| CliError::new("target inspection returned no function"))?;
	let grpc = client(options, true)?.grpc()?;
	let call_request_id = request_id(
		"cli-call",
		format!(
			"{target}:{}",
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map_err(|error| CliError::new(error.to_string()))?
				.as_nanos()
		)
		.as_bytes(),
	);
	let mut calls = grpc.calls();
	install_call_interrupt();
	let call = grpc
		.block_on(calls.create(pb::CreateCallRequest {
			r#type: pb::CallType::Unary as i32,
			target: Some(pb::CallTarget { function: Some(revision), receiver: None }),
			inputs: vec![invocation_input(&values, format!("{call_request_id}:0"))?],
			inputs_closed: true,
			graph: Some(pb::CallGraph::default()),
			request_id: call_request_id,
			labels: HashMap::new(),
			result_ttl_millis_presence: None,
		}))
		.map_err(status_error)?
		.into_inner();
	let call_id = call
		.r#ref
		.ok_or_else(|| CliError::new("call creation omitted ID"))?
		.call_id;
	wait_call(&grpc, &call_id)
}

fn cmd_run(args: RunArgs, options: &TransportOptions) -> Result<i32> {
	if let Some(target) = args.image.as_ref().filter(|target| target.contains("::")) {
		if args.dockerfile.is_some()
			|| args.name.is_some()
			|| args.detach
			|| args.block_network
			|| args.arch.is_some()
		{
			return err("sandbox options cannot be used with a durable function target");
		}
		return cmd_run_function(target.clone(), strip_dashdash(args.cmd), options);
	}
	if args.image.is_none() && args.dockerfile.is_none() {
		return err("provide an image (for example: vmon run alpine)");
	}
	let client = client(options, true)?;
	let mut body = Map::new();
	insert_opt(&mut body, "image", args.image.clone());
	insert_opt(&mut body, "dockerfile", args.dockerfile.clone());
	body.insert("context".to_owned(), json!(args.build_context));
	insert_opt(&mut body, "name", args.name.clone());
	body.insert("memory".to_owned(), json!(args.mem));
	body.insert("cpus".to_owned(), json!(args.cpus));
	body.insert("disk_mb".to_owned(), json!(args.disk_mb));
	body.insert("timeout".to_owned(), json!(args.timeout));
	body.insert("block_network".to_owned(), json!(args.block_network));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let cmd = strip_dashdash(args.cmd);
	if args.detach && !cmd.is_empty() {
		body.insert("command".to_owned(), json!(&cmd));
	}
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request =
		pb::CreateSandboxRequest { spec_json: Value::Object(body).to_string(), no_wait: false };
	let view = json_view(
		grpc
			.block_on(sandboxes.create(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let name = view_name(&view)?.to_owned();
	if args.detach {
		println!("started {name}  image={}", string_field(&view, "image").unwrap_or("-"));
		println!(
			"hint: vmon exec {name} -- ... | vmon snapshot {name} <snapshot> | vmon stop {name}"
		);
		return Ok(0);
	}
	if cmd.is_empty() {
		let _ = attach_console(&grpc, &name);
		return Ok(exit_code_from_view(&sandbox_view(&grpc, &name)?));
	}
	let exit = pump_exec(&grpc, ExecRpc::Exec, exec_start(&name, cmd, false), false, false, false)?;
	let _ = grpc.block_on(
		sandboxes
			.stop(pb::StopSandboxRequest { id: name, returncode: Some(i64::from(exit)) }),
	);
	Ok(exit)
}

fn cmd_ps(options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let response = grpc
		.block_on(sandboxes.list(pb::ListSandboxesRequest { tags: Vec::new() }))
		.map_err(status_error)?
		.into_inner();
	if response.sandboxes_json.is_empty() {
		println!("no sandboxes — boot one with `vmon run <image>`");
		return Ok(0);
	}
	println!("{:<24} {:<12} {:<8} IMAGE", "NAME", "STATUS", "PID");
	for sandbox in response.sandboxes_json {
		let sandbox: Value = serde_json::from_str(&sandbox)?;
		let name = string_field(&sandbox, "name")
			.or_else(|| string_field(&sandbox, "id"))
			.unwrap_or("-");
		let status = string_field(&sandbox, "status").unwrap_or("-");
		let pid = value_to_string(sandbox.get("pid")).unwrap_or_else(|| "-".to_owned());
		let image = string_field(&sandbox, "image").unwrap_or("-");
		println!("{name:<24} {status:<12} {pid:<8} {image}");
	}
	Ok(0)
}

fn cmd_exec(args: ExecArgs, options: &TransportOptions) -> Result<i32> {
	let argv = strip_dashdash(args.cmd);
	if argv.is_empty() {
		return err("exec requires a command (vmon exec NAME -- <cmd>)");
	}
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	if args.tty {
		return pump_exec(&grpc, ExecRpc::Exec, exec_start(&args.name, argv, true), true, true, true);
	}
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   args.name,
		exec: Some(pb::ExecStart { cmd: argv, ..Default::default() }),
	};
	let response = grpc
		.block_on(sandboxes.exec_capture(request))
		.map_err(status_error)?
		.into_inner();
	io::stdout().write_all(&response.stdout)?;
	io::stdout().flush()?;
	io::stderr().write_all(&response.stderr)?;
	io::stderr().flush()?;
	Ok(clamp_exit(response.code))
}

fn cmd_shell(args: ShellArgs, options: &TransportOptions) -> Result<i32> {
	let tty = if args.pty {
		true
	} else if args.no_pty {
		false
	} else {
		io::stdin().is_terminal() && io::stdout().is_terminal()
	};
	if args.command.is_none() && !tty {
		return err("vmon shell needs a terminal; pass -c '<cmd>' for a one-off or --pty");
	}
	let client = client(options, true)?;
	let mut params = Map::new();
	insert_opt(&mut params, "ref", args.reference);
	insert_opt(&mut params, "image", args.image);
	if let Some(command) = args.command {
		params.insert("cmd".to_owned(), json!(["/bin/sh", "-c", command]));
	}
	let env = parse_env(&args.env)?;
	if !env.is_empty() {
		params.insert("env".to_owned(), json!(env));
	}
	params.insert("mem".to_owned(), json!(args.mem));
	params.insert("cpus".to_owned(), json!(args.cpus));
	params.insert("disk_mb".to_owned(), json!(args.disk_mb));
	params.insert("timeout".to_owned(), json!(args.timeout));
	let grpc = client.grpc()?;
	let params = pb::exec_input::Input::ShellParamsJson(Value::Object(params).to_string());
	pump_exec(&grpc, ExecRpc::Shell, params, tty, tty, true)
}

fn cmd_cp(args: CpArgs, options: &TransportOptions) -> Result<i32> {
	let src_remote = parse_remote(&args.src);
	let dst_remote = parse_remote(&args.dst);
	if src_remote.is_some() == dst_remote.is_some() {
		return err("cp requires exactly one remote operand of the form <name>:<path>");
	}
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	if let Some((name, remote_path)) = src_remote {
		let request = pb::FilePathRequest { id: name, path: remote_path.clone() };
		let content = grpc
			.block_on(sandboxes.file_read(request))
			.map_err(status_error)?
			.into_inner();
		let mut out = PathBuf::from(&args.dst);
		if out.is_dir() {
			out.push(Path::new(&remote_path).file_name().unwrap_or_default());
		}
		fs::write(out, content.data)?;
	} else if let Some((name, remote_path)) = dst_remote {
		let data = fs::read(&args.src)?;
		let request = pb::FileWriteRequest { id: name, path: remote_path, data };
		grpc
			.block_on(sandboxes.file_write(request))
			.map_err(status_error)?;
	}
	Ok(0)
}

fn cmd_logs(args: LogsArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::LogsRequest { id: args.name, follow: args.follow, tail: None };
	let mut stream = grpc
		.block_on(sandboxes.logs(request))
		.map_err(status_error)?
		.into_inner();
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(chunk)) => {
				io::stdout().write_all(&chunk.data)?;
				io::stdout().flush()?;
			},
			Ok(None) => return Ok(0),
			Err(status) => return Err(status_error(status)),
		}
	}
}
fn function_shell(grpc: &Grpc, reference: &str, options: &TransportOptions) -> Result<i32> {
	let revision = lookup_revision(grpc, reference)?;
	let spec = revision
		.spec
		.as_ref()
		.ok_or_else(|| CliError::new("function revision omitted its specification"))?;
	let image = spec
		.image
		.as_ref()
		.and_then(|image| image.source.as_ref())
		.map(|source| match source {
			pb::image_spec::Source::Registry(registry) => registry.reference.clone(),
			pb::image_spec::Source::Python(python) => {
				format!("python:{}-{}", python.python_version, python.variant)
			},
			pb::image_spec::Source::Template(template) => template.name.clone(),
			pb::image_spec::Source::Dockerfile(_) => String::new(),
		})
		.filter(|value| !value.is_empty())
		.ok_or_else(|| CliError::new("function revision has no shell-capable image"))?;
	let source = spec
		.package
		.as_ref()
		.and_then(|package| package.source.clone())
		.ok_or_else(|| CliError::new("function revision omitted its source artifact"))?;
	let package = download_artifact(grpc, source)?;
	let mut sandboxes = grpc.sandboxes();
	let view = json_view(
		grpc
			.block_on(
				sandboxes.create(pb::CreateSandboxRequest {
					spec_json: json!({
						"image": image,
						"memory": 512,
						"cpus": 1,
						"disk_mb": 1024,
						"timeout": 300.0,
					})
					.to_string(),
					no_wait:   false,
				}),
			)
			.map_err(status_error)?
			.into_inner(),
	)?;
	let sandbox = view_name(&view)?.to_owned();
	if let Err(status) = grpc.block_on(sandboxes.file_write(pb::FileWriteRequest {
		id:   sandbox.clone(),
		path: "/tmp/vmon-function.zip".to_owned(),
		data: package,
	})) {
		let _ = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: sandbox }));
		return Err(status_error(status));
	}
	let result = cmd_shell(
		ShellArgs {
			reference: Some(sandbox.clone()),
			command:   None,
			image:     None,
			env:       vec!["PYTHONPATH=/tmp/vmon-function.zip".to_owned()],
			mem:       512,
			cpus:      1,
			disk_mb:   1024,
			timeout:   300.0,
			pty:       true,
			no_pty:    false,
		},
		options,
	);
	let _ = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: sandbox }));
	result
}

fn cmd_function(command: FunctionCommands, options: &TransportOptions) -> Result<i32> {
	let grpc = client(options, true)?.grpc()?;
	match command {
		FunctionCommands::Ls => {
			let mut functions = grpc.functions();
			let mut page_token = String::new();
			println!("{:<32} {:<28} STATUS", "FUNCTION", "REVISION");
			loop {
				let response = grpc
					.block_on(functions.list(pb::ListFunctionsRequest {
						namespace_presence: None,
						function_presence: None,
						page_size: 200,
						page_token,
					}))
					.map_err(status_error)?
					.into_inner();
				for revision in response.revisions {
					let Some(reference) = revision.r#ref else {
						continue;
					};
					let function = reference.function.unwrap_or_default();
					let status = pb::FunctionRevisionStatus::try_from(revision.status)
						.map_or("unknown", |status| status.as_str_name());
					println!(
						"{:<32} {:<28} {status}",
						format!("{}/{}", function.namespace, function.name),
						reference.revision_id
					);
				}
				if response.next_page_token.is_empty() {
					break;
				}
				page_token = response.next_page_token;
			}
			Ok(0)
		},
		FunctionCommands::Shell { reference } => function_shell(&grpc, &reference, options),
	}
}

fn call_status_name(status: i32) -> &'static str {
	pb::CallStatus::try_from(status).map_or("CALL_STATUS_UNKNOWN", |status| status.as_str_name())
}

fn print_call_results(grpc: &Grpc, call: pb::CallRef) -> Result<i32> {
	let mut calls = grpc.calls();
	let mut after_sequence = 0;
	let mut exit_code = 0;
	loop {
		let requested_after = after_sequence;
		let page = grpc
			.block_on(calls.list_results(pb::ListCallResultsRequest {
				cursor:    Some(pb::ResultCursor { call: Some(call.clone()), after_sequence }),
				page_size: 200,
			}))
			.map_err(status_error)?
			.into_inner();
		for result in page.results {
			after_sequence = after_sequence.max(result.sequence);
			let outcome = result
				.outcome
				.ok_or_else(|| CliError::new("call result omitted outcome"))?;
			exit_code = exit_code.max(outcome_exit_code(&outcome));
			match outcome {
				pb::call_result::Outcome::Value(value) => {
					println!("{}", serde_json::to_string_pretty(&envelope_json(grpc, &value)?)?);
				},
				pb::call_result::Outcome::Error(error) => print_call_error(&error),
			}
		}
		if page.end {
			return Ok(exit_code);
		}
		let next = page
			.next_cursor
			.ok_or_else(|| CliError::new("call result page omitted continuation cursor"))?
			.after_sequence;
		if next <= requested_after {
			return err("call result cursor did not advance");
		}
		after_sequence = next;
	}
}

fn cmd_call(command: CallCommands, options: &TransportOptions) -> Result<i32> {
	let grpc = client(options, true)?.grpc()?;
	match command {
		CallCommands::Get { id } => {
			let mut calls = grpc.calls();
			let record = grpc
				.block_on(calls.get(pb::CallRef { call_id: id }))
				.map_err(status_error)?
				.into_inner();
			let call_id = record
				.r#ref
				.as_ref()
				.map_or("-", |value| value.call_id.as_str());
			println!(
				"{}",
				serde_json::to_string_pretty(&json!({
					"call_id": call_id,
					"status": call_status_name(record.status),
					"input_count": record.input_count,
					"result_count": record.result_count,
					"created_at_unix_millis": record.created_at_unix_millis,
					"updated_at_unix_millis": record.updated_at_unix_millis,
				}))?
			);
			if record.result_count > 0 {
				let call = record
					.r#ref
					.ok_or_else(|| CliError::new("call record omitted its ID"))?;
				print_call_results(&grpc, call)
			} else if let Some(pb::call_record::ErrorPresence::Error(error)) = record.error_presence {
				print_call_error(&error);
				Ok(1)
			} else {
				Ok(0)
			}
		},
		CallCommands::Logs { id, follow } => follow_call_logs(&grpc, &id, follow),
	}
}

fn follow_call_logs(grpc: &Grpc, call_id: &str, follow: bool) -> Result<i32> {
	let mut after_sequence = 0;
	loop {
		let mut calls = grpc.calls();
		let response = grpc.block_on(calls.watch(pb::WatchCallRequest {
			cursor: Some(pb::EventCursor {
				call: Some(pb::CallRef { call_id: call_id.to_owned() }),
				after_sequence,
			}),
			follow,
		}));
		let mut stream = match response {
			Ok(response) => response.into_inner(),
			Err(status) if follow && status.code() == tonic::Code::Unavailable => {
				thread::sleep(Duration::from_millis(100));
				continue;
			},
			Err(status) => return Err(status_error(status)),
		};
		loop {
			match grpc.block_on(stream.message()) {
				Ok(Some(event)) => {
					if !accept_event_sequence(&mut after_sequence, event.sequence) {
						continue;
					}
					match event.payload {
						Some(pb::call_event::Payload::Log(log)) => {
							io::stdout().write_all(&log.data)?;
							io::stdout().flush()?;
						},
						Some(pb::call_event::Payload::Status(status))
							if terminal_status(status.status) =>
						{
							return Ok(0);
						},
						_ => {},
					}
				},
				Ok(None) if follow => break,
				Ok(None) => return Ok(0),
				Err(status) if follow && status.code() == tonic::Code::Unavailable => break,
				Err(status) => return Err(status_error(status)),
			}
		}
		thread::sleep(Duration::from_millis(100));
	}
}

enum LifecycleVerb {
	Stop,
	Pause,
	Resume,
	Suspend,
}

fn lifecycle(
	options: &TransportOptions,
	name: &str,
	verb: LifecycleVerb,
	label: &str,
) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(async {
			match verb {
				LifecycleVerb::Stop => {
					sandboxes
						.stop(pb::StopSandboxRequest { id: name.to_owned(), returncode: None })
						.await
				},
				LifecycleVerb::Pause => {
					sandboxes
						.pause(pb::SandboxRef { id: name.to_owned() })
						.await
				},
				LifecycleVerb::Resume => {
					sandboxes
						.resume(pb::SandboxRef { id: name.to_owned() })
						.await
				},
				LifecycleVerb::Suspend => {
					sandboxes
						.suspend(pb::SandboxRef { id: name.to_owned() })
						.await
				},
			}
		})
		.map_err(status_error)?;
	println!("{label} {name}");
	Ok(0)
}

fn cmd_history(args: NameArg, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let response = grpc
		.block_on(sandboxes.history(pb::SandboxRef { id: args.name.clone() }))
		.map_err(status_error)?
		.into_inner();
	if response.points.is_empty() {
		println!("no recovery points for {}", args.name);
		return Ok(0);
	}
	println!("{:<32} {:<12} {:>16} {:>12}", "POINT", "KIND", "CREATED_MS", "SIZE_BYTES");
	for point in response.points {
		println!(
			"{:<32} {:<12} {:>16} {:>12}",
			point.name, point.kind, point.created_at_unix_millis, point.size_bytes
		);
	}
	Ok(0)
}

fn cmd_rollback(args: RollbackArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(sandboxes.rollback(pb::RollbackSandboxRequest {
			id:             args.name.clone(),
			recovery_point: args.recovery_point.clone(),
		}))
		.map_err(status_error)?;
	println!("rolled back {} to {}", args.name, args.recovery_point);
	Ok(0)
}

fn cmd_rm(args: NameArg, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(sandboxes.remove(pb::SandboxRef { id: args.name.clone() }))
		.map_err(status_error)?;
	println!("removed {}", args.name);
	Ok(0)
}

fn cmd_extend(args: ExtendArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExtendSandboxRequest { id: args.name.clone(), secs: args.secs };
	let response = json_view(
		grpc
			.block_on(sandboxes.extend(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	if let Some(deadline) = response.get("deadline_unix") {
		println!("extended {} deadline to {deadline}", args.name);
	} else {
		println!("extended {} deadline by {}s", args.name, args.secs);
	}
	Ok(0)
}

fn cmd_snapshot(args: SnapshotArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request =
		pb::SnapshotRequest { id: args.name, name: Some(args.snapshot), stop: args.stop };
	let response = json_view(
		grpc
			.block_on(sandboxes.snapshot(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	println!(
		"snapshot {} -> {}",
		string_field(&response, "snapshot").unwrap_or("-"),
		string_field(&response, "dir").unwrap_or("-")
	);
	Ok(0)
}

fn cmd_restore(args: RestoreArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let mut body = Map::new();
	insert_opt(&mut body, "name", args.name);
	body.insert("agent".to_owned(), json!(args.agent));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let grpc = client.grpc()?;
	let mut snapshots = grpc.snapshots();
	let request = pb::RestoreSnapshotRequest {
		name:      args.snapshot.clone(),
		body_json: Value::Object(body).to_string(),
	};
	let view = json_view(
		grpc
			.block_on(snapshots.restore(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let name = view_name(&view)?.to_owned();
	println!("restored {name} from {}", args.snapshot);
	if args.detach {
		return Ok(0);
	}
	let _ = attach_console(&grpc, &name);
	Ok(exit_code_from_view(&sandbox_view(&grpc, &name)?))
}

fn cmd_fork(args: ForkArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let mut body = Map::new();
	body.insert("count".to_owned(), json!(args.count));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let grpc = client.grpc()?;
	let mut snapshots = grpc.snapshots();
	let request = pb::ForkSnapshotRequest {
		name:      args.snapshot.clone(),
		body_json: Value::Object(body).to_string(),
	};
	let response = json_view(
		grpc
			.block_on(snapshots.fork(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let clones = response
		.get("clones")
		.and_then(Value::as_array)
		.cloned()
		.unwrap_or_default();
	println!("forked {} clone(s) from {}", clones.len(), args.snapshot);
	for clone in clones {
		println!(
			"{}  (pid {}, CoW reconstruct={}ms)",
			string_field(&clone, "name").unwrap_or("-"),
			value_to_string(clone.get("pid")).unwrap_or_else(|| "-".to_owned()),
			value_to_string(clone.get("reconstruct_ms")).unwrap_or_else(|| "n/a".to_owned())
		);
	}
	Ok(0)
}

fn cmd_stats(name: &str, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.metrics(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	print_json(&json_view(view)?)
}

fn cmd_metrics(args: MetricsArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	if let Some(name) = args.name {
		return cmd_stats(&name, options);
	}
	print!("{}", client.request_text("GET", "/metrics")?);
	Ok(0)
}

fn cmd_fs(command: FsCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let (reference, list) = match command {
		FsCommands::List { reference } => (reference, true),
		FsCommands::Stat { reference } => (reference, false),
	};
	let (name, path) = parse_ref(&reference);
	let request = pb::FilePathRequest { id: name, path };
	let view = grpc
		.block_on(async {
			if list {
				sandboxes.file_list(request).await
			} else {
				sandboxes.file_stat(request).await
			}
		})
		.map_err(status_error)?
		.into_inner();
	print_json(&json_view(view)?)
}

fn cmd_volume(command: VolumeCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut volumes = grpc.volumes();
	match command {
		VolumeCommands::Ls => {
			let response = grpc
				.block_on(volumes.list(pb::ListVolumesRequest {}))
				.map_err(status_error)?
				.into_inner();
			for volume in response.volumes {
				println!("{volume}");
			}
			Ok(0)
		},
		VolumeCommands::Rm { name } => {
			grpc
				.block_on(volumes.delete(pb::VolumeRef { name: name.clone() }))
				.map_err(status_error)?;
			println!("removed volume {name}");
			Ok(0)
		},
	}
}

fn cmd_pool(command: PoolCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut pools = grpc.pools();
	match command {
		PoolCommands::Ls => {
			let view = grpc
				.block_on(pools.list(pb::ListPoolsRequest {}))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
		PoolCommands::Set(args) => {
			let mut body = Map::new();
			body.insert("size".to_owned(), json!(args.size));
			insert_opt(&mut body, "image", args.image);
			insert_opt(&mut body, "template", args.template);
			insert_opt(&mut body, "memory", args.mem);
			insert_opt(&mut body, "cpus", args.cpus);
			insert_opt(&mut body, "disk_mb", args.disk_mb);
			if args.block_network {
				body.insert("block_network".to_owned(), json!(true));
			}
			let request = pb::PoolSetRequest {
				reference: args.reference,
				body_json: Value::Object(body).to_string(),
			};
			let view = grpc
				.block_on(pools.set(request))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
		PoolCommands::Rm { reference } => {
			grpc
				.block_on(pools.delete(pb::PoolRef { reference: reference.clone() }))
				.map_err(status_error)?;
			println!("removed pool {reference}");
			Ok(0)
		},
	}
}

fn cmd_doctor(args: DoctorArgs) -> i32 {
	if args.serve {
		let mut overrides = HashMap::new();
		if let Some(path) = args.config_path {
			overrides.insert("config".to_owned(), path.display().to_string());
		}
		let (rows, checks) = vmond::doctor::collect_serve_doctor(&overrides);
		if !rows.is_empty() {
			println!("{:<28} {:<24} {:<10} {:<24} FLAG", "KEY", "VALUE", "SOURCE", "ENV");
			for row in rows {
				println!(
					"{:<28} {:<24} {:<10} {:<24} {}",
					row.key,
					truncate(&row.value, 24),
					row.source,
					row.env,
					row.flag
				);
			}
			println!();
		}
		return print_checks(checks);
	}
	print_checks(vmond::doctor::collect_checks())
}

fn cmd_daemon(command: DaemonCommands) -> Result<i32> {
	match command {
		DaemonCommands::Status => Ok(daemon_status()),
		DaemonCommands::Stop => daemon_stop(),
	}
}

fn daemon_status() -> i32 {
	let client = ApiClient::local(false);
	if client.request_json("GET", "/healthz", None).is_ok() {
		let info = client
			.grpc()
			.and_then(|grpc| {
				let mut system = grpc.system();
				let view = grpc
					.block_on(system.info(pb::InfoRequest {}))
					.map_err(status_error)?
					.into_inner();
				json_view(view)
			})
			.unwrap_or_else(|_| json!({}));
		let pid = read_pid().unwrap_or_else(|| "?".to_owned());
		let sock = vmond::home::Home::new(vmond::home::state_dir()).vmond_sock();
		println!(
			"vmond: running (pid {pid}) socket={} version={}",
			sock.display(),
			string_field(&info, "version").unwrap_or("?")
		);
		0
	} else {
		println!("vmond: not running");
		1
	}
}

fn daemon_stop() -> Result<i32> {
	let Some(pid_text) = read_pid() else {
		println!("vmond: not running");
		return Ok(0);
	};
	let pid = pid_text
		.trim()
		.parse::<libc::pid_t>()
		.map_err(|_| CliError::new(format!("invalid pid file contents: {pid_text:?}")))?;
	// SAFETY: `pid` comes from vmond's pid file and `SIGTERM` is a constant signal;
	// no pointers are passed.
	let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
	if rc != 0 {
		let err = io::Error::last_os_error();
		if err.raw_os_error() == Some(libc::ESRCH) {
			println!("vmond: not running");
			return Ok(0);
		}
		return Err(err.into());
	}
	let client = ApiClient::local(false);
	let deadline = Instant::now() + Duration::from_secs(5);
	while Instant::now() < deadline {
		if client.request_json("GET", "/healthz", None).is_err() {
			println!("vmond stopped (pid {pid})");
			return Ok(0);
		}
		thread::sleep(Duration::from_millis(100));
	}
	println!("sent SIGTERM to vmond (pid {pid})");
	Ok(0)
}

fn target_fingerprint(target: &str) -> Result<Vec<(String, Vec<u8>)>> {
	let inspected = inspect_target(target)?;
	let mut values = inspected
		.functions
		.into_iter()
		.map(|function| {
			let mut hasher = Sha256::new();
			hasher.update(&function.package);
			hasher.update(function.spec.encode_to_vec());
			(function.binding, hasher.finalize().to_vec())
		})
		.collect::<Vec<_>>();
	values.sort_by(|left, right| left.0.cmp(&right.0));
	Ok(values)
}

fn cmd_serve(mut args: ServeArgs, options: &TransportOptions) -> Result<i32> {
	let Some(target) = args.target.take() else {
		vmond::api::serve(args.overrides())?;
		return Ok(0);
	};
	let watch = args.watch;
	let overrides = args.overrides();
	let (server_tx, server_rx) = mpsc::sync_channel(1);
	thread::Builder::new()
		.name("vmon-serve".to_owned())
		.spawn(move || {
			let _ = server_tx.send(vmond::api::serve(overrides));
		})
		.map_err(|error| CliError::new(format!("failed to start vmond: {error}")))?;
	let deadline = Instant::now() + Duration::from_secs(10);
	loop {
		if client(options, false)
			.and_then(|client| client.grpc())
			.is_ok()
		{
			break;
		}
		if let Ok(result) = server_rx.try_recv() {
			result?;
			return err("vmon serve exited before accepting connections");
		}
		if Instant::now() >= deadline {
			return err("vmon serve did not become ready");
		}
		thread::sleep(Duration::from_millis(100));
	}
	let mut fingerprint = target_fingerprint(&target)?;
	deploy_target_mode(&target, options, false, true)?;
	loop {
		if let Ok(result) = server_rx.try_recv() {
			result?;
			return err("vmon serve exited unexpectedly");
		}
		thread::sleep(Duration::from_millis(500));
		if !watch {
			continue;
		}
		let current = target_fingerprint(&target)?;
		if !changed_function_bindings(&fingerprint, &current).is_empty() {
			deploy_target_mode(&target, options, false, true)?;
			fingerprint = current;
		}
	}
}

fn cmd_context(command: ContextCommands) -> Result<i32> {
	match command {
		ContextCommands::Add(args) => context_add(args),
		ContextCommands::Ls => Ok(context_ls()),
		ContextCommands::Use { name } => {
			let mut store = ContextStore::load_default();
			store.use_context(&name)?;
			println!("using context {name}");
			Ok(0)
		},
		ContextCommands::Rm { name } => {
			let mut store = ContextStore::load_default();
			store.remove(&name)?;
			println!("removed context {name}");
			Ok(0)
		},
	}
}

fn context_add(args: ContextAddArgs) -> Result<i32> {
	if args.name == LOCAL {
		return err("'local' is reserved for the local daemon");
	}
	let token = args.token.or_else(|| std::env::var("VMON_API_TOKEN").ok());
	if token.as_deref().is_none_or(str::is_empty) {
		return err("a token is required: pass --token or set VMON_API_TOKEN");
	}
	let url = normalize_server(&args.server);
	let endpoints = match ApiClient::remote(vec![url.clone()], token.clone()).and_then(|client| {
		let grpc = client.grpc()?;
		let mut system = grpc.system();
		let view = grpc
			.block_on(system.mesh_status(pb::MeshStatusRequest {}))
			.map_err(status_error)?
			.into_inner();
		json_view(view)
	}) {
		Ok(status) => roster_from_status(&status, &url),
		Err(error) => {
			eprintln!("warning: could not fetch mesh status ({error}); storing the single endpoint");
			vec![url]
		},
	};
	let mut store = ContextStore::load_default();
	store.put(Context {
		name: args.name.clone(),
		endpoints,
		region: args.region,
		updated: now_secs(),
	})?;
	if args.save_token {
		store.save_token(&args.name, token.as_deref().unwrap_or_default())?;
	}
	if store.current().is_none() {
		store.use_context(&args.name)?;
	}
	println!("context {} added", args.name);
	if !args.save_token {
		println!("hint: set VMON_API_TOKEN to authenticate cluster commands");
	}
	Ok(0)
}

fn context_ls() -> i32 {
	let store = ContextStore::load_default();
	let current = store.current_name();
	println!("{:<3} {:<20} {:<7} ENDPOINTS", "", "NAME", "TOKEN");
	println!(
		"{:<3} {:<20} {:<7} UDS",
		if current.as_deref() == Some(LOCAL) {
			"*"
		} else {
			""
		},
		LOCAL,
		"-"
	);
	for context in store.list() {
		let marker = if current.as_deref() == Some(context.name.as_str()) {
			"*"
		} else {
			""
		};
		let token = if store.has_token(&context.name) {
			"saved"
		} else {
			"env"
		};
		println!("{marker:<3} {:<20} {:<7} {}", context.name, token, context.endpoints.join(","));
	}
	0
}

fn cmd_mesh(command: MeshCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	match command {
		MeshCommands::Setup(args) => {
			let response = client.request_json(
				"POST",
				"/v1/mesh/setup",
				Some(json!({
					"advertise": args.advertise,
					"region": args.region,
					"max_vcpus": args.max_vcpus,
					"max_mem_mib": args.max_mem_mib,
				})),
			)?;
			if let Some(advertise) = string_field(&response, "advertise") {
				println!("cluster ready on {advertise}");
			}
			if let Some(blob) = string_field(&response, "blob") {
				println!("vmon mesh join {blob}");
			}
			Ok(0)
		},
		MeshCommands::Join(args) => {
			let response = client.request_json(
				"POST",
				"/v1/mesh/join",
				Some(json!({"blob": args.blob, "advertise": args.advertise, "region": args.region})),
			)?;
			print_json(&response)
		},
		MeshCommands::Leave(args) => {
			let response =
				client.request_json("POST", "/v1/mesh/leave", Some(json!({"drain": args.drain})))?;
			print_json(&response)
		},
		MeshCommands::Migrate(args) => {
			let grpc = client.grpc()?;
			let mut sandboxes = grpc.sandboxes();
			let view = grpc
				.block_on(sandboxes.migrate(pb::MigrateRequest { id: args.name, target: args.node }))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
		MeshCommands::Status => {
			let grpc = client.grpc()?;
			let mut system = grpc.system();
			let view = grpc
				.block_on(system.mesh_status(pb::MeshStatusRequest {}))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
	}
}

fn cmd_completion(args: CompletionArgs) -> i32 {
	let shell = match args.shell {
		CompletionShell::Bash => clap_complete::Shell::Bash,
		CompletionShell::Zsh => clap_complete::Shell::Zsh,
		CompletionShell::Fish => clap_complete::Shell::Fish,
	};
	let mut command = Cli::command();
	clap_complete::generate(shell, &mut command, "vmon", &mut io::stdout());
	0
}

impl ServeArgs {
	fn overrides(self) -> HashMap<String, String> {
		let mut overrides = HashMap::new();
		insert_override(
			&mut overrides,
			"config",
			self.config_path.map(|path| path.display().to_string()),
		);
		insert_override(&mut overrides, "home", self.home.map(|path| path.display().to_string()));
		insert_override(&mut overrides, "host", self.host);
		insert_override(&mut overrides, "port", self.port);
		insert_override(&mut overrides, "token", self.token);
		insert_override(&mut overrides, "client_token", self.client_token);
		insert_override(&mut overrides, "tenant_tokens", self.tenant_tokens);
		insert_override(&mut overrides, "tenant_keys", self.tenant_keys);
		insert_override(&mut overrides, "tls_cert", self.tls_cert);
		insert_override(&mut overrides, "tls_key", self.tls_key);
		insert_override(&mut overrides, "idle_timeout", self.idle_timeout);
		insert_override(&mut overrides, "history_disk_sec", self.history_disk_sec);
		insert_override(&mut overrides, "history_checkpoint_sec", self.history_checkpoint_sec);
		insert_override(&mut overrides, "history_retention", self.history_retention);
		insert_override(&mut overrides, "history_max_age_sec", self.history_max_age_sec);
		insert_override(&mut overrides, "template_ttl_sec", self.template_ttl_sec);
		insert_override(
			&mut overrides,
			"function_artifact_max_bytes",
			self.function_artifact_max_bytes,
		);
		insert_override(&mut overrides, "replicate_sec", self.replicate_sec);
		insert_override(&mut overrides, "replicas", self.replicas);
		insert_override(&mut overrides, "replicate_concurrency", self.replicate_concurrency);
		if self.restore_quorum {
			overrides.insert("restore_quorum".to_owned(), "true".to_owned());
		} else if self.no_restore_quorum {
			overrides.insert("restore_quorum".to_owned(), "false".to_owned());
		}
		insert_override(&mut overrides, "warm_pool_size", self.warm_pool_size);
		insert_override(&mut overrides, "warm_images", self.warm_images);
		insert_override(&mut overrides, "mesh_heartbeat_sec", self.mesh_heartbeat_sec);
		insert_override(&mut overrides, "mesh_reap_sec", self.mesh_reap_sec);
		insert_override(&mut overrides, "mesh_idem_ttl_sec", self.mesh_idem_ttl_sec);
		insert_override(&mut overrides, "mesh_create_timeout_sec", self.mesh_create_timeout_sec);
		insert_override(&mut overrides, "mesh_w_warm", self.mesh_w_warm);
		insert_override(&mut overrides, "mesh_w_free", self.mesh_w_free);
		insert_override(&mut overrides, "mesh_w_local", self.mesh_w_local);
		insert_override(&mut overrides, "mesh_w_region", self.mesh_w_region);
		insert_override(&mut overrides, "mesh_w_inflight", self.mesh_w_inflight);
		insert_override(&mut overrides, "orch_redis", self.orch_redis);
		insert_override(&mut overrides, "orch_url", self.orch_url);
		insert_override(&mut overrides, "tunnel_bind", self.tunnel_bind);
		insert_override(&mut overrides, "orch_id", self.orch_id);
		insert_override(&mut overrides, "orch_heartbeat_sec", self.orch_heartbeat_sec);
		insert_override(&mut overrides, "orch_dead_after_sec", self.orch_dead_after_sec);
		insert_override(&mut overrides, "orch_max_sandboxes", self.orch_max_sandboxes);
		insert_override(&mut overrides, "boot_concurrency", self.boot_concurrency);
		overrides
	}
}

impl Arch {
	const fn as_str(&self) -> &'static str {
		match self {
			Self::X86_64 => "x86_64",
			Self::Aarch64 => "aarch64",
		}
	}
}

fn attach_console(grpc: &Grpc, name: &str) -> Result<()> {
	let mut sandboxes = grpc.sandboxes();
	let mut stream = grpc
		.block_on(sandboxes.attach(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(output)) => {
				if let Some(pb::exec_output::Output::Chunk(chunk)) = output.output {
					write_chunk(&chunk)?;
				}
			},
			Ok(None) => return Ok(()),
			Err(status) => return Err(status_error(status)),
		}
	}
}

enum ExecRpc {
	Exec,
	Shell,
}

const fn exec_input(input: pb::exec_input::Input) -> pb::ExecInput {
	pb::ExecInput { input: Some(input) }
}

fn exec_start(sandbox_id: &str, cmd: Vec<String>, tty: bool) -> pb::exec_input::Input {
	pb::exec_input::Input::Start(pb::ExecStart {
		sandbox_id: sandbox_id.to_owned(),
		cmd,
		tty,
		..Default::default()
	})
}

/// Bridges an Exec/Shell bidi stream onto the synchronous terminal loop:
/// stdin is forwarded from a plain thread through the outbound channel while
/// this thread blocks on inbound frames.
fn pump_exec(
	grpc: &Grpc,
	rpc: ExecRpc,
	first: pb::exec_input::Input,
	raw_mode: bool,
	forward_stdin: bool,
	show_ready: bool,
) -> Result<i32> {
	let (tx, rx) = tokio::sync::mpsc::channel::<pb::ExecInput>(16);
	grpc
		.block_on(tx.send(exec_input(first)))
		.map_err(|_| CliError::new("exec stream closed before start"))?;
	let mut sandboxes = grpc.sandboxes();
	let outbound = ReceiverStream::new(rx);
	let response = grpc
		.block_on(async {
			match rpc {
				ExecRpc::Exec => sandboxes.exec(outbound).await,
				ExecRpc::Shell => sandboxes.shell(outbound).await,
			}
		})
		.map_err(status_error)?;
	let mut stream = response.into_inner();
	let _raw = RawModeGuard::enable(raw_mode)?;
	let _stdin = forward_stdin.then(|| spawn_stdin_forward(tx.clone()));
	let mut exit = None;
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(output)) => match output.output {
				Some(pb::exec_output::Output::Chunk(chunk)) => write_chunk(&chunk)?,
				Some(pb::exec_output::Output::Exit(code)) => exit = Some(clamp_exit(code.code)),
				Some(pb::exec_output::Output::Ready(ready)) => {
					if show_ready {
						eprintln!("ready: {}", ready.sandbox_id);
					}
				},
				// Session metadata only flows on the PTY RPCs; ignore it defensively.
				Some(pb::exec_output::Output::Pty(_)) => {},
				None => return err("invalid exec output frame"),
			},
			Ok(None) => break,
			Err(status) => return Err(status_error(status)),
		}
	}
	drop(tx);
	Ok(exit.unwrap_or(0))
}

fn write_chunk(chunk: &pb::Output) -> Result<()> {
	if pb::Stream::try_from(chunk.stream) == Ok(pb::Stream::Stderr) {
		io::stderr().write_all(&chunk.data)?;
		io::stderr().flush()?;
	} else {
		io::stdout().write_all(&chunk.data)?;
		io::stdout().flush()?;
	}
	Ok(())
}

fn clamp_exit(code: i64) -> i32 {
	code.clamp(0, 255) as i32
}

fn spawn_stdin_forward(tx: tokio::sync::mpsc::Sender<pb::ExecInput>) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		let mut stdin = io::stdin();
		let mut buf = [0_u8; 8192];
		loop {
			match stdin.read(&mut buf) {
				Ok(0) => {
					let _ = tx.blocking_send(exec_input(pb::exec_input::Input::Eof(pb::Eof {})));
					break;
				},
				Ok(n) => {
					if tx
						.blocking_send(exec_input(pb::exec_input::Input::Stdin(buf[..n].to_vec())))
						.is_err()
					{
						break;
					}
				},
				Err(_) => break,
			}
		}
	})
}

struct RawModeGuard(bool);

impl RawModeGuard {
	fn enable(enable: bool) -> Result<Self> {
		if enable {
			crossterm::terminal::enable_raw_mode()
				.map_err(|error| CliError::new(format!("failed to enter raw mode: {error}")))?;
		}
		Ok(Self(enable))
	}
}

impl Drop for RawModeGuard {
	fn drop(&mut self) {
		if self.0 {
			let _ = crossterm::terminal::disable_raw_mode();
		}
	}
}

fn parse_env(pairs: &[String]) -> Result<HashMap<String, String>> {
	let mut env = HashMap::new();
	for pair in pairs {
		if let Some((key, value)) = pair.split_once('=') {
			env.insert(key.to_owned(), value.to_owned());
		} else if let Ok(value) = std::env::var(pair) {
			env.insert(pair.to_owned(), value);
		} else {
			return err(format!("environment variable {pair:?} is not set"));
		}
	}
	Ok(env)
}

fn parse_remote(spec: &str) -> Option<(String, String)> {
	let (name, path) = spec.split_once(':')?;
	if name.is_empty() || path.is_empty() {
		return None;
	}
	Some((name.to_owned(), path.to_owned()))
}

fn parse_ref(spec: &str) -> (String, String) {
	if let Some((name, path)) = spec.split_once(':') {
		(
			name.to_owned(),
			if path.is_empty() {
				"/".to_owned()
			} else {
				path.to_owned()
			},
		)
	} else {
		(spec.to_owned(), "/".to_owned())
	}
}

fn strip_dashdash(mut args: Vec<String>) -> Vec<String> {
	if args.first().is_some_and(|arg| arg == "--") {
		args.remove(0);
	}
	args
}

fn insert_opt<T: serde::Serialize>(map: &mut Map<String, Value>, key: &str, value: Option<T>) {
	if let Some(value) = value {
		map.insert(key.to_owned(), json!(value));
	}
}

fn insert_override<T: ToString>(map: &mut HashMap<String, String>, key: &str, value: Option<T>) {
	if let Some(value) = value {
		map.insert(key.to_owned(), value.to_string());
	}
}

fn view_name(value: &Value) -> Result<&str> {
	value
		.get("name")
		.or_else(|| value.get("id"))
		.and_then(Value::as_str)
		.ok_or_else(|| CliError::new("API response did not include a sandbox name"))
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
	value.get(key).and_then(Value::as_str)
}

fn value_to_string(value: Option<&Value>) -> Option<String> {
	match value? {
		Value::Null => None,
		Value::String(text) => Some(text.clone()),
		other => Some(other.to_string()),
	}
}

fn exit_code_from_view(value: &Value) -> i32 {
	let code = value.get("returncode").and_then(Value::as_i64).unwrap_or(0);
	if code < 0 {
		(128 + (-code as i32)).clamp(0, 255)
	} else {
		(code as i32).clamp(0, 255)
	}
}

fn json_view(view: pb::JsonView) -> Result<Value> {
	serde_json::from_str(&view.json).map_err(Into::into)
}

fn sandbox_view(grpc: &Grpc, name: &str) -> Result<Value> {
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.get(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	json_view(view)
}

fn print_json(value: &Value) -> Result<i32> {
	println!("{}", serde_json::to_string_pretty(value)?);
	Ok(0)
}

fn changed_function_bindings(
	previous: &[(String, Vec<u8>)],
	current: &[(String, Vec<u8>)],
) -> Vec<String> {
	let previous_map = previous.iter().cloned().collect::<HashMap<_, _>>();
	let current_map = current.iter().cloned().collect::<HashMap<_, _>>();
	let mut changed = current
		.iter()
		.filter(|(name, digest)| previous_map.get(name) != Some(digest))
		.map(|(name, _)| name.clone())
		.collect::<Vec<_>>();
	changed.extend(
		previous
			.iter()
			.filter(|(name, _)| !current_map.contains_key(name))
			.map(|(name, _)| name.clone()),
	);
	changed.sort();
	changed
}

const fn accept_event_sequence(after_sequence: &mut u64, sequence: u64) -> bool {
	if sequence <= *after_sequence {
		return false;
	}
	*after_sequence = sequence;
	true
}

fn print_checks(checks: Vec<vmond::doctor::Check>) -> i32 {
	let mut failed = false;
	println!("{:<8} {:<24} DETAIL", "STATUS", "CHECK");
	for check in checks {
		let status = match check.status {
			vmond::doctor::Status::Ok => "ok",
			vmond::doctor::Status::Warn => "warn",
			vmond::doctor::Status::Fail => {
				failed = true;
				"fail"
			},
		};
		println!("{status:<8} {:<24} {}", check.name, check.detail);
		if !check.hint.is_empty() {
			println!("{:8} {:<24} hint: {}", "", "", check.hint);
		}
	}
	i32::from(failed)
}

fn truncate(value: &str, width: usize) -> String {
	if value.chars().count() <= width {
		value.to_owned()
	} else {
		let mut out = value
			.chars()
			.take(width.saturating_sub(1))
			.collect::<String>();
		out.push('…');
		out
	}
}

fn read_pid() -> Option<String> {
	let home = vmond::home::Home::new(vmond::home::state_dir());
	fs::read_to_string(home.vmond_pid())
		.ok()
		.map(|text| text.trim().to_owned())
}

#[cfg(test)]
mod durable_cli_tests {
	use super::*;

	#[test]
	fn legacy_run_and_serve_shapes_are_unchanged() {
		let run = Cli::try_parse_from(["vmon", "run", "alpine", "--", "echo", "ok"]).unwrap();
		let Commands::Run(run) = run.command else {
			panic!("expected run")
		};
		assert_eq!(run.image.as_deref(), Some("alpine"));
		assert_eq!(run.cmd, ["echo", "ok"]);

		let serve = Cli::try_parse_from(["vmon", "serve", "--port", "9000"]).unwrap();
		let Commands::Serve(serve) = serve.command else {
			panic!("expected serve")
		};
		assert_eq!(serve.port, Some(9000));
		assert!(serve.target.is_none());
		assert!(!serve.watch);
		let serve =
			Cli::try_parse_from(["vmon", "serve", "--function-artifact-max-bytes", "4294967296"])
				.unwrap();
		let Commands::Serve(serve) = serve.command else {
			panic!("expected serve")
		};
		assert_eq!(serve.function_artifact_max_bytes, Some(4_294_967_296));
		assert!(
			Cli::try_parse_from(["vmon", "serve", "--function-artifact-max-bytes", "0",]).is_err()
		);
	}

	#[test]
	fn durable_command_shapes_parse() {
		assert!(matches!(
			Cli::try_parse_from(["vmon", "deploy", "app.py"]).unwrap().command,
			Commands::Deploy(DeployArgs { target }) if target == "app.py"
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "function", "ls"])
				.unwrap()
				.command,
			Commands::Function { command: FunctionCommands::Ls }
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "function", "shell", "demo/embed@r1"])
				.unwrap()
				.command,
			Commands::Function {
				command: FunctionCommands::Shell { reference }
			} if reference == "demo/embed@r1"
		));
		assert!(matches!(
			Cli::try_parse_from([
				"vmon",
				"image",
				"publish-gce-rootfs",
				"gs://bucket/image.tar.gz",
			])
			.unwrap()
			.command,
			Commands::Image {
				command: ImageCommands::PublishGceRootfs { reference }
			} if reference == "gs://bucket/image.tar.gz"
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "call", "get", "call-1"]).unwrap().command,
			Commands::Call { command: CallCommands::Get { id } } if id == "call-1"
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "call", "logs", "call-1", "--follow"])
				.unwrap()
				.command,
			Commands::Call {
				command: CallCommands::Logs { id, follow: true }
			} if id == "call-1"
		));
		let serve = Cli::try_parse_from(["vmon", "serve", "app.py", "--watch"]).unwrap();
		assert!(matches!(
			serve.command,
			Commands::Serve(ServeArgs { target: Some(target), watch: true, .. })
				if target == "app.py"
		));
		let run = Cli::try_parse_from(["vmon", "run", "app.py::embed", "1", "{\"x\":2}"]).unwrap();
		assert!(matches!(
			run.command,
			Commands::Run(RunArgs { image: Some(target), cmd, .. })
				if target == "app.py::embed" && cmd == ["1", "{\"x\":2}"]
		));
	}

	#[test]
	fn durable_lifecycle_command_shapes_parse() {
		assert!(matches!(
			Cli::try_parse_from(["vmon", "suspend", "box"]).unwrap().command,
			Commands::Suspend(NameArg { name }) if name == "box"
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "history", "box"]).unwrap().command,
			Commands::History(NameArg { name }) if name == "box"
		));
		assert!(matches!(
			Cli::try_parse_from(["vmon", "rollback", "box", "checkpoint-7"])
				.unwrap()
				.command,
			Commands::Rollback(RollbackArgs { name, recovery_point })
				if name == "box" && recovery_point == "checkpoint-7"
		));
	}

	#[test]
	fn invalid_json_is_rejected_before_target_or_rpc_work() {
		let error = cmd_run_function(
			"missing.py::embed".to_owned(),
			vec!["{".to_owned()],
			&TransportOptions::default(),
		)
		.unwrap_err();
		assert!(error.to_string().contains("invalid JSON argument"));
	}

	#[test]
	fn invalid_target_is_rejected_before_rpc_work() {
		let error = inspect_target("definitely-missing.py::embed")
			.err()
			.expect("target must fail");
		assert!(error.to_string().contains("not an existing file"));
	}

	#[test]
	fn run_result_and_error_outcomes_have_distinct_exit_codes() {
		let value = pb::call_result::Outcome::Value(pb::ValueEnvelope::default());
		let error = pb::call_result::Outcome::Error(pb::CallError::default());
		assert_eq!(outcome_exit_code(&value), 0);
		assert_eq!(outcome_exit_code(&error), 1);
		let input =
			invocation_input(&[json!(1), json!({"key": "value"})], "request-1:0".to_owned()).unwrap();
		assert!(!input.input_id.is_empty());
		let Some(pb::call_input::Payload::Arguments(arguments)) = input.payload else {
			panic!("expected structured invocation arguments");
		};
		assert_eq!(arguments.positional.len(), 2);
		assert!(arguments.named.is_empty());
	}

	#[test]
	fn current_and_pinned_references_are_distinct() {
		let (current, revision) = parse_function_ref("team/embed").unwrap();
		assert_eq!((current.namespace.as_str(), current.name.as_str()), ("team", "embed"));
		assert!(revision.is_none());
		let (pinned, revision) = parse_function_ref("team/embed@rev-7").unwrap();
		assert_eq!(pinned, current);
		assert_eq!(revision.as_deref(), Some("rev-7"));
		assert_eq!(app_binding_reference("demo.embed"), Some(("demo", "embed")));
		assert_eq!(app_binding_reference("team/embed@rev-7"), None);
	}

	#[test]
	fn log_cursor_rejects_replayed_sequences_after_reconnect() {
		let mut cursor = 0;
		assert!(accept_event_sequence(&mut cursor, 1));
		assert!(accept_event_sequence(&mut cursor, 4));
		assert!(!accept_event_sequence(&mut cursor, 4));
		assert!(!accept_event_sequence(&mut cursor, 2));
		assert!(accept_event_sequence(&mut cursor, 5));
	}

	#[test]
	fn watch_invalidates_only_changed_function_inputs() {
		let before = vec![("a".to_owned(), vec![1]), ("b".to_owned(), vec![2])];
		let after = vec![("a".to_owned(), vec![1]), ("b".to_owned(), vec![3])];
		assert_eq!(changed_function_bindings(&before, &after), ["b"]);
		assert_eq!(changed_function_bindings(&before, &after[..1]), ["b"]);
	}

	#[test]
	fn registration_is_deterministic_and_activation_is_fresh_per_command() {
		let spec = pb::FunctionSpec {
			function: Some(pb::FunctionRef {
				namespace: "default".to_owned(),
				name:      "embed".to_owned(),
			}),
			..Default::default()
		};
		let encoded = spec.encode_to_vec();
		assert_eq!(request_id("cli-register", &encoded), request_id("cli-register", &encoded));
		let request = pb::RegisterFunctionRequest {
			spec:              Some(spec),
			request_id:        request_id("cli-register", &encoded),
			transient_secrets: Vec::new(),
		};
		assert!(request.transient_secrets.is_empty());
		assert!(!String::from_utf8_lossy(&request.encode_to_vec()).contains("secret-value"));

		let revision = |name: &str, id: &str| pb::AppFunctionBinding {
			name:     name.to_owned(),
			revision: Some(pb::RevisionRef {
				function:    Some(pb::FunctionRef {
					namespace: "default".to_owned(),
					name:      name.to_owned(),
				}),
				revision_id: id.to_owned(),
			}),
		};
		let first_command_id = deploy_request_id().unwrap();
		let second_command_id = deploy_request_id().unwrap();
		assert_ne!(first_command_id, second_command_id);
		assert!(first_command_id.starts_with("cli-activate-"));

		let app = pb::AppRef { namespace: "default".to_owned(), name: "demo".to_owned() };
		let activation = activation_request(
			app.clone(),
			vec![revision("zeta", "r2"), revision("alpha", "r1")],
			first_command_id.clone(),
		);
		let retry = activation.clone();
		assert_eq!(activation.request_id, first_command_id);
		assert_eq!(retry.request_id, activation.request_id);
		assert_eq!(activation.app.as_ref(), Some(&app));
		assert_eq!(
			activation
				.functions
				.iter()
				.map(|binding| binding.name.as_str())
				.collect::<Vec<_>>(),
			["alpha", "zeta"]
		);
		assert!(
			activation
				.functions
				.iter()
				.all(|binding| binding.revision.is_some())
		);
		assert_eq!(activation.request_id.len(), "cli-activate-".len() + 36);
		assert!(activation.expected_current_presence.is_none());
		assert!(!String::from_utf8_lossy(&activation.encode_to_vec()).contains("secret-value"));
	}
}
