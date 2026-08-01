//! `vmon bench` — sandbox create and snapshot-fork throughput benchmark.
//!
//! Two load models share this module:
//!
//! - **Closed loop** (default): `--concurrency` workers keep requests in flight
//!   against `SandboxService.Create` or `SnapshotService.Fork`; the measured
//!   latency is the full RPC including VM launch, so throughput is bounded by
//!   response latency.
//! - **Open loop** (`--target-rps` + `--reference-memory-mib`): requests fire
//!   at absolute scheduled instants, independent of response latency, modelling
//!   a real arrival process. The offered rate is the full-scale target scaled
//!   to live scheduler capacity and the request count fills the memory headroom
//!   (`capacity × headroom / sandbox_memory`). Creates use `no_wait` and ride
//!   `BatchCreate` streams when the server supports them (detected once during
//!   warmup, unary fallback otherwise). **Admission** (create accepted,
//!   identity assigned) is measured separately from **readiness** (the sandbox
//!   reports `running` on a `Watch` stream). `--waves` repeats
//!   schedule→drain→report→cleanup cycles.
//!
//! A warmup phase (excluded from measurements) preconnects every client
//! channel and primes the image or decrypted snapshot template. Created
//! sandboxes are removed afterwards unless `--keep` is passed; open-loop
//! cleanup runs only after reporting and polls `List` until every benchmark
//! sandbox is gone.
//!
//! The post-schedule drain is bounded: once nothing has resolved (admission,
//! readiness, or failure) for [`STALL_GRACE`], the remaining watchers are
//! abandoned as `stalled` so a few doomed stragglers cannot hold the wave for
//! the full `--timeout`. Cleanup treats `NotFound` as success — failed
//! sandboxes reap themselves server-side.

use std::{
	collections::{BTreeMap, HashSet},
	sync::{
		Arc, Mutex,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	time::{Duration, Instant},
};

use tonic::{
	Request,
	codegen::tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream},
	metadata::{Ascii, MetadataValue},
	transport::{Channel, Endpoint},
};
use vmon_proto::v1 as pb;

use crate::error::{CliError, Result};

/// Open-loop drain stall budget: once the schedule has ended and no request
/// has resolved for this long, pending watchers are abandoned as `stalled`.
const STALL_GRACE: Duration = Duration::from_secs(5);

/// Everything `vmon bench` needs, parsed by the CLI layer.
pub struct BenchOptions {
	/// Scheduler (or worker) base URL, e.g. `http://1.2.3.4:8100`.
	pub server:      String,
	/// Bearer token.
	pub token:       Option<String>,
	/// Total measured create requests.
	pub count:       usize,
	/// Concurrent in-flight creates.
	pub concurrency: usize,
	/// Client HTTP/2 connections; 0 sizes automatically from the load.
	pub connections: usize,
	/// Open-loop schedule→drain→cleanup repetitions.
	pub waves:       usize,
	/// Guest image for the spec.
	pub image:       String,
	/// Guest memory MiB (the "smallest one you can pick" knob).
	pub memory:      u32,
	/// Guest vCPUs.
	pub cpus:        u32,
	/// Per-request client-side deadline.
	pub timeout:     Duration,
	/// Untimed warmup creates before the measured run.
	pub warmup:      usize,
	/// Leave sandboxes running instead of removing them.
	pub keep:        bool,
	/// Emit a machine-readable JSON summary line at the end.
	pub json:        bool,
	/// Optional memory-normalized open-loop burst.
	pub scaled:      Option<ScaledBurstOptions>,
	/// Named snapshot to fork instead of creating from an image.
	pub snapshot:    Option<String>,
}

/// Full-scale target mapped onto the live scheduler capacity.
pub struct ScaledBurstOptions {
	/// Create rate expected at `reference_memory_mib`.
	pub target_rps:           f64,
	/// Memory capacity represented by `target_rps`.
	pub reference_memory_mib: u64,
	/// Fraction of live capacity available to the benchmark.
	pub headroom:             f64,
}

#[derive(Clone, Copy)]
struct BurstPlan {
	capacity_mib:     u64,
	used_mib:         u64,
	safe_budget_mib:  u64,
	count:            usize,
	offered_rps:      f64,
	scheduled_window: Duration,
}

#[derive(Clone)]
enum BenchRequest {
	Create { spec: String },
	Fork { snapshot: String },
}

enum CreateOutcome {
	Created(Sample),
	Failed(String),
}

struct Sample {
	latency: Duration,
	node:    String,
	sid:     Option<String>,
}

#[derive(Default)]
struct Tally {
	ok:     Vec<Sample>,
	errors: BTreeMap<String, usize>,
}

impl Tally {
	fn record(&mut self, outcome: CreateOutcome) {
		match outcome {
			CreateOutcome::Created(sample) => self.ok.push(sample),
			CreateOutcome::Failed(code) => *self.errors.entry(code).or_insert(0) += 1,
		}
	}
}

/// Relaxed counters sampled per event; the only shared state on the open-loop
/// timed path.
#[derive(Default)]
struct Gauges {
	offered:            AtomicU64,
	completed:          AtomicU64,
	admission_wall_ns:  AtomicU64,
	ready_wall_ns:      AtomicU64,
	pending_ready:      AtomicU64,
	peak_in_flight:     AtomicU64,
	peak_pending_ready: AtomicU64,
	/// Elapsed time of the latest progress event (admission, readiness, or
	/// failure); the stall monitor reads this to bound the drain.
	last_event_ns:      AtomicU64,
}

impl Gauges {
	fn fired(&self) {
		let offered = self.offered.fetch_add(1, Ordering::Relaxed) + 1;
		let completed = self.completed.load(Ordering::Relaxed);
		self
			.peak_in_flight
			.fetch_max(offered.saturating_sub(completed), Ordering::Relaxed);
	}

	fn admitted(&self, elapsed: Duration) {
		self.touch(elapsed);
		self.mark_admission_settled(elapsed);
		let pending = self.pending_ready.fetch_add(1, Ordering::Relaxed) + 1;
		self
			.peak_pending_ready
			.fetch_max(pending, Ordering::Relaxed);
	}

	fn completed_ready(&self, elapsed: Duration) {
		self.touch(elapsed);
		self.leave_pending();
		self.completed.fetch_add(1, Ordering::Relaxed);
		let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
		self.ready_wall_ns.fetch_max(nanos, Ordering::Relaxed);
	}

	fn completed_before_admission(&self, elapsed: Duration) {
		self.touch(elapsed);
		self.mark_admission_settled(elapsed);
		self.completed.fetch_add(1, Ordering::Relaxed);
	}

	fn completed_after_admission(&self, elapsed: Duration) {
		self.touch(elapsed);
		self.leave_pending();
		self.completed.fetch_add(1, Ordering::Relaxed);
	}

	fn touch(&self, elapsed: Duration) {
		let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
		self.last_event_ns.fetch_max(nanos, Ordering::Relaxed);
	}

	fn last_event(&self) -> Duration {
		Duration::from_nanos(self.last_event_ns.load(Ordering::Relaxed))
	}

	fn mark_admission_settled(&self, elapsed: Duration) {
		let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
		self.admission_wall_ns.fetch_max(nanos, Ordering::Relaxed);
	}

	fn admission_wall(&self) -> Duration {
		Duration::from_nanos(self.admission_wall_ns.load(Ordering::Relaxed))
	}

	fn ready_wall(&self) -> Duration {
		Duration::from_nanos(self.ready_wall_ns.load(Ordering::Relaxed))
	}

	fn leave_pending(&self) {
		let previous = self.pending_ready.fetch_sub(1, Ordering::Relaxed);
		debug_assert!(previous > 0, "completed an admission that was not pending");
	}
}

/// One scheduled request's fate, produced off the timed path.
struct FireOutcome {
	admission:  Option<Duration>,
	completion: Option<Duration>,
	sid:        Option<String>,
	error:      Option<String>,
}

impl FireOutcome {
	const fn failed(code: String) -> Self {
		Self { admission: None, completion: None, sid: None, error: Some(code) }
	}
}

/// Lane-local results; merged once after the wave, never shared while timed.
#[derive(Default)]
struct LaneTally {
	admissions:  Vec<Duration>,
	completions: Vec<Duration>,
	failures:    BTreeMap<String, usize>,
	sids:        Vec<String>,
	first_fire:  Option<tokio::time::Instant>,
	last_fire:   Option<tokio::time::Instant>,
}

impl LaneTally {
	fn note_fire(&mut self, now: tokio::time::Instant) {
		self.first_fire.get_or_insert(now);
		self.last_fire = Some(now);
	}

	fn fail(&mut self, code: String) {
		*self.failures.entry(code).or_insert(0) += 1;
	}

	fn absorb(&mut self, outcome: FireOutcome) {
		if let Some(admission) = outcome.admission {
			self.admissions.push(admission);
		}
		if let Some(completion) = outcome.completion {
			self.completions.push(completion);
		}
		if let Some(sid) = outcome.sid {
			self.sids.push(sid);
		}
		if let Some(code) = outcome.error {
			self.fail(code);
		}
	}

	fn merge(&mut self, other: Self) {
		self.admissions.extend(other.admissions);
		self.completions.extend(other.completions);
		for (code, count) in other.failures {
			*self.failures.entry(code).or_insert(0) += count;
		}
		self.sids.extend(other.sids);
		self.first_fire = match (self.first_fire, other.first_fire) {
			(Some(a), Some(b)) => Some(a.min(b)),
			(a, b) => a.or(b),
		};
		self.last_fire = match (self.last_fire, other.last_fire) {
			(Some(a), Some(b)) => Some(a.max(b)),
			(a, b) => a.or(b),
		};
	}
}

/// One open-loop wave's merged results.
#[derive(Default)]
struct WaveOutcome {
	fired:              usize,
	admissions:         Vec<Duration>,
	completions:        Vec<Duration>,
	failures:           BTreeMap<String, usize>,
	busy:               usize,
	sids:               Vec<String>,
	offered_window:     Duration,
	admission_wall:     Duration,
	ready_wall:         Duration,
	wall:               Duration,
	peak_in_flight:     u64,
	peak_pending_ready: u64,
}

impl WaveOutcome {
	fn failed(&self) -> usize {
		self.failures.values().sum::<usize>() + self.busy
	}

	fn absorb(&mut self, other: &Self) {
		self.fired += other.fired;
		self.admissions.extend_from_slice(&other.admissions);
		self.completions.extend_from_slice(&other.completions);
		for (code, count) in &other.failures {
			*self.failures.entry(code.clone()).or_insert(0) += count;
		}
		self.busy += other.busy;
		self.offered_window += other.offered_window;
		self.admission_wall += other.admission_wall;
		self.ready_wall += other.ready_wall;
		self.wall += other.wall;
		self.peak_in_flight = self.peak_in_flight.max(other.peak_in_flight);
		self.peak_pending_ready = self.peak_pending_ready.max(other.peak_pending_ready);
	}
}

/// Everything a lane needs; cloned per lane and per fired request (all fields
/// are cheap handles, copies, or refcount bumps).
#[derive(Clone)]
struct OpenLoopContext {
	bearer:  Option<MetadataValue<Ascii>>,
	spec:    Arc<String>,
	timeout: Duration,
	start:   tokio::time::Instant,
	plan:    BurstPlan,
	lanes:   usize,
	gauges:  Arc<Gauges>,
	/// Trips true when the stall monitor abandons the drain.
	cutoff:  tokio::sync::watch::Receiver<bool>,
}

/// Run the benchmark; returns the process exit code.
pub fn run(options: BenchOptions) -> Result<i32> {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.map_err(|error| CliError::new(format!("tokio runtime: {error}")))?;
	runtime.block_on(run_async(options))
}

async fn run_async(options: BenchOptions) -> Result<i32> {
	if options.snapshot.is_some() && options.scaled.is_some() {
		return Err(CliError::new("snapshot forks do not support scaled bursts"));
	}
	let bearer = options
		.token
		.as_deref()
		.map(|token| {
			MetadataValue::<Ascii>::try_from(format!("Bearer {token}"))
				.map_err(|_| CliError::new("token is not valid header metadata"))
		})
		.transpose()?;
	let control_channel = channel(&options.server)?;
	let burst = match &options.scaled {
		Some(scaled) => {
			Some(scaled_burst_plan(&control_channel, bearer.as_ref(), scaled, options.memory).await?)
		},
		None => None,
	};
	let count = burst.as_ref().map_or(options.count, |plan| plan.count);
	let connections = connection_count(options.connections, burst.as_ref(), options.concurrency);
	let mut channels = Vec::with_capacity(connections);
	channels.push(control_channel);
	for _ in 1..connections {
		channels.push(channel(&options.server)?);
	}
	// Preconnect every channel before anything is timed.
	prime_channels(&channels, bearer.as_ref()).await?;

	// Prebuilt once; the timed path only clones the resulting string.
	let spec = serde_json::json!({
		"image": options.image,
		"cpus": options.cpus,
		"memory": options.memory,
		// Self-cleaning guard rail: leaked sandboxes die on their own.
		"timeout": 600.0,
	})
	.to_string();
	let request = options.snapshot.as_ref().map_or_else(
		|| BenchRequest::Create { spec: spec.clone() },
		|snapshot| BenchRequest::Fork { snapshot: snapshot.clone() },
	);

	if options.warmup > 0 {
		let warmup_target = if options.snapshot.is_some() {
			"decrypted snapshot template"
		} else {
			"image templates"
		};
		println!("warmup: {} request(s) (untimed; primes {warmup_target})...", options.warmup);
		let mut warm_sids = Vec::new();
		for index in 0..options.warmup {
			let channel = &channels[index % channels.len()];
			let mut sandboxes = sandbox_client(channel);
			let mut snapshots = snapshot_client(channel);
			match request_once(
				&mut sandboxes,
				&mut snapshots,
				bearer.as_ref(),
				&request,
				Duration::from_mins(10),
			)
			.await
			{
				CreateOutcome::Created(sample) => {
					if let Some(sid) = sample.sid {
						warm_sids.push(sid);
					}
				},
				CreateOutcome::Failed(code) => {
					return Err(CliError::new(format!(
						"warmup request failed ({code}) — aborting benchmark"
					)));
				},
			}
		}
		remove_all(&channels, bearer.as_ref(), warm_sids).await;
	}

	if let Some(plan) = &burst {
		println!(
			"scaled burst: {} creates over {:.2}ms at {:.2}/s; capacity {} MiB, used {} MiB, safe \
			 budget {} MiB",
			plan.count,
			plan.scheduled_window.as_secs_f64() * 1e3,
			plan.offered_rps,
			plan.capacity_mib,
			plan.used_mib,
			plan.safe_budget_mib,
		);
		return run_open_loop_waves(&options, &channels, bearer.as_ref(), spec, plan).await;
	} else if let Some(snapshot) = &options.snapshot {
		println!("bench: {count} forks, concurrency {}, snapshot {snapshot}", options.concurrency);
	} else {
		println!(
			"bench: {count} creates, concurrency {}, spec {{image: {}, cpus: {}, memory: {} MiB}}",
			options.concurrency, options.image, options.cpus, options.memory
		);
	}

	let (tally, wall) = run_closed_loop(
		&channels,
		bearer.as_ref(),
		&request,
		options.timeout,
		count,
		options.concurrency,
	)
	.await?;
	report(&tally, wall, &options);

	if !options.keep {
		let sids: Vec<String> = tally
			.ok
			.iter()
			.filter_map(|sample| sample.sid.clone())
			.collect();
		println!("cleanup: removing {} sandboxes...", sids.len());
		remove_all(&channels, bearer.as_ref(), sids).await;
	}
	let failed = tally.errors.values().sum::<usize>();
	Ok(i32::from(failed != 0))
}

/// Drive `--waves` open-loop cycles: schedule → drain → report → cleanup.
async fn run_open_loop_waves(
	options: &BenchOptions,
	channels: &[Channel],
	bearer: Option<&MetadataValue<Ascii>>,
	spec: String,
	plan: &BurstPlan,
) -> Result<i32> {
	let spec = Arc::new(spec);
	let batch = batch_create_supported(&channels[0], bearer).await;
	let transport = if batch {
		"BatchCreate"
	} else {
		"unary Create (BatchCreate unimplemented)"
	};
	println!("transport: {transport}; {} connections (one lane each)", channels.len());

	let waves = options.waves.max(1);
	let mut aggregate = WaveOutcome::default();
	let mut any_failed = false;
	for wave in 1..=waves {
		if waves > 1 {
			println!("wave {wave}/{waves}: scheduling {} creates...", plan.count);
		}
		let mut outcome =
			run_open_loop(channels, bearer, &spec, options.timeout, plan, batch).await?;
		let label = if waves > 1 {
			format!(", wave {wave}/{waves}")
		} else {
			String::new()
		};
		report_open_loop(&label, &outcome, plan, options, transport, channels.len());
		any_failed |= outcome.failed() > 0;

		// Cleanup is excluded from the measured window and from the report.
		let sids = std::mem::take(&mut outcome.sids);
		if !options.keep {
			println!("cleanup: removing {} sandboxes...", sids.len());
			let started = Instant::now();
			let ours: HashSet<String> = sids.iter().cloned().collect();
			remove_all(channels, bearer, sids).await;
			wait_until_removed(&channels[0], bearer, &ours).await;
			println!("cleanup: done in {:.2}s", started.elapsed().as_secs_f64());
		}
		aggregate.absorb(&outcome);
	}
	if waves > 1 {
		aggregate.admissions.sort_unstable();
		aggregate.completions.sort_unstable();
		report_open_loop(", aggregate", &aggregate, plan, options, transport, channels.len());
	}
	Ok(i32::from(any_failed))
}

fn channel(server: &str) -> Result<Channel> {
	Ok(Endpoint::from_shared(server.to_owned())
		.map_err(|error| CliError::new(format!("invalid server URL: {error}")))?
		.connect_timeout(Duration::from_secs(5))
		.connect_lazy())
}

/// Size the connection pool so no HTTP/2 connection carries more than ~64
/// concurrent streams. `explicit` (from `--connections`) wins when nonzero.
fn connection_count(explicit: usize, plan: Option<&BurstPlan>, concurrency: usize) -> usize {
	if explicit > 0 {
		return explicit;
	}
	// Open loop: every request can be in flight at once in the worst case,
	// and each in-flight create may also hold a Watch stream (~2s of offered
	// load as a worst-case latency bound). Closed loop: exactly
	// `concurrency` streams.
	let streams =
		plan.map_or(concurrency as f64, |plan| (plan.count as f64).max(plan.offered_rps * 2.0));
	((streams / 64.0).ceil() as usize).clamp(8, 256)
}

fn system_client(channel: &Channel) -> pb::system_service_client::SystemServiceClient<Channel> {
	pb::system_service_client::SystemServiceClient::new(channel.clone())
}

/// Pure open-loop plan math: count fills the memory headroom, the offered
/// rate is the full-scale target scaled to live capacity.
fn plan_burst(
	capacity_mib: u64,
	used_mib: u64,
	options: &ScaledBurstOptions,
	memory_mib: u32,
) -> Result<BurstPlan> {
	if !options.target_rps.is_finite() || options.target_rps <= 0.0 {
		return Err(CliError::new("--target-rps must be finite and greater than zero"));
	}
	if options.reference_memory_mib == 0 {
		return Err(CliError::new("--reference-memory-mib must be greater than zero"));
	}
	if !(options.headroom.is_finite() && 0.0 < options.headroom && options.headroom <= 1.0) {
		return Err(CliError::new("--memory-headroom must be in (0, 1]"));
	}
	if memory_mib == 0 {
		return Err(CliError::new("sandbox memory must be greater than zero"));
	}
	let safe_capacity_mib = (capacity_mib as f64 * options.headroom).floor() as u64;
	let safe_budget_mib = safe_capacity_mib.saturating_sub(used_mib);
	let count_u64 = safe_budget_mib / u64::from(memory_mib);
	if count_u64 == 0 {
		return Err(CliError::new(format!(
			"no scaled-burst capacity: {safe_budget_mib} MiB budget for {memory_mib} MiB sandboxes"
		)));
	}
	let count = usize::try_from(count_u64)
		.map_err(|_| CliError::new("scaled-burst request count exceeds usize"))?;
	let offered_rps = options.target_rps * capacity_mib as f64 / options.reference_memory_mib as f64;
	if !offered_rps.is_finite() || offered_rps <= 0.0 {
		return Err(CliError::new("scaled offered rate is not finite and positive"));
	}
	let scheduled_window = Duration::from_secs_f64(count as f64 / offered_rps);
	Ok(BurstPlan { capacity_mib, used_mib, safe_budget_mib, count, offered_rps, scheduled_window })
}

async fn scaled_burst_plan(
	channel: &Channel,
	bearer: Option<&MetadataValue<Ascii>>,
	options: &ScaledBurstOptions,
	memory_mib: u32,
) -> Result<BurstPlan> {
	let mut client = system_client(channel);
	let request = authed(pb::InfoRequest {}, bearer);
	let response = tokio::time::timeout(Duration::from_secs(10), client.info(request))
		.await
		.map_err(|_| CliError::new("timed out reading scheduler capacity"))?
		.map_err(|status| {
			CliError::new(format!(
				"reading scheduler capacity failed ({}): {}",
				code_of(&status),
				status.message()
			))
		})?
		.into_inner();
	let info: serde_json::Value = serde_json::from_str(&response.json)?;
	let capacity_mib = info
		.pointer("/capacity/mem_mib")
		.and_then(serde_json::Value::as_u64)
		.ok_or_else(|| {
			CliError::new(
				"scaled bursts require a scheduler whose Info response includes capacity.mem_mib",
			)
		})?;
	let used_mib = info
		.pointer("/used/mem_mib")
		.and_then(serde_json::Value::as_u64)
		.unwrap_or(0);
	plan_burst(capacity_mib, used_mib, options, memory_mib)
}

/// Preconnect every channel concurrently (untimed warmup work).
async fn prime_channels(channels: &[Channel], bearer: Option<&MetadataValue<Ascii>>) -> Result<()> {
	let mut tasks = Vec::with_capacity(channels.len());
	for channel in channels {
		let mut client = system_client(channel);
		let request = authed(pb::InfoRequest {}, bearer);
		tasks.push(tokio::spawn(async move {
			tokio::time::timeout(Duration::from_secs(10), client.info(request)).await
		}));
	}
	for task in tasks {
		task
			.await
			.map_err(|error| CliError::new(format!("priming task failed: {error}")))?
			.map_err(|_| CliError::new("timed out priming benchmark connection"))?
			.map_err(|status| {
				CliError::new(format!(
					"priming benchmark connection failed ({}): {}",
					code_of(&status),
					status.message()
				))
			})?;
	}
	Ok(())
}

/// Probe once whether the target implements the `BatchCreate` firehose; an
/// empty request stream is side-effect free either way.
async fn batch_create_supported(channel: &Channel, bearer: Option<&MetadataValue<Ascii>>) -> bool {
	let (sender, receiver) = tokio::sync::mpsc::channel::<pb::BatchCreateRequest>(1);
	drop(sender);
	let mut client = sandbox_client(channel);
	let request = authed(ReceiverStream::new(receiver), bearer);
	match tokio::time::timeout(Duration::from_secs(10), client.batch_create(request)).await {
		Ok(Ok(_response)) => true,
		Ok(Err(status)) => {
			if status.code() != tonic::Code::Unimplemented {
				eprintln!(
					"warning: BatchCreate probe failed ({}); falling back to unary creates",
					status.message()
				);
			}
			false
		},
		Err(_elapsed) => {
			eprintln!("warning: BatchCreate probe timed out; falling back to unary creates");
			false
		},
	}
}

async fn create_once(
	client: &mut pb::sandbox_service_client::SandboxServiceClient<Channel>,
	bearer: Option<&MetadataValue<Ascii>>,
	spec: &str,
	timeout: Duration,
) -> CreateOutcome {
	let request =
		authed(pb::CreateSandboxRequest { spec_json: spec.to_owned(), no_wait: false }, bearer);
	let begun = Instant::now();
	match tokio::time::timeout(timeout, client.create(request)).await {
		Ok(Ok(response)) => {
			let view = response.into_inner().json;
			CreateOutcome::Created(Sample {
				latency: begun.elapsed(),
				node:    view_field(&view, "node").unwrap_or_else(|| "?".into()),
				sid:     view_field(&view, "name"),
			})
		},
		Ok(Err(status)) => CreateOutcome::Failed(code_of(&status)),
		Err(_elapsed) => CreateOutcome::Failed("client_timeout".into()),
	}
}

async fn fork_once(
	client: &mut pb::snapshot_service_client::SnapshotServiceClient<Channel>,
	bearer: Option<&MetadataValue<Ascii>>,
	snapshot: &str,
	timeout: Duration,
) -> CreateOutcome {
	let request = authed(
		pb::ForkSnapshotRequest {
			name:      snapshot.to_owned(),
			body_json: r#"{"count":1}"#.to_owned(),
		},
		bearer,
	);
	let begun = Instant::now();
	match tokio::time::timeout(timeout, client.fork(request)).await {
		Ok(Ok(response)) => {
			let response = response.into_inner();
			let Some(sample) = fork_sample(&response.json, begun.elapsed()) else {
				return CreateOutcome::Failed("invalid_response".into());
			};
			CreateOutcome::Created(sample)
		},
		Ok(Err(status)) => CreateOutcome::Failed(code_of(&status)),
		Err(_elapsed) => CreateOutcome::Failed("client_timeout".into()),
	}
}

async fn request_once(
	sandboxes: &mut pb::sandbox_service_client::SandboxServiceClient<Channel>,
	snapshots: &mut pb::snapshot_service_client::SnapshotServiceClient<Channel>,
	bearer: Option<&MetadataValue<Ascii>>,
	request: &BenchRequest,
	timeout: Duration,
) -> CreateOutcome {
	match request {
		BenchRequest::Create { spec } => create_once(sandboxes, bearer, spec, timeout).await,
		BenchRequest::Fork { snapshot } => fork_once(snapshots, bearer, snapshot, timeout).await,
	}
}

async fn run_closed_loop(
	channels: &[Channel],
	bearer: Option<&MetadataValue<Ascii>>,
	request: &BenchRequest,
	timeout: Duration,
	count: usize,
	concurrency: usize,
) -> Result<(Tally, Duration)> {
	let tally = Arc::new(Mutex::new(Tally::default()));
	let next = Arc::new(AtomicUsize::new(0));
	let started = Instant::now();
	let mut tasks = Vec::with_capacity(concurrency);
	for worker_index in 0..concurrency {
		let channel = channels[worker_index % channels.len()].clone();
		let bearer = bearer.cloned();
		let request = request.clone();
		let tally = Arc::clone(&tally);
		let next = Arc::clone(&next);
		tasks.push(tokio::spawn(async move {
			let mut sandboxes = sandbox_client(&channel);
			let mut snapshots = snapshot_client(&channel);
			loop {
				if next.fetch_add(1, Ordering::Relaxed) >= count {
					return;
				}
				let outcome =
					request_once(&mut sandboxes, &mut snapshots, bearer.as_ref(), &request, timeout)
						.await;
				tally.lock().expect("tally lock").record(outcome);
			}
		}));
	}
	for task in tasks {
		task
			.await
			.map_err(|error| CliError::new(format!("benchmark task failed: {error}")))?;
	}
	let wall = started.elapsed();
	let tally = Arc::try_unwrap(tally)
		.map_err(|_| CliError::new("benchmark tasks leaked"))?
		.into_inner()
		.expect("tally lock");
	Ok((tally, wall))
}

/// Schedule offset of request `index` at the offered rate.
fn fire_offset(index: usize, offered_rps: f64) -> Duration {
	Duration::from_secs_f64(index as f64 / offered_rps)
}

/// Indices lane `lane` owns: `lane, lane+lanes, lane+2·lanes, …`.
fn lane_indices(lane: usize, lanes: usize, count: usize) -> impl Iterator<Item = usize> {
	(lane..count).step_by(lanes.max(1))
}

/// Whether the drain has stalled: the schedule is over and nothing has
/// resolved for a full [`STALL_GRACE`].
fn drain_stalled(elapsed: Duration, window: Duration, last_event: Duration) -> bool {
	elapsed >= window + STALL_GRACE && elapsed.saturating_sub(last_event) >= STALL_GRACE
}

/// Trip the cutoff once the drain stops making progress, abandoning pending
/// watchers as `stalled`; exits silently when every fired request resolves.
async fn stall_monitor(
	gauges: Arc<Gauges>,
	start: tokio::time::Instant,
	window: Duration,
	cutoff: tokio::sync::watch::Sender<bool>,
) {
	let mut tick = tokio::time::interval(Duration::from_millis(100));
	loop {
		tick.tick().await;
		let elapsed = start.elapsed();
		let offered = gauges.offered.load(Ordering::Relaxed);
		let completed = gauges.completed.load(Ordering::Relaxed);
		if elapsed >= window && offered > 0 && completed >= offered {
			return;
		}
		if drain_stalled(elapsed, window, gauges.last_event()) {
			let unresolved = offered.saturating_sub(completed);
			eprintln!(
				"drain: no progress for {:.1}s; abandoning {unresolved} unresolved request(s)",
				STALL_GRACE.as_secs_f64(),
			);
			let _ = cutoff.send(true);
			return;
		}
	}
}

/// Resolves only when the stall monitor trips the cutoff; pends forever when
/// the wave completes naturally.
async fn cutoff_tripped(mut cutoff: tokio::sync::watch::Receiver<bool>) {
	if cutoff.wait_for(|&tripped| tripped).await.is_err() {
		std::future::pending::<()>().await;
	}
}

/// Fire one open-loop wave: requests fire at absolute instants sharded across
/// one lane per connection; lane tallies merge only after the window.
async fn run_open_loop(
	channels: &[Channel],
	bearer: Option<&MetadataValue<Ascii>>,
	spec: &Arc<String>,
	timeout: Duration,
	plan: &BurstPlan,
	batch: bool,
) -> Result<WaveOutcome> {
	let lanes = channels.len().max(1);
	let (cutoff_sender, cutoff) = tokio::sync::watch::channel(false);
	let context = OpenLoopContext {
		bearer: bearer.cloned(),
		spec: Arc::clone(spec),
		timeout,
		start: tokio::time::Instant::now() + Duration::from_millis(100),
		plan: *plan,
		lanes,
		gauges: Arc::new(Gauges::default()),
		cutoff,
	};
	let monitor = tokio::spawn(stall_monitor(
		Arc::clone(&context.gauges),
		context.start,
		plan.scheduled_window,
		cutoff_sender,
	));
	let mut pending = Vec::with_capacity(lanes);
	for (lane, channel) in channels.iter().enumerate().take(lanes) {
		let channel = channel.clone();
		let context = context.clone();
		pending.push(tokio::spawn(async move {
			if batch {
				open_loop_batch_lane(channel, context, lane).await
			} else {
				open_loop_unary_lane(channel, context, lane).await
			}
		}));
	}
	let mut merged = LaneTally::default();
	for lane in pending {
		let tally = lane
			.await
			.map_err(|error| CliError::new(format!("benchmark lane failed: {error}")))?;
		merged.merge(tally);
	}
	monitor.abort();
	let wall = context.start.elapsed();
	let interval = fire_offset(1, plan.offered_rps);
	let offered_window = match (merged.first_fire, merged.last_fire) {
		(Some(first), Some(last)) => last.duration_since(first) + interval,
		_ => Duration::ZERO,
	};
	merged.admissions.sort_unstable();
	merged.completions.sort_unstable();
	let mut failures = merged.failures;
	let busy = failures.remove("busy").unwrap_or(0);
	Ok(WaveOutcome {
		fired: context.gauges.offered.load(Ordering::Relaxed) as usize,
		admissions: merged.admissions,
		completions: merged.completions,
		failures,
		busy,
		sids: merged.sids,
		offered_window,
		admission_wall: context.gauges.admission_wall(),
		ready_wall: context.gauges.ready_wall(),
		wall,
		peak_in_flight: context.gauges.peak_in_flight.load(Ordering::Relaxed),
		peak_pending_ready: context.gauges.peak_pending_ready.load(Ordering::Relaxed),
	})
}

/// Unary fallback lane: each owned index sleeps to its absolute instant, then
/// the request runs as its own task so response latency never delays the
/// schedule.
async fn open_loop_unary_lane(
	channel: Channel,
	context: OpenLoopContext,
	lane: usize,
) -> LaneTally {
	let mut tally = LaneTally::default();
	let mut pending = Vec::new();
	for index in lane_indices(lane, context.lanes, context.plan.count) {
		let scheduled = context.start + fire_offset(index, context.plan.offered_rps);
		tokio::time::sleep_until(scheduled).await;
		context.gauges.fired();
		tally.note_fire(tokio::time::Instant::now());
		pending.push(tokio::spawn(fire_unary(channel.clone(), context.clone(), scheduled)));
	}
	for task in pending {
		match task.await {
			Ok(outcome) => tally.absorb(outcome),
			Err(_join) => tally.fail("task_panicked".into()),
		}
	}
	tally
}

async fn fire_unary(
	channel: Channel,
	context: OpenLoopContext,
	scheduled: tokio::time::Instant,
) -> FireOutcome {
	let mut client = sandbox_client(&channel);
	let request = authed(
		pb::CreateSandboxRequest { spec_json: (*context.spec).clone(), no_wait: true },
		context.bearer.as_ref(),
	);
	let response = tokio::select! {
		outcome = tokio::time::timeout(context.timeout, client.create(request)) => outcome,
		() = cutoff_tripped(context.cutoff.clone()) => {
			context
				.gauges
				.completed_before_admission(context.start.elapsed());
			return FireOutcome::failed("stalled".into());
		},
	};
	match response {
		Ok(Ok(response)) => {
			let now = tokio::time::Instant::now();
			let admission = now.duration_since(scheduled);
			context
				.gauges
				.admitted(now.saturating_duration_since(context.start));
			let view = response.into_inner().json;
			admitted_outcome(channel, context, view, admission, scheduled).await
		},
		Ok(Err(status)) => {
			context
				.gauges
				.completed_before_admission(context.start.elapsed());
			FireOutcome::failed(code_of(&status))
		},
		Err(_elapsed) => {
			context
				.gauges
				.completed_before_admission(context.start.elapsed());
			FireOutcome::failed("client_timeout".into())
		},
	}
}

/// An admitted create's remaining journey: an already-running synchronous
/// response counts ready immediately; otherwise readiness arrives on a
/// `Watch{until_ready}` stream.
async fn admitted_outcome(
	channel: Channel,
	context: OpenLoopContext,
	view: String,
	admission: Duration,
	scheduled: tokio::time::Instant,
) -> FireOutcome {
	let sid = view_field(&view, "name").or_else(|| view_field(&view, "id"));
	let Some(sid) = sid else {
		context
			.gauges
			.completed_after_admission(context.start.elapsed());
		return FireOutcome {
			admission:  Some(admission),
			completion: None,
			sid:        None,
			error:      Some("missing_sid".into()),
		};
	};
	if view_field(&view, "status").as_deref() == Some("running") {
		context
			.gauges
			.completed_ready(tokio::time::Instant::now().saturating_duration_since(context.start));
		return FireOutcome {
			admission:  Some(admission),
			completion: Some(admission),
			sid:        Some(sid),
			error:      None,
		};
	}
	let watched = tokio::select! {
		result = watch_ready(&channel, &context, &sid, scheduled) => result,
		() = cutoff_tripped(context.cutoff.clone()) => Err("stalled".to_owned()),
	};
	match watched {
		Ok(completion) => {
			context
				.gauges
				.completed_ready(tokio::time::Instant::now().saturating_duration_since(context.start));
			FireOutcome {
				admission:  Some(admission),
				completion: Some(completion),
				sid:        Some(sid),
				error:      None,
			}
		},
		Err(code) => {
			context
				.gauges
				.completed_after_admission(context.start.elapsed());
			FireOutcome {
				admission:  Some(admission),
				completion: None,
				sid:        Some(sid),
				error:      Some(code),
			}
		},
	}
}

/// Follow one sandbox's `Watch` stream until it reports `running`; the
/// returned duration is fire→ready against the scheduled instant.
async fn watch_ready(
	channel: &Channel,
	context: &OpenLoopContext,
	sid: &str,
	scheduled: tokio::time::Instant,
) -> std::result::Result<Duration, String> {
	let mut client = sandbox_client(channel);
	let request = authed(
		pb::WatchSandboxRequest { id: sid.to_owned(), until_ready: true },
		context.bearer.as_ref(),
	);
	let watched = tokio::time::timeout(context.timeout, async {
		let mut stream = client
			.watch(request)
			.await
			.map_err(|status| code_of(&status))?
			.into_inner();
		loop {
			match stream.message().await {
				Ok(Some(frame)) => match view_field(&frame.json, "status").as_deref() {
					Some("running") => {
						return Ok(tokio::time::Instant::now().duration_since(scheduled));
					},
					Some("failed") => return Err("failed".to_owned()),
					_ => {},
				},
				Ok(None) => return Err("watch_ended".to_owned()),
				Err(status) => return Err(code_of(&status)),
			}
		}
	})
	.await;
	match watched {
		Ok(inner) => inner,
		Err(_elapsed) => Err("ready_timeout".to_owned()),
	}
}

/// `BatchCreate` lane: one bidi stream per connection. A feeder task fires
/// seq-tagged items at their absolute instants; this task drains out-of-order
/// responses, correlating each seq back to its scheduled fire time.
async fn open_loop_batch_lane(
	channel: Channel,
	context: OpenLoopContext,
	lane: usize,
) -> LaneTally {
	let mut tally = LaneTally::default();
	let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<pb::BatchCreateRequest>();
	let mut client = sandbox_client(&channel);
	let call =
		client.batch_create(authed(UnboundedReceiverStream::new(receiver), context.bearer.as_ref()));

	let feeder = {
		let context = context.clone();
		tokio::spawn(async move {
			let mut fired = 0usize;
			let mut first_fire = None;
			let mut last_fire = None;
			for index in lane_indices(lane, context.lanes, context.plan.count) {
				let scheduled = context.start + fire_offset(index, context.plan.offered_rps);
				tokio::time::sleep_until(scheduled).await;
				let item = pb::BatchCreateRequest {
					seq:    index as u64,
					create: Some(pb::CreateSandboxRequest {
						spec_json: (*context.spec).clone(),
						no_wait:   true,
					}),
				};
				if sender.send(item).is_err() {
					break;
				}
				context.gauges.fired();
				let now = tokio::time::Instant::now();
				first_fire.get_or_insert(now);
				last_fire = Some(now);
				fired += 1;
			}
			(fired, first_fire, last_fire)
		})
	};

	let mut received = 0usize;
	let mut stream_error = None;
	let mut watchers = Vec::new();
	let drain_deadline = context.start + context.plan.scheduled_window + context.timeout;
	match call.await {
		Ok(response) => {
			let mut stream = response.into_inner();
			loop {
				let message = tokio::select! {
					message = tokio::time::timeout_at(drain_deadline, stream.message()) => message,
					() = cutoff_tripped(context.cutoff.clone()) => {
						stream_error = Some("stalled".into());
						break;
					},
				};
				let frame = match message {
					Ok(Ok(Some(frame))) => frame,
					Ok(Ok(None)) => break,
					Ok(Err(status)) => {
						stream_error = Some(code_of(&status));
						break;
					},
					Err(_elapsed) => {
						stream_error = Some("client_timeout".into());
						break;
					},
				};
				received += 1;
				let scheduled =
					context.start + fire_offset(frame.seq as usize, context.plan.offered_rps);
				match frame.outcome {
					Some(pb::batch_create_response::Outcome::Json(view)) => {
						let now = tokio::time::Instant::now();
						let admission = now.duration_since(scheduled);
						context
							.gauges
							.admitted(now.saturating_duration_since(context.start));
						watchers.push(tokio::spawn(admitted_outcome(
							channel.clone(),
							context.clone(),
							view,
							admission,
							scheduled,
						)));
					},
					Some(pb::batch_create_response::Outcome::Error(error)) => {
						context
							.gauges
							.completed_before_admission(context.start.elapsed());
						tally.fail(error.code);
					},
					None => {
						context
							.gauges
							.completed_before_admission(context.start.elapsed());
						tally.fail("invalid_response".into());
					},
				}
			}
		},
		Err(status) => stream_error = Some(code_of(&status)),
	}

	let (fired, first_fire, last_fire) = match feeder.await {
		Ok(result) => result,
		Err(_join) => {
			tally.fail("task_panicked".into());
			(0, None, None)
		},
	};
	tally.first_fire = first_fire;
	tally.last_fire = last_fire;
	let unresolved = fired.saturating_sub(received);
	let unresolved_code = stream_error.unwrap_or_else(|| "unresolved".into());
	let admission_elapsed = context.start.elapsed();
	for _ in 0..unresolved {
		context.gauges.completed_before_admission(admission_elapsed);
		tally.fail(unresolved_code.clone());
	}
	if received > fired {
		for _ in fired..received {
			tally.fail("invalid_response".into());
		}
	}
	for watcher in watchers {
		match watcher.await {
			Ok(outcome) => tally.absorb(outcome),
			Err(_join) => {
				context
					.gauges
					.completed_after_admission(context.start.elapsed());
				tally.fail("task_panicked".into());
			},
		}
	}
	tally
}

fn sandbox_client(channel: &Channel) -> pb::sandbox_service_client::SandboxServiceClient<Channel> {
	pb::sandbox_service_client::SandboxServiceClient::new(channel.clone())
		.max_decoding_message_size(64 * 1024 * 1024)
		.max_encoding_message_size(64 * 1024 * 1024)
}

fn snapshot_client(
	channel: &Channel,
) -> pb::snapshot_service_client::SnapshotServiceClient<Channel> {
	pb::snapshot_service_client::SnapshotServiceClient::new(channel.clone())
		.max_decoding_message_size(64 * 1024 * 1024)
		.max_encoding_message_size(64 * 1024 * 1024)
}

fn authed<T>(message: T, bearer: Option<&MetadataValue<Ascii>>) -> Request<T> {
	let mut request = Request::new(message);
	if let Some(bearer) = bearer {
		request
			.metadata_mut()
			.insert("authorization", bearer.clone());
	}
	request
}

fn code_of(status: &tonic::Status) -> String {
	status
		.metadata()
		.get("vmon-code")
		.and_then(|value| value.to_str().ok())
		.map_or_else(|| format!("{:?}", status.code()), str::to_owned)
}

fn view_field(json: &str, key: &str) -> Option<String> {
	serde_json::from_str::<serde_json::Value>(json)
		.ok()?
		.get(key)?
		.as_str()
		.map(str::to_owned)
}

fn fork_sample(json: &str, latency: Duration) -> Option<Sample> {
	let response = serde_json::from_str::<serde_json::Value>(json).ok()?;
	let view = response.get("clones")?.as_array()?.first()?;
	let sid = view.get("name")?.as_str()?.to_owned();
	let node = view
		.get("node")
		.and_then(serde_json::Value::as_str)
		.unwrap_or("?")
		.to_owned();
	Some(Sample { latency, node, sid: Some(sid) })
}

/// Remove sandboxes with bounded concurrency; failures only warn (timeouts
/// self-clean anything left behind). `NotFound` is success: failed sandboxes
/// reap themselves server-side before cleanup runs. Removal latency is
/// dominated by the worker waiting for each VMM to exit, so four tasks per
/// connection overlap those waits without extra connections.
async fn remove_all(
	channels: &[Channel],
	bearer: Option<&MetadataValue<Ascii>>,
	sids: Vec<String>,
) {
	let next = Arc::new(AtomicUsize::new(0));
	let sids = Arc::new(sids);
	let mut tasks = Vec::new();
	let workers = (channels.len() * 4).clamp(8, 128).min(sids.len().max(1));
	for index in 0..workers {
		let channel = channels[index % channels.len()].clone();
		let bearer = bearer.cloned();
		let next = Arc::clone(&next);
		let sids = Arc::clone(&sids);
		tasks.push(tokio::spawn(async move {
			let mut client = sandbox_client(&channel);
			loop {
				let slot = next.fetch_add(1, Ordering::Relaxed);
				let Some(sid) = sids.get(slot) else { return };
				let request = authed(pb::SandboxRef { id: sid.clone() }, bearer.as_ref());
				let outcome =
					tokio::time::timeout(Duration::from_mins(2), client.remove(request)).await;
				match outcome {
					Ok(Ok(_view)) => {},
					Ok(Err(status)) if status.code() == tonic::Code::NotFound => {},
					Ok(Err(status)) => {
						eprintln!("cleanup: remove {sid} failed: {}", status.message());
					},
					Err(_elapsed) => eprintln!("cleanup: remove {sid} timed out"),
				}
			}
		}));
	}
	for task in tasks {
		let _ = task.await;
	}
}

/// Poll `List` until none of our sandboxes remain (bounded at five minutes),
/// so the next wave starts against reclaimed capacity.
async fn wait_until_removed(
	channel: &Channel,
	bearer: Option<&MetadataValue<Ascii>>,
	sids: &HashSet<String>,
) {
	if sids.is_empty() {
		return;
	}
	let mut client = sandbox_client(channel);
	let deadline = Instant::now() + Duration::from_mins(5);
	loop {
		let request = authed(pb::ListSandboxesRequest { tags: Vec::new() }, bearer);
		let remaining =
			match tokio::time::timeout(Duration::from_secs(30), client.list(request)).await {
				Ok(Ok(response)) => response
					.into_inner()
					.sandboxes_json
					.iter()
					.filter_map(|view| view_field(view, "name").or_else(|| view_field(view, "id")))
					.filter(|name| sids.contains(name))
					.count(),
				// Transient list failure: keep polling until the deadline.
				Ok(Err(_)) | Err(_) => usize::MAX,
			};
		if remaining == 0 {
			return;
		}
		if Instant::now() >= deadline {
			if remaining == usize::MAX {
				eprintln!("cleanup: could not confirm removal within 5m");
			} else {
				eprintln!("cleanup: {remaining} benchmark sandboxes still listed after 5m");
			}
			return;
		}
		tokio::time::sleep(Duration::from_millis(500)).await;
	}
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
	if sorted.is_empty() {
		return Duration::ZERO;
	}
	let rank = ((sorted.len() - 1) as f64 * q).round() as usize;
	sorted[rank.min(sorted.len() - 1)]
}

fn print_latency(label: &str, sorted: &[Duration]) {
	if sorted.is_empty() {
		return;
	}
	println!(
		"{label} min {:.1}ms  p50 {:.1}ms  p90 {:.1}ms  p99 {:.1}ms  max {:.1}ms",
		sorted[0].as_secs_f64() * 1e3,
		percentile(sorted, 0.50).as_secs_f64() * 1e3,
		percentile(sorted, 0.90).as_secs_f64() * 1e3,
		percentile(sorted, 0.99).as_secs_f64() * 1e3,
		sorted[sorted.len() - 1].as_secs_f64() * 1e3,
	);
}

fn latency_json(sorted: &[Duration]) -> serde_json::Value {
	serde_json::json!({
		"min": sorted.first().map_or(0.0, |d| d.as_secs_f64() * 1e3),
		"p50": percentile(sorted, 0.50).as_secs_f64() * 1e3,
		"p90": percentile(sorted, 0.90).as_secs_f64() * 1e3,
		"p99": percentile(sorted, 0.99).as_secs_f64() * 1e3,
		"max": sorted.last().map_or(0.0, |d| d.as_secs_f64() * 1e3),
	})
}

/// Report one open-loop wave (or the multi-wave aggregate). Admission and
/// readiness are separate distributions; `busy` rejections are counted apart
/// from other failures.
fn report_open_loop(
	label: &str,
	outcome: &WaveOutcome,
	plan: &BurstPlan,
	options: &BenchOptions,
	transport: &str,
	lanes: usize,
) {
	let window_s = outcome.offered_window.as_secs_f64();
	let offered_rps = if window_s > 0.0 {
		outcome.fired as f64 / window_s
	} else {
		0.0
	};
	let admitted = outcome.admissions.len();
	let ready = outcome.completions.len();
	let admission_wall_s = outcome.admission_wall.as_secs_f64();
	let admitted_rps = if admission_wall_s > 0.0 {
		admitted as f64 / admission_wall_s
	} else {
		0.0
	};
	let ready_wall_s = outcome.ready_wall.as_secs_f64();
	let ready_rps = if ready_wall_s > 0.0 {
		ready as f64 / ready_wall_s
	} else {
		0.0
	};
	let failed: usize = outcome.failures.values().sum();

	println!("── vmon bench (open loop{label}) ──────────────────────");
	println!("target        {}", options.server);
	println!("transport     {transport} × {lanes} lanes");
	println!(
		"plan          {} creates at {:.2}/s over {:.2}ms",
		plan.count,
		plan.offered_rps,
		plan.scheduled_window.as_secs_f64() * 1e3,
	);
	println!(
		"capacity      {} MiB total, {} MiB used, {} MiB safe budget",
		plan.capacity_mib, plan.used_mib, plan.safe_budget_mib,
	);
	println!(
		"offered       {} fired in {:.2}ms → {offered_rps:.2} req/s",
		outcome.fired,
		window_s * 1e3,
	);
	println!(
		"admitted      {admitted} ({admitted_rps:.2}/s over {:.2}ms admission)",
		admission_wall_s * 1e3,
	);
	println!(
		"ready         {ready} ({ready_rps:.2}/s over {:.2}ms ready window)",
		ready_wall_s * 1e3,
	);
	println!("busy          {}", outcome.busy);
	println!("failed        {failed}");
	print_latency("admission    ", &outcome.admissions);
	print_latency("completion   ", &outcome.completions);
	println!(
		"peaks         in-flight {} (offered−completed), pending-ready {}",
		outcome.peak_in_flight, outcome.peak_pending_ready,
	);
	for (code, count) in &outcome.failures {
		println!("error         {code}: {count}");
	}
	println!("wall          {:.2}s", outcome.wall.as_secs_f64());

	if options.json {
		let summary = serde_json::json!({
			"target": options.server,
			"mode": "open-loop",
			"wave": label.trim_start_matches(", "),
			"transport": transport,
			"lanes": lanes,
			"plan": {
				"count": plan.count,
				"offered_rps": plan.offered_rps,
				"scheduled_window_s": plan.scheduled_window.as_secs_f64(),
			},
			"capacity": {
				"total_mib": plan.capacity_mib,
				"used_mib": plan.used_mib,
				"safe_budget_mib": plan.safe_budget_mib,
			},
			"offered": { "fired": outcome.fired, "window_s": window_s, "rps": offered_rps },
			"admitted": {
				"count": admitted,
				"wall_s": admission_wall_s,
				"rps": admitted_rps,
			},
			"ready": { "count": ready, "window_s": ready_wall_s, "rps": ready_rps },
			"busy": outcome.busy,
			"failed": failed,
			"admission_ms": latency_json(&outcome.admissions),
			"completion_ms": latency_json(&outcome.completions),
			"peak_in_flight": outcome.peak_in_flight,
			"peak_pending_ready": outcome.peak_pending_ready,
			"errors": outcome.failures,
			"wall_s": outcome.wall.as_secs_f64(),
		});
		println!("{summary}");
	}
}

fn report(tally: &Tally, wall: Duration, options: &BenchOptions) {
	let mut latencies: Vec<Duration> = tally.ok.iter().map(|sample| sample.latency).collect();
	latencies.sort_unstable();
	let ok = latencies.len();
	let failed: usize = tally.errors.values().sum();
	let throughput = ok as f64 / wall.as_secs_f64();
	let (operation, throughput_unit, scope) = if options.snapshot.is_some() {
		("snapshot-fork", "snapshot forks/s", "snapshot-fork")
	} else {
		("create", "ready creates/s", "vm-ready")
	};
	let mean = latencies
		.iter()
		.sum::<Duration>()
		.checked_div(ok.max(1) as u32)
		.unwrap_or_default();
	let mut by_node: BTreeMap<&str, usize> = BTreeMap::new();
	for sample in &tally.ok {
		*by_node.entry(sample.node.as_str()).or_insert(0) += 1;
	}

	println!("── vmon bench ─────────────────────────────────────────");
	println!("target        {}", options.server);
	if let Some(snapshot) = &options.snapshot {
		println!("snapshot      {snapshot}");
	}
	println!("requests      {ok} ok, {failed} failed");
	println!("wall          {:.2}s", wall.as_secs_f64());
	println!("throughput    {throughput:.2} {throughput_unit}");
	if ok > 0 {
		println!(
			"latency       min {:.0}ms  mean {:.0}ms  p50 {:.0}ms  p90 {:.0}ms  p99 {:.0}ms  max \
			 {:.0}ms",
			latencies[0].as_secs_f64() * 1e3,
			mean.as_secs_f64() * 1e3,
			percentile(&latencies, 0.50).as_secs_f64() * 1e3,
			percentile(&latencies, 0.90).as_secs_f64() * 1e3,
			percentile(&latencies, 0.99).as_secs_f64() * 1e3,
			latencies[ok - 1].as_secs_f64() * 1e3,
		);
	}
	for (node, count) in &by_node {
		println!("placement     {node}: {count}");
	}
	for (code, count) in &tally.errors {
		println!("error         {code}: {count}");
	}
	if options.json {
		let summary = serde_json::json!({
			"target": options.server,
			"operation": operation,
			"snapshot": options.snapshot,
			"ok": ok,
			"failed": failed,
			"wall_s": wall.as_secs_f64(),
			"requests_per_s": throughput,
			"scope": scope,
			"latency_ms": {
				"min": latencies.first().map_or(0.0, |d| d.as_secs_f64() * 1e3),
				"mean": mean.as_secs_f64() * 1e3,
				"p50": percentile(&latencies, 0.50).as_secs_f64() * 1e3,
				"p90": percentile(&latencies, 0.90).as_secs_f64() * 1e3,
				"p99": percentile(&latencies, 0.99).as_secs_f64() * 1e3,
				"max": latencies.last().map_or(0.0, |d| d.as_secs_f64() * 1e3),
			},
			"placement": by_node,
			"errors": tally.errors,
		});
		println!("{summary}");
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn percentiles_pick_expected_ranks() {
		let sorted: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
		// Nearest-rank on 0..=99: round((100-1)*0.5) = 50 → the 51st sample.
		assert_eq!(percentile(&sorted, 0.50), Duration::from_millis(51));
		assert_eq!(percentile(&sorted, 0.99), Duration::from_millis(99));
		assert_eq!(percentile(&sorted, 1.0), Duration::from_millis(100));
		assert_eq!(percentile(&sorted, 0.0), Duration::from_millis(1));
		assert_eq!(percentile(&[], 0.5), Duration::ZERO);
		let single = [Duration::from_millis(9)];
		assert_eq!(percentile(&single, 0.0), Duration::from_millis(9));
		assert_eq!(percentile(&single, 1.0), Duration::from_millis(9));
	}

	#[test]
	fn admission_wall_excludes_readiness_drain() {
		let gauges = Gauges::default();
		gauges.admitted(Duration::from_millis(20));
		gauges.completed_ready(Duration::from_millis(30));
		assert_eq!(gauges.admission_wall(), Duration::from_millis(20));

		gauges.completed_before_admission(Duration::from_millis(35));
		assert_eq!(
			gauges.admission_wall(),
			Duration::from_millis(35),
			"the last admission decision, not readiness, closes the admission window"
		);
	}

	#[test]
	fn ready_wall_tracks_last_ready_completion() {
		let gauges = Gauges::default();
		gauges.admitted(Duration::from_millis(5));
		gauges.admitted(Duration::from_millis(6));
		gauges.completed_ready(Duration::from_millis(90));
		gauges.completed_ready(Duration::from_millis(40));
		assert_eq!(
			gauges.ready_wall(),
			Duration::from_millis(90),
			"the latest ready completion closes the readiness window"
		);
	}

	#[test]
	fn last_event_tracks_every_progress_kind() {
		let gauges = Gauges::default();
		gauges.admitted(Duration::from_millis(10));
		assert_eq!(gauges.last_event(), Duration::from_millis(10));
		gauges.completed_before_admission(Duration::from_millis(20));
		gauges.completed_ready(Duration::from_millis(30));
		gauges.admitted(Duration::from_millis(31));
		gauges.completed_after_admission(Duration::from_millis(40));
		assert_eq!(gauges.last_event(), Duration::from_millis(40));
		// Progress never rewinds on an out-of-order sample.
		gauges.admitted(Duration::from_millis(35));
		assert_eq!(gauges.last_event(), Duration::from_millis(40));
	}

	#[test]
	fn drain_stall_needs_a_finished_schedule_and_a_quiet_grace() {
		let window = Duration::from_secs(1);
		let quiet = Duration::from_millis(500);
		// Still scheduling: never stalled, even with zero progress.
		assert!(!drain_stalled(window / 2, window, Duration::ZERO));
		// Schedule over but progress within the grace: not stalled.
		let busy = window + STALL_GRACE + Duration::from_secs(1);
		assert!(!drain_stalled(
			busy,
			window,
			busy.checked_sub(STALL_GRACE).unwrap() + Duration::from_millis(1)
		));
		// Schedule over and a full grace without progress: stalled.
		assert!(drain_stalled(busy, window, quiet));
		// The grace also gates right at the schedule boundary.
		assert!(!drain_stalled(
			(window + STALL_GRACE)
				.checked_sub(Duration::from_millis(1))
				.unwrap(),
			window,
			quiet
		));
	}

	#[test]
	fn fork_sample_reads_first_clone() {
		let latency = Duration::from_millis(7);
		let sample = fork_sample(
			r#"{"clones":[{"name":"fork-1","node":"worker-a","status":"running"}]}"#,
			latency,
		)
		.expect("fork sample");
		assert_eq!(sample.sid.as_deref(), Some("fork-1"));
		assert_eq!(sample.node, "worker-a");
		assert_eq!(sample.latency, latency);
		assert!(fork_sample(r#"{"clones":[]}"#, latency).is_none());
	}

	#[test]
	fn burst_plan_matches_full_scale_example() {
		// 786432 MiB × 0.9 headroom / 128 MiB = 5529 creates over 276.45 ms
		// at 20k/s when live capacity equals the reference.
		let options = ScaledBurstOptions {
			target_rps:           20_000.0,
			reference_memory_mib: 786_432,
			headroom:             0.9,
		};
		let plan = plan_burst(786_432, 0, &options, 128).expect("plan");
		assert_eq!(plan.count, 5529);
		assert_eq!(plan.safe_budget_mib, 707_788);
		assert!((plan.offered_rps - 20_000.0).abs() < 1e-9);
		let window_ms = plan.scheduled_window.as_secs_f64() * 1e3;
		assert!((window_ms - 276.45).abs() < 5e-3, "window {window_ms}ms");
	}

	#[test]
	fn burst_plan_scales_rate_with_live_capacity_and_budget_with_usage() {
		let options = ScaledBurstOptions {
			target_rps:           20_000.0,
			reference_memory_mib: 786_432,
			headroom:             0.9,
		};
		// Half the reference capacity offers half the rate.
		let half = plan_burst(393_216, 0, &options, 128).expect("plan");
		assert!((half.offered_rps - 10_000.0).abs() < 1e-9);
		assert_eq!(half.count, 353_894 / 128);
		// Used memory shrinks the budget (and count), never the rate.
		let used = plan_burst(786_432, 100_000, &options, 128).expect("plan");
		assert_eq!(used.count, (707_788 - 100_000) / 128);
		assert!((used.offered_rps - 20_000.0).abs() < 1e-9);
	}

	#[test]
	fn burst_plan_rejects_bad_inputs() {
		let ok = ScaledBurstOptions {
			target_rps:           20_000.0,
			reference_memory_mib: 786_432,
			headroom:             0.9,
		};
		assert!(plan_burst(64, 0, &ok, 128).is_err(), "budget below one sandbox");
		assert!(plan_burst(786_432, 0, &ok, 0).is_err(), "zero sandbox memory");
		let zero_rate = ScaledBurstOptions {
			target_rps:           0.0,
			reference_memory_mib: 786_432,
			headroom:             0.9,
		};
		assert!(plan_burst(786_432, 0, &zero_rate, 128).is_err());
		let bad_headroom = ScaledBurstOptions {
			target_rps:           20_000.0,
			reference_memory_mib: 786_432,
			headroom:             1.5,
		};
		assert!(plan_burst(786_432, 0, &bad_headroom, 128).is_err());
	}

	#[test]
	fn lanes_partition_the_schedule() {
		let lanes = 7;
		let count = 100;
		let mut hits = vec![0usize; count];
		for lane in 0..lanes {
			for index in lane_indices(lane, lanes, count) {
				assert_eq!(index % lanes, lane, "lane {lane} fired foreign index {index}");
				hits[index] += 1;
			}
		}
		assert!(hits.iter().all(|&seen| seen == 1), "indices fired exactly once");
		assert_eq!(lane_indices(0, 1, 3).collect::<Vec<_>>(), vec![0, 1, 2]);
		assert_eq!(lane_indices(5, 8, 4).count(), 0, "lane beyond count is empty");
	}

	#[test]
	fn fire_offsets_follow_the_offered_rate() {
		assert_eq!(fire_offset(0, 20_000.0), Duration::ZERO);
		assert!((fire_offset(1, 20_000.0).as_secs_f64() - 5e-5).abs() < 1e-12);
		// The last request of the full-scale example fires just inside the window.
		let last = fire_offset(5528, 20_000.0).as_secs_f64();
		assert!((last - 0.2764).abs() < 1e-9);
	}

	#[test]
	fn connection_sizing_bounds_streams_per_connection() {
		// Closed loop: ceil(concurrency/64) clamped to [8, 256].
		assert_eq!(connection_count(0, None, 32), 8);
		assert_eq!(connection_count(0, None, 4096), 64);
		assert_eq!(connection_count(0, None, 1_000_000), 256);
		// Explicit --connections wins.
		assert_eq!(connection_count(3, None, 4096), 3);
		// Open loop: sized from worst-case concurrent streams.
		let plan = BurstPlan {
			capacity_mib:     786_432,
			used_mib:         0,
			safe_budget_mib:  707_788,
			count:            5529,
			offered_rps:      20_000.0,
			scheduled_window: Duration::from_secs_f64(0.27645),
		};
		// max(5529, 40000)/64 = 625 → clamped to 256.
		assert_eq!(connection_count(0, Some(&plan), 32), 256);
		let small = BurstPlan { count: 640, offered_rps: 100.0, ..plan };
		// max(640, 200)/64 = 10.
		assert_eq!(connection_count(0, Some(&small), 32), 10);
	}
}
