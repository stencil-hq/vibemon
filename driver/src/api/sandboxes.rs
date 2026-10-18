//! Sandbox, snapshot, and admin-sandbox endpoints of the E2B API.
//!
//! Wire reference: `spec/e2b/infra/spec/openapi.yml`; semantics in
//! ARCHITECTURE.md §9.4. Handlers validate the request and delegate
//! transitions to [`crate::lifecycle`].

pub mod dto;
pub mod listing;

use std::{
	collections::{BTreeMap, HashMap},
	time::Duration,
};

use axum::{
	Json, Router,
	body::Bytes,
	extract::{Path, RawQuery, State},
	http::{HeaderMap, HeaderValue, StatusCode},
	response::{IntoResponse, Response},
	routing::{get, post, put},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use vmon_cluster::kv::{Compare, Op, Txn};
use vmon_node::api::{LogQuery, MetricSample, RouteState, SandboxRoute};

use self::dto::{
	AdminKillDto, AdminListedSandboxDto, AdminSandboxDto, ConnectBody, ErrorDto, ForkBody,
	ForkResultDto, LifecycleDto, ListedSandboxDto, LogEntryDto, LogLineDto, LogsV1Dto, LogsV2Dto,
	MetricDto, MountDto, NewSandboxBody, PauseBody, RefreshBody, ResumeBody, SandboxDetailDto,
	SandboxDto, SandboxesWithMetricsDto, SnapshotBody, TimeoutBody, rfc3339,
};
use crate::{
	auth::{AdminAuth, TeamAuth},
	builds,
	error::{ApiError, ApiResult},
	lifecycle::{
		self, Connected, CreateSpec, KillReason, SandboxRef, datetime,
		limits::{self, CLIENT_ID, V1_DEFAULT_TIMEOUT_S, V2_DEFAULT_TIMEOUT_S, normalize_sandbox_id},
		network::NetworkUpdate,
		now_ms,
		routes::{get_route, list_routes, team_prefix},
		unix_ms,
	},
	state::AppState,
	store::{
		Team,
		sandboxes::{ListAfter, ListOrder, SandboxFilter, SandboxRow, SandboxState},
	},
};

/// Retention of `Idempotency-Key` records (upstream default 24 h).
const IDEMPOTENCY_TTL: Duration = Duration::from_hours(24);
/// A pending idempotent create older than this has an unknown outcome (422).
const IDEMPOTENCY_STALE_MS: u64 = 600_000;
/// Max IDs in `GET /sandboxes/metrics`.
const MAX_METRIC_IDS: usize = 100;
/// Max `search` length of the v2 logs API.
const MAX_LOG_SEARCH: usize = 256;
/// Max/default page of the logs APIs.
const MAX_LOG_LIMIT: u32 = 1000;

/// Sandbox routes (merged by [`crate::api::router`]).
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/sandboxes", get(list_v1).post(create_v1))
		.route("/v2/sandboxes", get(list_v2).post(create_v2))
		.route("/sandboxes/metrics", get(metrics_batch))
		.route("/sandboxes/{sandbox_id}", get(detail).delete(kill))
		.route("/sandboxes/{sandbox_id}/logs", get(logs_v1))
		.route("/v2/sandboxes/{sandbox_id}/logs", get(logs_v2))
		.route("/sandboxes/{sandbox_id}/metrics", get(metrics))
		.route("/sandboxes/{sandbox_id}/pause", post(pause))
		.route("/sandboxes/{sandbox_id}/resume", post(resume))
		.route("/sandboxes/{sandbox_id}/fork", post(fork))
		.route("/sandboxes/{sandbox_id}/connect", post(connect_v1))
		.route("/v2/sandboxes/{sandbox_id}/connect", post(connect_v2))
		.route("/sandboxes/{sandbox_id}/timeout", post(set_timeout))
		.route("/sandboxes/{sandbox_id}/network", put(update_network))
		.route("/sandboxes/{sandbox_id}/refreshes", post(refresh))
		.route("/sandboxes/{sandbox_id}/snapshots", post(snapshot))
		.route("/snapshots", get(list_snapshots))
		.route("/admin/teams/{team_id}/sandboxes/kill", post(admin_kill_team))
		.route("/admin/sandboxes", get(admin_list))
		.route("/admin/sandboxes/running-counts", get(admin_running_counts))
		.route("/admin/sandboxes/{sandbox_id}", get(admin_sandbox))
}

// ---------------------------------------------------------------- helpers

fn parse_json<T: DeserializeOwned>(body: &Bytes) -> ApiResult<T> {
	serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("validation error: {e}")))
}

fn parse_optional_json<T: DeserializeOwned + Default>(body: &Bytes) -> ApiResult<T> {
	if body.iter().all(u8::is_ascii_whitespace) {
		Ok(T::default())
	} else {
		parse_json(body)
	}
}

/// Decoded query pairs (repeated keys preserved).
fn query_pairs(raw: Option<&str>) -> Vec<(String, String)> {
	url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
		.into_owned()
		.collect()
}

fn query_first<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
	pairs
		.iter()
		.find(|(k, _)| k == key)
		.map(|(_, v)| v.as_str())
}

/// Every value of `key`, each also split on `,` (explode and CSV styles).
fn query_list(pairs: &[(String, String)], key: &str) -> Vec<String> {
	pairs
		.iter()
		.filter(|(k, _)| k == key)
		.flat_map(|(_, v)| {
			v.split(',')
				.map(str::trim)
				.filter(|s| !s.is_empty())
				.map(str::to_owned)
		})
		.collect()
}

fn query_int(pairs: &[(String, String)], key: &str) -> ApiResult<Option<i64>> {
	query_first(pairs, key)
		.filter(|v| !v.is_empty())
		.map(|v| {
			v.parse::<i64>()
				.map_err(|_| ApiError::bad_request(format!("{key} must be an integer")))
		})
		.transpose()
}

fn domain(state: &AppState) -> Option<String> {
	Some(state.config.domain.clone()).filter(|d| !d.is_empty())
}

fn sandbox_dto(state: &AppState, sandbox: &SandboxRef) -> SandboxDto {
	SandboxDto {
		template_id:          sandbox.template_id.clone(),
		sandbox_id:           sandbox.sandbox_id.clone(),
		alias:                sandbox.alias.clone(),
		client_id:            CLIENT_ID.to_owned(),
		envd_version:         sandbox.envd_version.clone(),
		envd_access_token:    state.keys.envd_token(&sandbox.sandbox_id),
		traffic_access_token: (!sandbox.allow_public_traffic)
			.then(|| state.keys.traffic_token(&sandbox.sandbox_id)),
		domain:               domain(state),
	}
}

fn with_header(mut response: Response, name: &'static str, value: &str) -> Response {
	if let Ok(value) = HeaderValue::from_str(value) {
		response.headers_mut().insert(name, value);
	}
	response
}

// ----------------------------------------------------------------- create

async fn create_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	body: Bytes,
) -> ApiResult<Response> {
	let body: NewSandboxBody = parse_json(&body)?;
	let spec = create_spec(&auth.team, body, V1_DEFAULT_TIMEOUT_S)?;
	let sandbox = lifecycle::create(&state, &auth.team, spec).await?;
	Ok((StatusCode::CREATED, Json(sandbox_dto(&state, &sandbox))).into_response())
}

async fn create_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	headers: HeaderMap,
	raw: Bytes,
) -> ApiResult<Response> {
	let body: NewSandboxBody = parse_json(&raw)?;
	let spec = create_spec(&auth.team, body, V2_DEFAULT_TIMEOUT_S)?;
	let Some(key) = headers.get("idempotency-key") else {
		let sandbox = lifecycle::create(&state, &auth.team, spec).await?;
		return Ok((StatusCode::CREATED, Json(sandbox_dto(&state, &sandbox))).into_response());
	};
	let key = key
		.to_str()
		.map_err(|_| ApiError::bad_request("Invalid Idempotency-Key"))?;
	let idem = Idempotency::begin(&state, &auth.team.id, key, &raw).await?;
	let record_key = match idem {
		IdemStart::Replay(dto) => return Ok((StatusCode::CREATED, Json(dto)).into_response()),
		IdemStart::Fresh(record_key) => record_key,
	};
	match lifecycle::create(&state, &auth.team, spec).await {
		Ok(sandbox) => {
			let dto = sandbox_dto(&state, &sandbox);
			Idempotency::finish(&state, &record_key, &raw, &dto).await;
			Ok((StatusCode::CREATED, Json(dto)).into_response())
		},
		Err(error) => {
			state.cluster.kv.delete(&record_key).await.ok();
			Err(error)
		},
	}
}

/// Validates a create body into a [`CreateSpec`] (shared by v1/v2).
fn create_spec(team: &Team, body: NewSandboxBody, default_timeout_s: i64) -> ApiResult<CreateSpec> {
	if body.template_id.trim().is_empty() {
		return Err(ApiError::bad_request("templateID is required"));
	}
	if body
		.iam
		.as_ref()
		.and_then(|iam| iam.tokens.as_ref())
		.is_some_and(|tokens| !tokens.is_empty())
	{
		return Err(ApiError::bad_request(
			"Sandbox workload identity (iam) is not supported by this deployment",
		));
	}
	let timeout_ms = limits::validate_timeout(
		body.timeout.unwrap_or(default_timeout_s),
		team.limits.max_length_hours,
	)?;
	let flags = limits::lifecycle_flags(
		body.auto_pause,
		body.auto_pause_memory,
		body.auto_resume.map(|a| a.enabled),
	)?;
	let s3_mounts = body.vmon_s3_mounts.unwrap_or_default();
	let volume_paths: Vec<String> = body
		.volume_mounts
		.iter()
		.flatten()
		.map(|mount| mount.path.clone())
		.collect();
	lifecycle::s3mounts::validate(&s3_mounts, &volume_paths)?;
	Ok(CreateSpec {
		template_ref: body.template_id,
		timeout_ms,
		flags,
		allow_internet_access: body.allow_internet_access,
		network: body.network.unwrap_or_default(),
		metadata: body.metadata.unwrap_or_default(),
		env_vars: body.env_vars.unwrap_or_default(),
		volume_mounts: body
			.volume_mounts
			.unwrap_or_default()
			.into_iter()
			.map(Into::into)
			.collect(),
		s3_mounts,
	})
}

/// `idem/<teamID>/<key>` record of an `Idempotency-Key` create.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdemRecord {
	/// SHA-256 of the request body.
	hash:       String,
	created_ms: u64,
	/// The 201 body once the create succeeded.
	response:   Option<SandboxDto>,
}

enum IdemStart {
	/// First request with this key; the record key to finish/delete.
	Fresh(String),
	/// Completed earlier with the same body: replay the response.
	Replay(SandboxDto),
}

struct Idempotency;

impl Idempotency {
	async fn begin(state: &AppState, team_id: &str, key: &str, body: &[u8]) -> ApiResult<IdemStart> {
		if key.is_empty()
			|| key.len() > 255
			|| !key
				.bytes()
				.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
		{
			return Err(ApiError::bad_request("Idempotency-Key must match ^[a-z0-9]{1,255}$"));
		}
		let record_key = format!("idem/{team_id}/{key}");
		let hash = hex::encode(Sha256::digest(body));
		let record = IdemRecord { hash: hash.clone(), created_ms: now_ms(), response: None };
		let txn = Txn {
			compare: vec![Compare::Missing(record_key.clone())],
			success: vec![Op::Put {
				key:    record_key.clone(),
				value:  serde_json::to_vec(&record).expect("record serializes"),
				ttl_ms: Some(ttl_ms(IDEMPOTENCY_TTL)),
			}],
			failure: Vec::new(),
		};
		if state.cluster.kv.txn(txn).await?.succeeded {
			return Ok(IdemStart::Fresh(record_key));
		}
		let existing = state.cluster.kv.get_consistent(&record_key).await?;
		let Some(existing) =
			existing.and_then(|e| serde_json::from_slice::<IdemRecord>(&e.value).ok())
		else {
			return Err(
				ApiError::conflict("A request with this Idempotency-Key is in progress")
					.with_code("idempotency_in_progress"),
			);
		};
		classify_idempotent(&existing, &hash, now_ms())
	}

	async fn finish(state: &AppState, record_key: &str, body: &[u8], dto: &SandboxDto) {
		let record = IdemRecord {
			hash:       hex::encode(Sha256::digest(body)),
			created_ms: now_ms(),
			response:   Some(dto.clone()),
		};
		let value = serde_json::to_vec(&record).expect("record serializes");
		if let Err(error) = state
			.cluster
			.kv
			.put(record_key, value, Some(IDEMPOTENCY_TTL))
			.await
		{
			tracing::warn!(%error, "storing idempotent create outcome failed");
		}
	}
}

/// Decides how a repeated `Idempotency-Key` is answered.
fn classify_idempotent(existing: &IdemRecord, hash: &str, now: u64) -> ApiResult<IdemStart> {
	if existing.hash != hash {
		return Err(
			ApiError::conflict("Idempotency-Key was already used with a different request")
				.with_code("idempotency_request_mismatch"),
		);
	}
	if let Some(response) = &existing.response {
		return Ok(IdemStart::Replay(response.clone()));
	}
	if now.saturating_sub(existing.created_ms) > IDEMPOTENCY_STALE_MS {
		return Err(
			ApiError::unprocessable(
				"The outcome of the earlier request with this Idempotency-Key is unknown",
			)
			.with_code("idempotency_outcome_unknown"),
		);
	}
	Err(
		ApiError::conflict("A request with this Idempotency-Key is in progress")
			.with_code("idempotency_in_progress"),
	)
}

fn ttl_ms(ttl: Duration) -> u64 {
	u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}

// ------------------------------------------------------------------- list

/// Live view of a row: KV route state overrides the durable state.
enum Live {
	Running(Option<Box<SandboxRoute>>),
	Paused,
	Hidden,
}

async fn live_state(state: &AppState, row: &SandboxRow) -> ApiResult<Live> {
	Ok(live_view(get_route(&state.cluster.kv, &row.id).await?, row))
}

/// Overlays a row's KV route (if any) on its durable state.
fn live_view(route: Option<SandboxRoute>, row: &SandboxRow) -> Live {
	match (route, row.state()) {
		(Some(route), _) if route.state == RouteState::Killing => Live::Hidden,
		(Some(route), _) if route.state == RouteState::Pausing => Live::Paused,
		(Some(route), _) => Live::Running(Some(Box::new(route))),
		(None, SandboxState::Paused) => Live::Paused,
		(None, SandboxState::Running) => Live::Running(None),
		(None, SandboxState::Killed) => Live::Hidden,
	}
}

fn listed(row: &SandboxRow, live: &Live) -> Option<ListedSandboxDto> {
	let (state, end_at) = match live {
		Live::Hidden => return None,
		Live::Running(route) => {
			("running", route.as_ref().map_or(row.end_at, |r| datetime(r.end_at_ms)))
		},
		Live::Paused => ("paused", row.paused_at.unwrap_or(row.end_at)),
	};
	Some(ListedSandboxDto {
		template_id: row.template_id.clone(),
		alias: row.alias.clone(),
		sandbox_id: row.id.clone(),
		client_id: CLIENT_ID.to_owned(),
		started_at: rfc3339(row.started_at),
		end_at: rfc3339(end_at),
		cpu_count: row.vcpus,
		memory_mb: row.memory_mb,
		disk_size_mb: row.disk_mb,
		metadata: row.metadata.clone(),
		state,
		envd_version: row.envd_version.clone(),
		volume_mounts: row.volume_mounts.iter().map(MountDto::from).collect(),
	})
}

async fn list_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let pairs = query_pairs(raw.as_deref());
	let filter = SandboxFilter {
		team_id: Some(&auth.team.id),
		states: vec![SandboxState::Running],
		metadata: listing::parse_metadata_filter(query_first(&pairs, "metadata"))?,
		..SandboxFilter::default()
	};
	let mut out = Vec::new();
	let mut after: Option<ListAfter> = None;
	loop {
		let rows = state
			.store
			.list_sandboxes(&filter, Default::default(), after.as_ref(), 500)
			.await?;
		let done = rows.len() < 500;
		for row in &rows {
			let live = live_state(&state, row).await?;
			if matches!(live, Live::Running(_))
				&& let Some(dto) = listed(row, &live)
			{
				out.push(dto);
			}
		}
		match rows.last() {
			Some(last) if !done => {
				after = Some(ListAfter { started_at: last.started_at, id: last.id.clone() });
			},
			_ => break,
		}
	}
	Ok(Json(out).into_response())
}

async fn list_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let pairs = query_pairs(raw.as_deref());
	let states = listing::parse_states(Some(query_list(&pairs, "state").join(",")).as_deref())?;
	let order = listing::parse_order(query_first(&pairs, "order"))?;
	let limit = listing::parse_limit(query_int(&pairs, "limit")?)?;
	let after = query_first(&pairs, "nextToken")
		.filter(|t| !t.is_empty())
		.map(|t| listing::decode_token(t, order))
		.transpose()?;
	let filter = SandboxFilter {
		team_id:       Some(&auth.team.id),
		states:        states.clone(),
		metadata:      listing::parse_metadata_filter(query_first(&pairs, "metadata"))?,
		started_after: listing::parse_started_after(query_first(&pairs, "startedAfter"))?,
		template:      query_first(&pairs, "template").filter(|t| !t.is_empty()),
	};
	let mut rows = state
		.store
		.list_sandboxes(&filter, order, after.as_ref(), limit + 1)
		.await?;
	let more = rows.len() > limit as usize;
	rows.truncate(limit as usize);
	let mut out = Vec::with_capacity(rows.len());
	for row in &rows {
		if let Some(dto) = listed(row, &live_state(&state, row).await?) {
			out.push(dto);
		}
	}
	let mut response = Json(out).into_response();
	if more && let Some(last) = rows.last() {
		let token = listing::encode_token(
			&ListAfter { started_at: last.started_at, id: last.id.clone() },
			order,
		);
		response = with_header(response, "x-next-token", &token);
	}
	if states.contains(&SandboxState::Running) {
		let running = SandboxFilter { states: vec![SandboxState::Running], ..filter };
		let total = state.store.count_sandboxes(&running).await?;
		response = with_header(response, "x-total-running", &total.to_string());
	}
	Ok(response)
}

// ----------------------------------------------------------------- detail

async fn owned_row(state: &AppState, team: &Team, id: &str) -> ApiResult<SandboxRow> {
	state
		.store
		.sandbox(id)
		.await?
		.filter(|row| row.team_id == team.id)
		.ok_or_else(|| ApiError::sandbox_not_found(id))
}

async fn detail(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let row = owned_row(&state, &auth.team, &id).await?;
	let live = live_state(&state, &row).await?;
	let (state_name, end_at, domain, mounts) = match &live {
		Live::Hidden => return Err(ApiError::sandbox_not_found(&id)),
		Live::Running(route) => (
			"running",
			route.as_ref().map_or(row.end_at, |r| datetime(r.end_at_ms)),
			domain(&state),
			Some(row.volume_mounts.iter().map(MountDto::from).collect()),
		),
		Live::Paused => ("paused", row.paused_at.unwrap_or(row.end_at), None, None),
	};
	let started_at = match &live {
		Live::Running(Some(route)) => datetime(route.started_at_ms),
		_ => row.started_at,
	};
	let dto = SandboxDetailDto {
		template_id: row.template_id.clone(),
		alias: row.alias.clone(),
		sandbox_id: row.id.clone(),
		client_id: CLIENT_ID.to_owned(),
		started_at: rfc3339(started_at),
		end_at: rfc3339(end_at),
		envd_version: row.envd_version.clone(),
		envd_access_token: state.keys.envd_token(&row.id),
		allow_internet_access: row.allow_internet_access,
		domain,
		cpu_count: row.vcpus,
		memory_mb: row.memory_mb,
		disk_size_mb: row.disk_mb,
		metadata: row.metadata.clone(),
		state: state_name,
		network: row.network.redacted(),
		lifecycle: LifecycleDto {
			auto_resume: row.auto_resume,
			on_timeout:  if row.auto_pause { "pause" } else { "kill" },
		},
		volume_mounts: mounts,
	};
	Ok(Json(dto).into_response())
}

// -------------------------------------------------------------- lifecycle

async fn kill(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
) -> ApiResult<StatusCode> {
	let id = normalize_sandbox_id(&raw_id)?;
	lifecycle::kill(&state, Some(&auth.team.id), &id, KillReason::Request).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn pause(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<StatusCode> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: PauseBody = parse_optional_json(&body)?;
	lifecycle::pause(&state, Some(&auth.team.id), &id, body.memory.unwrap_or(true)).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn resume(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: ResumeBody = parse_optional_json(&body)?;
	let timeout_ms = limits::validate_timeout(
		body.timeout.unwrap_or(V1_DEFAULT_TIMEOUT_S),
		auth.team.limits.max_length_hours,
	)?;
	let cold_boot = body.memory == Some(false);
	let sandbox = lifecycle::resume(&state, &auth.team, &id, timeout_ms, cold_boot).await?;
	Ok((StatusCode::CREATED, Json(sandbox_dto(&state, &sandbox))).into_response())
}

async fn connect_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: ConnectBody = parse_json(&body)?;
	let timeout_s = body
		.timeout
		.ok_or_else(|| ApiError::bad_request("timeout is required"))?;
	// v1 only checks the upper bound; zero keeps the current expiry of a running
	// sandbox.
	let timeout_ms = if timeout_s <= 0 {
		0
	} else {
		limits::validate_timeout(timeout_s, auth.team.limits.max_length_hours)?
	};
	connect_response(&state, &auth.team, &id, timeout_ms, body.memory == Some(false)).await
}

async fn connect_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: ConnectBody = parse_optional_json(&body)?;
	let timeout_ms = limits::validate_timeout(
		body.timeout.unwrap_or(V2_DEFAULT_TIMEOUT_S),
		auth.team.limits.max_length_hours,
	)?;
	connect_response(&state, &auth.team, &id, timeout_ms, body.memory == Some(false)).await
}

async fn connect_response(
	state: &AppState,
	team: &Team,
	id: &str,
	timeout_ms: u64,
	cold_boot: bool,
) -> ApiResult<Response> {
	Ok(match lifecycle::connect(state, team, id, timeout_ms, cold_boot).await? {
		Connected::Running(sandbox) => {
			(StatusCode::OK, Json(sandbox_dto(state, &sandbox))).into_response()
		},
		Connected::Resumed(sandbox) => {
			(StatusCode::CREATED, Json(sandbox_dto(state, &sandbox))).into_response()
		},
	})
}

async fn set_timeout(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<StatusCode> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: TimeoutBody = parse_json(&body)?;
	let timeout = body
		.timeout
		.ok_or_else(|| ApiError::bad_request("timeout is required"))?;
	lifecycle::set_timeout(&state, &auth.team, &id, timeout).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn refresh(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<StatusCode> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: RefreshBody = parse_optional_json(&body)?;
	let duration_ms = limits::refresh_duration_ms(body.duration)?;
	lifecycle::refresh(&state, &auth.team, &id, duration_ms).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn update_network(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<StatusCode> {
	let id = normalize_sandbox_id(&raw_id)?;
	let update: NetworkUpdate = parse_optional_json(&body)?;
	lifecycle::update_network(&state, &auth.team, &id, &update).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn snapshot(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: SnapshotBody = parse_optional_json(&body)?;
	let name = body.name.as_deref().filter(|n| !n.is_empty());
	let target =
		lifecycle::snapshot(&state, &auth.team, &id, name, body.memory.unwrap_or(true)).await?;
	let info = builds::SnapshotInfo { snapshot_id: target.snapshot_id, names: target.names };
	Ok((StatusCode::CREATED, Json(info)).into_response())
}

async fn list_snapshots(
	State(state): State<AppState>,
	auth: TeamAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let pairs = query_pairs(raw.as_deref());
	let limit = listing::parse_limit(query_int(&pairs, "limit")?)?;
	let sandbox_id = query_first(&pairs, "sandboxID").filter(|s| !s.is_empty());
	let name = query_first(&pairs, "name").filter(|s| !s.is_empty());
	let token = query_first(&pairs, "nextToken").filter(|s| !s.is_empty());
	let (items, next) =
		builds::list_snapshots(&state, &auth.team, sandbox_id, name, limit, token).await?;
	let response = Json(items).into_response();
	Ok(match next {
		Some(next) => with_header(response, "x-next-token", &next),
		None => response,
	})
}

async fn fork(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	body: Bytes,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let body: ForkBody = parse_optional_json(&body)?;
	let limits = auth.team.limits;
	let count = limits::validate_fork_count(body.count, limits.max_concurrent_sandboxes)?;
	let timeout_ms = limits::validate_timeout(
		body.timeout.unwrap_or(V1_DEFAULT_TIMEOUT_S),
		limits.max_length_hours,
	)?;
	let results = lifecycle::fork(&state, &auth.team, &id, timeout_ms, count).await?;
	let out: Vec<ForkResultDto> = results
		.iter()
		.map(|result| match result {
			Ok(sandbox) => {
				ForkResultDto { sandbox: Some(sandbox_dto(&state, sandbox)), error: None }
			},
			Err(error) => ForkResultDto { sandbox: None, error: Some(ErrorDto::from(error)) },
		})
		.collect();
	Ok((StatusCode::CREATED, Json(out)).into_response())
}

// ------------------------------------------------------------ logs/metrics

async fn logs_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let pairs = query_pairs(raw.as_deref());
	let start = query_int(&pairs, "start")?
		.filter(|s| *s >= 0)
		.and_then(|s| u64::try_from(s).ok());
	let limit = query_int(&pairs, "limit")?
		.map_or(MAX_LOG_LIMIT, |l| u32::try_from(l.max(0)).unwrap_or(u32::MAX));
	let (client, _) = lifecycle::node_for(&state, &auth.team, &id).await?;
	let query = LogQuery {
		cursor:    start,
		limit:     Some(limit),
		direction: Some("forward".into()),
		level:     None,
		search:    None,
	};
	let entries = client.logs(&id, &query).await?;
	let dto = LogsV1Dto {
		logs:        entries.iter().map(LogLineDto::from).collect(),
		log_entries: entries.iter().map(LogEntryDto::from).collect(),
	};
	Ok(Json(dto).into_response())
}

async fn logs_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let pairs = query_pairs(raw.as_deref());
	let cursor = query_int(&pairs, "cursor")?
		.map(|c| u64::try_from(c).map_err(|_| ApiError::bad_request("cursor must be >= 0")))
		.transpose()?;
	let limit = match query_int(&pairs, "limit")? {
		None => MAX_LOG_LIMIT,
		Some(l) => u32::try_from(l)
			.ok()
			.filter(|l| *l <= MAX_LOG_LIMIT)
			.ok_or_else(|| {
				ApiError::bad_request(format!("limit must be between 0 and {MAX_LOG_LIMIT}"))
			})?,
	};
	let direction = match query_first(&pairs, "direction") {
		None | Some("" | "backward") => "backward",
		Some("forward") => "forward",
		Some(other) => return Err(ApiError::bad_request(format!("Invalid direction: {other}"))),
	};
	let level = match query_first(&pairs, "level") {
		None | Some("") => None,
		Some(level @ ("debug" | "info" | "warn" | "error")) => Some(level.to_owned()),
		Some(other) => return Err(ApiError::bad_request(format!("Invalid level: {other}"))),
	};
	let search = query_first(&pairs, "search")
		.filter(|s| !s.is_empty())
		.map(str::to_owned);
	if search.as_ref().is_some_and(|s| s.len() > MAX_LOG_SEARCH) {
		return Err(ApiError::bad_request(format!(
			"search must be at most {MAX_LOG_SEARCH} characters"
		)));
	}
	let (client, _) = lifecycle::node_for(&state, &auth.team, &id).await?;
	let query =
		LogQuery { cursor, limit: Some(limit), direction: Some(direction.into()), level, search };
	let entries = client.logs(&id, &query).await?;
	Ok(Json(LogsV2Dto { logs: entries.iter().map(LogEntryDto::from).collect() }).into_response())
}

/// Sample spacing for a metrics range (upstream `ClickHouse` step table).
const fn metrics_step_ms(range_ms: u64) -> u64 {
	const HOUR: u64 = 3_600_000;
	if range_ms < HOUR {
		5_000
	} else if range_ms < 6 * HOUR {
		30_000
	} else if range_ms < 12 * HOUR {
		60_000
	} else if range_ms < 24 * HOUR {
		120_000
	} else if range_ms < 7 * 24 * HOUR {
		300_000
	} else {
		900_000
	}
}

/// Keeps the latest sample of every `step_ms` bucket counted from `start_ms`.
fn downsample(mut samples: Vec<MetricSample>, start_ms: u64, step_ms: u64) -> Vec<MetricSample> {
	samples.sort_by_key(|s| s.timestamp_ms);
	let mut out: Vec<MetricSample> = Vec::with_capacity(samples.len());
	let mut last_bucket = None;
	for sample in samples {
		let bucket = sample.timestamp_ms.saturating_sub(start_ms) / step_ms;
		if last_bucket == Some(bucket) {
			*out.last_mut().expect("bucket has a sample") = sample;
		} else {
			out.push(sample);
			last_bucket = Some(bucket);
		}
	}
	out
}

async fn metrics(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(raw_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let id = normalize_sandbox_id(&raw_id)?;
	let pairs = query_pairs(raw.as_deref());
	let secs = |key: &str| -> ApiResult<Option<u64>> {
		query_int(&pairs, key)?
			.map(|v| {
				u64::try_from(v)
					.map(|s| s * 1000)
					.map_err(|_| ApiError::bad_request(format!("{key} must be >= 0")))
			})
			.transpose()
	};
	let (start, end) = (secs("start")?, secs("end")?);
	let (client, row) = lifecycle::node_for(&state, &auth.team, &id).await?;
	let start = start.unwrap_or_else(|| unix_ms(row.started_at));
	let end = end.unwrap_or_else(|| row.ended_at.map_or_else(now_ms, unix_ms));
	if start > end {
		return Err(ApiError::bad_request("start must not be after end"));
	}
	let samples = client.metrics(&id, Some(start), Some(end)).await?;
	let stepped = downsample(samples, start, metrics_step_ms(end - start));
	Ok(Json(stepped.iter().map(MetricDto::from).collect::<Vec<_>>()).into_response())
}

async fn metrics_batch(
	State(state): State<AppState>,
	auth: TeamAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let pairs = query_pairs(raw.as_deref());
	let mut ids = query_list(&pairs, "sandbox_ids");
	ids.dedup();
	if ids.len() > MAX_METRIC_IDS {
		return Err(ApiError::bad_request(format!(
			"at most {MAX_METRIC_IDS} sandbox IDs are allowed"
		)));
	}
	let now = now_ms();
	let lookups = ids.iter().map(|raw_id| {
		let state = &state;
		let team = &auth.team;
		async move {
			let id = normalize_sandbox_id(raw_id).ok()?;
			let route = get_route(&state.cluster.kv, &id).await.ok()??;
			if route.team_id != team.id {
				return None;
			}
			let client = state.node(route.node_id).ok()?;
			let samples = client
				.metrics(&id, Some(now.saturating_sub(60_000)), Some(now))
				.await
				.ok()?;
			let latest = samples.iter().max_by_key(|s| s.timestamp_ms)?;
			Some((id, MetricDto::from(latest)))
		}
	});
	let sandboxes: BTreeMap<String, MetricDto> = futures::future::join_all(lookups)
		.await
		.into_iter()
		.flatten()
		.collect();
	Ok(Json(SandboxesWithMetricsDto { sandboxes }).into_response())
}

// ------------------------------------------------------------------ admin

async fn admin_kill_team(
	State(state): State<AppState>,
	_admin: AdminAuth,
	Path(team_id): Path<String>,
) -> ApiResult<Json<AdminKillDto>> {
	if state.store.team_by_id(&team_id).await?.is_none() {
		return Err(ApiError::not_found(format!("team '{team_id}' not found")));
	}
	let (killed_count, failed_count) = lifecycle::kill_team(&state, &team_id).await?;
	Ok(Json(AdminKillDto { killed_count, failed_count }))
}

/// `GET /admin/sandboxes` item of `row` with its live overlay; `None` when
/// hidden.
fn admin_listed(
	row: &SandboxRow,
	route: Option<SandboxRoute>,
	team_name: String,
) -> Option<AdminListedSandboxDto> {
	let node_id = route
		.as_ref()
		.map_or_else(|| row.node_id.clone(), |r| r.node_id.to_string());
	listed(row, &live_view(route, row)).map(|sandbox| AdminListedSandboxDto {
		sandbox,
		team_id: row.team_id.clone(),
		team_name,
		node_id,
	})
}

/// `GET /admin/sandboxes` (vmon extension): every team's running/paused
/// sandboxes, newest first, keyset paginated like `/v2/sandboxes`; `id`
/// narrows it to that one sandbox (no pagination).
async fn admin_list(
	State(state): State<AppState>,
	_admin: AdminAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Response> {
	let pairs = query_pairs(raw.as_deref());
	let filter = SandboxFilter {
		team_id: query_first(&pairs, "team").filter(|t| !t.is_empty()),
		states: listing::parse_states(Some(query_list(&pairs, "state").join(",")).as_deref())?,
		..SandboxFilter::default()
	};
	if let Some(raw_id) = query_first(&pairs, "id").filter(|id| !id.is_empty()) {
		return Ok(Json(admin_one(&state, &filter, raw_id).await?).into_response());
	}
	let order = ListOrder::Desc;
	let limit = listing::parse_admin_limit(query_int(&pairs, "limit")?)?;
	let after = query_first(&pairs, "nextToken")
		.filter(|t| !t.is_empty())
		.map(|t| listing::decode_token(t, order))
		.transpose()?;
	let mut rows = state
		.store
		.list_sandboxes(&filter, order, after.as_ref(), limit + 1)
		.await?;
	let more = rows.len() > limit as usize;
	rows.truncate(limit as usize);
	let mut routes: HashMap<String, SandboxRoute> =
		list_routes(&state.cluster.kv).await?.into_iter().collect();
	let team_names: HashMap<String, String> = state
		.store
		.list_teams()
		.await?
		.into_iter()
		.map(|team| (team.id, team.name))
		.collect();
	let out: Vec<AdminListedSandboxDto> = rows
		.iter()
		.filter_map(|row| {
			let team_name = team_names.get(&row.team_id).cloned().unwrap_or_default();
			admin_listed(row, routes.remove(&row.id), team_name)
		})
		.collect();
	let mut response = Json(out).into_response();
	if more && let Some(last) = rows.last() {
		let token = listing::encode_token(
			&ListAfter { started_at: last.started_at, id: last.id.clone() },
			order,
		);
		response = with_header(response, "x-next-token", &token);
	}
	Ok(response)
}

/// `GET /admin/sandboxes?id=`: `[row]` when the sandbox passes `filter` (same
/// durable-state/team semantics as the listing) and is not hidden, else `[]`.
async fn admin_one(
	state: &AppState,
	filter: &SandboxFilter<'_>,
	raw_id: &str,
) -> ApiResult<Vec<AdminListedSandboxDto>> {
	let id = normalize_sandbox_id(raw_id)?;
	let Some(row) = state.store.sandbox(&id).await?.filter(|row| {
		filter.states.contains(&row.state()) && filter.team_id.is_none_or(|t| t == row.team_id)
	}) else {
		return Ok(Vec::new());
	};
	let route = get_route(&state.cluster.kv, &id).await?;
	let team_name = state
		.store
		.team_by_id(&row.team_id)
		.await?
		.map(|team| team.name)
		.unwrap_or_default();
	Ok(admin_listed(&row, route, team_name).into_iter().collect())
}

/// Counts admission-index entries per team (`team/<t>/sbx/<id>`).
fn count_index(keys: impl IntoIterator<Item = String>) -> BTreeMap<String, i64> {
	let mut counts = BTreeMap::new();
	for key in keys {
		let Some(rest) = key.strip_prefix("team/") else {
			continue;
		};
		let Some((team, id)) = rest.split_once("/sbx/") else {
			continue;
		};
		if !id.is_empty() && key.starts_with(&team_prefix(team)) {
			*counts.entry(team.to_owned()).or_insert(0) += 1;
		}
	}
	counts
}

async fn admin_running_counts(
	State(state): State<AppState>,
	_admin: AdminAuth,
) -> ApiResult<Json<BTreeMap<String, i64>>> {
	let entries = state.cluster.kv.list("team/").await?;
	Ok(Json(count_index(entries.into_iter().map(|e| e.key))))
}

async fn admin_sandbox(
	State(state): State<AppState>,
	_admin: AdminAuth,
	Path(raw_id): Path<String>,
) -> ApiResult<Json<AdminSandboxDto>> {
	let id = normalize_sandbox_id(&raw_id)?;
	let route = get_route(&state.cluster.kv, &id).await?;
	let row = state.store.sandbox(&id).await?;
	let (team_id, state_name) = match (&route, &row) {
		(Some(route), _) => (route.team_id.clone(), match route.state {
			RouteState::Killing => "killed",
			RouteState::Pausing => "paused",
			RouteState::Running | RouteState::Resuming | RouteState::Snapshotting => "running",
		}),
		(None, Some(row)) => (row.team_id.clone(), row.state().as_str()),
		(None, None) => return Err(ApiError::sandbox_not_found(&id)),
	};
	Ok(Json(AdminSandboxDto { sandbox_id: id, team_id, state: state_name }))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn metric_steps_follow_upstream_table() {
		const H: u64 = 3_600_000;
		assert_eq!(metrics_step_ms(H - 1), 5_000);
		assert_eq!(metrics_step_ms(H), 30_000);
		assert_eq!(metrics_step_ms(6 * H), 60_000);
		assert_eq!(metrics_step_ms(12 * H), 120_000);
		assert_eq!(metrics_step_ms(24 * H), 300_000);
		assert_eq!(metrics_step_ms(7 * 24 * H), 900_000);
	}

	#[test]
	fn downsample_keeps_latest_per_bucket_in_time_order() {
		let s = |t: u64| MetricSample {
			timestamp_ms: t,
			cpu_count: u32::try_from(t).unwrap(),
			..MetricSample::default()
		};
		let out = downsample(vec![s(31_000), s(1_000), s(29_000), s(61_000), s(5_000)], 0, 30_000);
		assert_eq!(out.iter().map(|m| m.timestamp_ms).collect::<Vec<_>>(), vec![
			29_000, 31_000, 61_000
		]);
	}

	#[test]
	fn idempotency_classification() {
		let pending = IdemRecord { hash: "h".into(), created_ms: 1_000, response: None };
		let busy = classify_idempotent(&pending, "h", 2_000).err().unwrap();
		assert_eq!(
			(busy.status, busy.error_code),
			(StatusCode::CONFLICT, Some("idempotency_in_progress"))
		);
		let stale = classify_idempotent(&pending, "h", 1_000 + IDEMPOTENCY_STALE_MS + 1)
			.err()
			.unwrap();
		assert_eq!(
			(stale.status, stale.error_code),
			(StatusCode::UNPROCESSABLE_ENTITY, Some("idempotency_outcome_unknown"))
		);
		let mismatch = classify_idempotent(&pending, "other", 2_000).err().unwrap();
		assert_eq!(mismatch.error_code, Some("idempotency_request_mismatch"));
		let dto = SandboxDto {
			template_id:          "t".into(),
			sandbox_id:           "i1".into(),
			alias:                None,
			client_id:            CLIENT_ID.into(),
			envd_version:         "0.6.4".into(),
			envd_access_token:    "x".into(),
			traffic_access_token: None,
			domain:               None,
		};
		let done = IdemRecord { response: Some(dto.clone()), ..pending };
		assert!(
			matches!(classify_idempotent(&done, "h", 2_000), Ok(IdemStart::Replay(d)) if d == dto)
		);
		// A different body still conflicts even after completion.
		assert!(classify_idempotent(&done, "other", 2_000).is_err());
	}

	#[test]
	fn running_counts_group_index_keys_by_team() {
		let keys = ["team/a/sbx/i1", "team/a/sbx/i2", "team/b/sbx/i3", "team/c/other", "team/d/sbx/"]
			.map(String::from);
		let counts = count_index(keys);
		assert_eq!(counts, BTreeMap::from([("a".to_owned(), 2), ("b".to_owned(), 1)]));
	}

	#[test]
	fn create_spec_applies_version_defaults_and_rejects_iam() {
		let team = Team {
			id:         "t".into(),
			name:       "t".into(),
			slug:       "t".into(),
			limits:     crate::state::TeamLimits { max_length_hours: 1, ..Default::default() },
			blocked:    false,
			system:     false,
			created_at: chrono::Utc::now(),
		};
		let body = |v: serde_json::Value| serde_json::from_value::<NewSandboxBody>(v).unwrap();
		let v2 =
			create_spec(&team, body(serde_json::json!({"templateID": "base"})), V2_DEFAULT_TIMEOUT_S)
				.unwrap();
		assert_eq!(v2.timeout_ms, 300_000);
		let v1 =
			create_spec(&team, body(serde_json::json!({"templateID": "base"})), V1_DEFAULT_TIMEOUT_S)
				.unwrap();
		assert_eq!(v1.timeout_ms, 15_000);
		assert!(
			create_spec(&team, body(serde_json::json!({"templateID": "base", "timeout": 3601})), 300)
				.is_err()
		);
		let iam = serde_json::json!({"templateID": "base", "iam": {"tokens": {"a": {"audience": "x", "tokenType": "JWT-SVID"}}}});
		assert!(create_spec(&team, body(iam), 300).is_err());
		let empty_iam = serde_json::json!({"templateID": "base", "iam": {"tokens": {}}});
		assert!(create_spec(&team, body(empty_iam), 300).is_ok());
	}
}
