//! Template, build, tag, and alias endpoints
//! (`spec/e2b/infra/spec/openapi.yml`). Domain logic lives in
//! [`crate::builds`].

use std::collections::HashMap;

use axum::{
	Json, Router,
	body::Body,
	extract::{Path, Query, State},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{get, post, put},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use super::pagination::{PageQuery, paged_json, split_page};
use crate::{
	auth::{AdminAuth, TeamAuth},
	builds::{
		self, BuildRequest, StartRequest, TemplateAccess,
		names::{DEFAULT_TAG, build_tags, parse_name, valid_files_hash},
	},
	error::{ApiError, ApiResult},
	state::AppState,
	store::{
		Cursor,
		builds::{BuildRow, BuildStatus, LogDirection, LogLevel, LogRow},
		micros,
		templates::{AliasRow, TemplateFilter, TemplateKind, TemplateRow},
	},
};

/// Maximum log entries per status/logs page.
const MAX_LOG_PAGE: u32 = 100;

/// Template routes (merged by [`crate::api::router`]).
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/v3/templates", post(request_build))
		.route("/v2/templates", get(list_templates_v2))
		.route("/templates", get(list_templates_v1))
		.route("/templates/tags", post(assign_tags).delete(delete_tags))
		.route("/templates/aliases/{alias}", get(check_alias))
		.route(
			"/templates/{template_id}",
			get(get_template)
				.delete(delete_template)
				.patch(update_template_v1),
		)
		.route("/v2/templates/{template_id}", axum::routing::patch(update_template_v2))
		.route("/templates/{template_id}/tags", get(list_tags))
		.route("/templates/{template_id}/files/{hash}", get(file_upload_link))
		.route("/templates/{template_id}/files/{hash}/upload", put(upload_files))
		.route("/v2/templates/{template_id}/builds/{build_id}", post(start_build))
		.route("/templates/{template_id}/builds/{build_id}/status", get(build_status))
		.route("/templates/{template_id}/builds/{build_id}/logs", get(build_logs))
		.route("/admin/teams/{team_id}/builds/cancel", post(admin_cancel_builds))
}

/// RFC 3339 with milliseconds, `Z` suffix.
fn rfc3339(at: DateTime<Utc>) -> String {
	at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ---------------------------------------------------------------- DTOs ----

/// `TemplateRequestResponseV3`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TemplateRequestResponse {
	#[serde(rename = "templateID")]
	template_id: String,
	#[serde(rename = "buildID")]
	build_id:    String,
	public:      bool,
	names:       Vec<String>,
	tags:        Vec<String>,
	aliases:     Vec<String>,
}

/// E2B `Template`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TemplateDto {
	#[serde(rename = "templateID")]
	template_id:     String,
	#[serde(rename = "buildID")]
	build_id:        String,
	cpu_count:       u32,
	#[serde(rename = "memoryMB")]
	memory_mb:       u64,
	#[serde(rename = "diskSizeMB")]
	disk_size_mb:    u64,
	public:          bool,
	aliases:         Vec<String>,
	names:           Vec<String>,
	created_at:      String,
	updated_at:      String,
	created_by:      Option<serde_json::Value>,
	last_spawned_at: Option<String>,
	spawn_count:     i64,
	build_count:     i32,
	envd_version:    String,
	build_status:    &'static str,
}

/// E2B `TemplateBuild`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TemplateBuildDto {
	#[serde(rename = "buildID")]
	build_id:     String,
	status:       &'static str,
	created_at:   String,
	updated_at:   String,
	#[serde(skip_serializing_if = "Option::is_none")]
	finished_at:  Option<String>,
	cpu_count:    u32,
	#[serde(rename = "memoryMB")]
	memory_mb:    u64,
	#[serde(rename = "diskSizeMB")]
	disk_size_mb: u64,
	#[serde(skip_serializing_if = "Option::is_none")]
	envd_version: Option<String>,
}

impl From<&BuildRow> for TemplateBuildDto {
	fn from(build: &BuildRow) -> Self {
		Self {
			build_id:     build.id.clone(),
			status:       build.status().as_str(),
			created_at:   rfc3339(build.created_at),
			updated_at:   rfc3339(build.updated_at),
			finished_at:  build.finished_at.map(rfc3339),
			cpu_count:    build.vcpus,
			memory_mb:    build.memory_mb,
			disk_size_mb: build.disk_mb,
			envd_version: build.envd_version.clone(),
		}
	}
}

/// E2B `TemplateWithBuilds`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TemplateWithBuildsDto {
	#[serde(rename = "templateID")]
	template_id:     String,
	public:          bool,
	aliases:         Vec<String>,
	names:           Vec<String>,
	created_at:      String,
	updated_at:      String,
	last_spawned_at: Option<String>,
	spawn_count:     i64,
	builds:          Vec<TemplateBuildDto>,
}

/// E2B `BuildLogEntry`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LogEntryDto {
	timestamp: String,
	message:   String,
	level:     String,
	#[serde(skip_serializing_if = "Option::is_none")]
	step:      Option<String>,
}

impl From<&LogRow> for LogEntryDto {
	fn from(row: &LogRow) -> Self {
		Self {
			timestamp: rfc3339(row.ts),
			message:   row.message.clone(),
			level:     row.level.clone(),
			step:      row.step.clone(),
		}
	}
}

/// E2B `BuildStatusReason`.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReasonDto {
	message:     String,
	#[serde(skip_serializing_if = "Option::is_none")]
	step:        Option<String>,
	log_entries: Vec<LogEntryDto>,
}

/// E2B `TemplateBuildInfo`.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BuildInfoDto {
	logs:        Vec<String>,
	log_entries: Vec<LogEntryDto>,
	#[serde(rename = "templateID")]
	template_id: String,
	#[serde(rename = "buildID")]
	build_id:    String,
	status:      &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	reason:      Option<ReasonDto>,
}

/// Assembles `TemplateBuildInfo` from a build and one log page: `waiting`
/// builds report no entries; `reason.logEntries` are the page's entries of
/// the failed step at level ≥ warn.
pub(crate) fn build_info(template_id: &str, build: &BuildRow, page: &[LogRow]) -> BuildInfoDto {
	let status = build.status();
	let entries: Vec<LogEntryDto> = if status == BuildStatus::Waiting {
		Vec::new()
	} else {
		page.iter().map(LogEntryDto::from).collect()
	};
	let reason = match (status, &build.reason) {
		(BuildStatus::Error, Some(reason)) => Some(ReasonDto {
			message:     reason.message.clone(),
			step:        reason.step.clone(),
			log_entries: page
				.iter()
				.filter(|row| {
					row.step == reason.step && LogLevel::normalize(&row.level) >= LogLevel::Warn
				})
				.map(LogEntryDto::from)
				.collect(),
		}),
		(BuildStatus::Error, None) => Some(ReasonDto {
			message:     "The build failed.".into(),
			step:        None,
			log_entries: Vec::new(),
		}),
		_ => None,
	};
	BuildInfoDto {
		logs: Vec::new(),
		log_entries: entries,
		template_id: template_id.to_owned(),
		build_id: build.id.clone(),
		status: status.as_str(),
		reason,
	}
}

fn names_of(aliases: &[AliasRow]) -> (Vec<String>, Vec<String>) {
	let names = aliases.iter().map(AliasRow::name).collect();
	let mut bare: Vec<String> = Vec::new();
	for alias in aliases {
		if !bare.contains(&alias.alias) {
			bare.push(alias.alias.clone());
		}
	}
	(names, bare)
}

/// `Template` DTOs for rows (batched lookups); rows without builds are skipped.
async fn template_dtos(state: &AppState, rows: &[TemplateRow]) -> ApiResult<Vec<TemplateDto>> {
	let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
	let mut aliases: HashMap<String, Vec<AliasRow>> = HashMap::new();
	for alias in state.store.aliases_of(&ids).await? {
		aliases
			.entry(alias.template_id.clone())
			.or_default()
			.push(alias);
	}
	let latest: HashMap<String, BuildRow> = state
		.store
		.latest_builds(&ids)
		.await?
		.into_iter()
		.map(|b| (b.template_id.clone(), b))
		.collect();
	let ready: HashMap<String, BuildRow> = state
		.store
		.latest_ready_builds(&ids)
		.await?
		.into_iter()
		.map(|b| (b.template_id.clone(), b))
		.collect();
	let mut out = Vec::with_capacity(rows.len());
	for row in rows {
		let Some(latest_build) = latest.get(&row.id) else {
			continue;
		};
		let shown = ready.get(&row.id).unwrap_or(latest_build);
		let (names, bare) = names_of(aliases.get(&row.id).map_or(&[][..], Vec::as_slice));
		out.push(TemplateDto {
			template_id: row.id.clone(),
			build_id: shown.id.clone(),
			cpu_count: shown.vcpus,
			memory_mb: shown.memory_mb,
			disk_size_mb: shown.disk_mb,
			public: row.public,
			aliases: bare,
			names,
			created_at: rfc3339(row.created_at),
			updated_at: rfc3339(row.updated_at),
			created_by: None,
			last_spawned_at: row.last_spawned_at.map(rfc3339),
			spawn_count: row.spawn_count,
			build_count: row.build_count,
			envd_version: shown.envd_version.clone().unwrap_or_default(),
			build_status: latest_build.status().as_str(),
		});
	}
	Ok(out)
}

fn cursor_of(row: &TemplateRow) -> Cursor {
	Cursor { created_at_us: micros(row.created_at), id: row.id.clone() }
}

// ------------------------------------------------------------ handlers ----

async fn request_build(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(req): Json<BuildRequest>,
) -> ApiResult<Response> {
	let registered = builds::register_build(&state, &auth.team, &req).await?;
	let body = TemplateRequestResponse {
		template_id: registered.template_id,
		build_id:    registered.build_id,
		public:      registered.public,
		names:       registered.names,
		tags:        registered.tags,
		aliases:     registered.aliases,
	};
	Ok((StatusCode::ACCEPTED, Json(body)).into_response())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TeamQuery {
	#[serde(rename = "teamID")]
	team_id: Option<String>,
	#[serde(flatten)]
	page:    PageQuery,
}

fn check_team_param(auth: &TeamAuth, team_id: Option<&str>) -> ApiResult<()> {
	match team_id {
		Some(id) if !id.is_empty() && id != auth.team.id => {
			Err(ApiError::forbidden("You are not allowed to access this team"))
		},
		_ => Ok(()),
	}
}

async fn list_templates_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	Query(query): Query<TeamQuery>,
) -> ApiResult<Response> {
	check_team_param(&auth, query.team_id.as_deref())?;
	let page = query.page.page()?;
	let rows = state
		.store
		.list_templates(
			&auth.team.id,
			TemplateKind::Template,
			&TemplateFilter::default(),
			page.after.as_ref(),
			Some(page.limit + 1),
		)
		.await?;
	let (rows, next) = split_page(rows, page.limit, cursor_of);
	Ok(paged_json(template_dtos(&state, &rows).await?, next))
}

async fn list_templates_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	Query(query): Query<TeamQuery>,
) -> ApiResult<Json<Vec<TemplateDto>>> {
	check_team_param(&auth, query.team_id.as_deref())?;
	let rows = state
		.store
		.list_templates(&auth.team.id, TemplateKind::Template, &TemplateFilter::default(), None, None)
		.await?;
	Ok(Json(template_dtos(&state, &rows).await?))
}

async fn get_template(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(template_ref): Path<String>,
	Query(query): Query<PageQuery>,
) -> ApiResult<Response> {
	let page = query.page()?;
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	let rows = state
		.store
		.template_builds(&template.id, page.after.as_ref(), Some(page.limit + 1))
		.await?;
	let (rows, next) = split_page(rows, page.limit, |b: &BuildRow| Cursor {
		created_at_us: micros(b.created_at),
		id:            b.id.clone(),
	});
	let aliases = state
		.store
		.aliases_of(std::slice::from_ref(&template.id))
		.await?;
	let (names, bare) = names_of(&aliases);
	let body = TemplateWithBuildsDto {
		template_id: template.id,
		public: template.public,
		aliases: bare,
		names,
		created_at: rfc3339(template.created_at),
		updated_at: rfc3339(template.updated_at),
		last_spawned_at: template.last_spawned_at.map(rfc3339),
		spawn_count: template.spawn_count,
		builds: rows.iter().map(TemplateBuildDto::from).collect(),
	};
	let mut response = Json(body).into_response();
	if let Some(next) = next
		&& let Ok(value) = next.encode().parse()
	{
		response
			.headers_mut()
			.insert(super::pagination::NEXT_TOKEN_HEADER, value);
	}
	Ok(response)
}

async fn delete_template(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(template_ref): Path<String>,
) -> ApiResult<StatusCode> {
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	builds::delete_template(&state, &template.id).await?;
	Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct UpdateTemplate {
	public: Option<bool>,
}

/// Deprecated v1 update: toggles `public`. Upstream, publishing also claims
/// bare (non-namespaced) aliases; here bare aliases are reserved for system
/// teams, whose publish claims them on either version
/// ([`crate::store::Store::set_template_public`]).
async fn update_template_v1(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(template_ref): Path<String>,
	Json(req): Json<UpdateTemplate>,
) -> ApiResult<StatusCode> {
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	if let Some(public) = req.public {
		state
			.store
			.set_template_public(&template.id, public)
			.await?;
	}
	Ok(StatusCode::OK)
}

#[derive(Debug, Serialize)]
struct TemplateUpdateResponse {
	names: Vec<String>,
}

async fn update_template_v2(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(template_ref): Path<String>,
	Json(req): Json<UpdateTemplate>,
) -> ApiResult<Json<TemplateUpdateResponse>> {
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	if let Some(public) = req.public {
		state
			.store
			.set_template_public(&template.id, public)
			.await?;
	}
	let aliases = state
		.store
		.aliases_of(std::slice::from_ref(&template.id))
		.await?;
	Ok(Json(TemplateUpdateResponse { names: names_of(&aliases).0 }))
}

#[derive(Debug, Serialize)]
struct FileUploadDto {
	present: bool,
	url:     String,
}

async fn file_upload_link(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path((template_ref, hash)): Path<(String, String)>,
) -> ApiResult<Response> {
	if !valid_files_hash(&hash) {
		return Err(ApiError::bad_request("Invalid files hash"));
	}
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	let present = builds::file_present(&state, &template.id, &hash).await?;
	let url = builds::upload_url(&state, &template.id, &hash);
	Ok((StatusCode::CREATED, Json(FileUploadDto { present, url })).into_response())
}

#[derive(Debug, Deserialize)]
struct UploadQuery {
	expires:   i64,
	signature: String,
}

/// Signed `PUT` target of [`file_upload_link`]: no E2B auth, the URL
/// signature authorizes the upload.
async fn upload_files(
	State(state): State<AppState>,
	Path((template_id, hash)): Path<(String, String)>,
	Query(query): Query<UploadQuery>,
	body: Body,
) -> ApiResult<StatusCode> {
	let path = builds::upload_path(&template_id, &hash);
	if !state
		.keys
		.verify_upload_url(&path, query.expires, &query.signature)
	{
		return Err(ApiError::forbidden("invalid or expired upload signature"));
	}
	if !valid_files_hash(&hash) {
		return Err(ApiError::bad_request("Invalid files hash"));
	}
	if state.store.template(&template_id).await?.is_none() {
		return Err(ApiError::not_found(format!("template '{template_id}' not found")));
	}
	builds::store_upload(&state, &template_id, &hash, body).await?;
	Ok(StatusCode::OK)
}

async fn start_build(
	State(state): State<AppState>,
	auth: TeamAuth,
	headers: HeaderMap,
	Path((template_id, build_id)): Path<(String, String)>,
	Json(req): Json<StartRequest>,
) -> ApiResult<StatusCode> {
	let user_agent = headers
		.get(header::USER_AGENT)
		.and_then(|v| v.to_str().ok());
	builds::start_build(&state, &auth.team, &template_id, &build_id, req, user_agent).await?;
	Ok(StatusCode::ACCEPTED)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusQuery {
	logs_offset: Option<i64>,
	limit:       Option<i64>,
	level:       Option<String>,
}

fn parse_level(level: Option<&str>) -> ApiResult<LogLevel> {
	match level {
		None | Some("") => Ok(LogLevel::Debug),
		Some(raw) => LogLevel::parse(raw)
			.ok_or_else(|| ApiError::bad_request(format!("invalid log level '{raw}'"))),
	}
}

fn parse_limit(limit: Option<i64>) -> ApiResult<u32> {
	match limit {
		None => Ok(MAX_LOG_PAGE),
		Some(n) if (0..=i64::from(MAX_LOG_PAGE)).contains(&n) => {
			Ok(u32::try_from(n).unwrap_or(MAX_LOG_PAGE))
		},
		Some(_) => Err(ApiError::bad_request(format!("limit must be between 0 and {MAX_LOG_PAGE}"))),
	}
}

/// Build of a team-owned template (404 for unknown or foreign builds).
async fn owned_build(
	state: &AppState,
	auth: &TeamAuth,
	template_ref: &str,
	build_id: &str,
) -> ApiResult<(String, BuildRow)> {
	let (template, _) = builds::owned_template(state, &auth.team, template_ref).await?;
	let build = state
		.store
		.build(build_id)
		.await?
		.filter(|build| build.template_id == template.id)
		.ok_or_else(|| ApiError::not_found(format!("build '{build_id}' not found")))?;
	Ok((template.id, build))
}

async fn build_status(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path((template_ref, build_id)): Path<(String, String)>,
	Query(query): Query<StatusQuery>,
) -> ApiResult<Json<BuildInfoDto>> {
	let offset = u64::try_from(query.logs_offset.unwrap_or(0))
		.map_err(|_| ApiError::bad_request("logsOffset must not be negative"))?;
	let limit = parse_limit(query.limit)?;
	let level = parse_level(query.level.as_deref())?;
	let (template_id, build) = owned_build(&state, &auth, &template_ref, &build_id).await?;
	let page = if build.status() == BuildStatus::Waiting || limit == 0 {
		Vec::new()
	} else {
		state
			.store
			.build_logs_page(&build.id, level, offset, limit)
			.await?
	};
	Ok(Json(build_info(&template_id, &build, &page)))
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
	cursor:    Option<i64>,
	limit:     Option<i64>,
	direction: Option<String>,
	level:     Option<String>,
	/// `temporary` | `persistent`; both read the same persisted log.
	source:    Option<String>,
}

#[derive(Debug, Serialize)]
struct BuildLogsDto {
	logs: Vec<LogEntryDto>,
}

async fn build_logs(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path((template_ref, build_id)): Path<(String, String)>,
	Query(query): Query<LogsQuery>,
) -> ApiResult<Json<BuildLogsDto>> {
	let limit = parse_limit(query.limit)?;
	let level = parse_level(query.level.as_deref())?;
	let direction = match query.direction.as_deref() {
		None | Some("" | "forward") => LogDirection::Forward,
		Some("backward") => LogDirection::Backward,
		Some(other) => return Err(ApiError::bad_request(format!("invalid direction '{other}'"))),
	};
	if let Some(source) = query.source.as_deref()
		&& !matches!(source, "" | "temporary" | "persistent")
	{
		return Err(ApiError::bad_request(format!("invalid source '{source}'")));
	}
	let cursor = match query.cursor {
		Some(ms) if ms < 0 => return Err(ApiError::bad_request("cursor must not be negative")),
		Some(ms) => DateTime::from_timestamp_millis(ms).unwrap_or_default(),
		None if direction == LogDirection::Backward => Utc::now() + chrono::Duration::days(1),
		None => DateTime::UNIX_EPOCH,
	};
	let (_, build) = owned_build(&state, &auth, &template_ref, &build_id).await?;
	let rows = if limit == 0 {
		Vec::new()
	} else {
		state
			.store
			.build_logs_by_time(&build.id, cursor, direction, level, limit)
			.await?
	};
	Ok(Json(BuildLogsDto { logs: rows.iter().map(LogEntryDto::from).collect() }))
}

#[derive(Debug, Deserialize)]
struct AssignTagsRequest {
	target: String,
	tags:   Vec<String>,
}

#[derive(Debug, Serialize)]
struct AssignedTagsDto {
	tags:     Vec<String>,
	#[serde(rename = "buildID")]
	build_id: String,
}

async fn assign_tags(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(req): Json<AssignTagsRequest>,
) -> ApiResult<Response> {
	if req.tags.is_empty() {
		return Err(ApiError::bad_request("tags must not be empty"));
	}
	let tags = build_tags(None, &req.tags).map_err(ApiError::bad_request)?;
	let (template, name) = builds::owned_template(&state, &auth.team, &req.target).await?;
	let source_tag = name.tag_or_default();
	let build_id = state
		.store
		.tag_build(&template.id, source_tag)
		.await?
		.ok_or_else(|| {
			ApiError::not_found(format!(
				"tag '{source_tag}' does not exist for template '{}'",
				template.id
			))
		})?;
	state
		.store
		.assign_tags(&template.id, &build_id, &tags)
		.await?;
	Ok((StatusCode::CREATED, Json(AssignedTagsDto { tags, build_id })).into_response())
}

#[derive(Debug, Deserialize)]
struct DeleteTagsRequest {
	name: String,
	tags: Vec<String>,
}

async fn delete_tags(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(req): Json<DeleteTagsRequest>,
) -> ApiResult<StatusCode> {
	if req.tags.is_empty() {
		return Err(ApiError::bad_request("tags must not be empty"));
	}
	let tags = build_tags(None, &req.tags).map_err(ApiError::bad_request)?;
	if tags.iter().any(|tag| tag == DEFAULT_TAG) {
		return Err(ApiError::bad_request("the 'default' tag cannot be deleted"));
	}
	let (template, _) = builds::owned_template(&state, &auth.team, &req.name).await?;
	if state.store.delete_tags(&template.id, &tags).await? == 0 {
		return Err(ApiError::not_found(format!(
			"none of the tags exist for template '{}'",
			template.id
		)));
	}
	Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
struct TagDto {
	tag:        String,
	#[serde(rename = "buildID")]
	build_id:   String,
	#[serde(rename = "createdAt")]
	created_at: String,
}

async fn list_tags(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(template_ref): Path<String>,
) -> ApiResult<Json<Vec<TagDto>>> {
	let (template, _) = builds::owned_template(&state, &auth.team, &template_ref).await?;
	let tags = state.store.template_tags(&template.id).await?;
	Ok(Json(
		tags
			.into_iter()
			.map(|tag| TagDto {
				tag:        tag.tag,
				build_id:   tag.build_id,
				created_at: rfc3339(tag.created_at),
			})
			.collect(),
	))
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AliasDto {
	#[serde(rename = "templateID")]
	template_id: String,
	public:      bool,
}

/// `GET /templates/aliases/{alias}` decision: missing → 404, another team's
/// template (even public) → 403, owner → 200; an explicit tag must exist.
pub(crate) fn alias_response(
	reference: &str,
	found: Option<&TemplateRow>,
	team_id: &str,
	tag_missing: bool,
) -> ApiResult<AliasDto> {
	let Some(template) = found else {
		return Err(ApiError::not_found(format!("template '{reference}' not found")));
	};
	if TemplateAccess::of(template, team_id) != TemplateAccess::Owner {
		return Err(ApiError::forbidden(format!("You don't have access to template '{reference}'")));
	}
	if tag_missing {
		return Err(ApiError::not_found(format!("tag does not exist for template '{reference}'")));
	}
	Ok(AliasDto { template_id: template.id.clone(), public: template.public })
}

async fn check_alias(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(alias): Path<String>,
) -> ApiResult<Json<AliasDto>> {
	let name =
		parse_name(&alias).map_err(|e| ApiError::bad_request(format!("Invalid alias: {e}")))?;
	let mut found = None;
	for lookup in builds::lookup_order(&name, &auth.team.slug) {
		if let builds::Lookup::Alias { namespace, alias } = lookup
			&& let Some(template) = state.store.template_by_alias(&namespace, &alias).await?
		{
			found = Some(template);
			break;
		}
	}
	let tag_missing = match (&found, &name.tag) {
		(Some(template), Some(tag)) => state.store.tag_build(&template.id, tag).await?.is_none(),
		_ => false,
	};
	alias_response(&alias, found.as_ref(), &auth.team.id, tag_missing).map(Json)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelResultDto {
	cancelled_count: u64,
	failed_count:    u64,
}

async fn admin_cancel_builds(
	State(state): State<AppState>,
	_admin: AdminAuth,
	Path(team_id): Path<String>,
) -> ApiResult<Json<CancelResultDto>> {
	if state.store.team_by_id(&team_id).await?.is_none() {
		return Err(ApiError::not_found(format!("team '{team_id}' not found")));
	}
	let mut result = CancelResultDto { cancelled_count: 0, failed_count: 0 };
	for build in state.store.team_unfinished_builds(&team_id).await? {
		if builds::cancel_build(&state, &build, "The build was cancelled by an administrator.").await
		{
			result.cancelled_count += 1;
		} else {
			result.failed_count += 1;
		}
	}
	Ok(Json(result))
}

#[cfg(test)]
mod tests;
