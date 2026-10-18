//! `/secrets` CRUD: versioned team secrets (values encrypted, never
//! returned).

use std::collections::BTreeMap;

use axum::{
	Json, Router,
	extract::{Path, Query, State},
	http::StatusCode,
	response::{IntoResponse, Response},
	routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::pagination::{PageQuery, paged_json, split_page};
use crate::{
	auth::TeamAuth,
	error::{ApiError, ApiResult},
	state::AppState,
	store::{self, SecretRow, secrets::SECRET_ID_PREFIX},
};

/// Longest secret name.
const MAX_NAME_LEN: usize = 128;
/// Metadata limits (E2B `SecretMetadata`).
const MAX_METADATA_ENTRIES: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 1024;
const MAX_METADATA_TOTAL_BYTES: usize = 8192;
/// Secrets a team may hold.
pub const MAX_SECRETS_PER_TEAM: i64 = 1000;
/// Largest accepted secret value.
const MAX_VALUE_BYTES: usize = 64 * 1024;

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/secrets", get(list_secrets).post(create_secret))
		.route("/secrets/{secret_id}", get(get_secret).post(update_secret).delete(delete_secret))
}

/// E2B `Secret`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SecretDto {
	#[serde(rename = "secretID")]
	secret_id:       String,
	name:            String,
	current_version: i64,
	metadata:        BTreeMap<String, String>,
	created_at:      DateTime<Utc>,
	updated_at:      DateTime<Utc>,
}

impl From<SecretRow> for SecretDto {
	fn from(row: SecretRow) -> Self {
		Self {
			secret_id:       row.id,
			name:            row.name,
			current_version: row.current_version,
			metadata:        row.metadata,
			created_at:      row.created_at,
			updated_at:      row.updated_at,
		}
	}
}

#[derive(Deserialize)]
struct NewSecret {
	name:     String,
	value:    String,
	metadata: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize)]
struct SecretUpdate {
	value:    String,
	metadata: Option<BTreeMap<String, String>>,
}

/// Validates and canonicalizes (lower-cases) a secret name.
pub fn canonical_name(name: &str) -> Result<String, ApiError> {
	if name.is_empty() || name.len() > MAX_NAME_LEN {
		return Err(ApiError::bad_request(format!(
			"Secret name must be 1-{MAX_NAME_LEN} characters"
		)));
	}
	if !name
		.bytes()
		.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
	{
		return Err(ApiError::bad_request(
			"Secret name may only contain letters, digits, '_' and '-'",
		));
	}
	let lower = name.to_ascii_lowercase();
	if lower.starts_with(SECRET_ID_PREFIX) {
		return Err(ApiError::bad_request(format!(
			"Secret names must not start with '{SECRET_ID_PREFIX}'"
		)));
	}
	Ok(lower)
}

/// Enforces the `SecretMetadata` size limits.
pub fn validate_metadata(metadata: &BTreeMap<String, String>) -> Result<(), ApiError> {
	if metadata.len() > MAX_METADATA_ENTRIES {
		return Err(ApiError::bad_request(format!(
			"Secret metadata may have at most {MAX_METADATA_ENTRIES} entries"
		)));
	}
	let mut total = 0;
	for (key, value) in metadata {
		if key.is_empty() || key.len() > MAX_METADATA_KEY_BYTES {
			return Err(ApiError::bad_request(format!(
				"Secret metadata keys must be 1-{MAX_METADATA_KEY_BYTES} bytes"
			)));
		}
		if value.len() > MAX_METADATA_VALUE_BYTES {
			return Err(ApiError::bad_request(format!(
				"Secret metadata values must be at most {MAX_METADATA_VALUE_BYTES} bytes"
			)));
		}
		total += key.len() + value.len();
	}
	if total > MAX_METADATA_TOTAL_BYTES {
		return Err(ApiError::bad_request(format!(
			"Secret metadata must be at most {MAX_METADATA_TOTAL_BYTES} bytes"
		)));
	}
	Ok(())
}

fn validate_value(value: &str) -> Result<(), ApiError> {
	if value.len() > MAX_VALUE_BYTES {
		return Err(ApiError::bad_request(format!(
			"Secret value must be at most {MAX_VALUE_BYTES} bytes"
		)));
	}
	Ok(())
}

fn not_found(secret_id: &str) -> ApiError {
	ApiError::not_found(format!("Secret '{secret_id}' not found"))
}

async fn list_secrets(
	State(state): State<AppState>,
	auth: TeamAuth,
	Query(query): Query<PageQuery>,
) -> ApiResult<Response> {
	let page = query.page()?;
	let rows = state
		.store
		.list_secrets(&auth.team.id, page.after.as_ref(), page.limit + 1)
		.await?;
	let (rows, next) = split_page(rows, page.limit, SecretRow::cursor);
	Ok(paged_json(rows.into_iter().map(SecretDto::from).collect(), next))
}

async fn create_secret(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(body): Json<NewSecret>,
) -> ApiResult<Response> {
	let name = canonical_name(&body.name)?;
	validate_value(&body.value)?;
	let metadata = body.metadata.unwrap_or_default();
	validate_metadata(&metadata)?;
	if state.store.count_secrets(&auth.team.id).await? >= MAX_SECRETS_PER_TEAM {
		return Err(
			ApiError::new(
				StatusCode::TOO_MANY_REQUESTS,
				format!("You have reached the maximum number of secrets ({MAX_SECRETS_PER_TEAM})"),
			)
			.with_code("secret_limit_reached"),
		);
	}
	let enc = state.keys.encrypt_str(&body.value);
	match state
		.store
		.create_secret(&auth.team.id, &name, &enc, &metadata)
		.await
	{
		Ok(row) => Ok((StatusCode::CREATED, Json(SecretDto::from(row))).into_response()),
		Err(store::Error::Duplicate(_)) => {
			Err(ApiError::conflict(format!("Secret with name '{name}' already exists")))
		},
		Err(e) => Err(e.into()),
	}
}

async fn get_secret(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(secret_id): Path<String>,
) -> ApiResult<Json<SecretDto>> {
	let row = state
		.store
		.secret(&auth.team.id, &secret_id)
		.await?
		.ok_or_else(|| not_found(&secret_id))?;
	Ok(Json(row.into()))
}

async fn update_secret(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(secret_id): Path<String>,
	Json(body): Json<SecretUpdate>,
) -> ApiResult<Json<SecretDto>> {
	validate_value(&body.value)?;
	if let Some(metadata) = &body.metadata {
		validate_metadata(metadata)?;
	}
	let current = state
		.store
		.secret(&auth.team.id, &secret_id)
		.await?
		.ok_or_else(|| not_found(&secret_id))?;
	let enc = state.keys.encrypt_str(&body.value);
	let row = state
		.store
		.update_secret(&auth.team.id, &current.id, &enc, body.metadata.as_ref())
		.await?
		.ok_or_else(|| not_found(&secret_id))?;
	Ok(Json(row.into()))
}

async fn delete_secret(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(secret_id): Path<String>,
) -> ApiResult<StatusCode> {
	let current = state
		.store
		.secret(&auth.team.id, &secret_id)
		.await?
		.ok_or_else(|| not_found(&secret_id))?;
	if !state
		.store
		.delete_secret(&auth.team.id, &current.id)
		.await?
	{
		return Err(not_found(&secret_id));
	}
	Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn names_are_lowercased_and_validated() {
		assert_eq!(canonical_name("My_Key-1").unwrap(), "my_key-1");
		assert!(canonical_name("").is_err());
		assert!(canonical_name("has space").is_err());
		assert!(canonical_name("SEC_abc").is_err());
		assert!(canonical_name(&"a".repeat(129)).is_err());
		assert!(canonical_name(&"a".repeat(128)).is_ok());
	}

	#[test]
	fn metadata_limits_are_enforced_at_the_boundaries() {
		let mut ok: BTreeMap<String, String> =
			(0..32).map(|i| (format!("k{i}"), String::new())).collect();
		assert!(validate_metadata(&ok).is_ok());
		ok.insert("k32".into(), String::new());
		assert!(validate_metadata(&ok).is_err());
		let big_value = BTreeMap::from([("k".to_owned(), "v".repeat(1025))]);
		assert!(validate_metadata(&big_value).is_err());
		let big_key = BTreeMap::from([("k".repeat(129), String::new())]);
		assert!(validate_metadata(&big_key).is_err());
		let total: BTreeMap<String, String> = (0..9)
			.map(|i| (format!("k{i}"), "v".repeat(1000)))
			.collect();
		assert!(validate_metadata(&total).is_err());
	}
}
