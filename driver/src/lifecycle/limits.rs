//! Pure lifecycle rules: sandbox IDs, timeouts, and lifecycle flags.
//!
//! Timeout validation and clamping follow upstream's API (the `timeout`
//! fields of `spec/e2b/infra/spec/openapi.yml`) and ARCHITECTURE.md §9.4.

use rand::RngExt;

use crate::error::{ApiError, ApiResult};

/// Upstream constant returned as `clientID` everywhere.
pub const CLIENT_ID: &str = "6532622b";
/// Default timeout of v1 create / resume / fork (seconds).
pub const V1_DEFAULT_TIMEOUT_S: i64 = 15;
/// Default timeout of v2 create / connect (seconds).
pub const V2_DEFAULT_TIMEOUT_S: i64 = 300;
/// Minimum (and default) timeout applied by traffic-triggered auto-resume
/// (seconds).
pub const MIN_AUTO_RESUME_TIMEOUT_S: u64 = 300;
/// `POST /refreshes` default and floor (seconds).
pub const REFRESH_MIN_S: u64 = 15;
/// `POST /refreshes` maximum (seconds).
pub const REFRESH_MAX_S: u64 = 3600;
/// Fork `count` bounds.
pub const FORK_MAX: u32 = 100;

const ID_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz1234567890";
const ID_LEN: usize = 20;

/// New sandbox ID: `i` + 20 × `[a-z0-9]`.
pub fn new_sandbox_id() -> String {
	let mut rng = rand::rng();
	let mut id = String::with_capacity(ID_LEN + 1);
	id.push('i');
	for _ in 0..ID_LEN {
		id.push(char::from(ID_ALPHABET[rng.random_range(0..ID_ALPHABET.len())]));
	}
	id
}

/// Normalizes a path sandbox ID: the legacy `<id>-<clientID>` form keeps the
/// first part; anything else must match `^[a-z0-9]+$` (400 `Invalid sandbox
/// ID`).
pub fn normalize_sandbox_id(raw: &str) -> ApiResult<String> {
	let mut parts = raw.split('-');
	let id = parts.next().unwrap_or_default();
	let extra = parts.count();
	if extra > 1
		|| id.is_empty()
		|| !id
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
	{
		return Err(ApiError::bad_request("Invalid sandbox ID"));
	}
	Ok(id.to_owned())
}

/// Maximum sandbox lifetime per execution, in ms.
pub fn max_length_ms(max_length_hours: u32) -> u64 {
	u64::from(max_length_hours) * 3_600_000
}

/// Validates a create/resume/connect timeout (seconds) and returns it in ms.
pub fn validate_timeout(timeout_s: i64, max_length_hours: u32) -> ApiResult<u64> {
	if timeout_s <= 0 {
		return Err(ApiError::bad_request("Timeout must be greater than 0"));
	}
	let ms = u64::try_from(timeout_s)
		.unwrap_or(u64::MAX)
		.saturating_mul(1000);
	if ms > max_length_ms(max_length_hours) {
		return Err(ApiError::bad_request(format!(
			"Timeout cannot be greater than {max_length_hours} hours"
		)));
	}
	Ok(ms)
}

/// Caps a requested expiry at `started + max_len`.
pub fn clamp_end(started_ms: u64, requested_end_ms: u64, max_len_ms: u64) -> u64 {
	requested_end_ms.min(started_ms.saturating_add(max_len_ms))
}

/// `POST /sandboxes/{id}/timeout`: `now + max(timeout, 0)`, may shorten,
/// capped at the max length; 400 once the max length has elapsed.
pub fn timeout_end(
	now_ms: u64,
	started_ms: u64,
	timeout_s: i64,
	max_len_ms: u64,
) -> ApiResult<u64> {
	if now_ms.saturating_sub(started_ms) >= max_len_ms {
		return Err(ApiError::bad_request("Max instance length exceeded"));
	}
	let ttl_ms = u64::try_from(timeout_s.max(0))
		.unwrap_or_default()
		.saturating_mul(1000);
	Ok(clamp_end(started_ms, now_ms.saturating_add(ttl_ms), max_len_ms))
}

/// Connect/refresh semantics: extend to `now + ttl` (capped) but never shorten.
pub fn extend_end(
	current_end_ms: u64,
	now_ms: u64,
	started_ms: u64,
	ttl_ms: u64,
	max_len_ms: u64,
) -> u64 {
	current_end_ms.max(clamp_end(started_ms, now_ms.saturating_add(ttl_ms), max_len_ms))
}

/// `SandboxRefreshRequest.duration`: default and floor 15 s, maximum 3600 s.
pub fn refresh_duration_ms(duration_s: Option<i64>) -> ApiResult<u64> {
	let secs = duration_s.unwrap_or(0);
	if secs < 0 || u64::try_from(secs).is_ok_and(|s| s > REFRESH_MAX_S) {
		return Err(ApiError::bad_request(format!(
			"Duration must be between 0 and {REFRESH_MAX_S} seconds"
		)));
	}
	Ok(u64::try_from(secs).unwrap_or_default().max(REFRESH_MIN_S) * 1000)
}

/// Timeout (seconds) remembered for traffic-triggered resumes: the create
/// timeout raised to the 300 s minimum and capped at the team max length.
pub fn auto_resume_timeout_s(timeout_s: u64, max_length_hours: u32) -> u64 {
	timeout_s
		.max(MIN_AUTO_RESUME_TIMEOUT_S)
		.min(u64::from(max_length_hours) * 3600)
}

/// Effective lifecycle flags of a create request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleFlags {
	pub auto_pause:        bool,
	pub auto_pause_memory: bool,
	pub auto_resume:       bool,
}

/// Applies defaults (`autoPause=false`, `autoPauseMemory=true`,
/// `autoResume=false`) and rejects the combinations upstream rejects.
pub fn lifecycle_flags(
	auto_pause: Option<bool>,
	auto_pause_memory: Option<bool>,
	auto_resume: Option<bool>,
) -> ApiResult<LifecycleFlags> {
	let flags = LifecycleFlags {
		auto_pause:        auto_pause.unwrap_or(false),
		auto_pause_memory: auto_pause_memory.unwrap_or(true),
		auto_resume:       auto_resume.unwrap_or(false),
	};
	if !flags.auto_pause_memory && !flags.auto_pause {
		return Err(ApiError::bad_request(
			"autoPauseMemory can only be set when autoPause is enabled",
		));
	}
	if !flags.auto_pause_memory && flags.auto_resume {
		return Err(ApiError::bad_request(
			"autoPauseMemory=false cannot be combined with autoResume",
		));
	}
	Ok(flags)
}

/// Validates a fork `count` against the spec bounds and the team concurrency.
pub fn validate_fork_count(count: Option<i64>, max_concurrent: u32) -> ApiResult<u32> {
	let count = count.unwrap_or(1);
	let bounded = u32::try_from(count)
		.ok()
		.filter(|c| (1..=FORK_MAX).contains(c));
	let count = bounded
		.ok_or_else(|| ApiError::bad_request(format!("count must be between 1 and {FORK_MAX}")))?;
	if count >= max_concurrent {
		return Err(ApiError::bad_request(format!(
			"count must be lower than the team's concurrent sandbox limit ({max_concurrent})"
		)));
	}
	Ok(count)
}

#[cfg(test)]
mod tests {
	use super::*;

	const HOUR_MS: u64 = 3_600_000;

	#[test]
	fn sandbox_ids_have_the_upstream_shape() {
		for _ in 0..100 {
			let id = new_sandbox_id();
			assert_eq!(id.len(), 21);
			assert!(id.starts_with('i'));
			assert_eq!(normalize_sandbox_id(&id).unwrap(), id);
		}
	}

	#[test]
	fn path_ids_accept_legacy_client_suffix_only() {
		assert_eq!(normalize_sandbox_id("iabc123-6532622b").unwrap(), "iabc123");
		for bad in ["", "a-b-c", "ABC", "abc_1", "-x", "ab/c"] {
			assert_eq!(normalize_sandbox_id(bad).unwrap_err().status, 400, "{bad}");
		}
	}

	#[test]
	fn timeout_validation_bounds() {
		assert_eq!(validate_timeout(1, 1).unwrap(), 1000);
		assert_eq!(validate_timeout(3600, 1).unwrap(), HOUR_MS);
		let over = validate_timeout(3601, 1).unwrap_err();
		assert_eq!(over.message, "Timeout cannot be greater than 1 hours");
		assert_eq!(validate_timeout(0, 1).unwrap_err().message, "Timeout must be greater than 0");
		assert!(validate_timeout(-5, 24).is_err());
	}

	#[test]
	fn set_timeout_may_shorten_and_is_capped_at_max_length() {
		let start = 1_000_000;
		// 10 minutes in: shorten to 5 s from now.
		let now = start + 600_000;
		assert_eq!(timeout_end(now, start, 5, HOUR_MS).unwrap(), now + 5_000);
		// Negative means expire now.
		assert_eq!(timeout_end(now, start, -10, HOUR_MS).unwrap(), now);
		// Capped at start + max length.
		assert_eq!(timeout_end(now, start, 7200, HOUR_MS).unwrap(), start + HOUR_MS);
		// Max length already elapsed.
		assert_eq!(
			timeout_end(start + HOUR_MS, start, 5, HOUR_MS)
				.unwrap_err()
				.message,
			"Max instance length exceeded"
		);
	}

	#[test]
	fn extend_never_shortens() {
		let start = 0;
		assert_eq!(extend_end(500_000, 100_000, start, 60_000, HOUR_MS), 500_000);
		assert_eq!(extend_end(500_000, 480_000, start, 60_000, HOUR_MS), 540_000);
		assert_eq!(extend_end(500_000, HOUR_MS - 1000, start, 60_000, HOUR_MS), HOUR_MS);
	}

	#[test]
	fn refresh_duration_floor_default_and_max() {
		assert_eq!(refresh_duration_ms(None).unwrap(), 15_000);
		assert_eq!(refresh_duration_ms(Some(3)).unwrap(), 15_000);
		assert_eq!(refresh_duration_ms(Some(3600)).unwrap(), 3_600_000);
		assert!(refresh_duration_ms(Some(3601)).is_err());
		assert!(refresh_duration_ms(Some(-1)).is_err());
	}

	#[test]
	fn auto_resume_timeout_is_clamped_both_ways() {
		assert_eq!(auto_resume_timeout_s(15, 24), 300);
		assert_eq!(auto_resume_timeout_s(900, 24), 900);
		assert_eq!(auto_resume_timeout_s(100_000, 1), 3600);
	}

	#[test]
	fn lifecycle_flag_defaults_and_conflicts() {
		assert_eq!(lifecycle_flags(None, None, None).unwrap(), LifecycleFlags {
			auto_pause:        false,
			auto_pause_memory: true,
			auto_resume:       false,
		});
		assert!(lifecycle_flags(None, Some(false), None).is_err());
		assert!(lifecycle_flags(Some(true), Some(false), Some(true)).is_err());
		let fs_only = lifecycle_flags(Some(true), Some(false), Some(false)).unwrap();
		assert!(fs_only.auto_pause && !fs_only.auto_pause_memory);
		// autoResume without autoPause is allowed (manual pause can still
		// auto-resume).
		assert!(lifecycle_flags(None, None, Some(true)).unwrap().auto_resume);
	}

	#[test]
	fn fork_count_bounds() {
		assert_eq!(validate_fork_count(None, 20).unwrap(), 1);
		assert_eq!(validate_fork_count(Some(19), 20).unwrap(), 19);
		assert!(validate_fork_count(Some(20), 20).is_err());
		assert!(validate_fork_count(Some(0), 200).is_err());
		assert!(validate_fork_count(Some(101), 200).is_err());
		assert_eq!(validate_fork_count(Some(100), 200).unwrap(), 100);
	}
}
