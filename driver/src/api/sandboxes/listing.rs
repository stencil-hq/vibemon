//! `GET /v2/sandboxes` query parsing.
//!
//! Covers the SDKs' triple-encoded `metadata` filter, the `state` CSV, and
//! the upstream cursor format `urlsafe-base64("<RFC3339Nano>__<id>[__asc]")`.

use std::collections::BTreeMap;

use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};

use crate::{
	error::{ApiError, ApiResult},
	store::sandboxes::{ListAfter, ListOrder, SandboxState},
};

/// Go `url.QueryUnescape`: `+` → space, `%XX` → byte; malformed escapes fail.
pub fn query_unescape(raw: &str) -> Option<String> {
	let bytes = raw.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		match bytes[i] {
			b'+' => out.push(b' '),
			b'%' => {
				let hex = bytes.get(i + 1..i + 3)?;
				let hex = std::str::from_utf8(hex).ok()?;
				out.push(u8::from_str_radix(hex, 16).ok()?);
				i += 2;
			},
			b => out.push(b),
		}
		i += 1;
	}
	String::from_utf8(out).ok()
}

/// Decodes the `metadata` query value (already decoded once by the HTTP
/// framework): unescape the whole string, split on `&` and `=`, unescape
/// every key and value. Empty input = no filter.
pub fn parse_metadata_filter(raw: Option<&str>) -> ApiResult<BTreeMap<String, String>> {
	let invalid = || ApiError::bad_request("Invalid metadata filter");
	let Some(raw) = raw.filter(|r| !r.is_empty()) else {
		return Ok(BTreeMap::new());
	};
	let whole = query_unescape(raw).ok_or_else(invalid)?;
	let mut out = BTreeMap::new();
	for pair in whole.split('&').filter(|p| !p.is_empty()) {
		let (key, value) = pair.split_once('=').ok_or_else(invalid)?;
		let key = query_unescape(key).ok_or_else(invalid)?;
		if key.is_empty() {
			return Err(invalid());
		}
		out.insert(key, query_unescape(value).ok_or_else(invalid)?);
	}
	Ok(out)
}

/// `state=running,paused` (default: both).
pub fn parse_states(raw: Option<&str>) -> ApiResult<Vec<SandboxState>> {
	let Some(raw) = raw.filter(|r| !r.trim().is_empty()) else {
		return Ok(vec![SandboxState::Running, SandboxState::Paused]);
	};
	let mut states = Vec::new();
	for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
		let state = match part {
			"running" => SandboxState::Running,
			"paused" => SandboxState::Paused,
			other => return Err(ApiError::bad_request(format!("Invalid state: {other}"))),
		};
		if !states.contains(&state) {
			states.push(state);
		}
	}
	Ok(states)
}

/// `order=asc|desc` (default `desc`).
pub fn parse_order(raw: Option<&str>) -> ApiResult<ListOrder> {
	match raw {
		None | Some("" | "desc") => Ok(ListOrder::Desc),
		Some("asc") => Ok(ListOrder::Asc),
		Some(other) => Err(ApiError::bad_request(format!("Invalid order: {other}"))),
	}
}

/// `limit` (default 100, 1..=100).
pub fn parse_limit(raw: Option<i64>) -> ApiResult<u32> {
	match raw {
		None => Ok(100),
		Some(n @ 1..=100) => Ok(u32::try_from(n).unwrap_or(100)),
		Some(_) => Err(ApiError::bad_request("limit must be between 1 and 100")),
	}
}

/// `limit` of `GET /admin/sandboxes` (default 500, 1..=1000).
pub fn parse_admin_limit(raw: Option<i64>) -> ApiResult<u32> {
	match raw {
		None => Ok(500),
		Some(n @ 1..=1000) => Ok(u32::try_from(n).unwrap_or(500)),
		Some(_) => Err(ApiError::bad_request("limit must be between 1 and 1000")),
	}
}

/// `startedAfter` (RFC 3339).
pub fn parse_started_after(raw: Option<&str>) -> ApiResult<Option<DateTime<Utc>>> {
	raw.filter(|r| !r.is_empty())
		.map(|r| {
			DateTime::parse_from_rfc3339(r)
				.map(|t| t.with_timezone(&Utc))
				.map_err(|_| ApiError::bad_request("Invalid startedAfter: expected RFC 3339"))
		})
		.transpose()
}

const ASC_SUFFIX: &str = "__asc";

/// Renders the `X-Next-Token` for the last row of a page.
pub fn encode_token(after: &ListAfter, order: ListOrder) -> String {
	let mut raw =
		format!("{}__{}", after.started_at.to_rfc3339_opts(SecondsFormat::Nanos, true), after.id);
	if order == ListOrder::Asc {
		raw.push_str(ASC_SUFFIX);
	}
	base64::engine::general_purpose::URL_SAFE.encode(raw)
}

/// Parses a `nextToken`; tokens minted for the other order are rejected.
pub fn decode_token(token: &str, order: ListOrder) -> ApiResult<ListAfter> {
	let invalid = || ApiError::bad_request("Invalid next token");
	let raw = base64::engine::general_purpose::URL_SAFE
		.decode(token)
		.map_err(|_| invalid())?;
	let raw = String::from_utf8(raw).map_err(|_| invalid())?;
	let body = match (order, raw.strip_suffix(ASC_SUFFIX)) {
		(ListOrder::Asc, Some(body)) => body,
		(ListOrder::Desc, None) => raw.as_str(),
		_ => return Err(invalid()),
	};
	let (time, id) = body.split_once("__").ok_or_else(invalid)?;
	let started_at = DateTime::parse_from_rfc3339(time)
		.map_err(|_| invalid())?
		.with_timezone(&Utc);
	if id.is_empty() {
		return Err(invalid());
	}
	Ok(ListAfter { started_at, id: id.to_owned() })
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Percent-encodes like JS `encodeURIComponent` / Python `quote(safe='')`.
	fn encode_component(s: &str) -> String {
		percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
	}

	/// Mirrors the SDKs: encode each key/value, join with `URLSearchParams`
	/// (which escapes `%` again); the HTTP client's encoding is undone by the
	/// framework before our parser runs, so it is not applied here.
	fn sdk_encode(pairs: &[(&str, &str)]) -> String {
		pairs
			.iter()
			.map(|(k, v)| {
				format!(
					"{}={}",
					encode_component(&encode_component(k)),
					encode_component(&encode_component(v))
				)
			})
			.collect::<Vec<_>>()
			.join("&")
	}

	#[test]
	fn metadata_filter_decodes_sdk_encoding() {
		let pairs = [("user id", "a&b=c"), ("emoji", "ü+%"), ("plain", "v")];
		let parsed = parse_metadata_filter(Some(&sdk_encode(&pairs))).unwrap();
		let expected: BTreeMap<String, String> = pairs
			.iter()
			.map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
			.collect();
		assert_eq!(parsed, expected);
	}

	#[test]
	fn metadata_filter_accepts_single_encoding_and_plus() {
		let parsed = parse_metadata_filter(Some("color=red&size=x+l")).unwrap();
		assert_eq!(parsed["color"], "red");
		assert_eq!(parsed["size"], "x l");
		assert!(parse_metadata_filter(None).unwrap().is_empty());
		assert!(parse_metadata_filter(Some("")).unwrap().is_empty());
	}

	#[test]
	fn metadata_filter_rejects_malformed() {
		for bad in ["novalue", "=v", "k=%zz", "k=%2", "%"] {
			assert_eq!(parse_metadata_filter(Some(bad)).unwrap_err().status, 400, "{bad}");
		}
	}

	#[test]
	fn states_default_dedupe_and_reject_unknown() {
		assert_eq!(parse_states(None).unwrap(), vec![SandboxState::Running, SandboxState::Paused]);
		assert_eq!(parse_states(Some("paused, paused")).unwrap(), vec![SandboxState::Paused]);
		assert!(parse_states(Some("running,killed")).is_err());
	}

	#[test]
	fn limit_bounds() {
		assert_eq!(parse_limit(None).unwrap(), 100);
		assert_eq!(parse_limit(Some(1)).unwrap(), 1);
		assert!(parse_limit(Some(0)).is_err());
		assert!(parse_limit(Some(101)).is_err());
		assert_eq!(parse_admin_limit(None).unwrap(), 500);
		assert_eq!(parse_admin_limit(Some(1000)).unwrap(), 1000);
		assert!(parse_admin_limit(Some(0)).is_err());
		assert!(parse_admin_limit(Some(1001)).is_err());
	}

	#[test]
	fn tokens_round_trip_and_are_order_bound() {
		let after = ListAfter {
			started_at: DateTime::parse_from_rfc3339("2026-10-11T12:00:00.123Z")
				.unwrap()
				.with_timezone(&Utc),
			id:         "iabc".into(),
		};
		let desc = encode_token(&after, ListOrder::Desc);
		assert_eq!(decode_token(&desc, ListOrder::Desc).unwrap(), after);
		assert!(decode_token(&desc, ListOrder::Asc).is_err());
		let asc = encode_token(&after, ListOrder::Asc);
		assert_eq!(decode_token(&asc, ListOrder::Asc).unwrap(), after);
		assert!(decode_token(&asc, ListOrder::Desc).is_err());
		// Upstream format: padded urlsafe base64 of `<RFC3339Nano>__<id>`.
		let raw = base64::engine::general_purpose::URL_SAFE
			.decode(&desc)
			.unwrap();
		assert_eq!(String::from_utf8(raw).unwrap(), "2026-10-11T12:00:00.123000000Z__iabc");
		for bad in ["", "!!!", &base64::engine::general_purpose::URL_SAFE.encode("nodelim")] {
			assert!(decode_token(bad, ListOrder::Desc).is_err(), "{bad}");
		}
	}
}
