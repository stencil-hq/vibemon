//! Sandbox logs (ARCHITECTURE.md §8.7).
//!
//! The VMM console (`console.log`) holds guest kernel output plus structured
//! `vmon-log:{json}` lines from the agent and envd. This module parses them
//! into [`LogEntry`] and answers `GET /node/v1/sandboxes/{id}/logs` queries
//! (cursor, direction, level, search, limit).

use std::{collections::BTreeMap, path::Path};

use serde_json::Value;

use crate::api::{LogEntry, LogQuery};

/// Structured line prefix written by the guest (`vmon_agent::logging`).
const LOG_PREFIX: &str = "vmon-log:";
/// Default and maximum page size.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

fn level_rank(level: &str) -> u8 {
	match level {
		"debug" | "trace" => 0,
		"warn" | "warning" => 2,
		"error" | "fatal" | "panic" => 3,
		_ => 1,
	}
}

fn normalize_level(level: &str) -> &'static str {
	match level_rank(&level.to_ascii_lowercase()) {
		0 => "debug",
		2 => "warn",
		3 => "error",
		_ => "info",
	}
}

/// Parses one console line. Structured lines carry their own timestamp;
/// plain lines (kernel, init) get `fallback_ms` and level `info`. Blank
/// lines yield `None`.
pub fn parse_line(line: &str, fallback_ms: u64) -> Option<LogEntry> {
	let line = line.trim_end_matches(['\r', '\n']);
	if line.trim().is_empty() {
		return None;
	}
	// The prefix may follow console noise on the same line.
	if let Some(start) = line.find(LOG_PREFIX)
		&& let Ok(Value::Object(mut map)) =
			serde_json::from_str::<Value>(&line[start + LOG_PREFIX.len()..])
	{
		let timestamp_ms = map
			.remove("ts")
			.and_then(|ts| ts.as_u64())
			.unwrap_or(fallback_ms);
		let level = map
			.remove("level")
			.and_then(|l| l.as_str().map(normalize_level))
			.unwrap_or("info");
		let message = match map.remove("msg") {
			Some(Value::String(message)) => message,
			Some(other) => other.to_string(),
			None => String::new(),
		};
		let fields = map
			.into_iter()
			.map(|(key, value)| {
				let value = match value {
					Value::String(text) => text,
					other => other.to_string(),
				};
				(key, value)
			})
			.collect();
		return Some(LogEntry { timestamp_ms, level: level.to_owned(), message, fields });
	}
	Some(LogEntry {
		timestamp_ms: fallback_ms,
		level:        "info".into(),
		message:      line.to_owned(),
		fields:       BTreeMap::new(),
	})
}

/// Parses a whole console log. Plain lines inherit the timestamp of the
/// previous structured line (or `start_ms`), keeping the sequence ordered.
pub fn parse_console(text: &str, start_ms: u64) -> Vec<LogEntry> {
	let mut last = start_ms;
	let mut out = Vec::new();
	for line in text.lines() {
		if let Some(entry) = parse_line(line, last) {
			last = last.max(entry.timestamp_ms);
			out.push(entry);
		}
	}
	out
}

/// Applies a [`LogQuery`] to entries in log order. `forward` returns entries
/// with `timestamp ≥ cursor` oldest first; `backward` (default) entries with
/// `timestamp ≤ cursor` newest first.
pub fn query(entries: &[LogEntry], q: &LogQuery) -> Vec<LogEntry> {
	let min_rank = q
		.level
		.as_deref()
		.map_or(0, |level| level_rank(&level.to_ascii_lowercase()));
	let limit = q
		.limit
		.map_or(DEFAULT_LIMIT, |l| usize::try_from(l).unwrap_or(MAX_LIMIT))
		.clamp(1, MAX_LIMIT);
	let matches = |entry: &&LogEntry| {
		level_rank(&entry.level) >= min_rank
			&& q
				.search
				.as_deref()
				.is_none_or(|needle| entry.message.contains(needle))
	};
	let forward = q.direction.as_deref() == Some("forward");
	if forward {
		let cursor = q.cursor.unwrap_or(0);
		entries
			.iter()
			.filter(|e| e.timestamp_ms >= cursor)
			.filter(matches)
			.take(limit)
			.cloned()
			.collect()
	} else {
		let cursor = q.cursor.unwrap_or(u64::MAX);
		entries
			.iter()
			.rev()
			.filter(|e| e.timestamp_ms <= cursor)
			.filter(matches)
			.take(limit)
			.cloned()
			.collect()
	}
}

/// Reads and queries a sandbox console log; a missing file means no logs.
pub fn read(console: &Path, start_ms: u64, q: &LogQuery) -> std::io::Result<Vec<LogEntry>> {
	let bytes = match std::fs::read(console) {
		Ok(bytes) => bytes,
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(e) => return Err(e),
	};
	Ok(query(&parse_console(&String::from_utf8_lossy(&bytes), start_ms), q))
}

#[cfg(test)]
mod tests {
	use super::*;

	const CONSOLE: &str = "\
[    0.000000] Booting Linux on physical CPU 0x0
vmon-log:{\"ts\":1000,\"level\":\"info\",\"msg\":\"agent started\",\"pid\":1}
[    0.512000] random: crng init done

vmon-log:{\"ts\":2000,\"level\":\"WARN\",\"msg\":\"slow disk\",\"ms\":{\"p99\":12}}
noise vmon-log:{\"ts\":3000,\"level\":\"error\",\"msg\":\"envd crashed\"}
vmon-log:{not json
";

	fn q(
		direction: Option<&str>,
		cursor: Option<u64>,
		level: Option<&str>,
		search: Option<&str>,
		limit: Option<u32>,
	) -> LogQuery {
		LogQuery {
			cursor,
			limit,
			direction: direction.map(str::to_owned),
			level: level.map(str::to_owned),
			search: search.map(str::to_owned),
		}
	}

	#[test]
	fn parses_structured_and_plain_lines_with_inherited_timestamps() {
		let entries = parse_console(CONSOLE, 500);
		let summary: Vec<_> = entries
			.iter()
			.map(|e| (e.timestamp_ms, e.level.as_str()))
			.collect();
		assert_eq!(summary, [
			(500, "info"),
			(1000, "info"),
			(1000, "info"),
			(2000, "warn"),
			(3000, "error"),
			(3000, "info")
		]);
		assert_eq!(entries[1].fields.get("pid").map(String::as_str), Some("1"));
		assert_eq!(entries[3].fields.get("ms").map(String::as_str), Some(r#"{"p99":12}"#));
		assert_eq!(entries[4].message, "envd crashed");
		// Malformed structured line is kept verbatim as plain text.
		assert_eq!(entries[5].message, "vmon-log:{not json");
	}

	#[test]
	fn backward_is_newest_first_and_cursor_inclusive() {
		let entries = parse_console(CONSOLE, 500);
		let got: Vec<_> = query(&entries, &q(None, Some(2000), None, None, Some(2)))
			.into_iter()
			.map(|e| e.message)
			.collect();
		assert_eq!(got, ["slow disk", "[    0.512000] random: crng init done"]);
	}

	#[test]
	fn forward_level_and_search_filters_compose() {
		let entries = parse_console(CONSOLE, 500);
		let warn_up: Vec<_> =
			query(&entries, &q(Some("forward"), Some(1000), Some("warn"), None, None))
				.into_iter()
				.map(|e| e.message)
				.collect();
		assert_eq!(warn_up, ["slow disk", "envd crashed"]);
		let searched = query(&entries, &q(Some("forward"), None, None, Some("crng"), None));
		assert_eq!(searched.len(), 1);
		// Search is case-sensitive.
		assert_eq!(
			query(&entries, &q(Some("forward"), None, None, Some("CRNG"), None)),
			Vec::<LogEntry>::new()
		);
		// Cursor past the end yields nothing forward.
		assert_eq!(
			query(&entries, &q(Some("forward"), Some(9999), None, None, None)),
			Vec::<LogEntry>::new()
		);
	}

	#[test]
	fn limit_is_clamped() {
		let text = (0..2000)
			.map(|i| format!("line {i}"))
			.collect::<Vec<_>>()
			.join("\n");
		let entries = parse_console(&text, 1);
		assert_eq!(query(&entries, &q(None, None, None, None, Some(5000))).len(), MAX_LIMIT);
		assert_eq!(query(&entries, &q(None, None, None, None, Some(0))).len(), 1);
		assert_eq!(query(&entries, &q(None, None, None, None, None)).len(), DEFAULT_LIMIT);
	}
}
