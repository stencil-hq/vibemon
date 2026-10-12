//! E2B wire types of the sandbox endpoints (`spec/e2b/infra/spec/openapi.yml`).

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use vmon_node::api::{LogEntry, MetricSample, VolumeMount};

use crate::{
	error::ApiError,
	lifecycle::{MountRequest, network::NetworkConfig},
};

/// RFC 3339 with milliseconds and `Z`.
pub fn rfc3339(at: DateTime<Utc>) -> String {
	at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Unix ms → RFC 3339.
pub fn rfc3339_ms(ms: u64) -> String {
	rfc3339(crate::lifecycle::datetime(ms))
}

/// `SandboxAutoResumeConfig`.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct AutoResume {
	pub enabled: bool,
}

/// `SandboxVolumeMount`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountDto {
	pub name: String,
	pub path: String,
}

impl From<MountDto> for MountRequest {
	fn from(m: MountDto) -> Self {
		Self { name: m.name, path: m.path }
	}
}

impl From<&VolumeMount> for MountDto {
	fn from(m: &VolumeMount) -> Self {
		Self { name: m.name.clone(), path: m.path.clone() }
	}
}

/// `SandboxIam` (workload identity).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Iam {
	#[serde(default)]
	pub tokens: Option<BTreeMap<String, Value>>,
}

/// `NewSandbox` / `NewSandboxV2` (same fields; defaults differ by version).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewSandboxBody {
	#[serde(rename = "templateID")]
	pub template_id:           String,
	pub timeout:               Option<i64>,
	pub auto_pause:            Option<bool>,
	pub auto_pause_memory:     Option<bool>,
	pub auto_resume:           Option<AutoResume>,
	/// Deprecated and ignored (always secured).
	pub secure:                Option<bool>,
	#[serde(rename = "allow_internet_access")]
	pub allow_internet_access: Option<bool>,
	pub network:               Option<NetworkConfig>,
	pub metadata:              Option<BTreeMap<String, String>>,
	pub env_vars:              Option<BTreeMap<String, String>>,
	/// Free-form MCP gateway config; consumed client-side, never logged.
	pub mcp:                   Option<Value>,
	pub iam:                   Option<Iam>,
	pub volume_mounts:         Option<Vec<MountDto>>,
	/// Vibemon extension: bring-your-own-bucket S3 mounts.
	pub vmon_s3_mounts:        Option<Vec<crate::lifecycle::s3mounts::S3MountRequest>>,
}

/// `ResumedSandbox`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeBody {
	pub timeout:    Option<i64>,
	/// Deprecated; ignored (the stored policy applies).
	pub auto_pause: Option<bool>,
	pub memory:     Option<bool>,
}

/// `ConnectSandbox` / `ConnectSandboxV2`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ConnectBody {
	pub timeout: Option<i64>,
	pub memory:  Option<bool>,
}

/// `SandboxTimeoutRequest`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct TimeoutBody {
	pub timeout: Option<i64>,
}

/// `SandboxRefreshRequest`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct RefreshBody {
	pub duration: Option<i64>,
}

/// `SandboxPauseRequest`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct PauseBody {
	pub memory: Option<bool>,
}

/// `SandboxSnapshotRequest`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct SnapshotBody {
	pub name:   Option<String>,
	pub memory: Option<bool>,
}

/// `SandboxForkRequest`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ForkBody {
	pub timeout: Option<i64>,
	pub count:   Option<i64>,
}

/// `Sandbox` (create/connect/resume/fork response).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDto {
	#[serde(rename = "templateID")]
	pub template_id:          String,
	#[serde(rename = "sandboxID")]
	pub sandbox_id:           String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub alias:                Option<String>,
	#[serde(rename = "clientID")]
	pub client_id:            String,
	pub envd_version:         String,
	pub envd_access_token:    String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub traffic_access_token: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub domain:               Option<String>,
}

/// `SandboxLifecycle`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleDto {
	pub auto_resume: bool,
	pub on_timeout:  &'static str,
}

/// `SandboxDetail`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDetailDto {
	#[serde(rename = "templateID")]
	pub template_id:           String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub alias:                 Option<String>,
	#[serde(rename = "sandboxID")]
	pub sandbox_id:            String,
	#[serde(rename = "clientID")]
	pub client_id:             String,
	pub started_at:            String,
	pub end_at:                String,
	pub envd_version:          String,
	pub envd_access_token:     String,
	pub allow_internet_access: Option<bool>,
	pub domain:                Option<String>,
	pub cpu_count:             u32,
	#[serde(rename = "memoryMB")]
	pub memory_mb:             u64,
	#[serde(rename = "diskSizeMB")]
	pub disk_size_mb:          u64,
	pub metadata:              BTreeMap<String, String>,
	pub state:                 &'static str,
	pub network:               NetworkConfig,
	pub lifecycle:             LifecycleDto,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub volume_mounts:         Option<Vec<MountDto>>,
}

/// `ListedSandbox`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedSandboxDto {
	#[serde(rename = "templateID")]
	pub template_id:   String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub alias:         Option<String>,
	#[serde(rename = "sandboxID")]
	pub sandbox_id:    String,
	#[serde(rename = "clientID")]
	pub client_id:     String,
	pub started_at:    String,
	pub end_at:        String,
	pub cpu_count:     u32,
	#[serde(rename = "memoryMB")]
	pub memory_mb:     u64,
	#[serde(rename = "diskSizeMB")]
	pub disk_size_mb:  u64,
	pub metadata:      BTreeMap<String, String>,
	pub state:         &'static str,
	pub envd_version:  String,
	pub volume_mounts: Vec<MountDto>,
}

/// `SandboxMetric`.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricDto {
	pub timestamp:      String,
	pub timestamp_unix: i64,
	pub cpu_count:      u32,
	pub cpu_used_pct:   f64,
	pub mem_used:       u64,
	pub mem_total:      u64,
	pub mem_cache:      u64,
	pub disk_used:      u64,
	pub disk_total:     u64,
}

impl From<&MetricSample> for MetricDto {
	fn from(s: &MetricSample) -> Self {
		Self {
			timestamp:      rfc3339_ms(s.timestamp_ms),
			timestamp_unix: i64::try_from(s.timestamp_ms / 1000).unwrap_or(i64::MAX),
			cpu_count:      s.cpu_count,
			cpu_used_pct:   s.cpu_used_pct,
			mem_used:       s.mem_used,
			mem_total:      s.mem_total,
			mem_cache:      s.mem_cache,
			disk_used:      s.disk_used,
			disk_total:     s.disk_total,
		}
	}
}

/// `SandboxLogEntry`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LogEntryDto {
	pub timestamp: String,
	pub message:   String,
	pub level:     String,
	pub fields:    BTreeMap<String, String>,
}

impl From<&LogEntry> for LogEntryDto {
	fn from(e: &LogEntry) -> Self {
		Self {
			timestamp: rfc3339_ms(e.timestamp_ms),
			message:   e.message.clone(),
			level:     e.level.clone(),
			fields:    e.fields.clone(),
		}
	}
}

/// `SandboxLog` (deprecated raw line).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LogLineDto {
	pub timestamp: String,
	pub line:      String,
}

impl From<&LogEntry> for LogLineDto {
	/// Upstream returns the raw collector line: a JSON object of the entry's
	/// fields plus `level` and `message` (`e2b sandbox logs` `JSON.parse`s it).
	fn from(e: &LogEntry) -> Self {
		let mut line: serde_json::Map<String, Value> = e
			.fields
			.iter()
			.map(|(k, v)| (k.clone(), Value::String(v.clone())))
			.collect();
		line.insert("level".into(), Value::String(e.level.clone()));
		line.insert("message".into(), Value::String(e.message.clone()));
		Self { timestamp: rfc3339_ms(e.timestamp_ms), line: Value::Object(line).to_string() }
	}
}

/// `SandboxLogs` (v1 response).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsV1Dto {
	pub logs:        Vec<LogLineDto>,
	pub log_entries: Vec<LogEntryDto>,
}

/// `SandboxLogsV2Response`.
#[derive(Clone, Debug, Serialize)]
pub struct LogsV2Dto {
	pub logs: Vec<LogEntryDto>,
}

/// `SandboxesWithMetrics`.
#[derive(Clone, Debug, Serialize)]
pub struct SandboxesWithMetricsDto {
	pub sandboxes: BTreeMap<String, MetricDto>,
}

/// `SandboxForkResult`.
#[derive(Clone, Debug, Serialize)]
pub struct ForkResultDto {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<SandboxDto>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub error:   Option<ErrorDto>,
}

/// `Error` embedded in a fork result.
#[derive(Clone, Debug, Serialize)]
pub struct ErrorDto {
	pub code:       u16,
	pub message:    String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub error_code: Option<&'static str>,
}

impl From<&ApiError> for ErrorDto {
	fn from(e: &ApiError) -> Self {
		Self {
			code:       e.status.as_u16(),
			message:    e.message.clone(),
			error_code: e.error_code,
		}
	}
}

/// `AdminSandboxKillResult`.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminKillDto {
	pub killed_count: u64,
	pub failed_count: u64,
}

/// `AdminSandbox`.
#[derive(Clone, Debug, Serialize)]
pub struct AdminSandboxDto {
	#[serde(rename = "sandboxID")]
	pub sandbox_id: String,
	#[serde(rename = "teamID")]
	pub team_id:    String,
	pub state:      &'static str,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn create_body_accepts_python_and_js_shapes() {
		let py: NewSandboxBody = serde_json::from_value(serde_json::json!({
			"templateID": "base", "metadata": {}, "envVars": {}, "allow_internet_access": false,
			"autoResume": {"enabled": true}, "mcp": {"github/a/b": {"runCmd": "x"}}, "volumeMounts": [{"name": "v", "path": "/v"}]
		}))
		.unwrap();
		assert_eq!(py.allow_internet_access, Some(false));
		assert!(py.auto_resume.unwrap().enabled);
		assert_eq!(py.volume_mounts.unwrap()[0].path, "/v");
		let js: NewSandboxBody =
			serde_json::from_value(serde_json::json!({"templateID": "base", "mcp": null})).unwrap();
		assert!(js.metadata.is_none() && js.timeout.is_none());
	}

	#[test]
	fn v1_log_line_is_raw_json_with_level_and_message() {
		let entry = LogEntry {
			timestamp_ms: 1_700_000_000_123,
			level:        "warn".into(),
			message:      "disk \"low\"".into(),
			fields:       BTreeMap::from([("logger".into(), "processSvc".into())]),
		};
		let dto = LogLineDto::from(&entry);
		let line: Value = serde_json::from_str(&dto.line).unwrap();
		assert_eq!(
			line,
			serde_json::json!({"level": "warn", "message": "disk \"low\"", "logger": "processSvc"})
		);
		assert_eq!(dto.timestamp, rfc3339_ms(1_700_000_000_123));
	}

	#[test]
	fn sandbox_dto_omits_absent_optionals() {
		let dto = SandboxDto {
			template_id:          "t".into(),
			sandbox_id:           "i1".into(),
			alias:                None,
			client_id:            "6532622b".into(),
			envd_version:         "0.6.4".into(),
			envd_access_token:    "tok".into(),
			traffic_access_token: None,
			domain:               None,
		};
		let json = serde_json::to_value(&dto).unwrap();
		assert_eq!(json["sandboxID"], "i1");
		assert_eq!(json["clientID"], "6532622b");
		assert!(json.get("trafficAccessToken").is_none() && json.get("alias").is_none());
	}
}
