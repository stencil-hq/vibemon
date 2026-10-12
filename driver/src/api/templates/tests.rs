use axum::{body::Body, http::Request};
use chrono::TimeZone;
use sqlx::types::Json as SqlJson;
use tower::ServiceExt;
use vmon_node::api::BuildReason;

use super::*;

fn build(status: &str, reason: Option<BuildReason>) -> BuildRow {
	let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
	BuildRow {
		id:               "11111111-1111-4111-8111-111111111111".into(),
		template_id:      "tpl".into(),
		team_id:          "team".into(),
		status:           status.into(),
		kind:             "build".into(),
		vcpus:            2,
		memory_mb:        1024,
		disk_mb:          4096,
		min_free_disk_mb: None,
		envd_version:     None,
		arch:             None,
		node_id:          None,
		tags:             SqlJson(vec!["default".into()]),
		spec:             None,
		template_version: None,
		start_cmd:        None,
		ready_cmd:        None,
		reason:           reason.map(SqlJson),
		artifact:         None,
		created_at:       at,
		updated_at:       at,
		started_at:       None,
		finished_at:      None,
	}
}

fn log(seq: u64, level: &str, step: Option<&str>, message: &str) -> LogRow {
	LogRow {
		seq,
		ts: Utc.timestamp_millis_opt(1_767_323_045_123).unwrap(),
		level: level.into(),
		step: step.map(Into::into),
		message: message.into(),
	}
}

#[test]
fn waiting_build_reports_no_entries() {
	let info = build_info("tpl", &build("waiting", None), &[log(0, "info", None, "early")]);
	assert!(info.log_entries.is_empty(), "unexpected log entries: {:?}", info.log_entries);
	assert_eq!(info.status, "waiting");
	assert!(info.reason.is_none());
}

#[test]
fn terminal_page_past_the_end_is_empty_and_keeps_the_status() {
	let info = build_info("tpl", &build("ready", None), &[]);
	let json = serde_json::to_value(&info).unwrap();
	assert_eq!(json["status"], "ready");
	assert_eq!(json["logEntries"], serde_json::json!([]));
	assert_eq!(json["logs"], serde_json::json!([]));
	assert!(json.get("reason").is_none());
}

#[test]
fn error_reason_carries_failed_step_warnings_of_the_page() {
	let reason = BuildReason { message: "exit status 1".into(), step: Some("2".into()) };
	let page = [
		log(0, "info", Some("2"), "[builder 2/3] RUN false"),
		log(1, "error", Some("2"), "command failed"),
		log(2, "error", Some("1"), "other step"),
		log(3, "warn", Some("2"), "retrying"),
	];
	let info = build_info("tpl", &build("error", Some(reason)), &page);
	let json = serde_json::to_value(&info).unwrap();
	assert_eq!(json["logEntries"].as_array().unwrap().len(), 4);
	assert_eq!(json["reason"]["step"], "2");
	let reasons: Vec<_> = json["reason"]["logEntries"]
		.as_array()
		.unwrap()
		.iter()
		.map(|e| e["message"].as_str().unwrap())
		.collect();
	assert_eq!(reasons, ["command failed", "retrying"]);
	assert_eq!(json["logEntries"][0]["timestamp"], "2026-01-02T03:04:05.123Z");
	assert_eq!(json["logEntries"][0]["step"], "2");
}

fn template(team: &str, public: bool) -> TemplateRow {
	let at = Utc::now();
	TemplateRow {
		id: "abcdefghij0123456789".into(),
		team_id: team.into(),
		public,
		kind: "template".into(),
		source_sandbox_id: None,
		created_at: at,
		updated_at: at,
		spawn_count: 0,
		last_spawned_at: None,
		build_count: 1,
		deleted_at: None,
	}
}

#[test]
fn alias_check_is_200_owner_403_other_team_404_missing() {
	assert_eq!(alias_response("x", None, "a", false).unwrap_err().status, StatusCode::NOT_FOUND);
	assert_eq!(
		alias_response("x", Some(&template("b", true)), "a", false)
			.unwrap_err()
			.status,
		StatusCode::FORBIDDEN
	);
	assert_eq!(
		alias_response("x", Some(&template("b", false)), "a", false)
			.unwrap_err()
			.status,
		StatusCode::FORBIDDEN
	);
	let ok = alias_response("x", Some(&template("a", true)), "a", false).unwrap();
	assert_eq!(ok, AliasDto { template_id: "abcdefghij0123456789".into(), public: true });
	assert_eq!(
		alias_response("x:v9", Some(&template("a", true)), "a", true)
			.unwrap_err()
			.status,
		StatusCode::NOT_FOUND
	);
}

#[tokio::test]
async fn encoded_slash_and_colon_decode_into_one_path_segment() {
	let router: Router = Router::new().route(
		"/templates/{template_id}/files/{hash}",
		get(|Path((id, hash)): Path<(String, String)>| async move { format!("{id}|{hash}") }),
	);
	let response = router
		.oneshot(
			Request::get("/templates/acme%2Fmy-tpl%3Av1/files/abc%3Ad")
				.body(Body::empty())
				.unwrap(),
		)
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	let body = axum::body::to_bytes(response.into_body(), 1024)
		.await
		.unwrap();
	assert_eq!(&body[..], b"acme/my-tpl:v1|abc:d");
	let name = parse_name("acme/my-tpl:v1").unwrap();
	assert_eq!(
		(name.namespace.as_deref(), name.alias.as_str(), name.tag.as_deref()),
		(Some("acme"), "my-tpl", Some("v1"))
	);
}
