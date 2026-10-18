//! Lifecycle webhook delivery, run by the raft-leader driver.
//!
//! Polls the sandbox event log past each enabled webhook's cursor, POSTs the
//! signed payload (`e2b-signature` = unpadded base64(SHA-256(secret + body))),
//! retries up to 3 attempts, and records every attempt as a webhook delivery.

use std::time::{Duration, Instant};

use futures::future::join_all;
use http::{HeaderMap, HeaderValue};

use crate::{
	crypto,
	state::AppState,
	store::{self, NewDelivery, SandboxEventRow, WebhookRow},
};

/// Poll interval of the delivery loop.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Attempts per event (E2B: up to 3 in total).
pub const MAX_ATTEMPTS: u32 = 3;
/// Per-attempt request timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Delay before attempt `n` (n ≥ 2) is `RETRY_BASE * 2^(n-2)`.
const RETRY_BASE: Duration = Duration::from_millis(500);
/// Events delivered per webhook per poll.
const BATCH: u32 = 100;
/// Stored response body cap.
const MAX_RESPONSE_BODY: usize = 4096;

/// Starts the delivery loop; it only works while this driver is the raft
/// leader so each event is delivered by one driver.
pub fn spawn_delivery(state: AppState) {
	tokio::spawn(async move {
		let mut tick = tokio::time::interval(POLL_INTERVAL);
		tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			tick.tick().await;
			if !state.cluster.is_leader() {
				continue;
			}
			if let Err(error) = deliver_pending(&state).await {
				tracing::warn!(%error, "webhook delivery pass failed");
			}
		}
	});
}

/// One delivery pass over every enabled webhook.
///
/// # Errors
///
/// Store failures while listing the webhooks (per-webhook failures are
/// logged).
pub async fn deliver_pending(state: &AppState) -> store::Result<()> {
	let hooks = state.store.enabled_webhooks().await?;
	let results = join_all(hooks.iter().map(|hook| deliver_hook(state, hook))).await;
	for (hook, result) in hooks.iter().zip(results) {
		if let Err(error) = result {
			tracing::warn!(%error, webhook = %hook.id, "webhook delivery failed");
		}
	}
	Ok(())
}

async fn deliver_hook(state: &AppState, hook: &WebhookRow) -> store::Result<()> {
	let events = state
		.store
		.events_after(&hook.team_id, hook.last_event_seq, &hook.events, BATCH)
		.await?;
	if events.is_empty() {
		return Ok(());
	}
	let secret = state.keys.decrypt_str(&hook.signature_secret_enc).ok();
	for event in &events {
		for attempt in 1..=MAX_ATTEMPTS {
			if attempt > 1 {
				tokio::time::sleep(RETRY_BASE * 2u32.pow(attempt - 2)).await;
			}
			let mut delivery = deliver_once(&state.http, hook, secret.as_deref(), event).await;
			delivery.first = attempt == 1;
			let ok = delivery.status == "success";
			state.store.insert_delivery(&delivery).await?;
			if ok || delivery.error_class == Some("signature_error") {
				break;
			}
		}
		state
			.store
			.advance_webhook_cursor(&hook.id, event.seq)
			.await?;
	}
	Ok(())
}

fn headers_json(headers: &HeaderMap, redact: &[&str]) -> String {
	let map: serde_json::Map<String, serde_json::Value> = headers
		.iter()
		.map(|(name, value)| {
			let shown = if redact.contains(&name.as_str()) {
				"[REDACTED]".to_owned()
			} else {
				value.to_str().unwrap_or("").to_owned()
			};
			(name.as_str().to_owned(), serde_json::Value::String(shown))
		})
		.collect();
	serde_json::Value::Object(map).to_string()
}

fn classify(error: &reqwest::Error) -> &'static str {
	if error.is_timeout() {
		"timeout"
	} else if error.is_builder() || error.is_request() && !error.is_connect() {
		"request_error"
	} else if error.is_connect() && format!("{error:?}").to_ascii_lowercase().contains("dns") {
		"dns_error"
	} else {
		"transport_error"
	}
}

/// Performs one signed POST of `event` to `hook.url` and describes the attempt.
///
/// The attempt is marked as the event's first (the delivery loop clears
/// [`NewDelivery::first`] on retries). `secret` is `None` when the stored
/// secret cannot be decrypted (recorded as `signature_error`, nothing is
/// sent).
pub async fn deliver_once(
	http: &reqwest::Client,
	hook: &WebhookRow,
	secret: Option<&str>,
	event: &SandboxEventRow,
) -> NewDelivery {
	let body = event.to_webhook_json().to_string();
	let delivery_id = uuid::Uuid::new_v4().to_string();
	let mut record = NewDelivery {
		team_id:          hook.team_id.clone(),
		webhook_id:       hook.id.clone(),
		event_id:         event.id.clone(),
		sandbox_id:       event.sandbox_id.clone(),
		event_type:       event.event_type.clone(),
		status:           "failed",
		duration_ms:      0,
		request_url:      hook.url.clone(),
		request_headers:  String::from("{}"),
		request_body:     body.clone(),
		response_status:  None,
		response_headers: None,
		response_body:    None,
		error_class:      None,
		error_message:    None,
		first:            true,
	};
	let Some(secret) = secret else {
		record.error_class = Some("signature_error");
		record.error_message = Some("webhook signature secret cannot be decrypted".into());
		return record;
	};
	let mut headers = HeaderMap::new();
	headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
	for (name, value) in [
		("e2b-webhook-id", hook.id.as_str()),
		("e2b-delivery-id", delivery_id.as_str()),
		("e2b-signature-version", "v1"),
	] {
		if let Ok(value) = HeaderValue::from_str(value) {
			headers.insert(name, value);
		}
	}
	let signature = crypto::webhook_signature(secret, body.as_bytes());
	if let Ok(value) = HeaderValue::from_str(&signature) {
		headers.insert("e2b-signature", value);
	}
	record.request_headers = headers_json(&headers, &["e2b-signature"]);
	let clock = Instant::now();
	let result = http
		.post(&hook.url)
		.headers(headers)
		.body(body)
		.timeout(REQUEST_TIMEOUT)
		.send()
		.await;
	match result {
		Ok(response) => {
			let status = response.status();
			record.response_status = Some(i32::from(status.as_u16()));
			record.response_headers = Some(headers_json(response.headers(), &["set-cookie"]));
			let text = response.text().await.unwrap_or_default();
			let mut cut = text.len().min(MAX_RESPONSE_BODY);
			while !text.is_char_boundary(cut) {
				cut -= 1;
			}
			record.response_body = Some(text[..cut].to_owned());
			if status.is_success() {
				record.status = "success";
			} else {
				record.error_class = Some("http_error");
			}
		},
		Err(error) => {
			record.error_class = Some(classify(&error));
			record.error_message = Some(error.to_string());
		},
	}
	record.duration_ms = i32::try_from(clock.elapsed().as_millis()).unwrap_or(i32::MAX);
	record
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use axum::{Router, body::Bytes, http::StatusCode, routing::post};
	use base64::Engine;
	use chrono::Utc;
	use parking_lot::Mutex;
	use sha2::{Digest, Sha256};

	use super::*;

	fn hook(url: String) -> WebhookRow {
		WebhookRow {
			id: "11111111-1111-1111-1111-111111111111".into(),
			team_id: "team".into(),
			name: "h".into(),
			url,
			events: vec![crate::store::event_types::KILLED.into()],
			enabled: true,
			signature_secret_enc: Vec::new(),
			last_event_seq: 0,
			created_at: Utc::now(),
			updated_at: Utc::now(),
		}
	}

	fn event() -> SandboxEventRow {
		SandboxEventRow {
			seq:          1,
			id:           "e1".into(),
			team_id:      "team".into(),
			sandbox_id:   "isbx".into(),
			execution_id: "x".into(),
			template_id:  "t".into(),
			build_id:     "b".into(),
			event_type:   crate::store::event_types::KILLED.into(),
			ts:           Utc::now(),
			data:         Some(serde_json::json!({"kill_reason": "request"})),
		}
	}

	/// Receiver that verifies signatures exactly as the E2B docs do.
	async fn receiver(status: StatusCode) -> (String, Arc<Mutex<Vec<(HeaderMap, Bytes)>>>) {
		let seen = Arc::new(Mutex::new(Vec::new()));
		let sink = seen.clone();
		let app = Router::new().route(
			"/hook",
			post(move |headers: HeaderMap, body: Bytes| {
				let sink = sink.clone();
				async move {
					sink.lock().push((headers, body));
					status
				}
			}),
		);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		(format!("http://{addr}/hook"), seen)
	}

	fn documented_verify(secret: &str, payload: &[u8], signature: &str) -> bool {
		let mut data = secret.as_bytes().to_vec();
		data.extend_from_slice(payload);
		let expected = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(&data));
		expected.trim_end_matches('=') == signature
	}

	#[tokio::test]
	async fn delivery_is_signed_verifiable_and_redacted() {
		let (url, seen) = receiver(StatusCode::OK).await;
		let record =
			deliver_once(&reqwest::Client::new(), &hook(url), Some("s3cret"), &event()).await;
		assert_eq!(record.status, "success");
		assert_eq!(record.response_status, Some(200));
		let (headers, body) = seen.lock().pop().unwrap();
		let signature = headers.get("e2b-signature").unwrap().to_str().unwrap();
		assert!(documented_verify("s3cret", &body, signature));
		assert!(!documented_verify("wrong", &body, signature));
		assert_eq!(headers.get("e2b-signature-version").unwrap(), "v1");
		assert_eq!(headers.get("e2b-webhook-id").unwrap(), "11111111-1111-1111-1111-111111111111");
		let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
		assert_eq!(payload["type"], "sandbox.lifecycle.killed");
		assert_eq!(payload["sandbox_id"], "isbx");
		assert!(
			!record.request_headers.contains(signature),
			"signature must be redacted in the record"
		);
	}

	#[tokio::test]
	async fn failures_are_classified() {
		let (url, _) = receiver(StatusCode::INTERNAL_SERVER_ERROR).await;
		let http = reqwest::Client::new();
		let record = deliver_once(&http, &hook(url), Some("s"), &event()).await;
		assert_eq!((record.status, record.error_class), ("failed", Some("http_error")));
		let unreachable =
			deliver_once(&http, &hook("http://127.0.0.1:1/hook".into()), Some("s"), &event()).await;
		assert_eq!(
			(unreachable.status, unreachable.error_class),
			("failed", Some("transport_error"))
		);
		let no_secret =
			deliver_once(&http, &hook("http://127.0.0.1:1/hook".into()), None, &event()).await;
		assert_eq!(no_secret.error_class, Some("signature_error"));
	}
}
