//! In-process end-to-end tests: a real envd server on 127.0.0.1:0 driven
//! over HTTP/1.1 with the Connect JSON protocol, covering process lifecycle
//! (output, exit codes, stdin, signals, retention, deadlines, disconnects,
//! PTY), filesystem RPC error mapping, token enforcement, and `/files`.

use std::{net::SocketAddr, time::Duration};

use base64::Engine;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use serde_json::{Value, json};

use super::{
	auth::{FileOp, file_signature},
	connect::{FLAG_END_STREAM, envelope},
	server::{EnvdConfig, EnvdHandle, spawn},
};

/// Start a server whose default user is the test's own user, so the suite
/// also runs unprivileged (only root may switch users).
async fn start_server() -> EnvdHandle {
	let srv = spawn(EnvdConfig {
		port: 0,
		bind: "127.0.0.1".parse().unwrap(),
		env: vec![("PATH".into(), "/usr/bin:/bin".into()), ("TEMPLATE_VAR".into(), "t".into())],
		..EnvdConfig::default()
	})
	.unwrap();
	// SAFETY: geteuid is always safe.
	let me = super::users::NameCache::default().user(unsafe { libc::geteuid() });
	let r = init(srv.local_addr(), json!({"defaultUser": me}), &[]).await;
	assert_eq!(r.status, StatusCode::NO_CONTENT);
	srv
}

struct Resp {
	status:  StatusCode,
	headers: http::HeaderMap,
	body:    Bytes,
}

async fn send(addr: SocketAddr, req: Request<Full<Bytes>>) -> Resp {
	let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
	let (mut sender, conn) =
		hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
			.await
			.unwrap();
	tokio::spawn(conn);
	let resp = sender.send_request(req).await.unwrap();
	let (parts, body) = resp.into_parts();
	let body = tokio::time::timeout(Duration::from_secs(20), body.collect())
		.await
		.unwrap()
		.unwrap()
		.to_bytes();
	Resp { status: parts.status, headers: parts.headers, body }
}

fn rpc_req(path: &str, ct: &str, body: Bytes, headers: &[(&str, &str)]) -> Request<Full<Bytes>> {
	let mut b = Request::post(format!("http://envd{path}")).header("content-type", ct);
	for (k, v) in headers {
		b = b.header(*k, *v);
	}
	b.body(Full::new(body)).unwrap()
}

async fn unary(
	addr: SocketAddr,
	path: &str,
	msg: Value,
	headers: &[(&str, &str)],
) -> (StatusCode, Value) {
	let r =
		send(addr, rpc_req(path, "application/json", Bytes::from(msg.to_string()), headers)).await;
	(r.status, serde_json::from_slice(&r.body).unwrap_or(Value::Null))
}

/// Run a server stream to completion: (messages, end-stream JSON).
async fn stream(
	addr: SocketAddr,
	path: &str,
	msg: Value,
	headers: &[(&str, &str)],
) -> (Vec<Value>, Value) {
	let r = send(
		addr,
		rpc_req(path, "application/connect+json", envelope(0, msg.to_string().as_bytes()), headers),
	)
	.await;
	assert_eq!(r.status, StatusCode::OK);
	parse_stream(&r.body)
}

fn parse_stream(mut body: &[u8]) -> (Vec<Value>, Value) {
	let mut msgs = Vec::new();
	while body.len() >= 5 {
		let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
		let v: Value = serde_json::from_slice(&body[5..5 + len]).unwrap();
		if body[0] & FLAG_END_STREAM != 0 {
			assert_eq!(body.len(), 5 + len, "end-stream must be last");
			return (msgs, v);
		}
		msgs.push(v);
		body = &body[5 + len..];
	}
	panic!("stream ended without end-stream frame");
}

fn b64(s: &str) -> String {
	base64::engine::general_purpose::STANDARD.encode(s)
}

fn output(events: &[Value], key: &str) -> String {
	events
		.iter()
		.filter_map(|e| e["event"]["data"][key].as_str())
		.map(|d| {
			String::from_utf8(base64::engine::general_purpose::STANDARD.decode(d).unwrap()).unwrap()
		})
		.collect()
}

fn start_msg(cmd: &str) -> Value {
	json!({"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}, "stdin": false})
}

#[tokio::test(flavor = "multi_thread")]
async fn start_streams_output_and_exactly_one_end() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let (events, end) = stream(
		addr,
		"/process.Process/Start",
		json!({"process": {"cmd": "/bin/sh", "args": ["-c", "echo out; echo err >&2; echo $TEMPLATE_VAR$REQ $E2B_SANDBOX; exit 3"], "envs": {"REQ": "r"}}}),
		&[],
	)
	.await;
	assert_eq!(end, json!({}));
	assert!(events[0]["event"]["start"]["pid"].as_u64().unwrap() > 0);
	assert_eq!(output(&events, "stdout"), "out\ntr true\n");
	assert_eq!(output(&events, "stderr"), "err\n");
	let ends: Vec<_> = events
		.iter()
		.filter(|e| e["event"].get("end").is_some())
		.collect();
	assert_eq!(ends.len(), 1);
	assert_eq!(
		ends[0]["event"]["end"],
		json!({"exitCode": 3, "exited": true, "status": "exit status 3", "error": "exit status 3"})
	);
	assert!(events.last().unwrap()["event"].get("end").is_some(), "end is the last message");
}

#[tokio::test(flavor = "multi_thread")]
async fn start_errors_are_stream_errors() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let (events, end) =
		stream(addr, "/process.Process/Start", json!({"process": {"cmd": "/nonexistent/bin"}}), &[])
			.await;
	assert!(events.is_empty(), "unexpected events: {events:?}");
	assert_eq!(end["error"]["code"], "not_found");
	let (_, end) = stream(
		addr,
		"/process.Process/Start",
		json!({"process": {"cmd": "/bin/true", "cwd": "/no/such/dir"}}),
		&[],
	)
	.await;
	assert_eq!(end["error"]["code"], "invalid_argument");
	let (_, end) = stream(addr, "/process.Process/Start", start_msg("true"), &[(
		"authorization",
		"Basic bm9ib2R5LXh5ejo=",
	)])
	.await;
	assert_eq!(end["error"]["code"], "unauthenticated");
}

#[tokio::test(flavor = "multi_thread")]
async fn stdin_input_and_close() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let task = tokio::spawn(stream(
		addr,
		"/process.Process/Start",
		json!({"process": {"cmd": "/bin/cat"}, "tag": "cat-1"}),
		&[],
	));
	let pid = wait_for_tag(addr, "cat-1").await;
	let (st, _) = unary(
		addr,
		"/process.Process/SendInput",
		json!({"process": {"pid": pid}, "input": {"stdin": b64("hello\n")}}),
		&[],
	)
	.await;
	assert_eq!(st, StatusCode::OK);
	// PTY input to a pipe process is an internal error.
	let (st, body) = unary(
		addr,
		"/process.Process/SendInput",
		json!({"process": {"pid": pid}, "input": {"pty": b64("x")}}),
		&[],
	)
	.await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::INTERNAL_SERVER_ERROR, Some("internal")));
	let (st, _) =
		unary(addr, "/process.Process/CloseStdin", json!({"process": {"tag": "cat-1"}}), &[]).await;
	assert_eq!(st, StatusCode::OK);
	let (events, end) = task.await.unwrap();
	assert_eq!(end, json!({}));
	assert_eq!(output(&events, "stdout"), "hello\n");
	assert_eq!(events.last().unwrap()["event"]["end"]["exited"], true);
}

async fn wait_for_tag(addr: SocketAddr, tag: &str) -> u64 {
	for _ in 0..200 {
		let (_, list) = unary(addr, "/process.Process/List", json!({}), &[]).await;
		if let Some(p) = list["processes"]
			.as_array()
			.and_then(|ps| ps.iter().find(|p| p["tag"] == tag))
		{
			return p["pid"].as_u64().unwrap();
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("process with tag {tag} never appeared");
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_keeps_process_and_connect_reattaches_and_replays_end() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	// Start, then drop the client right after the first message.
	{
		let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
		let (mut sender, conn) =
			hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
				.await
				.unwrap();
		let conn = tokio::spawn(conn);
		let body = envelope(0, json!({"process": {"cmd": "/bin/sh", "args": ["-c", "sleep 0.3; echo late"]}, "tag": "bg"}).to_string().as_bytes());
		let mut resp = sender
			.send_request(rpc_req("/process.Process/Start", "application/connect+json", body, &[]))
			.await
			.unwrap();
		let first = resp.body_mut().frame().await.unwrap().unwrap();
		assert!(first.is_data());
		drop(resp);
		drop(sender);
		conn.abort();
	}
	let pid = wait_for_tag(addr, "bg").await;
	let (events, end) =
		stream(addr, "/process.Process/Connect", json!({"process": {"pid": pid}}), &[]).await;
	assert_eq!(end, json!({}));
	assert_eq!(events[0]["event"]["start"]["pid"].as_u64(), Some(pid));
	assert_eq!(output(&events, "stdout"), "late\n");
	assert_eq!(events.last().unwrap()["event"]["end"]["status"], "exit status 0");
	// Exited: gone from List, but Connect replays start + end.
	let (_, list) = unary(addr, "/process.Process/List", json!({}), &[]).await;
	assert!(
		list["processes"]
			.as_array()
			.is_none_or(|ps| ps.iter().all(|p| p["pid"].as_u64() != Some(pid)))
	);
	let (events, end) =
		stream(addr, "/process.Process/Connect", json!({"process": {"tag": "bg"}}), &[]).await;
	assert_eq!(end, json!({}));
	assert_eq!(events.len(), 2);
	assert_eq!(events[1]["event"]["end"]["exited"], true);
	// Signals to an exited process: not_found.
	let (st, body) = unary(
		addr,
		"/process.Process/SendSignal",
		json!({"process": {"pid": pid}, "signal": "SIGNAL_SIGKILL"}),
		&[],
	)
	.await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
	let (_, end) =
		stream(addr, "/process.Process/Connect", json!({"process": {"pid": 999_999}}), &[]).await;
	assert_eq!(end["error"]["code"], "not_found");
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_reports_signal_and_bad_signal_is_unimplemented() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let task = tokio::spawn(stream(
		addr,
		"/process.Process/Start",
		json!({"process": {"cmd": "/bin/sleep", "args": ["30"]}, "tag": "sl"}),
		&[],
	));
	let pid = wait_for_tag(addr, "sl").await;
	let (st, body) = unary(
		addr,
		"/process.Process/SendSignal",
		json!({"process": {"pid": pid}, "signal": "SIGNAL_UNSPECIFIED"}),
		&[],
	)
	.await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::NOT_IMPLEMENTED, Some("unimplemented")));
	let (st, _) = unary(
		addr,
		"/process.Process/SendSignal",
		json!({"process": {"pid": pid}, "signal": "SIGNAL_SIGKILL"}),
		&[],
	)
	.await;
	assert_eq!(st, StatusCode::OK);
	let (events, end) = task.await.unwrap();
	assert_eq!(end, json!({}));
	assert_eq!(
		events.last().unwrap()["event"]["end"],
		json!({"exitCode": -1, "status": "signal: killed", "error": "signal: killed"})
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn start_deadline_kills_process_and_ends_with_deadline_exceeded() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let started = std::time::Instant::now();
	let (events, end) = stream(addr, "/process.Process/Start", start_msg("sleep 30"), &[(
		"connect-timeout-ms",
		"300",
	)])
	.await;
	assert!(started.elapsed() < Duration::from_secs(5));
	assert_eq!(events.last().unwrap()["event"]["end"]["status"], "signal: killed");
	assert_eq!(end["error"]["code"], "deadline_exceeded");
	let (_, end) =
		stream(addr, "/process.Process/Start", start_msg("true"), &[("connect-timeout-ms", "abc")])
			.await;
	assert_eq!(end["error"]["code"], "invalid_argument");
}

#[tokio::test(flavor = "multi_thread")]
async fn keepalive_is_sent_on_idle_streams() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let (events, _) = stream(addr, "/process.Process/Start", start_msg("sleep 2.5"), &[(
		"keepalive-ping-interval",
		"1",
	)])
	.await;
	let pings = events
		.iter()
		.filter(|e| e["event"].get("keepalive").is_some())
		.count();
	assert!(pings >= 2, "{events:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn pty_merges_output_resizes_and_rejects_close_stdin() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let task = tokio::spawn(stream(
		addr,
		"/process.Process/Start",
		json!({"process": {"cmd": "/bin/sh", "args": ["-c", "read line; stty size; echo got:$line; tty >/dev/null && echo istty"]}, "pty": {"size": {"cols": 100, "rows": 30}}, "tag": "pty"}),
		&[],
	));
	let pid = wait_for_tag(addr, "pty").await;
	let (st, _) = unary(
		addr,
		"/process.Process/Update",
		json!({"process": {"pid": pid}, "pty": {"size": {"cols": 120, "rows": 40}}}),
		&[],
	)
	.await;
	assert_eq!(st, StatusCode::OK);
	let (st, body) =
		unary(addr, "/process.Process/CloseStdin", json!({"process": {"pid": pid}}), &[]).await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::INTERNAL_SERVER_ERROR, Some("unknown")));
	let (st, _) = unary(
		addr,
		"/process.Process/SendInput",
		json!({"process": {"pid": pid}, "input": {"pty": b64("abc\n")}}),
		&[],
	)
	.await;
	assert_eq!(st, StatusCode::OK);
	let (events, _) = task.await.unwrap();
	let out = output(&events, "pty");
	assert!(out.contains("40 120"), "{out:?}");
	assert!(out.contains("got:abc"), "{out:?}");
	assert!(out.contains("istty"), "{out:?}");
	let stdout = output(&events, "stdout");
	assert!(stdout.is_empty(), "unexpected stdout: {stdout:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn filesystem_error_codes_and_proto_codec() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let (st, body) =
		unary(addr, "/filesystem.Filesystem/Stat", json!({"path": "/definitely/missing"}), &[]).await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
	let (st, body) =
		unary(addr, "/filesystem.Filesystem/MakeDir", json!({"path": "/tmp"}), &[]).await;
	assert_eq!((st, body["code"].as_str()), (StatusCode::CONFLICT, Some("already_exists")));
	let (st, _) =
		unary(addr, "/filesystem.Filesystem/Remove", json!({"path": "/tmp/envd-e2e-missing"}), &[])
			.await;
	assert_eq!(st, StatusCode::OK);
	let (st, body) = unary(addr, "/filesystem.Filesystem/Stat", json!({"path": "/tmp"}), &[]).await;
	assert_eq!(st, StatusCode::OK);
	assert_eq!(body["entry"]["type"], "FILE_TYPE_DIRECTORY");
	// Binary proto codec on the same handler.
	let msg =
		prost::Message::encode_to_vec(&super::pb::filesystem::StatRequest { path: "/tmp".into() });
	let r = send(
		addr,
		rpc_req("/filesystem.Filesystem/Stat", "application/proto", Bytes::from(msg), &[]),
	)
	.await;
	assert_eq!(r.status, StatusCode::OK);
	assert_eq!(r.headers["content-type"], "application/proto");
	let decoded: super::pb::filesystem::StatResponse = prost::Message::decode(&r.body[..]).unwrap();
	assert_eq!(decoded.entry.unwrap().name, "tmp");
	// Unknown procedure / bad content type.
	let (st, _) = unary(addr, "/filesystem.Filesystem/Nope", json!({}), &[]).await;
	assert_eq!(st, StatusCode::NOT_FOUND);
	let r =
		send(addr, rpc_req("/filesystem.Filesystem/Stat", "text/plain", Bytes::new(), &[])).await;
	assert_eq!(r.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_dir_stream_starts_and_reports_events() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let dir = std::env::temp_dir().join(format!("envd-e2e-watch-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).unwrap();
	let d = dir.clone();
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_millis(300)).await;
		std::fs::write(d.join("new.txt"), "x").unwrap();
	});
	let (events, end) = stream(
		addr,
		"/filesystem.Filesystem/WatchDir",
		json!({"path": dir.to_str().unwrap(), "includeEntry": true}),
		&[("connect-timeout-ms", "1000")],
	)
	.await;
	assert_eq!(events[0], json!({"start": {}}));
	let create = events
		.iter()
		.find(|e| e["filesystem"]["type"] == "EVENT_TYPE_CREATE")
		.expect("create event");
	assert_eq!(create["filesystem"]["name"], "new.txt");
	assert_eq!(create["filesystem"]["entry"]["name"], "new.txt");
	assert_eq!(end["error"]["code"], "deadline_exceeded");
	let (_, end) = stream(
		addr,
		"/filesystem.Filesystem/WatchDir",
		json!({"path": dir.join("nope").to_str().unwrap()}),
		&[],
	)
	.await;
	assert_eq!(end["error"]["code"], "not_found");
	std::fs::remove_dir_all(&dir).unwrap();
}

async fn init(addr: SocketAddr, body: Value, headers: &[(&str, &str)]) -> Resp {
	let mut b = Request::post("http://envd/init").header("content-type", "application/json");
	for (k, v) in headers {
		b = b.header(*k, *v);
	}
	send(addr, b.body(Full::new(Bytes::from(body.to_string()))).unwrap()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn init_token_enforcement_env_and_signed_files() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let r = init(
		addr,
		json!({"accessToken": "tok", "envVars": {"FROM_INIT": "1"}, "sandboxId": "sbx1"}),
		&[],
	)
	.await;
	assert_eq!(r.status, StatusCode::NO_CONTENT);
	assert_eq!(r.headers["x-envd-version"], super::ENVD_VERSION);
	// RPC and /envs now need the token; /health does not.
	let (st, body) = unary(addr, "/process.Process/List", json!({}), &[]).await;
	assert_eq!((st, body["code"].as_u64()), (StatusCode::UNAUTHORIZED, Some(401)));
	let health = send(
		addr,
		Request::get("http://envd/health")
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	assert_eq!(health.status, StatusCode::NO_CONTENT);
	let envs = send(
		addr,
		Request::get("http://envd/envs")
			.header("x-access-token", "tok")
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	let envs: Value = serde_json::from_slice(&envs.body).unwrap();
	assert_eq!(
		(envs["FROM_INIT"].as_str(), envs["E2B_SANDBOX_ID"].as_str()),
		(Some("1"), Some("sbx1"))
	);
	// /init from a non-root/unknown peer without the old token cannot rotate it
	// (the test runs as root on loopback when sudo'd, which is the tunnel case).
	// SAFETY: geteuid is always safe.
	if unsafe { libc::geteuid() } != 0 {
		assert_eq!(
			init(addr, json!({"accessToken": "new"}), &[]).await.status,
			StatusCode::UNAUTHORIZED
		);
	}
	assert_eq!(
		init(addr, json!({"accessToken": "tok"}), &[("x-access-token", "tok")])
			.await
			.status,
		StatusCode::NO_CONTENT
	);

	// Signed upload + download without the header.
	let path = std::env::temp_dir().join(format!("envd-e2e-signed-{}.txt", std::process::id()));
	let p = path.to_str().unwrap();
	let sig = file_signature(p, FileOp::Write, "", "tok", None);
	let enc = |s: &str| {
		percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
	};
	let up = send(
		addr,
		Request::post(format!("http://envd/files?path={}&signature={}", enc(p), enc(&sig)))
			.header("content-type", "application/octet-stream")
			.body(Full::new(Bytes::from_static(b"signed-data")))
			.unwrap(),
	)
	.await;
	assert_eq!(up.status, StatusCode::OK, "{:?}", up.body);
	// Read signature does not authorize... a write signature for reading.
	let bad = send(
		addr,
		Request::get(format!("http://envd/files?path={}&signature={}", enc(p), enc(&sig)))
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	assert_eq!(bad.status, StatusCode::UNAUTHORIZED);
	let exp = (super::time::now_unix() + 60).to_string();
	let rsig = file_signature(p, FileOp::Read, "", "tok", Some(exp.parse().unwrap()));
	let down = send(
		addr,
		Request::get(format!(
			"http://envd/files?path={}&signature={}&signature_expiration={exp}",
			enc(p),
			enc(&rsig)
		))
		.body(Full::new(Bytes::new()))
		.unwrap(),
	)
	.await;
	assert_eq!((down.status, &down.body[..]), (StatusCode::OK, &b"signed-data"[..]));
	std::fs::remove_file(&path).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn files_multipart_gzip_and_errors() {
	let srv = start_server().await;
	let addr = srv.local_addr();
	let dir = std::env::temp_dir().join(format!("envd-e2e-files-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	let a = dir.join("x/a.txt");
	let b = dir.join("b.bin");
	let boundary = "XBOUNDARY";
	let body = format!(
		"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
		 filename=\"{}\"\r\n\r\nAAA\r\n--{boundary}\r\nContent-Disposition: form-data; \
		 name=\"other\"\r\n\r\nignored\r\n--{boundary}\r\nContent-Disposition: form-data; \
		 name=\"file\"; filename=\"{}\"\r\n\r\nBBBB\r\n--{boundary}--\r\n",
		a.display(),
		b.display()
	);
	let r = send(
		addr,
		Request::post("http://envd/files")
			.header("content-type", format!("multipart/form-data; boundary={boundary}"))
			.header("x-metadata-Purpose", "test")
			.body(Full::new(Bytes::from(body.clone())))
			.unwrap(),
	)
	.await;
	assert_eq!(r.status, StatusCode::OK, "{:?}", r.body);
	let written: Value = serde_json::from_slice(&r.body).unwrap();
	assert_eq!(written[0]["path"], a.to_str().unwrap());
	assert_eq!(written[0]["type"], "file");
	assert_eq!(written[1]["name"], "b.bin");
	assert_eq!(std::fs::read_to_string(&a).unwrap(), "AAA");
	assert_eq!(std::fs::read_to_string(&b).unwrap(), "BBBB");
	// Same body with a path query → both parts target one path → 400.
	let r = send(
		addr,
		Request::post(format!("http://envd/files?path={}", dir.join("same").display()))
			.header("content-type", format!("multipart/form-data; boundary={boundary}"))
			.body(Full::new(Bytes::from(body)))
			.unwrap(),
	)
	.await;
	assert_eq!(r.status, StatusCode::BAD_REQUEST);
	// gzip octet-stream upload, gzip download.
	let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
	std::io::Write::write_all(&mut enc, &b"zipped ".repeat(100)).unwrap();
	let gz = enc.finish().unwrap();
	let c = dir.join("c.txt");
	let r = send(
		addr,
		Request::post(format!("http://envd/files?path={}", c.display()))
			.header("content-type", "application/octet-stream")
			.header("content-encoding", "gzip")
			.body(Full::new(Bytes::from(gz)))
			.unwrap(),
	)
	.await;
	assert_eq!(r.status, StatusCode::OK);
	let r = send(
		addr,
		Request::get(format!("http://envd/files?path={}", c.display()))
			.header("accept-encoding", "gzip")
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	assert_eq!(r.headers["content-encoding"], "gzip");
	let mut plain = Vec::new();
	std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&r.body[..]), &mut plain).unwrap();
	assert_eq!(plain, b"zipped ".repeat(100));
	// Range request.
	let r = send(
		addr,
		Request::get(format!("http://envd/files?path={}", c.display()))
			.header("range", "bytes=0-5")
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	assert_eq!((r.status, &r.body[..]), (StatusCode::PARTIAL_CONTENT, &b"zipped"[..]));
	// Errors: missing → 404, directory → 400, octet without path → 400, unknown
	// user → 401.
	let get = |q: String| {
		Request::get(format!("http://envd/files?{q}"))
			.body(Full::new(Bytes::new()))
			.unwrap()
	};
	assert_eq!(
		send(addr, get(format!("path={}/nope", dir.display())))
			.await
			.status,
		StatusCode::NOT_FOUND
	);
	assert_eq!(
		send(addr, get(format!("path={}", dir.display())))
			.await
			.status,
		StatusCode::BAD_REQUEST
	);
	assert_eq!(
		send(addr, get(format!("path={}&username=no-such-user-xyz", c.display())))
			.await
			.status,
		StatusCode::UNAUTHORIZED
	);
	let r = send(
		addr,
		Request::post("http://envd/files")
			.header("content-type", "application/octet-stream")
			.body(Full::new(Bytes::new()))
			.unwrap(),
	)
	.await;
	assert_eq!(r.status, StatusCode::BAD_REQUEST);
	let err: Value = serde_json::from_slice(&r.body).unwrap();
	assert_eq!(err["code"], 400);
	std::fs::remove_dir_all(&dir).unwrap();
}
