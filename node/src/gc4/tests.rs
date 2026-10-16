//! GC4 client tests against an in-memory fake agent.

use std::{
	collections::{HashMap, VecDeque},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, WriteHalf};
use vmon_agent::proto;

use super::*;

/// Window the fake agent advertises; small so the host must wait for credit.
const AGENT_WINDOW: u32 = 4096;

struct FakeStream {
	/// Bytes received from the host and not yet credited back.
	inflight:    u64,
	/// Credit the host granted us.
	host_credit: u64,
	/// Echo bytes waiting for host credit.
	pending:     VecDeque<u8>,
	host_closed: bool,
	closed_sent: bool,
}

#[derive(Default)]
struct FakeReport {
	window_violation:  AtomicBool,
	host_close_reason: parking_lot::Mutex<Option<String>>,
}

async fn send(writer: &mut WriteHalf<DuplexStream>, ty: u8, id: u32, payload: &[u8]) {
	writer
		.write_all(&encode(ty, id, payload))
		.await
		.expect("fake write");
}

async fn flush_echo(writer: &mut WriteHalf<DuplexStream>, id: u32, stream: &mut FakeStream) {
	while !stream.pending.is_empty() && stream.host_credit > 0 {
		let n = stream
			.pending
			.len()
			.min(usize::try_from(stream.host_credit).unwrap())
			.min(1000);
		let chunk: Vec<u8> = stream.pending.drain(..n).collect();
		stream.host_credit -= n as u64;
		send(writer, proto::FRAME_DATA, id, &chunk).await;
	}
	if stream.host_closed && stream.pending.is_empty() && !stream.closed_sent {
		stream.closed_sent = true;
		send(writer, proto::FRAME_CLOSE, id, &[]).await;
	}
}

/// Minimal agent: `hello`, `exec`, `fs_read`, `tcp_connect` (port 7 echo, port
/// 8 floods past the host window, anything else refused).
async fn fake_agent(stream: DuplexStream, report: Arc<FakeReport>) {
	let (mut reader, mut writer) = tokio::io::split(stream);
	let mut streams: HashMap<u32, FakeStream> = HashMap::new();
	while let Ok(Some(frame)) = read_frame(&mut reader).await {
		match frame.ty {
			proto::FRAME_REQ => {
				let req: Value = serde_json::from_slice(&frame.payload).unwrap();
				match serde_json::from_value::<Op>(req["op"].clone()).unwrap() {
					Op::Hello => {
						let resp = json!({ "ok": true, "epoch": req["epoch"] });
						send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes()).await;
					},
					Op::Exec => {
						send(&mut writer, proto::FRAME_STDOUT, frame.id, b"out").await;
						send(&mut writer, proto::FRAME_STDERR, frame.id, b"err").await;
						send(&mut writer, proto::FRAME_STDOUT, frame.id, b"").await;
						send(&mut writer, proto::FRAME_EXIT, frame.id, br#"{"code":3,"signal":null}"#)
							.await;
					},
					Op::FsRead => {
						send(&mut writer, proto::FRAME_STDOUT, frame.id, b"hello ").await;
						send(&mut writer, proto::FRAME_STDOUT, frame.id, b"file").await;
						send(&mut writer, proto::FRAME_STDOUT, frame.id, b"").await;
						send(&mut writer, proto::FRAME_RESP, frame.id, br#"{"ok":true,"size":10}"#).await;
					},
					Op::TcpConnect => {
						let host_window = req["window"].as_u64().unwrap();
						match req["port"].as_u64().unwrap() {
							7 => {
								streams.insert(frame.id, FakeStream {
									inflight:    0,
									host_credit: host_window,
									pending:     VecDeque::new(),
									host_closed: false,
									closed_sent: false,
								});
								let resp = json!({ "ok": true, "window": AGENT_WINDOW });
								send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes())
									.await;
							},
							8 => {
								let resp = json!({ "ok": true, "window": AGENT_WINDOW });
								send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes())
									.await;
								// One byte past the window, in frames within the
								// payload limit (the window may equal it).
								let flood = vec![7u8; usize::try_from(host_window).unwrap() + 1];
								for chunk in flood.chunks(proto::MAX_PAYLOAD_LEN) {
									send(&mut writer, proto::FRAME_DATA, frame.id, chunk).await;
								}
							},
							_ => {
								let resp =
									json!({ "ok": false, "error": "Connection refused", "errno": 111 });
								send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes())
									.await;
							},
						}
					},
					_ => {
						let resp = json!({ "ok": false, "error": "unknown op", "errno": 22 });
						send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes()).await;
					},
				}
			},
			proto::FRAME_DATA => {
				let Some(stream) = streams.get_mut(&frame.id) else {
					continue;
				};
				stream.inflight += frame.payload.len() as u64;
				if stream.inflight > u64::from(AGENT_WINDOW) {
					report.window_violation.store(true, Ordering::SeqCst);
				}
				// "Write to the guest socket" (echo queue), then credit it back.
				stream.pending.extend(frame.payload.iter().copied());
				let grant = u32::try_from(stream.inflight).unwrap();
				stream.inflight = 0;
				send(&mut writer, proto::FRAME_WINDOW, frame.id, &proto::encode_window(grant)).await;
				flush_echo(&mut writer, frame.id, stream).await;
			},
			proto::FRAME_WINDOW => {
				let Some(stream) = streams.get_mut(&frame.id) else {
					continue;
				};
				stream.host_credit += u64::from(proto::decode_window(&frame.payload).unwrap());
				flush_echo(&mut writer, frame.id, stream).await;
			},
			proto::FRAME_CLOSE => {
				if frame.payload.is_empty() {
					if let Some(stream) = streams.get_mut(&frame.id) {
						stream.host_closed = true;
						flush_echo(&mut writer, frame.id, stream).await;
					}
				} else {
					*report.host_close_reason.lock() =
						Some(String::from_utf8_lossy(&frame.payload).into_owned());
					streams.remove(&frame.id);
				}
			},
			_ => {},
		}
	}
}

async fn connected() -> (AgentClient, Arc<FakeReport>) {
	let (host, guest) = tokio::io::duplex(16 * 1024);
	let report = Arc::new(FakeReport::default());
	tokio::spawn(fake_agent(guest, Arc::clone(&report)));
	(AgentClient::handshake(host).await.expect("handshake"), report)
}

#[tokio::test]
async fn stale_reply_from_an_earlier_connection_is_not_routed_to_this_one() {
	let (host, guest) = tokio::io::duplex(4096);
	tokio::spawn(async move {
		let (mut reader, mut writer) = tokio::io::split(guest);
		let frame = read_frame(&mut reader).await.unwrap().unwrap();
		let req: Value = serde_json::from_slice(&frame.payload).unwrap();
		// The guest first answers the `hello` an earlier, timed-out connection
		// left in the console (that connection's first id, a stale epoch).
		send(&mut writer, proto::FRAME_RESP, 1, br#"{"ok":true,"epoch":42}"#).await;
		let resp = json!({ "ok": true, "epoch": req["epoch"] });
		send(&mut writer, proto::FRAME_RESP, frame.id, resp.to_string().as_bytes()).await;
		while let Ok(Some(_)) = read_frame(&mut reader).await {}
	});
	let client = AgentClient::handshake(host).await.expect("handshake");
	assert_ne!(client.epoch(), 42);
}

#[tokio::test]
async fn tunnel_echoes_megabytes_without_exceeding_the_agent_window() {
	let (client, report) = connected().await;
	let stream = client.tcp_connect(7).await.expect("connect");
	let (mut read, mut write) = tokio::io::split(stream);
	let payload: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
	let expected = payload.clone();
	let writer = tokio::spawn(async move {
		write.write_all(&payload).await.expect("tunnel write");
		write.shutdown().await.expect("tunnel shutdown");
		write
	});
	let mut echoed = Vec::new();
	tokio::time::timeout(Duration::from_secs(20), read.read_to_end(&mut echoed))
		.await
		.expect("echo finished")
		.expect("tunnel read");
	drop(writer.await.unwrap());
	assert_eq!(echoed.len(), expected.len());
	assert!(echoed == expected, "echoed bytes differ");
	assert!(!report.window_violation.load(Ordering::SeqCst), "host sent more than the agent window");
}

#[tokio::test]
async fn refused_port_maps_to_refused_error() {
	let (client, _) = connected().await;
	assert!(matches!(client.tcp_connect(9).await, Err(ConnectError::Refused)));
	// The connection stays usable after a refused stream.
	assert!(matches!(client.tcp_connect(9).await, Err(ConnectError::Refused)));
}

#[tokio::test]
async fn agent_exceeding_host_window_aborts_the_stream() {
	let (client, report) = connected().await;
	let mut stream = client.tcp_connect(8).await.expect("connect");
	// Read nothing until the host aborted: consuming would return credit and
	// legitimize the excess byte.
	tokio::time::timeout(Duration::from_secs(5), async {
		while report.host_close_reason.lock().is_none() {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.expect("host aborted the flooded stream");
	assert_eq!(report.host_close_reason.lock().as_deref(), Some("window exceeded"));
	// Bytes accepted within the window drain first, then the abort surfaces.
	let mut sink = Vec::new();
	let error = stream
		.read_to_end(&mut sink)
		.await
		.expect_err("flooded stream must fail");
	assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
	assert!(error.to_string().contains("window exceeded"), "{error}");
}

#[tokio::test]
async fn exec_and_fs_read_collect_frames_until_terminal_event() {
	let (client, _) = connected().await;
	let session = client
		.exec(&ExecRequest { cmd: vec!["true".into()], ..ExecRequest::default() })
		.await
		.expect("exec");
	let (stdout, stderr, code) = session.output().await.expect("output");
	assert_eq!((stdout.as_slice(), stderr.as_slice(), code), (&b"out"[..], &b"err"[..], 3));
	assert_eq!(client.fs_read("/etc/x").await.expect("fs_read"), b"hello file");
	let error = client
		.request(Op::Poweroff, Value::Null)
		.await
		.expect_err("fake agent rejects poweroff");
	assert!(
		error
			.to_string()
			.contains("agent poweroff: unknown op (errno 22)"),
		"{error}"
	);
	let error = client
		.request(Op::Ping, json!("not an object"))
		.await
		.expect_err("non-object arguments");
	assert!(error.to_string().contains("must be a JSON object"), "{error}");
}

/// Guest agent on a Unix socket (as behind the VMM's `agent.sock`) that
/// answers `hello` after `delay`: a cold boot still starting its init
/// services on a busy host. For paused-clock tests.
fn slow_booting_agent(path: &std::path::Path, delay: Duration) {
	// A paused clock jumps to the next timer whenever the runtime idles, even
	// while the client's reader task has a reply to read: keep that timer at
	// most 1 s away so the jump cannot skip past the client's deadline.
	tokio::spawn(async {
		loop {
			tokio::time::sleep(Duration::from_secs(1)).await;
		}
	});
	let listener = tokio::net::UnixListener::bind(path).expect("bind agent socket");
	tokio::spawn(async move {
		let (stream, _) = listener.accept().await.expect("accept");
		let (mut reader, mut writer) = tokio::io::split(stream);
		let hello = read_frame(&mut reader).await.unwrap().unwrap();
		let req: Value = serde_json::from_slice(&hello.payload).unwrap();
		tokio::time::sleep(delay).await;
		let resp = json!({ "ok": true, "epoch": req["epoch"] });
		let frame = encode(proto::FRAME_RESP, hello.id, resp.to_string().as_bytes());
		writer.write_all(&frame).await.expect("hello reply");
		while let Ok(Some(_)) = read_frame(&mut reader).await {}
	});
}

#[tokio::test(start_paused = true)]
async fn connect_waits_for_hello_up_to_the_callers_timeout() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("agent.sock");
	slow_booting_agent(&path, REQUEST_TIMEOUT + Duration::from_secs(15));
	AgentClient::connect(&path, Duration::from_mins(5))
		.await
		.expect("hello answered within the boot budget");
}

#[tokio::test(start_paused = true)]
async fn connect_gives_up_at_the_callers_timeout() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("agent.sock");
	slow_booting_agent(&path, Duration::from_mins(10));
	let error = AgentClient::connect(&path, Duration::from_mins(2))
		.await
		.expect_err("the agent never answered in time")
		.to_string();
	assert!(error.contains("hello") && error.contains("within 120s"), "{error}");
}

#[tokio::test]
async fn lost_connection_fails_pending_streams_and_requests() {
	let (host, guest) = tokio::io::duplex(4096);
	let report = Arc::new(FakeReport::default());
	let fake = tokio::spawn(fake_agent(guest, report));
	let client = AgentClient::handshake(host).await.expect("handshake");
	let mut stream = client.tcp_connect(7).await.expect("connect");
	fake.abort();
	let mut buf = [0u8; 4];
	let error = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
		.await
		.expect("read finished")
		.expect_err("stream must fail");
	assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
	assert!(client.is_closed());
	assert!(client.ping().await.is_err());
}
