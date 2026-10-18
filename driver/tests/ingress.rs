//! End-to-end ingress: public listener → KV route → node tunnel → fake guest.
//!
//! A single-driver in-process cluster holds the routes; a node-role cluster
//! member advertises a fake node server that implements
//! `GET /node/v1/sandboxes/{id}/tunnel/{port}` by serving an in-process
//! "guest" HTTP app on the upgraded connection. The store lives in the
//! driver's raft KV.

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
	Router,
	body::Body,
	extract::{Path, Request, State, WebSocketUpgrade, ws::Message},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{any, get},
};
use bytes::Bytes;
use futures::StreamExt;
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	sync::Notify,
};
use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeCapacity, NodeId, Roles};
use vmon_driver::{AppState, DriverConfig, TeamLimits, crypto::Keys, ingress, store::Store};
use vmon_node::api::{RouteState, SandboxRoute};

const SECRET: &str = "ingress-test-secret";
const DOMAIN: &str = "e2b.test";

/// Guest-side observations shared with the test.
#[derive(Default)]
struct Guest {
	/// Tunnel attempts per port.
	dials:            Mutex<HashMap<u16, u32>>,
	/// Ports that answer `port_closed` for the next N dials.
	closed_for:       Mutex<HashMap<u16, u32>>,
	/// Signalled when the streaming endpoint sees its reader go away.
	stream_cancelled: Notify,
	/// Makes `/break` abort its connection mid-body (the VM went away).
	break_stream:     Notify,
}

struct Harness {
	public:    SocketAddr,
	state:     AppState,
	guest:     Arc<Guest>,
	node_id:   NodeId,
	http:      reqwest::Client,
	_clusters: Vec<Cluster>,
	_dir:      tempfile::TempDir,
}

async fn spawn_router(router: Router) -> SocketAddr {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
	addr
}

/// The in-guest application reachable through tunnels.
fn guest_app(guest: Arc<Guest>) -> Router {
	Router::new()
		.route(
			"/echo",
			any(|headers: HeaderMap, req: Request| async move {
				let method = req.method().to_string();
				let uri = req.uri().to_string();
				let body = axum::body::to_bytes(req.into_body(), usize::MAX)
					.await
					.unwrap();
				let h = |name: &str| headers.get(name).map(|v| v.to_str().unwrap().to_owned());
				axum::Json(json!({
					"method": method,
					"uri": uri,
					"host": h("host"),
					"forwardedHost": h("x-forwarded-host"),
					"body": String::from_utf8_lossy(&body),
				}))
			}),
		)
		.route(
			"/stream",
			get(|State(guest): State<Arc<Guest>>| async move {
				let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
				tokio::spawn(async move {
					for i in 0.. {
						if tx
							.send(Ok(Bytes::from(format!("chunk-{i}\n"))))
							.await
							.is_err()
						{
							guest.stream_cancelled.notify_one();
							return;
						}
						tokio::time::sleep(Duration::from_millis(50)).await;
					}
				});
				Body::from_stream(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
			}),
		)
		.route(
			"/break",
			get(|State(guest): State<Arc<Guest>>| async move {
				let chunks = futures::stream::once(async {
					Ok::<_, std::io::Error>(Bytes::from_static(b"first\n"))
				})
				.chain(futures::stream::once(async move {
					guest.break_stream.notified().await;
					Err(std::io::Error::other("vm stopped"))
				}));
				Body::from_stream(chunks)
			}),
		)
		.route(
			"/ws",
			get(|ws: WebSocketUpgrade| async move {
				ws.on_upgrade(|mut socket| async move {
					while let Some(Ok(message)) = socket.recv().await {
						if let Message::Text(text) = message
							&& socket
								.send(Message::Text(format!("echo:{}", text.as_str()).into()))
								.await
								.is_err()
						{
							return;
						}
					}
				})
			}),
		)
		.with_state(guest)
}

/// Fake node: the tunnel endpoint upgrades and serves [`guest_app`] on the
/// raw connection, or refuses with `port_closed` / `not_found`.
fn fake_node(guest: Arc<Guest>) -> Router {
	Router::new()
		.route(
			"/node/v1/sandboxes/{id}/tunnel/{port}",
			get(
				|State(guest): State<Arc<Guest>>,
				 Path((id, port)): Path<(String, u16)>,
				 mut req: Request| async move {
					assert_eq!(req.headers()[header::AUTHORIZATION], format!("Bearer {SECRET}"));
					*guest.dials.lock().entry(port).or_default() += 1;
					if id != "irunning" {
						return (
							StatusCode::NOT_FOUND,
							axum::Json(json!({"code":"not_found","message":"no sandbox"})),
						)
							.into_response();
					}
					{
						let mut closed = guest.closed_for.lock();
						if let Some(left) = closed.get_mut(&port)
							&& *left > 0
						{
							*left -= 1;
							return (
								StatusCode::BAD_GATEWAY,
								axum::Json(json!({"code":"port_closed","message":"refused"})),
							)
								.into_response();
						}
					}
					let upgrade = hyper::upgrade::on(&mut req);
					let app = guest_app(guest.clone());
					tokio::spawn(async move {
						let io = upgrade.await.unwrap();
						if port == TLS_GUEST_PORT {
							python_https_guest(io).await;
							return;
						}
						let _ = hyper::server::conn::http1::Builder::new()
							.serve_connection(io, TowerToHyperService::new(app))
							.with_upgrades()
							.await;
					});
					Response::builder()
						.status(StatusCode::SWITCHING_PROTOCOLS)
						.header(header::CONNECTION, "upgrade")
						.header(header::UPGRADE, "vmon-tunnel")
						.body(Body::empty())
						.unwrap()
				},
			),
		)
		.with_state(guest)
}

fn cluster_config(
	dir: &std::path::Path,
	roles: Roles,
	cluster_url: String,
	join: Vec<String>,
) -> ClusterConfig {
	ClusterConfig {
		id: NodeId::random(),
		data_dir: dir.to_path_buf(),
		roles,
		gossip_bind: "127.0.0.1:0".parse().unwrap(),
		gossip_advertise: "127.0.0.1:0".parse().unwrap(),
		cluster_url,
		join,
		secret: SECRET.as_bytes().to_vec(),
		bootstrap_expect: 1,
		raft_reap_after: Duration::from_mins(10),
	}
}

async fn harness() -> Harness {
	let dir = tempfile::tempdir().unwrap();
	let guest = Arc::new(Guest::default());

	// Driver cluster: serve its router on a pre-bound port so raft can reach it.
	let driver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let driver_url = format!("http://{}", driver_listener.local_addr().unwrap());
	let driver_roles = Roles { node: false, driver: true };
	let driver_cfg =
		cluster_config(&dir.path().join("driver"), driver_roles, driver_url, Vec::new());
	let driver =
		Cluster::start(driver_cfg, MemberMeta { version: "test".into(), ..MemberMeta::default() })
			.await
			.unwrap();
	let cluster_router = driver.router();
	tokio::spawn(async move { axum::serve(driver_listener, cluster_router).await.unwrap() });

	// Node member advertising the fake node server.
	let node_addr = spawn_router(fake_node(guest.clone())).await;
	let node_cfg = cluster_config(
		&dir.path().join("node"),
		Roles { node: true, driver: false },
		format!("http://{node_addr}"),
		vec![driver.gossip.local_addr().to_string()],
	);
	let node_id = node_cfg.id;
	let node_meta = MemberMeta {
		version: "test".into(),
		node: Some(NodeCapacity {
			arch: "aarch64".into(),
			cpus: 4,
			memory_mb: 4096,
			..NodeCapacity::default()
		}),
		..MemberMeta::default()
	};
	let node = Cluster::start(node_cfg, node_meta).await.unwrap();

	let store = Store::new(driver.kv.clone());
	let config = DriverConfig {
		domain:         DOMAIN.into(),
		public_url:     "http://127.0.0.1".into(),
		admin_token:    None,
		cluster_secret: SECRET.into(),
		data_dir:       dir.path().join("data"),
		default_limits: TeamLimits::default(),
		shared_volumes: false,
		object_store:   None,
	};
	let state = AppState::new(config, store, driver.clone(), Keys::derive(SECRET.as_bytes()));

	// Wait for raft and for gossip to see the node.
	let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
	loop {
		let kv_ready = driver
			.kv
			.put("test/ready", b"1".to_vec(), None)
			.await
			.is_ok();
		if kv_ready && state.node(node_id).is_ok() {
			break;
		}
		assert!(tokio::time::Instant::now() < deadline, "cluster did not converge");
		tokio::time::sleep(Duration::from_millis(100)).await;
	}

	let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let public_addr = public.local_addr().unwrap();
	let api = Router::new().route("/health", get(|| async { StatusCode::NO_CONTENT }));
	let router = ingress::wrap(state.clone(), api);
	tokio::spawn(ingress::listener::serve_listener(public, None, router, std::future::pending()));

	Harness {
		public: public_addr,
		state,
		guest,
		node_id,
		http: reqwest::Client::new(),
		_clusters: vec![driver, node],
		_dir: dir,
	}
}

impl Harness {
	async fn put_route(&self, id: &str, edit: impl FnOnce(&mut SandboxRoute)) {
		let mut route = SandboxRoute {
			team_id:              "team".into(),
			node_id:              self.node_id,
			template_id:          "tpl".into(),
			build_id:             "build".into(),
			state:                RouteState::Running,
			started_at_ms:        0,
			end_at_ms:            u64::MAX,
			auto_pause:           false,
			auto_resume:          false,
			allow_public_traffic: true,
			traffic_token:        None,
			mask_request_host:    None,
			https_ports:          Vec::new(),
			vcpus:                1,
			memory_mb:            512,
			envd_version:         "0.6.4".into(),
		};
		edit(&mut route);
		self
			.state
			.cluster
			.kv
			.put(&format!("sbx/{id}"), serde_json::to_vec(&route).unwrap(), None)
			.await
			.unwrap();
	}

	fn url(&self, path: &str) -> String {
		format!("http://{}{path}", self.public)
	}

	fn get_host(&self, host: &str, path: &str) -> reqwest::RequestBuilder {
		self.http.get(self.url(path)).header(header::HOST, host)
	}

	fn dials(&self, port: u16) -> u32 {
		self.guest.dials.lock().get(&port).copied().unwrap_or(0)
	}
}

/// Pausing ends every tunnel; the client must not see that as a failed stream
/// while the sandbox is paused (the SDK would health-probe and auto-resume
/// it), but must once the sandbox runs again or is gone.
#[tokio::test(flavor = "multi_thread")]
async fn broken_stream_is_held_while_paused() {
	let h = harness().await;
	let host = format!("8000-irunning.{DOMAIN}");
	let open_stream = || async {
		let response = h.get_host(&host, "/break").send().await.unwrap();
		assert_eq!(response.status(), StatusCode::OK);
		let mut stream = response.bytes_stream();
		assert_eq!(stream.next().await.unwrap().unwrap(), Bytes::from_static(b"first\n"));
		stream
	};
	let next_within = Duration::from_millis;

	// Running sandbox: an upstream failure reaches the client at once.
	h.put_route("irunning", |_| {}).await;
	let mut stream = open_stream().await;
	h.guest.break_stream.notify_one();
	let ended = tokio::time::timeout(next_within(2_000), stream.next())
		.await
		.expect("not held");
	assert!(ended.is_some_and(|item| item.is_err()), "running: failure is reported");

	// Pausing: held while paused, released with the failure once resumed.
	let mut stream = open_stream().await;
	h.put_route("irunning", |r| r.state = RouteState::Pausing)
		.await;
	h.guest.break_stream.notify_one();
	assert!(
		tokio::time::timeout(next_within(1_500), stream.next())
			.await
			.is_err(),
		"held while the sandbox pauses"
	);
	h.put_route("irunning", |_| {}).await;
	let ended = tokio::time::timeout(next_within(2_000), stream.next())
		.await
		.expect("released on resume");
	assert!(ended.is_some_and(|item| item.is_err()));

	// Pausing, then gone without a paused row (killed): released.
	let mut stream = open_stream().await;
	h.put_route("irunning", |r| r.state = RouteState::Pausing)
		.await;
	h.guest.break_stream.notify_one();
	assert!(
		tokio::time::timeout(next_within(1_000), stream.next())
			.await
			.is_err()
	);
	h.state.cluster.kv.delete("sbx/irunning").await.unwrap();
	let ended = tokio::time::timeout(next_within(2_000), stream.next())
		.await
		.expect("released when gone");
	assert!(ended.is_some_and(|item| item.is_err()));
}

#[tokio::test(flavor = "multi_thread")]
async fn ingress_end_to_end() {
	let h = harness().await;
	h.put_route("irunning", |_| {}).await;

	// API traffic still reaches the API router, with CORS added.
	let response = h.http.get(h.url("/health")).send().await.unwrap();
	assert_eq!(response.status(), StatusCode::NO_CONTENT);
	assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");

	// Plain HTTP by Host: Host preserved, request body forwarded.
	let host = format!("8000-irunning.{DOMAIN}");
	let response = h
		.http
		.post(h.url("/echo?x=1"))
		.header(header::HOST, &host)
		.body("ping")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
	let body: Value = response.json().await.unwrap();
	assert_eq!(body["method"], "POST");
	assert_eq!(body["uri"], "/echo?x=1");
	assert_eq!(body["host"], host.as_str());
	assert_eq!(body["forwardedHost"], Value::Null);
	assert_eq!(body["body"], "ping");

	// Header routing from localhost.
	let response = h
		.http
		.get(h.url("/echo"))
		.header("e2b-sandbox-id", "irunning")
		.header("e2b-sandbox-port", "8000")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::OK);

	// envd control paths never reach the guest.
	let before = h.dials(49983);
	let response = h
		.http
		.post(h.url("/init"))
		.header(header::HOST, format!("49983-irunning.{DOMAIN}"))
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::NOT_FOUND);
	assert_eq!(h.dials(49983), before);

	// Streaming: the first chunk arrives while the body is still open (the
	// guest never ends it), and dropping the client cancels the guest stream.
	let response = h.get_host(&host, "/stream").send().await.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	let mut stream = response.bytes_stream();
	let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
		.await
		.expect("first chunk in time");
	assert_eq!(first.unwrap().unwrap(), Bytes::from_static(b"chunk-0\n"));
	drop(stream);
	tokio::time::timeout(Duration::from_secs(5), h.guest.stream_cancelled.notified())
		.await
		.expect("client disconnect propagates to the guest");

	// WebSocket echo through the tunnel.
	assert_eq!(
		websocket_echo(h.public, &format!("6080-irunning.{DOMAIN}"), "hello").await,
		"echo:hello"
	);

	// Port closed twice, then open: dial retries succeed.
	h.guest.closed_for.lock().insert(8100, 2);
	let response = h
		.get_host(&format!("8100-irunning.{DOMAIN}"), "/echo")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	assert_eq!(h.dials(8100), 3);

	// Port never opens: 5 attempts, then the upstream 502 body.
	h.guest.closed_for.lock().insert(8200, u32::MAX);
	let response = h
		.get_host(&format!("8200-irunning.{DOMAIN}"), "/echo")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
	assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
	let body: Value = response.json().await.unwrap();
	assert_eq!(
		body,
		json!({"sandboxId":"irunning","message":"The sandbox is running but port is not open","port":8200,"code":502})
	);
	assert_eq!(h.dials(8200), 5);

	// Unknown sandbox: no route, nothing paused in the store.
	let unknown = "iunknown0000000000000";
	let response = h
		.get_host(&format!("49983-{unknown}.{DOMAIN}"), "/health")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
	let body: Value = response.json().await.unwrap();
	assert_eq!(body, json!({"sandboxId":unknown,"message":"The sandbox was not found","code":502}));

	// Private traffic + masked host.
	h.put_route("irunning", |r| {
		r.allow_public_traffic = false;
		r.traffic_token = Some("tok".into());
		r.mask_request_host = Some("app.local:${PORT}".into());
	})
	.await;
	let response = h.get_host(&host, "/echo").send().await.unwrap();
	assert_eq!(response.status(), StatusCode::FORBIDDEN);
	let response = h
		.get_host(&host, "/echo")
		.header("e2b-traffic-access-token", "bad")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::FORBIDDEN);
	let response = h
		.get_host(&host, "/echo")
		.header("e2b-traffic-access-token", "tok")
		.send()
		.await
		.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	let body: Value = response.json().await.unwrap();
	assert_eq!(body["host"], "app.local:8000");
	assert_eq!(body["forwardedHost"], host.as_str());
}

/// Guest port served by [`python_https_guest`].
const TLS_GUEST_PORT: u16 = 8443;

/// Mimics the upstream SDK test's `http.server` + `ssl.wrap_socket` backend:
/// HTTP/1.0 response delimited by connection close, and a TCP FIN without a
/// TLS `close_notify` (Python's `SSLSocket.shutdown` skips it).
async fn python_https_guest(io: hyper::upgrade::Upgraded) {
	let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
	let config = rustls::ServerConfig::builder_with_provider(Arc::new(
		rustls::crypto::ring::default_provider(),
	))
	.with_safe_default_protocol_versions()
	.unwrap()
	.with_no_client_auth()
	.with_single_cert(
		vec![key.cert.der().clone()],
		rustls::pki_types::PrivateKeyDer::Pkcs8(key.signing_key.serialize_der().into()),
	)
	.unwrap();
	let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
	let Ok(mut tls) = acceptor.accept(hyper_util::rt::TokioIo::new(io)).await else {
		return;
	};
	let mut head = Vec::new();
	while !head.ends_with(b"\r\n\r\n") {
		let Ok(byte) = tls.read_u8().await else {
			return;
		};
		head.push(byte);
	}
	tls.write_all(b"HTTP/1.0 200 OK\r\nServer: BaseHTTP/0.6 Python/3.12.3\r\n\r\nhttps backend")
		.await
		.unwrap();
	tls.flush().await.unwrap();
	let (mut raw, _session) = tls.into_inner();
	let _ = raw.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn https_port_close_delimited_without_close_notify() {
	let h = harness().await;
	h.put_route("irunning", |r| r.https_ports = vec![TLS_GUEST_PORT])
		.await;
	let host = format!("{TLS_GUEST_PORT}-irunning.{DOMAIN}");
	let response = tokio::time::timeout(
		Duration::from_secs(10),
		h.get_host(&host, "/")
			.header("sec-fetch-mode", "cors")
			.send(),
	)
	.await
	.expect("response head in time")
	.unwrap();
	assert_eq!(response.status(), StatusCode::OK);
	let body = tokio::time::timeout(Duration::from_secs(10), response.text())
		.await
		.expect("body completes")
		.expect("body is not truncated by the guest's missing close_notify");
	assert_eq!(body, "https backend");

	// Node's fetch negotiates HTTP/2: same request over h2c.
	for streaming_request_body in [false, true] {
		let body = tokio::time::timeout(
			Duration::from_secs(10),
			h2_get(h.public, &host, streaming_request_body),
		)
		.await
		.expect("h2 response in time");
		assert_eq!(
			body,
			(200, "https backend".to_owned()),
			"streaming body {streaming_request_body}"
		);
	}
}

/// GET over HTTP/2 prior knowledge (`:authority` = `authority`, no Host).
/// `streaming_request_body` sends HEADERS without `END_STREAM` followed by an
/// empty DATA frame, as browsers and undici may do.
async fn h2_get(addr: SocketAddr, authority: &str, streaming_request_body: bool) -> (u16, String) {
	use http_body_util::{BodyExt, Empty, StreamBody};
	let tcp = TcpStream::connect(addr).await.unwrap();
	let (mut sender, conn) = hyper::client::conn::http2::handshake(
		hyper_util::rt::TokioExecutor::new(),
		hyper_util::rt::TokioIo::new(tcp),
	)
	.await
	.unwrap();
	tokio::spawn(conn);
	let uri = format!("http://{authority}/");
	let request = hyper::Request::get(uri).header("sec-fetch-mode", "cors");
	let response = if streaming_request_body {
		let frames =
			futures::stream::iter(Vec::<Result<hyper::body::Frame<Bytes>, std::io::Error>>::new());
		sender
			.send_request(
				request
					.body(BodyExt::boxed(StreamBody::new(frames)))
					.unwrap(),
			)
			.await
	} else {
		sender
			.send_request(
				request
					.body(BodyExt::boxed(Empty::<Bytes>::new().map_err(|never| match never {})))
					.unwrap(),
			)
			.await
	}
	.unwrap();
	let status = response.status().as_u16();
	let body = response
		.into_body()
		.collect()
		.await
		.expect("h2 body completes")
		.to_bytes();
	(status, String::from_utf8_lossy(&body).into_owned())
}

/// Minimal RFC 6455 client: handshake, one masked text frame, one reply.
async fn websocket_echo(addr: SocketAddr, host: &str, text: &str) -> String {
	let mut stream = TcpStream::connect(addr).await.unwrap();
	let request = format!(
		"GET /ws HTTP/1.1\r\nHost: {host}\r\nConnection: Upgrade\r\nUpgrade: \
		 websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: \
		 dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	);
	stream.write_all(request.as_bytes()).await.unwrap();
	let mut head = Vec::new();
	while !head.ends_with(b"\r\n\r\n") {
		head.push(stream.read_u8().await.unwrap());
	}
	let head = String::from_utf8(head).unwrap();
	assert!(head.starts_with("HTTP/1.1 101"), "{head}");

	let mask = [1u8, 2, 3, 4];
	let mut frame = vec![0x81, 0x80 | u8::try_from(text.len()).unwrap()];
	frame.extend_from_slice(&mask);
	frame.extend(text.bytes().enumerate().map(|(i, b)| b ^ mask[i % 4]));
	stream.write_all(&frame).await.unwrap();

	let mut header = [0u8; 2];
	stream.read_exact(&mut header).await.unwrap();
	assert_eq!(header[0], 0x81);
	let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
	stream.read_exact(&mut payload).await.unwrap();
	String::from_utf8(payload).unwrap()
}
