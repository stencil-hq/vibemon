//! Distributed `vmon serve` end to end (ARCHITECTURE.md §5, §9.4).
//!
//! Gated by `VMON_CLUSTER_E2E=1` (plus a usable hypervisor). Starts three
//! driver-only and two node-only processes on loopback, sharing one fresh
//! cluster secret (and so one raft KV holding the driver store), then proves:
//!
//! - raft bootstraps exactly once and every driver sees both nodes;
//! - a template builds on a node; sandboxes created through one driver are
//!   reachable (envd exec + files over ingress) through the other two, and the
//!   KV route reads the same on every driver;
//! - draining the builder evacuates its sandbox to the other node and places
//!   the next sandbox there (peer artifact fetch); an explicit migration moves
//!   the sandbox back;
//! - SIGKILL of the raft leader: a new leader is elected, the API keeps
//!   answering on the survivors, and the new leader's reaper still enforces
//!   timeouts; the restarted driver rejoins without a second bootstrap;
//! - SIGKILL of a node: its sandboxes become killed (store + events), placement
//!   avoids it, and its restart kills the orphaned VMM;
//! - SIGTERM + restart of a node (gossip `Left`): its sandboxes survive and are
//!   adopted;
//! - handover: `POST /admin/sandboxes/{id}/migrate` moves a running sandbox to
//!   the other node with its guest processes, memory and files, and draining a
//!   node evacuates its running and node-local paused sandboxes.
//!
//! Environment: `VMON_KERNEL`, `VMON_AGENT` (default
//! `target/test-assets/{Image,vmon-agent}-<arch>`), `VMON_CLUSTER_NET` (`--net`
//! of the nodes, default `user`), `VMON_CLUSTER_IMAGE` (template base
//! image, default `alpine:3.20`). The portable-pause case additionally requires
//! `VMON_CLUSTER_OBJECT_STORE` (a dedicated writable S3 URL; normal S3
//! credentials and endpoint configuration apply). The other case leaves object
//! storage off. On macOS run it through
//! `demo/hvf-test-runner.sh` so the spawned `vmon` carries the HVF
//! entitlement (`just cluster-e2e`).
#![cfg(unix)]

use std::{
	collections::HashMap,
	fs,
	net::{TcpListener, UdpSocket},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	time::{Duration, Instant},
};

use base64::Engine as _;
use serde_json::{Value, json};

const SECRET: &str = "cluster-e2e-secret";
const ADMIN_TOKEN: &str = "cluster-e2e-admin";
const DOMAIN: &str = "cluster.test";
const ENVD_PORT: u16 = 49983;
/// Raft voters dead this long are removed (fast for the test).
const RAFT_REAP_AFTER_SECS: u64 = 20;

fn enabled() -> bool {
	if std::env::var("VMON_CLUSTER_E2E").as_deref() != Ok("1") {
		return false;
	}
	if cfg!(target_os = "linux") {
		return Path::new("/dev/kvm").exists();
	}
	Command::new("sysctl")
		.args(["-n", "kern.hv_support"])
		.output()
		.is_ok_and(|o| o.stdout.starts_with(b"1"))
}

const fn arch() -> &'static str {
	if cfg!(target_arch = "aarch64") {
		"aarch64"
	} else {
		"x86_64"
	}
}

fn asset(var: &str, name: &str) -> PathBuf {
	let path = std::env::var(var).map_or_else(
		|_| {
			Path::new(env!("CARGO_MANIFEST_DIR"))
				.join("target/test-assets")
				.join(name)
		},
		PathBuf::from,
	);
	assert!(path.exists(), "{} missing (set {var})", path.display());
	path
}

/// A loopback port free for both TCP and UDP (gossip binds both).
fn free_port() -> u16 {
	loop {
		let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
		let port = tcp.local_addr().unwrap().port();
		if UdpSocket::bind(("127.0.0.1", port)).is_ok() {
			return port;
		}
	}
}

/// One `vmon serve` process of the topology.
struct Proc {
	name:     String,
	driver:   bool,
	api:      u16,
	cluster:  u16,
	gossip:   u16,
	data_dir: PathBuf,
	run_dir:  PathBuf,
	log:      PathBuf,
	child:    Option<Child>,
}

impl Proc {
	fn api_url(&self) -> String {
		format!("http://127.0.0.1:{}", self.api)
	}

	fn cluster_url(&self) -> String {
		format!("http://127.0.0.1:{}", self.cluster)
	}

	fn node_id(&self) -> String {
		fs::read_to_string(self.data_dir.join("node-id"))
			.unwrap()
			.trim()
			.to_owned()
	}

	fn log_text(&self) -> String {
		fs::read_to_string(&self.log).unwrap_or_default()
	}

	fn signal(&mut self, signal: libc::c_int) -> std::process::ExitStatus {
		let mut child = self.child.take().expect("process running");
		let pid = libc::pid_t::try_from(child.id()).unwrap();
		// SAFETY: `kill` on our own child's pid has no memory effects.
		assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
		child.wait().unwrap()
	}
}

struct Cluster {
	procs:        Vec<Proc>,
	join:         String,
	kernel:       PathBuf,
	agent:        PathBuf,
	net:          String,
	root:         PathBuf,
	http:         reqwest::Client,
	key:          String,
	object_store: Option<String>,
}

impl Cluster {
	fn get(&self, name: &str) -> &Proc {
		self.procs.iter().find(|p| p.name == name).unwrap()
	}

	fn get_mut(&mut self, name: &str) -> &mut Proc {
		self.procs.iter_mut().find(|p| p.name == name).unwrap()
	}

	fn spawn(&mut self, name: &str) {
		let (join, kernel, agent, net) =
			(self.join.clone(), self.kernel.clone(), self.agent.clone(), self.net.clone());
		let object_store = self.object_store.clone();
		let p = self.get_mut(name);
		let log = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&p.log)
			.unwrap();
		let mut cmd = Command::new(env!("CARGO_BIN_EXE_vmon"));
		cmd.arg("serve")
			.args(["--role", if p.driver { "driver" } else { "node" }])
			.arg("--data-dir")
			.arg(&p.data_dir)
			.arg("--run-dir")
			.arg(&p.run_dir)
			.args(["--listen", &format!("127.0.0.1:{}", p.api)])
			.args(["--cluster-listen", &format!("127.0.0.1:{}", p.cluster)])
			.args(["--gossip-listen", &format!("127.0.0.1:{}", p.gossip)])
			.args(["--join", &join, "--cluster-secret", SECRET, "--domain", DOMAIN])
			.env("VMON_LOG", "info")
			.env_remove("VMON_OBJECT_STORE")
			.stdin(Stdio::null())
			.stdout(log.try_clone().unwrap())
			.stderr(log);
		if p.driver {
			cmd.args(["--bootstrap-expect", "3"])
				.args(["--admin-token", ADMIN_TOKEN])
				.args(["--raft-reap-after", &RAFT_REAP_AFTER_SECS.to_string()]);
		} else {
			cmd.arg("--kernel")
				.arg(kernel)
				.arg("--agent")
				.arg(agent)
				.args(["--net", &net, "--max-vcpus", "4", "--max-memory-mb", "4096"]);
		}
		if let Some(url) = object_store {
			cmd.args(["--object-store", &url]);
		}
		p.child = Some(cmd.spawn().unwrap());
	}

	fn drivers(&self) -> impl Iterator<Item = &Proc> {
		self.procs.iter().filter(|p| p.driver && p.child.is_some())
	}

	/// The driver whose log last announced `becomes leader` (and not `steps
	/// down from leader` after it).
	fn leader(&self) -> Option<String> {
		self
			.drivers()
			.find(|p| {
				let log = p.log_text();
				let become_at = log.rfind("becomes leader");
				become_at.is_some() && become_at > log.rfind("steps down from leader")
			})
			.map(|p| p.name.clone())
	}

	async fn api(
		&self,
		via: &str,
		method: reqwest::Method,
		path: &str,
		body: Option<Value>,
	) -> (u16, Value) {
		let mut req = self
			.http
			.request(method, format!("{}{path}", self.get(via).api_url()))
			.header("X-API-Key", &self.key)
			.header("X-Admin-Token", ADMIN_TOKEN);
		if let Some(body) = body {
			req = req.json(&body);
		}
		match req.send().await {
			Ok(resp) => {
				let status = resp.status().as_u16();
				let bytes = resp.bytes().await.unwrap_or_default();
				(status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
			},
			Err(_) => (0, Value::Null),
		}
	}

	/// KV route `sbx/<id>` as read by `via` (`None` when absent).
	async fn route(
		&self,
		via: &str,
		sandbox: &str,
		consistent: bool,
	) -> Result<Option<Value>, String> {
		self.kv(via, &format!("sbx/{sandbox}"), consistent).await
	}

	async fn kv(&self, via: &str, key: &str, consistent: bool) -> Result<Option<Value>, String> {
		let resp = self
			.http
			.post(format!("{}/cluster/v1/kv/get", self.get(via).cluster_url()))
			.bearer_auth(SECRET)
			.json(&json!({"key": key, "consistent": consistent}))
			.send()
			.await
			.map_err(|e| e.to_string())?;
		let body: Value = resp.json().await.map_err(|e| e.to_string())?;
		match (&body["ok"], &body["err"]) {
			(Value::Null, Value::Null) if body.get("ok").is_some() => Ok(None),
			(Value::Object(entry), _) => {
				let raw = hex::decode(entry["value"].as_str().unwrap()).unwrap();
				Ok(Some(serde_json::from_slice(&raw).unwrap()))
			},
			_ => Err(body.to_string()),
		}
	}

	fn envd(
		&self,
		via: &str,
		sandbox: &Sandbox,
		method: reqwest::Method,
		path: &str,
	) -> reqwest::RequestBuilder {
		self
			.http
			.request(method, format!("{}{path}", self.get(via).api_url()))
			.header("E2b-Sandbox-Id", &sandbox.id)
			.header("E2b-Sandbox-Port", ENVD_PORT.to_string())
			.header("X-Access-Token", &sandbox.token)
	}

	/// Runs `sh -c cmd` in the sandbox through `via`'s ingress; returns
	/// (stdout, exit code).
	async fn exec(&self, via: &str, sandbox: &Sandbox, cmd: &str) -> (String, i64) {
		let msg = json!({"process": {"cmd": "/bin/sh", "args": ["-c", cmd]}}).to_string();
		let mut body = vec![0u8];
		body.extend_from_slice(&u32::try_from(msg.len()).unwrap().to_be_bytes());
		body.extend_from_slice(msg.as_bytes());
		let resp = self
			.envd(via, sandbox, reqwest::Method::POST, "/process.Process/Start")
			.header("content-type", "application/connect+json")
			.body(body)
			.send()
			.await
			.unwrap();
		assert_eq!(resp.status(), 200, "exec via {via}");
		let bytes = resp.bytes().await.unwrap();
		let (mut rest, mut stdout, mut code) = (&bytes[..], String::new(), -1);
		while rest.len() >= 5 {
			let len = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
			let frame: Value = serde_json::from_slice(&rest[5..5 + len]).unwrap();
			if rest[0] & 2 == 0 {
				if let Some(data) = frame["event"]["data"]["stdout"].as_str() {
					let raw = base64::engine::general_purpose::STANDARD
						.decode(data)
						.unwrap();
					stdout.push_str(&String::from_utf8_lossy(&raw));
				}
				if let Some(exit) = frame["event"]["end"]["exitCode"].as_i64() {
					code = exit;
				} else if frame["event"].get("end").is_some() {
					code = 0;
				}
			}
			rest = &rest[5 + len..];
		}
		(stdout, code)
	}

	async fn wait_for<F, Fut>(&self, what: &str, timeout: Duration, mut check: F) -> Duration
	where
		F: FnMut() -> Fut,
		Fut: Future<Output = bool>,
	{
		let start = Instant::now();
		while start.elapsed() < timeout {
			if check().await {
				return start.elapsed();
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		self.dump_logs();
		panic!("timed out after {timeout:?} waiting for {what}");
	}

	fn dump_logs(&self) {
		for p in &self.procs {
			let log = p.log_text();
			let tail: Vec<&str> = log.lines().rev().take(40).collect();
			eprintln!("==== {} ({}) tail ====", p.name, p.log.display());
			for line in tail.iter().rev() {
				eprintln!("{line}");
			}
		}
	}

	async fn create(&self, via: &str, timeout_s: u64) -> Sandbox {
		let (status, body) = self
			.api(
				via,
				reqwest::Method::POST,
				"/sandboxes",
				Some(json!({"templateID": "alp", "timeout": timeout_s, "secure": true})),
			)
			.await;
		assert_eq!(status, 201, "create via {via}: {body}");
		Sandbox {
			id:    body["sandboxID"].as_str().unwrap().to_owned(),
			token: body["envdAccessToken"].as_str().unwrap().to_owned(),
		}
	}

	async fn node_of(&self, via: &str, sandbox: &Sandbox) -> String {
		let route = self
			.route(via, &sandbox.id, true)
			.await
			.unwrap()
			.expect("route");
		let node = route["nodeId"].as_str().unwrap().to_owned();
		self
			.procs
			.iter()
			.find(|p| !p.driver && p.node_id() == node)
			.unwrap()
			.name
			.clone()
	}

	/// The `sandbox.lifecycle.killed` event of `sandbox`, once recorded.
	async fn killed_event(&self, via: &str, sandbox: &str) -> Option<Value> {
		let (_, events) = self
			.api(via, reqwest::Method::GET, &format!("/events/sandboxes/{sandbox}"), None)
			.await;
		events
			.as_array()?
			.iter()
			.find(|e| e["type"] == "sandbox.lifecycle.killed")
			.cloned()
	}

	/// Starts a detached guest loop that rewrites `/tmp/<tag>-counter` every
	/// second and returns its identity (pid and kernel start time), which only
	/// a memory restore of the same guest preserves.
	async fn start_counter(&self, via: &str, sandbox: &Sandbox, tag: &str) -> String {
		let (identity, code) = self
			.exec(
				via,
				sandbox,
				&format!(
					"nohup /bin/sh -c 'i=0; while :; do i=$((i+1)); printf \"%s\\n\" \"$i\" > \
					 /tmp/{tag}-counter.next; mv /tmp/{tag}-counter.next /tmp/{tag}-counter; sleep 1; \
					 done' {tag}-loop </dev/null >/tmp/{tag}.log 2>&1 & pid=$!; echo \"$pid\" > \
					 /tmp/{tag}-pid; while [ ! -s /tmp/{tag}-counter ]; do sleep 0.1; done; kill -0 \
					 \"$pid\" && printf '%s\\n' \"$pid\" && awk '{{print $22}}' /proc/$pid/stat"
				),
			)
			.await;
		assert_eq!(code, 0, "start {tag} loop: {identity:?}");
		assert_eq!(identity.lines().count(), 2, "pid and kernel process start time");
		identity
	}

	/// The identity of `start_counter`'s loop while it still runs.
	async fn counter_identity(&self, via: &str, sandbox: &Sandbox, tag: &str) -> (String, i64) {
		self
			.exec(
				via,
				sandbox,
				&format!(
					"pid=$(cat /tmp/{tag}-pid); kill -0 \"$pid\" && printf '%s\\n' \"$pid\" && awk \
					 '{{print $22}}' /proc/$pid/stat"
				),
			)
			.await
	}

	/// Waits until `start_counter`'s loop counts past its current value.
	async fn counter_advances(&self, via: &str, sandbox: &Sandbox, tag: &str) {
		let read = format!("cat /tmp/{tag}-counter");
		let (now, code) = self.exec(via, sandbox, &read).await;
		assert_eq!(code, 0, "{tag} counter: {now:?}");
		let now: u64 = now.trim().parse().unwrap();
		self
			.wait_for(&format!("{tag} loop makes progress"), Duration::from_secs(10), || async {
				let (next, code) = self.exec(via, sandbox, &read).await;
				code == 0 && next.trim().parse::<u64>().is_ok_and(|next| next > now)
			})
			.await;
	}

	/// Sets a node's operator status (`ready`, `draining`, `unhealthy`) via d1.
	async fn set_node_status(&self, node: &str, status: &str) {
		let (code, body) = self
			.api(
				"d1",
				reqwest::Method::POST,
				&format!("/nodes/{}", self.get(node).node_id()),
				Some(json!({"status": status})),
			)
			.await;
		assert_eq!(code, 204, "{node} -> {status}: {body}");
	}

	/// `GET /admin/sandboxes?id=` item of a running or paused sandbox.
	async fn listed(&self, via: &str, sandbox: &str) -> Option<Value> {
		let (_, list) = self
			.api(via, reqwest::Method::GET, &format!("/admin/sandboxes?id={sandbox}"), None)
			.await;
		list.as_array()?.first().cloned()
	}

	/// Sandbox IDs the node's own API reports (`None` while unreachable).
	async fn node_inventory(&self, node: &str) -> Option<Vec<String>> {
		let resp = self
			.http
			.get(format!("{}/node/v1/sandboxes", self.get(node).cluster_url()))
			.bearer_auth(SECRET)
			.send()
			.await
			.ok()?;
		let list: Value = resp.json().await.ok()?;
		Some(
			list
				.as_array()?
				.iter()
				.filter_map(|s| s["sandboxId"].as_str().map(str::to_owned))
				.collect(),
		)
	}
}

impl Drop for Cluster {
	fn drop(&mut self) {
		for p in &mut self.procs {
			if let Some(mut child) = p.child.take() {
				let _ = child.kill();
				let _ = child.wait();
			}
		}
		// VMMs run in their own process group and outlive their node by
		// design; reap every VMM started under this run's directories.
		let _ = Command::new("pkill")
			.arg("-KILL")
			.arg("-f")
			.arg(self.root.display().to_string())
			.status();
	}
}

struct Sandbox {
	id:    String,
	token: String,
}

/// Kill the topology before removing its per-case directories.
fn cleanup(c: Cluster) {
	let root = c.root.clone();
	drop(c);
	let _ = fs::remove_dir_all(&root);
}

fn log_count(cluster: &Cluster, needle: &str) -> usize {
	cluster
		.procs
		.iter()
		.map(|p| p.log_text().matches(needle).count())
		.sum()
}

/// Each case gets isolated process directories and its own raft KV, even when
/// run in parallel.
async fn topology(suffix: &str, object_store: Option<String>) -> Cluster {
	// Short root: per-VM Unix sockets live under the run dirs (104-byte limit).
	let root = PathBuf::from(format!("/tmp/vce{suffix}-{}", std::process::id()));
	let _ = fs::remove_dir_all(&root);
	fs::create_dir_all(&root).unwrap();

	let names = [("d1", true), ("d2", true), ("d3", true), ("n1", false), ("n2", false)];
	let procs: Vec<Proc> = names
		.iter()
		.map(|(name, driver)| Proc {
			name:     (*name).to_owned(),
			driver:   *driver,
			api:      free_port(),
			cluster:  free_port(),
			gossip:   free_port(),
			data_dir: root.join(name).join("data"),
			run_dir:  root.join(name).join("run"),
			log:      root.join(format!("{name}.log")),
			child:    None,
		})
		.collect();
	let join = procs
		.iter()
		.filter(|p| p.driver)
		.map(|p| format!("127.0.0.1:{}", p.gossip))
		.collect::<Vec<_>>()
		.join(",");
	let mut c = Cluster {
		procs,
		join,
		kernel: asset("VMON_KERNEL", &format!("Image-{}", arch())),
		agent: asset("VMON_AGENT", &format!("vmon-agent-{}", arch())),
		net: std::env::var("VMON_CLUSTER_NET").unwrap_or_else(|_| "user".into()),
		root: root.clone(),
		http: reqwest::Client::builder()
			.timeout(Duration::from_mins(1))
			.build()
			.unwrap(),
		key: String::new(),
		object_store,
	};
	eprintln!("cluster e2e: logs under {}", root.display());
	for (name, _) in names {
		c.spawn(name);
	}

	// Raft bootstraps once; every driver sees both nodes in one cluster.
	let node_ids: Vec<String> = {
		c.wait_for("node ids", Duration::from_secs(30), || async {
			c.procs.iter().all(|p| p.data_dir.join("node-id").exists())
		})
		.await;
		["n1", "n2"].iter().map(|n| c.get(n).node_id()).collect()
	};
	let took = c
		.wait_for("both nodes ready on every driver", Duration::from_mins(1), || async {
			let mut cluster_ids = Vec::new();
			for d in ["d1", "d2", "d3"] {
				let (status, nodes) = c.api(d, reqwest::Method::GET, "/nodes", None).await;
				let Some(nodes) = nodes.as_array().filter(|_| status == 200) else {
					return false;
				};
				let ready: Vec<&str> = nodes
					.iter()
					.filter(|n| n["status"] == "ready")
					.filter_map(|n| n["id"].as_str())
					.collect();
				if !node_ids.iter().all(|id| ready.contains(&id.as_str())) {
					return false;
				}
				cluster_ids.extend(nodes.iter().map(|n| n["clusterID"].clone()));
			}
			cluster_ids.windows(2).all(|w| w[0] == w[1]) && cluster_ids[0].is_string()
		})
		.await;
	eprintln!("[topology] 3 drivers + 2 nodes converged in {took:?}");
	assert_eq!(log_count(&c, "bootstrapping raft cluster"), 1, "raft bootstraps exactly once");

	// Team + API key, written through the drivers' cluster KV.
	let cluster_urls = ["d1", "d2", "d3"]
		.iter()
		.map(|d| format!("http://127.0.0.1:{}", c.get(d).cluster))
		.collect::<Vec<_>>()
		.join(",");
	let out = Command::new(env!("CARGO_BIN_EXE_vmon"))
		.args(["admin", "--cluster-url", &cluster_urls, "--cluster-secret", SECRET])
		.args(["create-team", "cluster-e2e"])
		.output()
		.unwrap();
	let text = String::from_utf8_lossy(&out.stdout);
	c.key = text
		.lines()
		.find_map(|l| l.strip_prefix("api key").map(|k| k.trim().to_owned()))
		.unwrap_or_else(|| panic!("no api key in {text}"));
	c
}

async fn build_template(c: &Cluster) {
	// Template build through d1 (runs on one of the nodes).
	let image = std::env::var("VMON_CLUSTER_IMAGE").unwrap_or_else(|_| "alpine:3.20".into());
	let (status, req) = c
		.api(
			"d1",
			reqwest::Method::POST,
			"/v3/templates",
			Some(json!({"name": "alp", "cpuCount": 1, "memoryMB": 512})),
		)
		.await;
	assert_eq!(status, 202, "{req}");
	let (tid, bid) = (req["templateID"].as_str().unwrap(), req["buildID"].as_str().unwrap());
	let (status, body) = c
		.api(
			"d1",
			reqwest::Method::POST,
			&format!("/v2/templates/{tid}/builds/{bid}"),
			Some(json!({"fromImage": image, "steps": [], "force": false})),
		)
		.await;
	assert_eq!(status, 202, "{body}");
	let took = c
		.wait_for("template build", Duration::from_mins(15), || async {
			let (_, s) = c
				.api("d2", reqwest::Method::GET, &format!("/templates/{tid}/builds/{bid}/status"), None)
				.await;
			assert_ne!(s["status"], "error", "build failed: {s}");
			s["status"] == "ready"
		})
		.await;
	eprintln!("[build] template alp ({image}) ready in {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_topology_survives_driver_and_node_failures() {
	if !enabled() {
		eprintln!("skipping: set VMON_CLUSTER_E2E=1 on a host with KVM/HVF");
		return;
	}
	let mut c = topology("", None).await;
	build_template(&c).await;

	// Cross-driver ingress + KV route consistency.
	let s1 = c.create("d1", 600).await;
	let builder = c.node_of("d1", &s1).await;
	let other = if builder == "n1" { "n2" } else { "n1" };
	let resp = c
		.envd("d2", &s1, reqwest::Method::POST, "/files?path=/tmp/hello.txt")
		.header("content-type", "application/octet-stream")
		.body("written via d2")
		.send()
		.await
		.unwrap();
	assert!(resp.status().is_success(), "upload via d2: {}", resp.status());
	for via in ["d1", "d2", "d3"] {
		let (out, code) = c.exec(via, &s1, "cat /tmp/hello.txt; echo; hostname").await;
		assert_eq!(code, 0);
		assert!(out.starts_with("written via d2\n"), "exec via {via}: {out:?}");
		let got = c
			.envd(via, &s1, reqwest::Method::GET, "/files?path=/tmp/hello.txt")
			.send()
			.await
			.unwrap()
			.text()
			.await
			.unwrap();
		assert_eq!(got, "written via d2", "download via {via}");
	}
	let reference = c.route("d1", &s1.id, true).await.unwrap().unwrap();
	c.wait_for("identical KV routes on every driver", Duration::from_secs(5), || async {
		let mut same = true;
		for via in ["d1", "d2", "d3"] {
			same &= c.route(via, &s1.id, false).await.ok().flatten().as_ref() == Some(&reference);
			same &= c.route(via, &s1.id, true).await.ok().flatten().as_ref() == Some(&reference);
		}
		same
	})
	.await;
	eprintln!("[ingress] {} on {builder}: exec/files via d1,d2,d3 ok; KV route identical", s1.id);

	// Drain the builder: the next sandbox lands on the other node (artifact
	// fetched from the builder), then the builder is ready again. Draining
	// also evacuates s1, so wait for the node to be empty first.
	c.set_node_status(&builder, "draining").await;
	c.wait_for("builder evacuated", Duration::from_mins(5), || async {
		c.node_inventory(&builder)
			.await
			.is_some_and(|ids| ids.is_empty())
	})
	.await;
	assert_eq!(c.node_of("d2", &s1).await, other, "draining evacuates running sandboxes");
	let started = Instant::now();
	let s2 = c.create("d3", 600).await;
	assert_eq!(c.node_of("d3", &s2).await, other, "draining node is skipped");
	eprintln!(
		"[placement] drained {builder}; {} placed on {other} in {:?} (peer artifact fetch)",
		s2.id,
		started.elapsed()
	);
	c.set_node_status(&builder, "ready").await;
	assert_eq!(c.exec("d1", &s2, "echo hi").await, ("hi\n".to_owned(), 0));

	// The failure cases below need s1 on the builder again.
	let (status, moved) = c
		.api(
			"d2",
			reqwest::Method::POST,
			&format!("/admin/sandboxes/{}/migrate", s1.id),
			Some(json!({"nodeID": c.get(&builder).node_id()})),
		)
		.await;
	assert_eq!(status, 200, "migrate back: {moved}");
	assert_eq!(c.node_of("d1", &s1).await, builder);
	assert_eq!(c.exec("d3", &s1, "cat /tmp/hello.txt").await, ("written via d2".to_owned(), 0));
	let reference = c.route("d1", &s1.id, true).await.unwrap().unwrap();

	// Leader failover: kill the raft leader; the survivors elect a new one,
	// keep serving, and the new leader's reaper enforces a short timeout.
	let leader = c.leader().expect("a raft leader");
	let survivors: Vec<String> = ["d1", "d2", "d3"]
		.iter()
		.filter(|d| **d != leader)
		.map(|d| (*d).to_owned())
		.collect();
	let short = c.create(&survivors[0], 600).await;
	let (status, _) = c
		.api(
			&survivors[0],
			reqwest::Method::POST,
			&format!("/sandboxes/{}/timeout", short.id),
			Some(json!({"timeout": 8})),
		)
		.await;
	assert_eq!(status, 204);
	let end_at_ms = c
		.route(&survivors[0], &short.id, true)
		.await
		.unwrap()
		.unwrap()["endAtMs"]
		.as_u64()
		.unwrap();
	let killed_at = Instant::now();
	let _ = c.get_mut(&leader).signal(libc::SIGKILL);
	let election = c
		.wait_for("new raft leader", Duration::from_secs(15), || async {
			c.leader().is_some_and(|l| l != leader)
				&& c
					.route(&survivors[1], &s1.id, true)
					.await
					.is_ok_and(|r| r.is_some())
		})
		.await;
	let api_failures = std::cell::Cell::new(0u32);
	let reaped = c
		.wait_for("reaper on the new leader", Duration::from_secs(40), || async {
			for d in &survivors {
				if c
					.api(d, reqwest::Method::GET, "/v2/sandboxes", None)
					.await
					.0 != 200
				{
					api_failures.set(api_failures.get() + 1);
				}
			}
			c.api(&survivors[1], reqwest::Method::GET, &format!("/sandboxes/{}", short.id), None)
				.await
				.0 == 404
				&& c.killed_event(&survivors[0], &short.id).await.is_some()
		})
		.await;
	let now_ms = u64::try_from(
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis(),
	)
	.unwrap();
	eprintln!(
		"[failover] SIGKILL leader {leader}: new leader {} after {election:?}; {} reaped {:.1}s \
		 after endAt ({reaped:?} after kill); {} API failures",
		c.leader().unwrap(),
		short.id,
		now_ms.saturating_sub(end_at_ms) as f64 / 1000.0,
		api_failures.get(),
	);
	assert_eq!(api_failures.get(), 0, "survivors kept serving");
	assert!(killed_at.elapsed() < Duration::from_secs(40));
	let killed = c.killed_event(&survivors[0], &short.id).await.unwrap();
	assert_eq!(killed["eventData"]["kill_reason"], "timeout", "{killed}");

	// The killed driver restarts and rejoins (no second bootstrap).
	c.spawn(&leader);
	let rejoin = c
		.wait_for("restarted driver serves KV", Duration::from_secs(30), || async {
			c.route(&leader, &s1.id, true)
				.await
				.is_ok_and(|r| r.is_some())
				&& c.route(&leader, &s1.id, false).await.ok().flatten() == Some(reference.clone())
		})
		.await;
	eprintln!("[rejoin] {leader} back in raft after {rejoin:?}");
	assert_eq!(log_count(&c, "bootstrapping raft cluster"), 1, "no second bootstrap");

	// Node loss: SIGKILL the builder; its sandbox is marked killed, placement
	// avoids it; its restart kills the orphaned (still running) VMM.
	let lost_at = Instant::now();
	let _ = c.get_mut(&builder).signal(libc::SIGKILL);
	let detected = c
		.wait_for("node loss reconciled", Duration::from_mins(1), || async {
			c.api("d1", reqwest::Method::GET, &format!("/sandboxes/{}", s1.id), None)
				.await
				.0 == 404
				&& c.killed_event("d2", &s1.id).await.is_some()
		})
		.await;
	let killed = c.killed_event("d2", &s1.id).await.unwrap();
	assert_eq!(killed["eventData"]["kill_reason"], "orphaned", "{killed}");
	let s3 = c.create("d2", 600).await;
	assert_eq!(c.node_of("d2", &s3).await, other, "placement avoids the dead node");
	eprintln!(
		"[node-loss] SIGKILL {builder}: {} killed (store + event) after {detected:?}; new sandbox \
		 on {other}",
		s1.id
	);
	assert!(lost_at.elapsed() < Duration::from_mins(1));
	let vmm_alive = || {
		Command::new("pgrep")
			.args(["-f", &s1.id])
			.output()
			.is_ok_and(|o| !o.stdout.is_empty())
	};
	assert!(vmm_alive(), "a SIGKILLed node leaves its VMMs running");
	c.spawn(&builder);
	let adopted = c
		.wait_for("restarted node answers", Duration::from_secs(30), || async {
			c.node_inventory(&builder).await.is_some()
		})
		.await;
	let cleaned = c
		.wait_for("orphaned VMM killed after node restart", Duration::from_mins(1), || async {
			c.node_inventory(&builder)
				.await
				.is_some_and(|l| !l.contains(&s1.id))
				&& !vmm_alive()
		})
		.await;
	eprintln!(
		"[node-restart] {builder} up after {adopted:?}; orphan VMM of {} killed {cleaned:?} later",
		s1.id
	);

	// Graceful node restart: gossip `Left`, VMMs keep running and are
	// adopted; the reconciler must not kill them.
	assert_eq!(
		c.exec("d3", &s2, "echo before > /tmp/state; echo ok")
			.await
			.0,
		"ok\n"
	);
	let status = c.get_mut(other).signal(libc::SIGTERM);
	assert!(status.success(), "graceful node shutdown exits 0: {status}");
	tokio::time::sleep(Duration::from_secs(15)).await;
	c.spawn(other);
	let other_id = c.get(other).node_id();
	c.wait_for("restarted node alive", Duration::from_secs(30), || async {
		let (_, nodes) = c.api("d1", reqwest::Method::GET, "/nodes", None).await;
		nodes.as_array().is_some_and(|n| {
			n.iter()
				.any(|n| n["id"] == other_id.as_str() && n["status"] == "ready")
		})
	})
	.await;
	tokio::time::sleep(Duration::from_secs(12)).await;
	for s in [&s2, &s3] {
		assert_eq!(
			c.exec("d1", s, "cat /tmp/state 2>/dev/null; echo alive")
				.await
				.1,
			0,
			"{} survived",
			s.id
		);
	}
	assert_eq!(c.exec("d2", &s2, "cat /tmp/state").await.0, "before\n");
	eprintln!(
		"[graceful-restart] SIGTERM {other} for 15 s: {} and {} adopted and still running",
		s2.id, s3.id
	);

	for s in [&s2, &s3] {
		let (status, _) = c
			.api("d1", reqwest::Method::DELETE, &format!("/sandboxes/{}", s.id), None)
			.await;
		assert_eq!(status, 204);
	}
	let mut by_node: HashMap<String, Option<Vec<String>>> = HashMap::new();
	for n in ["n1", "n2"] {
		by_node.insert(n.into(), c.node_inventory(n).await);
	}
	eprintln!("[cleanup] node inventories after kill: {by_node:?}");
	cleanup(c);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_pause_survives_holder_loss_and_removes_remote_state_on_kill() {
	use vmon_node::paused_store::{Remote, Store};

	if !enabled() {
		eprintln!("skipping: set VMON_CLUSTER_E2E=1 on a host with KVM/HVF");
		return;
	}
	let Ok(url) = std::env::var("VMON_CLUSTER_OBJECT_STORE") else {
		eprintln!("skipping: set VMON_CLUSTER_OBJECT_STORE to a dedicated writable S3 URL");
		return;
	};
	assert!(!url.trim().is_empty(), "VMON_CLUSTER_OBJECT_STORE must be a writable S3 URL");
	let store = Store::new(
		vmon_cloud::s3::S3Client::from_env(&url)
			.await
			.expect("dedicated object store configured"),
	);
	let mut c = topology("_pause", Some(url)).await;
	build_template(&c).await;

	let sandbox = c.create("d1", 600).await;
	let holder = c.node_of("d1", &sandbox).await;
	let other = if holder == "n1" { "n2" } else { "n1" };
	let holder_id = c.get(&holder).node_id();
	let file = "written before portable pause\n";
	let resp = c
		.envd("d2", &sandbox, reqwest::Method::POST, "/files?path=/tmp/portable-pause.txt")
		.header("content-type", "application/octet-stream")
		.body(file)
		.send()
		.await
		.unwrap();
	assert!(resp.status().is_success(), "write file: {}", resp.status());

	// A real, detached guest process keeps a counter in shell memory. Neither
	// the command nor its counter is part of template initialization: a cold
	// boot cannot satisfy the process identity and post-resume progress checks.
	let identity = c.start_counter("d3", &sandbox, "portable-pause").await;

	let (status, body) = c
		.api(
			"d1",
			reqwest::Method::POST,
			&format!("/sandboxes/{}/pause", sandbox.id),
			Some(json!({"memory": true})),
		)
		.await;
	assert_eq!(status, 204, "pause: {body}");
	let remote_key = vmon_node::paused_store::key(&sandbox.id);
	c.wait_for("portable paused generation published", Duration::from_mins(10), || async {
		for via in ["d1", "d2", "d3"] {
			if !c
				.kv(via, &remote_key, true)
				.await
				.is_ok_and(|r| r.is_some())
			{
				return false;
			}
		}
		true
	})
	.await;
	let remote: Remote = serde_json::from_value(
		c.kv("d1", &remote_key, true)
			.await
			.unwrap()
			.expect("remote generation"),
	)
	.unwrap();
	assert_eq!(remote.arch, arch());
	assert!(remote.bytes > 0, "remote contains packed extent data");
	let manifest = store
		.manifest(&remote)
		.await
		.expect("published manifest readable");
	assert!(manifest.has_snapshot, "memory pause must include a restorable snapshot");
	assert!(
		manifest
			.files
			.iter()
			.any(|f| f.path.starts_with("snapshot/")),
		"snapshot objects are in the remote generation"
	);
	let generation = store.generation_prefix(&remote).unwrap();
	assert!(
		generation.starts_with(&format!("paused/{}/", sandbox.id)),
		"remote objects are scoped to this sandbox"
	);
	let mut objects: Vec<String> = manifest
		.files
		.iter()
		.map(|f| format!("{generation}/{}", f.path))
		.collect();
	objects.push(remote.manifest.clone());
	for key in &objects {
		assert!(
			store
				.s3()
				.exists(key)
				.await
				.expect("inspect published object"),
			"{key} published"
		);
	}
	let (status, detail) = c
		.api("d2", reqwest::Method::GET, &format!("/sandboxes/{}", sandbox.id), None)
		.await;
	assert_eq!(status, 200, "{detail}");
	assert_eq!(detail["state"], "paused");
	assert!(c.route("d3", &sandbox.id, true).await.unwrap().is_none());

	let _ = c.get_mut(&holder).signal(libc::SIGKILL);
	c.wait_for("holder gossip dead on every driver", Duration::from_mins(1), || async {
		for via in ["d1", "d2", "d3"] {
			let (status, nodes) = c.api(via, reqwest::Method::GET, "/nodes", None).await;
			if status != 200
				|| !nodes.as_array().is_some_and(|nodes| {
					nodes
						.iter()
						.any(|n| n["id"] == holder_id && n["status"] == "unhealthy")
				}) {
				return false;
			}
		}
		true
	})
	.await;
	assert!(c.killed_event("d1", &sandbox.id).await.is_none(), "paused sandbox survives node loss");
	assert!(c.kv("d2", &remote_key, true).await.unwrap().is_some());

	let (status, connected) = c
		.api(
			"d3",
			reqwest::Method::POST,
			&format!("/v2/sandboxes/{}/connect", sandbox.id),
			Some(json!({"timeout": 600})),
		)
		.await;
	assert_eq!(status, 201, "off-owner connect: {connected}");
	assert_eq!(connected["sandboxID"], sandbox.id);
	let resumed = Sandbox {
		id:    sandbox.id.clone(),
		token: connected["envdAccessToken"].as_str().unwrap().to_owned(),
	};
	assert_eq!(c.node_of("d2", &resumed).await, other, "resume lands on the surviving node");
	assert_eq!(
		c.counter_identity("d1", &resumed, "portable-pause").await,
		(identity, 0),
		"same running guest process survives memory restore"
	);
	let got = c
		.envd("d2", &resumed, reqwest::Method::GET, "/files?path=/tmp/portable-pause.txt")
		.send()
		.await
		.unwrap();
	assert!(got.status().is_success(), "read restored file: {}", got.status());
	assert_eq!(got.text().await.unwrap(), file);
	c.counter_advances("d2", &resumed, "portable-pause").await;

	let (status, body) = c
		.api("d1", reqwest::Method::DELETE, &format!("/sandboxes/{}", resumed.id), None)
		.await;
	assert_eq!(status, 204, "kill resumed sandbox: {body}");
	c.wait_for(
		"kill removes remote KV and every generation object",
		Duration::from_mins(1),
		|| async {
			for via in ["d1", "d2", "d3"] {
				if !c
					.kv(via, &remote_key, true)
					.await
					.is_ok_and(|r| r.is_none())
				{
					return false;
				}
			}
			for key in &objects {
				if store
					.s3()
					.exists(key)
					.await
					.expect("inspect object deletion")
				{
					return false;
				}
			}
			true
		},
	)
	.await;
	assert!(c.route("d3", &resumed.id, true).await.unwrap().is_none());
	c.wait_for("resumed sandbox removed from node", Duration::from_secs(30), || async {
		c.node_inventory(other)
			.await
			.is_some_and(|ids| !ids.contains(&resumed.id))
	})
	.await;
	cleanup(c);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handover_moves_sandboxes_and_draining_evacuates_a_node() {
	if !enabled() {
		eprintln!("skipping: set VMON_CLUSTER_E2E=1 on a host with KVM/HVF");
		return;
	}
	let c = topology("_handover", None).await;
	build_template(&c).await;

	// Explicit migration of a running sandbox: files, processes and their
	// memory survive; the source keeps nothing.
	let s = c.create("d1", 600).await;
	let first = c.node_of("d1", &s).await;
	let second = if first == "n1" { "n2" } else { "n1" };
	let file = "written before the handover\n";
	let resp = c
		.envd("d2", &s, reqwest::Method::POST, "/files?path=/tmp/handover.txt")
		.header("content-type", "application/octet-stream")
		.body(file)
		.send()
		.await
		.unwrap();
	assert!(resp.status().is_success(), "write file: {}", resp.status());
	let identity = c.start_counter("d3", &s, "handover").await;
	let started = Instant::now();
	let (status, moved) = c
		.api("d2", reqwest::Method::POST, &format!("/admin/sandboxes/{}/migrate", s.id), None)
		.await;
	assert_eq!(status, 200, "migrate: {moved}");
	eprintln!("[handover] {} moved {first} -> {second} in {:?}", s.id, started.elapsed());
	assert_eq!(moved["nodeID"], c.get(second).node_id());
	assert_eq!(moved["state"], "running");
	assert_eq!(c.node_of("d3", &s).await, second);
	assert_eq!(
		c.counter_identity("d1", &s, "handover").await,
		(identity.clone(), 0),
		"the same guest process runs on the target"
	);
	let got = c
		.envd("d3", &s, reqwest::Method::GET, "/files?path=/tmp/handover.txt")
		.send()
		.await
		.unwrap();
	assert_eq!(got.text().await.unwrap(), file);
	c.counter_advances("d2", &s, "handover").await;
	c.wait_for("source dropped its copy", Duration::from_secs(30), || async {
		c.node_inventory(&first)
			.await
			.is_some_and(|ids| !ids.contains(&s.id))
	})
	.await;

	// A node-local paused sandbox next to it on the same node.
	c.set_node_status(&first, "draining").await;
	let p = c.create("d1", 600).await;
	assert_eq!(c.node_of("d1", &p).await, second, "placement skips the draining node");
	c.set_node_status(&first, "ready").await;
	let resp = c
		.envd("d1", &p, reqwest::Method::POST, "/files?path=/tmp/paused.txt")
		.header("content-type", "application/octet-stream")
		.body("paused on the drained node")
		.send()
		.await
		.unwrap();
	assert!(resp.status().is_success(), "write file: {}", resp.status());
	let (status, body) = c
		.api("d2", reqwest::Method::POST, &format!("/sandboxes/{}/pause", p.id), None)
		.await;
	assert_eq!(status, 204, "pause: {body}");

	// Draining the node evacuates both.
	let first_id = c.get(&first).node_id();
	let started = Instant::now();
	c.set_node_status(second, "draining").await;
	c.wait_for("draining node evacuated", Duration::from_mins(5), || async {
		let running = c
			.route("d2", &s.id, true)
			.await
			.ok()
			.flatten()
			.is_some_and(|r| r["nodeId"] == first_id.as_str() && r["state"] == "running");
		let paused = c
			.listed("d3", &p.id)
			.await
			.is_some_and(|l| l["nodeID"] == first_id.as_str() && l["state"] == "paused");
		running
			&& paused
			&& c
				.node_inventory(second)
				.await
				.is_some_and(|ids| ids.is_empty())
	})
	.await;
	eprintln!("[evacuation] {second} drained to {first} in {:?}", started.elapsed());
	assert_eq!(c.counter_identity("d3", &s, "handover").await, (identity, 0));
	c.counter_advances("d1", &s, "handover").await;
	let (status, connected) = c
		.api(
			"d1",
			reqwest::Method::POST,
			&format!("/v2/sandboxes/{}/connect", p.id),
			Some(json!({"timeout": 600})),
		)
		.await;
	assert_eq!(status, 201, "resume moved paused sandbox: {connected}");
	assert_eq!(c.node_of("d2", &p).await, first, "it resumes where its state moved");
	assert_eq!(
		c.exec("d3", &p, "cat /tmp/paused.txt").await,
		("paused on the drained node".to_owned(), 0)
	);
	c.set_node_status(second, "ready").await;

	for id in [&s.id, &p.id] {
		let (status, _) = c
			.api("d1", reqwest::Method::DELETE, &format!("/sandboxes/{id}"), None)
			.await;
		assert_eq!(status, 204);
	}
	cleanup(c);
}
