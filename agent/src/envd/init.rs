//! `POST /init`: authorize (current token, first-time setup, or a
//! loopback root peer = the node's GC4 tunnel, our stand-in for MMDS), step
//! the clock, thaw a pre-pause freeze, and apply env vars, token, default
//! user/workdir, sandbox identity, CA bundle, volume mounts, and CPU target
//! (newest timestamp wins).

use std::{
	collections::BTreeMap, ffi::CString, io, net::SocketAddr, os::unix::ffi::OsStrExt, path::Path,
	sync::Arc, time::Instant,
};

use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;

use super::{
	ENVD_VERSION, auth,
	http::{Resp, no_content, text_error},
	state::{EnvdState, write_run_file},
	time,
};

/// System trust-store bundles a CA is appended to (those that exist).
const CA_BUNDLES: [&str; 4] = [
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/cert.pem",
	"/etc/ssl/ca-bundle.pem",
];

/// A volume to mount before workloads run. `tag` = `virtio-fs` (vmon);
/// `nfs_target` = `host:/export` `NFSv3` over TCP (upstream E2B).
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
pub struct VolumeMount {
	#[serde(default)]
	pub tag:        Option<String>,
	#[serde(default)]
	pub nfs_target: Option<String>,
	pub path:       String,
}

/// `/init` request body (upstream fields plus `sandboxId`/`templateId`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitRequest {
	pub volume_mounts:   Option<Vec<VolumeMount>>,
	#[serde(rename = "hyperloopIP")]
	pub hyperloop_ip:    Option<String>,
	pub env_vars:        Option<BTreeMap<String, String>>,
	pub access_token:    Option<String>,
	pub timestamp:       Option<String>,
	pub default_user:    Option<String>,
	pub default_workdir: Option<String>,
	pub ca_bundle:       Option<String>,
	pub cpu_count:       Option<u32>,
	pub thaw:            Option<String>,
	pub sandbox_id:      Option<String>,
	pub template_id:     Option<String>,
}

/// Who may call `/init`.
pub fn authorized(current: Option<&str>, headers: &HeaderMap, local_root_peer: bool) -> bool {
	match current {
		None => true,
		Some(token) => local_root_peer || auth::token_ok(headers, Some(token)),
	}
}

/// Handle `/init` with a fully read body.
pub async fn handle(
	state: &Arc<EnvdState>,
	headers: &HeaderMap,
	body: &[u8],
	peer: SocketAddr,
	local: SocketAddr,
) -> Resp {
	let received = Instant::now();
	let req: InitRequest = if body.iter().all(u8::is_ascii_whitespace) {
		InitRequest::default()
	} else {
		match serde_json::from_slice(body) {
			Ok(r) => r,
			Err(e) => return text_error(StatusCode::BAD_REQUEST, format!("invalid init body: {e}")),
		}
	};
	let skip_thaw = req.thaw.as_deref() == Some("skip");
	let _init = state.init_lock.lock().await;

	let current = state.access_token();
	let local_root = current.is_some() && auth::peer_is_local_root(peer, local);
	if !authorized(current.as_deref(), headers, local_root) {
		let mut resp = text_error(StatusCode::UNAUTHORIZED, "access token validation failed");
		if skip_thaw {
			resp
				.headers_mut()
				.insert("x-envd-thaw", HeaderValue::from_static("skip"));
		}
		return resp;
	}

	if !skip_thaw {
		let _freeze = state.freeze_lock.lock().await;
		if let Err(e) = state.cgroups.thaw() {
			crate::logging::emit(
				crate::logging::Level::Warn,
				"envd: init thaw failed",
				serde_json::json!({"error": e}),
			);
		}
	}

	let host_ns = match req.timestamp.as_deref() {
		Some(ts) => match time::parse_rfc3339(ts) {
			Some(ns) => Some(ns),
			None => return text_error(StatusCode::BAD_REQUEST, format!("invalid timestamp: {ts}")),
		},
		None => None,
	};
	let correction = received.elapsed();
	let stale = host_ns.is_some_and(|ns| {
		state
			.defaults
			.read()
			.last_init_ns
			.is_some_and(|last| ns <= last)
	});
	if !stale {
		if let Some(ns) = host_ns {
			let corrected = ns + i128::try_from(correction.as_nanos()).unwrap_or(0);
			if time::needs_step(time::now_ns(), corrected)
				&& let Err(e) = time::set_realtime(corrected)
			{
				crate::logging::emit(
					crate::logging::Level::Warn,
					"envd: clock step failed",
					serde_json::json!({"error": e.to_string()}),
				);
			}
		}
		if let Err(e) = apply(state, req, host_ns).await {
			return text_error(StatusCode::BAD_REQUEST, e);
		}
	}

	let mut resp = no_content();
	let h = resp.headers_mut();
	h.insert("x-envd-version", HeaderValue::from_static(ENVD_VERSION));
	h.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
	let defaults = {
		let d = state.defaults.read();
		serde_json::json!({ "user": d.user, "workdir": d.workdir, "fallback": d.user == "root" && d.workdir.is_none() })
	};
	if let Ok(v) = HeaderValue::from_str(&defaults.to_string()) {
		h.insert("x-envd-defaults", v);
	}
	// SAFETY: sysconf is always safe.
	let online = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
	h.insert("x-envd-cpus", HeaderValue::from(online));
	if let Some((total, ..)) = std::fs::read_to_string("/proc/meminfo")
		.ok()
		.and_then(|m| super::metrics::parse_meminfo(&m))
	{
		h.insert("x-envd-memory", HeaderValue::from(total));
	}
	if host_ns.is_some() {
		h.insert(
			"x-envd-init-clock-correction",
			HeaderValue::from(u64::try_from(correction.as_millis()).unwrap_or(u64::MAX)),
		);
	}
	if skip_thaw {
		h.insert("x-envd-thaw", HeaderValue::from_static("skip"));
	}
	resp
}

async fn apply(
	state: &Arc<EnvdState>,
	req: InitRequest,
	host_ns: Option<i128>,
) -> Result<(), String> {
	if let Some(mounts) = &req.volume_mounts {
		let mounts: Vec<(Option<String>, Option<String>, String)> = mounts
			.iter()
			.map(|m| (m.tag.clone(), m.nfs_target.clone(), m.path.clone()))
			.collect();
		tokio::task::spawn_blocking(move || {
			for (tag, nfs, path) in mounts {
				mount_volume(tag.as_deref(), nfs.as_deref(), &path)?;
			}
			Ok::<(), String>(())
		})
		.await
		.map_err(|e| e.to_string())??;
	}
	if let Some(bundle) = req.ca_bundle.as_deref().filter(|b| !b.trim().is_empty()) {
		install_ca(bundle).map_err(|e| format!("install CA bundle: {e}"))?;
	}
	if let Some(ip) = req.hyperloop_ip.as_deref().filter(|ip| !ip.is_empty()) {
		let _ = set_events_host(ip);
	}
	let mut d = state.defaults.write();
	if let Some(env) = req.env_vars {
		d.init_env = env;
	}
	d.access_token = req.access_token.filter(|t| !t.is_empty());
	if let Some(user) = req.default_user.filter(|u| !u.is_empty()) {
		d.user = user;
	}
	if let Some(workdir) = req.default_workdir.filter(|w| !w.is_empty()) {
		d.workdir = Some(workdir);
	}
	if let Some(id) = req.sandbox_id.filter(|s| !s.is_empty()) {
		write_run_file(".E2B_SANDBOX_ID", &id);
		d.sandbox_id = Some(id);
	}
	if let Some(id) = req.template_id.filter(|s| !s.is_empty()) {
		write_run_file(".E2B_TEMPLATE_ID", &id);
		d.template_id = Some(id);
	}
	if let Some(ip) = req.hyperloop_ip.filter(|ip| !ip.is_empty()) {
		d.events_address = Some(format!("http://{ip}"));
	}
	if host_ns.is_some() {
		d.last_init_ns = host_ns;
	}
	drop(d);
	if let Some(target) = req.cpu_count.filter(|&n| n > 0) {
		*state.cpu_target.lock() = (target, 0);
		let st = state.clone();
		tokio::task::spawn_blocking(move || {
			let attempts = hotplug_cpus(target);
			let mut t = st.cpu_target.lock();
			if t.0 == target {
				t.1 = attempts;
			}
		});
	}
	Ok(())
}

/// Bring CPUs `0..target` online and the rest offline (best effort);
/// returns the number of sysfs writes attempted.
fn hotplug_cpus(target: u32) -> u32 {
	let mut attempts = 0;
	let Ok(entries) = std::fs::read_dir("/sys/devices/system/cpu") else {
		return 0;
	};
	for e in entries.filter_map(Result::ok) {
		let name = e.file_name();
		let Some(n) = name
			.to_str()
			.and_then(|s| s.strip_prefix("cpu"))
			.and_then(|s| s.parse::<u32>().ok())
		else {
			continue;
		};
		let online = e.path().join("online");
		if n == 0 || !online.exists() {
			continue;
		}
		let want = if n < target { "1" } else { "0" };
		if std::fs::read_to_string(&online).is_ok_and(|v| v.trim() == want) {
			continue;
		}
		attempts += 1;
		let _ = std::fs::write(&online, want);
	}
	attempts
}

/// Append `bundle` to every existing trust-store file (creating the Debian
/// path when none exists); idempotent.
fn install_ca(bundle: &str) -> io::Result<()> {
	let mut pem = bundle.trim().to_owned();
	pem.push('\n');
	let existing: Vec<&str> = CA_BUNDLES
		.iter()
		.copied()
		.filter(|p| Path::new(p).exists())
		.collect();
	let targets = if existing.is_empty() {
		vec![CA_BUNDLES[0]]
	} else {
		existing
	};
	for target in targets {
		let path = Path::new(target);
		let current = std::fs::read_to_string(path).unwrap_or_default();
		if current.contains(pem.trim()) {
			continue;
		}
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent)?;
		}
		let mut next = current;
		if !next.is_empty() && !next.ends_with('\n') {
			next.push('\n');
		}
		next.push_str(&pem);
		// Resolve symlinked bundles (e.g. /etc/ssl/cert.pem → certs/...).
		let real = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
		std::fs::write(real, next)?;
	}
	Ok(())
}

/// Point `events.e2b.local` at `ip` in `/etc/hosts`.
fn set_events_host(ip: &str) -> io::Result<()> {
	let hosts = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
	let mut out = String::with_capacity(hosts.len() + 64);
	for line in hosts.lines().filter(|l| {
		!l.split_whitespace()
			.skip(1)
			.any(|h| h == "events.e2b.local")
	}) {
		out.push_str(line);
		out.push('\n');
	}
	out.push_str(ip);
	out.push_str(" events.e2b.local\n");
	std::fs::write("/etc/hosts", out)
}

fn is_mounted(path: &str) -> bool {
	std::fs::read_to_string("/proc/self/mountinfo")
		.is_ok_and(|m| m.lines().any(|l| l.split_whitespace().nth(4) == Some(path)))
}

fn mount_volume(tag: Option<&str>, nfs_target: Option<&str>, path: &str) -> Result<(), String> {
	if !path.starts_with('/') {
		return Err(format!("volume mount path must be absolute: {path}"));
	}
	if is_mounted(path) {
		return Ok(());
	}
	std::fs::create_dir_all(path).map_err(|e| format!("create {path}: {e}"))?;
	let (source, fstype, data) = match (tag, nfs_target) {
		(Some(tag), _) => (tag.to_owned(), "virtiofs", String::new()),
		(None, Some(target)) => {
			let host = target.split_once(':').map_or(target, |(h, _)| h);
			(
				target.to_owned(),
				"nfs",
				format!("vers=3,proto=tcp,nolock,mountproto=tcp,addr={host},mountaddr={host}"),
			)
		},
		(None, None) => {
			return Err(format!("volume mount for {path} has neither tag nor nfs_target"));
		},
	};
	let c = |s: &str| CString::new(s).map_err(|_| format!("invalid mount argument: {s:?}"));
	let (src, dst, ty, dat) = (
		c(&source)?,
		CString::new(Path::new(path).as_os_str().as_bytes()).map_err(|_| "bad path".to_owned())?,
		c(fstype)?,
		c(&data)?,
	);
	// SAFETY: all arguments are valid NUL-terminated strings.
	let rc = unsafe { libc::mount(src.as_ptr(), dst.as_ptr(), ty.as_ptr(), 0, dat.as_ptr().cast()) };
	if rc != 0 {
		return Err(format!("mount {source} ({fstype}) at {path}: {}", io::Error::last_os_error()));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn init_authorization_rules() {
		let mut h = HeaderMap::new();
		// First-time setup: anyone.
		assert!(authorized(None, &h, false));
		// Token set: need the same token...
		assert!(!authorized(Some("old"), &h, false));
		h.insert("x-access-token", "new".parse().unwrap());
		assert!(!authorized(Some("old"), &h, false));
		h.insert("x-access-token", "old".parse().unwrap());
		assert!(authorized(Some("old"), &h, false));
		// ...or the node's tunnel (loopback root peer) rotating it.
		assert!(authorized(Some("old"), &HeaderMap::new(), true));
	}

	#[test]
	fn body_parses_upstream_and_extension_fields() {
		let req: InitRequest = serde_json::from_str(
			r#"{"envVars":{"A":"1"},"accessToken":"t","timestamp":"2024-01-01T00:00:00Z",
			"defaultUser":"user","hyperloopIP":"192.0.2.1","lifecycleID":"l","cpuCount":2,
			"volumeMounts":[{"nfs_target":"192.0.2.1:/v","path":"/mnt/v"},{"tag":"vol1","path":"/data"}],
			"sandboxId":"sbx","templateId":"tpl","unknown":true}"#,
		)
		.unwrap();
		assert_eq!(req.env_vars.unwrap()["A"], "1");
		assert_eq!(req.hyperloop_ip.as_deref(), Some("192.0.2.1"));
		let mounts = req.volume_mounts.unwrap();
		assert_eq!(mounts[0].nfs_target.as_deref(), Some("192.0.2.1:/v"));
		assert_eq!(mounts[1].tag.as_deref(), Some("vol1"));
		assert_eq!(req.sandbox_id.as_deref(), Some("sbx"));
	}
}
