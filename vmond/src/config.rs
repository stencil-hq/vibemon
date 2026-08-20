//! `ServeConfig`: TOML file / env / CLI precedence. Port of
//! python/vmon/config.py.

use std::{
	collections::{BTreeSet, HashMap},
	env, fs,
	net::IpAddr,
	path::PathBuf,
};

use crate::{EngineError, Result};

/// Configuration sources in increasing precedence order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigSource {
	/// Built-in dataclass default.
	Default,
	/// TOML configuration file.
	File,
	/// `VMON_*` environment variable.
	Env,
	/// Explicit CLI flag.
	Flag,
}

impl ConfigSource {
	/// Return the Python `_sources` spelling.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Default => "default",
			Self::File => "file",
			Self::Env => "env",
			Self::Flag => "flag",
		}
	}
}

/// One image reference to prewarm and its requested pool size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmImage {
	/// Image, template, or Dockerfile reference.
	pub reference: String,
	/// Requested ready-instance count.
	pub count:     usize,
}

/// Persistence substrate selected for mesh metadata and portable artifacts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClusterMode {
	/// One process owns all state and may use `SQLite` and the local content
	/// store.
	#[default]
	SingleNode,
	/// Multiple processes share `PostgreSQL` metadata and S3-compatible objects.
	Production,
}

impl ClusterMode {
	fn parse(value: &str) -> Result<Self> {
		match value.trim().to_ascii_lowercase().as_str() {
			"single-node" | "single_node" | "local" => Ok(Self::SingleNode),
			"production" | "cluster" => Ok(Self::Production),
			_ => Err(EngineError::invalid("cluster_mode must be single-node or production")),
		}
	}
}

/// Resolved configuration consumed by `vmon serve` and doctor checks.
#[derive(Clone, Debug, PartialEq)]
pub struct ServeConfig {
	/// Vibemon home directory.
	pub home: PathBuf,
	/// TCP bind host.
	pub host: String,
	/// TCP bind port.
	pub port: u16,
	/// Admin bearer token.
	pub token: Option<String>,
	/// Restricted client bearer token.
	pub client_token: Option<String>,
	/// JSON map from tenant bearer tokens to stable tenant IDs.
	pub tenant_tokens: HashMap<String, String>,
	/// JSON map from tenant IDs to customer-managed key IDs.
	pub tenant_keys: HashMap<String, String>,
	/// Maximum accepted size of one streamed function artifact.
	pub function_artifact_max_bytes: u64,
	/// TLS certificate path.
	pub tls_cert: Option<String>,
	/// TLS private-key path.
	pub tls_key: Option<String>,
	/// Unix socket for the externally managed privileged network broker.
	pub network_broker_socket: Option<PathBuf>,
	/// Preallocated network slot pool size; zero disables pooling.
	pub net_slots: usize,
	/// Maximum concurrently booting sandboxes; zero sizes the gate from the
	/// host CPU count. Admission is unaffected — queued boots wait here.
	pub boot_concurrency: usize,
	/// Idle sandbox timeout in seconds.
	pub idle_timeout: f64,
	/// Stored sandbox-state quota in MiB; zero disables eviction.
	pub storage_quota_mb: u64,
	/// Live disk-history capture cadence in seconds; zero disables it.
	pub history_disk_sec: f64,
	/// Full checkpoint-history cadence in seconds; zero disables it.
	pub history_checkpoint_sec: f64,
	/// Maximum retained recovery points per sandbox.
	pub history_retention: usize,
	/// Maximum recovery-point age in seconds; zero disables age pruning.
	pub history_max_age_sec: f64,
	/// Maximum age for unused image templates in seconds.
	pub template_ttl_sec: f64,
	/// Replica checkpoint cadence override.
	pub replicate_sec: Option<f64>,
	/// Desired async replica count.
	pub replicas: usize,
	/// Concurrent replica transfer limit.
	pub replicate_concurrency: usize,
	/// Restore-quorum override.
	pub restore_quorum: Option<bool>,
	/// Default warm-pool size.
	pub warm_pool_size: usize,
	/// Images to prewarm at startup.
	pub warm_images: Vec<WarmImage>,
	/// Mesh heartbeat cadence in seconds.
	pub mesh_heartbeat_sec: f64,
	/// Mesh reap age in seconds.
	pub mesh_reap_sec: f64,
	/// Mesh idempotency cache TTL in seconds.
	pub mesh_idem_ttl_sec: f64,
	/// Mesh create dispatch timeout in seconds.
	pub mesh_create_timeout_sec: f64,
	/// Placement weight for warm capacity.
	pub mesh_w_warm: f64,
	/// Placement weight for free capacity.
	pub mesh_w_free: f64,
	/// Placement weight for local node bias.
	pub mesh_w_local: f64,
	/// Placement weight for region affinity.
	pub mesh_w_region: f64,
	/// Placement penalty for inflight creates.
	pub mesh_w_inflight: f64,
	/// Orchestration Redis URL; enables the v2 worker publisher when set.
	pub orch_redis: Option<String>,
	/// Advertised worker base URL for the orchestration layer.
	pub orch_url: Option<String>,
	/// Host IP that per-sandbox TCP tunnels bind; default loopback. Non-loopback
	/// binds require each sandbox to set `inbound_cidr_allowlist`.
	pub tunnel_bind: Option<String>,
	/// Stable orchestration worker id override.
	pub orch_id: Option<String>,
	/// Orchestration heartbeat cadence in seconds.
	pub orch_heartbeat_sec: f64,
	/// Age in seconds after which an unrefreshed worker key expires.
	pub orch_dead_after_sec: f64,
	/// Optional operator safety ceiling; zero lets measured memory govern
	/// admission.
	pub orch_max_sandboxes: u64,
	/// Available memory reserved so CoW divergence cannot starve host services,
	/// in MiB. Zero disables the reserve.
	pub orch_memory_reserve_mib: u64,
	/// Cluster substrate mode.
	pub cluster_mode: ClusterMode,
	/// `PostgreSQL` connection URL.
	pub postgres_url: Option<String>,
	/// S3-compatible storage endpoint URL.
	pub s3_endpoint: Option<String>,
	/// S3-compatible storage bucket.
	pub s3_bucket: Option<String>,
	/// S3-compatible storage region.
	pub s3_region: Option<String>,
	/// S3-compatible storage access key.
	pub s3_access_key: Option<String>,
	/// S3-compatible storage secret key.
	pub s3_secret_key: Option<String>,
	/// S3-compatible key prefix.
	pub s3_prefix: Option<String>,
	/// Age threshold in seconds for stale multipart uploads; zero disables the
	/// janitor.
	pub s3_multipart_stale_sec: f64,
	/// Shared key ID provisioned with identical material on every production
	/// node for portable recovery archives.
	pub portable_history_key_id: Option<String>,
	sources: HashMap<String, ConfigSource>,
}

impl ServeConfig {
	/// Return the source that supplied a field's final value.
	pub fn source(&self, key: &str) -> Option<ConfigSource> {
		if !SERVE_CONFIG_KEYS.contains(&key) {
			return None;
		}
		Some(
			self
				.sources
				.get(key)
				.copied()
				.unwrap_or(ConfigSource::Default),
		)
	}

	/// Return the actual checkpoint cadence for the current mesh state.
	pub fn effective_replicate_sec(&self, mesh_enabled: bool) -> f64 {
		self
			.replicate_sec
			.unwrap_or(if mesh_enabled { 60.0 } else { 0.0 })
	}

	/// Return whether automatic restore should require majority confirmation.
	pub fn restore_quorum_enabled(&self, expected_members: usize) -> bool {
		self.restore_quorum.unwrap_or(expected_members >= 3)
	}

	/// Return the default per-sandbox HA tier for a create request.
	pub const fn default_ha(&self, mesh_enabled: bool) -> &'static str {
		if mesh_enabled { "async" } else { "off" }
	}

	/// Return placement scoring weights in mesh names.
	pub fn placement_weights(&self) -> HashMap<&'static str, f64> {
		HashMap::from([
			("warm", self.mesh_w_warm),
			("free", self.mesh_w_free),
			("local", self.mesh_w_local),
			("region", self.mesh_w_region),
			("inflight", self.mesh_w_inflight),
		])
	}

	fn set_source(&mut self, key: &str, source: ConfigSource) {
		self.sources.insert(key.to_owned(), source);
	}
}

impl Default for ServeConfig {
	fn default() -> Self {
		let sources = SERVE_CONFIG_KEYS
			.iter()
			.map(|key| ((*key).to_owned(), ConfigSource::Default))
			.collect();
		Self {
			home: default_home(),
			host: "127.0.0.1".to_owned(),
			port: 8000,
			token: None,
			client_token: None,
			tenant_tokens: HashMap::new(),
			tenant_keys: HashMap::new(),
			function_artifact_max_bytes: 4 * 1024 * 1024 * 1024,
			tls_cert: None,
			tls_key: None,
			network_broker_socket: None,
			net_slots: 0,
			boot_concurrency: 0,
			idle_timeout: 300.0,
			storage_quota_mb: 0,
			history_disk_sec: 300.0,
			history_checkpoint_sec: 3600.0,
			history_retention: 24,
			history_max_age_sec: 7.0 * 24.0 * 3600.0,
			template_ttl_sec: 30.0 * 24.0 * 3600.0,
			replicate_sec: None,
			replicas: 1,
			replicate_concurrency: 2,
			restore_quorum: None,
			warm_pool_size: 1,
			warm_images: Vec::new(),
			mesh_heartbeat_sec: 3.0,
			mesh_reap_sec: 300.0,
			mesh_idem_ttl_sec: 900.0,
			mesh_create_timeout_sec: 120.0,
			mesh_w_warm: 1000.0,
			mesh_w_free: 100.0,
			mesh_w_local: 50.0,
			mesh_w_region: 30.0,
			mesh_w_inflight: 80.0,
			orch_redis: None,
			orch_url: None,
			tunnel_bind: None,
			orch_id: None,
			orch_heartbeat_sec: 1.0,
			orch_dead_after_sec: 5.0,
			orch_max_sandboxes: 0,
			orch_memory_reserve_mib: 0,
			cluster_mode: ClusterMode::default(),
			postgres_url: None,
			s3_endpoint: None,
			s3_bucket: None,
			s3_region: None,
			s3_access_key: None,
			s3_secret_key: None,
			s3_prefix: None,
			s3_multipart_stale_sec: 7.0 * 24.0 * 3600.0,
			portable_history_key_id: None,
			sources,
		}
	}
}

/// Field names in Python dataclass order.
pub const SERVE_CONFIG_KEYS: &[&str] = &[
	"home",
	"host",
	"port",
	"token",
	"client_token",
	"tenant_tokens",
	"tenant_keys",
	"function_artifact_max_bytes",
	"tls_cert",
	"tls_key",
	"network_broker_socket",
	"net_slots",
	"boot_concurrency",
	"idle_timeout",
	"storage_quota_mb",
	"history_disk_sec",
	"history_checkpoint_sec",
	"history_retention",
	"history_max_age_sec",
	"template_ttl_sec",
	"replicate_sec",
	"replicas",
	"replicate_concurrency",
	"restore_quorum",
	"warm_pool_size",
	"warm_images",
	"mesh_heartbeat_sec",
	"mesh_reap_sec",
	"mesh_idem_ttl_sec",
	"mesh_create_timeout_sec",
	"mesh_w_warm",
	"mesh_w_free",
	"mesh_w_local",
	"mesh_w_region",
	"mesh_w_inflight",
	"orch_redis",
	"orch_url",
	"tunnel_bind",
	"orch_id",
	"orch_heartbeat_sec",
	"orch_dead_after_sec",
	"orch_max_sandboxes",
	"orch_memory_reserve_mib",
	"cluster_mode",
	"postgres_url",
	"s3_endpoint",
	"s3_bucket",
	"s3_region",
	"s3_access_key",
	"s3_secret_key",
	"s3_prefix",
	"s3_multipart_stale_sec",
	"portable_history_key_id",
];

/// Environment variable names for serve config fields.
pub const ENV_KEYS: &[(&str, &str)] = &[
	("home", "VMON_HOME"),
	("host", "VMON_SERVE_HOST"),
	("port", "VMON_SERVE_PORT"),
	("token", "VMON_API_TOKEN"),
	("client_token", "VMON_CLIENT_TOKEN"),
	("tenant_tokens", "VMON_TENANT_TOKENS"),
	("tenant_keys", "VMON_TENANT_KEYS"),
	("function_artifact_max_bytes", "VMON_FUNCTION_ARTIFACT_MAX_BYTES"),
	("tls_cert", "VMON_TLS_CERT"),
	("tls_key", "VMON_TLS_KEY"),
	("network_broker_socket", "VMON_NETWORK_BROKER_SOCKET"),
	("net_slots", "VMON_NET_SLOTS"),
	("boot_concurrency", "VMON_BOOT_CONCURRENCY"),
	("idle_timeout", "VMON_IDLE_TIMEOUT"),
	("storage_quota_mb", "VMON_STORAGE_QUOTA_MB"),
	("history_disk_sec", "VMON_HISTORY_DISK_SEC"),
	("history_checkpoint_sec", "VMON_HISTORY_CHECKPOINT_SEC"),
	("history_retention", "VMON_HISTORY_RETENTION"),
	("history_max_age_sec", "VMON_HISTORY_MAX_AGE_SEC"),
	("template_ttl_sec", "VMON_TEMPLATE_TTL_SEC"),
	("replicate_sec", "VMON_REPLICATE_SEC"),
	("replicas", "VMON_REPLICAS"),
	("replicate_concurrency", "VMON_REPLICATE_CONCURRENCY"),
	("restore_quorum", "VMON_RESTORE_QUORUM"),
	("warm_pool_size", "VMON_WARM_POOL_SIZE"),
	("warm_images", "VMON_WARM_IMAGES"),
	("mesh_heartbeat_sec", "VMON_MESH_HEARTBEAT_SEC"),
	("mesh_reap_sec", "VMON_MESH_REAP_SEC"),
	("mesh_idem_ttl_sec", "VMON_MESH_IDEM_TTL_SEC"),
	("mesh_create_timeout_sec", "VMON_MESH_CREATE_TIMEOUT_SEC"),
	("mesh_w_warm", "VMON_MESH_W_WARM"),
	("mesh_w_free", "VMON_MESH_W_FREE"),
	("mesh_w_local", "VMON_MESH_W_LOCAL"),
	("mesh_w_region", "VMON_MESH_W_REGION"),
	("mesh_w_inflight", "VMON_MESH_W_INFLIGHT"),
	("orch_redis", "VMON_ORCH_REDIS"),
	("orch_url", "VMON_ORCH_URL"),
	("tunnel_bind", "VMON_TUNNEL_BIND"),
	("orch_id", "VMON_ORCH_ID"),
	("orch_heartbeat_sec", "VMON_ORCH_HEARTBEAT_SEC"),
	("orch_dead_after_sec", "VMON_ORCH_DEAD_AFTER_SEC"),
	("orch_max_sandboxes", "VMON_ORCH_MAX_SANDBOXES"),
	("orch_memory_reserve_mib", "VMON_ORCH_MEMORY_RESERVE_MIB"),
	("cluster_mode", "VMON_CLUSTER_MODE"),
	("postgres_url", "VMON_POSTGRES_URL"),
	("s3_endpoint", "VMON_S3_ENDPOINT"),
	("s3_bucket", "VMON_S3_BUCKET"),
	("s3_region", "VMON_S3_REGION"),
	("s3_access_key", "VMON_S3_ACCESS_KEY"),
	("s3_secret_key", "VMON_S3_SECRET_KEY"),
	("s3_prefix", "VMON_S3_PREFIX"),
	("s3_multipart_stale_sec", "VMON_S3_MULTIPART_STALE_SEC"),
	("portable_history_key_id", "VMON_PORTABLE_HISTORY_KEY_ID"),
];

/// CLI option spellings for serve config fields.
pub const CLI_OPTIONS: &[(&str, &str)] = &[
	("home", "--home"),
	("host", "--host"),
	("port", "--port"),
	("token", "--token"),
	("client_token", "--client-token"),
	("tenant_tokens", "--tenant-tokens"),
	("tenant_keys", "--tenant-keys"),
	("function_artifact_max_bytes", "--function-artifact-max-bytes"),
	("tls_cert", "--tls-cert"),
	("tls_key", "--tls-key"),
	("network_broker_socket", "--network-broker-socket"),
	("net_slots", "--net-slots"),
	("boot_concurrency", "--boot-concurrency"),
	("idle_timeout", "--idle-timeout"),
	("storage_quota_mb", "--storage-quota-mb"),
	("history_disk_sec", "--history-disk-sec"),
	("history_checkpoint_sec", "--history-checkpoint-sec"),
	("history_retention", "--history-retention"),
	("history_max_age_sec", "--history-max-age-sec"),
	("template_ttl_sec", "--template-ttl-sec"),
	("replicate_sec", "--replicate-sec"),
	("replicas", "--replicas"),
	("replicate_concurrency", "--replicate-concurrency"),
	("restore_quorum", "--restore-quorum/--no-restore-quorum"),
	("warm_pool_size", "--warm-pool-size"),
	("warm_images", "--warm-images"),
	("mesh_heartbeat_sec", "--mesh-heartbeat-sec"),
	("mesh_reap_sec", "--mesh-reap-sec"),
	("mesh_idem_ttl_sec", "--mesh-idem-ttl-sec"),
	("mesh_create_timeout_sec", "--mesh-create-timeout-sec"),
	("mesh_w_warm", "--mesh-w-warm"),
	("mesh_w_free", "--mesh-w-free"),
	("mesh_w_local", "--mesh-w-local"),
	("mesh_w_region", "--mesh-w-region"),
	("mesh_w_inflight", "--mesh-w-inflight"),
	("orch_redis", "--orch-redis"),
	("orch_url", "--orch-url"),
	("tunnel_bind", "--tunnel-bind"),
	("orch_id", "--orch-id"),
	("orch_heartbeat_sec", "--orch-heartbeat-sec"),
	("orch_dead_after_sec", "--orch-dead-after-sec"),
	("orch_max_sandboxes", "--orch-max-sandboxes"),
	("orch_memory_reserve_mib", "--orch-memory-reserve-mib"),
	("cluster_mode", "--cluster-mode"),
	("postgres_url", "--postgres-url"),
	("s3_endpoint", "--s3-endpoint"),
	("s3_bucket", "--s3-bucket"),
	("s3_region", "--s3-region"),
	("s3_access_key", "--s3-access-key"),
	("s3_secret_key", "--s3-secret-key"),
	("s3_prefix", "--s3-prefix"),
	("s3_multipart_stale_sec", "--s3-multipart-stale-sec"),
	("portable_history_key_id", "--portable-history-key-id"),
];

const CONFIG_PATH_KEYS: &[&str] = &["config", "config_path"];
const BOOL_TRUE: &[&str] = &["1", "true", "yes", "on"];
const BOOL_FALSE: &[&str] = &["0", "false", "no", "off"];

/// Resolve `vmon serve` configuration from defaults, file, env, then flags.
#[allow(
	clippy::implicit_hasher,
	reason = "The migration contract fixes this public entry point to HashMap<String, String>."
)]
pub fn resolve_serve_config(cli_overrides: &HashMap<String, String>) -> Result<ServeConfig> {
	let overrides = clean_cli_overrides(cli_overrides)?;
	let config_path = resolve_config_path(&overrides);
	let file_values = match config_path {
		Some(path) => read_config_file(&path)?,
		None => HashMap::new(),
	};
	let env_values = env_values()?;
	let mut config = ServeConfig::default();
	overlay_toml(&mut config, &file_values, ConfigSource::File)?;
	overlay_strings(&mut config, &env_values, ConfigSource::Env)?;
	overlay_strings(&mut config, &overrides, ConfigSource::Flag)?;
	if config.cluster_mode == ClusterMode::Production {
		if config
			.postgres_url
			.as_ref()
			.is_none_or(|s| s.trim().is_empty())
		{
			return Err(EngineError::invalid("production mode requires postgres_url"));
		}
		if config
			.s3_endpoint
			.as_ref()
			.is_none_or(|s| s.trim().is_empty())
		{
			return Err(EngineError::invalid("production mode requires s3_endpoint"));
		}
		if config
			.s3_bucket
			.as_ref()
			.is_none_or(|s| s.trim().is_empty())
		{
			return Err(EngineError::invalid("production mode requires s3_bucket"));
		}
		for required in ["s3_region", "s3_access_key", "s3_secret_key"] {
			let present = match required {
				"s3_region" => config.s3_region.as_deref(),
				"s3_access_key" => config.s3_access_key.as_deref(),
				"s3_secret_key" => config.s3_secret_key.as_deref(),
				_ => unreachable!("all production S3 requirements are enumerated above"),
			};
			if present.is_none_or(|value| value.trim().is_empty()) {
				return Err(EngineError::invalid(format!("production mode requires {required}")));
			}
		}
		if config
			.portable_history_key_id
			.as_deref()
			.is_none_or(|key_id| key_id.trim().is_empty() || key_id == "default")
		{
			return Err(EngineError::invalid(
				"production mode requires a non-default portable_history_key_id shared by every node",
			));
		}
	}
	Ok(config)
}

/// Return the environment variable name for a config field.
pub fn env_var_for(key: &str) -> Option<&'static str> {
	ENV_KEYS
		.iter()
		.find_map(|(field, env)| (*field == key).then_some(*env))
}

/// Return the CLI option spelling for a config field.
pub fn cli_option_for(key: &str) -> Option<&'static str> {
	CLI_OPTIONS
		.iter()
		.find_map(|(field, option)| (*field == key).then_some(*option))
}

/// Verify field metadata covers every serve config key.
pub fn validate_knob_inventory() -> Result<()> {
	let fields = SERVE_CONFIG_KEYS.iter().copied().collect::<BTreeSet<_>>();
	let env = ENV_KEYS
		.iter()
		.map(|(key, _)| *key)
		.collect::<BTreeSet<_>>();
	let cli = CLI_OPTIONS
		.iter()
		.map(|(key, _)| *key)
		.collect::<BTreeSet<_>>();
	let missing_env = fields.difference(&env).copied().collect::<Vec<_>>();
	let missing_cli = fields.difference(&cli).copied().collect::<Vec<_>>();
	if missing_env.is_empty() && missing_cli.is_empty() {
		return Ok(());
	}
	Err(EngineError::invalid(format!(
		"incomplete serve config metadata: missing env={missing_env:?}, cli={missing_cli:?}"
	)))
}

fn default_home() -> PathBuf {
	env::var_os("HOME")
		.map_or_else(|| PathBuf::from(".vmon"), |home| PathBuf::from(home).join(".vmon"))
}

fn clean_cli_overrides(overrides: &HashMap<String, String>) -> Result<HashMap<String, String>> {
	let unknown = overrides
		.keys()
		.filter(|key| {
			let key = key.as_str();
			!SERVE_CONFIG_KEYS.contains(&key) && !CONFIG_PATH_KEYS.contains(&key)
		})
		.cloned()
		.collect::<BTreeSet<_>>();
	if !unknown.is_empty() {
		let joined = unknown.into_iter().collect::<Vec<_>>().join(", ");
		return Err(EngineError::invalid(format!("unknown CLI override key(s): {joined}")));
	}
	let mut cleaned = HashMap::new();
	for (key, value) in overrides {
		if CONFIG_PATH_KEYS.contains(&key.as_str()) {
			if !value.is_empty() {
				cleaned.insert(key.clone(), value.clone());
			}
			continue;
		}
		if key == "warm_images" && value.is_empty() {
			continue;
		}
		cleaned.insert(key.clone(), value.clone());
	}
	Ok(cleaned)
}

fn resolve_config_path(overrides: &HashMap<String, String>) -> Option<PathBuf> {
	let raw = overrides
		.get("config")
		.or_else(|| overrides.get("config_path"))
		.cloned()
		.or_else(|| env::var("VMON_CONFIG").ok())?;
	let trimmed = raw.trim();
	if trimmed.is_empty() {
		None
	} else {
		Some(expand_user(trimmed))
	}
}

fn read_config_file(path: &PathBuf) -> Result<HashMap<String, toml::Value>> {
	let text = fs::read_to_string(path).map_err(|err| {
		EngineError::invalid(format!("could not read config file {}: {err}", path.display()))
	})?;
	let root = text.parse::<toml::Value>().map_err(|err| {
		EngineError::invalid(format!("could not parse config file {}: {err}", path.display()))
	})?;
	let table = root.as_table().ok_or_else(|| {
		EngineError::invalid(format!("config file {} must contain a TOML table", path.display()))
	})?;
	let data = if let Some(serve) = table.get("serve") {
		let extra = table
			.keys()
			.filter(|key| key.as_str() != "serve")
			.cloned()
			.collect::<BTreeSet<_>>();
		if !extra.is_empty() {
			let joined = extra.into_iter().collect::<Vec<_>>().join(", ");
			return Err(EngineError::invalid(format!("unknown top-level config key(s): {joined}")));
		}
		serve
			.as_table()
			.ok_or_else(|| EngineError::invalid("config key 'serve' must be a TOML table"))?
	} else {
		table
	};
	let unknown = data
		.keys()
		.filter(|key| !SERVE_CONFIG_KEYS.contains(&key.as_str()))
		.cloned()
		.collect::<BTreeSet<_>>();
	if !unknown.is_empty() {
		let keys = unknown.into_iter().collect::<Vec<_>>().join(", ");
		return Err(EngineError::invalid(format!("unknown serve config key(s): {keys}")));
	}
	Ok(data
		.iter()
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect())
}

fn env_values() -> Result<HashMap<String, String>> {
	let mut values = HashMap::new();
	for (key, env_key) in ENV_KEYS {
		match env::var(env_key) {
			Ok(value) => {
				values.insert((*key).to_owned(), value);
			},
			Err(env::VarError::NotPresent) => {},
			Err(env::VarError::NotUnicode(_)) => {
				return Err(EngineError::invalid(format!(
					"environment variable {env_key} must be UTF-8"
				)));
			},
		}
	}
	Ok(values)
}

fn overlay_toml(
	config: &mut ServeConfig,
	incoming: &HashMap<String, toml::Value>,
	source: ConfigSource,
) -> Result<()> {
	for key in SERVE_CONFIG_KEYS {
		if let Some(value) = incoming.get(*key) {
			apply_value(config, key, RawValue::Toml(value), source)?;
		}
	}
	Ok(())
}

fn overlay_strings(
	config: &mut ServeConfig,
	incoming: &HashMap<String, String>,
	source: ConfigSource,
) -> Result<()> {
	for key in SERVE_CONFIG_KEYS {
		if let Some(value) = incoming.get(*key) {
			apply_value(config, key, RawValue::String(value), source)?;
		}
	}
	Ok(())
}

fn apply_value(
	config: &mut ServeConfig,
	key: &str,
	value: RawValue<'_>,
	source: ConfigSource,
) -> Result<()> {
	match key {
		"home" => config.home = expand_user(&coerce_string(key, value)?),
		"token" => config.token = empty_string_as_none(key, value)?,
		"client_token" => config.client_token = empty_string_as_none(key, value)?,
		"tenant_tokens" => config.tenant_tokens = coerce_string_map(key, value)?,
		"tenant_keys" => config.tenant_keys = coerce_string_map(key, value)?,
		"function_artifact_max_bytes" => {
			const MAX_ARTIFACT_BYTES: i64 = 1_i64 << 50;
			let parsed = coerce_int(key, value)?;
			if !(1..=MAX_ARTIFACT_BYTES).contains(&parsed) {
				return Err(EngineError::invalid(format!(
					"{key} must be between 1 and {MAX_ARTIFACT_BYTES}"
				)));
			}
			config.function_artifact_max_bytes = parsed as u64;
		},
		"tls_cert" => config.tls_cert = empty_string_as_none(key, value)?,
		"network_broker_socket" => {
			config.network_broker_socket =
				empty_string_as_none(key, value)?.map(|socket| expand_user(&socket));
		},
		"net_slots" => config.net_slots = non_negative_usize(key, value)?,
		"boot_concurrency" => config.boot_concurrency = non_negative_usize(key, value)?,
		"tls_key" => config.tls_key = empty_string_as_none(key, value)?,
		"host" => {
			let host = coerce_string(key, value)?.trim().to_owned();
			if host.is_empty() {
				return Err(EngineError::invalid("host must not be empty"));
			}
			config.host = host;
		},
		"port" => {
			let port = coerce_int(key, value)?;
			if !(0..=65535).contains(&port) {
				return Err(EngineError::invalid("port must be between 0 and 65535"));
			}
			config.port = u16::try_from(port)
				.map_err(|_| EngineError::invalid("port must be between 0 and 65535"))?;
		},
		"replicas" => config.replicas = non_negative_usize(key, value)?,
		"warm_pool_size" => config.warm_pool_size = non_negative_usize(key, value)?,
		"history_retention" => config.history_retention = non_negative_usize(key, value)?,
		"replicate_concurrency" => {
			let parsed = coerce_int(key, value)?;
			if parsed <= 0 {
				return Err(EngineError::invalid("replicate_concurrency must be positive"));
			}
			config.replicate_concurrency = usize::try_from(parsed)
				.map_err(|_| EngineError::invalid("replicate_concurrency must be positive"))?;
		},
		"restore_quorum" => config.restore_quorum = Some(coerce_bool(key, value)?),
		"warm_images" => {
			config.warm_images = coerce_warm_images(value, config.warm_pool_size)?;
		},
		"idle_timeout" => config.idle_timeout = positive_float(key, value)?,
		"storage_quota_mb" => {
			let parsed = coerce_int(key, value)?;
			config.storage_quota_mb = u64::try_from(parsed)
				.map_err(|_| EngineError::invalid("storage_quota_mb must be non-negative"))?;
		},
		"history_disk_sec" => config.history_disk_sec = non_negative_float(key, value)?,
		"history_checkpoint_sec" => {
			config.history_checkpoint_sec = non_negative_float(key, value)?;
		},
		"history_max_age_sec" => config.history_max_age_sec = non_negative_float(key, value)?,
		"template_ttl_sec" => config.template_ttl_sec = non_negative_float(key, value)?,
		"replicate_sec" => {
			let parsed = coerce_float(key, value)?;
			if parsed < 0.0 {
				return Err(EngineError::invalid("replicate_sec must be positive"));
			}
			config.replicate_sec = Some(parsed);
		},
		"mesh_heartbeat_sec" => config.mesh_heartbeat_sec = positive_float(key, value)?,
		"mesh_reap_sec" => config.mesh_reap_sec = positive_float(key, value)?,
		"mesh_idem_ttl_sec" => config.mesh_idem_ttl_sec = positive_float(key, value)?,
		"mesh_create_timeout_sec" => {
			config.mesh_create_timeout_sec = positive_float(key, value)?;
		},
		"mesh_w_warm" => config.mesh_w_warm = positive_float(key, value)?,
		"mesh_w_free" => config.mesh_w_free = positive_float(key, value)?,
		"mesh_w_local" => config.mesh_w_local = positive_float(key, value)?,
		"mesh_w_region" => config.mesh_w_region = positive_float(key, value)?,
		"mesh_w_inflight" => config.mesh_w_inflight = positive_float(key, value)?,
		"orch_redis" => config.orch_redis = empty_string_as_none(key, value)?,
		"orch_url" => config.orch_url = empty_string_as_none(key, value)?,
		"tunnel_bind" => {
			let tunnel_bind = empty_string_as_none(key, value)?;
			if let Some(value) = tunnel_bind.as_deref() {
				value
					.parse::<IpAddr>()
					.map_err(|_| EngineError::invalid("tunnel_bind must be a valid IP address"))?;
			}
			config.tunnel_bind = tunnel_bind;
		},
		"orch_id" => config.orch_id = empty_string_as_none(key, value)?,
		"orch_heartbeat_sec" => config.orch_heartbeat_sec = positive_float(key, value)?,
		"orch_dead_after_sec" => config.orch_dead_after_sec = positive_float(key, value)?,
		"orch_max_sandboxes" => {
			let parsed = coerce_int(key, value)?;
			if parsed < 0 {
				return Err(EngineError::invalid("orch_max_sandboxes must be non-negative"));
			}
			config.orch_max_sandboxes = parsed as u64;
		},
		"orch_memory_reserve_mib" => {
			let parsed = coerce_int(key, value)?;
			if parsed < 0 {
				return Err(EngineError::invalid("orch_memory_reserve_mib must be non-negative"));
			}
			config.orch_memory_reserve_mib = parsed as u64;
		},
		"cluster_mode" => {
			config.cluster_mode = ClusterMode::parse(&coerce_string(key, value)?)?;
		},
		"postgres_url" => {
			config.postgres_url = empty_string_as_none(key, value)?;
		},
		"s3_endpoint" => {
			config.s3_endpoint = empty_string_as_none(key, value)?;
		},
		"s3_bucket" => {
			config.s3_bucket = empty_string_as_none(key, value)?;
		},
		"s3_region" => {
			config.s3_region = empty_string_as_none(key, value)?;
		},
		"s3_access_key" => {
			config.s3_access_key = empty_string_as_none(key, value)?;
		},
		"s3_secret_key" => {
			config.s3_secret_key = empty_string_as_none(key, value)?;
		},
		"s3_prefix" => {
			config.s3_prefix = empty_string_as_none(key, value)?;
		},
		"s3_multipart_stale_sec" => {
			let parsed = non_negative_float(key, value)?;
			if parsed > 0.0 && parsed < 3600.0 {
				return Err(EngineError::invalid(format!(
					"{key} must be zero or at least 3600 seconds"
				)));
			}
			config.s3_multipart_stale_sec = parsed;
		},
		"portable_history_key_id" => {
			config.portable_history_key_id = empty_string_as_none(key, value)?;
		},
		_ => return Err(EngineError::invalid(format!("unsupported serve config key {key:?}"))),
	}
	config.set_source(key, source);
	Ok(())
}

#[derive(Clone, Copy)]
enum RawValue<'a> {
	String(&'a str),
	Toml(&'a toml::Value),
}

fn coerce_string(key: &str, value: RawValue<'_>) -> Result<String> {
	match value {
		RawValue::String(value) => Ok(value.to_owned()),
		RawValue::Toml(toml::Value::String(value)) => Ok(value.clone()),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a string"))),
	}
}

fn coerce_string_map(key: &str, value: RawValue<'_>) -> Result<HashMap<String, String>> {
	let map = match value {
		RawValue::String(text) => serde_json::from_str::<HashMap<String, String>>(text)
			.map_err(|_| EngineError::invalid(format!("{key} must be a JSON string map")))?,
		RawValue::Toml(toml::Value::String(text)) => {
			serde_json::from_str::<HashMap<String, String>>(text)
				.map_err(|_| EngineError::invalid(format!("{key} must be a JSON string map")))?
		},
		RawValue::Toml(toml::Value::Table(table)) => table
			.iter()
			.map(|(entry_key, entry_value)| match entry_value {
				toml::Value::String(value) => Ok((entry_key.clone(), value.clone())),
				_ => Err(EngineError::invalid(format!("{key} values must be strings"))),
			})
			.collect::<Result<HashMap<_, _>>>()?,
		RawValue::Toml(_) => {
			return Err(EngineError::invalid(format!("{key} must be a string map")));
		},
	};
	if map
		.iter()
		.any(|(entry_key, entry_value)| entry_key.is_empty() || entry_value.is_empty())
	{
		return Err(EngineError::invalid(format!("{key} entries must not be empty")));
	}
	Ok(map)
}

fn empty_string_as_none(key: &str, value: RawValue<'_>) -> Result<Option<String>> {
	let text = coerce_string(key, value)?.trim().to_owned();
	Ok((!text.is_empty()).then_some(text))
}

fn coerce_int(key: &str, value: RawValue<'_>) -> Result<i64> {
	match value {
		RawValue::String(value) => {
			let text = value.trim();
			if text.is_empty() {
				return Err(EngineError::invalid(format!("{key} must be an integer")));
			}
			text
				.parse::<i64>()
				.map_err(|_| EngineError::invalid(format!("{key} has invalid value {value:?}")))
		},
		RawValue::Toml(toml::Value::Integer(value)) => Ok(*value),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be an integer"))),
	}
}

fn coerce_float(key: &str, value: RawValue<'_>) -> Result<f64> {
	match value {
		RawValue::String(value) => {
			let text = value.trim();
			if text.is_empty() {
				return Err(EngineError::invalid(format!("{key} must be a number")));
			}
			text
				.parse::<f64>()
				.map_err(|_| EngineError::invalid(format!("{key} has invalid value {value:?}")))
		},
		RawValue::Toml(toml::Value::Integer(value)) => Ok(*value as f64),
		RawValue::Toml(toml::Value::Float(value)) => Ok(*value),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a number"))),
	}
}

fn coerce_bool(key: &str, value: RawValue<'_>) -> Result<bool> {
	match value {
		RawValue::String(value) => {
			let text = value.trim().to_ascii_lowercase();
			if BOOL_TRUE.contains(&text.as_str()) {
				Ok(true)
			} else if BOOL_FALSE.contains(&text.as_str()) {
				Ok(false)
			} else {
				Err(EngineError::invalid(format!("{key} must be a boolean")))
			}
		},
		RawValue::Toml(toml::Value::Boolean(value)) => Ok(*value),
		RawValue::Toml(toml::Value::Integer(0)) => Ok(false),
		RawValue::Toml(toml::Value::Integer(1)) => Ok(true),
		RawValue::Toml(_) => Err(EngineError::invalid(format!("{key} must be a boolean"))),
	}
}

fn non_negative_usize(key: &str, value: RawValue<'_>) -> Result<usize> {
	let parsed = coerce_int(key, value)?;
	if parsed < 0 {
		return Err(EngineError::invalid(format!("{key} must be non-negative")));
	}
	usize::try_from(parsed).map_err(|_| EngineError::invalid(format!("{key} must be non-negative")))
}

fn positive_float(key: &str, value: RawValue<'_>) -> Result<f64> {
	let parsed = coerce_float(key, value)?;
	if parsed <= 0.0 {
		return Err(EngineError::invalid(format!("{key} must be positive")));
	}
	Ok(parsed)
}

fn non_negative_float(key: &str, value: RawValue<'_>) -> Result<f64> {
	let parsed = coerce_float(key, value)?;
	if !parsed.is_finite() || parsed < 0.0 {
		return Err(EngineError::invalid(format!("{key} must be a finite non-negative number")));
	}
	Ok(parsed)
}

fn coerce_warm_images(value: RawValue<'_>, default_count: usize) -> Result<Vec<WarmImage>> {
	let entries = warm_image_entries(value)?;
	let mut parsed = Vec::new();
	for entry in entries {
		let item = entry.trim();
		if item.is_empty() {
			continue;
		}
		let (raw_ref, count) = split_warm_image_count(item, default_count)?;
		let reference = raw_ref.trim();
		if reference.is_empty() {
			return Err(EngineError::invalid("warm_images entries must include an image reference"));
		}
		parsed.push(WarmImage { reference: reference.to_owned(), count });
	}
	Ok(parsed)
}

fn warm_image_entries(value: RawValue<'_>) -> Result<Vec<String>> {
	match value {
		RawValue::String(value) => Ok(value
			.split(|ch: char| ch == ',' || ch.is_whitespace())
			.map(ToOwned::to_owned)
			.collect()),
		RawValue::Toml(toml::Value::String(value)) => Ok(value
			.split(|ch: char| ch == ',' || ch.is_whitespace())
			.map(ToOwned::to_owned)
			.collect()),
		RawValue::Toml(toml::Value::Array(values)) => values
			.iter()
			.map(|value| match value {
				toml::Value::String(value) => Ok(value.clone()),
				_ => Err(EngineError::invalid("warm_images entries must be strings")),
			})
			.collect(),
		RawValue::Toml(_) => Err(EngineError::invalid("warm_images must be a string or list")),
	}
}

fn split_warm_image_count(item: &str, default_count: usize) -> Result<(&str, usize)> {
	if let Some((reference, raw_count)) = item.rsplit_once('=') {
		return Ok((reference, warm_image_count(raw_count)?));
	}
	if let Some((reference, raw_count)) = item.rsplit_once(':') {
		let count = raw_count.trim();
		if looks_like_integer(count) {
			return Ok((reference, warm_image_count(count)?));
		}
	}
	Ok((item, default_count))
}

fn looks_like_integer(text: &str) -> bool {
	let digits = text
		.strip_prefix('-')
		.or_else(|| text.strip_prefix('+'))
		.unwrap_or(text);
	!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
}

fn warm_image_count(raw_count: &str) -> Result<usize> {
	let count = coerce_int("warm_images count", RawValue::String(raw_count))?;
	if count < 0 {
		return Err(EngineError::invalid("warm_images counts must be non-negative integers"));
	}
	usize::try_from(count)
		.map_err(|_| EngineError::invalid("warm_images counts must be non-negative integers"))
}

fn expand_user(text: &str) -> PathBuf {
	if text == "~" {
		return env::var_os("HOME").map_or_else(|| PathBuf::from(text), PathBuf::from);
	}
	if let Some(rest) = text.strip_prefix("~/") {
		return env::var_os("HOME")
			.map_or_else(|| PathBuf::from(text), |home| PathBuf::from(home).join(rest));
	}
	PathBuf::from(text)
}

#[cfg(test)]
mod tests {
	use std::ffi::OsString;

	use super::*;
	use crate::home::test_home;

	struct EnvGuard {
		saved: Vec<(String, Option<OsString>)>,
	}

	impl EnvGuard {
		fn clear() -> Self {
			let keys = ENV_KEYS
				.iter()
				.map(|(_, env_key)| (*env_key).to_owned())
				.chain(["VMON_CONFIG".to_owned()])
				.collect::<Vec<_>>();
			let saved = keys
				.iter()
				.map(|key| (key.clone(), env::var_os(key)))
				.collect::<Vec<_>>();
			for key in keys {
				// SAFETY: config tests serialize all environment mutation with the shared
				// test_home lock.
				unsafe { env::remove_var(key) };
			}
			Self { saved }
		}

		fn set(key: &str, value: &str) {
			// SAFETY: config tests serialize all environment mutation with the shared
			// test_home lock.
			unsafe { env::set_var(key, value) };
		}
	}

	impl Drop for EnvGuard {
		fn drop(&mut self) {
			for (key, value) in &self.saved {
				match value {
					Some(value) => {
						// SAFETY: config tests serialize all environment mutation with the shared
						// test_home lock.
						unsafe { env::set_var(key, value) };
					},
					None => {
						// SAFETY: config tests serialize all environment mutation with the shared
						// test_home lock.
						unsafe { env::remove_var(key) };
					},
				}
			}
		}
	}

	#[test]
	fn precedence_tracks_default_file_env_flag_sources() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let tmp = tempfile::tempdir().expect("tempdir");
		let config_path = tmp.path().join("serve.toml");
		fs::write(
			&config_path,
			"[serve]\nhost = '0.0.0.0'\nport = 9001\nclient_token = 'from-file'\nreplicas = 2\n",
		)
		.expect("write config");
		EnvGuard::set("VMON_CONFIG", config_path.to_str().expect("utf8 path"));
		EnvGuard::set("VMON_SERVE_PORT", "9002");
		let overrides = HashMap::from([
			("host".to_owned(), "192.0.2.1".to_owned()),
			("replicas".to_owned(), "5".to_owned()),
		]);
		let config = resolve_serve_config(&overrides).expect("resolve config");
		assert_eq!(config.host, "192.0.2.1");
		assert_eq!(config.port, 9002);
		assert_eq!(config.client_token.as_deref(), Some("from-file"));
		assert_eq!(config.replicas, 5);
		assert_eq!(config.idle_timeout, 300.0);
		assert_eq!(config.source("host"), Some(ConfigSource::Flag));
		assert_eq!(config.source("port"), Some(ConfigSource::Env));
		assert_eq!(config.source("client_token"), Some(ConfigSource::File));
		assert_eq!(config.source("idle_timeout"), Some(ConfigSource::Default));
	}

	#[test]
	fn coercion_accepts_bool_numbers_floats_and_warm_images() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let overrides = HashMap::from([
			("restore_quorum".to_owned(), "yes".to_owned()),
			("port".to_owned(), "0".to_owned()),
			("idle_timeout".to_owned(), "1.5".to_owned()),
			("warm_pool_size".to_owned(), "3".to_owned()),
			("warm_images".to_owned(), "alpine:2 ubuntu=4,debian".to_owned()),
		]);
		let config = resolve_serve_config(&overrides).expect("resolve config");
		assert_eq!(config.restore_quorum, Some(true));
		assert_eq!(config.port, 0);
		assert_eq!(config.idle_timeout, 1.5);
		assert_eq!(config.warm_images[0], WarmImage { reference: "alpine".to_owned(), count: 2 });
		assert_eq!(config.warm_images[1], WarmImage { reference: "ubuntu".to_owned(), count: 4 });
		assert_eq!(config.warm_images[2], WarmImage { reference: "debian".to_owned(), count: 3 });
	}

	#[test]
	fn function_artifact_quota_defaults_and_honors_env_and_cli() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		assert_eq!(
			resolve_serve_config(&HashMap::new())
				.expect("default config")
				.function_artifact_max_bytes,
			4 * 1024 * 1024 * 1024
		);
		EnvGuard::set("VMON_FUNCTION_ARTIFACT_MAX_BYTES", "67108864");
		let from_env = resolve_serve_config(&HashMap::new()).expect("env quota");
		assert_eq!(from_env.function_artifact_max_bytes, 67_108_864);
		assert_eq!(from_env.source("function_artifact_max_bytes"), Some(ConfigSource::Env));
		let from_cli = resolve_serve_config(&HashMap::from([(
			"function_artifact_max_bytes".to_owned(),
			"134217728".to_owned(),
		)]))
		.expect("CLI quota");
		assert_eq!(from_cli.function_artifact_max_bytes, 134_217_728);
		assert_eq!(from_cli.source("function_artifact_max_bytes"), Some(ConfigSource::Flag));
	}

	#[test]
	fn malformed_values_name_the_key() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		for (key, value) in
			[("port", "abc"), ("restore_quorum", "maybe"), ("warm_images", "alpine=-1")]
		{
			let overrides = HashMap::from([(key.to_owned(), value.to_owned())]);
			let err = resolve_serve_config(&overrides).expect_err("bad value");
			assert!(err.message.contains(key), "expected {key:?} in error {:?}", err.message);
		}
	}

	#[test]
	fn function_artifact_quota_rejects_zero_and_excessive_values() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		for value in ["0", "-1", "1125899906842625"] {
			let error = resolve_serve_config(&HashMap::from([(
				"function_artifact_max_bytes".to_owned(),
				value.to_owned(),
			)]))
			.expect_err("invalid artifact quota");
			assert!(error.message.contains("function_artifact_max_bytes"));
		}
	}

	#[test]
	fn s3_multipart_stale_sec_defaults_and_validation() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();

		// 1. Default value assertion
		let default_config = resolve_serve_config(&HashMap::new()).expect("default config");
		assert_eq!(default_config.s3_multipart_stale_sec, 7.0 * 24.0 * 3600.0);
		assert_eq!(default_config.source("s3_multipart_stale_sec"), Some(ConfigSource::Default));

		// 2. Env overlay assertion
		EnvGuard::set("VMON_S3_MULTIPART_STALE_SEC", "86400");
		let env_config = resolve_serve_config(&HashMap::new()).expect("env config");
		assert_eq!(env_config.s3_multipart_stale_sec, 86400.0);
		assert_eq!(env_config.source("s3_multipart_stale_sec"), Some(ConfigSource::Env));

		// 3. CLI override assertion
		let cli_config = resolve_serve_config(&HashMap::from([(
			"s3_multipart_stale_sec".to_owned(),
			"172800".to_owned(),
		)]))
		.expect("CLI config");
		assert_eq!(cli_config.s3_multipart_stale_sec, 172800.0);
		assert_eq!(cli_config.source("s3_multipart_stale_sec"), Some(ConfigSource::Flag));

		// Reset EnvGuard for subsequent test cases
		let _env_guard = EnvGuard::clear();

		// 4. TOML override assertion
		let tmp = tempfile::tempdir().expect("tempdir");
		let config_path = tmp.path().join("serve.toml");
		fs::write(&config_path, "[serve]\ns3_multipart_stale_sec = 10000.5\n").expect("write config");
		EnvGuard::set("VMON_CONFIG", config_path.to_str().expect("utf8 path"));
		let toml_config = resolve_serve_config(&HashMap::new()).expect("toml config");
		assert_eq!(toml_config.s3_multipart_stale_sec, 10000.5);
		assert_eq!(toml_config.source("s3_multipart_stale_sec"), Some(ConfigSource::File));

		// 5. Validations
		// Zero is valid (disables janitor)
		let zero_config = resolve_serve_config(&HashMap::from([(
			"s3_multipart_stale_sec".to_owned(),
			"0".to_owned(),
		)]))
		.expect("zero is valid");
		assert_eq!(zero_config.s3_multipart_stale_sec, 0.0);

		// >= 3600 is valid
		let valid_config = resolve_serve_config(&HashMap::from([(
			"s3_multipart_stale_sec".to_owned(),
			"3600".to_owned(),
		)]))
		.expect("3600 is valid");
		assert_eq!(valid_config.s3_multipart_stale_sec, 3600.0);

		// >0 and <3600 is invalid
		for invalid_val in ["1", "3599.9"] {
			let err = resolve_serve_config(&HashMap::from([(
				"s3_multipart_stale_sec".to_owned(),
				invalid_val.to_owned(),
			)]))
			.expect_err("should reject values under 3600");
			assert!(err.message.contains("s3_multipart_stale_sec"));
			assert!(
				err.message
					.contains("must be zero or at least 3600 seconds")
			);
		}

		// Negative is invalid
		for negative_val in ["-1", "-3600"] {
			let err = resolve_serve_config(&HashMap::from([(
				"s3_multipart_stale_sec".to_owned(),
				negative_val.to_owned(),
			)]))
			.expect_err("should reject negative values");
			assert!(err.message.contains("s3_multipart_stale_sec"));
		}
	}

	#[test]
	fn rejects_unknown_config_keys() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let overrides = HashMap::from([("bogus".to_owned(), "1".to_owned())]);
		let err = resolve_serve_config(&overrides).expect_err("unknown key");
		assert!(err.message.contains("unknown CLI override"));
		validate_knob_inventory().expect("metadata covers fields");
	}

	#[test]
	fn cluster_mode_validation() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		// Default single-node should parse without extra fields
		let config = resolve_serve_config(&HashMap::new()).expect("default");
		assert_eq!(config.cluster_mode, ClusterMode::SingleNode);

		// Single-node explicitly set
		let config = resolve_serve_config(&HashMap::from([(
			"cluster_mode".to_owned(),
			"single-node".to_owned(),
		)]))
		.expect("explicit single-node");
		assert_eq!(config.cluster_mode, ClusterMode::SingleNode);

		// Production with missing dependencies should fail
		let err = resolve_serve_config(&HashMap::from([(
			"cluster_mode".to_owned(),
			"production".to_owned(),
		)]))
		.expect_err("should reject missing postgres");
		assert!(err.message.contains("postgres_url"));

		// Production with postgres but missing S3 endpoint should fail
		let err = resolve_serve_config(&HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), "postgresql://user:pass@host/db".to_owned()),
		]))
		.expect_err("should reject missing s3 endpoint");
		assert!(err.message.contains("s3_endpoint"));

		// Production with postgres and S3 endpoint but missing bucket should fail
		let err = resolve_serve_config(&HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), "postgresql://user:pass@host/db".to_owned()),
			("s3_endpoint".to_owned(), "http://localhost:9000".to_owned()),
		]))
		.expect_err("should reject missing s3 bucket");
		assert!(err.message.contains("s3_bucket"));

		// Production must also provide signed-request settings.
		let err = resolve_serve_config(&HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), "postgresql://user:pass@host/db".to_owned()),
			("s3_endpoint".to_owned(), "http://localhost:9000".to_owned()),
			("s3_bucket".to_owned(), "my-bucket".to_owned()),
		]))
		.expect_err("should reject missing S3 signing configuration");
		assert!(err.message.contains("s3_region"));

		let err = resolve_serve_config(&HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), "postgresql://user:pass@host/db".to_owned()),
			("s3_endpoint".to_owned(), "http://localhost:9000".to_owned()),
			("s3_bucket".to_owned(), "my-bucket".to_owned()),
			("s3_region".to_owned(), "us-east-1".to_owned()),
			("s3_access_key".to_owned(), "key".to_owned()),
			("s3_secret_key".to_owned(), "secret".to_owned()),
		]))
		.expect_err("should reject missing shared portable-history key");
		assert!(err.message.contains("portable_history_key_id"));

		// Production with all required configuration should pass
		let config = resolve_serve_config(&HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), "postgresql://user:pass@host/db".to_owned()),
			("s3_endpoint".to_owned(), "http://localhost:9000".to_owned()),
			("s3_bucket".to_owned(), "my-bucket".to_owned()),
			("s3_region".to_owned(), "us-east-1".to_owned()),
			("s3_access_key".to_owned(), "key".to_owned()),
			("s3_secret_key".to_owned(), "secret".to_owned()),
			("s3_prefix".to_owned(), "pre/".to_owned()),
			("portable_history_key_id".to_owned(), "cluster-recovery".to_owned()),
		]))
		.expect("valid production configuration");
		assert_eq!(config.cluster_mode, ClusterMode::Production);
		assert_eq!(config.postgres_url.as_deref(), Some("postgresql://user:pass@host/db"));
		assert_eq!(config.s3_endpoint.as_deref(), Some("http://localhost:9000"));
		assert_eq!(config.s3_bucket.as_deref(), Some("my-bucket"));
		assert_eq!(config.s3_region.as_deref(), Some("us-east-1"));
		assert_eq!(config.s3_access_key.as_deref(), Some("key"));
		assert_eq!(config.s3_secret_key.as_deref(), Some("secret"));
		assert_eq!(config.portable_history_key_id.as_deref(), Some("cluster-recovery"));
		assert_eq!(config.s3_prefix.as_deref(), Some("pre/"));
	}

	#[test]
	fn tunnel_bind_defaults_overlays_and_validation() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let config = resolve_serve_config(&HashMap::new()).expect("default config");
		assert_eq!(config.tunnel_bind, None);
		assert_eq!(config.source("tunnel_bind"), Some(ConfigSource::Default));

		EnvGuard::set("VMON_TUNNEL_BIND", "10.0.0.2");
		let env_config = resolve_serve_config(&HashMap::new()).expect("env config");
		assert_eq!(env_config.tunnel_bind.as_deref(), Some("10.0.0.2"));
		assert_eq!(env_config.source("tunnel_bind"), Some(ConfigSource::Env));

		let flag_config = resolve_serve_config(&HashMap::from([(
			"tunnel_bind".to_owned(),
			"2001:db8::2".to_owned(),
		)]))
		.expect("flag config");
		assert_eq!(flag_config.tunnel_bind.as_deref(), Some("2001:db8::2"));
		assert_eq!(flag_config.source("tunnel_bind"), Some(ConfigSource::Flag));

		let error =
			resolve_serve_config(&HashMap::from([("tunnel_bind".to_owned(), "not-an-ip".to_owned())]))
				.expect_err("invalid bind IP");
		assert_eq!(error.code.as_str(), "invalid");
		assert_eq!(error.message, "tunnel_bind must be a valid IP address");
	}

	#[test]
	fn orch_knobs_defaults_overlays_and_validation() {
		let _lock = test_home::lock();
		let _env_guard = EnvGuard::clear();
		let config = resolve_serve_config(&HashMap::new()).expect("default config");
		assert_eq!(config.orch_redis, None);
		assert_eq!(config.orch_url, None);
		assert_eq!(config.orch_id, None);
		assert_eq!(config.orch_heartbeat_sec, 1.0);
		assert_eq!(config.orch_dead_after_sec, 5.0);
		assert_eq!(config.orch_max_sandboxes, 0);
		assert_eq!(config.orch_memory_reserve_mib, 0);
		assert_eq!(config.source("orch_redis"), Some(ConfigSource::Default));

		EnvGuard::set("VMON_ORCH_REDIS", "redis://cache:6379");
		EnvGuard::set("VMON_ORCH_MAX_SANDBOXES", "16");
		EnvGuard::set("VMON_ORCH_MEMORY_RESERVE_MIB", "8192");
		let env_config = resolve_serve_config(&HashMap::new()).expect("env config");
		assert_eq!(env_config.orch_redis.as_deref(), Some("redis://cache:6379"));
		assert_eq!(env_config.orch_max_sandboxes, 16);
		assert_eq!(env_config.orch_memory_reserve_mib, 8192);
		assert_eq!(env_config.source("orch_redis"), Some(ConfigSource::Env));
		assert_eq!(env_config.source("orch_max_sandboxes"), Some(ConfigSource::Env));
		assert_eq!(env_config.source("orch_memory_reserve_mib"), Some(ConfigSource::Env));

		// A CLI empty string disables the env-provided endpoint.
		let cli_config = resolve_serve_config(&HashMap::from([
			("orch_redis".to_owned(), String::new()),
			("orch_url".to_owned(), "http://worker:8000".to_owned()),
			("orch_id".to_owned(), "w-stable".to_owned()),
			("orch_heartbeat_sec".to_owned(), "0.5".to_owned()),
			("orch_dead_after_sec".to_owned(), "2.5".to_owned()),
		]))
		.expect("cli config");
		assert_eq!(cli_config.orch_redis, None);
		assert_eq!(cli_config.orch_url.as_deref(), Some("http://worker:8000"));
		assert_eq!(cli_config.orch_id.as_deref(), Some("w-stable"));
		assert_eq!(cli_config.orch_heartbeat_sec, 0.5);
		assert_eq!(cli_config.orch_dead_after_sec, 2.5);
		assert_eq!(cli_config.source("orch_url"), Some(ConfigSource::Flag));

		for (key, value) in [
			("orch_heartbeat_sec", "0"),
			("orch_dead_after_sec", "-1"),
			("orch_max_sandboxes", "-2"),
			("orch_memory_reserve_mib", "-2"),
		] {
			let overrides = HashMap::from([(key.to_owned(), value.to_owned())]);
			let err = resolve_serve_config(&overrides).expect_err("bad orch value");
			assert!(err.message.contains(key), "expected {key:?} in error {:?}", err.message);
		}
	}
}
