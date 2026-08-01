//! Sandbox registry: `VmRecord` + rehydrate. Port of python/vmon/core.py
//! registry.

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fs, io,
	path::Path,
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{EngineError, Result, engine::spawn::replace_meta_map, home::Home};

/// Engine seam for re-acquiring writable volume locks after registry rehydrate.
pub type VolumeLockRequests = Vec<(String, Vec<String>)>;

/// Stored-state retention policy for a sandbox.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum PersistencePolicy {
	#[default]
	Persistent,
	Sticky {
		#[serde(default = "default_sticky_priority")]
		priority: u8,
	},
	Ephemeral,
}

const fn default_sticky_priority() -> u8 {
	5
}

impl PersistencePolicy {
	pub const fn sticky_priority(&self) -> Option<u8> {
		match self {
			Self::Sticky { priority } => Some(*priority),
			_ => None,
		}
	}
}

/// A lifecycle state encoded as the historical lowercase JSON string.
///
/// Unknown values round-trip exactly so a newer control plane cannot be
/// downgraded into a destructive state by an older monitor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum LifecyclePhase {
	Running,
	#[default]
	Stopped,
	Suspended,
	Paused,
	Terminated,
	Unknown(String),
}

impl LifecyclePhase {
	/// Return the stable JSON spelling of this phase.
	pub fn as_str(&self) -> &str {
		match self {
			Self::Running => "running",
			Self::Stopped => "stopped",
			Self::Suspended => "suspended",
			Self::Paused => "paused",
			Self::Terminated => "terminated",
			Self::Unknown(value) => value,
		}
	}

	fn from_str(value: &str) -> Self {
		match value {
			"running" => Self::Running,
			"stopped" => Self::Stopped,
			"suspended" => Self::Suspended,
			"paused" => Self::Paused,
			"terminated" => Self::Terminated,
			_ => Self::Unknown(value.to_owned()),
		}
	}
}

impl Serialize for LifecyclePhase {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

impl<'de> Deserialize<'de> for LifecyclePhase {
	fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		String::deserialize(deserializer).map(|value| Self::from_str(&value))
	}
}

/// Monotonic lifecycle write version used to reject stale completers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StateGeneration(pub u64);

/// A durable operation that remains pending even when desired and observed
/// phases happen to be equal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleOperation {
	/// Materialize and switch to the exact durable recovery point.
	Rollback { recovery_point: String },
	/// Forward-compatible operation marker for non-state-changing work.
	Other { kind: String },
}

impl LifecycleOperation {
	/// Stable operation name for reconciliation logs and metrics.
	pub fn kind(&self) -> &str {
		match self {
			Self::Rollback { .. } => "rollback",
			Self::Other { kind } => kind,
		}
	}
}

/// Durable desired/observed lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleState {
	pub desired:    LifecyclePhase,
	pub observed:   LifecyclePhase,
	pub generation: StateGeneration,
	pub operation:  Option<LifecycleOperation>,
	pub failure:    Option<String>,
}

impl Default for LifecycleState {
	fn default() -> Self {
		Self {
			desired:    LifecyclePhase::Stopped,
			observed:   LifecyclePhase::Stopped,
			generation: StateGeneration(0),
			operation:  None,
			failure:    None,
		}
	}
}

impl LifecycleState {
	/// Whether an operation still needs lifecycle reconciliation.
	pub fn is_converged(&self) -> bool {
		self.operation.is_none() && self.failure.is_none() && self.desired == self.observed
	}
}

/// The generation and target returned by a successful transition begin/retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleTransition {
	pub generation: StateGeneration,
	pub desired:    LifecyclePhase,
	pub observed:   LifecyclePhase,
}

/// Whether a caller owns the side effects for a requested transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransitionDisposition {
	/// This call durably installed a new generation and owns execution.
	Acquired,
	/// Another caller already owns the same non-converged generation.
	Joined,
	/// The requested state has already converged.
	AlreadyObserved,
}

/// Ownership-fenced result of starting a lifecycle transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionBegin {
	pub disposition: TransitionDisposition,
	pub generation:  StateGeneration,
	pub desired:     LifecyclePhase,
	pub observed:    LifecyclePhase,
	pub operation:   Option<LifecycleOperation>,
}

/// Safe, restartable runtime inputs. This deliberately has no secret values.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SafeRuntimeIdentity {
	#[serde(alias = "env")]
	pub environment:            BTreeMap<String, String>,
	pub workdir:                Option<String>,
	pub network_policy:         Option<Value>,
	/// Complete non-secret network inputs retained for older persisted records.
	#[serde(default)]
	pub network:                Value,
	pub tunnels:                Value,
	pub mounts:                 Vec<Value>,
	pub timeout_secs:           Option<f64>,
	pub source:                 Option<String>,
	pub template:               Option<String>,
	pub pool:                   Option<String>,
	pub secret_names:           BTreeSet<String>,
	pub available_secret_names: BTreeSet<String>,
	#[serde(default)]
	pub version:                u32,
	#[serde(default)]
	pub identity_complete:      bool,
}

impl SafeRuntimeIdentity {
	/// Remove values associated with a declared secret name before persistence.
	pub fn without_secret_values(mut self) -> Self {
		self
			.environment
			.retain(|name, _| !self.secret_names.contains(name));
		self
	}
}

/// A registry record requiring startup or background convergence.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconciliationInput {
	pub record:    VmRecord,
	pub lifecycle: LifecycleState,
	pub pid_alive: bool,
}

/// One sandbox registry entry shared by API and engine layers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VmRecord {
	/// Stable sandbox id.
	pub id:                  String,
	/// Human-facing sandbox name.
	pub name:                String,
	/// Lifecycle status.
	pub status:              String,
	/// Per-VM monitor process id.
	pub pid:                 Option<i32>,
	/// Image, template, fork, or restore source.
	pub source:              Option<String>,
	/// Unix creation timestamp.
	pub created_at:          f64,
	/// Timeout in seconds from last activity.
	pub timeout:             Option<f64>,
	/// Raw metadata detail spread into API views.
	pub detail:              Value,
	/// String tags used for filtering.
	pub tags:                HashMap<String, String>,
	/// Unix timestamp of last activity.
	pub last_active:         f64,
	/// Unix timestamp of the last qualifying network sample.
	#[serde(default)]
	pub last_network_active: f64,
	/// Stored-state retention and eviction behavior.
	#[serde(default)]
	pub persistence:         PersistencePolicy,
	/// Unix termination timestamp.
	pub terminated_at:       Option<f64>,
	/// Terminal error string.
	pub error:               Option<String>,
	/// Desired/observed lifecycle state and its generation.
	#[serde(default)]
	pub lifecycle:           LifecycleState,
	/// Stable production ownership incarnation, distinct from mutable owner
	/// epoch.
	#[serde(default)]
	pub incarnation_epoch:   i64,
	/// The complete non-secret identity needed to rebuild runtime resources.
	#[serde(default)]
	pub runtime_identity:    SafeRuntimeIdentity,
}

impl VmRecord {
	/// Build a record with Python's timestamp defaults.
	pub fn new(id: impl Into<String>, name: impl Into<String>, status: impl Into<String>) -> Self {
		let now = unix_time();
		let status = status.into();
		let phase = LifecyclePhase::from_str(&status);
		Self {
			id: id.into(),
			name: name.into(),
			status,
			pid: None,
			source: None,
			created_at: now,
			timeout: None,
			detail: Value::Object(Map::new()),
			tags: HashMap::new(),
			last_active: now,
			last_network_active: now,
			persistence: PersistencePolicy::default(),
			terminated_at: None,
			error: None,
			incarnation_epoch: 0,
			lifecycle: LifecycleState {
				desired: phase.clone(),
				observed: phase,
				..LifecycleState::default()
			},
			runtime_identity: SafeRuntimeIdentity::default(),
		}
	}

	/// Return the timeout deadline, if one is armed.
	pub fn expires_at(&self) -> Option<f64> {
		let timeout = self.timeout?;
		(timeout > 0.0).then_some(self.last_active + timeout)
	}

	/// Refresh `last_active` to the current Unix time.
	pub fn touch(&mut self) {
		self.last_active = unix_time();
	}

	/// Return the API view, with canonical keys overriding raw detail.
	pub fn view(&self) -> Value {
		let mut out = self.detail.as_object().cloned().unwrap_or_default();
		let returncode = out.get("returncode").cloned().unwrap_or(Value::Null);
		out.remove("idempotency_key");
		out.insert("id".to_owned(), json!(self.id));
		out.insert("name".to_owned(), json!(self.name));
		out.insert("status".to_owned(), json!(self.status));
		out.insert("desired_state".to_owned(), json!(self.lifecycle.desired));
		out.insert("observed_state".to_owned(), json!(self.lifecycle.observed));
		out.insert("state_generation".to_owned(), json!(self.lifecycle.generation));
		out.insert("lifecycle_failure".to_owned(), json!(self.lifecycle.failure));
		out.insert("pid".to_owned(), json!(self.pid));
		out.insert("source".to_owned(), json!(self.source));
		out.insert("created_at".to_owned(), json!(self.created_at));
		out.insert("last_active".to_owned(), json!(self.last_active));
		out.insert("last_network_active".to_owned(), json!(self.last_network_active));
		out.insert("persistence".to_owned(), json!(self.persistence));
		out.insert("expires_at".to_owned(), json!(self.expires_at()));
		out.insert("terminated_at".to_owned(), json!(self.terminated_at));
		out.insert("error".to_owned(), json!(self.error));
		out.insert("tags".to_owned(), json!(self.tags));
		out.insert("returncode".to_owned(), returncode);
		Value::Object(out)
	}
}

/// In-memory sandbox registry plus create idempotency index.
#[derive(Debug, Default)]
pub struct Registry {
	records:     RwLock<HashMap<String, VmRecord>>,
	idempotency: RwLock<HashMap<String, String>>,
}

impl Registry {
	/// Create an empty registry.
	pub fn new() -> Self {
		Self::default()
	}

	/// Rebuild records from `$VMON_HOME/vms/<name>/meta.json`.
	///
	/// Canonical identity and lifecycle timestamps are restored from metadata;
	/// legacy records without them keep their directory name as the stable ID.
	/// The returned `(record_id, volume_names)` entries are the volume locks the
	/// engine must re-acquire for records still backed by a live VMM process.
	pub fn rehydrate(&self, home: &Home) -> Result<VolumeLockRequests> {
		let vms_dir = home.vms_dir();
		if !vms_dir.is_dir() {
			self.replace(HashMap::new(), HashMap::new());
			return Ok(Vec::new());
		}

		let mut vm_dirs = Vec::new();
		for entry in fs::read_dir(&vms_dir)? {
			let entry = entry?;
			if entry.file_type()?.is_dir() {
				vm_dirs.push(entry.path());
			}
		}
		vm_dirs.sort_by(|left, right| left.file_name().cmp(&right.file_name()));

		let mut records = HashMap::new();
		let mut lock_requests = Vec::new();
		for dir in vm_dirs {
			let Some(name) = dir
				.file_name()
				.map(|name| name.to_string_lossy().into_owned())
			else {
				continue;
			};
			let meta_path = home.meta_path(&name);
			let mut meta = read_meta_map(&meta_path)?;
			let pid = pid_of(&meta);
			let running = pid.is_some_and(pid_lives);
			let saved_status = meta
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or("stopped")
				.to_owned();
			let has_persisted_lifecycle = meta.contains_key("lifecycle")
				|| meta.contains_key("desired_state")
				|| meta.contains_key("observed_state");
			let mut lifecycle = lifecycle_of(&meta, &saved_status);
			if !has_persisted_lifecycle {
				let actual = if running {
					LifecyclePhase::Running
				} else {
					match lifecycle.observed {
						LifecyclePhase::Suspended | LifecyclePhase::Terminated => {
							lifecycle.observed.clone()
						},
						_ => LifecyclePhase::Stopped,
					}
				};
				lifecycle.desired = actual.clone();
				lifecycle.observed = actual;
			}
			let lifecycle_changed = normalize_observed_for_pid(&mut lifecycle, running);
			let status = status_for_observed(&lifecycle.observed).to_owned();
			let now = unix_time();
			let id = string_field(&meta, "sandbox_id").unwrap_or_else(|| name.clone());
			let created_at = number_field(&meta, "created_at").unwrap_or(now);
			let runtime_identity = runtime_identity_of(&meta, source_of(&meta), timeout_of(&meta));
			let mut record = VmRecord {
				id: id.clone(),
				name: name.clone(),
				status: status.clone(),
				pid,
				source: source_of(&meta),
				created_at,
				timeout: timeout_of(&meta),
				detail: Value::Object(meta.clone()),
				tags: tags_of(&meta),
				last_active: number_field(&meta, "last_active").unwrap_or(created_at),
				last_network_active: number_field(&meta, "last_network_active")
					.or_else(|| number_field(&meta, "last_active"))
					.unwrap_or(created_at),
				persistence: meta
					.get("persistence")
					.cloned()
					.and_then(|value| serde_json::from_value(value).ok())
					.unwrap_or_default(),
				terminated_at: number_field(&meta, "terminated_at"),
				error: string_field(&meta, "error"),
				incarnation_epoch: meta
					.get("incarnation_epoch")
					.and_then(Value::as_i64)
					.unwrap_or_default(),
				lifecycle,
				runtime_identity,
			};

			if running {
				let volume_names = volume_names(&meta);
				if !volume_names.is_empty() {
					lock_requests.push((name.clone(), volume_names));
				}
				if meta.get("tap").is_some_and(json_truthy) {
					detail_object_mut(&mut record.detail)
						.insert("tunnels_lost".to_owned(), Value::Bool(true));
				}
			}
			if lifecycle_changed || saved_status != status {
				set_lifecycle_meta(&mut meta, &record.lifecycle);
				meta.insert("status".to_owned(), json!(status));
				let detail = detail_object_mut(&mut record.detail);
				set_lifecycle_detail(detail, &record.lifecycle);
				detail.insert("status".to_owned(), json!(status));
				if let Err(_err) = write_meta_map(&meta_path, &meta) {
					// Startup observes the current process even when a damaged
					// filesystem prevents best-effort normalization persistence.
				}
			}
			records.insert(id, record);
		}

		let mut idempotency = HashMap::new();
		for (name, record) in &records {
			if record.status == "terminated" {
				continue;
			}
			if let Some(key) = record
				.detail
				.get("idempotency_key")
				.and_then(Value::as_str)
				.filter(|key| !key.is_empty())
			{
				idempotency.insert(key.to_owned(), name.clone());
			}
		}
		self.replace(records, idempotency);
		Ok(lock_requests)
	}

	/// Return a cloned record by stable ID.
	pub fn get(&self, id: &str) -> Option<VmRecord> {
		self.records.read().get(id).cloned()
	}

	/// Return all records sorted by name for deterministic callers.
	pub fn list(&self) -> Vec<VmRecord> {
		let mut records = self.records.read().values().cloned().collect::<Vec<_>>();
		records.sort_by(|left, right| left.name.cmp(&right.name));
		records
	}

	/// Return the sandbox name previously recorded for an idempotency key.
	pub fn find_by_idempotency_key(&self, key: &str) -> Option<String> {
		self.idempotency.read().get(key).cloned()
	}

	/// Record an idempotency key mapping for a stable sandbox ID.
	pub fn record_idempotency(&self, id: &str, key: &str) {
		if !key.is_empty() {
			self
				.idempotency
				.write()
				.insert(key.to_owned(), id.to_owned());
		}
	}

	/// Insert or replace one record by stable ID.
	pub fn insert(&self, record: VmRecord) {
		self.records.write().insert(record.id.clone(), record);
	}

	/// Persist canonical identity fields, then insert the record.
	pub fn insert_persisted(&self, home: &Home, mut record: VmRecord) -> Result<()> {
		let mut meta = read_meta_map(&home.meta_path(&record.name))?;
		if record.runtime_identity == SafeRuntimeIdentity::default() {
			record.runtime_identity =
				runtime_identity_of(&meta, record.source.clone(), record.timeout)
					.without_secret_values();
			if meta.contains_key("runtime_identity") {
				// An already-written identity may contain future safe fields this
				// binary does not understand, so do not replace that field.
			} else {
				meta.insert(
					"runtime_identity".to_owned(),
					serde_json::to_value(&record.runtime_identity)?,
				);
			}
		} else {
			record.runtime_identity = record.runtime_identity.without_secret_values();
			meta
				.insert("runtime_identity".to_owned(), serde_json::to_value(&record.runtime_identity)?);
		}
		meta.insert("sandbox_id".to_owned(), json!(record.id));
		meta.insert("created_at".to_owned(), json!(record.created_at));
		meta.insert("last_active".to_owned(), json!(record.last_active));
		meta.insert("last_network_active".to_owned(), json!(record.last_network_active));
		meta.insert("persistence".to_owned(), json!(record.persistence));
		meta.insert("status".to_owned(), json!(record.status));
		meta.insert("terminated_at".to_owned(), json!(record.terminated_at));
		meta.insert("error".to_owned(), json!(record.error));
		set_lifecycle_meta(&mut meta, &record.lifecycle);
		write_meta_map(&home.meta_path(&record.name), &meta)?;
		self.insert(record);
		Ok(())
	}

	/// Atomically begin a generation-guarded transition.
	///
	/// Repeating a request that already installed the same pending target is
	/// idempotent, including when its caller only knows the prior generation.
	/// Begin a state-only transition. Operations that need execution despite an
	/// equal desired/observed state must use [`Self::begin_operation`].
	pub fn begin_transition(
		&self,
		home: &Home,
		id: &str,
		expected: StateGeneration,
		desired: LifecyclePhase,
	) -> Result<TransitionBegin> {
		self.begin_operation(home, id, expected, desired, None)
	}

	pub fn begin_operation(
		&self,
		home: &Home,
		id: &str,
		expected: StateGeneration,
		desired: LifecyclePhase,
		operation: Option<LifecycleOperation>,
	) -> Result<TransitionBegin> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation == expected
			&& record.lifecycle.desired == desired
			&& record.lifecycle.operation == operation
			&& record.lifecycle.failure.is_none()
		{
			return Ok(begin_of(
				&record.lifecycle,
				if record.lifecycle.is_converged() {
					TransitionDisposition::AlreadyObserved
				} else {
					TransitionDisposition::Joined
				},
			));
		}
		if record.lifecycle.generation == StateGeneration(expected.0.saturating_add(1))
			&& record.lifecycle.desired == desired
			&& record.lifecycle.operation == operation
			&& record.lifecycle.failure.is_none()
			&& !record.lifecycle.is_converged()
		{
			return Ok(begin_of(&record.lifecycle, TransitionDisposition::Joined));
		}
		if record.lifecycle.generation != expected {
			return Err(stale_generation(id, expected, record.lifecycle.generation));
		}
		if record.lifecycle.desired == desired
			&& record.lifecycle.operation == operation
			&& record.lifecycle.failure.is_some()
		{
			let next = expected.0.checked_add(1).ok_or_else(|| {
				EngineError::invalid(format!("lifecycle generation exhausted for '{id}'"))
			})?;
			let mut updated = record.clone();
			updated.lifecycle.generation = StateGeneration(next);
			updated.lifecycle.failure = None;
			persist_lifecycle_record(home, &mut updated)?;
			let begin = begin_of(&updated.lifecycle, TransitionDisposition::Acquired);
			*record = updated;
			return Ok(begin);
		}
		if !record.lifecycle.is_converged() {
			return Err(EngineError::invalid(format!(
				"lifecycle transition for '{id}' is still pending"
			)));
		}
		let next = expected.0.checked_add(1).ok_or_else(|| {
			EngineError::invalid(format!("lifecycle generation exhausted for '{id}'"))
		})?;
		let mut updated = record.clone();
		updated.lifecycle.generation = StateGeneration(next);
		updated.lifecycle.desired = desired;
		updated.lifecycle.operation = operation;
		updated.lifecycle.failure = None;
		persist_lifecycle_record(home, &mut updated)?;
		let begin = begin_of(&updated.lifecycle, TransitionDisposition::Acquired);
		*record = updated;
		Ok(begin)
	}

	/// Record a generation-guarded observation. Duplicate completion is a no-op.
	pub fn observe_transition(
		&self,
		home: &Home,
		id: &str,
		generation: StateGeneration,
		observed: LifecyclePhase,
	) -> Result<LifecycleTransition> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation != generation {
			return Err(stale_generation(id, generation, record.lifecycle.generation));
		}
		if record.lifecycle.observed == observed
			&& record.lifecycle.failure.is_none()
			&& record.lifecycle.operation.is_none()
		{
			return Ok(transition_of(&record.lifecycle));
		}
		let mut updated = record.clone();
		updated.lifecycle.observed = observed;
		updated.lifecycle.operation = None;
		updated.lifecycle.failure = None;
		status_for_observed(&updated.lifecycle.observed).clone_into(&mut updated.status);
		persist_lifecycle_record(home, &mut updated)?;
		let transition = transition_of(&updated.lifecycle);
		*record = updated;
		Ok(transition)
	}

	/// Record a failed attempt without losing the desired state that must retry.
	pub fn fail_transition(
		&self,
		home: &Home,
		id: &str,
		generation: StateGeneration,
		error: impl Into<String>,
	) -> Result<LifecycleTransition> {
		let error = error.into();
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation != generation {
			return Err(stale_generation(id, generation, record.lifecycle.generation));
		}
		if record.lifecycle.failure.as_deref() == Some(&error) {
			return Ok(transition_of(&record.lifecycle));
		}
		let mut updated = record.clone();
		updated.lifecycle.failure = Some(error);
		persist_lifecycle_record(home, &mut updated)?;
		let transition = transition_of(&updated.lifecycle);
		*record = updated;
		Ok(transition)
	}

	/// Cancel an interrupted transition after preserving the observed runtime.
	///
	/// This exact-generation CAS is idempotent for the same observed state and
	/// reason, and leaves a durable non-secret explanation for reconciliation.
	pub fn cancel_transition(
		&self,
		home: &Home,
		id: &str,
		generation: StateGeneration,
		observed: LifecyclePhase,
		reason: impl Into<String>,
	) -> Result<LifecycleTransition> {
		let reason = reason.into();
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation != generation {
			return Err(stale_generation(id, generation, record.lifecycle.generation));
		}
		if record.lifecycle.is_converged()
			&& record.lifecycle.observed == observed
			&& record.error.as_deref() == Some(&reason)
		{
			return Ok(transition_of(&record.lifecycle));
		}
		let mut updated = record.clone();
		updated.lifecycle.desired = observed.clone();
		updated.lifecycle.observed = observed;
		updated.lifecycle.operation = None;
		updated.lifecycle.failure = None;
		status_for_observed(&updated.lifecycle.observed).clone_into(&mut updated.status);
		updated.error = Some(reason);
		persist_cancelled_record(home, &mut updated)?;
		let transition = transition_of(&updated.lifecycle);
		*record = updated;
		Ok(transition)
	}

	/// Claim an interrupted persisted transition for reconciliation.
	///
	/// A resumed generation fences delayed completers from the crashed owner,
	/// while retaining its desired state and pending operation payload.
	pub fn resume_transition(
		&self,
		home: &Home,
		id: &str,
		generation: StateGeneration,
	) -> Result<TransitionBegin> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation != generation {
			return Err(stale_generation(id, generation, record.lifecycle.generation));
		}
		if record.lifecycle.is_converged() {
			return Ok(begin_of(&record.lifecycle, TransitionDisposition::AlreadyObserved));
		}
		let next = generation.0.checked_add(1).ok_or_else(|| {
			EngineError::invalid(format!("lifecycle generation exhausted for '{id}'"))
		})?;
		let mut updated = record.clone();
		updated.lifecycle.generation = StateGeneration(next);
		updated.lifecycle.failure = None;
		persist_lifecycle_record(home, &mut updated)?;
		let begin = begin_of(&updated.lifecycle, TransitionDisposition::Acquired);
		*record = updated;
		Ok(begin)
	}

	/// Start the next generation for a previously failed desired transition.
	pub fn retry_transition(
		&self,
		home: &Home,
		id: &str,
		generation: StateGeneration,
	) -> Result<LifecycleTransition> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.lifecycle.generation != generation {
			return Err(stale_generation(id, generation, record.lifecycle.generation));
		}
		if record.lifecycle.failure.is_none() {
			return Ok(transition_of(&record.lifecycle));
		}
		let next = generation.0.checked_add(1).ok_or_else(|| {
			EngineError::invalid(format!("lifecycle generation exhausted for '{id}'"))
		})?;
		let mut updated = record.clone();
		updated.lifecycle.generation = StateGeneration(next);
		updated.lifecycle.failure = None;
		persist_lifecycle_record(home, &mut updated)?;
		let transition = transition_of(&updated.lifecycle);
		*record = updated;
		Ok(transition)
	}

	/// Return every record whose desired state has not durably converged.
	pub fn non_converged_transitions(&self) -> Vec<ReconciliationInput> {
		self
			.list()
			.into_iter()
			.filter_map(|record| {
				let pid_alive = record.pid.is_some_and(pid_lives);
				(!record.lifecycle.is_converged()).then(|| ReconciliationInput {
					lifecycle: record.lifecycle.clone(),
					record,
					pid_alive,
				})
			})
			.collect()
	}

	/// Inputs for startup reconciliation; deliberately identical to background
	/// reconciliation so an interrupted operation follows one convergence path.
	pub fn startup_reconciliation_inputs(&self) -> Vec<ReconciliationInput> {
		self.non_converged_transitions()
	}

	/// Atomically persist restart-safe runtime identity, never secret values.
	pub fn persist_runtime_identity(
		&self,
		home: &Home,
		id: &str,
		identity: SafeRuntimeIdentity,
	) -> Result<()> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		let identity = identity.without_secret_values();
		if record.runtime_identity == identity {
			return Ok(());
		}
		let mut meta = read_meta_map(&home.meta_path(&record.name))?;
		meta.insert("runtime_identity".to_owned(), serde_json::to_value(&identity)?);
		write_meta_map(&home.meta_path(&record.name), &meta)?;
		record.runtime_identity = identity;
		Ok(())
	}

	/// Atomically replace a sandbox's idle policy and rearm its network-idle
	/// clock.
	pub fn set_idle_timeout_persisted(
		&self,
		home: &Home,
		id: &str,
		timeout_secs: f64,
		rearmed_at: f64,
	) -> Result<VmRecord> {
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		let mut meta = read_meta_map(&home.meta_path(&record.name))?;
		meta.insert("idle_timeout_secs".to_owned(), json!(timeout_secs));
		meta.insert("last_network_active".to_owned(), json!(rearmed_at));
		if let Some(params) = meta
			.get_mut("relaunch_recipe")
			.and_then(Value::as_object_mut)
			.and_then(|recipe| recipe.get_mut("params"))
			.and_then(Value::as_object_mut)
		{
			params.insert("idle_timeout_secs".to_owned(), json!(timeout_secs));
		}
		set_lifecycle_meta(&mut meta, &record.lifecycle);
		meta.insert("status".to_owned(), json!(record.status));
		write_meta_map(&home.meta_path(&record.name), &meta)?;
		record.last_network_active = rearmed_at;
		record.detail = Value::Object(meta);
		Ok(record.clone())
	}

	/// Atomically merge non-lifecycle metadata without allowing a concurrent
	/// detail writer to erase the current lifecycle generation.
	///
	/// All facade metadata mutations must use this transaction rather than
	/// independently writing `meta.json`.
	pub fn update_detail_persisted<F>(&self, home: &Home, id: &str, update: F) -> Result<VmRecord>
	where
		F: FnOnce(&mut Map<String, Value>),
	{
		let mut records = self.records.write();
		let record = records
			.get_mut(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		let mut meta = read_meta_map(&home.meta_path(&record.name))?;
		update(&mut meta);
		set_lifecycle_meta(&mut meta, &record.lifecycle);
		meta.insert("status".to_owned(), json!(record.status));
		write_meta_map(&home.meta_path(&record.name), &meta)?;
		let mut updated = record.clone();
		updated.detail = Value::Object(meta);
		*record = updated.clone();
		Ok(updated)
	}

	/// Remove one record by stable ID and clear its idempotency entries.
	pub fn remove(&self, id: &str) -> Option<VmRecord> {
		let removed = self.records.write().remove(id);
		if removed.is_some() {
			self
				.idempotency
				.write()
				.retain(|_, sandbox_id| sandbox_id != id);
		}
		removed
	}

	/// Mutate one record by stable ID and return the updated clone.
	pub fn update<F>(&self, id: &str, update: F) -> Option<VmRecord>
	where
		F: FnOnce(&mut VmRecord),
	{
		let mut records = self.records.write();
		let record = records.get_mut(id)?;
		update(record);
		Some(record.clone())
	}

	/// Drop a stale idempotency mapping if it still points at `id`.
	pub fn remove_idempotency_for(&self, key: &str, id: &str) {
		let mut idempotency = self.idempotency.write();
		if idempotency
			.get(key)
			.is_some_and(|sandbox_id| sandbox_id == id)
		{
			idempotency.remove(key);
		}
	}

	/// Replace or add one field in the durable detail object.
	pub fn set_detail_field(&self, id: &str, key: &str, value: Value) -> Option<VmRecord> {
		self.update(id, |record| {
			detail_object_mut(&mut record.detail).insert(key.to_owned(), value);
		})
	}

	/// Rebuild the idempotency index from current records.
	pub fn rebuild_idempotency_index(&self) {
		let records = self.records.read();
		let mut idempotency = HashMap::new();
		for (name, record) in records.iter() {
			if record.status == "terminated" {
				continue;
			}
			if let Some(key) = record
				.detail
				.get("idempotency_key")
				.and_then(Value::as_str)
				.filter(|key| !key.is_empty())
			{
				idempotency.insert(key.to_owned(), name.clone());
			}
		}
		*self.idempotency.write() = idempotency;
	}

	/// Update a record by stable ID and persist terminal status into
	/// `meta.json`.
	pub fn persist_record_status(
		&self,
		home: &Home,
		id: &str,
		status: &str,
		returncode: Option<i64>,
		terminated_at: Option<f64>,
	) -> Result<()> {
		let mut records = self.records.write();
		let Some(record) = records.get_mut(id) else {
			let mut meta = read_meta_map(&home.meta_path(id))?;
			meta.insert("status".to_owned(), json!(status));
			if let Some(code) = returncode {
				meta.insert("returncode".to_owned(), json!(code));
			}
			if let Some(ts) = terminated_at {
				meta.insert("terminated_at".to_owned(), json!(ts));
			}
			return write_meta_map(&home.meta_path(id), &meta);
		};
		let mut updated = record.clone();
		status.clone_into(&mut updated.status);
		if let Some(ts) = terminated_at {
			updated.terminated_at = Some(ts);
		}
		let mut meta = read_meta_map(&home.meta_path(&updated.name))?;
		meta.insert("status".to_owned(), json!(status));
		if let Some(code) = returncode {
			meta.insert("returncode".to_owned(), json!(code));
		}
		if let Some(ts) = terminated_at {
			meta.insert("terminated_at".to_owned(), json!(ts));
		}
		set_lifecycle_meta(&mut meta, &updated.lifecycle);
		write_meta_map(&home.meta_path(&updated.name), &meta)?;
		updated.detail = Value::Object(meta);
		*record = updated;
		Ok(())
	}

	fn replace(&self, records: HashMap<String, VmRecord>, idempotency: HashMap<String, String>) {
		*self.records.write() = records;
		*self.idempotency.write() = idempotency;
	}
}

fn read_meta_map(path: &Path) -> Result<Map<String, Value>> {
	let text = match fs::read_to_string(path) {
		Ok(text) => text,
		Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Map::new()),
		Err(err) => return Err(err.into()),
	};
	let value = serde_json::from_str::<Value>(&text)
		.map_err(|err| EngineError::invalid(format!("could not parse {}: {err}", path.display())))?;
	value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::invalid(format!("{} must contain a JSON object", path.display())))
}

fn write_meta_map(path: &Path, meta: &Map<String, Value>) -> Result<()> {
	replace_meta_map(path, meta)
}

fn lifecycle_of(meta: &Map<String, Value>, legacy_status: &str) -> LifecycleState {
	if let Some(value) = meta.get("lifecycle")
		&& let Ok(lifecycle) = serde_json::from_value::<LifecycleState>(value.clone())
	{
		return lifecycle;
	}
	let desired = string_field(meta, "desired_state").unwrap_or_else(|| legacy_status.to_owned());
	let observed = string_field(meta, "observed_state").unwrap_or_else(|| legacy_status.to_owned());
	LifecycleState {
		desired:    LifecyclePhase::from_str(&desired),
		observed:   LifecyclePhase::from_str(&observed),
		generation: StateGeneration(
			meta
				.get("state_generation")
				.and_then(Value::as_u64)
				.unwrap_or(0),
		),
		operation:  None,
		failure:    string_field(meta, "lifecycle_failure"),
	}
}

fn normalize_observed_for_pid(lifecycle: &mut LifecycleState, pid_alive: bool) -> bool {
	let normalized = if pid_alive {
		match lifecycle.observed {
			LifecyclePhase::Stopped | LifecyclePhase::Suspended | LifecyclePhase::Terminated => {
				LifecyclePhase::Running
			},
			_ => return false,
		}
	} else {
		match lifecycle.observed {
			LifecyclePhase::Running | LifecyclePhase::Paused => LifecyclePhase::Stopped,
			_ => return false,
		}
	};
	lifecycle.observed = normalized;
	true
}

fn status_for_observed(observed: &LifecyclePhase) -> &str {
	match observed {
		LifecyclePhase::Paused => LifecyclePhase::Running.as_str(),
		phase => phase.as_str(),
	}
}

fn set_lifecycle_meta(meta: &mut Map<String, Value>, lifecycle: &LifecycleState) {
	meta.insert(
		"lifecycle".to_owned(),
		serde_json::to_value(lifecycle).expect("lifecycle state is serializable"),
	);
	set_lifecycle_detail(meta, lifecycle);
}

fn set_lifecycle_detail(detail: &mut Map<String, Value>, lifecycle: &LifecycleState) {
	detail.insert("desired_state".to_owned(), json!(lifecycle.desired));
	detail.insert("observed_state".to_owned(), json!(lifecycle.observed));
	detail.insert("state_generation".to_owned(), json!(lifecycle.generation));
	detail.insert("lifecycle_failure".to_owned(), json!(lifecycle.failure));
}

fn transition_of(lifecycle: &LifecycleState) -> LifecycleTransition {
	LifecycleTransition {
		generation: lifecycle.generation,
		desired:    lifecycle.desired.clone(),
		observed:   lifecycle.observed.clone(),
	}
}

fn stale_generation(id: &str, expected: StateGeneration, actual: StateGeneration) -> EngineError {
	EngineError::invalid(format!(
		"stale lifecycle generation for '{id}': expected {}, found {}",
		expected.0, actual.0
	))
}

fn persist_lifecycle_record(home: &Home, record: &mut VmRecord) -> Result<()> {
	let mut meta = read_meta_map(&home.meta_path(&record.name))?;
	set_lifecycle_meta(&mut meta, &record.lifecycle);
	meta.insert("status".to_owned(), json!(record.status));
	write_meta_map(&home.meta_path(&record.name), &meta)?;
	let detail = detail_object_mut(&mut record.detail);
	set_lifecycle_detail(detail, &record.lifecycle);
	detail.insert("status".to_owned(), json!(record.status));
	Ok(())
}

fn persist_cancelled_record(home: &Home, record: &mut VmRecord) -> Result<()> {
	let mut meta = read_meta_map(&home.meta_path(&record.name))?;
	set_lifecycle_meta(&mut meta, &record.lifecycle);
	meta.insert("status".to_owned(), json!(record.status));
	meta.insert("error".to_owned(), json!(record.error));
	write_meta_map(&home.meta_path(&record.name), &meta)?;
	let detail = detail_object_mut(&mut record.detail);
	set_lifecycle_detail(detail, &record.lifecycle);
	detail.insert("status".to_owned(), json!(record.status));
	detail.insert("error".to_owned(), json!(record.error));
	Ok(())
}

fn runtime_identity_of(
	meta: &Map<String, Value>,
	source: Option<String>,
	timeout_secs: Option<f64>,
) -> SafeRuntimeIdentity {
	if let Some(value) = meta.get("runtime_identity")
		&& let Ok(mut identity) = serde_json::from_value::<SafeRuntimeIdentity>(value.clone())
	{
		if identity.timeout_secs.is_none() {
			identity.timeout_secs = timeout_secs;
		}
		if identity.source.is_none() {
			identity.source = source;
		}
		if identity.workdir.is_none() {
			identity.workdir = string_field(meta, "workdir");
		}
		if identity.mounts.is_empty() {
			identity.mounts = ["volumes", "s3_mounts"]
				.into_iter()
				.filter_map(|key| meta.get(key).cloned())
				.collect();
		}
		return identity.without_secret_values();
	}
	let secret_names = string_set(meta.get("secret_names"));
	let available_secret_names = available_secret_names_of(
		meta.get("available_secret_names"),
		meta.get("secret_availability"),
	);
	let environment = meta
		.get("env")
		.and_then(Value::as_object)
		.map(|env| {
			env.iter()
				.filter_map(|(name, value)| {
					(!secret_names.contains(name))
						.then(|| value.as_str().map(|value| (name.clone(), value.to_owned())))
						.flatten()
				})
				.collect()
		})
		.unwrap_or_default();
	let network = meta.get("network").and_then(Value::as_object);
	let mounts = ["volumes", "s3_mounts"]
		.into_iter()
		.filter_map(|key| meta.get(key).cloned())
		.collect();
	SafeRuntimeIdentity {
		environment,
		workdir: string_field(meta, "workdir"),
		network_policy: network.and_then(|network| network.get("policy").cloned()),
		network: network.map_or(Value::Null, |network| Value::Object(network.clone())),
		tunnels: network
			.and_then(|network| network.get("tunnels").cloned())
			.unwrap_or(Value::Null),
		mounts,
		timeout_secs,
		source,
		template: string_field(meta, "template"),
		pool: ["pool", "pool_id", "warm_pool"]
			.into_iter()
			.find_map(|key| string_field(meta, key)),
		secret_names,
		available_secret_names,
		version: 1,
		identity_complete: false,
	}
}
fn available_secret_names_of(
	names: Option<&Value>,
	availability: Option<&Value>,
) -> BTreeSet<String> {
	let mut out = string_set(names);
	if let Some(availability) = availability.and_then(Value::as_object) {
		out.extend(
			availability
				.iter()
				.filter_map(|(name, available)| json_truthy(available).then_some(name.clone())),
		);
	}
	out
}

fn string_set(value: Option<&Value>) -> BTreeSet<String> {
	value
		.and_then(Value::as_array)
		.map(|values| {
			values
				.iter()
				.filter_map(Value::as_str)
				.filter(|name| !name.is_empty())
				.map(str::to_owned)
				.collect()
		})
		.unwrap_or_default()
}

fn pid_of(meta: &Map<String, Value>) -> Option<i32> {
	let pid = meta.get("pid")?.as_i64()?;
	i32::try_from(pid).ok()
}

fn timeout_of(meta: &Map<String, Value>) -> Option<f64> {
	meta.get("timeout_secs")?.as_f64()
}

fn string_field(meta: &Map<String, Value>, key: &str) -> Option<String> {
	meta
		.get(key)
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty())
		.map(str::to_owned)
}

fn begin_of(lifecycle: &LifecycleState, disposition: TransitionDisposition) -> TransitionBegin {
	TransitionBegin {
		disposition,
		generation: lifecycle.generation,
		desired: lifecycle.desired.clone(),
		observed: lifecycle.observed.clone(),
		operation: lifecycle.operation.clone(),
	}
}

fn number_field(meta: &Map<String, Value>, key: &str) -> Option<f64> {
	meta
		.get(key)
		.and_then(Value::as_f64)
		.filter(|value| value.is_finite())
}

fn tags_of(meta: &Map<String, Value>) -> HashMap<String, String> {
	let Some(tags) = meta.get("tags").and_then(Value::as_object) else {
		return HashMap::new();
	};
	tags
		.iter()
		.filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
		.collect()
}

fn source_of(meta: &Map<String, Value>) -> Option<String> {
	for key in ["image", "restored_from", "forked_from", "restored_snapshot"] {
		if let Some(source) = meta
			.get(key)
			.and_then(Value::as_str)
			.filter(|source| !source.is_empty())
		{
			return Some(source.to_owned());
		}
	}
	None
}

fn volume_names(meta: &Map<String, Value>) -> Vec<String> {
	let Some(volumes) = meta.get("volumes").and_then(Value::as_object) else {
		return Vec::new();
	};
	let mut writable = HashMap::<String, bool>::new();
	for info in volumes.values() {
		let Some(info) = info.as_object() else {
			continue;
		};
		let Some(name) = info
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		let read_only = info
			.get("read_only")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		writable.insert(name.to_owned(), writable.get(name).copied().unwrap_or(false) || !read_only);
	}
	let mut out = writable
		.into_iter()
		.filter_map(|(name, writable)| writable.then_some(name))
		.collect::<Vec<_>>();
	out.sort();
	out
}

fn detail_object_mut(detail: &mut Value) -> &mut Map<String, Value> {
	if !detail.is_object() {
		*detail = Value::Object(Map::new());
	}
	detail
		.as_object_mut()
		.expect("detail was just initialized as an object")
}

fn json_truthy(value: &Value) -> bool {
	match value {
		Value::Null => false,
		Value::Bool(value) => *value,
		Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
		Value::String(value) => !value.is_empty(),
		Value::Array(value) => !value.is_empty(),
		Value::Object(value) => !value.is_empty(),
	}
}

fn pid_lives(pid: i32) -> bool {
	if pid <= 0 {
		return false;
	}
	// SAFETY: `kill(pid, 0)` probes liveness without delivering a signal and does
	// not retain pointers into Rust memory.
	unsafe { libc::kill(pid, 0) == 0 }
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn view_spreads_detail_then_overrides_canonical_keys() {
		let mut record = VmRecord::new("canonical-id", "canonical-name", "running");
		record.created_at = 10.0;
		record.last_active = 20.0;
		record.timeout = Some(5.0);
		record.tags = HashMap::from([("role".to_owned(), "api".to_owned())]);
		record.detail = json!({
			"id": "bad-id",
			"name": "bad-name",
			"status": "bad-status",
			"created_at": -1,
			"last_active": -1,
			"expires_at": -1,
			"terminated_at": 123,
			"error": "bad-error",
			"tags": {"bad": "tag"},
			"returncode": 7,
			"idempotency_key": "secret",
			"extra": "kept"
		});

		let view = record.view();
		assert_eq!(view["id"], "canonical-id");
		assert_eq!(view["name"], "canonical-name");
		assert_eq!(view["status"], "running");
		assert_eq!(view["created_at"], 10.0);
		assert_eq!(view["last_active"], 20.0);
		assert_eq!(view["expires_at"], 25.0);
		assert_eq!(view["terminated_at"], Value::Null);
		assert_eq!(view["error"], Value::Null);
		assert_eq!(view["tags"], json!({"role": "api"}));
		assert_eq!(view["returncode"], 7);
		assert_eq!(view["extra"], "kept");
		assert!(view.get("idempotency_key").is_none());
	}

	#[test]
	fn rehydrate_reads_python_meta_and_rebuilds_indexes() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		write_fixture(
			&home,
			"dead",
			json!({
				"status": "running",
				"pid": i32::MAX,
				"image": "alpine",
				"timeout_secs": 42,
				"tags": {"role": "test", "ignored": 5},
				"idempotency_key": "idem-dead",
				"volumes": {"/data": {"name": "data"}}
			}),
		);
		write_fixture(
			&home,
			"terminated",
			json!({
				"status": "terminated",
				"pid": i32::MAX,
				"idempotency_key": "idem-terminated"
			}),
		);
		write_fixture(
			&home,
			"live",
			json!({
				"status": "stopped",
				"pid": i32::try_from(std::process::id()).expect("pid fits i32"),
				"forked_from": "base-snap",
				"tap": "tap0",
				"idempotency_key": "idem-live",
				"volumes": {
					"/data": {"name": "data"},
					"/logs": {"name": "logs", "read_only": true}
				}
			}),
		);

		let registry = Registry::new();
		let locks = registry.rehydrate(&home).expect("rehydrate");
		assert_eq!(locks, vec![("live".to_owned(), vec!["data".to_owned()])]);

		let dead = registry.get("dead").expect("dead record");
		assert_eq!(dead.status, "stopped");
		assert_eq!(dead.pid, Some(i32::MAX));
		assert_eq!(dead.source.as_deref(), Some("alpine"));
		assert_eq!(dead.timeout, Some(42.0));
		assert_eq!(dead.tags, HashMap::from([("role".to_owned(), "test".to_owned())]));
		assert_eq!(dead.lifecycle.desired, LifecyclePhase::Stopped);
		assert_eq!(dead.lifecycle.observed, LifecyclePhase::Stopped);
		assert_eq!(registry.find_by_idempotency_key("idem-dead"), Some("dead".to_owned()));
		let dead_meta = read_meta_map(&home.meta_path("dead")).expect("dead meta");
		assert_eq!(dead_meta.get("status"), Some(&json!("stopped")));

		write_fixture(
			&home,
			"suspended",
			json!({
				"status": "suspended",
				"pid": i32::MAX,
				"suspend_recovery_point": "point-1"
			}),
		);
		let suspended_registry = Registry::new();
		suspended_registry
			.rehydrate(&home)
			.expect("rehydrate suspended");
		assert_eq!(
			suspended_registry
				.get("suspended")
				.expect("suspended record")
				.status,
			"suspended"
		);
		assert_eq!(
			read_meta_map(&home.meta_path("suspended")).expect("suspended meta")["status"],
			"suspended"
		);

		let terminated = registry.get("terminated").expect("terminated record");
		assert_eq!(terminated.status, "terminated");
		assert_eq!(registry.find_by_idempotency_key("idem-terminated"), None);

		let live = registry.get("live").expect("live record");
		assert_eq!(live.status, "running");
		assert_eq!(live.source.as_deref(), Some("base-snap"));
		assert_eq!(live.detail["tunnels_lost"], true);
		assert_eq!(registry.find_by_idempotency_key("idem-live"), Some("live".to_owned()));
	}

	#[test]
	fn persisted_identity_survives_registry_rehydrate() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let mut record = VmRecord::new("sb-stable", "sandbox-directory", "stopped");
		record.created_at = 10.0;
		record.last_active = 12.0;
		record.terminated_at = Some(13.0);
		record.error = Some("guest exited".to_owned());

		let registry = Registry::new();
		registry
			.insert_persisted(&home, record)
			.expect("persist record");
		let rehydrated = Registry::new();
		rehydrated.rehydrate(&home).expect("rehydrate");

		let restored = rehydrated.get("sb-stable").expect("stable id");
		assert_eq!(restored.name, "sandbox-directory");
		assert_eq!(restored.created_at, 10.0);
		assert_eq!(restored.last_active, 12.0);
		assert_eq!(restored.terminated_at, Some(13.0));
		assert_eq!(restored.error.as_deref(), Some("guest exited"));
		assert!(rehydrated.get("sandbox-directory").is_none());
	}

	#[test]
	fn desired_write_survives_crash_before_observation() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		let record = VmRecord::new("sandbox", "sandbox", "stopped");
		registry
			.insert_persisted(&home, record)
			.expect("persist initial record");

		let transition = registry
			.begin_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
			.expect("write desired state");
		assert_eq!(transition.generation, StateGeneration(1));

		let restarted = Registry::new();
		restarted.rehydrate(&home).expect("rehydrate");
		let restored = restarted.get("sandbox").expect("restored record");
		assert_eq!(restored.lifecycle.desired, LifecyclePhase::Running);
		assert_eq!(restored.lifecycle.observed, LifecyclePhase::Stopped);
		assert_eq!(restored.lifecycle.generation, StateGeneration(1));
		assert_eq!(restarted.startup_reconciliation_inputs().len(), 1);
	}

	#[test]
	fn stale_generation_cannot_complete_newer_transition() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "stopped"))
			.expect("persist initial record");
		let transition = registry
			.begin_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
			.expect("begin transition");

		assert!(
			registry
				.observe_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Running,)
				.is_err()
		);
		registry
			.observe_transition(&home, "sandbox", transition.generation, LifecyclePhase::Running)
			.expect("complete current generation");
		assert_eq!(
			registry.get("sandbox").expect("record").lifecycle.observed,
			LifecyclePhase::Running
		);
	}

	#[test]
	fn rehydrate_normalizes_observed_state_from_live_pid() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		write_fixture(
			&home,
			"live",
			json!({
				"pid": i32::try_from(std::process::id()).expect("pid fits"),
				"status": "stopped",
				"lifecycle": {
					"desired": "running",
					"observed": "stopped",
					"generation": 7,
					"failure": null
				}
			}),
		);
		let registry = Registry::new();
		registry.rehydrate(&home).expect("rehydrate");
		let record = registry.get("live").expect("record");
		assert_eq!(record.status, "running");
		assert_eq!(record.lifecycle.desired, LifecyclePhase::Running);
		assert_eq!(record.lifecycle.observed, LifecyclePhase::Running);
		assert_eq!(record.lifecycle.generation, StateGeneration(7));
	}

	#[test]
	fn safe_runtime_identity_round_trips_without_secret_values() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let mut record = VmRecord::new("sandbox", "sandbox", "stopped");
		record.runtime_identity = SafeRuntimeIdentity {
			environment:            BTreeMap::from([
				("MODE".to_owned(), "production".to_owned()),
				("TOKEN".to_owned(), "must-not-persist".to_owned()),
			]),
			workdir:                Some("/srv/app".to_owned()),
			network_policy:         Some(json!({"block_network": false})),
			network:                json!({"flavor": "user", "tunnels": {"ssh": 22}}),
			tunnels:                json!({"ssh": 22}),
			mounts:                 vec![json!({"name": "data", "path": "/data"})],
			timeout_secs:           Some(30.0),
			source:                 Some("oci:example/app".to_owned()),
			template:               Some("/templates/app".to_owned()),
			pool:                   Some("warm-app".to_owned()),
			secret_names:           BTreeSet::from(["TOKEN".to_owned()]),
			available_secret_names: BTreeSet::from(["TOKEN".to_owned()]),
			version:                1,
			identity_complete:      true,
		};
		let expected = record.runtime_identity.clone().without_secret_values();
		let registry = Registry::new();
		registry
			.insert_persisted(&home, record)
			.expect("persist record");

		let rehydrated = Registry::new();
		rehydrated.rehydrate(&home).expect("rehydrate");
		let restored = rehydrated.get("sandbox").expect("restored");
		assert_eq!(restored.runtime_identity, expected);
		let persisted = read_meta_map(&home.meta_path("sandbox")).expect("metadata");
		assert!(
			!serde_json::to_string(&persisted)
				.expect("serialize metadata")
				.contains("must-not-persist")
		);
	}

	#[test]
	fn failed_transition_retries_same_target_and_rejects_conflict() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "stopped"))
			.expect("persist initial record");
		let started = registry
			.begin_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
			.expect("start");
		registry
			.fail_transition(&home, "sandbox", started.generation, "vmm unavailable")
			.expect("fail");
		assert!(
			registry
				.begin_transition(&home, "sandbox", started.generation, LifecyclePhase::Suspended,)
				.is_err()
		);
		let retried = registry
			.begin_transition(&home, "sandbox", started.generation, LifecyclePhase::Running)
			.expect("retry matching target");
		assert_eq!(retried.generation, StateGeneration(2));
		assert!(
			registry
				.get("sandbox")
				.expect("record")
				.lifecycle
				.failure
				.is_none()
		);
	}

	#[test]
	fn only_one_concurrent_duplicate_transition_acquires_side_effects() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = std::sync::Arc::new(Registry::new());
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "stopped"))
			.expect("persist initial record");
		let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
		let first_registry = std::sync::Arc::clone(&registry);
		let first_home = home.clone();
		let first_barrier = std::sync::Arc::clone(&barrier);
		let first = std::thread::spawn(move || {
			first_barrier.wait();
			first_registry
				.begin_transition(&first_home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
				.expect("first begin")
				.disposition
		});
		let second_registry = std::sync::Arc::clone(&registry);
		let second_home = home;
		let second_barrier = std::sync::Arc::clone(&barrier);
		let second = std::thread::spawn(move || {
			second_barrier.wait();
			second_registry
				.begin_transition(&second_home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
				.expect("second begin")
				.disposition
		});
		barrier.wait();
		let dispositions = [first.join().expect("first join"), second.join().expect("second join")];
		assert_eq!(
			dispositions
				.iter()
				.filter(|&&disposition| disposition == TransitionDisposition::Acquired)
				.count(),
			1
		);
		assert_eq!(
			dispositions
				.iter()
				.filter(|&&disposition| disposition == TransitionDisposition::Joined)
				.count(),
			1
		);
	}

	#[test]
	fn same_state_rollback_remains_pending_until_observed() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "running"))
			.expect("persist running record");
		let rollback = registry
			.begin_operation(
				&home,
				"sandbox",
				StateGeneration(0),
				LifecyclePhase::Running,
				Some(LifecycleOperation::Rollback { recovery_point: "point-17".to_owned() }),
			)
			.expect("acquire rollback");
		assert_eq!(rollback.disposition, TransitionDisposition::Acquired);
		assert_eq!(rollback.generation, StateGeneration(1));
		let restarted = Registry::new();
		restarted
			.rehydrate(&home)
			.expect("rehydrate pending rollback");
		let inputs = restarted.startup_reconciliation_inputs();
		assert_eq!(inputs.len(), 1);
		assert_eq!(
			inputs[0].lifecycle.operation,
			Some(LifecycleOperation::Rollback { recovery_point: "point-17".to_owned() })
		);
		restarted
			.observe_transition(&home, "sandbox", rollback.generation, LifecyclePhase::Running)
			.expect("complete rollback");
		assert!(restarted.startup_reconciliation_inputs().is_empty());
		assert!(
			restarted
				.get("sandbox")
				.expect("record")
				.lifecycle
				.operation
				.is_none()
		);
	}

	#[test]
	fn cancellation_converges_pending_suspend_and_rejects_stale_generation() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "running"))
			.expect("persist running record");
		let suspend = registry
			.begin_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Suspended)
			.expect("begin suspend");
		assert_eq!(suspend.disposition, TransitionDisposition::Acquired);
		let cancelled = registry
			.cancel_transition(
				&home,
				"sandbox",
				suspend.generation,
				LifecyclePhase::Running,
				"recovery publication failed",
			)
			.expect("cancel suspend");
		assert_eq!(cancelled.desired, LifecyclePhase::Running);
		assert_eq!(cancelled.observed, LifecyclePhase::Running);
		assert!(registry.startup_reconciliation_inputs().is_empty());
		assert_eq!(
			registry.get("sandbox").expect("record").error.as_deref(),
			Some("recovery publication failed")
		);
		registry
			.cancel_transition(
				&home,
				"sandbox",
				suspend.generation,
				LifecyclePhase::Running,
				"recovery publication failed",
			)
			.expect("duplicate cancellation");
		assert!(
			registry
				.cancel_transition(
					&home,
					"sandbox",
					StateGeneration(0),
					LifecyclePhase::Running,
					"stale",
				)
				.is_err()
		);
	}

	#[test]
	fn resuming_pending_transition_fences_crashed_generation() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		registry
			.insert_persisted(&home, VmRecord::new("sandbox", "sandbox", "stopped"))
			.expect("persist record");
		let original = registry
			.begin_transition(&home, "sandbox", StateGeneration(0), LifecyclePhase::Running)
			.expect("begin");
		let resumed = registry
			.resume_transition(&home, "sandbox", original.generation)
			.expect("resume");
		assert_eq!(resumed.disposition, TransitionDisposition::Acquired);
		assert_eq!(resumed.generation, StateGeneration(2));
		assert!(
			registry
				.observe_transition(&home, "sandbox", original.generation, LifecyclePhase::Running)
				.is_err()
		);
	}

	#[test]
	fn idle_policy_update_is_atomic_and_survives_rehydrate() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let home = Home::new(tmp.path());
		let registry = Registry::new();
		write_fixture(
			&home,
			"sandbox",
			json!({
				"relaunch_recipe": {
					"params": {
						"idle_timeout_secs": 300.0,
					}
				}
			}),
		);
		let record = VmRecord::new("sandbox", "sandbox", "stopped");
		registry
			.insert_persisted(&home, record)
			.expect("persist record");

		let updated = registry
			.set_idle_timeout_persisted(&home, "sandbox", 12.5, 42.0)
			.expect("set idle policy");
		assert_eq!(updated.last_network_active, 42.0);
		assert_eq!(updated.detail["idle_timeout_secs"], 12.5);
		assert_eq!(updated.detail["relaunch_recipe"]["params"]["idle_timeout_secs"], 12.5);

		let rehydrated = Registry::new();
		rehydrated.rehydrate(&home).expect("rehydrate");
		let restored = rehydrated.get("sandbox").expect("sandbox");
		assert_eq!(restored.last_network_active, 42.0);
		assert_eq!(restored.detail["idle_timeout_secs"], 12.5);
		assert_eq!(restored.detail["relaunch_recipe"]["params"]["idle_timeout_secs"], 12.5);
	}

	fn write_fixture(home: &Home, name: &str, value: Value) {
		let path = home.meta_path(name);
		fs::create_dir_all(path.parent().expect("meta parent")).expect("mkdir");
		fs::write(path, serde_json::to_string_pretty(&value).expect("fixture json"))
			.expect("write fixture");
	}
}
