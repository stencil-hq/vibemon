//! Durable mesh create-record replication.
//!
//! Python stores one JSON file per acknowledged create at
//! `$VMON_HOME/records/<sid>.json`.  The `params.secrets` field is never
//! written: it is split into a memory-only side map and reattached only while
//! this process still has it.  That invariant is load-bearing for HA rerun and
//! record anti-entropy, so this module keeps the same split at the type
//! boundary.

use std::{
	collections::BTreeMap,
	fmt, fs,
	io::ErrorKind,
	os::unix::fs::DirBuilderExt,
	path::{Path, PathBuf},
	process,
	time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::{EngineError, Result, home::Home};

/// Mesh HA tiers accepted by Python's `CreateRecord.from_wire`.
pub const ALLOWED_HA: &[&str] = &["off", "async", "rerun", "async+rerun"];

const RECORD_DIR_MODE: u32 = 0o700;
pub(crate) const HANDOFF_TARGET_FIELD: &str = "_mesh_migration_handoff_target";
pub(crate) const HANDOFF_TOKEN_FIELD: &str = "_mesh_migration_handoff_token";
pub(crate) const HANDOFF_STATE_FIELD: &str = "_mesh_migration_handoff_state";
pub(crate) const HANDOFF_SOURCE_FIELD: &str = "_mesh_migration_handoff_source";
pub(crate) const HANDOFF_CLAIMED_FIELD: &str = "_mesh_migration_handoff_claimed";
const MIGRATION_COMMITTED_FIELD: &str = "_mesh_migration_committed";

pub type Params = JsonMap<String, JsonValue>;

/// Return the restart policy implied by a durability tier.
pub fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

/// Required create-record acknowledgements for an expected mesh size.
///
/// The caller supplies `live_peer_count` because the two-node rule is weaker
/// than quorum by design: expected <= 2 requires local + every currently-live
/// peer; expected >= 3 requires strict majority.
pub const fn required_record_acks(expected_members: usize, live_peer_count: usize) -> usize {
	if expected_members <= 2 {
		1 + live_peer_count
	} else {
		expected_members / 2 + 1
	}
}

/// Error code used by routes when synchronous create-record replication misses
/// its required acknowledgement count.
pub const RECORD_UNREPLICATED_CODE: &str = "record_unreplicated";

/// Synchronous mesh metadata for an acknowledged create.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateRecord {
	pub sid:               String,
	pub params:            Params,
	pub owner:             String,
	pub epoch:             i64,
	/// Stable sandbox incarnation; owner epoch may advance during handoff.
	pub incarnation_epoch: i64,
	pub idempotency_key:   String,
	pub ha:                String,
	pub restart_policy:    String,
	pub created_at:        f64,
}

impl CreateRecord {
	#[allow(clippy::too_many_arguments, reason = "record construction mirrors the mesh wire schema")]
	pub fn new(
		sid: impl Into<String>,
		params: Params,
		owner: impl Into<String>,
		epoch: i64,
		idempotency_key: impl Into<String>,
		ha: impl Into<String>,
		restart_policy: impl Into<String>,
		created_at: f64,
	) -> Result<Self> {
		let sid = non_empty_string("record sid is required", sid.into())?;
		let owner = non_empty_string("record owner is required", owner.into())?;
		let idempotency_key =
			non_empty_string("record idempotency_key is required", idempotency_key.into())?;
		let ha = ha.into();
		if !ALLOWED_HA.contains(&ha.as_str()) {
			return Err(EngineError::invalid(format!("invalid ha tier {ha:?}")));
		}
		let restart_policy = restart_policy.into();
		let created_at = if created_at.is_finite() {
			created_at
		} else {
			unix_now()
		};
		Ok(Self {
			sid,
			params,
			owner,
			epoch,
			incarnation_epoch: epoch,
			idempotency_key,
			ha,
			restart_policy,
			created_at,
		})
	}

	/// Return a JSON-ready representation for disk and peer replication.
	pub fn to_wire(&self) -> JsonValue {
		JsonValue::Object(self.to_wire_map())
	}

	pub fn to_wire_map(&self) -> Params {
		let mut out = Params::new();
		out.insert("sid".to_owned(), JsonValue::String(self.sid.clone()));
		out.insert("params".to_owned(), JsonValue::Object(self.params.clone()));
		out.insert("owner".to_owned(), JsonValue::String(self.owner.clone()));
		out.insert("epoch".to_owned(), JsonValue::Number(JsonNumber::from(self.epoch)));
		out.insert(
			"incarnation_epoch".to_owned(),
			JsonValue::Number(JsonNumber::from(self.incarnation_epoch)),
		);
		out.insert("idempotency_key".to_owned(), JsonValue::String(self.idempotency_key.clone()));
		out.insert("ha".to_owned(), JsonValue::String(self.ha.clone()));
		out.insert("restart_policy".to_owned(), JsonValue::String(self.restart_policy.clone()));
		out.insert("created_at".to_owned(), json_f64(self.created_at));
		out
	}

	/// Build and validate a record from persisted or peer-provided data.
	pub fn from_wire(data: &JsonValue) -> Result<Self> {
		let object = data
			.as_object()
			.ok_or_else(|| EngineError::invalid("record must be an object"))?;
		Self::from_wire_map(object)
	}

	pub fn from_wire_map(data: &Params) -> Result<Self> {
		let sid = required_string(data, "sid", "record sid is required")?;
		let params = data
			.get("params")
			.and_then(JsonValue::as_object)
			.cloned()
			.ok_or_else(|| EngineError::invalid("record params must be an object"))?;
		let owner = required_string(data, "owner", "record owner is required")?;
		let idempotency_key =
			required_string(data, "idempotency_key", "record idempotency_key is required")?;
		let ha = optional_string(data.get("ha"), "off");
		let restart_policy = optional_string(data.get("restart_policy"), restart_policy_for_ha(&ha));
		let epoch = data.get("epoch").and_then(JsonValue::as_i64).unwrap_or(0);
		let mut record = Self::new(
			sid,
			params,
			owner,
			epoch,
			idempotency_key,
			ha,
			restart_policy,
			data
				.get("created_at")
				.and_then(JsonValue::as_f64)
				.unwrap_or_default(),
		)?;
		record.incarnation_epoch = data
			.get("incarnation_epoch")
			.and_then(JsonValue::as_i64)
			.unwrap_or(epoch);
		Ok(record)
	}
}

/// Build a durable create record from normalized request params.
pub fn make_create_record(
	sid: impl Into<String>,
	owner: impl Into<String>,
	epoch: i64,
	idempotency_key: impl Into<String>,
	mut params: Params,
	kind: impl Into<String>,
) -> Result<CreateRecord> {
	params.remove("idempotency_key");
	params.insert("_kind".to_owned(), JsonValue::String(kind.into()));
	let ha = optional_string(params.get("ha"), "off");
	let restart_policy = optional_string(params.get("restart_policy"), restart_policy_for_ha(&ha));
	params.insert("restart_policy".to_owned(), JsonValue::String(restart_policy.clone()));
	CreateRecord::new(sid, params, owner, epoch, idempotency_key, ha, restart_policy, unix_now())
}

/// Persist create records while keeping secret env material memory-only.
pub struct RecordStore {
	root:  PathBuf,
	inner: RwLock<RecordInner>,
}

#[derive(Default)]
struct RecordInner {
	meta:    BTreeMap<String, CreateRecord>,
	secrets: BTreeMap<String, JsonValue>,
}

impl fmt::Debug for RecordStore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RecordStore")
			.field("root", &self.root)
			.finish_non_exhaustive()
	}
}

impl Default for RecordStore {
	fn default() -> Self {
		Self::for_home(Home::default())
	}
}

impl RecordStore {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into(), inner: RwLock::new(RecordInner::default()) }
	}

	pub fn for_home(home: Home) -> Self {
		Self::new(home.records_dir())
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	/// Load persisted records, ignoring corrupt or incomplete files.
	pub fn load(&self) {
		let entries = fs::read_dir(&self.root);
		let mut inner = self.inner.write();
		inner.meta.clear();
		inner.secrets.clear();
		let Ok(entries) = entries else {
			return;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
				continue;
			}
			let Ok(text) = fs::read_to_string(&path) else {
				continue;
			};
			let Ok(value) = serde_json::from_str::<JsonValue>(&text) else {
				continue;
			};
			let Ok(mut record) = CreateRecord::from_wire(&value) else {
				continue;
			};
			let (clean, _) = split_secrets(&record.params);
			record.params = clean;
			inner.meta.insert(record.sid.clone(), record);
		}
	}

	/// Persist or replace a create record atomically.
	pub fn put(&self, mut record: CreateRecord) -> Result<()> {
		let (clean, secrets) = split_secrets(&record.params);
		record.params = clean;
		let mut inner = self.inner.write();
		write_record(&self.root, &record.sid, &record.to_wire())?;
		store_secrets(&mut inner, &record.sid, secrets);
		inner.meta.insert(record.sid.clone(), record);
		Ok(())
	}

	/// Apply the `/v1/mesh/record/put` acceptance rule: accept iff no local
	/// record exists or the incoming epoch is at least the existing epoch.
	pub fn put_if_newer(&self, mut record: CreateRecord) -> Result<bool> {
		let mut inner = self.inner.write();
		let existing = inner.meta.get(&record.sid).cloned();
		if existing
			.as_ref()
			.is_some_and(|existing| record.epoch < existing.epoch)
		{
			return Ok(false);
		}
		let same_lineage = existing
			.as_ref()
			.is_some_and(|existing| record.owner == existing.owner && record.epoch == existing.epoch);
		clear_handoff(&mut record.params);
		if same_lineage && let Some(existing) = &existing {
			if migration_committed(&existing.params) && !migration_committed(&record.params) {
				record.params.clone_from(&existing.params);
			} else {
				for key in handoff_fields() {
					if let Some(value) = existing.params.get(key) {
						record.params.insert(key.to_owned(), value.clone());
					}
				}
			}
		}
		let (params, secrets) = split_secrets(&record.params);
		record.params = params;
		self.persist_meta(&mut inner, &record)?;
		if let Some(secrets) = secrets {
			inner.secrets.insert(record.sid.clone(), secrets);
		} else if !same_lineage {
			inner.secrets.remove(&record.sid);
		}
		Ok(true)
	}

	/// Return a create record, reattaching in-memory secrets if still present.
	pub fn get(&self, sid: &str) -> Option<CreateRecord> {
		let inner = self.inner.read();
		let mut record = inner.meta.get(sid)?.clone();
		if let Some(secrets) = inner.secrets.get(sid) {
			record.params.insert("secrets".to_owned(), secrets.clone());
		}
		Some(record)
	}

	/// Remember secret material for a durable record without writing it to disk.
	pub(crate) fn remember_secrets(&self, sid: &str, secrets: Option<JsonValue>) {
		store_secrets(&mut self.inner.write(), sid, secrets);
	}

	/// Reattach process-local secret material to a record loaded from durable
	/// storage.
	pub(crate) fn attach_secrets(&self, record: &mut CreateRecord) {
		if let Some(secrets) = self.inner.read().secrets.get(&record.sid) {
			record.params.insert("secrets".to_owned(), secrets.clone());
		}
	}

	/// Forget process-local secret material after its durable record is removed.
	pub(crate) fn forget_secrets(&self, sid: &str) {
		self.inner.write().secrets.remove(sid);
	}

	/// Update a record's authoritative owner and persist the replacement.
	pub fn update_owner(
		&self,
		sid: &str,
		owner: impl Into<String>,
		epoch: i64,
	) -> Result<Option<CreateRecord>> {
		let Some(mut record) = self.get(sid) else {
			return Ok(None);
		};
		record.owner = owner.into();
		record.epoch = epoch;
		self.put(record.clone())?;
		Ok(Some(record))
	}

	/// Claim one observed lineage as a durable, non-serving restore intent.
	pub(crate) fn claim_restore_pending(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		new_owner: &str,
		pending: &JsonValue,
	) -> Result<CreateRecord> {
		if sid.is_empty()
			|| expected_owner.is_empty()
			|| new_owner.is_empty()
			|| expected_epoch < 0
			|| !pending.is_object()
		{
			return Err(EngineError::invalid("invalid pending restore claim"));
		}
		let mut inner = self.inner.write();
		let mut record =
			inner.meta.get(sid).cloned().ok_or_else(|| {
				EngineError::not_found(format!("sandbox {sid} has no ownership record"))
			})?;
		if record.owner == new_owner && record.params.get("_mesh_restore_pending") == Some(pending) {
			return Ok(with_secrets(&inner, record));
		}
		if record.owner != expected_owner || record.epoch != expected_epoch {
			return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
		}
		if let Some(existing) = record.params.get("_mesh_restore_pending")
			&& existing != pending
		{
			return Err(EngineError::invalid(format!(
				"pending restore intent for {sid} conflicts with the claimed lineage"
			)));
		}
		record.epoch = record
			.epoch
			.checked_add(1)
			.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
		new_owner.clone_into(&mut record.owner);
		record
			.params
			.insert("_mesh_restore_pending".to_owned(), pending.clone());
		self.persist_meta(&mut inner, &record)?;
		Ok(with_secrets(&inner, record))
	}

	/// Verify that one exact local restore claim remains pending and
	/// non-serving.
	pub(crate) fn renew_restore_pending(&self, sid: &str, owner: &str, epoch: i64) -> bool {
		self.inner.read().meta.get(sid).is_some_and(|record| {
			record.owner == owner
				&& record.epoch == epoch
				&& record.params.contains_key("_mesh_restore_pending")
		})
	}

	/// Promote an exact pending claim by persisting restored parameters before
	/// routing it.
	pub(crate) fn commit_restore_pending(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		params: Params,
		ha: &str,
		restart_policy: &str,
	) -> Result<CreateRecord> {
		let (mut params, secrets) = split_secrets(&params);
		for field in [
			"_mesh_restore_pending",
			"_mesh_rollback_pending",
			"restore_pending",
			"rollback_pending",
			"_mesh_lifecycle_operation",
		] {
			params.remove(field);
		}
		let mut inner = self.inner.write();
		let mut record =
			inner.meta.get(sid).cloned().ok_or_else(|| {
				EngineError::not_found(format!("sandbox {sid} has no ownership record"))
			})?;
		if record.owner != owner || record.epoch != epoch {
			return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
		}
		record.params = params;
		ha.clone_into(&mut record.ha);
		restart_policy.clone_into(&mut record.restart_policy);
		self.persist_meta(&mut inner, &record)?;
		store_secrets(&mut inner, sid, secrets);
		Ok(with_secrets(&inner, record))
	}

	/// Release only an exact pending claim while advancing beyond every observed
	/// epoch.
	pub(crate) fn release_restore_claim(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		previous_owner: &str,
		previous_epoch: i64,
	) -> Result<Option<CreateRecord>> {
		if owner.is_empty() || previous_owner.is_empty() || epoch < 0 || previous_epoch < 0 {
			return Err(EngineError::invalid("invalid restore claim release"));
		}
		let restored_epoch = previous_epoch
			.max(epoch)
			.checked_add(1)
			.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
		let mut inner = self.inner.write();
		let Some(mut record) = inner.meta.get(sid).cloned() else {
			return Ok(None);
		};
		if record.owner != owner
			|| record.epoch != epoch
			|| !record.params.contains_key("_mesh_restore_pending")
		{
			return Ok(None);
		}
		record.params.remove("_mesh_restore_pending");
		previous_owner.clone_into(&mut record.owner);
		record.epoch = restored_epoch;
		self.persist_meta(&mut inner, &record)?;
		Ok(Some(with_secrets(&inner, record)))
	}

	/// Persist one exact source-to-target migration authorization.
	pub(crate) fn begin_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		epoch: i64,
		target: &str,
		token: &str,
	) -> Result<()> {
		validate_handoff(sid, source, epoch, target, token)?;
		let mut inner = self.inner.write();
		let mut record =
			inner.meta.get(sid).cloned().ok_or_else(|| {
				EngineError::not_found(format!("sandbox {sid} has no ownership record"))
			})?;
		let target_epoch = epoch
			.checked_add(1)
			.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
		if record.owner == target
			&& record.epoch == target_epoch
			&& handoff_matches(&record.params, source, target, token)
			&& handoff_state(&record.params) == Some("active")
			&& handoff_claimed(&record.params)
		{
			return Ok(());
		}
		if record.owner != source || record.epoch != epoch {
			return Err(EngineError::invalid(format!(
				"migration handoff for {sid} lost source lease"
			)));
		}
		if handoff_present(&record.params) {
			if handoff_matches(&record.params, source, target, token)
				&& handoff_state(&record.params) == Some("active")
			{
				return Ok(());
			}
			return Err(EngineError::busy(format!("migration handoff for {sid} already exists")));
		}
		set_handoff(&mut record.params, source, target, token, "active", false);
		self.persist_meta(&mut inner, &record)
	}

	/// Mark the exact source authorization consumed before the target can run.
	#[allow(clippy::too_many_arguments, reason = "the exact handoff lineage is the fencing key")]
	pub(crate) fn confirm_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		source_epoch: i64,
		target: &str,
		target_epoch: i64,
		token: &str,
	) -> Result<()> {
		validate_handoff(sid, source, source_epoch, target, token)?;
		if source_epoch.checked_add(1) != Some(target_epoch) {
			return Err(EngineError::invalid("migration target epoch is not the source successor"));
		}
		let mut inner = self.inner.write();
		let mut record =
			inner.meta.get(sid).cloned().ok_or_else(|| {
				EngineError::not_found(format!("sandbox {sid} has no ownership record"))
			})?;
		if record.owner == target
			&& record.epoch == target_epoch
			&& handoff_matches(&record.params, source, target, token)
			&& handoff_claimed(&record.params)
		{
			return Ok(());
		}
		if record.owner != source
			|| record.epoch != source_epoch
			|| !handoff_matches(&record.params, source, target, token)
			|| handoff_state(&record.params) != Some("active")
		{
			return Err(EngineError::invalid(format!("migration handoff for {sid} was fenced")));
		}
		record
			.params
			.insert(HANDOFF_CLAIMED_FIELD.to_owned(), JsonValue::Bool(true));
		target.clone_into(&mut record.owner);
		record.epoch = target_epoch;
		self.persist_meta(&mut inner, &record)
	}

	/// Install a target intent only for the observed source generation.
	pub(crate) fn claim_migration_handoff(
		&self,
		source: &str,
		source_epoch: i64,
		target: &str,
		token: &str,
		mut intent: CreateRecord,
	) -> Result<CreateRecord> {
		validate_handoff(&intent.sid, source, source_epoch, target, token)?;
		let target_epoch = source_epoch
			.checked_add(1)
			.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
		if intent.owner != target || intent.epoch != target_epoch {
			return Err(EngineError::invalid("migration intent is not the source successor"));
		}
		let mut inner = self.inner.write();
		let current = inner.meta.get(&intent.sid).cloned().ok_or_else(|| {
			EngineError::not_found(format!("sandbox {} has no ownership record", intent.sid))
		})?;
		if current.owner == target
			&& current.epoch == target_epoch
			&& handoff_matches(&current.params, source, target, token)
			&& handoff_claimed(&current.params)
		{
			return Ok(with_secrets(&inner, current));
		}
		if current.owner != source || current.epoch != source_epoch {
			return Err(EngineError::invalid(format!(
				"migration handoff for {} was fenced",
				intent.sid
			)));
		}
		if handoff_present(&current.params)
			&& (!handoff_matches(&current.params, source, target, token)
				|| handoff_state(&current.params) != Some("active"))
		{
			return Err(EngineError::invalid(format!(
				"migration handoff for {} was fenced",
				intent.sid
			)));
		}
		set_handoff(&mut intent.params, source, target, token, "active", true);
		let (params, secrets) = split_secrets(&intent.params);
		intent.params = params;
		store_secrets(&mut inner, &intent.sid, secrets);
		self.persist_meta(&mut inner, &intent)?;
		Ok(with_secrets(&inner, intent))
	}

	/// Revoke only an exact, unclaimed authorization and advance the source
	/// epoch.
	pub(crate) fn abort_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		source_epoch: i64,
		target: &str,
		token: &str,
	) -> Result<Option<CreateRecord>> {
		validate_handoff(sid, source, source_epoch, target, token)?;
		let fresh_epoch = source_epoch
			.checked_add(1)
			.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
		let mut inner = self.inner.write();
		let Some(mut record) = inner.meta.get(sid).cloned() else {
			return Ok(None);
		};
		if record.owner == source
			&& record.epoch == fresh_epoch
			&& handoff_matches(&record.params, source, target, token)
			&& handoff_state(&record.params) == Some("aborted")
		{
			return Ok(Some(with_secrets(&inner, record)));
		}
		if record.owner != source
			|| record.epoch != source_epoch
			|| !handoff_matches(&record.params, source, target, token)
			|| handoff_state(&record.params) != Some("active")
			|| handoff_claimed(&record.params)
		{
			return Ok(None);
		}
		record.epoch = fresh_epoch;
		record
			.params
			.insert(HANDOFF_STATE_FIELD.to_owned(), JsonValue::String("aborted".to_owned()));
		self.persist_meta(&mut inner, &record)?;
		Ok(Some(with_secrets(&inner, record)))
	}

	/// Clear an aborted handoff only for its exact token and fresh source epoch.
	pub(crate) fn complete_migration_abort(
		&self,
		sid: &str,
		token: &str,
		owner: &str,
		fresh_epoch: i64,
	) -> Result<CreateRecord> {
		let mut inner = self.inner.write();
		let mut record = inner.meta.get(sid).cloned().ok_or_else(|| {
			EngineError::busy(format!("migration abort completion for {sid} fenced"))
		})?;
		if record.owner != owner
			|| record.epoch != fresh_epoch
			|| handoff_state(&record.params) != Some("aborted")
			|| record
				.params
				.get(HANDOFF_TOKEN_FIELD)
				.and_then(JsonValue::as_str)
				!= Some(token)
		{
			return Err(EngineError::busy(format!("migration abort completion for {sid} fenced")));
		}
		clear_handoff(&mut record.params);
		self.persist_meta(&mut inner, &record)?;
		Ok(with_secrets(&inner, record))
	}

	fn persist_meta(&self, inner: &mut RecordInner, record: &CreateRecord) -> Result<()> {
		write_record(&self.root, &record.sid, &record.to_wire())?;
		inner.meta.insert(record.sid.clone(), record.clone());
		Ok(())
	}

	/// Return all valid records in stable sandbox-id order.
	#[allow(
		clippy::needless_collect,
		reason = "collecting keys releases the metadata read lock before each record load"
	)]
	pub fn list(&self) -> Vec<CreateRecord> {
		let keys = self.inner.read().meta.keys().cloned().collect::<Vec<_>>();
		keys.into_iter().filter_map(|sid| self.get(&sid)).collect()
	}

	/// Remove a create record and any memory-only secret material.
	pub fn remove(&self, sid: &str) -> Result<()> {
		let mut inner = self.inner.write();
		match fs::remove_file(self.root.join(format!("{sid}.json"))) {
			Ok(()) => {},
			Err(err) if err.kind() == ErrorKind::NotFound => {},
			Err(err) => return Err(err.into()),
		}
		inner.meta.remove(sid);
		inner.secrets.remove(sid);
		Ok(())
	}

	pub fn drop_record(&self, sid: &str) -> Result<()> {
		self.remove(sid)
	}

	pub fn contains(&self, sid: &str) -> bool {
		self.inner.read().meta.contains_key(sid)
	}
}

fn validate_handoff(sid: &str, source: &str, epoch: i64, target: &str, token: &str) -> Result<()> {
	if sid.is_empty() || source.is_empty() || target.is_empty() || token.is_empty() || epoch < 0 {
		Err(EngineError::invalid("invalid migration handoff"))
	} else {
		Ok(())
	}
}

const fn handoff_fields() -> [&'static str; 5] {
	[
		HANDOFF_TARGET_FIELD,
		HANDOFF_TOKEN_FIELD,
		HANDOFF_STATE_FIELD,
		HANDOFF_SOURCE_FIELD,
		HANDOFF_CLAIMED_FIELD,
	]
}

fn migration_committed(params: &Params) -> bool {
	params
		.get(MIGRATION_COMMITTED_FIELD)
		.and_then(JsonValue::as_bool)
		== Some(true)
}
fn handoff_present(params: &Params) -> bool {
	handoff_fields()
		.into_iter()
		.any(|key| params.contains_key(key))
}

fn handoff_matches(params: &Params, source: &str, target: &str, token: &str) -> bool {
	params.get(HANDOFF_SOURCE_FIELD).and_then(JsonValue::as_str) == Some(source)
		&& params.get(HANDOFF_TARGET_FIELD).and_then(JsonValue::as_str) == Some(target)
		&& params.get(HANDOFF_TOKEN_FIELD).and_then(JsonValue::as_str) == Some(token)
}

fn handoff_state(params: &Params) -> Option<&str> {
	params.get(HANDOFF_STATE_FIELD).and_then(JsonValue::as_str)
}

fn handoff_claimed(params: &Params) -> bool {
	params
		.get(HANDOFF_CLAIMED_FIELD)
		.and_then(JsonValue::as_bool)
		== Some(true)
}

fn set_handoff(
	params: &mut Params,
	source: &str,
	target: &str,
	token: &str,
	state: &str,
	claimed: bool,
) {
	params.insert(HANDOFF_SOURCE_FIELD.to_owned(), JsonValue::String(source.to_owned()));
	params.insert(HANDOFF_TARGET_FIELD.to_owned(), JsonValue::String(target.to_owned()));
	params.insert(HANDOFF_TOKEN_FIELD.to_owned(), JsonValue::String(token.to_owned()));
	params.insert(HANDOFF_STATE_FIELD.to_owned(), JsonValue::String(state.to_owned()));
	params.insert(HANDOFF_CLAIMED_FIELD.to_owned(), JsonValue::Bool(claimed));
}

fn clear_handoff(params: &mut Params) {
	for key in handoff_fields() {
		params.remove(key);
	}
}

fn with_secrets(inner: &RecordInner, mut record: CreateRecord) -> CreateRecord {
	if let Some(secrets) = inner.secrets.get(&record.sid) {
		record.params.insert("secrets".to_owned(), secrets.clone());
	}
	record
}

fn store_secrets(inner: &mut RecordInner, sid: &str, secrets: Option<JsonValue>) {
	if let Some(secrets) = secrets {
		inner.secrets.insert(sid.to_owned(), secrets);
	} else {
		inner.secrets.remove(sid);
	}
}

/// Remove `params.secrets` for durable storage and return the memory-only
/// value.
pub fn split_secrets(params: &Params) -> (Params, Option<JsonValue>) {
	let mut clean = params.clone();
	let secrets = match clean.remove("secrets") {
		Some(JsonValue::Null) | None => None,
		Some(value) => Some(value),
	};
	(clean, secrets)
}

fn write_record(root: &Path, sid: &str, value: &JsonValue) -> Result<()> {
	ensure_private_dir(root)?;
	let path = root.join(format!("{sid}.json"));
	let tmp = temp_path_for(&path);
	let bytes = serde_json::to_vec(value)?;
	fs::write(&tmp, [&bytes[..], b"\n"].concat())?;
	fs::rename(&tmp, path)?;
	Ok(())
}

fn required_string(data: &Params, key: &str, message: &'static str) -> Result<String> {
	let value = data
		.get(key)
		.and_then(JsonValue::as_str)
		.unwrap_or_default();
	non_empty_string(message, value.to_owned())
}

fn non_empty_string(message: &'static str, value: String) -> Result<String> {
	if value.is_empty() {
		Err(EngineError::invalid(message))
	} else {
		Ok(value)
	}
}

fn optional_string(value: Option<&JsonValue>, default: &str) -> String {
	match value {
		Some(JsonValue::String(text)) if !text.is_empty() => text.clone(),
		Some(JsonValue::Number(number)) => number.to_string(),
		Some(JsonValue::Bool(flag)) if *flag => "true".to_owned(),
		_ => default.to_owned(),
	}
}

fn json_f64(value: f64) -> JsonValue {
	JsonValue::Number(JsonNumber::from_f64(value).unwrap_or_else(|| JsonNumber::from(0)))
}

fn unix_now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| {
			duration.as_secs() as f64 + f64::from(duration.subsec_nanos()) / 1_000_000_000.0
		})
}

fn ensure_private_dir(path: &Path) -> Result<()> {
	let mut builder = fs::DirBuilder::new();
	builder.recursive(true).mode(RECORD_DIR_MODE);
	builder.create(path).or_else(|err| {
		if err.kind() == ErrorKind::AlreadyExists {
			Ok(())
		} else {
			Err(err)
		}
	})?;
	Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
	let file_name = path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("record.json");
	path.with_file_name(format!(".{file_name}.{}.tmp", process::id()))
}

#[cfg(test)]
mod tests {
	use super::*;

	const SID: &str = "sandbox-1";
	const SOURCE: &str = "source";
	const TARGET: &str = "target";
	const TOKEN: &str = "migration-token";
	const SOURCE_EPOCH: i64 = 7;
	const TARGET_EPOCH: i64 = 8;

	fn source_record() -> CreateRecord {
		make_create_record(SID, SOURCE, SOURCE_EPOCH, "create-token", Params::new(), "sandbox")
			.unwrap()
	}

	fn target_intent() -> CreateRecord {
		let mut record = source_record();
		record.owner = TARGET.to_owned();
		record.epoch = TARGET_EPOCH;
		record
			.params
			.insert("_mesh_migration_token".to_owned(), JsonValue::String(TOKEN.to_owned()));
		record
	}

	#[test]
	fn confirmed_handoff_survives_restart_and_fences_source_abort() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		store.put(source_record()).unwrap();
		store
			.begin_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
			.unwrap();
		drop(store);

		let restarted = RecordStore::new(temp.path());
		restarted.load();
		restarted
			.begin_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
			.unwrap();
		restarted
			.confirm_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TARGET_EPOCH, TOKEN)
			.unwrap();
		drop(restarted);

		let recovered = RecordStore::new(temp.path());
		recovered.load();
		let record = recovered.get(SID).unwrap();
		assert_eq!((record.owner.as_str(), record.epoch), (TARGET, TARGET_EPOCH));
		assert!(handoff_claimed(&record.params));
		assert!(
			recovered
				.abort_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn target_claim_survives_restart_and_is_idempotent() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		store.put(source_record()).unwrap();
		store
			.begin_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
			.unwrap();
		store
			.claim_migration_handoff(SOURCE, SOURCE_EPOCH, TARGET, TOKEN, target_intent())
			.unwrap();
		drop(store);

		let recovered = RecordStore::new(temp.path());
		recovered.load();
		recovered
			.begin_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
			.unwrap();
		let record = recovered
			.claim_migration_handoff(SOURCE, SOURCE_EPOCH, TARGET, TOKEN, target_intent())
			.unwrap();
		assert_eq!((record.owner.as_str(), record.epoch), (TARGET, TARGET_EPOCH));
		assert!(handoff_claimed(&record.params));
	}

	#[test]
	fn equal_epoch_replication_preserves_only_local_handoff_authority() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		let source = source_record();
		store.put(source.clone()).unwrap();

		let mut replicated = source.clone();
		set_handoff(&mut replicated.params, "attacker", "other", "forged", "active", true);
		assert!(store.put_if_newer(replicated).unwrap());
		assert!(!handoff_present(&store.get(SID).unwrap().params));

		store
			.begin_migration_handoff(SID, SOURCE, SOURCE_EPOCH, TARGET, TOKEN)
			.unwrap();
		let mut replicated = source;
		set_handoff(&mut replicated.params, "attacker", "other", "forged", "active", true);
		assert!(store.put_if_newer(replicated).unwrap());
		let record = store.get(SID).unwrap();
		assert!(handoff_matches(&record.params, SOURCE, TARGET, TOKEN));
		assert!(!handoff_claimed(&record.params));
	}

	#[test]
	fn equal_epoch_replication_cannot_rollback_committed_migration() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		let mut committed = target_intent();
		committed
			.params
			.insert(MIGRATION_COMMITTED_FIELD.to_owned(), JsonValue::Bool(true));
		committed
			.params
			.insert("rootfs".to_owned(), JsonValue::String("target-rootfs".to_owned()));
		store.put(committed).unwrap();

		let mut stale = target_intent();
		stale
			.params
			.insert("rootfs".to_owned(), JsonValue::String("source-rootfs".to_owned()));
		assert!(store.put_if_newer(stale).unwrap());

		let record = store.get(SID).unwrap();
		assert!(migration_committed(&record.params));
		assert_eq!(record.params.get("rootfs").and_then(JsonValue::as_str), Some("target-rootfs"));
	}

	#[test]
	fn pending_restore_claim_is_durable_and_releases_to_a_fresh_epoch() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		store.put(source_record()).unwrap();
		let pending = serde_json::json!({
			"kind": "replica",
			"source_owner": SOURCE,
			"source_epoch": SOURCE_EPOCH,
		});
		let claimed = store
			.claim_restore_pending(SID, SOURCE, SOURCE_EPOCH, TARGET, &pending)
			.unwrap();
		assert_eq!((claimed.owner.as_str(), claimed.epoch), (TARGET, TARGET_EPOCH));
		assert!(store.renew_restore_pending(SID, TARGET, TARGET_EPOCH));
		drop(store);

		let recovered = RecordStore::new(temp.path());
		recovered.load();
		assert!(recovered.renew_restore_pending(SID, TARGET, TARGET_EPOCH));
		assert!(
			recovered
				.release_restore_claim(SID, "other", TARGET_EPOCH, SOURCE, SOURCE_EPOCH)
				.unwrap()
				.is_none()
		);
		let released = recovered
			.release_restore_claim(SID, TARGET, TARGET_EPOCH, SOURCE, SOURCE_EPOCH)
			.unwrap()
			.unwrap();
		assert_eq!((released.owner.as_str(), released.epoch), (SOURCE, TARGET_EPOCH + 1));
		assert!(!released.params.contains_key("_mesh_restore_pending"));
		assert!(!recovered.put_if_newer(source_record()).unwrap());
	}

	#[test]
	fn pending_restore_claim_promotes_only_its_exact_epoch() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		store.put(source_record()).unwrap();
		let pending = serde_json::json!({"kind": "rerun", "source_owner": SOURCE});
		store
			.claim_restore_pending(SID, SOURCE, SOURCE_EPOCH, TARGET, &pending)
			.unwrap();
		let mut params = Params::new();
		params.insert("name".to_owned(), JsonValue::String(SID.to_owned()));
		let committed = store
			.commit_restore_pending(SID, TARGET, TARGET_EPOCH, params, "async", "rerun")
			.unwrap();
		assert_eq!((committed.owner.as_str(), committed.epoch), (TARGET, TARGET_EPOCH));
		assert!(!committed.params.contains_key("_mesh_restore_pending"));
		assert!(!store.renew_restore_pending(SID, TARGET, TARGET_EPOCH));
	}
	#[test]
	fn failed_restore_attempt_retries_the_same_pending_generation() {
		let temp = tempfile::tempdir().unwrap();
		let store = RecordStore::new(temp.path());
		store.put(source_record()).unwrap();
		let pending = serde_json::json!({
			"kind": "replica",
			"source_owner": SOURCE,
			"source_epoch": SOURCE_EPOCH,
		});
		let first = store
			.claim_restore_pending(SID, SOURCE, SOURCE_EPOCH, TARGET, &pending)
			.unwrap();
		assert!(store.renew_restore_pending(SID, TARGET, TARGET_EPOCH));

		let retry = store
			.claim_restore_pending(SID, SOURCE, SOURCE_EPOCH, TARGET, &pending)
			.unwrap();
		assert_eq!(retry, first);
		assert_eq!(retry.params.get("_mesh_restore_pending"), Some(&pending));
	}

	#[test]
	fn failed_restore_commit_keeps_the_pending_record_in_memory() {
		let temp = tempfile::tempdir().unwrap();
		let root = temp.path().join("records");
		let backup = temp.path().join("records-backup");
		let store = RecordStore::new(&root);
		store.put(source_record()).unwrap();
		let pending = serde_json::json!({"kind": "rerun", "source_owner": SOURCE});
		store
			.claim_restore_pending(SID, SOURCE, SOURCE_EPOCH, TARGET, &pending)
			.unwrap();
		fs::rename(&root, &backup).unwrap();
		fs::write(&root, b"not a directory").unwrap();

		let mut params = Params::new();
		params.insert("name".to_owned(), JsonValue::String(SID.to_owned()));
		assert!(
			store
				.commit_restore_pending(SID, TARGET, TARGET_EPOCH, params, "async", "rerun")
				.is_err()
		);
		let record = store.get(SID).unwrap();
		assert_eq!(record.params.get("_mesh_restore_pending"), Some(&pending));
		assert!(store.renew_restore_pending(SID, TARGET, TARGET_EPOCH));
	}
}
