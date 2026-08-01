//! `PostgreSQL` ownership and lease metadata for production clusters.

use std::time::{SystemTime, UNIX_EPOCH};

use ::postgres::{Row, Transaction, types::ToSql};
use parking_lot::Mutex;
use serde_json::Value;
use uuid::Uuid;

use super::{lease::LeaseRecord, record::CreateRecord, replica::ReplicaRecord};
use crate::{
	EngineError, Result,
	postgres::{self as pg, Client},
};

pub(crate) const OWNER_LEASE_TTL: f64 = 15.0;
const OWNER_LEASE_RENEW_TIMEOUT_MS: i64 = 3_000;
/// Synchronous `PostgreSQL` store used by the blocking engine layer.
pub(crate) struct ProductionStore {
	url:              String,
	object_namespace: String,
	client:           Mutex<Option<Client>>,
}

/// Encrypted tenant credential persisted in the shared production store.
#[derive(Clone, Debug)]
pub(crate) struct TenantCredential {
	pub tenant:                 String,
	pub name:                   String,
	pub key_id:                 String,
	pub ciphertext:             Vec<u8>,
	pub allowed_domains:        String,
	pub header_names:           String,
	pub expires_at_unix_millis: Option<i64>,
	pub requests_per_minute:    i64,
	pub version:                String,
}

/// The exact portable point that fences a sandbox out of serving while it is
/// suspended or being restored. It deliberately excludes create parameters
/// and therefore cannot carry secrets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SuspensionMarker {
	pub sid:               String,
	pub owner:             String,
	pub epoch:             i64,
	pub incarnation_epoch: i64,
	pub state:             String,
	pub point:             String,
	pub generation:        u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RollbackMarker {
	pub sid:                   String,
	pub owner:                 String,
	pub epoch:                 i64,
	pub incarnation_epoch:     i64,
	pub source_owner:          String,
	pub source_epoch:          i64,
	pub target:                String,
	pub safety:                String,
	pub operation_generation:  u64,
	pub checkpoint_generation: u64,
}

/// The cluster-authoritative result for replaying a rollback journal tied to
/// one exact owner generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RollbackDisposition {
	RollingBack(RollbackMarker),
	CommittedRunning,
	Other,
}

/// A dedicated `PostgreSQL` session that serializes replica object publication
/// with portable-history GC for one immutable bundle key. Session scope is
/// intentional: it spans the asynchronous S3 overwrite/stat before metadata
/// publication on the shared store connection.
pub(crate) struct ReplicaPublicationLease {
	client: Option<Client>,
}

/// A dedicated session read guard for one immutable replica bundle. It shares
/// the publication/GC advisory key and deliberately carries no ownership
/// mutation authority.
pub(crate) struct ReplicaReadLease {
	client: Option<Client>,
}

impl ReplicaPublicationLease {
	/// Commit replica metadata on the same session that holds the object lock.
	/// Revalidating here closes the async S3-to-metadata ownership race without
	/// attempting a conflicting transaction advisory lock.
	pub(crate) fn commit(&mut self, record: &ReplicaRecord) -> Result<()> {
		let params = serde_json::to_string(&record.params)?;
		let source_epoch = i64::try_from(record.source_epoch)
			.map_err(|_| EngineError::invalid("replica source epoch exceeds PostgreSQL range"))?;
		let checkpoint_generation = i64::try_from(record.checkpoint_generation).map_err(|_| {
			EngineError::invalid("replica checkpoint generation exceeds PostgreSQL range")
		})?;
		let updated_at = unix_now()?;
		pg::blocking(|| {
			let client = self
				.client
				.as_mut()
				.ok_or_else(|| EngineError::engine("replica publication lease was released"))?;
			let mut transaction = client.transaction().map_err(database_error)?;
			let owner = transaction
				.query_opt(
					"SELECT owner, epoch FROM sandbox_ownership WHERE sid = $1 AND owner_lease_owner = \
					 $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
					 clock_timestamp()) AND deleting = FALSE FOR UPDATE",
					&[&record.sid, &record.source_node, &source_epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {} has no ownership record", record.sid))
				})?;
			let current_owner = owner.try_get::<_, String>(0).map_err(database_error)?;
			let current_epoch = owner.try_get::<_, i64>(1).map_err(database_error)?;
			if current_owner != record.source_node || current_epoch != source_epoch {
				return Err(fencing_error(
					&record.sid,
					&record.source_node,
					source_epoch,
					&current_owner,
					current_epoch,
				));
			}
			let existing = transaction
				.query_opt(
					"SELECT digest, object_key, source_epoch, checkpoint_generation FROM \
					 sandbox_replicas WHERE sid = $1 FOR UPDATE",
					&[&record.sid],
				)
				.map_err(database_error)?;
			if let Some(existing) = existing {
				let digest = existing.try_get::<_, String>(0).map_err(database_error)?;
				let object_key = existing.try_get::<_, String>(1).map_err(database_error)?;
				let existing_source_epoch = existing.try_get::<_, i64>(2).map_err(database_error)?;
				let generation = existing.try_get::<_, i64>(3).map_err(database_error)?;
				if !replica_overwrite_required(
					&record.sid,
					&digest,
					&object_key,
					existing_source_epoch,
					generation,
					&record.digest,
					&record.object_key,
					source_epoch,
					checkpoint_generation,
				)? {
					transaction.commit().map_err(database_error)?;
					return Ok(());
				}
			}
			let written = transaction
				.execute(
					"INSERT INTO sandbox_replicas (sid, digest, object_key, source_node, source_epoch, \
					 checkpoint_generation, snapshot_dir, params, needs_secrets, updated_at) VALUES \
					 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (sid) DO UPDATE SET digest \
					 = EXCLUDED.digest, object_key = EXCLUDED.object_key, source_node = \
					 EXCLUDED.source_node, source_epoch = EXCLUDED.source_epoch, checkpoint_generation \
					 = EXCLUDED.checkpoint_generation, snapshot_dir = EXCLUDED.snapshot_dir, params = \
					 EXCLUDED.params, needs_secrets = EXCLUDED.needs_secrets, updated_at = \
					 EXCLUDED.updated_at WHERE (sandbox_replicas.source_epoch, \
					 sandbox_replicas.checkpoint_generation) < (EXCLUDED.source_epoch, \
					 EXCLUDED.checkpoint_generation)",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.digest,
						&record.object_key,
						&record.source_node,
						&source_epoch,
						&checkpoint_generation,
						&record.snapshot_dir,
						&params,
						&record.needs_secrets,
						&updated_at,
					],
				)
				.map_err(database_error)?;
			if written == 0 {
				let current = transaction
					.query_one(
						"SELECT digest, object_key, source_epoch, checkpoint_generation FROM \
						 sandbox_replicas WHERE sid = $1",
						&[&record.sid],
					)
					.map_err(database_error)?;
				replica_overwrite_required(
					&record.sid,
					&current.try_get::<_, String>(0).map_err(database_error)?,
					&current.try_get::<_, String>(1).map_err(database_error)?,
					current.try_get::<_, i64>(2).map_err(database_error)?,
					current.try_get::<_, i64>(3).map_err(database_error)?,
					&record.digest,
					&record.object_key,
					source_epoch,
					checkpoint_generation,
				)?;
			}
			transaction.commit().map_err(database_error)
		})
	}
}

/// Resolve the canonical production object namespace from the shared cluster
/// identity.
pub(crate) fn production_object_namespace(postgres_url: &str) -> Result<String> {
	let mut client = pg::connect(postgres_url, "cluster PostgreSQL store")?;
	pg::blocking(|| migrate(&mut client))?;
	Ok(cluster_object_namespace(&pg::blocking(|| bootstrap_cluster_id(&mut client))?))
}

impl ProductionStore {
	/// Connect to `PostgreSQL` and fail unless the cluster schema is usable.
	pub(crate) fn connect(url: &str) -> Result<Self> {
		let mut client = pg::connect(url, "cluster PostgreSQL store")?;
		pg::blocking(|| migrate(&mut client))?;
		let cluster_id = pg::blocking(|| bootstrap_cluster_id(&mut client))?;
		Ok(Self {
			url:              url.to_owned(),
			object_namespace: cluster_object_namespace(&cluster_id),
			client:           Mutex::new(Some(client)),
		})
	}

	pub(crate) fn object_namespace(&self) -> &str {
		&self.object_namespace
	}

	pub(crate) fn put_tenant_credential(&self, row: &TenantCredential) -> Result<()> {
		self.with_client(|client| {
			client
				.execute(
					"INSERT INTO tenant_credentials \
					 (tenant,name,key_id,ciphertext,allowed_domains,header_names,\
					 expires_at_unix_millis,requests_per_minute,version) VALUES \
					 ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (tenant,name) DO UPDATE SET \
					 key_id=EXCLUDED.key_id,ciphertext=EXCLUDED.ciphertext,allowed_domains=EXCLUDED.\
					 allowed_domains,header_names=EXCLUDED.header_names,\
					 expires_at_unix_millis=EXCLUDED.expires_at_unix_millis,\
					 requests_per_minute=EXCLUDED.requests_per_minute,version=EXCLUDED.version",
					&[
						&row.tenant,
						&row.name,
						&row.key_id,
						&row.ciphertext,
						&row.allowed_domains,
						&row.header_names,
						&row.expires_at_unix_millis,
						&row.requests_per_minute,
						&row.version,
					],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	pub(crate) fn tenant_credential(
		&self,
		tenant: &str,
		name: &str,
	) -> Result<Option<TenantCredential>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT tenant,name,key_id,ciphertext,allowed_domains,header_names,\
					 expires_at_unix_millis,requests_per_minute,version FROM tenant_credentials WHERE \
					 tenant=$1 AND name=$2",
					&[&tenant, &name],
				)
				.map_err(database_error)?
				.map(|row| tenant_credential_from_row(&row))
				.transpose()
		})
	}

	pub(crate) fn list_tenant_credentials(&self, tenant: &str) -> Result<Vec<TenantCredential>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT tenant,name,key_id,ciphertext,allowed_domains,header_names,\
					 expires_at_unix_millis,requests_per_minute,version FROM tenant_credentials WHERE \
					 tenant=$1 ORDER BY name",
					&[&tenant],
				)
				.map_err(database_error)?
				.iter()
				.map(tenant_credential_from_row)
				.collect()
		})
	}

	pub(crate) fn delete_tenant_credential(&self, tenant: &str, name: &str) -> Result<bool> {
		self.with_client(|client| {
			client
				.execute("DELETE FROM tenant_credentials WHERE tenant=$1 AND name=$2", &[
					&tenant, &name,
				])
				.map(|count| count == 1)
				.map_err(database_error)
		})
	}

	/// Register a verifier for a key identifier exactly once. A later
	/// mismatching verifier is a configuration split-brain, never a rotation.
	pub(crate) fn verify_or_register_key(&self, key_id: &str, verifier: &str) -> Result<()> {
		if key_id.is_empty() || verifier.is_empty() {
			return Err(EngineError::invalid("key verifier requires an id and verifier"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			transaction
				.execute(
					"INSERT INTO cluster_key_verifiers (key_id, verifier) VALUES ($1, $2) ON CONFLICT \
					 (key_id) DO NOTHING",
					&[&key_id, &verifier],
				)
				.map_err(database_error)?;
			let stored = transaction
				.query_one("SELECT verifier FROM cluster_key_verifiers WHERE key_id = $1", &[&key_id])
				.map_err(database_error)?
				.try_get::<_, String>(0)
				.map_err(database_error)?;
			if stored != verifier {
				return Err(EngineError::invalid(format!("key verifier mismatch for key id {key_id}")));
			}
			transaction.commit().map_err(database_error)
		})
	}

	/// Acquire a dedicated-session object lock before a runtime overwrites or
	/// validates one exact immutable replica object.
	pub(crate) fn acquire_replica_publication(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		object_key: &str,
	) -> Result<ReplicaPublicationLease> {
		if owner.is_empty() || epoch < 0 || object_key.is_empty() {
			return Err(EngineError::invalid("invalid replica publication lease"));
		}
		let mut lease = ReplicaPublicationLease {
			client: Some(pg::connect(&self.url, "replica publication lease")?),
		};
		pg::blocking(|| {
			let client = lease
				.client
				.as_mut()
				.ok_or_else(|| EngineError::engine("replica publication lease was released"))?;
			let key = replica_object_lock_key(object_key);
			client
				.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
				.map_err(database_error)?;
			let row = client
				.query_opt(
					"SELECT owner, epoch FROM sandbox_ownership WHERE sid = $1 AND owner_lease_owner = \
					 $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
					 clock_timestamp()) AND deleting = FALSE",
					&[&sid, &owner, &epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let current_owner = row.try_get::<_, String>(0).map_err(database_error)?;
			let current_epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
			if current_owner != owner || current_epoch != epoch {
				return Err(fencing_error(sid, owner, epoch, &current_owner, current_epoch));
			}
			Ok(())
		})?;
		Ok(lease)
	}

	/// Acquire the same session-scoped bundle lock used by publication and GC
	/// before reading, extracting, or digest-verifying a replica object.
	pub(crate) fn acquire_replica_read(&self, object_key: &str) -> Result<ReplicaReadLease> {
		if object_key.is_empty() {
			return Err(EngineError::invalid("replica read lease requires an object key"));
		}
		let mut lease =
			ReplicaReadLease { client: Some(pg::connect(&self.url, "replica read lease")?) };
		pg::blocking(|| {
			let client = lease
				.client
				.as_mut()
				.ok_or_else(|| EngineError::engine("replica read lease was released"))?;
			let key = replica_object_lock_key(object_key);
			client
				.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
				.map_err(database_error)?;
			Ok::<(), EngineError>(())
		})?;
		Ok(lease)
	}

	/// runtime. The client remains serialized while the operation executes.
	fn with_client<T: Send>(
		&self,
		operation: impl FnOnce(&mut Client) -> Result<T> + Send,
	) -> Result<T> {
		let mut client = self.client.lock();
		let client = client
			.as_mut()
			.ok_or_else(|| EngineError::engine("PostgreSQL metadata store is closed"))?;
		pg::blocking(|| operation(client))
	}

	/// Insert or advance one sandbox ownership record transactionally.
	pub(crate) fn record(&self, record: &CreateRecord) -> Result<CreateRecord> {
		let record = record_without_secrets(record);
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{}", record.sid))?;
			if !record.idempotency_key.is_empty() {
				advisory_lock(
					&mut transaction,
					&format!("sandbox-idempotency:{}", record.idempotency_key),
				)?;
				if let Some(row) = transaction
					.query_opt(
						"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
						 created_at, incarnation_epoch FROM sandbox_ownership WHERE idempotency_key = $1",
						&[&record.idempotency_key],
					)
					.map_err(database_error)?
				{
					let original = record_from_row(&row)?;
					transaction.commit().map_err(database_error)?;
					return Ok(original);
				}
			}

			if let Some(row) = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, incarnation_epoch FROM sandbox_ownership WHERE sid = $1 AND deleting \
					 = FALSE FOR UPDATE",
					&[&record.sid],
				)
				.map_err(database_error)?
			{
				let current = record_from_row(&row)?;
				if record.epoch < current.epoch
					|| (record.epoch == current.epoch && record.owner != current.owner)
				{
					return Err(fencing_error(
						&record.sid,
						&record.owner,
						record.epoch,
						&current.owner,
						current.epoch,
					));
				}
				if record.epoch == current.epoch {
					transaction.commit().map_err(database_error)?;
					return Ok(current);
				}
				let params = serde_json::to_string(&record.params)?;
				let idempotency_key = optional_key(&record.idempotency_key);
				transaction
					.execute(
						"UPDATE sandbox_ownership SET owner = $2, epoch = $3, idempotency_key = $4, \
						 params = $5, ha = $6, restart_policy = $7, created_at = $8, owner_lease_owner \
						 = $2, owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
						 clock_timestamp()) + $9::DOUBLE PRECISION WHERE sid = $1",
						&[
							&record.sid as &(dyn ToSql + Sync),
							&record.owner,
							&record.epoch,
							&idempotency_key,
							&params,
							&record.ha,
							&record.restart_policy,
							&record.created_at,
							&OWNER_LEASE_TTL,
						],
					)
					.map_err(database_error)?;
			} else {
				let reservation = transaction
					.query_opt(
						"SELECT epoch, incarnation_epoch FROM sandbox_create_reservations WHERE sid = \
						 $1 AND owner = $2 AND idempotency_key = $3 AND expires_at > EXTRACT(EPOCH FROM \
						 clock_timestamp()) FOR UPDATE",
						&[&record.sid, &record.owner, &record.idempotency_key],
					)
					.map_err(database_error)?
					.ok_or_else(|| {
						EngineError::busy(format!(
							"sandbox {} has no live create reservation",
							record.sid
						))
					})?;
				let reserved_epoch = reservation.try_get::<_, i64>(0).map_err(database_error)?;
				let reserved_incarnation = reservation.try_get::<_, i64>(1).map_err(database_error)?;
				if record.epoch != reserved_epoch || record.incarnation_epoch != reserved_incarnation {
					return Err(EngineError::busy(format!(
						"sandbox {} create reservation is fenced",
						record.sid
					)));
				}
				let allocated = record.clone();
				let params = serde_json::to_string(&allocated.params)?;
				let idempotency_key = optional_key(&allocated.idempotency_key);
				transaction
					.execute(
						"INSERT INTO sandbox_ownership (sid, owner, epoch, incarnation_epoch, \
						 idempotency_key, params, ha, restart_policy, created_at, owner_lease_owner, \
						 owner_lease_epoch, owner_lease_expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, \
						 $8, $9, $2, $3, EXTRACT(EPOCH FROM clock_timestamp()) + $10::DOUBLE PRECISION)",
						&[
							&allocated.sid as &(dyn ToSql + Sync),
							&allocated.owner,
							&allocated.epoch,
							&allocated.incarnation_epoch,
							&idempotency_key,
							&params,
							&allocated.ha,
							&allocated.restart_policy,
							&allocated.created_at,
							&OWNER_LEASE_TTL,
						],
					)
					.map_err(database_error)?;
				transaction
					.execute(
						"DELETE FROM sandbox_create_reservations WHERE sid = $1 AND epoch = $2 AND \
						 owner = $3 AND idempotency_key = $4",
						&[&allocated.sid, &allocated.epoch, &allocated.owner, &allocated.idempotency_key],
					)
					.map_err(database_error)?;
				transaction
					.execute(
						"INSERT INTO sandbox_epoch_floors (sid, last_epoch) VALUES ($1, $2) ON CONFLICT \
						 (sid) DO UPDATE SET last_epoch = GREATEST(sandbox_epoch_floors.last_epoch, \
						 EXCLUDED.last_epoch)",
						&[&allocated.sid, &allocated.epoch],
					)
					.map_err(database_error)?;
				transaction.commit().map_err(database_error)?;
				return Ok(allocated);
			}
			transaction.commit().map_err(database_error)?;
			Ok(record.clone())
		})
	}

	/// Permanently reserve the next ownership epoch before local resource
	/// creation.
	pub(crate) fn reserve_create_epoch(
		&self,
		sid: &str,
		owner: &str,
		idempotency_key: &str,
	) -> Result<i64> {
		if sid.is_empty() || owner.is_empty() || idempotency_key.is_empty() {
			return Err(EngineError::invalid(
				"sandbox id, owner, and idempotency key are required to reserve an ownership epoch",
			));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			if transaction
				.query_opt("SELECT 1 FROM sandbox_ownership WHERE sid = $1 AND deleting = FALSE", &[
					&sid,
				])
				.map_err(database_error)?
				.is_some()
			{
				return Err(EngineError::busy(format!("sandbox {sid} already has ownership")));
			}
			if let Some(row) = transaction
				.query_opt(
					"SELECT epoch, owner, idempotency_key, expires_at FROM sandbox_create_reservations \
					 WHERE sid = $1 FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
			{
				let epoch = row.try_get::<_, i64>(0).map_err(database_error)?;
				let reserved_owner = row.try_get::<_, String>(1).map_err(database_error)?;
				let reserved_key = row.try_get::<_, String>(2).map_err(database_error)?;
				let expires_at = row.try_get::<_, f64>(3).map_err(database_error)?;
				let live = transaction
					.query_one("SELECT EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE PRECISION", &[])
					.map_err(database_error)?
					.try_get::<_, f64>(0)
					.map_err(database_error)?
					< expires_at;
				if live && reserved_owner == owner && reserved_key == idempotency_key {
					transaction.commit().map_err(database_error)?;
					return Ok(epoch);
				}
				if live {
					return Err(EngineError::busy(format!(
						"sandbox {sid} has a competing create reservation"
					)));
				}
				transaction
					.execute("DELETE FROM sandbox_create_reservations WHERE sid = $1", &[&sid])
					.map_err(database_error)?;
			}
			let epoch = match transaction
				.query_opt("SELECT last_epoch FROM sandbox_epoch_floors WHERE sid = $1 FOR UPDATE", &[
					&sid,
				])
				.map_err(database_error)?
			{
				Some(row) => row
					.try_get::<_, i64>(0)
					.map_err(database_error)?
					.checked_add(1)
					.ok_or_else(|| {
						EngineError::invalid(format!("ownership epoch exhausted for {sid}"))
					})?,
				None => 0,
			};
			transaction
				.execute(
					"INSERT INTO sandbox_epoch_floors (sid, last_epoch) VALUES ($1, $2) ON CONFLICT \
					 (sid) DO UPDATE SET last_epoch = EXCLUDED.last_epoch",
					&[&sid, &epoch],
				)
				.map_err(database_error)?;
			transaction
				.execute(
					"INSERT INTO sandbox_create_reservations (sid, epoch, incarnation_epoch, owner, \
					 idempotency_key, expires_at) VALUES ($1, $2, $2, $3, $4, EXTRACT(EPOCH FROM \
					 clock_timestamp()) + 300)",
					&[&sid, &epoch, &owner, &idempotency_key],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(epoch)
		})
	}

	/// Persist metadata for the exact, still-live ownership generation. Unlike
	/// `record`, this never performs create/idempotency arbitration and never
	/// advances or refreshes the owner lease.
	pub(crate) fn update_exact_record(&self, record: &CreateRecord) -> Result<CreateRecord> {
		let record = record_without_secrets(record);
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{}", record.sid))?;
			let current = transaction
				.query_opt(
					"SELECT owner, epoch FROM sandbox_ownership WHERE sid = $1 AND owner_lease_owner = \
					 $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
					 clock_timestamp()) FOR UPDATE",
					&[&record.sid, &record.owner, &record.epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::invalid(format!(
						"record update for {} lost ownership {}/{}",
						record.sid, record.owner, record.epoch
					))
				})?;
			let current_owner = current.try_get::<_, String>(0).map_err(database_error)?;
			let current_epoch = current.try_get::<_, i64>(1).map_err(database_error)?;
			if current_owner != record.owner || current_epoch != record.epoch {
				return Err(fencing_error(
					&record.sid,
					&record.owner,
					record.epoch,
					&current_owner,
					current_epoch,
				));
			}
			let params = serde_json::to_string(&record.params)?;
			let changed = transaction
				.execute(
					"UPDATE sandbox_ownership SET params = $4, ha = $5, restart_policy = $6, \
					 created_at = $7 WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = FALSE",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&record.ha,
						&record.restart_policy,
						&record.created_at,
					],
				)
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::busy(format!(
					"record update for {} lost its live ownership lease",
					record.sid
				)));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Resolve the authoritative owner and create parameters for a sandbox.
	pub(crate) fn resolve(&self, sid: &str) -> Result<Option<CreateRecord>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, incarnation_epoch FROM sandbox_ownership WHERE sid = $1 AND deleting \
					 = FALSE",
					&[&sid],
				)
				.map_err(database_error)?
				.as_ref()
				.map(record_from_row)
				.transpose()
		})
	}

	/// List every authoritative sandbox record in stable identifier order.
	pub(crate) fn list(&self) -> Result<Vec<CreateRecord>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, incarnation_epoch FROM sandbox_ownership WHERE deleting = FALSE ORDER \
					 BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(record_from_row)
				.collect()
		})
	}

	/// Return the stable incarnation for one exact live ownership generation.
	pub(crate) fn current_incarnation(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
	) -> Result<i64> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT incarnation_epoch FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND \
					 epoch = $3 AND deleting = FALSE",
					&[&sid, &expected_owner, &expected_epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| EngineError::busy(format!("sandbox {sid} ownership was fenced")))?
				.try_get(0)
				.map_err(database_error)
		})
	}

	/// List sandbox identifiers currently fenced to one owner.
	pub(crate) fn owned_by(&self, owner: &str) -> Result<Vec<String>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid FROM sandbox_ownership WHERE owner = $1 AND deleting = FALSE ORDER BY \
					 sid",
					&[&owner],
				)
				.map_err(database_error)?
				.iter()
				.map(|row| row.try_get::<_, String>(0).map_err(database_error))
				.collect()
		})
	}

	/// Resolve shared replica metadata for a sandbox.
	pub(crate) fn replica(&self, sid: &str) -> Result<Option<ReplicaRecord>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, digest, object_key, source_node, source_epoch, checkpoint_generation, \
					 snapshot_dir, params, needs_secrets FROM sandbox_replicas WHERE sid = $1 AND \
					 object_key <> ''",
					&[&sid],
				)
				.map_err(database_error)?
				.as_ref()
				.map(replica_from_row)
				.transpose()
		})
	}

	/// List the authoritative replica records, including their exact scoped
	/// object identities, for local cache-root reconciliation.
	pub(crate) fn replica_records(&self) -> Result<Vec<ReplicaRecord>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, digest, object_key, source_node, source_epoch, checkpoint_generation, \
					 snapshot_dir, params, needs_secrets FROM sandbox_replicas WHERE object_key <> '' \
					 ORDER BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(replica_from_row)
				.collect()
		})
	}

	/// List every sandbox with a checkpoint in shared object storage.
	pub(crate) fn list_replicas(&self) -> Result<Vec<String>> {
		self.with_client(|client| {
			client
				.query("SELECT sid FROM sandbox_replicas ORDER BY sid", &[])
				.map_err(database_error)?
				.iter()
				.map(|row| row.try_get::<_, String>(0).map_err(database_error))
				.collect()
		})
	}

	/// Remove replica metadata administratively.
	pub(crate) fn remove_replica(&self, sid: &str) -> Result<()> {
		self.with_client(|client| {
			client
				.execute("DELETE FROM sandbox_replicas WHERE sid = $1", &[&sid])
				.map_err(database_error)?;
			Ok(())
		})
	}

	/// Remove a replica only if it remains the exact checkpoint consumed by a
	/// restore. A late restore must not erase a newer publication.
	pub(crate) fn remove_replica_exact(&self, record: &ReplicaRecord) -> Result<()> {
		let source_epoch = i64::try_from(record.source_epoch)
			.map_err(|_| EngineError::invalid("replica source epoch exceeds PostgreSQL range"))?;
		let checkpoint_generation = i64::try_from(record.checkpoint_generation).map_err(|_| {
			EngineError::invalid("replica checkpoint generation exceeds PostgreSQL range")
		})?;
		self.with_client(|client| {
			client
				.execute(
					"DELETE FROM sandbox_replicas WHERE sid = $1 AND digest = $2 AND object_key = $3 \
					 AND source_node = $4 AND source_epoch = $5 AND checkpoint_generation = $6",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.digest,
						&record.object_key,
						&record.source_node,
						&source_epoch,
						&checkpoint_generation,
					],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	/// Atomically advance a sandbox only after its prior owner lease expired.
	pub(crate) fn claim_next(&self, sid: &str, owner: &str) -> Result<CreateRecord> {
		if owner.is_empty() {
			return Err(EngineError::invalid("ownership claims require an owner"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION FROM sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			if lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			owner.clone_into(&mut record.owner);
			transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, owner_lease_owner = $2, \
					 owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1",
					&[&sid as &(dyn ToSql + Sync), &record.owner, &record.epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Fence an expired non-serving suspension only if its exact durable
	/// marker still matches. Unlike `claim_next`, this cannot steal a newer
	/// point or lifecycle generation between enumeration and recovery.
	pub(crate) fn claim_suspend_marker(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		expected_state: &str,
		expected_point: &str,
		expected_generation: u64,
		new_owner: &str,
	) -> Result<CreateRecord> {
		let expected_generation = i64::try_from(expected_generation)
			.map_err(|_| EngineError::invalid("suspend generation exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION, lifecycle_state, suspend_point, suspend_generation FROM \
					 sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let mut record = record_from_row(&row)?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let state = row.try_get::<_, String>(10).map_err(database_error)?;
			let point = row
				.try_get::<_, Option<String>>(11)
				.map_err(database_error)?;
			let generation = row.try_get::<_, Option<i64>>(12).map_err(database_error)?;
			if record.owner != expected_owner
				|| record.epoch != expected_epoch
				|| state != expected_state
				|| point.as_deref() != Some(expected_point)
				|| generation != Some(expected_generation)
			{
				return Err(EngineError::busy(format!(
					"suspend marker for {sid} changed before claim"
				)));
			}
			if lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			new_owner.clone_into(&mut record.owner);
			transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, owner_lease_owner = $2, \
					 owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1",
					&[&sid as &(dyn ToSql + Sync), &record.owner, &record.epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Atomically claim a source generation and persist its target staging
	/// intent. Retrying the exact token joins the already-claimed generation;
	/// a different token cannot steal or overwrite that intent.
	pub(crate) fn claim_expected_with_intent(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		new_owner: &str,
		intent_record: &CreateRecord,
	) -> Result<CreateRecord> {
		let Some(intent) = intent_record
			.params
			.get("_mesh_migration_token")
			.and_then(Value::as_str)
			.filter(|intent| !intent.is_empty())
		else {
			return Err(EngineError::invalid(
				"expected ownership intent claim requires a migration token",
			));
		};
		if expected_owner.is_empty() || new_owner.is_empty() || expected_epoch < 0 {
			return Err(EngineError::invalid("invalid expected ownership intent claim"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION FROM sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			if record.owner == new_owner
				&& expected_epoch.checked_add(1) == Some(record.epoch)
				&& record.params.get("_mesh_migration_token") == Some(&Value::String(intent.to_owned()))
			{
				transaction.commit().map_err(database_error)?;
				return Ok(record);
			}
			if record.owner != expected_owner || record.epoch != expected_epoch {
				return Err(fencing_error(
					sid,
					expected_owner,
					expected_epoch,
					&record.owner,
					record.epoch,
				));
			}
			if lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			new_owner.clone_into(&mut record.owner);
			record.params.clone_from(&intent_record.params);
			let params = serde_json::to_string(&record.params)?;
			transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, params = $4, \
					 owner_lease_owner = $2, owner_lease_epoch = $3, owner_lease_expires_at = \
					 EXTRACT(EPOCH FROM clock_timestamp()) + $5::DOUBLE PRECISION WHERE sid = $1",
					&[
						&sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&OWNER_LEASE_TTL,
					],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Mark a locally paused source generation for one exact live migration
	/// target. This is the only path that permits a successor before the source
	/// lease expires.
	pub(crate) fn begin_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		epoch: i64,
		target: &str,
		token: &str,
	) -> Result<()> {
		if source.is_empty() || target.is_empty() || token.is_empty() || epoch < 0 {
			return Err(EngineError::invalid("invalid migration handoff"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT owner, epoch, migration_target, migration_token FROM sandbox_ownership \
					 WHERE sid = $1 AND owner = $2 AND epoch = $3 AND deleting = FALSE AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) FOR UPDATE",
					&[&sid, &source, &epoch],
				)
				.map_err(database_error)?;
			let Some(row) = row else {
				let target_epoch = epoch
					.checked_add(1)
					.ok_or_else(|| EngineError::invalid("ownership epoch exceeds local range"))?;
				let claimed = transaction
					.query_opt(
						"SELECT 1 FROM sandbox_ownership WHERE sid = $1 AND owner = $3 AND epoch = $5 \
						 AND deleting = FALSE AND migration_target = $3 AND migration_token = $4 AND \
						 migration_source = $2 AND migration_state = 'active' AND migration_claimed = \
						 TRUE",
						&[&sid as &(dyn ToSql + Sync), &source, &target, &token, &target_epoch],
					)
					.map_err(database_error)?;
				if claimed.is_some() {
					transaction.commit().map_err(database_error)?;
					return Ok(());
				}
				return Err(EngineError::invalid(format!(
					"migration handoff for {sid} lost source lease"
				)));
			};
			let existing_target = row
				.try_get::<_, Option<String>>(2)
				.map_err(database_error)?;
			let existing_token = row
				.try_get::<_, Option<String>>(3)
				.map_err(database_error)?;
			if existing_target.is_some() || existing_token.is_some() {
				if existing_target.as_deref() == Some(target)
					&& existing_token.as_deref() == Some(token)
				{
					transaction.commit().map_err(database_error)?;
					return Ok(());
				}
				return Err(EngineError::busy(format!("migration handoff for {sid} already exists")));
			}
			transaction
				.execute(
					"UPDATE sandbox_ownership SET migration_target = $4, migration_token = $5, \
					 migration_state = 'active', migration_id = $5, migration_source = $2, \
					 migration_claimed = FALSE WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 deleting = FALSE",
					&[&sid as &(dyn ToSql + Sync), &source, &epoch, &target, &token],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)
		})
	}

	/// Verify that the shared store durably consumed one exact migration token.
	#[allow(clippy::too_many_arguments, reason = "the full migration lineage is the fencing key")]
	pub(crate) fn confirm_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		source_epoch: i64,
		target: &str,
		target_epoch: i64,
		token: &str,
	) -> Result<()> {
		if source.is_empty()
			|| target.is_empty()
			|| token.is_empty()
			|| source_epoch < 0
			|| source_epoch.checked_add(1) != Some(target_epoch)
		{
			return Err(EngineError::invalid("invalid migration handoff confirmation"));
		}
		self.with_client(|client| {
			let row = client
				.query_opt(
					"SELECT owner, epoch, migration_target, migration_token, migration_claimed, \
					 migration_source FROM sandbox_ownership WHERE sid = $1 AND deleting = FALSE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let owner = row.try_get::<_, String>(0).map_err(database_error)?;
			let epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
			let marker_target = row
				.try_get::<_, Option<String>>(2)
				.map_err(database_error)?;
			let marker_token = row
				.try_get::<_, Option<String>>(3)
				.map_err(database_error)?;
			let claimed = row.try_get::<_, bool>(4).map_err(database_error)?;
			let marker_source = row
				.try_get::<_, Option<String>>(5)
				.map_err(database_error)?;
			if owner != target
				|| epoch != target_epoch
				|| marker_target.as_deref() != Some(target)
				|| marker_token.as_deref() != Some(token)
				|| marker_source.as_deref() != Some(source)
				|| !claimed
			{
				return Err(EngineError::busy(format!(
					"migration handoff confirmation for {sid} was fenced"
				)));
			}
			Ok(())
		})
	}

	/// Consume an exact pause-authorized handoff and atomically install the
	/// target intent with a fresh target lease.
	pub(crate) fn claim_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		source_epoch: i64,
		target: &str,
		token: &str,
		intent_record: &CreateRecord,
	) -> Result<CreateRecord> {
		if source.is_empty() || target.is_empty() || token.is_empty() || source_epoch < 0 {
			return Err(EngineError::invalid("invalid migration handoff claim"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, migration_target, migration_token, migration_claimed FROM \
					 sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let mut record = record_from_row(&row)?;
			let marker_target = row
				.try_get::<_, Option<String>>(8)
				.map_err(database_error)?;
			let marker_token = row
				.try_get::<_, Option<String>>(9)
				.map_err(database_error)?;
			let marker_claimed = row.try_get::<_, bool>(10).map_err(database_error)?;
			if record.owner == target
				&& source_epoch.checked_add(1) == Some(record.epoch)
				&& marker_target.as_deref() == Some(target)
				&& marker_token.as_deref() == Some(token)
				&& marker_claimed
			{
				transaction.commit().map_err(database_error)?;
				return Ok(record);
			}
			if record.owner != source
				|| record.epoch != source_epoch
				|| marker_target.as_deref() != Some(target)
				|| marker_token.as_deref() != Some(token)
				|| marker_claimed
			{
				return Err(EngineError::invalid(format!("migration handoff for {sid} was fenced")));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			target.clone_into(&mut record.owner);
			record.params.clone_from(&intent_record.params);
			let params = serde_json::to_string(&record.params)?;
			let claimed = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, params = $4, \
					 migration_claimed = TRUE, owner_lease_owner = $2, owner_lease_epoch = $3, \
					 owner_lease_expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + $5::DOUBLE \
					 PRECISION WHERE sid = $1 AND owner = $7 AND epoch = $8 AND deleting = FALSE AND \
					 migration_target = $2 AND migration_token = $6 AND migration_claimed = FALSE",
					&[
						&sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&OWNER_LEASE_TTL,
						&token,
						&source,
						&source_epoch,
					],
				)
				.map_err(database_error)?;
			if claimed != 1 {
				return Err(EngineError::invalid(format!("migration handoff for {sid} was fenced")));
			}

			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Abort only an unclaimed exact handoff. The source returns at a fresh
	/// epoch, permanently fencing work from its original generation.
	pub(crate) fn abort_migration_handoff(
		&self,
		sid: &str,
		source: &str,
		source_epoch: i64,
		target: &str,
		token: &str,
	) -> Result<Option<CreateRecord>> {
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, incarnation_epoch, migration_target, migration_token, \
					 migration_claimed, migration_state, migration_source FROM sandbox_ownership WHERE \
					 sid = $1 FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?;
			let Some(row) = row else {
				return Ok(None);
			};
			let mut record = record_from_row(&row)?;
			let marker_target = row
				.try_get::<_, Option<String>>(9)
				.map_err(database_error)?;
			let marker_token = row
				.try_get::<_, Option<String>>(10)
				.map_err(database_error)?;
			let claimed = row.try_get::<_, bool>(11).map_err(database_error)?;
			let state = row
				.try_get::<_, Option<String>>(12)
				.map_err(database_error)?;
			let provenance = row
				.try_get::<_, Option<String>>(13)
				.map_err(database_error)?;
			if record.owner == source
				&& record.epoch == source_epoch + 1
				&& state.as_deref() == Some("aborted")
				&& marker_target.as_deref() == Some(target)
				&& marker_token.as_deref() == Some(token)
				&& provenance.as_deref() == Some(source)
			{
				transaction.commit().map_err(database_error)?;
				return Ok(Some(record));
			}
			if record.owner != source
				|| record.epoch != source_epoch
				|| marker_target.as_deref() != Some(target)
				|| marker_token.as_deref() != Some(token)
				|| claimed
				|| state.as_deref() != Some("active")
			{
				transaction.commit().map_err(database_error)?;
				return Ok(None);
			}
			record.epoch = source_epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			let aborted = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, migration_state = 'aborted', \
					 migration_id = $8, migration_source = $2, migration_claimed = FALSE, \
					 owner_lease_owner = $2, owner_lease_epoch = $3, owner_lease_expires_at = \
					 EXTRACT(EPOCH FROM clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND \
					 owner = $5 AND epoch = $6 AND migration_target = $7 AND migration_token = $8 AND \
					 migration_claimed = FALSE AND migration_state = 'active'",
					&[
						&sid as &(dyn ToSql + Sync),
						&source,
						&record.epoch,
						&OWNER_LEASE_TTL,
						&source,
						&source_epoch,
						&target,
						&token,
					],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			if aborted == 1 {
				Ok(Some(record))
			} else {
				Ok(None)
			}
		})
	}

	/// Clear one durably recorded aborted handoff only after its source has
	/// been restored locally.  The token is the completion fence.
	pub(crate) fn complete_migration_abort(
		&self,
		sid: &str,
		token: &str,
		owner: &str,
		fresh_epoch: i64,
	) -> Result<CreateRecord> {
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, incarnation_epoch, migration_state, migration_token FROM \
					 sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 FOR UPDATE",
					&[&sid, &owner, &fresh_epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::busy(format!("migration abort completion for {sid} fenced"))
				})?;
			let record = record_from_row(&row)?;
			let state = row
				.try_get::<_, Option<String>>(9)
				.map_err(database_error)?;
			let recorded_token = row
				.try_get::<_, Option<String>>(10)
				.map_err(database_error)?;
			if state.is_none() && recorded_token.is_none() {
				transaction.commit().map_err(database_error)?;
				return Ok(record);
			}
			if state.as_deref() != Some("aborted") || recorded_token.as_deref() != Some(token) {
				return Err(EngineError::busy(format!("migration abort completion for {sid} fenced")));
			}
			let changed = transaction
				.execute(
					"UPDATE sandbox_ownership SET migration_state = NULL, migration_id = NULL, \
					 migration_target = NULL, migration_token = NULL, migration_source = NULL, \
					 migration_claimed = FALSE WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 migration_state = 'aborted' AND migration_token = $4",
					&[&sid, &owner, &fresh_epoch, &token],
				)
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::busy(format!("migration abort completion for {sid} fenced")));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Atomically claim an observed lineage and persist its immutable restore
	/// intent before any candidate is allowed to materialize a VM.
	pub(crate) fn claim_expected_pending(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		new_owner: &str,
		pending: &Value,
	) -> Result<CreateRecord> {
		if expected_owner.is_empty()
			|| new_owner.is_empty()
			|| expected_epoch < 0
			|| !pending.is_object()
		{
			return Err(EngineError::invalid("invalid pending restore claim"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION, lifecycle_state FROM sandbox_ownership WHERE sid = $1 AND deleting = \
					 FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let lifecycle_state = row.try_get::<_, String>(10).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			let existing_pending = record.params.get("_mesh_restore_pending");
			if record.owner == new_owner && existing_pending == Some(pending) {
				let renewed = transaction
					.execute(
						"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
						 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND owner = $2 AND \
						 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND deleting = FALSE",
						&[&sid as &(dyn ToSql + Sync), &new_owner, &record.epoch, &OWNER_LEASE_TTL],
					)
					.map_err(database_error)?;
				if renewed != 1 {
					return Err(EngineError::invalid(format!("pending restore for {sid} was fenced")));
				}
				transaction.commit().map_err(database_error)?;
				return Ok(record);
			}
			if record.owner != expected_owner || record.epoch != expected_epoch {
				return Err(fencing_error(
					sid,
					expected_owner,
					expected_epoch,
					&record.owner,
					record.epoch,
				));
			}
			if let Some(existing) = existing_pending
				&& existing != pending
			{
				return Err(EngineError::invalid(format!(
					"pending restore intent for {sid} conflicts with the claimed lineage"
				)));
			}
			if lifecycle_state != "suspended" && lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			new_owner.clone_into(&mut record.owner);
			record
				.params
				.insert("_mesh_restore_pending".to_owned(), pending.clone());
			let params = serde_json::to_string(&record.params)?;
			let updated = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, params = $4, \
					 owner_lease_owner = $2, owner_lease_epoch = $3, owner_lease_expires_at = \
					 EXTRACT(EPOCH FROM clock_timestamp()) + $5::DOUBLE PRECISION WHERE sid = $1 AND \
					 owner = $6 AND epoch = $7 AND deleting = FALSE",
					&[
						&sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&OWNER_LEASE_TTL,
						&expected_owner,
						&expected_epoch,
					],
				)
				.map_err(database_error)?;
			if updated != 1 {
				return Err(EngineError::invalid(format!("pending restore for {sid} was fenced")));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Atomically transfer a known ownership generation to a restoring owner.
	///
	/// Unlike [`Self::claim_next`], this rejects a claim if the source owner or
	/// epoch changed between portable artifact selection and restore.
	pub(crate) fn claim_expected(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		new_owner: &str,
	) -> Result<CreateRecord> {
		if expected_owner.is_empty() || new_owner.is_empty() || expected_epoch < 0 {
			return Err(EngineError::invalid(
				"expected ownership claims require owners and a non-negative epoch",
			));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION, lifecycle_state FROM sandbox_ownership WHERE sid = $1 AND deleting = \
					 FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let lifecycle_state = row.try_get::<_, String>(10).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			if record.owner != expected_owner || record.epoch != expected_epoch {
				return Err(fencing_error(
					sid,
					expected_owner,
					expected_epoch,
					&record.owner,
					record.epoch,
				));
			}
			// A durable suspended marker explicitly hands the identity to a
			// restorer; every other claim must still wait for owner expiry.
			if lifecycle_state != "suspended" && lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			new_owner.clone_into(&mut record.owner);
			transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, owner_lease_owner = $2, \
					 owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1",
					&[&sid as &(dyn ToSql + Sync), &record.owner, &record.epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Atomically claim an observed generation for rollback and persist the
	/// exact rollback target before any candidate may launch.
	pub(crate) fn claim_expected_rollback(
		&self,
		sid: &str,
		expected_owner: &str,
		expected_epoch: i64,
		new_owner: &str,
		target: &str,
		operation_generation: u64,
		checkpoint_generation: u64,
	) -> Result<CreateRecord> {
		if expected_owner.is_empty()
			|| new_owner.is_empty()
			|| target.is_empty()
			|| expected_epoch < 0
		{
			return Err(EngineError::invalid("invalid expected rollback claim"));
		}
		let operation_generation = i64::try_from(operation_generation).map_err(|_| {
			EngineError::invalid("rollback operation generation exceeds PostgreSQL range")
		})?;
		let checkpoint_generation = i64::try_from(checkpoint_generation).map_err(|_| {
			EngineError::invalid("rollback checkpoint generation exceeds PostgreSQL range")
		})?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			// Marker creation and history pruning serialize in this order.
			advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let target_exists = transaction
				.query_opt(
					"SELECT 1 FROM portable_recovery_points WHERE sid = $1 AND name = $2 AND \
					 owner_node = $3 AND owner_epoch = $4 FOR KEY SHARE",
					&[&sid, &target, &expected_owner, &expected_epoch],
				)
				.map_err(database_error)?
				.is_some();
			if !target_exists {
				return Err(EngineError::not_found(format!(
					"rollback target {target} for sandbox {sid} does not exist"
				)));
			}
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, owner_lease_expires_at, EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE \
					 PRECISION FROM sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let lease_expires_at = row.try_get::<_, f64>(8).map_err(database_error)?;
			let db_now = row.try_get::<_, f64>(9).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			if record.owner != expected_owner || record.epoch != expected_epoch {
				return Err(fencing_error(
					sid,
					expected_owner,
					expected_epoch,
					&record.owner,
					record.epoch,
				));
			}
			if lease_expires_at > db_now {
				return Err(EngineError::busy(format!(
					"ownership lease for sandbox {sid} has not expired"
				)));
			}
			record.epoch = record
				.epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))?;
			new_owner.clone_into(&mut record.owner);
			let changed = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, owner_lease_owner = $2, \
					 owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION, lifecycle_state = 'rolling_back', \
					 rollback_target = $5, rollback_safety = $5, rollback_incarnation_epoch = \
					 incarnation_epoch, rollback_operation_generation = $6, \
					 rollback_checkpoint_generation = $7, rollback_source_owner = $8, \
					 rollback_source_epoch = $9 WHERE sid = $1 AND owner = $10 AND epoch = $11 AND \
					 deleting = FALSE",
					&[
						&sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&OWNER_LEASE_TTL,
						&target,
						&operation_generation,
						&checkpoint_generation,
						&expected_owner,
						&expected_epoch,
						&expected_owner,
						&expected_epoch,
					],
				)
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::busy(format!("rollback claim fenced for sandbox {sid}")));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Release a failed restore claim only while the exact claimed epoch is
	/// still current. Restoring the previous owner advances to a *new* epoch,
	/// so delayed work fenced by the original ownership generation remains
	/// permanently fenced.
	pub(crate) fn release_claim(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		previous_owner: &str,
		previous_epoch: i64,
	) -> Result<Option<CreateRecord>> {
		if owner.is_empty() || previous_owner.is_empty() {
			return Err(EngineError::invalid("invalid restore claim release"));
		}
		let restored_epoch = release_claim_epoch(previous_epoch, epoch)?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let Some(row) = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, \
					 created_at, lifecycle_state FROM sandbox_ownership WHERE sid = $1 AND deleting = \
					 FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
			else {
				return Err(EngineError::not_found(format!("sandbox {sid} has no ownership record")));
			};
			let lifecycle_state = row.try_get::<_, String>(8).map_err(database_error)?;
			let mut record = record_from_row(&row)?;
			if record.owner != owner || record.epoch != epoch {
				transaction.commit().map_err(database_error)?;
				return Ok(None);
			}
			let releasable = lifecycle_state == "suspended"
				|| (lifecycle_state == "running"
					&& record.params.contains_key("_mesh_restore_pending"));
			if !releasable {
				transaction.commit().map_err(database_error)?;
				return Ok(None);
			}
			record.params.remove("_mesh_restore_pending");
			let params = serde_json::to_string(&record.params)?;
			previous_owner.clone_into(&mut record.owner);
			record.epoch = restored_epoch;
			transaction
				.execute(
					"UPDATE sandbox_ownership SET owner = $2, epoch = $3, owner_lease_owner = $2, \
					 owner_lease_epoch = $3, owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION, params = $5 WHERE sid = $1",
					&[
						&sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&OWNER_LEASE_TTL,
						&params,
					],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			Ok(Some(record))
		})
	}

	/// Re-adopt an exact live rollback owner after restart without advancing
	/// its epoch. A stale, expired, or differently marked row is never revived.
	pub(crate) fn adopt_live_rollback(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
	) -> Result<CreateRecord> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid("invalid live rollback adoption"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND lifecycle_state = 'rolling_back' AND \
					 deleting = FALSE FOR UPDATE",
					&[&sid, &owner, &epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::busy(format!("rollback adoption for {sid} lost ownership"))
				})?;
			let record = record_from_row(&row)?;
			let renewed = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND owner = $2 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND lifecycle_state = 'rolling_back' AND \
					 deleting = FALSE",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?;
			if renewed != 1 {
				return Err(EngineError::busy(format!("rollback adoption for {sid} lost ownership")));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Re-adopt one exact live non-serving lifecycle generation after restart
	/// without advancing its epoch. The marker's point/generation is fenced
	/// before its lease is renewed.
	pub(crate) fn adopt_live_lifecycle(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		state: &str,
		point: &str,
		generation: u64,
	) -> Result<CreateRecord> {
		if owner.is_empty()
			|| epoch < 0
			|| point.is_empty()
			|| !matches!(state, "suspending" | "suspended" | "resuming")
		{
			return Err(EngineError::invalid("invalid live lifecycle adoption"));
		}
		let generation = i64::try_from(generation)
			.map_err(|_| EngineError::invalid("lifecycle generation exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND lifecycle_state = $4 AND suspend_point \
					 = $5 AND suspend_generation = $6 AND deleting = FALSE FOR UPDATE",
					&[&sid, &owner, &epoch, &state, &point, &generation],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::busy(format!("lifecycle adoption for {sid} lost ownership"))
				})?;
			let record = record_from_row(&row)?;
			let renewed = transaction
				.execute(
					"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND owner = $2 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND lifecycle_state = $5 AND suspend_point \
					 = $6 AND suspend_generation = $7 AND deleting = FALSE",
					&[
						&sid as &(dyn ToSql + Sync),
						&owner,
						&epoch,
						&OWNER_LEASE_TTL,
						&state,
						&point,
						&generation,
					],
				)
				.map_err(database_error)?;
			if renewed != 1 {
				return Err(EngineError::busy(format!("lifecycle adoption for {sid} lost ownership")));
			}
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Renew an owner lease only while the exact ownership generation remains
	/// authoritative, returning remaining TTL measured by `PostgreSQL` in the
	/// same statement. Callers derive a monotonic deadline from call start.
	pub(crate) fn renew_owner_lease(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
	) -> Result<Option<f64>> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid("invalid owner lease renewal"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			let timeout = format!("{OWNER_LEASE_RENEW_TIMEOUT_MS}ms");
			for setting in ["lock_timeout", "statement_timeout"] {
				transaction
					.execute("SELECT set_config($1, $2, true)", &[&setting, &timeout])
					.map_err(database_error)?;
			}
			let renewed = transaction
				.query_opt(
					"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
					 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND owner = $2 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = FALSE RETURNING \
					 owner_lease_expires_at - EXTRACT(EPOCH FROM clock_timestamp())",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?
				.map(|row| row.try_get::<_, f64>(0).map_err(database_error))
				.transpose()?;
			let renewed = if renewed.is_some() {
				renewed
			} else {
				transaction
					.query_opt(
						"UPDATE sandbox_create_reservations SET expires_at = EXTRACT(EPOCH FROM \
						 clock_timestamp()) + $4::DOUBLE PRECISION WHERE sid = $1 AND owner = $2 AND \
						 epoch = $3 AND expires_at > EXTRACT(EPOCH FROM clock_timestamp()) RETURNING \
						 expires_at - EXTRACT(EPOCH FROM clock_timestamp())",
						&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &OWNER_LEASE_TTL],
					)
					.map_err(database_error)?
					.map(|row| row.try_get::<_, f64>(0).map_err(database_error))
					.transpose()?
			};
			transaction.commit().map_err(database_error)?;
			Ok(renewed)
		})
	}

	/// Persist the exact recovery point after its portable commit and before
	/// local source teardown. The advisory lock makes retries idempotent while
	/// rejecting a different point for the same source generation.
	pub(crate) fn prepare_suspend(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		point: &str,
		generation: u64,
	) -> Result<()> {
		if owner.is_empty() || epoch < 0 || point.is_empty() {
			return Err(EngineError::invalid("invalid suspend marker"));
		}
		let generation = i64::try_from(generation)
			.map_err(|_| EngineError::invalid("suspend generation exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			require_current_portable_recovery_point(&mut transaction, sid, point, owner, epoch)?;
			let row = transaction
				.query_opt(
					"SELECT lifecycle_state, suspend_point, suspend_generation FROM sandbox_ownership \
					 WHERE sid = $1 AND owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND \
					 owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
					 clock_timestamp()) AND deleting = FALSE FOR UPDATE",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| EngineError::busy(format!("suspend marker for {sid} lost ownership")))?;
			let state = row.try_get::<_, String>(0).map_err(database_error)?;
			let existing_point = row
				.try_get::<_, Option<String>>(1)
				.map_err(database_error)?;
			let existing_generation = row.try_get::<_, Option<i64>>(2).map_err(database_error)?;
			if state == "suspending"
				&& existing_point.as_deref() == Some(point)
				&& existing_generation == Some(generation)
			{
				transaction.commit().map_err(database_error)?;
				return Ok(());
			}
			if state != "running" {
				return Err(EngineError::busy(format!("sandbox {sid} lifecycle is {state}")));
			}
			transaction
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'suspending', suspend_point = $4, \
					 suspend_generation = $5, suspend_incarnation_epoch = incarnation_epoch WHERE sid \
					 = $1 AND owner = $2 AND epoch = $3",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &point, &generation],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)
		})
	}

	/// Commit a prepared marker only while its exact source generation still
	/// owns a live lease. A committed retry is safe, but a fence cannot turn a
	/// stale source into a suspended authority.
	pub(crate) fn commit_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT lifecycle_state, owner, epoch, owner_lease_owner, owner_lease_epoch, \
					 owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) FROM \
					 sandbox_ownership WHERE sid = $1 AND deleting = FALSE FOR UPDATE",
					&[&sid],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let state = row.try_get::<_, String>(0).map_err(database_error)?;
			let current_owner = row.try_get::<_, String>(1).map_err(database_error)?;
			let current_epoch = row.try_get::<_, i64>(2).map_err(database_error)?;
			let lease_owner = row.try_get::<_, String>(3).map_err(database_error)?;
			let lease_epoch = row.try_get::<_, i64>(4).map_err(database_error)?;
			let live = row.try_get::<_, bool>(5).map_err(database_error)?;
			if current_owner != owner || current_epoch != epoch {
				return Err(fencing_error(sid, owner, epoch, &current_owner, current_epoch));
			}
			if state == "suspended" {
				transaction.commit().map_err(database_error)?;
				return Ok(());
			}
			if state != "suspending" || lease_owner != owner || lease_epoch != epoch || !live {
				return Err(EngineError::busy(format!("suspend commit for {sid} lost ownership")));
			}
			transaction
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'suspended' WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND lifecycle_state = 'suspending'",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)
		})
	}

	/// Clear only a still-prepared marker. The source remains the owner, so a
	/// successful abort returns it to normal serving reconciliation.
	pub(crate) fn abort_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		self.with_client(|client| {
			let cleared = client
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'running', suspend_point = NULL, \
					 suspend_generation = NULL, suspend_incarnation_epoch = NULL, \
					 owner_lease_expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + $4::DOUBLE \
					 PRECISION WHERE sid = $1 AND owner = $2 AND owner_lease_owner = $2 AND \
					 owner_lease_epoch = $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM \
					 clock_timestamp()) AND lifecycle_state IN ('suspending', 'running') AND deleting \
					 = FALSE",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &OWNER_LEASE_TTL],
				)
				.map_err(database_error)?;
			if cleared != 1 {
				return Err(EngineError::busy(format!("suspend abort for {sid} lost ownership")));
			}
			Ok(())
		})
	}

	/// Fence the precise rollback target and portable safety point before
	/// destructive replacement. Both points are immutable names, never a
	/// history-list selection.
	pub(crate) fn prepare_rollback(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		target: &str,
		safety: &str,
		operation_generation: u64,
		checkpoint_generation: u64,
	) -> Result<()> {
		if owner.is_empty() || epoch < 0 || target.is_empty() || safety.is_empty() {
			return Err(EngineError::invalid("invalid rollback marker"));
		}
		let operation_generation = i64::try_from(operation_generation).map_err(|_| {
			EngineError::invalid("rollback operation generation exceeds PostgreSQL range")
		})?;
		let checkpoint_generation = i64::try_from(checkpoint_generation).map_err(|_| {
			EngineError::invalid("rollback checkpoint generation exceeds PostgreSQL range")
		})?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("portable-history:{sid}"))?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			require_portable_recovery_point(&mut transaction, sid, target)?;
			if safety != target {
				require_current_portable_recovery_point(&mut transaction, sid, safety, owner, epoch)?;
			}
			let changed = transaction
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'rolling_back', suspend_point = \
					 $4, suspend_generation = $6, suspend_incarnation_epoch = incarnation_epoch, \
					 rollback_target = $4, rollback_safety = $5, rollback_incarnation_epoch = \
					 incarnation_epoch, rollback_operation_generation = $6, \
					 rollback_checkpoint_generation = $7, rollback_source_owner = $2, \
					 rollback_source_epoch = $3 WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = FALSE AND lifecycle_state = \
					 'running'",
					&[
						&sid as &(dyn ToSql + Sync),
						&owner,
						&epoch,
						&target,
						&safety,
						&operation_generation,
						&checkpoint_generation,
					],
				)
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::busy(format!("rollback marker for {sid} lost ownership")));
			}
			transaction.commit().map_err(database_error)
		})
	}

	/// Atomically publish the restored non-secret create metadata and clear
	/// the exact rollback marker before routing can serve the candidate.
	pub(crate) fn finish_rollback(&self, record: &CreateRecord) -> Result<()> {
		let params = serde_json::to_string(&record.params)?;
		let changed = self.with_client(|client| {
			client
				.execute(
					"UPDATE sandbox_ownership SET params = $4, ha = $5, restart_policy = $6, \
					 created_at = $7, lifecycle_state = 'running', suspend_point = NULL, \
					 suspend_generation = NULL, suspend_incarnation_epoch = NULL, rollback_target = \
					 NULL, rollback_safety = NULL, rollback_incarnation_epoch = NULL, \
					 rollback_operation_generation = NULL, rollback_checkpoint_generation = NULL, \
					 rollback_source_owner = NULL, rollback_source_epoch = NULL WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 \
					 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = \
					 FALSE AND (lifecycle_state = 'rolling_back' OR (lifecycle_state = 'running' AND \
					 params = $4 AND ha = $5 AND restart_policy = $6 AND created_at = $7 AND \
					 rollback_target IS NULL AND rollback_safety IS NULL))",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&record.ha,
						&record.restart_policy,
						&record.created_at,
					],
				)
				.map_err(database_error)
		})?;
		if changed != 1 {
			return Err(EngineError::busy(format!(
				"rollback finish for {} lost ownership",
				record.sid
			)));
		}
		Ok(())
	}

	/// Return the original source to serving only when its rollback marker is
	/// still exact and live; a stale worker cannot clear a later operation.
	pub(crate) fn abort_rollback(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		self.clear_rollback_marker(sid, owner, epoch, "rolling_back")
	}

	fn clear_rollback_marker(&self, sid: &str, owner: &str, epoch: i64, state: &str) -> Result<()> {
		self.with_client(|client| {
			let changed = client
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'running', suspend_point = NULL, \
					 suspend_generation = NULL, suspend_incarnation_epoch = NULL, rollback_target = \
					 NULL, rollback_safety = NULL, rollback_incarnation_epoch = NULL, \
					 rollback_operation_generation = NULL, rollback_checkpoint_generation = NULL, \
					 rollback_source_owner = NULL, rollback_source_epoch = NULL WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 \
					 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = \
					 FALSE AND lifecycle_state = $4",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &state],
				)
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::busy(format!("rollback commit for {sid} lost ownership")));
			}
			Ok(())
		})
	}

	/// Return all durable non-serving lifecycle markers so startup and
	/// reconciliation never infer suspension from host-local registry state.
	pub(crate) fn list_suspend_markers(&self) -> Result<Vec<SuspensionMarker>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, owner, epoch, lifecycle_state, suspend_point, suspend_generation, \
					 suspend_incarnation_epoch FROM sandbox_ownership WHERE deleting = FALSE AND \
					 lifecycle_state IN ('suspending', 'suspended', 'resuming') ORDER BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(|row| {
					let generation = row
						.try_get::<_, Option<i64>>(5)
						.map_err(database_error)?
						.ok_or_else(|| {
							EngineError::engine("non-serving lifecycle marker has no generation")
						})?;
					Ok(SuspensionMarker {
						sid:               row.try_get(0).map_err(database_error)?,
						owner:             row.try_get(1).map_err(database_error)?,
						epoch:             row.try_get(2).map_err(database_error)?,
						incarnation_epoch: row
							.try_get::<_, Option<i64>>(6)
							.map_err(database_error)?
							.unwrap_or(0),
						state:             row.try_get(3).map_err(database_error)?,
						point:             row
							.try_get::<_, Option<String>>(4)
							.map_err(database_error)?
							.ok_or_else(|| {
								EngineError::engine("non-serving lifecycle marker has no point")
							})?,
						generation:        u64::try_from(generation)
							.map_err(|_| EngineError::engine("negative suspend generation"))?,
					})
				})
				.collect()
		})
	}

	/// Read the durable marker for one sandbox. Callers must use its point
	/// verbatim; selecting a newer history entry would violate suspend's
	/// recovery contract.
	pub(crate) fn suspend_marker(&self, sid: &str) -> Result<Option<SuspensionMarker>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, owner, epoch, lifecycle_state, suspend_point, suspend_generation, \
					 suspend_incarnation_epoch FROM sandbox_ownership WHERE sid = $1 AND deleting = \
					 FALSE AND lifecycle_state IN ('suspending', 'suspended', 'resuming')",
					&[&sid],
				)
				.map_err(database_error)?
				.map(|row| {
					let generation = row
						.try_get::<_, Option<i64>>(5)
						.map_err(database_error)?
						.ok_or_else(|| {
							EngineError::engine("non-serving lifecycle marker has no generation")
						})?;
					Ok(SuspensionMarker {
						sid:               row.try_get(0).map_err(database_error)?,
						owner:             row.try_get(1).map_err(database_error)?,
						epoch:             row.try_get(2).map_err(database_error)?,
						incarnation_epoch: row
							.try_get::<_, Option<i64>>(6)
							.map_err(database_error)?
							.unwrap_or(0),
						state:             row.try_get(3).map_err(database_error)?,
						point:             row
							.try_get::<_, Option<String>>(4)
							.map_err(database_error)?
							.ok_or_else(|| {
								EngineError::engine("non-serving lifecycle marker has no point")
							})?,
						generation:        u64::try_from(generation)
							.map_err(|_| EngineError::engine("negative suspend generation"))?,
					})
				})
				.transpose()
		})
	}

	pub(crate) fn list_rollback_markers(&self) -> Result<Vec<RollbackMarker>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, owner, epoch, rollback_target, rollback_safety, \
					 rollback_operation_generation, rollback_checkpoint_generation, \
					 rollback_source_owner, rollback_source_epoch, rollback_incarnation_epoch FROM \
					 sandbox_ownership WHERE deleting = FALSE AND lifecycle_state = 'rolling_back' AND \
					 rollback_source_owner IS NOT NULL AND rollback_source_epoch IS NOT NULL AND \
					 EXISTS (SELECT 1 FROM portable_recovery_points p WHERE p.sid = \
					 sandbox_ownership.sid AND p.name = rollback_target AND p.owner_node = \
					 rollback_source_owner AND p.owner_epoch = rollback_source_epoch) AND EXISTS \
					 (SELECT 1 FROM portable_recovery_points p WHERE p.sid = sandbox_ownership.sid AND \
					 p.name = rollback_safety AND p.owner_node = rollback_source_owner AND \
					 p.owner_epoch = rollback_source_epoch) ORDER BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(|row| {
					Ok(RollbackMarker {
						sid:                   row.try_get(0).map_err(database_error)?,
						incarnation_epoch:     row
							.try_get::<_, Option<i64>>(9)
							.map_err(database_error)?
							.unwrap_or(0),
						owner:                 row.try_get(1).map_err(database_error)?,
						epoch:                 row.try_get(2).map_err(database_error)?,
						target:                row
							.try_get::<_, Option<String>>(3)
							.map_err(database_error)?
							.ok_or_else(|| EngineError::engine("rollback marker has no target"))?,
						safety:                row
							.try_get::<_, Option<String>>(4)
							.map_err(database_error)?
							.ok_or_else(|| EngineError::engine("rollback marker has no safety point"))?,
						operation_generation:  u64::try_from(
							row.try_get::<_, Option<i64>>(5)
								.map_err(database_error)?
								.ok_or_else(|| {
									EngineError::engine("rollback marker has no operation generation")
								})?,
						)
						.map_err(|_| EngineError::engine("negative rollback operation generation"))?,
						checkpoint_generation: u64::try_from(
							row.try_get::<_, Option<i64>>(6)
								.map_err(database_error)?
								.ok_or_else(|| {
									EngineError::engine("rollback marker has no checkpoint generation")
								})?,
						)
						.map_err(|_| EngineError::engine("negative rollback checkpoint generation"))?,
						source_owner:          row
							.try_get::<_, Option<String>>(7)
							.map_err(database_error)?
							.ok_or_else(|| EngineError::engine("rollback marker has no source owner"))?,
						source_epoch:          row
							.try_get::<_, Option<i64>>(8)
							.map_err(database_error)?
							.ok_or_else(|| EngineError::engine("rollback marker has no source epoch"))?,
					})
				})
				.collect()
		})
	}

	pub(crate) fn rollback_marker(&self, sid: &str) -> Result<Option<RollbackMarker>> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT sid, owner, epoch, rollback_target, rollback_safety, \
					 rollback_operation_generation, rollback_checkpoint_generation, \
					 rollback_source_owner, rollback_source_epoch FROM sandbox_ownership WHERE sid = \
					 $1 AND deleting = FALSE AND lifecycle_state = 'rolling_back' AND \
					 rollback_source_owner IS NOT NULL AND rollback_source_epoch IS NOT NULL AND \
					 EXISTS (SELECT 1 FROM portable_recovery_points p WHERE p.sid = \
					 sandbox_ownership.sid AND p.name = rollback_target AND p.owner_node = \
					 rollback_source_owner AND p.owner_epoch = rollback_source_epoch) AND EXISTS \
					 (SELECT 1 FROM portable_recovery_points p WHERE p.sid = sandbox_ownership.sid AND \
					 p.name = rollback_safety AND p.owner_node = rollback_source_owner AND \
					 p.owner_epoch = rollback_source_epoch)",
					&[&sid],
				)
				.map_err(database_error)?
				.map(row_to_rollback_marker)
				.transpose()
		})
	}

	/// Resolve rollback-journal replay exclusively from one exact ownership
	/// generation. No caller-local registry state participates in this result.
	pub(crate) fn rollback_disposition(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
	) -> Result<RollbackDisposition> {
		self.with_client(|client| {
			let row = client
				.query_opt(
					"SELECT lifecycle_state, rollback_target, rollback_safety, \
					 rollback_operation_generation, rollback_checkpoint_generation, \
					 rollback_source_owner, rollback_source_epoch, rollback_incarnation_epoch FROM \
					 sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 AND deleting = \
					 FALSE",
					&[&sid, &owner, &epoch],
				)
				.map_err(database_error)?;
			let Some(row) = row else {
				return Ok(RollbackDisposition::Other);
			};
			let state = row.try_get::<_, String>(0).map_err(database_error)?;
			if state == "running"
				&& row
					.try_get::<_, Option<String>>(1)
					.map_err(database_error)?
					.is_none()
				&& row
					.try_get::<_, Option<String>>(2)
					.map_err(database_error)?
					.is_none()
			{
				return Ok(RollbackDisposition::CommittedRunning);
			}
			if state != "rolling_back" {
				return Ok(RollbackDisposition::Other);
			}
			let target = row
				.try_get::<_, Option<String>>(1)
				.map_err(database_error)?;
			let safety = row
				.try_get::<_, Option<String>>(2)
				.map_err(database_error)?;
			let operation_generation = row.try_get::<_, Option<i64>>(3).map_err(database_error)?;
			let checkpoint_generation = row.try_get::<_, Option<i64>>(4).map_err(database_error)?;
			let source_owner = row
				.try_get::<_, Option<String>>(5)
				.map_err(database_error)?;
			let source_epoch = row.try_get::<_, Option<i64>>(6).map_err(database_error)?;
			let incarnation_epoch = row.try_get::<_, Option<i64>>(7).map_err(database_error)?;
			let (
				Some(target),
				Some(safety),
				Some(operation_generation),
				Some(checkpoint_generation),
				Some(source_owner),
				Some(source_epoch),
				Some(incarnation_epoch),
			) = (
				target,
				safety,
				operation_generation,
				checkpoint_generation,
				source_owner,
				source_epoch,
				incarnation_epoch,
			)
			else {
				return Ok(RollbackDisposition::Other);
			};
			let source_points = client
				.query_one(
					"SELECT EXISTS (SELECT 1 FROM portable_recovery_points WHERE sid = $1 AND name = \
					 $2 AND owner_node = $3 AND owner_epoch = $4 AND incarnation_epoch = $6) AND \
					 EXISTS (SELECT 1 FROM portable_recovery_points WHERE sid = $1 AND name = $5 AND \
					 owner_node = $3 AND owner_epoch = $4 AND incarnation_epoch = $6)",
					&[&sid, &target, &source_owner, &source_epoch, &safety, &incarnation_epoch],
				)
				.map_err(database_error)?
				.try_get::<_, bool>(0)
				.map_err(database_error)?;
			if !source_points {
				return Ok(RollbackDisposition::Other);
			}
			Ok(RollbackDisposition::RollingBack(RollbackMarker {
				sid: sid.to_owned(),
				incarnation_epoch,
				owner: owner.to_owned(),
				epoch,
				target,
				source_owner,
				source_epoch,
				safety,
				operation_generation: u64::try_from(operation_generation)
					.map_err(|_| EngineError::engine("negative rollback operation generation"))?,
				checkpoint_generation: u64::try_from(checkpoint_generation)
					.map_err(|_| EngineError::engine("negative rollback checkpoint generation"))?,
			}))
		})
	}

	/// Stage a claimed suspended point for restoration. This is deliberately
	/// separate from generic restore staging: only an exact durable suspended
	/// marker may enter this state.
	pub(crate) fn begin_resume(&self, sid: &str, owner: &str, epoch: i64) -> Result<bool> {
		self.with_client(|client| {
			let changed = client
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'resuming' WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 \
					 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = \
					 FALSE AND lifecycle_state = 'suspended'",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?;
			if changed == 1 {
				return Ok(true);
			}
			let state = client
				.query_opt(
					"SELECT lifecycle_state FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND \
					 epoch = $3 AND deleting = FALSE",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?
				.map(|row| row.try_get::<_, String>(0).map_err(database_error))
				.transpose()?;
			Ok(state.as_deref() == Some("resuming"))
		})
	}

	/// Make a restored candidate serving only after its exact durable create
	/// metadata is updated. This is the sole path that removes the marker.
	pub(crate) fn finish_resume(&self, record: &CreateRecord) -> Result<()> {
		let params = serde_json::to_string(&record.params)?;
		let changed = self.with_client(|client| {
			client
				.execute(
					"UPDATE sandbox_ownership SET params = $4, ha = $5, restart_policy = $6, \
					 created_at = $7, lifecycle_state = 'running', suspend_point = NULL, \
					 suspend_generation = NULL, suspend_incarnation_epoch = NULL WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 \
					 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = \
					 FALSE AND (lifecycle_state = 'resuming' OR (lifecycle_state = 'running' AND \
					 params = $4 AND ha = $5 AND restart_policy = $6 AND created_at = $7))",
					&[
						&record.sid as &(dyn ToSql + Sync),
						&record.owner,
						&record.epoch,
						&params,
						&record.ha,
						&record.restart_policy,
						&record.created_at,
					],
				)
				.map_err(database_error)
		})?;
		if changed != 1 {
			return Err(EngineError::busy(format!("resume commit for {} lost ownership", record.sid)));
		}
		Ok(())
	}

	pub(crate) fn abort_resume(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		self.with_client(|client| {
			let changed = client
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = 'suspended' WHERE sid = $1 AND \
					 owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 \
					 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = \
					 FALSE AND lifecycle_state = 'resuming'",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?;
			if changed == 1 {
				return Ok(());
			}
			let suspended = client
				.query_opt(
					"SELECT 1 FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = FALSE AND lifecycle_state = \
					 'suspended'",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)?
				.is_some();
			if suspended {
				Ok(())
			} else {
				Err(EngineError::busy(format!("resume abort for {sid} lost ownership")))
			}
		})
	}

	/// Tombstone an exact live ownership generation before external history
	/// cleanup. Higher-epoch claims may supersede it; this generation cannot
	/// be silently resumed.
	pub(crate) fn begin_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let token = delete_token(owner, epoch);
		self.with_client(|client| {
			let updated = client
				.execute(
					"UPDATE sandbox_ownership SET deleting = TRUE, delete_token = $4 WHERE sid = $1 \
					 AND owner = $2 AND epoch = $3 AND owner_lease_owner = $2 AND owner_lease_epoch = \
					 $3 AND owner_lease_expires_at > EXTRACT(EPOCH FROM clock_timestamp()) AND \
					 (deleting = FALSE OR delete_token = $4)",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &token],
				)
				.map_err(database_error)?;
			if updated == 1 {
				Ok(())
			} else {
				Err(EngineError::invalid(format!("delete tombstone for {sid} lost ownership")))
			}
		})
	}

	/// List durable deletion tombstones for crash-safe history cleanup retry.
	pub(crate) fn list_deleting(&self) -> Result<Vec<CreateRecord>> {
		self.with_client(|client| {
			client
				.query(
					"SELECT sid, params, owner, epoch, idempotency_key, ha, restart_policy, created_at \
					 FROM sandbox_ownership WHERE deleting = TRUE ORDER BY sid",
					&[],
				)
				.map_err(database_error)?
				.iter()
				.map(record_from_row)
				.collect()
		})
	}

	/// Remove a sandbox only after external history cleanup committed under the
	/// exact durable tombstone. Replica metadata is removed in the same
	/// transaction; deleting the ownership row releases its idempotency index.
	pub(crate) fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let token = delete_token(owner, epoch);
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let deleted = transaction
				.execute(
					"WITH deleted AS (DELETE FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND \
					 epoch = $3 AND deleting = TRUE AND delete_token = $4 RETURNING sid, epoch) INSERT \
					 INTO sandbox_epoch_floors (sid, last_epoch) SELECT sid, epoch FROM deleted ON \
					 CONFLICT (sid) DO UPDATE SET last_epoch = \
					 GREATEST(sandbox_epoch_floors.last_epoch, EXCLUDED.last_epoch)",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch, &token],
				)
				.map_err(database_error)?;
			if deleted != 1 {
				return Err(EngineError::invalid(format!("delete commit for {sid} lost tombstone")));
			}
			transaction
				.execute("DELETE FROM sandbox_replicas WHERE sid = $1", &[&sid])
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)
		})
	}

	/// Atomically allocate the next checkpoint generation for the exact
	/// authoritative owner epoch. Restored params cannot rewind this durable
	/// counter and a fenced owner cannot allocate a publishable checkpoint.
	pub(crate) fn allocate_checkpoint_generation(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
	) -> Result<u64> {
		if owner.is_empty() || epoch < 0 {
			return Err(EngineError::invalid("invalid checkpoint generation allocation"));
		}
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("sandbox:{sid}"))?;
			let row = transaction
				.query_opt(
					"SELECT owner, epoch, checkpoint_generation FROM sandbox_ownership WHERE sid = $1 \
					 AND owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at \
					 > EXTRACT(EPOCH FROM clock_timestamp()) AND deleting = FALSE FOR UPDATE",
					&[&sid, &owner, &epoch],
				)
				.map_err(database_error)?
				.ok_or_else(|| {
					EngineError::not_found(format!("sandbox {sid} has no ownership record"))
				})?;
			let current_owner = row.try_get::<_, String>(0).map_err(database_error)?;
			let current_epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
			if current_owner != owner || current_epoch != epoch {
				return Err(fencing_error(sid, owner, epoch, &current_owner, current_epoch));
			}
			let next = row
				.try_get::<_, i64>(2)
				.map_err(database_error)?
				.checked_add(1)
				.ok_or_else(|| {
					EngineError::invalid("checkpoint generation exceeds PostgreSQL range")
				})?;
			transaction
				.execute("UPDATE sandbox_ownership SET checkpoint_generation = $2 WHERE sid = $1", &[
					&sid as &(dyn ToSql + Sync),
					&next,
				])
				.map_err(database_error)?;
			transaction.commit().map_err(database_error)?;
			u64::try_from(next).map_err(|_| EngineError::engine("checkpoint generation is negative"))
		})
	}

	/// Return whether this node still owns exactly the generation it claimed.
	pub(crate) fn owns_epoch(&self, sid: &str, owner: &str, epoch: i64) -> Result<bool> {
		self.with_client(|client| {
			client
				.query_opt(
					"SELECT 1 FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3 AND \
					 owner_lease_owner = $2 AND owner_lease_epoch = $3 AND owner_lease_expires_at > \
					 EXTRACT(EPOCH FROM clock_timestamp())",
					&[&sid as &(dyn ToSql + Sync), &owner, &epoch],
				)
				.map_err(database_error)
				.map(|row| row.is_some())
		})
	}

	/// Acquire or renew a cluster-wide writable-volume lease.
	pub(crate) fn acquire_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
		ttl: f64,
	) -> Result<LeaseRecord> {
		let epoch = i64::try_from(epoch)
			.map_err(|_| EngineError::invalid("lease epoch exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			let mut transaction = client.transaction().map_err(database_error)?;
			advisory_lock(&mut transaction, &format!("volume:{volume}"))?;
			let db_now = transaction
				.query_one("SELECT EXTRACT(EPOCH FROM clock_timestamp())::DOUBLE PRECISION", &[])
				.map_err(database_error)?
				.try_get::<_, f64>(0)
				.map_err(database_error)?;
			let row = transaction
				.query_opt(
					"SELECT holder, epoch, granted_at, ttl FROM volume_leases WHERE volume = $1 FOR \
					 UPDATE",
					&[&volume],
				)
				.map_err(database_error)?;
			if let Some(row) = row {
				let current_holder = row.try_get::<_, String>(0).map_err(database_error)?;
				let current_epoch = row.try_get::<_, i64>(1).map_err(database_error)?;
				let granted_at = row.try_get::<_, f64>(2).map_err(database_error)?;
				let current_ttl = row.try_get::<_, f64>(3).map_err(database_error)?;
				let expired = granted_at + current_ttl <= db_now;
				let renewal = current_holder == holder && epoch >= current_epoch;
				if !expired && !renewal {
					return Err(EngineError::busy(format!(
						"Lease collision: volume {volume} is leased by {current_holder} at epoch \
						 {current_epoch}"
					)));
				}
				if renewal && epoch < current_epoch {
					return Err(EngineError::invalid(format!(
						"stale lease epoch {epoch} for volume {volume}; current epoch is {current_epoch}"
					)));
				}
				transaction
					.execute(
						"UPDATE volume_leases SET holder = $2, epoch = $3, granted_at = EXTRACT(EPOCH \
						 FROM clock_timestamp()), ttl = $4 WHERE volume = $1",
						&[&volume as &(dyn ToSql + Sync), &holder, &epoch, &ttl],
					)
					.map_err(database_error)?;
			} else {
				transaction
					.execute(
						"INSERT INTO volume_leases (volume, holder, epoch, granted_at, ttl) VALUES ($1, \
						 $2, $3, EXTRACT(EPOCH FROM clock_timestamp()), $4)",
						&[&volume as &(dyn ToSql + Sync), &holder, &epoch, &ttl],
					)
					.map_err(database_error)?;
			}
			let record = transaction
				.query_one("SELECT granted_at, ttl FROM volume_leases WHERE volume = $1", &[&volume])
				.map_err(database_error)?;
			let record = LeaseRecord::new(
				volume,
				holder,
				epoch as u64,
				record.try_get::<_, f64>(0).map_err(database_error)?,
				record.try_get::<_, f64>(1).map_err(database_error)?,
			)?;
			transaction.commit().map_err(database_error)?;
			Ok(record)
		})
	}

	/// Release the exact writable-volume lease generation held by a node.
	pub(crate) fn release_lease(&self, volume: &str, holder: &str, epoch: u64) -> Result<()> {
		let epoch = i64::try_from(epoch)
			.map_err(|_| EngineError::invalid("lease epoch exceeds PostgreSQL range"))?;
		self.with_client(|client| {
			client
				.execute(
					"DELETE FROM volume_leases WHERE volume = $1 AND holder = $2 AND epoch = $3",
					&[&volume as &(dyn ToSql + Sync), &holder, &epoch],
				)
				.map_err(database_error)?;
			Ok(())
		})
	}

	#[cfg(test)]
	pub(crate) fn reserve_fixture_epoch(
		&self,
		sid: &str,
		owner: &str,
		requested_epoch: i64,
		idempotency_key: &str,
	) -> Result<i64> {
		if requested_epoch < 0 {
			return Err(EngineError::invalid("fixture ownership epoch is negative"));
		}
		self.with_client(|client| {
			crate::portable_history::migrate_for_test(client)?;
			if requested_epoch > 0 {
				let predecessor = requested_epoch - 1;
				client
					.execute(
						"INSERT INTO sandbox_epoch_floors (sid, last_epoch) VALUES ($1, $2) ON CONFLICT \
						 (sid) DO UPDATE SET last_epoch = GREATEST(sandbox_epoch_floors.last_epoch, \
						 EXCLUDED.last_epoch)",
						&[&sid, &predecessor],
					)
					.map_err(database_error)?;
			}
			Ok(())
		})?;
		self.reserve_create_epoch(sid, owner, idempotency_key)
	}

	#[cfg(test)]
	pub(crate) fn expire_owner_for_test(&self, sid: &str) -> Result<()> {
		self.with_client(|client| {
			let changed = client
				.execute("UPDATE sandbox_ownership SET owner_lease_expires_at = 0 WHERE sid = $1", &[
					&sid,
				])
				.map_err(database_error)?;
			if changed != 1 {
				return Err(EngineError::not_found(format!("sandbox {sid} has no ownership record")));
			}
			Ok(())
		})
	}

	#[cfg(test)]
	pub(crate) fn clear_for_test(&self) -> Result<()> {
		self.with_client(|client| {
			crate::portable_history::migrate_for_test(client)?;
			client
				.batch_execute(
					"TRUNCATE portable_rollback_pins, portable_recovery_points, tenant_credentials, \
					 sandbox_create_reservations, sandbox_epoch_floors, sandbox_ownership, \
					 sandbox_replicas, volume_leases",
				)
				.map_err(database_error)
		})
	}
}

fn cluster_object_namespace(cluster_id: &str) -> String {
	format!("clusters/{cluster_id}")
}

/// Canonical advisory-lock identity for one immutable replica bundle.
///
/// Publication, readers, and collectors must all use this exact key.
pub(crate) fn replica_object_lock_key(object_key: &str) -> String {
	format!("replica-object:{object_key}")
}

fn bootstrap_cluster_id(client: &mut Client) -> Result<String> {
	let generated = Uuid::new_v4().to_string();
	client
		.execute(
			"INSERT INTO cluster_identity (singleton, cluster_id) VALUES (TRUE, $1) ON CONFLICT \
			 (singleton) DO NOTHING",
			&[&generated],
		)
		.map_err(database_error)?;
	client
		.query_one("SELECT cluster_id FROM cluster_identity WHERE singleton = TRUE", &[])
		.map_err(database_error)?
		.try_get::<_, String>(0)
		.map_err(database_error)
}
fn migrate(client: &mut Client) -> Result<()> {
	client
		.batch_execute(
			r"
			CREATE TABLE IF NOT EXISTS cluster_identity (
				singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
				cluster_id TEXT NOT NULL
			);
			CREATE TABLE IF NOT EXISTS cluster_key_verifiers (
				key_id TEXT PRIMARY KEY,
				verifier TEXT NOT NULL
			);
			CREATE TABLE IF NOT EXISTS tenant_credentials (
				tenant TEXT NOT NULL,
				name TEXT NOT NULL,
				key_id TEXT NOT NULL,
				ciphertext BYTEA NOT NULL,
				allowed_domains TEXT NOT NULL,
				header_names TEXT NOT NULL,
				expires_at_unix_millis BIGINT,
				requests_per_minute BIGINT NOT NULL,
				version TEXT NOT NULL,
				PRIMARY KEY (tenant, name)
			);
			CREATE TABLE IF NOT EXISTS sandbox_epoch_floors (
				sid TEXT PRIMARY KEY,
				last_epoch BIGINT NOT NULL
			);
			CREATE TABLE IF NOT EXISTS sandbox_create_reservations (
				sid TEXT PRIMARY KEY,
				epoch BIGINT NOT NULL,
				incarnation_epoch BIGINT NOT NULL DEFAULT 0,
				owner TEXT NOT NULL DEFAULT '',
				idempotency_key TEXT NOT NULL DEFAULT '',
				expires_at DOUBLE PRECISION NOT NULL DEFAULT 0
			);
			ALTER TABLE sandbox_create_reservations ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT '';
			ALTER TABLE sandbox_create_reservations ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';
			ALTER TABLE sandbox_create_reservations ADD COLUMN IF NOT EXISTS expires_at DOUBLE PRECISION NOT NULL DEFAULT 0;
			ALTER TABLE sandbox_create_reservations ADD COLUMN IF NOT EXISTS incarnation_epoch BIGINT;
			UPDATE sandbox_create_reservations SET incarnation_epoch = epoch WHERE incarnation_epoch IS NULL;
			ALTER TABLE sandbox_create_reservations ALTER COLUMN incarnation_epoch SET NOT NULL;
			CREATE TABLE IF NOT EXISTS sandbox_ownership (
				sid TEXT PRIMARY KEY,
				owner TEXT NOT NULL DEFAULT '',
				epoch BIGINT NOT NULL DEFAULT 0,
				incarnation_epoch BIGINT NOT NULL DEFAULT 0,
				idempotency_key TEXT,
				params TEXT NOT NULL,
				ha TEXT NOT NULL,
				restart_policy TEXT NOT NULL,
				created_at DOUBLE PRECISION NOT NULL,
				checkpoint_generation BIGINT NOT NULL DEFAULT 0,
				owner_lease_owner TEXT NOT NULL DEFAULT '',
				owner_lease_epoch BIGINT NOT NULL DEFAULT 0,
				owner_lease_expires_at DOUBLE PRECISION NOT NULL DEFAULT 0
			);
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS owner TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS epoch BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS params TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS ha TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS restart_policy TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS created_at DOUBLE PRECISION;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS owner_lease_owner TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS owner_lease_epoch BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS owner_lease_expires_at DOUBLE PRECISION;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS checkpoint_generation BIGINT;
			UPDATE sandbox_ownership SET owner = '' WHERE owner IS NULL;
			UPDATE sandbox_ownership SET epoch = 0 WHERE epoch IS NULL;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS incarnation_epoch BIGINT;
			UPDATE sandbox_ownership SET incarnation_epoch = epoch WHERE incarnation_epoch IS NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN incarnation_epoch SET NOT NULL;
			INSERT INTO sandbox_epoch_floors (sid, last_epoch)
			SELECT sid, epoch FROM sandbox_ownership
			ON CONFLICT (sid) DO UPDATE
			SET last_epoch = GREATEST(sandbox_epoch_floors.last_epoch, EXCLUDED.last_epoch);
			UPDATE sandbox_ownership SET params = '{}' WHERE params IS NULL;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_target TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_token TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_state TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_id TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_source TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS deleting BOOLEAN NOT NULL DEFAULT FALSE;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS delete_token TEXT;
			UPDATE sandbox_ownership SET ha = 'off' WHERE ha IS NULL;
			UPDATE sandbox_ownership SET restart_policy = 'none' WHERE restart_policy IS NULL;
			UPDATE sandbox_ownership SET created_at = 0 WHERE created_at IS NULL;
			UPDATE sandbox_ownership
			SET owner_lease_owner = owner,
				owner_lease_epoch = epoch,
				owner_lease_expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + 15
			WHERE owner_lease_owner IS NULL
				OR owner_lease_epoch IS NULL
				OR owner_lease_expires_at IS NULL
				OR (owner_lease_owner = '' AND owner_lease_epoch = 0 AND owner_lease_expires_at = 0);
			UPDATE sandbox_ownership SET owner_lease_owner = owner WHERE owner_lease_owner IS NULL;
			UPDATE sandbox_ownership SET owner_lease_epoch = epoch WHERE owner_lease_epoch IS NULL;
			UPDATE sandbox_ownership SET owner_lease_expires_at = 0 WHERE owner_lease_expires_at IS NULL;
			UPDATE sandbox_ownership SET checkpoint_generation = 0 WHERE checkpoint_generation IS NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN owner SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN epoch SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN owner SET DEFAULT '';
			ALTER TABLE sandbox_ownership ALTER COLUMN epoch SET DEFAULT 0;
			ALTER TABLE sandbox_ownership ALTER COLUMN params SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN ha SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN restart_policy SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN created_at SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN owner_lease_owner SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN owner_lease_epoch SET NOT NULL;
			ALTER TABLE sandbox_ownership ALTER COLUMN owner_lease_expires_at SET NOT NULL;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS migration_claimed BOOLEAN NOT NULL DEFAULT FALSE;
			ALTER TABLE sandbox_ownership ALTER COLUMN checkpoint_generation SET NOT NULL;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS lifecycle_state TEXT NOT NULL DEFAULT 'running';
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS suspend_point TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS suspend_generation BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_target TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_safety TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_operation_generation BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_checkpoint_generation BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS suspend_incarnation_epoch BIGINT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_incarnation_epoch BIGINT;
			UPDATE sandbox_ownership SET suspend_incarnation_epoch = incarnation_epoch
			WHERE suspend_point IS NOT NULL AND suspend_incarnation_epoch IS NULL;
			UPDATE sandbox_ownership SET rollback_incarnation_epoch = incarnation_epoch
			WHERE rollback_target IS NOT NULL AND rollback_incarnation_epoch IS NULL;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_source_owner TEXT;
			ALTER TABLE sandbox_ownership ADD COLUMN IF NOT EXISTS rollback_source_epoch BIGINT;
			CREATE TABLE IF NOT EXISTS sandbox_replicas (
				sid TEXT PRIMARY KEY,
				digest TEXT NOT NULL,
				object_key TEXT NOT NULL,
				source_node TEXT NOT NULL,
				source_epoch BIGINT NOT NULL DEFAULT 0,
				checkpoint_generation BIGINT NOT NULL DEFAULT 0,
				snapshot_dir TEXT NOT NULL,
				params TEXT NOT NULL,
				needs_secrets BOOLEAN NOT NULL,
				updated_at DOUBLE PRECISION NOT NULL
			);
			ALTER TABLE sandbox_replicas ADD COLUMN IF NOT EXISTS object_key TEXT NOT NULL DEFAULT '';
			DELETE FROM sandbox_replicas WHERE object_key = '';
			ALTER TABLE sandbox_replicas ADD COLUMN IF NOT EXISTS source_epoch BIGINT;
			ALTER TABLE sandbox_replicas ADD COLUMN IF NOT EXISTS checkpoint_generation BIGINT;
			UPDATE sandbox_replicas AS replica SET source_epoch = ownership.epoch
				FROM sandbox_ownership AS ownership
				WHERE replica.source_epoch IS NULL AND replica.sid = ownership.sid
					AND replica.source_node = ownership.owner;
			UPDATE sandbox_replicas SET source_epoch = 0 WHERE source_epoch IS NULL;
			UPDATE sandbox_replicas SET checkpoint_generation = 0
				WHERE checkpoint_generation IS NULL;
			ALTER TABLE sandbox_replicas ALTER COLUMN source_epoch SET NOT NULL;
			ALTER TABLE sandbox_replicas ALTER COLUMN checkpoint_generation SET NOT NULL;

			CREATE TABLE IF NOT EXISTS volume_leases (
				volume TEXT PRIMARY KEY,
				holder TEXT NOT NULL,
				epoch BIGINT NOT NULL,
				granted_at DOUBLE PRECISION NOT NULL,
				ttl DOUBLE PRECISION NOT NULL
			);
			",
		)
		.map_err(database_error)
}

fn advisory_lock(transaction: &mut Transaction<'_>, key: &str) -> Result<()> {
	transaction
		.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key])
		.map_err(database_error)?;
	Ok(())
}

/// Require an already committed portable point while the caller holds
/// `portable-history:{sid}` followed by `sandbox:{sid}`. Marker writers use
/// this before publishing any non-serving ownership reference.
fn require_portable_recovery_point(
	transaction: &mut Transaction<'_>,
	sid: &str,
	point: &str,
) -> Result<()> {
	let exists = transaction
		.query_opt("SELECT 1 FROM portable_recovery_points WHERE sid = $1 AND name = $2", &[
			&sid, &point,
		])
		.map_err(database_error)?
		.is_some();
	if exists {
		Ok(())
	} else {
		Err(EngineError::not_found(format!(
			"portable recovery point {point:?} for sandbox {sid} is not committed"
		)))
	}
}

/// Suspend markers may only cite a point from their exact serving lineage.
fn require_current_portable_recovery_point(
	transaction: &mut Transaction<'_>,
	sid: &str,
	point: &str,
	owner: &str,
	epoch: i64,
) -> Result<()> {
	let exists = transaction
		.query_opt(
			"SELECT 1 FROM portable_recovery_points WHERE sid = $1 AND name = $2 AND owner_node = $3 \
			 AND owner_epoch = $4",
			&[&sid, &point, &owner, &epoch],
		)
		.map_err(database_error)?
		.is_some();
	if exists {
		Ok(())
	} else {
		Err(EngineError::not_found(format!(
			"portable recovery point {point:?} for sandbox {sid} is not committed for this owner"
		)))
	}
}

fn tenant_credential_from_row(row: &Row) -> Result<TenantCredential> {
	Ok(TenantCredential {
		tenant:                 row.try_get(0).map_err(database_error)?,
		name:                   row.try_get(1).map_err(database_error)?,
		key_id:                 row.try_get(2).map_err(database_error)?,
		ciphertext:             row.try_get(3).map_err(database_error)?,
		allowed_domains:        row.try_get(4).map_err(database_error)?,
		header_names:           row.try_get(5).map_err(database_error)?,
		expires_at_unix_millis: row.try_get(6).map_err(database_error)?,
		requests_per_minute:    row.try_get(7).map_err(database_error)?,
		version:                row.try_get(8).map_err(database_error)?,
	})
}

fn record_from_row(row: &Row) -> Result<CreateRecord> {
	let sid = row.try_get::<_, String>(0).map_err(database_error)?;
	let params = row.try_get::<_, String>(1).map_err(database_error)?;
	let params = serde_json::from_str(&params).map_err(|error| {
		EngineError::engine(format!("invalid sandbox metadata for {sid}: {error}"))
	})?;
	let owner = row.try_get::<_, String>(2).map_err(database_error)?;
	let epoch = row.try_get::<_, i64>(3).map_err(database_error)?;
	let incarnation_epoch = row.try_get::<_, i64>("incarnation_epoch").unwrap_or(epoch);
	let idempotency_key = row
		.try_get::<_, Option<String>>(4)
		.map_err(database_error)?
		.unwrap_or_default();
	let ha = row.try_get::<_, String>(5).map_err(database_error)?;
	let restart_policy = row.try_get::<_, String>(6).map_err(database_error)?;
	let created_at = row.try_get::<_, f64>(7).map_err(database_error)?;
	let mut record = CreateRecord::new(
		sid,
		params,
		owner,
		epoch,
		idempotency_key,
		ha,
		restart_policy,
		created_at,
	)?;
	record.incarnation_epoch = incarnation_epoch;
	Ok(record)
}

fn delete_token(owner: &str, epoch: i64) -> String {
	format!("delete:{owner}:{epoch}")
}

fn record_without_secrets(record: &CreateRecord) -> CreateRecord {
	let mut durable = record.clone();
	durable.params.remove("secrets");
	durable
}

fn replica_from_row(row: &Row) -> Result<ReplicaRecord> {
	let sid = row.try_get::<_, String>(0).map_err(database_error)?;
	let digest = row.try_get::<_, String>(1).map_err(database_error)?;
	let object_key = row.try_get::<_, String>(2).map_err(database_error)?;
	let source_node = row.try_get::<_, String>(3).map_err(database_error)?;
	let source_epoch = row.try_get::<_, i64>(4).map_err(database_error)?;
	let checkpoint_generation = row.try_get::<_, i64>(5).map_err(database_error)?;
	let snapshot_dir = row.try_get::<_, String>(6).map_err(database_error)?;
	let params = row.try_get::<_, String>(7).map_err(database_error)?;
	let params = serde_json::from_str(&params).map_err(|error| {
		EngineError::engine(format!("invalid replica metadata for {sid}: {error}"))
	})?;
	let needs_secrets = row.try_get::<_, bool>(8).map_err(database_error)?;
	ReplicaRecord::new_fenced(
		sid,
		digest,
		source_node,
		u64::try_from(source_epoch)
			.map_err(|_| EngineError::engine("replica source epoch is negative"))?,
		u64::try_from(checkpoint_generation)
			.map_err(|_| EngineError::engine("replica checkpoint generation is negative"))?,
		snapshot_dir,
		params,
		needs_secrets,
	)?
	.with_object_key(object_key)
}

fn optional_key(key: &str) -> Option<&str> {
	(!key.is_empty()).then_some(key)
}

/// Return the fresh epoch assigned when a failed restore returns ownership to
/// its prior node. The original generation must never be reused.
fn release_claim_epoch(previous_epoch: i64, claimed_epoch: i64) -> Result<i64> {
	if previous_epoch < 0
		|| claimed_epoch < 1
		|| claimed_epoch
			!= previous_epoch
				.checked_add(1)
				.ok_or_else(|| EngineError::invalid("restore claim epoch exceeds PostgreSQL range"))?
	{
		return Err(EngineError::invalid("invalid restore claim release"));
	}
	claimed_epoch
		.checked_add(1)
		.ok_or_else(|| EngineError::invalid("ownership epoch exceeds PostgreSQL range"))
}

/// Decide whether an incoming checkpoint may replace a durable replica row.
fn replica_overwrite_required(
	sid: &str,
	current_digest: &str,
	current_object_key: &str,
	current_source_epoch: i64,
	current_generation: i64,
	incoming_digest: &str,
	incoming_object_key: &str,
	incoming_source_epoch: i64,
	incoming_generation: i64,
) -> Result<bool> {
	let current = (current_source_epoch, current_generation);
	let incoming = (incoming_source_epoch, incoming_generation);
	if current > incoming {
		return Ok(false);
	}
	if current == incoming {
		if current_digest == incoming_digest && current_object_key == incoming_object_key {
			return Ok(false);
		}
		return Err(EngineError::invalid(format!(
			"replica {sid} lineage ({incoming_source_epoch}, {incoming_generation}) has conflicting \
			 object identity"
		)));
	}
	Ok(true)
}
fn fencing_error(
	sid: &str,
	owner: &str,
	epoch: i64,
	current_owner: &str,
	current_epoch: i64,
) -> EngineError {
	EngineError::invalid(format!(
		"Fencing error: ownership claim for {sid} by {owner} at epoch {epoch} was fenced by \
		 {current_owner} at epoch {current_epoch}"
	))
}

fn unix_now() -> Result<f64> {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_secs_f64())
		.map_err(|error| EngineError::engine(format!("system clock precedes Unix epoch: {error}")))
}

fn row_to_rollback_marker(row: Row) -> Result<RollbackMarker> {
	Ok(RollbackMarker {
		sid:                   row.try_get(0).map_err(database_error)?,
		incarnation_epoch:     row
			.try_get::<_, Option<i64>>(9)
			.map_err(database_error)?
			.unwrap_or(0),
		owner:                 row.try_get(1).map_err(database_error)?,
		epoch:                 row.try_get(2).map_err(database_error)?,
		target:                row
			.try_get::<_, Option<String>>(3)
			.map_err(database_error)?
			.ok_or_else(|| EngineError::engine("rollback marker has no target"))?,
		safety:                row
			.try_get::<_, Option<String>>(4)
			.map_err(database_error)?
			.ok_or_else(|| EngineError::engine("rollback marker has no safety point"))?,
		operation_generation:  u64::try_from(
			row.try_get::<_, Option<i64>>(5)
				.map_err(database_error)?
				.ok_or_else(|| EngineError::engine("rollback marker has no operation generation"))?,
		)
		.map_err(|_| EngineError::engine("negative rollback operation generation"))?,
		checkpoint_generation: u64::try_from(
			row.try_get::<_, Option<i64>>(6)
				.map_err(database_error)?
				.ok_or_else(|| EngineError::engine("negative rollback checkpoint generation"))?,
		)
		.map_err(|_| EngineError::engine("negative rollback checkpoint generation"))?,
		source_owner:          row
			.try_get::<_, Option<String>>(7)
			.map_err(database_error)?
			.ok_or_else(|| EngineError::engine("rollback marker has no source owner"))?,
		source_epoch:          row
			.try_get::<_, Option<i64>>(8)
			.map_err(database_error)?
			.ok_or_else(|| EngineError::engine("rollback marker has no source epoch"))?,
	})
}

fn database_error(error: ::postgres::Error) -> EngineError {
	EngineError::engine(format!("PostgreSQL metadata store: {error}"))
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn reserve_fixture_record(
		store: &ProductionStore,
		record: &CreateRecord,
	) -> Result<CreateRecord> {
		let mut reserved = record.clone();
		if reserved.idempotency_key.is_empty() {
			reserved.idempotency_key = format!("fixture-{}-{}", reserved.sid, reserved.epoch);
		}
		let epoch = store.reserve_fixture_epoch(
			&reserved.sid,
			&reserved.owner,
			reserved.epoch,
			&reserved.idempotency_key,
		)?;
		if epoch != reserved.epoch {
			return Err(EngineError::invalid(format!(
				"fixture requested epoch {} for {}, allocated {epoch}",
				reserved.epoch, reserved.sid
			)));
		}
		store.record(&reserved)
	}

	fn test_database_guard() -> tokio::sync::MutexGuard<'static, ()> {
		pg::TEST_DATABASE_LOCK.blocking_lock()
	}

	#[test]
	fn fresh_schema_bootstrap_supports_owner_epoch_queries() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP fresh schema bootstrap test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("bootstrap schema");
		store
			.with_client(|client| {
				client
					.query("SELECT owner, epoch FROM sandbox_ownership LIMIT 0", &[])
					.map_err(database_error)?;
				Ok(())
			})
			.expect("fresh schema ownership query");
	}

	#[test]
	fn durable_records_exclude_secrets() {
		let params = json!({
			"image": "alpine",
			"secrets": [{"name": "migration", "values": {"TOKEN": "sensitive"}}],
		})
		.as_object()
		.expect("record parameters must be an object")
		.clone();
		let record = CreateRecord::new("sandbox", params, "node-a", 1, "request", "off", "none", 1.0)
			.expect("record must be valid");

		let durable = record_without_secrets(&record);

		assert_eq!(durable.params.get("image"), Some(&json!("alpine")));
		assert!(!durable.params.contains_key("secrets"));
		assert!(record.params.contains_key("secrets"));
	}

	#[test]
	fn late_stale_replica_upsert_cannot_replace_newer_generation() {
		let newer_wins =
			replica_overwrite_required("sandbox", "new", "key-new", 1, 8, "old", "key-old", 1, 7)
				.expect("stale checkpoint is a no-op");
		assert!(!newer_wins);

		let advances =
			replica_overwrite_required("sandbox", "old", "key-old", 1, 7, "new", "key-new", 1, 8)
				.expect("newer checkpoint can replace old state");
		assert!(advances);
	}

	#[test]
	fn equal_generation_replica_digest_mismatch_is_rejected() {
		assert!(
			replica_overwrite_required(
				"sandbox", "digest-a", "key-a", 1, 8, "digest-b", "key-b", 1, 8,
			)
			.is_err(),
			"same lifecycle generation cannot name two different checkpoints"
		);
		assert!(
			!replica_overwrite_required(
				"sandbox", "digest-a", "key-a", 1, 8, "digest-a", "key-a", 1, 8,
			)
			.expect("same object identity is idempotent")
		);
		assert!(
			replica_overwrite_required(
				"sandbox", "digest-a", "key-a", 1, 8, "digest-a", "key-b", 1, 8,
			)
			.is_err(),
			"key rotation must advance checkpoint generation before publication"
		);
	}

	#[test]
	fn newer_owner_epoch_supersedes_predecessor_checkpoint_generation() {
		assert!(
			replica_overwrite_required(
				"sandbox",
				"owner-a-final",
				"key-a",
				1,
				10,
				"owner-b-first",
				"key-b",
				2,
				1,
			)
			.expect("the newer owner epoch wins")
		);
	}

	#[test]
	fn released_restore_claim_uses_fresh_epoch_and_keeps_delayed_work_fenced() {
		let restored = release_claim_epoch(41, 42).expect("exact restore claim");
		assert_eq!(restored, 43);
		assert!(
			release_claim_epoch(41, 41).is_err(),
			"an operation fenced at 41 cannot be released or made authoritative"
		);
		assert_ne!(restored, 41);
	}

	#[test]
	fn object_namespaces_isolate_independent_cluster_ids() {
		let first_id = "11111111-1111-4111-8111-111111111111";
		let second_id = "22222222-2222-4222-8222-222222222222";
		let first = cluster_object_namespace(first_id);
		let second = cluster_object_namespace(second_id);

		assert_eq!(first, format!("clusters/{first_id}"));
		assert_eq!(second, format!("clusters/{second_id}"));
		assert_ne!(first, second, "independent cluster identities must not share S3 paths");
	}

	#[test]
	fn concurrent_cluster_bootstrap_returns_one_stable_identity() {
		use std::{
			net::TcpStream,
			sync::{Arc, Barrier, mpsc},
			thread,
		};

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP cluster bootstrap test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		drop(ProductionStore::connect(URL).expect("prime fixture schema"));
		let barrier = Arc::new(Barrier::new(2));
		let (identity_tx, identity_rx) = mpsc::channel();
		for _ in 0..2 {
			let barrier = Arc::clone(&barrier);
			let identity_tx = identity_tx.clone();
			thread::spawn(move || {
				barrier.wait();
				let store = ProductionStore::connect(URL).expect("connect cluster store");
				identity_tx
					.send(store.object_namespace().to_owned())
					.expect("send object namespace");
			});
		}
		drop(identity_tx);
		let first = identity_rx.recv().expect("first identity");
		let second = identity_rx.recv().expect("second identity");
		assert_eq!(first, second, "singleton bootstrap must converge concurrent connections");
		assert!(first.starts_with("clusters/"));
	}

	#[test]
	fn verifier_registration_is_idempotent_but_rejects_mismatch() {
		use std::net::TcpStream;

		use uuid::Uuid;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP key verifier test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		let key_id = format!("test-key-{}", Uuid::new_v4());
		store
			.verify_or_register_key(&key_id, "verifier-a")
			.expect("first verifier registration");
		store
			.verify_or_register_key(&key_id, "verifier-a")
			.expect("same verifier is idempotent");
		assert!(
			store.verify_or_register_key(&key_id, "verifier-b").is_err(),
			"a second verifier for one key id is a split-brain configuration"
		);
	}

	#[test]
	fn publication_lock_orders_gc_and_same_session_commit() {
		use std::{
			net::TcpStream,
			sync::mpsc,
			thread,
			time::{Duration, Instant},
		};

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP publication lock test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let ownership = CreateRecord::new(
			"publication-lock",
			serde_json::Map::new(),
			"node-a",
			1,
			"publication-lock-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &ownership).expect("record ownership");
		let replica = ReplicaRecord::new_fenced(
			"publication-lock",
			"digest-publication",
			"node-a",
			1,
			1,
			"/checkpoint",
			serde_json::Map::new(),
			false,
		)
		.expect("replica")
		.with_object_key("digest-publication")
		.expect("replica object key");
		let key = replica_object_lock_key("digest-publication");

		// GC acquires first: publication must wait, then commit a row only
		// after the protected object key becomes available.
		let mut gc = pg::connect(URL, "gc fixture").expect("connect gc");
		let mut gc_tx = gc.transaction().expect("begin gc");
		gc_tx
			.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key])
			.expect("lock gc object");
		let (published_tx, published_rx) = mpsc::channel();
		thread::spawn(move || {
			let store = ProductionStore::connect(URL).expect("connect publisher");
			let mut lease = store
				.acquire_replica_publication("publication-lock", "node-a", 1, "digest-publication")
				.expect("publisher lock");
			lease.commit(&replica).expect("publisher commit");
			published_tx.send(()).expect("signal publication");
		});
		assert!(
			published_rx
				.recv_timeout(Duration::from_millis(100))
				.is_err(),
			"publisher must wait for a GC-held object lock"
		);
		gc_tx.commit().expect("release gc lock");
		published_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("publisher completes after GC");
		assert_eq!(
			store
				.replica("publication-lock")
				.expect("read replica")
				.expect("replica")
				.digest,
			"digest-publication"
		);

		// Publisher acquires first: same-session metadata commit must not
		// deadlock while GC waits on the exact same key.
		let mut lease = store
			.acquire_replica_publication("publication-lock", "node-a", 1, "digest-publication")
			.expect("publisher lock");
		let (gc_tx, gc_rx) = mpsc::channel();
		thread::spawn(move || {
			let mut gc = pg::connect(URL, "gc fixture").expect("connect gc");
			let mut transaction = gc.transaction().expect("begin gc");
			transaction
				.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key])
				.expect("lock gc object");
			gc_tx.send(()).expect("signal gc");
			transaction.commit().expect("finish gc");
		});
		assert!(
			gc_rx.recv_timeout(Duration::from_millis(100)).is_err(),
			"GC must wait for an in-progress publisher"
		);

		let started = Instant::now();
		lease
			.commit(
				&ReplicaRecord::new_fenced(
					"publication-lock",
					"digest-publication",
					"node-a",
					1,
					1,
					"/checkpoint",
					serde_json::Map::new(),
					false,
				)
				.expect("idempotent replica")
				.with_object_key("digest-publication")
				.expect("idempotent replica object key"),
			)
			.expect("same-session commit");
		assert!(
			started.elapsed() < Duration::from_secs(1),
			"metadata commit must not self-deadlock on the held object lock"
		);
		drop(lease);
		gc_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("GC releases after publisher");
	}

	#[test]
	fn concurrent_handoff_abort_and_claim_have_exactly_one_winner() {
		use std::{
			net::TcpStream,
			sync::{Arc, Barrier, mpsc},
			thread,
		};

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP handoff race test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let source = CreateRecord::new(
			"handoff-race",
			serde_json::Map::new(),
			"node-a",
			1,
			"handoff-source-key",
			"async",
			"none",
			0.0,
		)
		.expect("source record");
		reserve_fixture_record(&store, &source).expect("record source");
		store
			.begin_migration_handoff("handoff-race", "node-a", 1, "node-b", "handoff-token")
			.expect("mark handoff");

		let barrier = Arc::new(Barrier::new(2));
		let (result_tx, result_rx) = mpsc::channel();
		let claim_barrier = Arc::clone(&barrier);
		let claim_tx = result_tx.clone();
		thread::spawn(move || {
			claim_barrier.wait();
			let store = ProductionStore::connect(URL).expect("connect claimant");
			let intent = CreateRecord::new(
				"handoff-race",
				serde_json::Map::new(),
				"node-b",
				2,
				"handoff-target-key",
				"async",
				"none",
				0.0,
			)
			.expect("target intent");
			claim_tx
				.send(
					store
						.claim_migration_handoff(
							"handoff-race",
							"node-a",
							1,
							"node-b",
							"handoff-token",
							&intent,
						)
						.is_ok(),
				)
				.expect("send claim result");
		});
		thread::spawn(move || {
			barrier.wait();
			let store = ProductionStore::connect(URL).expect("connect aborter");
			result_tx
				.send(matches!(
					store.abort_migration_handoff(
						"handoff-race",
						"node-a",
						1,
						"node-b",
						"handoff-token",
					),
					Ok(Some(_))
				))
				.expect("send abort result");
		});
		assert_eq!(
			[result_rx.recv().expect("claim result"), result_rx.recv().expect("abort result")]
				.into_iter()
				.filter(|won| *won)
				.count(),
			1,
			"the marker may be consumed by either claimant or aborter, never both"
		);
		let resolved = store
			.resolve("handoff-race")
			.expect("resolve owner")
			.expect("owner row");
		assert_eq!(resolved.epoch, 2);
		let retry = store
			.abort_migration_handoff("handoff-race", "node-a", 1, "node-b", "handoff-token")
			.expect("retry abort");
		match resolved.owner.as_str() {
			"node-a" => {
				let replay = retry.expect("the winning abort remains idempotently replayable");
				assert_eq!((replay.owner.as_str(), replay.epoch), ("node-a", 2));
			},
			"node-b" => {
				assert!(retry.is_none(), "an abort retry cannot undo the claimed handoff");
				store
					.confirm_migration_handoff("handoff-race", "node-a", 1, "node-b", 2, "handoff-token")
					.expect("confirm claimed handoff");
				store
					.begin_migration_handoff("handoff-race", "node-a", 1, "node-b", "handoff-token")
					.expect("claimed handoff begin is idempotent");
			},
			owner => panic!("unexpected handoff owner {owner}"),
		}
	}

	#[test]
	fn complete_migration_abort_is_idempotent_only_for_its_exact_cleared_epoch() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP migration abort completion test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let source = CreateRecord::new(
			"abort-completion",
			serde_json::Map::new(),
			"node-a",
			1,
			"abort-completion-key",
			"async",
			"none",
			0.0,
		)
		.expect("source record");
		reserve_fixture_record(&store, &source).expect("record source");
		store
			.begin_migration_handoff("abort-completion", "node-a", 1, "node-b", "abort-token")
			.expect("mark handoff");
		let aborted = store
			.abort_migration_handoff("abort-completion", "node-a", 1, "node-b", "abort-token")
			.expect("abort handoff")
			.expect("fresh source record");
		assert_eq!(aborted.owner, "node-a");
		assert_eq!(aborted.epoch, 2);
		assert!(
			store
				.complete_migration_abort("abort-completion", "wrong-token", "node-a", 2)
				.is_err(),
			"an uncleared marker must reject a different token"
		);
		let first = store
			.complete_migration_abort("abort-completion", "abort-token", "node-a", 2)
			.expect("first completion");
		let retry = store
			.complete_migration_abort("abort-completion", "abort-token", "node-a", 2)
			.expect("idempotent completion retry");
		assert_eq!(first, retry);
		store
			.with_client(|client| {
				client
					.execute(
						"UPDATE sandbox_ownership SET owner_lease_expires_at = 0 WHERE sid = $1",
						&[&"abort-completion"],
					)
					.map_err(database_error)?;
				Ok(())
			})
			.expect("expire completed abort owner");
		store
			.claim_next("abort-completion", "node-a")
			.expect("advance ownership");
		assert!(
			store
				.complete_migration_abort("abort-completion", "abort-token", "node-a", 2)
				.is_err(),
			"a later owner epoch must fence an old completion retry"
		);
	}

	#[test]
	fn expired_owner_cannot_publish_or_allocate_checkpoint() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP expired owner test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let ownership = CreateRecord::new(
			"expired-owner",
			serde_json::Map::new(),
			"node-a",
			1,
			"expired-owner-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &ownership).expect("record ownership");
		let replica = ReplicaRecord::new_fenced(
			"expired-owner",
			"expired-digest",
			"node-a",
			1,
			1,
			"/checkpoint",
			serde_json::Map::new(),
			false,
		)
		.expect("replica")
		.with_object_key("expired-digest")
		.expect("replica object key");
		let mut publication = store
			.acquire_replica_publication("expired-owner", "node-a", 1, "expired-digest")
			.expect("fresh owner can acquire");
		let mut client = pg::connect(URL, "expire fixture").expect("connect fixture");
		client
			.execute("UPDATE sandbox_ownership SET owner_lease_expires_at = 0 WHERE sid = $1", &[
				&"expired-owner",
			])
			.expect("expire lease");
		assert!(
			!store
				.owns_epoch("expired-owner", "node-a", 1)
				.expect("verify expired lease"),
			"exact ownership checks must fence at the PostgreSQL expiry boundary"
		);
		assert!(
			store
				.renew_owner_lease("expired-owner", "node-a", 1)
				.expect("renew expired lease")
				.is_none(),
			"renewal must not revive an expired ownership epoch"
		);
		assert!(
			publication.commit(&replica).is_err(),
			"expiry between object work and metadata commit fences publication"
		);
		drop(publication);
		assert!(
			store
				.replica("expired-owner")
				.expect("read replica")
				.is_none(),
			"failed late commit must leave no metadata row"
		);
		assert!(
			store
				.acquire_replica_publication("expired-owner", "node-a", 1, "expired-digest")
				.is_err(),
			"expired owner cannot begin a new publication"
		);
		assert!(
			store
				.allocate_checkpoint_generation("expired-owner", "node-a", 1)
				.is_err(),
			"expired owner cannot allocate a publishable generation"
		);
	}

	#[test]
	fn committed_suspend_is_immediately_claimable_at_a_fresh_epoch() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP suspend claim test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let record = CreateRecord::new(
			"suspend-claim",
			serde_json::Map::new(),
			"node-a",
			1,
			"suspend-claim-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &record).expect("record ownership");
		store
			.with_client(|client| {
				client
					.execute(
						"INSERT INTO portable_recovery_points (sid, name, kind, created_at_unix_millis, \
						 archive_size_bytes, archive_key, archive_sha256, manifest_key, \
						 manifest_sha256, owner_node, owner_epoch, incarnation_epoch, \
						 lifecycle_generation, committed_at_unix_millis) VALUES ($1, $2, 'checkpoint', \
						 0, 1, 'archive', 'digest', 'manifest', 'digest', 'node-a', 1, 1, 7, 0)",
						&[&"suspend-claim", &"exact-checkpoint"],
					)
					.map_err(database_error)?;
				Ok(())
			})
			.expect("commit portable suspension point");
		store
			.prepare_suspend("suspend-claim", "node-a", 1, "exact-checkpoint", 7)
			.expect("prepare suspension");
		store
			.commit_suspend("suspend-claim", "node-a", 1)
			.expect("commit suspension");

		let claimed = store
			.claim_expected("suspend-claim", "node-a", 1, "node-b")
			.expect("immediate suspended claim");
		assert_eq!((claimed.owner.as_str(), claimed.epoch), ("node-b", 2));
		assert!(
			store
				.begin_resume("suspend-claim", "node-b", 2)
				.expect("begin exact resume"),
			"claimed suspended marker enters resuming"
		);
		assert!(
			store
				.release_claim("suspend-claim", "node-b", 2, "node-a", 1)
				.expect("release after resume is observed")
				.is_none(),
			"release must not undo a possibly materialized resuming claim"
		);
		let current = store
			.resolve("suspend-claim")
			.expect("read resuming owner")
			.expect("ownership remains");
		assert_eq!((current.owner.as_str(), current.epoch), ("node-b", 2));
		assert_eq!(
			store
				.suspend_marker("suspend-claim")
				.expect("read exact lifecycle marker")
				.expect("resuming marker")
				.state,
			"resuming"
		);
	}
	#[test]
	fn suspend_abort_requires_exact_live_owner_lease() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP suspend abort lease test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let record = CreateRecord::new(
			"suspend-abort",
			serde_json::Map::new(),
			"node-a",
			1,
			"suspend-abort-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &record).expect("record ownership");
		let mut client = pg::connect(URL, "suspend abort fixture").expect("connect fixture");
		client
			.execute(
				"UPDATE sandbox_ownership SET lifecycle_state = 'suspending', owner_lease_expires_at \
				 = 0 WHERE sid = $1",
				&[&"suspend-abort"],
			)
			.expect("expire owner lease");
		assert!(
			store.abort_suspend("suspend-abort", "node-a", 1).is_err(),
			"expired owner must not regain serving authority"
		);
		client
			.execute(
				"UPDATE sandbox_ownership SET owner = 'node-b', epoch = 2, owner_lease_owner = \
				 'node-b', owner_lease_epoch = 2, owner_lease_expires_at = EXTRACT(EPOCH FROM \
				 clock_timestamp()) + 15 WHERE sid = $1",
				&[&"suspend-abort"],
			)
			.expect("supersede owner");
		assert!(
			store.abort_suspend("suspend-abort", "node-a", 1).is_err(),
			"superseded owner must not regain serving authority"
		);
	}

	#[test]
	fn live_lifecycle_adoption_reuses_exact_epoch_for_all_suspend_states() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP live lifecycle adoption test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let record = CreateRecord::new(
			"live-lifecycle-adoption",
			serde_json::Map::new(),
			"node-a",
			1,
			"live-lifecycle-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &record).expect("record ownership");
		let mut client = pg::connect(URL, "live lifecycle fixture").expect("connect fixture");
		for state in ["suspending", "suspended", "resuming"] {
			client
				.execute(
					"UPDATE sandbox_ownership SET lifecycle_state = $2, suspend_point = 'exact-point', \
					 suspend_generation = 9, owner_lease_owner = 'node-a', owner_lease_epoch = 1, \
					 owner_lease_expires_at = EXTRACT(EPOCH FROM clock_timestamp()) + 15 WHERE sid = $1",
					&[&"live-lifecycle-adoption", &state],
				)
				.expect("set live marker");
			let adopted = store
				.adopt_live_lifecycle("live-lifecycle-adoption", "node-a", 1, state, "exact-point", 9)
				.expect("re-adopt exact live marker");
			assert_eq!((adopted.owner.as_str(), adopted.epoch), ("node-a", 1));
		}
	}

	#[test]
	fn expected_rollback_claim_persists_exact_marker_before_restore() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP rollback claim test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let record = CreateRecord::new(
			"rollback-claim",
			serde_json::Map::new(),
			"node-a",
			1,
			"rollback-claim-key",
			"async",
			"none",
			0.0,
		)
		.expect("ownership");
		reserve_fixture_record(&store, &record).expect("record ownership");
		let mut client = pg::connect(URL, "expire rollback fixture").expect("connect fixture");
		client
			.execute(
				"INSERT INTO portable_recovery_points (sid, name, kind, created_at_unix_millis, \
				 archive_size_bytes, archive_key, archive_sha256, manifest_key, manifest_sha256, \
				 owner_node, owner_epoch, incarnation_epoch, lifecycle_generation, \
				 committed_at_unix_millis) VALUES ($1, $2, 'checkpoint', 0, 1, 'archive', 'digest', \
				 'manifest', 'digest', 'node-a', 1, 1, 7, 0)",
				&[&"rollback-claim", &"exact-rollback-target"],
			)
			.expect("insert exact rollback target");
		client
			.execute("UPDATE sandbox_ownership SET owner_lease_expires_at = 0 WHERE sid = $1", &[
				&"rollback-claim",
			])
			.expect("expire source lease");

		let claimed = store
			.claim_expected_rollback(
				"rollback-claim",
				"node-a",
				1,
				"node-b",
				"exact-rollback-target",
				12,
				7,
			)
			.expect("atomic rollback claim");
		assert_eq!((claimed.owner.as_str(), claimed.epoch), ("node-b", 2));
		let marker = store
			.list_rollback_markers()
			.expect("durable rollback marker")
			.into_iter()
			.find(|marker| marker.sid == "rollback-claim")
			.expect("marker survives transaction boundary");
		// The target belongs to the fenced source, not post-claim node-b.
		// Persisting this provenance distinguishes a valid rollback source
		// point from an unprovenanced stale point after same-SID recreation.
		assert_eq!(marker.target, "exact-rollback-target");
		assert_eq!(marker.safety, "exact-rollback-target");
		assert_eq!((marker.source_owner.as_str(), marker.source_epoch), ("node-a", 1));
		assert_eq!((marker.operation_generation, marker.checkpoint_generation), (12, 7));
		assert!(matches!(
			store
				.rollback_disposition("rollback-claim", "node-b", 2)
				.expect("exact claimed marker"),
			RollbackDisposition::RollingBack(_)
		));
		let adopted = store
			.adopt_live_rollback("rollback-claim", "node-b", 2)
			.expect("restart adopts exact live rollback marker");
		assert_eq!((adopted.owner.as_str(), adopted.epoch), ("node-b", 2));
		assert!(
			store
				.claim_expected_rollback("rollback-claim", "node-a", 1, "node-c", "other-target", 13, 8,)
				.is_err(),
			"stale or conflicting claim cannot overwrite the exact marker"
		);
		store
			.finish_rollback(&claimed)
			.expect("first committed finish");
		store
			.finish_rollback(&claimed)
			.expect("applied commit retry is idempotent");
		assert!(
			store
				.release_claim("rollback-claim", "node-b", 2, "node-a", 1)
				.expect("delayed abort observes committed owner")
				.is_none(),
			"post-commit abort must not revert the clean running owner"
		);
		let current = store
			.resolve("rollback-claim")
			.expect("read committed owner")
			.expect("ownership remains");
		assert_eq!((current.owner.as_str(), current.epoch), ("node-b", 2));
	}
	#[test]
	fn exact_record_update_persists_marker_and_rejects_stale_epoch() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP exact record update test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let mut record = CreateRecord::new(
			"exact-update",
			serde_json::Map::new(),
			"node-a",
			1,
			"exact-update-key",
			"async",
			"none",
			1.0,
		)
		.expect("record");
		reserve_fixture_record(&store, &record).expect("persist record");
		record
			.params
			.insert("_mesh_migration_committed".to_owned(), serde_json::Value::Bool(true));
		store
			.update_exact_record(&record)
			.expect("same epoch marker update");
		assert_eq!(
			store
				.resolve("exact-update")
				.expect("resolve")
				.expect("record")
				.params
				.get("_mesh_migration_committed"),
			Some(&serde_json::Value::Bool(true))
		);
		store
			.with_client(|client| {
				client
					.execute(
						"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
						 clock_timestamp()) - 1 WHERE sid = $1",
						&[&"exact-update"],
					)
					.map_err(database_error)?;
				Ok(())
			})
			.expect("expire owner lease");
		assert!(
			store.update_exact_record(&record).is_err(),
			"expired lease cannot acknowledge an exact record update"
		);
		store
			.with_client(|client| {
				client
					.execute(
						"UPDATE sandbox_ownership SET owner_lease_expires_at = EXTRACT(EPOCH FROM \
						 clock_timestamp()) + 60 WHERE sid = $1",
						&[&"exact-update"],
					)
					.map_err(database_error)?;
				Ok(())
			})
			.expect("restore owner lease");
		record.epoch = 0;
		assert!(
			store.update_exact_record(&record).is_err(),
			"stale ownership generation cannot update migration marker"
		);
	}

	#[test]
	fn migration_backfills_only_legacy_owner_lease_sentinel() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP legacy lease migration test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let record = CreateRecord::new(
			"legacy-lease",
			serde_json::Map::new(),
			"node-a",
			7,
			"legacy-lease-key",
			"async",
			"none",
			1.0,
		)
		.expect("record");
		reserve_fixture_record(&store, &record).expect("persist record");
		let mut client = pg::connect(URL, "legacy lease migration fixture").expect("connect fixture");
		client
			.execute(
				"UPDATE sandbox_ownership SET owner_lease_owner = '', owner_lease_epoch = 0, \
				 owner_lease_expires_at = 0 WHERE sid = $1",
				&[&"legacy-lease"],
			)
			.expect("install legacy sentinel");
		migrate(&mut client).expect("backfill migration");
		let row = client
			.query_one(
				"SELECT owner_lease_owner, owner_lease_epoch, owner_lease_expires_at > EXTRACT(EPOCH \
				 FROM clock_timestamp()) FROM sandbox_ownership WHERE sid = $1",
				&[&"legacy-lease"],
			)
			.expect("read migrated lease");
		assert_eq!(row.try_get::<_, String>(0).expect("lease owner"), "node-a");
		assert_eq!(row.try_get::<_, i64>(1).expect("lease epoch"), 7);
		assert!(row.try_get::<_, bool>(2).expect("lease expiry"));
	}

	#[test]
	fn tombstone_blocks_serving_and_commit_releases_replica_and_idempotency() {
		use std::net::TcpStream;

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP tombstone store test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		store.clear_for_test().expect("clear fixture");
		let initial_epoch = store
			.reserve_create_epoch("delete-me", "node-a", "reusable-key")
			.expect("reserve initial");
		assert_eq!(initial_epoch, 0);
		let record = CreateRecord::new(
			"delete-me",
			serde_json::Map::new(),
			"node-a",
			initial_epoch,
			"reusable-key",
			"async",
			"none",
			1.0,
		)
		.expect("record");
		store.record(&record).expect("persist record");
		let replica = ReplicaRecord::new_fenced(
			"delete-me",
			"delete-digest",
			"node-a",
			u64::try_from(record.epoch).expect("non-negative ownership epoch"),
			1,
			"/checkpoint",
			serde_json::Map::new(),
			false,
		)
		.expect("replica")
		.with_object_key("delete-digest")
		.expect("replica object key");
		store
			.acquire_replica_publication("delete-me", "node-a", record.epoch, "delete-digest")
			.expect("acquire publication")
			.commit(&replica)
			.expect("persist replica");
		store
			.begin_delete("delete-me", "node-a", record.epoch)
			.expect("tombstone");
		assert!(store.resolve("delete-me").expect("resolve").is_none());
		assert_eq!(store.list_deleting().expect("tombstones").len(), 1);
		assert!(
			store
				.renew_owner_lease("delete-me", "node-a", record.epoch)
				.expect("renew")
				.is_none()
		);
		store
			.commit_delete("delete-me", "node-a", record.epoch)
			.expect("commit deletion");
		assert!(store.replica("delete-me").expect("replica").is_none());
		let stale = CreateRecord::new(
			"delete-me",
			serde_json::Map::new(),
			"node-a",
			record.epoch,
			"stale-key",
			"off",
			"none",
			2.0,
		)
		.expect("stale record");
		assert!(store.record(&stale).is_err(), "deleted epoch needs an explicit current reservation");
		let replacement_epoch = store
			.reserve_create_epoch("delete-me", "node-b", "reusable-key")
			.expect("reserve replacement");
		let recreated = CreateRecord::new(
			"delete-me",
			serde_json::Map::new(),
			"node-b",
			replacement_epoch,
			"reusable-key",
			"async",
			"none",
			2.0,
		)
		.expect("recreated record");
		let recreated = store
			.record(&recreated)
			.expect("idempotency key released with tombstone commit");
		assert!(
			recreated.epoch > record.epoch,
			"recreating a deleted sandbox must allocate a new ownership incarnation"
		);
		assert!(
			store.update_exact_record(&record).is_err(),
			"delayed writes from the deleted incarnation must not mutate the replacement"
		);
	}

	#[test]
	fn replica_read_guard_blocks_object_gc_and_publication_sessions() {
		use std::{net::TcpStream, sync::mpsc, thread, time::Duration};

		const URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";
		if TcpStream::connect(("127.0.0.1", 15433)).is_err() {
			eprintln!("SKIP replica read guard test: PostgreSQL fixture unavailable");
			return;
		}
		let _database_guard = test_database_guard();
		let store = ProductionStore::connect(URL).expect("connect fixture");
		let guard = store
			.acquire_replica_read("read-digest")
			.expect("reader lock");
		let key = replica_object_lock_key("read-digest");
		let (acquired_tx, acquired_rx) = mpsc::channel();
		thread::spawn(move || {
			let mut client = pg::connect(URL, "object writer fixture").expect("connect writer");
			client
				.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key])
				.expect("writer object lock");
			acquired_tx.send(()).expect("signal writer");
		});
		assert!(
			acquired_rx
				.recv_timeout(Duration::from_millis(100))
				.is_err(),
			"publisher/GC session must wait while a replica read is verifying bytes"
		);
		drop(guard);
		acquired_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("writer unblocked after reader");
	}
}
