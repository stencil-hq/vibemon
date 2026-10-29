//! The cache index in the raft KV ([`super::records`]).
//!
//! Reservations, finalization, scoped lookup, blob holders, deletion, and
//! the driver's sweep. Writes are optimistic transactions guarded by the
//! `mod_rev` of every record they read, rerun when a guard fails (the store
//! rules).

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use vmon_cluster::{
	NodeId,
	kv::{Compare, Kv, Op, Txn},
};

use super::records::{
	CacheBlobRow, CacheEntryRow, CacheGrant, CacheScope, ENTRIES, EntryState, GRANTS, blob_key,
	entry_key, grant_key, key_prefix, repo_prefix,
};

/// An upload not finalized within this long expires (its reservation is
/// released).
pub const UPLOAD_TTL: Duration = Duration::from_hours(1);
/// Entries unused this long are removed (GitHub's 7 days).
pub const UNUSED_EXPIRY: Duration = Duration::from_hours(7 * 24);
/// Default per-repository budget (GitHub's 10 GiB is too small for Rust
/// targets; the bytes are the operator's own disks).
pub const REPO_BUDGET: u64 = 50 << 30;
/// `last_used_at` is rewritten at most this often per entry (ms).
const TOUCH_EVERY_MS: i64 = 60_000;
/// Optimistic transaction attempts before giving up.
const ATTEMPTS: usize = 32;

fn ttl_ms(ttl: Duration) -> u64 {
	u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}

fn put<T: Serialize>(key: &str, value: &T, ttl: Option<Duration>) -> Op {
	Op::Put {
		key:    key.to_owned(),
		value:  serde_json::to_vec(value).expect("records serialize"),
		ttl_ms: ttl.map(ttl_ms),
	}
}

fn delete(key: &str) -> Op {
	Op::Delete { key: key.to_owned() }
}

/// A record and its `mod_rev` (0 = absent, the guard for "still absent").
async fn read<T: DeserializeOwned>(kv: &Kv, key: &str) -> Result<(Option<T>, u64)> {
	match kv.get_consistent(key).await? {
		Some(entry) => {
			let value =
				serde_json::from_slice(&entry.value).with_context(|| format!("decoding {key}"))?;
			Ok((Some(value), entry.mod_rev))
		},
		None => Ok((None, 0)),
	}
}

fn guard(key: &str, rev: u64) -> Compare {
	Compare::ModRev { key: key.to_owned(), rev }
}

async fn commit(kv: &Kv, compare: Vec<Compare>, success: Vec<Op>) -> Result<bool> {
	Ok(kv
		.txn(Txn { compare, success, failure: Vec::new() })
		.await?
		.succeeded)
}

async fn list<T: DeserializeOwned>(kv: &Kv, prefix: &str) -> Result<Vec<(String, T)>> {
	kv.list(prefix)
		.await?
		.into_iter()
		.map(|entry| {
			let value = serde_json::from_slice(&entry.value)
				.with_context(|| format!("decoding {}", entry.key))?;
			Ok((entry.key, value))
		})
		.collect()
}

// ------------------------------------------------------------------ grants

/// The grant of `sandbox_id`, if it is a CI runner sandbox.
pub async fn grant(kv: &Kv, sandbox_id: &str) -> Result<Option<CacheGrant>> {
	Ok(read(kv, &grant_key(sandbox_id)).await?.0)
}

/// Every grant, by sandbox id.
pub async fn grants(kv: &Kv) -> Result<BTreeMap<String, CacheGrant>> {
	let prefix = format!("{GRANTS}/");
	Ok(list::<CacheGrant>(kv, &prefix)
		.await?
		.into_iter()
		.map(|(key, grant)| {
			let id = key[prefix.len()..].replace("%2F", "/").replace("%25", "%");
			(id, grant)
		})
		.collect())
}

/// Writes a sandbox's grant (expires with the sandbox, `ttl`).
pub async fn put_grant(kv: &Kv, sandbox_id: &str, grant: &CacheGrant, ttl: Duration) -> Result<()> {
	kv.put(&grant_key(sandbox_id), serde_json::to_vec(grant)?, Some(ttl))
		.await?;
	Ok(())
}

/// Removes a sandbox's grant.
pub async fn delete_grant(kv: &Kv, sandbox_id: &str) -> Result<()> {
	kv.delete(&grant_key(sandbox_id)).await?;
	Ok(())
}

// ----------------------------------------------------------------- entries

/// What [`reserve`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Reserved {
	/// The entry is reserved for this upload.
	New(CacheEntryRow),
	/// `(scope, key, version)` exists (entries are immutable) or another job
	/// is uploading it.
	Exists,
}

/// Reserves `(scope.write, key, version)` for an upload by `job_id` on
/// `node` (`CreateCacheEntry`).
pub async fn reserve(
	kv: &Kv,
	scope: &CacheScope,
	key: &str,
	version: &str,
	job_id: u64,
	node: NodeId,
	now: DateTime<Utc>,
) -> Result<Reserved> {
	let entry_key = entry_key(scope.repo_id, &scope.write, key, version);
	let row = CacheEntryRow {
		// Fits a JavaScript number (the toolkit parses `entry_id`).
		id: (rand::random::<u64>() >> 11).max(1),
		repo_id: scope.repo_id,
		repo: scope.repo.clone(),
		scope: scope.write.clone(),
		key: key.to_owned(),
		version: version.to_owned(),
		state: EntryState::Uploading,
		digest: None,
		size: 0,
		node,
		job_id,
		created_at: now,
		last_used_at: now,
	};
	let created = commit(kv, vec![Compare::Missing(entry_key.clone())], vec![put(
		&entry_key,
		&row,
		Some(UPLOAD_TTL),
	)])
	.await?;
	Ok(if created {
		Reserved::New(row)
	} else {
		Reserved::Exists
	})
}

/// Releases an upload reservation that failed (no-op once finalized).
pub async fn release(kv: &Kv, entry_key: &str) -> Result<()> {
	for _ in 0..ATTEMPTS {
		let (row, rev) = read::<CacheEntryRow>(kv, entry_key).await?;
		if row.is_none_or(|row| row.state != EntryState::Uploading) {
			return Ok(());
		}
		if commit(kv, vec![guard(entry_key, rev)], vec![delete(entry_key)]).await? {
			return Ok(());
		}
	}
	anyhow::bail!("releasing {entry_key}: too much contention")
}

/// Finalizes the upload of `entry_key` as the archive `digest` of `size`
/// bytes held by `node` (`FinalizeCacheEntryUpload`). `None`: no upload of
/// that entry is pending (expired, or already finalized).
pub async fn finalize(
	kv: &Kv,
	entry_key: &str,
	digest: &str,
	size: u64,
	node: NodeId,
	now: DateTime<Utc>,
) -> Result<Option<CacheEntryRow>> {
	let blob_key = blob_key(digest);
	for _ in 0..ATTEMPTS {
		let (row, entry_rev) = read::<CacheEntryRow>(kv, entry_key).await?;
		let Some(mut row) = row.filter(|row| row.state == EntryState::Uploading) else {
			return Ok(None);
		};
		let (blob, blob_rev) = read::<CacheBlobRow>(kv, &blob_key).await?;
		let mut blob = blob.unwrap_or_else(|| CacheBlobRow {
			size,
			holders: Vec::new(),
			entries: Vec::new(),
			created_at: now,
		});
		if !blob.holders.contains(&node) {
			blob.holders.push(node);
		}
		if !blob.entries.iter().any(|key| key == entry_key) {
			blob.entries.push(entry_key.to_owned());
		}
		row.state = EntryState::Ready;
		row.digest = Some(digest.to_owned());
		row.size = size;
		row.last_used_at = now;
		if commit(kv, vec![guard(entry_key, entry_rev), guard(&blob_key, blob_rev)], vec![
			put(entry_key, &row, None),
			put(&blob_key, &blob, None),
		])
		.await?
		{
			return Ok(Some(row));
		}
	}
	anyhow::bail!("finalizing {entry_key}: too much contention")
}

/// The restorable entry for `key`/`restore_keys` at `version`: per scope in
/// `scope.read` order, the exact `key`, then each restore key as a prefix
/// (the newest matching entry wins) — GitHub's lookup order.
pub async fn lookup(
	kv: &Kv,
	scope: &CacheScope,
	key: &str,
	restore_keys: &[String],
	version: &str,
) -> Result<Option<CacheEntryRow>> {
	kv.sync().await?;
	for scope_ref in &scope.read {
		let exact = entry_key(scope.repo_id, scope_ref, key, version);
		if let (Some(row), _) = read::<CacheEntryRow>(kv, &exact).await?
			&& row.state == EntryState::Ready
		{
			return Ok(Some(row));
		}
		for prefix in restore_keys {
			let rows =
				list::<CacheEntryRow>(kv, &key_prefix(scope.repo_id, scope_ref, prefix)).await?;
			let newest = rows
				.into_iter()
				.map(|(_, row)| row)
				.filter(|row| {
					row.state == EntryState::Ready
						&& row.version == version
						&& row.key.starts_with(prefix.as_str())
				})
				.max_by_key(|row| (row.created_at, row.id));
			if newest.is_some() {
				return Ok(newest);
			}
		}
	}
	Ok(None)
}

/// Records a restore of `row` (LRU and the 7-day expiry); best effort.
pub async fn touch(kv: &Kv, row: &CacheEntryRow, now: DateTime<Utc>) -> Result<()> {
	if (now - row.last_used_at).num_milliseconds() < TOUCH_EVERY_MS {
		return Ok(());
	}
	let key = entry_key(row.repo_id, &row.scope, &row.key, &row.version);
	let (current, rev) = read::<CacheEntryRow>(kv, &key).await?;
	let Some(mut current) = current.filter(|current| current.id == row.id) else {
		return Ok(());
	};
	current.last_used_at = now;
	commit(kv, vec![guard(&key, rev)], vec![put(&key, &current, None)]).await?;
	Ok(())
}

/// Ready and uploading entries of one repository (`None`: all), in key
/// order.
pub async fn entries(kv: &Kv, repo_id: Option<u64>) -> Result<Vec<CacheEntryRow>> {
	let prefix = repo_id.map_or_else(|| format!("{ENTRIES}/"), repo_prefix);
	Ok(list::<CacheEntryRow>(kv, &prefix)
		.await?
		.into_iter()
		.map(|(_, row)| row)
		.collect())
}

/// Deletes an entry (with `id`: only that entry, not one created under the
/// same key since); returns the deleted row.
///
/// The archive's record goes with its last entry; holders drop their copies
/// on their next sweep.
pub async fn delete_entry(
	kv: &Kv,
	entry_key: &str,
	id: Option<u64>,
) -> Result<Option<CacheEntryRow>> {
	for _ in 0..ATTEMPTS {
		let (row, entry_rev) = read::<CacheEntryRow>(kv, entry_key).await?;
		let Some(row) = row.filter(|row| id.is_none_or(|id| row.id == id)) else {
			return Ok(None);
		};
		let mut compare = vec![guard(entry_key, entry_rev)];
		let mut ops = vec![delete(entry_key)];
		if let Some(digest) = &row.digest {
			let blob_key = blob_key(digest);
			let (blob, blob_rev) = read::<CacheBlobRow>(kv, &blob_key).await?;
			compare.push(guard(&blob_key, blob_rev));
			if let Some(mut blob) = blob {
				blob.entries.retain(|key| key != entry_key);
				ops.push(if blob.entries.is_empty() {
					delete(&blob_key)
				} else {
					put(&blob_key, &blob, None)
				});
			}
		}
		if commit(kv, compare, ops).await? {
			return Ok(Some(row));
		}
	}
	anyhow::bail!("deleting {entry_key}: too much contention")
}

// ------------------------------------------------------------------- blobs

/// The record of one archive.
pub async fn blob(kv: &Kv, digest: &str) -> Result<Option<CacheBlobRow>> {
	Ok(read(kv, &blob_key(digest)).await?.0)
}

/// Every archive record, by digest.
pub async fn blobs(kv: &Kv) -> Result<BTreeMap<String, CacheBlobRow>> {
	let prefix = format!("{}/", super::records::BLOBS);
	Ok(list::<CacheBlobRow>(kv, &prefix)
		.await?
		.into_iter()
		.map(|(key, row)| (key[prefix.len()..].to_owned(), row))
		.collect())
}

/// Adds `node` as a holder of `digest` (it fetched a copy). `false`: the
/// archive is no longer referenced (the copy should go).
pub async fn add_holder(kv: &Kv, digest: &str, node: NodeId) -> Result<bool> {
	let key = blob_key(digest);
	for _ in 0..ATTEMPTS {
		let (blob, rev) = read::<CacheBlobRow>(kv, &key).await?;
		let Some(mut blob) = blob else {
			return Ok(false);
		};
		if blob.holders.contains(&node) {
			return Ok(true);
		}
		blob.holders.push(node);
		if commit(kv, vec![guard(&key, rev)], vec![put(&key, &blob, None)]).await? {
			return Ok(true);
		}
	}
	anyhow::bail!("adding a holder of {digest}: too much contention")
}

/// Removes `node` as a holder of `digest` (it evicted its copy). The last
/// holder takes the archive's record and entries with it (nothing could
/// restore them any more). Returns the entries deleted.
pub async fn drop_holder(kv: &Kv, digest: &str, node: NodeId) -> Result<Vec<String>> {
	let key = blob_key(digest);
	for _ in 0..ATTEMPTS {
		let (blob, rev) = read::<CacheBlobRow>(kv, &key).await?;
		let Some(mut blob) = blob else {
			return Ok(Vec::new());
		};
		if !blob.holders.contains(&node) {
			return Ok(Vec::new());
		}
		blob.holders.retain(|holder| *holder != node);
		let (ops, gone) = if blob.holders.is_empty() {
			let mut ops = vec![delete(&key)];
			ops.extend(blob.entries.iter().map(|entry| delete(entry)));
			(ops, blob.entries.clone())
		} else {
			(vec![put(&key, &blob, None)], Vec::new())
		};
		if commit(kv, vec![guard(&key, rev)], ops).await? {
			return Ok(gone);
		}
	}
	anyhow::bail!("dropping a holder of {digest}: too much contention")
}

// ------------------------------------------------------------------- sweep

/// What one [`sweep`] removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swept {
	/// Entries unused for [`UNUSED_EXPIRY`].
	pub expired: usize,
	/// Entries evicted for a repository's budget.
	pub evicted: usize,
	/// Bytes of the removed entries.
	pub bytes:   u64,
}

/// Which ready entries go: unused for [`UNUSED_EXPIRY`], then, per
/// repository over `budget` bytes, the least recently used until it fits.
pub fn plan_sweep(
	entries: &[CacheEntryRow],
	now: DateTime<Utc>,
	budget: u64,
) -> (Vec<&CacheEntryRow>, Vec<&CacheEntryRow>) {
	let cutoff = now - chrono::Duration::from_std(UNUSED_EXPIRY).unwrap_or(chrono::Duration::MAX);
	let mut expired = Vec::new();
	let mut repos: BTreeMap<u64, Vec<&CacheEntryRow>> = BTreeMap::new();
	for row in entries.iter().filter(|row| row.state == EntryState::Ready) {
		if row.last_used_at < cutoff {
			expired.push(row);
		} else {
			repos.entry(row.repo_id).or_default().push(row);
		}
	}
	let mut evicted = Vec::new();
	for rows in repos.values_mut() {
		let mut total: u64 = rows.iter().map(|row| row.size).sum();
		rows.sort_by_key(|row| (row.last_used_at, row.created_at, row.id));
		for row in rows.iter() {
			if total <= budget {
				break;
			}
			total -= row.size;
			evicted.push(*row);
		}
	}
	(expired, evicted)
}

/// Removes expired entries and enforces the per-repository `budget` (run by
/// the driver leader).
pub async fn sweep(kv: &Kv, now: DateTime<Utc>, budget: u64) -> Result<Swept> {
	let all = entries(kv, None).await?;
	let (expired, evicted) = plan_sweep(&all, now, budget);
	let mut swept = Swept::default();
	for (rows, count) in [(expired, &mut swept.expired), (evicted, &mut swept.evicted)] {
		for row in rows {
			let key = entry_key(row.repo_id, &row.scope, &row.key, &row.version);
			if let Some(deleted) = delete_entry(kv, &key, Some(row.id)).await? {
				*count += 1;
				swept.bytes += deleted.size;
			}
		}
	}
	Ok(swept)
}
