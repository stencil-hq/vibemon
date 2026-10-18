//! Build file-context archives (`COPY` sources): signed upload URLs on this
//! driver, archive sniffing (gzip or plain tar), streaming to a storage node
//! (`PUT /node/v1/files/{templateId}/{hash}`), and the `file/<tid>/<hash>`
//! KV record (ARCHITECTURE.md §6, §8.6).

use std::{
	io,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use vmon_cluster::NodeId;

use super::file_key;
use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
};

/// Validity of a signed upload URL (upstream: 30 minutes).
const UPLOAD_URL_TTL: Duration = Duration::from_mins(30);
/// Bytes needed to recognise a tar header (`ustar` magic at offset 257).
const SNIFF_LEN: usize = 263;

/// KV value of `file/<templateID>/<hash>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
	/// Node storing the archive.
	pub node: NodeId,
	pub size: u64,
}

/// Recognised archive formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveKind {
	Gzip,
	Tar,
}

/// Path of the upload endpoint (also the signed message).
pub fn upload_path(template_id: &str, hash: &str) -> String {
	format!("/templates/{template_id}/files/{hash}/upload")
}

/// Signed absolute `PUT` URL on this driver's public base.
pub fn upload_url(state: &AppState, template_id: &str, hash: &str) -> String {
	let path = upload_path(template_id, hash);
	let expires = chrono::Utc::now().timestamp() + UPLOAD_URL_TTL.as_secs() as i64;
	let query = state.keys.sign_upload_url(&path, expires);
	format!("{}{path}?{query}", state.config.public_url.trim_end_matches('/'))
}

/// Whether the archive is stored on an alive node.
pub async fn file_present(state: &AppState, template_id: &str, hash: &str) -> ApiResult<bool> {
	Ok(file_location(state, template_id, hash).await?.is_some())
}

/// The alive node storing an archive.
pub(super) async fn file_location(
	state: &AppState,
	template_id: &str,
	hash: &str,
) -> ApiResult<Option<NodeId>> {
	let Some(entry) = state.cluster.kv.get(&file_key(template_id, hash)).await? else {
		return Ok(None);
	};
	let Ok(record) = serde_json::from_slice::<FileRecord>(&entry.value) else {
		return Ok(None);
	};
	Ok(state
		.node_members()
		.iter()
		.any(|(id, _)| *id == record.node)
		.then_some(record.node))
}

/// Classifies an archive from its first bytes; `complete` says the whole
/// body is in `prefix`. An all-zero body (empty tar) counts as tar.
pub fn sniff_archive(prefix: &[u8], complete: bool) -> Option<ArchiveKind> {
	if prefix.starts_with(&[0x1f, 0x8b]) {
		return Some(ArchiveKind::Gzip);
	}
	if prefix.len() >= SNIFF_LEN && &prefix[257..262] == b"ustar" {
		return Some(ArchiveKind::Tar);
	}
	if complete
		&& !prefix.is_empty()
		&& prefix.len().is_multiple_of(512)
		&& prefix.iter().all(|b| *b == 0)
	{
		return Some(ArchiveKind::Tar);
	}
	None
}

/// Streams an upload to the storage node with the most free disk and
/// records its location. 400 when the body is not a tar/tar.gz archive.
pub async fn store_upload(
	state: &AppState,
	template_id: &str,
	hash: &str,
	body: axum::body::Body,
) -> ApiResult<()> {
	let mut stream = body.into_data_stream();
	let mut prefix = BytesMut::new();
	let mut complete = false;
	while prefix.len() < SNIFF_LEN {
		match stream.next().await {
			Some(Ok(chunk)) => prefix.extend_from_slice(&chunk),
			Some(Err(error)) => return Err(ApiError::bad_request(format!("reading upload: {error}"))),
			None => {
				complete = true;
				break;
			},
		}
	}
	let kind = sniff_archive(&prefix, complete).ok_or_else(|| {
		ApiError::bad_request("upload must be a tar or gzip-compressed tar archive")
	})?;
	let (node_id, client) = storage_node(state)?;
	let size = Arc::new(AtomicU64::new(0));
	let counter = Arc::clone(&size);
	let head = futures::stream::once(std::future::ready(Ok::<Bytes, io::Error>(prefix.freeze())));
	let rest = stream.map_err(io::Error::other);
	let body = head.chain(rest).inspect_ok(move |chunk| {
		counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
	});
	client
		.put_file(template_id, hash, reqwest::Body::wrap_stream(body))
		.await?;
	let record = FileRecord { node: node_id, size: size.load(Ordering::Relaxed) };
	let value = serde_json::to_vec(&record).map_err(|e| ApiError::internal(e.to_string()))?;
	state
		.cluster
		.kv
		.put(&file_key(template_id, hash), value, None)
		.await?;
	tracing::info!(template = template_id, hash, node = %node_id, size = record.size, ?kind, "stored build files");
	Ok(())
}

/// Alive, non-draining node with the most free disk.
fn storage_node(state: &AppState) -> ApiResult<(NodeId, vmon_node::client::NodeClient)> {
	let (id, _) = state
		.node_members()
		.into_iter()
		.filter(|(_, meta)| meta.node.as_ref().is_some_and(|cap| !cap.draining))
		.max_by_key(|(_, meta)| meta.node.as_ref().map_or(0, |cap| cap.disk_free_mb))
		.ok_or_else(|| ApiError::unavailable("no node is available to store build files"))?;
	Ok((id, state.node(id)?))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sniffs_gzip_tar_and_rejects_other_bytes() {
		assert_eq!(sniff_archive(&[0x1f, 0x8b, 8, 0], true), Some(ArchiveKind::Gzip));
		let mut tar = vec![0u8; 512];
		tar[..4].copy_from_slice(b"file");
		tar[257..263].copy_from_slice(b"ustar\0");
		assert_eq!(sniff_archive(&tar, false), Some(ArchiveKind::Tar));
		assert_eq!(sniff_archive(&[0u8; 1024], true), Some(ArchiveKind::Tar));
		assert_eq!(
			sniff_archive(&[0u8; 300], false),
			None,
			"zeros of an incomplete body are not an empty tar"
		);
		assert_eq!(sniff_archive(b"PK\x03\x04 zip file", true), None);
		assert_eq!(sniff_archive(b"", true), None);
	}
}
