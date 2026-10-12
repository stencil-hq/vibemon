//! Build file-context archives (`files/<templateID>/<hash>.tar`): uploaded by
//! the driver (`PUT /node/v1/files/{templateId}/{hash}`) and fetched by
//! builders on this or other nodes (`GET`).

use std::{io, path::PathBuf};

use axum::{
	Router,
	body::Body,
	extract::{Path, State},
	http::{StatusCode, header},
	response::{IntoResponse, Response},
	routing::put,
};
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::server::ApiError;

/// File-context archives under `<data_dir>/files`.
#[derive(Clone, Debug)]
pub struct FileStore {
	root: PathBuf,
}

/// Template IDs and hashes are lowercase alphanumeric tokens.
fn check_token(kind: &str, value: &str) -> Result<(), ApiError> {
	if value.is_empty()
		|| value.len() > 128
		|| !value
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		return Err(ApiError::invalid(format!("invalid {kind} {value:?}")));
	}
	Ok(())
}

impl FileStore {
	/// Store rooted at `root`.
	pub const fn new(root: PathBuf) -> Self {
		Self { root }
	}

	/// Archive path for `(template_id, hash)`.
	pub fn path(&self, template_id: &str, hash: &str) -> Result<PathBuf, ApiError> {
		check_token("template id", template_id)?;
		check_token("hash", hash)?;
		Ok(self.root.join(template_id).join(format!("{hash}.tar")))
	}

	/// Streams `body` to the archive atomically (temp file + rename). The
	/// hash is E2B's `filesHash` over the archive inputs, not the tar bytes,
	/// so it is a key here, not a checksum.
	pub async fn put(&self, template_id: &str, hash: &str, body: Body) -> Result<u64, ApiError> {
		let dest = self.path(template_id, hash)?;
		let dir = dest
			.parent()
			.map_or_else(|| self.root.clone(), PathBuf::from);
		tokio::fs::create_dir_all(&dir)
			.await
			.map_err(ApiError::internal)?;
		let tmp = dir.join(format!(".{hash}.{}.part", rand::random::<u32>()));
		let result = async {
			let mut file = tokio::fs::File::create(&tmp)
				.await
				.map_err(ApiError::internal)?;
			let mut size = 0u64;
			let mut stream = body.into_data_stream().map_err(io::Error::other);
			while let Some(chunk) = stream
				.try_next()
				.await
				.map_err(|e| ApiError::invalid(format!("upload interrupted: {e}")))?
			{
				size += chunk.len() as u64;
				file.write_all(&chunk).await.map_err(ApiError::internal)?;
			}
			file.sync_all().await.map_err(ApiError::internal)?;
			Ok::<_, ApiError>(size)
		}
		.await;
		match result {
			Ok(size) => {
				tokio::fs::rename(&tmp, &dest)
					.await
					.map_err(ApiError::internal)?;
				Ok(size)
			},
			Err(error) => {
				let _ = tokio::fs::remove_file(&tmp).await;
				Err(error)
			},
		}
	}

	/// Opens an archive for streaming.
	pub async fn open(
		&self,
		template_id: &str,
		hash: &str,
	) -> Result<(tokio::fs::File, u64), ApiError> {
		let path = self.path(template_id, hash)?;
		let file = tokio::fs::File::open(&path)
			.await
			.map_err(|e| match e.kind() {
				io::ErrorKind::NotFound => {
					ApiError::not_found(format!("file context {template_id}/{hash} not found"))
				},
				_ => ApiError::internal(e),
			})?;
		let len = file.metadata().await.map_err(ApiError::internal)?.len();
		Ok((file, len))
	}
}

/// Routes relative to `/node/v1`.
pub fn routes(store: FileStore) -> Router {
	Router::new()
		.route("/files/{template_id}/{hash}", put(put_file).get(get_file))
		.with_state(store)
}

async fn put_file(
	State(store): State<FileStore>,
	Path((template_id, hash)): Path<(String, String)>,
	body: Body,
) -> Result<StatusCode, ApiError> {
	store.put(&template_id, &hash, body).await?;
	Ok(StatusCode::CREATED)
}

async fn get_file(
	State(store): State<FileStore>,
	Path((template_id, hash)): Path<(String, String)>,
) -> Result<Response, ApiError> {
	let (file, len) = store.open(&template_id, &hash).await?;
	Ok((
		[
			(header::CONTENT_TYPE, "application/x-tar".to_owned()),
			(header::CONTENT_LENGTH, len.to_string()),
		],
		Body::from_stream(ReaderStream::new(file)),
	)
		.into_response())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn put_then_get_roundtrips_and_rejects_traversal() {
		let dir = tempfile::tempdir().unwrap();
		let store = FileStore::new(dir.path().to_owned());
		let size = store
			.put("tpl1", "abc123", Body::from("tar bytes"))
			.await
			.unwrap();
		assert_eq!(size, 9);
		let (_, len) = store.open("tpl1", "abc123").await.unwrap();
		assert_eq!(len, 9);
		assert_eq!(std::fs::read(dir.path().join("tpl1/abc123.tar")).unwrap(), b"tar bytes");
		assert!(store.path("..", "x").is_err());
		assert!(store.path("tpl", "../../etc").is_err());
		assert!(store.open("tpl1", "missing").await.is_err());
		// No temp files left behind.
		assert_eq!(std::fs::read_dir(dir.path().join("tpl1")).unwrap().count(), 1);
	}
}
