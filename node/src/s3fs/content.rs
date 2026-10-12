//! The volume-content API (`spec/e2b/sdk/spec/openapi-volumecontent.yml`)
//! over a volume stored in the operator object store.
//!
//! Same wire behavior as
//! the node-local implementation in [`crate::volumes`]; the driver serves it
//! directly against S3. Objects use the [`super::bucket`] layout, so files
//! written here and by sandbox mounts are the same objects.

use std::{sync::Arc, time::Duration};

use axum::{
	Json,
	body::Body,
	http::{Method, StatusCode, header},
	response::{IntoResponse, Response},
};
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use vmon_cloud::s3::{PutOptions, S3Client};

use super::bucket::{Attrs, Bucket, FsError, Kind, Stat, dir_prefix, parent, unix_now};
use crate::volumes::{ContentQuery, MetadataPatch, VolumeEntryStat, VolumeError, components};

/// Largest upload accepted without a `Content-Length`.
const MAX_UNSIZED_UPLOAD: usize = 256 << 20;
/// Concurrent `HEAD`s while listing.
const STAT_CONCURRENCY: usize = 16;

fn fs_error(error: &FsError, path: &str) -> VolumeError {
	match error {
		FsError::NotFound => {
			VolumeError::new(StatusCode::NOT_FOUND, "not_found", format!("path {path} not found"))
		},
		FsError::Exists => {
			VolumeError::new(StatusCode::CONFLICT, "conflict", format!("path {path} already exists"))
		},
		FsError::Access(message) => {
			VolumeError::new(StatusCode::FORBIDDEN, "forbidden", format!("{path}: {message}"))
		},
		FsError::NotDir | FsError::IsDir | FsError::NotEmpty | FsError::Invalid(_) => {
			VolumeError::invalid(format!("{path}: {error}"))
		},
		other => {
			VolumeError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", format!("{path}: {other}"))
		},
	}
}

fn rfc3339(secs: u64) -> String {
	chrono::DateTime::from_timestamp(i64::try_from(secs).unwrap_or(0), 0)
		.unwrap_or_default()
		.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// API entry for `stat` at the mount-relative `rel` (`""` = root).
fn entry(rel: &str, stat: &Stat) -> VolumeEntryStat {
	let dir = stat.kind == Kind::Dir;
	let time = rfc3339(stat.attrs.mtime.unwrap_or(stat.mtime));
	VolumeEntryStat {
		name:   rel
			.rsplit('/')
			.next()
			.filter(|name| !name.is_empty())
			.unwrap_or("/")
			.to_owned(),
		kind:   if dir { "directory" } else { "file" },
		path:   format!("/{rel}"),
		size:   if dir {
			0
		} else {
			i64::try_from(stat.size).unwrap_or(i64::MAX)
		},
		mode:   stat.attrs.perm.unwrap_or(if dir { 0o777 } else { 0o666 }),
		uid:    stat.attrs.uid.unwrap_or(0),
		gid:    stat.attrs.gid.unwrap_or(0),
		atime:  time.clone(),
		mtime:  time.clone(),
		ctime:  time,
		target: None,
	}
}

async fn require(bucket: &Bucket, rel: &str) -> Result<Stat, VolumeError> {
	bucket
		.stat(rel, false)
		.await
		.map_err(|error| fs_error(&error, &format!("/{rel}")))?
		.ok_or_else(|| fs_error(&FsError::NotFound, &format!("/{rel}")))
}

/// Entries below the directory `rel` up to `depth` levels, depth-first by
/// name like the node-local listing.
async fn list(bucket: &Bucket, rel: &str, depth: u32) -> Result<Vec<VolumeEntryStat>, VolumeError> {
	let mut out = Vec::new();
	// (directory, remaining depth, insertion index of its entries in `out`)
	let mut pending = vec![(rel.to_owned(), depth, 0usize)];
	while let Some((dir, depth, at)) = pending.pop() {
		let children = bucket
			.list(&dir)
			.await
			.map_err(|error| fs_error(&error, &format!("/{dir}")))?;
		let paths: Vec<String> = children
			.iter()
			.map(|child| {
				if dir.is_empty() {
					child.name.clone()
				} else {
					format!("{dir}/{}", child.name)
				}
			})
			.collect();
		let mut stats: Vec<Result<Option<Stat>, FsError>> = Vec::with_capacity(paths.len());
		for chunk in paths.chunks(STAT_CONCURRENCY) {
			let heads = chunk.iter().map(|path| bucket.stat(path, false));
			stats.extend(futures::future::join_all(heads).await);
		}
		let mut entries = Vec::with_capacity(paths.len());
		for (path, stat) in paths.iter().zip(stats) {
			if let Some(stat) = stat.map_err(|error| fs_error(&error, &format!("/{path}")))? {
				entries.push((path.clone(), stat));
			}
		}
		// Splice this directory's entries in right after its own entry.
		let mut insert = at;
		let mut subdirs = Vec::new();
		for (path, stat) in entries {
			out.insert(insert, entry(&path, &stat));
			insert += 1;
			if stat.kind == Kind::Dir && depth > 1 {
				subdirs.push((path, insert));
			}
		}
		// The stack pops the subdirectory with the largest position first, and
		// every splice of its subtree lands at or after that position: the
		// recorded positions of the remaining (smaller) ones stay valid.
		for (path, position) in subdirs {
			pending.push((path, depth - 1, position));
		}
	}
	Ok(out)
}

/// Creates directory markers for `rel` and (with `parents`) its missing
/// ancestors.
async fn make_dirs(
	bucket: &Bucket,
	rel: &str,
	parents: bool,
	attrs: Attrs,
) -> Result<(), VolumeError> {
	let display = format!("/{rel}");
	if bucket
		.stat(rel, false)
		.await
		.map_err(|error| fs_error(&error, &display))?
		.is_some()
	{
		return Err(fs_error(&FsError::Exists, &display));
	}
	// Walk up to the nearest existing ancestor.
	let mut missing = Vec::new();
	let mut up = parent(rel);
	loop {
		match bucket
			.stat(up, false)
			.await
			.map_err(|error| fs_error(&error, &display))?
		{
			Some(stat) if stat.kind == Kind::Dir => break,
			Some(_) => return Err(fs_error(&FsError::NotDir, &display)),
			None if parents => {
				missing.push(up.to_owned());
				up = parent(up);
			},
			None => return Err(fs_error(&FsError::NotFound, &display)),
		}
	}
	for ancestor in missing.iter().rev() {
		bucket
			.put_marker(ancestor, Attrs { perm: attrs.perm, mtime: attrs.mtime, ..Attrs::default() })
			.await
			.map_err(|error| fs_error(&error, &display))?;
	}
	bucket
		.put_marker(rel, attrs)
		.await
		.map_err(|error| fs_error(&error, &display))?;
	bucket.invalidate(rel);
	Ok(())
}

/// Fails unless every ancestor of `rel` is a directory or missing.
async fn check_ancestors(bucket: &Bucket, rel: &str) -> Result<(), VolumeError> {
	let mut current = parent(rel);
	while !current.is_empty() {
		if let Some(stat) = bucket
			.stat(current, false)
			.await
			.map_err(|error| fs_error(&error, &format!("/{rel}")))?
			&& stat.kind != Kind::Dir
		{
			return Err(fs_error(&FsError::NotDir, &format!("/{rel}")));
		}
		current = parent(current);
	}
	Ok(())
}

/// Serves one volume-content request against the volume rooted at `client`
/// (boxed: keeps the caller's handler future type small).
///
/// # Errors
///
/// Spec-shaped [`VolumeError`]s (`400 invalid`, `404 not_found`,
/// `409 conflict`, …).
pub fn serve(
	client: S3Client,
	op: String,
	method: Method,
	query: ContentQuery,
	content_length: Option<u64>,
	body: Body,
) -> futures::future::BoxFuture<'static, Result<Response, VolumeError>> {
	Box::pin(async move { serve_inner(client, &op, &method, query, content_length, body).await })
}

async fn serve_inner(
	client: S3Client,
	op: &str,
	method: &Method,
	query: ContentQuery,
	content_length: Option<u64>,
	body: Body,
) -> Result<Response, VolumeError> {
	let bucket = Arc::new(Bucket::new(client, Duration::ZERO, 0));
	let input = query
		.path
		.clone()
		.filter(|path| !path.is_empty())
		.ok_or_else(|| VolumeError::invalid("missing path"))?;
	let rel = components(&input)?.join("/");
	let display = format!("/{rel}");
	match (op, method.as_str()) {
		("path", "GET") => Ok(Json(entry(&rel, &require(&bucket, &rel).await?)).into_response()),
		("path", "PATCH") => {
			let bytes = axum::body::to_bytes(body, 64 * 1024)
				.await
				.map_err(|error| VolumeError::invalid(error.to_string()))?;
			let patch: MetadataPatch = serde_json::from_slice(&bytes)
				.map_err(|error| VolumeError::invalid(error.to_string()))?;
			if rel.is_empty() {
				return Err(VolumeError::invalid("the volume root's metadata cannot be changed"));
			}
			let stat = require(&bucket, &rel).await?;
			let update = Attrs {
				perm:  patch.mode.map(|mode| mode & 0o7777),
				uid:   patch.uid,
				gid:   patch.gid,
				mtime: None,
			};
			let attrs = stat.attrs.merge(update);
			let attrs = Attrs { mtime: attrs.mtime.or(Some(stat.mtime)), ..attrs };
			let changed = if stat.kind == Kind::Dir {
				bucket.put_marker(&rel, attrs).await.map(drop)
			} else {
				bucket.set_metadata(&rel, attrs).await.map(drop)
			};
			changed.map_err(|error| fs_error(&error, &display))?;
			bucket.invalidate(&rel);
			Ok(Json(entry(&rel, &require(&bucket, &rel).await?)).into_response())
		},
		("path", "DELETE") => {
			if rel.is_empty() {
				return Err(VolumeError::invalid("cannot delete the volume root"));
			}
			let stat = require(&bucket, &rel).await?;
			let client = bucket.client();
			let deleted = if stat.kind == Kind::Dir {
				client.delete_prefix(&dir_prefix(&rel)).await.map(drop)
			} else {
				client.delete(&rel).await
			};
			deleted.map_err(|error| fs_error(&error.into(), &display))?;
			Ok(StatusCode::NO_CONTENT.into_response())
		},
		("dir", "GET") => {
			let stat = require(&bucket, &rel).await?;
			if stat.kind != Kind::Dir {
				return Err(VolumeError::invalid(format!("{display} is not a directory")));
			}
			let out = list(&bucket, &rel, query.depth.unwrap_or(1).max(1)).await?;
			Ok(Json(out).into_response())
		},
		("dir", "POST") => {
			if rel.is_empty() {
				return Err(fs_error(&FsError::Exists, &display));
			}
			let attrs = Attrs {
				perm:  Some(query.mode.unwrap_or(0o755) & 0o7777),
				uid:   query.uid,
				gid:   query.gid,
				mtime: Some(unix_now()),
			};
			make_dirs(&bucket, &rel, query.force.unwrap_or(false), attrs).await?;
			Ok((StatusCode::CREATED, Json(entry(&rel, &require(&bucket, &rel).await?)))
				.into_response())
		},
		("file", "GET") => {
			let stat = require(&bucket, &rel).await?;
			if stat.kind != Kind::File {
				return Err(VolumeError::invalid(format!("{display} is not a file")));
			}
			let object = bucket
				.client()
				.get(&rel)
				.await
				.map_err(|error| fs_error(&error.into(), &display))?;
			let body = Body::from_stream(object.body.map_err(std::io::Error::from));
			Ok((
				[
					(header::CONTENT_TYPE, "application/octet-stream".to_owned()),
					(header::CONTENT_LENGTH, object.meta.size.to_string()),
				],
				body,
			)
				.into_response())
		},
		("file", "PUT") => {
			if rel.is_empty() {
				return Err(VolumeError::invalid("cannot write the volume root"));
			}
			match bucket
				.stat(&rel, false)
				.await
				.map_err(|error| fs_error(&error, &display))?
			{
				Some(stat) if stat.kind == Kind::Dir => {
					return Err(fs_error(&FsError::IsDir, &display));
				},
				Some(_) if !query.force.unwrap_or(false) => {
					return Err(fs_error(&FsError::Exists, &display));
				},
				_ => {},
			}
			check_ancestors(&bucket, &rel).await?;
			let attrs = Attrs {
				perm:  Some(query.mode.unwrap_or(0o644) & 0o7777),
				uid:   query.uid,
				gid:   query.gid,
				mtime: Some(unix_now()),
			};
			let options =
				PutOptions { metadata: attrs.to_metadata(Kind::File), ..PutOptions::default() };
			let stream = body.into_data_stream().map_err(std::io::Error::other);
			let uploaded = if let Some(size) = content_length {
				bucket
					.client()
					.upload_stream(&rel, size, stream, &options)
					.await
			} else {
				let mut buffer = BytesMut::new();
				let mut stream = std::pin::pin!(stream);
				while let Some(chunk) = stream
					.try_next()
					.await
					.map_err(|error| VolumeError::invalid(error.to_string()))?
				{
					if buffer.len() + chunk.len() > MAX_UNSIZED_UPLOAD {
						return Err(VolumeError::new(
							StatusCode::LENGTH_REQUIRED,
							"invalid",
							"uploads without Content-Length are limited to 256 MiB",
						));
					}
					buffer.extend_from_slice(&chunk);
				}
				let bytes: Bytes = buffer.freeze();
				bucket.client().put(&rel, bytes, &options).await
			};
			uploaded.map_err(|error| fs_error(&error.into(), &display))?;
			bucket.invalidate(&rel);
			Ok((StatusCode::CREATED, Json(entry(&rel, &require(&bucket, &rel).await?)))
				.into_response())
		},
		_ => Err(VolumeError::new(
			StatusCode::METHOD_NOT_ALLOWED,
			"invalid",
			format!("{method} {op} is not supported"),
		)),
	}
}

#[cfg(test)]
mod tests {
	use vmon_cloud::s3::fake::FakeS3;

	use super::*;

	fn query(path: &str) -> ContentQuery {
		ContentQuery { path: Some(path.to_owned()), ..ContentQuery::default() }
	}

	async fn json(response: Response) -> (StatusCode, serde_json::Value) {
		let status = response.status();
		let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
			.await
			.unwrap();
		(status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
	}

	async fn call(
		fake: &FakeS3,
		op: &str,
		method: Method,
		query: ContentQuery,
		body: &'static str,
	) -> Result<Response, VolumeError> {
		let len = (method == Method::PUT).then_some(body.len() as u64);
		serve(fake.client("ops", "volumes/v1"), op.to_owned(), method, query, len, Body::from(body))
			.await
	}

	#[tokio::test]
	async fn content_api_round_trip_against_s3() {
		let fake = FakeS3::start().await;
		let mut put = query("/a/b/hello.txt");
		put.mode = Some(0o600);
		put.uid = Some(1000);
		let (status, stat) = json(
			call(&fake, "file", Method::PUT, put, "hi there")
				.await
				.unwrap(),
		)
		.await;
		assert_eq!(status, StatusCode::CREATED);
		assert_eq!(
			(stat["type"].as_str(), stat["size"].as_i64(), stat["mode"].as_u64()),
			(Some("file"), Some(8), Some(0o600))
		);
		assert_eq!(
			(stat["path"].as_str(), stat["uid"].as_u64()),
			(Some("/a/b/hello.txt"), Some(1000))
		);
		assert!(
			fake
				.keys()
				.contains(&"ops/volumes/v1/a/b/hello.txt".to_owned())
		);

		let conflict = call(&fake, "file", Method::PUT, query("/a/b/hello.txt"), "x").await;
		assert_eq!(conflict.unwrap_err().into_response().status(), StatusCode::CONFLICT);
		let mut force = query("/a/b/hello.txt");
		force.force = Some(true);
		call(&fake, "file", Method::PUT, force, "replaced")
			.await
			.unwrap();
		let body = call(&fake, "file", Method::GET, query("a/b/hello.txt"), "")
			.await
			.unwrap();
		let bytes = axum::body::to_bytes(body.into_body(), 1024).await.unwrap();
		assert_eq!(bytes.as_ref(), b"replaced");

		let mut deep = query("/");
		deep.depth = Some(3);
		let (_, listing) = json(call(&fake, "dir", Method::GET, deep, "").await.unwrap()).await;
		let paths: Vec<_> = listing
			.as_array()
			.unwrap()
			.iter()
			.map(|entry| entry["path"].as_str().unwrap().to_owned())
			.collect();
		assert_eq!(paths, ["/a", "/a/b", "/a/b/hello.txt"]);

		let patch = Body::from(r#"{"mode": 416}"#);
		let patched = serve(
			fake.client("ops", "volumes/v1"),
			"path".to_owned(),
			Method::PATCH,
			query("/a/b/hello.txt"),
			None,
			patch,
		)
		.await
		.unwrap();
		assert_eq!(json(patched).await.1["mode"], 0o640);

		let missing_parent = call(&fake, "dir", Method::POST, query("/x/y"), "").await;
		assert_eq!(missing_parent.unwrap_err().into_response().status(), StatusCode::NOT_FOUND);
		let mut parents = query("/x/y");
		parents.force = Some(true);
		let (status, _) = json(call(&fake, "dir", Method::POST, parents, "").await.unwrap()).await;
		assert_eq!(status, StatusCode::CREATED);
		assert!(fake.keys().contains(&"ops/volumes/v1/x/y/".to_owned()));
		let mut deep = query("/");
		deep.depth = Some(3);
		let (_, listing) = json(call(&fake, "dir", Method::GET, deep, "").await.unwrap()).await;
		let paths: Vec<_> = listing
			.as_array()
			.unwrap()
			.iter()
			.map(|entry| entry["path"].as_str().unwrap().to_owned())
			.collect();
		assert_eq!(paths, ["/a", "/a/b", "/a/b/hello.txt", "/x", "/x/y"], "depth-first by name");

		let escape = call(&fake, "file", Method::GET, query("../../etc/passwd"), "").await;
		assert_eq!(escape.unwrap_err().into_response().status(), StatusCode::BAD_REQUEST);
		let root = call(&fake, "path", Method::DELETE, query("/"), "").await;
		assert_eq!(root.unwrap_err().into_response().status(), StatusCode::BAD_REQUEST);
		let deleted = call(&fake, "path", Method::DELETE, query("/a"), "")
			.await
			.unwrap();
		assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
		let gone = call(&fake, "path", Method::GET, query("/a"), "").await;
		assert_eq!(gone.unwrap_err().into_response().status(), StatusCode::NOT_FOUND);
		assert!(
			!fake
				.keys()
				.iter()
				.any(|key| key.starts_with("ops/volumes/v1/a"))
		);
	}
}
