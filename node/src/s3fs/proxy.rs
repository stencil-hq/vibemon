//! Per-sandbox Unix-socket server of the remote virtio-fs proxy protocol.
//!
//! One socket serves every S3 mount of a sandbox (requests carry their tag).
//! Each VMM device holds one connection and sends one request at a time;
//! connections are served concurrently. Credentials never leave the host.

use std::{collections::HashMap, io, path::PathBuf, sync::Arc};

use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{UnixListener, UnixStream},
	task::JoinSet,
};
use vmm::remotefs::proto;

use super::{
	bucket::FsError,
	mount::{Mount, Reply, request_tag},
};

/// A running proxy; stops accepting (and drops its connections) on drop.
#[derive(Debug)]
pub struct Proxy {
	sock:   PathBuf,
	mounts: Arc<HashMap<String, Arc<Mount>>>,
	task:   tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
	fn drop(&mut self) {
		self.task.abort();
		let _ = std::fs::remove_file(&self.sock);
	}
}

impl Proxy {
	/// Binds `sock` (replacing a stale socket, mode `0666` so a
	/// privilege-dropped VMM can connect; the parent directory guards it)
	/// and serves `mounts` by tag.
	///
	/// # Errors
	///
	/// Fails when the socket cannot be bound.
	pub fn start(sock: PathBuf, mounts: HashMap<String, Arc<Mount>>) -> io::Result<Self> {
		let _ = std::fs::remove_file(&sock);
		let listener = UnixListener::bind(&sock)?;
		{
			use std::os::unix::fs::PermissionsExt;
			std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o666))?;
		}
		let mounts = Arc::new(mounts);
		let served = Arc::clone(&mounts);
		let task = tokio::spawn(async move {
			let mut connections = JoinSet::new();
			loop {
				tokio::select! {
					accepted = listener.accept() => match accepted {
						Ok((stream, _)) => {
							let mounts = Arc::clone(&served);
							connections.spawn(async move {
								if let Err(error) = serve(stream, &mounts).await
									&& error.kind() != io::ErrorKind::UnexpectedEof
								{
									tracing::debug!(%error, "s3fs proxy connection ended");
								}
							});
						},
						Err(error) => {
							tracing::warn!(%error, "s3fs proxy accept failed");
							tokio::time::sleep(std::time::Duration::from_millis(50)).await;
						},
					},
					Some(_) = connections.join_next(), if !connections.is_empty() => {},
				}
			}
		});
		Ok(Self { sock, mounts, task })
	}

	/// Socket path.
	pub const fn sock(&self) -> &PathBuf {
		&self.sock
	}

	/// Uploads every dirty write-back file of every mount and waits.
	///
	/// # Errors
	///
	/// Returns the first failure after trying every mount.
	pub async fn sync_all(&self) -> Result<(), FsError> {
		let mut first = None;
		for (tag, mount) in self.mounts.iter() {
			if let Err(error) = mount.sync_all().await {
				tracing::warn!(tag, %error, "s3fs: flushing a mount failed");
				first.get_or_insert(error);
			}
		}
		first.map_or(Ok(()), Err)
	}
}

async fn read_frame(stream: &mut UnixStream) -> io::Result<(u8, u32, Vec<u8>)> {
	let mut header = [0u8; proto::HEADER_LEN];
	stream.read_exact(&mut header).await?;
	let len = u32::from_le_bytes(header[..4].try_into().expect("fixed header")) as usize;
	if len > proto::MAX_FRAME {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized proxy frame"));
	}
	let id = u32::from_le_bytes(header[5..].try_into().expect("fixed header"));
	let mut payload = vec![0; len];
	stream.read_exact(&mut payload).await?;
	Ok((header[4], id, payload))
}

async fn write_frame(stream: &mut UnixStream, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
	if payload.len() > proto::MAX_FRAME {
		return Err(io::Error::new(io::ErrorKind::InvalidInput, "oversized proxy reply"));
	}
	let mut header = [0u8; proto::HEADER_LEN];
	header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
	header[4] = ty;
	header[5..].copy_from_slice(&id.to_le_bytes());
	stream.write_all(&header).await?;
	stream.write_all(payload).await
}

async fn serve(mut stream: UnixStream, mounts: &HashMap<String, Arc<Mount>>) -> io::Result<()> {
	loop {
		let (ty, id, payload) = read_frame(&mut stream).await?;
		let (json, data) = match ty {
			proto::REQ => (payload.as_slice(), [].as_slice()),
			proto::REQ_DATA => proto::decode_data_request(&payload)?,
			other => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("unexpected proxy frame type {other}"),
				));
			},
		};
		let result = match serde_json::from_slice::<proto::Request>(json) {
			Ok(request) => match mounts.get(request_tag(&request)) {
				Some(mount) => mount.handle(request, data).await,
				None => Err(FsError::NotFound),
			},
			Err(error) => Err(FsError::Invalid(error.to_string())),
		};
		match result {
			Ok(Reply::Json(body)) => write_frame(&mut stream, proto::OK_JSON, id, &body).await?,
			Ok(Reply::Data(body)) => write_frame(&mut stream, proto::OK_DATA, id, &body).await?,
			Err(error) => {
				if !matches!(error, FsError::NotFound | FsError::Exists | FsError::NotEmpty) {
					tracing::debug!(%error, "s3fs request failed");
				}
				let reply = proto::ErrReply { code: error.code().to_owned(), msg: error.to_string() };
				let body = serde_json::to_vec(&reply).map_err(io::Error::other)?;
				write_frame(&mut stream, proto::ERR, id, &body).await?;
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use vmon_cloud::s3::fake::FakeS3;

	use super::*;
	use crate::{api::S3MountMode, s3fs::bucket::Bucket};

	/// Drives the proxy with the VMM's own blocking client framing.
	fn call(sock: &std::path::Path, frame: u8, payload: &[u8]) -> (u8, Vec<u8>) {
		let mut conn = std::os::unix::net::UnixStream::connect(sock).unwrap();
		proto::write_frame(&mut conn, frame, 7, payload).unwrap();
		let (ty, id, body) = proto::read_frame(&mut conn).unwrap();
		assert_eq!(id, 7);
		(ty, body)
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn proxy_routes_tags_and_carries_write_data() {
		let fake = FakeS3::start().await;
		fake.insert("bkt/p/hello.txt", "hello");
		let dir = tempfile::tempdir().unwrap();
		let bucket = Arc::new(Bucket::new(fake.client("bkt", "p"), Duration::ZERO, 1 << 20));
		let mount =
			Arc::new(Mount::new(bucket, S3MountMode::Writeback, &dir.path().join("stage")).unwrap());
		let sock = dir.path().join("proxy.sock");
		let proxy = Proxy::start(sock.clone(), HashMap::from([("s3_0".to_owned(), mount)])).unwrap();

		let sock2 = sock.clone();
		let (ty, body) = tokio::task::spawn_blocking(move || {
			let read = serde_json::to_vec(&proto::Request::Read {
				tag:    "s3_0".into(),
				path:   "hello.txt".into(),
				offset: 1,
				len:    3,
			})
			.unwrap();
			call(&sock2, proto::REQ, &read)
		})
		.await
		.unwrap();
		assert_eq!((ty, body.as_slice()), (proto::OK_DATA, b"ell".as_slice()));

		let sock2 = sock.clone();
		let (ty, body) = tokio::task::spawn_blocking(move || {
			let write = serde_json::to_vec(&proto::Request::Write {
				tag:    "s3_0".into(),
				path:   "hello.txt".into(),
				offset: 5,
			})
			.unwrap();
			call(&sock2, proto::REQ_DATA, &proto::encode_data_request(&write, b"!!").unwrap())
		})
		.await
		.unwrap();
		assert_eq!(ty, proto::OK_JSON);
		let reply: proto::WriteReply = serde_json::from_slice(&body).unwrap();
		assert_eq!(reply.written, 2);

		let sock2 = sock.clone();
		let (ty, body) = tokio::task::spawn_blocking(move || {
			let stat = serde_json::to_vec(&proto::Request::Stat {
				tag:  "nope".into(),
				path: "hello.txt".into(),
			})
			.unwrap();
			call(&sock2, proto::REQ, &stat)
		})
		.await
		.unwrap();
		assert_eq!(ty, proto::ERR);
		let error: proto::ErrReply = serde_json::from_slice(&body).unwrap();
		assert_eq!(error.code, "not_found");

		proxy.sync_all().await.unwrap();
		assert_eq!(fake.objects()["bkt/p/hello.txt"].data.as_ref(), b"hello!!");
		drop(proxy);
		assert!(!sock.exists(), "socket removed on drop");
	}
}
