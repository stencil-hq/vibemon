//! Focused cluster substrate integration tests.
//!
//! Verifies single-writer fencing, lease expiry/reclaim, idempotent create,
//! owner relocation resolution, and S3 replica rehydration end-to-end across
//! independently constructed server/runtime instances.

#[cfg(test)]
mod tests {
	#[cfg(unix)]
	use std::os::unix::fs::PermissionsExt;
	use std::{
		collections::HashMap,
		io::{Read, Write},
		net::{SocketAddr, TcpListener, TcpStream},
		sync::{
			Arc, Barrier,
			atomic::{AtomicBool, AtomicU64, Ordering},
		},
		thread,
		time::Duration,
	};

	use crate::{
		config::ServeConfig,
		home::Home,
		mesh::{
			cluster_store::{ProductionStore, replica_object_lock_key},
			reconciler::ReplicaStore as ReconcileReplicaStore,
			routes::{CreateRecordWire, MeshLeaseManager, MeshRecordStore, MeshReplicaStore},
			runtime::{MeshRuntime, replica_object_key},
		},
		portable_history::{
			PortableHistory, PortablePointInput, PortableSuspendIntent, RetentionPolicy,
		},
		postgres as pg,
		security::Keyring,
	};

	const TEST_DB_URL: &str = "postgresql://fastbench:fastbench@127.0.0.1:15433/fastbench";

	fn require_test_database() -> bool {
		if TcpStream::connect(("127.0.0.1", 15433)).is_ok() {
			true
		} else {
			eprintln!("SKIP production cluster store: PostgreSQL is unavailable on port 15433");
			false
		}
	}

	fn portable_keyring(home: &tempfile::TempDir) -> Keyring {
		let home = Home::new(home.path());
		let keyring = Keyring::open(&home).unwrap();
		let key_path = home.keys_dir().join("cluster-recovery.key");
		std::fs::write(&key_path, "11".repeat(32)).unwrap();
		#[cfg(unix)]
		std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
		keyring
	}

	struct MockS3 {
		addr:   SocketAddr,
		stop:   Arc<AtomicBool>,
		handle: Option<thread::JoinHandle<()>>,
	}

	impl MockS3 {
		fn start() -> Self {
			Self::start_with_delay(Duration::ZERO)
		}

		fn start_with_delay(delay: Duration) -> Self {
			let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind S3 mock");
			listener
				.set_nonblocking(true)
				.expect("set S3 mock nonblocking");
			let addr = listener.local_addr().expect("S3 mock address");
			let stop = Arc::new(AtomicBool::new(false));
			let delay_millis = Arc::new(AtomicU64::new(delay.as_millis().try_into().unwrap()));
			let thread_stop = Arc::clone(&stop);
			let thread_delay = Arc::clone(&delay_millis);
			let handle = thread::spawn(move || {
				while !thread_stop.load(Ordering::Relaxed) {
					match listener.accept() {
						Ok((mut stream, _)) => serve_mock_s3(&mut stream, &thread_delay),
						Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
							thread::sleep(Duration::from_millis(5));
						},
						Err(_) => {
							thread::sleep(Duration::from_millis(5));
						},
					}
				}
			});
			Self { addr, stop, handle: Some(handle) }
		}
	}

	impl Drop for MockS3 {
		fn drop(&mut self) {
			self.stop.store(true, Ordering::Relaxed);
			let _ = TcpStream::connect(self.addr);
			if let Some(handle) = self.handle.take() {
				let _ = handle.join();
			}
		}
	}

	// Simple state store for S3 mock to allow true roundtrips!
	type MultipartPart = (u32, Vec<u8>);
	type MultipartUploads = HashMap<String, Vec<MultipartPart>>;

	thread_local! {
		static BUCKET_STORE: std::cell::RefCell<HashMap<String, Vec<u8>>> = std::cell::RefCell::new(HashMap::new());
		static MULTIPART_UPLOADS: std::cell::RefCell<MultipartUploads> = std::cell::RefCell::new(HashMap::new());
	}

	fn mock_s3_key(clean_path: &str) -> &str {
		clean_path
			.strip_prefix("/bucket/")
			.or_else(|| clean_path.strip_prefix("/bucket"))
			.unwrap_or(clean_path)
	}

	fn mock_s3_header<'a>(request: &'a str, expected: &str) -> Option<&'a str> {
		request.lines().find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name.eq_ignore_ascii_case(expected).then_some(value.trim())
		})
	}

	fn mock_s3_range(request: &str) -> Option<(usize, usize)> {
		let value = mock_s3_header(request, "range")?;
		let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
		Some((start.parse().ok()?, end.parse().ok()?))
	}

	fn serve_mock_s3(stream: &mut TcpStream, delay_millis: &AtomicU64) {
		let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
		let mut request_bytes = Vec::new();
		let mut buf = [0u8; 4096];
		loop {
			match stream.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					request_bytes.extend_from_slice(&buf[..n]);
					if request_bytes.windows(4).any(|w| w == b"\r\n\r\n") {
						break;
					}
					if request_bytes.len() > 16384 {
						break;
					}
				},
				Err(_) => break,
			}
		}

		let request = String::from_utf8_lossy(&request_bytes);
		let lines: Vec<&str> = request.lines().collect();
		if lines.is_empty() {
			return;
		}

		let req_line = lines[0];
		let parts: Vec<&str> = req_line.split_whitespace().collect();
		if parts.len() < 2 {
			return;
		}

		let method = parts[0];
		let path = parts[1];

		// Extract body if PUT or POST
		let mut body = Vec::new();
		if method == "PUT" || method == "POST" {
			let mut content_len = 0;
			for line in &lines {
				if line.to_ascii_lowercase().starts_with("content-length:")
					&& let Some(val) = line.split(':').nth(1)
				{
					content_len = val.trim().parse::<usize>().unwrap_or(0);
				}
			}
			if content_len > 0
				&& let Some(pos) = request_bytes.windows(4).position(|w| w == b"\r\n\r\n")
			{
				let body_start = pos + 4;
				if request_bytes.len() >= body_start + content_len {
					body = request_bytes[body_start..body_start + content_len].to_vec();
				} else {
					let needed = content_len - (request_bytes.len() - body_start);
					let mut temp_body = request_bytes[body_start..].to_vec();
					let mut read_buf = vec![0u8; needed];
					let _ = stream.read_exact(&mut read_buf);
					temp_body.extend_from_slice(&read_buf);
					body = temp_body;
				}
			}
		}
		if matches!(method, "GET" | "PUT" | "POST") {
			thread::sleep(Duration::from_millis(delay_millis.load(Ordering::Relaxed)));
		}

		let mut query = "";
		let mut clean_path = path;
		if let Some((p, q)) = path.split_once('?') {
			clean_path = p;
			query = q;
		}

		let (status, resp_body) = if method == "PUT" {
			if let Some(source) = mock_s3_header(&request, "x-amz-copy-source") {
				let source = source
					.split('?')
					.next()
					.unwrap_or(source)
					.replace("%2F", "/")
					.replace("%2f", "/");
				let copied = BUCKET_STORE.with(|store| store.borrow().get(&source).cloned());
				BUCKET_STORE.with(|store| {
					store
						.borrow_mut()
						.insert(clean_path.to_owned(), copied.expect("mock copy source exists"));
				});
			} else if query.contains("uploadId=") {
				let mut upload_id = String::new();
				let mut part_number = 1u32;
				for param in query.split('&') {
					if let Some((k, v)) = param.split_once('=') {
						if k == "uploadId" {
							upload_id = v.to_owned();
						} else if k == "partNumber" {
							part_number = v.parse::<u32>().unwrap_or(1);
						}
					}
				}
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				MULTIPART_UPLOADS.with(|store| {
					if let Some(parts) = store.borrow_mut().get_mut(&upload_key) {
						parts.push((part_number, body));
					}
				});
			} else {
				BUCKET_STORE.with(|store| {
					store.borrow_mut().insert(clean_path.to_owned(), body);
				});
			}
			("200 OK", Vec::new())
		} else if method == "POST" {
			if query.contains("uploads") {
				let upload_id = "test-upload-id-123";
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				MULTIPART_UPLOADS.with(|store| {
					store.borrow_mut().insert(upload_key, Vec::new());
				});
				let s3_key = mock_s3_key(clean_path);
				let xml = format!(
					r"<InitiateMultipartUploadResult>
						<Bucket>bucket</Bucket>
						<Key>{s3_key}</Key>
						<UploadId>{upload_id}</UploadId>
					</InitiateMultipartUploadResult>"
				);
				("200 OK", xml.into_bytes())
			} else if query.contains("uploadId=") {
				let mut upload_id = String::new();
				for param in query.split('&') {
					if let Some((k, v)) = param.split_once('=')
						&& k == "uploadId"
					{
						upload_id = v.to_owned();
					}
				}
				let upload_key = format!("{clean_path}?uploadId={upload_id}");
				let complete_body = MULTIPART_UPLOADS.with(|store| {
					let mut store = store.borrow_mut();
					if let Some(mut parts) = store.remove(&upload_key) {
						parts.sort_by_key(|p| p.0);
						let mut full = Vec::new();
						for (_, part_bytes) in parts {
							full.extend_from_slice(&part_bytes);
						}
						Some(full)
					} else {
						None
					}
				});
				if let Some(full_body) = complete_body {
					BUCKET_STORE.with(|store| {
						store.borrow_mut().insert(clean_path.to_owned(), full_body);
					});
				}
				let s3_key = mock_s3_key(clean_path);
				let xml = format!(
					r#"<CompleteMultipartUploadResult>
						<Location>http://127.0.0.1/bucket{clean_path}</Location>
						<Bucket>bucket</Bucket>
						<Key>{s3_key}</Key>
						<ETag>"etag-hash"</ETag>
					</CompleteMultipartUploadResult>"#
				);
				("200 OK", xml.into_bytes())
			} else {
				("400 Bad Request", Vec::new())
			}
		} else if method == "GET" {
			if path.contains("list-type=2") || path.contains("list-type%3D2") {
				let prefix = if let Some(pos) = path.find("prefix=") {
					let sub = &path[pos + 7..];
					let end = sub.find('&').unwrap_or(sub.len());
					sub[..end].replace("%2F", "/").replace("%2f", "/")
				} else {
					String::new()
				};

				let mut xml = r"<ListBucketResult><IsTruncated>false</IsTruncated>".to_owned();
				let mut contents = String::new();
				let target_prefix = format!("/bucket/{prefix}");
				BUCKET_STORE.with(|store| {
					for (key, val) in &*store.borrow() {
						if key.starts_with(&target_prefix) {
							let s3_key = &key[8..];
							use std::fmt::Write as _;
							let _ = write!(
								contents,
								"<Contents><Key>{s3_key}</Key><LastModified>2024-01-01T00:00:00.000Z</\
								 LastModified><ETag>&quot;etag-hash&quot;</ETag><Size>{}</Size></Contents>",
								val.len()
							);
						}
					}
				});
				xml.push_str(&contents);
				xml.push_str("</ListBucketResult>");
				("200 OK", xml.into_bytes())
			} else {
				let stored = BUCKET_STORE.with(|store| store.borrow().get(clean_path).cloned());
				match &stored {
					Some(data) => {
						if let Some((start, end)) = mock_s3_range(&request) {
							let end = end.min(data.len().saturating_sub(1));
							if start <= end {
								("206 Partial Content", data[start..=end].to_vec())
							} else {
								("416 Range Not Satisfiable", Vec::new())
							}
						} else {
							("200 OK", data.clone())
						}
					},
					None => ("404 Not Found", b"not found".to_vec()),
				}
			}
		} else if method == "DELETE" {
			BUCKET_STORE.with(|store| {
				store.borrow_mut().remove(clean_path);
			});
			("204 No Content", Vec::new())
		} else if method == "HEAD" {
			let exists = BUCKET_STORE.with(|store| store.borrow().contains_key(clean_path));
			if exists {
				("200 OK", Vec::new())
			} else {
				("404 Not Found", Vec::new())
			}
		} else {
			("400 Bad Request", Vec::new())
		};

		let content_length = if method == "HEAD" && status == "200 OK" {
			BUCKET_STORE.with(|store| store.borrow().get(clean_path).map_or(0, Vec::len))
		} else {
			resp_body.len()
		};
		let content_range = if status == "206 Partial Content" {
			let (start, _) = mock_s3_range(&request).expect("range response has request range");
			let total = BUCKET_STORE.with(|store| store.borrow().get(clean_path).map_or(0, Vec::len));
			format!(
				"Content-Range: bytes {start}-{}/{total}\r\n",
				start + resp_body.len().saturating_sub(1)
			)
		} else {
			String::new()
		};
		let last_modified = if method == "HEAD" && status == "200 OK" {
			"Last-Modified: Mon, 01 Jan 2024 00:00:00 GMT\r\n"
		} else {
			""
		};
		let header = format!(
			"HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nETag: \
			 \"etag-hash\"\r\n{content_range}{last_modified}Connection: close\r\n\r\n",
		);
		let _ = stream.write_all(header.as_bytes());
		let _ = stream.write_all(&resp_body);
	}

	fn test_config(mock_s3_addr: SocketAddr) -> ServeConfig {
		let overrides = HashMap::from([
			("cluster_mode".to_owned(), "production".to_owned()),
			("postgres_url".to_owned(), TEST_DB_URL.to_owned()),
			("s3_endpoint".to_owned(), format!("http://{mock_s3_addr}")),
			("s3_bucket".to_owned(), "bucket".to_owned()),
			("s3_region".to_owned(), "us-east-1".to_owned()),
			("s3_access_key".to_owned(), "test-key".to_owned()),
			("s3_secret_key".to_owned(), "test-secret".to_owned()),
			("s3_prefix".to_owned(), "test-prefix/".to_owned()),
			("portable_history_key_id".to_owned(), "cluster-recovery".to_owned()),
		]);
		crate::config::resolve_serve_config(&overrides).unwrap()
	}

	fn build_runtime(mock_s3_addr: SocketAddr, home: &tempfile::TempDir) -> Arc<MeshRuntime> {
		let mut config = test_config(mock_s3_addr);
		let home_inst = crate::home::Home::new(home.path());
		Keyring::open(&home_inst).unwrap();
		let key_path = home_inst.keys_dir().join("cluster-recovery.key");
		std::fs::write(&key_path, "11".repeat(32)).unwrap();
		#[cfg(unix)]
		std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
		config.home = home.path().to_path_buf();
		let engine = Arc::new(crate::engine::Engine::new(config.clone()).unwrap());
		MeshRuntime::new(config, home_inst, engine).unwrap()
	}

	fn reset_test_database() {
		ProductionStore::connect(TEST_DB_URL)
			.expect("connect clean production fixture")
			.clear_for_test()
			.expect("clear production fixture");
	}

	fn put_record(runtime: &MeshRuntime, mut record: CreateRecordWire) -> CreateRecordWire {
		if record.idempotency_key.is_empty() {
			record.idempotency_key = format!("fixture-{}-{}", record.sid, record.epoch);
		}
		if MeshRecordStore::get(runtime, &record.sid)
			.expect("read fixture ownership")
			.is_some()
		{
			return MeshRecordStore::put(runtime, record).expect("replay fixture ownership");
		}
		let store = runtime.cluster_store.as_ref().expect("production store");
		record.epoch = store
			.reserve_fixture_epoch(&record.sid, &record.owner, record.epoch, &record.idempotency_key)
			.expect("reserve fixture ownership");
		MeshRecordStore::put(runtime, record).expect("record fixture ownership")
	}

	fn admin_request<T>(message: T) -> tonic::Request<T> {
		let mut request = tonic::Request::new(message);
		request
			.extensions_mut()
			.insert(crate::security::tenant::Principal::local_admin());
		request
			.metadata_mut()
			.insert("x-vmon-principal-role", tonic::metadata::MetadataValue::from_static("admin"));
		request
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_mesh_fencing_and_idempotency_and_relocation() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		// 1. Idempotent Create: Same key returns the original resource
		let mut record_1_params = serde_json::Map::new();
		record_1_params.insert("image".to_owned(), serde_json::Value::String("debian".to_owned()));
		let secrets = serde_json::json!([{
			"name": "runtime-only",
			"values": {"TOKEN": "sensitive"},
		}]);
		record_1_params.insert("secrets".to_owned(), secrets.clone());
		let record_1 = CreateRecordWire {
			sid:             "sb-1".to_owned(),
			params:          record_1_params,
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "idem-key-123".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      12345.0,
		};

		put_record(&rt_a, record_1.clone());
		let local_record = MeshRecordStore::get(&*rt_a, "sb-1").unwrap().unwrap();
		assert_eq!(local_record.params.get("secrets"), Some(&secrets));
		let durable_record = MeshRecordStore::get(&*rt_b, "sb-1").unwrap().unwrap();
		assert!(!durable_record.params.contains_key("secrets"));

		// Put again on Independent Instance B with same idempotency key but different
		// details
		let mut record_2_params = serde_json::Map::new();
		record_2_params.insert("image".to_owned(), serde_json::Value::String("ubuntu".to_owned()));
		let record_2 = CreateRecordWire {
			sid:             "sb-1".to_owned(),
			params:          record_2_params,
			owner:           "node-b".to_owned(),
			epoch:           2,
			idempotency_key: "idem-key-123".to_owned(), // same idempotency key
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      99999.0,
		};

		put_record(&rt_b, record_2.clone());

		// Fetch and assert that it returned the ORIGINAL record due to idempotency!
		let resolved = MeshRecordStore::get(&*rt_b, "sb-1").unwrap().unwrap();
		assert_eq!(resolved.owner, "node-a");
		assert_eq!(resolved.epoch, 1);
		assert_eq!(resolved.created_at, 12345.0);
		let store_b = rt_b.cluster_store.as_ref().unwrap();

		// A portable restore uses the observation it validated: only one
		// claimant can advance that exact expired ownership generation.
		store_b.expire_owner_for_test("sb-1").unwrap();
		let pending = serde_json::json!({"restore": "fixture"});
		let claimed = store_b
			.claim_expected_pending("sb-1", "node-a", 1, "node-b", &pending)
			.unwrap();
		assert_eq!(claimed.owner, "node-b");
		assert_eq!(claimed.epoch, 2);
		let concurrent = store_b.claim_expected("sb-1", "node-a", 1, "node-c");
		assert!(concurrent.is_err(), "a stale expected epoch must not launch a second restorer");
		assert!(store_b.owns_epoch("sb-1", "node-b", 2).unwrap());

		// Abort releases only the still-current pre-launch claim.
		let released = store_b
			.release_claim("sb-1", "node-b", 2, "node-a", 1)
			.unwrap()
			.expect("the exact claim is released at a fresh epoch");
		assert_eq!(released.owner, "node-a");
		assert_eq!(released.epoch, 3);
		assert!(store_b.owns_epoch("sb-1", "node-a", 3).unwrap());

		// A stale finalize/abort token cannot undo a newer claimant.
		store_b.expire_owner_for_test("sb-1").unwrap();
		let second = store_b
			.claim_expected("sb-1", "node-a", 3, "node-c")
			.unwrap();
		assert_eq!(second.epoch, 4);
		assert!(
			store_b
				.release_claim("sb-1", "node-b", 2, "node-a", 1)
				.unwrap()
				.is_none()
		);
		assert!(store_b.owns_epoch("sb-1", "node-c", 4).unwrap());

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn background_migration_update_preserves_process_local_secrets() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();
		let runtime_a = build_runtime(mock_s3.addr, &home_a);
		let runtime_b = build_runtime(mock_s3.addr, &home_b);
		let secrets = serde_json::json!([{
			"name": "migration",
			"values": {"MIGRATION_SECRET": "retained"},
		}]);
		let mut params = serde_json::Map::new();
		params.insert("image".to_owned(), serde_json::json!("debian"));
		params.insert("secrets".to_owned(), secrets.clone());
		params.insert("_mesh_migration_id".to_owned(), serde_json::json!("token"));
		let mut record = put_record(&runtime_a, CreateRecordWire {
			sid: "background-migration".to_owned(),
			params,
			owner: "node-target".to_owned(),
			epoch: 1,
			idempotency_key: "background-migration-key".to_owned(),
			ha: "off".to_owned(),
			restart_policy: "none".to_owned(),
			created_at: 1.0,
		});
		record
			.params
			.insert("_mesh_migration_delta_dir".to_owned(), serde_json::json!("/staged/delta"));

		let updated = MeshRecordStore::update_exact(&*runtime_a, record)
			.expect("persist background migration stage");
		assert_eq!(updated.params.get("secrets"), Some(&secrets));
		let durable = MeshRecordStore::get(&*runtime_b, "background-migration")
			.expect("read durable migration")
			.expect("migration record");
		assert!(
			!durable.params.contains_key("secrets"),
			"process-local secrets must remain absent from PostgreSQL"
		);
		let sanitized_update = MeshRecordStore::update_exact(&*runtime_a, durable)
			.expect("persist sanitized background retry");
		assert_eq!(
			sanitized_update.params.get("secrets"),
			Some(&secrets),
			"a durable-record retry must retain same-lineage process-local secrets"
		);

		tokio::task::spawn_blocking(move || {
			drop(runtime_a);
			drop(runtime_b);
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_portable_points_share_lifecycle_generation_but_fence_stale_owner() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		let record = CreateRecordWire {
			sid:             "portable-sb".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-sb-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};
		put_record(&runtime, record);
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let keyring = portable_keyring(&home);
		let history = PortableHistory::connect(&config, &keyring)
			.unwrap()
			.unwrap();
		let archive = home.path().join("recovery.venc");
		std::fs::write(&archive, b"portable recovery bytes").unwrap();
		for (name, kind) in
			[("disk-one", "disk"), ("checkpoint-one", "checkpoint"), ("disk-two", "disk")]
		{
			let prepared = history
				.prepare(PortablePointInput {
					sid:                    "portable-sb".to_owned(),
					name:                   name.to_owned(),
					kind:                   kind.to_owned(),
					created_at_unix_millis: 1,
					owner_node:             "node-a".to_owned(),
					owner_epoch:            1,
					lifecycle_generation:   7,
					archive:                archive.clone(),
					incarnation_epoch:      1,
				})
				.unwrap();
			let published = history.publish(&prepared).unwrap();
			history
				.commit(&published, RetentionPolicy {
					per_kind:            24,
					max_age_unix_millis: None,
				})
				.unwrap();
		}
		assert_eq!(history.history("portable-sb").unwrap().len(), 3);

		// Claiming the next owner generation fences the old publisher: it cannot
		// publish a post-transfer point with the stale ownership tuple.
		store.expire_owner_for_test("portable-sb").unwrap();
		store
			.claim_expected("portable-sb", "node-a", 1, "node-b")
			.unwrap();
		assert_eq!(
			history.history("portable-sb").unwrap().len(),
			3,
			"same-incarnation owner handoff keeps prior committed history addressable"
		);
		let stale = history
			.prepare(PortablePointInput {
				sid: "portable-sb".to_owned(),
				name: "stale-after-claim".to_owned(),
				kind: "disk".to_owned(),
				created_at_unix_millis: 2,
				owner_node: "node-a".to_owned(),
				owner_epoch: 1,
				lifecycle_generation: 7,
				archive,
				incarnation_epoch: 1,
			})
			.unwrap();
		let stale = history.publish(&stale).unwrap();
		assert!(
			history
				.commit(&stale, RetentionPolicy { per_kind: 24, max_age_unix_millis: None })
				.is_err()
		);

		// An unchanged owner/epoch whose lease expired has nevertheless lost
		// publication authority. Uploaded bytes remain unreferenced and cannot
		// become a remotely restorable point.
		let expired = CreateRecordWire {
			sid:             "portable-expired".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-expired-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};
		put_record(&runtime, expired);
		store.expire_owner_for_test("portable-expired").unwrap();
		let expired_archive = home.path().join("expired.venc");
		std::fs::write(&expired_archive, b"expired owner bytes").unwrap();
		let expired = history
			.prepare(PortablePointInput {
				sid:                    "portable-expired".to_owned(),
				name:                   "must-not-commit".to_owned(),
				kind:                   "disk".to_owned(),
				created_at_unix_millis: 3,
				owner_node:             "node-a".to_owned(),
				owner_epoch:            1,
				lifecycle_generation:   1,
				archive:                expired_archive,
				incarnation_epoch:      1,
			})
			.unwrap();
		let expired = history.publish(&expired).unwrap();
		assert!(
			history
				.commit(&expired, RetentionPolicy {
					per_kind:            24,
					max_age_unix_millis: None,
				})
				.is_err()
		);
		assert!(history.history("portable-expired").unwrap().is_empty());

		// Use independent PostgreSQL connections and begin both operations from
		// one barrier.  The shared sandbox advisory lock admits either ordering:
		// a valid pre-claim point, or a fenced stale commit—never a post-claim
		// old-owner row.
		let race = CreateRecordWire {
			sid:             "portable-race".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-race-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};
		put_record(&runtime, race);
		store.expire_owner_for_test("portable-race").unwrap();
		let race_archive = home.path().join("race.venc");
		std::fs::write(&race_archive, b"race archive").unwrap();
		let prepared = history
			.prepare(PortablePointInput {
				sid:                    "portable-race".to_owned(),
				name:                   "race-point".to_owned(),
				kind:                   "disk".to_owned(),
				created_at_unix_millis: 3,
				owner_node:             "node-a".to_owned(),
				owner_epoch:            1,
				lifecycle_generation:   9,
				archive:                race_archive,
				incarnation_epoch:      1,
			})
			.unwrap();
		let published = history.publish(&prepared).unwrap();
		let barrier = Arc::new(Barrier::new(3));
		let commit_history = Arc::clone(&history);
		let commit_barrier = Arc::clone(&barrier);
		let commit = thread::spawn(move || {
			commit_barrier.wait();
			commit_history.commit(&published, RetentionPolicy {
				per_kind:            24,
				max_age_unix_millis: None,
			})
		});
		let claim_barrier = Arc::clone(&barrier);
		let claim = thread::spawn(move || {
			let store = ProductionStore::connect(TEST_DB_URL).unwrap();
			claim_barrier.wait();
			store.claim_expected("portable-race", "node-a", 1, "node-b")
		});
		barrier.wait();
		let committed = commit.join().unwrap();
		let claim = claim.join().unwrap().unwrap();
		assert_eq!(claim.owner, "node-b");
		assert_eq!(claim.epoch, 2);
		match committed {
			Ok(point) => {
				assert_eq!(point.owner_node, "node-a");
				assert_eq!(point.owner_epoch, 1);
			},
			Err(error) => assert!(
				error.to_string().contains("fenced"),
				"only ownership fencing may reject the raced publication: {error}"
			),
		}
		let rows = history.history("portable-race").unwrap();
		assert!(
			rows
				.iter()
				.all(|point| point.owner_node == "node-a" && point.owner_epoch == 1)
		);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn portable_suspend_commit_atomically_exposes_exact_point_and_marker() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		put_record(&runtime, CreateRecordWire {
			sid:             "portable-suspend-atomic".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-suspend-atomic-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		});
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let history = PortableHistory::connect(&config, &portable_keyring(&home))
			.unwrap()
			.unwrap();
		let archive = home.path().join("atomic-suspend.venc");
		std::fs::write(&archive, b"atomic suspended recovery bytes").unwrap();
		let input = PortablePointInput {
			sid:                    "portable-suspend-atomic".to_owned(),
			name:                   "atomic-point".to_owned(),
			kind:                   "checkpoint".to_owned(),
			created_at_unix_millis: 41,
			owner_node:             "node-a".to_owned(),
			owner_epoch:            1,
			lifecycle_generation:   17,
			archive:                archive.clone(),
			incarnation_epoch:      1,
		};
		let prepared = history.prepare(input).unwrap();
		let published = history.publish(&prepared).unwrap();
		let intent = PortableSuspendIntent {
			sid:                  "portable-suspend-atomic".to_owned(),
			owner:                "node-a".to_owned(),
			epoch:                1,
			point:                "atomic-point".to_owned(),
			lifecycle_generation: 17,
		};
		let retention = RetentionPolicy { per_kind: 24, max_age_unix_millis: None };
		assert_eq!(
			history
				.commit_suspend(&published, retention, &intent)
				.unwrap()
				.name,
			"atomic-point"
		);
		let marker = store
			.suspend_marker("portable-suspend-atomic")
			.unwrap()
			.unwrap();
		assert_eq!(
			(marker.state.as_str(), marker.point.as_str(), marker.generation),
			("suspending", "atomic-point", 17)
		);

		// Replaying the exact post-commit request is idempotent and still
		// confirms the same durable marker, rather than skipping marker work.
		history
			.commit_suspend(&published, retention, &intent)
			.unwrap();
		assert_eq!(
			store
				.suspend_marker("portable-suspend-atomic")
				.unwrap()
				.unwrap(),
			marker
		);

		// The subsequent tier prune takes portable-history then sandbox locks.
		// It must retain the non-serving marker's old checkpoint even when a
		// newer checkpoint would otherwise exhaust its per-tier quota.
		let newest = history
			.prepare(PortablePointInput {
				sid:                    "portable-suspend-atomic".to_owned(),
				name:                   "newest-checkpoint".to_owned(),
				kind:                   "checkpoint".to_owned(),
				created_at_unix_millis: 43,
				owner_node:             "node-a".to_owned(),
				owner_epoch:            1,
				lifecycle_generation:   18,
				archive:                archive.clone(),
				incarnation_epoch:      1,
			})
			.unwrap();
		let newest = history.publish(&newest).unwrap();
		history
			.commit(&newest, RetentionPolicy { per_kind: 1, max_age_unix_millis: None })
			.unwrap();
		let retained = history.history("portable-suspend-atomic").unwrap();
		assert!(retained.iter().any(|point| point.name == "atomic-point"));
		assert!(
			retained
				.iter()
				.any(|point| point.name == "newest-checkpoint")
		);

		let mismatched_prepared = history
			.prepare(PortablePointInput {
				sid: "portable-suspend-atomic".to_owned(),
				name: "must-not-appear".to_owned(),
				kind: "checkpoint".to_owned(),
				created_at_unix_millis: 42,
				owner_node: "node-a".to_owned(),
				owner_epoch: 1,
				lifecycle_generation: 17,
				archive,
				incarnation_epoch: 1,
			})
			.unwrap();
		let mismatched = history.publish(&mismatched_prepared).unwrap();
		assert!(
			history
				.commit_suspend(&mismatched, retention, &intent)
				.is_err()
		);
		assert!(
			!history
				.history("portable-suspend-atomic")
				.unwrap()
				.iter()
				.any(|point| point.name == "must-not-appear")
		);
		assert_eq!(
			store
				.suspend_marker("portable-suspend-atomic")
				.unwrap()
				.unwrap(),
			marker
		);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn delayed_portable_commit_is_fenced_after_same_sid_recreation() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		let record = CreateRecordWire {
			sid:             "portable-aba".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-aba-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};
		put_record(&runtime, record.clone());
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let history = PortableHistory::connect(&config, &portable_keyring(&home))
			.unwrap()
			.unwrap();
		let archive = home.path().join("portable-aba.venc");
		std::fs::write(&archive, b"delayed old owner publication").unwrap();
		let prepared = history
			.prepare(PortablePointInput {
				sid: record.sid.clone(),
				name: "delayed-old-point".to_owned(),
				kind: "disk".to_owned(),
				created_at_unix_millis: 1,
				owner_node: record.owner.clone(),
				owner_epoch: 1,
				lifecycle_generation: 1,
				archive,
				incarnation_epoch: 1,
			})
			.unwrap();
		let published = history.publish(&prepared).unwrap();

		store.begin_delete(&record.sid, &record.owner, 1).unwrap();
		store.commit_delete(&record.sid, &record.owner, 1).unwrap();
		put_record(&runtime, record.clone());
		let recreated = store.resolve(&record.sid).unwrap().unwrap();
		assert!(recreated.epoch > 1, "recreated sid must receive a fresh epoch");

		assert!(
			history
				.commit(&published, RetentionPolicy {
					per_kind:            24,
					max_age_unix_millis: None,
				})
				.is_err()
		);
		assert!(
			history
				.lookup(&record.sid, "delayed-old-point")
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn predecessor_portable_points_are_hidden_after_same_sid_recreation() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		let sid = "portable-predecessor";
		let original = CreateRecordWire {
			sid:             sid.to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-predecessor-a".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		};
		put_record(&runtime, original.clone());
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let history = PortableHistory::connect(&config, &portable_keyring(&home))
			.unwrap()
			.unwrap();
		let old_archive = home.path().join("portable-predecessor-old.venc");
		std::fs::write(&old_archive, b"old lineage archive").unwrap();
		let old = history
			.commit(
				&history
					.publish(
						&history
							.prepare(PortablePointInput {
								sid:                    sid.to_owned(),
								name:                   "old-lineage".to_owned(),
								kind:                   "disk".to_owned(),
								created_at_unix_millis: 1,
								owner_node:             original.owner,
								owner_epoch:            1,
								lifecycle_generation:   1,
								archive:                old_archive,
								incarnation_epoch:      1,
							})
							.unwrap(),
					)
					.unwrap(),
				RetentionPolicy { per_kind: 24, max_age_unix_millis: None },
			)
			.unwrap();

		// Deliberately bypass the facade's ordered history cleanup to preserve
		// a predecessor row, as a crash/legacy deployment could have done.
		store.begin_delete(sid, "node-a", 1).unwrap();
		store.commit_delete(sid, "node-a", 1).unwrap();
		put_record(&runtime, CreateRecordWire {
			sid:             sid.to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-b".to_owned(),
			epoch:           1,
			idempotency_key: "portable-predecessor-b".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      2.0,
		});
		let recreated = store.resolve(sid).unwrap().unwrap();
		assert!(recreated.epoch > old.owner_epoch);

		assert!(history.history(sid).unwrap().is_empty());
		assert!(history.lookup(sid, &old.name).unwrap().is_none());
		assert!(
			history
				.download(&old, &home.path().join("predecessor-download.venc"))
				.is_err()
		);
		assert!(
			history
				.pin_rollback_target(sid, &old.name, 1, &recreated.owner, recreated.epoch)
				.is_err()
		);

		let new_archive = home.path().join("portable-predecessor-new.venc");
		std::fs::write(&new_archive, b"new lineage archive").unwrap();
		let new = history
			.commit(
				&history
					.publish(
						&history
							.prepare(PortablePointInput {
								sid:                    sid.to_owned(),
								name:                   "new-lineage".to_owned(),
								kind:                   "checkpoint".to_owned(),
								created_at_unix_millis: 2,
								owner_node:             recreated.owner.clone(),
								owner_epoch:            recreated.epoch,
								lifecycle_generation:   1,
								archive:                new_archive,
								incarnation_epoch:      recreated.incarnation_epoch,
							})
							.unwrap(),
					)
					.unwrap(),
				RetentionPolicy { per_kind: 24, max_age_unix_millis: None },
			)
			.unwrap();
		assert_eq!(history.history(sid).unwrap(), vec![new.clone()]);
		assert_eq!(history.lookup(sid, &new.name).unwrap(), Some(new));
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn migration_backfill_does_not_revive_predecessor_portable_rows() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		put_record(&runtime, CreateRecordWire {
			sid:             "portable-migration-lineage".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-b".to_owned(),
			epoch:           1,
			idempotency_key: "portable-migration-lineage-b".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      100.0,
		});
		let current = store
			.resolve("portable-migration-lineage")
			.unwrap()
			.expect("live ownership");
		let mut database = pg::connect(TEST_DB_URL, "portable migration lineage fixture").unwrap();
		pg::blocking(|| {
			database.execute("DELETE FROM portable_recovery_points WHERE sid = $1", &[&current.sid])
		})
		.unwrap();
		for (name, created) in [("predecessor", 99_999_i64), ("current", 100_000_i64)] {
			pg::blocking(|| {
				database.execute(
					"INSERT INTO portable_recovery_points (sid, name, kind, created_at_unix_millis, \
					 archive_size_bytes, archive_key, archive_sha256, manifest_key, manifest_sha256, \
					 owner_node, owner_epoch, incarnation_epoch, lifecycle_generation, \
					 committed_at_unix_millis) VALUES ($1, $2, 'disk', $3, 1, 'archive', \
					 'archive-sha', 'manifest', 'manifest-sha', $4, $5, 0, 1, $3)",
					&[&current.sid, &name, &created, &current.owner, &current.epoch],
				)
			})
			.unwrap();
		}
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let history = PortableHistory::connect(&config, &portable_keyring(&home))
			.unwrap()
			.unwrap();
		let rows = pg::blocking(|| {
			database.query(
				"SELECT name, incarnation_epoch FROM portable_recovery_points WHERE sid = $1 ORDER BY \
				 name",
				&[&current.sid],
			)
		})
		.unwrap();
		assert_eq!(rows[0].try_get::<_, String>(0).unwrap(), "current");
		assert_eq!(
			rows[0].try_get::<_, i64>(1).unwrap(),
			current.incarnation_epoch,
			"row captured after the current ownership creation is backfilled"
		);
		assert_eq!(rows[1].try_get::<_, String>(0).unwrap(), "predecessor");
		assert_eq!(
			rows[1].try_get::<_, i64>(1).unwrap(),
			0,
			"predecessor row remains unassigned and therefore not restorable"
		);
		assert_eq!(
			history
				.history(&current.sid)
				.unwrap()
				.into_iter()
				.map(|point| point.name)
				.collect::<Vec<_>>(),
			vec!["current"]
		);
		drop(database);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_portable_history_deletion_requires_tombstone_and_drops_all_tiers() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		put_record(&runtime, CreateRecordWire {
			sid:             "portable-delete".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-delete-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		});
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let keyring = portable_keyring(&home);
		let history = PortableHistory::connect(&config, &keyring)
			.unwrap()
			.unwrap();
		let archive = home.path().join("delete.venc");
		std::fs::write(&archive, b"portable delete bytes").unwrap();
		for (name, kind) in [("disk", "disk"), ("checkpoint", "checkpoint")] {
			let prepared = history
				.prepare(PortablePointInput {
					sid:                    "portable-delete".to_owned(),
					name:                   name.to_owned(),
					kind:                   kind.to_owned(),
					created_at_unix_millis: 1,
					owner_node:             "node-a".to_owned(),
					owner_epoch:            1,
					lifecycle_generation:   1,
					archive:                archive.clone(),
					incarnation_epoch:      1,
				})
				.unwrap();
			let published = history.publish(&prepared).unwrap();
			history
				.commit(&published, RetentionPolicy {
					per_kind:            24,
					max_age_unix_millis: None,
				})
				.unwrap();
		}
		assert!(
			history
				.delete_sandbox_history("portable-delete", "node-a", 1)
				.is_err()
		);
		let mut database = pg::connect(TEST_DB_URL, "portable deletion tombstone test").unwrap();
		// Upload completes while the sandbox is live.  The later tombstone must
		// still fence its metadata commit, so it cannot recreate history after
		// the deletion purge.
		let delayed = history
			.prepare(PortablePointInput {
				sid: "portable-delete".to_owned(),
				name: "delayed-after-delete".to_owned(),
				kind: "disk".to_owned(),
				created_at_unix_millis: 2,
				owner_node: "node-a".to_owned(),
				owner_epoch: 1,
				lifecycle_generation: 2,
				archive,
				incarnation_epoch: 1,
			})
			.unwrap();
		let delayed = history.publish(&delayed).unwrap();
		pg::blocking(|| {
			database.execute(
				"UPDATE sandbox_ownership SET deleting = TRUE, delete_token = $4 WHERE sid = $1 AND \
				 owner = $2 AND epoch = $3",
				&[&"portable-delete", &"node-a", &1_i64, &"delete:node-a:1"],
			)
		})
		.unwrap();
		assert!(
			history
				.commit(&delayed, RetentionPolicy {
					per_kind:            24,
					max_age_unix_millis: None,
				})
				.is_err()
		);
		assert!(history.history("portable-delete").unwrap().is_empty());
		history
			.delete_sandbox_history("portable-delete", "node-a", 1)
			.unwrap();
		assert!(history.history("portable-delete").unwrap().is_empty());
		// Repeating after the final ownership deletion is a no-op, not a path
		// that could resurrect a point or delete another owner's objects.
		pg::blocking(|| {
			database.execute(
				"DELETE FROM sandbox_ownership WHERE sid = $1 AND owner = $2 AND epoch = $3",
				&[&"portable-delete", &"node-a", &1_i64],
			)
		})
		.unwrap();
		assert_eq!(
			history
				.delete_sandbox_history("portable-delete", "node-a", 1)
				.unwrap(),
			0
		);
		drop(database);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_portable_dedicated_sessions_survive_slow_io_and_gc_race() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start_with_delay(Duration::from_millis(300));
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		put_record(&runtime, CreateRecordWire {
			sid:             "portable-slow".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "portable-slow-idem".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		});
		let mut config = test_config(mock_s3.addr);
		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let keyring = portable_keyring(&home);
		let history = PortableHistory::connect(&config, &keyring)
			.unwrap()
			.unwrap();
		let archive = home.path().join("slow.venc");
		std::fs::write(&archive, vec![7_u8; 128 * 1024]).unwrap();
		let input = |name: &str| PortablePointInput {
			sid:                    "portable-slow".to_owned(),
			name:                   name.to_owned(),
			kind:                   "disk".to_owned(),
			created_at_unix_millis: 1,
			owner_node:             "node-a".to_owned(),
			owner_epoch:            1,
			lifecycle_generation:   1,
			archive:                archive.clone(),
			incarnation_epoch:      1,
		};

		let prepared = history.prepare(input("slow-io")).unwrap();
		let publishing_history = Arc::clone(&history);
		let publisher = thread::spawn(move || publishing_history.publish(&prepared));
		thread::sleep(Duration::from_millis(75));
		let listed_at = std::time::Instant::now();
		assert!(history.history("portable-slow").unwrap().is_empty());
		assert!(
			listed_at.elapsed() < Duration::from_millis(200),
			"publish must not hold the metadata client while S3 is slow"
		);
		let published = publisher.join().unwrap().unwrap();
		let point = history
			.commit(&published, RetentionPolicy { per_kind: 24, max_age_unix_millis: None })
			.unwrap();

		let destination = home.path().join("slow-download.venc");
		let downloading_history = Arc::clone(&history);
		let downloading_point = point.clone();
		let downloader =
			thread::spawn(move || downloading_history.download(&downloading_point, &destination));
		thread::sleep(Duration::from_millis(75));
		let listed_at = std::time::Instant::now();
		assert_eq!(history.history("portable-slow").unwrap(), vec![point]);
		assert!(
			listed_at.elapsed() < Duration::from_millis(200),
			"download must not hold the metadata client while S3 is slow"
		);
		downloader.join().unwrap().unwrap();

		let prepared = history.prepare(input("gc-race")).unwrap();
		let published = history.publish(&prepared).unwrap();
		let barrier = Arc::new(Barrier::new(3));
		let commit_history = Arc::clone(&history);
		let commit_barrier = Arc::clone(&barrier);
		let commit = thread::spawn(move || {
			commit_barrier.wait();
			commit_history.commit(&published, RetentionPolicy {
				per_kind:            24,
				max_age_unix_millis: None,
			})
		});
		let gc_history = Arc::clone(&history);
		let gc_barrier = Arc::clone(&barrier);
		let gc = thread::spawn(move || {
			gc_barrier.wait();
			gc_history.gc()
		});
		barrier.wait();
		commit.join().unwrap().unwrap();
		gc.join().unwrap().unwrap();
		assert_eq!(history.history("portable-slow").unwrap().len(), 2);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn stale_replica_staging_gc_waits_for_final_publication_lock_and_keeps_malformed_keys() {
		if !require_test_database() {
			return;
		}
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();
		let home = tempfile::tempdir().unwrap();
		let runtime = build_runtime(mock_s3.addr, &home);
		let store = runtime.cluster_store.as_ref().unwrap();
		store.clear_for_test().unwrap();
		let namespace = store.object_namespace().to_owned();
		let mut config = test_config(mock_s3.addr);
		let s3 = Arc::new(
			crate::s3::S3Client::new(crate::s3::S3MountConfig {
				bucket:    config.s3_bucket.clone().unwrap(),
				prefix:    config.s3_prefix.clone().unwrap(),
				region:    config.s3_region.clone().unwrap(),
				endpoint:  config.s3_endpoint.clone(),
				read_only: false,
				creds:     Some(crate::s3::S3Credentials {
					access_key:    config.s3_access_key.clone().unwrap(),
					secret_key:    config.s3_secret_key.clone().unwrap(),
					session_token: None,
				}),
				auth:      crate::s3::S3Auth::Inline,
			})
			.unwrap(),
		);
		let scope = "b".repeat(64);
		let digest = "a".repeat(64);
		let staging = format!(
			"{namespace}/replicas/.staging/by-key/{scope}/{digest}-{}.vbundle",
			uuid::Uuid::new_v4()
		);
		s3.put(&staging, b"stale staged replica".to_vec())
			.await
			.unwrap();

		let mut lock_client = pg::connect(TEST_DB_URL, "staged replica GC lock").unwrap();
		let final_object = format!("{namespace}/replicas/by-key/{scope}/{digest}.vbundle");
		let lock_key = replica_object_lock_key(&final_object);
		pg::blocking(|| {
			lock_client.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&lock_key])
		})
		.unwrap();

		config.portable_history_key_id = Some("cluster-recovery".to_owned());
		let history = PortableHistory::connect(&config, &portable_keyring(&home))
			.unwrap()
			.unwrap();
		let (started_tx, started_rx) = std::sync::mpsc::channel();
		let (result_tx, result_rx) = std::sync::mpsc::channel();
		let gc_history = Arc::clone(&history);
		let gc = thread::spawn(move || {
			started_tx.send(()).unwrap();
			result_tx.send(gc_history.gc()).unwrap();
		});
		started_rx.recv().unwrap();
		assert!(matches!(
			result_rx.recv_timeout(Duration::from_millis(75)),
			Err(std::sync::mpsc::RecvTimeoutError::Timeout)
		));
		pg::blocking(|| {
			lock_client.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", &[&lock_key])
		})
		.unwrap();
		drop(lock_client);
		assert_eq!(
			result_rx
				.recv_timeout(Duration::from_secs(5))
				.expect("GC completes after publication lock release")
				.unwrap(),
			1
		);
		gc.join().unwrap();
		let removed_staging = s3.stat(&staging).await;
		assert!(
			matches!(removed_staging, Err(crate::s3::S3Error::NotFound)),
			"stale staging object remains after GC: {removed_staging:?}"
		);

		let malformed = format!("{namespace}/replicas/.staging/by-key/{scope}/not-a-digest.vbundle");
		s3.put(&malformed, b"leave malformed staging alone".to_vec())
			.await
			.unwrap();
		assert_eq!(history.gc().unwrap(), 0);
		assert!(s3.stat(&malformed).await.is_ok());
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_mesh_lease_expiry_and_reclaim() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		// Acquire lease on volume-1 with TTL 0.2s
		let decision_1 = rt_a
			.vote_grant("vol-1".to_owned(), "node-1".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_1.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Collision: node-2 tries to steal it at the same epoch -> fails
		let decision_2 = rt_b
			.vote_grant("vol-1".to_owned(), "node-2".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_2.get("granted").unwrap(), &serde_json::Value::Bool(false));
		assert!(
			decision_2
				.get("reason")
				.unwrap()
				.as_str()
				.unwrap()
				.contains("Lease collision")
		);

		// Wait 250ms for expiry
		tokio::time::sleep(Duration::from_millis(250)).await;

		// Reclaim: after expiry, node-2 can claim the lease!
		let decision_3 = rt_b
			.vote_grant("vol-1".to_owned(), "node-2".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_3.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Release lease cleanly
		let release_decision = rt_b
			.vote_release("vol-1".to_owned(), "node-2".to_owned(), 1)
			.unwrap();
		assert_eq!(release_decision.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// After release, node-1 can immediately acquire it again without waiting
		let decision_4 = rt_a
			.vote_grant("vol-1".to_owned(), "node-1".to_owned(), 1, 0.2)
			.unwrap();
		assert_eq!(decision_4.get("granted").unwrap(), &serde_json::Value::Bool(true));

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_s3_replica_rehydration() {
		if !require_test_database() {
			return;
		}
		// Start Mock S3 server
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let mock_s3 = MockS3::start();

		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);
		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();
		let owner = put_record(&rt_a, CreateRecordWire {
			sid:             "sb-replica-1".to_owned(),
			params:          serde_json::Map::new(),
			owner:           "node-a".to_owned(),
			epoch:           1,
			idempotency_key: "sb-replica-1".to_owned(),
			ha:              "async".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1.0,
		});
		assert_eq!(owner.epoch, 1);

		// Create a dummy replica checkpoint directory on A
		let snapshot_dir = home_a.path().join("replica-checkpoint-1");
		std::fs::create_dir_all(&snapshot_dir).unwrap();

		// Write a dummy test file inside the checkpoint
		let file_path = snapshot_dir.join("test-file.txt");
		std::fs::write(&file_path, "replica roundtrip payload content!").unwrap();

		// Write the required marker file agent-ready.json to index it
		let marker_path = snapshot_dir.join("agent-ready.json");
		std::fs::write(&marker_path, "{}").unwrap();

		// Index the exact transfer-tree digest used by replica verification.
		let digest = crate::image::cas::snapshot_digest(&snapshot_dir).unwrap();
		crate::image::cas::index_template(&snapshot_dir, Some(&digest)).unwrap();
		let keyring = portable_keyring(&home_a);
		let snapshot = keyring.snapshot("cluster-recovery").unwrap();
		let object_key = replica_object_key(
			rt_a.cluster_store.as_ref().unwrap().object_namespace(),
			&digest,
			&snapshot,
		)
		.unwrap();

		// Call put on rt_a's replica store -> publishes .vbundle to S3!
		MeshReplicaStore::put(
			&*rt_a,
			"sb-replica-1".to_owned(),
			object_key,
			digest.clone(),
			"node-a".to_owned(),
			1,
			1,
			snapshot_dir.to_string_lossy().into_owned(),
			serde_json::Map::from_iter([(
				"encryption_key_id".to_owned(),
				serde_json::Value::String("cluster-recovery".to_owned()),
			)]),
		)
		.await
		.unwrap();
		// Materialize the shared record on independent instance B from S3.
		let rehydrated = ReconcileReplicaStore::get(&*rt_b, "sb-replica-1")
			.await
			.expect("read shared replica")
			.expect("shared replica exists");
		assert!(rehydrated.snapshot_dir.exists());

		// Verify that the rehydrated directory on B contains our exact test file.
		let read_file = rehydrated.snapshot_dir.join("test-file.txt");
		assert!(read_file.exists());
		let content = std::fs::read_to_string(read_file).unwrap();
		assert_eq!(content, "replica roundtrip payload content!");

		// Safe blocking drop of runtimes
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		tokio::task::spawn_blocking(move || {
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_artifact_store_portability() {
		if !require_test_database() {
			return;
		}
		let mock_s3 = MockS3::start();
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();

		let root_a = tempfile::tempdir().unwrap();
		let root_b = tempfile::tempdir().unwrap();

		let config = test_config(mock_s3.addr);

		// Store A gets instantiated with root_a
		let store_a =
			crate::function::artifact::ArtifactStore::open_with_config(root_a.path(), &config)
				.unwrap();

		// Store B gets instantiated with root_b
		let store_b =
			crate::function::artifact::ArtifactStore::open_with_config(root_b.path(), &config)
				.unwrap();

		// Put some artifact into Store A
		let data = b"portable artifact roundtrip content bytes!";
		let artifact = store_a.put(data).unwrap();
		let digest = artifact.digest.clone();

		// Verify it was uploaded to store_a locally
		assert_eq!(artifact.size, data.len() as u64);
		assert!(store_a.path_for(&digest).unwrap().exists());
		drop(artifact);

		// Now read/verify the same digest from store_b, which has an empty local cache
		// root_b!
		assert!(!store_b.path_for(&digest).unwrap().exists()); // empty initially

		let read_bytes = store_b.read(&digest, Some(data.len() as u64)).unwrap();
		assert_eq!(read_bytes, data);

		// Now it should be cached in root_b
		assert!(store_b.path_for(&digest).unwrap().exists());

		// Now removal on Store A makes the remote object unavailable
		store_a.remove(&digest).unwrap();

		// Remove the locally cached file in root_b so reading must pull from S3 again
		let path_b = store_b.path_for(&digest).unwrap();
		let _ = std::fs::remove_file(&path_b);

		// Clear store_b's S3 client caches so it actually query/stat S3 again
		store_b.clear_cache();

		// Attempting to read it from store_b now should fail because it was removed
		// from S3 Authoritatively!
		let err = store_b.read(&digest, Some(data.len() as u64)).unwrap_err();
		assert!(
			err.to_string().contains("failed to read chunk from S3") || err.to_string().contains("S3")
		);
	}
	#[tokio::test(flavor = "multi_thread")]
	async fn test_production_grpc_gateway_transparency() {
		use std::sync::atomic::AtomicU64;

		use pb::sandbox_service_server::SandboxService;
		use vmon_proto::v1 as pb;

		use crate::{
			api::{ApiState, GrpcApi, Transport, boot_gate},
			function::FunctionDomain,
			mesh::routes::MeshControl,
		};

		if !require_test_database() {
			return;
		}

		let mock_s3 = MockS3::start();
		let _guard = pg::TEST_DATABASE_LOCK.lock().await;
		reset_test_database();
		let home_a = tempfile::tempdir().unwrap();
		let home_b = tempfile::tempdir().unwrap();

		let rt_a = build_runtime(mock_s3.addr, &home_a);
		let rt_b = build_runtime(mock_s3.addr, &home_b);

		// Setup to enable mesh
		rt_a
			.setup("127.0.0.1:1234".to_owned(), "us-east-1".to_owned(), None)
			.unwrap();
		rt_b
			.setup("127.0.0.1:1235".to_owned(), "us-east-1".to_owned(), None)
			.unwrap();

		rt_a
			.cluster_store
			.as_ref()
			.unwrap()
			.clear_for_test()
			.unwrap();

		let node_id_a = MeshControl::node_id(&*rt_a);
		let node_id_b = MeshControl::node_id(&*rt_b);

		// Register a sandbox owned by node A
		let mut params_a = serde_json::Map::new();
		let mut tags_a = serde_json::Map::new();
		tags_a.insert("env".to_owned(), serde_json::Value::String("prod".to_owned()));
		params_a.insert("tags".to_owned(), serde_json::Value::Object(tags_a));

		let record_a = CreateRecordWire {
			sid:             "sb-prod-1".to_owned(),
			params:          params_a,
			owner:           node_id_a.clone(),
			epoch:           1,
			idempotency_key: "idem-prod-1".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      1000.0,
		};
		put_record(&rt_a, record_a);

		// Register a sandbox owned by node B
		let mut params_b = serde_json::Map::new();
		let mut tags_b = serde_json::Map::new();
		tags_b.insert("env".to_owned(), serde_json::Value::String("dev".to_owned()));
		params_b.insert("tags".to_owned(), serde_json::Value::Object(tags_b));

		let record_b = CreateRecordWire {
			sid:             "sb-dev-2".to_owned(),
			params:          params_b,
			owner:           node_id_b.clone(),
			epoch:           1,
			idempotency_key: "idem-dev-2".to_owned(),
			ha:              "off".to_owned(),
			restart_policy:  "none".to_owned(),
			created_at:      2000.0,
		};
		put_record(&rt_b, record_b);

		// Construct GrpcApi on instance A
		let home_inst_a = crate::home::Home::new(home_a.path());
		let func_domain_a =
			FunctionDomain::open(home_inst_a, rt_a.engine.engine.clone(), &rt_a.config).unwrap();
		let state_a = ApiState {
			engine:        rt_a.engine.engine.clone(),
			functions:     func_domain_a,
			config:        Arc::new(rt_a.config.clone()),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport:     Transport::Unix,
			mesh:          Some(rt_a.clone()),
			orch_worker:   None,
			boot_gate:     boot_gate(&rt_a.config),
		};
		let api_a = GrpcApi::new(state_a);

		// Construct GrpcApi on instance B
		let home_inst_b = crate::home::Home::new(home_b.path());
		let func_domain_b =
			FunctionDomain::open(home_inst_b, rt_b.engine.engine.clone(), &rt_b.config).unwrap();
		let state_b = ApiState {
			engine:        rt_b.engine.engine.clone(),
			functions:     func_domain_b,
			config:        Arc::new(rt_b.config.clone()),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport:     Transport::Unix,
			mesh:          Some(rt_b.clone()),
			orch_worker:   None,
			boot_gate:     boot_gate(&rt_b.config),
		};
		let api_b = GrpcApi::new(state_b);

		// 1. Gateway-Transparent List from Node A: must see BOTH sandboxes (remotely
		//    and locally owned)
		let req_list_a = admin_request(pb::ListSandboxesRequest { tags: vec![] });
		let res_list_a = api_a.list(req_list_a).await.unwrap().into_inner();
		for item in &res_list_a.sandboxes_json {
			println!("Listed item: {item}");
		}
		assert_eq!(res_list_a.sandboxes_json.len(), 2);

		// 2. Tag filter list: Dev tag filter from Node A must see only sb-dev-2
		let req_list_dev =
			admin_request(pb::ListSandboxesRequest { tags: vec!["env=dev".to_owned()] });
		let res_list_dev = api_a.list(req_list_dev).await.unwrap().into_inner();
		assert_eq!(res_list_dev.sandboxes_json.len(), 1);
		let dev_sandbox: serde_json::Value =
			serde_json::from_str(&res_list_dev.sandboxes_json[0]).unwrap();
		assert_eq!(dev_sandbox.get("id").unwrap().as_str().unwrap(), "sb-dev-2");

		// 3. Transparent Get (remotely owned sandbox): Node A can retrieve sb-dev-2
		//    (proxies / resolves owner via PgStore)
		let req_list_b = admin_request(pb::ListSandboxesRequest { tags: vec![] });
		let res_list_b = api_b.list(req_list_b).await.unwrap().into_inner();
		assert_eq!(res_list_b.sandboxes_json.len(), 2);

		// 4. Transparent Get: Local get routes locally to node-a, but since it's absent
		//    from local active engine state, it returns Unavailable (owner unreachable)
		let req_get_local = admin_request(pb::SandboxRef { id: "sb-prod-1".to_owned() });
		let err_get_local = api_a.get(req_get_local).await.unwrap_err();
		assert_eq!(err_get_local.code(), tonic::Code::Unavailable);

		// 5. Transparent Get: Remote get of remotely-owned sandbox also correctly
		//    returns Unavailable since node-b has no peer URL registered via gossip yet
		let req_get_remote = admin_request(pb::SandboxRef { id: "sb-dev-2".to_owned() });
		let err_get_remote = api_a.get(req_get_remote).await.unwrap_err();
		assert_eq!(err_get_remote.code(), tonic::Code::Unavailable);

		// Safe blocking drop of runtimes and API states
		let mut rt_a_opt = Some(rt_a);
		let mut rt_b_opt = Some(rt_b);
		let mut api_a_opt = Some(api_a);
		let mut api_b_opt = Some(api_b);
		tokio::task::spawn_blocking(move || {
			drop(api_a_opt.take());
			drop(api_b_opt.take());
			drop(rt_a_opt.take());
			drop(rt_b_opt.take());
		})
		.await
		.unwrap();
	}
}
