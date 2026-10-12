use std::collections::BTreeMap;

use axum::http::StatusCode;
use bytes::Bytes;
use futures::StreamExt;

use super::{fake::FakeS3, *};

fn location(value: &str) -> S3Config {
	S3Config { location: value.into(), ..S3Config::default() }
}

#[test]
fn locations_resolve_addressing_and_encode_keys() {
	let aws = S3Location::parse(&location("s3://paused-bucket/team/state/"), "eu-west-1").unwrap();
	assert_eq!(aws.url(), "s3://paused-bucket/team/state");
	assert_eq!(aws.prefix(), "team/state");
	assert_eq!(
		aws.object_url("paused/id/a b%/memory.bin")
			.unwrap()
			.as_str(),
		"https://paused-bucket.s3.eu-west-1.amazonaws.com/team/state/paused/id/a%20b%25/memory.bin"
	);
	let dotted = S3Location::parse(&location("s3://my.bucket"), "us-east-1").unwrap();
	assert_eq!(
		dotted.object_url("k").unwrap().as_str(),
		"https://s3.us-east-1.amazonaws.com/my.bucket/k",
		"dotted buckets fall back to path-style (TLS wildcard)"
	);
	let custom = S3Config {
		endpoint: Some("http://127.0.0.1:19000".into()),
		allow_http: true,
		..location("s3://dev/pre")
	};
	assert_eq!(
		S3Location::parse(&custom, "us-east-1")
			.unwrap()
			.object_url("x/y")
			.unwrap()
			.as_str(),
		"http://127.0.0.1:19000/dev/pre/x/y"
	);
	let path = S3Location::parse(&location("https://objects.example/bucket/team"), "r").unwrap();
	assert_eq!(
		path.object_url("one/two/").unwrap().as_str(),
		"https://objects.example/bucket/team/one/two/"
	);
	let hosted = S3Config {
		addressing: Some(Addressing::VirtualHosted),
		..location("https://objects.example:8443/bucket")
	};
	assert_eq!(
		S3Location::parse(&hosted, "r")
			.unwrap()
			.object_url("k")
			.unwrap()
			.as_str(),
		"https://bucket.objects.example:8443/k"
	);
	let child = path.child("volumes/v1").unwrap();
	assert_eq!(child.prefix(), "team/volumes/v1");
	assert_eq!(child.url(), "https://objects.example/bucket/team/volumes/v1");
	assert_eq!(child.relative("team/volumes/v1/a"), Some("a"));
	assert_eq!(child.relative("team/volumes/v10/a"), None);
	for bad in ["", "/a", "a//b", "a/../b", "./a", "a/."] {
		assert!(path.object_url(bad).is_err(), "{bad:?}");
	}
	for url in [
		"http://objects.example/bucket",
		"https://objects.example/",
		"https://user:secret@objects.example/bucket",
		"s3://bucket/prefix?token=x",
		"s3://bucket:9000/prefix",
		"file:///tmp/paused",
	] {
		assert!(S3Location::parse(&location(url), "us-east-1").is_err(), "{url}");
	}
	let override_on_https =
		S3Config { endpoint: Some("https://e".into()), ..location("https://objects.example/b") };
	assert!(S3Location::parse(&override_on_https, "r").is_err());
}

#[test]
fn errors_classify_by_status_and_code() {
	let (error, transient) = classify(StatusCode::NOT_FOUND, "NoSuchKey", "");
	assert_eq!((error, transient), (S3Error::NotFound, false));
	assert_eq!(classify(StatusCode::PRECONDITION_FAILED, "", "").0, S3Error::PreconditionFailed);
	assert_eq!(
		classify(StatusCode::CONFLICT, "ConditionalRequestConflict", "").0,
		S3Error::PreconditionFailed
	);
	assert_eq!(
		classify(StatusCode::SERVICE_UNAVAILABLE, "SlowDown", ""),
		(S3Error::Throttled, true)
	);
	assert!(matches!(classify(StatusCode::FORBIDDEN, "AccessDenied", "no").0, S3Error::Auth(_)));
	assert!(classify(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", "").1);
	assert!(!classify(StatusCode::BAD_REQUEST, "InvalidArgument", "").1);
	assert_eq!(
		embedded_error("<Error><Code>InternalError</Code><Message>m</Message></Error>"),
		Some(S3Error::Other("HTTP 500 Internal Server Error InternalError: m".into()))
	);
	assert_eq!(xml_unescape("a&amp;b&lt;&#x41;&#66;&bogus;"), "a&b<AB&bogus;");
}

#[test]
fn part_sizes_respect_the_part_count_limit() {
	assert_eq!(part_size(1).unwrap(), PART_BYTES as u64);
	assert_eq!(part_size(PART_BYTES as u64 * MAX_PARTS).unwrap(), PART_BYTES as u64);
	let big = PART_BYTES as u64 * MAX_PARTS + 1;
	let part = part_size(big).unwrap();
	assert!(big.div_ceil(part) <= MAX_PARTS && part.is_multiple_of(1 << 20));
	assert!(part_size(60 << 40).is_err(), "parts above 5 GiB");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_roundtrip_conditions_metadata_and_ranges() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "root");
	let mut options = PutOptions::default();
	options.metadata.insert("Mode".into(), "0644".into());
	options.if_none_match = true;
	let etag = s3
		.put("dir/a b.txt", Bytes::from_static(b"hello world"), &options)
		.await
		.unwrap();
	assert!(fake.keys().contains(&"bucket/root/dir/a b.txt".to_owned()));
	assert_eq!(
		s3.put("dir/a b.txt", Bytes::from_static(b"x"), &options)
			.await,
		Err(S3Error::PreconditionFailed),
		"If-None-Match: * refuses to overwrite"
	);
	let head = s3.head("dir/a b.txt").await.unwrap();
	assert_eq!((head.size, head.etag.as_deref()), (11, Some(etag.as_str())));
	assert_eq!(head.metadata, BTreeMap::from([("mode".to_owned(), "0644".to_owned())]));
	assert!(head.last_modified.is_some());
	assert_eq!(s3.get_bytes("dir/a b.txt", 64).await.unwrap(), "hello world");
	assert!(s3.get_bytes("dir/a b.txt", 4).await.is_err());
	assert_eq!(s3.get_range("dir/a b.txt", 6, 100).await.unwrap(), "world");
	assert_eq!(s3.get_range("dir/a b.txt", 11, 4).await.unwrap(), "");
	assert_eq!(s3.get_range("dir/a b.txt", 0, 0).await.unwrap(), "");
	assert_eq!(
		s3.get_range_if_match("dir/a b.txt", 0, 5, &etag)
			.await
			.unwrap(),
		"hello"
	);
	assert_eq!(
		s3.get_range_if_match("dir/a b.txt", 0, 5, "\"stale\"")
			.await,
		Err(S3Error::PreconditionFailed)
	);
	let replaced = PutOptions { if_match: Some(etag.clone()), ..PutOptions::default() };
	s3.put("dir/a b.txt", Bytes::from_static(b"v2"), &replaced)
		.await
		.unwrap();
	assert_eq!(
		s3.put("dir/a b.txt", Bytes::from_static(b"v3"), &replaced)
			.await,
		Err(S3Error::PreconditionFailed)
	);
	let mut object = s3.get("dir/a b.txt").await.unwrap();
	assert_eq!(object.meta.size, 2);
	assert_eq!(object.body.next().await.unwrap().unwrap(), "v2");
	assert_eq!(s3.head("missing").await, Err(S3Error::NotFound));
	assert!(!s3.exists("missing").await.unwrap());
	s3.delete("dir/a b.txt").await.unwrap();
	s3.delete("dir/a b.txt").await.unwrap();
	let keys = fake.keys();
	assert!(keys.is_empty(), "objects left behind: {keys:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_pages_delimits_and_scopes_to_the_prefix() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "p");
	fake.insert("bucket/other/x", "outside the prefix");
	fake.insert("bucket/p2/x", "sibling prefix");
	for key in ["a/1", "a/2", "b/c/3", "d", "e&f"] {
		s3.put(key, Bytes::from(key.to_owned()), &PutOptions::default())
			.await
			.unwrap();
	}
	let page = s3.list("", Some("/"), None, None).await.unwrap();
	let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
	assert_eq!(keys, ["d", "e&f"]);
	assert_eq!(page.common_prefixes, ["a/", "b/"]);
	assert_eq!(page.next, None);
	assert_eq!(page.objects[1].size, 3);
	assert!(page.objects[0].etag.is_some() && page.objects[0].last_modified.is_some());
	let mut all = Vec::new();
	let mut token = None;
	loop {
		let page = s3.list("", None, token.as_deref(), Some(2)).await.unwrap();
		all.extend(page.objects.into_iter().map(|o| o.key));
		token = page.next;
		if token.is_none() {
			break;
		}
	}
	assert_eq!(all, ["a/1", "a/2", "b/c/3", "d", "e&f"]);
	let scoped = s3.with_prefix("a").unwrap();
	let page = scoped.list("", None, None, None).await.unwrap();
	assert_eq!(
		page
			.objects
			.iter()
			.map(|o| o.key.as_str())
			.collect::<Vec<_>>(),
		["1", "2"]
	);
	assert_eq!(s3.delete_prefix("a/").await.unwrap(), 2);
	assert_eq!(s3.delete_prefix("").await.unwrap(), 3);
	assert_eq!(fake.keys(), ["bucket/other/x", "bucket/p2/x"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streamed_multipart_uploads_complete_or_abort() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "");
	let size = PART_BYTES * 2 + 12_345;
	let data: Bytes = (0..size)
		.map(|i| (i % 251) as u8)
		.collect::<Vec<_>>()
		.into();
	// Odd chunk sizes exercise part boundaries inside chunks.
	let chunks = |data: Bytes| {
		futures::stream::iter(
			(0..data.len())
				.step_by(1_000_003)
				.map(move |at| Ok(data.slice(at..(at + 1_000_003).min(data.len()))))
				.collect::<Vec<_>>(),
		)
	};
	s3.upload_stream("big", size as u64, chunks(data.clone()), &PutOptions::default())
		.await
		.unwrap();
	assert_eq!(fake.objects()["bucket/big"].data, data);
	let parts: Vec<_> = fake
		.requests()
		.into_iter()
		.filter(|r| r.query.contains_key("partNumber"))
		.map(|r| r.len)
		.collect();
	assert_eq!(parts.iter().sum::<usize>(), size);
	assert_eq!(parts.iter().filter(|len| **len == PART_BYTES).count(), 2);
	assert_eq!(fake.open_uploads(), 0);

	// Small bodies are one PUT; length mismatches fail.
	s3.upload_stream("small", 3, chunks(Bytes::from_static(b"abc")), &PutOptions::default())
		.await
		.unwrap();
	assert!(
		s3.upload_stream("short", 4, chunks(Bytes::from_static(b"abc")), &PutOptions::default())
			.await
			.is_err()
	);
	assert!(
		s3.upload_stream("long", 2, chunks(Bytes::from_static(b"abc")), &PutOptions::default())
			.await
			.is_err()
	);

	// A failing part aborts the upload (after retries).
	fake.set_hook(|request| {
		request
			.query
			.contains_key("partNumber")
			.then_some(StatusCode::BAD_REQUEST)
	});
	assert!(
		s3.upload_stream("broken", size as u64, chunks(data.clone()), &PutOptions::default())
			.await
			.is_err()
	);
	fake.clear_hook();
	for _ in 0..100 {
		if fake.aborted_uploads() == 1 {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert_eq!((fake.aborted_uploads(), fake.open_uploads()), (1, 0));
	assert!(!fake.keys().contains(&"bucket/broken".to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failures_retry_and_auth_failures_do_not() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "");
	let remaining = Mutex::new(2u32);
	fake.set_hook(move |_| {
		let mut remaining = remaining.lock();
		let fail = *remaining > 0;
		*remaining = remaining.saturating_sub(1);
		fail.then_some(StatusCode::SERVICE_UNAVAILABLE)
	});
	s3.put("k", Bytes::from_static(b"v"), &PutOptions::default())
		.await
		.unwrap();
	assert_eq!(fake.requests().len(), 3);
	fake.set_hook(|_| Some(StatusCode::FORBIDDEN));
	assert!(matches!(s3.head("k").await, Err(S3Error::Auth(_))));
	fake.clear_hook();
	let wrong = S3Config {
		auth: S3Auth::Explicit(crate::aws::Credentials::permanent("fake-access", "wrong", None)),
		..fake.config("bucket", "")
	};
	assert!(matches!(S3Client::new(&wrong).unwrap().head("k").await, Err(S3Error::Auth(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copies_keep_or_replace_metadata() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "pre");
	let mut options = PutOptions::default();
	options.metadata.insert("owner".into(), "a".into());
	s3.put("src", Bytes::from_static(b"payload"), &options)
		.await
		.unwrap();
	s3.copy("src", "kept", None).await.unwrap();
	assert_eq!(s3.head("kept").await.unwrap().metadata["owner"], "a");
	let replaced = BTreeMap::from([("owner".to_owned(), "b".to_owned())]);
	s3.copy("src", "dir/replaced", Some(&replaced))
		.await
		.unwrap();
	assert_eq!(s3.head("dir/replaced").await.unwrap().metadata["owner"], "b");
	assert_eq!(s3.get_bytes("dir/replaced", 64).await.unwrap(), "payload");
	assert_eq!(s3.copy("missing", "x", None).await, Err(S3Error::NotFound));
	// Multipart copy path (exercised directly: 5 GiB objects are too big here).
	let source = format!("/bucket/{}", s3.location().object_key("src").unwrap());
	s3.copy_multipart(&source, "parts", 7, &PutOptions::default())
		.await
		.unwrap();
	assert_eq!(s3.get_bytes("parts", 64).await.unwrap(), "payload");
}

#[tokio::test]
async fn presigned_urls_need_no_headers() {
	let fake = FakeS3::start().await;
	let s3 = fake.client("bucket", "pre");
	s3.put("a b", Bytes::from_static(b"signed"), &PutOptions::default())
		.await
		.unwrap();
	let url = s3
		.presign_get("a b", Duration::from_secs(60))
		.await
		.unwrap();
	assert!(
		url.as_str().contains("/bucket/pre/a%20b?") && url.as_str().contains("X-Amz-Signature=")
	);
	let anonymous =
		S3Client::new(&S3Config { auth: S3Auth::Anonymous, ..fake.config("bucket", "pre") }).unwrap();
	assert_eq!(
		anonymous
			.presign_get("a b", Duration::from_secs(60))
			.await
			.unwrap()
			.as_str(),
		format!("{}/bucket/pre/a%20b", fake.endpoint())
	);
}

/// Against a real S3-compatible server: `VMON_S3_TEST_URL=http://127.0.0.1:
/// 19000/vmon-dev/<prefix>` with `VMON_S3_ACCESS_KEY`/`VMON_S3_SECRET_KEY`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_server_roundtrip() {
	let Ok(url) = std::env::var("VMON_S3_TEST_URL") else {
		eprintln!("skipped: set VMON_S3_TEST_URL");
		return;
	};
	let config = S3Config {
		location: format!("{}/cloud-test-{}", url.trim_end_matches('/'), std::process::id()),
		..S3Config::from_env(&url)
	};
	let s3 = S3Client::connect(config).await.unwrap();
	let mut options = PutOptions::default();
	options.metadata.insert("mode".into(), "0600".into());
	let etag = s3
		.put("dir/one two", Bytes::from_static(b"hello"), &options)
		.await
		.unwrap();
	let head = s3.head("dir/one two").await.unwrap();
	assert_eq!((head.size, head.etag.as_deref()), (5, Some(etag.as_str())));
	assert_eq!(head.metadata["mode"], "0600");
	assert_eq!(s3.get_range("dir/one two", 1, 3).await.unwrap(), "ell");
	assert_eq!(s3.get_range("dir/one two", 5, 3).await.unwrap(), "");
	let size = PART_BYTES + 777;
	let data: Bytes = vec![7u8; size].into();
	s3.upload_stream(
		"big",
		size as u64,
		futures::stream::iter([Ok(data.slice(..1000)), Ok(data.slice(1000..))]),
		&PutOptions::default(),
	)
	.await
	.unwrap();
	assert_eq!(s3.head("big").await.unwrap().size, size as u64);
	s3.copy("dir/one two", "copy", None).await.unwrap();
	let page = s3.list("", Some("/"), None, None).await.unwrap();
	assert_eq!(page.common_prefixes, ["dir/"]);
	let presigned = s3
		.presign_get("copy", Duration::from_secs(60))
		.await
		.unwrap();
	assert_eq!(reqwest::get(presigned).await.unwrap().text().await.unwrap(), "hello");
	assert_eq!(s3.delete_prefix("").await.unwrap(), 3);
	let objects = s3.list("", None, None, None).await.unwrap().objects;
	assert!(objects.is_empty(), "unexpected objects: {objects:?}");
}
