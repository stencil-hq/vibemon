//! Connect protocol v1 wire format: codecs (JSON / binary proto), the 5-byte
//! stream envelope, end-stream messages, error codes with their HTTP status
//! mapping, and request header parsing (`Connect-Timeout-Ms`,
//! `Keepalive-Ping-Interval`, content encodings). Used by the RPC dispatcher
//! in `server.rs` and the stream handlers.

use std::{io::Read, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::HeaderMap;
use serde::{Serialize, de::DeserializeOwned};

/// Envelope flag: payload is compressed with the negotiated encoding.
pub const FLAG_COMPRESSED: u8 = 0x01;
/// Envelope flag: end-of-stream message (JSON `{error?, metadata?}`).
pub const FLAG_END_STREAM: u8 = 0x02;
/// Largest accepted request message (decoded), guards against memory abuse.
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;
/// Keepalive cadence when the client sends no usable `Keepalive-Ping-Interval`.
pub const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(90);

/// Connect error codes (<https://connectrpc.com/docs/protocol#error-codes>).
#[allow(dead_code, reason = "complete protocol code table; not every code is produced")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
	Canceled,
	Unknown,
	InvalidArgument,
	DeadlineExceeded,
	NotFound,
	AlreadyExists,
	PermissionDenied,
	ResourceExhausted,
	FailedPrecondition,
	Aborted,
	OutOfRange,
	Unimplemented,
	Internal,
	Unavailable,
	DataLoss,
	Unauthenticated,
}

impl Code {
	/// Wire name (`not_found`, ...).
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Canceled => "canceled",
			Self::Unknown => "unknown",
			Self::InvalidArgument => "invalid_argument",
			Self::DeadlineExceeded => "deadline_exceeded",
			Self::NotFound => "not_found",
			Self::AlreadyExists => "already_exists",
			Self::PermissionDenied => "permission_denied",
			Self::ResourceExhausted => "resource_exhausted",
			Self::FailedPrecondition => "failed_precondition",
			Self::Aborted => "aborted",
			Self::OutOfRange => "out_of_range",
			Self::Unimplemented => "unimplemented",
			Self::Internal => "internal",
			Self::Unavailable => "unavailable",
			Self::DataLoss => "data_loss",
			Self::Unauthenticated => "unauthenticated",
		}
	}

	/// HTTP status for unary error responses.
	pub const fn http_status(self) -> u16 {
		match self {
			Self::Canceled => 499,
			Self::Unknown | Self::Internal | Self::DataLoss => 500,
			Self::InvalidArgument | Self::FailedPrecondition | Self::OutOfRange => 400,
			Self::DeadlineExceeded => 504,
			Self::NotFound => 404,
			Self::AlreadyExists | Self::Aborted => 409,
			Self::PermissionDenied => 403,
			Self::ResourceExhausted => 429,
			Self::Unimplemented => 501,
			Self::Unavailable => 503,
			Self::Unauthenticated => 401,
		}
	}
}

/// An RPC failure carried to the client as a Connect error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcError {
	pub code:    Code,
	pub message: String,
}

impl RpcError {
	/// Error with an explicit code.
	pub fn new(code: Code, message: impl Into<String>) -> Self {
		Self { code, message: message.into() }
	}

	/// `invalid_argument`.
	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(Code::InvalidArgument, message)
	}

	/// `not_found`.
	pub fn not_found(message: impl Into<String>) -> Self {
		Self::new(Code::NotFound, message)
	}

	/// `internal`.
	pub fn internal(message: impl Into<String>) -> Self {
		Self::new(Code::Internal, message)
	}

	/// Connect JSON error object `{code, message}`.
	pub fn to_json(&self) -> serde_json::Value {
		serde_json::json!({ "code": self.code.as_str(), "message": self.message })
	}
}

impl std::fmt::Display for RpcError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}: {}", self.code.as_str(), self.message)
	}
}

/// Message codec negotiated from the request `Content-Type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
	Json,
	Proto,
}

/// Whether a request uses the unary or the streaming content types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
	Unary,
	Stream,
}

impl Codec {
	/// Parse a Connect content type (`application/json`,
	/// `application/connect+proto`, parameters ignored) and the framing it
	/// implies. `None` = unsupported media type (HTTP 415).
	pub fn from_content_type(content_type: &str) -> Option<(Self, Framing)> {
		let essence = content_type
			.split(';')
			.next()
			.unwrap_or("")
			.trim()
			.to_ascii_lowercase();
		match essence.as_str() {
			"application/json" => Some((Self::Json, Framing::Unary)),
			"application/proto" => Some((Self::Proto, Framing::Unary)),
			"application/connect+json" => Some((Self::Json, Framing::Stream)),
			"application/connect+proto" => Some((Self::Proto, Framing::Stream)),
			_ => None,
		}
	}

	/// Response content type for this codec and framing.
	pub const fn content_type(self, framing: Framing) -> &'static str {
		match (self, framing) {
			(Self::Json, Framing::Unary) => "application/json",
			(Self::Proto, Framing::Unary) => "application/proto",
			(Self::Json, Framing::Stream) => "application/connect+json",
			(Self::Proto, Framing::Stream) => "application/connect+proto",
		}
	}

	/// Decode one message. JSON tolerates unknown fields; an empty JSON body
	/// is the default message (as connect-go accepts).
	pub fn decode<M>(self, bytes: &[u8]) -> Result<M, RpcError>
	where
		M: prost::Message + Default + DeserializeOwned,
	{
		match self {
			Self::Json if bytes.iter().all(u8::is_ascii_whitespace) => Ok(M::default()),
			Self::Json => serde_json::from_slice(bytes)
				.map_err(|e| RpcError::invalid(format!("unmarshal message: {e}"))),
			Self::Proto => {
				M::decode(bytes).map_err(|e| RpcError::invalid(format!("unmarshal message: {e}")))
			},
		}
	}

	/// Encode one message.
	pub fn encode<M>(self, msg: &M) -> Bytes
	where
		M: prost::Message + Serialize,
	{
		match self {
			// Serializing generated pbjson types into a Vec cannot fail.
			Self::Json => Bytes::from(serde_json::to_vec(msg).unwrap_or_default()),
			Self::Proto => Bytes::from(msg.encode_to_vec()),
		}
	}
}

/// Wrap `payload` in a Connect envelope.
pub fn envelope(flags: u8, payload: &[u8]) -> Bytes {
	let mut out = BytesMut::with_capacity(5 + payload.len());
	out.put_u8(flags);
	out.put_u32(u32::try_from(payload.len()).unwrap_or(u32::MAX));
	out.put_slice(payload);
	out.freeze()
}

/// End-stream envelope: `{}` on success, `{"error":{code,message}}` on failure.
pub fn end_stream(error: Option<&RpcError>) -> Bytes {
	let body =
		error.map_or_else(|| serde_json::json!({}), |e| serde_json::json!({ "error": e.to_json() }));
	envelope(FLAG_END_STREAM, body.to_string().as_bytes())
}

/// Incremental envelope parser for streaming request bodies.
#[derive(Default)]
pub struct EnvelopeReader {
	buf: BytesMut,
}

impl EnvelopeReader {
	/// Append received bytes.
	pub fn push(&mut self, data: &[u8]) {
		self.buf.extend_from_slice(data);
	}

	/// Next complete `(flags, payload)` envelope, if buffered.
	pub fn next(&mut self) -> Result<Option<(u8, Bytes)>, RpcError> {
		if self.buf.len() < 5 {
			return Ok(None);
		}
		let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
		if len > MAX_MESSAGE_BYTES {
			return Err(RpcError::new(
				Code::ResourceExhausted,
				format!("message size {len} exceeds limit {MAX_MESSAGE_BYTES}"),
			));
		}
		if self.buf.len() < 5 + len {
			return Ok(None);
		}
		let flags = self.buf[0];
		self.buf.advance(5);
		Ok(Some((flags, self.buf.split_to(len).freeze())))
	}

	/// True when no partial envelope is pending.
	pub fn is_empty(&self) -> bool {
		self.buf.is_empty()
	}
}

/// Content encodings this server can decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
	Identity,
	Gzip,
}

impl Encoding {
	/// Parse an encoding header value; `None` = unsupported.
	pub fn parse(value: Option<&str>) -> Option<Self> {
		match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
			None | Some("" | "identity") => Some(Self::Identity),
			Some("gzip") => Some(Self::Gzip),
			Some(_) => None,
		}
	}

	/// Decode a whole buffer, bounded by [`MAX_MESSAGE_BYTES`].
	pub fn decode(self, data: Bytes) -> Result<Bytes, RpcError> {
		match self {
			Self::Identity => Ok(data),
			Self::Gzip => {
				let mut out = Vec::new();
				flate2::read::GzDecoder::new(&data[..])
					.take(MAX_MESSAGE_BYTES as u64 + 1)
					.read_to_end(&mut out)
					.map_err(|e| RpcError::invalid(format!("decompress message: {e}")))?;
				if out.len() > MAX_MESSAGE_BYTES {
					return Err(RpcError::new(
						Code::ResourceExhausted,
						"decompressed message too large",
					));
				}
				Ok(Bytes::from(out))
			},
		}
	}
}

/// The client of a server stream went away (response body dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disconnected;

/// Producer side of a server-streaming response: encodes and envelopes
/// messages; the dispatcher appends the end-stream frame.
pub struct StreamSink {
	tx:    tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
	codec: Codec,
}

impl StreamSink {
	/// Wrap the response body channel.
	pub const fn new(
		tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
		codec: Codec,
	) -> Self {
		Self { tx, codec }
	}

	/// Send one message; waits while the client applies backpressure.
	pub async fn send<M>(&self, msg: &M) -> Result<(), Disconnected>
	where
		M: prost::Message + Serialize,
	{
		let frame = envelope(0, &self.codec.encode(msg));
		self.tx.send(Ok(frame)).await.map_err(|_| Disconnected)
	}

	/// Resolves once the client has gone away.
	pub async fn closed(&self) {
		self.tx.closed().await;
	}

	/// Finish the stream with the end-stream frame.
	pub async fn finish(self, error: Option<&RpcError>) {
		let _ = self.tx.send(Ok(end_stream(error))).await;
	}
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
	headers.get(name).and_then(|v| v.to_str().ok())
}

/// `Connect-Timeout-Ms`: absent or `0` = no deadline; otherwise 1–10 ASCII
/// digits (the protocol's limit), else `invalid_argument`.
pub fn parse_timeout(headers: &HeaderMap) -> Result<Option<Duration>, RpcError> {
	let Some(raw) = headers.get("connect-timeout-ms") else {
		return Ok(None);
	};
	let raw = raw.to_str().unwrap_or("").trim();
	if raw.is_empty() || raw.len() > 10 || !raw.bytes().all(|b| b.is_ascii_digit()) {
		return Err(RpcError::invalid(format!("invalid Connect-Timeout-Ms: {raw:?}")));
	}
	let ms: u64 = raw
		.parse()
		.map_err(|_| RpcError::invalid(format!("invalid Connect-Timeout-Ms: {raw:?}")))?;
	Ok((ms > 0).then(|| Duration::from_millis(ms)))
}

/// `Keepalive-Ping-Interval` in seconds; missing, invalid or ≤ 0 → 90 s.
pub fn parse_keepalive(headers: &HeaderMap) -> Duration {
	header(headers, "keepalive-ping-interval")
		.and_then(|v| v.trim().parse::<i64>().ok())
		.filter(|&s| s > 0)
		.map_or(DEFAULT_KEEPALIVE, |s| Duration::from_secs(s.unsigned_abs()))
}

/// Request body encoding: `Content-Encoding` for unary,
/// `Connect-Content-Encoding` for streams.
pub fn request_encoding(headers: &HeaderMap, framing: Framing) -> Result<Encoding, RpcError> {
	let name = match framing {
		Framing::Unary => "content-encoding",
		Framing::Stream => "connect-content-encoding",
	};
	let value = header(headers, name);
	Encoding::parse(value).ok_or_else(|| {
		RpcError::new(
			Code::Unimplemented,
			format!(
				"unknown compression {:?}: supported encodings are gzip, identity",
				value.unwrap_or("")
			),
		)
	})
}

#[cfg(test)]
mod tests {
	use std::io::Write;

	use super::*;
	use crate::envd::pb::{filesystem as fs, process as pr};

	#[test]
	fn code_status_mapping_matches_connect_spec() {
		assert_eq!(Code::NotFound.http_status(), 404);
		assert_eq!(Code::AlreadyExists.http_status(), 409);
		assert_eq!(Code::Unauthenticated.http_status(), 401);
		assert_eq!(Code::FailedPrecondition.http_status(), 400);
		assert_eq!(Code::DeadlineExceeded.http_status(), 504);
		assert_eq!(Code::Canceled.http_status(), 499);
		assert_eq!(Code::ResourceExhausted.http_status(), 429);
		assert_eq!(Code::Unimplemented.http_status(), 501);
		assert_eq!(Code::Unavailable.http_status(), 503);
		assert_eq!(Code::DataLoss.http_status(), 500);
	}

	#[test]
	fn envelope_reader_handles_split_and_coalesced_frames() {
		let mut wire = envelope(0, b"{\"a\":1}").to_vec();
		wire.extend_from_slice(&end_stream(None));
		let mut reader = EnvelopeReader::default();
		// Feed byte by byte: nothing is produced until a frame completes.
		let mut frames = Vec::new();
		for b in &wire {
			reader.push(&[*b]);
			while let Some(frame) = reader.next().unwrap() {
				frames.push(frame);
			}
		}
		assert_eq!(frames.len(), 2);
		assert_eq!(frames[0], (0, Bytes::from_static(b"{\"a\":1}")));
		assert_eq!(frames[1].0, FLAG_END_STREAM);
		assert_eq!(&frames[1].1[..], b"{}");
		assert!(reader.is_empty());
	}

	#[test]
	fn envelope_reader_rejects_oversized_length_before_buffering() {
		let mut reader = EnvelopeReader::default();
		reader.push(&[0, 0xff, 0xff, 0xff, 0xff]);
		assert_eq!(reader.next().unwrap_err().code, Code::ResourceExhausted);
	}

	#[test]
	fn end_stream_carries_error_object() {
		let frame = end_stream(Some(&RpcError::not_found("process 7 not found")));
		assert_eq!(frame[0], FLAG_END_STREAM);
		let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
		assert_eq!(len, frame.len() - 5);
		let v: serde_json::Value = serde_json::from_slice(&frame[5..]).unwrap();
		assert_eq!(
			v,
			serde_json::json!({"error": {"code": "not_found", "message": "process 7 not found"}})
		);
	}

	#[test]
	fn content_types_select_codec_and_framing() {
		assert_eq!(
			Codec::from_content_type("application/json; charset=utf-8"),
			Some((Codec::Json, Framing::Unary))
		);
		assert_eq!(
			Codec::from_content_type("Application/Connect+Proto"),
			Some((Codec::Proto, Framing::Stream))
		);
		assert_eq!(Codec::from_content_type("application/grpc"), None);
		assert_eq!(Codec::from_content_type("text/plain"), None);
	}

	#[test]
	fn timeout_header_parsing() {
		let mut h = HeaderMap::new();
		assert_eq!(parse_timeout(&h).unwrap(), None);
		h.insert("connect-timeout-ms", "0".parse().unwrap());
		assert_eq!(parse_timeout(&h).unwrap(), None);
		h.insert("connect-timeout-ms", "60000".parse().unwrap());
		assert_eq!(parse_timeout(&h).unwrap(), Some(Duration::from_mins(1)));
		h.insert("connect-timeout-ms", "12345678901".parse().unwrap());
		assert_eq!(parse_timeout(&h).unwrap_err().code, Code::InvalidArgument);
		h.insert("connect-timeout-ms", "-5".parse().unwrap());
		assert_eq!(parse_timeout(&h).unwrap_err().code, Code::InvalidArgument);
	}

	#[test]
	fn keepalive_header_defaults() {
		let mut h = HeaderMap::new();
		assert_eq!(parse_keepalive(&h), DEFAULT_KEEPALIVE);
		h.insert("keepalive-ping-interval", "50".parse().unwrap());
		assert_eq!(parse_keepalive(&h), Duration::from_secs(50));
		h.insert("keepalive-ping-interval", "0".parse().unwrap());
		assert_eq!(parse_keepalive(&h), DEFAULT_KEEPALIVE);
		h.insert("keepalive-ping-interval", "abc".parse().unwrap());
		assert_eq!(parse_keepalive(&h), DEFAULT_KEEPALIVE);
	}

	#[test]
	fn gzip_messages_decode_and_unknown_encoding_is_rejected() {
		let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
		enc.write_all(b"{\"path\":\"/x\"}").unwrap();
		let gz = Bytes::from(enc.finish().unwrap());
		assert_eq!(&Encoding::Gzip.decode(gz).unwrap()[..], b"{\"path\":\"/x\"}");
		let mut h = HeaderMap::new();
		h.insert("connect-content-encoding", "br".parse().unwrap());
		assert_eq!(request_encoding(&h, Framing::Stream).unwrap_err().code, Code::Unimplemented);
		// Unary framing reads a different header.
		assert_eq!(request_encoding(&h, Framing::Unary).unwrap(), Encoding::Identity);
	}

	#[test]
	fn json_mapping_is_canonical_proto3() {
		// Unknown fields are tolerated; lowerCamelCase and enum names accepted.
		let req: fs::WatchDirRequest = Codec::Json
			.decode(br#"{"path":"/tmp","recursive":true,"includeEntry":true,"futureField":1}"#)
			.unwrap();
		assert!(req.recursive && req.include_entry);
		let sig: pr::SendSignalRequest = Codec::Json
			.decode(br#"{"process":{"pid":12},"signal":"SIGNAL_SIGKILL"}"#)
			.unwrap();
		assert_eq!(sig.signal, pr::Signal::Sigkill as i32);
		assert_eq!(sig.process.unwrap().selector, Some(pr::process_selector::Selector::Pid(12)));
		// Empty body = default message.
		let list: pr::ListRequest = Codec::Json.decode(b"").unwrap();
		assert_eq!(list, pr::ListRequest::default());

		// bytes → base64, oneof → member name, zero values omitted.
		let ev = pr::StartResponse {
			event: Some(pr::ProcessEvent {
				event: Some(pr::process_event::Event::Data(pr::process_event::DataEvent {
					output: Some(pr::process_event::data_event::Output::Stdout(b"hi\n".to_vec())),
				})),
			}),
		};
		let json: serde_json::Value = serde_json::from_slice(&Codec::Json.encode(&ev)).unwrap();
		assert_eq!(json, serde_json::json!({"event": {"data": {"stdout": "aGkK"}}}));
		let end = pr::ProcessEvent {
			event: Some(pr::process_event::Event::End(pr::process_event::EndEvent {
				exit_code: 0,
				exited:    true,
				status:    "exit status 0".into(),
				error:     None,
			})),
		};
		let json: serde_json::Value = serde_json::from_slice(&Codec::Json.encode(&end)).unwrap();
		assert_eq!(json, serde_json::json!({"end": {"exited": true, "status": "exit status 0"}}));
		let keepalive = pr::ProcessEvent {
			event: Some(pr::process_event::Event::Keepalive(pr::process_event::KeepAlive {})),
		};
		assert_eq!(&Codec::Json.encode(&keepalive)[..], br#"{"keepalive":{}}"#);

		// int64 as string, enum as name, timestamp RFC 3339.
		let entry = fs::EntryInfo {
			name: "a".into(),
			r#type: fs::FileType::File as i32,
			size: 5,
			modified_time: Some(pbjson_types::Timestamp { seconds: 1, nanos: 0 }),
			..Default::default()
		};
		let json: serde_json::Value = serde_json::from_slice(&Codec::Json.encode(&entry)).unwrap();
		assert_eq!(json["size"], "5");
		assert_eq!(json["type"], "FILE_TYPE_FILE");
		assert_eq!(json["modifiedTime"], "1970-01-01T00:00:01+00:00");
	}

	#[test]
	fn proto_codec_round_trips() {
		let msg = pr::SendInputRequest {
			process: Some(pr::ProcessSelector {
				selector: Some(pr::process_selector::Selector::Tag("t".into())),
			}),
			input:   Some(pr::ProcessInput {
				input: Some(pr::process_input::Input::Pty(vec![0, 1, 2])),
			}),
		};
		let back: pr::SendInputRequest = Codec::Proto.decode(&Codec::Proto.encode(&msg)).unwrap();
		assert_eq!(back, msg);
		assert_eq!(
			Codec::Proto
				.decode::<pr::SendInputRequest>(&[0xff, 0xff])
				.unwrap_err()
				.code,
			Code::InvalidArgument
		);
	}
}
