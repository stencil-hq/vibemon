//! gRPC-over-WebSocket bridge (`GET /grpc`, proto/vmon/v1/bridge.proto).
//!
//! One RPC per socket. Every WS binary message in either direction is exactly
//! one encoded `BridgeFrame`; text frames are protocol errors. The client
//! sends `call` → `message`* → `half_close`; the server replies `message`* →
//! `end{status, message, trailers}` (including `vmon-code`) and closes.
//!
//! `message` payloads are single encoded gRPC messages WITHOUT the 5-byte
//! gRPC wire prefix — the bridge adds/strips it when relaying onto the
//! in-process tonic services. Early socket close drops the request body and
//! the response stream, so tonic cancellation fires exactly like a native
//! client disconnect (e.g. the exec `kill(15)` path).

use std::{convert::Infallible, pin::Pin};

use axum::{
	body::Body as AxumBody,
	extract::ws::{Message, WebSocket},
	http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri, header},
};
use futures_util::{
	SinkExt, StreamExt,
	stream::{SplitSink, SplitStream},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
	Code, Status,
	codegen::{Body as _, Service},
	metadata::KeyAndValueRef,
	service::Routes,
};
use vmon_proto::{
	prost::{
		Message as _,
		bytes::{BufMut, Bytes, BytesMut},
	},
	v1 as pb,
};

use super::grpc::MAX_MESSAGE_SIZE;
use crate::security::Principal;

/// Request headers a bridge client must not override.
const RESTRICTED_METADATA: &[&str] =
	&["content-type", "te", "host", "connection", "content-length", "transfer-encoding", "upgrade"];

type BodyChunk = Result<Bytes, Infallible>;

pub async fn serve_bridge(routes: Routes, principal: Principal, socket: WebSocket) {
	let (sender, receiver) = socket.split();
	let (out_tx, out_rx) = mpsc::channel::<pb::BridgeFrame>(16);
	let writer = write_frames(sender, out_rx);
	let session = run_session(routes, principal, receiver, out_tx);
	tokio::join!(writer, session);
}

async fn run_session(
	mut routes: Routes,
	principal: Principal,
	mut receiver: SplitStream<WebSocket>,
	out_tx: mpsc::Sender<pb::BridgeFrame>,
) {
	let call = loop {
		match receiver.next().await {
			Some(Ok(Message::Binary(bytes))) => match decode_frame(&bytes) {
				Some(pb::bridge_frame::Frame::Call(call)) => break call,
				_ => return send_frame(&out_tx, protocol_end("expected call frame")).await,
			},
			Some(Ok(Message::Text(_))) => {
				return send_frame(&out_tx, protocol_end("expected binary bridge frame")).await;
			},
			Some(Ok(Message::Ping(_) | Message::Pong(_))) => {},
			Some(Ok(Message::Close(_)) | Err(_)) | None => return,
		}
	};
	if !principal.is_admin() && super::state::is_admin_path(&call.method) {
		return send_frame(&out_tx, forbidden_end()).await;
	}
	let (mut request, body_tx) = match build_request(call) {
		Ok(parts) => parts,
		Err(end) => return send_frame(&out_tx, end).await,
	};
	super::state::set_principal_headers(request.headers_mut(), &principal);
	request.extensions_mut().insert(principal);
	// `Routes` is infallibly ready (`Error = Infallible`).
	let _ =
		std::future::poll_fn(|cx| Service::<Request<AxumBody>>::poll_ready(&mut routes, cx)).await;
	let response = Service::<Request<AxumBody>>::call(&mut routes, request);
	let inbound = pump_inbound(receiver, body_tx, out_tx.clone());
	let outbound = pump_outbound(response, out_tx);
	// `inbound` only returns on socket close or a protocol error; winning the
	// select drops the response future and request body, cancelling the RPC.
	tokio::select! {
		() = outbound => {},
		() = inbound => {},
	}
}

/// Build the in-process gRPC request from the opening `BridgeCall`.
fn build_request(
	call: pb::BridgeCall,
) -> Result<(Request<AxumBody>, mpsc::Sender<BodyChunk>), pb::BridgeFrame> {
	if !call.method.starts_with('/') {
		return Err(protocol_end("invalid method path"));
	}
	let uri: Uri = call
		.method
		.parse()
		.map_err(|_| protocol_end("invalid method path"))?;
	let (body_tx, body_rx) = mpsc::channel::<BodyChunk>(16);
	let mut builder = Request::builder()
		.method(Method::POST)
		.uri(uri)
		.header(header::CONTENT_TYPE, "application/grpc")
		.header("te", "trailers");
	for (key, value) in &call.metadata {
		let lower = key.to_ascii_lowercase();
		// ASCII metadata only; binary (`-bin`) keys and transport headers are
		// not bridgeable.
		if RESTRICTED_METADATA.contains(&lower.as_str()) || lower.ends_with("-bin") {
			continue;
		}
		let (Ok(name), Ok(header_value)) =
			(HeaderName::try_from(lower), HeaderValue::try_from(value.as_str()))
		else {
			continue;
		};
		builder = builder.header(name, header_value);
	}
	let request = builder
		.body(AxumBody::from_stream(ReceiverStream::new(body_rx)))
		.map_err(|_| protocol_end("invalid call metadata"))?;
	Ok((request, body_tx))
}

/// Client → server: message frames become gRPC-framed body chunks;
/// `half_close` ends the request body. Returns only on socket close/error or
/// a protocol violation, so it doubles as the cancellation watchdog after
/// `half_close`.
async fn pump_inbound(
	mut receiver: SplitStream<WebSocket>,
	body_tx: mpsc::Sender<BodyChunk>,
	out_tx: mpsc::Sender<pb::BridgeFrame>,
) {
	let mut body_tx = Some(body_tx);
	while let Some(message) = receiver.next().await {
		let bytes = match message {
			Ok(Message::Binary(bytes)) => bytes,
			Ok(Message::Text(_)) => {
				return send_frame(&out_tx, protocol_end("expected binary bridge frame")).await;
			},
			Ok(Message::Ping(_) | Message::Pong(_)) => continue,
			Ok(Message::Close(_)) | Err(_) => return,
		};
		match decode_frame(&bytes) {
			Some(pb::bridge_frame::Frame::Message(payload)) => {
				if payload.len() > MAX_MESSAGE_SIZE {
					return send_frame(&out_tx, too_large_end()).await;
				}
				if let Some(tx) = &body_tx
					&& tx.send(Ok(grpc_frame(&payload))).await.is_err()
				{
					// The RPC already produced a response; drain quietly.
					body_tx = None;
				}
			},
			Some(pb::bridge_frame::Frame::HalfClose(_)) => {
				body_tx = None;
			},
			_ => return send_frame(&out_tx, protocol_end("unexpected bridge frame")).await,
		}
	}
}

/// Server → client: de-frame the gRPC response body into one bridge `message`
/// per gRPC message, then mirror status + trailers as the terminal `end`.
async fn pump_outbound<F>(response: F, out_tx: mpsc::Sender<pb::BridgeFrame>)
where
	F: Future<Output = Result<axum::http::Response<tonic::body::Body>, Infallible>>,
{
	let response = match response.await {
		Ok(response) => response,
		Err(never) => match never {},
	};
	let (parts, mut body) = response.into_parts();
	// Trailers-only responses (immediate errors) carry the status in headers.
	if parts.headers.contains_key("grpc-status") {
		return send_frame(&out_tx, end_from_headers(&parts.headers)).await;
	}
	let mut buffer = BytesMut::new();
	loop {
		let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
		match frame {
			Some(Ok(frame)) => {
				if frame.is_data() {
					let Ok(data) = frame.into_data() else {
						continue;
					};
					buffer.extend_from_slice(&data);
					match drain_messages(&mut buffer) {
						Ok(messages) => {
							for message in messages {
								if out_tx.send(message_frame(message)).await.is_err() {
									return;
								}
							}
						},
						Err(end) => return send_frame(&out_tx, end).await,
					}
				} else if let Ok(trailers) = frame.into_trailers() {
					return send_frame(&out_tx, end_from_headers(&trailers)).await;
				}
			},
			Some(Err(status)) => return send_frame(&out_tx, end_from_status(&status)).await,
			None => {
				return send_frame(&out_tx, end_of(Code::Internal, "missing grpc trailers", &[])).await;
			},
		}
	}
}

async fn write_frames(
	mut sender: SplitSink<WebSocket, Message>,
	mut out_rx: mpsc::Receiver<pb::BridgeFrame>,
) {
	while let Some(frame) = out_rx.recv().await {
		let is_end = matches!(frame.frame, Some(pb::bridge_frame::Frame::End(_)));
		if sender
			.send(Message::Binary(frame.encode_to_vec().into()))
			.await
			.is_err()
		{
			return;
		}
		if is_end {
			break;
		}
	}
	let _ = sender.close().await;
}

async fn send_frame(out_tx: &mpsc::Sender<pb::BridgeFrame>, frame: pb::BridgeFrame) {
	let _ = out_tx.send(frame).await;
}

fn decode_frame(bytes: &[u8]) -> Option<pb::bridge_frame::Frame> {
	pb::BridgeFrame::decode(bytes)
		.ok()
		.and_then(|frame| frame.frame)
}

/// Wrap one bridge message payload in the 5-byte gRPC wire prefix
/// (uncompressed flag + big-endian length).
fn grpc_frame(payload: &[u8]) -> Bytes {
	let mut chunk = BytesMut::with_capacity(payload.len() + 5);
	chunk.put_u8(0);
	chunk.put_u32(payload.len() as u32);
	chunk.extend_from_slice(payload);
	chunk.freeze()
}

/// Strip complete gRPC messages off the response buffer.
fn drain_messages(buffer: &mut BytesMut) -> Result<Vec<Vec<u8>>, pb::BridgeFrame> {
	let mut messages = Vec::new();
	loop {
		if buffer.len() < 5 {
			return Ok(messages);
		}
		if buffer[0] != 0 {
			return Err(end_of(Code::Internal, "compressed grpc message unsupported", &[]));
		}
		let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
		if length > MAX_MESSAGE_SIZE {
			return Err(too_large_end());
		}
		if buffer.len() < 5 + length {
			return Ok(messages);
		}
		let _ = buffer.split_to(5);
		messages.push(buffer.split_to(length).to_vec());
	}
}

const fn message_frame(payload: Vec<u8>) -> pb::BridgeFrame {
	pb::BridgeFrame { frame: Some(pb::bridge_frame::Frame::Message(payload)) }
}

const fn end_frame(end: pb::BridgeEnd) -> pb::BridgeFrame {
	pb::BridgeFrame { frame: Some(pb::bridge_frame::Frame::End(end)) }
}

fn end_of(code: Code, message: &str, trailers: &[(&str, &str)]) -> pb::BridgeFrame {
	end_frame(pb::BridgeEnd {
		status:   code as i32,
		message:  message.to_owned(),
		trailers: trailers
			.iter()
			.map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
			.collect(),
	})
}

fn protocol_end(message: &str) -> pb::BridgeFrame {
	end_of(Code::InvalidArgument, message, &[("vmon-code", "invalid")])
}

fn forbidden_end() -> pb::BridgeFrame {
	end_of(Code::PermissionDenied, "forbidden", &[("vmon-code", "unauthorized")])
}

fn too_large_end() -> pb::BridgeFrame {
	end_of(Code::ResourceExhausted, "message exceeds the 64 MiB limit", &[])
}

fn end_from_headers(headers: &HeaderMap) -> pb::BridgeFrame {
	Status::from_header_map(headers).map_or_else(
		|| end_of(Code::Internal, "missing grpc-status", &[]),
		|status| end_from_status(&status),
	)
}

fn end_from_status(status: &Status) -> pb::BridgeFrame {
	let trailers = status
		.metadata()
		.iter()
		.filter_map(|entry| match entry {
			KeyAndValueRef::Ascii(key, value) => {
				// Standard response headers leak into trailers-only statuses.
				if key.as_str() == "content-type" {
					None
				} else {
					Some((key.as_str().to_owned(), value.to_str().ok()?.to_owned()))
				}
			},
			KeyAndValueRef::Binary(..) => None,
		})
		.collect();
	end_frame(pb::BridgeEnd {
		status: status.code() as i32,
		message: status.message().to_owned(),
		trailers,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bridge_frames_round_trip() {
		let call = pb::BridgeFrame {
			frame: Some(pb::bridge_frame::Frame::Call(pb::BridgeCall {
				method:   "/vmon.v1.SystemService/Info".to_owned(),
				metadata: std::iter::once(("authorization".to_owned(), "Bearer tok".to_owned()))
					.collect(),
			})),
		};
		let decoded = pb::BridgeFrame::decode(call.encode_to_vec().as_slice()).unwrap();
		match decoded.frame {
			Some(pb::bridge_frame::Frame::Call(decoded)) => {
				assert_eq!(decoded.method, "/vmon.v1.SystemService/Info");
				assert_eq!(
					decoded.metadata.get("authorization").map(String::as_str),
					Some("Bearer tok")
				);
			},
			other => panic!("unexpected frame: {other:?}"),
		}

		let message = message_frame(b"payload".to_vec());
		match decode_frame(&message.encode_to_vec()) {
			Some(pb::bridge_frame::Frame::Message(payload)) => assert_eq!(payload, b"payload"),
			other => panic!("unexpected frame: {other:?}"),
		}

		let end = end_of(Code::NotFound, "gone", &[("vmon-code", "not_found")]);
		match decode_frame(&end.encode_to_vec()) {
			Some(pb::bridge_frame::Frame::End(end)) => {
				assert_eq!(end.status, Code::NotFound as i32);
				assert_eq!(end.message, "gone");
				assert_eq!(end.trailers.get("vmon-code").map(String::as_str), Some("not_found"));
			},
			other => panic!("unexpected frame: {other:?}"),
		}
	}

	#[test]
	fn grpc_frame_prefix_and_drain_are_inverse() {
		let mut buffer = BytesMut::new();
		buffer.extend_from_slice(&grpc_frame(b"one"));
		buffer.extend_from_slice(&grpc_frame(b"two"));
		// Split mid-message to prove partial buffers are kept.
		let mut partial = buffer.clone();
		let tail = partial.split_off(partial.len() - 2);
		let first = drain_messages(&mut partial).unwrap();
		assert_eq!(first, vec![b"one".to_vec()]);
		partial.extend_from_slice(&tail);
		let rest = drain_messages(&mut partial).unwrap();
		assert_eq!(rest, vec![b"two".to_vec()]);
		assert!(partial.is_empty());
	}

	#[test]
	fn drain_rejects_compressed_and_oversized_messages() {
		let mut compressed = BytesMut::from(&[1_u8, 0, 0, 0, 0][..]);
		assert!(drain_messages(&mut compressed).is_err());

		let mut oversized = BytesMut::new();
		oversized.put_u8(0);
		oversized.put_u32(u32::MAX);
		assert!(drain_messages(&mut oversized).is_err());
	}

	#[test]
	fn end_from_status_carries_vmon_code() {
		let status = crate::api::grpc::status_from(&crate::api::ApiError::invalid("bad"));
		match end_from_status(&status).frame {
			Some(pb::bridge_frame::Frame::End(end)) => {
				assert_eq!(end.status, Code::InvalidArgument as i32);
				assert_eq!(end.message, "bad");
				assert_eq!(end.trailers.get("vmon-code").map(String::as_str), Some("invalid"));
			},
			other => panic!("unexpected frame: {other:?}"),
		}
	}
}
