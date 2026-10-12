//! HTTP plumbing shared by the envd handlers: the response body type
//! (buffered or channel-streamed), envd's JSON error responses, CORS, and
//! request body helpers.

use std::{
	pin::Pin,
	task::{Context, Poll},
};

use bytes::Bytes;
use futures::Stream;
use http::{HeaderValue, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::sync::mpsc;

/// Response body for every envd route.
pub type RespBody = BoxBody<Bytes, std::io::Error>;
/// Response type for every envd route.
pub type Resp = Response<RespBody>;

/// Buffered body.
pub fn full(data: impl Into<Bytes>) -> RespBody {
	Full::new(data.into())
		.map_err(|never| match never {})
		.boxed()
}

/// Empty body.
pub fn empty() -> RespBody {
	full(Bytes::new())
}

/// Body fed by a channel; dropping the response (client gone) closes the
/// channel so producers observe `Sender::closed`/send failures.
pub struct ChannelBody {
	rx: mpsc::Receiver<Result<Bytes, std::io::Error>>,
}

impl Body for ChannelBody {
	type Data = Bytes;
	type Error = std::io::Error;

	fn poll_frame(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
		self
			.rx
			.poll_recv(cx)
			.map(|item| item.map(|r| r.map(Frame::data)))
	}

	fn size_hint(&self) -> SizeHint {
		SizeHint::default()
	}
}

/// A streaming body plus its producer handle (bounded to `depth` chunks).
pub fn channel(depth: usize) -> (mpsc::Sender<Result<Bytes, std::io::Error>>, RespBody) {
	let (tx, rx) = mpsc::channel(depth);
	(tx, ChannelBody { rx }.boxed())
}

/// Plain response with a status and body.
pub fn response(status: StatusCode, body: RespBody) -> Resp {
	let mut resp = Response::new(body);
	*resp.status_mut() = status;
	resp
}

/// `204 No Content`.
pub fn no_content() -> Resp {
	response(StatusCode::NO_CONTENT, empty())
}

/// `200` JSON response.
pub fn json(status: StatusCode, value: &impl serde::Serialize) -> Resp {
	let body = serde_json::to_vec(value).unwrap_or_default();
	let mut resp = response(status, full(body));
	resp
		.headers_mut()
		.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
	resp
}

/// envd HTTP error: `{"code": <status>, "message": ...}` with
/// `application/json; charset=utf-8` and `X-Content-Type-Options: nosniff`.
pub fn error(status: StatusCode, message: impl Into<String>) -> Resp {
	let body = serde_json::json!({ "code": status.as_u16(), "message": message.into() });
	let mut resp = response(status, full(body.to_string()));
	let h = resp.headers_mut();
	h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
	h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
	resp
}

/// Plain-text error (envd's `/init` style).
pub fn text_error(status: StatusCode, message: impl Into<String>) -> Resp {
	let mut resp = response(status, full(message.into()));
	resp
		.headers_mut()
		.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
	resp
}

/// Headers exposed to browsers (connect-cors defaults + envd's extras).
const EXPOSED_HEADERS: &str = "Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin, \
                               Content-Encoding, Connect-Content-Encoding, Location, \
                               Cache-Control, X-Content-Type-Options";

/// Answer a CORS preflight (envd: any origin, any header, 2 h max-age).
pub fn preflight(request_headers: Option<&HeaderValue>) -> Resp {
	let mut resp = no_content();
	let h = resp.headers_mut();
	h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
	h.insert(
		header::ACCESS_CONTROL_ALLOW_METHODS,
		HeaderValue::from_static("HEAD, GET, POST, PUT, PATCH, DELETE"),
	);
	h.insert(
		header::ACCESS_CONTROL_ALLOW_HEADERS,
		request_headers
			.cloned()
			.unwrap_or_else(|| HeaderValue::from_static("*")),
	);
	h.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("7200"));
	resp
}

/// Add CORS response headers to an actual response.
pub fn add_cors(resp: &mut Resp) {
	let h = resp.headers_mut();
	h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
	h.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static(EXPOSED_HEADERS));
}

/// Read a whole request body up to `limit` bytes.
pub async fn read_body(body: Incoming, limit: usize) -> Result<Bytes, BodyError> {
	let limited = http_body_util::Limited::new(body, limit);
	match limited.collect().await {
		Ok(collected) => Ok(collected.to_bytes()),
		Err(e) if e.is::<http_body_util::LengthLimitError>() => Err(BodyError::TooLarge),
		Err(e) => Err(BodyError::Io(e.to_string())),
	}
}

/// Why reading a request body failed.
#[derive(Debug)]
pub enum BodyError {
	TooLarge,
	Io(String),
}

impl std::fmt::Display for BodyError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::TooLarge => f.write_str("request body too large"),
			Self::Io(e) => write!(f, "read request body: {e}"),
		}
	}
}

/// The request body as a stream of data chunks (trailers skipped).
pub fn body_stream(body: Incoming) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
	futures::stream::unfold(body, |mut body| async move {
		loop {
			match body.frame().await? {
				Ok(frame) => {
					if let Ok(data) = frame.into_data() {
						return Some((Ok(data), body));
					}
				},
				Err(e) => return Some((Err(std::io::Error::other(e)), body)),
			}
		}
	})
}
