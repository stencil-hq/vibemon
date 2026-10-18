//! Redis protocol (RESP2) front end for the cluster KV, so `redis-cli`,
//! `redis-benchmark`, and Redis client libraries can talk to it directly
//! (ARCHITECTURE.md §5.3).
//!
//! Commands: `AUTH [default] <cluster secret>` (required before anything but
//! `QUIT`), `PING [message]`, `QUIT`, `GET key`,
//! `SET key value [NX|XX] [EX seconds|PX milliseconds]`, `DEL key…`,
//! `EXISTS key…`. Keys are the KV's own (UTF-8) keys. `GET`/`EXISTS` are
//! linearizable reads ([`Kv::get_consistent`]); `SET` is one transaction and
//! `DEL` one per key. Consecutive pipelined writes are committed together by
//! one [`Kv::txns`] call (one raft entry, in order); any other command first
//! waits for them, so replies keep their order and a pipeline reads its own
//! writes.
//!
//! Requests come in multibulk or inline form. Before `AUTH` they are capped
//! like Redis caps unauthenticated clients (10 arguments, 16 KiB bulk
//! strings); a protocol error is answered and closes the connection.

use std::{fmt::Display, future::Future, io, ops::Range, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	task::JoinSet,
};

use super::{Compare, Kv, Op, Txn};

/// Request caps for authenticated clients. Values travel hex-encoded inside
/// raft entries, so bulk strings stay far below the raft body limit.
const AUTHED: Limits = Limits { args: 1 << 20, bulk: 16 << 20 };
/// Request caps before `AUTH` (Redis' unauthenticated limits).
const ANON: Limits = Limits { args: 10, bulk: 16 << 10 };
/// Longest inline command or header line.
const MAX_LINE: usize = 64 << 10;
/// Unparsed input one connection may buffer.
const MAX_QUERY: usize = 64 << 20;
/// Spare input capacity reserved before each read.
const READ_CHUNK: usize = 16 << 10;
/// Reply bytes buffered before they are written mid-pipeline.
const FLUSH_AT: usize = 64 << 10;
/// Pipelined writes committed together are cut at this many txns or key and
/// value bytes, which bounds the size of one raft entry.
const GROUP_TXNS: usize = 1024;
const GROUP_BYTES: usize = 4 << 20;
/// Pause after a failed `accept` (e.g. out of file descriptors).
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Serves the RESP front end on `listener` until `shutdown` resolves; open
/// connections are dropped then. Clients authenticate with `secret`; failed
/// accepts are logged and retried.
pub async fn serve_resp(
	listener: TcpListener,
	kv: Kv,
	secret: Arc<[u8]>,
	shutdown: impl Future<Output = ()>,
) {
	let mut sessions = JoinSet::new();
	tokio::pin!(shutdown);
	loop {
		tokio::select! {
			() = &mut shutdown => return,
			accepted = listener.accept() => match accepted {
				Ok((stream, peer)) => {
					let session = Session { kv: kv.clone(), secret: secret.clone(), authed: false };
					sessions.spawn(async move {
						if let Err(error) = session.run(stream).await {
							tracing::debug!(%peer, %error, "kv resp connection failed");
						}
					});
				},
				Err(error) => {
					tracing::warn!(%error, "kv resp accept failed");
					tokio::time::sleep(ACCEPT_BACKOFF).await;
				},
			},
			Some(_) = sessions.join_next(), if !sessions.is_empty() => {},
		}
	}
}

/// Per-connection request caps.
#[derive(Clone, Copy)]
struct Limits {
	args: usize,
	bulk: usize,
}

/// Malformed request; answered with `-ERR Protocol error: …`, then the
/// connection closes.
#[derive(Debug, PartialEq, Eq)]
struct ProtocolError(&'static str);

/// A complete command found at the start of the input buffer.
struct Frame {
	/// Argument byte ranges within the frame.
	args: Vec<Range<usize>>,
	/// Bytes the command occupies.
	len:  usize,
}

/// Splits the next complete command off `buf`; `Ok(None)` while it is
/// incomplete. Blank inline lines and `*0` yield an empty command.
fn parse(buf: &mut BytesMut, limits: Limits) -> Result<Option<Vec<Bytes>>, ProtocolError> {
	let parsed = if buf.first() == Some(&b'*') {
		multibulk(buf, limits)?
	} else {
		inline(buf)?
	};
	let Some(Frame { args, len }) = parsed else {
		return Ok(None);
	};
	let frame = buf.split_to(len).freeze();
	Ok(Some(args.into_iter().map(|arg| frame.slice(arg)).collect()))
}

/// The `*<n>\r\n($<len>\r\n<bytes>\r\n)*` command at the start of `buf`.
fn multibulk(buf: &[u8], limits: Limits) -> Result<Option<Frame>, ProtocolError> {
	let Some((count, mut at)) = line(buf, 1)? else {
		return Ok(None);
	};
	let count = integer(&buf[count]).ok_or(ProtocolError("invalid multibulk length"))?;
	// A negative count is a null array: an empty command.
	let count = usize::try_from(count).unwrap_or(0);
	if count > limits.args {
		return Err(ProtocolError("invalid multibulk length"));
	}
	let mut args = Vec::with_capacity(count.min(16));
	for _ in 0..count {
		match buf.get(at) {
			None => return Ok(None),
			Some(b'$') => {},
			Some(_) => return Err(ProtocolError("expected '$'")),
		}
		let Some((len, start)) = line(buf, at + 1)? else {
			return Ok(None);
		};
		let len = integer(&buf[len])
			.and_then(|n| usize::try_from(n).ok())
			.filter(|&n| n <= limits.bulk)
			.ok_or(ProtocolError("invalid bulk length"))?;
		let end = start + len;
		match buf.get(end..end + 2) {
			None => return Ok(None),
			Some(b"\r\n") => {},
			Some(_) => return Err(ProtocolError("expected CRLF after bulk string")),
		}
		args.push(start..end);
		at = end + 2;
	}
	Ok(Some(Frame { args, len: at }))
}

/// The inline command (one line of whitespace-separated words) at the start
/// of `buf`.
fn inline(buf: &[u8]) -> Result<Option<Frame>, ProtocolError> {
	let Some((text, len)) = line(buf, 0)? else {
		return Ok(None);
	};
	let mut args = Vec::new();
	let mut at = text.start;
	for word in buf[text].split(u8::is_ascii_whitespace) {
		if !word.is_empty() {
			args.push(at..at + word.len());
		}
		at += word.len() + 1;
	}
	Ok(Some(Frame { args, len }))
}

/// The line starting at `at` (without its `\r\n` or `\n`) and the offset
/// after it.
fn line(buf: &[u8], at: usize) -> Result<Option<(Range<usize>, usize)>, ProtocolError> {
	let rest = buf.get(at..).unwrap_or_default();
	let Some(newline) = rest.iter().position(|&b| b == b'\n') else {
		return if rest.len() > MAX_LINE {
			Err(ProtocolError("too big inline request"))
		} else {
			Ok(None)
		};
	};
	if newline > MAX_LINE {
		return Err(ProtocolError("too big inline request"));
	}
	let end = if newline > 0 && rest[newline - 1] == b'\r' {
		newline - 1
	} else {
		newline
	};
	Ok(Some((at..at + end, at + newline + 1)))
}

fn integer(bytes: &[u8]) -> Option<i64> {
	std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Commands this front end implements.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Command {
	Auth,
	Ping,
	Quit,
	Get,
	Set,
	Del,
	Exists,
}

impl Command {
	const ALL: [(&str, Self); 7] = [
		("auth", Self::Auth),
		("ping", Self::Ping),
		("quit", Self::Quit),
		("get", Self::Get),
		("set", Self::Set),
		("del", Self::Del),
		("exists", Self::Exists),
	];

	fn lookup(name: &[u8]) -> Option<(&'static str, Self)> {
		Self::ALL
			.into_iter()
			.find(|(known, _)| name.eq_ignore_ascii_case(known.as_bytes()))
	}
}

/// Error reply line, without the leading `-` (e.g. `ERR syntax error`).
struct ErrorReply(String);

impl ErrorReply {
	fn err(message: impl Display) -> Self {
		Self(format!("ERR {message}"))
	}

	fn arity(name: &str) -> Self {
		Self::err(format_args!("wrong number of arguments for '{name}' command"))
	}
}

/// Whether the connection stays open after a command.
#[derive(PartialEq, Eq)]
enum Flow {
	Continue,
	Close,
}

/// Consecutive pipelined writes (`SET`, `DEL`), committed together by one
/// [`Kv::txns`] call, and the reply each command is owed.
#[derive(Default)]
struct Writes {
	txns:    Vec<Txn>,
	replies: Vec<Owed>,
	/// Key and value bytes of `txns`.
	bytes:   usize,
}

impl Writes {
	fn push(&mut self, txn: Txn) {
		self.bytes += txn.payload_len();
		self.txns.push(txn);
	}

	/// Whether the group reached [`GROUP_TXNS`] or [`GROUP_BYTES`].
	const fn is_full(&self) -> bool {
		self.txns.len() >= GROUP_TXNS || self.bytes >= GROUP_BYTES
	}
}

/// Reply owed to a queued write command.
enum Owed {
	/// `SET`: `+OK` when its txn succeeded, else nil.
	Set,
	/// `DEL` of this many keys: how many of them existed.
	Del(usize),
	/// Rejected before reaching the KV.
	Error(ErrorReply),
}

/// One client connection.
struct Session {
	kv:     Kv,
	secret: Arc<[u8]>,
	authed: bool,
}

impl Session {
	async fn run(mut self, mut stream: TcpStream) -> io::Result<()> {
		stream.set_nodelay(true)?;
		let mut input = BytesMut::new();
		let mut out = Vec::new();
		let mut writes = Writes::default();
		loop {
			input.reserve(READ_CHUNK);
			if stream.read_buf(&mut input).await? == 0 {
				return Ok(());
			}
			loop {
				let limits = if self.authed { AUTHED } else { ANON };
				let error = match parse(&mut input, limits) {
					Ok(Some(args)) if args.is_empty() => continue,
					Ok(Some(args)) => {
						if self.execute(&args, &mut writes, &mut out).await == Flow::Close {
							return stream.write_all(&out).await;
						}
						if writes.is_full() {
							self.commit(&mut writes, &mut out).await;
						}
						if out.len() >= FLUSH_AT {
							stream.write_all(&out).await?;
							out.clear();
						}
						continue;
					},
					Ok(None) if input.len() > MAX_QUERY => "query buffer limit exceeded",
					Ok(None) => break,
					Err(ProtocolError(message)) => message,
				};
				self.commit(&mut writes, &mut out).await;
				return close(&mut stream, out, error).await;
			}
			self.commit(&mut writes, &mut out).await;
			if !out.is_empty() {
				stream.write_all(&out).await?;
				out.clear();
			}
		}
	}

	/// Runs one command and appends its reply to `out`; writes join `writes`
	/// instead, which any other command commits first so replies stay in order
	/// and reads see earlier writes.
	async fn execute(&mut self, args: &[Bytes], writes: &mut Writes, out: &mut Vec<u8>) -> Flow {
		let rest = &args[1..];
		let command = Command::lookup(&args[0]);
		if self.authed
			&& let Some((_, write @ (Command::Set | Command::Del))) = command
		{
			let owed = if write == Command::Set {
				set_txn(rest).map(|txn| {
					writes.push(txn);
					Owed::Set
				})
			} else {
				del_txns(rest).map(|txns| {
					let keys = txns.len();
					for txn in txns {
						writes.push(txn);
					}
					Owed::Del(keys)
				})
			};
			writes.replies.push(owed.unwrap_or_else(Owed::Error));
			return Flow::Continue;
		}
		self.commit(writes, out).await;
		let Some((name, command)) = command else {
			let name = args[0].escape_ascii().to_string();
			let name: String = name.chars().take(128).collect();
			write_error(out, &format!("ERR unknown command '{name}'"));
			return Flow::Continue;
		};
		if !self.authed && !matches!(command, Command::Auth | Command::Quit) {
			write_error(out, "NOAUTH Authentication required.");
			return Flow::Continue;
		}
		let result = match command {
			Command::Auth => self.auth(rest, out),
			Command::Ping => match rest {
				[] => {
					out.extend_from_slice(b"+PONG\r\n");
					Ok(())
				},
				[message] => {
					write_bulk(out, Some(message.as_ref()));
					Ok(())
				},
				_ => Err(ErrorReply::arity(name)),
			},
			Command::Quit => {
				out.extend_from_slice(b"+OK\r\n");
				return Flow::Close;
			},
			Command::Get => self.get(rest, out).await,
			Command::Exists => self.exists(rest, out).await,
			// Authenticated writes were queued above.
			Command::Set | Command::Del => unreachable!("writes are queued"),
		};
		if let Err(ErrorReply(line)) = result {
			write_error(out, &line);
		}
		Flow::Continue
	}

	fn auth(&mut self, args: &[Bytes], out: &mut Vec<u8>) -> Result<(), ErrorReply> {
		let password = match args {
			[password] => password,
			[user, password] if &user[..] == b"default" => password,
			[_, _] => return Err(wrong_password()),
			_ => return Err(ErrorReply::arity("auth")),
		};
		if !crate::constant_time_eq(password, &self.secret) {
			return Err(wrong_password());
		}
		self.authed = true;
		out.extend_from_slice(b"+OK\r\n");
		Ok(())
	}

	async fn get(&self, args: &[Bytes], out: &mut Vec<u8>) -> Result<(), ErrorReply> {
		let [key] = args else {
			return Err(ErrorReply::arity("get"));
		};
		let entry = self
			.kv
			.get_consistent(utf8_key(key)?)
			.await
			.map_err(ErrorReply::err)?;
		write_bulk(out, entry.as_ref().map(|entry| entry.value.as_slice()));
		Ok(())
	}

	/// Commits the queued writes as one [`Kv::txns`] call and appends their
	/// replies to `out`.
	async fn commit(&self, writes: &mut Writes, out: &mut Vec<u8>) {
		if writes.replies.is_empty() {
			return;
		}
		writes.bytes = 0;
		let outcome = self.kv.txns(std::mem::take(&mut writes.txns)).await;
		let mut results = outcome.as_ref().map(|results| results.iter());
		for owed in writes.replies.drain(..) {
			match (owed, &mut results) {
				(Owed::Error(ErrorReply(line)), _) => write_error(out, &line),
				(_, Err(error)) => write_error(out, &format!("ERR {error}")),
				(Owed::Set, Ok(results)) => {
					if results.next().is_some_and(|result| result.succeeded) {
						out.extend_from_slice(b"+OK\r\n");
					} else {
						write_bulk(out, None);
					}
				},
				(Owed::Del(keys), Ok(results)) => {
					write_integer(out, results.take(keys).filter(|result| result.succeeded).count());
				},
			}
		}
	}

	async fn exists(&self, keys: &[Bytes], out: &mut Vec<u8>) -> Result<(), ErrorReply> {
		if keys.is_empty() {
			return Err(ErrorReply::arity("exists"));
		}
		let keys = utf8_keys(keys)?;
		let mut found = 0_usize;
		for key in keys {
			if self
				.kv
				.get_consistent(key)
				.await
				.map_err(ErrorReply::err)?
				.is_some()
			{
				found += 1;
			}
		}
		write_integer(out, found);
		Ok(())
	}
}

/// The txn of `SET key value [options]`.
fn set_txn(args: &[Bytes]) -> Result<Txn, ErrorReply> {
	let [key, value, options @ ..] = args else {
		return Err(ErrorReply::arity("set"));
	};
	let key = utf8_key(key)?;
	let SetOptions { condition, ttl_ms } = SetOptions::parse(options)?;
	let compare = condition.map(|condition| match condition {
		Condition::Missing => Compare::Missing(key.to_owned()),
		Condition::Exists => Compare::Exists(key.to_owned()),
	});
	Ok(Txn {
		compare: compare.into_iter().collect(),
		success: vec![Op::Put { key: key.to_owned(), value: value.to_vec(), ttl_ms }],
		failure: Vec::new(),
	})
}

/// One txn per key of `DEL key…`, succeeding iff the key existed. A malformed
/// key rejects the whole command before anything is deleted.
fn del_txns(keys: &[Bytes]) -> Result<Vec<Txn>, ErrorReply> {
	if keys.is_empty() {
		return Err(ErrorReply::arity("del"));
	}
	let txns = utf8_keys(keys)?
		.into_iter()
		.map(|key| Txn {
			compare: vec![Compare::Exists(key.to_owned())],
			success: vec![Op::Delete { key: key.to_owned() }],
			failure: Vec::new(),
		})
		.collect();
	Ok(txns)
}

/// `SET` guard (`NX` / `XX`).
#[derive(Clone, Copy)]
enum Condition {
	Missing,
	Exists,
}

/// Options after `SET key value`.
#[derive(Default)]
struct SetOptions {
	condition: Option<Condition>,
	ttl_ms:    Option<u64>,
}

impl SetOptions {
	fn parse(args: &[Bytes]) -> Result<Self, ErrorReply> {
		let syntax = || ErrorReply::err("syntax error");
		let mut options = Self::default();
		let mut args = args.iter();
		while let Some(option) = args.next() {
			let is = |name: &str| option.eq_ignore_ascii_case(name.as_bytes());
			let condition = if is("nx") {
				Some(Condition::Missing)
			} else if is("xx") {
				Some(Condition::Exists)
			} else {
				None
			};
			let scale = if is("ex") {
				Some(1000)
			} else if is("px") {
				Some(1)
			} else {
				None
			};
			match (condition, scale) {
				(Some(condition), _) if options.condition.is_none() => {
					options.condition = Some(condition);
				},
				(_, Some(scale)) if options.ttl_ms.is_none() => {
					let amount = args.next().ok_or_else(syntax)?;
					let amount = integer(amount)
						.ok_or_else(|| ErrorReply::err("value is not an integer or out of range"))?;
					let ttl_ms = u64::try_from(amount)
						.ok()
						.filter(|&n| n > 0)
						.and_then(|n| n.checked_mul(scale))
						.ok_or_else(|| ErrorReply::err("invalid expire time in 'set' command"))?;
					options.ttl_ms = Some(ttl_ms);
				},
				_ => return Err(syntax()),
			}
		}
		Ok(options)
	}
}

fn wrong_password() -> ErrorReply {
	ErrorReply("WRONGPASS invalid username-password pair or user is disabled.".into())
}

fn utf8_key(key: &[u8]) -> Result<&str, ErrorReply> {
	std::str::from_utf8(key).map_err(|_| ErrorReply::err("key is not valid UTF-8"))
}

fn utf8_keys(keys: &[Bytes]) -> Result<Vec<&str>, ErrorReply> {
	keys.iter().map(|key| utf8_key(key)).collect()
}

/// Writes pending replies plus a protocol error, then ends the connection.
async fn close(stream: &mut TcpStream, mut out: Vec<u8>, message: &str) -> io::Result<()> {
	write_error(&mut out, &format!("ERR Protocol error: {message}"));
	stream.write_all(&out).await
}

/// `-<line>\r\n`; CR/LF inside `line` (e.g. from a KV error) become spaces.
fn write_error(out: &mut Vec<u8>, line: &str) {
	out.push(b'-');
	out.extend(
		line
			.bytes()
			.map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
	);
	out.extend_from_slice(b"\r\n");
}

fn write_integer(out: &mut Vec<u8>, value: usize) {
	out.extend_from_slice(format!(":{value}\r\n").as_bytes());
}

/// Bulk string, or the null bulk string for `None`.
fn write_bulk(out: &mut Vec<u8>, value: Option<&[u8]>) {
	match value {
		Some(value) => {
			out.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
			out.extend_from_slice(value);
			out.extend_from_slice(b"\r\n");
		},
		None => out.extend_from_slice(b"$-1\r\n"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Parses everything in `bytes`, fed one byte at a time.
	fn parse_trickled(bytes: &[u8], limits: Limits) -> Result<Vec<Vec<Bytes>>, ProtocolError> {
		let mut buf = BytesMut::new();
		let mut commands = Vec::new();
		for &byte in bytes {
			buf.extend_from_slice(&[byte]);
			while let Some(command) = parse(&mut buf, limits)? {
				commands.push(command);
			}
		}
		assert!(buf.is_empty(), "unparsed tail: {buf:?}");
		Ok(commands)
	}

	#[test]
	fn pipelined_multibulk_and_inline_commands_survive_any_split() {
		let wire = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n\r\n  GET   k\n*0\r\nPING\r\n*1\r\n$0\r\n\r\n";
		let commands = parse_trickled(wire, AUTHED).unwrap();
		let expected: [&[&[u8]]; 6] =
			[&[b"SET", b"k", b"a\r\nb"], &[], &[b"GET", b"k"], &[], &[b"PING"], &[b""]];
		assert_eq!(commands.len(), expected.len());
		for (command, expected) in commands.iter().zip(expected) {
			assert_eq!(command.iter().map(AsRef::as_ref).collect::<Vec<&[u8]>>(), expected);
		}
	}

	#[test]
	fn unauthenticated_limits_reject_oversized_requests_before_their_data() {
		let mut eleven = BytesMut::from(&b"*11\r\n"[..]);
		assert_eq!(parse(&mut eleven, ANON), Err(ProtocolError("invalid multibulk length")));
		assert_eq!(parse(&mut eleven, AUTHED), Ok(None), "authenticated: waits for arguments");

		let mut big = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$16385\r\n"[..]);
		assert_eq!(parse(&mut big, ANON), Err(ProtocolError("invalid bulk length")));
		let mut fits = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$16384\r\n"[..]);
		assert_eq!(parse(&mut fits, ANON), Ok(None));
	}

	#[test]
	fn malformed_frames_are_protocol_errors() {
		for (wire, error) in [
			(&b"*1\r\n:3\r\n"[..], "expected '$'"),
			(&b"*1\r\n$3\r\nGETX\r\n"[..], "expected CRLF after bulk string"),
			(&b"*x\r\n"[..], "invalid multibulk length"),
			(&b"*1\r\n$-1\r\n"[..], "invalid bulk length"),
		] {
			assert_eq!(parse(&mut BytesMut::from(wire), AUTHED), Err(ProtocolError(error)));
		}
		let endless = vec![b'a'; MAX_LINE + 1];
		assert_eq!(
			parse(&mut BytesMut::from(endless.as_slice()), AUTHED),
			Err(ProtocolError("too big inline request"))
		);
	}

	#[test]
	fn set_options_follow_redis_rules() {
		// (NX?, ttl_ms) or the error line.
		let options = |words: &[&str]| {
			let args: Vec<Bytes> = words
				.iter()
				.map(|w| Bytes::copy_from_slice(w.as_bytes()))
				.collect();
			SetOptions::parse(&args)
				.map(|o| (o.condition.map(|c| matches!(c, Condition::Missing)), o.ttl_ms))
				.map_err(|ErrorReply(line)| line)
		};
		assert_eq!(options(&["nx", "PX", "250"]), Ok((Some(true), Some(250))));
		assert_eq!(options(&["EX", "2", "xx"]), Ok((Some(false), Some(2000))));
		for bad in [&["nx", "xx"][..], &["ex", "1", "px", "1"][..], &["ex"][..], &["keepttl"][..]] {
			assert_eq!(options(bad), Err("ERR syntax error".into()), "{bad:?}");
		}
		for bad in [&["ex", "0"][..], &["px", "-5"][..], &["ex", "9223372036854775807"][..]] {
			assert_eq!(
				options(bad),
				Err("ERR invalid expire time in 'set' command".into()),
				"{bad:?}"
			);
		}
	}
}
