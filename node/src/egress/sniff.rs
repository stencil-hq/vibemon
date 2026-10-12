//! Hostname sniffing for the egress proxy: TLS `ClientHello` SNI (port 443)
//! and the HTTP/1.x `Host` header (port 80).
//!
//! Both parsers are pure and incremental: the proxy keeps appending bytes
//! read from the guest and re-runs the parser until it returns something
//! other than [`Sniff::NeedMore`]. The consumed bytes are replayed upstream
//! unchanged.

/// Upper bound on bytes buffered while sniffing; beyond it the flow is
/// treated as carrying no hostname.
pub const MAX_SNIFF_BYTES: usize = 64 * 1024;

const TLS_HANDSHAKE: u8 = 0x16;
const CLIENT_HELLO: u8 = 0x01;
const EXT_SERVER_NAME: u16 = 0x0000;
const SNI_HOST_NAME: u8 = 0x00;

/// Result of one sniff attempt over the bytes buffered so far.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sniff {
	/// The prefix is consistent with the protocol but incomplete.
	NeedMore,
	/// A hostname (lowercased, port stripped).
	Host(String),
	/// Not this protocol, or it carries no usable hostname.
	NoHost,
}

/// Extract the SNI host name from a TLS `ClientHello`, which may be split
/// across several TLS records and arbitrarily across reads.
pub fn tls_sni(buf: &[u8]) -> Sniff {
	let mut handshake = Vec::new();
	let mut rest = buf;
	loop {
		if rest.is_empty() {
			return Sniff::NeedMore;
		}
		if rest[0] != TLS_HANDSHAKE {
			return Sniff::NoHost;
		}
		if rest.len() >= 2 && rest[1] != 0x03 {
			return Sniff::NoHost;
		}
		if rest.len() < 5 {
			return Sniff::NeedMore;
		}
		let record_len = usize::from(u16::from_be_bytes([rest[3], rest[4]]));
		if record_len == 0 {
			return Sniff::NoHost;
		}
		if rest.len() < 5 + record_len {
			// Parse what we have so far to fail fast on non-ClientHello.
			handshake.extend_from_slice(&rest[5..]);
			if handshake.first().is_some_and(|ty| *ty != CLIENT_HELLO) {
				return Sniff::NoHost;
			}
			return if buf.len() >= MAX_SNIFF_BYTES {
				Sniff::NoHost
			} else {
				Sniff::NeedMore
			};
		}
		handshake.extend_from_slice(&rest[5..5 + record_len]);
		rest = &rest[5 + record_len..];
		if handshake[0] != CLIENT_HELLO {
			return Sniff::NoHost;
		}
		if handshake.len() < 4 {
			continue;
		}
		let body_len = usize::from(handshake[1]) << 16
			| usize::from(handshake[2]) << 8
			| usize::from(handshake[3]);
		if handshake.len() < 4 + body_len {
			if 4 + body_len > MAX_SNIFF_BYTES {
				return Sniff::NoHost;
			}
			continue;
		}
		return client_hello_sni(&handshake[4..4 + body_len]).map_or(Sniff::NoHost, Sniff::Host);
	}
}

/// Byte cursor over a `ClientHello` body; every read is bounds-checked.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
	const fn take(&mut self, n: usize) -> Option<&'a [u8]> {
		if self.0.len() < n {
			return None;
		}
		let (head, tail) = self.0.split_at(n);
		self.0 = tail;
		Some(head)
	}

	fn u8(&mut self) -> Option<u8> {
		self.take(1).map(|b| b[0])
	}

	fn u16(&mut self) -> Option<u16> {
		self.take(2).map(|b| u16::from_be_bytes([b[0], b[1]]))
	}

	fn vec8(&mut self) -> Option<&'a [u8]> {
		let len = self.u8()?;
		self.take(usize::from(len))
	}

	fn vec16(&mut self) -> Option<&'a [u8]> {
		let len = self.u16()?;
		self.take(usize::from(len))
	}
}

fn client_hello_sni(body: &[u8]) -> Option<String> {
	let mut r = Reader(body);
	r.take(2 + 32)?; // legacy_version + random
	r.vec8()?; // session id
	r.vec16()?; // cipher suites
	r.vec8()?; // compression methods
	let mut extensions = Reader(r.vec16()?);
	while !extensions.0.is_empty() {
		let ty = extensions.u16()?;
		let data = extensions.vec16()?;
		if ty != EXT_SERVER_NAME {
			continue;
		}
		let mut list = Reader(Reader(data).vec16()?);
		while !list.0.is_empty() {
			let name_type = list.u8()?;
			let name = list.vec16()?;
			if name_type == SNI_HOST_NAME {
				return hostname(name);
			}
		}
		return None;
	}
	None
}

fn hostname(raw: &[u8]) -> Option<String> {
	let name = std::str::from_utf8(raw).ok()?.trim_end_matches('.');
	let valid = !name.is_empty()
		&& name.len() <= 253
		&& name
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'));
	valid.then(|| name.to_ascii_lowercase())
}

/// Extract the `Host` header of an HTTP/1.x request head.
pub fn http_host(buf: &[u8]) -> Sniff {
	// The request line must start with an uppercase token method.
	let method_len = buf.iter().take_while(|b| b.is_ascii_uppercase()).count();
	if method_len == buf.len() {
		return if method_len > 16 {
			Sniff::NoHost
		} else {
			Sniff::NeedMore
		};
	}
	if method_len == 0 || buf[method_len] != b' ' {
		return Sniff::NoHost;
	}
	let Some(head_end) = find(buf, b"\r\n\r\n") else {
		return if buf.len() >= MAX_SNIFF_BYTES {
			Sniff::NoHost
		} else {
			Sniff::NeedMore
		};
	};
	let head = &buf[..head_end];
	for line in head.split(|b| *b == b'\n').skip(1) {
		let line = line.strip_suffix(b"\r").unwrap_or(line);
		let Some(colon) = line.iter().position(|b| *b == b':') else {
			continue;
		};
		if !line[..colon].eq_ignore_ascii_case(b"host") {
			continue;
		}
		let Ok(value) = std::str::from_utf8(&line[colon + 1..]) else {
			return Sniff::NoHost;
		};
		return strip_port(value.trim())
			.and_then(|host| hostname(host.as_bytes()))
			.map_or(Sniff::NoHost, Sniff::Host);
	}
	Sniff::NoHost
}

/// `example.com:8080` → `example.com`; `[::1]:80` → `::1` (rejected later
/// by [`hostname`] since it is not a DNS name).
fn strip_port(value: &str) -> Option<&str> {
	if let Some(rest) = value.strip_prefix('[') {
		return rest.split_once(']').map(|(host, _)| host);
	}
	Some(value.rsplit_once(':').map_or(value, |(host, _)| host))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

#[cfg(test)]
pub(in crate::egress) mod tests {
	use super::*;

	/// A minimal TLS 1.2-framed `ClientHello` with the given SNI.
	fn client_hello(sni: Option<&str>) -> Vec<u8> {
		client_hello_with_key_share(sni, 0)
	}

	/// A `ClientHello` whose SNI follows a `key_share` extension of
	/// `key_share` bytes (OpenSSL 3.5 sends X25519MLKEM768 ≈ 1.2 KB, pushing
	/// the hello past one TCP segment).
	pub(in crate::egress) fn client_hello_with_key_share(
		sni: Option<&str>,
		key_share: usize,
	) -> Vec<u8> {
		let mut body = vec![0x03, 0x03];
		body.extend_from_slice(&[7; 32]);
		body.push(0); // session id
		body.extend_from_slice(&[0, 2, 0x13, 0x01]); // one cipher suite
		body.extend_from_slice(&[1, 0]); // null compression
		let mut extensions = Vec::new();
		// An unrelated extension first (supported_versions).
		extensions.extend_from_slice(&[0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04]);
		if key_share > 0 {
			extensions.extend_from_slice(&[0x00, 0x33]);
			extensions.extend_from_slice(&u16::try_from(key_share).expect("len").to_be_bytes());
			extensions.extend(std::iter::repeat_n(0x5a, key_share));
		}
		if let Some(name) = sni {
			let name = name.as_bytes();
			let entry_len = 3 + name.len();
			extensions.extend_from_slice(&EXT_SERVER_NAME.to_be_bytes());
			extensions.extend_from_slice(&u16::try_from(entry_len + 2).expect("len").to_be_bytes());
			extensions.extend_from_slice(&u16::try_from(entry_len).expect("len").to_be_bytes());
			extensions.push(SNI_HOST_NAME);
			extensions.extend_from_slice(&u16::try_from(name.len()).expect("len").to_be_bytes());
			extensions.extend_from_slice(name);
		}
		body.extend_from_slice(&u16::try_from(extensions.len()).expect("len").to_be_bytes());
		body.extend_from_slice(&extensions);
		let mut handshake = vec![CLIENT_HELLO];
		handshake.extend_from_slice(&u32::try_from(body.len()).expect("len").to_be_bytes()[1..]);
		handshake.extend_from_slice(&body);
		records(&handshake, handshake.len())
	}

	/// Frame a handshake into TLS records of at most `chunk` bytes.
	fn records(handshake: &[u8], chunk: usize) -> Vec<u8> {
		let mut out = Vec::new();
		for part in handshake.chunks(chunk) {
			out.extend_from_slice(&[TLS_HANDSHAKE, 0x03, 0x01]);
			out.extend_from_slice(&u16::try_from(part.len()).expect("len").to_be_bytes());
			out.extend_from_slice(part);
		}
		out
	}

	#[test]
	fn sni_from_complete_hello() {
		assert_eq!(
			tls_sni(&client_hello(Some("API.Example.com"))),
			Sniff::Host("api.example.com".into())
		);
		assert_eq!(tls_sni(&client_hello(None)), Sniff::NoHost);
	}

	#[test]
	fn sni_after_large_key_share() {
		let hello = client_hello_with_key_share(Some("example.com"), 1500);
		assert!(hello.len() > 1460, "must exceed one Ethernet-MSS segment");
		assert_eq!(tls_sni(&hello[..1448]), Sniff::NeedMore);
		assert_eq!(tls_sni(&hello), Sniff::Host("example.com".into()));
	}
	#[test]
	fn sni_across_reads_and_records() {
		let hello = client_hello(Some("example.org"));
		for cut in 0..hello.len() {
			assert_eq!(tls_sni(&hello[..cut]), Sniff::NeedMore, "cut at {cut}");
		}
		// Re-frame the same handshake into 7-byte records.
		let handshake = {
			let mut out = Vec::new();
			let mut rest = &hello[..];
			while !rest.is_empty() {
				let len = usize::from(u16::from_be_bytes([rest[3], rest[4]]));
				out.extend_from_slice(&rest[5..5 + len]);
				rest = &rest[5 + len..];
			}
			out
		};
		let fragmented = records(&handshake, 7);
		for cut in 0..fragmented.len() {
			assert_eq!(tls_sni(&fragmented[..cut]), Sniff::NeedMore, "cut at {cut}");
		}
		assert_eq!(tls_sni(&fragmented), Sniff::Host("example.org".into()));
	}

	#[test]
	fn non_tls_and_non_hello_are_rejected_early() {
		assert_eq!(tls_sni(b"GET / HTTP/1.1\r\n"), Sniff::NoHost);
		assert_eq!(tls_sni(&[TLS_HANDSHAKE, 0x02]), Sniff::NoHost);
		// ServerHello-type handshake.
		assert_eq!(tls_sni(&[TLS_HANDSHAKE, 0x03, 0x01, 0x00, 0x10, 0x02]), Sniff::NoHost);
		// Truncated extension list inside a complete record.
		let mut hello = client_hello(Some("example.org"));
		let len = hello.len();
		hello[len - 3] = 0xff; // corrupt the SNI name length
		assert_eq!(tls_sni(&hello), Sniff::NoHost);
	}

	#[test]
	fn http_host_header() {
		let req = b"GET /x HTTP/1.1\r\nUser-Agent: t\r\nHOST: Example.COM:8080\r\n\r\nbody";
		assert_eq!(http_host(req), Sniff::Host("example.com".into()));
		for cut in 0..req.len() - 4 {
			assert_eq!(http_host(&req[..cut]), Sniff::NeedMore, "cut at {cut}");
		}
		assert_eq!(http_host(b"POST / HTTP/1.1\r\nhost:a.io\r\n\r\n"), Sniff::Host("a.io".into()));
		assert_eq!(http_host(b"GET / HTTP/1.0\r\nAccept: */*\r\n\r\n"), Sniff::NoHost);
		assert_eq!(http_host(b"GET / HTTP/1.1\r\nHost: [::1]:80\r\n\r\n"), Sniff::NoHost);
		assert_eq!(http_host(b"\x16\x03\x01"), Sniff::NoHost);
		assert_eq!(http_host(b"SSH-2.0-OpenSSH\r\n"), Sniff::NoHost);
	}
}
