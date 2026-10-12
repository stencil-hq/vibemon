//! Guest CRNG reseeding for [`Op::Reseed`](crate::proto::Op::Reseed).
//!
//! Every VM restored from one memory snapshot starts with the same kernel CRNG
//! state; the host sends fresh entropy after each restore so forks diverge
//! before anything in the guest draws randomness.

use std::{io, os::fd::AsRawFd};

use base64::Engine as _;
use serde_json::Value;

/// Bytes of host entropy a reseed request carries (512 bits).
const ENTROPY_BYTES: usize = 64;

/// Credits the request's 64 host bytes to the input pool and forces a CRNG
/// reseed from it.
///
/// # Errors
/// `InvalidInput` when `bytes` is missing or does not decode to exactly 64
/// bytes; the `ioctl` error when the kernel rejects the entropy or reseed.
pub fn reseed(request: &Value) -> io::Result<()> {
	add_entropy(parse(request)?)
}

/// The 64 entropy bytes of a reseed request.
fn parse(request: &Value) -> io::Result<[u8; ENTROPY_BYTES]> {
	let invalid = |message: &str| io::Error::new(io::ErrorKind::InvalidInput, message.to_owned());
	let encoded = request
		.get("bytes")
		.and_then(Value::as_str)
		.ok_or_else(|| invalid("bytes must be base64"))?;
	let decoded = base64::engine::general_purpose::STANDARD
		.decode(encoded)
		.map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
	decoded
		.try_into()
		.map_err(|_| invalid("bytes must encode exactly 64 bytes"))
}

/// Credits `bytes` to the input pool, then forces a CRNG reseed from it.
fn add_entropy(bytes: [u8; ENTROPY_BYTES]) -> io::Result<()> {
	/// `struct rand_pool_info` with a 64-byte buffer.
	#[repr(C)]
	struct Pool {
		entropy_count: libc::c_int,
		buf_size:      libc::c_int,
		buf:           [u8; ENTROPY_BYTES],
	}
	// Linux random.h: _IOW('R', 0x03, int[2]) and _IO('R', 0x07).
	const RNDADDENTROPY: libc::Ioctl = 0x4008_5203;
	const RNDRESEEDCRNG: libc::Ioctl = 0x5207;
	let pool = Pool {
		entropy_count: 512,
		buf_size:      ENTROPY_BYTES as libc::c_int,
		buf:           bytes,
	};
	let random = std::fs::OpenOptions::new()
		.read(true)
		.write(true)
		.open("/dev/urandom")?;
	// SAFETY: `pool` has the kernel's rand_pool_info layout and outlives the
	// call; `random` is an open random-device descriptor.
	if unsafe { libc::ioctl(random.as_raw_fd(), RNDADDENTROPY, &raw const pool) } < 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: RNDRESEEDCRNG takes no argument; `random` is still open.
	if unsafe { libc::ioctl(random.as_raw_fd(), RNDRESEEDCRNG) } < 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn requests_carry_exactly_64_base64_bytes() {
		let encode =
			|bytes: &[u8]| json!({"bytes": base64::engine::general_purpose::STANDARD.encode(bytes)});
		assert_eq!(parse(&encode(&[42; 64])).unwrap(), [42; 64]);
		for len in [0, 63, 65, 66] {
			assert!(parse(&encode(&vec![0; len])).is_err(), "{len}");
		}
		assert!(parse(&json!({"bytes": "!".repeat(88)})).is_err());
		assert!(parse(&json!({"bytes": 64})).is_err());
		assert!(parse(&json!({})).is_err());
	}
}
