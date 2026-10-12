//! Conntrack flush by connection mark over ctnetlink, without conntrack-tools
//! (ARCHITECTURE.md §8.4).
//!
//! Every TAP's guest flows carry the lease's connection mark (`CONNMARK
//! --set-mark`, installed with the TAP's NAT rules). Recycling or tearing
//! down a lease deletes them with one `IPCTNL_MSG_CT_DELETE` filtered by
//! `CTA_MARK`/`CTA_MARK_MASK`, so the next sandbox on the same address never
//! inherits NAT mappings or `ESTABLISHED` state of the previous one.

use std::{
	io, mem,
	os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

/// `nfnetlink` subsystem of connection tracking.
const NFNL_SUBSYS_CTNETLINK: u16 = 1;
/// ctnetlink "delete" message; without a tuple it is a (filtered) flush.
const IPCTNL_MSG_CT_DELETE: u16 = 2;
/// Netlink request flags: `NLM_F_REQUEST | NLM_F_ACK`.
const REQUEST_ACK: u16 = 0x1 | 0x4;
/// `NLMSG_ERROR`: the kernel's acknowledgement (error 0) or failure.
const NLMSG_ERROR: u16 = 2;
/// `nfgenmsg.nfgen_family` (`AF_INET`; TAP networking is IPv4).
const AF_INET: u8 = 2;
/// `nfgenmsg.version`.
const NFNETLINK_V0: u8 = 0;
/// `ctattr_type::CTA_MARK` (big-endian u32).
const CTA_MARK: u16 = 8;
/// `ctattr_type::CTA_MARK_MASK` (big-endian u32).
const CTA_MARK_MASK: u16 = 21;
/// `sizeof(struct nlmsghdr)`.
const NLMSG_HDRLEN: usize = 16;
/// Connection marks are owned whole (`--set-mark` without a mask).
const MARK_MASK: u32 = u32::MAX;
/// Upper bound on waiting for the kernel's acknowledgement.
const ACK_TIMEOUT_SECS: libc::time_t = 5;

/// Deletes every conntrack entry whose mark is `mark`. Blocking (one
/// netlink round trip; the kernel walks the conntrack table).
pub(super) fn flush_mark(mark: u32) -> io::Result<()> {
	// SAFETY: socket(2) with constant arguments; the result is checked.
	let fd = unsafe {
		libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_NETFILTER)
	};
	if fd < 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: `fd` is a fresh socket nothing else owns.
	let socket = unsafe { OwnedFd::from_raw_fd(fd) };
	let timeout = libc::timeval { tv_sec: ACK_TIMEOUT_SECS, tv_usec: 0 };
	// SAFETY: the socket is live; `timeout` is a timeval of the advertised
	// size.
	let rc = unsafe {
		libc::setsockopt(
			socket.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_RCVTIMEO,
			(&raw const timeout).cast(),
			mem::size_of::<libc::timeval>() as libc::socklen_t,
		)
	};
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: sockaddr_nl is plain old data; all-zero is a valid value.
	let mut kernel: libc::sockaddr_nl = unsafe { mem::zeroed() };
	kernel.nl_family = libc::AF_NETLINK as libc::sa_family_t;
	let seq = 1;
	let message = flush_message(mark, seq);
	// SAFETY: `message` is valid for its length; `kernel` is a sockaddr_nl of
	// the advertised size.
	let sent = unsafe {
		libc::sendto(
			socket.as_raw_fd(),
			message.as_ptr().cast(),
			message.len(),
			0,
			(&raw const kernel).cast(),
			mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
		)
	};
	if sent < 0 {
		return Err(io::Error::last_os_error());
	}
	let mut buf = [0_u8; 4096];
	loop {
		// SAFETY: `buf` is writable for its length.
		let received =
			unsafe { libc::recv(socket.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
		if received < 0 {
			let error = io::Error::last_os_error();
			if error.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			return Err(error);
		}
		if let Some(result) = parse_ack(&buf[..received as usize], seq) {
			return result;
		}
	}
}

/// `IPCTNL_MSG_CT_DELETE` request flushing the IPv4 entries marked `mark`.
fn flush_message(mark: u32, seq: u32) -> Vec<u8> {
	let mut message = Vec::with_capacity(NLMSG_HDRLEN + 4 + 2 * 8);
	// nlmsghdr (host byte order); the length is patched below.
	message.extend_from_slice(&0_u32.to_ne_bytes());
	message.extend_from_slice(&((NFNL_SUBSYS_CTNETLINK << 8) | IPCTNL_MSG_CT_DELETE).to_ne_bytes());
	message.extend_from_slice(&REQUEST_ACK.to_ne_bytes());
	message.extend_from_slice(&seq.to_ne_bytes());
	message.extend_from_slice(&0_u32.to_ne_bytes());
	// nfgenmsg: family, version, res_id (big endian).
	message.extend_from_slice(&[AF_INET, NFNETLINK_V0, 0, 0]);
	// Two 8-byte nlattrs (already 4-aligned) with big-endian u32 payloads.
	for (kind, value) in [(CTA_MARK, mark), (CTA_MARK_MASK, MARK_MASK)] {
		message.extend_from_slice(&8_u16.to_ne_bytes());
		message.extend_from_slice(&kind.to_ne_bytes());
		message.extend_from_slice(&value.to_be_bytes());
	}
	let len = message.len() as u32;
	message[..4].copy_from_slice(&len.to_ne_bytes());
	message
}

/// Outcome of the `NLMSG_ERROR` acknowledging `seq` in a received datagram,
/// if it holds one. `ENOENT` (nothing matched) counts as success.
fn parse_ack(mut datagram: &[u8], seq: u32) -> Option<io::Result<()>> {
	let u32_at = |bytes: &[u8], at: usize| {
		u32::from_ne_bytes(bytes[at..at + 4].try_into().expect("4-byte slice"))
	};
	while datagram.len() >= NLMSG_HDRLEN {
		let len = u32_at(datagram, 0) as usize;
		if len < NLMSG_HDRLEN || len > datagram.len() {
			return None;
		}
		let kind = u16::from_ne_bytes([datagram[4], datagram[5]]);
		if kind == NLMSG_ERROR && u32_at(datagram, 8) == seq && len >= NLMSG_HDRLEN + 4 {
			let error = u32_at(datagram, NLMSG_HDRLEN) as i32;
			return Some(match error {
				0 => Ok(()),
				error if error == -libc::ENOENT => Ok(()),
				error => Err(io::Error::from_raw_os_error(error.wrapping_neg())),
			});
		}
		datagram = &datagram[((len + 3) & !3).min(datagram.len())..];
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn flush_message_encodes_mark_filtered_ct_delete() {
		let message = flush_message(0x0001_0203, 7);
		let mut expected = Vec::new();
		expected.extend_from_slice(&36_u32.to_ne_bytes());
		expected.extend_from_slice(&0x0102_u16.to_ne_bytes());
		expected.extend_from_slice(&0x0005_u16.to_ne_bytes());
		expected.extend_from_slice(&7_u32.to_ne_bytes());
		expected.extend_from_slice(&0_u32.to_ne_bytes());
		expected.extend_from_slice(&[2, 0, 0, 0]);
		expected.extend_from_slice(&8_u16.to_ne_bytes());
		expected.extend_from_slice(&8_u16.to_ne_bytes());
		expected.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
		expected.extend_from_slice(&8_u16.to_ne_bytes());
		expected.extend_from_slice(&21_u16.to_ne_bytes());
		expected.extend_from_slice(&[0xff; 4]);
		assert_eq!(message, expected);
	}

	#[test]
	fn ack_parsing_accepts_success_and_enoent_only_for_our_seq() {
		let ack = |seq: u32, error: i32| {
			let mut datagram = Vec::new();
			datagram.extend_from_slice(&36_u32.to_ne_bytes());
			datagram.extend_from_slice(&NLMSG_ERROR.to_ne_bytes());
			datagram.extend_from_slice(&0_u16.to_ne_bytes());
			datagram.extend_from_slice(&seq.to_ne_bytes());
			datagram.extend_from_slice(&0_u32.to_ne_bytes());
			datagram.extend_from_slice(&error.to_ne_bytes());
			// Echoed request header.
			datagram.extend_from_slice(&[0; NLMSG_HDRLEN]);
			datagram
		};
		assert!(matches!(parse_ack(&ack(1, 0), 1), Some(Ok(()))));
		assert!(matches!(parse_ack(&ack(1, -libc::ENOENT), 1), Some(Ok(()))));
		let error = parse_ack(&ack(1, -libc::EPERM), 1)
			.expect("ack")
			.expect_err("EPERM");
		assert_eq!(error.raw_os_error(), Some(libc::EPERM));
		assert!(parse_ack(&ack(2, 0), 1).is_none());
		assert!(parse_ack(&[0; 8], 1).is_none());
	}
}
