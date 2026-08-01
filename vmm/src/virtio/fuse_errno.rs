//! Linux errno values carried by the guest-facing FUSE wire protocol.

use std::io;

pub(super) const ENOENT: i32 = 2;
pub(super) const EINTR: i32 = 4;
pub(super) const EIO: i32 = 5;
pub(super) const E2BIG: i32 = 7;
pub(super) const EAGAIN: i32 = 11;
pub(super) const ENOMEM: i32 = 12;
pub(super) const EACCES: i32 = 13;
pub(super) const EBUSY: i32 = 16;
pub(super) const EEXIST: i32 = 17;
pub(super) const EXDEV: i32 = 18;
pub(super) const ENOTDIR: i32 = 20;
pub(super) const EISDIR: i32 = 21;
pub(super) const EINVAL: i32 = 22;
pub(super) const ETXTBSY: i32 = 26;
pub(super) const EFBIG: i32 = 27;
pub(super) const ENOSPC: i32 = 28;
pub(super) const ESPIPE: i32 = 29;
pub(super) const EROFS: i32 = 30;
pub(super) const EMLINK: i32 = 31;
pub(super) const EPIPE: i32 = 32;
pub(super) const EDEADLK: i32 = 35;
pub(super) const ENAMETOOLONG: i32 = 36;
pub(super) const ENOSYS: i32 = 38;
pub(super) const ENOTEMPTY: i32 = 39;
#[cfg(any(target_os = "macos", all(test, not(target_os = "windows"))))]
pub(super) const ELOOP: i32 = 40;
#[cfg(any(target_os = "macos", target_os = "windows"))]
pub(super) const EOVERFLOW: i32 = 75;
pub(super) const EOPNOTSUPP: i32 = 95;
pub(super) const EADDRINUSE: i32 = 98;
pub(super) const EADDRNOTAVAIL: i32 = 99;
pub(super) const ENETDOWN: i32 = 100;
pub(super) const ENETUNREACH: i32 = 101;
pub(super) const ECONNABORTED: i32 = 103;
pub(super) const ECONNRESET: i32 = 104;
pub(super) const ENOTCONN: i32 = 107;
pub(super) const ETIMEDOUT: i32 = 110;
pub(super) const ECONNREFUSED: i32 = 111;
pub(super) const EHOSTUNREACH: i32 = 113;
pub(super) const ESTALE: i32 = 116;
pub(super) const EDQUOT: i32 = 122;

/// Translate a host I/O error into the positive Linux errno expected by FUSE.
pub(super) fn from_io(error: &io::Error) -> i32 {
	#[cfg(target_os = "linux")]
	if let Some(code) = error.raw_os_error().filter(|code| *code > 0) {
		return code;
	}

	#[cfg(target_os = "macos")]
	if let Some(code) = error.raw_os_error().and_then(macos_raw_errno) {
		return code;
	}

	match error.kind() {
		io::ErrorKind::NotFound => ENOENT,
		io::ErrorKind::PermissionDenied => EACCES,
		io::ErrorKind::ConnectionRefused => ECONNREFUSED,
		io::ErrorKind::ConnectionReset => ECONNRESET,
		io::ErrorKind::HostUnreachable => EHOSTUNREACH,
		io::ErrorKind::NetworkUnreachable => ENETUNREACH,
		io::ErrorKind::ConnectionAborted => ECONNABORTED,
		io::ErrorKind::NotConnected => ENOTCONN,
		io::ErrorKind::AddrInUse => EADDRINUSE,
		io::ErrorKind::AddrNotAvailable => EADDRNOTAVAIL,
		io::ErrorKind::NetworkDown => ENETDOWN,
		io::ErrorKind::BrokenPipe => EPIPE,
		io::ErrorKind::AlreadyExists => EEXIST,
		io::ErrorKind::WouldBlock => EAGAIN,
		io::ErrorKind::NotADirectory => ENOTDIR,
		io::ErrorKind::IsADirectory => EISDIR,
		io::ErrorKind::DirectoryNotEmpty => ENOTEMPTY,
		io::ErrorKind::ReadOnlyFilesystem => EROFS,
		io::ErrorKind::StaleNetworkFileHandle => ESTALE,
		io::ErrorKind::InvalidInput => EINVAL,
		io::ErrorKind::TimedOut => ETIMEDOUT,
		io::ErrorKind::StorageFull => ENOSPC,
		io::ErrorKind::NotSeekable => ESPIPE,
		io::ErrorKind::QuotaExceeded => EDQUOT,
		io::ErrorKind::FileTooLarge => EFBIG,
		io::ErrorKind::ResourceBusy => EBUSY,
		io::ErrorKind::ExecutableFileBusy => ETXTBSY,
		io::ErrorKind::Deadlock => EDEADLK,
		io::ErrorKind::CrossesDevices => EXDEV,
		io::ErrorKind::TooManyLinks => EMLINK,
		io::ErrorKind::InvalidFilename => ENAMETOOLONG,
		io::ErrorKind::ArgumentListTooLong => E2BIG,
		io::ErrorKind::Interrupted => EINTR,
		io::ErrorKind::Unsupported => EOPNOTSUPP,
		io::ErrorKind::OutOfMemory => ENOMEM,
		io::ErrorKind::InvalidData | io::ErrorKind::WriteZero | io::ErrorKind::UnexpectedEof => EIO,
		_ => EIO,
	}
}

#[cfg(target_os = "macos")]
const fn macos_raw_errno(code: i32) -> Option<i32> {
	match code {
		libc::ELOOP => Some(ELOOP),
		libc::ENAMETOOLONG => Some(ENAMETOOLONG),
		libc::ENOSYS => Some(ENOSYS),
		libc::EOVERFLOW => Some(EOVERFLOW),
		libc::ENOTSUP | libc::EOPNOTSUPP => Some(EOPNOTSUPP),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn semantic_io_errors_map_to_linux_errno() {
		assert_eq!(from_io(&io::Error::from(io::ErrorKind::NotFound)), ENOENT);
		assert_eq!(from_io(&io::Error::from(io::ErrorKind::DirectoryNotEmpty)), ENOTEMPTY);
		assert_eq!(from_io(&io::Error::from(io::ErrorKind::Unsupported)), EOPNOTSUPP);
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn darwin_errno_values_are_not_leaked_to_linux_guests() {
		assert_eq!(from_io(&io::Error::from_raw_os_error(libc::ELOOP)), ELOOP);
		assert_eq!(from_io(&io::Error::from_raw_os_error(libc::ENOSYS)), ENOSYS);
		assert_eq!(from_io(&io::Error::from_raw_os_error(libc::EOVERFLOW)), EOVERFLOW);
	}
}
