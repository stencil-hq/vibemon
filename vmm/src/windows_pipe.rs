use std::{
	ffi::OsStr,
	io,
	os::windows::{
		ffi::OsStrExt,
		io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
	},
	path::Path,
};

use windows_sys::Win32::{
	Foundation::{
		DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, ERROR_NO_DATA,
		ERROR_PIPE_CONNECTED, ERROR_PIPE_LISTENING, HANDLE, INVALID_HANDLE_VALUE,
	},
	Storage::FileSystem::{PIPE_ACCESS_DUPLEX, ReadFile, WriteFile},
	System::{
		Pipes::{
			ConnectNamedPipe, CreateNamedPipeW, DisconnectNamedPipe, PIPE_NOWAIT, PIPE_READMODE_BYTE,
			PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
		},
		Threading::GetCurrentProcess,
	},
};

const PIPE_PREFIX: &str = r"\\.\pipe\";

/// Convert a socket-like path into a stable Windows named-pipe name.
pub fn pipe_name(path: &Path) -> Vec<u16> {
	let raw: Vec<u16> = path.as_os_str().encode_wide().collect();
	let explicit = String::from_utf16_lossy(&raw);
	let name = if explicit.to_ascii_lowercase().starts_with(PIPE_PREFIX) {
		explicit
	} else {
		let mut hash = 0xcbf29ce484222325u64;
		for unit in raw {
			for byte in unit.to_le_bytes() {
				hash ^= u64::from(byte);
				hash = hash.wrapping_mul(0x100000001b3);
			}
		}
		format!(r"{PIPE_PREFIX}vibemon-{hash:016x}")
	};
	OsStr::new(&name).encode_wide().chain(Some(0)).collect()
}

/// Owns one listening Windows named-pipe instance.
pub struct NamedPipeServer {
	handle: OwnedHandle,
}

impl NamedPipeServer {
	pub(super) fn bind(path: &Path) -> io::Result<Self> {
		let name = pipe_name(path);
		// SAFETY: `name` is NUL-terminated and optional security attributes are
		// null.
		let handle = unsafe {
			CreateNamedPipeW(
				name.as_ptr(),
				PIPE_ACCESS_DUPLEX,
				PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_NOWAIT | PIPE_REJECT_REMOTE_CLIENTS,
				1,
				64 * 1024,
				64 * 1024,
				0,
				std::ptr::null(),
			)
		};
		if handle == INVALID_HANDLE_VALUE {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: CreateNamedPipeW returned a fresh owned handle.
		let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
		Ok(Self { handle })
	}

	pub(super) fn try_accept(&self) -> io::Result<Option<NamedPipeClient>> {
		let handle = self.handle.as_raw_handle() as HANDLE;
		// SAFETY: `handle` is a live named-pipe server handle.
		if unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) } == 0 {
			let error = io::Error::last_os_error();
			if error.raw_os_error() == Some(ERROR_PIPE_LISTENING as i32) {
				return Ok(None);
			}
			if error.raw_os_error() != Some(ERROR_PIPE_CONNECTED as i32) {
				return Err(error);
			}
		}
		// SAFETY: retrieving the current-process pseudo-handle has no
		// preconditions.
		let process = unsafe { GetCurrentProcess() };
		let mut duplicate = std::ptr::null_mut();
		// SAFETY: `process` is the current-process pseudo-handle, `handle` is
		// live, and `duplicate` points to writable handle storage.
		if unsafe {
			DuplicateHandle(process, handle, process, &mut duplicate, 0, 0, DUPLICATE_SAME_ACCESS)
		} == 0
		{
			return Err(io::Error::last_os_error());
		}
		// SAFETY: `DuplicateHandle` returned a fresh owned handle.
		let handle = unsafe { OwnedHandle::from_raw_handle(duplicate as RawHandle) };
		Ok(Some(NamedPipeClient { handle }))
	}
}

impl Drop for NamedPipeServer {
	fn drop(&mut self) {
		// SAFETY: best-effort disconnect of this live server handle.
		unsafe { DisconnectNamedPipe(self.handle.as_raw_handle() as HANDLE) };
	}
}

/// Owns a connected duplicate of a named-pipe instance.
pub struct NamedPipeClient {
	handle: OwnedHandle,
}

impl NamedPipeClient {
	pub(super) fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let mut transferred = 0;
		// SAFETY: buffer is writable for the supplied length and handle is
		// connected.
		let ok = unsafe {
			ReadFile(
				self.handle.as_raw_handle() as HANDLE,
				buf.as_mut_ptr().cast(),
				buf.len().min(u32::MAX as usize) as u32,
				&mut transferred,
				std::ptr::null_mut(),
			)
		};
		if ok == 0 {
			let error = io::Error::last_os_error();
			if error.raw_os_error() == Some(ERROR_NO_DATA as i32) {
				return Err(io::Error::from(io::ErrorKind::WouldBlock));
			}
			return Err(error);
		}
		Ok(transferred as usize)
	}

	pub(super) fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		let mut transferred = 0;
		// SAFETY: buffer is readable for the supplied length and handle is
		// connected.
		let ok = unsafe {
			WriteFile(
				self.handle.as_raw_handle() as HANDLE,
				buf.as_ptr().cast(),
				buf.len().min(u32::MAX as usize) as u32,
				&mut transferred,
				std::ptr::null_mut(),
			)
		};
		if ok == 0 {
			return Err(io::Error::last_os_error());
		}
		Ok(transferred as usize)
	}

	pub(super) fn disconnected(error: &io::Error) -> bool {
		error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32)
	}
}

impl Drop for NamedPipeClient {
	fn drop(&mut self) {
		// SAFETY: best-effort disconnect of the live connected pipe instance.
		unsafe { DisconnectNamedPipe(self.handle.as_raw_handle() as HANDLE) };
	}
}
