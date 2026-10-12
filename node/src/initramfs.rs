//! Generated initramfs for template unpack VMs (ARCHITECTURE.md §8.6 step 2).
//!
//! The image is an uncompressed cpio "newc" archive holding the static guest
//! agent as `/init` plus the handful of directories and the `/dev/console`
//! node the kernel needs before the agent mounts devtmpfs itself. Built in
//! Rust so nodes need neither `cpio` nor root privileges.

use std::io::{self, Write};

/// Mode bits of a cpio entry type (`S_IFDIR`, `S_IFREG`, `S_IFCHR`).
const S_IFDIR: u32 = 0o040_000;
const S_IFREG: u32 = 0o100_000;
const S_IFCHR: u32 = 0o020_000;
const TRAILER: &str = "TRAILER!!!";
const MAGIC: &[u8; 6] = b"070701";

/// Streaming writer for cpio newc archives.
pub struct CpioWriter<W: Write> {
	out:      W,
	next_ino: u32,
	written:  u64,
}

impl<W: Write> CpioWriter<W> {
	/// Starts an archive on `out`.
	pub const fn new(out: W) -> Self {
		Self { out, next_ino: 1, written: 0 }
	}

	/// Adds a directory with permission bits `perm`.
	pub fn dir(&mut self, path: &str, perm: u32) -> io::Result<()> {
		self.entry(path, S_IFDIR | (perm & 0o7777), 2, (0, 0), &[])
	}

	/// Adds a regular file with permission bits `perm`.
	pub fn file(&mut self, path: &str, perm: u32, data: &[u8]) -> io::Result<()> {
		self.entry(path, S_IFREG | (perm & 0o7777), 1, (0, 0), data)
	}

	/// Adds a character device node `major:minor`.
	pub fn char_device(&mut self, path: &str, perm: u32, major: u32, minor: u32) -> io::Result<()> {
		self.entry(path, S_IFCHR | (perm & 0o7777), 1, (major, minor), &[])
	}

	/// Writes the trailer and returns the underlying writer.
	pub fn finish(mut self) -> io::Result<W> {
		self.entry(TRAILER, 0, 1, (0, 0), &[])?;
		self.out.flush()?;
		Ok(self.out)
	}

	fn entry(
		&mut self,
		path: &str,
		mode: u32,
		nlink: u32,
		rdev: (u32, u32),
		data: &[u8],
	) -> io::Result<()> {
		let name = path.trim_start_matches('/');
		if name.is_empty() || name.contains('\0') {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("invalid cpio path {path:?}"),
			));
		}
		let size = u32::try_from(data.len())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cpio entry over 4 GiB"))?;
		let name_len = u32::try_from(name.len() + 1)
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cpio name too long"))?;
		let ino = if name == TRAILER { 0 } else { self.next_ino };
		self.next_ino += 1;
		let fields = [ino, mode, 0, 0, nlink, 0, size, 0, 0, rdev.0, rdev.1, name_len, 0];
		let mut header = Vec::with_capacity(110 + name.len() + 4);
		header.extend_from_slice(MAGIC);
		for field in fields {
			header.extend_from_slice(format!("{field:08x}").as_bytes());
		}
		header.extend_from_slice(name.as_bytes());
		header.push(0);
		self.write_padded(&header)?;
		self.write_padded(data)
	}

	fn write_padded(&mut self, bytes: &[u8]) -> io::Result<()> {
		self.out.write_all(bytes)?;
		self.written += bytes.len() as u64;
		let pad = (4 - (self.written % 4)) % 4;
		if pad != 0 {
			self
				.out
				.write_all(&[0; 3][..usize::try_from(pad).unwrap_or(0)])?;
			self.written += pad;
		}
		Ok(())
	}
}

/// Builds the unpack-VM initramfs: `agent` becomes `/init`; mount points for
/// the target disk (`/mnt/root`) and the read-only layer share
/// (`/mnt/layers`) are pre-created.
pub fn unpack_initramfs(agent: &[u8]) -> io::Result<Vec<u8>> {
	let mut cpio = CpioWriter::new(Vec::with_capacity(agent.len() + 4096));
	for dir in ["dev", "proc", "sys", "run", "tmp", "mnt", "mnt/root", "mnt/layers", "etc", "root"] {
		let perm = if dir == "tmp" { 0o1777 } else { 0o755 };
		cpio.dir(dir, perm)?;
	}
	cpio.char_device("dev/console", 0o600, 5, 1)?;
	cpio.char_device("dev/null", 0o666, 1, 3)?;
	cpio.file("init", 0o755, agent)?;
	cpio.finish()
}

#[cfg(test)]
mod tests {
	use super::*;

	struct Entry {
		name: String,
		mode: u32,
		rdev: (u32, u32),
		data: Vec<u8>,
	}

	fn parse(mut bytes: &[u8]) -> Vec<Entry> {
		let total = bytes.len();
		let mut entries = Vec::new();
		loop {
			assert_eq!(&bytes[..6], MAGIC, "bad magic at offset {}", total - bytes.len());
			let field = |i: usize| {
				u32::from_str_radix(std::str::from_utf8(&bytes[6 + i * 8..14 + i * 8]).unwrap(), 16)
					.unwrap()
			};
			let (mode, size, rdev, name_len) =
				(field(1), field(6) as usize, (field(9), field(10)), field(11) as usize);
			let name_end = 110 + name_len;
			let name = std::str::from_utf8(&bytes[110..name_end - 1])
				.unwrap()
				.to_owned();
			assert_eq!(bytes[name_end - 1], 0);
			let data_start = name_end.next_multiple_of(4);
			let data = bytes[data_start..data_start + size].to_vec();
			let next = (data_start + size).next_multiple_of(4);
			bytes = &bytes[next..];
			if name == TRAILER {
				assert!(bytes.is_empty(), "data after trailer");
				return entries;
			}
			entries.push(Entry { name, mode, rdev, data });
		}
	}

	#[test]
	fn round_trip_preserves_names_modes_devices_and_odd_sized_data() {
		let agent: Vec<u8> = (0..4097u32).map(|i| (i % 251) as u8).collect();
		let image = unpack_initramfs(&agent).unwrap();
		assert_eq!(image.len() % 4, 0);
		let entries = parse(&image);
		let init = entries.iter().find(|e| e.name == "init").unwrap();
		assert_eq!(init.mode, S_IFREG | 0o755);
		assert_eq!(init.data, agent);
		let console = entries.iter().find(|e| e.name == "dev/console").unwrap();
		assert_eq!((console.mode, console.rdev), (S_IFCHR | 0o600, (5, 1)));
		let tmp = entries.iter().find(|e| e.name == "tmp").unwrap();
		assert_eq!(tmp.mode, S_IFDIR | 0o1777);
		assert!(entries.iter().any(|e| e.name == "mnt/layers"));
		// Parents precede children so the kernel can create them in order.
		let pos = |n: &str| entries.iter().position(|e| e.name == n).unwrap();
		assert!(pos("mnt") < pos("mnt/root"));
	}

	#[test]
	fn names_of_every_length_stay_aligned() {
		let mut cpio = CpioWriter::new(Vec::new());
		for len in 1..=9 {
			cpio
				.file(&"a".repeat(len), 0o644, &vec![b'x'; len])
				.unwrap();
		}
		let entries = parse(&cpio.finish().unwrap());
		assert_eq!(entries.len(), 9);
		for (i, entry) in entries.iter().enumerate() {
			assert_eq!(entry.name.len(), i + 1);
			assert_eq!(entry.data.len(), i + 1);
		}
	}

	#[test]
	fn rejects_empty_and_nul_names() {
		let mut cpio = CpioWriter::new(Vec::new());
		assert!(cpio.file("/", 0o644, b"").is_err());
		assert!(cpio.file("a\0b", 0o644, b"").is_err());
	}
}
