//! The REAPI messages the Bazel cache validates action results with
//! (`build/bazel/remote/execution/v2/remote_execution.proto`), read from the
//! protobuf wire format: only the CAS digests an `ActionResult` and a `Tree`
//! reference. Malformed input is `None`.

use super::records::valid_digest;

/// Largest protobuf field number.
const MAX_FIELD: u64 = (1 << 29) - 1;

/// A referenced CAS entry: sha256 (lowercase hex) and size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digest<'a> {
	pub hash: &'a str,
	pub size: u64,
}

/// A `Digest` as encoded (size as the raw varint of its `int64`).
#[derive(Clone, Copy, Default)]
struct RawDigest<'a> {
	hash: &'a [u8],
	size: u64,
}

/// A field's value; fixed-width values are skipped.
#[derive(Clone, Copy)]
enum Value<'a> {
	Varint(u64),
	Bytes(&'a [u8]),
	Fixed,
}

impl<'a> Value<'a> {
	const fn bytes(self) -> Option<&'a [u8]> {
		if let Self::Bytes(bytes) = self {
			Some(bytes)
		} else {
			None
		}
	}

	const fn varint(self) -> Option<u64> {
		if let Self::Varint(value) = self {
			Some(value)
		} else {
			None
		}
	}
}

/// The CAS entries an encoded `ActionResult` references, `(blobs, trees)`:
/// output files with stdout and stderr, and the `Tree`s of output
/// directories. Empty entries (size 0) are left out: they need no upload.
pub fn action_result(buf: &[u8]) -> Option<(Vec<Digest<'_>>, Vec<Digest<'_>>)> {
	let (mut blobs, mut trees) = (Vec::new(), Vec::new());
	let (mut stdout, mut stderr) = (None, None);
	fields(buf, |number, value| {
		match number {
			// output_files: OutputFile.digest
			2 => push(&mut blobs, nested_digest(value.bytes()?, 2)?)?,
			// output_directories: OutputDirectory.tree_digest
			3 => push(&mut trees, nested_digest(value.bytes()?, 3)?)?,
			6 => merge_digest(&mut stdout, value.bytes()?)?,
			8 => merge_digest(&mut stderr, value.bytes()?)?,
			_ => {},
		}
		Some(())
	})?;
	for digest in [stdout, stderr].into_iter().flatten() {
		push(&mut blobs, digest)?;
	}
	Some((blobs, trees))
}

/// Appends the files an encoded `Tree` lists (in its root and children
/// `Directory`s) to `into`, leaving out empty ones.
pub fn tree_files<'a>(buf: &'a [u8], into: &mut Vec<Digest<'a>>) -> Option<()> {
	fields(buf, |number, value| {
		if matches!(number, 1 | 2) {
			fields(value.bytes()?, |number, value| {
				// Directory.files: FileNode.digest
				if number == 1 {
					push(into, nested_digest(value.bytes()?, 2)?)?;
				}
				Some(())
			})?;
		}
		Some(())
	})
}

/// Appends `digest` unless empty; its hash must be a sha256.
fn push<'a>(into: &mut Vec<Digest<'a>>, digest: RawDigest<'a>) -> Option<()> {
	if digest.size > 0 {
		let hash = str::from_utf8(digest.hash)
			.ok()
			.filter(|hash| valid_digest(hash))?;
		into.push(Digest { hash, size: digest.size });
	}
	Some(())
}

/// The (required) digest in field `number` of a message.
fn nested_digest(buf: &[u8], number: u64) -> Option<RawDigest<'_>> {
	let mut digest = None;
	fields(buf, |field, value| {
		if field == number {
			merge_digest(&mut digest, value.bytes()?)?;
		}
		Some(())
	})?;
	digest
}

/// Merges an encoded `Digest` into `into`, as a repeated message field merges.
fn merge_digest<'a>(into: &mut Option<RawDigest<'a>>, buf: &'a [u8]) -> Option<()> {
	let digest = into.get_or_insert_default();
	fields(buf, |number, value| {
		match number {
			1 => digest.hash = value.bytes()?,
			2 => digest.size = value.varint()?,
			_ => {},
		}
		Some(())
	})
}

/// Calls `field` with the number and value of each field of the message in
/// `buf`; `None` if the encoding is malformed or `field` returns `None`.
fn fields<'a>(
	mut buf: &'a [u8],
	mut field: impl FnMut(u64, Value<'a>) -> Option<()>,
) -> Option<()> {
	while !buf.is_empty() {
		let key = varint(&mut buf)?;
		let value = match key & 7 {
			0 => Value::Varint(varint(&mut buf)?),
			1 => {
				buf = buf.get(8..)?;
				Value::Fixed
			},
			2 => {
				let len = usize::try_from(varint(&mut buf)?).ok()?;
				let (bytes, rest) = buf.split_at_checked(len)?;
				buf = rest;
				Value::Bytes(bytes)
			},
			5 => {
				buf = buf.get(4..)?;
				Value::Fixed
			},
			// Groups (3, 4) are not used by these messages; 6 and 7 do not exist.
			_ => return None,
		};
		let number = key >> 3;
		if !(1..=MAX_FIELD).contains(&number) {
			return None;
		}
		field(number, value)?;
	}
	Some(())
}

/// Reads a base-128 varint (at most 10 bytes, fitting 64 bits).
fn varint(buf: &mut &[u8]) -> Option<u64> {
	let mut value = 0;
	for shift in (0..64).step_by(7) {
		let (&byte, rest) = buf.split_first()?;
		*buf = rest;
		value |= u64::from(byte & 0x7f) << shift;
		if byte < 0x80 {
			return (shift < 63 || byte <= 1).then_some(value);
		}
	}
	None
}

/// Encoders for tests.
#[cfg(test)]
pub mod testing {
	use sha2::{Digest as _, Sha256};

	fn varint(out: &mut Vec<u8>, mut value: u64) {
		while value >= 0x80 {
			out.push(value as u8 | 0x80);
			value >>= 7;
		}
		out.push(value as u8);
	}

	fn bytes(out: &mut Vec<u8>, number: u64, data: &[u8]) {
		varint(out, (number << 3) | 2);
		varint(out, data.len() as u64);
		out.extend_from_slice(data);
	}

	/// The encoded `Digest` of `blob`.
	pub fn digest(blob: &[u8]) -> Vec<u8> {
		let mut out = Vec::new();
		bytes(&mut out, 1, hex::encode(Sha256::digest(blob)).as_bytes());
		if !blob.is_empty() {
			varint(&mut out, 2 << 3);
			varint(&mut out, blob.len() as u64);
		}
		out
	}

	/// A `FileNode` or `OutputFile` (digest in field 2) named `path`.
	fn file(path: &str, blob: &[u8]) -> Vec<u8> {
		let mut out = Vec::new();
		bytes(&mut out, 1, path.as_bytes());
		bytes(&mut out, 2, &digest(blob));
		// is_executable
		varint(&mut out, 4 << 3);
		varint(&mut out, 1);
		out
	}

	/// An `ActionResult` with output files `files`, output directories whose
	/// encoded `Tree`s are `trees`, and stdout (stderr is empty).
	pub fn action_result(files: &[&[u8]], trees: &[&[u8]], stdout: &[u8]) -> Vec<u8> {
		let mut out = Vec::new();
		for (n, blob) in files.iter().enumerate() {
			bytes(&mut out, 2, &file(&format!("bazel-out/f{n}"), blob));
		}
		for (n, tree) in trees.iter().enumerate() {
			let mut dir = Vec::new();
			bytes(&mut dir, 1, format!("bazel-out/d{n}").as_bytes());
			bytes(&mut dir, 3, &digest(tree));
			// is_topologically_sorted
			varint(&mut dir, 4 << 3);
			varint(&mut dir, 1);
			bytes(&mut out, 3, &dir);
		}
		bytes(&mut out, 6, &digest(stdout));
		bytes(&mut out, 8, &digest(b""));
		// execution_metadata { worker }
		let mut metadata = Vec::new();
		bytes(&mut metadata, 1, b"vmon-ci");
		bytes(&mut out, 9, &metadata);
		out
	}

	/// A `Tree` with `root` files in its root and `child` files in a child.
	pub fn tree(root: &[&[u8]], child: &[&[u8]]) -> Vec<u8> {
		let dir = |files: &[&[u8]]| {
			let mut out = Vec::new();
			for (n, blob) in files.iter().enumerate() {
				bytes(&mut out, 1, &file(&format!("f{n}"), blob));
			}
			out
		};
		let mut out = Vec::new();
		bytes(&mut out, 1, &dir(root));
		bytes(&mut out, 2, &dir(child));
		out
	}
}

#[cfg(test)]
mod tests {
	use sha2::{Digest as _, Sha256};

	use super::{testing, *};

	fn sha(data: &[u8]) -> String {
		hex::encode(Sha256::digest(data))
	}

	#[test]
	fn action_results_and_trees_list_their_non_empty_blobs() {
		let tree = testing::tree(&[b"root file", b""], &[b"child file"]);
		let result = testing::action_result(&[b"out", b""], &[&tree], b"log");
		let (blobs, trees) = action_result(&result).unwrap();
		let hashes = |digests: &[Digest<'_>]| {
			digests
				.iter()
				.map(|digest| (digest.hash.to_owned(), digest.size))
				.collect::<Vec<_>>()
		};
		assert_eq!(hashes(&blobs), [(sha(b"out"), 3), (sha(b"log"), 3)]);
		assert_eq!(hashes(&trees), [(sha(&tree), tree.len() as u64)]);
		let mut files = Vec::new();
		tree_files(&tree, &mut files).unwrap();
		assert_eq!(hashes(&files), [(sha(b"root file"), 9), (sha(b"child file"), 10)]);
		assert_eq!(action_result(&[]), Some((Vec::new(), Vec::new())), "no outputs");
	}

	#[test]
	fn malformed_messages_are_rejected() {
		let result = testing::action_result(&[b"out"], &[], b"");
		for bad in [
			&result[..result.len() - 1], // truncated
			b"action result".as_slice(), // a group
			&[0x12, 0x80],               // unterminated length
			&[0x12, 0x05, 0x00],         // length past the end
			&[0x10, 0x01],               // output_files as a varint
			&[0x12, 0x00],               // an output file without a digest
			&[0x02, 0x00],               // field 0
			&[0x20, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02], // varint over 64 bits
			&[0x12, 0x08, 0x12, 0x06, 0x0a, 0x02, b'a', b'b', 0x10, 0x01], // hash not a sha256
		] {
			assert_eq!(action_result(bad), None, "{bad:02x?}");
		}
		assert_eq!(
			tree_files(&[0x0a, 0x02, 0x0a, 0x00], &mut Vec::new()),
			None,
			"file without a digest"
		);
		// Fixed-width and unknown fields are skipped.
		let mut skipped = vec![0x51, 1, 2, 3, 4, 5, 6, 7, 8, 0x55, 1, 2, 3, 4, 0x20, 0x01];
		skipped.extend_from_slice(&result);
		assert_eq!(action_result(&skipped).unwrap().0.len(), 1);
	}
}
