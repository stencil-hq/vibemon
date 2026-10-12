//! Guest user database access (`/etc/passwd` + `/etc/group` via libc, which
//! is static musl without NSS): resolve the user an RPC or file transfer runs
//! as, and map uid/gid back to names for `EntryInfo.owner/group`.

use std::{
	collections::HashMap,
	ffi::{CStr, CString},
	io,
	path::PathBuf,
};

/// A resolved account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
	pub name:   String,
	pub uid:    u32,
	pub gid:    u32,
	pub home:   PathBuf,
	pub shell:  String,
	/// Primary gid first, then supplementary groups.
	pub groups: Vec<u32>,
}

/// Look up `name`; `Ok(None)` when no such user exists.
pub fn lookup(name: &str) -> io::Result<Option<User>> {
	let Ok(cname) = CString::new(name) else {
		return Ok(None);
	};
	let mut buf = vec![0u8; 4096];
	loop {
		// SAFETY: zeroed passwd is a valid out-parameter.
		let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
		let mut result: *mut libc::passwd = std::ptr::null_mut();
		// SAFETY: all pointers are valid for the call; buf outlives pwd use.
		let rc = unsafe {
			libc::getpwnam_r(cname.as_ptr(), &mut pwd, buf.as_mut_ptr().cast(), buf.len(), &mut result)
		};
		if rc == libc::ERANGE && buf.len() < (1 << 20) {
			buf.resize(buf.len() * 2, 0);
			continue;
		}
		if rc != 0 && rc != libc::ENOENT && rc != libc::ESRCH {
			return Err(io::Error::from_raw_os_error(rc));
		}
		if result.is_null() {
			return Ok(None);
		}
		// SAFETY: getpwnam_r succeeded, so the string fields point into buf.
		let (home, shell) = unsafe { (cstr(pwd.pw_dir), cstr(pwd.pw_shell)) };
		let groups = group_list(&cname, pwd.pw_gid);
		return Ok(Some(User {
			name: name.to_owned(),
			uid: pwd.pw_uid,
			gid: pwd.pw_gid,
			home: PathBuf::from(if home.is_empty() {
				"/".to_owned()
			} else {
				home
			}),
			shell: if shell.is_empty() {
				"/bin/sh".to_owned()
			} else {
				shell
			},
			groups,
		}));
	}
}

/// # Safety
/// `p` must be null or a valid NUL-terminated string.
unsafe fn cstr(p: *const libc::c_char) -> String {
	if p.is_null() {
		return String::new();
	}
	// SAFETY: guaranteed by the caller.
	unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

fn group_list(name: &CStr, gid: u32) -> Vec<u32> {
	let mut n: libc::c_int = 32;
	loop {
		let mut groups = vec![0 as libc::gid_t; usize::try_from(n).unwrap_or(32)];
		let mut count = n;
		// SAFETY: groups has `count` slots; name is NUL-terminated.
		let rc = unsafe { libc::getgrouplist(name.as_ptr(), gid, groups.as_mut_ptr(), &mut count) };
		if rc >= 0 {
			groups.truncate(usize::try_from(count).unwrap_or(0));
			let mut out = vec![gid];
			out.extend(groups.into_iter().filter(|&g| g != gid));
			return out;
		}
		if count <= n || n > 65536 {
			return vec![gid];
		}
		n = count;
	}
}

/// Per-request uid/gid → name cache for `EntryInfo` owner/group.
#[derive(Default)]
pub struct NameCache {
	users:  HashMap<u32, String>,
	groups: HashMap<u32, String>,
}

impl NameCache {
	/// User name for `uid`, or the number when unknown (as Go's envd does).
	pub fn user(&mut self, uid: u32) -> String {
		self
			.users
			.entry(uid)
			.or_insert_with(|| user_name(uid).unwrap_or_else(|| uid.to_string()))
			.clone()
	}

	/// Group name for `gid`, or the number when unknown.
	pub fn group(&mut self, gid: u32) -> String {
		self
			.groups
			.entry(gid)
			.or_insert_with(|| group_name(gid).unwrap_or_else(|| gid.to_string()))
			.clone()
	}
}

fn user_name(uid: u32) -> Option<String> {
	let mut buf = vec![0u8; 4096];
	// SAFETY: zeroed passwd is a valid out-parameter.
	let mut pwd: libc::passwd = unsafe { std::mem::zeroed() };
	let mut result: *mut libc::passwd = std::ptr::null_mut();
	// SAFETY: valid pointers for the call.
	let rc =
		unsafe { libc::getpwuid_r(uid, &mut pwd, buf.as_mut_ptr().cast(), buf.len(), &mut result) };
	if rc != 0 || result.is_null() {
		return None;
	}
	// SAFETY: success, so pw_name points into buf.
	Some(unsafe { cstr(pwd.pw_name) })
}

fn group_name(gid: u32) -> Option<String> {
	let mut buf = vec![0u8; 16384];
	// SAFETY: zeroed group is a valid out-parameter.
	let mut grp: libc::group = unsafe { std::mem::zeroed() };
	let mut result: *mut libc::group = std::ptr::null_mut();
	// SAFETY: valid pointers for the call.
	let rc =
		unsafe { libc::getgrgid_r(gid, &mut grp, buf.as_mut_ptr().cast(), buf.len(), &mut result) };
	if rc != 0 || result.is_null() {
		return None;
	}
	// SAFETY: success, so gr_name points into buf.
	Some(unsafe { cstr(grp.gr_name) })
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root_resolves_and_unknown_user_is_none() {
		let root = lookup("root")
			.unwrap()
			.expect("root exists in every test image");
		assert_eq!((root.uid, root.gid), (0, 0));
		assert_eq!(root.groups[0], 0);
		assert!(lookup("no-such-user-envd-test").unwrap().is_none());
		assert!(lookup("bad\0name").unwrap().is_none());
		let mut cache = NameCache::default();
		assert_eq!(cache.user(0), "root");
		assert_eq!(cache.user(4_000_000), "4000000");
	}
}
