//! Minimal rootfs provisioning for the GC4 `provision` op (template builds).
//!
//! Creates the E2B default user, file layout, and the agent binary.
//! Distro-specific work (packages, init/service units) is done by the node
//! through `exec`.

use std::{
	collections::{BTreeMap, HashSet},
	fmt::Write as _,
	fs::{self, OpenOptions},
	io::{self, Write},
	os::unix::fs::{OpenOptionsExt, PermissionsExt, chown},
	path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// `provision` request (see `proto` module docs).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProvisionRequest {
	/// Root of the target filesystem.
	pub root:       PathBuf,
	/// Login name of the default user.
	pub user:       String,
	/// Preferred uid (and gid); the next free id ≥ 1000 is used if taken.
	pub uid:        u32,
	/// Grant passwordless sudo when sudo is installed.
	pub sudo:       bool,
	/// Guest hostname for `/etc/hostname` and `/etc/hosts`.
	pub hostname:   String,
	/// Agent binary to install.
	pub agent_src:  PathBuf,
	/// Install path of the agent inside `root` (absolute in guest terms).
	pub agent_dest: PathBuf,
	/// `KEY=VALUE` lines written to `/.e2b` (`ENV_ID`, `TEMPLATE_ID`,
	/// `BUILD_ID`).
	pub metadata:   BTreeMap<String, String>,
}

impl Default for ProvisionRequest {
	fn default() -> Self {
		Self {
			root:       PathBuf::from("/"),
			user:       "user".into(),
			uid:        1000,
			sudo:       true,
			hostname:   "vibemon".into(),
			agent_src:  PathBuf::from("/proc/self/exe"),
			agent_dest: PathBuf::from("/usr/bin/envd"),
			metadata:   BTreeMap::new(),
		}
	}
}

/// Resolved user, returned in the `provision` RESP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProvisionedUser {
	pub user:  String,
	pub uid:   u32,
	pub gid:   u32,
	pub home:  String,
	pub shell: String,
}

/// Run every provisioning step inside `request.root`.
pub fn provision(request: &ProvisionRequest) -> io::Result<ProvisionedUser> {
	if request.user.is_empty() || request.user.contains([':', '\n', '/']) {
		return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid user name"));
	}
	let root = &request.root;
	let etc = root.join("etc");
	fs::create_dir_all(&etc)?;
	let user = ensure_user(root, request)?;
	if request.sudo {
		grant_sudo(root, &user.user)?;
	}
	install_agent(root, &request.agent_src, &request.agent_dest)?;

	fs::write(etc.join("hostname"), format!("{}\n", request.hostname))?;
	fs::write(
		etc.join("hosts"),
		format!(
			"127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n127.0.1.1\t{}\n",
			request.hostname
		),
	)?;
	// Like upstream's rootfs layer: always a regular file. Images often ship a
	// symlink into /run (systemd-resolved) that dangles in the VM, which makes
	// every later write fail with ENOENT.
	let resolv = etc.join("resolv.conf");
	match fs::symlink_metadata(&resolv) {
		Ok(meta) if meta.file_type().is_symlink() => {
			fs::remove_file(&resolv)?;
			fs::write(&resolv, "nameserver 1.1.1.1\nnameserver 8.8.8.8\n")?;
		},
		Ok(_) => {},
		Err(_) => fs::write(&resolv, "nameserver 1.1.1.1\nnameserver 8.8.8.8\n")?,
	}
	for (dir, mode) in [("tmp", 0o1777), ("run/e2b", 0o755), ("code", 0o777), ("usr/local", 0o777)] {
		let path = root.join(dir);
		fs::create_dir_all(&path)?;
		fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
	}
	let mut e2b = String::new();
	for (key, value) in &request.metadata {
		let _ = writeln!(e2b, "{key}={value}");
	}
	fs::write(root.join(".e2b"), e2b)?;
	Ok(user)
}

/// Create (or reuse) the user, its primary group, shadow entry, and home.
fn ensure_user(root: &Path, request: &ProvisionRequest) -> io::Result<ProvisionedUser> {
	let passwd_path = root.join("etc/passwd");
	let group_path = root.join("etc/group");
	let passwd = read_or_empty(&passwd_path)?;
	let group = read_or_empty(&group_path)?;
	if let Some(fields) = find_entry(&passwd, &request.user)
		&& fields.len() >= 7
	{
		let parse = |v: &str| {
			v.parse::<u32>()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad passwd entry"))
		};
		return Ok(ProvisionedUser {
			user:  request.user.clone(),
			uid:   parse(fields[2])?,
			gid:   parse(fields[3])?,
			home:  fields[5].to_owned(),
			shell: fields[6].to_owned(),
		});
	}
	let uid = next_free_id(&used_ids(&passwd), request.uid);
	let existing_group =
		find_entry(&group, &request.user).and_then(|f| f.get(2)?.parse::<u32>().ok());
	let gid = if let Some(gid) = existing_group {
		gid
	} else {
		let gid = next_free_id(&used_ids(&group), uid);
		append_line(&group_path, &format!("{}:x:{gid}:", request.user))?;
		let gshadow = root.join("etc/gshadow");
		if gshadow.exists() {
			append_line(&gshadow, &format!("{}:!::", request.user))?;
		}
		gid
	};
	let shell = if fs::symlink_metadata(root.join("bin/bash")).is_ok() {
		"/bin/bash"
	} else {
		"/bin/sh"
	};
	let home = format!("/home/{}", request.user);
	append_line(&passwd_path, &format!("{}:x:{uid}:{gid}::{home}:{shell}", request.user))?;
	let shadow = root.join("etc/shadow");
	if shadow.exists() {
		// Empty password field = passwordless (`passwd -d`).
		append_line(&shadow, &format!("{}::19000:0:99999:7:::", request.user))?;
	}
	let home_path = root.join(home.trim_start_matches('/'));
	fs::create_dir_all(&home_path)?;
	copy_skel(&root.join("etc/skel"), &home_path, uid, gid)?;
	chown(&home_path, Some(uid), Some(gid))?;
	fs::set_permissions(&home_path, fs::Permissions::from_mode(0o755))?;
	Ok(ProvisionedUser { user: request.user.clone(), uid, gid, home, shell: shell.to_owned() })
}

/// Drop-in NOPASSWD rule plus membership in `sudo`/`wheel` when sudo exists.
fn grant_sudo(root: &Path, user: &str) -> io::Result<()> {
	let installed = ["etc/sudoers", "usr/bin/sudo", "bin/sudo"]
		.iter()
		.any(|p| fs::symlink_metadata(root.join(p)).is_ok());
	if !installed {
		return Ok(());
	}
	let dir = root.join("etc/sudoers.d");
	fs::create_dir_all(&dir)?;
	fs::set_permissions(&dir, fs::Permissions::from_mode(0o750))?;
	let mut file = OpenOptions::new()
		.write(true)
		.create(true)
		.truncate(true)
		.mode(0o440)
		.open(dir.join("vmon-user"))?;
	file.write_all(format!("{user} ALL=(ALL:ALL) NOPASSWD: ALL\n").as_bytes())?;
	fs::set_permissions(dir.join("vmon-user"), fs::Permissions::from_mode(0o440))?;
	let group_path = root.join("etc/group");
	let group = read_or_empty(&group_path)?;
	let updated = add_member(&group, &["sudo", "wheel"], user);
	if updated != group {
		fs::write(&group_path, updated)?;
	}
	Ok(())
}

/// Copy the agent binary to `<root>/<dest>` atomically, mode 0755.
fn install_agent(root: &Path, src: &Path, dest: &Path) -> io::Result<()> {
	let target = root.join(dest.strip_prefix("/").unwrap_or(dest));
	let parent = target.parent().unwrap_or(root);
	fs::create_dir_all(parent)?;
	let tmp = parent.join(".vmon-agent.tmp");
	fs::copy(src, &tmp)?;
	fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))?;
	fs::rename(&tmp, &target)
}

/// Copy `/etc/skel` into a fresh home (no overwrite), owned by the user.
fn copy_skel(skel: &Path, home: &Path, uid: u32, gid: u32) -> io::Result<()> {
	let Ok(entries) = fs::read_dir(skel) else {
		return Ok(());
	};
	for entry in entries {
		let entry = entry?;
		let to = home.join(entry.file_name());
		if fs::symlink_metadata(&to).is_ok() {
			continue;
		}
		let kind = entry.file_type()?;
		if kind.is_dir() {
			fs::create_dir(&to)?;
			copy_skel(&entry.path(), &to, uid, gid)?;
		} else if kind.is_symlink() {
			std::os::unix::fs::symlink(fs::read_link(entry.path())?, &to)?;
		} else {
			fs::copy(entry.path(), &to)?;
		}
		std::os::unix::fs::lchown(&to, Some(uid), Some(gid))?;
	}
	Ok(())
}

fn read_or_empty(path: &Path) -> io::Result<String> {
	match fs::read_to_string(path) {
		Ok(text) => Ok(text),
		Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(String::new()),
		Err(err) => Err(err),
	}
}

fn append_line(path: &Path, line: &str) -> io::Result<()> {
	let mut text = read_or_empty(path)?;
	if !text.is_empty() && !text.ends_with('\n') {
		text.push('\n');
	}
	text.push_str(line);
	text.push('\n');
	fs::write(path, text)
}

/// Fields of the `name:` entry in a passwd/group-style file.
fn find_entry<'a>(text: &'a str, name: &str) -> Option<Vec<&'a str>> {
	text
		.lines()
		.map(|line| line.split(':').collect::<Vec<_>>())
		.find(|fields| fields.first() == Some(&name))
}

/// Numeric ids (third field) used in a passwd/group-style file.
fn used_ids(text: &str) -> HashSet<u32> {
	text
		.lines()
		.filter_map(|line| line.split(':').nth(2)?.parse().ok())
		.collect()
}

/// `wanted` if free, else the first free id ≥ max(wanted, 1000).
fn next_free_id(used: &HashSet<u32>, wanted: u32) -> u32 {
	if !used.contains(&wanted) {
		return wanted;
	}
	(wanted.max(1000)..u32::MAX)
		.find(|id| !used.contains(id))
		.unwrap_or(u32::MAX)
}

/// Add `user` to the member list of each named group that exists.
fn add_member(text: &str, groups: &[&str], user: &str) -> String {
	let mut out = String::with_capacity(text.len() + 16);
	for line in text.lines() {
		let fields: Vec<&str> = line.split(':').collect();
		if fields.len() == 4
			&& groups.contains(&fields[0])
			&& !fields[3].split(',').any(|m| m == user)
		{
			let members = if fields[3].is_empty() {
				user.to_owned()
			} else {
				format!("{},{user}", fields[3])
			};
			let _ = write!(out, "{}:{}:{}:{members}", fields[0], fields[1], fields[2]);
		} else {
			out.push_str(line);
		}
		out.push('\n');
	}
	out
}

#[cfg(test)]
mod tests {
	use std::os::unix::fs::MetadataExt;

	use super::*;

	fn rootfs(name: &str) -> PathBuf {
		let root = std::env::temp_dir().join(format!("vmon-prov-{name}-{}", std::process::id()));
		let _ = fs::remove_dir_all(&root);
		fs::create_dir_all(root.join("etc/skel")).unwrap();
		fs::create_dir_all(root.join("bin")).unwrap();
		fs::write(
			root.join("etc/passwd"),
			"root:x:0:0:root:/root:/bin/sh\nnode:x:1000:1000::/home/node:/bin/sh",
		)
		.unwrap();
		fs::write(root.join("etc/group"), "root:x:0:\nnode:x:1000:\nsudo:x:27:\nwheel:x:10:root\n")
			.unwrap();
		fs::write(root.join("etc/shadow"), "root:*:19000:0:99999:7:::\n").unwrap();
		fs::write(root.join("etc/sudoers"), "").unwrap();
		fs::write(root.join("etc/skel/.bashrc"), "# rc").unwrap();
		fs::write(root.join("bin/bash"), "").unwrap();
		root
	}

	fn request(root: &Path) -> ProvisionRequest {
		let src = root.join("agent-src");
		fs::write(&src, b"\x7fELF").unwrap();
		ProvisionRequest {
			root: root.to_owned(),
			agent_src: src,
			hostname: "sbx".into(),
			metadata: BTreeMap::from([("TEMPLATE_ID".into(), "t1".into())]),
			..ProvisionRequest::default()
		}
	}

	#[test]
	fn creates_user_on_next_free_id_with_sudo_and_layout() {
		let root = rootfs("new");
		let user = provision(&request(&root)).unwrap();
		assert_eq!(user, ProvisionedUser {
			user:  "user".into(),
			uid:   1001,
			gid:   1001,
			home:  "/home/user".into(),
			shell: "/bin/bash".into(),
		});
		let passwd = fs::read_to_string(root.join("etc/passwd")).unwrap();
		assert!(passwd.contains(
			"\nnode:x:1000:1000::/home/node:/bin/sh\nuser:x:1001:1001::/home/user:/bin/bash\n"
		));
		assert!(
			fs::read_to_string(root.join("etc/shadow"))
				.unwrap()
				.contains("\nuser::")
		);
		let group = fs::read_to_string(root.join("etc/group")).unwrap();
		assert!(group.contains("sudo:x:27:user\n") && group.contains("wheel:x:10:root,user\n"));
		assert!(group.contains("user:x:1001:\n"));
		let sudoers = root.join("etc/sudoers.d/vmon-user");
		assert_eq!(fs::read_to_string(&sudoers).unwrap(), "user ALL=(ALL:ALL) NOPASSWD: ALL\n");
		assert_eq!(fs::metadata(&sudoers).unwrap().mode() & 0o777, 0o440);
		let home = fs::metadata(root.join("home/user")).unwrap();
		assert_eq!((home.uid(), home.gid()), (1001, 1001));
		assert_eq!(fs::metadata(root.join("home/user/.bashrc")).unwrap().uid(), 1001);
		let agent = fs::metadata(root.join("usr/bin/envd")).unwrap();
		assert_eq!(agent.mode() & 0o777, 0o755);
		assert_eq!(fs::metadata(root.join("tmp")).unwrap().mode() & 0o7777, 0o1777);
		assert_eq!(
			fs::read_to_string(root.join("etc/hosts"))
				.unwrap()
				.lines()
				.last(),
			Some("127.0.1.1\tsbx")
		);
		assert_eq!(fs::read_to_string(root.join(".e2b")).unwrap(), "TEMPLATE_ID=t1\n");
		assert!(root.join("etc/resolv.conf").exists());
		fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn existing_user_is_idempotent_and_no_sudo_without_sudo() {
		let root = rootfs("again");
		fs::remove_file(root.join("etc/sudoers")).unwrap();
		let req = request(&root);
		let first = provision(&req).unwrap();
		let passwd = fs::read_to_string(root.join("etc/passwd")).unwrap();
		let second = provision(&req).unwrap();
		assert_eq!(first, second);
		assert_eq!(fs::read_to_string(root.join("etc/passwd")).unwrap(), passwd);
		assert!(!root.join("etc/sudoers.d").exists());
		fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn dangling_resolv_conf_symlink_becomes_a_regular_file() {
		let root = rootfs("resolv");
		std::os::unix::fs::symlink(
			"../run/systemd/resolve/stub-resolv.conf",
			root.join("etc/resolv.conf"),
		)
		.unwrap();
		provision(&request(&root)).unwrap();
		let meta = fs::symlink_metadata(root.join("etc/resolv.conf")).unwrap();
		assert!(meta.file_type().is_file());
		assert!(
			fs::read_to_string(root.join("etc/resolv.conf"))
				.unwrap()
				.starts_with("nameserver ")
		);
		fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn rejects_hostile_user_names() {
		let root = rootfs("bad");
		let mut req = request(&root);
		req.user = "x:0:0".into();
		assert!(provision(&req).is_err());
		fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn id_and_group_helpers() {
		let used: HashSet<u32> = [1000, 1001, 5].into();
		assert_eq!(next_free_id(&used, 1000), 1002);
		assert_eq!(next_free_id(&used, 7), 7);
		assert_eq!(next_free_id(&used, 5), 1002);
		assert_eq!(add_member("sudo:x:27:user\n", &["sudo"], "user"), "sudo:x:27:user\n");
	}
}
