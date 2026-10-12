//! E2B v2 build steps (e2b-api reference Part D §4).
//!
//! Parses the wire `{type, args}` form and renders the guest shell that
//! implements each step with upstream semantics. Rendering is pure so it is
//! unit-tested on the host; the pipeline runs the scripts through GC4 `exec`.

use std::{collections::BTreeMap, fmt::Write as _};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::api::BuildStep;

/// Directory (in the steps VM) where the file-context share is mounted.
pub const CONTEXT_MOUNT: &str = "/run/vmon-context";
/// Admin-group record written by provisioning (sourced by user scripts).
const DISTRO_ENV: &str = "/usr/local/share/e2b/distro.env";
/// Directories upstream appends to an existing `PATH` for RUN steps.
const PATH_SUFFIX: &str = ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Mutable build state carried across layers (and stored with cached layers).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildContext {
	/// Current `USER` (steps run as this user unless overridden).
	pub user:      String,
	/// Current `WORKDIR`; `None` = the user's home.
	pub workdir:   Option<String>,
	/// Accumulated environment (image `Env` + `ENV`/`ARG` steps).
	pub env:       BTreeMap<String, String>,
	/// Kernel `init=` for this rootfs.
	pub boot_init: String,
	/// Whether `/bin/bash` exists (RUN uses `bash -l -c`, else `sh -c`).
	pub has_bash:  bool,
}

impl BuildContext {
	/// Environment for a guest command: the context env with upstream's PATH
	/// rule.
	pub fn command_env(&self) -> BTreeMap<String, String> {
		let mut env = self.env.clone();
		let path = match env.get("PATH") {
			Some(path) if !path.is_empty() => format!("{path}{PATH_SUFFIX}"),
			_ => DEFAULT_PATH.to_owned(),
		};
		env.insert("PATH".to_owned(), path);
		env
	}

	/// argv running `script` through the template shell, starting in the
	/// workdir (or `$HOME` when none is set).
	pub fn shell_argv(&self, script: &str) -> Vec<String> {
		let shell = if self.has_bash {
			"/bin/bash -l -c"
		} else {
			"/bin/sh -c"
		};
		let cd = match &self.workdir {
			Some(dir) => format!("cd {} || exit 1", sh_quote(dir)),
			None => "cd \"$HOME\" 2>/dev/null || cd /".to_owned(),
		};
		vec![
			"/bin/sh".to_owned(),
			"-c".to_owned(),
			format!("{cd}; exec {shell} \"$0\""),
			script.to_owned(),
		]
	}
}

/// A parsed build step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
	/// `RUN [cmd, user?]`.
	Run { cmd: String, user: Option<String> },
	/// `COPY`/`ADD [src, dest, user, mode]`.
	Copy(CopySpec),
	/// `ENV`/`ARG [k1, v1, …]`.
	Env(Vec<(String, String)>),
	/// `WORKDIR [path]`.
	Workdir(String),
	/// `USER [name, "true"?]`; `sudo` grants passwordless sudo.
	User { name: String, sudo: bool },
}

impl Step {
	/// Parses a wire step (type is case-insensitive; `ADD` = `COPY`, `ARG` =
	/// `ENV`).
	pub fn parse(step: &BuildStep) -> Result<Self> {
		let kind = step.kind.to_ascii_uppercase();
		let args = &step.args;
		match kind.as_str() {
			"RUN" => match args.as_slice() {
				[cmd] => Ok(Self::Run { cmd: cmd.clone(), user: None }),
				[cmd, user] => {
					Ok(Self::Run { cmd: cmd.clone(), user: (!user.is_empty()).then(|| user.clone()) })
				},
				_ => bail!("RUN expects [command, user?], got {} args", args.len()),
			},
			"COPY" | "ADD" => {
				if step.files_hash.as_deref().is_none_or(str::is_empty) {
					bail!("{kind} requires filesHash");
				}
				Ok(Self::Copy(CopySpec::parse(args)?))
			},
			"ENV" | "ARG" => {
				if kind == "ARG" && args.len() == 1 {
					return Ok(Self::Env(vec![(args[0].clone(), String::new())]));
				}
				if args.is_empty() || !args.len().is_multiple_of(2) {
					bail!("{kind} expects key/value pairs, got {} args", args.len());
				}
				let pairs: Vec<(String, String)> = args
					.chunks(2)
					.map(|kv| (kv[0].clone(), kv[1].clone()))
					.collect();
				if let Some((key, _)) = pairs.iter().find(|(k, _)| !valid_env_key(k)) {
					bail!("invalid environment variable name {key:?}");
				}
				Ok(Self::Env(pairs))
			},
			"WORKDIR" => match args.as_slice() {
				[path] if !path.is_empty() => Ok(Self::Workdir(path.clone())),
				_ => bail!("WORKDIR expects [path]"),
			},
			"USER" => match args.as_slice() {
				[name] if !name.is_empty() => Ok(Self::User { name: name.clone(), sudo: false }),
				[name, flag] if !name.is_empty() => {
					Ok(Self::User { name: name.clone(), sudo: flag == "true" })
				},
				_ => bail!("USER expects [user]"),
			},
			_ => bail!("command type {} is not implemented", step.kind),
		}
	}
}

fn valid_env_key(key: &str) -> bool {
	!key.is_empty() && !key.contains(['=', '\0'])
}

/// Arguments of a COPY step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopySpec {
	/// Path inside the file-context archive to copy: a file, or a directory
	/// whose contents are merged (globs are stripped to their base directory,
	/// the archive only holds matching files).
	pub source: String,
	/// Destination as given (absolute, or relative to WORKDIR/home); a
	/// trailing `/` means "into this directory".
	pub dest:   String,
	/// `user[:group]` override; `None` = the current USER.
	pub owner:  Option<String>,
	/// Permission bits to apply to copied entries.
	pub mode:   Option<u32>,
}

impl CopySpec {
	/// Parses `[src, dest, user, mode]` (user and mode may be empty or absent).
	pub fn parse(args: &[String]) -> Result<Self> {
		let (src, dest) = match args {
			[src, dest, ..] if args.len() <= 4 => (src, dest),
			_ => bail!("COPY expects [src, dest, user?, mode?], got {} args", args.len()),
		};
		if dest.is_empty() {
			bail!("COPY destination is empty");
		}
		let owner = args.get(2).filter(|u| !u.is_empty()).cloned();
		if let Some(owner) = &owner
			&& (owner.contains(char::is_whitespace)
				|| owner.split(':').count() > 2
				|| owner.starts_with(':'))
		{
			bail!("invalid COPY owner {owner:?}");
		}
		let mode = match args.get(3).map(String::as_str) {
			None | Some("") => None,
			Some(mode) => Some(parse_mode(mode)?),
		};
		Ok(Self { source: glob_base(src)?, dest: dest.clone(), owner, mode })
	}

	/// `user:group` for chown: an explicit `u:g` as given, `u` → `u:u`, and
	/// the current user (`cur:cur`) when no owner was requested.
	pub fn chown_spec(&self, current_user: &str) -> String {
		match &self.owner {
			Some(owner) if owner.contains(':') => owner.clone(),
			Some(owner) => format!("{owner}:{owner}"),
			None => format!("{current_user}:{current_user}"),
		}
	}

	/// Guest script (run as root) copying `<context>/<hash>/<source>` to the
	/// destination with owner/mode applied to every copied entry only.
	pub fn script(&self, files_hash: &str, ctx: &BuildContext) -> String {
		let source = if self.source.is_empty() {
			format!("{CONTEXT_MOUNT}/{files_hash}")
		} else {
			format!("{CONTEXT_MOUNT}/{files_hash}/{}", self.source)
		};
		let dest = match (&ctx.workdir, self.dest.starts_with('/')) {
			(_, true) => sh_quote(&self.dest),
			(Some(workdir), false) => {
				sh_quote(&format!("{}/{}", workdir.trim_end_matches('/'), self.dest))
			},
			(None, false) => {
				format!("\"$(user_home {})/\"{}", sh_quote(&ctx.user), sh_quote(&self.dest))
			},
		};
		let mode = self.mode.map(|m| format!("{m:o}")).unwrap_or_default();
		format!(
			r#"set -eu
user_home() {{ h=$(awk -F: -v u="$1" '$1==u {{print $6}}' /etc/passwd); echo "${{h:-/root}}"; }}
src={src}
dest={dest}
O={owner}
M={mode}
export O M
if [ ! -e "$src" ] && [ ! -L "$src" ]; then
	echo "COPY source not found in the uploaded files" >&2
	exit 1
fi
if [ -d "$src" ] && [ ! -L "$src" ]; then
	created=0
	[ -e "$dest" ] || created=1
	mkdir -p "$dest"
	cp -R -P -p "$src/." "$dest/"
	D=$dest
	export D
	cd "$src"
	find . -mindepth 1 -exec sh -c 'for p; do t="$D/${{p#./}}"; [ -L "$p" ] || m=${{M:-$(stat -c %a "$p")}}; chown -h "$O" "$t"; [ -L "$t" ] || chmod "$m" "$t"; done' sh {{}} +
	if [ "$created" = 1 ]; then chown -h "$O" "$dest"; fi
else
	case "$dest" in
	*/) target="$dest$(basename "$src")" ;;
	*) if [ -d "$dest" ] && [ ! -L "$dest" ]; then target="$dest/$(basename "$src")"; else target="$dest"; fi ;;
	esac
	mkdir -p "$(dirname "$target")"
	rm -f "$target"
	cp -P -p "$src" "$target"
	[ -L "$src" ] || m=${{M:-$(stat -c %a "$src")}}
	chown -h "$O" "$target"
	# chown clears setuid/setgid, so the mode is (re)applied afterwards.
	[ -L "$target" ] || chmod "$m" "$target"
fi
"#,
			src = sh_quote(&source),
			owner = sh_quote(&self.chown_spec(&ctx.user)),
			mode = sh_quote(&mode),
		)
	}
}

/// Parses a 3–4 digit octal mode (`"0755"`, `"644"`).
pub fn parse_mode(mode: &str) -> Result<u32> {
	let digits = mode.trim();
	if !(1..=4).contains(&digits.len()) || !digits.bytes().all(|b| (b'0'..=b'7').contains(&b)) {
		bail!("invalid COPY mode {mode:?} (expected octal like 0755)");
	}
	Ok(u32::from_str_radix(digits, 8)?)
}

/// Strips a doublestar glob to its literal base directory (upstream
/// `doublestar.SplitPattern`); literal paths are returned cleaned.
/// Rejects absolute paths and `..` escapes.
pub fn glob_base(src: &str) -> Result<String> {
	if src.starts_with('/') {
		bail!("COPY source {src:?} must be relative to the file context");
	}
	let meta = src.find(['*', '?', '[', '{']);
	let literal = match meta {
		Some(index) => src[..index].rfind('/').map_or("", |slash| &src[..slash]),
		None => src,
	};
	let mut parts = Vec::new();
	for part in literal.split('/') {
		match part {
			"" | "." => {},
			".." => bail!("COPY source {src:?} escapes the file context"),
			part => parts.push(part),
		}
	}
	Ok(parts.join("/"))
}

/// Joins a WORKDIR argument onto the current workdir (default `/`) and
/// normalizes `.`/`..` lexically.
pub fn resolve_workdir(current: Option<&str>, path: &str) -> String {
	let joined = if path.starts_with('/') {
		path.to_owned()
	} else {
		format!("{}/{path}", current.unwrap_or("/"))
	};
	let mut parts: Vec<&str> = Vec::new();
	for part in joined.split('/') {
		match part {
			"" | "." => {},
			".." => {
				parts.pop();
			},
			part => parts.push(part),
		}
	}
	format!("/{}", parts.join("/"))
}

/// Guest script creating `dir` and chowning newly created ancestors to `user`.
pub fn workdir_script(dir: &str, user: &str) -> String {
	format!(
		r#"set -eu
d={dir}
u={user}
p="$d"
first=""
while [ ! -e "$p" ]; do first="$p"; p=$(dirname "$p"); done
mkdir -p "$d"
if [ -n "$first" ]; then chown -R "$u:$(id -g "$u")" "$first"; fi
"#,
		dir = sh_quote(dir),
		user = sh_quote(user),
	)
}

/// Guest script for `USER name [true]`: create the user if missing (bash
/// shell when present), and with `sudo` add it to the admin group, clear its
/// password, and grant passwordless sudo.
pub fn user_script(name: &str, sudo: bool) -> String {
	let mut script = format!(
		r#"set -eu
u={name}
if ! id -u "$u" >/dev/null 2>&1; then
	shell=/bin/sh
	[ -x /bin/bash ] && shell=/bin/bash
	if command -v useradd >/dev/null 2>&1; then
		useradd --create-home --shell "$shell" "$u"
	else
		adduser -D -s "$shell" "$u"
	fi
fi
"#,
		name = sh_quote(name)
	);
	if sudo {
		let _ = write!(
			script,
			r#"E2B_ADMIN_GROUP=
[ -f {DISTRO_ENV} ] && . {DISTRO_ENV}
group_exists() {{ grep -q "^$1:" /etc/group; }}
g=${{E2B_ADMIN_GROUP:-}}
if [ -z "$g" ] || ! group_exists "$g"; then
	if group_exists sudo; then g=sudo; elif group_exists wheel; then g=wheel; else g=; fi
fi
if [ -n "$g" ]; then
	if command -v usermod >/dev/null 2>&1; then usermod -aG "$g" "$u"; else addgroup "$u" "$g" 2>/dev/null || true; fi
else
	echo "warning: neither the sudo nor the wheel group exists; relying on sudoers" >&2
fi
passwd -d "$u" >/dev/null 2>&1 || sed -i "s/^$u:[^:]*:/$u::/" /etc/shadow 2>/dev/null || true
touch /etc/sudoers
if ! grep -q "^$u ALL=(ALL:ALL) NOPASSWD: ALL" /etc/sudoers; then
	echo "$u ALL=(ALL:ALL) NOPASSWD: ALL" >>/etc/sudoers
fi
"#
		);
	}
	script
}

/// Guest script evaluating ENV values like upstream.
///
/// Each value goes through `printf "%s" "<value>"` so `$VAR`/`${VAR}`
/// expand but command substitution does not. Values are printed
/// NUL-separated in order.
pub fn env_eval_script(values: &[&str]) -> String {
	let mut script = String::from("printf '%s\\0'");
	for value in values {
		script.push_str(" \"");
		script.push_str(&escape_env_value(value));
		script.push('"');
	}
	script
}

/// Escapes `\`, `"`, `` ` `` and `$(` so only parameter expansion remains.
pub fn escape_env_value(value: &str) -> String {
	value
		.replace('\\', "\\\\")
		.replace('"', "\\\"")
		.replace('`', "\\`")
		.replace("$(", "\\$(")
}

/// Single-quotes `s` for POSIX sh.
pub fn sh_quote(s: &str) -> String {
	format!("'{}'", s.replace('\'', r"'\''"))
}

/// Upstream log header text for a step: `<TYPE> <args joined by space>`.
pub fn describe(step: &BuildStep) -> String {
	let args = step.args.join(" ");
	if args.is_empty() {
		step.kind.to_ascii_uppercase()
	} else {
		format!("{} {args}", step.kind.to_ascii_uppercase())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn step(kind: &str, args: &[&str]) -> BuildStep {
		BuildStep {
			kind:       kind.into(),
			args:       args.iter().map(ToString::to_string).collect(),
			files_hash: Some("ab".repeat(32)),
			force:      false,
		}
	}

	fn sv(a: &[&str]) -> Vec<String> {
		a.iter().map(ToString::to_string).collect()
	}

	#[test]
	fn copy_owner_defaults_and_overrides() {
		let spec = CopySpec::parse(&sv(&["a", "/b", "", ""])).unwrap();
		assert_eq!(spec.chown_spec("user"), "user:user", "empty user = current USER");
		assert_eq!(spec.chown_spec("root"), "root:root");
		let spec = CopySpec::parse(&sv(&["a", "/b", "www", ""])).unwrap();
		assert_eq!(spec.chown_spec("user"), "www:www", "bare user doubles as group");
		let spec = CopySpec::parse(&sv(&["a", "/b", "www:staff", ""])).unwrap();
		assert_eq!(spec.chown_spec("user"), "www:staff");
		let spec = CopySpec::parse(&sv(&["a", "/b"])).unwrap();
		assert_eq!((spec.owner, spec.mode), (None, None), "two-arg form is accepted");
		for bad in [":g", "a:b:c", "a b"] {
			assert!(CopySpec::parse(&sv(&["a", "/b", bad, ""])).is_err(), "{bad}");
		}
	}

	#[test]
	fn copy_mode_parsing() {
		assert_eq!(CopySpec::parse(&sv(&["a", "/b", "", "0755"])).unwrap().mode, Some(0o755));
		assert_eq!(
			CopySpec::parse(&sv(&["a", "/b", "", "4755"])).unwrap().mode,
			Some(0o4755),
			"setuid survives"
		);
		assert_eq!(CopySpec::parse(&sv(&["a", "/b", "", "644"])).unwrap().mode, Some(0o644));
		for bad in ["0789", "rwx", "07777", "-1", "0x1ff"] {
			assert!(CopySpec::parse(&sv(&["a", "/b", "", bad])).is_err(), "{bad}");
		}
		assert!(CopySpec::parse(&sv(&["a"])).is_err());
		assert!(CopySpec::parse(&sv(&["a", "/b", "", "", "extra"])).is_err());
	}

	#[test]
	fn glob_sources_strip_to_base_directory() {
		assert_eq!(glob_base("dir/*.txt").unwrap(), "dir");
		assert_eq!(glob_base("src/**/*.py").unwrap(), "src");
		assert_eq!(glob_base("*.txt").unwrap(), "");
		assert_eq!(glob_base("./app/").unwrap(), "app");
		assert_eq!(glob_base(".").unwrap(), "");
		assert_eq!(glob_base("a/b/file.txt").unwrap(), "a/b/file.txt");
		assert!(glob_base("/etc/passwd").is_err());
		assert!(glob_base("../secret").is_err());
	}

	#[test]
	fn copy_script_quotes_and_resolves_destination() {
		let ctx = BuildContext {
			user: "user".into(),
			workdir: Some("/app".into()),
			..BuildContext::default()
		};
		let spec = CopySpec::parse(&sv(&["it's.tar.gz", "rel/", "", "0640"])).unwrap();
		let script = spec.script("h", &ctx);
		assert!(script.contains(r"src='/run/vmon-context/h/it'\''s.tar.gz'"), "{script}");
		assert!(script.contains("dest='/app/rel/'"));
		assert!(script.contains("O='user:user'"));
		assert!(script.contains("M='640'"));
		let home = BuildContext { user: "bob".into(), ..BuildContext::default() };
		let script = CopySpec::parse(&sv(&["x", "data", "", ""]))
			.unwrap()
			.script("h", &home);
		assert!(script.contains(r#"dest="$(user_home 'bob')/"'data'"#), "{script}");
		assert!(script.contains("M=''"));
	}

	#[test]
	fn parse_step_types_case_insensitively_with_aliases() {
		assert_eq!(Step::parse(&step("run", &["echo hi"])).unwrap(), Step::Run {
			cmd:  "echo hi".into(),
			user: None,
		});
		assert_eq!(Step::parse(&step("RUN", &["x", "root"])).unwrap(), Step::Run {
			cmd:  "x".into(),
			user: Some("root".into()),
		});
		assert!(matches!(Step::parse(&step("add", &["a", "/b", "", ""])).unwrap(), Step::Copy(_)));
		assert_eq!(
			Step::parse(&step("ARG", &["V"])).unwrap(),
			Step::Env(vec![("V".into(), String::new())])
		);
		assert!(Step::parse(&step("ENV", &["A", "1", "B"])).is_err(), "odd count");
		assert_eq!(Step::parse(&step("USER", &["user", "true"])).unwrap(), Step::User {
			name: "user".into(),
			sudo: true,
		});
		let err = Step::parse(&step("HEALTHCHECK", &["x"])).unwrap_err();
		assert_eq!(err.to_string(), "command type HEALTHCHECK is not implemented");
		let mut no_hash = step("COPY", &["a", "/b", "", ""]);
		no_hash.files_hash = None;
		assert!(Step::parse(&no_hash).is_err());
	}

	#[test]
	fn workdir_resolution() {
		assert_eq!(resolve_workdir(None, "app"), "/app");
		assert_eq!(resolve_workdir(Some("/app"), "sub/../x"), "/app/x");
		assert_eq!(resolve_workdir(Some("/app"), "/abs/"), "/abs");
		assert_eq!(resolve_workdir(Some("/"), "../.."), "/");
	}

	#[test]
	fn env_values_allow_parameter_expansion_only() {
		assert_eq!(
			escape_env_value(r#"a "b" \c `d` $(e) ${F} $G"#),
			r#"a \"b\" \\c \`d\` \$(e) ${F} $G"#
		);
		assert_eq!(env_eval_script(&["$HOME/bin", "x"]), r#"printf '%s\0' "$HOME/bin" "x""#);
	}

	#[test]
	fn command_env_appends_standard_path() {
		let mut ctx = BuildContext::default();
		assert_eq!(ctx.command_env()["PATH"], DEFAULT_PATH);
		ctx.env.insert("PATH".into(), "/opt/bin".into());
		assert_eq!(ctx.command_env()["PATH"], format!("/opt/bin{PATH_SUFFIX}"));
	}

	#[test]
	fn shell_argv_prefers_login_bash_and_honors_workdir() {
		let ctx =
			BuildContext { has_bash: true, workdir: Some("/w d".into()), ..BuildContext::default() };
		let argv = ctx.shell_argv("echo $0");
		assert_eq!(argv[..2], ["/bin/sh", "-c"]);
		assert_eq!(argv[2], "cd '/w d' || exit 1; exec /bin/bash -l -c \"$0\"");
		assert_eq!(argv[3], "echo $0");
		let ctx = BuildContext::default();
		assert!(ctx.shell_argv("x")[2].ends_with("exec /bin/sh -c \"$0\""));
	}

	#[test]
	fn user_script_grants_sudo_only_when_asked() {
		assert!(!user_script("bob", false).contains("NOPASSWD"));
		let script = user_script("bob", true);
		assert!(script.contains("u='bob'"));
		assert!(script.contains("NOPASSWD: ALL"));
	}
}
