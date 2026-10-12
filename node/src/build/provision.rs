//! Base-image provisioning and template configuration scripts.
//!
//! Ported from upstream `phases/base/provision.sh`,
//! `base/distro/{distro,init}.go` and `phases/finalize/configure.sh`
//! (vendored under `spec/e2b/infra/…`). Differences from upstream, all
//! driven by vibemon's VM model:
//! - The guest agent is installed as `/usr/bin/envd` and registered as the
//!   `envd` service (systemd unit / `OpenRC` script); it serves GC4 on its
//!   `vmon.gc4` virtio-console port and envd on :49983, PID 1 or not.
//! - No PHC clock: chrony gets an NTP pool line directly.
//! - Images without a known profile (NixOS, distroless, no os-release, and the
//!   upstream-rejected RHEL/OL/Amazon IDs) are not rejected: they boot with the
//!   agent as PID 1 (`init=/usr/bin/envd`) and skip package setup.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use super::steps::{sh_quote, user_script};

/// Where the agent binary is installed inside templates.
pub const ENVD_PATH: &str = "/usr/bin/envd";
/// Kernel `init=` for images whose init system was provisioned.
pub const SYSTEM_INIT: &str = "/usr/sbin/init";
/// Distro record written by provisioning and read back by the host.
pub const DISTRO_ENV: &str = "/usr/local/share/e2b/distro.env";
/// Bump when the provisioning contract changes (invalidates base layers).
const VERSION: &str = "vmon-1";

/// Init family of a distro profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Init {
	Systemd,
	OpenRc,
}

impl Init {
	const fn name(self) -> &'static str {
		match self {
			Self::Systemd => "systemd",
			Self::OpenRc => "openrc",
		}
	}
}

/// One distro family (upstream `distro.Profile`).
struct Profile {
	ids:           &'static [&'static str],
	init:          Init,
	packages:      &'static [&'static str],
	pkg_query:     &'static str,
	pkg_install:   &'static str,
	init_binary:   &'static str,
	timesync_unit: &'static str,
	ssh_unit:      &'static str,
	admin_group:   &'static str,
	ca_bundle:     &'static str,
	ca_refresh:    &'static str,
}

const PROFILES: &[Profile] = &[
	Profile {
		ids:           &["debian", "ubuntu"],
		init:          Init::Systemd,
		packages:      &[
			"systemd",
			"systemd-sysv",
			"openssh-server",
			"sudo",
			"chrony",
			"socat",
			"curl",
			"ca-certificates",
			"fuse3",
			"iptables",
			"git",
			"nfs-common",
			"less",
			"nftables",
			"iputils-ping",
			"jq",
			"iproute2",
		],
		pkg_query:     r#"dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed""#,
		pkg_install:   r#"apt-get -q update && DEBIAN_FRONTEND=noninteractive DEBCONF_NOWARNINGS=yes apt-get -qq -o=Dpkg::Use-Pty=0 install -y --no-install-recommends "$@""#,
		init_binary:   "/lib/systemd/systemd",
		timesync_unit: "chrony",
		ssh_unit:      "ssh",
		admin_group:   "sudo",
		ca_bundle:     "/etc/ssl/certs/ca-certificates.crt",
		ca_refresh:    "update-ca-certificates",
	},
	Profile {
		ids:           &["fedora", "centos", "rocky", "almalinux"],
		init:          Init::Systemd,
		packages:      &[
			"systemd",
			"shadow-utils",
			"passwd",
			"openssh-server",
			"sudo",
			"chrony",
			"socat",
			"curl",
			"ca-certificates",
			"fuse3",
			"iptables",
			"git",
			"nfs-utils",
			"less",
			"nftables",
			"iputils",
			"jq",
			"bash",
			"tar",
			"iproute",
			"findutils",
		],
		pkg_query:     r#"rpm -q "$1" >/dev/null 2>&1"#,
		pkg_install:   r#"dnf -y install --allowerasing "$@" || microdnf -y install "$@" || yum -y install "$@""#,
		init_binary:   "/usr/lib/systemd/systemd",
		timesync_unit: "chronyd",
		ssh_unit:      "sshd",
		admin_group:   "wheel",
		ca_bundle:     "/etc/ssl/certs/ca-certificates.crt",
		ca_refresh:    r#"update-ca-trust extract && ln -sf /etc/pki/tls/certs/ca-bundle.crt "$E2B_CA_BUNDLE""#,
	},
	Profile {
		ids:           &["arch", "archarm"],
		init:          Init::Systemd,
		packages:      &[
			"systemd",
			"shadow",
			"openssh",
			"sudo",
			"chrony",
			"socat",
			"curl",
			"ca-certificates",
			"fuse3",
			"iptables",
			"git",
			"nfs-utils",
			"less",
			"nftables",
			"iputils",
			"jq",
			"bash",
		],
		pkg_query:     r#"pacman -Q "$1" >/dev/null 2>&1"#,
		pkg_install:   r#"pacman -Syu --noconfirm --needed "$@""#,
		init_binary:   "/usr/lib/systemd/systemd",
		timesync_unit: "chronyd",
		ssh_unit:      "sshd",
		admin_group:   "wheel",
		ca_bundle:     "/etc/ssl/certs/ca-certificates.crt",
		ca_refresh:    "update-ca-trust extract",
	},
	Profile {
		ids:           &["alpine"],
		init:          Init::OpenRc,
		packages:      &[
			"openrc",
			"shadow",
			"openssh",
			"sudo",
			"chrony",
			"socat",
			"curl",
			"ca-certificates",
			"fuse3",
			"iptables",
			"git",
			"nfs-utils",
			"less",
			"nftables",
			"iputils",
			"jq",
			"bash",
			"util-linux-misc",
			"iproute2",
		],
		pkg_query:     r#"apk info -e "$1" >/dev/null 2>&1"#,
		pkg_install:   r#"apk add --no-cache "$@""#,
		init_binary:   "/bin/busybox",
		timesync_unit: "chronyd",
		ssh_unit:      "sshd",
		admin_group:   "wheel",
		ca_bundle:     "/etc/ssl/certs/ca-certificates.crt",
		ca_refresh:    "update-ca-certificates",
	},
];

/// systemd unit running the agent as envd.
const ENVD_UNIT: &str = "[Unit]
Description=envd (vibemon guest agent)
DefaultDependencies=no
After=local-fs.target
Before=multi-user.target shutdown.target
Conflicts=shutdown.target

[Service]
Type=simple
ExecStart=/usr/bin/envd
Restart=always
RestartSec=1
KillMode=process
OOMScoreAdjust=-1000

[Install]
WantedBy=multi-user.target
";

/// `OpenRC` service script running the agent as envd.
const ENVD_OPENRC: &str = "#!/sbin/openrc-run
name=\"envd\"
description=\"envd (vibemon guest agent)\"
command=\"/usr/bin/envd\"
supervisor=supervise-daemon
respawn_delay=1
respawn_max=0

depend() {
	need localmount
	after bootmisc
}
";

const SYSTEMD_SETUP: &str = r#"echo "Don't wait for ttyS0 (serial console)"
systemctl mask serial-getty@ttyS0.service
echo "Disable network online wait"
systemctl mask systemd-networkd-wait-online.service
echo "Disable system first boot wizard"
systemctl mask systemd-firstboot.service
echo "Enable time synchronization ($E2B_TIMESYNC_UNIT)"
systemctl enable "$E2B_TIMESYNC_UNIT" || echo "time sync unit $E2B_TIMESYNC_UNIT not available"
echo "Enable SSH ($E2B_SSH_UNIT)"
systemctl enable "$E2B_SSH_UNIT.service" || echo "ssh unit $E2B_SSH_UNIT not available"
echo "Enable envd autostart"
mkdir -p /etc/systemd/system
printf '%s' "$E2B_ENVD_UNIT" >/etc/systemd/system/envd.service
systemctl enable envd.service
echo "Disable chrony-wait"
systemctl mask chrony-wait.service
echo "Disable slow boot units not needed in the sandbox"
systemctl mask systemd-binfmt.service
systemctl mask e2scrub_reap.service"#;

const OPENRC_SETUP: &str = r#"echo "Installing boot inittab (busybox init -> OpenRC runlevels)"
printf '%s\n' \
    '::sysinit:/sbin/openrc sysinit' \
    '::sysinit:/sbin/openrc boot' \
    '::wait:/sbin/openrc default' \
    '::shutdown:/sbin/openrc shutdown' \
    '::ctrlaltdel:/sbin/reboot' \
    > /etc/inittab
echo "Registering base OpenRC services"
for svc in devfs sysfs procfs dmesg mdev; do
    if [ -e "/etc/init.d/$svc" ]; then rc-update add "$svc" sysinit; else echo "OpenRC service $svc not present on this image; skipping"; fi
done
for svc in localmount sysctl hostname bootmisc; do
    if [ -e "/etc/init.d/$svc" ]; then rc-update add "$svc" boot; else echo "OpenRC service $svc not present on this image; skipping"; fi
done
mkdir -p /etc/network
printf 'auto lo\niface lo inet loopback\n' > /etc/network/interfaces
if [ -e /etc/init.d/networking ]; then rc-update add networking boot; fi
echo "Enable time synchronization ($E2B_TIMESYNC_UNIT)"
rc-update add "$E2B_TIMESYNC_UNIT" default || echo "time sync service $E2B_TIMESYNC_UNIT not available"
echo "Enable envd autostart"
printf '%s' "$E2B_ENVD_OPENRC" >/etc/init.d/envd
chmod 0755 /etc/init.d/envd
rc-update add envd default
echo "Enable sshd"
if [ -e /etc/init.d/sshd ]; then rc-update add sshd default; else echo "sshd service not present on this image; skipping"; fi"#;

/// Renders the distro selector (upstream `ShellSelector`) with our fallback.
fn selector() -> String {
	let mut b = String::from("e2b_select_profile() {\n  e2b_profile_matched=\n  case \"$1\" in\n");
	for p in PROFILES {
		let _ = writeln!(b, "  {})", p.ids.join("|"));
		let _ = writeln!(b, "    E2B_PACKAGES={}", sh_quote(&p.packages.join(" ")));
		let _ = writeln!(b, "    e2b_pkg_query() {{ {}; }}", p.pkg_query);
		let _ = writeln!(b, "    e2b_pkg_install() {{ {}; }}", p.pkg_install);
		let _ = writeln!(b, "    E2B_INIT_BIN={}", sh_quote(p.init_binary));
		let _ = writeln!(b, "    E2B_TIMESYNC_UNIT={}", sh_quote(p.timesync_unit));
		let _ = writeln!(b, "    E2B_SSH_UNIT={}", sh_quote(p.ssh_unit));
		let _ = writeln!(b, "    E2B_ADMIN_GROUP={}", sh_quote(p.admin_group));
		let _ = writeln!(b, "    E2B_CA_BUNDLE={}", sh_quote(p.ca_bundle));
		let _ = writeln!(b, "    e2b_ca_refresh() {{ {}; }}", p.ca_refresh);
		let _ = writeln!(b, "    E2B_INIT_SYSTEM={}", p.init.name());
		let setup = match p.init {
			Init::Systemd => SYSTEMD_SETUP,
			Init::OpenRc => OPENRC_SETUP,
		};
		let _ = writeln!(b, "    e2b_init_setup() {{\n{}\n    }}", indent(setup, "        "));
		b.push_str("    e2b_profile_matched=1\n    ;;\n");
	}
	b.push_str("  *)\n    ;;\n  esac\n}\n\n");
	b.push_str(
		r#"e2b_select_profile "$E2B_DISTRO_ID"
if [ -z "$e2b_profile_matched" ]; then
  for e2b_like in $E2B_ID_LIKE; do
    e2b_select_profile "$e2b_like"
    if [ -n "$e2b_profile_matched" ]; then
      echo "[provision] WARNING: base image distribution ID='$E2B_DISTRO_ID' is not officially supported; provisioning it as '$e2b_like' from ID_LIKE. This is best effort and untested." >&2
      break
    fi
  done
fi
if [ -z "$e2b_profile_matched" ]; then
  echo "[provision] WARNING: no provisioning profile for distribution '$E2B_DISTRO_ID'; the template boots with envd as PID 1 and no init system." >&2
  E2B_INIT_SYSTEM=none
  E2B_INIT_BIN=/usr/bin/envd
  E2B_PACKAGES=
  E2B_ADMIN_GROUP=
  E2B_TIMESYNC_UNIT=none
fi
"#,
	);
	b
}

fn indent(block: &str, prefix: &str) -> String {
	block
		.lines()
		.map(|l| {
			if l.is_empty() {
				String::new()
			} else {
				format!("{prefix}{l}")
			}
		})
		.collect::<Vec<_>>()
		.join("\n")
}

/// The provisioning script run as root (POSIX sh) in the steps VM, with
/// network access, after the image is unpacked.
#[expect(
	clippy::literal_string_with_formatting_args,
	reason = "shell `${VAR:-default}` expansions, not format arguments"
)]
pub fn provision_script() -> String {
	let mut s = String::from("set -eu\necho \"Starting provisioning script\"\n");
	let _ = writeln!(s, "E2B_ENVD_UNIT={}", sh_quote(ENVD_UNIT));
	let _ = writeln!(s, "E2B_ENVD_OPENRC={}", sh_quote(ENVD_OPENRC));
	s.push_str(
		r#"echo "Detecting base image distribution"
if [ -r /etc/os-release ]; then
    . /etc/os-release
    E2B_DISTRO_ID="${ID:-unknown}"
    E2B_ID_LIKE="${ID_LIKE:-}"
else
    E2B_DISTRO_ID="unknown"
    E2B_ID_LIKE=""
fi
"#,
	);
	s.push_str(&selector());
	s.push_str(
		r#"echo "Provisioning for distro '$E2B_DISTRO_ID' (init=$E2B_INIT_BIN, timesync=$E2B_TIMESYNC_UNIT, admin-group=$E2B_ADMIN_GROUP)"
mkdir -p /usr/local/share/e2b
{
    echo "E2B_DISTRO_ID='$E2B_DISTRO_ID'"
    echo "E2B_INIT_SYSTEM='$E2B_INIT_SYSTEM'"
    echo "E2B_ADMIN_GROUP='$E2B_ADMIN_GROUP'"
} > /usr/local/share/e2b/distro.env
if [ "$E2B_INIT_SYSTEM" = none ]; then
    echo "Finished provisioning script (no init system)"
    exit 0
fi
PACKAGES="$E2B_PACKAGES"
echo "Checking presence of the following packages: $PACKAGES"
MISSING=""
for pkg in $PACKAGES; do
    if ! e2b_pkg_query "$pkg"; then
        echo "Package $pkg is missing, will install it."
        MISSING="$MISSING $pkg"
    fi
done
if [ -n "$MISSING" ]; then
    echo "Missing packages detected, installing:$MISSING"
    # shellcheck disable=SC2086
    e2b_pkg_install $MISSING
else
    echo "All required packages are already installed."
fi
if [ ! -s "$E2B_CA_BUNDLE" ]; then
    echo "CA trust bundle missing at $E2B_CA_BUNDLE - running the profile's refresh"
    e2b_ca_refresh
fi
mkdir -p /etc/tmpfiles.d
echo 'z /dev/fuse 0666 root root -' > /etc/tmpfiles.d/fuse.conf
echo "Setting up shell"
mkdir -p /etc/profile.d /root
echo "export SHELL='/bin/bash'" >/etc/profile.d/shell.sh
echo "export PS1='\w \$ '" >/etc/profile.d/prompt.sh
echo "export PS1='\w \$ '" >>"/etc/profile"
echo "export PS1='\w \$ '" >>"/root/.bashrc"
echo "Use .bashrc and .profile"
echo "if [ -f ~/.bashrc ]; then source ~/.bashrc; fi; if [ -f ~/.profile ]; then source ~/.profile; fi" >>/etc/profile
echo "Remove root password"
if [ -f /etc/passwd ]; then passwd -d root; else echo "No /etc/passwd on this image; skipping root password removal"; fi
echo "Setting up chrony"
mkdir -p /etc/chrony
{
    echo "pool pool.ntp.org iburst"
    echo "makestep 1.0 3"
} >/etc/chrony/chrony.conf
echo "include /etc/chrony/chrony.conf" >/etc/chrony.conf
echo "Setting up SSH"
mkdir -p /etc/ssh
if grep -qE '^Include[[:space:]]+/etc/ssh/sshd_config\.d/' /etc/ssh/sshd_config 2>/dev/null; then
  E2B_SSHD_CONF=/etc/ssh/sshd_config.d/00-e2b.conf
  mkdir -p /etc/ssh/sshd_config.d
else
  E2B_SSHD_CONF=/etc/ssh/sshd_config
fi
cat <<EOF >>"$E2B_SSHD_CONF"
PermitRootLogin yes
PermitEmptyPasswords yes
PasswordAuthentication yes
EOF
echo "Increasing inotify watch limit"
echo 'fs.inotify.max_user_watches=65536' >>/etc/sysctl.conf
echo "Disabling proactive memory compaction"
echo 'vm.compaction_proactiveness=0' >>/etc/sysctl.conf
e2b_init_setup
rm -rf /etc/machine-id
echo "Linking $E2B_INIT_BIN to init"
mkdir -p /usr/sbin
ln -sf "$E2B_INIT_BIN" /usr/sbin/init
[ -L /sbin ] || ln -sf "$E2B_INIT_BIN" /sbin/init
echo "Finished provisioning script"
"#,
	);
	s
}

/// Fingerprint of the provisioning contract (part of the base-layer key).
pub fn fingerprint() -> String {
	hex::encode(Sha256::digest(format!("{VERSION}\0{}", provision_script()).as_bytes()))
}

/// Kernel `init=` for a provisioned rootfs given the `E2B_INIT_SYSTEM`
/// recorded in [`DISTRO_ENV`] (missing file = unprovisioned = agent as PID 1).
pub fn boot_init(distro_env: Option<&str>) -> &'static str {
	let init_system = distro_env
		.into_iter()
		.flat_map(str::lines)
		.find_map(|line| line.strip_prefix("E2B_INIT_SYSTEM="))
		.map(|v| v.trim().trim_matches('\''));
	match init_system {
		Some("systemd" | "openrc") => SYSTEM_INIT,
		_ => ENVD_PATH,
	}
}

/// upstream `configure.sh` (finalize phase, every build, as root), POSIX sh
/// with busybox fallbacks.
pub fn configure_script(template_id: &str, build_id: &str) -> String {
	let mut s = format!(
		r#"set -eu
echo "Starting configuration script"
cat <<EOF > /.e2b
ENV_ID={template_id}
TEMPLATE_ID={template_id}
BUILD_ID={build_id}
EOF
echo "Create default user 'user' (if doesn't exist yet)"
"#
	);
	s.push_str(&user_script("user", false));
	s.push_str(
		r#"if [ -d /home/user ] && [ -d /etc/skel ]; then
    echo "Copy skeleton files to /home/user (keeping existing files)"
    (cd /etc/skel && find . -mindepth 1) | while IFS= read -r rel; do
        rel=${rel#./}
        if [ -e "/home/user/$rel" ] || [ -L "/home/user/$rel" ]; then continue; fi
        if [ -d "/etc/skel/$rel" ] && [ ! -L "/etc/skel/$rel" ]; then
            mkdir "/home/user/$rel"
        else
            cp -P -p "/etc/skel/$rel" "/home/user/$rel"
        fi
    done
fi
echo "Add sudo to 'user' with no password"
"#,
	);
	s.push_str(&user_script("user", true));
	s.push_str(
		r#"echo "Give 'user' ownership to /home/user"
mkdir -p /home/user
chown -R "user:$(id -g user)" /home/user
echo "Give 777 permission to /usr/local"
chmod -R 777 /usr/local
echo "Create /code directory"
mkdir -p /code
echo "Give 777 permission to /code"
chmod -R 777 /code
echo "Finished configuration script"
"#,
	);
	s
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn boot_init_follows_recorded_init_system() {
		assert_eq!(
			boot_init(Some("E2B_DISTRO_ID='debian'\nE2B_INIT_SYSTEM='systemd'\n")),
			SYSTEM_INIT
		);
		assert_eq!(boot_init(Some("E2B_INIT_SYSTEM='openrc'")), SYSTEM_INIT);
		assert_eq!(boot_init(Some("E2B_INIT_SYSTEM='none'")), ENVD_PATH);
		assert_eq!(boot_init(None), ENVD_PATH);
	}

	#[test]
	fn selector_covers_every_profile_id_and_falls_back() {
		let script = provision_script();
		for profile in PROFILES {
			assert!(script.contains(&format!("  {})", profile.ids.join("|"))));
		}
		assert!(script.contains("E2B_INIT_SYSTEM=none"));
		assert!(script.contains("systemctl enable envd.service"));
		assert!(script.contains("rc-update add envd default"));
		assert!(script.contains("ExecStart=/usr/bin/envd"), "unit text is embedded");
	}

	#[test]
	fn configure_writes_ids_and_fingerprint_is_stable() {
		let script = configure_script("tpl123", "b-1");
		assert!(script.contains("TEMPLATE_ID=tpl123\nBUILD_ID=b-1"));
		assert!(script.contains("chmod -R 777 /usr/local"));
		assert_eq!(fingerprint(), fingerprint());
		assert_eq!(fingerprint().len(), 64);
	}
}
