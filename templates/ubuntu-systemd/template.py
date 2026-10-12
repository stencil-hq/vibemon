"""`ubuntu-systemd`: stock Ubuntu 24.04 with systemd as PID 1.

The reference for templates built `fromImage` on a plain distro image: the
platform's base phase installs systemd (with the envd unit) into
`ubuntu:24.04` and boots the template VM with systemd as PID 1. On top of
that this template

- adds a trivial oneshot unit, `vmon-probe.service`, enabled through its
  `multi-user.target.wants` symlink (steps run with the guest agent as PID 1,
  so no systemd manager is around to ask), which writes `ok` to
  `/run/vmon-probe` at boot;
- runs a heartbeat loop as its start command, captured live in the template
  snapshot (`/tmp/vmon-heartbeat` counts seconds, `/tmp/vmon-heartbeat.pid`
  names the loop's process);
- makes `user` the default user, `/home/user` the default workdir.

`node/src/build/e2e.rs` (`VMON_BUILD_E2E_ONLY=ubuntu`) builds the same
definition and checks all of the above in a VM forked from the snapshot; keep
the two in sync.
"""

from pathlib import Path

from e2b import Template

CPU_COUNT = 2
MEMORY_MB = 1024

INSTALL_PROBE = r"""set -eu
cat > /etc/systemd/system/vmon-probe.service <<'EOF'
[Unit]
Description=vibemon probe: systemd starts enabled units at boot

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'echo ok > /run/vmon-probe'

[Install]
WantedBy=multi-user.target
EOF
mkdir -p /etc/systemd/system/multi-user.target.wants
ln -sf ../vmon-probe.service /etc/systemd/system/multi-user.target.wants/vmon-probe.service
"""

INSTALL_HEARTBEAT = r"""set -eu
cat > /usr/local/bin/vmon-heartbeat <<'EOF'
#!/bin/sh
# Start command of the ubuntu-systemd template: counts seconds into
# /tmp/vmon-heartbeat (replaced atomically) and records its pid.
echo $$ > /tmp/vmon-heartbeat.pid
i=0
while :; do
	i=$((i + 1))
	echo "$i" > /tmp/vmon-heartbeat.tmp
	mv -f /tmp/vmon-heartbeat.tmp /tmp/vmon-heartbeat
	sleep 1
done
EOF
chmod 0755 /usr/local/bin/vmon-heartbeat
"""

START_CMD = "/usr/local/bin/vmon-heartbeat"
READY_CMD = "test -s /tmp/vmon-heartbeat"


def make_template():
    return (
        Template(file_context_path=Path(__file__).parent)
        .from_image("ubuntu:24.04")
        .run_cmd(INSTALL_PROBE, user="root")
        .run_cmd(INSTALL_HEARTBEAT, user="root")
        .set_user("user")
        .set_workdir("/home/user")
        .set_start_cmd(START_CMD, READY_CMD)
    )
