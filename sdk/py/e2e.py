#!/usr/bin/env python3.14
"""End-to-end smoke test for the vmon / vmon sandbox stack.

Boots real microVMs on a Linux/KVM or macOS/HVF host and exercises every
user-facing feature in a single run:

  * exec (argv / env / workdir) and the filesystem RPC facade
  * pty exec (isatty + resize)
  * suspend + resume (vCPU parking proven via guest-clock continuity)
  * snapshot + warm restore (disk + memory state)
  * copy-on-write fork (shared base, isolated writes)
  * successive public v1 snapshots (snapshot listing + independent restore)
  * writable and read-only virtio-fs volumes that persist across sandboxes
  * read-only host directory shares (``fs_dir``)
  * secrets (injected into exec env, never written to meta.json)
  * networking: ``block_network`` egress denial, guest IP/route over a host TAP,
    exposed-port tunnels, and host-side IP lease allocation
  * tags + listing
  * warm pools (pre-forked clones)
  * server-enforced timeouts (return code 124) and live deadline ``extend``
  * the async (``aio``) facade
  * durable functions: warm workers, rich-type arguments, streaming generators,
    detached calls, bounded maps, and stateful ``@vmon.cls`` actors

This complements the pytest suite in ``tests/test_e2e.py`` with a single
self-contained driver that prints a PASS/FAIL/SKIP table and returns a
non-zero exit code if anything fails.

Prerequisites (a Linux/KVM or macOS/HVF host; e.g. bare metal, an Apple-silicon
Mac, or a nested-KVM Lima VM):
  * a usable hypervisor          (Linux ``/dev/kvm`` or macOS Hypervisor.framework)
  * a built ``vmon`` binary       (``cargo build --release`` or ``VMON_BIN``)
  * a startable Rust API server   (this driver spawns ``vmon serve --port 0``)
  * server-side image/kernel/agent/tooling checks reported by ``vmon doctor``

Note: feature sandboxes below boot with ``block_network=True`` (no NIC, no root).
The dedicated networking test opts into user-mode NAT on macOS/HVF or a host TAP
on Linux. The tunnel test remains TAP-only because macOS user-net has no inbound
port forwarding.

Usage:
  python3.14 sdk/py/e2e.py                 # run everything
  python3.14 sdk/py/e2e.py exec snapshot   # run a subset (substring match)
  python3.14 sdk/py/e2e.py --list          # list test names
  python3.14 sdk/py/e2e.py --keep          # leave the temp VMON_HOME in place
  VMON_E2E_IMAGE=alpine:latest python3.14 sdk/py/e2e.py
"""

import argparse
import asyncio
import contextlib
import hashlib
import io
import ipaddress
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vmon
from e2e_remote import RemoteCounter, remote_add, remote_double, remote_shift, remote_ticks
from vmon import (
    APIError,
    Client,
    FunctionCall,
    RemoteFunctionError,
    Sandbox,
    Secret,
    SerializerPolicy,
    ValueCodec,
    Volume,
    connect,
)

IMAGE = os.environ.get("VMON_E2E_IMAGE", "alpine:latest")
RUN = f"e2e{os.getpid()}"
REPO = Path(__file__).resolve().parents[1]
STATE = Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))

_seq = 0
_virtiofs: bool | None = None
_client: Client | None = None

# --------------------------------------------------------------------------- #
# harness
# --------------------------------------------------------------------------- #


class Skip(Exception):
    """Raised by a test to report it cannot run in this environment."""


TESTS: list[Callable[[object], None]] = []
VOLUMES: set[str] = set()


def sdk() -> Client:
    """Return the single object-hierarchy client used by this run."""
    global _client
    if _client is None:
        _client = connect()
    return _client


def e2e(fn: Callable[[object], None]) -> Callable[[object], None]:
    """Register a test, preserving definition order."""
    TESTS.append(fn)
    return fn


def uid(prefix: str) -> str:
    """A per-run unique, snapshot/volume-name-safe identifier."""
    global _seq
    _seq += 1
    return f"{prefix}-{RUN}-{_seq}"


def configure_home(home: Path) -> None:
    """Point the SDK and local helpers at this run's isolated server home."""
    global STATE, _client, _virtiofs
    if _client is not None:
        _client.close()
        _client = None
    STATE = home
    _virtiofs = None
    os.environ["VMON_HOME"] = str(home)
    os.environ.pop("VMON_CONTEXT", None)


def make_sandbox(**kw: Any) -> Sandbox:
    """Create a sandbox, defaulting to no network.

    Feature tests boot with ``block_network=True`` since they don't need
    networking; the dedicated networking/tunnel tests opt in explicitly.
    """
    kw.setdefault("block_network", True)
    return sdk().sandboxes.create(image=IMAGE, **kw)


def run(
    sb: Sandbox,
    *argv: str,
    timeout: float = 30,
    pty: bool = False,
    env: dict[str, str] | None = None,
    workdir: str | None = None,
) -> tuple[int | None, str, str]:
    """Exec ``argv`` in ``sb``; return ``(returncode, stdout, stderr)`` as text."""
    proc = sb.exec(*argv, timeout=timeout, pty=pty, env=env, workdir=workdir)
    if not pty:
        try:
            proc.stdin.close()
        except Exception:
            pass
    try:
        exit_result = proc.wait(timeout=timeout)
        rc = exit_result.code
    except TimeoutError:
        proc.close()
        rc = None
    out = proc.stdout.read().decode("utf-8", "replace")
    err = proc.stderr.read().decode("utf-8", "replace")
    return rc, out, err


def uptime(sb: Sandbox) -> float:
    """Guest uptime (seconds) from ``/proc/uptime``."""
    _, out, _ = run(sb, "cat", "/proc/uptime", timeout=15)
    return float(out.split()[0])


def _is_executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def find_vmon_binary() -> Path:
    """Resolve the Rust ``vmon`` binary from VMON_BIN, target dirs, or PATH."""
    candidates: list[Path] = []
    if os.environ.get("VMON_BIN"):
        candidates.append(Path(os.environ["VMON_BIN"]).expanduser())

    target = Path(os.environ.get("CARGO_TARGET_DIR", str(REPO / "target"))).expanduser()
    for profile in ("release", "debug"):
        candidates.append(target / profile / "vmon")
        if target.exists():
            candidates.extend(sorted(target.glob(f"*/{profile}/vmon")))

    if found := shutil.which("vmon"):
        candidates.append(Path(found))

    seen: set[Path] = set()
    for path in candidates:
        path = path.resolve()
        if path in seen:
            continue
        seen.add(path)
        if _is_executable(path):
            return path
    raise RuntimeError("vmon binary not found; run `cargo build --release` or set VMON_BIN")


def hypervisor_present() -> bool:
    """True when the host can launch real microVMs."""
    system = platform.system()
    if system == "Linux":
        return Path("/dev/kvm").exists()
    if system == "Darwin":
        try:
            out = subprocess.run(
                ["sysctl", "-n", "kern.hv_support"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=3,
                check=False,
            )
            return out.stdout.strip() == "1"
        except OSError, subprocess.SubprocessError:
            return False
    return False


def _run_host(
    argv: Sequence[object], *, env: dict[str, str] | None = None, timeout: float = 30
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(part) for part in argv],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )


def _toml_string(value: Path | str) -> str:
    return json.dumps(str(value))


def _tail(path: Path, lines: int = 80) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(data[-lines:])


class VmonServer:
    """A local ``vmon serve`` process scoped to this e2e run's VMON_HOME."""

    def __init__(self, binary: Path, home: Path) -> None:
        self.binary = binary
        self.home = home
        self.sock = home / "vmond.sock"
        self.log_path = home / "e2e-serve.log"
        self.config_path = home / "e2e-serve.toml"
        self.proc: subprocess.Popen[str] | None = None
        self._log_fh: io.TextIOWrapper | None = None

    def env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["VMON_HOME"] = str(self.home)
        env["VMON_BIN"] = str(self.binary)
        env.pop("VMON_CONTEXT", None)
        env.pop("VMON_CONFIG", None)
        # Root-only network cases cannot launch the VMM sandbox without a
        # non-root sandbox identity, which the test server does not configure.
        if os.geteuid() == 0:
            env.setdefault("VMON_NO_SANDBOX", "1")
        return env

    def write_config(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            "\n".join(
                [
                    "[serve]",
                    f"home = {_toml_string(self.home)}",
                    'host = "127.0.0.1"',
                    "port = 0",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def run_doctor_serve(self) -> None:
        check = _run_host(
            [self.binary, "doctor", "--serve", "--config", self.config_path],
            env=self.env(),
            timeout=30,
        )
        if check.returncode != 0:
            raise RuntimeError(f"`vmon doctor --serve` failed:\n{check.stdout.strip()}")

    def run_doctor(self) -> None:
        check = _run_host([self.binary, "doctor"], env=self.env(), timeout=30)
        if check.returncode != 0:
            raise RuntimeError(f"`vmon doctor` failed:\n{check.stdout.strip()}")

    def healthz(self) -> vmon.Health:
        with connect(f"vmon+unix://{self.sock}", discover=False) as client:
            return client.health()

    def start(self) -> None:
        self.write_config()
        self.run_doctor_serve()
        self._log_fh = self.log_path.open("w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [
                str(self.binary),
                "serve",
                "--home",
                str(self.home),
                "--host",
                "127.0.0.1",
                "--port",
                "0",
            ],
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
            text=True,
            env=self.env(),
            start_new_session=True,
        )
        deadline = time.time() + 20
        last: BaseException | None = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"`vmon serve` exited with {self.proc.returncode}; log:\n{_tail(self.log_path)}"
                )
            try:
                if self.healthz().ok:
                    self.run_doctor()
                    return
            except BaseException as exc:
                last = exc
            time.sleep(0.1)
        raise RuntimeError(
            f"`vmon serve` never became healthy ({last}); log:\n{_tail(self.log_path)}"
        )

    def stop(self) -> None:
        proc = self.proc
        if proc is not None and proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
        if self._log_fh is not None:
            with contextlib.suppress(Exception):
                self._log_fh.close()


def pool_stats() -> dict[str, dict[str, object]]:
    return {pool.ref: dict(pool.stats().raw) for pool in sdk().pools.list()}


def pool_counter(field: str) -> int:
    total = 0
    for stats in pool_stats().values():
        value = stats.get(field, 0)
        if isinstance(value, int | str) and not isinstance(value, bool):
            total += int(value)
    return total


def snapshot_names() -> set[str]:
    return set(sdk().snapshots.list())


def rm_snapshot(name: str) -> None:
    for root in (STATE / "snapshots", STATE / "templates"):
        shutil.rmtree(root / name, ignore_errors=True)


def is_missing_error(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    if isinstance(exc, APIError):
        return exc.status == 404 or "No such file or directory" in str(exc)
    return isinstance(exc, OSError)


def assert_missing(fn: Callable[[], object], message: str) -> None:
    try:
        fn()
    except BaseException as exc:
        if is_missing_error(exc):
            return
        raise
    raise AssertionError(message)


def wait_view(
    sb: Sandbox, deadline: float, predicate: Callable[[dict[str, object]], bool]
) -> dict[str, object]:
    end = time.time() + deadline
    last: dict[str, object] = dict(sb.info.raw)
    while time.time() < end:
        last = dict(sb.refresh().raw)
        if predicate(last):
            return last
        time.sleep(0.2)
    return dict(sb.refresh().raw)


def scan_json_for_value(root: Path, needle: str) -> list[Path]:
    hits: list[Path] = []
    skip_dirs = {"assets", "images"}
    for path in root.rglob("*.json"):
        if any(part in skip_dirs for part in path.parts):
            continue
        try:
            if path.stat().st_size > 1_000_000:
                continue
            if needle in path.read_text(encoding="utf-8", errors="replace"):
                hits.append(path)
        except OSError:
            continue
    return hits


def vm_meta(name: str) -> dict[str, object]:
    path = STATE / "vms" / name / "meta.json"
    return json.loads(path.read_text(encoding="utf-8"))


def net_admin() -> bool:
    """True if the host can create TAP devices (Linux + root/CAP_NET_ADMIN)."""
    if platform.system() != "Linux":
        return False
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return True
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("CapEff:"):
                mask = int(line.split()[1], 16)
                return bool(mask & (1 << 12))  # CAP_NET_ADMIN
    except OSError, ValueError, IndexError:
        return False
    return False


def networking_supported() -> bool:
    """True if networked sandboxes can run on this host."""
    return platform.system() == "Darwin" or net_admin()


def tap_name(name: str) -> str:
    return "tv" + hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]


def lease_entry(name: str) -> dict[str, object]:
    path = STATE / "network" / "leases.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    value = data.get(name)
    if not isinstance(value, dict):
        raise AssertionError(f"lease for {name!r} missing from {path}: {data}")
    return value


def guest_ip_from_network(network: str) -> str:
    net = ipaddress.ip_network(network, strict=False)
    return str(net.network_address + 2)


def _fetch_once(host: str, port: int) -> bytes:
    with socket.create_connection((host, port), timeout=2) as conn:
        conn.sendall(b"GET / HTTP/1.0\r\n\r\n")
        conn.settimeout(2)
        body = b""
        while True:
            try:
                chunk = conn.recv(256)
            except TimeoutError:
                break
            if not chunk:
                break
            body += chunk
        return body


def wait_for_tunnel(host: str, port: int, marker: bytes, *, deadline: float) -> bytes:
    end = time.time() + deadline
    last = b""
    while time.time() < end:
        try:
            last = _fetch_once(host, port)
            if marker in last:
                return last
        except OSError:
            pass
        time.sleep(0.2)
    return last


class VirtiofsUnavailable(RuntimeError):
    pass


def mount_virtiofs(sb: Sandbox, tag: str, mountpoint: str, *, ro: bool = True) -> None:
    rc, _, err = run(sb, "mkdir", "-p", mountpoint, timeout=15)
    assert rc == 0, f"mkdir {mountpoint} failed: {err!r}"
    argv = ["mount", "-t", "virtiofs"]
    if ro:
        argv.extend(["-o", "ro"])
    argv.extend([tag, mountpoint])
    rc, out, err = run(sb, *argv, timeout=20)
    if rc == 0:
        return
    msg = f"{out}\n{err}".lower()
    if "no such device" in msg or "unknown filesystem" in msg or "invalid argument" in msg:
        raise VirtiofsUnavailable(msg.strip())
    raise AssertionError(f"virtio-fs mount failed: rc={rc} out={out!r} err={err!r}")


def virtiofs_supported() -> bool:
    """True if the guest kernel can mount virtio-fs (cached one-shot probe)."""
    global _virtiofs
    if _virtiofs is not None:
        return _virtiofs
    share = Path(tempfile.mkdtemp(prefix="vmon-vfs-probe-"))
    sb: Sandbox | None = None
    try:
        sb = make_sandbox(fs_dir=str(share))
        mount_virtiofs(sb, "host", "/mnt/_vfs_probe", ro=True)
        _virtiofs = True
    except VirtiofsUnavailable:
        _virtiofs = False
    finally:
        if sb is not None:
            with contextlib.suppress(Exception):
                sb.terminate()
        shutil.rmtree(share, ignore_errors=True)
    return _virtiofs


def require_virtiofs() -> None:
    """Skip the calling test unless the guest kernel can mount virtio-fs."""
    if not virtiofs_supported():
        raise Skip("guest kernel lacks virtio-fs; set VMON_KERNEL to a virtio-fs-capable kernel")


# --------------------------------------------------------------------------- #
# tests
# --------------------------------------------------------------------------- #


@e2e
def t_exec(_):
    """exec returns stdout + exit code; env and workdir are honored."""
    sb = make_sandbox()
    try:
        rc, out, _ = run(sb, "sh", "-lc", "printf ok")
        assert rc == 0 and out == "ok", f"rc={rc} out={out!r}"
        _, env_out, _ = run(sb, "sh", "-lc", 'printf %s "$E2E"', env={"E2E": "val"})
        assert env_out == "val", f"env not injected: {env_out!r}"
        _, wd_out, _ = run(sb, "pwd", workdir="/tmp")
        assert wd_out.strip() == "/tmp", f"workdir ignored: {wd_out!r}"
    finally:
        sb.terminate()


@e2e
def t_rng(_):
    """The guest exposes a working /dev/hwrng entropy device by default."""
    sb = make_sandbox(memory=256)
    try:
        rc, out, err = run(
            sb,
            "sh",
            "-lc",
            "test -c /dev/hwrng && dd if=/dev/hwrng bs=32 count=1 2>/dev/null | wc -c",
        )
        assert rc == 0, f"hwrng check failed: rc={rc} err={err!r}"
        assert out.strip() == "32", f"expected 32 entropy bytes from /dev/hwrng, got {out!r}"
    finally:
        sb.terminate()


@e2e
def t_pty(_):
    """pty exec presents a tty on stdout; plain exec does not; resize is accepted."""
    sb = make_sandbox()
    try:
        _, tty, _ = run(sb, "sh", "-lc", "test -t 1 && echo TTY || echo NOTTY", pty=True)
        assert "TTY" in tty and "NOTTY" not in tty, f"pty stdout not a tty: {tty!r}"
        _, notty, _ = run(sb, "sh", "-lc", "test -t 1 && echo TTY || echo NOTTY")
        assert "NOTTY" in notty, f"plain exec unexpectedly a tty: {notty!r}"
        proc = sb.exec("sh", pty=True, timeout=15)
        proc.resize(40, 120)
        proc.stdin.write("exit\n")
        assert proc.wait(timeout=15).code == 0
    finally:
        sb.terminate()


@e2e
def t_filesystem(_):
    """Filesystem RPC: mkdir, write/read round-trip, list, stat, remove."""
    sb = make_sandbox()
    try:
        fs = sb.files
        fs.mkdir("/e2e/sub")
        fs.write_text("/e2e/sub/file.txt", "hello-fs")
        assert fs.read_text("/e2e/sub/file.txt") == "hello-fs"
        assert any(item.name == "file.txt" for item in fs.list("/e2e/sub"))
        assert fs.stat("/e2e/sub/file.txt").size > 0
        fs.delete("/e2e/sub/file.txt")
        assert_missing(
            lambda: fs.read_text("/e2e/sub/file.txt"), "read of a removed file should fail"
        )
    finally:
        sb.terminate()


@e2e
def t_suspend_resume(_):
    """pause parks the vCPUs without allowing guest processes to run."""
    sb = make_sandbox()
    heartbeat = None

    def heartbeat_size() -> int:
        rc, out, err = run(sb, "sh", "-lc", "wc -c < /root/pause-heartbeat")
        assert rc == 0, f"heartbeat read failed: rc={rc} err={err!r}"
        return int(out.strip())

    try:
        sb.files.write_text("/root/pre-pause", "kept")
        heartbeat = sb.exec(
            "sh",
            "-lc",
            "while :; do printf x >> /root/pause-heartbeat; sleep 0.1; done",
            timeout=30,
        )
        deadline = time.time() + 5
        start = 0
        while start < 3 and time.time() < deadline:
            time.sleep(0.1)
            start = heartbeat_size()
        assert start >= 3, f"guest heartbeat never started (size={start})"

        sb.pause()
        try:
            time.sleep(3)
        finally:
            sb.resume()
        end = heartbeat_size()

        # A running guest adds roughly 30 beats during the host sleep. A parked
        # guest can add one expired-timer beat on resume plus RPC scheduling noise.
        assert end - start < 10, f"guest heartbeat advanced {end - start} times while paused"
        assert sb.files.read_text("/root/pre-pause") == "kept"
        rc, out, _ = run(sb, "sh", "-lc", "echo alive")
        assert rc == 0 and "alive" in out
    finally:
        if heartbeat is not None:
            heartbeat.close()
        sb.terminate()


@e2e
def t_snapshot_restore(_):
    """snapshot then warm restore preserves disk state and continues the guest clock."""
    sb = make_sandbox(memory=256)
    name = uid("snap")
    restored = None
    try:
        sb.files.write_text("/root/marker", "snapshotted")
        # Grow guest uptime well past a fresh boot so a cold reboot (uptime ~0)
        # cannot masquerade as a warm restore.
        deadline = time.time() + 20
        u0 = uptime(sb)
        while u0 < 6 and time.time() < deadline:
            time.sleep(0.5)
            u0 = uptime(sb)
        assert u0 >= 6, f"guest uptime never reached 6s (got {u0:.1f}); cannot prove restore"
        sb.snapshot(name)
        restored = sdk().snapshots.restore(name, block_network=True)
        assert restored.files.read_text("/root/marker") == "snapshotted"
        u1 = uptime(restored)
        assert u1 >= u0 - 1, (
            f"restored uptime {u1:.1f} < pre-snapshot {u0:.1f} - 1; cold boot, not restore"
        )
        rc, out, _ = run(restored, "sh", "-lc", "echo warm")
        assert rc == 0 and "warm" in out
    finally:
        if restored is not None:
            restored.terminate()
        sb.terminate()
        rm_snapshot(name)


@e2e
def t_fork(_):
    """CoW fork: clones share the base image but writes stay private to each clone."""
    sb = make_sandbox(memory=256)
    name = uid("forkbase")
    clones: list[Sandbox] = []
    try:
        sb.files.write_text("/root/shared", "base")
        sb.snapshot(name)
        clones = sdk().snapshots.fork(name, count=2, block_network=True)
        for c in clones:
            assert c.files.read_text("/root/shared") == "base"
        clones[0].files.write_text("/root/only0", "c0")
        assert_missing(
            lambda: clones[1].files.read_text("/root/only0"),
            "fork clones are not CoW-isolated",
        )
    finally:
        for c in clones:
            try:
                c.terminate()
            except Exception:
                pass
        sb.terminate()
        rm_snapshot(name)


@e2e
def t_delta_snapshot(_):
    """Successive public v1 snapshots are listed and restore independent states."""
    source: Sandbox | None = make_sandbox(memory=256)
    base, delta = uid("dbase"), uid("ddelta")
    restored_base: Sandbox | None = None
    restored_delta: Sandbox | None = None
    try:
        source.files.write_text("/root/fill", "base")
        source.snapshot(base)
        assert base in snapshot_names(), f"base snapshot {base!r} missing from snapshot list"
        rc, _, err = run(source, "sh", "-lc", "echo post-base > /root/fill && sync")
        assert rc == 0, f"post-base mutation failed: {err!r}"
        source.snapshot(delta)
        assert delta in snapshot_names(), f"delta snapshot {delta!r} missing from snapshot list"
        source.terminate()
        source = None

        restored_base = sdk().snapshots.restore(base, block_network=True)
        assert restored_base.files.read_text("/root/fill").strip() == "base", (
            "base snapshot did not preserve pre-mutation state"
        )
        restored_delta = sdk().snapshots.restore(delta, block_network=True)
        assert restored_delta.files.read_text("/root/fill").strip() == "post-base", (
            "successive snapshot did not preserve post-mutation state"
        )
        rc, out, _ = run(restored_delta, "sh", "-lc", "echo snapshot-ok")
        assert rc == 0 and "snapshot-ok" in out
    finally:
        for item in (restored_delta, restored_base, source):
            if item is not None:
                with contextlib.suppress(Exception):
                    item.terminate()
        rm_snapshot(delta)
        rm_snapshot(base)


@e2e
def t_snapshot_filesystem_image(_):
    """snapshot_filesystem yields a template that client.sandboxes.create can boot."""
    sb = make_sandbox(memory=256)
    img = uid("img")
    clone = None
    try:
        sb.files.write_text("/root/img-marker", "image")
        sb.snapshot_filesystem(img)
        clone = make_sandbox(template=img)
        assert clone.files.read_text("/root/img-marker") == "image"
    finally:
        if clone is not None:
            clone.terminate()
        sb.terminate()
        rm_snapshot(img)


@e2e
def t_volume_persist(_):
    """A writable named volume keeps data across sandbox lifetimes."""
    require_virtiofs()
    name = uid("vol")
    VOLUMES.add(name)
    sb1 = make_sandbox(volumes={"/data": Volume(name)})
    try:
        rc, _, err = run(sb1, "sh", "-lc", "echo persisted > /data/x && sync")
        assert rc == 0, f"volume write failed: {err!r}"
    finally:
        sb1.terminate()
    sb2 = make_sandbox(volumes={"/data": Volume(name)})
    try:
        assert sb2.files.read_text("/data/x").strip() == "persisted"
    finally:
        sb2.terminate()


@e2e
def t_volume_readonly(_):
    """A read-only volume is readable but rejects writes."""
    require_virtiofs()
    name = uid("rovol")
    VOLUMES.add(name)
    sb = make_sandbox(volumes={"/data": Volume(name)})
    try:
        run(sb, "sh", "-lc", "echo seed > /data/seed && sync")
    finally:
        sb.terminate()
    sb2 = make_sandbox(volumes={"/data": (Volume(name), True)})
    try:
        assert sb2.files.read_text("/data/seed").strip() == "seed"
        rc, _, _ = run(sb2, "sh", "-lc", "echo nope > /data/seed2")
        assert rc != 0, "write to a read-only volume unexpectedly succeeded"
    finally:
        sb2.terminate()


@e2e
def t_host_share(_):
    """A read-only host directory is visible inside the guest over virtio-fs."""
    require_virtiofs()
    share = Path(tempfile.mkdtemp(prefix="e2e-share-"))
    (share / "hello.txt").write_text("from-host")
    sb = make_sandbox(fs_dir=str(share))
    try:
        mount_virtiofs(sb, "host", "/mnt/host", ro=True)
        assert sb.files.read_text("/mnt/host/hello.txt") == "from-host"
    finally:
        sb.terminate()
        shutil.rmtree(share, ignore_errors=True)


@e2e
def t_secrets(_):
    """Secrets reach the exec env but are never written into meta.json."""
    value = "s3cr3t-" + RUN
    sb = make_sandbox(secrets=[Secret.from_dict({"E2E_SECRET": value})])
    try:
        _, out, _ = run(sb, "sh", "-lc", 'printf %s "$E2E_SECRET"')
        assert out == value, f"secret not injected: {out!r}"
        view = sb.refresh()
        assert "secret" in (view.raw.get("secret_names") or []), "secret bundle name not recorded"
        hits = scan_json_for_value(STATE, value)
        assert not hits, f"secret value leaked into persisted JSON: {hits}"
    finally:
        sb.terminate()


@e2e
def t_network_block(_):
    """A block_network sandbox boots and exec works, but has no guest egress device."""
    sb = make_sandbox(block_network=True)
    try:
        rc, out, _ = run(sb, "sh", "-lc", "echo up")
        assert rc == 0 and "up" in out, "block_network sandbox failed to boot/exec"
        rc, addr, err = run(sb, "ip", "-4", "addr", "show", "scope", "global", timeout=15)
        assert rc == 0, f"ip addr failed in block_network sandbox: rc={rc} err={err!r}"
        assert "inet " not in addr, f"block_network sandbox has a guest IPv4 address: {addr!r}"
        rc, route, err = run(sb, "ip", "route", timeout=15)
        assert rc == 0, f"ip route failed in block_network sandbox: rc={rc} err={err!r}"
        assert "default" not in route, f"block_network sandbox has a default route: {route!r}"
    finally:
        sb.terminate()


@e2e
def t_network_lease(_):
    """Host-side TAP networking records a deterministic, non-overlapping lease."""
    if not net_admin():
        raise Skip("needs Linux root/CAP_NET_ADMIN for host TAP lease state")
    name = uid("net")
    sb = sdk().sandboxes.create(image=IMAGE, name=name, block_network=False)
    try:
        entry = lease_entry(name)
        network = str(entry.get("network") or "")
        assert network.endswith("/30"), f"lease network is not /30: {entry}"
        assert entry.get("tap") == tap_name(name), f"unexpected TAP name: {entry}"
        guest_ip = guest_ip_from_network(network)
        rc, addr, err = run(sb, "ip", "-4", "addr", "show", "eth0", timeout=15)
        assert rc == 0, f"ip addr failed: {err!r}"
        assert guest_ip in addr, f"guest did not receive leased IP {guest_ip}: {addr!r}"
    finally:
        sb.remove()


@e2e
def t_networking(_):
    """A networked sandbox fresh-boots with a guest IPv4 address and default route."""
    if not networking_supported():
        raise Skip("needs macOS/HVF user-net or Linux root/CAP_NET_ADMIN")
    sb = sdk().sandboxes.create(image=IMAGE, block_network=False)
    try:
        _, addr, _ = run(sb, "ip", "-4", "addr", "show", "eth0")
        assert "inet " in addr, f"eth0 has no IPv4: {addr!r}"
        _, route, _ = run(sb, "ip", "route")
        assert "default" in route, f"no default route: {route!r}"
    finally:
        sb.terminate()


@e2e
def t_ports_tunnels(_):
    """An exposed guest port is reachable end-to-end through the host tunnel."""
    if not net_admin():
        raise Skip("needs Linux root/CAP_NET_ADMIN; macOS user-net has no inbound forwarding")
    sb = sdk().sandboxes.create(image=IMAGE, ports=[18080])
    listener = None
    try:
        tunnels = sb.tunnels()
        assert 18080 in tunnels, f"port 18080 not tunneled: {tunnels}"
        host, hport = tunnels[18080]
        assert host and int(hport) > 0, tunnels
        # Serve a known response inside the guest, then fetch it through the tunnel.
        listener = sb.exec(
            "sh",
            "-lc",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nhi'"
            " | nc -l -p 18080; done",
            timeout=60,
        )
        body = wait_for_tunnel(host, int(hport), b"hi", deadline=15)
        assert b"hi" in body, f"tunnel did not deliver the guest response: {body!r}"
    finally:
        if listener is not None:
            try:
                listener.close()
            except Exception:
                pass
        sb.terminate()


@e2e
def t_tags_and_list(_):
    """Tags persist on the sandbox view and the sandbox appears in v1 listing."""
    sb = make_sandbox(tags={"e2e": "yes", "run": RUN})
    try:
        view = sb.refresh()
        assert view.tags.get("e2e") == "yes"
        assert view.tags.get("run") == RUN
        assert sb.id in [item.id for item in sdk().sandboxes.list(tags={"run": RUN})]
    finally:
        sb.terminate()


@e2e
def t_warm_pool(_):
    """A server-owned warm pool is stocked and consumed by client.sandboxes.create."""
    handle = None
    claimed: Sandbox | None = None
    try:
        handle = sdk().pools.set(IMAGE, 1, memory=256)
        end = time.time() + 180
        while time.time() < end and pool_counter("ready") < 1:
            time.sleep(0.5)
        ready = pool_counter("ready")
        assert ready >= 1, f"pool never warmed: {pool_stats()}"
        before_hits = pool_counter("hits")
        claimed = make_sandbox(memory=256)
        rc, out, _ = run(claimed, "sh", "-lc", "echo pooled")
        assert rc == 0 and "pooled" in out
        after_hits = pool_counter("hits")
        assert after_hits > before_hits, (
            f"pool hit counter did not increase: {before_hits}->{after_hits}"
        )
    finally:
        if claimed is not None:
            with contextlib.suppress(Exception):
                claimed.remove()
        if handle is not None:
            sdk().pools.clear()


@e2e
def t_timeout(_):
    """A server-enforced deadline self-terminates with return code 124."""
    sb = make_sandbox(timeout_secs=3)
    try:
        view = wait_view(
            sb,
            30,
            lambda value: value.get("status") != "running" and value.get("returncode") == 124,
        )
        assert view.get("returncode") == 124, f"timeout returncode not observed: {view}"
    finally:
        with contextlib.suppress(Exception):
            sb.remove()


@e2e
def t_extend(_):
    """extend moves the deadline so the sandbox survives past its original timeout."""
    sb = make_sandbox(timeout_secs=4)
    try:
        result = sb.extend(60)
        assert "deadline_unix" in result.raw, f"extend did not return a deadline: {result}"
        time.sleep(7)  # past the original 4s deadline
        view = sb.refresh()
        assert view.status == "running", (
            f"sandbox died at the original deadline despite extend: {view}"
        )
    finally:
        with contextlib.suppress(Exception):
            sb.remove()


def _py_image() -> vmon.Image:
    return vmon.Image.from_registry(os.environ.get("VMON_E2E_PYTHON_IMAGE", "python:3.14-slim"))


def _function_options(
    *,
    serializer: SerializerPolicy | None = None,
    min_workers: int = 0,
) -> vmon.FunctionOptions:
    selected_serializer = serializer or SerializerPolicy()
    image = _py_image()
    if ValueCodec.CLOUDPICKLE in (
        selected_serializer.input_serializer,
        selected_serializer.result_serializer,
    ):
        image = image.uv_install("cloudpickle==3.1.2")
    return vmon.FunctionOptions(
        image=image,
        timeouts=vmon.TimeoutPolicy(execution=120),
        workers=vmon.WorkerPolicy(min_workers=min_workers),
        serializer=selected_serializer,
        resources=vmon.ResourcePolicy(
            network=vmon.NetworkPolicy(block_network=True),
        ),
    )


_RemoteCounter = vmon.cls(options=_function_options())(RemoteCounter)


@e2e
def t_remote_function_warm(_):
    """Durable calls preserve warm execution and structured remote failures."""
    remote = sdk().function(
        remote_double,
        options=_function_options(min_workers=1),
    )
    cold_started = time.monotonic()
    assert remote.remote(21) == 42
    cold = time.monotonic() - cold_started
    warm_started = time.monotonic()
    assert remote.remote(4) == 8
    warm = time.monotonic() - warm_started
    try:
        remote.remote(-1)
    except RemoteFunctionError as exc:
        assert "non-negative" in str(exc)
        assert exc.remote_type.endswith("ValueError")
        assert exc.code
    else:
        raise AssertionError("remote exception was not propagated")
    print(f"    remote(): cold={cold:.3f}s warm={warm:.4f}s ({cold / max(warm, 1e-9):.0f}x)")
    assert warm < cold / 10, f"warm call too slow: cold={cold:.3f}s warm={warm:.3f}s"


@e2e
def t_remote_function_rich_args(_):
    """Explicit trusted-Python serialization preserves rich Python values."""
    import datetime

    remote = sdk().function(
        remote_shift,
        options=_function_options(
            serializer=SerializerPolicy(
                input_serializer=ValueCodec.CLOUDPICKLE,
                result_serializer=ValueCodec.CLOUDPICKLE,
                allow_trusted_python=True,
            )
        ),
    )
    stamp = datetime.datetime(2026, 7, 11, 8, 30, 0)
    shifted, swapped = remote.remote(stamp, (1, 2))
    assert shifted == stamp + datetime.timedelta(days=1), f"datetime lost: {shifted!r}"
    assert swapped == (2, 1) and isinstance(swapped, tuple), f"tuple lost: {swapped!r}"


@e2e
def t_remote_function_stream(_):
    """Durable generator yields arrive incrementally."""
    remote = sdk().function(
        remote_ticks,
        options=_function_options(),
    )
    started = time.monotonic()
    arrivals: list[tuple[int, float]] = []
    for value in remote.remote_gen():
        arrivals.append((value, time.monotonic() - started))
    assert [value for value, _at in arrivals] == [0, 1, 2]
    total = arrivals[-1][1]
    first = arrivals[0][1]
    assert total >= 0.35, f"generator finished too fast to prove streaming: {total:.3f}s"
    assert first < total - 0.3, f"first yield arrived late: first={first:.3f}s total={total:.3f}s"


@e2e
def t_remote_function_spawn_map(_):
    """Detached calls reconstruct by ID and lazy maps preserve input order."""
    remote = sdk().function(
        remote_add,
        options=_function_options(),
    )
    first = remote.spawn(1, 2)
    second = remote.spawn(30, 12)
    reconstructed = FunctionCall.from_id(first.id, client=sdk())
    assert FunctionCall.gather(reconstructed, second) == [3, 42]
    assert first.done() and second.done()

    assert list(remote.map(range(6), kwargs={"b": 10}, max_in_flight=2)) == [
        10,
        11,
        12,
        13,
        14,
        15,
    ]
    mixed = list(
        remote.map(
            [1, "x"],
            kwargs={"b": 1},
            return_exceptions=True,
            max_in_flight=1,
        )
    )
    assert mixed[0] == 2
    failure = mixed[1]
    assert isinstance(failure, RemoteFunctionError), (
        f"expected RemoteFunctionError slot, got {failure!r}"
    )
    assert failure.remote_type.endswith("TypeError")


@e2e
def t_remote_cls(_):
    """A durable actor preserves state and is explicitly deleted."""
    app = vmon.App("python-e2e-actors", client=sdk())
    app.add(_RemoteCounter)
    app.deploy()
    counter = _RemoteCounter(start=40)
    deleted = False
    try:
        assert counter.bump.remote() == 41, "constructor + enter hook must run"
        assert counter.bump.remote() == 42, "state must persist across calls"
        try:
            _ = counter.value
        except AttributeError as exc:
            assert "@vmon.method" in str(exc)
        else:
            raise AssertionError("untagged attribute access must fail")
        started = time.monotonic()
        counter.delete()
        deleted = True
        elapsed = time.monotonic() - started
        assert elapsed < 10, f"delete exceeded the exit-hook grace period: {elapsed:.1f}s"
    finally:
        if not deleted:
            with contextlib.suppress(Exception):
                counter.delete()


@e2e
def t_async(_):
    """The aio facade mirrors the sync SDK for exec."""
    sb = make_sandbox()
    try:

        async def go():
            proc = await sb.aio.exec("sh", "-lc", "printf aio")
            result = proc.wait(timeout=15)
            return result.code, proc.stdout.read().decode()

        rc, out = asyncio.run(go())
        assert rc == 0 and out == "aio", f"rc={rc} out={out!r}"
    finally:
        sb.terminate()


# --------------------------------------------------------------------------- #
# runner
# --------------------------------------------------------------------------- #


def preflight(home: Path) -> tuple[VmonServer | None, str | None]:
    """Start ``vmon serve`` for this run, or return a human-readable skip reason."""
    system = platform.system()
    if system not in ("Linux", "Darwin"):
        return None, f"needs Linux + /dev/kvm or macOS + Hypervisor.framework; this is {system}"
    if not hypervisor_present():
        return (
            None,
            "/dev/kvm not present; run on a KVM host (or a nested-KVM Lima VM)"
            if system == "Linux"
            else "no usable hypervisor; needs an Apple-silicon Mac with "
            "Hypervisor.framework (kern.hv_support=1)",
        )
    try:
        binary = find_vmon_binary()
    except RuntimeError as exc:
        return None, str(exc)
    server = VmonServer(binary, home)
    try:
        server.start()
    except RuntimeError as exc:
        server.stop()
        return None, str(exc)
    return server, None


def display_name(fn: Callable[[object], None]) -> str:
    return fn.__name__[2:] if fn.__name__.startswith("t_") else fn.__name__


def main(argv: Sequence[str] | None = None) -> int:
    global IMAGE, _client
    ap = argparse.ArgumentParser(description="vmon end-to-end smoke test")
    ap.add_argument("tests", nargs="*", help="substring filters (default: run all)")
    ap.add_argument("--list", action="store_true", help="list test names and exit")
    ap.add_argument("--keep", action="store_true", help="leave the temp VMON_HOME in place")
    ap.add_argument("--image", help="container image (default $VMON_E2E_IMAGE or alpine:latest)")
    args = ap.parse_args(argv)

    if args.list:
        for fn in TESTS:
            print(display_name(fn))
        return 0
    if args.image:
        IMAGE = args.image

    selected = [
        fn for fn in TESTS if not args.tests or any(f in display_name(fn) for f in args.tests)
    ]

    tmp_parent = Path(
        os.environ.get(
            "VMON_E2E_TMPDIR", "/tmp" if Path("/tmp").is_dir() else tempfile.gettempdir()
        )
    )
    tmp_parent.mkdir(parents=True, exist_ok=True)
    home = Path(tempfile.mkdtemp(prefix=f"ve2e{os.getpid()}-", dir=tmp_parent))
    configure_home(home)
    server: VmonServer | None = None
    try:
        server, reason = preflight(home)
        if reason:
            print(f"SKIP: {reason}")
            return 0

        print(f"e2e: image={IMAGE} state={STATE} tests={len(selected)}")

        results: list[tuple[str, str, float, str]] = []
        for fn in selected:
            name = display_name(fn)
            t0 = time.time()
            try:
                fn(None)
                dt = time.time() - t0
                results.append(("PASS", name, dt, ""))
                print(f"  PASS  {name}  ({dt:.1f}s)")
            except Skip as exc:
                results.append(("SKIP", name, time.time() - t0, str(exc)))
                print(f"  SKIP  {name}: {exc}")
            except BaseException as exc:  # noqa: BLE001 - report and continue
                results.append(("FAIL", name, time.time() - t0, repr(exc)))
                print(f"  FAIL  {name}: {exc}")
                traceback.print_exc()

        npass = sum(r[0] == "PASS" for r in results)
        nfail = sum(r[0] == "FAIL" for r in results)
        nskip = sum(r[0] == "SKIP" for r in results)
        print(f"\n{'=' * 52}\n{npass} passed, {nfail} failed, {nskip} skipped")
        if nfail:
            print("failed: " + ", ".join(r[1] for r in results if r[0] == "FAIL"))
        return 1 if nfail else 0
    finally:
        if _client is not None:
            _client.close()
            _client = None
        if server is not None:
            server.stop()
        if args.keep:
            print(f"kept VMON_HOME={home}")
        else:
            shutil.rmtree(home, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
