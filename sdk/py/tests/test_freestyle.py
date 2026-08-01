from __future__ import annotations

from typing import Any, cast

import pytest

from vmon.client import Client
from vmon.errors import APIError
from vmon.freestyle import Freestyle, PtySession, Vm
from vmon.models import ExecExit, FileInfo, SandboxInfo
from vmon.process import ByteStream
from vmon.sandbox import ExecResult


class FakePtyStream:
    def __init__(self) -> None:
        self.session_id = "pty-1"
        self.stdout = ByteStream()
        self.wait_calls: list[float | None] = []

    def wait(self, timeout: float | None = None) -> ExecExit:
        self.wait_calls.append(timeout)
        return ExecExit(code=23, signal=None)


class FakeSandbox:
    def __init__(self, identifier: str = "vm-1") -> None:
        self.id = identifier
        self.info = SandboxInfo(id=identifier, status="running")
        self.files = FakeFiles()
        self.run_calls: list[tuple[list[str], float | None]] = []
        self.lifecycle_calls: list[tuple[str, float | None]] = []
        self.resize_calls: list[dict[str, Any]] = []
        self.snapshot_names: list[str | None] = []
        self.terminated = False
        self.removed = False

    def run(self, command: list[str], *, timeout: float | None = None) -> ExecResult:
        self.run_calls.append((command, timeout))
        return ExecResult(returncode=7, stdout=b"out", stderr=b"err")

    def resume(self) -> SandboxInfo:
        self.lifecycle_calls.append(("resume", None))
        return self.info

    def set_idle_timeout(self, seconds: float) -> SandboxInfo:
        self.lifecycle_calls.append(("idle", seconds))
        return self.info

    def resize(self, **kwargs: Any) -> SandboxInfo:
        self.resize_calls.append(kwargs)
        return self.info

    def snapshot(self, name: str | None = None) -> str:
        self.snapshot_names.append(name)
        return "snap-1"

    def terminate(self) -> None:
        self.terminated = True

    def remove(self) -> None:
        self.removed = True


class FakeFiles:
    def __init__(self) -> None:
        self.stat_error: APIError | None = None

    def stat(self, _path: str) -> FileInfo:
        if self.stat_error is not None:
            raise self.stat_error
        return FileInfo(name="file", type="file", size=1, mode=0o644, mtime=0)


class FakeSandboxes:
    def __init__(self, sandbox: FakeSandbox) -> None:
        self.sandbox = sandbox
        self.create_calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> FakeSandbox:
        self.create_calls.append(kwargs)
        return self.sandbox

    def ref(self, _identifier: str) -> FakeSandbox:
        return self.sandbox


class FakeSnapshots:
    def __init__(self) -> None:
        self.fork_calls: list[tuple[str, int]] = []

    def fork(self, snapshot: str, *, count: int) -> list[FakeSandbox]:
        self.fork_calls.append((snapshot, count))
        return [FakeSandbox(f"fork-{index}") for index in range(count)]


class FakeClient:
    def __init__(self, sandbox: FakeSandbox | None = None) -> None:
        self.sandbox = sandbox or FakeSandbox()
        self.sandboxes = FakeSandboxes(self.sandbox)
        self.snapshots = FakeSnapshots()


def facade(client: FakeClient) -> Freestyle:
    return Freestyle.from_client(cast(Client, client))


def test_create_snapshots_default_and_fully_configured_specs() -> None:
    client = FakeClient()
    api = facade(client)

    default = api.vms.create()
    assert default.vm_id == "vm-1"
    assert client.sandboxes.create_calls[0] == {
        "image": None,
        "template": None,
        "cpus": 1,
        "memory": 512,
        "disk_mb": 1024,
        "nics": None,
        "name": None,
        "env": None,
        "workdir": None,
        "tags": None,
        "timeout": None,
        "timeout_secs": 0,
        "block_network": False,
        "activity_threshold_bytes": None,
        "persistence": None,
    }

    created = api.vms.create(
        name="mapped",
        idle_timeout_seconds=None,
        activity_threshold_bytes=4096,
        persistence={"type": "sticky", "priority": 99},
        nics=[{"vpc": "vpc-1", "mode": "routed", "ipv4": True}],
        image="alpine",
        cpu=4,
        memory=2,
        storage=8,
        env={"A": "B"},
        workdir="/app",
        tags={"suite": "mapping"},
    )
    assert created.vm_id == "vm-1"
    assert created.domains == []
    assert client.sandboxes.create_calls[1] == {
        "image": "alpine",
        "template": None,
        "cpus": 4,
        "memory": 2048,
        "disk_mb": 8192,
        "nics": [{"vpc": "vpc-1", "ipv4": True, "default": True}],
        "name": "mapped",
        "env": {"A": "B"},
        "workdir": "/app",
        "tags": {"suite": "mapping"},
        "timeout": None,
        "timeout_secs": 0,
        "block_network": False,
        "idle_timeout_secs": 0,
        "activity_threshold_bytes": 4096,
        "persistence": {"type": "sticky", "priority": 10},
    }


def test_exec_wraps_shell_and_maps_result() -> None:
    client = FakeClient()
    vm = Vm(cast(Client, client), cast(Any, client.sandbox))

    result = vm.exec("printf hi", timeout_ms=2500)

    assert client.sandbox.run_calls == [(["sh", "-c", "printf hi"], 2.5)]
    assert (result.stdout, result.stderr, result.status_code) == ("out", "err", 7)


def test_start_sets_idle_policy_before_resuming() -> None:
    sandbox = FakeSandbox()
    vm = Vm(cast(Client, FakeClient(sandbox)), cast(Any, sandbox))

    vm.start()
    vm.start(idle_timeout_seconds=30)
    vm.start(idle_timeout_seconds=None)

    assert sandbox.lifecycle_calls == [
        ("resume", None),
        ("idle", 30),
        ("resume", None),
        ("idle", 0),
        ("resume", None),
    ]


def test_resize_converts_units_and_rejects_before_rpc() -> None:
    sandbox = FakeSandbox()
    vm = Vm(cast(Client, FakeClient(sandbox)), cast(Any, sandbox))

    vm.resize(cpu=4, memory=2, storage=3)
    assert sandbox.resize_calls == [{"cpus": 4, "memory_mib": 2048, "disk_mb": 3072}]

    with pytest.raises(TypeError, match="cpu must be a power of two"):
        vm.resize(cpu=3)
    with pytest.raises(TypeError, match="memory must be a power of two"):
        vm.resize(memory=3)
    assert len(sandbox.resize_calls) == 1


def test_delete_tolerates_not_found() -> None:
    sandbox = FakeSandbox()

    def missing() -> None:
        raise APIError("gone", code="not_found", status=404)

    sandbox.remove = missing  # type: ignore[method-assign]
    facade(FakeClient(sandbox)).vms.delete("vm-1")
    assert not sandbox.removed


def test_exists_only_narrows_not_found() -> None:
    sandbox = FakeSandbox()
    vm = Vm(cast(Client, FakeClient(sandbox)), cast(Any, sandbox))
    sandbox.files.stat_error = APIError("missing", code="not_found")
    assert vm.fs.exists("/missing") is False

    denied = APIError("denied", code="permission_denied", status=403)
    sandbox.files.stat_error = denied
    with pytest.raises(APIError) as caught:
        vm.fs.exists("/secret")
    assert caught.value is denied


def test_fork_snapshots_then_maps_each_clone() -> None:
    client = FakeClient()
    vm = Vm(cast(Client, client), cast(Any, client.sandbox))

    result = vm.fork(count=2)

    assert client.sandbox.snapshot_names == [None]
    assert client.snapshots.fork_calls == [("snap-1", 2)]
    assert [fork.vm_id for fork in result.forks] == ["fork-0", "fork-1"]


def test_pty_session_exposes_output_and_exit_status() -> None:
    stream = FakePtyStream()
    session = PtySession(cast(Any, FakeSandbox()), cast(Any, stream))

    assert session.read(timeout=0) == b""

    stream.stdout.feed(b"step 1\r\n")
    stream.stdout.feed(b"step 2\r\n")
    stream.stdout.close()

    assert session.read(timeout=0.25) == b"step 1\r\n"
    assert list(session.iter_output()) == [b"step 2\r\n"]
    assert session.read(timeout=0) == b""
    assert session.wait(timeout=0.5) == 23
    assert stream.wait_calls == [0.5]
