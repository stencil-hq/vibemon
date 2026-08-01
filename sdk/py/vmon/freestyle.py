"""Freestyle-shaped VM compatibility subset over the vmon SDK.

This follows ``freestyle@0.1.63`` closely enough that VM code can usually port by
changing imports, but it is deliberately not a complete drop-in replacement.

Mapping onto vmon:
- ``api_key`` and ``access_token`` are vmon API tokens; ``base_url`` is the vmon DSN.
- VM creation has no wall-clock lease. ``idle_timeout_seconds=None`` disables
  network-idle reclaim. Snapshot restore resumes captured processes and memory rather
  than fresh-booting.
- ``stop`` discards in-memory state while retaining disk and identity; ``start`` resumes
  suspended VMs or fresh-boots stopped VMs.
- ``fork`` takes a full VM snapshot and atomically forks it; the snapshot is retained.
- PTY sessions are guest-owned and persistent. Attachments can detach and reconnect by
  stable id; sessions survive suspend/resume and memory-preserving forks.

Divergences from Freestyle:
- create always returns ``domains=[]`` and additionally accepts vmon image, template,
  sizing, environment, workdir, and tags options.
- persistence, activity thresholds, and terminal exec use vmon-native semantics.
  Ephemeral VMs discard stored state on stop/suspend; sticky VMs may be evicted by
  storage GC in ascending priority. One routed VPC NIC is supported on Linux hosts;
  macOS rejects VPC NICs.
- Files are bytes rather than Node Buffers; stat has no owner/group; snapshot timestamps
  are absent; PTY options are a simplified subset.
- There is no facade for git, domains, identities, DNS, serverless, cron, whoami, or raw
  fetch because vmon has no backing services for them.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Self

from .client import Client, connect
from .errors import APIError
from .models import SandboxInfo
from .process import PtyStream
from .sandbox import Sandbox


class _Unset:
    __slots__ = ()


_UNSET = _Unset()


@dataclass(frozen=True, slots=True)
class VmExecResult:
    """Captured output of one Vm.exec command."""

    stdout: str | None
    stderr: str | None
    status_code: int | None


@dataclass(frozen=True, slots=True)
class CreatedVm:
    """Result of Vms.create containing the VM instance, ID, and domains."""

    vm: Vm
    vm_id: str
    domains: list[str]


@dataclass(frozen=True, slots=True)
class VmFork:
    """One forked VM entry."""

    vm: Vm
    vm_id: str


@dataclass(frozen=True, slots=True)
class ForkedVms:
    """Result of Vm.fork containing the list of forked VMs."""

    forks: list[VmFork]


@dataclass(frozen=True, slots=True)
class VmSnapshot:
    """One snapshot row; vmon reports names only, so timestamps are absent."""

    snapshot_id: str
    name: str | None = None
    source_vm_id: str | None = None
    state: str | None = None
    created_at: str | None = None


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """Result of Vm.snapshot containing the snapshot and source VM identifiers."""

    snapshot_id: str
    source_vm_id: str


@dataclass(frozen=True, slots=True)
class SnapshotsResult:
    """Result of VmSnapshots.list containing the list of VM snapshots."""

    snapshots: list[VmSnapshot]


@dataclass(frozen=True, slots=True)
class VmListEntry:
    """One row of Vms.list."""

    id: str
    state: str
    created_at: str | None
    last_network_activity: str | None
    snapshot_id: str | None = None
    deleted: bool = False


@dataclass(frozen=True, slots=True)
class VmsResult:
    """Result of Vms.list containing VM entries and status counts."""

    vms: list[VmListEntry]
    total_count: int
    running_count: int
    starting_count: int
    suspended_count: int
    stopped_count: int


@dataclass(frozen=True, slots=True)
class GotVm:
    """Result of Vms.get containing the VM handle."""

    vm: Vm


@dataclass(frozen=True, slots=True)
class FsEntry:
    """One directory entry returned by VmFs.read_dir."""

    name: str
    kind: str


@dataclass(frozen=True, slots=True)
class FsStat:
    """File status information returned by VmFs.stat."""

    size: int
    is_file: bool
    is_directory: bool
    is_symlink: bool
    permissions: str
    modified: str
    owner: str | None = None
    group: str | None = None


@dataclass(frozen=True, slots=True)
class PtySessionInfo:
    """Session metadata reported by VmPty.list."""

    session_id: str
    running: bool
    exit_code: int | None
    cols: int
    rows: int
    exec: str | None
    created_at_ms: int
    attached_count: int
    suspended: bool


@dataclass(frozen=True, slots=True)
class PtySessionsResult:
    """Result of VmPty.list containing PTY session information."""

    sessions: list[PtySessionInfo]


@dataclass(frozen=True, slots=True)
class ClosedPty:
    """Result of closing a PTY session."""

    session_id: str
    exit_code: int | None


@dataclass(frozen=True, slots=True)
class FreestyleVpc:
    """VPC identity returned by the Freestyle-compatible namespace."""

    vpc_id: str
    name: str | None = None
    cidr: str | None = None


@dataclass(frozen=True, slots=True)
class CreatedVpc:
    """Result of Vpc.create containing the VPC handle."""

    vpc_id: str
    vpc: FreestyleVpc


@dataclass(frozen=True, slots=True)
class VpcsResult:
    """Result of Vpc.list containing VPC entries."""

    vpcs: list[FreestyleVpc]


def _not_found(error: APIError) -> bool:
    return error.code == "not_found" or error.status == 404


def _iso(value: object) -> str | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    return datetime.fromtimestamp(float(value), UTC).isoformat().replace("+00:00", "Z")


def _vm_state(info: SandboxInfo) -> str:
    state = info.observed_state or info.status
    if state == "running":
        return "running"
    if state in {"starting", "booting", "creating", "pending"}:
        return "starting"
    if state == "suspending":
        return "suspending"
    if state in {"suspended", "paused"}:
        return "suspended"
    if state in {"lost", "building"}:
        return state
    return "stopped"


def _sticky(persistence: Mapping[str, object] | None) -> dict[str, object] | None:
    if persistence is None:
        return None
    result = dict(persistence)
    if result.get("type") == "sticky":
        raw = result.get("priority", 5)
        if not isinstance(raw, int | float) or isinstance(raw, bool):
            raise TypeError("sticky persistence priority must be numeric")
        result["priority"] = min(10, max(0, int(raw)))
    return result


class VmSnapshots:
    """Snapshot collection operations."""

    def __init__(self, freestyle_client: Freestyle) -> None:
        self._freestyle = freestyle_client

    def list(self, **_filters: bool) -> SnapshotsResult:
        """List snapshots; vmon retains only ready snapshots, so filters are no-ops."""
        snapshots = [
            VmSnapshot(snapshot_id=name, state="ready")
            for name in self._freestyle.client.snapshots.list()
        ]
        return SnapshotsResult(snapshots)

    def get(self, snapshot_id: str) -> VmSnapshot:
        """Fetch one snapshot by identifier."""
        for snapshot in self.list().snapshots:
            if snapshot.snapshot_id == snapshot_id:
                return snapshot
        raise APIError(
            f"snapshot {snapshot_id} does not exist",
            code="not_found",
            status=404,
        )


class Vms:
    """VM collection operations."""

    def __init__(self, freestyle_client: Freestyle) -> None:
        self._freestyle = freestyle_client
        self.snapshots = VmSnapshots(freestyle_client)

    def create(
        self,
        *,
        snapshot_id: str | None = None,
        name: str | None = None,
        idle_timeout_seconds: int | None | _Unset = _UNSET,
        persistence: Mapping[str, object] | None = None,
        activity_threshold_bytes: int | None = None,
        nics: Sequence[Mapping[str, object]] | None = None,
        image: str | None = None,
        template: str | None = None,
        cpu: int | None = None,
        memory: float | None = None,
        storage: float | None = None,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> CreatedVm:
        """Create a VM, either fresh or from a snapshot."""
        if nics is not None and len(nics) > 1:
            raise TypeError("vmon VMs have a single NIC")
        nic_wire: list[dict[str, object]] | None = None
        if nics is not None:
            nic_wire = []
            for nic in nics:
                if nic.get("mode") != "routed":
                    raise TypeError('vmon VPC NICs require mode: "routed"')
                nic_wire.append(
                    {
                        "vpc": nic["vpc"],
                        "ipv4": nic["ipv4"],
                        "default": nic.get("default", True),
                    }
                )
        common: dict[str, Any] = {
            "name": name,
            "env": env,
            "workdir": workdir,
            "tags": tags,
            "timeout_secs": 0,
            "block_network": False,
            "activity_threshold_bytes": activity_threshold_bytes,
            "persistence": _sticky(persistence),
        }
        if not isinstance(idle_timeout_seconds, _Unset):
            common["idle_timeout_secs"] = (
                0 if idle_timeout_seconds is None else idle_timeout_seconds
            )
        client = self._freestyle.client
        if snapshot_id is not None:
            if cpu is not None or memory is not None or storage is not None:
                raise TypeError(
                    "cpu/memory/storage are fixed by the snapshot and cannot be changed"
                )
            sandbox = client.snapshots.restore(snapshot_id, **common)
        else:
            sandbox = client.sandboxes.create(
                image=image,
                template=template,
                cpus=cpu if cpu is not None else 1,
                memory=int(memory * 1024) if memory is not None else 512,
                timeout=None,
                disk_mb=int(storage * 1024) if storage is not None else 1024,
                nics=nic_wire,
                **common,
            )
        vm = Vm(client, sandbox)
        return CreatedVm(vm=vm, vm_id=vm.vm_id, domains=[])

    def list(self) -> VmsResult:
        """List every reachable VM with lifecycle metadata and state counts."""
        rows = []
        for sandbox in self._freestyle.client.sandboxes.list():
            info = sandbox.info
            rows.append(
                VmListEntry(
                    id=sandbox.id,
                    state=_vm_state(info),
                    created_at=_iso(info.created_at),
                    last_network_activity=_iso(
                        info.raw.get("last_network_active", info.last_active)
                    ),
                )
            )
        return VmsResult(
            vms=rows,
            total_count=len(rows),
            running_count=sum(row.state == "running" for row in rows),
            starting_count=sum(row.state == "starting" for row in rows),
            suspended_count=sum(row.state in {"suspended", "suspending"} for row in rows),
            stopped_count=sum(
                row.state not in {"running", "starting", "suspended", "suspending"} for row in rows
            ),
        )

    def get(self, vm_id: str) -> GotVm:
        """Reconnect to a VM by identifier."""
        client = self._freestyle.client
        return GotVm(Vm(client, client.sandboxes.get(vm_id)))

    def ref(self, vm_id: str) -> Vm:
        """Create an unfetched VM reference."""
        client = self._freestyle.client
        return Vm(client, client.sandboxes.ref(vm_id))

    def delete(self, vm_id: str) -> None:
        """Permanently remove a VM; already-deleted VMs are ignored."""
        _delete_sandbox(self._freestyle.client.sandboxes.ref(vm_id))


class Vm:
    """One Freestyle-shaped VM handle bound to a vmon sandbox."""

    def __init__(self, client: Client, sandbox: Sandbox) -> None:
        self._client = client
        self.sandbox = sandbox
        self.fs = VmFs(sandbox)
        self.pty = VmPty(sandbox)

    @property
    def vm_id(self) -> str:
        """Stable VM identifier."""
        return self.sandbox.id

    def start(self, *, idle_timeout_seconds: int | None | _Unset = _UNSET) -> SandboxInfo:
        """Wake or fresh-boot this VM; ``None`` disables its network-idle policy."""
        if not isinstance(idle_timeout_seconds, _Unset):
            self.sandbox.set_idle_timeout(
                0 if idle_timeout_seconds is None else idle_timeout_seconds
            )
        return self.sandbox.resume()

    def stop(self) -> None:
        """Stop execution while retaining the VM disk and identity for a later fresh boot."""
        self.sandbox.stop()

    def snapshot(self, *, name: str | None = None) -> SnapshotResult:
        """Create a snapshot of this VM."""
        snapshot_id = self.sandbox.snapshot(name)
        return SnapshotResult(snapshot_id=snapshot_id, source_vm_id=self.vm_id)

    def fork(self, *, count: int) -> ForkedVms:
        """Fork the VM into count copies of its current state."""
        snapshot = self.sandbox.snapshot()
        clones = self._client.snapshots.fork(snapshot, count=count)
        return ForkedVms([VmFork(vm=Vm(self._client, clone), vm_id=clone.id) for clone in clones])

    def delete(self) -> None:
        """Permanently remove this VM."""
        _delete_sandbox(self.sandbox)

    def exec(
        self,
        command: str,
        *,
        terminal: str | None = None,
        timeout_ms: int | None = None,
    ) -> VmExecResult:
        """Run a command through sh -c, or as a sibling in a persistent terminal context."""
        timeout = None if timeout_ms is None else timeout_ms / 1000
        if terminal is not None:
            result = self.sandbox.pty_exec(terminal, command, timeout)
        else:
            result = self.sandbox.run(["sh", "-c", command], timeout=timeout)
        return VmExecResult(
            stdout=result.stdout.decode("utf-8", "replace"),
            stderr=result.stderr.decode("utf-8", "replace"),
            status_code=result.returncode,
        )

    def resize(
        self,
        *,
        cpu: int | None = None,
        memory: float | None = None,
        storage: float | None = None,
    ) -> SandboxInfo:
        """Resize VM vCPU count, memory, or disk storage."""
        if cpu is not None and (isinstance(cpu, bool) or cpu <= 0 or cpu & (cpu - 1)):
            raise TypeError("cpu must be a power of two")
        if memory is not None and not _power_of_two(memory):
            raise TypeError("memory must be a power of two")
        return self.sandbox.resize(
            cpus=cpu,
            memory_mib=None if memory is None else int(memory * 1024),
            disk_mb=None if storage is None else int(storage * 1024),
        )


class VmFs:
    """Freestyle-shaped guest filesystem bound to one VM."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def read_file(self, path: str) -> bytes:
        """Read raw guest file bytes."""
        return self._sandbox.files.read_bytes(path)

    def write_file(self, path: str, content: bytes | bytearray | memoryview | str) -> None:
        """Write raw guest file content."""
        data = content.encode() if isinstance(content, str) else content
        self._sandbox.files.write_bytes(path, data)

    def read_text_file(self, path: str) -> str:
        """Read a UTF-8 guest file."""
        return self._sandbox.files.read_text(path)

    def write_text_file(self, path: str, content: str) -> None:
        """Write UTF-8 guest file content."""
        self._sandbox.files.write_text(path, content)

    def read_dir(self, path: str) -> list[FsEntry]:
        """List guest directory entries; kind is file, dir, symlink, or other."""
        return [
            FsEntry(name=entry.name, kind=entry.type or "other")
            for entry in self._sandbox.files.list(path)
        ]

    def mkdir(self, path: str) -> None:
        """Create a guest directory and its parents."""
        self._sandbox.files.mkdir(path)

    def remove(self, path: str, *, recursive: bool = False) -> None:
        """Delete a guest path; pass recursive for non-empty trees."""
        self._sandbox.files.delete(path, recursive)

    def exists(self, path: str) -> bool:
        """Check whether a guest path exists."""
        try:
            self._sandbox.files.stat(path)
        except APIError as error:
            if _not_found(error):
                return False
            raise
        return True

    def stat(self, path: str) -> FsStat:
        """Stat a guest path; the vmon agent reports no owner/group."""
        info = self._sandbox.files.stat(path)
        return FsStat(
            size=info.size,
            is_file=info.type == "file",
            is_directory=info.type == "dir",
            is_symlink=info.type == "symlink",
            permissions=f"{info.mode & 0o7777:03o}",
            modified=_iso(info.mtime) or "",
        )


class PtySession:
    """One attachment to a guest-owned persistent terminal session."""

    def __init__(self, sandbox: Sandbox, stream: PtyStream) -> None:
        self._sandbox = sandbox
        self._stream = stream
        self.session_id = stream.session_id

    @property
    def ready_state(self) -> int:
        """Return the WebSocket-compatible readyState indicator (1 open, 3 closed)."""
        return 3 if self._stream.returncode is not None else 1

    def read(self, timeout: float | None = None) -> bytes:
        """Read available terminal output bytes."""
        try:
            return self._stream.stdout.read_chunk(timeout) or b""
        except TimeoutError:
            return b""

    def iter_output(self) -> Iterator[bytes]:
        """Yield chunks of terminal output until the session closes."""
        yield from self._stream.stdout

    def wait(self, timeout: float | None = None) -> int:
        """Wait for process completion and return its exit code."""
        return self._stream.wait(timeout).code

    def write(self, data: bytes | bytearray | memoryview | str) -> Self:
        """Write input data to the terminal standard input."""
        self._stream.write(data)
        return self

    def resize(self, *, cols: int, rows: int) -> Self:
        """Resize the terminal dimensions."""
        self._stream.resize(rows, cols)
        return self

    def signal(self, sig: Literal["SIGINT", "SIGKILL"]) -> None:
        """Send a signal to the running terminal session."""
        if sig == "SIGINT":
            self.write(b"\x03")
        else:
            self._sandbox.pty_close(self.session_id)

    def detach(self) -> None:
        """Disconnect this client stream while leaving the guest session running."""
        self._stream.detach()

    def info(self) -> PtySessionInfo:
        """Return point-in-time session metadata."""
        return _pty_info(self._stream.session)


class VmPty:
    """Interactive PTY operations bound to one VM."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    def open(
        self,
        *,
        cols: int | None = None,
        rows: int | None = None,
        exec: str | None = None,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
        session_id: str | None = None,
    ) -> PtySession:
        """Open a new interactive PTY session on the guest."""
        return PtySession(
            self._sandbox,
            self._sandbox.pty_open(
                session_id=session_id,
                cols=cols,
                rows=rows,
                exec=exec,
                env=env,
                workdir=workdir,
            ),
        )

    def attach(
        self,
        session_id: str,
        *,
        cols: int | None = None,
        rows: int | None = None,
    ) -> PtySession:
        """Attach to an existing persistent PTY session by ID."""
        return PtySession(
            self._sandbox,
            self._sandbox.pty_attach(session_id, cols=cols, rows=rows),
        )

    def list(self) -> PtySessionsResult:
        """List active PTY sessions on the VM."""
        return PtySessionsResult([_pty_info(session) for session in self._sandbox.pty_list()])

    def close(self, session_id: str) -> ClosedPty:
        """Close a PTY session by ID."""
        response = self._sandbox.pty_close(session_id)
        exit_code = response.exit_code if response.HasField("exit_code") else None
        return ClosedPty(session_id=response.session_id, exit_code=exit_code)


class Vpc:
    """Freestyle-shaped VPC operations backed by vmon's routed VPC service."""

    def __init__(self, freestyle_client: Freestyle) -> None:
        self._freestyle = freestyle_client

    def create(self, *, cidr: str | None = None, name: str | None = None) -> CreatedVpc:
        """Create a routed VPC."""
        created = self._freestyle.client.vpcs.create(name=name, cidr=cidr)
        facade = FreestyleVpc(vpc_id=created.id)
        return CreatedVpc(vpc_id=created.id, vpc=facade)

    def list(self) -> VpcsResult:
        """List routed VPCs (vmon extension)."""
        return VpcsResult(
            [
                FreestyleVpc(vpc_id=vpc.id, name=vpc.name, cidr=vpc.cidr)
                for vpc in self._freestyle.client.vpcs.list()
            ]
        )

    def delete(self, vpc_id: str) -> None:
        """Delete an unattached routed VPC (vmon extension)."""
        self._freestyle.client.vpcs.delete(vpc_id)


class Freestyle:
    """Freestyle-shaped root client backed by a lazily connected vmon client."""

    def __init__(
        self,
        api_key: str | None = None,
        access_token: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self._client: Client | None = None
        self._base_url = base_url
        self._token = access_token if access_token is not None else api_key
        self.vms = Vms(self)
        self.vpc = Vpc(self)

    @classmethod
    def from_client(cls, client: Client) -> Freestyle:
        """Create a Freestyle facade instance wrapping an existing vmon client."""
        instance = cls()
        instance._client = client
        return instance

    @property
    def client(self) -> Client:
        """Underlying vmon client, connected on first access."""
        if self._client is None:
            self._client = connect(self._base_url, token=self._token)
        return self._client

    def close(self) -> None:
        """Close the underlying vmon client if one was ever connected."""
        if self._client is not None:
            self._client.close()


def _power_of_two(value: float) -> bool:
    if isinstance(value, bool) or value <= 0:
        return False
    exponent = value.bit_length() - 1 if isinstance(value, int) else None
    if exponent is not None:
        return value == 2**exponent
    import math

    return math.log2(value).is_integer()


def _delete_sandbox(sandbox: Sandbox) -> None:
    try:
        sandbox.remove()
    except APIError as error:
        if not _not_found(error):
            raise


def _pty_info(session: Any) -> PtySessionInfo:
    exit_code = session.exit_code if session.HasField("exit_code") else None
    exec_value = session.exec if session.HasField("exec") else None
    return PtySessionInfo(
        session_id=session.session_id,
        running=session.running,
        exit_code=exit_code,
        cols=session.cols,
        rows=session.rows,
        exec=exec_value,
        created_at_ms=session.created_at_unix_millis,
        attached_count=session.attached_count,
        suspended=session.suspended,
    )


freestyle = Freestyle()


__all__ = [
    "ClosedPty",
    "CreatedVm",
    "CreatedVpc",
    "ForkedVms",
    "Freestyle",
    "FreestyleVpc",
    "FsEntry",
    "FsStat",
    "GotVm",
    "PtySession",
    "PtySessionInfo",
    "PtySessionsResult",
    "SnapshotResult",
    "SnapshotsResult",
    "Vm",
    "VmExecResult",
    "VmFork",
    "VmFs",
    "VmListEntry",
    "VmPty",
    "VmSnapshot",
    "VmSnapshots",
    "Vms",
    "Vpc",
    "VpcsResult",
    "freestyle",
]
