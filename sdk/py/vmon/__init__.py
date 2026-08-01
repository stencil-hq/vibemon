"""Typed Python SDK for vmon sandboxes and durable functions."""

__version__ = "0.2.0"

from ._endpoint import WebSocketConnection
from .app import App, AppBinding, AppManifest, AppRevision, Cron, Period, Schedule
from .call_group import CallGroup
from .client import Client, Pool, connect
from .cls import (
    ActorCheckpoint,
    LifecycleMetadata,
    RemoteClass,
    RemoteObject,
    cls,
    enter,
    exit,
    method,
    on_restore,
    service,
)
from .context import Context, ContextStore
from .decorators import CurrentCall, batched, concurrent, current_call
from .driver import Driver, MeshDriver, parse_dsn
from .errors import ActorLostError, APIError, ProtocolError, RemoteFunctionError, TransportError
from .freestyle import Freestyle, freestyle
from .image import Image, ImageError, ImageSource, ImageStep
from .models import (
    EventRecord,
    ExecExit,
    FileInfo,
    Health,
    MeshNode,
    MeshStatus,
    PoolStats,
    RecoveryPoint,
    SandboxInfo,
    SandboxMetrics,
    SandboxNetworkPolicy,
    ServerInfo,
    TunnelSet,
    TunnelTarget,
)
from .options import (
    BatchingPolicy,
    ConcurrencyPolicy,
    CpuArchitecture,
    FunctionOptions,
    FunctionVolumeMount,
    HighAvailabilityPolicy,
    NetworkPolicy,
    OptionsError,
    ResourcePolicy,
    RetryPolicy,
    SerializerPolicy,
    TimeoutPolicy,
    WorkerPolicy,
)
from .package import (
    ManifestFile,
    PackageArtifact,
    PackageError,
    PackageManifest,
    SerializedCallable,
    build_package,
    inspect_manifest,
    package_callable,
)
from .process import ConsoleStream, EventStream, LogStream, Process
from .remote import (
    AsyncGeneratorRemoteFunction,
    AsyncRemoteFunction,
    BatchCall,
    FunctionCall,
    GeneratorRemoteFunction,
    RemoteFunction,
    SyncRemoteFunction,
    function,
    is_remote,
)
from .sandbox import ExecResult, Files, Sandbox
from .secret import Secret
from .values import (
    ABIMismatchError,
    ArtifactPayload,
    CodecMismatchError,
    EnvelopeIntegrityError,
    ValueCodec,
    ValueEnvelope,
    decode_value,
    encode_value,
)
from .volume import S3Mount, Volume

# These functions predate the public export contract; keep their definitive
# user-facing documentation at the package boundary until their modules are
# split into private codec/building primitives.
build_package.__doc__ = "Build a deterministic Python package artifact from a source root."
inspect_manifest.__doc__ = "Read and validate the manifest in a Python package artifact."
package_callable.__doc__ = "Package a Python callable as importable or trusted serialized code."
encode_value.__doc__ = "Encode a Python value into a checksummed portable value envelope."
decode_value.__doc__ = "Decode and verify a value envelope, requiring trust for Python code."
function.__doc__ = "Decorate a callable as a typed zero-deploy remote function."
is_remote.__doc__ = "Return whether execution is currently inside a vmon worker."

__all__ = [
    "ABIMismatchError",
    "APIError",
    "ActorCheckpoint",
    "ActorLostError",
    "App",
    "AppBinding",
    "AppManifest",
    "AppRevision",
    "ArtifactPayload",
    "AsyncGeneratorRemoteFunction",
    "AsyncRemoteFunction",
    "BatchCall",
    "BatchingPolicy",
    "CallGroup",
    "Client",
    "CodecMismatchError",
    "ConcurrencyPolicy",
    "ConsoleStream",
    "Context",
    "ContextStore",
    "CpuArchitecture",
    "Cron",
    "CurrentCall",
    "Driver",
    "EnvelopeIntegrityError",
    "EventRecord",
    "ExecExit",
    "ExecResult",
    "Freestyle",
    "FileInfo",
    "Files",
    "FunctionCall",
    "FunctionOptions",
    "FunctionVolumeMount",
    "GeneratorRemoteFunction",
    "HighAvailabilityPolicy",
    "Health",
    "Image",
    "ImageError",
    "ImageSource",
    "ImageStep",
    "EventStream",
    "LifecycleMetadata",
    "LogStream",
    "ManifestFile",
    "MeshDriver",
    "MeshNode",
    "MeshStatus",
    "NetworkPolicy",
    "OptionsError",
    "PackageArtifact",
    "PackageError",
    "PackageManifest",
    "Period",
    "Pool",
    "PoolStats",
    "Process",
    "ProtocolError",
    "RemoteClass",
    "RemoteFunction",
    "RemoteFunctionError",
    "RemoteObject",
    "ResourcePolicy",
    "RecoveryPoint",
    "RetryPolicy",
    "Sandbox",
    "SandboxInfo",
    "SandboxMetrics",
    "SandboxNetworkPolicy",
    "Schedule",
    "S3Mount",
    "Secret",
    "SerializedCallable",
    "SerializerPolicy",
    "ServerInfo",
    "TimeoutPolicy",
    "SyncRemoteFunction",
    "TransportError",
    "TunnelSet",
    "TunnelTarget",
    "ValueCodec",
    "ValueEnvelope",
    "Volume",
    "WebSocketConnection",
    "WorkerPolicy",
    "batched",
    "build_package",
    "cls",
    "concurrent",
    "connect",
    "current_call",
    "decode_value",
    "encode_value",
    "freestyle",
    "enter",
    "exit",
    "function",
    "inspect_manifest",
    "is_remote",
    "method",
    "on_restore",
    "package_callable",
    "parse_dsn",
    "service",
]
