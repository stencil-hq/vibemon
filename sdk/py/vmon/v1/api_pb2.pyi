from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Stream(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STREAM_UNSPECIFIED: _ClassVar[Stream]
    STREAM_STDOUT: _ClassVar[Stream]
    STREAM_STDERR: _ClassVar[Stream]
    STREAM_CONSOLE: _ClassVar[Stream]

class DigestAlgorithm(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DIGEST_ALGORITHM_UNSPECIFIED: _ClassVar[DigestAlgorithm]
    DIGEST_ALGORITHM_SHA256: _ClassVar[DigestAlgorithm]

class ValueSerializer(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VALUE_SERIALIZER_UNSPECIFIED: _ClassVar[ValueSerializer]
    VALUE_SERIALIZER_JSON: _ClassVar[ValueSerializer]
    VALUE_SERIALIZER_CBOR: _ClassVar[ValueSerializer]
    VALUE_SERIALIZER_CLOUDPICKLE: _ClassVar[ValueSerializer]

class ValueCompression(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VALUE_COMPRESSION_NONE: _ClassVar[ValueCompression]
    VALUE_COMPRESSION_GZIP: _ClassVar[ValueCompression]
    VALUE_COMPRESSION_ZSTD: _ClassVar[ValueCompression]

class FunctionLifecycle(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_LIFECYCLE_UNSPECIFIED: _ClassVar[FunctionLifecycle]
    FUNCTION_LIFECYCLE_STATELESS: _ClassVar[FunctionLifecycle]
    FUNCTION_LIFECYCLE_INSTANCE: _ClassVar[FunctionLifecycle]
    FUNCTION_LIFECYCLE_ACTOR: _ClassVar[FunctionLifecycle]

class PackageMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PACKAGE_MODE_UNSPECIFIED: _ClassVar[PackageMode]
    PACKAGE_MODE_MODULE: _ClassVar[PackageMode]
    PACKAGE_MODE_PACKAGE: _ClassVar[PackageMode]
    PACKAGE_MODE_TRUSTED_SERIALIZED: _ClassVar[PackageMode]

class CpuArchitecture(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CPU_ARCHITECTURE_UNSPECIFIED: _ClassVar[CpuArchitecture]
    CPU_ARCHITECTURE_AMD64: _ClassVar[CpuArchitecture]
    CPU_ARCHITECTURE_ARM64: _ClassVar[CpuArchitecture]

class HighAvailabilityPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HIGH_AVAILABILITY_POLICY_UNSPECIFIED: _ClassVar[HighAvailabilityPolicy]
    HIGH_AVAILABILITY_POLICY_NONE: _ClassVar[HighAvailabilityPolicy]
    HIGH_AVAILABILITY_POLICY_HOST: _ClassVar[HighAvailabilityPolicy]
    HIGH_AVAILABILITY_POLICY_ZONE: _ClassVar[HighAvailabilityPolicy]

class CallType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CALL_TYPE_UNSPECIFIED: _ClassVar[CallType]
    CALL_TYPE_UNARY: _ClassVar[CallType]
    CALL_TYPE_GENERATOR: _ClassVar[CallType]
    CALL_TYPE_BATCH: _ClassVar[CallType]
    CALL_TYPE_ACTOR: _ClassVar[CallType]

class CallStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CALL_STATUS_UNSPECIFIED: _ClassVar[CallStatus]
    CALL_STATUS_PENDING: _ClassVar[CallStatus]
    CALL_STATUS_QUEUED: _ClassVar[CallStatus]
    CALL_STATUS_RUNNING: _ClassVar[CallStatus]
    CALL_STATUS_SUCCEEDED: _ClassVar[CallStatus]
    CALL_STATUS_FAILED: _ClassVar[CallStatus]
    CALL_STATUS_CANCELLING: _ClassVar[CallStatus]
    CALL_STATUS_CANCELLED: _ClassVar[CallStatus]

class CallEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CALL_EVENT_TYPE_UNSPECIFIED: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_STATUS: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_LOG: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_YIELD: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_RESULT: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_ATTEMPT: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_ERROR: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_INPUT_CLOSED: _ClassVar[CallEventType]
    CALL_EVENT_TYPE_CANCEL_REQUESTED: _ClassVar[CallEventType]

class LogStream(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOG_STREAM_UNSPECIFIED: _ClassVar[LogStream]
    LOG_STREAM_STDOUT: _ClassVar[LogStream]
    LOG_STREAM_STDERR: _ClassVar[LogStream]
    LOG_STREAM_STRUCTURED: _ClassVar[LogStream]

class AttemptStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATTEMPT_STATUS_UNSPECIFIED: _ClassVar[AttemptStatus]
    ATTEMPT_STATUS_STARTED: _ClassVar[AttemptStatus]
    ATTEMPT_STATUS_SUCCEEDED: _ClassVar[AttemptStatus]
    ATTEMPT_STATUS_FAILED: _ClassVar[AttemptStatus]
    ATTEMPT_STATUS_CANCELLED: _ClassVar[AttemptStatus]

class StartupKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STARTUP_KIND_UNSPECIFIED: _ClassVar[StartupKind]
    STARTUP_KIND_COLD: _ClassVar[StartupKind]
    STARTUP_KIND_WARM: _ClassVar[StartupKind]
    STARTUP_KIND_SNAPSHOT: _ClassVar[StartupKind]

class ActorStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACTOR_STATUS_UNSPECIFIED: _ClassVar[ActorStatus]
    ACTOR_STATUS_CREATING: _ClassVar[ActorStatus]
    ACTOR_STATUS_READY: _ClassVar[ActorStatus]
    ACTOR_STATUS_BUSY: _ClassVar[ActorStatus]
    ACTOR_STATUS_STOPPED: _ClassVar[ActorStatus]
    ACTOR_STATUS_FAILED: _ClassVar[ActorStatus]
    ACTOR_STATUS_DELETED: _ClassVar[ActorStatus]

class ScheduleStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCHEDULE_STATUS_UNSPECIFIED: _ClassVar[ScheduleStatus]
    SCHEDULE_STATUS_ACTIVE: _ClassVar[ScheduleStatus]
    SCHEDULE_STATUS_PAUSED: _ClassVar[ScheduleStatus]

class FunctionRevisionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUNCTION_REVISION_STATUS_UNSPECIFIED: _ClassVar[FunctionRevisionStatus]
    FUNCTION_REVISION_STATUS_READY: _ClassVar[FunctionRevisionStatus]
    FUNCTION_REVISION_STATUS_UNAVAILABLE: _ClassVar[FunctionRevisionStatus]

class AttemptFailureKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ATTEMPT_FAILURE_KIND_UNSPECIFIED: _ClassVar[AttemptFailureKind]
    ATTEMPT_FAILURE_KIND_USER: _ClassVar[AttemptFailureKind]
    ATTEMPT_FAILURE_KIND_INFRASTRUCTURE: _ClassVar[AttemptFailureKind]
    ATTEMPT_FAILURE_KIND_CANCELLED: _ClassVar[AttemptFailureKind]
STREAM_UNSPECIFIED: Stream
STREAM_STDOUT: Stream
STREAM_STDERR: Stream
STREAM_CONSOLE: Stream
DIGEST_ALGORITHM_UNSPECIFIED: DigestAlgorithm
DIGEST_ALGORITHM_SHA256: DigestAlgorithm
VALUE_SERIALIZER_UNSPECIFIED: ValueSerializer
VALUE_SERIALIZER_JSON: ValueSerializer
VALUE_SERIALIZER_CBOR: ValueSerializer
VALUE_SERIALIZER_CLOUDPICKLE: ValueSerializer
VALUE_COMPRESSION_NONE: ValueCompression
VALUE_COMPRESSION_GZIP: ValueCompression
VALUE_COMPRESSION_ZSTD: ValueCompression
FUNCTION_LIFECYCLE_UNSPECIFIED: FunctionLifecycle
FUNCTION_LIFECYCLE_STATELESS: FunctionLifecycle
FUNCTION_LIFECYCLE_INSTANCE: FunctionLifecycle
FUNCTION_LIFECYCLE_ACTOR: FunctionLifecycle
PACKAGE_MODE_UNSPECIFIED: PackageMode
PACKAGE_MODE_MODULE: PackageMode
PACKAGE_MODE_PACKAGE: PackageMode
PACKAGE_MODE_TRUSTED_SERIALIZED: PackageMode
CPU_ARCHITECTURE_UNSPECIFIED: CpuArchitecture
CPU_ARCHITECTURE_AMD64: CpuArchitecture
CPU_ARCHITECTURE_ARM64: CpuArchitecture
HIGH_AVAILABILITY_POLICY_UNSPECIFIED: HighAvailabilityPolicy
HIGH_AVAILABILITY_POLICY_NONE: HighAvailabilityPolicy
HIGH_AVAILABILITY_POLICY_HOST: HighAvailabilityPolicy
HIGH_AVAILABILITY_POLICY_ZONE: HighAvailabilityPolicy
CALL_TYPE_UNSPECIFIED: CallType
CALL_TYPE_UNARY: CallType
CALL_TYPE_GENERATOR: CallType
CALL_TYPE_BATCH: CallType
CALL_TYPE_ACTOR: CallType
CALL_STATUS_UNSPECIFIED: CallStatus
CALL_STATUS_PENDING: CallStatus
CALL_STATUS_QUEUED: CallStatus
CALL_STATUS_RUNNING: CallStatus
CALL_STATUS_SUCCEEDED: CallStatus
CALL_STATUS_FAILED: CallStatus
CALL_STATUS_CANCELLING: CallStatus
CALL_STATUS_CANCELLED: CallStatus
CALL_EVENT_TYPE_UNSPECIFIED: CallEventType
CALL_EVENT_TYPE_STATUS: CallEventType
CALL_EVENT_TYPE_LOG: CallEventType
CALL_EVENT_TYPE_YIELD: CallEventType
CALL_EVENT_TYPE_RESULT: CallEventType
CALL_EVENT_TYPE_ATTEMPT: CallEventType
CALL_EVENT_TYPE_ERROR: CallEventType
CALL_EVENT_TYPE_INPUT_CLOSED: CallEventType
CALL_EVENT_TYPE_CANCEL_REQUESTED: CallEventType
LOG_STREAM_UNSPECIFIED: LogStream
LOG_STREAM_STDOUT: LogStream
LOG_STREAM_STDERR: LogStream
LOG_STREAM_STRUCTURED: LogStream
ATTEMPT_STATUS_UNSPECIFIED: AttemptStatus
ATTEMPT_STATUS_STARTED: AttemptStatus
ATTEMPT_STATUS_SUCCEEDED: AttemptStatus
ATTEMPT_STATUS_FAILED: AttemptStatus
ATTEMPT_STATUS_CANCELLED: AttemptStatus
STARTUP_KIND_UNSPECIFIED: StartupKind
STARTUP_KIND_COLD: StartupKind
STARTUP_KIND_WARM: StartupKind
STARTUP_KIND_SNAPSHOT: StartupKind
ACTOR_STATUS_UNSPECIFIED: ActorStatus
ACTOR_STATUS_CREATING: ActorStatus
ACTOR_STATUS_READY: ActorStatus
ACTOR_STATUS_BUSY: ActorStatus
ACTOR_STATUS_STOPPED: ActorStatus
ACTOR_STATUS_FAILED: ActorStatus
ACTOR_STATUS_DELETED: ActorStatus
SCHEDULE_STATUS_UNSPECIFIED: ScheduleStatus
SCHEDULE_STATUS_ACTIVE: ScheduleStatus
SCHEDULE_STATUS_PAUSED: ScheduleStatus
FUNCTION_REVISION_STATUS_UNSPECIFIED: FunctionRevisionStatus
FUNCTION_REVISION_STATUS_READY: FunctionRevisionStatus
FUNCTION_REVISION_STATUS_UNAVAILABLE: FunctionRevisionStatus
ATTEMPT_FAILURE_KIND_UNSPECIFIED: AttemptFailureKind
ATTEMPT_FAILURE_KIND_USER: AttemptFailureKind
ATTEMPT_FAILURE_KIND_INFRASTRUCTURE: AttemptFailureKind
ATTEMPT_FAILURE_KIND_CANCELLED: AttemptFailureKind

class JsonView(_message.Message):
    __slots__ = ("json",)
    JSON_FIELD_NUMBER: _ClassVar[int]
    json: str
    def __init__(self, json: _Optional[str] = ...) -> None: ...

class Ok(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SandboxRef(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class CreateSandboxRequest(_message.Message):
    __slots__ = ("spec_json", "no_wait")
    SPEC_JSON_FIELD_NUMBER: _ClassVar[int]
    NO_WAIT_FIELD_NUMBER: _ClassVar[int]
    spec_json: str
    no_wait: bool
    def __init__(self, spec_json: _Optional[str] = ..., no_wait: _Optional[bool] = ...) -> None: ...

class BatchCreateRequest(_message.Message):
    __slots__ = ("seq", "create")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    seq: int
    create: CreateSandboxRequest
    def __init__(self, seq: _Optional[int] = ..., create: _Optional[_Union[CreateSandboxRequest, _Mapping]] = ...) -> None: ...

class BatchCreateError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class BatchCreateResponse(_message.Message):
    __slots__ = ("seq", "json", "error")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    seq: int
    json: str
    error: BatchCreateError
    def __init__(self, seq: _Optional[int] = ..., json: _Optional[str] = ..., error: _Optional[_Union[BatchCreateError, _Mapping]] = ...) -> None: ...

class WatchSandboxRequest(_message.Message):
    __slots__ = ("id", "until_ready")
    ID_FIELD_NUMBER: _ClassVar[int]
    UNTIL_READY_FIELD_NUMBER: _ClassVar[int]
    id: str
    until_ready: bool
    def __init__(self, id: _Optional[str] = ..., until_ready: _Optional[bool] = ...) -> None: ...

class ListSandboxesRequest(_message.Message):
    __slots__ = ("tags",)
    TAGS_FIELD_NUMBER: _ClassVar[int]
    tags: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tags: _Optional[_Iterable[str]] = ...) -> None: ...

class ListSandboxesResponse(_message.Message):
    __slots__ = ("sandboxes_json",)
    SANDBOXES_JSON_FIELD_NUMBER: _ClassVar[int]
    sandboxes_json: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, sandboxes_json: _Optional[_Iterable[str]] = ...) -> None: ...

class StopSandboxRequest(_message.Message):
    __slots__ = ("id", "returncode")
    ID_FIELD_NUMBER: _ClassVar[int]
    RETURNCODE_FIELD_NUMBER: _ClassVar[int]
    id: str
    returncode: int
    def __init__(self, id: _Optional[str] = ..., returncode: _Optional[int] = ...) -> None: ...

class RollbackSandboxRequest(_message.Message):
    __slots__ = ("id", "recovery_point")
    ID_FIELD_NUMBER: _ClassVar[int]
    RECOVERY_POINT_FIELD_NUMBER: _ClassVar[int]
    id: str
    recovery_point: str
    def __init__(self, id: _Optional[str] = ..., recovery_point: _Optional[str] = ...) -> None: ...

class ExtendSandboxRequest(_message.Message):
    __slots__ = ("id", "secs")
    ID_FIELD_NUMBER: _ClassVar[int]
    SECS_FIELD_NUMBER: _ClassVar[int]
    id: str
    secs: int
    def __init__(self, id: _Optional[str] = ..., secs: _Optional[int] = ...) -> None: ...

class SetIdleTimeoutRequest(_message.Message):
    __slots__ = ("id", "idle_timeout_secs")
    ID_FIELD_NUMBER: _ClassVar[int]
    IDLE_TIMEOUT_SECS_FIELD_NUMBER: _ClassVar[int]
    id: str
    idle_timeout_secs: float
    def __init__(self, id: _Optional[str] = ..., idle_timeout_secs: _Optional[float] = ...) -> None: ...

class LogsRequest(_message.Message):
    __slots__ = ("id", "follow", "tail")
    ID_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    id: str
    follow: bool
    tail: int
    def __init__(self, id: _Optional[str] = ..., follow: _Optional[bool] = ..., tail: _Optional[int] = ...) -> None: ...

class LogChunk(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class ExecStart(_message.Message):
    __slots__ = ("cmd", "workdir", "env", "timeout", "tty", "sandbox_id")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CMD_FIELD_NUMBER: _ClassVar[int]
    WORKDIR_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    TTY_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    cmd: _containers.RepeatedScalarFieldContainer[str]
    workdir: str
    env: _containers.ScalarMap[str, str]
    timeout: float
    tty: bool
    sandbox_id: str
    def __init__(self, cmd: _Optional[_Iterable[str]] = ..., workdir: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., timeout: _Optional[float] = ..., tty: _Optional[bool] = ..., sandbox_id: _Optional[str] = ...) -> None: ...

class ExecCaptureRequest(_message.Message):
    __slots__ = ("id", "exec")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_FIELD_NUMBER: _ClassVar[int]
    id: str
    exec: ExecStart
    def __init__(self, id: _Optional[str] = ..., exec: _Optional[_Union[ExecStart, _Mapping]] = ...) -> None: ...

class ExecCaptureResponse(_message.Message):
    __slots__ = ("code", "signal", "stdout", "stderr")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    code: int
    signal: int
    stdout: bytes
    stderr: bytes
    def __init__(self, code: _Optional[int] = ..., signal: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ...) -> None: ...

class ExecInput(_message.Message):
    __slots__ = ("start", "shell_params_json", "stdin", "eof", "resize", "pty_open", "pty_attach")
    START_FIELD_NUMBER: _ClassVar[int]
    SHELL_PARAMS_JSON_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    PTY_OPEN_FIELD_NUMBER: _ClassVar[int]
    PTY_ATTACH_FIELD_NUMBER: _ClassVar[int]
    start: ExecStart
    shell_params_json: str
    stdin: bytes
    eof: Eof
    resize: Resize
    pty_open: PtyOpenStart
    pty_attach: PtyAttachStart
    def __init__(self, start: _Optional[_Union[ExecStart, _Mapping]] = ..., shell_params_json: _Optional[str] = ..., stdin: _Optional[bytes] = ..., eof: _Optional[_Union[Eof, _Mapping]] = ..., resize: _Optional[_Union[Resize, _Mapping]] = ..., pty_open: _Optional[_Union[PtyOpenStart, _Mapping]] = ..., pty_attach: _Optional[_Union[PtyAttachStart, _Mapping]] = ...) -> None: ...

class ExecOutput(_message.Message):
    __slots__ = ("chunk", "exit", "ready", "pty")
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    EXIT_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    PTY_FIELD_NUMBER: _ClassVar[int]
    chunk: Output
    exit: Exit
    ready: Ready
    pty: PtySession
    def __init__(self, chunk: _Optional[_Union[Output, _Mapping]] = ..., exit: _Optional[_Union[Exit, _Mapping]] = ..., ready: _Optional[_Union[Ready, _Mapping]] = ..., pty: _Optional[_Union[PtySession, _Mapping]] = ...) -> None: ...

class PtyOpenStart(_message.Message):
    __slots__ = ("sandbox_id", "session_id", "cols", "rows", "exec", "env", "workdir")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    EXEC_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    WORKDIR_FIELD_NUMBER: _ClassVar[int]
    sandbox_id: str
    session_id: str
    cols: int
    rows: int
    exec: str
    env: _containers.ScalarMap[str, str]
    workdir: str
    def __init__(self, sandbox_id: _Optional[str] = ..., session_id: _Optional[str] = ..., cols: _Optional[int] = ..., rows: _Optional[int] = ..., exec: _Optional[str] = ..., env: _Optional[_Mapping[str, str]] = ..., workdir: _Optional[str] = ...) -> None: ...

class PtyAttachStart(_message.Message):
    __slots__ = ("sandbox_id", "session_id", "cols", "rows")
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    sandbox_id: str
    session_id: str
    cols: int
    rows: int
    def __init__(self, sandbox_id: _Optional[str] = ..., session_id: _Optional[str] = ..., cols: _Optional[int] = ..., rows: _Optional[int] = ...) -> None: ...

class PtySession(_message.Message):
    __slots__ = ("session_id", "running", "exit_code", "cols", "rows", "exec", "created_at_unix_millis", "attached_count", "suspended")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUNNING_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    EXEC_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ATTACHED_COUNT_FIELD_NUMBER: _ClassVar[int]
    SUSPENDED_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    running: bool
    exit_code: int
    cols: int
    rows: int
    exec: str
    created_at_unix_millis: int
    attached_count: int
    suspended: bool
    def __init__(self, session_id: _Optional[str] = ..., running: _Optional[bool] = ..., exit_code: _Optional[int] = ..., cols: _Optional[int] = ..., rows: _Optional[int] = ..., exec: _Optional[str] = ..., created_at_unix_millis: _Optional[int] = ..., attached_count: _Optional[int] = ..., suspended: _Optional[bool] = ...) -> None: ...

class PtySessionList(_message.Message):
    __slots__ = ("sessions",)
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    sessions: _containers.RepeatedCompositeFieldContainer[PtySession]
    def __init__(self, sessions: _Optional[_Iterable[_Union[PtySession, _Mapping]]] = ...) -> None: ...

class PtyCloseRequest(_message.Message):
    __slots__ = ("id", "session_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    session_id: str
    def __init__(self, id: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class PtySessionCloseResponse(_message.Message):
    __slots__ = ("session_id", "exit_code")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    exit_code: int
    def __init__(self, session_id: _Optional[str] = ..., exit_code: _Optional[int] = ...) -> None: ...

class PtyExecRequest(_message.Message):
    __slots__ = ("id", "session_id", "command", "timeout")
    ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    id: str
    session_id: str
    command: str
    timeout: float
    def __init__(self, id: _Optional[str] = ..., session_id: _Optional[str] = ..., command: _Optional[str] = ..., timeout: _Optional[float] = ...) -> None: ...

class PtyExecResponse(_message.Message):
    __slots__ = ("code", "stdout", "stderr")
    CODE_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    code: int
    stdout: bytes
    stderr: bytes
    def __init__(self, code: _Optional[int] = ..., stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ...) -> None: ...

class ResizeSandboxRequest(_message.Message):
    __slots__ = ("id", "cpus", "memory_mib", "disk_mb")
    ID_FIELD_NUMBER: _ClassVar[int]
    CPUS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_MIB_FIELD_NUMBER: _ClassVar[int]
    DISK_MB_FIELD_NUMBER: _ClassVar[int]
    id: str
    cpus: int
    memory_mib: int
    disk_mb: int
    def __init__(self, id: _Optional[str] = ..., cpus: _Optional[int] = ..., memory_mib: _Optional[int] = ..., disk_mb: _Optional[int] = ...) -> None: ...

class Vpc(_message.Message):
    __slots__ = ("id", "name", "cidr", "created_at_unix_millis")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CIDR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    cidr: str
    created_at_unix_millis: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., cidr: _Optional[str] = ..., created_at_unix_millis: _Optional[int] = ...) -> None: ...

class VpcCreateRequest(_message.Message):
    __slots__ = ("name", "cidr")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CIDR_FIELD_NUMBER: _ClassVar[int]
    name: str
    cidr: str
    def __init__(self, name: _Optional[str] = ..., cidr: _Optional[str] = ...) -> None: ...

class ListVpcsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class VpcList(_message.Message):
    __slots__ = ("vpcs",)
    VPCS_FIELD_NUMBER: _ClassVar[int]
    vpcs: _containers.RepeatedCompositeFieldContainer[Vpc]
    def __init__(self, vpcs: _Optional[_Iterable[_Union[Vpc, _Mapping]]] = ...) -> None: ...

class VpcRef(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class FilePathRequest(_message.Message):
    __slots__ = ("id", "path")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ...) -> None: ...

class FileContent(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class FileWriteRequest(_message.Message):
    __slots__ = ("id", "path", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    data: bytes
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class FileDeleteRequest(_message.Message):
    __slots__ = ("id", "path", "recursive")
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    id: str
    path: str
    recursive: bool
    def __init__(self, id: _Optional[str] = ..., path: _Optional[str] = ..., recursive: _Optional[bool] = ...) -> None: ...

class StringList(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class NetworkSetRequest(_message.Message):
    __slots__ = ("id", "block_network", "cidr_allow", "domain_allow")
    ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_NETWORK_FIELD_NUMBER: _ClassVar[int]
    CIDR_ALLOW_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_ALLOW_FIELD_NUMBER: _ClassVar[int]
    id: str
    block_network: bool
    cidr_allow: StringList
    domain_allow: StringList
    def __init__(self, id: _Optional[str] = ..., block_network: _Optional[bool] = ..., cidr_allow: _Optional[_Union[StringList, _Mapping]] = ..., domain_allow: _Optional[_Union[StringList, _Mapping]] = ...) -> None: ...

class MigrateRequest(_message.Message):
    __slots__ = ("id", "target")
    ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    id: str
    target: str
    def __init__(self, id: _Optional[str] = ..., target: _Optional[str] = ...) -> None: ...

class SnapshotRequest(_message.Message):
    __slots__ = ("id", "name", "stop")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    stop: bool
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., stop: _Optional[bool] = ...) -> None: ...

class SnapshotFsRequest(_message.Message):
    __slots__ = ("id", "name")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class ListSnapshotsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SnapshotList(_message.Message):
    __slots__ = ("snapshots",)
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, snapshots: _Optional[_Iterable[str]] = ...) -> None: ...

class SnapshotRef(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class RecoveryPoint(_message.Message):
    __slots__ = ("name", "kind", "created_at_unix_millis", "size_bytes")
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    name: str
    kind: str
    created_at_unix_millis: int
    size_bytes: int
    def __init__(self, name: _Optional[str] = ..., kind: _Optional[str] = ..., created_at_unix_millis: _Optional[int] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class RecoveryPointList(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[RecoveryPoint]
    def __init__(self, points: _Optional[_Iterable[_Union[RecoveryPoint, _Mapping]]] = ...) -> None: ...

class ListCredentialsRequest(_message.Message):
    __slots__ = ("tenant",)
    TENANT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    def __init__(self, tenant: _Optional[str] = ...) -> None: ...

class CredentialHeader(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: bytes
    def __init__(self, name: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutCredentialRequest(_message.Message):
    __slots__ = ("name", "allowed_domains", "headers", "expires_at_unix_millis", "tenant", "requests_per_minute")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_PER_MINUTE_FIELD_NUMBER: _ClassVar[int]
    name: str
    allowed_domains: _containers.RepeatedScalarFieldContainer[str]
    headers: _containers.RepeatedCompositeFieldContainer[CredentialHeader]
    expires_at_unix_millis: int
    tenant: str
    requests_per_minute: int
    def __init__(self, name: _Optional[str] = ..., allowed_domains: _Optional[_Iterable[str]] = ..., headers: _Optional[_Iterable[_Union[CredentialHeader, _Mapping]]] = ..., expires_at_unix_millis: _Optional[int] = ..., tenant: _Optional[str] = ..., requests_per_minute: _Optional[int] = ...) -> None: ...

class CredentialRef(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DeleteCredentialRequest(_message.Message):
    __slots__ = ("credential", "tenant")
    CREDENTIAL_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    credential: CredentialRef
    tenant: str
    def __init__(self, credential: _Optional[_Union[CredentialRef, _Mapping]] = ..., tenant: _Optional[str] = ...) -> None: ...

class CredentialRecord(_message.Message):
    __slots__ = ("name", "allowed_domains", "header_names", "expires_at_unix_millis", "requests_per_minute", "version")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    HEADER_NAMES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_PER_MINUTE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    allowed_domains: _containers.RepeatedScalarFieldContainer[str]
    header_names: _containers.RepeatedScalarFieldContainer[str]
    expires_at_unix_millis: int
    requests_per_minute: int
    version: str
    def __init__(self, name: _Optional[str] = ..., allowed_domains: _Optional[_Iterable[str]] = ..., header_names: _Optional[_Iterable[str]] = ..., expires_at_unix_millis: _Optional[int] = ..., requests_per_minute: _Optional[int] = ..., version: _Optional[str] = ...) -> None: ...

class CredentialList(_message.Message):
    __slots__ = ("credentials",)
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    credentials: _containers.RepeatedCompositeFieldContainer[CredentialRecord]
    def __init__(self, credentials: _Optional[_Iterable[_Union[CredentialRecord, _Mapping]]] = ...) -> None: ...

class RestoreSnapshotRequest(_message.Message):
    __slots__ = ("name", "body_json")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    name: str
    body_json: str
    def __init__(self, name: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class ForkSnapshotRequest(_message.Message):
    __slots__ = ("name", "body_json")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    name: str
    body_json: str
    def __init__(self, name: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class ListVolumesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class VolumeList(_message.Message):
    __slots__ = ("volumes",)
    VOLUMES_FIELD_NUMBER: _ClassVar[int]
    volumes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, volumes: _Optional[_Iterable[str]] = ...) -> None: ...

class VolumeRef(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListPoolsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PoolSetRequest(_message.Message):
    __slots__ = ("reference", "body_json")
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    reference: str
    body_json: str
    def __init__(self, reference: _Optional[str] = ..., body_json: _Optional[str] = ...) -> None: ...

class PoolRef(_message.Message):
    __slots__ = ("reference",)
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    reference: str
    def __init__(self, reference: _Optional[str] = ...) -> None: ...

class InfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MeshStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EventsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Eof(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Resize(_message.Message):
    __slots__ = ("rows", "cols")
    ROWS_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    rows: int
    cols: int
    def __init__(self, rows: _Optional[int] = ..., cols: _Optional[int] = ...) -> None: ...

class Output(_message.Message):
    __slots__ = ("stream", "data")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    stream: Stream
    data: bytes
    def __init__(self, stream: _Optional[_Union[Stream, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class Exit(_message.Message):
    __slots__ = ("code", "signal")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    code: int
    signal: int
    def __init__(self, code: _Optional[int] = ..., signal: _Optional[int] = ...) -> None: ...

class Ready(_message.Message):
    __slots__ = ("sandbox_id",)
    SANDBOX_ID_FIELD_NUMBER: _ClassVar[int]
    sandbox_id: str
    def __init__(self, sandbox_id: _Optional[str] = ...) -> None: ...

class Digest(_message.Message):
    __slots__ = ("algorithm", "value")
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    algorithm: DigestAlgorithm
    value: bytes
    def __init__(self, algorithm: _Optional[_Union[DigestAlgorithm, str]] = ..., value: _Optional[bytes] = ...) -> None: ...

class ArtifactRef(_message.Message):
    __slots__ = ("digest",)
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    digest: Digest
    def __init__(self, digest: _Optional[_Union[Digest, _Mapping]] = ...) -> None: ...

class ArtifactRecord(_message.Message):
    __slots__ = ("ref", "size_bytes", "stored_size_bytes", "media_type", "created_at_unix_millis", "expires_at_unix_millis")
    REF_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    STORED_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ref: ArtifactRef
    size_bytes: int
    stored_size_bytes: int
    media_type: str
    created_at_unix_millis: int
    expires_at_unix_millis: int
    def __init__(self, ref: _Optional[_Union[ArtifactRef, _Mapping]] = ..., size_bytes: _Optional[int] = ..., stored_size_bytes: _Optional[int] = ..., media_type: _Optional[str] = ..., created_at_unix_millis: _Optional[int] = ..., expires_at_unix_millis: _Optional[int] = ...) -> None: ...

class PutArtifactHeader(_message.Message):
    __slots__ = ("expected_digest", "expected_size_bytes", "media_type", "ttl_millis")
    EXPECTED_DIGEST_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TTL_MILLIS_FIELD_NUMBER: _ClassVar[int]
    expected_digest: Digest
    expected_size_bytes: int
    media_type: str
    ttl_millis: int
    def __init__(self, expected_digest: _Optional[_Union[Digest, _Mapping]] = ..., expected_size_bytes: _Optional[int] = ..., media_type: _Optional[str] = ..., ttl_millis: _Optional[int] = ...) -> None: ...

class PutArtifactRequest(_message.Message):
    __slots__ = ("header", "data")
    HEADER_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    header: PutArtifactHeader
    data: bytes
    def __init__(self, header: _Optional[_Union[PutArtifactHeader, _Mapping]] = ..., data: _Optional[bytes] = ...) -> None: ...

class GetArtifactRequest(_message.Message):
    __slots__ = ("artifact", "range")
    ARTIFACT_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    artifact: ArtifactRef
    range: ByteRange
    def __init__(self, artifact: _Optional[_Union[ArtifactRef, _Mapping]] = ..., range: _Optional[_Union[ByteRange, _Mapping]] = ...) -> None: ...

class ByteRange(_message.Message):
    __slots__ = ("offset", "length")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    offset: int
    length: int
    def __init__(self, offset: _Optional[int] = ..., length: _Optional[int] = ...) -> None: ...

class ArtifactChunk(_message.Message):
    __slots__ = ("offset", "data", "eof")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    EOF_FIELD_NUMBER: _ClassVar[int]
    offset: int
    data: bytes
    eof: bool
    def __init__(self, offset: _Optional[int] = ..., data: _Optional[bytes] = ..., eof: _Optional[bool] = ...) -> None: ...

class PythonCodecMetadata(_message.Message):
    __slots__ = ("implementation", "abi_tag", "python_version", "cloudpickle_version")
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    ABI_TAG_FIELD_NUMBER: _ClassVar[int]
    PYTHON_VERSION_FIELD_NUMBER: _ClassVar[int]
    CLOUDPICKLE_VERSION_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    abi_tag: str
    python_version: str
    cloudpickle_version: str
    def __init__(self, implementation: _Optional[str] = ..., abi_tag: _Optional[str] = ..., python_version: _Optional[str] = ..., cloudpickle_version: _Optional[str] = ...) -> None: ...

class ValueEnvelope(_message.Message):
    __slots__ = ("schema_version", "serializer", "compression", "checksum", "uncompressed_size_bytes", "inline_data", "artifact", "python", "type_name")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SERIALIZER_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    INLINE_DATA_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_FIELD_NUMBER: _ClassVar[int]
    PYTHON_FIELD_NUMBER: _ClassVar[int]
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    serializer: ValueSerializer
    compression: ValueCompression
    checksum: Digest
    uncompressed_size_bytes: int
    inline_data: bytes
    artifact: ArtifactRef
    python: PythonCodecMetadata
    type_name: str
    def __init__(self, schema_version: _Optional[int] = ..., serializer: _Optional[_Union[ValueSerializer, str]] = ..., compression: _Optional[_Union[ValueCompression, str]] = ..., checksum: _Optional[_Union[Digest, _Mapping]] = ..., uncompressed_size_bytes: _Optional[int] = ..., inline_data: _Optional[bytes] = ..., artifact: _Optional[_Union[ArtifactRef, _Mapping]] = ..., python: _Optional[_Union[PythonCodecMetadata, _Mapping]] = ..., type_name: _Optional[str] = ...) -> None: ...

class FunctionRef(_message.Message):
    __slots__ = ("namespace", "name")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    name: str
    def __init__(self, namespace: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class RevisionRef(_message.Message):
    __slots__ = ("function", "revision_id")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    REVISION_ID_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    revision_id: str
    def __init__(self, function: _Optional[_Union[FunctionRef, _Mapping]] = ..., revision_id: _Optional[str] = ...) -> None: ...

class FunctionSelector(_message.Message):
    __slots__ = ("current", "pinned")
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    PINNED_FIELD_NUMBER: _ClassVar[int]
    current: FunctionRef
    pinned: RevisionRef
    def __init__(self, current: _Optional[_Union[FunctionRef, _Mapping]] = ..., pinned: _Optional[_Union[RevisionRef, _Mapping]] = ...) -> None: ...

class AppRef(_message.Message):
    __slots__ = ("namespace", "name")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    name: str
    def __init__(self, namespace: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class AppRevisionRef(_message.Message):
    __slots__ = ("app", "revision_id")
    APP_FIELD_NUMBER: _ClassVar[int]
    REVISION_ID_FIELD_NUMBER: _ClassVar[int]
    app: AppRef
    revision_id: str
    def __init__(self, app: _Optional[_Union[AppRef, _Mapping]] = ..., revision_id: _Optional[str] = ...) -> None: ...

class AppSelector(_message.Message):
    __slots__ = ("current", "pinned")
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    PINNED_FIELD_NUMBER: _ClassVar[int]
    current: AppRef
    pinned: AppRevisionRef
    def __init__(self, current: _Optional[_Union[AppRef, _Mapping]] = ..., pinned: _Optional[_Union[AppRevisionRef, _Mapping]] = ...) -> None: ...

class PackageSpec(_message.Message):
    __slots__ = ("source", "module", "lockfile", "content_digest", "mode", "qualname", "python", "included_paths", "include_globs", "exclude_globs")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    LOCKFILE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    QUALNAME_FIELD_NUMBER: _ClassVar[int]
    PYTHON_FIELD_NUMBER: _ClassVar[int]
    INCLUDED_PATHS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_GLOBS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_GLOBS_FIELD_NUMBER: _ClassVar[int]
    source: ArtifactRef
    module: str
    lockfile: ArtifactRef
    content_digest: Digest
    mode: PackageMode
    qualname: str
    python: PythonCodeMetadata
    included_paths: _containers.RepeatedScalarFieldContainer[str]
    include_globs: _containers.RepeatedScalarFieldContainer[str]
    exclude_globs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, source: _Optional[_Union[ArtifactRef, _Mapping]] = ..., module: _Optional[str] = ..., lockfile: _Optional[_Union[ArtifactRef, _Mapping]] = ..., content_digest: _Optional[_Union[Digest, _Mapping]] = ..., mode: _Optional[_Union[PackageMode, str]] = ..., qualname: _Optional[str] = ..., python: _Optional[_Union[PythonCodeMetadata, _Mapping]] = ..., included_paths: _Optional[_Iterable[str]] = ..., include_globs: _Optional[_Iterable[str]] = ..., exclude_globs: _Optional[_Iterable[str]] = ...) -> None: ...

class ImageSpec(_message.Message):
    __slots__ = ("python", "registry", "dockerfile", "template", "apt_packages", "uv_packages", "commands", "environment", "local_artifact_mounts", "resolved_oci_digest", "platform")
    class EnvironmentEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    PYTHON_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    APT_PACKAGES_FIELD_NUMBER: _ClassVar[int]
    UV_PACKAGES_FIELD_NUMBER: _ClassVar[int]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    LOCAL_ARTIFACT_MOUNTS_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_OCI_DIGEST_FIELD_NUMBER: _ClassVar[int]
    PLATFORM_FIELD_NUMBER: _ClassVar[int]
    python: PythonImageSource
    registry: RegistryImageSource
    dockerfile: DockerfileImageSource
    template: TemplateImageSource
    apt_packages: _containers.RepeatedCompositeFieldContainer[AptPackage]
    uv_packages: _containers.RepeatedCompositeFieldContainer[UvPackage]
    commands: _containers.RepeatedCompositeFieldContainer[ImageBuildCommand]
    environment: _containers.ScalarMap[str, str]
    local_artifact_mounts: _containers.RepeatedCompositeFieldContainer[LocalArtifactMount]
    resolved_oci_digest: Digest
    platform: str
    def __init__(self, python: _Optional[_Union[PythonImageSource, _Mapping]] = ..., registry: _Optional[_Union[RegistryImageSource, _Mapping]] = ..., dockerfile: _Optional[_Union[DockerfileImageSource, _Mapping]] = ..., template: _Optional[_Union[TemplateImageSource, _Mapping]] = ..., apt_packages: _Optional[_Iterable[_Union[AptPackage, _Mapping]]] = ..., uv_packages: _Optional[_Iterable[_Union[UvPackage, _Mapping]]] = ..., commands: _Optional[_Iterable[_Union[ImageBuildCommand, _Mapping]]] = ..., environment: _Optional[_Mapping[str, str]] = ..., local_artifact_mounts: _Optional[_Iterable[_Union[LocalArtifactMount, _Mapping]]] = ..., resolved_oci_digest: _Optional[_Union[Digest, _Mapping]] = ..., platform: _Optional[str] = ...) -> None: ...

class ResourceSpec(_message.Message):
    __slots__ = ("cpus", "memory_bytes", "ephemeral_disk_bytes", "architecture", "high_availability", "volume_mounts", "network")
    CPUS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    EPHEMERAL_DISK_BYTES_FIELD_NUMBER: _ClassVar[int]
    ARCHITECTURE_FIELD_NUMBER: _ClassVar[int]
    HIGH_AVAILABILITY_FIELD_NUMBER: _ClassVar[int]
    VOLUME_MOUNTS_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    cpus: int
    memory_bytes: int
    ephemeral_disk_bytes: int
    architecture: CpuArchitecture
    high_availability: HighAvailabilityPolicy
    volume_mounts: _containers.RepeatedCompositeFieldContainer[FunctionVolumeMount]
    network: NetworkPolicy
    def __init__(self, cpus: _Optional[int] = ..., memory_bytes: _Optional[int] = ..., ephemeral_disk_bytes: _Optional[int] = ..., architecture: _Optional[_Union[CpuArchitecture, str]] = ..., high_availability: _Optional[_Union[HighAvailabilityPolicy, str]] = ..., volume_mounts: _Optional[_Iterable[_Union[FunctionVolumeMount, _Mapping]]] = ..., network: _Optional[_Union[NetworkPolicy, _Mapping]] = ...) -> None: ...

class PythonCodeMetadata(_message.Message):
    __slots__ = ("implementation", "version", "abi_tag", "bytecode_magic", "cloudpickle_version")
    IMPLEMENTATION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ABI_TAG_FIELD_NUMBER: _ClassVar[int]
    BYTECODE_MAGIC_FIELD_NUMBER: _ClassVar[int]
    CLOUDPICKLE_VERSION_FIELD_NUMBER: _ClassVar[int]
    implementation: str
    version: str
    abi_tag: str
    bytecode_magic: bytes
    cloudpickle_version: str
    def __init__(self, implementation: _Optional[str] = ..., version: _Optional[str] = ..., abi_tag: _Optional[str] = ..., bytecode_magic: _Optional[bytes] = ..., cloudpickle_version: _Optional[str] = ...) -> None: ...

class PythonImageSource(_message.Message):
    __slots__ = ("python_version", "variant")
    PYTHON_VERSION_FIELD_NUMBER: _ClassVar[int]
    VARIANT_FIELD_NUMBER: _ClassVar[int]
    python_version: str
    variant: str
    def __init__(self, python_version: _Optional[str] = ..., variant: _Optional[str] = ...) -> None: ...

class RegistryImageSource(_message.Message):
    __slots__ = ("reference",)
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    reference: str
    def __init__(self, reference: _Optional[str] = ...) -> None: ...

class DockerfileImageSource(_message.Message):
    __slots__ = ("context", "dockerfile_path")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_PATH_FIELD_NUMBER: _ClassVar[int]
    context: ArtifactRef
    dockerfile_path: str
    def __init__(self, context: _Optional[_Union[ArtifactRef, _Mapping]] = ..., dockerfile_path: _Optional[str] = ...) -> None: ...

class TemplateImageSource(_message.Message):
    __slots__ = ("name", "revision")
    NAME_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    name: str
    revision: str
    def __init__(self, name: _Optional[str] = ..., revision: _Optional[str] = ...) -> None: ...

class AptPackage(_message.Message):
    __slots__ = ("name", "version")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class UvPackage(_message.Message):
    __slots__ = ("name", "version", "index_url")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INDEX_URL_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    index_url: str
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ..., index_url: _Optional[str] = ...) -> None: ...

class ImageBuildCommand(_message.Message):
    __slots__ = ("argv",)
    ARGV_FIELD_NUMBER: _ClassVar[int]
    argv: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, argv: _Optional[_Iterable[str]] = ...) -> None: ...

class LocalArtifactMount(_message.Message):
    __slots__ = ("artifact", "path", "read_only")
    ARTIFACT_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    artifact: ArtifactRef
    path: str
    read_only: bool
    def __init__(self, artifact: _Optional[_Union[ArtifactRef, _Mapping]] = ..., path: _Optional[str] = ..., read_only: _Optional[bool] = ...) -> None: ...

class FunctionVolumeMount(_message.Message):
    __slots__ = ("volume", "mount_path", "read_only")
    VOLUME_FIELD_NUMBER: _ClassVar[int]
    MOUNT_PATH_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    volume: VolumeRef
    mount_path: str
    read_only: bool
    def __init__(self, volume: _Optional[_Union[VolumeRef, _Mapping]] = ..., mount_path: _Optional[str] = ..., read_only: _Optional[bool] = ...) -> None: ...

class NetworkPolicy(_message.Message):
    __slots__ = ("block_network", "egress_cidrs", "egress_domains", "inbound_cidrs")
    BLOCK_NETWORK_FIELD_NUMBER: _ClassVar[int]
    EGRESS_CIDRS_FIELD_NUMBER: _ClassVar[int]
    EGRESS_DOMAINS_FIELD_NUMBER: _ClassVar[int]
    INBOUND_CIDRS_FIELD_NUMBER: _ClassVar[int]
    block_network: bool
    egress_cidrs: _containers.RepeatedScalarFieldContainer[str]
    egress_domains: _containers.RepeatedScalarFieldContainer[str]
    inbound_cidrs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, block_network: _Optional[bool] = ..., egress_cidrs: _Optional[_Iterable[str]] = ..., egress_domains: _Optional[_Iterable[str]] = ..., inbound_cidrs: _Optional[_Iterable[str]] = ...) -> None: ...

class RetryPolicy(_message.Message):
    __slots__ = ("max_attempts", "initial_backoff_millis", "max_backoff_millis", "backoff_multiplier", "retryable_codes")
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_BACKOFF_MILLIS_FIELD_NUMBER: _ClassVar[int]
    MAX_BACKOFF_MILLIS_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_CODES_FIELD_NUMBER: _ClassVar[int]
    max_attempts: int
    initial_backoff_millis: int
    max_backoff_millis: int
    backoff_multiplier: float
    retryable_codes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, max_attempts: _Optional[int] = ..., initial_backoff_millis: _Optional[int] = ..., max_backoff_millis: _Optional[int] = ..., backoff_multiplier: _Optional[float] = ..., retryable_codes: _Optional[_Iterable[str]] = ...) -> None: ...

class TimeoutSpec(_message.Message):
    __slots__ = ("execution_millis", "queue_millis", "startup_millis", "graceful_shutdown_millis", "result_ttl_millis")
    EXECUTION_MILLIS_FIELD_NUMBER: _ClassVar[int]
    QUEUE_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STARTUP_MILLIS_FIELD_NUMBER: _ClassVar[int]
    GRACEFUL_SHUTDOWN_MILLIS_FIELD_NUMBER: _ClassVar[int]
    RESULT_TTL_MILLIS_FIELD_NUMBER: _ClassVar[int]
    execution_millis: int
    queue_millis: int
    startup_millis: int
    graceful_shutdown_millis: int
    result_ttl_millis: int
    def __init__(self, execution_millis: _Optional[int] = ..., queue_millis: _Optional[int] = ..., startup_millis: _Optional[int] = ..., graceful_shutdown_millis: _Optional[int] = ..., result_ttl_millis: _Optional[int] = ...) -> None: ...

class WorkerSpec(_message.Message):
    __slots__ = ("min_workers", "max_workers", "idle_timeout_millis", "max_calls_per_worker", "buffer_workers", "max_outstanding_inputs")
    MIN_WORKERS_FIELD_NUMBER: _ClassVar[int]
    MAX_WORKERS_FIELD_NUMBER: _ClassVar[int]
    IDLE_TIMEOUT_MILLIS_FIELD_NUMBER: _ClassVar[int]
    MAX_CALLS_PER_WORKER_FIELD_NUMBER: _ClassVar[int]
    BUFFER_WORKERS_FIELD_NUMBER: _ClassVar[int]
    MAX_OUTSTANDING_INPUTS_FIELD_NUMBER: _ClassVar[int]
    min_workers: int
    max_workers: int
    idle_timeout_millis: int
    max_calls_per_worker: int
    buffer_workers: int
    max_outstanding_inputs: int
    def __init__(self, min_workers: _Optional[int] = ..., max_workers: _Optional[int] = ..., idle_timeout_millis: _Optional[int] = ..., max_calls_per_worker: _Optional[int] = ..., buffer_workers: _Optional[int] = ..., max_outstanding_inputs: _Optional[int] = ...) -> None: ...

class ConcurrencySpec(_message.Message):
    __slots__ = ("max_concurrent_calls", "serialize_actor_calls")
    MAX_CONCURRENT_CALLS_FIELD_NUMBER: _ClassVar[int]
    SERIALIZE_ACTOR_CALLS_FIELD_NUMBER: _ClassVar[int]
    max_concurrent_calls: int
    serialize_actor_calls: bool
    def __init__(self, max_concurrent_calls: _Optional[int] = ..., serialize_actor_calls: _Optional[bool] = ...) -> None: ...

class BatchingSpec(_message.Message):
    __slots__ = ("enabled", "max_batch_size", "max_wait_millis")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    MAX_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_WAIT_MILLIS_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    max_batch_size: int
    max_wait_millis: int
    def __init__(self, enabled: _Optional[bool] = ..., max_batch_size: _Optional[int] = ..., max_wait_millis: _Optional[int] = ...) -> None: ...

class SerializerSpec(_message.Message):
    __slots__ = ("input_serializer", "result_serializer", "compression", "allow_trusted_python")
    INPUT_SERIALIZER_FIELD_NUMBER: _ClassVar[int]
    RESULT_SERIALIZER_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    ALLOW_TRUSTED_PYTHON_FIELD_NUMBER: _ClassVar[int]
    input_serializer: ValueSerializer
    result_serializer: ValueSerializer
    compression: ValueCompression
    allow_trusted_python: bool
    def __init__(self, input_serializer: _Optional[_Union[ValueSerializer, str]] = ..., result_serializer: _Optional[_Union[ValueSerializer, str]] = ..., compression: _Optional[_Union[ValueCompression, str]] = ..., allow_trusted_python: _Optional[bool] = ...) -> None: ...

class ReproducibilitySpec(_message.Message):
    __slots__ = ("build_inputs_digest", "builder_id", "builder_version", "source_date_epoch", "environment")
    class EnvironmentEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BUILD_INPUTS_DIGEST_FIELD_NUMBER: _ClassVar[int]
    BUILDER_ID_FIELD_NUMBER: _ClassVar[int]
    BUILDER_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DATE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    build_inputs_digest: Digest
    builder_id: str
    builder_version: str
    source_date_epoch: int
    environment: _containers.ScalarMap[str, str]
    def __init__(self, build_inputs_digest: _Optional[_Union[Digest, _Mapping]] = ..., builder_id: _Optional[str] = ..., builder_version: _Optional[str] = ..., source_date_epoch: _Optional[int] = ..., environment: _Optional[_Mapping[str, str]] = ...) -> None: ...

class SecretRef(_message.Message):
    __slots__ = ("name", "version")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class TransientSecretMaterial(_message.Message):
    __slots__ = ("secret", "value")
    SECRET_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    secret: SecretRef
    value: bytes
    def __init__(self, secret: _Optional[_Union[SecretRef, _Mapping]] = ..., value: _Optional[bytes] = ...) -> None: ...

class LifecycleHookRef(_message.Message):
    __slots__ = ("module", "qualname")
    MODULE_FIELD_NUMBER: _ClassVar[int]
    QUALNAME_FIELD_NUMBER: _ClassVar[int]
    module: str
    qualname: str
    def __init__(self, module: _Optional[str] = ..., qualname: _Optional[str] = ...) -> None: ...

class LifecycleHooks(_message.Message):
    __slots__ = ("initialize", "shutdown", "snapshot", "restore", "snapshot_after_initialize", "snapshot_on_worker_retire")
    INITIALIZE_FIELD_NUMBER: _ClassVar[int]
    SHUTDOWN_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    RESTORE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_AFTER_INITIALIZE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_ON_WORKER_RETIRE_FIELD_NUMBER: _ClassVar[int]
    initialize: LifecycleHookRef
    shutdown: LifecycleHookRef
    snapshot: LifecycleHookRef
    restore: LifecycleHookRef
    snapshot_after_initialize: bool
    snapshot_on_worker_retire: bool
    def __init__(self, initialize: _Optional[_Union[LifecycleHookRef, _Mapping]] = ..., shutdown: _Optional[_Union[LifecycleHookRef, _Mapping]] = ..., snapshot: _Optional[_Union[LifecycleHookRef, _Mapping]] = ..., restore: _Optional[_Union[LifecycleHookRef, _Mapping]] = ..., snapshot_after_initialize: _Optional[bool] = ..., snapshot_on_worker_retire: _Optional[bool] = ...) -> None: ...

class FunctionSnapshotRef(_message.Message):
    __slots__ = ("snapshot_id",)
    SNAPSHOT_ID_FIELD_NUMBER: _ClassVar[int]
    snapshot_id: str
    def __init__(self, snapshot_id: _Optional[str] = ...) -> None: ...

class FunctionSnapshotRecord(_message.Message):
    __slots__ = ("ref", "revision", "artifact", "protocol_version", "runner_digest", "image_digest", "package_digest", "created_at_unix_millis", "initialize_hook")
    REF_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    RUNNER_DIGEST_FIELD_NUMBER: _ClassVar[int]
    IMAGE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    PACKAGE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    INITIALIZE_HOOK_FIELD_NUMBER: _ClassVar[int]
    ref: FunctionSnapshotRef
    revision: RevisionRef
    artifact: ArtifactRef
    protocol_version: int
    runner_digest: Digest
    image_digest: Digest
    package_digest: Digest
    created_at_unix_millis: int
    initialize_hook: LifecycleHookRef
    def __init__(self, ref: _Optional[_Union[FunctionSnapshotRef, _Mapping]] = ..., revision: _Optional[_Union[RevisionRef, _Mapping]] = ..., artifact: _Optional[_Union[ArtifactRef, _Mapping]] = ..., protocol_version: _Optional[int] = ..., runner_digest: _Optional[_Union[Digest, _Mapping]] = ..., image_digest: _Optional[_Union[Digest, _Mapping]] = ..., package_digest: _Optional[_Union[Digest, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., initialize_hook: _Optional[_Union[LifecycleHookRef, _Mapping]] = ...) -> None: ...

class FunctionSpec(_message.Message):
    __slots__ = ("function", "package", "image", "resources", "retry", "timeouts", "workers", "concurrency", "batching", "serializer", "lifecycle", "reproducibility", "labels", "secrets", "lifecycle_hooks")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    PACKAGE_FIELD_NUMBER: _ClassVar[int]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUTS_FIELD_NUMBER: _ClassVar[int]
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    BATCHING_FIELD_NUMBER: _ClassVar[int]
    SERIALIZER_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_FIELD_NUMBER: _ClassVar[int]
    REPRODUCIBILITY_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    LIFECYCLE_HOOKS_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    package: PackageSpec
    image: ImageSpec
    resources: ResourceSpec
    retry: RetryPolicy
    timeouts: TimeoutSpec
    workers: WorkerSpec
    concurrency: ConcurrencySpec
    batching: BatchingSpec
    serializer: SerializerSpec
    lifecycle: FunctionLifecycle
    reproducibility: ReproducibilitySpec
    labels: _containers.ScalarMap[str, str]
    secrets: _containers.RepeatedCompositeFieldContainer[SecretRef]
    lifecycle_hooks: LifecycleHooks
    def __init__(self, function: _Optional[_Union[FunctionRef, _Mapping]] = ..., package: _Optional[_Union[PackageSpec, _Mapping]] = ..., image: _Optional[_Union[ImageSpec, _Mapping]] = ..., resources: _Optional[_Union[ResourceSpec, _Mapping]] = ..., retry: _Optional[_Union[RetryPolicy, _Mapping]] = ..., timeouts: _Optional[_Union[TimeoutSpec, _Mapping]] = ..., workers: _Optional[_Union[WorkerSpec, _Mapping]] = ..., concurrency: _Optional[_Union[ConcurrencySpec, _Mapping]] = ..., batching: _Optional[_Union[BatchingSpec, _Mapping]] = ..., serializer: _Optional[_Union[SerializerSpec, _Mapping]] = ..., lifecycle: _Optional[_Union[FunctionLifecycle, str]] = ..., reproducibility: _Optional[_Union[ReproducibilitySpec, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ..., secrets: _Optional[_Iterable[_Union[SecretRef, _Mapping]]] = ..., lifecycle_hooks: _Optional[_Union[LifecycleHooks, _Mapping]] = ...) -> None: ...

class FunctionRevision(_message.Message):
    __slots__ = ("ref", "spec", "spec_digest", "created_at_unix_millis", "status", "unavailable_secrets", "snapshot")
    REF_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    SPEC_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UNAVAILABLE_SECRETS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    ref: RevisionRef
    spec: FunctionSpec
    spec_digest: Digest
    created_at_unix_millis: int
    status: FunctionRevisionStatus
    unavailable_secrets: _containers.RepeatedCompositeFieldContainer[SecretRef]
    snapshot: FunctionSnapshotRecord
    def __init__(self, ref: _Optional[_Union[RevisionRef, _Mapping]] = ..., spec: _Optional[_Union[FunctionSpec, _Mapping]] = ..., spec_digest: _Optional[_Union[Digest, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., status: _Optional[_Union[FunctionRevisionStatus, str]] = ..., unavailable_secrets: _Optional[_Iterable[_Union[SecretRef, _Mapping]]] = ..., snapshot: _Optional[_Union[FunctionSnapshotRecord, _Mapping]] = ...) -> None: ...

class FunctionRecord(_message.Message):
    __slots__ = ("function", "current", "updated_at_unix_millis")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    current: RevisionRef
    updated_at_unix_millis: int
    def __init__(self, function: _Optional[_Union[FunctionRef, _Mapping]] = ..., current: _Optional[_Union[RevisionRef, _Mapping]] = ..., updated_at_unix_millis: _Optional[int] = ...) -> None: ...

class RegisterFunctionRequest(_message.Message):
    __slots__ = ("spec", "request_id", "transient_secrets")
    SPEC_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSIENT_SECRETS_FIELD_NUMBER: _ClassVar[int]
    spec: FunctionSpec
    request_id: str
    transient_secrets: _containers.RepeatedCompositeFieldContainer[TransientSecretMaterial]
    def __init__(self, spec: _Optional[_Union[FunctionSpec, _Mapping]] = ..., request_id: _Optional[str] = ..., transient_secrets: _Optional[_Iterable[_Union[TransientSecretMaterial, _Mapping]]] = ...) -> None: ...

class GetFunctionRequest(_message.Message):
    __slots__ = ("function",)
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    function: FunctionSelector
    def __init__(self, function: _Optional[_Union[FunctionSelector, _Mapping]] = ...) -> None: ...

class ListFunctionsRequest(_message.Message):
    __slots__ = ("namespace", "function", "page_size", "page_token")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    function: FunctionRef
    page_size: int
    page_token: str
    def __init__(self, namespace: _Optional[str] = ..., function: _Optional[_Union[FunctionRef, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListFunctionsResponse(_message.Message):
    __slots__ = ("revisions", "next_page_token")
    REVISIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    revisions: _containers.RepeatedCompositeFieldContainer[FunctionRevision]
    next_page_token: str
    def __init__(self, revisions: _Optional[_Iterable[_Union[FunctionRevision, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ActivateFunctionRequest(_message.Message):
    __slots__ = ("revision", "expected_current")
    REVISION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CURRENT_FIELD_NUMBER: _ClassVar[int]
    revision: RevisionRef
    expected_current: RevisionRef
    def __init__(self, revision: _Optional[_Union[RevisionRef, _Mapping]] = ..., expected_current: _Optional[_Union[RevisionRef, _Mapping]] = ...) -> None: ...

class DeleteFunctionRequest(_message.Message):
    __slots__ = ("revision",)
    REVISION_FIELD_NUMBER: _ClassVar[int]
    revision: RevisionRef
    def __init__(self, revision: _Optional[_Union[RevisionRef, _Mapping]] = ...) -> None: ...

class AppFunctionBinding(_message.Message):
    __slots__ = ("name", "revision")
    NAME_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    name: str
    revision: RevisionRef
    def __init__(self, name: _Optional[str] = ..., revision: _Optional[_Union[RevisionRef, _Mapping]] = ...) -> None: ...

class AppRevision(_message.Message):
    __slots__ = ("ref", "functions", "content_digest", "created_at_unix_millis", "previous")
    REF_FIELD_NUMBER: _ClassVar[int]
    FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_FIELD_NUMBER: _ClassVar[int]
    ref: AppRevisionRef
    functions: _containers.RepeatedCompositeFieldContainer[AppFunctionBinding]
    content_digest: Digest
    created_at_unix_millis: int
    previous: AppRevisionRef
    def __init__(self, ref: _Optional[_Union[AppRevisionRef, _Mapping]] = ..., functions: _Optional[_Iterable[_Union[AppFunctionBinding, _Mapping]]] = ..., content_digest: _Optional[_Union[Digest, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., previous: _Optional[_Union[AppRevisionRef, _Mapping]] = ...) -> None: ...

class ActivateAppRequest(_message.Message):
    __slots__ = ("app", "functions", "expected_current", "request_id")
    APP_FIELD_NUMBER: _ClassVar[int]
    FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CURRENT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    app: AppRef
    functions: _containers.RepeatedCompositeFieldContainer[AppFunctionBinding]
    expected_current: AppRevisionRef
    request_id: str
    def __init__(self, app: _Optional[_Union[AppRef, _Mapping]] = ..., functions: _Optional[_Iterable[_Union[AppFunctionBinding, _Mapping]]] = ..., expected_current: _Optional[_Union[AppRevisionRef, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class GetAppRequest(_message.Message):
    __slots__ = ("app",)
    APP_FIELD_NUMBER: _ClassVar[int]
    app: AppSelector
    def __init__(self, app: _Optional[_Union[AppSelector, _Mapping]] = ...) -> None: ...

class RollbackAppRequest(_message.Message):
    __slots__ = ("target", "expected_current", "request_id")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_CURRENT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    target: AppRevisionRef
    expected_current: AppRevisionRef
    request_id: str
    def __init__(self, target: _Optional[_Union[AppRevisionRef, _Mapping]] = ..., expected_current: _Optional[_Union[AppRevisionRef, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ScheduleRef(_message.Message):
    __slots__ = ("schedule_id",)
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    def __init__(self, schedule_id: _Optional[str] = ...) -> None: ...

class CronSchedule(_message.Message):
    __slots__ = ("expression", "time_zone")
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    TIME_ZONE_FIELD_NUMBER: _ClassVar[int]
    expression: str
    time_zone: str
    def __init__(self, expression: _Optional[str] = ..., time_zone: _Optional[str] = ...) -> None: ...

class PeriodSchedule(_message.Message):
    __slots__ = ("period_millis", "anchor_unix_millis")
    PERIOD_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ANCHOR_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    period_millis: int
    anchor_unix_millis: int
    def __init__(self, period_millis: _Optional[int] = ..., anchor_unix_millis: _Optional[int] = ...) -> None: ...

class ScheduleTarget(_message.Message):
    __slots__ = ("function", "input")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    function: RevisionRef
    input: ValueEnvelope
    def __init__(self, function: _Optional[_Union[RevisionRef, _Mapping]] = ..., input: _Optional[_Union[ValueEnvelope, _Mapping]] = ...) -> None: ...

class ScheduleSpec(_message.Message):
    __slots__ = ("name", "app", "target", "cron", "period", "status", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    APP_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    CRON_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    name: str
    app: AppRevisionRef
    target: ScheduleTarget
    cron: CronSchedule
    period: PeriodSchedule
    status: ScheduleStatus
    labels: _containers.ScalarMap[str, str]
    def __init__(self, name: _Optional[str] = ..., app: _Optional[_Union[AppRevisionRef, _Mapping]] = ..., target: _Optional[_Union[ScheduleTarget, _Mapping]] = ..., cron: _Optional[_Union[CronSchedule, _Mapping]] = ..., period: _Optional[_Union[PeriodSchedule, _Mapping]] = ..., status: _Optional[_Union[ScheduleStatus, str]] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ScheduleRecord(_message.Message):
    __slots__ = ("ref", "spec", "created_at_unix_millis", "updated_at_unix_millis", "next_run_unix_millis")
    REF_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    NEXT_RUN_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ref: ScheduleRef
    spec: ScheduleSpec
    created_at_unix_millis: int
    updated_at_unix_millis: int
    next_run_unix_millis: int
    def __init__(self, ref: _Optional[_Union[ScheduleRef, _Mapping]] = ..., spec: _Optional[_Union[ScheduleSpec, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., updated_at_unix_millis: _Optional[int] = ..., next_run_unix_millis: _Optional[int] = ...) -> None: ...

class CreateScheduleRequest(_message.Message):
    __slots__ = ("schedule_id", "spec", "request_id")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    spec: ScheduleSpec
    request_id: str
    def __init__(self, schedule_id: _Optional[str] = ..., spec: _Optional[_Union[ScheduleSpec, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ListSchedulesRequest(_message.Message):
    __slots__ = ("app", "function", "page_size", "page_token")
    APP_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    app: AppRef
    function: FunctionRef
    page_size: int
    page_token: str
    def __init__(self, app: _Optional[_Union[AppRef, _Mapping]] = ..., function: _Optional[_Union[FunctionRef, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListSchedulesResponse(_message.Message):
    __slots__ = ("schedules", "next_page_token")
    SCHEDULES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schedules: _containers.RepeatedCompositeFieldContainer[ScheduleRecord]
    next_page_token: str
    def __init__(self, schedules: _Optional[_Iterable[_Union[ScheduleRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CallRef(_message.Message):
    __slots__ = ("call_id",)
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    def __init__(self, call_id: _Optional[str] = ...) -> None: ...

class ActorRef(_message.Message):
    __slots__ = ("actor_id",)
    ACTOR_ID_FIELD_NUMBER: _ClassVar[int]
    actor_id: str
    def __init__(self, actor_id: _Optional[str] = ...) -> None: ...

class InvocationArguments(_message.Message):
    __slots__ = ("positional", "named")
    class NamedEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: ValueEnvelope
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[ValueEnvelope, _Mapping]] = ...) -> None: ...
    POSITIONAL_FIELD_NUMBER: _ClassVar[int]
    NAMED_FIELD_NUMBER: _ClassVar[int]
    positional: _containers.RepeatedCompositeFieldContainer[ValueEnvelope]
    named: _containers.MessageMap[str, ValueEnvelope]
    def __init__(self, positional: _Optional[_Iterable[_Union[ValueEnvelope, _Mapping]]] = ..., named: _Optional[_Mapping[str, ValueEnvelope]] = ...) -> None: ...

class ActorTarget(_message.Message):
    __slots__ = ("actor", "method")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    actor: ActorRef
    method: str
    def __init__(self, actor: _Optional[_Union[ActorRef, _Mapping]] = ..., method: _Optional[str] = ...) -> None: ...

class ServiceTarget(_message.Message):
    __slots__ = ("service_key", "method", "constructor")
    SERVICE_KEY_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    CONSTRUCTOR_FIELD_NUMBER: _ClassVar[int]
    service_key: str
    method: str
    constructor: InvocationArguments
    def __init__(self, service_key: _Optional[str] = ..., method: _Optional[str] = ..., constructor: _Optional[_Union[InvocationArguments, _Mapping]] = ...) -> None: ...

class ParentEdge(_message.Message):
    __slots__ = ("call_id", "input_id")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    input_id: str
    def __init__(self, call_id: _Optional[str] = ..., input_id: _Optional[str] = ...) -> None: ...

class CallTarget(_message.Message):
    __slots__ = ("function", "actor", "service")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    function: RevisionRef
    actor: ActorTarget
    service: ServiceTarget
    def __init__(self, function: _Optional[_Union[RevisionRef, _Mapping]] = ..., actor: _Optional[_Union[ActorTarget, _Mapping]] = ..., service: _Optional[_Union[ServiceTarget, _Mapping]] = ...) -> None: ...

class CallInput(_message.Message):
    __slots__ = ("index", "value", "arguments", "input_id")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    index: int
    value: ValueEnvelope
    arguments: InvocationArguments
    input_id: str
    def __init__(self, index: _Optional[int] = ..., value: _Optional[_Union[ValueEnvelope, _Mapping]] = ..., arguments: _Optional[_Union[InvocationArguments, _Mapping]] = ..., input_id: _Optional[str] = ...) -> None: ...

class InputRef(_message.Message):
    __slots__ = ("input_id", "input_index")
    INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_INDEX_FIELD_NUMBER: _ClassVar[int]
    input_id: str
    input_index: int
    def __init__(self, input_id: _Optional[str] = ..., input_index: _Optional[int] = ...) -> None: ...

class CallGraph(_message.Message):
    __slots__ = ("parents", "root_call_id")
    PARENTS_FIELD_NUMBER: _ClassVar[int]
    ROOT_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    parents: _containers.RepeatedCompositeFieldContainer[ParentEdge]
    root_call_id: str
    def __init__(self, parents: _Optional[_Iterable[_Union[ParentEdge, _Mapping]]] = ..., root_call_id: _Optional[str] = ...) -> None: ...

class CreateCallRequest(_message.Message):
    __slots__ = ("type", "target", "inputs", "inputs_closed", "graph", "request_id", "labels", "result_ttl_millis")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_CLOSED_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    RESULT_TTL_MILLIS_FIELD_NUMBER: _ClassVar[int]
    type: CallType
    target: CallTarget
    inputs: _containers.RepeatedCompositeFieldContainer[CallInput]
    inputs_closed: bool
    graph: CallGraph
    request_id: str
    labels: _containers.ScalarMap[str, str]
    result_ttl_millis: int
    def __init__(self, type: _Optional[_Union[CallType, str]] = ..., target: _Optional[_Union[CallTarget, _Mapping]] = ..., inputs: _Optional[_Iterable[_Union[CallInput, _Mapping]]] = ..., inputs_closed: _Optional[bool] = ..., graph: _Optional[_Union[CallGraph, _Mapping]] = ..., request_id: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ..., result_ttl_millis: _Optional[int] = ...) -> None: ...

class CallRecord(_message.Message):
    __slots__ = ("ref", "type", "target", "status", "inputs_closed", "input_count", "result_count", "graph", "created_at_unix_millis", "updated_at_unix_millis", "error", "stats", "labels", "result_cursor")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REF_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_CLOSED_FIELD_NUMBER: _ClassVar[int]
    INPUT_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESULT_COUNT_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    RESULT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    ref: CallRef
    type: CallType
    target: CallTarget
    status: CallStatus
    inputs_closed: bool
    input_count: int
    result_count: int
    graph: CallGraph
    created_at_unix_millis: int
    updated_at_unix_millis: int
    error: CallError
    stats: CallStats
    labels: _containers.ScalarMap[str, str]
    result_cursor: ResultCursor
    def __init__(self, ref: _Optional[_Union[CallRef, _Mapping]] = ..., type: _Optional[_Union[CallType, str]] = ..., target: _Optional[_Union[CallTarget, _Mapping]] = ..., status: _Optional[_Union[CallStatus, str]] = ..., inputs_closed: _Optional[bool] = ..., input_count: _Optional[int] = ..., result_count: _Optional[int] = ..., graph: _Optional[_Union[CallGraph, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., updated_at_unix_millis: _Optional[int] = ..., error: _Optional[_Union[CallError, _Mapping]] = ..., stats: _Optional[_Union[CallStats, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ..., result_cursor: _Optional[_Union[ResultCursor, _Mapping]] = ...) -> None: ...

class StreamCallInputsRequest(_message.Message):
    __slots__ = ("call", "input")
    CALL_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    input: CallInput
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., input: _Optional[_Union[CallInput, _Mapping]] = ...) -> None: ...

class StreamCallInputsResponse(_message.Message):
    __slots__ = ("call", "committed_input_count", "last_input", "max_inputs_outstanding")
    CALL_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_INPUT_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_INPUT_FIELD_NUMBER: _ClassVar[int]
    MAX_INPUTS_OUTSTANDING_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    committed_input_count: int
    last_input: InputRef
    max_inputs_outstanding: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., committed_input_count: _Optional[int] = ..., last_input: _Optional[_Union[InputRef, _Mapping]] = ..., max_inputs_outstanding: _Optional[int] = ...) -> None: ...

class CloseCallInputsRequest(_message.Message):
    __slots__ = ("call", "expected_input_count")
    CALL_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_INPUT_COUNT_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    expected_input_count: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., expected_input_count: _Optional[int] = ...) -> None: ...

class ListCallsRequest(_message.Message):
    __slots__ = ("function", "status", "actor", "created_after_unix_millis", "page_size", "page_token")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AFTER_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    status: CallStatus
    actor: ActorRef
    created_after_unix_millis: int
    page_size: int
    page_token: str
    def __init__(self, function: _Optional[_Union[FunctionRef, _Mapping]] = ..., status: _Optional[_Union[CallStatus, str]] = ..., actor: _Optional[_Union[ActorRef, _Mapping]] = ..., created_after_unix_millis: _Optional[int] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListCallsResponse(_message.Message):
    __slots__ = ("calls", "next_page_token")
    CALLS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    calls: _containers.RepeatedCompositeFieldContainer[CallRecord]
    next_page_token: str
    def __init__(self, calls: _Optional[_Iterable[_Union[CallRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetCallResultRequest(_message.Message):
    __slots__ = ("call", "index")
    CALL_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    index: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., index: _Optional[int] = ...) -> None: ...

class CallResult(_message.Message):
    __slots__ = ("call", "index", "value", "error", "created_at_unix_millis", "sequence", "input_id", "input_index", "yield_index")
    CALL_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_INDEX_FIELD_NUMBER: _ClassVar[int]
    YIELD_INDEX_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    index: int
    value: ValueEnvelope
    error: CallError
    created_at_unix_millis: int
    sequence: int
    input_id: str
    input_index: int
    yield_index: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., index: _Optional[int] = ..., value: _Optional[_Union[ValueEnvelope, _Mapping]] = ..., error: _Optional[_Union[CallError, _Mapping]] = ..., created_at_unix_millis: _Optional[int] = ..., sequence: _Optional[int] = ..., input_id: _Optional[str] = ..., input_index: _Optional[int] = ..., yield_index: _Optional[int] = ...) -> None: ...

class ResultCursor(_message.Message):
    __slots__ = ("call", "after_sequence")
    CALL_FIELD_NUMBER: _ClassVar[int]
    AFTER_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    after_sequence: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., after_sequence: _Optional[int] = ...) -> None: ...

class EventCursor(_message.Message):
    __slots__ = ("call", "after_sequence")
    CALL_FIELD_NUMBER: _ClassVar[int]
    AFTER_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    after_sequence: int
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., after_sequence: _Optional[int] = ...) -> None: ...

class ListCallResultsRequest(_message.Message):
    __slots__ = ("cursor", "page_size")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    cursor: ResultCursor
    page_size: int
    def __init__(self, cursor: _Optional[_Union[ResultCursor, _Mapping]] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListCallResultsResponse(_message.Message):
    __slots__ = ("results", "next_cursor", "end")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[CallResult]
    next_cursor: ResultCursor
    end: bool
    def __init__(self, results: _Optional[_Iterable[_Union[CallResult, _Mapping]]] = ..., next_cursor: _Optional[_Union[ResultCursor, _Mapping]] = ..., end: _Optional[bool] = ...) -> None: ...

class WatchCallRequest(_message.Message):
    __slots__ = ("cursor", "follow")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    cursor: EventCursor
    follow: bool
    def __init__(self, cursor: _Optional[_Union[EventCursor, _Mapping]] = ..., follow: _Optional[bool] = ...) -> None: ...

class StatusEvent(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: CallStatus
    def __init__(self, status: _Optional[_Union[CallStatus, str]] = ...) -> None: ...

class LogEvent(_message.Message):
    __slots__ = ("stream", "data")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    stream: LogStream
    data: bytes
    def __init__(self, stream: _Optional[_Union[LogStream, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class AttemptEvent(_message.Message):
    __slots__ = ("attempt_id", "user_attempt", "infra_attempt", "status", "startup", "worker_id", "error", "failure_kind")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    INFRA_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STARTUP_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    FAILURE_KIND_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    user_attempt: int
    infra_attempt: int
    status: AttemptStatus
    startup: StartupKind
    worker_id: str
    error: CallError
    failure_kind: AttemptFailureKind
    def __init__(self, attempt_id: _Optional[str] = ..., user_attempt: _Optional[int] = ..., infra_attempt: _Optional[int] = ..., status: _Optional[_Union[AttemptStatus, str]] = ..., startup: _Optional[_Union[StartupKind, str]] = ..., worker_id: _Optional[str] = ..., error: _Optional[_Union[CallError, _Mapping]] = ..., failure_kind: _Optional[_Union[AttemptFailureKind, str]] = ...) -> None: ...

class CallEvent(_message.Message):
    __slots__ = ("call", "sequence", "created_at_unix_millis", "type", "status", "log", "yield_result", "result", "attempt_event", "error", "input_closed", "cancel_requested", "input_id", "input_index", "attempt_id")
    CALL_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    YIELD_RESULT_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_EVENT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    INPUT_CLOSED_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    sequence: int
    created_at_unix_millis: int
    type: CallEventType
    status: StatusEvent
    log: LogEvent
    yield_result: CallResult
    result: CallResult
    attempt_event: AttemptEvent
    error: CallError
    input_closed: StreamCallInputsResponse
    cancel_requested: CancelCallRequest
    input_id: str
    input_index: int
    attempt_id: str
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., sequence: _Optional[int] = ..., created_at_unix_millis: _Optional[int] = ..., type: _Optional[_Union[CallEventType, str]] = ..., status: _Optional[_Union[StatusEvent, _Mapping]] = ..., log: _Optional[_Union[LogEvent, _Mapping]] = ..., yield_result: _Optional[_Union[CallResult, _Mapping]] = ..., result: _Optional[_Union[CallResult, _Mapping]] = ..., attempt_event: _Optional[_Union[AttemptEvent, _Mapping]] = ..., error: _Optional[_Union[CallError, _Mapping]] = ..., input_closed: _Optional[_Union[StreamCallInputsResponse, _Mapping]] = ..., cancel_requested: _Optional[_Union[CancelCallRequest, _Mapping]] = ..., input_id: _Optional[str] = ..., input_index: _Optional[int] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class ErrorFrame(_message.Message):
    __slots__ = ("file", "line", "function", "code")
    FILE_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    file: str
    line: int
    function: str
    code: str
    def __init__(self, file: _Optional[str] = ..., line: _Optional[int] = ..., function: _Optional[str] = ..., code: _Optional[str] = ...) -> None: ...

class CallError(_message.Message):
    __slots__ = ("code", "message", "type", "retryable", "frames", "cause", "details")
    class DetailsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    FRAMES_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    type: str
    retryable: bool
    frames: _containers.RepeatedCompositeFieldContainer[ErrorFrame]
    cause: CallError
    details: _containers.ScalarMap[str, str]
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., type: _Optional[str] = ..., retryable: _Optional[bool] = ..., frames: _Optional[_Iterable[_Union[ErrorFrame, _Mapping]]] = ..., cause: _Optional[_Union[CallError, _Mapping]] = ..., details: _Optional[_Mapping[str, str]] = ...) -> None: ...

class AttemptStats(_message.Message):
    __slots__ = ("attempt_id", "user_attempt", "infra_attempt", "startup", "failure_kind", "queued_millis", "startup_millis", "execution_millis", "cpu_millis", "peak_memory_bytes")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    INFRA_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STARTUP_FIELD_NUMBER: _ClassVar[int]
    FAILURE_KIND_FIELD_NUMBER: _ClassVar[int]
    QUEUED_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STARTUP_MILLIS_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_MILLIS_FIELD_NUMBER: _ClassVar[int]
    CPU_MILLIS_FIELD_NUMBER: _ClassVar[int]
    PEAK_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    user_attempt: int
    infra_attempt: int
    startup: StartupKind
    failure_kind: AttemptFailureKind
    queued_millis: int
    startup_millis: int
    execution_millis: int
    cpu_millis: int
    peak_memory_bytes: int
    def __init__(self, attempt_id: _Optional[str] = ..., user_attempt: _Optional[int] = ..., infra_attempt: _Optional[int] = ..., startup: _Optional[_Union[StartupKind, str]] = ..., failure_kind: _Optional[_Union[AttemptFailureKind, str]] = ..., queued_millis: _Optional[int] = ..., startup_millis: _Optional[int] = ..., execution_millis: _Optional[int] = ..., cpu_millis: _Optional[int] = ..., peak_memory_bytes: _Optional[int] = ...) -> None: ...

class CallStats(_message.Message):
    __slots__ = ("queue_millis", "startup_millis", "execution_millis", "wall_millis", "cpu_millis", "peak_memory_bytes", "attempts")
    QUEUE_MILLIS_FIELD_NUMBER: _ClassVar[int]
    STARTUP_MILLIS_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_MILLIS_FIELD_NUMBER: _ClassVar[int]
    WALL_MILLIS_FIELD_NUMBER: _ClassVar[int]
    CPU_MILLIS_FIELD_NUMBER: _ClassVar[int]
    PEAK_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    queue_millis: int
    startup_millis: int
    execution_millis: int
    wall_millis: int
    cpu_millis: int
    peak_memory_bytes: int
    attempts: _containers.RepeatedCompositeFieldContainer[AttemptStats]
    def __init__(self, queue_millis: _Optional[int] = ..., startup_millis: _Optional[int] = ..., execution_millis: _Optional[int] = ..., wall_millis: _Optional[int] = ..., cpu_millis: _Optional[int] = ..., peak_memory_bytes: _Optional[int] = ..., attempts: _Optional[_Iterable[_Union[AttemptStats, _Mapping]]] = ...) -> None: ...

class CancelCallRequest(_message.Message):
    __slots__ = ("call", "reason", "request_id")
    CALL_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    call: CallRef
    reason: str
    request_id: str
    def __init__(self, call: _Optional[_Union[CallRef, _Mapping]] = ..., reason: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class ActorCheckpointRef(_message.Message):
    __slots__ = ("checkpoint_id",)
    CHECKPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    checkpoint_id: str
    def __init__(self, checkpoint_id: _Optional[str] = ...) -> None: ...

class ActorRecord(_message.Message):
    __slots__ = ("ref", "function", "status", "created_at_unix_millis", "updated_at_unix_millis", "latest_checkpoint", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    REF_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    LATEST_CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    ref: ActorRef
    function: RevisionRef
    status: ActorStatus
    created_at_unix_millis: int
    updated_at_unix_millis: int
    latest_checkpoint: ActorCheckpointRef
    labels: _containers.ScalarMap[str, str]
    def __init__(self, ref: _Optional[_Union[ActorRef, _Mapping]] = ..., function: _Optional[_Union[RevisionRef, _Mapping]] = ..., status: _Optional[_Union[ActorStatus, str]] = ..., created_at_unix_millis: _Optional[int] = ..., updated_at_unix_millis: _Optional[int] = ..., latest_checkpoint: _Optional[_Union[ActorCheckpointRef, _Mapping]] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ActorCheckpoint(_message.Message):
    __slots__ = ("ref", "actor", "function", "state", "sequence", "created_at_unix_millis")
    REF_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MILLIS_FIELD_NUMBER: _ClassVar[int]
    ref: ActorCheckpointRef
    actor: ActorRef
    function: RevisionRef
    state: ValueEnvelope
    sequence: int
    created_at_unix_millis: int
    def __init__(self, ref: _Optional[_Union[ActorCheckpointRef, _Mapping]] = ..., actor: _Optional[_Union[ActorRef, _Mapping]] = ..., function: _Optional[_Union[RevisionRef, _Mapping]] = ..., state: _Optional[_Union[ValueEnvelope, _Mapping]] = ..., sequence: _Optional[int] = ..., created_at_unix_millis: _Optional[int] = ...) -> None: ...

class CreateActorRequest(_message.Message):
    __slots__ = ("function", "initial_value", "initial_arguments", "request_id", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    INITIAL_VALUE_FIELD_NUMBER: _ClassVar[int]
    INITIAL_ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    function: RevisionRef
    initial_value: ValueEnvelope
    initial_arguments: InvocationArguments
    request_id: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, function: _Optional[_Union[RevisionRef, _Mapping]] = ..., initial_value: _Optional[_Union[ValueEnvelope, _Mapping]] = ..., initial_arguments: _Optional[_Union[InvocationArguments, _Mapping]] = ..., request_id: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CheckpointActorRequest(_message.Message):
    __slots__ = ("actor", "request_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    actor: ActorRef
    request_id: str
    def __init__(self, actor: _Optional[_Union[ActorRef, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class RestoreActorRequest(_message.Message):
    __slots__ = ("actor", "checkpoint", "request_id")
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    actor: ActorRef
    checkpoint: ActorCheckpointRef
    request_id: str
    def __init__(self, actor: _Optional[_Union[ActorRef, _Mapping]] = ..., checkpoint: _Optional[_Union[ActorCheckpointRef, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ForkActorRequest(_message.Message):
    __slots__ = ("checkpoint", "request_id", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    checkpoint: ActorCheckpointRef
    request_id: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, checkpoint: _Optional[_Union[ActorCheckpointRef, _Mapping]] = ..., request_id: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...
