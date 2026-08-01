"""Streaming process, console, event, and log objects."""

from __future__ import annotations

import contextlib
import queue
import threading
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any

import grpc

from ._endpoint import decode_view, translate_rpc_error
from .errors import ProtocolError, TransportError
from .models import EventRecord
from .v1 import api_pb2

if TYPE_CHECKING:
    from .models import ExecExit

_EOF = object()
_INPUT_DONE = object()

# A session starter receives a factory producing a fresh ExecInput iterator per
# connection attempt and returns the response stream plus the chosen endpoint.
SessionStarter = Callable[[Callable[[], Iterator[api_pb2.ExecInput]]], tuple[Any, str | None]]


class ByteStream(Iterable[bytes]):
    """Thread-safe byte iterator used for process output streams."""

    def __init__(self) -> None:
        self._queue: queue.Queue[bytes | object] = queue.Queue()
        self._buffer = bytearray()
        self._closed = False
        self._eof = False

    def feed(self, data: bytes) -> None:
        if data:
            self._queue.put(bytes(data))

    def close(self) -> None:
        """Mark the stream complete and wake blocked readers."""
        if not self._closed:
            self._closed = True
            self._queue.put(_EOF)

    def __iter__(self) -> ByteStream:
        return self

    def __next__(self) -> bytes:
        if self._buffer:
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        if self._eof:
            raise StopIteration
        item = self._queue.get()
        if item is _EOF:
            self._eof = True
            raise StopIteration
        if not isinstance(item, bytes):
            raise ProtocolError("process stream received an invalid chunk")
        return item

    def read_chunk(self, timeout: float | None = None) -> bytes | None:
        """Return the next available chunk, or ``None`` at end-of-stream.

        Raises ``TimeoutError`` when no chunk arrives within ``timeout`` seconds.
        """
        if self._buffer:
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        if self._eof:
            return None
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("timed out waiting for process output") from None
        if item is _EOF:
            self._eof = True
            return None
        if not isinstance(item, bytes):
            raise ProtocolError("process stream received an invalid chunk")
        return item

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes, or all bytes through end-of-stream."""
        if size == 0:
            return b""
        if size > 0:
            while len(self._buffer) < size and not self._eof:
                item = self._queue.get()
                if item is _EOF:
                    self._eof = True
                    break
                if not isinstance(item, bytes):
                    raise ProtocolError("process stream received an invalid chunk")
                self._buffer.extend(item)
            data = bytes(self._buffer[:size])
            del self._buffer[:size]
            return data

        chunks: list[bytes] = []
        if self._buffer:
            chunks.append(bytes(self._buffer))
            self._buffer.clear()
        while not self._eof:
            item = self._queue.get()
            if item is _EOF:
                self._eof = True
                break
            if not isinstance(item, bytes):
                raise ProtocolError("process stream received an invalid chunk")
            chunks.append(item)
        return b"".join(chunks)


class _ProcessStdin:
    def __init__(self, session: _ProcessSession) -> None:
        self._session = session
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        """Write bytes or UTF-8 text to the process standard input."""
        self._session.write_stdin(data)

    def close(self) -> None:
        """Send end-of-file once."""
        if not self._closed:
            try:
                self._session.close_stdin()
            finally:
                self._closed = True


class _ProcessSession:
    def __init__(
        self,
        starter: SessionStarter,
        first_input: api_pb2.ExecInput,
        *,
        timeout: float | None,
        tty: bool,
        consume_ready: bool,
        consume_pty: bool,
        thread_name: str,
    ) -> None:
        self.stdout = ByteStream()
        self.stderr = ByteStream()
        self._timeout = timeout
        self._tty = tty
        self._exit = threading.Event()
        self._returncode: int | None = None
        self._signal: int | None = None
        self._ready_name: str | None = None
        self._error: BaseException | None = None
        self._pty: api_pb2.PtySession | None = None
        self._send_lock = threading.Lock()
        self._stdin_closed = False
        self._closing = False
        self._first = first_input
        self._inputs: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._responses, self._endpoint = starter(self._make_inputs)
        try:
            if consume_ready:
                self._consume_ready()
            if consume_pty:
                self._consume_pty()
        except BaseException:
            self._shutdown()
            raise
        self._reader = threading.Thread(target=self._read_loop, name=thread_name, daemon=True)
        self._reader.start()

    def _make_inputs(self) -> Iterator[api_pb2.ExecInput]:
        yield self._first
        while True:
            item = self._inputs.get()
            if item is _INPUT_DONE:
                return
            yield item

    def _shutdown(self) -> None:
        self._inputs.put(_INPUT_DONE)
        with contextlib.suppress(Exception):
            self._responses.cancel()

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @property
    def signal(self) -> int | None:
        return self._signal

    @property
    def ready_name(self) -> str | None:
        return self._ready_name

    @property
    def pty(self) -> api_pb2.PtySession | None:
        return self._pty

    def _consume_ready(self) -> None:
        try:
            frame = next(iter(self._responses))
        except StopIteration:
            raise ProtocolError("shell closed before its ready frame") from None
        except grpc.RpcError as exc:
            raise translate_rpc_error(
                exc, endpoint=self._endpoint, fallback_message="shell setup failed"
            ) from exc
        if frame.WhichOneof("output") != "ready" or not frame.ready.sandbox_id:
            raise ProtocolError("shell ready frame omitted the sandbox id")
        self._ready_name = frame.ready.sandbox_id

    def _consume_pty(self) -> None:
        try:
            frame = next(iter(self._responses))
        except StopIteration:
            raise ProtocolError("PTY stream closed before its metadata frame") from None
        except grpc.RpcError as exc:
            raise translate_rpc_error(
                exc, endpoint=self._endpoint, fallback_message="PTY setup failed"
            ) from exc
        if frame.WhichOneof("output") != "pty" or not frame.pty.session_id:
            raise ProtocolError("PTY metadata frame omitted the session id")
        self._pty = frame.pty

    def write_stdin(self, data: bytes | bytearray | memoryview | str) -> None:
        raw = data.encode() if isinstance(data, str) else bytes(data)
        if not raw:
            return
        with self._send_lock:
            if self._stdin_closed:
                raise TransportError("process stdin is closed")
            self._inputs.put(api_pb2.ExecInput(stdin=raw))

    def close_stdin(self) -> None:
        with self._send_lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
            self._inputs.put(api_pb2.ExecInput(eof=api_pb2.Eof()))

    def resize(self, rows: int, cols: int) -> None:
        if rows <= 0 or cols <= 0:
            raise ValueError("terminal rows and columns must be positive")
        self._inputs.put(api_pb2.ExecInput(resize=api_pb2.Resize(rows=int(rows), cols=int(cols))))

    def kill(self, signal: int = 15) -> None:
        self._closing = True
        self._signal = int(signal)
        self._shutdown()

    def wait(self, timeout: float | None = None) -> ExecExit:
        if not self._tty and not self._stdin_closed:
            with contextlib.suppress(Exception):
                self.close_stdin()
        effective = self._timeout if timeout is None else timeout
        if not self._exit.wait(effective):
            self.kill()
            raise TimeoutError("process timed out")
        if self._error is not None:
            raise self._error
        from .models import ExecExit

        return ExecExit(
            code=self._returncode if self._returncode is not None else -1,
            signal=self._signal,
        )

    def _read_loop(self) -> None:
        try:
            for frame in self._responses:
                kind = frame.WhichOneof("output")
                if kind == "chunk":
                    target = (
                        self.stderr if frame.chunk.stream == api_pb2.STREAM_STDERR else self.stdout
                    )
                    target.feed(frame.chunk.data)
                    continue
                if kind == "exit":
                    self._returncode = frame.exit.code
                    self._signal = frame.exit.signal if frame.exit.HasField("signal") else None
                    return
                if kind == "ready":
                    self._error = ProtocolError("unexpected process ready frame")
                    return
                if kind == "pty":
                    self._error = ProtocolError("unexpected PTY metadata frame")
                    return
                self._error = ProtocolError("unknown process frame")
            if self._returncode is None:
                self._error = ProtocolError("process stream closed before an exit frame")
        except grpc.RpcError as exc:
            if not self._closing:
                self._error = translate_rpc_error(
                    exc, endpoint=self._endpoint, fallback_message="exec failed"
                )
        except BaseException as exc:
            if not self._closing:
                self._error = exc
        finally:
            self.stdout.close()
            self.stderr.close()
            self._exit.set()
            self._shutdown()


class Process:
    """A streaming command or shell with stdin, output streams, and an exit result."""

    def __init__(self, session: _ProcessSession) -> None:
        self._session = session
        self.stdout = session.stdout
        self.stderr = session.stderr
        self.stdin = _ProcessStdin(session)

    @property
    def returncode(self) -> int | None:
        """Return the exit code after completion, otherwise ``None``."""
        return self._session.returncode

    @property
    def sandbox_id(self) -> str | None:
        """Return the ephemeral shell sandbox id announced by the server."""
        return self._session.ready_name

    def resize(self, rows: int, cols: int) -> None:
        """Resize the remote pseudo-terminal."""
        self._session.resize(rows, cols)

    def wait(self, timeout: float | None = None) -> ExecExit:
        """Wait for completion and return both exit code and signal."""
        try:
            return self._session.wait(timeout)
        finally:
            self._close_streams()

    def close(self) -> None:
        """Close this process, terminating a command that is still running."""
        if self.returncode is None:
            self._session.kill()
        self._close_streams()

    def _close_streams(self) -> None:
        with contextlib.suppress(Exception):
            self.stdout.close()
        with contextlib.suppress(Exception):
            self.stderr.close()

    def __enter__(self) -> Process:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class PtyStream(Process):
    """One client attachment to a server-persistent pseudo-terminal session."""

    def __init__(self, session: _ProcessSession) -> None:
        super().__init__(session)
        if session.pty is None:
            raise ProtocolError("PTY stream omitted session metadata")
        self.session = session.pty

    @property
    def session_id(self) -> str:
        """Return the stable server-assigned session identifier."""
        return self.session.session_id

    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        """Write bytes or UTF-8 text to the terminal."""
        self.stdin.write(data)

    def detach(self) -> None:
        """Disconnect this attachment without terminating the guest session."""
        self.close()


class ConsoleStream(Iterable[bytes]):
    """A closeable byte stream from a sandbox console attachment."""

    def __init__(self, responses: Any, endpoint: str | None = None) -> None:
        self._responses = responses
        self._endpoint = endpoint
        self._stream = ByteStream()
        self._done = threading.Event()
        self._error: BaseException | None = None
        self._closing = False
        self._reader = threading.Thread(
            target=self._read_loop,
            name="vmon-console-attach",
            daemon=True,
        )
        self._reader.start()

    def __iter__(self) -> ConsoleStream:
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._stream)
        except StopIteration:
            self.wait()
            raise

    def read(self, size: int = -1) -> bytes:
        """Read console bytes through completion or the requested size."""
        data = self._stream.read(size)
        if size < 0:
            self.wait()
        return data

    def wait(self, timeout: float | None = None) -> None:
        """Wait for completion and raise a structured stream error."""
        if not self._done.wait(timeout):
            raise TimeoutError("console attachment timed out")
        if self._error is not None:
            raise self._error

    def close(self) -> None:
        """Close the console attachment idempotently."""
        self._closing = True
        with contextlib.suppress(Exception):
            self._responses.cancel()
        self._stream.close()

    def _read_loop(self) -> None:
        try:
            for frame in self._responses:
                if (
                    frame.WhichOneof("output") != "chunk"
                    or frame.chunk.stream != api_pb2.STREAM_CONSOLE
                ):
                    self._error = ProtocolError("unknown console frame")
                    return
                self._stream.feed(frame.chunk.data)
        except grpc.RpcError as exc:
            if not self._closing:
                self._error = translate_rpc_error(
                    exc,
                    endpoint=self._endpoint,
                    fallback_message="console attachment failed",
                )
        except BaseException as exc:
            if not self._closing:
                self._error = exc
        finally:
            self._stream.close()
            self._done.set()
            with contextlib.suppress(Exception):
                self._responses.cancel()

    def __enter__(self) -> ConsoleStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class EventStream(Iterable[EventRecord]):
    """A closeable iterator over typed engine events from the Events RPC."""

    def __init__(self, responses: Any, endpoint: str | None = None) -> None:
        self._responses = responses
        self._endpoint = endpoint
        self._closed = False

    def __iter__(self) -> Iterator[EventRecord]:
        try:
            for view in self._responses:
                yield self._decode(view.json)
        except grpc.RpcError as exc:
            if not self._closed:
                raise translate_rpc_error(
                    exc, endpoint=self._endpoint, fallback_message="event stream failed"
                ) from exc
        finally:
            self.close()

    @staticmethod
    def _decode(payload: str) -> EventRecord:
        value = decode_view(payload, "vmon event stream returned malformed data")
        return EventRecord.from_dict(value)

    def close(self) -> None:
        """Cancel the streaming call idempotently."""
        if not self._closed:
            self._closed = True
            with contextlib.suppress(Exception):
                self._responses.cancel()

    def __enter__(self) -> EventStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class LogStream(Iterable[str]):
    """A closeable iterator over decoded sandbox log chunks."""

    def __init__(self, responses: Any, endpoint: str | None = None) -> None:
        self._responses = responses
        self._endpoint = endpoint
        self._closed = False

    def __iter__(self) -> Iterator[str]:
        try:
            for chunk in self._responses:
                yield chunk.data.decode("utf-8", errors="replace")
        except grpc.RpcError as exc:
            if not self._closed:
                raise translate_rpc_error(
                    exc, endpoint=self._endpoint, fallback_message="log stream failed"
                ) from exc
        finally:
            self.close()

    def close(self) -> None:
        """Cancel the streaming call idempotently."""
        if not self._closed:
            self._closed = True
            with contextlib.suppress(Exception):
                self._responses.cancel()

    def __enter__(self) -> LogStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_process(
    starter: SessionStarter,
    first_input: api_pb2.ExecInput,
    *,
    timeout: float | None = None,
    tty: bool = False,
    consume_ready: bool = False,
    consume_pty: bool = False,
    thread_name: str = "vmon-process",
) -> Process:
    """Open a streaming exec or shell session over a bidi gRPC call."""
    return Process(
        _ProcessSession(
            starter,
            first_input,
            timeout=timeout,
            tty=tty,
            consume_ready=consume_ready,
            consume_pty=consume_pty,
            thread_name=thread_name,
        )
    )


def open_pty_stream(
    starter: SessionStarter,
    first_input: api_pb2.ExecInput,
    *,
    thread_name: str = "vmon-pty",
) -> PtyStream:
    """Open an attachment to a server-persistent PTY over the exec stream."""
    return PtyStream(
        _ProcessSession(
            starter,
            first_input,
            timeout=None,
            tty=True,
            consume_ready=False,
            consume_pty=True,
            thread_name=thread_name,
        )
    )
