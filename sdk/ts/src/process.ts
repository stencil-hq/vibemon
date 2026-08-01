import type { MessageInitShape } from "@bufbuild/protobuf";
import { AsyncQueue, deferred } from "./async-queue";
import { ProtocolError, parseResponseJson } from "./errors";
import type {
  ExecInputSchema,
  ExecOutput,
  PtySession as PtySessionMessage,
} from "./gen/vmon/v1/api_pb";
import { Stream } from "./gen/vmon/v1/api_pb";
import type { EventRecord, ExecExit } from "./models";

/** One decoded process output event. */
export interface ExecEvent {
  stream: "stdout" | "stderr" | "console";
  data: Uint8Array;
}
/** Optional process output callbacks. */
export interface ProcessOptions {
  /** Receive each stdout chunk as it arrives. */
  onStdout?: (data: Uint8Array) => void;
  /** Receive each stderr chunk as it arrives. */
  onStderr?: (data: Uint8Array) => void;
}

/** Init shape for one client-side exec/shell input frame. */
export type ExecInputInit = MessageInitShape<typeof ExecInputSchema>;

/** Cancelable server-streaming RPC handle consumed by the stream wrappers. */
export interface StreamHandle<T> {
  stream: AsyncIterable<T>;
  /** Cancel the underlying RPC stream. */
  cancel(): void;
}

/** Bidirectional exec or shell session over the SandboxService bidi streams. */
export class Process implements AsyncIterable<ExecEvent> {
  readonly #inputs = new AsyncQueue<ExecInputInit>();
  readonly #events = new AsyncQueue<ExecEvent>();
  readonly #exit = deferred<ExecExit>();
  readonly #ready = deferred<void>();
  readonly #options: ProcessOptions;
  #finished = false;
  #closed = false;
  #cancel: (() => void) | null = null;
  /** Writable process standard input. */
  readonly stdin: { write(data: Uint8Array | string): void; close(): void };
  /** Start a session from its first input frame and an RPC opener. */
  constructor(
    open: (inputs: AsyncIterable<ExecInputInit>) => Promise<StreamHandle<ExecOutput>>,
    first: ExecInputInit["input"],
    options: ProcessOptions = {},
  ) {
    this.#options = options;
    this.stdin = {
      write: (data) =>
        this.#send({
          case: "stdin",
          value: typeof data === "string" ? new TextEncoder().encode(data) : data,
        }),
      close: () => this.#send({ case: "eof", value: {} }),
    };
    this.#inputs.push({ input: first });
    void this.#run(open);
  }
  /** Resize the guest terminal. */
  resize(rows: number, cols: number): void {
    this.#send({ case: "resize", value: { rows, cols } });
  }
  /** Wait for the process exit frame. */
  wait(): Promise<ExecExit> {
    return this.#exit.promise;
  }
  /** Cancel the exec RPC; the server terminates the guest process. */
  close(): void {
    this.#closed = true;
    this.#inputs.end();
    this.#cancel?.();
  }
  /** Wait for a shell ready frame. */
  ready(): Promise<void> {
    return Promise.race([
      this.#ready.promise,
      this.#exit.promise.then(() => {
        throw new ProtocolError("process exited before a ready frame");
      }),
    ]);
  }
  /** Iterate decoded output events. */
  [Symbol.asyncIterator](): AsyncIterator<ExecEvent> {
    return this.#events[Symbol.asyncIterator]();
  }
  async #run(
    open: (inputs: AsyncIterable<ExecInputInit>) => Promise<StreamHandle<ExecOutput>>,
  ): Promise<void> {
    try {
      const handle = await open(this.#inputs);
      this.#cancel = () => handle.cancel();
      if (this.#closed) handle.cancel();
      for await (const output of handle.stream) this.#deliver(output);
      if (!this.#finished)
        this.#fail(new ProtocolError("process stream ended before an exit frame"));
    } catch (error) {
      this.#fail(error instanceof Error ? error : new ProtocolError("process stream failed"));
    }
  }
  #deliver(output: ExecOutput): void {
    switch (output.output.case) {
      case "chunk": {
        const { stream, data } = output.output.value;
        const name = streamName(stream);
        if (name === "stdout") this.#options.onStdout?.(data);
        if (name === "stderr") this.#options.onStderr?.(data);
        this.#events.push({ stream: name, data });
        return;
      }
      case "exit": {
        const exit = output.output.value;
        this.#finished = true;
        this.#exit.resolve({
          code: Number(exit.code),
          signal: exit.signal !== undefined ? Number(exit.signal) : null,
        });
        this.#events.end();
        this.#inputs.end();
        return;
      }
      case "ready": {
        this.#ready.resolve();
        return;
      }
      default:
        throw new ProtocolError("unknown exec output frame");
    }
  }
  #send(input: ExecInputInit["input"]): void {
    if (this.#finished || this.#closed) throw new ProtocolError("process is not open");
    this.#inputs.push({ input });
  }
  #fail(error: Error): void {
    if (this.#finished) return;
    this.#finished = true;
    this.#inputs.end();
    this.#events.fail(error);
    this.#exit.reject(error);
  }
}

/** Options for a persistent PTY stream. */
export interface PtyStreamOptions {
  onData?: (data: Uint8Array) => void;
  onExit?: (code: number) => void;
  onClose?: () => void;
  onError?: (error: Error) => void;
}

/** Bidirectional attachment to a server-persistent PTY session. */
export class PtyStream implements AsyncIterable<Uint8Array> {
  readonly #inputs = new AsyncQueue<ExecInputInit>();
  readonly #chunks = new AsyncQueue<Uint8Array>();
  readonly #metadata = deferred<PtySessionMessage>();
  readonly #options: PtyStreamOptions;
  #closed = false;
  #session: PtySessionMessage | null = null;
  #sawExit = false;
  /** Start an open or attach stream. */
  constructor(
    open: (inputs: AsyncIterable<ExecInputInit>) => Promise<StreamHandle<ExecOutput>>,
    first: ExecInputInit["input"],
    options: PtyStreamOptions = {},
  ) {
    this.#options = options;
    this.#inputs.push({ input: first });
    void this.#run(open);
  }
  /** Metadata from the mandatory first server frame. */
  get meta(): Promise<PtySessionMessage> {
    return this.#metadata.promise;
  }
  /** Stable session id, once the first frame has arrived. */
  get sessionId(): string | undefined {
    return this.#session?.sessionId;
  }
  /** Write bytes to the PTY. */
  write(data: Uint8Array | string): void {
    this.#send({
      case: "stdin",
      value: typeof data === "string" ? new TextEncoder().encode(data) : data,
    });
  }
  /** Change the PTY dimensions. */
  resize(rows: number, cols: number): void {
    this.#send({ case: "resize", value: { rows, cols } });
  }
  /** Detach this stream without terminating the server session. */
  detach(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#inputs.push({ input: { case: "eof", value: {} } });
    this.#inputs.end();
  }
  /** Alias for detach; closing a stream never terminates its PTY session. */
  close(): void {
    this.detach();
  }
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return this.#chunks[Symbol.asyncIterator]();
  }
  async #run(
    open: (inputs: AsyncIterable<ExecInputInit>) => Promise<StreamHandle<ExecOutput>>,
  ): Promise<void> {
    try {
      const handle = await open(this.#inputs);
      for await (const output of handle.stream) {
        if (this.#session === null) {
          if (output.output.case !== "pty")
            throw new ProtocolError("PTY stream did not begin with session metadata");
          this.#session = output.output.value;
          this.#metadata.resolve(output.output.value);
          continue;
        }
        if (output.output.case === "chunk") {
          const data = output.output.value.data;
          this.#options.onData?.(data);
          this.#chunks.push(data);
        } else if (output.output.case === "exit") {
          this.#sawExit = true;
          this.#options.onExit?.(Number(output.output.value.code));
        } else {
          throw new ProtocolError("malformed PTY output frame");
        }
      }
      if (!this.#closed && !this.#sawExit)
        throw new ProtocolError("PTY transport ended unexpectedly");
      this.#chunks.end();
      this.#options.onClose?.();
    } catch (error) {
      const failure = error instanceof Error ? error : new ProtocolError("PTY stream failed");
      this.#metadata.reject(failure);
      this.#chunks.fail(failure);
      this.#options.onError?.(failure);
    }
  }
  #send(input: ExecInputInit["input"]): void {
    if (this.#closed) throw new ProtocolError("PTY stream is not open");
    this.#inputs.push({ input });
  }
}

/** Read-only console attachment stream. */
export class ConsoleStream implements AsyncIterable<Uint8Array> {
  readonly #chunks = new AsyncQueue<Uint8Array>();
  #cancel: (() => void) | null = null;
  #closed = false;
  /** Bind a console stream to an attach RPC opener. */
  constructor(open: () => Promise<StreamHandle<ExecOutput>>) {
    void this.#run(open);
  }
  /** Cancel the attach RPC. */
  close(): void {
    this.#closed = true;
    this.#cancel?.();
  }
  /** Iterate decoded console bytes. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return this.#chunks[Symbol.asyncIterator]();
  }
  async #run(open: () => Promise<StreamHandle<ExecOutput>>): Promise<void> {
    try {
      const handle = await open();
      this.#cancel = () => handle.cancel();
      if (this.#closed) handle.cancel();
      for await (const output of handle.stream) {
        if (output.output.case === "chunk" && output.output.value.stream === Stream.CONSOLE)
          this.#chunks.push(output.output.value.data);
        else throw new ProtocolError("malformed console frame");
      }
      this.#chunks.end();
    } catch (error) {
      this.#chunks.fail(error);
    }
  }
}

/** Server-streamed daemon event feed. */
export class EventStream implements AsyncIterable<EventRecord> {
  readonly #handle: StreamHandle<{ json: string }>;
  /** Bind an event stream to a server-streaming RPC handle. */
  constructor(handle: StreamHandle<{ json: string }>) {
    this.#handle = handle;
  }
  /** Cancel the event stream. */
  close(): void {
    this.#handle.cancel();
  }
  /** Iterate decoded daemon events. */
  async *[Symbol.asyncIterator](): AsyncIterator<EventRecord> {
    for await (const view of this.#handle.stream) {
      const parsed = parseResponseJson(view.json);
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
        throw new ProtocolError("event document must be an object");
      yield parsed;
    }
  }
}

/** Server-streamed follow-log feed. */
export class LogStream implements AsyncIterable<string> {
  readonly #handle: StreamHandle<{ data: Uint8Array }>;
  /** Bind a log stream to a server-streaming RPC handle. */
  constructor(handle: StreamHandle<{ data: Uint8Array }>) {
    this.#handle = handle;
  }
  /** Cancel the log stream. */
  close(): void {
    this.#handle.cancel();
  }
  /** Iterate decoded log chunks. */
  async *[Symbol.asyncIterator](): AsyncIterator<string> {
    const decoder = new TextDecoder();
    for await (const chunk of this.#handle.stream) {
      const text = decoder.decode(chunk.data, { stream: true });
      if (text) yield text;
    }
  }
}

function streamName(stream: Stream): "stdout" | "stderr" | "console" {
  switch (stream) {
    case Stream.STDOUT:
      return "stdout";
    case Stream.STDERR:
      return "stderr";
    case Stream.CONSOLE:
      return "console";
    default:
      throw new ProtocolError("unknown output stream");
  }
}
