import type {
  DescMessage,
  DescMethodBiDiStreaming,
  DescMethodServerStreaming,
  DescMethodUnary,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import { base64Encode } from "@bufbuild/protobuf/wire";
import type { Client } from "./client";
import type { DriverRequestOptions, DriverResponse, RpcStream } from "./driver";
import { APIError, apiError, ProtocolError, parseResponseJson } from "./errors";
import { SandboxService } from "./gen/vmon/v1/api_pb";
import type {
  ExecRequest,
  ExecResult,
  FileInfo,
  NetworkPolicy,
  RecoveryPoint,
  SandboxInfo,
  SandboxMetrics,
  TunnelSet,
} from "./models";
import type { ProcessOptions } from "./process";
import { ConsoleStream, LogStream, Process, PtyStream } from "./process";
import type { SecretInput } from "./values";
import { mergeSecretEnv } from "./values";

/** Options for captured command execution. */
export interface RunOptions extends Omit<ExecRequest, "cmd"> {
  secrets?: Iterable<SecretInput> | null;
}
/** Options for streaming command execution. */
export interface ExecOptions extends RunOptions, ProcessOptions {}
/** Readiness probe and polling options. */
export interface WaitReadyOptions {
  probe?: number | string | { port: number };
  timeout?: number;
  interval?: number;
}
/** Sandbox list filters. */
export interface SandboxListOptions {
  tags?: Record<string, string>;
}

const sandboxChannel = Symbol("vmon.sandbox.channel");
const sandboxToken = Symbol("vmon.sandbox.token");

/** A sandbox resource bound to its client; mesh routing remains private. */
export class Sandbox {
  readonly #client: Client;
  #info: SandboxInfo;
  readonly [sandboxChannel]: SandboxChannel;
  [sandboxToken]?: string;
  readonly files: Files;
  readonly ports: Ports;
  /** Bind a sandbox view or identifier to a client. */
  constructor(client: Client, info: SandboxInfo | string) {
    this.#client = client;
    this.#info = typeof info === "string" ? { id: info } : info;
    this[sandboxChannel] = new SandboxChannel(client, this.id);
    this.files = new Files(this);
    this.ports = new Ports(this);
  }
  /** Stable sandbox identifier. */
  get id(): string {
    return this.#info.id;
  }
  /** Latest typed sandbox view. */
  get info(): Readonly<SandboxInfo> {
    return this.#info;
  }
  /** Mesh node that reported the latest sandbox view. */
  get node(): string | null {
    return typeof this.#info.node === "string" ? this.#info.node : null;
  }
  /** Owning root client. */
  get client(): Client {
    return this.#client;
  }

  /** Refresh and merge the sandbox view. */
  async refresh(): Promise<SandboxInfo> {
    const view = await channelFor(this).call(SandboxService.method.get, { id: this.id });
    this.#info = mergeInfo(this.#info, parseResponseJson(view.json));
    return this.#info;
  }
  /** Run a command and capture its output. */
  async run(cmd: string[], options: RunOptions = {}): Promise<ExecResult> {
    const { secrets, ...request } = options;
    const body: ExecRequest = { ...request, cmd };
    if (secrets) body.env = { ...(body.env ?? {}), ...mergeSecretEnv(secrets) };
    const result = await channelFor(this).call(SandboxService.method.execCapture, {
      id: this.id,
      exec: {
        cmd,
        workdir: body.workdir ?? undefined,
        env: body.env ?? {},
        timeout: body.timeout ?? undefined,
        tty: body.tty ?? false,
      },
    });
    return {
      exit: Number(result.code),
      stdout_b64: base64Encode(result.stdout),
      stderr_b64: base64Encode(result.stderr),
    };
  }
  /** Open a streaming command process. */
  async exec(cmd: string[], options: ExecOptions = {}): Promise<Process> {
    const { secrets, onStdout, onStderr, ...request } = options;
    const body: ExecRequest = { ...request, cmd };
    if (secrets) body.env = { ...(body.env ?? {}), ...mergeSecretEnv(secrets) };
    return new Process(
      (inputs) => channelFor(this).duplex(SandboxService.method.exec, inputs),
      {
        case: "start",
        value: {
          sandboxId: this.id,
          cmd,
          workdir: body.workdir ?? undefined,
          env: body.env ?? {},
          timeout: body.timeout ?? undefined,
          tty: body.tty ?? false,
        },
      },
      { onStdout, onStderr },
    );
  }
  /** Open a server-persistent PTY session. */
  async ptyOpen(
    options: {
      sessionId?: string;
      cols?: number;
      rows?: number;
      exec?: string;
      env?: Record<string, string>;
      workdir?: string;
      onData?: (data: Uint8Array) => void;
      onExit?: (code: number) => void;
      onClose?: () => void;
      onError?: (error: Error) => void;
    } = {},
  ): Promise<PtyStream> {
    const { onData, onExit, onClose, onError, ...start } = options;
    const stream = new PtyStream(
      (inputs) => channelFor(this).duplex(SandboxService.method.ptyOpen, inputs),
      {
        case: "ptyOpen",
        value: {
          sandboxId: this.id,
          sessionId: start.sessionId ?? "",
          cols: start.cols ?? 80,
          rows: start.rows ?? 24,
          exec: start.exec,
          env: start.env ?? {},
          workdir: start.workdir,
        },
      },
      { onData, onExit, onClose, onError },
    );
    await stream.meta;
    return stream;
  }
  /** Attach to an existing server-persistent PTY session. */
  async ptyAttach(
    sessionId: string,
    options: {
      cols?: number;
      rows?: number;
      onData?: (data: Uint8Array) => void;
      onExit?: (code: number) => void;
      onClose?: () => void;
      onError?: (error: Error) => void;
    } = {},
  ): Promise<PtyStream> {
    const { cols, rows, onData, onExit, onClose, onError } = options;
    const stream = new PtyStream(
      (inputs) => channelFor(this).duplex(SandboxService.method.ptyAttach, inputs),
      { case: "ptyAttach", value: { sandboxId: this.id, sessionId, cols, rows } },
      { onData, onExit, onClose, onError },
    );
    await stream.meta;
    return stream;
  }
  /** List persistent PTY sessions in this sandbox. */
  async ptyList() {
    return (await channelFor(this).call(SandboxService.method.ptyList, { id: this.id })).sessions;
  }
  /** Terminate and remove a persistent PTY session. */
  async ptyClose(sessionId: string) {
    return channelFor(this).call(SandboxService.method.ptyClose, {
      id: this.id,
      sessionId,
    });
  }
  /** Execute a sibling command in a persistent PTY session's context. */
  async ptyExec(sessionId: string, command: string, timeoutMs = 60_000) {
    const response = await channelFor(this).call(SandboxService.method.ptyExec, {
      id: this.id,
      sessionId,
      command,
      timeout: timeoutMs / 1000,
    });
    return { code: Number(response.code), stdout: response.stdout, stderr: response.stderr };
  }
  /** Attach a read-only console stream. */
  async attach(): Promise<ConsoleStream> {
    return new ConsoleStream(() =>
      channelFor(this).stream(SandboxService.method.attach, { id: this.id }),
    );
  }
  /** Fetch current logs as text. */
  async logs(): Promise<string> {
    const handle = await channelFor(this).stream(SandboxService.method.logs, {
      id: this.id,
      follow: false,
    });
    const decoder = new TextDecoder();
    let text = "";
    for await (const chunk of handle.stream) text += decoder.decode(chunk.data, { stream: true });
    return text;
  }
  /** Follow logs as a stream. */
  async followLogs(): Promise<LogStream> {
    return new LogStream(
      await channelFor(this).stream(SandboxService.method.logs, { id: this.id, follow: true }),
    );
  }
  /** Fetch sandbox metrics. */
  async metrics(): Promise<SandboxMetrics> {
    const value = parseResponseJson(
      (await channelFor(this).call(SandboxService.method.metrics, { id: this.id })).json,
    );
    if (value === null || typeof value !== "object" || Array.isArray(value))
      throw new ProtocolError("sandbox metrics must be an object");
    return value;
  }
  /** Gracefully stop the sandbox. */
  async stop(wait = true): Promise<SandboxInfo> {
    this.#info = mergeInfo(
      this.#info,
      parseResponseJson(
        (await channelFor(this).call(SandboxService.method.stop, { id: this.id })).json,
      ),
    );
    if (wait) await this.#waitForState(new Set(["stopped", "exited"]));
    return this.#info;
  }
  /** Forcefully terminate the sandbox. */
  async terminate(wait = true): Promise<void> {
    await channelFor(this).call(SandboxService.method.terminate, { id: this.id });
    if (wait) await this.#waitForState(new Set(["terminated", "stopped", "exited"]));
  }
  /** Remove the sandbox record. */
  async remove(): Promise<void> {
    await channelFor(this).call(SandboxService.method.remove, { id: this.id });
  }
  /** Pause sandbox execution. */
  async pause(): Promise<SandboxInfo> {
    this.#info = mergeInfo(
      this.#info,
      parseResponseJson(
        (await channelFor(this).call(SandboxService.method.pause, { id: this.id })).json,
      ),
    );
    return this.#info;
  }
  /** Resume paused/suspended state or fresh-boot a stopped sandbox. */
  async resume(): Promise<SandboxInfo> {
    this.#info = mergeInfo(
      this.#info,
      parseResponseJson(
        (await channelFor(this).call(SandboxService.method.resume, { id: this.id })).json,
      ),
    );
    return this.#info;
  }
  /** Resize CPU, memory, or the grow-only root disk. */
  async resize(options: {
    cpus?: number;
    memoryMib?: number;
    diskMb?: number | bigint;
  }): Promise<SandboxInfo> {
    const view = await channelFor(this).call(SandboxService.method.resize, {
      id: this.id,
      cpus: options.cpus,
      memoryMib: options.memoryMib,
      diskMb: options.diskMb === undefined ? undefined : BigInt(options.diskMb),
    });
    this.#info = mergeInfo(this.#info, parseResponseJson(view.json));
    return this.#info;
  }

  /** Durably checkpoint and release the live VM while preserving its identity. */
  async suspend(): Promise<SandboxInfo> {
    this.#info = mergeInfo(
      this.#info,
      parseResponseJson(
        (await channelFor(this).call(SandboxService.method.suspend, { id: this.id })).json,
      ),
    );
    return this.#info;
  }
  /** List retained recovery points from oldest to newest. */
  async history(): Promise<RecoveryPoint[]> {
    const response = await channelFor(this).call(SandboxService.method.history, { id: this.id });
    return response.points.map((point) => ({
      name: point.name,
      kind: point.kind,
      created_at_unix_millis: point.createdAtUnixMillis,
      size_bytes: point.sizeBytes,
    }));
  }
  /** Restore this sandbox identity to a retained recovery point. */
  async rollback(recoveryPoint: string): Promise<SandboxInfo> {
    if (!recoveryPoint) throw new TypeError("recovery point is required");
    this.#info = mergeInfo(
      this.#info,
      parseResponseJson(
        (
          await channelFor(this).call(SandboxService.method.rollback, {
            id: this.id,
            recoveryPoint,
          })
        ).json,
      ),
    );
    return this.#info;
  }
  /** Extend the sandbox lease. */
  async extend(secs: number): Promise<SandboxInfo> {
    const view = await channelFor(this).call(SandboxService.method.extend, {
      id: this.id,
      secs: BigInt(secs),
    });
    this.#info = mergeInfo(this.#info, parseResponseJson(view.json));
    return this.#info;
  }
  /** Replace and rearm the network-idle suspension policy; zero disables it. */
  async setIdleTimeout(secs: number): Promise<SandboxInfo> {
    if (!Number.isFinite(secs) || secs < 0)
      throw new TypeError("idle timeout seconds must be non-negative");
    const view = await channelFor(this).call(SandboxService.method.setIdleTimeout, {
      id: this.id,
      idleTimeoutSecs: secs,
    });
    this.#info = mergeInfo(this.#info, parseResponseJson(view.json));
    return this.#info;
  }
  /** Migrate the sandbox to a mesh node and re-pin its serving endpoint. */
  async migrate(target: string): Promise<SandboxInfo> {
    if (!target) throw new TypeError("target node id is required");
    const view = await channelFor(this).call(SandboxService.method.migrate, {
      id: this.id,
      target,
    });
    this.#info = mergeInfo(this.#info, parseResponseJson(view.json));
    await channelFor(this).relocate();
    return this.#info;
  }
  /** Create a full VM snapshot. */
  async snapshot(name?: string, stop = false): Promise<string> {
    const view = await channelFor(this).call(SandboxService.method.snapshot, {
      id: this.id,
      name: name ?? undefined,
      stop,
    });
    return responseField(parseResponseJson(view.json), "snapshot");
  }
  /** Create a filesystem snapshot. */
  async snapshotFilesystem(name?: string): Promise<string> {
    const view = await channelFor(this).call(SandboxService.method.snapshotFs, {
      id: this.id,
      name: name ?? undefined,
    });
    return responseField(parseResponseJson(view.json), "image");
  }
  /** Fetch the network policy. */
  async network(): Promise<NetworkPolicy> {
    return networkPolicy(
      parseResponseJson(
        (await channelFor(this).call(SandboxService.method.networkGet, { id: this.id })).json,
      ),
    );
  }
  /** Replace the network policy. */
  async setNetwork(policy: NetworkPolicy): Promise<NetworkPolicy> {
    const view = await channelFor(this).call(SandboxService.method.networkSet, {
      id: this.id,
      blockNetwork: policy.block_network ?? undefined,
      cidrAllow: policy.cidr_allow != null ? { values: policy.cidr_allow } : undefined,
      domainAllow: policy.domain_allow != null ? { values: policy.domain_allow } : undefined,
    });
    return networkPolicy(parseResponseJson(view.json));
  }
  /** Fetch tunnels and cache the proxy token. */
  async tunnels(): Promise<TunnelSet> {
    const view = await channelFor(this).call(SandboxService.method.tunnels, { id: this.id });
    const value = parseResponseJson(view.json);
    if (!isTunnelSet(value)) throw new ProtocolError("tunnel response must be an object");
    if (typeof value.connect_token === "string") this[sandboxToken] = value.connect_token;
    return value;
  }
  /** Wait for a command or port readiness probe. */
  async waitReady(options: WaitReadyOptions = {}): Promise<SandboxInfo> {
    if (options.probe === undefined) return this.#info;
    const deadline = Date.now() + (options.timeout ?? 300) * 1_000;
    const interval = options.interval ?? 100;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const port =
          typeof options.probe === "number"
            ? options.probe
            : typeof options.probe === "object"
              ? options.probe.port
              : undefined;
        if (port !== undefined) {
          const response = await this.ports.http(port, "GET");
          if (!response.ok) throw new Error(`readiness port returned HTTP ${response.status}`);
        } else {
          const result = await this.run(["sh", "-lc", String(options.probe)], { timeout: 5 });
          if (result.exit !== 0) throw new Error(`readiness command exited with ${result.exit}`);
        }
        return this.#info;
      } catch (error) {
        lastError = error;
        await sleep(interval);
      }
    }
    throw new Error(
      `sandbox readiness probe timed out${lastError instanceof Error ? `: ${lastError.message}` : ""}`,
      lastError === undefined ? undefined : { cause: lastError },
    );
  }
  async #waitForState(states: Set<string>): Promise<void> {
    const deadline = Date.now() + 300_000;
    while (Date.now() < deadline) {
      const info = await this.refresh();
      const state = typeof info.observed_state === "string" ? info.observed_state : info.status;
      if (state && states.has(state)) return;
      await sleep(250);
    }
    throw new Error(`sandbox state wait timed out after 300 seconds`);
  }
}

/** Build a sandbox carrying an SDK-internal mesh route. */
export function sandboxFromRoute(
  client: Client,
  info: SandboxInfo | string,
  endpoint?: string,
): Sandbox {
  const sandbox = new Sandbox(client, info);
  sandbox[sandboxChannel].bind(endpoint);
  return sandbox;
}

class SandboxChannel {
  readonly #client: Client;
  readonly id: string;
  #endpoint?: string;

  constructor(client: Client, id: string, endpoint?: string) {
    this.#client = client;
    this.id = id;
    this.#endpoint = endpoint;
  }

  get endpoint(): string | undefined {
    return this.#endpoint;
  }

  bind(endpoint?: string): void {
    this.#endpoint = endpoint;
  }

  async relocate(): Promise<void> {
    this.#endpoint = await this.#client.driver.resolveSandbox(this.id, this.#endpoint);
  }

  async call<I extends DescMessage, O extends DescMessage>(
    method: DescMethodUnary<I, O>,
    input: MessageInitShape<I>,
  ): Promise<MessageShape<O>> {
    const execute = async () => {
      const result = await this.#client.driver.call(method, input, { endpoint: this.#endpoint });
      this.#endpoint = result.endpoint;
      return result.message;
    };
    return this.#relocating(execute);
  }

  async stream<I extends DescMessage, O extends DescMessage>(
    method: DescMethodServerStreaming<I, O>,
    input: MessageInitShape<I>,
  ): Promise<RpcStream<MessageShape<O>>> {
    const execute = async () => {
      const result = await this.#client.driver.serverStream(method, input, {
        endpoint: this.#endpoint,
      });
      this.#endpoint = result.endpoint;
      return result;
    };
    return this.#relocating(execute);
  }

  async duplex<I extends DescMessage, O extends DescMessage>(
    method: DescMethodBiDiStreaming<I, O>,
    input: AsyncIterable<MessageInitShape<I>>,
  ): Promise<RpcStream<MessageShape<O>>> {
    const execute = async () => {
      const result = await this.#client.driver.duplex(method, input, {
        endpoint: this.#endpoint,
      });
      this.#endpoint = result.endpoint;
      return result;
    };
    return this.#relocating(execute);
  }

  async requestRaw(
    method: string,
    suffix: string,
    options: DriverRequestOptions = {},
  ): Promise<DriverResponse> {
    const execute = async () => {
      const response = await this.#client.driver.request(
        method,
        `/v1/sandboxes/${encodeURIComponent(this.id)}${suffix}`,
        {
          ...options,
          endpoint: this.#endpoint,
        },
      );
      this.#endpoint = response.endpoint;
      return response;
    };
    const response = await execute();
    if (response.status !== 404 || this.#client.driver.endpoints().length <= 1) return response;
    const error = await apiError(response.clone());
    if (error.code !== "not_found") return response;
    await this.relocate();
    return execute();
  }

  async websocket(
    suffix: string,
    query?: URLSearchParams | Record<string, string>,
  ): Promise<[WebSocket, string]> {
    const execute = async () => {
      const result = await this.#client.driver.websocket(
        `/v1/sandboxes/${encodeURIComponent(this.id)}${suffix}`,
        {
          query,
          endpoint: this.#endpoint,
        },
      );
      this.#endpoint = result[1];
      return result;
    };
    try {
      return await execute();
    } catch (error) {
      if (
        !(error instanceof APIError) ||
        error.code !== "not_found" ||
        this.#client.driver.endpoints().length <= 1
      )
        throw error;
      await this.relocate();
      return execute();
    }
  }

  async #relocating<T>(execute: () => Promise<T>): Promise<T> {
    try {
      return await execute();
    } catch (error) {
      if (
        !(error instanceof APIError) ||
        error.code !== "not_found" ||
        this.#client.driver.endpoints().length <= 1
      )
        throw error;
      await this.relocate();
      return execute();
    }
  }
}

export function channelFor(sandbox: Sandbox): SandboxChannel {
  return sandbox[sandboxChannel];
}

async function proxyToken(sandbox: Sandbox): Promise<string> {
  let token = sandbox[sandboxToken];
  if (!token) {
    await sandbox.tunnels();
    token = sandbox[sandboxToken];
  }
  if (!token) throw new ProtocolError("server did not return a connect token");
  return token;
}

/** Bound guest filesystem operations. */
export class Files {
  readonly #sandbox: Sandbox;
  /** Bind filesystem operations to a sandbox. */
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  /** Read raw guest file bytes. */
  async read(path: string): Promise<Uint8Array> {
    const content = await channelFor(this.#sandbox).call(SandboxService.method.fileRead, {
      id: this.#sandbox.id,
      path,
    });
    return content.data;
  }
  /** Read a UTF-8 guest file. */
  async readText(path: string): Promise<string> {
    return new TextDecoder().decode(await this.read(path));
  }
  /** Write raw guest file content. */
  async write(path: string, content: BodyInit): Promise<void> {
    const data = new Uint8Array(await new Response(content).arrayBuffer());
    await channelFor(this.#sandbox).call(SandboxService.method.fileWrite, {
      id: this.#sandbox.id,
      path,
      data,
    });
  }
  /** Write UTF-8 guest file content. */
  writeText(path: string, content: string): Promise<void> {
    return this.write(path, content);
  }
  /** List guest directory entries. */
  async list(path = "."): Promise<FileInfo[]> {
    const view = await channelFor(this.#sandbox).call(SandboxService.method.fileList, {
      id: this.#sandbox.id,
      path,
    });
    const body = parseResponseJson(view.json);
    if (!isRecord(body) || !Array.isArray(body.entries))
      throw new ProtocolError("file list response did not include entries");
    const entries: FileInfo[] = [];
    for (const entry of body.entries) {
      if (!isFileInfo(entry))
        throw new ProtocolError("file list response included invalid metadata");
      entries.push(entry);
    }
    return entries;
  }
  /** Stat a guest filesystem path. */
  async stat(path: string): Promise<FileInfo> {
    const view = await channelFor(this.#sandbox).call(SandboxService.method.fileStat, {
      id: this.#sandbox.id,
      path,
    });
    const value = parseResponseJson(view.json);
    if (!isFileInfo(value)) throw new ProtocolError("file stat response was invalid");
    return value;
  }
  /** Delete a guest path, optionally recursively. */
  async delete(path: string, recursive = false): Promise<void> {
    await channelFor(this.#sandbox).call(SandboxService.method.fileDelete, {
      id: this.#sandbox.id,
      path,
      recursive,
    });
  }
  /** Create a guest directory and its parents. */
  async mkdir(path: string): Promise<void> {
    const result = await this.#sandbox.run(["mkdir", "-p", "--", path]);
    if (result.exit !== 0)
      throw new APIError({
        status: 0,
        code: "engine",
        message: new TextDecoder().decode(decode(result.stderr_b64)),
      });
  }
  /** Open a streaming guest file reader. */
  async open(path: string): Promise<ReadableStream<Uint8Array>> {
    const data = await this.read(path);
    return new ReadableStream({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });
  }
}

/** Bound HTTP and WebSocket port proxy operations. */
export class Ports {
  readonly #sandbox: Sandbox;
  /** Bind port proxies to a sandbox. */
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  /** Proxy an HTTP request to an exposed guest port. */
  async http(
    port: number,
    method: string,
    path = "",
    options: Omit<DriverRequestOptions, "endpoint"> = {},
  ): Promise<Response> {
    validatePort(port);
    const params = sanitizedParams(options.params);
    const token = await proxyToken(this.#sandbox);
    if (token) params.set("connect_token", token);
    return channelFor(this.#sandbox).requestRaw(
      method,
      `/ports/${port}/${path.replace(/^\//, "")}`,
      {
        ...options,
        params,
      },
    );
  }
  /** Proxy a WebSocket to an exposed guest port. */
  async websocket(
    port: number,
    path = "",
    query: URLSearchParams | Record<string, string> = {},
  ): Promise<WebSocket> {
    validatePort(port);
    const params = sanitizedParams(query);
    const token = await proxyToken(this.#sandbox);
    if (token) params.set("connect_token", token);
    const [socket] = await channelFor(this.#sandbox).websocket(
      `/ports/${port}/ws/${path.replace(/^\//, "")}`,
      params,
    );
    return socket;
  }
}

function validatePort(port: number): void {
  if (!Number.isInteger(port) || port < 0 || port > 65_535)
    throw new RangeError("port must be an integer between 0 and 65535");
}

async function sleep(milliseconds: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, milliseconds);
  return promise;
}

function sanitizedParams(
  input?: URLSearchParams | Record<string, string | number | boolean | null | undefined>,
): URLSearchParams {
  const params = new URLSearchParams();
  if (input instanceof URLSearchParams) {
    input.forEach((value, key) => {
      params.append(key, value);
    });
  } else if (input)
    for (const key in input) {
      const value = input[key];
      if (value !== null && value !== undefined) params.set(key, String(value));
    }
  params.delete("connect_token");
  params.delete("token");
  params.delete("access_token");
  return params;
}
function responseField(value: unknown, field: "snapshot" | "image"): string {
  if (isRecord(value) && typeof value[field] === "string") return value[field];
  throw new ProtocolError(`snapshot response did not include ${field}`);
}
function mergeInfo(current: SandboxInfo, value: unknown): SandboxInfo {
  if (!isRecord(value)) throw new ProtocolError("sandbox response must be an object");
  return { ...current, ...value };
}
function networkPolicy(value: unknown): NetworkPolicy {
  if (!isNetworkPolicy(value)) throw new ProtocolError("network response must be an object");
  return value;
}
function isNetworkPolicy(value: unknown): value is NetworkPolicy {
  return (
    isRecord(value) &&
    (value.block_network === undefined || typeof value.block_network === "boolean") &&
    (value.cidr_allow === undefined ||
      (Array.isArray(value.cidr_allow) &&
        value.cidr_allow.every((entry) => typeof entry === "string"))) &&
    (value.domain_allow === undefined ||
      (Array.isArray(value.domain_allow) &&
        value.domain_allow.every((entry) => typeof entry === "string")))
  );
}
function isTunnelSet(value: unknown): value is TunnelSet {
  if (
    !isRecord(value) ||
    (value.connect_token !== undefined && typeof value.connect_token !== "string")
  )
    return false;
  if (value.tunnels === undefined) return true;
  if (!isRecord(value.tunnels) || Array.isArray(value.tunnels)) return false;
  for (const port in value.tunnels) {
    const target = value.tunnels[port];
    if (
      !isRecord(target) ||
      typeof target.host !== "string" ||
      typeof target.port !== "number" ||
      !Number.isInteger(target.port)
    )
      return false;
  }
  return true;
}
function isFileInfo(value: unknown): value is FileInfo {
  return (
    isRecord(value) &&
    (value.ok === undefined || typeof value.ok === "boolean") &&
    (value.name === undefined || typeof value.name === "string") &&
    (value.type === undefined || typeof value.type === "string") &&
    (value.size === undefined || typeof value.size === "number") &&
    (value.mode === undefined || typeof value.mode === "number") &&
    (value.mtime === undefined || typeof value.mtime === "number")
  );
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
function decode(value: string): Uint8Array {
  if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(value, "base64"));
  return Uint8Array.from(atob(value), (character) => character.charCodeAt(0));
}
