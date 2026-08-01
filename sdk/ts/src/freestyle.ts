/**
 * Freestyle-shaped VM compatibility subset over the vmon SDK.
 *
 * Follows the published `freestyle` npm package's VM surface (`freestyle@0.1.63`
 * `index.d.mts`, https://www.freestyle.sh/docs/vms) closely enough that most VM
 * code ports by swapping the import, but it is deliberately NOT a drop-in for
 * every published declaration — see the divergence list below.
 *
 * ```ts
 * import { freestyle } from "@vmon/sdk/freestyle";
 *
 * const { vm, vmId } = await freestyle.vms.create();
 * const result = await vm.exec("echo 'hello from vibemon'");
 * await vm.fs.writeTextFile("/tmp/hello.txt", "hi");
 * const { forks } = await vm.fork({ count: 2 });
 * await freestyle.vms.delete({ vmId });
 * ```
 *
 * Mapping onto vmon:
 * - [`Freestyle`] options: `apiKey`/`accessToken` are the vmon API token, `baseUrl`
 *   the vmon DSN, and `fetch` overrides the HTTP transport.
 * - `vms.create` → sandbox create; Freestyle VMs have no wall-clock lease.
 *   `idleTimeoutSeconds: null` disables network-idle reclaim.
 *   `create({ snapshotId })` → snapshot restore, which resumes the captured machine
 *   state (processes and memory) rather than fresh-booting it.
 * - `vm.stop()` discards in-memory state while retaining the VM disk and identity;
 *   `vm.start()` resumes paused/suspended VMs or fresh-boots stopped VMs.
 * - `vm.fork()` → full VM snapshot + atomic snapshot fork; the base snapshot is retained.
 * - `vm.pty.open()` creates a guest-owned persistent terminal. Attachments may detach
 *   and reconnect by stable session id; sessions survive suspend/resume and are inherited
 *   by memory-preserving forks.
 *
 * Divergences from the published declarations (why this is a subset, not a drop-in):
 * - `vms.create` always returns `domains: []` and additionally accepts vmon sizing
 *   (`image`, `cpu`, `memory` GB, `storage` GB, `env`, `workdir`, `tags`).
 * - `persistence`, `activityThresholdBytes`, and `exec({ terminal })` use vmon-native
 *   semantics. Ephemeral VMs discard stored state on stop/suspend; sticky VMs retain
 *   state but may be evicted by storage GC in ascending priority. VPC NICs are backed
 *   on Linux hosts, support one NIC, and require `mode: "routed"`; macOS rejects them.
 * - Type-level: `vm.fs.readFile` returns `Uint8Array` (not a Node `Buffer`) to stay
 *   browser-compatible; `stat.owner`/`stat.group` and snapshot `createdAt` are optional
 *   because the vmon API does not report them; PTY open/attach option shapes remain a
 *   simplified subset while reconnect uses bounded exponential backoff.
 * - Freestyle surfaces with no vmon backing service are not provided at all:
 *   `git`, `domains`, `identities`, `dns`, `serverless`, `cron`, `whoami`, and raw
 *   `fetch`. Code using them fails at compile time by design.
 */

import { Client, type ConnectOptions, connect } from "./client";
import type { VmonFetch } from "./driver";
import { APIError } from "./errors";
import type { PtySession as ProtoPtySession } from "./gen/vmon/v1/api_pb";
import type { SandboxCreateRequest, SandboxInfo } from "./models";
import type { PtyStream } from "./process";
import type { Sandbox } from "./sandbox";

/** Connection options for the Freestyle-shaped client. */
export interface FreestyleOptions {
  /** vmon API bearer token (Freestyle's `apiKey` slot). */
  apiKey?: string;
  /** vmon API bearer token (Freestyle's `accessToken` slot); wins over `apiKey`. */
  accessToken?: string;
  /** vmon server URL or DSN (Freestyle's `baseUrl` slot); defaults to the environment. */
  baseUrl?: string;
  /** Override the HTTP transport used for non-RPC requests. */
  fetch?: VmonFetch;
}

/** VM lifecycle states reported by [`VmsNamespace.list`]. */
export type VmState =
  | "starting"
  | "running"
  | "suspending"
  | "suspended"
  | "stopped"
  | "lost"
  | "building";

/** One row of [`VmsNamespace.list`]. */
export interface VmListEntry {
  id: string;
  state: VmState;
  createdAt?: string | null;
  lastNetworkActivity?: string | null;
  snapshotId?: string | null;
  deleted?: boolean;
}

/** Options accepted by [`VmsNamespace.create`]. */
export interface VmCreateOptions {
  /**
   * Restore a snapshot created with [`Vm.snapshot`] instead of booting an image.
   * Full VM snapshots resume the captured machine state — processes and memory
   * included — they do not fresh-boot.
   */
  snapshotId?: string | null;
  /** Human-readable VM name. */
  name?: string | null;
  /** Seconds without qualifying network activity before reclaim; `null` disables reclaim. */
  idleTimeoutSeconds?: number | null;
  /** Raw guest NIC bytes per sampling interval that still count as idle. */
  activityThresholdBytes?: number | null;
  /** Stored-state retention and storage-GC policy. */
  persistence?:
    | { type: "persistent" }
    | { type: "sticky"; priority?: number }
    | { type: "ephemeral" };
  /** One routed VPC NIC. Linux hosts only; vmon VMs have a single NIC. */
  nics?: { default?: boolean; vpc: string; mode: "routed"; ipv4: string | true }[];
  /** vmon extension: OCI image reference to boot. */
  image?: string;
  /** vmon extension: filesystem template name (e.g. from `sandbox.snapshotFilesystem()`). */
  template?: string;
  /** vmon extension: whole vCPU count (vmon fixes sizing at create time). */
  cpu?: number;
  /** vmon extension: memory in GB. */
  memory?: number;
  /** vmon extension: root filesystem size in GB. */
  storage?: number;
  /** vmon extension: guest environment variables. */
  env?: Record<string, string>;
  /** vmon extension: guest working directory. */
  workdir?: string;
  /** vmon extension: searchable sandbox tags. */
  tags?: Record<string, string>;
}

/** One snapshot row; vmon reports names only, so timestamps are absent. */
export interface VmSnapshot {
  snapshotId: string;
  name?: string | null;
  sourceVmId?: string | null;
  state?: string;
  createdAt?: string;
}

/** Captured output of one [`Vm.exec`] command. */
export interface VmExecResult {
  stdout?: string | null;
  stderr?: string | null;
  statusCode?: number | null;
}

/** Snapshot collection operations. */
export class VmSnapshotsNamespace {
  readonly #freestyle: Freestyle;
  constructor(freestyle: Freestyle) {
    this.#freestyle = freestyle;
  }
  /** List snapshots; vmon retains only ready snapshots, so filters are no-ops. */
  async list(_options?: {
    includeDeleted?: boolean;
    includeFailed?: boolean;
    includeBuilding?: boolean;
    includeCancelled?: boolean;
    includeLost?: boolean;
  }): Promise<{ snapshots: VmSnapshot[] }> {
    const names = await this.#freestyle.client.snapshots.list();
    return { snapshots: names.map((name) => ({ snapshotId: name, state: "ready" })) };
  }
  /**
   * Fetch one snapshot by identifier.
   *
   * # Errors
   * Throws [`APIError`] with code `not_found` when the snapshot does not exist.
   */
  async get(options: { snapshotId: string }): Promise<VmSnapshot> {
    const { snapshots } = await this.list();
    const snapshot = snapshots.find((row) => row.snapshotId === options.snapshotId);
    if (snapshot === undefined)
      throw new APIError({
        status: 404,
        code: "not_found",
        message: `snapshot ${options.snapshotId} does not exist`,
      });
    return snapshot;
  }
}

/** VM collection operations. */
export class VmsNamespace {
  /** Snapshot collection operations. */
  readonly snapshots: VmSnapshotsNamespace;
  readonly #freestyle: Freestyle;
  constructor(freestyle: Freestyle) {
    this.#freestyle = freestyle;
    this.snapshots = new VmSnapshotsNamespace(freestyle);
  }
  /**
   * Create a VM, either fresh or from a snapshot.
   *
   * Throws `TypeError` for multiple NICs, non-routed NICs, or when sizing is
   * combined with `snapshotId` (snapshots fix CPU, memory, and disk).
   */
  async create(
    options: VmCreateOptions = {},
  ): Promise<{ vm: Vm; vmId: string; domains: string[] }> {
    if (options.nics !== undefined && options.nics.length > 1)
      throw new TypeError("vmon VMs have a single NIC");
    if (options.nics?.some((nic) => nic.mode !== "routed"))
      throw new TypeError('vmon VPC NICs require mode: "routed"');
    const client = this.#freestyle.client;
    // Freestyle VMs have no wall-clock lease; idle reclaim is network-activity based.
    const idleTimeout =
      options.idleTimeoutSeconds === undefined ? undefined : (options.idleTimeoutSeconds ?? 0);
    const persistence =
      options.persistence?.type === "sticky"
        ? {
            type: "sticky" as const,
            priority: Math.min(10, Math.max(0, Math.trunc(options.persistence.priority ?? 5))),
          }
        : options.persistence;
    let sandbox: Sandbox;
    if (options.snapshotId != null) {
      if (
        options.cpu !== undefined ||
        options.memory !== undefined ||
        options.storage !== undefined
      )
        throw new TypeError("cpu/memory/storage are fixed by the snapshot and cannot be changed");
      sandbox = await client.snapshots.restore(options.snapshotId, {
        name: options.name,
        env: options.env,
        workdir: options.workdir,
        tags: options.tags,
        timeout_secs: 0,
        block_network: false,
        idle_timeout_secs: idleTimeout,
        activity_threshold_bytes: options.activityThresholdBytes ?? undefined,
        persistence,
      });
    } else {
      const request: SandboxCreateRequest = {
        name: options.name,
        image: options.image,
        template: options.template,
        env: options.env,
        workdir: options.workdir,
        tags: options.tags,
        timeout_secs: 0,
        block_network: false,
        idle_timeout_secs: idleTimeout,
        activity_threshold_bytes: options.activityThresholdBytes ?? undefined,
        persistence,
        nics: options.nics?.map((nic) => ({
          vpc: nic.vpc,
          ipv4: nic.ipv4,
          default: nic.default ?? true,
        })),
      };
      if (options.cpu !== undefined) request.cpus = options.cpu;
      if (options.memory !== undefined) request.memory = options.memory * 1024;
      if (options.storage !== undefined) request.disk_mb = options.storage * 1024;
      sandbox = await client.sandboxes.create(request);
    }
    const vm = new Vm(client, sandbox);
    // vmon has no domain-routing service; VMs never come with public hostnames.
    return { vm, vmId: vm.vmId, domains: [] };
  }
  /** List every reachable VM with lifecycle metadata and state counts. */
  async list(): Promise<{
    vms: VmListEntry[];
    totalCount: number;
    runningCount: number;
    startingCount: number;
    suspendedCount: number;
    stoppedCount: number;
  }> {
    const sandboxes = await this.#freestyle.client.sandboxes.list();
    const vms = sandboxes.map((sandbox) => {
      const info = sandbox.info;
      return {
        id: sandbox.id,
        state: vmState(info),
        createdAt: unixToIso(info.created_at),
        lastNetworkActivity: unixToIso(info.last_network_active ?? info.last_active),
        snapshotId: null,
        deleted: false,
      };
    });
    const counts = { running: 0, starting: 0, suspended: 0, stopped: 0 };
    for (const row of vms) {
      if (row.state === "running") counts.running += 1;
      else if (row.state === "starting") counts.starting += 1;
      else if (row.state === "suspended" || row.state === "suspending") counts.suspended += 1;
      else counts.stopped += 1;
    }
    return {
      vms,
      totalCount: vms.length,
      runningCount: counts.running,
      startingCount: counts.starting,
      suspendedCount: counts.suspended,
      stoppedCount: counts.stopped,
    };
  }
  /** Reconnect to a VM by identifier. */
  async get(options: { vmId: string }): Promise<{ vm: Vm }> {
    const client = this.#freestyle.client;
    return { vm: new Vm(client, await client.sandboxes.get(options.vmId)) };
  }
  /** Create an unfetched VM reference. */
  ref(options: { vmId: string }): Vm {
    const client = this.#freestyle.client;
    return new Vm(client, client.sandboxes.ref(options.vmId));
  }
  /** Permanently remove a VM; already-deleted VMs are ignored. */
  delete(options: { vmId: string }): Promise<unknown> {
    return deleteSandbox(this.#freestyle.client.sandboxes.ref(options.vmId));
  }
}

/** One Freestyle-shaped VM handle bound to a vmon sandbox. */
export class Vm {
  /** Interactive PTY sessions. */
  readonly pty: VmPty;
  /** Guest filesystem operations. */
  readonly fs: VmFs;
  /** Underlying vmon sandbox; escape hatch to the full SDK surface. */
  readonly sandbox: Sandbox;
  readonly #client: Client;
  /** Wrap an existing sandbox in the Freestyle-shaped API. */
  constructor(client: Client, sandbox: Sandbox) {
    this.#client = client;
    this.sandbox = sandbox;
    this.fs = new VmFs(sandbox);
    this.pty = new VmPty(sandbox);
  }
  /** Stable VM identifier. */
  get vmId(): string {
    return this.sandbox.id;
  }

  /** Wake or fresh-boot this VM; `null` disables its network-idle policy. */
  async start(options: { idleTimeoutSeconds?: number | null } = {}): Promise<unknown> {
    if (options.idleTimeoutSeconds !== undefined)
      await this.sandbox.setIdleTimeout(options.idleTimeoutSeconds ?? 0);
    return this.sandbox.resume();
  }
  /** Stop execution while retaining the VM disk and identity for a later fresh boot. */
  async stop(): Promise<SandboxInfo> {
    return this.sandbox.stop();
  }
  /**
   * Capture a full VM snapshot usable with `vms.create({ snapshotId })`.
   * Restoring it resumes the captured machine state (processes and memory);
   * it is not a disk-only template.
   */
  async snapshot(
    options: { name?: string | null } = {},
  ): Promise<{ snapshotId: string; sourceVmId: string }> {
    const snapshotId = await this.sandbox.snapshot(options.name ?? undefined);
    return { snapshotId, sourceVmId: this.vmId };
  }
  /** Fork the VM into `count` copies of its current state. */
  async fork(options: { count: number }): Promise<{ forks: { vm: Vm; vmId: string }[] }> {
    const snapshot = await this.sandbox.snapshot();
    const clones = await this.#client.snapshots.fork(snapshot, { count: options.count });
    return {
      forks: clones.map((clone) => ({ vm: new Vm(this.#client, clone), vmId: clone.id })),
    };
  }
  /** Terminate and permanently remove this VM. */
  delete(): Promise<unknown> {
    return deleteSandbox(this.sandbox);
  }
  /** Run a command through `sh -c`, or as a sibling in a persistent terminal context. */
  async exec(
    options: string | { command: string; terminal?: string; timeoutMs?: number },
  ): Promise<VmExecResult> {
    const request = typeof options === "string" ? { command: options } : options;
    if (request.terminal !== undefined) {
      const result = await this.sandbox.ptyExec(
        request.terminal,
        request.command,
        request.timeoutMs,
      );
      return {
        stdout: new TextDecoder().decode(result.stdout),
        stderr: new TextDecoder().decode(result.stderr),
        statusCode: result.code,
      };
    }
    const result = await this.sandbox.run(["sh", "-c", request.command], {
      timeout: request.timeoutMs === undefined ? undefined : request.timeoutMs / 1000,
    });
    return {
      stdout: decodeBase64Text(result.stdout_b64),
      stderr: decodeBase64Text(result.stderr_b64),
      statusCode: result.exit,
    };
  }
  /**
   * Resize CPU, memory, or grow the root disk. Freestyle requires CPU and
   * memory sizes to be powers of two.
   */
  async resize(options: { cpu?: number; memory?: number; storage?: number }): Promise<SandboxInfo> {
    if (
      options.cpu !== undefined &&
      (!Number.isInteger(options.cpu) ||
        !(options.cpu > 0) ||
        !Number.isInteger(Math.log2(options.cpu)))
    )
      throw new TypeError("cpu must be a power of two");
    if (
      options.memory !== undefined &&
      (!(options.memory > 0) || !Number.isInteger(Math.log2(options.memory)))
    )
      throw new TypeError("memory must be a power of two");
    return this.sandbox.resize({
      cpus: options.cpu,
      memoryMib: options.memory === undefined ? undefined : options.memory * 1024,
      diskMb: options.storage === undefined ? undefined : options.storage * 1024,
    });
  }
}

/** Freestyle-shaped guest filesystem bound to one VM. */
export class VmFs {
  readonly #sandbox: Sandbox;
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  /** Read raw guest file bytes. */
  readFile(path: string): Promise<Uint8Array> {
    return this.#sandbox.files.read(path);
  }
  /** Write raw guest file content. */
  writeFile(path: string, content: Uint8Array | string): Promise<void> {
    // Copy into an ArrayBuffer-backed view so SharedArrayBuffer-backed inputs satisfy BodyInit.
    return this.#sandbox.files.write(
      path,
      typeof content === "string" ? content : new Uint8Array(content),
    );
  }
  /** Read a UTF-8 guest file. */
  readTextFile(path: string): Promise<string> {
    return this.#sandbox.files.readText(path);
  }
  /** Write UTF-8 guest file content. */
  writeTextFile(path: string, content: string): Promise<void> {
    return this.#sandbox.files.writeText(path, content);
  }
  /** List guest directory entries; `kind` is `file`, `dir`, `symlink`, or `other`. */
  async readDir(path: string): Promise<{ name: string; kind: string }[]> {
    const entries = await this.#sandbox.files.list(path);
    return entries.map((entry) => ({ name: entry.name ?? "", kind: entry.type ?? "other" }));
  }
  /** Create a guest directory and its parents. */
  mkdir(path: string): Promise<void> {
    return this.#sandbox.files.mkdir(path);
  }
  /** Delete a guest path; pass `recursive` (vmon extension) for non-empty trees. */
  remove(path: string, options: { recursive?: boolean } = {}): Promise<void> {
    return this.#sandbox.files.delete(path, options.recursive ?? false);
  }
  /**
   * Report whether a guest path exists.
   *
   * # Errors
   * Rethrows every failure other than `not_found` (auth, engine, transport) —
   * a permission error is not the same as a missing path.
   */
  async exists(path: string): Promise<boolean> {
    try {
      await this.#sandbox.files.stat(path);
      return true;
    } catch (error) {
      if (error instanceof APIError && (error.code === "not_found" || error.status === 404))
        return false;
      throw error;
    }
  }
  /** Stat a guest path; the vmon agent reports no `owner`/`group`. */
  async stat(path: string): Promise<{
    size: number;
    isFile: boolean;
    isDirectory: boolean;
    isSymlink: boolean;
    permissions: string;
    owner?: string;
    group?: string;
    modified: string;
  }> {
    const info = await this.#sandbox.files.stat(path);
    return {
      size: info.size ?? 0,
      isFile: info.type === "file",
      isDirectory: info.type === "dir",
      isSymlink: info.type === "symlink",
      permissions: ((info.mode ?? 0) & 0o7777).toString(8).padStart(3, "0"),
      modified: unixToIso(info.mtime) ?? "",
    };
  }
}

/** Session metadata reported by [`VmPty.list`]. */
export interface PtySessionInfo {
  sessionId: string;
  running: boolean;
  exitCode?: number | null;
  cols: number;
  rows: number;
  exec?: string | null;
  createdAtMs: number;
  attachedCount: number;
  suspended: boolean;
}

/** Options accepted by [`VmPty.open`]. */
export interface PtyOpenOptions {
  cols?: number;
  rows?: number;
  exec?: string;
  env?: Record<string, string>;
  workdir?: string;
  sessionId?: string;
}

/** Output and lifecycle callbacks for a PTY session. */
export interface PtySessionEvents {
  onData?: (data: Uint8Array) => void;
  onExit?: (exitCode: number) => void;
  onClose?: (info: { wasClean: boolean; code: number; reason: string }) => void;
  onError?: (err: unknown) => void;
}

export interface PtyReconnectOptions {
  enabled?: boolean;
  maxAttempts?: number;
  baseDelayMs?: number;
  onReconnecting?: (attempt: number) => void;
  onReconnect?: () => void;
}

/** One attachment to a server-persistent PTY session. */
export class PtySession {
  readonly sessionId: string;
  readonly #sandbox: Sandbox;
  readonly #events: PtySessionEvents;
  readonly #reconnect: PtyReconnectOptions;
  #stream: PtyStream;
  #cols: number;
  #rows: number;
  #exec: string | null;
  #createdAtMs: number;
  #exitCode: number | null = null;
  #closed = false;
  #reconnecting = false;
  readonly #queued: (Uint8Array | string)[] = [];
  constructor(
    sandbox: Sandbox,
    stream: PtyStream,
    info: PtySessionInfo,
    events: PtySessionEvents,
    reconnect: PtyReconnectOptions = {},
  ) {
    this.#sandbox = sandbox;
    this.#stream = stream;
    this.#events = events;
    this.#reconnect = reconnect;
    this.sessionId = info.sessionId;
    this.#cols = info.cols;
    this.#rows = info.rows;
    this.#exec = info.exec ?? null;
    this.#createdAtMs = info.createdAtMs;
  }
  get readyState(): number {
    return this.#closed ? 3 : this.#reconnecting ? 0 : 1;
  }
  write(data: Uint8Array | string): void {
    if (this.#closed) throw new Error("PTY session has ended");
    if (this.#reconnecting) this.#queued.push(data);
    else this.#stream.write(data);
  }
  resize(options: { cols: number; rows: number }): void {
    this.#cols = options.cols;
    this.#rows = options.rows;
    if (!this.#reconnecting) this.#stream.resize(options.rows, options.cols);
  }
  signal(sig: "SIGINT" | "SIGKILL"): void {
    if (sig === "SIGINT") this.write(new Uint8Array([0x03]));
    else void this.#sandbox.ptyClose(this.sessionId);
  }
  /** Disconnect this client stream while leaving the guest session running. */
  detach(): void {
    this.#closed = true;
    this.#stream.detach();
    this.#events.onClose?.({ wasClean: true, code: 1000, reason: "detached" });
  }
  info(): PtySessionInfo {
    return {
      sessionId: this.sessionId,
      running: !this.#closed,
      exitCode: this.#exitCode,
      cols: this.#cols,
      rows: this.#rows,
      exec: this.#exec,
      createdAtMs: this.#createdAtMs,
      attachedCount: this.#closed ? 0 : 1,
      suspended: false,
    };
  }
  /** Handle transport failure, reattaching with bounded exponential backoff. */
  async reconnectAfter(error: Error): Promise<void> {
    if (this.#reconnect.enabled !== true) {
      this.#events.onError?.(error);
      return;
    }
    this.#reconnecting = true;
    const maximum = this.#reconnect.maxAttempts ?? 5;
    const base = this.#reconnect.baseDelayMs ?? 100;
    for (let attempt = 1; attempt <= maximum; attempt += 1) {
      this.#reconnect.onReconnecting?.(attempt);
      if (attempt > 1)
        await new Promise<void>((resolve) => setTimeout(resolve, base * 2 ** (attempt - 2)));
      try {
        this.#stream = await this.#sandbox.ptyAttach(this.sessionId, {
          cols: this.#cols,
          rows: this.#rows,
          onData: this.#events.onData,
          onClose: () => this.observeClose(),
          onExit: (code) => this.#observeExit(code),
          onError: (next) => void this.reconnectAfter(next),
        });
        this.#reconnecting = false;
        for (const data of this.#queued.splice(0)) this.#stream.write(data);
        this.#reconnect.onReconnect?.();
        return;
      } catch (next) {
        if (attempt === maximum) {
          this.#reconnecting = false;
          this.#events.onError?.(next);
        }
      }
    }
  }
  observeClose(): void {
    if (this.#closed || this.#reconnecting) return;
    this.#events.onClose?.({ wasClean: true, code: 1000, reason: "detached" });
  }
  observeExit(code: number): void {
    this.#observeExit(code);
  }
  #observeExit(code: number): void {
    this.#closed = true;
    this.#exitCode = code;
    this.#events.onExit?.(code);
    this.#events.onClose?.({ wasClean: true, code: 1000, reason: "exited" });
  }
}

/** Interactive PTY operations bound to one VM. */
export class VmPty {
  readonly #sandbox: Sandbox;
  constructor(sandbox: Sandbox) {
    this.#sandbox = sandbox;
  }
  async open(
    options: PtyOpenOptions & PtySessionEvents & { reconnect?: PtyReconnectOptions } = {},
  ): Promise<PtySession> {
    let session: PtySession;
    const stream = await this.#sandbox.ptyOpen({
      onClose: () => session.observeClose(),
      sessionId: options.sessionId,
      cols: options.cols,
      rows: options.rows,
      exec: options.exec,
      env: options.env,
      workdir: options.workdir,
      onData: options.onData,
      onExit: (code) => session.observeExit(code),
      onError: (error) => void session.reconnectAfter(error),
    });
    const meta = await stream.meta;
    session = new PtySession(this.#sandbox, stream, ptyInfo(meta), options, options.reconnect);
    return session;
  }
  async attach(
    options: {
      sessionId: string;
      cols?: number;
      rows?: number;
      reconnect?: PtyReconnectOptions;
    } & PtySessionEvents,
  ): Promise<PtySession> {
    let session: PtySession;
    const stream = await this.#sandbox.ptyAttach(options.sessionId, {
      cols: options.cols,
      rows: options.rows,
      onData: options.onData,
      onClose: () => session.observeClose(),
      onExit: (code) => session.observeExit(code),
      onError: (error) => void session.reconnectAfter(error),
    });
    const meta = await stream.meta;
    session = new PtySession(this.#sandbox, stream, ptyInfo(meta), options, options.reconnect);
    return session;
  }
  async list(): Promise<{ sessions: PtySessionInfo[] }> {
    return { sessions: (await this.#sandbox.ptyList()).map(ptyInfo) };
  }
  async close(options: { sessionId: string }): Promise<{
    sessionId: string;
    exitCode?: number | null;
  }> {
    const closed = await this.#sandbox.ptyClose(options.sessionId);
    return {
      sessionId: closed.sessionId,
      exitCode: closed.exitCode === undefined ? null : Number(closed.exitCode),
    };
  }
}

function ptyInfo(session: ProtoPtySession): PtySessionInfo {
  return {
    sessionId: session.sessionId,
    running: session.running,
    exitCode: session.exitCode === undefined ? null : Number(session.exitCode),
    cols: session.cols,
    rows: session.rows,
    exec: session.exec ?? null,
    createdAtMs: Number(session.createdAtUnixMillis),
    attachedCount: session.attachedCount,
    suspended: session.suspended,
  };
}

/** VPC identity returned by the Freestyle-compatible namespace. */
export interface FreestyleVpc {
  vpcId: string;
}

/** Freestyle-shaped VPC operations backed by vmon's routed VPC service. */
export class VpcNamespaceFacade {
  readonly #freestyle: Freestyle;
  constructor(freestyle: Freestyle) {
    this.#freestyle = freestyle;
  }
  /** Create a routed VPC. */
  async create(options: { cidr?: string; name?: string } = {}): Promise<{
    vpcId: string;
    vpc: FreestyleVpc;
  }> {
    const created = await this.#freestyle.client.vpcs.create(options);
    return { vpcId: created.id, vpc: { vpcId: created.id } };
  }
  /** List routed VPCs (vmon extension). */
  async list(): Promise<{ vpcs: (FreestyleVpc & { name: string; cidr: string })[] }> {
    const vpcs = await this.#freestyle.client.vpcs.list();
    return {
      vpcs: vpcs.map((vpc) => ({ vpcId: vpc.id, name: vpc.name, cidr: vpc.cidr })),
    };
  }
  /** Delete an unattached routed VPC (vmon extension). */
  async delete(options: { vpcId: string }): Promise<void> {
    await this.#freestyle.client.vpcs.delete(options.vpcId);
  }
}

/** Freestyle-shaped root client backed by a lazily connected vmon [`Client`]. */
export class Freestyle {
  /** VM collection operations. */
  readonly vms: VmsNamespace;
  /** Routed VPC operations. */
  readonly vpc: VpcNamespaceFacade;
  #client: Client | null = null;
  readonly #dsn?: string;
  readonly #options: ConnectOptions;
  /**
   * Bind to an existing vmon [`Client`], or defer connection until first use.
   * With no options, the DSN resolves from `VMON_DSN`/`VMON_CONTEXT`.
   */
  constructor(options: FreestyleOptions | Client = {}) {
    if (options instanceof Client) {
      this.#client = options;
      this.#options = {};
    } else {
      this.#dsn = options.baseUrl;
      this.#options = { token: options.accessToken ?? options.apiKey, fetch: options.fetch };
    }
    this.vms = new VmsNamespace(this);
    this.vpc = new VpcNamespaceFacade(this);
  }
  /** Underlying vmon client, connected on first access. */
  get client(): Client {
    this.#client ??= connect(this.#dsn, this.#options);
    return this.#client;
  }
  /** Close the underlying vmon client if one was ever connected. */
  close(): void | Promise<void> {
    return this.#client?.close();
  }
}

/** Default client; connects from the environment on first use. */
export const freestyle = new Freestyle();

/** Permanently remove a sandbox, tolerating already-deleted VMs. */
async function deleteSandbox(sandbox: Sandbox): Promise<void> {
  try {
    await sandbox.remove();
  } catch (error) {
    if (error instanceof APIError && (error.code === "not_found" || error.status === 404)) return;
    throw error;
  }
}

/** Map a vmon sandbox view onto Freestyle's VM state vocabulary. */
function vmState(info: SandboxInfo): VmState {
  const state = typeof info.observed_state === "string" ? info.observed_state : info.status;
  switch (state) {
    case "running":
      return "running";
    case "starting":
    case "booting":
    case "creating":
    case "pending":
      return "starting";
    case "suspending":
      return "suspending";
    case "suspended":
    case "paused":
      return "suspended";
    case "lost":
      return "lost";
    case "building":
      return "building";
    default:
      return "stopped";
  }
}

function unixToIso(value: unknown): string | null {
  return typeof value === "number" ? new Date(value * 1000).toISOString() : null;
}

function decodeBase64Text(value: string): string {
  const raw = atob(value);
  const bytes = new Uint8Array(raw.length);
  for (let index = 0; index < raw.length; index += 1) bytes[index] = raw.charCodeAt(index);
  return new TextDecoder().decode(bytes);
}
