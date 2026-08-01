import type { Driver, DriverOptions, DriverRequestOptions } from "./driver";
import { MeshDriver } from "./driver";
import { APIError, apiError, ProtocolError, parseResponseJson, TransportError } from "./errors";
import type { FunctionLookupOptions, FunctionValueAdapter } from "./functions";
import { App, RemoteFunction } from "./functions";
import {
  PoolService,
  SandboxService,
  SnapshotService,
  SystemService,
  VolumeService,
  VpcService,
} from "./gen/vmon/v1/api_pb";
import type {
  ForkRequest,
  Health,
  MeshNode,
  MeshStatus,
  PoolSetRequest,
  PoolStats,
  RestoreRequest,
  SandboxCreateRequest,
  SandboxInfo,
  ServerInfo,
} from "./models";
import { EventStream, Process } from "./process";
import type { SandboxListOptions } from "./sandbox";
import { Sandbox, sandboxFromRoute } from "./sandbox";
import type { SecretInput } from "./values";
import { secretWires, Volume } from "./values";

/** Options accepted by the synchronous connect factory. */
export interface ConnectOptions extends DriverOptions {}
/** Sandbox creation body with typed secret value objects. */
export type SandboxCreateRequestWithSecrets = Omit<SandboxCreateRequest, "secrets"> & {
  secrets?: Iterable<SecretInput> | null;
};

/** Root SDK object exposing service namespaces and daemon-wide operations. */
export class Client {
  readonly driver: Driver;
  readonly sandboxes: SandboxAPI;
  readonly snapshots: SnapshotAPI;
  readonly volumes: VolumeAPI;
  readonly pools: PoolAPI;
  readonly mesh: MeshAPI;
  readonly vpcs: VpcAPI;
  readonly functions: FunctionAPI;
  readonly apps: AppAPI;
  /** Bind the client to a driver implementation. */
  constructor(driver: Driver) {
    this.driver = driver;
    this.sandboxes = new SandboxAPI(this);
    this.snapshots = new SnapshotAPI(this);
    this.volumes = new VolumeAPI(this);
    this.pools = new PoolAPI(this);
    this.vpcs = new VpcAPI(this);
    this.mesh = new MeshAPI(this);
    this.functions = new FunctionAPI(this);
    this.apps = new AppAPI(this);
  }
  /** Check daemon health. */
  async health(): Promise<Health> {
    const value = parseResponseJson(await (await this.response("GET", "/healthz")).text());
    if (!isHealth(value)) throw new ProtocolError("health response must include a boolean ok");
    return value;
  }
  /** Fetch daemon and node information. */
  async info(): Promise<ServerInfo> {
    const { message } = await this.driver.call(SystemService.method.info, {});
    const value = parseResponseJson(message.json);
    if (!isServerInfo(value)) throw new ProtocolError("server info response is malformed");
    return value;
  }
  /** Fetch Prometheus metrics text. */
  async metrics(): Promise<string> {
    return (await this.response("GET", "/metrics")).text();
  }
  /** Open the daemon event stream. */
  async events(): Promise<EventStream> {
    return new EventStream(await this.driver.serverStream(SystemService.method.events, {}));
  }
  /** Open an interactive shell process after its ready frame. */
  async shell(request: Record<string, unknown> = {}): Promise<Process> {
    const process = new Process(
      (inputs) => this.driver.duplex(SandboxService.method.shell, inputs),
      { case: "shellParamsJson", value: JSON.stringify(request) },
    );
    await process.ready();
    return process;
  }
  /** Close the underlying driver. */
  close(): void | Promise<void> {
    return this.driver.close();
  }
  /** Perform an authenticated request and reject API errors. */
  async response(method: string, path: string, options: DriverRequestOptions = {}) {
    const response = await this.driver.request(method, path, options);
    if (!response.ok) throw await apiError(response);
    return response;
  }
}

/** Deployed function lookup operations. */
export class FunctionAPI {
  readonly #client: Client;
  /** Bind deployed-function lookups to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Resolve and pin the active deployed function revision. */
  fromName<Input, Result>(
    name: string,
    options: FunctionLookupOptions<Input, Result> & {
      input: FunctionValueAdapter<Input>;
      result: FunctionValueAdapter<Result>;
    },
  ): Promise<RemoteFunction<Input, Result>>;
  fromName(name: string, options?: FunctionLookupOptions): Promise<RemoteFunction>;
  fromName<Input, Result>(
    name: string,
    options: FunctionLookupOptions<Input, Result> = {},
  ): Promise<RemoteFunction<Input, Result> | RemoteFunction> {
    if (options.input && options.result) {
      return RemoteFunction.fromName(this.#client, name, {
        ...options,
        input: options.input,
        result: options.result,
      });
    }
    return RemoteFunction.fromName(this.#client, name, {
      namespace: options.namespace,
      revisionId: options.revisionId,
      serializer: options.serializer,
      compression: options.compression,
      inlineLimit: options.inlineLimit,
      typeName: options.typeName,
      labels: options.labels,
      resultTtlMillis: options.resultTtlMillis,
      signal: options.signal,
    });
  }
}

/** Deployed application lookup operations. */
export class AppAPI {
  readonly #client: Client;
  /** Bind deployed-application lookups to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Resolve and pin the current deployed application revision. */
  fromName(name: string, options: { namespace?: string; revisionId?: string } = {}): Promise<App> {
    return App.fromName(this.#client, name, options);
  }
}

/** Create a client backed by a lazily discovering mesh driver. */
export function connect(dsn?: string, options: ConnectOptions = {}): Client {
  return new Client(new MeshDriver(dsn, options));
}

/** Sandbox collection operations. */
export class SandboxAPI {
  readonly #client: Client;
  /** Bind sandbox operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Create a sandbox. */
  async create(request: SandboxCreateRequestWithSecrets): Promise<Sandbox> {
    const { secrets, ...rest } = request;
    const body: SandboxCreateRequest =
      secrets === undefined
        ? rest
        : { ...rest, secrets: secrets === null ? null : secretWires(secrets) };
    const { message, endpoint } = await this.#client.driver.call(SandboxService.method.create, {
      specJson: JSON.stringify(body),
    });
    return sandboxFromRoute(this.#client, sandboxInfo(parseResponseJson(message.json)), endpoint);
  }
  /** Reconnect to a sandbox by stable ID and pin its current endpoint. */
  async get(id: string): Promise<Sandbox> {
    const getAt = async (endpoint?: string) => {
      const result = await this.#client.driver.call(
        SandboxService.method.get,
        { id },
        { endpoint },
      );
      return sandboxFromRoute(
        this.#client,
        sandboxInfo(parseResponseJson(result.message.json)),
        result.endpoint,
      );
    };
    try {
      return await getAt();
    } catch (error) {
      if (
        !(error instanceof APIError) ||
        (error.code !== "not_found" && error.status !== 404) ||
        this.#client.driver.endpoints().length <= 1
      )
        throw error;
    }
    return getAt(await this.#client.driver.resolveSandbox(id));
  }
  /** Create an unfetched bound sandbox reference. */
  ref(id: string): Sandbox {
    return new Sandbox(this.#client, id);
  }
  /** List and merge sandboxes across live mesh endpoints. */
  async list(options: SandboxListOptions = {}): Promise<Sandbox[]> {
    const endpoints = this.#client.driver.endpoints().filter((entry) => entry.healthy);
    const tags: string[] = [];
    if (options.tags) for (const key in options.tags) tags.push(`${key}=${options.tags[key]}`);
    if (endpoints.length <= 1) {
      const { message, endpoint } = await this.#client.driver.call(SandboxService.method.list, {
        tags,
      });
      return sandboxRows(message.sandboxesJson.map(parseResponseJson)).map((row) =>
        sandboxFromRoute(this.#client, row, endpoint),
      );
    }
    const attempts = await Promise.allSettled(
      endpoints.map(async (entry) => {
        const { message, endpoint } = await this.#client.driver.call(
          SandboxService.method.list,
          { tags },
          { endpoint: entry.url },
        );
        return {
          rows: sandboxRows(message.sandboxesJson.map(parseResponseJson)),
          endpoint,
        };
      }),
    );
    const merged = new Map<string, Sandbox>();
    let lastTransportError: TransportError | undefined;
    for (const attempt of attempts) {
      if (attempt.status === "rejected") {
        if (!(attempt.reason instanceof TransportError)) throw attempt.reason;
        lastTransportError = attempt.reason;
        continue;
      }
      for (const row of attempt.value.rows)
        if (!merged.has(row.id))
          merged.set(row.id, sandboxFromRoute(this.#client, row, attempt.value.endpoint));
    }
    if (attempts.every((attempt) => attempt.status === "rejected")) throw lastTransportError;
    return [...merged.values()];
  }
}

/** Snapshot collection, restore, and fork operations. */
export class SnapshotAPI {
  readonly #client: Client;
  /** Bind snapshot operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List snapshot names. */
  async list(): Promise<string[]> {
    return (await this.#client.driver.call(SnapshotService.method.list, {})).message.snapshots;
  }

  /** Permanently delete one named snapshot. */
  async delete(name: string): Promise<void> {
    await this.#client.driver.call(SnapshotService.method.delete, { name });
  }
  /** Restore one snapshot into a sandbox. */
  async restore(name: string, request: RestoreRequest = {}): Promise<Sandbox> {
    const { message, endpoint } = await this.#client.driver.call(SnapshotService.method.restore, {
      name,
      bodyJson: JSON.stringify(request),
    });
    return sandboxFromRoute(this.#client, sandboxInfo(parseResponseJson(message.json)), endpoint);
  }
  /** Fork one snapshot into multiple sandboxes. */
  async fork(name: string, request: ForkRequest): Promise<Sandbox[]> {
    const { message, endpoint } = await this.#client.driver.call(SnapshotService.method.fork, {
      name,
      bodyJson: JSON.stringify(request),
    });
    const body = parseResponseJson(message.json);
    return sandboxRows(isRecord(body) ? body.clones : null).map((row) =>
      sandboxFromRoute(this.#client, row, endpoint),
    );
  }
}

/** Persistent volume collection operations. */
export class VolumeAPI {
  readonly #client: Client;
  /** Bind volume operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List persistent volumes. */
  async list(): Promise<Volume[]> {
    const { message } = await this.#client.driver.call(VolumeService.method.list, {});
    return message.volumes.map((name) => new Volume(name));
  }
  /** Create a persistent volume. */
  async create(name: string): Promise<Volume> {
    await this.#client.driver.call(VolumeService.method.create, { name });
    return new Volume(name);
  }
  /** Delete a persistent volume. */
  async delete(name: string): Promise<void> {
    await this.#client.driver.call(VolumeService.method.delete, { name });
  }
}

/** Private network returned by the VPC service. */
export interface Vpc {
  id: string;
  name: string;
  cidr: string;
  createdAtUnixMillis: bigint;
}

/** Private routed network operations. */
export class VpcAPI {
  readonly #client: Client;
  /** Bind VPC operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Create a private network. */
  async create(options: { name?: string; cidr?: string } = {}): Promise<Vpc> {
    return (
      await this.#client.driver.call(VpcService.method.create, {
        name: options.name ?? "",
        cidr: options.cidr ?? "",
      })
    ).message;
  }
  /** List private networks in creation order. */
  async list(): Promise<Vpc[]> {
    return (await this.#client.driver.call(VpcService.method.list, {})).message.vpcs;
  }
  /** Delete an unattached private network. */
  async delete(id: string): Promise<void> {
    await this.#client.driver.call(VpcService.method.delete, { id });
  }
}

/** Bound warm-pool value returned by pool operations. */
export class Pool {
  readonly #api: PoolAPI;
  readonly ref: string;
  readonly count: number;
  readonly stats?: PoolStats;
  /** Bind a pool result to its service. */
  constructor(api: PoolAPI, ref: string, count: number, stats?: PoolStats) {
    this.#api = api;
    this.ref = ref;
    this.count = count;
    this.stats = stats;
  }
  /** Delete this warm pool. */
  delete(): Promise<void> {
    return this.#api.delete(this.ref);
  }
}
/** Warm-pool collection operations. */
export class PoolAPI {
  readonly #client: Client;
  /** Bind pool operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** List warm pools. */
  async list(): Promise<Pool[]> {
    const body = parseResponseJson(
      (await this.#client.driver.call(PoolService.method.list, {})).message.json,
    );
    if (!isRecord(body) || Array.isArray(body))
      throw new ProtocolError("pool list response must be an object");
    const pools: Pool[] = [];
    for (const ref in body) {
      const stats = body[ref];
      if (!isRecord(stats) || Array.isArray(stats))
        throw new ProtocolError(`pool ${ref} statistics must be an object`);
      const count = Number(stats.count ?? stats.size ?? 0);
      if (!Number.isFinite(count)) throw new ProtocolError(`pool ${ref} size must be finite`);
      pools.push(new Pool(this, ref, count, stats));
    }
    return pools;
  }
  /** Set the desired warm-pool size and template. */
  async set(ref: string, count: number, template: Record<string, unknown> = {}): Promise<Pool> {
    const body: PoolSetRequest = { ...template, size: count };
    const { message } = await this.#client.driver.call(PoolService.method.set, {
      reference: ref,
      bodyJson: JSON.stringify(body),
    });
    const stats = parseResponseJson(message.json);
    if (!isRecord(stats) || Array.isArray(stats))
      throw new ProtocolError(`pool ${ref} statistics must be an object`);
    return new Pool(this, ref, count, stats);
  }
  /** Delete a warm pool. */
  async delete(ref: string): Promise<void> {
    await this.#client.driver.call(PoolService.method.delete, { reference: ref });
  }
  /** Delete every warm pool. */
  async clear(): Promise<void> {
    const pools = await this.list();
    await Promise.all(pools.map((pool) => this.delete(pool.ref)));
  }
}

/** Mesh status and node views. */
export class MeshAPI {
  readonly #client: Client;
  /** Bind mesh operations to a client. */
  constructor(client: Client) {
    this.#client = client;
  }
  /** Fetch typed mesh status. */
  async status(): Promise<MeshStatus> {
    const { message } = await this.#client.driver.call(SystemService.method.meshStatus, {});
    const value = parseResponseJson(message.json);
    if (!isMeshStatus(value)) throw new ProtocolError("mesh status response is malformed");
    return value;
  }
  /** Return the local and peer mesh nodes. */
  async nodes(): Promise<MeshNode[]> {
    const status = await this.status();
    return [status.self, ...status.peers];
  }
}

function sandboxRows(body: unknown): SandboxInfo[] {
  const rows = Array.isArray(body)
    ? body
    : isRecord(body) && Array.isArray(body.sandboxes)
      ? body.sandboxes
      : isRecord(body) && Array.isArray(body.items)
        ? body.items
        : null;
  if (rows === null || !rows.every(isSandboxInfo))
    throw new ProtocolError("sandbox list response is malformed");
  return rows;
}

function sandboxInfo(value: unknown): SandboxInfo {
  if (!isSandboxInfo(value)) throw new ProtocolError("sandbox response has no id");
  return value;
}

function isSandboxInfo(value: unknown): value is SandboxInfo {
  return isRecord(value) && typeof value.id === "string" && value.id.length > 0;
}

function isHealth(value: unknown): value is Health {
  return isRecord(value) && typeof value.ok === "boolean";
}

function isServerInfo(value: unknown): value is ServerInfo {
  if (
    !isRecord(value) ||
    typeof value.version !== "string" ||
    (value.platform !== undefined && typeof value.platform !== "string") ||
    (value.arch !== undefined && typeof value.arch !== "string") ||
    (value.backend !== undefined && typeof value.backend !== "string")
  )
    return false;
  if (value.capabilities === undefined) return true;
  if (!isRecord(value.capabilities) || Array.isArray(value.capabilities)) return false;
  for (const name in value.capabilities)
    if (typeof value.capabilities[name] !== "boolean") return false;
  return true;
}

function isMeshStatus(value: unknown): value is MeshStatus {
  return (
    isRecord(value) &&
    isMeshNode(value.self) &&
    Array.isArray(value.peers) &&
    value.peers.every(isMeshNode) &&
    typeof value.replicas_held === "number" &&
    Number.isFinite(value.replicas_held)
  );
}

function isMeshNode(value: unknown): value is MeshNode {
  return (
    isRecord(value) &&
    typeof value.node_id === "string" &&
    (value.advertise === undefined ||
      value.advertise === null ||
      typeof value.advertise === "string") &&
    (value.region === undefined || value.region === null || typeof value.region === "string")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
