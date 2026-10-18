// Same-origin client for the driver's admin and E2B REST APIs. Every call
// carries `X-Admin-Token`; team-scoped E2B routes also get `X-Team-ID`, which
// the driver accepts together with the admin token for any team.

const TOKEN_KEY = "vmon.admin-token";

/** Error returned by the API (`{code, message}` bodies) or the ingress proxy. */
export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

type AuthListener = (signedIn: boolean) => void;
const authListeners = new Set<AuthListener>();

/** Admin token from this tab (session) or, when remembered, the device. */
export function adminToken(): string {
  return sessionStorage.getItem(TOKEN_KEY) ?? localStorage.getItem(TOKEN_KEY) ?? "";
}

export function signIn(token: string, remember: boolean): void {
  sessionStorage.setItem(TOKEN_KEY, token);
  if (remember) localStorage.setItem(TOKEN_KEY, token);
  else localStorage.removeItem(TOKEN_KEY);
  for (const listener of authListeners) listener(true);
}

export function signOut(): void {
  sessionStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(TOKEN_KEY);
  for (const listener of authListeners) listener(false);
}

export function onAuthChange(listener: AuthListener): () => void {
  authListeners.add(listener);
  return () => authListeners.delete(listener);
}

/** Extracts a human message from an error response body. */
export async function errorFromResponse(res: Response): Promise<ApiError> {
  const text = await res.text().catch(() => "");
  let message = text.trim() || res.statusText || `HTTP ${res.status}`;
  try {
    const body = JSON.parse(text) as { message?: unknown; error?: { message?: unknown } };
    if (typeof body.message === "string") message = body.message;
    else if (typeof body.error?.message === "string") message = body.error.message;
  } catch {
    // Plain-text body (ingress errors): keep it as is.
  }
  return new ApiError(res.status, message);
}

interface RequestOptions {
  method?: string;
  team?: string;
  body?: unknown;
  signal?: AbortSignal;
}

async function call(path: string, options: RequestOptions = {}): Promise<Response> {
  const headers: Record<string, string> = { "X-Admin-Token": adminToken() };
  if (options.team) headers["X-Team-ID"] = options.team;
  if (options.body !== undefined) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method: options.method ?? "GET",
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    signal: options.signal,
    cache: "no-store",
  });
  if (!res.ok) {
    const error = await errorFromResponse(res);
    if (res.status === 401 && !options.team) signOut();
    throw error;
  }
  return res;
}

async function json<T>(path: string, options?: RequestOptions): Promise<T> {
  return (await call(path, options)).json() as Promise<T>;
}

async function empty(path: string, options?: RequestOptions): Promise<void> {
  await call(path, options);
}

// ------------------------------------------------------------------ types

export type MemberState = "alive" | "suspect" | "dead" | "left";
export type NodeStatus = "ready" | "draining" | "unhealthy" | "connecting" | "shutting_down";

export interface NodeInfo {
  status: NodeStatus;
  arch: string;
  os: string;
  backend: string;
  cpuModel: string;
  cpus: number;
  vcpusUsed: number;
  memoryMB: number;
  memoryUsedMB: number;
  diskFreeMB: number;
  sandboxes: number;
  builds: number;
  features: string[];
  cachedBuilds: number;
  createSuccesses: number;
  createFails: number;
  inflight: number;
  gpus: GpuInfo[];
}

/** One host GPU (or vGPU VF) a node publishes. */
export interface GpuInfo {
  /** PCI address, e.g. `0000:52:00.0`. */
  address: string;
  model: string;
  /** Sandbox holding it; null when free. */
  sandbox: string | null;
  /** Why sandboxes cannot use it; empty when assignable. */
  problem: string;
}

export interface RaftInfo {
  clusterId: string | null;
  leader: string | null;
  voter: boolean;
  term: number;
}

export interface ClusterMember {
  id: string;
  hostname: string;
  state: MemberState;
  roles: { node: boolean; driver: boolean };
  version: string;
  startedAt: string;
  /** Admin URL through the address the serving driver currently routes to. */
  clusterURL: string;
  /** Every advertised IP, in the order the serving driver holds them. */
  addrs: string[];
  publicURL: string | null;
  raft: RaftInfo | null;
  node: NodeInfo | null;
}

/** One 5 s fleet sample kept by the serving driver (1 h window). */
export interface FleetSample {
  ts: number;
  nodes: number;
  sandboxes: number;
  builds: number;
  vcpus: number;
  vcpusUsed: number;
  memoryMB: number;
  memoryUsedMB: number;
  starts: number;
}

export interface ClusterView {
  clusterID: string;
  localID: string;
  members: ClusterMember[];
  history: FleetSample[];
}

export interface TeamLimits {
  maxConcurrentSandboxes: number;
  maxConcurrentBuilds: number;
  maxVcpus: number;
  maxMemoryMB: number;
  maxDiskMB: number;
  maxLengthHours: number;
}

export interface TeamView {
  teamID: string;
  name: string;
  slug: string;
  blocked: boolean;
  system: boolean;
  createdAt: string;
  limits: TeamLimits;
  sandboxes: { running: number; paused: number };
}

export type SandboxState = "running" | "paused";

export interface SandboxRow {
  templateID: string;
  alias?: string | null;
  sandboxID: string;
  startedAt: string;
  endAt: string;
  cpuCount: number;
  memoryMB: number;
  diskSizeMB: number;
  metadata: Record<string, string>;
  state: SandboxState;
  envdVersion: string;
  teamID: string;
  teamName: string;
  nodeID: string;
}

export interface SandboxDetail {
  templateID: string;
  alias?: string | null;
  sandboxID: string;
  startedAt: string;
  endAt: string;
  envdVersion: string;
  envdAccessToken: string;
  allow_internet_access?: boolean | null;
  domain?: string | null;
  cpuCount: number;
  memoryMB: number;
  diskSizeMB: number;
  metadata: Record<string, string>;
  state: SandboxState;
  lifecycle: { autoResume: boolean; onTimeout: "pause" | "kill" };
}

export interface SandboxMetric {
  timestampUnix: number;
  cpuCount: number;
  cpuUsedPct: number;
  memUsed: number;
  memTotal: number;
  diskUsed: number;
  diskTotal: number;
}

export interface LogEntry {
  timestamp: string;
  message: string;
  level: string;
  fields: Record<string, string>;
}

export interface NodeSandbox {
  sandboxId: string;
  teamId: string;
  templateId: string;
  state: SandboxState;
  vcpus: number;
  memoryMb: number;
  startedAtMs: number;
  endAtMs: number;
}

export interface NodeDetail {
  id: string;
  sandboxes?: NodeSandbox[];
  cachedBuilds?: string[];
}

export interface Template {
  templateID: string;
  public: boolean;
  aliases: string[];
  names: string[];
  cpuCount: number;
  memoryMB: number;
  buildStatus: string;
}

export interface SandboxPage {
  rows: SandboxRow[];
  next: string | null;
}

export interface NewSandbox {
  templateID: string;
  timeout: number;
  autoPause: boolean;
  allow_internet_access: boolean;
  metadata?: Record<string, string>;
}

// ------------------------------------------------------------------ calls

const enc = encodeURIComponent;

export const api = {
  cluster: (signal?: AbortSignal) => json<ClusterView>("/admin/cluster", { signal }),
  teams: (signal?: AbortSignal) => json<TeamView[]>("/admin/teams", { signal }),

  async sandboxes(
    query: { id?: string; state?: SandboxState; team?: string; next?: string | null },
    signal?: AbortSignal,
  ): Promise<SandboxPage> {
    const params = new URLSearchParams({ limit: "500" });
    if (query.id) params.set("id", query.id);
    if (query.state) params.set("state", query.state);
    if (query.team) params.set("team", query.team);
    if (query.next) params.set("nextToken", query.next);
    const res = await call(`/admin/sandboxes?${params}`, { signal });
    return { rows: (await res.json()) as SandboxRow[], next: res.headers.get("x-next-token") };
  },

  sandbox: (team: string, id: string, signal?: AbortSignal) =>
    json<SandboxDetail>(`/sandboxes/${enc(id)}`, { team, signal }),
  kill: (team: string, id: string) => empty(`/sandboxes/${enc(id)}`, { team, method: "DELETE" }),
  pause: (team: string, id: string) =>
    empty(`/sandboxes/${enc(id)}/pause`, { team, method: "POST", body: {} }),
  /** Resumes a paused sandbox (connect = resume + set timeout). */
  resume: (team: string, id: string, timeout: number) =>
    empty(`/v2/sandboxes/${enc(id)}/connect`, { team, method: "POST", body: { timeout } }),
  setTimeout: (team: string, id: string, timeout: number) =>
    empty(`/sandboxes/${enc(id)}/timeout`, { team, method: "POST", body: { timeout } }),
  metrics: (team: string, id: string, startSecs: number, signal?: AbortSignal) =>
    json<SandboxMetric[]>(`/sandboxes/${enc(id)}/metrics?start=${startSecs}`, { team, signal }),
  logs: (
    team: string,
    id: string,
    query: { level?: string; search?: string },
    signal?: AbortSignal,
  ) => {
    const params = new URLSearchParams({ limit: "500", direction: "backward" });
    if (query.level) params.set("level", query.level);
    if (query.search) params.set("search", query.search);
    return json<{ logs: LogEntry[] }>(`/v2/sandboxes/${enc(id)}/logs?${params}`, {
      team,
      signal,
    });
  },
  create: (team: string, body: NewSandbox) =>
    json<{ sandboxID: string }>("/v2/sandboxes", { team, method: "POST", body }),
  templates: (team: string, signal?: AbortSignal) =>
    json<Template[]>("/templates", { team, signal }),

  node: (id: string, signal?: AbortSignal) => json<NodeDetail>(`/nodes/${enc(id)}`, { signal }),
  setNodeStatus: (id: string, status: "ready" | "draining" | "unhealthy") =>
    empty(`/nodes/${enc(id)}`, { method: "POST", body: { status } }),
  killTeamSandboxes: (team: string) =>
    json<{ killedCount: number; failedCount: number }>(`/admin/teams/${enc(team)}/sandboxes/kill`, {
      method: "POST",
    }),
};
