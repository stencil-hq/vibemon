import type { JsonValue } from "./function-values";
import type { SecretWire } from "./values";

/** Daemon health response. */
export interface Health {
  ok: boolean;
}

/** S3 bucket or prefix mount accepted by sandbox creation. */
export interface S3MountSpec {
  uri: string;
  endpoint?: string;
  region?: string;
  read_only?: boolean;
  access_key?: string;
  secret_key?: string;
  session_token?: string;
}

/** Sandbox creation request accepted by the gRPC create operation. */
export interface SandboxCreateRequest {
  arch?: string | null;
  block_network?: boolean;
  command?: string[] | null;
  context?: string;
  cpus?: number;
  disk_mb?: number;
  dockerfile?: string | null;
  egress_allow?: string[] | null;
  egress_allow_domains?: string[] | null;
  env?: Record<string, string> | null;
  fs_dir?: string | null;
  ha?: string | null;
  /** Seconds without qualifying guest network activity before the idle action runs; 0 disables. */
  idle_timeout_secs?: number | null;
  /** Raw guest NIC byte delta per activity-sampling interval below which the VM counts idle. */
  activity_threshold_bytes?: number | null;
  /** Stored-state lifecycle: retention, storage-GC eviction priority, or discard on stop/suspend. */
  persistence?:
    | { type: "persistent" }
    | { type: "sticky"; priority?: number }
    | { type: "ephemeral" }
    | null;
  /** Single routed NIC attachment to a VPC (Linux hosts). */
  nics?: { vpc: string; ipv4: string | true; default?: boolean }[] | null;
  idempotency_key?: string | null;
  image?: string | null;
  inbound_cidr_allowlist?: string[] | null;
  memory?: number;
  name?: string | null;
  pool_size?: number;
  ports?: number[] | null;
  readiness_probe?: number | string | { port: number } | null;
  secrets?: SecretWire[] | null;
  /** Host-brokered credential names; credential values never enter this request. */
  credentials?: string[] | null;
  s3_mounts?: Record<string, S3MountSpec | string> | null;
  tags?: Record<string, string> | null;
  template?: string | null;
  timeout?: number | null;
  timeout_secs?: number | null;
  volumes?: Record<string, string | { name: string; read_only?: boolean }> | null;
  workdir?: string | null;
}

/** Captured or streaming exec request. */
export interface ExecRequest {
  cmd?: string[];
  env?: Record<string, string> | null;
  timeout?: number | null;
  tty?: boolean;
  workdir?: string | null;
}

/** Captured exec result. */
export interface ExecResult {
  exit: number;
  stderr_b64: string;
  stdout_b64: string;
}

/** Sandbox network policy. */
export interface NetworkPolicy {
  block_network?: boolean | null;
  cidr_allow?: string[] | null;
  domain_allow?: string[] | null;
}

/** Full VM snapshot request. */
export interface SnapshotRequest {
  name?: string | null;
  stop?: boolean;
}

/** Filesystem snapshot request. */
export interface SnapshotFilesystemRequest {
  name?: string | null;
}

/** Runtime-only fields that can change without altering captured VM devices. */
export interface SnapshotRuntimeOptions {
  agent?: boolean | null;
  /** Block or allow guest networking after restore. */
  block_network?: boolean | null;
  command?: string[] | null;
  env?: Record<string, string> | null;
  readiness_probe?: number | string | { port: number } | null;
  secrets?: SecretWire[] | null;
  s3_mounts?: Record<string, S3MountSpec | string> | null;
  tags?: Record<string, string> | null;
  timeout?: number | null;
  timeout_secs?: number | null;
  idle_timeout_secs?: number | null;
  activity_threshold_bytes?: number | null;
  persistence?:
    | { type: "persistent" }
    | { type: "sticky"; priority?: number }
    | { type: "ephemeral" };
  workdir?: string | null;
}

/** Snapshot restore request. */
export interface RestoreRequest extends SnapshotRuntimeOptions {
  name?: string | null;
}

/** Atomic snapshot fork request for 1 through 32 clones. */
export interface ForkRequest extends SnapshotRuntimeOptions {
  count: number;
}

/** An immutable retained `disk` or `checkpoint` recovery point. */
export interface RecoveryPoint {
  name: string;
  /** `disk` cold-boots; `checkpoint` restores VM execution state. */
  kind: string;
  created_at_unix_millis: bigint;
  size_bytes: bigint;
}

/** Warm-pool update request. */
export type PoolSetRequest = Partial<SandboxCreateRequest> & {
  size: number;
};

/** Tolerant sandbox view returned by the daemon. */
export interface SandboxInfo {
  id: string;
  name?: string | null;
  /** Serving status summary; use desired/observed state for lifecycle progress. */
  status?: string;
  desired_state?: string;
  observed_state?: string;
  state_generation?: number;
  lifecycle_failure?: string | null;
  pid?: number | null;
  source?: string | null;
  created_at?: number;
  last_active?: number;
  last_network_active?: number;
  expires_at?: number | null;
  terminated_at?: number | null;
  error?: string | null;
  tags?: Record<string, string> | null;
  returncode?: number | null;
  node?: string | null;
  ha?: string;
  restart_policy?: string;
  [key: string]: unknown;
}

/** Process exit status. */
export interface ExecExit {
  code: number;
  signal: number | null;
}

/** Guest filesystem entry metadata. */
export interface FileInfo {
  ok?: boolean;
  name?: string;
  type?: string;
  size?: number;
  mode?: number;
  mtime?: number;
  [key: string]: unknown;
}

/** One node advertised by mesh status. */
export interface MeshNode {
  node_id: string;
  advertise?: string | null;
  region?: string | null;
  [key: string]: unknown;
}

/** Typed mesh status response. */
export interface MeshStatus {
  self: MeshNode;
  peers: MeshNode[];
  replicas_held: number;
  [key: string]: unknown;
}

/** Warm-pool statistics. */
export interface PoolStats {
  size?: number;
  ready?: number;
  hits?: number;
  misses?: number;
  [key: string]: unknown;
}

/** Open sandbox runtime metrics keyed by subsystem or counter name. */
export interface SandboxMetrics {
  [key: string]: JsonValue;
}

/** Daemon build, host, and capability information. */
export interface ServerInfo {
  version: string;
  platform?: string;
  arch?: string;
  backend?: string;
  capabilities?: Record<string, boolean>;
  [key: string]: unknown;
}

/** Daemon-side target for one exposed guest port. */
export interface TunnelTarget {
  host: string;
  port: number;
  [key: string]: unknown;
}

/** Sandbox tunnels and proxy authorization token. */
export interface TunnelSet {
  connect_token?: string;
  tunnels?: Record<string, TunnelTarget>;
  [key: string]: unknown;
}

/** One daemon event payload. */
export interface EventRecord {
  [key: string]: JsonValue;
}
