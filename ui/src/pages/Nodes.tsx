import { useCluster } from "../App.tsx";
import { api, type ClusterMember, type GpuInfo, type NodeStatus } from "../api.ts";
import {
  Card,
  confirmAction,
  Empty,
  ErrorNote,
  Menu,
  type MenuItem,
  Meter,
  Status,
  type Tone,
} from "../components.tsx";
import { fmt, memberName } from "../format.ts";
import { attempt } from "../hooks.ts";
import { Link } from "../router.tsx";

export const NODE_TONE: Record<NodeStatus, Tone> = {
  ready: "ok",
  draining: "warn",
  unhealthy: "error",
  connecting: "warn",
  shutting_down: "muted",
};

/** Free and assignable (no holder, no problem). */
export function gpuAvailable(gpu: GpuInfo): boolean {
  return gpu.sandbox === null && gpu.problem === "";
}

/** Operator status changes (`POST /nodes/{id}`), confirming the disruptive ones. */
export function nodeActions(member: ClusterMember, onChanged: () => void): MenuItem[] {
  const status = member.node?.status;
  const set = async (next: "ready" | "draining" | "unhealthy", label: string) => {
    if (next !== "ready") {
      const ok = await confirmAction({
        title: next === "draining" ? "Drain node?" : "Mark node unhealthy?",
        body: (
          <p>
            <strong>{memberName(member)}</strong>{" "}
            {next === "draining"
              ? "stops receiving new sandboxes and builds. Its sandboxes move to other compatible nodes, keeping their processes and files."
              : "is excluded from placement until marked ready again."}
          </p>
        ),
        action: label,
        danger: next === "unhealthy",
      });
      if (!ok) return;
    }
    if (
      await attempt(`${memberName(member)} is now ${next}`, () =>
        api.setNodeStatus(member.id, next),
      )
    )
      onChanged();
  };
  return [
    {
      label: "Mark ready",
      disabled: status === "ready",
      onSelect: () => void set("ready", "Mark Ready"),
    },
    {
      label: "Drain",
      disabled: status === "draining",
      onSelect: () => void set("draining", "Drain Node"),
    },
    {
      label: "Mark unhealthy",
      danger: true,
      disabled: status === "unhealthy",
      onSelect: () => void set("unhealthy", "Mark Unhealthy"),
    },
  ];
}

/** `free / total` GPUs of a node, noting the unassignable ones. */
function GpuCell({ gpus }: { gpus: GpuInfo[] }) {
  if (gpus.length === 0) return <td className="right num muted">—</td>;
  const broken = gpus.filter((g) => g.problem !== "").length;
  return (
    <td className="right num">
      {gpus.filter(gpuAvailable).length} / {gpus.length}
      {broken > 0 && (
        <div className="cell-sub" style={{ color: "var(--warn)" }}>
          {broken} unavailable
        </div>
      )}
    </td>
  );
}

export function NodesPage() {
  const cluster = useCluster();
  const nodes = (cluster.data?.members ?? [])
    .filter((m) => m.node)
    .sort((a, b) => memberName(a).localeCompare(memberName(b)) || a.id.localeCompare(b.id));
  const showGpus = nodes.some((m) => (m.node?.gpus.length ?? 0) > 0);

  return (
    <div className="page">
      <div className="page__head">
        <div>
          <h1 className="page__title">Nodes</h1>
          <p className="page__lede">Hosts running microVMs, as seen through gossip.</p>
        </div>
      </div>
      <ErrorNote error={cluster.error} />
      <Card flush>
        {nodes.length === 0 ? (
          <Empty title={cluster.loading ? "Loading…" : "No nodes"}>
            {!cluster.loading && "Start a process with the node role and join it to this cluster."}
          </Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th>Status</th>
                  <th>Platform</th>
                  <th className="right">Sandboxes</th>
                  <th style={{ width: 170 }}>vCPU</th>
                  <th style={{ width: 170 }}>Memory</th>
                  <th className="right">Disk free</th>
                  {showGpus && <th className="right">GPUs free</th>}
                  <th className="right">Creates</th>
                  <th className="shrink">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {nodes.map((m) => {
                  const n = m.node;
                  if (!n) return null;
                  return (
                    <tr key={m.id}>
                      <td>
                        <Link to={`/nodes/${m.id}`} className="row-link" title={m.id}>
                          {memberName(m)}
                        </Link>
                        <div className="cell-sub mono">{m.clusterURL}</div>
                      </td>
                      <td>
                        <Status tone={NODE_TONE[n.status]}>{n.status.replace("_", " ")}</Status>
                      </td>
                      <td className="muted">
                        {n.os}/{n.arch} · {n.backend}
                      </td>
                      <td className="right num">
                        {fmt.int(n.sandboxes)}
                        {n.builds > 0 && <div className="cell-sub">{n.builds} building</div>}
                      </td>
                      <td>
                        <span className="num">
                          {n.vcpusUsed} / {n.cpus}
                        </span>
                        <Meter label="vCPU allocation" used={n.vcpusUsed} total={n.cpus} />
                      </td>
                      <td>
                        <span className="num">
                          {fmt.mib(n.memoryUsedMB)} / {fmt.mib(n.memoryMB)}
                        </span>
                        <Meter label="Memory allocation" used={n.memoryUsedMB} total={n.memoryMB} />
                      </td>
                      <td className="right num muted">{fmt.mib(n.diskFreeMB)}</td>
                      {showGpus && <GpuCell gpus={n.gpus} />}
                      <td className="right num muted">
                        {fmt.int(n.createSuccesses)}
                        {n.createFails > 0 && (
                          <span style={{ color: "var(--error)" }}>
                            {" "}
                            · {fmt.int(n.createFails)} failed
                          </span>
                        )}
                      </td>
                      <td className="shrink">
                        <Menu
                          label={`Actions for node ${memberName(m)}`}
                          items={nodeActions(m, cluster.refresh)}
                        />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
