import { useCluster } from "../App.tsx";
import { api } from "../api.ts";
import {
  Badge,
  Card,
  CopyButton,
  Detail,
  Empty,
  ErrorNote,
  Menu,
  Meter,
  Stat,
  Status,
} from "../components.tsx";
import { fmt, memberName, shortId } from "../format.ts";
import { useNow, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link } from "../router.tsx";
import { gpuAvailable, NODE_TONE, nodeActions } from "./Nodes.tsx";

/** Host of a URL without IPv6 brackets ("" when unparsable). */
function urlHost(url: string): string {
  try {
    return new URL(url).hostname.replace(/^\[|\]$/g, "");
  } catch {
    return "";
  }
}

export function NodeDetailPage({ id }: { id: string }) {
  const cluster = useCluster();
  const member = cluster.data?.members.find((m) => m.id === id);
  const detail = usePoll(`node|${id}`, (signal) => api.node(id, signal), 5000);
  const now = useNow(5000);
  const n = member?.node;
  const route = member ? urlHost(member.clusterURL) : "";
  const sandboxes = [...(detail.data?.sandboxes ?? [])].sort(
    (a, b) => b.startedAtMs - a.startedAtMs,
  );

  return (
    <div className="page">
      <div className="page__head">
        <div style={{ minWidth: 0 }}>
          <div className="page__eyebrow">
            <Link to="/nodes">Nodes</Link>
            <Icon name="chevron" size={12} />
          </div>
          <h1 className={`page__title truncate${member ? "" : " mono"}`}>
            {member ? memberName(member) : shortId(id)}
          </h1>
          <p className="page__lede">
            {n ? (
              <Status tone={NODE_TONE[n.status]}>{n.status.replace("_", " ")}</Status>
            ) : (
              "Unknown node"
            )}
            {member && (
              <>
                <span className="faint"> · </span>
                <span className="mono" title={id}>
                  {shortId(id)}
                </span>
              </>
            )}
            {member && (
              <>
                <span className="faint"> · </span>
                <span className="mono">{member.clusterURL}</span>
                <span className="faint"> · v{member.version}</span>
              </>
            )}
          </p>
        </div>
        <div className="page__actions">
          <CopyButton value={id} label="Copy node ID" />
          {member && (
            <Menu
              label="Node actions"
              items={nodeActions(member, () => {
                cluster.refresh();
                detail.refresh();
              })}
            />
          )}
        </div>
      </div>
      <ErrorNote error={detail.error} />

      {n && (
        <div className="stats">
          <Stat
            label="Sandboxes"
            value={fmt.int(n.sandboxes)}
            sub={`${n.inflight} starting · ${n.builds} builds`}
          />
          <Stat
            label="vCPU allocated"
            value={
              <>
                {n.vcpusUsed}
                <small>/ {n.cpus}</small>
              </>
            }
            sub={<Meter label="vCPU allocation" used={n.vcpusUsed} total={n.cpus} />}
          />
          <Stat
            label="Memory allocated"
            value={fmt.mib(n.memoryUsedMB)}
            sub={
              <>
                of {fmt.mib(n.memoryMB)}
                <Meter label="Memory allocation" used={n.memoryUsedMB} total={n.memoryMB} />
              </>
            }
          />
          <Stat
            label="Disk free"
            value={fmt.mib(n.diskFreeMB)}
            sub={`${fmt.int(n.cachedBuilds)} cached builds`}
          />
          <Stat
            label="Creates"
            value={fmt.int(n.createSuccesses)}
            sub={
              n.createFails
                ? `${fmt.int(n.createFails)} failed (this driver)`
                : "No failures (this driver)"
            }
          />
        </div>
      )}

      <div className="stack">
        {n && member && (
          <Card title="Host">
            <dl className="kv">
              <Detail label="Platform">
                {n.os}/{n.arch}
              </Detail>
              <Detail label="Hypervisor">{n.backend}</Detail>
              <Detail label="CPU">{n.cpuModel || "—"}</Detail>
              <Detail label="Addresses">
                <span className="chips">
                  {member.addrs.length
                    ? member.addrs.map((addr) =>
                        addr === route ? (
                          <span key={addr} title={`Current route: ${member.clusterURL}`}>
                            <Badge tone="info">
                              <span className="mono">{addr}</span> route
                            </Badge>
                          </span>
                        ) : (
                          <Badge key={addr}>
                            <span className="mono">{addr}</span>
                          </Badge>
                        ),
                      )
                    : "—"}
                </span>
              </Detail>
              <Detail label="Up">{fmt.duration(now - new Date(member.startedAt).getTime())}</Detail>
              <Detail label="Roles">
                {[member.roles.driver && "driver", "node"].filter(Boolean).join(" + ")}
              </Detail>
              <Detail label="Features">
                <span className="chips">
                  {n.features.length ? n.features.map((f) => <Badge key={f}>{f}</Badge>) : "—"}
                </span>
              </Detail>
            </dl>
          </Card>
        )}

        {n && n.gpus.length > 0 && (
          <Card
            title="GPUs"
            subtitle={`${n.gpus.filter(gpuAvailable).length} of ${n.gpus.length} free`}
            flush
          >
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Model</th>
                    <th>PCI address</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  {n.gpus.map((g) => (
                    <tr key={g.address}>
                      <td>{g.model || "—"}</td>
                      <td className="mono muted">{g.address}</td>
                      <td>
                        {g.problem ? (
                          <Status tone="warn">{g.problem}</Status>
                        ) : g.sandbox ? (
                          <Status tone="live">
                            in use by{" "}
                            <Link to={`/sandboxes/${g.sandbox}`} className="row-link mono">
                              {g.sandbox}
                            </Link>
                          </Status>
                        ) : (
                          <Status tone="ok">free</Status>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        )}

        <Card title="Sandboxes on this node" subtitle="Reported by the node itself" flush>
          {sandboxes.length === 0 ? (
            <Empty title={detail.loading ? "Loading…" : "No sandboxes"} />
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Sandbox</th>
                    <th>State</th>
                    <th>Template</th>
                    <th className="right">Resources</th>
                    <th className="right">Started</th>
                    <th className="right">Ends</th>
                  </tr>
                </thead>
                <tbody>
                  {sandboxes.map((s) => (
                    <tr key={s.sandboxId}>
                      <td>
                        <Link to={`/sandboxes/${s.sandboxId}`} className="row-link mono">
                          {s.sandboxId}
                        </Link>
                      </td>
                      <td>
                        {s.state === "running" ? (
                          <Status tone="live">running</Status>
                        ) : (
                          <Status tone="muted">paused</Status>
                        )}
                      </td>
                      <td className="mono muted">{s.templateId}</td>
                      <td className="right num muted">
                        {s.vcpus} vCPU · {fmt.mib(s.memoryMb)}
                      </td>
                      <td className="right num muted">{fmt.ago(s.startedAtMs, now)}</td>
                      <td className="right num muted">
                        {s.state === "running" ? fmt.duration(s.endAtMs - now) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}
