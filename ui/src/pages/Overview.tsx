import { useState } from "react";
import { useCluster } from "../App.tsx";
import type { ClusterMember, FleetSample } from "../api.ts";
import { Chart } from "../chart.tsx";
import {
  Badge,
  Card,
  Empty,
  ErrorNote,
  Meter,
  Segmented,
  Stat,
  Status,
  type Tone,
} from "../components.tsx";
import { fmt, shortId } from "../format.ts";
import { useNow } from "../hooks.ts";
import { Link } from "../router.tsx";

type Range = "15m" | "1h";
const RANGE_MS: Record<Range, number> = { "15m": 15 * 60_000, "1h": 60 * 60_000 };

/** Sums `starts` into one-minute buckets so the bar chart stays readable. */
function startsPerMinute(samples: FleetSample[]): { times: number[]; values: number[] } {
  const times: number[] = [];
  const values: number[] = [];
  for (const sample of samples) {
    const minute = Math.floor(sample.ts / 60_000) * 60_000;
    if (times.at(-1) === minute) values[values.length - 1] = (values.at(-1) ?? 0) + sample.starts;
    else {
      times.push(minute);
      values.push(sample.starts);
    }
  }
  return { times, values };
}

export const MEMBER_TONE: Record<ClusterMember["state"], Tone> = {
  alive: "ok",
  suspect: "warn",
  dead: "error",
  left: "muted",
};

function raftRole(member: ClusterMember): string {
  if (!member.raft) return "—";
  if (member.raft.leader === member.id) return `leader · term ${member.raft.term}`;
  return member.raft.voter ? "voter" : "learner";
}

export function OverviewPage() {
  const cluster = useCluster();
  const [range, setRange] = useState<Range>("15m");
  const now = useNow(5000);
  const view = cluster.data;
  const history = (view?.history ?? []).filter((s) => s.ts >= now - RANGE_MS[range]);
  const latest = view?.history.at(-1);
  const hourStarts = (view?.history ?? []).reduce((sum, s) => sum + s.starts, 0);
  const nodes = (view?.members ?? []).filter((m) => m.node);
  const alive = nodes.filter((m) => m.state === "alive");
  const times = history.map((s) => s.ts);
  const domain: [number, number] = [now - RANGE_MS[range], now];
  const starts = startsPerMinute(history);
  const members = [...(view?.members ?? [])].sort(
    (a, b) => Number(b.roles.driver) - Number(a.roles.driver) || a.id.localeCompare(b.id),
  );

  return (
    <div className="page">
      <div className="page__head">
        <div>
          <h1 className="page__title">Overview</h1>
          <p className="page__lede">Fleet capacity and sandbox activity across every node.</p>
        </div>
        <div className="page__actions">
          <Segmented
            label="Chart range"
            value={range}
            onChange={setRange}
            options={[
              { value: "15m", label: "15 min" },
              { value: "1h", label: "1 hour" },
            ]}
          />
        </div>
      </div>
      <ErrorNote error={cluster.error} />

      <div className="stats">
        <Stat
          label="Running sandboxes"
          value={latest ? fmt.int(latest.sandboxes) : "—"}
          sub={`${fmt.int(hourStarts)} started in the last hour`}
        />
        <Stat
          label="Nodes"
          value={
            <>
              {fmt.int(alive.length)}
              <small>/ {fmt.int(nodes.length)}</small>
            </>
          }
          sub={
            alive.length === nodes.length
              ? "All healthy"
              : `${nodes.length - alive.length} not alive`
          }
        />
        <Stat
          label="vCPU allocated"
          value={
            latest ? (
              <>
                {fmt.int(latest.vcpusUsed)}
                <small>/ {fmt.int(latest.vcpus)}</small>
              </>
            ) : (
              "—"
            )
          }
          sub={
            latest ? (
              <Meter label="vCPU allocation" used={latest.vcpusUsed} total={latest.vcpus} />
            ) : undefined
          }
        />
        <Stat
          label="Memory allocated"
          value={latest ? fmt.mib(latest.memoryUsedMB) : "—"}
          sub={
            latest ? (
              <>
                of {fmt.mib(latest.memoryMB)}
                <Meter
                  label="Memory allocation"
                  used={latest.memoryUsedMB}
                  total={latest.memoryMB}
                />
              </>
            ) : undefined
          }
        />
        <Stat
          label="Template builds"
          value={latest ? fmt.int(latest.builds) : "—"}
          sub="Running on nodes now"
        />
      </div>

      <div className="stack">
        <div className="grid-2">
          <Card title="Running sandboxes" subtitle="Sampled every 5 seconds">
            <Chart
              label="Running sandboxes over time"
              times={times}
              domain={domain}
              series={[
                { label: "Running", values: history.map((s) => s.sandboxes), style: "lead" },
              ]}
              format={(v) => fmt.int(Math.round(v))}
            />
          </Card>
          <Card title="Sandbox starts" subtitle="Creates and resumes per minute">
            <Chart
              label="Sandbox starts per minute"
              bucketMs={60_000}
              domain={domain}
              times={starts.times}
              series={[{ label: "Starts", values: starts.values, style: "lead" }]}
              format={(v) => fmt.int(Math.round(v))}
            />
          </Card>
          <Card title="vCPU" subtitle="Allocated to sandboxes vs. offered by nodes">
            <Chart
              label="vCPU allocation over time"
              times={times}
              domain={domain}
              series={[
                { label: "Allocated", values: history.map((s) => s.vcpusUsed), style: "lead" },
                { label: "Capacity", values: history.map((s) => s.vcpus), style: "ref" },
              ]}
              format={(v) => fmt.num(v)}
            />
          </Card>
          <Card title="Memory" subtitle="Allocated to sandboxes vs. offered by nodes">
            <Chart
              label="Memory allocation over time"
              times={times}
              domain={domain}
              binary
              series={[
                { label: "Allocated", values: history.map((s) => s.memoryUsedMB), style: "lead" },
                { label: "Capacity", values: history.map((s) => s.memoryMB), style: "ref" },
              ]}
              format={(v) => fmt.mib(v)}
            />
          </Card>
        </div>

        <Card title="Cluster members" subtitle="Gossip view of the serving driver" flush>
          {members.length === 0 ? (
            <Empty title={cluster.loading ? "Loading…" : "No members"} />
          ) : (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Member</th>
                    <th>State</th>
                    <th>Roles</th>
                    <th>Raft</th>
                    <th>Platform</th>
                    <th className="right">Sandboxes</th>
                    <th>Version</th>
                    <th className="right">Up</th>
                  </tr>
                </thead>
                <tbody>
                  {members.map((m) => (
                    <tr key={m.id}>
                      <td>
                        {m.node ? (
                          <Link to={`/nodes/${m.id}`} className="row-link mono">
                            {shortId(m.id)}
                          </Link>
                        ) : (
                          <span className="mono">{shortId(m.id)}</span>
                        )}
                        {m.id === view?.localID && (
                          <>
                            {" "}
                            <Badge tone="info">this driver</Badge>
                          </>
                        )}
                        <div className="cell-sub mono">{m.clusterURL}</div>
                      </td>
                      <td>
                        <Status tone={MEMBER_TONE[m.state]}>{m.state}</Status>
                      </td>
                      <td>
                        {[m.roles.driver && "driver", m.roles.node && "node"]
                          .filter(Boolean)
                          .join(" + ")}
                      </td>
                      <td className="muted">{raftRole(m)}</td>
                      <td className="muted">
                        {m.node ? `${m.node.os}/${m.node.arch} · ${m.node.backend}` : "—"}
                      </td>
                      <td className="right num">{m.node ? fmt.int(m.node.sandboxes) : "—"}</td>
                      <td className="mono muted">{m.version}</td>
                      <td className="right num muted">
                        {fmt.duration(now - new Date(m.startedAt).getTime())}
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
