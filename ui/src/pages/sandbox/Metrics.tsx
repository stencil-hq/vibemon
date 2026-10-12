import { useState } from "react";
import { api } from "../../api.ts";
import { Chart } from "../../chart.tsx";
import { Card, ErrorNote, Meter, Segmented, Stat } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { usePoll } from "../../hooks.ts";

type Range = "15m" | "1h" | "6h" | "all";
const RANGE_SECS: Record<Range, number> = {
  "15m": 900,
  "1h": 3600,
  "6h": 21_600,
  all: Number.POSITIVE_INFINITY,
};

/** envd samples (CPU, memory, disk) from the owning node, stepped by range. */
export function MetricsPanel({
  teamID,
  sandboxID,
  startedAt,
}: {
  teamID: string;
  sandboxID: string;
  startedAt: string;
}) {
  const [range, setRange] = useState<Range>("15m");
  const metrics = usePoll(
    `metrics|${sandboxID}|${range}`,
    (signal) => {
      const started = Math.floor(new Date(startedAt).getTime() / 1000);
      const start = Math.max(started, Math.floor(Date.now() / 1000 - RANGE_SECS[range]));
      return api.metrics(teamID, sandboxID, start, signal);
    },
    10_000,
  );
  const samples = metrics.data ?? [];
  const times = samples.map((s) => s.timestampUnix * 1000);
  const now = Date.now();
  const started = new Date(startedAt).getTime();
  const domain: [number, number] = [Math.max(started, now - RANGE_SECS[range] * 1000), now];
  const latest = samples.at(-1);

  return (
    <div className="stack">
      <div className="page__head" style={{ marginBottom: 0 }}>
        <p className="muted">Reported by envd inside the sandbox every few seconds.</p>
        <Segmented
          label="Range"
          value={range}
          onChange={setRange}
          options={[
            { value: "15m", label: "15 min" },
            { value: "1h", label: "1 hour" },
            { value: "6h", label: "6 hours" },
            { value: "all", label: "Since start" },
          ]}
        />
      </div>
      <ErrorNote error={metrics.error} />
      <div className="stats" style={{ marginBottom: 0 }}>
        <Stat
          label="CPU"
          value={latest ? `${fmt.num(latest.cpuUsedPct)}%` : "—"}
          sub={latest ? `of ${latest.cpuCount} vCPU` : undefined}
        />
        <Stat
          label="Memory"
          value={latest ? fmt.bytes(latest.memUsed) : "—"}
          sub={
            latest && (
              <>
                of {fmt.bytes(latest.memTotal)}
                <Meter label="Memory used" used={latest.memUsed} total={latest.memTotal} />
              </>
            )
          }
        />
        <Stat
          label="Disk"
          value={latest ? fmt.bytes(latest.diskUsed) : "—"}
          sub={
            latest && (
              <>
                of {fmt.bytes(latest.diskTotal)}
                <Meter label="Disk used" used={latest.diskUsed} total={latest.diskTotal} />
              </>
            )
          }
        />
      </div>
      <div className="grid-2">
        <Card title="CPU" subtitle="Percent of the sandbox's vCPUs">
          <Chart
            label="CPU usage"
            times={times}
            domain={domain}
            series={[{ label: "CPU", values: samples.map((s) => s.cpuUsedPct), style: "lead" }]}
            format={(v) => `${fmt.num(v)}%`}
            empty={metrics.loading ? "Loading…" : "No samples in this range"}
          />
        </Card>
        <Card title="Memory" subtitle="Used vs. total guest memory">
          <Chart
            label="Memory usage"
            times={times}
            domain={domain}
            binary
            series={[
              { label: "Used", values: samples.map((s) => s.memUsed), style: "lead" },
              { label: "Total", values: samples.map((s) => s.memTotal), style: "ref" },
            ]}
            format={fmt.bytes}
            empty={metrics.loading ? "Loading…" : "No samples in this range"}
          />
        </Card>
        <Card title="Disk" subtitle="Used vs. total root filesystem">
          <Chart
            label="Disk usage"
            times={times}
            domain={domain}
            binary
            series={[
              { label: "Used", values: samples.map((s) => s.diskUsed), style: "lead" },
              { label: "Total", values: samples.map((s) => s.diskTotal), style: "ref" },
            ]}
            format={fmt.bytes}
            empty={metrics.loading ? "Loading…" : "No samples in this range"}
          />
        </Card>
      </div>
    </div>
  );
}
