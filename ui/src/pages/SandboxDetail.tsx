import { useNodeName } from "../App.tsx";
import { api, type SandboxDetail } from "../api.ts";
import {
  Badge,
  Button,
  Card,
  CopyButton,
  Detail,
  Empty,
  ErrorNote,
  Menu,
  Status,
  Tabs,
} from "../components.tsx";
import type { EnvdTarget } from "../envd.ts";
import { fmt } from "../format.ts";
import { useNow, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link, navigate, useRoute } from "../router.tsx";
import {
  killSandbox,
  pauseSandbox,
  resumeSandbox,
  type SandboxRef,
  setSandboxTimeout,
} from "./actions.tsx";
import { FilesPanel } from "./sandbox/Files.tsx";
import { LogsPanel } from "./sandbox/Logs.tsx";
import { MetricsPanel } from "./sandbox/Metrics.tsx";
import { TerminalPanel } from "./sandbox/Terminal.tsx";

const TABS = [
  { id: "terminal", label: "Terminal", icon: "terminal" },
  { id: "files", label: "Files", icon: "folder" },
  { id: "metrics", label: "Metrics", icon: "chart" },
  { id: "logs", label: "Logs", icon: "logs" },
] as const;

type TabId = (typeof TABS)[number]["id"];

interface Loaded {
  detail: SandboxDetail;
  teamID: string;
  teamName: string;
  nodeID: string;
}

/** Admin row (team, node), then the team-scoped detail carrying the envd token. */
async function loadSandbox(id: string, signal: AbortSignal): Promise<Loaded> {
  const [row] = (await api.sandboxes({ id }, signal)).rows;
  if (!row) throw new Error("It was killed or never existed.");
  const detail = await api.sandbox(row.teamID, id, signal);
  return { detail, teamID: row.teamID, teamName: row.teamName, nodeID: row.nodeID };
}

export function SandboxDetailPage({ id }: { id: string }) {
  const route = useRoute();
  const tabParam = route.query.get("tab");
  const tab: TabId = TABS.some((t) => t.id === tabParam) ? (tabParam as TabId) : "terminal";
  const sandbox = usePoll(`sandbox|${id}`, (signal) => loadSandbox(id, signal), 5000);
  const now = useNow(1000);
  const nodeName = useNodeName();
  const data = sandbox.data;

  if (!data) {
    return (
      <div className="page">
        <Eyebrow />
        <h1 className="page__title mono">{id}</h1>
        <div style={{ marginTop: 24 }}>
          {sandbox.error ? (
            <Empty title="Sandbox unavailable">
              {sandbox.error.message} <Link to="/sandboxes">Back to sandboxes</Link>.
            </Empty>
          ) : (
            <Empty title="Loading…" />
          )}
        </div>
      </div>
    );
  }

  const { detail } = data;
  const ref: SandboxRef = { sandboxID: id, teamID: data.teamID };
  const running = detail.state === "running";
  const endsIn = new Date(detail.endAt).getTime() - now;
  const target: EnvdTarget = { sandboxID: id, token: detail.envdAccessToken };
  const after = (ok: boolean) => {
    if (ok) sandbox.refresh();
  };
  const extend = (secs: number, label: string) =>
    void setSandboxTimeout(ref, secs, label).then(after);
  const meta = Object.entries(detail.metadata ?? {});

  return (
    <div className="page">
      <div className="page__head">
        <div style={{ minWidth: 0 }}>
          <Eyebrow />
          <h1 className="page__title mono truncate">{id}</h1>
          <p className="page__lede">
            {running ? <Status tone="live">running</Status> : <Status tone="muted">paused</Status>}
            <span className="faint"> · </span>
            {detail.alias ?? detail.templateID}
            <span className="faint"> · </span>
            {detail.cpuCount} vCPU · {fmt.mib(detail.memoryMB)} RAM · {fmt.mib(detail.diskSizeMB)}{" "}
            disk
          </p>
        </div>
        <div className="page__actions">
          {running ? (
            <Button icon="pause" onClick={() => void pauseSandbox(ref).then(after)}>
              Pause
            </Button>
          ) : (
            <Button
              icon="play"
              variant="primary"
              onClick={() => void resumeSandbox(ref).then(after)}
            >
              Resume
            </Button>
          )}
          <Button
            variant="danger"
            icon="kill"
            onClick={() =>
              void killSandbox(ref).then((ok) => {
                if (ok) navigate("/sandboxes");
              })
            }
          >
            Kill
          </Button>
          <Menu
            label="Set timeout"
            items={[
              {
                label: "Expire in 5 minutes",
                icon: "clock",
                disabled: !running,
                onSelect: () => extend(300, "in 5 minutes"),
              },
              {
                label: "Expire in 1 hour",
                icon: "clock",
                disabled: !running,
                onSelect: () => extend(3600, "in 1 hour"),
              },
              {
                label: "Expire in 8 hours",
                icon: "clock",
                disabled: !running,
                onSelect: () => extend(28_800, "in 8 hours"),
              },
              {
                label: "Expire in 24 hours",
                icon: "clock",
                disabled: !running,
                onSelect: () => extend(86_400, "in 24 hours"),
              },
            ]}
          />
        </div>
      </div>
      <ErrorNote error={sandbox.error} />

      <Card className="detail-card">
        <dl className="kv" style={{ paddingTop: 18 }}>
          <Detail label="Team">
            <Link to={`/sandboxes?team=${data.teamID}`} className="row-link">
              {data.teamName}
            </Link>
          </Detail>
          <Detail label="Node">
            {data.nodeID ? (
              <Link to={`/nodes/${data.nodeID}`} className="row-link" title={data.nodeID}>
                {nodeName(data.nodeID)}
              </Link>
            ) : (
              "—"
            )}
          </Detail>
          <Detail label="Template">
            <span className="mono truncate">{detail.templateID}</span>
            <CopyButton value={detail.templateID} label="Copy template ID" />
          </Detail>
          <Detail label="Started">
            <span title={fmt.dateTime(detail.startedAt)}>{fmt.ago(detail.startedAt, now)}</span>
          </Detail>
          <Detail label={running ? "Expires" : "Paused"}>
            {running ? (
              <span title={fmt.dateTime(detail.endAt)} className="num">
                {endsIn > 0 ? `in ${fmt.duration(endsIn)}` : <Badge tone="warn">expiring</Badge>}
              </span>
            ) : (
              <span title={fmt.dateTime(detail.endAt)}>{fmt.ago(detail.endAt, now)}</span>
            )}
          </Detail>
          <Detail label="On timeout">
            {detail.lifecycle.onTimeout === "pause" ? "Pause" : "Kill"}
          </Detail>
          <Detail label="Auto-resume">{detail.lifecycle.autoResume ? "On" : "Off"}</Detail>
          <Detail label="Internet">
            {detail.allow_internet_access === false ? "Blocked" : "Allowed"}
          </Detail>
          <Detail label="envd">
            <span className="mono">{detail.envdVersion}</span>
          </Detail>
          {meta.length > 0 && (
            <Detail label="Metadata">
              <span className="mono muted" style={{ fontSize: 12.5 }}>
                {meta.map(([k, v]) => `${k}=${v}`).join("  ")}
              </span>
            </Detail>
          )}
        </dl>
      </Card>

      <div style={{ height: 20 }} />
      <Tabs tabs={[...TABS]} active={tab} base={`/sandboxes/${id}`} />

      {(tab === "terminal" || tab === "files") && !running ? (
        <Card>
          <Empty title="This sandbox is paused">
            <p style={{ marginBottom: 12 }}>Resume it to open a terminal or browse its files.</p>
            <Button
              variant="primary"
              icon="play"
              onClick={() => void resumeSandbox(ref).then(after)}
            >
              Resume Sandbox
            </Button>
          </Empty>
        </Card>
      ) : tab === "terminal" ? (
        <TerminalPanel target={target} />
      ) : tab === "files" ? (
        <FilesPanel target={target} />
      ) : tab === "metrics" ? (
        <MetricsPanel teamID={data.teamID} sandboxID={id} startedAt={detail.startedAt} />
      ) : (
        <LogsPanel teamID={data.teamID} sandboxID={id} />
      )}
    </div>
  );
}

function Eyebrow() {
  return (
    <div className="page__eyebrow">
      <Link to="/sandboxes">Sandboxes</Link>
      <Icon name="chevron" size={12} />
    </div>
  );
}
