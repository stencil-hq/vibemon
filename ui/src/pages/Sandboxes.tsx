import { type FormEvent, useMemo, useState } from "react";
import { useNodeName } from "../App.tsx";
import { api, type SandboxRow, type SandboxState, type TeamView } from "../api.ts";
import {
  Badge,
  Button,
  Card,
  Empty,
  ErrorNote,
  Menu,
  Modal,
  Segmented,
  Status,
} from "../components.tsx";
import { fmt } from "../format.ts";
import { attempt, useNow, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link, navigate, setQuery, useRoute } from "../router.tsx";
import { killSandbox, pauseSandbox, resumeSandbox } from "./actions.tsx";

type StateFilter = "all" | SandboxState;

/** Every row a filter can match on, lowercased once per row. */
function haystack(row: SandboxRow, nodeName: string): string {
  const meta = Object.entries(row.metadata ?? {}).map(([k, v]) => `${k}=${v}`);
  return [
    row.sandboxID,
    row.templateID,
    row.alias ?? "",
    row.teamName,
    row.teamID,
    row.nodeID,
    nodeName,
    ...meta,
  ]
    .join(" ")
    .toLowerCase();
}

export function SandboxesPage() {
  const route = useRoute();
  const state = (route.query.get("state") ?? "all") as StateFilter;
  const team = route.query.get("team") ?? "";
  const [search, setSearch] = useState(route.query.get("q") ?? "");
  const [extra, setExtra] = useState<{ rows: SandboxRow[]; next: string | null } | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [creating, setCreating] = useState(false);
  const now = useNow(1000);
  const nodeName = useNodeName();

  const filterKey = `${state}|${team}`;
  const list = usePoll(
    `sandboxes|${filterKey}`,
    (signal) =>
      api.sandboxes(
        { state: state === "all" ? undefined : state, team: team || undefined },
        signal,
      ),
    5000,
  );
  const teams = usePoll("teams", api.teams, 30000);

  const rows = useMemo(() => {
    const all = [...(list.data?.rows ?? []), ...(extra?.rows ?? [])];
    const needle = search.trim().toLowerCase();
    return needle ? all.filter((row) => haystack(row, nodeName(row.nodeID)).includes(needle)) : all;
  }, [list.data, extra, search, nodeName]);
  const next = extra ? extra.next : (list.data?.next ?? null);

  const loadMore = async () => {
    if (!next) return;
    setLoadingMore(true);
    try {
      const page = await api.sandboxes({
        state: state === "all" ? undefined : state,
        team: team || undefined,
        next,
      });
      setExtra((prev) => ({ rows: [...(prev?.rows ?? []), ...page.rows], next: page.next }));
    } finally {
      setLoadingMore(false);
    }
  };

  const refresh = () => {
    setExtra(null);
    list.refresh();
  };

  return (
    <div className="page">
      <div className="page__head">
        <div>
          <h1 className="page__title">Sandboxes</h1>
          <p className="page__lede">Running and paused sandboxes of every team.</p>
        </div>
        <div className="page__actions">
          <Button variant="accent" icon="plus" onClick={() => setCreating(true)}>
            New Sandbox
          </Button>
        </div>
      </div>
      <ErrorNote error={list.error} />

      <Card flush>
        <div className="toolbar" style={{ paddingTop: 14 }}>
          <label className="search">
            <Icon name="search" />
            <span className="visually-hidden">Filter sandboxes</span>
            <input
              className="input"
              type="search"
              name="q"
              autoComplete="off"
              spellCheck={false}
              placeholder="Filter by ID, template, team, node, metadata…"
              value={search}
              onChange={(event) => {
                setSearch(event.currentTarget.value);
                setQuery("q", event.currentTarget.value);
              }}
            />
          </label>
          <Segmented
            label="State"
            value={state}
            onChange={(value) => {
              setExtra(null);
              setQuery("state", value === "all" ? null : value);
            }}
            options={[
              { value: "all", label: "All" },
              { value: "running", label: "Running" },
              { value: "paused", label: "Paused" },
            ]}
          />
          <select
            className="select"
            style={{ width: 200 }}
            aria-label="Team"
            value={team}
            onChange={(event) => {
              setExtra(null);
              setQuery("team", event.currentTarget.value);
            }}
          >
            <option value="">All teams</option>
            {(teams.data ?? []).map((t) => (
              <option key={t.teamID} value={t.teamID}>
                {t.name}
              </option>
            ))}
          </select>
          <span className="topbar__spacer" />
          <span className="faint num" aria-live="polite">
            {fmt.int(rows.length)} shown
          </span>
          <Button variant="ghost" size="sm" icon="refresh" aria-label="Refresh" onClick={refresh} />
        </div>

        {rows.length === 0 ? (
          <Empty title={list.loading ? "Loading…" : search ? "No sandboxes match" : "No sandboxes"}>
            {!list.loading &&
              !search &&
              "Create one here or with the E2B SDK; it shows up within seconds."}
          </Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Sandbox</th>
                  <th>Team</th>
                  <th>State</th>
                  <th>Node</th>
                  <th className="right">Resources</th>
                  <th className="right">Started</th>
                  <th className="right">Ends</th>
                  <th className="shrink">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <SandboxRowView
                    key={row.sandboxID}
                    row={row}
                    node={nodeName(row.nodeID)}
                    now={now}
                    onChanged={refresh}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
        {next && (
          <div className="toolbar" style={{ justifyContent: "center", paddingTop: 12 }}>
            <Button busy={loadingMore} onClick={() => void loadMore()}>
              Load More
            </Button>
          </div>
        )}
      </Card>

      <CreateSandbox
        open={creating}
        teams={teams.data ?? []}
        defaultTeam={team}
        onClose={() => setCreating(false)}
      />
    </div>
  );
}

function SandboxRowView({
  row,
  node,
  now,
  onChanged,
}: {
  row: SandboxRow;
  node: string;
  now: number;
  onChanged: () => void;
}) {
  const running = row.state === "running";
  const endsIn = new Date(row.endAt).getTime() - now;
  const after = (ok: boolean) => {
    if (ok) onChanged();
  };
  return (
    <tr>
      <td>
        <Link to={`/sandboxes/${row.sandboxID}`} className="row-link mono">
          {row.sandboxID}
        </Link>
        <div className="cell-sub truncate">{row.alias ?? row.templateID}</div>
      </td>
      <td>
        <Link to={`/sandboxes?team=${row.teamID}`} className="row-link" style={{ fontWeight: 400 }}>
          {row.teamName}
        </Link>
      </td>
      <td>
        {running ? <Status tone="live">running</Status> : <Status tone="muted">paused</Status>}
      </td>
      <td>
        <Link to={`/nodes/${row.nodeID}`} className="muted" title={row.nodeID}>
          {node}
        </Link>
      </td>
      <td className="right num muted">
        {row.cpuCount} vCPU · {fmt.mib(row.memoryMB)}
      </td>
      <td className="right num muted" title={fmt.dateTime(row.startedAt)}>
        {fmt.ago(row.startedAt, now)}
      </td>
      <td className="right num" title={fmt.dateTime(row.endAt)}>
        {running ? (
          endsIn > 0 ? (
            <span className={endsIn < 60_000 ? "" : "muted"}>{fmt.duration(endsIn)}</span>
          ) : (
            <Badge tone="warn">expiring</Badge>
          )
        ) : (
          <span className="faint">—</span>
        )}
      </td>
      <td className="shrink">
        <Menu
          label={`Actions for ${row.sandboxID}`}
          items={[
            {
              label: "Open terminal",
              icon: "terminal",
              disabled: !running,
              onSelect: () => navigate(`/sandboxes/${row.sandboxID}?tab=terminal`),
            },
            {
              label: "Browse files",
              icon: "folder",
              disabled: !running,
              onSelect: () => navigate(`/sandboxes/${row.sandboxID}?tab=files`),
            },
            running
              ? {
                  label: "Pause",
                  icon: "pause",
                  onSelect: () => void pauseSandbox(row).then(after),
                }
              : {
                  label: "Resume",
                  icon: "play",
                  onSelect: () => void resumeSandbox(row).then(after),
                },
            {
              label: "Kill",
              icon: "kill",
              danger: true,
              onSelect: () => void killSandbox(row).then(after),
            },
          ]}
        />
      </td>
    </tr>
  );
}

const TIMEOUTS: { label: string; secs: number }[] = [
  { label: "5 minutes", secs: 300 },
  { label: "15 minutes", secs: 900 },
  { label: "1 hour", secs: 3600 },
  { label: "8 hours", secs: 28_800 },
  { label: "24 hours", secs: 86_400 },
];

function CreateSandbox({
  open,
  teams,
  defaultTeam,
  onClose,
}: {
  open: boolean;
  teams: TeamView[];
  defaultTeam: string;
  onClose: () => void;
}) {
  const [team, setTeam] = useState(defaultTeam);
  const [template, setTemplate] = useState("base");
  const [timeout, setTimeoutSecs] = useState(900);
  const [autoPause, setAutoPause] = useState(false);
  const [internet, setInternet] = useState(true);
  const [busy, setBusy] = useState(false);
  const teamID = team || teams.find((t) => !t.system)?.teamID || teams[0]?.teamID || "";
  const templates = usePoll(
    `templates|${teamID}`,
    (signal) => (teamID ? api.templates(teamID, signal) : Promise.resolve([])),
    0,
  );

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    let created = "";
    const ok = await attempt("Sandbox created", async () => {
      created = (
        await api.create(teamID, {
          templateID: template.trim(),
          timeout,
          autoPause,
          allow_internet_access: internet,
          metadata: { createdBy: "vmon-dashboard" },
        })
      ).sandboxID;
    });
    setBusy(false);
    if (ok) {
      onClose();
      navigate(`/sandboxes/${created}?tab=terminal`);
    }
  };

  return (
    <Modal
      title="New sandbox"
      open={open}
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            type="submit"
            form="create-sandbox"
            variant="primary"
            busy={busy}
            disabled={!teamID || !template.trim()}
          >
            Create Sandbox
          </Button>
        </>
      }
    >
      <form id="create-sandbox" className="form-grid" onSubmit={(event) => void submit(event)}>
        <label className="field span-2">
          <span className="field__label">Team</span>
          <select
            className="select"
            name="team"
            value={teamID}
            onChange={(event) => setTeam(event.currentTarget.value)}
          >
            {teams.map((t) => (
              <option key={t.teamID} value={t.teamID}>
                {t.name}
                {t.system ? " (system)" : ""}
              </option>
            ))}
          </select>
          <span className="field__hint">The sandbox counts against this team's limits.</span>
        </label>
        <label className="field span-2">
          <span className="field__label">Template</span>
          <input
            className="input mono"
            name="template"
            list="template-options"
            autoComplete="off"
            spellCheck={false}
            placeholder="base…"
            value={template}
            onChange={(event) => setTemplate(event.currentTarget.value)}
            required
          />
          <datalist id="template-options">
            {["base", "code-interpreter-v1", "desktop", "mcp-gateway"].map((alias) => (
              <option key={alias} value={alias} />
            ))}
            {(templates.data ?? []).flatMap((t) =>
              (t.names.length ? t.names : [t.templateID]).map((name) => (
                <option key={`${t.templateID}/${name}`} value={name} />
              )),
            )}
          </datalist>
          <span className="field__hint">
            Template ID or alias; public system templates resolve for every team.
          </span>
        </label>
        <label className="field">
          <span className="field__label">Timeout</span>
          <select
            className="select"
            name="timeout"
            value={timeout}
            onChange={(event) => setTimeoutSecs(Number(event.currentTarget.value))}
          >
            {TIMEOUTS.map((t) => (
              <option key={t.secs} value={t.secs}>
                {t.label}
              </option>
            ))}
          </select>
        </label>
        <div className="field">
          <span className="field__label">Options</span>
          <label className="check">
            <input
              type="checkbox"
              checked={autoPause}
              onChange={(event) => setAutoPause(event.currentTarget.checked)}
            />
            Pause instead of kill on timeout
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={internet}
              onChange={(event) => setInternet(event.currentTarget.checked)}
            />
            Internet access
          </label>
        </div>
      </form>
    </Modal>
  );
}
