import { useState } from "react";
import { api, type TeamView } from "../api.ts";
import { Badge, Card, CopyButton, confirmAction, Empty, ErrorNote, Menu } from "../components.tsx";
import { fmt } from "../format.ts";
import { toast, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link, navigate } from "../router.tsx";

async function killAll(team: TeamView, onDone: () => void): Promise<void> {
  const total = team.sandboxes.running + team.sandboxes.paused;
  const ok = await confirmAction({
    title: `Kill every sandbox of ${team.name}?`,
    body: (
      <p>
        {fmt.int(total)} running and paused {total === 1 ? "sandbox is" : "sandboxes are"} killed
        and their state deleted. The team itself and its templates stay.
      </p>
    ),
    action: "Kill All Sandboxes",
    danger: true,
  });
  if (!ok) return;
  try {
    const { killedCount, failedCount } = await api.killTeamSandboxes(team.teamID);
    toast(
      `${team.name}: ${fmt.int(killedCount)} killed${failedCount ? `, ${fmt.int(failedCount)} failed` : ""}`,
      failedCount ? "error" : "info",
    );
    onDone();
  } catch (error) {
    toast(error instanceof Error ? error.message : String(error), "error");
  }
}

export function TeamsPage() {
  const teams = usePoll("teams", api.teams, 10_000);
  const [search, setSearch] = useState("");
  const needle = search.trim().toLowerCase();
  const rows = (teams.data ?? []).filter(
    (t) => !needle || `${t.name} ${t.slug} ${t.teamID}`.toLowerCase().includes(needle),
  );

  return (
    <div className="page">
      <div className="page__head">
        <div>
          <h1 className="page__title">Teams</h1>
          <p className="page__lede">
            Tenants and their limits. Create teams, users, and keys with <code>vmon admin</code>.
          </p>
        </div>
      </div>
      <ErrorNote error={teams.error} />
      <Card flush>
        <div className="toolbar" style={{ paddingTop: 14 }}>
          <label className="search">
            <Icon name="search" />
            <span className="visually-hidden">Filter teams</span>
            <input
              className="input"
              type="search"
              name="team-search"
              autoComplete="off"
              spellCheck={false}
              placeholder="Filter by name, slug, or ID…"
              value={search}
              onChange={(event) => setSearch(event.currentTarget.value)}
            />
          </label>
        </div>
        {rows.length === 0 ? (
          <Empty title={teams.loading ? "Loading…" : "No teams"}>
            {!teams.loading && !needle && (
              <>
                Create one with <code>vmon admin create-team &lt;name&gt;</code>.
              </>
            )}
          </Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Team</th>
                  <th>ID</th>
                  <th className="right">Running</th>
                  <th className="right">Paused</th>
                  <th className="right">Sandbox limit</th>
                  <th className="right">Per sandbox</th>
                  <th className="right">Max lifetime</th>
                  <th className="right">Created</th>
                  <th className="shrink">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((t) => (
                  <tr key={t.teamID}>
                    <td>
                      <Link to={`/sandboxes?team=${t.teamID}`} className="row-link">
                        {t.name}
                      </Link>{" "}
                      {t.system && <Badge tone="info">system</Badge>}{" "}
                      {t.blocked && <Badge tone="error">blocked</Badge>}
                      <div className="cell-sub mono">{t.slug}</div>
                    </td>
                    <td>
                      <span className="kv__value" style={{ margin: 0 }}>
                        <span className="mono muted truncate" style={{ maxWidth: 110 }}>
                          {t.teamID}
                        </span>
                        <CopyButton value={t.teamID} label="Copy team ID" />
                      </span>
                    </td>
                    <td className="right num">{fmt.int(t.sandboxes.running)}</td>
                    <td className="right num muted">{fmt.int(t.sandboxes.paused)}</td>
                    <td className="right num muted">{fmt.int(t.limits.maxConcurrentSandboxes)}</td>
                    <td className="right num muted">
                      {t.limits.maxVcpus} vCPU · {fmt.mib(t.limits.maxMemoryMB)}
                    </td>
                    <td className="right num muted">{t.limits.maxLengthHours} h</td>
                    <td className="right num muted" title={fmt.dateTime(t.createdAt)}>
                      {fmt.ago(t.createdAt)}
                    </td>
                    <td className="shrink">
                      <Menu
                        label={`Actions for ${t.name}`}
                        items={[
                          {
                            label: "View sandboxes",
                            icon: "sandbox",
                            onSelect: () => navigate(`/sandboxes?team=${t.teamID}`),
                          },
                          {
                            label: "Kill all sandboxes",
                            icon: "kill",
                            danger: true,
                            disabled: t.sandboxes.running + t.sandboxes.paused === 0,
                            onSelect: () => void killAll(t, teams.refresh),
                          },
                        ]}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
