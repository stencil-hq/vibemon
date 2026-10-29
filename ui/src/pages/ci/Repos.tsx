import { useState } from "react";
import { type CiAccount, ci } from "../../api.ts";
import { Badge, Button, Card, Empty, ErrorNote, Status } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { usePoll } from "../../hooks.ts";
import { Icon } from "../../icons.tsx";
import { Link, setQuery, useRoute } from "../../router.tsx";
import { External, repoUrl } from "./shared.tsx";

/** Repositories the installations can see, and whether their jobs may run. */
export function ReposPanel({ accounts }: { accounts: CiAccount[] }) {
  const route = useRoute();
  const account = route.query.get("account") ?? "";
  const [search, setSearch] = useState("");
  const repos = usePoll(`ci-repos|${account}`, (signal) => ci.repos(account, signal), 30_000);
  const needle = search.trim().toLowerCase();
  const rows = (repos.data?.repos ?? []).filter(
    (r) => !needle || r.repo.toLowerCase().includes(needle),
  );

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <label className="search">
          <Icon name="search" />
          <span className="visually-hidden">Filter repositories</span>
          <input
            className="input"
            type="search"
            name="repo-search"
            autoComplete="off"
            spellCheck={false}
            placeholder="Filter repositories…"
            value={search}
            onChange={(event) => setSearch(event.currentTarget.value)}
          />
        </label>
        <select
          className="select"
          style={{ width: 200 }}
          aria-label="Account"
          value={account}
          onChange={(event) => setQuery("account", event.currentTarget.value)}
        >
          <option value="">All accounts</option>
          {accounts.map((a) => (
            <option key={a.installationId} value={a.login}>
              {a.login}
            </option>
          ))}
        </select>
        <span className="topbar__spacer" />
        <span className="faint num" aria-live="polite">
          {fmt.int(rows.length)} shown
        </span>
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={repos.refresh}
        />
      </div>
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={repos.error} />
      </div>
      {rows.length === 0 ? (
        <Empty
          title={repos.loading ? "Loading…" : needle ? "No repositories match" : "No repositories"}
        >
          {!repos.loading && !needle && "Installations list the repositories they were granted."}
        </Empty>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Repository</th>
                <th>Visibility</th>
                <th>Jobs</th>
                <th>Reason</th>
                <th className="shrink">
                  <span className="visually-hidden">Links</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.repo}>
                  <td>
                    <Link
                      to={`/ci?tab=jobs&repo=${encodeURIComponent(r.repo)}`}
                      className="row-link"
                    >
                      {r.repo}
                    </Link>
                    <div className="cell-sub">{r.account}</div>
                  </td>
                  <td>{r.private ? <Badge>private</Badge> : <Badge tone="info">public</Badge>}</td>
                  <td>
                    {r.allowed ? (
                      <Status tone="ok">allowed</Status>
                    ) : (
                      <Status tone="error">refused</Status>
                    )}
                  </td>
                  <td className="muted">{r.reason ?? <span className="faint">—</span>}</td>
                  <td className="shrink">
                    <External
                      href={repoUrl(r.repo)}
                      className="muted"
                      aria-label={`${r.repo} on GitHub`}
                    >
                      GitHub
                    </External>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
