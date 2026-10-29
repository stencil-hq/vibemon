import { useEffect, useState } from "react";
import { useNodeName } from "../../App.tsx";
import { type CiAccount, type CiJob, type CiJobFilter, type CiJobPage, ci } from "../../api.ts";
import { Badge, Button, Card, Empty, ErrorNote, Segmented } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { useNow, usePoll } from "../../hooks.ts";
import { Icon } from "../../icons.tsx";
import { Link, setQuery, useRoute } from "../../router.tsx";
import { Duration, JobState, runMs, sizeLabel, waitMs } from "./shared.tsx";

const FILTERS: { value: CiJobFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "active", label: "Active" },
  { value: "failed", label: "Failed" },
  { value: "refused", label: "Refused" },
];

/** Every recorded job, newest first, filtered server-side and paged with `nextToken`. */
export function JobsPanel({ accounts }: { accounts: CiAccount[] }) {
  const route = useRoute();
  const stateParam = route.query.get("state") as CiJobFilter | null;
  const state = FILTERS.some((f) => f.value === stateParam) ? (stateParam as CiJobFilter) : "all";
  const account = route.query.get("account") ?? "";
  const repo = route.query.get("repo") ?? "";
  const [repoInput, setRepoInput] = useState(repo);
  // Pages fetched with "Load More", tagged with the filter they belong to.
  const [extra, setExtra] = useState<(CiJobPage & { key: string }) | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const now = useNow(1000);

  // Follow links that set ?repo= while this panel is mounted.
  useEffect(() => setRepoInput(repo), [repo]);
  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (repoInput.trim() !== repo) setQuery("repo", repoInput.trim());
    }, 400);
    return () => window.clearTimeout(timer);
  }, [repoInput, repo]);

  const filter = { state, account, repo };
  const key = `${state}|${account}|${repo}`;
  const list = usePoll(`ci-jobs|${key}`, (signal) => ci.jobs(filter, signal), 5000);
  const more = extra?.key === key ? extra : null;
  const rows = [...(list.data?.jobs ?? []), ...(more?.jobs ?? [])];
  const next = more ? more.next : (list.data?.next ?? null);

  const loadMore = async () => {
    if (!next) return;
    setLoadingMore(true);
    try {
      const page = await ci.jobs({ ...filter, next });
      setExtra({ key, jobs: [...(more?.jobs ?? []), ...page.jobs], next: page.next });
    } finally {
      setLoadingMore(false);
    }
  };

  const filtered = state !== "all" || account !== "" || repo !== "";

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <label className="search">
          <Icon name="search" />
          <span className="visually-hidden">Repository</span>
          <input
            className="input"
            type="search"
            name="repo"
            autoComplete="off"
            spellCheck={false}
            placeholder="Repository (owner/name)…"
            value={repoInput}
            onChange={(event) => setRepoInput(event.currentTarget.value)}
          />
        </label>
        <Segmented
          label="State"
          value={state}
          onChange={(value) => setQuery("state", value === "all" ? null : value)}
          options={FILTERS}
        />
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
          onClick={() => {
            setExtra(null);
            list.refresh();
          }}
        />
      </div>
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={list.error} />
      </div>
      {rows.length === 0 ? (
        <Empty title={list.loading ? "Loading…" : filtered ? "No jobs match" : "No jobs yet"}>
          {!list.loading && !filtered && (
            <>
              Jobs show up here when a workflow of a linked account uses <code>runs-on: vmon</code>.
            </>
          )}
        </Empty>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Job</th>
                <th>Repository</th>
                <th>State</th>
                <th>Runner</th>
                <th className="right">Wait</th>
                <th className="right">Run</th>
                <th className="right">Queued</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((job) => (
                <JobRow key={`${job.jobId}/${job.runAttempt}`} job={job} now={now} />
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
  );
}

function JobRow({ job, now }: { job: CiJob; now: number }) {
  const nodeName = useNodeName();
  return (
    <tr>
      <td style={{ maxWidth: 320 }}>
        <Link to={`/ci/jobs/${job.jobId}`} className="row-link">
          {job.job}
        </Link>
        <div className="cell-sub truncate" title={job.workflow}>
          {job.workflow}
          {job.runAttempt > 1 && ` · attempt ${job.runAttempt}`}
        </div>
      </td>
      <td>
        <Link to={`/ci?repo=${encodeURIComponent(job.repo)}`} style={{ fontWeight: 400 }}>
          {job.repo}
        </Link>{" "}
        {!job.private && <Badge>public</Badge>}
        <div className="cell-sub mono">
          {job.branch ?? "—"} · {job.sha.slice(0, 7)}
        </div>
      </td>
      <td style={{ maxWidth: 300 }}>
        <JobState job={job} />
        {job.reason && (
          <div className="cell-sub" title={job.reason}>
            {job.reason}
          </div>
        )}
      </td>
      <td>
        <span className="mono" style={{ fontSize: 12.5 }}>
          {sizeLabel(job)}
        </span>
        {job.nodeId && <div className="cell-sub">{nodeName(job.nodeId)}</div>}
      </td>
      <td className="right num muted">
        <Duration ms={waitMs(job, now)} />
      </td>
      <td className="right num muted">
        <Duration ms={runMs(job, now)} />
      </td>
      <td className="right num muted" title={fmt.dateTime(job.queuedAt)}>
        {fmt.ago(job.queuedAt, now)}
      </td>
    </tr>
  );
}
