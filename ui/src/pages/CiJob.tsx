import { useMemo, useState } from "react";
import { useNodeName } from "../App.tsx";
import { ApiError, ci } from "../api.ts";
import { Badge, Button, Card, CopyButton, Detail, Empty } from "../components.tsx";
import { fmt } from "../format.ts";
import { useNow, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link, navigate } from "../router.tsx";
import {
  ButtonLink,
  Duration,
  External,
  JobState,
  repoUrl,
  runMs,
  sizeLabel,
  UNFINISHED,
  waitMs,
} from "./ci/shared.tsx";

export function CiJobPage({ id }: { id: string }) {
  const job = usePoll(`ci-job|${id}`, (signal) => ci.job(id, signal), 5000);
  const now = useNow(1000);
  const nodeName = useNodeName();
  const data = job.data;

  if (!data) {
    return (
      <div className="page">
        <Eyebrow />
        <h1 className="page__title mono">Job {id}</h1>
        <div style={{ marginTop: 24 }}>
          {job.error ? (
            <Empty title="Job unavailable">
              {job.error.message} <Link to="/ci">Back to CI</Link>.
            </Empty>
          ) : (
            <Empty title="Loading…" />
          )}
        </div>
      </div>
    );
  }

  // The sandbox is up (and its terminal reachable) only in these states.
  const live =
    data.sandboxId !== null &&
    (data.state === "starting" || data.state === "running" || data.state === "held");
  const heldFor = data.heldUntil ? new Date(data.heldUntil).getTime() - now : 0;

  return (
    <div className="page">
      <div className="page__head">
        <div style={{ minWidth: 0 }}>
          <Eyebrow />
          <h1 className="page__title truncate">{data.job}</h1>
          <p className="page__lede">
            <JobState job={data} />
            <span className="faint"> · </span>
            {data.workflow}
            <span className="faint"> · </span>
            <span className="mono">{data.repo}</span>
          </p>
        </div>
        <div className="page__actions">
          {live && data.sandboxId && (
            <Button
              variant="primary"
              icon="terminal"
              onClick={() => navigate(`/sandboxes/${data.sandboxId}?tab=terminal`)}
            >
              Open Terminal
            </Button>
          )}
          <ButtonLink href={data.htmlUrl}>View on GitHub</ButtonLink>
        </div>
      </div>
      {job.error && (
        <p className="error-note" role="alert">
          {job.error.message}
        </p>
      )}
      {data.reason && (
        <p className={data.state === "waiting" ? "warn-note" : "error-note"} role="status">
          {data.reason}
        </p>
      )}
      {data.state === "held" && data.heldUntil && (
        <p className="warn-note" role="status">
          Debug hold: the failed job's sandbox stays up{" "}
          {heldFor > 0 ? `for another ${fmt.duration(heldFor)}` : "until the reaper runs"} (until{" "}
          {fmt.dateTime(data.heldUntil)}). Open its terminal to inspect{" "}
          <code>/home/user/actions-runner/_work</code>.
        </p>
      )}

      <Card className="detail-card">
        <dl className="kv" style={{ paddingTop: 18 }}>
          <Detail label="Repository">
            <External href={repoUrl(data.repo)} className="row-link">
              {data.repo}
            </External>
            {!data.private && <Badge tone="info">public</Badge>}
          </Detail>
          <Detail label="Account">
            <Link to={`/ci?tab=jobs&account=${encodeURIComponent(data.account)}`}>
              {data.account}
            </Link>
          </Detail>
          <Detail label="Branch">
            <span className="mono">{data.branch ?? "—"}</span>
          </Detail>
          <Detail label="Commit">
            <External
              href={`${repoUrl(data.repo)}/commit/${data.sha}`}
              className="mono"
              title={data.sha}
            >
              {data.sha.slice(0, 10)}
            </External>
            <CopyButton value={data.sha} label="Copy commit SHA" />
          </Detail>
          <Detail label="Workflow run">
            <External href={`${repoUrl(data.repo)}/actions/runs/${data.runId}`} className="mono">
              {data.runId}
            </External>
            {data.runAttempt > 1 && <span className="faint"> · attempt {data.runAttempt}</span>}
          </Detail>
          <Detail label="Job ID">
            <span className="mono">{data.jobId}</span>
            <CopyButton value={String(data.jobId)} label="Copy job ID" />
          </Detail>
          <Detail label="Labels">
            <span className="chips">
              {data.labels.map((label) => (
                <Badge key={label}>{label}</Badge>
              ))}
            </span>
          </Detail>
          <Detail label="Runner">
            <span className="mono">{sizeLabel(data)}</span>
          </Detail>
          <Detail label="Template">
            <span className="mono">{data.from ?? (data.size ? "ci" : "—")}</span>
          </Detail>
          <Detail label="Debug hold">{data.debug ? "On failure, 30 min" : "Off"}</Detail>
          <Detail label="Conclusion">{data.conclusion ?? <span className="faint">—</span>}</Detail>
          <Detail label="Sandbox">
            {data.sandboxId ? (
              <>
                {live ? (
                  <Link to={`/sandboxes/${data.sandboxId}?tab=terminal`} className="row-link mono">
                    {data.sandboxId}
                  </Link>
                ) : (
                  <span className="mono muted" title="Killed">
                    {data.sandboxId}
                  </span>
                )}
                <CopyButton value={data.sandboxId} label="Copy sandbox ID" />
              </>
            ) : (
              <span className="faint">—</span>
            )}
          </Detail>
          <Detail label="Node">
            {data.nodeId ? (
              <Link to={`/nodes/${data.nodeId}`} className="row-link" title={data.nodeId}>
                {nodeName(data.nodeId)}
              </Link>
            ) : (
              <span className="faint">—</span>
            )}
          </Detail>
          <Detail label="Runner name">
            <span className="mono truncate">{data.runnerName ?? "—"}</span>
          </Detail>
          <Detail label="Queued">
            <span title={fmt.dateTime(data.queuedAt)}>{fmt.ago(data.queuedAt, now)}</span>
          </Detail>
          <Detail label="Started">
            {data.startedAt ? (
              <span title={fmt.dateTime(data.startedAt)}>{fmt.ago(data.startedAt, now)}</span>
            ) : (
              <span className="faint">—</span>
            )}
          </Detail>
          <Detail label="Completed">
            {data.completedAt ? (
              <span title={fmt.dateTime(data.completedAt)}>{fmt.ago(data.completedAt, now)}</span>
            ) : (
              <span className="faint">—</span>
            )}
          </Detail>
          <Detail label="Wait">
            <span className="num">
              <Duration ms={waitMs(data, now)} />
            </span>
          </Detail>
          <Detail label="Run">
            <span className="num">
              <Duration ms={runMs(data, now)} />
            </span>
          </Detail>
          {data.heldUntil && (
            <Detail label="Held until">
              <span title={fmt.dateTime(data.heldUntil)}>{fmt.ago(data.heldUntil, now)}</span>
            </Detail>
          )}
        </dl>
      </Card>

      <div style={{ height: 20 }} />
      {data.events.length > 0 && (
        <>
          <Card flush title="History" subtitle="State changes recorded by the CI controller">
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>State</th>
                    <th>Message</th>
                    <th>Runner</th>
                    <th>Sandbox</th>
                  </tr>
                </thead>
                <tbody>
                  {data.events.map((event, index) => (
                    // biome-ignore lint/suspicious/noArrayIndexKey: events are append-only and have no identity
                    <tr key={index}>
                      <td className="num muted" title={fmt.dateTime(event.at)}>
                        {fmt.time(new Date(event.at).getTime())}
                        <div className="cell-sub">{fmt.ago(event.at, now)}</div>
                      </td>
                      <td>
                        <JobState job={{ state: event.state, conclusion: null }} />
                      </td>
                      <td className="muted">{event.message ?? <span className="faint">—</span>}</td>
                      <td className="mono muted" style={{ fontSize: 12.5 }}>
                        {event.runner ?? <span className="faint">—</span>}
                      </td>
                      <td className="mono" style={{ fontSize: 12.5 }}>
                        {event.sandboxId ? (
                          <Link to={`/sandboxes/${event.sandboxId}`} className="muted">
                            {event.sandboxId}
                          </Link>
                        ) : (
                          <span className="faint">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
          <div style={{ height: 20 }} />
        </>
      )}
      {UNFINISHED[data.state] ? (
        <Card title="Log">
          <Empty title="The log is available when the job completes">
            Follow it on <External href={data.htmlUrl}>GitHub</External>
            {live && data.sandboxId ? (
              <>
                {" "}
                or in the{" "}
                <Link to={`/sandboxes/${data.sandboxId}?tab=terminal`}>sandbox terminal</Link>.
              </>
            ) : (
              "."
            )}
          </Empty>
        </Card>
      ) : (
        <JobLog id={id} />
      )}
    </div>
  );
}

/** Class of a GitHub log line by its workflow command marker. */
function lineClass(text: string): string {
  if (text.includes("##[error]")) return " ci-log__line--error";
  if (text.includes("##[warning]")) return " ci-log__line--warning";
  if (text.includes("##[group]") || text.includes("##[section]")) return " ci-log__line--group";
  if (text.includes("[command]")) return " ci-log__line--command";
  return "";
}

/** GitHub prefixes each line with an RFC 3339 timestamp (7 fractional digits). */
const STAMP = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z) ?/;

function JobLog({ id }: { id: string }) {
  const log = usePoll(`ci-log|${id}`, (signal) => ci.jobLogs(id, signal), 0);
  const [input, setInput] = useState("");
  const [stamps, setStamps] = useState(false);
  const needle = input.trim().toLowerCase();

  const lines = useMemo(
    () =>
      (log.data ?? "")
        .replace(/\n$/, "")
        .split("\n")
        .map((raw, index) => {
          const match = STAMP.exec(raw);
          return {
            no: index + 1,
            time: match?.[1] ?? null,
            text: match ? raw.slice(match[0].length) : raw,
          };
        }),
    [log.data],
  );
  const shown = needle ? lines.filter((l) => l.text.toLowerCase().includes(needle)) : lines;

  const download = () => {
    const url = URL.createObjectURL(new Blob([log.data ?? ""], { type: "text/plain" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `job-${id}.log`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const unavailable = log.error instanceof ApiError && log.error.status === 409;

  return (
    <Card flush title="Log" subtitle="GitHub's log of this job">
      <div className="toolbar">
        <label className="search">
          <Icon name="search" />
          <span className="visually-hidden">Search log</span>
          <input
            className="input"
            type="search"
            name="log-search"
            autoComplete="off"
            spellCheck={false}
            placeholder="Search lines…"
            value={input}
            onChange={(event) => setInput(event.currentTarget.value)}
          />
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={stamps}
            onChange={(event) => setStamps(event.currentTarget.checked)}
          />
          Timestamps
        </label>
        <span className="topbar__spacer" />
        {log.data !== undefined && (
          <span className="faint num">
            {needle ? `${fmt.int(shown.length)} of ` : ""}
            {fmt.int(lines.length)} lines
          </span>
        )}
        <Button
          variant="ghost"
          size="sm"
          icon="download"
          aria-label="Download log"
          disabled={log.data === undefined}
          onClick={download}
        />
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={log.refresh}
        />
      </div>
      {log.error ? (
        <Empty title={unavailable ? "Log not available" : "Could not load the log"}>
          {log.error.message}
        </Empty>
      ) : log.data === undefined ? (
        <Empty title="Loading…" />
      ) : shown.length === 0 ? (
        <Empty title={needle ? "No lines match" : "The log is empty"} />
      ) : (
        <div className="ci-log">
          {shown.map((line) => (
            <div key={line.no} className={`ci-log__line${lineClass(line.text)}`}>
              <span className="ci-log__no">{line.no}</span>
              <span>
                {stamps && line.time && (
                  <span className="ci-log__time">{fmt.time(new Date(line.time).getTime())} </span>
                )}
                {line.text}
              </span>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

function Eyebrow() {
  return (
    <div className="page__eyebrow">
      <Link to="/ci">CI</Link>
      <Icon name="chevron" size={12} />
      <Link to="/ci?tab=jobs">Jobs</Link>
      <Icon name="chevron" size={12} />
    </div>
  );
}
