// Pieces shared by the CI views: job state badges, durations, and links out to GitHub.

import type { AnchorHTMLAttributes, ReactNode } from "react";
import type { CiJob, CiJobState } from "../../api.ts";
import { Badge, type Tone } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { Icon, type IconName } from "../../icons.tsx";

/** States whose job has not finished on GitHub yet (no log to show). */
export const UNFINISHED: Record<CiJobState, boolean> = {
  queued: true,
  waiting: true,
  starting: true,
  running: true,
  held: false,
  completed: false,
  refused: false,
  failed: false,
};

const STATE_TONE: Record<CiJobState, Tone> = {
  queued: "muted",
  waiting: "warn",
  starting: "info",
  running: "live",
  held: "warn",
  completed: "ok",
  refused: "error",
  failed: "error",
};

/** Tone of a GitHub conclusion (`success`, `failure`, `cancelled`, …). */
function conclusionTone(conclusion: string | null): Tone {
  if (conclusion === "success") return "ok";
  if (conclusion === "failure" || conclusion === "timed_out" || conclusion === "startup_failure")
    return "error";
  return "muted";
}

/** State badge; completed jobs show GitHub's conclusion instead of the bare state. */
export function JobState({ job }: { job: Pick<CiJob, "state" | "conclusion"> }) {
  if (job.state === "completed" && job.conclusion) {
    return (
      <Badge tone={conclusionTone(job.conclusion)}>{job.conclusion.replaceAll("_", " ")}</Badge>
    );
  }
  return <Badge tone={STATE_TONE[job.state]}>{job.state}</Badge>;
}

/** Queue wait: final when known, else counted live until the runner starts. */
export function waitMs(job: CiJob, now: number): number | null {
  if (job.waitMs !== null) return job.waitMs;
  // Refused and vmon-failed jobs that never got a runner have no wait to count.
  if (!job.startedAt && !UNFINISHED[job.state]) return null;
  const end = job.startedAt ? new Date(job.startedAt).getTime() : now;
  return end - new Date(job.queuedAt).getTime();
}

/** Run time: final when known, else counted live while the job runs. */
export function runMs(job: CiJob, now: number): number | null {
  if (job.runMs !== null) return job.runMs;
  if (!job.startedAt) return null;
  const end = job.completedAt ? new Date(job.completedAt).getTime() : now;
  return end - new Date(job.startedAt).getTime();
}

export function Duration({ ms }: { ms: number | null }) {
  return ms === null ? <span className="faint">—</span> : fmt.duration(ms);
}

/** `vmon-large/from=omp-ci/debug` style summary of the parsed runs-on label. */
export function sizeLabel(job: CiJob): string {
  if (!job.size) return "—";
  return [job.size, job.from && `from=${job.from}`, job.debug && "debug"].filter(Boolean).join("/");
}

type ExternalProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "target" | "rel"> & {
  href: string;
  children: ReactNode;
};

/** Link to github.com in a new tab, marked as leaving the dashboard. */
export function External({ children, style, ...rest }: ExternalProps) {
  return (
    <a
      target="_blank"
      rel="noreferrer"
      style={{ display: "inline-flex", alignItems: "center", gap: 4, ...style }}
      {...rest}
    >
      {children}
      <Icon name="external" size={12} />
    </a>
  );
}

/** Anchor drawn as a button (GitHub install/settings pages open in a new tab). */
export function ButtonLink({
  href,
  icon = "external",
  variant = "secondary",
  children,
}: {
  href: string;
  icon?: IconName;
  variant?: "primary" | "secondary" | "accent" | "ghost";
  children: ReactNode;
}) {
  return (
    <a className={`btn btn--${variant} btn--md`} href={href} target="_blank" rel="noreferrer">
      <Icon name={icon} />
      {children}
    </a>
  );
}

export function repoUrl(repo: string): string {
  return `https://github.com/${repo}`;
}
