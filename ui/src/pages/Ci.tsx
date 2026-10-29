import { api, type CiStats, ci } from "../api.ts";
import { Card, Empty, ErrorNote, Stat, Tabs } from "../components.tsx";
import { fmt } from "../format.ts";
import { usePoll } from "../hooks.ts";
import { useRoute } from "../router.tsx";
import { AccountsPanel } from "./ci/Accounts.tsx";
import { JobsPanel } from "./ci/Jobs.tsx";
import { ReposPanel } from "./ci/Repos.tsx";
import { AppCard, CreateAppCard, ManualAppCard } from "./ci/Setup.tsx";

const TABS = [
  { id: "jobs", label: "Jobs", icon: "ci" },
  { id: "accounts", label: "Accounts", icon: "team" },
  { id: "repos", label: "Repositories", icon: "folder" },
  { id: "app", label: "GitHub App", icon: "link" },
] as const;

type TabId = (typeof TABS)[number]["id"];

export function CiPage() {
  const route = useRoute();
  const overview = usePoll("ci", ci.overview, 5000);
  const teams = usePoll("teams", api.teams, 30_000);
  const data = overview.data;
  const tabParam = route.query.get("tab");
  const fallback: TabId = data && !data.app ? "app" : "jobs";
  const tab: TabId = TABS.some((t) => t.id === tabParam) ? (tabParam as TabId) : fallback;

  return (
    <div className="page">
      <div className="page__head">
        <div>
          <h1 className="page__title">CI</h1>
          <p className="page__lede">
            GitHub Actions jobs with <code>runs-on: vmon</code> run in a fresh sandbox each, killed
            when the job ends.
          </p>
        </div>
      </div>
      <ErrorNote error={overview.error} />
      {data && data.problems.length > 0 && (
        <div className="warn-note" role="status">
          {data.problems.length === 1 ? (
            data.problems[0]
          ) : (
            <>
              Setup needs attention:
              <ul>
                {data.problems.map((p) => (
                  <li key={p}>{p}</li>
                ))}
              </ul>
            </>
          )}
        </div>
      )}

      {data?.app && <StatsRow stats={data.stats} />}

      <Tabs tabs={[...TABS]} active={tab} base="/ci" />

      {!data ? (
        <Card>
          <Empty title={overview.loading ? "Loading…" : "CI is unavailable"} />
        </Card>
      ) : tab === "jobs" ? (
        <JobsPanel accounts={data.accounts} />
      ) : tab === "accounts" ? (
        <AccountsPanel
          accounts={data.accounts}
          teams={teams.data ?? []}
          installUrl={data.app?.installUrl ?? null}
          loading={overview.loading}
          onChanged={overview.refresh}
        />
      ) : tab === "repos" ? (
        <ReposPanel accounts={data.accounts} />
      ) : data.app ? (
        <AppCard app={data.app} dashboardUrl={data.dashboardUrl} onChanged={overview.refresh} />
      ) : (
        <div className="grid-2" style={{ alignItems: "start" }}>
          <CreateAppCard />
          <ManualAppCard onChanged={overview.refresh} />
        </div>
      )}
    </div>
  );
}

function StatsRow({ stats }: { stats: CiStats }) {
  const finished = stats.succeeded + stats.failed + stats.cancelled;
  return (
    <div className="stats">
      <Stat
        label="Jobs"
        value={fmt.int(stats.jobs)}
        sub={<span title={`Since ${fmt.dateTime(stats.since)}`}>Last 7 days</span>}
      />
      <Stat
        label="Succeeded"
        value={fmt.int(stats.succeeded)}
        sub={
          finished ? `${fmt.pct(stats.succeeded / finished)} of finished jobs` : "No finished jobs"
        }
      />
      <Stat
        label="Failed"
        value={fmt.int(stats.failed)}
        sub={`${fmt.int(stats.cancelled)} cancelled`}
      />
      <Stat label="Refused" value={fmt.int(stats.refused)} sub="Never ran on vmon" />
      <Stat
        label="Queue wait p50"
        value={stats.waitP50Ms === null ? "—" : fmt.duration(stats.waitP50Ms)}
        sub={stats.waitP95Ms === null ? "p95 —" : `p95 ${fmt.duration(stats.waitP95Ms)}`}
      />
      <Stat
        label="Runner time"
        value={fmt.duration(stats.runSeconds * 1000)}
        sub="Sandbox time used by jobs"
      />
    </div>
  );
}
