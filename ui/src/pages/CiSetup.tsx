// Landing pages GitHub redirects the browser to: `/ci/connect` after the
// manifest flow created the app, `/ci/install` after the app was installed.

import { useState } from "react";
import { api, type CiAccount, type CiApp, ci, type TeamView } from "../api.ts";
import { Badge, Button, Card, Detail, Empty, ErrorNote } from "../components.tsx";
import { toast, usePoll } from "../hooks.ts";
import { Icon } from "../icons.tsx";
import { Link, navigate, useRoute } from "../router.tsx";
import { TeamSelect } from "./ci/Accounts.tsx";
import { ButtonLink, External } from "./ci/shared.tsx";

/**
 * GitHub's manifest code is single-use: one conversion per code, shared by
 * remounts (StrictMode, re-renders) so the page never redeems it twice.
 */
const conversions = new Map<string, Promise<CiApp>>();

function convertOnce(code: string, state: string): Promise<CiApp> {
  let pending = conversions.get(code);
  if (!pending) {
    pending = ci.convert(code, state);
    conversions.set(code, pending);
  }
  return pending;
}

export function CiConnectPage() {
  const route = useRoute();
  const code = route.query.get("code") ?? "";
  const state = route.query.get("state") ?? "";
  const result = usePoll(
    `ci-connect|${code}`,
    () => (code ? convertOnce(code, state) : Promise.reject(new Error("GitHub sent no code."))),
    0,
  );
  const app = result.data;

  return (
    <div className="page">
      <Eyebrow />
      <div className="page__head">
        <div>
          <h1 className="page__title">Connect GitHub App</h1>
          <p className="page__lede">Storing the credentials GitHub created for the new app.</p>
        </div>
      </div>
      {result.error ? (
        <Card>
          <Empty title="The app could not be connected">
            <p className="error-note" style={{ margin: "8px 0 12px" }}>
              {result.error.message}
            </p>
            The code from GitHub is single-use and expires after an hour.{" "}
            <Link to="/ci?tab=app">Start over</Link>.
          </Empty>
        </Card>
      ) : !app ? (
        <Card>
          <Empty title="Finishing setup…">
            Exchanging GitHub's code for the app's credentials.
          </Empty>
        </Card>
      ) : (
        <Card
          title={
            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <Icon name="check" />
              {app.name} is connected
            </span>
          }
          subtitle="Next, install it on the accounts whose workflows should run on vmon"
          actions={
            <ButtonLink href={app.installUrl} variant="primary">
              Install on GitHub
            </ButtonLink>
          }
        >
          <dl className="kv">
            <Detail label="App">
              <External href={app.htmlUrl} className="row-link">
                {app.slug}
              </External>
            </Detail>
            <Detail label="Owner">{app.owner}</Detail>
            <Detail label="App ID">
              <span className="mono">{app.appId}</span>
            </Detail>
            <Detail label="Webhook URL">
              <span className="mono truncate" style={{ fontSize: 12.5 }}>
                {app.webhookUrl ?? "—"}
              </span>
            </Detail>
          </dl>
          <p className="muted" style={{ marginTop: 18, fontSize: 13 }}>
            GitHub returns to this dashboard after the installation to link it to a team.{" "}
            <Link to="/ci?tab=app">Back to CI</Link>.
          </p>
        </Card>
      )}
    </div>
  );
}

export function CiInstallPage() {
  const route = useRoute();
  const installationId = Number(route.query.get("installation_id") ?? "");
  const setupAction = route.query.get("setup_action");
  const sync = usePoll(`ci-install|${installationId}`, () => ci.syncAccounts(), 0);
  const teams = usePoll("teams", api.teams, 30_000);
  const [linked, setLinked] = useState<CiAccount | null>(null);
  const account =
    linked ?? sync.data?.accounts.find((a) => a.installationId === installationId) ?? null;

  return (
    <div className="page">
      <Eyebrow />
      <div className="page__head">
        <div>
          <h1 className="page__title">Link installation</h1>
          <p className="page__lede">
            Jobs of an installation run as sandboxes of one team, within that team's limits.
          </p>
        </div>
      </div>
      {setupAction === "request" && (
        <p className="warn-note" role="status">
          GitHub asked an organization owner to approve this installation. Link it once it is
          approved, here or under CI › Accounts.
        </p>
      )}
      <ErrorNote error={sync.error} />
      {!Number.isSafeInteger(installationId) || installationId <= 0 ? (
        <Card>
          <Empty title="No installation given">
            GitHub links here with <code>installation_id</code>.{" "}
            <Link to="/ci?tab=accounts">See all accounts</Link>.
          </Empty>
        </Card>
      ) : !account ? (
        <Card>
          <Empty
            title={sync.loading ? "Syncing installations from GitHub…" : "Installation not found"}
          >
            {!sync.loading && (
              <>
                GitHub does not list installation <span className="mono">{installationId}</span> for
                this app. <Link to="/ci?tab=accounts">See all accounts</Link>.
              </>
            )}
          </Empty>
        </Card>
      ) : (
        <LinkCard
          key={account.installationId}
          account={account}
          teams={teams.data ?? []}
          onLinked={setLinked}
        />
      )}
    </div>
  );
}

function LinkCard({
  account,
  teams,
  onLinked,
}: {
  account: CiAccount;
  teams: TeamView[];
  onLinked: (account: CiAccount) => void;
}) {
  const [team, setTeam] = useState(account.team?.id ?? "");
  const [busy, setBusy] = useState(false);

  const link = async () => {
    setBusy(true);
    try {
      const updated = await ci.link(account.installationId, team);
      toast(`${account.login} linked to ${updated.team?.name ?? "the team"}`);
      onLinked(updated);
    } catch (error) {
      toast(error instanceof Error ? error.message : String(error), "error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card
      title={
        <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
          {account.login}
          <Badge>{account.type === "Organization" ? "org" : "user"}</Badge>
          {account.suspended && <Badge tone="error">suspended</Badge>}
        </span>
      }
      subtitle={`Installation ${account.installationId}`}
    >
      <dl className="kv">
        <Detail label="Linked team">
          {account.team ? (
            <Link to={`/sandboxes?team=${account.team.id}`} className="row-link">
              {account.team.name}
            </Link>
          ) : (
            <Badge tone="warn">not linked</Badge>
          )}
        </Detail>
        <Detail label="Public repositories">{account.public.enabled ? "Allowed" : "Off"}</Detail>
      </dl>
      <div className="toolbar" style={{ paddingTop: 18 }}>
        <TeamSelect label="Team" teams={teams} value={team} onChange={setTeam} allowNone={false} />
        <Button
          variant="primary"
          icon="link"
          busy={busy}
          disabled={!team || team === account.team?.id}
          onClick={() => void link()}
        >
          Link to Team
        </Button>
        {account.team && <Button onClick={() => navigate("/ci?tab=accounts")}>Done</Button>}
      </div>
    </Card>
  );
}

function Eyebrow() {
  return (
    <div className="page__eyebrow">
      <Link to="/ci">CI</Link>
      <Icon name="chevron" size={12} />
    </div>
  );
}
