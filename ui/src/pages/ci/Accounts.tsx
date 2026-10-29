import { type FormEvent, useState } from "react";
import { type CiAccount, type CiPublicPolicy, ci, type TeamView } from "../../api.ts";
import { Badge, Button, Card, confirmAction, Empty, Modal } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { attempt, toast } from "../../hooks.ts";
import { Link } from "../../router.tsx";
import { External } from "./shared.tsx";

/** One-line summary of an account's public-repository policy. */
export function policySummary(policy: CiPublicPolicy): string {
  if (!policy.enabled) return "Off";
  if (policy.anyone) return "Anyone";
  const n = policy.users.length;
  return n === 0 ? "Nobody listed" : `${n} user${n === 1 ? "" : "s"}`;
}

/** Links an installation to a team, or unlinks it (`""`) after confirmation. */
export async function setAccountTeam(account: CiAccount, team: string): Promise<boolean> {
  if (team === "") {
    const ok = await confirmAction({
      title: `Unlink ${account.login}?`,
      body: (
        <p>
          New jobs from <strong>{account.login}</strong> are refused until it is linked to a team
          again. Jobs already running keep their sandboxes.
        </p>
      ),
      action: "Unlink",
      danger: true,
    });
    return ok && attempt(`${account.login} unlinked`, () => ci.unlink(account.installationId));
  }
  return attempt(`${account.login} linked`, () => ci.link(account.installationId, team));
}

/** Team picker: every team plus "Not linked". */
export function TeamSelect({
  teams,
  value,
  onChange,
  label,
  allowNone = true,
}: {
  teams: TeamView[];
  value: string;
  onChange: (team: string) => void;
  label: string;
  allowNone?: boolean;
}) {
  return (
    <select
      className="select"
      style={{ width: 200 }}
      aria-label={label}
      value={value}
      onChange={(event) => onChange(event.currentTarget.value)}
    >
      {allowNone ? <option value="">Not linked</option> : <option value="">Choose a team…</option>}
      {teams.map((t) => (
        <option key={t.teamID} value={t.teamID}>
          {t.name}
          {t.system ? " (system)" : ""}
        </option>
      ))}
    </select>
  );
}

export function AccountsPanel({
  accounts,
  teams,
  installUrl,
  loading,
  onChanged,
}: {
  accounts: CiAccount[];
  teams: TeamView[];
  installUrl: string | null;
  loading: boolean;
  onChanged: () => void;
}) {
  const [editing, setEditing] = useState<CiAccount | null>(null);
  const [syncing, setSyncing] = useState(false);

  const sync = async () => {
    setSyncing(true);
    const ok = await attempt("Installations synced from GitHub", () => ci.syncAccounts());
    setSyncing(false);
    if (ok) onChanged();
  };

  return (
    <Card
      flush
      title="Accounts"
      subtitle="GitHub users and organizations that installed the app, and the team their jobs run in"
      actions={
        <Button size="sm" icon="refresh" busy={syncing} onClick={() => void sync()}>
          Sync from GitHub
        </Button>
      }
    >
      {accounts.length === 0 ? (
        <Empty title={loading ? "Loading…" : "No installations"}>
          {!loading &&
            (installUrl ? (
              <>
                <External href={installUrl}>Install the app</External> on a GitHub account, then
                link it to a team here.
              </>
            ) : (
              "Configure the GitHub App first."
            ))}
        </Empty>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Account</th>
                <th>Team</th>
                <th>Public repositories</th>
                <th className="right">Installed</th>
              </tr>
            </thead>
            <tbody>
              {accounts.map((a) => (
                <tr key={a.installationId}>
                  <td>
                    <Link
                      to={`/ci?tab=repos&account=${encodeURIComponent(a.login)}`}
                      className="row-link"
                    >
                      {a.login}
                    </Link>{" "}
                    <Badge>{a.type === "Organization" ? "org" : "user"}</Badge>{" "}
                    {a.suspended && <Badge tone="error">suspended</Badge>}{" "}
                    {!a.team && <Badge tone="warn">unlinked</Badge>}
                    <div className="cell-sub mono">installation {a.installationId}</div>
                  </td>
                  <td>
                    <TeamSelect
                      label={`Team of ${a.login}`}
                      teams={teams}
                      value={a.team?.id ?? ""}
                      onChange={(team) =>
                        void setAccountTeam(a, team).then((ok) => ok && onChanged())
                      }
                    />
                  </td>
                  <td>
                    <span className={a.public.enabled ? "" : "muted"}>
                      {policySummary(a.public)}
                    </span>
                    {a.public.enabled && !a.public.anyone && a.public.users.length > 0 && (
                      <div className="cell-sub mono truncate" style={{ maxWidth: 260 }}>
                        {a.public.users.join(", ")}
                      </div>
                    )}
                    <div>
                      <Button
                        variant="ghost"
                        size="sm"
                        icon="rename"
                        style={{ marginLeft: -8 }}
                        onClick={() => setEditing(a)}
                      >
                        Edit Policy
                      </Button>
                    </div>
                  </td>
                  <td className="right num muted" title={fmt.dateTime(a.createdAt)}>
                    {fmt.ago(a.createdAt)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {editing && (
        <PolicyDialog
          key={editing.installationId}
          account={editing}
          onClose={() => setEditing(null)}
          onSaved={() => {
            setEditing(null);
            onChanged();
          }}
        />
      )}
    </Card>
  );
}

/** Splits a free-form list of GitHub logins (commas, spaces, newlines; `@` optional). */
function parseUsers(text: string): string[] {
  return [
    ...new Set(
      text
        .split(/[\s,]+/)
        .map((u) => u.replace(/^@/, "").trim())
        .filter(Boolean),
    ),
  ];
}

function PolicyDialog({
  account,
  onClose,
  onSaved,
}: {
  account: CiAccount;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [enabled, setEnabled] = useState(account.public.enabled);
  const [anyone, setAnyone] = useState(account.public.anyone);
  const [users, setUsers] = useState(account.public.users.join("\n"));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await ci.setPublic(account.installationId, { enabled, anyone, users: parseUsers(users) });
      toast(`Public repository policy of ${account.login} saved`);
      onSaved();
    } catch (err) {
      // 400 `invalid` names the GitHub users that do not exist; keep the dialog open.
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      title={`Public repositories of ${account.login}`}
      open
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" form="ci-policy" variant="primary" busy={busy}>
            Save Policy
          </Button>
        </>
      }
    >
      <form id="ci-policy" className="stack" onSubmit={(event) => void submit(event)}>
        <p className="muted" style={{ fontSize: 13 }}>
          Workflows of public repositories can be triggered by anyone who opens a pull request.
          Private repositories always run.
        </p>
        <label className="check">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event) => setEnabled(event.currentTarget.checked)}
          />
          Run jobs of public repositories
        </label>
        <label className="check" style={{ opacity: enabled ? 1 : 0.5 }}>
          <input
            type="checkbox"
            checked={anyone}
            disabled={!enabled}
            onChange={(event) => setAnyone(event.currentTarget.checked)}
          />
          For anyone, including pull requests from forks
        </label>
        <label className="field">
          <span className="field__label">Allowed GitHub users</span>
          <textarea
            className="input textarea"
            name="users"
            rows={4}
            spellCheck={false}
            placeholder={"octocat\nhubot"}
            disabled={!enabled || anyone}
            value={users}
            onChange={(event) => setUsers(event.currentTarget.value)}
          />
          <span className="field__hint">
            One login per line (or comma-separated). A run is allowed when both the actor and the
            triggering actor are listed.
          </span>
        </label>
        {error && (
          <p className="error-note" role="alert" style={{ margin: 0 }}>
            {error}
          </p>
        )}
      </form>
    </Modal>
  );
}
