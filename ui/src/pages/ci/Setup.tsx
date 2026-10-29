// GitHub App setup: the configured app, or the two ways to configure one
// (GitHub's manifest flow, or the credentials of an existing app).

import { type FormEvent, useState } from "react";
import { type CiApp, ci } from "../../api.ts";
import { Button, Card, CopyButton, confirmAction, Detail } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { attempt, toast } from "../../hooks.ts";
import { ButtonLink, External } from "./shared.tsx";

/** Posts the manifest to GitHub as a top-level form submission (GitHub's manifest flow). */
function submitManifest(action: string, manifest: string): void {
  const form = document.createElement("form");
  form.method = "post";
  form.action = action;
  form.hidden = true;
  const field = document.createElement("input");
  field.type = "hidden";
  field.name = "manifest";
  field.value = manifest;
  form.append(field);
  document.body.append(form);
  form.submit();
}

export function AppCard({
  app,
  dashboardUrl,
  onChanged,
}: {
  app: CiApp;
  dashboardUrl: string;
  onChanged: () => void;
}) {
  const remove = async () => {
    const ok = await confirmAction({
      title: `Remove ${app.name}?`,
      body: (
        <p>
          vmon forgets the app's credentials and stops accepting its webhooks; queued jobs are no
          longer picked up. The app itself stays on GitHub: delete it there in its settings.
        </p>
      ),
      action: "Remove App",
      danger: true,
    });
    if (ok && (await attempt(`${app.name} removed`, () => ci.deleteApp()))) onChanged();
  };
  return (
    <Card
      title="GitHub App"
      subtitle="Receives workflow_job webhooks and registers one just-in-time runner per job"
      actions={
        <>
          <ButtonLink href={app.installUrl} variant="primary">
            Install on GitHub
          </ButtonLink>
          <Button variant="danger" icon="trash" onClick={() => void remove()}>
            Remove
          </Button>
        </>
      }
    >
      <dl className="kv">
        <Detail label="Name">
          <External href={app.htmlUrl} className="row-link">
            {app.name}
          </External>
        </Detail>
        <Detail label="Owner">{app.owner}</Detail>
        <Detail label="App ID">
          <span className="mono">{app.appId}</span>
          <CopyButton value={String(app.appId)} label="Copy app ID" />
        </Detail>
        <Detail label="Slug">
          <span className="mono">{app.slug}</span>
        </Detail>
        <Detail label="Webhook URL">
          {app.webhookUrl ? (
            <span className="mono truncate" title={app.webhookUrl} style={{ fontSize: 12.5 }}>
              {app.webhookUrl}
            </span>
          ) : (
            <span className="faint">not set</span>
          )}
        </Detail>
        <Detail label="Dashboard URL">
          <span className="mono truncate" title={dashboardUrl} style={{ fontSize: 12.5 }}>
            {dashboardUrl}
          </span>
        </Detail>
        <Detail label="Configured">
          <span title={fmt.dateTime(app.createdAt)}>{fmt.ago(app.createdAt)}</span>
        </Detail>
      </dl>
    </Card>
  );
}

export function CreateAppCard() {
  const [webhookUrl, setWebhookUrl] = useState(`${window.location.origin}/ci/github/webhook`);
  const [org, setOrg] = useState("");
  const [name, setName] = useState("");
  const [isPublic, setIsPublic] = useState(false);
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    try {
      const { action, manifest } = await ci.manifest({
        // GitHub sends the browser back to `<dashboardUrl>/ci/connect` and `/ci/install`.
        dashboardUrl: `${window.location.origin}/dashboard`,
        webhookUrl: webhookUrl.trim(),
        org: org.trim() || undefined,
        public: isPublic,
        name: name.trim() || undefined,
      });
      submitManifest(action, manifest);
    } catch (error) {
      toast(error instanceof Error ? error.message : String(error), "error");
      setBusy(false);
    }
  };

  return (
    <Card
      title="Create a GitHub App"
      subtitle="GitHub creates the app from a manifest and sends you back here with its credentials"
    >
      <form className="form-grid" onSubmit={(event) => void submit(event)}>
        <label className="field span-2">
          <span className="field__label">Webhook URL</span>
          <input
            className="input mono"
            name="webhook-url"
            type="url"
            autoComplete="off"
            spellCheck={false}
            value={webhookUrl}
            onChange={(event) => setWebhookUrl(event.currentTarget.value)}
            required
          />
          <span className="field__hint">
            Public HTTPS address GitHub delivers webhooks to: this driver's{" "}
            <code>/ci/github/webhook</code>, or a relay that forwards there.
          </span>
        </label>
        <label className="field">
          <span className="field__label">Organization</span>
          <input
            className="input mono"
            name="org"
            autoComplete="off"
            spellCheck={false}
            placeholder="Personal account…"
            value={org}
            onChange={(event) => setOrg(event.currentTarget.value)}
          />
          <span className="field__hint">Owner of the app; empty creates it on your account.</span>
        </label>
        <label className="field">
          <span className="field__label">App name</span>
          <input
            className="input"
            name="app-name"
            autoComplete="off"
            spellCheck={false}
            placeholder="vmon CI…"
            value={name}
            onChange={(event) => setName(event.currentTarget.value)}
          />
          <span className="field__hint">Unique on GitHub; you can rename it there later.</span>
        </label>
        <div className="field span-2">
          <label className="check">
            <input
              type="checkbox"
              checked={isPublic}
              onChange={(event) => setIsPublic(event.currentTarget.checked)}
            />
            Public app (other accounts can install it)
          </label>
        </div>
        <div className="span-2">
          <Button
            type="submit"
            variant="accent"
            icon="external"
            busy={busy}
            disabled={!webhookUrl.trim()}
          >
            Create GitHub App
          </Button>
        </div>
      </form>
    </Card>
  );
}

export function ManualAppCard({ onChanged }: { onChanged: () => void }) {
  const [appId, setAppId] = useState("");
  const [privateKey, setPrivateKey] = useState("");
  const [webhookSecret, setWebhookSecret] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    const ok = await attempt("GitHub App configured", () =>
      ci.setApp({ appId: Number(appId), privateKey: privateKey.trim(), webhookSecret }),
    );
    setBusy(false);
    if (ok) onChanged();
  };

  return (
    <Card
      title="Use an existing app"
      subtitle="Credentials are checked against GitHub, then stored encrypted in the cluster"
    >
      <form className="form-grid" onSubmit={(event) => void submit(event)}>
        <label className="field">
          <span className="field__label">App ID</span>
          <input
            className="input mono"
            name="app-id"
            inputMode="numeric"
            pattern="[0-9]+"
            autoComplete="off"
            value={appId}
            onChange={(event) => setAppId(event.currentTarget.value)}
            required
          />
        </label>
        <label className="field">
          <span className="field__label">Webhook secret</span>
          <input
            className="input mono"
            name="webhook-secret"
            type="password"
            autoComplete="off"
            value={webhookSecret}
            onChange={(event) => setWebhookSecret(event.currentTarget.value)}
            required
          />
        </label>
        <label className="field span-2">
          <span className="field__label">Private key</span>
          <textarea
            className="input textarea"
            name="private-key"
            rows={6}
            spellCheck={false}
            placeholder="-----BEGIN RSA PRIVATE KEY-----…"
            value={privateKey}
            onChange={(event) => setPrivateKey(event.currentTarget.value)}
            required
          />
          <span className="field__hint">The app's PEM file, generated in its GitHub settings.</span>
        </label>
        <div className="span-2">
          <Button
            type="submit"
            variant="primary"
            busy={busy}
            disabled={!appId || !privateKey.trim() || !webhookSecret}
          >
            Save App
          </Button>
        </div>
      </form>
    </Card>
  );
}
