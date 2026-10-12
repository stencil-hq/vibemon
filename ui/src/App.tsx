import { createContext, type FormEvent, useContext, useEffect, useState } from "react";
import { adminToken, api, type ClusterView, onAuthChange, signIn, signOut } from "./api.ts";
import { Button, DialogHost, StatusDot, Toaster } from "./components.tsx";
import { shortId } from "./format.ts";
import { type Poll, usePoll } from "./hooks.ts";
import { Icon, type IconName } from "./icons.tsx";
import { NodeDetailPage } from "./pages/NodeDetail.tsx";
import { NodesPage } from "./pages/Nodes.tsx";
import { OverviewPage } from "./pages/Overview.tsx";
import { SandboxDetailPage } from "./pages/SandboxDetail.tsx";
import { SandboxesPage } from "./pages/Sandboxes.tsx";
import { TeamsPage } from "./pages/Teams.tsx";
import { Link, useRoute } from "./router.tsx";

const ClusterContext = createContext<Poll<ClusterView> | null>(null);

/** Cluster view shared by the shell and pages, polled every 5 s (the sampler tick). */
export function useCluster(): Poll<ClusterView> {
  const poll = useContext(ClusterContext);
  if (!poll) throw new Error("useCluster outside <App>");
  return poll;
}

export function App() {
  const [signedIn, setSignedIn] = useState(() => adminToken() !== "");
  useEffect(() => onAuthChange(setSignedIn), []);
  return (
    <>
      {signedIn ? <Console /> : <Login />}
      <DialogHost />
      <Toaster />
    </>
  );
}

function BrandMark() {
  return (
    <svg className="brand__mark" viewBox="0 0 32 32" aria-hidden="true">
      <defs>
        <linearGradient id="brand-gradient" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#ffb224" />
          <stop offset="1" stopColor="#ff5d3a" />
        </linearGradient>
      </defs>
      <rect width="32" height="32" rx="8" fill="#0f0f10" />
      <rect
        x="0.5"
        y="0.5"
        width="31"
        height="31"
        rx="7.5"
        fill="none"
        stroke="rgb(255 255 255 / 0.14)"
      />
      <path
        d="M8 11l8 11 8-11"
        fill="none"
        stroke="url(#brand-gradient)"
        strokeWidth="3.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function Brand() {
  return (
    <span className="brand">
      <BrandMark />
      vibemon
      <span className="brand__slash" aria-hidden="true">
        /
      </span>
      <span className="brand__app">admin</span>
    </span>
  );
}

function Login() {
  const [token, setToken] = useState("");
  const [remember, setRemember] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    const res = await fetch("/admin/cluster", { headers: { "X-Admin-Token": token.trim() } }).catch(
      () => null,
    );
    setBusy(false);
    if (res?.ok) {
      signIn(token.trim(), remember);
    } else if (res?.status === 401) {
      setError("That token was rejected. Use the value of --admin-token (VMON_ADMIN_TOKEN).");
    } else {
      setError(res ? `The driver answered ${res.status}.` : "The driver is unreachable.");
    }
  };

  return (
    <main className="login">
      <div className="login__card">
        <Brand />
        <h1 className="login__title">Sign in</h1>
        <p className="login__lede">
          Paste the driver's admin token. It is sent only to this server, as{" "}
          <code>X-Admin-Token</code>.
        </p>
        <form className="login__form" onSubmit={(event) => void submit(event)}>
          <label className="field">
            <span className="field__label">Admin token</span>
            <input
              className="input mono"
              name="admin-token"
              type="password"
              autoComplete="current-password"
              spellCheck={false}
              placeholder="VMON_ADMIN_TOKEN…"
              value={token}
              onChange={(event) => setToken(event.currentTarget.value)}
              required
              // biome-ignore lint/a11y/noAutofocus: the only control on a sign-in page
              autoFocus
            />
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={remember}
              onChange={(event) => setRemember(event.currentTarget.checked)}
            />
            Remember on this device
          </label>
          {error && (
            <p className="error-note" role="alert">
              {error}
            </p>
          )}
          <Button type="submit" variant="accent" busy={busy} disabled={token.trim() === ""}>
            Continue
          </Button>
        </form>
      </div>
    </main>
  );
}

interface NavItem {
  to: string;
  label: string;
  icon: IconName;
  section: string;
  count?: number;
}

function Console() {
  const cluster = usePoll("cluster", api.cluster, 5000);
  const route = useRoute();
  const [section = "", id] = route.segments;
  const members = cluster.data?.members ?? [];
  const nodes = members.filter((m) => m.node);
  const latest = cluster.data?.history.at(-1);
  const local = members.find((m) => m.id === cluster.data?.localID);

  const nav: NavItem[] = [
    { to: "/", label: "Overview", icon: "overview", section: "" },
    {
      to: "/sandboxes",
      label: "Sandboxes",
      icon: "sandbox",
      section: "sandboxes",
      count: latest?.sandboxes,
    },
    {
      to: "/nodes",
      label: "Nodes",
      icon: "node",
      section: "nodes",
      count: nodes.length || undefined,
    },
    { to: "/teams", label: "Teams", icon: "team", section: "teams" },
  ];

  let page: React.ReactNode;
  if (section === "") page = <OverviewPage />;
  else if (section === "sandboxes" && id) page = <SandboxDetailPage id={id} />;
  else if (section === "sandboxes") page = <SandboxesPage />;
  else if (section === "nodes" && id) page = <NodeDetailPage id={id} />;
  else if (section === "nodes") page = <NodesPage />;
  else if (section === "teams") page = <TeamsPage />;
  else
    page = (
      <div className="page">
        <h1 className="page__title">Not found</h1>
        <p className="page__lede">
          Nothing lives at this address. <Link to="/">Back to the overview</Link>.
        </p>
      </div>
    );

  return (
    <ClusterContext.Provider value={cluster}>
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <header className="topbar">
        <Link to="/" aria-label="vibemon admin overview">
          <Brand />
        </Link>
        <span className="topbar__spacer" />
        <span className="topbar__meta">
          {cluster.error ? (
            <>
              <StatusDot tone="error" />
              Disconnected
            </>
          ) : cluster.data ? (
            <>
              <StatusDot tone="live" pulse />
              <span title={cluster.data.clusterID || undefined}>
                {cluster.data.clusterID ? `cluster ${shortId(cluster.data.clusterID)}` : "cluster"}
              </span>
              {local && <span className="faint mono">v{local.version}</span>}
            </>
          ) : (
            "Connecting…"
          )}
        </span>
        <Button variant="ghost" size="sm" icon="logout" onClick={signOut}>
          Sign out
        </Button>
      </header>
      <nav className="sidebar" aria-label="Main">
        <div className="sidebar__heading">Platform</div>
        {nav.map((item) => (
          <Link
            key={item.to}
            to={item.to}
            className="nav-row"
            aria-current={item.section === section ? "page" : undefined}
          >
            <Icon name={item.icon} />
            {item.label}
            {item.count !== undefined && <span className="nav-row__count">{item.count}</span>}
          </Link>
        ))}
        <div className="sidebar__foot">
          Served by driver <span className="mono">{local ? shortId(local.id) : "…"}</span>
          <br />
          Charts keep this driver's last hour.
        </div>
      </nav>
      <main id="main" className="panel" key={`${section}/${id ?? ""}`}>
        {page}
      </main>
    </ClusterContext.Provider>
  );
}
