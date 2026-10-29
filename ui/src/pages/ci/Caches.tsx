import { useState } from "react";
import { ci } from "../../api.ts";
import { Badge, Button, Card, confirmAction, Empty, ErrorNote } from "../../components.tsx";
import { fmt, shortId } from "../../format.ts";
import { attempt, usePoll } from "../../hooks.ts";
import { Icon } from "../../icons.tsx";
import { Link, useRoute } from "../../router.tsx";

/** Deletes a repository's cache entries (keys starting with `keyPrefix`) after confirmation. */
async function deleteCaches(repo: string, keyPrefix = ""): Promise<boolean> {
  const ok = await confirmAction({
    title: keyPrefix
      ? `Delete ${repo} Caches Starting “${keyPrefix}”?`
      : `Delete All ${repo} Caches?`,
    body: (
      <p>
        Jobs miss these entries until they save them again. Entries still uploading are dropped too.
      </p>
    ),
    action: "Delete Caches",
    danger: true,
  });
  return ok && attempt("Caches deleted", () => ci.deleteCaches(repo, keyPrefix));
}

/** CI cache usage per repository; one repository's entries with `?cache=owner/name`. */
export function CachesPanel() {
  const route = useRoute();
  const selected = route.query.get("cache");
  return selected ? <CacheEntries repo={selected} /> : <CacheRepos />;
}

function CacheRepos() {
  const caches = usePoll("ci-caches", ci.caches, 30_000);
  const rows = caches.data?.repos ?? [];

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <span className="muted">
          Actions caches of vmon jobs, stored on the nodes. Unused entries expire after 7 days.
        </span>
        <span className="topbar__spacer" />
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={caches.refresh}
        />
      </div>
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={caches.error} />
      </div>
      {rows.length === 0 ? (
        <Empty title={caches.loading ? "Loading…" : "No caches"}>
          {!caches.loading && "Jobs on vmon runners store their actions/cache entries here."}
        </Empty>
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Repository</th>
                <th className="num">Entries</th>
                <th className="num">Size</th>
                <th>Last Used</th>
                <th className="shrink">
                  <span className="visually-hidden">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.repoId}>
                  <td>
                    <Link
                      to={`/ci?tab=caches&cache=${encodeURIComponent(r.repo)}`}
                      className="row-link"
                    >
                      {r.repo}
                    </Link>
                  </td>
                  <td className="num">{fmt.int(r.entries)}</td>
                  <td className="num">
                    {fmt.bytes(r.sizeBytes)}
                    <div className="cell-sub">of {fmt.bytes(r.budgetBytes)}</div>
                  </td>
                  <td>
                    {r.lastUsedAt ? (
                      <span title={fmt.dateTime(r.lastUsedAt)}>{fmt.ago(r.lastUsedAt)}</span>
                    ) : (
                      <span className="faint">—</span>
                    )}
                  </td>
                  <td className="shrink">
                    <Button
                      variant="ghost"
                      size="sm"
                      icon="trash"
                      aria-label={`Delete all caches of ${r.repo}`}
                      onClick={async () => {
                        if (await deleteCaches(r.repo)) caches.refresh();
                      }}
                    />
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

function CacheEntries({ repo }: { repo: string }) {
  const cache = usePoll(`ci-cache|${repo}`, (signal) => ci.cache(repo, signal), 15_000);
  const [search, setSearch] = useState("");
  const needle = search.trim();
  const entries = (cache.data?.entries ?? []).filter((e) => e.key.startsWith(needle));
  const summary = cache.data?.repo;

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <Link to="/ci?tab=caches" className="muted">
          All repositories
        </Link>
        <strong>{repo}</strong>
        <label className="search">
          <Icon name="search" />
          <span className="visually-hidden">Filter by key prefix</span>
          <input
            className="input"
            type="search"
            name="cache-key-prefix"
            autoComplete="off"
            spellCheck={false}
            placeholder="Key prefix, e.g. cargo-…"
            value={search}
            onChange={(event) => setSearch(event.currentTarget.value)}
          />
        </label>
        <span className="topbar__spacer" />
        {summary && (
          <span className="faint num" aria-live="polite">
            {fmt.int(summary.entries)} entries · {fmt.bytes(summary.sizeBytes)} of{" "}
            {fmt.bytes(summary.budgetBytes)}
          </span>
        )}
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={cache.refresh}
        />
        <Button
          variant="danger"
          size="sm"
          icon="trash"
          disabled={entries.length === 0}
          onClick={async () => {
            if (await deleteCaches(summary?.repo ?? repo, needle)) cache.refresh();
          }}
        >
          {needle ? "Delete Matching" : "Delete All"}
        </Button>
      </div>
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={cache.error} />
      </div>
      {entries.length === 0 ? (
        <Empty
          title={cache.loading ? "Loading…" : needle ? "No entries match" : "No cache entries"}
        />
      ) : (
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Key</th>
                <th>Scope</th>
                <th className="num">Size</th>
                <th>Last Used</th>
                <th>Nodes</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <tr key={e.id}>
                  <td style={{ wordBreak: "break-all" }}>
                    <span className="mono">{e.key}</span>
                    {e.state === "uploading" && <Badge tone="info">uploading</Badge>}
                    <div className="cell-sub mono" title={e.version}>
                      version {shortId(e.version)} · job{" "}
                      <Link to={`/ci/jobs/${e.jobId}`}>{e.jobId}</Link>
                    </div>
                  </td>
                  <td className="mono">{e.scope}</td>
                  <td className="num">{fmt.bytes(e.sizeBytes)}</td>
                  <td>
                    <span title={fmt.dateTime(e.lastUsedAt)}>{fmt.ago(e.lastUsedAt)}</span>
                  </td>
                  <td className="mono">
                    {e.holders.length === 0 ? (
                      <span className="faint">—</span>
                    ) : (
                      e.holders.map(shortId).join(", ")
                    )}
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
