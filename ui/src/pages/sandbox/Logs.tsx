import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { api } from "../../api.ts";
import { Button, Card, Empty, ErrorNote, Segmented } from "../../components.tsx";
import { fmt } from "../../format.ts";
import { usePoll } from "../../hooks.ts";
import { Icon } from "../../icons.tsx";

type Level = "" | "debug" | "info" | "warn" | "error";

/** Console + envd log lines from the node's log store, newest at the bottom. */
export function LogsPanel({ teamID, sandboxID }: { teamID: string; sandboxID: string }) {
  const [level, setLevel] = useState<Level>("");
  const [input, setInput] = useState("");
  const [search, setSearch] = useState("");
  const [follow, setFollow] = useState(true);
  const scroller = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const timer = window.setTimeout(() => setSearch(input.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [input]);

  const logs = usePoll(
    `logs|${sandboxID}|${level}|${search}`,
    (signal) => api.logs(teamID, sandboxID, { level, search }, signal),
    follow ? 3000 : 0,
  );
  // `backward` pages newest-first; show them in reading order.
  const entries = [...(logs.data?.logs ?? [])].reverse();

  useLayoutEffect(() => {
    const element = scroller.current;
    if (follow && element && entries.length) element.scrollTop = element.scrollHeight;
  });

  return (
    <Card flush>
      <div className="toolbar" style={{ paddingTop: 14 }}>
        <label className="search">
          <Icon name="search" />
          <span className="visually-hidden">Search logs</span>
          <input
            className="input"
            type="search"
            name="log-search"
            autoComplete="off"
            spellCheck={false}
            placeholder="Search messages…"
            value={input}
            onChange={(event) => setInput(event.currentTarget.value)}
          />
        </label>
        <Segmented
          label="Minimum level"
          value={level}
          onChange={setLevel}
          options={[
            { value: "", label: "All" },
            { value: "info", label: "Info" },
            { value: "warn", label: "Warn" },
            { value: "error", label: "Error" },
          ]}
        />
        <span className="topbar__spacer" />
        <label className="check">
          <input
            type="checkbox"
            checked={follow}
            onChange={(event) => setFollow(event.currentTarget.checked)}
          />
          Follow
        </label>
        <Button
          variant="ghost"
          size="sm"
          icon="refresh"
          aria-label="Refresh"
          onClick={logs.refresh}
        />
      </div>
      <div style={{ padding: "0 18px" }}>
        <ErrorNote error={logs.error} />
      </div>
      {entries.length === 0 ? (
        <Empty title={logs.loading ? "Loading…" : "No log lines"}>
          {!logs.loading && (search || level) && "Nothing matches these filters."}
        </Empty>
      ) : (
        <div className="logs" ref={scroller}>
          {entries.map((entry, index) => {
            const fields = Object.entries(entry.fields ?? {})
              .filter(([key]) => key !== "logger")
              .map(([key, value]) => `${key}=${value}`)
              .join(" ");
            return (
              // biome-ignore lint/suspicious/noArrayIndexKey: log lines have no identity
              <div key={index} className="log-row">
                <span className="log-row__time" title={fmt.dateTime(entry.timestamp)}>
                  {fmt.time(new Date(entry.timestamp).getTime())}
                </span>
                <span className={`log-row__level--${entry.level}`}>{entry.level}</span>
                <span className="log-row__msg">
                  {entry.message}
                  {fields && <span className="log-row__fields"> {fields}</span>}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
