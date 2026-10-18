// Locale-aware display formatting for counts, sizes, durations, and times.

import type { ClusterMember } from "./api.ts";

const integer = new Intl.NumberFormat();
const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const percent = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 0 });
const clock = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" });
const clockSeconds = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
const dateTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" });
const date = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });
const relative = new Intl.RelativeTimeFormat(undefined, { numeric: "auto", style: "narrow" });

export const fmt = {
  int: (value: number) => integer.format(value),
  num: (value: number) => decimal.format(value),
  pct: (ratio: number) => percent.format(Number.isFinite(ratio) ? ratio : 0),
  clock: (ms: number) => clock.format(ms),
  time: (ms: number) => clockSeconds.format(ms),
  dateTime: (value: string | number) => dateTime.format(new Date(value)),

  /** Binary size from bytes (`512 MiB`, `1.5 GiB`). */
  bytes(value: number): string {
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let size = Math.max(0, value);
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit += 1;
    }
    return `${unit === 0 ? integer.format(size) : decimal.format(size)} ${units[unit]}`;
  },

  /** Binary size from MiB, the unit of every capacity field in the API. */
  mib: (value: number) => fmt.bytes(value * 1024 * 1024),

  /** Compact duration (`45s`, `12m`, `3h 5m`, `2d 4h`). */
  duration(ms: number): string {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m`;
    const h = Math.floor(m / 60);
    if (h < 24) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`;
    const d = Math.floor(h / 24);
    return h % 24 ? `${d}d ${h % 24}h` : `${d}d`;
  },

  /** Relative to now (`5 min. ago`, `in 3 hr.`); a plain date beyond 30 days. */
  ago(value: string | number, now = Date.now()): string {
    const diff = new Date(value).getTime() - now;
    const abs = Math.abs(diff);
    if (abs >= 30 * 86_400_000) return date.format(new Date(value));
    if (abs < 60_000) return relative.format(Math.round(diff / 1000), "second");
    if (abs < 3_600_000) return relative.format(Math.round(diff / 60_000), "minute");
    if (abs < 86_400_000) return relative.format(Math.round(diff / 3_600_000), "hour");
    return relative.format(Math.round(diff / 86_400_000), "day");
  },
};

/** First 8 characters of a long opaque ID (node IDs, UUIDs). */
export function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 8) : id;
}

/** Display name of a cluster member: its hostname, else its short node ID. */
export function memberName(member: Pick<ClusterMember, "id" | "hostname">): string {
  return member.hostname || shortId(member.id);
}
