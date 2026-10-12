import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";

export interface Poll<T> {
  data: T | undefined;
  error: Error | undefined;
  /** True until the first response (success or failure) arrives. */
  loading: boolean;
  /** Time of the last successful response (ms). */
  updatedAt: number | undefined;
  refresh: () => void;
}

/**
 * Calls `load` now and every `intervalMs` while the tab is visible; reloads
 * from scratch when `key` changes. In-flight requests are aborted on
 * unmount, key change, and overlap.
 */
export function usePoll<T>(
  key: string,
  load: (signal: AbortSignal) => Promise<T>,
  intervalMs: number,
): Poll<T> {
  const [state, setState] = useState<Omit<Poll<T>, "refresh">>({
    data: undefined,
    error: undefined,
    loading: true,
    updatedAt: undefined,
  });
  const loadRef = useRef(load);
  loadRef.current = load;
  const [tick, setTick] = useState(0);
  const refresh = useCallback(() => setTick((n) => n + 1), []);
  const loadedKey = useRef(key);

  useEffect(() => {
    void tick;
    if (loadedKey.current !== key) {
      loadedKey.current = key;
      setState({ data: undefined, error: undefined, loading: true, updatedAt: undefined });
    }
    let controller: AbortController | undefined;
    let timer: number | undefined;
    let stopped = false;
    const run = async () => {
      controller?.abort();
      controller = new AbortController();
      const { signal } = controller;
      try {
        const data = await loadRef.current(signal);
        if (!signal.aborted) {
          setState({ data, error: undefined, loading: false, updatedAt: Date.now() });
        }
      } catch (error) {
        if (!signal.aborted) {
          setState((prev) => ({
            ...prev,
            error: error instanceof Error ? error : new Error(String(error)),
            loading: false,
          }));
        }
      }
      if (!stopped && intervalMs > 0) timer = window.setTimeout(schedule, intervalMs);
    };
    const schedule = () => {
      if (document.visibilityState === "visible") void run();
      else timer = window.setTimeout(schedule, intervalMs);
    };
    const onVisible = () => {
      if (document.visibilityState === "visible") {
        window.clearTimeout(timer);
        void run();
      }
    };
    void run();
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      stopped = true;
      controller?.abort();
      window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [key, intervalMs, tick]);

  return { ...state, refresh };
}

/** Current time, re-rendered every `intervalMs` (countdowns, "ago" labels). */
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs]);
  return now;
}

// ----------------------------------------------------------------- toasts

export interface Toast {
  id: number;
  message: string;
  tone: "info" | "error";
}

let toasts: Toast[] = [];
let toastSeq = 0;
const toastListeners = new Set<() => void>();

function emitToasts(next: Toast[]): void {
  toasts = next;
  for (const listener of toastListeners) listener();
}

export function dismissToast(id: number): void {
  emitToasts(toasts.filter((t) => t.id !== id));
}

/** Shows a transient notification; errors stay twice as long. */
export function toast(message: string, tone: Toast["tone"] = "info"): void {
  toastSeq += 1;
  const id = toastSeq;
  emitToasts([...toasts, { id, message, tone }].slice(-4));
  window.setTimeout(() => dismissToast(id), tone === "error" ? 8000 : 4000);
}

export function useToasts(): Toast[] {
  return useSyncExternalStore(
    (listener) => {
      toastListeners.add(listener);
      return () => toastListeners.delete(listener);
    },
    () => toasts,
  );
}

/** Runs an action, toasting its outcome; returns whether it succeeded. */
export async function attempt(label: string, action: () => Promise<unknown>): Promise<boolean> {
  try {
    await action();
    toast(label);
    return true;
  } catch (error) {
    toast(error instanceof Error ? error.message : String(error), "error");
    return false;
  }
}
