import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";
import { useEffect, useRef, useState } from "react";
import { Button, Status, type Tone } from "../../components.tsx";
import { type EnvdTarget, fromBase64, processes } from "../../envd.ts";

type Phase = "connecting" | "connected" | "exited" | "failed";

const PHASE: Record<Phase, { tone: Tone; label: string }> = {
  connecting: { tone: "warn", label: "Connecting…" },
  connected: { tone: "live", label: "Connected" },
  exited: { tone: "muted", label: "Exited" },
  failed: { tone: "error", label: "Disconnected" },
};

const THEME = {
  background: "#050505",
  foreground: "#ededed",
  cursor: "#ffb224",
  cursorAccent: "#050505",
  selectionBackground: "rgba(255, 178, 36, 0.3)",
  black: "#1a1a1a",
  brightBlack: "#5c5c5c",
  red: "#f4644a",
  brightRed: "#f7836e",
  green: "#4ade80",
  brightGreen: "#8df0b0",
  yellow: "#f5b04a",
  brightYellow: "#fbd08f",
  blue: "#5b8cff",
  brightBlue: "#8ab0ff",
  magenta: "#a86af4",
  brightMagenta: "#c9a4ff",
  cyan: "#44cfff",
  brightCyan: "#8ae3ff",
  white: "#d4d4d4",
  brightWhite: "#ffffff",
};

/**
 * Interactive login shell in the sandbox over envd's process service: a PTY
 * process started with `Start` (output streamed back), keystrokes sent with
 * `SendInput`, size changes with `Update`. Leaving the tab kills the shell.
 */
export function TerminalPanel({ target }: { target: EnvdTarget }) {
  const host = useRef<HTMLDivElement>(null);
  const term = useRef<Terminal | null>(null);
  const [user, setUser] = useState("");
  const [phase, setPhase] = useState<Phase>("connecting");
  const [session, setSession] = useState(0);
  const { sandboxID, token } = target;

  useEffect(() => {
    const element = host.current;
    if (!element) return;
    const terminal = new Terminal({
      fontFamily:
        '"Berkeley Mono", "Geist Mono", "JetBrains Mono", ui-monospace, "SF Mono", Menlo, monospace',
      fontSize: 13,
      lineHeight: 1.25,
      cursorBlink: true,
      scrollback: 5000,
      allowProposedApi: false,
      theme: THEME,
    });
    const fit = new FitAddon();
    terminal.loadAddon(fit);
    terminal.open(element);
    fit.fit();
    term.current = terminal;
    const observer = new ResizeObserver(() => fit.fit());
    observer.observe(element);
    return () => {
      observer.disconnect();
      term.current = null;
      terminal.dispose();
    };
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: `session` is the reconnect trigger
  useEffect(() => {
    const terminal = term.current;
    if (!terminal) return;
    const envd: EnvdTarget = { sandboxID, token, user: user || undefined };
    const controller = new AbortController();
    const encoder = new TextEncoder();
    let pid: number | null = null;
    let pending: Uint8Array[] = [];
    let sending = Promise.resolve();
    let resizeTimer: number | undefined;

    const send = (bytes: Uint8Array) => {
      if (pid === null) {
        pending.push(bytes);
        return;
      }
      const target = pid;
      sending = sending
        .then(() => processes.sendPty(envd, target, bytes))
        .then(
          () => undefined,
          () => undefined,
        );
    };
    const input = terminal.onData((data) => send(encoder.encode(data)));
    const binary = terminal.onBinary((data) => send(Uint8Array.from(data, (c) => c.charCodeAt(0))));
    const resize = terminal.onResize(({ cols, rows }) => {
      window.clearTimeout(resizeTimer);
      resizeTimer = window.setTimeout(() => {
        if (pid !== null) void processes.resize(envd, pid, { cols, rows }).catch(() => undefined);
      }, 80);
    });

    terminal.reset();
    terminal.write(
      `\x1b[2m[vibemon] opening a shell in ${sandboxID}${user ? ` as ${user}` : ""}…\x1b[0m\r\n`,
    );
    setPhase("connecting");

    void (async () => {
      try {
        for await (const message of processes.startShell(
          envd,
          { cols: terminal.cols, rows: terminal.rows },
          controller.signal,
        )) {
          const event = message.event;
          if (event?.start) {
            pid = event.start.pid;
            setPhase("connected");
            terminal.focus();
            const queued = pending;
            pending = [];
            for (const bytes of queued) send(bytes);
          } else if (event?.data?.pty) {
            terminal.write(fromBase64(event.data.pty));
          } else if (event?.end) {
            const code = event.end.exitCode ?? 0;
            terminal.write(
              `\r\n\x1b[2m[vibemon] shell exited (${event.end.status ?? `code ${code}`})\x1b[0m\r\n`,
            );
            pid = null;
            setPhase("exited");
          }
        }
      } catch (error) {
        if (controller.signal.aborted) return;
        const message = error instanceof Error ? error.message : String(error);
        terminal.write(`\r\n\x1b[31m[vibemon] ${message}\x1b[0m\r\n`);
        setPhase("failed");
      }
    })();

    return () => {
      input.dispose();
      binary.dispose();
      resize.dispose();
      window.clearTimeout(resizeTimer);
      if (pid !== null) void processes.kill(envd, pid).catch(() => undefined);
      controller.abort();
    };
  }, [sandboxID, token, user, session]);

  const { tone, label } = PHASE[phase];
  return (
    <div className="terminal">
      <div className="terminal__bar">
        <Status tone={tone}>{label}</Status>
        <span className="topbar__spacer" />
        <label className="visually-hidden" htmlFor="terminal-user">
          Run as
        </label>
        <select
          id="terminal-user"
          className="select"
          value={user}
          onChange={(event) => setUser(event.currentTarget.value)}
        >
          <option value="">Default user</option>
          <option value="root">root</option>
        </select>
        <Button size="sm" icon="refresh" onClick={() => setSession((n) => n + 1)}>
          {phase === "connected" ? "Restart" : "Reconnect"}
        </Button>
      </div>
      <div className="terminal__host" ref={host} />
    </div>
  );
}
