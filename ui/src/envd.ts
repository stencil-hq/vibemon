// Browser client for a sandbox's envd (process + filesystem Connect services,
// `/files`) through the driver's ingress: same-origin requests routed by the
// `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers and authorized with the
// sandbox's envd access token. Connect JSON codec; server streams are read
// from fetch bodies (5-byte envelopes), the same transport the E2B JS SDK
// uses in browsers.

import { ApiError, errorFromResponse } from "./api.ts";

const ENVD_PORT = "49983";
const FLAG_END_STREAM = 0x02;

/** Which sandbox to reach and as whom (`user` unset = the template's default user). */
export interface EnvdTarget {
  sandboxID: string;
  token: string;
  user?: string;
}

function headers(target: EnvdTarget, extra: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {
    "E2b-Sandbox-Id": target.sandboxID,
    "E2b-Sandbox-Port": ENVD_PORT,
    "X-Access-Token": target.token,
    ...extra,
  };
  if (target.user) out.Authorization = `Basic ${btoa(`${target.user}:`)}`;
  return out;
}

async function unary<T>(
  target: EnvdTarget,
  method: string,
  request: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(`/${method}`, {
    method: "POST",
    headers: headers(target, {
      "Content-Type": "application/json",
      "Connect-Protocol-Version": "1",
    }),
    body: JSON.stringify(request),
    signal,
  });
  if (!res.ok) throw await errorFromResponse(res);
  return (await res.json()) as T;
}

/** Server-streaming Connect call; yields decoded messages until end-of-stream. */
async function* serverStream<T>(
  target: EnvdTarget,
  method: string,
  request: unknown,
  signal: AbortSignal,
): AsyncGenerator<T> {
  const payload = new TextEncoder().encode(JSON.stringify(request));
  const body = new Uint8Array(5 + payload.length);
  new DataView(body.buffer).setUint32(1, payload.length);
  body.set(payload, 5);
  const res = await fetch(`/${method}`, {
    method: "POST",
    headers: headers(target, {
      "Content-Type": "application/connect+json",
      "Connect-Protocol-Version": "1",
      "Keepalive-Ping-Interval": "30",
    }),
    body,
    signal,
  });
  if (!res.ok || !res.body) throw await errorFromResponse(res);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = new Uint8Array(0);
  try {
    for (;;) {
      while (buffer.length >= 5) {
        const flags = buffer[0] ?? 0;
        const length = new DataView(buffer.buffer, buffer.byteOffset).getUint32(1);
        if (buffer.length < 5 + length) break;
        const message = JSON.parse(decoder.decode(buffer.subarray(5, 5 + length))) as unknown;
        buffer = buffer.slice(5 + length);
        if (flags & FLAG_END_STREAM) {
          const end = message as { error?: { code?: string; message?: string } };
          if (end.error)
            throw new ApiError(500, end.error.message ?? end.error.code ?? "stream failed");
          return;
        }
        yield message as T;
      }
      const { done, value } = await reader.read();
      if (done) {
        if (buffer.length > 0) throw new ApiError(502, "stream ended mid-message");
        return;
      }
      const next = new Uint8Array(buffer.length + value.length);
      next.set(buffer);
      next.set(value, buffer.length);
      buffer = next;
    }
  } finally {
    reader.releaseLock();
  }
}

/** Standard base64 of raw bytes (protobuf `bytes` in JSON). */
export function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary);
}

export function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) out[i] = binary.charCodeAt(i);
  return out;
}

// --------------------------------------------------------------- process

export interface ProcessEvent {
  event?: {
    start?: { pid: number };
    data?: { stdout?: string; stderr?: string; pty?: string };
    end?: { exitCode?: number; exited?: boolean; status?: string; error?: string };
    keepalive?: Record<string, never>;
  };
}

export interface PtySize {
  cols: number;
  rows: number;
}

/** Login shell: bash when the image has it, else sh. */
const SHELL = "if command -v bash >/dev/null 2>&1; then exec bash -il; else exec sh -il; fi";

export const processes = {
  /** Starts an interactive login shell on a PTY; events stream until it exits. */
  startShell: (target: EnvdTarget, size: PtySize, signal: AbortSignal) =>
    serverStream<ProcessEvent>(
      target,
      "process.Process/Start",
      {
        process: {
          cmd: "/bin/sh",
          args: ["-c", SHELL],
          envs: { TERM: "xterm-256color", COLORTERM: "truecolor", LANG: "C.UTF-8" },
        },
        pty: { size },
      },
      signal,
    ),
  sendPty: (target: EnvdTarget, pid: number, data: Uint8Array) =>
    unary(target, "process.Process/SendInput", {
      process: { pid },
      input: { pty: toBase64(data) },
    }),
  resize: (target: EnvdTarget, pid: number, size: PtySize) =>
    unary(target, "process.Process/Update", { process: { pid }, pty: { size } }),
  kill: (target: EnvdTarget, pid: number) =>
    unary(target, "process.Process/SendSignal", { process: { pid }, signal: "SIGNAL_SIGKILL" }),
};

// ------------------------------------------------------------ filesystem

export type FileType =
  | "FILE_TYPE_FILE"
  | "FILE_TYPE_DIRECTORY"
  | "FILE_TYPE_SYMLINK"
  | "FILE_TYPE_UNSPECIFIED";

export interface EntryInfo {
  name: string;
  type?: FileType;
  path: string;
  /** int64 → JSON string. */
  size?: string;
  permissions?: string;
  owner?: string;
  group?: string;
  modifiedTime?: string;
  symlinkTarget?: string;
}

function filesUrl(path: string): string {
  return `/files?${new URLSearchParams({ path })}`;
}

export const files = {
  stat: (target: EnvdTarget, path: string) =>
    unary<{ entry: EntryInfo }>(target, "filesystem.Filesystem/Stat", { path }).then(
      (r) => r.entry,
    ),
  list: (target: EnvdTarget, path: string, signal?: AbortSignal) =>
    unary<{ entries?: EntryInfo[] }>(
      target,
      "filesystem.Filesystem/ListDir",
      { path, depth: 1 },
      signal,
    ).then((r) => r.entries ?? []),
  makeDir: (target: EnvdTarget, path: string) =>
    unary(target, "filesystem.Filesystem/MakeDir", { path }),
  move: (target: EnvdTarget, source: string, destination: string) =>
    unary(target, "filesystem.Filesystem/Move", { source, destination }),
  remove: (target: EnvdTarget, path: string) =>
    unary(target, "filesystem.Filesystem/Remove", { path }),

  /** Whole file as a Blob (`GET /files`). */
  async download(target: EnvdTarget, path: string): Promise<Blob> {
    const res = await fetch(filesUrl(path), { headers: headers(target, {}) });
    if (!res.ok) throw await errorFromResponse(res);
    return res.blob();
  },

  /** First `limit` bytes of a file (single byte range) and whether more follow. */
  async head(
    target: EnvdTarget,
    path: string,
    limit: number,
  ): Promise<{ bytes: Uint8Array; truncated: boolean }> {
    const res = await fetch(filesUrl(path), {
      headers: headers(target, { Range: `bytes=0-${limit - 1}`, "Accept-Encoding": "identity" }),
    });
    if (!res.ok) throw await errorFromResponse(res);
    const bytes = new Uint8Array(await res.arrayBuffer());
    const total = Number(res.headers.get("content-range")?.split("/")[1] ?? bytes.length);
    return { bytes, truncated: res.status === 206 && total > bytes.length };
  },

  /** Raw-body upload to `path` (parents created); reports progress in [0, 1]. */
  upload(
    target: EnvdTarget,
    path: string,
    file: Blob,
    onProgress: (ratio: number) => void,
  ): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const xhr = new XMLHttpRequest();
    xhr.open("POST", filesUrl(path));
    const requestHeaders = headers(target, { "Content-Type": "application/octet-stream" });
    for (const name in requestHeaders) xhr.setRequestHeader(name, requestHeaders[name] ?? "");
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded / event.total);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else
        void errorFromResponse(new Response(xhr.responseText, { status: xhr.status })).then(reject);
    };
    xhr.onerror = () => reject(new ApiError(0, `upload of ${path} failed: network error`));
    xhr.send(file);
    return promise;
  },
};
