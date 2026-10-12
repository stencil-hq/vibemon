// Lifecycle actions shared by the sandbox list and detail views: confirm when
// destructive, toast the outcome, report success to refresh the caller.

import { api } from "../api.ts";
import { confirmAction } from "../components.tsx";
import { attempt } from "../hooks.ts";

/** Lifetime given to a resumed sandbox (seconds), matching the SDK default. */
export const RESUME_TIMEOUT_SECS = 900;

export interface SandboxRef {
  sandboxID: string;
  teamID: string;
}

export function pauseSandbox(ref: SandboxRef): Promise<boolean> {
  return attempt(`Paused ${ref.sandboxID}`, () => api.pause(ref.teamID, ref.sandboxID));
}

export function resumeSandbox(ref: SandboxRef): Promise<boolean> {
  return attempt(`Resumed ${ref.sandboxID}`, () =>
    api.resume(ref.teamID, ref.sandboxID, RESUME_TIMEOUT_SECS),
  );
}

export async function killSandbox(ref: SandboxRef): Promise<boolean> {
  const ok = await confirmAction({
    title: "Kill sandbox?",
    body: (
      <p>
        <span className="mono">{ref.sandboxID}</span> stops immediately and its disk, memory, and
        any paused state are deleted. This cannot be undone.
      </p>
    ),
    action: "Kill sandbox",
    danger: true,
  });
  return ok && attempt(`Killed ${ref.sandboxID}`, () => api.kill(ref.teamID, ref.sandboxID));
}

export function setSandboxTimeout(
  ref: SandboxRef,
  seconds: number,
  label: string,
): Promise<boolean> {
  return attempt(`${ref.sandboxID} now expires ${label}`, () =>
    api.setTimeout(ref.teamID, ref.sandboxID, seconds),
  );
}
