# Freestyle Compatibility

The TypeScript `@stencil-hq/vibemon/freestyle`, Python `vmon.freestyle`, and Go `sdk/go/freestyle` packages provide a Freestyle-shaped facade over vmon. Existing code that uses the Freestyle VM surface can usually port by swapping its import while keeping the VM object model; this is a compatibility subset, not an implementation of every Freestyle service.

## Quickstart

Each facade connects lazily. These examples create a VM, execute a command, write and read a file, fork the captured machine state, and delete every created VM.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
from vmon.freestyle import freestyle

created = freestyle.vms.create(image="alpine:latest")
vm = created.vm
try:
    result = vm.exec("printf hello")
    assert result.status_code == 0

    vm.fs.write_text_file("/tmp/message", "from vmon")
    assert vm.fs.read_text_file("/tmp/message") == "from vmon"

    forked = vm.fork(count=1)
    for fork in forked.forks:
        fork.vm.delete()
finally:
    vm.delete()
    freestyle.close()
```

</div>
<div data-sdk-language="go">

```go
package main

import (
    "context"
    "log"

    "github.com/stencil-hq/vibemon/sdk/go/freestyle"
)

func main() {
    ctx := context.Background()
    client := freestyle.New()
    defer client.Close()

    created, err := client.Vms.Create(ctx, &freestyle.CreateOptions{Image: "alpine:latest"})
    if err != nil {
        log.Fatal(err)
    }
    vm := created.Vm
    defer vm.Delete(ctx)

    result, err := vm.ExecCommand(ctx, "printf hello")
    if err != nil || result.StatusCode != 0 {
        log.Fatalf("exec: status=%d err=%v", result.StatusCode, err)
    }
    if err := vm.Fs.WriteTextFile(ctx, "/tmp/message", "from vmon"); err != nil {
        log.Fatal(err)
    }
    content, err := vm.Fs.ReadTextFile(ctx, "/tmp/message")
    if err != nil || content != "from vmon" {
        log.Fatalf("read: content=%q err=%v", content, err)
    }

    forked, err := vm.Fork(ctx, freestyle.ForkOptions{Count: 1})
    if err != nil {
        log.Fatal(err)
    }
    for _, fork := range forked.Forks {
        if err := fork.Vm.Delete(ctx); err != nil {
            log.Fatal(err)
        }
    }
}
```

</div>
<div data-sdk-language="typescript">

```ts
import { freestyle } from "@stencil-hq/vibemon/freestyle";

const { vm } = await freestyle.vms.create({ image: "alpine:latest" });
try {
  const result = await vm.exec("printf hello");
  if (result.statusCode !== 0) throw new Error(result.stderr ?? "exec failed");

  await vm.fs.writeTextFile("/tmp/message", "from vmon");
  const content = await vm.fs.readTextFile("/tmp/message");
  if (content !== "from vmon") throw new Error("unexpected file content");

  const { forks } = await vm.fork({ count: 1 });
  await Promise.all(forks.map(({ vm: fork }) => fork.delete()));
} finally {
  await vm.delete();
  await freestyle.close();
}
```

</div>
</div>

The default clients resolve the vmon endpoint from the normal environment and context configuration. TypeScript and Python also expose `Freestyle` constructors; Go uses `freestyle.New`. Their API-key and access-token options become vmon bearer tokens, and the Freestyle base URL becomes the vmon DSN.

## Background jobs over PTY sessions

PTY sessions live in the guest agent, not in a client attachment. They survive suspend/resume and forks. Detaching disconnects only the current client; attaching by the stable session ID replays retained scrollback before delivering live output.

The examples below launch a multi-step job with an environment variable and working directory, observe its first progress line, detach and reattach, verify that the first line is replayed, wait for a successful exit, and then close the retained session.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
from vmon.freestyle import PtySession, freestyle


def read_line(session: PtySession) -> str:
    buffered = bytearray()
    while b"\n" not in buffered:
        chunk = session.read(timeout=5)
        if not chunk:
            raise TimeoutError("timed out waiting for PTY output")
        buffered.extend(chunk)
    return bytes(buffered).splitlines()[0].decode()


created = freestyle.vms.create(image="alpine:latest")
vm = created.vm
session_id: str | None = None
try:
    session = vm.pty.open(
        exec='echo "$JOB_NAME step 1"; sleep 1; echo "$JOB_NAME step 2"; '
        'sleep 1; echo "$JOB_NAME done"',
        env={"JOB_NAME": "docs-job"},
        workdir="/tmp",
    )
    session_id = session.session_id

    progress = read_line(session)
    assert progress == "docs-job step 1"
    session.detach()

    attached = vm.pty.attach(session_id)
    replayed = read_line(attached)
    assert replayed == progress

    exit_code = attached.wait(timeout=10)
    assert exit_code == 0
    vm.pty.close(session_id)
    session_id = None
finally:
    if session_id is not None:
        vm.pty.close(session_id)
    vm.delete()
    freestyle.close()
```

</div>
<div data-sdk-language="go">

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "log"
    "strings"

    "github.com/stencil-hq/vibemon/sdk/go/freestyle"
)

func readLine(ctx context.Context, session *freestyle.PtySession) (string, error) {
    var buffered []byte
    for {
        event, err := session.Receive(ctx)
        if err != nil {
            return "", err
        }
        if event.Exit != nil {
            return "", fmt.Errorf("PTY exited before a progress line: %d", event.Exit.Code)
        }
        buffered = append(buffered, event.Data...)
        if newline := bytes.IndexByte(buffered, '\n'); newline >= 0 {
            return strings.TrimSuffix(string(buffered[:newline]), "\r"), nil
        }
    }
}

func main() {
    ctx := context.Background()
    client := freestyle.New()
    defer client.Close()

    created, err := client.Vms.Create(ctx, &freestyle.CreateOptions{Image: "alpine:latest"})
    if err != nil {
        log.Fatal(err)
    }
    vm := created.Vm
    defer vm.Delete(ctx)

    session, err := vm.Pty.Open(ctx, &freestyle.PtyOpenOptions{
        Exec:    `echo "$JOB_NAME step 1"; sleep 1; echo "$JOB_NAME step 2"; sleep 1; echo "$JOB_NAME done"`,
        Env:     map[string]string{"JOB_NAME": "docs-job"},
        Workdir: "/tmp",
    })
    if err != nil {
        log.Fatal(err)
    }
    sessionID := session.SessionID

    progress, err := readLine(ctx, session)
    if err != nil || progress != "docs-job step 1" {
        log.Fatalf("progress=%q err=%v", progress, err)
    }
    if err := session.Detach(ctx); err != nil {
        log.Fatal(err)
    }

    attached, err := vm.Pty.Attach(
        ctx,
        freestyle.PtyAttachOptions{SessionID: sessionID},
    )
    if err != nil {
        log.Fatal(err)
    }
    replayed, err := readLine(ctx, attached)
    if err != nil || replayed != progress {
        log.Fatalf("replay=%q err=%v", replayed, err)
    }

    exit, err := attached.Wait(ctx)
    if err != nil || exit.Code != 0 {
        log.Fatalf("exit=%d err=%v", exit.Code, err)
    }
    if _, err := vm.Pty.Close(ctx, sessionID); err != nil {
        log.Fatal(err)
    }
}
```

</div>
<div data-sdk-language="typescript">

```ts
import { freestyle } from "@stencil-hq/vibemon/freestyle";

function lineReader() {
  const decoder = new TextDecoder();
  let buffered = "";
  const queued: string[] = [];
  const waiting: Array<(line: string) => void> = [];

  return {
    onData(data: Uint8Array): void {
      buffered += decoder.decode(data, { stream: true });
      const complete = buffered.split(/\r?\n/);
      buffered = complete.pop() ?? "";
      for (const line of complete) {
        const resolve = waiting.shift();
        if (resolve) resolve(line);
        else queued.push(line);
      }
    },
    readLine(): Promise<string> {
      const line = queued.shift();
      return line === undefined
        ? new Promise((resolve) => waiting.push(resolve))
        : Promise.resolve(line);
    },
  };
}

const { vm } = await freestyle.vms.create({ image: "alpine:latest" });
let sessionId: string | undefined;
try {
  const firstOutput = lineReader();
  const session = await vm.pty.open({
    exec:
      'echo "$JOB_NAME step 1"; sleep 1; echo "$JOB_NAME step 2"; ' +
      'sleep 1; echo "$JOB_NAME done"',
    env: { JOB_NAME: "docs-job" },
    workdir: "/tmp",
    onData: firstOutput.onData,
  });
  const id = session.sessionId;
  sessionId = id;

  const progress = await firstOutput.readLine();
  if (progress !== "docs-job step 1") throw new Error(`unexpected progress: ${progress}`);
  session.detach();

  const replayOutput = lineReader();
  let resolveExit!: (exitCode: number) => void;
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve;
  });
  const attached = await vm.pty.attach({
    sessionId: id,
    onData: replayOutput.onData,
    onExit: resolveExit,
  });
  const replayed = await replayOutput.readLine();
  if (replayed !== progress) throw new Error(`unexpected replay: ${replayed}`);

  const exitCode = await exited;
  if (exitCode !== 0 || attached.readyState !== 3) {
    throw new Error(`PTY exited with status ${exitCode}`);
  }
  await vm.pty.close({ sessionId: id });
  sessionId = undefined;
} finally {
  if (sessionId !== undefined) await vm.pty.close({ sessionId });
  await vm.delete();
  await freestyle.close();
}
```

</div>
</div>

## Provided surface

Names below use the TypeScript spelling. Python uses `snake_case`; Go exports the corresponding capitalized methods and fields.

| Freestyle surface | vmon backing and semantics |
| --- | --- |
| `vms.create` | Creates a network-enabled sandbox, or restores a full-VM snapshot when `snapshotId` is set. It returns a bound VM handle and stable VM ID. |
| `vms.list` | Lists reachable sandboxes and maps their observed lifecycle states into Freestyle VM states and counts. |
| `vms.get` | Fetches a sandbox by stable ID and returns a bound VM handle. The facades also expose an unfetched `ref` helper. |
| `vms.delete` / `vm.delete` | Terminates the sandbox and permanently removes its record. A missing record is accepted. |
| `vms.snapshots.list` / `get` | Lists ready vmon snapshot names or resolves one name. The filters accepted by `list` are no-ops because vmon retains ready snapshots here. |
| `vm.exec` | Runs the command through `sh -c` and captures stdout, stderr, and the exit status. Supplying `terminal` uses PTY-context execution instead. |
| `vm.fs.readFile` / `writeFile` | Reads or writes raw guest file bytes through the guest file service. |
| `vm.fs.readTextFile` / `writeTextFile` | Reads or writes UTF-8 guest text. |
| `vm.fs.readDir` / `mkdir` / `remove` | Lists a directory, creates a directory and its parents, or removes a path. `recursive` is a vmon extension for non-empty trees. |
| `vm.fs.exists` / `stat` | Checks existence without hiding non-`not_found` failures, or returns guest metadata, mode, type, size, and modification time. |
| `vm.stop` | Stops execution while retaining disk and VM identity. In-memory state is discarded. |
| `vm.start` | Replaces and rearms the idle policy before waking the VM when `idleTimeoutSeconds` is supplied, preventing a stale policy from racing resume. Omission preserves the current policy and `null` disables it. A stopped record cold-boots from its retained disk. |
| `vm.resize` | Replaces CPU or memory and can grow, but not shrink, the root disk. Running VMs stop and cold-boot; stopped VMs use the new shape on their next start. Disk growth is transactional through ext4 growth, and a resize or reboot failure restores the original disk and shape. |
| `vm.snapshot` | Captures disk, processes, and memory as a full-VM snapshot. Restoring it resumes that captured execution state. |
| `vm.fork` | Takes a full-VM snapshot, atomically creates the requested copies, and retains the base snapshot. |
| `vm.pty.open` | Opens a guest-agent-owned persistent terminal and returns a stable session ID plus an attached stream. |
| `vm.pty.attach` / session `detach` | Reattaches by stable session ID, replaying retained scrollback before live output. Detaching closes only the client stream and leaves the guest process running. Attaching to a suspended VM resumes it first. |
| `vm.pty.list` / `close` | Lists active and recently exited sessions, or SIGKILLs and removes a session. |
| `vm.exec({ terminal })` | Runs a captured sibling command through `sh -c` with the terminal session leader's working directory and environment. It does not write into the interactive stream. |
| `vpc.create` / `list` / `delete` | Creates and lists routed vmon VPCs, or deletes a VPC with no attached sandboxes. Listing and deletion are vmon extensions to the facade. |
| `nics` | Maps one requested routed NIC onto the sandbox create specification. vmon supports one NIC, and the VPC must already exist. |
| `persistence: { type: "persistent" }` | Retains stored state without storage-GC eviction. |
| `persistence: { type: "sticky", priority }` | Retains stored state, but storage GC may evict sticky VMs in ascending priority. An omitted priority becomes 5 and facade values are constrained to 0–10. |
| `persistence: { type: "ephemeral" }` | Discards stored state when the VM stops or suspends. |
| `idleTimeoutSeconds` | Reclaims a VM after the configured seconds without qualifying network activity. A disabled value maps to zero. |
| `activityThresholdBytes` | Sets the raw guest-NIC byte count per sampling interval that still qualifies as idle. |

PTY sessions live in the guest agent rather than in a client attachment. They survive client disconnect and sandbox suspend/resume, and memory-preserving forks inherit them; stopping the sandbox ends them.

## Create option mapping

| Freestyle option | vmon create or restore field |
| --- | --- |
| `idleTimeoutSeconds` | `idle_timeout_secs`; omission inherits the daemon default and a disabled value becomes `0`. The facade also sets `timeout_secs: 0`, so there is no wall-clock lease. |
| `persistence` | `persistence`, preserving `persistent`, `sticky`, or `ephemeral`; sticky priority defaults to 5 and is constrained to 0–10. |
| `activityThresholdBytes` | `activity_threshold_bytes`. |
| `nics` | `nics`; `vpc`, `ipv4`, and `default` are forwarded after enforcing a single NIC with `mode: "routed"`. Guest networking is enabled by default even when no VPC NIC is supplied. |
| `cpu` | `cpus`, in whole vCPUs. |
| `memory` | `memory`/`memory_mib`, converted from Freestyle GB to MiB by multiplying by 1024. |
| `storage` | `disk_mb`, converted from Freestyle GB to MiB by multiplying by 1024. |
| `snapshotId` | Snapshot restore rather than image creation. CPU, memory, and storage cannot be overridden because the snapshot fixes them. A full-VM restore resumes captured processes and memory rather than fresh-booting. |

The vmon extensions `image`, `template`, `env`, `workdir`, `tags`, and `name` map directly to the corresponding sandbox create or restore values.

## Divergences from Freestyle

- `vms.create` always returns an empty `domains` collection. It also accepts the vmon image, template, sizing, environment, workdir, and tag options described above.
- `persistence`, `activityThresholdBytes`, and terminal execution use vmon-native semantics. VPCs and NICs work only on Linux hosts, only one NIC is supported, and its mode must be `routed`; macOS rejects VPC operations.
- Raw file reads return `Uint8Array` in TypeScript and `bytes` in Python, not a Node `Buffer`. Go returns `[]byte`.
- File-stat owner and group and snapshot `createdAt` are optional or absent because vmon does not report them. PTY open and attach option shapes are a simplified subset; TypeScript reconnect uses bounded exponential backoff.
- The unbacked `git`, `domains`, `identities`, `dns`, `serverless`, and `cron` namespaces are absent, as are `whoami` and raw `fetch`. Code that uses them fails at compile time or import/attribute lookup rather than at a remote call.
- A resize that must cold-boot a VM created with secrets returns vmon `busy` after the daemon restarts, because that server process no longer holds the secret material.
- The facades validate CPU and memory resize values client-side as positive powers of two before sending the vmon resize request.

For the underlying resource model and lifecycle, see [Sandboxes](sandboxes.md). For full-state restore and fork behavior, see [Snapshots](snapshots.md). Facade and daemon failures use the vmon error model documented in [Error Codes](../reference/errors.md).
