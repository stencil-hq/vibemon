import { expect, test } from "bun:test";
import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { APIError, Client, MeshDriver } from "../src";
import { Freestyle } from "../src/freestyle";
import { SandboxService, SnapshotService, VpcService } from "../src/gen/vmon/v1/api_pb";

interface RecordedRpc {
  method: string;
  input: object;
}

interface FakeState {
  view: unknown;
  /** Per-method response overrides taking precedence over `view`. */
  views: Record<string, unknown>;
  rows: unknown[];
  snapshots: string[];
  exec: { code: bigint; stdout: Uint8Array; stderr: Uint8Array };
  file: Uint8Array;
  missingPaths: Set<string>;
  deniedPaths: Set<string>;
  vpcs: { id: string; name: string; cidr: string; createdAtUnixMillis: bigint }[];
}

/** In-memory vmon gRPC surface exercising the Freestyle facade mappings. */
function fakeVmon() {
  const rpcs: RecordedRpc[] = [];
  const state: FakeState = {
    view: {},
    views: {},
    rows: [],
    snapshots: [],
    exec: { code: 0n, stdout: new Uint8Array(), stderr: new Uint8Array() },
    file: new TextEncoder().encode("payload"),
    missingPaths: new Set(),
    deniedPaths: new Set(),
    vpcs: [],
  };
  const view = (method: string, input: object) => {
    rpcs.push({ method, input });
    return { json: JSON.stringify(state.views[method] ?? state.view) };
  };
  const router = createRouterTransport(({ service }) => {
    service(SandboxService, {
      create: (req) => view("Create", req),
      get: (req) => view("Get", req),
      list: (req) => {
        rpcs.push({ method: "List", input: req });
        return { sandboxesJson: state.rows.map((row) => JSON.stringify(row)) };
      },
      terminate: (req) => view("Terminate", req),
      stop: (req) => view("Stop", req),
      remove: (req) => view("Remove", req),
      suspend: (req) => view("Suspend", req),
      resume: (req) => view("Resume", req),
      resize: (req) => view("Resize", req),
      extend: (req) => view("Extend", req),
      setIdleTimeout: (req) => view("SetIdleTimeout", req),
      snapshot: (req) => view("Snapshot", req),
      execCapture: (req) => {
        rpcs.push({ method: "ExecCapture", input: req });
        return state.exec;
      },
      ptyExec: (req) => {
        rpcs.push({ method: "PtyExec", input: req });
        return state.exec;
      },
      fileRead: (req) => {
        rpcs.push({ method: "FileRead", input: req });
        return { data: state.file };
      },
      fileWrite: (req) => {
        rpcs.push({ method: "FileWrite", input: req });
        return {};
      },
      fileStat: (req) => {
        const path = String(Reflect.get(req, "path"));
        if (state.missingPaths.has(path)) throw new ConnectError("no such path", Code.NotFound);
        if (state.deniedPaths.has(path))
          throw new ConnectError("permission denied", Code.PermissionDenied);
        return view("FileStat", req);
      },
    });
    service(SnapshotService, {
      list: () => ({ snapshots: state.snapshots }),
      restore: (req) => view("Restore", req),
      fork: (req) => view("Fork", req),
    });
    service(VpcService, {
      create: (req) => {
        rpcs.push({ method: "VpcCreate", input: req });
        return {
          id: "vpc-1234abcd",
          name: req.name,
          cidr: req.cidr || "10.77.0.0/16",
          createdAtUnixMillis: 1n,
        };
      },
      list: () => {
        rpcs.push({ method: "VpcList", input: {} });
        return { vpcs: state.vpcs };
      },
      delete: (req) => {
        rpcs.push({ method: "VpcDelete", input: req });
        return {};
      },
    });
  });
  return { rpcs, state, transport: () => router };
}

function connectFreestyle() {
  const fake = fakeVmon();
  const client = new Client(
    new MeshDriver("http://node-a", { discover: false, transport: fake.transport }),
  );
  return { ...fake, freestyle: new Freestyle(client) };
}

function specOf(call: RecordedRpc | undefined): Record<string, unknown> {
  const raw = call === undefined ? undefined : Reflect.get(call.input, "specJson");
  return JSON.parse(String(raw));
}

test("create snapshots default and fully configured requests", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-default" };
  const defaultVm = await freestyle.vms.create();
  expect(defaultVm.vmId).toBe("vm-default");
  expect(specOf(rpcs.at(-1))).toEqual({ timeout_secs: 0, block_network: false });

  state.view = { id: "vm-full" };
  const created = await freestyle.vms.create({
    name: "workspace",
    template: "fs-snap-1",
    cpu: 8,
    memory: 16,
    storage: 80,
    idleTimeoutSeconds: null,
    activityThresholdBytes: 4096,
    persistence: { type: "sticky", priority: 99 },
    nics: [{ vpc: "vpc-1234abcd", mode: "routed", ipv4: "10.88.0.4", default: false }],
    env: { A: "1" },
    workdir: "/workspace",
    tags: { suite: "compat" },
  });
  expect(created.vmId).toBe("vm-full");
  expect(created.vm.vmId).toBe("vm-full");
  expect(created.domains).toEqual([]);
  expect(specOf(rpcs.at(-1))).toEqual({
    name: "workspace",
    template: "fs-snap-1",
    env: { A: "1" },
    workdir: "/workspace",
    tags: { suite: "compat" },
    timeout_secs: 0,
    block_network: false,
    idle_timeout_secs: 0,
    activity_threshold_bytes: 4096,
    persistence: { type: "sticky", priority: 10 },
    nics: [{ vpc: "vpc-1234abcd", ipv4: "10.88.0.4", default: false }],
    cpus: 8,
    memory: 16_384,
    disk_mb: 81_920,
  });
});

test("create rejects multiple NICs before contacting the daemon", async () => {
  const { rpcs, freestyle } = connectFreestyle();
  expect(
    freestyle.vms.create({
      nics: [
        { vpc: "vpc-1234abcd", mode: "routed", ipv4: true },
        { vpc: "vpc-deadbeef", mode: "routed", ipv4: "10.88.0.4" },
      ],
    }),
  ).rejects.toThrow("single NIC");
  expect(rpcs).toEqual([]);
});

test("VPC namespaces map create, list, and delete RPCs", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  const created = await freestyle.vpc.create({ name: "private", cidr: "10.88.0.0/24" });
  expect(created).toEqual({
    vpcId: "vpc-1234abcd",
    vpc: { vpcId: "vpc-1234abcd" },
  });
  expect(rpcs.at(-1)).toMatchObject({
    method: "VpcCreate",
    input: { name: "private", cidr: "10.88.0.0/24" },
  });
  state.vpcs = [
    {
      id: "vpc-1234abcd",
      name: "private",
      cidr: "10.88.0.0/24",
      createdAtUnixMillis: 1n,
    },
  ];
  expect(await freestyle.vpc.list()).toEqual({
    vpcs: [{ vpcId: "vpc-1234abcd", name: "private", cidr: "10.88.0.0/24" }],
  });
  await freestyle.vpc.delete({ vpcId: "vpc-1234abcd" });
  expect(rpcs.at(-1)).toMatchObject({
    method: "VpcDelete",
    input: { id: "vpc-1234abcd" },
  });
});

test("create from a snapshot restores and rejects resizing", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-3" };
  await freestyle.vms.create({
    snapshotId: "snap-1",
    name: "clone",
    idleTimeoutSeconds: 30,
    activityThresholdBytes: 512,
    persistence: { type: "ephemeral" },
  });
  const restore = rpcs.at(-1);
  expect(restore?.method).toBe("Restore");
  expect(Reflect.get(restore?.input ?? {}, "name")).toBe("snap-1");
  expect(JSON.parse(String(Reflect.get(restore?.input ?? {}, "bodyJson")))).toEqual({
    name: "clone",
    timeout_secs: 0,
    block_network: false,
    idle_timeout_secs: 30,
    activity_threshold_bytes: 512,
    persistence: { type: "ephemeral" },
  });
  expect(freestyle.vms.create({ snapshotId: "snap-1", memory: 4 })).rejects.toThrow(
    "fixed by the snapshot",
  );
});

test("exec wraps shell strings and decodes captured output", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-4" };
  const { vm } = await freestyle.vms.create();
  state.exec = {
    code: 3n,
    stdout: new TextEncoder().encode("out"),
    stderr: new TextEncoder().encode("err"),
  };
  const result = await vm.exec("echo out");
  expect(result).toEqual({ stdout: "out", stderr: "err", statusCode: 3 });
  expect(rpcs.at(-1)).toMatchObject({
    method: "ExecCapture",
    input: { id: "vm-4", exec: { cmd: ["sh", "-c", "echo out"] } },
  });
  await vm.exec({ command: "sleep 9", timeoutMs: 1_500 });
  expect(rpcs.at(-1)).toMatchObject({ input: { exec: { timeout: 1.5 } } });
  const terminalResult = await vm.exec({ command: "pwd", terminal: "term-1", timeoutMs: 2_000 });
  expect(terminalResult).toEqual({ stdout: "out", stderr: "err", statusCode: 3 });
  expect(rpcs.at(-1)).toMatchObject({
    method: "PtyExec",
    input: {
      id: "vm-4",
      sessionId: "term-1",
      command: "pwd",
      timeout: 2,
    },
  });
});

test("fs round-trips text and reports existence and stat", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-5" };
  const { vm } = await freestyle.vms.create();
  await vm.fs.writeTextFile("/tmp/hello.txt", "Hello");
  expect(rpcs.at(-1)).toMatchObject({
    method: "FileWrite",
    input: { id: "vm-5", path: "/tmp/hello.txt" },
  });
  state.file = new TextEncoder().encode("Hello");
  expect(await vm.fs.readTextFile("/tmp/hello.txt")).toBe("Hello");

  state.views.FileStat = { type: "dir", size: 4096, mode: 0o40755, mtime: 1_700_000_000 };
  expect(await vm.fs.stat("/tmp")).toEqual({
    size: 4096,
    isFile: false,
    isDirectory: true,
    isSymlink: false,
    permissions: "755",
    modified: new Date(1_700_000_000_000).toISOString(),
  });
  expect(await vm.fs.exists("/tmp")).toBe(true);
  state.missingPaths.add("/nope");
  expect(await vm.fs.exists("/nope")).toBe(false);
  state.deniedPaths.add("/root/secret");
  expect(vm.fs.exists("/root/secret")).rejects.toThrow(APIError);
});

test("stop calls Stop and start after stop calls Resume", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-6" };
  const { vm } = await freestyle.vms.create();
  state.views.Stop = { id: "vm-6", status: "stopped" };
  state.views.Get = { id: "vm-6", observed_state: "stopped" };
  await vm.stop();
  expect(rpcs.map((call) => call.method)).toContain("Stop");
  expect(rpcs.find((call) => call.method === "Stop")).toMatchObject({ input: { id: "vm-6" } });
  state.views.Resume = { id: "vm-6", status: "running" };
  await vm.start();
  expect(rpcs.at(-1)).toMatchObject({ method: "Resume", input: { id: "vm-6" } });
});

test("start sets the idle policy before resuming", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-6-idle" };
  const { vm } = await freestyle.vms.create();
  await vm.start();
  expect(rpcs.at(-1)?.method).toBe("Resume");
  await vm.start({ idleTimeoutSeconds: 900 });
  expect(rpcs.map((call) => call.method).slice(-2)).toEqual(["SetIdleTimeout", "Resume"]);
  expect(Reflect.get(rpcs.at(-2)?.input ?? {}, "idleTimeoutSecs")).toBe(900);
  await vm.start({ idleTimeoutSeconds: null });
  expect(rpcs.map((call) => call.method).slice(-2)).toEqual(["SetIdleTimeout", "Resume"]);
  expect(Reflect.get(rpcs.at(-2)?.input ?? {}, "idleTimeoutSecs")).toBe(0);
});

test("fork snapshots the VM and wraps every clone", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-7" };
  const { vm } = await freestyle.vms.create();
  state.views.Snapshot = { snapshot: "snap-vm-7" };
  state.views.Fork = { clones: [{ id: "fork-a" }, { id: "fork-b" }] };
  const { forks } = await vm.fork({ count: 2 });
  expect(forks.map((fork) => fork.vmId)).toEqual(["fork-a", "fork-b"]);
  expect(forks.map((fork) => fork.vm.vmId)).toEqual(["fork-a", "fork-b"]);
  const fork = rpcs.at(-1);
  expect(fork?.method).toBe("Fork");
  expect(Reflect.get(fork?.input ?? {}, "name")).toBe("snap-vm-7");
  expect(JSON.parse(String(Reflect.get(fork?.input ?? {}, "bodyJson")))).toEqual({ count: 2 });
});

test("vm.snapshot returns the server-assigned snapshot id", async () => {
  const { state, freestyle } = connectFreestyle();
  state.view = { id: "vm-8" };
  const { vm } = await freestyle.vms.create();
  state.views.Snapshot = { snapshot: "snap-8" };
  expect(await vm.snapshot({ name: "base" })).toEqual({
    snapshotId: "snap-8",
    sourceVmId: "vm-8",
  });
});

test("delete removes the sandbox through one RPC", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-9", observed_state: "terminated" };
  await freestyle.vms.delete({ vmId: "vm-9" });
  expect(rpcs.map((call) => call.method)).toEqual(["Remove"]);
});

test("list reports Freestyle states and counts", async () => {
  const { state, freestyle } = connectFreestyle();
  state.rows = [
    {
      id: "a",
      observed_state: "running",
      created_at: 1_000,
      last_active: 2_000,
      last_network_active: 3_000,
    },
    { id: "b", observed_state: "paused" },
    { id: "c", status: "exited" },
  ];
  const listing = await freestyle.vms.list();
  expect(listing.vms.map((row) => [row.id, row.state])).toEqual([
    ["a", "running"],
    ["b", "suspended"],
    ["c", "stopped"],
  ]);
  expect(listing.vms[0]?.createdAt).toBe(new Date(1_000_000).toISOString());
  expect(listing.vms[0]?.lastNetworkActivity).toBe(new Date(3_000_000).toISOString());
  expect(listing.totalCount).toBe(3);
  expect(listing.runningCount).toBe(1);
  expect(listing.suspendedCount).toBe(1);
  expect(listing.stoppedCount).toBe(1);
  expect(listing.startingCount).toBe(0);
});

test("snapshots namespace lists names and rejects unknown ids", async () => {
  const { state, freestyle } = connectFreestyle();
  state.snapshots = ["snap-a", "snap-b"];
  const { snapshots } = await freestyle.vms.snapshots.list();
  expect(snapshots.map((row) => row.snapshotId)).toEqual(["snap-a", "snap-b"]);
  expect(await freestyle.vms.snapshots.get({ snapshotId: "snap-a" })).toMatchObject({
    snapshotId: "snap-a",
    state: "ready",
  });
  expect(freestyle.vms.snapshots.get({ snapshotId: "snap-z" })).rejects.toThrow(APIError);
});

test("resize maps Freestyle units onto ResizeSandboxRequest", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-10" };
  const { vm } = await freestyle.vms.create();
  await vm.resize({ cpu: 8, memory: 16, storage: 80 });
  expect(rpcs.at(-1)).toMatchObject({
    method: "Resize",
    input: { id: "vm-10", cpus: 8, memoryMib: 16_384, diskMb: 81_920n },
  });
});

test("resize rejects non-power-of-two cpu without issuing an RPC", async () => {
  const { rpcs, state, freestyle } = connectFreestyle();
  state.view = { id: "vm-11" };
  const { vm } = await freestyle.vms.create();
  const callsBeforeResize = rpcs.length;
  expect(vm.resize({ cpu: 3 })).rejects.toThrow(TypeError);
  expect(rpcs).toHaveLength(callsBeforeResize);
});

test("constructor maps apiKey/accessToken and fetch onto the vmon driver", async () => {
  const seen: Request[] = [];
  const facade = new Freestyle({
    baseUrl: "http://node-a?discover=off",
    accessToken: "tok-1",
    fetch: (input, init) => {
      seen.push(new Request(input, init));
      return Promise.resolve(Response.json({ ok: true }));
    },
  });
  expect((await facade.client.health()).ok).toBe(true);
  expect(seen[0]?.headers.get("authorization")).toBe("Bearer tok-1");
  await facade.close();
});
