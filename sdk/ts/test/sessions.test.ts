import { afterEach, expect, test } from "bun:test";
import { create, fromBinary, type MessageInitShape, toBinary } from "@bufbuild/protobuf";
import { Code, createRouterTransport } from "@connectrpc/connect";
import type { Driver, DriverRequestOptions, DriverResponse, EndpointInfo } from "../src";
import { APIError, Client, EventStream, LogStream, MeshDriver } from "../src";
import {
  type ExecInput,
  ExecInputSchema,
  ExecOutputSchema,
  JsonViewSchema,
  SandboxRefSchema,
  SandboxService,
  Stream,
} from "../src/gen/vmon/v1/api_pb";
import { BridgeFrameSchema } from "../src/gen/vmon/v1/bridge_pb";
import { channelFor, sandboxFromRoute } from "../src/sandbox";

const servers: Bun.Server<BridgeSocketState>[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});

export interface BridgeConn {
  sendMessage(payload: Uint8Array): void;
  end(status?: number, message?: string, trailers?: Record<string, string>): void;
}
export interface RpcStub {
  onCall?(conn: BridgeConn, metadata: Record<string, string>): void;
  onMessage?(conn: BridgeConn, payload: Uint8Array): void;
  onHalfClose?(conn: BridgeConn): void;
}
interface BridgeSocketState {
  stub: RpcStub | null;
}

function bridgeFrame(frame: MessageInitShape<typeof BridgeFrameSchema>["frame"]): Uint8Array {
  return toBinary(BridgeFrameSchema, create(BridgeFrameSchema, { frame }));
}

/** A Bun WS server speaking the /grpc BridgeFrame protocol. */
export function bridgeServer(routes: Record<string, RpcStub>): Bun.Server<BridgeSocketState> {
  const server = Bun.serve<BridgeSocketState>({
    port: 0,
    fetch(request, server) {
      const url = new URL(request.url);
      if (url.pathname !== "/grpc") return new Response("not found", { status: 404 });
      return server.upgrade(request, { data: { stub: null } })
        ? undefined
        : new Response("upgrade required", { status: 426 });
    },
    websocket: {
      open() {},
      message(ws, message) {
        const conn: BridgeConn = {
          sendMessage: (payload) => ws.send(bridgeFrame({ case: "message", value: payload })),
          end: (status = 0, text = "", trailers = {}) => {
            ws.send(bridgeFrame({ case: "end", value: { status, message: text, trailers } }));
            ws.close();
          },
        };
        if (typeof message === "string") {
          conn.end(Code.Internal, "expected binary bridge frame", {});
          return;
        }
        const frame = fromBinary(BridgeFrameSchema, new Uint8Array(message)).frame;
        switch (frame.case) {
          case "call": {
            const stub = routes[frame.value.method];
            if (!stub) {
              conn.end(Code.Unimplemented, `unknown method ${frame.value.method}`, {});
              return;
            }
            ws.data.stub = stub;
            stub.onCall?.(conn, frame.value.metadata);
            return;
          }
          case "message":
            ws.data.stub?.onMessage?.(conn, frame.value);
            return;
          case "halfClose":
            ws.data.stub?.onHalfClose?.(conn);
            return;
          default:
            conn.end(Code.Internal, "unexpected bridge frame", {});
        }
      },
    },
  });
  servers.push(server);
  return server;
}

export function clientFor(server: Bun.Server<BridgeSocketState>): Client {
  return new Client(new MeshDriver(`http://127.0.0.1:${server.port}`, { discover: false }));
}

function execOutput(output: MessageInitShape<typeof ExecOutputSchema>["output"]): Uint8Array {
  return toBinary(ExecOutputSchema, create(ExecOutputSchema, { output }));
}

test("Process sends start, resize, stdin and decodes stdout and exit over the bridge", async () => {
  const inputs: ExecInput["input"][] = [];
  const server = bridgeServer({
    "/vmon.v1.SandboxService/Exec": {
      onMessage(conn, payload) {
        const input = fromBinary(ExecInputSchema, payload).input;
        inputs.push(input);
        if (input.case === "stdin") {
          conn.sendMessage(
            execOutput({ case: "chunk", value: { stream: Stream.STDOUT, data: input.value } }),
          );
          conn.sendMessage(execOutput({ case: "exit", value: { code: 0n } }));
          conn.end();
        }
      },
    },
  });
  const client = clientFor(server);
  const stdout: string[] = [];
  const process = await client.sandboxes.ref("s1").exec(["cat"], {
    tty: true,
    onStdout: (data) => stdout.push(new TextDecoder().decode(data)),
  });
  process.resize(24, 80);
  process.stdin.write("hi");
  const events = [];
  for await (const event of process)
    events.push({ stream: event.stream, text: new TextDecoder().decode(event.data) });
  expect(await process.wait()).toEqual({ code: 0, signal: null });
  expect(inputs.map((input) => input.case)).toEqual(["start", "resize", "stdin"]);
  const [start, resize, stdin] = inputs;
  expect(
    start?.case === "start" && {
      sandboxId: start.value.sandboxId,
      cmd: start.value.cmd,
      tty: start.value.tty,
    },
  ).toEqual({ sandboxId: "s1", cmd: ["cat"], tty: true });
  expect(resize?.case === "resize" && { rows: resize.value.rows, cols: resize.value.cols }).toEqual(
    { rows: 24, cols: 80 },
  );
  expect(stdin?.case === "stdin" && new TextDecoder().decode(stdin.value)).toBe("hi");
  expect(events).toEqual([{ stream: "stdout", text: "hi" }]);
  expect(stdout).toEqual(["hi"]);
});

test("PtyStream parses mandatory metadata before delivering output", async () => {
  const inputs: ExecInput["input"][] = [];
  const server = bridgeServer({
    "/vmon.v1.SandboxService/PtyOpen": {
      onMessage(conn, payload) {
        const input = fromBinary(ExecInputSchema, payload).input;
        inputs.push(input);
        if (input.case === "ptyOpen")
          conn.sendMessage(
            execOutput({
              case: "pty",
              value: {
                sessionId: "term-1",
                running: true,
                cols: 100,
                rows: 40,
                createdAtUnixMillis: 123n,
                attachedCount: 1,
                suspended: false,
              },
            }),
          );
        if (input.case === "stdin") {
          conn.sendMessage(
            execOutput({ case: "chunk", value: { stream: Stream.CONSOLE, data: input.value } }),
          );
          conn.sendMessage(execOutput({ case: "exit", value: { code: 0n } }));
          conn.end();
        }
      },
    },
  });
  const stream = await clientFor(server).sandboxes.ref("s1").ptyOpen({
    sessionId: "term-1",
    cols: 100,
    rows: 40,
  });
  expect(stream.sessionId).toBe("term-1");
  expect(await stream.meta).toMatchObject({ sessionId: "term-1", cols: 100, rows: 40 });
  stream.write("hello");
  const chunks: string[] = [];
  for await (const chunk of stream) chunks.push(new TextDecoder().decode(chunk));
  expect(chunks).toEqual(["hello"]);
  expect(inputs.map((input) => input.case)).toEqual(["ptyOpen", "stdin"]);
});

test("shell waits for the ready frame and decodes its exit", async () => {
  const params: unknown[] = [];
  const server = bridgeServer({
    "/vmon.v1.SandboxService/Shell": {
      onMessage(conn, payload) {
        const input = fromBinary(ExecInputSchema, payload).input;
        if (input.case === "shellParamsJson") {
          params.push(JSON.parse(input.value));
          conn.sendMessage(execOutput({ case: "ready", value: { sandboxId: "box" } }));
          conn.sendMessage(execOutput({ case: "exit", value: { code: 0n } }));
          conn.end();
        }
      },
    },
  });
  const shell = await clientFor(server).shell({ image: "alpine" });
  expect(await shell.wait()).toEqual({ code: 0, signal: null });
  expect(params).toEqual([{ image: "alpine" }]);
});

test("Process turns a failed stream status into APIError", async () => {
  const server = bridgeServer({
    "/vmon.v1.SandboxService/Exec": {
      onMessage(conn, payload) {
        if (fromBinary(ExecInputSchema, payload).input.case === "start")
          conn.end(Code.FailedPrecondition, "stopped", { "vmon-code": "not_running" });
      },
    },
  });
  const process = await clientFor(server).sandboxes.ref("s1").exec(["true"]);
  await expect(process.wait()).rejects.toMatchObject({
    name: "APIError",
    code: "not_running",
    message: "stopped",
  });
});

test("unary RPCs round-trip messages and vmon-code trailers over the bridge", async () => {
  const server = bridgeServer({
    "/vmon.v1.SandboxService/Get": {
      onMessage(conn, payload) {
        const ref = fromBinary(SandboxRefSchema, payload);
        if (ref.id === "missing") {
          conn.end(Code.NotFound, "gone", { "vmon-code": "not_found" });
          return;
        }
        conn.sendMessage(
          toBinary(
            JsonViewSchema,
            create(JsonViewSchema, { json: JSON.stringify({ id: ref.id, status: "running" }) }),
          ),
        );
        conn.end();
      },
    },
  });
  const client = clientFor(server);
  const sandbox = await client.sandboxes.get("s1");
  expect(sandbox.info).toEqual({ id: "s1", status: "running" });
  await expect(client.sandboxes.get("missing")).rejects.toMatchObject({
    name: "APIError",
    code: "not_found",
    status: 404,
    message: "gone",
  });
});

test("attach opens the console stream and close cancels it", async () => {
  const called = Promise.withResolvers<void>();
  const server = bridgeServer({
    "/vmon.v1.SandboxService/Attach": {
      onCall() {
        called.resolve();
      },
    },
  });
  const console = await clientFor(server).sandboxes.ref("s1").attach();
  console.close();
  expect(await Array.fromAsync(console)).toEqual([]);
  // The server saw the Attach call before the cancel closed the socket.
  await called.promise;
});

test("port WebSockets relocate a migrated sandbox after not_found", async () => {
  const server = bridgeServer({});
  const socket = new WebSocket(`ws://127.0.0.1:${server.port}/grpc`);
  let attempts = 0;
  const rpcTransport = createRouterTransport(({ service }) => {
    service(SandboxService, {
      tunnels() {
        return { json: '{"connect_token":"token","tunnels":{}}' };
      },
    });
  });
  const rpcDriver = new MeshDriver("http://node-a", {
    discover: false,
    transport: () => rpcTransport,
  });
  const driver: Driver = {
    request(
      _method: string,
      _path: string,
      _options?: DriverRequestOptions,
    ): Promise<DriverResponse> {
      throw new Error("unexpected HTTP request");
    },
    call: rpcDriver.call.bind(rpcDriver),
    serverStream(): never {
      throw new Error("unexpected streaming RPC");
    },
    duplex(): never {
      throw new Error("unexpected bidi RPC");
    },
    websocket(): Promise<[WebSocket, string]> {
      attempts += 1;
      if (attempts === 1) throw new APIError({ status: 404, code: "not_found", message: "moved" });
      return Promise.resolve([socket, "http://node-b"]);
    },
    resolveSandbox(): Promise<string> {
      return Promise.resolve("http://node-b");
    },
    endpoints(): EndpointInfo[] {
      return [
        { url: "http://node-a", healthy: true, source: "seed" },
        { url: "http://node-b", healthy: true, source: "seed" },
      ];
    },
    refresh(): Promise<void> {
      return Promise.resolve();
    },
    close() {},
  };
  const sandbox = sandboxFromRoute(new Client(driver), { id: "moved" }, "http://node-a");
  expect(await sandbox.ports.websocket(8080, "attach")).toBe(socket);
  expect(channelFor(sandbox).endpoint).toBe("http://node-b");
  socket.close();
});

test("stream close cancels the underlying RPC handle", async () => {
  let eventCancelled = false;
  let logCancelled = false;
  const events = new EventStream({
    stream: (async function* (): AsyncGenerator<{ json: string }> {})(),
    cancel() {
      eventCancelled = true;
    },
  });
  const logs = new LogStream({
    stream: (async function* (): AsyncGenerator<{ data: Uint8Array }> {
      yield { data: new TextEncoder().encode("line") };
    })(),
    cancel() {
      logCancelled = true;
    },
  });
  expect((await Array.fromAsync(logs)).join("")).toBe("line");
  events.close();
  logs.close();
  expect(eventCancelled).toBe(true);
  expect(logCancelled).toBe(true);
});
