import { BatchCall, FunctionCall, connect, type PortableValue } from "@stencil-hq/vibemon";

const BATCH_SIZE = 12;

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} must be set`);
  return value;
}

async function* batchInputs(runName: string): AsyncGenerator<PortableValue> {
  // The generator is consumed lazily. Keeping the count bounded also prevents an
  // accidental unbounded submission if this example is used against production.
  for (let index = 0; index < BATCH_SIZE; index += 1) {
    yield { runName, index };
  }
}

async function main(): Promise<void> {
  const jsonNamespace = requiredEnvironment("VMON_JSON_FUNCTION_NAMESPACE");
  const jsonName = requiredEnvironment("VMON_JSON_FUNCTION_NAME");
  const cborNamespace = requiredEnvironment("VMON_CBOR_FUNCTION_NAMESPACE");
  const cborName = requiredEnvironment("VMON_CBOR_FUNCTION_NAME");
  const runName = process.env.VMON_RUN_NAME?.trim() || `durable-ts-${crypto.randomUUID()}`;

  // VMON_SERVER_URL can point directly at a server. When it is absent, connect()
  // uses the SDK's normal VMON_DSN / VMON_CONTEXT lookup. VMON_API_TOKEN (or the
  // selected context's token) is applied by the connection layer.
  const client = connect(process.env.VMON_SERVER_URL);
  const jsonFunction = await client.functions.fromName(jsonName, {
    namespace: jsonNamespace,
    serializer: "json",
    labels: { example: "durable-functions", run: runName },
  });
  const cborFunction = await client.functions.fromName(cborName, {
    namespace: cborNamespace,
    serializer: "cbor",
    labels: { example: "durable-functions", run: runName },
  });

  const immediate = await jsonFunction.remote({ runName, operation: "remote" });
  console.log("remote JSON result", immediate);

  const spawned = await jsonFunction.spawn({ runName, operation: "spawn" });
  console.log("durable unary call ID", spawned.id);

  // A call ID is sufficient to reconnect from a different process. Persist this
  // ID in a real application rather than retaining the original in-memory handle.
  const reconstructed = FunctionCall.fromId(client, spawned.id);
  console.log("reconstructed unary result", await reconstructed.get());
  console.log("unary status", await reconstructed.status());
  console.log("unary stats", await reconstructed.stats());
  for await (const log of reconstructed.logs({ follow: false })) {
    console.log(`[${log.stream} #${log.sequence}] ${log.text}`);
  }

  const batch = await jsonFunction.spawnMap(batchInputs(runName));
  console.log("detached batch call ID", batch.id);

  // Input submission uses server-advertised backpressure. Do not terminate the
  // submitting process until the lazy input producer has finished or cancel it.
  const ordered: PortableValue[] = [];
  for await (const result of batch) ordered.push(result);
  console.log("batch results in input order", ordered);

  const reconstructedBatch = BatchCall.fromId(client, batch.id);
  for await (const result of reconstructedBatch.results()) {
    // results() follows durable result sequence, which exposes completion order;
    // inputIndex preserves correlation with the original lazy input stream.
    console.log("batch completion", {
      inputIndex: result.inputIndex,
      sequence: result.sequence,
      value: result.value,
    });
  }
  console.log("batch status", await reconstructedBatch.status());
  console.log("batch stats", await reconstructedBatch.stats());

  const cborResult = await cborFunction.remote({
    runName,
    operation: "cbor",
    bytes: new Uint8Array([0, 1, 2, 127, 255]),
    exactInteger: 9_007_199_254_740_993n,
  });
  console.log("CBOR result with Uint8Array and bigint", cborResult);

  const cancellable = await jsonFunction.spawn({ runName, operation: "cancel" });
  const controller = new AbortController();
  const cancelledResult = cancellable.get({ signal: controller.signal });
  controller.abort();
  try {
    await cancelledResult;
  } catch (error) {
    console.log("cancelled call", cancellable.id, error);
  }
  console.log("cancelled status", await cancellable.status());

  // Durable execution is at least once. Functions that write externally must use
  // an idempotency key such as runName (plus inputIndex for batch items); client
  // cancellation can race with an already-running side effect.
}

await main();
