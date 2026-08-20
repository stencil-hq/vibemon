# Durable Functions

Durable functions are owned and scheduled by the `vmon serve` daemon. A call is recorded before execution, receives a stable call ID, and can be observed or reconstructed by another process. Execution attempts have **at-least-once** semantics after infrastructure failures: make external side effects idempotent with a durable request or business key, and do not add a second client retry loop merely because a local wait ended.

See [Durable calls in Shared Concepts](shared-concepts.md#durable-calls) for the protocol model, including sequence-resumable results and events, retention, and observability boundaries.

## Define or resolve a function

The SDKs share the durable call protocol, but they do not share a source deployment model.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python can turn an importable module-level callable into a durable function with `@vmon.function`. The decorator preserves its typed signature and produces the matching remote wrapper for a synchronous function, coroutine, synchronous generator, or async generator.

```python
# myapp/tasks.py
import vmon

@vmon.function
def add(left: int, right: int) -> int:
    return left + right

@vmon.function
def chunks(text: str):
    for word in text.split():
        yield word
```

The definition, its execution configuration, and its packaged source are sent to the daemon; the Python process does not run a worker, image runtime, or daemon. Pass resource, image, network, volume, timeout, worker, retry, serialization, and packaging settings directly to `@vmon.function` or through `FunctionOptions`. `@vmon.concurrent(...)` sets worker concurrency, and `@vmon.batched(max_batch_size=..., max_wait=...)` enables bounded worker-side batching.

```python
import vmon

@vmon.function(retries=3, execution_timeout=60)
def charge(order_id: str) -> str:
    return f"charged {order_id}"
```

</div>
<div data-sdk-language="go">

Go invokes functions already deployed through the platform workflow. It does not register Go functions or scan Go packages. `LookupFunction[T]` resolves the current deployed revision and pins the returned `*vmon.Function[T]` to that immutable revision; use `LookupFunctionRevision[T]` to select a revision explicitly.

```go
type Echo struct {
    Message string `json:"message"`
}

function, err := vmon.LookupFunction[Echo](ctx, client, "production", "echo")
if err != nil {
    return err
}
fmt.Println(function.RevisionID())
```

Looking up “current” is a point-in-time resolution. Look it up again to use a subsequently deployed revision. `WithOptions` derives a handle for the same pinned revision and accepts `WithCallLabels`, `WithResultTTLMillis`, and `WithValueEncoding`.

</div>
<div data-sdk-language="typescript">

TypeScript invokes functions already deployed to the daemon. It does not package or deploy TypeScript source and does not execute functions locally. `client.functions.fromName()` resolves the active revision by default or an explicitly requested immutable revision.

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect();
const double = await client.functions.fromName("double", {
  namespace: "production",
  revisionId: "revision-42",
});

console.log(double.revision.revisionId);
```

The default namespace is `default`. Lookup options can establish per-call defaults for `serializer`, `compression`, `inlineLimit`, `typeName`, `labels`, `resultTtlMillis`, and `signal`. `withOptions()` returns a handle for the same pinned revision with merged defaults; labels are merged.

</div>
</div>

Applications use the same immutable-revision model.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python defines callable functions directly; use the platform's application workflow when a deployed application revision and named bindings are required.

</div>
<div data-sdk-language="go">

`LookupApp` resolves the current application revision, `LookupAppRevision` selects one explicitly, and `AppFunction[T](app, name)` returns a pinned function binding. A missing binding is an error.

</div>
<div data-sdk-language="typescript">

`client.apps.fromName(name, { namespace?, revisionId? })` resolves an application revision. `app.function(bindingName, options?)` returns a `RemoteFunction` for a named binding and throws when the binding is absent.

```ts
const app = await client.apps.fromName("billing", { namespace: "production" });
const charge = app.function("charge", { serializer: "cbor" });
const receipt = await charge.remote({ invoice: "inv-42" });
```

</div>
</div>

## Invoke a function

A direct remote call creates a durable call and waits for its result. Spawn when the stable call ID and lifecycle handle are needed first.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Use `remote()` for unary functions, `spawn()` to retain the call handle, and `remote_gen()` for generators. The SDK validates the local signature before submitting arguments.

```python
from myapp.tasks import add, chunks

assert add.remote(20, 22) == 42

call = add.spawn(20, 22)
print(call.id)
assert call.get(timeout=30) == 42

assert list(chunks.remote_gen("one two")) == ["one", "two"]
```

`get(timeout=...)` raises `TimeoutError` when its local wait expires without changing server execution.

</div>
<div data-sdk-language="go">

`Remote(ctx, input)` waits for unary result index zero. `Spawn(ctx, input)` returns `*FunctionCall[T]` immediately.

```go
call, err := function.Spawn(ctx, Echo{Message: "hello"})
if err != nil {
    return err
}
fmt.Printf("persist call ID %q in durable application storage\n", call.ID())

result, err := call.Get(ctx)
if err != nil {
    return err
}
fmt.Println(result.Message)
```

</div>
<div data-sdk-language="typescript">

`remote(input)` waits for a unary result; `spawn(input)` returns the call first. Use `spawnArguments` or `remoteArguments` for explicit positional and named arguments.

```ts
const call = await double.spawn(21, { labels: { request: "example" } });
console.log(call.id);
const answer = await call.get();

const total = await double.remoteArguments({
  positional: [20],
  named: { adjustment: 1 },
});
```

`get({ signal })` requests daemon-side cancellation with reason `aborted by AbortSignal` if the signal is already aborted or aborts while waiting. A `signal` in call options is also bound to a newly spawned call; this is not merely a local timeout.

</div>
</div>

## Python async facade

The Python durable-function API also exposes `.aio` methods for event-loop applications. The asynchronous facade preserves the same durable call semantics, validation, serializers, results, and errors as the synchronous API.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

@vmon.function
async def double(value: int) -> int:
    return value * 2

async def use_function() -> None:
    call = await double.aio.spawn(21)
    assert await call.get(timeout=30) == 42
```

The async call handle provides `await get(timeout=None)` and `await cancel(reason="cancelled by client")`. Ordinary and coroutine functions use `await function.aio.remote(...)`; generator and async-generator functions use `async for value in function.aio.remote_gen(...)`. Choosing the wrong call kind raises `ValueError` before submission.

</div>
<div data-sdk-language="go">

Go APIs are context-aware blocking calls and streams rather than a separate async facade. Use goroutines when application concurrency requires them, and propagate cancellation through `context.Context`.

</div>
<div data-sdk-language="typescript">

TypeScript durable-function operations already return promises or async iterables, so there is no separate `.aio` namespace.

</div>
</div>

## Inspect, resume, and cancel calls

Call state and cancellation are durable server operations. Cancellation can race completion, so inspect the terminal result or status rather than assuming that cancellation won.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`FunctionCall` exposes `id`, `get_record()`, `stats()`, `graph()`, `cancel()`, `events()`, `logs()`, and, for generator calls, `iter_yields()`. Reconstruct a handle with `FunctionCall.from_id(call_id, client=client)`.

Inside a worker, `vmon.is_remote()` reports whether `VMON_REMOTE=1`, and `vmon.current_call()` exposes metadata including `call_id`, `attempt`, `input_id`, and `actor_id`. `current_call()` raises `RuntimeError` outside a vmon-dispatched call.

</div>
<div data-sdk-language="go">

`FunctionCallFromID[T](client, id)` validates a stable call ID by fetching its record. Its initial lookup has a built-in ten-second deadline; afterward use `Status`, `Stats`, `Graph`, `Result`, `Watch`, `Cancel`, or `Get` with your own context.

`Watch(ctx, afterSequence, follow)` returns event and error channels. It resumes after a durable sequence and reconnects after transport failures while advancing its cursor; API and protocol errors end the watch. Consume both channels.

```go
events, failures := call.Watch(ctx, 0, true)
for event := range events {
    if event.Kind == vmon.CallEventLog {
        fmt.Printf("%d: %s\n", event.Sequence, event.Log)
    }
}
if err := <-failures; err != nil {
    return err
}
```

`Cancel(ctx, reason)` requests durable cancellation. Statuses include pending, queued, running, succeeded, failed, cancelling, and cancelled.

</div>
<div data-sdk-language="typescript">

Reconstruct a handle with `FunctionCall.fromId(client, id)`. A call provides `status()`, `stats()`, `graph()`, reconnectable `events({ afterSequence, follow })`, decoded `logs({ afterSequence, follow })`, indexed `result()` and `resultEntry()`, paged `results()`, and `cancel(reason?)`.

```ts
const record = await call.status();
const statistics = await call.stats();
const relations = await call.graph();
```

</div>
</div>

## Batch calls

Batch submission and execution are durable, but a locally produced input is not committed until the daemon acknowledges it. Persist input identity and reconciliation state before submission when a producer must survive process loss.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`spawn_map()` creates a detached `BatchCall`; `map()` streams results; `starmap()` treats each item as positional arguments; and `for_each()` consumes results. `max_in_flight` must be positive. `BatchCall.results(return_exceptions=True)` can return remote exceptions as values; otherwise the first fetched failure is raised.

```python
for outcome in charge.map(["a-1", "a-2"], max_in_flight=2):
    print(outcome)
```

</div>
<div data-sdk-language="go">

`SpawnMap` creates a closed batch from a finite collection. `Map` accepts a receive-only input channel, commits one input at a time with bounded submission pressure, and returns after the channel closes and the daemon acknowledges closure. It requests cancellation if submission fails or its context ends.

`BatchCallFromID` reconstructs and validates a batch. `Results` streams values in durable **completion order**, not input order. `CompletionResults` also returns `IndexedResult[T]` with `InputID`, `InputIndex`, and completion `Sequence`. Both return value and error channels; drain values, then check errors. `Result(ctx, index)` retrieves one result.

</div>
<div data-sdk-language="typescript">

`spawnMap(inputs)` creates a streamed `BatchCall`; `map(inputs)` yields decoded results in input order. Inputs may be `Iterable<Input>` or `AsyncIterable<Input>`, and encoding and submission are lazy.

```ts
const batch = await double.spawnMap([1, 2, 3]);
for await (const value of batch) {
  console.log(value);
}
```

`result(index)` and `resultEntry(index)` wait for that input to be submitted. `cancel()` first stops local input production, then requests durable cancellation. Submission errors trigger a cancellation request before the SDK preserves the submission error.

</div>
</div>

## Serialization, artifacts, and trust

Arguments and results travel in checksummed value envelopes. Portable JSON is the default across SDKs; CBOR supports values JSON cannot represent. Decoding verifies size, compression, artifact integrity, and the uncompressed SHA-256 checksum before decoding. Large values may be stored as immutable artifacts and fetched through the call handle.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Select `json`, `cbor`, or `cloudpickle` with `serializer=` or `SerializerPolicy`. JSON rejects non-finite numbers and values outside the SDK's JSON-compatible profile. CBOR has a broader value model but is still a value codec, not source packaging.

Cloudpickle serializes Python objects and code, is tied to the Python ABI and Cloudpickle version, and can execute code during decoding. The SDK rejects it unless the function definition explicitly enables trusted Python serialization. Use it only when every producer, stored payload, and consumer is trusted; never use it for untrusted caller input or cross-language compatibility.

```python
import vmon

trusted_python = vmon.SerializerPolicy(
    input_serializer=vmon.ValueCodec.CLOUDPICKLE,
    result_serializer=vmon.ValueCodec.CLOUDPICKLE,
    allow_trusted_python=True,
)

@vmon.function(serializer=trusted_python, package_mode="cloudpickle")
def trusted_only(value):
    return value
```

Envelope decoding also requires an explicit trust gate:

```python
envelope = vmon.encode_value({"safe": ["portable"]}, vmon.ValueCodec.JSON)
value = vmon.decode_value(envelope)

# Only for a Cloudpickle envelope from a source you trust:
# value = vmon.decode_value(envelope, trusted=True)
```

Decode failures include `EnvelopeIntegrityError`, `CodecMismatchError`, and `ABIMismatchError`.

</div>
<div data-sdk-language="go">

Select encoding on a derived handle. Supported compression choices are `CompressionNone`, `CompressionGZIP`, and `CompressionZSTD`.

```go
portable := function.WithOptions(
    vmon.WithValueEncoding(vmon.ValueCBOR, vmon.CompressionZSTD),
)
result, err := portable.Remote(ctx, Echo{Message: "compressed"})
```

`ValueJSON` is deterministic RFC 8259 JSON and enforces I-JSON numeric safety, rejecting integral values outside the IEEE-754 safe integer range. `ValueCBOR` supports representations such as wide integers and binary payloads. Both are canonicalized. Artifact-backed envelopes are fetched and digest-checked by the call handle. Go explicitly rejects Cloudpickle.

</div>
<div data-sdk-language="typescript">

`PortableValue` supports `null`, booleans, numbers, `bigint`, strings, `Uint8Array`, arrays, and recursively string-keyed objects. JSON accepts only JSON-compatible values. CBOR supports `Uint8Array` and `bigint` in the accepted 64-bit range from $-2^{64}$ through $2^{64}-1$. Serializer choices are `"json"` and `"cbor"`; compression is `"none"` or `"gzip"`.

Values up to `inlineLimit` (256 KiB by default) remain inline; larger values are uploaded through the client-backed artifact store. Standalone `encodeValue` and `decodeValue` accept an explicit `ValueArtifactStore` for artifact-backed envelopes.

```ts
import { decodeValue, encodeValue } from "@stencil-hq/vibemon";

const envelope = await encodeValue(
  { bytes: new Uint8Array([0, 1, 255]), wide: 2n ** 53n },
  { serializer: "cbor" },
);
const value = await decodeValue(envelope);
```

For typed domain values, pass **both** `input` and `result` `FunctionValueAdapter`s to `fromName`; supplying only one is invalid. TypeScript rejects the Python-only Cloudpickle serializer for encoding and decoding.

</div>
</div>

## Python source packaging and worker images

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

With the default `package_mode="package"`, the SDK creates a deterministic ZIP of the callable's source package and sends a manifest containing the Python ABI, module, qualified name, and checksums. The daemon imports that module in a worker. This is Python-specific and is not the portable cross-language function ABI.

The callable must come from a normal importable module with filesystem source. `__main__` callables, nested functions, and closures cannot use package mode. Set `package_root`, `include`, `exclude`, and `local_packages` when the inferred module root is insufficient.

```python
import vmon

@vmon.function(
    package_root=".",
    include=("myapp/**/*.py",),
    exclude=("**/__pycache__/**",),
    local_packages=("../shared_python",),
)
def summarize(lines: list[str]) -> dict[str, int]:
    return {"lines": len(lines)}
```

`vmon.package_callable()` constructs the same artifact, `vmon.inspect_manifest()` validates its embedded `vmon-package.json`, and `vmon.build_package()` builds from an explicit root, module, and qualified name.

`package_mode="cloudpickle"` serializes the callable itself. It can represent closures that package mode rejects, but remains an explicit trusted-Python choice and is neither safe for untrusted data nor cross-language or ABI-independent.

The worker image is a separate artifact from the callable package. Pass an image reference or compose one with `vmon.Image`; the client sends its configuration but does not build or run it locally.

```python
image = (
    vmon.Image.python("3.14")
    .uv_install("numpy")
    .add_local_python_source("myapp")
)

@vmon.function(image=image)
def mean(values: list[float]) -> float:
    import numpy
    return float(numpy.mean(values))
```

Image sources include `Image.from_registry(...)`, `Image.from_dockerfile(...)`, and `Image.from_template(...)`; templates require an immutable SHA-256 revision. Available steps include `uv_sync`, `uv_install`, `apt_install`, `env`, `run_commands`, `add_local_file`, and `add_local_python_source`.

</div>
<div data-sdk-language="go">

Go does not package callable source or construct a worker image through the functions API. Deploy the function revision and worker environment through the platform workflow, then resolve its immutable revision from the SDK.

</div>
<div data-sdk-language="typescript">

TypeScript does not package callable source or construct a worker image through the functions API. Deploy the function revision and worker environment through the platform workflow, then resolve its immutable revision from the SDK.

</div>
</div>

## Failures

Remote function failures are distinct from API and transport failures. Handle them according to the language-specific error type and inspect durable status when coordination matters. See [Errors](errors.md); for endpoint and authentication configuration, see [Connection Strings and Contexts](connection-strings.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`get()` raises the remote execution error for a failed call. Batch result iteration can instead return remote exceptions as values when explicitly requested.

</div>
<div data-sdk-language="go">

`Get` returns `vmon.ErrCallCancelled` for terminal cancellation and `*vmon.RemoteCallError` when the deployed function failed.

</div>
<div data-sdk-language="typescript">

A remote failure becomes `FunctionExecutionError`, with `code`, `remoteType`, `retryable`, decoded stack `frames`, string-valued `details`, and a nested `cause` when supplied by the service.

</div>
</div>
