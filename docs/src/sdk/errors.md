# Errors

The SDKs expose stable error categories so applications do not need to parse human-readable messages. First distinguish a structured server error from a transport, protocol, or durable function-execution failure. Then inspect the structured fields provided by that category.

The public control-plane API is gRPC. For server-classified failures, the stable `vmon-code` metadata value takes precedence over the gRPC status, an SDK's HTTP-equivalent status, and the human-readable message. Branch on the stable code. The status is a compatibility aid, not a separate API or transport contract.

## Stable server error codes

| Stable server code | gRPC status | HTTP-equivalent status | Meaning | Typical caller action |
| --- | --- | ---: | --- | --- |
| `not_found` | `NOT_FOUND` | 404 | The named resource does not exist. | Do not retry unchanged; refresh the resource reference. |
| `invalid` | `INVALID_ARGUMENT` | 400 | A request or its configuration is invalid. | Correct the request. |
| `unauthorized` | `UNAUTHENTICATED` | 401 | Authentication was not accepted. | Supply or refresh credentials. |
| `not_running` | `FAILED_PRECONDITION` | 409 | The operation requires a running or suitable sandbox state. | Change state, then retry if appropriate. |
| `busy` | `ABORTED` | 409 | A required resource is locked or contended. | Retry only when the operation is safe to repeat. |
| `unsupported` | `UNIMPLEMENTED` | 501 | The server or deployment cannot perform the request. | Choose a supported configuration or capability. |
| `engine` | `UNAVAILABLE` | 503 | An unclassified engine or allocation failure occurred. | Treat as potentially transient; inspect the message and retry only when safe. |

`EngineError` is a server implementation type, not an SDK contract. Its legacy JSON-envelope spelling for the final variant is `engine_error`; the v1 gRPC contract and SDK fallback mapping use `engine`. Do not substitute `engine_error` for the stable `vmon-code` value.

All three SDKs represent a structured server rejection as `APIError`. Its stable code and message are authoritative. Python's HTTP `APIError.status` is not populated for gRPC calls; Go and TypeScript expose an HTTP-equivalent status even for gRPC failures. In every case, prefer the stable code over the status.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

try:
    with vmon.connect("vmon://node-a,node-b") as client:
        client.health()
except vmon.APIError as error:
    if error.code == "unauthorized":
        refresh_credentials()
    else:
        print(error.code, error.status, error.message)
```

`APIError.code` defaults to `"engine"` when no more specific structured code is available.

</div>
<div data-sdk-language="go">

```go
var api *vmon.APIError
if errors.As(err, &api) {
    switch api.Code {
    case "unauthorized":
        return refreshCredentials()
    default:
        return fmt.Errorf("vmon API %q (status %d): %w", api.Code, api.StatusCode, err)
    }
}
```

`*vmon.APIError` contains `StatusCode`, stable `Code`, `Message`, and `Truncated`. Preserve it with `%w`, then use `errors.As`; do not branch on `Error()` text.

</div>
<div data-sdk-language="typescript">

```ts
import { APIError } from "@stencil-hq/vibemon";

try {
  await client.health();
} catch (error) {
  if (error instanceof APIError) {
    if (error.code === "unauthorized") {
      await refreshCredentials();
    } else {
      console.error(error.code, error.status, error.message);
    }
  }
  throw error;
}
```

HTTP failures are normalized into `APIError`. For gRPC failures, the SDK reads the `vmon-code` trailer when present and otherwise maps selected gRPC statuses to stable codes. `status` is HTTP-equivalent even when the request used gRPC.

</div>
</div>

## Failure categories and retry behavior

| Failure category | Meaning | Retry rule |
| --- | --- | --- |
| Structured server error | The request reached vmon and the daemon rejected or could not process it. | Apply the stable code's rule above. Mesh routing does not fail over merely because the daemon returned an `APIError`. |
| Transport failure | Connection, DNS, I/O, or transport-timeout failure prevented a reliable server result. | Replay only operations known to be safe. A mesh client may try another endpoint for an eligible transport failure, but that does not make arbitrary application operations idempotent. |
| Protocol failure | The client could not decode a wire frame or response payload. | Do not blindly replay. Retain endpoint and version diagnostics and investigate interoperability. |
| Function-execution failure | Deployed user code ran and its durable call recorded a failure. | Handle it as an application failure, not as a failed control-plane request. A remote `retryable` field is not an SDK retry guarantee. |

Client transport failover is distinct from daemon-owned durable function retry. Durable attempts can execute at least once after infrastructure failure, so externally visible effects must be idempotent. A local wait timeout also does not cancel a durable call: inspect the handle, wait again, or explicitly cancel it. See [Durable Functions](functions.md) and [Shared Concepts](shared-concepts.md).

## Transport and protocol errors

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`TransportError` contains `message` and an optional `endpoint`. `ProtocolError` covers malformed HTTP or WebSocket payloads, gRPC `JsonView` values, and gRPC stream frames.

```python
import vmon

try:
    with vmon.connect("vmon://node-a,node-b") as client:
        client.health()
except vmon.TransportError as error:
    print(f"could not reach {error.endpoint}: {error.message}")
except vmon.ProtocolError:
    print("endpoint returned an invalid protocol payload")
```

</div>
<div data-sdk-language="go">

`*vmon.TransportError` may include `Endpoint` and unwraps its underlying cause. `*vmon.ProtocolError` contains `Operation`, `Message`, and an optional wrapped cause. `*vmon.ResponseTooLargeError` reports that a buffered HTTP response or gRPC guest-file payload exceeded `Limit`.

```go
var transport *vmon.TransportError
var protocol *vmon.ProtocolError
var tooLarge *vmon.ResponseTooLargeError
switch {
case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
    return err
case errors.As(err, &transport):
    return fmt.Errorf("endpoint %q: %w", transport.Endpoint, err)
case errors.As(err, &protocol):
    return fmt.Errorf("protocol operation %q: %w", protocol.Operation, err)
case errors.As(err, &tooLarge):
    return fmt.Errorf("response exceeded %d bytes: %w", tooLarge.Limit, err)
}
```

A gRPC cancellation or deadline remains the corresponding context error when no daemon code overrides it. `FunctionCall.Watch` resumes after transport failure from its durable event cursor, but sends an API or protocol failure on its error channel and then ends.

</div>
<div data-sdk-language="typescript">

`TransportError` represents connection, DNS, or transport-timeout failures eligible for mesh-driver failover. `ProtocolError` represents an invalid wire frame or malformed response payload. Both extend `Error`.

```ts
import { APIError, ProtocolError, TransportError } from "@stencil-hq/vibemon";

function reportFailure(error: unknown): void {
  if (error instanceof TransportError) {
    console.error("transport failure", error.message);
  } else if (error instanceof ProtocolError) {
    console.error("protocol failure", error.message);
  } else if (error instanceof APIError) {
    console.error("API failure", error.code);
  } else {
    console.error("unexpected failure", error);
  }
}
```

</div>
</div>

## Durable function failures

An API error describes a daemon API request. A durable function error is a recorded result from deployed user code and carries remote diagnostics. Keep these categories separate.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`RemoteFunctionError` has `message`, `code` (default `"remote_error"`), `remote_type`, and the remote `traceback` text when supplied by the server. `FunctionCall.get()` raises it for a failed durable result and may raise `TimeoutError` when only the caller's local wait expires.

```python
try:
    result = some_function.remote("input")
except vmon.RemoteFunctionError as error:
    print(error.code, error.remote_type)
    print(error.traceback)
```

`BatchCall.results()` raises a fetched remote failure by default. With `return_exceptions=True`, it returns the failure as an item so the caller can inspect the remaining results; the failed input does not become successful.

</div>
<div data-sdk-language="go">

`*vmon.RemoteCallError` contains `Code`, `Message`, `Type`, `Retryable`, `Details`, `Frames`, and an optional structured `Cause`. It unwraps that cause. `vmon.ErrCallCancelled` is the distinct terminal cancelled outcome.

```go
value, err := function.Remote(ctx, Input{Key: "invoice-42"})
if err != nil {
    var remote *vmon.RemoteCallError
    switch {
    case errors.Is(err, context.DeadlineExceeded):
        return fmt.Errorf("waiting for invoice: %w", err)
    case errors.Is(err, vmon.ErrCallCancelled):
        return err
    case errors.As(err, &remote):
        return fmt.Errorf("remote code %q type %q: %w", remote.Code, remote.Type, err)
    default:
        return err
    }
}
fmt.Println(value)
```

`Retryable` describes the remote failure payload. It does not promise that the SDK retried, will retry, or can safely repeat an application side effect.

</div>
<div data-sdk-language="typescript">

`FunctionExecutionError` includes `code`, `remoteType`, `retryable`, `frames`, and `details` as `Readonly<Record<string, string>>`. Nested remote failures use the standard `Error.cause` chain.

```ts
import { FunctionExecutionError } from "@stencil-hq/vibemon";

try {
  await call.get();
} catch (error) {
  if (error instanceof FunctionExecutionError) {
    console.error(error.code, error.remoteType, error.retryable);
    for (const frame of error.frames) {
      console.error(`${frame.file}:${frame.line} in ${frame.functionName}`);
    }
  }
  throw error;
}
```

`retryable` reports the daemon's failure payload; it does not document an automatic SDK retry guarantee.

</div>
</div>

## Additional local errors

Some failures are detected locally before an RPC or while decoding data.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python provides the following additional typed exceptions:

- `ActorLostError` is raised when a durable actor is terminally `failed` or `deleted`; it exposes `actor_id` and `status`. A stopped actor remains recoverable and is not silently recreated.
- `OptionsError` reports invalid or conflicting function options, including untrusted Cloudpickle configuration.
- `PackageError` reports invalid Python source packaging, such as an invalid package root, unsupported mode, non-importable source, or a closure in package mode.
- `EnvelopeIntegrityError` reports checksum, compression-payload, or envelope-integrity failure.
- `CodecMismatchError` reports an unsupported codec or version.
- `ABIMismatchError` reports a Python payload requiring another Python ABI.
- `JSONProfileError` in `vmon.values` is a `ValueError` for data outside the portable JSON profile, including non-finite numbers.
- Ordinary `ValueError` and `TypeError` report invalid SDK arguments.

```python
try:
    value = vmon.decode_value(envelope)
except vmon.EnvelopeIntegrityError:
    discard_corrupt_payload()
except vmon.CodecMismatchError:
    request_a_supported_codec()
except vmon.ABIMismatchError:
    request_a_compatible_python_payload()
```

Never answer an ABI or codec mismatch by blindly enabling trusted deserialization. Cloudpickle decoding with `trusted=True` may execute Python code and is appropriate only for explicitly trusted data.

</div>
<div data-sdk-language="go">

Envelope decoding validates the serializer, compression, artifact digest, uncompressed size, and checksum. Unsupported serializers and invalid JSON or CBOR payloads return errors rather than best-effort conversions. Preserve wrapping when adding context so callers can continue to use `errors.Is` and `errors.As`.

</div>
<div data-sdk-language="typescript">

Use the SDK's exported error classes with `instanceof`. For unknown errors, rethrow the original value after any local reporting so its class, message, stack, and `cause` chain remain intact.

</div>
</div>

For endpoint selection, credentials, discovery, and timeout configuration, see [Connection Strings and Contexts](connection-strings.md). The canonical server-code reference is [Error Codes](../reference/errors.md).
