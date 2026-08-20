# Volumes and Secrets

Volumes and secrets describe data supplied to remote sandboxes managed by `vmon serve`. A named volume preserves guest filesystem data independently of sandbox lifecycle. A secret is an in-memory bundle of environment values supplied during sandbox creation or guest execution. Neither feature creates local storage or a local secret-management service. See [Storage and Volumes](../platform/storage.md) for platform semantics.

For client setup and shared error behavior, see [Connect](connect.md), [Connection Strings and Contexts](connection-strings.md), and [Error Codes](../reference/errors.md).

## Named volumes

Use the client's volume service to create, list, and delete daemon-owned persistent volumes. Volume names are 1–64 characters matching `[a-z0-9_][a-z0-9_.-]{0,63}`.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`Volume(name)` is the validated Python value type. `client.volumes.create(name)` creates a volume if it does not already exist and returns a `Volume`. `delete()` accepts either a `Volume` or its name.

```python
from vmon import Volume, connect

with connect() as client:
    cache = client.volumes.create("build-cache")
    assert cache.name == "build-cache"
    print([volume.name for volume in client.volumes.list()])

    client.volumes.delete(cache)
```

</div>
<div data-sdk-language="go">

`vmon.Volume` is the validated Go value type. Construct one with `vmon.NewVolume`, or let `client.Volumes.Create` provision and return it. `List` returns `[]vmon.Volume`; create and delete operations return validation or server errors.

```go
volume, err := client.Volumes.Create(ctx, "build-cache")
if err != nil {
    return err
}
fmt.Println(volume.Name())

volumes, err := client.Volumes.List(ctx)
if err != nil {
    return err
}
for _, volume := range volumes {
    fmt.Println(volume.Name())
}

if err := client.Volumes.Delete(ctx, "build-cache"); err != nil {
    return err
}
```

</div>
<div data-sdk-language="typescript">

`Volume` is the TypeScript value type. `client.volumes.create()` and `list()` return `Volume` objects; `new Volume(name)` rejects an empty name.

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect();
const volume = await client.volumes.create("build-cache");
console.log(volume.name);

const volumes = await client.volumes.list();
console.log(volumes.map((item) => item.name));

await client.volumes.delete("build-cache");
```

</div>
</div>

A volume's lifecycle is separate from any sandbox that mounts it. Terminating or removing a sandbox does not delete the volume. Delete it explicitly only after its retained data is no longer needed, and do not delete a volume that another sandbox still requires. The SDK does not track usage or garbage-collect volumes.

Volumes are tenant-scoped. A tenant cannot list, attach, or delete another
tenant's volume. Detached persistent volume data is encrypted with the owning
tenant's customer key ID. If that key is unavailable when the volume is
attached, the daemon returns an error rather than mounting a new empty or
plaintext volume.

## Mount a volume at creation

A volume mount belongs to the sandbox creation request. Map each absolute guest path to the SDK's accepted mount value; read-only and writable mappings can refer to the same persistent volume.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python accepts a `Volume` or volume-name string for a writable mount. Use a two-item tuple of the volume value and `True` for a read-only mount.

```python
from vmon import Volume

cache = Volume("build-cache")
sandbox = client.sandboxes.create(
    image="alpine",
    volumes={
        "/var/cache/build": cache,
        "/mnt/cache-ro": ("build-cache", True),
    },
)
try:
    sandbox.run("sh", "-lc", "printf artifact > /var/cache/build/output.txt")
finally:
    sandbox.terminate()
```

</div>
<div data-sdk-language="go">

Call `volume.Mount(readOnly)` to construct a `vmon.VolumeMount`, then place it in `SandboxCreateRequest.Volumes`. `Mount(false)` is writable and `Mount(true)` is read-only. `VolumeMount.Volume()` returns its backing volume and `ReadOnly()` reports the flag. Do not handcraft the JSON representation.

```go
cache, err := vmon.NewVolume("build-cache")
if err != nil {
    return err
}

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image: "alpine:3.21",
    Volumes: map[string]vmon.VolumeMount{
        "/var/cache/build": cache.Mount(false),
        "/mnt/cache-ro":    cache.Mount(true),
    },
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

</div>
<div data-sdk-language="typescript">

`volume.mount(readOnly = false)` returns the typed descriptor `{ name, read_only }`. The request also accepts an inline descriptor or a string volume name.

```ts
const sandbox = await client.sandboxes.create({
  image: "alpine",
  volumes: {
    "/var/cache/build": volume.mount(),
    "/mnt/cache-ro": volume.mount(true),
    "/mnt/inline": { name: "build-cache", read_only: true },
  },
});
```

</div>
</div>

Mount timing, sharing, persistence, and final validation are daemon behavior. Use the SDK value constructors where available so local validation and request representation remain consistent.

## S3 mounts

An S3 mount maps an **absolute guest mountpoint** to `s3://bucket[/prefix]`. It is a remote filesystem, not a local path or named persistent volume. The URI shorthand is sufficient where supported; a structured value adds endpoint, region, access credentials, and read-only behavior.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python accepts either a URI string or `S3Mount`. The SDK checks the value type but leaves URI syntax, absolute mountpoints, endpoint reachability, credential pairing, and access probes to the daemon. A daemon accepts at most eight S3 mounts and requires both `access_key` and `secret_key` if any inline credential field is supplied.

```python
from vmon import S3Mount

sandbox = client.sandboxes.create(
    image="alpine",
    s3_mounts={
        "/mnt/assets": "s3://example-assets/public",
        "/mnt/build-input": S3Mount(
            uri="s3://example-builds/input",
            endpoint="https://objects.example.invalid",
            region="us-east-1",
            read_only=True,
        ),
    },
)
```

</div>
<div data-sdk-language="go">

Go requires a `vmon.S3Mount` value rather than a bare URI string.

```go
sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image: "alpine:3.21",
    S3Mounts: map[string]vmon.S3Mount{
        "/mnt/assets": {
            URI:      "s3://example-assets/public",
            ReadOnly: true,
        },
        "/mnt/build-input": {
            URI:      "s3://example-builds/input",
            Endpoint: "https://objects.example.invalid",
            Region:   "us-east-1",
            ReadOnly: true,
        },
    },
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

</div>
<div data-sdk-language="typescript">

TypeScript accepts either the URI shorthand or a structured `S3MountSpec`.

```ts
import type { S3MountSpec } from "@stencil-hq/vibemon";

const buildInput: S3MountSpec = {
  uri: "s3://example-builds/input",
  endpoint: "https://objects.example.invalid",
  region: "us-east-1",
  read_only: true,
};

const sandbox = await client.sandboxes.create({
  image: "alpine",
  s3_mounts: {
    "/mnt/assets": "s3://example-assets/public",
    "/mnt/build-input": buildInput,
  },
});
```

</div>
</div>

The structured endpoint selects an S3-compatible path-style endpoint, and the region overrides the SigV4 region. Credential fields are `access_key`, `secret_key`, and optional `session_token` (capitalized as `AccessKey`, `SecretKey`, and `SessionToken` in Go). Prefer the daemon's AWS environment credentials. If inline credentials are necessary, obtain them from a secure runtime source and pass them only through the mount value over a secured client-to-daemon connection. Never print, log, serialize, commit, or embed real credentials in source.

Inline credentials are request-only, and snapshot mount metadata excludes them. Restoring a snapshot that used inline or environment credentials requires usable daemon environment credentials again; anonymous mounts do not.

A read-only mount exposes remote data without guest writes. When read-only is omitted or false, the daemon presents a writable **volatile overlay**: guest writes are not synchronized to S3 and do not survive sandbox removal or snapshotting as S3 objects. Do not use that overlay as persistent storage. S3 mounts have no named-volume lifecycle.

## Host-brokered credential names

The `credentials` creation field contains tenant-local credential names only.
The daemon resolves those names through its host gateway and injects the
credential's configured headers only for allowed domains. The SDK request,
guest environment, sandbox metadata, logs, and snapshots do not receive the
header values. An unknown, expired, revoked, rate-limited, or domain-mismatched
credential fails at the gateway rather than becoming a direct guest secret.

Credential records are administered by the gRPC `CredentialService`; its
`List` and `Put` responses expose metadata, never header values. The
convenience SDKs intentionally expose only names here.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
sandbox = client.sandboxes.create(
    image="alpine",
    credentials=["github-api"],
)
```

</div>
<div data-sdk-language="go">

```go
sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:       "alpine:3.21",
    Credentials: []string{"github-api"},
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

</div>
<div data-sdk-language="typescript">

```ts
const sandbox = await client.sandboxes.create({
  image: "alpine",
  credentials: ["github-api"],
});
```

</div>
</div>

## In-memory secret environment

A secret is a named, validated bundle of environment variables. Names cannot be empty or contain `=` or NUL; values cannot contain NUL. Constructors can copy an explicit mapping or select existing variables from the client process environment. Missing requested host variables are omitted.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Use `Secret.from_dict(values, name=...)` or `Secret.from_env(*names, name=...)`. A create request accepts `Secret` objects and plain name-to-value mappings. Multiple entries are flattened in order, so later entries replace earlier keys.

```python
from vmon import Secret

credentials = Secret.from_dict(
    {"API_TOKEN": "token-from-a-secure-source"},
    name="service-token",
)
from_host = Secret.from_env("CI_JOB_TOKEN", name="ci")

sandbox = client.sandboxes.create(
    image="alpine",
    secrets=[credentials, from_host],
)
```

`names()` returns sorted variable names, `as_env()` returns a copy of the values, and `repr(secret)` shows only the bundle name and variable names.

</div>
<div data-sdk-language="go">

Use `vmon.NewSecret(name, values)` or `vmon.SecretFromEnvironment(name, variables...)`. The constructor copies its input map. Add the resulting `vmon.Secret` values to `SandboxCreateRequest.Secrets`.

```go
credentials, err := vmon.NewSecret("registry", map[string]string{
    "REGISTRY_TOKEN": "token-from-a-secure-source",
    "REGISTRY_USER":  "ci-bot",
})
if err != nil {
    return err
}

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:   "alpine:3.21",
    Secrets: []vmon.Secret{credentials},
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

`Name()` returns the bundle name and `Names()` returns sorted variable names. `String()` and Go-syntax formatting redact values.

</div>
<div data-sdk-language="typescript">

Use `Secret.fromDict(values, name?)` or `Secret.fromEnv(names, name?)`. `fromEnv` reads the Node/Bun process environment when `process` is available; it does not read a browser environment. `SecretInput` also accepts a `SecretWire` or plain dictionary. A bare dictionary is normalized under the name `"secret"`.

```ts
import { Secret, type SecretWire } from "@stencil-hq/vibemon";

const credentials = Secret.fromDict(
  { API_TOKEN: "token-from-a-secure-source" },
  "service-token",
);
const wire: SecretWire = {
  name: "registry",
  values: { REGISTRY_USER: "ci-bot" },
};

const sandbox = await client.sandboxes.create({
  image: "alpine",
  secrets: [credentials, wire],
});
```

`names()` returns sorted variable names and `asEnv()` returns a copy. The SDK validates and serializes each input to `{ name, values }`; `secrets: null` is sent unchanged, while `undefined` omits the field.

</div>
</div>

Secrets are environment injection, not guest file mounts, persisted client configuration, or a secret resource service. Keep source and copied values in the application's secret-management flow. Avoid logging source mappings, process environments, serialized requests, command output, or error context that might reveal them. Sandbox metadata does not provide a secret-value readback API.

## Execution precedence and request scope

Creation-time secrets and ordinary environment values have SDK-specific execution behavior. Use ordinary environment fields only for non-sensitive configuration.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python binds creation-time secrets for guest exec sessions. Later secret entries overwrite earlier keys. At execution time, an explicit `env=` passed to `sandbox.run()` or `sandbox.exec()` overrides the sandbox-bound environment, including secret values.

```python
sandbox = client.sandboxes.create(
    image="alpine",
    secrets=[Secret.from_dict({"TOKEN": "initial"})],
)
try:
    result = sandbox.run(
        "sh", "-lc", 'printf %s "$TOKEN"', env={"TOKEN": "per-call"}
    )
    assert result.stdout == b"per-call"
finally:
    sandbox.terminate()
```

</div>
<div data-sdk-language="go">

Go sends both `Env` and `Secrets` in `SandboxCreateRequest`. Per-process `ExecRequest.Env` is a separate, non-secret environment map for run or exec operations.

```go
secret, err := vmon.NewSecret("application", map[string]string{
    "DATABASE_PASSWORD": "token-from-a-secure-source",
})
if err != nil {
    return err
}

_, err = client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image: "alpine:3.21",
    Env: map[string]string{
        "LOG_LEVEL": "info",
    },
    Secrets: []vmon.Secret{secret},
})
return err
```

</div>
<div data-sdk-language="typescript">

For `sandbox.run()` and `sandbox.exec()`, `RunOptions.secrets` is separate from creation secrets. Later secret inputs overwrite earlier keys, and merged secret values overwrite same-named keys in `options.env`.

```ts
const result = await sandbox.run(["sh", "-lc", "test -n \"$API_TOKEN\""], {
  env: { LOG_LEVEL: "info" },
  secrets: [Secret.fromDict({ API_TOKEN: "token" }, "api")],
});

if (result.exit !== 0) {
  throw new Error("token was not available to the command");
}
```

</div>
</div>

Pass a bounded request context where the SDK supports one, secure the client-to-daemon connection, and handle creation errors before retaining a sandbox handle. Snapshot restore and fork have distinct daemon-defined behavior; see [Snapshots](snapshots.md). See [Shared Concepts](shared-concepts.md#security-boundary) for the security boundary and resource ownership rules.
