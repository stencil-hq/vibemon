# e2e — official E2B SDK suites against vibemon

`run.py` runs the **unmodified** upstream test suites of
[`e2b-dev/E2B`](https://github.com/e2b-dev/E2B) (`packages/python-sdk/tests`,
`packages/js-sdk/tests`) against a fresh monolithic `vmon serve`, the same way
upstream CI does (`.github/workflows/python_sdk_tests.yml`,
`js_sdk_tests.yml`): `uv run pytest --verbose --numprocesses=4` and
`vitest run` with `E2B_TEST_MAX_WORKERS=1`, non-debug (no `E2B_DEBUG`).

The upstream monorepo is fetched as a tarball at the pinned commit
`E2B_REF` in `run.py` (the same snapshot as `spec/e2b/SOURCES.md`) into
`e2e/.cache/E2B-<ref>/` — never vendored.

## One command

```sh
just e2e python                 # build + sign vmon, build the musl agent, start MySQL, run
just e2e js
just e2e all -- -k sandbox      # args after `--` go to pytest/vitest
python3 e2e/run.py all --ingress tls     # direct; uses $CARGO_TARGET_DIR binaries
python3 e2e/run.py serve --ingress tls   # stack only; prints `e2e: stack ready: source …/env.sh`
```

Each run, from a clean slate:

1. drops/creates MySQL database `vmon_<name>` (Docker container `vmon-mysql`,
   or a local `mysql` client with `--mysql-container ''`);
2. `vmon admin create-team` → API key;
3. starts `vmon serve` (node + driver) on slot ports
   (`--slot 31`: API `3100`, cluster `7147`, gossip `7146`) with data dir
   `/tmp/vmon-<name>`; `--sudo` runs it via `sudo -n` (Linux TAP);
4. builds templates with the pinned SDK (`build_templates.py`): `base`
   (`e2bdev/base`, as upstream's `templates/base`), `httpbin` (upstream
   `templates/httpbin/e2b.Dockerfile`, go-httpbin start + ready command, as
   upstream's `templates.yml`) and `e2e-warm-ubuntu` (warms the
   `ubuntu:22.04` layer cache many template tests start from);
5. runs the suite(s), prints junit counts (`/tmp/vmon-<name>-*-junit.xml`);
6. tears down: serve, leftover VMMs, database, data dir (`--keep` keeps
   them, `--reuse` restarts on a kept stack and skips existing templates).

OCI blobs and manifests are cached across runs in `e2e/.cache/oci`
(`--oci-cache ''` disables); cached tags resolve with uncounted `HEAD`
requests. If Docker Hub still rate-limits (`HTTP 429 TOOMANYREQUESTS`), export
`VMON_REGISTRY_MIRROR=https://mirror.gcr.io` (inherited by `vmon serve`) or
pass `--serve-arg=--registry-mirror=<url>`. Other workstreams: pick your own
`--name` and `--slot`.

### Ingress modes

- `--ingress plain` (default): SDK traffic goes to the plain-HTTP listener via
  `E2B_API_URL` / `E2B_SANDBOX_URL` (`E2b-Sandbox-*` headers).
  `https://<port>-<id>.<domain>` hosts are unreachable, so tests using
  `get_host()` / signed file URLs fail by construction.
- `--ingress tls`: upstream's topology. Domain `<name>.localhost` (resolves to
  loopback on macOS and systemd-resolved Linux), `vmon serve --tls-listen
  [::]:443` with a throwaway CA + `*.<domain>` certificate; clients trust it via
  `SSL_CERT_FILE` (system roots + CA) and `NODE_EXTRA_CA_CERTS`. The JS suite
  then runs with **exactly upstream CI's environment** (`E2B_API_KEY`,
  `E2B_DOMAIN`). One TLS stack per host (port 443).
  - **macOS + Python exception**: the Python SDK's pyqwest transport verifies
    TLS through `rustls-platform-verifier`, which on macOS only trusts the
    keychain and ignores `SSL_CERT_FILE`; adding a trusted root needs
    interactive authorization. So on macOS the Python SDK's own API/envd
    traffic keeps the `E2B_API_URL`/`E2B_SANDBOX_URL` routing (template upload
    URLs are served on plain HTTP via `--public-url`), while the tests' own
    `httpx`/`urllib` requests to `https://<port>-<id>.<domain>` use the TLS
    ingress. Offline tests (`tests/test_*.py`, `*/test_config_propagation.py`
    — mock servers and `ConnectionConfig` unit tests asserting URLs derived
    from `E2B_DOMAIN` alone) run in a first pass with upstream's exact
    environment, because the routing variables override what they assert.

## Results (macOS 27 / HVF, Apple M4 Max, upstream E2B `df6c368`)

| Run | Mode | python-sdk | js-sdk |
|---|---|---|---|
| Baseline (before fixes) | plain, one pass, `E2B_HTTP_VERSION=1.1` | 1356 passed, **70 failed, 26 errors**, 34 skipped (12 min) | — |
| Mid | tls, one pass | 1419 passed, 33 failed, 34 skipped | 1003 passed, 5 failed, 36 skipped |
| Previous full `run.py all --ingress tls` | tls | offline pass 505/505; live pass 937 passed, 10 failed, 34 skipped | 1004 passed, 4 failed, 36 skipped |
| **Final** (2026-10-12, `--kernel target/kernel/Image-aarch64 --serve-arg=--registry-mirror=https://mirror.gcr.io`, 10 min) | tls | offline pass **505/505**; live pass **939 passed, 8 failed**, 34 skipped (1444 passed in total) | **1006 passed, 2 failed**, 36 skipped |
| JS re-run after the ingress hold fix (2026-10-12, `just e2e js --ingress tls`, same flags, 3 min) | tls | — | **1007 passed, 1 failed**, 36 skipped |

Every failure from the previous run that had a fix (filesystem-only pause,
snapshot pause/resume, `httpsPorts`, `mask_request_host`, RUN unknown user)
passes in the final run. One JS test regressed between the two runs; it was
fixed afterwards (below) and passes in the JS re-run, whose only failure is
the Linux-only `firewall transform injects headers`.

### Remaining failures in the final run

| Test(s) | Root cause | Status |
|---|---|---|
| py `files/test_secured.py::test_download_url_with_*` (sync+async, 6) | macOS-Python harness limitation: `download_url()` builds on `envd_direct_url`, which the SDK sets to `E2B_SANDBOX_URL`; a bare `…/files?signature=…` carries no sandbox id. Would pass with pure upstream env (Linux `--ingress tls`). | Justified (harness/platform) |
| py `test_network.py::test_firewall_transform_injects_headers` (2), js `firewall transform injects headers` | Egress header `rules` need the Linux egress MITM proxy; macOS user-net nodes are not eligible → `503 no compatible node`. | Justified (Linux-only feature; Linux/KVM run not possible — Lima VM stopped by request) |
| js `lifecyclePayload.test.ts > auto-resume wakes paused sandbox on http request` | Regression between the two runs. Pausing stops the VM and ends the test's open background-command stream; the JS SDK reacts with an envd `GET /health` (port 49983, valid token) through the ingress, which auto-resumed the sandbox about 180 ms after the pause, before the test's 500 ms poll could see `paused`. Upstream has the same mechanism but pauses/resumes slower than one poll. | **Fixed** after the run (`driver/src/ingress/hold.rs`: a guest stream that breaks while the sandbox is pausing/paused is held open until it runs again, is killed, or 620 s pass). Passes in the full JS re-run |

### Skips (identical to upstream CI)

- 31 py + 32 js git tests: gated on `ENABLE_GIT_TESTS`, unset in upstream CI.
- 2 py + 1 js volume `depth` tests: upstream `skip("depth option not yet supported")`.
- 1 py + 3 js Windows-path tests: platform-gated.

### Fixes made from suite failures

| Failure | Fix (file:function) | Root cause |
|---|---|---|
| ubuntu:22.04 builds: `write /etc/resolv.conf: ENOENT` | `agent/src/provision.rs:provision`, `agent/src/main.rs:write_resolv_conf` | Dangling `/etc/resolv.conf` → `/run/systemd/resolve/…` symlink kept; now replaced by a regular file like upstream's rootfs layer. |
| `test_file_symlink`: `symlink` instead of `file` | `agent/src/envd/entry.rs:entry_from_meta` | We set the unreleased `is_symlink` flag (added upstream after envd 0.9.0); released envd never sets it. |
| `test_list_sandboxes_started_after` | `driver/src/store/sandboxes.rs:push_filter` | `startedAfter` was exclusive; upstream is inclusive (truncated to the column grid). |
| tags tests: "build was canceled because a newer build…" | `driver/src/store/builds.rs:template_building_builds`, `driver/src/builds/start.rs` | Starting a build cancelled every in-progress build of the template; upstream cancels only builds sharing a tag. |
| `test_mask_request_host` (py+js): no `Host:` | `driver/src/ingress/proxy.rs:guest_http1` | Guest requests used lowercase header names; upstream's Go proxy sends canonical case. |
| `test_run_command_as_user_that_does_not_exist` | NodeBuild: `node/src/build/pipeline.rs`, agent user lookup | Error wording differed from upstream's `failed to run command '…': command failed: unauthenticated: invalid username: '…'`. |
| Concurrent creates: `agent hello: no reply within 30s` (most baseline timeouts/errors) | NodeRuntime: `vmm` HVF pause parks vCPUs only outside interrupt handlers | Snapshots captured mid-IRQ restored with a stuck GIC priority. |
| Filesystem-only resume: wrong hello epoch | `node/src/gc4/mod.rs:first_request_id` | Stale replies from timed-out connections matched restarted request ids. |
| Env-sensitive unit tests (`test_connection_config`, `test_api_client_transport`, …) | harness (`--ingress tls`, offline pass, no `E2B_HTTP_VERSION`) | Routing variables leaked into tests asserting defaults. |

## code-interpreter

The `code-interpreter-v1` default template (`templates/code-interpreter/`)
against three upstream suites. Each suite runs with its upstream CI flags,
non-debug, and uses the key of a **non-system** team (`e2e-<name>`). Every
sandbox is created as unqualified `code-interpreter-v1`, which resolves to the
system team's published template through the bare alias.

| Suite (pinned) | Upstream CI command | Result |
|---|---|---|
| SDK Python, E2B `df6c368` `packages/code-interpreter-python/tests` (sync + async + offline) | `uv run pytest --verbose --numprocesses=1` (Python 3.10) | **83 passed**, 0 failed, 0 skipped (43 s) |
| SDK JS, E2B `df6c368` `packages/code-interpreter-js/tests` | `pnpm test` with `E2B_TEST_MAX_WORKERS=1`, Node 22.18.0 (`.tool-versions`) | **41 passed**, 0 failed, 0 skipped |
| same, the other CI matrix legs | `pnpm test:bun` (bun 1.4.3), `pnpm test:deno` (deno 2.9.7, `DENO_CERT=<ca.pem>`), `pnpm test:cf` | 41/41 each |
| Template HTTP tests, code-interpreter `a4934a5` `tests/` (kernels python/R/JS/TS/Java/bash, charts, contexts, cwd, interrupt, systemd restart, Jupyter socket) | `uv run pytest --verbose --numprocesses=1` (`E2B_TESTS_TEMPLATE` unset → `code-interpreter-v1`) | **86 passed**, 0 failed, 0 skipped (82 s) |

The interrupt/recovery suites ran as well: upstream's `run_recovery_tests`
defaults to true. The JS legs and the template HTTP tests reach Jupyter's
`/execute` NDJSON stream through the TLS ingress
(`https://49999-<id>.<name>.localhost`). The Python SDK uses the macOS
`E2B_SANDBOX_URL` routing described above.

Reproduce (macOS / HVF, 2026-10-12):

```sh
# stack: TLS ingress, fresh data dir + DB, the run.py team = non-system key
python3 e2e/run.py serve --name ci --slot 36 --ingress tls --keep \
  --kernel target/kernel/Image-aarch64 --serve-arg=--registry-mirror=https://mirror.gcr.io
# (run.py's own base/httpbin builds land in the e2e team's namespace and do not
#  affect `code-interpreter-v1` resolution; the run above skipped them)
# default templates as the system team (secret: /tmp/vmon-ci/e2e-state.json)
VMON_BIN=$CARGO_TARGET_DIR/debug/vmon VMON_MYSQL_URL=mysql://root:vmon@127.0.0.1:13306/vmon_ci \
  VMON_CLUSTER_SECRET=<secret> E2B_API_URL=http://127.0.0.1:3600 \
  uv run templates/build.py base code-interpreter-v1 --skip-cache
# suites, from an E2B df6c368 checkout (deps installed as in upstream CI)
(cd packages/code-interpreter-python && . /tmp/vmon-ci/env.sh && uv run pytest --verbose --numprocesses=1)
(cd packages/code-interpreter-js && . /tmp/vmon-ci/env-node.sh && E2B_TEST_MAX_WORKERS=1 pnpm test)
# template HTTP tests, from a code-interpreter a4934a5 checkout
(cd tests && . /tmp/vmon-ci/env.sh && uv run pytest --verbose --numprocesses=1)
```

Build: `base` took 70 s (OCI cache warm) and `code-interpreter-v1` took
10 min 50 s cold (2 vCPU / 2 GiB, aarch64 Temurin JDK 11). All earlier
attempts failed in pip, R and curl downloads ("Connection interrupted",
"versions: none", `curl: (28) Resolving timed out after 734487 ms`). The
cause was not the network: the guest's counter jumped forward on HVF snapshot
restore by the time since the snapshot, so every in-flight timeout fired at
once. NetRestore fixed it in vmm (the HVF vtimer counter is persisted;
`SNAPSHOT_VERSION` 4). After that fix the cold build passed first time, with
no download warnings.

Outside upstream CI's environment: under **Node 26**, `killedSandbox.test.ts`
fails (`expected … /sandbox was killed while the request …/ but got
'terminated'`). Node 26's `fetch` negotiates HTTP/2 through ALPN, and E2B's
cloud edge accepts h2 too. When the sandbox dies mid-stream, the ingress
resets the stream (`RST_STREAM INTERNAL_ERROR`, as Go's h2 server does for an
aborted proxy handler). undici reports that as `ERR_HTTP2_STREAM_ERROR`, which
the SDK's `isConnectionClosedError` does not recognize: it lists only
`ECONNRESET`/`EPIPE`/`UND_ERR_SOCKET`. This is an SDK/runtime gap. Node
22.18.0 (CI's pin) uses HTTP/1.1 and passes.

## Not covered here

- Linux/KVM and the distributed topology (Lima VM stopped on request);
  `--sudo --ingress tls` is the intended Linux invocation (pure upstream env
  for both SDKs there).
- desktop SDK suites (only the Templates workstream's desktop smoke ran).
