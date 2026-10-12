# Vendored E2B specs

All files in this directory are copied byte-for-byte from upstream E2B repositories.
Do not edit them by hand; re-vendor from upstream instead. Both upstream repositories are
licensed under the Apache License 2.0 (`SPDX: Apache-2.0`), "Copyright 2023 FoundryLabs, Inc."
(E2B). See the upstream `LICENSE` files:

- https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/LICENSE
- https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/LICENSE

Fetched 2026-10-11.

## `infra/` — github.com/e2b-dev/runtime (formerly `e2b-dev/infra`; GitHub redirects the old name)

Snapshot commit: `cf536c284434fdd2f09bbab35ec807a5dacc87e4` (2026-10-11T10:26:27Z, `main`).
Upstream relative paths are preserved below `infra/`.

| Vendored file | Upstream URL | Last commit touching file | License |
|---|---|---|---|
| `infra/spec/openapi.yml` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/spec/openapi.yml | `ddbaa36e416537575d3950738a8f2ab8553183bc` 2026-10-09T16:57:27Z | Apache-2.0 |
| `infra/spec/openapi-edge.yml` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/spec/openapi-edge.yml | `c13dc2db0160162a21f49b77bab32894fb892e98` 2026-09-07T15:03:07Z | Apache-2.0 |
| `infra/spec/openapi-dashboard.yml` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/spec/openapi-dashboard.yml | `92fb580e3b4c5c4f000c5e6892ee46ec36cd2ccc` 2026-10-09T23:27:54Z | Apache-2.0 |
| `infra/spec/openapi-hyperloop.yml` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/spec/openapi-hyperloop.yml | `fe10b1d6b62ec8a1b2b65751962577c2e5f26d9d` 2026-06-25T07:35:28Z | Apache-2.0 |
| `infra/packages/envd/spec/envd.yaml` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/packages/envd/spec/envd.yaml | `b999fd6180ebbe10bc7227f1746457b6c5ac4c0b` 2026-10-09T14:21:30Z | Apache-2.0 |
| `infra/packages/envd/spec/process/process.proto` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/packages/envd/spec/process/process.proto | `01da054ac9ed73de4b2d803bfa45e02d955ab4c9` 2026-02-09T18:37:23Z | Apache-2.0 |
| `infra/packages/envd/spec/filesystem/filesystem.proto` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/packages/envd/spec/filesystem/filesystem.proto | `90367fa7004447d7ee3548375b305fefeea787f3` 2026-09-29T15:24:18Z | Apache-2.0 |
| `infra/packages/envd/spec/upgrade/handover.proto` | https://github.com/e2b-dev/runtime/blob/cf536c284434fdd2f09bbab35ec807a5dacc87e4/packages/envd/spec/upgrade/handover.proto | `e7b859b30b60d516e2948bf1ce9e3af9da469dea` 2026-10-05T08:57:17Z | Apache-2.0 |

`filesystem.proto` imports `google/protobuf/timestamp.proto` (well-known type, not vendored).
`handover.proto` is envd-internal (in-place envd upgrade state handover); no SDK uses it.

## `sdk/` — github.com/e2b-dev/E2B (the SDK + CLI monorepo; the copies the SDKs generate clients from)

Snapshot commit: `df6c368152b067512702fc677952c0fc4e13c2f0` (2026-10-09T14:54:43Z, `main`).
Upstream relative paths are preserved below `sdk/`.

| Vendored file | Upstream URL | Last commit touching file | License |
|---|---|---|---|
| `sdk/spec/openapi.yml` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/openapi.yml | `b52cd30c14bf7038ba5bfbd15456b1dabe567082` 2026-10-08T13:27:51Z | Apache-2.0 |
| `sdk/spec/openapi-volumecontent.yml` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/openapi-volumecontent.yml | `4fcf7cb1504e20ea1933e5b36d7405cc6032b1e7` 2026-07-24T14:37:02Z | Apache-2.0 |
| `sdk/spec/mcp-server.json` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/mcp-server.json | `7af41e9fab47358cafdcff2135a5f209934ac61a` 2026-08-19T18:57:12Z | Apache-2.0 |
| `sdk/spec/runtime-ref` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/runtime-ref | `b52cd30c14bf7038ba5bfbd15456b1dabe567082` 2026-10-08T13:27:51Z | Apache-2.0 |
| `sdk/spec/belt-ref` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/belt-ref | `4fcf7cb1504e20ea1933e5b36d7405cc6032b1e7` 2026-07-24T14:37:02Z | Apache-2.0 |
| `sdk/spec/envd/envd.yaml` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/envd/envd.yaml | `b52cd30c14bf7038ba5bfbd15456b1dabe567082` 2026-10-08T13:27:51Z | Apache-2.0 |
| `sdk/spec/envd/process/process.proto` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/envd/process/process.proto | `4fcf7cb1504e20ea1933e5b36d7405cc6032b1e7` 2026-07-24T14:37:02Z | Apache-2.0 |
| `sdk/spec/envd/filesystem/filesystem.proto` | https://github.com/e2b-dev/E2B/blob/df6c368152b067512702fc677952c0fc4e13c2f0/spec/envd/filesystem/filesystem.proto | `b52cd30c14bf7038ba5bfbd15456b1dabe567082` 2026-10-08T13:27:51Z | Apache-2.0 |

## Relationship between the two copies

- Per `sdk/spec/README.md` upstream, `openapi.yml` and `envd/*` in the E2B repo are synced by
  Copybara from `e2b-dev/runtime` at the commit pinned in `spec/runtime-ref`
  (`90ad3bdb1a4ad78161463dc90b66b4f8303897b`; that SHA is not reachable in the public
  `e2b-dev/runtime` history — the public repo is a squashed/filtered mirror). `openapi-volumecontent.yml`
  comes from the private `e2b-dev/belt` repo pinned by `spec/belt-ref`.
- The SDKs generate clients from `sdk/` copies, filtered by `redocly.yaml` to tags
  `sandboxes, snapshots, templates, tags, volumes, secrets` (+ `auth` for JS, used by the CLI for `/teams`),
  dropping anything marked `x-not-implemented: true` (control plane) or `x-internal: true` (envd).
- `process.proto` and `filesystem.proto` are byte-identical between the two copies.
- `infra/spec/openapi.yml` vs `sdk/spec/openapi.yml` (runtime is ~2 days newer):
  - adds `GET /admin/sandboxes/{sandboxID}` (+ `AdminSandbox`, `AdminSandboxState` schemas) — admin only;
  - adds optional `Idempotency-Key` header on `POST /v2/sandboxes` (409 `idempotency_request_mismatch` /
    `idempotency_in_progress`, 422 `idempotency_outcome_unknown`);
  - adds `SandboxEgressProxyTLSConfig` (`tls` on the egress proxy config);
  - marks `NewSandbox.secure` deprecated/ignored;
  - adds `pattern: ^[0-9a-f]{64}$` to template-build `filesHash` fields;
  - adds `x-api-group: delete` annotations.
- `infra/packages/envd/spec/envd.yaml` vs `sdk/spec/envd/envd.yaml`: runtime adds `/init` body fields
  `cpuCount`, `thaw`, response headers `X-Envd-Thaw`, `X-Envd-Init-Clock-Correction`,
  `X-Envd-Init-Unobserved`, and `cpu_possible`/`cpu_target`/`cpu_target_attempts`/`cpu_write_pending_ms`
  in `Metrics`. All are orchestrator-facing (`x-internal`) or additive.

## Template provisioning references (runtime @ cf536c284434fdd2f09bbab35ec807a5dacc87e4)

Vendored verbatim as the reference for vibemon's template provisioning
(base phase distro profiles, init-system setup, finalize configuration):

- `infra/packages/orchestrator/pkg/template/build/phases/base/provision.sh`
- `infra/packages/orchestrator/pkg/template/build/phases/base/distro/distro.go`
- `infra/packages/orchestrator/pkg/template/build/phases/base/distro/init.go`
- `infra/packages/orchestrator/pkg/template/build/phases/finalize/configure.sh`

Source: https://github.com/e2b-dev/runtime/tree/cf536c284434fdd2f09bbab35ec807a5dacc87e4/packages/orchestrator/pkg/template/build/phases — Apache-2.0.
