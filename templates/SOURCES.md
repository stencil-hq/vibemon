# Template sources

E2B v2 template definitions for the default templates the E2B SDKs ask for by
bare alias, plus vibemon's `ubuntu-systemd` reference template (stock Ubuntu
24.04 with systemd as PID 1). `uv run templates/build.py` (or
`just templates`) builds them as public templates of the system team; see
`build.py` and ARCHITECTURE.md §7 (bare aliases).

| Alias | Directory | Upstream | License |
|---|---|---|---|
| `base` | `base/` | `Template().from_base_image()` (`e2bdev/base`), as the E2B SDK defines it | — (vibemon) |
| `code-interpreter-v1` | `code-interpreter/` | [e2b-dev/code-interpreter `template/`](https://github.com/e2b-dev/code-interpreter/tree/a4934a5dfc6b0763efaa867ef54d07d49bdb1fb1/template) @ `a4934a5dfc6b0763efaa867ef54d07d49bdb1fb1` | Apache-2.0, Copyright 2023 FoundryLabs, Inc. (`code-interpreter/LICENSE`) |
| `desktop` | `desktop/` | [e2b-dev/desktop `template/`](https://github.com/e2b-dev/desktop/tree/1ff98a36306989d155ce5eceab2a2d38c9a8d6d2/template) @ `1ff98a36306989d155ce5eceab2a2d38c9a8d6d2` | Apache-2.0, Copyright 2023 FoundryLabs, Inc. (`desktop/LICENSE`) |
| `mcp-gateway` | `mcp-gateway/` | vibemon implementation of the documented gateway contract (`gateway/`); downloads at build time docker-ce (download.docker.com), [docker/mcp-gateway](https://github.com/docker/mcp-gateway) v0.44.1 (MIT), the Docker MCP Catalog v3, and E2B `spec/mcp-server.json` @ `df6c368` (Apache-2.0); needs the `kernel/` guest kernel (Docker engine in the guest) | — (vibemon) |
| `ubuntu-systemd` | `ubuntu-systemd/` | `Template().from_image("ubuntu:24.04")` plus a probe unit and heartbeat start command; mirrored by `node/src/build/e2e.rs` | — (vibemon) |

Copied byte-for-byte from upstream (do not edit; re-vendor instead):

- `code-interpreter/`: `server/`, `systemd/`, `startup_scripts/`, `matplotlibrc`,
  `jupyter-healthcheck.sh`, `jupyter_server_config.py`,
  `ipython_kernel_config.py`, `requirements.txt`, `LICENSE`.
- `desktop/`: `files/` (except `wallpaper.png` and `novnc/`), `LICENSE`.

vibemon originals: `desktop/files/wallpaper.png` (replaces E2B's wallpaper) and
`desktop/files/novnc/` (noVNC title icons replacing the e2b-dev/noVNC fork's
E2B ones), rendered from `desktop/branding/` by `desktop/branding/render.sh`.

Modified (each file's header lists the changes): `code-interpreter/template.py`
(from `template/template.py` + `build_prod.py`) and `desktop/template.py`
(from `template/template.py` + `build_prod.py`). Both pick architecture-specific
packages at build time so the templates build on aarch64 and x86_64 guests.

Each `<dir>/template.py` exposes `make_template()` and the build resources
`CPU_COUNT`, `MEMORY_MB` (optionally `MIN_FREE_DISK_MB`), which `build.py`
passes to `Template.build`.
