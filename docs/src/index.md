<div class="vm-hero">
<div class="vm-eyebrow">KVM · HVF microVM platform</div>
<h1 class="vm-wordmark">Vibe<span>mon</span></h1>
<p class="vm-lead">Self-hosted sandboxes that speak the <strong>E2B API</strong>. Point the official E2B SDKs or the <code>e2b</code> CLI at <code>vmon</code> and every sandbox runs as a <strong>hardware-isolated microVM</strong>, forked copy-on-write from a template snapshot.</p>
<div class="vm-term"><b>$</b>vmon serve --role node,driver</div>
</div>

Vibemon is one Rust binary, `vmon`. A **driver** role serves the E2B REST API,
sandbox ingress, and scheduling; a **node** role hosts the microVMs and builds
templates. Run both in one process on a laptop, or any number of each across
hosts, joined by gossip, with drivers sharing a raft-replicated KV and MySQL
holding durable state. There is no vibemon SDK: the clients are the official
`e2b`, `e2b_code_interpreter` / `@e2b/code-interpreter`, `e2b_desktop` /
`@e2b/desktop` packages and the `e2b` CLI, unmodified.

```python
# E2B_API_KEY=e2b_… E2B_DOMAIN=localhost
# E2B_API_URL=http://127.0.0.1:3000 E2B_SANDBOX_URL=http://127.0.0.1:3000
from e2b import Sandbox

sbx = Sandbox.create()                      # copy-on-write fork of the `base` template
print(sbx.commands.run("uname -a").stdout)  # runs inside the microVM
sbx.pause()                                 # memory + disk snapshot
sbx = Sandbox.connect(sbx.sandbox_id)       # resumed where it left off
sbx.kill()
```

## Start here

<div class="vm-cards">
<a class="vm-card" href="get-started/quickstart.md">
<span class="vm-card-kicker">Get started</span>
<span class="vm-card-title">Quickstart</span>
<span class="vm-card-body">A monolithic server, a team key, the <code>base</code> template, and a first sandbox on one machine.</span>
</a>
<a class="vm-card" href="get-started/clients.md">
<span class="vm-card-kicker">Use</span>
<span class="vm-card-title">Connect the E2B SDKs</span>
<span class="vm-card-body">Environment variables, ingress modes, CLI login, and calling vibemon extensions.</span>
</a>
<a class="vm-card" href="concepts/architecture.md">
<span class="vm-card-kicker">Understand</span>
<span class="vm-card-title">Architecture</span>
<span class="vm-card-body">Drivers, nodes, the per-VM monitor, the guest agent, and where state lives.</span>
</a>
<a class="vm-card" href="operate/serve.md">
<span class="vm-card-kicker">Operate</span>
<span class="vm-card-title">Run <code>vmon serve</code></span>
<span class="vm-card-body">Roles, listeners, data directories, privileges, and capacity.</span>
</a>
<a class="vm-card" href="operate/cluster.md">
<span class="vm-card-kicker">Scale</span>
<span class="vm-card-title">Clusters</span>
<span class="vm-card-body">Gossip, raft bootstrap, placement, draining, and failover across hosts.</span>
</a>
<a class="vm-card" href="reference/e2b-compat.md">
<span class="vm-card-kicker">Reference</span>
<span class="vm-card-title">E2B compatibility</span>
<span class="vm-card-body">Every upstream API operation, envd surface, and CLI command, with its status.</span>
</a>
</div>

## Platforms

| | Linux / KVM | macOS 15+ / HVF | Windows / WHP |
|---|---|---|---|
| Guest architecture | x86_64, aarch64 | aarch64 | x86_64 |
| node role | ✅ TAP + NAT, transparent egress proxy | ✅ user-mode NAT, in-VMM egress filter | — |
| driver role | ✅ | ✅ | — |
| `vmon vmm` | ✅ | ✅ | ✅ |

Details per platform: [Installation](get-started/installation.md) and
[Networking and egress](operate/networking.md).

## Source and design

The code lives at [github.com/stencil-hq/vibemon](https://github.com/stencil-hq/vibemon).
[`ARCHITECTURE.md`](https://github.com/stencil-hq/vibemon/blob/main/ARCHITECTURE.md)
is the normative design; these pages describe how to run and use the system.
