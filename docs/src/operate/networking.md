# Networking and egress

How sandboxes reach the network on each platform, and how vibemon enforces the E2B egress policy (`allow_internet_access`, `network.allowOut`/`denyOut`, `rules`, `egressProxy`). Read this before running nodes on Linux or before giving sandboxes a restricted network.

Flag reference: [`vmon serve` options](../reference/serve.md#networking-and-egress).

Inbound traffic never uses the guest network: the driver's ingress reaches guest ports through tunnels over the agent channel, so services bound to `localhost` in the guest work and no inbound NAT exists on any platform. See [Domains, TLS, and ingress](ingress.md).

## Network modes

`vmon serve --net` (`VMON_NET`) selects how a node connects its VMs.

| Value | Linux | macOS |
|---|---|---|
| `auto` (default) | `tap`, or no networking without net-admin (see below) | `user` |
| `tap` | Host TAP per sandbox + NAT via iptables | Rejected at startup |
| `user` | Rejected at startup | User-mode NAT inside the VMM (libslirp) |

On Linux, `--net auto` without root or `CAP_NET_ADMIN` starts the node with a warning (`no CAP_NET_ADMIN: this node cannot network sandboxes and will reject creates`) and every create on it fails. `--net tap` without the capability refuses to start. Nodes advertise `net-tap` or `net-user` in their gossip features; an `auto` node without net-admin advertises neither.

Guests run no DHCP client: after every boot, fork, and resume the node pushes the address over the agent channel.

| | Linux TAP | macOS user-net |
|---|---|---|
| Guest address | `<lease>+2`/30 | `10.0.2.15/24` |
| Gateway | `<lease>+1` (TAP side) | `10.0.2.2` |
| DNS | `8.8.8.8`, `1.1.1.1` | `10.0.2.3` (slirp resolver) |

## Linux: TAP and NAT

Each sandbox gets a TAP device `vmon<N>` with a /30 lease from `--net-pool` (`VMON_NET_POOL`, default `172.20.0.0/16`). The pool prefix must be between /8 and /30 with no host bits set; a /16 holds 16384 leases. Leases are handed out round-robin, so a just-released block is reused as late as possible.

Setting up a TAP:

- creates the device (owned by the `--vmm-user` uid when VMMs drop privileges), assigns the host address, brings it up, and attempts to disable IPv6 on it (write errors are ignored);
- writes `1` to `/proc/sys/net/ipv4/ip_forward`;
- installs, in one `iptables-restore --noflush` transaction per table: nat `POSTROUTING` `MASQUERADE` for the lease, filter `FORWARD` accept rules for the guest, and mangle `PREROUTING` `CONNMARK --set-mark <index+1>` tagging every guest flow;
- installs the egress hooks with closed policy chains (below).

The node needs `ip`, `iptables`, `iptables-restore`, `ip6tables`, and `ip6tables-restore` on the host.

### Ready TAP pool

After restart recovery, the node removes idle `vmon<N>` TAPs that no recovered sandbox holds, then pre-creates one ready TAP per boot slot (`--boot-concurrency`), at most 32, and refills the pool in the background. Creates and resumes take a ready TAP instead of running `ip`/iptables on the request path.

When a sandbox's VM exits, its TAP's policy chains are reset to closed, the guest's conntrack entries are flushed (one ctnetlink delete filtered by the lease's connection mark; `conntrack-tools` is not needed), and the TAP returns to the pool while the pool holds fewer than twice its target. Otherwise it is torn down: egress rules, NAT rules, and the device are removed and the mark's conntrack entries flushed. Node shutdown removes the idle TAPs.

Flushing conntrack on reuse keeps NAT mappings and established UDP state of the previous sandbox from leaking to the next one on the same address.

### Egress hooks

Every pooled TAP carries these rules from creation, tagged `-m comment --comment vmon-egress`:

| Table / chain | Rule |
|---|---|
| nat `PREROUTING` | `-i <tap> -p tcp -j REDIRECT` to the node's transparent egress proxy |
| filter `INPUT` | accept TCP to the proxy port, drop everything else from the TAP: guests never reach host services |
| filter `FORWARD` | jump into `vmon-e-<tap>`: always-denied ranges (with `--egress-internal-allow` exemptions), then `vmon-u-<tap>`: the sandbox's allow (`RETURN`) and deny (`DROP`) CIDRs for UDP/ICMP |
| ip6tables `INPUT`, `FORWARD` | drop everything from the TAP (IPv4-only networking; an `ip6tables-restore` failure logs a warning and skips these drops) |

The policy chains are closed (a single `DROP`) while no sandbox is attached, so a resumed sandbox cannot reach the network before its policy is in place. Attaching a sandbox is one add-only `iptables-restore` that inserts its policy above the `DROP` and runs while the guest configures its network; a live update replaces the chains atomically; detaching resets them to closed in the background.

Template build VMs get a fresh TAP without egress hooks: builds have unrestricted egress.

## macOS: user-mode NAT

The VMM runs libslirp and NATs guest traffic through the host's sockets; networking itself needs no extra privileges or entitlements (the VMM still needs its Hypervisor.framework entitlement). Every user-net sandbox VMM gets a compiled packet filter (`vmon vmm --user-net-policy <file>`, written to `sandboxes/<id>/net-policy.json`), replaced live through the VMM control method `set_net_policy`. Even with an empty policy the filter blocks the private ranges, which include the slirp gateway `10.0.2.2` (the host's loopback).

The filter admits a new flow by the first matching rule:

1. DHCP to slirp, and DNS to `10.0.2.3:53` unless the policy denies `8.8.8.8`.
2. TCP to port 80 or 443 of an address learned from a slirp-DNS answer for a name matching an `allowOut` domain, unless that address is in an always-denied range.
3. Always-denied ranges → drop, unless exempted by `--egress-internal-allow`.
4. `allowOut` CIDRs → pass.
5. `denyOut` CIDRs → drop.
6. Pass.

TCP is judged on the SYN; established flows keep passing after a policy update, so updates affect only new connections. UDP and ICMP are judged per packet. Learned addresses expire with their DNS TTL, clamped to 60 s–1 h, in a table capped at 4096 entries. Addresses resolved through any other resolver (DNS over HTTPS, `8.8.8.8`, …) never unlock a domain.

## Egress policy

The E2B API carries the policy; the driver validates and normalizes it and the node enforces it. It applies from the first instruction the sandbox runs after create, fork, or resume, and follows the sandbox through pause, resume, and resume on another node.

| Field (API / Python SDK / JS SDK) | Meaning |
|---|---|
| `allow_internet_access` / `allow_internet_access` / `allowInternetAccess` | `false` replaces `denyOut` with `["0.0.0.0/0"]`; `allowOut` is kept |
| `network.allowOut` / `allow_out` / `allowOut` | IPs, CIDRs, and domain patterns that are allowed |
| `network.denyOut` / `deny_out` / `denyOut` | IPs and CIDRs that are denied |
| `network.rules` / `rules` / `rules` | Per-domain HTTPS request transforms: `{"<domain>": [{"transform": {"headers": {…}}}]}` |
| `network.egressProxy` / `egress_proxy` / `egressProxy` | SOCKS5 upstream for outbound TCP: `address` (`host:port`), `username`, `password`, `tls` (`enabled`, `serverName`, `caCert`) |

### Matching

- **IP / CIDR**: IPv4 or IPv6; a bare address becomes `/32` or `/128`. In `denyOut`, an unspecified address (`0.0.0.0`, `::`) is only accepted as the literal `0.0.0.0/0` (the SDKs export it as `ALL_TRAFFIC`).
- **Domains** (`allowOut` only): `example.com` (exact), `*.example.com` (strict subdomains at any depth, not the apex), or `*` (every hostname). Matching is case-insensitive. A domain is recognized from the HTTP `Host` header on port 80 and the TLS SNI on port 443; on other ports only CIDR rules apply.
- `allowOut` with any domain requires `denyOut` to contain `0.0.0.0/0`; otherwise the request fails with 400.
- When `allowOut` has domains, `8.8.8.8` is implicitly allowed so DNS works under deny-all.

Precedence for a TCP flow:

1. Allowed domain → allowed. The flow is re-dialed by hostname from the host, and a resolved address in an always-denied range is never dialed.
2. Destination in an always-denied range → denied.
3. `allowOut` CIDR → allowed.
4. `denyOut` CIDR → denied.
5. Otherwise allowed.

UDP and ICMP follow steps 2–5.

### Always-denied ranges

By default, sandboxes cannot reach these destinations:

```text
10.0.0.0/8      100.64.0.0/10   127.0.0.0/8     169.254.0.0/16
172.16.0.0/12   192.168.0.0/16  ::1/128         fc00::/7        fe80::/10
```

`vmon serve --egress-internal-allow` (`VMON_EGRESS_INTERNAL_ALLOW`, comma-separated CIDRs) exempts ranges on that node, for example an internal package mirror:

```sh
vmon serve --role node --egress-internal-allow 10.20.0.0/16,192.168.5.10/32
```

The exemption only lifts the always-denied block; the sandbox's own `denyOut` still applies.

### HTTPS header rules

`rules` injects or overrides request headers on HTTPS requests to matching hosts. Keys are exact names or a single leading wildcard (`*.example.com`); a bare `*` is rejected, the most specific key wins, and matching rule sets are not merged. Rules do not grant access: the destination must also be allowed.

On Linux, a TLS flow to port 443 whose SNI matches a rule is intercepted: the proxy presents a leaf certificate minted for that name by the node's CA, opens verified TLS to the real origin, and proxies HTTP/1.1 (ALPN `http/1.1`) with the headers applied to every request. `Upgrade` requests (WebSocket) are spliced through after the 101.

The CA is created once per node in `<data-dir>/egress/` (`ca.pem`, `ca-key.pem`). Every sandbox on a Linux node receives it in envd `/init`, which appends it to whichever of these guest trust bundles exist (else creates the first):

```text
/etc/ssl/certs/ca-certificates.crt
/etc/pki/tls/certs/ca-bundle.crt
/etc/ssl/cert.pem
/etc/ssl/ca-bundle.pem
```

API limits: at most 10 rule domains, each 1–128 characters; one rule per domain; at most 20 headers per rule; header names ≤ 64 characters, values ≤ 2048.

### SOCKS5 egress proxy

`egressProxy` tunnels every allowed outbound TCP flow through a SOCKS5 proxy you operate, after allow/deny filtering. Domain-allowed flows are sent to the proxy by hostname (remote DNS); others by address. UDP and ICMP are not proxied. `tls.enabled` wraps the hop to the proxy in TLS, verified against `tls.caCert` (PEM, ≤ 8 KiB) instead of the system roots when set; `tls.serverName` overrides the verified name.

Validation: `address` must be `host:port`; `username` and `password` ≤ 255 bytes; `password` requires `username`; `serverName`/`caCert` require `enabled: true`. `GET /sandboxes/{id}` returns the network config without the proxy password and CA.

### Platform support

| Feature | Linux (TAP) | macOS (user-net) |
|---|---|---|
| `allow_internet_access=false` | ✅ | ✅ |
| `allowOut` / `denyOut` CIDRs and IPs | ✅ TCP via the proxy, UDP/ICMP via iptables | ✅ in the VMM filter |
| `allowOut` domains | ✅ Host/SNI, re-dialed by name | ✅ TCP 80/443 to addresses learned from slirp DNS |
| Always-denied ranges, `--egress-internal-allow` | ✅ | ✅ |
| `rules` | ✅ | ❌ |
| `egressProxy` | ✅ | ❌ |
| Node feature | `egress-domains`, `egress-proxy` | `egress-domains` |

Placement only considers nodes with the features a policy needs: `egress-proxy` for `rules` or `egressProxy`, `egress-domains` for domain entries. When no such node exists, create fails with 503 and `error_code` `sandbox_no_compatible_node`. A live update that needs a feature the sandbox's node lacks fails with 400.

## SDK examples

Deny everything except two package hosts:

```python
from e2b import ALL_TRAFFIC, Sandbox

sbx = Sandbox.create(
    network={
        "allow_out": ["pypi.org", "*.pythonhosted.org"],
        "deny_out": [ALL_TRAFFIC],
    },
)
```

```ts
import { ALL_TRAFFIC, Sandbox } from 'e2b'

const sbx = await Sandbox.create({
  network: {
    allowOut: ['pypi.org', '*.pythonhosted.org'],
    denyOut: [ALL_TRAFFIC],
  },
})
```

No internet access:

```python
sbx = Sandbox.create(allow_internet_access=False)
```

```ts
const sbx = await Sandbox.create({ allowInternetAccess: false })
```

Inject a header and tunnel through a SOCKS5 proxy (Linux nodes only):

```python
sbx = Sandbox.create(
    network={
        "rules": {
            "api.example.com": [
                {"transform": {"headers": {"Authorization": "Bearer <token>"}}},
            ],
        },
        "egress_proxy": {
            "address": "proxy.example.com:1080",
            "username": "proxy-user",
            "password": "proxy-password",
        },
    },
)
```

```ts
const sbx = await Sandbox.create({
  network: {
    rules: {
      'api.example.com': [
        { transform: { headers: { Authorization: 'Bearer <token>' } } },
      ],
    },
    egressProxy: {
      address: 'proxy.example.com:1080',
      username: 'proxy-user',
      password: 'proxy-password',
    },
  },
})
```

## Live updates

`PUT /sandboxes/{id}/network` replaces the egress policy of a running sandbox (204; 409 unless running). It is a full replacement: omitted fields (`allowOut`, `denyOut`, `rules`, `egressProxy`) are cleared, while ingress settings (`allowPublicTraffic`, `maskRequestHost`, `httpsPorts`) are kept. The body takes `allow_internet_access` in snake case, as on create.

```python
sbx.update_network({"allow_out": ["github.com"], "deny_out": [ALL_TRAFFIC]})
```

```ts
await sbx.updateNetwork({ allowOut: ['github.com'], denyOut: [ALL_TRAFFIC] })
```

On Linux the node swaps the TAP's policy chains in one transaction and the proxy's policy for the guest. On macOS it sends `set_net_policy` to the VMM; established TCP flows survive.
