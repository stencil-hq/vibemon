# Distributed, Linux/KVM, and TLS verification

How to rerun the checks for the distributed topology, for the Linux/KVM node
path, and for TLS subdomain routing. The design is specified in
[`ARCHITECTURE.md`](https://github.com/stencil-hq/vibemon/blob/main/ARCHITECTURE.md)
§5 and §9.4.

## 1. Gated distributed test (`tests/cluster.rs`)

```sh
just cluster-e2e
# equivalent on macOS:
CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER=$PWD/demo/hvf-test-runner.sh \
VMON_ENTITLEMENTS=$PWD/hvf.entitlements VMON_CLUSTER_E2E=1 \
  cargo test --test cluster -- --nocapture
```

Requirements:

- `target/test-assets/{Image,vmon-agent}-<arch>`, or set `VMON_KERNEL` and `VMON_AGENT`.
- Optional: `VMON_CLUSTER_NET` (nodes' `--net`, default `user`) and `VMON_CLUSTER_IMAGE` (default `alpine:3.20`).

The test starts 3 `--role driver --bootstrap-expect 3` processes and 2 `--role node` processes, joined through gossip seeds, on loopback ports. Logs go to `/tmp/vce-<pid>/<name>.log`. It runs in about 3 minutes, and each `[phase]` line prints its timings. It asserts each of the following:

1. Raft bootstraps exactly once, every driver lists both nodes in `GET /nodes`, and all of them report one `clusterID`.
2. A template builds through d1. A sandbox created through d1 runs envd exec, file upload, and download through the d1, d2, and d3 ingress. Its `sbx/<id>` KV route reads the same on every driver, both locally and consistently.
3. With the builder `draining`, the next sandbox lands on the other node through a peer artifact fetch.
4. After SIGKILL of the raft leader:
   - a new leader is elected;
   - the survivors return 0 failed API calls;
   - the new leader's reaper kills an 8-second-timeout sandbox and records a `killed` event with reason `timeout`.

   The restarted driver serves consistent KV, and there is no second bootstrap.
5. After SIGKILL of a node:
   - its sandbox gets the `killed` event with reason `orphaned`;
   - placement uses the other node, which needs the artifact-holder record from step 3;
   - when the node restarts, the orphaned VMM, which is still alive, is killed.
6. After SIGTERM of a node, 15 seconds down (gossip `Left`), and a restart, its sandboxes survive, keep their files, and are adopted.

## 2. Linux/KVM in Lima (TAP networking + egress proxy)

```sh
limactl shell kvm -- bash -lc 'cd <worktree> && . ~/.cargo/env && \
  CARGO_TARGET_DIR=$HOME/target-<you> cargo build -p vmon'
# root is needed for TAP and iptables:
limactl shell kvm -- sudo -E $HOME/target-<you>/debug/vmon serve \
  --data-dir /tmp/<you>/mono --run-dir /tmp/<you>-run \
  --listen 127.0.0.1:3500 --cluster-listen 127.0.0.1:3501 --gossip-listen 127.0.0.1:3502 \
  --net tap --net-pool 172.29.0.0/16 --kernel <Image-aarch64> --agent <static vmon-agent> \
  --cluster-secret <s> --domain cluster.test
```

1. Create a key with `vmon admin --cluster-url http://127.0.0.1:3501 --cluster-secret <s> create-team kvm` (inside Lima).
2. Build `alp` with `Template().from_image("alpine:3.20")`. This takes about 6½ minutes under nested KVM.
3. Run `uv run --with e2b python e2e/cluster/linux.py` inside Lima with:

   ```sh
   E2B_API_URL=http://127.0.0.1:3500 E2B_SANDBOX_URL=http://127.0.0.1:3500 \
   E2B_DOMAIN=cluster.test E2B_HTTP_VERSION=1.1
   ```

   The script exercises create, commands, files, pause, resume, kill, the `allowOut` domain, a `denyOut` CIDR, `allow_internet_access=false`, and volumes shared between two sandboxes.
4. For the distributed variant, start `--role driver` and `--role node` processes with distinct ports, data dirs, and run dirs, all with the same `--join` seeds. These are the same flags that `tests/cluster.rs` passes.
5. When finished, remove what you created: `pkill -f <run-dir>`, and check `ip -o link | grep tap` and `iptables -S | grep vmon`.

The Python SDK sends the volume content API (`Volume.read_file`, …) to `https://api.<domain>` and ignores `E2B_API_URL`. That API therefore needs the TLS setup in §3.

## 3. TLS and true subdomain routing (no `E2B_SANDBOX_URL`)

1. Create a CA and a leaf certificate for `*.vmon.test`, `api.vmon.test`, and `vmon.test`:

   ```sh
   openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 30 -subj '/CN=vmon test CA' \
     -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign'
   openssl req -newkey rsa:2048 -nodes -keyout key.pem -out req.csr -subj '/CN=*.vmon.test'
   printf 'subjectAltName=DNS:*.vmon.test,DNS:api.vmon.test,DNS:vmon.test\nextendedKeyUsage=serverAuth\n' > ext.cnf
   openssl x509 -req -in req.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out leaf.pem -days 30 -extfile ext.cnf
   cat leaf.pem ca.pem > cert.pem
   ```

2. Start the server: `vmon serve … --domain vmon.test --tls-listen 127.0.0.1:3463 --tls-cert cert.pem --tls-key key.pem --public-url https://api.vmon.test`.

3. Resolve `*.vmon.test`. The SDKs' HTTP stacks have their own resolvers: Python's pyqwest uses hickory DNS, and Bun uses its own. Without root, use `e2e/cluster/resolver_proxy.py` instead:

   ```sh
   VMON_TLS_PORT=3463 python3 e2e/cluster/resolver_proxy.py 3464
   ```

   It is an HTTPS CONNECT proxy that maps `*.vmon.test:443` to `127.0.0.1:3463`. TLS stays end to end: the SNI, the certificate check, and ALPN negotiation all happen between the SDK and vmon. With root, a dnsmasq `address=/vmon.test/127.0.0.1` entry and port 443 do the same job.

4. Python:

   ```sh
   env -u E2B_SANDBOX_URL -u E2B_API_URL -u E2B_HTTP_VERSION E2B_API_KEY=… E2B_DOMAIN=vmon.test \
     uv run --with e2b --with h2 env PYTHONPATH=e2e/cluster/hook VMON_TEST_CA=ca.pem \
     SSL_CERT_FILE=ca.pem HTTPS_PROXY=http://127.0.0.1:3464 python e2e/cluster/tls.py
   ```

   The hook is needed on macOS because the SDK's pyqwest transports use the platform verifier, which ignores `SSL_CERT_FILE`. The hook passes the test CA through `tls_ca_cert`.

   JS: `NODE_EXTRA_CA_CERTS=ca.pem HTTPS_PROXY=http://127.0.0.1:3464 E2B_DOMAIN=vmon.test bun <script>`.

   To check that the server offers h2 over ALPN: `curl -v --cacert ca.pem --resolve api.vmon.test:3463:127.0.0.1 https://api.vmon.test:3463/health` should print `ALPN: server accepted h2`.
