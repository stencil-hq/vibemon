#!/usr/bin/env python3
"""Run the official E2B SDK test suites against a fresh monolithic `vmon serve`.

One command, end to end (see e2e/README.md):

    python3 e2e/run.py python          # upstream packages/python-sdk/tests
    python3 e2e/run.py js              # upstream packages/js-sdk/tests
    python3 e2e/run.py all             # both
    python3 e2e/run.py serve           # bring the stack up, print the SDK env, block
    python3 e2e/run.py shell           # stack up + interactive $SHELL with the SDK env

Steps: fetch upstream E2B at a pinned commit into e2e/.cache, start `vmon
serve` with a clean data dir, mint a team + API key with `vmon admin` against
its cluster listener, build the `base` and `httpbin` templates with the
official SDK, run the suite with the SDK environment, and tear everything down
(server, leftover VMMs, data dir) unless --keep is given.

Standard library only; needs `uv` (Python suite + template builds), `pnpm` and
`node` (JS suite).
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
E2E = REPO / "e2e"
CACHE = E2E / ".cache"

# github.com/e2b-dev/E2B snapshot the suites run from; matches the vendored
# specs in spec/e2b/SOURCES.md. Bump both together.
E2B_REF = "df6c368152b067512702fc677952c0fc4e13c2f0"

READY_LINE = "e2e: stack ready"


def log(msg: str) -> None:
    print(f"e2e: {msg}", file=sys.stderr, flush=True)


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    log("$ " + shlex.join(str(c) for c in cmd))
    return subprocess.run(cmd, check=True, **kw)


# ----------------------------------------------------------------- upstream


def fetch_e2b(ref: str) -> Path:
    """Unpacks the upstream E2B monorepo at `ref` into e2e/.cache/E2B-<ref>."""
    dest = CACHE / f"E2B-{ref}"
    if (dest / ".complete").exists():
        return dest
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True)
    url = f"https://codeload.github.com/e2b-dev/E2B/tar.gz/{ref}"
    log(f"fetching {url}")
    with urllib.request.urlopen(url) as resp, tarfile.open(fileobj=resp, mode="r|gz") as tar:
        for member in tar:
            parts = Path(member.name).parts[1:]
            if not parts:
                continue
            member.name = str(Path(*parts))
            tar.extract(member, dest, filter="tar")
    (dest / ".complete").touch()
    return dest


# ---------------------------------------------------------------- the stack


class Stack:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.name = args.name
        self.data_dir = Path(args.data_dir or f"/tmp/vmon-{self.name}")
        slot = args.slot
        self.api_port = slot * 100
        self.cluster_port = 7047 + (slot - 30) * 100
        self.gossip_port = self.cluster_port - 1
        self.api = f"http://127.0.0.1:{self.api_port}"
        self.state_file = self.data_dir / "e2e-state.json"
        self.server: subprocess.Popen | None = None
        self.state: dict = {}
        self.tls = args.ingress == "tls"
        self.domain = args.domain or (f"{self.name}.localhost" if self.tls else "e2e.vmon.test")
        self.sudo = ["sudo", "-n"] if args.sudo else []

    # -- lifecycle

    def up(self) -> None:
        args = self.args
        reuse = args.reuse and self.state_file.exists()
        if reuse:
            self.state = json.loads(self.state_file.read_text())
            log(f"reusing {self.data_dir}")
        else:
            self.destroy_persistent()
            self.data_dir.mkdir(parents=True)
            if args.oci_cache:
                # Content-addressed blobs written via rename: safe to share
                # between stacks and runs, and saves the multi-minute pulls.
                Path(args.oci_cache).mkdir(parents=True, exist_ok=True)
                (self.data_dir / "cache").mkdir()
                (self.data_dir / "cache" / "oci").symlink_to(Path(args.oci_cache).resolve())
            if self.tls:
                self.make_certs()
            self.state = {"secret": secrets.token_hex(24)}
        self.start_server()
        if not reuse:
            # `vmon admin` writes through the running driver's cluster listener.
            out = subprocess.run(
                [args.vmon, "admin", "--cluster-url", f"http://127.0.0.1:{self.cluster_port}",
                 "--cluster-secret", self.state["secret"], "create-team", f"e2e-{self.name}"],
                check=True, capture_output=True, text=True,
            ).stdout
            self.state["api_key"] = next(
                line.split()[-1] for line in out.splitlines() if line.startswith("api key")
            )
            self.state_file.write_text(json.dumps(self.state))
        self.build_templates()
        for name, pyqwest in (("env.sh", True), ("env-node.sh", False)):
            (self.data_dir / name).write_text(
                "unset E2B_API_URL E2B_SANDBOX_URL E2B_HTTP_VERSION E2B_DEBUG\n"
                + "".join(f"export {k}={shlex.quote(v)}\n" for k, v in self.sdk_env(pyqwest=pyqwest).items())
            )
        print(f"{READY_LINE}: source {self.data_dir / 'env.sh'} (Python) or "
              f"{self.data_dir / 'env-node.sh'} (Node)", flush=True)

    def start_server(self) -> None:
        args = self.args
        log_path = self.data_dir / "serve.log"
        cmd = [
            args.vmon, "serve",
            "--data-dir", str(self.data_dir),
            "--cluster-secret", self.state["secret"],
            "--listen", f"127.0.0.1:{self.api_port}",
            "--cluster-listen", f"127.0.0.1:{self.cluster_port}",
            "--gossip-listen", f"127.0.0.1:{self.gossip_port}",
            "--domain", self.domain,
            "--agent", args.agent,
        ]
        if args.kernel:
            cmd += ["--kernel", args.kernel]
        if self.tls:
            # macOS lets unprivileged processes bind 443 only on the wildcard
            # address; `*.localhost` resolves to ::1 first, so listen dual-stack.
            cmd += [
                "--tls-listen", "[::]:443",
                "--tls-cert", str(self.data_dir / "tls/cert.pem"),
                "--tls-key", str(self.data_dir / "tls/key.pem"),
                # Template file upload URLs. The macOS Python SDK cannot trust
                # our CA (see sdk_env), so there they stay on plain HTTP — like
                # upstream's presigned storage URLs, any reachable URL works.
                "--public-url", self.api if sys.platform == "darwin" else f"https://api.{self.domain}",
            ]
        cmd = self.sudo + cmd + args.serve_arg
        log(f"starting vmon serve (log: {log_path})")
        log("$ " + shlex.join(cmd))
        self.server = subprocess.Popen(
            cmd, stdout=open(log_path, "ab"), stderr=subprocess.STDOUT, start_new_session=True,
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                raise SystemExit(f"vmon serve exited with {self.server.returncode}; see {log_path}")
            try:
                with urllib.request.urlopen(f"{self.api}/health", timeout=2) as resp:
                    if resp.status < 300:
                        log(f"vmon serve healthy at {self.api}")
                        return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(0.5)
        raise SystemExit(f"vmon serve not healthy after 120s; see {log_path}")

    def make_certs(self) -> None:
        """A throwaway CA and a `*.<domain>` leaf for the TLS ingress, plus a
        CA bundle (system roots + ours) for SSL_CERT_FILE."""
        tls = self.data_dir / "tls"
        tls.mkdir()
        ext = tls / "leaf.ext"
        ext.write_text(
            f"subjectAltName=DNS:{self.domain},DNS:*.{self.domain}\n"
            "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
        )
        quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
        run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
             "-nodes", "-days", "30", "-subj", f"/CN=vmon e2e CA {self.name}",
             "-addext", "basicConstraints=critical,CA:TRUE",
             "-addext", "keyUsage=critical,keyCertSign,cRLSign",
             "-keyout", str(tls / "ca.key"), "-out", str(tls / "ca.pem")], **quiet)
        run(["openssl", "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
             "-subj", f"/CN=*.{self.domain}", "-keyout", str(tls / "key.pem"),
             "-out", str(tls / "leaf.csr")], **quiet)
        run(["openssl", "x509", "-req", "-in", str(tls / "leaf.csr"), "-CA", str(tls / "ca.pem"),
             "-CAkey", str(tls / "ca.key"), "-CAcreateserial", "-days", "30",
             "-extfile", str(ext), "-out", str(tls / "cert.pem")], **quiet)
        system = next(
            (Path(p) for p in ("/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt",
                               "/etc/ssl/cert.pem") if Path(p).is_file()),
            None,
        )
        bundle = (system.read_text() if system else "") + (tls / "ca.pem").read_text()
        (tls / "bundle.pem").write_text(bundle)

    def build_templates(self) -> None:
        checkout = self.args.checkout
        run(
            ["uv", "run", "--project", str(checkout / "packages/python-sdk"),
             "python", str(E2E / "build_templates.py"), str(checkout), *self.args.templates],
            env=self.env(),
        )

    def down(self) -> None:
        if self.server and self.server.poll() is None:
            log("stopping vmon serve")
            self.signal_server("TERM")
            try:
                self.server.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.signal_server("KILL")
                self.server.wait()
        # VMMs are spawned in their own sessions; reap any that outlived serve.
        subprocess.run(self.sudo + ["pkill", "-KILL", "-f", f"{self.data_dir}/"], check=False)
        if not self.args.keep:
            self.destroy_persistent()

    def signal_server(self, sig: str) -> None:
        """Signals serve's process group (sudo-owned when --sudo)."""
        if self.sudo:
            subprocess.run(self.sudo + ["kill", f"-{sig}", f"-{self.server.pid}"], check=False)
        else:
            os.killpg(self.server.pid, getattr(signal, f"SIG{sig}"))

    def destroy_persistent(self) -> None:
        if self.data_dir.exists():
            if self.sudo:
                subprocess.run(self.sudo + ["rm", "-rf", str(self.data_dir)], check=True)
            else:
                shutil.rmtree(self.data_dir)

    # -- environment

    def sdk_env(self, *, pyqwest: bool = True) -> dict[str, str]:
        """SDK environment.

        `plain` ingress: the SDKs route API and envd traffic to the plain-HTTP
        listener through E2B_API_URL / E2B_SANDBOX_URL (`E2b-Sandbox-*`
        headers); `<port>-<id>.<domain>` hosts are unreachable.

        `tls` ingress: exactly upstream CI's environment (E2B_API_KEY +
        E2B_DOMAIN) with our CA trusted, except for the Python SDK on macOS:
        its pyqwest transport verifies through the macOS keychain
        (rustls-platform-verifier ignores SSL_CERT_FILE there), so its API and
        envd traffic keeps the plain-HTTP routing. `pyqwest=False` is for
        Node/httpx/urllib consumers, which honor the CA bundle everywhere.
        """
        env = {"E2B_API_KEY": self.state["api_key"], "E2B_DOMAIN": self.domain}
        plain = {"E2B_API_URL": self.api, "E2B_SANDBOX_URL": self.api}
        if not self.tls:
            return env | plain
        tls = self.data_dir / "tls"
        env |= {"SSL_CERT_FILE": str(tls / "bundle.pem"), "NODE_EXTRA_CA_CERTS": str(tls / "ca.pem")}
        if pyqwest and sys.platform == "darwin":
            env |= plain
        return env

    def env(self, *, pyqwest: bool = True, **extra: str) -> dict[str, str]:
        drop = {"E2B_DEBUG", "E2B_ACCESS_TOKEN", "E2B_API_URL", "E2B_SANDBOX_URL", "E2B_HTTP_VERSION"}
        env = {k: v for k, v in os.environ.items() if k not in drop}
        env.update(self.sdk_env(pyqwest=pyqwest))
        env.update(extra)
        return env


# ------------------------------------------------------------------- suites


def junit_counts(path: Path) -> dict[str, int]:
    counts = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    if not path.exists():
        return counts
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    for suite in suites:
        for key in counts:
            counts[key] += int(suite.get(key, 0) or 0)
    return counts


def run_python(stack: Stack, extra: list[str]) -> int:
    """Upstream CI's `uv run pytest --verbose --numprocesses=4`.

    When the Python SDK needs the E2B_API_URL/E2B_SANDBOX_URL routing (plain
    ingress, or TLS ingress on macOS), the offline tests — mock servers and
    ConnectionConfig unit tests that assert the URLs derived from E2B_DOMAIN
    alone — run first in a separate pass with exactly upstream's environment,
    since the routing variables would override what they assert. Everything
    else runs in the second pass. An explicit test selection runs as one pass.
    """
    sdk = stack.args.checkout / "packages/python-sdk"
    run(["uv", "sync", "--locked"], cwd=sdk)
    env = stack.env()
    routed = "E2B_SANDBOX_URL" in env
    offline = sorted(str(p.relative_to(sdk)) for p in (sdk / "tests").glob("test_*.py"))
    offline += sorted(str(p.relative_to(sdk)) for p in (sdk / "tests").glob("*/*/test_config_propagation.py"))
    if extra or not routed:
        passes = [("python", env, extra)]
    else:
        upstream_env = {k: v for k, v in env.items() if k not in ("E2B_API_URL", "E2B_SANDBOX_URL")}
        passes = [
            ("python-offline", upstream_env, offline),
            ("python", env, ["tests", *(f"--ignore={path}" for path in offline)]),
        ]
    code = 0
    for label, pass_env, selection in passes:
        junit = stack.data_dir.parent / f"vmon-{stack.name}-{label}-junit.xml"
        cmd = ["uv", "run", "pytest", "--verbose", f"--numprocesses={stack.args.workers}",
               f"--junitxml={junit}", *selection]
        log("$ " + shlex.join(cmd))
        code |= subprocess.run(cmd, cwd=sdk, env=pass_env).returncode
        log(f"{label} junit {junit}: {junit_counts(junit)}")
    return code


def run_js(stack: Stack, extra: list[str]) -> int:
    root = stack.args.checkout
    sdk = root / "packages/js-sdk"
    junit = stack.data_dir.parent / f"vmon-{stack.name}-js-junit.xml"
    run(["pnpm", "install", "--frozen-lockfile", "--config.auto-install-peers=true",
         "--config.exclude-links-from-lockfile=true"], cwd=root)
    run(["pnpm", "build"], cwd=sdk)
    cmd = ["pnpm", "exec", "vitest", "run", "--reporter=default", "--reporter=junit",
           f"--outputFile.junit={junit}", *extra]
    log("$ " + shlex.join(cmd))
    code = subprocess.run(
        cmd, cwd=sdk, env=stack.env(pyqwest=False, E2B_TEST_MAX_WORKERS="1", CI="1")
    ).returncode
    log(f"js-sdk junit {junit}: {junit_counts(junit)}")
    return code


# --------------------------------------------------------------------- main


def default_target_dir() -> Path:
    return Path(os.environ.get("CARGO_TARGET_DIR", REPO / "target"))


def parse_args() -> argparse.Namespace:
    target = default_target_dir()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("suite", choices=["python", "js", "all", "serve", "shell", "none"])
    p.add_argument("--name", default=os.environ.get("VMON_E2E_NAME", "e2e"),
                   help="stack name: data dir /tmp/vmon-<name>, database vmon_<name>")
    p.add_argument("--slot", type=int, default=int(os.environ.get("VMON_E2E_SLOT", "31")),
                   help="port slot 30-39: API <slot>00, cluster 7<slot-30>47, gossip 7<slot-30>46")
    p.add_argument("--data-dir")
    p.add_argument("--vmon", default=os.environ.get("VMON_BIN", str(target / "debug/vmon")))
    arch = "aarch64" if os.uname().machine in ("arm64", "aarch64") else "x86_64"
    p.add_argument("--agent", default=os.environ.get(
        "VMON_AGENT", str(target / f"{arch}-unknown-linux-musl/release/vmon-agent")))
    p.add_argument("--kernel", default=os.environ.get("VMON_KERNEL"),
                   help="guest kernel (default: vmon's own resolution, see `vmon serve --help`)")
    p.add_argument("--domain", help="sandbox domain (default: e2e.vmon.test, or <name>.localhost with --ingress tls)")
    p.add_argument("--ingress", choices=["plain", "tls"], default=os.environ.get("VMON_E2E_INGRESS", "plain"),
                   help="plain: SDK traffic via E2B_API_URL/E2B_SANDBOX_URL on the HTTP listener; "
                        "tls: upstream topology, `https://<port>-<id>.<name>.localhost` on [::]:443 "
                        "with a throwaway CA (one tls stack per host)")
    p.add_argument("--sudo", action="store_true", help="run vmon serve via `sudo -n` (Linux TAP networking)")
    p.add_argument("--oci-cache", default=os.environ.get("VMON_E2E_OCI_CACHE", str(CACHE / "oci")),
                   help="persistent OCI blob cache shared across runs ('' = per-run)")
    p.add_argument("--e2b-ref", default=E2B_REF)
    p.add_argument("--templates", nargs="*", default=["base", "httpbin", "e2e-warm-ubuntu"])
    p.add_argument("--workers", type=int, default=4, help="pytest-xdist workers (upstream CI: 4)")
    p.add_argument("--serve-arg", action="append", default=[], help="extra `vmon serve` argument")
    p.add_argument("--keep", action="store_true", help="keep data dir and database after the run")
    p.add_argument("--reuse", action="store_true",
                   help="reuse a kept data dir/database (skips rebuilding existing templates)")
    argv = sys.argv[1:]
    split = argv.index("--") if "--" in argv else len(argv)
    args = p.parse_args(argv[:split])
    # After `--`: extra pytest/vitest arguments (test selection, -k, ...).
    args.extra = argv[split + 1:]
    for flag in ("vmon", "agent", "kernel"):
        if getattr(args, flag) is None:
            continue
        path = Path(getattr(args, flag)).resolve()
        if not path.is_file():
            p.error(f"--{flag} {path} does not exist")
        setattr(args, flag, str(path))
    return args


def main() -> int:
    args = parse_args()
    args.checkout = fetch_e2b(args.e2b_ref)
    stack = Stack(args)

    def on_signal(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, on_signal)
    code = 0
    try:
        stack.up()
        if args.suite in ("python", "all"):
            code |= run_python(stack, args.extra)
        if args.suite in ("js", "all"):
            code |= run_js(stack, args.extra)
        if args.suite == "shell":
            code = subprocess.run([os.environ.get("SHELL", "bash")], env=stack.env()).returncode
        if args.suite == "serve":
            log("stack is up; Ctrl-C / SIGTERM tears it down")
            while stack.server.poll() is None:
                time.sleep(1)
            code = stack.server.returncode or 1
    except KeyboardInterrupt:
        code = 130
    finally:
        stack.down()
    return code


if __name__ == "__main__":
    sys.exit(main())
