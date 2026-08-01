from __future__ import annotations

import asyncio
import inspect

import pytest
from v1_stub import V1StubServer, configure_context

import vmon
from vmon import APIError, ProtocolError, connect


def test_client_root_operations_stream_events_and_send_bearer_auth(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        server.required_token = "test-token"
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            assert client.health().ok is True
            assert client.info().version == "test"
            assert "vmon_test_total 1" in client.metrics()
            assert client.snapshots.list() == []
            with client.events() as stream:
                events = list(stream)
                assert events[0]["type"] == "ready"
                assert events[0]["sequence"] == 1

        http_paths = {"/healthz", "/metrics"}
        checked = [request for request in server.requests if request.path in http_paths]
        assert {request.path for request in checked} == http_paths
        assert all(request.headers["authorization"] == "Bearer test-token" for request in checked)
        for method in ("SystemService/Info", "SystemService/Events", "SnapshotService/List"):
            assert server.last_rpc(method).headers["authorization"] == "Bearer test-token"


def test_typed_responses_reject_malformed_payloads(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        with connect() as client:
            health_responses: tuple[object, ...] = (
                {},
                [],
                None,
                {"ok": True, "bad": float("nan")},
            )
            for response in health_responses:
                server.health_response = response
                with pytest.raises(ProtocolError):
                    client.health()

            server.server_info_response = {}
            with pytest.raises(ProtocolError):
                client.info()

            sandbox = client.sandboxes.create(name="malformed")
            metric_responses: tuple[object, ...] = (
                [],
                None,
                {"bad": float("nan")},
            )
            for response in metric_responses:
                server.sandbox_metrics_response = response
                with pytest.raises(ProtocolError):
                    sandbox.metrics()
            event_responses: tuple[list[object], ...] = (
                [[]],
                [{"bad": float("nan")}],
            )
            for response in event_responses:
                server.events = response
                with client.events() as stream, pytest.raises(ProtocolError):
                    list(stream)

            server.sandboxes["malformed"]["network"] = {"block_network": "false"}
            with pytest.raises(ProtocolError):
                sandbox.network()

            server.sandboxes["malformed"]["tunnels"] = {
                "8080": {"host": "127.0.0.1", "port": "bad"}
            }
            with pytest.raises(ProtocolError):
                sandbox.tunnels()


def test_pool_namespace_escapes_references_and_owns_resources(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            pool = client.pools.set("python/base", 2, memory=256)
            assert pool.stats().ready == 2
            assert [(item.ref, item.count) for item in client.pools.list()] == [("python/base", 2)]
            pool.delete()
            pool.delete()
            assert client.pools.list() == []
            assert len(server.rpcs("PoolService/Delete", reference="python/base")) == 1

            client.pools.set("one", 1)
            client.pools.set("two", 1)
            client.pools.clear()
            assert client.pools.list() == []


def test_sandbox_lifecycle_network_snapshots_and_forks(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            sandbox = client.sandboxes.create(
                image="python:3.14-slim",
                context=".",
                name="box/name",
                credentials=["github-api"],
                tags={"suite": "coverage"},
                ha="async+rerun",
            )
            create_request = server.last_rpc("SandboxService/Create")
            assert create_request.json["context"] == "."
            assert create_request.json["name"] == "box/name"

            assert create_request.json["credentials"] == ["github-api"]
            assert sandbox.info.desired_state == "running"
            assert sandbox.info.observed_state == "running"
            assert sandbox.info.state_generation == 1
            assert sandbox.info.lifecycle_failure is None
            assert sandbox.info.ha == "async+rerun"
            assert sandbox.info.restart_policy == "rerun"
            policy = sandbox.network()
            assert policy.block_network is False
            assert policy.cidr_allow == ()
            assert policy.domain_allow == ()
            updated_policy = sandbox.set_network(
                {
                    "block_network": True,
                    "cidr_allow": ["10.0.0.0/8"],
                    "domain_allow": ["example.com"],
                }
            )
            assert updated_policy.block_network is True
            assert updated_policy.cidr_allow == ("10.0.0.0/8",)
            assert updated_policy.domain_allow == ("example.com",)
            assert sandbox.pause().observed_state == "paused"
            assert sandbox.resume().observed_state == "running"
            suspended = sandbox.suspend()
            assert suspended.desired_state == "suspended"
            assert suspended.observed_state == "suspended"
            assert suspended.state_generation == 4
            assert suspended.lifecycle_failure is None
            assert sandbox.resume().observed_state == "running"
            points = sandbox.history()
            assert points[0].name == "recovery-box/name"
            assert points[0].kind == "checkpoint"
            assert points[0].created_at_unix_millis == 1_700_000_000_000
            assert sandbox.rollback(points[0].name).observed_state == "running"
            assert sandbox.extend(15).raw["deadline_unix"] == 1_800_000_000
            assert sandbox.set_idle_timeout(0).raw["idle_timeout_secs"] == 0
            idle_update = server.last_rpc("SandboxService/SetIdleTimeout", id="box/name")
            assert idle_update.json["idle_timeout_secs"] == 0
            assert sandbox.metrics()["vcpu_exits"] == 7
            assert sandbox.node == server.node_id
            assert sandbox.migrate("node-b").node == "node-b"
            migration = server.last_rpc("SandboxService/Migrate", id="box/name")
            assert migration.json["target"] == "node-b"

            assert sandbox.snapshot("base-snapshot") == "base-snapshot"
            assert client.snapshots.list() == ["base-snapshot"]
            client.snapshots.delete("base-snapshot")
            assert client.snapshots.list() == []
            assert sandbox.snapshot_filesystem("filesystem-image") == "filesystem-image"

            restored = client.snapshots.restore("base-snapshot", name="restored")
            assert restored.id == "restored"
            clones = client.snapshots.fork("base-snapshot", count=2)
            assert len(clones) == 2
            assert len({clone.id for clone in clones}) == 2

            assert sandbox.refresh().name == "box/name"
            listed = client.sandboxes.list(tags={"suite": "coverage"})
            assert [item.id for item in listed] == ["box/name"]
            assert (
                server.last_rpc("SandboxService/Get", id="box/name").headers["authorization"]
                == "Bearer test-token"
            )

            for clone in clones:
                clone.remove()
            restored.terminate()
            restored.remove()
            assert "restored" not in server.sandboxes
            sandbox.stop()
            assert sandbox.info.status == "stopped"
            sandbox.remove()


def test_exec_files_attach_logs_tunnels_and_port_proxies(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        with connect() as client:
            sandbox = client.sandboxes.create(name="streams")
            server.sandboxes["streams"]["tunnels"] = {"8080": {"host": "127.0.0.1", "port": 48080}}

            captured = sandbox.run("printf", "captured")
            assert captured.returncode == 0
            assert captured.stdout == b"captured"
            assert captured.stderr == b""

            process = sandbox.exec("printf", "streamed")
            assert process.wait(timeout=5).code == 0
            assert process.stdout.read() == b"streamed"

            sandbox.files.write_bytes("/tmp/remove-me", b"gone")
            sandbox.files.delete("/tmp/remove-me")
            with pytest.raises(APIError) as exc_info:
                sandbox.files.read_bytes("/tmp/remove-me")
            assert exc_info.value.code == "not_found"

            with sandbox.attach() as console:
                assert console.read() == b"created\n"
            with sandbox.logs(follow=True) as logs:
                assert list(logs) == ["created\n"]
            assert sandbox.logs() == "created\n"

            tunnels = sandbox.tunnels()
            assert tunnels.connect_token == "token-streams"
            assert tunnels[8080].host == "127.0.0.1"
            assert tunnels[8080].port == 48080
            methods = ["GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH"]
            for method in methods:
                response = sandbox.ports.http(
                    8080,
                    method,
                    "api/a b",
                    params={"q": "x y", "token": "caller-token"},
                    headers={"Authorization": "must-not-replace-daemon-auth"},
                    content=b"payload" if method in {"PUT", "POST", "PATCH"} else None,
                )
                assert response.status_code == 200
                if method != "HEAD":
                    assert response.json()["method"] == method
                    assert response.json()["path"] == "api/a b"

            proxy_request = server.last("PATCH", "/v1/sandboxes/streams/ports/8080/api/a%20b")
            assert proxy_request.headers["authorization"] == "Bearer test-token"
            assert proxy_request.query == {
                "q": ["x y"],
                "connect_token": ["token-streams"],
            }

            traversal = sandbox.ports.http(8080, "GET", "../admin")
            assert traversal.json()["path"] == "../admin"
            server.last("GET", "/v1/sandboxes/streams/ports/8080/%2E%2E/admin")

            with sandbox.ports.websocket(
                8080,
                "echo/path",
                params={"q": "hello world", "access_token": "caller"},
            ) as websocket:
                websocket.send_bytes(b"echo")
                assert websocket.recv_message() == b"echo"
            websocket_request = server.last("GET", "/v1/sandboxes/streams/ports/8080/ws/echo/path")
            assert websocket_request.query == {
                "q": ["hello world"],
                "connect_token": ["token-streams"],
            }
            assert websocket_request.headers["authorization"] == "Bearer test-token"

            sandbox.remove()


def test_shell_and_aio_preserve_streaming_semantics(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)

        with connect() as client:
            process = client.shell("printf", "shell-output", timeout=5)
            assert process.wait(timeout=5).code == 0
            assert process.stdout.read() == b"shell-output"
            assert process.sandbox_id == "shell-stub"

            sandbox = client.sandboxes.create(name="async-box")

            async def exercise() -> None:
                result = await sandbox.aio.run("printf", "async-output")
                assert result.stdout == b"async-output"
                assert (await sandbox.aio.network()).block_network is False
                assert (await sandbox.aio.pause()).status == "paused"
                assert (await sandbox.aio.resume()).status == "running"
                await sandbox.aio.files.write_text("/tmp/value", "value")
                assert await sandbox.aio.files.read_text("/tmp/value") == "value"
                await sandbox.aio.remove()

            asyncio.run(exercise())


def test_stream_errors_are_structured(monkeypatch, mvm_home) -> None:
    with V1StubServer() as server:
        server._create_sandbox({"name": "protected"})
        server.required_token = "correct-token"
        configure_context(monkeypatch, mvm_home, server, token="wrong-token")
        with connect() as client:
            sandbox = client.sandboxes.ref("protected")
            with pytest.raises(APIError) as exc_info:
                sandbox.attach()
            assert exc_info.value.code == "unauthorized"
            assert "invalid bearer token" in str(exc_info.value)

    with V1StubServer() as server:
        configure_context(monkeypatch, mvm_home, server)
        with connect() as client:
            missing = client.sandboxes.ref("missing")
            with pytest.raises(APIError) as exc_info:
                missing.attach()
            assert exc_info.value.code == "not_found"


def test_all_exported_callables_have_useful_docs() -> None:
    undocumented = [
        name
        for name in vmon.__all__
        if callable(symbol := getattr(vmon, name)) and not inspect.getdoc(symbol)
    ]
    assert undocumented == []
