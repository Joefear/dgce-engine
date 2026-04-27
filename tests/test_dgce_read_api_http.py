import json
from pathlib import Path
from uuid import UUID

from fastapi.testclient import TestClient
import logging
import pytest

from apps.aether_api.main import create_app
from aether.dgce import DGCESection, run_section_with_workspace
from aether.dgce import read_api as dgce_read_api
from aether.dgce.read_api_http import ROUTE_POLICIES, resolve_scope, router as dgce_read_router
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


@pytest.fixture(autouse=True)
def no_auth_env(monkeypatch):
    monkeypatch.delenv("DGCE_API_KEY", raising=False)


def _section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        requirements=["support mission templates", "track progression state"],
        constraints=["keep save format stable", "support mod extension points"],
    )


def _workspace_dir(name: str) -> Path:
    base = Path("tests/.tmp") / name
    if base.exists():
        for path in sorted(base.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        if base.exists():
            base.rmdir()
    return base


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output="Summary output",
        status=ArtifactStatus.EXPERIMENTAL,
        executor="stub",
        metadata={
            "real_model_called": False,
            "model_backend": "stub",
            "model_name": None,
            "estimated_tokens": len(content) / 4,
            "estimated_cost": (len(content) / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "stub",
            "worth_running": True,
        },
    )


def _build_workspace(monkeypatch, name: str) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root)
    return project_root


def _assert_response_safety_headers(response) -> None:
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-XSS-Protection"] == "0"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    request_id = response.headers["X-Request-ID"]
    assert request_id
    assert str(UUID(request_id)) == request_id


class TestDGCEReadAPIHTTP:
    def test_route_policies_and_scope_resolution_are_locked(self):
        assert ROUTE_POLICIES == {
            "/health": "public",
            "/version": "public",
            "/v1/dgce/": "read",
        }
        assert resolve_scope("/health") == "public"
        assert resolve_scope("/version") == "public"
        assert resolve_scope("/v1/dgce/dashboard") == "read"
        assert resolve_scope("/v1/unknown") == "read"

    def test_endpoints_map_directly_to_read_api_functions(self, monkeypatch):
        client = TestClient(create_app())
        calls: list[tuple[str, str]] = []

        def reader(name: str):
            def _reader(workspace_path):
                calls.append((name, str(workspace_path)))
                return {"artifact_type": name}

            return _reader

        monkeypatch.setattr(dgce_read_api, "get_dashboard", reader("dashboard"))
        monkeypatch.setattr(dgce_read_api, "get_workspace_index", reader("workspace_index"))
        monkeypatch.setattr(dgce_read_api, "get_lifecycle_trace", reader("lifecycle_trace"))
        monkeypatch.setattr(dgce_read_api, "get_consumer_contract", reader("consumer_contract"))
        monkeypatch.setattr(dgce_read_api, "get_export_contract", reader("export_contract"))
        monkeypatch.setattr(dgce_read_api, "get_artifact_manifest", reader("artifact_manifest"))
        monkeypatch.setattr(dgce_read_api, "list_gce_stage0_artifacts", reader("gce_stage0_artifact_index"))

        expected_routes = [
            ("/v1/dgce/dashboard", "dashboard"),
            ("/v1/dgce/workspace-index", "workspace_index"),
            ("/v1/dgce/lifecycle-trace", "lifecycle_trace"),
            ("/v1/dgce/consumer-contract", "consumer_contract"),
            ("/v1/dgce/export-contract", "export_contract"),
            ("/v1/dgce/artifact-manifest", "artifact_manifest"),
            ("/v1/dgce/gce/stage0-artifacts", "gce_stage0_artifact_index"),
        ]

        for path, artifact_type in expected_routes:
            response = client.get(path, params={"workspace_path": "workspace-root"})
            assert response.status_code == 200
            assert response.json() == {"artifact_type": artifact_type}
            assert response.json() == {"artifact_type": artifact_type}
            assert set(response.json()) == {"artifact_type"}
            _assert_response_safety_headers(response)

        assert calls == [(artifact_type, "workspace-root") for _, artifact_type in expected_routes]

    def test_http_returns_exact_read_api_payload_without_wrapper_fields(self, monkeypatch):
        client = TestClient(create_app())
        payload = {
            "artifact_type": "dashboard",
            "schema_version": "1.0",
            "sections": [{"section_id": "alpha"}],
        }

        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: payload)

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})

        assert response.status_code == 200
        assert response.json() == payload
        assert set(response.json()) == set(payload)
        assert "data" not in response.json()
        assert "meta" not in response.json()
        _assert_response_safety_headers(response)

    def test_valid_workspace_returns_expected_payloads_and_is_repeatable(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_read_api_http_success")
        client = TestClient(create_app())
        expected_files = {
            "/v1/dgce/dashboard": ".dce/dashboard.json",
            "/v1/dgce/workspace-index": ".dce/workspace_index.json",
            "/v1/dgce/lifecycle-trace": ".dce/lifecycle_trace.json",
            "/v1/dgce/consumer-contract": ".dce/consumer_contract.json",
            "/v1/dgce/export-contract": ".dce/export_contract.json",
            "/v1/dgce/artifact-manifest": ".dce/artifact_manifest.json",
        }

        for route_path, relative_path in expected_files.items():
            first_response = client.get(route_path, params={"workspace_path": str(project_root)})
            second_response = client.get(route_path, params={"workspace_path": str(project_root)})
            expected_payload = json.loads((project_root / relative_path).read_text(encoding="utf-8"))

            assert first_response.status_code == 200
            assert second_response.status_code == 200
            assert first_response.json() == expected_payload
            assert second_response.json() == expected_payload
            assert first_response.content == second_response.content
            _assert_response_safety_headers(first_response)
            _assert_response_safety_headers(second_response)
            assert first_response.headers["X-Request-ID"] != second_response.headers["X-Request-ID"]

    def test_invalid_artifact_returns_http_error(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_read_api_http_invalid")
        dashboard_path = project_root / ".dce" / "dashboard.json"
        invalid_payload = json.loads(dashboard_path.read_text(encoding="utf-8"))
        invalid_payload.pop("sections")
        dashboard_path.write_text(json.dumps(invalid_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        client = TestClient(create_app())

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": str(project_root)})

        assert response.status_code == 400
        assert response.json() == {"detail": response.json()["detail"]}
        assert "dashboard.json" in response.json()["detail"]
        _assert_response_safety_headers(response)

    def test_http_reads_have_no_write_side_effects(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_read_api_http_no_writes")
        client = TestClient(create_app())
        artifact_paths = [
            project_root / ".dce" / "dashboard.json",
            project_root / ".dce" / "workspace_index.json",
            project_root / ".dce" / "lifecycle_trace.json",
            project_root / ".dce" / "consumer_contract.json",
            project_root / ".dce" / "export_contract.json",
            project_root / ".dce" / "artifact_manifest.json",
        ]
        before = {path: path.read_bytes() for path in artifact_paths}

        for route_path in (
            "/v1/dgce/dashboard",
            "/v1/dgce/workspace-index",
            "/v1/dgce/lifecycle-trace",
            "/v1/dgce/consumer-contract",
            "/v1/dgce/export-contract",
            "/v1/dgce/artifact-manifest",
            "/v1/dgce/gce/stage0-artifacts",
        ):
            response = client.get(route_path, params={"workspace_path": str(project_root)})
            assert response.status_code == 200
            _assert_response_safety_headers(response)

        assert before == {path: path.read_bytes() for path in artifact_paths}

    def test_read_router_inventory_is_locked_to_six_get_routes(self):
        dgce_read_routes = {
            route.path: route.methods
            for route in dgce_read_router.routes
            if route.path.startswith("/v1/dgce/")
        }

        assert dgce_read_routes == {
            "/v1/dgce/dashboard": {"GET"},
            "/v1/dgce/workspace-index": {"GET"},
            "/v1/dgce/lifecycle-trace": {"GET"},
            "/v1/dgce/consumer-contract": {"GET"},
            "/v1/dgce/export-contract": {"GET"},
            "/v1/dgce/artifact-manifest": {"GET"},
            "/v1/dgce/gce/stage0-artifacts": {"GET"},
            "/v1/dgce/gce/stage0-artifacts/{artifact_name}": {"GET"},
        }

    def test_app_exposes_no_non_get_methods_for_read_routes(self):
        client = TestClient(create_app())
        response = client.post("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})
        assert response.status_code == 405

    def test_auth_disabled_allows_requests_without_header(self, monkeypatch):
        client = TestClient(create_app())
        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: {"artifact_type": "dashboard"})

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})

        assert response.status_code == 200
        assert response.json() == {"artifact_type": "dashboard"}
        _assert_response_safety_headers(response)

    def test_auth_enabled_allows_matching_key(self, monkeypatch):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        client = TestClient(create_app())
        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: {"artifact_type": "dashboard"})

        response = client.get(
            "/v1/dgce/dashboard",
            params={"workspace_path": "workspace-root"},
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        assert response.json() == {"artifact_type": "dashboard"}
        _assert_response_safety_headers(response)

    def test_public_routes_remain_accessible_without_key_when_auth_enabled(self, monkeypatch):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        client = TestClient(create_app())

        health_response = client.get("/health")
        version_response = client.get("/version")

        assert health_response.status_code == 200
        assert health_response.json() == {"status": "ok"}
        assert version_response.status_code == 200
        assert version_response.json() == {
            "service": "aether-api",
            "dgce_version": "5.x",
            "api_version": "v1",
        }
        _assert_response_safety_headers(health_response)
        _assert_response_safety_headers(version_response)

    def test_auth_enabled_rejects_missing_key_with_401(self, monkeypatch):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        client = TestClient(create_app())

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})

        assert response.status_code == 401
        assert response.json() == {"detail": "Unauthorized"}
        _assert_response_safety_headers(response)

    def test_auth_enabled_rejects_wrong_key_with_401(self, monkeypatch):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        client = TestClient(create_app())

        response = client.get(
            "/v1/dgce/dashboard",
            params={"workspace_path": "workspace-root"},
            headers={"X-API-Key": "wrong-key"},
        )

        assert response.status_code == 401
        assert response.json() == {"detail": "Unauthorized"}
        _assert_response_safety_headers(response)

    def test_missing_artifact_returns_http_404(self):
        client = TestClient(create_app())

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": "tests/.tmp/does-not-exist"})

        assert response.status_code == 404
        assert response.json() == {"detail": response.json()["detail"]}
        _assert_response_safety_headers(response)

    def test_missing_artifact_returns_http_404_with_auth(self, monkeypatch):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        client = TestClient(create_app())

        response = client.get(
            "/v1/dgce/dashboard",
            params={"workspace_path": "tests/.tmp/does-not-exist"},
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404
        assert response.json() == {"detail": response.json()["detail"]}
        _assert_response_safety_headers(response)

    def test_invalid_workspace_path_returns_http_400(self):
        from pathlib import Path

        workspace_path = _workspace_dir("dgce_read_api_http_missing_dce")
        workspace_path.mkdir(parents=True, exist_ok=True)
        client = TestClient(create_app())

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": str(workspace_path)})

        assert response.status_code == 400
        assert response.json() == {"detail": response.json()["detail"]}
        assert ".dce" in response.json()["detail"]
        _assert_response_safety_headers(response)

    def test_relative_escape_workspace_path_returns_http_400(self):
        client = TestClient(create_app())

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": ".."})

        assert response.status_code == 400
        assert response.json() == {"detail": response.json()["detail"]}
        assert "current working directory" in response.json()["detail"]
        _assert_response_safety_headers(response)

    def test_request_id_log_matches_response_header(self, monkeypatch, caplog):
        caplog.set_level(logging.INFO, logger="aether.api")
        client = TestClient(create_app())
        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: {"artifact_type": "dashboard"})

        response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})

        assert response.status_code == 200
        request_id = response.headers["X-Request-ID"]
        log_records = [record for record in caplog.records if record.message == "request complete"]
        assert log_records
        assert log_records[-1].request_id == request_id
        assert log_records[-1].method == "GET"
        assert log_records[-1].path == "/v1/dgce/dashboard"
        assert log_records[-1].status_code == 200

    def test_rate_limit_allows_requests_under_limit(self, monkeypatch):
        monkeypatch.setattr("apps.aether_api.main.time.time", lambda: 1000.0)
        client = TestClient(create_app())
        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: {"artifact_type": "dashboard"})

        for _ in range(60):
            response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})
            assert response.status_code == 200
            assert response.json() == {"artifact_type": "dashboard"}
            _assert_response_safety_headers(response)

    def test_rate_limit_returns_429_when_exceeded(self, monkeypatch):
        monkeypatch.setattr("apps.aether_api.main.time.time", lambda: 1000.0)
        client = TestClient(create_app())
        monkeypatch.setattr(dgce_read_api, "get_dashboard", lambda workspace_path: {"artifact_type": "dashboard"})

        for _ in range(60):
            response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})
            assert response.status_code == 200

        limited_response = client.get("/v1/dgce/dashboard", params={"workspace_path": "workspace-root"})

        assert limited_response.status_code == 429
        assert limited_response.json() == {"detail": "Too Many Requests"}
        _assert_response_safety_headers(limited_response)

    def test_health_and_version_are_not_rate_limited(self, monkeypatch):
        monkeypatch.setattr("apps.aether_api.main.time.time", lambda: 1000.0)
        client = TestClient(create_app())

        for _ in range(65):
            health_response = client.get("/health")
            version_response = client.get("/version")
            assert health_response.status_code == 200
            assert version_response.status_code == 200
            assert health_response.json() == {"status": "ok"}
            assert version_response.json() == {
                "service": "aether-api",
                "dgce_version": "5.x",
                "api_version": "v1",
            }
            _assert_response_safety_headers(health_response)
            _assert_response_safety_headers(version_response)
