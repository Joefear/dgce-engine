import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from apps.aether_api.main import create_app
from aether.dgce import DGCESection, run_section_with_workspace
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


def _workspace_level_dce_bytes(project_root: Path) -> dict[str, bytes]:
    dce_root = project_root / ".dce"
    return {
        str(path.relative_to(dce_root)): path.read_bytes()
        for path in dce_root.rglob("*")
        if path.is_file() and (path.parent == dce_root or path.parent == dce_root / "reviews")
    }


def _non_dce_bytes(project_root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(project_root)): path.read_bytes()
        for path in project_root.rglob("*")
        if path.is_file() and ".dce" not in path.parts
    }


class TestDGCERefreshAPI:
    def test_refresh_endpoint_is_callable_and_returns_expected_payload(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_refresh_api_success")
        client = TestClient(create_app())

        response = client.post("/v1/dgce/refresh", json={"workspace_path": str(project_root)})

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "workspace": str(project_root.resolve()),
            "artifacts_refreshed": True,
        }

    def test_refresh_endpoint_is_deterministic_for_repeated_calls(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_refresh_api_repeat")
        client = TestClient(create_app())

        first_response = client.post("/v1/dgce/refresh", json={"workspace_path": str(project_root)})
        first_dce = _workspace_level_dce_bytes(project_root)
        second_response = client.post("/v1/dgce/refresh", json={"workspace_path": str(project_root)})
        second_dce = _workspace_level_dce_bytes(project_root)

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_dce == second_dce

    def test_refresh_endpoint_does_not_create_or_modify_non_dce_files(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_refresh_api_non_dce")
        project_file = project_root / "notes.txt"
        project_file.write_text("keep me stable", encoding="utf-8")
        before_non_dce = _non_dce_bytes(project_root)
        client = TestClient(create_app())

        response = client.post("/v1/dgce/refresh", json={"workspace_path": str(project_root)})

        assert response.status_code == 200
        assert _non_dce_bytes(project_root) == before_non_dce

    def test_refresh_endpoint_uses_existing_workspace_path_validation(self):
        client = TestClient(create_app())

        response = client.post("/v1/dgce/refresh", json={"workspace_path": ".."})

        assert response.status_code == 400
        assert response.json() == {"detail": response.json()["detail"]}

    def test_refresh_endpoint_no_longer_accepts_query_param_only_input(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_refresh_api_query_only_rejected")
        client = TestClient(create_app())

        response = client.post("/v1/dgce/refresh", params={"workspace_path": str(project_root)})

        assert response.status_code == 422
