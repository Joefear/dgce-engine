import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from apps.aether_api.main import create_app
from aether.dgce import (
    DGCESection,
    SectionApprovalInput,
    SectionExecutionGateInput,
    SectionPreflightInput,
    record_section_approval,
    record_section_execution_gate,
    run_section_with_workspace,
)
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


def _alpha_section() -> DGCESection:
    return DGCESection(
        section_id="alpha-section",
        section_type="ui_system",
        title="Alpha Section",
        description="A second deterministic section used to verify isolation.",
        requirements=["keep alpha artifacts isolated"],
        constraints=["no cross-section leakage"],
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
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    return project_root


def _mark_section_ready(project_root: Path, *, selected_mode: str = "create_only") -> None:
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=selected_mode,
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )


def _all_file_bytes(project_root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(project_root)): path.read_bytes()
        for path in project_root.rglob("*")
        if path.is_file()
    }


class TestDGCEExecuteAPI:
    def test_execute_endpoint_runs_eligible_section_successfully(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_success")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "executed": True,
            "artifacts_updated": True,
        }
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists()
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists()

    def test_execute_endpoint_returns_400_for_non_eligible_section_without_writes(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_non_eligible")
        before_files = _all_file_bytes(project_root)
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Section is not eligible for execution: mission-board"}
        assert _all_file_bytes(project_root) == before_files

    def test_execute_endpoint_returns_404_for_invalid_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_missing_section")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/not-a-section/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 404
        assert response.json() == {"detail": "Section not found: not-a-section"}

    def test_execute_endpoint_updates_only_target_section_artifacts(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_isolation")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        alpha_paths = [
            project_root / ".dce" / "input" / "alpha-section.json",
            project_root / ".dce" / "plans" / "alpha-section.preview.json",
            project_root / ".dce" / "reviews" / "alpha-section.review.md",
        ]
        before_alpha = {path: path.read_bytes() for path in alpha_paths}
        notes_path = project_root / "notes.txt"
        notes_path.write_text("leave me alone", encoding="utf-8")
        notes_before = notes_path.read_bytes()
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert {path: path.read_bytes() for path in alpha_paths} == before_alpha
        assert notes_path.read_bytes() == notes_before
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "alpha-section.json").exists() is False
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists()
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists()

    def test_execute_endpoint_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_repeat_two")
        _mark_section_ready(first_root)
        _mark_section_ready(second_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(first_root)},
        )
        second_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(second_root)},
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content
        assert (first_root / ".dce" / "execution" / "mission-board.execution.json").read_bytes() == (
            second_root / ".dce" / "execution" / "mission-board.execution.json"
        ).read_bytes()
        assert (first_root / ".dce" / "outputs" / "mission-board.json").read_bytes() == (
            second_root / ".dce" / "outputs" / "mission-board.json"
        ).read_bytes()
