import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from apps.aether_api.main import create_app
from aether.dgce import (
    DGCESection,
    run_section_with_workspace,
)
import aether.dgce.decompose as dgce_decompose
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
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    return project_root


def _alpha_section() -> DGCESection:
    return DGCESection(
        section_id="alpha-section",
        section_type="ui_system",
        title="Alpha Section",
        description="A second deterministic section used to verify isolation.",
        requirements=["keep alpha artifacts isolated"],
        constraints=["no cross-section leakage"],
    )


class TestDGCEApproveAPI:
    def test_default_approve_uses_preview_recommended_mode(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_writes_current_state")
        client = TestClient(create_app())
        preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
        review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
        input_path = project_root / ".dce" / "input" / "mission-board.json"
        preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))

        response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={
                "workspace_path": str(project_root),
                "approved_by": "alice",
                "notes": "approved from current section state",
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "approved": True,
        }

        approval_payload = json.loads(
            (project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8")
        )
        assert approval_payload["approval_status"] == "approved"
        assert approval_payload["selected_mode"] == preview_payload["recommended_mode"]
        assert approval_payload["recommended_mode"] == preview_payload["recommended_mode"]
        assert approval_payload["preview_outcome_class"] == preview_payload["preview_outcome_class"]
        assert approval_payload["input_fingerprint"] == dgce_decompose.compute_json_file_fingerprint(input_path)
        assert approval_payload["preview_fingerprint"] == preview_payload["artifact_fingerprint"]
        assert approval_payload["review_fingerprint"] == dgce_decompose.compute_review_artifact_fingerprint(
            review_path.read_text(encoding="utf-8")
        )
        assert approval_payload["approved_by"] == "alice"
        assert approval_payload["notes"] == "approved from current section state"
        assert approval_payload["execution_permitted"] is True
        assert json.loads((project_root / ".dce" / "preflight" / "mission-board.preflight.json").read_text(encoding="utf-8"))["preflight_status"] == "preflight_pass"
        assert json.loads((project_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8"))["stale_status"] == "stale_valid"
        assert json.loads((project_root / ".dce" / "preflight" / "mission-board.execution_gate.json").read_text(encoding="utf-8"))["gate_status"] == "gate_pass"

    def test_override_selected_mode_changes_execution_permission_outcome(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_override_review_required")
        client = TestClient(create_app())

        approve_response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root), "selected_mode": "review_required"},
        )
        assert approve_response.status_code == 200

        approval_payload = json.loads(
            (project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8")
        )
        preflight_payload = json.loads(
            (project_root / ".dce" / "preflight" / "mission-board.preflight.json").read_text(encoding="utf-8")
        )
        gate_payload = json.loads(
            (project_root / ".dce" / "preflight" / "mission-board.execution_gate.json").read_text(encoding="utf-8")
        )

        assert approval_payload["selected_mode"] == "review_required"
        assert approval_payload["execution_permitted"] is False
        assert preflight_payload["execution_allowed"] is False
        assert gate_payload["execution_blocked"] is True

    def test_safe_modify_override_allows_prepare_when_other_gates_pass(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_safe_modify_override")
        client = TestClient(create_app())

        approve_response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root), "selected_mode": "safe_modify"},
        )
        assert approve_response.status_code == 200

        prepare_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert prepare_response.status_code == 200
        assert prepare_response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "eligible": True,
            "checks": {
                "section_exists": True,
                "artifacts_valid": True,
                "approval_ready": True,
                "preflight_ready": True,
                "gate_ready": True,
            },
        }
        approval_payload = json.loads(
            (project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8")
        )
        assert approval_payload["selected_mode"] == "safe_modify"
        assert approval_payload["execution_permitted"] is True

    def test_approve_recomputes_only_target_section_and_does_not_execute(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_scoped_recompute")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        alpha_preflight_path = project_root / ".dce" / "preflight" / "alpha-section.preflight.json"
        alpha_stale_path = project_root / ".dce" / "preflight" / "alpha-section.stale_check.json"
        alpha_gate_path = project_root / ".dce" / "preflight" / "alpha-section.execution_gate.json"

        response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert alpha_preflight_path.exists() is False
        assert alpha_stale_path.exists() is False
        assert alpha_gate_path.exists() is False
        assert (project_root / ".dce" / "preflight" / "mission-board.preflight.json").exists()
        assert (project_root / ".dce" / "preflight" / "mission-board.stale_check.json").exists()
        assert (project_root / ".dce" / "preflight" / "mission-board.execution_gate.json").exists()
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_approve_endpoint_blocks_when_required_source_artifact_is_missing(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_missing_review")
        client = TestClient(create_app())
        review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
        review_path.unlink()

        response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {
            "detail": "Section approval requires current artifacts: review"
        }
        assert (project_root / ".dce" / "approvals" / "mission-board.approval.json").exists() is False

    def test_approve_endpoint_rejects_invalid_selected_mode(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_approve_api_invalid_mode")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root), "selected_mode": "launch_the_missiles"},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Invalid selected_mode: launch_the_missiles"}
        assert (project_root / ".dce" / "approvals" / "mission-board.approval.json").exists() is False
