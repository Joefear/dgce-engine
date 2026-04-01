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


def _mark_section_ready(project_root: Path) -> None:
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
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


def _create_stale_gate_drift(project_root: Path) -> str:
    _mark_section_ready(project_root)
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    current_preview_fingerprint = json.loads(preview_path.read_text(encoding="utf-8"))["artifact_fingerprint"]

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["preview_fingerprint"] = "stale-preview-fingerprint"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["preview_fingerprint"] = current_preview_fingerprint
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return current_preview_fingerprint


def _create_review_fingerprint_stale_gate_drift(project_root: Path) -> str:
    _mark_section_ready(project_root)
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
    current_review_fingerprint = dgce_decompose.compute_review_artifact_fingerprint(review_path.read_text(encoding="utf-8"))

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_fingerprint"] = "stale-review-fingerprint"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_fingerprint"] = current_review_fingerprint
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return current_review_fingerprint


def _all_file_bytes(project_root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(project_root)): path.read_bytes()
        for path in project_root.rglob("*")
        if path.is_file()
    }


class TestDGCEPrepareAPI:
    def test_prepare_endpoint_returns_eligible_true_for_valid_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_valid")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
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

    def test_prepare_endpoint_returns_404_for_invalid_section_id(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_missing_section")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/not-a-section/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 404
        assert response.json() == {"detail": "Section not found: not-a-section"}

    def test_prepare_endpoint_returns_ineligible_when_approval_is_missing(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_missing_approval")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "eligible": False,
            "checks": {
                "section_exists": True,
                "artifacts_valid": True,
                "approval_ready": False,
                "preflight_ready": False,
                "gate_ready": False,
            },
        }

    def test_prepare_endpoint_returns_ineligible_when_preflight_is_not_ready(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_preflight_blocked")
        record_section_approval(
            project_root,
            "mission-board",
            SectionApprovalInput(
                approval_status="rejected",
                selected_mode="create_only",
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
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "eligible": False,
            "checks": {
                "section_exists": True,
                "artifacts_valid": True,
                "approval_ready": False,
                "preflight_ready": False,
                "gate_ready": False,
            },
        }

    def test_prepare_endpoint_returns_ineligible_when_gate_is_not_satisfied(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_gate_blocked")
        _mark_section_ready(project_root)
        input_path = project_root / ".dce" / "input" / "mission-board.json"
        input_payload = json.loads(input_path.read_text(encoding="utf-8"))
        input_payload["constraints"].append("operator input changed after approval")
        input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "eligible": False,
            "checks": {
                "section_exists": True,
                "artifacts_valid": True,
                "approval_ready": True,
                "preflight_ready": True,
                "gate_ready": False,
            },
        }

    def test_prepare_endpoint_returns_ineligible_when_artifact_linkage_is_broken(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_broken_linkage")
        _mark_section_ready(project_root)
        workspace_index_path = project_root / ".dce" / "workspace_index.json"
        workspace_index = json.loads(workspace_index_path.read_text(encoding="utf-8"))
        workspace_index["sections"][0]["artifact_links"][0]["path"] = ".dce/preflight/missing.preflight.json"
        workspace_index_path.write_text(json.dumps(workspace_index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "eligible": False,
            "checks": {
                "section_exists": True,
                "artifacts_valid": False,
                "approval_ready": True,
                "preflight_ready": True,
                "gate_ready": True,
            },
        }

    def test_prepare_endpoint_is_deterministic_and_read_only(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_repeatable")
        _mark_section_ready(project_root)
        before_files = _all_file_bytes(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )
        second_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content
        assert _all_file_bytes(project_root) == before_files

    def test_prepare_recomputes_stale_and_gate_from_current_approval_state(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_recompute_current_approval")
        current_preview_fingerprint = _create_stale_gate_drift(project_root)
        stale_path = project_root / ".dce" / "preflight" / "mission-board.stale_check.json"
        gate_path = project_root / ".dce" / "preflight" / "mission-board.execution_gate.json"
        stale_before = json.loads(stale_path.read_text(encoding="utf-8"))
        gate_before = json.loads(gate_path.read_text(encoding="utf-8"))
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert stale_before["stale_status"] == "stale_invalidated"
        assert stale_before["approval_preview_fingerprint"] == "stale-preview-fingerprint"
        assert gate_before["gate_status"] == "gate_blocked_stale"
        assert response.status_code == 200
        assert response.json() == {
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
        approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
        assert approval_payload["preview_fingerprint"] == current_preview_fingerprint

    def test_prepare_returns_eligible_true_after_review_fingerprint_recompute(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_prepare_api_recompute_current_review")
        current_review_fingerprint = _create_review_fingerprint_stale_gate_drift(project_root)
        stale_path = project_root / ".dce" / "preflight" / "mission-board.stale_check.json"
        gate_path = project_root / ".dce" / "preflight" / "mission-board.execution_gate.json"
        stale_before = json.loads(stale_path.read_text(encoding="utf-8"))
        gate_before = json.loads(gate_path.read_text(encoding="utf-8"))
        client = TestClient(create_app())

        response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert stale_before["stale_status"] == "stale_invalidated"
        assert stale_before["stale_reason"] == "approval_review_fingerprint_mismatch"
        assert gate_before["gate_status"] == "gate_blocked_stale"
        assert response.status_code == 200
        assert response.json() == {
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
        approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
        assert approval_payload["review_fingerprint"] == current_review_fingerprint
