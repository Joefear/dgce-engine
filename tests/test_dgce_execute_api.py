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


def _realign_preview_fingerprint_after_stale_gate(project_root: Path) -> str:
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


def _all_file_bytes(project_root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(project_root)): path.read_bytes()
        for path in project_root.rglob("*")
        if path.is_file()
    }


def _file_bytes(project_root: Path, *relative_paths: str) -> dict[str, bytes]:
    return {
        relative_path: (project_root / relative_path).read_bytes()
        for relative_path in relative_paths
        if (project_root / relative_path).exists()
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

    def test_second_execution_without_rerun_returns_400_and_does_not_write(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_missing_rerun")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        _mark_section_ready(project_root)
        before_files = _all_file_bytes(project_root)
        second_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert second_response.status_code == 400
        assert second_response.json() == {
            "detail": "Section has prior execution artifacts; rerun=true required: mission-board"
        }
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

    def test_second_execution_with_rerun_and_valid_ownership_succeeds(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_success")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        _mark_section_ready(project_root)
        rerun_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root), "rerun": True},
        )

        assert rerun_response.status_code == 200
        assert rerun_response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "executed": True,
            "artifacts_updated": True,
        }

    def test_prepare_eligible_immediate_rerun_execute_succeeds_without_source_changes(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_prepare_execute_consistent")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        _mark_section_ready(project_root)
        current_preview_fingerprint = _realign_preview_fingerprint_after_stale_gate(project_root)
        prepare_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )

        assert prepare_response.status_code == 200
        assert prepare_response.json()["eligible"] is True
        approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
        assert approval_payload["preview_fingerprint"] == current_preview_fingerprint

        rerun_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root), "rerun": True},
        )

        assert rerun_response.status_code == 200
        assert rerun_response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "executed": True,
            "artifacts_updated": True,
        }

    def test_execute_blocks_when_source_changes_after_prepare(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_prepare_execute_source_changed")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        _mark_section_ready(project_root)
        prepare_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )
        assert prepare_response.status_code == 200
        assert prepare_response.json()["eligible"] is True

        input_path = project_root / ".dce" / "input" / "mission-board.json"
        input_payload = json.loads(input_path.read_text(encoding="utf-8"))
        input_payload["constraints"].append("operator input changed after prepare")
        input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        before_files = _file_bytes(
            project_root,
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        )

        rerun_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root), "rerun": True},
        )

        assert rerun_response.status_code == 400
        assert rerun_response.json() == {"detail": "Section is not eligible for execution: mission-board"}
        assert _file_bytes(
            project_root,
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        ) == before_files

    def test_rerun_with_failed_safe_modify_returns_400_and_does_not_execute(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_safe_modify_block")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        outputs_path = project_root / ".dce" / "outputs" / "mission-board.json"
        outputs_payload = json.loads(outputs_path.read_text(encoding="utf-8"))
        outputs_payload["file_plan"] = {
            "project_name": "DGCE",
            "files": [
                {
                    "path": "docs/readme.md",
                    "language": "markdown",
                    "purpose": "rerun-safe-modify-check",
                    "content": "updated docs\n",
                }
            ],
        }
        outputs_path.write_text(json.dumps(outputs_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        ownership_index_path = project_root / ".dce" / "ownership_index.json"
        ownership_index_path.write_text(
            json.dumps(
                {
                    "files": [
                        {
                            "path": "docs/readme.md",
                            "section_id": "mission-board",
                        }
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        service_path = project_root / "docs" / "readme.md"
        service_path.parent.mkdir(parents=True, exist_ok=True)
        service_path.write_text("old docs\n", encoding="utf-8")
        _mark_section_ready(project_root, selected_mode="create_only")
        before_files = _file_bytes(
            project_root,
            "docs/readme.md",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        )
        rerun_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root), "rerun": True},
        )

        assert rerun_response.status_code == 400
        assert rerun_response.json() == {
            "detail": "Section rerun requires safe_modify approval: mission-board"
        }
        assert _file_bytes(
            project_root,
            "docs/readme.md",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        ) == before_files

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

    def test_rerun_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_repeat_two")
        client = TestClient(create_app())

        for project_root in (first_root, second_root):
            _mark_section_ready(project_root)
            initial_response = client.post(
                "/v1/dgce/sections/mission-board/execute",
                json={"workspace_path": str(project_root)},
            )
            assert initial_response.status_code == 200
            _mark_section_ready(project_root)

        first_rerun = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(first_root), "rerun": True},
        )
        second_rerun = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(second_root), "rerun": True},
        )

        assert first_rerun.status_code == 200
        assert second_rerun.status_code == 200
        assert first_rerun.json() == second_rerun.json()
        assert first_rerun.content == second_rerun.content
        assert (first_root / ".dce" / "execution" / "mission-board.execution.json").read_bytes() == (
            second_root / ".dce" / "execution" / "mission-board.execution.json"
        ).read_bytes()
        assert (first_root / ".dce" / "outputs" / "mission-board.json").read_bytes() == (
            second_root / ".dce" / "outputs" / "mission-board.json"
        ).read_bytes()
