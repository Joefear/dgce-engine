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
from aether.dgce.file_plan import FilePlan
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


def _owned_materialization_section() -> DGCESection:
    section = _section().model_copy()
    section.expected_targets = [
        {
            "path": "api/missionboardservice.py",
            "purpose": "Mission board API",
            "source": "expected_targets",
        },
        {
            "path": "models/mission.py",
            "purpose": "Mission model",
            "source": "expected_targets",
        },
    ]
    return section


def _owned_bundle_section() -> DGCESection:
    return DGCESection(
        section_id="owned-bundle",
        section_type="system_breakdown",
        title="Owned Bundle",
        description="Materialize a small explicit owned bundle through the governed execute path.",
        requirements=[
            "Use explicit ownership sets for output files",
            "Keep bundle generation deterministic",
        ],
        constraints=[
            "Do not infer undeclared files",
            "Do not write across section boundaries",
        ],
        expected_targets=[
            {
                "path": "src/api/ingest.py",
                "purpose": "Observation ingest API",
                "source": "expected_targets",
            }
        ],
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


def _build_owned_workspace(monkeypatch, name: str) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_owned_materialization_section(), project_root, incremental_mode="incremental_v2_2")
    return project_root


def _build_owned_bundle_workspace(monkeypatch, name: str) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        metadata = _stub_executor_result(content).metadata
        if "plan the system breakdown" in lowered:
            return ExecutionResult(
                output=json.dumps(
                    {
                        "modules": [
                            {
                                "name": "IngestModule",
                                "layer": "application",
                                "responsibility": "Handle observation ingest artifacts.",
                                "inputs": [{"name": "request", "type": "ObservationRequest"}],
                                "outputs": [{"name": "ingest_api", "type": "artifact"}],
                                "dependencies": [],
                                "governance_touchpoints": ["governed_execution"],
                                "failure_modes": ["invalid_request"],
                                "owned_paths": [
                                    "src/api/ingest.py",
                                    "src/models/rsobservation.py",
                                ],
                                "implementation_order": 1,
                            },
                            {
                                "name": "ReviewModule",
                                "layer": "application",
                                "responsibility": "Handle anomaly review artifacts.",
                                "inputs": [{"name": "request", "type": "ReviewRequest"}],
                                "outputs": [{"name": "review_api", "type": "artifact"}],
                                "dependencies": [{"name": "IngestModule", "kind": "module", "reference": "src/api/ingest.py"}],
                                "governance_touchpoints": ["governed_execution"],
                                "failure_modes": ["invalid_review"],
                                "owned_paths": [
                                    "src/api/review.py",
                                    "src/models/anomaly_record.py",
                                ],
                                "implementation_order": 2,
                            },
                        ],
                        "file_groups": [
                            {
                                "name": "ingest_bundle",
                                "module": "IngestModule",
                                "placement": "src",
                                "files": [
                                    {"path": "src/api/ingest.py", "purpose": "Observation ingest API", "kind": "api"},
                                    {"path": "src/models/rsobservation.py", "purpose": "RS observation model", "kind": "model"},
                                ],
                            },
                            {
                                "name": "review_bundle",
                                "module": "ReviewModule",
                                "placement": "src",
                                "files": [
                                    {"path": "src/api/review.py", "purpose": "Anomaly review API", "kind": "api"},
                                    {"path": "src/models/anomaly_record.py", "purpose": "Anomaly review model", "kind": "model"},
                                ],
                            },
                            {
                                "name": "rogue_bundle",
                                "module": "RogueModule",
                                "placement": "src",
                                "files": [
                                    {"path": "src/rogue/unowned.py", "purpose": "Unowned rogue file", "kind": "service"},
                                ],
                            },
                        ],
                        "implementation_units": [
                            {"name": "implement_ingest_bundle", "module": "IngestModule", "order": 1},
                            {"name": "implement_review_bundle", "module": "ReviewModule", "order": 2},
                        ],
                        "build_graph": {
                            "edges": [["IngestModule", "ReviewModule"]],
                        },
                        "tests": [
                            {"name": "owned_bundle_paths_are_explicit", "targets": ["IngestModule", "ReviewModule"]},
                        ],
                        "determinism_rules": ["Stable file ordering"],
                        "acceptance_criteria": ["Only explicitly owned files may materialize"],
                    }
                ),
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata=metadata,
            )
        if "implement a data model class" in lowered:
            return ExecutionResult(
                output=json.dumps(
                    {
                        "modules": [{"name": "OwnedBundleModels", "entities": ["RSObservation"], "relationships": [], "required": [], "identity_keys": []}],
                        "entities": [{"name": "RSObservation", "fields": [{"name": "object_id", "type": "string"}]}],
                        "fields": ["object_id"],
                        "relationships": [],
                        "validation_rules": ["object_id required"],
                    }
                ),
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata=metadata,
            )
        if "implement an api surface" in lowered:
            return ExecutionResult(
                output=json.dumps(
                    {
                        "interfaces": ["OwnedBundleApi"],
                        "methods": ["ingest", "review"],
                        "inputs": ["payload"],
                        "outputs": ["result"],
                        "error_cases": ["invalid_payload"],
                    }
                ),
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata=metadata,
            )
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_owned_bundle_section(), project_root, incremental_mode="incremental_v2_2")
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


def _mark_owned_bundle_ready(project_root: Path, *, selected_mode: str = "create_only") -> None:
    record_section_approval(
        project_root,
        "owned-bundle",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=selected_mode,
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "owned-bundle",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )


def _prepare_section(client: TestClient, project_root: Path, section_id: str = "mission-board") -> dict:
    response = client.post(
        f"/v1/dgce/sections/{section_id}/prepare",
        json={"workspace_path": str(project_root)},
    )
    assert response.status_code == 200
    assert response.json()["eligible"] is True
    return response.json()


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
    def test_execute_materializes_only_owned_expected_target_files(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_owned_materialization")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        notes_path = project_root / "notes.txt"
        notes_path.write_text("leave me alone", encoding="utf-8")
        notes_before = notes_path.read_bytes()

        monkeypatch.setattr(
            "aether.dgce.decompose.build_file_plan",
            lambda responses: FilePlan(
                project_name="DGCE",
                files=[
                    {
                        "path": "api/unowned.py",
                        "purpose": "Unowned broad generation",
                        "source": "api_surface",
                    }
                ],
            ),
        )

        approve_response = client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        )
        assert approve_response.status_code == 200

        prepare_response = client.post(
            "/v1/dgce/sections/mission-board/prepare",
            json={"workspace_path": str(project_root)},
        )
        assert prepare_response.status_code == 200
        assert prepare_response.json()["eligible"] is True

        execute_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert execute_response.status_code == 200
        assert execute_response.json() == {
            "status": "ok",
            "section_id": "mission-board",
            "executed": True,
            "artifacts_updated": True,
        }
        assert (project_root / "api" / "missionboardservice.py").exists()
        assert (project_root / "models" / "mission.py").exists()
        assert (project_root / "api" / "unowned.py").exists() is False
        assert notes_path.read_bytes() == notes_before
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False
        outputs_payload = json.loads((project_root / ".dce" / "outputs" / "mission-board.json").read_text(encoding="utf-8"))
        execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        assert sorted(artifact["path"] for artifact in outputs_payload["generated_artifacts"]) == [
            "api/missionboardservice.py",
            "models/mission.py",
        ]
        assert execution_payload["written_files"] == [
            {
                "path": "api/missionboardservice.py",
                "operation": "create",
                "bytes_written": len((project_root / "api" / "missionboardservice.py").read_bytes()),
            },
            {
                "path": "models/mission.py",
                "operation": "create",
                "bytes_written": len((project_root / "models" / "mission.py").read_bytes()),
            },
        ]
        assert all(entry["path"] != "api/unowned.py" for entry in execution_payload["written_files"])

    def test_execute_endpoint_runs_eligible_section_successfully(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_success")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

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
        execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        assert execution_payload["written_files"] == []

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

    def test_execute_requires_valid_prepared_plan_artifact(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_missing_prepared_plan")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_path.unlink()

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Section requires prepared file plan artifact: mission-board"}

    def test_execute_rejects_malformed_prepared_plan_artifact(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_malformed_prepared_plan")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_path.write_text(
            json.dumps(
                {
                    "artifact_type": "prepared_execution_plan",
                    "generated_by": "DGCE",
                    "schema_version": "1.0",
                    "section_id": "other-section",
                    "file_plan": {"project_name": "DGCE", "files": []},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan artifact section mismatch: mission-board"}

    def test_execute_rejects_prepared_plan_when_selected_mode_changes_after_prepare(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_binding_selected_mode")
        _mark_section_ready(project_root, selected_mode="create_only")
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        _mark_section_ready(project_root, selected_mode="safe_modify")
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan binding mismatch: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_approval_basis_changes_after_prepare(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_binding_review_change")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
        review_path.write_text(review_path.read_text(encoding="utf-8") + "\nBinding drift.\n", encoding="utf-8")
        _mark_section_ready(project_root)
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan binding mismatch: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_section_input_changes_after_prepare(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_binding_input_change")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        input_path = project_root / ".dce" / "input" / "mission-board.json"
        input_payload = json.loads(input_path.read_text(encoding="utf-8"))
        input_payload["constraints"].append("input drift after prepare")
        input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _mark_section_ready(project_root)
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan binding mismatch: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_second_execution_without_rerun_returns_400_and_does_not_write(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_missing_rerun")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

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
        _prepare_section(client, project_root)

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
        _prepare_section(client, project_root)

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        _mark_section_ready(project_root)
        _prepare_section(client, project_root)
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

    def test_owned_materialization_second_execution_without_rerun_returns_400(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_owned_missing_rerun")
        client = TestClient(create_app())

        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _prepare_section(client, project_root)
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200

        before_files = _file_bytes(
            project_root,
            "api/missionboardservice.py",
            "models/mission.py",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        )
        second_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert second_response.status_code == 400
        assert second_response.json() == {
            "detail": "Section has prior execution artifacts; rerun=true required: mission-board"
        }
        assert _file_bytes(
            project_root,
            "api/missionboardservice.py",
            "models/mission.py",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        ) == before_files

    def test_owned_materialization_rerun_safe_modify_is_still_enforced(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_owned_safe_modify_block")
        client = TestClient(create_app())

        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root), "selected_mode": "create_only"},
        ).status_code == 200
        _prepare_section(client, project_root)
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200

        target_path = project_root / "api" / "missionboardservice.py"
        target_path.write_text("operator modified file\n", encoding="utf-8")
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root), "selected_mode": "create_only"},
        ).status_code == 200
        _prepare_section(client, project_root)
        before_files = _file_bytes(
            project_root,
            "api/missionboardservice.py",
            "models/mission.py",
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
            "api/missionboardservice.py",
            "models/mission.py",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        ) == before_files

    def test_prepare_eligible_immediate_rerun_execute_succeeds_without_source_changes(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_prepare_execute_consistent")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

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

    def test_execute_uses_prepared_plan_and_does_not_recompute_wider_set(self, monkeypatch):
        project_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_prepare_execute_source_changed")
        _mark_owned_bundle_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "owned-bundle")
        sealed_plan_before = json.loads(
            (project_root / ".dce" / "plans" / "owned-bundle.prepared_plan.json").read_text(encoding="utf-8")
        )

        def wider_run(self, executor_name, content):
            lowered = content.lower()
            metadata = _stub_executor_result(content).metadata
            if "plan the system breakdown" in lowered:
                return ExecutionResult(
                    output=json.dumps(
                        {
                            "modules": [
                                {
                                    "name": "IngestModule",
                                    "layer": "application",
                                    "responsibility": "Handle observation ingest artifacts.",
                                    "inputs": [{"name": "request", "type": "ObservationRequest"}],
                                    "outputs": [{"name": "ingest_api", "type": "artifact"}],
                                    "dependencies": [],
                                    "governance_touchpoints": ["governed_execution"],
                                    "failure_modes": ["invalid_request"],
                                    "owned_paths": [
                                        "src/api/ingest.py",
                                        "src/models/rsobservation.py",
                                        "src/api/expanded.py",
                                    ],
                                    "implementation_order": 1,
                                }
                            ],
                            "file_groups": [
                                {
                                    "name": "expanded_bundle",
                                    "module": "IngestModule",
                                    "placement": "src",
                                    "files": [
                                        {"path": "src/api/ingest.py", "purpose": "Observation ingest API", "kind": "api"},
                                        {"path": "src/models/rsobservation.py", "purpose": "RS observation model", "kind": "model"},
                                        {"path": "src/api/expanded.py", "purpose": "Expanded API", "kind": "api"},
                                    ],
                                }
                            ],
                            "implementation_units": [{"name": "implement_expanded_bundle", "module": "IngestModule", "order": 1}],
                            "build_graph": {"edges": [["IngestModule", "IngestModule"]]},
                            "tests": [{"name": "expanded_bundle_paths_are_explicit", "targets": ["IngestModule"]}],
                            "determinism_rules": ["Stable file ordering"],
                            "acceptance_criteria": ["Prepared plan remains authoritative"],
                        }
                    ),
                    status=ArtifactStatus.EXPERIMENTAL,
                    executor=executor_name,
                    metadata=metadata,
                )
            return _stub_executor_result(content)

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", wider_run)
        response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert json.loads((project_root / ".dce" / "plans" / "owned-bundle.prepared_plan.json").read_text(encoding="utf-8")) == sealed_plan_before
        assert (project_root / "src" / "api" / "expanded.py").exists() is False
        assert sorted(
            json.loads((project_root / ".dce" / "execution" / "owned-bundle.execution.json").read_text(encoding="utf-8"))["written_files"],
            key=lambda entry: entry["path"],
        ) == sorted(
            [
                {
                    "path": "src/api/ingest.py",
                    "operation": "create",
                    "bytes_written": len((project_root / "src" / "api" / "ingest.py").read_bytes()),
                },
                {
                    "path": "src/api/review.py",
                    "operation": "create",
                    "bytes_written": len((project_root / "src" / "api" / "review.py").read_bytes()),
                },
                {
                    "path": "src/models/anomaly_record.py",
                    "operation": "create",
                    "bytes_written": len((project_root / "src" / "models" / "anomaly_record.py").read_bytes()),
                },
                {
                    "path": "src/models/rsobservation.py",
                    "operation": "create",
                    "bytes_written": len((project_root / "src" / "models" / "rsobservation.py").read_bytes()),
                },
            ],
            key=lambda entry: entry["path"],
        )

    def test_rerun_with_failed_safe_modify_returns_400_and_does_not_execute(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_safe_modify_block")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["file_plan"] = {
            "project_name": "DGCE",
            "files": [
                {
                    "path": "docs/readme.md",
                    "purpose": "rerun-safe-modify-check",
                    "source": "expected_targets",
                }
            ],
        }
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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
            ".dce/plans/mission-board.prepared_plan.json",
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
            ".dce/plans/mission-board.prepared_plan.json",
            ".dce/execution/mission-board.execution.json",
            ".dce/outputs/mission-board.json",
        ) == before_files

    def test_execute_endpoint_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_repeat_two")
        _mark_section_ready(first_root)
        _mark_section_ready(second_root)
        client = TestClient(create_app())
        _prepare_section(client, first_root)
        _prepare_section(client, second_root)

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

    def test_owned_materialization_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_owned_repeat_one")
        second_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_owned_repeat_two")
        client = TestClient(create_app())

        for project_root in (first_root, second_root):
            response = client.post(
                "/v1/dgce/sections/mission-board/approve",
                json={"workspace_path": str(project_root)},
            )
            assert response.status_code == 200
            _prepare_section(client, project_root)

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
        assert (first_root / "api" / "missionboardservice.py").read_bytes() == (
            second_root / "api" / "missionboardservice.py"
        ).read_bytes()
        assert (first_root / "models" / "mission.py").read_bytes() == (
            second_root / "models" / "mission.py"
        ).read_bytes()
        assert json.loads((first_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))["written_files"] == (
            json.loads((second_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))["written_files"]
        )
        assert (first_root / ".dce" / "outputs" / "mission-board.json").read_bytes() == (
            second_root / ".dce" / "outputs" / "mission-board.json"
        ).read_bytes()

    def test_execute_materializes_explicit_owned_bundle_only(self, monkeypatch):
        project_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_owned_bundle_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "owned-bundle")
        notes_path = project_root / "notes.txt"
        notes_path.write_text("leave me alone", encoding="utf-8")
        notes_before = notes_path.read_bytes()

        response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_id": "owned-bundle",
            "executed": True,
            "artifacts_updated": True,
        }
        expected_paths = [
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
        ]
        assert all((project_root / relative_path).exists() for relative_path in expected_paths)
        assert (project_root / "src" / "rogue" / "unowned.py").exists() is False
        assert notes_path.read_bytes() == notes_before
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False
        outputs_payload = json.loads((project_root / ".dce" / "outputs" / "owned-bundle.json").read_text(encoding="utf-8"))
        execution_payload = json.loads((project_root / ".dce" / "execution" / "owned-bundle.execution.json").read_text(encoding="utf-8"))
        assert sorted(artifact["path"] for artifact in outputs_payload["generated_artifacts"]) == expected_paths
        assert execution_payload["written_files"] == [
            {
                "path": relative_path,
                "operation": "create",
                "bytes_written": len((project_root / relative_path).read_bytes()),
            }
            for relative_path in expected_paths
        ]
        assert all(entry["path"] != "src/rogue/unowned.py" for entry in execution_payload["written_files"])

    def test_owned_bundle_second_execution_without_rerun_returns_400(self, monkeypatch):
        project_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_missing_rerun")
        _mark_owned_bundle_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "owned-bundle")

        first_response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        before_files = _file_bytes(
            project_root,
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
            ".dce/execution/owned-bundle.execution.json",
            ".dce/outputs/owned-bundle.json",
        )
        second_response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root)},
        )

        assert second_response.status_code == 400
        assert second_response.json() == {
            "detail": "Section has prior execution artifacts; rerun=true required: owned-bundle"
        }
        assert _file_bytes(
            project_root,
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
            ".dce/execution/owned-bundle.execution.json",
            ".dce/outputs/owned-bundle.json",
        ) == before_files

    def test_owned_bundle_rerun_safe_modify_is_still_enforced(self, monkeypatch):
        project_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_safe_modify_block")
        _mark_owned_bundle_ready(project_root, selected_mode="create_only")
        client = TestClient(create_app())
        _prepare_section(client, project_root, "owned-bundle")

        assert client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200

        target_path = project_root / "src" / "api" / "ingest.py"
        target_path.write_text("operator modified file\n", encoding="utf-8")
        _mark_owned_bundle_ready(project_root, selected_mode="create_only")
        _prepare_section(client, project_root, "owned-bundle")
        before_files = _file_bytes(
            project_root,
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
            ".dce/execution/owned-bundle.execution.json",
            ".dce/outputs/owned-bundle.json",
        )

        rerun_response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(project_root), "rerun": True},
        )

        assert rerun_response.status_code == 400
        assert rerun_response.json() == {
            "detail": "Section rerun requires safe_modify approval: owned-bundle"
        }
        assert _file_bytes(
            project_root,
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
            ".dce/execution/owned-bundle.execution.json",
            ".dce/outputs/owned-bundle.json",
        ) == before_files

    def test_owned_bundle_execute_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_repeat_one")
        second_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_repeat_two")
        client = TestClient(create_app())

        for project_root in (first_root, second_root):
            _mark_owned_bundle_ready(project_root)
            _prepare_section(client, project_root, "owned-bundle")

        first_response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(first_root)},
        )
        second_response = client.post(
            "/v1/dgce/sections/owned-bundle/execute",
            json={"workspace_path": str(second_root)},
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content
        for relative_path in (
            "src/api/ingest.py",
            "src/api/review.py",
            "src/models/anomaly_record.py",
            "src/models/rsobservation.py",
            ".dce/execution/owned-bundle.execution.json",
            ".dce/outputs/owned-bundle.json",
        ):
            assert (first_root / relative_path).read_bytes() == (second_root / relative_path).read_bytes()

    def test_rerun_is_deterministic_across_identical_prepared_workspaces(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_rerun_repeat_two")
        client = TestClient(create_app())

        for project_root in (first_root, second_root):
            _mark_section_ready(project_root)
            _prepare_section(client, project_root)
            initial_response = client.post(
                "/v1/dgce/sections/mission-board/execute",
                json={"workspace_path": str(project_root)},
            )
            assert initial_response.status_code == 200
            _mark_section_ready(project_root)
            _prepare_section(client, project_root)

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
