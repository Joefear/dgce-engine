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
import aether.dgce.execute_api as dgce_execute_api
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


def _dependency_section(section_id: str, *, dependencies: list[str] | None = None) -> DGCESection:
    return DGCESection(
        section_id=section_id,
        section_type="system_component",
        title=section_id.replace("-", " ").title(),
        description=f"Deterministic planning fixture for {section_id}.",
        requirements=["preserve deterministic planning"],
        constraints=["read-only planning only"],
        dependencies=list(dependencies or []),
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


def _build_dependency_workspace(monkeypatch, name: str, sections: list[DGCESection]) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for section in sections:
        run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
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


def _mark_alpha_ready(project_root: Path, *, selected_mode: str = "create_only") -> None:
    record_section_approval(
        project_root,
        "alpha-section",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=selected_mode,
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "alpha-section",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )


def _mark_section_id_ready(project_root: Path, section_id: str, *, selected_mode: str = "create_only") -> None:
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=selected_mode,
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        section_id,
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


def _execute_bundle(
    client: TestClient,
    project_root: Path,
    section_ids: list[str],
    *,
    planned_order: list[str] | None = None,
    verify_dependencies: bool = False,
    rerun: bool = False,
):
    payload: dict[str, object] = {
        "workspace_path": str(project_root),
        "section_ids": section_ids,
        "verify_dependencies": verify_dependencies,
        "rerun": rerun,
    }
    if planned_order is not None:
        payload["planned_order"] = planned_order
    return client.post("/v1/dgce/sections/execute-bundle", json=payload)


def _plan_bundle(client: TestClient, project_root: Path, section_ids: list[str]):
    return client.post(
        "/v1/dgce/sections/plan-bundle",
        json={
            "workspace_path": str(project_root),
            "section_ids": section_ids,
        },
    )


def _get_bundle_manifest(client: TestClient, project_root: Path, bundle_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/{bundle_fingerprint}",
        params={"workspace_path": str(project_root)},
    )


def _get_bundles_by_input(client: TestClient, project_root: Path, bundle_input_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/by-input/{bundle_input_fingerprint}",
        params={"workspace_path": str(project_root)},
    )


def _get_section_bundles(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/bundles",
        params={"workspace_path": str(project_root)},
    )


def _get_section_provenance(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/provenance",
        params={"workspace_path": str(project_root)},
    )


def _verify_section(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/verify",
        params={"workspace_path": str(project_root)},
    )


def _verify_bundle(client: TestClient, project_root: Path, bundle_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/{bundle_fingerprint}/verify",
        params={"workspace_path": str(project_root)},
    )


def _get_section_summary(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/summary",
        params={"workspace_path": str(project_root)},
    )


def _get_bundle_summary(client: TestClient, project_root: Path, bundle_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/{bundle_fingerprint}/summary",
        params={"workspace_path": str(project_root)},
    )


def _get_section_overview(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/overview",
        params={"workspace_path": str(project_root)},
    )


def _get_bundle_overview(client: TestClient, project_root: Path, bundle_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/{bundle_fingerprint}/overview",
        params={"workspace_path": str(project_root)},
    )


def _get_section_dashboard(client: TestClient, project_root: Path, section_id: str):
    return client.get(
        f"/v1/dgce/sections/{section_id}/dashboard",
        params={"workspace_path": str(project_root)},
    )


def _get_bundle_dashboard(client: TestClient, project_root: Path, bundle_fingerprint: str):
    return client.get(
        f"/v1/dgce/bundles/{bundle_fingerprint}/dashboard",
        params={"workspace_path": str(project_root)},
    )


def _bundle_manifest_payload(project_root: Path) -> dict:
    bundle_paths = sorted(
        path
        for path in (project_root / ".dce" / "execution" / "bundles").glob("*.json")
        if path.name != "index.json"
    )
    assert len(bundle_paths) == 1
    return json.loads(bundle_paths[0].read_text(encoding="utf-8"))


def _bundle_manifest_bytes(project_root: Path) -> bytes:
    bundle_paths = sorted(
        path
        for path in (project_root / ".dce" / "execution" / "bundles").glob("*.json")
        if path.name != "index.json"
    )
    assert len(bundle_paths) == 1
    return bundle_paths[0].read_bytes()


def _bundle_index_payload(project_root: Path) -> dict:
    return json.loads((project_root / ".dce" / "execution" / "bundles" / "index.json").read_text(encoding="utf-8"))


def _bundle_index_bytes(project_root: Path) -> bytes:
    return (project_root / ".dce" / "execution" / "bundles" / "index.json").read_bytes()


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
    def test_execute_bundle_runs_sections_in_exact_order_via_single_section_execute_path(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_order")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        original_execute_prepared_section = dgce_execute_api.execute_prepared_section
        executed_sections: list[str] = []

        def record_bundle_order(workspace_path, section_id, *, rerun=False):
            executed_sections.append(section_id)
            return original_execute_prepared_section(workspace_path, section_id, rerun=rerun)

        monkeypatch.setattr("aether.dgce.execute_api.execute_prepared_section", record_bundle_order)
        response = _execute_bundle(client, project_root, ["alpha-section", "mission-board"])

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_results": [
                {
                    "status": "ok",
                    "section_id": "alpha-section",
                    "executed": True,
                    "artifacts_updated": True,
                },
                {
                    "status": "ok",
                    "section_id": "mission-board",
                    "executed": True,
                    "artifacts_updated": True,
                },
            ],
            "first_failing_section": None,
            "stopped_early": False,
        }
        assert executed_sections == ["alpha-section", "mission-board"]
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_index = _bundle_index_payload(project_root)
        assert bundle_manifest["section_ids"] == ["alpha-section", "mission-board"]
        assert bundle_manifest["input_section_ids"] == ["alpha-section", "mission-board"]
        assert bundle_manifest["effective_execution_order"] == ["alpha-section", "mission-board"]
        assert bundle_manifest["order_source"] == "input_order"
        assert [entry["section_id"] for entry in bundle_manifest["sections"]] == ["alpha-section", "mission-board"]
        assert bundle_manifest["execution_status"] == "success"
        assert bundle_manifest["stopped_early"] is False
        assert bundle_manifest["first_failing_section"] is None
        assert bundle_manifest["bundle_input_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
            {"section_ids": ["alpha-section", "mission-board"]}
        )
        bundle_core = {
            key: value
            for key, value in bundle_manifest.items()
            if key != "bundle_fingerprint"
        }
        assert bundle_manifest["bundle_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(bundle_core)
        alpha_execution = json.loads((project_root / ".dce" / "execution" / "alpha-section.execution.json").read_text(encoding="utf-8"))
        mission_execution = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        assert bundle_manifest["sections"] == [
            {
                "approval_lineage_fingerprint": alpha_execution["prepared_plan_audit_manifest"]["approval_lineage_fingerprint"],
                "binding_fingerprint": alpha_execution["prepared_plan_audit_manifest"]["binding_fingerprint"],
                "execution_artifact_path": ".dce/execution/alpha-section.execution.json",
                "prepared_plan_audit_fingerprint": alpha_execution["prepared_plan_audit_fingerprint"],
                "prepared_plan_fingerprint": alpha_execution["prepared_plan_audit_manifest"]["prepared_plan_fingerprint"],
                "section_id": "alpha-section",
                "status": "success",
            },
            {
                "approval_lineage_fingerprint": mission_execution["prepared_plan_audit_manifest"]["approval_lineage_fingerprint"],
                "binding_fingerprint": mission_execution["prepared_plan_audit_manifest"]["binding_fingerprint"],
                "execution_artifact_path": ".dce/execution/mission-board.execution.json",
                "prepared_plan_audit_fingerprint": mission_execution["prepared_plan_audit_fingerprint"],
                "prepared_plan_fingerprint": mission_execution["prepared_plan_audit_manifest"]["prepared_plan_fingerprint"],
                "section_id": "mission-board",
                "status": "success",
            },
        ]
        assert bundle_index == {
            "artifact_type": "bundle_execution_audit_index",
            "bundles": [
                {
                    "bundle_fingerprint": bundle_manifest["bundle_fingerprint"],
                    "bundle_input_fingerprint": bundle_manifest["bundle_input_fingerprint"],
                    "execution_status": "success",
                    "first_failing_section": None,
                    "manifest_path": f".dce/execution/bundles/{bundle_manifest['bundle_fingerprint']}.json",
                    "section_ids": ["alpha-section", "mission-board"],
                    "stopped_early": False,
                }
            ],
            "by_section": {
                "alpha-section": [bundle_manifest["bundle_fingerprint"]],
                "mission-board": [bundle_manifest["bundle_fingerprint"]],
            },
            "generated_by": "DGCE",
            "schema_version": "1.0",
        }
        assert dgce_execute_api.get_bundle_index_record_by_fingerprint(project_root, bundle_manifest["bundle_fingerprint"]) == (
            bundle_index["bundles"][0]
        )
        assert dgce_execute_api.get_bundle_index_records_by_input_fingerprint(
            project_root,
            bundle_manifest["bundle_input_fingerprint"],
        ) == bundle_index["bundles"]
        assert dgce_execute_api.get_bundle_fingerprints_for_section(project_root, "alpha-section") == [
            bundle_manifest["bundle_fingerprint"]
        ]

    def test_execute_bundle_stops_immediately_on_first_failure(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_fail_fast")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        response = _execute_bundle(client, project_root, ["mission-board", "alpha-section"])

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "mission-board",
                    "status": "failed",
                    "detail": "Section has prior execution artifacts; rerun=true required: mission-board",
                }
            ],
            "first_failing_section": "mission-board",
            "stopped_early": True,
        }
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_index = _bundle_index_payload(project_root)
        assert bundle_manifest["execution_status"] == "failed"
        assert bundle_manifest["stopped_early"] is True
        assert bundle_manifest["first_failing_section"] == "mission-board"
        assert bundle_manifest["section_ids"] == ["mission-board", "alpha-section"]
        assert bundle_manifest["input_section_ids"] == ["mission-board", "alpha-section"]
        assert bundle_manifest["effective_execution_order"] == ["mission-board", "alpha-section"]
        assert bundle_manifest["order_source"] == "input_order"
        assert bundle_manifest["sections"] == [
            {
                "approval_lineage_fingerprint": None,
                "binding_fingerprint": None,
                "execution_artifact_path": ".dce/execution/mission-board.execution.json",
                "prepared_plan_audit_fingerprint": None,
                "prepared_plan_fingerprint": None,
                "section_id": "mission-board",
                "status": "failed",
            }
        ]
        assert bundle_index["bundles"] == [
            {
                "bundle_fingerprint": bundle_manifest["bundle_fingerprint"],
                "bundle_input_fingerprint": bundle_manifest["bundle_input_fingerprint"],
                "execution_status": "failed",
                "first_failing_section": "mission-board",
                "manifest_path": f".dce/execution/bundles/{bundle_manifest['bundle_fingerprint']}.json",
                "section_ids": ["mission-board", "alpha-section"],
                "stopped_early": True,
            }
        ]
        assert bundle_index["by_section"] == {
            "alpha-section": [bundle_manifest["bundle_fingerprint"]],
            "mission-board": [bundle_manifest["bundle_fingerprint"]],
        }

    def test_execute_bundle_rejects_duplicate_section_ids(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_duplicates")
        client = TestClient(create_app())

        response = _execute_bundle(client, project_root, ["mission-board", "mission-board"])

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle section_ids must be unique",
        }

    def test_execute_bundle_rejects_empty_section_list(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_empty")
        client = TestClient(create_app())

        response = _execute_bundle(client, project_root, [])

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle requires at least one section_id",
        }

    def test_execute_bundle_accepts_valid_planned_order(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "section_results": [
                {
                    "status": "ok",
                    "section_id": "alpha-section",
                    "executed": True,
                    "artifacts_updated": True,
                },
                {
                    "status": "ok",
                    "section_id": "mission-board",
                    "executed": True,
                    "artifacts_updated": True,
                },
            ],
            "first_failing_section": None,
            "stopped_early": False,
        }
        bundle_manifest = _bundle_manifest_payload(project_root)
        assert bundle_manifest["section_ids"] == ["alpha-section", "mission-board"]
        assert bundle_manifest["input_section_ids"] == ["mission-board", "alpha-section"]
        assert bundle_manifest["effective_execution_order"] == ["alpha-section", "mission-board"]
        assert bundle_manifest["order_source"] == "planned_order"

    def test_execute_bundle_rejects_planned_order_mismatch(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_mismatch")
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board", "not-a-section"],
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order must contain exactly the same section_ids as section_ids",
        }

    def test_execute_bundle_rejects_duplicate_planned_order(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_duplicates")
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board", "mission-board"],
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order must be unique",
        }

    def test_execute_bundle_rejects_planned_order_with_missing_entries(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_missing")
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board"],
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order must contain exactly the same section_ids as section_ids",
        }

    def test_execute_bundle_uses_planned_order_exactly(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_exact")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        original_execute_prepared_section = dgce_execute_api.execute_prepared_section
        executed_sections: list[str] = []

        def record_bundle_order(workspace_path, section_id, *, rerun=False):
            executed_sections.append(section_id)
            return original_execute_prepared_section(workspace_path, section_id, rerun=rerun)

        monkeypatch.setattr("aether.dgce.execute_api.execute_prepared_section", record_bundle_order)
        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        )

        assert response.status_code == 200
        assert executed_sections == ["alpha-section", "mission-board"]

    def test_execute_bundle_fail_fast_still_works_with_planned_order(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_fail_fast")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board", "alpha-section"],
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "mission-board",
                    "status": "failed",
                    "detail": "Section has prior execution artifacts; rerun=true required: mission-board",
                }
            ],
            "first_failing_section": "mission-board",
            "stopped_early": True,
        }
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False

    def test_execute_bundle_planned_order_does_not_widen_section_set(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_no_widen")
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board", "alpha-section", "not-a-section"],
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order must contain exactly the same section_ids as section_ids",
        }

    def test_execute_bundle_with_planned_order_is_deterministic_for_identical_prepared_state(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_planned_order_repeat_two")
        client = TestClient(create_app())

        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")

        first_response = _execute_bundle(
            client,
            first_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        )
        second_response = _execute_bundle(
            client,
            second_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content
        assert _bundle_manifest_bytes(first_root) == _bundle_manifest_bytes(second_root)
        assert _bundle_index_bytes(first_root) == _bundle_index_bytes(second_root)

    def test_execute_bundle_manifest_records_raw_input_order_audit_linkage(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_order_audit_input")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        response = _execute_bundle(client, project_root, ["mission-board", "alpha-section"])

        assert response.status_code == 200
        manifest = _bundle_manifest_payload(project_root)
        assert manifest["input_section_ids"] == ["mission-board", "alpha-section"]
        assert manifest["effective_execution_order"] == ["mission-board", "alpha-section"]
        assert manifest["order_source"] == "input_order"

    def test_execute_bundle_manifest_records_planned_order_audit_linkage(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_order_audit_planned")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        )

        assert response.status_code == 200
        manifest = _bundle_manifest_payload(project_root)
        assert manifest["input_section_ids"] == ["mission-board", "alpha-section"]
        assert manifest["effective_execution_order"] == ["alpha-section", "mission-board"]
        assert manifest["order_source"] == "planned_order"

    def test_execute_bundle_with_verify_dependencies_accepts_valid_planned_order(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_valid",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())
        _mark_section_id_ready(project_root, "section-a")
        _mark_section_id_ready(project_root, "section-b")
        _prepare_section(client, project_root, "section-a")
        _prepare_section(client, project_root, "section-b")
        response = _execute_bundle(
            client,
            project_root,
            ["section-b", "section-a"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )

        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_execute_bundle_with_verify_dependencies_rejects_invalid_dependency_order(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_invalid_order",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["section-b", "section-a"],
            planned_order=["section-b", "section-a"],
            verify_dependencies=True,
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order violates dependency order: section-a -> section-b",
        }

    def test_execute_bundle_with_verify_dependencies_rejects_missing_dependency_in_section_ids(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_missing_dependency",
            [
                _dependency_section("section-a", dependencies=["section-missing"]),
                _dependency_section("section-b"),
            ],
        )
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["section-a", "section-b"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle planned_order dependency missing from section_ids: section-missing -> section-a",
        }

    def test_execute_bundle_with_verify_dependencies_requires_planned_order(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_requires_planned",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["section-b", "section-a"],
            verify_dependencies=True,
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [],
            "first_failing_section": None,
            "stopped_early": True,
            "detail": "Bundle verify_dependencies requires planned_order",
        }

    def test_execute_bundle_with_verify_dependencies_false_preserves_existing_behavior(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_false",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["section-b", "section-a"],
            planned_order=["section-b", "section-a"],
            verify_dependencies=False,
        )

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "section-b",
                    "status": "failed",
                    "detail": "Section is not eligible for execution: section-b",
                }
            ],
            "first_failing_section": "section-b",
            "stopped_early": True,
        }

    def test_execute_bundle_with_verify_dependencies_still_follows_planned_order_exactly(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_exact_order",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())
        _mark_section_id_ready(project_root, "section-a")
        _mark_section_id_ready(project_root, "section-b")
        _prepare_section(client, project_root, "section-a")
        _prepare_section(client, project_root, "section-b")

        original_execute_prepared_section = dgce_execute_api.execute_prepared_section
        executed_sections: list[str] = []

        def record_bundle_order(workspace_path, section_id, *, rerun=False):
            executed_sections.append(section_id)
            return original_execute_prepared_section(workspace_path, section_id, rerun=rerun)

        monkeypatch.setattr("aether.dgce.execute_api.execute_prepared_section", record_bundle_order)
        response = _execute_bundle(
            client,
            project_root,
            ["section-b", "section-a"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )

        assert response.status_code == 200
        assert executed_sections == ["section-a", "section-b"]

    def test_execute_bundle_with_verify_dependencies_preserves_fail_fast_behavior(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_verify_dependencies_fail_fast")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")

        first_response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert first_response.status_code == 200

        response = _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["mission-board", "alpha-section"],
            verify_dependencies=True,
        )

        assert response.status_code == 400
        assert response.json()["first_failing_section"] == "mission-board"
        assert response.json()["stopped_early"] is True

    def test_execute_bundle_with_verify_dependencies_is_deterministic(self, monkeypatch):
        first_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_repeat_one",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        second_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_repeat_two",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            _mark_section_id_ready(project_root, "section-a")
            _mark_section_id_ready(project_root, "section-b")
            _prepare_section(client, project_root, "section-a")
            _prepare_section(client, project_root, "section-b")

        first_response = _execute_bundle(
            client,
            first_root,
            ["section-b", "section-a"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )
        second_response = _execute_bundle(
            client,
            second_root,
            ["section-b", "section-a"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content

    def test_execute_bundle_with_verify_dependencies_does_not_widen_section_set(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_execute_api_bundle_verify_dependencies_no_widen",
            [
                _dependency_section("section-a", dependencies=["section-missing"]),
                _dependency_section("section-b"),
                _dependency_section("section-missing"),
            ],
        )
        client = TestClient(create_app())

        response = _execute_bundle(
            client,
            project_root,
            ["section-a", "section-b"],
            planned_order=["section-a", "section-b"],
            verify_dependencies=True,
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Bundle planned_order dependency missing from section_ids: section-missing -> section-a"

    def test_execute_bundle_rejects_unknown_section_id(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_unknown")
        client = TestClient(create_app())

        response = _execute_bundle(client, project_root, ["not-a-section"])

        assert response.status_code == 404
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "not-a-section",
                    "status": "failed",
                    "detail": "Section not found: not-a-section",
                }
            ],
            "first_failing_section": "not-a-section",
            "stopped_early": True,
        }

    def test_execute_bundle_preserves_per_section_written_files_and_isolation(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_bundle_written_files")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())

        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _mark_alpha_ready(project_root)
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")

        response = _execute_bundle(client, project_root, ["mission-board", "alpha-section"])

        assert response.status_code == 200
        mission_execution = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        alpha_execution = json.loads((project_root / ".dce" / "execution" / "alpha-section.execution.json").read_text(encoding="utf-8"))
        assert mission_execution["written_files"] == [
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
        assert alpha_execution["written_files"] == []
        assert (project_root / ".dce" / "outputs" / "alpha-section.json").exists()
        assert all(entry["path"] not in {"api/missionboardservice.py", "models/mission.py"} for entry in alpha_execution["written_files"])

    def test_execute_bundle_preserves_safe_modify_enforcement_per_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_safe_modify")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")

        first_response = _execute_bundle(client, project_root, ["mission-board"])
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
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        ownership_index_path = project_root / ".dce" / "ownership_index.json"
        ownership_index_path.write_text(
            json.dumps({"files": [{"path": "docs/readme.md", "section_id": "mission-board"}]}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        readme_path = project_root / "docs" / "readme.md"
        readme_path.parent.mkdir(parents=True, exist_ok=True)
        readme_path.write_text("old docs\n", encoding="utf-8")
        _mark_section_ready(project_root, selected_mode="create_only")
        _mark_alpha_ready(project_root)

        response = _execute_bundle(client, project_root, ["mission-board", "alpha-section"], rerun=True)

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "mission-board",
                    "status": "failed",
                    "detail": "Prepared file plan artifact exceeds approved preview scope: mission-board",
                }
            ],
            "first_failing_section": "mission-board",
            "stopped_early": True,
        }
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False

    def test_execute_bundle_preserves_rerun_enforcement_per_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_rerun_enforcement")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")

        assert _execute_bundle(client, project_root, ["mission-board"]).status_code == 200

        response = _execute_bundle(client, project_root, ["mission-board", "alpha-section"])

        assert response.status_code == 400
        assert response.json() == {
            "status": "failed",
            "section_results": [
                {
                    "section_id": "mission-board",
                    "status": "failed",
                    "detail": "Section has prior execution artifacts; rerun=true required: mission-board",
                }
            ],
            "first_failing_section": "mission-board",
            "stopped_early": True,
        }
        assert (project_root / ".dce" / "execution" / "alpha-section.execution.json").exists() is False

    def test_execute_bundle_does_not_recompute_or_widen_prepared_file_sets(self, monkeypatch):
        project_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_bundle_no_widen")
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
        response = _execute_bundle(client, project_root, ["owned-bundle"])

        assert response.status_code == 200
        assert json.loads((project_root / ".dce" / "plans" / "owned-bundle.prepared_plan.json").read_text(encoding="utf-8")) == sealed_plan_before
        assert (project_root / "src" / "api" / "expanded.py").exists() is False

    def test_execute_bundle_is_deterministic_for_identical_prepared_state(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_repeat_two")
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")

        first_response = _execute_bundle(client, first_root, ["alpha-section", "mission-board"])
        second_response = _execute_bundle(client, second_root, ["alpha-section", "mission-board"])

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content
        assert (first_root / ".dce" / "execution" / "alpha-section.execution.json").read_bytes() == (
            second_root / ".dce" / "execution" / "alpha-section.execution.json"
        ).read_bytes()
        assert (first_root / ".dce" / "execution" / "mission-board.execution.json").read_bytes() == (
            second_root / ".dce" / "execution" / "mission-board.execution.json"
        ).read_bytes()
        assert _bundle_manifest_bytes(first_root) == _bundle_manifest_bytes(second_root)
        assert _bundle_index_bytes(first_root) == _bundle_index_bytes(second_root)

    def test_execute_bundle_order_changes_bundle_input_fingerprint(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_fingerprint_order_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_fingerprint_order_two")
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")

        assert _execute_bundle(client, first_root, ["alpha-section", "mission-board"]).status_code == 200
        assert _execute_bundle(client, second_root, ["mission-board", "alpha-section"]).status_code == 200

        first_manifest = _bundle_manifest_payload(first_root)
        second_manifest = _bundle_manifest_payload(second_root)
        assert first_manifest["bundle_input_fingerprint"] != second_manifest["bundle_input_fingerprint"]
        assert first_manifest["section_ids"] == ["alpha-section", "mission-board"]
        assert second_manifest["section_ids"] == ["mission-board", "alpha-section"]
        assert _bundle_index_payload(first_root)["bundles"][0]["bundle_input_fingerprint"] != (
            _bundle_index_payload(second_root)["bundles"][0]["bundle_input_fingerprint"]
        )

    def test_bundle_manifest_contains_only_references_and_fingerprints(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_manifest_shape")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        response = _execute_bundle(client, project_root, ["alpha-section", "mission-board"])

        assert response.status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        serialized_manifest = json.dumps(bundle_manifest, indent=2, sort_keys=True)
        assert "written_files" not in serialized_manifest
        assert "execution_timestamp" not in serialized_manifest
        assert "\"approval_lineage\":" not in serialized_manifest
        assert "\"binding\":" not in serialized_manifest
        assert "prepared_plan_audit_manifest" not in serialized_manifest

    def test_bundle_index_is_idempotent_for_repeated_identical_bundle_runs(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_index_idempotent")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root, selected_mode="safe_modify")
        _mark_alpha_ready(project_root, selected_mode="safe_modify")
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        first_response = _execute_bundle(client, project_root, ["alpha-section", "mission-board"])
        assert first_response.status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        index_bytes_after_first = _bundle_index_bytes(project_root)

        dgce_execute_api._update_bundle_execution_index(project_root, bundle_manifest)

        assert _bundle_index_bytes(project_root) == index_bytes_after_first
        assert len(_bundle_index_payload(project_root)["bundles"]) == 1

    def test_bundle_index_tracks_section_participation_across_distinct_bundle_runs(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_index_participation")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root, selected_mode="safe_modify")
        _mark_alpha_ready(project_root, selected_mode="safe_modify")
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        first_response = _execute_bundle(client, project_root, ["alpha-section", "mission-board"])
        assert first_response.status_code == 200
        second_response = _execute_bundle(client, project_root, ["alpha-section"])
        assert second_response.status_code == 400

        bundle_index = _bundle_index_payload(project_root)
        expected_alpha_fingerprints = sorted(entry["bundle_fingerprint"] for entry in bundle_index["bundles"])
        expected_mission_fingerprints = sorted(
            entry["bundle_fingerprint"]
            for entry in bundle_index["bundles"]
            if "mission-board" in entry["section_ids"]
        )

        assert len(bundle_index["bundles"]) == 2
        assert bundle_index["by_section"] == {
            "alpha-section": expected_alpha_fingerprints,
            "mission-board": expected_mission_fingerprints,
        }
        assert dgce_execute_api.get_bundle_fingerprints_for_section(project_root, "alpha-section") == expected_alpha_fingerprints
        assert dgce_execute_api.get_bundle_fingerprints_for_section(project_root, "mission-board") == expected_mission_fingerprints
        assert dgce_execute_api.get_bundle_index_records_by_input_fingerprint(
            project_root,
            bundle_index["bundles"][0]["bundle_input_fingerprint"],
        ) == [bundle_index["bundles"][0]]

    def test_load_bundle_execution_index_rejects_malformed_index(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_index_malformed")
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(
            json.dumps(
                {
                    "artifact_type": "bundle_execution_audit_index",
                    "bundles": "not-a-list",
                    "by_section": {},
                    "generated_by": "DGCE",
                    "schema_version": "1.0",
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="Bundle index is malformed"):
            dgce_execute_api.load_bundle_execution_index(project_root)

    def test_bundle_index_contains_only_compact_bundle_references(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_index_shape")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")

        response = _execute_bundle(client, project_root, ["alpha-section", "mission-board"])

        assert response.status_code == 200
        serialized_index = json.dumps(_bundle_index_payload(project_root), indent=2, sort_keys=True)
        assert "\"sections\":" not in serialized_index
        assert "prepared_plan_fingerprint" not in serialized_index
        assert "prepared_plan_audit_fingerprint" not in serialized_index
        assert "approval_lineage_fingerprint" not in serialized_index
        assert "binding_fingerprint" not in serialized_index
        assert "\"written_files\":" not in serialized_index

    def test_get_bundle_by_fingerprint_returns_exact_stored_manifest(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_manifest")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200

        bundle_manifest = _bundle_manifest_payload(project_root)
        response = _get_bundle_manifest(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json() == bundle_manifest

    def test_get_bundles_by_input_returns_compact_records_in_deterministic_order(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundles_by_input")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root, selected_mode="safe_modify")
        _mark_alpha_ready(project_root, selected_mode="safe_modify")
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"], rerun=True).status_code == 400

        index_payload = _bundle_index_payload(project_root)
        target_record = index_payload["bundles"][0]
        response = _get_bundles_by_input(client, project_root, target_record["bundle_input_fingerprint"])

        assert response.status_code == 200
        assert response.json() == [target_record]
        assert "sections" not in json.dumps(response.json(), sort_keys=True)

    def test_get_section_bundles_returns_compact_records_in_deterministic_order(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_section_bundles")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        assert _execute_bundle(client, project_root, ["alpha-section"]).status_code == 400

        index_payload = _bundle_index_payload(project_root)
        records_by_fingerprint = {
            record["bundle_fingerprint"]: record
            for record in index_payload["bundles"]
        }
        expected_records = [
            records_by_fingerprint[bundle_fingerprint]
            for bundle_fingerprint in index_payload["by_section"]["alpha-section"]
        ]
        response = _get_section_bundles(client, project_root, "alpha-section")

        assert response.status_code == 200
        assert response.json() == expected_records
        assert "prepared_plan_fingerprint" not in json.dumps(response.json(), sort_keys=True)

    def test_get_bundle_by_unknown_fingerprint_returns_not_found(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_unknown")
        client = TestClient(create_app())

        response = _get_bundle_manifest(client, project_root, "missing-bundle")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle not found: missing-bundle"}

    def test_get_bundles_by_unknown_input_returns_not_found(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_input_unknown")
        client = TestClient(create_app())

        response = _get_bundles_by_input(client, project_root, "missing-input")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle input fingerprint not found: missing-input"}

    def test_get_section_bundles_for_unknown_participation_returns_not_found(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_section_unknown")
        client = TestClient(create_app())

        response = _get_section_bundles(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section bundle participation not found: missing-section"}

    def test_get_bundles_by_input_rejects_malformed_index(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_malformed_index")
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(
            json.dumps(
                {
                    "artifact_type": "bundle_execution_audit_index",
                    "bundles": "broken",
                    "by_section": {},
                    "generated_by": "DGCE",
                    "schema_version": "1.0",
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        client = TestClient(create_app())

        response = _get_bundles_by_input(client, project_root, "any-input")

        assert response.status_code == 400
        assert response.json() == {"detail": "Bundle index is malformed"}

    def test_get_bundle_by_fingerprint_rejects_malformed_manifest(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_malformed_manifest")
        bundle_path = project_root / ".dce" / "execution" / "bundles" / "bad-bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(
            json.dumps(
                {
                    "artifact_type": "bundle_execution_audit_manifest",
                    "bundle_fingerprint": "bad-bundle",
                    "bundle_input_fingerprint": "input",
                    "execution_status": "success",
                    "first_failing_section": None,
                    "generated_by": "DGCE",
                    "schema_version": "1.0",
                    "section_ids": ["alpha-section"],
                    "sections": [],
                    "stopped_early": False,
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        client = TestClient(create_app())

        response = _get_bundle_manifest(client, project_root, "bad-bundle")

        assert response.status_code == 400
        assert response.json() == {"detail": "Bundle audit manifest is malformed"}

    def test_bundle_read_endpoints_are_deterministic_across_identical_prepared_state(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_get_bundle_repeat_two")
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")
            assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200

        first_manifest = _bundle_manifest_payload(first_root)
        second_manifest = _bundle_manifest_payload(second_root)
        first_manifest_response = _get_bundle_manifest(client, first_root, first_manifest["bundle_fingerprint"])
        second_manifest_response = _get_bundle_manifest(client, second_root, second_manifest["bundle_fingerprint"])
        first_index_record = _bundle_index_payload(first_root)["bundles"][0]
        second_index_record = _bundle_index_payload(second_root)["bundles"][0]
        first_input_response = _get_bundles_by_input(client, first_root, first_index_record["bundle_input_fingerprint"])
        second_input_response = _get_bundles_by_input(client, second_root, second_index_record["bundle_input_fingerprint"])
        first_section_response = _get_section_bundles(client, first_root, "alpha-section")
        second_section_response = _get_section_bundles(client, second_root, "alpha-section")

        assert first_manifest_response.status_code == 200
        assert second_manifest_response.status_code == 200
        assert first_manifest_response.content == second_manifest_response.content
        assert first_input_response.status_code == 200
        assert second_input_response.status_code == 200
        assert first_input_response.content == second_input_response.content
        assert first_section_response.status_code == 200
        assert second_section_response.status_code == 200
        assert first_section_response.content == second_section_response.content

    def test_get_section_provenance_returns_compact_chain_for_fully_executed_section(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_provenance_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _mark_alpha_ready(project_root)
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
        approval_lineage = dgce_execute_api._compute_prepared_plan_approval_lineage(project_root, "mission-board")
        prepared_plan_payload = json.loads((project_root / ".dce" / "plans" / "mission-board.prepared_plan.json").read_text(encoding="utf-8"))
        execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        bundle_references = dgce_execute_api.get_bundle_index_records_for_section(project_root, "mission-board")

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval": {
                "approval_artifact_fingerprint": approval_lineage["approval_artifact_fingerprint"],
                "approval_path": ".dce/approvals/mission-board.approval.json",
                "approval_record_fingerprint": approval_lineage["approval_record_fingerprint"],
                "approval_status": approval_payload["approval_status"],
                "execution_permitted": approval_payload["execution_permitted"],
                "selected_mode": approval_payload["selected_mode"],
            },
            "prepared_plan": {
                "approval_lineage_fingerprint": prepared_plan_payload["approval_lineage_fingerprint"],
                "binding_fingerprint": prepared_plan_payload["binding_fingerprint"],
                "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
                "prepared_plan_path": ".dce/plans/mission-board.prepared_plan.json",
            },
            "execution": {
                "execution_artifact_path": ".dce/execution/mission-board.execution.json",
                "execution_status": execution_payload["execution_status"],
                "prepared_plan_audit_fingerprint": execution_payload["prepared_plan_audit_fingerprint"],
                "prepared_plan_cross_link_fingerprint": execution_payload["prepared_plan_cross_link_fingerprint"],
                "written_files": execution_payload["prepared_plan_audit_manifest"]["written_files"],
            },
            "bundle_references": bundle_references,
        }

    def test_get_section_provenance_returns_partial_deterministic_chain_when_only_approval_exists(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_partial")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
        approval_lineage = dgce_execute_api._compute_prepared_plan_approval_lineage(project_root, "mission-board")
        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval": {
                "approval_artifact_fingerprint": approval_payload["artifact_fingerprint"],
                "approval_path": ".dce/approvals/mission-board.approval.json",
                "approval_record_fingerprint": approval_lineage["approval_record_fingerprint"],
                "approval_status": approval_payload["approval_status"],
                "execution_permitted": approval_payload["execution_permitted"],
                "selected_mode": approval_payload["selected_mode"],
            },
            "prepared_plan": {
                "approval_lineage_fingerprint": None,
                "binding_fingerprint": None,
                "prepared_plan_fingerprint": None,
                "prepared_plan_path": None,
            },
            "execution": {
                "execution_artifact_path": None,
                "execution_status": None,
                "prepared_plan_audit_fingerprint": None,
                "prepared_plan_cross_link_fingerprint": None,
                "written_files": None,
            },
            "bundle_references": [],
        }

    def test_get_section_provenance_returns_not_found_for_ungrounded_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_missing")
        client = TestClient(create_app())

        response = _get_section_provenance(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section provenance not found: missing-section"}

    def test_get_section_provenance_rejects_malformed_approval_artifact(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_bad_approval")
        _mark_section_ready(project_root)
        approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
        approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
        approval_payload["artifact_fingerprint"] = None
        approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        client = TestClient(create_app())

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 400
        assert response.json() == {"detail": "Approval artifact is malformed: mission-board"}

    def test_get_section_provenance_rejects_malformed_prepared_plan_artifact(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_bad_prepared")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding_fingerprint"] = None
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan artifact is malformed: mission-board"}

    def test_get_section_provenance_rejects_malformed_execution_artifact(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_bad_execution")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_payload["prepared_plan_cross_link"]["section_id"] = "other-section"
        execution_payload["prepared_plan_cross_link_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )
        execution_path.write_text(json.dumps(execution_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 400
        assert response.json() == {
            "detail": "execution_stamp schema validation failed: prepared_plan_cross_link.section_id must match section_id"
        }

    def test_get_section_provenance_rejects_malformed_bundle_index(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_provenance_bad_index")
        _mark_section_ready(project_root)
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(
            json.dumps(
                {
                    "artifact_type": "bundle_execution_audit_index",
                    "bundles": "broken",
                    "by_section": {},
                    "generated_by": "DGCE",
                    "schema_version": "1.0",
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        client = TestClient(create_app())

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 400
        assert response.json() == {"detail": "Bundle index is malformed"}

    def test_section_provenance_response_is_deterministic_across_identical_state(self, monkeypatch):
        first_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_provenance_repeat_one")
        second_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_provenance_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            assert client.post(
                "/v1/dgce/sections/mission-board/approve",
                json={"workspace_path": str(project_root)},
            ).status_code == 200
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "mission-board")
            _prepare_section(client, project_root, "alpha-section")
            assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        first_response = _get_section_provenance(client, first_root, "mission-board")
        second_response = _get_section_provenance(client, second_root, "mission-board")

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content

    def test_section_provenance_remains_compact_without_large_payload_duplication(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_provenance_compact")
        client = TestClient(create_app())
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200

        response = _get_section_provenance(client, project_root, "mission-board")

        assert response.status_code == 200
        serialized = json.dumps(response.json(), indent=2, sort_keys=True)
        assert "\"file_plan\":" not in serialized
        assert "\"binding\":" not in serialized
        assert "\"approval_lineage\":" not in serialized
        assert "\"prepared_plan_audit_manifest\":" not in serialized
        assert "\"sections\":" not in serialized

    def test_verify_section_returns_verified_true_for_valid_fully_executed_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_section_valid")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200

        response = _verify_section(client, project_root, "mission-board")

        assert response.status_code == 200
        payload = response.json()
        assert payload["subject_type"] == "section"
        assert payload["subject_id"] == "mission-board"
        assert payload["verified"] is True
        assert payload["failure_count"] == 0
        assert [check["check_id"] for check in payload["checks"]] == [
            "approval.exists",
            "approval.valid",
            "approval.identity",
            "prepared_plan.exists",
            "prepared_plan.valid",
            "prepared_plan.binding",
            "prepared_plan.lineage",
            "prepared_plan.fingerprint",
            "execution.exists",
            "execution.valid",
            "execution.audit",
            "execution.cross_link",
            "execution.prepared_plan_identity",
        ]

    def test_verify_section_detects_prepared_plan_self_seal_mismatch(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_section_bad_prepared")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding_fingerprint"] = "tampered"
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _verify_section(client, project_root, "mission-board")

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert payload["failure_count"] >= 1
        assert any(
            check["check_id"] == "prepared_plan.valid" and check["status"] == "fail"
            for check in payload["checks"]
        )

    def test_verify_section_detects_execution_cross_link_mismatch(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_section_bad_cross_link")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_payload["prepared_plan_cross_link"]["section_id"] = "other-section"
        execution_payload["prepared_plan_cross_link_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )
        execution_path.write_text(json.dumps(execution_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _verify_section(client, project_root, "mission-board")

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert any(
            check["check_id"] == "execution.valid" and check["status"] == "fail"
            for check in payload["checks"]
        )

    def test_verify_section_handles_partial_grounded_state_deterministically(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_section_partial")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = _verify_section(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "subject_type": "section",
            "subject_id": "mission-board",
            "verified": True,
            "checks": [
                {"check_id": "approval.exists", "status": "pass", "artifact": "approval", "reason": "Approval artifact exists"},
                {"check_id": "approval.valid", "status": "pass", "artifact": "approval", "reason": "Approval artifact is valid"},
                {"check_id": "approval.identity", "status": "pass", "artifact": "approval", "reason": "Approval identity fields are present"},
                {"check_id": "prepared_plan.exists", "status": "pass", "artifact": "prepared_plan", "reason": "Prepared plan artifact not present"},
                {"check_id": "execution.exists", "status": "pass", "artifact": "execution", "reason": "Execution artifact not present"},
            ],
            "failure_count": 0,
        }

    def test_verify_section_returns_not_found_for_unknown_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_section_missing")
        client = TestClient(create_app())

        response = _verify_section(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section not found: missing-section"}

    def test_verify_bundle_returns_verified_true_for_valid_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_bundle_valid")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)

        response = _verify_bundle(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["subject_type"] == "bundle"
        assert payload["subject_id"] == bundle_manifest["bundle_fingerprint"]
        assert payload["verified"] is True
        assert payload["failure_count"] == 0

    def test_verify_bundle_detects_manifest_index_mismatch(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_bundle_bad_index")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_payload = json.loads(index_path.read_text(encoding="utf-8"))
        index_payload["bundles"][0]["execution_status"] = "failed"
        index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _verify_bundle(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert any(
            check["check_id"] == "bundle_index.match" and check["status"] == "fail"
            for check in payload["checks"]
        )

    def test_verify_bundle_detects_per_section_fingerprint_mismatch(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_bundle_bad_section")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_payload["prepared_plan_audit_manifest"]["binding_fingerprint"] = "tampered"
        execution_payload["prepared_plan_audit_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_audit_manifest"]
        )
        execution_payload["prepared_plan_cross_link"]["prepared_plan_audit_fingerprint"] = execution_payload["prepared_plan_audit_fingerprint"]
        execution_payload["prepared_plan_cross_link_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )
        execution_path.write_text(json.dumps(execution_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _verify_bundle(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert any(
            check["check_id"] == "bundle_section.mission-board.binding" and check["status"] == "fail"
            for check in payload["checks"]
        )

    def test_verify_bundle_detects_fail_fast_incoherence_when_manifest_is_tampered(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_bundle_fail_fast")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 400
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_path = project_root / ".dce" / "execution" / "bundles" / f"{bundle_manifest['bundle_fingerprint']}.json"
        tampered_manifest = dict(bundle_manifest)
        tampered_manifest["sections"] = bundle_manifest["sections"] + [
            {
                "approval_lineage_fingerprint": None,
                "binding_fingerprint": None,
                "execution_artifact_path": ".dce/execution/alpha-section.execution.json",
                "prepared_plan_audit_fingerprint": None,
                "prepared_plan_fingerprint": None,
                "section_id": "alpha-section",
                "status": "failed",
            }
        ]
        tampered_manifest["bundle_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            {key: value for key, value in tampered_manifest.items() if key != "bundle_fingerprint"}
        )
        bundle_path.unlink()
        (project_root / ".dce" / "execution" / "bundles" / "index.json").write_text(
            json.dumps(
                {
                    **_bundle_index_payload(project_root),
                    "bundles": [
                        {
                            **_bundle_index_payload(project_root)["bundles"][0],
                            "bundle_fingerprint": tampered_manifest["bundle_fingerprint"],
                            "manifest_path": f".dce/execution/bundles/{tampered_manifest['bundle_fingerprint']}.json",
                        }
                    ],
                    "by_section": {
                        "alpha-section": [tampered_manifest["bundle_fingerprint"]],
                        "mission-board": [tampered_manifest["bundle_fingerprint"]],
                    },
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        (project_root / ".dce" / "execution" / "bundles" / f"{tampered_manifest['bundle_fingerprint']}.json").write_text(
            json.dumps(tampered_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        response = _verify_bundle(client, project_root, tampered_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert any(
            check["check_id"] == "bundle_manifest.fail_fast" and check["status"] == "fail"
            for check in payload["checks"]
        )

    @pytest.mark.parametrize(
        ("field_name", "field_value", "expected_check_id"),
        [
            ("input_section_ids", ["alpha-section", "mission-board"], "bundle_manifest.input"),
            ("effective_execution_order", ["mission-board", "alpha-section"], "bundle_manifest.order"),
            ("order_source", "input_order", "bundle_manifest.order"),
        ],
    )
    def test_verify_bundle_detects_order_linkage_tampering(self, monkeypatch, field_name, field_value, expected_check_id):
        project_root = _build_workspace(monkeypatch, f"dgce_execute_api_verify_bundle_order_tamper_{field_name}")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(
            client,
            project_root,
            ["mission-board", "alpha-section"],
            planned_order=["alpha-section", "mission-board"],
        ).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_path = project_root / ".dce" / "execution" / "bundles" / f"{bundle_manifest['bundle_fingerprint']}.json"
        tampered_manifest = dict(bundle_manifest)
        tampered_manifest[field_name] = field_value
        tampered_manifest["bundle_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            {key: value for key, value in tampered_manifest.items() if key != "bundle_fingerprint"}
        )
        bundle_path.unlink()
        index_payload = _bundle_index_payload(project_root)
        index_payload["bundles"][0]["bundle_fingerprint"] = tampered_manifest["bundle_fingerprint"]
        index_payload["bundles"][0]["manifest_path"] = f".dce/execution/bundles/{tampered_manifest['bundle_fingerprint']}.json"
        index_payload["by_section"] = {
            "alpha-section": [tampered_manifest["bundle_fingerprint"]],
            "mission-board": [tampered_manifest["bundle_fingerprint"]],
        }
        (project_root / ".dce" / "execution" / "bundles" / "index.json").write_text(
            json.dumps(index_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (project_root / ".dce" / "execution" / "bundles" / f"{tampered_manifest['bundle_fingerprint']}.json").write_text(
            json.dumps(tampered_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        response = _verify_bundle(client, project_root, tampered_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["verified"] is False
        assert any(
            check["check_id"] == expected_check_id and check["status"] == "fail"
            for check in payload["checks"]
        )

    def test_verify_bundle_returns_not_found_for_unknown_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_bundle_missing")
        client = TestClient(create_app())

        response = _verify_bundle(client, project_root, "missing-bundle")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle not found: missing-bundle"}

    def test_verify_endpoints_are_deterministic_and_compact(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_verify_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")
            assert client.post(
                "/v1/dgce/sections/mission-board/execute",
                json={"workspace_path": str(project_root)},
            ).status_code == 200
            assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 400

        first_bundle = _bundle_manifest_payload(first_root)["bundle_fingerprint"]
        second_bundle = _bundle_manifest_payload(second_root)["bundle_fingerprint"]
        first_section_response = _verify_section(client, first_root, "mission-board")
        second_section_response = _verify_section(client, second_root, "mission-board")
        first_bundle_response = _verify_bundle(client, first_root, first_bundle)
        second_bundle_response = _verify_bundle(client, second_root, second_bundle)

        assert first_section_response.status_code == 200
        assert second_section_response.status_code == 200
        assert first_section_response.content == second_section_response.content
        assert first_bundle_response.status_code == 200
        assert second_bundle_response.status_code == 200
        assert first_bundle_response.content == second_bundle_response.content
        assert "\"written_files\":" not in first_bundle_response.text
        assert "\"prepared_plan_audit_manifest\":" not in first_section_response.text

    def test_get_section_summary_returns_expected_compact_summary_for_fully_executed_section(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_summary_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _mark_alpha_ready(project_root)
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        prepared_plan_payload = json.loads((project_root / ".dce" / "plans" / "mission-board.prepared_plan.json").read_text(encoding="utf-8"))
        execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
        response = _get_section_summary(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval_present": True,
            "approval_status": "superseded",
            "selected_mode": "create_only",
            "execution_permitted": False,
            "prepared_plan_present": True,
            "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
            "binding_fingerprint": prepared_plan_payload["binding_fingerprint"],
            "approval_lineage_fingerprint": prepared_plan_payload["approval_lineage_fingerprint"],
            "execution_present": True,
            "execution_status": execution_payload["execution_status"],
            "execution_artifact_path": ".dce/execution/mission-board.execution.json",
            "prepared_plan_audit_fingerprint": execution_payload["prepared_plan_audit_fingerprint"],
            "prepared_plan_cross_link_fingerprint": execution_payload["prepared_plan_cross_link_fingerprint"],
            "written_files_count": len(execution_payload["prepared_plan_audit_manifest"]["written_files"]),
            "written_file_paths": [entry["path"] for entry in execution_payload["prepared_plan_audit_manifest"]["written_files"]],
            "provenance_verified": True,
            "verification_failure_count": 0,
            "failing_check_ids": [],
            "bundle_count": 1,
            "bundle_references": [
                {
                    "bundle_fingerprint": _bundle_index_payload(project_root)["bundles"][0]["bundle_fingerprint"],
                    "execution_status": "success",
                }
            ],
            "simulation": {
                "findings_count": 0,
                "finding_codes": [],
                "provider_selection_source": "not_applicable",
                "reason_code": None,
                "reason_summary": None,
                "simulation_provider": None,
                "simulation_stage_applicable": True,
                "simulation_status": "skipped",
                "simulation_triggered": False,
            },
        }

    def test_get_section_summary_reflects_failing_check_ids_deterministically_when_tampered(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_summary_tampered")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding_fingerprint"] = "tampered"
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_section_summary(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json()["provenance_verified"] is False
        assert response.json()["verification_failure_count"] > 0
        assert response.json()["failing_check_ids"] == [
            "prepared_plan.valid",
            "prepared_plan.binding",
            "prepared_plan.lineage",
            "prepared_plan.fingerprint",
        ]

    def test_get_section_summary_returns_partial_grounded_summary_deterministically(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_summary_partial")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = _get_section_summary(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval_present": True,
            "approval_status": "approved",
            "selected_mode": "create_only",
            "execution_permitted": True,
            "prepared_plan_present": False,
            "prepared_plan_fingerprint": None,
            "binding_fingerprint": None,
            "approval_lineage_fingerprint": None,
            "execution_present": False,
            "execution_status": None,
            "execution_artifact_path": None,
            "prepared_plan_audit_fingerprint": None,
            "prepared_plan_cross_link_fingerprint": None,
            "written_files_count": 0,
            "written_file_paths": [],
            "provenance_verified": True,
            "verification_failure_count": 0,
            "failing_check_ids": [],
            "bundle_count": 0,
            "bundle_references": [],
            "simulation": {
                "findings_count": 0,
                "finding_codes": [],
                "provider_selection_source": None,
                "reason_code": None,
                "reason_summary": None,
                "simulation_provider": None,
                "simulation_stage_applicable": False,
                "simulation_status": None,
                "simulation_triggered": False,
            },
        }

    def test_get_section_summary_returns_not_found_for_unknown_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_summary_missing")
        client = TestClient(create_app())

        response = _get_section_summary(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section provenance not found: missing-section"}

    def test_get_section_summary_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_summary_repeat_one")
        second_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_summary_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            assert client.post(
                "/v1/dgce/sections/mission-board/approve",
                json={"workspace_path": str(project_root)},
            ).status_code == 200
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "mission-board")
            _prepare_section(client, project_root, "alpha-section")
            assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        first_response = _get_section_summary(client, first_root, "mission-board")
        second_response = _get_section_summary(client, second_root, "mission-board")

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"written_files\":" not in serialized
        assert "\"checks\":" not in serialized
        assert "\"binding\":" not in serialized
        assert "\"approval_lineage\":" not in serialized

    def test_get_bundle_summary_returns_expected_compact_summary_for_valid_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_index = _bundle_index_payload(project_root)

        response = _get_bundle_summary(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json() == {
            "bundle_fingerprint": bundle_manifest["bundle_fingerprint"],
            "bundle_input_fingerprint": bundle_manifest["bundle_input_fingerprint"],
            "execution_status": "success",
            "stopped_early": False,
            "first_failing_section": None,
            "section_count": 2,
            "section_ids": ["alpha-section", "mission-board"],
            "bundle_verified": True,
            "verification_failure_count": 0,
            "failing_check_ids": [],
            "manifest_path": f".dce/execution/bundles/{bundle_manifest['bundle_fingerprint']}.json",
            "index_present": True,
            "sections": bundle_manifest["sections"],
        }
        assert bundle_index["bundles"][0]["bundle_fingerprint"] == bundle_manifest["bundle_fingerprint"]

    def test_get_bundle_summary_reflects_failing_check_ids_when_bundle_is_tampered(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_tampered")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_payload = json.loads(index_path.read_text(encoding="utf-8"))
        index_payload["bundles"][0]["execution_status"] = "failed"
        index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_bundle_summary(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json()["bundle_verified"] is False
        assert response.json()["verification_failure_count"] > 0
        assert response.json()["failing_check_ids"] == ["bundle_index.match"]

    def test_get_bundle_summary_returns_not_found_for_unknown_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_missing")
        client = TestClient(create_app())

        response = _get_bundle_summary(client, project_root, "missing-bundle")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle not found: missing-bundle"}

    def test_get_bundle_summary_returns_grounded_inconsistent_bundle_with_verification_failures(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_inconsistent")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        bundle_path = project_root / ".dce" / "execution" / "bundles" / f"{bundle_manifest['bundle_fingerprint']}.json"
        tampered_manifest = dict(bundle_manifest)
        tampered_manifest["sections"] = "broken"
        bundle_path.write_text(json.dumps(tampered_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_bundle_summary(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json() == {
            "bundle_fingerprint": bundle_manifest["bundle_fingerprint"],
            "bundle_input_fingerprint": bundle_manifest["bundle_input_fingerprint"],
            "execution_status": "success",
            "stopped_early": False,
            "first_failing_section": None,
            "section_count": 2,
            "section_ids": ["alpha-section", "mission-board"],
            "bundle_verified": False,
            "verification_failure_count": 3,
            "failing_check_ids": [
                "bundle_manifest.valid",
                "bundle_manifest.input",
                "bundle_manifest.order",
            ],
            "manifest_path": f".dce/execution/bundles/{bundle_manifest['bundle_fingerprint']}.json",
            "index_present": True,
            "sections": [],
        }

    def test_get_bundle_summary_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_summary_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")
            assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200

        first_bundle = _bundle_manifest_payload(first_root)["bundle_fingerprint"]
        second_bundle = _bundle_manifest_payload(second_root)["bundle_fingerprint"]
        first_response = _get_bundle_summary(client, first_root, first_bundle)
        second_response = _get_bundle_summary(client, second_root, second_bundle)

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"checks\":" not in serialized
        assert "\"written_files\":" not in serialized
        assert "\"prepared_plan_audit_manifest\":" not in serialized

    def test_get_section_overview_returns_correct_composed_data_for_fully_executed_section(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_overview_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _mark_alpha_ready(project_root)
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        summary = _get_section_summary(client, project_root, "mission-board").json()
        verification = _verify_section(client, project_root, "mission-board").json()
        bundle_refs = summary["bundle_references"]
        latest_bundle_fingerprint = sorted(ref["bundle_fingerprint"] for ref in bundle_refs)[-1]

        response = _get_section_overview(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval_present": summary["approval_present"],
            "approval_status": summary["approval_status"],
            "selected_mode": summary["selected_mode"],
            "execution_permitted": summary["execution_permitted"],
            "prepared_plan_present": summary["prepared_plan_present"],
            "prepared_plan_fingerprint": summary["prepared_plan_fingerprint"],
            "execution_present": summary["execution_present"],
            "execution_status": summary["execution_status"],
            "written_files_count": summary["written_files_count"],
            "provenance_verified": verification["verified"],
            "verification_failure_count": verification["failure_count"],
            "failing_check_ids": [check["check_id"] for check in verification["checks"] if check["status"] == "fail"],
            "execution_artifact_path": summary["execution_artifact_path"],
            "prepared_plan_audit_fingerprint": summary["prepared_plan_audit_fingerprint"],
            "prepared_plan_cross_link_fingerprint": summary["prepared_plan_cross_link_fingerprint"],
            "bundle_count": summary["bundle_count"],
            "latest_bundle_fingerprint": latest_bundle_fingerprint,
            "bundle_references": bundle_refs,
            "simulation": summary["simulation"],
            "is_executable": False,
            "has_been_executed": True,
            "has_provenance_issues": False,
        }

    def test_get_section_overview_reflects_verification_failures_and_state_flags(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_overview_tampered")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding_fingerprint"] = "tampered"
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_section_overview(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json()["provenance_verified"] is False
        assert response.json()["verification_failure_count"] == 4
        assert response.json()["failing_check_ids"] == [
            "prepared_plan.valid",
            "prepared_plan.binding",
            "prepared_plan.lineage",
            "prepared_plan.fingerprint",
        ]
        assert response.json()["is_executable"] is True
        assert response.json()["has_been_executed"] is False
        assert response.json()["has_provenance_issues"] is True

    def test_get_section_overview_returns_partial_grounded_overview_deterministically(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_overview_partial")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = _get_section_overview(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "approval_present": True,
            "approval_status": "approved",
            "selected_mode": "create_only",
            "execution_permitted": True,
            "prepared_plan_present": False,
            "prepared_plan_fingerprint": None,
            "execution_present": False,
            "execution_status": None,
            "written_files_count": 0,
            "provenance_verified": True,
            "verification_failure_count": 0,
            "failing_check_ids": [],
            "execution_artifact_path": None,
            "prepared_plan_audit_fingerprint": None,
            "prepared_plan_cross_link_fingerprint": None,
            "bundle_count": 0,
            "latest_bundle_fingerprint": None,
            "bundle_references": [],
            "simulation": {
                "findings_count": 0,
                "finding_codes": [],
                "provider_selection_source": None,
                "reason_code": None,
                "reason_summary": None,
                "simulation_provider": None,
                "simulation_stage_applicable": False,
                "simulation_status": None,
                "simulation_triggered": False,
            },
            "is_executable": True,
            "has_been_executed": False,
            "has_provenance_issues": False,
        }

    def test_get_section_overview_returns_not_found_for_unknown_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_overview_missing")
        client = TestClient(create_app())

        response = _get_section_overview(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section provenance not found: missing-section"}

    def test_get_section_overview_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_overview_repeat_one")
        second_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_overview_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            assert client.post(
                "/v1/dgce/sections/mission-board/approve",
                json={"workspace_path": str(project_root)},
            ).status_code == 200
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "mission-board")
            _prepare_section(client, project_root, "alpha-section")
            assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        first_response = _get_section_overview(client, first_root, "mission-board")
        second_response = _get_section_overview(client, second_root, "mission-board")

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"checks\":" not in serialized
        assert "\"written_files\":" not in serialized
        assert "\"prepared_plan_audit_manifest\":" not in serialized
        assert "\"binding\":" not in serialized

    def test_get_bundle_overview_returns_correct_composed_data_for_valid_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_overview_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        summary = _get_bundle_summary(client, project_root, _bundle_manifest_payload(project_root)["bundle_fingerprint"]).json()
        verification = _verify_bundle(client, project_root, summary["bundle_fingerprint"]).json()

        response = _get_bundle_overview(client, project_root, summary["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json() == {
            "bundle_fingerprint": summary["bundle_fingerprint"],
            "bundle_input_fingerprint": summary["bundle_input_fingerprint"],
            "execution_status": summary["execution_status"],
            "stopped_early": summary["stopped_early"],
            "first_failing_section": summary["first_failing_section"],
            "section_count": summary["section_count"],
            "section_ids": summary["section_ids"],
            "bundle_verified": verification["verified"],
            "verification_failure_count": verification["failure_count"],
            "failing_check_ids": [check["check_id"] for check in verification["checks"] if check["status"] == "fail"],
            "manifest_path": summary["manifest_path"],
            "index_present": summary["index_present"],
            "sections": [
                {
                    "section_id": section["section_id"],
                    "status": section["status"],
                    "execution_artifact_path": section["execution_artifact_path"],
                    "prepared_plan_fingerprint": section["prepared_plan_fingerprint"],
                    "prepared_plan_audit_fingerprint": section["prepared_plan_audit_fingerprint"],
                }
                for section in summary["sections"]
            ],
            "is_complete_success": True,
            "has_failures": False,
            "has_verification_issues": False,
        }

    def test_get_bundle_overview_reflects_verification_failures_and_state_flags(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_overview_tampered")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_payload = json.loads(index_path.read_text(encoding="utf-8"))
        index_payload["bundles"][0]["execution_status"] = "failed"
        index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_bundle_overview(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json()["bundle_verified"] is False
        assert response.json()["verification_failure_count"] == 1
        assert response.json()["failing_check_ids"] == ["bundle_index.match"]
        assert response.json()["is_complete_success"] is True
        assert response.json()["has_failures"] is False
        assert response.json()["has_verification_issues"] is True

    def test_get_bundle_overview_returns_not_found_for_unknown_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_overview_missing")
        client = TestClient(create_app())

        response = _get_bundle_overview(client, project_root, "missing-bundle")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle not found: missing-bundle"}

    def test_get_bundle_overview_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_overview_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_overview_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")
            assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200

        first_bundle = _bundle_manifest_payload(first_root)["bundle_fingerprint"]
        second_bundle = _bundle_manifest_payload(second_root)["bundle_fingerprint"]
        first_response = _get_bundle_overview(client, first_root, first_bundle)
        second_response = _get_bundle_overview(client, second_root, second_bundle)

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"checks\":" not in serialized
        assert "\"written_files\":" not in serialized
        assert "\"binding_fingerprint\":" not in serialized
        assert "\"approval_lineage_fingerprint\":" not in serialized

    def test_get_section_dashboard_returns_correct_compact_data_for_fully_executed_healthy_section(self, monkeypatch):
        project_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_dashboard_full")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        client = TestClient(create_app())
        assert client.post(
            "/v1/dgce/sections/mission-board/approve",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        _mark_alpha_ready(project_root)
        _prepare_section(client, project_root, "mission-board")
        _prepare_section(client, project_root, "alpha-section")
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        summary = _get_section_summary(client, project_root, "mission-board").json()
        overview = _get_section_overview(client, project_root, "mission-board").json()
        response = _get_section_dashboard(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json() == {
            "section_id": "mission-board",
            "health_status": "healthy",
            "provenance_verified": overview["provenance_verified"],
            "verification_failure_count": overview["verification_failure_count"],
            "alert_check_ids": [],
            "approval_status": summary["approval_status"],
            "selected_mode": summary["selected_mode"],
            "execution_permitted": summary["execution_permitted"],
            "execution_status": overview["execution_status"],
            "is_executable": overview["is_executable"],
            "has_been_executed": overview["has_been_executed"],
            "written_files_count": overview["written_files_count"],
            "prepared_plan_present": summary["prepared_plan_present"],
            "prepared_plan_fingerprint": summary["prepared_plan_fingerprint"],
            "binding_fingerprint": summary["binding_fingerprint"],
            "approval_lineage_fingerprint": summary["approval_lineage_fingerprint"],
            "execution_artifact_path": overview["execution_artifact_path"],
            "prepared_plan_audit_fingerprint": overview["prepared_plan_audit_fingerprint"],
            "prepared_plan_cross_link_fingerprint": overview["prepared_plan_cross_link_fingerprint"],
            "bundle_count": overview["bundle_count"],
            "latest_bundle_fingerprint": overview["latest_bundle_fingerprint"],
            "bundle_references": overview["bundle_references"],
            "simulation": summary["simulation"],
        }

    def test_get_section_summary_exposes_triggered_fail_simulation_projection(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_summary_stage75_fail")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        def failing_provider(_request):
            return {
                "simulation_status": "fail",
                "findings": [
                    {"code": "infra_modify_candidate", "summary": "Infrastructure dry-run detected a modify candidate.", "target": "deploy/docker-compose.yaml"}
                ],
            }

        monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", failing_provider)
        dgce_decompose.execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=True,
                simulation_provider="workspace_artifact",
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )

        response = _get_section_summary(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json()["simulation"] == {
            "findings_count": 1,
            "finding_codes": ["infra_modify_candidate"],
            "provider_selection_source": "explicit",
            "reason_code": "simulation_fail",
            "reason_summary": "Simulation produced concrete blocking findings.",
            "simulation_provider": "workspace_artifact",
            "simulation_stage_applicable": True,
            "simulation_status": "fail",
            "simulation_triggered": True,
        }

    def test_get_section_operator_surfaces_share_skipped_simulation_projection(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_stage75_skipped_consistency")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        dgce_decompose.execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=False,
                simulation_provider="infra_dry_run",
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )

        summary = _get_section_summary(client, project_root, "mission-board")
        overview = _get_section_overview(client, project_root, "mission-board")
        dashboard = _get_section_dashboard(client, project_root, "mission-board")

        assert summary.status_code == 200
        assert overview.status_code == 200
        assert dashboard.status_code == 200
        assert summary.json()["simulation"] == {
            "findings_count": 0,
            "finding_codes": [],
            "provider_selection_source": "not_applicable",
            "reason_code": None,
            "reason_summary": None,
            "simulation_provider": "infra_dry_run",
            "simulation_stage_applicable": True,
            "simulation_status": "skipped",
            "simulation_triggered": False,
        }
        assert overview.json()["simulation"] == summary.json()["simulation"]
        assert dashboard.json()["simulation"] == summary.json()["simulation"]

    def test_get_section_dashboard_health_is_warning_for_partial_grounded_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_dashboard_warning")
        _mark_section_ready(project_root)
        client = TestClient(create_app())

        response = _get_section_dashboard(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json()["health_status"] == "warning"
        assert response.json()["provenance_verified"] is True
        assert response.json()["verification_failure_count"] == 0
        assert response.json()["alert_check_ids"] == []

    def test_get_section_dashboard_health_is_error_for_grounded_section_with_verification_failures(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_dashboard_error")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding_fingerprint"] = "tampered"
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_section_dashboard(client, project_root, "mission-board")

        assert response.status_code == 200
        assert response.json()["health_status"] == "error"
        assert response.json()["alert_check_ids"] == [
            "prepared_plan.valid",
            "prepared_plan.binding",
            "prepared_plan.lineage",
            "prepared_plan.fingerprint",
        ]
        assert response.json()["verification_failure_count"] == 4

    def test_get_section_dashboard_returns_not_found_for_unknown_section(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_section_dashboard_missing")
        client = TestClient(create_app())

        response = _get_section_dashboard(client, project_root, "missing-section")

        assert response.status_code == 404
        assert response.json() == {"detail": "Section provenance not found: missing-section"}

    def test_get_section_dashboard_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_dashboard_repeat_one")
        second_root = _build_owned_workspace(monkeypatch, "dgce_execute_api_section_dashboard_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            assert client.post(
                "/v1/dgce/sections/mission-board/approve",
                json={"workspace_path": str(project_root)},
            ).status_code == 200
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "mission-board")
            _prepare_section(client, project_root, "alpha-section")
            assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 200

        first_response = _get_section_dashboard(client, first_root, "mission-board")
        second_response = _get_section_dashboard(client, second_root, "mission-board")

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"checks\":" not in serialized
        assert "\"written_files\":" not in serialized
        assert "\"prepared_plan_audit_manifest\":" not in serialized
        assert "\"sections\":" not in serialized

    def test_get_bundle_dashboard_returns_correct_compact_data_for_healthy_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_healthy")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        overview = _get_bundle_overview(client, project_root, _bundle_manifest_payload(project_root)["bundle_fingerprint"]).json()

        response = _get_bundle_dashboard(client, project_root, overview["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json() == {
            "bundle_fingerprint": overview["bundle_fingerprint"],
            "bundle_input_fingerprint": overview["bundle_input_fingerprint"],
            "health_status": "healthy",
            "bundle_verified": overview["bundle_verified"],
            "verification_failure_count": overview["verification_failure_count"],
            "alert_check_ids": [],
            "execution_status": overview["execution_status"],
            "stopped_early": overview["stopped_early"],
            "first_failing_section": overview["first_failing_section"],
            "section_count": overview["section_count"],
            "is_complete_success": overview["is_complete_success"],
            "has_failures": overview["has_failures"],
            "manifest_path": overview["manifest_path"],
            "index_present": overview["index_present"],
            "sections": overview["sections"],
            "has_verification_issues": overview["has_verification_issues"],
        }

    def test_get_bundle_dashboard_preserves_existing_verification_behavior_for_failed_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_warning")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "mission-board")
        assert client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
        assert _execute_bundle(client, project_root, ["mission-board", "alpha-section"]).status_code == 400
        bundle_fingerprint = _bundle_manifest_payload(project_root)["bundle_fingerprint"]
        verification = _verify_bundle(client, project_root, bundle_fingerprint).json()

        response = _get_bundle_dashboard(client, project_root, bundle_fingerprint)

        assert response.status_code == 200
        assert response.json()["health_status"] == "error"
        assert response.json()["bundle_verified"] == verification["verified"]
        assert response.json()["verification_failure_count"] == verification["failure_count"]
        assert response.json()["alert_check_ids"] == [
            check["check_id"] for check in verification["checks"] if check["status"] == "fail"
        ]

    def test_get_bundle_dashboard_health_is_error_for_bundle_with_verification_failures(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_error")
        run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
        _mark_section_ready(project_root)
        _mark_alpha_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root, "alpha-section")
        _prepare_section(client, project_root, "mission-board")
        assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200
        bundle_manifest = _bundle_manifest_payload(project_root)
        index_path = project_root / ".dce" / "execution" / "bundles" / "index.json"
        index_payload = json.loads(index_path.read_text(encoding="utf-8"))
        index_payload["bundles"][0]["execution_status"] = "failed"
        index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = _get_bundle_dashboard(client, project_root, bundle_manifest["bundle_fingerprint"])

        assert response.status_code == 200
        assert response.json()["health_status"] == "error"
        assert response.json()["verification_failure_count"] == 1
        assert response.json()["alert_check_ids"] == ["bundle_index.match"]
        assert response.json()["has_verification_issues"] is True

    def test_get_bundle_dashboard_returns_not_found_for_unknown_bundle(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_missing")
        client = TestClient(create_app())

        response = _get_bundle_dashboard(client, project_root, "missing-bundle")

        assert response.status_code == 404
        assert response.json() == {"detail": "Bundle not found: missing-bundle"}

    def test_get_bundle_dashboard_is_deterministic_and_compact(self, monkeypatch):
        first_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_repeat_one")
        second_root = _build_workspace(monkeypatch, "dgce_execute_api_bundle_dashboard_repeat_two")
        client = TestClient(create_app())
        for project_root in (first_root, second_root):
            run_section_with_workspace(_alpha_section(), project_root, incremental_mode="incremental_v2_2")
            _mark_section_ready(project_root)
            _mark_alpha_ready(project_root)
            _prepare_section(client, project_root, "alpha-section")
            _prepare_section(client, project_root, "mission-board")
            assert _execute_bundle(client, project_root, ["alpha-section", "mission-board"]).status_code == 200

        first_bundle = _bundle_manifest_payload(first_root)["bundle_fingerprint"]
        second_bundle = _bundle_manifest_payload(second_root)["bundle_fingerprint"]
        first_response = _get_bundle_dashboard(client, first_root, first_bundle)
        second_response = _get_bundle_dashboard(client, second_root, second_bundle)

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.content == second_response.content
        serialized = first_response.text
        assert "\"checks\":" not in serialized
        assert "\"written_files\":" not in serialized
        assert "\"binding_fingerprint\":" not in serialized
        assert "\"approval_lineage_fingerprint\":" not in serialized

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
        prepared_plan_payload = json.loads(
            (project_root / ".dce" / "plans" / "mission-board.prepared_plan.json").read_text(encoding="utf-8")
        )

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
        assert execution_payload["prepared_plan_audit_manifest"] == {
            "approval_lineage_fingerprint": prepared_plan_payload["approval_lineage_fingerprint"],
            "binding_fingerprint": prepared_plan_payload["binding_fingerprint"],
            "execution_permitted": True,
            "execution_status": execution_payload["execution_status"],
            "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
            "prepared_plan_path": ".dce/plans/mission-board.prepared_plan.json",
            "section_id": "mission-board",
            "selected_mode": "create_only",
            "written_files": [],
        }
        assert execution_payload["prepared_plan_audit_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_audit_manifest"]
        )
        assert execution_payload["prepared_plan_cross_link"] == {
            "prepared_plan_audit_fingerprint": execution_payload["prepared_plan_audit_fingerprint"],
            "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
            "prepared_plan_path": ".dce/plans/mission-board.prepared_plan.json",
            "section_id": "mission-board",
        }
        assert execution_payload["prepared_plan_cross_link_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )

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
        assert response.json() == {"detail": "Prepared file plan artifact is malformed: mission-board"}

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
        assert response.json() == {"detail": "Prepared file plan approval lineage mismatch: mission-board"}
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
        assert response.json() == {"detail": "Section is not eligible for execution: mission-board"}
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
        assert response.json() == {"detail": "Prepared file plan approval lineage mismatch: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_binding_payload_is_tampered(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_binding_tamper")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["binding"]["selected_mode"] = "safe_modify"
        prepared_plan_payload["binding_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            prepared_plan_payload["binding"]
        )
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan binding mismatch: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_file_plan_exceeds_approved_preview_scope(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_unapproved_file_plan_scope")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["file_plan"]["files"].append(
            {
                "path": "docs/unapproved.md",
                "purpose": "rogue write",
                "source": "expected_targets",
                "requirements": [],
            }
        )
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan artifact exceeds approved preview scope: mission-board"}
        assert (project_root / "docs" / "unapproved.md").exists() is False
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_file_plan_attempts_path_escape(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_path_escape")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["file_plan"]["files"].append(
            {
                "path": "../escape.py",
                "purpose": "rogue write",
                "source": "expected_targets",
                "requirements": [],
            }
        )
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan artifact contains invalid path"}
        assert (project_root.parent / "escape.py").exists() is False
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_approval_artifact_mutates_after_prepare(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_approval_lineage_mutation")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
        approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
        approval_payload["notes"] = "mutated after prepare"
        approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Section is not eligible for execution: mission-board"}
        assert (project_root / ".dce" / "execution" / "mission-board.execution.json").exists() is False
        assert (project_root / ".dce" / "outputs" / "mission-board.json").exists() is False

    def test_execute_rejects_prepared_plan_when_approval_lineage_section_mismatches(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_approval_lineage_section_mismatch")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        prepared_plan_path = project_root / ".dce" / "plans" / "mission-board.prepared_plan.json"
        prepared_plan_payload = json.loads(prepared_plan_path.read_text(encoding="utf-8"))
        prepared_plan_payload["approval_lineage"]["section_id"] = "other-section"
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
        prepared_plan_path.write_text(json.dumps(prepared_plan_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Prepared file plan artifact section mismatch: mission-board"}

    def test_execution_artifact_cross_link_validation_rejects_inconsistent_metadata(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_cross_link_validation")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert response.status_code == 200

        execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_payload["prepared_plan_cross_link"]["prepared_plan_path"] = ".dce/plans/other-section.prepared_plan.json"

        with pytest.raises(ValueError, match="prepared_plan_cross_link.prepared_plan_path"):
            dgce_decompose._validate_locked_artifact_schema(execution_path, execution_payload)

    def test_execution_artifact_cross_link_validation_rejects_section_misuse(self, monkeypatch):
        project_root = _build_workspace(monkeypatch, "dgce_execute_api_cross_link_section_misuse")
        _mark_section_ready(project_root)
        client = TestClient(create_app())
        _prepare_section(client, project_root)

        response = client.post(
            "/v1/dgce/sections/mission-board/execute",
            json={"workspace_path": str(project_root)},
        )
        assert response.status_code == 200

        execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
        execution_payload["prepared_plan_cross_link"]["section_id"] = "other-section"
        execution_payload["prepared_plan_cross_link_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )

        with pytest.raises(ValueError, match="prepared_plan_cross_link.section_id"):
            dgce_decompose._validate_locked_artifact_schema(execution_path, execution_payload)

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

        assert client.post(
            "/v1/dgce/refresh",
            json={"workspace_path": str(project_root)},
        ).status_code == 200
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
        prepared_plan_payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload)
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
            "detail": "Prepared file plan artifact exceeds approved preview scope: mission-board"
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
        prepared_plan_payload = json.loads(
            (project_root / ".dce" / "plans" / "owned-bundle.prepared_plan.json").read_text(encoding="utf-8")
        )
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
        assert execution_payload["prepared_plan_audit_manifest"] == {
            "approval_lineage_fingerprint": prepared_plan_payload["approval_lineage_fingerprint"],
            "binding_fingerprint": prepared_plan_payload["binding_fingerprint"],
            "execution_permitted": True,
            "execution_status": execution_payload["execution_status"],
            "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
            "prepared_plan_path": ".dce/plans/owned-bundle.prepared_plan.json",
            "section_id": "owned-bundle",
            "selected_mode": "create_only",
            "written_files": execution_payload["written_files"],
        }
        assert execution_payload["prepared_plan_audit_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_audit_manifest"]
        )
        assert execution_payload["prepared_plan_cross_link"] == {
            "prepared_plan_audit_fingerprint": execution_payload["prepared_plan_audit_fingerprint"],
            "prepared_plan_fingerprint": dgce_decompose.compute_json_payload_fingerprint(prepared_plan_payload),
            "prepared_plan_path": ".dce/plans/owned-bundle.prepared_plan.json",
            "section_id": "owned-bundle",
        }
        assert execution_payload["prepared_plan_cross_link_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
            execution_payload["prepared_plan_cross_link"]
        )
        assert all(entry["path"] != "src/rogue/unowned.py" for entry in execution_payload["written_files"])
        assert all(
            entry["path"] != "src/rogue/unowned.py"
            for entry in execution_payload["prepared_plan_audit_manifest"]["written_files"]
        )

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

    def test_owned_bundle_rerun_audit_manifest_is_distinct_and_deterministic(self, monkeypatch):
        first_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_audit_rerun_one")
        second_root = _build_owned_bundle_workspace(monkeypatch, "dgce_execute_api_owned_bundle_audit_rerun_two")
        client = TestClient(create_app())

        initial_audit_fingerprints: dict[Path, str] = {}
        rerun_audit_payloads: dict[Path, bytes] = {}

        for project_root in (first_root, second_root):
            _mark_owned_bundle_ready(project_root, selected_mode="create_only")
            _prepare_section(client, project_root, "owned-bundle")
            first_response = client.post(
                "/v1/dgce/sections/owned-bundle/execute",
                json={"workspace_path": str(project_root)},
            )
            assert first_response.status_code == 200
            initial_execution_payload = json.loads(
                (project_root / ".dce" / "execution" / "owned-bundle.execution.json").read_text(encoding="utf-8")
            )
            initial_audit_fingerprints[project_root] = initial_execution_payload["prepared_plan_audit_fingerprint"]

            target_path = project_root / "src" / "api" / "ingest.py"
            target_path.write_text("operator modified file\n", encoding="utf-8")
            _mark_owned_bundle_ready(project_root, selected_mode="safe_modify")
            _prepare_section(client, project_root, "owned-bundle")
            rerun_response = client.post(
                "/v1/dgce/sections/owned-bundle/execute",
                json={"workspace_path": str(project_root), "rerun": True},
            )
            assert rerun_response.status_code == 200
            rerun_execution_payload = json.loads(
                (project_root / ".dce" / "execution" / "owned-bundle.execution.json").read_text(encoding="utf-8")
            )
            assert rerun_execution_payload["prepared_plan_audit_fingerprint"] != initial_audit_fingerprints[project_root]
            assert rerun_execution_payload["prepared_plan_cross_link"]["prepared_plan_audit_fingerprint"] == (
                rerun_execution_payload["prepared_plan_audit_fingerprint"]
            )
            rerun_audit_payloads[project_root] = json.dumps(
                {
                    "prepared_plan_audit_manifest": rerun_execution_payload["prepared_plan_audit_manifest"],
                    "prepared_plan_cross_link": rerun_execution_payload["prepared_plan_cross_link"],
                    "prepared_plan_cross_link_fingerprint": rerun_execution_payload["prepared_plan_cross_link_fingerprint"],
                },
                indent=2,
                sort_keys=True,
            ).encode("utf-8")

        assert rerun_audit_payloads[first_root] == rerun_audit_payloads[second_root]

    def test_plan_bundle_preserves_input_order_for_independent_sections(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_independent",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b"),
                _dependency_section("section-c"),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-c", "section-a", "section-b"])

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "plan_valid": True,
            "ordered_section_ids": ["section-c", "section-a", "section-b"],
            "input_section_ids": ["section-c", "section-a", "section-b"],
            "dependency_edges": [],
            "cycles_detected": [],
            "missing_dependencies": [],
        }

    def test_plan_bundle_orders_simple_dependency(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_simple_dependency",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-b", "section-a"])

        assert response.status_code == 200
        assert response.json()["ordered_section_ids"] == ["section-a", "section-b"]
        assert response.json()["plan_valid"] is True

    def test_plan_bundle_orders_multi_level_dependencies(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_multi_level_dependency",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
                _dependency_section("section-c", dependencies=["section-b"]),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-c", "section-b", "section-a"])

        assert response.status_code == 200
        assert response.json()["ordered_section_ids"] == ["section-a", "section-b", "section-c"]
        assert response.json()["plan_valid"] is True

    def test_plan_bundle_detects_cycles_without_throwing(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_cycle",
            [
                _dependency_section("section-a", dependencies=["section-b"]),
                _dependency_section("section-b", dependencies=["section-a"]),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-a", "section-b"])

        assert response.status_code == 200
        assert response.json() == {
            "status": "invalid",
            "plan_valid": False,
            "ordered_section_ids": [],
            "input_section_ids": ["section-a", "section-b"],
            "dependency_edges": [
                {"from": "section-a", "to": "section-b"},
                {"from": "section-b", "to": "section-a"},
            ],
            "cycles_detected": [["section-a", "section-b"]],
            "missing_dependencies": [],
            "detail": "Bundle plan contains dependency cycles",
        }

    def test_plan_bundle_detects_missing_dependencies(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_missing_dependency",
            [
                _dependency_section("section-a", dependencies=["section-missing"]),
                _dependency_section("section-b"),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-a", "section-b"])

        assert response.status_code == 200
        assert response.json() == {
            "status": "invalid",
            "plan_valid": False,
            "ordered_section_ids": ["section-a", "section-b"],
            "input_section_ids": ["section-a", "section-b"],
            "dependency_edges": [],
            "cycles_detected": [],
            "missing_dependencies": ["section-missing"],
            "detail": "Bundle plan contains missing dependencies",
        }

    def test_plan_bundle_rejects_duplicate_section_ids(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_duplicate_input",
            [_dependency_section("section-a")],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-a", "section-a"])

        assert response.status_code == 400
        assert response.json() == {"detail": "Bundle section_ids must be unique"}

    def test_plan_bundle_rejects_empty_section_list(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_empty_input",
            [_dependency_section("section-a")],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, [])

        assert response.status_code == 400
        assert response.json() == {"detail": "Bundle requires at least one section_id"}

    def test_plan_bundle_is_deterministic_across_runs(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_deterministic",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
                _dependency_section("section-c"),
            ],
        )
        client = TestClient(create_app())

        first_response = _plan_bundle(client, project_root, ["section-c", "section-b", "section-a"])
        second_response = _plan_bundle(client, project_root, ["section-c", "section-b", "section-a"])

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert first_response.json() == second_response.json()
        assert first_response.content == second_response.content

    def test_plan_bundle_does_not_expand_missing_section_set(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_no_expansion",
            [
                _dependency_section("section-a", dependencies=["section-missing"]),
                _dependency_section("section-b"),
                _dependency_section("section-missing"),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-a", "section-b"])

        assert response.status_code == 200
        payload = response.json()
        assert payload["plan_valid"] is False
        assert payload["ordered_section_ids"] == ["section-a", "section-b"]
        assert payload["input_section_ids"] == ["section-a", "section-b"]
        assert "section-missing" not in payload["ordered_section_ids"]

    def test_plan_bundle_reports_dependency_edges(self, monkeypatch):
        project_root = _build_dependency_workspace(
            monkeypatch,
            "dgce_plan_bundle_edges",
            [
                _dependency_section("section-a"),
                _dependency_section("section-b", dependencies=["section-a"]),
                _dependency_section("section-c", dependencies=["section-a", "section-b"]),
            ],
        )
        client = TestClient(create_app())

        response = _plan_bundle(client, project_root, ["section-c", "section-b", "section-a"])

        assert response.status_code == 200
        assert response.json()["dependency_edges"] == [
            {"from": "section-a", "to": "section-b"},
            {"from": "section-a", "to": "section-c"},
            {"from": "section-b", "to": "section-c"},
        ]
