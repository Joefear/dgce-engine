import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from apps.aether_api.main import create_app
from aether.dgce import DGCESection, run_section_with_workspace
from aether.dgce.context_assembly import assemble_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


@pytest.fixture(autouse=True)
def no_auth_env(monkeypatch):
    monkeypatch.delenv("DGCE_API_KEY", raising=False)


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
    output = _structured_stub_output(content)
    return ExecutionResult(
        output=output,
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


def _structured_stub_output(content: str) -> str:
    lowered = content.lower()
    if "data model" in lowered or "data_model" in lowered:
        return json.dumps(
            {
                "modules": [
                    {
                        "name": "SmokeModel",
                        "entities": ["SmokeRecord"],
                        "relationships": [],
                        "required": ["id"],
                        "identity_keys": ["id"],
                    }
                ],
                "entities": [
                    {
                        "name": "SmokeRecord",
                        "description": "Smoke fixture record",
                        "fields": [{"name": "id", "type": "str", "required": True}],
                    }
                ],
                "fields": [{"entity": "SmokeRecord", "name": "id", "type": "str"}],
                "relationships": [],
                "validation_rules": ["id is required"],
            },
            sort_keys=True,
        )
    if "api surface" in lowered or "api_surface" in lowered:
        return json.dumps(
            {
                "interfaces": ["SmokeApi"],
                "methods": {"get_smoke": {"method": "GET", "path": "/smoke/{id}"}},
                "inputs": {"get_smoke": {"id": "str"}},
                "outputs": {"get_smoke": {"id": "str", "status": "str"}},
                "error_cases": {"get_smoke": ["not_found"]},
            },
            sort_keys=True,
        )
    if "system breakdown" in lowered or "system_breakdown" in lowered:
        return json.dumps(
            {
                "module_name": "SmokeModule",
                "purpose": "Materialize the smoke API and model.",
                "subcomponents": ["SmokeApi", "SmokeRecord"],
                "dependencies": [],
                "implementation_order": ["SmokeApi", "SmokeRecord"],
            },
            sort_keys=True,
        )
    return "Summary output"


def _smoke_section() -> DGCESection:
    return DGCESection(
        section_id="smoke-system",
        section_type="game_system",
        title="Smoke System",
        description="A compact deterministic section for the Stage 0 through Stage 9 smoke fixture.",
        requirements=["materialize a tiny smoke API", "materialize a tiny smoke model"],
        constraints=["only write explicit expected targets", "keep lifecycle artifacts deterministic"],
        expected_targets=[
            {
                "path": "api/smoke.py",
                "purpose": "Smoke API surface",
                "source": "expected_targets",
            },
            {
                "path": "models/smoke.py",
                "purpose": "Smoke model surface",
                "source": "expected_targets",
            },
        ],
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _non_dce_files(project_root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(project_root).as_posix(): path.read_bytes()
        for path in project_root.rglob("*")
        if path.is_file() and path.relative_to(project_root).parts[:1] != (".dce",)
    }


def test_stage0_to_stage9_lifecycle_smoke_fixture(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    project_root = _workspace_dir("dgce_stage0_to_stage9_lifecycle_smoke")

    section = _smoke_section()
    stage0 = assemble_stage0_input(section.model_dump())
    assert stage0.ok is True
    assert stage0.adapter == "software"
    assert stage0.stage_1_release_blocked is False
    assert stage0.package["source_input"]["section_id"] == "smoke-system"

    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    assert _non_dce_files(project_root) == {}

    client = TestClient(create_app())
    approve_response = client.post(
        "/v1/dgce/sections/smoke-system/approve",
        json={
            "workspace_path": str(project_root),
            "approved_by": "smoke-operator",
            "notes": "compact end-to-end smoke approval",
            "selected_mode": "create_only",
        },
    )
    assert approve_response.status_code == 200
    assert approve_response.json() == {
        "status": "ok",
        "section_id": "smoke-system",
        "approved": True,
    }

    prepare_response = client.post(
        "/v1/dgce/sections/smoke-system/prepare",
        json={"workspace_path": str(project_root)},
    )
    assert prepare_response.status_code == 200
    assert prepare_response.json() == {
        "status": "ok",
        "section_id": "smoke-system",
        "eligible": True,
        "checks": {
            "section_exists": True,
            "artifacts_valid": True,
            "approval_ready": True,
            "preflight_ready": True,
            "gate_ready": True,
        },
    }

    dce_root = project_root / ".dce"
    gate_path = dce_root / "execution" / "gate" / "smoke-system.execution_gate.json"
    gate_input_path = dce_root / "execution" / "gate" / "smoke-system.gate_input.json"
    prepared_plan_path = dce_root / "plans" / "smoke-system.prepared_plan.json"
    gate_payload = _read_json(gate_path)
    gate_input_payload = _read_json(gate_input_path)
    prepared_plan_payload = _read_json(prepared_plan_path)

    assert gate_payload["gate_status"] == "gate_pass"
    assert gate_payload["execution_blocked"] is False
    assert gate_payload["guardrail_decision"] == "ALLOW"
    assert gate_payload["gate_input_path"] == ".dce/execution/gate/smoke-system.gate_input.json"
    assert gate_payload["gate_input_fingerprint"] == gate_input_payload["gate_input_fingerprint"]
    assert dgce_decompose.verify_artifact_fingerprint(gate_input_path) is True
    assert dgce_decompose.verify_artifact_fingerprint(prepared_plan_path) is True
    assert dgce_decompose.verify_artifact_fingerprint(dce_root / "approvals" / "smoke-system.approval.json") is True
    assert dgce_decompose.verify_artifact_fingerprint(dce_root / "preflight" / "smoke-system.preflight.json") is True
    assert dgce_decompose.verify_review_artifact_fingerprint(dce_root / "reviews" / "smoke-system.review.md") is True
    assert prepared_plan_payload["binding"]["fingerprints"]["execution_gate"] == dgce_decompose.compute_json_file_fingerprint(gate_path)
    assert prepared_plan_payload["binding"]["fingerprints"]["approval"] == dgce_decompose.compute_json_file_fingerprint(
        dce_root / "approvals" / "smoke-system.approval.json"
    )

    execute_response = client.post(
        "/v1/dgce/sections/smoke-system/execute",
        json={"workspace_path": str(project_root)},
    )
    assert execute_response.status_code == 200, execute_response.text
    assert execute_response.json() == {
        "status": "ok",
        "section_id": "smoke-system",
        "executed": True,
        "artifacts_updated": True,
    }

    expected_written_files = {"api/smoke.py", "models/smoke.py"}
    assert set(_non_dce_files(project_root)) == expected_written_files

    execution_path = dce_root / "execution" / "smoke-system.execution.json"
    output_path = dce_root / "outputs" / "smoke-system.json"
    simulation_trigger_path = dce_root / "execution" / "simulation" / "smoke-system.simulation_trigger.json"
    simulation_path = dce_root / "execution" / "simulation" / "smoke-system.simulation.json"
    alignment_path = dce_root / "execution" / "alignment" / "smoke-system.alignment.json"
    assert execution_path.exists()
    assert output_path.exists()
    assert simulation_trigger_path.exists()
    assert not simulation_path.exists()
    assert dgce_decompose.verify_artifact_fingerprint(alignment_path) is True
    assert dgce_decompose.verify_artifact_fingerprint(simulation_trigger_path) is True

    execution_payload = _read_json(execution_path)
    output_payload = _read_json(output_path)
    simulation_trigger_payload = _read_json(simulation_trigger_path)
    assert execution_payload["execution_status"] == "execution_completed"
    assert execution_payload["execution_blocked"] is False
    assert execution_payload["simulation_triggered"] is False
    assert execution_payload["simulation_status"] == "simulation_skipped"
    assert sorted(entry["path"] for entry in execution_payload["written_files"]) == ["api/smoke.py", "models/smoke.py"]
    assert execution_payload["prepared_plan_audit_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
        execution_payload["prepared_plan_audit_manifest"]
    )
    assert execution_payload["prepared_plan_cross_link_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(
        execution_payload["prepared_plan_cross_link"]
    )
    assert sorted(artifact["path"] for artifact in output_payload["generated_artifacts"]) == ["api/smoke.py", "models/smoke.py"]
    assert output_payload["run_outcome_class"] == "success_create_only"
    assert simulation_trigger_payload["simulation_stage_status"] == "simulation_skipped"
    assert simulation_trigger_payload["simulation_triggered"] is False

    summary_response = client.get(
        "/v1/dgce/sections/smoke-system/summary",
        params={"workspace_path": str(project_root)},
    )
    overview_response = client.get(
        "/v1/dgce/sections/smoke-system/overview",
        params={"workspace_path": str(project_root)},
    )
    verify_response = client.get(
        "/v1/dgce/sections/smoke-system/verify",
        params={"workspace_path": str(project_root)},
    )
    workspace_summary = _read_json(dce_root / "workspace_summary.json")
    lifecycle_trace = _read_json(dce_root / "lifecycle_trace.json")
    review_index = _read_json(dce_root / "reviews" / "index.json")

    assert summary_response.status_code == 200
    assert overview_response.status_code == 200
    assert verify_response.status_code == 200
    assert verify_response.json()["verified"] is True
    assert summary_response.json()["execution_status"] == "execution_completed"
    assert summary_response.json()["written_file_paths"] == ["api/smoke.py", "models/smoke.py"]
    assert summary_response.json()["simulation"]["simulation_status"] == "skipped"
    assert summary_response.json()["simulation"]["simulation_triggered"] is False
    assert overview_response.json()["execution_status"] == "execution_completed"
    assert overview_response.json()["provenance_verified"] is True
    assert overview_response.json()["has_been_executed"] is True
    assert workspace_summary["sections"][0]["section_id"] == "smoke-system"
    assert workspace_summary["sections"][0]["latest_stage"] == "outputs"
    assert workspace_summary["sections"][0]["gate_status"] == "gate_pass"
    assert workspace_summary["sections"][0]["guardrail_decision"] == "ALLOW"
    assert workspace_summary["sections"][0]["execution_status"] == "execution_completed"
    assert workspace_summary["sections"][0]["section_summary"]["simulation"]["simulation_status"] == "skipped"
    assert workspace_summary["sections"][0]["section_summary"]["simulation"]["simulation_stage_applicable"] is True
    assert lifecycle_trace["lifecycle_order"] == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]
    assert "simulation" not in lifecycle_trace["lifecycle_order"]
    assert review_index["sections"][0]["output_path"] == ".dce/outputs/smoke-system.json"
