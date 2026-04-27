import inspect
import json
from pathlib import Path

from aether.dgce import DGCESection, assemble_stage0_input, persist_stage0_input, run_section_with_workspace
import aether.dgce.context_assembly as context_assembly
import aether.dgce.decompose as dgce_decompose
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


def _metadata() -> dict:
    return {
        "project_id": "frontier-colony",
        "project_name": "Frontier Colony",
        "owner": "Design Authority",
        "source_id": "gdd-frontier-colony-v1",
        "created_at": "2026-04-26T00:00:00Z",
        "updated_at": "2026-04-26T00:00:00Z",
    }


def _sections() -> list[dict]:
    return [
        {
            "section_id": "project_identity",
            "title": "Project Identity",
            "classification": "durable",
            "authorship": "human",
            "required": True,
            "content": {
                "purpose": "Define the colony simulation identity and generation bounds.",
            },
        },
        {
            "section_id": "current_state",
            "title": "Current State",
            "classification": "volatile",
            "authorship": "injected",
            "required": False,
            "content": {
                "placeholder": "registered_stage_0_source",
            },
        },
    ]


def _formal_gdd_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "formal_gdd",
        "metadata": _metadata(),
        "document": {
            "session_objective": "Generate a bounded mission board system from the approved GDD.",
            "sections": _sections(),
        },
        "ambiguities": [],
    }


def _structured_intent_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": _metadata(),
        "intent": {
            "session_objective": "Generate a bounded mission board system from the approved GDD.",
            "sections": _sections(),
        },
        "ambiguities": [],
    }


def _software_section() -> DGCESection:
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


def test_gce_valid_formal_gdd_passes_stage0_boundary_to_normalized_package():
    result = assemble_stage0_input(_formal_gdd_input())

    assert result.ok is True
    assert result.adapter == "gce"
    assert result.stage_1_release_blocked is False
    assert result.package["artifact_type"] == "stage0_input_package"
    assert result.package["input_path"] == "formal_gdd"
    assert result.package["reason_code"] is None
    assert result.package["stage_1_release"] == {
        "blocked": False,
        "reason_code": None,
    }
    assert result.package["validation_report"]["status"] == "PASS"
    normalized = result.package["normalized_session_intent"]
    assert normalized["contract_name"] == "GCESessionIntent"
    assert normalized["source_input_path"] == "formal_gdd"
    assert normalized["section_classifications"]["current_state"] == {
        "classification": "volatile",
        "authorship": "injected",
        "required": False,
    }


def test_gce_valid_structured_intent_passes_stage0_boundary_to_normalized_package():
    result = assemble_stage0_input(_structured_intent_input())

    assert result.ok is True
    assert result.adapter == "gce"
    assert result.stage_1_release_blocked is False
    assert result.package["normalized_session_intent"]["source_input_path"] == "structured_intent"
    assert result.package["clarification_request"] is None


def test_invalid_gce_input_is_blocked_before_stage1():
    payload = _structured_intent_input()
    del payload["metadata"]["owner"]

    result = assemble_stage0_input(payload)

    assert result.ok is False
    assert result.adapter == "gce"
    assert result.stage_1_release_blocked is True
    assert result.package["stage_1_release"] == {
        "blocked": True,
        "reason_code": "validation_failed",
    }
    assert result.package["normalized_session_intent"] is None
    assert result.package["clarification_request"] is None
    assert result.package["validation_report"]["status"] == "FAIL"
    assert result.package["input_path"] == "structured_intent"
    assert result.package["reason_code"] == "validation_failed"
    assert "missing required fields: owner" in result.errors[0]["condition"]


def test_clarification_request_blocks_stage1_release():
    payload = _formal_gdd_input()
    payload["ambiguities"] = [
        {
            "field_path": "document.sections[0].content.scope",
            "question": "Which generation scope is authoritative?",
            "blocking": True,
        }
    ]

    result = assemble_stage0_input(payload)

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.package["stage_1_release"] == {
        "blocked": True,
        "reason_code": "clarification_required",
    }
    assert result.package["input_path"] == "formal_gdd"
    assert result.package["reason_code"] == "clarification_required"
    assert result.package["normalized_session_intent"] is None
    assert result.package["clarification_request"]["artifact_type"] == "clarification_request"
    assert result.package["clarification_request"]["stage_1_release_blocked"] is True


def test_non_gce_software_input_is_passed_through_without_contract_rewrite():
    software_payload = _software_section().model_dump()

    result = assemble_stage0_input(software_payload)

    assert result.ok is True
    assert result.adapter == "software"
    assert result.stage_1_release_blocked is False
    assert result.package["validation_report"]["status"] == "PASS_THROUGH"
    assert result.package["source_input"] == software_payload
    assert result.package["normalized_session_intent"] is None


def test_valid_gce_stage0_package_is_persisted_deterministically():
    project_root = _workspace_dir("gce_stage0_persist_valid")
    payload = _formal_gdd_input()

    first = persist_stage0_input(project_root, payload)
    artifact_path = project_root / first.artifact_path
    first_bytes = artifact_path.read_bytes()
    second = persist_stage0_input(project_root, payload)

    assert first.persisted is True
    assert first.artifact_path == ".dce/input/gce/gdd-frontier-colony-v1.formal-gdd.stage0.json"
    assert second.artifact_path == first.artifact_path
    assert artifact_path.read_bytes() == first_bytes
    assert second.artifact == first.artifact
    assert dgce_decompose.verify_artifact_fingerprint(artifact_path) is True
    persisted = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert persisted["artifact_type"] == "stage0_input_package"
    assert persisted["contract_name"] == "DGCEStage0InputPackage"
    assert persisted["contract_version"] == "dgce.stage0.input_package.v1"
    assert persisted["input_path"] == "formal_gdd"
    assert persisted["stage_1_release"]["blocked"] is False
    assert persisted["reason_code"] is None
    assert persisted["normalized_session_intent"]["contract_name"] == "GCESessionIntent"
    assert persisted["artifact_fingerprint"] == dgce_decompose.compute_json_payload_fingerprint(persisted)


def test_invalid_gce_stage0_package_is_persisted_and_blocks_stage1():
    project_root = _workspace_dir("gce_stage0_persist_invalid")
    payload = _structured_intent_input()
    del payload["metadata"]["owner"]

    result = persist_stage0_input(project_root, payload)

    assert result.persisted is True
    assert result.boundary_result.ok is False
    assert result.boundary_result.stage_1_release_blocked is True
    artifact_path = project_root / result.artifact_path
    persisted = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert persisted["input_path"] == "structured_intent"
    assert persisted["stage_1_release"] == {
        "blocked": True,
        "reason_code": "validation_failed",
    }
    assert persisted["reason_code"] == "validation_failed"
    assert persisted["normalized_session_intent"] is None
    assert persisted["clarification_request"] is None
    assert "missing required fields: owner" in persisted["validation_report"]["errors"][0]["condition"]
    assert dgce_decompose.verify_artifact_fingerprint(artifact_path) is True


def test_clarification_request_stage0_package_is_persisted_and_blocks_stage1():
    project_root = _workspace_dir("gce_stage0_persist_clarification")
    payload = _formal_gdd_input()
    payload["ambiguities"] = [
        {
            "field_path": "document.sections[0].content.scope",
            "question": "Which generation scope is authoritative?",
            "blocking": True,
        }
    ]

    result = persist_stage0_input(project_root, payload)

    assert result.persisted is True
    artifact_path = project_root / result.artifact_path
    persisted = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert persisted["input_path"] == "formal_gdd"
    assert persisted["stage_1_release"] == {
        "blocked": True,
        "reason_code": "clarification_required",
    }
    assert persisted["reason_code"] == "clarification_required"
    assert persisted["normalized_session_intent"] is None
    assert persisted["clarification_request"]["artifact_type"] == "clarification_request"
    assert persisted["clarification_request"]["stage_1_release_blocked"] is True
    assert dgce_decompose.verify_artifact_fingerprint(artifact_path) is True


def test_raw_natural_language_stage0_input_is_blocked_without_persistence():
    project_root = _workspace_dir("gce_stage0_raw_language_blocked")

    result = persist_stage0_input(project_root, "Build the mission board from this idea.")

    assert result.persisted is False
    assert result.boundary_result.ok is False
    assert result.boundary_result.stage_1_release_blocked is True
    assert result.boundary_result.package["stage_1_release"] == {
        "blocked": True,
        "reason_code": "unsupported_input",
    }
    assert not (project_root / ".dce").exists()


def test_existing_software_workspace_ingestion_still_persists_section_input(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    project_root = _workspace_dir("gce_stage0_boundary_software_unchanged")

    run_section_with_workspace(_software_section(), project_root, incremental_mode="incremental_v2_2")

    input_payload = json.loads((project_root / ".dce" / "input" / "mission-board.json").read_text(encoding="utf-8"))
    assert input_payload == _software_section().model_dump()


def test_stage0_persistence_leaves_non_gce_software_persistence_unchanged():
    project_root = _workspace_dir("gce_stage0_persist_software_unchanged")
    software_payload = _software_section().model_dump()

    result = persist_stage0_input(project_root, software_payload)

    assert result.persisted is False
    assert result.artifact_path is None
    assert result.artifact is None
    assert result.boundary_result.adapter == "software"
    assert not (project_root / ".dce").exists()


def test_stage0_boundary_does_not_change_stage75_lifecycle_order():
    assert dgce_decompose.DGCE_LIFECYCLE_ORDER == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]


def test_stage0_boundary_introduces_no_code_graph_dependency():
    source = inspect.getsource(context_assembly).lower()

    assert "code_graph" not in source
    assert "dcg" not in source
