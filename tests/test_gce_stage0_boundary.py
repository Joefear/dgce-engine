import inspect
import json
from pathlib import Path

from aether.dgce import DGCESection, assemble_stage0_input, run_section_with_workspace
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


def test_existing_software_workspace_ingestion_still_persists_section_input(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    project_root = _workspace_dir("gce_stage0_boundary_software_unchanged")

    run_section_with_workspace(_software_section(), project_root, incremental_mode="incremental_v2_2")

    input_payload = json.loads((project_root / ".dce" / "input" / "mission-board.json").read_text(encoding="utf-8"))
    assert input_payload == _software_section().model_dump()


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
