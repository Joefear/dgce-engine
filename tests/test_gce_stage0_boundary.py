import inspect
import json
from pathlib import Path

from aether.dgce import (
    DGCESection,
    assemble_stage0_input,
    compute_gce_clarification_request_fingerprint,
    persist_stage0_input,
    release_gce_stage0_input,
    resolve_gce_clarification_response,
    run_section_with_workspace,
)
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


def _ambiguous_gce_input() -> dict:
    payload = _formal_gdd_input()
    payload["ambiguities"] = [
        {
            "field_path": "document.sections[0].content.scope",
            "question": "Which generation scope is authoritative?",
            "blocking": True,
        }
    ]
    return payload


def _clarification_package() -> dict:
    return persist_stage0_input(_workspace_dir("gce_clarification_source"), _ambiguous_gce_input()).artifact


def _clarification_response(package: dict) -> dict:
    return {
        "contract_name": "GCEClarificationResponse",
        "contract_version": "gce.clarification_response.v1",
        "source_clarification_request_fingerprint": compute_gce_clarification_request_fingerprint(
            package["clarification_request"]
        ),
        "operator_response": {
            "operator_id": "operator-1",
            "responded_at": "2026-04-27T00:00:00Z",
        },
        "resolved_fields": {
            "session_objective": "Generate a bounded mission board system from the resolved design intent.",
            "sections": _sections(),
        },
    }


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
    payload = _ambiguous_gce_input()

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
    payload = _ambiguous_gce_input()

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


def test_valid_persisted_gce_package_releases_to_stage1():
    project_root = _workspace_dir("gce_stage0_release_valid")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / persisted.artifact_path

    release = release_gce_stage0_input(artifact_path)

    assert release.allowed is True
    assert release.result["artifact_type"] == "stage0_release_result"
    assert release.result["adapter"] == "gce"
    assert release.result["contract_name"] == "GCEStage0ReleaseResult"
    assert release.result["contract_version"] == "gce.stage0.release_result.v1"
    assert release.result["input_path"] == "formal_gdd"
    assert release.result["reason_code"] is None
    assert release.result["source_artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert release.result["stage_1_release"] == {
        "allowed": True,
        "blocked": False,
    }
    assert release.result["normalized_session_intent"]["contract_name"] == "GCESessionIntent"
    assert "clarification_request" not in release.result


def test_invalid_persisted_gce_package_is_blocked_from_stage1():
    project_root = _workspace_dir("gce_stage0_release_invalid")
    payload = _structured_intent_input()
    del payload["metadata"]["owner"]
    persisted = persist_stage0_input(project_root, payload)

    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert release.allowed is False
    assert release.result["input_path"] == "structured_intent"
    assert release.result["reason_code"] == "validation_failed"
    assert release.result["source_artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }
    assert "normalized_session_intent" not in release.result
    assert "clarification_request" not in release.result


def test_clarification_persisted_gce_package_is_blocked_and_preserves_request():
    project_root = _workspace_dir("gce_stage0_release_clarification")
    payload = _ambiguous_gce_input()
    persisted = persist_stage0_input(project_root, payload)

    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert release.allowed is False
    assert release.result["input_path"] == "formal_gdd"
    assert release.result["reason_code"] == "clarification_required"
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }
    assert release.result["clarification_request"] == persisted.artifact["clarification_request"]
    assert "normalized_session_intent" not in release.result


def test_malformed_gce_stage0_package_is_blocked_fail_closed():
    release = release_gce_stage0_input(
        {
            "artifact_type": "stage0_input_package",
            "contract_name": "WrongContract",
            "contract_version": "dgce.stage0.input_package.v1",
            "adapter": "gce",
            "input_path": "formal_gdd",
            "stage_1_release": {"blocked": False, "reason_code": None},
            "validation_report": {"status": "PASS"},
            "normalized_session_intent": {"contract_name": "GCESessionIntent"},
            "clarification_request": None,
        }
    )

    assert release.allowed is False
    assert release.result["reason_code"] == "package_malformed"
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }
    assert "normalized_session_intent" not in release.result
    assert "clarification_request" not in release.result


def test_gce_stage0_package_missing_normalized_session_intent_is_blocked_fail_closed():
    package = assemble_stage0_input(_formal_gdd_input()).package
    package["normalized_session_intent"] = None

    release = release_gce_stage0_input(package)

    assert release.allowed is False
    assert release.result["reason_code"] == "normalized_session_intent_missing"
    assert release.result["input_path"] == "formal_gdd"
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }
    assert "normalized_session_intent" not in release.result


def test_persisted_gce_stage0_artifact_with_invalid_fingerprint_is_blocked():
    project_root = _workspace_dir("gce_stage0_release_invalid_fingerprint")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / persisted.artifact_path
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["input_path"] = "structured_intent"
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    release = release_gce_stage0_input(artifact_path)

    assert release.allowed is False
    assert release.result["reason_code"] == "artifact_fingerprint_invalid"
    assert release.result["source_artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }


def test_persisted_gce_stage0_artifact_missing_fingerprint_is_blocked():
    project_root = _workspace_dir("gce_stage0_release_missing_fingerprint")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / persisted.artifact_path
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    del payload["artifact_fingerprint"]
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    release = release_gce_stage0_input(artifact_path)

    assert release.allowed is False
    assert release.result["reason_code"] == "artifact_fingerprint_missing"
    assert release.result["source_artifact_fingerprint"] is None
    assert release.result["stage_1_release"] == {
        "allowed": False,
        "blocked": True,
    }


def test_gce_stage0_release_result_is_deterministic_for_repeated_reads():
    project_root = _workspace_dir("gce_stage0_release_repeat")
    persisted = persist_stage0_input(project_root, _formal_gdd_input())
    artifact_path = project_root / persisted.artifact_path

    first = release_gce_stage0_input(artifact_path)
    second = release_gce_stage0_input(artifact_path)

    assert first.allowed is True
    assert second.allowed is True
    assert first.result == second.result


def test_valid_clarification_response_resolves_to_contract_valid_structured_intent():
    package = _clarification_package()
    response = _clarification_response(package)

    result = resolve_gce_clarification_response(package, response)

    assert result.ok is True
    assert result.blocked is False
    assert result.reason_code is None
    assert result.errors == []
    assert result.resolved_input == {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": package["clarification_request"]["metadata"],
        "intent": response["resolved_fields"],
        "ambiguities": [],
    }


def test_resolved_structured_intent_passes_stage0_boundary():
    package = _clarification_package()
    resolved = resolve_gce_clarification_response(package, _clarification_response(package))

    stage0 = assemble_stage0_input(resolved.resolved_input)

    assert stage0.ok is True
    assert stage0.adapter == "gce"
    assert stage0.package["stage_1_release"] == {
        "blocked": False,
        "reason_code": None,
    }
    assert stage0.package["normalized_session_intent"]["source_input_path"] == "structured_intent"


def test_resolved_structured_intent_can_persist_and_release_to_stage1():
    package = _clarification_package()
    resolved = resolve_gce_clarification_response(package, _clarification_response(package))
    project_root = _workspace_dir("gce_clarification_resolved_release")

    persisted = persist_stage0_input(project_root, resolved.resolved_input)
    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert persisted.boundary_result.ok is True
    assert release.allowed is True
    assert release.result["input_path"] == "structured_intent"
    assert release.result["normalized_session_intent"]["contract_name"] == "GCESessionIntent"


def test_malformed_clarification_response_fails_closed():
    package = _clarification_package()
    response = _clarification_response(package)
    del response["operator_response"]["operator_id"]

    result = resolve_gce_clarification_response(package, response)

    assert result.ok is False
    assert result.blocked is True
    assert result.reason_code == "clarification_response_malformed"
    assert result.resolved_input is None


def test_mismatched_clarification_source_fingerprint_fails_closed():
    package = _clarification_package()
    response = _clarification_response(package)
    response["source_clarification_request_fingerprint"] = "0" * 64

    result = resolve_gce_clarification_response(package, response)

    assert result.ok is False
    assert result.blocked is True
    assert result.reason_code == "clarification_source_mismatch"
    assert result.resolved_input is None


def test_clarification_response_with_unsupported_execution_field_fails_closed():
    package = _clarification_package()
    response = _clarification_response(package)
    response["execution_payload"] = "release this to Stage 1"

    result = resolve_gce_clarification_response(package, response)

    assert result.ok is False
    assert result.blocked is True
    assert result.reason_code == "unsupported_fields"
    assert result.resolved_input is None


def test_source_package_not_blocked_for_clarification_fails_closed():
    source = assemble_stage0_input(_formal_gdd_input()).package
    response = _clarification_response(_clarification_package())

    result = resolve_gce_clarification_response(source, response)

    assert result.ok is False
    assert result.blocked is True
    assert result.reason_code == "source_not_blocked_for_clarification"
    assert result.resolved_input is None


def test_clarification_resolution_is_deterministic():
    package = _clarification_package()
    response = _clarification_response(package)

    first = resolve_gce_clarification_response(package, response)
    second = resolve_gce_clarification_response(package, response)

    assert first == second


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
