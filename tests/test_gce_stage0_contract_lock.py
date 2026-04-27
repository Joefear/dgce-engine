import copy
import inspect
import json
from pathlib import Path

from aether.dgce import (
    DGCESection,
    assemble_stage0_input,
    get_artifact_manifest,
    get_gce_stage0_artifact,
    persist_stage0_input,
    release_gce_stage0_input,
    resolve_gce_clarification_response,
    run_section_with_workspace,
)
import aether.dgce.context_assembly as context_assembly
import aether.dgce.decompose as dgce_decompose
import aether.dgce.gce_ingestion as gce_ingestion
import aether.dgce.read_api as dgce_read_api
from aether.dgce.gce_ingestion import validate_gce_ingestion_input
from aether.dgce.read_api_http import router as dgce_read_router
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


FIXTURE_DIR = Path("tests/fixtures/gce_stage0")


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


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


def _software_section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        requirements=["support mission templates", "track progression state"],
        constraints=["keep save format stable", "support mod extension points"],
    )


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


def _artifact_name(persisted) -> str:
    return Path(str(persisted.artifact_path)).name


def _assert_stage0_artifact_fields(payload: dict, *, input_path: str, reason_code: str | None) -> None:
    assert {
        "artifact_type",
        "contract_name",
        "contract_version",
        "input_path",
        "reason_code",
        "stage_1_release",
        "artifact_fingerprint",
    }.issubset(payload)
    assert payload["artifact_type"] == "stage0_input_package"
    assert payload["contract_name"] == "DGCEStage0InputPackage"
    assert payload["contract_version"] == "dgce.stage0.input_package.v1"
    assert payload["input_path"] == input_path
    assert payload["reason_code"] == reason_code
    assert isinstance(payload["stage_1_release"], dict)
    assert isinstance(payload["artifact_fingerprint"], str)


def _assert_read_model_fields(detail: dict, *, input_path: str, reason_code: str | None) -> None:
    assert {
        "artifact_type",
        "contract_name",
        "contract_version",
        "input_path",
        "reason_code",
        "stage_1_release",
        "artifact_fingerprint",
        "normalized_session_intent_summary",
        "clarification_request_summary",
    }.issubset(detail)
    assert detail["artifact_type"] == "stage0_input_package"
    assert detail["contract_name"] == "DGCEStage0InputPackage"
    assert detail["contract_version"] == "dgce.stage0.input_package.v1"
    assert detail["input_path"] == input_path
    assert detail["reason_code"] == reason_code
    assert isinstance(detail["stage_1_release"], dict)
    assert isinstance(detail["artifact_fingerprint"], str)


def _assert_normalized_summary_shape(summary: dict, *, source_input_path: str) -> None:
    assert set(summary) == {
        "contract_name",
        "contract_version",
        "source_input_path",
        "session_objective",
        "metadata",
        "section_count",
        "section_ids",
        "section_classifications",
    }
    assert summary["contract_name"] == "GCESessionIntent"
    assert summary["contract_version"] == "gce.session_intent.v1"
    assert summary["source_input_path"] == source_input_path
    assert isinstance(summary["metadata"], dict)
    assert isinstance(summary["section_ids"], list)
    assert isinstance(summary["section_classifications"], dict)


def _assert_clarification_summary_shape(summary: dict) -> None:
    assert set(summary) == {
        "artifact_type",
        "contract_name",
        "contract_version",
        "source_input_path",
        "reason_code",
        "stage_1_release_blocked",
        "question_count",
        "questions",
        "clarification_request_fingerprint",
    }
    assert summary["artifact_type"] == "clarification_request"
    assert summary["contract_name"] == "GCEClarificationRequest"
    assert summary["contract_version"] == "gce.clarification_request.v1"
    assert summary["reason_code"] == "unresolved_intent"
    assert summary["stage_1_release_blocked"] is True
    assert isinstance(summary["questions"], list)
    assert isinstance(summary["clarification_request_fingerprint"], str)


def _assert_valid_chain(payload: dict, *, workspace_name: str, input_path: str) -> None:
    validation = validate_gce_ingestion_input(payload)
    assert validation.ok is True
    assert validation.normalized_session_intent["source_input_path"] == input_path

    assembled = assemble_stage0_input(payload)
    assert assembled.ok is True
    assert assembled.adapter == "gce"
    assert assembled.stage_1_release_blocked is False
    assert assembled.package["normalized_session_intent"]["source_input_path"] == input_path

    project_root = _workspace_dir(workspace_name)
    persisted = persist_stage0_input(project_root, payload)
    assert persisted.persisted is True
    _assert_stage0_artifact_fields(persisted.artifact, input_path=input_path, reason_code=None)
    assert dgce_decompose.verify_artifact_fingerprint(project_root / persisted.artifact_path) is True

    release = release_gce_stage0_input(project_root / persisted.artifact_path)
    assert release.allowed is True
    assert release.result["stage_1_release"] == {"allowed": True, "blocked": False}
    assert release.result["reason_code"] is None
    assert release.result["normalized_session_intent"]["source_input_path"] == input_path

    detail = get_gce_stage0_artifact(project_root, _artifact_name(persisted))
    _assert_read_model_fields(detail, input_path=input_path, reason_code=None)
    assert detail["artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    _assert_normalized_summary_shape(detail["normalized_session_intent_summary"], source_input_path=input_path)
    assert detail["clarification_request_summary"] is None


def test_contract_lock_valid_formal_gdd_complete_stage0_chain():
    _assert_valid_chain(
        _fixture("valid_formal_gdd.json"),
        workspace_name="gce_contract_lock_formal_gdd",
        input_path="formal_gdd",
    )


def test_contract_lock_valid_structured_intent_complete_stage0_chain():
    _assert_valid_chain(
        _fixture("valid_structured_intent.json"),
        workspace_name="gce_contract_lock_structured_intent",
        input_path="structured_intent",
    )


def test_contract_lock_ambiguous_input_complete_blocked_chain():
    payload = _fixture("ambiguous_formal_gdd.json")

    validation = validate_gce_ingestion_input(payload)
    assert validation.ok is False
    assert validation.clarification_request["artifact_type"] == "clarification_request"

    assembled = assemble_stage0_input(payload)
    assert assembled.ok is False
    assert assembled.stage_1_release_blocked is True
    assert assembled.package["reason_code"] == "clarification_required"
    assert assembled.package["clarification_request"] == validation.clarification_request

    project_root = _workspace_dir("gce_contract_lock_ambiguous")
    persisted = persist_stage0_input(project_root, payload)
    assert persisted.persisted is True
    _assert_stage0_artifact_fields(persisted.artifact, input_path="formal_gdd", reason_code="clarification_required")

    release = release_gce_stage0_input(project_root / persisted.artifact_path)
    assert release.allowed is False
    assert release.result["reason_code"] == "clarification_required"
    assert release.result["stage_1_release"] == {"allowed": False, "blocked": True}
    assert release.result["clarification_request"] == persisted.artifact["clarification_request"]

    detail = get_gce_stage0_artifact(project_root, _artifact_name(persisted))
    _assert_read_model_fields(detail, input_path="formal_gdd", reason_code="clarification_required")
    assert detail["stage_1_release"] == {"blocked": True, "reason_code": "clarification_required"}
    assert detail["normalized_session_intent_summary"] is None
    _assert_clarification_summary_shape(detail["clarification_request_summary"])


def test_contract_lock_clarification_response_complete_resolution_chain():
    blocked = assemble_stage0_input(_fixture("ambiguous_formal_gdd.json")).package
    response = _fixture("valid_clarification_response.json")

    resolved = resolve_gce_clarification_response(blocked, response)
    assert resolved.ok is True
    assert resolved.blocked is False
    assert resolved.resolved_input["input_path"] == "structured_intent"

    validation = validate_gce_ingestion_input(resolved.resolved_input)
    assert validation.ok is True
    assert validation.normalized_session_intent["source_input_path"] == "structured_intent"

    project_root = _workspace_dir("gce_contract_lock_clarification_resolution")
    persisted = persist_stage0_input(project_root, resolved.resolved_input)
    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert persisted.persisted is True
    _assert_stage0_artifact_fields(persisted.artifact, input_path="structured_intent", reason_code=None)
    assert release.allowed is True
    assert release.result["normalized_session_intent"]["source_input_path"] == "structured_intent"


def test_contract_lock_raw_natural_language_string_remains_blocked_without_persistence_or_release():
    project_root = _workspace_dir("gce_contract_lock_raw_language")
    result = persist_stage0_input(project_root, "Generate this game from my idea.")

    assert result.persisted is False
    assert result.artifact_path is None
    assert result.artifact is None
    assert result.boundary_result.ok is False
    assert result.boundary_result.stage_1_release_blocked is True
    assert result.boundary_result.package["stage_1_release"] == {
        "blocked": True,
        "reason_code": "unsupported_input",
    }
    assert not (project_root / ".dce").exists()

    release = release_gce_stage0_input(result.boundary_result.package)
    assert release.allowed is False
    assert release.result["reason_code"] == "package_malformed"


def test_contract_lock_invalid_partial_structured_intent_remains_blocked_and_cannot_release():
    payload = _fixture("invalid_partial_structured_intent.json")

    validation = validate_gce_ingestion_input(payload)
    assert validation.ok is False
    assert validation.clarification_request is None

    assembled = assemble_stage0_input(payload)
    assert assembled.ok is False
    assert assembled.package["reason_code"] == "validation_failed"
    assert assembled.package["normalized_session_intent"] is None

    project_root = _workspace_dir("gce_contract_lock_invalid_partial")
    persisted = persist_stage0_input(project_root, payload)
    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert persisted.persisted is True
    _assert_stage0_artifact_fields(persisted.artifact, input_path="structured_intent", reason_code="validation_failed")
    assert release.allowed is False
    assert release.result["reason_code"] == "validation_failed"
    assert "normalized_session_intent" not in release.result


def test_contract_lock_gce_stage0_read_api_exposes_only_get_routes():
    gce_routes = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/gce/stage0-artifacts")
    }

    assert gce_routes == {
        "/v1/dgce/gce/stage0-artifacts": {"GET"},
        "/v1/dgce/gce/stage0-artifacts/{artifact_name}": {"GET"},
    }


def test_contract_lock_stage75_lifecycle_order_is_unchanged():
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


def test_contract_lock_gce_stage0_modules_do_not_import_code_graph_or_dcg_facts():
    module_sources = [
        inspect.getsource(gce_ingestion),
        inspect.getsource(context_assembly),
        inspect.getsource(dgce_read_api),
    ]
    combined_source = "\n".join(module_sources).lower()

    assert "code_graph" not in combined_source
    assert "dcg.facts" not in combined_source
    assert "dcg-facts" not in combined_source


def test_contract_lock_software_ingestion_and_read_behavior_remain_unchanged(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    project_root = _workspace_dir("gce_contract_lock_software_unchanged")
    section = _software_section()

    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")

    input_payload = json.loads((project_root / ".dce" / "input" / "mission-board.json").read_text(encoding="utf-8"))
    manifest_payload = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    assert input_payload == section.model_dump()
    assert get_artifact_manifest(project_root) == manifest_payload


def test_contract_lock_key_artifact_and_read_model_fields_remain_stable():
    project_root = _workspace_dir("gce_contract_lock_stable_fields")
    formal = persist_stage0_input(project_root, _fixture("valid_formal_gdd.json"))
    ambiguous = persist_stage0_input(project_root, _fixture("ambiguous_formal_gdd.json"))

    formal_detail = get_gce_stage0_artifact(project_root, _artifact_name(formal))
    ambiguous_detail = get_gce_stage0_artifact(project_root, _artifact_name(ambiguous))

    _assert_stage0_artifact_fields(formal.artifact, input_path="formal_gdd", reason_code=None)
    _assert_read_model_fields(formal_detail, input_path="formal_gdd", reason_code=None)
    _assert_normalized_summary_shape(formal_detail["normalized_session_intent_summary"], source_input_path="formal_gdd")
    assert formal_detail["clarification_request_summary"] is None

    _assert_stage0_artifact_fields(ambiguous.artifact, input_path="formal_gdd", reason_code="clarification_required")
    _assert_read_model_fields(ambiguous_detail, input_path="formal_gdd", reason_code="clarification_required")
    assert ambiguous_detail["normalized_session_intent_summary"] is None
    _assert_clarification_summary_shape(ambiguous_detail["clarification_request_summary"])


def test_contract_lock_valid_chain_is_deterministic_on_repeated_runs():
    payload = _fixture("valid_formal_gdd.json")
    first_root = _workspace_dir("gce_contract_lock_deterministic_first")
    second_root = _workspace_dir("gce_contract_lock_deterministic_second")

    first = persist_stage0_input(first_root, payload)
    second = persist_stage0_input(second_root, copy.deepcopy(payload))
    first_release = release_gce_stage0_input(first_root / first.artifact_path)
    second_release = release_gce_stage0_input(second_root / second.artifact_path)
    first_detail = get_gce_stage0_artifact(first_root, _artifact_name(first))
    second_detail = get_gce_stage0_artifact(second_root, _artifact_name(second))

    assert first.artifact == second.artifact
    assert first_release.result == second_release.result
    assert first_detail == second_detail
