import json
from pathlib import Path

from aether.dgce import (
    assemble_stage0_input,
    compute_gce_clarification_request_fingerprint,
    persist_stage0_input,
    release_gce_stage0_input,
    resolve_gce_clarification_response,
)
from aether.dgce.gce_ingestion import validate_gce_ingestion_input


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


def test_valid_gce_fixture_inputs_pass_validation():
    formal = validate_gce_ingestion_input(_fixture("valid_formal_gdd.json"))
    structured = validate_gce_ingestion_input(_fixture("valid_structured_intent.json"))

    assert formal.ok is True
    assert formal.normalized_session_intent["source_input_path"] == "formal_gdd"
    assert structured.ok is True
    assert structured.normalized_session_intent["source_input_path"] == "structured_intent"


def test_ambiguous_fixture_blocks_with_clarification_request():
    result = assemble_stage0_input(_fixture("ambiguous_formal_gdd.json"))
    response = _fixture("valid_clarification_response.json")

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.package["reason_code"] == "clarification_required"
    assert result.package["clarification_request"]["artifact_type"] == "clarification_request"
    assert (
        compute_gce_clarification_request_fingerprint(result.package["clarification_request"])
        == response["source_clarification_request_fingerprint"]
    )


def test_clarification_fixture_resolves_and_releases_through_stage0_flow():
    source_package = assemble_stage0_input(_fixture("ambiguous_formal_gdd.json")).package
    response = _fixture("valid_clarification_response.json")

    resolved = resolve_gce_clarification_response(source_package, response)
    assert resolved.ok is True
    assert resolved.resolved_input["input_path"] == "structured_intent"

    stage0 = assemble_stage0_input(resolved.resolved_input)
    assert stage0.ok is True

    project_root = _workspace_dir("gce_stage0_fixture_release")
    persisted = persist_stage0_input(project_root, resolved.resolved_input)
    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert persisted.persisted is True
    assert release.allowed is True
    assert release.result["normalized_session_intent"]["source_input_path"] == "structured_intent"


def test_invalid_partial_structured_intent_fixture_fails_closed():
    result = assemble_stage0_input(_fixture("invalid_partial_structured_intent.json"))

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.package["reason_code"] == "validation_failed"
    assert result.package["normalized_session_intent"] is None
