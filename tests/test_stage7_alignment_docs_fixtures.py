import json
from pathlib import Path

from aether.dgce.read_api import get_stage7_alignment_read_model
from aether.dgce.read_api_http import router as dgce_read_router
from packages.dgce_contracts.alignment_artifacts import (
    build_alignment_record_read_model_v1,
    persist_alignment_record_v1,
)
from packages.dgce_contracts.alignment_builder import build_alignment_record_v1


DOC_PATH = Path("docs/stage7_alignment.md")
FIXTURE_DIR = Path("tests/fixtures/stage7_alignment_read_model")
READ_MODEL_FIELDS = {
    "section_id",
    "alignment_id",
    "alignment_result",
    "drift_detected",
    "execution_permitted",
    "blocking_issues_count",
    "informational_issues_count",
    "primary_reason",
    "drift_codes",
    "evidence_sources",
    "enrichment_status",
    "code_graph_used",
    "resolver_used",
}
FORBIDDEN_READ_MODEL_FIELDS = {
    "input_fingerprint",
    "approval_fingerprint",
    "preview_fingerprint",
    "timestamp",
    "drift_items",
    "evidence",
}
TIMESTAMP = "2026-05-02T22:00:00Z"
INPUT_FP = "1111111111111111111111111111111111111111111111111111111111111111"
APPROVAL_FP = "2222222222222222222222222222222222222222222222222222222222222222"
PREVIEW_FP = "3333333333333333333333333333333333333333333333333333333333333333"


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


def _target(target: str, *, reference: str | None = None, structure: dict | None = None) -> dict:
    payload = {
        "target": target,
        "reference": reference or f"artifact://{target}",
    }
    if structure is not None:
        payload["structure"] = structure
    return payload


def _alignment_record(*, alignment_id: str, misaligned: bool = False) -> dict:
    approved = [
        _target("api/mission.py", structure={"kind": "api", "version": 1}),
        _target("models/mission.py", structure={"kind": "model", "version": 1}),
    ]
    if misaligned:
        preview = [
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("debug/extra.py", structure={"kind": "debug", "version": 1}),
        ]
        observed = [
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
        ]
    else:
        preview = approved
        observed = approved
    return build_alignment_record_v1(
        alignment_id=alignment_id,
        timestamp=TIMESTAMP,
        input_fingerprint=INPUT_FP,
        approval_fingerprint=APPROVAL_FP,
        preview_fingerprint=PREVIEW_FP,
        approved_design_expectations=approved,
        preview_proposed_targets=preview,
        current_observed_targets=observed,
    )


def test_stage7_alignment_document_covers_locked_surfaces_and_boundaries():
    text = DOC_PATH.read_text(encoding="utf-8")

    for required in (
        ".dce/execution/alignment/{section_id}.alignment.json",
        "packages/dgce-contracts/schemas/alignment/alignment_record.v1.schema.json",
        "build_alignment_record_v1",
        "persist_alignment_record_v1",
        "GET /v1/dgce/stage7/alignment/{section_id}",
        "DGCEClient.get_stage7_alignment_read_model",
        "missing_expected_artifact",
        "unexpected_artifact",
        "structure_mismatch",
        "symbol_resolution_conflict",
        "insertion_point_invalid",
        "design_contract_violation",
        "dependency_mismatch",
        "adapter_constraint_violation",
        "does not perform policy evaluation",
        "simulation validation",
        "Blueprint mutation",
        "Unreal project writes",
        "lifecycle advancement",
    ):
        assert required in text


def test_aligned_read_model_fixture_matches_builder_projection():
    record = _alignment_record(alignment_id="alignment.docs.fixture.aligned")

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("aligned_read_model.json")


def test_misaligned_read_model_fixture_matches_builder_projection():
    record = _alignment_record(alignment_id="alignment.docs.fixture.misaligned", misaligned=True)

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("misaligned_read_model.json")


def test_missing_read_error_fixture_matches_read_api_projection():
    workspace_path = _workspace_dir("stage7_alignment_docs_missing_fixture")
    (workspace_path / ".dce").mkdir(parents=True)

    assert get_stage7_alignment_read_model(workspace_path, "mission-board") == _fixture("missing_read_error.json")


def test_read_model_fixture_field_list_is_exactly_bounded_surface():
    for fixture_name in ("aligned_read_model.json", "misaligned_read_model.json"):
        fixture = _fixture(fixture_name)

        assert set(fixture) == READ_MODEL_FIELDS
        for forbidden in FORBIDDEN_READ_MODEL_FIELDS:
            assert forbidden not in fixture


def test_read_model_fixtures_roundtrip_through_persistence_read_surface():
    workspace_path = _workspace_dir("stage7_alignment_docs_fixture_roundtrip")
    aligned = _alignment_record(alignment_id="alignment.docs.fixture.aligned")
    misaligned = _alignment_record(alignment_id="alignment.docs.fixture.misaligned", misaligned=True)

    persist_alignment_record_v1(aligned, workspace_path=workspace_path, section_id="mission-board")
    assert get_stage7_alignment_read_model(workspace_path, "mission-board") == _fixture("aligned_read_model.json")

    persist_alignment_record_v1(misaligned, workspace_path=workspace_path, section_id="mission-board")
    assert get_stage7_alignment_read_model(workspace_path, "mission-board") == _fixture("misaligned_read_model.json")


def test_stage7_alignment_api_route_remains_get_only():
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/stage7/alignment")
    }

    assert route_methods == {"/v1/dgce/stage7/alignment/{section_id}": {"GET"}}
