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
    "raw_symbols",
    "symbol_table",
    "resolver_payload",
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


def _resolver_context(case_name: str | None) -> dict | None:
    if case_name is None:
        return None
    if case_name == "exact_match":
        return {
            "resolved_symbols": [
                {
                    "symbol_name": "BP_MissionBoard",
                    "symbol_kind": "BlueprintClass",
                    "source_path": "Content/BP_MissionBoard.uasset",
                    "resolution_method": "path_metadata",
                    "confidence": "exact_path_match",
                }
            ],
            "unresolved_symbols": [],
            "resolution_status": "resolved",
        }
    if case_name == "candidate_match":
        return {
            "resolved_symbols": [
                {
                    "symbol_name": "BP_MissionBoard",
                    "symbol_kind": "BlueprintClass",
                    "source_path": "Content/BP_MissionBoard.uasset",
                    "resolution_method": "path_metadata",
                    "confidence": "candidate_match",
                }
            ],
            "unresolved_symbols": [],
            "resolution_status": "resolved",
        }
    if case_name == "unresolved_symbol":
        return {
            "resolved_symbols": [],
            "unresolved_symbols": [
                {
                    "symbol_name": "MissingMissionBoard",
                    "symbol_kind": "BlueprintClass",
                    "source_path": None,
                    "resolution_method": "path_metadata",
                    "confidence": "unresolved",
                }
            ],
            "resolution_status": "unresolved",
        }
    raise ValueError(f"unsupported resolver fixture case: {case_name}")


def _alignment_record(*, alignment_id: str, misaligned: bool = False, resolver_case: str | None = None) -> dict:
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
        resolver_context=_resolver_context(resolver_case),
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
        "resolver is optional",
        "resolver_used=false",
        "enrichment_status=not_used",
        "invalid, or incomplete resolver output is ignored",
        "Exact resolved symbols can add bounded resolver evidence",
        "Candidate matches create informational `symbol_resolution_conflict` drift",
        "Unresolved symbols create blocking `symbol_resolution_conflict` drift",
        "block before Stage 7.5 and Stage 8",
        "Code Graph Enrichment",
        "Code Graph enrichment is optional bounded non-authoritative enrichment.",
        "Code Graph absence, unavailability, malformed facts, invalid facts",
        "source=code_graph",
        "raw `dcg.facts.v1` payloads",
        "does not modify dcg.facts.v1",
        "does not store raw facts",
        "does not store full graphs",
        "does not store file contents",
        "does not store policy outcomes",
        "does not perform policy evaluation",
        "simulation validation",
        "Unreal project mutation",
        "Blueprint mutation",
        "raw symbol tables",
        "raw resolver payloads",
        "does not perform policy evaluation",
        "simulation validation",
        "Blueprint mutation",
        "Unreal project writes",
        "lifecycle advancement",
        "Final Lock Declaration v0.2",
        "Stage 7 Alignment is implemented and locked through Resolver Enrichment v0.1 and Code Graph Enrichment v0.1.",
        "Stage 7 is now complete for the current Phase 6 alignment scope",
        "c765255",
        "2daf330",
        "Resolver Enrichment v0.1",
        "Code Graph Enrichment v0.1",
        "47c2a4a",
        "21b05c0",
        "529c222",
        "d73452b",
        "07fd535",
        "7ecaf7d",
        "5349d49",
        "Legacy lifecycle compatibility view is preserved separately from the canonical v1 artifact.",
        "Resolver enrichment is optional bounded enrichment.",
        "Code Graph enrichment is optional bounded non-authoritative enrichment.",
        "Code Graph absence, invalid facts, or malformed facts do not block lifecycle.",
        "Code Graph remains non-authoritative and does not bypass Stage 6 Gate.",
        "Code Graph does not modify dcg.facts.v1.",
        "Stage 7.5 remains unchanged.",
        "Stage 8 remains unchanged.",
        "Stage 7 blocks before Stage 7.5 and Stage 8 when `execution_permitted` is `false`.",
        "informational drift does not block lifecycle.",
        "legacy drift_findings expose blocking-only drift.",
        "Resolver evidence is bounded and does not store raw symbol tables or raw resolver payloads.",
        "Code Graph evidence is bounded and does not store raw `dcg.facts.v1` payloads or full graphs.",
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


def test_resolver_absent_read_model_fixture_matches_builder_projection():
    record = _alignment_record(alignment_id="alignment.docs.fixture.resolver_absent")

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("resolver_absent_read_model.json")


def test_resolver_exact_match_read_model_fixture_matches_builder_projection():
    record = _alignment_record(
        alignment_id="alignment.docs.fixture.resolver_exact_match",
        resolver_case="exact_match",
    )

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("resolver_exact_match_read_model.json")


def test_resolver_unresolved_symbol_read_model_fixture_matches_builder_projection():
    record = _alignment_record(
        alignment_id="alignment.docs.fixture.resolver_unresolved_symbol",
        resolver_case="unresolved_symbol",
    )

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("resolver_unresolved_symbol_read_model.json")


def test_resolver_candidate_match_read_model_fixture_matches_builder_projection():
    record = _alignment_record(
        alignment_id="alignment.docs.fixture.resolver_candidate_match",
        resolver_case="candidate_match",
    )

    assert build_alignment_record_read_model_v1("mission-board", record) == _fixture("resolver_candidate_match_read_model.json")


def test_read_model_fixture_field_list_is_exactly_bounded_surface():
    for fixture_name in (
        "aligned_read_model.json",
        "misaligned_read_model.json",
        "resolver_absent_read_model.json",
        "resolver_exact_match_read_model.json",
        "resolver_unresolved_symbol_read_model.json",
        "resolver_candidate_match_read_model.json",
    ):
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


def test_resolver_read_model_fixtures_roundtrip_through_persistence_read_surface():
    workspace_path = _workspace_dir("stage7_alignment_docs_resolver_fixture_roundtrip")
    cases = [
        ("resolver_absent_read_model.json", _alignment_record(alignment_id="alignment.docs.fixture.resolver_absent")),
        (
            "resolver_exact_match_read_model.json",
            _alignment_record(alignment_id="alignment.docs.fixture.resolver_exact_match", resolver_case="exact_match"),
        ),
        (
            "resolver_unresolved_symbol_read_model.json",
            _alignment_record(alignment_id="alignment.docs.fixture.resolver_unresolved_symbol", resolver_case="unresolved_symbol"),
        ),
        (
            "resolver_candidate_match_read_model.json",
            _alignment_record(alignment_id="alignment.docs.fixture.resolver_candidate_match", resolver_case="candidate_match"),
        ),
    ]
    for fixture_name, record in cases:
        persist_alignment_record_v1(record, workspace_path=workspace_path, section_id="mission-board")
        assert get_stage7_alignment_read_model(workspace_path, "mission-board") == _fixture(fixture_name)


def test_resolver_read_model_fixtures_lock_enrichment_projection():
    absent = _fixture("resolver_absent_read_model.json")
    exact = _fixture("resolver_exact_match_read_model.json")
    unresolved = _fixture("resolver_unresolved_symbol_read_model.json")
    candidate = _fixture("resolver_candidate_match_read_model.json")

    assert absent["resolver_used"] is False
    assert absent["enrichment_status"] == "not_used"
    assert "resolver" not in absent["evidence_sources"]
    assert exact["resolver_used"] is True
    assert exact["enrichment_status"] == "full"
    assert "resolver" in exact["evidence_sources"]
    assert unresolved["alignment_result"] == "misaligned"
    assert unresolved["execution_permitted"] is False
    assert unresolved["drift_codes"] == ["symbol_resolution_conflict"]
    assert unresolved["resolver_used"] is True
    assert unresolved["enrichment_status"] == "partial"
    assert candidate["alignment_result"] == "aligned"
    assert candidate["execution_permitted"] is True
    assert candidate["drift_codes"] == ["symbol_resolution_conflict"]
    assert candidate["informational_issues_count"] == 1


def test_stage7_alignment_api_route_remains_get_only():
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/stage7/alignment")
    }

    assert route_methods == {"/v1/dgce/stage7/alignment/{section_id}": {"GET"}}
