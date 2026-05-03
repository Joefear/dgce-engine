import json
import inspect
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from packages.dgce_contracts.alignment_builder import (
    build_alignment_record_v1,
    validate_alignment_record_v1,
)


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = CONTRACT_ROOT / "schemas" / "alignment" / "alignment_record.v1.schema.json"
TIMESTAMP = "2026-05-02T19:00:00Z"
INPUT_FP = "1111111111111111111111111111111111111111111111111111111111111111"
APPROVAL_FP = "2222222222222222222222222222222222222222222222222222222222222222"
PREVIEW_FP = "3333333333333333333333333333333333333333333333333333333333333333"


def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _assert_schema_valid(record: dict) -> None:
    errors = sorted(_validator().iter_errors(record), key=lambda error: list(error.path))
    assert errors == []
    assert validate_alignment_record_v1(record) is True


def _target(target: str, *, reference: str | None = None, structure: dict | None = None) -> dict:
    payload = {
        "target": target,
        "reference": reference or f"artifact://{target}",
    }
    if structure is not None:
        payload["structure"] = structure
    return payload


def _build(**kwargs) -> dict:
    defaults = {
        "alignment_id": "alignment.builder.test.001",
        "timestamp": TIMESTAMP,
        "input_fingerprint": INPUT_FP,
        "approval_fingerprint": APPROVAL_FP,
        "preview_fingerprint": PREVIEW_FP,
        "approved_design_expectations": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "preview_proposed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "current_observed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
    }
    defaults.update(kwargs)
    return build_alignment_record_v1(**defaults)


def test_aligned_baseline_produces_schema_valid_record():
    record = _build()

    _assert_schema_valid(record)
    assert record["alignment_result"] == "aligned"
    assert record["drift_detected"] is False
    assert record["execution_permitted"] is True
    assert record["drift_items"] == []
    assert record["alignment_enrichment"] == {
        "code_graph_used": False,
        "resolver_used": False,
        "enrichment_status": "not_used",
    }
    assert {item["source"] for item in record["evidence"]} == {"approval", "preview", "runtime_state"}


def test_missing_expected_artifact_blocks_execution():
    record = _build(
        preview_proposed_targets=[_target("api/mission.py", structure={"kind": "api", "version": 1})],
        current_observed_targets=[_target("api/mission.py", structure={"kind": "api", "version": 1})],
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "misaligned"
    assert record["drift_detected"] is True
    assert record["execution_permitted"] is False
    assert record["drift_items"] == [
        {
            "code": "missing_expected_artifact",
            "summary": "Approved expected artifact is missing from preview and observed targets.",
            "target": "models/mission.py",
            "severity": "blocking",
        }
    ]
    assert record["alignment_summary"]["blocking_issues_count"] == 1
    assert record["alignment_summary"]["informational_issues_count"] == 0


def test_unexpected_artifact_blocks_execution():
    record = _build(
        preview_proposed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
            _target("debug/extra.py", structure={"kind": "debug", "version": 1}),
        ],
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "misaligned"
    assert record["execution_permitted"] is False
    assert record["drift_items"] == [
        {
            "code": "unexpected_artifact",
            "summary": "Preview or observed target is outside approved expectations.",
            "target": "debug/extra.py",
            "severity": "blocking",
        }
    ]


def test_mixed_blocking_and_informational_counts_are_correct():
    record = _build(
        preview_proposed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
            _target("debug/extra.py", structure={"kind": "debug", "version": 1}),
        ],
        current_observed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "misaligned"
    assert record["execution_permitted"] is False
    assert record["alignment_summary"]["blocking_issues_count"] == 1
    assert record["alignment_summary"]["informational_issues_count"] == 1
    assert [item["code"] for item in record["drift_items"]] == [
        "unexpected_artifact",
        "structure_mismatch",
    ]
    assert [item["severity"] for item in record["drift_items"]] == [
        "blocking",
        "informational",
    ]


def test_informational_structure_mismatch_does_not_block_by_itself():
    record = _build(
        preview_proposed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        current_observed_targets=[
            _target("api/mission.py", structure={"kind": "api", "version": 2}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "aligned"
    assert record["drift_detected"] is False
    assert record["execution_permitted"] is True
    assert record["alignment_summary"]["blocking_issues_count"] == 0
    assert record["alignment_summary"]["informational_issues_count"] == 1
    assert record["drift_items"][0]["code"] == "structure_mismatch"
    assert record["drift_items"][0]["severity"] == "informational"


def test_execution_permitted_is_derived_from_alignment_result_and_validates():
    aligned = _build()
    misaligned = _build(preview_proposed_targets=[], current_observed_targets=[])

    assert aligned["alignment_result"] == "aligned"
    assert aligned["execution_permitted"] is True
    assert misaligned["alignment_result"] == "misaligned"
    assert misaligned["execution_permitted"] is False

    assert "execution_permitted" not in inspect.signature(build_alignment_record_v1).parameters
    with pytest.raises(TypeError):
        _build(execution_permitted=False)


def test_builder_rejects_free_text_targets_and_extra_fields():
    with pytest.raises(ValueError, match="approved_design_expectations\\[0\\] must be an object"):
        _build(approved_design_expectations=["api/mission.py"])

    with pytest.raises(ValueError, match="unsupported fields"):
        _build(approved_design_expectations=[{"target": "api/mission.py", "raw_text": "free form"}])


def test_builder_outputs_no_policy_simulation_resolver_or_code_graph_evidence():
    record = _build()

    assert {item["source"] for item in record["evidence"]} == {"approval", "preview", "runtime_state"}
    assert record["alignment_enrichment"] == {
        "code_graph_used": False,
        "resolver_used": False,
        "enrichment_status": "not_used",
    }


def test_resolver_exact_match_produces_full_enrichment_and_schema_valid_record():
    record = _build(
        resolver_context={
            "resolved_symbols": [
                {
                    "symbol_name": "BP_PlayerShip",
                    "symbol_kind": "BlueprintClass",
                    "source_path": "Content/Blueprints/BP_PlayerShip.uasset",
                    "resolution_method": "path_metadata",
                    "confidence": "exact_path_match",
                }
            ],
            "unresolved_symbols": [],
            "resolution_status": "resolved",
        }
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "aligned"
    assert record["execution_permitted"] is True
    assert record["drift_items"] == []
    assert record["alignment_enrichment"] == {
        "code_graph_used": False,
        "resolver_used": True,
        "enrichment_status": "full",
    }
    resolver_evidence = [item for item in record["evidence"] if item["source"] == "resolver"]
    assert len(resolver_evidence) == 1
    assert resolver_evidence[0]["reference"] == "resolver:resolved:BlueprintClass:BP_PlayerShip"


def test_resolver_candidate_match_produces_informational_drift_and_partial_enrichment():
    record = _build(
        resolver_context={
            "resolved_symbols": [
                {
                    "symbol_name": "BP_PlayerShip",
                    "symbol_kind": "BlueprintClass",
                    "source_path": "Content/Blueprints/BP_PlayerShip.uasset",
                    "resolution_method": "path_metadata",
                    "confidence": "candidate_match",
                }
            ],
            "unresolved_symbols": [],
            "resolution_status": "resolved",
        }
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "aligned"
    assert record["execution_permitted"] is True
    assert len(record["drift_items"]) == 1
    item = record["drift_items"][0]
    assert item["code"] == "symbol_resolution_conflict"
    assert item["severity"] == "informational"
    assert item["target"] == "BP_PlayerShip"
    assert record["alignment_enrichment"]["resolver_used"] is True
    assert record["alignment_enrichment"]["enrichment_status"] == "partial"
    assert record["alignment_enrichment"]["code_graph_used"] is False
    assert record["alignment_summary"]["blocking_issues_count"] == 0
    assert record["alignment_summary"]["informational_issues_count"] == 1


def test_resolver_unresolved_symbol_produces_blocking_drift_and_blocks_execution():
    record = _build(
        resolver_context={
            "resolved_symbols": [],
            "unresolved_symbols": [
                {
                    "symbol_name": "MissingEvent",
                    "symbol_kind": "BlueprintClass",
                    "source_path": None,
                    "resolution_method": "path_metadata",
                    "confidence": "unresolved",
                }
            ],
            "resolution_status": "unresolved",
        }
    )

    _assert_schema_valid(record)
    assert record["alignment_result"] == "misaligned"
    assert record["execution_permitted"] is False
    assert len(record["drift_items"]) == 1
    item = record["drift_items"][0]
    assert item["code"] == "symbol_resolution_conflict"
    assert item["severity"] == "blocking"
    assert item["target"] == "MissingEvent"
    assert record["alignment_enrichment"]["resolver_used"] is True
    assert record["alignment_enrichment"]["enrichment_status"] == "partial"
    assert record["alignment_enrichment"]["code_graph_used"] is False


def test_resolver_evidence_keys_are_bounded_to_source_and_reference_only():
    record = _build(
        resolver_context={
            "resolved_symbols": [
                {
                    "symbol_name": "BP_PlayerShip",
                    "symbol_kind": "BlueprintClass",
                    "source_path": "Content/Blueprints/BP_PlayerShip.uasset",
                    "resolution_method": "path_metadata",
                    "confidence": "exact_path_match",
                }
            ],
            "unresolved_symbols": [],
            "resolution_status": "resolved",
        }
    )

    for evidence_item in record["evidence"]:
        assert set(evidence_item.keys()) <= {"source", "reference", "snippet_hash"}
        for forbidden in ("raw_symbols", "symbol_table", "resolver_payload", "raw_model_text", "raw_file_content"):
            assert forbidden not in evidence_item
