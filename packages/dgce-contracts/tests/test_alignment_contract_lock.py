import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = CONTRACT_ROOT / "schemas" / "alignment" / "alignment_record.v1.schema.json"
FIXTURE_DIR = CONTRACT_ROOT / "fixtures" / "alignment"
DCG_SCHEMA_PATH = CONTRACT_ROOT.parents[1] / "contracts" / "dcg" / "dcg-facts-v1.schema.json"
DCG_CHECKSUM_PATH = CONTRACT_ROOT.parents[1] / "contracts" / "dcg" / "dcg-facts-v1.sha256"

DRIFT_CODES = [
    "missing_expected_artifact",
    "unexpected_artifact",
    "structure_mismatch",
    "symbol_resolution_conflict",
    "insertion_point_invalid",
    "design_contract_violation",
    "dependency_mismatch",
    "adapter_constraint_violation",
]

EVIDENCE_SOURCES = [
    "preview",
    "approval",
    "code_graph",
    "resolver",
    "runtime_state",
]


def _schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    return Draft202012Validator(_schema(), format_checker=FormatChecker())


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _fixture_names() -> list[str]:
    return [
        "aligned_minimal.json",
        "misaligned_blocking.json",
        "misaligned_mixed.json",
    ]


def _assert_valid(payload: dict) -> None:
    errors = sorted(_validator().iter_errors(payload), key=lambda error: list(error.path))
    assert errors == []


def _object_schemas(schema_node: object) -> list[dict]:
    found = []
    if isinstance(schema_node, dict):
        if schema_node.get("type") == "object":
            found.append(schema_node)
        for value in schema_node.values():
            found.extend(_object_schemas(value))
    elif isinstance(schema_node, list):
        for value in schema_node:
            found.extend(_object_schemas(value))
    return found


def test_alignment_schema_declares_draft_2020_12_and_strict_objects():
    schema = _schema()

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["title"] == "alignment_record"
    for object_schema in _object_schemas(schema):
        assert object_schema.get("additionalProperties") is False


@pytest.mark.parametrize("fixture_name", _fixture_names())
def test_alignment_fixtures_validate_against_schema(fixture_name):
    _assert_valid(_fixture(fixture_name))


def test_alignment_schema_enum_completeness():
    schema = _schema()

    assert schema["properties"]["alignment_result"]["enum"] == ["aligned", "misaligned"]
    assert schema["$defs"]["DriftCode"]["enum"] == DRIFT_CODES
    assert schema["$defs"]["DriftItem"]["properties"]["severity"]["enum"] == [
        "blocking",
        "informational",
    ]
    assert schema["$defs"]["EvidenceItem"]["properties"]["source"]["enum"] == EVIDENCE_SOURCES
    assert schema["$defs"]["AlignmentEnrichment"]["properties"]["enrichment_status"]["enum"] == [
        "not_used",
        "partial",
        "full",
    ]


def test_alignment_drift_code_snapshot():
    assert _schema()["$defs"]["DriftCode"]["enum"] == DRIFT_CODES


def test_alignment_fixtures_include_at_least_two_drift_scenarios():
    observed_codes = {
        item["code"]
        for fixture_name in _fixture_names()
        for item in _fixture(fixture_name)["drift_items"]
    }

    assert len(observed_codes) >= 2
    assert {"missing_expected_artifact", "symbol_resolution_conflict"}.issubset(observed_codes)


def test_alignment_contract_fails_on_additional_properties():
    payload = _fixture("aligned_minimal.json")
    payload["extra"] = "not allowed"

    assert list(_validator().iter_errors(payload))

    nested = _fixture("misaligned_blocking.json")
    nested["drift_items"][0]["extra"] = "not allowed"
    assert list(_validator().iter_errors(nested))

    evidence = _fixture("misaligned_mixed.json")
    evidence["evidence"][0]["extra"] = "not allowed"
    assert list(_validator().iter_errors(evidence))

    enrichment = _fixture("aligned_minimal.json")
    enrichment["alignment_enrichment"]["extra"] = "not allowed"
    assert list(_validator().iter_errors(enrichment))

    summary = _fixture("aligned_minimal.json")
    summary["alignment_summary"]["extra"] = "not allowed"
    assert list(_validator().iter_errors(summary))


def test_alignment_contract_rejects_invalid_enums_and_loose_types():
    invalid_enum = _fixture("misaligned_blocking.json")
    invalid_enum["drift_items"][0]["code"] = "graph_guess"
    assert list(_validator().iter_errors(invalid_enum))

    loose_type = _fixture("aligned_minimal.json")
    loose_type["drift_detected"] = "false"
    assert list(_validator().iter_errors(loose_type))

    nullable = _fixture("aligned_minimal.json")
    nullable["approval_fingerprint"] = None
    assert list(_validator().iter_errors(nullable))

    bad_timestamp = _fixture("aligned_minimal.json")
    bad_timestamp["timestamp"] = "2026-05-02 18:00:00"
    assert list(_validator().iter_errors(bad_timestamp))


def test_alignment_contract_allows_optional_snippet_hash_only_when_valid():
    payload = _fixture("misaligned_blocking.json")
    _assert_valid(payload)

    without_hash = copy.deepcopy(payload)
    del without_hash["evidence"][0]["snippet_hash"]
    _assert_valid(without_hash)

    invalid_hash = copy.deepcopy(payload)
    invalid_hash["evidence"][0]["snippet_hash"] = "not-a-sha256"
    assert list(_validator().iter_errors(invalid_hash))


def test_stage7_code_graph_enrichment_does_not_change_dcg_facts_v1_contract():
    import hashlib

    expected = DCG_CHECKSUM_PATH.read_text(encoding="utf-8").strip().split()[0]

    assert hashlib.sha256(DCG_SCHEMA_PATH.read_bytes()).hexdigest() == expected
    dcg_schema = json.loads(DCG_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert dcg_schema["properties"]["contract_version"]["const"] == "dcg.facts.v1"
    assert "alignment_enrichment" not in json.dumps(dcg_schema, sort_keys=True)


def test_alignment_fixtures_lock_resolver_enrichment_fields():
    blocking = _fixture("misaligned_blocking.json")
    aligned = _fixture("aligned_minimal.json")

    assert blocking["alignment_enrichment"]["resolver_used"] is True
    assert blocking["alignment_enrichment"]["code_graph_used"] is False
    assert blocking["alignment_enrichment"]["enrichment_status"] == "partial"
    assert any(e["source"] == "resolver" for e in blocking["evidence"])

    assert aligned["alignment_enrichment"]["resolver_used"] is False
    assert aligned["alignment_enrichment"]["enrichment_status"] == "not_used"
    assert aligned["alignment_enrichment"]["code_graph_used"] is False
    assert all(e["source"] != "resolver" for e in aligned["evidence"])
