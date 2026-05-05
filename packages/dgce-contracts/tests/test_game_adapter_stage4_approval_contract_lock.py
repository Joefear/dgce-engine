import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = CONTRACT_ROOT / "schemas" / "game_adapter" / "stage4_approval.v1.schema.json"
FIXTURE_DIR = CONTRACT_ROOT / "fixtures" / "game_adapter" / "stage4_approval"

APPROVAL_STATUS_ENUM = [
    "approved",
    "rejected",
    "blocked",
]
EVIDENCE_SOURCE_ENUM = [
    "review_bundle",
    "preview",
    "unreal_manifest",
    "symbol_candidate_index",
    "resolver",
    "alignment",
    "operator_context",
]
ROOT_FIELDS = {
    "artifact_type",
    "contract_name",
    "contract_version",
    "adapter",
    "domain",
    "approval_id",
    "section_id",
    "created_at",
    "approved_by",
    "approval_status",
    "source_review_id",
    "source_review_fingerprint",
    "source_preview_fingerprint",
    "source_input_fingerprint",
    "approved_change_ids",
    "rejected_change_ids",
    "approval_summary",
    "stale_detection",
    "approval_constraints",
    "evidence",
    "forbidden_runtime_actions",
}
APPROVAL_SUMMARY_FIELDS = {
    "operator_summary",
    "approval_scope_summary",
    "risk_acknowledgement",
}
STALE_DETECTION_FIELDS = {
    "captured_review_fingerprint",
    "captured_preview_fingerprint",
    "captured_input_fingerprint",
    "stale_check_required",
}
EVIDENCE_FIELDS = {
    "source",
    "reference",
    "snippet_hash",
}
FORBIDDEN_BOUNDARY_FIELDS = [
    "execution_permission",
    "execute",
    "write_targets",
    "write_directives",
    "blueprint_mutation",
    "simulation_result",
    "guardrail_policy_decision",
    "stage6_gate_decision",
    "stage7_alignment_result",
    "stage8_execution_stamp",
    "auto_approved",
]


def _schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    return Draft202012Validator(_schema(), format_checker=FormatChecker())


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _fixture_names() -> list[str]:
    return [
        "approved_minimal.json",
        "rejected_minimal.json",
        "blocked_stale_review.json",
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


def _schema_property_names(schema_node: object) -> set[str]:
    names: set[str] = set()
    if isinstance(schema_node, dict):
        properties = schema_node.get("properties")
        if isinstance(properties, dict):
            names.update(str(key) for key in properties)
        for value in schema_node.values():
            names.update(_schema_property_names(value))
    elif isinstance(schema_node, list):
        for value in schema_node:
            names.update(_schema_property_names(value))
    return names


def test_stage4_approval_schema_declares_draft_2020_12_and_strict_objects():
    schema = _schema()

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["title"] == "game_adapter_stage4_approval"
    for object_schema in _object_schemas(schema):
        assert object_schema.get("additionalProperties") is False


def test_stage4_approval_schema_enum_locks():
    schema = _schema()

    assert schema["$defs"]["ApprovalStatus"]["enum"] == APPROVAL_STATUS_ENUM
    assert schema["$defs"]["EvidenceSource"]["enum"] == EVIDENCE_SOURCE_ENUM


def test_stage4_approval_fixtures_validate_against_schema():
    for fixture_name in _fixture_names():
        _assert_valid(_fixture(fixture_name))


def test_stage4_approval_fixture_field_sets_are_stable():
    for fixture_name in _fixture_names():
        fixture = _fixture(fixture_name)

        assert set(fixture) == ROOT_FIELDS
        assert set(fixture["approval_summary"]) == APPROVAL_SUMMARY_FIELDS
        assert set(fixture["stale_detection"]) == STALE_DETECTION_FIELDS
        for evidence in fixture["evidence"]:
            assert set(evidence) <= EVIDENCE_FIELDS


def test_stage4_approval_status_fixture_semantics_are_authorization_only():
    approved = _fixture("approved_minimal.json")
    rejected = _fixture("rejected_minimal.json")
    blocked = _fixture("blocked_stale_review.json")

    assert approved["approval_status"] == "approved"
    assert approved["approved_change_ids"] == ["change.player-component", "change.interact-binding"]
    assert approved["rejected_change_ids"] == []
    assert rejected["approval_status"] == "rejected"
    assert rejected["approved_change_ids"] == []
    assert rejected["rejected_change_ids"] == ["change.player-component"]
    assert blocked["approval_status"] == "blocked"
    assert blocked["approved_change_ids"] == []
    assert blocked["rejected_change_ids"] == []
    assert blocked["stale_detection"]["stale_check_required"] is True

    for fixture in (approved, rejected, blocked):
        serialized = json.dumps(fixture, sort_keys=True)
        assert "execution_permission" not in fixture
        assert "execution_permitted" not in fixture
        assert "execute" not in fixture
        assert "write_targets" not in fixture
        assert "stage8_execution_stamp" not in fixture
        assert "Approval does not grant execution permission" in serialized or "do not grant execution permission" in serialized


def test_stage4_approval_stale_detection_fields_are_required_and_bounded():
    baseline = _fixture("approved_minimal.json")

    for field_name in STALE_DETECTION_FIELDS:
        tampered = copy.deepcopy(baseline)
        del tampered["stale_detection"][field_name]

        assert list(_validator().iter_errors(tampered)), field_name

    invalid_fingerprint = copy.deepcopy(baseline)
    invalid_fingerprint["stale_detection"]["captured_review_fingerprint"] = "not-a-fingerprint"
    assert list(_validator().iter_errors(invalid_fingerprint))

    invalid_type = copy.deepcopy(baseline)
    invalid_type["stale_detection"]["stale_check_required"] = "true"
    assert list(_validator().iter_errors(invalid_type))


def test_stage4_approval_schema_contains_no_runtime_boundary_fields():
    schema_properties = _schema_property_names(_schema())

    for forbidden in FORBIDDEN_BOUNDARY_FIELDS:
        assert forbidden not in schema_properties


def test_stage4_approval_rejects_runtime_policy_gate_alignment_simulation_and_execution_fields():
    baseline = _fixture("approved_minimal.json")

    for forbidden in FORBIDDEN_BOUNDARY_FIELDS:
        tampered = copy.deepcopy(baseline)
        tampered[forbidden] = True

        assert list(_validator().iter_errors(tampered)), forbidden


def test_stage4_approval_rejects_nested_runtime_and_raw_reasoning_fields():
    baseline = _fixture("approved_minimal.json")
    nested_tamper_cases = [
        ("approval_summary", "freeform_llm_reasoning", "operator approved because model says so"),
        ("stale_detection", "stage6_gate_decision", "ALLOW"),
        ("evidence", "raw_preview", {"full": "preview"}),
        ("evidence", "raw_resolver_payload", {"symbols": []}),
        ("evidence", "simulation_result", {"status": "pass"}),
    ]

    for collection, key, value in nested_tamper_cases:
        tampered = copy.deepcopy(baseline)
        if collection == "evidence":
            tampered[collection][0][key] = value
        else:
            tampered[collection][key] = value

        assert list(_validator().iter_errors(tampered)), key


def test_stage4_approval_rejects_invalid_status_evidence_source_and_unbounded_text():
    baseline = _fixture("approved_minimal.json")

    invalid_status = copy.deepcopy(baseline)
    invalid_status["approval_status"] = "auto_approved"
    assert list(_validator().iter_errors(invalid_status))

    invalid_evidence = copy.deepcopy(baseline)
    invalid_evidence["evidence"][0]["source"] = "guardrail"
    assert list(_validator().iter_errors(invalid_evidence))

    unbounded_summary = copy.deepcopy(baseline)
    unbounded_summary["approval_summary"]["operator_summary"] = "x" * 513
    assert list(_validator().iter_errors(unbounded_summary))


def test_stage4_approval_forbidden_runtime_actions_are_documentation_only():
    fixture = _fixture("approved_minimal.json")

    assert fixture["forbidden_runtime_actions"] == [
        "no_auto_approval",
        "no_binary_blueprint_parsing",
        "no_blueprint_mutation",
        "no_execution_performed",
        "no_execution_permission_granted",
        "no_guardrail_policy_decision",
        "no_stage6_gate_decision",
        "no_stage7_alignment_result",
        "no_stage75_simulation_result",
        "no_stage8_execution_stamp",
        "no_unreal_project_writes",
        "no_write_directives",
    ]
    assert "execution_permission" not in fixture
    assert "stage6_gate_decision" not in fixture
    assert "stage7_alignment_result" not in fixture
    assert "simulation_result" not in fixture
    assert "stage8_execution_stamp" not in fixture


def test_stage4_approval_contract_is_not_execution_authority():
    approved = _fixture("approved_minimal.json")
    summary_text = json.dumps(approved["approval_summary"], sort_keys=True)

    assert approved["approval_status"] == "approved"
    assert "later lifecycle processing" in summary_text
    assert "does not grant execution permission" in summary_text
    assert "bypass gate, alignment, simulation, or execution controls" in summary_text
    assert "execution_permitted" not in approved
    assert "selected_mode" not in approved
