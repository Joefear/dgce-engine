import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = CONTRACT_ROOT / "schemas" / "game_adapter" / "stage3_review_bundle.v1.schema.json"
FIXTURE_DIR = CONTRACT_ROOT / "fixtures" / "game_adapter" / "stage3_review_bundle"

REVIEW_STATUS_ENUM = [
    "ready_for_operator_review",
    "blocked",
]
OPERATION_ENUM = [
    "create",
    "modify",
    "ignore",
]
OUTPUT_STRATEGY_ENUM = [
    "blueprint",
    "cpp",
    "both",
    "none",
    "unknown",
]
EVIDENCE_SOURCE_ENUM = [
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
    "review_id",
    "section_id",
    "created_at",
    "source_preview_fingerprint",
    "source_input_fingerprint",
    "review_status",
    "review_summary",
    "proposed_changes",
    "dependency_notes",
    "operator_questions",
    "approval_readiness",
    "evidence",
    "forbidden_runtime_actions",
}
REVIEW_SUMMARY_FIELDS = {
    "title",
    "primary_intent",
    "operator_summary",
    "risk_summary",
}
PROPOSED_CHANGE_FIELDS = {
    "change_id",
    "target_path",
    "target_kind",
    "operation",
    "output_strategy",
    "human_readable_summary",
    "review_risk",
}
APPROVAL_READINESS_FIELDS = {
    "ready_for_approval",
    "blocking_review_issues_count",
    "informational_review_issues_count",
}
EVIDENCE_FIELDS = {
    "source",
    "reference",
    "snippet_hash",
}
SAFE_RELATIVE_PATH_PATTERN = r"^(?!.*\\)(?!.*(?:^|/)\.\.(?:/|$))[A-Za-z0-9_./:+#-]+$"
FORBIDDEN_BOUNDARY_FIELDS = [
    "approval_status",
    "approval_granted",
    "approved",
    "execution_permitted",
    "execution_permission",
    "stage8_write_instructions",
    "write_instructions",
    "blueprint_mutation",
    "blueprint_mutations",
    "simulation_result",
    "simulation_status",
    "guardrail_policy_decision",
    "policy_decision",
]


def _schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    return Draft202012Validator(_schema(), format_checker=FormatChecker())


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _fixture_names() -> list[str]:
    return [
        "ready_minimal.json",
        "ready_with_multiple_changes.json",
        "blocked_with_operator_questions.json",
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


def test_stage3_review_bundle_schema_declares_draft_2020_12_and_strict_objects():
    schema = _schema()

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["title"] == "game_adapter_stage3_review_bundle"
    for object_schema in _object_schemas(schema):
        assert object_schema.get("additionalProperties") is False


def test_stage3_review_bundle_schema_enum_locks():
    schema = _schema()

    assert schema["properties"]["review_status"]["enum"] == REVIEW_STATUS_ENUM
    assert schema["$defs"]["Operation"]["enum"] == OPERATION_ENUM
    assert schema["$defs"]["OutputStrategy"]["enum"] == OUTPUT_STRATEGY_ENUM
    assert schema["$defs"]["EvidenceSource"]["enum"] == EVIDENCE_SOURCE_ENUM
    assert schema["$defs"]["SafeRelativePath"]["pattern"] == SAFE_RELATIVE_PATH_PATTERN


def test_stage3_review_bundle_fixtures_validate_against_schema():
    for fixture_name in _fixture_names():
        _assert_valid(_fixture(fixture_name))


def test_stage3_review_bundle_accepts_stage2_unreal_package_and_source_paths():
    baseline = _fixture("ready_minimal.json")
    for target_path in ("/Game/Blueprints/BP_Player", "/Source/Game/InventorySubsystem"):
        payload = copy.deepcopy(baseline)
        payload["proposed_changes"][0]["target_path"] = target_path

        _assert_valid(payload)

    for target_path in ("../Bad", "/Game/../Bad", "Game\\Bad", "Content\\Blueprints\\BP_Player.uasset"):
        payload = copy.deepcopy(baseline)
        payload["proposed_changes"][0]["target_path"] = target_path

        assert list(_validator().iter_errors(payload)), target_path


def test_stage3_review_bundle_target_path_character_class_remains_bounded():
    baseline = _fixture("ready_minimal.json")
    accepted = (
        "Content/Blueprints/BP_Player.uasset",
        "/Game/Blueprints/BP_Player",
        "/Source/Game/InventorySubsystem",
        "Source/Game/Public/InventoryComponent.h",
        "Config/DefaultGame.ini",
        "Plugin:Inventory+Gameplay#Target",
    )
    rejected = (
        "Content/Blueprints/BP Player.uasset",
        "Content/Blueprints/BP_Player.uasset?",
        "Content/Blueprints/BP_Player.uasset*",
        "Content/Blueprints/BP_Player.uasset|",
        "Content/Blueprints/\tBP_Player.uasset",
    )

    for target_path in accepted:
        payload = copy.deepcopy(baseline)
        payload["proposed_changes"][0]["target_path"] = target_path

        _assert_valid(payload)

    for target_path in rejected:
        payload = copy.deepcopy(baseline)
        payload["proposed_changes"][0]["target_path"] = target_path

        assert list(_validator().iter_errors(payload)), target_path


def test_stage3_review_bundle_fixture_field_sets_are_stable():
    for fixture_name in _fixture_names():
        fixture = _fixture(fixture_name)

        assert set(fixture) == ROOT_FIELDS
        assert set(fixture["review_summary"]) == REVIEW_SUMMARY_FIELDS
        assert set(fixture["approval_readiness"]) == APPROVAL_READINESS_FIELDS
        for change in fixture["proposed_changes"]:
            assert set(change) == PROPOSED_CHANGE_FIELDS
        for evidence in fixture["evidence"]:
            assert set(evidence) <= EVIDENCE_FIELDS


def test_stage3_review_bundle_ready_and_blocked_fixture_semantics():
    ready_minimal = _fixture("ready_minimal.json")
    ready_multiple = _fixture("ready_with_multiple_changes.json")
    blocked = _fixture("blocked_with_operator_questions.json")

    assert ready_minimal["review_status"] == "ready_for_operator_review"
    assert ready_minimal["approval_readiness"]["ready_for_approval"] is True
    assert ready_minimal["operator_questions"] == []
    assert ready_multiple["review_status"] == "ready_for_operator_review"
    assert len(ready_multiple["proposed_changes"]) == 3
    assert blocked["review_status"] == "blocked"
    assert blocked["approval_readiness"]["ready_for_approval"] is False
    assert blocked["approval_readiness"]["blocking_review_issues_count"] == 1
    assert len(blocked["operator_questions"]) == 2


def test_stage3_review_bundle_schema_contains_no_runtime_boundary_fields():
    schema_properties = _schema_property_names(_schema())

    for forbidden in FORBIDDEN_BOUNDARY_FIELDS:
        assert forbidden not in schema_properties


def test_stage3_review_bundle_rejects_approval_execution_simulation_policy_and_write_fields():
    baseline = _fixture("ready_minimal.json")

    for forbidden in FORBIDDEN_BOUNDARY_FIELDS:
        tampered = copy.deepcopy(baseline)
        tampered[forbidden] = True

        assert list(_validator().iter_errors(tampered)), forbidden


def test_stage3_review_bundle_rejects_nested_runtime_and_raw_content_fields():
    baseline = _fixture("ready_minimal.json")
    nested_tamper_cases = [
        ("proposed_changes", 0, "stage8_write_instructions", ["write Content/Blueprints/BP_Player.uasset"]),
        ("proposed_changes", 0, "blueprint_mutation", {"node": "raw graph mutation"}),
        ("proposed_changes", 0, "raw_model_text", "provider text"),
        ("evidence", 0, "raw_artifact", {"full_preview": "unbounded"}),
        ("evidence", 0, "full_symbol_table", ["BP_Player", "InventoryComponent"]),
        ("review_summary", None, "freeform_llm_reasoning", "unbounded model rationale"),
    ]

    for collection, index, key, value in nested_tamper_cases:
        tampered = copy.deepcopy(baseline)
        if index is None:
            tampered[collection][key] = value
        else:
            tampered[collection][index][key] = value

        assert list(_validator().iter_errors(tampered)), key


def test_stage3_review_bundle_rejects_invalid_enums_and_unbounded_strings():
    invalid_status = _fixture("ready_minimal.json")
    invalid_status["review_status"] = "approved"
    assert list(_validator().iter_errors(invalid_status))

    invalid_operation = _fixture("ready_minimal.json")
    invalid_operation["proposed_changes"][0]["operation"] = "delete"
    assert list(_validator().iter_errors(invalid_operation))

    invalid_strategy = _fixture("ready_minimal.json")
    invalid_strategy["proposed_changes"][0]["output_strategy"] = "stage8"
    assert list(_validator().iter_errors(invalid_strategy))

    invalid_evidence = _fixture("ready_minimal.json")
    invalid_evidence["evidence"][0]["source"] = "guardrail"
    assert list(_validator().iter_errors(invalid_evidence))

    unbounded_text = _fixture("ready_minimal.json")
    unbounded_text["review_summary"]["operator_summary"] = "x" * 513
    assert list(_validator().iter_errors(unbounded_text))


def test_stage3_review_bundle_evidence_is_bounded_references_only():
    for fixture_name in _fixture_names():
        fixture = _fixture(fixture_name)
        serialized = json.dumps(fixture, sort_keys=True)

        assert "raw_artifact" not in serialized
        assert "full_symbol_table" not in serialized
        assert "full_preview" not in serialized
        assert "raw_model_text" not in serialized
        for evidence in fixture["evidence"]:
            assert set(evidence) <= EVIDENCE_FIELDS
            assert evidence["source"] in EVIDENCE_SOURCE_ENUM
            assert len(evidence["reference"]) <= 512


def test_stage3_review_bundle_forbidden_runtime_actions_are_documentation_only():
    fixture = _fixture("ready_minimal.json")

    assert fixture["forbidden_runtime_actions"] == [
        "no_approval_granted",
        "no_execution_performed",
        "no_stage8_write_instructions",
        "no_blueprint_mutation",
        "no_unreal_project_writes",
        "no_binary_blueprint_parsing",
        "no_simulation_run",
        "no_guardrail_policy_decision",
    ]
    assert "approval_status" not in fixture
    assert "execution_permitted" not in fixture
    assert "simulation_result" not in fixture
    assert "guardrail_policy_decision" not in fixture
