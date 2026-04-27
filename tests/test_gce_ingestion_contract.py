import copy
import inspect
import json
from pathlib import Path

import aether.dgce.gce_ingestion as gce_ingestion
import aether.dgce.decompose as dgce_decompose
from aether.dgce.gce_ingestion import validate_gce_ingestion_input


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


def test_gce_ingestion_schema_file_defines_core_contract():
    schema_path = Path("contracts/gce/gce-ingestion-core-v1.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert schema["title"] == "GCEIngestionCore"
    assert "FormalGddInput" in schema["$defs"]
    assert "StructuredIntentInput" in schema["$defs"]
    assert "NormalizedSessionIntent" in schema["$defs"]
    assert "ClarificationRequest" in schema["$defs"]
    assert "ClarificationResponse" in schema["$defs"]


def test_valid_formal_gdd_input_validates():
    result = validate_gce_ingestion_input(_formal_gdd_input())

    assert result.ok is True
    assert result.stage_1_release_blocked is False
    assert result.clarification_request is None
    assert result.errors == []
    assert result.normalized_session_intent["source_input_path"] == "formal_gdd"
    assert result.normalized_session_intent["stage_1_release"] == {
        "blocked": False,
        "reason_code": None,
    }


def test_valid_structured_intent_input_validates():
    result = validate_gce_ingestion_input(_structured_intent_input())

    assert result.ok is True
    assert result.stage_1_release_blocked is False
    assert result.clarification_request is None
    assert result.errors == []
    assert result.normalized_session_intent["source_input_path"] == "structured_intent"


def test_valid_paths_normalize_to_same_top_level_session_intent_shape():
    formal = validate_gce_ingestion_input(_formal_gdd_input()).normalized_session_intent
    structured = validate_gce_ingestion_input(_structured_intent_input()).normalized_session_intent

    assert list(formal.keys()) == list(structured.keys())
    assert formal["contract_name"] == "GCESessionIntent"
    assert structured["contract_name"] == "GCESessionIntent"
    assert formal["contract_version"] == structured["contract_version"] == "gce.session_intent.v1"
    assert formal["metadata"] == structured["metadata"]
    assert formal["sections"] == structured["sections"]
    assert formal["section_classifications"] == structured["section_classifications"]


def test_missing_required_metadata_fails_closed():
    payload = _structured_intent_input()
    del payload["metadata"]["owner"]

    result = validate_gce_ingestion_input(payload)

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.normalized_session_intent is None
    assert result.clarification_request is None
    assert "missing required fields: owner" in result.errors[0]["condition"]


def test_partial_structured_intent_fails_closed():
    payload = _structured_intent_input()
    del payload["intent"]["sections"]

    result = validate_gce_ingestion_input(payload)

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.normalized_session_intent is None
    assert result.clarification_request is None
    assert "intent missing required fields: sections" in result.errors[0]["condition"]


def test_invalid_section_classification_fails_closed():
    payload = _formal_gdd_input()
    payload["document"]["sections"][0]["classification"] = "semi_durable"

    result = validate_gce_ingestion_input(payload)

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.normalized_session_intent is None
    assert result.clarification_request is None
    assert "classification must be one of the allowed contract values" in result.errors[0]["condition"]


def test_unresolved_intent_returns_deterministic_clarification_request_and_blocks_stage_1():
    payload = _structured_intent_input()
    payload["ambiguities"] = [
        {
            "field_path": "intent.sections[0].content.scope",
            "question": "Which mission board scope is authoritative for this session?",
            "blocking": True,
        }
    ]

    first = validate_gce_ingestion_input(copy.deepcopy(payload))
    second = validate_gce_ingestion_input(copy.deepcopy(payload))

    assert first.ok is False
    assert first.stage_1_release_blocked is True
    assert first.normalized_session_intent is None
    assert first.clarification_request == second.clarification_request
    assert first.clarification_request == {
        "contract_name": "GCEClarificationRequest",
        "contract_version": "gce.clarification_request.v1",
        "artifact_type": "clarification_request",
        "source_input_path": "structured_intent",
        "stage_1_release_blocked": True,
        "reason_code": "unresolved_intent",
        "metadata": _metadata(),
        "questions": [
            {
                "id": "clarification-001",
                "field_path": "intent.sections[0].content.scope",
                "question": "Which mission board scope is authoritative for this session?",
                "blocking": True,
            }
        ],
    }


def test_gce_ingestion_rejects_natural_language_input_without_parsing():
    result = validate_gce_ingestion_input("Build the mission board from the GDD.")

    assert result.ok is False
    assert result.stage_1_release_blocked is True
    assert result.normalized_session_intent is None
    assert result.clarification_request is None
    assert "natural_language_parsing_not_implemented" in result.errors[0]["condition"]


def test_gce_ingestion_introduces_no_code_graph_dependency():
    source = inspect.getsource(gce_ingestion).lower()

    assert "code_graph" not in source
    assert "dcg" not in source


def test_gce_ingestion_does_not_change_stage75_lifecycle_order():
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
