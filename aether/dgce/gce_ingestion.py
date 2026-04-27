"""Stage 0 GCE ingestion contract validation and normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import re
from typing import Any


INGESTION_CONTRACT_NAME = "GCEIngestionCore"
INGESTION_CONTRACT_VERSION = "gce.ingestion.core.v1"
SESSION_INTENT_CONTRACT_NAME = "GCESessionIntent"
SESSION_INTENT_CONTRACT_VERSION = "gce.session_intent.v1"
CLARIFICATION_CONTRACT_NAME = "GCEClarificationRequest"
CLARIFICATION_CONTRACT_VERSION = "gce.clarification_request.v1"
CLARIFICATION_RESPONSE_CONTRACT_NAME = "GCEClarificationResponse"
CLARIFICATION_RESPONSE_CONTRACT_VERSION = "gce.clarification_response.v1"

INPUT_PATH_FORMAL_GDD = "formal_gdd"
INPUT_PATH_STRUCTURED_INTENT = "structured_intent"
ALLOWED_INPUT_PATHS = {INPUT_PATH_FORMAL_GDD, INPUT_PATH_STRUCTURED_INTENT}

SECTION_CLASSIFICATIONS = {"durable", "volatile"}
SECTION_AUTHORSHIP = {"human", "injected"}
SECTION_CLASSIFICATION_AUTHORSHIP = {
    "durable": "human",
    "volatile": "injected",
}
REQUIRED_METADATA_FIELDS = {
    "project_id",
    "project_name",
    "owner",
    "source_id",
    "created_at",
    "updated_at",
}
RFC3339_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


@dataclass(frozen=True)
class GCEIngestionValidationResult:
    """Structured Stage 0 outcome for GCE ingestion boundary validation."""

    ok: bool
    stage_1_release_blocked: bool
    normalized_session_intent: dict[str, Any] | None = None
    clarification_request: dict[str, Any] | None = None
    errors: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class GCEClarificationResolutionResult:
    """Structured result for resolving a blocked GCE clarification request."""

    ok: bool
    blocked: bool
    reason_code: str | None
    resolved_input: dict[str, Any] | None = None
    errors: list[dict[str, str]] = field(default_factory=list)


def validate_gce_ingestion_input(raw_input: Any) -> GCEIngestionValidationResult:
    """Validate one formal GDD or structured intent input and normalize it.

    Natural-language parsing is intentionally outside this slice. Inputs must
    already be structured contract payloads.
    """
    try:
        payload = _expect_dict(raw_input, "gce_ingestion_input")
        input_path = _parse_common_envelope(payload)
        metadata = _parse_metadata(payload.get("metadata"), "metadata")
        ambiguities = _parse_ambiguities(payload.get("ambiguities"), "ambiguities")
        body = _parse_input_body(payload, input_path)
        sections = _parse_sections(body.get("sections"), f"{_body_field(input_path)}.sections")
        session_objective = _require_non_empty_string(
            body.get("session_objective"),
            f"{_body_field(input_path)}.session_objective",
        )
        if ambiguities:
            return GCEIngestionValidationResult(
                ok=False,
                stage_1_release_blocked=True,
                clarification_request=_build_clarification_request(
                    input_path=input_path,
                    metadata=metadata,
                    ambiguities=ambiguities,
                ),
                errors=[
                    _error(
                        "ambiguities",
                        "unresolved_intent",
                    )
                ],
            )
        return GCEIngestionValidationResult(
            ok=True,
            stage_1_release_blocked=False,
            normalized_session_intent=_build_normalized_session_intent(
                input_path=input_path,
                metadata=metadata,
                session_objective=session_objective,
                sections=sections,
            ),
            errors=[],
        )
    except ValueError as exc:
        return GCEIngestionValidationResult(
            ok=False,
            stage_1_release_blocked=True,
            errors=[_error("gce_ingestion_input", str(exc))],
        )


def resolve_gce_clarification_response(
    blocked_stage0_package: Any,
    clarification_response: Any,
) -> GCEClarificationResolutionResult:
    """Resolve a blocked GCE clarification request into structured intent.

    This accepts only a structured operator response. It does not parse natural
    language and does not release Stage 1 directly.
    """
    try:
        package = _expect_dict(blocked_stage0_package, "blocked_stage0_package")
        clarification_request = _extract_blocking_clarification_request(package)
        response = _parse_clarification_response(clarification_response)
        expected_fingerprint = compute_gce_clarification_request_fingerprint(clarification_request)
        if response["source_clarification_request_fingerprint"] != expected_fingerprint:
            return _clarification_resolution_blocked("clarification_source_mismatch")

        resolved_input = {
            "contract_name": INGESTION_CONTRACT_NAME,
            "contract_version": INGESTION_CONTRACT_VERSION,
            "input_path": INPUT_PATH_STRUCTURED_INTENT,
            "metadata": dict(clarification_request["metadata"]),
            "intent": {
                "session_objective": response["resolved_fields"]["session_objective"],
                "sections": [dict(section) for section in response["resolved_fields"]["sections"]],
            },
            "ambiguities": [],
        }
        validation = validate_gce_ingestion_input(resolved_input)
        if not validation.ok:
            return GCEClarificationResolutionResult(
                ok=False,
                blocked=True,
                reason_code="resolved_intent_invalid",
                errors=list(validation.errors),
            )
        return GCEClarificationResolutionResult(
            ok=True,
            blocked=False,
            reason_code=None,
            resolved_input=resolved_input,
            errors=[],
        )
    except ValueError as exc:
        reason_code = str(exc)
        if reason_code not in {
            "clarification_request_missing",
            "clarification_response_malformed",
            "source_not_blocked_for_clarification",
            "unsupported_fields",
        }:
            reason_code = "clarification_response_malformed"
        return _clarification_resolution_blocked(reason_code)


def compute_gce_clarification_request_fingerprint(clarification_request: dict[str, Any]) -> str:
    """Return the deterministic SHA-256 fingerprint for one clarification request."""
    canonical_bytes = (json.dumps(clarification_request, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def _extract_blocking_clarification_request(package: dict[str, Any]) -> dict[str, Any]:
    stage_1_release = package.get("stage_1_release")
    if not isinstance(stage_1_release, dict) or stage_1_release.get("blocked") is not True:
        raise ValueError("source_not_blocked_for_clarification")
    reason_code = package.get("reason_code") or stage_1_release.get("reason_code")
    if reason_code != "clarification_required":
        raise ValueError("source_not_blocked_for_clarification")
    clarification_request = package.get("clarification_request")
    if not isinstance(clarification_request, dict):
        raise ValueError("clarification_request_missing")
    _validate_clarification_request_shape(clarification_request)
    return dict(clarification_request)


def _validate_clarification_request_shape(clarification_request: dict[str, Any]) -> None:
    _validate_fields(
        clarification_request,
        "clarification_request",
        required_fields={
            "contract_name",
            "contract_version",
            "artifact_type",
            "source_input_path",
            "stage_1_release_blocked",
            "reason_code",
            "metadata",
            "questions",
        },
        allowed_fields={
            "contract_name",
            "contract_version",
            "artifact_type",
            "source_input_path",
            "stage_1_release_blocked",
            "reason_code",
            "metadata",
            "questions",
        },
    )
    _require_exact_string(clarification_request.get("contract_name"), "clarification_request.contract_name", CLARIFICATION_CONTRACT_NAME)
    _require_exact_string(
        clarification_request.get("contract_version"),
        "clarification_request.contract_version",
        CLARIFICATION_CONTRACT_VERSION,
    )
    _require_exact_string(clarification_request.get("artifact_type"), "clarification_request.artifact_type", "clarification_request")
    _require_enum(
        clarification_request.get("source_input_path"),
        "clarification_request.source_input_path",
        ALLOWED_INPUT_PATHS,
    )
    if clarification_request.get("stage_1_release_blocked") is not True:
        raise ValueError("clarification_response_malformed")
    _require_exact_string(clarification_request.get("reason_code"), "clarification_request.reason_code", "unresolved_intent")
    _parse_metadata(clarification_request.get("metadata"), "clarification_request.metadata")
    questions = _expect_list(clarification_request.get("questions"), "clarification_request.questions")
    if not questions:
        raise ValueError("clarification_response_malformed")
    for index, question in enumerate(questions):
        question_payload = _expect_dict(question, f"clarification_request.questions[{index}]")
        _validate_fields(
            question_payload,
            f"clarification_request.questions[{index}]",
            required_fields={"id", "field_path", "question", "blocking"},
            allowed_fields={"id", "field_path", "question", "blocking"},
        )
        _require_non_empty_string(question_payload.get("id"), f"clarification_request.questions[{index}].id")
        _require_non_empty_string(question_payload.get("field_path"), f"clarification_request.questions[{index}].field_path")
        _require_non_empty_string(question_payload.get("question"), f"clarification_request.questions[{index}].question")
        if question_payload.get("blocking") is not True:
            raise ValueError("clarification_response_malformed")


def _parse_clarification_response(raw_response: Any) -> dict[str, Any]:
    response = _expect_dict(raw_response, "clarification_response")
    try:
        _validate_fields(
            response,
            "clarification_response",
            required_fields={
                "contract_name",
                "contract_version",
                "source_clarification_request_fingerprint",
                "operator_response",
                "resolved_fields",
            },
            allowed_fields={
                "contract_name",
                "contract_version",
                "source_clarification_request_fingerprint",
                "operator_response",
                "resolved_fields",
            },
        )
        _require_exact_string(response.get("contract_name"), "clarification_response.contract_name", CLARIFICATION_RESPONSE_CONTRACT_NAME)
        _require_exact_string(
            response.get("contract_version"),
            "clarification_response.contract_version",
            CLARIFICATION_RESPONSE_CONTRACT_VERSION,
        )
        fingerprint = _require_non_empty_string(
            response.get("source_clarification_request_fingerprint"),
            "clarification_response.source_clarification_request_fingerprint",
        )
        if not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            raise ValueError("clarification_response_malformed")
        operator_response = _expect_dict(response.get("operator_response"), "clarification_response.operator_response")
        _validate_fields(
            operator_response,
            "clarification_response.operator_response",
            required_fields={"operator_id", "responded_at"},
            allowed_fields={"operator_id", "responded_at"},
        )
        _require_non_empty_string(operator_response.get("operator_id"), "clarification_response.operator_response.operator_id")
        _require_rfc3339(operator_response["responded_at"], "clarification_response.operator_response.responded_at")

        resolved_fields = _expect_dict(response.get("resolved_fields"), "clarification_response.resolved_fields")
        _validate_fields(
            resolved_fields,
            "clarification_response.resolved_fields",
            required_fields={"session_objective", "sections"},
            allowed_fields={"session_objective", "sections"},
        )
        return {
            "source_clarification_request_fingerprint": fingerprint,
            "operator_response": {
                "operator_id": operator_response["operator_id"],
                "responded_at": operator_response["responded_at"],
            },
            "resolved_fields": {
                "session_objective": _require_non_empty_string(
                    resolved_fields.get("session_objective"),
                    "clarification_response.resolved_fields.session_objective",
                ),
                "sections": _expect_list(
                    resolved_fields.get("sections"),
                    "clarification_response.resolved_fields.sections",
                ),
            },
        }
    except ValueError as exc:
        if "unsupported fields" in str(exc):
            raise ValueError("unsupported_fields") from exc
        raise ValueError("clarification_response_malformed") from exc


def _clarification_resolution_blocked(reason_code: str) -> GCEClarificationResolutionResult:
    return GCEClarificationResolutionResult(
        ok=False,
        blocked=True,
        reason_code=reason_code,
        resolved_input=None,
        errors=[_error("clarification_response", reason_code)],
    )


def _parse_common_envelope(payload: dict[str, Any]) -> str:
    _validate_fields(
        payload,
        "gce_ingestion_input",
        required_fields={
            "contract_name",
            "contract_version",
            "input_path",
            "metadata",
            "ambiguities",
        },
        allowed_fields={
            "contract_name",
            "contract_version",
            "input_path",
            "metadata",
            "document",
            "intent",
            "ambiguities",
        },
    )
    _require_exact_string(payload.get("contract_name"), "contract_name", INGESTION_CONTRACT_NAME)
    _require_exact_string(payload.get("contract_version"), "contract_version", INGESTION_CONTRACT_VERSION)
    input_path = _require_enum(payload.get("input_path"), "input_path", ALLOWED_INPUT_PATHS)
    if input_path == INPUT_PATH_FORMAL_GDD and "intent" in payload:
        raise ValueError("formal_gdd input must not include intent")
    if input_path == INPUT_PATH_STRUCTURED_INTENT and "document" in payload:
        raise ValueError("structured_intent input must not include document")
    return input_path


def _parse_input_body(payload: dict[str, Any], input_path: str) -> dict[str, Any]:
    field_name = _body_field(input_path)
    body = _expect_dict(payload.get(field_name), field_name)
    _validate_fields(
        body,
        field_name,
        required_fields={"session_objective", "sections"},
        allowed_fields={"session_objective", "sections"},
    )
    return body


def _body_field(input_path: str) -> str:
    if input_path == INPUT_PATH_FORMAL_GDD:
        return "document"
    return "intent"


def _parse_metadata(raw_value: Any, field_name: str) -> dict[str, str]:
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields=REQUIRED_METADATA_FIELDS,
        allowed_fields=REQUIRED_METADATA_FIELDS,
    )
    metadata = {
        key: _require_non_empty_string(payload.get(key), f"{field_name}.{key}")
        for key in sorted(REQUIRED_METADATA_FIELDS)
    }
    _require_rfc3339(metadata["created_at"], f"{field_name}.created_at")
    _require_rfc3339(metadata["updated_at"], f"{field_name}.updated_at")
    return metadata


def _parse_sections(raw_value: Any, field_name: str) -> list[dict[str, Any]]:
    items = _expect_list(raw_value, field_name)
    if not items:
        raise ValueError(f"{field_name} must contain at least one section")

    seen_section_ids: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        section = _expect_dict(item, item_field)
        _validate_fields(
            section,
            item_field,
            required_fields={
                "section_id",
                "title",
                "classification",
                "authorship",
                "required",
                "content",
            },
            allowed_fields={
                "section_id",
                "title",
                "classification",
                "authorship",
                "required",
                "content",
            },
        )
        section_id = _require_non_empty_string(section.get("section_id"), f"{item_field}.section_id")
        if section_id in seen_section_ids:
            raise ValueError(f"{item_field}.section_id must be unique")
        seen_section_ids.add(section_id)

        classification = _require_enum(
            section.get("classification"),
            f"{item_field}.classification",
            SECTION_CLASSIFICATIONS,
        )
        authorship = _require_enum(
            section.get("authorship"),
            f"{item_field}.authorship",
            SECTION_AUTHORSHIP,
        )
        if SECTION_CLASSIFICATION_AUTHORSHIP[classification] != authorship:
            raise ValueError(f"{item_field}.authorship must match section classification")

        content = _expect_dict(section.get("content"), f"{item_field}.content")
        if not content:
            raise ValueError(f"{item_field}.content must be non-empty")

        normalized.append(
            {
                "section_id": section_id,
                "title": _require_non_empty_string(section.get("title"), f"{item_field}.title"),
                "classification": classification,
                "authorship": authorship,
                "required": _require_bool(section.get("required"), f"{item_field}.required"),
                "content": dict(content),
            }
        )
    return normalized


def _parse_ambiguities(raw_value: Any, field_name: str) -> list[dict[str, Any]]:
    items = _expect_list(raw_value, field_name)
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        ambiguity = _expect_dict(item, item_field)
        _validate_fields(
            ambiguity,
            item_field,
            required_fields={"field_path", "question", "blocking"},
            allowed_fields={"field_path", "question", "blocking"},
        )
        blocking = _require_bool(ambiguity.get("blocking"), f"{item_field}.blocking")
        if blocking is not True:
            raise ValueError(f"{item_field}.blocking must be true")
        normalized.append(
            {
                "field_path": _require_non_empty_string(
                    ambiguity.get("field_path"),
                    f"{item_field}.field_path",
                ),
                "question": _require_non_empty_string(
                    ambiguity.get("question"),
                    f"{item_field}.question",
                ),
                "blocking": True,
            }
        )
    return normalized


def _build_normalized_session_intent(
    *,
    input_path: str,
    metadata: dict[str, str],
    session_objective: str,
    sections: list[dict[str, Any]],
) -> dict[str, Any]:
    section_classifications = {
        section["section_id"]: {
            "classification": section["classification"],
            "authorship": section["authorship"],
            "required": section["required"],
        }
        for section in sections
    }
    return {
        "contract_name": SESSION_INTENT_CONTRACT_NAME,
        "contract_version": SESSION_INTENT_CONTRACT_VERSION,
        "source_input_path": input_path,
        "metadata": dict(metadata),
        "session_objective": session_objective,
        "sections": [dict(section) for section in sections],
        "section_classifications": section_classifications,
        "stage_1_release": {
            "blocked": False,
            "reason_code": None,
        },
    }


def _build_clarification_request(
    *,
    input_path: str,
    metadata: dict[str, str],
    ambiguities: list[dict[str, Any]],
) -> dict[str, Any]:
    questions = [
        {
            "id": f"clarification-{index + 1:03d}",
            "field_path": ambiguity["field_path"],
            "question": ambiguity["question"],
            "blocking": True,
        }
        for index, ambiguity in enumerate(ambiguities)
    ]
    return {
        "contract_name": CLARIFICATION_CONTRACT_NAME,
        "contract_version": CLARIFICATION_CONTRACT_VERSION,
        "artifact_type": "clarification_request",
        "source_input_path": input_path,
        "stage_1_release_blocked": True,
        "reason_code": "unresolved_intent",
        "metadata": dict(metadata),
        "questions": questions,
    }


def _expect_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        if isinstance(value, str):
            raise ValueError(f"{field_name} natural_language_parsing_not_implemented")
        raise ValueError(f"{field_name} must be a dict")
    return dict(value)


def _expect_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return list(value)


def _validate_fields(
    payload: dict[str, Any],
    field_name: str,
    *,
    required_fields: set[str],
    allowed_fields: set[str],
) -> None:
    missing_fields = sorted(field for field in required_fields if field not in payload)
    if missing_fields:
        raise ValueError(f"{field_name} missing required fields: {', '.join(missing_fields)}")
    extra_fields = sorted(field for field in payload if field not in allowed_fields)
    if extra_fields:
        raise ValueError(f"{field_name} includes unsupported fields: {', '.join(extra_fields)}")


def _require_exact_string(value: Any, field_name: str, expected: str) -> str:
    normalized = _require_non_empty_string(value, field_name)
    if normalized != expected:
        raise ValueError(f"{field_name} must be {expected}")
    return normalized


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_enum(value: Any, field_name: str, allowed_values: set[str]) -> str:
    normalized = _require_non_empty_string(value, field_name)
    if normalized not in allowed_values:
        raise ValueError(f"{field_name} must be one of the allowed contract values")
    return normalized


def _require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a bool")
    return value


def _require_rfc3339(value: str, field_name: str) -> None:
    if not RFC3339_PATTERN.match(value):
        raise ValueError(f"{field_name} must be RFC 3339")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be RFC 3339") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must be RFC 3339")


def _error(field_name: str, condition: str) -> dict[str, str]:
    return {
        "field": field_name,
        "condition": condition,
        "severity": "HARD",
    }


__all__ = [
    "CLARIFICATION_CONTRACT_NAME",
    "CLARIFICATION_CONTRACT_VERSION",
    "CLARIFICATION_RESPONSE_CONTRACT_NAME",
    "CLARIFICATION_RESPONSE_CONTRACT_VERSION",
    "GCEClarificationResolutionResult",
    "GCEIngestionValidationResult",
    "INGESTION_CONTRACT_NAME",
    "INGESTION_CONTRACT_VERSION",
    "INPUT_PATH_FORMAL_GDD",
    "INPUT_PATH_STRUCTURED_INTENT",
    "SESSION_INTENT_CONTRACT_NAME",
    "SESSION_INTENT_CONTRACT_VERSION",
    "compute_gce_clarification_request_fingerprint",
    "resolve_gce_clarification_response",
    "validate_gce_ingestion_input",
]
