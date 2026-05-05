"""Deterministic builder for the locked Game Adapter Stage 4 approval contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "dgce-contracts"
    / "schemas"
    / "game_adapter"
    / "stage4_approval.v1.schema.json"
)

APPROVAL_STATUSES = {"approved", "rejected", "blocked"}
EVIDENCE_SOURCES = {
    "review_bundle",
    "preview",
    "unreal_manifest",
    "symbol_candidate_index",
    "resolver",
    "alignment",
    "operator_context",
}
FORBIDDEN_RUNTIME_ACTIONS = [
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
FORBIDDEN_INPUT_FIELDS = {
    "approval_granted",
    "auto_approved",
    "blueprint_mutation",
    "blueprint_mutations",
    "execute",
    "execution_permission",
    "execution_permitted",
    "execution_status",
    "freeform_llm_reasoning",
    "full_symbol_table",
    "guardrail_policy_decision",
    "policy_decision",
    "provider_output",
    "raw_model_text",
    "raw_preview",
    "raw_resolver_payload",
    "raw_review_bundle",
    "raw_symbols",
    "resolver_payload",
    "selected_mode",
    "simulation_result",
    "simulation_status",
    "stage6_gate_decision",
    "stage7_alignment_result",
    "stage8_execution_stamp",
    "write_directives",
    "write_instructions",
    "write_targets",
    "written_files",
}


def build_stage4_approval_v1(
    *,
    approval_id: str,
    section_id: str,
    created_at: str,
    approved_by: str,
    approval_status: str,
    source_review_id: str,
    source_review_fingerprint: str,
    source_preview_fingerprint: str,
    operator_summary: str,
    approval_scope_summary: str,
    risk_acknowledgement: str,
    evidence: Sequence[Mapping[str, Any]],
    source_input_fingerprint: str | None = None,
    approved_change_ids: Sequence[str] | None = None,
    rejected_change_ids: Sequence[str] | None = None,
    approval_constraints: Sequence[str] | None = None,
    captured_review_fingerprint: str | None = None,
    captured_preview_fingerprint: str | None = None,
    captured_input_fingerprint: str | None = None,
    stale_check_required: bool = True,
) -> dict[str, Any]:
    """Build a contract-valid Stage 4 approval from explicit operator input."""
    normalized_status = _approval_status(approval_status)
    normalized_approved_change_ids = _normalize_identifier_list(
        approved_change_ids or [],
        "approved_change_ids",
        max_items=100,
    )
    normalized_rejected_change_ids = _normalize_identifier_list(
        rejected_change_ids or [],
        "rejected_change_ids",
        max_items=100,
    )
    if normalized_status in {"rejected", "blocked"} and normalized_approved_change_ids:
        raise ValueError(f"{normalized_status} approvals must not include approved_change_ids")
    if stale_check_required is not True:
        raise ValueError("stale_check_required must be true")

    normalized_source_review_fingerprint = _fingerprint(
        source_review_fingerprint,
        "source_review_fingerprint",
    )
    normalized_source_preview_fingerprint = _fingerprint(
        source_preview_fingerprint,
        "source_preview_fingerprint",
    )
    normalized_source_input_fingerprint = _nullable_fingerprint(
        source_input_fingerprint,
        "source_input_fingerprint",
    )

    approval = {
        "artifact_type": "game_adapter_stage4_approval",
        "contract_name": "DGCEGameAdapterStage4Approval",
        "contract_version": "dgce.game_adapter.stage4.approval.v1",
        "adapter": "game",
        "domain": "game_adapter",
        "approval_id": _safe_identifier(approval_id, "approval_id"),
        "section_id": _safe_identifier(section_id, "section_id"),
        "created_at": _required_single_line_string(created_at, "created_at", max_length=64),
        "approved_by": _safe_identifier(approved_by, "approved_by"),
        "approval_status": normalized_status,
        "source_review_id": _safe_identifier(source_review_id, "source_review_id"),
        "source_review_fingerprint": normalized_source_review_fingerprint,
        "source_preview_fingerprint": normalized_source_preview_fingerprint,
        "source_input_fingerprint": normalized_source_input_fingerprint,
        "approved_change_ids": normalized_approved_change_ids,
        "rejected_change_ids": normalized_rejected_change_ids,
        "approval_summary": {
            "operator_summary": _bounded_text(operator_summary, "operator_summary"),
            "approval_scope_summary": _bounded_text(approval_scope_summary, "approval_scope_summary"),
            "risk_acknowledgement": _bounded_text(risk_acknowledgement, "risk_acknowledgement"),
        },
        "stale_detection": {
            "captured_review_fingerprint": _fingerprint(
                captured_review_fingerprint or normalized_source_review_fingerprint,
                "captured_review_fingerprint",
            ),
            "captured_preview_fingerprint": _fingerprint(
                captured_preview_fingerprint or normalized_source_preview_fingerprint,
                "captured_preview_fingerprint",
            ),
            "captured_input_fingerprint": _nullable_fingerprint(
                (
                    captured_input_fingerprint
                    if captured_input_fingerprint is not None
                    else normalized_source_input_fingerprint
                ),
                "captured_input_fingerprint",
            ),
            "stale_check_required": stale_check_required,
        },
        "approval_constraints": _normalize_bounded_text_list(
            approval_constraints or [],
            "approval_constraints",
            max_items=50,
        ),
        "evidence": _normalize_evidence(evidence),
        "forbidden_runtime_actions": list(FORBIDDEN_RUNTIME_ACTIONS),
    }
    validate_stage4_approval_v1(approval)
    return approval


def validate_stage4_approval_v1(approval: Mapping[str, Any]) -> bool:
    """Validate a Stage 4 approval against the locked schema."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(dict(approval)), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.path) or "<root>"
        raise ValueError(f"stage4_approval invalid at {path}: {first.message}")
    return True


def _approval_status(value: Any) -> str:
    status = _required_single_line_string(value, "approval_status", max_length=64)
    if status not in APPROVAL_STATUSES:
        raise ValueError("approval_status must be approved, rejected, or blocked")
    return status


def _normalize_evidence(evidence: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    if not isinstance(evidence, Sequence) or isinstance(evidence, (str, bytes)):
        raise ValueError("evidence must be an array of objects")
    if not evidence:
        raise ValueError("evidence must contain at least one item")
    if len(evidence) > 100:
        raise ValueError("evidence must contain at most 100 items")
    normalized_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for index, item in enumerate(evidence):
        if not isinstance(item, Mapping):
            raise ValueError(f"evidence[{index}] must be an object")
        _reject_forbidden_mapping_fields(item, f"evidence[{index}]")
        allowed_keys = {"source", "reference", "snippet_hash"}
        extra = sorted(set(item) - allowed_keys)
        if extra:
            raise ValueError(f"evidence[{index}] contains unsupported fields: {', '.join(extra)}")
        source = _required_single_line_string(item.get("source"), f"evidence[{index}].source", max_length=64)
        if source not in EVIDENCE_SOURCES:
            raise ValueError(f"evidence[{index}].source unsupported")
        reference = _bounded_text(item.get("reference"), f"evidence[{index}].reference")
        normalized = {"source": source, "reference": reference}
        snippet_hash = item.get("snippet_hash")
        if snippet_hash is not None:
            normalized["snippet_hash"] = _fingerprint(snippet_hash, f"evidence[{index}].snippet_hash")
        normalized_by_key[(source, reference)] = normalized
    return [normalized_by_key[key] for key in sorted(normalized_by_key)]


def _normalize_identifier_list(values: Sequence[str], field_name: str, *, max_items: int) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be an array of strings")
    if len(values) > max_items:
        raise ValueError(f"{field_name} must contain at most {max_items} items")
    return _sorted_unique(_safe_identifier(value, f"{field_name}[]") for value in values)


def _normalize_bounded_text_list(values: Sequence[str], field_name: str, *, max_items: int) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be an array of strings")
    if len(values) > max_items:
        raise ValueError(f"{field_name} must contain at most {max_items} items")
    return _sorted_unique(_bounded_text(value, f"{field_name}[]") for value in values)


def _sorted_unique(values: Sequence[str]) -> list[str]:
    return sorted(dict.fromkeys(str(value) for value in values))


def _reject_forbidden_mapping_fields(value: Mapping[str, Any], field_name: str) -> None:
    forbidden = sorted(set(value).intersection(FORBIDDEN_INPUT_FIELDS))
    if forbidden:
        raise ValueError(f"{field_name} contains forbidden runtime fields: {', '.join(forbidden)}")


def _safe_identifier(value: Any, field_name: str) -> str:
    return _required_single_line_string(value, field_name, max_length=128)


def _nullable_fingerprint(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _fingerprint(value, field_name)


def _fingerprint(value: Any, field_name: str) -> str:
    normalized = _required_single_line_string(value, field_name, max_length=64)
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise ValueError(f"{field_name} must be a lowercase sha256 fingerprint")
    return normalized


def _bounded_text(value: Any, field_name: str) -> str:
    return _required_single_line_string(value, field_name, max_length=512)


def _required_single_line_string(value: Any, field_name: str, *, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if "\n" in normalized or "\r" in normalized:
        raise ValueError(f"{field_name} must be single-line")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} must be at most {max_length} characters")
    return normalized


__all__ = [
    "FORBIDDEN_RUNTIME_ACTIONS",
    "build_stage4_approval_v1",
    "validate_stage4_approval_v1",
]
