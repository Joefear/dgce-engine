"""Deterministic builder for the locked Game Adapter Stage 3 review bundle."""

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
    / "stage3_review_bundle.v1.schema.json"
)

FORBIDDEN_RUNTIME_ACTIONS = [
    "no_approval_granted",
    "no_execution_performed",
    "no_stage8_write_instructions",
    "no_blueprint_mutation",
    "no_unreal_project_writes",
    "no_binary_blueprint_parsing",
    "no_simulation_run",
    "no_guardrail_policy_decision",
]

FORBIDDEN_INPUT_FIELDS = {
    "approval_status",
    "approval_granted",
    "approved",
    "execution_permitted",
    "execution_permission",
    "execution_status",
    "stage8_write_instructions",
    "write_instructions",
    "written_files",
    "blueprint_mutation",
    "blueprint_mutations",
    "simulation_result",
    "simulation_status",
    "guardrail_policy_decision",
    "policy_decision",
    "raw_model_text",
    "provider_output",
    "raw_preview",
    "full_symbol_table",
    "raw_resolver_payload",
}

TARGET_KIND_BY_STAGE2_KIND = {
    "blueprint": "blueprint",
    "Blueprint": "blueprint",
    "BlueprintClass": "blueprint",
    "ActorComponent": "blueprint",
    "Variable": "blueprint",
    "Event": "blueprint",
    "Binding": "blueprint",
    "InputAction": "asset",
    "cpp": "cpp",
    "CppClass": "cpp",
    "C++": "cpp",
    "asset": "asset",
    "Asset": "asset",
    "config": "config",
    "Config": "config",
    "documentation": "documentation",
    "Documentation": "documentation",
    "unknown": "unknown",
}
OPERATION_BY_VALUE = {
    "create": "create",
    "modify": "modify",
    "ignore": "ignore",
    "no_change": "ignore",
    "skip": "ignore",
}
OUTPUT_STRATEGY_BY_VALUE = {
    "Blueprint": "blueprint",
    "blueprint": "blueprint",
    "C++": "cpp",
    "cpp": "cpp",
    "both": "both",
    "none": "none",
    "unknown": "unknown",
}
EVIDENCE_SOURCES = {
    "preview",
    "unreal_manifest",
    "symbol_candidate_index",
    "resolver",
    "alignment",
    "operator_context",
}


def build_stage3_review_bundle_v1(
    *,
    review_id: str,
    section_id: str,
    created_at: str,
    source_preview_fingerprint: str,
    source_input_fingerprint: str | None = None,
    planned_changes: Sequence[Mapping[str, Any]] | None = None,
    review_summary: Mapping[str, Any] | None = None,
    dependency_notes: Sequence[str] | None = None,
    operator_questions: Sequence[str] | None = None,
    evidence: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a contract-valid Stage 3 review bundle from bounded structured input."""
    _reject_forbidden_mapping_fields(review_summary or {}, "review_summary")
    normalized_dependency_notes = _normalize_bounded_text_list(dependency_notes or [], "dependency_notes", max_items=100)
    normalized_operator_questions = _normalize_bounded_text_list(operator_questions or [], "operator_questions", max_items=50)
    normalized_evidence = _normalize_evidence(evidence)

    proposed_changes, generated_questions, blocking_issues, informational_issues = _normalize_planned_changes(
        planned_changes or []
    )
    all_operator_questions = _sorted_unique([*normalized_operator_questions, *generated_questions])
    blocking_count = min(100, blocking_issues + (1 if all_operator_questions else 0))
    informational_count = min(100, informational_issues)
    blocked = blocking_count > 0

    bundle = {
        "artifact_type": "game_adapter_stage3_review_bundle",
        "contract_name": "DGCEGameAdapterStage3ReviewBundle",
        "contract_version": "dgce.game_adapter.stage3.review_bundle.v1",
        "adapter": "game",
        "domain": "game_adapter",
        "review_id": _required_single_line_string(review_id, "review_id", max_length=128),
        "section_id": _required_single_line_string(section_id, "section_id", max_length=128),
        "created_at": _required_single_line_string(created_at, "created_at", max_length=64),
        "source_preview_fingerprint": _fingerprint(source_preview_fingerprint, "source_preview_fingerprint"),
        "source_input_fingerprint": (
            _fingerprint(source_input_fingerprint, "source_input_fingerprint")
            if source_input_fingerprint is not None
            else None
        ),
        "review_status": "blocked" if blocked else "ready_for_operator_review",
        "review_summary": _normalize_review_summary(
            review_summary,
            section_id=section_id,
            change_count=len(proposed_changes),
            blocked=blocked,
        ),
        "proposed_changes": proposed_changes,
        "dependency_notes": normalized_dependency_notes,
        "operator_questions": all_operator_questions,
        "approval_readiness": {
            "ready_for_approval": not blocked,
            "blocking_review_issues_count": blocking_count,
            "informational_review_issues_count": informational_count,
        },
        "evidence": normalized_evidence,
        "forbidden_runtime_actions": list(FORBIDDEN_RUNTIME_ACTIONS),
    }
    validate_stage3_review_bundle_v1(bundle)
    return bundle


def validate_stage3_review_bundle_v1(bundle: Mapping[str, Any]) -> bool:
    """Validate a Stage 3 review bundle against the locked schema."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(dict(bundle)), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.path) or "<root>"
        raise ValueError(f"stage3_review_bundle invalid at {path}: {first.message}")
    return True


def _normalize_planned_changes(
    planned_changes: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, str]], list[str], int, int]:
    if not isinstance(planned_changes, Sequence) or isinstance(planned_changes, (str, bytes)):
        raise ValueError("planned_changes must be an array of objects")
    proposed_changes: list[dict[str, str]] = []
    operator_questions: list[str] = []
    blocking_issues = 0
    informational_issues = 0
    for index, raw_change in enumerate(planned_changes):
        if not isinstance(raw_change, Mapping):
            raise ValueError(f"planned_changes[{index}] must be an object")
        _reject_forbidden_mapping_fields(raw_change, f"planned_changes[{index}]")
        target = raw_change.get("target")
        if target is not None:
            if not isinstance(target, Mapping):
                raise ValueError(f"planned_changes[{index}].target must be an object")
            _reject_forbidden_mapping_fields(target, f"planned_changes[{index}].target")

        missing_fields: list[str] = []
        change_id = _optional_single_line_string(raw_change.get("change_id"), f"planned_changes[{index}].change_id")
        if change_id is None:
            change_id = f"change.{index + 1:03d}.missing-id"
            missing_fields.append("change_id")

        target_path = _extract_target_path(raw_change, target)
        if target_path is None:
            target_path = f"unknown/target-{index + 1:03d}"
            missing_fields.append("target_path")

        target_kind = _normalize_target_kind(_extract_target_kind(raw_change, target))
        if target_kind == "unknown":
            informational_issues += 1

        operation = _normalize_operation(raw_change.get("operation"))
        if operation is None:
            operation = "ignore"
            missing_fields.append("operation")

        output_strategy = _normalize_output_strategy(raw_change.get("output_strategy", raw_change.get("strategy")))
        if output_strategy == "unknown":
            informational_issues += 1

        review_risk = _normalize_review_risk(raw_change, target_kind=target_kind, operation=operation)
        summary = _change_summary(change_id=change_id, target_path=target_path, operation=operation, output_strategy=output_strategy)
        proposed_changes.append(
            {
                "change_id": change_id,
                "target_path": target_path,
                "target_kind": target_kind,
                "operation": operation,
                "output_strategy": output_strategy,
                "human_readable_summary": summary,
                "review_risk": review_risk,
            }
        )
        if review_risk == "high":
            informational_issues += 1
        if missing_fields:
            blocking_issues += 1
            operator_questions.append(
                f"Provide structured {', '.join(missing_fields)} for {change_id} before approval review can proceed."
            )
    if not proposed_changes:
        operator_questions.append("Provide at least one structured Stage 2 planned change before approval review can proceed.")
    return sorted(proposed_changes, key=lambda item: item["change_id"]), operator_questions, blocking_issues, informational_issues


def _normalize_review_summary(
    review_summary: Mapping[str, Any] | None,
    *,
    section_id: str,
    change_count: int,
    blocked: bool,
) -> dict[str, str]:
    provided = dict(review_summary or {})
    allowed_keys = {"title", "primary_intent", "operator_summary", "risk_summary"}
    extra = sorted(set(provided) - allowed_keys)
    if extra:
        raise ValueError(f"review_summary contains unsupported fields: {', '.join(extra)}")
    default_risk = (
        "Review is blocked because required structured review data is missing."
        if blocked
        else "Review bundle is bounded and does not approve, execute, simulate, mutate, or write project files."
    )
    return {
        "title": _bounded_text(
            provided.get("title") or f"Game Adapter Review for {section_id}",
            "review_summary.title",
        ),
        "primary_intent": _bounded_text(
            provided.get("primary_intent") or "Review Stage 2 planned game adapter changes before approval.",
            "review_summary.primary_intent",
        ),
        "operator_summary": _bounded_text(
            provided.get("operator_summary") or f"Review {change_count} proposed Stage 2 change(s).",
            "review_summary.operator_summary",
        ),
        "risk_summary": _bounded_text(provided.get("risk_summary") or default_risk, "review_summary.risk_summary"),
    }


def _normalize_evidence(evidence: Sequence[Mapping[str, Any]] | None) -> list[dict[str, str]]:
    if evidence is None:
        return []
    if not isinstance(evidence, Sequence) or isinstance(evidence, (str, bytes)):
        raise ValueError("evidence must be an array of objects")
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


def _extract_target_path(raw_change: Mapping[str, Any], target: Any) -> str | None:
    path = raw_change.get("target_path")
    if path is None and isinstance(target, Mapping):
        path = target.get("target_path")
    return _optional_single_line_string(path, "target_path", max_length=260)


def _extract_target_kind(raw_change: Mapping[str, Any], target: Any) -> Any:
    if raw_change.get("target_kind") is not None:
        return raw_change.get("target_kind")
    if isinstance(target, Mapping):
        return target.get("target_kind")
    return None


def _normalize_target_kind(value: Any) -> str:
    if not isinstance(value, str):
        return "unknown"
    return TARGET_KIND_BY_STAGE2_KIND.get(value, TARGET_KIND_BY_STAGE2_KIND.get(value.strip(), "unknown"))


def _normalize_operation(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return OPERATION_BY_VALUE.get(value.strip())


def _normalize_output_strategy(value: Any) -> str:
    if not isinstance(value, str):
        return "unknown"
    return OUTPUT_STRATEGY_BY_VALUE.get(value.strip(), "unknown")


def _normalize_review_risk(raw_change: Mapping[str, Any], *, target_kind: str, operation: str) -> str:
    explicit_risk = raw_change.get("review_risk")
    if isinstance(explicit_risk, str) and explicit_risk in {"low", "medium", "high"}:
        return explicit_risk
    summary = raw_change.get("summary")
    if isinstance(summary, Mapping):
        _reject_forbidden_mapping_fields(summary, "planned_change.summary")
        summary_risk = summary.get("risk")
        if isinstance(summary_risk, str) and summary_risk in {"low", "medium", "high"}:
            return summary_risk
    if raw_change.get("risk") == "high" or raw_change.get("explicitly_risky") is True:
        return "high"
    if operation == "ignore" or target_kind == "documentation":
        return "low"
    return "medium"


def _change_summary(*, change_id: str, target_path: str, operation: str, output_strategy: str) -> str:
    return _bounded_text(
        f"Review {operation} change {change_id} for {target_path} using {output_strategy} output strategy.",
        "human_readable_summary",
    )


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


def _optional_single_line_string(value: Any, field_name: str, *, max_length: int = 128) -> str | None:
    if value is None:
        return None
    return _required_single_line_string(value, field_name, max_length=max_length)


__all__ = [
    "FORBIDDEN_RUNTIME_ACTIONS",
    "build_stage3_review_bundle_v1",
    "validate_stage3_review_bundle_v1",
]
