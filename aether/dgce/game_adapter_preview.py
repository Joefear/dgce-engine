"""Game Adapter Stage 2 preview-only contract helpers.

This module defines the isolated preview artifact contract for the authorized
Game Adapter Stage 2 slice. It intentionally does not inspect Unreal projects,
resolve symbols, validate Blueprint graphs, or perform execution writes.
"""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any, Mapping

from aether.dgce.decompose import compute_json_payload_fingerprint


ARTIFACT_TYPE = "game_adapter_stage2_preview"
CONTRACT_NAME = "DGCEGameAdapterStage2Preview"
CONTRACT_VERSION = "dgce.game_adapter.stage2.preview.v1"
ADAPTER = "game"
DOMAIN = "game_adapter"

ALLOWED_OPERATIONS = ("create", "modify", "delete")
ALLOWED_DOMAIN_TYPES = (
    "Blueprint",
    "C++",
    "component",
    "variable",
    "event",
    "binding",
    "asset",
    "input_action",
)
ALLOWED_STRATEGIES = ("Blueprint", "C++", "both")
ALLOWED_TARGET_KINDS = (
    "BlueprintClass",
    "CppClass",
    "ActorComponent",
    "Variable",
    "Event",
    "Binding",
    "Asset",
    "InputAction",
)
ALLOWED_SUMMARY_INTENTS = (
    "add_gameplay_capability",
    "adjust_existing_behavior",
    "remove_deprecated_behavior",
    "connect_existing_systems",
    "prepare_for_review",
)
ALLOWED_SUMMARY_IMPACTS = ("gameplay", "ui", "data", "input", "none")
ALLOWED_SUMMARY_RISKS = ("low", "medium", "high")
ALLOWED_SUMMARY_REVIEW_FOCUS = (
    "asset_scope",
    "logic_flow",
    "data_shape",
    "event_binding",
    "component_setup",
)
ALLOWED_POLICY_PACKS = ("game_adapter_stage2_preview", "game_adapter_stage2_guardrail")
SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_./:+#-]+$")
MAX_IDENTIFIER_LENGTH = 180

_PLANNED_CHANGE_KEYS = {
    "change_id",
    "target",
    "operation",
    "domain_type",
    "strategy",
    "summary",
}
_TARGET_KEYS = {"target_id", "target_path", "target_kind"}
_SUMMARY_KEYS = {"intent", "impact", "risk", "review_focus"}
_GOVERNANCE_KEYS = {"policy_pack", "guardrail_required"}
_TOP_LEVEL_KEYS = {
    "artifact_type",
    "contract_name",
    "contract_version",
    "adapter",
    "domain",
    "source_stage0_fingerprint",
    "source_input_reference",
    "planned_changes",
    "governance_context",
    "machine_view",
    "human_view",
    "artifact_fingerprint",
}
_FORBIDDEN_RAW_KEYS = {
    "raw_text",
    "raw_model_text",
    "raw_model_output",
    "raw_provider_text",
    "raw_provider_output",
    "provider_output",
    "provider_response",
    "model_output",
    "stdout",
    "stderr",
    "stack_trace",
    "traceback",
    "freeform",
    "free_form",
}


def build_game_adapter_stage2_preview(
    *,
    planned_changes: list[Mapping[str, Any]],
    source_stage0_fingerprint: str | None = None,
    source_input_reference: str | None = None,
    policy_pack: str = "game_adapter_stage2_preview",
    guardrail_required: bool = True,
) -> dict[str, Any]:
    """Build a deterministic preview-only Game Adapter Stage 2 artifact."""
    governance_context = _normalize_governance_context(
        {
            "policy_pack": policy_pack,
            "guardrail_required": guardrail_required,
        }
    )
    source_stage0_fingerprint = _normalize_optional_reference(
        source_stage0_fingerprint,
        "source_stage0_fingerprint",
    )
    source_input_reference = _normalize_optional_reference(source_input_reference, "source_input_reference")
    if source_stage0_fingerprint is None and source_input_reference is None:
        raise ValueError("source_stage0_fingerprint or source_input_reference is required")

    selected_changes = apply_game_adapter_stage2_strategy_selection(planned_changes)
    normalized_changes = _normalize_planned_changes(selected_changes)
    payload = {
        "artifact_type": ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": ADAPTER,
        "domain": DOMAIN,
        "source_stage0_fingerprint": source_stage0_fingerprint,
        "source_input_reference": source_input_reference,
        "planned_changes": normalized_changes,
        "governance_context": governance_context,
        "machine_view": render_game_adapter_stage2_machine_view(normalized_changes, governance_context),
        "human_view": render_game_adapter_stage2_human_view(normalized_changes, governance_context),
    }
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    validate_game_adapter_stage2_preview_contract(payload)
    return payload


def validate_game_adapter_stage2_preview_contract(payload: Mapping[str, Any]) -> bool:
    """Validate the Game Adapter Stage 2 preview contract fail-closed."""
    if not isinstance(payload, Mapping):
        raise ValueError("preview contract must be an object")
    _reject_forbidden_raw_keys(payload, "preview")
    _require_exact_keys(payload, _TOP_LEVEL_KEYS, "preview")
    _require_exact(payload.get("artifact_type"), ARTIFACT_TYPE, "artifact_type")
    _require_exact(payload.get("contract_name"), CONTRACT_NAME, "contract_name")
    _require_exact(payload.get("contract_version"), CONTRACT_VERSION, "contract_version")
    _require_exact(payload.get("adapter"), ADAPTER, "adapter")
    _require_exact(payload.get("domain"), DOMAIN, "domain")
    source_stage0_fingerprint = _normalize_optional_reference(
        payload.get("source_stage0_fingerprint"),
        "source_stage0_fingerprint",
    )
    source_input_reference = _normalize_optional_reference(payload.get("source_input_reference"), "source_input_reference")
    if source_stage0_fingerprint is None and source_input_reference is None:
        raise ValueError("source_stage0_fingerprint or source_input_reference is required")

    governance_context = _normalize_governance_context(_expect_mapping(payload.get("governance_context"), "governance_context"))
    planned_changes = _normalize_planned_changes(_expect_list(payload.get("planned_changes"), "planned_changes"))
    apply_game_adapter_stage2_strategy_selection(planned_changes)
    if payload.get("planned_changes") != planned_changes:
        raise ValueError("planned_changes must be canonical")
    if payload.get("governance_context") != governance_context:
        raise ValueError("governance_context must be canonical")
    expected_machine_view = render_game_adapter_stage2_machine_view(planned_changes, governance_context)
    expected_human_view = render_game_adapter_stage2_human_view(planned_changes, governance_context)
    if payload.get("machine_view") != expected_machine_view:
        raise ValueError("machine_view must be deterministically derived from planned_changes")
    if payload.get("human_view") != expected_human_view:
        raise ValueError("human_view must be deterministically derived from planned_changes")
    artifact_fingerprint = payload.get("artifact_fingerprint")
    if not isinstance(artifact_fingerprint, str) or not artifact_fingerprint:
        raise ValueError("artifact_fingerprint is required")
    if artifact_fingerprint != compute_json_payload_fingerprint(dict(payload)):
        raise ValueError("artifact_fingerprint invalid")
    return True


def select_game_adapter_stage2_strategy(planned_change: Mapping[str, Any]) -> str:
    """Select the preview-only Blueprint/C++ output strategy from bounded fields."""
    if not isinstance(planned_change, Mapping):
        raise ValueError("planned_change must be an object")
    domain_type = planned_change.get("domain_type")
    if not isinstance(domain_type, str) or domain_type not in ALLOWED_DOMAIN_TYPES:
        raise ValueError("planned_change.domain_type is unsupported")
    target = planned_change.get("target")
    if not isinstance(target, Mapping):
        raise ValueError("planned_change.target must be an object")
    target_kind = target.get("target_kind")
    if not isinstance(target_kind, str) or target_kind not in ALLOWED_TARGET_KINDS:
        raise ValueError("planned_change.target.target_kind is unsupported")
    summary = planned_change.get("summary")
    if not isinstance(summary, Mapping):
        raise ValueError("planned_change.summary must be an object")
    intent = summary.get("intent")
    review_focus = summary.get("review_focus")
    if not isinstance(intent, str) or intent not in ALLOWED_SUMMARY_INTENTS:
        raise ValueError("planned_change.summary.intent is unsupported")
    if not isinstance(review_focus, str) or review_focus not in ALLOWED_SUMMARY_REVIEW_FOCUS:
        raise ValueError("planned_change.summary.review_focus is unsupported")

    selected = _strategy_from_bounded_fields(
        domain_type=domain_type,
        target_kind=target_kind,
        intent=intent,
        review_focus=review_focus,
    )
    explicit_strategy = planned_change.get("strategy")
    if explicit_strategy is None:
        return selected
    if not isinstance(explicit_strategy, str) or explicit_strategy not in ALLOWED_STRATEGIES:
        raise ValueError("planned_change.strategy is unsupported")
    if explicit_strategy != selected:
        raise ValueError("planned_change.strategy does not match selected strategy")
    return explicit_strategy


def apply_game_adapter_stage2_strategy_selection(
    planned_changes: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return planned changes with selected strategies recorded before preview build."""
    if not isinstance(planned_changes, list) or not planned_changes:
        raise ValueError("planned_changes must be a non-empty array")
    selected_changes: list[dict[str, Any]] = []
    for change in planned_changes:
        if not isinstance(change, Mapping):
            raise ValueError("planned_changes entries must be objects")
        selected = dict(change)
        selected["strategy"] = select_game_adapter_stage2_strategy(change)
        selected_changes.append(selected)
    return selected_changes


def _strategy_from_bounded_fields(
    *,
    domain_type: str,
    target_kind: str,
    intent: str,
    review_focus: str,
) -> str:
    if domain_type == "C++" or target_kind == "CppClass":
        if domain_type != "C++" or target_kind != "CppClass":
            raise ValueError("planned_change strategy fields are ambiguous")
        return "C++"
    if domain_type == "Blueprint" or target_kind == "BlueprintClass":
        if domain_type != "Blueprint" or target_kind != "BlueprintClass":
            raise ValueError("planned_change strategy fields are ambiguous")
        return "Blueprint"
    if domain_type in {"binding", "asset", "input_action"}:
        if target_kind not in {"Binding", "Asset", "InputAction"}:
            raise ValueError("planned_change strategy fields are ambiguous")
        return "Blueprint"
    if domain_type in {"component", "variable", "event"}:
        if target_kind not in {"ActorComponent", "Variable", "Event"}:
            raise ValueError("planned_change strategy fields are ambiguous")
        if intent == "prepare_for_review" and review_focus == "logic_flow":
            return "both"
        return "Blueprint"
    raise ValueError("planned_change.domain_type is unsupported")


def render_game_adapter_stage2_machine_view(
    planned_changes: list[Mapping[str, Any]],
    governance_context: Mapping[str, Any],
) -> dict[str, Any]:
    """Render the deterministic machine view consumed by DGCE/Guardrail."""
    normalized_changes = _normalize_planned_changes(planned_changes)
    normalized_governance = _normalize_governance_context(governance_context)
    return {
        "view_type": "machine",
        "contract_version": CONTRACT_VERSION,
        "adapter": ADAPTER,
        "domain": DOMAIN,
        "guardrail_required": normalized_governance["guardrail_required"],
        "policy_pack": normalized_governance["policy_pack"],
        "change_count": len(normalized_changes),
        "changes": [
            {
                "change_id": change["change_id"],
                "target_id": change["target"]["target_id"],
                "target_path": change["target"]["target_path"],
                "target_kind": change["target"]["target_kind"],
                "operation": change["operation"],
                "domain_type": change["domain_type"],
                "strategy": change["strategy"],
                "summary_codes": dict(change["summary"]),
            }
            for change in normalized_changes
        ],
    }


def render_game_adapter_stage2_human_view(
    planned_changes: list[Mapping[str, Any]],
    governance_context: Mapping[str, Any],
) -> dict[str, Any]:
    """Render a bounded, deterministic game-developer readable view."""
    normalized_changes = _normalize_planned_changes(planned_changes)
    normalized_governance = _normalize_governance_context(governance_context)
    return {
        "view_type": "human",
        "contract_version": CONTRACT_VERSION,
        "columns": [
            "change_id",
            "target",
            "operation",
            "domain_type",
            "strategy",
            "intent",
            "impact",
            "risk",
            "review_focus",
            "guardrail",
        ],
        "rows": [
            {
                "change_id": change["change_id"],
                "target": _human_target_label(change["target"]),
                "operation": change["operation"],
                "domain_type": change["domain_type"],
                "strategy": change["strategy"],
                "intent": change["summary"]["intent"],
                "impact": change["summary"]["impact"],
                "risk": change["summary"]["risk"],
                "review_focus": change["summary"]["review_focus"],
                "guardrail": "required" if normalized_governance["guardrail_required"] else "not_required",
            }
            for change in normalized_changes
        ],
        "totals": _human_totals(normalized_changes),
    }


def _normalize_planned_changes(planned_changes: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(planned_changes, list) or not planned_changes:
        raise ValueError("planned_changes must be a non-empty array")
    normalized = [_normalize_planned_change(change, index) for index, change in enumerate(planned_changes)]
    change_ids = [change["change_id"] for change in normalized]
    if len(change_ids) != len(set(change_ids)):
        raise ValueError("planned_changes.change_id values must be unique")
    return sorted(normalized, key=lambda change: change["change_id"])


def _normalize_planned_change(change: Mapping[str, Any], index: int) -> dict[str, Any]:
    if not isinstance(change, Mapping):
        raise ValueError(f"planned_changes[{index}] must be an object")
    _require_exact_keys(change, _PLANNED_CHANGE_KEYS, f"planned_changes[{index}]")
    return {
        "change_id": _safe_identifier(change.get("change_id"), f"planned_changes[{index}].change_id"),
        "target": _normalize_target(_expect_mapping(change.get("target"), f"planned_changes[{index}].target"), index),
        "operation": _require_allowed(change.get("operation"), ALLOWED_OPERATIONS, f"planned_changes[{index}].operation"),
        "domain_type": _require_allowed(change.get("domain_type"), ALLOWED_DOMAIN_TYPES, f"planned_changes[{index}].domain_type"),
        "strategy": _require_allowed(change.get("strategy"), ALLOWED_STRATEGIES, f"planned_changes[{index}].strategy"),
        "summary": _normalize_summary(_expect_mapping(change.get("summary"), f"planned_changes[{index}].summary"), index),
    }


def _normalize_target(target: Mapping[str, Any], change_index: int) -> dict[str, str]:
    _require_exact_keys(target, _TARGET_KEYS, f"planned_changes[{change_index}].target")
    return {
        "target_id": _safe_identifier(target.get("target_id"), f"planned_changes[{change_index}].target.target_id"),
        "target_path": _safe_identifier(target.get("target_path"), f"planned_changes[{change_index}].target.target_path"),
        "target_kind": _require_allowed(
            target.get("target_kind"),
            ALLOWED_TARGET_KINDS,
            f"planned_changes[{change_index}].target.target_kind",
        ),
    }


def _normalize_summary(summary: Mapping[str, Any], change_index: int) -> dict[str, str]:
    _require_exact_keys(summary, _SUMMARY_KEYS, f"planned_changes[{change_index}].summary")
    return {
        "intent": _require_allowed(summary.get("intent"), ALLOWED_SUMMARY_INTENTS, f"planned_changes[{change_index}].summary.intent"),
        "impact": _require_allowed(summary.get("impact"), ALLOWED_SUMMARY_IMPACTS, f"planned_changes[{change_index}].summary.impact"),
        "risk": _require_allowed(summary.get("risk"), ALLOWED_SUMMARY_RISKS, f"planned_changes[{change_index}].summary.risk"),
        "review_focus": _require_allowed(
            summary.get("review_focus"),
            ALLOWED_SUMMARY_REVIEW_FOCUS,
            f"planned_changes[{change_index}].summary.review_focus",
        ),
    }


def _normalize_governance_context(governance_context: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(governance_context, Mapping):
        raise ValueError("governance_context must be an object")
    _require_exact_keys(governance_context, _GOVERNANCE_KEYS, "governance_context")
    guardrail_required = governance_context.get("guardrail_required")
    if not isinstance(guardrail_required, bool):
        raise ValueError("governance_context.guardrail_required must be boolean")
    return {
        "policy_pack": _require_allowed(governance_context.get("policy_pack"), ALLOWED_POLICY_PACKS, "governance_context.policy_pack"),
        "guardrail_required": guardrail_required,
    }


def _human_target_label(target: Mapping[str, Any]) -> str:
    return f"{target['target_kind']}:{target['target_path']}#{target['target_id']}"


def _human_totals(planned_changes: list[Mapping[str, Any]]) -> dict[str, dict[str, int] | int]:
    operation_counts = {operation: 0 for operation in ALLOWED_OPERATIONS}
    strategy_counts = {strategy: 0 for strategy in ALLOWED_STRATEGIES}
    domain_counts = {domain_type: 0 for domain_type in ALLOWED_DOMAIN_TYPES}
    for change in planned_changes:
        operation_counts[str(change["operation"])] += 1
        strategy_counts[str(change["strategy"])] += 1
        domain_counts[str(change["domain_type"])] += 1
    return {
        "change_count": len(planned_changes),
        "operations": operation_counts,
        "strategies": strategy_counts,
        "domain_types": domain_counts,
    }


def _require_exact_keys(payload: Mapping[str, Any], allowed_keys: set[str], field_name: str) -> None:
    keys = set(payload.keys())
    missing = sorted(allowed_keys - keys)
    extra = sorted(keys - allowed_keys)
    if missing:
        raise ValueError(f"{field_name} missing required fields: {', '.join(missing)}")
    if extra:
        raise ValueError(f"{field_name} contains unsupported fields: {', '.join(extra)}")


def _reject_forbidden_raw_keys(value: Any, field_name: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            if key_text in _FORBIDDEN_RAW_KEYS or key_text.startswith("raw_"):
                raise ValueError(f"{field_name}.{key_text} is not allowed")
            _reject_forbidden_raw_keys(child, f"{field_name}.{key_text}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_forbidden_raw_keys(child, f"{field_name}[{index}]")


def _require_exact(value: Any, expected: str, field_name: str) -> None:
    if value != expected:
        raise ValueError(f"{field_name} must be {expected}")


def _require_allowed(value: Any, allowed_values: tuple[str, ...], field_name: str) -> str:
    if not isinstance(value, str) or value not in allowed_values:
        raise ValueError(f"{field_name} must be one of: {', '.join(allowed_values)}")
    return value


def _safe_identifier(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"{field_name} exceeds maximum length")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be single-line")
    if not SAFE_IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{field_name} contains unsupported characters")
    return value


def _normalize_optional_reference(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _safe_identifier(value, field_name)


def _expect_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _expect_list(value: Any, field_name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be an array")
    return deepcopy(value)


__all__ = [
    "ADAPTER",
    "ALLOWED_DOMAIN_TYPES",
    "ALLOWED_OPERATIONS",
    "ALLOWED_STRATEGIES",
    "ARTIFACT_TYPE",
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "DOMAIN",
    "apply_game_adapter_stage2_strategy_selection",
    "build_game_adapter_stage2_preview",
    "render_game_adapter_stage2_human_view",
    "render_game_adapter_stage2_machine_view",
    "select_game_adapter_stage2_strategy",
    "validate_game_adapter_stage2_preview_contract",
]
