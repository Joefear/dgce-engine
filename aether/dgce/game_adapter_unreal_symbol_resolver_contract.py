"""Contract-only Light Unreal Symbol Resolver schemas for Game Adapter.

This module validates metadata-only resolver input/output artifacts. It does
not resolve symbols, inspect Unreal projects, parse Blueprint assets, validate
Blueprint graphs, or perform execution writes.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
import re
from typing import Any

from aether.dgce.decompose import compute_json_payload_fingerprint as _compute_json_payload_fingerprint
from aether.dgce.game_adapter_preview import ADAPTER, ALLOWED_TARGET_KINDS, DOMAIN


INPUT_ARTIFACT_TYPE = "game_adapter_unreal_symbol_resolver_input"
OUTPUT_ARTIFACT_TYPE = "game_adapter_unreal_symbol_resolver_output"
CONTRACT_NAME = "DGCEGameAdapterUnrealSymbolResolver"
CONTRACT_VERSION = "dgce.game_adapter.unreal_symbol_resolver.v1"
RESOLUTION_METHOD = "path_metadata"

ALLOWED_STAGE_USAGES = ("Stage2Preview", "Stage7Alignment", "both")
ALLOWED_SYMBOL_KINDS = ALLOWED_TARGET_KINDS
ALLOWED_RESOLUTION_STATUSES = ("resolved", "partially_resolved", "unresolved", "input_invalid")
ALLOWED_RESOLVED_CONFIDENCE = ("exact_path_match", "candidate_match")
UNRESOLVED_CONFIDENCE = "unresolved"
MAX_IDENTIFIER_LENGTH = 180
MAX_SOURCE_PATH_LENGTH = 260
MAX_SYMBOL_REQUESTS = 100

SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_./:+#-]+$")

_INPUT_BASE_KEYS = {
    "artifact_type",
    "contract_name",
    "contract_version",
    "adapter",
    "domain",
    "source_manifest_fingerprint",
    "source_candidate_index_fingerprint",
    "allowed_symbol_kinds",
    "stage_usage",
}
_REQUESTED_SYMBOL_KEYS = {"requested_symbols"}
_REQUESTED_TARGET_KEYS = {"requested_targets"}
_REQUESTED_TARGET_KEYS_EXACT = {"target_id", "target_path", "target_kind"}
_OUTPUT_KEYS = {
    "artifact_type",
    "contract_name",
    "contract_version",
    "adapter",
    "domain",
    "source_input_fingerprint",
    "resolved_symbols",
    "unresolved_symbols",
    "resolution_status",
    "integration_points",
    "artifact_fingerprint",
}
_SYMBOL_ENTRY_KEYS = {
    "symbol_name",
    "symbol_kind",
    "source_path",
    "resolution_method",
    "confidence",
}
_INTEGRATION_POINTS_KEYS = {"stage2_preview_context", "stage7_alignment_context"}
_INTEGRATION_CONTEXT_KEYS = {"stage", "context_kind", "symbol_metadata_available"}
_FORBIDDEN_KEYS = {
    "binary_blueprint_payload",
    "blueprint_binary_payload",
    "blueprint_graph",
    "blueprint_payload",
    "cpp_write",
    "execution_directive",
    "execution_directives",
    "file_content",
    "file_contents",
    "graph",
    "graph_data",
    "graph_node",
    "graph_nodes",
    "model_output",
    "node",
    "node_data",
    "nodes",
    "output_file_content",
    "provider_output",
    "provider_response",
    "raw_file_content",
    "raw_file_contents",
    "raw_model_output",
    "raw_model_text",
    "raw_provider_output",
    "raw_provider_text",
    "stage8_execution",
    "stderr",
    "stdout",
    "write_directive",
    "write_directives",
    "write_targets",
    "written_files",
}


def validate_resolver_input_contract(payload: Mapping[str, Any]) -> bool:
    """Validate the resolver input contract without performing resolution."""
    if not isinstance(payload, Mapping):
        raise ValueError("resolver input contract must be an object")
    _reject_forbidden_keys(payload, "resolver_input")
    has_symbols = "requested_symbols" in payload
    has_targets = "requested_targets" in payload
    if has_symbols == has_targets:
        raise ValueError("resolver input must contain exactly one of requested_symbols or requested_targets")
    expected_keys = set(_INPUT_BASE_KEYS)
    expected_keys.update(_REQUESTED_SYMBOL_KEYS if has_symbols else _REQUESTED_TARGET_KEYS)
    _require_exact_keys(payload, expected_keys, "resolver_input")
    _require_common_contract_fields(payload, INPUT_ARTIFACT_TYPE)
    _safe_identifier(payload.get("source_manifest_fingerprint"), "source_manifest_fingerprint")
    _safe_identifier(payload.get("source_candidate_index_fingerprint"), "source_candidate_index_fingerprint")
    _validate_allowed_symbol_kinds(payload.get("allowed_symbol_kinds"))
    _require_allowed(payload.get("stage_usage"), ALLOWED_STAGE_USAGES, "stage_usage")
    if has_symbols:
        _validate_requested_symbols(payload.get("requested_symbols"))
    else:
        _validate_requested_targets(payload.get("requested_targets"))
    return True


def validate_resolver_output_contract(payload: Mapping[str, Any]) -> bool:
    """Validate the resolver output contract without performing resolution."""
    if not isinstance(payload, Mapping):
        raise ValueError("resolver output contract must be an object")
    _reject_forbidden_keys(payload, "resolver_output")
    _require_exact_keys(payload, _OUTPUT_KEYS, "resolver_output")
    _require_common_contract_fields(payload, OUTPUT_ARTIFACT_TYPE)
    _safe_identifier(payload.get("source_input_fingerprint"), "source_input_fingerprint")
    _validate_resolved_symbols(payload.get("resolved_symbols"))
    _validate_unresolved_symbols(payload.get("unresolved_symbols"))
    _require_allowed(payload.get("resolution_status"), ALLOWED_RESOLUTION_STATUSES, "resolution_status")
    _validate_integration_points(payload.get("integration_points"))
    artifact_fingerprint = payload.get("artifact_fingerprint")
    if not isinstance(artifact_fingerprint, str) or not artifact_fingerprint:
        raise ValueError("artifact_fingerprint is required")
    if artifact_fingerprint != _compute_json_payload_fingerprint(dict(payload)):
        raise ValueError("artifact_fingerprint invalid")
    return True


def _require_common_contract_fields(payload: Mapping[str, Any], artifact_type: str) -> None:
    _require_exact(payload.get("artifact_type"), artifact_type, "artifact_type")
    _require_exact(payload.get("contract_name"), CONTRACT_NAME, "contract_name")
    _require_exact(payload.get("contract_version"), CONTRACT_VERSION, "contract_version")
    _require_exact(payload.get("adapter"), ADAPTER, "adapter")
    _require_exact(payload.get("domain"), DOMAIN, "domain")


def _validate_allowed_symbol_kinds(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError("allowed_symbol_kinds must be a non-empty array")
    if len(value) > len(ALLOWED_SYMBOL_KINDS):
        raise ValueError("allowed_symbol_kinds exceeds supported symbol kinds")
    if value != sorted(value):
        raise ValueError("allowed_symbol_kinds must be sorted")
    if len(value) != len(set(value)):
        raise ValueError("allowed_symbol_kinds must be unique")
    for symbol_kind in value:
        _require_allowed(symbol_kind, ALLOWED_SYMBOL_KINDS, "allowed_symbol_kinds")


def _validate_requested_symbols(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError("requested_symbols must be a non-empty array")
    if len(value) > MAX_SYMBOL_REQUESTS:
        raise ValueError("requested_symbols exceeds maximum length")
    if value != sorted(value):
        raise ValueError("requested_symbols must be sorted")
    if len(value) != len(set(value)):
        raise ValueError("requested_symbols must be unique")
    for index, symbol_name in enumerate(value):
        _safe_identifier(symbol_name, f"requested_symbols[{index}]")


def _validate_requested_targets(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError("requested_targets must be a non-empty array")
    if len(value) > MAX_SYMBOL_REQUESTS:
        raise ValueError("requested_targets exceeds maximum length")
    normalized_sort_keys: list[tuple[str, str, str]] = []
    for index, target in enumerate(value):
        if not isinstance(target, Mapping):
            raise ValueError(f"requested_targets[{index}] must be an object")
        _require_exact_keys(target, _REQUESTED_TARGET_KEYS_EXACT, f"requested_targets[{index}]")
        target_id = _safe_identifier(target.get("target_id"), f"requested_targets[{index}].target_id")
        target_path = _safe_identifier(target.get("target_path"), f"requested_targets[{index}].target_path")
        target_kind = _require_allowed(
            target.get("target_kind"),
            ALLOWED_SYMBOL_KINDS,
            f"requested_targets[{index}].target_kind",
        )
        normalized_sort_keys.append((target_kind, target_path, target_id))
    if normalized_sort_keys != sorted(normalized_sort_keys):
        raise ValueError("requested_targets must be sorted")
    if len(normalized_sort_keys) != len(set(normalized_sort_keys)):
        raise ValueError("requested_targets must be unique")


def _validate_resolved_symbols(value: Any) -> None:
    if not isinstance(value, list):
        raise ValueError("resolved_symbols must be an array")
    if len(value) > MAX_SYMBOL_REQUESTS:
        raise ValueError("resolved_symbols exceeds maximum length")
    sort_keys = [_validate_symbol_entry(entry, index, resolved=True) for index, entry in enumerate(value)]
    _require_sorted_unique(sort_keys, "resolved_symbols")


def _validate_unresolved_symbols(value: Any) -> None:
    if not isinstance(value, list):
        raise ValueError("unresolved_symbols must be an array")
    if len(value) > MAX_SYMBOL_REQUESTS:
        raise ValueError("unresolved_symbols exceeds maximum length")
    sort_keys = [_validate_symbol_entry(entry, index, resolved=False) for index, entry in enumerate(value)]
    _require_sorted_unique(sort_keys, "unresolved_symbols")


def _validate_symbol_entry(entry: Any, index: int, *, resolved: bool) -> tuple[str, str, str]:
    field_name = "resolved_symbols" if resolved else "unresolved_symbols"
    if not isinstance(entry, Mapping):
        raise ValueError(f"{field_name}[{index}] must be an object")
    _require_exact_keys(entry, _SYMBOL_ENTRY_KEYS, f"{field_name}[{index}]")
    symbol_name = _safe_identifier(entry.get("symbol_name"), f"{field_name}[{index}].symbol_name")
    symbol_kind = _require_allowed(entry.get("symbol_kind"), ALLOWED_SYMBOL_KINDS, f"{field_name}[{index}].symbol_kind")
    _require_exact(entry.get("resolution_method"), RESOLUTION_METHOD, f"{field_name}[{index}].resolution_method")
    if resolved:
        source_path = _safe_relative_path(entry.get("source_path"), f"{field_name}[{index}].source_path")
        _require_allowed(entry.get("confidence"), ALLOWED_RESOLVED_CONFIDENCE, f"{field_name}[{index}].confidence")
    else:
        if entry.get("source_path") is not None:
            raise ValueError(f"{field_name}[{index}].source_path must be null")
        _require_exact(entry.get("confidence"), UNRESOLVED_CONFIDENCE, f"{field_name}[{index}].confidence")
        source_path = ""
    return (symbol_kind, symbol_name, source_path)


def _validate_integration_points(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ValueError("integration_points must be an object")
    _require_exact_keys(value, _INTEGRATION_POINTS_KEYS, "integration_points")
    _validate_integration_context(
        value.get("stage2_preview_context"),
        "Stage2Preview",
        "integration_points.stage2_preview_context",
    )
    _validate_integration_context(
        value.get("stage7_alignment_context"),
        "Stage7Alignment",
        "integration_points.stage7_alignment_context",
    )


def _validate_integration_context(value: Any, stage: str, field_name: str) -> None:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    _require_exact_keys(value, _INTEGRATION_CONTEXT_KEYS, field_name)
    _require_exact(value.get("stage"), stage, f"{field_name}.stage")
    _require_exact(value.get("context_kind"), "resolver_metadata", f"{field_name}.context_kind")
    if not isinstance(value.get("symbol_metadata_available"), bool):
        raise ValueError(f"{field_name}.symbol_metadata_available must be boolean")


def _reject_forbidden_keys(value: Any, field_name: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            if key_text in _FORBIDDEN_KEYS or key_text.startswith("raw_"):
                raise ValueError(f"{field_name}.{key_text} is not allowed")
            _reject_forbidden_keys(child, f"{field_name}.{key_text}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_forbidden_keys(child, f"{field_name}[{index}]")


def _require_sorted_unique(sort_keys: list[tuple[str, str, str]], field_name: str) -> None:
    if sort_keys != sorted(sort_keys):
        raise ValueError(f"{field_name} must be sorted")
    if len(sort_keys) != len(set(sort_keys)):
        raise ValueError(f"{field_name} must be unique")


def _require_exact_keys(payload: Mapping[str, Any], allowed_keys: set[str], field_name: str) -> None:
    keys = set(payload.keys())
    missing = sorted(allowed_keys - keys)
    extra = sorted(keys - allowed_keys)
    if missing:
        raise ValueError(f"{field_name} missing required fields: {', '.join(missing)}")
    if extra:
        raise ValueError(f"{field_name} contains unsupported fields: {', '.join(extra)}")


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


def _safe_relative_path(value: Any, field_name: str) -> str:
    path_text = _safe_identifier(value, field_name)
    if len(path_text) > MAX_SOURCE_PATH_LENGTH:
        raise ValueError(f"{field_name} exceeds maximum length")
    path = Path(path_text)
    if path.is_absolute() or "\\" in path_text or ".." in path.parts:
        raise ValueError(f"{field_name} must be a bounded relative path")
    return path_text


__all__ = [
    "ADAPTER",
    "ALLOWED_RESOLUTION_STATUSES",
    "ALLOWED_STAGE_USAGES",
    "ALLOWED_SYMBOL_KINDS",
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "DOMAIN",
    "INPUT_ARTIFACT_TYPE",
    "OUTPUT_ARTIFACT_TYPE",
    "RESOLUTION_METHOD",
    "UNRESOLVED_CONFIDENCE",
    "validate_resolver_input_contract",
    "validate_resolver_output_contract",
]
