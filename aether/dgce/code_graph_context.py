"""Optional Defiant Code Graph facts contract for the DGCE function-stub slice."""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any, TypedDict

CONTRACT_NAME = "DefiantCodeGraphFacts"
CONTRACT_VERSION = "dcg.facts.v1"
RFC3339_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


class CodeGraphSpan(TypedDict):
    start_line: int | None
    end_line: int | None


class CodeGraphOutlineEntry(TypedDict):
    symbol_id: str
    symbol_kind: str
    span: CodeGraphSpan | None


class CodeGraphInsertionPointHint(TypedDict):
    anchor_symbol_id: str | None
    position: str
    span: CodeGraphSpan | None


class CodeGraphCollisionFlag(TypedDict):
    symbol_id: str
    reason: str


class CodeGraphOwnershipBoundaryMarker(TypedDict):
    owner_id: str
    span: CodeGraphSpan | None


class CodeGraphPlacementFacts(TypedDict):
    target_symbol_id: str | None
    file_outline_summary: list[CodeGraphOutlineEntry] | None
    insertion_point_hints: list[CodeGraphInsertionPointHint] | None
    collision_flags: list[CodeGraphCollisionFlag] | None
    ownership_boundary_markers: list[CodeGraphOwnershipBoundaryMarker] | None


class DefiantCodeGraphFacts(TypedDict):
    contract_name: str
    contract_version: str
    generated_at: str | None
    placement_facts: CodeGraphPlacementFacts | None
    related_symbols: list[str] | None
    touched_symbols: list[str] | None
    dependent_symbols: list[str] | None


def parse_code_graph_context(raw_context: Any) -> DefiantCodeGraphFacts:
    """Validate and normalize optional read-only Defiant Code Graph facts."""
    field_name = "function_stub.code_graph_context"
    payload = _expect_dict(raw_context, field_name)
    _require_exact_fields(
        payload,
        field_name,
        {
            "contract_name",
            "contract_version",
            "generated_at",
            "placement_facts",
            "related_symbols",
            "touched_symbols",
            "dependent_symbols",
        },
    )
    contract_name = _require_exact_string(payload.get("contract_name"), f"{field_name}.contract_name", CONTRACT_NAME)
    contract_version = _require_exact_string(
        payload.get("contract_version"),
        f"{field_name}.contract_version",
        CONTRACT_VERSION,
    )
    generated_at = _parse_generated_at(payload.get("generated_at"), f"{field_name}.generated_at")
    placement_facts = _parse_placement_facts(payload.get("placement_facts"), f"{field_name}.placement_facts")
    related_symbols = _parse_symbol_id_list(payload.get("related_symbols"), f"{field_name}.related_symbols")
    touched_symbols = _parse_symbol_id_list(payload.get("touched_symbols"), f"{field_name}.touched_symbols")
    dependent_symbols = _parse_symbol_id_list(payload.get("dependent_symbols"), f"{field_name}.dependent_symbols")
    return {
        "contract_name": contract_name,
        "contract_version": contract_version,
        "generated_at": generated_at,
        "placement_facts": placement_facts,
        "related_symbols": related_symbols,
        "touched_symbols": touched_symbols,
        "dependent_symbols": dependent_symbols,
    }


def _parse_placement_facts(raw_value: Any, field_name: str) -> CodeGraphPlacementFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _require_exact_fields(
        payload,
        field_name,
        {
            "target_symbol_id",
            "file_outline_summary",
            "insertion_point_hints",
            "collision_flags",
            "ownership_boundary_markers",
        },
    )
    return {
        "target_symbol_id": _parse_nullable_string(payload.get("target_symbol_id"), f"{field_name}.target_symbol_id"),
        "file_outline_summary": _parse_outline_entries(payload.get("file_outline_summary"), f"{field_name}.file_outline_summary"),
        "insertion_point_hints": _parse_insertion_point_hints(
            payload.get("insertion_point_hints"),
            f"{field_name}.insertion_point_hints",
        ),
        "collision_flags": _parse_collision_flags(payload.get("collision_flags"), f"{field_name}.collision_flags"),
        "ownership_boundary_markers": _parse_ownership_boundary_markers(
            payload.get("ownership_boundary_markers"),
            f"{field_name}.ownership_boundary_markers",
        ),
    }


def _parse_outline_entries(raw_value: Any, field_name: str) -> list[CodeGraphOutlineEntry] | None:
    if raw_value is None:
        return None
    items = _expect_list(raw_value, field_name)
    normalized: list[CodeGraphOutlineEntry] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        payload = _expect_dict(item, item_field)
        _require_exact_fields(payload, item_field, {"symbol_id", "symbol_kind", "span"})
        normalized.append(
            {
                "symbol_id": _require_non_empty_string(payload.get("symbol_id"), f"{item_field}.symbol_id"),
                "symbol_kind": _require_non_empty_string(payload.get("symbol_kind"), f"{item_field}.symbol_kind"),
                "span": _parse_span(payload.get("span"), f"{item_field}.span"),
            }
        )
    return normalized


def _parse_insertion_point_hints(raw_value: Any, field_name: str) -> list[CodeGraphInsertionPointHint] | None:
    if raw_value is None:
        return None
    items = _expect_list(raw_value, field_name)
    normalized: list[CodeGraphInsertionPointHint] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        payload = _expect_dict(item, item_field)
        _require_exact_fields(payload, item_field, {"anchor_symbol_id", "position", "span"})
        normalized.append(
            {
                "anchor_symbol_id": _parse_nullable_string(payload.get("anchor_symbol_id"), f"{item_field}.anchor_symbol_id"),
                "position": _require_non_empty_string(payload.get("position"), f"{item_field}.position"),
                "span": _parse_span(payload.get("span"), f"{item_field}.span"),
            }
        )
    return normalized


def _parse_collision_flags(raw_value: Any, field_name: str) -> list[CodeGraphCollisionFlag] | None:
    if raw_value is None:
        return None
    items = _expect_list(raw_value, field_name)
    normalized: list[CodeGraphCollisionFlag] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        payload = _expect_dict(item, item_field)
        _require_exact_fields(payload, item_field, {"symbol_id", "reason"})
        normalized.append(
            {
                "symbol_id": _require_non_empty_string(payload.get("symbol_id"), f"{item_field}.symbol_id"),
                "reason": _require_non_empty_string(payload.get("reason"), f"{item_field}.reason"),
            }
        )
    return normalized


def _parse_ownership_boundary_markers(raw_value: Any, field_name: str) -> list[CodeGraphOwnershipBoundaryMarker] | None:
    if raw_value is None:
        return None
    items = _expect_list(raw_value, field_name)
    normalized: list[CodeGraphOwnershipBoundaryMarker] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        payload = _expect_dict(item, item_field)
        _require_exact_fields(payload, item_field, {"owner_id", "span"})
        normalized.append(
            {
                "owner_id": _require_non_empty_string(payload.get("owner_id"), f"{item_field}.owner_id"),
                "span": _parse_span(payload.get("span"), f"{item_field}.span"),
            }
        )
    return normalized


def _parse_symbol_id_list(raw_value: Any, field_name: str) -> list[str] | None:
    if raw_value is None:
        return None
    items = _expect_list(raw_value, field_name)
    normalized: list[str] = []
    for index, item in enumerate(items):
        normalized.append(_require_non_empty_string(item, f"{field_name}[{index}]"))
    return normalized


def _parse_span(raw_value: Any, field_name: str) -> CodeGraphSpan | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _require_exact_fields(payload, field_name, {"start_line", "end_line"})
    start_line = _parse_nullable_int(payload.get("start_line"), f"{field_name}.start_line")
    end_line = _parse_nullable_int(payload.get("end_line"), f"{field_name}.end_line")
    if start_line is not None and end_line is not None and end_line < start_line:
        raise ValueError(f"{field_name}.end_line must be greater than or equal to {field_name}.start_line")
    return {
        "start_line": start_line,
        "end_line": end_line,
    }


def _parse_generated_at(raw_value: Any, field_name: str) -> str | None:
    if raw_value is None:
        return None
    normalized = _require_non_empty_string(raw_value, field_name)
    if not RFC3339_PATTERN.match(normalized):
        raise ValueError(f"{field_name} must be RFC 3339 when provided")
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be RFC 3339 when provided") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must be RFC 3339 when provided")
    return normalized


def _expect_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dict")
    return dict(value)


def _expect_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return list(value)


def _require_exact_fields(payload: dict[str, Any], field_name: str, allowed_fields: set[str]) -> None:
    missing_fields = sorted(field for field in allowed_fields if field not in payload)
    if missing_fields:
        raise ValueError(f"{field_name} must include all contract fields")
    extra_fields = sorted(field for field in payload if field not in allowed_fields)
    if extra_fields:
        raise ValueError(f"{field_name} must not include extra fields")


def _require_exact_string(value: Any, field_name: str, expected: str) -> str:
    normalized = _require_non_empty_string(value, field_name)
    if normalized != expected:
        raise ValueError(f"{field_name} must be {expected}")
    return normalized


def _parse_nullable_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty_string(value, field_name)


def _parse_nullable_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an int or null")
    return int(value)


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
