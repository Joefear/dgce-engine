"""Optional bounded Code Graph context contract for the DGCE function-stub slice."""

from __future__ import annotations

from typing import Any


def parse_code_graph_context(raw_context: Any) -> dict[str, Any]:
    """Validate and normalize optional read-only Code Graph context."""
    if not isinstance(raw_context, dict):
        raise ValueError("function_stub.code_graph_context must be a dict")
    if not raw_context:
        raise ValueError("function_stub.code_graph_context must not be empty")
    normalized: dict[str, Any] = {}
    if "target_symbol" in raw_context:
        normalized["target_symbol"] = _parse_symbol_metadata(
            raw_context["target_symbol"],
            "function_stub.code_graph_context.target_symbol",
        )
    if "file_outline_summary" in raw_context:
        normalized["file_outline_summary"] = _parse_symbol_list(
            raw_context["file_outline_summary"],
            "function_stub.code_graph_context.file_outline_summary",
        )
    if "insertion_point_hints" in raw_context:
        normalized["insertion_point_hints"] = _parse_keyed_list(
            raw_context["insertion_point_hints"],
            "function_stub.code_graph_context.insertion_point_hints",
            required_fields=("anchor", "position"),
        )
    if "collision_flags" in raw_context:
        normalized["collision_flags"] = _parse_keyed_list(
            raw_context["collision_flags"],
            "function_stub.code_graph_context.collision_flags",
            required_fields=("symbol", "reason"),
        )
    if "nearby_related_symbols" in raw_context:
        normalized["nearby_related_symbols"] = _parse_symbol_list(
            raw_context["nearby_related_symbols"],
            "function_stub.code_graph_context.nearby_related_symbols",
        )
    if "ownership_boundary_markers" in raw_context:
        normalized["ownership_boundary_markers"] = _parse_keyed_list(
            raw_context["ownership_boundary_markers"],
            "function_stub.code_graph_context.ownership_boundary_markers",
            required_fields=("path", "owner"),
        )
    if not normalized:
        raise ValueError("function_stub.code_graph_context must include at least one supported field")
    return normalized


def _parse_symbol_metadata(raw_value: Any, field_name: str) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        raise ValueError(f"{field_name} must be a dict")
    return {
        "name": _require_non_empty_string(raw_value.get("name"), f"{field_name}.name"),
        "kind": _require_non_empty_string(raw_value.get("kind"), f"{field_name}.kind"),
    }


def _parse_symbol_list(raw_value: Any, field_name: str) -> list[dict[str, str]]:
    return _parse_keyed_list(raw_value, field_name, required_fields=("name", "kind"))


def _parse_keyed_list(raw_value: Any, field_name: str, *, required_fields: tuple[str, ...]) -> list[dict[str, str]]:
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty list")
    normalized_items: list[dict[str, str]] = []
    for index, item in enumerate(raw_value):
        if not isinstance(item, dict):
            raise ValueError(f"{field_name}[{index}] must be a dict")
        normalized_item: dict[str, str] = {}
        for required_field in required_fields:
            normalized_item[required_field] = _require_non_empty_string(
                item.get(required_field),
                f"{field_name}[{index}].{required_field}",
            )
        normalized_items.append(normalized_item)
    return normalized_items


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
