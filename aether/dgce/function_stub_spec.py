"""Structured input contract for deterministic DGCE function stubs."""

from __future__ import annotations

from typing import Any


def parse_function_stub_spec(structured_input: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one function-stub spec."""
    if not isinstance(structured_input, dict):
        raise ValueError("function_stub spec must be a dict")
    name = _require_non_empty_string(structured_input.get("name"), "function_stub.name")
    raw_parameters = structured_input.get("parameters", structured_input.get("inputs"))
    if raw_parameters is None:
        raise ValueError("function_stub.parameters is required")
    if not isinstance(raw_parameters, list) or not raw_parameters:
        raise ValueError("function_stub.parameters must be a non-empty list")
    parameters: list[dict[str, str]] = []
    for index, raw_parameter in enumerate(raw_parameters):
        if not isinstance(raw_parameter, dict):
            raise ValueError(f"function_stub.parameters[{index}] must be a dict")
        parameter_name = _require_non_empty_string(
            raw_parameter.get("name"),
            f"function_stub.parameters[{index}].name",
        )
        parameter_type = _require_non_empty_string(
            raw_parameter.get("type"),
            f"function_stub.parameters[{index}].type",
        )
        parameters.append({"name": parameter_name, "type": parameter_type})
    return_type = _require_non_empty_string(
        structured_input.get("return_type", structured_input.get("output")),
        "function_stub.return_type",
    )
    return {"name": name, "parameters": parameters, "return_type": return_type}


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
