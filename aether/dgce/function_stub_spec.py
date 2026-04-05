"""Structured input contract for deterministic DGCE function stubs."""

from __future__ import annotations

from typing import Any


def parse_function_stub_spec(structured_input: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the bounded function-stub file spec."""
    if not isinstance(structured_input, dict):
        raise ValueError("function_stub spec must be a dict")
    if "functions" in structured_input:
        _reject_ambiguous_single_function_fields(structured_input)
        raw_functions = structured_input.get("functions")
        if not isinstance(raw_functions, list) or not raw_functions:
            raise ValueError("function_stub.functions must be a non-empty list")
        functions: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        for index, raw_function in enumerate(raw_functions):
            if not isinstance(raw_function, dict):
                raise ValueError(f"function_stub.functions[{index}] must be a dict")
            normalized_function = _parse_single_function_spec(raw_function, f"function_stub.functions[{index}]")
            function_name = normalized_function["name"]
            if function_name in seen_names:
                raise ValueError(f"function_stub.functions[{index}].name must be unique")
            seen_names.add(function_name)
            functions.append(normalized_function)
        return {"functions": functions}
    return {"functions": [_parse_single_function_spec(structured_input, "function_stub")]}


def _parse_single_function_spec(raw_spec: dict[str, Any], field_name: str) -> dict[str, Any]:
    name = _require_non_empty_string(raw_spec.get("name"), f"{field_name}.name")
    raw_parameters = raw_spec.get("parameters", raw_spec.get("inputs"))
    if raw_parameters is None:
        raise ValueError(f"{field_name}.parameters is required")
    if not isinstance(raw_parameters, list) or not raw_parameters:
        raise ValueError(f"{field_name}.parameters must be a non-empty list")
    parameters: list[dict[str, str]] = []
    for index, raw_parameter in enumerate(raw_parameters):
        if not isinstance(raw_parameter, dict):
            raise ValueError(f"{field_name}.parameters[{index}] must be a dict")
        parameter_name = _require_non_empty_string(
            raw_parameter.get("name"),
            f"{field_name}.parameters[{index}].name",
        )
        parameter_type = _require_non_empty_string(
            raw_parameter.get("type"),
            f"{field_name}.parameters[{index}].type",
        )
        parameters.append({"name": parameter_name, "type": parameter_type})
    return_type = _require_non_empty_string(
        raw_spec.get("return_type", raw_spec.get("output")),
        f"{field_name}.return_type",
    )
    return {"name": name, "parameters": parameters, "return_type": return_type}


def _reject_ambiguous_single_function_fields(structured_input: dict[str, Any]) -> None:
    ambiguous_fields = {"name", "parameters", "inputs", "return_type", "output"}
    present_fields = sorted(field for field in ambiguous_fields if field in structured_input)
    if present_fields:
        raise ValueError("function_stub.functions cannot be combined with top-level single-function fields")


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
