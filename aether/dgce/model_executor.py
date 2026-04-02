"""Deterministic DGCE model executor for single-function stubs."""

from __future__ import annotations

from typing import Any


def generate_function_stub(structured_input: dict, config: dict) -> str:
    """Generate one deterministic Python function stub from validated structured input."""
    name = _require_non_empty_string(structured_input.get("name"), "structured_input.name")
    output_type = _require_non_empty_string(structured_input.get("output"), "structured_input.output")
    inputs = structured_input.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("structured_input.inputs must be a non-empty list")
    _build_prompt(structured_input, config)
    signature_parts = []
    for index, raw_input in enumerate(inputs):
        if not isinstance(raw_input, dict):
            raise ValueError(f"structured_input.inputs[{index}] must be a dict")
        input_name = _require_non_empty_string(raw_input.get("name"), f"structured_input.inputs[{index}].name")
        input_type = _require_non_empty_string(raw_input.get("type"), f"structured_input.inputs[{index}].type")
        signature_parts.append(f"{input_name}: {input_type}")
    signature = ", ".join(signature_parts)
    return "\n".join(
        [
            f"def {name}({signature}) -> {output_type}:",
            f"    return {_stub_return_expression(output_type)}",
            "",
        ]
    )


def _build_prompt(structured_input: dict[str, Any], config: dict[str, Any]) -> str:
    inputs = structured_input["inputs"]
    rendered_inputs = ", ".join(
        f"{_require_non_empty_string(item.get('name'), 'structured_input.inputs.name')}: "
        f"{_require_non_empty_string(item.get('type'), 'structured_input.inputs.type')}"
        for item in inputs
        if isinstance(item, dict)
    )
    return (
        "Generate a Python function with:\n"
        f"* name: {_require_non_empty_string(structured_input.get('name'), 'structured_input.name')}\n"
        f"* inputs: {rendered_inputs}\n"
        f"* output: {_require_non_empty_string(structured_input.get('output'), 'structured_input.output')}\n"
        f"* model: {_require_non_empty_string(config.get('model_id'), 'config.model_id')}\n"
        "Return ONLY valid Python function code."
    )


def _stub_return_expression(output_type: str) -> str:
    normalized = output_type.replace(" ", "")
    if normalized in {"None", "NoneType"}:
        return "None"
    if normalized == "bool":
        return "False"
    if normalized in {"int", "float"}:
        return "0" if normalized == "int" else "0.0"
    if normalized == "str":
        return '""'
    if normalized.startswith(("list[", "tuple[")):
        return "[]"
    if normalized.startswith(("dict[", "Mapping[", "MutableMapping[")) or normalized == "dict":
        return "{}"
    if normalized.startswith(("set[", "frozenset[")) or normalized in {"set", "frozenset"}:
        return "set()"
    return "None"


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
