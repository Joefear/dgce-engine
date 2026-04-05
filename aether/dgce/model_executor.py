"""Deterministic DGCE model executor for single-function stubs."""

from __future__ import annotations

import json
from typing import Any

from aether.dgce import model_provider


def generate_function_stub(structured_input: dict, config: dict) -> str:
    """Generate one deterministic Python function stub from validated structured input."""
    name = _require_non_empty_string(structured_input.get("name"), "structured_input.name")
    _require_non_empty_string(structured_input.get("output"), "structured_input.output")
    inputs = structured_input.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("structured_input.inputs must be a non-empty list")
    for index, raw_input in enumerate(inputs):
        if not isinstance(raw_input, dict):
            raise ValueError(f"structured_input.inputs[{index}] must be a dict")
        _require_non_empty_string(raw_input.get("name"), f"structured_input.inputs[{index}].name")
        _require_non_empty_string(raw_input.get("type"), f"structured_input.inputs[{index}].type")
    prompt = _build_prompt(structured_input, config)
    raw_output = model_provider.generate_text(prompt, config)
    if not isinstance(raw_output, str) or not raw_output:
        raise ValueError("Model provider must return a non-empty string")
    return raw_output


def _build_prompt(structured_input: dict[str, Any], config: dict[str, Any]) -> str:
    spec = {
        "name": _require_non_empty_string(structured_input.get("name"), "structured_input.name"),
        "inputs": [
            {
                "name": _require_non_empty_string(item.get("name"), "structured_input.inputs.name"),
                "type": _require_non_empty_string(item.get("type"), "structured_input.inputs.type"),
            }
            for item in structured_input["inputs"]
            if isinstance(item, dict)
        ],
        "output": _require_non_empty_string(structured_input.get("output"), "structured_input.output"),
    }
    rendered_inputs = ", ".join(f"{item['name']}: {item['type']}" for item in spec["inputs"])
    return (
        "Generate a Python function with:\n"
        f"* name: {spec['name']}\n"
        f"* inputs: {rendered_inputs}\n"
        f"* output: {spec['output']}\n"
        f"* provider: {_require_non_empty_string(config.get('provider'), 'config.provider')}\n"
        f"* model: {_require_non_empty_string(config.get('model_id'), 'config.model_id')}\n"
        f"FUNCTION_STUB_SPEC: {json.dumps(spec, sort_keys=True)}\n"
        "Return ONLY valid Python function code."
    )


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
