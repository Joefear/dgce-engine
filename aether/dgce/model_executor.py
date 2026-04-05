"""Deterministic DGCE model executor for single-function stubs."""

from __future__ import annotations

from aether.dgce import model_provider
from aether.dgce.model_config import get_model_execution_config
from aether.dgce.prompt_templates import build_function_stub_prompt


def generate_function_stub(structured_input: dict, config: dict) -> str:
    """Generate one deterministic Python function stub from validated structured input."""
    _require_non_empty_string(structured_input.get("name"), "structured_input.name")
    _require_non_empty_string(structured_input.get("output"), "structured_input.output")
    inputs = structured_input.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("structured_input.inputs must be a non-empty list")
    for index, raw_input in enumerate(inputs):
        if not isinstance(raw_input, dict):
            raise ValueError(f"structured_input.inputs[{index}] must be a dict")
        _require_non_empty_string(raw_input.get("name"), f"structured_input.inputs[{index}].name")
        _require_non_empty_string(raw_input.get("type"), f"structured_input.inputs[{index}].type")
    execution_config = get_model_execution_config(config)
    prompt = build_function_stub_prompt(structured_input, execution_config["prompt_template_version"])
    raw_output = model_provider.generate_text(prompt, execution_config)
    if not isinstance(raw_output, str) or not raw_output:
        raise ValueError("Model provider must return a non-empty string")
    return raw_output


def _require_non_empty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
