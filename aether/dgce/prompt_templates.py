"""Versioned prompt templates for deterministic DGCE model execution."""

from __future__ import annotations

import json
from typing import Any

SUPPORTED_PROMPT_TEMPLATE_VERSIONS = {"v1"}


def build_function_stub_prompt(structured_input: dict[str, Any], template_version: str) -> str:
    """Build the deterministic function-stub prompt for one supported template version."""
    normalized_version = _require_non_empty_string(template_version, "template_version")
    if normalized_version not in SUPPORTED_PROMPT_TEMPLATE_VERSIONS:
        raise ValueError("template_version must be one of: v1")
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
        f"* template_version: {normalized_version}\n"
        f"* name: {spec['name']}\n"
        f"* inputs: {rendered_inputs}\n"
        f"* output: {spec['output']}\n"
        f"FUNCTION_STUB_SPEC: {json.dumps(spec, sort_keys=True)}\n"
        "Return ONLY valid Python function code."
    )


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
