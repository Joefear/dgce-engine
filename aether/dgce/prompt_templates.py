"""Versioned prompt templates for deterministic DGCE model execution."""

from __future__ import annotations

import json
from typing import Any

from aether.dgce.function_stub_spec import parse_function_stub_spec

SUPPORTED_PROMPT_TEMPLATE_VERSIONS = {"v1"}


def build_function_stub_prompt(structured_input: dict[str, Any], template_version: str) -> str:
    """Build the deterministic function-stub prompt for one supported template version."""
    normalized_version = _require_non_empty_string(template_version, "template_version")
    if normalized_version not in SUPPORTED_PROMPT_TEMPLATE_VERSIONS:
        raise ValueError("template_version must be one of: v1")
    spec = parse_function_stub_spec(structured_input)
    rendered_functions = "\n".join(
        [
            (
                f"  - name: {function_spec['name']}; "
                f"inputs: {', '.join(f'{item['name']}: {item['type']}' for item in function_spec['parameters'])}; "
                f"output: {function_spec['return_type']}"
            )
            for function_spec in spec["functions"]
        ]
    )
    code_graph_context = spec.get("code_graph_context")
    rendered_code_graph_context = ""
    if code_graph_context is not None:
        rendered_code_graph_context = f"CODE_GRAPH_CONTEXT: {json.dumps(code_graph_context, sort_keys=True)}\n"
    return (
        "Generate Python functions for one file with:\n"
        f"* template_version: {normalized_version}\n"
        f"* function_count: {len(spec['functions'])}\n"
        "* functions:\n"
        f"{rendered_functions}\n"
        f"{rendered_code_graph_context}"
        f"FUNCTION_STUB_SPEC: {json.dumps(spec, sort_keys=True)}\n"
        "Return ONLY valid Python function code for that one file, containing exactly the requested functions and no extra text."
    )


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
