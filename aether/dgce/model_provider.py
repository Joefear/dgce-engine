"""Minimal model provider adapter for deterministic DGCE function stubs."""

from __future__ import annotations

import json
from typing import Any


def generate_text(prompt: str, config: dict[str, Any]) -> str:
    """Return raw model text for the configured provider."""
    _require_non_empty_string(prompt, "prompt")
    provider = _require_non_empty_string(config.get("provider"), "config.provider")
    if provider != "stub":
        raise ValueError(f"Unsupported model provider: {provider}")
    spec = _parse_function_stub_spec(prompt)
    signature = ", ".join(f"{item['name']}: {item['type']}" for item in spec["inputs"])
    return "\n".join(
        [
            f"def {spec['name']}({signature}) -> {spec['output']}:",
            f"    return {_stub_return_expression(spec['output'])}",
            "",
        ]
    )


def _parse_function_stub_spec(prompt: str) -> dict[str, Any]:
    prefix = "FUNCTION_STUB_SPEC: "
    for line in prompt.splitlines():
        if line.startswith(prefix):
            payload = json.loads(line[len(prefix) :])
            if not isinstance(payload, dict):
                raise ValueError("FUNCTION_STUB_SPEC must decode to a dict")
            return payload
    raise ValueError("Prompt must contain FUNCTION_STUB_SPEC")


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
