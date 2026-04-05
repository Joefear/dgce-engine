"""Minimal model provider adapter for deterministic DGCE function stubs."""

from __future__ import annotations

import json
from typing import Any

from aether.dgce.provider_response import build_provider_response, normalize_provider_response
from aether.dgce.providers import claude_provider


def generate_response(prompt: str, config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized provider response for the configured provider."""
    _require_non_empty_string(prompt, "prompt")
    provider = _require_non_empty_string(config.get("provider"), "config.provider")
    if provider == "stub":
        return normalize_provider_response(_generate_stub_response(prompt))
    if provider == "claude":
        return normalize_provider_response(claude_provider.generate_response(prompt, config))
    raise ValueError(f"Unsupported model provider: {provider}")


def generate_text(prompt: str, config: dict[str, Any]) -> str:
    """Backward-compatible raw-text accessor for the configured provider."""
    return generate_response(prompt, config)["raw_text"]


def _generate_stub_response(prompt: str) -> dict[str, Any]:
    spec = _parse_function_stub_spec(prompt)
    signature = ", ".join(f"{item['name']}: {item['type']}" for item in spec["parameters"])
    return build_provider_response(
        "\n".join(
            [
                f"def {spec['name']}({signature}) -> {spec['return_type']}:",
                f"    return {_stub_return_expression(spec['return_type'])}",
                "",
            ]
        ),
        request_attempted=False,
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
