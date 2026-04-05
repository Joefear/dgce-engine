"""Scaffolded Claude provider boundary for DGCE model execution."""

from __future__ import annotations

import os
from typing import Any


def generate_text(prompt: str, config: dict[str, Any]) -> str:
    """Validate Claude provider config and stop before live execution."""
    _require_non_empty_string(prompt, "prompt")
    model_id = _require_non_empty_string(config.get("model_id"), "config.model_id")
    api_key = _resolve_api_key(config)
    if api_key is None:
        raise ValueError(
            "Claude provider requires config.api_key or config.api_key_env with a populated environment variable"
        )
    raise ValueError(f"Claude provider live execution is not configured for model_id={model_id}")


def _resolve_api_key(config: dict[str, Any]) -> str | None:
    raw_api_key = config.get("api_key")
    if isinstance(raw_api_key, str) and raw_api_key.strip():
        return raw_api_key.strip()
    api_key_env = config.get("api_key_env")
    if api_key_env is None:
        return None
    env_name = _require_non_empty_string(api_key_env, "config.api_key_env")
    env_value = os.getenv(env_name)
    if not isinstance(env_value, str) or not env_value.strip():
        return None
    return env_value.strip()


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
