"""Minimal Claude provider boundary for DGCE model execution."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from typing import Any

from aether.dgce.provider_response import build_provider_response

DEFAULT_API_BASE_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MAX_TOKENS = 512


class ClaudeProviderError(ValueError):
    """Bounded Claude provider error with request-attempt metadata."""

    def __init__(self, message: str, *, request_attempted: bool) -> None:
        super().__init__(message)
        self.request_attempted = request_attempted


def generate_response(prompt: str, config: dict[str, Any]) -> dict[str, Any]:
    """Validate Claude provider config and return normalized response from one Claude request."""
    _require_non_empty_string(prompt, "prompt")
    model_id = _require_non_empty_string(config.get("model_id"), "config.model_id")
    api_key = _resolve_api_key(config)
    if api_key is None:
        raise ClaudeProviderError("Claude provider requires config.api_key", request_attempted=False)
    request = _build_request(prompt, config, model_id, api_key)
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return build_provider_response(_extract_text(payload), request_attempted=True)
    except ClaudeProviderError:
        raise
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise ClaudeProviderError("Claude provider request failed", request_attempted=True) from exc


def generate_text(prompt: str, config: dict[str, Any]) -> str:
    """Backward-compatible raw-text accessor for Claude responses."""
    return generate_response(prompt, config)["raw_text"]


def _build_request(prompt: str, config: dict[str, Any], model_id: str, api_key: str) -> Request:
    api_base_url = str(config.get("api_base_url", DEFAULT_API_BASE_URL)).strip() or DEFAULT_API_BASE_URL
    payload: dict[str, Any] = {
        "model": model_id,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "messages": [{"role": "user", "content": prompt}],
    }
    temperature = config.get("temperature")
    if isinstance(temperature, (int, float)) and not isinstance(temperature, bool):
        payload["temperature"] = float(temperature)
    body = json.dumps(payload).encode("utf-8")
    return Request(
        api_base_url,
        data=body,
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )


def _extract_text(payload: Any) -> str:
    if not isinstance(payload, dict):
        raise ClaudeProviderError("Claude provider response missing text content", request_attempted=True)
    content = payload.get("content")
    if not isinstance(content, list) or not content:
        raise ClaudeProviderError("Claude provider response missing text content", request_attempted=True)
    text_segments: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "text":
            continue
        text = item.get("text")
        if isinstance(text, str) and text:
            text_segments.append(text)
    if not text_segments:
        raise ClaudeProviderError("Claude provider response missing text content", request_attempted=True)
    return "".join(text_segments)


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
