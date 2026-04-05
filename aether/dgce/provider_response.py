"""Bounded provider response normalization for the DGCE function-stub slice."""

from __future__ import annotations

from typing import Any


def build_provider_response(raw_text: str, *, request_attempted: bool) -> dict[str, Any]:
    """Build one normalized provider response payload."""
    if not isinstance(raw_text, str) or not raw_text:
        raise ValueError("provider_response.raw_text must be a non-empty string")
    return {
        "raw_text": raw_text,
        "request_attempted": bool(request_attempted),
    }


def normalize_provider_response(response: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one provider response payload."""
    if not isinstance(response, dict):
        raise ValueError("provider_response must be a dict")
    raw_text = response.get("raw_text")
    if not isinstance(raw_text, str) or not raw_text:
        raise ValueError("provider_response.raw_text must be a non-empty string")
    request_attempted = response.get("request_attempted")
    if not isinstance(request_attempted, bool):
        raise ValueError("provider_response.request_attempted must be a bool")
    return {
        "raw_text": raw_text,
        "request_attempted": request_attempted,
    }
