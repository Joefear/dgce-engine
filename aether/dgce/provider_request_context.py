"""Audit-safe provider request context for the DGCE function-stub slice."""

from __future__ import annotations

from typing import Any


def build_provider_request_context(config: dict[str, Any], *, request_attempted: bool) -> dict[str, Any]:
    """Build bounded provider request context metadata."""
    return {
        "provider": str(config["provider"]),
        "model_id": str(config["model_id"]),
        "prompt_template_version": str(config["prompt_template_version"]),
        "temperature": float(config["temperature"]),
        "request_attempted": bool(request_attempted),
    }
