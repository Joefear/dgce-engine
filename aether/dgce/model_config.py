"""Static deterministic model execution config for governed DGCE stubs."""

from __future__ import annotations

from typing import Any

SUPPORTED_MODEL_PROVIDERS = {"stub", "claude"}

MODEL_EXECUTION_CONFIG: dict[str, Any] = {
    "provider": "stub",
    "model_id": "stub-model-v1",
    "temperature": 0.0,
    "prompt_template_version": "v1",
    "postprocess": "strict_function_stub_v1",
}


def get_model_execution_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return validated model execution config with optional explicit overrides."""
    config = dict(MODEL_EXECUTION_CONFIG)
    if overrides is not None:
        config.update(overrides)
    provider = config.get("provider")
    if not isinstance(provider, str) or provider.strip() not in SUPPORTED_MODEL_PROVIDERS:
        raise ValueError(
            "config.provider must be one of: claude, stub"
        )
    config["provider"] = provider.strip()
    model_id = config.get("model_id")
    if not isinstance(model_id, str) or not model_id.strip():
        raise ValueError("config.model_id must be a non-empty string")
    config["model_id"] = model_id.strip()
    return config


def build_model_execution_audit(config: dict[str, Any]) -> dict[str, Any]:
    """Return the compact audit-safe model execution metadata."""
    return {
        "provider": str(config["provider"]),
        "model_id": str(config["model_id"]),
        "prompt_template_version": str(config["prompt_template_version"]),
        "temperature": float(config["temperature"]),
    }
