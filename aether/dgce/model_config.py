"""Static deterministic model execution config for governed DGCE stubs."""

from __future__ import annotations

from typing import Any

SUPPORTED_MODEL_PROVIDERS = {"stub", "claude"}
SUPPORTED_PROMPT_TEMPLATE_VERSIONS = {"v1"}
SUPPORTED_POSTPROCESS_VALUES = {"strict_function_stub_v1"}

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
    temperature = config.get("temperature")
    if not isinstance(temperature, (int, float)) or isinstance(temperature, bool):
        raise ValueError("config.temperature must be a float")
    config["temperature"] = float(temperature)
    prompt_template_version = config.get("prompt_template_version")
    if not isinstance(prompt_template_version, str) or prompt_template_version.strip() not in SUPPORTED_PROMPT_TEMPLATE_VERSIONS:
        raise ValueError("config.prompt_template_version must be one of: v1")
    config["prompt_template_version"] = prompt_template_version.strip()
    postprocess = config.get("postprocess")
    if not isinstance(postprocess, str) or postprocess.strip() not in SUPPORTED_POSTPROCESS_VALUES:
        raise ValueError("config.postprocess must be one of: strict_function_stub_v1")
    config["postprocess"] = postprocess.strip()
    return config


def build_model_execution_metadata(config: dict[str, Any]) -> dict[str, Any]:
    """Return the compact audit-safe model execution metadata."""
    return {
        "provider": str(config["provider"]),
        "model_id": str(config["model_id"]),
        "prompt_template_version": str(config["prompt_template_version"]),
        "temperature": float(config["temperature"]),
        "postprocess": str(config["postprocess"]),
    }


def build_model_execution_audit(config: dict[str, Any]) -> dict[str, Any]:
    """Backward-compatible alias for audit-safe model execution metadata."""
    return build_model_execution_metadata(config)
