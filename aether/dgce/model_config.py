"""Static deterministic model execution config for governed DGCE stubs."""

from __future__ import annotations

from typing import Any


MODEL_EXECUTION_CONFIG: dict[str, Any] = {
    "model_id": "stub-model-v1",
    "temperature": 0.0,
    "prompt_template_version": "v1",
    "postprocess": "strict_function_stub_v1",
}


def get_model_execution_config() -> dict[str, Any]:
    """Return a copy of the fixed model execution config."""
    return dict(MODEL_EXECUTION_CONFIG)


def build_model_execution_audit(config: dict[str, Any]) -> dict[str, Any]:
    """Return the compact audit-safe model execution metadata."""
    return {
        "model_id": str(config["model_id"]),
        "prompt_template_version": str(config["prompt_template_version"]),
        "temperature": float(config["temperature"]),
    }
