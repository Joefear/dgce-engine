"""Deterministic DGCE model executor for single-function stubs."""

from __future__ import annotations

from aether.dgce import model_provider
from aether.dgce.function_stub_spec import parse_function_stub_spec
from aether.dgce.model_config import get_model_execution_config
from aether.dgce.prompt_templates import build_function_stub_prompt


def generate_function_stub(structured_input: dict, config: dict) -> str:
    """Generate one deterministic Python function stub from validated structured input."""
    normalized_spec = parse_function_stub_spec(structured_input)
    execution_config = get_model_execution_config(config)
    prompt = build_function_stub_prompt(normalized_spec, execution_config["prompt_template_version"])
    raw_output = model_provider.generate_text(prompt, execution_config)
    if not isinstance(raw_output, str) or not raw_output:
        raise ValueError("Model provider must return a non-empty string")
    return raw_output
