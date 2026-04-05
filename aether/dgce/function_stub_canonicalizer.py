"""Bounded canonicalization for validated DGCE function-stub output."""

from __future__ import annotations


def canonicalize_function_stub_output(validated_output: str) -> str:
    """Return canonical function-stub output with harmless formatting normalized."""
    if not isinstance(validated_output, str) or not validated_output:
        raise ValueError("validated_output must be a non-empty string")
    normalized = validated_output.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in normalized.split("\n")]
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise ValueError("validated_output must contain function content")
    return "\n".join(lines) + "\n"
