"""Bounded execution failure classification for the DGCE function-stub slice."""

from __future__ import annotations

from typing import Any


def classify_function_stub_execution_failure(*, raw_output_obtained: bool) -> dict[str, str]:
    """Return the bounded failure classification for one function-stub execution failure."""
    if raw_output_obtained:
        return {
            "execution_failure_category": "validation_failure",
            "execution_failure_reason": "validator_rejected_output",
        }
    return {
        "execution_failure_category": "provider_failure",
        "execution_failure_reason": "pre_output_failure",
    }


def build_execution_failure_metadata(classification: dict[str, Any] | None) -> dict[str, str] | None:
    """Normalize one bounded failure classification payload for artifact persistence."""
    if classification is None:
        return None
    category = classification.get("execution_failure_category")
    reason = classification.get("execution_failure_reason")
    if not isinstance(category, str) or not category:
        raise ValueError("execution_failure_category must be a non-empty string")
    if not isinstance(reason, str) or not reason:
        raise ValueError("execution_failure_reason must be a non-empty string")
    return {
        "execution_failure_category": category,
        "execution_failure_reason": reason,
    }
