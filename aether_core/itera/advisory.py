"""Deterministic read-only advisory helpers for Itera."""

from typing import Optional


def build_advisory(execution_outcome: dict, section_id: str) -> Optional[dict]:
    """Return a minimal advisory signal derived from a DGCE execution outcome."""
    validation = execution_outcome.get("validation_summary", {})
    status = execution_outcome.get("status")
    execution = execution_outcome.get("execution_summary", {})

    if validation.get("ok") is False:
        explanation = ["validation_failed"]
        if validation.get("missing_keys"):
            explanation.append("missing_required_keys")
        return {
            "type": "policy_adjustment",
            "summary": f"Review schema contract handling for {section_id}",
            "explanation": explanation,
        }

    if status == "error":
        return {
            "type": "process_adjustment",
            "summary": f"Review failed DGCE run flow for {section_id}",
            "explanation": ["execution_error"],
        }

    if status == "partial":
        explanation = ["partial_run"]
        if execution.get("skipped_modify_count", 0) > 0:
            explanation.append("skipped_modify")
        if execution.get("skipped_ignore_count", 0) > 0:
            explanation.append("skipped_ignore")
        return {
            "type": "process_adjustment",
            "summary": f"Review incremental skip behavior for {section_id}",
            "explanation": explanation,
        }

    return None
