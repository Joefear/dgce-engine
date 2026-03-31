"""Minimal structured-output validator backed by the schema registry."""

from dataclasses import dataclass
from typing import Any, Optional

from aether_core.contracts.schema_registry import SCHEMAS


@dataclass
class ValidationResult:
    """Structured validation outcome."""

    ok: bool
    missing_keys: list[str]
    error: Optional[str] = None


def validate_output(schema_name: str, data: Any) -> ValidationResult:
    """Validate top-level structured output against a registered schema."""
    schema = SCHEMAS.get(schema_name)
    if schema is None:
        return ValidationResult(ok=False, missing_keys=[], error="unknown_schema")

    if not isinstance(data, dict):
        return ValidationResult(ok=False, missing_keys=[], error="invalid_type")

    required_keys = schema.get("required_keys", [])
    missing_keys = [key for key in required_keys if key not in data]
    if not missing_keys:
        return ValidationResult(ok=True, missing_keys=[], error=None)

    accepted_required_key_sets = schema.get("accepted_required_key_sets", [])
    for accepted_keys in accepted_required_key_sets:
        if (
            isinstance(accepted_keys, list)
            and accepted_keys
            and all(key in data for key in accepted_keys)
        ):
            return ValidationResult(ok=True, missing_keys=[], error=None)
    if missing_keys:
        return ValidationResult(ok=False, missing_keys=missing_keys, error="missing_keys")
    return ValidationResult(ok=True, missing_keys=[], error=None)
