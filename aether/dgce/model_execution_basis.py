"""Deterministic model execution basis identity for the DGCE function-stub slice."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def build_function_stub_model_execution_basis_fingerprint(
    function_stub_spec: dict[str, Any],
    model_execution_metadata: dict[str, Any],
    target_path: str,
) -> str:
    """Return a bounded deterministic fingerprint for the function-stub model execution basis."""
    normalized_target_path = _require_non_empty_string(target_path, "target_path")
    basis = {
        "function_stub_spec": function_stub_spec,
        "model_execution_metadata": model_execution_metadata,
        "target_path": normalized_target_path,
    }
    return hashlib.sha256(json.dumps(basis, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def assert_function_stub_model_execution_basis_consistent(
    expected_fingerprint: str,
    function_stub_spec: dict[str, Any],
    model_execution_metadata: dict[str, Any],
    target_path: str,
) -> None:
    """Fail deterministically if the current function-stub basis differs from the expected basis."""
    expected = _require_non_empty_string(expected_fingerprint, "expected_fingerprint")
    actual = build_function_stub_model_execution_basis_fingerprint(
        function_stub_spec,
        model_execution_metadata,
        target_path,
    )
    if actual != expected:
        raise ValueError("function_stub model execution basis mismatch")


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
