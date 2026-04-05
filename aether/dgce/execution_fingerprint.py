"""Deterministic execution fingerprinting for the DGCE function-stub slice."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def build_function_stub_execution_fingerprint(
    function_stub_spec: dict[str, Any],
    model_execution_metadata: dict[str, Any],
    target_path: str,
    validated_content: str,
) -> str:
    """Return a deterministic fingerprint for the bounded function-stub write basis."""
    normalized_target_path = _require_non_empty_string(target_path, "target_path")
    normalized_content = _require_non_empty_content(validated_content, "validated_content")
    basis = {
        "function_stub_spec": function_stub_spec,
        "model_execution_metadata": model_execution_metadata,
        "target_path": normalized_target_path,
        "validated_content_sha256": hashlib.sha256(normalized_content.encode("utf-8")).hexdigest(),
    }
    return hashlib.sha256(json.dumps(basis, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def determine_function_stub_write_idempotence_status(
    project_root: Path,
    target_path: str,
    validated_content: str,
) -> str:
    """Return deterministic write idempotence status for one governed target."""
    normalized_target_path = _require_non_empty_string(target_path, "target_path")
    normalized_content = _require_non_empty_content(validated_content, "validated_content")
    existing_path = (project_root / normalized_target_path).resolve()
    if not existing_path.exists():
        return "new_content"
    if existing_path.is_dir():
        raise ValueError(f"function_stub write target is a directory: {normalized_target_path}")
    if existing_path.read_text(encoding="utf-8") == normalized_content:
        return "existing_content_match"
    return "existing_content_differs"


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_non_empty_content(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value
