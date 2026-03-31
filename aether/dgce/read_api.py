"""Read-only validated accessors for DGCE workspace artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aether.dgce.decompose import _validate_locked_artifact_schema


def _workspace_root_path(workspace_path: str | Path) -> Path:
    return Path(workspace_path)


def _artifact_file_path(workspace_path: str | Path, *parts: str) -> Path:
    return _workspace_root_path(workspace_path) / ".dce" / Path(*parts)


def _read_validated_json_artifact(workspace_path: str | Path, *parts: str) -> dict[str, Any]:
    artifact_path = _artifact_file_path(workspace_path, *parts)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{artifact_path.name} must contain a JSON object")
    _validate_locked_artifact_schema(artifact_path, payload)
    return payload


def get_dashboard(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "dashboard.json")


def get_workspace_index(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "workspace_index.json")


def get_lifecycle_trace(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "lifecycle_trace.json")


def get_consumer_contract(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "consumer_contract.json")


def get_export_contract(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "export_contract.json")


def get_artifact_manifest(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "artifact_manifest.json")


def list_available_artifacts(workspace_path: str | Path) -> dict[str, Any]:
    return get_artifact_manifest(workspace_path)
