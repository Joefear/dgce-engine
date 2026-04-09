"""Thin wrapper for deterministic DGCE workspace view refresh."""

from __future__ import annotations

import json
from pathlib import Path

from aether.dgce.decompose import (
    SectionExecutionGateInput,
    SectionPreflightInput,
    SectionStaleCheckInput,
    _build_execution_gate_artifact,
    _build_gate_input_artifact,
    _build_preflight_artifact,
    _build_stale_check_artifact,
    _ensure_workspace,
    _read_workspace_index,
    _refresh_workspace_views,
    _write_json,
    _write_json_with_artifact_fingerprint,
)
from aether.dgce.path_utils import resolve_workspace_path


def _refresh_one_section_governed_artifacts(workspace: dict[str, Path], section_id: str) -> None:
    default_timestamp = "1970-01-01T00:00:00Z"
    approval_path = workspace["approvals"] / f"{section_id}.approval.json"
    preflight_path = workspace["preflight"] / f"{section_id}.preflight.json"
    stale_check_path = workspace["preflight"] / f"{section_id}.stale_check.json"
    execution_gate_path = workspace["gate"] / f"{section_id}.execution_gate.json"
    if not approval_path.exists():
        return

    existing_preflight = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
    existing_stale = json.loads(stale_check_path.read_text(encoding="utf-8")) if stale_check_path.exists() else {}
    existing_gate = json.loads(execution_gate_path.read_text(encoding="utf-8")) if execution_gate_path.exists() else {}

    preflight_payload = _build_preflight_artifact(
        workspace["root"],
        section_id,
        SectionPreflightInput(
            validation_timestamp=str(existing_preflight.get("validation_timestamp", default_timestamp))
        ),
    )
    stale_payload = _build_stale_check_artifact(
        workspace["root"],
        section_id,
        SectionStaleCheckInput(
            validation_timestamp=str(existing_stale.get("validation_timestamp", default_timestamp))
        ),
    )
    gate_input_path = workspace["gate"] / f"{section_id}.gate_input.json"
    gate_input_payload = _write_json_with_artifact_fingerprint(
        gate_input_path,
        _build_gate_input_artifact(workspace["root"], section_id),
    )
    _write_json_with_artifact_fingerprint(preflight_path, preflight_payload)
    _write_json(stale_check_path, stale_payload)
    gate_payload = _build_execution_gate_artifact(
        workspace["root"],
        section_id,
        require_preflight_pass=bool(existing_gate.get("require_preflight_pass", True)),
        gate_input=SectionExecutionGateInput(
            gate_timestamp=str(existing_gate.get("gate_timestamp", default_timestamp))
        ),
        gate_input_payload=gate_input_payload,
        preflight_payload=preflight_payload,
        stale_check_payload=stale_payload,
    )
    _write_json(execution_gate_path, gate_payload)


def _refresh_section_governed_artifacts(workspace: dict[str, Path]) -> None:
    for section_id in _read_workspace_index(workspace["index"]):
        if not any(
            (workspace["preflight"] / f"{section_id}.{suffix}.json").exists()
            for suffix in ("preflight", "stale_check", "execution_gate")
        ):
            continue
        _refresh_one_section_governed_artifacts(workspace, section_id)


def refresh_section_artifacts(workspace_path: str | Path, section_id: str) -> Path:
    project_root = resolve_workspace_path(workspace_path)
    workspace = _ensure_workspace(project_root)
    _refresh_one_section_governed_artifacts(workspace, section_id)
    _refresh_workspace_views(workspace)
    return project_root


def refresh_workspace_artifacts(workspace_path: str | Path) -> Path:
    project_root = resolve_workspace_path(workspace_path)
    workspace = _ensure_workspace(project_root)
    _refresh_section_governed_artifacts(workspace)
    _refresh_workspace_views(workspace)
    return project_root
