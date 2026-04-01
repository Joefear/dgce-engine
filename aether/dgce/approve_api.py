"""Governed DGCE section approval helpers for the HTTP transport layer."""

from __future__ import annotations

import json
from pathlib import Path

from aether.dgce import SectionApprovalInput, record_section_approval
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import (
    _artifact_manifest_entries_by_path,
    _section_artifacts_valid,
    _section_exists,
    _workspace_index_entry,
)
from aether.dgce.read_api import get_artifact_manifest, get_workspace_index
from aether.dgce.refresh_api import refresh_section_artifacts


def approve_section_execution(
    workspace_path: str | Path,
    section_id: str,
    *,
    approved_by: str = "operator",
    notes: str = "",
) -> dict[str, str | bool]:
    project_root = resolve_workspace_path(workspace_path)
    artifact_manifest = get_artifact_manifest(project_root)
    workspace_index = get_workspace_index(project_root)
    manifest_entries = _artifact_manifest_entries_by_path(artifact_manifest)

    if not _section_exists(section_id, workspace_index, manifest_entries):
        raise FileNotFoundError(f"Section not found: {section_id}")

    required_artifacts = {
        "input": project_root / ".dce" / "input" / f"{section_id}.json",
        "preview": project_root / ".dce" / "plans" / f"{section_id}.preview.json",
        "review": project_root / ".dce" / "reviews" / f"{section_id}.review.md",
    }
    missing_artifacts = [name for name, artifact_path in required_artifacts.items() if not artifact_path.exists()]
    if missing_artifacts:
        raise ValueError(
            f"Section approval requires current artifacts: {', '.join(missing_artifacts)}"
        )

    workspace_index_entry = _workspace_index_entry(workspace_index, section_id)
    if not _section_artifacts_valid(project_root, manifest_entries, workspace_index_entry, section_id):
        raise ValueError(f"Section artifacts are invalid: {section_id}")

    preview_payload = json.loads(required_artifacts["preview"].read_text(encoding="utf-8"))
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(
            approval_status="approved",
            selected_mode=str(preview_payload.get("recommended_mode", "review_required")),
            approval_source="operator",
            approved_by=approved_by,
            notes=notes,
        ),
    )
    refresh_section_artifacts(project_root, section_id)
    return {
        "status": "ok",
        "section_id": section_id,
        "approved": True,
    }
