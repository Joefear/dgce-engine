"""Governed DGCE section execution helpers for the HTTP transport layer."""

from __future__ import annotations

from pathlib import Path

from aether.dgce import run_dgce_section
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import prepare_section_execution


def execute_prepared_section(workspace_path: str | Path, section_id: str) -> dict[str, str | bool]:
    project_root = resolve_workspace_path(workspace_path)
    preparation = prepare_section_execution(project_root, section_id)
    if preparation["eligible"] is not True:
        raise ValueError(f"Section is not eligible for execution: {section_id}")

    result = run_dgce_section(section_id, project_root, governed=True)
    if str(result.status) != "success":
        raise ValueError(f"Section execution blocked: {result.reason}")

    return {
        "status": "ok",
        "section_id": section_id,
        "executed": True,
        "artifacts_updated": True,
    }
