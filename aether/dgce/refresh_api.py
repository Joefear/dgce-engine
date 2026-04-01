"""Thin wrapper for deterministic DGCE workspace view refresh."""

from __future__ import annotations

from pathlib import Path

from aether.dgce.decompose import _ensure_workspace, _refresh_workspace_views
from aether.dgce.path_utils import resolve_workspace_path


def refresh_workspace_artifacts(workspace_path: str | Path) -> Path:
    project_root = resolve_workspace_path(workspace_path)
    workspace = _ensure_workspace(project_root)
    _refresh_workspace_views(workspace)
    return project_root
