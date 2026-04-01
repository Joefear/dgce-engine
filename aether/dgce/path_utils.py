"""Deterministic path validation helpers for DGCE workspaces."""

from __future__ import annotations

from pathlib import Path


def resolve_workspace_path(workspace_path: str | Path) -> Path:
    raw_path = Path(workspace_path)
    base_root = Path.cwd().resolve()
    resolved_path = (base_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()

    if not raw_path.is_absolute():
        try:
            resolved_path.relative_to(base_root)
        except ValueError as exc:
            raise ValueError("workspace_path must remain within the current working directory") from exc

    if not resolved_path.exists():
        raise FileNotFoundError(f"Workspace path does not exist: {resolved_path}")
    if not resolved_path.is_dir():
        raise ValueError(f"Workspace path must be a directory: {resolved_path}")
    if not (resolved_path / ".dce").is_dir():
        raise ValueError(f"Workspace path must contain a .dce directory: {resolved_path}")

    return resolved_path
