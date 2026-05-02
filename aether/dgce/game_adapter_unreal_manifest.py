"""Read-only Unreal-like project structure manifest for Game Adapter Stage 2.

This module records bounded structural facts only. It does not parse file
contents, inspect Blueprint binaries, resolve symbols, validate Blueprint
graphs, or perform execution writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aether.dgce.decompose import _write_json_with_artifact_fingerprint, compute_json_payload_fingerprint


ARTIFACT_TYPE = "game_adapter_unreal_project_structure_manifest"
CONTRACT_NAME = "DGCEGameAdapterUnrealProjectStructureManifest"
CONTRACT_VERSION = "dgce.game_adapter.unreal_project_structure_manifest.v1"
ADAPTER = "game"
DOMAIN = "game_adapter"
DEFAULT_MAX_DISCOVERED_PATHS = 2000
UNREAL_PROJECT_STRUCTURE_MANIFEST_ID = "unreal-project-structure"
UNREAL_PROJECT_STRUCTURE_MANIFEST_RELATIVE_PATH = (
    Path(".dce") / "plans" / f"{UNREAL_PROJECT_STRUCTURE_MANIFEST_ID}.manifest.json"
)

_HEADER_SUFFIXES = (".h", ".hpp", ".hh", ".hxx")
_SOURCE_SUFFIXES = (".cc", ".cpp", ".cxx")
_BLUEPRINT_LIKE_SUFFIXES = (".uasset",)


@dataclass(frozen=True)
class UnrealProjectStructureManifestPersistResult:
    manifest_artifact: dict[str, Any]
    artifact_path: str


def build_unreal_project_structure_manifest(
    project_path: str | Path,
    *,
    max_discovered_paths: int = DEFAULT_MAX_DISCOVERED_PATHS,
) -> dict[str, Any]:
    """Build a deterministic read-only manifest for an Unreal-like project tree."""
    if not isinstance(max_discovered_paths, int) or max_discovered_paths < 1:
        raise ValueError("max_discovered_paths must be a positive integer")

    project_root = _resolve_project_root(project_path)
    discovered_paths = _discover_paths(project_root, max_discovered_paths=max_discovered_paths)
    if not _supported_project_shape(discovered_paths):
        raise ValueError("unsupported Unreal project structure")

    payload = {
        "artifact_type": ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": ADAPTER,
        "domain": DOMAIN,
        "project_root_reference": _project_root_reference(project_root),
        "structural_summary": _structural_summary(discovered_paths),
        "discovered_paths": discovered_paths,
    }
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    validate_unreal_project_structure_manifest(payload)
    return payload


def persist_unreal_project_structure_manifest(
    project_path: str | Path,
    *,
    workspace_path: str | Path,
    manifest_id: str = UNREAL_PROJECT_STRUCTURE_MANIFEST_ID,
    max_discovered_paths: int = DEFAULT_MAX_DISCOVERED_PATHS,
) -> UnrealProjectStructureManifestPersistResult:
    """Persist the manifest artifact under a DGCE preview-safe `.dce/plans` path."""
    manifest = build_unreal_project_structure_manifest(
        project_path,
        max_discovered_paths=max_discovered_paths,
    )
    relative_path = Path(".dce") / "plans" / f"{_safe_manifest_id(manifest_id)}.manifest.json"
    workspace_root = _resolve_manifest_workspace(workspace_path)
    persisted = _write_json_with_artifact_fingerprint(workspace_root / relative_path, manifest)
    validate_unreal_project_structure_manifest(persisted)
    return UnrealProjectStructureManifestPersistResult(
        manifest_artifact=persisted,
        artifact_path=relative_path.as_posix(),
    )


def validate_unreal_project_structure_manifest(payload: dict[str, Any]) -> bool:
    """Validate the bounded manifest shape and fingerprint."""
    if not isinstance(payload, dict):
        raise ValueError("manifest must be an object")
    expected_keys = {
        "artifact_type",
        "contract_name",
        "contract_version",
        "adapter",
        "domain",
        "project_root_reference",
        "structural_summary",
        "discovered_paths",
        "artifact_fingerprint",
    }
    if set(payload) != expected_keys:
        raise ValueError("manifest fields are not canonical")
    if payload["artifact_type"] != ARTIFACT_TYPE:
        raise ValueError("artifact_type is invalid")
    if payload["contract_name"] != CONTRACT_NAME:
        raise ValueError("contract_name is invalid")
    if payload["contract_version"] != CONTRACT_VERSION:
        raise ValueError("contract_version is invalid")
    if payload["adapter"] != ADAPTER or payload["domain"] != DOMAIN:
        raise ValueError("adapter/domain is invalid")
    if not isinstance(payload["project_root_reference"], str) or not payload["project_root_reference"]:
        raise ValueError("project_root_reference is required")
    _validate_discovered_paths(payload["discovered_paths"])
    _validate_structural_summary(payload["structural_summary"], payload["discovered_paths"])
    if payload["artifact_fingerprint"] != compute_json_payload_fingerprint(payload):
        raise ValueError("artifact_fingerprint invalid")
    return True


def _safe_manifest_id(manifest_id: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(manifest_id).strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    normalized = normalized.strip("-")
    if not normalized or not normalized.startswith(UNREAL_PROJECT_STRUCTURE_MANIFEST_ID):
        raise ValueError("manifest_id must start with unreal-project-structure")
    return normalized


def _resolve_manifest_workspace(workspace_path: str | Path) -> Path:
    raw_path = Path(workspace_path)
    if ".." in raw_path.parts:
        raise ValueError("workspace_path must not contain traversal segments")
    base_root = Path.cwd().resolve()
    resolved_path = (base_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
    if not raw_path.is_absolute():
        try:
            resolved_path.relative_to(base_root)
        except ValueError as exc:
            raise ValueError("workspace_path must remain within the current working directory") from exc
    resolved_path.mkdir(parents=True, exist_ok=True)
    return resolved_path


def _resolve_project_root(project_path: str | Path) -> Path:
    raw_path = Path(project_path)
    if ".." in raw_path.parts:
        raise ValueError("project_path must not contain traversal segments")
    base_root = Path.cwd().resolve()
    resolved_path = (base_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
    if not raw_path.is_absolute():
        try:
            resolved_path.relative_to(base_root)
        except ValueError as exc:
            raise ValueError("project_path must remain within the current working directory") from exc
    if not resolved_path.exists():
        raise FileNotFoundError(f"Project root does not exist: {resolved_path}")
    if not resolved_path.is_dir():
        raise ValueError(f"Project root must be a directory: {resolved_path}")
    return resolved_path


def _discover_paths(project_root: Path, *, max_discovered_paths: int) -> dict[str, list[str]]:
    source_root = project_root / "Source"
    content_root = project_root / "Content"
    config_root = project_root / "Config"
    discovered = {
        "uproject_files": _relative_files(project_root, project_root.glob("*.uproject")),
        "source_module_directories": _source_module_directories(project_root, source_root),
        "cpp_headers": _relative_files(
            project_root,
            _files_with_suffixes(source_root, _HEADER_SUFFIXES),
        ),
        "cpp_sources": _relative_files(
            project_root,
            _files_with_suffixes(source_root, _SOURCE_SUFFIXES),
        ),
        "blueprint_assets": _relative_files(
            project_root,
            _files_with_suffixes(content_root, _BLUEPRINT_LIKE_SUFFIXES),
        ),
        "config_directories": ["Config"] if config_root.is_dir() else [],
    }
    total_paths = sum(len(paths) for paths in discovered.values())
    if total_paths > max_discovered_paths:
        raise ValueError("project structure exceeds max_discovered_paths")
    return discovered


def _relative_files(project_root: Path, paths: Any) -> list[str]:
    relative_paths: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        relative_paths.append(_relative_path(project_root, path))
    return sorted(relative_paths)


def _files_with_suffixes(root: Path, suffixes: tuple[str, ...]) -> list[Path]:
    if not root.is_dir():
        return []
    return [path for path in root.rglob("*") if path.is_file() and path.suffix in suffixes]


def _source_module_directories(project_root: Path, source_root: Path) -> list[str]:
    if not source_root.is_dir():
        return []
    module_directories = [
        _relative_path(project_root, path)
        for path in source_root.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    ]
    return sorted(module_directories)


def _relative_path(project_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError("discovered path must remain inside project root") from exc


def _supported_project_shape(discovered_paths: dict[str, list[str]]) -> bool:
    return any(
        discovered_paths[key]
        for key in (
            "uproject_files",
            "source_module_directories",
            "cpp_headers",
            "cpp_sources",
            "blueprint_assets",
            "config_directories",
        )
    )


def _project_root_reference(project_root: Path) -> str:
    base_root = Path.cwd().resolve()
    try:
        return project_root.resolve().relative_to(base_root).as_posix()
    except ValueError:
        return project_root.name


def _structural_summary(discovered_paths: dict[str, list[str]]) -> dict[str, Any]:
    total_paths = sum(len(paths) for paths in discovered_paths.values())
    return {
        "uproject_file_count": len(discovered_paths["uproject_files"]),
        "source_module_count": len(discovered_paths["source_module_directories"]),
        "cpp_header_count": len(discovered_paths["cpp_headers"]),
        "cpp_source_count": len(discovered_paths["cpp_sources"]),
        "blueprint_asset_count": len(discovered_paths["blueprint_assets"]),
        "config_present": bool(discovered_paths["config_directories"]),
        "total_discovered_path_count": total_paths,
    }


def _validate_discovered_paths(discovered_paths: Any) -> None:
    expected_keys = {
        "uproject_files",
        "source_module_directories",
        "cpp_headers",
        "cpp_sources",
        "blueprint_assets",
        "config_directories",
    }
    if not isinstance(discovered_paths, dict) or set(discovered_paths) != expected_keys:
        raise ValueError("discovered_paths fields are not canonical")
    for key, paths in discovered_paths.items():
        if not isinstance(paths, list) or paths != sorted(paths):
            raise ValueError(f"discovered_paths.{key} must be a sorted array")
        for path in paths:
            if not isinstance(path, str) or not path or path.startswith("/") or "\\" in path:
                raise ValueError(f"discovered_paths.{key} contains invalid path")
            if ".." in Path(path).parts:
                raise ValueError(f"discovered_paths.{key} contains traversal")


def _validate_structural_summary(summary: Any, discovered_paths: dict[str, list[str]]) -> None:
    expected_summary = _structural_summary(discovered_paths)
    if summary != expected_summary:
        raise ValueError("structural_summary does not match discovered_paths")


__all__ = [
    "ADAPTER",
    "ARTIFACT_TYPE",
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "DEFAULT_MAX_DISCOVERED_PATHS",
    "DOMAIN",
    "UNREAL_PROJECT_STRUCTURE_MANIFEST_ID",
    "UNREAL_PROJECT_STRUCTURE_MANIFEST_RELATIVE_PATH",
    "UnrealProjectStructureManifestPersistResult",
    "build_unreal_project_structure_manifest",
    "persist_unreal_project_structure_manifest",
    "validate_unreal_project_structure_manifest",
]
