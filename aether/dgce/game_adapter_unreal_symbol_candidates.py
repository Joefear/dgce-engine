"""Path-derived Unreal symbol candidate index for Game Adapter Stage 2.

This is not a symbol resolver. It consumes only the bounded Unreal project
structure manifest payload and derives filename/path candidates for later
resolver work.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aether.dgce.decompose import _write_json_with_artifact_fingerprint, compute_json_payload_fingerprint
from aether.dgce.game_adapter_unreal_manifest import (
    ADAPTER,
    DOMAIN,
    validate_unreal_project_structure_manifest,
)


ARTIFACT_TYPE = "game_adapter_unreal_symbol_candidate_index"
CONTRACT_NAME = "DGCEGameAdapterUnrealSymbolCandidateIndex"
CONTRACT_VERSION = "dgce.game_adapter.unreal_symbol_candidate_index.v1"
UNREAL_SYMBOL_CANDIDATE_INDEX_ID = "unreal-symbol-candidates"
UNREAL_SYMBOL_CANDIDATE_INDEX_RELATIVE_PATH = (
    Path(".dce") / "plans" / f"{UNREAL_SYMBOL_CANDIDATE_INDEX_ID}.index.json"
)
DEFAULT_MAX_CANDIDATES = 2000
RESOLUTION_STATUS = "path_candidate"

_PATH_CATEGORY_TO_KIND = {
    "uproject_files": "uproject",
    "source_module_directories": "source_module",
    "cpp_headers": "cpp_header",
    "cpp_sources": "cpp_source",
    "blueprint_assets": "blueprint_asset",
    "config_directories": "config_directory",
}


@dataclass(frozen=True)
class UnrealSymbolCandidateIndexPersistResult:
    candidate_index_artifact: dict[str, Any]
    artifact_path: str


def build_unreal_symbol_candidate_index(
    manifest_payload: dict[str, Any],
    *,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> dict[str, Any]:
    """Build a deterministic candidate index from manifest paths only."""
    if not isinstance(max_candidates, int) or max_candidates < 1:
        raise ValueError("max_candidates must be a positive integer")
    validate_unreal_project_structure_manifest(manifest_payload)
    discovered_paths = manifest_payload.get("discovered_paths")
    if not isinstance(discovered_paths, dict) or set(discovered_paths) != set(_PATH_CATEGORY_TO_KIND):
        raise ValueError("manifest discovered_paths contains unsupported path categories")

    candidates = _candidate_entries(discovered_paths)
    if len(candidates) > max_candidates:
        raise ValueError("symbol candidate index exceeds max_candidates")

    payload = {
        "artifact_type": ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": ADAPTER,
        "domain": DOMAIN,
        "source_manifest_fingerprint": manifest_payload["artifact_fingerprint"],
        "structural_summary": dict(manifest_payload["structural_summary"]),
        "candidates": candidates,
    }
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    validate_unreal_symbol_candidate_index(payload)
    return payload


def persist_unreal_symbol_candidate_index(
    manifest_payload: dict[str, Any],
    *,
    workspace_path: str | Path,
    index_id: str = UNREAL_SYMBOL_CANDIDATE_INDEX_ID,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> UnrealSymbolCandidateIndexPersistResult:
    """Persist the candidate index under a DGCE preview-safe `.dce/plans` path."""
    candidate_index = build_unreal_symbol_candidate_index(
        manifest_payload,
        max_candidates=max_candidates,
    )
    relative_path = Path(".dce") / "plans" / f"{_safe_index_id(index_id)}.index.json"
    workspace_root = _resolve_index_workspace(workspace_path)
    persisted = _write_json_with_artifact_fingerprint(workspace_root / relative_path, candidate_index)
    validate_unreal_symbol_candidate_index(persisted)
    return UnrealSymbolCandidateIndexPersistResult(
        candidate_index_artifact=persisted,
        artifact_path=relative_path.as_posix(),
    )


def validate_unreal_symbol_candidate_index(payload: dict[str, Any]) -> bool:
    """Validate the candidate-index artifact and fingerprint."""
    if not isinstance(payload, dict):
        raise ValueError("candidate index must be an object")
    expected_keys = {
        "artifact_type",
        "contract_name",
        "contract_version",
        "adapter",
        "domain",
        "source_manifest_fingerprint",
        "structural_summary",
        "candidates",
        "artifact_fingerprint",
    }
    if set(payload) != expected_keys:
        raise ValueError("candidate index fields are not canonical")
    if payload["artifact_type"] != ARTIFACT_TYPE:
        raise ValueError("artifact_type is invalid")
    if payload["contract_name"] != CONTRACT_NAME:
        raise ValueError("contract_name is invalid")
    if payload["contract_version"] != CONTRACT_VERSION:
        raise ValueError("contract_version is invalid")
    if payload["adapter"] != ADAPTER or payload["domain"] != DOMAIN:
        raise ValueError("adapter/domain is invalid")
    if not isinstance(payload["source_manifest_fingerprint"], str) or not payload["source_manifest_fingerprint"]:
        raise ValueError("source_manifest_fingerprint is required")
    if not isinstance(payload["structural_summary"], dict):
        raise ValueError("structural_summary must be an object")
    _validate_candidates(payload["candidates"])
    if payload["artifact_fingerprint"] != compute_json_payload_fingerprint(payload):
        raise ValueError("artifact_fingerprint invalid")
    return True


def _candidate_entries(discovered_paths: dict[str, list[str]]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for category in sorted(_PATH_CATEGORY_TO_KIND):
        paths = discovered_paths.get(category)
        if not isinstance(paths, list):
            raise ValueError(f"manifest discovered_paths.{category} must be an array")
        for source_path in paths:
            if not isinstance(source_path, str) or not source_path:
                raise ValueError(f"manifest discovered_paths.{category} contains invalid path")
            candidates.append(
                {
                    "candidate_name": _candidate_name(source_path),
                    "candidate_kind": _PATH_CATEGORY_TO_KIND[category],
                    "source_path": source_path,
                    "resolution_status": RESOLUTION_STATUS,
                }
            )
    return sorted(candidates, key=lambda candidate: (candidate["candidate_kind"], candidate["source_path"]))


def _candidate_name(source_path: str) -> str:
    path = Path(source_path)
    if ".." in path.parts or path.is_absolute() or "\\" in source_path:
        raise ValueError("source_path must be a bounded relative path")
    name = path.stem if path.suffix else path.name
    if not name:
        raise ValueError("candidate_name could not be derived")
    return name


def _validate_candidates(candidates: Any) -> None:
    if not isinstance(candidates, list) or candidates != sorted(
        candidates,
        key=lambda candidate: (candidate.get("candidate_kind"), candidate.get("source_path"))
        if isinstance(candidate, dict)
        else ("", ""),
    ):
        raise ValueError("candidates must be a sorted array")
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) != {
            "candidate_name",
            "candidate_kind",
            "source_path",
            "resolution_status",
        }:
            raise ValueError("candidate fields are not canonical")
        if candidate["candidate_kind"] not in set(_PATH_CATEGORY_TO_KIND.values()):
            raise ValueError("candidate_kind is unsupported")
        if candidate["resolution_status"] != RESOLUTION_STATUS:
            raise ValueError("resolution_status is unsupported")
        if candidate["candidate_name"] != _candidate_name(candidate["source_path"]):
            raise ValueError("candidate_name must be path-derived")


def _safe_index_id(index_id: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(index_id).strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    normalized = normalized.strip("-")
    if not normalized or not normalized.startswith(UNREAL_SYMBOL_CANDIDATE_INDEX_ID):
        raise ValueError("index_id must start with unreal-symbol-candidates")
    return normalized


def _resolve_index_workspace(workspace_path: str | Path) -> Path:
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


__all__ = [
    "ARTIFACT_TYPE",
    "CONTRACT_NAME",
    "CONTRACT_VERSION",
    "DEFAULT_MAX_CANDIDATES",
    "RESOLUTION_STATUS",
    "UNREAL_SYMBOL_CANDIDATE_INDEX_ID",
    "UNREAL_SYMBOL_CANDIDATE_INDEX_RELATIVE_PATH",
    "UnrealSymbolCandidateIndexPersistResult",
    "build_unreal_symbol_candidate_index",
    "persist_unreal_symbol_candidate_index",
    "validate_unreal_symbol_candidate_index",
]
