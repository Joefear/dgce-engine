"""Persistence and read-model helpers for Stage 7 alignment_record.v1 artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from packages.dgce_contracts.alignment_builder import validate_alignment_record_v1


ALIGNMENT_ARTIFACT_RELATIVE_DIR = Path(".dce") / "execution" / "alignment"


@dataclass(frozen=True)
class AlignmentRecordPersistResult:
    alignment_record_artifact: dict[str, Any]
    artifact_path: str
    read_model: dict[str, Any]


def persist_alignment_record_v1(
    alignment_record: Mapping[str, Any],
    *,
    workspace_path: str | Path,
    section_id: str,
) -> AlignmentRecordPersistResult:
    """Persist one contract-valid alignment_record.v1 artifact."""
    payload = dict(alignment_record)
    validate_alignment_record_v1(payload)
    safe_section_id = _safe_section_id(section_id)
    relative_path = ALIGNMENT_ARTIFACT_RELATIVE_DIR / f"{safe_section_id}.alignment.json"
    workspace_root = _resolve_workspace(workspace_path)
    artifact_path = workspace_root / relative_path
    _write_json(artifact_path, payload)
    return AlignmentRecordPersistResult(
        alignment_record_artifact=payload,
        artifact_path=relative_path.as_posix(),
        read_model=build_alignment_record_read_model_v1(safe_section_id, payload),
    )


def load_alignment_record_read_model_v1(
    workspace_path: str | Path,
    section_id: str,
) -> dict[str, Any]:
    """Load a persisted alignment artifact and return its bounded read model."""
    safe_section_id = _safe_section_id(section_id)
    workspace_root = _resolve_workspace(workspace_path, create=False)
    artifact_path = workspace_root / ALIGNMENT_ARTIFACT_RELATIVE_DIR / f"{safe_section_id}.alignment.json"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError("alignment artifact missing") from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("alignment artifact malformed") from exc
    if not isinstance(payload, dict):
        raise ValueError("alignment artifact malformed")
    validate_alignment_record_v1(payload)
    return build_alignment_record_read_model_v1(safe_section_id, payload)


def build_alignment_record_read_model_v1(section_id: str, alignment_record: Mapping[str, Any]) -> dict[str, Any]:
    """Build a compact operator-facing projection from an alignment record."""
    safe_section_id = _safe_section_id(section_id)
    payload = dict(alignment_record)
    validate_alignment_record_v1(payload)
    summary = payload["alignment_summary"]
    enrichment = payload["alignment_enrichment"]
    return {
        "section_id": safe_section_id,
        "alignment_id": payload["alignment_id"],
        "alignment_result": payload["alignment_result"],
        "drift_detected": payload["drift_detected"],
        "execution_permitted": payload["execution_permitted"],
        "blocking_issues_count": summary["blocking_issues_count"],
        "informational_issues_count": summary["informational_issues_count"],
        "primary_reason": summary["primary_reason"],
        "drift_codes": sorted({item["code"] for item in payload["drift_items"]}),
        "evidence_sources": sorted({item["source"] for item in payload["evidence"]}),
        "enrichment_status": enrichment["enrichment_status"],
        "code_graph_used": enrichment["code_graph_used"],
        "resolver_used": enrichment["resolver_used"],
    }


def alignment_record_artifact_path(workspace_path: str | Path, section_id: str) -> Path:
    """Return the canonical on-disk path for one alignment artifact."""
    return (
        _resolve_workspace(workspace_path, create=False)
        / ALIGNMENT_ARTIFACT_RELATIVE_DIR
        / f"{_safe_section_id(section_id)}.alignment.json"
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _safe_section_id(section_id: str) -> str:
    if not isinstance(section_id, str) or not section_id:
        raise ValueError("section_id must be a non-empty string")
    if Path(section_id).name != section_id:
        raise ValueError("section_id must not contain path separators")
    normalized = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in section_id.strip())
    normalized = normalized.strip(".-_")
    if not normalized:
        raise ValueError("section_id must contain a safe filename character")
    return normalized


def _resolve_workspace(workspace_path: str | Path, *, create: bool = True) -> Path:
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
    if create:
        resolved_path.mkdir(parents=True, exist_ok=True)
    return resolved_path


__all__ = [
    "ALIGNMENT_ARTIFACT_RELATIVE_DIR",
    "AlignmentRecordPersistResult",
    "alignment_record_artifact_path",
    "build_alignment_record_read_model_v1",
    "load_alignment_record_read_model_v1",
    "persist_alignment_record_v1",
]
