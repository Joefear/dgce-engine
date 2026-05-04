"""Persistence and read-model helpers for Game Adapter Stage 3 review bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import validate_stage3_review_bundle_v1


STAGE3_REVIEW_BUNDLE_RELATIVE_DIR = Path(".dce") / "review"


@dataclass(frozen=True)
class Stage3ReviewBundlePersistResult:
    review_bundle_artifact: dict[str, Any]
    artifact_path: str
    read_model: dict[str, Any]


def persist_stage3_review_bundle_v1(
    review_bundle: Mapping[str, Any],
    *,
    workspace_path: str | Path,
    section_id: str,
) -> Stage3ReviewBundlePersistResult:
    """Persist one contract-valid Stage 3 review bundle artifact."""
    payload = dict(review_bundle)
    validate_stage3_review_bundle_v1(payload)
    safe_section_id = _safe_section_id(section_id)
    relative_path = STAGE3_REVIEW_BUNDLE_RELATIVE_DIR / f"{safe_section_id}.stage3_review.json"
    workspace_root = _resolve_workspace(workspace_path)
    artifact_path = workspace_root / relative_path
    _write_json(artifact_path, payload)
    return Stage3ReviewBundlePersistResult(
        review_bundle_artifact=payload,
        artifact_path=relative_path.as_posix(),
        read_model=build_stage3_review_bundle_read_model_v1(safe_section_id, payload),
    )


def load_stage3_review_bundle_read_model_v1(
    workspace_path: str | Path,
    section_id: str,
) -> dict[str, Any]:
    """Load a persisted Stage 3 review bundle and return its bounded read model."""
    safe_section_id = _safe_section_id(section_id)
    workspace_root = _resolve_workspace(workspace_path, create=False)
    artifact_path = workspace_root / STAGE3_REVIEW_BUNDLE_RELATIVE_DIR / f"{safe_section_id}.stage3_review.json"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError("stage3 review bundle artifact missing") from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("stage3 review bundle artifact malformed") from exc
    if not isinstance(payload, dict):
        raise ValueError("stage3 review bundle artifact malformed")
    validate_stage3_review_bundle_v1(payload)
    return build_stage3_review_bundle_read_model_v1(safe_section_id, payload)


def build_stage3_review_bundle_read_model_v1(section_id: str, review_bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Build a compact operator-facing projection from a Stage 3 review bundle."""
    safe_section_id = _safe_section_id(section_id)
    payload = dict(review_bundle)
    validate_stage3_review_bundle_v1(payload)
    approval_readiness = payload["approval_readiness"]
    proposed_changes = [dict(item) for item in payload["proposed_changes"]]
    return {
        "section_id": safe_section_id,
        "review_id": payload["review_id"],
        "review_status": payload["review_status"],
        "ready_for_approval": approval_readiness["ready_for_approval"],
        "blocking_review_issues_count": approval_readiness["blocking_review_issues_count"],
        "informational_review_issues_count": approval_readiness["informational_review_issues_count"],
        "proposed_change_count": len(proposed_changes),
        "proposed_change_targets": sorted({str(item["target_path"]) for item in proposed_changes}),
        "proposed_change_operations": sorted({str(item["operation"]) for item in proposed_changes}),
        "output_strategies": sorted({str(item["output_strategy"]) for item in proposed_changes}),
        "review_risk_summary": _review_risk_summary(proposed_changes),
        "operator_question_count": len(payload["operator_questions"]),
        "evidence_sources": sorted({str(item["source"]) for item in payload["evidence"]}),
        "forbidden_runtime_actions": list(payload["forbidden_runtime_actions"]),
    }


def stage3_review_bundle_artifact_path(workspace_path: str | Path, section_id: str) -> Path:
    """Return the canonical on-disk path for one Stage 3 review bundle artifact."""
    return (
        _resolve_workspace(workspace_path, create=False)
        / STAGE3_REVIEW_BUNDLE_RELATIVE_DIR
        / f"{_safe_section_id(section_id)}.stage3_review.json"
    )


def _review_risk_summary(proposed_changes: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"low": 0, "medium": 0, "high": 0}
    for change in proposed_changes:
        risk = str(change["review_risk"])
        if risk in counts:
            counts[risk] += 1
    return counts


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
    "STAGE3_REVIEW_BUNDLE_RELATIVE_DIR",
    "Stage3ReviewBundlePersistResult",
    "build_stage3_review_bundle_read_model_v1",
    "load_stage3_review_bundle_read_model_v1",
    "persist_stage3_review_bundle_v1",
    "stage3_review_bundle_artifact_path",
]
