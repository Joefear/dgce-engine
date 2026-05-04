"""Isolated Game Adapter Stage 2 preview dispatch from released GCE Stage 0.

The lifecycle handoff remains narrow: when a workspace is provided, the
validated Stage 2 preview is immediately converted into a Stage 3 review bundle
and persisted for operator review. No approval, gate, alignment, simulation, or
execution behavior is created here.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from aether.dgce.context_assembly import release_gce_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint, _write_json_with_artifact_fingerprint
from aether.dgce.game_adapter_preview import (
    build_game_adapter_stage2_preview,
    validate_game_adapter_stage2_preview_contract,
)
from packages.dgce_contracts.game_adapter_stage3_review_bundle_artifacts import (
    Stage3ReviewBundlePersistResult,
    persist_stage3_review_bundle_v1,
)
from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import build_stage3_review_bundle_v1


GAME_ADAPTER_STAGE2_PREVIEW_ID = "game-adapter-stage2"
GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH = Path(".dce") / "plans" / f"{GAME_ADAPTER_STAGE2_PREVIEW_ID}.preview.json"
GAME_ADAPTER_STAGE3_REVIEW_CREATED_AT = "1970-01-01T00:00:00Z"


@dataclass(frozen=True)
class GameAdapterStage2PreviewDispatchResult:
    """Result for the Game Adapter Stage 2 preview and optional Stage 3 handoff."""

    preview_artifact: dict[str, Any]
    artifact_path: str | None
    release_result: dict[str, Any]
    stage3_review_bundle_artifact: dict[str, Any] | None = None
    stage3_review_artifact_path: str | None = None
    stage3_review_read_model: dict[str, Any] | None = None


@dataclass(frozen=True)
class GameAdapterStage3ReviewLifecycleResult:
    """Result for the bounded Game Adapter Stage 3 lifecycle handoff."""

    review_bundle_artifact: dict[str, Any]
    artifact_path: str | None
    read_model: dict[str, Any] | None


def build_game_adapter_stage2_preview_from_released_stage0(
    stage0_source: dict[str, Any] | str | Path,
    *,
    workspace_path: str | Path | None = None,
    preview_id: str = GAME_ADAPTER_STAGE2_PREVIEW_ID,
    resolver_input: dict[str, Any] | None = None,
    resolver_manifest_payload: dict[str, Any] | None = None,
    resolver_candidate_index_payload: dict[str, Any] | None = None,
    build_stage3_review: bool = True,
    stage3_section_id: str | None = None,
    stage3_created_at: str = GAME_ADAPTER_STAGE3_REVIEW_CREATED_AT,
    stage3_operator_questions: list[str] | None = None,
) -> GameAdapterStage2PreviewDispatchResult:
    """Build and optionally persist Game Adapter Stage 2 and Stage 3 artifacts.

    The dispatch consumes the locked GCE Stage
    0 release result, extracts bounded Game Adapter planned changes from the
    normalized session intent, builds the preview contract, and then persists a
    bounded Stage 3 review bundle when a workspace is provided. It stops before
    approval and does not perform Stage 4-9 behavior.
    """
    release = release_gce_stage0_input(stage0_source)
    release_payload = dict(release.result)
    if release.allowed is not True:
        raise ValueError(f"stage0_release_blocked:{release_payload.get('reason_code') or 'unknown'}")

    normalized_session_intent = release_payload.get("normalized_session_intent")
    if not isinstance(normalized_session_intent, dict):
        raise ValueError("normalized_session_intent_missing")

    planned_changes = _extract_game_adapter_planned_changes(normalized_session_intent)
    preview = build_game_adapter_stage2_preview(
        source_stage0_fingerprint=release_payload.get("source_artifact_fingerprint"),
        source_input_reference=_source_input_reference(release_payload, normalized_session_intent),
        planned_changes=planned_changes,
        policy_pack="game_adapter_stage2_preview",
        guardrail_required=True,
        resolver_input=resolver_input,
        resolver_manifest_payload=resolver_manifest_payload,
        resolver_candidate_index_payload=resolver_candidate_index_payload,
    )
    validate_game_adapter_stage2_preview_contract(preview)

    artifact_path: str | None = None
    if workspace_path is not None:
        relative_path = Path(".dce") / "plans" / f"{_safe_preview_id(preview_id)}.preview.json"
        persisted = _write_json_with_artifact_fingerprint(_resolve_workspace_path(workspace_path) / relative_path, preview)
        validate_game_adapter_stage2_preview_contract(persisted)
        preview = persisted
        artifact_path = relative_path.as_posix()

    stage3_review: GameAdapterStage3ReviewLifecycleResult | None = None
    if build_stage3_review and workspace_path is not None:
        stage3_review = build_game_adapter_stage3_review_bundle_from_stage2_preview(
            preview,
            workspace_path=workspace_path,
            section_id=stage3_section_id or _stage3_section_id(normalized_session_intent, preview_id),
            created_at=stage3_created_at,
            source_preview_reference=artifact_path,
            operator_questions=stage3_operator_questions,
        )

    return GameAdapterStage2PreviewDispatchResult(
        preview_artifact=preview,
        artifact_path=artifact_path,
        release_result=release_payload,
        stage3_review_bundle_artifact=stage3_review.review_bundle_artifact if stage3_review else None,
        stage3_review_artifact_path=stage3_review.artifact_path if stage3_review else None,
        stage3_review_read_model=stage3_review.read_model if stage3_review else None,
    )


def build_game_adapter_stage3_review_bundle_from_stage2_preview(
    stage2_preview: dict[str, Any] | str | Path,
    *,
    workspace_path: str | Path | None = None,
    section_id: str,
    created_at: str = GAME_ADAPTER_STAGE3_REVIEW_CREATED_AT,
    review_id: str | None = None,
    source_preview_reference: str | None = None,
    operator_questions: list[str] | None = None,
) -> GameAdapterStage3ReviewLifecycleResult:
    """Build and optionally persist Stage 3 from structured Stage 2 preview data."""
    preview_payload = _load_stage2_preview_payload(stage2_preview)
    source_preview_fingerprint = _stage3_source_preview_fingerprint(preview_payload)
    planned_changes = _stage3_planned_changes_from_preview(preview_payload)
    bundle = build_stage3_review_bundle_v1(
        review_id=review_id or _stage3_review_id(section_id, source_preview_fingerprint),
        section_id=section_id,
        created_at=created_at,
        source_preview_fingerprint=source_preview_fingerprint,
        source_input_fingerprint=_stage3_source_input_fingerprint(preview_payload),
        planned_changes=planned_changes,
        operator_questions=operator_questions,
        evidence=[{"source": "preview", "reference": source_preview_reference or "stage2_preview"}],
    )
    if workspace_path is None:
        return GameAdapterStage3ReviewLifecycleResult(
            review_bundle_artifact=bundle,
            artifact_path=None,
            read_model=None,
        )
    persisted: Stage3ReviewBundlePersistResult = persist_stage3_review_bundle_v1(
        bundle,
        workspace_path=workspace_path,
        section_id=section_id,
    )
    return GameAdapterStage3ReviewLifecycleResult(
        review_bundle_artifact=persisted.review_bundle_artifact,
        artifact_path=persisted.artifact_path,
        read_model=persisted.read_model,
    )


def _extract_game_adapter_planned_changes(normalized_session_intent: dict[str, Any]) -> list[dict[str, Any]]:
    sections = normalized_session_intent.get("sections")
    if not isinstance(sections, list):
        raise ValueError("normalized_session_intent.sections_missing")

    candidate_changes: list[dict[str, Any]] = []
    for index, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        content = section.get("content")
        if not isinstance(content, dict):
            continue
        if not _declares_game_adapter_stage2_domain(content):
            continue
        preview_content = content.get("game_adapter_stage2_preview")
        if isinstance(preview_content, dict) and "planned_changes" in preview_content:
            planned_changes = preview_content.get("planned_changes")
        else:
            planned_changes = content.get("planned_changes")
        if not isinstance(planned_changes, list):
            raise ValueError(f"sections[{index}].planned_changes_missing")
        candidate_changes.extend(dict(change) for change in planned_changes if isinstance(change, dict))

    if not candidate_changes:
        raise ValueError("game_adapter_stage2_domain_missing")
    return candidate_changes


def _declares_game_adapter_stage2_domain(content: dict[str, Any]) -> bool:
    return (
        content.get("adapter_domain") == "game_adapter"
        or content.get("domain") == "game_adapter"
        or content.get("adapter") == "game"
        or isinstance(content.get("game_adapter_stage2_preview"), dict)
    )


def _stage3_section_id(normalized_session_intent: dict[str, Any], preview_id: str) -> str:
    sections = normalized_session_intent.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue
            content = section.get("content")
            section_id = section.get("section_id")
            if isinstance(content, dict) and _declares_game_adapter_stage2_domain(content):
                if isinstance(section_id, str) and section_id.strip():
                    return section_id
    return _safe_preview_id(preview_id)


def _load_stage2_preview_payload(stage2_preview: dict[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(stage2_preview, (str, Path)):
        payload = json.loads(Path(stage2_preview).read_text(encoding="utf-8"))
    else:
        payload = dict(stage2_preview)
    if not isinstance(payload, dict):
        raise ValueError("stage2_preview must be an object")
    return payload


def _stage3_source_preview_fingerprint(preview_payload: dict[str, Any]) -> str:
    fingerprint = preview_payload.get("artifact_fingerprint")
    if isinstance(fingerprint, str) and _is_sha256_fingerprint(fingerprint):
        return fingerprint
    return compute_preview_payload_fingerprint(preview_payload)


def compute_preview_payload_fingerprint(preview_payload: dict[str, Any]) -> str:
    """Return a deterministic source fingerprint without mutating the preview."""
    return compute_json_payload_fingerprint(preview_payload)


def _stage3_source_input_fingerprint(preview_payload: dict[str, Any]) -> str | None:
    source_stage0_fingerprint = preview_payload.get("source_stage0_fingerprint")
    if isinstance(source_stage0_fingerprint, str) and _is_sha256_fingerprint(source_stage0_fingerprint):
        return source_stage0_fingerprint
    return None


def _stage3_planned_changes_from_preview(preview_payload: dict[str, Any]) -> list[dict[str, Any]]:
    planned_changes = preview_payload.get("planned_changes")
    if isinstance(planned_changes, list):
        return [dict(change) for change in planned_changes if isinstance(change, dict)]

    machine_view = preview_payload.get("machine_view")
    machine_changes = machine_view.get("changes") if isinstance(machine_view, dict) else None
    if not isinstance(machine_changes, list):
        return []

    mapped_changes: list[dict[str, Any]] = []
    for change in machine_changes:
        if not isinstance(change, dict):
            continue
        mapped = {
            "change_id": change.get("change_id"),
            "target": {
                "target_id": change.get("target_id"),
                "target_path": change.get("target_path"),
                "target_kind": change.get("target_kind"),
            },
            "operation": change.get("operation"),
            "domain_type": change.get("domain_type"),
            "strategy": change.get("strategy"),
        }
        summary_codes = change.get("summary_codes")
        if isinstance(summary_codes, dict):
            mapped["summary"] = dict(summary_codes)
        mapped_changes.append(mapped)
    return mapped_changes


def _stage3_review_id(section_id: str, source_preview_fingerprint: str) -> str:
    return f"review:{section_id}:{source_preview_fingerprint[:16]}"


def _is_sha256_fingerprint(value: str) -> bool:
    return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def _source_input_reference(release_payload: dict[str, Any], normalized_session_intent: dict[str, Any]) -> str:
    for value in (
        release_payload.get("input_path"),
        normalized_session_intent.get("source_input_path"),
    ):
        if isinstance(value, str) and value:
            return value
    return "gce-stage0"


def _safe_preview_id(preview_id: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(preview_id).strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    normalized = normalized.strip("-")
    if not normalized:
        raise ValueError("preview_id must not be empty")
    return normalized


def _resolve_workspace_path(workspace_path: str | Path) -> Path:
    raw_path = Path(workspace_path)
    resolved = raw_path if raw_path.is_absolute() else Path.cwd() / raw_path
    resolved = resolved.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def load_game_adapter_stage2_preview_artifact(path: str | Path) -> dict[str, Any]:
    """Load and validate one persisted Game Adapter Stage 2 preview artifact."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("game_adapter_stage2_preview must be an object")
    validate_game_adapter_stage2_preview_contract(payload)
    return payload


__all__ = [
    "GAME_ADAPTER_STAGE2_PREVIEW_ID",
    "GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH",
    "GAME_ADAPTER_STAGE3_REVIEW_CREATED_AT",
    "GameAdapterStage2PreviewDispatchResult",
    "GameAdapterStage3ReviewLifecycleResult",
    "build_game_adapter_stage3_review_bundle_from_stage2_preview",
    "build_game_adapter_stage2_preview_from_released_stage0",
    "compute_preview_payload_fingerprint",
    "load_game_adapter_stage2_preview_artifact",
]
