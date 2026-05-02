"""Isolated Game Adapter Stage 2 preview dispatch from released GCE Stage 0."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from aether.dgce.context_assembly import release_gce_stage0_input
from aether.dgce.decompose import _write_json_with_artifact_fingerprint
from aether.dgce.game_adapter_preview import (
    build_game_adapter_stage2_preview,
    validate_game_adapter_stage2_preview_contract,
)


GAME_ADAPTER_STAGE2_PREVIEW_ID = "game-adapter-stage2"
GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH = Path(".dce") / "plans" / f"{GAME_ADAPTER_STAGE2_PREVIEW_ID}.preview.json"


@dataclass(frozen=True)
class GameAdapterStage2PreviewDispatchResult:
    """Result for preview-only Game Adapter Stage 2 dispatch."""

    preview_artifact: dict[str, Any]
    artifact_path: str | None
    release_result: dict[str, Any]


def build_game_adapter_stage2_preview_from_released_stage0(
    stage0_source: dict[str, Any] | str | Path,
    *,
    workspace_path: str | Path | None = None,
    preview_id: str = GAME_ADAPTER_STAGE2_PREVIEW_ID,
    resolver_input: dict[str, Any] | None = None,
    resolver_manifest_payload: dict[str, Any] | None = None,
    resolver_candidate_index_payload: dict[str, Any] | None = None,
) -> GameAdapterStage2PreviewDispatchResult:
    """Build and optionally persist a Game Adapter Stage 2 preview artifact.

    The dispatch is intentionally preview-only. It consumes the locked GCE Stage
    0 release result, extracts bounded Game Adapter planned changes from the
    normalized session intent, builds the preview contract, and stops before any
    Stage 3-9 behavior.
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

    return GameAdapterStage2PreviewDispatchResult(
        preview_artifact=preview,
        artifact_path=artifact_path,
        release_result=release_payload,
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
    "GameAdapterStage2PreviewDispatchResult",
    "build_game_adapter_stage2_preview_from_released_stage0",
    "load_game_adapter_stage2_preview_artifact",
]
