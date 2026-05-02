import copy
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import (
    assemble_stage0_input,
    persist_stage0_input,
    release_gce_stage0_input,
)
from aether.dgce.decompose import compute_json_payload_fingerprint, verify_artifact_fingerprint
from aether.dgce.file_plan import FilePlan
from aether.dgce.game_adapter_preview import (
    ARTIFACT_TYPE,
    render_game_adapter_stage2_human_view,
    render_game_adapter_stage2_machine_view,
    validate_game_adapter_stage2_preview_contract,
)
from aether.dgce.game_adapter_stage2_dispatch import (
    GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH,
    build_game_adapter_stage2_preview_from_released_stage0,
    load_game_adapter_stage2_preview_artifact,
)
from aether.dgce.incremental import build_incremental_preview_artifact


def _workspace_dir(name: str) -> Path:
    base = Path("tests/.tmp") / name
    if base.exists():
        for path in sorted(base.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        if base.exists():
            base.rmdir()
    return base


def _metadata() -> dict:
    return {
        "project_id": "frontier-colony",
        "project_name": "Frontier Colony",
        "owner": "Design Authority",
        "source_id": "frontier-colony-stage2",
        "created_at": "2026-04-27T00:00:00Z",
        "updated_at": "2026-04-27T00:00:00Z",
    }


def _planned_changes() -> list[dict]:
    return [
        {
            "change_id": "change.player-component",
            "target": {
                "target_id": "BP_Player.InventoryComponent",
                "target_path": "/Game/Blueprints/BP_Player",
                "target_kind": "ActorComponent",
            },
            "operation": "modify",
            "domain_type": "component",
            "strategy": "Blueprint",
            "summary": {
                "intent": "add_gameplay_capability",
                "impact": "gameplay",
                "risk": "medium",
                "review_focus": "component_setup",
            },
        },
        {
            "change_id": "change.interact-binding",
            "target": {
                "target_id": "IA_Interact",
                "target_path": "/Game/Input/IA_Interact",
                "target_kind": "InputAction",
            },
            "operation": "create",
            "domain_type": "binding",
            "strategy": "Blueprint",
            "summary": {
                "intent": "connect_existing_systems",
                "impact": "input",
                "risk": "low",
                "review_focus": "event_binding",
            },
        },
    ]


def _game_adapter_stage0_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": _metadata(),
        "intent": {
            "session_objective": "Generate a bounded Game Adapter Stage 2 preview.",
            "sections": [
                {
                    "section_id": "game_adapter_stage2_scope",
                    "title": "Game Adapter Stage 2 Scope",
                    "classification": "durable",
                    "authorship": "human",
                    "required": True,
                    "content": {
                        "adapter_domain": "game_adapter",
                        "game_adapter_stage2_preview": {
                            "planned_changes": _planned_changes(),
                        },
                    },
                },
                {
                    "section_id": "current_state",
                    "title": "Current State",
                    "classification": "volatile",
                    "authorship": "injected",
                    "required": False,
                    "content": {
                        "placeholder": "registered_stage_0_source",
                    },
                },
            ],
        },
        "ambiguities": [],
    }


def _blocked_game_adapter_stage0_input() -> dict:
    payload = _game_adapter_stage0_input()
    payload["ambiguities"] = [
        {
            "field_path": "intent.sections[0].content.game_adapter_stage2_preview.planned_changes",
            "question": "Which bounded Game Adapter changes are approved?",
            "blocking": True,
        }
    ]
    return payload


def test_valid_released_gce_session_intent_produces_game_adapter_stage2_preview_artifact():
    project_root = _workspace_dir("game_adapter_stage2_preview_valid_release")
    persisted = persist_stage0_input(project_root, _game_adapter_stage0_input())

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    preview_path = project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH
    assert result.artifact_path == GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH.as_posix()
    assert preview_path.exists()
    assert verify_artifact_fingerprint(preview_path) is True
    assert result.preview_artifact == load_game_adapter_stage2_preview_artifact(preview_path)
    assert result.preview_artifact["artifact_type"] == ARTIFACT_TYPE
    assert result.preview_artifact["source_stage0_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert result.preview_artifact["source_input_reference"] == "structured_intent"
    assert result.preview_artifact["governance_context"]["guardrail_required"] is True
    assert result.preview_artifact["machine_view"]["change_count"] == 2
    assert len(result.preview_artifact["human_view"]["rows"]) == 2


def test_game_adapter_stage2_preview_artifact_validates_against_contract():
    persisted = persist_stage0_input(
        _workspace_dir("game_adapter_stage2_preview_contract_validation"),
        _game_adapter_stage0_input(),
    )

    result = build_game_adapter_stage2_preview_from_released_stage0(persisted.artifact)

    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["artifact_fingerprint"] == compute_json_payload_fingerprint(result.preview_artifact)


def test_game_adapter_stage2_preview_machine_and_human_views_are_deterministic():
    persisted = persist_stage0_input(
        _workspace_dir("game_adapter_stage2_preview_deterministic_views"),
        _game_adapter_stage0_input(),
    )

    first = build_game_adapter_stage2_preview_from_released_stage0(persisted.artifact).preview_artifact
    second = build_game_adapter_stage2_preview_from_released_stage0(copy.deepcopy(persisted.artifact)).preview_artifact

    assert first == second
    assert first["machine_view"] == render_game_adapter_stage2_machine_view(
        first["planned_changes"],
        first["governance_context"],
    )
    assert first["human_view"] == render_game_adapter_stage2_human_view(
        first["planned_changes"],
        first["governance_context"],
    )


def test_game_adapter_stage2_preview_blocked_stage0_release_fails_closed():
    project_root = _workspace_dir("game_adapter_stage2_preview_blocked_stage0")
    persisted = persist_stage0_input(project_root, _blocked_game_adapter_stage0_input())

    with pytest.raises(ValueError, match="stage0_release_blocked:clarification_required"):
        build_game_adapter_stage2_preview_from_released_stage0(project_root / persisted.artifact_path, workspace_path=project_root)

    assert not (project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH).exists()
    assert not (project_root / ".dce" / "execution").exists()


def test_game_adapter_stage2_preview_missing_normalized_session_intent_fails_closed():
    package = assemble_stage0_input(_game_adapter_stage0_input()).package
    package["normalized_session_intent"] = None

    with pytest.raises(ValueError, match="stage0_release_blocked:normalized_session_intent_missing"):
        build_game_adapter_stage2_preview_from_released_stage0(package)


def test_game_adapter_stage2_preview_missing_game_adapter_domain_fails_closed():
    package = assemble_stage0_input(_game_adapter_stage0_input()).package
    package["normalized_session_intent"]["sections"][0]["content"] = {
        "purpose": "Software-only GCE intent without Game Adapter Stage 2 marker.",
    }

    with pytest.raises(ValueError, match="game_adapter_stage2_domain_missing"):
        build_game_adapter_stage2_preview_from_released_stage0(package)


def test_game_adapter_stage2_preview_does_not_create_execution_artifacts():
    project_root = _workspace_dir("game_adapter_stage2_preview_no_execution_artifacts")
    persisted = persist_stage0_input(project_root, _game_adapter_stage0_input())

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert result.preview_artifact["artifact_type"] == ARTIFACT_TYPE
    assert not (project_root / ".dce" / "execution").exists()
    assert not (project_root / ".dce" / "outputs").exists()
    assert "execution_status" not in result.preview_artifact
    assert "written_files" not in result.preview_artifact


def test_game_adapter_stage2_dispatch_leaves_software_preview_behavior_unchanged():
    project_root = _workspace_dir("game_adapter_stage2_dispatch_software_unchanged")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": "api/mission.py",
                "purpose": "Mission API",
                "source": "expected_targets",
            }
        ],
    )

    preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        [],
        project_root,
        mode="incremental_v2_2",
    )

    assert preview["section_id"] == "mission-board"
    assert preview["mode"] == "incremental_v2_2"
    assert preview["preview_outcome_class"] == "preview_create_only"
    assert preview["recommended_mode"] == "create_only"
    assert preview["previews"][0]["path"] == "api/mission.py"
    assert preview["previews"][0]["planned_action"] == "create"
    assert "machine_view" not in preview
    assert "human_view" not in preview


def test_game_adapter_stage2_dispatch_does_not_change_gce_stage0_release_behavior():
    project_root = _workspace_dir("game_adapter_stage2_dispatch_stage0_untouched")
    persisted = persist_stage0_input(project_root, _game_adapter_stage0_input())
    before_release = release_gce_stage0_input(project_root / persisted.artifact_path)

    build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )
    after_release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert before_release == after_release
    assert before_release.allowed is True
    assert before_release.result["source_artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
