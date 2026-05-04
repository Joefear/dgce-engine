import json
from pathlib import Path

from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.game_adapter_stage2_dispatch import (
    build_game_adapter_stage2_preview_from_released_stage0,
    build_game_adapter_stage3_review_bundle_from_stage2_preview,
)
from aether.dgce.read_api import get_game_adapter_stage3_review_bundle_read_model
from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import validate_stage3_review_bundle_v1


TIMESTAMP = "2026-05-04T12:00:00Z"
READ_MODEL_FIELDS = {
    "section_id",
    "review_id",
    "review_status",
    "ready_for_approval",
    "blocking_review_issues_count",
    "informational_review_issues_count",
    "proposed_change_count",
    "proposed_change_targets",
    "proposed_change_operations",
    "output_strategies",
    "review_risk_summary",
    "operator_question_count",
    "evidence_sources",
    "forbidden_runtime_actions",
}
FORBIDDEN_READ_MODEL_FIELDS = {
    "source_preview_fingerprint",
    "source_input_fingerprint",
    "proposed_changes",
    "evidence",
    "raw_preview",
    "raw_symbols",
    "symbol_table",
    "resolver_payload",
    "model_text",
}


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
        "metadata": {
            "project_id": "frontier-colony",
            "project_name": "Frontier Colony",
            "owner": "Design Authority",
            "source_id": "frontier-colony-stage3",
            "created_at": "2026-05-04T00:00:00Z",
            "updated_at": "2026-05-04T00:00:00Z",
        },
        "intent": {
            "session_objective": "Generate a bounded Game Adapter review bundle.",
            "sections": [
                {
                    "section_id": "mission-board",
                    "title": "Mission Board",
                    "classification": "durable",
                    "authorship": "human",
                    "required": True,
                    "content": {
                        "adapter_domain": "game_adapter",
                        "game_adapter_stage2_preview": {
                            "planned_changes": _planned_changes(),
                        },
                    },
                }
            ],
        },
        "ambiguities": [],
    }


def _persisted_stage0(workspace: Path) -> Path:
    persisted = persist_stage0_input(workspace, _game_adapter_stage0_input())
    return workspace / persisted.artifact_path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_no_downstream_artifacts(workspace: Path, section_id: str = "mission-board") -> None:
    assert not (workspace / ".dce" / "approvals").exists()
    assert not (workspace / ".dce" / "approval").exists()
    assert not (workspace / ".dce" / "execution" / "gate" / f"{section_id}.execution_gate.json").exists()
    assert not (workspace / ".dce" / "execution" / "alignment" / f"{section_id}.alignment.json").exists()
    assert not (workspace / ".dce" / "execution" / "simulation").exists()
    assert not (workspace / ".dce" / "execution" / "stage8").exists()
    assert not (workspace / ".dce" / "execution" / f"{section_id}.execution.json").exists()
    assert not (workspace / ".dce" / "outputs").exists()
    assert not (workspace / ".dce" / "output").exists()


def _workspace_files(workspace: Path) -> list[str]:
    return sorted(path.relative_to(workspace).as_posix() for path in workspace.rglob("*") if path.is_file())


def test_stage3_review_bundle_is_produced_after_stage2_preview_and_readable():
    workspace = _workspace_dir("stage3_lifecycle_ready")

    result = build_game_adapter_stage2_preview_from_released_stage0(
        _persisted_stage0(workspace),
        workspace_path=workspace,
        stage3_created_at=TIMESTAMP,
    )
    preview_path = workspace / ".dce" / "plans" / "game-adapter-stage2.preview.json"
    review_path = workspace / ".dce" / "review" / "mission-board.stage3_review.json"
    review_bundle = _read_json(review_path)

    assert preview_path.exists()
    assert result.stage3_review_artifact_path == ".dce/review/mission-board.stage3_review.json"
    assert review_path.exists()
    assert review_bundle["source_preview_fingerprint"] == result.preview_artifact["artifact_fingerprint"]
    assert validate_stage3_review_bundle_v1(review_bundle) is True
    assert get_game_adapter_stage3_review_bundle_read_model(workspace, "mission-board") == result.stage3_review_read_model
    assert result.stage3_review_read_model["review_status"] == "ready_for_operator_review"
    assert set(result.stage3_review_read_model) == READ_MODEL_FIELDS
    assert result.stage3_review_read_model["proposed_change_targets"] == [
        "/Game/Blueprints/BP_Player",
        "/Game/Input/IA_Interact",
    ]


def test_ready_stage3_review_does_not_auto_approve_or_create_approval_artifacts():
    workspace = _workspace_dir("stage3_lifecycle_ready_no_approval")

    result = build_game_adapter_stage2_preview_from_released_stage0(
        _persisted_stage0(workspace),
        workspace_path=workspace,
        stage3_created_at=TIMESTAMP,
    )

    assert result.stage3_review_read_model["ready_for_approval"] is True
    assert result.stage3_review_bundle_artifact["approval_readiness"]["ready_for_approval"] is True
    assert result.stage3_review_bundle_artifact["review_status"] == "ready_for_operator_review"
    _assert_no_downstream_artifacts(workspace)


def test_blocked_stage3_review_does_not_proceed_to_stage4_6_7_75_or_8():
    workspace = _workspace_dir("stage3_lifecycle_blocked_questions")

    result = build_game_adapter_stage2_preview_from_released_stage0(
        _persisted_stage0(workspace),
        workspace_path=workspace,
        stage3_created_at=TIMESTAMP,
        stage3_operator_questions=["Confirm the exact Blueprint asset before approval review can proceed."],
    )

    assert result.stage3_review_artifact_path == ".dce/review/mission-board.stage3_review.json"
    assert result.stage3_review_read_model["review_status"] == "blocked"
    assert result.stage3_review_read_model["ready_for_approval"] is False
    assert result.stage3_review_read_model["operator_question_count"] == 1
    _assert_no_downstream_artifacts(workspace)


def test_lifecycle_stage3_writes_only_review_artifact_after_stage2_preview():
    workspace = _workspace_dir("stage3_lifecycle_only_review_after_preview")
    stage0_path = _persisted_stage0(workspace)

    result = build_game_adapter_stage2_preview_from_released_stage0(
        stage0_path,
        workspace_path=workspace,
        build_stage3_review=False,
        stage3_created_at=TIMESTAMP,
    )
    before_stage3_files = _workspace_files(workspace)

    stage3 = build_game_adapter_stage3_review_bundle_from_stage2_preview(
        result.preview_artifact,
        workspace_path=workspace,
        section_id="mission-board",
        created_at=TIMESTAMP,
        source_preview_reference=result.artifact_path,
    )
    after_stage3_files = _workspace_files(workspace)

    assert stage3.artifact_path == ".dce/review/mission-board.stage3_review.json"
    assert sorted(set(after_stage3_files) - set(before_stage3_files)) == [
        ".dce/review/mission-board.stage3_review.json"
    ]
    _assert_no_downstream_artifacts(workspace)


def test_lifecycle_read_api_projection_excludes_raw_and_fingerprint_fields():
    workspace = _workspace_dir("stage3_lifecycle_read_surface_bounded")

    build_game_adapter_stage2_preview_from_released_stage0(
        _persisted_stage0(workspace),
        workspace_path=workspace,
        stage3_created_at=TIMESTAMP,
    )
    read_model = get_game_adapter_stage3_review_bundle_read_model(workspace, "mission-board")
    serialized = json.dumps(read_model, sort_keys=True)

    assert set(read_model) == READ_MODEL_FIELDS
    assert len(read_model) == 14
    for forbidden in FORBIDDEN_READ_MODEL_FIELDS:
        assert forbidden not in read_model
        if forbidden != "evidence":
            assert forbidden not in serialized


def test_missing_structured_stage2_change_data_persists_blocked_review_only():
    workspace = _workspace_dir("stage3_lifecycle_blocked_missing_structured_data")
    preview = {
        "artifact_fingerprint": "1" * 64,
        "source_stage0_fingerprint": "2" * 64,
        "machine_view": {"changes": []},
    }

    result = build_game_adapter_stage3_review_bundle_from_stage2_preview(
        preview,
        workspace_path=workspace,
        section_id="mission-board",
        created_at=TIMESTAMP,
        source_preview_reference=".dce/plans/game-adapter-stage2.preview.json",
    )

    assert result.artifact_path == ".dce/review/mission-board.stage3_review.json"
    assert result.read_model["review_status"] == "blocked"
    assert result.read_model["ready_for_approval"] is False
    assert result.review_bundle_artifact["operator_questions"] == [
        "Provide at least one structured Stage 2 planned change before approval review can proceed."
    ]
    assert validate_stage3_review_bundle_v1(result.review_bundle_artifact) is True
    _assert_no_downstream_artifacts(workspace)


def test_lifecycle_stage3_writes_no_stage8_unreal_project_or_blueprint_assets():
    workspace = _workspace_dir("stage3_lifecycle_no_unreal_writes")
    unreal_project = workspace / "FixtureGame" / "FixtureGame.uproject"
    blueprint_asset = workspace / "FixtureGame" / "Content" / "Blueprints" / "BP_Player.uasset"
    unreal_project.parent.mkdir(parents=True)
    blueprint_asset.parent.mkdir(parents=True)
    unreal_project.write_text('{"FileVersion":3}\n', encoding="utf-8")
    blueprint_asset.write_bytes(b"binary-blueprint-placeholder")
    before_project = unreal_project.read_bytes()
    before_blueprint = blueprint_asset.read_bytes()

    build_game_adapter_stage2_preview_from_released_stage0(
        _persisted_stage0(workspace),
        workspace_path=workspace,
        stage3_created_at=TIMESTAMP,
    )

    assert unreal_project.read_bytes() == before_project
    assert blueprint_asset.read_bytes() == before_blueprint
    assert (workspace / ".dce" / "review" / "mission-board.stage3_review.json").exists()
    assert not (workspace / ".dce" / "execution" / "stage8").exists()
    assert not (workspace / ".dce" / "execution" / "output").exists()
    _assert_no_downstream_artifacts(workspace)
