import copy
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import (
    apply_game_adapter_stage2_strategy_selection,
    build_game_adapter_stage2_preview,
    select_game_adapter_stage2_strategy,
)
from aether.dgce.game_adapter_stage2_dispatch import (
    GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH,
    build_game_adapter_stage2_preview_from_released_stage0,
)


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


def _planned_change(
    *,
    change_id: str = "change.player-component",
    domain_type: str = "component",
    target_kind: str = "ActorComponent",
    target_id: str = "BP_Player.InventoryComponent",
    target_path: str = "/Game/Blueprints/BP_Player",
    intent: str = "add_gameplay_capability",
    review_focus: str = "component_setup",
    strategy: str | None = None,
) -> dict:
    change = {
        "change_id": change_id,
        "target": {
            "target_id": target_id,
            "target_path": target_path,
            "target_kind": target_kind,
        },
        "operation": "modify",
        "domain_type": domain_type,
        "summary": {
            "intent": intent,
            "impact": "gameplay",
            "risk": "medium",
            "review_focus": review_focus,
        },
    }
    if strategy is not None:
        change["strategy"] = strategy
    return change


def _stage0_input(planned_changes: list[dict]) -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": {
            "project_id": "strategy-fixture",
            "project_name": "Strategy Fixture",
            "owner": "Design Authority",
            "source_id": "strategy-selector",
            "created_at": "2026-04-27T00:00:00Z",
            "updated_at": "2026-04-27T00:00:00Z",
        },
        "intent": {
            "session_objective": "Generate bounded Game Adapter strategy preview.",
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
                            "planned_changes": planned_changes,
                        },
                    },
                }
            ],
        },
        "ambiguities": [],
    }


def test_blueprint_intended_planned_change_selects_blueprint():
    change = _planned_change(domain_type="component", target_kind="ActorComponent")

    assert select_game_adapter_stage2_strategy(change) == "Blueprint"


def test_cpp_intended_planned_change_selects_cpp():
    change = _planned_change(
        change_id="change.native-subsystem",
        domain_type="C++",
        target_kind="CppClass",
        target_id="UInventorySubsystem",
        target_path="/Source/Game/InventorySubsystem",
        intent="adjust_existing_behavior",
        review_focus="logic_flow",
    )

    assert select_game_adapter_stage2_strategy(change) == "C++"


def test_mixed_system_level_planned_change_selects_both():
    change = _planned_change(
        change_id="change.inventory-system",
        domain_type="component",
        target_kind="ActorComponent",
        intent="prepare_for_review",
        review_focus="logic_flow",
    )

    assert select_game_adapter_stage2_strategy(change) == "both"


def test_unknown_or_unsupported_domain_type_fails_closed():
    change = _planned_change(domain_type="material")

    with pytest.raises(ValueError, match="domain_type"):
        select_game_adapter_stage2_strategy(change)


def test_explicit_mismatched_strategy_fails_closed():
    change = _planned_change(
        domain_type="C++",
        target_kind="CppClass",
        target_id="UInventorySubsystem",
        target_path="/Source/Game/InventorySubsystem",
        strategy="Blueprint",
    )

    with pytest.raises(ValueError, match="does not match selected strategy"):
        apply_game_adapter_stage2_strategy_selection([change])


def test_strategy_selection_records_strategy_in_machine_and_human_views():
    changes = [
        _planned_change(change_id="change.bp", domain_type="component", target_kind="ActorComponent"),
        _planned_change(
            change_id="change.cpp",
            domain_type="C++",
            target_kind="CppClass",
            target_id="UInventorySubsystem",
            target_path="/Source/Game/InventorySubsystem",
            intent="adjust_existing_behavior",
            review_focus="logic_flow",
        ),
        _planned_change(
            change_id="change.both",
            domain_type="event",
            target_kind="Event",
            target_id="OnInventoryChanged",
            target_path="/Game/Blueprints/BP_Player",
            intent="prepare_for_review",
            review_focus="logic_flow",
        ),
    ]

    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=changes,
    )

    strategies = {change["change_id"]: change["strategy"] for change in preview["planned_changes"]}
    machine_strategies = {change["change_id"]: change["strategy"] for change in preview["machine_view"]["changes"]}
    human_strategies = {row["change_id"]: row["strategy"] for row in preview["human_view"]["rows"]}
    assert strategies == {
        "change.both": "both",
        "change.bp": "Blueprint",
        "change.cpp": "C++",
    }
    assert machine_strategies == strategies
    assert human_strategies == strategies


def test_strategy_selection_is_deterministic():
    changes = [
        _planned_change(change_id="change.bp", domain_type="component", target_kind="ActorComponent"),
        _planned_change(
            change_id="change.cpp",
            domain_type="C++",
            target_kind="CppClass",
            target_id="UInventorySubsystem",
            target_path="/Source/Game/InventorySubsystem",
        ),
    ]

    first = build_game_adapter_stage2_preview(source_input_reference="gce-session-1", planned_changes=changes)
    second = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=copy.deepcopy(changes),
    )

    assert first == second


def test_dispatch_applies_strategy_selection_before_preview_artifact_creation():
    project_root = _workspace_dir("game_adapter_stage2_strategy_dispatch")
    change = _planned_change(
        change_id="change.native-subsystem",
        domain_type="C++",
        target_kind="CppClass",
        target_id="UInventorySubsystem",
        target_path="/Source/Game/InventorySubsystem",
    )
    persisted = persist_stage0_input(project_root, _stage0_input([change]))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert result.preview_artifact["planned_changes"][0]["strategy"] == "C++"
    assert (project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH).exists()
    assert not (project_root / ".dce" / "execution").exists()
    assert not (project_root / ".dce" / "output").exists()
    assert not (project_root / ".dce" / "outputs").exists()


def test_strategy_selector_keeps_stage75_lifecycle_order_locked():
    assert dgce_decompose.DGCE_LIFECYCLE_ORDER == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]
