import json
from pathlib import Path

import pytest

from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import (
    FORBIDDEN_RUNTIME_ACTIONS,
    build_stage3_review_bundle_v1,
    validate_stage3_review_bundle_v1,
)


FIXTURE_DIR = Path("packages/dgce-contracts/fixtures/game_adapter/stage3_review_bundle")
TIMESTAMP = "2026-05-03T13:00:00Z"
PREVIEW_FP = "1111111111111111111111111111111111111111111111111111111111111111"
INPUT_FP = "2222222222222222222222222222222222222222222222222222222222222222"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _planned_change(
    *,
    change_id: str = "change.player-inventory-component",
    target_path: str = "Content/Blueprints/BP_Player.uasset",
    target_kind: str = "BlueprintClass",
    operation: str = "modify",
    strategy: str = "Blueprint",
    risk: str = "medium",
) -> dict:
    return {
        "change_id": change_id,
        "target": {
            "target_id": "BP_Player.InventoryComponent",
            "target_path": target_path,
            "target_kind": target_kind,
        },
        "operation": operation,
        "strategy": strategy,
        "summary": {
            "intent": "add_gameplay_capability",
            "impact": "gameplay",
            "risk": risk,
            "review_focus": "component_setup",
        },
    }


def _build(**kwargs) -> dict:
    defaults = {
        "review_id": "review:mission-board:test",
        "section_id": "mission-board",
        "created_at": TIMESTAMP,
        "source_preview_fingerprint": PREVIEW_FP,
        "source_input_fingerprint": INPUT_FP,
        "planned_changes": [_planned_change()],
        "review_summary": {
            "title": "Mission Board Review",
            "primary_intent": "Review inventory gameplay changes.",
            "operator_summary": "Review one bounded game adapter change.",
            "risk_summary": "Medium risk bounded review only.",
        },
        "evidence": [
            {
                "source": "preview",
                "reference": ".dce/plans/game-adapter-stage2.preview.json",
            }
        ],
    }
    defaults.update(kwargs)
    return build_stage3_review_bundle_v1(**defaults)


def test_stage3_review_bundle_builder_ready_minimal_validates():
    bundle = _build()

    assert validate_stage3_review_bundle_v1(bundle) is True
    assert bundle["review_status"] == "ready_for_operator_review"
    assert bundle["approval_readiness"] == {
        "ready_for_approval": True,
        "blocking_review_issues_count": 0,
        "informational_review_issues_count": 0,
    }
    assert bundle["forbidden_runtime_actions"] == FORBIDDEN_RUNTIME_ACTIONS


def test_stage3_review_bundle_builder_multiple_proposed_changes_validate():
    bundle = _build(
        planned_changes=[
            _planned_change(change_id="change.player-inventory-component"),
            _planned_change(
                change_id="change.inventory-cpp",
                target_path="Source/FixtureGame/Public/InventoryComponent.h",
                target_kind="CppClass",
                operation="create",
                strategy="C++",
            ),
            _planned_change(
                change_id="change.design-note",
                target_path="Docs/InventoryReview.md",
                target_kind="documentation",
                operation="ignore",
                strategy="none",
                risk="low",
            ),
        ],
        evidence=[
            {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
            {"source": "unreal_manifest", "reference": ".dce/plans/unreal-project-structure.manifest.json#BP_Player"},
            {"source": "symbol_candidate_index", "reference": ".dce/plans/unreal-symbol-candidates.index.json#InventoryComponent"},
            {
                "source": "resolver",
                "reference": ".dce/plans/unreal-symbol-resolver.resolution.json#BP_Player",
                "snippet_hash": "3333333333333333333333333333333333333333333333333333333333333333",
            },
            {"source": "alignment", "reference": ".dce/execution/alignment/mission-board.alignment.json"},
        ],
    )

    assert validate_stage3_review_bundle_v1(bundle) is True
    assert [change["change_id"] for change in bundle["proposed_changes"]] == [
        "change.design-note",
        "change.inventory-cpp",
        "change.player-inventory-component",
    ]
    assert {change["target_kind"] for change in bundle["proposed_changes"]} == {"blueprint", "cpp", "documentation"}
    assert {change["output_strategy"] for change in bundle["proposed_changes"]} == {"blueprint", "cpp", "none"}


def test_stage3_review_bundle_builder_blocked_with_operator_questions_validates():
    bundle = _build(
        planned_changes=[
            {
                "change_id": "change.missing-target",
                "operation": "modify",
                "strategy": "Blueprint",
            }
        ],
        operator_questions=["Which Unreal asset should receive the requested gameplay change?"],
    )

    assert validate_stage3_review_bundle_v1(bundle) is True
    assert bundle["review_status"] == "blocked"
    assert bundle["approval_readiness"]["ready_for_approval"] is False
    assert bundle["approval_readiness"]["blocking_review_issues_count"] == 2
    assert bundle["proposed_changes"][0]["target_path"] == "unknown/target-001"
    assert "Provide structured target_path" in " ".join(bundle["operator_questions"])


def test_stage3_review_bundle_builder_maps_stage2_planned_outputs_deterministically():
    bundle = _build(
        planned_changes=[
            _planned_change(
                change_id="change.input-action",
                target_path="Content/Input/IA_Interact.uasset",
                target_kind="InputAction",
                operation="create",
                strategy="Blueprint",
            )
        ]
    )

    assert bundle["proposed_changes"] == [
        {
            "change_id": "change.input-action",
            "target_path": "Content/Input/IA_Interact.uasset",
            "target_kind": "asset",
            "operation": "create",
            "output_strategy": "blueprint",
            "human_readable_summary": (
                "Review create change change.input-action for Content/Input/IA_Interact.uasset "
                "using blueprint output strategy."
            ),
            "review_risk": "medium",
        }
    ]


def test_stage3_review_bundle_builder_blocks_missing_required_structured_preview_data():
    bundle = _build(planned_changes=[])

    assert validate_stage3_review_bundle_v1(bundle) is True
    assert bundle["review_status"] == "blocked"
    assert bundle["approval_readiness"] == {
        "ready_for_approval": False,
        "blocking_review_issues_count": 1,
        "informational_review_issues_count": 0,
    }
    assert bundle["operator_questions"] == [
        "Provide at least one structured Stage 2 planned change before approval review can proceed."
    ]
    assert bundle["proposed_changes"] == []


def test_stage3_review_bundle_builder_rejects_approval_execution_simulation_and_policy_fields():
    forbidden_inputs = [
        {"approval_status": "approved"},
        {"execution_permitted": True},
        {"simulation_result": {"status": "passed"}},
        {"guardrail_policy_decision": "ALLOW"},
        {"stage8_write_instructions": ["write asset"]},
        {"blueprint_mutation": {"node": "AddComponent"}},
    ]

    for forbidden in forbidden_inputs:
        planned_change = _planned_change()
        planned_change.update(forbidden)
        with pytest.raises(ValueError, match="forbidden runtime fields"):
            _build(planned_changes=[planned_change])


def test_stage3_review_bundle_builder_populates_forbidden_runtime_actions():
    bundle = _build()

    assert bundle["forbidden_runtime_actions"] == [
        "no_approval_granted",
        "no_execution_performed",
        "no_stage8_write_instructions",
        "no_blueprint_mutation",
        "no_unreal_project_writes",
        "no_binary_blueprint_parsing",
        "no_simulation_run",
        "no_guardrail_policy_decision",
    ]


def test_stage3_review_bundle_builder_evidence_remains_bounded():
    bundle = _build(
        evidence=[
            {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
            {"source": "operator_context", "reference": "operator_context:mission-board-note"},
        ]
    )

    assert validate_stage3_review_bundle_v1(bundle) is True
    assert bundle["evidence"] == [
        {"source": "operator_context", "reference": "operator_context:mission-board-note"},
        {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
    ]
    serialized = json.dumps(bundle["evidence"], sort_keys=True)
    assert "raw_preview" not in serialized
    assert "full_symbol_table" not in serialized
    assert "raw_resolver_payload" not in serialized

    with pytest.raises(ValueError, match="unsupported fields|forbidden runtime fields"):
        _build(evidence=[{"source": "preview", "reference": "preview", "raw_artifact": {"full": "payload"}}])


def test_stage3_review_bundle_builder_output_is_stable_across_repeated_runs():
    first = _build()
    second = _build()

    assert first == second


def test_stage3_review_bundle_locked_fixtures_still_validate():
    for fixture_name in (
        "ready_minimal.json",
        "ready_with_multiple_changes.json",
        "blocked_with_operator_questions.json",
    ):
        assert validate_stage3_review_bundle_v1(_fixture(fixture_name)) is True
