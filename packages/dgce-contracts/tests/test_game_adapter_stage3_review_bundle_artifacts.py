import copy
import json
from pathlib import Path

import pytest

from packages.dgce_contracts.game_adapter_stage3_review_bundle_artifacts import (
    build_stage3_review_bundle_read_model_v1,
    load_stage3_review_bundle_read_model_v1,
    persist_stage3_review_bundle_v1,
    stage3_review_bundle_artifact_path,
)
from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import (
    build_stage3_review_bundle_v1,
    validate_stage3_review_bundle_v1,
)


TIMESTAMP = "2026-05-03T14:00:00Z"
PREVIEW_FP = "1111111111111111111111111111111111111111111111111111111111111111"
INPUT_FP = "2222222222222222222222222222222222222222222222222222222222222222"
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
    "full_symbol_table",
    "raw_resolver_payload",
    "raw_model_text",
    "provider_output",
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
            "target_id": change_id,
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


def _ready_bundle(**kwargs) -> dict:
    defaults = {
        "review_id": "review:mission-board:persist",
        "section_id": "mission-board",
        "created_at": TIMESTAMP,
        "source_preview_fingerprint": PREVIEW_FP,
        "source_input_fingerprint": INPUT_FP,
        "planned_changes": [_planned_change()],
        "evidence": [
            {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
            {"source": "resolver", "reference": ".dce/plans/unreal-symbol-resolver.resolution.json#BP_Player"},
        ],
    }
    defaults.update(kwargs)
    return build_stage3_review_bundle_v1(**defaults)


def _blocked_bundle() -> dict:
    return build_stage3_review_bundle_v1(
        review_id="review:mission-board:blocked",
        section_id="mission-board",
        created_at=TIMESTAMP,
        source_preview_fingerprint=PREVIEW_FP,
        source_input_fingerprint=None,
        planned_changes=[
            {
                "change_id": "change.missing-target",
                "operation": "modify",
                "strategy": "Blueprint",
            }
        ],
        operator_questions=["Which Unreal asset should receive the requested gameplay change?"],
        evidence=[{"source": "operator_context", "reference": "operator_context:mission-board-question"}],
    )


def test_persists_valid_review_bundle_to_expected_dce_path():
    workspace = _workspace_dir("stage3_review_persist_valid")
    bundle = _ready_bundle()

    result = persist_stage3_review_bundle_v1(bundle, workspace_path=workspace, section_id="mission-board")
    artifact_path = workspace / ".dce" / "review" / "mission-board.stage3_review.json"

    assert result.artifact_path == ".dce/review/mission-board.stage3_review.json"
    assert artifact_path.exists()
    assert stage3_review_bundle_artifact_path(workspace, "mission-board") == artifact_path.resolve()
    assert json.loads(artifact_path.read_text(encoding="utf-8")) == bundle
    assert validate_stage3_review_bundle_v1(result.review_bundle_artifact) is True


def test_refuses_to_persist_invalid_review_bundle_and_creates_no_dce():
    workspace = _workspace_dir("stage3_review_persist_invalid")
    invalid_bundle = _ready_bundle()
    invalid_bundle["review_status"] = "approved"

    with pytest.raises(ValueError, match="stage3_review_bundle invalid"):
        persist_stage3_review_bundle_v1(invalid_bundle, workspace_path=workspace, section_id="mission-board")

    assert not (workspace / ".dce").exists()


def test_persisted_artifact_validates_against_locked_schema():
    workspace = _workspace_dir("stage3_review_persist_validates")
    bundle = _ready_bundle()

    persist_stage3_review_bundle_v1(bundle, workspace_path=workspace, section_id="mission-board")
    persisted = json.loads((workspace / ".dce" / "review" / "mission-board.stage3_review.json").read_text(encoding="utf-8"))

    assert validate_stage3_review_bundle_v1(persisted) is True


def test_read_model_projection_is_correct_for_ready_bundle():
    workspace = _workspace_dir("stage3_review_read_model_ready")
    bundle = _ready_bundle()
    result = persist_stage3_review_bundle_v1(bundle, workspace_path=workspace, section_id="mission-board")
    expected = {
        "section_id": "mission-board",
        "review_id": "review:mission-board:persist",
        "review_status": "ready_for_operator_review",
        "ready_for_approval": True,
        "blocking_review_issues_count": 0,
        "informational_review_issues_count": 0,
        "proposed_change_count": 1,
        "proposed_change_targets": ["Content/Blueprints/BP_Player.uasset"],
        "proposed_change_operations": ["modify"],
        "output_strategies": ["blueprint"],
        "review_risk_summary": {"low": 0, "medium": 1, "high": 0},
        "operator_question_count": 0,
        "evidence_sources": ["preview", "resolver"],
        "forbidden_runtime_actions": bundle["forbidden_runtime_actions"],
    }

    assert result.read_model == expected
    assert build_stage3_review_bundle_read_model_v1("mission-board", bundle) == expected
    assert load_stage3_review_bundle_read_model_v1(workspace, "mission-board") == expected
    assert set(result.read_model) == READ_MODEL_FIELDS


def test_read_model_projection_is_correct_for_blocked_bundle():
    workspace = _workspace_dir("stage3_review_read_model_blocked")
    bundle = _blocked_bundle()
    result = persist_stage3_review_bundle_v1(bundle, workspace_path=workspace, section_id="mission-board")

    assert result.read_model["review_status"] == "blocked"
    assert result.read_model["ready_for_approval"] is False
    assert result.read_model["blocking_review_issues_count"] == 2
    assert result.read_model["operator_question_count"] == 2
    assert result.read_model["proposed_change_targets"] == ["unknown/target-001"]
    assert result.read_model["proposed_change_operations"] == ["modify"]
    assert result.read_model["output_strategies"] == ["blueprint"]
    assert result.read_model["review_risk_summary"] == {"low": 0, "medium": 1, "high": 0}
    assert result.read_model["evidence_sources"] == ["operator_context"]


def test_read_model_includes_targets_operations_and_output_strategies_for_multiple_changes():
    bundle = _ready_bundle(
        planned_changes=[
            _planned_change(change_id="change.bp", target_path="Content/Blueprints/BP_Player.uasset"),
            _planned_change(
                change_id="change.cpp",
                target_path="Source/FixtureGame/Public/InventoryComponent.h",
                target_kind="CppClass",
                operation="create",
                strategy="C++",
            ),
            _planned_change(
                change_id="change.doc",
                target_path="Docs/InventoryReview.md",
                target_kind="documentation",
                operation="ignore",
                strategy="none",
                risk="low",
            ),
        ]
    )
    read_model = build_stage3_review_bundle_read_model_v1("mission-board", bundle)

    assert read_model["proposed_change_count"] == 3
    assert read_model["proposed_change_targets"] == [
        "Content/Blueprints/BP_Player.uasset",
        "Docs/InventoryReview.md",
        "Source/FixtureGame/Public/InventoryComponent.h",
    ]
    assert read_model["proposed_change_operations"] == ["create", "ignore", "modify"]
    assert read_model["output_strategies"] == ["blueprint", "cpp", "none"]
    assert read_model["review_risk_summary"] == {"low": 1, "medium": 2, "high": 0}


def test_read_model_excludes_fingerprints_full_nested_objects_and_raw_blobs():
    bundle = _ready_bundle()
    read_model = build_stage3_review_bundle_read_model_v1("mission-board", bundle)
    serialized = json.dumps(read_model, sort_keys=True)

    for forbidden in FORBIDDEN_READ_MODEL_FIELDS:
        assert forbidden not in read_model
    for forbidden in FORBIDDEN_READ_MODEL_FIELDS - {"evidence"}:
        assert forbidden not in serialized
    assert PREVIEW_FP not in serialized
    assert INPUT_FP not in serialized
    assert "human_readable_summary" not in serialized
    assert ".dce/plans/game-adapter-stage2.preview.json" not in serialized


def test_persistence_creates_no_approval_stage8_execution_or_lifecycle_artifacts():
    workspace = _workspace_dir("stage3_review_no_lifecycle_side_effects")

    persist_stage3_review_bundle_v1(_ready_bundle(), workspace_path=workspace, section_id="mission-board")

    assert (workspace / ".dce" / "review" / "mission-board.stage3_review.json").exists()
    assert not (workspace / ".dce" / "approvals").exists()
    assert not (workspace / ".dce" / "execution").exists()
    assert not (workspace / ".dce" / "outputs").exists()
    assert not (workspace / ".dce" / "output").exists()
    assert not (workspace / ".dce" / "lifecycle_trace.json").exists()


def test_invalid_loaded_artifact_does_not_return_read_model():
    workspace = _workspace_dir("stage3_review_invalid_loaded_artifact")
    bundle = _ready_bundle()
    persist_stage3_review_bundle_v1(bundle, workspace_path=workspace, section_id="mission-board")
    artifact_path = workspace / ".dce" / "review" / "mission-board.stage3_review.json"
    invalid = copy.deepcopy(bundle)
    invalid["evidence"][0]["raw_preview"] = {"full": "payload"}
    artifact_path.write_text(json.dumps(invalid, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="stage3_review_bundle invalid"):
        load_stage3_review_bundle_read_model_v1(workspace, "mission-board")


def test_missing_stage3_review_bundle_read_model_fails_without_creating_artifacts():
    workspace = _workspace_dir("stage3_review_missing_read_model")
    workspace.mkdir(parents=True)

    with pytest.raises(ValueError, match="stage3 review bundle artifact missing"):
        load_stage3_review_bundle_read_model_v1(workspace, "mission-board")

    assert not (workspace / ".dce").exists()
