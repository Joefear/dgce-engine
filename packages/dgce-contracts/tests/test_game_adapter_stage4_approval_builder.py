import json
from pathlib import Path

import pytest

from packages.dgce_contracts.game_adapter_stage4_approval_builder import (
    FORBIDDEN_RUNTIME_ACTIONS,
    build_stage4_approval_v1,
    validate_stage4_approval_v1,
)


FIXTURE_DIR = Path("packages/dgce-contracts/fixtures/game_adapter/stage4_approval")
TIMESTAMP = "2026-05-05T12:00:00Z"
REVIEW_FP = "1111111111111111111111111111111111111111111111111111111111111111"
PREVIEW_FP = "2222222222222222222222222222222222222222222222222222222222222222"
INPUT_FP = "3333333333333333333333333333333333333333333333333333333333333333"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _build(**kwargs) -> dict:
    defaults = {
        "approval_id": "approval:mission-board:test",
        "section_id": "mission-board",
        "created_at": TIMESTAMP,
        "approved_by": "operator",
        "approval_status": "approved",
        "source_review_id": "review:mission-board:stage3",
        "source_review_fingerprint": REVIEW_FP,
        "source_preview_fingerprint": PREVIEW_FP,
        "source_input_fingerprint": INPUT_FP,
        "approved_change_ids": ["change.player-component", "change.interact-binding"],
        "rejected_change_ids": [],
        "operator_summary": "Approved for downstream lifecycle consideration only.",
        "approval_scope_summary": "Operator approves the listed review bundle changes for later lifecycle processing.",
        "risk_acknowledgement": (
            "Approval does not grant execution permission or bypass gate, alignment, "
            "simulation, or execution controls."
        ),
        "approval_constraints": [
            "Approval is limited to the proposed Stage 3 review bundle changes and still requires downstream gates."
        ],
        "evidence": [
            {
                "source": "preview",
                "reference": ".dce/plans/game-adapter-stage2.preview.json",
            },
            {
                "source": "review_bundle",
                "reference": ".dce/review/mission-board.stage3_review.json",
            },
        ],
    }
    defaults.update(kwargs)
    return build_stage4_approval_v1(**defaults)


def test_stage4_approval_builder_approved_minimal_validates():
    approval = _build()

    assert validate_stage4_approval_v1(approval) is True
    assert approval["approval_status"] == "approved"
    assert approval["forbidden_runtime_actions"] == FORBIDDEN_RUNTIME_ACTIONS
    assert "execution_permission" not in approval
    assert "auto_approved" not in approval


def test_stage4_approval_builder_rejected_validates_without_approved_change_ids():
    approval = _build(
        approval_id="approval:mission-board:rejected",
        approval_status="rejected",
        approved_change_ids=[],
        rejected_change_ids=["change.player-component"],
        operator_summary="Rejected for downstream lifecycle consideration.",
        approval_scope_summary="Operator rejects the listed review bundle changes.",
        risk_acknowledgement="Rejected approval does not grant execution permission.",
        approval_constraints=[],
    )

    assert validate_stage4_approval_v1(approval) is True
    assert approval["approval_status"] == "rejected"
    assert approval["approved_change_ids"] == []
    assert approval["rejected_change_ids"] == ["change.player-component"]


def test_stage4_approval_builder_blocked_validates_without_approved_change_ids():
    approval = _build(
        approval_id="approval:mission-board:blocked",
        approval_status="blocked",
        approved_change_ids=[],
        rejected_change_ids=[],
        operator_summary="Blocked because the review fingerprint must be refreshed before approval.",
        approval_scope_summary="Operator blocks approval until stale review evidence is resolved.",
        risk_acknowledgement="Blocked approval does not grant execution permission.",
        approval_constraints=["Refresh the Stage 3 review bundle before approval can be reconsidered."],
    )

    assert validate_stage4_approval_v1(approval) is True
    assert approval["approval_status"] == "blocked"
    assert approval["approved_change_ids"] == []
    assert approval["approval_constraints"] == [
        "Refresh the Stage 3 review bundle before approval can be reconsidered."
    ]


def test_stage4_approval_builder_rejects_approved_change_ids_for_rejected_or_blocked_status():
    for status in ("rejected", "blocked"):
        with pytest.raises(ValueError, match="must not include approved_change_ids"):
            _build(approval_status=status, approved_change_ids=["change.player-component"])


def test_stage4_approval_builder_rejects_forbidden_runtime_policy_gate_alignment_simulation_and_execution_fields():
    forbidden_fields = [
        "execution_permission",
        "execute",
        "write_targets",
        "write_directives",
        "blueprint_mutation",
        "simulation_result",
        "guardrail_policy_decision",
        "stage6_gate_decision",
        "stage7_alignment_result",
        "stage8_execution_stamp",
        "auto_approved",
        "raw_review_bundle",
        "raw_preview",
        "raw_resolver_payload",
        "raw_model_text",
    ]

    for forbidden in forbidden_fields:
        with pytest.raises(ValueError, match="forbidden runtime fields|unsupported fields"):
            _build(
                evidence=[
                    {
                        "source": "review_bundle",
                        "reference": ".dce/review/mission-board.stage3_review.json",
                        forbidden: True,
                    }
                ]
            )


def test_stage4_approval_builder_never_emits_execution_permission_or_auto_approval_fields():
    approval = _build()
    serialized = json.dumps(approval, sort_keys=True)

    assert "execution_permission" not in approval
    assert "execute" not in approval
    assert "auto_approved" not in approval
    assert "write_targets" not in approval
    assert "stage8_execution_stamp" not in approval
    assert '"execution_permission"' not in serialized
    assert '"auto_approved"' not in serialized


def test_stage4_approval_builder_stale_detection_captures_review_preview_and_input_fingerprints():
    approval = _build()

    assert approval["stale_detection"] == {
        "captured_review_fingerprint": REVIEW_FP,
        "captured_preview_fingerprint": PREVIEW_FP,
        "captured_input_fingerprint": INPUT_FP,
        "stale_check_required": True,
    }


def test_stage4_approval_builder_requires_stale_check():
    with pytest.raises(ValueError, match="stale_check_required must be true"):
        _build(stale_check_required=False)


def test_stage4_approval_builder_accepts_nullable_input_fingerprint():
    approval = _build(source_input_fingerprint=None)

    assert validate_stage4_approval_v1(approval) is True
    assert approval["source_input_fingerprint"] is None
    assert approval["stale_detection"]["captured_input_fingerprint"] is None


def test_stage4_approval_builder_evidence_remains_bounded_and_sorted():
    approval = _build(
        evidence=[
            {"source": "operator_context", "reference": "operator_context:approval-note"},
            {
                "source": "review_bundle",
                "reference": ".dce/review/mission-board.stage3_review.json",
                "snippet_hash": "4444444444444444444444444444444444444444444444444444444444444444",
            },
            {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
        ]
    )

    assert validate_stage4_approval_v1(approval) is True
    assert approval["evidence"] == [
        {"source": "operator_context", "reference": "operator_context:approval-note"},
        {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
        {
            "source": "review_bundle",
            "reference": ".dce/review/mission-board.stage3_review.json",
            "snippet_hash": "4444444444444444444444444444444444444444444444444444444444444444",
        },
    ]
    serialized = json.dumps(approval["evidence"], sort_keys=True)
    assert "raw_review_bundle" not in serialized
    assert "raw_preview" not in serialized
    assert "raw_resolver_payload" not in serialized


def test_stage4_approval_builder_rejects_unbounded_or_unsupported_evidence():
    with pytest.raises(ValueError, match="at least one"):
        _build(evidence=[])

    with pytest.raises(ValueError, match="unsupported"):
        _build(evidence=[{"source": "guardrail", "reference": "guardrail:decision"}])

    with pytest.raises(ValueError, match="at most 512"):
        _build(evidence=[{"source": "review_bundle", "reference": "x" * 513}])


def test_stage4_approval_builder_populates_forbidden_runtime_actions():
    approval = _build()

    assert approval["forbidden_runtime_actions"] == [
        "no_auto_approval",
        "no_binary_blueprint_parsing",
        "no_blueprint_mutation",
        "no_execution_performed",
        "no_execution_permission_granted",
        "no_guardrail_policy_decision",
        "no_stage6_gate_decision",
        "no_stage7_alignment_result",
        "no_stage75_simulation_result",
        "no_stage8_execution_stamp",
        "no_unreal_project_writes",
        "no_write_directives",
    ]


def test_stage4_approval_builder_output_is_stable_across_repeated_runs():
    first = _build()
    second = _build()

    assert first == second


def test_stage4_approval_locked_fixtures_still_validate_with_builder_helper():
    for fixture_name in (
        "approved_minimal.json",
        "rejected_minimal.json",
        "blocked_stale_review.json",
    ):
        assert validate_stage4_approval_v1(_fixture(fixture_name)) is True
