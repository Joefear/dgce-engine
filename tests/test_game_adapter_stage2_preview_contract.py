import copy
import json
from pathlib import Path

import pytest

from aether.dgce.decompose import compute_json_payload_fingerprint
from aether.dgce.file_plan import FilePlan
from aether.dgce.game_adapter_preview import (
    ARTIFACT_TYPE,
    CONTRACT_NAME,
    CONTRACT_VERSION,
    build_game_adapter_stage2_preview,
    render_game_adapter_stage2_human_view,
    validate_game_adapter_stage2_preview_contract,
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


def _planned_changes() -> list[dict]:
    return [
        {
            "change_id": "change.input-binding",
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
    ]


def _preview() -> dict:
    return build_game_adapter_stage2_preview(
        source_stage0_fingerprint="stage0abcdef123456",
        planned_changes=_planned_changes(),
        policy_pack="game_adapter_stage2_preview",
        guardrail_required=True,
    )


def test_game_adapter_stage2_preview_contract_schema_file_defines_locked_contract():
    schema_path = Path("contracts/game_adapter/game-adapter-stage2-preview-v1.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert schema["title"] == CONTRACT_NAME
    assert schema["properties"]["artifact_type"]["const"] == ARTIFACT_TYPE
    assert schema["properties"]["contract_version"]["const"] == CONTRACT_VERSION
    assert schema["properties"]["adapter"]["const"] == "game"
    assert schema["properties"]["domain"]["const"] == "game_adapter"
    assert schema["additionalProperties"] is False
    assert "PlannedChange" in schema["$defs"]


def test_valid_game_adapter_stage2_preview_contract_validates():
    preview = _preview()

    assert validate_game_adapter_stage2_preview_contract(preview) is True
    assert preview["artifact_type"] == ARTIFACT_TYPE
    assert preview["contract_name"] == CONTRACT_NAME
    assert preview["contract_version"] == CONTRACT_VERSION
    assert preview["adapter"] == "game"
    assert preview["domain"] == "game_adapter"
    assert preview["governance_context"] == {
        "policy_pack": "game_adapter_stage2_preview",
        "guardrail_required": True,
    }
    assert preview["artifact_fingerprint"] == compute_json_payload_fingerprint(preview)


def test_human_view_is_deterministically_derived_from_planned_changes():
    preview = _preview()
    expected_human_view = render_game_adapter_stage2_human_view(
        preview["planned_changes"],
        preview["governance_context"],
    )

    assert preview["human_view"] == expected_human_view
    tampered = copy.deepcopy(preview)
    tampered["human_view"]["rows"][0]["risk"] = "raw provider says probably fine"
    tampered["artifact_fingerprint"] = compute_json_payload_fingerprint(tampered)
    with pytest.raises(ValueError, match="human_view must be deterministically derived"):
        validate_game_adapter_stage2_preview_contract(tampered)


def test_same_game_adapter_stage2_input_produces_identical_output():
    first = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=list(reversed(_planned_changes())),
    )
    second = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=list(reversed(_planned_changes())),
    )

    assert first == second
    assert [change["change_id"] for change in first["planned_changes"]] == [
        "change.input-binding",
        "change.player-component",
    ]


def test_game_adapter_stage2_invalid_operation_fails_closed():
    changes = _planned_changes()
    changes[0]["operation"] = "rename"

    with pytest.raises(ValueError, match="operation"):
        build_game_adapter_stage2_preview(source_input_reference="gce-session-1", planned_changes=changes)


def test_game_adapter_stage2_invalid_strategy_fails_closed():
    changes = _planned_changes()
    changes[0]["strategy"] = "Python"

    with pytest.raises(ValueError, match="strategy"):
        build_game_adapter_stage2_preview(source_input_reference="gce-session-1", planned_changes=changes)


def test_game_adapter_stage2_human_view_cannot_contain_freeform_or_raw_text():
    preview = _preview()
    allowed_row_keys = {
        "change_id",
        "target",
        "operation",
        "domain_type",
        "strategy",
        "intent",
        "impact",
        "risk",
        "review_focus",
        "guardrail",
    }

    assert set(preview["human_view"]["rows"][0]) == allowed_row_keys
    tampered = copy.deepcopy(preview)
    tampered["human_view"]["rows"][0]["freeform"] = "Create a Blueprint graph however the model thinks best."
    tampered["artifact_fingerprint"] = compute_json_payload_fingerprint(tampered)
    with pytest.raises(ValueError, match="freeform|human_view"):
        validate_game_adapter_stage2_preview_contract(tampered)


def test_game_adapter_stage2_raw_model_or_provider_text_is_rejected():
    changes = _planned_changes()
    changes[0]["raw_model_text"] = "provider raw text must not be persisted"

    with pytest.raises(ValueError, match="unsupported fields"):
        build_game_adapter_stage2_preview(source_input_reference="gce-session-1", planned_changes=changes)

    preview = _preview()
    tampered = copy.deepcopy(preview)
    tampered["provider_output"] = "provider raw text must not be persisted"
    tampered["artifact_fingerprint"] = compute_json_payload_fingerprint(tampered)
    with pytest.raises(ValueError, match="provider_output"):
        validate_game_adapter_stage2_preview_contract(tampered)


def test_game_adapter_stage2_preview_contract_does_not_trigger_stage8_execution():
    project_root = _workspace_dir("game_adapter_stage2_preview_no_execution")

    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
    )

    assert preview["machine_view"]["change_count"] == 2
    assert not (project_root / ".dce" / "execution").exists()
    assert "execution_status" not in preview
    assert "written_files" not in preview


def test_game_adapter_stage2_preview_contract_does_not_touch_gce_stage0_artifacts():
    project_root = _workspace_dir("game_adapter_stage2_preview_stage0_untouched")

    preview = build_game_adapter_stage2_preview(
        source_stage0_fingerprint="stage0abcdef123456",
        planned_changes=_planned_changes(),
    )

    assert preview["source_stage0_fingerprint"] == "stage0abcdef123456"
    assert not (project_root / ".dce" / "input" / "gce").exists()
    assert "stage_1_release_blocked" not in preview
    assert "stage0_release_result" not in preview


def test_existing_phase4_software_preview_behavior_remains_unchanged():
    project_root = _workspace_dir("game_adapter_stage2_preview_software_unchanged")
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

    assert preview == {
        "section_id": "mission-board",
        "mode": "incremental_v2_2",
        "summary": {
            "total_targets": 1,
            "total_create": 1,
            "total_modify": 0,
            "total_ignore": 0,
            "total_write": 1,
            "total_skip": 0,
            "total_eligible": 1,
            "total_blocked": 0,
            "total_identical": 0,
            "total_blocked_ownership": 0,
            "total_blocked_modify_disabled": 0,
            "total_blocked_ignore": 0,
        },
        "preview_outcome_class": "preview_create_only",
        "recommended_mode": "create_only",
        "previews": [
            {
                "path": "api/mission.py",
                "section_id": "mission-board",
                "planned_action": "create",
                "eligibility": "eligible",
                "preview_decision": "write",
                "preview_reason": "create",
                "identical_content": False,
                "existing_bytes": 0,
                "generated_bytes": 216,
                "approximate_line_delta": 11,
            }
        ],
    }
