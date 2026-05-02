import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import (
    render_game_adapter_stage2_human_view,
    validate_game_adapter_stage2_preview_contract,
)
from aether.dgce.game_adapter_stage2_dispatch import (
    GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH,
    build_game_adapter_stage2_preview_from_released_stage0,
)
from aether.dgce.read_api import get_game_adapter_stage2_preview_artifact


FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage2_preview")


def _fixture(name: str):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


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


def test_fixture_driven_valid_preview_builds_and_validates():
    project_root = _workspace_dir("game_adapter_stage2_fixture_valid")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["planned_changes"] == _fixture("expected_planned_changes.json")
    assert result.preview_artifact["source_stage0_fingerprint"] == persisted.artifact["artifact_fingerprint"]


def test_fixture_human_view_is_deterministically_derived_from_planned_changes():
    project_root = _workspace_dir("game_adapter_stage2_fixture_human_view")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    preview = build_game_adapter_stage2_preview_from_released_stage0(persisted.artifact).preview_artifact

    assert preview["human_view"] == _fixture("expected_human_view.json")
    assert preview["human_view"] == render_game_adapter_stage2_human_view(
        preview["planned_changes"],
        preview["governance_context"],
    )


def test_fixture_persisted_preview_read_path_returns_bounded_read_model():
    project_root = _workspace_dir("game_adapter_stage2_fixture_read_model")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))
    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )
    preview_path = project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH

    detail = get_game_adapter_stage2_preview_artifact(project_root, preview_path.name)

    assert detail["read_model_type"] == "game_adapter_stage2_preview_read_model"
    assert detail["artifact_type"] == "game_adapter_stage2_preview"
    assert detail["artifact_fingerprint"] == result.preview_artifact["artifact_fingerprint"]
    assert detail["planned_changes_summary"] == {
        "change_count": 2,
        "operations": {"create": 1, "modify": 1},
        "domain_types": {"binding": 1, "component": 1},
        "strategies": {"Blueprint": 2},
    }
    assert detail["governance_context_summary"] == {
        "guardrail_required": True,
        "policy_pack": "game_adapter_stage2_preview",
    }
    assert "raw_model_text" not in json.dumps(detail, sort_keys=True)
    assert "raw_provider_text" not in json.dumps(detail, sort_keys=True)


def test_invalid_raw_freeform_preview_fixture_fails_closed():
    invalid_preview = _fixture("invalid_raw_freeform_preview.json")

    with pytest.raises(ValueError):
        validate_game_adapter_stage2_preview_contract(invalid_preview)


def test_fixture_preview_read_does_not_create_execution_or_output_artifacts():
    project_root = _workspace_dir("game_adapter_stage2_fixture_no_execution")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))
    build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    get_game_adapter_stage2_preview_artifact(project_root, GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH.name)

    assert not (project_root / ".dce" / "execution").exists()
    assert not (project_root / ".dce" / "output").exists()
    assert not (project_root / ".dce" / "outputs").exists()


def test_fixture_smoke_keeps_stage75_lifecycle_order_locked():
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
