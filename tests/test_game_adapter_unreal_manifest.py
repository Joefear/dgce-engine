import json
from pathlib import Path

import pytest

from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import build_game_adapter_stage2_preview
from aether.dgce.game_adapter_unreal_manifest import (
    ARTIFACT_TYPE,
    CONTRACT_NAME,
    CONTRACT_VERSION,
    build_unreal_project_structure_manifest,
    validate_unreal_project_structure_manifest,
)


FIXTURE_PROJECT = Path("tests/fixtures/unreal_project_structure/FixtureGame")


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
            "summary": {
                "intent": "add_gameplay_capability",
                "impact": "gameplay",
                "risk": "medium",
                "review_focus": "component_setup",
            },
        }
    ]


def test_valid_fixture_project_produces_deterministic_manifest():
    first = build_unreal_project_structure_manifest(FIXTURE_PROJECT)
    second = build_unreal_project_structure_manifest(FIXTURE_PROJECT)

    assert first == second
    assert validate_unreal_project_structure_manifest(first) is True
    assert first["artifact_type"] == ARTIFACT_TYPE
    assert first["contract_name"] == CONTRACT_NAME
    assert first["contract_version"] == CONTRACT_VERSION
    assert first["adapter"] == "game"
    assert first["domain"] == "game_adapter"
    assert first["project_root_reference"] == FIXTURE_PROJECT.as_posix()
    assert first["artifact_fingerprint"] == compute_json_payload_fingerprint(first)
    assert first["discovered_paths"] == {
        "uproject_files": ["FixtureGame.uproject"],
        "source_module_directories": ["Source/FixtureGame"],
        "cpp_headers": ["Source/FixtureGame/Public/InventoryComponent.h"],
        "cpp_sources": ["Source/FixtureGame/Private/InventoryComponent.cpp"],
        "blueprint_assets": ["Content/Blueprints/BP_Player.uasset"],
        "config_directories": ["Config"],
    }
    assert first["structural_summary"] == {
        "uproject_file_count": 1,
        "source_module_count": 1,
        "cpp_header_count": 1,
        "cpp_source_count": 1,
        "blueprint_asset_count": 1,
        "config_present": True,
        "total_discovered_path_count": 6,
    }


def test_manifest_includes_no_file_contents():
    manifest = build_unreal_project_structure_manifest(FIXTURE_PROJECT)
    serialized = json.dumps(manifest, sort_keys=True)

    assert "fixture header content" not in serialized
    assert "fixture source content" not in serialized
    assert "fixture blueprint asset content" not in serialized
    assert "fixture config content" not in serialized


def test_manifest_paths_are_sorted_and_bounded():
    project_root = _workspace_dir("unreal_manifest_sorted")
    (project_root / "Source" / "Beta" / "Public").mkdir(parents=True)
    (project_root / "Source" / "Alpha" / "Private").mkdir(parents=True)
    (project_root / "Content" / "Blueprints").mkdir(parents=True)
    (project_root / "Alpha.uproject").write_text("{}", encoding="utf-8")
    (project_root / "Zeta.uproject").write_text("{}", encoding="utf-8")
    (project_root / "Source" / "Beta" / "Public" / "Zeta.h").write_text("", encoding="utf-8")
    (project_root / "Source" / "Alpha" / "Private" / "Alpha.cpp").write_text("", encoding="utf-8")
    (project_root / "Content" / "Blueprints" / "Z_BP.uasset").write_text("", encoding="utf-8")
    (project_root / "Content" / "Blueprints" / "A_BP.uasset").write_text("", encoding="utf-8")

    manifest = build_unreal_project_structure_manifest(project_root)

    for paths in manifest["discovered_paths"].values():
        assert paths == sorted(paths)
    with pytest.raises(ValueError, match="max_discovered_paths"):
        build_unreal_project_structure_manifest(project_root, max_discovered_paths=2)


def test_missing_root_fails_closed():
    with pytest.raises(FileNotFoundError, match="Project root does not exist"):
        build_unreal_project_structure_manifest("tests/.tmp/missing-unreal-project")


def test_path_traversal_fails_closed():
    with pytest.raises(ValueError, match="traversal|current working directory"):
        build_unreal_project_structure_manifest("../outside")


def test_unsupported_project_shape_fails_closed():
    project_root = _workspace_dir("unreal_manifest_unsupported")
    project_root.mkdir(parents=True)

    with pytest.raises(ValueError, match="unsupported Unreal project structure"):
        build_unreal_project_structure_manifest(project_root)


def test_manifest_does_not_alter_game_adapter_preview_output_unless_called():
    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
    )

    assert "structural_summary" not in preview
    assert "discovered_paths" not in preview
    assert "project_root_reference" not in preview


def test_manifest_builder_creates_no_execution_or_output_artifacts():
    manifest = build_unreal_project_structure_manifest(FIXTURE_PROJECT)

    assert manifest["artifact_type"] == ARTIFACT_TYPE
    assert not (FIXTURE_PROJECT / ".dce" / "execution").exists()
    assert not (FIXTURE_PROJECT / ".dce" / "output").exists()
    assert not (FIXTURE_PROJECT / ".dce" / "outputs").exists()


def test_manifest_builder_keeps_stage75_lifecycle_order_locked():
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
