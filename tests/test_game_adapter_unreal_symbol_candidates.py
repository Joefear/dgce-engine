import copy
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import build_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import (
    ARTIFACT_TYPE,
    CONTRACT_NAME,
    CONTRACT_VERSION,
    RESOLUTION_STATUS,
    UNREAL_SYMBOL_CANDIDATE_INDEX_RELATIVE_PATH,
    build_unreal_symbol_candidate_index,
    persist_unreal_symbol_candidate_index,
    validate_unreal_symbol_candidate_index,
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


def _manifest() -> dict:
    return build_unreal_project_structure_manifest(FIXTURE_PROJECT)


def _stage0_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": {
            "project_id": "symbol-candidates",
            "project_name": "Symbol Candidates",
            "owner": "Design Authority",
            "source_id": "symbol-candidates",
            "created_at": "2026-04-27T00:00:00Z",
            "updated_at": "2026-04-27T00:00:00Z",
        },
        "intent": {
            "session_objective": "Generate bounded Game Adapter preview.",
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
                            "planned_changes": [
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
                            ],
                        },
                    },
                }
            ],
        },
        "ambiguities": [],
    }


def test_valid_manifest_produces_deterministic_candidate_index():
    manifest = _manifest()

    first = build_unreal_symbol_candidate_index(manifest)
    second = build_unreal_symbol_candidate_index(copy.deepcopy(manifest))

    assert first == second
    assert validate_unreal_symbol_candidate_index(first) is True
    assert first["artifact_type"] == ARTIFACT_TYPE
    assert first["contract_name"] == CONTRACT_NAME
    assert first["contract_version"] == CONTRACT_VERSION
    assert first["adapter"] == "game"
    assert first["domain"] == "game_adapter"
    assert first["source_manifest_fingerprint"] == manifest["artifact_fingerprint"]
    assert first["structural_summary"] == manifest["structural_summary"]
    assert first["artifact_fingerprint"] == compute_json_payload_fingerprint(first)
    assert first["candidates"] == [
        {
            "candidate_name": "BP_Player",
            "candidate_kind": "blueprint_asset",
            "source_path": "Content/Blueprints/BP_Player.uasset",
            "resolution_status": RESOLUTION_STATUS,
        },
        {
            "candidate_name": "Config",
            "candidate_kind": "config_directory",
            "source_path": "Config",
            "resolution_status": RESOLUTION_STATUS,
        },
        {
            "candidate_name": "InventoryComponent",
            "candidate_kind": "cpp_header",
            "source_path": "Source/FixtureGame/Public/InventoryComponent.h",
            "resolution_status": RESOLUTION_STATUS,
        },
        {
            "candidate_name": "InventoryComponent",
            "candidate_kind": "cpp_source",
            "source_path": "Source/FixtureGame/Private/InventoryComponent.cpp",
            "resolution_status": RESOLUTION_STATUS,
        },
        {
            "candidate_name": "FixtureGame",
            "candidate_kind": "source_module",
            "source_path": "Source/FixtureGame",
            "resolution_status": RESOLUTION_STATUS,
        },
        {
            "candidate_name": "FixtureGame",
            "candidate_kind": "uproject",
            "source_path": "FixtureGame.uproject",
            "resolution_status": RESOLUTION_STATUS,
        },
    ]


def test_candidates_are_filename_and_path_derived_only():
    index = build_unreal_symbol_candidate_index(_manifest())

    for candidate in index["candidates"]:
        source_path = Path(candidate["source_path"])
        expected_name = source_path.stem if source_path.suffix else source_path.name
        assert candidate["candidate_name"] == expected_name
        assert candidate["resolution_status"] == "path_candidate"
        assert set(candidate) == {
            "candidate_name",
            "candidate_kind",
            "source_path",
            "resolution_status",
        }


def test_candidate_index_exposes_no_raw_file_contents():
    serialized = json.dumps(build_unreal_symbol_candidate_index(_manifest()), sort_keys=True)

    assert "fixture header content" not in serialized
    assert "fixture source content" not in serialized
    assert "fixture blueprint asset content" not in serialized
    assert "fixture config content" not in serialized


def test_malformed_manifest_fails_closed():
    manifest = _manifest()
    del manifest["artifact_fingerprint"]

    with pytest.raises(ValueError, match="manifest|fields|artifact_fingerprint"):
        build_unreal_symbol_candidate_index(manifest)


def test_unsupported_path_category_fails_closed():
    manifest = _manifest()
    manifest["discovered_paths"]["plugin_descriptors"] = ["Plugins/Test/Test.uplugin"]
    manifest["structural_summary"]["total_discovered_path_count"] += 1
    manifest["artifact_fingerprint"] = compute_json_payload_fingerprint(manifest)

    with pytest.raises(ValueError, match="discovered_paths|unsupported"):
        build_unreal_symbol_candidate_index(manifest)


def test_candidate_ordering_is_deterministic():
    manifest = _manifest()
    manifest["discovered_paths"]["cpp_headers"] = list(reversed(manifest["discovered_paths"]["cpp_headers"]))
    manifest["artifact_fingerprint"] = compute_json_payload_fingerprint(manifest)

    first = build_unreal_symbol_candidate_index(manifest)
    second = build_unreal_symbol_candidate_index(copy.deepcopy(manifest))

    assert first["candidates"] == second["candidates"]
    assert first["candidates"] == sorted(
        first["candidates"],
        key=lambda candidate: (candidate["candidate_kind"], candidate["source_path"]),
    )


def test_excessive_candidates_fail_closed():
    manifest = _manifest()

    with pytest.raises(ValueError, match="max_candidates"):
        build_unreal_symbol_candidate_index(manifest, max_candidates=2)


def test_candidate_index_persistence_uses_preview_safe_path_only():
    workspace_path = _workspace_dir("unreal_symbol_candidates_persist")
    persisted = persist_unreal_symbol_candidate_index(_manifest(), workspace_path=workspace_path)

    assert persisted.artifact_path == UNREAL_SYMBOL_CANDIDATE_INDEX_RELATIVE_PATH.as_posix()
    assert (workspace_path / persisted.artifact_path).exists()
    assert persisted.candidate_index_artifact["artifact_type"] == ARTIFACT_TYPE
    assert not (workspace_path / ".dce" / "execution").exists()
    assert not (workspace_path / ".dce" / "output").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()


def test_preview_dispatch_remains_unchanged_unless_candidate_index_is_explicitly_called():
    workspace_path = _workspace_dir("unreal_symbol_candidates_preview_unchanged")
    persisted_stage0 = persist_stage0_input(workspace_path, _stage0_input())

    result = build_game_adapter_stage2_preview_from_released_stage0(
        workspace_path / persisted_stage0.artifact_path,
        workspace_path=workspace_path,
    )

    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert not (workspace_path / UNREAL_SYMBOL_CANDIDATE_INDEX_RELATIVE_PATH).exists()
    assert "symbol_candidate" not in json.dumps(result.preview_artifact, sort_keys=True)


def test_candidate_index_keeps_stage75_lifecycle_order_locked():
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
