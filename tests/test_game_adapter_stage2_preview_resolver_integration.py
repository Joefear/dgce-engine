import copy
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import (
    build_game_adapter_stage2_preview,
    render_game_adapter_stage2_human_view,
    render_game_adapter_stage2_machine_view,
    validate_game_adapter_stage2_preview_contract,
)
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import build_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import build_unreal_symbol_candidate_index
from aether.dgce.game_adapter_unreal_symbol_resolver import resolve_unreal_symbols_from_path_metadata


FIXTURE_PROJECT = Path("tests/fixtures/unreal_project_structure/FixtureGame")
STAGE2_FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage2_preview")


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


def _stage2_fixture(name: str) -> dict:
    return json.loads((STAGE2_FIXTURE_DIR / name).read_text(encoding="utf-8"))


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


def _manifest_and_candidate_index() -> tuple[dict, dict]:
    manifest = build_unreal_project_structure_manifest(FIXTURE_PROJECT)
    return manifest, build_unreal_symbol_candidate_index(manifest)


def _resolver_input(manifest: dict, candidate_index: dict) -> dict:
    return {
        "artifact_type": "game_adapter_unreal_symbol_resolver_input",
        "contract_name": "DGCEGameAdapterUnrealSymbolResolver",
        "contract_version": "dgce.game_adapter.unreal_symbol_resolver.v1",
        "adapter": "game",
        "domain": "game_adapter",
        "source_manifest_fingerprint": manifest["artifact_fingerprint"],
        "source_candidate_index_fingerprint": candidate_index["artifact_fingerprint"],
        "requested_symbols": ["BP_Player", "InventoryComponent", "MissingInventoryEvent"],
        "allowed_symbol_kinds": ["BlueprintClass", "CppClass", "Event"],
        "stage_usage": "both",
    }


def _preview_without_resolver() -> dict:
    return build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
    )


def test_preview_without_resolver_remains_baseline_unchanged():
    baseline = _preview_without_resolver()
    with_missing_resolver_inputs = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
        resolver_input={"not": "used"},
    )

    assert baseline == with_missing_resolver_inputs
    assert validate_game_adapter_stage2_preview_contract(baseline) is True
    assert "resolver_context" not in baseline
    assert baseline["human_view"] == render_game_adapter_stage2_human_view(
        baseline["planned_changes"],
        baseline["governance_context"],
    )


def test_preview_with_resolver_enriches_output_deterministically():
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)

    first = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
        resolver_input=resolver_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )
    second = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=copy.deepcopy(_planned_changes()),
        resolver_input=copy.deepcopy(resolver_input),
        resolver_manifest_payload=copy.deepcopy(manifest),
        resolver_candidate_index_payload=copy.deepcopy(candidate_index),
    )

    assert first == second
    assert validate_game_adapter_stage2_preview_contract(first) is True
    assert first["resolver_context"]["resolved_symbols"] == [
        {
            "symbol_name": "BP_Player",
            "symbol_kind": "BlueprintClass",
            "source_path": "Content/Blueprints/BP_Player.uasset",
            "resolution_method": "path_metadata",
            "confidence": "candidate_match",
        },
        {
            "symbol_name": "InventoryComponent",
            "symbol_kind": "CppClass",
            "source_path": "Source/FixtureGame/Public/InventoryComponent.h",
            "resolution_method": "path_metadata",
            "confidence": "candidate_match",
        },
    ]
    assert first["resolver_context"]["unresolved_symbols"] == [
        {
            "symbol_name": "MissingInventoryEvent",
            "symbol_kind": "BlueprintClass",
            "source_path": None,
            "resolution_method": "path_metadata",
            "confidence": "unresolved",
        }
    ]
    assert first["machine_view"] == render_game_adapter_stage2_machine_view(
        first["planned_changes"],
        first["governance_context"],
    )
    assert first["human_view"] == render_game_adapter_stage2_human_view(
        first["planned_changes"],
        first["governance_context"],
    )


def test_preview_resolver_context_matches_standalone_resolver():
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)
    standalone = resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)

    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
        resolver_input=resolver_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )

    assert preview["resolver_context"] == {
        "resolved_symbols": standalone["resolved_symbols"],
        "unresolved_symbols": standalone["unresolved_symbols"],
    }


def test_invalid_resolver_input_fails_closed_without_breaking_preview():
    manifest, candidate_index = _manifest_and_candidate_index()
    invalid_input = _resolver_input(manifest, candidate_index)
    invalid_input["raw_model_text"] = "raw model text must not enter preview"

    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
        resolver_input=invalid_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )

    assert validate_game_adapter_stage2_preview_contract(preview) is True
    assert "resolver_context" not in preview
    assert preview == _preview_without_resolver()


def test_stage2_dispatch_can_opt_in_to_resolver_without_changing_default_dispatch():
    workspace = _workspace_dir("game_adapter_stage2_resolver_dispatch")
    stage0 = persist_stage0_input(workspace, _stage2_fixture("valid_released_gce_source_input.json"))
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)

    baseline = build_game_adapter_stage2_preview_from_released_stage0(
        workspace / stage0.artifact_path,
        workspace_path=workspace,
    )
    enriched = build_game_adapter_stage2_preview_from_released_stage0(
        workspace / stage0.artifact_path,
        workspace_path=workspace,
        preview_id="game-adapter-stage2-resolver",
        resolver_input=resolver_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )

    assert "resolver_context" not in baseline.preview_artifact
    assert enriched.preview_artifact["resolver_context"]["resolved_symbols"]
    assert validate_game_adapter_stage2_preview_contract(enriched.preview_artifact) is True
    assert not (workspace / ".dce" / "execution").exists()
    assert not (workspace / ".dce" / "output").exists()
    assert not (workspace / ".dce" / "outputs").exists()


def test_preview_resolver_context_exposes_no_raw_content_or_model_text():
    manifest, candidate_index = _manifest_and_candidate_index()
    preview = build_game_adapter_stage2_preview(
        source_input_reference="gce-session-1",
        planned_changes=_planned_changes(),
        resolver_input=_resolver_input(manifest, candidate_index),
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )
    serialized = json.dumps(preview, sort_keys=True)

    for forbidden in (
        "fixture header content",
        "fixture source content",
        "fixture blueprint asset content",
        "raw_model_text",
        "provider_output",
        "graph_nodes",
    ):
        assert forbidden not in serialized


def test_stage75_lifecycle_order_remains_unchanged_with_stage2_resolver_hook():
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
