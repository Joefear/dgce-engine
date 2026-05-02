import copy
import json
from pathlib import Path

import aether.dgce.decompose as dgce_decompose
from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
from aether.dgce.game_adapter_preview import (
    build_game_adapter_stage2_preview,
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


def _fixture(name: str):
    return json.loads((STAGE2_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _planned_changes() -> list[dict]:
    return copy.deepcopy(_fixture("expected_planned_changes.json"))


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


def _baseline_preview() -> dict:
    return build_game_adapter_stage2_preview(
        source_input_reference="structured_intent",
        planned_changes=_planned_changes(),
        policy_pack="game_adapter_stage2_preview",
        guardrail_required=True,
    )


def _enriched_preview() -> tuple[dict, dict]:
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)
    preview = build_game_adapter_stage2_preview(
        source_input_reference="structured_intent",
        planned_changes=_planned_changes(),
        policy_pack="game_adapter_stage2_preview",
        guardrail_required=True,
        resolver_input=resolver_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )
    standalone = resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)
    return preview, standalone


def test_contract_lock_baseline_stage2_preview_without_resolver_matches_pre_resolver_fixture_shape():
    preview = _baseline_preview()

    assert validate_game_adapter_stage2_preview_contract(preview) is True
    assert "resolver_context" not in preview
    assert preview["planned_changes"] == _fixture("expected_planned_changes.json")
    assert preview["human_view"] == _fixture("expected_human_view.json")
    assert preview["machine_view"] == render_game_adapter_stage2_machine_view(
        _fixture("expected_planned_changes.json"),
        preview["governance_context"],
    )
    assert preview == build_game_adapter_stage2_preview(
        source_input_reference="structured_intent",
        planned_changes=_planned_changes(),
        policy_pack="game_adapter_stage2_preview",
        guardrail_required=True,
    )


def test_contract_lock_resolver_enrichment_is_opt_in_only_and_missing_inputs_preserve_baseline():
    baseline = _baseline_preview()
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)
    missing_one_or_more = [
        {"resolver_input": resolver_input},
        {"resolver_manifest_payload": manifest},
        {"resolver_candidate_index_payload": candidate_index},
        {"resolver_input": resolver_input, "resolver_manifest_payload": manifest},
        {"resolver_input": resolver_input, "resolver_candidate_index_payload": candidate_index},
        {"resolver_manifest_payload": manifest, "resolver_candidate_index_payload": candidate_index},
    ]

    for kwargs in missing_one_or_more:
        assert build_game_adapter_stage2_preview(
            source_input_reference="structured_intent",
            planned_changes=_planned_changes(),
            policy_pack="game_adapter_stage2_preview",
            guardrail_required=True,
            **kwargs,
        ) == baseline

    enriched, _standalone = _enriched_preview()
    assert enriched != baseline
    assert "resolver_context" in enriched


def test_contract_lock_valid_resolver_inputs_attach_only_bounded_resolver_context():
    preview, standalone = _enriched_preview()

    assert validate_game_adapter_stage2_preview_contract(preview) is True
    assert set(preview["resolver_context"]) == {"resolved_symbols", "unresolved_symbols"}
    assert preview["resolver_context"] == {
        "resolved_symbols": standalone["resolved_symbols"],
        "unresolved_symbols": standalone["unresolved_symbols"],
    }
    assert set(preview["resolver_context"]["resolved_symbols"][0]) == {
        "symbol_name",
        "symbol_kind",
        "source_path",
        "resolution_method",
        "confidence",
    }
    assert set(preview["resolver_context"]["unresolved_symbols"][0]) == {
        "symbol_name",
        "symbol_kind",
        "source_path",
        "resolution_method",
        "confidence",
    }


def test_contract_lock_resolver_context_contains_no_raw_content_graph_data_or_write_directives():
    preview, _standalone = _enriched_preview()
    serialized = json.dumps(preview["resolver_context"], sort_keys=True)

    for forbidden in (
        "fixture header content",
        "fixture source content",
        "fixture blueprint asset content",
        "raw_file_contents",
        "raw_model_text",
        "raw_provider_text",
        "provider_output",
        "graph",
        "graph_nodes",
        "node_data",
        "write_directive",
        "write_targets",
        "written_files",
    ):
        assert forbidden not in serialized


def test_contract_lock_invalid_resolver_inputs_and_artifacts_fail_closed_to_baseline_preview():
    baseline = _baseline_preview()
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)
    invalid_input = copy.deepcopy(resolver_input)
    invalid_input["raw_model_text"] = "raw provider text must not be exposed"
    invalid_manifest = copy.deepcopy(manifest)
    invalid_manifest["artifact_fingerprint"] = "wrongmanifestfingerprint"
    invalid_candidate_index = copy.deepcopy(candidate_index)
    invalid_candidate_index["candidates"][0]["candidate_kind"] = "blueprint_graph"
    invalid_candidate_index["artifact_fingerprint"] = compute_json_payload_fingerprint(invalid_candidate_index)

    cases = [
        (invalid_input, manifest, candidate_index),
        (resolver_input, invalid_manifest, candidate_index),
        (resolver_input, manifest, invalid_candidate_index),
    ]
    for resolver_payload, manifest_payload, candidate_payload in cases:
        assert build_game_adapter_stage2_preview(
            source_input_reference="structured_intent",
            planned_changes=_planned_changes(),
            policy_pack="game_adapter_stage2_preview",
            guardrail_required=True,
            resolver_input=resolver_payload,
            resolver_manifest_payload=manifest_payload,
            resolver_candidate_index_payload=candidate_payload,
        ) == baseline


def test_contract_lock_stage2_dispatch_persists_enriched_preview_without_execution_output_or_alignment():
    workspace = _workspace_dir("game_adapter_contract_lock_resolver_enriched_dispatch")
    stage0 = persist_stage0_input(workspace, _fixture("valid_released_gce_source_input.json"))
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _resolver_input(manifest, candidate_index)

    result = build_game_adapter_stage2_preview_from_released_stage0(
        workspace / stage0.artifact_path,
        workspace_path=workspace,
        preview_id="game-adapter-stage2-resolver-contract-lock",
        resolver_input=resolver_input,
        resolver_manifest_payload=manifest,
        resolver_candidate_index_payload=candidate_index,
    )

    persisted_path = workspace / str(result.artifact_path)
    assert result.artifact_path == ".dce/plans/game-adapter-stage2-resolver-contract-lock.preview.json"
    assert persisted_path.exists()
    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["resolver_context"]["resolved_symbols"]
    assert not (workspace / ".dce" / "execution" / "alignment").exists()
    assert not (workspace / ".dce" / "execution").exists()
    assert not (workspace / ".dce" / "output").exists()
    assert not (workspace / ".dce" / "outputs").exists()


def test_contract_lock_stage75_lifecycle_order_remains_unchanged_after_resolver_enrichment():
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
