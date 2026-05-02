import copy
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import validate_game_adapter_stage2_preview_contract
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import build_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import build_unreal_symbol_candidate_index
from aether.dgce.game_adapter_unreal_symbol_resolver import resolve_unreal_symbols_from_path_metadata
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import (
    CONTRACT_NAME,
    CONTRACT_VERSION,
    INPUT_ARTIFACT_TYPE,
    validate_resolver_output_contract,
)


FIXTURE_PROJECT = Path("tests/fixtures/unreal_project_structure/FixtureGame")
STAGE2_FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage2_preview")


def _fixture(name: str):
    return json.loads((STAGE2_FIXTURE_DIR / name).read_text(encoding="utf-8"))


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


def _candidate_index(manifest: dict) -> dict:
    return build_unreal_symbol_candidate_index(manifest)


def _resolver_input(
    manifest: dict,
    candidate_index: dict,
    *,
    requested_symbols: list[str] | None = None,
    requested_targets: list[dict] | None = None,
    allowed_symbol_kinds: list[str] | None = None,
    stage_usage: str = "both",
) -> dict:
    payload = {
        "artifact_type": INPUT_ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": "game",
        "domain": "game_adapter",
        "source_manifest_fingerprint": manifest["artifact_fingerprint"],
        "source_candidate_index_fingerprint": candidate_index["artifact_fingerprint"],
        "allowed_symbol_kinds": allowed_symbol_kinds or ["BlueprintClass", "CppClass"],
        "stage_usage": stage_usage,
    }
    if requested_targets is not None:
        payload["requested_targets"] = requested_targets
    else:
        payload["requested_symbols"] = requested_symbols or ["BP_Player"]
    return payload


def _resolve(**kwargs) -> dict:
    manifest = _manifest()
    candidate_index = _candidate_index(manifest)
    return resolve_unreal_symbols_from_path_metadata(
        _resolver_input(manifest, candidate_index, **kwargs),
        manifest,
        candidate_index,
    )


def test_exact_target_path_match_resolves_from_path_metadata():
    output = _resolve(
        requested_targets=[
            {
                "target_id": "BP_Player",
                "target_path": "/Game/Blueprints/BP_Player",
                "target_kind": "BlueprintClass",
            }
        ],
        allowed_symbol_kinds=["BlueprintClass"],
    )

    assert validate_resolver_output_contract(output) is True
    assert output["resolution_status"] == "resolved"
    assert output["resolved_symbols"] == [
        {
            "symbol_name": "BP_Player",
            "symbol_kind": "BlueprintClass",
            "source_path": "Content/Blueprints/BP_Player.uasset",
            "resolution_method": "path_metadata",
            "confidence": "exact_path_match",
        }
    ]
    assert output["unresolved_symbols"] == []


def test_exact_symbol_name_match_resolves_as_candidate_match():
    output = _resolve(requested_symbols=["BP_Player"], allowed_symbol_kinds=["BlueprintClass"])

    assert output["resolution_status"] == "resolved"
    assert output["resolved_symbols"][0]["symbol_name"] == "BP_Player"
    assert output["resolved_symbols"][0]["confidence"] == "candidate_match"
    assert output["resolved_symbols"][0]["resolution_method"] == "path_metadata"


def test_candidate_kind_filtering_uses_allowed_symbol_kinds():
    output = _resolve(
        requested_symbols=["BP_Player", "InventoryComponent"],
        allowed_symbol_kinds=["CppClass"],
    )

    assert output["resolution_status"] == "partially_resolved"
    assert output["resolved_symbols"] == [
        {
            "symbol_name": "InventoryComponent",
            "symbol_kind": "CppClass",
            "source_path": "Source/FixtureGame/Public/InventoryComponent.h",
            "resolution_method": "path_metadata",
            "confidence": "candidate_match",
        }
    ]
    assert output["unresolved_symbols"] == [
        {
            "symbol_name": "BP_Player",
            "symbol_kind": "CppClass",
            "source_path": None,
            "resolution_method": "path_metadata",
            "confidence": "unresolved",
        }
    ]


def test_partial_resolution_sets_partially_resolved_status():
    output = _resolve(
        requested_symbols=["BP_Player", "MissingInventoryEvent"],
        allowed_symbol_kinds=["BlueprintClass", "Event"],
    )

    assert output["resolution_status"] == "partially_resolved"
    assert [entry["symbol_name"] for entry in output["resolved_symbols"]] == ["BP_Player"]
    assert output["unresolved_symbols"] == [
        {
            "symbol_name": "MissingInventoryEvent",
            "symbol_kind": "BlueprintClass",
            "source_path": None,
            "resolution_method": "path_metadata",
            "confidence": "unresolved",
        }
    ]


def test_unresolved_requests_are_bounded_and_metadata_only():
    output = _resolve(requested_symbols=["MissingInventoryEvent"], allowed_symbol_kinds=["Event"])

    assert output["resolution_status"] == "unresolved"
    assert output["resolved_symbols"] == []
    assert output["unresolved_symbols"] == [
        {
            "symbol_name": "MissingInventoryEvent",
            "symbol_kind": "Event",
            "source_path": None,
            "resolution_method": "path_metadata",
            "confidence": "unresolved",
        }
    ]
    serialized = json.dumps(output, sort_keys=True)
    for forbidden in ("fixture header content", "fixture source content", "provider_output", "raw_model_text", "graph_nodes"):
        assert forbidden not in serialized


def test_source_fingerprint_mismatch_fails_closed():
    manifest = _manifest()
    candidate_index = _candidate_index(manifest)
    resolver_input = _resolver_input(manifest, candidate_index)
    resolver_input["source_manifest_fingerprint"] = "wrongmanifestfingerprint"

    with pytest.raises(ValueError, match="source_manifest_fingerprint mismatch"):
        resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)


def test_invalid_input_manifest_and_candidate_index_fail_closed():
    manifest = _manifest()
    candidate_index = _candidate_index(manifest)

    invalid_input = _resolver_input(manifest, candidate_index)
    invalid_input["raw_model_text"] = "raw text must not enter resolver"
    with pytest.raises(ValueError, match="raw_model_text"):
        resolve_unreal_symbols_from_path_metadata(invalid_input, manifest, candidate_index)

    invalid_manifest = copy.deepcopy(manifest)
    del invalid_manifest["artifact_fingerprint"]
    with pytest.raises(ValueError, match="manifest|fields|artifact_fingerprint"):
        resolve_unreal_symbols_from_path_metadata(_resolver_input(manifest, candidate_index), invalid_manifest, candidate_index)

    invalid_candidate_index = copy.deepcopy(candidate_index)
    invalid_candidate_index["candidates"][0]["candidate_kind"] = "blueprint_graph"
    invalid_candidate_index["artifact_fingerprint"] = compute_json_payload_fingerprint(invalid_candidate_index)
    with pytest.raises(ValueError, match="candidate_kind"):
        resolve_unreal_symbols_from_path_metadata(_resolver_input(manifest, candidate_index), manifest, invalid_candidate_index)


def test_output_validates_against_locked_resolver_contract():
    output = _resolve(requested_symbols=["BP_Player", "InventoryComponent"], allowed_symbol_kinds=["BlueprintClass", "CppClass"])

    assert validate_resolver_output_contract(output) is True
    assert output["artifact_fingerprint"] == compute_json_payload_fingerprint(output)


def test_repeated_runs_are_deterministic():
    manifest = _manifest()
    candidate_index = _candidate_index(manifest)
    resolver_input = _resolver_input(
        manifest,
        candidate_index,
        requested_symbols=["BP_Player", "InventoryComponent"],
        allowed_symbol_kinds=["BlueprintClass", "CppClass"],
    )

    first = resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)
    second = resolve_unreal_symbols_from_path_metadata(copy.deepcopy(resolver_input), copy.deepcopy(manifest), copy.deepcopy(candidate_index))

    assert first == second
    assert first["resolved_symbols"] == sorted(
        first["resolved_symbols"],
        key=lambda entry: (entry["symbol_kind"], entry["symbol_name"], entry["source_path"]),
    )


def test_resolver_does_not_read_or_expose_file_contents(monkeypatch):
    manifest = _manifest()
    candidate_index = _candidate_index(manifest)
    resolver_input = _resolver_input(manifest, candidate_index, requested_symbols=["BP_Player"])

    def fail_read_text(self, *args, **kwargs):
        raise AssertionError(f"unexpected file read: {self}")

    monkeypatch.setattr(Path, "read_text", fail_read_text)
    output = resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)

    assert output["resolution_status"] == "resolved"
    serialized = json.dumps(output, sort_keys=True)
    assert "fixture blueprint asset content" not in serialized
    assert "fixture header content" not in serialized
    assert "fixture source content" not in serialized


def test_resolver_creates_no_execution_or_output_artifacts():
    workspace = _workspace_dir("unreal_symbol_resolver_no_execution")

    output = _resolve(requested_symbols=["BP_Player"], allowed_symbol_kinds=["BlueprintClass"])

    assert output["artifact_type"] == "game_adapter_unreal_symbol_resolver_output"
    assert not (workspace / ".dce" / "execution").exists()
    assert not (workspace / ".dce" / "output").exists()
    assert not (workspace / ".dce" / "outputs").exists()


def test_stage2_preview_dispatch_remains_unchanged():
    project_root = _workspace_dir("unreal_symbol_resolver_stage2_dispatch_unchanged")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert "symbol_resolver" not in json.dumps(result.preview_artifact, sort_keys=True)
    assert not (project_root / ".dce" / "plans" / "unreal-symbol-resolver.output.json").exists()
    assert not (project_root / ".dce" / "execution").exists()


def test_stage75_lifecycle_order_remains_unchanged():
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
