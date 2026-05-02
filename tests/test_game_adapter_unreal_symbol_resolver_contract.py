import copy
import inspect
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import validate_game_adapter_stage2_preview_contract
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
import aether.dgce.game_adapter_unreal_symbol_resolver_contract as resolver_contract
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import (
    CONTRACT_NAME,
    CONTRACT_VERSION,
    INPUT_ARTIFACT_TYPE,
    OUTPUT_ARTIFACT_TYPE,
    validate_resolver_input_contract,
    validate_resolver_output_contract,
)


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


def _valid_input() -> dict:
    return {
        "artifact_type": INPUT_ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": "game",
        "domain": "game_adapter",
        "source_manifest_fingerprint": "manifestabcdef123456",
        "source_candidate_index_fingerprint": "candidateabcdef123456",
        "requested_symbols": ["BP_Player", "InventoryComponent"],
        "allowed_symbol_kinds": ["ActorComponent", "BlueprintClass", "CppClass"],
        "stage_usage": "both",
    }


def _valid_output() -> dict:
    payload = {
        "artifact_type": OUTPUT_ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": "game",
        "domain": "game_adapter",
        "source_input_fingerprint": compute_json_payload_fingerprint(_valid_input()),
        "resolved_symbols": [
            {
                "symbol_name": "BP_Player",
                "symbol_kind": "BlueprintClass",
                "source_path": "Content/Blueprints/BP_Player.uasset",
                "resolution_method": "path_metadata",
                "confidence": "exact_path_match",
            },
            {
                "symbol_name": "InventoryComponent",
                "symbol_kind": "CppClass",
                "source_path": "Source/FixtureGame/Public/InventoryComponent.h",
                "resolution_method": "path_metadata",
                "confidence": "candidate_match",
            },
        ],
        "unresolved_symbols": [
            {
                "symbol_name": "MissingInventoryEvent",
                "symbol_kind": "Event",
                "source_path": None,
                "resolution_method": "path_metadata",
                "confidence": "unresolved",
            }
        ],
        "resolution_status": "partially_resolved",
        "integration_points": {
            "stage2_preview_context": {
                "stage": "Stage2Preview",
                "context_kind": "resolver_metadata",
                "symbol_metadata_available": True,
            },
            "stage7_alignment_context": {
                "stage": "Stage7Alignment",
                "context_kind": "resolver_metadata",
                "symbol_metadata_available": True,
            },
        },
    }
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    return payload


def test_resolver_contract_schema_file_defines_input_and_output_contracts():
    schema_path = Path("contracts/game_adapter/game-adapter-unreal-symbol-resolver-v1.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert schema["title"] == CONTRACT_NAME
    assert "ResolverInputContract" in schema["$defs"]
    assert "ResolverOutputContract" in schema["$defs"]
    assert schema["$defs"]["ResolverInputContract"]["properties"]["artifact_type"]["const"] == INPUT_ARTIFACT_TYPE
    assert schema["$defs"]["ResolverOutputContract"]["properties"]["artifact_type"]["const"] == OUTPUT_ARTIFACT_TYPE
    assert schema["$defs"]["ResolverInputContract"]["properties"]["stage_usage"]["enum"] == [
        "Stage2Preview",
        "Stage7Alignment",
        "both",
    ]
    assert schema["$defs"]["ResolverOutputContract"]["properties"]["resolution_status"]["enum"] == [
        "resolved",
        "partially_resolved",
        "unresolved",
        "input_invalid",
    ]


def test_valid_resolver_input_contract_validates():
    payload = _valid_input()

    assert validate_resolver_input_contract(payload) is True
    assert payload["artifact_type"] == INPUT_ARTIFACT_TYPE
    assert payload["contract_name"] == CONTRACT_NAME
    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["adapter"] == "game"
    assert payload["domain"] == "game_adapter"


def test_valid_resolver_output_contract_validates():
    payload = _valid_output()

    assert validate_resolver_output_contract(payload) is True
    assert payload["artifact_type"] == OUTPUT_ARTIFACT_TYPE
    assert payload["contract_name"] == CONTRACT_NAME
    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["source_input_fingerprint"] == compute_json_payload_fingerprint(_valid_input())
    assert payload["artifact_fingerprint"] == compute_json_payload_fingerprint(payload)


def test_invalid_stage_usage_fails_closed():
    payload = _valid_input()
    payload["stage_usage"] = "Stage8Execution"

    with pytest.raises(ValueError, match="stage_usage"):
        validate_resolver_input_contract(payload)


def test_unsupported_symbol_kind_fails_closed():
    input_payload = _valid_input()
    input_payload["allowed_symbol_kinds"] = ["BlueprintGraph"]
    with pytest.raises(ValueError, match="allowed_symbol_kinds"):
        validate_resolver_input_contract(input_payload)

    output_payload = _valid_output()
    output_payload["resolved_symbols"][0]["symbol_kind"] = "BlueprintGraph"
    output_payload["artifact_fingerprint"] = compute_json_payload_fingerprint(output_payload)
    with pytest.raises(ValueError, match="symbol_kind"):
        validate_resolver_output_contract(output_payload)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("raw_file_contents", "fixture source content"),
        ("raw_model_text", "provider text"),
        ("raw_provider_text", "provider text"),
        ("binary_blueprint_payload", "uasset bytes"),
        ("provider_output", "raw provider output"),
    ],
)
def test_raw_file_content_model_provider_fields_fail_closed(field_name, field_value):
    payload = _valid_output()
    payload[field_name] = field_value
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)

    with pytest.raises(ValueError, match=field_name):
        validate_resolver_output_contract(payload)


@pytest.mark.parametrize("field_name", ["graph", "graph_nodes", "node_data", "nodes", "blueprint_graph"])
def test_graph_and_node_fields_fail_closed(field_name):
    payload = _valid_output()
    payload["resolved_symbols"][0][field_name] = []
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)

    with pytest.raises(ValueError, match=field_name):
        validate_resolver_output_contract(payload)


@pytest.mark.parametrize("field_name", ["execution_directive", "write_directives", "write_targets", "written_files"])
def test_execution_and_write_directives_fail_closed(field_name):
    payload = _valid_output()
    payload["integration_points"][field_name] = ["write Content/Blueprints/BP_Player.uasset"]
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)

    with pytest.raises(ValueError, match=field_name):
        validate_resolver_output_contract(payload)


def test_output_integration_points_are_bounded_to_stage2_preview_and_stage7_alignment():
    payload = _valid_output()
    assert set(payload["integration_points"]) == {"stage2_preview_context", "stage7_alignment_context"}
    assert payload["integration_points"]["stage2_preview_context"]["stage"] == "Stage2Preview"
    assert payload["integration_points"]["stage7_alignment_context"]["stage"] == "Stage7Alignment"

    tampered = copy.deepcopy(payload)
    tampered["integration_points"]["stage7_alignment_context"]["stage"] = "Stage8Execution"
    tampered["artifact_fingerprint"] = compute_json_payload_fingerprint(tampered)
    with pytest.raises(ValueError, match="Stage7Alignment|stage"):
        validate_resolver_output_contract(tampered)


def test_no_resolver_implementation_exists_in_contract_module():
    public_functions = {
        name
        for name, value in inspect.getmembers(resolver_contract, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_functions == {
        "validate_resolver_input_contract",
        "validate_resolver_output_contract",
    }
    assert not any(name.startswith(("build_", "persist_", "resolve_")) for name in public_functions)


def test_stage2_preview_contract_locked_behavior_remains_unchanged():
    project_root = _workspace_dir("unreal_symbol_resolver_stage2_preview_unchanged")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert "symbol_resolver" not in json.dumps(result.preview_artifact, sort_keys=True)
    assert "source_input_fingerprint" not in result.preview_artifact
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
