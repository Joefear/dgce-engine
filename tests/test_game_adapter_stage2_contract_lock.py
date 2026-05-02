import copy
import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input, release_gce_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import (
    build_game_adapter_stage2_preview,
    render_game_adapter_stage2_human_view,
    render_game_adapter_stage2_machine_view,
    select_game_adapter_stage2_strategy,
    validate_game_adapter_stage2_preview_contract,
)
from aether.dgce.game_adapter_stage2_dispatch import (
    GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH,
    build_game_adapter_stage2_preview_from_released_stage0,
)
from aether.dgce.game_adapter_unreal_manifest import (
    build_unreal_project_structure_manifest,
    persist_unreal_project_structure_manifest,
)
from aether.dgce.game_adapter_unreal_symbol_candidates import (
    build_unreal_symbol_candidate_index,
    persist_unreal_symbol_candidate_index,
)
from aether.dgce.read_api import (
    get_game_adapter_unreal_project_structure_manifest,
    get_game_adapter_unreal_symbol_candidate_index,
)
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient


FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage2_preview")
UNREAL_FIXTURE_PROJECT = Path("tests/fixtures/unreal_project_structure/FixtureGame")


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


def _planned_change(
    *,
    change_id: str = "change.player-component",
    domain_type: str = "component",
    target_kind: str = "ActorComponent",
    target_id: str = "BP_Player.InventoryComponent",
    target_path: str = "/Game/Blueprints/BP_Player",
    intent: str = "add_gameplay_capability",
    review_focus: str = "component_setup",
    operation: str = "modify",
    strategy: str | None = None,
) -> dict:
    change = {
        "change_id": change_id,
        "target": {
            "target_id": target_id,
            "target_path": target_path,
            "target_kind": target_kind,
        },
        "operation": operation,
        "domain_type": domain_type,
        "summary": {
            "intent": intent,
            "impact": "gameplay",
            "risk": "medium",
            "review_focus": review_focus,
        },
    }
    if strategy is not None:
        change["strategy"] = strategy
    return change


def test_contract_lock_released_gce_stage0_fixture_produces_valid_preview():
    project_root = _workspace_dir("game_adapter_contract_lock_preview")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert validate_game_adapter_stage2_preview_contract(result.preview_artifact) is True
    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert result.preview_artifact["source_stage0_fingerprint"] == persisted.artifact["artifact_fingerprint"]
    assert (project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH).exists()


def test_contract_lock_preview_views_are_deterministic_and_bounded():
    persisted = persist_stage0_input(
        _workspace_dir("game_adapter_contract_lock_views"),
        _fixture("valid_released_gce_source_input.json"),
    )
    first = build_game_adapter_stage2_preview_from_released_stage0(persisted.artifact).preview_artifact
    second = build_game_adapter_stage2_preview_from_released_stage0(copy.deepcopy(persisted.artifact)).preview_artifact

    assert first == second
    assert first["machine_view"] == render_game_adapter_stage2_machine_view(
        first["planned_changes"],
        first["governance_context"],
    )
    assert first["human_view"] == render_game_adapter_stage2_human_view(
        first["planned_changes"],
        first["governance_context"],
    )
    serialized_human = json.dumps(first["human_view"], sort_keys=True)
    for forbidden in ("raw_model", "raw_provider", "provider_output", "freeform", "free_form"):
        assert forbidden not in serialized_human


def test_contract_lock_strategy_selector_and_invalid_inputs_fail_closed():
    blueprint = _planned_change(domain_type="component", target_kind="ActorComponent")
    cpp = _planned_change(
        domain_type="C++",
        target_kind="CppClass",
        target_id="UInventorySubsystem",
        target_path="/Source/Game/InventorySubsystem",
    )
    both = _planned_change(
        domain_type="event",
        target_kind="Event",
        target_id="OnInventoryChanged",
        target_path="/Game/Blueprints/BP_Player",
        intent="prepare_for_review",
        review_focus="logic_flow",
    )

    assert select_game_adapter_stage2_strategy(blueprint) == "Blueprint"
    assert select_game_adapter_stage2_strategy(cpp) == "C++"
    assert select_game_adapter_stage2_strategy(both) == "both"

    with pytest.raises(ValueError, match="strategy"):
        build_game_adapter_stage2_preview(source_input_reference="gce-session-1", planned_changes=[{**cpp, "strategy": "Blueprint"}])
    with pytest.raises(ValueError, match="domain_type"):
        build_game_adapter_stage2_preview(
            source_input_reference="gce-session-1",
            planned_changes=[_planned_change(domain_type="material")],
        )
    with pytest.raises(ValueError, match="operation"):
        build_game_adapter_stage2_preview(
            source_input_reference="gce-session-1",
            planned_changes=[_planned_change(operation="rename")],
        )


def test_contract_lock_preview_dispatch_does_not_create_execution_or_output_artifacts():
    project_root = _workspace_dir("game_adapter_contract_lock_no_execution")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert not (project_root / ".dce" / "execution").exists()
    assert not (project_root / ".dce" / "output").exists()
    assert not (project_root / ".dce" / "outputs").exists()


def test_contract_lock_unreal_manifest_is_path_facts_only_and_read_model_verifies_fingerprint():
    workspace = _workspace_dir("game_adapter_contract_lock_manifest")
    persisted = persist_unreal_project_structure_manifest(UNREAL_FIXTURE_PROJECT, workspace_path=workspace)

    manifest = build_unreal_project_structure_manifest(UNREAL_FIXTURE_PROJECT)
    detail = get_game_adapter_unreal_project_structure_manifest(workspace, Path(persisted.artifact_path).name)

    assert manifest == build_unreal_project_structure_manifest(UNREAL_FIXTURE_PROJECT)
    assert detail["read_model_type"] == "game_adapter_unreal_project_structure_manifest_read_model"
    assert detail["artifact_fingerprint"] == persisted.manifest_artifact["artifact_fingerprint"]
    serialized = json.dumps(detail, sort_keys=True)
    for forbidden in ("fixture header content", "fixture source content", "fixture blueprint asset content"):
        assert forbidden not in serialized


def test_contract_lock_symbol_candidates_are_path_derived_and_read_model_verifies_fingerprint():
    workspace = _workspace_dir("game_adapter_contract_lock_candidates")
    manifest = build_unreal_project_structure_manifest(UNREAL_FIXTURE_PROJECT)
    persisted = persist_unreal_symbol_candidate_index(manifest, workspace_path=workspace)

    index = build_unreal_symbol_candidate_index(manifest)
    detail = get_game_adapter_unreal_symbol_candidate_index(workspace, Path(persisted.artifact_path).name)

    assert index == build_unreal_symbol_candidate_index(copy.deepcopy(manifest))
    assert detail["read_model_type"] == "game_adapter_unreal_symbol_candidate_index_read_model"
    assert detail["artifact_fingerprint"] == persisted.candidate_index_artifact["artifact_fingerprint"]
    for candidate in detail["candidates"]:
        source_path = Path(candidate["source_path"])
        expected_name = source_path.stem if source_path.suffix else source_path.name
        assert candidate["candidate_name"] == expected_name
        assert candidate["resolution_status"] == "path_candidate"
    serialized = json.dumps(detail, sort_keys=True)
    assert "fixture header content" not in serialized
    assert "fixture source content" not in serialized


def test_contract_lock_candidate_index_is_not_used_by_preview_dispatch_unless_explicitly_called():
    project_root = _workspace_dir("game_adapter_contract_lock_candidate_not_dispatch")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))

    build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )

    assert not (project_root / ".dce" / "plans" / "unreal-symbol-candidates.index.json").exists()
    assert not (project_root / ".dce" / "plans" / "unreal-project-structure.manifest.json").exists()


def test_contract_lock_game_adapter_http_routes_are_get_only():
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/game-adapter/")
    }

    assert route_methods == {
        "/v1/dgce/game-adapter/stage2-preview-artifacts": {"GET"},
        "/v1/dgce/game-adapter/stage2-preview-artifacts/{artifact_name}": {"GET"},
        "/v1/dgce/game-adapter/unreal-project-structure-manifests": {"GET"},
        "/v1/dgce/game-adapter/unreal-project-structure-manifests/{artifact_name}": {"GET"},
        "/v1/dgce/game-adapter/unreal-symbol-candidate-indexes": {"GET"},
        "/v1/dgce/game-adapter/unreal-symbol-candidate-indexes/{artifact_name}": {"GET"},
    }


def test_contract_lock_game_adapter_sdk_helpers_are_get_only(monkeypatch):
    calls: list[tuple[str, str]] = []
    client = DGCEClient("http://example.test", api_key="secret-key")

    class _Response:
        def read(self) -> bytes:
            return b'{"ok": true}'

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        calls.append((request.get_method(), request.full_url))
        assert request.headers["X-api-key"] == "secret-key"
        return _Response()

    monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

    client.list_game_adapter_stage2_preview_artifacts("workspace-root")
    client.get_game_adapter_stage2_preview_artifact("workspace-root", "game-adapter-stage2.preview.json")
    client.list_game_adapter_unreal_project_structure_manifests("workspace-root")
    client.get_game_adapter_unreal_project_structure_manifest("workspace-root", "unreal-project-structure.manifest.json")
    client.list_game_adapter_unreal_symbol_candidate_indexes("workspace-root")
    client.get_game_adapter_unreal_symbol_candidate_index("workspace-root", "unreal-symbol-candidates.index.json")

    assert {method for method, _ in calls} == {"GET"}
    assert len(calls) == 6


def test_contract_lock_gce_stage0_release_and_lifecycle_order_remain_locked():
    project_root = _workspace_dir("game_adapter_contract_lock_stage0")
    persisted = persist_stage0_input(project_root, _fixture("valid_released_gce_source_input.json"))
    release = release_gce_stage0_input(project_root / persisted.artifact_path)

    assert release.allowed is True
    assert release.result["source_artifact_fingerprint"] == persisted.artifact["artifact_fingerprint"]
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
