import json
from pathlib import Path

import pytest

from aether.dgce.context_assembly import persist_stage0_input
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import build_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import build_unreal_symbol_candidate_index
from aether.dgce.game_adapter_unreal_symbol_resolver import (
    UNREAL_SYMBOL_RESOLVER_OUTPUT_RELATIVE_PATH,
    persist_unreal_symbol_resolver_output,
    resolve_unreal_symbols_from_path_metadata,
)
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import validate_resolver_output_contract
from aether.dgce.read_api import get_game_adapter_unreal_symbol_resolver_output


FIXTURE_DIR = Path("tests/fixtures/light_unreal_symbol_resolver")
STAGE2_FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage2_preview")
UNREAL_PROJECT = Path("tests/fixtures/unreal_project_structure/FixtureGame")


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _stage2_fixture(name: str) -> dict:
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


def _manifest_and_candidate_index() -> tuple[dict, dict]:
    manifest = build_unreal_project_structure_manifest(UNREAL_PROJECT)
    return manifest, build_unreal_symbol_candidate_index(manifest)


def test_valid_fixture_resolves_and_validates_against_expected_output():
    manifest, candidate_index = _manifest_and_candidate_index()
    resolver_input = _fixture("valid_resolver_input.json")
    expected_output = _fixture("expected_resolver_output.json")

    output = resolve_unreal_symbols_from_path_metadata(resolver_input, manifest, candidate_index)

    assert validate_resolver_output_contract(output) is True
    assert output == expected_output


def test_expected_resolved_and_unresolved_symbols_match_fixture():
    manifest, candidate_index = _manifest_and_candidate_index()
    output = resolve_unreal_symbols_from_path_metadata(
        _fixture("valid_resolver_input.json"),
        manifest,
        candidate_index,
    )
    expected = _fixture("expected_resolver_output.json")

    assert output["resolution_status"] == "partially_resolved"
    assert output["resolved_symbols"] == expected["resolved_symbols"]
    assert output["unresolved_symbols"] == expected["unresolved_symbols"]


def test_persisted_fixture_resolver_output_reads_successfully():
    workspace = _workspace_dir("light_unreal_symbol_resolver_fixture_read")
    manifest, candidate_index = _manifest_and_candidate_index()
    output = resolve_unreal_symbols_from_path_metadata(
        _fixture("valid_resolver_input.json"),
        manifest,
        candidate_index,
    )

    persisted = persist_unreal_symbol_resolver_output(output, workspace_path=workspace)
    detail = get_game_adapter_unreal_symbol_resolver_output(workspace, Path(persisted.artifact_path).name)

    assert persisted.artifact_path == UNREAL_SYMBOL_RESOLVER_OUTPUT_RELATIVE_PATH.as_posix()
    assert detail["read_model_type"] == "game_adapter_unreal_symbol_resolver_output_read_model"
    assert detail["resolution_status"] == output["resolution_status"]
    assert detail["resolved_symbols"] == output["resolved_symbols"]
    assert detail["unresolved_symbols"] == output["unresolved_symbols"]
    assert detail["artifact_fingerprint"] == output["artifact_fingerprint"]


def test_invalid_raw_freeform_fixture_fails_closed():
    manifest, candidate_index = _manifest_and_candidate_index()

    with pytest.raises(ValueError, match="raw_model_text"):
        resolve_unreal_symbols_from_path_metadata(
            _fixture("invalid_raw_freeform_resolver_input.json"),
            manifest,
            candidate_index,
        )


def test_source_fingerprint_mismatch_fixture_fails_closed():
    manifest, candidate_index = _manifest_and_candidate_index()

    with pytest.raises(ValueError, match="source_manifest_fingerprint mismatch"):
        resolve_unreal_symbols_from_path_metadata(
            _fixture("source_fingerprint_mismatch_resolver_input.json"),
            manifest,
            candidate_index,
        )


def test_fixture_smoke_creates_no_execution_output_or_alignment_artifacts():
    workspace = _workspace_dir("light_unreal_symbol_resolver_fixture_no_execution")
    manifest, candidate_index = _manifest_and_candidate_index()
    output = resolve_unreal_symbols_from_path_metadata(
        _fixture("valid_resolver_input.json"),
        manifest,
        candidate_index,
    )
    persist_unreal_symbol_resolver_output(output, workspace_path=workspace)

    assert not (workspace / ".dce" / "execution" / "alignment").exists()
    assert not (workspace / ".dce" / "execution").exists()
    assert not (workspace / ".dce" / "output").exists()
    assert not (workspace / ".dce" / "outputs").exists()


def test_stage2_preview_dispatch_remains_unchanged_by_resolver_fixtures():
    workspace = _workspace_dir("light_unreal_symbol_resolver_fixture_stage2_unchanged")
    persisted = persist_stage0_input(workspace, _stage2_fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        workspace / persisted.artifact_path,
        workspace_path=workspace,
    )

    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert "symbol_resolver" not in json.dumps(result.preview_artifact, sort_keys=True)
    assert not (workspace / ".dce" / "plans" / "unreal-symbol-resolver.resolution.json").exists()
    assert not (workspace / ".dce" / "execution").exists()


def test_resolver_fixture_smoke_keeps_stage75_lifecycle_order_locked():
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
