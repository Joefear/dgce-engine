import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import build_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import build_unreal_symbol_candidate_index
from aether.dgce.game_adapter_unreal_symbol_resolver import (
    UNREAL_SYMBOL_RESOLVER_OUTPUT_RELATIVE_PATH,
    persist_unreal_symbol_resolver_output,
    resolve_unreal_symbols_from_path_metadata,
)
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import OUTPUT_ARTIFACT_TYPE
from aether.dgce.read_api import (
    get_game_adapter_unreal_symbol_resolver_output,
    list_game_adapter_unreal_symbol_resolver_outputs,
)
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient


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


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolver_input(manifest: dict, candidate_index: dict) -> dict:
    return {
        "artifact_type": "game_adapter_unreal_symbol_resolver_input",
        "contract_name": "DGCEGameAdapterUnrealSymbolResolver",
        "contract_version": "dgce.game_adapter.unreal_symbol_resolver.v1",
        "adapter": "game",
        "domain": "game_adapter",
        "source_manifest_fingerprint": manifest["artifact_fingerprint"],
        "source_candidate_index_fingerprint": candidate_index["artifact_fingerprint"],
        "requested_symbols": ["BP_Player", "InventoryComponent"],
        "allowed_symbol_kinds": ["BlueprintClass", "CppClass"],
        "stage_usage": "both",
    }


def _persist_resolution(workspace_path: Path):
    manifest = build_unreal_project_structure_manifest(FIXTURE_PROJECT)
    candidate_index = build_unreal_symbol_candidate_index(manifest)
    output = resolve_unreal_symbols_from_path_metadata(
        _resolver_input(manifest, candidate_index),
        manifest,
        candidate_index,
    )
    return persist_unreal_symbol_resolver_output(output, workspace_path=workspace_path)


def test_valid_persisted_resolver_output_appears_in_list_and_detail_read_model():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_valid")
    persisted = _persist_resolution(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path

    index = list_game_adapter_unreal_symbol_resolver_outputs(workspace_path)
    detail = get_game_adapter_unreal_symbol_resolver_output(workspace_path, artifact_path.name)

    assert persisted.artifact_path == UNREAL_SYMBOL_RESOLVER_OUTPUT_RELATIVE_PATH.as_posix()
    assert index["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_index"
    assert index["adapter"] == "game"
    assert index["domain"] == "game_adapter"
    assert index["artifact_count"] == 1
    assert index["artifacts"] == [detail]
    assert detail["read_model_type"] == "game_adapter_unreal_symbol_resolver_output_read_model"
    assert detail["artifact_type"] == OUTPUT_ARTIFACT_TYPE
    assert detail["contract_name"] == "DGCEGameAdapterUnrealSymbolResolver"
    assert detail["contract_version"] == "dgce.game_adapter.unreal_symbol_resolver.v1"
    assert detail["source_input_fingerprint"] == persisted.resolver_output_artifact["source_input_fingerprint"]
    assert detail["resolution_status"] == "resolved"
    assert detail["resolved_symbols"] == persisted.resolver_output_artifact["resolved_symbols"]
    assert detail["resolved_symbols_summary"] == {
        "symbol_count": 2,
        "symbol_kinds": {"BlueprintClass": 1, "CppClass": 1},
        "confidence": {"candidate_match": 2},
    }
    assert detail["unresolved_symbols"] == []
    assert detail["unresolved_symbols_summary"] == {
        "symbol_count": 0,
        "symbol_kinds": {},
        "confidence": {},
    }
    assert detail["integration_points"] == persisted.resolver_output_artifact["integration_points"]
    assert detail["artifact_fingerprint"] == persisted.resolver_output_artifact["artifact_fingerprint"]


def test_resolver_output_detail_read_verifies_artifact_fingerprint_fail_closed():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_invalid_fingerprint")
    persisted = _persist_resolution(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    payload = _read_json(artifact_path)
    payload["resolution_status"] = "unresolved"
    _write_json(artifact_path, payload)

    detail = get_game_adapter_unreal_symbol_resolver_output(workspace_path, artifact_path.name)

    assert detail["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_read_error"
    assert detail["reason_code"] == "artifact_fingerprint_invalid"
    assert detail["artifact_fingerprint"] == persisted.resolver_output_artifact["artifact_fingerprint"]
    assert detail["resolved_symbols"] is None
    assert detail["unresolved_symbols"] is None
    assert detail["integration_points"] is None


def test_missing_malformed_and_missing_fingerprint_resolver_artifacts_fail_closed():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_malformed")
    persisted = _persist_resolution(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    malformed_path = workspace_path / ".dce" / "plans" / "unreal-symbol-resolver-broken.resolution.json"
    malformed_path.write_text("{not valid json", encoding="utf-8")
    payload = _read_json(artifact_path)
    del payload["artifact_fingerprint"]
    _write_json(artifact_path, payload)

    missing = get_game_adapter_unreal_symbol_resolver_output(
        workspace_path,
        "unreal-symbol-resolver-missing.resolution.json",
    )
    malformed = get_game_adapter_unreal_symbol_resolver_output(workspace_path, malformed_path.name)
    missing_fingerprint = get_game_adapter_unreal_symbol_resolver_output(workspace_path, artifact_path.name)

    assert missing["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_read_error"
    assert missing["reason_code"] == "artifact_missing"
    assert malformed["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_read_error"
    assert malformed["reason_code"] == "artifact_malformed"
    assert missing_fingerprint["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_read_error"
    assert missing_fingerprint["reason_code"] == "artifact_fingerprint_missing"


def test_contract_invalid_resolver_output_fails_closed_after_valid_fingerprint_check():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_contract_invalid")
    persisted = _persist_resolution(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    payload = _read_json(artifact_path)
    payload["resolved_symbols"][0]["confidence"] = "provider_guess"
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    _write_json(artifact_path, payload)

    detail = get_game_adapter_unreal_symbol_resolver_output(workspace_path, artifact_path.name)

    assert detail["artifact_type"] == "game_adapter_unreal_symbol_resolver_output_read_error"
    assert detail["reason_code"] == "contract_invalid"
    assert detail["artifact_fingerprint"] == payload["artifact_fingerprint"]


def test_resolver_output_read_model_exposes_no_raw_file_contents_or_provider_text():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_no_contents")
    persisted = _persist_resolution(workspace_path)

    detail = get_game_adapter_unreal_symbol_resolver_output(workspace_path, Path(persisted.artifact_path).name)
    serialized = json.dumps(detail, sort_keys=True)

    for forbidden in (
        "fixture header content",
        "fixture source content",
        "fixture blueprint asset content",
        "fixture config content",
        "raw_model_text",
        "provider_output",
        "raw_provider_text",
    ):
        assert forbidden not in serialized


def test_resolver_output_http_routes_are_get_only_and_read_exact_payloads():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_http")
    persisted = _persist_resolution(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    index_response = client.get(
        "/v1/dgce/game-adapter/unreal-symbol-resolutions",
        params={"workspace_path": str(workspace_path)},
    )
    detail_response = client.get(
        f"/v1/dgce/game-adapter/unreal-symbol-resolutions/{artifact_path.name}",
        params={"workspace_path": str(workspace_path)},
    )
    post_response = client.post(
        "/v1/dgce/game-adapter/unreal-symbol-resolutions",
        params={"workspace_path": str(workspace_path)},
    )

    assert index_response.status_code == 200
    assert detail_response.status_code == 200
    assert post_response.status_code == 405
    assert index_response.json()["artifacts"] == [detail_response.json()]
    assert artifact_path.read_bytes() == before
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/game-adapter/unreal-symbol-resolutions")
    }
    assert route_methods == {
        "/v1/dgce/game-adapter/unreal-symbol-resolutions": {"GET"},
        "/v1/dgce/game-adapter/unreal-symbol-resolutions/{artifact_name}": {"GET"},
    }


def test_resolver_output_sdk_helpers_are_read_only(monkeypatch):
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

    assert client.list_game_adapter_unreal_symbol_resolver_outputs("workspace-root") == {"ok": True}
    assert client.get_game_adapter_unreal_symbol_resolver_output(
        "workspace-root",
        "unreal-symbol-resolver.resolution.json",
    ) == {"ok": True}

    assert calls == [
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/unreal-symbol-resolutions?workspace_path=workspace-root",
        ),
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/unreal-symbol-resolutions/unreal-symbol-resolver.resolution.json?workspace_path=workspace-root",
        ),
    ]


def test_stage2_preview_dispatch_remains_unchanged_until_resolver_is_explicitly_called():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_stage2_unchanged")
    persisted = persist_stage0_input(workspace_path, _fixture("valid_released_gce_source_input.json"))

    result = build_game_adapter_stage2_preview_from_released_stage0(
        workspace_path / persisted.artifact_path,
        workspace_path=workspace_path,
    )

    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert not (workspace_path / ".dce" / "plans" / "unreal-symbol-resolver.resolution.json").exists()
    assert list_game_adapter_unreal_symbol_resolver_outputs(workspace_path)["artifact_count"] == 0
    assert not (workspace_path / ".dce" / "execution").exists()


def test_resolver_reads_do_not_create_stage7_alignment_execution_or_output_artifacts():
    workspace_path = _workspace_dir("unreal_symbol_resolver_read_no_execution")
    persisted = _persist_resolution(workspace_path)

    list_game_adapter_unreal_symbol_resolver_outputs(workspace_path)
    get_game_adapter_unreal_symbol_resolver_output(workspace_path, Path(persisted.artifact_path).name)

    assert not (workspace_path / ".dce" / "execution" / "alignment").exists()
    assert not (workspace_path / ".dce" / "execution").exists()
    assert not (workspace_path / ".dce" / "output").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()


def test_resolver_read_surface_keeps_stage75_lifecycle_order_locked():
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
