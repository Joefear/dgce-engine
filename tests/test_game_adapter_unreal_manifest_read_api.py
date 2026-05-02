import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_stage2_dispatch import build_game_adapter_stage2_preview_from_released_stage0
from aether.dgce.game_adapter_unreal_manifest import (
    ARTIFACT_TYPE,
    UNREAL_PROJECT_STRUCTURE_MANIFEST_RELATIVE_PATH,
    persist_unreal_project_structure_manifest,
)
from aether.dgce.read_api import (
    get_game_adapter_unreal_project_structure_manifest,
    list_game_adapter_unreal_project_structure_manifests,
)
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient


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


def _stage0_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": {
            "project_id": "preview-unchanged",
            "project_name": "Preview Unchanged",
            "owner": "Design Authority",
            "source_id": "preview-unchanged",
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


def _persist_manifest(workspace_path: Path):
    return persist_unreal_project_structure_manifest(
        FIXTURE_PROJECT,
        workspace_path=workspace_path,
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_valid_persisted_manifest_appears_in_list_and_detail_read_model():
    workspace_path = _workspace_dir("unreal_manifest_read_valid")
    persisted = _persist_manifest(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path

    index = list_game_adapter_unreal_project_structure_manifests(workspace_path)
    detail = get_game_adapter_unreal_project_structure_manifest(workspace_path, artifact_path.name)

    assert persisted.artifact_path == UNREAL_PROJECT_STRUCTURE_MANIFEST_RELATIVE_PATH.as_posix()
    assert index["artifact_type"] == "game_adapter_unreal_project_structure_manifest_index"
    assert index["adapter"] == "game"
    assert index["domain"] == "game_adapter"
    assert index["artifact_count"] == 1
    assert index["artifacts"] == [detail]
    assert detail["read_model_type"] == "game_adapter_unreal_project_structure_manifest_read_model"
    assert detail["artifact_type"] == ARTIFACT_TYPE
    assert detail["contract_name"] == "DGCEGameAdapterUnrealProjectStructureManifest"
    assert detail["contract_version"] == "dgce.game_adapter.unreal_project_structure_manifest.v1"
    assert detail["project_root_reference"] == FIXTURE_PROJECT.as_posix()
    assert detail["artifact_fingerprint"] == persisted.manifest_artifact["artifact_fingerprint"]
    assert detail["structural_summary"] == persisted.manifest_artifact["structural_summary"]
    assert detail["discovered_paths"] == persisted.manifest_artifact["discovered_paths"]


def test_manifest_detail_read_verifies_artifact_fingerprint_fail_closed():
    workspace_path = _workspace_dir("unreal_manifest_read_invalid_fingerprint")
    persisted = _persist_manifest(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    payload = _read_json(artifact_path)
    payload["project_root_reference"] = "tampered"
    _write_json(artifact_path, payload)

    detail = get_game_adapter_unreal_project_structure_manifest(workspace_path, artifact_path.name)

    assert detail["artifact_type"] == "game_adapter_unreal_project_structure_manifest_read_error"
    assert detail["reason_code"] == "artifact_fingerprint_invalid"
    assert detail["artifact_fingerprint"] == persisted.manifest_artifact["artifact_fingerprint"]
    assert detail["structural_summary"] is None
    assert detail["discovered_paths"] is None


def test_malformed_and_missing_fingerprint_manifest_artifacts_fail_closed():
    workspace_path = _workspace_dir("unreal_manifest_read_malformed")
    persisted = _persist_manifest(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    malformed_path = workspace_path / ".dce" / "plans" / "unreal-project-structure-broken.manifest.json"
    malformed_path.write_text("{not valid json", encoding="utf-8")
    payload = _read_json(artifact_path)
    del payload["artifact_fingerprint"]
    _write_json(artifact_path, payload)

    malformed = get_game_adapter_unreal_project_structure_manifest(workspace_path, malformed_path.name)
    missing_fingerprint = get_game_adapter_unreal_project_structure_manifest(workspace_path, artifact_path.name)

    assert malformed["artifact_type"] == "game_adapter_unreal_project_structure_manifest_read_error"
    assert malformed["reason_code"] == "artifact_malformed"
    assert missing_fingerprint["artifact_type"] == "game_adapter_unreal_project_structure_manifest_read_error"
    assert missing_fingerprint["reason_code"] == "artifact_fingerprint_missing"


def test_contract_invalid_manifest_fails_closed_after_valid_fingerprint_check():
    workspace_path = _workspace_dir("unreal_manifest_read_contract_invalid")
    persisted = _persist_manifest(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    payload = _read_json(artifact_path)
    payload["structural_summary"]["cpp_header_count"] = 99
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    _write_json(artifact_path, payload)

    detail = get_game_adapter_unreal_project_structure_manifest(workspace_path, artifact_path.name)

    assert detail["artifact_type"] == "game_adapter_unreal_project_structure_manifest_read_error"
    assert detail["reason_code"] == "contract_invalid"
    assert detail["artifact_fingerprint"] == payload["artifact_fingerprint"]


def test_manifest_read_model_exposes_no_raw_file_contents():
    workspace_path = _workspace_dir("unreal_manifest_read_no_contents")
    persisted = _persist_manifest(workspace_path)

    detail = get_game_adapter_unreal_project_structure_manifest(
        workspace_path,
        Path(persisted.artifact_path).name,
    )
    serialized = json.dumps(detail, sort_keys=True)

    assert "fixture header content" not in serialized
    assert "fixture source content" not in serialized
    assert "fixture blueprint asset content" not in serialized
    assert "fixture config content" not in serialized


def test_manifest_http_routes_are_get_only_and_read_exact_payloads():
    workspace_path = _workspace_dir("unreal_manifest_read_http")
    persisted = _persist_manifest(workspace_path)
    artifact_path = workspace_path / persisted.artifact_path
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    index_response = client.get(
        "/v1/dgce/game-adapter/unreal-project-structure-manifests",
        params={"workspace_path": str(workspace_path)},
    )
    detail_response = client.get(
        f"/v1/dgce/game-adapter/unreal-project-structure-manifests/{artifact_path.name}",
        params={"workspace_path": str(workspace_path)},
    )
    post_response = client.post(
        "/v1/dgce/game-adapter/unreal-project-structure-manifests",
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
        if route.path.startswith("/v1/dgce/game-adapter/unreal-project-structure-manifests")
    }
    assert route_methods == {
        "/v1/dgce/game-adapter/unreal-project-structure-manifests": {"GET"},
        "/v1/dgce/game-adapter/unreal-project-structure-manifests/{artifact_name}": {"GET"},
    }


def test_manifest_sdk_helpers_are_read_only(monkeypatch):
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

    assert client.list_game_adapter_unreal_project_structure_manifests("workspace-root") == {"ok": True}
    assert client.get_game_adapter_unreal_project_structure_manifest(
        "workspace-root",
        "unreal-project-structure.manifest.json",
    ) == {"ok": True}

    assert calls == [
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/unreal-project-structure-manifests?workspace_path=workspace-root",
        ),
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/unreal-project-structure-manifests/unreal-project-structure.manifest.json?workspace_path=workspace-root",
        ),
    ]


def test_preview_dispatch_remains_unchanged_until_manifest_is_explicitly_called():
    workspace_path = _workspace_dir("unreal_manifest_preview_unchanged")
    persisted = persist_stage0_input(workspace_path, _stage0_input())

    result = build_game_adapter_stage2_preview_from_released_stage0(
        workspace_path / persisted.artifact_path,
        workspace_path=workspace_path,
    )

    assert result.preview_artifact["artifact_type"] == "game_adapter_stage2_preview"
    assert not (workspace_path / ".dce" / "plans" / "unreal-project-structure.manifest.json").exists()
    assert list_game_adapter_unreal_project_structure_manifests(workspace_path)["artifact_count"] == 0


def test_manifest_reads_create_no_execution_or_output_artifacts():
    workspace_path = _workspace_dir("unreal_manifest_read_no_execution")
    persisted = _persist_manifest(workspace_path)

    list_game_adapter_unreal_project_structure_manifests(workspace_path)
    get_game_adapter_unreal_project_structure_manifest(workspace_path, Path(persisted.artifact_path).name)

    assert not (workspace_path / ".dce" / "execution").exists()
    assert not (workspace_path / ".dce" / "output").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()


def test_manifest_read_surface_keeps_stage75_lifecycle_order_locked():
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
