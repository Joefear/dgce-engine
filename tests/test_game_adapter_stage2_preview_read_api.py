import inspect
import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.context_assembly import persist_stage0_input
from aether.dgce.decompose import compute_json_payload_fingerprint
import aether.dgce.decompose as dgce_decompose
from aether.dgce.game_adapter_preview import ARTIFACT_TYPE
from aether.dgce.game_adapter_stage2_dispatch import (
    GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH,
    build_game_adapter_stage2_preview_from_released_stage0,
)
from aether.dgce.read_api import (
    get_game_adapter_stage2_preview_artifact,
    list_game_adapter_stage2_preview_artifacts,
)
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient
import aether.dgce.read_api as dgce_read_api


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


def _metadata() -> dict:
    return {
        "project_id": "frontier-colony",
        "project_name": "Frontier Colony",
        "owner": "Design Authority",
        "source_id": "frontier-colony-stage2-read",
        "created_at": "2026-04-27T00:00:00Z",
        "updated_at": "2026-04-27T00:00:00Z",
    }


def _planned_changes() -> list[dict]:
    return [
        {
            "change_id": "change.interact-binding",
            "target": {
                "target_id": "IA_Interact",
                "target_path": "/Game/Input/IA_Interact",
                "target_kind": "InputAction",
            },
            "operation": "create",
            "domain_type": "binding",
            "strategy": "Blueprint",
            "summary": {
                "intent": "connect_existing_systems",
                "impact": "input",
                "risk": "low",
                "review_focus": "event_binding",
            },
        },
        {
            "change_id": "change.player-component",
            "target": {
                "target_id": "BP_Player.InventoryComponent",
                "target_path": "/Game/Blueprints/BP_Player",
                "target_kind": "ActorComponent",
            },
            "operation": "modify",
            "domain_type": "component",
            "strategy": "Blueprint",
            "summary": {
                "intent": "add_gameplay_capability",
                "impact": "gameplay",
                "risk": "medium",
                "review_focus": "component_setup",
            },
        },
    ]


def _game_adapter_stage0_input() -> dict:
    return {
        "contract_name": "GCEIngestionCore",
        "contract_version": "gce.ingestion.core.v1",
        "input_path": "structured_intent",
        "metadata": _metadata(),
        "intent": {
            "session_objective": "Generate a bounded Game Adapter Stage 2 preview.",
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
                            "planned_changes": _planned_changes(),
                        },
                    },
                },
            ],
        },
        "ambiguities": [],
    }


def _persist_preview(project_root: Path) -> Path:
    persisted = persist_stage0_input(project_root, _game_adapter_stage0_input())
    build_game_adapter_stage2_preview_from_released_stage0(
        project_root / persisted.artifact_path,
        workspace_path=project_root,
    )
    return project_root / GAME_ADAPTER_STAGE2_PREVIEW_RELATIVE_PATH


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_valid_game_adapter_preview_appears_in_list_and_detail_read_model():
    project_root = _workspace_dir("game_adapter_stage2_read_valid")
    preview_path = _persist_preview(project_root)
    artifact_name = preview_path.name
    payload = _read_json(preview_path)

    index = list_game_adapter_stage2_preview_artifacts(project_root)
    detail = get_game_adapter_stage2_preview_artifact(project_root, artifact_name)

    assert index["artifact_type"] == "game_adapter_stage2_preview_index"
    assert index["adapter"] == "game"
    assert index["domain"] == "game_adapter"
    assert index["artifact_count"] == 1
    assert index["artifacts"] == [detail]
    assert detail["read_model_type"] == "game_adapter_stage2_preview_read_model"
    assert detail["artifact_type"] == ARTIFACT_TYPE
    assert detail["contract_name"] == "DGCEGameAdapterStage2Preview"
    assert detail["contract_version"] == "dgce.game_adapter.stage2.preview.v1"
    assert detail["source_stage0_fingerprint"] == payload["source_stage0_fingerprint"]
    assert detail["source_input_reference"] == "structured_intent"
    assert detail["artifact_fingerprint"] == payload["artifact_fingerprint"]
    assert detail["planned_changes_summary"] == {
        "change_count": 2,
        "operations": {"create": 1, "modify": 1},
        "domain_types": {"binding": 1, "component": 1},
        "strategies": {"Blueprint": 2},
    }
    assert detail["governance_context_summary"] == {
        "policy_pack": "game_adapter_stage2_preview",
        "guardrail_required": True,
    }
    assert detail["machine_view"] == payload["machine_view"]
    assert detail["human_view"] == payload["human_view"]


def test_game_adapter_preview_detail_read_verifies_artifact_fingerprint_fail_closed():
    project_root = _workspace_dir("game_adapter_stage2_read_invalid_fingerprint")
    preview_path = _persist_preview(project_root)
    payload = _read_json(preview_path)
    payload["adapter"] = "software"
    _write_json(preview_path, payload)

    detail = get_game_adapter_stage2_preview_artifact(project_root, preview_path.name)

    assert detail["artifact_type"] == "game_adapter_stage2_preview_read_error"
    assert detail["reason_code"] == "artifact_fingerprint_invalid"
    assert detail["artifact_fingerprint"] == payload["artifact_fingerprint"]
    assert detail["machine_view"] is None
    assert detail["human_view"] is None


def test_malformed_game_adapter_preview_fails_closed():
    project_root = _workspace_dir("game_adapter_stage2_read_malformed")
    artifact_path = project_root / ".dce" / "plans" / "game-adapter-stage2-broken.preview.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("{not valid json", encoding="utf-8")

    detail = get_game_adapter_stage2_preview_artifact(project_root, artifact_path.name)

    assert detail["artifact_type"] == "game_adapter_stage2_preview_read_error"
    assert detail["reason_code"] == "artifact_malformed"
    assert detail["artifact_fingerprint"] is None


def test_missing_and_missing_fingerprint_preview_artifacts_fail_closed():
    project_root = _workspace_dir("game_adapter_stage2_read_missing_fingerprint")
    preview_path = _persist_preview(project_root)
    payload = _read_json(preview_path)
    del payload["artifact_fingerprint"]
    _write_json(preview_path, payload)

    missing = get_game_adapter_stage2_preview_artifact(project_root, "game-adapter-stage2-missing.preview.json")
    missing_fingerprint = get_game_adapter_stage2_preview_artifact(project_root, preview_path.name)

    assert missing["artifact_type"] == "game_adapter_stage2_preview_read_error"
    assert missing["reason_code"] == "artifact_missing"
    assert missing_fingerprint["artifact_type"] == "game_adapter_stage2_preview_read_error"
    assert missing_fingerprint["reason_code"] == "artifact_fingerprint_missing"


def test_contract_invalid_game_adapter_preview_fails_closed_after_valid_fingerprint_check():
    project_root = _workspace_dir("game_adapter_stage2_read_contract_invalid")
    preview_path = _persist_preview(project_root)
    payload = _read_json(preview_path)
    payload["planned_changes"][0]["operation"] = "rename"
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    _write_json(preview_path, payload)

    detail = get_game_adapter_stage2_preview_artifact(project_root, preview_path.name)

    assert detail["artifact_type"] == "game_adapter_stage2_preview_read_error"
    assert detail["reason_code"] == "contract_invalid"
    assert detail["artifact_fingerprint"] == payload["artifact_fingerprint"]


def test_game_adapter_preview_http_routes_are_get_only_and_read_exact_payloads():
    project_root = _workspace_dir("game_adapter_stage2_read_http")
    preview_path = _persist_preview(project_root)
    before = preview_path.read_bytes()
    client = TestClient(create_app())

    index_response = client.get(
        "/v1/dgce/game-adapter/stage2-preview-artifacts",
        params={"workspace_path": str(project_root)},
    )
    detail_response = client.get(
        f"/v1/dgce/game-adapter/stage2-preview-artifacts/{preview_path.name}",
        params={"workspace_path": str(project_root)},
    )
    post_response = client.post(
        "/v1/dgce/game-adapter/stage2-preview-artifacts",
        params={"workspace_path": str(project_root)},
    )

    assert index_response.status_code == 200
    assert detail_response.status_code == 200
    assert post_response.status_code == 405
    assert index_response.json()["artifacts"] == [detail_response.json()]
    assert preview_path.read_bytes() == before
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/game-adapter/stage2-preview-artifacts")
    }
    assert route_methods == {
        "/v1/dgce/game-adapter/stage2-preview-artifacts": {"GET"},
        "/v1/dgce/game-adapter/stage2-preview-artifacts/{artifact_name}": {"GET"},
    }


def test_game_adapter_preview_sdk_helpers_are_get_only(monkeypatch):
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

    assert client.list_game_adapter_stage2_preview_artifacts("workspace-root") == {"ok": True}
    assert client.get_game_adapter_stage2_preview_artifact(
        "workspace-root",
        "game-adapter-stage2.preview.json",
    ) == {"ok": True}

    assert calls == [
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/stage2-preview-artifacts?workspace_path=workspace-root",
        ),
        (
            "GET",
            "http://example.test/v1/dgce/game-adapter/stage2-preview-artifacts/game-adapter-stage2.preview.json?workspace_path=workspace-root",
        ),
    ]


def test_game_adapter_preview_reads_do_not_create_execution_or_output_artifacts():
    project_root = _workspace_dir("game_adapter_stage2_read_no_execution")
    _persist_preview(project_root)

    list_game_adapter_stage2_preview_artifacts(project_root)
    get_game_adapter_stage2_preview_artifact(project_root, "game-adapter-stage2.preview.json")

    assert not (project_root / ".dce" / "execution").exists()
    assert not (project_root / ".dce" / "output").exists()
    assert not (project_root / ".dce" / "outputs").exists()


def test_game_adapter_preview_read_api_does_not_confuse_software_preview_artifacts():
    project_root = _workspace_dir("game_adapter_stage2_read_software_unchanged")
    software_preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    _write_json(
        software_preview_path,
        {
            "artifact_type": "incremental_preview",
            "section_id": "mission-board",
            "artifact_fingerprint": "software-preview",
        },
    )

    assert list_game_adapter_stage2_preview_artifacts(project_root) == {
        "artifact_type": "game_adapter_stage2_preview_index",
        "adapter": "game",
        "domain": "game_adapter",
        "contract_name": "DGCEGameAdapterStage2PreviewReadModel",
        "contract_version": "dgce.game_adapter.stage2.preview.read_model.v1",
        "artifact_count": 0,
        "artifacts": [],
    }
    assert get_game_adapter_stage2_preview_artifact(project_root, software_preview_path.name)["reason_code"] == (
        "artifact_name_invalid"
    )


def test_game_adapter_preview_read_api_does_not_change_stage75_lifecycle_order():
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


def test_game_adapter_preview_read_api_introduces_no_code_graph_dependency():
    source = inspect.getsource(dgce_read_api).lower()

    assert "code_graph" not in source
    assert "dcg" not in source
