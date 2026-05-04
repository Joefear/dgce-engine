import inspect
import json
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.read_api import get_game_adapter_stage3_review_bundle_read_model
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient
import aether.dgce.decompose as dgce_decompose
from packages.dgce_contracts.game_adapter_stage3_review_bundle_artifacts import (
    load_stage3_review_bundle_read_model_v1,
    persist_stage3_review_bundle_v1,
)
from packages.dgce_contracts.game_adapter_stage3_review_bundle_builder import build_stage3_review_bundle_v1


TIMESTAMP = "2026-05-03T15:00:00Z"
PREVIEW_FP = "1111111111111111111111111111111111111111111111111111111111111111"
INPUT_FP = "2222222222222222222222222222222222222222222222222222222222222222"
READ_MODEL_FIELDS = {
    "section_id",
    "review_id",
    "review_status",
    "ready_for_approval",
    "blocking_review_issues_count",
    "informational_review_issues_count",
    "proposed_change_count",
    "proposed_change_targets",
    "proposed_change_operations",
    "output_strategies",
    "review_risk_summary",
    "operator_question_count",
    "evidence_sources",
    "forbidden_runtime_actions",
}


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
    change_id: str = "change.player-inventory-component",
    target_path: str = "Content/Blueprints/BP_Player.uasset",
    target_kind: str = "BlueprintClass",
    operation: str = "modify",
    strategy: str = "Blueprint",
    risk: str = "medium",
) -> dict:
    return {
        "change_id": change_id,
        "target": {
            "target_id": change_id,
            "target_path": target_path,
            "target_kind": target_kind,
        },
        "operation": operation,
        "strategy": strategy,
        "summary": {
            "intent": "add_gameplay_capability",
            "impact": "gameplay",
            "risk": risk,
            "review_focus": "component_setup",
        },
    }


def _ready_bundle(**kwargs) -> dict:
    defaults = {
        "review_id": "review:mission-board:read-api",
        "section_id": "mission-board",
        "created_at": TIMESTAMP,
        "source_preview_fingerprint": PREVIEW_FP,
        "source_input_fingerprint": INPUT_FP,
        "planned_changes": [_planned_change()],
        "evidence": [
            {"source": "preview", "reference": ".dce/plans/game-adapter-stage2.preview.json"},
            {"source": "resolver", "reference": ".dce/plans/unreal-symbol-resolver.resolution.json#BP_Player"},
        ],
    }
    defaults.update(kwargs)
    return build_stage3_review_bundle_v1(**defaults)


def _blocked_bundle() -> dict:
    return build_stage3_review_bundle_v1(
        review_id="review:mission-board:blocked-read-api",
        section_id="mission-board",
        created_at=TIMESTAMP,
        source_preview_fingerprint=PREVIEW_FP,
        source_input_fingerprint=None,
        planned_changes=[
            {
                "change_id": "change.missing-target",
                "operation": "modify",
                "strategy": "Blueprint",
            }
        ],
        operator_questions=["Which Unreal asset should receive the requested gameplay change?"],
        evidence=[{"source": "operator_context", "reference": "operator_context:mission-board-question"}],
    )


def _persist_review(workspace_path: Path, *, bundle: dict | None = None, section_id: str = "mission-board") -> dict:
    persist_stage3_review_bundle_v1(bundle or _ready_bundle(), workspace_path=workspace_path, section_id=section_id)
    return load_stage3_review_bundle_read_model_v1(workspace_path, section_id)


def test_stage3_review_bundle_api_returns_compact_read_model_for_valid_artifact():
    workspace_path = _workspace_dir("stage3_review_read_api_valid")
    expected = _persist_review(workspace_path)
    artifact_path = workspace_path / ".dce" / "review" / "mission-board.stage3_review.json"
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 200
    assert response.json() == expected
    assert set(response.json()) == READ_MODEL_FIELDS
    assert get_game_adapter_stage3_review_bundle_read_model(workspace_path, "mission-board") == expected
    assert artifact_path.read_bytes() == before


def test_stage3_review_bundle_api_returns_blocked_projection_for_blocked_artifact():
    workspace_path = _workspace_dir("stage3_review_read_api_blocked")
    expected = _persist_review(workspace_path, bundle=_blocked_bundle())
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 200
    assert response.json() == expected
    assert response.json()["review_status"] == "blocked"
    assert response.json()["ready_for_approval"] is False
    assert response.json()["operator_question_count"] == 2
    assert response.json()["evidence_sources"] == ["operator_context"]


def test_stage3_review_bundle_api_returns_safe_missing_artifact_response():
    workspace_path = _workspace_dir("stage3_review_read_api_missing")
    (workspace_path / ".dce").mkdir(parents=True)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 200
    assert response.json() == {
        "read_model_type": "game_adapter_stage3_review_bundle_read_error",
        "artifact_type": "game_adapter_stage3_review_bundle_read_error",
        "section_id": "mission-board",
        "artifact_path": ".dce/review/mission-board.stage3_review.json",
        "reason_code": "artifact_missing",
    }
    assert sorted(path.relative_to(workspace_path).as_posix() for path in workspace_path.rglob("*")) == [".dce"]


def test_stage3_review_bundle_api_does_not_create_dce_when_workspace_has_no_dce():
    workspace_path = _workspace_dir("stage3_review_read_api_missing_dce")
    workspace_path.mkdir(parents=True)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 400
    assert not (workspace_path / ".dce").exists()


def test_stage3_review_bundle_read_route_is_get_only():
    client = TestClient(create_app())

    response = client.post(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": "workspace-root"},
    )
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/game-adapter/stage3-review-bundles")
    }

    assert response.status_code == 405
    assert route_methods == {"/v1/dgce/game-adapter/stage3-review-bundles/{section_id}": {"GET"}}


def test_stage3_review_bundle_sdk_helper_returns_same_projection_as_api_read_model(monkeypatch):
    workspace_path = _workspace_dir("stage3_review_read_sdk")
    expected = _persist_review(workspace_path)
    calls: list[tuple[str, str]] = []
    client = DGCEClient("http://example.test", api_key="secret-key")

    class _Response:
        def read(self) -> bytes:
            return json.dumps(expected, sort_keys=True).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        calls.append((request.get_method(), request.full_url))
        assert request.headers["X-api-key"] == "secret-key"
        return _Response()

    monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

    assert client.get_stage3_review_bundle_read_model(workspace_path, "mission-board") == expected
    query = urlencode({"workspace_path": str(workspace_path)})
    assert calls == [
        (
            "GET",
            f"http://example.test/v1/dgce/game-adapter/stage3-review-bundles/mission-board?{query}",
        )
    ]


def test_stage3_review_bundle_response_excludes_raw_contract_fields_and_payloads():
    workspace_path = _workspace_dir("stage3_review_read_api_excludes_raw")
    _persist_review(workspace_path)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )
    payload = response.json()
    serialized = json.dumps(payload, sort_keys=True)

    assert response.status_code == 200
    assert set(payload) == READ_MODEL_FIELDS
    for forbidden_key in (
        "source_preview_fingerprint",
        "source_input_fingerprint",
        "proposed_changes",
        "evidence",
        "raw_preview",
        "full_symbol_table",
        "raw_resolver_payload",
        "raw_model_text",
        "provider_output",
    ):
        assert forbidden_key not in payload
    for forbidden in (
        PREVIEW_FP,
        INPUT_FP,
        "human_readable_summary",
        ".dce/plans/game-adapter-stage2.preview.json",
        ".dce/plans/unreal-symbol-resolver.resolution.json",
    ):
        assert forbidden not in serialized


def test_stage3_review_bundle_reads_create_no_approval_execution_stage8_or_lifecycle_artifacts():
    workspace_path = _workspace_dir("stage3_review_read_api_no_writes")
    _persist_review(workspace_path)
    client = TestClient(create_app())

    client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )
    get_game_adapter_stage3_review_bundle_read_model(workspace_path, "mission-board")

    assert (workspace_path / ".dce" / "review" / "mission-board.stage3_review.json").exists()
    assert not (workspace_path / ".dce" / "approvals").exists()
    assert not (workspace_path / ".dce" / "approval").exists()
    assert not (workspace_path / ".dce" / "execution").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()
    assert not (workspace_path / ".dce" / "output").exists()
    assert not (workspace_path / ".dce" / "lifecycle_trace.json").exists()


def test_stage3_review_bundle_read_api_introduces_no_lifecycle_wiring():
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
    assert "stage3_review_bundle" not in inspect.getsource(dgce_decompose)
