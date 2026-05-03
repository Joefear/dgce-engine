import json
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.read_api import get_stage7_alignment_read_model
from aether.dgce.read_api_http import router as dgce_read_router
from aether.dgce.sdk import DGCEClient
from packages.dgce_contracts.alignment_artifacts import (
    load_alignment_record_read_model_v1,
    persist_alignment_record_v1,
)
from packages.dgce_contracts.alignment_builder import build_alignment_record_v1


TIMESTAMP = "2026-05-02T21:00:00Z"
INPUT_FP = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
APPROVAL_FP = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
PREVIEW_FP = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
READ_MODEL_FIELDS = {
    "section_id",
    "alignment_id",
    "alignment_result",
    "drift_detected",
    "execution_permitted",
    "blocking_issues_count",
    "informational_issues_count",
    "primary_reason",
    "drift_codes",
    "evidence_sources",
    "enrichment_status",
    "code_graph_used",
    "resolver_used",
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


def _target(target: str, *, reference: str | None = None, structure: dict | None = None) -> dict:
    payload = {
        "target": target,
        "reference": reference or f"artifact://{target}",
    }
    if structure is not None:
        payload["structure"] = structure
    return payload


def _alignment_record(**kwargs) -> dict:
    defaults = {
        "alignment_id": "alignment.read-api.test.001",
        "timestamp": TIMESTAMP,
        "input_fingerprint": INPUT_FP,
        "approval_fingerprint": APPROVAL_FP,
        "preview_fingerprint": PREVIEW_FP,
        "approved_design_expectations": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "preview_proposed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
        "current_observed_targets": [
            _target("api/mission.py", structure={"kind": "api", "version": 1}),
            _target("models/mission.py", structure={"kind": "model", "version": 1}),
        ],
    }
    defaults.update(kwargs)
    return build_alignment_record_v1(**defaults)


def _persist_alignment(workspace_path: Path, *, section_id: str = "mission-board", record: dict | None = None) -> dict:
    persist_alignment_record_v1(record or _alignment_record(), workspace_path=workspace_path, section_id=section_id)
    return load_alignment_record_read_model_v1(workspace_path, section_id)


def test_stage7_alignment_api_returns_compact_read_model_for_valid_artifact():
    workspace_path = _workspace_dir("stage7_alignment_read_api_valid")
    expected = _persist_alignment(workspace_path)
    artifact_path = workspace_path / ".dce" / "execution" / "alignment" / "mission-board.alignment.json"
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 200
    assert response.json() == expected
    assert set(response.json()) == READ_MODEL_FIELDS
    assert get_stage7_alignment_read_model(workspace_path, "mission-board") == expected
    assert artifact_path.read_bytes() == before


def test_stage7_alignment_api_returns_safe_not_found_for_missing_artifact():
    workspace_path = _workspace_dir("stage7_alignment_read_api_missing")
    (workspace_path / ".dce").mkdir(parents=True)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 200
    assert response.json() == {
        "read_model_type": "stage7_alignment_read_error",
        "artifact_type": "stage7_alignment_read_error",
        "section_id": "mission-board",
        "artifact_path": ".dce/execution/alignment/mission-board.alignment.json",
        "reason_code": "artifact_missing",
    }
    assert sorted(path.relative_to(workspace_path).as_posix() for path in workspace_path.rglob("*")) == [".dce"]


def test_stage7_alignment_api_does_not_create_dce_when_workspace_has_no_dce():
    workspace_path = _workspace_dir("stage7_alignment_read_api_missing_dce")
    workspace_path.mkdir(parents=True)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": str(workspace_path)},
    )

    assert response.status_code == 400
    assert not (workspace_path / ".dce").exists()


def test_stage7_alignment_sdk_helper_returns_same_projection_as_api_read_model(monkeypatch):
    workspace_path = _workspace_dir("stage7_alignment_read_sdk")
    expected = _persist_alignment(workspace_path)
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

    assert client.get_stage7_alignment_read_model(workspace_path, "mission-board") == expected
    query = urlencode({"workspace_path": str(workspace_path)})
    assert calls == [
        (
            "GET",
            f"http://example.test/v1/dgce/stage7/alignment/mission-board?{query}",
        )
    ]


def test_stage7_alignment_response_excludes_raw_record_fields():
    workspace_path = _workspace_dir("stage7_alignment_read_api_excludes_raw")
    _persist_alignment(workspace_path)
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": str(workspace_path)},
    )
    serialized = json.dumps(response.json(), sort_keys=True)

    assert response.status_code == 200
    assert set(response.json()) == READ_MODEL_FIELDS
    for forbidden_key in (
        "input_fingerprint",
        "approval_fingerprint",
        "preview_fingerprint",
        "timestamp",
        "drift_items",
        "evidence",
    ):
        assert forbidden_key not in response.json()
    for forbidden in (
        "artifact://api/mission.py",
        "artifact://models/mission.py",
    ):
        assert forbidden not in serialized


def test_stage7_alignment_reads_create_no_lifecycle_execution_or_stage8_artifacts():
    workspace_path = _workspace_dir("stage7_alignment_read_api_no_execution")
    _persist_alignment(workspace_path)
    client = TestClient(create_app())

    client.get(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": str(workspace_path)},
    )
    load_alignment_record_read_model_v1(workspace_path, "mission-board")

    assert (workspace_path / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").exists()
    assert not (workspace_path / ".dce" / "execution" / "mission-board.execution.json").exists()
    assert not (workspace_path / ".dce" / "execution" / "gate").exists()
    assert not (workspace_path / ".dce" / "execution" / "simulation").exists()
    assert not (workspace_path / ".dce" / "execution" / "stage8").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()
    assert not (workspace_path / ".dce" / "output").exists()


def test_stage7_alignment_read_route_is_get_only():
    client = TestClient(create_app())

    response = client.post(
        "/v1/dgce/stage7/alignment/mission-board",
        params={"workspace_path": "workspace-root"},
    )
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/stage7/alignment")
    }

    assert response.status_code == 405
    assert route_methods == {"/v1/dgce/stage7/alignment/{section_id}": {"GET"}}
