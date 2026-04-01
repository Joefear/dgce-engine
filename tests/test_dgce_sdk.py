import io
import json
from urllib.error import HTTPError

import pytest

from aether.dgce.sdk import DGCEClient


class _MockResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload, sort_keys=True).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _http_error(url: str, status: int, detail: str) -> HTTPError:
    return HTTPError(
        url=url,
        code=status,
        msg="error",
        hdrs=None,
        fp=io.BytesIO(json.dumps({"detail": detail}, sort_keys=True).encode("utf-8")),
    )


class TestDGCESDK:
    def test_sdk_methods_map_to_expected_endpoints(self, monkeypatch):
        calls: list[str] = []
        client = DGCEClient("http://example.test")

        def fake_urlopen(request, timeout):
            calls.append(request.full_url)
            return _MockResponse({"ok": True})

        monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

        workspace_path = "workspace-root"
        assert client.get_dashboard(workspace_path) == {"ok": True}
        assert client.get_workspace_index(workspace_path) == {"ok": True}
        assert client.get_lifecycle_trace(workspace_path) == {"ok": True}
        assert client.get_consumer_contract(workspace_path) == {"ok": True}
        assert client.get_export_contract(workspace_path) == {"ok": True}
        assert client.get_artifact_manifest(workspace_path) == {"ok": True}
        assert client.list_available_artifacts(workspace_path) == {"ok": True}

        assert calls == [
            "http://example.test/v1/dgce/dashboard?workspace_path=workspace-root",
            "http://example.test/v1/dgce/workspace-index?workspace_path=workspace-root",
            "http://example.test/v1/dgce/lifecycle-trace?workspace_path=workspace-root",
            "http://example.test/v1/dgce/consumer-contract?workspace_path=workspace-root",
            "http://example.test/v1/dgce/export-contract?workspace_path=workspace-root",
            "http://example.test/v1/dgce/artifact-manifest?workspace_path=workspace-root",
            "http://example.test/v1/dgce/artifact-manifest?workspace_path=workspace-root",
        ]

    def test_sdk_returns_http_json_exactly_without_transformation(self, monkeypatch):
        payload = {
            "artifact_type": "dashboard",
            "schema_version": "1.0",
            "sections": [{"section_id": "alpha"}],
        }
        client = DGCEClient("http://example.test")

        monkeypatch.setattr("aether.dgce.sdk.urlopen", lambda request, timeout: _MockResponse(payload))

        assert client.get_dashboard("workspace-root") == payload

    def test_sdk_repeated_calls_are_deterministic(self, monkeypatch):
        payload = {"artifact_type": "artifact_manifest", "artifacts": []}
        client = DGCEClient("http://example.test")

        monkeypatch.setattr("aether.dgce.sdk.urlopen", lambda request, timeout: _MockResponse(payload))

        first = client.get_artifact_manifest("workspace-root")
        second = client.get_artifact_manifest("workspace-root")

        assert first == second

    def test_sdk_maps_http_400_to_value_error(self, monkeypatch):
        client = DGCEClient("http://example.test")

        def fake_urlopen(request, timeout):
            raise _http_error(request.full_url, 400, "invalid workspace")

        monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

        with pytest.raises(ValueError, match="invalid workspace"):
            client.get_dashboard("workspace-root")

    def test_sdk_maps_http_404_to_file_not_found_error(self, monkeypatch):
        client = DGCEClient("http://example.test")

        def fake_urlopen(request, timeout):
            raise _http_error(request.full_url, 404, "missing workspace")

        monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

        with pytest.raises(FileNotFoundError, match="missing workspace"):
            client.get_dashboard("workspace-root")

    def test_sdk_maps_other_http_errors_to_runtime_error(self, monkeypatch):
        client = DGCEClient("http://example.test")

        def fake_urlopen(request, timeout):
            raise _http_error(request.full_url, 500, "server error")

        monkeypatch.setattr("aether.dgce.sdk.urlopen", fake_urlopen)

        with pytest.raises(RuntimeError, match="server error"):
            client.get_dashboard("workspace-root")
