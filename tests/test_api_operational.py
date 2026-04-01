import logging
from pathlib import Path

from fastapi.testclient import TestClient

from aether.dgce.config import get_config
from apps.aether_api.main import create_app


class TestAPIOperational:
    def _paths(self, name: str) -> tuple[Path, Path, Path]:
        base = Path("tests/.tmp")
        base.mkdir(parents=True, exist_ok=True)
        return (
            base / f"{name}_telemetry.jsonl",
            base / f"{name}_cache.json",
            base / f"{name}_artifacts.jsonl",
        )

    def test_health_returns_expected_payload(self, monkeypatch):
        monkeypatch.delenv("DGCE_API_KEY", raising=False)
        telemetry_path, cache_path, artifact_path = self._paths("operational_health")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_version_returns_expected_payload(self, monkeypatch):
        monkeypatch.delenv("DGCE_API_KEY", raising=False)
        telemetry_path, cache_path, artifact_path = self._paths("operational_version")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.get("/version")

        assert response.status_code == 200
        assert response.json() == {
            "service": "aether-api",
            "dgce_version": "5.x",
            "api_version": "v1",
        }

    def test_get_config_returns_expected_values(self, monkeypatch):
        monkeypatch.delenv("DGCE_API_KEY", raising=False)
        assert get_config() == {"api_key": None}

        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        assert get_config() == {"api_key": "test-key"}

    def test_startup_does_not_crash_without_api_key(self, monkeypatch, caplog):
        monkeypatch.delenv("DGCE_API_KEY", raising=False)
        caplog.set_level(logging.INFO, logger="aether.api")
        telemetry_path, cache_path, artifact_path = self._paths("startup_no_key")

        with TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        ) as client:
            response = client.get("/health")

        assert response.status_code == 200
        startup_records = [record for record in caplog.records if record.message == "DGCE API startup"]
        assert startup_records
        assert startup_records[-1].dgce_api_key_configured is False

    def test_startup_does_not_crash_with_api_key(self, monkeypatch, caplog):
        monkeypatch.setenv("DGCE_API_KEY", "test-key")
        caplog.set_level(logging.INFO, logger="aether.api")
        telemetry_path, cache_path, artifact_path = self._paths("startup_with_key")

        with TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        ) as client:
            response = client.get("/health")

        assert response.status_code == 200
        startup_records = [record for record in caplog.records if record.message == "DGCE API startup"]
        assert startup_records
        assert startup_records[-1].dgce_api_key_configured is True

    def test_request_logging_does_not_break_requests(self, monkeypatch, caplog):
        monkeypatch.delenv("DGCE_API_KEY", raising=False)
        caplog.set_level(logging.INFO, logger="aether.api")
        telemetry_path, cache_path, artifact_path = self._paths("request_logging")

        with TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        ) as client:
            response = client.get("/version")

        assert response.status_code == 200
        request_records = [record for record in caplog.records if record.message == "Aether API request complete"]
        assert request_records
        assert request_records[-1].request_method == "GET"
        assert request_records[-1].request_path == "/version"
        assert request_records[-1].status_code == 200
