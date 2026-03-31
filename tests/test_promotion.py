import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether_core.enums import ArtifactStatus


class TestPromotionFlow:
    def _paths(self, name: str) -> tuple[Path, Path, Path]:
        base = Path("tests/.tmp")
        base.mkdir(parents=True, exist_ok=True)
        return (
            base / f"{name}_telemetry.jsonl",
            base / f"{name}_cache.json",
            base / f"{name}_artifacts.jsonl",
        )

    def _clean_paths(self, *paths: Path) -> None:
        for path in paths:
            if path.exists():
                path.unlink()

    def test_promote_changes_status_to_approved(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_status")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        execute = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-status-001"},
        )
        assert execute.status_code == 200
        assert execute.json()["status"] == ArtifactStatus.EXPERIMENTAL.value

        promote = client.post("/v1/promote/promote-status-001")

        assert promote.status_code == 200
        assert promote.json()["status"] == ArtifactStatus.APPROVED.value

    def test_promote_seeds_cache_and_next_request_reuses(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_seed")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-seed-001"},
        )
        assert first.status_code == 200
        assert first.json()["route"]["reused"] is False

        promote = client.post("/v1/promote/promote-seed-001")
        assert promote.status_code == 200

        second = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-seed-002"},
        )

        assert second.status_code == 200
        assert second.json()["route"]["decision"] == "REUSE"
        assert second.json()["route"]["reused"] is True
        assert second.json()["status"] == ArtifactStatus.APPROVED.value

    def test_promote_unknown_artifact_returns_404(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_missing")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        promote = client.post("/v1/promote/does-not-exist")

        assert promote.status_code == 404

    def test_promote_a_does_not_affect_b(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_isolation")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-a-001"},
        )
        assert first.status_code == 200

        promote = client.post("/v1/promote/promote-a-001")
        assert promote.status_code == 200

        other = client.post(
            "/v1/execute",
            json={"content": "plan the roadmap", "request_id": "promote-b-001"},
        )

        assert other.status_code == 200
        assert other.json()["route"]["reused"] is False
        assert other.json()["status"] == ArtifactStatus.EXPERIMENTAL.value

    def test_execute_promote_execute_again_hits_reuse(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_e2e")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-e2e-001"},
        )
        assert first.status_code == 200
        assert first.json()["status"] == ArtifactStatus.EXPERIMENTAL.value
        assert first.json()["route"]["reused"] is False

        promote = client.post("/v1/promote/promote-e2e-001")
        assert promote.status_code == 200

        second = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promote-e2e-002"},
        )
        assert second.status_code == 200
        assert second.json()["route"]["decision"] == "REUSE"
        assert second.json()["route"]["reused"] is True
        assert second.json()["output"] == first.json()["output"]

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        promotion_event = next(
            (
                event
                for event in events
                if event["request_id"] == "promote-e2e-001"
                and event["event_type"] == "promotion_completed"
            ),
            None,
        )
        assert promotion_event is not None

    def test_promotion_reuse_works_under_strict_scope(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_strict_scope")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "promote-strict-001",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
                "reuse_scope": "strict",
            },
        )
        assert first.status_code == 200

        promote = client.post("/v1/promote/promote-strict-001")
        assert promote.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "promote-strict-002",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
                "reuse_scope": "strict",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is True

    def test_promotion_reuse_works_under_project_scope(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("promotion_project_scope")
        self._clean_paths(telemetry_path, cache_path, artifact_path)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "promote-project-001",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
                "reuse_scope": "project",
            },
        )
        assert first.status_code == 200

        promote = client.post("/v1/promote/promote-project-001")
        assert promote.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "promote-project-002",
                "project": "Aether",
                "task_type": "design",
                "priority": "low",
                "user": "alex",
                "reuse_scope": "project",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is True
