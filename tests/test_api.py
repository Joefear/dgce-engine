import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether_core.enums import ArtifactStatus
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.router.executors import ExecutionResult


class TestAPI:
    def _paths(self, name: str) -> tuple[Path, Path, Path]:
        base = Path("tests/.tmp")
        base.mkdir(parents=True, exist_ok=True)
        return (
            base / f"{name}_telemetry.jsonl",
            base / f"{name}_cache.json",
            base / f"{name}_artifacts.jsonl",
        )

    def test_health_works(self):
        telemetry_path, cache_path, artifact_path = self._paths("health")
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

    def test_classify_returns_classification(self):
        telemetry_path, cache_path, artifact_path = self._paths("classify")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/classify",
            json={"content": "how does this work", "request_id": "api-classify-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["request_id"] == "api-classify-001"
        assert body["status"] == ArtifactStatus.EXPERIMENTAL.value

    def test_decide_returns_task_bucket_and_status(self):
        telemetry_path, cache_path, artifact_path = self._paths("decide")
        client = TestClient(create_app(
            telemetry_path=telemetry_path, cache_path=cache_path, artifact_store_path=artifact_path
        ))

        response = client.post(
            "/v1/decide",
            json={"content": "plan the architecture", "request_id": "api-decide-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["request_id"] == "api-decide-001"
        assert body["task_bucket"] == "planning"
        assert body["status"] == ArtifactStatus.APPROVED.value
        assert "classifier_confidence" in body

    def test_execute_returns_reuse_hit_when_available(self):
        telemetry_path, cache_path, artifact_path = self._paths("execute_hit")
        cache = ExactMatchCache(cache_path)
        cache.store(
            task_bucket="planning",
            content="plan the architecture",
            output="Approved reused output",
            status=ArtifactStatus.APPROVED,
            context={"reuse_scope": "strict"},
        )
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-exec-hit-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["route"]["decision"] == "REUSE"
        assert body["route"]["reused"] is True
        assert body["status"] == ArtifactStatus.APPROVED.value
        assert body["output"] == "Approved reused output"

    def test_reuse_summary_is_correct(self):
        telemetry_path, cache_path, artifact_path = self._paths("reuse_summary")
        cache = ExactMatchCache(cache_path)
        cache.store(
            task_bucket="planning",
            content="plan the architecture",
            output="Approved reused output",
            status=ArtifactStatus.APPROVED,
            context={"reuse_scope": "strict"},
        )
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "reuse-summary-001"},
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["final_decision"] == "REUSE"
        assert summary["reused"] is True
        assert summary["worth_running"] is False
        assert summary["inference_avoided"] is True
        assert summary["backend_used"] == "reuse"
        assert summary["estimated_tokens"] == 0
        assert summary["estimated_cost"] == 0
        assert summary["artifact_status"] == ArtifactStatus.APPROVED.value
        assert summary["short_reason"] == "Reused approved artifact - inference avoided"

    def test_execute_falls_back_to_stub_on_miss(self):
        telemetry_path, cache_path, artifact_path = self._paths("execute_miss")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-exec-miss-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["route"]["decision"] == "MID_MODEL"
        assert body["route"]["reused"] is False
        assert body["status"] == ArtifactStatus.EXPERIMENTAL.value

    def test_fresh_execution_summary_is_correct(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("fresh_summary")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "fresh-summary-001"},
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["final_decision"] == "MID_MODEL"
        assert summary["reused"] is False
        assert summary["worth_running"] is True
        assert summary["inference_avoided"] is False
        assert summary["backend_used"] == "stub"
        assert summary["estimated_tokens"] == len("plan the architecture") / 4
        assert summary["estimated_cost"] == (len("plan the architecture") / 4) * 0.000002
        assert summary["artifact_status"] == ArtifactStatus.EXPERIMENTAL.value
        assert summary["short_reason"] == "No reusable artifact - execution required"

    def test_execute_blocked_content_returns_blocked_status(self):
        telemetry_path, cache_path, artifact_path = self._paths("execute_blocked")
        client = TestClient(create_app(
            telemetry_path=telemetry_path, cache_path=cache_path, artifact_store_path=artifact_path
        ))

        response = client.post(
            "/v1/execute",
            json={"content": "violence and harm", "request_id": "api-exec-blocked-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == ArtifactStatus.BLOCKED.value
        assert body["output"] == ""
        assert body["route"]["reused"] is False

    def test_blocked_request_summary_is_correct(self):
        telemetry_path, cache_path, artifact_path = self._paths("blocked_summary")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "violence and harm", "request_id": "blocked-summary-001"},
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["final_decision"] == "BLOCKED"
        assert summary["reused"] is False
        assert summary["worth_running"] is False
        assert summary["inference_avoided"] is True
        assert summary["backend_used"] == "blocked"
        assert summary["estimated_tokens"] == 0
        assert summary["estimated_cost"] == 0.0
        assert summary["artifact_status"] == ArtifactStatus.BLOCKED.value
        assert summary["short_reason"] == "Blocked by policy - execution prevented"

    def test_telemetry_file_receives_events(self):
        telemetry_path, cache_path, artifact_path = self._paths("telemetry")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-telemetry-001"},
        )

        assert response.status_code == 200
        assert telemetry_path.exists()

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        event_types = [event["event_type"] for event in events if event["request_id"] == "api-telemetry-001"]
        assert "request_received" in event_types
        assert "classification_completed" in event_types
        assert "guardrail_decision" in event_types
        assert "execution_path_taken" in event_types
        assert "response_returned" in event_types

    def test_execute_telemetry_contains_execution_metadata(self, monkeypatch):
        telemetry_path, cache_path, artifact_path = self._paths("telemetry_metadata")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", True)

        class MockResponse:
            def read(self) -> bytes:
                return b'{"response": "Ollama planning output"}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout: MockResponse())

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-telemetry-meta-001"},
        )

        assert response.status_code == 200

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        execution_event = next(
            event
            for event in events
            if event["request_id"] == "api-telemetry-meta-001"
            and event["event_type"] == "execution_path_taken"
        )

        assert execution_event["data"]["real_model_called"] is True
        assert execution_event["data"]["model_backend"] == "ollama"
        assert execution_event["data"]["worth_running"] is True
        assert execution_event["data"]["inference_avoided"] is False
        assert execution_event["data"]["backend_used"] == "ollama"

    def test_reuse_telemetry_marks_inference_avoided_true(self):
        telemetry_path, cache_path, artifact_path = self._paths("reuse_telemetry_metadata")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()
        cache = ExactMatchCache(cache_path)
        cache.store(
            task_bucket="planning",
            content="plan the architecture",
            output="Approved reused output",
            status=ArtifactStatus.APPROVED,
            context={"reuse_scope": "strict"},
        )
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-reuse-meta-001"},
        )

        assert response.status_code == 200

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        execution_event = next(
            event
            for event in events
            if event["request_id"] == "api-reuse-meta-001"
            and event["event_type"] == "execution_path_taken"
        )

        assert execution_event["data"]["inference_avoided"] is True
        assert execution_event["data"]["worth_running"] is False
        assert execution_event["data"]["backend_used"] == "reuse"
        assert execution_event["data"]["estimated_tokens"] == 0
        assert execution_event["data"]["estimated_cost"] == 0

    def test_execute_telemetry_includes_execution_metadata(self):
        telemetry_path, cache_path, artifact_path = self._paths("exec_meta_telemetry")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(create_app(
            telemetry_path=telemetry_path,
            cache_path=cache_path,
            artifact_store_path=artifact_path
        ))

        response = client.post("/v1/execute", json={
            "content": "plan the architecture",
            "request_id": "meta-tel-001"
        })

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        exec_event = next(
            (e for e in events
             if e["request_id"] == "meta-tel-001"
             and e["event_type"] == "execution_path_taken"),
            None,
        )

        assert exec_event is not None
        assert "real_model_called" in exec_event["data"]
        assert "model_backend" in exec_event["data"]
        assert "model_name" in exec_event["data"]

        response_event = next(
            (e for e in events
             if e["request_id"] == "meta-tel-001"
             and e["event_type"] == "response_returned"),
            None,
        )

        assert response_event is not None
        assert response_event["data"]["summary"] == response.json()["summary"]

    def test_execute_stub_artifact_context_contains_stub_metadata(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

        telemetry_path, cache_path, artifact_path = self._paths("artifact_stub_metadata")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "api-artifact-meta-001"},
        )

        assert response.status_code == 200

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "api-artifact-meta-001"
        )
        assert stored["context"]["real_model_called"] is False
        assert stored["context"]["model_backend"] == "stub"

    def test_request_with_context_persists_end_to_end(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("structured_context")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "ctx-001",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
            },
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["project"] == "Aether"
        assert summary["task_type"] == "planning"
        assert summary["priority"] == "high"
        assert summary["reuse_scope"] == "strict"

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]
        stored = next(
            artifact for artifact in reversed(artifacts) if artifact["artifact_id"] == "ctx-001"
        )
        assert stored["context"]["project"] == "Aether"
        assert stored["context"]["task_type"] == "planning"
        assert stored["context"]["priority"] == "high"
        assert stored["context"]["user"] == "sam"

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]
        execution_event = next(
            event
            for event in events
            if event["request_id"] == "ctx-001" and event["event_type"] == "execution_path_taken"
        )
        assert execution_event["data"]["project"] == "Aether"
        assert execution_event["data"]["task_type"] == "planning"
        assert execution_event["data"]["priority"] == "high"
        assert execution_event["data"]["user"] == "sam"

    def test_request_without_context_has_unchanged_behavior(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("structured_context_none")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "ctx-none-001"},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["route"]["decision"] == "MID_MODEL"
        assert body["route"]["reused"] is False
        assert body["summary"]["project"] is None
        assert body["summary"]["task_type"] is None
        assert body["summary"]["priority"] is None
        assert body["summary"]["reuse_scope"] == "strict"

    def test_strict_scope_keeps_full_context_isolation(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("reuse_scope_strict")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "strict-001",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
                "reuse_scope": "strict",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/strict-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "strict-002",
                "project": "Aether",
                "task_type": "planning",
                "priority": "low",
                "user": "alex",
                "reuse_scope": "strict",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is False
        assert second.json()["route"]["decision"] == "MID_MODEL"

    def test_project_scope_allows_reuse_across_priority_user_and_task_type(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("reuse_scope_project")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "project-001",
                "project": "Aether",
                "task_type": "planning",
                "priority": "high",
                "user": "sam",
                "reuse_scope": "project",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/project-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "project-002",
                "project": "Aether",
                "task_type": "design",
                "priority": "low",
                "user": "alex",
                "reuse_scope": "project",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["decision"] == "REUSE"
        assert second.json()["route"]["reused"] is True
        assert second.json()["summary"]["reuse_scope"] == "project"

    def test_preset_applies_defaults_and_reuse_still_works(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("preset_reuse")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "preset-001",
                "preset": "dgce_planning",
            },
        )
        assert first.status_code == 200
        assert first.json()["summary"]["project"] == "DGCE"
        assert first.json()["summary"]["task_type"] == "planning"
        assert first.json()["summary"]["priority"] == "high"
        assert first.json()["summary"]["reuse_scope"] == "project"

        promoted = client.post("/v1/promote/preset-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "preset-002",
                "preset": "dgce_planning",
                "priority": "low",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is True
        assert second.json()["summary"]["project"] == "DGCE"
        assert second.json()["summary"]["task_type"] == "planning"
        assert second.json()["summary"]["priority"] == "low"

    def test_prompt_profile_separates_scaffolded_and_default_project_reuse(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("prompt_profile_separation")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "prompt-profile-001",
                "preset": "dgce_planning",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/prompt-profile-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "prompt-profile-002",
                "project": "DGCE",
                "reuse_scope": "project",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is False
        assert second.json()["route"]["decision"] == "MID_MODEL"

    def test_same_prompt_profile_reuses_after_promotion(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("prompt_profile_same_preset")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "prompt-profile-same-001",
                "preset": "dgce_planning",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/prompt-profile-same-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "prompt-profile-same-002",
                "preset": "dgce_planning",
                "priority": "low",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is True
        assert second.json()["route"]["decision"] == "REUSE"

    def test_no_preset_requests_use_default_prompt_profile(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("prompt_profile_default")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "prompt-default-001"},
        )

        assert response.status_code == 200
        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "prompt-default-001"
        )
        assert stored["context"]["prompt_profile"] == "default"

    def test_no_preset_reuse_behavior_remains_unchanged_with_default_profile(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("prompt_profile_default_reuse")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "prompt-default-reuse-001"},
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/prompt-default-reuse-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "prompt-default-reuse-002"},
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is True
        assert second.json()["route"]["decision"] == "REUSE"

    def test_preset_populates_request_defaults(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("preset_defaults")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "analyze the sensor pipeline",
                "request_id": "preset-defaults-001",
                "preset": "defiant_sky_analysis",
            },
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["project"] == "DefiantSky"
        assert summary["task_type"] == "sensor_fusion_analysis"
        assert summary["priority"] == "high"
        assert summary["reuse_scope"] == "project"

    def test_explicit_fields_override_preset(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("preset_override")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "analyze the sensor pipeline",
                "request_id": "preset-override-001",
                "preset": "defiant_sky_analysis",
                "priority": "low",
                "reuse_scope": "project",
            },
        )

        assert response.status_code == 200
        summary = response.json()["summary"]
        assert summary["project"] == "DefiantSky"
        assert summary["task_type"] == "sensor_fusion_analysis"
        assert summary["priority"] == "low"
        assert summary["reuse_scope"] == "project"

    def test_all_new_presets_resolve_correctly(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("preset_matrix")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        cases = [
            ("dgce_planning", "DGCE", "planning", "high", "project"),
            ("dgce_system_design", "DGCE", "game_system_design", "high", "project"),
            ("defiant_sky_analysis", "DefiantSky", "sensor_fusion_analysis", "high", "project"),
            ("defiant_sky_sniper_planning", "DefiantSky", "sniper_planning", "critical", "project"),
        ]

        for index, (preset, project, task_type, priority, reuse_scope) in enumerate(cases, start=1):
            response = client.post(
                "/v1/execute",
                json={
                    "content": "analyze the workflow",
                    "request_id": f"preset-matrix-{index:03d}",
                    "preset": preset,
                },
            )

            assert response.status_code == 200
            summary = response.json()["summary"]
            assert summary["project"] == project
            assert summary["task_type"] == task_type
            assert summary["priority"] == priority
            assert summary["reuse_scope"] == reuse_scope

    def test_preset_hints_are_incorporated_into_execution_prompt(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("preset_prompt_hints")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        captured = {}

        def fake_run(self, executor_name, content):
            captured["executor_name"] = executor_name
            captured["content"] = content
            return ExecutionResult(
                output="Scaffolded output",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "preset-prompt-001",
                "preset": "dgce_planning",
            },
        )

        assert response.status_code == 200
        assert captured["executor_name"] == "MID_MODEL"
        assert captured["content"].startswith("plan the architecture")
        assert "Execution scaffolds:" in captured["content"]
        assert "Domain hint: Defiant Game Creation Engine internal planning" in captured["content"]
        assert "Output style: Return a practical implementation slice with milestones, dependencies, and next coding steps." in captured["content"]
        assert "System hint: Prefer engine-building, pipeline design, modular implementation, and builder-oriented recommendations." in captured["content"]
        assert "You MUST return output in JSON format with the following top-level keys: systems, modules, dependencies, implementation_steps" in captured["content"]
        assert "Do not include extra commentary outside the JSON." in captured["content"]

    def test_no_preset_leaves_execution_prompt_unchanged(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("preset_prompt_none")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        captured = {}

        def fake_run(self, executor_name, content):
            captured["content"] = content
            return ExecutionResult(
                output="Plain output",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "preset-prompt-none-001"},
        )

        assert response.status_code == 200
        assert captured["content"] == "plan the architecture"

    def test_explicit_content_is_preserved_when_preset_hints_apply(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("preset_prompt_preserve")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        captured = {}

        def fake_run(self, executor_name, content):
            captured["content"] = content
            return ExecutionResult(
                output="Prompt-preserved output",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        content = "analyze the sensor pipeline"
        response = client.post(
            "/v1/execute",
            json={
                "content": content,
                "request_id": "preset-prompt-preserve-001",
                "preset": "defiant_sky_analysis",
            },
        )

        assert response.status_code == 200
        assert captured["content"].split("\n\n", 1)[0] == content
        assert captured["content"].count(content) == 1

    def test_preset_prompt_hints_remain_deterministic(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("preset_prompt_deterministic")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        captured = []

        def fake_run(self, executor_name, content):
            captured.append(content)
            return ExecutionResult(
                output="Deterministic output",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        for request_id in ("preset-prompt-det-001", "preset-prompt-det-002"):
            response = client.post(
                "/v1/execute",
                json={
                    "content": "plan the architecture",
                    "request_id": request_id,
                    "preset": "dgce_planning",
                },
            )
            assert response.status_code == 200

        assert len(captured) == 2
        assert captured[0] == captured[1]

    def test_structured_preset_valid_json_passes(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("structured_valid")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        def fake_run(self, executor_name, content):
            return ExecutionResult(
                output=json.dumps(
                    {
                        "systems": ["core"],
                        "modules": ["routing"],
                        "dependencies": ["cache"],
                        "implementation_steps": ["wire planner"],
                    }
                ),
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "structured-valid-001",
                "preset": "dgce_planning",
            },
        )

        assert response.status_code == 200

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "structured-valid-001"
        )
        assert stored["context"]["structure_valid"] is True
        assert stored["structured_content"]["systems"] == ["core"]

    def test_structured_preset_missing_keys_is_flagged_but_succeeds(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("structured_missing_keys")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        def fake_run(self, executor_name, content):
            return ExecutionResult(
                output=json.dumps(
                    {
                        "systems": ["core"],
                        "modules": ["routing"],
                    }
                ),
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "structured-missing-001",
                "preset": "dgce_planning",
            },
        )

        assert response.status_code == 200

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "structured-missing-001"
        )
        assert stored["context"]["structure_valid"] is False
        assert stored["context"]["structure_error"] == "missing_keys"
        assert stored["structured_content"] is None

    def test_structured_preset_invalid_json_is_flagged_but_succeeds(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("structured_invalid_json")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        def fake_run(self, executor_name, content):
            return ExecutionResult(
                output="not json at all",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "structured-invalid-001",
                "preset": "dgce_planning",
            },
        )

        assert response.status_code == 200

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "structured-invalid-001"
        )
        assert stored["context"]["structure_valid"] is False
        assert stored["context"]["structure_error"] == "invalid_json"
        assert stored["structured_content"] is None

    def test_structured_preset_unknown_schema_is_flagged_but_succeeds(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("structured_unknown_schema")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        monkeypatch.setattr(
            "aether_core.models.request.get_preset",
            lambda name: {
                "project": "DGCE",
                "reuse_scope": "project",
                "output_contract": {
                    "mode": "structured",
                    "schema_name": "does_not_exist",
                },
            }
            if name == "broken_structured"
            else {},
        )

        def fake_run(self, executor_name, content):
            return ExecutionResult(
                output='{"systems": ["core"]}',
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "structured-unknown-001",
                "preset": "broken_structured",
            },
        )

        assert response.status_code == 200

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact
            for artifact in reversed(artifacts)
            if artifact["artifact_id"] == "structured-unknown-001"
        )
        assert stored["context"]["structure_valid"] is False
        assert stored["context"]["structure_error"] == "unknown_schema"
        assert stored["structured_content"] is None

    def test_freeform_preset_keeps_existing_behavior(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        monkeypatch.setattr("aether_core.presets.loader._PRESET_CACHE", None)
        telemetry_path, cache_path, artifact_path = self._paths("freeform_contract")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

        captured = {}

        def fake_run(self, executor_name, content):
            captured["content"] = content
            return ExecutionResult(
                output="freeform output",
                status=ArtifactStatus.EXPERIMENTAL,
                executor=executor_name,
                metadata={
                    "real_model_called": False,
                    "model_backend": "stub",
                    "model_name": None,
                    "estimated_tokens": len(content) / 4,
                    "estimated_cost": (len(content) / 4) * 0.000002,
                    "inference_avoided": False,
                    "backend_used": "stub",
                    "worth_running": True,
                },
            )

        monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        response = client.post(
            "/v1/execute",
            json={
                "content": "design the gameplay loop",
                "request_id": "freeform-001",
                "preset": "dgce_system_design",
            },
        )

        assert response.status_code == 200
        assert "You MUST return output in JSON format" not in captured["content"]

        with open(artifact_path, "r", encoding="utf-8") as f:
            artifacts = [json.loads(line) for line in f if line.strip()]

        stored = next(
            artifact for artifact in reversed(artifacts) if artifact["artifact_id"] == "freeform-001"
        )
        assert "structure_valid" not in stored["context"]
        assert stored["structured_content"] is None

    def test_project_scope_still_isolates_different_projects(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("reuse_scope_project_isolated")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "project-iso-001",
                "project": "Aether",
                "reuse_scope": "project",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/project-iso-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "project-iso-002",
                "project": "OtherProject",
                "reuse_scope": "project",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is False
        assert second.json()["route"]["decision"] == "MID_MODEL"

    def test_project_scope_without_project_falls_back_to_strict_like_isolation(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
        telemetry_path, cache_path, artifact_path = self._paths("reuse_scope_project_no_project")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()

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
                "request_id": "project-none-001",
                "priority": "high",
                "reuse_scope": "project",
            },
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/project-none-001")
        assert promoted.status_code == 200

        second = client.post(
            "/v1/execute",
            json={
                "content": "plan the architecture",
                "request_id": "project-none-002",
                "priority": "low",
                "reuse_scope": "project",
            },
        )

        assert second.status_code == 200
        assert second.json()["route"]["reused"] is False
        assert second.json()["route"]["decision"] == "MID_MODEL"

    def test_promotion_makes_exact_request_reusable(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

        telemetry_path, cache_path, artifact_path = self._paths("promotion_reuse")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promo-run-001"},
        )

        assert first.status_code == 200
        assert first.json()["status"] == ArtifactStatus.EXPERIMENTAL.value
        assert first.json()["route"]["reused"] is False

        promoted = client.post("/v1/promote/promo-run-001")
        assert promoted.status_code == 200
        assert promoted.json()["status"] == ArtifactStatus.APPROVED.value

        second = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promo-run-002"},
        )

        assert second.status_code == 200
        assert second.json()["route"]["decision"] == "REUSE"
        assert second.json()["route"]["reused"] is True
        assert second.json()["status"] == ArtifactStatus.APPROVED.value
        assert second.json()["output"] == first.json()["output"]

    def test_promotion_does_not_affect_unrelated_requests(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

        telemetry_path, cache_path, artifact_path = self._paths("promotion_unrelated")
        for path in (telemetry_path, cache_path, artifact_path):
            if path.exists():
                path.unlink()
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promo-other-001"},
        )
        assert first.status_code == 200
        assert first.json()["route"]["reused"] is False

        promoted = client.post("/v1/promote/promo-other-001")
        assert promoted.status_code == 200

        other = client.post(
            "/v1/execute",
            json={"content": "plan the roadmap", "request_id": "promo-other-002"},
        )

        assert other.status_code == 200
        assert other.json()["route"]["reused"] is False
        assert other.json()["route"]["decision"] == "MID_MODEL"
        assert other.json()["status"] == ArtifactStatus.EXPERIMENTAL.value

    def test_promotion_emits_telemetry_event(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

        telemetry_path, cache_path, artifact_path = self._paths("promotion_telemetry")
        client = TestClient(
            create_app(
                telemetry_path=telemetry_path,
                cache_path=cache_path,
                artifact_store_path=artifact_path,
            )
        )

        first = client.post(
            "/v1/execute",
            json={"content": "plan the architecture", "request_id": "promo-tel-001"},
        )
        assert first.status_code == 200

        promoted = client.post("/v1/promote/promo-tel-001")
        assert promoted.status_code == 200

        with open(telemetry_path, "r", encoding="utf-8") as f:
            events = [json.loads(line) for line in f if line.strip()]

        promotion_event = next(
            (
                event
                for event in events
                if event["request_id"] == "promo-tel-001"
                and event["event_type"] == "promotion_completed"
            ),
            None,
        )

        assert promotion_event is not None
        assert promotion_event["data"]["artifact_id"] == "promo-tel-001"
        assert promotion_event["data"]["task_bucket"] == "planning"
