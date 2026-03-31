import json
from urllib.error import URLError

from aether_core import config
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import StubExecutors


class _MockHTTPResponse:
    def __init__(self, body: dict):
        self._body = json.dumps(body).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestOllamaExecutor:
    def test_disabled_config_uses_stub_path(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

        result = StubExecutors().run("MID_MODEL", "plan the architecture")

        assert result.status == ArtifactStatus.EXPERIMENTAL
        assert result.output == "[MID_MODEL] Placeholder output for: plan the architecture"
        assert result.metadata == {
            "real_model_called": False,
            "model_backend": "stub",
            "model_name": None,
            "estimated_tokens": len("plan the architecture") / 4,
            "estimated_cost": (len("plan the architecture") / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "stub",
            "worth_running": True,
        }

    def test_enabled_config_and_success_uses_ollama(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", True)

        captured = {}

        def fake_urlopen(req, timeout):
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            captured["payload"] = json.loads(req.data.decode("utf-8"))
            return _MockHTTPResponse({"response": "Ollama planning output"})

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

        result = StubExecutors().run("MID_MODEL", "plan the architecture")

        assert captured["url"] == "http://localhost:11434/api/generate"
        assert captured["timeout"] == config.OLLAMA_TIMEOUT
        assert captured["payload"] == {
            "model": config.OLLAMA_MODEL,
            "prompt": "plan the architecture",
            "stream": False,
        }
        assert result.status == ArtifactStatus.EXPERIMENTAL
        assert result.output == "Ollama planning output"
        assert result.metadata == {
            "real_model_called": True,
            "model_backend": "ollama",
            "model_name": config.OLLAMA_MODEL,
            "estimated_tokens": len("plan the architecture") / 4,
            "estimated_cost": (len("plan the architecture") / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "ollama",
            "worth_running": True,
        }

    def test_ollama_failure_falls_back_to_stub(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", True)

        def fake_urlopen(req, timeout):
            raise URLError("connection failed")

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

        result = StubExecutors().run("MID_MODEL", "plan the architecture")

        assert result.status == ArtifactStatus.EXPERIMENTAL
        assert result.output == "[MID_MODEL] Placeholder output for: plan the architecture"
        assert result.metadata == {
            "real_model_called": False,
            "model_backend": "stub",
            "model_name": None,
            "estimated_tokens": len("plan the architecture") / 4,
            "estimated_cost": (len("plan the architecture") / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "stub",
            "worth_running": True,
        }

    def test_non_planning_bucket_uses_stub_only(self, monkeypatch):
        monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", True)

        def fail_if_called(req, timeout):
            raise AssertionError("Ollama should not be called for non-planning buckets")

        monkeypatch.setattr("urllib.request.urlopen", fail_if_called)

        result = StubExecutors().run("SMALL_MODEL", "hello world")

        assert result.status == ArtifactStatus.EXPERIMENTAL
        assert result.output == "[SMALL_MODEL] Placeholder output for: hello world"
        assert result.metadata == {
            "real_model_called": False,
            "model_backend": "stub",
            "model_name": None,
            "estimated_tokens": len("hello world") / 4,
            "estimated_cost": (len("hello world") / 4) * 0.000002,
            "inference_avoided": False,
            "backend_used": "stub",
            "worth_running": True,
        }
