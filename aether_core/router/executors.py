"""Stub router executors for Aether Phase 1.5."""

import http.client
import json
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib import error, request

from aether_core import config
from aether_core.enums import ArtifactStatus


@dataclass
class ExecutionResult:
    """Result returned by a stub executor."""

    output: str
    status: ArtifactStatus
    executor: str
    metadata: dict[str, Any] = field(default_factory=dict)


class StubExecutors:
    """Placeholder executors for future model-backed execution."""

    def run(self, executor_name: str, content: str) -> ExecutionResult:
        """Use Ollama for opt-in planning calls, otherwise return stub output."""
        if self._should_use_ollama(executor_name):
            ollama_result = self._run_ollama(executor_name, content)
            if ollama_result is not None:
                return ollama_result

        return ExecutionResult(
            output=f"[{executor_name}] Placeholder output for: {content}",
            status=ArtifactStatus.EXPERIMENTAL,
            executor=executor_name,
            metadata=self._metadata(content, real_model_called=False, model_backend="stub"),
        )

    def _should_use_ollama(self, executor_name: str) -> bool:
        """Allow Ollama only for the planning bucket executor."""
        return executor_name in {"MID_MODEL", "LARGE_MODEL"} and config.OLLAMA_ENABLED is True

    def _run_ollama(self, executor_name: str, content: str) -> Optional[ExecutionResult]:
        """Attempt a single non-streaming Ollama generation and fall back on failure."""
        payload = json.dumps(
            {
                "model": config.OLLAMA_MODEL,
                "prompt": content,
                "stream": False,
            }
        ).encode("utf-8")
        ollama_request = request.Request(
            url=f"{config.OLLAMA_BASE_URL.rstrip('/')}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(ollama_request, timeout=config.OLLAMA_TIMEOUT) as response:
                response_data = json.loads(response.read().decode("utf-8"))
        except (
            error.URLError,
            TimeoutError,
            ValueError,
            json.JSONDecodeError,
            http.client.HTTPException,
        ):
            return None

        if not isinstance(response_data, dict):
            return None

        output = response_data.get("response")
        if not isinstance(output, str) or not output.strip():
            return None

        return ExecutionResult(
            output=output,
            status=ArtifactStatus.EXPERIMENTAL,
            executor=executor_name,
            metadata=self._metadata(content, real_model_called=True, model_backend="ollama"),
        )

    def _metadata(
        self,
        content: str,
        real_model_called: bool,
        model_backend: str,
    ) -> dict[str, Any]:
        """Build execution metadata without affecting routing or reuse."""
        estimated_tokens = len(content) / 4
        return {
            "real_model_called": real_model_called,
            "model_backend": model_backend,
            "model_name": config.OLLAMA_MODEL if real_model_called else None,
            "estimated_tokens": estimated_tokens,
            "estimated_cost": estimated_tokens * 0.000002,
            "inference_avoided": False,
            "backend_used": model_backend,
            "worth_running": True,
        }
