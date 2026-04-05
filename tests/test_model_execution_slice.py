import json
from pathlib import Path

import pytest

from aether.dgce import (
    DGCESection,
    SectionApprovalInput,
    SectionExecutionGateInput,
    SectionPreflightInput,
    record_section_approval,
    record_section_execution_gate,
    run_section_with_workspace,
)
from aether.dgce.execute_api import execute_prepared_section, load_section_execution_artifact
from aether.dgce.model_config import build_model_execution_metadata, get_model_execution_config
from aether.dgce.model_executor import generate_function_stub
from aether.dgce.prompt_templates import build_function_stub_prompt
from aether.dgce.providers import claude_provider
from aether.dgce.prepare_api import prepare_section_execution
from aether.dgce.model_validator import validate_function_stub
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


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


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output="Summary output",
        status=ArtifactStatus.EXPERIMENTAL,
        executor="stub",
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


def _function_stub_section() -> DGCESection:
    return DGCESection(
        section_id="function-stub",
        section_type="function_stub",
        title="Function Stub",
        description="Generate one deterministic Python function stub during governed execution.",
        requirements=["produce exactly one valid function"],
        constraints=["single file only", "no autonomous writes"],
        expected_targets=[
            {
                "path": "src/function_stub.py",
                "purpose": "Deterministic function stub",
                "source": "expected_targets",
                "name": "build_payload",
                "inputs": [{"name": "payload", "type": "dict[str, object]"}],
                "output": "dict[str, object]",
            }
        ],
    )


def _build_function_workspace(monkeypatch, name: str) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_function_stub_section(), project_root, incremental_mode="incremental_v2_2")
    return project_root


def _mark_section_ready(project_root: Path) -> None:
    record_section_approval(
        project_root,
        "function-stub",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-04-02T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "function-stub",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-04-02T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-04-02T00:00:00Z"),
    )


def test_execute_prepared_function_stub_writes_validated_output_and_model_audit(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_valid")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")

    result = execute_prepared_section(project_root, "function-stub")

    assert result["status"] == "ok"
    assert (project_root / "src/function_stub.py").read_text(encoding="utf-8") == (
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
        "    return {}\n"
    )
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["model_execution"] == {
        "provider": "stub",
        "model_id": "stub-model-v1",
        "prompt_template_version": "v1",
        "temperature": 0.0,
        "postprocess": "strict_function_stub_v1",
    }


def test_generate_function_stub_uses_model_provider_boundary(monkeypatch):
    captured: dict[str, object] = {}

    def fake_generate_text(prompt: str, config: dict) -> str:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return "provider output"

    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_text", fake_generate_text)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "inputs": [{"name": "payload", "type": "dict[str, object]"}],
            "output": "dict[str, object]",
        },
        {
            "provider": "stub",
            "model_id": "stub-model-v1",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
        },
    )

    assert raw_output == "provider output"
    assert "FUNCTION_STUB_SPEC:" in str(captured["prompt"])
    assert "* template_version: v1" in str(captured["prompt"])
    assert captured["config"] == {
        "provider": "stub",
        "model_id": "stub-model-v1",
        "temperature": 0.0,
        "prompt_template_version": "v1",
        "postprocess": "strict_function_stub_v1",
    }


def test_get_model_execution_config_defaults_to_stub():
    assert get_model_execution_config()["provider"] == "stub"


def test_get_model_execution_config_rejects_unsupported_provider():
    with pytest.raises(ValueError, match="config.provider must be one of: claude, stub"):
        get_model_execution_config({"provider": "unknown"})


def test_get_model_execution_config_rejects_unsupported_prompt_template_version():
    with pytest.raises(ValueError, match="config.prompt_template_version must be one of: v1"):
        get_model_execution_config({"prompt_template_version": "v2"})


def test_get_model_execution_config_rejects_unsupported_postprocess():
    with pytest.raises(ValueError, match="config.postprocess must be one of: strict_function_stub_v1"):
        get_model_execution_config({"postprocess": "invalid"})


def test_build_function_stub_prompt_is_deterministic():
    structured_input = {
        "name": "build_payload",
        "inputs": [{"name": "payload", "type": "dict[str, object]"}],
        "output": "dict[str, object]",
    }
    assert build_function_stub_prompt(structured_input, "v1") == build_function_stub_prompt(structured_input, "v1")


def test_build_function_stub_prompt_rejects_unsupported_template_version():
    with pytest.raises(ValueError, match="template_version must be one of: v1"):
        build_function_stub_prompt(
            {
                "name": "build_payload",
                "inputs": [{"name": "payload", "type": "dict[str, object]"}],
                "output": "dict[str, object]",
            },
            "v2",
        )


def test_build_model_execution_metadata_is_audit_safe_and_bounded():
    metadata = build_model_execution_metadata(
        {
            "provider": "claude",
            "model_id": "claude-3-7-sonnet",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
            "api_key": "secret-key",
            "raw_prompt": "ignored",
        }
    )

    assert metadata == {
        "provider": "claude",
        "model_id": "claude-3-7-sonnet",
        "temperature": 0.0,
        "prompt_template_version": "v1",
        "postprocess": "strict_function_stub_v1",
    }


def test_generate_function_stub_with_claude_provider_missing_config_fails_deterministically():
    with pytest.raises(ValueError, match="Claude provider requires config.api_key"):
        generate_function_stub(
            {
                "name": "build_payload",
                "inputs": [{"name": "payload", "type": "dict[str, object]"}],
                "output": "dict[str, object]",
            },
            {
                "provider": "claude",
                "model_id": "claude-3-7-sonnet",
                "temperature": 0.0,
                "prompt_template_version": "v1",
                "postprocess": "strict_function_stub_v1",
            },
        )


def test_generate_function_stub_dispatches_to_claude_provider_boundary(monkeypatch):
    def fake_generate_text(prompt: str, config: dict) -> str:
        assert "FUNCTION_STUB_SPEC:" in prompt
        assert config["provider"] == "claude"
        return "claude output"

    monkeypatch.setattr("aether.dgce.model_provider.claude_provider.generate_text", fake_generate_text)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "inputs": [{"name": "payload", "type": "dict[str, object]"}],
            "output": "dict[str, object]",
        },
        {
            "provider": "claude",
            "model_id": "claude-3-7-sonnet",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
            "api_key": "test-key",
        },
    )

    assert raw_output == "claude output"


def test_generate_function_stub_uses_prompt_template_module(monkeypatch):
    captured: dict[str, object] = {}

    def fake_build_function_stub_prompt(structured_input: dict, template_version: str) -> str:
        captured["structured_input"] = structured_input
        captured["template_version"] = template_version
        return "built prompt"

    def fake_generate_text(prompt: str, config: dict) -> str:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return "provider output"

    monkeypatch.setattr("aether.dgce.model_executor.build_function_stub_prompt", fake_build_function_stub_prompt)
    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_text", fake_generate_text)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "inputs": [{"name": "payload", "type": "dict[str, object]"}],
            "output": "dict[str, object]",
        },
        {
            "provider": "stub",
            "model_id": "stub-model-v1",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
        },
    )

    assert raw_output == "provider output"
    assert captured["template_version"] == "v1"
    assert captured["prompt"] == "built prompt"


class _MockClaudeResponse:
    def __init__(self, payload: object):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_claude_provider_returns_raw_text_on_valid_response(monkeypatch):
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return _MockClaudeResponse({"content": [{"type": "text", "text": "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n"}]})

    monkeypatch.setattr("aether.dgce.providers.claude_provider.urlopen", fake_urlopen)

    raw_output = claude_provider.generate_text(
        "prompt text",
        {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key", "temperature": 0.0},
    )

    assert raw_output.startswith("def build_payload")
    assert captured["url"] == claude_provider.DEFAULT_API_BASE_URL
    assert captured["method"] == "POST"
    assert captured["timeout"] == 30
    assert captured["body"] == {
        "model": "claude-3-7-sonnet",
        "max_tokens": claude_provider.DEFAULT_MAX_TOKENS,
        "messages": [{"role": "user", "content": "prompt text"}],
        "temperature": 0.0,
    }
    assert captured["headers"]["X-api-key"] == "secret-key"
    assert captured["headers"]["Anthropic-version"] == "2023-06-01"


def test_claude_provider_malformed_response_fails_deterministically(monkeypatch):
    monkeypatch.setattr(
        "aether.dgce.providers.claude_provider.urlopen",
        lambda request, timeout: _MockClaudeResponse({"content": [{"type": "tool_use", "name": "ignored"}]}),
    )

    with pytest.raises(ValueError, match="Claude provider response missing text content"):
        claude_provider.generate_text("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key"})


def test_claude_provider_request_failure_fails_deterministically(monkeypatch):
    def fail_urlopen(request, timeout):
        raise OSError("network down")

    monkeypatch.setattr("aether.dgce.providers.claude_provider.urlopen", fail_urlopen)

    with pytest.raises(ValueError, match="Claude provider request failed"):
        claude_provider.generate_text("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key"})


def test_claude_provider_missing_api_key_fails_deterministically():
    with pytest.raises(ValueError, match="Claude provider requires config.api_key"):
        claude_provider.generate_text("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet"})


def test_validate_function_stub_blocks_malformed_output():
    with pytest.raises(ValueError, match="valid Python syntax"):
        validate_function_stub("def broken(", {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"})


def test_validate_function_stub_blocks_multiple_functions():
    with pytest.raises(ValueError, match="exactly one function"):
        validate_function_stub(
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n\n"
            "def second(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_wrong_function_name():
    with pytest.raises(ValueError, match="function name mismatch"):
        validate_function_stub(
            "def wrong_name(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_execute_prepared_function_stub_does_not_write_on_validation_failure(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.generate_function_stub",
        lambda structured_input, config: "def wrong_name(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )

    with pytest.raises(ValueError, match="function name mismatch"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    execution_path = project_root / ".dce/execution/function-stub.execution.json"
    if execution_path.exists():
        payload = json.loads(execution_path.read_text(encoding="utf-8"))
        assert payload.get("written_files") == []


def test_execute_prepared_function_stub_does_not_write_on_provider_config_failure(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_provider_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.get_model_execution_config",
        lambda: {
            "provider": "claude",
            "model_id": "claude-3-7-sonnet",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
        },
    )

    with pytest.raises(ValueError, match="Claude provider requires config.api_key"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    assert not (project_root / ".dce/execution/function-stub.execution.json").exists()


def test_execute_prepared_function_stub_does_not_write_on_claude_transport_failure(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_claude_transport_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.get_model_execution_config",
        lambda: {
            "provider": "claude",
            "model_id": "claude-3-7-sonnet",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
            "api_key": "secret-key",
        },
    )
    monkeypatch.setattr(
        "aether.dgce.providers.claude_provider.urlopen",
        lambda request, timeout: (_ for _ in ()).throw(OSError("network down")),
    )

    with pytest.raises(ValueError, match="Claude provider request failed"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    assert not (project_root / ".dce/execution/function-stub.execution.json").exists()
