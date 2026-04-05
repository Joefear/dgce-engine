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
from aether.dgce.execution_fingerprint import (
    build_function_stub_execution_fingerprint,
    determine_function_stub_write_idempotence_status,
)
from aether.dgce.execution_failure import classify_function_stub_execution_failure
from aether.dgce.execution_timing import build_execution_timing
from aether.dgce.function_stub_canonicalizer import canonicalize_function_stub_output
from aether.dgce.code_graph_context import (
    CONTRACT_SCHEMA_ID,
    VENDORED_CHECKSUM_PATH,
    VENDORED_SCHEMA_PATH,
    compute_schema_sha256,
    parse_code_graph_context,
    verify_vendored_code_graph_schema_checksum,
)
from aether.dgce.function_stub_spec import parse_function_stub_spec
from aether.dgce.model_execution_basis import (
    assert_function_stub_model_execution_basis_consistent,
    build_function_stub_model_execution_basis_fingerprint,
)
from aether.dgce.model_config import build_model_execution_metadata, get_model_execution_config
from aether.dgce.model_provider import generate_response
from aether.dgce.model_executor import generate_function_stub
from aether.dgce.prompt_templates import build_function_stub_prompt
from aether.dgce.provider_request_context import build_provider_request_context
from aether.dgce.provider_response import build_provider_response, normalize_provider_response
from aether.dgce.providers import claude_provider
from aether.dgce.prepare_api import prepare_section_execution
from aether.dgce.model_validator import validate_function_stub
from aether_core.enums import ArtifactStatus
from aether_core.router.executors import ExecutionResult


def _valid_code_graph_context() -> dict:
    return {
        "contract_name": "DefiantCodeGraphFacts",
        "contract_version": "dcg.facts.v1",
        "graph_id": "graph:payload-builder",
        "workspace_id": "workspace:dgce",
        "repo_id": "repo:dgce-engine",
        "language": "python",
        "generated_at": "2026-04-04T10:15:30Z",
        "source": "defiant-code-graph",
        "target": {
            "file_path": "src/function_stub.py",
            "symbol_id": "sym:payload_builder",
            "symbol_name": "build_payload",
            "symbol_kind": "function",
            "span": {"start_line": 10, "end_line": 12},
        },
        "intent_facts": {
            "structural_scope": "file",
            "module_boundary_crossed": False,
            "trust_boundary_crossed": False,
            "ownership_boundary_crossed": False,
            "protected_region_overlap": False,
            "governed_region_overlap": True,
            "related_symbols": ["sym:serialize_payload"],
            "related_files": ["src/serializer.py"],
        },
        "patch_facts": {
            "touched_files": ["src/function_stub.py"],
            "touched_symbols": ["sym:build_payload"],
            "structural_scope_expanded": False,
            "module_boundary_crossed": False,
            "trust_boundary_crossed": False,
            "ownership_boundary_crossed": False,
            "protected_region_overlap": False,
            "governed_region_overlap": True,
            "claimed_intent_match": True,
        },
        "placement_facts": {
            "insertion_candidates": [
                {
                    "file_path": "src/function_stub.py",
                    "symbol_id": "sym:existing_helper",
                    "symbol_name": "existing_helper",
                    "strategy": "append_after_symbol",
                    "span": {"start_line": 7, "end_line": 7},
                }
            ],
            "generation_collision_detected": True,
            "recommended_edit_strategy": "bounded_insert",
        },
        "impact_facts": {
            "blast_radius": {"files": 1, "symbols": 2},
            "dependency_crossings": ["module:serializer->module:function_stub"],
            "dependent_symbols": ["sym:payload_consumer"],
        },
        "ownership_facts": {
            "target_ownership": "governed",
            "touched_ownership_classes": ["governed"],
        },
        "meta": {
            "parser_family": "tree-sitter",
            "snapshot_id": "snapshot:123",
            "notes": ["read-only facts"],
        },
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


def _multi_function_stub_section() -> DGCESection:
    return DGCESection(
        section_id="function-stub",
        section_type="function_stub",
        title="Function Stub",
        description="Generate deterministic Python function stubs for one governed file during execution.",
        requirements=["produce exactly the required functions in one file"],
        constraints=["single file only", "no autonomous writes"],
        expected_targets=[
            {
                "path": "src/function_stub.py",
                "purpose": "Deterministic function stub file",
                "source": "expected_targets",
                "functions": [
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "dict[str, object]",
                    },
                    {
                        "name": "is_payload_empty",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "bool",
                    },
                ],
            }
        ],
    )


def _build_function_workspace(monkeypatch, name: str, section: DGCESection | None = None) -> Path:
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(name)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(section or _function_stub_section(), project_root, incremental_mode="incremental_v2_2")
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
    assert isinstance(execution_artifact["execution_content_fingerprint"], str)
    assert isinstance(execution_artifact["model_execution_basis_fingerprint"], str)
    assert execution_artifact["write_idempotence_status"] == "new_content"
    assert "execution_failure_category" not in execution_artifact
    assert "execution_failure_reason" not in execution_artifact
    assert execution_artifact["provider_request_context"] == {
        "provider": "stub",
        "model_id": "stub-model-v1",
        "prompt_template_version": "v1",
        "temperature": 0.0,
        "request_attempted": False,
    }
    assert sorted(execution_artifact["execution_timing"].keys()) == [
        "provider_duration_ms",
        "total_model_path_duration_ms",
        "validation_duration_ms",
    ]
    for value in execution_artifact["execution_timing"].values():
        assert isinstance(value, (int, float))
        assert float(value) >= 0.0


def test_execute_prepared_multi_function_stub_writes_validated_output_for_one_file(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_multi_function_valid", _multi_function_stub_section())
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")

    result = execute_prepared_section(project_root, "function-stub")

    assert result["status"] == "ok"
    assert (project_root / "src/function_stub.py").read_text(encoding="utf-8") == (
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
        "    return {}\n\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\n"
        "    return False\n"
    )
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["model_execution"]["provider"] == "stub"
    assert execution_artifact["write_idempotence_status"] == "new_content"


def test_generate_function_stub_uses_model_provider_boundary(monkeypatch):
    captured: dict[str, object] = {}

    def fake_generate_response(prompt: str, config: dict) -> dict:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return {"raw_text": "provider output", "request_attempted": False}

    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fake_generate_response)

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
    assert "* function_count: 1" in str(captured["prompt"])
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
        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
        "return_type": "dict[str, object]",
    }
    assert build_function_stub_prompt(structured_input, "v1") == build_function_stub_prompt(structured_input, "v1")


def test_build_function_stub_prompt_uses_normalized_multi_function_spec():
    prompt = build_function_stub_prompt(
        {
            "functions": [
                {
                    "name": "build_payload",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "dict[str, object]",
                },
                {
                    "name": "is_payload_empty",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "bool",
                },
            ]
        },
        "v1",
    )

    assert "* function_count: 2" in prompt
    assert "build_payload" in prompt
    assert "is_payload_empty" in prompt
    assert "CODE_GRAPH_CONTEXT:" not in prompt


def test_build_function_stub_prompt_accepts_optional_code_graph_context_deterministically():
    structured_input = {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ],
        "code_graph_context": _valid_code_graph_context(),
    }

    first = build_function_stub_prompt(structured_input, "v1")
    second = build_function_stub_prompt(structured_input, "v1")

    assert first == second
    assert "CODE_GRAPH_CONTEXT:" in first
    assert '"contract_name": "DefiantCodeGraphFacts"' in first


def test_build_function_stub_prompt_rejects_unsupported_template_version():
    with pytest.raises(ValueError, match="template_version must be one of: v1"):
        build_function_stub_prompt(
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
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


def test_provider_response_helpers_are_bounded():
    assert build_provider_response("output", request_attempted=True) == {
        "raw_text": "output",
        "request_attempted": True,
    }
    assert normalize_provider_response({"raw_text": "output", "request_attempted": False}) == {
        "raw_text": "output",
        "request_attempted": False,
    }


def test_build_function_stub_execution_fingerprint_is_deterministic():
    assert build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    ) == build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )


def test_build_function_stub_model_execution_basis_fingerprint_is_deterministic():
    assert build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    ) == build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    )


def test_build_function_stub_model_execution_basis_fingerprint_changes_with_spec():
    first = build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    )
    second = build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}, {"name": "flag", "type": "bool"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    )
    assert first != second


def test_build_function_stub_model_execution_basis_fingerprint_changes_with_execution_config():
    first = build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    )
    second = build_function_stub_model_execution_basis_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "claude", "model_id": "claude-3-7-sonnet", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
    )
    assert first != second


def test_assert_function_stub_model_execution_basis_consistent_rejects_mismatch():
    with pytest.raises(ValueError, match="function_stub model execution basis mismatch"):
        assert_function_stub_model_execution_basis_consistent(
            "not-the-right-fingerprint",
            {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
            {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
            "src/function_stub.py",
        )


def test_build_function_stub_execution_fingerprint_changes_with_spec():
    first = build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )
    second = build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}, {"name": "flag", "type": "bool"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object], flag: bool) -> dict[str, object]:\n    return {}\n",
    )
    assert first != second


def test_build_function_stub_execution_fingerprint_changes_with_execution_config():
    first = build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )
    second = build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "claude", "model_id": "claude-3-7-sonnet", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )
    assert first != second


def test_determine_function_stub_write_idempotence_status_is_deterministic():
    tmp_path = _workspace_dir("function_stub_idempotence_status")
    assert determine_function_stub_write_idempotence_status(
        tmp_path,
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    ) == "new_content"
    target_path = tmp_path / "src" / "function_stub.py"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
        encoding="utf-8",
    )
    assert determine_function_stub_write_idempotence_status(
        tmp_path,
        "src/function_stub.py",
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    ) == "existing_content_match"


def test_execution_fingerprinting_and_idempotence_use_canonical_content():
    canonical = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n"
    )
    variant = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\r\n    return {}   \r\n\r\n"
    )
    assert canonical == variant
    assert build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        canonical,
    ) == build_function_stub_execution_fingerprint(
        {"name": "build_payload", "parameters": [{"name": "payload", "type": "dict[str, object]"}], "return_type": "dict[str, object]"},
        {"provider": "stub", "model_id": "stub-model-v1", "temperature": 0.0, "prompt_template_version": "v1", "postprocess": "strict_function_stub_v1"},
        "src/function_stub.py",
        variant,
    )


def test_multi_function_execution_fingerprinting_uses_canonical_content():
    spec = parse_function_stub_spec(
        {
            "functions": [
                {
                    "name": "build_payload",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "dict[str, object]",
                },
                {
                    "name": "is_payload_empty",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "bool",
                },
            ]
        }
    )
    metadata = {
        "provider": "stub",
        "model_id": "stub-model-v1",
        "temperature": 0.0,
        "prompt_template_version": "v1",
        "postprocess": "strict_function_stub_v1",
    }
    canonical = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
        "    return {}\n\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\n"
        "    return False\n"
    )
    variant = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\r\n"
        "    return {}   \r\n\r\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\r\n"
        "    return False   \r\n\r\n"
    )

    assert build_function_stub_execution_fingerprint(
        spec,
        metadata,
        "src/function_stub.py",
        canonical,
    ) == build_function_stub_execution_fingerprint(
        spec,
        metadata,
        "src/function_stub.py",
        variant,
    )


def test_classify_function_stub_execution_failure_is_bounded():
    assert classify_function_stub_execution_failure(raw_output_obtained=False) == {
        "execution_failure_category": "provider_failure",
        "execution_failure_reason": "pre_output_failure",
    }
    assert classify_function_stub_execution_failure(raw_output_obtained=True) == {
        "execution_failure_category": "validation_failure",
        "execution_failure_reason": "validator_rejected_output",
    }


def test_build_provider_request_context_is_audit_safe_and_bounded():
    assert build_provider_request_context(
        {
            "provider": "claude",
            "model_id": "claude-3-7-sonnet",
            "prompt_template_version": "v1",
            "temperature": 0.0,
            "api_key": "secret-key",
            "api_base_url": "https://example.invalid",
        },
        request_attempted=True,
    ) == {
        "provider": "claude",
        "model_id": "claude-3-7-sonnet",
        "prompt_template_version": "v1",
        "temperature": 0.0,
        "request_attempted": True,
    }


def test_build_execution_timing_is_bounded():
    assert build_execution_timing(
        provider_duration_ms=1.0,
        validation_duration_ms=2.0,
        total_model_path_duration_ms=3.0,
    ) == {
        "provider_duration_ms": 1.0,
        "validation_duration_ms": 2.0,
        "total_model_path_duration_ms": 3.0,
    }


def test_canonicalize_function_stub_output_is_deterministic():
    assert canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\r\n    return {}   \r\n\r\n"
    ) == "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n"


def test_harmless_formatting_differences_normalize_to_same_canonical_content():
    first = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n"
    )
    second = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\r\n    return {}   \r\n\r\n\r\n"
    )
    assert first == second


def test_multi_function_harmless_formatting_differences_normalize_to_same_canonical_content():
    first = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
        "    return {}\n\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\n"
        "    return False\n"
    )
    second = canonicalize_function_stub_output(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\r\n"
        "    return {}   \r\n\r\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\r\n"
        "    return False   \r\n\r\n\r\n"
    )
    assert first == second


def test_parse_function_stub_spec_normalizes_valid_input_deterministically():
    assert parse_function_stub_spec(
        {
            "name": " build_payload ",
            "parameters": [{"name": " payload ", "type": " dict[str, object] "}],
            "return_type": " dict[str, object] ",
        }
    ) == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ],
    }


def test_parse_function_stub_spec_supports_compatibility_aliases():
    assert parse_function_stub_spec(
        {
            "name": "build_payload",
            "inputs": [{"name": "payload", "type": "dict[str, object]"}],
            "output": "dict[str, object]",
        }
    ) == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ],
    }


def test_parse_function_stub_spec_accepts_multi_function_single_file_shape():
    assert parse_function_stub_spec(
        {
            "functions": [
                {
                    "name": " build_payload ",
                    "parameters": [{"name": " payload ", "type": " dict[str, object] "}],
                    "return_type": " dict[str, object] ",
                },
                {
                    "name": " is_payload_empty ",
                    "parameters": [{"name": " payload ", "type": " dict[str, object] "}],
                    "return_type": " bool ",
                },
            ]
        }
    ) == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            },
            {
                "name": "is_payload_empty",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "bool",
            },
        ]
    }


def test_parse_code_graph_context_accepts_valid_v1_payload():
    assert parse_code_graph_context(_valid_code_graph_context()) == _valid_code_graph_context()


def test_vendored_code_graph_schema_checksum_matches_raw_bytes():
    verify_vendored_code_graph_schema_checksum()
    vendored_checksum = VENDORED_CHECKSUM_PATH.read_text(encoding="utf-8").strip().split()[0]
    assert compute_schema_sha256(VENDORED_SCHEMA_PATH) == vendored_checksum


def test_vendored_code_graph_schema_has_expected_id():
    schema = json.loads(VENDORED_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["$id"] == CONTRACT_SCHEMA_ID


def test_vendored_code_graph_schema_checksum_drift_is_detected():
    tmp_path = _workspace_dir("code_graph_schema_checksum_drift")
    tmp_path.mkdir(parents=True, exist_ok=True)
    schema_path = tmp_path / "dcg-facts-v1.schema.json"
    checksum_path = tmp_path / "dcg-facts-v1.sha256"
    schema_path.write_bytes(VENDORED_SCHEMA_PATH.read_bytes() + b"\n")
    checksum_path.write_text(VENDORED_CHECKSUM_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    with pytest.raises(ValueError, match="Vendored Defiant Code Graph schema checksum mismatch"):
        verify_vendored_code_graph_schema_checksum(schema_path, checksum_path)


def test_parse_code_graph_context_preserves_symbol_id_field_meaning_exactly():
    payload = _valid_code_graph_context()
    parsed = parse_code_graph_context(payload)

    assert parsed["target"]["symbol_id"] == "sym:payload_builder"
    assert parsed["intent_facts"]["related_symbols"] == ["sym:serialize_payload"]
    assert parsed["patch_facts"]["touched_symbols"] == ["sym:build_payload"]
    assert parsed["impact_facts"]["dependent_symbols"] == ["sym:payload_consumer"]
    assert parsed["placement_facts"]["insertion_candidates"][0]["symbol_id"] == "sym:existing_helper"


def test_parse_function_stub_spec_accepts_optional_code_graph_context():
    assert parse_function_stub_spec(
        {
            "name": "build_payload",
            "parameters": [{"name": "payload", "type": "dict[str, object]"}],
            "return_type": "dict[str, object]",
            "code_graph_context": _valid_code_graph_context(),
        }
    ) == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ],
        "code_graph_context": _valid_code_graph_context(),
    }


def test_parse_function_stub_spec_rejects_malformed_parameters():
    with pytest.raises(ValueError, match="function_stub.parameters\\[0\\]\\.type must be a non-empty string"):
        parse_function_stub_spec(
            {
                "name": "build_payload",
                "parameters": [{"name": "payload"}],
                "return_type": "dict[str, object]",
            }
        )


def test_parse_function_stub_spec_rejects_empty_multi_function_input():
    with pytest.raises(ValueError, match="function_stub.functions must be a non-empty list"):
        parse_function_stub_spec({"functions": []})


def test_parse_function_stub_spec_rejects_duplicate_function_names():
    with pytest.raises(ValueError, match="function_stub.functions\\[1\\]\\.name must be unique"):
        parse_function_stub_spec(
            {
                "functions": [
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "dict[str, object]",
                    },
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "bool",
                    },
                ]
            }
        )


def test_parse_code_graph_context_rejects_wrong_contract_name():
    payload = _valid_code_graph_context()
    payload["contract_name"] = "WrongContract"

    with pytest.raises(ValueError, match="function_stub.code_graph_context.contract_name must be DefiantCodeGraphFacts"):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_wrong_contract_version():
    payload = _valid_code_graph_context()
    payload["contract_version"] = "dcg.facts.v2"

    with pytest.raises(ValueError, match="function_stub.code_graph_context.contract_version must be dcg.facts.v1"):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_extra_fields():
    payload = _valid_code_graph_context()
    payload["unexpected"] = "value"

    with pytest.raises(ValueError, match="function_stub.code_graph_context must not include extra fields"):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_extra_nested_fields():
    payload = _valid_code_graph_context()
    payload["placement_facts"]["insertion_candidates"][0]["extra"] = "value"

    with pytest.raises(
        ValueError,
        match="function_stub.code_graph_context.placement_facts.insertion_candidates\\[0\\] must not include extra fields",
    ):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_invalid_generated_at():
    payload = _valid_code_graph_context()
    payload["generated_at"] = "2026-04-04 10:15:30"

    with pytest.raises(ValueError, match="function_stub.code_graph_context.generated_at must be RFC 3339 when provided"):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_invalid_span_invariant():
    payload = _valid_code_graph_context()
    payload["placement_facts"]["insertion_candidates"][0]["span"] = {"start_line": 10, "end_line": 3}

    with pytest.raises(
        ValueError,
        match="function_stub.code_graph_context.placement_facts.insertion_candidates\\[0\\]\\.span.end_line must be greater than or equal to function_stub.code_graph_context.placement_facts.insertion_candidates\\[0\\]\\.span.start_line",
    ):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_invalid_object_placement():
    payload = _valid_code_graph_context()
    payload["intent_facts"]["related_symbols"] = {"symbol_id": "sym:serialize_payload"}

    with pytest.raises(ValueError, match="function_stub.code_graph_context.intent_facts.related_symbols must be a list"):
        parse_code_graph_context(payload)


def test_parse_code_graph_context_rejects_invalid_null_placement():
    payload = _valid_code_graph_context()
    payload["placement_facts"]["insertion_candidates"] = [None]

    with pytest.raises(ValueError, match="function_stub.code_graph_context.placement_facts.insertion_candidates\\[0\\] must be a dict"):
        parse_code_graph_context(payload)


def test_parse_function_stub_spec_rejects_malformed_code_graph_context():
    payload = _valid_code_graph_context()
    payload["contract_version"] = "dcg.facts.v2"

    with pytest.raises(ValueError, match="function_stub.code_graph_context.contract_version must be dcg.facts.v1"):
        parse_function_stub_spec(
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
                "code_graph_context": payload,
            }
        )


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
    def fake_generate_response(prompt: str, config: dict) -> dict:
        assert "FUNCTION_STUB_SPEC:" in prompt
        assert config["provider"] == "claude"
        return {"raw_text": "claude output", "request_attempted": True}

    monkeypatch.setattr("aether.dgce.model_provider.claude_provider.generate_response", fake_generate_response)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "parameters": [{"name": "payload", "type": "dict[str, object]"}],
            "return_type": "dict[str, object]",
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

    def fake_generate_response(prompt: str, config: dict) -> dict:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return {"raw_text": "provider output", "request_attempted": False}

    monkeypatch.setattr("aether.dgce.model_executor.build_function_stub_prompt", fake_build_function_stub_prompt)
    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fake_generate_response)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "parameters": [{"name": "payload", "type": "dict[str, object]"}],
            "return_type": "dict[str, object]",
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
    assert captured["structured_input"] == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ]
    }


def test_generate_function_stub_passes_optional_code_graph_context_through_prompt_module(monkeypatch):
    captured: dict[str, object] = {}

    def fake_build_function_stub_prompt(structured_input: dict, template_version: str) -> str:
        captured["structured_input"] = structured_input
        captured["template_version"] = template_version
        return "built prompt"

    def fake_generate_response(prompt: str, config: dict) -> dict:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return {"raw_text": "provider output", "request_attempted": False}

    monkeypatch.setattr("aether.dgce.model_executor.build_function_stub_prompt", fake_build_function_stub_prompt)
    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fake_generate_response)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "parameters": [{"name": "payload", "type": "dict[str, object]"}],
            "return_type": "dict[str, object]",
            "code_graph_context": _valid_code_graph_context(),
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
    assert captured["structured_input"] == {
        "functions": [
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            }
        ],
        "code_graph_context": _valid_code_graph_context(),
    }


def test_generate_function_stub_rejects_malformed_input_before_provider_call(monkeypatch):
    def fail_generate_response(prompt: str, config: dict) -> dict:
        raise AssertionError("provider should not be called")

    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fail_generate_response)

    with pytest.raises(ValueError, match="function_stub.parameters is required"):
        generate_function_stub(
            {"name": "build_payload", "return_type": "dict[str, object]"},
            {
                "provider": "stub",
                "model_id": "stub-model-v1",
                "temperature": 0.0,
                "prompt_template_version": "v1",
                "postprocess": "strict_function_stub_v1",
            },
        )


def test_generate_function_stub_rejects_malformed_code_graph_context_before_provider_call(monkeypatch):
    def fail_generate_response(prompt: str, config: dict) -> dict:
        raise AssertionError("provider should not be called")

    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fail_generate_response)

    malformed_payload = _valid_code_graph_context()
    malformed_payload["generated_at"] = "2026-04-04 10:15:30"

    with pytest.raises(ValueError, match="function_stub.code_graph_context.generated_at must be RFC 3339 when provided"):
        generate_function_stub(
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
                "code_graph_context": malformed_payload,
            },
            {
                "provider": "stub",
                "model_id": "stub-model-v1",
                "temperature": 0.0,
                "prompt_template_version": "v1",
                "postprocess": "strict_function_stub_v1",
            },
        )


class _MockClaudeResponse:
    def __init__(self, payload: object):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_stub_provider_returns_normalized_response_contract():
    assert generate_response(
        build_function_stub_prompt(
            {
                "name": "build_payload",
                "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                "return_type": "dict[str, object]",
            },
            "v1",
        ),
        {
            "provider": "stub",
            "model_id": "stub-model-v1",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
        },
    ) == {
        "raw_text": "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
        "request_attempted": False,
    }


def test_stub_provider_returns_multi_function_single_file_response_contract():
    assert generate_response(
        build_function_stub_prompt(
            {
                "functions": [
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "dict[str, object]",
                    },
                    {
                        "name": "is_payload_empty",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "bool",
                    },
                ]
            },
            "v1",
        ),
        {
            "provider": "stub",
            "model_id": "stub-model-v1",
            "temperature": 0.0,
            "prompt_template_version": "v1",
            "postprocess": "strict_function_stub_v1",
        },
    ) == {
        "raw_text": (
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
            "    return {}\n\n"
            "def is_payload_empty(payload: dict[str, object]) -> bool:\n"
            "    return False\n"
        ),
        "request_attempted": False,
    }


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

    normalized = claude_provider.generate_response(
        "prompt text",
        {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key", "temperature": 0.0},
    )

    assert normalized == {
        "raw_text": "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
        "request_attempted": True,
    }
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
        claude_provider.generate_response("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key"})


def test_claude_provider_request_failure_fails_deterministically(monkeypatch):
    def fail_urlopen(request, timeout):
        raise OSError("network down")

    monkeypatch.setattr("aether.dgce.providers.claude_provider.urlopen", fail_urlopen)

    with pytest.raises(ValueError, match="Claude provider request failed"):
        claude_provider.generate_response("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet", "api_key": "secret-key"})


def test_claude_provider_missing_api_key_fails_deterministically():
    with pytest.raises(ValueError, match="Claude provider requires config.api_key"):
        claude_provider.generate_response("prompt text", {"provider": "claude", "model_id": "claude-3-7-sonnet"})


def test_executor_extracts_only_normalized_raw_text(monkeypatch):
    captured: dict[str, object] = {}

    def fake_generate_response(prompt: str, config: dict) -> dict:
        captured["prompt"] = prompt
        captured["config"] = dict(config)
        return {"raw_text": "normalized raw text", "request_attempted": True}

    monkeypatch.setattr("aether.dgce.model_executor.model_provider.generate_response", fake_generate_response)

    raw_output = generate_function_stub(
        {
            "name": "build_payload",
            "parameters": [{"name": "payload", "type": "dict[str, object]"}],
            "return_type": "dict[str, object]",
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

    assert raw_output == "normalized raw text"


def test_validate_function_stub_blocks_malformed_output():
    with pytest.raises(ValueError, match="valid Python syntax"):
        validate_function_stub("def broken(", {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"})


def test_validate_function_stub_accepts_valid_multi_function_single_file_output():
    assert validate_function_stub(
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\n    return False\n",
        {
            "functions": [
                {
                    "name": "build_payload",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "dict[str, object]",
                },
                {
                    "name": "is_payload_empty",
                    "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                    "return_type": "bool",
                },
            ]
        },
    ) == (
        "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
        "    return {}\n\n"
        "def is_payload_empty(payload: dict[str, object]) -> bool:\n"
        "    return False\n"
    )


def test_validate_function_stub_blocks_partial_multi_function_output():
    with pytest.raises(ValueError, match="exactly the required functions"):
        validate_function_stub(
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {
                "functions": [
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "dict[str, object]",
                    },
                    {
                        "name": "is_payload_empty",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "bool",
                    },
                ]
            },
        )


def test_validate_function_stub_blocks_wrong_function_name():
    with pytest.raises(ValueError, match="function name mismatch"):
        validate_function_stub(
            "def wrong_name(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_non_function_top_level_output():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "import os\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_top_level_print_statement():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "print('debug')\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_top_level_assignment():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "value = {}\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_top_level_class_definition():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "class Helper:\n    pass\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_if_main_guard():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "if __name__ == '__main__':\n    print('debug')\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_async_top_level_function():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "async def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_top_level_try_statement():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "try:\n    pass\nexcept Exception:\n    pass\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_top_level_with_statement():
    with pytest.raises(ValueError, match="only the required top-level functions"):
        validate_function_stub(
            "with open('x') as handle:\n    handle.read()\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_extra_helper_function():
    with pytest.raises(ValueError, match="exactly the required functions"):
        validate_function_stub(
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n\n"
            "def helper(payload: dict[str, object]) -> dict[str, object]:\n    return payload\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_decorated_function():
    with pytest.raises(ValueError, match="must not include decorators"):
        validate_function_stub(
            "@staticmethod\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_nested_function_definition():
    with pytest.raises(ValueError, match="must not include nested definitions"):
        validate_function_stub(
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
            "    def helper() -> dict[str, object]:\n"
            "        return {}\n"
            "    return helper()\n",
            {"name": "build_payload", "inputs": [{"name": "payload", "type": "dict[str, object]"}], "output": "dict[str, object]"},
        )


def test_validate_function_stub_blocks_wrong_ordered_function_set():
    with pytest.raises(ValueError, match="function name mismatch"):
        validate_function_stub(
            "def is_payload_empty(payload: dict[str, object]) -> bool:\n    return False\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
            {
                "functions": [
                    {
                        "name": "build_payload",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "dict[str, object]",
                    },
                    {
                        "name": "is_payload_empty",
                        "parameters": [{"name": "payload", "type": "dict[str, object]"}],
                        "return_type": "bool",
                    },
                ]
            },
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
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "validation_failure"
    assert execution_artifact["execution_failure_reason"] == "validator_rejected_output"
    assert execution_artifact["provider_request_context"]["request_attempted"] is False
    assert sorted(execution_artifact["execution_timing"].keys()) == [
        "provider_duration_ms",
        "total_model_path_duration_ms",
        "validation_duration_ms",
    ]
    assert "api_key" not in json.dumps(execution_artifact)
    assert "wrong_name" not in json.dumps(execution_artifact)
    assert execution_artifact.get("written_files") == []


def test_execute_prepared_multi_function_stub_does_not_write_on_partial_output(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_multi_function_no_write_on_partial_output", _multi_function_stub_section())
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.generate_function_stub",
        lambda structured_input, config: "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n    return {}\n",
    )

    with pytest.raises(ValueError, match="exactly the required functions"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "validation_failure"
    assert execution_artifact["execution_failure_reason"] == "validator_rejected_output"
    assert execution_artifact["provider_request_context"]["request_attempted"] is False


def test_execute_prepared_function_stub_does_not_write_on_structurally_disallowed_output(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_structural_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.generate_function_stub",
        lambda structured_input, config: (
            "value = {}\n\n"
            "def build_payload(payload: dict[str, object]) -> dict[str, object]:\n"
            "    return {}\n"
        ),
    )

    with pytest.raises(ValueError, match="only the required top-level functions"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "validation_failure"
    assert execution_artifact["execution_failure_reason"] == "validator_rejected_output"


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
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "provider_failure"
    assert execution_artifact["execution_failure_reason"] == "pre_output_failure"
    assert execution_artifact["provider_request_context"] == {
        "provider": "claude",
        "model_id": "claude-3-7-sonnet",
        "prompt_template_version": "v1",
        "temperature": 0.0,
        "request_attempted": False,
    }
    assert sorted(execution_artifact["execution_timing"].keys()) == [
        "provider_duration_ms",
        "total_model_path_duration_ms",
    ]
    assert "secret-key" not in json.dumps(execution_artifact)


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
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "provider_failure"
    assert execution_artifact["execution_failure_reason"] == "pre_output_failure"
    assert execution_artifact["provider_request_context"]["request_attempted"] is True
    assert sorted(execution_artifact["execution_timing"].keys()) == [
        "provider_duration_ms",
        "total_model_path_duration_ms",
    ]
    assert "secret-key" not in json.dumps(execution_artifact)


def test_execute_prepared_function_stub_does_not_write_on_input_contract_failure(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_input_contract_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose._build_function_stub_structured_input",
        lambda section, file_plan: ({"name": "build_payload", "return_type": "dict[str, object]"}, "src/function_stub.py"),
    )

    with pytest.raises(ValueError, match="function_stub.parameters is required"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "provider_failure"
    assert execution_artifact["execution_failure_reason"] == "pre_output_failure"
    assert "provider_request_context" not in execution_artifact
    assert sorted(execution_artifact["execution_timing"].keys()) == ["total_model_path_duration_ms"]
    assert isinstance(execution_artifact["execution_timing"]["total_model_path_duration_ms"], (int, float))
    assert float(execution_artifact["execution_timing"]["total_model_path_duration_ms"]) >= 0.0


def test_execute_prepared_function_stub_records_deterministic_fingerprint_and_idempotence_on_repeat(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_repeat_fingerprint")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")

    first_result = execute_prepared_section(project_root, "function-stub")
    assert first_result["status"] == "ok"
    first_artifact = load_section_execution_artifact(project_root, "function-stub")
    first_fingerprint = first_artifact["execution_content_fingerprint"]
    validated_content = (project_root / "src/function_stub.py").read_text(encoding="utf-8")
    second_fingerprint = build_function_stub_execution_fingerprint(
        parse_function_stub_spec(
            {
                "name": "build_payload",
                "inputs": [{"name": "payload", "type": "dict[str, object]"}],
                "output": "dict[str, object]",
            }
        ),
        first_artifact["model_execution"],
        "src/function_stub.py",
        validated_content,
    )

    assert second_fingerprint == first_fingerprint
    assert determine_function_stub_write_idempotence_status(
        project_root,
        "src/function_stub.py",
        validated_content,
    ) == "existing_content_match"


def test_execute_prepared_function_stub_does_not_write_if_fingerprint_logic_fails(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_fingerprint_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.build_function_stub_execution_fingerprint",
        lambda function_stub_spec, model_execution_metadata, target_path, validated_content: (_ for _ in ()).throw(
            ValueError("execution fingerprint failed")
        ),
    )

    with pytest.raises(ValueError, match="execution fingerprint failed"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    assert not (project_root / ".dce/execution/function-stub.execution.json").exists()


def test_execute_prepared_function_stub_does_not_write_if_model_execution_basis_is_inconsistent(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_basis_mismatch")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")

    sequence = iter(["first-fingerprint", "second-fingerprint"])

    monkeypatch.setattr(
        "aether.dgce.decompose.build_function_stub_model_execution_basis_fingerprint",
        lambda function_stub_spec, model_execution_metadata, target_path: next(sequence),
    )

    with pytest.raises(ValueError, match="function_stub model execution basis mismatch"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    assert not (project_root / ".dce/execution/function-stub.execution.json").exists()


def test_execute_prepared_function_stub_does_not_write_if_canonicalization_fails(monkeypatch):
    project_root = _build_function_workspace(monkeypatch, "model_execution_no_write_on_canonicalization_failure")
    _mark_section_ready(project_root)
    prepare_section_execution(project_root, "function-stub")
    monkeypatch.setattr(
        "aether.dgce.decompose.canonicalize_function_stub_output",
        lambda validated_output: (_ for _ in ()).throw(ValueError("canonicalization failed")),
    )

    with pytest.raises(ValueError, match="canonicalization failed"):
        execute_prepared_section(project_root, "function-stub")

    assert not (project_root / "src/function_stub.py").exists()
    assert not (project_root / ".dce/outputs/function-stub.json").exists()
    execution_artifact = load_section_execution_artifact(project_root, "function-stub")
    assert execution_artifact["execution_failure_category"] == "validation_failure"
