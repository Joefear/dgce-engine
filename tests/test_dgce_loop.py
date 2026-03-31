import json
from pathlib import Path

from fastapi.testclient import TestClient

from aether.dgce import (
    DGCESection,
    build_file_plan,
    decompose_section,
    preflight_section,
    run_section,
    run_section_and_write,
    run_section_with_workspace,
    write_file_plan,
)
from aether.dgce.decompose import ResponseEnvelope
from aether.dgce.file_writer import render_file_entry_content
from aether.dgce.file_plan import FilePlan
from aether_core.classifier.rules import ClassifierRules, TaskBucket
from aether_core.classifier.service import ClassificationService
from aether_core.itera.artifact_store import ArtifactStore
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.models import ClassificationRequest
from aether_core.models.request import OutputContract
from aether_core.router.executors import ExecutionResult
from aether_core.router.planner import RouterPlanner
from aether_core.enums import ArtifactStatus
from apps.aether_api.main import create_app


def _section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        requirements=[
            "support mission templates",
            "track progression state",
        ],
        constraints=[
            "keep save format stable",
            "support mod extension points",
        ],
    )


def _section_named(title: str) -> DGCESection:
    section = _section().model_copy()
    section.title = title
    return section


def _section_with_targets(*paths: str) -> DGCESection:
    section = _section().model_copy()
    section.expected_targets = list(paths)
    return section


def _data_model_section() -> DGCESection:
    return DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE lifecycle data model.",
        requirements=[
            "Define lifecycle artifacts",
            "Keep the model deterministic and auditable",
        ],
        constraints=[
            "Do not bypass governed execution",
            "Keep lifecycle status derivation explicit",
        ],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )


def _api_surface_section() -> DGCESection:
    return DGCESection(
        section_type="system_component",
        title="API Surface",
        description="Define the governed DGCE API surface as a deterministic, auditable contract.",
        requirements=[
            "Define lifecycle-safe endpoints",
            "Expose explicit status and next_action behavior",
        ],
        constraints=[
            "Do not bypass governed lifecycle stages",
            "Keep the contract implementation-ready and stable",
        ],
        expected_targets=["aether/dgce/decompose.py", "aether_core/router/planner.py"],
    )


def _system_breakdown_section() -> DGCESection:
    return DGCESection(
        section_type="system_component",
        title="System Breakdown",
        description="Define the DGCE system as concrete modules and a deterministic build graph.",
        requirements=[
            "Define implementation-ready module contracts",
            "Make build order explicit",
        ],
        constraints=[
            "Do not change DGCE governance",
            "Avoid narrative-only output",
        ],
        expected_targets=["aether/dgce/decompose.py", "aether_core/router/planner.py"],
    )


def _typed_system_breakdown_section() -> DGCESection:
    return DGCESection(
        section_type="system_breakdown",
        title="Implementation Contract",
        description="Define the DGCE system as concrete modules and a deterministic build graph.",
        requirements=[
            "Define implementation-ready module contracts",
            "Make build order explicit",
        ],
        constraints=[
            "Do not change DGCE governance",
            "Avoid narrative-only output",
        ],
        expected_targets=["aether/dgce/decompose.py", "aether_core/router/planner.py"],
    )


def _dgce_non_api_section() -> DGCESection:
    return DGCESection(
        section_type="system_component",
        title="Execution Summary",
        description="Summarize DGCE lifecycle behavior without defining an API surface.",
        requirements=[
            "Summarize lifecycle state",
        ],
        constraints=[
            "Do not change DGCE governance",
        ],
    )


def _paths(name: str):
    base = Path("tests/.tmp")
    base.mkdir(parents=True, exist_ok=True)
    return (
        base / f"{name}_telemetry.jsonl",
        base / f"{name}_cache.json",
        base / f"{name}_artifacts.jsonl",
    )


def _scaffold_dir(name: str) -> Path:
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


def _stub_executor_output(content: str) -> str:
    lowered = content.lower()
    if "system breakdown" in lowered:
        return json.dumps(
            {
                "module_name": "mission_board",
                "purpose": "coordinate mission generation",
                "subcomponents": ["templates", "tracker"],
                "dependencies": ["save_state"],
                "implementation_order": ["templates", "tracker"],
            }
        )
    if "data model" in lowered:
        return json.dumps(
            {
                "entities": ["Mission"],
                "fields": ["id", "state"],
                "relationships": ["mission->player"],
                "validation_rules": ["id required"],
            }
        )
    if "api surface" in lowered:
        return json.dumps(
            {
                "interfaces": ["MissionBoardService"],
                "methods": ["create_mission"],
                "inputs": ["template_id"],
                "outputs": ["mission_id"],
                "error_cases": ["template_missing"],
            }
        )
    return "Summary output"


def _stub_executor_result(content: str) -> ExecutionResult:
    return ExecutionResult(
        output=_stub_executor_output(content),
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


def test_decomposition_produces_expected_number_of_tasks():
    tasks = decompose_section(_section())

    assert len(tasks) == 4


def test_data_model_decomposition_excludes_system_breakdown_tasks():
    tasks = decompose_section(_data_model_section())

    assert [task.task_type for task in tasks] == [
        "data_model",
        "api_surface",
        "system_summary",
    ]
    assert all(task.task_type != "system_breakdown" for task in tasks)


def test_decomposition_tasks_have_expected_buckets_and_subtypes():
    classifier = ClassifierRules()
    tasks = decompose_section(_section())

    expected_task_types = [
        "system_breakdown",
        "data_model",
        "api_surface",
        "system_summary",
    ]
    expected_buckets = [
        TaskBucket.PLANNING,
        TaskBucket.CODE_ROUTINE,
        TaskBucket.CODE_ROUTINE,
        TaskBucket.PLANNING,
    ]

    assert [task.task_type for task in tasks] == expected_task_types
    assert [task.metadata["task_subtype"] for task in tasks] == expected_task_types
    assert [task.metadata["project_id"] for task in tasks] == ["DGCE"] * 4
    assert [task.metadata["prompt_profile"] for task in tasks] == ["dgce_system_design"] * 4
    assert [classifier.classify(task.content)["bucket"] for task in tasks] == expected_buckets
    assert tasks[0].output_contract is not None
    assert tasks[0].output_contract.schema_name == "dgce_system_breakdown_v1"
    assert tasks[1].output_contract is not None
    assert tasks[1].output_contract.schema_name == "dgce_data_model_v1"
    assert tasks[2].output_contract is not None
    assert tasks[2].output_contract.schema_name == "dgce_api_surface_v1"
    assert tasks[3].output_contract is None


def test_data_model_section_uses_contract_driven_generation_prompt():
    tasks = decompose_section(_data_model_section())
    data_model_task = tasks[0]

    assert "Required entities in stable order" in data_model_task.content
    assert "SectionInput, PreviewArtifact, ReviewArtifact, ApprovalArtifact, PreflightRecord, ExecutionGate, AlignmentRecord, ExecutionStamp, OutputArtifact" in data_model_task.content
    assert "modules, entities, fields, relationships, and validation_rules" in data_model_task.content
    assert "You MUST return ONLY valid JSON." in data_model_task.content
    assert "Do NOT include any explanation, prose, markdown, or comments." in data_model_task.content
    assert "The output must be directly parsable by json.loads()." in data_model_task.content
    assert "Return EXACTLY this format:" in data_model_task.content
    assert '"modules": [...],' in data_model_task.content
    assert '"entities": [...],' in data_model_task.content
    assert '"fields": [...],' in data_model_task.content
    assert '"relationships": [...],' in data_model_task.content
    assert '"validation_rules": [...]' in data_model_task.content
    assert "If the output is not valid JSON, the system will fail validation." in data_model_task.content
    assert "The modules key must be a non-empty array. The entities key must be a non-empty array." in data_model_task.content
    assert "Entity names must be short PascalCase nouns" in data_model_task.content
    assert "Never derive entity names from descriptions, field dumps, dict text, or storage-path prose." in data_model_task.content
    assert "Do not emit duplicate entities or near-duplicates" in data_model_task.content
    assert data_model_task.metadata["require_non_empty_structured_output"] is True
    assert "Do not emit empty structured output." in data_model_task.content
    assert "Validation will fail if the output does not include a top-level 'modules' array." in data_model_task.content
    assert "Each module must include name, entities, relationships, required, and identity_keys." in data_model_task.content
    assert "identity_keys" in data_model_task.content
    assert "storage_path" in data_model_task.content
    assert "artifact_fingerprint" in data_model_task.content
    assert "run_outcome_class" in data_model_task.content
    assert "Pydantic BaseModel generation" in data_model_task.content
    assert 'class SectionInput(BaseModel): section_id: str;' in data_model_task.content
    assert '{"name":"section_id","type":"string","required":true}' in data_model_task.content
    assert 'Example output: {"modules":[{"name":"DGCEDataModel"' in data_model_task.content
    assert "Do not return only entities. Do not return only relationships. Do not return descriptive prose." in data_model_task.content
    assert "Prefer concrete schema detail over prose" in data_model_task.content


def test_api_surface_section_uses_contract_driven_generation_prompt():
    tasks = decompose_section(_api_surface_section())
    api_surface_task = tasks[2]

    assert "Produce a deterministic, implementation-ready API contract as structured JSON" in api_surface_task.content
    assert "Use top-level keys interfaces, methods, inputs, outputs, error_cases, and endpoints" in api_surface_task.content
    assert "Required lifecycle operations in stable order" in api_surface_task.content
    assert "preview, review, approval, preflight, gate, alignment, execution, status" in api_surface_task.content
    assert "status_contract" in api_surface_task.content
    assert "error_model" in api_surface_task.content
    assert "Avoid repeated suffixes like InterfaceInterface" in api_surface_task.content
    assert "Method names should exactly match the lifecycle operation names" in api_surface_task.content
    assert 'Example naming shape: {"interfaces":["PreviewService"]' in api_surface_task.content
    assert "Preserve Guardrail authority" in api_surface_task.content


def test_system_breakdown_section_uses_contract_driven_generation_prompt():
    tasks = decompose_section(_system_breakdown_section())
    system_breakdown_task = tasks[0]

    assert "Produce a deterministic, implementation-ready module contract and build graph as structured JSON" in system_breakdown_task.content
    assert "Use explicit top-level keys modules, build_graph, file_groups, implementation_units, tests, determinism_rules, and acceptance_criteria" in system_breakdown_task.content
    assert "Include deterministic file_groups" in system_breakdown_task.content
    assert "Each module must include name, layer, responsibility, typed inputs, typed outputs, anchored dependencies with name/kind/reference, governance_touchpoints, failure_modes, owned_paths, and implementation_order" in system_breakdown_task.content
    assert "Validation will fail if any module is missing dependencies, inputs, outputs, owned_paths, or implementation_order, or if build_graph.edges or tests is missing" in system_breakdown_task.content
    assert "Validation WILL FAIL if build_graph.edges is missing or empty. Validation WILL FAIL if any module is missing owned_paths." in system_breakdown_task.content
    assert "The modules array must always be present and non-empty. Every module MUST include a non-empty owned_paths array." in system_breakdown_task.content
    assert ".dce/input/, .dce/plans/, .dce/reviews/, .dce/approvals/, .dce/preflight/, .dce/execution/gate/, .dce/execution/alignment/, .dce/execution/stamps/, and .dce/outputs/" in system_breakdown_task.content
    assert "Represent request schema_fields as explicit field objects" in system_breakdown_task.content
    assert "for array fields use type=array plus items=string instead of array[string]" in system_breakdown_task.content
    assert "Add a top-level tests array" in system_breakdown_task.content
    assert "complete build_graph DAG with stable ordering for all declared producer/consumer dependencies" in system_breakdown_task.content
    assert "Include an explicit stale-check module owning .dce/preflight/{section_id}.stale_check.json" in system_breakdown_task.content
    assert 'Example top-level shape: {"modules":[{"name":"ExampleModule"' in system_breakdown_task.content
    assert 'Example module: {"name":"SectionInputHandler"' in system_breakdown_task.content
    assert "Do not emit modules or build_graph without these fields." in system_breakdown_task.content
    assert "Do not emit any module object without owned_paths." in system_breakdown_task.content
    assert "Do not fall back to generic architecture summaries or component prose. Emit only concrete contract fields that satisfy the validator." in system_breakdown_task.content
    assert "Avoid vague architecture prose" in system_breakdown_task.content


def test_system_breakdown_section_type_uses_contract_driven_generation_prompt():
    tasks = decompose_section(_typed_system_breakdown_section())
    system_breakdown_task = tasks[0]

    assert "Produce a deterministic, implementation-ready module contract and build graph as structured JSON" in system_breakdown_task.content
    assert "Use explicit top-level keys modules, build_graph, file_groups, implementation_units, tests, determinism_rules, and acceptance_criteria" in system_breakdown_task.content


def test_non_system_breakdown_section_type_does_not_inherit_rich_system_breakdown_prompt():
    section = DGCESection(
        section_type="system_component",
        title="Implementation Contract",
        description="Describe DGCE implementation notes without defining a system breakdown contract.",
    )

    tasks = decompose_section(section)
    system_breakdown_task = tasks[0]

    assert system_breakdown_task.content.endswith("Identify subsystems, interfaces, responsibilities, and build order.")
    assert "Use explicit top-level keys modules, build_graph, file_groups, implementation_units, tests, determinism_rules, and acceptance_criteria" not in system_breakdown_task.content
    assert "Include an explicit stale-check module owning .dce/preflight/{section_id}.stale_check.json" not in system_breakdown_task.content


def test_non_api_section_does_not_get_governance_endpoint_prompt_from_dgce_description():
    tasks = decompose_section(_dgce_non_api_section())
    api_surface_task = tasks[2]

    assert "Produce a deterministic, implementation-ready API contract as structured JSON" in api_surface_task.content
    assert "Required lifecycle operations in stable order" not in api_surface_task.content
    assert "preview, review, approval" not in api_surface_task.content
    assert "Preserve Guardrail authority" not in api_surface_task.content


def test_preflight_section_returns_deterministic_metadata():
    result = preflight_section(_section())

    assert result == {
        "section_id": "mission-board",
        "normalized_title": "mission-board",
    }


def test_preflight_section_prefers_explicit_section_id_over_title_slug():
    section = DGCESection(
        section_id="data-model",
        section_type="data_model",
        title="DGCE Core Data Model",
        description="Governed data model section.",
    )

    assert preflight_section(section) == {
        "section_id": "data-model",
        "normalized_title": "dgce-core-data-model",
    }


def test_preflight_section_rejects_missing_required_fields():
    invalid = DGCESection(
        section_type="game_system",
        title="  ",
        description="valid",
    )

    try:
        preflight_section(invalid)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_dgce_section_accepts_expected_targets():
    section = DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        expected_targets=["api/missionboardservice.py", "models/mission.py"],
    )

    assert section.expected_targets == ["api/missionboardservice.py", "models/mission.py"]


def test_run_section_uses_router_validated_data_model_payload_downstream(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "plan the system breakdown" in lowered:
            output = json.dumps(
                {
                    "modules": [
                        {
                            "name": "LifecycleCoordinator",
                            "layer": "DGCE Core",
                            "responsibility": "Coordinate lifecycle steps.",
                            "inputs": [],
                            "outputs": [],
                            "dependencies": [],
                            "governance_touchpoints": [],
                            "failure_modes": [],
                            "owned_paths": [".dce/input/"],
                            "implementation_order": 1,
                        }
                    ],
                    "build_graph": {"edges": [["LifecycleCoordinator", "LifecycleCoordinator"]]},
                    "tests": [],
                }
            )
        elif "data model" in lowered:
            output = json.dumps(
                {
                    "entities": [
                        {"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]},
                        {
                            "name": "PreviewArtifact",
                            "fields": [{"name": "artifact_fingerprint", "type": "string"}],
                        },
                    ],
                    "fields": ["section_id", "artifact_fingerprint"],
                    "relationships": ["SectionInput->PreviewArtifact"],
                    "validation_rules": ["section_id required"],
                }
            )
        elif "api surface" in lowered:
            output = json.dumps(
                {
                    "interfaces": ["DGCESectionGovernanceAPI"],
                    "methods": ["get_section_status"],
                    "inputs": ["section_id"],
                    "outputs": ["status"],
                    "error_cases": ["section_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    result = run_section(_data_model_section())

    data_model_response = next(response for response in result.responses if response.task_type == "data_model")

    assert data_model_response.structured_content is not None
    assert data_model_response.structured_content["modules"]
    assert json.loads(data_model_response.output)["modules"]
    assert any(entry["source"] == "data_model" for entry in result.file_plan.files)


def test_run_section_executes_without_errors(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_run")
    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    assert len(result.responses) == 4
    assert all(response.status for response in result.responses)
    assert all(response.output for response in result.responses)
    assert result.file_plan.project_name == "DGCE"


def test_run_section_returns_outputs_in_order(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_order")
    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    assert [response.task_type for response in result.responses] == [
        "system_breakdown",
        "data_model",
        "api_surface",
        "system_summary",
    ]


def test_run_section_continues_after_single_task_failure(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    class FailingPlanner:
        def route(self, task, classification):
            if task.task_type == "data_model":
                raise RuntimeError("route failure")
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": f"ok:{task.task_type}",
                    "reused": False,
                },
            )()

    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=FailingPlanner(),
    )

    assert len(result.responses) == 4
    assert result.responses[0].status != "error"
    assert result.responses[1].status == "error"
    assert result.responses[1].decision == "ERROR"
    assert result.responses[2].status != "error"
    assert result.responses[3].status != "error"


def test_system_breakdown_task_stores_structured_artifact_when_valid(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_structured_breakdown")

    def fake_run(self, executor_name, content):
        if "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "data model" in content.lower():
            output = json.dumps(
                {
                    "entities": ["mission"],
                    "fields": ["id", "state"],
                    "relationships": ["mission->player"],
                    "validation_rules": ["id required"],
                }
            )
        elif "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    with open(artifact_path, "r", encoding="utf-8") as f:
        artifacts = [json.loads(line) for line in f if line.strip()]

    stored = next(
        artifact for artifact in artifacts if artifact["artifact_id"].endswith("system-breakdown")
    )
    assert stored["context"]["structure_valid"] is True
    assert stored["structured_content"]["module_name"] == "mission_board"
    assert any(file_entry["path"] == "mission_board/service.py" for file_entry in result.file_plan.files)


def test_data_model_task_stores_structured_artifact_when_valid(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_structured_data_model")

    def fake_run(self, executor_name, content):
        if "data model" in content.lower():
            output = json.dumps(
                {
                    "entities": ["mission"],
                    "fields": ["id", "state"],
                    "relationships": ["mission->player"],
                    "validation_rules": ["id required"],
                }
            )
        elif "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    with open(artifact_path, "r", encoding="utf-8") as f:
        artifacts = [json.loads(line) for line in f if line.strip()]

    stored = next(
        artifact for artifact in artifacts if artifact["artifact_id"].endswith("data-model")
    )
    assert stored["context"]["structure_valid"] is True
    assert stored["structured_content"]["entities"] == ["mission"]
    assert any(file_entry["path"] == "models/mission.py" for file_entry in result.file_plan.files)


def test_api_surface_task_stores_structured_artifact_when_valid(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_structured_api_surface")

    def fake_run(self, executor_name, content):
        if "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        elif "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "data model" in content.lower():
            output = json.dumps(
                {
                    "entities": ["mission"],
                    "fields": ["id", "state"],
                    "relationships": ["mission->player"],
                    "validation_rules": ["id required"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    with open(artifact_path, "r", encoding="utf-8") as f:
        artifacts = [json.loads(line) for line in f if line.strip()]

    stored = next(
        artifact for artifact in artifacts if artifact["artifact_id"].endswith("api-surface")
    )
    assert stored["context"]["structure_valid"] is True
    assert stored["structured_content"]["interfaces"] == ["MissionBoardService"]
    assert any(file_entry["path"] == "api/missionboardservice.py" for file_entry in result.file_plan.files)


def test_invalid_structured_output_is_flagged_and_run_still_succeeds(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_structured_invalid")

    def fake_run(self, executor_name, content):
        if "data model" in content.lower():
            output = "not valid json"
        elif "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    assert len(result.responses) == 4
    assert result.responses[1].status == "error"

    with open(artifact_path, "r", encoding="utf-8") as f:
        artifacts = [json.loads(line) for line in f if line.strip()]

    stored = next(
        artifact for artifact in artifacts if artifact["artifact_id"].endswith("data-model")
    )
    assert stored["context"]["structure_valid"] is False
    assert stored["context"]["structure_error"] == "invalid_json"
    assert stored["structured_content"] is None
    assert all(file_entry["path"] != "models/mission.py" for file_entry in result.file_plan.files)


def test_missing_required_structured_keys_are_repaired_in_dgce_flow_when_backfill_applies(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_structured_missing_keys")

    def fake_run(self, executor_name, content):
        if "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "data model" in content.lower():
            output = json.dumps(
                {
                    "entities": ["Mission"],
                    "fields": ["id", "state"],
                }
            )
        elif "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_path),
    )
    result = run_section(
        _section(),
        classification_service=ClassificationService(),
        router_planner=planner,
    )

    assert len(result.responses) == 4
    assert result.responses[1].status == "experimental_output"
    assert result.responses[1].structured_content["relationships"] == []
    assert result.responses[1].structured_content["validation_rules"] == []
    assert any(file_entry["path"] == "models/mission.py" for file_entry in result.file_plan.files)

    with open(artifact_path, "r", encoding="utf-8") as f:
        artifacts = [json.loads(line) for line in f if line.strip()]

    stored = next(
        artifact for artifact in artifacts if artifact["artifact_id"].endswith("data-model")
    )
    assert stored["context"]["structure_valid"] is False
    assert stored["structured_content"] is None


def test_build_file_plan_is_deterministic_and_ignores_invalid_structured_responses():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "api_surface",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "interfaces": ["MissionBoardService"],
                        "methods": ["create_mission"],
                        "inputs": ["template_id"],
                        "outputs": ["mission_id"],
                        "error_cases": ["template_missing"],
                    }
                ),
            },
        )(),
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": "not json",
            },
        )(),
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "system_breakdown",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "module_name": "inventory",
                        "purpose": "manage inventory state",
                        "subcomponents": ["storage"],
                        "dependencies": ["save_state"],
                        "implementation_order": ["storage"],
                    }
                ),
            },
        )(),
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "entities": ["InventoryItem"],
                        "fields": ["item_id", "count"],
                        "relationships": ["item->inventory"],
                        "validation_rules": ["item_id required"],
                    }
                ),
            },
        )(),
    ]

    first = build_file_plan(responses)
    second = build_file_plan(list(reversed(responses)))

    assert first == second
    assert [file_entry["path"] for file_entry in first.files] == sorted(
        [file_entry["path"] for file_entry in first.files]
    )
    assert "inventory/service.py" in [file_entry["path"] for file_entry in first.files]
    assert "inventory/models.py" in [file_entry["path"] for file_entry in first.files]
    assert "models/inventoryitem.py" in [file_entry["path"] for file_entry in first.files]
    assert "api/missionboardservice.py" in [file_entry["path"] for file_entry in first.files]


def test_build_file_plan_data_model_entity_dict_uses_entity_name_not_description_for_filename():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "entities": [
                            {
                                "name": "AlignmentRecord",
                                "description": (
                                    "Alignment checks for SectionInput fields name storage_path fingerprint "
                                    "relationship validation invariants and execution linkage."
                                ),
                                "fields": [],
                            }
                        ],
                        "fields": ["section_id"],
                        "relationships": [],
                        "validation_rules": [],
                    }
                ),
            },
        )(),
    ]

    result = build_file_plan(responses)

    assert result.files == [
        {
            "path": "models/alignmentrecord.py",
            "purpose": "Data model for AlignmentRecord",
            "source": "data_model",
        }
    ]


def test_build_file_plan_data_model_filename_is_capped_and_windows_safe():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "entities": [
                            {
                                "name": "Alignment Record:*?<>| with spaces and separators "
                                + ("VeryLongName" * 10),
                                "description": "Long description should not affect filename.",
                                "fields": [],
                            }
                        ],
                        "fields": [],
                        "relationships": [],
                        "validation_rules": [],
                    }
                ),
            },
        )(),
    ]

    result = build_file_plan(responses)
    path = result.files[0]["path"]
    basename = path.removeprefix("models/").removesuffix(".py")

    assert path.startswith("models/")
    assert path.endswith(".py")
    assert len(basename) <= 64
    assert basename
    assert all(character.islower() or character.isdigit() or character == "_" for character in basename)


def test_build_file_plan_api_surface_filename_behavior_is_unchanged():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "api_surface",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "interfaces": ["MissionBoardService"],
                        "methods": ["create_mission"],
                        "inputs": ["template_id"],
                        "outputs": ["mission_id"],
                        "error_cases": ["template_missing"],
                    }
                ),
            },
        )(),
    ]

    result = build_file_plan(responses)

    assert result.files == [
        {
            "path": "api/missionboardservice.py",
            "purpose": "API interface for MissionBoardService",
            "source": "api_surface",
        }
    ]


def test_build_file_plan_normalizes_noisy_data_model_entity_names():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "entities": [
                            "description_alignment_record_for_a_section_input_fields_name_alignment_id_required_true_type_string"
                        ],
                        "fields": [],
                        "relationships": [],
                        "validation_rules": [],
                    }
                ),
            },
        )(),
    ]

    result = build_file_plan(responses)

    assert result.files == [
        {
            "path": "models/alignmentrecord.py",
            "purpose": "Data model for AlignmentRecord",
            "source": "data_model",
        }
    ]


def test_build_file_plan_normalizes_noisy_api_surface_interface_names():
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "api_surface",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "interfaces": [
                            "description_interface_for_managing_data_model_entities_in_dgce_name_datamodelservice",
                            "DataModelServiceInterfaceInterface",
                        ],
                        "methods": [],
                        "inputs": [],
                        "outputs": [],
                        "error_cases": [],
                    }
                ),
            },
        )(),
    ]

    result = build_file_plan(responses)

    assert result.files == [
        {
            "path": "api/datamodelservice.py",
            "purpose": "API interface for DataModelService",
            "source": "api_surface",
        }
    ]


def test_build_file_plan_recognizes_rich_system_breakdown_contract_shape():
    router = RouterPlanner()
    structured_payload = router._apply_dgce_system_breakdown_contract(
        {
            "modules": [
                {
                    "name": "PreviewGenerator",
                    "layer": "application",
                    "responsibility": "generate deterministic preview artifacts",
                    "inputs": [{"name": "section_input", "type": "SectionInputRequest"}],
                    "outputs": [{"name": "preview_artifact", "type": "PreviewArtifact"}],
                    "dependencies": [],
                    "governance_touchpoints": ["preview"],
                    "failure_modes": ["preview_generation_failed"],
                    "owned_paths": ["src/components/preview_generator.py"],
                    "implementation_order": 2,
                },
                {
                    "name": "SectionInputHandler",
                    "layer": "application",
                    "responsibility": "persist validated section input",
                    "inputs": [{"name": "raw_section_input", "type": "SectionInputRequest"}],
                    "outputs": [{"name": "section_input_record", "type": "SectionInput"}],
                    "dependencies": [],
                    "governance_touchpoints": ["input_capture"],
                    "failure_modes": ["input_validation_failed"],
                    "owned_paths": ["src/api/ingest.py"],
                    "implementation_order": 1,
                },
            ],
            "build_graph": {
                "type": "directed_acyclic_graph",
                "edges": [["SectionInputHandler", "PreviewGenerator"]],
            },
            "tests": [
                {
                    "name": "preview_generation_requires_validated_input",
                    "purpose": "Verify input persistence feeds preview generation.",
                    "targets": ["SectionInputHandler", "PreviewGenerator"],
                }
            ],
        }
    )
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "system_breakdown",
                "status": "experimental_output",
                "output": json.dumps(structured_payload),
                "structured_content": structured_payload,
            },
        )(),
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(
                    {
                        "entities": ["PreviewArtifact"],
                        "fields": ["artifact_id", "artifact_fingerprint"],
                        "relationships": ["preview->section_input"],
                        "validation_rules": ["artifact_id required"],
                    }
                ),
            },
        )(),
    ]

    first = build_file_plan(responses)
    second = build_file_plan(list(reversed(responses)))

    assert first == second
    assert [file_entry["path"] for file_entry in first.files] == sorted(
        [file_entry["path"] for file_entry in first.files]
    )
    assert "previewgenerator/models.py" in [file_entry["path"] for file_entry in first.files]
    assert "previewgenerator/service.py" in [file_entry["path"] for file_entry in first.files]
    assert "sectioninputhandler/models.py" in [file_entry["path"] for file_entry in first.files]
    assert "sectioninputhandler/service.py" in [file_entry["path"] for file_entry in first.files]
    assert "models/previewartifact.py" in [file_entry["path"] for file_entry in first.files]
    first_by_path = {entry["path"]: entry for entry in first.files}
    assert first_by_path["sectioninputhandler/service.py"]["file_group"] == {
        "module": "SectionInputHandler",
        "name": "sectioninputhandler",
        "placement": "sectioninputhandler",
    }
    assert first_by_path["sectioninputhandler/service.py"]["file_kind"] == "service"
    assert first_by_path["sectioninputhandler/service.py"]["module_contract"]["name"] == "SectionInputHandler"


def test_write_file_plan_creates_expected_paths_and_directories():
    output_dir = _scaffold_dir("dgce_scaffold_paths")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "inventory/service.py", "purpose": "Inventory service orchestration", "source": "system_breakdown"},
            {"path": "models/inventoryitem.py", "purpose": "Data model for InventoryItem", "source": "data_model"},
            {"path": "api/inventoryservice.py", "purpose": "API interface for InventoryService", "source": "api_surface"},
        ],
    )

    written = write_file_plan(file_plan, output_dir)

    assert written == [
        "inventory/service.py",
        "models/inventoryitem.py",
        "api/inventoryservice.py",
    ]
    assert (output_dir / "inventory" / "service.py").exists()
    assert (output_dir / "models" / "inventoryitem.py").exists()
    assert (output_dir / "api" / "inventoryservice.py").exists()


def test_write_file_plan_contents_are_deterministic():
    output_dir = _scaffold_dir("dgce_scaffold_contents")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "inventory/service.py", "purpose": "Inventory service orchestration", "source": "system_breakdown"},
        ],
    )

    write_file_plan(file_plan, output_dir)
    content = (output_dir / "inventory" / "service.py").read_text(encoding="utf-8")

    assert content == "\n".join(
        [
            "# Generated by Aether",
            "# Path: inventory/service.py",
            "# Purpose: Inventory service orchestration",
            "# Source: system_breakdown",
            "",
            '"""Service scaffold for Inventory service orchestration."""',
            "",
            "class InventoryService:",
            '    """Coordinates inventory service orchestration."""',
            "",
            "    pass",
            "",
        ]
    )


def test_generated_files_include_expected_header_block_and_docstrings():
    output_dir = _scaffold_dir("dgce_scaffold_headers")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "models/inventoryitem.py", "purpose": "Data model for InventoryItem", "source": "data_model"},
            {"path": "api/inventoryservice.py", "purpose": "API interface for InventoryService", "source": "api_surface"},
            {"path": "inventory/models.py", "purpose": "Inventory data structures", "source": "system_breakdown"},
        ],
    )

    write_file_plan(file_plan, output_dir)

    model_content = (output_dir / "models" / "inventoryitem.py").read_text(encoding="utf-8")
    api_content = (output_dir / "api" / "inventoryservice.py").read_text(encoding="utf-8")
    module_models_content = (output_dir / "inventory" / "models.py").read_text(encoding="utf-8")

    assert "# Generated by Aether" in model_content
    assert "# Path: models/inventoryitem.py" in model_content
    assert '"""Model scaffold for Data model for InventoryItem."""' in model_content
    assert "from pydantic import BaseModel" in model_content
    assert "class InventoryItem(BaseModel):" in model_content
    assert '"""Structured fallback model when entity schema details are unavailable."""' in model_content
    assert "raw_payload: dict[str, Any]" in model_content

    assert "# Path: api/inventoryservice.py" in api_content
    assert '"""API scaffold for API interface for InventoryService."""' in api_content
    assert "class Inventoryservice:" in api_content
    assert '"""Defines the api interface for inventoryservice."""' in api_content

    assert "# Path: inventory/models.py" in module_models_content
    assert '"""Model container scaffold for Inventory data structures."""' in module_models_content
    assert "class InventoryModel:" in module_models_content
    assert '"""Placeholder model for inventory data structures."""' in module_models_content


def test_render_file_entry_content_generates_structured_system_breakdown_scaffolds():
    service_content = render_file_entry_content(
        {
            "path": "sectioninputhandler/service.py",
            "purpose": "Persist section input artifacts. service orchestration",
            "source": "system_breakdown",
            "file_kind": "service",
            "file_group": {"module": "SectionInputHandler", "name": "sectioninputhandler", "placement": "sectioninputhandler"},
            "module_contract": {
                "name": "SectionInputHandler",
                "layer": "DGCE Core",
                "responsibility": "Persist section input artifacts.",
                "inputs": [{"name": "raw_section_input", "type": "SectionInputRequest"}],
                "outputs": [{"name": "section_input_record", "type": "SectionInput"}],
                "dependencies": [{"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"}],
                "owned_paths": [".dce/input/{section_id}.json"],
            },
        }
    )
    models_content = render_file_entry_content(
        {
            "path": "sectioninputhandler/models.py",
            "purpose": "Persist section input artifacts. data structures",
            "source": "system_breakdown",
            "file_kind": "models",
            "file_group": {"module": "SectionInputHandler", "name": "sectioninputhandler", "placement": "sectioninputhandler"},
            "module_contract": {
                "name": "SectionInputHandler",
                "layer": "DGCE Core",
                "responsibility": "Persist section input artifacts.",
                "inputs": [{"name": "raw_section_input", "type": "SectionInputRequest"}],
                "outputs": [{"name": "section_input_record", "type": "SectionInput"}],
                "dependencies": [{"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"}],
                "owned_paths": [".dce/input/{section_id}.json"],
            },
        }
    )

    assert "class SectionInputHandlerService:" in service_content
    assert "DEPENDENCIES = ('artifact_writer',)" in service_content
    assert '"""Execute one deterministic implementation unit."""' in service_content
    assert '"section_input_record": None' in service_content
    assert "MODULE_NAME = 'SectionInputHandler'" in models_content
    assert "INPUT_PORTS =" in models_content
    assert "OUTPUT_PORTS =" in models_content


def test_dgce_core_system_breakdown_full_circle_output_is_identical_across_repeated_runs():
    router = RouterPlanner()
    payload = {
        "modules": [
            {
                "name": "Review Manager",
                "layer": "DGCE Core",
                "responsibility": "Review section previews.",
                "inputs": [
                    {
                        "name": "preview_artifact",
                        "type": "artifact",
                        "artifact_path": ".dce/plans/{section_id}.preview.json",
                    }
                ],
                "outputs": [
                    {
                        "name": "review_artifact",
                        "type": "artifact",
                        "artifact_path": ".dce/reviews/{section_id}.review.md",
                    }
                ],
                "dependencies": [
                    {"name": "audit_writer", "kind": "module", "reference": "audit.py"},
                    {"name": "preview_loader", "kind": "module", "reference": "preview.py"},
                ],
                "governance_touchpoints": ["review"],
                "failure_modes": ["review_missing"],
                "owned_paths": [".dce/reviews/{section_id}.review.md"],
                "implementation_order": 2,
            },
            {
                "name": "SectionInputHandler",
                "layer": "DGCE Core",
                "responsibility": "Persist section input artifacts.",
                "inputs": [
                    {
                        "name": "raw_section_input",
                        "type": "SectionInputRequest",
                        "schema_fields": [
                            {"name": "section_id", "type": "string", "required": True},
                        ],
                    }
                ],
                "outputs": [
                    {
                        "name": "SectionInput",
                        "type": "artifact",
                        "artifact_path": ".dce/input/{section_id}.json",
                    }
                ],
                "dependencies": [
                    {"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"},
                ],
                "governance_touchpoints": ["input validation"],
                "failure_modes": ["invalid input structure"],
                "owned_paths": [".dce/input/{section_id}.json"],
                "implementation_order": 1,
            },
            {
                "name": "StaleCheckEvaluator",
                "layer": "DGCE Core",
                "responsibility": "Evaluate staleness before execution.",
                "inputs": [
                    {
                        "name": "section_input",
                        "type": "artifact",
                        "artifact_path": ".dce/input/{section_id}.json",
                    }
                ],
                "outputs": [
                    {
                        "name": "stale_check_artifact",
                        "type": "artifact",
                        "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
                    }
                ],
                "dependencies": [
                    {"name": "SectionInputHandler", "kind": "module", "reference": "SectionInputHandler"},
                ],
                "governance_touchpoints": ["stale_check"],
                "failure_modes": ["stale_check_failed"],
                "owned_paths": [".dce/preflight/{section_id}.stale_check.json"],
                "implementation_order": 3,
            },
        ],
        "build_graph": {
            "edges": [
                ["SectionInputHandler", "Review Manager"],
                ["SectionInputHandler", "StaleCheckEvaluator"],
            ]
        },
        "tests": [{"name": "build_graph_is_complete"}],
    }
    request = ClassificationRequest(
        content="Structured output for dgce_system_breakdown_v1",
        request_id="repeatable-dgce-core-system-breakdown-full-circle",
        output_contract=OutputContract(mode="structured", schema_name="dgce_system_breakdown_v1"),
        metadata={"section_type": "system_component", "task_subtype": "system_breakdown"},
    )

    def _run_full_circle() -> tuple[dict, object, dict[str, str]]:
        _, structured = router._validate_structured_output(request, json.dumps(payload))
        response = ResponseEnvelope(
            request_id="full-circle-system-breakdown",
            task_type="system_breakdown",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured),
            reused=False,
            structured_content=structured,
        )
        file_plan = build_file_plan([response])
        rendered_by_path = {
            entry["path"]: render_file_entry_content(entry)
            for entry in file_plan.files
        }
        return structured, file_plan, rendered_by_path

    structured_first, file_plan_first, rendered_first = _run_full_circle()
    structured_second, file_plan_second, rendered_second = _run_full_circle()

    assert structured_first == structured_second
    assert file_plan_first == file_plan_second
    assert rendered_first == rendered_second
    assert [module["name"] for module in structured_first["modules"]] == [
        "SectionInputHandler",
        "Review Manager",
        "StaleCheckEvaluator",
    ]
    assert [group["name"] for group in structured_first["file_groups"]] == [
        "sectioninputhandler",
        "review_manager",
        "stalecheckevaluator",
    ]
    assert [unit["name"] for unit in structured_first["implementation_units"]] == [
        "implement_sectioninputhandler",
        "implement_review_manager",
        "implement_stalecheckevaluator",
    ]
    assert list(rendered_first) == [
        "review_manager/models.py",
        "review_manager/service.py",
        "sectioninputhandler/models.py",
        "sectioninputhandler/service.py",
        "stalecheckevaluator/models.py",
        "stalecheckevaluator/service.py",
    ]
    assert "DEPENDENCIES = ('audit_writer', 'preview_loader')" in rendered_first["review_manager/service.py"]
    assert "MODULE_NAME = 'SectionInputHandler'" in rendered_first["sectioninputhandler/models.py"]
    assert "OWNED_PATHS = ('.dce/preflight/{section_id}.stale_check.json',)" in rendered_first["stalecheckevaluator/service.py"]


def test_build_file_plan_carries_structured_data_model_schema_for_runtime_generation():
    structured_payload = {
        "modules": [
            {
                "name": "DGCEDataModel",
                "entities": ["SectionInput", "PreviewArtifact"],
                "relationships": ["SectionInput->PreviewArtifact"],
                "required": ["section_id"],
                "identity_keys": ["section_id"],
            }
        ],
        "entities": [
            {
                "name": "SectionInput",
                "identity_keys": ["section_id"],
                "fields": [
                    {"name": "section_id", "type": "string", "required": True},
                    {"name": "content", "type": "object", "required": False},
                ],
            },
            {
                "name": "PreviewArtifact",
                "identity_keys": ["artifact_fingerprint"],
                "fields": [
                    {"name": "artifact_fingerprint", "type": "string", "required": True},
                ],
            },
        ],
        "fields": ["section_id", "content", "artifact_fingerprint"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }
    responses = [
        type(
            "ResponseEnvelopeLike",
            (),
            {
                "task_type": "data_model",
                "status": "experimental_output",
                "output": json.dumps(structured_payload),
                "structured_content": structured_payload,
            },
        )(),
    ]

    result = build_file_plan(responses)

    assert result.files == [
        {
            "path": "models/previewartifact.py",
            "purpose": "Data model for PreviewArtifact",
            "source": "data_model",
            "entity_schema": {
                "name": "PreviewArtifact",
                "identity_keys": ["artifact_fingerprint"],
                "fields": [
                    {"name": "artifact_fingerprint", "required": True, "type": "string"},
                ],
                "invariants": [],
            },
            "entity_relationships": [],
            "known_entity_names": ["PreviewArtifact", "SectionInput"],
        },
        {
            "path": "models/sectioninput.py",
            "purpose": "Data model for SectionInput",
            "source": "data_model",
            "entity_schema": {
                "name": "SectionInput",
                "identity_keys": ["section_id"],
                "fields": [
                    {"name": "content", "required": False, "type": "object"},
                    {"name": "section_id", "required": True, "type": "string"},
                ],
                "invariants": [],
            },
            "entity_relationships": [
                {"from_entity": "SectionInput", "relationship_type": "references", "to_entity": "PreviewArtifact"},
            ],
            "known_entity_names": ["PreviewArtifact", "SectionInput"],
        },
    ]


def test_render_file_entry_content_generates_pydantic_model_for_structured_data_model_entity():
    content = render_file_entry_content(
        {
            "path": "models/sectioninput.py",
            "purpose": "Data model for SectionInput",
            "source": "data_model",
            "entity_schema": {
                "name": "SectionInput",
                "identity_keys": ["section_id"],
                "fields": [
                    {"name": "section_id", "type": "string", "required": True},
                    {"name": "content", "type": "object", "required": False},
                ],
                "invariants": [],
            },
            "entity_relationships": [
                {"from_entity": "SectionInput", "to_entity": "PreviewArtifact", "relationship_type": "references"},
            ],
            "known_entity_names": ["PreviewArtifact", "SectionInput"],
        }
    )

    assert "# Path: models/sectioninput.py" in content
    assert "from __future__ import annotations" in content
    assert "from typing import Any" in content
    assert "from pydantic import BaseModel" in content
    assert "class SectionInput(BaseModel):" in content
    assert "section_id: str" in content
    assert "content: dict[str, Any] | None = None" in content
    assert "preview_artifact: PreviewArtifact | None = None" in content
    assert content.index('"""Pydantic model for SectionInput."""') < content.index("from __future__ import annotations")


def test_render_file_entry_content_normalizes_data_model_field_types_deterministically():
    content = render_file_entry_content(
        {
            "path": "models/outputartifact.py",
            "purpose": "Data model for OutputArtifact",
            "source": "data_model",
            "entity_schema": {
                "name": "OutputArtifact",
                "identity_keys": ["section_id"],
                "fields": [
                    {"name": "metadata", "type": "json", "required": False},
                    {"name": "payload", "type": "dict[str, Any]", "required": False},
                    {"name": "section_id", "type": "str", "required": True},
                    {"name": "status", "type": "string", "required": True},
                ],
                "invariants": [],
            },
            "entity_relationships": [],
            "known_entity_names": ["OutputArtifact"],
        }
    )

    assert "section_id: str" in content
    assert "status: str" in content
    assert "metadata: dict[str, Any] | None = None" in content
    assert "payload: dict[str, Any] | None = None" in content
    assert "Any | None = None" not in content


def test_render_file_entry_content_supports_datetime_and_enum_literals():
    content = render_file_entry_content(
        {
            "path": "models/executionstamp.py",
            "purpose": "Data model for ExecutionStamp",
            "source": "data_model",
            "entity_schema": {
                "name": "ExecutionStamp",
                "identity_keys": ["section_id"],
                "fields": [
                    {"name": "recorded_at", "type": "datetime", "required": True},
                    {"name": "status", "type": "string", "required": True, "enum_values": ["pending", "approved", "rejected"]},
                ],
                "invariants": [],
            },
            "entity_relationships": [],
            "known_entity_names": ["ExecutionStamp"],
        }
    )

    assert "from datetime import datetime" in content
    assert "from typing import Literal" in content
    assert "recorded_at: datetime" in content
    assert "status: Literal['pending', 'approved', 'rejected']" in content


def test_render_file_entry_content_uses_precise_fallback_class_name_for_sectioninput():
    content = render_file_entry_content(
        {
            "path": "models/sectioninput.py",
            "purpose": "Data model for SectionInput",
            "source": "data_model",
        }
    )

    assert "class SectionInput(BaseModel):" in content


def test_dgce_core_data_model_file_plan_and_rendering_are_identical_across_repeated_repairs():
    router = RouterPlanner()
    payload = {
        "entities": [
            {
                "name": "SectionInput",
                "fields": [{"name": "section_id", "type": "string", "required": True}],
            },
            {
                "name": "ApprovalArtifact",
                "fields": [{"name": "section_id", "type": "string", "required": True}],
            },
        ],
        "fields": ["section_id"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }
    request = ClassificationRequest(
        content="Structured output for dgce_data_model_v1",
        request_id="repeatable-dgce-core-data-model",
        output_contract=OutputContract(mode="structured", schema_name="dgce_data_model_v1"),
        metadata={"section_type": "data_model"},
    )
    _, structured_first = router._validate_structured_output(request, json.dumps(payload))
    _, structured_second = router._validate_structured_output(request, json.dumps(payload))

    responses_first = [
        ResponseEnvelope(
            request_id="first",
            task_type="data_model",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured_first),
            reused=False,
            structured_content=structured_first,
        )
    ]
    responses_second = [
        ResponseEnvelope(
            request_id="second",
            task_type="data_model",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured_second),
            reused=False,
            structured_content=structured_second,
        )
    ]

    file_plan_first = build_file_plan(responses_first)
    file_plan_second = build_file_plan(responses_second)

    assert file_plan_first == file_plan_second

    first_by_path = {entry["path"]: entry for entry in file_plan_first.files}
    second_by_path = {entry["path"]: entry for entry in file_plan_second.files}
    assert first_by_path["models/sectioninput.py"]["entity_schema"] == second_by_path["models/sectioninput.py"]["entity_schema"]
    assert first_by_path["models/approvalartifact.py"]["entity_schema"] == second_by_path["models/approvalartifact.py"]["entity_schema"]
    assert render_file_entry_content(first_by_path["models/sectioninput.py"]) == render_file_entry_content(
        second_by_path["models/sectioninput.py"]
    )
    assert render_file_entry_content(first_by_path["models/approvalartifact.py"]) == render_file_entry_content(
        second_by_path["models/approvalartifact.py"]
    )


def test_dgce_core_api_surface_file_plan_and_rendering_are_identical_across_repeated_repairs():
    router = RouterPlanner()
    payload = {
        "interfaces": ["preview service"],
        "methods": {
            "generate preview": {
                "method": "POST",
                "path": "/sections/{section_id}/preview",
            }
        },
        "inputs": {},
        "outputs": {},
        "error_cases": {},
    }
    request = ClassificationRequest(
        content="Structured output for dgce_api_surface_v1",
        request_id="repeatable-dgce-core-api-surface",
        output_contract=OutputContract(mode="structured", schema_name="dgce_api_surface_v1"),
        metadata={"section_type": "api_surface", "task_subtype": "api_surface"},
    )
    _, structured_first = router._validate_structured_output(request, json.dumps(payload))
    _, structured_second = router._validate_structured_output(request, json.dumps(payload))

    responses_first = [
        ResponseEnvelope(
            request_id="first-api",
            task_type="api_surface",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured_first),
            reused=False,
            structured_content=structured_first,
        )
    ]
    responses_second = [
        ResponseEnvelope(
            request_id="second-api",
            task_type="api_surface",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured_second),
            reused=False,
            structured_content=structured_second,
        )
    ]

    file_plan_first = build_file_plan(responses_first)
    file_plan_second = build_file_plan(responses_second)

    assert file_plan_first == file_plan_second

    first_by_path = {entry["path"]: entry for entry in file_plan_first.files}
    second_by_path = {entry["path"]: entry for entry in file_plan_second.files}
    assert first_by_path["api/previewservice.py"]["interface_schema"] == second_by_path["api/previewservice.py"]["interface_schema"]
    assert first_by_path["api/approvalservice.py"]["interface_schema"] == second_by_path["api/approvalservice.py"]["interface_schema"]
    assert render_file_entry_content(first_by_path["api/previewservice.py"]) == render_file_entry_content(
        second_by_path["api/previewservice.py"]
    )
    assert render_file_entry_content(first_by_path["api/approvalservice.py"]) == render_file_entry_content(
        second_by_path["api/approvalservice.py"]
    )


def test_dgce_core_api_surface_full_circle_output_is_identical_across_repeated_runs():
    router = RouterPlanner()
    payload = {
        "interfaces": ["preview service"],
        "methods": {
            "generate preview": {
                "method": "POST",
                "path": "/sections/{section_id}/preview",
            }
        },
        "inputs": {},
        "outputs": {},
        "error_cases": {},
    }
    request = ClassificationRequest(
        content="Structured output for dgce_api_surface_v1",
        request_id="repeatable-dgce-core-api-surface-full-circle",
        output_contract=OutputContract(mode="structured", schema_name="dgce_api_surface_v1"),
        metadata={"section_type": "api_surface", "task_subtype": "api_surface"},
    )

    def _run_full_circle() -> tuple[dict, object, dict[str, str]]:
        _, structured = router._validate_structured_output(request, json.dumps(payload))
        response = ResponseEnvelope(
            request_id="full-circle-api",
            task_type="api_surface",
            status="experimental_output",
            task_bucket="planning",
            decision="MID_MODEL",
            output=json.dumps(structured),
            reused=False,
            structured_content=structured,
        )
        file_plan = build_file_plan([response])
        rendered_by_path = {
            entry["path"]: render_file_entry_content(entry)
            for entry in file_plan.files
        }
        return structured, file_plan, rendered_by_path

    structured_first, file_plan_first, rendered_first = _run_full_circle()
    structured_second, file_plan_second, rendered_second = _run_full_circle()

    assert structured_first == structured_second
    assert file_plan_first == file_plan_second
    assert rendered_first == rendered_second
    assert list(rendered_first) == [
        "api/alignmentservice.py",
        "api/approvalservice.py",
        "api/executionservice.py",
        "api/gateservice.py",
        "api/preflightservice.py",
        "api/previewservice.py",
        "api/reviewservice.py",
        "api/statusservice.py",
    ]
    assert "@router.post(\"/preview\", response_model=PreviewResponse)" in rendered_first["api/previewservice.py"]
    assert "@router.get(\"/status/{section_id}\", response_model=StatusResponse)" in rendered_first["api/statusservice.py"]
    assert "class ApiError(BaseModel):" in rendered_first["api/statusservice.py"]


def test_render_file_entry_content_generates_structured_api_service_from_interface_schema():
    content = render_file_entry_content(
        {
            "path": "api/previewservice.py",
            "purpose": "API interface for PreviewService",
            "source": "api_surface",
            "interface_schema": {
                "name": "PreviewService",
                "methods": [
                    {
                        "name": "preview",
                        "method": "POST",
                        "operation_name": "preview",
                        "path": "/preview",
                        "request_schema": "PreviewRequest",
                        "response_schema": "PreviewResponse",
                        "error_schema": "ApiError",
                        "input": {"section_id": "string"},
                        "output": {"section_id": "string", "status": "string", "preview_path": "string"},
                        "error_cases": ["section_missing", "invalid_preview_request"],
                    }
                ],
                "schemas": {
                    "PreviewRequest": {
                        "fields": [
                            {"name": "section_id", "type": "string", "required": True},
                        ]
                    },
                    "PreviewResponse": {
                        "fields": [
                            {"name": "section_id", "type": "string", "required": True},
                            {"name": "status", "type": "string", "required": True},
                            {"name": "preview_path", "type": "string", "required": True},
                        ]
                    },
                    "ApiError": {
                        "fields": [
                            {"name": "code", "type": "string", "required": True},
                            {"name": "message", "type": "string", "required": True},
                            {"name": "section_id", "type": "string", "required": False},
                        ]
                    },
                },
            },
        }
    )

    assert '"""Service contract for PreviewService."""' in content
    assert "from fastapi import APIRouter" in content
    assert "class PreviewRequest(BaseModel):" in content
    assert "class PreviewResponse(BaseModel):" in content
    assert "@router.post(\"/preview\", response_model=PreviewResponse)" in content
    assert "def preview(payload: PreviewRequest) -> PreviewResponse:" in content
    assert "# Errors use ApiError: section_missing, invalid_preview_request" in content
    assert "section_id=payload.section_id" in content
    assert 'status="pending"' in content
    assert 'preview_path=""' in content


def test_expected_targets_model_scaffold_is_purpose_and_type_aware():
    content = render_file_entry_content(
        {
            "path": "src/models/anomaly_record.py",
            "purpose": "Store anomaly detection results",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "# Generated by Aether" in content
    assert "# Path: src/models/anomaly_record.py" in content
    assert "# Purpose: Store anomaly detection results" in content
    assert "# Source: expected_targets" in content
    assert "from dataclasses import dataclass" in content
    assert "class AnomalyRecord:" in content
    assert "# TODO: replace placeholder fields with typed model attributes." in content
    assert "def validate(self) -> None:" in content


def test_expected_targets_component_scaffold_is_purpose_and_type_aware():
    content = render_file_entry_content(
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "class AnomalyClassifier:" in content
    assert "def classify(self, observation: dict, expected_state: dict) -> dict:" in content
    assert 'return {"anomaly_type": "unknown", "confidence": 0.0}' in content


def test_expected_targets_class_name_normalization_handles_rsobservation_and_snake_case():
    rso_content = render_file_entry_content(
        {
            "path": "src/models/rsobservation.py",
            "purpose": "Represent a resident-space observation",
            "source": "expected_targets",
            "requirements": [],
        }
    )
    normalizer_content = render_file_entry_content(
        {
            "path": "src/components/observation_normalizer.py",
            "purpose": "Normalize orbital observations",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "class RSObservation:" in rso_content
    assert "class ObservationNormalizer:" in normalizer_content


def test_expected_targets_test_file_scaffold_uses_pytest_shape():
    content = render_file_entry_content(
        {
            "path": "tests/test_anomaly_classifier.py",
            "purpose": "Verify anomaly classifier behavior",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "import pytest" in content
    assert "def test_anomaly_classifier() -> None:" in content
    assert "assert True" in content


def test_expected_targets_ingest_api_scaffold_uses_structured_ingestion_flow():
    content = render_file_entry_content(
        {
            "path": "src/api/ingest.py",
            "purpose": "Ingest normalized observations into the anomaly pipeline",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "def ingest_observation(payload: dict) -> dict:" in content
    assert "normalized = payload  # placeholder" in content
    assert "expected_state = {}" in content
    assert "deviation_score = 0.0" in content
    assert '"anomaly_type": "unknown"' in content
    assert '"confidence": 0.0' in content
    assert '"deviation_score": deviation_score' in content
    assert "return result" in content


def test_expected_targets_review_api_scaffold_contains_guard_and_return_keys():
    content = render_file_entry_content(
        {
            "path": "src/api/review.py",
            "purpose": "Review anomaly records in the operator workflow",
            "source": "expected_targets",
            "requirements": [],
        }
    )

    assert "def review_anomaly(anomaly_record: dict, status: str) -> dict:" in content
    assert 'if status not in {"approved", "rejected"}:' in content
    assert 'raise ValueError("status must be approved or rejected")' in content
    assert "return {" in content
    assert 'updated_record["status"] = status' in content
    assert '"anomaly_id": updated_record.get("anomaly_id", "")' in content
    assert '"status": status' in content
    assert '"updated_record": updated_record' in content


def test_expected_targets_rsobservation_uses_requirement_fields_in_model_scaffold():
    content = render_file_entry_content(
        {
            "path": "src/models/rsobservation.py",
            "purpose": "Represent normalized orbital observations",
            "source": "expected_targets",
            "requirements": [
                "must be a dataclass",
                "must include typed fields: object_id, timestamp, position_eci, velocity_eci, sensor_source, data_quality",
                "must include validate()",
                "Define an RSObservation model with object_id, timestamp, position_eci, velocity_eci, sensor_source, and data_quality",
                "Define an AnomalyRecord model with anomaly_id, observation_id, object_id, anomaly_type, deviation_score, confidence, status, guardrail_cleared, operator_review_required, reason_codes, and reasoning",
            ],
        }
    )

    assert "@dataclass" in content
    assert "class RSObservation:" in content
    assert 'object_id: str = ""' in content
    assert 'timestamp: str = ""' in content
    assert "position_eci: tuple[float, float, float] = (0.0, 0.0, 0.0)" in content
    assert "velocity_eci: tuple[float, float, float] = (0.0, 0.0, 0.0)" in content
    assert "def validate(self) -> None:" in content
    assert 'raise ValueError("object_id is required")' in content
    assert "anomaly_id:" not in content
    assert "status:" not in content


def test_expected_targets_anomaly_record_includes_status_and_mark_reviewed():
    content = render_file_entry_content(
        {
            "path": "src/models/anomaly_record.py",
            "purpose": "Persist anomaly review workflow state",
            "source": "expected_targets",
            "requirements": [
                "Define an RSObservation model with object_id, timestamp, position_eci, velocity_eci, sensor_source, and data_quality",
                "Define an AnomalyRecord model with anomaly_id, observation_id, object_id, status, confidence",
                "status values: pending, approved, rejected",
                "must include validate()",
            ],
        }
    )

    assert "class AnomalyRecord:" in content
    assert 'status: str = "pending"' in content
    assert "pending, approved, rejected" in content
    assert 'if self.status not in {"pending", "approved", "rejected"}:' in content
    assert "def mark_reviewed(self, status: str) -> None:" in content
    assert 'if status not in {"approved", "rejected"}:' in content
    assert 'raise ValueError("status must be approved or rejected")' in content
    assert "self.status = status" in content
    assert "timestamp:" not in content
    assert "position_eci:" not in content


def test_expected_targets_classifier_signature_uses_requirement_shape():
    content = render_file_entry_content(
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
                "return structure should include type and confidence",
            ],
        }
    )

    assert "def classify(self, observation: dict, expected_state: dict) -> dict:" in content
    assert "anomaly_type should be one of RPO, maneuver, conjunction, or unknown" in content
    assert 'return {"anomaly_type": "unknown", "confidence": 0.0}' in content


def test_expected_targets_guardrail_gateway_uses_policy_method_shape():
    content = render_file_entry_content(
        {
            "path": "src/components/guardrail_gateway.py",
            "purpose": "Enforce Guardrail policy before escalation or outbound action",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
            ],
        }
    )

    assert "class GuardrailGateway:" in content
    assert "def enforce_policy(self, anomaly_record: dict) -> bool:" in content
    assert "Enforce Guardrail policy before escalation or outbound action" in content
    assert "return True" in content
    assert "def classify(" not in content


def test_expected_targets_observation_normalizer_uses_normalize_method_shape():
    content = render_file_entry_content(
        {
            "path": "src/components/observation_normalizer.py",
            "purpose": "Normalize raw orbital observation data into a standard format",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
            ],
        }
    )

    assert "class ObservationNormalizer:" in content
    assert "def normalize_observation(self, raw_data: dict) -> dict:" in content
    assert "Normalize raw orbital observation data into a standard structure." in content
    assert "return dict(raw_data)" in content
    assert "def classify(" not in content


def test_expected_targets_orbit_propagator_uses_propagation_method_shape():
    content = render_file_entry_content(
        {
            "path": "src/components/orbit_propagator.py",
            "purpose": "Estimate expected motion of RSOs from normalized observations",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
            ],
        }
    )

    assert "class OrbitPropagator:" in content
    assert "def propagate_orbit(self, normalized_observation: dict, time_step: float) -> dict:" in content
    assert "Estimate expected motion for a normalized observation." in content
    assert '"expected_position_eci": normalized_observation.get("position_eci", (0.0, 0.0, 0.0))' in content
    assert '"expected_velocity_eci": normalized_observation.get("velocity_eci", (0.0, 0.0, 0.0))' in content
    assert '"time_step": time_step' in content
    assert "def classify(" not in content


def test_expected_targets_deviation_scorer_uses_scoring_method_shape():
    content = render_file_entry_content(
        {
            "path": "src/components/deviation_scorer.py",
            "purpose": "Score deviations between observed and expected motion",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
            ],
        }
    )

    assert "class DeviationScorer:" in content
    assert "def score_deviation(self, observed_state: dict, expected_state: dict) -> float:" in content
    assert "Compute a deviation score between observed and expected state." in content
    assert "# Placeholder: simple magnitude difference proxy" in content
    assert "return 0.0" in content
    assert "def classify(" not in content


def test_expected_targets_review_queue_manager_uses_queue_review_methods():
    content = render_file_entry_content(
        {
            "path": "src/components/review_queue_manager.py",
            "purpose": "Queue anomalies for operator review and track review state",
            "source": "expected_targets",
            "requirements": [
                "must expose classify(observation, expected_state)",
            ],
        }
    )

    assert "class ReviewQueueManager:" in content
    assert "def submit_for_review(self, anomaly_record: dict) -> dict:" in content
    assert '"anomaly_id": anomaly_record.get("anomaly_id", "")' in content
    assert '"queued": True' in content
    assert "def update_review_status(self, anomaly_id: str, status: str) -> dict:" in content
    assert '"anomaly_id": anomaly_id' in content
    assert '"status": status' in content
    assert "def classify(" not in content


def test_same_file_plan_produces_identical_contents():
    first_dir = _scaffold_dir("dgce_scaffold_repeat_a")
    second_dir = _scaffold_dir("dgce_scaffold_repeat_b")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "inventory/service.py", "purpose": "Inventory service orchestration", "source": "system_breakdown"},
        ],
    )

    write_file_plan(file_plan, first_dir)
    write_file_plan(file_plan, second_dir)

    first_content = (first_dir / "inventory" / "service.py").read_text(encoding="utf-8")
    second_content = (second_dir / "inventory" / "service.py").read_text(encoding="utf-8")

    assert first_content == second_content
    assert "202" not in first_content


def test_write_file_plan_handles_existing_file_collision_cleanly():
    output_dir = _scaffold_dir("dgce_scaffold_collision")
    target = output_dir / "inventory" / "service.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "inventory/service.py", "purpose": "Inventory service orchestration", "source": "system_breakdown"},
        ],
    )

    try:
        write_file_plan(file_plan, output_dir)
        assert False, "Expected FileExistsError"
    except FileExistsError:
        pass


def test_write_file_plan_allows_explicit_overwrite_paths_only():
    output_dir = _scaffold_dir("dgce_scaffold_safe_modify_overwrite")
    target = output_dir / "inventory" / "service.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "inventory/service.py", "purpose": "Inventory service orchestration", "source": "system_breakdown"},
        ],
    )

    written = write_file_plan(file_plan, output_dir, overwrite_paths={"inventory/service.py"})

    assert written == ["inventory/service.py"]
    assert (output_dir / "inventory" / "service.py").read_text(encoding="utf-8") != "existing"


def test_write_file_plan_rejects_paths_outside_output_dir():
    output_dir = _scaffold_dir("dgce_scaffold_escape")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "../escape.py", "purpose": "Escape attempt", "source": "system_breakdown"},
        ],
    )

    try:
        write_file_plan(file_plan, output_dir)
        assert False, "Expected ValueError"
    except ValueError:
        pass
    assert not (output_dir.parent / "escape.py").exists()


def test_write_file_plan_rejects_absolute_paths():
    output_dir = _scaffold_dir("dgce_scaffold_absolute_escape")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": str(Path("/tmp/escape.py")), "purpose": "Absolute escape attempt", "source": "system_breakdown"},
        ],
    )

    try:
        write_file_plan(file_plan, output_dir)
        assert False, "Expected ValueError"
    except ValueError:
        pass

    assert not output_dir.exists() or not any(output_dir.rglob("*"))


def test_run_section_and_write_returns_written_files(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    output_dir = _scaffold_dir("dgce_scaffold_run")

    def fake_run(self, executor_name, content):
        if "system breakdown" in content.lower():
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": ["templates", "tracker"],
                    "dependencies": ["save_state"],
                    "implementation_order": ["templates", "tracker"],
                }
            )
        elif "data model" in content.lower():
            output = json.dumps(
                {
                    "entities": ["Mission"],
                    "fields": ["id", "state"],
                    "relationships": ["mission->player"],
                    "validation_rules": ["id required"],
                }
            )
        elif "api surface" in content.lower():
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
            )
        else:
            output = "Summary output"

        return ExecutionResult(
            output=output,
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

    result = run_section_and_write(_section(), output_dir)

    assert "mission_board/service.py" in result.written_files
    assert "models/mission.py" in result.written_files
    assert "api/missionboardservice.py" in result.written_files
    assert (output_dir / "mission_board" / "service.py").exists()


def test_run_section_with_workspace_creates_dce_structure_and_input_file(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_structure")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    section_id = preflight_section(_section())["section_id"]
    dce_root = project_root / ".dce"

    assert result.written_files
    assert not any(path.startswith(".dce") for path in result.written_files)
    assert dce_root.exists()
    assert (dce_root / "input").is_dir()
    assert (dce_root / "plans").is_dir()
    assert (dce_root / "outputs").is_dir()
    assert (dce_root / "state").is_dir()
    assert (dce_root / "index.yaml").exists()

    persisted_input = json.loads((dce_root / "input" / f"{section_id}.json").read_text(encoding="utf-8"))
    advisory_index = json.loads((dce_root / "advisory_index.json").read_text(encoding="utf-8"))
    ownership_index = json.loads((dce_root / "ownership_index.json").read_text(encoding="utf-8"))
    assert persisted_input == _section().model_dump()
    assert result.run_mode == "create_only"
    assert result.run_outcome_class == "success_create_only"
    assert "mission_board/service.py" in result.written_files
    assert (project_root / "mission_board" / "service.py").exists()
    assert not (dce_root / "mission_board" / "service.py").exists()
    assert (dce_root / "index.yaml").read_text(encoding="utf-8") == "sections:\n  - mission-board\n"
    assert result.execution_outcome == {
        "section_id": "mission-board",
        "stage": "WRITE",
        "status": "success",
        "validation_summary": {
            "ok": True,
            "error": None,
            "missing_keys": [],
        },
        "change_plan_summary": {
            "create_count": 4,
            "modify_count": 0,
            "ignore_count": 0,
        },
        "execution_summary": {
            "written_files_count": 4,
            "skipped_modify_count": 0,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 0,
            "skipped_exists_fallback_count": 0,
        },
    }
    assert result.advisory is None
    assert advisory_index == {
        "run_outcome_class": "success_create_only",
        "run_mode": "create_only",
        "section_id": "mission-board",
        "status": "success",
        "validation_ok": True,
        "advisory_type": None,
        "advisory_explanation": None,
        "written_files_count": 4,
        "skipped_modify_count": 0,
        "skipped_ignore_count": 0,
    }
    assert ownership_index == result.ownership_index


def test_run_section_with_workspace_plan_file_contains_tasks(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_plan")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root)
    section_id = preflight_section(_section())["section_id"]
    plan = json.loads((project_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert len(plan) == 4
    assert [entry["task_type"] for entry in plan] == [
        "system_breakdown",
        "data_model",
        "api_surface",
        "system_summary",
    ]
    assert [entry["task_bucket"] for entry in plan] == [
        "planning",
        "code_routine",
        "code_routine",
        "planning",
    ]
    assert all(sorted(entry.keys()) == ["status", "task_bucket", "task_id", "task_type"] for entry in plan)
    assert all(entry["status"] == "completed" for entry in plan)


def test_run_section_with_workspace_data_model_plan_excludes_system_breakdown(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_data_model_plan")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_data_model_section(), project_root)
    section_id = preflight_section(_data_model_section())["section_id"]
    plan = json.loads((project_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert [entry["task_type"] for entry in plan] == [
        "data_model",
        "api_surface",
        "system_summary",
    ]
    assert all(entry["task_type"] != "system_breakdown" for entry in plan)


def test_run_section_with_workspace_updates_state_through_lifecycle(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_state")
    observed_states = []

    def fake_decompose(section):
        section_id = preflight_section(section)["section_id"]
        state_path = project_root / ".dce" / "state" / f"{section_id}.json"
        observed_states.append(json.loads(state_path.read_text(encoding="utf-8"))["stage"])
        return decompose_section(section)

    def fake_write_file_plan(file_plan, output_dir, overwrite_paths=None):
        section_id = preflight_section(_section())["section_id"]
        state_path = project_root / ".dce" / "state" / f"{section_id}.json"
        observed_states.append(json.loads(state_path.read_text(encoding="utf-8"))["stage"])
        written_files = []
        for entry in file_plan.files:
            path = output_dir / Path(entry["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"generated:{entry['path']}", encoding="utf-8")
            written_files.append(entry["path"])
        return written_files

    def fake_run(self, executor_name, content):
        section_id = preflight_section(_section())["section_id"]
        state_path = project_root / ".dce" / "state" / f"{section_id}.json"
        observed_states.append(json.loads(state_path.read_text(encoding="utf-8"))["stage"])
        return _stub_executor_result(content)

    monkeypatch.setattr("aether.dgce.decompose.decompose_section", fake_decompose)
    monkeypatch.setattr("aether.dgce.decompose.write_file_plan", fake_write_file_plan)
    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root)
    section_id = preflight_section(_section())["section_id"]
    final_state = json.loads((project_root / ".dce" / "state" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert "PLAN" in observed_states
    assert "EXECUTE" in observed_states
    assert "WRITE" in observed_states
    assert final_state == {
        "section_id": "mission-board",
        "stage": "FINALIZE",
        "status": "complete",
        "tasks_completed": 4,
        "tasks_failed": 0,
    }


def test_run_section_with_workspace_updates_task_statuses(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_task_status")
    observed_plan_statuses = []

    class TrackingPlanner:
        def route(self, task, classification):
            plan_path = project_root / ".dce" / "plans" / f"{preflight_section(_section())['section_id']}.json"
            observed_plan_statuses.append(
                [entry["status"] for entry in json.loads(plan_path.read_text(encoding="utf-8"))]
            )
            structured_content = None
            if task.task_type == "data_model":
                structured_content = {
                    "modules": [
                        {
                            "name": "DGCEDataModel",
                            "entities": ["Mission"],
                            "relationships": ["mission->player"],
                            "required": [],
                            "identity_keys": [],
                        }
                    ],
                    "entities": [
                        {
                            "name": "Mission",
                            "fields": [{"name": "id", "type": "string"}, {"name": "state", "type": "string"}],
                            "description": "",
                        }
                    ],
                    "fields": ["id", "state"],
                    "relationships": ["mission->player"],
                    "validation_rules": ["id required"],
                }
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": json.dumps(structured_content) if structured_content is not None else _stub_executor_output(task.content),
                    "reused": False,
                    "structured_content": structured_content,
                },
            )()

    run_section_with_workspace(
        _section(),
        project_root,
        classification_service=ClassificationService(),
        router_planner=TrackingPlanner(),
    )
    plan_path = project_root / ".dce" / "plans" / f"{preflight_section(_section())['section_id']}.json"
    final_plan = json.loads(plan_path.read_text(encoding="utf-8"))

    assert observed_plan_statuses[0] == ["pending", "pending", "pending", "pending"]
    assert final_plan[-1]["status"] == "completed"
    assert [entry["status"] for entry in final_plan] == ["completed", "completed", "completed", "completed"]
    assert all("task_bucket" in entry for entry in final_plan)


def test_run_section_with_workspace_marks_valid_data_model_task_completed(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_data_model_task_completion")

    class TrackingPlanner:
        def route(self, task, classification):
            structured_content = None
            output = _stub_executor_output(task.content)
            if task.task_type == "data_model":
                structured_content = {
                    "modules": [
                        {
                            "name": "DGCEDataModel",
                            "entities": ["SectionInput"],
                            "relationships": [],
                            "required": [],
                            "identity_keys": [],
                        }
                    ],
                    "entities": [
                        {
                            "name": "SectionInput",
                            "fields": [{"name": "section_id", "type": "string"}],
                            "description": "",
                        }
                    ],
                    "fields": ["section_id"],
                    "relationships": [],
                    "validation_rules": ["section_id required"],
                }
                output = json.dumps(structured_content)
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type == "system_summary" else "code_routine",
                    "decision": "MID_MODEL",
                    "output": output,
                    "reused": False,
                    "structured_content": structured_content,
                },
            )()

    run_section_with_workspace(
        _data_model_section(),
        project_root,
        classification_service=ClassificationService(),
        router_planner=TrackingPlanner(),
    )
    section_id = preflight_section(_data_model_section())["section_id"]
    plan = json.loads((project_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert [entry["task_type"] for entry in plan] == ["data_model", "api_surface", "system_summary"]
    assert plan[0]["status"] == "completed"
    assert "pending" not in [entry["status"] for entry in plan]


def test_run_section_with_workspace_can_complete_with_partial_failure(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_partial_failure")

    class FailingPlanner:
        def route(self, task, classification):
            if task.task_type == "data_model":
                raise RuntimeError("route failure")
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": _stub_executor_output(task.content),
                    "reused": False,
                },
            )()

    result = run_section_with_workspace(
        _section(),
        project_root,
        classification_service=ClassificationService(),
        router_planner=FailingPlanner(),
    )
    section_id = preflight_section(_section())["section_id"]
    plan = json.loads((project_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8"))
    state = json.loads((project_root / ".dce" / "state" / f"{section_id}.json").read_text(encoding="utf-8"))
    advisory_index = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))

    assert len(result.responses) == 4
    assert result.responses[1].status == "error"
    assert [entry["status"] for entry in plan] == ["completed", "error", "completed", "completed"]
    assert state == {
        "section_id": "mission-board",
        "stage": "FINALIZE",
        "status": "complete",
        "tasks_completed": 3,
        "tasks_failed": 1,
    }
    assert "mission_board/service.py" in result.written_files
    assert result.execution_outcome == {
        "section_id": "mission-board",
        "stage": "WRITE",
        "status": "error",
        "validation_summary": {
            "ok": True,
            "error": None,
            "missing_keys": [],
        },
        "change_plan_summary": {
            "create_count": 3,
            "modify_count": 0,
            "ignore_count": 0,
        },
        "execution_summary": {
            "written_files_count": 3,
            "skipped_modify_count": 0,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 0,
            "skipped_exists_fallback_count": 0,
        },
    }
    assert result.advisory == {
        "type": "process_adjustment",
        "summary": "Review failed DGCE run flow for mission-board",
        "explanation": ["execution_error"],
    }
    assert advisory_index == {
        "run_outcome_class": "execution_error",
        "run_mode": "create_only",
        "section_id": "mission-board",
        "status": "error",
        "validation_ok": True,
        "advisory_type": "process_adjustment",
        "advisory_explanation": result.advisory["explanation"],
        "written_files_count": result.execution_outcome["execution_summary"]["written_files_count"],
        "skipped_modify_count": result.execution_outcome["execution_summary"]["skipped_modify_count"],
        "skipped_ignore_count": result.execution_outcome["execution_summary"]["skipped_ignore_count"],
    }


def test_run_section_with_workspace_persists_file_plan_metadata(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_outputs")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    section_id = preflight_section(_section())["section_id"]
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert outputs_payload["section_id"] == section_id
    assert outputs_payload["run_mode"] == result.run_mode == "create_only"
    assert outputs_payload["run_outcome_class"] == result.run_outcome_class == "success_create_only"
    assert outputs_payload["file_plan"] == result.file_plan.model_dump()
    assert outputs_payload["execution_outcome"] == result.execution_outcome
    assert outputs_payload["advisory"] == result.advisory
    assert outputs_payload["write_transparency"] == result.write_transparency
    assert ownership_index == result.ownership_index
    assert outputs_payload["advisory"] is None
    assert outputs_payload["execution_outcome"]["execution_summary"]["written_files_count"] == len(
        result.written_files
    )
    assert (project_root / "mission_board" / "service.py").exists()
    assert not any(
        path.relative_to(project_root).parts[0] == ".dce" and path.name == "service.py"
        for path in project_root.rglob("service.py")
    )


def test_run_section_with_workspace_persists_deterministic_output_artifact_records(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _scaffold_dir("dgce_workspace_output_artifact_records_first")
    second_root = _scaffold_dir("dgce_workspace_output_artifact_records_second")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    first_result = run_section_with_workspace(_section(), first_root)
    second_result = run_section_with_workspace(_section(), second_root)
    section_id = preflight_section(_section())["section_id"]
    first_payload = json.loads((first_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))
    second_payload = json.loads((second_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert first_result.execution_outcome == second_result.execution_outcome
    assert first_payload == second_payload
    assert [artifact["path"] for artifact in first_payload["generated_artifacts"]] == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert [artifact["artifact_id"] for artifact in first_payload["generated_artifacts"]] == [
        "mission-board:api/missionboardservice.py",
        "mission-board:mission_board/models.py",
        "mission-board:mission_board/service.py",
        "mission-board:models/mission.py",
    ]
    assert [artifact["implementation_unit"] for artifact in first_payload["generated_artifacts"]] == [
        "generate_missionboardservice_api",
        "implement_mission_board",
        "implement_mission_board",
        "generate_mission_model",
    ]
    assert all(artifact["write_decision"] == "written" for artifact in first_payload["generated_artifacts"])
    assert first_payload["output_summary"] == {
        "artifact_count": 4,
        "execution_status": "success",
        "execution_summary": {
            "skipped_exists_fallback_count": 0,
            "skipped_identical_count": 0,
            "skipped_ignore_count": 0,
            "skipped_modify_count": 0,
            "skipped_ownership_count": 0,
            "written_files_count": 4,
        },
        "primary_artifact_path": "api/missionboardservice.py",
        "run_outcome_class": "success_create_only",
        "section_id": "mission-board",
        "sources": ["api_surface", "data_model", "system_breakdown"],
        "written_artifact_count": 4,
    }


def test_run_section_with_workspace_uses_expected_targets_when_file_plan_is_empty(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_expected_targets_fallback")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(
        "aether.dgce.decompose.build_file_plan",
        lambda responses: FilePlan(project_name="DGCE", files=[]),
    )

    section = _section_with_targets("api/missionboardservice.py", "models/mission.py")
    result = run_section_with_workspace(section, project_root)

    assert result.file_plan.files == [
        {
            "path": "api/missionboardservice.py",
            "purpose": "",
            "source": "expected_targets",
            "requirements": ["support mission templates", "track progression state"],
        },
        {
            "path": "models/mission.py",
            "purpose": "",
            "source": "expected_targets",
            "requirements": ["support mission templates", "track progression state"],
        },
    ]
    assert "api/missionboardservice.py" in result.written_files
    assert "models/mission.py" in result.written_files


def test_run_section_with_workspace_uses_rich_expected_targets_when_file_plan_is_empty(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_expected_targets_rich")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(
        "aether.dgce.decompose.build_file_plan",
        lambda responses: FilePlan(project_name="DGCE", files=[]),
    )

    section = _section().model_copy()
    section.expected_targets = [
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
            "source": "expected_targets",
        }
    ]
    result = run_section_with_workspace(section, project_root)

    assert result.file_plan.files == [
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
            "source": "expected_targets",
            "requirements": ["support mission templates", "track progression state"],
        }
    ]


def test_run_section_with_workspace_supports_mixed_expected_targets_when_file_plan_is_empty(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_expected_targets_mixed")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(
        "aether.dgce.decompose.build_file_plan",
        lambda responses: FilePlan(project_name="DGCE", files=[]),
    )

    section = _section().model_copy()
    section.expected_targets = [
        "src/components/degraded_mode_handler.py",
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
        },
    ]
    result = run_section_with_workspace(section, project_root)

    assert result.file_plan.files == [
        {
            "path": "src/components/degraded_mode_handler.py",
            "purpose": "",
            "source": "expected_targets",
            "requirements": ["support mission templates", "track progression state"],
        },
        {
            "path": "src/components/anomaly_classifier.py",
            "purpose": "Classify anomaly candidates from deviation scores",
            "source": "expected_targets",
            "requirements": ["support mission templates", "track progression state"],
        },
    ]


def test_run_section_with_workspace_prefers_real_file_plan_over_expected_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_expected_targets_bypass")
    real_plan = FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": "generated/from_model.py",
                "purpose": "Model-derived file",
                "source": "api_surface",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: real_plan)

    section = _section_with_targets("api/missionboardservice.py")
    result = run_section_with_workspace(section, project_root)

    assert result.file_plan.files == real_plan.files
    assert "generated/from_model.py" in result.written_files
    assert "api/missionboardservice.py" not in result.written_files


def test_run_section_with_workspace_enriches_system_breakdown_expected_targets_from_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_system_breakdown_expected_targets_enriched")

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "plan the system breakdown" in lowered:
            output = json.dumps(
                {
                    "modules": [
                        {
                            "name": "SectionInputHandler",
                            "layer": "DGCE Core",
                            "responsibility": "Persist section input artifacts.",
                            "inputs": [
                                {
                                    "name": "raw_section_input",
                                    "type": "SectionInputRequest",
                                    "schema_fields": [{"name": "section_id", "type": "string", "required": True}],
                                }
                            ],
                            "outputs": [
                                {"name": "SectionInput", "type": "artifact", "artifact_path": ".dce/input/{section_id}.json"}
                            ],
                            "dependencies": [
                                {"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"}
                            ],
                            "governance_touchpoints": ["input validation"],
                            "failure_modes": ["invalid input structure"],
                            "owned_paths": [".dce/input/{section_id}.json"],
                            "implementation_order": 1,
                        },
                        {
                            "name": "PreviewGenerator",
                            "layer": "DGCE Core",
                            "responsibility": "Generate preview artifacts.",
                            "inputs": [
                                {"name": "SectionInput", "type": "artifact", "artifact_path": ".dce/input/{section_id}.json"}
                            ],
                            "outputs": [
                                {"name": "PreviewArtifact", "type": "artifact", "artifact_path": ".dce/plans/{section_id}.preview.json"}
                            ],
                            "dependencies": [
                                {"name": "decomposition_engine", "kind": "module", "reference": "planner/decompose.py"}
                            ],
                            "governance_touchpoints": ["input fingerprint validation"],
                            "failure_modes": ["invalid decomposition"],
                            "owned_paths": [".dce/plans/{section_id}.preview.json"],
                            "implementation_order": 2,
                        },
                    ],
                    "build_graph": {
                        "type": "directed_acyclic_graph",
                        "edges": [["SectionInputHandler", "PreviewGenerator"]],
                    },
                    "tests": [
                        {
                            "name": "build_graph_is_complete",
                            "purpose": "Verify graph completeness.",
                            "targets": ["build_graph"],
                        }
                    ],
                }
            )
        else:
            output = _stub_executor_output(content)

        return ExecutionResult(
            output=output,
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
    monkeypatch.setattr(
        "aether.dgce.decompose.build_file_plan",
        lambda responses: FilePlan(project_name="DGCE", files=[]),
    )

    section = _system_breakdown_section()
    section.expected_targets = [
        "aether/dgce/decompose.py",
        "aether/dgce/incremental.py",
        "dce.py",
    ]
    result = run_section_with_workspace(section, project_root)

    assert [entry["path"] for entry in result.file_plan.files] == [
        "aether/dgce/decompose.py",
        "aether/dgce/incremental.py",
        "dce.py",
    ]
    assert [entry["source"] for entry in result.file_plan.files] == ["expected_targets"] * 3
    assert result.file_plan.files[0]["purpose"] == "System-breakdown orchestration and contract rendering"
    assert result.file_plan.files[1]["purpose"] == "System-breakdown target grounding and change planning"
    assert result.file_plan.files[2]["purpose"] == "System-breakdown CLI orchestration entrypoint"
    assert "Module contracts: SectionInputHandler, PreviewGenerator" in result.file_plan.files[0]["requirements"]
    assert "Build graph: SectionInputHandler->PreviewGenerator" in result.file_plan.files[0]["requirements"]
    assert "Verification: build_graph_is_complete" in result.file_plan.files[0]["requirements"]


def test_dgce_outputs_artifact_contract_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_outputs_contract_shape")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root)
    section_id = preflight_section(_section())["section_id"]
    payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )

    assert sorted(payload.keys()) == [
        "advisory",
        "execution_outcome",
        "file_plan",
        "generated_artifacts",
        "output_summary",
        "run_mode",
        "run_outcome_class",
        "section_id",
        "write_transparency",
    ]
    assert payload["section_id"] == section_id
    assert payload["run_mode"] in {"create_only", "safe_modify"}
    assert isinstance(payload["run_outcome_class"], str)
    assert isinstance(payload["file_plan"], dict)
    assert isinstance(payload["execution_outcome"], dict)
    assert isinstance(payload["generated_artifacts"], list)
    assert isinstance(payload["output_summary"], dict)
    assert payload["advisory"] is None or isinstance(payload["advisory"], dict)
    assert isinstance(payload["write_transparency"], dict)

    file_plan = payload["file_plan"]
    assert sorted(file_plan.keys()) == ["files", "project_name"]
    assert isinstance(file_plan["project_name"], str)
    assert isinstance(file_plan["files"], list)
    if file_plan["files"]:
        first_entry = file_plan["files"][0]
        assert isinstance(first_entry, dict)
        assert sorted(first_entry.keys()) == ["path", "purpose", "source"]
        assert isinstance(first_entry["path"], str)
        assert isinstance(first_entry["purpose"], str)
        assert isinstance(first_entry["source"], str)

    outcome = payload["execution_outcome"]
    assert sorted(outcome.keys()) == [
        "change_plan_summary",
        "execution_summary",
        "section_id",
        "stage",
        "status",
        "validation_summary",
    ]
    assert isinstance(outcome["section_id"], str)
    assert isinstance(outcome["stage"], str)
    assert isinstance(outcome["status"], str)
    assert isinstance(outcome["validation_summary"], dict)
    assert isinstance(outcome["change_plan_summary"], dict)
    assert isinstance(outcome["execution_summary"], dict)

    validation_summary = outcome["validation_summary"]
    assert sorted(validation_summary.keys()) == ["error", "missing_keys", "ok"]
    assert isinstance(validation_summary["ok"], bool)
    assert validation_summary["error"] is None or isinstance(validation_summary["error"], str)
    assert isinstance(validation_summary["missing_keys"], list)

    change_plan_summary = outcome["change_plan_summary"]
    assert sorted(change_plan_summary.keys()) == ["create_count", "ignore_count", "modify_count"]
    assert all(isinstance(change_plan_summary[key], int) for key in change_plan_summary)

    execution_summary = outcome["execution_summary"]
    assert sorted(execution_summary.keys()) == [
        "skipped_exists_fallback_count",
        "skipped_identical_count",
        "skipped_ignore_count",
        "skipped_modify_count",
        "skipped_ownership_count",
        "written_files_count",
    ]
    assert all(isinstance(execution_summary[key], int) for key in execution_summary)

    generated_artifacts = payload["generated_artifacts"]
    if generated_artifacts:
        first_artifact = generated_artifacts[0]
        assert sorted(first_artifact.keys()) == [
            "artifact_id",
            "artifact_kind",
            "bytes_written",
            "implementation_unit",
            "path",
            "producer_ref",
            "purpose",
            "source",
            "write_decision",
            "write_reason",
        ]
        assert isinstance(first_artifact["artifact_id"], str)
        assert isinstance(first_artifact["artifact_kind"], str)
        assert isinstance(first_artifact["bytes_written"], int)
        assert isinstance(first_artifact["implementation_unit"], str)
        assert isinstance(first_artifact["path"], str)
        assert isinstance(first_artifact["producer_ref"], str)
        assert isinstance(first_artifact["purpose"], str)
        assert isinstance(first_artifact["source"], str)
        assert isinstance(first_artifact["write_decision"], str)
        assert isinstance(first_artifact["write_reason"], str)

    output_summary = payload["output_summary"]
    assert sorted(output_summary.keys()) == [
        "artifact_count",
        "execution_status",
        "execution_summary",
        "primary_artifact_path",
        "run_outcome_class",
        "section_id",
        "sources",
        "written_artifact_count",
    ]
    assert isinstance(output_summary["artifact_count"], int)
    assert isinstance(output_summary["execution_status"], str)
    assert isinstance(output_summary["execution_summary"], dict)
    assert output_summary["primary_artifact_path"] is None or isinstance(output_summary["primary_artifact_path"], str)
    assert isinstance(output_summary["run_outcome_class"], str)
    assert isinstance(output_summary["section_id"], str)
    assert isinstance(output_summary["sources"], list)
    assert isinstance(output_summary["written_artifact_count"], int)

    write_transparency = payload["write_transparency"]
    assert sorted(write_transparency.keys()) == ["write_decisions", "write_summary"]
    assert isinstance(write_transparency["write_decisions"], list)
    assert isinstance(write_transparency["write_summary"], dict)

    write_summary = write_transparency["write_summary"]
    assert sorted(write_summary.keys()) == [
        "after_bytes_total",
        "before_bytes_total",
        "bytes_written_total",
        "changed_lines_estimate_total",
        "diff_visible_count",
        "modify_written_count",
        "skipped_exists_fallback_count",
        "skipped_identical_count",
        "skipped_ignore_count",
        "skipped_modify_count",
        "skipped_ownership_count",
        "written_count",
    ]
    assert all(isinstance(write_summary[key], int) for key in write_summary)

    if write_transparency["write_decisions"]:
        first_entry = write_transparency["write_decisions"][0]
        assert isinstance(first_entry, dict)
        assert "path" in first_entry
        assert "decision" in first_entry
        assert "reason" in first_entry
        assert isinstance(first_entry["path"], str)
        assert isinstance(first_entry["decision"], str)
        assert isinstance(first_entry["reason"], str)
        if "diff_visibility" in first_entry:
            assert sorted(first_entry["diff_visibility"].keys()) == [
                "after_bytes",
                "before_bytes",
                "changed_lines_estimate",
            ]
            assert all(isinstance(first_entry["diff_visibility"][key], int) for key in first_entry["diff_visibility"])
        if "bytes_written" in first_entry:
            assert isinstance(first_entry["bytes_written"], int)


def test_dgce_ownership_index_contract_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_ownership_index_contract_shape")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    payload = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert payload == result.ownership_index
    assert sorted(payload.keys()) == ["files"]
    assert isinstance(payload["files"], list)
    if payload["files"]:
        first_entry = payload["files"][0]
        assert sorted(first_entry.keys()) == ["last_written_stage", "path", "section_id", "write_reason"]
        assert isinstance(first_entry["path"], str)
        assert isinstance(first_entry["section_id"], str)
        assert isinstance(first_entry["last_written_stage"], str)
        assert isinstance(first_entry["write_reason"], str)


def test_dgce_advisory_index_contract_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_advisory_index_contract_shape")

    class FailingPlanner:
        def route(self, task, classification):
            if task.task_type == "data_model":
                raise RuntimeError("route failure")
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": _stub_executor_output(task.content),
                    "reused": False,
                    "execution_metadata": {},
                },
            )()

    run_section_with_workspace(
        _section(),
        project_root,
        classification_service=ClassificationService(),
        router_planner=FailingPlanner(),
    )
    payload = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))

    assert sorted(payload.keys()) == [
        "advisory_explanation",
        "advisory_type",
        "run_mode",
        "run_outcome_class",
        "section_id",
        "skipped_ignore_count",
        "skipped_modify_count",
        "status",
        "validation_ok",
        "written_files_count",
    ]
    assert payload["run_mode"] in {"create_only", "safe_modify"}
    assert isinstance(payload["run_outcome_class"], str)
    assert isinstance(payload["section_id"], str)
    assert isinstance(payload["status"], str)
    assert isinstance(payload["validation_ok"], bool)
    assert isinstance(payload["advisory_type"], str)
    assert isinstance(payload["advisory_explanation"], list)
    assert isinstance(payload["written_files_count"], int)
    assert isinstance(payload["skipped_modify_count"], int)
    assert isinstance(payload["skipped_ignore_count"], int)


def test_dgce_workspace_summary_single_section_success(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_summary_success")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    payload = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert payload == {
        "total_sections_seen": 1,
        "sections": [
            {
                "section_id": "mission-board",
                "latest_run_mode": "create_only",
                "latest_run_outcome_class": "success_create_only",
                "latest_status": result.execution_outcome["status"],
                "latest_validation_ok": result.execution_outcome["validation_summary"]["ok"],
                "latest_advisory_type": None,
                "latest_advisory_explanation": None,
                "latest_written_files_count": result.execution_outcome["execution_summary"]["written_files_count"],
                "latest_skipped_modify_count": result.execution_outcome["execution_summary"]["skipped_modify_count"],
                "latest_skipped_ignore_count": result.execution_outcome["execution_summary"]["skipped_ignore_count"],
                "preview_path": None,
                "review_path": None,
                "preview_outcome_class": None,
                "recommended_mode": None,
                "approval_path": None,
                "approval_status": None,
                "selected_mode": None,
                "execution_permitted": None,
                "preflight_path": None,
                "preflight_status": None,
                "stale_check_path": None,
                "stale_status": None,
                "stale_detected": None,
                "execution_allowed": None,
                "execution_gate_path": None,
                "gate_status": None,
                "execution_blocked": None,
                "alignment_path": None,
                "alignment_status": None,
                "alignment_blocked": None,
                "execution_path": ".dce/execution/mission-board.execution.json",
                "execution_status": "execution_not_governed",
                "approval_consumed": False,
                "approval_status_after": None,
            }
        ],
    }


def test_dgce_workspace_summary_is_sorted_when_multiple_outputs_exist(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_workspace_summary_sorted")
    outputs_dir = project_root / ".dce" / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    (outputs_dir / "zeta-section.json").write_text(
        json.dumps(
            {
                "section_id": "zeta-section",
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {"project_name": "DGCE", "files": []},
                "execution_outcome": {
                    "section_id": "zeta-section",
                    "stage": "WRITE",
                    "status": "success",
                    "validation_summary": {"ok": True, "error": None, "missing_keys": []},
                    "change_plan_summary": {"create_count": 0, "modify_count": 0, "ignore_count": 0},
                    "execution_summary": {
                        "written_files_count": 1,
                        "skipped_modify_count": 0,
                        "skipped_ignore_count": 0,
                    },
                },
                "advisory": None,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Alpha Section"), project_root)
    payload = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert payload["total_sections_seen"] == 2
    assert [entry["section_id"] for entry in payload["sections"]] == ["alpha-section", "zeta-section"]
    assert [entry["latest_run_mode"] for entry in payload["sections"]] == ["create_only", "create_only"]
    assert [entry["latest_run_outcome_class"] for entry in payload["sections"]] == ["success_create_only", "success_create_only"]


def test_dgce_outputs_artifact_non_null_advisory_contract_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _scaffold_dir("dgce_outputs_advisory_contract_shape")

    class FailingPlanner:
        def route(self, task, classification):
            if task.task_type == "data_model":
                raise RuntimeError("route failure")
            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": _stub_executor_output(task.content),
                    "reused": False,
                    "execution_metadata": {},
                },
            )()

    result = run_section_with_workspace(
        _section(),
        project_root,
        classification_service=ClassificationService(),
        router_planner=FailingPlanner(),
    )
    section_id = preflight_section(_section())["section_id"]
    payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )

    assert isinstance(result.advisory, dict)
    assert isinstance(payload["advisory"], dict)
    assert sorted(payload["advisory"].keys()) == ["explanation", "summary", "type"]
    assert isinstance(payload["advisory"]["type"], str)
    assert isinstance(payload["advisory"]["summary"], str)
    assert isinstance(payload["advisory"]["explanation"], list)


def test_run_section_with_workspace_repeated_run_produces_stable_structure(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _scaffold_dir("dgce_workspace_repeat_a")
    second_root = _scaffold_dir("dgce_workspace_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), first_root)
    run_section_with_workspace(_section(), second_root)
    section_id = preflight_section(_section())["section_id"]

    first_plan = (first_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8")
    second_plan = (second_root / ".dce" / "plans" / f"{section_id}.json").read_text(encoding="utf-8")
    first_state = (first_root / ".dce" / "state" / f"{section_id}.json").read_text(encoding="utf-8")
    second_state = (second_root / ".dce" / "state" / f"{section_id}.json").read_text(encoding="utf-8")
    first_output = (first_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    second_output = (second_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")

    assert (first_root / ".dce" / "index.yaml").read_text(encoding="utf-8") == (
        second_root / ".dce" / "index.yaml"
    ).read_text(encoding="utf-8")
    assert first_plan == second_plan
    assert first_state == second_state
    assert first_output == second_output


def test_dgce_section_endpoint_returns_ordered_responses(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    telemetry_path, cache_path, artifact_path = _paths("dgce_endpoint")

    client = TestClient(
        create_app(
            telemetry_path=telemetry_path,
            cache_path=cache_path,
            artifact_store_path=artifact_path,
        )
    )

    response = client.post("/v1/dgce/section", json=_section().model_dump())

    assert response.status_code == 200
    body = response.json()
    assert len(body["responses"]) == 4
    assert [item["task_type"] for item in body["responses"]] == [
        "system_breakdown",
        "data_model",
        "api_surface",
        "system_summary",
    ]
    assert body["file_plan"]["project_name"] == "DGCE"
