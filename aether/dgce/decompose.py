"""Deterministic DGCE section decomposition and execution loop."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from aether.dgce.file_plan import FilePlan, _system_breakdown_files, build_file_plan
from aether.dgce.incremental import (
    build_change_plan,
    build_incremental_change_plan,
    build_incremental_preview_artifact,
    build_write_transparency,
    finalize_write_transparency,
    load_change_plan,
    load_owned_paths,
    overwrite_paths_from_transparency,
    render_incremental_review_markdown,
    scan_workspace_file_paths,
    scan_workspace_inventory,
)
from aether.dgce.file_writer import write_file_plan
from aether.dgce.function_stub_spec import parse_function_stub_spec
from aether.dgce.model_config import build_model_execution_metadata, get_model_execution_config
from aether.dgce.model_executor import generate_function_stub
from aether.dgce.model_validator import validate_function_stub
from aether_core.classifier.rules import ClassifierRules
from aether_core.classifier.service import ClassificationService
from aether_core.itera.advisory import build_advisory
from aether_core.itera.artifact_store import ArtifactStore
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.contracts.validator import validate_output
from aether_core.models import ClassificationRequest
from aether_core.models.request import OutputContract
from aether_core.router.planner import RouterPlanner


class DGCESection(BaseModel):
    """Structured DGCE section input for deterministic task decomposition."""

    section_id: str = ""
    section_type: str
    title: str
    description: str
    requirements: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    expected_targets: List[Any] = Field(default_factory=list)


class ResponseEnvelope(BaseModel):
    """Minimal response envelope returned by the DGCE creation loop."""

    request_id: str
    task_type: str
    status: str
    task_bucket: str
    decision: str
    output: str
    reused: bool
    structured_content: Optional[dict] = None


class RunSectionResult(BaseModel):
    """Collected DGCE task responses plus a deterministic file plan."""

    responses: List[ResponseEnvelope]
    file_plan: FilePlan


class RunSectionWriteResult(BaseModel):
    """Collected DGCE task responses plus scaffold write results."""

    responses: List[ResponseEnvelope]
    file_plan: FilePlan
    written_files: List[str]
    run_mode: Optional[str] = None
    run_outcome_class: Optional[str] = None
    execution_outcome: Optional[dict] = None
    advisory: Optional[dict] = None
    write_transparency: Optional[dict] = None
    ownership_index: Optional[dict] = None
    model_execution: Optional[dict] = None


class DGCERunOrchestratorResult(BaseModel):
    """Structured result for the productized DGCE section orchestrator."""

    section_id: str
    status: str
    reason: str
    artifact_paths: dict[str, str | None]
    run_outcome_class: Optional[str] = None


class DGCEWorkspaceStage:
    """Named lifecycle stages for filesystem-backed DGCE runs."""

    PREPARE = "PREPARE"
    PLAN = "PLAN"
    EXECUTE = "EXECUTE"
    WRITE = "WRITE"
    FINALIZE = "FINALIZE"


class SectionApprovalInput(BaseModel):
    """Deterministic approval intent input for one section review outcome."""

    approval_status: str = "pending"
    selected_mode: str = "review_required"
    approval_source: str = "manual"
    approved_by: str = "operator"
    approval_timestamp: str = "1970-01-01T00:00:00Z"
    notes: str = ""


class SectionPreflightInput(BaseModel):
    """Deterministic preflight validation input for one section approval intent."""

    validation_timestamp: str = "1970-01-01T00:00:00Z"


class SectionExecutionGateInput(BaseModel):
    """Deterministic execution-gate input for explicit preflight enforcement."""

    gate_timestamp: str = "1970-01-01T00:00:00Z"


class SectionStaleCheckInput(BaseModel):
    """Deterministic stale-approval validation input for one section."""

    validation_timestamp: str = "1970-01-01T00:00:00Z"


class SectionAlignmentInput(BaseModel):
    """Deterministic alignment-check input for explicit approved-mode enforcement."""

    alignment_timestamp: str = "1970-01-01T00:00:00Z"


class SectionExecutionStampInput(BaseModel):
    """Deterministic execution-stamp input for one coordinator run."""

    execution_timestamp: str = "1970-01-01T00:00:00Z"


DGCE_LIFECYCLE_ORDER = [
    "preview",
    "review",
    "approval",
    "preflight",
    "gate",
    "alignment",
    "execution",
    "outputs",
]

DGCE_ARTIFACT_SCHEMA_VERSION = "1.0"
DGCE_ARTIFACT_GENERATED_BY = "DGCE"


def decompose_section(section: DGCESection) -> List[ClassificationRequest]:
    """Convert one DGCE section into a small deterministic set of Aether requests."""
    project_id = "DGCE"
    shared_metadata = {
        "project_id": project_id,
        "section_type": section.section_type,
        "section_title": section.title,
        "prompt_profile": "dgce_system_design",
    }

    requirements = _format_list(section.requirements, "Requirements")
    constraints = _format_list(section.constraints, "Constraints")
    section_body = "\n".join(
        [
            f"Section type: {section.section_type}",
            f"Title: {section.title}",
            f"Description: {section.description}",
            requirements,
            constraints,
        ]
    )

    tasks: list[ClassificationRequest] = []
    if section.section_type != "data_model":
        tasks.append(
            ClassificationRequest(
                content=(
                    "Plan the system breakdown for this DGCE section.\n"
                    f"{section_body}\n"
                    f"{_system_breakdown_generation_prompt(section)}"
                ),
                request_id=f"{_slug(section.title)}-system-breakdown",
                preset="dgce_system_design",
                project=project_id,
                task_type="system_breakdown",
                priority="high",
                reuse_scope="project",
                output_contract=OutputContract(
                    mode="structured",
                    schema_name="dgce_system_breakdown_v1",
                ),
                metadata={**shared_metadata, "task_subtype": "system_breakdown"},
            )
        )

    tasks.extend(
        [
            ClassificationRequest(
                content=(
                    "Implement a data model class and method definitions for this DGCE section.\n"
                    f"{section_body}\n"
                    f"{_data_model_generation_prompt(section)}"
                ),
                request_id=f"{_slug(section.title)}-data-model",
                preset="dgce_system_design",
                project=project_id,
                task_type="data_model",
                priority="high",
                reuse_scope="project",
            output_contract=OutputContract(
                mode="structured",
                schema_name="dgce_data_model_v1",
            ),
            metadata={
                **shared_metadata,
                "task_subtype": "data_model",
                "require_non_empty_structured_output": True,
            },
        ),
            ClassificationRequest(
                content=(
                    "Implement an API surface with class and method contracts for this DGCE section.\n"
                    f"{section_body}\n"
                    f"{_api_surface_generation_prompt(section)}"
                ),
                request_id=f"{_slug(section.title)}-api-surface",
                preset="dgce_system_design",
                project=project_id,
                task_type="api_surface",
                priority="high",
                reuse_scope="project",
                output_contract=OutputContract(
                    mode="structured",
                    schema_name="dgce_api_surface_v1",
                ),
                metadata={**shared_metadata, "task_subtype": "api_surface"},
            ),
            ClassificationRequest(
                content=(
                    "Design a concise system summary for this DGCE section.\n"
                    f"{section_body}\n"
                    "Summarize architecture intent, implementation milestones, and follow-up questions."
                ),
                request_id=f"{_slug(section.title)}-system-summary",
                preset="dgce_system_design",
                project=project_id,
                task_type="system_summary",
                priority="high",
                reuse_scope="project",
                metadata={**shared_metadata, "task_subtype": "system_summary"},
            ),
        ]
    )
    if section.section_type == "data_model":
        assert all(task.task_type != "system_breakdown" for task in tasks)
    return tasks


def _system_breakdown_generation_prompt(section: DGCESection) -> str:
    """Return the scoped system-breakdown generation brief for one DGCE section."""
    base_prompt = "Identify subsystems, interfaces, responsibilities, and build order."
    if section.section_type != "system_breakdown" and "system breakdown" not in section.title.lower():
        return base_prompt

    return (
        "Produce a deterministic, implementation-ready module contract and build graph as structured JSON.\n"
        "Use explicit top-level keys modules, build_graph, file_groups, implementation_units, tests, determinism_rules, and acceptance_criteria.\n"
        "Each module must include name, layer, responsibility, typed inputs, typed outputs, anchored dependencies with name/kind/reference, governance_touchpoints, failure_modes, owned_paths, and implementation_order.\n"
        "Include deterministic file_groups that map each module to stable generated file placements, and deterministic implementation_units that describe ordered implementation work.\n"
        "Validation will fail if any module is missing dependencies, inputs, outputs, owned_paths, or implementation_order, or if build_graph.edges or tests is missing.\n"
        "Validation WILL FAIL if build_graph.edges is missing or empty. Validation WILL FAIL if any module is missing owned_paths.\n"
        "The modules array must always be present and non-empty. Every module MUST include a non-empty owned_paths array. owned_paths must be filesystem paths owned by that module, such as .dce/input/, .dce/plans/, .dce/reviews/, .dce/approvals/, .dce/preflight/, .dce/execution/gate/, .dce/execution/alignment/, .dce/execution/stamps/, and .dce/outputs/. The build_graph object must always include a non-empty edges array with at least one edge. The tests array must always be present.\n"
        "Represent request schema_fields as explicit field objects. Use field name, type, and required status; for array fields use type=array plus items=string instead of array[string].\n"
        "The output must define a concrete module list and a complete build_graph DAG with stable ordering for all declared producer/consumer dependencies, not just the dominant pipeline chain.\n"
        "Add a top-level tests array that defines implementation-ready verification cases for generation and implementation.\n"
        "Use disambiguated ownership boundaries with exact artifact path patterns or non-overlapping owned paths; do not let multiple modules ambiguously own the same undivided path.\n"
        "Include an explicit stale-check module owning .dce/preflight/{section_id}.stale_check.json because DGCE persists a distinct stale-check artifact.\n"
        'Example top-level shape: {"modules":[{"name":"ExampleModule","inputs":["SectionInput"],"outputs":["PreviewArtifact"],"dependencies":[],"owned_paths":[".dce/input/",".dce/plans/"],"implementation_order":1},{"name":"ReviewManager","inputs":["PreviewArtifact"],"outputs":["ReviewArtifact"],"dependencies":["ExampleModule"],"owned_paths":[".dce/reviews/"],"implementation_order":2}],"file_groups":[{"name":"example_module","module":"ExampleModule","placement":"example_module","files":[{"path":"example_module/models.py","kind":"models"},{"path":"example_module/service.py","kind":"service"}]}],"implementation_units":[{"name":"implement_example_module","module":"ExampleModule","order":1}],"build_graph":{"edges":[["ExampleModule","ReviewManager"]]},"tests":[{"name":"module_contract_is_complete"}]}.\n'
        'Example module: {"name":"SectionInputHandler","layer":"DGCE Core","responsibility":"Validate and persist section input artifacts.","inputs":[{"name":"raw_section_input","type":"SectionInputRequest","schema_fields":[{"name":"section_id","type":"string","required":true}]}],"outputs":[{"name":"SectionInput","type":"artifact","artifact_path":".dce/input/{section_id}.json"}],"dependencies":[{"name":"artifact_writer","kind":"module","reference":"planner/io.py"}],"governance_touchpoints":["input validation"],"failure_modes":["invalid input structure"],"owned_paths":[".dce/input/{section_id}.json"],"implementation_order":1}.\n'
        "Do not emit modules or build_graph without these fields.\n"
        "Do not emit any module object without owned_paths.\n"
        "Do not fall back to generic architecture summaries or component prose. Emit only concrete contract fields that satisfy the validator.\n"
        "Avoid vague architecture prose; prefer concrete contract fields that can directly drive implementation."
    )


def _data_model_generation_prompt(section: DGCESection) -> str:
    """Return the scoped data-model generation brief for one DGCE section."""
    base_prompt = "Describe entities, fields, relationships, and persistence considerations."
    if section.section_type != "data_model":
        return base_prompt

    artifact_entities = [
        "SectionInput",
        "PreviewArtifact",
        "ReviewArtifact",
        "ApprovalArtifact",
        "PreflightRecord",
        "ExecutionGate",
        "AlignmentRecord",
        "ExecutionStamp",
        "OutputArtifact",
    ]
    artifact_list = ", ".join(artifact_entities)
    return (
        "Produce a deterministic, implementation-ready governed lifecycle data model.\n"
        "You MUST return ONLY valid JSON.\n"
        "Do NOT include any explanation, prose, markdown, or comments.\n"
        "The output must be directly parsable by json.loads().\n"
        "Output must start with { and end with }.\n"
        f"Required entities in stable order: {artifact_list}.\n"
        "Use explicit top-level keys modules, entities, fields, relationships, and validation_rules.\n"
        "Return EXACTLY this format:\n"
        '{\n'
        '"modules": [...],\n'
        '"entities": [...],\n'
        '"fields": [...],\n'
        '"relationships": [...],\n'
        '"validation_rules": [...]\n'
        "}\n"
        "Validation will fail if the output does not include a top-level 'modules' array.\n"
        "If the output is not valid JSON, the system will fail validation.\n"
        "The modules key must be a non-empty array. The entities key must be a non-empty array. Do not emit empty structured output.\n"
        "Entity names must be short PascalCase nouns such as SectionInput, ApprovalArtifact, or AlignmentRecord.\n"
        "Derive filenames and downstream identifiers only from entity names. Never derive entity names from descriptions, field dumps, dict text, or storage-path prose.\n"
        "Do not emit duplicate entities or near-duplicates that describe the same governed artifact under different verbose names.\n"
        "Each module must include name, entities, relationships, required, and identity_keys.\n"
        "For each entity, include explicit identity_keys, storage_path, fields, field types, required flags, enum values, and invariants.\n"
        "Every entity must be implementation-ready for immediate Pydantic BaseModel generation. Do not emit placeholder entities or empty field lists when the JSON contract implies concrete attributes.\n"
        "Represent each field as a JSON object with name, type, and required. Derive field names directly from governed JSON keys, derive Python-compatible types from the field type string, and enforce required=true as a non-optional field in downstream code generation.\n"
        "Prefer concrete field objects such as {\"name\":\"section_id\",\"type\":\"string\",\"required\":true} over prose or loose field-name arrays.\n"
        "Where entities reference other governed artifacts, represent those relationships explicitly so downstream code generation can emit typed references.\n"
        "Represent lifecycle relationships explicitly, including approval, preflight, stale-check, gate, alignment, execution, output, and status derivation links.\n"
        "Include fingerprint, artifact_fingerprint, input_fingerprint, staleness, and run_outcome_class concepts where they apply.\n"
        'Example output: {"modules":[{"name":"DGCEDataModel","entities":["SectionInput","PreviewArtifact","ApprovalArtifact"],"relationships":["SectionInput->PreviewArtifact","PreviewArtifact->ApprovalArtifact"],"required":["section_id"],"identity_keys":["section_id"]}],"entities":[{"name":"SectionInput","identity_keys":["section_id"],"storage_path":".dce/input/{section_id}.json","fields":[{"name":"section_id","type":"string","required":true},{"name":"input_fingerprint","type":"string","required":false}],"invariants":["section_id is stable"],"description":"Governed section input."},{"name":"PreviewArtifact","identity_keys":["section_id"],"storage_path":".dce/plans/{section_id}.preview.json","fields":[{"name":"section_id","type":"string","required":true},{"name":"artifact_fingerprint","type":"string","required":false}],"invariants":["preview links back to section input"],"description":"Preview artifact for review."}],"fields":["artifact_fingerprint","input_fingerprint","section_id"],"relationships":["SectionInput->PreviewArtifact","PreviewArtifact->ApprovalArtifact"],"validation_rules":["section_id required","approval requires current preview fingerprint"]}.\n'
        'Target code shape for downstream generation: class SectionInput(BaseModel): section_id: str; content: dict[str, Any] | None = None; artifact_fingerprint: str | None = None.\n'
        "Do not return only entities. Do not return only relationships. Do not return descriptive prose.\n"
        "Use top-level fields as a canonical field catalog, relationships as explicit objects, and validation_rules for invariants and determinism rules.\n"
        "Prefer concrete schema detail over prose so downstream code generation does not need to guess."
    )


def _api_surface_generation_prompt(section: DGCESection) -> str:
    """Return the scoped api-surface generation brief for one DGCE section."""
    base_prompt = (
        "Produce a deterministic, implementation-ready API contract as structured JSON.\n"
        "Define concrete contract fields rather than descriptive prose.\n"
        "Use top-level keys interfaces, methods, inputs, outputs, error_cases, and endpoints.\n"
        "For each endpoint include name, method, path, purpose, request_body, success_response, error_responses, preconditions, idempotency, and side_effects.\n"
        "Make lifecycle and governance preconditions explicit rather than implied.\n"
        "Status-oriented responses must include actionable next_action values derived from persisted state.\n"
        "Keep ordering stable so identical input produces identical contract-visible output."
    )

    lowered_title = section.title.lower()
    if "api surface" not in lowered_title:
        return base_prompt

    required_endpoints = [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "status",
    ]
    endpoint_list = ", ".join(required_endpoints)
    return (
        f"{base_prompt}\n"
        "Model the DGCE governed lifecycle without bypassing preview, review, approval, preflight, gate, alignment, execution, or output controls.\n"
        f"Required lifecycle operations in stable order: {endpoint_list}.\n"
        "Interface names must be short intentional PascalCase names such as DGCEGovernanceAPI or SectionLifecycleService.\n"
        "Avoid repeated suffixes like InterfaceInterface, and never leak description_ or field-dump text into interface, method, input, or output names.\n"
        "Method names should exactly match the lifecycle operation names, and input/output identifiers should be concise snake_case nouns.\n"
        "Use JSON-over-HTTP endpoint contracts with explicit request_schema, response_schema, error_schema, success response shapes, structured error responses, lifecycle preconditions, idempotency expectations, and side effects.\n"
        "Include an explicit schemas map, an explicit status_contract with next_action, and an explicit error_model with stable error codes.\n"
        'Example naming shape: {"interfaces":["PreviewService"],"methods":{"status":{"method":"GET","path":"/status/{section_id}","response_schema":"StatusResponse","error_schema":"ApiError"}},"inputs":{"status":{"section_id":"string"}},"outputs":{"status":{"section_id":"string","status":"string","next_action":"string"}},"error_cases":{"status":["section_missing"]},"schemas":{"StatusResponse":{"fields":[{"name":"section_id","type":"string","required":true},{"name":"status","type":"string","required":true},{"name":"next_action","type":"string","required":true}]}}}.\n'
        "Preserve Guardrail authority and describe blocked or stale governance states as structured contract outcomes."
    )


def preflight_section(section: DGCESection) -> Dict[str, str]:
    """Validate deterministic section metadata without side effects."""
    required_fields = {
        "section_type": section.section_type,
        "title": section.title,
        "description": section.description,
    }
    for field_name, value in required_fields.items():
        if not value.strip():
            raise ValueError(f"section.{field_name} is required")

    explicit_section_id = str(section.section_id).strip()
    normalized_section_id = _slug(explicit_section_id) if explicit_section_id else _slug(section.title)
    normalized_title = _slug(section.title)
    return {
        "section_id": normalized_section_id,
        "normalized_title": normalized_title,
    }


def run_section(
    section: DGCESection,
    classification_service: Optional[ClassificationService] = None,
    router_planner: Optional[RouterPlanner] = None,
) -> RunSectionResult:
    """Run a DGCE section through the existing Aether pipeline sequentially."""
    service = classification_service or ClassificationService()
    planner = router_planner or RouterPlanner(
        cache=ExactMatchCache(),
        artifact_store=ArtifactStore(),
    )

    responses: List[ResponseEnvelope] = []
    for task in decompose_section(section):
        # Keep creation-loop continuity even if one task fails mid-run.
        try:
            classification = service.classify(task)
            route_result = planner.route(task, classification)
            responses.append(_build_response_envelope(task, route_result))
        except Exception:
            responses.append(
                ResponseEnvelope(
                    request_id=task.request_id,
                    task_type=task.task_type or "",
                    status="error",
                    task_bucket="",
                    decision="ERROR",
                    output="",
                    reused=False,
                )
            )
    return RunSectionResult(
        responses=responses,
        file_plan=build_file_plan(responses),
    )


def compute_governed_execution_file_plan(
    section: DGCESection,
    classification_service: Optional[ClassificationService] = None,
    router_planner: Optional[RouterPlanner] = None,
) -> FilePlan:
    """Compute the exact governed file plan for a section without performing workspace writes."""
    result = run_section(
        section,
        classification_service=classification_service,
        router_planner=router_planner,
    )
    file_plan = result.file_plan
    if not file_plan.files and section.expected_targets:
        file_plan = _fallback_expected_target_file_plan(section, result.responses)
    return _governed_owned_target_file_plan(
        section,
        result.responses,
        file_plan,
        require_preflight_pass=True,
        incremental_mode=None,
    )


def run_section_with_workspace(
    section: DGCESection,
    project_root: Path,
    classification_service: Optional[ClassificationService] = None,
    router_planner: Optional[RouterPlanner] = None,
    *,
    allow_safe_modify: bool = False,
    incremental_mode: Optional[str] = None,
    require_preflight_pass: bool = False,
    preflight_validation_timestamp: str = "1970-01-01T00:00:00Z",
    gate_timestamp: str = "1970-01-01T00:00:00Z",
    alignment_timestamp: str = "1970-01-01T00:00:00Z",
    execution_timestamp: str = "1970-01-01T00:00:00Z",
    prepared_file_plan: Optional[FilePlan] = None,
) -> RunSectionWriteResult:
    """Run a DGCE section with deterministic filesystem-backed workspace metadata."""
    service = classification_service or ClassificationService()
    planner = router_planner or RouterPlanner(
        cache=ExactMatchCache(),
        artifact_store=ArtifactStore(),
    )

    workspace = _ensure_workspace(project_root)
    effective_allow_safe_modify = _effective_allow_safe_modify(section, allow_safe_modify)
    run_mode = _run_mode_from_allow_safe_modify(effective_allow_safe_modify)
    preflight = preflight_section(section)
    section_id = preflight["section_id"]
    state_path = workspace["state"] / f"{section_id}.json"
    plan_path = workspace["plans"] / f"{section_id}.json"
    change_plan_path = workspace["plans"] / f"{section_id}.change_plan.json"
    preview_path = workspace["plans"] / f"{section_id}.preview.json"
    review_path = workspace["root"] / "reviews" / f"{section_id}.review.md"
    outputs_path = workspace["outputs"] / f"{section_id}.json"
    advisory_index_path = workspace["root"] / "advisory_index.json"
    ownership_index_path = workspace["root"] / "ownership_index.json"
    workspace_summary_path = workspace["root"] / "workspace_summary.json"
    gate_artifact: dict[str, Any] | None = None

    _write_json(
        workspace["input"] / f"{section_id}.json",
        section.model_dump(),
    )
    _write_state(state_path, section_id, DGCEWorkspaceStage.PREPARE)
    _update_workspace_index(workspace["index"], section_id)

    _write_state(state_path, section_id, DGCEWorkspaceStage.PLAN)
    tasks = decompose_section(section)
    plan_entries = [_plan_entry(task) for task in tasks]
    _write_json(plan_path, plan_entries)
    if require_preflight_pass:
        gate_artifact = record_section_execution_gate(
            project_root,
            section_id,
            require_preflight_pass=True,
            gate=SectionExecutionGateInput(gate_timestamp=gate_timestamp),
            preflight=SectionPreflightInput(validation_timestamp=preflight_validation_timestamp),
        )
        if gate_artifact["execution_blocked"]:
            execution_artifact = record_section_execution_stamp(
                project_root,
                section_id,
                require_preflight_pass=True,
                execution=SectionExecutionStampInput(execution_timestamp=execution_timestamp),
                run_outcome_class=(
                    "blocked_stale"
                    if gate_artifact["gate_status"] == "gate_blocked_stale"
                    else
                    "blocked_execution_not_allowed"
                    if gate_artifact["gate_status"] == "gate_blocked_execution_not_allowed"
                    else "blocked_preflight"
                ),
                execution_blocked=True,
                write_transparency={"write_summary": {"written_count": 0, "modify_written_count": 0}},
            )
            _write_state(
                state_path,
                section_id,
                DGCEWorkspaceStage.FINALIZE,
                status="blocked",
                tasks_completed=0,
                tasks_failed=0,
            )
            return RunSectionWriteResult(
                responses=[],
                file_plan=FilePlan(project_name="DGCE", files=[]),
                written_files=[],
                run_mode=incremental_mode or run_mode,
                run_outcome_class=(
                    "blocked_stale"
                    if gate_artifact["gate_status"] == "gate_blocked_stale"
                    else
                    "blocked_execution_not_allowed"
                    if gate_artifact["gate_status"] == "gate_blocked_execution_not_allowed"
                    else "blocked_preflight"
                ),
                execution_outcome={
                    "status": "blocked",
                    "gate_status": gate_artifact["gate_status"],
                    "preflight_status": gate_artifact["preflight_status"],
                    "stale_status": gate_artifact.get("stale_status"),
                    "execution_status": execution_artifact["execution_status"],
                    "written_files_count": 0,
                },
                advisory=None,
                write_transparency=None,
                ownership_index=None,
            )

    _write_state(state_path, section_id, DGCEWorkspaceStage.EXECUTE)
    responses: List[ResponseEnvelope] = []
    failed_tasks = 0
    validation_summary = {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }
    for index, task in enumerate(tasks):
        try:
            classification = service.classify(task)
            route_result = planner.route(task, classification)
            response = _build_response_envelope(task, route_result)
            _update_validation_summary(
                validation_summary,
                task,
                response,
                getattr(route_result, "execution_metadata", {}) or {},
            )
            plan_entries[index]["status"] = _task_status_after_execution(task, response, validation_summary)
            if response.status == "error":
                failed_tasks += 1
        except Exception:
            failed_tasks += 1
            response = ResponseEnvelope(
                request_id=task.request_id,
                task_type=task.task_type or "",
                status="error",
                task_bucket="",
                decision="ERROR",
                output="",
                reused=False,
            )
            plan_entries[index]["status"] = "error"

        responses.append(response)
        _write_json(plan_path, plan_entries)

    _write_state(state_path, section_id, DGCEWorkspaceStage.WRITE)
    if prepared_file_plan is not None:
        file_plan = FilePlan.model_validate(prepared_file_plan.model_dump())
    else:
        file_plan = build_file_plan(responses)
        if not file_plan.files and section.expected_targets:
            file_plan = _fallback_expected_target_file_plan(section, responses)
        file_plan = _governed_owned_target_file_plan(
            section,
            responses,
            file_plan,
            require_preflight_pass=require_preflight_pass,
            incremental_mode=incremental_mode,
        )
    if any(Path(str(file_entry["path"])).parts[:1] == (".dce",) for file_entry in file_plan.files):
        raise ValueError("Scaffold file plan must not target the .dce workspace directory")
    if incremental_mode in {"incremental_v1", "incremental_v1_1", "incremental_v2", "incremental_v2_1", "incremental_v2_2"}:
        incremental_change_plan = build_incremental_change_plan(
            section_id,
            file_plan,
            scan_workspace_file_paths(project_root),
            mode=incremental_mode,
            project_root=project_root,
        )
        _write_json(change_plan_path, incremental_change_plan)
    if incremental_mode == "incremental_v1":
        _refresh_workspace_views(workspace)
        _write_state(
            state_path,
            section_id,
            DGCEWorkspaceStage.FINALIZE,
            status="complete",
            tasks_completed=len(tasks) - failed_tasks,
            tasks_failed=failed_tasks,
        )
        return RunSectionWriteResult(
            responses=responses,
            file_plan=file_plan,
            written_files=[],
            run_mode="incremental_v1",
            run_outcome_class="planned_incremental_v1",
            execution_outcome=None,
            advisory=None,
            write_transparency=None,
            ownership_index=None,
        )
    if incremental_mode in {"incremental_v2", "incremental_v2_1", "incremental_v2_2"}:
        change_plan = load_change_plan(change_plan_path)
        preview_artifact = build_incremental_preview_artifact(
            section_id,
            file_plan,
            change_plan,
            project_root,
            allow_modify_write=effective_allow_safe_modify,
            owned_paths=load_owned_paths(ownership_index_path),
            mode=incremental_mode,
        )
        preview_artifact = _write_json_with_artifact_fingerprint(preview_path, preview_artifact)
        if incremental_mode == "incremental_v2_2":
            _write_review_with_artifact_fingerprint(review_path, render_incremental_review_markdown(preview_artifact))
        _refresh_workspace_views(workspace)
        _write_state(
            state_path,
            section_id,
            DGCEWorkspaceStage.FINALIZE,
            status="complete",
            tasks_completed=len(tasks) - failed_tasks,
            tasks_failed=failed_tasks,
        )
        return RunSectionWriteResult(
            responses=responses,
            file_plan=file_plan,
            written_files=[],
            run_mode=incremental_mode,
            run_outcome_class=(
                "review_incremental_v2_2"
                if incremental_mode == "incremental_v2_2"
                else "preview_incremental_v2_1"
                if incremental_mode == "incremental_v2_1"
                else "preview_incremental_v2"
            ),
            execution_outcome=None,
            advisory=None,
            write_transparency=None,
            ownership_index=None,
        )
    elif incremental_mode is None:
        workspace_inventory = scan_workspace_inventory(project_root)
        _write_json(
            change_plan_path,
            {
                "section_id": section_id,
                "expected_targets": sorted(str(file_entry["path"]) for file_entry in file_plan.files),
                "workspace_inventory": workspace_inventory,
                "changes": build_change_plan(section_id, file_plan, workspace_inventory, project_root=project_root),
            },
        )
    change_plan = load_change_plan(change_plan_path)
    owned_paths = load_owned_paths(ownership_index_path)
    write_plan, write_transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=effective_allow_safe_modify,
        owned_paths=owned_paths,
    )
    if require_preflight_pass:
        alignment_artifact = record_section_alignment(
            project_root,
            section_id,
            require_preflight_pass=True,
            alignment=SectionAlignmentInput(alignment_timestamp=alignment_timestamp),
            write_transparency=write_transparency,
        )
        if alignment_artifact["alignment_blocked"]:
            execution_artifact = record_section_execution_stamp(
                project_root,
                section_id,
                require_preflight_pass=True,
                execution=SectionExecutionStampInput(execution_timestamp=execution_timestamp),
                run_outcome_class="blocked_alignment",
                execution_blocked=True,
                write_transparency=write_transparency,
            )
            _write_state(
                state_path,
                section_id,
                DGCEWorkspaceStage.FINALIZE,
                status="blocked",
                tasks_completed=len(tasks) - failed_tasks,
                tasks_failed=failed_tasks,
            )
            return RunSectionWriteResult(
                responses=responses,
                file_plan=file_plan,
                written_files=[],
                run_mode=incremental_mode or run_mode,
                run_outcome_class="blocked_alignment",
                execution_outcome={
                    "status": "blocked",
                    "gate_status": gate_artifact["gate_status"] if gate_artifact else None,
                    "preflight_status": gate_artifact["preflight_status"] if gate_artifact else None,
                    "alignment_status": alignment_artifact["alignment_status"],
                    "alignment_reason": alignment_artifact["alignment_reason"],
                    "execution_status": execution_artifact["execution_status"],
                    "written_files_count": 0,
                },
                advisory=None,
                write_transparency=None,
                ownership_index=None,
            )
    model_execution = None
    if require_preflight_pass and section.section_type == "function_stub":
        structured_input, target_path = _build_function_stub_structured_input(section, file_plan)
        model_config = get_model_execution_config()
        raw_output = generate_function_stub(structured_input, model_config)
        validated_output = validate_function_stub(raw_output, structured_input)
        file_plan = _inject_function_stub_content(file_plan, target_path, validated_output)
        write_plan, write_transparency = build_write_transparency(
            file_plan,
            change_plan,
            project_root,
            allow_modify_write=effective_allow_safe_modify,
            owned_paths=owned_paths,
        )
        model_execution = build_model_execution_metadata(model_config)
    execution_outcome = _build_execution_outcome(
        section_id=section_id,
        stage=DGCEWorkspaceStage.WRITE,
        validation_summary=validation_summary,
        change_plan=change_plan,
        write_transparency=write_transparency,
        failed_tasks=failed_tasks,
    )
    written_files = write_file_plan(
        write_plan,
        project_root,
        overwrite_paths=overwrite_paths_from_transparency(write_transparency),
    )
    write_transparency = finalize_write_transparency(write_transparency, project_root)
    execution_outcome["execution_summary"]["written_files_count"] = len(written_files)
    execution_outcome["status"] = _outcome_status(
        failed_tasks=failed_tasks,
        skipped_modify_count=execution_outcome["execution_summary"]["skipped_modify_count"],
        skipped_ignore_count=execution_outcome["execution_summary"]["skipped_ignore_count"],
        skipped_ownership_count=execution_outcome["execution_summary"]["skipped_ownership_count"],
    )
    run_outcome_class = _build_run_outcome_class(run_mode, execution_outcome)
    advisory = build_advisory(execution_outcome, section_id)
    existing_ownership_index = _load_ownership_index(ownership_index_path)
    ownership_index = _merge_ownership_index(existing_ownership_index, section_id, write_transparency)
    _write_json(
        outputs_path,
        _build_output_artifact_payload(
            section_id=section_id,
            run_mode=run_mode,
            run_outcome_class=run_outcome_class,
            file_plan=file_plan,
            execution_outcome=execution_outcome,
            advisory=advisory,
            write_transparency=write_transparency,
        ),
    )
    _write_json(
        advisory_index_path,
        _build_advisory_index_entry(section_id, run_mode, run_outcome_class, execution_outcome, advisory),
    )
    _write_json(ownership_index_path, ownership_index)
    record_section_execution_stamp(
        project_root,
        section_id,
        require_preflight_pass=require_preflight_pass,
        execution=SectionExecutionStampInput(execution_timestamp=execution_timestamp),
        run_outcome_class=run_outcome_class,
        execution_blocked=False,
        write_transparency=write_transparency,
        model_execution=model_execution,
    )
    _refresh_workspace_views(workspace)

    _write_state(
        state_path,
        section_id,
        DGCEWorkspaceStage.FINALIZE,
        status="complete",
        tasks_completed=len(tasks) - failed_tasks,
        tasks_failed=failed_tasks,
    )
    return RunSectionWriteResult(
        responses=responses,
        file_plan=file_plan,
        written_files=written_files,
        run_mode=run_mode,
        run_outcome_class=run_outcome_class,
        execution_outcome=execution_outcome,
        advisory=advisory,
        write_transparency=write_transparency,
        ownership_index=ownership_index,
        model_execution=model_execution,
    )


def run_section_and_write(section: DGCESection, output_dir: Path) -> RunSectionWriteResult:
    """Run a DGCE section and write its deterministic scaffold under output_dir."""
    result = run_section(section)
    written_files = write_file_plan(result.file_plan, output_dir)
    return RunSectionWriteResult(
        responses=result.responses,
        file_plan=result.file_plan,
        written_files=written_files,
        run_mode=None,
        run_outcome_class=None,
        execution_outcome=None,
        advisory=None,
        write_transparency=None,
        ownership_index=None,
        model_execution=None,
    )


def _build_function_stub_structured_input(section: DGCESection, file_plan: FilePlan) -> tuple[dict[str, Any], str]:
    if len(file_plan.files) != 1:
        raise ValueError("function_stub execution requires exactly one planned file")
    if len(section.expected_targets) != 1 or not isinstance(section.expected_targets[0], dict):
        raise ValueError("function_stub execution requires exactly one structured expected_target")
    target = dict(section.expected_targets[0])
    target_path = str(target.get("path", "")).strip()
    planned_path = str(file_plan.files[0].get("path", "")).strip()
    if not target_path or target_path != planned_path:
        raise ValueError("function_stub execution requires expected_target path to match the governed file plan")
    try:
        normalized_spec = parse_function_stub_spec(target)
    except ValueError as exc:
        raise ValueError(f"function_stub execution requires valid structured spec: {exc}") from exc
    return normalized_spec, target_path


def _inject_function_stub_content(file_plan: FilePlan, target_path: str, content: str) -> FilePlan:
    payload = file_plan.model_dump()
    injected = False
    for file_entry in payload.get("files", []):
        if str(file_entry.get("path", "")).strip() != target_path:
            continue
        file_entry["content"] = content
        injected = True
    if not injected:
        raise ValueError("function_stub execution target must remain inside the governed file plan")
    return FilePlan.model_validate(payload)


def run_dgce_section(
    section_id: str,
    project_root: Path,
    *,
    governed: bool = True,
    prepared_file_plan: FilePlan | None = None,
) -> DGCERunOrchestratorResult:
    """Run one DGCE section through the existing unmanaged or governed workspace pipeline."""
    section = _load_section_from_workspace_input(project_root, section_id)
    normalized_section_id = _slug(str(section.section_id).strip()) if str(section.section_id).strip() else preflight_section(section)["section_id"]
    if normalized_section_id != section_id:
        raise ValueError(f"Section id mismatch: expected {section_id}, got {normalized_section_id}")

    if not governed:
        result = run_section_with_workspace(section, project_root)
        return DGCERunOrchestratorResult(
            section_id=section_id,
            status="success" if not str(result.run_outcome_class).startswith("blocked_") else "blocked",
            reason=str(result.run_outcome_class),
            artifact_paths=_collect_orchestrator_artifact_paths(project_root, section_id),
            run_outcome_class=result.run_outcome_class,
        )

    approval_path = _ensure_workspace(project_root)["approvals"] / f"{section_id}.approval.json"
    if not approval_path.exists():
        run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    if not approval_path.exists():
        return DGCERunOrchestratorResult(
            section_id=section_id,
            status="approval_required",
            reason="missing_approval",
            artifact_paths=_collect_orchestrator_artifact_paths(project_root, section_id),
            run_outcome_class=None,
        )

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    selected_mode = str(approval_payload.get("selected_mode"))
    result = run_section_with_workspace(
        section,
        project_root,
        allow_safe_modify=selected_mode == "safe_modify",
        require_preflight_pass=True,
        prepared_file_plan=prepared_file_plan,
    )
    return DGCERunOrchestratorResult(
        section_id=section_id,
        status="success" if not str(result.run_outcome_class).startswith("blocked_") else "blocked",
        reason=str(result.run_outcome_class),
        artifact_paths=_collect_orchestrator_artifact_paths(project_root, section_id),
        run_outcome_class=result.run_outcome_class,
    )


def record_section_approval(
    project_root: Path,
    section_id: str,
    approval: SectionApprovalInput | None = None,
) -> dict[str, Any]:
    """Persist a deterministic approval intent artifact and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    approval_input = approval or SectionApprovalInput()
    approval_path = workspace["approvals"] / f"{section_id}.approval.json"
    approval_artifact = _build_approval_artifact(workspace["root"], section_id, approval_input)
    approval_artifact = _write_json_with_artifact_fingerprint(approval_path, approval_artifact)
    _refresh_workspace_views(workspace)
    return approval_artifact


def record_section_preflight(
    project_root: Path,
    section_id: str,
    preflight: SectionPreflightInput | None = None,
) -> dict[str, Any]:
    """Persist a deterministic approval preflight artifact and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    preflight_input = preflight or SectionPreflightInput()
    preflight_path = workspace["preflight"] / f"{section_id}.preflight.json"
    preflight_artifact = _build_preflight_artifact(workspace["root"], section_id, preflight_input)
    preflight_artifact = _write_json_with_artifact_fingerprint(preflight_path, preflight_artifact)
    _refresh_workspace_views(workspace)
    return preflight_artifact


def record_section_stale_check(
    project_root: Path,
    section_id: str,
    stale_check: SectionStaleCheckInput | None = None,
) -> dict[str, Any]:
    """Persist a deterministic stale-approval validation artifact and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    stale_input = stale_check or SectionStaleCheckInput()
    stale_artifact = _build_stale_check_artifact(workspace["root"], section_id, stale_input)
    _write_json(workspace["preflight"] / f"{section_id}.stale_check.json", stale_artifact)
    _refresh_workspace_views(workspace)
    return stale_artifact


def record_section_execution_gate(
    project_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool = False,
    gate: SectionExecutionGateInput | None = None,
    preflight: SectionPreflightInput | None = None,
) -> dict[str, Any]:
    """Persist a deterministic execution-gate artifact and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    gate_input = gate or SectionExecutionGateInput()
    preflight_path = workspace["preflight"] / f"{section_id}.preflight.json"
    stale_check_path = workspace["preflight"] / f"{section_id}.stale_check.json"
    if require_preflight_pass:
        record_section_preflight(project_root, section_id, preflight)
    existing_stale_payload = json.loads(stale_check_path.read_text(encoding="utf-8")) if stale_check_path.exists() else {}
    stale_input = SectionStaleCheckInput(
        validation_timestamp=str(
            existing_stale_payload.get(
                "validation_timestamp",
                preflight.validation_timestamp if preflight else "1970-01-01T00:00:00Z",
            )
        )
    )
    stale_artifact = _build_stale_check_artifact(workspace["root"], section_id, stale_input)
    _write_json(stale_check_path, stale_artifact)
    preflight_payload = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else None
    gate_artifact = _build_execution_gate_artifact(
        workspace["root"],
        section_id,
        require_preflight_pass=require_preflight_pass,
        gate_input=gate_input,
        preflight_payload=preflight_payload,
        stale_check_payload=stale_artifact,
    )
    _write_json(workspace["preflight"] / f"{section_id}.execution_gate.json", gate_artifact)
    _refresh_workspace_views(workspace)
    return gate_artifact


def record_section_alignment(
    project_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool = False,
    alignment: SectionAlignmentInput | None = None,
    write_transparency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a deterministic alignment artifact and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    alignment_input = alignment or SectionAlignmentInput()
    alignment_artifact = _build_alignment_artifact(
        workspace["root"],
        section_id,
        require_preflight_pass=require_preflight_pass,
        alignment_input=alignment_input,
        write_transparency=write_transparency or {},
    )
    _write_json(workspace["preflight"] / f"{section_id}.alignment.json", alignment_artifact)
    _refresh_workspace_views(workspace)
    return alignment_artifact


def record_section_execution_stamp(
    project_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool = False,
    execution: SectionExecutionStampInput | None = None,
    run_outcome_class: str | None = None,
    execution_blocked: bool = False,
    write_transparency: dict[str, Any] | None = None,
    model_execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a deterministic execution stamp and refresh workspace linkage."""
    workspace = _ensure_workspace(project_root)
    execution_input = execution or SectionExecutionStampInput()
    execution_artifact = _build_execution_stamp_artifact(
        workspace["root"],
        section_id,
        require_preflight_pass=require_preflight_pass,
        execution_input=execution_input,
        run_outcome_class=run_outcome_class,
        execution_blocked=execution_blocked,
        write_transparency=write_transparency or {},
        model_execution=model_execution,
    )
    if execution_artifact["approval_consumed"]:
        _supersede_approval_artifact(workspace["root"], section_id)
        execution_artifact["approval_status_after"] = "superseded"
    _write_json(workspace["execution"] / f"{section_id}.execution.json", execution_artifact)
    _refresh_workspace_views(workspace)
    return execution_artifact


def _ensure_workspace(project_root: Path) -> Dict[str, Path]:
    """Create the deterministic filesystem workspace layout under project_root."""
    dce_root = project_root / ".dce"
    workspace = {
        "root": dce_root,
        "input": dce_root / "input",
        "plans": dce_root / "plans",
        "outputs": dce_root / "outputs",
        "reviews": dce_root / "reviews",
        "approvals": dce_root / "approvals",
        "preflight": dce_root / "preflight",
        "execution": dce_root / "execution",
        "state": dce_root / "state",
        "index": dce_root / "index.yaml",
    }
    for key in ("root", "input", "plans", "outputs", "reviews", "approvals", "preflight", "execution", "state"):
        workspace[key].mkdir(parents=True, exist_ok=True)
    if not workspace["index"].exists():
        workspace["index"].write_text("sections: []\n", encoding="utf-8")
    return workspace


def _load_section_from_workspace_input(project_root: Path, section_id: str) -> DGCESection:
    """Load one persisted DGCE section input by section_id from the deterministic workspace."""
    input_path = _ensure_workspace(project_root)["input"] / f"{section_id}.json"
    if not input_path.exists():
        raise FileNotFoundError(f"Section input not found: {input_path}")
    return DGCESection.model_validate(json.loads(input_path.read_text(encoding="utf-8")))


def _collect_orchestrator_artifact_paths(project_root: Path, section_id: str) -> dict[str, str | None]:
    """Collect the known artifact paths for one section under the deterministic workspace layout."""
    workspace = _ensure_workspace(project_root)
    artifact_locations = {
        "input_path": workspace["input"] / f"{section_id}.json",
        "preview_path": workspace["plans"] / f"{section_id}.preview.json",
        "review_path": workspace["reviews"] / f"{section_id}.review.md",
        "approval_path": workspace["approvals"] / f"{section_id}.approval.json",
        "stale_check_path": workspace["preflight"] / f"{section_id}.stale_check.json",
        "preflight_path": workspace["preflight"] / f"{section_id}.preflight.json",
        "execution_gate_path": workspace["preflight"] / f"{section_id}.execution_gate.json",
        "alignment_path": workspace["preflight"] / f"{section_id}.alignment.json",
        "output_path": workspace["outputs"] / f"{section_id}.json",
        "execution_path": workspace["execution"] / f"{section_id}.execution.json",
    }
    return {
        key: path.relative_to(project_root).as_posix() if path.exists() else None
        for key, path in sorted(artifact_locations.items(), key=lambda item: str(item[0]))
    }


def _load_section_artifacts(workspace_root: Path, section_id: str) -> dict[str, Any]:
    """Load persisted workspace artifacts and normalized paths for one section."""
    project_root = workspace_root.parent
    artifact_paths = _collect_orchestrator_artifact_paths(project_root, section_id)
    payloads: dict[str, dict[str, Any]] = {}
    for artifact_key, artifact_path in artifact_paths.items():
        if artifact_path is None or not artifact_path.endswith(".json"):
            payloads[artifact_key] = {}
            continue
        absolute_path = project_root / Path(artifact_path)
        payloads[artifact_key] = json.loads(absolute_path.read_text(encoding="utf-8")) if absolute_path.exists() else {}
    return {
        "artifact_paths": artifact_paths,
        "payloads": payloads,
        "section_id": section_id,
    }


def _normalize_artifact_path(path: str | Path | None) -> str | None:
    """Normalize one DGCE artifact path to a stable repo-relative POSIX string."""
    if path is None:
        return None
    normalized = Path(os.path.normpath(str(path))).as_posix()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized or None


def _normalized_workspace_artifact_path(filename: str) -> str:
    return _normalize_artifact_path(Path(".dce") / filename) or f".dce/{filename}"


def _section_artifact_link_specs() -> list[tuple[str, str, str]]:
    return [
        ("preview", "preview_path", "preview_artifact"),
        ("review", "review_path", "review_artifact"),
        ("approval", "approval_path", "approval_artifact"),
        ("preflight", "preflight_path", "preflight_record"),
        ("stale_check", "stale_check_path", "stale_check_record"),
        ("gate", "execution_gate_path", "execution_gate_record"),
        ("alignment", "alignment_path", "alignment_record"),
        ("execution", "execution_path", "execution_record"),
        ("outputs", "output_path", "output_record"),
    ]


def _build_section_artifact_links(artifact_paths: dict[str, str | None]) -> list[dict[str, str]]:
    return [
        {
            "artifact_role": artifact_role,
            "path": normalized_path,
        }
        for artifact_role, artifact_key, _artifact_type in _section_artifact_link_specs()
        for normalized_path in [_normalize_artifact_path(artifact_paths.get(artifact_key))]
        if normalized_path is not None
    ]


def _build_section_navigation_links(artifact_paths: dict[str, str | None]) -> list[dict[str, str | None]]:
    artifact_link_map = {entry["artifact_role"]: entry["path"] for entry in _build_section_artifact_links(artifact_paths)}
    return [
        {"link_role": "preview", "path": artifact_link_map.get("preview")},
        {"link_role": "review", "path": artifact_link_map.get("review")},
        {"link_role": "approval", "path": artifact_link_map.get("approval")},
        {"link_role": "lifecycle_trace", "path": _normalized_workspace_artifact_path("lifecycle_trace.json")},
        {"link_role": "execution", "path": artifact_link_map.get("execution")},
        {"link_role": "outputs", "path": artifact_link_map.get("outputs")},
    ]


def _supported_consumer_artifact_specs() -> list[dict[str, Any]]:
    return [
        {
            "artifact_path": _normalized_workspace_artifact_path("dashboard.json"),
            "artifact_type": "dashboard",
            "consumer_scopes": ["ui", "reporting"],
            "export_scope": "external",
            "supported_fields": [
                "artifact_paths.lifecycle_trace_path",
                "artifact_paths.review_index_path",
                "artifact_paths.workspace_index_path",
                "section_order",
                "sections[].section_id",
                "sections[].approval_status",
                "sections[].current_stage",
                "sections[].decision_source",
                "sections[].latest_decision",
                "sections[].navigation_links.approval",
                "sections[].navigation_links.execution",
                "sections[].navigation_links.lifecycle_trace",
                "sections[].navigation_links.outputs",
                "sections[].navigation_links.review",
                "sections[].progress.available_artifact_count",
                "sections[].progress.completed_stage_count",
                "sections[].progress.lifecycle_stage_count",
                "sections[].progress.trace_entry_count",
                "sections[].review_status",
                "sections[].stage_status",
                "sections[].section_summary.approval_status",
                "sections[].section_summary.decision_source",
                "sections[].section_summary.latest_decision",
                "sections[].section_summary.latest_decision_source",
                "sections[].section_summary.latest_stage",
                "sections[].section_summary.latest_stage_status",
                "sections[].section_summary.review_status",
                "sections[].section_summary.section_id",
                "summary.approval_status_counts",
                "summary.current_stage_counts",
                "summary.review_status_counts",
                "summary.stage_status_counts",
                "summary.total_sections",
            ],
        },
        {
            "artifact_path": _normalized_workspace_artifact_path("workspace_index.json"),
            "artifact_type": "workspace_index",
            "consumer_scopes": ["sdk", "reporting"],
            "export_scope": "external",
            "supported_fields": [
                "artifact_paths.lifecycle_trace_path",
                "artifact_paths.review_index_path",
                "artifact_paths.workspace_summary_path",
                "section_order",
                "sections[].section_id",
                "sections[].artifact_links",
                "sections[].execution_path",
                "sections[].execution_status",
                "sections[].approval_status",
                "sections[].decision_source",
                "sections[].review_status",
                "sections[].latest_decision",
                "sections[].latest_decision_source",
                "sections[].latest_run_outcome_class",
                "sections[].latest_stage",
                "sections[].latest_stage_status",
                "sections[].lifecycle_trace_path",
                "sections[].output_path",
                "sections[].trace_entry_count",
                "sections[].section_summary.approval_status",
                "sections[].section_summary.decision_source",
                "sections[].section_summary.latest_decision",
                "sections[].section_summary.latest_decision_source",
                "sections[].section_summary.latest_stage",
                "sections[].section_summary.latest_stage_status",
                "sections[].section_summary.review_status",
                "sections[].section_summary.section_id",
                "sections[].trace_summary.available_artifact_count",
                "sections[].trace_summary.approval_status",
                "sections[].trace_summary.completed_stage_count",
                "sections[].trace_summary.decision_source",
                "sections[].trace_summary.latest_decision",
                "sections[].trace_summary.latest_decision_source",
                "sections[].trace_summary.latest_stage",
                "sections[].trace_summary.latest_stage_status",
                "sections[].trace_summary.review_status",
                "sections[].trace_summary.section_id",
                "sections[].trace_summary.trace_entry_count",
                "summary.latest_stage_counts",
                "summary.sections_with_execution",
                "summary.sections_with_lifecycle_trace",
                "summary.sections_with_outputs",
                "summary.total_sections_seen",
            ],
        },
        {
            "artifact_path": _normalized_workspace_artifact_path("reviews/index.json"),
            "artifact_type": "review_index",
            "consumer_scopes": ["audit", "reporting"],
            "export_scope": "external",
            "supported_fields": [
                "section_order",
                "sections[].entry_order",
                "sections[].section_id",
                "sections[].preview_path",
                "sections[].review_path",
                "sections[].approval_path",
                "sections[].execution_path",
                "sections[].output_path",
                "sections[].lifecycle_trace_path",
                "sections[].approval_status",
                "sections[].selected_mode",
                "sections[].preflight_status",
                "sections[].stale_status",
                "sections[].gate_status",
                "sections[].alignment_status",
                "sections[].execution_status",
                "sections[].decision_source",
                "sections[].review_status",
                "sections[].latest_decision",
                "sections[].latest_decision_source",
                "sections[].approval_timestamp",
                "sections[].review_approval_summary.approval_status",
                "sections[].review_approval_summary.decision_source",
                "sections[].review_approval_summary.latest_decision",
                "sections[].review_approval_summary.latest_decision_source",
                "sections[].review_approval_summary.review_status",
                "sections[].section_summary.approval_status",
                "sections[].section_summary.decision_source",
                "sections[].section_summary.latest_decision",
                "sections[].section_summary.latest_decision_source",
                "sections[].section_summary.latest_stage",
                "sections[].section_summary.latest_stage_status",
                "sections[].section_summary.review_status",
                "sections[].section_summary.section_id",
                "sections[].navigation_links",
                "summary.sections_with_approval",
                "summary.sections_with_execution",
                "summary.sections_with_outputs",
                "summary.sections_with_review",
                "summary.total_sections_seen",
            ],
        },
        {
            "artifact_path": _normalized_workspace_artifact_path("lifecycle_trace.json"),
            "artifact_type": "lifecycle_trace",
            "consumer_scopes": ["audit", "reporting"],
            "export_scope": "external",
            "supported_fields": [
                "lifecycle_order",
                "total_sections_seen",
                "sections[].section_id",
                "sections[].approval_status",
                "sections[].decision_source",
                "sections[].review_status",
                "sections[].latest_decision",
                "sections[].latest_decision_source",
                "sections[].latest_stage",
                "sections[].latest_stage_status",
                "sections[].section_summary.approval_status",
                "sections[].section_summary.decision_source",
                "sections[].section_summary.latest_decision",
                "sections[].section_summary.latest_decision_source",
                "sections[].section_summary.latest_stage",
                "sections[].section_summary.latest_stage_status",
                "sections[].section_summary.review_status",
                "sections[].section_summary.section_id",
                "sections[].trace_summary.available_artifact_count",
                "sections[].trace_summary.approval_status",
                "sections[].trace_summary.completed_stage_count",
                "sections[].trace_summary.decision_source",
                "sections[].trace_summary.latest_decision",
                "sections[].trace_summary.latest_decision_source",
                "sections[].trace_summary.latest_stage",
                "sections[].trace_summary.latest_stage_status",
                "sections[].trace_summary.review_status",
                "sections[].trace_summary.section_id",
                "sections[].trace_summary.trace_entry_count",
                "sections[].trace_entries[].artifact_path",
                "sections[].trace_entries[].artifact_present",
                "sections[].trace_entries[].stage",
                "sections[].trace_entries[].stage_order",
                "sections[].trace_entries[].stage_status",
                "sections[].trace_entries[].linkage",
            ],
        },
        {
            "artifact_path": _normalized_workspace_artifact_path("artifact_manifest.json"),
            "artifact_type": "artifact_manifest",
            "consumer_scopes": ["sdk", "reporting"],
            "export_scope": "external",
            "supported_fields": [
                "artifacts[].artifact_path",
                "artifacts[].artifact_type",
                "artifacts[].schema_version",
                "artifacts[].scope",
                "artifacts[].section_id",
            ],
        },
        {
            "artifact_path": _normalized_workspace_artifact_path("workspace_summary.json"),
            "artifact_type": "workspace_summary",
            "consumer_scopes": ["reporting", "sdk"],
            "export_scope": "external",
            "supported_fields": [
                "total_sections_seen",
                "sections[].section_id",
                "sections[].latest_run_mode",
                "sections[].latest_run_outcome_class",
                "sections[].latest_status",
                "sections[].latest_validation_ok",
                "sections[].latest_advisory_type",
                "sections[].latest_advisory_explanation",
                "sections[].latest_written_files_count",
                "sections[].latest_skipped_modify_count",
                "sections[].latest_skipped_ignore_count",
                "sections[].preview_path",
                "sections[].review_path",
                "sections[].preview_outcome_class",
                "sections[].recommended_mode",
                "sections[].approval_path",
                "sections[].approval_status",
                "sections[].selected_mode",
                "sections[].execution_permitted",
                "sections[].preflight_path",
                "sections[].preflight_status",
                "sections[].stale_check_path",
                "sections[].stale_status",
                "sections[].stale_detected",
                "sections[].execution_allowed",
                "sections[].execution_gate_path",
                "sections[].gate_status",
                "sections[].execution_blocked",
                "sections[].alignment_path",
                "sections[].alignment_status",
                "sections[].alignment_blocked",
                "sections[].execution_path",
                "sections[].execution_status",
                "sections[].approval_consumed",
                "sections[].approval_status_after",
                "sections[].decision_source",
                "sections[].review_status",
                "sections[].latest_decision",
                "sections[].latest_decision_source",
                "sections[].latest_stage",
                "sections[].latest_stage_status",
                "sections[].section_summary.approval_status",
                "sections[].section_summary.decision_source",
                "sections[].section_summary.latest_decision",
                "sections[].section_summary.latest_decision_source",
                "sections[].section_summary.latest_stage",
                "sections[].section_summary.latest_stage_status",
                "sections[].section_summary.review_status",
                "sections[].section_summary.section_id",
            ],
        },
    ]


def _build_section_lifecycle_trace_entries_from_artifacts(section_artifacts: dict[str, Any]) -> list[dict[str, Any]]:
    artifact_paths = section_artifacts["artifact_paths"]
    payloads = section_artifacts["payloads"]
    stage_specs = [
        ("preview", "preview_path", payloads["preview_path"].get("preview_outcome_class"), ["input_path", "review_path", "approval_path"]),
        (
            "review",
            "review_path",
            "review_available" if artifact_paths.get("review_path") is not None else None,
            ["preview_path", "approval_path"],
        ),
        (
            "approval",
            "approval_path",
            payloads["approval_path"].get("approval_status"),
            ["preview_path", "review_path", "preflight_path"],
        ),
        (
            "preflight",
            "preflight_path",
            payloads["preflight_path"].get("preflight_status"),
            ["approval_path", "preview_path", "review_path", "execution_gate_path"],
        ),
        (
            "gate",
            "execution_gate_path",
            payloads["execution_gate_path"].get("gate_status"),
            ["preflight_path", "stale_check_path", "alignment_path", "execution_path"],
        ),
        (
            "alignment",
            "alignment_path",
            payloads["alignment_path"].get("alignment_status"),
            ["approval_path", "execution_gate_path", "execution_path"],
        ),
        (
            "execution",
            "execution_path",
            payloads["execution_path"].get("execution_status"),
            ["approval_path", "preflight_path", "execution_gate_path", "alignment_path", "output_path"],
        ),
        ("outputs", "output_path", payloads["output_path"].get("run_outcome_class"), ["execution_path"]),
    ]
    return [
        {
            "artifact_path": artifact_paths.get(artifact_key),
            "artifact_present": artifact_paths.get(artifact_key) is not None,
            "linkage": [{"ref_name": ref_name, "ref_path": artifact_paths.get(ref_name)} for ref_name in linkage_names],
            "stage": stage_name,
            "stage_order": stage_index,
            "stage_status": stage_status,
        }
        for stage_index, (stage_name, artifact_key, stage_status, linkage_names) in enumerate(stage_specs, start=1)
    ]


def _build_section_convergence_summary(section_artifacts: dict[str, Any], trace_entries: list[dict[str, Any]]) -> dict[str, Any]:
    section_id = str(section_artifacts["section_id"])
    artifact_paths = section_artifacts["artifact_paths"]
    payloads = section_artifacts["payloads"]
    preview_payload = payloads["preview_path"]
    approval_payload = payloads["approval_path"]
    present_entries = [entry for entry in trace_entries if entry["artifact_present"]]
    latest_entry = present_entries[-1] if present_entries else None
    latest_decision = approval_payload.get("selected_mode") or preview_payload.get("recommended_mode")
    latest_decision_source = "approval" if approval_payload.get("selected_mode") is not None else (
        "preview_recommendation" if preview_payload.get("recommended_mode") is not None else None
    )
    return {
        "approval_status": approval_payload.get("approval_status"),
        "decision_source": latest_decision_source,
        "latest_decision": latest_decision,
        "latest_decision_source": latest_decision_source,
        "latest_stage": latest_entry["stage"] if latest_entry else None,
        "latest_stage_status": latest_entry["stage_status"] if latest_entry else None,
        "review_status": "review_available" if artifact_paths.get("review_path") is not None else None,
        "section_id": section_id,
        "summary_sources": {
            "approval_status": "approval" if artifact_paths.get("approval_path") is not None else None,
            "latest_decision": (
                "approval.selected_mode"
                if approval_payload.get("selected_mode") is not None
                else "preview.recommended_mode"
                if preview_payload.get("recommended_mode") is not None
                else None
            ),
            "latest_stage": "lifecycle_trace",
            "latest_stage_status": "lifecycle_trace",
            "review_status": "review" if artifact_paths.get("review_path") is not None else None,
        },
    }


def _write_state(
    state_path: Path,
    section_id: str,
    stage: str,
    *,
    status: str = "running",
    tasks_completed: int = 0,
    tasks_failed: int = 0,
) -> None:
    """Persist the current lifecycle stage for one workspace run."""
    _write_json(
        state_path,
        {
            "section_id": section_id,
            "stage": stage,
            "status": status,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed,
        },
    )


def _schema_error(artifact_name: str, message: str) -> None:
    raise ValueError(f"{artifact_name} schema validation failed: {message}")


def _with_artifact_metadata(artifact_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "generated_by": DGCE_ARTIFACT_GENERATED_BY,
        "schema_version": DGCE_ARTIFACT_SCHEMA_VERSION,
        **payload,
    }


def _expect_dict(value: Any, artifact_name: str, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _schema_error(artifact_name, f"{field_name} must be a dict")
    return value


def _expect_list(value: Any, artifact_name: str, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        _schema_error(artifact_name, f"{field_name} must be a list")
    return value


def _expect_required_field(container: dict[str, Any], key: str, artifact_name: str) -> Any:
    if key not in container:
        _schema_error(artifact_name, f"missing required field: {key}")
    return container[key]


def _expect_optional_str(value: Any, artifact_name: str, field_name: str) -> None:
    if value is not None and not isinstance(value, str):
        _schema_error(artifact_name, f"{field_name} must be a str or null")


def _expect_optional_bool(value: Any, artifact_name: str, field_name: str) -> None:
    if value is not None and not isinstance(value, bool):
        _schema_error(artifact_name, f"{field_name} must be a bool or null")


def _expect_optional_int(value: Any, artifact_name: str, field_name: str) -> None:
    if value is not None and not isinstance(value, int):
        _schema_error(artifact_name, f"{field_name} must be an int or null")


def _expect_str(value: Any, artifact_name: str, field_name: str) -> None:
    if not isinstance(value, str):
        _schema_error(artifact_name, f"{field_name} must be a str")


def _expect_bool(value: Any, artifact_name: str, field_name: str) -> None:
    if not isinstance(value, bool):
        _schema_error(artifact_name, f"{field_name} must be a bool")


def _expect_int(value: Any, artifact_name: str, field_name: str) -> None:
    if not isinstance(value, int):
        _schema_error(artifact_name, f"{field_name} must be an int")


def _expect_str_list(value: Any, artifact_name: str, field_name: str) -> None:
    items = _expect_list(value, artifact_name, field_name)
    for index, item in enumerate(items):
        _expect_str(item, artifact_name, f"{field_name}[{index}]")


def _validate_section_summary_schema(summary: Any, artifact_name: str, field_name: str = "section_summary") -> None:
    payload = _expect_dict(summary, artifact_name, field_name)
    _expect_optional_str(_expect_required_field(payload, "approval_status", artifact_name), artifact_name, f"{field_name}.approval_status")
    _expect_optional_str(_expect_required_field(payload, "decision_source", artifact_name), artifact_name, f"{field_name}.decision_source")
    _expect_optional_str(_expect_required_field(payload, "latest_decision", artifact_name), artifact_name, f"{field_name}.latest_decision")
    _expect_optional_str(_expect_required_field(payload, "latest_decision_source", artifact_name), artifact_name, f"{field_name}.latest_decision_source")
    _expect_optional_str(_expect_required_field(payload, "latest_stage", artifact_name), artifact_name, f"{field_name}.latest_stage")
    _expect_optional_str(_expect_required_field(payload, "latest_stage_status", artifact_name), artifact_name, f"{field_name}.latest_stage_status")
    _expect_optional_str(_expect_required_field(payload, "review_status", artifact_name), artifact_name, f"{field_name}.review_status")
    _expect_str(_expect_required_field(payload, "section_id", artifact_name), artifact_name, f"{field_name}.section_id")
    summary_sources = _expect_dict(_expect_required_field(payload, "summary_sources", artifact_name), artifact_name, f"{field_name}.summary_sources")
    for source_key in ("approval_status", "latest_decision", "latest_stage", "latest_stage_status", "review_status"):
        _expect_optional_str(_expect_required_field(summary_sources, source_key, artifact_name), artifact_name, f"{field_name}.summary_sources.{source_key}")


def _validate_artifact_metadata(payload: dict[str, Any], artifact_name: str, artifact_type: str) -> None:
    _expect_str(_expect_required_field(payload, "artifact_type", artifact_name), artifact_name, "artifact_type")
    _expect_str(_expect_required_field(payload, "generated_by", artifact_name), artifact_name, "generated_by")
    _expect_str(_expect_required_field(payload, "schema_version", artifact_name), artifact_name, "schema_version")
    if payload["artifact_type"] != artifact_type:
        _schema_error(artifact_name, f"artifact_type must be {artifact_type}")
    if payload["generated_by"] != DGCE_ARTIFACT_GENERATED_BY:
        _schema_error(artifact_name, f"generated_by must be {DGCE_ARTIFACT_GENERATED_BY}")
    if payload["schema_version"] != DGCE_ARTIFACT_SCHEMA_VERSION:
        _schema_error(artifact_name, f"schema_version must be {DGCE_ARTIFACT_SCHEMA_VERSION}")


def _validate_review_index_schema(payload: Any) -> None:
    artifact_name = "reviews/index.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "review_index")
    _expect_str_list(_expect_required_field(artifact, "section_order", artifact_name), artifact_name, "section_order")
    sections = _expect_list(_expect_required_field(artifact, "sections", artifact_name), artifact_name, "sections")
    summary = _expect_dict(_expect_required_field(artifact, "summary", artifact_name), artifact_name, "summary")
    for key in ("sections_with_approval", "sections_with_execution", "sections_with_outputs", "sections_with_review", "total_sections_seen"):
        _expect_int(_expect_required_field(summary, key, artifact_name), artifact_name, f"summary.{key}")
    for index, section in enumerate(sections):
        entry = _expect_dict(section, artifact_name, f"sections[{index}]")
        _expect_int(_expect_required_field(entry, "entry_order", artifact_name), artifact_name, f"sections[{index}].entry_order")
        _expect_str(_expect_required_field(entry, "section_id", artifact_name), artifact_name, f"sections[{index}].section_id")
        for key in (
            "preview_path",
            "review_path",
            "approval_path",
            "execution_path",
            "output_path",
            "lifecycle_trace_path",
            "approval_status",
            "selected_mode",
            "preflight_status",
            "stale_status",
            "gate_status",
            "alignment_status",
            "execution_status",
            "decision_source",
            "review_status",
            "latest_decision",
            "latest_decision_source",
            "approval_timestamp",
        ):
            _expect_optional_str(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        for key in ("execution_permitted", "stale_detected", "execution_allowed", "execution_blocked", "alignment_blocked", "approval_consumed"):
            _expect_optional_bool(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        review_approval_summary = _expect_dict(_expect_required_field(entry, "review_approval_summary", artifact_name), artifact_name, f"sections[{index}].review_approval_summary")
        for key in ("approval_status", "decision_source", "latest_decision", "latest_decision_source", "review_status"):
            _expect_optional_str(_expect_required_field(review_approval_summary, key, artifact_name), artifact_name, f"sections[{index}].review_approval_summary.{key}")
        _validate_section_summary_schema(_expect_required_field(entry, "section_summary", artifact_name), artifact_name, f"sections[{index}].section_summary")
        navigation_links = _expect_list(_expect_required_field(entry, "navigation_links", artifact_name), artifact_name, f"sections[{index}].navigation_links")
        for link_index, link in enumerate(navigation_links):
            link_entry = _expect_dict(link, artifact_name, f"sections[{index}].navigation_links[{link_index}]")
            _expect_str(_expect_required_field(link_entry, "link_role", artifact_name), artifact_name, f"sections[{index}].navigation_links[{link_index}].link_role")
            _expect_optional_str(_expect_required_field(link_entry, "path", artifact_name), artifact_name, f"sections[{index}].navigation_links[{link_index}].path")


def _validate_lifecycle_trace_schema(payload: Any) -> None:
    artifact_name = "lifecycle_trace.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "lifecycle_trace")
    _expect_str_list(_expect_required_field(artifact, "lifecycle_order", artifact_name), artifact_name, "lifecycle_order")
    _expect_int(_expect_required_field(artifact, "total_sections_seen", artifact_name), artifact_name, "total_sections_seen")
    sections = _expect_list(_expect_required_field(artifact, "sections", artifact_name), artifact_name, "sections")
    for index, section in enumerate(sections):
        entry = _expect_dict(section, artifact_name, f"sections[{index}]")
        for key in ("section_id", "approval_status", "decision_source", "review_status", "latest_decision", "latest_decision_source", "latest_stage", "latest_stage_status"):
            validator = _expect_str if key == "section_id" else _expect_optional_str
            validator(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        _validate_section_summary_schema(_expect_required_field(entry, "section_summary", artifact_name), artifact_name, f"sections[{index}].section_summary")
        trace_summary = _expect_dict(_expect_required_field(entry, "trace_summary", artifact_name), artifact_name, f"sections[{index}].trace_summary")
        for key in ("available_artifact_count", "completed_stage_count", "trace_entry_count"):
            _expect_int(_expect_required_field(trace_summary, key, artifact_name), artifact_name, f"sections[{index}].trace_summary.{key}")
        for key in ("approval_status", "decision_source", "latest_decision", "latest_decision_source", "latest_stage", "latest_stage_status", "review_status"):
            _expect_optional_str(_expect_required_field(trace_summary, key, artifact_name), artifact_name, f"sections[{index}].trace_summary.{key}")
        _expect_str(_expect_required_field(trace_summary, "section_id", artifact_name), artifact_name, f"sections[{index}].trace_summary.section_id")
        trace_entries = _expect_list(_expect_required_field(entry, "trace_entries", artifact_name), artifact_name, f"sections[{index}].trace_entries")
        for trace_index, trace_entry in enumerate(trace_entries):
            trace_payload = _expect_dict(trace_entry, artifact_name, f"sections[{index}].trace_entries[{trace_index}]")
            _expect_optional_str(_expect_required_field(trace_payload, "artifact_path", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].artifact_path")
            _expect_bool(_expect_required_field(trace_payload, "artifact_present", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].artifact_present")
            _expect_str(_expect_required_field(trace_payload, "stage", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].stage")
            _expect_int(_expect_required_field(trace_payload, "stage_order", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].stage_order")
            _expect_optional_str(_expect_required_field(trace_payload, "stage_status", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].stage_status")
            linkage = _expect_list(_expect_required_field(trace_payload, "linkage", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].linkage")
            for link_index, link in enumerate(linkage):
                link_entry = _expect_dict(link, artifact_name, f"sections[{index}].trace_entries[{trace_index}].linkage[{link_index}]")
                _expect_str(_expect_required_field(link_entry, "ref_name", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].linkage[{link_index}].ref_name")
                _expect_optional_str(_expect_required_field(link_entry, "ref_path", artifact_name), artifact_name, f"sections[{index}].trace_entries[{trace_index}].linkage[{link_index}].ref_path")


def _validate_workspace_summary_schema(payload: Any) -> None:
    artifact_name = "workspace_summary.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "workspace_summary")
    _expect_int(_expect_required_field(artifact, "total_sections_seen", artifact_name), artifact_name, "total_sections_seen")
    sections = _expect_list(_expect_required_field(artifact, "sections", artifact_name), artifact_name, "sections")
    for index, section in enumerate(sections):
        entry = _expect_dict(section, artifact_name, f"sections[{index}]")
        _expect_str(_expect_required_field(entry, "section_id", artifact_name), artifact_name, f"sections[{index}].section_id")
        for key in (
            "latest_run_mode",
            "latest_run_outcome_class",
            "latest_status",
            "latest_advisory_type",
            "preview_path",
            "review_path",
            "preview_outcome_class",
            "recommended_mode",
            "approval_path",
            "approval_status",
            "selected_mode",
            "preflight_path",
            "preflight_status",
            "stale_check_path",
            "stale_status",
            "execution_gate_path",
            "gate_status",
            "alignment_path",
            "alignment_status",
            "execution_path",
            "execution_status",
            "approval_status_after",
            "decision_source",
            "review_status",
            "latest_decision",
            "latest_decision_source",
            "latest_stage",
            "latest_stage_status",
        ):
            _expect_optional_str(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        for key in (
            "latest_validation_ok",
            "execution_permitted",
            "stale_detected",
            "execution_allowed",
            "execution_blocked",
            "alignment_blocked",
            "approval_consumed",
        ):
            _expect_optional_bool(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        for key in ("latest_written_files_count", "latest_skipped_modify_count", "latest_skipped_ignore_count"):
            _expect_int(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        advisory_explanation = _expect_required_field(entry, "latest_advisory_explanation", artifact_name)
        if advisory_explanation is not None:
            _expect_list(advisory_explanation, artifact_name, f"sections[{index}].latest_advisory_explanation")
        _validate_section_summary_schema(_expect_required_field(entry, "section_summary", artifact_name), artifact_name, f"sections[{index}].section_summary")


def _validate_workspace_index_schema(payload: Any) -> None:
    artifact_name = "workspace_index.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "workspace_index")
    artifact_paths = _expect_dict(_expect_required_field(artifact, "artifact_paths", artifact_name), artifact_name, "artifact_paths")
    for key in ("lifecycle_trace_path", "review_index_path", "workspace_summary_path"):
        _expect_str(_expect_required_field(artifact_paths, key, artifact_name), artifact_name, f"artifact_paths.{key}")
    _expect_str_list(_expect_required_field(artifact, "section_order", artifact_name), artifact_name, "section_order")
    sections = _expect_list(_expect_required_field(artifact, "sections", artifact_name), artifact_name, "sections")
    summary = _expect_dict(_expect_required_field(artifact, "summary", artifact_name), artifact_name, "summary")
    for key in ("sections_with_execution", "sections_with_lifecycle_trace", "sections_with_outputs", "total_sections_seen"):
        _expect_int(_expect_required_field(summary, key, artifact_name), artifact_name, f"summary.{key}")
    latest_stage_counts = _expect_list(_expect_required_field(summary, "latest_stage_counts", artifact_name), artifact_name, "summary.latest_stage_counts")
    for count_index, count_entry in enumerate(latest_stage_counts):
        count_payload = _expect_dict(count_entry, artifact_name, f"summary.latest_stage_counts[{count_index}]")
        _expect_int(_expect_required_field(count_payload, "section_count", artifact_name), artifact_name, f"summary.latest_stage_counts[{count_index}].section_count")
        _expect_str(_expect_required_field(count_payload, "stage", artifact_name), artifact_name, f"summary.latest_stage_counts[{count_index}].stage")
    for index, section in enumerate(sections):
        entry = _expect_dict(section, artifact_name, f"sections[{index}]")
        _expect_int(_expect_required_field(entry, "entry_order", artifact_name), artifact_name, f"sections[{index}].entry_order")
        _expect_optional_int(_expect_required_field(entry, "trace_entry_count", artifact_name), artifact_name, f"sections[{index}].trace_entry_count")
        for key in ("section_id", "execution_status", "approval_status", "decision_source", "review_status", "latest_decision", "latest_decision_source", "latest_run_outcome_class", "latest_stage", "latest_stage_status", "lifecycle_trace_path", "execution_path", "output_path"):
            validator = _expect_str if key in {"section_id", "lifecycle_trace_path"} else _expect_optional_str
            validator(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        artifact_links = _expect_list(_expect_required_field(entry, "artifact_links", artifact_name), artifact_name, f"sections[{index}].artifact_links")
        for link_index, link in enumerate(artifact_links):
            link_entry = _expect_dict(link, artifact_name, f"sections[{index}].artifact_links[{link_index}]")
            _expect_str(_expect_required_field(link_entry, "artifact_role", artifact_name), artifact_name, f"sections[{index}].artifact_links[{link_index}].artifact_role")
            _expect_str(_expect_required_field(link_entry, "path", artifact_name), artifact_name, f"sections[{index}].artifact_links[{link_index}].path")
        _validate_section_summary_schema(_expect_required_field(entry, "section_summary", artifact_name), artifact_name, f"sections[{index}].section_summary")
        _expect_dict(_expect_required_field(entry, "trace_summary", artifact_name), artifact_name, f"sections[{index}].trace_summary")


def _validate_dashboard_schema(payload: Any) -> None:
    artifact_name = "dashboard.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "dashboard")
    artifact_paths = _expect_dict(_expect_required_field(artifact, "artifact_paths", artifact_name), artifact_name, "artifact_paths")
    for key in ("lifecycle_trace_path", "review_index_path", "workspace_index_path"):
        _expect_str(_expect_required_field(artifact_paths, key, artifact_name), artifact_name, f"artifact_paths.{key}")
    _expect_str_list(_expect_required_field(artifact, "section_order", artifact_name), artifact_name, "section_order")
    sections = _expect_list(_expect_required_field(artifact, "sections", artifact_name), artifact_name, "sections")
    summary = _expect_dict(_expect_required_field(artifact, "summary", artifact_name), artifact_name, "summary")
    for key in ("approval_status_counts", "current_stage_counts", "review_status_counts", "stage_status_counts"):
        count_entries = _expect_list(_expect_required_field(summary, key, artifact_name), artifact_name, f"summary.{key}")
        for count_index, count_entry in enumerate(count_entries):
            count_payload = _expect_dict(count_entry, artifact_name, f"summary.{key}[{count_index}]")
            _expect_int(_expect_required_field(count_payload, "section_count", artifact_name), artifact_name, f"summary.{key}[{count_index}].section_count")
            _expect_str(_expect_required_field(count_payload, "value", artifact_name), artifact_name, f"summary.{key}[{count_index}].value")
    _expect_int(_expect_required_field(summary, "total_sections", artifact_name), artifact_name, "summary.total_sections")
    for index, section in enumerate(sections):
        entry = _expect_dict(section, artifact_name, f"sections[{index}]")
        _expect_int(_expect_required_field(entry, "entry_order", artifact_name), artifact_name, f"sections[{index}].entry_order")
        for key in ("section_id", "current_stage", "stage_status", "latest_decision", "decision_source", "approval_status", "review_status"):
            validator = _expect_str if key == "section_id" else _expect_optional_str
            validator(_expect_required_field(entry, key, artifact_name), artifact_name, f"sections[{index}].{key}")
        progress = _expect_dict(_expect_required_field(entry, "progress", artifact_name), artifact_name, f"sections[{index}].progress")
        for key in ("available_artifact_count", "completed_stage_count", "lifecycle_stage_count", "trace_entry_count"):
            _expect_int(_expect_required_field(progress, key, artifact_name), artifact_name, f"sections[{index}].progress.{key}")
        navigation_links = _expect_dict(_expect_required_field(entry, "navigation_links", artifact_name), artifact_name, f"sections[{index}].navigation_links")
        for key in ("approval", "execution", "lifecycle_trace", "outputs", "review"):
            _expect_optional_str(_expect_required_field(navigation_links, key, artifact_name), artifact_name, f"sections[{index}].navigation_links.{key}")
        _validate_section_summary_schema(_expect_required_field(entry, "section_summary", artifact_name), artifact_name, f"sections[{index}].section_summary")


def _validate_execution_output_schema(payload: Any) -> None:
    artifact_name = "outputs"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "output_record")
    for key in ("section_id", "run_mode", "run_outcome_class"):
        _expect_str(_expect_required_field(artifact, key, artifact_name), artifact_name, key)
    file_plan = _expect_dict(_expect_required_field(artifact, "file_plan", artifact_name), artifact_name, "file_plan")
    _expect_str(_expect_required_field(file_plan, "project_name", artifact_name), artifact_name, "file_plan.project_name")
    files = _expect_list(_expect_required_field(file_plan, "files", artifact_name), artifact_name, "file_plan.files")
    for index, file_entry in enumerate(files):
        payload_entry = _expect_dict(file_entry, artifact_name, f"file_plan.files[{index}]")
        for key in ("path", "purpose", "source"):
            _expect_str(_expect_required_field(payload_entry, key, artifact_name), artifact_name, f"file_plan.files[{index}].{key}")
    execution_outcome = _expect_dict(_expect_required_field(artifact, "execution_outcome", artifact_name), artifact_name, "execution_outcome")
    for key in ("section_id", "stage", "status"):
        _expect_str(_expect_required_field(execution_outcome, key, artifact_name), artifact_name, f"execution_outcome.{key}")
    validation_summary = _expect_dict(_expect_required_field(execution_outcome, "validation_summary", artifact_name), artifact_name, "execution_outcome.validation_summary")
    _expect_bool(_expect_required_field(validation_summary, "ok", artifact_name), artifact_name, "execution_outcome.validation_summary.ok")
    if _expect_required_field(validation_summary, "error", artifact_name) is not None:
        _expect_str(validation_summary["error"], artifact_name, "execution_outcome.validation_summary.error")
    _expect_list(_expect_required_field(validation_summary, "missing_keys", artifact_name), artifact_name, "execution_outcome.validation_summary.missing_keys")
    change_plan_summary = _expect_dict(_expect_required_field(execution_outcome, "change_plan_summary", artifact_name), artifact_name, "execution_outcome.change_plan_summary")
    execution_summary = _expect_dict(_expect_required_field(execution_outcome, "execution_summary", artifact_name), artifact_name, "execution_outcome.execution_summary")
    for key in ("create_count", "ignore_count", "modify_count"):
        _expect_int(_expect_required_field(change_plan_summary, key, artifact_name), artifact_name, f"execution_outcome.change_plan_summary.{key}")
    for key in (
        "written_files_count",
        "skipped_modify_count",
        "skipped_ignore_count",
        "skipped_identical_count",
        "skipped_ownership_count",
        "skipped_exists_fallback_count",
    ):
        _expect_int(_expect_required_field(execution_summary, key, artifact_name), artifact_name, f"execution_outcome.execution_summary.{key}")
    advisory = _expect_required_field(artifact, "advisory", artifact_name)
    if advisory is not None:
        _expect_dict(advisory, artifact_name, "advisory")
    generated_artifacts = _expect_list(_expect_required_field(artifact, "generated_artifacts", artifact_name), artifact_name, "generated_artifacts")
    for index, artifact_entry in enumerate(generated_artifacts):
        payload_entry = _expect_dict(artifact_entry, artifact_name, f"generated_artifacts[{index}]")
        for key in ("artifact_id", "artifact_kind", "implementation_unit", "path", "producer_ref", "purpose", "source", "write_decision", "write_reason"):
            _expect_str(_expect_required_field(payload_entry, key, artifact_name), artifact_name, f"generated_artifacts[{index}].{key}")
        _expect_int(_expect_required_field(payload_entry, "bytes_written", artifact_name), artifact_name, f"generated_artifacts[{index}].bytes_written")
    output_summary = _expect_dict(_expect_required_field(artifact, "output_summary", artifact_name), artifact_name, "output_summary")
    for key in ("artifact_count", "written_artifact_count"):
        _expect_int(_expect_required_field(output_summary, key, artifact_name), artifact_name, f"output_summary.{key}")
    for key in ("execution_status", "run_outcome_class", "section_id"):
        _expect_str(_expect_required_field(output_summary, key, artifact_name), artifact_name, f"output_summary.{key}")
    _expect_optional_str(_expect_required_field(output_summary, "primary_artifact_path", artifact_name), artifact_name, "output_summary.primary_artifact_path")
    _expect_dict(_expect_required_field(output_summary, "execution_summary", artifact_name), artifact_name, "output_summary.execution_summary")
    _expect_str_list(_expect_required_field(output_summary, "sources", artifact_name), artifact_name, "output_summary.sources")
    write_transparency = _expect_dict(_expect_required_field(artifact, "write_transparency", artifact_name), artifact_name, "write_transparency")
    _expect_list(_expect_required_field(write_transparency, "write_decisions", artifact_name), artifact_name, "write_transparency.write_decisions")
    _expect_dict(_expect_required_field(write_transparency, "write_summary", artifact_name), artifact_name, "write_transparency.write_summary")


def _validate_execution_stamp_schema(payload: Any) -> None:
    artifact_name = "execution_stamp"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "execution_record")
    for key in (
        "section_id",
        "execution_status",
        "approval_path",
        "preflight_path",
        "execution_gate_path",
        "alignment_path",
        "outputs_path",
        "selected_mode",
        "effective_execution_mode",
        "approval_status_before",
        "approval_status_after",
        "run_outcome_class",
        "execution_timestamp",
    ):
        validator = _expect_str if key in {"section_id", "execution_status", "effective_execution_mode", "execution_timestamp"} else _expect_optional_str
        validator(_expect_required_field(artifact, key, artifact_name), artifact_name, key)
    for key in ("governed_execution", "require_preflight_pass", "approval_consumed", "execution_blocked"):
        _expect_bool(_expect_required_field(artifact, key, artifact_name), artifact_name, key)
    for key in ("written_file_count", "modify_written_count", "created_written_count"):
        _expect_int(_expect_required_field(artifact, key, artifact_name), artifact_name, key)
    linked_artifacts = _expect_list(_expect_required_field(artifact, "linked_artifacts", artifact_name), artifact_name, "linked_artifacts")
    for index, link in enumerate(linked_artifacts):
        payload_entry = _expect_dict(link, artifact_name, f"linked_artifacts[{index}]")
        _expect_str(_expect_required_field(payload_entry, "artifact_role", artifact_name), artifact_name, f"linked_artifacts[{index}].artifact_role")
        _expect_optional_str(_expect_required_field(payload_entry, "artifact_path", artifact_name), artifact_name, f"linked_artifacts[{index}].artifact_path")
        _expect_bool(_expect_required_field(payload_entry, "present", artifact_name), artifact_name, f"linked_artifacts[{index}].present")
    artifact_results = _expect_list(_expect_required_field(artifact, "artifact_results", artifact_name), artifact_name, "artifact_results")
    for index, artifact_entry in enumerate(artifact_results):
        payload_entry = _expect_dict(artifact_entry, artifact_name, f"artifact_results[{index}]")
        for key in ("artifact_id", "artifact_kind", "implementation_unit", "path", "producer_ref", "result_status", "source", "write_decision", "write_reason"):
            _expect_str(_expect_required_field(payload_entry, key, artifact_name), artifact_name, f"artifact_results[{index}].{key}")
        _expect_int(_expect_required_field(payload_entry, "bytes_written", artifact_name), artifact_name, f"artifact_results[{index}].bytes_written")
    unit_results = _expect_list(_expect_required_field(artifact, "unit_results", artifact_name), artifact_name, "unit_results")
    for index, unit_entry in enumerate(unit_results):
        payload_entry = _expect_dict(unit_entry, artifact_name, f"unit_results[{index}]")
        for key in ("artifact_count", "skipped_artifact_count", "written_artifact_count"):
            _expect_int(_expect_required_field(payload_entry, key, artifact_name), artifact_name, f"unit_results[{index}].{key}")
        _expect_str(_expect_required_field(payload_entry, "unit_id", artifact_name), artifact_name, f"unit_results[{index}].unit_id")
        _expect_str(_expect_required_field(payload_entry, "unit_status", artifact_name), artifact_name, f"unit_results[{index}].unit_status")
        _expect_str_list(_expect_required_field(payload_entry, "paths", artifact_name), artifact_name, f"unit_results[{index}].paths")
    for key in ("executed_units", "skipped_units", "failed_units"):
        _expect_str_list(_expect_required_field(artifact, key, artifact_name), artifact_name, key)
    model_execution = artifact.get("model_execution")
    if model_execution is not None:
        model_payload = _expect_dict(model_execution, artifact_name, "model_execution")
        _expect_str(_expect_required_field(model_payload, "provider", artifact_name), artifact_name, "model_execution.provider")
        _expect_str(_expect_required_field(model_payload, "model_id", artifact_name), artifact_name, "model_execution.model_id")
        _expect_str(
            _expect_required_field(model_payload, "prompt_template_version", artifact_name),
            artifact_name,
            "model_execution.prompt_template_version",
        )
        temperature = _expect_required_field(model_payload, "temperature", artifact_name)
        if not isinstance(temperature, (int, float)) or isinstance(temperature, bool):
            _schema_error(artifact_name, "model_execution.temperature must be a float")
        _expect_str(_expect_required_field(model_payload, "postprocess", artifact_name), artifact_name, "model_execution.postprocess")
    execution_record_summary = _expect_dict(_expect_required_field(artifact, "execution_record_summary", artifact_name), artifact_name, "execution_record_summary")
    for key in ("execution_status",):
        _expect_str(_expect_required_field(execution_record_summary, key, artifact_name), artifact_name, f"execution_record_summary.{key}")
    for key in ("run_outcome_class",):
        _expect_optional_str(_expect_required_field(execution_record_summary, key, artifact_name), artifact_name, f"execution_record_summary.{key}")
    _expect_bool(_expect_required_field(execution_record_summary, "execution_blocked", artifact_name), artifact_name, "execution_record_summary.execution_blocked")
    for key in ("executed_unit_count", "linked_artifact_count", "result_artifact_count", "skipped_artifact_count", "skipped_unit_count", "written_artifact_count"):
        _expect_int(_expect_required_field(execution_record_summary, key, artifact_name), artifact_name, f"execution_record_summary.{key}")
    prepared_plan_audit_manifest = artifact.get("prepared_plan_audit_manifest")
    prepared_plan_audit_fingerprint = artifact.get("prepared_plan_audit_fingerprint")
    prepared_plan_cross_link = artifact.get("prepared_plan_cross_link")
    prepared_plan_cross_link_fingerprint = artifact.get("prepared_plan_cross_link_fingerprint")
    cross_link_fields_present = any(
        value is not None
        for value in (
            prepared_plan_cross_link,
            prepared_plan_cross_link_fingerprint,
        )
    )
    if cross_link_fields_present:
        audit_manifest = _expect_dict(
            _expect_required_field(artifact, "prepared_plan_audit_manifest", artifact_name),
            artifact_name,
            "prepared_plan_audit_manifest",
        )
        audit_fingerprint = _expect_required_field(artifact, "prepared_plan_audit_fingerprint", artifact_name)
        _expect_str(audit_fingerprint, artifact_name, "prepared_plan_audit_fingerprint")
        audit_manifest_prepared_plan_path = _expect_required_field(audit_manifest, "prepared_plan_path", artifact_name)
        _expect_str(audit_manifest_prepared_plan_path, artifact_name, "prepared_plan_audit_manifest.prepared_plan_path")
        audit_manifest_prepared_plan_fingerprint = _expect_required_field(audit_manifest, "prepared_plan_fingerprint", artifact_name)
        _expect_str(audit_manifest_prepared_plan_fingerprint, artifact_name, "prepared_plan_audit_manifest.prepared_plan_fingerprint")
        audit_manifest_section_id = _expect_required_field(audit_manifest, "section_id", artifact_name)
        _expect_str(audit_manifest_section_id, artifact_name, "prepared_plan_audit_manifest.section_id")
        cross_link = _expect_dict(
            _expect_required_field(artifact, "prepared_plan_cross_link", artifact_name),
            artifact_name,
            "prepared_plan_cross_link",
        )
        for key in ("prepared_plan_audit_fingerprint", "prepared_plan_fingerprint", "prepared_plan_path", "section_id"):
            _expect_str(_expect_required_field(cross_link, key, artifact_name), artifact_name, f"prepared_plan_cross_link.{key}")
        prepared_plan_cross_link_fingerprint = _expect_required_field(artifact, "prepared_plan_cross_link_fingerprint", artifact_name)
        _expect_str(prepared_plan_cross_link_fingerprint, artifact_name, "prepared_plan_cross_link_fingerprint")
        if cross_link["section_id"] != artifact["section_id"]:
            _schema_error(artifact_name, "prepared_plan_cross_link.section_id must match section_id")
        expected_plan_path = f".dce/plans/{artifact['section_id']}.prepared_plan.json"
        if cross_link["prepared_plan_path"] != expected_plan_path:
            _schema_error(artifact_name, "prepared_plan_cross_link.prepared_plan_path must match section prepared-plan path")
        if audit_manifest_prepared_plan_path != cross_link["prepared_plan_path"]:
            _schema_error(artifact_name, "prepared_plan_cross_link.prepared_plan_path must match prepared_plan_audit_manifest.prepared_plan_path")
        if audit_manifest_prepared_plan_fingerprint != cross_link["prepared_plan_fingerprint"]:
            _schema_error(artifact_name, "prepared_plan_cross_link.prepared_plan_fingerprint must match prepared_plan_audit_manifest.prepared_plan_fingerprint")


def _validate_artifact_manifest_schema(payload: Any) -> None:
    artifact_name = "artifact_manifest.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "artifact_manifest")
    artifacts = _expect_list(_expect_required_field(artifact, "artifacts", artifact_name), artifact_name, "artifacts")
    for index, entry in enumerate(artifacts):
        manifest_entry = _expect_dict(entry, artifact_name, f"artifacts[{index}]")
        _expect_str(_expect_required_field(manifest_entry, "artifact_path", artifact_name), artifact_name, f"artifacts[{index}].artifact_path")
        _expect_str(_expect_required_field(manifest_entry, "artifact_type", artifact_name), artifact_name, f"artifacts[{index}].artifact_type")
        _expect_str(_expect_required_field(manifest_entry, "schema_version", artifact_name), artifact_name, f"artifacts[{index}].schema_version")
        _expect_str(_expect_required_field(manifest_entry, "scope", artifact_name), artifact_name, f"artifacts[{index}].scope")
        _expect_optional_str(_expect_required_field(manifest_entry, "section_id", artifact_name), artifact_name, f"artifacts[{index}].section_id")
        if manifest_entry["scope"] not in {"workspace", "section"}:
            _schema_error(artifact_name, f"artifacts[{index}].scope must be workspace or section")


def _validate_consumer_contract_schema(payload: Any) -> None:
    artifact_name = "consumer_contract.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "consumer_contract")
    supported_artifacts = _expect_list(
        _expect_required_field(artifact, "supported_artifacts", artifact_name),
        artifact_name,
        "supported_artifacts",
    )
    for index, entry in enumerate(supported_artifacts):
        supported_entry = _expect_dict(entry, artifact_name, f"supported_artifacts[{index}]")
        _expect_str(_expect_required_field(supported_entry, "artifact_type", artifact_name), artifact_name, f"supported_artifacts[{index}].artifact_type")
        _expect_str(_expect_required_field(supported_entry, "schema_version", artifact_name), artifact_name, f"supported_artifacts[{index}].schema_version")
        _expect_str(_expect_required_field(supported_entry, "artifact_path", artifact_name), artifact_name, f"supported_artifacts[{index}].artifact_path")
        _expect_str(_expect_required_field(supported_entry, "contract_stability", artifact_name), artifact_name, f"supported_artifacts[{index}].contract_stability")
        supported_fields = _expect_required_field(supported_entry, "supported_fields", artifact_name)
        _expect_str_list(supported_fields, artifact_name, f"supported_artifacts[{index}].supported_fields")
        export_scope = _expect_required_field(supported_entry, "export_scope", artifact_name)
        _expect_str(export_scope, artifact_name, f"supported_artifacts[{index}].export_scope")
        if export_scope not in {"external", "internal"}:
            _schema_error(artifact_name, f"supported_artifacts[{index}].export_scope must be external or internal")
        export_fields = supported_entry.get("export_fields")
        if export_fields is not None:
            _expect_str_list(export_fields, artifact_name, f"supported_artifacts[{index}].export_fields")
            supported_field_values = list(supported_fields)
            for export_field_index, export_field in enumerate(export_fields):
                if export_field not in supported_field_values:
                    _schema_error(
                        artifact_name,
                        f"supported_artifacts[{index}].export_fields[{export_field_index}] must be present in supported_fields",
                    )
        consumer_scopes = _expect_required_field(supported_entry, "consumer_scopes", artifact_name)
        if consumer_scopes is not None:
            _expect_str_list(consumer_scopes, artifact_name, f"supported_artifacts[{index}].consumer_scopes")


def _validate_export_contract_schema(payload: Any) -> None:
    artifact_name = "export_contract.json"
    artifact = _expect_dict(payload, artifact_name, artifact_name)
    _validate_artifact_metadata(artifact, artifact_name, "export_contract")
    supported_artifacts = _expect_list(
        _expect_required_field(artifact, "supported_artifacts", artifact_name),
        artifact_name,
        "supported_artifacts",
    )
    for index, entry in enumerate(supported_artifacts):
        export_entry = _expect_dict(entry, artifact_name, f"supported_artifacts[{index}]")
        _expect_str(_expect_required_field(export_entry, "artifact_type", artifact_name), artifact_name, f"supported_artifacts[{index}].artifact_type")
        _expect_str(_expect_required_field(export_entry, "schema_version", artifact_name), artifact_name, f"supported_artifacts[{index}].schema_version")
        _expect_str(_expect_required_field(export_entry, "artifact_path", artifact_name), artifact_name, f"supported_artifacts[{index}].artifact_path")
        _expect_str(_expect_required_field(export_entry, "contract_stability", artifact_name), artifact_name, f"supported_artifacts[{index}].contract_stability")
        export_scope = _expect_required_field(export_entry, "export_scope", artifact_name)
        _expect_str(export_scope, artifact_name, f"supported_artifacts[{index}].export_scope")
        if export_scope != "external":
            _schema_error(artifact_name, f"supported_artifacts[{index}].export_scope must be external")
        export_fields = _expect_required_field(export_entry, "export_fields", artifact_name)
        _expect_str_list(export_fields, artifact_name, f"supported_artifacts[{index}].export_fields")
        consumer_scopes = export_entry.get("consumer_scopes")
        if consumer_scopes is not None:
            _expect_str_list(consumer_scopes, artifact_name, f"supported_artifacts[{index}].consumer_scopes")


def _normalized_path_parts(path: Path) -> tuple[str, ...]:
    normalized = os.path.normpath(str(path))
    return tuple(Path(normalized).parts)


def _artifact_path_matches(path: Path, expected_parts: tuple[str, ...]) -> bool:
    parts = _normalized_path_parts(path)
    return len(parts) >= len(expected_parts) and tuple(parts[-len(expected_parts):]) == expected_parts


def _artifact_path_matches_outputs_json(path: Path) -> bool:
    parts = _normalized_path_parts(path)
    return (
        len(parts) >= 3
        and tuple(parts[-3:-1]) == (".dce", "outputs")
        and parts[-1].endswith(".json")
        and not parts[-1].endswith(".execution.json")
    )


def _artifact_path_matches_execution_json(path: Path) -> bool:
    parts = _normalized_path_parts(path)
    return len(parts) >= 3 and tuple(parts[-3:-1]) == (".dce", "execution") and parts[-1].endswith(".execution.json")


def _validate_locked_artifact_schema(path: Path, payload: object) -> None:
    if _artifact_path_matches(path, (".dce", "reviews", "index.json")):
        _validate_review_index_schema(payload)
    elif _artifact_path_matches(path, (".dce", "lifecycle_trace.json")):
        _validate_lifecycle_trace_schema(payload)
    elif _artifact_path_matches(path, (".dce", "workspace_summary.json")):
        _validate_workspace_summary_schema(payload)
    elif _artifact_path_matches(path, (".dce", "workspace_index.json")):
        _validate_workspace_index_schema(payload)
    elif _artifact_path_matches(path, (".dce", "dashboard.json")):
        _validate_dashboard_schema(payload)
    elif _artifact_path_matches(path, (".dce", "artifact_manifest.json")):
        _validate_artifact_manifest_schema(payload)
    elif _artifact_path_matches(path, (".dce", "consumer_contract.json")):
        _validate_consumer_contract_schema(payload)
    elif _artifact_path_matches(path, (".dce", "export_contract.json")):
        _validate_export_contract_schema(payload)
    elif _artifact_path_matches_outputs_json(path):
        _validate_execution_output_schema(payload)
    elif _artifact_path_matches_execution_json(path):
        _validate_execution_stamp_schema(payload)


def _write_json(path: Path, payload: object) -> None:
    """Persist deterministic JSON with stable formatting."""
    _validate_locked_artifact_schema(path, payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def compute_artifact_fingerprint(path: Path) -> str:
    """Return the SHA-256 hex digest for the exact bytes currently stored at path."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonicalize_fingerprint_value(value: Any) -> Any:
    """Return a recursively normalized value for deterministic fingerprinting."""
    if isinstance(value, dict):
        canonical_items: dict[str, Any] = {}
        for key in sorted(value):
            if key == "artifact_fingerprint" or key.endswith("_timestamp"):
                continue
            canonical_items[str(key)] = _canonicalize_fingerprint_value(value[key])
        return canonical_items

    if isinstance(value, list):
        canonical_items = [_canonicalize_fingerprint_value(item) for item in value]
        return sorted(
            canonical_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )

    return value


def _canonicalize_preview_fingerprint_value(value: Any) -> Any:
    """Return a preview-specific normalized value for deterministic preview fingerprinting."""
    if isinstance(value, dict):
        canonical_items: dict[str, Any] = {}
        for key in sorted(value):
            if key == "artifact_fingerprint" or key.endswith("_timestamp"):
                continue
            normalized_value = _canonicalize_preview_fingerprint_value(value[key])
            if key == "files" and isinstance(normalized_value, list):
                normalized_value = sorted(
                    normalized_value,
                    key=lambda item: (
                        str(item.get("path", "")) if isinstance(item, dict) else "",
                        str(item.get("purpose", "")) if isinstance(item, dict) else "",
                        json.dumps(item, sort_keys=True, separators=(",", ":")),
                    ),
                )
            elif key == "previews" and isinstance(normalized_value, list):
                normalized_value = sorted(
                    normalized_value,
                    key=lambda item: (
                        str(item.get("path", "")) if isinstance(item, dict) else "",
                        json.dumps(item, sort_keys=True, separators=(",", ":")),
                    ),
                )
            canonical_items[str(key)] = normalized_value
        return canonical_items

    if isinstance(value, list):
        canonical_items = [_canonicalize_preview_fingerprint_value(item) for item in value]
        return sorted(
            canonical_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )

    if isinstance(value, str):
        return value.strip()

    return value


def compute_json_payload_fingerprint(payload: dict[str, Any]) -> str:
    """Return the canonical SHA-256 fingerprint for deterministic JSON payload content."""
    canonical_payload = _canonicalize_fingerprint_value(payload)
    canonical_bytes = (json.dumps(canonical_payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def compute_preview_payload_fingerprint(payload: dict[str, Any]) -> str:
    """Return the canonical SHA-256 fingerprint for preview payload content."""
    canonical_payload = _canonicalize_preview_fingerprint_value(payload)
    canonical_bytes = (json.dumps(canonical_payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def _is_preview_artifact_payload(payload: dict[str, Any]) -> bool:
    """Return True when the payload matches the persisted DGCE preview-artifact shape."""
    return (
        isinstance(payload.get("section_id"), str)
        and isinstance(payload.get("mode"), str)
        and isinstance(payload.get("summary"), dict)
        and isinstance(payload.get("previews"), list)
        and isinstance(payload.get("preview_outcome_class"), str)
        and isinstance(payload.get("recommended_mode"), str)
    )


def _compute_json_artifact_fingerprint(payload: dict[str, Any]) -> str:
    """Return the appropriate canonical artifact fingerprint for one JSON payload."""
    if _is_preview_artifact_payload(payload):
        return compute_preview_payload_fingerprint(payload)
    return compute_json_payload_fingerprint(payload)


def compute_json_file_fingerprint(path: Path) -> str:
    """Return the canonical SHA-256 fingerprint for a JSON file's parsed payload."""
    return compute_json_payload_fingerprint(json.loads(path.read_text(encoding="utf-8")))


def verify_artifact_fingerprint(path: Path) -> bool:
    """Return True when a JSON artifact's stored fingerprint matches its canonical payload."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    stored_fingerprint = payload.get("artifact_fingerprint")
    if not isinstance(stored_fingerprint, str):
        return False
    return stored_fingerprint == _compute_json_artifact_fingerprint(payload)


def _write_json_with_artifact_fingerprint(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    """Persist a JSON artifact with a canonical fingerprint derived from payload content excluding the field itself."""
    stamped_payload = dict(payload)
    stamped_payload["artifact_fingerprint"] = _compute_json_artifact_fingerprint(payload)
    _write_json(path, stamped_payload)
    return stamped_payload


def compute_review_artifact_fingerprint(content: str) -> str:
    """Return the canonical SHA-256 fingerprint for review markdown before the fingerprint wrapper is inserted."""
    canonical_content = strip_review_fingerprint_wrapper(content)
    return hashlib.sha256(canonical_content.encode("utf-8")).hexdigest()


def strip_review_fingerprint_wrapper(content: str) -> str:
    """Return the canonical review markdown body with the fingerprint wrapper removed."""
    lines = content.splitlines()
    stripped_lines: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("- artifact_fingerprint: "):
            if stripped_lines and stripped_lines[-1] == "":
                stripped_lines.pop()
            index += 1
            continue
        stripped_lines.append(line)
        index += 1
    return "\n".join(stripped_lines) + "\n"


def verify_review_artifact_fingerprint(path: Path) -> bool:
    """Return True when a review markdown artifact's embedded fingerprint matches its canonical body."""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return False
    stored_fingerprint = None
    for line in content.splitlines():
        if line.startswith("- artifact_fingerprint: "):
            stored_fingerprint = line.removeprefix("- artifact_fingerprint: ").strip()
            break
    if not stored_fingerprint:
        return False
    return stored_fingerprint == compute_review_artifact_fingerprint(content)


def _write_review_with_artifact_fingerprint(path: Path, content: str) -> str:
    """Persist a markdown review artifact with a canonical fingerprint line derived from the body content."""
    fingerprint = compute_review_artifact_fingerprint(content)
    lines = content.splitlines()
    if lines:
        stamped = "\n".join([lines[0], "", f"- artifact_fingerprint: {fingerprint}", *lines[1:]]) + "\n"
    else:
        stamped = f"- artifact_fingerprint: {fingerprint}\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(stamped, encoding="utf-8")
    return stamped


def _build_response_envelope(task: ClassificationRequest, route_result: object) -> ResponseEnvelope:
    """Convert a route result into a DGCE response, hard-failing invalid structured artifacts."""
    execution_metadata = getattr(route_result, "execution_metadata", {}) or {}
    structured_content = getattr(route_result, "structured_content", None)
    normalized_output = (
        json.dumps(structured_content, sort_keys=True)
        if isinstance(structured_content, dict)
        else getattr(route_result, "output", "")
    )
    if execution_metadata.get("structure_valid") is False:
        return ResponseEnvelope(
            request_id=task.request_id,
            task_type=task.task_type or "",
            status="error",
            task_bucket=getattr(route_result, "task_bucket", ""),
            decision="ERROR",
            output=normalized_output,
            reused=getattr(route_result, "reused", False),
            structured_content=structured_content if isinstance(structured_content, dict) else None,
        )

    return ResponseEnvelope(
        request_id=task.request_id,
        task_type=task.task_type or "",
        status=getattr(route_result.status, "value", route_result.status),
        task_bucket=route_result.task_bucket,
        decision=route_result.decision,
        output=normalized_output,
        reused=route_result.reused,
        structured_content=structured_content if isinstance(structured_content, dict) else None,
    )


def _update_validation_summary(
    validation_summary: dict,
    task: ClassificationRequest,
    response: ResponseEnvelope,
    execution_metadata: dict,
) -> None:
    """Track the first structured validation failure across task execution."""
    schema_name = _schema_name_for_task_type(task.task_type)
    validation = _validate_write_stage_structured_content(task, response)
    if validation is not None:
        if validation.ok:
            return
        validation_summary["ok"] = False
        if validation_summary["error"] is None:
            validation_summary["error"] = validation.error
            validation_summary["missing_keys"] = list(validation.missing_keys)
        return

    if schema_name is not None:
        if not _should_use_primary_task_validation_metadata(task):
            return
        if execution_metadata.get("structure_valid") is not False:
            return
        missing_keys = list(execution_metadata.get("structure_missing_keys", []))
        if task.task_type == "data_model":
            assert "interfaces" not in missing_keys
        validation_summary["ok"] = False
        if validation_summary["error"] is None:
            validation_summary["error"] = execution_metadata.get("structure_error")
            validation_summary["missing_keys"] = missing_keys
        return

    if execution_metadata.get("structure_valid") is not False:
        return

    validation_summary["ok"] = False
    if validation_summary["error"] is None:
        validation_summary["error"] = execution_metadata.get("structure_error")
        validation_summary["missing_keys"] = list(execution_metadata.get("structure_missing_keys", []))


def _schema_name_for_task_type(task_type: str | None) -> str | None:
    """Return the DGCE structured schema for a task type without cross-schema fallback."""
    if task_type == "data_model":
        return "dgce_data_model_v1"
    if task_type == "api_surface":
        return "dgce_api_surface_v1"
    if task_type == "system_breakdown":
        return "dgce_system_breakdown_v1"
    return None


def _validate_write_stage_structured_content(
    task: ClassificationRequest,
    response: ResponseEnvelope,
):
    """Validate DGCE WRITE-stage structured content using strict task-type schema mapping."""
    schema_name = _schema_name_for_task_type(task.task_type)
    structured_content = response.structured_content
    if schema_name is None or not isinstance(structured_content, dict):
        return None

    validation = validate_output(schema_name, structured_content)
    if task.task_type == "data_model":
        assert "interfaces" not in validation.missing_keys
    return validation


def _should_use_primary_task_validation_metadata(task: ClassificationRequest) -> bool:
    """Allow metadata fallback only for the primary DGCE section task, never sibling structured tasks."""
    metadata = task.metadata or {}
    section_type = metadata.get("section_type")
    return isinstance(section_type, str) and task.task_type == section_type


def _task_status_after_execution(task: ClassificationRequest, response: ResponseEnvelope, validation_summary: dict) -> str:
    """Return the persisted DGCE task status after one task execution."""
    if response.status == "error":
        return "error"
    if task.task_type == "data_model":
        if response.structured_content is not None and validation_summary.get("ok") is True:
            return "completed"
        return "pending"
    return "completed"


def _build_execution_outcome(
    *,
    section_id: str,
    stage: str,
    validation_summary: dict,
    change_plan: List[Dict[str, str]],
    write_transparency: dict,
    failed_tasks: int,
) -> dict:
    """Build a deterministic DGCE execution outcome summary."""
    action_counts = {"create": 0, "modify": 0, "ignore": 0}
    for entry in change_plan:
        action = str(entry.get("action", "ignore"))
        if action in action_counts:
            action_counts[action] += 1

    write_summary = write_transparency.get("write_summary", {})
    skipped_modify_count = int(write_summary.get("skipped_modify_count", 0))
    skipped_ignore_count = int(write_summary.get("skipped_ignore_count", 0))
    skipped_identical_count = int(write_summary.get("skipped_identical_count", 0))
    skipped_ownership_count = int(write_summary.get("skipped_ownership_count", 0))
    skipped_exists_fallback_count = int(write_summary.get("skipped_exists_fallback_count", 0))

    return {
        "section_id": section_id,
        "stage": stage,
        "status": _outcome_status(
            failed_tasks=failed_tasks,
            skipped_modify_count=skipped_modify_count,
            skipped_ignore_count=skipped_ignore_count,
            skipped_ownership_count=skipped_ownership_count,
        ),
        "validation_summary": {
            "ok": bool(validation_summary["ok"]),
            "error": validation_summary["error"],
            "missing_keys": list(validation_summary["missing_keys"]),
        },
        "change_plan_summary": {
            "create_count": action_counts["create"],
            "modify_count": action_counts["modify"],
            "ignore_count": action_counts["ignore"],
        },
        "execution_summary": {
            "written_files_count": 0,
            "skipped_modify_count": skipped_modify_count,
            "skipped_ignore_count": skipped_ignore_count,
            "skipped_identical_count": skipped_identical_count,
            "skipped_ownership_count": skipped_ownership_count,
            "skipped_exists_fallback_count": skipped_exists_fallback_count,
        },
    }


def _outcome_status(
    *,
    failed_tasks: int,
    skipped_modify_count: int,
    skipped_ignore_count: int,
    skipped_ownership_count: int,
) -> str:
    """Return a deterministic high-level execution outcome status."""
    if failed_tasks > 0:
        return "error"
    if skipped_modify_count > 0 or skipped_ignore_count > 0 or skipped_ownership_count > 0:
        return "partial"
    return "success"


def _build_advisory_index_entry(
    section_id: str,
    run_mode: str,
    run_outcome_class: str,
    execution_outcome: dict,
    advisory: Optional[dict],
) -> dict:
    """Build the minimal advisory index payload for the current section run."""
    validation = execution_outcome.get("validation_summary", {})
    execution = execution_outcome.get("execution_summary", {})
    return {
        "section_id": section_id,
        "run_mode": run_mode,
        "run_outcome_class": run_outcome_class,
        "status": execution_outcome.get("status"),
        "validation_ok": validation.get("ok"),
        "advisory_type": advisory.get("type") if advisory else None,
        "advisory_explanation": advisory.get("explanation") if advisory else None,
        "written_files_count": execution.get("written_files_count", 0),
        "skipped_modify_count": execution.get("skipped_modify_count", 0),
        "skipped_ignore_count": execution.get("skipped_ignore_count", 0),
    }


def _build_output_artifact_payload(
    *,
    section_id: str,
    run_mode: str,
    run_outcome_class: str,
    file_plan: FilePlan,
    execution_outcome: dict,
    advisory: Optional[dict],
    write_transparency: dict[str, Any],
) -> dict[str, Any]:
    """Build the persisted output artifact payload for one DGCE run."""
    generated_artifacts = _build_generated_artifact_records(
        section_id=section_id,
        file_plan=file_plan,
        write_transparency=write_transparency,
    )
    return _with_artifact_metadata(
        "output_record",
        {
        "section_id": section_id,
        "run_mode": run_mode,
        "run_outcome_class": run_outcome_class,
        "file_plan": file_plan.model_dump(),
        "execution_outcome": execution_outcome,
        "advisory": advisory,
        "write_transparency": write_transparency,
        "generated_artifacts": generated_artifacts,
        "output_summary": _build_output_summary(
            section_id=section_id,
            run_outcome_class=run_outcome_class,
            execution_outcome=execution_outcome,
            generated_artifacts=generated_artifacts,
        ),
        },
    )


def _build_generated_artifact_records(
    *,
    section_id: str,
    file_plan: FilePlan,
    write_transparency: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build deterministic output-artifact records from the file plan plus finalized write decisions."""
    decisions_by_path: dict[str, dict[str, Any]] = {}
    for decision in write_transparency.get("write_decisions", []):
        if not isinstance(decision, dict):
            continue
        path = str(decision.get("path", "")).strip()
        if not path:
            continue
        decisions_by_path[path] = dict(decision)

    artifacts: list[dict[str, Any]] = []
    for file_entry in file_plan.files:
        normalized_path = Path(str(file_entry["path"])).as_posix()
        decision = decisions_by_path.get(
            normalized_path,
            {"decision": "skipped", "reason": "unplanned"},
        )
        artifacts.append(
            {
                "artifact_id": f"{section_id}:{normalized_path}",
                "artifact_kind": _output_artifact_kind(file_entry),
                "bytes_written": int(decision.get("bytes_written", 0)),
                "implementation_unit": _output_artifact_implementation_unit(file_entry),
                "path": normalized_path,
                "producer_ref": _output_artifact_producer_ref(file_entry),
                "purpose": str(file_entry.get("purpose", "")),
                "source": str(file_entry.get("source", "")),
                "write_decision": str(decision.get("decision", "skipped")),
                "write_reason": str(decision.get("reason", "unplanned")),
            }
        )

    return sorted(
        artifacts,
        key=lambda item: (
            str(item["path"]),
            str(item["source"]),
            str(item["implementation_unit"]),
        ),
    )


def _build_output_summary(
    *,
    section_id: str,
    run_outcome_class: str,
    execution_outcome: dict[str, Any],
    generated_artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the compact deterministic output summary for one DGCE run."""
    written_artifacts = [artifact for artifact in generated_artifacts if artifact["write_decision"] == "written"]
    return {
        "artifact_count": len(generated_artifacts),
        "execution_status": execution_outcome.get("status"),
        "execution_summary": execution_outcome.get("execution_summary", {}),
        "primary_artifact_path": (
            str(written_artifacts[0]["path"])
            if written_artifacts
            else str(generated_artifacts[0]["path"])
            if generated_artifacts
            else None
        ),
        "run_outcome_class": run_outcome_class,
        "section_id": section_id,
        "sources": sorted({str(artifact["source"]) for artifact in generated_artifacts}),
        "written_artifact_count": len(written_artifacts),
    }


def _output_artifact_kind(file_entry: dict[str, Any]) -> str:
    """Return the deterministic artifact kind for one generated file."""
    normalized_path = Path(str(file_entry.get("path", ""))).as_posix()
    if normalized_path.endswith("/service.py"):
        return "service"
    if normalized_path.endswith("/models.py"):
        return "models"
    if normalized_path.startswith("api/"):
        return "api"
    if normalized_path.startswith("models/"):
        return "data_model"
    return "file"


def _output_artifact_producer_ref(file_entry: dict[str, Any]) -> str:
    """Return the stable producer reference for one generated artifact record."""
    if isinstance(file_entry.get("module_contract"), dict):
        return str(file_entry["module_contract"].get("name", ""))
    if isinstance(file_entry.get("entity_schema"), dict):
        return str(file_entry["entity_schema"].get("name", ""))
    if isinstance(file_entry.get("interface_schema"), dict):
        return str(file_entry["interface_schema"].get("name", ""))
    path = Path(str(file_entry.get("path", "")))
    if str(file_entry.get("source", "")) == "system_breakdown" and path.parent.as_posix() not in {"", "."}:
        return path.parent.name
    return path.stem


def _output_artifact_implementation_unit(file_entry: dict[str, Any]) -> str:
    """Return the stable implementation-unit reference for one generated artifact record."""
    if isinstance(file_entry.get("file_group"), dict):
        group_name = str(file_entry["file_group"].get("name", "")).strip()
        if group_name:
            return f"implement_{group_name}"
    producer_ref = _output_artifact_token(_output_artifact_producer_ref(file_entry))
    source = str(file_entry.get("source", "")).strip()
    if source == "data_model":
        return f"generate_{producer_ref}_model"
    if source == "api_surface":
        return f"generate_{producer_ref}_api"
    if source == "expected_targets":
        return f"materialize_{producer_ref}"
    return f"implement_{producer_ref or 'artifact'}"


def _output_artifact_token(value: str) -> str:
    """Return the stable underscore-normalized token for output artifact metadata."""
    cleaned = "".join(char.lower() if char.isalnum() else "_" for char in value)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "artifact"


def _execution_permitted(approval_status: str, selected_mode: str) -> bool:
    """Return whether the current approval intent permits later execution."""
    return approval_status == "approved" and selected_mode in {"create_only", "safe_modify", "no_changes"}


def _build_approval_artifact(
    workspace_root: Path,
    section_id: str,
    approval_input: SectionApprovalInput,
) -> dict[str, Any]:
    """Build a deterministic approval artifact from existing preview/review linkage plus explicit inputs."""
    input_path = workspace_root / "input" / f"{section_id}.json"
    preview_path = workspace_root / "plans" / f"{section_id}.preview.json"
    review_path = workspace_root / "reviews" / f"{section_id}.review.md"
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8")) if preview_path.exists() else {}
    approval_status = str(approval_input.approval_status)
    selected_mode = str(approval_input.selected_mode)
    return {
        "section_id": section_id,
        "approval_status": approval_status,
        "selected_mode": selected_mode,
        "execution_permitted": _execution_permitted(approval_status, selected_mode),
        "input_path": input_path.relative_to(workspace_root.parent).as_posix(),
        "input_fingerprint": compute_json_file_fingerprint(input_path) if input_path.exists() else None,
        "preview_path": preview_path.relative_to(workspace_root.parent).as_posix() if preview_path.exists() else None,
        "review_path": review_path.relative_to(workspace_root.parent).as_posix() if review_path.exists() else None,
        "preview_fingerprint": preview_payload.get("artifact_fingerprint") if preview_path.exists() else None,
        "review_fingerprint": compute_review_artifact_fingerprint(review_path.read_text(encoding="utf-8")) if review_path.exists() else None,
        "preview_outcome_class": preview_payload.get("preview_outcome_class"),
        "recommended_mode": preview_payload.get("recommended_mode"),
        "approval_source": str(approval_input.approval_source),
        "approved_by": str(approval_input.approved_by),
        "approval_timestamp": str(approval_input.approval_timestamp),
        "notes": str(approval_input.notes),
    }


def _build_preflight_artifact(
    workspace_root: Path,
    section_id: str,
    preflight_input: SectionPreflightInput,
) -> dict[str, Any]:
    """Build a deterministic preflight artifact from approval plus current linkage state."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    preview_path = workspace_root / "plans" / f"{section_id}.preview.json"
    review_path = workspace_root / "reviews" / f"{section_id}.review.md"

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    approval_path_str = approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None
    preview_path_str = preview_path.relative_to(workspace_root.parent).as_posix() if preview_path.exists() else None
    review_path_str = review_path.relative_to(workspace_root.parent).as_posix() if review_path.exists() else None

    approval_status = approval_payload.get("approval_status")
    selected_mode = approval_payload.get("selected_mode")
    execution_permitted = approval_payload.get("execution_permitted")
    preview_outcome_class = approval_payload.get("preview_outcome_class")
    recommended_mode = approval_payload.get("recommended_mode")

    if not approval_path.exists():
        preflight_status = "preflight_missing_approval"
        execution_allowed = False
        preflight_reason = "missing_approval"
    elif not preview_path.exists():
        preflight_status = "preflight_missing_preview"
        execution_allowed = False
        preflight_reason = "missing_preview"
    elif not review_path.exists():
        preflight_status = "preflight_missing_review"
        execution_allowed = False
        preflight_reason = "missing_review"
    elif approval_status == "rejected":
        preflight_status = "preflight_rejected"
        execution_allowed = False
        preflight_reason = "approval_rejected"
    elif approval_status == "superseded":
        preflight_status = "preflight_superseded"
        execution_allowed = False
        preflight_reason = "approval_superseded"
    elif approval_payload.get("preview_path") != preview_path_str:
        preflight_status = "preflight_invalid_linkage"
        execution_allowed = False
        preflight_reason = "approval_preview_path_mismatch"
    elif approval_payload.get("review_path") != review_path_str:
        preflight_status = "preflight_invalid_linkage"
        execution_allowed = False
        preflight_reason = "approval_review_path_mismatch"
    elif approval_status != "approved" or execution_permitted is not True:
        preflight_status = "preflight_execution_not_permitted"
        execution_allowed = False
        preflight_reason = "approval_not_permitted"
    else:
        preflight_status = "preflight_pass"
        execution_allowed = True
        preflight_reason = "approved_and_linked"

    checked_artifacts = [
        {
            "artifact_role": "approval",
            "artifact_path": approval_path_str,
            "present": approval_path.exists(),
        },
        {
            "artifact_role": "preview",
            "artifact_path": preview_path_str,
            "present": preview_path.exists(),
        },
        {
            "artifact_role": "review",
            "artifact_path": review_path_str,
            "present": review_path.exists(),
        },
    ]
    checks = _build_preflight_checks(
        approval_exists=approval_path.exists(),
        preview_exists=preview_path.exists(),
        review_exists=review_path.exists(),
        approval_path=approval_path_str,
        preview_path=preview_path_str,
        review_path=review_path_str,
        approval_payload=approval_payload,
        approval_status=approval_status,
        execution_permitted=execution_permitted,
    )
    findings = _build_preflight_findings(section_id, checks)
    readiness_decision = "ready" if execution_allowed else "blocked"
    readiness_summary = _build_preflight_readiness_summary(
        checked_artifacts=checked_artifacts,
        checks=checks,
        findings=findings,
        readiness_decision=readiness_decision,
        readiness_reason=preflight_reason,
    )

    return {
        "section_id": section_id,
        "preflight_status": preflight_status,
        "readiness_decision": readiness_decision,
        "execution_allowed": execution_allowed,
        "approval_path": approval_path_str,
        "preview_path": preview_path_str,
        "review_path": review_path_str,
        "checked_artifacts": checked_artifacts,
        "checks": checks,
        "findings": findings,
        "readiness_summary": readiness_summary,
        "selected_mode": selected_mode,
        "approval_status": approval_status,
        "execution_permitted": execution_permitted,
        "preview_outcome_class": preview_outcome_class,
        "recommended_mode": recommended_mode,
        "preflight_reason": preflight_reason,
        "validation_timestamp": str(preflight_input.validation_timestamp),
    }


def _build_preflight_checks(
    *,
    approval_exists: bool,
    preview_exists: bool,
    review_exists: bool,
    approval_path: str | None,
    preview_path: str | None,
    review_path: str | None,
    approval_payload: dict[str, Any],
    approval_status: Any,
    execution_permitted: Any,
) -> list[dict[str, Any]]:
    approval_preview_matches = approval_exists and preview_exists and approval_payload.get("preview_path") == preview_path
    approval_review_matches = approval_exists and review_exists and approval_payload.get("review_path") == review_path
    approval_status_allows_execution = approval_status == "approved"
    execution_permission_granted = approval_status_allows_execution and execution_permitted is True
    return [
        _preflight_check_entry(
            check_id="approval_artifact_present",
            category="approval",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result="passed" if approval_exists else "failed",
            issue_code=None if approval_exists else "missing_approval",
            detail="approval artifact present" if approval_exists else "approval artifact missing",
        ),
        _preflight_check_entry(
            check_id="preview_artifact_present",
            category="preview",
            checked_artifact_role="preview",
            checked_artifact_path=preview_path,
            result="passed" if preview_exists else "failed",
            issue_code=None if preview_exists else "missing_preview",
            detail="preview artifact present" if preview_exists else "preview artifact missing",
        ),
        _preflight_check_entry(
            check_id="review_artifact_present",
            category="review",
            checked_artifact_role="review",
            checked_artifact_path=review_path,
            result="passed" if review_exists else "failed",
            issue_code=None if review_exists else "missing_review",
            detail="review artifact present" if review_exists else "review artifact missing",
        ),
        _preflight_check_entry(
            check_id="approval_status_allows_execution",
            category="approval",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result=(
                "not_evaluated"
                if not approval_exists
                else "passed"
                if approval_status_allows_execution
                else "failed"
            ),
            issue_code=(
                None
                if not approval_exists or approval_status_allows_execution
                else "approval_rejected"
                if approval_status == "rejected"
                else "approval_superseded"
                if approval_status == "superseded"
                else "approval_not_permitted"
            ),
            detail=(
                "approval unavailable for state validation"
                if not approval_exists
                else "approval status allows execution"
                if approval_status_allows_execution
                else f"approval status blocks execution: {approval_status}"
            ),
        ),
        _preflight_check_entry(
            check_id="approval_preview_linkage_valid",
            category="linkage",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result=(
                "not_evaluated"
                if not approval_exists or not preview_exists
                else "passed"
                if approval_preview_matches
                else "failed"
            ),
            issue_code=(
                None
                if not approval_exists or not preview_exists or approval_preview_matches
                else "approval_preview_path_mismatch"
            ),
            detail=(
                "approval or preview unavailable for linkage validation"
                if not approval_exists or not preview_exists
                else "approval preview linkage valid"
                if approval_preview_matches
                else "approval preview linkage mismatch"
            ),
        ),
        _preflight_check_entry(
            check_id="approval_review_linkage_valid",
            category="linkage",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result=(
                "not_evaluated"
                if not approval_exists or not review_exists
                else "passed"
                if approval_review_matches
                else "failed"
            ),
            issue_code=(
                None
                if not approval_exists or not review_exists or approval_review_matches
                else "approval_review_path_mismatch"
            ),
            detail=(
                "approval or review unavailable for linkage validation"
                if not approval_exists or not review_exists
                else "approval review linkage valid"
                if approval_review_matches
                else "approval review linkage mismatch"
            ),
        ),
        _preflight_check_entry(
            check_id="execution_permission_granted",
            category="execution_permission",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result=(
                "not_evaluated"
                if not approval_exists or not approval_status_allows_execution
                else "passed"
                if execution_permission_granted
                else "failed"
            ),
            issue_code=(
                None
                if not approval_exists or not approval_status_allows_execution or execution_permission_granted
                else "approval_not_permitted"
            ),
            detail=(
                "approval unavailable for execution permission validation"
                if not approval_exists
                else "execution permission blocked by approval status"
                if not approval_status_allows_execution
                else "execution permission granted"
                if execution_permission_granted
                else "execution permission denied"
            ),
        ),
    ]


def _preflight_check_entry(
    *,
    check_id: str,
    category: str,
    checked_artifact_role: str,
    checked_artifact_path: str | None,
    result: str,
    issue_code: str | None,
    detail: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "category": category,
        "checked_artifact_path": checked_artifact_path,
        "checked_artifact_role": checked_artifact_role,
        "detail": detail,
        "issue_code": issue_code,
        "result": result,
    }


def _build_preflight_findings(section_id: str, checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, check in enumerate(checks, start=1):
        if check["result"] != "failed":
            continue
        findings.append(
            {
                "finding_id": f"{index:02d}_{check['check_id']}",
                "category": str(check["category"]),
                "severity": _preflight_issue_severity(check.get("issue_code")),
                "checked_artifact_path": check["checked_artifact_path"],
                "checked_artifact_role": check["checked_artifact_role"],
                "issue_code": check["issue_code"],
                "message": check["detail"],
                "section_id": section_id,
            }
        )
    return findings


def _preflight_issue_severity(issue_code: Any) -> str:
    if issue_code in {"missing_approval", "missing_preview", "missing_review"}:
        return "critical"
    return "error"


def _build_preflight_readiness_summary(
    *,
    checked_artifacts: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    readiness_decision: str,
    readiness_reason: str,
) -> dict[str, Any]:
    passed_check_count = sum(1 for check in checks if check["result"] == "passed")
    failed_check_count = sum(1 for check in checks if check["result"] == "failed")
    not_evaluated_check_count = sum(1 for check in checks if check["result"] == "not_evaluated")
    return {
        "checked_artifact_count": len(checked_artifacts),
        "failed_check_count": failed_check_count,
        "blocking_finding_count": len(findings),
        "not_evaluated_check_count": not_evaluated_check_count,
        "passed_check_count": passed_check_count,
        "readiness_decision": readiness_decision,
        "readiness_reason": readiness_reason,
        "ready_for_gate": readiness_decision == "ready",
        "total_check_count": len(checks),
    }


def _build_stale_check_artifact(
    workspace_root: Path,
    section_id: str,
    stale_input: SectionStaleCheckInput,
) -> dict[str, Any]:
    """Build a deterministic stale-check artifact from approval linkage versus current preview/review paths."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    input_path = workspace_root / "input" / f"{section_id}.json"
    preview_path = workspace_root / "plans" / f"{section_id}.preview.json"
    review_path = workspace_root / "reviews" / f"{section_id}.review.md"

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    approval_path_str = approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None
    current_input_path = input_path.relative_to(workspace_root.parent).as_posix() if input_path.exists() else None
    current_preview_path = preview_path.relative_to(workspace_root.parent).as_posix() if preview_path.exists() else None
    current_review_path = review_path.relative_to(workspace_root.parent).as_posix() if review_path.exists() else None
    approval_input_path = approval_payload.get("input_path")
    approval_preview_path = approval_payload.get("preview_path")
    approval_review_path = approval_payload.get("review_path")
    approval_input_fingerprint = approval_payload.get("input_fingerprint")
    approval_preview_fingerprint = approval_payload.get("preview_fingerprint")
    approval_review_fingerprint = approval_payload.get("review_fingerprint")
    current_input_fingerprint = compute_json_file_fingerprint(input_path) if input_path.exists() else None
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8")) if preview_path.exists() else {}
    current_preview_fingerprint = preview_payload.get("artifact_fingerprint") if preview_path.exists() else None
    current_review_fingerprint = (
        compute_review_artifact_fingerprint(review_path.read_text(encoding="utf-8")) if review_path.exists() else None
    )

    if not approval_path.exists():
        stale_status = "stale_missing_approval"
        stale_detected = True
        stale_reason = "missing_approval"
    elif not preview_path.exists():
        stale_status = "stale_missing_preview"
        stale_detected = True
        stale_reason = "missing_preview"
    elif not review_path.exists():
        stale_status = "stale_missing_review"
        stale_detected = True
        stale_reason = "missing_review"
    elif not input_path.exists():
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "missing_input"
    elif approval_input_path != current_input_path:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_input_path_mismatch"
    elif approval_preview_path != current_preview_path:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_preview_path_mismatch"
    elif approval_review_path != current_review_path:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_review_path_mismatch"
    elif approval_preview_fingerprint is not None and approval_preview_fingerprint != current_preview_fingerprint:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_preview_fingerprint_mismatch"
    elif approval_review_fingerprint is not None and approval_review_fingerprint != current_review_fingerprint:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_review_fingerprint_mismatch"
    elif approval_input_fingerprint is not None and approval_input_fingerprint != current_input_fingerprint:
        stale_status = "stale_invalidated"
        stale_detected = True
        stale_reason = "approval_input_fingerprint_mismatch"
    else:
        stale_status = "stale_valid"
        stale_detected = False
        stale_reason = "approval_links_current"

    return {
        "section_id": section_id,
        "stale_status": stale_status,
        "stale_detected": stale_detected,
        "approval_path": approval_path_str,
        "approval_input_path": approval_input_path,
        "approval_preview_path": approval_preview_path,
        "approval_review_path": approval_review_path,
        "current_input_path": current_input_path,
        "current_preview_path": current_preview_path,
        "current_review_path": current_review_path,
        "approval_input_fingerprint": approval_input_fingerprint,
        "approval_preview_fingerprint": approval_preview_fingerprint,
        "approval_review_fingerprint": approval_review_fingerprint,
        "current_input_fingerprint": current_input_fingerprint,
        "current_preview_fingerprint": current_preview_fingerprint,
        "current_review_fingerprint": current_review_fingerprint,
        "stale_reason": stale_reason,
        "validation_timestamp": str(stale_input.validation_timestamp),
    }


def _build_execution_gate_artifact(
    workspace_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool,
    gate_input: SectionExecutionGateInput,
    preflight_payload: dict[str, Any] | None,
    stale_check_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build a deterministic execution-gate artifact from explicit preflight enforcement inputs."""
    preflight_path = workspace_root / "preflight" / f"{section_id}.preflight.json"
    stale_check_path = workspace_root / "preflight" / f"{section_id}.stale_check.json"
    preflight_path_str = preflight_path.relative_to(workspace_root.parent).as_posix() if preflight_path.exists() else None
    stale_check_path_str = stale_check_path.relative_to(workspace_root.parent).as_posix() if stale_check_path.exists() else None

    if not require_preflight_pass:
        gate_status = "gate_not_required"
        execution_attempted = False
        execution_blocked = False
        gate_reason = "preflight_not_required"
    elif stale_check_payload and stale_check_payload.get("stale_detected") is True:
        gate_status = "gate_blocked_stale"
        execution_attempted = True
        execution_blocked = True
        gate_reason = "stale_detected"
    elif preflight_payload is None:
        gate_status = "gate_blocked_missing_preflight"
        execution_attempted = True
        execution_blocked = True
        gate_reason = "missing_preflight"
    elif preflight_payload.get("preflight_status") != "preflight_pass":
        gate_status = "gate_blocked_preflight_failed"
        execution_attempted = True
        execution_blocked = True
        gate_reason = "preflight_failed"
    elif preflight_payload.get("execution_allowed") is not True:
        gate_status = "gate_blocked_execution_not_allowed"
        execution_attempted = True
        execution_blocked = True
        gate_reason = "execution_not_allowed"
    else:
        gate_status = "gate_pass"
        execution_attempted = True
        execution_blocked = False
        gate_reason = "preflight_passed"

    checked_artifacts = [
        {
            "artifact_role": "preflight",
            "artifact_path": preflight_path_str,
            "present": preflight_payload is not None,
        },
        {
            "artifact_role": "stale_check",
            "artifact_path": stale_check_path_str,
            "present": stale_check_payload is not None,
        },
    ]
    checks = _build_gate_checks(
        require_preflight_pass=require_preflight_pass,
        preflight_path=preflight_path_str,
        stale_check_path=stale_check_path_str,
        preflight_payload=preflight_payload,
        stale_check_payload=stale_check_payload,
    )
    reasons = _build_gate_reasons(section_id, checks)
    decision_summary = _build_gate_decision_summary(
        checked_artifacts=checked_artifacts,
        checks=checks,
        reasons=reasons,
        gate_status=gate_status,
        gate_reason=gate_reason,
        execution_blocked=execution_blocked,
    )

    return {
        "section_id": section_id,
        "require_preflight_pass": require_preflight_pass,
        "gate_status": gate_status,
        "execution_attempted": execution_attempted,
        "execution_blocked": execution_blocked,
        "checked_artifacts": checked_artifacts,
        "checks": checks,
        "reasons": reasons,
        "decision_summary": decision_summary,
        "preflight_path": preflight_path_str,
        "preflight_status": preflight_payload.get("preflight_status") if preflight_payload else None,
        "stale_check_path": stale_check_path_str,
        "stale_status": stale_check_payload.get("stale_status") if stale_check_payload else None,
        "stale_detected": stale_check_payload.get("stale_detected") if stale_check_payload else None,
        "execution_allowed": preflight_payload.get("execution_allowed") if preflight_payload else None,
        "selected_mode": preflight_payload.get("selected_mode") if preflight_payload else None,
        "gate_reason": gate_reason,
        "gate_timestamp": str(gate_input.gate_timestamp),
    }


def _build_gate_checks(
    *,
    require_preflight_pass: bool,
    preflight_path: str | None,
    stale_check_path: str | None,
    preflight_payload: dict[str, Any] | None,
    stale_check_payload: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    preflight_present = preflight_payload is not None
    stale_check_present = stale_check_payload is not None
    stale_detected = stale_check_payload.get("stale_detected") is True if stale_check_payload else False
    preflight_passed = preflight_payload.get("preflight_status") == "preflight_pass" if preflight_payload else False
    execution_allowed = preflight_payload.get("execution_allowed") is True if preflight_payload else False
    return [
        _gate_check_entry(
            check_id="preflight_required",
            category="policy",
            checked_artifact_role="gate",
            checked_artifact_path=None,
            result="passed" if require_preflight_pass else "not_required",
            issue_code=None if not require_preflight_pass else None,
            detail="preflight gate required" if require_preflight_pass else "preflight gate not required",
        ),
        _gate_check_entry(
            check_id="stale_check_clear",
            category="stale_check",
            checked_artifact_role="stale_check",
            checked_artifact_path=stale_check_path,
            result=(
                "not_required"
                if not require_preflight_pass
                else "passed"
                if stale_check_present and not stale_detected
                else "failed"
            ),
            issue_code=(
                None
                if not require_preflight_pass or (stale_check_present and not stale_detected)
                else "stale_detected"
            ),
            detail=(
                "stale check not required"
                if not require_preflight_pass
                else "stale check clear"
                if stale_check_present and not stale_detected
                else "stale check blocked execution"
            ),
        ),
        _gate_check_entry(
            check_id="preflight_artifact_present",
            category="preflight",
            checked_artifact_role="preflight",
            checked_artifact_path=preflight_path,
            result=(
                "not_required"
                if not require_preflight_pass
                else "passed"
                if preflight_present
                else "failed"
            ),
            issue_code=(
                None
                if not require_preflight_pass or preflight_present
                else "missing_preflight"
            ),
            detail=(
                "preflight artifact not required"
                if not require_preflight_pass
                else "preflight artifact present"
                if preflight_present
                else "preflight artifact missing"
            ),
        ),
        _gate_check_entry(
            check_id="preflight_status_passed",
            category="preflight",
            checked_artifact_role="preflight",
            checked_artifact_path=preflight_path,
            result=(
                "not_required"
                if not require_preflight_pass
                else "not_evaluated"
                if not preflight_present
                else "passed"
                if preflight_passed
                else "failed"
            ),
            issue_code=(
                None
                if not require_preflight_pass or not preflight_present or preflight_passed
                else "preflight_failed"
            ),
            detail=(
                "preflight status not required"
                if not require_preflight_pass
                else "preflight artifact unavailable for status validation"
                if not preflight_present
                else "preflight status passed"
                if preflight_passed
                else f"preflight status blocks execution: {preflight_payload.get('preflight_status')}"
            ),
        ),
        _gate_check_entry(
            check_id="execution_permission_confirmed",
            category="execution_permission",
            checked_artifact_role="preflight",
            checked_artifact_path=preflight_path,
            result=(
                "not_required"
                if not require_preflight_pass
                else "not_evaluated"
                if not preflight_present
                else "passed"
                if execution_allowed
                else "failed"
            ),
            issue_code=(
                None
                if not require_preflight_pass or not preflight_present or execution_allowed
                else "execution_not_allowed"
            ),
            detail=(
                "execution permission not required"
                if not require_preflight_pass
                else "preflight artifact unavailable for execution permission validation"
                if not preflight_present
                else "execution permission confirmed"
                if execution_allowed
                else "execution permission blocked"
            ),
        ),
    ]


def _gate_check_entry(
    *,
    check_id: str,
    category: str,
    checked_artifact_role: str,
    checked_artifact_path: str | None,
    result: str,
    issue_code: str | None,
    detail: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "check_id": check_id,
        "checked_artifact_path": checked_artifact_path,
        "checked_artifact_role": checked_artifact_role,
        "detail": detail,
        "issue_code": issue_code,
        "result": result,
    }


def _gate_reason_severity(issue_code: Any) -> str:
    if issue_code in {"stale_detected", "missing_preflight"}:
        return "critical"
    return "error"


def _build_gate_reasons(section_id: str, checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    for index, check in enumerate(checks, start=1):
        if check["result"] not in {"failed"}:
            continue
        reasons.append(
            {
                "category": str(check["category"]),
                "checked_artifact_path": check["checked_artifact_path"],
                "checked_artifact_role": check["checked_artifact_role"],
                "issue_code": check["issue_code"],
                "message": check["detail"],
                "reason_id": f"{index:02d}_{check['check_id']}",
                "section_id": section_id,
                "severity": _gate_reason_severity(check.get("issue_code")),
            }
        )
    return reasons


def _build_gate_decision_summary(
    *,
    checked_artifacts: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    reasons: list[dict[str, Any]],
    gate_status: str,
    gate_reason: str,
    execution_blocked: bool,
) -> dict[str, Any]:
    passed_check_count = sum(1 for check in checks if check["result"] == "passed")
    failed_check_count = sum(1 for check in checks if check["result"] == "failed")
    not_evaluated_check_count = sum(1 for check in checks if check["result"] == "not_evaluated")
    not_required_check_count = sum(1 for check in checks if check["result"] == "not_required")
    return {
        "allow_execution": execution_blocked is False,
        "blocking_reason_count": len(reasons),
        "checked_artifact_count": len(checked_artifacts),
        "failed_check_count": failed_check_count,
        "gate_reason": gate_reason,
        "gate_status": gate_status,
        "not_evaluated_check_count": not_evaluated_check_count,
        "not_required_check_count": not_required_check_count,
        "passed_check_count": passed_check_count,
        "total_check_count": len(checks),
    }


def _effective_execution_mode(write_transparency: dict[str, Any]) -> str:
    """Return the deterministic effective execution behavior for the current write plan."""
    write_summary = write_transparency.get("write_summary", {})
    if int(write_summary.get("modify_written_count", 0)) > 0:
        return "safe_modify"
    if int(write_summary.get("written_count", 0)) > 0:
        return "create_only"
    return "no_changes"


def _write_summary_counts(write_transparency: dict[str, Any]) -> tuple[int, int, int]:
    """Return deterministic written counters from actual write transparency."""
    write_summary = write_transparency.get("write_summary", {})
    written_file_count = int(write_summary.get("written_count", 0))
    modify_written_count = int(write_summary.get("modify_written_count", 0))
    created_written_count = max(0, written_file_count - modify_written_count)
    return written_file_count, modify_written_count, created_written_count


def _execution_status_from_stamp_inputs(
    *,
    require_preflight_pass: bool,
    execution_blocked: bool,
    written_file_count: int,
) -> str:
    """Return the deterministic high-level execution-stamp status."""
    if not require_preflight_pass:
        return "execution_not_governed"
    if execution_blocked:
        return "execution_blocked"
    if written_file_count == 0:
        return "execution_completed_no_changes"
    return "execution_completed"


def _build_execution_stamp_artifact(
    workspace_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool,
    execution_input: SectionExecutionStampInput,
    run_outcome_class: str | None,
    execution_blocked: bool,
    write_transparency: dict[str, Any],
    model_execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic execution-stamp artifact from current run facts and linked metadata."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    preflight_path = workspace_root / "preflight" / f"{section_id}.preflight.json"
    execution_gate_path = workspace_root / "preflight" / f"{section_id}.execution_gate.json"
    alignment_path = workspace_root / "preflight" / f"{section_id}.alignment.json"
    outputs_path = workspace_root / "outputs" / f"{section_id}.json"

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    preflight_payload = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
    gate_payload = json.loads(execution_gate_path.read_text(encoding="utf-8")) if execution_gate_path.exists() else {}
    alignment_payload = json.loads(alignment_path.read_text(encoding="utf-8")) if alignment_path.exists() else {}
    outputs_payload = json.loads(outputs_path.read_text(encoding="utf-8")) if outputs_path.exists() else {}

    effective_execution_mode = _effective_execution_mode(write_transparency)
    if execution_blocked:
        written_file_count, modify_written_count, created_written_count = 0, 0, 0
    else:
        written_file_count, modify_written_count, created_written_count = _write_summary_counts(write_transparency)
    execution_status = _execution_status_from_stamp_inputs(
        require_preflight_pass=require_preflight_pass,
        execution_blocked=execution_blocked,
        written_file_count=written_file_count,
    )
    governed_execution = require_preflight_pass and execution_gate_path.exists()
    approval_status_before = approval_payload.get("approval_status")
    approval_consumed = (
        governed_execution
        and execution_status in {"execution_completed", "execution_completed_no_changes"}
        and execution_blocked is False
    )
    approval_status_after = "superseded" if approval_consumed else approval_status_before
    selected_mode = (
        alignment_payload.get("selected_mode")
        or gate_payload.get("selected_mode")
        or preflight_payload.get("selected_mode")
        or approval_payload.get("selected_mode")
    )
    artifact_results = _build_execution_artifact_results(
        outputs_payload,
        write_transparency,
        execution_blocked=execution_blocked,
    )
    written_files = _build_execution_written_files(
        write_transparency,
        execution_blocked=execution_blocked,
    )
    unit_results = _build_execution_unit_results(artifact_results)
    executed_units = [unit["unit_id"] for unit in unit_results if unit["unit_status"] == "executed"]
    skipped_units = [unit["unit_id"] for unit in unit_results if unit["unit_status"] == "skipped"]
    failed_units = [unit["unit_id"] for unit in unit_results if unit["unit_status"] == "failed"]
    linked_artifacts = [
        {
            "artifact_role": "approval",
            "artifact_path": approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None,
            "present": approval_path.exists(),
        },
        {
            "artifact_role": "preflight",
            "artifact_path": preflight_path.relative_to(workspace_root.parent).as_posix() if preflight_path.exists() else None,
            "present": preflight_path.exists(),
        },
        {
            "artifact_role": "execution_gate",
            "artifact_path": execution_gate_path.relative_to(workspace_root.parent).as_posix() if execution_gate_path.exists() else None,
            "present": execution_gate_path.exists(),
        },
        {
            "artifact_role": "alignment",
            "artifact_path": alignment_path.relative_to(workspace_root.parent).as_posix() if alignment_path.exists() else None,
            "present": alignment_path.exists(),
        },
        {
            "artifact_role": "outputs",
            "artifact_path": outputs_path.relative_to(workspace_root.parent).as_posix() if outputs_path.exists() else None,
            "present": outputs_path.exists(),
        },
    ]
    execution_record_summary = _build_execution_record_summary(
        linked_artifacts=linked_artifacts,
        artifact_results=artifact_results,
        unit_results=unit_results,
        execution_status=execution_status,
        run_outcome_class=run_outcome_class,
        execution_blocked=execution_blocked,
    )

    payload = {
        "section_id": section_id,
        "execution_status": execution_status,
        "governed_execution": governed_execution,
        "require_preflight_pass": require_preflight_pass,
        "linked_artifacts": linked_artifacts,
        "artifact_results": artifact_results,
        "written_files": written_files,
        "unit_results": unit_results,
        "executed_units": executed_units,
        "skipped_units": skipped_units,
        "failed_units": failed_units,
        "execution_record_summary": execution_record_summary,
        "approval_path": approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None,
        "preflight_path": preflight_path.relative_to(workspace_root.parent).as_posix() if preflight_path.exists() else None,
        "execution_gate_path": execution_gate_path.relative_to(workspace_root.parent).as_posix() if execution_gate_path.exists() else None,
        "alignment_path": alignment_path.relative_to(workspace_root.parent).as_posix() if alignment_path.exists() else None,
        "outputs_path": outputs_path.relative_to(workspace_root.parent).as_posix() if outputs_path.exists() else None,
        "selected_mode": selected_mode,
        "effective_execution_mode": effective_execution_mode,
        "approval_status_before": approval_status_before,
        "approval_consumed": approval_consumed,
        "approval_status_after": approval_status_after,
        "execution_blocked": execution_blocked,
        "run_outcome_class": run_outcome_class,
        "written_file_count": written_file_count,
        "modify_written_count": modify_written_count,
        "created_written_count": created_written_count,
        "execution_timestamp": str(execution_input.execution_timestamp),
    }
    if model_execution is not None:
        payload["model_execution"] = model_execution
    return _with_artifact_metadata("execution_record", payload)


def _build_execution_artifact_results(
    outputs_payload: dict[str, Any],
    write_transparency: dict[str, Any],
    *,
    execution_blocked: bool,
) -> list[dict[str, Any]]:
    if execution_blocked:
        return []
    generated_artifacts = outputs_payload.get("generated_artifacts", [])
    if isinstance(generated_artifacts, list) and generated_artifacts:
        return [
            {
                "artifact_id": str(artifact.get("artifact_id", "")),
                "artifact_kind": str(artifact.get("artifact_kind", "")),
                "bytes_written": int(artifact.get("bytes_written", 0)),
                "implementation_unit": str(artifact.get("implementation_unit", "")),
                "path": str(artifact.get("path", "")),
                "producer_ref": str(artifact.get("producer_ref", "")),
                "result_status": "written" if str(artifact.get("write_decision", "")) == "written" else "skipped",
                "source": str(artifact.get("source", "")),
                "write_decision": str(artifact.get("write_decision", "")),
                "write_reason": str(artifact.get("write_reason", "")),
            }
            for artifact in generated_artifacts
        ]

    artifact_results: list[dict[str, Any]] = []
    for decision in write_transparency.get("write_decisions", []):
        if not isinstance(decision, dict):
            continue
        normalized_path = Path(str(decision.get("path", ""))).as_posix()
        if not normalized_path:
            continue
        artifact_results.append(
            {
                "artifact_id": normalized_path,
                "artifact_kind": "file",
                "bytes_written": int(decision.get("bytes_written", 0)),
                "implementation_unit": Path(normalized_path).stem or normalized_path,
                "path": normalized_path,
                "producer_ref": "",
                "result_status": "written" if str(decision.get("decision", "")) == "written" else "skipped",
                "source": "",
                "write_decision": str(decision.get("decision", "")),
                "write_reason": str(decision.get("reason", "")),
            }
        )
    return sorted(
        artifact_results,
        key=lambda item: (
            str(item["path"]),
            str(item["source"]),
            str(item["implementation_unit"]),
        ),
    )


def _build_execution_written_files(
    write_transparency: dict[str, Any],
    *,
    execution_blocked: bool,
) -> list[dict[str, Any]]:
    """Return the exact files written during execution from the executed write plan."""
    if execution_blocked:
        return []

    written_files: list[dict[str, Any]] = []
    for decision in write_transparency.get("write_decisions", []):
        if not isinstance(decision, dict):
            continue
        if str(decision.get("decision", "")) != "written":
            continue
        normalized_path = Path(str(decision.get("path", ""))).as_posix()
        if not normalized_path:
            continue
        written_files.append(
            {
                "path": normalized_path,
                "operation": "modify" if str(decision.get("reason", "")) == "modify" else "create",
                "bytes_written": int(decision.get("bytes_written", 0)),
            }
        )
    return written_files


def _build_execution_unit_results(artifact_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for artifact in artifact_results:
        unit_id = str(artifact.get("implementation_unit", "")).strip() or str(artifact.get("path", "")).strip()
        grouped.setdefault(unit_id, []).append(artifact)

    unit_results: list[dict[str, Any]] = []
    for unit_id in sorted(grouped):
        unit_artifacts = sorted(
            grouped[unit_id],
            key=lambda item: (
                str(item["path"]),
                str(item["source"]),
                str(item["artifact_kind"]),
            ),
        )
        written_count = sum(1 for artifact in unit_artifacts if artifact["result_status"] == "written")
        skipped_count = sum(1 for artifact in unit_artifacts if artifact["result_status"] == "skipped")
        unit_results.append(
            {
                "artifact_count": len(unit_artifacts),
                "paths": [str(artifact["path"]) for artifact in unit_artifacts],
                "skipped_artifact_count": skipped_count,
                "unit_id": unit_id,
                "unit_status": "executed" if written_count > 0 else "skipped",
                "written_artifact_count": written_count,
            }
        )
    return unit_results


def _build_execution_record_summary(
    *,
    linked_artifacts: list[dict[str, Any]],
    artifact_results: list[dict[str, Any]],
    unit_results: list[dict[str, Any]],
    execution_status: str,
    run_outcome_class: str | None,
    execution_blocked: bool,
) -> dict[str, Any]:
    written_artifacts = [artifact for artifact in artifact_results if artifact["result_status"] == "written"]
    skipped_artifacts = [artifact for artifact in artifact_results if artifact["result_status"] == "skipped"]
    executed_units = [unit for unit in unit_results if unit["unit_status"] == "executed"]
    skipped_units = [unit for unit in unit_results if unit["unit_status"] == "skipped"]
    return {
        "execution_blocked": execution_blocked,
        "execution_status": execution_status,
        "executed_unit_count": len(executed_units),
        "linked_artifact_count": len(linked_artifacts),
        "result_artifact_count": len(artifact_results),
        "run_outcome_class": run_outcome_class,
        "skipped_artifact_count": len(skipped_artifacts),
        "skipped_unit_count": len(skipped_units),
        "written_artifact_count": len(written_artifacts),
    }


def _supersede_approval_artifact(workspace_root: Path, section_id: str) -> None:
    """Deterministically update an existing approval artifact to superseded after governed consumption."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    if not approval_path.exists():
        return
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["approval_status"] = "superseded"
    approval_payload["execution_permitted"] = _execution_permitted(
        str(approval_payload.get("approval_status")),
        str(approval_payload.get("selected_mode")),
    )
    _write_json(approval_path, approval_payload)


def _build_alignment_artifact(
    workspace_root: Path,
    section_id: str,
    *,
    require_preflight_pass: bool,
    alignment_input: SectionAlignmentInput,
    write_transparency: dict[str, Any],
) -> dict[str, Any]:
    """Build a deterministic alignment artifact from approved mode plus effective write behavior."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    execution_gate_path = workspace_root / "preflight" / f"{section_id}.execution_gate.json"
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    gate_payload = json.loads(execution_gate_path.read_text(encoding="utf-8")) if execution_gate_path.exists() else {}
    selected_mode = approval_payload.get("selected_mode")
    effective_execution_mode = _effective_execution_mode(write_transparency)
    written_file_count, modify_written_count, created_written_count = _write_summary_counts(write_transparency)

    if selected_mode == "review_required":
        alignment_status = "alignment_blocked"
        alignment_blocked = True
        alignment_reason = "review_required_selected"
    elif selected_mode == "no_changes" and effective_execution_mode != "no_changes":
        alignment_status = "alignment_blocked"
        alignment_blocked = True
        alignment_reason = "writes_detected"
    elif selected_mode == "create_only" and effective_execution_mode == "safe_modify":
        alignment_status = "alignment_blocked"
        alignment_blocked = True
        alignment_reason = "modify_write_detected"
    elif selected_mode == "safe_modify":
        alignment_status = "alignment_pass"
        alignment_blocked = False
        alignment_reason = "safe_modify_permitted"
    elif selected_mode == "create_only":
        alignment_status = "alignment_pass"
        alignment_blocked = False
        alignment_reason = "create_only_aligned"
    elif selected_mode == "no_changes":
        alignment_status = "alignment_pass"
        alignment_blocked = False
        alignment_reason = "no_changes_aligned"
    else:
        alignment_status = "alignment_blocked"
        alignment_blocked = True
        alignment_reason = "unknown_selected_mode"

    approval_path_str = approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None
    execution_gate_path_str = execution_gate_path.relative_to(workspace_root.parent).as_posix() if execution_gate_path.exists() else None
    compared_artifacts = [
        {
            "artifact_role": "approval",
            "artifact_path": approval_path_str,
            "present": approval_path.exists(),
        },
        {
            "artifact_role": "execution_gate",
            "artifact_path": execution_gate_path_str,
            "present": execution_gate_path.exists(),
        },
    ]
    checks = _build_alignment_checks(
        selected_mode=selected_mode,
        effective_execution_mode=effective_execution_mode,
        approval_path=approval_path_str,
        execution_gate_path=execution_gate_path_str,
        gate_status=gate_payload.get("gate_status"),
    )
    mismatches = _build_alignment_mismatches(section_id, checks)
    remediation_summary = _build_alignment_remediation_summary(
        compared_artifacts=compared_artifacts,
        checks=checks,
        mismatches=mismatches,
        alignment_status=alignment_status,
        alignment_reason=alignment_reason,
        selected_mode=selected_mode,
        effective_execution_mode=effective_execution_mode,
    )

    return {
        "section_id": section_id,
        "alignment_status": alignment_status,
        "alignment_blocked": alignment_blocked,
        "compared_artifacts": compared_artifacts,
        "checks": checks,
        "mismatches": mismatches,
        "remediation_summary": remediation_summary,
        "selected_mode": selected_mode,
        "effective_execution_mode": effective_execution_mode,
        "written_file_count": written_file_count,
        "modify_written_count": modify_written_count,
        "created_written_count": created_written_count,
        "approval_path": approval_path_str,
        "execution_gate_path": execution_gate_path_str,
        "gate_status": gate_payload.get("gate_status"),
        "alignment_reason": alignment_reason,
        "require_preflight_pass": require_preflight_pass,
        "alignment_timestamp": str(alignment_input.alignment_timestamp),
    }


def _build_alignment_checks(
    *,
    selected_mode: Any,
    effective_execution_mode: str,
    approval_path: str | None,
    execution_gate_path: str | None,
    gate_status: Any,
) -> list[dict[str, Any]]:
    selected_mode_known = selected_mode in {"review_required", "no_changes", "create_only", "safe_modify"}
    return [
        _alignment_check_entry(
            check_id="selected_mode_known",
            category="approval_mode",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result="passed" if selected_mode_known else "failed",
            issue_code=None if selected_mode_known else "unknown_selected_mode",
            detail=(
                f"selected mode recognized: {selected_mode}"
                if selected_mode_known
                else f"selected mode is unknown: {selected_mode}"
            ),
        ),
        _alignment_check_entry(
            check_id="gate_context_available",
            category="gate_context",
            checked_artifact_role="execution_gate",
            checked_artifact_path=execution_gate_path,
            result="passed" if execution_gate_path else "not_evaluated",
            issue_code=None,
            detail=(
                f"gate context available: {gate_status}"
                if execution_gate_path
                else "gate context unavailable"
            ),
        ),
        _alignment_check_entry(
            check_id="selected_mode_matches_effective_execution",
            category="execution_mode",
            checked_artifact_role="approval",
            checked_artifact_path=approval_path,
            result=(
                "not_evaluated"
                if not selected_mode_known
                else "failed"
                if selected_mode == "review_required"
                else "passed"
                if selected_mode == "safe_modify"
                else "passed"
                if selected_mode == "create_only" and effective_execution_mode in {"create_only", "no_changes"}
                else "passed"
                if selected_mode == "no_changes" and effective_execution_mode == "no_changes"
                else "failed"
            ),
            issue_code=(
                None
                if not selected_mode_known
                else "review_required_selected"
                if selected_mode == "review_required"
                else None
                if selected_mode == "safe_modify"
                else None
                if selected_mode == "create_only" and effective_execution_mode in {"create_only", "no_changes"}
                else None
                if selected_mode == "no_changes" and effective_execution_mode == "no_changes"
                else "modify_write_detected"
                if selected_mode == "create_only" and effective_execution_mode == "safe_modify"
                else "writes_detected"
            ),
            detail=(
                "selected mode unavailable for execution-mode alignment validation"
                if not selected_mode_known
                else "review required blocks alignment"
                if selected_mode == "review_required"
                else f"effective execution mode aligned: {effective_execution_mode}"
                if selected_mode == "safe_modify"
                else f"effective execution mode aligned: {effective_execution_mode}"
                if selected_mode == "create_only" and effective_execution_mode in {"create_only", "no_changes"}
                else "no changes aligned"
                if selected_mode == "no_changes" and effective_execution_mode == "no_changes"
                else f"effective execution mode mismatched: {effective_execution_mode}"
            ),
        ),
    ]


def _alignment_check_entry(
    *,
    check_id: str,
    category: str,
    checked_artifact_role: str,
    checked_artifact_path: str | None,
    result: str,
    issue_code: str | None,
    detail: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "check_id": check_id,
        "checked_artifact_path": checked_artifact_path,
        "checked_artifact_role": checked_artifact_role,
        "detail": detail,
        "issue_code": issue_code,
        "result": result,
    }


def _alignment_mismatch_severity(issue_code: Any) -> str:
    if issue_code in {"review_required_selected", "unknown_selected_mode"}:
        return "critical"
    return "error"


def _build_alignment_mismatches(section_id: str, checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for index, check in enumerate(checks, start=1):
        if check["result"] != "failed":
            continue
        mismatches.append(
            {
                "category": str(check["category"]),
                "checked_artifact_path": check["checked_artifact_path"],
                "checked_artifact_role": check["checked_artifact_role"],
                "issue_code": check["issue_code"],
                "message": check["detail"],
                "mismatch_id": f"{index:02d}_{check['check_id']}",
                "section_id": section_id,
                "severity": _alignment_mismatch_severity(check.get("issue_code")),
            }
        )
    return mismatches


def _build_alignment_remediation_summary(
    *,
    compared_artifacts: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    mismatches: list[dict[str, Any]],
    alignment_status: str,
    alignment_reason: str,
    selected_mode: Any,
    effective_execution_mode: str,
) -> dict[str, Any]:
    passed_check_count = sum(1 for check in checks if check["result"] == "passed")
    failed_check_count = sum(1 for check in checks if check["result"] == "failed")
    not_evaluated_check_count = sum(1 for check in checks if check["result"] == "not_evaluated")
    return {
        "alignment_reason": alignment_reason,
        "alignment_status": alignment_status,
        "compared_artifact_count": len(compared_artifacts),
        "effective_execution_mode": effective_execution_mode,
        "failed_check_count": failed_check_count,
        "mismatch_count": len(mismatches),
        "next_action": "proceed_to_execution" if alignment_status == "alignment_pass" else "review_alignment_mismatch",
        "not_evaluated_check_count": not_evaluated_check_count,
        "passed_check_count": passed_check_count,
        "selected_mode": selected_mode,
        "total_check_count": len(checks),
    }


def _build_review_index(workspace_root: Path, section_ids: List[str]) -> dict:
    """Build a deterministic review index from known section preview/review artifacts."""
    sections: list[dict[str, Any]] = []
    for section_id in sorted(section_ids):
        section_artifacts = _load_section_artifacts(workspace_root, section_id)
        artifact_paths = section_artifacts["artifact_paths"]
        if not any(
            artifact_paths.get(path_key) is not None
            for path_key in (
                "preview_path",
                "review_path",
                "approval_path",
                "preflight_path",
                "stale_check_path",
                "execution_gate_path",
                "alignment_path",
                "execution_path",
            )
        ):
            continue
        payloads = section_artifacts["payloads"]
        section_summary = _build_section_convergence_summary(
            section_artifacts,
            _build_section_lifecycle_trace_entries_from_artifacts(section_artifacts),
        )
        sections.append(
            {
                "entry_order": len(sections) + 1,
                "section_id": section_id,
                "preview_path": artifact_paths.get("preview_path"),
                "review_path": artifact_paths.get("review_path"),
                "preview_outcome_class": payloads["preview_path"].get("preview_outcome_class"),
                "recommended_mode": payloads["preview_path"].get("recommended_mode"),
                "approval_path": artifact_paths.get("approval_path"),
                "approval_status": section_summary["approval_status"],
                "selected_mode": payloads["approval_path"].get("selected_mode"),
                "execution_permitted": payloads["approval_path"].get("execution_permitted"),
                "preflight_path": artifact_paths.get("preflight_path"),
                "preflight_status": payloads["preflight_path"].get("preflight_status"),
                "stale_check_path": artifact_paths.get("stale_check_path"),
                "stale_status": payloads["stale_check_path"].get("stale_status"),
                "stale_detected": payloads["stale_check_path"].get("stale_detected"),
                "execution_allowed": payloads["preflight_path"].get("execution_allowed"),
                "execution_gate_path": artifact_paths.get("execution_gate_path"),
                "gate_status": payloads["execution_gate_path"].get("gate_status"),
                "execution_blocked": payloads["execution_gate_path"].get("execution_blocked"),
                "alignment_path": artifact_paths.get("alignment_path"),
                "alignment_status": payloads["alignment_path"].get("alignment_status"),
                "alignment_blocked": payloads["alignment_path"].get("alignment_blocked"),
                "execution_path": artifact_paths.get("execution_path"),
                "execution_status": payloads["execution_path"].get("execution_status"),
                "approval_consumed": payloads["execution_path"].get("approval_consumed"),
                "approval_status_after": payloads["execution_path"].get("approval_status_after"),
                "approval_timestamp": payloads["approval_path"].get("approval_timestamp"),
                "decision_source": section_summary["decision_source"],
                "review_status": section_summary["review_status"],
                "latest_decision": section_summary["latest_decision"],
                "latest_decision_source": section_summary["latest_decision_source"],
                "lifecycle_trace_path": _normalized_workspace_artifact_path("lifecycle_trace.json"),
                "output_path": _normalize_artifact_path(artifact_paths.get("output_path")),
                "review_approval_summary": {
                    "approval_status": section_summary["approval_status"],
                    "decision_source": section_summary["decision_source"],
                    "latest_decision": section_summary["latest_decision"],
                    "latest_decision_source": section_summary["latest_decision_source"],
                    "review_status": section_summary["review_status"],
                },
                "section_summary": section_summary,
                "navigation_links": _build_section_navigation_links(artifact_paths),
            }
        )

    sections = sorted(sections, key=lambda entry: str(entry["section_id"]))
    for entry_order, entry in enumerate(sections, start=1):
        entry["entry_order"] = entry_order

    return _with_artifact_metadata(
        "review_index",
        {
        "section_order": [entry["section_id"] for entry in sections],
        "sections": sections,
        "summary": {
            "sections_with_approval": sum(1 for entry in sections if entry["approval_path"] is not None),
            "sections_with_execution": sum(1 for entry in sections if entry["execution_path"] is not None),
            "sections_with_outputs": sum(1 for entry in sections if entry["output_path"] is not None),
            "sections_with_review": sum(1 for entry in sections if entry["review_path"] is not None),
            "total_sections_seen": len(sections),
        },
        },
    )


def _build_workspace_summary(
    workspace_root: Path,
    section_ids: List[str],
    review_index: dict[str, Any] | None = None,
) -> dict:
    """Build a deterministic workspace summary from persisted .dce artifacts."""
    if review_index is None:
        review_index = _build_review_index(workspace_root, section_ids)
    review_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in review_index.get("sections", [])
        if entry.get("section_id")
    }
    known_section_ids = set(section_ids)
    for output_path in (workspace_root / "outputs").glob("*.json"):
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        derived_section_id = payload.get("section_id") or output_path.stem
        if derived_section_id:
            known_section_ids.add(str(derived_section_id))

    sections: list[dict] = []
    for section_id in sorted(known_section_ids):
        section_artifacts = _load_section_artifacts(workspace_root, section_id)
        artifact_paths = section_artifacts["artifact_paths"]
        payloads = section_artifacts["payloads"]
        payload = payloads["output_path"]
        execution_outcome = payload.get("execution_outcome", {})
        advisory = payload.get("advisory")
        validation = execution_outcome.get("validation_summary", {})
        execution = execution_outcome.get("execution_summary", {})
        review_entry = review_sections.get(section_id, {})
        section_summary = _build_section_convergence_summary(
            section_artifacts,
            _build_section_lifecycle_trace_entries_from_artifacts(section_artifacts),
        )
        sections.append(
            {
                "section_id": payload.get("section_id") or section_id,
                "latest_run_mode": payload.get("run_mode"),
                "latest_run_outcome_class": payload.get("run_outcome_class"),
                "latest_status": execution_outcome.get("status"),
                "latest_validation_ok": validation.get("ok"),
                "latest_advisory_type": advisory.get("type") if isinstance(advisory, dict) else None,
                "latest_advisory_explanation": advisory.get("explanation") if isinstance(advisory, dict) else None,
                "latest_written_files_count": execution.get("written_files_count", 0),
                "latest_skipped_modify_count": execution.get("skipped_modify_count", 0),
                "latest_skipped_ignore_count": execution.get("skipped_ignore_count", 0),
                "preview_path": review_entry.get("preview_path"),
                "review_path": review_entry.get("review_path"),
                "preview_outcome_class": review_entry.get("preview_outcome_class"),
                "recommended_mode": review_entry.get("recommended_mode"),
                "approval_path": review_entry.get("approval_path"),
                "approval_status": review_entry.get("approval_status"),
                "selected_mode": review_entry.get("selected_mode"),
                "execution_permitted": review_entry.get("execution_permitted"),
                "preflight_path": review_entry.get("preflight_path"),
                "preflight_status": review_entry.get("preflight_status"),
                "stale_check_path": review_entry.get("stale_check_path"),
                "stale_status": review_entry.get("stale_status"),
                "stale_detected": review_entry.get("stale_detected"),
                "execution_allowed": review_entry.get("execution_allowed"),
                "execution_gate_path": review_entry.get("execution_gate_path"),
                "gate_status": review_entry.get("gate_status"),
                "execution_blocked": review_entry.get("execution_blocked"),
                "alignment_path": review_entry.get("alignment_path"),
                "alignment_status": review_entry.get("alignment_status"),
                "alignment_blocked": review_entry.get("alignment_blocked"),
                "execution_path": review_entry.get("execution_path"),
                "execution_status": review_entry.get("execution_status"),
                "approval_consumed": review_entry.get("approval_consumed"),
                "approval_status_after": review_entry.get("approval_status_after"),
                "decision_source": section_summary["decision_source"],
                "review_status": section_summary["review_status"],
                "latest_decision": section_summary["latest_decision"],
                "latest_decision_source": section_summary["latest_decision_source"],
                "latest_stage": section_summary["latest_stage"],
                "latest_stage_status": section_summary["latest_stage_status"],
                "section_summary": section_summary,
            }
        )

    sections = sorted(sections, key=lambda entry: str(entry["section_id"]))
    return _with_artifact_metadata(
        "workspace_summary",
        {
        "total_sections_seen": len(sections),
        "sections": sections,
        },
    )


def _build_lifecycle_trace(workspace_root: Path, section_ids: List[str]) -> dict[str, Any]:
    known_section_ids = set(section_ids)
    for output_path in (workspace_root / "outputs").glob("*.json"):
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        derived_section_id = payload.get("section_id") or output_path.stem
        if derived_section_id:
            known_section_ids.add(str(derived_section_id))

    sections: list[dict[str, Any]] = []
    for section_id in sorted(known_section_ids):
        section_artifacts = _load_section_artifacts(workspace_root, section_id)
        trace_entries = _build_section_lifecycle_trace_entries_from_artifacts(section_artifacts)
        if not any(entry["artifact_present"] for entry in trace_entries):
            continue
        section_summary = _build_section_convergence_summary(section_artifacts, trace_entries)
        sections.append(
            {
                "section_id": section_id,
                "approval_status": section_summary["approval_status"],
                "decision_source": section_summary["decision_source"],
                "review_status": section_summary["review_status"],
                "latest_decision": section_summary["latest_decision"],
                "latest_decision_source": section_summary["latest_decision_source"],
                "latest_stage": section_summary["latest_stage"],
                "latest_stage_status": section_summary["latest_stage_status"],
                "section_summary": section_summary,
                "trace_entries": trace_entries,
                "trace_summary": _build_section_lifecycle_trace_summary(section_summary, trace_entries),
            }
        )

    return _with_artifact_metadata(
        "lifecycle_trace",
        {
        "lifecycle_order": DGCE_LIFECYCLE_ORDER,
        "total_sections_seen": len(sections),
        "sections": sections,
        },
    )


def _build_section_lifecycle_trace_entries(workspace_root: Path, section_id: str) -> list[dict[str, Any]]:
    return _build_section_lifecycle_trace_entries_from_artifacts(_load_section_artifacts(workspace_root, section_id))


def _build_section_lifecycle_trace_summary(section_summary: dict[str, Any], trace_entries: list[dict[str, Any]]) -> dict[str, Any]:
    present_entries = [entry for entry in trace_entries if entry["artifact_present"]]
    return {
        "available_artifact_count": len(present_entries),
        "approval_status": section_summary["approval_status"],
        "completed_stage_count": len(present_entries),
        "decision_source": section_summary["decision_source"],
        "latest_decision": section_summary["latest_decision"],
        "latest_decision_source": section_summary["latest_decision_source"],
        "latest_stage": section_summary["latest_stage"],
        "latest_stage_status": section_summary["latest_stage_status"],
        "review_status": section_summary["review_status"],
        "section_id": section_summary["section_id"],
        "trace_entry_count": len(trace_entries),
    }


def _build_workspace_index(
    workspace_root: Path,
    section_ids: List[str],
    workspace_summary: dict[str, Any] | None = None,
    lifecycle_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if workspace_summary is None:
        workspace_summary = _build_workspace_summary(workspace_root, section_ids)
    if lifecycle_trace is None:
        lifecycle_trace = _build_lifecycle_trace(workspace_root, section_ids)
    trace_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in lifecycle_trace.get("sections", [])
        if entry.get("section_id")
    }
    summary_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in workspace_summary.get("sections", [])
        if entry.get("section_id")
    }
    known_section_ids = sorted(set(summary_sections) | set(trace_sections))
    latest_stage_counts = {stage: 0 for stage in DGCE_LIFECYCLE_ORDER}
    section_entries: list[dict[str, Any]] = []
    for entry_order, section_id in enumerate(known_section_ids, start=1):
        summary_entry = summary_sections.get(section_id, {})
        trace_entry = trace_sections.get(section_id, {})
        trace_summary = dict(trace_entry.get("trace_summary", {}))
        section_summary = dict(summary_entry.get("section_summary", trace_entry.get("section_summary", {})))
        latest_stage = trace_summary.get("latest_stage")
        if latest_stage in latest_stage_counts:
            latest_stage_counts[str(latest_stage)] += 1
        artifact_paths = _collect_orchestrator_artifact_paths(workspace_root.parent, section_id)
        artifact_links = _build_section_artifact_links(artifact_paths)
        section_entries.append(
            {
                "artifact_links": artifact_links,
                "entry_order": entry_order,
                "execution_path": _normalize_artifact_path(artifact_paths.get("execution_path")),
                "execution_status": summary_entry.get("execution_status"),
                "approval_status": section_summary.get("approval_status"),
                "decision_source": section_summary.get("decision_source"),
                "review_status": section_summary.get("review_status"),
                "latest_decision": section_summary.get("latest_decision"),
                "latest_decision_source": section_summary.get("latest_decision_source"),
                "latest_run_outcome_class": summary_entry.get("latest_run_outcome_class"),
                "latest_stage": latest_stage,
                "latest_stage_status": trace_summary.get("latest_stage_status"),
                "lifecycle_trace_path": _normalized_workspace_artifact_path("lifecycle_trace.json"),
                "output_path": _normalize_artifact_path(artifact_paths.get("output_path")),
                "section_id": section_id,
                "section_summary": section_summary,
                "trace_entry_count": trace_summary.get("trace_entry_count"),
                "trace_summary": trace_summary,
            }
        )

    return _with_artifact_metadata(
        "workspace_index",
        {
        "artifact_paths": {
            "lifecycle_trace_path": _normalized_workspace_artifact_path("lifecycle_trace.json"),
            "review_index_path": _normalized_workspace_artifact_path("reviews/index.json"),
            "workspace_summary_path": _normalized_workspace_artifact_path("workspace_summary.json"),
        },
        "section_order": [entry["section_id"] for entry in section_entries],
        "sections": section_entries,
        "summary": {
            "latest_stage_counts": [
                {
                    "section_count": latest_stage_counts[stage],
                    "stage": stage,
                }
                for stage in DGCE_LIFECYCLE_ORDER
            ],
            "sections_with_execution": sum(1 for entry in section_entries if entry["execution_path"] is not None),
            "sections_with_lifecycle_trace": len(section_entries),
            "sections_with_outputs": sum(1 for entry in section_entries if entry["output_path"] is not None),
            "total_sections_seen": len(section_entries),
        },
        },
    )


def _count_summary_values(
    sections: list[dict[str, Any]],
    field_name: str,
    *,
    allowed_values: list[str] | None = None,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    if allowed_values is not None:
        counts = {value: 0 for value in allowed_values}
    for section in sections:
        raw_value = section.get(field_name)
        normalized_value = "none" if raw_value is None else str(raw_value)
        if normalized_value not in counts:
            counts[normalized_value] = 0
        counts[normalized_value] += 1
    ordered_values = list(allowed_values) if allowed_values is not None else sorted(counts)
    for value in sorted(counts):
        if value not in ordered_values:
            ordered_values.append(value)
    return [{"section_count": counts[value], "value": value} for value in ordered_values]


def _build_dashboard_view(
    workspace_root: Path,
    section_ids: List[str],
    review_index: dict[str, Any] | None = None,
    lifecycle_trace: dict[str, Any] | None = None,
    workspace_index: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if review_index is None:
        review_index = _build_review_index(workspace_root, section_ids)
    if lifecycle_trace is None:
        lifecycle_trace = _build_lifecycle_trace(workspace_root, section_ids)
    if workspace_index is None:
        workspace_index = _build_workspace_index(workspace_root, section_ids)
    review_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in review_index.get("sections", [])
        if entry.get("section_id")
    }
    trace_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in lifecycle_trace.get("sections", [])
        if entry.get("section_id")
    }
    index_sections = {
        str(entry.get("section_id")): dict(entry)
        for entry in workspace_index.get("sections", [])
        if entry.get("section_id")
    }
    known_section_ids = sorted(set(review_sections) | set(trace_sections) | set(index_sections))
    section_cards: list[dict[str, Any]] = []
    for entry_order, section_id in enumerate(known_section_ids, start=1):
        review_entry = review_sections.get(section_id, {})
        trace_entry = trace_sections.get(section_id, {})
        index_entry = index_sections.get(section_id, {})
        section_summary = dict(
            index_entry.get("section_summary", trace_entry.get("section_summary", review_entry.get("section_summary", {})))
        )
        trace_summary = dict(trace_entry.get("trace_summary", {}))
        artifact_links = {
            str(link.get("artifact_role")): link.get("path")
            for link in index_entry.get("artifact_links", [])
            if isinstance(link, dict) and link.get("artifact_role")
        }
        section_cards.append(
            {
                "approval_status": section_summary.get("approval_status"),
                "current_stage": section_summary.get("latest_stage"),
                "decision_source": section_summary.get("decision_source"),
                "entry_order": entry_order,
                "latest_decision": section_summary.get("latest_decision"),
                "progress": {
                    "available_artifact_count": trace_summary.get("available_artifact_count", 0),
                    "completed_stage_count": trace_summary.get("completed_stage_count", 0),
                    "lifecycle_stage_count": len(DGCE_LIFECYCLE_ORDER),
                    "trace_entry_count": trace_summary.get("trace_entry_count", 0),
                },
                "navigation_links": {
                    "approval": artifact_links.get("approval"),
                    "execution": artifact_links.get("execution"),
                    "lifecycle_trace": _normalize_artifact_path(
                        index_entry.get("lifecycle_trace_path", _normalized_workspace_artifact_path("lifecycle_trace.json"))
                    ),
                    "outputs": artifact_links.get("outputs"),
                    "review": artifact_links.get("review"),
                },
                "review_status": section_summary.get("review_status"),
                "section_id": section_id,
                "section_summary": section_summary,
                "stage_status": section_summary.get("latest_stage_status"),
            }
        )

    return _with_artifact_metadata(
        "dashboard",
        {
        "artifact_paths": {
            "lifecycle_trace_path": _normalized_workspace_artifact_path("lifecycle_trace.json"),
            "review_index_path": _normalized_workspace_artifact_path("reviews/index.json"),
            "workspace_index_path": _normalized_workspace_artifact_path("workspace_index.json"),
        },
        "section_order": [entry["section_id"] for entry in section_cards],
        "sections": section_cards,
        "summary": {
            "approval_status_counts": _count_summary_values(section_cards, "approval_status"),
            "current_stage_counts": _count_summary_values(
                section_cards,
                "current_stage",
                allowed_values=DGCE_LIFECYCLE_ORDER,
            ),
            "review_status_counts": _count_summary_values(section_cards, "review_status"),
            "stage_status_counts": _count_summary_values(section_cards, "stage_status"),
            "total_sections": len(section_cards),
        },
        },
    )


def _build_artifact_manifest(
    review_index: dict[str, Any],
    workspace_summary: dict[str, Any],
    lifecycle_trace: dict[str, Any],
    workspace_index: dict[str, Any],
    dashboard: dict[str, Any],
) -> dict[str, Any]:
    workspace_artifacts = [
        (_normalized_workspace_artifact_path("reviews/index.json"), review_index),
        (_normalized_workspace_artifact_path("workspace_summary.json"), workspace_summary),
        (_normalized_workspace_artifact_path("lifecycle_trace.json"), lifecycle_trace),
        (_normalized_workspace_artifact_path("workspace_index.json"), workspace_index),
        (_normalized_workspace_artifact_path("dashboard.json"), dashboard),
        (_normalized_workspace_artifact_path("artifact_manifest.json"), _with_artifact_metadata("artifact_manifest", {})),
        (_normalized_workspace_artifact_path("consumer_contract.json"), _with_artifact_metadata("consumer_contract", {})),
        (_normalized_workspace_artifact_path("export_contract.json"), _with_artifact_metadata("export_contract", {})),
    ]
    artifacts = [
        {
            "artifact_path": _normalize_artifact_path(artifact_path),
            "artifact_type": str(artifact_payload.get("artifact_type")),
            "schema_version": str(artifact_payload.get("schema_version")),
            "scope": "workspace",
            "section_id": None,
        }
        for artifact_path, artifact_payload in workspace_artifacts
    ]

    section_artifacts: list[dict[str, Any]] = []
    for section_entry in workspace_index.get("sections", []):
        if not isinstance(section_entry, dict):
            continue
        section_id = section_entry.get("section_id")
        if not isinstance(section_id, str):
            continue
        section_link_map = {
            str(link.get("artifact_role")): _normalize_artifact_path(link.get("path"))
            for link in section_entry.get("artifact_links", [])
            if isinstance(link, dict) and link.get("artifact_role")
        }
        for artifact_role, _artifact_key, artifact_type in _section_artifact_link_specs():
            artifact_path = section_link_map.get(artifact_role)
            if artifact_path is None:
                continue
            section_artifacts.append(
                {
                    "artifact_path": artifact_path,
                    "artifact_type": artifact_type,
                    "schema_version": DGCE_ARTIFACT_SCHEMA_VERSION,
                    "scope": "section",
                    "section_id": section_id,
                }
            )

    artifacts.extend(
        sorted(
            section_artifacts,
            key=lambda entry: (
                str(entry["section_id"]),
                next(
                    index
                    for index, (_artifact_role, _artifact_key, artifact_type) in enumerate(_section_artifact_link_specs())
                    if artifact_type == entry["artifact_type"]
                ),
                str(entry["artifact_path"]),
            ),
        )
    )
    return _with_artifact_metadata("artifact_manifest", {"artifacts": artifacts})


def _build_consumer_contract(
    review_index: dict[str, Any],
    workspace_summary: dict[str, Any],
    lifecycle_trace: dict[str, Any],
    workspace_index: dict[str, Any],
    dashboard: dict[str, Any],
    artifact_manifest: dict[str, Any],
) -> dict[str, Any]:
    artifacts_by_path = {
        _normalized_workspace_artifact_path("reviews/index.json"): review_index,
        _normalized_workspace_artifact_path("workspace_summary.json"): workspace_summary,
        _normalized_workspace_artifact_path("lifecycle_trace.json"): lifecycle_trace,
        _normalized_workspace_artifact_path("workspace_index.json"): workspace_index,
        _normalized_workspace_artifact_path("dashboard.json"): dashboard,
        _normalized_workspace_artifact_path("artifact_manifest.json"): artifact_manifest,
    }
    supported_artifacts = []
    for spec in _supported_consumer_artifact_specs():
        artifact_path = str(spec["artifact_path"])
        artifact_payload = artifacts_by_path[artifact_path]
        supported_artifacts.append(
            {
                "artifact_type": str(artifact_payload.get("artifact_type")),
                "schema_version": str(artifact_payload.get("schema_version")),
                "artifact_path": artifact_path,
                "supported_fields": list(spec["supported_fields"]),
                "contract_stability": "supported",
                "consumer_scopes": list(spec["consumer_scopes"]),
                "export_scope": str(spec["export_scope"]),
            }
        )
    return _with_artifact_metadata("consumer_contract", {"supported_artifacts": supported_artifacts})


def _artifact_manifest_entries_by_path(artifact_manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(_normalize_artifact_path(entry.get("artifact_path"))): dict(entry)
        for entry in artifact_manifest.get("artifacts", [])
        if isinstance(entry, dict) and _normalize_artifact_path(entry.get("artifact_path")) is not None
    }


def _get_exportable_contract(consumer_contract: dict[str, Any]) -> dict[str, Any]:
    exportable_artifacts: list[dict[str, Any]] = []
    for entry in consumer_contract.get("supported_artifacts", []):
        if not isinstance(entry, dict) or entry.get("export_scope") != "external":
            continue
        export_fields = entry.get("export_fields")
        exportable_entry = dict(entry)
        exportable_entry["export_fields"] = list(export_fields) if export_fields is not None else list(entry.get("supported_fields", []))
        exportable_artifacts.append(exportable_entry)
    return {
        "artifact_type": str(consumer_contract.get("artifact_type")),
        "generated_by": str(consumer_contract.get("generated_by")),
        "schema_version": str(consumer_contract.get("schema_version")),
        "supported_artifacts": exportable_artifacts,
    }


def _build_export_contract(consumer_contract: dict[str, Any]) -> dict[str, Any]:
    exportable_contract = _get_exportable_contract(consumer_contract)
    return _with_artifact_metadata("export_contract", {"supported_artifacts": list(exportable_contract["supported_artifacts"])})


def _assert_contract_aligns_with_manifest(artifact_manifest: dict[str, Any], consumer_contract: dict[str, Any]) -> None:
    manifest_entries = _artifact_manifest_entries_by_path(artifact_manifest)
    for index, contract_entry in enumerate(consumer_contract.get("supported_artifacts", [])):
        if not isinstance(contract_entry, dict):
            raise ValueError(f"consumer_contract supported_artifacts[{index}] must be a dict")
        artifact_path = _normalize_artifact_path(contract_entry.get("artifact_path"))
        if artifact_path is None or artifact_path not in manifest_entries:
            raise ValueError(f"consumer_contract supported_artifacts[{index}] must resolve to artifact_manifest")
        manifest_entry = manifest_entries[artifact_path]
        for key in ("artifact_type", "schema_version"):
            if str(contract_entry.get(key)) != str(manifest_entry.get(key)):
                raise ValueError(
                    f"consumer_contract supported_artifacts[{index}].{key} must match artifact_manifest for {artifact_path}"
                )
        export_scope = str(contract_entry.get("export_scope"))
        if export_scope not in {"external", "internal"}:
            raise ValueError(f"consumer_contract supported_artifacts[{index}].export_scope must be external or internal")
        supported_fields = [str(field_name) for field_name in contract_entry.get("supported_fields", [])]
        export_fields = contract_entry.get("export_fields")
        if export_fields is not None:
            for export_field_index, export_field in enumerate(export_fields):
                if str(export_field) not in supported_fields:
                    raise ValueError(
                        f"consumer_contract supported_artifacts[{index}].export_fields[{export_field_index}] must be present in supported_fields"
                    )
    exportable_contract = _get_exportable_contract(consumer_contract)
    for index, exportable_entry in enumerate(exportable_contract.get("supported_artifacts", [])):
        if str(exportable_entry.get("export_scope")) != "external":
            raise ValueError(f"exportable contract supported_artifacts[{index}] must remain external only")
        export_fields = [str(field_name) for field_name in exportable_entry.get("export_fields", [])]
        supported_fields = [str(field_name) for field_name in exportable_entry.get("supported_fields", [])]
        if any(field_name not in supported_fields for field_name in export_fields):
            raise ValueError(f"exportable contract supported_artifacts[{index}].export_fields must be a subset of supported_fields")


def _assert_export_contract_fully_converged(
    artifact_manifest: dict[str, Any],
    consumer_contract: dict[str, Any],
    consumer_contract_reference: str,
    export_contract: dict[str, Any],
) -> None:
    _assert_export_contract_matches_consumer_contract(consumer_contract, export_contract)
    _assert_export_contract_matches_manifest(artifact_manifest, export_contract)
    _assert_export_contract_matches_reference(consumer_contract, consumer_contract_reference, export_contract)


def _assert_export_contract_matches_consumer_contract(
    consumer_contract: dict[str, Any],
    export_contract: dict[str, Any],
) -> None:
    exportable_contract = _get_exportable_contract(consumer_contract)
    if str(export_contract.get("artifact_type")) != "export_contract":
        raise ValueError("export_contract.json artifact_type must remain export_contract")
    if str(export_contract.get("schema_version")) != str(consumer_contract.get("schema_version")):
        raise ValueError("export_contract.json schema_version must match consumer_contract.json")
    if str(export_contract.get("generated_by")) != str(consumer_contract.get("generated_by")):
        raise ValueError("export_contract.json generated_by must match consumer_contract.json")
    expected_export_order = [
        str(_normalize_artifact_path(entry.get("artifact_path")))
        for entry in consumer_contract.get("supported_artifacts", [])
        if isinstance(entry, dict) and entry.get("export_scope") == "external"
    ]
    actual_export_order = [
        str(_normalize_artifact_path(entry.get("artifact_path")))
        for entry in export_contract.get("supported_artifacts", [])
        if isinstance(entry, dict) and _normalize_artifact_path(entry.get("artifact_path")) is not None
    ]
    if actual_export_order != expected_export_order:
        raise ValueError("export_contract.json ordering must match consumer_contract.json export ordering exactly")
    if list(export_contract.get("supported_artifacts", [])) != list(exportable_contract.get("supported_artifacts", [])):
        raise ValueError("export_contract.json must match _get_exportable_contract(...) exactly")
    consumer_entries_by_path = {
        str(_normalize_artifact_path(entry.get("artifact_path"))): dict(entry)
        for entry in consumer_contract.get("supported_artifacts", [])
        if isinstance(entry, dict) and _normalize_artifact_path(entry.get("artifact_path")) is not None
    }
    for index, export_entry in enumerate(export_contract.get("supported_artifacts", [])):
        if not isinstance(export_entry, dict):
            raise ValueError(f"export_contract supported_artifacts[{index}] must be a dict")
        artifact_path = _normalize_artifact_path(export_entry.get("artifact_path"))
        if artifact_path not in consumer_entries_by_path:
            raise ValueError(f"export_contract supported_artifacts[{index}] must resolve to consumer_contract")
        consumer_entry = consumer_entries_by_path[artifact_path]
        if artifact_path != _normalize_artifact_path(consumer_entry.get("artifact_path")):
            raise ValueError(f"export_contract supported_artifacts[{index}].artifact_path must match consumer_contract for {artifact_path}")
        for key in ("artifact_type", "schema_version"):
            if str(export_entry.get(key)) != str(consumer_entry.get(key)):
                raise ValueError(
                    f"export_contract supported_artifacts[{index}].{key} must match consumer_contract for {artifact_path}"
                )
        expected_export_fields = consumer_entry.get("export_fields")
        if expected_export_fields is None:
            expected_export_fields = consumer_entry.get("supported_fields", [])
        if list(export_entry.get("export_fields", [])) != list(expected_export_fields):
            raise ValueError(
                f"export_contract supported_artifacts[{index}].export_fields must match consumer_contract exportable fields"
            )
        if str(export_entry.get("export_scope")) != "external":
            raise ValueError(f"export_contract supported_artifacts[{index}].export_scope must be external")
        export_fields = export_entry.get("export_fields")
        if not isinstance(export_fields, list) or not all(isinstance(field_name, str) for field_name in export_fields):
            raise ValueError(f"export_contract supported_artifacts[{index}].export_fields must be a list[str]")


def _assert_export_contract_matches_manifest(
    artifact_manifest: dict[str, Any],
    export_contract: dict[str, Any],
) -> None:
    manifest_entries = _artifact_manifest_entries_by_path(artifact_manifest)
    for index, export_entry in enumerate(export_contract.get("supported_artifacts", [])):
        if not isinstance(export_entry, dict):
            raise ValueError(f"export_contract supported_artifacts[{index}] must be a dict")
        artifact_path = _normalize_artifact_path(export_entry.get("artifact_path"))
        if artifact_path is None or artifact_path not in manifest_entries:
            raise ValueError(f"export_contract supported_artifacts[{index}] must resolve to artifact_manifest")
        manifest_entry = manifest_entries[artifact_path]
        if artifact_path != _normalize_artifact_path(manifest_entry.get("artifact_path")):
            raise ValueError(f"export_contract supported_artifacts[{index}].artifact_path must match artifact_manifest for {artifact_path}")
        for key in ("artifact_type", "schema_version"):
            if str(export_entry.get(key)) != str(manifest_entry.get(key)):
                raise ValueError(
                    f"export_contract supported_artifacts[{index}].{key} must match artifact_manifest for {artifact_path}"
                )
        if str(manifest_entry.get("scope")) != "workspace":
            raise ValueError(f"export_contract supported_artifacts[{index}] must resolve to a workspace artifact_manifest entry")


def _assert_export_contract_matches_reference(
    consumer_contract: dict[str, Any],
    consumer_contract_reference: str,
    export_contract: dict[str, Any],
) -> None:
    actual_export_order = [
        str(_normalize_artifact_path(entry.get("artifact_path")))
        for entry in export_contract.get("supported_artifacts", [])
        if isinstance(entry, dict) and _normalize_artifact_path(entry.get("artifact_path")) is not None
    ]
    expected_export_order = [
        str(_normalize_artifact_path(entry.get("artifact_path")))
        for entry in consumer_contract.get("supported_artifacts", [])
        if isinstance(entry, dict) and entry.get("export_scope") == "external"
    ]
    if actual_export_order != expected_export_order:
        raise ValueError("export_contract.json ordering must match consumer_contract.json export ordering exactly")
    reference_entries: list[dict[str, str]] = []
    current_reference_entry: dict[str, str] | None = None
    for line in consumer_contract_reference.splitlines():
        if line.startswith("### "):
            if current_reference_entry is not None:
                reference_entries.append(current_reference_entry)
            current_reference_entry = {"artifact_type": line.removeprefix("### ")}
        elif current_reference_entry is not None and line.startswith("- path: "):
            current_reference_entry["artifact_path"] = line.removeprefix("- path: ")
        elif current_reference_entry is not None and line.startswith("- schema_version: "):
            current_reference_entry["schema_version"] = line.removeprefix("- schema_version: ")
    if current_reference_entry is not None:
        reference_entries.append(current_reference_entry)
    export_reference_entries = [
        entry
        for entry in reference_entries
        if _normalize_artifact_path(entry.get("artifact_path")) in actual_export_order
    ]
    export_reference_paths = [str(_normalize_artifact_path(entry.get("artifact_path"))) for entry in export_reference_entries]
    if export_reference_paths != actual_export_order:
        raise ValueError("export_contract.json ordering must remain a reference-aligned subset of consumer_contract_reference.md")
    for index, reference_entry in enumerate(export_reference_entries):
        export_entry = export_contract["supported_artifacts"][index]
        for key in ("artifact_type", "schema_version"):
            if str(reference_entry.get(key)) != str(export_entry.get(key)):
                raise ValueError(f"export_contract supported_artifacts[{index}].{key} must match consumer_contract_reference.md")


def _build_consumer_contract_reference(consumer_contract: dict[str, Any]) -> str:
    lines = [
        "# DGCE Consumer Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {consumer_contract['schema_version']}",
        f"- generated_by: {consumer_contract['generated_by']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    for artifact in consumer_contract.get("supported_artifacts", []):
        lines.extend(
            [
                f"### {artifact['artifact_type']}",
                f"- path: {artifact['artifact_path']}",
                f"- schema_version: {artifact['schema_version']}",
                f"- contract_stability: {artifact['contract_stability']}",
            ]
        )
        consumer_scopes = artifact.get("consumer_scopes")
        if consumer_scopes:
            lines.append(f"- consumer_scopes: {', '.join(str(scope) for scope in consumer_scopes)}")
        lines.extend(
            [
                "",
                "#### Supported Fields",
            ]
        )
        lines.extend(f"- {field_name}" for field_name in artifact.get("supported_fields", []))
        lines.append("")
    return "\n".join(lines)


def _build_export_contract_reference(export_contract: dict[str, Any]) -> str:
    lines = [
        "# DGCE Export Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {export_contract['schema_version']}",
        f"- generated_by: {export_contract['generated_by']}",
        f"- artifact_type: {export_contract['artifact_type']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    for artifact in export_contract.get("supported_artifacts", []):
        lines.extend(
            [
                f"### {artifact['artifact_type']}",
                f"- artifact_path: {artifact['artifact_path']}",
                f"- schema_version: {artifact['schema_version']}",
                f"- contract_stability: {artifact['contract_stability']}",
                f"- export_scope: {artifact['export_scope']}",
            ]
        )
        consumer_scopes = artifact.get("consumer_scopes")
        if consumer_scopes:
            lines.append(f"- consumer_scopes: {', '.join(str(scope) for scope in consumer_scopes)}")
        lines.extend(
            [
                "",
                "#### Exported Fields",
            ]
        )
        lines.extend(f"- {field_name}" for field_name in artifact.get("export_fields", []))
        lines.append("")
    return "\n".join(lines)


def _assert_reference_aligns_with_contract(consumer_contract: dict[str, Any], consumer_contract_reference: str) -> None:
    lines = consumer_contract_reference.split("\n")
    expected_lines = [
        "# DGCE Consumer Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {consumer_contract['schema_version']}",
        f"- generated_by: {consumer_contract['generated_by']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    line_index = 0
    for expected_line in expected_lines:
        if line_index >= len(lines) or lines[line_index] != expected_line:
            raise ValueError("consumer_contract_reference.md metadata must match consumer_contract.json")
        line_index += 1
    for artifact in consumer_contract.get("supported_artifacts", []):
        artifact_lines = [
            f"### {artifact['artifact_type']}",
            f"- path: {artifact['artifact_path']}",
            f"- schema_version: {artifact['schema_version']}",
            f"- contract_stability: {artifact['contract_stability']}",
        ]
        if artifact.get("consumer_scopes"):
            artifact_lines.append(f"- consumer_scopes: {', '.join(str(scope) for scope in artifact['consumer_scopes'])}")
        artifact_lines.extend(["", "#### Supported Fields"])
        artifact_lines.extend(f"- {field_name}" for field_name in artifact.get("supported_fields", []))
        artifact_lines.append("")
        for expected_line in artifact_lines:
            if line_index >= len(lines) or lines[line_index] != expected_line:
                raise ValueError("consumer_contract_reference.md entries must match consumer_contract.json in order")
            line_index += 1
    if line_index != len(lines):
        raise ValueError("consumer_contract_reference.md must not include extra content")


def _assert_export_reference_matches_export_contract(export_contract: dict[str, Any], export_contract_reference: str) -> None:
    lines = export_contract_reference.split("\n")
    expected_lines = [
        "# DGCE Export Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {export_contract['schema_version']}",
        f"- generated_by: {export_contract['generated_by']}",
        f"- artifact_type: {export_contract['artifact_type']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    line_index = 0
    for expected_line in expected_lines:
        if line_index >= len(lines) or lines[line_index] != expected_line:
            raise ValueError("export_contract_reference.md metadata must match export_contract.json")
        line_index += 1
    for artifact in export_contract.get("supported_artifacts", []):
        artifact_lines = [
            f"### {artifact['artifact_type']}",
            f"- artifact_path: {artifact['artifact_path']}",
            f"- schema_version: {artifact['schema_version']}",
            f"- contract_stability: {artifact['contract_stability']}",
            f"- export_scope: {artifact['export_scope']}",
        ]
        if artifact.get("consumer_scopes"):
            artifact_lines.append(f"- consumer_scopes: {', '.join(str(scope) for scope in artifact['consumer_scopes'])}")
        artifact_lines.extend(["", "#### Exported Fields"])
        artifact_lines.extend(f"- {field_name}" for field_name in artifact.get("export_fields", []))
        artifact_lines.append("")
        for expected_line in artifact_lines:
            if line_index >= len(lines) or lines[line_index] != expected_line:
                raise ValueError("export_contract_reference.md entries must match export_contract.json in order")
            line_index += 1
    if line_index != len(lines):
        raise ValueError("export_contract_reference.md must not include extra content")


def _refresh_workspace_views(workspace: dict[str, Path]) -> None:
    section_ids = _read_workspace_index(workspace["index"])
    review_index = _build_review_index(workspace["root"], section_ids)
    workspace_summary = _build_workspace_summary(workspace["root"], section_ids, review_index)
    lifecycle_trace = _build_lifecycle_trace(workspace["root"], section_ids)
    workspace_index = _build_workspace_index(workspace["root"], section_ids, workspace_summary, lifecycle_trace)
    dashboard = _build_dashboard_view(workspace["root"], section_ids, review_index, lifecycle_trace, workspace_index)
    artifact_manifest = _build_artifact_manifest(review_index, workspace_summary, lifecycle_trace, workspace_index, dashboard)
    consumer_contract = _build_consumer_contract(
        review_index,
        workspace_summary,
        lifecycle_trace,
        workspace_index,
        dashboard,
        artifact_manifest,
    )
    _assert_contract_aligns_with_manifest(artifact_manifest, consumer_contract)
    export_contract = _build_export_contract(consumer_contract)
    consumer_contract_reference = _build_consumer_contract_reference(consumer_contract)
    export_contract_reference = _build_export_contract_reference(export_contract)
    _assert_export_contract_fully_converged(artifact_manifest, consumer_contract, consumer_contract_reference, export_contract)
    _assert_reference_aligns_with_contract(consumer_contract, consumer_contract_reference)
    _assert_export_reference_matches_export_contract(export_contract, export_contract_reference)
    _write_json(workspace["reviews"] / "index.json", review_index)
    _write_json(workspace["root"] / "workspace_summary.json", workspace_summary)
    _write_json(workspace["root"] / "lifecycle_trace.json", lifecycle_trace)
    _write_json(workspace["root"] / "workspace_index.json", workspace_index)
    _write_json(workspace["root"] / "dashboard.json", dashboard)
    _write_json(workspace["root"] / "artifact_manifest.json", artifact_manifest)
    _write_json(workspace["root"] / "consumer_contract.json", consumer_contract)
    _write_json(workspace["root"] / "export_contract.json", export_contract)
    (workspace["root"] / "consumer_contract_reference.md").write_text(consumer_contract_reference, encoding="utf-8")
    (workspace["root"] / "export_contract_reference.md").write_text(export_contract_reference, encoding="utf-8")


def _run_mode_from_allow_safe_modify(allow_safe_modify: bool) -> str:
    """Return the persisted DGCE run-mode label for the current workspace run."""
    return "safe_modify" if allow_safe_modify else "create_only"


def _effective_allow_safe_modify(section: DGCESection, allow_safe_modify: bool) -> bool:
    """Enable safe modify explicitly or for data-model sections in development mode."""
    return allow_safe_modify or _development_data_model_modify_enabled(section)


def _development_data_model_modify_enabled(section: DGCESection) -> bool:
    """Return True when development-mode data-model runs may write modify targets."""
    if section.section_type != "data_model":
        return False

    environment = (
        os.getenv("AETHER_ENVIRONMENT")
        or os.getenv("AETHER_ENV")
        or os.getenv("ENVIRONMENT")
        or ""
    ).strip().lower()
    return environment in {"dev", "development", "local"}


def _build_run_outcome_class(run_mode: str, execution_outcome: dict) -> str:
    """Return one deterministic normalized run-outcome class for the current workspace run."""
    validation = execution_outcome.get("validation_summary", {})
    if validation.get("ok") is False:
        return "validation_failure"

    change_plan = execution_outcome.get("change_plan_summary", {})
    execution = execution_outcome.get("execution_summary", {})
    planned_modify_count = int(change_plan.get("modify_count", 0))
    written_files_count = int(execution.get("written_files_count", 0))
    skipped_modify_count = int(execution.get("skipped_modify_count", 0))
    skipped_ignore_count = int(execution.get("skipped_ignore_count", 0))
    skipped_identical_count = int(execution.get("skipped_identical_count", 0))
    skipped_ownership_count = int(execution.get("skipped_ownership_count", 0))
    skipped_exists_fallback_count = int(execution.get("skipped_exists_fallback_count", 0))

    if (
        written_files_count == 0
        and skipped_ignore_count == 0
        and skipped_identical_count == 0
        and skipped_ownership_count == 0
        and skipped_exists_fallback_count == 0
    ):
        if planned_modify_count > 0 and skipped_modify_count > 0:
            return "execution_no_changes"
        if execution_outcome.get("status") == "error":
            return "success_safe_modify" if run_mode == "safe_modify" else "success_create_only"

    if execution_outcome.get("status") == "error":
        return "execution_error"

    if skipped_ownership_count > 0:
        return "partial_skipped_ownership"
    if skipped_modify_count > 0:
        return "partial_skipped_modify"
    if skipped_ignore_count > 0:
        return "partial_skipped_ignore"
    if skipped_exists_fallback_count > 0:
        return "partial_skipped_exists_fallback"
    if skipped_identical_count > 0:
        return "partial_skipped_identical"
    if run_mode == "safe_modify":
        return "success_safe_modify"
    return "success_create_only"


def _load_ownership_index(ownership_index_path: Path) -> dict:
    """Load the persisted ownership index, defaulting to the stable contract shape."""
    if not ownership_index_path.exists():
        return {"files": []}

    payload = ownership_index_path.read_text(encoding="utf-8").strip()
    if not payload:
        return {"files": []}

    parsed = json.loads(payload)
    files = parsed.get("files", [])
    if not isinstance(files, list):
        return {"files": []}
    return {"files": [dict(entry) for entry in files if isinstance(entry, dict)]}


def _merge_ownership_index(existing_ownership_index: dict, section_id: str, write_transparency: dict) -> dict:
    """Preserve existing ownership unless the current run positively refreshes written paths."""
    current_run_entries: list[dict] = []
    for entry in sorted(
        write_transparency.get("write_decisions", []),
        key=lambda item: str(item.get("path", "")),
    ):
        if entry.get("decision") != "written":
            continue
        current_run_entries.append(
            {
                "path": str(entry.get("path")),
                "section_id": section_id,
                "last_written_stage": DGCEWorkspaceStage.WRITE,
                "write_reason": str(entry.get("reason")),
            }
        )

    existing_files = existing_ownership_index.get("files", [])
    if not current_run_entries:
        return {"files": sorted([dict(entry) for entry in existing_files], key=lambda item: str(item.get("path", "")))}

    merged_by_path = {
        str(entry.get("path")): dict(entry)
        for entry in existing_files
        if isinstance(entry, dict) and entry.get("path")
    }
    for entry in current_run_entries:
        merged_by_path[entry["path"]] = entry

    return {"files": [merged_by_path[path] for path in sorted(merged_by_path)]}


def _plan_entry(task: ClassificationRequest) -> Dict[str, str]:
    """Build the lightweight persisted task metadata for one section task."""
    bucket = ClassifierRules().classify(task.content)["bucket"].value
    return {
        "task_id": task.request_id,
        "task_type": task.task_type or "",
        "task_bucket": bucket,
        "status": "pending",
    }


def _update_workspace_index(index_path: Path, section_id: str) -> None:
    """Persist a stable list of known section identifiers."""
    sections = _read_workspace_index(index_path)
    if section_id not in sections:
        sections.append(section_id)
    lines = ["sections:"] + [f"  - {value}" for value in sorted(sections)]
    index_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_workspace_index(index_path: Path) -> List[str]:
    """Read the minimal workspace index format."""
    if not index_path.exists():
        return []

    sections: List[str] = []
    for line in index_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            sections.append(stripped[2:].strip())
    return sections


def _format_list(items: List[str], label: str) -> str:
    """Format a deterministic inline list block for prompt templates."""
    if not items:
        return f"{label}: none"
    return f"{label}: " + "; ".join(items)


def _fallback_expected_target_file_plan(section: DGCESection, responses: List[ResponseEnvelope]) -> FilePlan:
    """Build the governed expected-target fallback file plan, enriching system-breakdown sections when possible."""
    requirements = section.requirements
    expected_targets = section.expected_targets
    if _load_system_breakdown_payload(responses):
        requirements = _system_breakdown_expected_target_requirements(section, responses)
        files = [
            _expected_target_to_file_entry(
                _system_breakdown_expected_target_entry(entry, requirements),
                requirements,
            )
            for entry in expected_targets
        ]
    else:
        files = [_expected_target_to_file_entry(entry, requirements) for entry in expected_targets]
    return FilePlan(project_name="DGCE", files=files)


def _governed_owned_target_file_plan(
    section: DGCESection,
    responses: List[ResponseEnvelope],
    file_plan: FilePlan,
    *,
    require_preflight_pass: bool,
    incremental_mode: Optional[str],
) -> FilePlan:
    """Return the explicit owned-file materialization set for previewed or governed runs."""
    if not require_preflight_pass and incremental_mode not in {"incremental_v2", "incremental_v2_1", "incremental_v2_2"}:
        return file_plan

    explicit_owned_bundle = _explicit_owned_bundle_file_plan(file_plan, responses)
    if explicit_owned_bundle.files and section.expected_targets:
        return _merge_governed_file_plans(explicit_owned_bundle, _fallback_expected_target_file_plan(section, responses))
    if explicit_owned_bundle.files:
        return explicit_owned_bundle
    if section.expected_targets:
        return _fallback_expected_target_file_plan(section, responses)
    return file_plan


def _explicit_owned_bundle_file_plan(
    file_plan: FilePlan,
    responses: List[ResponseEnvelope],
) -> FilePlan:
    """Return only planned files that are explicitly owned by the current section contract."""
    owned_paths = _collect_explicit_owned_paths(responses)
    if not owned_paths:
        return FilePlan(project_name=file_plan.project_name, files=[])

    candidate_files: list[dict[str, Any]] = [dict(file_entry) for file_entry in file_plan.files]
    for response in responses:
        payload = _structured_response_payload(response)
        if not payload or response.task_type != "system_breakdown":
            continue
        candidate_files.extend(_system_breakdown_files(payload, payload))

    files: list[dict[str, Any]] = []
    for file_entry in candidate_files:
        normalized_path = _normalize_governed_bundle_path(file_entry.get("path"))
        if normalized_path is None or normalized_path not in owned_paths:
            continue
        normalized_entry = dict(file_entry)
        normalized_entry["path"] = normalized_path
        files.append(normalized_entry)

    deduped = {
        str(entry["path"]): entry
        for entry in sorted(files, key=lambda entry: str(entry["path"]))
    }
    return FilePlan(project_name=file_plan.project_name, files=list(deduped.values()))


def _collect_explicit_owned_paths(responses: List[ResponseEnvelope]) -> set[str]:
    """Collect exact owned file paths declared in structured module contracts for the section."""
    owned_paths: set[str] = set()
    for response in responses:
        payload = _structured_response_payload(response)
        if not payload:
            continue
        for module in payload.get("modules", []):
            if not isinstance(module, dict):
                continue
            for owned_path in module.get("owned_paths", []):
                normalized_path = _normalize_governed_bundle_path(owned_path)
                if normalized_path is None:
                    continue
                owned_paths.add(normalized_path)
    return owned_paths


def _structured_response_payload(response: ResponseEnvelope) -> dict[str, Any] | None:
    """Return the normalized structured payload for one response when available."""
    payload = response.structured_content
    if isinstance(payload, dict):
        return payload
    if not response.output.strip():
        return None
    try:
        parsed = json.loads(response.output)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_governed_bundle_path(path_value: Any) -> str | None:
    """Normalize one explicit owned file path, ignoring templates and non-file path shapes."""
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    if "{" in path_value or "}" in path_value:
        return None
    try:
        normalized_path = Path(path_value).as_posix()
        normalized = Path(normalized_path)
        if normalized.is_absolute():
            return None
        normalized_parts = []
        for part in normalized.parts:
            if part in {"", "."}:
                continue
            if part == "..":
                return None
            normalized_parts.append(part)
        if not normalized_parts:
            return None
        return Path(*normalized_parts).as_posix()
    except (TypeError, ValueError):
        return None


def _merge_governed_file_plans(primary: FilePlan, secondary: FilePlan) -> FilePlan:
    """Merge governed file plans by exact path while preserving primary entries."""
    merged: dict[str, dict[str, Any]] = {}
    for plan in (primary, secondary):
        for file_entry in plan.files:
            normalized_path = _normalize_governed_bundle_path(file_entry.get("path"))
            if normalized_path is None or normalized_path in merged:
                continue
            normalized_entry = dict(file_entry)
            normalized_entry["path"] = normalized_path
            merged[normalized_path] = normalized_entry
    return FilePlan(
        project_name=primary.project_name or secondary.project_name,
        files=[merged[path] for path in sorted(merged)],
    )


def _system_breakdown_expected_target_requirements(
    section: DGCESection,
    responses: List[ResponseEnvelope],
) -> List[str]:
    """Derive deterministic expected-target requirements from the structured system-breakdown contract."""
    requirements = list(section.requirements)
    payload = _load_system_breakdown_payload(responses)
    if not payload:
        return requirements

    modules = payload.get("modules", [])
    module_names = [
        str(module.get("name"))
        for module in modules
        if isinstance(module, dict) and isinstance(module.get("name"), str)
    ]
    if module_names:
        requirements.append("Module contracts: " + ", ".join(module_names))

    build_graph = payload.get("build_graph", {})
    edges = build_graph.get("edges", []) if isinstance(build_graph, dict) else []
    formatted_edges = [
        f"{edge[0]}->{edge[1]}"
        for edge in edges
        if isinstance(edge, list)
        and len(edge) == 2
        and all(isinstance(node, str) for node in edge)
    ]
    if formatted_edges:
        requirements.append("Build graph: " + ", ".join(formatted_edges))

    tests = payload.get("tests", [])
    test_names = [
        str(test.get("name"))
        for test in tests
        if isinstance(test, dict) and isinstance(test.get("name"), str)
    ]
    if test_names:
        requirements.append("Verification: " + ", ".join(test_names))

    return requirements


def _load_system_breakdown_payload(responses: List[ResponseEnvelope]) -> dict[str, Any] | None:
    """Return the structured system-breakdown payload from current section responses when available."""
    for response in responses:
        if response.task_type != "system_breakdown" or not response.output.strip():
            continue
        try:
            payload = json.loads(response.output)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("modules"), list):
            return payload
    return None


def _system_breakdown_expected_target_entry(entry: Any, requirements: List[str]) -> Any:
    """Attach deterministic system-breakdown purpose text to one governed expected target entry."""
    if not isinstance(entry, dict):
        path = str(entry)
        return {
            "path": path,
            "purpose": _system_breakdown_expected_target_purpose(path),
            "source": "expected_targets",
            "requirements": requirements,
        }

    enriched = dict(entry)
    enriched["purpose"] = str(enriched.get("purpose") or _system_breakdown_expected_target_purpose(str(enriched.get("path", ""))))
    enriched.setdefault("source", "expected_targets")
    enriched["requirements"] = requirements
    return enriched


def _system_breakdown_expected_target_purpose(path: str) -> str:
    """Return a deterministic system-breakdown purpose label for one governed expected target path."""
    normalized = str(path).replace("\\", "/").lower()
    if normalized.endswith("decompose.py"):
        return "System-breakdown orchestration and contract rendering"
    if normalized.endswith("incremental.py"):
        return "System-breakdown target grounding and change planning"
    if normalized.endswith("file_writer.py"):
        return "System-breakdown scaffold writing from governed contract"
    if normalized.endswith("dce.py"):
        return "System-breakdown CLI orchestration entrypoint"
    return "System-breakdown governed contract implementation"


def _expected_target_to_file_entry(entry: Any, requirements: List[str] | None = None) -> dict[str, Any]:
    """Normalize one expected-target entry into a FilePlan file record."""
    normalized_requirements = list(requirements or [])
    if isinstance(entry, dict):
        return {
            "path": str(entry.get("path", "")),
            "purpose": str(entry.get("purpose", "")),
            "source": str(entry.get("source", "expected_targets")),
            "requirements": normalized_requirements,
        }

    return {
        "path": str(entry),
        "purpose": "",
        "source": "expected_targets",
        "requirements": normalized_requirements,
    }


def _slug(value: str) -> str:
    """Create a simple deterministic identifier from a title."""
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "section"
