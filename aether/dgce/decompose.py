"""Deterministic DGCE section decomposition and execution loop."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from aether.dgce.file_plan import FilePlan, build_file_plan
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
    file_plan = build_file_plan(responses)
    if not file_plan.files and section.expected_targets:
        file_plan = _fallback_expected_target_file_plan(section, responses)
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
        _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
        _write_json(
            workspace_summary_path,
            _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
        )
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
        _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
        _write_json(
            workspace_summary_path,
            _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
        )
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
    )
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace_summary_path,
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )

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
    )


def run_dgce_section(
    section_id: str,
    project_root: Path,
    *,
    governed: bool = True,
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

    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    approval_path = _ensure_workspace(project_root)["approvals"] / f"{section_id}.approval.json"
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
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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
        record_section_stale_check(project_root, section_id, SectionStaleCheckInput(validation_timestamp=preflight.validation_timestamp) if preflight else None)
    preflight_payload = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else None
    stale_check_payload = json.loads(stale_check_path.read_text(encoding="utf-8")) if stale_check_path.exists() else None
    gate_artifact = _build_execution_gate_artifact(
        workspace["root"],
        section_id,
        require_preflight_pass=require_preflight_pass,
        gate_input=gate_input,
        preflight_payload=preflight_payload,
        stale_check_payload=stale_check_payload,
    )
    _write_json(workspace["preflight"] / f"{section_id}.execution_gate.json", gate_artifact)
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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
    )
    if execution_artifact["approval_consumed"]:
        _supersede_approval_artifact(workspace["root"], section_id)
        execution_artifact["approval_status_after"] = "superseded"
    _write_json(workspace["execution"] / f"{section_id}.execution.json", execution_artifact)
    _write_json(workspace["reviews"] / "index.json", _build_review_index(workspace["root"], _read_workspace_index(workspace["index"])))
    _write_json(
        workspace["root"] / "workspace_summary.json",
        _build_workspace_summary(workspace["root"], _read_workspace_index(workspace["index"])),
    )
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


def _write_json(path: Path, payload: object) -> None:
    """Persist deterministic JSON with stable formatting."""
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
    return {
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
    }


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
        "current_input_fingerprint": current_input_fingerprint,
        "current_preview_fingerprint": current_preview_fingerprint,
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
) -> dict[str, Any]:
    """Build a deterministic execution-stamp artifact from current run facts and linked metadata."""
    approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
    preflight_path = workspace_root / "preflight" / f"{section_id}.preflight.json"
    execution_gate_path = workspace_root / "preflight" / f"{section_id}.execution_gate.json"
    alignment_path = workspace_root / "preflight" / f"{section_id}.alignment.json"

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    preflight_payload = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
    gate_payload = json.loads(execution_gate_path.read_text(encoding="utf-8")) if execution_gate_path.exists() else {}
    alignment_payload = json.loads(alignment_path.read_text(encoding="utf-8")) if alignment_path.exists() else {}

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

    return {
        "section_id": section_id,
        "execution_status": execution_status,
        "governed_execution": governed_execution,
        "require_preflight_pass": require_preflight_pass,
        "approval_path": approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None,
        "preflight_path": preflight_path.relative_to(workspace_root.parent).as_posix() if preflight_path.exists() else None,
        "execution_gate_path": execution_gate_path.relative_to(workspace_root.parent).as_posix() if execution_gate_path.exists() else None,
        "alignment_path": alignment_path.relative_to(workspace_root.parent).as_posix() if alignment_path.exists() else None,
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
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
    selected_mode = approval_payload.get("selected_mode")
    effective_execution_mode = _effective_execution_mode(write_transparency)

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

    return {
        "section_id": section_id,
        "alignment_status": alignment_status,
        "alignment_blocked": alignment_blocked,
        "selected_mode": selected_mode,
        "effective_execution_mode": effective_execution_mode,
        "alignment_reason": alignment_reason,
        "require_preflight_pass": require_preflight_pass,
        "alignment_timestamp": str(alignment_input.alignment_timestamp),
    }


def _build_review_index(workspace_root: Path, section_ids: List[str]) -> dict:
    """Build a deterministic review index from known section preview/review artifacts."""
    sections: list[dict[str, Any]] = []
    for section_id in sorted(section_ids):
        preview_path = workspace_root / "plans" / f"{section_id}.preview.json"
        review_path = workspace_root / "reviews" / f"{section_id}.review.md"
        approval_path = workspace_root / "approvals" / f"{section_id}.approval.json"
        preflight_path = workspace_root / "preflight" / f"{section_id}.preflight.json"
        stale_check_path = workspace_root / "preflight" / f"{section_id}.stale_check.json"
        execution_gate_path = workspace_root / "preflight" / f"{section_id}.execution_gate.json"
        alignment_path = workspace_root / "preflight" / f"{section_id}.alignment.json"
        execution_path = workspace_root / "execution" / f"{section_id}.execution.json"
        if not any(
            path.exists()
            for path in (
                preview_path,
                review_path,
                approval_path,
                preflight_path,
                stale_check_path,
                execution_gate_path,
                alignment_path,
                execution_path,
            )
        ):
            continue

        preview_payload = json.loads(preview_path.read_text(encoding="utf-8")) if preview_path.exists() else {}
        approval_payload = json.loads(approval_path.read_text(encoding="utf-8")) if approval_path.exists() else {}
        preflight_payload = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path.exists() else {}
        stale_check_payload = json.loads(stale_check_path.read_text(encoding="utf-8")) if stale_check_path.exists() else {}
        execution_gate_payload = json.loads(execution_gate_path.read_text(encoding="utf-8")) if execution_gate_path.exists() else {}
        alignment_payload = json.loads(alignment_path.read_text(encoding="utf-8")) if alignment_path.exists() else {}
        execution_payload = json.loads(execution_path.read_text(encoding="utf-8")) if execution_path.exists() else {}
        sections.append(
            {
                "section_id": section_id,
                "preview_path": preview_path.relative_to(workspace_root.parent).as_posix() if preview_path.exists() else None,
                "review_path": review_path.relative_to(workspace_root.parent).as_posix() if review_path.exists() else None,
                "preview_outcome_class": preview_payload.get("preview_outcome_class"),
                "recommended_mode": preview_payload.get("recommended_mode"),
                "approval_path": approval_path.relative_to(workspace_root.parent).as_posix() if approval_path.exists() else None,
                "approval_status": approval_payload.get("approval_status"),
                "selected_mode": approval_payload.get("selected_mode"),
                "execution_permitted": approval_payload.get("execution_permitted"),
                "preflight_path": preflight_path.relative_to(workspace_root.parent).as_posix() if preflight_path.exists() else None,
                "preflight_status": preflight_payload.get("preflight_status"),
                "stale_check_path": stale_check_path.relative_to(workspace_root.parent).as_posix() if stale_check_path.exists() else None,
                "stale_status": stale_check_payload.get("stale_status"),
                "stale_detected": stale_check_payload.get("stale_detected"),
                "execution_allowed": preflight_payload.get("execution_allowed"),
                "execution_gate_path": execution_gate_path.relative_to(workspace_root.parent).as_posix() if execution_gate_path.exists() else None,
                "gate_status": execution_gate_payload.get("gate_status"),
                "execution_blocked": execution_gate_payload.get("execution_blocked"),
                "alignment_path": alignment_path.relative_to(workspace_root.parent).as_posix() if alignment_path.exists() else None,
                "alignment_status": alignment_payload.get("alignment_status"),
                "alignment_blocked": alignment_payload.get("alignment_blocked"),
                "execution_path": execution_path.relative_to(workspace_root.parent).as_posix() if execution_path.exists() else None,
                "execution_status": execution_payload.get("execution_status"),
                "approval_consumed": execution_payload.get("approval_consumed"),
                "approval_status_after": execution_payload.get("approval_status_after"),
            }
        )

    return {"sections": sorted(sections, key=lambda entry: str(entry["section_id"]))}


def _build_workspace_summary(workspace_root: Path, section_ids: List[str]) -> dict:
    """Build a deterministic workspace summary from persisted .dce artifacts."""
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
        output_path = workspace_root / "outputs" / f"{section_id}.json"
        payload = json.loads(output_path.read_text(encoding="utf-8")) if output_path.exists() else {}
        execution_outcome = payload.get("execution_outcome", {})
        advisory = payload.get("advisory")
        validation = execution_outcome.get("validation_summary", {})
        execution = execution_outcome.get("execution_summary", {})
        review_entry = review_sections.get(section_id, {})
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
            }
        )

    sections = sorted(sections, key=lambda entry: str(entry["section_id"]))
    return {
        "total_sections_seen": len(sections),
        "sections": sections,
    }


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
