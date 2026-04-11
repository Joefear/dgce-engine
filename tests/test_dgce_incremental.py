import json
import subprocess
from pathlib import Path

import aether.dgce.decompose as dgce_decompose
import pytest
from aether.dgce import DGCESection, FilePlan, SectionAlignmentInput, SectionApprovalInput, SectionExecutionGateInput, SectionExecutionStampInput, SectionPreflightInput, SectionSimulationInput, SectionStaleCheckInput, compute_artifact_fingerprint, compute_json_payload_fingerprint, execute_reserved_simulation_gate, record_section_alignment, record_section_approval, record_section_execution_gate, record_section_execution_stamp, record_section_preflight, record_section_simulation, record_section_stale_check, run_dgce_section, run_section_with_workspace
from aether.dgce.decompose import ResponseEnvelope, _build_run_outcome_class, _validate_write_stage_structured_content, compute_preview_payload_fingerprint
from aether.dgce.incremental import (
    build_change_plan,
    build_incremental_change_plan,
    build_incremental_preview_artifact,
    build_write_transparency,
    classify_section_targets,
    classify_incremental_targets,
    finalize_write_transparency,
    filter_file_plan_for_controlled_write,
    load_change_plan,
    overwrite_paths_from_transparency,
    render_incremental_review_markdown,
    scan_workspace_file_paths,
    scan_workspace_inventory,
    summarize_incremental_preview,
    should_write_planned_file,
)
from aether.dgce.file_writer import render_file_entry_bytes
from aether_core.enums import ArtifactStatus
from aether_core.itera.artifact_store import ArtifactStore
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.models import ClassificationRequest
from aether_core.models.request import OutputContract
from aether_core.router.executors import ExecutionResult
from aether_core.router.planner import RouterPlanner


def _section() -> DGCESection:
    return DGCESection(
        section_type="game_system",
        title="Mission Board",
        description="A modular mission board that assembles contracts and tracks player progression.",
        requirements=["support mission templates", "track progression state"],
        constraints=["keep save format stable", "support mod extension points"],
    )


def _section_named(title: str) -> DGCESection:
    section = _section().model_copy()
    section.title = title
    return section


def _infra_file_plan(path: str = "deploy/docker-compose.yaml") -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Deployment manifest",
                "source": "expected_targets",
            }
        ],
    )


def _compose_file_plan(*paths: str) -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Compose manifest",
                "source": "expected_targets",
            }
            for path in paths
        ],
    )


def _dockerfile_file_plan(*paths: str) -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Dockerfile",
                "source": "expected_targets",
            }
            for path in paths
        ],
    )


def _k8s_file_plan(*paths: str) -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Kubernetes manifest",
                "source": "expected_targets",
            }
            for path in paths
        ],
    )


def _terraform_file_plan(*paths: str) -> FilePlan:
    return FilePlan(
        project_name="DGCE",
        files=[
            {
                "path": path,
                "purpose": "Terraform module",
                "source": "expected_targets",
            }
            for path in paths
        ],
    )


def _expected_trigger_reason_summary(*reason_codes: str) -> str | None:
    fragments = {
        "deployment_artifact": "deployment artifacts",
        "design_required_simulation": "approved design constraints",
        "infrastructure_touching": "infrastructure-touching changes",
        "irreversible_operation": "modify operations",
        "policy_required_simulation": "governance policy requirements",
        "runtime_control": "runtime-control artifacts",
    }
    if not reason_codes:
        return None
    return "Simulation was required due to " + "; ".join(fragments[code] for code in reason_codes) + "."


def _valid_code_graph_context(file_path: str = "mission_board/service.py") -> dict:
    return {
        "contract_name": "DefiantCodeGraphFacts",
        "contract_version": "dcg.facts.v1",
        "graph_id": "graph:mission-board",
        "workspace_id": "workspace:dgce",
        "repo_id": "repo:dgce-engine",
        "language": "python",
        "generated_at": "2026-04-04T10:15:30Z",
        "source": "defiant-code-graph",
        "target": {
            "file_path": file_path,
            "symbol_id": "sym:mission_board_service",
            "symbol_name": "MissionBoardService",
            "symbol_kind": "class",
            "span": {"start_line": 3, "end_line": 30},
        },
        "intent_facts": {
            "structural_scope": "file",
            "module_boundary_crossed": False,
            "trust_boundary_crossed": False,
            "ownership_boundary_crossed": False,
            "protected_region_overlap": False,
            "governed_region_overlap": True,
            "related_symbols": ["sym:mission_board_router"],
            "related_files": [file_path],
        },
        "patch_facts": {
            "touched_files": [file_path],
            "touched_symbols": ["sym:mission_board_service"],
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
                    "file_path": file_path,
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
            "dependency_crossings": [],
            "dependent_symbols": ["sym:payload_consumer"],
        },
        "ownership_facts": {
            "target_ownership": "governed",
            "touched_ownership_classes": ["governed"],
        },
        "meta": {
            "parser_family": "tree-sitter",
            "snapshot_id": "snapshot:mission-board",
            "notes": ["read-only facts"],
        },
    }


def _ranked_code_graph_context() -> dict:
    payload = _valid_code_graph_context()
    payload["placement_facts"]["recommended_edit_strategy"] = "rewrite_small_region"
    payload["placement_facts"]["generation_collision_detected"] = False
    payload["placement_facts"]["insertion_candidates"] = [
        {
            "file_path": "mission_board/service.py",
            "symbol_id": "sym:inside_target",
            "symbol_name": "MissionBoardService",
            "strategy": "inside_symbol",
            "span": {"start_line": 6, "end_line": 20},
        },
        {
            "file_path": "mission_board/service.py",
            "symbol_id": "sym:append_local",
            "symbol_name": "existing_helper",
            "strategy": "append_after_symbol",
            "span": {"start_line": 7, "end_line": 7},
        },
        {
            "file_path": "mission_board/service.py",
            "symbol_id": "sym:before_local",
            "symbol_name": "existing_helper",
            "strategy": "insert_before_symbol",
            "span": {"start_line": 8, "end_line": 8},
        },
    ]
    return payload


def _ambiguous_code_graph_context() -> dict:
    payload = _valid_code_graph_context()
    payload["placement_facts"]["generation_collision_detected"] = True
    payload["placement_facts"]["recommended_edit_strategy"] = "rewrite_small_region"
    payload["placement_facts"]["insertion_candidates"] = [
        {
            "file_path": "mission_board/service.py",
            "symbol_id": "sym:existing_helper",
            "symbol_name": "existing_helper",
            "strategy": "append_after_symbol",
            "span": {"start_line": 7, "end_line": 7},
        }
    ]
    return payload


def _malformed_code_graph_context() -> dict:
    return {"contract_name": "DefiantCodeGraphFacts", "contract_version": "wrong", "graph_id": "graph:bad"}


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


def _write_text(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_section_input(project_root: Path, section: DGCESection | None = None) -> None:
    target_section = section or _section()
    section_id = str(target_section.section_id).strip() or target_section.title.lower().replace(" ", "-")
    path = project_root / ".dce" / "input" / f"{section_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(target_section.model_dump(), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_ownership_index(project_root: Path, files: list[dict]) -> None:
    path = project_root / ".dce" / "ownership_index.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"files": files}, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _expected_section_summary(
    *,
    section_id: str,
    approval_status=None,
    decision_source=None,
    latest_decision=None,
    latest_stage=None,
    latest_stage_status=None,
    review_status=None,
):
    return {
        "approval_status": approval_status,
        "decision_source": decision_source,
        "latest_decision": latest_decision,
        "latest_decision_source": decision_source,
        "latest_stage": latest_stage,
        "latest_stage_status": latest_stage_status,
        "review_status": review_status,
        "section_id": section_id,
        "simulation": {
            "applicable_providers": [],
            "advisory_provider": None,
            "findings_count": 0,
            "finding_codes": [],
            "provider_execution_state": "not_run",
            "provider_execution_summary": "simulation not executed",
            "provider_execution_target": None,
            "provider_selection_source": None,
            "provider_resolution": None,
            "reason_code": None,
            "reason_summary": None,
            "selected_provider": None,
            "simulation_provider": None,
            "simulation_stage_applicable": False,
            "simulation_status": None,
            "simulation_triggered": False,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
        "summary_sources": {
            "approval_status": "approval" if approval_status is not None else None,
            "latest_decision": (
                "approval.selected_mode"
                if decision_source == "approval"
                else "preview.recommended_mode"
                if decision_source == "preview_recommendation"
                else None
            ),
            "latest_stage": "lifecycle_trace",
            "latest_stage_status": "lifecycle_trace",
            "review_status": "review" if review_status is not None else None,
            "simulation": None,
        },
    }


def _explicit_non_triggered_simulation_projection(simulation_provider=None):
    return {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 0,
        "finding_codes": [],
        "provider_execution_state": "not_run",
        "provider_execution_summary": "simulation not executed",
        "provider_execution_target": None,
        "provider_selection_source": "not_applicable",
        "provider_resolution": None,
        "reason_code": None,
        "reason_summary": None,
        "selected_provider": None,
        "simulation_provider": simulation_provider,
        "simulation_stage_applicable": True,
        "simulation_status": "skipped",
        "simulation_triggered": False,
        "trigger_reason_codes": [],
        "trigger_reason_summary": None,
    }


def _expected_artifact_metadata(artifact_type: str):
    return {
        "artifact_type": artifact_type,
        "generated_by": "DGCE",
        "schema_version": "1.0",
    }


def _changed_lines_estimate(before_bytes: bytes, after_bytes: bytes) -> int:
    before_lines = before_bytes.decode("utf-8", errors="replace").splitlines()
    after_lines = after_bytes.decode("utf-8", errors="replace").splitlines()
    max_lines = max(len(before_lines), len(after_lines))
    return sum(
        1
        for index in range(max_lines)
        if (before_lines[index] if index < len(before_lines) else None)
        != (after_lines[index] if index < len(after_lines) else None)
    )


def _artifact_section_map(project_root: Path) -> dict[str, dict[str, dict]]:
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    lifecycle_trace = json.loads((project_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"))
    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    dashboard = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))
    return {
        "dashboard": {entry["section_id"]: entry for entry in dashboard["sections"]},
        "review_index": {entry["section_id"]: entry for entry in review_index["sections"]},
        "workspace_summary": {entry["section_id"]: entry for entry in workspace_summary["sections"]},
        "lifecycle_trace": {entry["section_id"]: entry for entry in lifecycle_trace["sections"]},
        "workspace_index": {entry["section_id"]: entry for entry in workspace_index["sections"]},
    }


def _artifact_manifest_by_path(project_root: Path) -> dict[str, dict]:
    manifest = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    return {entry["artifact_path"]: entry for entry in manifest["artifacts"]}


def _expected_consumer_contract_supported_artifacts() -> list[dict]:
    return [
        {
            "artifact_type": "dashboard",
            "schema_version": "1.0",
            "artifact_path": ".dce/dashboard.json",
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
                "sections[].section_summary.simulation.findings_count",
                "sections[].section_summary.simulation.finding_codes",
                "sections[].section_summary.simulation.applicable_providers",
                "sections[].section_summary.simulation.advisory_provider",
                "sections[].section_summary.simulation.provider_execution_state",
                "sections[].section_summary.simulation.provider_execution_summary",
                "sections[].section_summary.simulation.provider_execution_target",
                "sections[].section_summary.simulation.provider_selection_source",
                "sections[].section_summary.simulation.provider_resolution",
                "sections[].section_summary.simulation.reason_code",
                "sections[].section_summary.simulation.reason_summary",
                "sections[].section_summary.simulation.selected_provider",
                "sections[].section_summary.simulation.simulation_provider",
                "sections[].section_summary.simulation.simulation_stage_applicable",
                "sections[].section_summary.simulation.simulation_status",
                "sections[].section_summary.simulation.simulation_triggered",
                "sections[].section_summary.simulation.trigger_reason_codes",
                "sections[].section_summary.simulation.trigger_reason_summary",
                "summary.approval_status_counts",
                "summary.current_stage_counts",
                "summary.review_status_counts",
                "summary.stage_status_counts",
                "summary.total_sections",
            ],
            "contract_stability": "supported",
            "consumer_scopes": ["ui", "reporting"],
            "export_scope": "external",
        },
        {
            "artifact_type": "workspace_index",
            "schema_version": "1.0",
            "artifact_path": ".dce/workspace_index.json",
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
                "sections[].section_summary.simulation.findings_count",
                "sections[].section_summary.simulation.finding_codes",
                "sections[].section_summary.simulation.applicable_providers",
                "sections[].section_summary.simulation.advisory_provider",
                "sections[].section_summary.simulation.provider_execution_state",
                "sections[].section_summary.simulation.provider_execution_summary",
                "sections[].section_summary.simulation.provider_execution_target",
                "sections[].section_summary.simulation.provider_selection_source",
                "sections[].section_summary.simulation.provider_resolution",
                "sections[].section_summary.simulation.reason_code",
                "sections[].section_summary.simulation.reason_summary",
                "sections[].section_summary.simulation.selected_provider",
                "sections[].section_summary.simulation.simulation_provider",
                "sections[].section_summary.simulation.simulation_stage_applicable",
                "sections[].section_summary.simulation.simulation_status",
                "sections[].section_summary.simulation.simulation_triggered",
                "sections[].section_summary.simulation.trigger_reason_codes",
                "sections[].section_summary.simulation.trigger_reason_summary",
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
            "contract_stability": "supported",
            "consumer_scopes": ["sdk", "reporting"],
            "export_scope": "external",
        },
        {
            "artifact_type": "review_index",
            "schema_version": "1.0",
            "artifact_path": ".dce/reviews/index.json",
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
                "sections[].section_summary.simulation.findings_count",
                "sections[].section_summary.simulation.finding_codes",
                "sections[].section_summary.simulation.applicable_providers",
                "sections[].section_summary.simulation.advisory_provider",
                "sections[].section_summary.simulation.provider_execution_state",
                "sections[].section_summary.simulation.provider_execution_summary",
                "sections[].section_summary.simulation.provider_execution_target",
                "sections[].section_summary.simulation.provider_selection_source",
                "sections[].section_summary.simulation.provider_resolution",
                "sections[].section_summary.simulation.reason_code",
                "sections[].section_summary.simulation.reason_summary",
                "sections[].section_summary.simulation.selected_provider",
                "sections[].section_summary.simulation.simulation_provider",
                "sections[].section_summary.simulation.simulation_stage_applicable",
                "sections[].section_summary.simulation.simulation_status",
                "sections[].section_summary.simulation.simulation_triggered",
                "sections[].section_summary.simulation.trigger_reason_codes",
                "sections[].section_summary.simulation.trigger_reason_summary",
                "sections[].navigation_links",
                "summary.sections_with_approval",
                "summary.sections_with_execution",
                "summary.sections_with_outputs",
                "summary.sections_with_review",
                "summary.total_sections_seen",
            ],
            "contract_stability": "supported",
            "consumer_scopes": ["audit", "reporting"],
            "export_scope": "external",
        },
        {
            "artifact_type": "lifecycle_trace",
            "schema_version": "1.0",
            "artifact_path": ".dce/lifecycle_trace.json",
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
                "sections[].section_summary.simulation.findings_count",
                "sections[].section_summary.simulation.finding_codes",
                "sections[].section_summary.simulation.applicable_providers",
                "sections[].section_summary.simulation.advisory_provider",
                "sections[].section_summary.simulation.provider_execution_state",
                "sections[].section_summary.simulation.provider_execution_summary",
                "sections[].section_summary.simulation.provider_execution_target",
                "sections[].section_summary.simulation.provider_selection_source",
                "sections[].section_summary.simulation.provider_resolution",
                "sections[].section_summary.simulation.reason_code",
                "sections[].section_summary.simulation.reason_summary",
                "sections[].section_summary.simulation.selected_provider",
                "sections[].section_summary.simulation.simulation_provider",
                "sections[].section_summary.simulation.simulation_stage_applicable",
                "sections[].section_summary.simulation.simulation_status",
                "sections[].section_summary.simulation.simulation_triggered",
                "sections[].section_summary.simulation.trigger_reason_codes",
                "sections[].section_summary.simulation.trigger_reason_summary",
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
            "contract_stability": "supported",
            "consumer_scopes": ["audit", "reporting"],
            "export_scope": "external",
        },
        {
            "artifact_type": "artifact_manifest",
            "schema_version": "1.0",
            "artifact_path": ".dce/artifact_manifest.json",
            "supported_fields": [
                "artifacts[].artifact_path",
                "artifacts[].artifact_type",
                "artifacts[].schema_version",
                "artifacts[].scope",
                "artifacts[].section_id",
            ],
            "contract_stability": "supported",
            "consumer_scopes": ["sdk", "reporting"],
            "export_scope": "external",
        },
        {
            "artifact_type": "workspace_summary",
            "schema_version": "1.0",
            "artifact_path": ".dce/workspace_summary.json",
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
                "sections[].section_summary.simulation.findings_count",
                "sections[].section_summary.simulation.finding_codes",
                "sections[].section_summary.simulation.applicable_providers",
                "sections[].section_summary.simulation.advisory_provider",
                "sections[].section_summary.simulation.provider_execution_state",
                "sections[].section_summary.simulation.provider_execution_summary",
                "sections[].section_summary.simulation.provider_execution_target",
                "sections[].section_summary.simulation.provider_selection_source",
                "sections[].section_summary.simulation.provider_resolution",
                "sections[].section_summary.simulation.reason_code",
                "sections[].section_summary.simulation.reason_summary",
                "sections[].section_summary.simulation.selected_provider",
                "sections[].section_summary.simulation.simulation_provider",
                "sections[].section_summary.simulation.simulation_stage_applicable",
                "sections[].section_summary.simulation.simulation_status",
                "sections[].section_summary.simulation.simulation_triggered",
                "sections[].section_summary.simulation.trigger_reason_codes",
                "sections[].section_summary.simulation.trigger_reason_summary",
            ],
            "contract_stability": "supported",
            "consumer_scopes": ["reporting", "sdk"],
            "export_scope": "external",
        },
    ]


def _consumer_contract_payload(project_root: Path) -> dict:
    return json.loads((project_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8"))


def _export_contract_payload(project_root: Path) -> dict:
    return json.loads((project_root / ".dce" / "export_contract.json").read_text(encoding="utf-8"))


def _expected_consumer_contract_reference(payload: dict) -> str:
    lines = [
        "# DGCE Consumer Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {payload['schema_version']}",
        f"- generated_by: {payload['generated_by']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    for artifact in payload["supported_artifacts"]:
        lines.extend(
            [
                f"### {artifact['artifact_type']}",
                f"- path: {artifact['artifact_path']}",
                f"- schema_version: {artifact['schema_version']}",
                f"- contract_stability: {artifact['contract_stability']}",
            ]
        )
        if artifact.get("consumer_scopes"):
            lines.append(f"- consumer_scopes: {', '.join(artifact['consumer_scopes'])}")
        lines.extend(
            [
                "",
                "#### Supported Fields",
            ]
        )
        lines.extend(f"- {field_name}" for field_name in artifact["supported_fields"])
        lines.append("")
    return "\n".join(lines)


def _consumer_contract_reference_text(project_root: Path) -> str:
    return (project_root / ".dce" / "consumer_contract_reference.md").read_text(encoding="utf-8")


def _expected_export_contract_reference(payload: dict) -> str:
    lines = [
        "# DGCE Export Contract Reference",
        "",
        "## Metadata",
        f"- schema_version: {payload['schema_version']}",
        f"- generated_by: {payload['generated_by']}",
        f"- artifact_type: {payload['artifact_type']}",
        "",
        "## Supported Artifacts",
        "",
    ]
    for artifact in payload["supported_artifacts"]:
        lines.extend(
            [
                f"### {artifact['artifact_type']}",
                f"- artifact_path: {artifact['artifact_path']}",
                f"- schema_version: {artifact['schema_version']}",
                f"- contract_stability: {artifact['contract_stability']}",
                f"- export_scope: {artifact['export_scope']}",
            ]
        )
        if artifact.get("consumer_scopes"):
            lines.append(f"- consumer_scopes: {', '.join(artifact['consumer_scopes'])}")
        lines.extend(
            [
                "",
                "#### Exported Fields",
            ]
        )
        lines.extend(f"- {field_name}" for field_name in artifact["export_fields"])
        lines.append("")
    return "\n".join(lines)


def _export_contract_reference_text(project_root: Path) -> str:
    return (project_root / ".dce" / "export_contract_reference.md").read_text(encoding="utf-8")


def _assert_dashboard_links_resolve_to_manifest(project_root: Path) -> None:
    manifest_by_path = _artifact_manifest_by_path(project_root)
    dashboard = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))
    for section in dashboard["sections"]:
        for artifact_path in section["navigation_links"].values():
            if artifact_path is None:
                continue
            assert artifact_path in manifest_by_path


def _assert_consumer_contract_aligns_with_manifest(project_root: Path) -> None:
    manifest_by_path = _artifact_manifest_by_path(project_root)
    consumer_contract = _consumer_contract_payload(project_root)
    assert consumer_contract["supported_artifacts"] == _expected_consumer_contract_supported_artifacts()
    assert [entry["artifact_path"] for entry in consumer_contract["supported_artifacts"]] == [
        ".dce/dashboard.json",
        ".dce/workspace_index.json",
        ".dce/reviews/index.json",
        ".dce/lifecycle_trace.json",
        ".dce/artifact_manifest.json",
        ".dce/workspace_summary.json",
    ]
    for entry in consumer_contract["supported_artifacts"]:
        assert entry["artifact_path"] in manifest_by_path
        manifest_entry = manifest_by_path[entry["artifact_path"]]
        assert manifest_entry["artifact_type"] == entry["artifact_type"]
        assert manifest_entry["schema_version"] == entry["schema_version"]
        assert manifest_entry["scope"] == "workspace"
        assert manifest_entry["section_id"] is None
        assert entry["export_scope"] == "external"
        assert entry.get("export_fields") is None or entry.get("export_fields") == entry["supported_fields"]


def _assert_consumer_contract_has_no_cross_section_leakage(project_root: Path, *section_ids: str) -> None:
    contract_text = (project_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8")
    for section_id in section_ids:
        assert section_id not in contract_text


def _assert_reference_aligns_with_contract(project_root: Path) -> None:
    consumer_contract = _consumer_contract_payload(project_root)
    reference_text = _consumer_contract_reference_text(project_root)
    assert reference_text == _expected_consumer_contract_reference(consumer_contract)
    assert [line.removeprefix("### ") for line in reference_text.splitlines() if line.startswith("### ")] == [
        entry["artifact_type"] for entry in consumer_contract["supported_artifacts"]
    ]
    assert [line.removeprefix("- path: ") for line in reference_text.splitlines() if line.startswith("- path: ")] == [
        entry["artifact_path"] for entry in consumer_contract["supported_artifacts"]
    ]
    assert [line.removeprefix("- schema_version: ") for line in reference_text.splitlines() if line.startswith("- schema_version: ")][1:] == [
        entry["schema_version"] for entry in consumer_contract["supported_artifacts"]
    ]


def _assert_exportable_contract_is_deterministic(project_root: Path) -> None:
    consumer_contract = _consumer_contract_payload(project_root)
    exportable_contract = dgce_decompose._get_exportable_contract(consumer_contract)
    assert exportable_contract["artifact_type"] == consumer_contract["artifact_type"]
    assert exportable_contract["generated_by"] == consumer_contract["generated_by"]
    assert exportable_contract["schema_version"] == consumer_contract["schema_version"]
    assert [entry["artifact_path"] for entry in exportable_contract["supported_artifacts"]] == [
        entry["artifact_path"]
        for entry in consumer_contract["supported_artifacts"]
        if entry["export_scope"] == "external"
    ]
    for entry in exportable_contract["supported_artifacts"]:
        assert entry["export_scope"] == "external"
        assert entry["export_fields"] == entry.get("export_fields", entry["supported_fields"])


def _assert_export_contract_aligns(project_root: Path) -> None:
    artifact_manifest = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    consumer_contract = _consumer_contract_payload(project_root)
    consumer_contract_reference = _consumer_contract_reference_text(project_root)
    export_contract = _export_contract_payload(project_root)
    manifest_by_path = _artifact_manifest_by_path(project_root)
    assert export_contract == {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": dgce_decompose._get_exportable_contract(consumer_contract)["supported_artifacts"],
    }
    for entry in export_contract["supported_artifacts"]:
        assert entry["export_scope"] == "external"
        assert isinstance(entry["export_fields"], list) and entry["export_fields"]
        assert entry["artifact_path"] in manifest_by_path
        manifest_entry = manifest_by_path[entry["artifact_path"]]
        assert manifest_entry["artifact_type"] == entry["artifact_type"]
        assert manifest_entry["schema_version"] == entry["schema_version"]
        assert manifest_entry["scope"] == "workspace"
        assert manifest_entry["section_id"] is None
    dgce_decompose._assert_export_contract_fully_converged(
        artifact_manifest,
        consumer_contract,
        consumer_contract_reference,
        export_contract,
    )


def _assert_export_reference_aligns(project_root: Path) -> None:
    export_contract = _export_contract_payload(project_root)
    reference_text = _export_contract_reference_text(project_root)
    assert reference_text == _expected_export_contract_reference(export_contract)
    assert [line.removeprefix("### ") for line in reference_text.splitlines() if line.startswith("### ")] == [
        entry["artifact_type"] for entry in export_contract["supported_artifacts"]
    ]
    assert [line.removeprefix("- artifact_path: ") for line in reference_text.splitlines() if line.startswith("- artifact_path: ")] == [
        entry["artifact_path"] for entry in export_contract["supported_artifacts"]
    ]
    assert [line.removeprefix("- schema_version: ") for line in reference_text.splitlines() if line.startswith("- schema_version: ")][1:] == [
        entry["schema_version"] for entry in export_contract["supported_artifacts"]
    ]
    assert all("- export_scope: internal" not in line for line in reference_text.splitlines())
    dgce_decompose._assert_export_reference_matches_export_contract(export_contract, reference_text)


def _assert_workspace_index_links_resolve_to_manifest(project_root: Path) -> None:
    manifest_by_path = _artifact_manifest_by_path(project_root)
    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    assert workspace_index["artifact_paths"]["lifecycle_trace_path"] in manifest_by_path
    assert workspace_index["artifact_paths"]["review_index_path"] in manifest_by_path
    assert workspace_index["artifact_paths"]["workspace_summary_path"] in manifest_by_path
    for section in workspace_index["sections"]:
        assert section["lifecycle_trace_path"] in manifest_by_path
        for key in ("execution_path", "output_path"):
            artifact_path = section[key]
            if artifact_path is not None:
                assert artifact_path in manifest_by_path
        for link in section["artifact_links"]:
            assert link["path"] in manifest_by_path


def _assert_review_and_trace_links_resolve_to_manifest(project_root: Path) -> None:
    manifest_by_path = _artifact_manifest_by_path(project_root)
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    lifecycle_trace = json.loads((project_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"))
    assert ".dce/reviews/index.json" in manifest_by_path
    assert ".dce/lifecycle_trace.json" in manifest_by_path
    for section in review_index["sections"]:
        assert section["lifecycle_trace_path"] in manifest_by_path
        for key in ("execution_path", "output_path"):
            artifact_path = section[key]
            if artifact_path is not None:
                assert artifact_path in manifest_by_path
        for link in section["navigation_links"]:
            artifact_path = link["path"]
            if artifact_path is not None:
                assert artifact_path in manifest_by_path
    for section in lifecycle_trace["sections"]:
        for trace_entry in section["trace_entries"]:
            artifact_path = trace_entry["artifact_path"]
            if artifact_path is not None:
                assert artifact_path in manifest_by_path
            for linkage in trace_entry["linkage"]:
                ref_path = linkage["ref_path"]
                if ref_path is not None and linkage["ref_name"] != "input_path":
                    assert ref_path in manifest_by_path


def _assert_cross_artifact_section_consistency(project_root: Path, section_id: str) -> None:
    artifacts = _artifact_section_map(project_root)
    review_entry = artifacts["review_index"][section_id]
    workspace_summary_entry = artifacts["workspace_summary"][section_id]
    lifecycle_entry = artifacts["lifecycle_trace"][section_id]
    workspace_index_entry = artifacts["workspace_index"][section_id]
    dashboard_entry = artifacts["dashboard"][section_id]
    expected_summary = review_entry["section_summary"]

    assert workspace_summary_entry["section_summary"] == expected_summary
    assert lifecycle_entry["section_summary"] == expected_summary
    assert workspace_index_entry["section_summary"] == expected_summary
    assert dashboard_entry["section_summary"] == expected_summary
    assert lifecycle_entry["trace_summary"]["latest_stage"] == expected_summary["latest_stage"]
    assert lifecycle_entry["trace_summary"]["latest_stage_status"] == expected_summary["latest_stage_status"]
    assert lifecycle_entry["trace_summary"]["latest_decision"] == expected_summary["latest_decision"]
    assert lifecycle_entry["trace_summary"]["decision_source"] == expected_summary["decision_source"]
    assert workspace_summary_entry["latest_stage"] == expected_summary["latest_stage"]
    assert workspace_summary_entry["latest_stage_status"] == expected_summary["latest_stage_status"]
    assert workspace_summary_entry["latest_decision"] == expected_summary["latest_decision"]
    assert workspace_summary_entry["decision_source"] == expected_summary["decision_source"]
    assert workspace_index_entry["latest_stage"] == expected_summary["latest_stage"]
    assert workspace_index_entry["latest_stage_status"] == expected_summary["latest_stage_status"]
    assert workspace_index_entry["latest_decision"] == expected_summary["latest_decision"]
    assert workspace_index_entry["decision_source"] == expected_summary["decision_source"]
    assert dashboard_entry["current_stage"] == expected_summary["latest_stage"]
    assert dashboard_entry["stage_status"] == expected_summary["latest_stage_status"]
    assert dashboard_entry["latest_decision"] == expected_summary["latest_decision"]
    assert dashboard_entry["decision_source"] == expected_summary["decision_source"]
    assert dashboard_entry["approval_status"] == expected_summary["approval_status"]
    assert dashboard_entry["review_status"] == expected_summary["review_status"]
    manifest_by_path = _artifact_manifest_by_path(project_root)
    assert dashboard_entry["navigation_links"]["lifecycle_trace"] == workspace_index_entry["lifecycle_trace_path"]
    assert review_entry["lifecycle_trace_path"] == workspace_index_entry["lifecycle_trace_path"]
    assert dashboard_entry["navigation_links"]["lifecycle_trace"] in manifest_by_path
    for key, link_role in (("execution_path", "execution"), ("output_path", "outputs")):
        expected_path = workspace_index_entry[key]
        assert review_entry[key] == expected_path
        assert dashboard_entry["navigation_links"][link_role] == expected_path
        if expected_path is not None:
            assert expected_path in manifest_by_path


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


def test_scan_workspace_inventory_excludes_environment_and_junk_paths():
    project_root = _workspace_dir("dgce_incremental_scan_excludes")
    _write_text(project_root / ".venv" / "Lib" / "site-packages" / "pip" / "__init__.py")
    _write_text(project_root / ".git" / "config")
    _write_text(project_root / ".dce" / "input" / "mission-board.json")
    _write_text(project_root / "__pycache__" / "module.pyc")
    _write_text(project_root / "node_modules" / "pkg" / "index.js")
    _write_text(project_root / "build" / "artifact.txt")
    _write_text(project_root / "dist" / "bundle.js")
    _write_text(project_root / "Saved" / "autosave.txt")
    _write_text(project_root / "Intermediate" / "temp.txt")
    _write_text(project_root / "Binaries" / "tool.bin")
    _write_text(project_root / "DerivedDataCache" / "cache.txt")
    _write_text(project_root / "logs" / "run.log")
    _write_text(project_root / "src" / "keep.py")
    _write_text(project_root / "docs" / "readme.md")

    inventory = scan_workspace_inventory(project_root)

    assert inventory == [
        {"ext": ".md", "path": "docs/readme.md", "size": 1},
        {"ext": ".py", "path": "src/keep.py", "size": 1},
    ]


def test_scan_workspace_inventory_is_deterministic_and_empty_safe():
    empty_root = _workspace_dir("dgce_incremental_empty")
    assert scan_workspace_inventory(empty_root) == []

    project_root = _workspace_dir("dgce_incremental_order")
    _write_text(project_root / "zeta.py")
    _write_text(project_root / "alpha.txt")
    _write_text(project_root / "nested" / "beta.py")

    first = scan_workspace_inventory(project_root)
    second = scan_workspace_inventory(project_root)

    assert first == second
    assert [entry["path"] for entry in first] == ["alpha.txt", "nested/beta.py", "zeta.py"]


def test_scan_workspace_file_paths_returns_normalized_sorted_relative_paths_only():
    project_root = _workspace_dir("dgce_incremental_path_only_inventory")
    _write_text(project_root / ".dce" / "state" / "ignore.json")
    _write_text(project_root / "node_modules" / "pkg" / "index.js")
    _write_text(project_root / "zeta.py")
    _write_text(project_root / "nested" / "beta.py")
    _write_text(project_root / "alpha.txt")

    inventory = scan_workspace_file_paths(project_root)

    assert inventory == ["alpha.txt", "nested/beta.py", "zeta.py"]


def test_classify_incremental_targets_marks_missing_as_create_and_present_as_modify():
    changes = classify_incremental_targets(
        ["api/missionboardservice.py", "mission_board/service.py"],
        ["mission_board/service.py", "docs/readme.md"],
    )

    assert changes == [
        {
            "action": "create",
            "path": "api/missionboardservice.py",
            "reason": "target_missing_from_workspace",
        },
        {
            "action": "modify",
            "path": "mission_board/service.py",
            "reason": "target_present_in_workspace",
        },
    ]


def test_build_incremental_change_plan_keeps_non_target_files_out_of_changes():
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
        ],
    )

    artifact = build_incremental_change_plan(
        "mission-board",
        file_plan,
        ["api/missionboardservice.py", "docs/readme.md", "src/extra.py"],
    )

    assert artifact == {
        "section_id": "mission-board",
        "mode": "incremental_v1",
        "summary": {
            "create_count": 0,
            "modify_count": 1,
            "ignore_count": 2,
        },
        "changes": [
            {
                "action": "modify",
                "path": "api/missionboardservice.py",
                "reason": "target_present_in_workspace",
            }
        ],
        "ignored_existing_files": ["docs/readme.md", "src/extra.py"],
    }


def test_build_change_plan_classifies_create_modify_and_ignore():
    expected = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
            {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
        ],
    )
    inventory = [
        {"path": "docs/readme.md", "ext": ".md", "size": 4},
        {"path": "mission_board/service.py", "ext": ".py", "size": 10},
    ]

    change_plan = build_change_plan("mission-board", expected, inventory)

    assert change_plan == [
        {
            "action": "create",
            "path": "api/missionboardservice.py",
            "reason": "expected_target_missing",
            "section_id": "mission-board",
        },
        {
            "action": "ignore",
            "path": "docs/readme.md",
            "reason": "not_in_expected_targets",
            "section_id": "mission-board",
        },
        {
            "action": "modify",
            "path": "mission_board/service.py",
            "reason": "expected_target_exists",
            "section_id": "mission-board",
        },
    ]


def test_build_change_plan_classifies_existing_target_under_project_root_as_modify():
    project_root = _workspace_dir("dgce_incremental_change_plan_existing_under_root")
    _write_text(project_root / "api" / "missionboardservice.py", "existing")
    expected = FilePlan(
        project_name="DGCE",
        files=[{"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"}],
    )

    change_plan = build_change_plan("mission-board", expected, [], project_root=project_root)

    assert change_plan == [
        {
            "action": "modify",
            "path": "api/missionboardservice.py",
            "reason": "expected_target_exists",
            "section_id": "mission-board",
        }
    ]


def test_api_surface_expected_targets_ground_against_host_repo_files():
    repo_root = _workspace_dir("dgce_incremental_expected_targets_host_repo")
    project_root = repo_root / "defiant-sky"
    _write_text(project_root / ".dce" / "input" / "api-surface.json", "{}")
    _write_text(repo_root / "aether" / "dgce" / "decompose.py", "existing-decompose")
    _write_text(repo_root / "apps" / "aether_api" / "routers" / "dgce.py", "existing-router")
    _write_text(repo_root / "dce.py", "existing-cli")

    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "aether/dgce/decompose.py", "purpose": "Orchestration", "source": "expected_targets"},
            {"path": "apps/aether_api/routers/dgce.py", "purpose": "Router", "source": "expected_targets"},
            {"path": "dce.py", "purpose": "CLI", "source": "expected_targets"},
        ],
    )

    change_plan = build_incremental_change_plan(
        "api-surface",
        file_plan,
        scan_workspace_file_paths(project_root),
        project_root=project_root,
    )

    assert [entry["action"] for entry in change_plan["changes"]] == ["modify", "modify", "modify"]
    assert [entry["path"] for entry in change_plan["changes"]] == [
        "aether/dgce/decompose.py",
        "apps/aether_api/routers/dgce.py",
        "dce.py",
    ]


def test_classify_section_targets_backfills_current_file_plan_paths_instead_of_ignoring_them():
    repo_root = _workspace_dir("dgce_incremental_classify_section_targets_backfill")
    project_root = repo_root / "defiant-sky"
    _write_text(project_root / ".dce" / "input" / "system-breakdown.json", "{}")
    _write_text(repo_root / "aether" / "dgce" / "decompose.py", "existing-decompose")
    _write_text(repo_root / "dce.py", "existing-cli")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "aether/dgce/decompose.py", "purpose": "Orchestration", "source": "expected_targets"},
            {"path": "dce.py", "purpose": "CLI", "source": "expected_targets"},
        ],
    )

    actions = classify_section_targets(file_plan, [], project_root)

    assert actions == {
        "aether/dgce/decompose.py": "modify",
        "dce.py": "modify",
    }


def test_build_change_plan_rejects_unsafe_paths():
    try:
        build_change_plan(
            "mission-board",
            FilePlan(
                project_name="DGCE",
                files=[{"path": "../escape.py", "purpose": "Escape", "source": "system_breakdown"}],
            ),
            [],
        )
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_filter_file_plan_for_controlled_write_skips_modify_and_ignore():
    project_root = _workspace_dir("dgce_incremental_controlled_filter")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
            {"path": "docs/readme.md", "purpose": "Docs", "source": "system_summary"},
            {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
        ],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/missionboardservice.py", "action": "create", "reason": "expected_target_missing"},
        {"section_id": "mission-board", "path": "docs/readme.md", "action": "ignore", "reason": "not_in_expected_targets"},
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "expected_target_exists"},
    ]

    filtered = filter_file_plan_for_controlled_write(file_plan, change_plan, project_root)

    assert filtered == FilePlan(
        project_name="DGCE",
        files=[{"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"}],
    )


def test_filter_file_plan_for_controlled_write_allows_modify_when_enabled():
    project_root = _workspace_dir("dgce_incremental_controlled_filter_modify_enabled")
    _write_text(project_root / "mission_board" / "service.py", "existing")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
            {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
        ],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/missionboardservice.py", "action": "create", "reason": "expected_target_missing"},
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "expected_target_exists"},
    ]

    filtered = filter_file_plan_for_controlled_write(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths={"mission_board/service.py"},
    )

    assert filtered == file_plan


def test_build_write_transparency_records_controlled_write_decisions():
    project_root = _workspace_dir("dgce_incremental_write_transparency")
    _write_text(project_root / "api" / "existing.py", "existing")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/create.py", "purpose": "Create", "source": "api_surface"},
            {"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"},
            {"path": "api/ignore.py", "purpose": "Ignore", "source": "api_surface"},
            {"path": "api/existing.py", "purpose": "Existing", "source": "api_surface"},
        ],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/create.py", "action": "create", "reason": "expected_target_missing"},
        {"section_id": "mission-board", "path": "api/modify.py", "action": "modify", "reason": "expected_target_exists"},
        {"section_id": "mission-board", "path": "api/ignore.py", "action": "ignore", "reason": "not_in_expected_targets"},
    ]

    write_plan, transparency = build_write_transparency(file_plan, change_plan, project_root)

    assert write_plan == FilePlan(
        project_name="DGCE",
        files=[{"path": "api/create.py", "purpose": "Create", "source": "api_surface"}],
    )
    assert transparency == {
        "write_decisions": [
            {"path": "api/create.py", "decision": "written", "reason": "create"},
            {"path": "api/modify.py", "decision": "skipped", "reason": "ownership"},
            {"path": "api/ignore.py", "decision": "skipped", "reason": "ignore"},
            {"path": "api/existing.py", "decision": "skipped", "reason": "ownership"},
        ],
        "write_summary": {
            "written_count": 1,
            "modify_written_count": 0,
            "diff_visible_count": 0,
            "skipped_modify_count": 0,
            "skipped_ignore_count": 1,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 2,
            "skipped_exists_fallback_count": 0,
            "before_bytes_total": 0,
            "after_bytes_total": 0,
            "changed_lines_estimate_total": 0,
            "bytes_written_total": 0,
        },
    }


def test_build_write_transparency_records_safe_modify_writes_when_enabled():
    project_root = _workspace_dir("dgce_incremental_write_transparency_modify_enabled")
    _write_text(project_root / "api" / "modify.py", "existing")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/create.py", "purpose": "Create", "source": "api_surface"},
            {"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"},
            {"path": "api/ignore.py", "purpose": "Ignore", "source": "api_surface"},
        ],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/create.py", "action": "create", "reason": "expected_target_missing"},
        {"section_id": "mission-board", "path": "api/modify.py", "action": "modify", "reason": "expected_target_exists"},
        {"section_id": "mission-board", "path": "api/ignore.py", "action": "ignore", "reason": "not_in_expected_targets"},
    ]

    write_plan, transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths={"api/modify.py"},
    )

    assert write_plan == FilePlan(
        project_name="DGCE",
        files=[
            {"path": "api/create.py", "purpose": "Create", "source": "api_surface"},
            {"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"},
        ],
    )
    modify_entry = next(entry for entry in transparency["write_decisions"] if entry["path"] == "api/modify.py")
    modify_after_bytes = render_file_entry_bytes({"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"})
    assert transparency["write_decisions"][0] == {"path": "api/create.py", "decision": "written", "reason": "create"}
    assert transparency["write_decisions"][2] == {"path": "api/ignore.py", "decision": "skipped", "reason": "ignore"}
    assert modify_entry["decision"] == "written"
    assert modify_entry["reason"] == "modify"
    assert modify_entry["diff_visibility"] == {
        "before_bytes": len(b"existing"),
        "after_bytes": len(modify_after_bytes),
        "changed_lines_estimate": _changed_lines_estimate(b"existing", modify_after_bytes),
    }
    assert transparency["write_summary"] == {
        "written_count": 2,
        "modify_written_count": 1,
        "diff_visible_count": 1,
        "skipped_modify_count": 0,
        "skipped_ignore_count": 1,
        "skipped_identical_count": 0,
        "skipped_ownership_count": 0,
        "skipped_exists_fallback_count": 0,
        "before_bytes_total": len(b"existing"),
        "after_bytes_total": len(modify_after_bytes),
        "changed_lines_estimate_total": _changed_lines_estimate(b"existing", modify_after_bytes),
        "bytes_written_total": 0,
    }


def test_build_write_transparency_blocks_unowned_modify_when_enabled():
    project_root = _workspace_dir("dgce_incremental_write_transparency_unowned_modify")
    _write_text(project_root / "api" / "modify.py", "existing")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"}],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/modify.py", "action": "modify", "reason": "expected_target_exists"},
    ]

    write_plan, transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths=set(),
    )

    assert write_plan == FilePlan(project_name="DGCE", files=[])
    assert transparency == {
        "write_decisions": [
            {"path": "api/modify.py", "decision": "skipped", "reason": "ownership"},
        ],
        "write_summary": {
            "written_count": 0,
            "modify_written_count": 0,
            "diff_visible_count": 0,
            "skipped_modify_count": 0,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 1,
            "skipped_exists_fallback_count": 0,
            "before_bytes_total": 0,
            "after_bytes_total": 0,
            "changed_lines_estimate_total": 0,
            "bytes_written_total": 0,
        },
    }
    assert overwrite_paths_from_transparency(transparency) == set()


def test_build_write_transparency_skips_identical_modify_when_enabled():
    project_root = _workspace_dir("dgce_incremental_write_transparency_identical")
    identical_entry = {
        "path": "api/modify.py",
        "purpose": "Modify",
        "source": "api_surface",
    }
    identical_path = project_root / "api" / "modify.py"
    identical_path.parent.mkdir(parents=True, exist_ok=True)
    identical_path.write_bytes(render_file_entry_bytes(identical_entry))
    file_plan = FilePlan(
        project_name="DGCE",
        files=[
            identical_entry,
            {"path": "api/create.py", "purpose": "Create", "source": "api_surface"},
        ],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/create.py", "action": "create", "reason": "expected_target_missing"},
        {"section_id": "mission-board", "path": "api/modify.py", "action": "modify", "reason": "expected_target_exists"},
    ]

    write_plan, transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths={"api/modify.py"},
    )

    assert write_plan == FilePlan(
        project_name="DGCE",
        files=[{"path": "api/create.py", "purpose": "Create", "source": "api_surface"}],
    )
    assert transparency == {
        "write_decisions": [
            {"path": "api/modify.py", "decision": "skipped", "reason": "identical"},
            {"path": "api/create.py", "decision": "written", "reason": "create"},
        ],
        "write_summary": {
            "written_count": 1,
            "modify_written_count": 0,
            "diff_visible_count": 0,
            "skipped_modify_count": 0,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 1,
            "skipped_ownership_count": 0,
            "skipped_exists_fallback_count": 0,
            "before_bytes_total": 0,
            "after_bytes_total": 0,
            "changed_lines_estimate_total": 0,
            "bytes_written_total": 0,
        },
    }


def test_should_write_planned_file_applies_v1_controlled_write_rules():
    project_root = _workspace_dir("dgce_incremental_write_decision")
    _write_text(project_root / "api" / "existing.py", "existing")
    actions_by_path = {
        "api/create.py": "create",
        "api/modify.py": "modify",
        "api/ignore.py": "ignore",
    }

    assert should_write_planned_file("api/create.py", actions_by_path, project_root) is True
    assert should_write_planned_file("api/modify.py", actions_by_path, project_root) is False
    assert should_write_planned_file("api/ignore.py", actions_by_path, project_root) is False
    assert should_write_planned_file("api/missing.py", actions_by_path, project_root) is True
    assert should_write_planned_file("api/existing.py", actions_by_path, project_root) is False


def test_should_write_planned_file_allows_modify_when_safe_modify_enabled():
    project_root = _workspace_dir("dgce_incremental_write_decision_modify_enabled")
    _write_text(project_root / "api" / "modify.py", "existing")
    actions_by_path = {"api/modify.py": "modify", "api/ignore.py": "ignore"}

    assert should_write_planned_file(
        "api/modify.py",
        actions_by_path,
        project_root,
        allow_modify_write=True,
    ) is True
    assert should_write_planned_file(
        "api/ignore.py",
        actions_by_path,
        project_root,
        allow_modify_write=True,
    ) is False


def test_run_section_with_workspace_persists_incremental_change_plan(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_workspace")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )
    _write_text(project_root / ".venv" / "Lib" / "site-packages" / "pip" / "__init__.py", "skip")
    _write_text(project_root / "docs" / "notes.md", "keep")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_write_file_plan(file_plan, output_dir, overwrite_paths=None):
        written_files = []
        for entry in file_plan.files:
            path = output_dir / Path(entry["path"])
            _write_text(path, f"generated:{entry['path']}")
            written_files.append(entry["path"])
        return written_files

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.write_file_plan", fake_write_file_plan)

    run_section_with_workspace(_section(), project_root)
    change_plan = json.loads(
        (project_root / ".dce" / "plans" / f"{section_id}.change_plan.json").read_text(encoding="utf-8")
    )

    assert change_plan["section_id"] == section_id
    assert change_plan["expected_targets"] == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert not any(entry["path"].startswith(".dce") for entry in change_plan["workspace_inventory"])
    assert not any(entry["path"].startswith(".venv") for entry in change_plan["workspace_inventory"])
    assert change_plan["changes"] == [
        {
            "action": "create",
            "path": "api/missionboardservice.py",
            "reason": "expected_target_missing",
            "section_id": "mission-board",
        },
        {
            "action": "ignore",
            "path": "docs/notes.md",
            "reason": "not_in_expected_targets",
            "section_id": "mission-board",
        },
        {
            "action": "create",
            "path": "mission_board/models.py",
            "reason": "expected_target_missing",
            "section_id": "mission-board",
        },
        {
            "action": "modify",
            "path": "mission_board/service.py",
            "reason": "expected_target_exists",
            "section_id": "mission-board",
        },
        {
            "action": "create",
            "path": "models/mission.py",
            "reason": "expected_target_missing",
            "section_id": "mission-board",
        },
    ]


def test_run_section_with_workspace_change_plan_uses_current_file_plan_as_source_of_truth(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    repo_root = _workspace_dir("dgce_incremental_current_file_plan_truth")
    project_root = repo_root / "defiant-sky"
    section = DGCESection(
        section_type="system_breakdown",
        title="System Breakdown",
        description="Ground expected targets against the host repo consistently.",
        expected_targets=[
            "aether/dgce/decompose.py",
            "aether/dgce/incremental.py",
            "aether/dgce/file_writer.py",
            "dce.py",
        ],
    )
    stale_outputs_path = project_root / ".dce" / "outputs" / "system-breakdown.json"
    stale_outputs_path.parent.mkdir(parents=True, exist_ok=True)
    stale_outputs_path.write_text(
        json.dumps(
            {
                "section_id": "system-breakdown",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [{"path": "apps/aether_api/routers/dgce.py", "purpose": "stale", "source": "expected_targets"}],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    for target in section.expected_targets:
        _write_text(repo_root / Path(target), f"existing:{target}")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))
    result = run_section_with_workspace(section, project_root)
    change_plan = json.loads(
        (project_root / ".dce" / "plans" / "system-breakdown.change_plan.json").read_text(encoding="utf-8")
    )

    assert sorted(entry["path"] for entry in result.file_plan.files) == sorted(section.expected_targets)
    assert change_plan["expected_targets"] == sorted(section.expected_targets)
    assert all(entry["action"] == "modify" for entry in change_plan["changes"])
    assert all(entry["reason"] == "expected_target_exists" for entry in change_plan["changes"])


def test_run_section_with_workspace_write_stage_only_writes_create_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_write_control")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    change_plan = load_change_plan(project_root / ".dce" / "plans" / f"{section_id}.change_plan.json")

    assert [entry["action"] for entry in change_plan] == ["create", "create", "modify", "create"]
    assert result.run_mode == "create_only"
    assert result.run_outcome_class == "partial_skipped_modify"
    assert result.written_files == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "models/mission.py",
    ]
    assert (project_root / "api" / "missionboardservice.py").exists()
    assert (project_root / "mission_board" / "models.py").exists()
    assert (project_root / "models" / "mission.py").exists()
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing"
    assert result.execution_outcome == {
        "section_id": "mission-board",
        "stage": "WRITE",
        "status": "partial",
        "validation_summary": {
            "ok": True,
            "error": None,
            "missing_keys": [],
        },
        "change_plan_summary": {
            "create_count": 3,
            "modify_count": 1,
            "ignore_count": 0,
        },
        "execution_summary": {
            "written_files_count": 3,
            "skipped_modify_count": 1,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 0,
            "skipped_exists_fallback_count": 0,
        },
    }
    assert result.advisory == {
        "type": "process_adjustment",
        "summary": "Review incremental skip behavior for mission-board",
        "explanation": ["partial_run", "skipped_modify"],
    }
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )
    advisory_index = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    assert outputs_payload["advisory"] == result.advisory
    assert outputs_payload["write_transparency"] == result.write_transparency
    assert ownership_index == result.ownership_index
    assert result.write_transparency["write_summary"] == {
        "written_count": 3,
        "modify_written_count": 0,
        "diff_visible_count": 0,
        "skipped_modify_count": 1,
        "skipped_ignore_count": 0,
        "skipped_identical_count": 0,
        "skipped_ownership_count": 0,
        "skipped_exists_fallback_count": 0,
        "before_bytes_total": 0,
        "after_bytes_total": 0,
        "changed_lines_estimate_total": 0,
        "bytes_written_total": sum(
            entry["bytes_written"] for entry in result.write_transparency["write_decisions"] if entry["decision"] == "written"
        ),
    }
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "modify"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert any(
        entry["path"] == "api/missionboardservice.py"
        and entry["decision"] == "written"
        and entry["reason"] == "create"
        and isinstance(entry["bytes_written"], int)
        for entry in result.write_transparency["write_decisions"]
    )
    assert all("diff_visibility" not in entry for entry in result.write_transparency["write_decisions"])
    assert ownership_index == {
        "files": [
            {
                "path": "api/missionboardservice.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "models/mission.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
        ]
    }
    assert advisory_index == {
        "run_outcome_class": "partial_skipped_modify",
        "run_mode": "create_only",
        "section_id": "mission-board",
        "status": "partial",
        "validation_ok": True,
        "advisory_type": "process_adjustment",
        "advisory_explanation": ["partial_run", "skipped_modify"],
        "written_files_count": 3,
        "skipped_modify_count": 1,
        "skipped_ignore_count": 0,
    }
    assert workspace_summary == {
        **_expected_artifact_metadata("workspace_summary"),
        "total_sections_seen": 1,
        "sections": [
            {
                "section_id": "mission-board",
                "latest_run_mode": "create_only",
                "latest_run_outcome_class": "partial_skipped_modify",
                "latest_status": "partial",
                "latest_validation_ok": True,
                "latest_advisory_type": "process_adjustment",
                "latest_advisory_explanation": ["partial_run", "skipped_modify"],
                "latest_written_files_count": 3,
                "latest_skipped_modify_count": 1,
                "latest_skipped_ignore_count": 0,
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
                "decision_source": None,
                "review_status": None,
                "latest_decision": None,
                "latest_decision_source": None,
                "latest_stage": "outputs",
                "latest_stage_status": "partial_skipped_modify",
                "section_summary": _expected_section_summary(
                    section_id="mission-board",
                    latest_stage="outputs",
                    latest_stage_status="partial_skipped_modify",
                ),
            }
        ],
    }


def test_run_section_with_workspace_safe_modify_enabled_writes_modify_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_safe_modify_enabled")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )
    advisory_index = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert result.run_mode == "safe_modify"
    assert result.run_outcome_class == "success_safe_modify"
    assert outputs_payload["run_mode"] == "safe_modify"
    assert outputs_payload["run_outcome_class"] == "success_safe_modify"
    assert result.written_files == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert result.execution_outcome["status"] == "success"
    assert result.execution_outcome["execution_summary"] == {
        "written_files_count": 4,
        "skipped_modify_count": 0,
        "skipped_ignore_count": 0,
        "skipped_identical_count": 0,
        "skipped_ownership_count": 0,
        "skipped_exists_fallback_count": 0,
    }
    assert result.advisory is None
    modify_entry = next(entry for entry in result.write_transparency["write_decisions"] if entry["path"] == "mission_board/service.py")
    modify_after_bytes = render_file_entry_bytes(
        {
            "path": "mission_board/service.py",
            "purpose": "coordinate mission generation service orchestration",
            "source": "system_breakdown",
        }
    )
    assert any(
        entry["path"] == "mission_board/service.py"
        and entry["decision"] == "written"
        and entry["reason"] == "modify"
        and isinstance(entry["bytes_written"], int)
        for entry in result.write_transparency["write_decisions"]
    )
    assert modify_entry["diff_visibility"] == {
        "before_bytes": len(b"existing"),
        "after_bytes": len(modify_after_bytes),
        "changed_lines_estimate": _changed_lines_estimate(b"existing", modify_after_bytes),
    }
    assert all(
        "diff_visibility" not in entry
        for entry in result.write_transparency["write_decisions"]
        if entry["path"] != "mission_board/service.py"
    )
    assert result.write_transparency["write_summary"] == {
        "written_count": 4,
        "modify_written_count": 1,
        "diff_visible_count": 1,
        "skipped_modify_count": 0,
        "skipped_ignore_count": 0,
        "skipped_identical_count": 0,
        "skipped_ownership_count": 0,
        "skipped_exists_fallback_count": 0,
        "before_bytes_total": len(b"existing"),
        "after_bytes_total": len(modify_after_bytes),
        "changed_lines_estimate_total": _changed_lines_estimate(b"existing", modify_after_bytes),
        "bytes_written_total": sum(
            entry["bytes_written"] for entry in result.write_transparency["write_decisions"] if entry["decision"] == "written"
        ),
    }
    assert outputs_payload["execution_outcome"] == result.execution_outcome
    assert outputs_payload["advisory"] == result.advisory
    assert outputs_payload["write_transparency"] == result.write_transparency
    assert advisory_index["run_mode"] == "safe_modify"
    assert advisory_index["run_outcome_class"] == "success_safe_modify"
    assert workspace_summary["sections"][0]["latest_run_mode"] == "safe_modify"
    assert workspace_summary["sections"][0]["latest_run_outcome_class"] == "success_safe_modify"
    assert ownership_index == result.ownership_index
    assert ownership_index == {
        "files": [
            {
                "path": "api/missionboardservice.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "modify",
            },
            {
                "path": "models/mission.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
        ]
    }


def test_data_model_preview_recommends_safe_modify_in_development_mode(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    monkeypatch.setenv("AETHER_ENVIRONMENT", "development")
    project_root = _workspace_dir("dgce_data_model_dev_preview_safe_modify")
    section = DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE data model.",
        requirements=[
            "Define SectionInput and ExecutionStamp entities",
            "Keep the model deterministic and auditable",
        ],
        constraints=["Keep the model independent of .dce file paths"],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )
    section_id = "data-model"

    for path in section.expected_targets:
        _write_text(project_root / path, "existing-development-content")
    _write_ownership_index(project_root, [{"path": path, "section_id": section_id} for path in section.expected_targets])

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    result = run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    preview = json.loads((project_root / ".dce" / "plans" / f"{section_id}.preview.json").read_text(encoding="utf-8"))

    assert result.run_mode == "incremental_v2_2"
    assert preview["preview_outcome_class"] == "preview_safe_modify_ready"
    assert preview["recommended_mode"] == "safe_modify"
    assert preview["summary"]["total_blocked_modify_disabled"] == 0
    assert all(entry["planned_action"] == "modify" for entry in preview["previews"])
    assert all(entry["preview_decision"] == "write" for entry in preview["previews"])


def test_data_model_execution_allows_modify_in_development_mode_without_safe_modify_flag(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    monkeypatch.setenv("AETHER_ENVIRONMENT", "development")
    project_root = _workspace_dir("dgce_data_model_dev_execute_safe_modify")
    section = DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE data model.",
        requirements=[
            "Define SectionInput and ExecutionStamp entities",
            "Keep the model deterministic and auditable",
        ],
        constraints=["Keep the model independent of .dce file paths"],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )
    section_id = "data-model"

    for path in section.expected_targets:
        _write_text(project_root / path, "existing-development-content")
    _write_ownership_index(project_root, [{"path": path, "section_id": section_id} for path in section.expected_targets])

    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    class FakePlanner:
        def route(self, task, classification):
            structured_content = None
            output = "Summary output"

            if task.task_type == "data_model":
                structured_content = {
                    "modules": [
                        {
                            "name": "DGCEDataModel",
                            "entities": ["SectionInput"],
                            "relationships": ["SectionInput->ExecutionStamp"],
                            "required": [],
                            "identity_keys": [],
                        }
                    ],
                    "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
                    "fields": ["section_id"],
                    "relationships": ["SectionInput->ExecutionStamp"],
                    "validation_rules": ["section_id required"],
                }
                output = json.dumps(structured_content)
            elif task.task_type == "api_surface":
                structured_content = {
                    "interfaces": ["DGCEDataModelService"],
                    "methods": ["describe_model"],
                    "inputs": ["section_id"],
                    "outputs": ["artifact"],
                    "error_cases": ["section_missing"],
                }
                output = json.dumps(structured_content)

            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": output,
                    "reused": False,
                    "execution_metadata": {"structure_valid": True} if structured_content is not None else {},
                    "structured_content": structured_content,
                },
            )()

    result = run_section_with_workspace(section, project_root, router_planner=FakePlanner())
    outputs_payload = json.loads((project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert result.run_mode == "safe_modify"
    assert result.run_outcome_class == "success_safe_modify"
    assert sorted(result.written_files) == sorted(section.expected_targets)
    assert outputs_payload["run_mode"] == "safe_modify"
    assert outputs_payload["run_outcome_class"] == "success_safe_modify"


def test_run_section_with_workspace_safe_modify_enabled_writes_when_diff_visibility_read_fails(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_safe_modify_diff_visibility_read_failure")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    target_path = project_root / "mission_board" / "service.py"
    _write_text(target_path, "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    original_read_bytes = Path.read_bytes

    def flaky_read_bytes(self):
        if self.as_posix().endswith("mission_board/service.py"):
            raise OSError("simulated disappearing file during diff visibility read")
        return original_read_bytes(self)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.incremental._has_identical_existing_content", lambda *args, **kwargs: False)
    monkeypatch.setattr(Path, "read_bytes", flaky_read_bytes)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )

    assert result.run_mode == "safe_modify"
    assert result.run_outcome_class == "success_safe_modify"
    assert outputs_payload["run_mode"] == "safe_modify"
    assert outputs_payload["run_outcome_class"] == "success_safe_modify"
    modify_entry = next(entry for entry in result.write_transparency["write_decisions"] if entry["path"] == "mission_board/service.py")

    assert "mission_board/service.py" in result.written_files
    assert modify_entry["decision"] == "written"
    assert modify_entry["reason"] == "modify"
    assert "diff_visibility" not in modify_entry
    assert isinstance(modify_entry["bytes_written"], int)
    assert result.write_transparency["write_summary"]["modify_written_count"] == 1
    assert result.write_transparency["write_summary"]["diff_visible_count"] == 0
    assert result.write_transparency["write_summary"]["before_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["after_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["changed_lines_estimate_total"] == 0
    assert outputs_payload["write_transparency"] == result.write_transparency


def test_run_section_with_workspace_safe_modify_enabled_skips_identical_modify_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_safe_modify_identical")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    identical_entry = {
        "path": "mission_board/service.py",
        "purpose": "coordinate mission generation service orchestration",
        "source": "system_breakdown",
    }
    identical_path = project_root / "mission_board" / "service.py"
    identical_path.parent.mkdir(parents=True, exist_ok=True)
    identical_path.write_bytes(render_file_entry_bytes(identical_entry))
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert "mission_board/service.py" not in result.written_files
    assert result.execution_outcome["status"] == "success"
    assert result.run_outcome_class == "partial_skipped_identical"
    assert result.execution_outcome["execution_summary"]["skipped_modify_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 1
    assert result.execution_outcome["execution_summary"]["skipped_ownership_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_exists_fallback_count"] == 0
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "identical"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert result.write_transparency["write_summary"]["written_count"] == 3
    assert result.write_transparency["write_summary"]["modify_written_count"] == 0
    assert result.write_transparency["write_summary"]["diff_visible_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_modify_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_ignore_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_identical_count"] == 1
    assert result.write_transparency["write_summary"]["skipped_ownership_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_exists_fallback_count"] == 0
    assert result.write_transparency["write_summary"]["before_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["after_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["changed_lines_estimate_total"] == 0
    assert result.write_transparency["write_summary"]["bytes_written_total"] == sum(
        entry["bytes_written"] for entry in result.write_transparency["write_decisions"] if entry["decision"] == "written"
    )
    assert all("diff_visibility" not in entry for entry in result.write_transparency["write_decisions"])
    assert ownership_index == result.ownership_index
    assert any(
        entry["path"] == "mission_board/service.py"
        and entry["section_id"] == "mission-board"
        and entry["write_reason"] == "create"
        for entry in ownership_index["files"]
    )
    assert outputs_payload["write_transparency"] == result.write_transparency


def test_run_section_with_workspace_safe_modify_enabled_blocks_unowned_modify_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_safe_modify_unowned")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "docs/legacy.md",
                "section_id": "legacy-section",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert "mission_board/service.py" not in result.written_files
    assert result.run_outcome_class == "partial_skipped_ownership"
    assert "api/missionboardservice.py" in result.written_files
    assert "mission_board/models.py" in result.written_files
    assert "models/mission.py" in result.written_files
    assert result.execution_outcome["status"] == "partial"
    assert result.execution_outcome["execution_summary"]["skipped_modify_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_ignore_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_ownership_count"] == 1
    assert result.execution_outcome["execution_summary"]["skipped_exists_fallback_count"] == 0
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert result.write_transparency["write_summary"]["skipped_ownership_count"] == 1
    assert result.write_transparency["write_summary"]["modify_written_count"] == 0
    assert result.write_transparency["write_summary"]["diff_visible_count"] == 0
    assert result.write_transparency["write_summary"]["before_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["after_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["changed_lines_estimate_total"] == 0
    assert ownership_index == result.ownership_index
    assert all(entry["path"] != "mission_board/service.py" for entry in ownership_index["files"])
    assert any(entry["path"] == "docs/legacy.md" for entry in ownership_index["files"])


def test_run_section_with_workspace_preserves_existing_ownership_on_no_write_run(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_preserve_ownership_no_write")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    identical_entry = {
        "path": "mission_board/service.py",
        "purpose": "coordinate mission generation service orchestration",
        "source": "system_breakdown",
    }
    identical_path = project_root / "mission_board" / "service.py"
    identical_path.parent.mkdir(parents=True, exist_ok=True)
    identical_path.write_bytes(render_file_entry_bytes(identical_entry))
    existing_ownership = {
        "files": [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ]
    }
    _write_ownership_index(project_root, existing_ownership["files"])

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "system breakdown" in lowered:
            output = json.dumps(
                {
                    "module_name": "mission_board",
                    "purpose": "coordinate mission generation",
                    "subcomponents": [],
                    "dependencies": [],
                    "implementation_order": [],
                }
            )
        elif "data model" in lowered:
            output = json.dumps(
                {
                    "entities": [],
                    "fields": [],
                    "relationships": [],
                    "validation_rules": [],
                }
            )
        elif "api surface" in lowered:
            output = json.dumps(
                {
                    "interfaces": [],
                    "methods": [],
                    "inputs": {},
                    "outputs": {},
                    "error_cases": {},
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
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "success_safe_modify"
    assert ownership_index == existing_ownership
    assert ownership_index == result.ownership_index


def test_run_section_with_workspace_merges_and_refreshes_ownership_entries(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_merge_refresh_ownership")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "api/missionboardservice.py", "purpose": "API", "source": "api_surface"},
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "docs/legacy.md",
                "section_id": "legacy-section",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/service.py",
                "section_id": "legacy-section",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert ownership_index == result.ownership_index
    assert ownership_index == {
        "files": [
            {
                "path": "api/missionboardservice.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "docs/legacy.md",
                "section_id": "legacy-section",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "modify",
            },
            {
                "path": "models/mission.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
        ]
    }


def test_run_section_with_workspace_skips_ignore_paths_without_collision(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_ignore_collision")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/models.py", "purpose": "Models", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "models.py", "existing-models")
    _write_text(project_root / "api" / "missionboardservice.py", "existing-api")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8")
    )
    advisory_index = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert "mission_board/models.py" not in result.written_files
    assert "api/missionboardservice.py" not in result.written_files
    assert (project_root / "mission_board" / "models.py").read_text(encoding="utf-8") == "existing-models"
    assert (project_root / "api" / "missionboardservice.py").read_text(encoding="utf-8") == "existing-api"
    assert result.execution_outcome["execution_summary"]["skipped_ignore_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_modify_count"] == 1
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_exists_fallback_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_ownership_count"] == 1
    assert result.execution_outcome["status"] == "partial"
    assert outputs_payload["execution_outcome"] == result.execution_outcome
    assert outputs_payload["advisory"] == result.advisory
    assert outputs_payload["write_transparency"] == result.write_transparency
    assert result.advisory == {
        "type": "process_adjustment",
        "summary": "Review incremental skip behavior for mission-board",
        "explanation": ["partial_run", "skipped_modify"],
    }
    assert advisory_index == {
        "run_outcome_class": "partial_skipped_ownership",
        "run_mode": "create_only",
        "section_id": "mission-board",
        "status": "partial",
        "validation_ok": True,
        "advisory_type": "process_adjustment",
        "advisory_explanation": ["partial_run", "skipped_modify"],
        "written_files_count": 2,
        "skipped_modify_count": 1,
        "skipped_ignore_count": 0,
    }
    assert any(
        entry == {"path": "mission_board/models.py", "decision": "skipped", "reason": "modify"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert any(
        entry == {"path": "api/missionboardservice.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert result.write_transparency["write_summary"]["skipped_exists_fallback_count"] == 0
    assert workspace_summary == {
        **_expected_artifact_metadata("workspace_summary"),
        "total_sections_seen": 1,
        "sections": [
                {
                    "section_id": "mission-board",
                    "latest_run_mode": "create_only",
                    "latest_run_outcome_class": "partial_skipped_ownership",
                "latest_status": "partial",
                "latest_validation_ok": True,
                "latest_advisory_type": "process_adjustment",
                "latest_advisory_explanation": ["partial_run", "skipped_modify"],
                "latest_written_files_count": 2,
                "latest_skipped_modify_count": 1,
                "latest_skipped_ignore_count": 0,
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
                "decision_source": None,
                "review_status": None,
                "latest_decision": None,
                "latest_decision_source": None,
                "latest_stage": "outputs",
                "latest_stage_status": "partial_skipped_ownership",
                "section_summary": _expected_section_summary(
                    section_id="mission-board",
                    latest_stage="outputs",
                    latest_stage_status="partial_skipped_ownership",
                ),
            }
        ],
    }


def test_run_section_with_workspace_safe_modify_enabled_still_skips_ignore_paths(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_safe_modify_ignore")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/models.py", "purpose": "Models", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "models.py", "existing-models")
    _write_text(project_root / "api" / "missionboardservice.py", "existing-api")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert result.run_mode == "safe_modify"
    assert result.run_outcome_class == "partial_skipped_ownership"
    assert "mission_board/models.py" in result.written_files
    assert "api/missionboardservice.py" not in result.written_files
    assert result.execution_outcome["status"] == "partial"
    assert result.execution_outcome["execution_summary"]["skipped_modify_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_ignore_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 0
    assert result.execution_outcome["execution_summary"]["skipped_ownership_count"] == 1
    assert result.execution_outcome["execution_summary"]["skipped_exists_fallback_count"] == 0
    assert any(
        entry["path"] == "mission_board/models.py"
        and entry["decision"] == "written"
        and entry["reason"] == "modify"
        and isinstance(entry["bytes_written"], int)
        for entry in result.write_transparency["write_decisions"]
    )
    assert any(
        entry == {"path": "api/missionboardservice.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert result.write_transparency["write_summary"]["skipped_identical_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_ownership_count"] == 1
    assert result.write_transparency["write_summary"]["modify_written_count"] == 1
    assert result.write_transparency["write_summary"]["diff_visible_count"] == 1
    assert result.write_transparency["write_summary"]["before_bytes_total"] == len("existing-models".encode("utf-8"))
    assert result.write_transparency["write_summary"]["after_bytes_total"] > 0
    assert result.write_transparency["write_summary"]["changed_lines_estimate_total"] > 0
    assert ownership_index == result.ownership_index
    assert ownership_index == {
        "files": [
            {
                "path": "mission_board/models.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "modify",
            },
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
            {
                "path": "models/mission.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            },
        ]
    }


def test_run_section_with_workspace_does_not_raise_file_exists_for_modify_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_no_collision")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "mission_board" / "service.py", "existing")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)

    assert result.written_files == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "models/mission.py",
    ]
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing"


def test_run_section_with_workspace_writes_files_missing_from_change_plan(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_missing_change_entry")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {"path": "mission_board/service.py", "purpose": "Service", "source": "system_breakdown"},
                    ],
                },
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

    result = run_section_with_workspace(_section(), project_root)

    assert "api/missionboardservice.py" in result.written_files
    assert "mission_board/models.py" in result.written_files
    assert "models/mission.py" in result.written_files


def test_run_section_with_workspace_records_exists_fallback_transparency(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_exists_fallback")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_text(project_root / "api" / "missionboardservice.py", "existing-api")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_scan_workspace_inventory(project_root_arg):
        return []

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.scan_workspace_inventory", fake_scan_workspace_inventory)

    result = run_section_with_workspace(_section(), project_root, allow_safe_modify=True)
    outputs_payload = json.loads((project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))
    ownership_index = json.loads((project_root / ".dce" / "ownership_index.json").read_text(encoding="utf-8"))

    assert "api/missionboardservice.py" not in result.written_files
    assert any(
        entry == {"path": "api/missionboardservice.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )
    assert result.write_transparency["write_summary"]["written_count"] == 3
    assert result.write_transparency["write_summary"]["modify_written_count"] == 0
    assert result.write_transparency["write_summary"]["diff_visible_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_modify_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_ignore_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_identical_count"] == 0
    assert result.write_transparency["write_summary"]["skipped_ownership_count"] == 1
    assert result.write_transparency["write_summary"]["skipped_exists_fallback_count"] == 0
    assert result.write_transparency["write_summary"]["before_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["after_bytes_total"] == 0
    assert result.write_transparency["write_summary"]["changed_lines_estimate_total"] == 0
    assert result.write_transparency["write_summary"]["bytes_written_total"] == sum(
        entry["bytes_written"] for entry in result.write_transparency["write_decisions"] if entry["decision"] == "written"
    )
    assert outputs_payload["write_transparency"] == result.write_transparency
    assert ownership_index == result.ownership_index
    assert all(entry["path"] != "api/missionboardservice.py" for entry in ownership_index["files"])
    assert result.run_outcome_class == "partial_skipped_ownership"


def test_run_section_with_workspace_reports_success_when_repair_normalizes_validation_gaps(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_validation_outcome")
    planner = RouterPlanner(
        cache=ExactMatchCache(project_root / ".dce" / "validation_outcome_cache.json"),
        artifact_store=ArtifactStore(project_root / ".dce" / "validation_outcome_artifacts.jsonl"),
    )

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "system breakdown" in lowered:
            output = json.dumps(
                {
                    "modules": [
                        {
                            "name": "MissionBoardCoordinator",
                            "layer": "DGCE Core",
                            "responsibility": "Coordinate mission generation",
                            "inputs": [
                                {
                                    "name": "raw_section_input",
                                    "type": "SectionInputRequest",
                                    "schema_fields": [
                                        {"name": "section_id", "type": "string", "required": True}
                                    ],
                                }
                            ],
                            "outputs": [
                                {
                                    "name": "StaleCheckRecord",
                                    "type": "artifact",
                                    "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
                                }
                            ],
                            "dependencies": [
                                {"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"}
                            ],
                            "governance_touchpoints": ["input validation"],
                            "failure_modes": ["invalid input structure"],
                            "owned_paths": [".dce/preflight/{section_id}.stale_check.json"],
                            "implementation_order": 1,
                        }
                    ],
                    "build_graph": {"edges": [["MissionBoardCoordinator", "MissionBoardCoordinator"]]},
                    "tests": [],
                }
            )
        elif "data model" in lowered:
            output = json.dumps(
                {
                    "entities": ["Mission"],
                    "fields": ["id", "state"],
                }
            )
        elif "api surface" in lowered:
            output = json.dumps(
                {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
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

    result = run_section_with_workspace(_section(), project_root, router_planner=planner)

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
    outputs_payload = json.loads(
        (project_root / ".dce" / "outputs" / "mission-board.json").read_text(encoding="utf-8")
    )
    advisory_index = json.loads((project_root / ".dce" / "advisory_index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    assert outputs_payload["advisory"] == result.advisory
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
    assert workspace_summary == {
        **_expected_artifact_metadata("workspace_summary"),
        "total_sections_seen": 1,
        "sections": [
            {
                "section_id": "mission-board",
                "latest_run_mode": "create_only",
                "latest_run_outcome_class": "success_create_only",
                "latest_status": "success",
                "latest_validation_ok": True,
                "latest_advisory_type": None,
                "latest_advisory_explanation": None,
                "latest_written_files_count": 4,
                "latest_skipped_modify_count": 0,
                "latest_skipped_ignore_count": 0,
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
                "decision_source": None,
                "review_status": None,
                "latest_decision": None,
                "latest_decision_source": None,
                "latest_stage": "outputs",
                "latest_stage_status": "success_create_only",
                "section_summary": _expected_section_summary(
                    section_id="mission-board",
                    latest_stage="outputs",
                    latest_stage_status="success_create_only",
                ),
            }
        ],
    }


def test_run_section_with_workspace_uses_structured_content_for_validation_summary(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_structured_content_validation_summary")
    planner = RouterPlanner(
        cache=ExactMatchCache(project_root / ".dce" / "structured_content_cache.json"),
        artifact_store=ArtifactStore(project_root / ".dce" / "structured_content_artifacts.jsonl"),
    )

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "system breakdown" in lowered:
            output = json.dumps(
                {
                    "modules": [
                        {
                            "name": "MissionBoardCoordinator",
                            "layer": "DGCE Core",
                            "responsibility": "Coordinate mission generation",
                            "inputs": [
                                {
                                    "name": "raw_section_input",
                                    "type": "SectionInputRequest",
                                    "schema_fields": [
                                        {"name": "section_id", "type": "string", "required": True}
                                    ],
                                }
                            ],
                            "outputs": [
                                {
                                    "name": "StaleCheckRecord",
                                    "type": "artifact",
                                    "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
                                }
                            ],
                            "dependencies": [
                                {"name": "artifact_writer", "kind": "module", "reference": "planner/io.py"}
                            ],
                            "governance_touchpoints": ["input validation"],
                            "failure_modes": ["invalid input structure"],
                            "owned_paths": [".dce/preflight/{section_id}.stale_check.json"],
                            "implementation_order": 1,
                        }
                    ],
                    "build_graph": {"edges": [["MissionBoardCoordinator", "MissionBoardCoordinator"]]},
                    "tests": [],
                }
            )
        elif "data model" in lowered:
            output = json.dumps(
                {
                    "entities": [
                        {"name": "Mission", "fields": [{"name": "id", "type": "string"}]},
                    ],
                    "fields": ["id"],
                    "relationships": ["Mission->Player"],
                    "validation_rules": ["id required"],
                }
            )
        elif "api surface" in lowered:
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

    result = run_section_with_workspace(_section(), project_root, router_planner=planner)

    assert result.execution_outcome["validation_summary"] == {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }


def test_run_section_with_workspace_data_model_validation_summary_ignores_cross_schema_metadata(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_validation_summary_schema_isolation")

    class FakePlanner:
        def route(self, task, classification):
            structured_content = None
            execution_metadata = {}
            output = "Summary output"

            if task.task_type == "system_breakdown":
                structured_content = {
                    "modules": [
                        {
                            "name": "MissionBoardCoordinator",
                            "layer": "DGCE Core",
                            "responsibility": "Coordinate mission generation",
                            "inputs": [],
                            "outputs": [
                                {
                                    "name": "stale_check_artifact",
                                    "type": "artifact",
                                    "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
                                }
                            ],
                            "dependencies": [],
                            "governance_touchpoints": [],
                            "failure_modes": [],
                            "owned_paths": [".dce/preflight/{section_id}.stale_check.json"],
                            "implementation_order": 1,
                        }
                    ],
                    "build_graph": {"edges": [["MissionBoardCoordinator", "MissionBoardCoordinator"]]},
                    "tests": [],
                }
                execution_metadata = {"structure_valid": True}
                output = json.dumps(structured_content)
            elif task.task_type == "data_model":
                structured_content = {
                    "modules": [
                        {
                            "name": "DGCEDataModel",
                            "entities": ["Mission"],
                            "relationships": ["Mission->Player"],
                            "required": [],
                            "identity_keys": [],
                        }
                    ],
                    "entities": [{"name": "Mission", "fields": [{"name": "id", "type": "string"}]}],
                    "fields": ["id"],
                    "relationships": ["Mission->Player"],
                    "validation_rules": ["id required"],
                }
                execution_metadata = {
                    "structure_valid": False,
                    "structure_error": "missing_keys",
                    "structure_missing_keys": ["interfaces"],
                }
                output = json.dumps(structured_content)
            elif task.task_type == "api_surface":
                structured_content = {
                    "interfaces": ["MissionBoardService"],
                    "methods": ["create_mission"],
                    "inputs": ["template_id"],
                    "outputs": ["mission_id"],
                    "error_cases": ["template_missing"],
                }
                output = json.dumps(structured_content)

            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": output,
                    "reused": False,
                    "execution_metadata": execution_metadata,
                    "structured_content": structured_content,
                },
            )()

    result = run_section_with_workspace(
        _section(),
        project_root,
        router_planner=FakePlanner(),
    )

    assert result.execution_outcome["validation_summary"] == {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }


def test_write_stage_data_model_validation_uses_data_model_schema_not_api_surface():
    task = ClassificationRequest(
        content="Describe the data model",
        request_id="dgce-write-stage-data-model-schema",
        task_type="data_model",
        output_contract=OutputContract(mode="structured", schema_name="dgce_data_model_v1"),
    )
    response = ResponseEnvelope(
        request_id="dgce-write-stage-data-model-schema",
        task_type="data_model",
        status="experimental_output",
        task_bucket="code_routine",
        decision="MID_MODEL",
        output="",
        reused=False,
        structured_content={
            "modules": [
                {
                    "name": "DGCEDataModel",
                    "entities": ["SectionInput"],
                    "relationships": ["SectionInput->ExecutionStamp"],
                    "required": [],
                    "identity_keys": [],
                }
            ],
            "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
            "fields": ["section_id"],
            "relationships": ["SectionInput->ExecutionStamp"],
            "validation_rules": ["section_id required"],
        },
    )

    validation = _validate_write_stage_structured_content(task, response)

    assert validation is not None
    assert validation.ok is True
    assert "interfaces" not in validation.missing_keys


def test_final_persisted_validation_summary_ignores_api_surface_metadata_in_data_model_write_flow(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_persisted_validation_summary_schema_isolation")
    section = DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE data model.",
        requirements=[
            "Define SectionInput and ExecutionStamp entities",
            "Keep the model deterministic and auditable",
        ],
        constraints=["Keep the model independent of .dce file paths"],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )
    section_id = "data-model"

    for path in section.expected_targets:
        _write_text(project_root / path, "existing-development-content")
    _write_ownership_index(project_root, [{"path": path, "section_id": section_id} for path in section.expected_targets])
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    class FakePlanner:
        def route(self, task, classification):
            structured_content = None
            execution_metadata = {}
            output = "Summary output"

            if task.task_type == "data_model":
                structured_content = {
                    "modules": [
                        {
                            "name": "DGCEDataModel",
                            "entities": ["SectionInput"],
                            "relationships": ["SectionInput->ExecutionStamp"],
                            "required": [],
                            "identity_keys": [],
                        }
                    ],
                    "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
                    "fields": ["section_id"],
                    "relationships": ["SectionInput->ExecutionStamp"],
                    "validation_rules": ["section_id required"],
                }
                output = json.dumps(structured_content)
            elif task.task_type == "api_surface":
                execution_metadata = {
                    "structure_valid": False,
                    "structure_error": "missing_keys",
                    "structure_missing_keys": ["interfaces", "methods", "inputs", "outputs", "error_cases"],
                }

            return type(
                "RouteResult",
                (),
                {
                    "status": ArtifactStatus.EXPERIMENTAL,
                    "task_bucket": "planning" if task.task_type in {"system_breakdown", "system_summary"} else "code_routine",
                    "decision": "MID_MODEL",
                    "output": output,
                    "reused": False,
                    "execution_metadata": execution_metadata,
                    "structured_content": structured_content,
                },
            )()

    result = run_section_with_workspace(section, project_root, router_planner=FakePlanner())
    outputs_payload = json.loads((project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))

    assert outputs_payload["execution_outcome"]["stage"] == "WRITE"
    assert outputs_payload["execution_outcome"]["validation_summary"] == {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }
    assert result.execution_outcome["validation_summary"] == {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }
    assert result.run_outcome_class != "validation_failure"
    assert result.advisory == {
        "type": "process_adjustment",
        "summary": f"Review failed DGCE run flow for {section_id}",
        "explanation": ["execution_error"],
    }


def test_run_section_with_workspace_calls_decompose_once(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_single_decompose")
    call_count = {"count": 0}
    from aether.dgce.decompose import decompose_section as original_decompose

    def fake_decompose(section):
        call_count["count"] += 1
        return original_decompose(section)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_write_file_plan(file_plan, output_dir, overwrite_paths=None):
        written_files = []
        for entry in file_plan.files:
            path = output_dir / Path(entry["path"])
            _write_text(path, f"generated:{entry['path']}")
            written_files.append(entry["path"])
        return written_files

    monkeypatch.setattr("aether.dgce.decompose.decompose_section", fake_decompose)
    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose.write_file_plan", fake_write_file_plan)

    run_section_with_workspace(_section(), project_root)

    assert call_count["count"] == 1


def test_run_section_with_workspace_incremental_v1_persists_plan_only_artifact(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_plan_only")
    _write_text(project_root / "mission_board" / "service.py", "existing")
    _write_text(project_root / "docs" / "readme.md", "existing-doc")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(
        "aether.dgce.decompose.write_file_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("write_file_plan should not run in incremental_v1")),
    )

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v1")
    change_plan = json.loads(
        (project_root / ".dce" / "plans" / "mission-board.change_plan.json").read_text(encoding="utf-8")
    )

    assert result.written_files == []
    assert result.run_mode == "incremental_v1"
    assert result.run_outcome_class == "planned_incremental_v1"
    assert result.execution_outcome is None
    assert result.write_transparency is None
    assert change_plan == {
        "section_id": "mission-board",
        "mode": "incremental_v1",
        "summary": {
            "create_count": 3,
            "modify_count": 1,
            "ignore_count": 1,
        },
        "changes": [
            {
                "action": "create",
                "path": "api/missionboardservice.py",
                "reason": "target_missing_from_workspace",
            },
            {
                "action": "create",
                "path": "mission_board/models.py",
                "reason": "target_missing_from_workspace",
            },
            {
                "action": "modify",
                "path": "mission_board/service.py",
                "reason": "target_present_in_workspace",
            },
            {
                "action": "create",
                "path": "models/mission.py",
                "reason": "target_missing_from_workspace",
            },
        ],
        "ignored_existing_files": ["docs/readme.md"],
    }
    assert (project_root / "api" / "missionboardservice.py").exists() is False
    assert (project_root / "mission_board" / "models.py").exists() is False
    assert (project_root / "models" / "mission.py").exists() is False
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing"


def test_run_section_with_workspace_default_flow_still_writes_files(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_default_unchanged")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root)

    assert result.run_mode == "create_only"
    assert sorted(result.written_files) == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]


def test_build_incremental_preview_artifact_create_target_reports_write_metadata():
    project_root = _workspace_dir("dgce_incremental_v2_create_preview")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": "api/create.py", "purpose": "Create", "source": "api_surface"}],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/create.py", "action": "create", "reason": "target_missing_from_workspace"},
    ]

    preview = build_incremental_preview_artifact("mission-board", file_plan, change_plan, project_root)
    generated_bytes = render_file_entry_bytes({"path": "api/create.py", "purpose": "Create", "source": "api_surface"})

    assert preview == {
        "section_id": "mission-board",
        "mode": "incremental_v2",
        "preview_outcome_class": "preview_create_only",
        "recommended_mode": "create_only",
        "summary": {
            "total_targets": 1,
            "total_create": 1,
            "total_modify": 0,
            "total_ignore": 0,
            "total_write": 1,
            "total_skip": 0,
            "total_eligible": 1,
            "total_blocked": 0,
            "total_identical": 0,
            "total_blocked_ownership": 0,
            "total_blocked_modify_disabled": 0,
            "total_blocked_ignore": 0,
        },
        "previews": [
            {
                "path": "api/create.py",
                "section_id": "mission-board",
                "planned_action": "create",
                "eligibility": "eligible",
                "preview_decision": "write",
                "preview_reason": "create",
                "identical_content": False,
                "existing_bytes": 0,
                "generated_bytes": len(generated_bytes),
                "approximate_line_delta": len(generated_bytes.decode("utf-8").splitlines()),
            }
        ],
    }


def test_build_incremental_preview_artifact_modify_target_reports_gated_outcomes():
    project_root = _workspace_dir("dgce_incremental_v2_modify_preview")
    modify_entry = {"path": "api/modify.py", "purpose": "Modify", "source": "api_surface"}
    modify_path = project_root / "api" / "modify.py"
    modify_path.parent.mkdir(parents=True, exist_ok=True)
    modify_path.write_text("existing", encoding="utf-8")
    change_plan = [
        {"section_id": "mission-board", "path": "api/modify.py", "action": "modify", "reason": "target_present_in_workspace"},
    ]
    file_plan = FilePlan(project_name="DGCE", files=[modify_entry])

    blocked_preview = build_incremental_preview_artifact("mission-board", file_plan, change_plan, project_root)
    owned_preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths={"api/modify.py"},
    )

    modify_path.write_bytes(render_file_entry_bytes(modify_entry))
    identical_preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
        owned_paths={"api/modify.py"},
    )

    assert blocked_preview["previews"] == [
        {
            "path": "api/modify.py",
            "section_id": "mission-board",
            "planned_action": "modify",
            "eligibility": "blocked",
            "preview_decision": "skip",
            "preview_reason": "ownership",
            "identical_content": False,
            "existing_bytes": len(b"existing"),
            "generated_bytes": len(render_file_entry_bytes(modify_entry)),
            "approximate_line_delta": _changed_lines_estimate(b"existing", render_file_entry_bytes(modify_entry)),
        }
    ]
    assert owned_preview["previews"][0]["preview_decision"] == "write"
    assert owned_preview["previews"][0]["preview_reason"] == "modify"
    assert owned_preview["previews"][0]["eligibility"] == "eligible"
    assert identical_preview["previews"][0]["preview_decision"] == "skip"
    assert identical_preview["previews"][0]["preview_reason"] == "identical"
    assert identical_preview["previews"][0]["identical_content"] is True


def test_build_incremental_preview_artifact_ignore_target_remains_skipped():
    project_root = _workspace_dir("dgce_incremental_v2_ignore_preview")
    _write_text(project_root / "api" / "ignore.py", "existing")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": "api/ignore.py", "purpose": "Ignore", "source": "api_surface"}],
    )
    change_plan = [
        {"section_id": "mission-board", "path": "api/ignore.py", "action": "ignore", "reason": "not_in_expected_targets"},
    ]

    preview = build_incremental_preview_artifact("mission-board", file_plan, change_plan, project_root)

    assert preview["previews"] == [
        {
            "path": "api/ignore.py",
            "section_id": "mission-board",
            "planned_action": "ignore",
            "eligibility": "blocked",
            "preview_decision": "skip",
            "preview_reason": "ignore",
            "identical_content": False,
            "existing_bytes": len(b"existing"),
            "generated_bytes": len(render_file_entry_bytes({"path": "api/ignore.py", "purpose": "Ignore", "source": "api_surface"})),
            "approximate_line_delta": _changed_lines_estimate(
                b"existing",
                render_file_entry_bytes({"path": "api/ignore.py", "purpose": "Ignore", "source": "api_surface"}),
            ),
        }
    ]


def test_build_incremental_preview_artifact_reports_existing_bytes_for_host_repo_target():
    repo_root = _workspace_dir("dgce_incremental_preview_host_repo_existing_bytes")
    project_root = repo_root / "defiant-sky"
    existing_content = "existing-cli-content"
    _write_text(project_root / ".dce" / "input" / "api-surface.json", "{}")
    _write_text(repo_root / "dce.py", existing_content)
    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": "dce.py", "purpose": "CLI", "source": "expected_targets"}],
    )
    change_plan = [
        {"section_id": "api-surface", "path": "dce.py", "action": "modify", "reason": "expected_target_exists"},
    ]

    preview = build_incremental_preview_artifact("api-surface", file_plan, change_plan, project_root)

    assert preview["previews"][0]["planned_action"] == "modify"
    assert preview["previews"][0]["existing_bytes"] == len(existing_content.encode("utf-8"))
    assert preview["previews"][0]["preview_reason"] in {"ownership", "modify", "identical"}


def test_build_incremental_preview_artifact_uses_valid_code_graph_facts_as_preview_guidance_only():
    project_root = _workspace_dir("dgce_incremental_v2_code_graph_guidance")
    modify_entry = {"path": "mission_board/service.py", "purpose": "Modify", "source": "service"}
    modify_path = project_root / "mission_board" / "service.py"
    modify_path.parent.mkdir(parents=True, exist_ok=True)
    modify_path.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(project_name="DGCE", files=[modify_entry])
    change_plan = [
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "target_present_in_workspace"},
    ]

    preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        code_graph_context=_valid_code_graph_context(),
    )

    assert preview["previews"][0]["preview_decision"] == "skip"
    assert preview["previews"][0]["preview_reason"] == "ownership"
    assert preview["planning_context"] == {
        "guidance_source": "code_graph_guided",
        "fallback_reason": None,
        "reasoning_summary": "Preview ranked Code Graph insertion candidates, interpreted collision risk, and selected Preview-only guidance.",
    }
    assert preview["previews"][0]["preview_planning_basis"] == "code_graph_guided"
    assert preview["previews"][0]["recommended_edit_strategy"] == "bounded_insert"
    assert preview["previews"][0]["preview_edit_strategy"] == "bounded_insert"
    assert preview["previews"][0]["generation_collision_detected"] is True
    assert preview["previews"][0]["collision_assessment"] == "ambiguous_overlap"
    assert preview["previews"][0]["selected_insertion_candidate"]["symbol_id"] == "sym:existing_helper"
    assert "collision assessment is ambiguous_overlap" in preview["previews"][0]["preview_reasoning"]
    assert preview["previews"][0]["insertion_candidates"] == [
        {
            "file_path": "mission_board/service.py",
            "symbol_id": "sym:existing_helper",
            "symbol_name": "existing_helper",
            "strategy": "append_after_symbol",
            "span": {"start_line": 7, "end_line": 7},
        }
    ]


def test_build_incremental_preview_artifact_ignores_malformed_code_graph_facts():
    project_root = _workspace_dir("dgce_incremental_v2_code_graph_invalid")
    modify_entry = {"path": "mission_board/service.py", "purpose": "Modify", "source": "service"}
    modify_path = project_root / "mission_board" / "service.py"
    modify_path.parent.mkdir(parents=True, exist_ok=True)
    modify_path.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(project_name="DGCE", files=[modify_entry])
    change_plan = [
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "target_present_in_workspace"},
    ]

    preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        code_graph_context={"contract_name": "DefiantCodeGraphFacts", "contract_version": "wrong", "graph_id": "graph:bad"},
    )

    assert preview["planning_context"] == {
        "guidance_source": "code_graph_fallback",
        "fallback_reason": "facts_malformed",
        "reasoning_summary": "Code Graph facts were provided but malformed, so Preview used baseline planning.",
    }
    assert preview["previews"] == [
        {
            "path": "mission_board/service.py",
            "section_id": "mission-board",
            "planned_action": "modify",
            "eligibility": "blocked",
            "preview_decision": "skip",
            "preview_reason": "ownership",
            "identical_content": False,
            "existing_bytes": len(b"existing"),
            "generated_bytes": len(render_file_entry_bytes(modify_entry)),
            "approximate_line_delta": _changed_lines_estimate(b"existing", render_file_entry_bytes(modify_entry)),
        }
    ]


def test_build_incremental_preview_artifact_ranks_candidates_deterministically_and_prefers_lower_invasiveness():
    project_root = _workspace_dir("dgce_incremental_v2_code_graph_ranking")
    modify_entry = {"path": "mission_board/service.py", "purpose": "Modify", "source": "service"}
    modify_path = project_root / "mission_board" / "service.py"
    modify_path.parent.mkdir(parents=True, exist_ok=True)
    modify_path.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(project_name="DGCE", files=[modify_entry])
    change_plan = [
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "target_present_in_workspace"},
    ]

    preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        code_graph_context=_ranked_code_graph_context(),
    )

    preview_entry = preview["previews"][0]

    assert [candidate["symbol_id"] for candidate in preview_entry["insertion_candidates"]] == [
        "sym:append_local",
        "sym:before_local",
        "sym:inside_target",
    ]
    assert preview_entry["selected_insertion_candidate"]["symbol_id"] == "sym:append_local"
    assert preview_entry["recommended_edit_strategy"] == "rewrite_small_region"
    assert preview_entry["preview_edit_strategy"] == "bounded_insert"
    assert preview_entry["collision_assessment"] == "likely_safe_extension"
    assert "preferred bounded_insert over contract guidance rewrite_small_region" in preview_entry["preview_reasoning"]


def test_build_incremental_preview_artifact_marks_ambiguous_overlap_without_changing_preview_decision():
    project_root = _workspace_dir("dgce_incremental_v2_code_graph_ambiguous")
    modify_entry = {"path": "mission_board/service.py", "purpose": "Modify", "source": "service"}
    modify_path = project_root / "mission_board" / "service.py"
    modify_path.parent.mkdir(parents=True, exist_ok=True)
    modify_path.write_text("existing", encoding="utf-8")
    file_plan = FilePlan(project_name="DGCE", files=[modify_entry])
    change_plan = [
        {"section_id": "mission-board", "path": "mission_board/service.py", "action": "modify", "reason": "target_present_in_workspace"},
    ]

    preview = build_incremental_preview_artifact(
        "mission-board",
        file_plan,
        change_plan,
        project_root,
        code_graph_context=_ambiguous_code_graph_context(),
    )

    assert preview["previews"][0]["preview_decision"] == "skip"
    assert preview["previews"][0]["preview_reason"] == "ownership"
    assert preview["previews"][0]["collision_assessment"] == "ambiguous_overlap"
    assert preview["previews"][0]["preview_edit_strategy"] == "bounded_insert"


def test_build_incremental_change_plan_keeps_create_for_truly_missing_target():
    repo_root = _workspace_dir("dgce_incremental_expected_targets_missing")
    project_root = repo_root / "defiant-sky"
    _write_text(project_root / ".dce" / "input" / "api-surface.json", "{}")
    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": "apps/aether_api/routers/new_endpoint.py", "purpose": "Router", "source": "expected_targets"}],
    )

    change_plan = build_incremental_change_plan(
        "api-surface",
        file_plan,
        scan_workspace_file_paths(project_root),
        project_root=project_root,
    )

    assert change_plan["changes"] == [
        {
            "action": "create",
            "path": "apps/aether_api/routers/new_endpoint.py",
            "reason": "target_missing_from_workspace",
        }
    ]


def test_system_breakdown_host_repo_targets_resolve_consistently_across_preview_and_write_transparency():
    repo_root = _workspace_dir("dgce_incremental_system_breakdown_host_repo_consistency")
    project_root = repo_root / "defiant-sky"
    _write_text(project_root / ".dce" / "input" / "system-breakdown.json", "{}")
    targets = [
        "aether/dgce/decompose.py",
        "aether/dgce/incremental.py",
        "aether/dgce/file_writer.py",
        "dce.py",
    ]
    for target in targets:
        _write_text(repo_root / Path(target), f"existing:{target}")

    file_plan = FilePlan(
        project_name="DGCE",
        files=[{"path": target, "purpose": target, "source": "expected_targets"} for target in targets],
    )

    preview = build_incremental_preview_artifact("system-breakdown", file_plan, [], project_root)
    _, transparency = build_write_transparency(file_plan, [], project_root)

    assert all(entry["planned_action"] == "modify" for entry in preview["previews"])
    assert all(entry["preview_reason"] == "ownership" for entry in preview["previews"])
    assert all(entry["existing_bytes"] > 0 for entry in preview["previews"])
    assert all(entry["reason"] == "ownership" for entry in transparency["write_decisions"])


def test_identical_host_repo_expected_targets_resolve_to_identical_skip_consistently():
    repo_root = _workspace_dir("dgce_incremental_identical_host_repo_targets")
    project_root = repo_root / "defiant-sky"
    _write_text(project_root / ".dce" / "input" / "system-breakdown.json", "{}")
    targets = [
        "aether/dgce/decompose.py",
        "aether/dgce/incremental.py",
        "aether/dgce/file_writer.py",
        "dce.py",
    ]
    file_entries = [{"path": target, "purpose": target, "source": "expected_targets"} for target in targets]
    for entry in file_entries:
        (repo_root / Path(entry["path"])).parent.mkdir(parents=True, exist_ok=True)
        (repo_root / Path(entry["path"])).write_bytes(render_file_entry_bytes(entry))

    file_plan = FilePlan(project_name="DGCE", files=file_entries)
    owned_paths = {entry["path"] for entry in file_entries}
    preview = build_incremental_preview_artifact(
        "system-breakdown",
        file_plan,
        [],
        project_root,
        allow_modify_write=True,
        owned_paths=owned_paths,
    )
    _, transparency = build_write_transparency(
        file_plan,
        [],
        project_root,
        allow_modify_write=True,
        owned_paths=owned_paths,
    )

    assert all(entry["planned_action"] == "modify" for entry in preview["previews"])
    assert all(entry["preview_reason"] == "identical" for entry in preview["previews"])
    assert all(entry["preview_decision"] == "skip" for entry in preview["previews"])
    assert all(entry["reason"] == "identical" for entry in transparency["write_decisions"])


def test_summarize_incremental_preview_derives_summary_counters():
    previews = [
        {"planned_action": "create", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "create", "identical_content": False},
        {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "ownership", "identical_content": False},
        {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "modify", "identical_content": False},
        {"planned_action": "ignore", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "ignore", "identical_content": False},
        {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "identical", "identical_content": True},
    ]

    summary, preview_outcome_class, recommended_mode = summarize_incremental_preview(previews)

    assert summary == {
        "total_targets": 5,
        "total_create": 1,
        "total_modify": 3,
        "total_ignore": 1,
        "total_write": 1,
        "total_skip": 4,
        "total_eligible": 1,
        "total_blocked": 4,
        "total_identical": 1,
        "total_blocked_ownership": 1,
        "total_blocked_modify_disabled": 1,
        "total_blocked_ignore": 1,
    }
    assert preview_outcome_class == "preview_blocked_ownership"
    assert recommended_mode == "review_required"


def test_summarize_incremental_preview_classifies_create_only():
    summary, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "create", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "create", "identical_content": False},
            {"planned_action": "create", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "create", "identical_content": False},
        ]
    )

    assert preview_outcome_class == "preview_create_only"
    assert recommended_mode == "create_only"
    assert summary["total_write"] == 2


def test_summarize_incremental_preview_classifies_safe_modify_ready():
    summary, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "modify", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "modify", "identical_content": False},
            {"planned_action": "create", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "create", "identical_content": False},
        ]
    )

    assert preview_outcome_class == "preview_safe_modify_ready"
    assert recommended_mode == "safe_modify"
    assert summary["total_blocked_modify_disabled"] == 0
    assert summary["total_write"] == 2


def test_summarize_incremental_preview_classifies_blocked_modify_disabled():
    _, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "modify", "identical_content": False},
        ]
    )

    assert preview_outcome_class == "preview_blocked_modify_disabled"
    assert recommended_mode == "review_required"


def test_summarize_incremental_preview_classifies_identical_only():
    _, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "identical", "identical_content": True},
            {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "identical", "identical_content": True},
        ]
    )

    assert preview_outcome_class == "preview_identical_only"
    assert recommended_mode == "no_changes"


def test_summarize_incremental_preview_classifies_ignore_only():
    _, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "ignore", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "ignore", "identical_content": False},
            {"planned_action": "ignore", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "ignore", "identical_content": False},
        ]
    )

    assert preview_outcome_class == "preview_ignore_only"
    assert recommended_mode == "no_changes"


def test_summarize_incremental_preview_classifies_mixed():
    _, preview_outcome_class, recommended_mode = summarize_incremental_preview(
        [
            {"planned_action": "create", "eligibility": "eligible", "preview_decision": "write", "preview_reason": "create", "identical_content": False},
            {"planned_action": "modify", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "modify", "identical_content": False},
            {"planned_action": "ignore", "eligibility": "blocked", "preview_decision": "skip", "preview_reason": "ignore", "identical_content": False},
        ]
    )

    assert preview_outcome_class == "preview_mixed"
    assert recommended_mode == "review_required"


def test_summarize_incremental_preview_classifies_empty():
    summary, preview_outcome_class, recommended_mode = summarize_incremental_preview([])

    assert summary == {
        "total_targets": 0,
        "total_create": 0,
        "total_modify": 0,
        "total_ignore": 0,
        "total_write": 0,
        "total_skip": 0,
        "total_eligible": 0,
        "total_blocked": 0,
        "total_identical": 0,
        "total_blocked_ownership": 0,
        "total_blocked_modify_disabled": 0,
        "total_blocked_ignore": 0,
    }
    assert preview_outcome_class == "preview_empty"
    assert recommended_mode == "no_changes"


def test_render_incremental_review_markdown_groups_files_and_stays_metadata_only():
    review = render_incremental_review_markdown(
        {
            "section_id": "mission-board",
            "mode": "incremental_v2_2",
            "preview_outcome_class": "preview_mixed",
            "recommended_mode": "review_required",
            "summary": {
                "total_targets": 5,
                "total_create": 1,
                "total_modify": 3,
                "total_ignore": 1,
                "total_write": 2,
                "total_skip": 3,
                "total_eligible": 2,
                "total_blocked": 3,
                "total_identical": 1,
                "total_blocked_ownership": 1,
                "total_blocked_modify_disabled": 0,
                "total_blocked_ignore": 1,
            },
            "previews": [
                {
                    "path": "api/create.py",
                    "section_id": "mission-board",
                    "planned_action": "create",
                    "preview_decision": "write",
                    "preview_reason": "create",
                    "existing_bytes": 0,
                    "generated_bytes": 120,
                    "approximate_line_delta": 12,
                },
                {
                    "path": "api/modify.py",
                    "section_id": "mission-board",
                    "planned_action": "modify",
                    "preview_decision": "write",
                    "preview_reason": "modify",
                    "existing_bytes": 90,
                    "generated_bytes": 150,
                    "approximate_line_delta": 6,
                },
                {
                    "path": "api/blocked.py",
                    "section_id": "mission-board",
                    "planned_action": "modify",
                    "preview_decision": "skip",
                    "preview_reason": "ownership",
                    "existing_bytes": 90,
                    "generated_bytes": 150,
                    "approximate_line_delta": 6,
                },
                {
                    "path": "api/identical.py",
                    "section_id": "mission-board",
                    "planned_action": "modify",
                    "preview_decision": "skip",
                    "preview_reason": "identical",
                    "existing_bytes": 90,
                    "generated_bytes": 90,
                    "approximate_line_delta": 0,
                },
                {
                    "path": "api/ignore.py",
                    "section_id": "mission-board",
                    "planned_action": "ignore",
                    "preview_decision": "skip",
                    "preview_reason": "ignore",
                    "existing_bytes": 90,
                    "generated_bytes": 150,
                    "approximate_line_delta": 0,
                },
            ],
        }
    )

    assert review.startswith("# Section Review: mission-board\n")
    assert "- Mode: incremental_v2_2" in review
    assert "- Preview outcome: preview_mixed" in review
    assert "- Recommended mode: review_required" in review
    assert "## Summary" in review
    assert "- Total targets: 5" in review
    assert "## Create candidates" in review
    assert "`api/create.py` -- decision: write / reason: create / existing: 0 / generated: 120 / delta: 12" in review
    assert "## Modify-ready candidates" in review
    assert "`api/modify.py` -- decision: write / reason: modify / existing: 90 / generated: 150 / delta: 6" in review
    assert "## Blocked candidates" in review
    assert "`api/blocked.py` -- decision: skip / reason: ownership / existing: 90 / generated: 150 / delta: 6" in review
    assert "## Identical / no-change candidates" in review
    assert "`api/identical.py` -- decision: skip / reason: identical / existing: 90 / generated: 90 / delta: 0" in review
    assert "## Ignored candidates" in review
    assert "`api/ignore.py` -- decision: skip / reason: ignore / existing: 90 / generated: 150 / delta: 0" in review
    assert "# Generated by Aether" not in review
    assert "```" not in review
    assert "@@" not in review


def test_compute_artifact_fingerprint_is_identical_for_identical_bytes():
    project_root = _workspace_dir("dgce_incremental_v3_0_fingerprint_identical")
    artifact_a = project_root / ".dce" / "plans" / "a.preview.json"
    artifact_b = project_root / ".dce" / "plans" / "b.preview.json"
    payload = json.dumps({"section_id": "mission-board", "value": 1}, indent=2, sort_keys=True) + "\n"
    _write_text(artifact_a, payload)
    _write_text(artifact_b, payload)

    assert compute_artifact_fingerprint(artifact_a) == compute_artifact_fingerprint(artifact_b)


def test_compute_artifact_fingerprint_changes_when_file_bytes_change():
    project_root = _workspace_dir("dgce_incremental_v3_0_fingerprint_changed")
    artifact_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    _write_text(artifact_path, "alpha")
    first = compute_artifact_fingerprint(artifact_path)
    _write_text(artifact_path, "beta")

    assert first != compute_artifact_fingerprint(artifact_path)


def test_compute_json_payload_fingerprint_is_stable_without_artifact_field():
    payload = {"section_id": "mission-board", "value": 1}
    stamped_payload = {"section_id": "mission-board", "value": 1, "artifact_fingerprint": "ignored"}

    assert compute_json_payload_fingerprint(payload) == compute_json_payload_fingerprint(stamped_payload)


def test_compute_json_payload_fingerprint_ignores_timestamp_fields_and_normalizes_list_order():
    first = {
        "section_id": "mission-board",
        "validation_timestamp": "2026-03-26T00:00:00Z",
        "requirements": ["track progression state", "support mission templates"],
        "previews": [
            {"path": "api/missionboardservice.py", "planned_action": "create"},
            {"path": "docs/readme.md", "planned_action": "modify"},
        ],
    }
    second = {
        "previews": [
            {"planned_action": "modify", "path": "docs/readme.md"},
            {"planned_action": "create", "path": "api/missionboardservice.py"},
        ],
        "requirements": ["support mission templates", "track progression state"],
        "section_id": "mission-board",
        "validation_timestamp": "2030-01-01T00:00:00Z",
    }

    assert compute_json_payload_fingerprint(first) == compute_json_payload_fingerprint(second)


def test_compute_preview_payload_fingerprint_canonicalizes_preview_files_and_strings():
    first = {
        "section_id": "mission-board",
        "review_timestamp": "2026-03-26T00:00:00Z",
        "file_plan": {
            "project_name": "DGCE",
            "files": [
                {"path": "z.py", "purpose": " Zebra ", "source": "api_surface"},
                {"path": "a.py", "purpose": "Alpha", "source": "system_breakdown"},
            ],
        },
        "previews": [
            {"path": " z.py ", "preview_reason": "create", "preview_decision": "write"},
            {"preview_decision": "write", "preview_reason": "create", "path": "a.py"},
        ],
    }
    second = {
        "previews": [
            {"path": "a.py", "preview_decision": "write", "preview_reason": "create"},
            {"path": "z.py", "preview_reason": "create", "preview_decision": "write"},
        ],
        "file_plan": {
            "files": [
                {"purpose": "Alpha", "source": "system_breakdown", "path": "a.py"},
                {"source": "api_surface", "purpose": "Zebra", "path": "z.py"},
            ],
            "project_name": "DGCE",
        },
        "section_id": "mission-board",
        "review_timestamp": "2030-01-01T00:00:00Z",
    }

    assert compute_preview_payload_fingerprint(first) == compute_preview_payload_fingerprint(second)


def test_incremental_v1_1_skips_modify_when_modify_mode_disabled(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_modify_disabled")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v1_1")
    change_plan = json.loads(
        (project_root / ".dce" / "plans" / "mission-board.change_plan.json").read_text(encoding="utf-8")
    )

    assert change_plan["mode"] == "incremental_v1_1"
    assert "mission_board/service.py" not in result.written_files
    assert "mission_board/models.py" in result.written_files
    assert "api/missionboardservice.py" in result.written_files
    assert "models/mission.py" in result.written_files
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing-service"
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "modify"}
        for entry in result.write_transparency["write_decisions"]
    )


def test_incremental_v1_1_skips_unowned_modify_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_unowned_modify")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "docs/legacy.md",
                "section_id": "legacy-section",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        incremental_mode="incremental_v1_1",
    )

    assert "mission_board/service.py" not in result.written_files
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing-service"
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )


def test_incremental_v1_1_skips_identical_modify_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_identical_modify")
    identical_entry = {
        "path": "mission_board/service.py",
        "purpose": "coordinate mission generation service orchestration",
        "source": "system_breakdown",
    }
    target_path = project_root / "mission_board" / "service.py"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(render_file_entry_bytes(identical_entry))
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        incremental_mode="incremental_v1_1",
    )

    assert "mission_board/service.py" not in result.written_files
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "identical"}
        for entry in result.write_transparency["write_decisions"]
    )


def test_incremental_v1_1_allows_modify_when_all_gates_pass(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_modify_allowed")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        incremental_mode="incremental_v1_1",
    )

    assert "mission_board/service.py" in result.written_files
    assert any(
        entry["path"] == "mission_board/service.py"
        and entry["decision"] == "written"
        and entry["reason"] == "modify"
        for entry in result.write_transparency["write_decisions"]
    )
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"


def test_run_section_with_workspace_incremental_v1_1_only_writes_create_targets(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_integration_create_only")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(project_root, [])

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v1_1")

    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing-service"
    assert "mission_board/service.py" not in result.written_files
    assert any(
        entry == {"path": "mission_board/service.py", "decision": "skipped", "reason": "ownership"}
        for entry in result.write_transparency["write_decisions"]
    )


def test_run_section_with_workspace_incremental_v1_1_allows_owned_modify_when_enabled(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v1_1_integration_owned_modify")
    _write_text(project_root / "mission_board" / "service.py", "old-service-content")
    _write_ownership_index(
        project_root,
        [
            {
                "path": "mission_board/service.py",
                "section_id": "mission-board",
                "last_written_stage": "WRITE",
                "write_reason": "create",
            }
        ],
    )

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v1_1",
        allow_safe_modify=True,
    )

    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") != "old-service-content"
    assert "mission_board/service.py" in result.written_files
    assert any(
        entry["path"] == "mission_board/service.py"
        and entry["decision"] == "written"
        and entry["reason"] == "modify"
        for entry in result.write_transparency["write_decisions"]
    )


def test_run_section_with_workspace_incremental_v2_writes_preview_under_dce_only(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_preview_artifact")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")
    _write_ownership_index(project_root, [])

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2")
    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview = json.loads(preview_path.read_text(encoding="utf-8"))

    assert result.run_mode == "incremental_v2"
    assert result.run_outcome_class == "preview_incremental_v2"
    assert result.written_files == []
    assert preview_path.exists()
    assert (project_root / "api" / "missionboardservice.py").exists() is False
    assert (project_root / "mission_board" / "models.py").exists() is False
    assert (project_root / "models" / "mission.py").exists() is False
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing-service"
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert preview["mode"] == "incremental_v2"
    assert preview["section_id"] == "mission-board"
    assert preview["preview_outcome_class"] == "preview_blocked_ownership"
    assert preview["recommended_mode"] == "review_required"
    assert sorted(preview["summary"].keys()) == [
        "total_blocked",
        "total_blocked_ignore",
        "total_blocked_modify_disabled",
        "total_blocked_ownership",
        "total_create",
        "total_eligible",
        "total_identical",
        "total_ignore",
        "total_modify",
        "total_skip",
        "total_targets",
        "total_write",
    ]
    assert [entry["path"] for entry in preview["previews"]] == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert all(entry["path"] != "docs/readme.md" for entry in preview["previews"])
    assert "planning_context" not in preview
    assert "# Generated by Aether" not in json.dumps(preview)


def test_run_section_with_workspace_incremental_v2_preview_is_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_preview_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_preview_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), first_root, incremental_mode="incremental_v2")
    run_section_with_workspace(_section(), second_root, incremental_mode="incremental_v2")

    first_preview = (first_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8")
    second_preview = (second_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8")

    assert first_preview == second_preview


def test_run_section_with_workspace_incremental_v2_2_persists_code_graph_preview_guidance_without_changing_governed_execution(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    section = _section().model_copy(update={"code_graph_context": _valid_code_graph_context()})

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    project_root = _workspace_dir("dgce_incremental_v2_2_code_graph_preview_guidance")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(project_root, [])
    preview_result = run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    preview = json.loads((project_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8"))
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-04-08T00:00:00Z",
        ),
    )
    governed_result = run_dgce_section("mission-board", project_root, governed=True)

    baseline_root = _workspace_dir("dgce_incremental_v2_2_code_graph_preview_baseline")
    _write_text(baseline_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(baseline_root, [])
    run_section_with_workspace(_section(), baseline_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        baseline_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-04-08T00:00:00Z",
        ),
    )
    baseline_governed_result = run_dgce_section("mission-board", baseline_root, governed=True)

    guided_preview = next(entry for entry in preview["previews"] if entry["path"] == "mission_board/service.py")

    assert preview_result.run_outcome_class == "review_incremental_v2_2"
    assert preview["planning_context"]["guidance_source"] == "code_graph_guided"
    assert guided_preview["recommended_edit_strategy"] == "bounded_insert"
    assert guided_preview["preview_edit_strategy"] == "bounded_insert"
    assert guided_preview["generation_collision_detected"] is True
    assert guided_preview["collision_assessment"] == "ambiguous_overlap"
    assert guided_preview["insertion_candidates"][0]["symbol_id"] == "sym:existing_helper"
    assert governed_result.status == "success"
    assert governed_result.run_outcome_class == baseline_governed_result.run_outcome_class
    assert governed_result.reason == baseline_governed_result.reason


def test_run_section_with_workspace_incremental_v2_1_persists_additive_preview_summary(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_1_preview")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(project_root, [])

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_1")
    preview = json.loads((project_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8"))

    assert result.run_mode == "incremental_v2_1"
    assert result.run_outcome_class == "preview_incremental_v2_1"
    assert preview["mode"] == "incremental_v2_1"
    assert isinstance(preview["summary"], dict)
    assert isinstance(preview["preview_outcome_class"], str)
    assert isinstance(preview["recommended_mode"], str)
    assert isinstance(preview["previews"], list)


def test_run_section_with_workspace_incremental_v2_2_writes_review_under_dce_only(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_2_review_artifact")
    _write_text(project_root / "mission_board" / "service.py", "existing-service")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")
    _write_ownership_index(project_root, [])

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
    review = review_path.read_text(encoding="utf-8")

    assert result.run_mode == "incremental_v2_2"
    assert result.run_outcome_class == "review_incremental_v2_2"
    assert result.written_files == []
    assert review_path.exists()
    assert (project_root / "api" / "missionboardservice.py").exists() is False
    assert (project_root / "mission_board" / "models.py").exists() is False
    assert (project_root / "models" / "mission.py").exists() is False
    assert (project_root / "mission_board" / "service.py").read_text(encoding="utf-8") == "existing-service"
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert "# Section Review: mission-board" in review
    assert "- Mode: incremental_v2_2" in review
    assert "## Summary" in review
    assert "## Create candidates" in review
    assert "## Blocked candidates" in review
    assert "# Generated by Aether" not in review
    assert "existing-service" not in review


def test_run_section_with_workspace_incremental_v2_2_review_explains_code_graph_guidance_and_fallback(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    guided_root = _workspace_dir("dgce_incremental_v2_2_review_guided_reasoning")
    _write_text(guided_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(guided_root, [])
    run_section_with_workspace(
        _section().model_copy(update={"code_graph_context": _ranked_code_graph_context()}),
        guided_root,
        incremental_mode="incremental_v2_2",
    )
    guided_review = (guided_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")

    fallback_root = _workspace_dir("dgce_incremental_v2_2_review_fallback_reasoning")
    _write_text(fallback_root / "mission_board" / "service.py", "existing-service")
    _write_ownership_index(fallback_root, [])
    run_section_with_workspace(
        _section().model_copy(
            update={"code_graph_context": {"contract_name": "DefiantCodeGraphFacts", "contract_version": "wrong", "graph_id": "graph:bad"}}
        ),
        fallback_root,
        incremental_mode="incremental_v2_2",
    )
    fallback_review = (fallback_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")

    assert "- Planning basis: code_graph_guided" in guided_review
    assert "strategy: bounded_insert" in guided_review
    assert "collision: likely_safe_extension" in guided_review
    assert "preferred bounded_insert over contract guidance rewrite_small_region" in guided_review
    assert "- Planning basis: code_graph_fallback" in fallback_review
    assert "- Planning fallback: facts_malformed" in fallback_review
    assert "baseline planning" in fallback_review


def test_run_section_with_workspace_incremental_v2_2_review_is_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_2_review_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_2_review_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), first_root, incremental_mode="incremental_v2_2")
    run_section_with_workspace(_section(), second_root, incremental_mode="incremental_v2_2")

    first_review = (first_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")
    second_review = (second_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")

    assert first_review == second_review


def test_run_section_with_workspace_incremental_v2_2_writes_review_index_and_workspace_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_2_review_index")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert review_index["section_order"] == ["alpha-section", "mission-board"]
    assert review_index["summary"] == {
        "sections_with_approval": 0,
        "sections_with_execution": 0,
        "sections_with_outputs": 0,
        "sections_with_review": 1,
        "total_sections_seen": 2,
    }
    assert [entry["section_id"] for entry in review_index["sections"]] == ["alpha-section", "mission-board"]
    assert [entry["entry_order"] for entry in review_index["sections"]] == [1, 2]
    assert review_index["sections"][0]["preview_path"] == ".dce/plans/alpha-section.preview.json"
    assert review_index["sections"][0]["review_path"] is None
    assert review_index["sections"][0]["approval_path"] is None
    assert review_index["sections"][0]["review_status"] is None
    assert review_index["sections"][0]["lifecycle_trace_path"] == ".dce/lifecycle_trace.json"
    assert review_index["sections"][1]["preview_path"] == ".dce/plans/mission-board.preview.json"
    assert review_index["sections"][1]["review_path"] == ".dce/reviews/mission-board.review.md"
    assert review_index["sections"][1]["approval_path"] is None
    assert review_index["sections"][1]["review_status"] == "review_available"
    assert review_index["sections"][1]["review_approval_summary"] == {
        "approval_status": None,
        "decision_source": "preview_recommendation",
        "latest_decision": review_index["sections"][1]["latest_decision"],
        "latest_decision_source": review_index["sections"][1]["latest_decision_source"],
        "review_status": "review_available",
    }
    assert workspace_summary["total_sections_seen"] == 2
    assert [entry["section_id"] for entry in workspace_summary["sections"]] == ["alpha-section", "mission-board"]
    assert workspace_summary["sections"][0]["preview_path"] == ".dce/plans/alpha-section.preview.json"
    assert workspace_summary["sections"][0]["review_path"] is None
    assert workspace_summary["sections"][0]["preview_outcome_class"] == "preview_create_only"
    assert workspace_summary["sections"][0]["recommended_mode"] == "create_only"
    assert workspace_summary["sections"][0]["approval_path"] is None
    assert workspace_summary["sections"][0]["approval_status"] is None
    assert workspace_summary["sections"][0]["selected_mode"] is None
    assert workspace_summary["sections"][0]["execution_permitted"] is None
    assert workspace_summary["sections"][0]["preflight_path"] is None
    assert workspace_summary["sections"][0]["preflight_status"] is None
    assert workspace_summary["sections"][0]["stale_check_path"] is None
    assert workspace_summary["sections"][0]["stale_status"] is None
    assert workspace_summary["sections"][0]["stale_detected"] is None
    assert workspace_summary["sections"][0]["execution_allowed"] is None
    assert workspace_summary["sections"][0]["execution_gate_path"] is None
    assert workspace_summary["sections"][0]["gate_status"] is None
    assert workspace_summary["sections"][0]["execution_blocked"] is None
    assert workspace_summary["sections"][0]["alignment_path"] is None
    assert workspace_summary["sections"][0]["alignment_status"] is None
    assert workspace_summary["sections"][0]["alignment_blocked"] is None
    assert workspace_summary["sections"][0]["execution_path"] is None
    assert workspace_summary["sections"][0]["execution_status"] is None
    assert workspace_summary["sections"][0]["approval_consumed"] is None
    assert workspace_summary["sections"][0]["approval_status_after"] is None
    assert workspace_summary["sections"][1]["preview_path"] == ".dce/plans/mission-board.preview.json"
    assert workspace_summary["sections"][1]["review_path"] == ".dce/reviews/mission-board.review.md"
    assert workspace_summary["sections"][1]["preview_outcome_class"] == "preview_create_only"
    assert workspace_summary["sections"][1]["recommended_mode"] == "create_only"
    assert workspace_summary["sections"][1]["approval_path"] is None
    assert workspace_summary["sections"][1]["approval_status"] is None
    assert workspace_summary["sections"][1]["selected_mode"] is None
    assert workspace_summary["sections"][1]["execution_permitted"] is None
    assert workspace_summary["sections"][1]["preflight_path"] is None
    assert workspace_summary["sections"][1]["preflight_status"] is None
    assert workspace_summary["sections"][1]["stale_check_path"] is None
    assert workspace_summary["sections"][1]["stale_status"] is None
    assert workspace_summary["sections"][1]["stale_detected"] is None
    assert workspace_summary["sections"][1]["execution_allowed"] is None
    assert workspace_summary["sections"][1]["execution_gate_path"] is None
    assert workspace_summary["sections"][1]["gate_status"] is None
    assert workspace_summary["sections"][1]["execution_blocked"] is None
    assert workspace_summary["sections"][1]["alignment_path"] is None
    assert workspace_summary["sections"][1]["alignment_status"] is None
    assert workspace_summary["sections"][1]["alignment_blocked"] is None
    assert workspace_summary["sections"][1]["execution_path"] is None
    assert workspace_summary["sections"][1]["execution_status"] is None
    assert workspace_summary["sections"][1]["approval_consumed"] is None
    assert workspace_summary["sections"][1]["approval_status_after"] is None
    assert (project_root / "api" / "missionboardservice.py").exists() is False
    assert (project_root / "alpha_section" / "service.py").exists() is False


def test_run_section_with_workspace_review_index_is_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_review_index_repeat_a")
    second_root = _workspace_dir("dgce_incremental_review_index_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), first_root, incremental_mode="incremental_v2_2")
    run_section_with_workspace(_section_named("Alpha Section"), first_root, incremental_mode="incremental_v2")
    run_section_with_workspace(_section_named("Mission Board"), second_root, incremental_mode="incremental_v2_2")
    run_section_with_workspace(_section_named("Alpha Section"), second_root, incremental_mode="incremental_v2")

    first_index = (first_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8")
    second_index = (second_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8")
    first_summary = (first_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8")
    second_summary = (second_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8")
    first_trace = (first_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8")
    second_trace = (second_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8")

    assert first_index == second_index
    assert first_summary == second_summary
    assert first_trace == second_trace

    payload = json.loads(first_index)
    assert payload["section_order"] == ["alpha-section", "mission-board"]
    assert payload["summary"] == {
        "sections_with_approval": 0,
        "sections_with_execution": 0,
        "sections_with_outputs": 0,
        "sections_with_review": 1,
        "total_sections_seen": 2,
    }
    assert [entry["entry_order"] for entry in payload["sections"]] == [1, 2]
    assert payload["sections"][0]["navigation_links"] == [
        {"link_role": "preview", "path": ".dce/plans/alpha-section.preview.json"},
        {"link_role": "review", "path": None},
        {"link_role": "approval", "path": None},
        {"link_role": "lifecycle_trace", "path": ".dce/lifecycle_trace.json"},
        {"link_role": "execution", "path": None},
        {"link_role": "outputs", "path": None},
    ]
    assert payload["sections"][1]["navigation_links"] == [
        {"link_role": "preview", "path": ".dce/plans/mission-board.preview.json"},
        {"link_role": "review", "path": ".dce/reviews/mission-board.review.md"},
        {"link_role": "approval", "path": None},
        {"link_role": "lifecycle_trace", "path": ".dce/lifecycle_trace.json"},
        {"link_role": "execution", "path": None},
        {"link_role": "outputs", "path": None},
    ]
    assert payload["sections"][1]["review_approval_summary"] == {
        "approval_status": None,
        "decision_source": "preview_recommendation",
        "latest_decision": "create_only",
        "latest_decision_source": "preview_recommendation",
        "review_status": "review_available",
    }


def test_review_index_emits_deterministic_review_approval_navigation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v3_3_review_approval_nav_a")
    second_root = _workspace_dir("dgce_incremental_v3_3_review_approval_nav_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section_named("Mission Board"), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section_named("Mission Board"),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )
        run_section_with_workspace(_section_named("Alpha Section"), root, incremental_mode="incremental_v2")

    first_index = json.loads((first_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    second_index = json.loads((second_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))

    assert first_index == second_index
    assert (first_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "reviews" / "index.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "workspace_summary.json"
    ).read_text(encoding="utf-8")

    assert first_index["section_order"] == ["alpha-section", "mission-board"]
    assert [entry["section_id"] for entry in first_index["sections"]] == ["alpha-section", "mission-board"]
    assert [entry["entry_order"] for entry in first_index["sections"]] == [1, 2]
    assert first_index["summary"] == {
        "sections_with_approval": 1,
        "sections_with_execution": 1,
        "sections_with_outputs": 1,
        "sections_with_review": 1,
        "total_sections_seen": 2,
    }

    alpha_entry = first_index["sections"][0]
    mission_entry = first_index["sections"][1]

    assert alpha_entry["review_status"] is None
    assert alpha_entry["approval_status"] is None
    assert alpha_entry["latest_decision"] == "review_required"
    assert alpha_entry["latest_decision_source"] == "preview_recommendation"
    assert alpha_entry["navigation_links"] == [
        {"link_role": "preview", "path": ".dce/plans/alpha-section.preview.json"},
        {"link_role": "review", "path": None},
        {"link_role": "approval", "path": None},
        {"link_role": "lifecycle_trace", "path": ".dce/lifecycle_trace.json"},
        {"link_role": "execution", "path": None},
        {"link_role": "outputs", "path": None},
    ]

    assert mission_entry["review_status"] == "review_available"
    assert mission_entry["approval_status"] == "superseded"
    assert mission_entry["latest_decision"] == "create_only"
    assert mission_entry["latest_decision_source"] == "approval"
    assert mission_entry["approval_timestamp"] == "2026-03-26T00:00:00Z"
    assert mission_entry["review_approval_summary"] == {
        "approval_status": "superseded",
        "decision_source": "approval",
        "latest_decision": "create_only",
        "latest_decision_source": "approval",
        "review_status": "review_available",
    }
    assert mission_entry["navigation_links"] == [
        {"link_role": "preview", "path": ".dce/plans/mission-board.preview.json"},
        {"link_role": "review", "path": ".dce/reviews/mission-board.review.md"},
        {"link_role": "approval", "path": ".dce/approvals/mission-board.approval.json"},
        {"link_role": "lifecycle_trace", "path": ".dce/lifecycle_trace.json"},
        {"link_role": "execution", "path": ".dce/execution/mission-board.execution.json"},
        {"link_role": "outputs", "path": ".dce/outputs/mission-board.json"},
    ]


def test_record_section_approval_derives_execution_permitted_correctly(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_4_permission_matrix")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is True
    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is True
    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="no_changes", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is True
    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="review_required", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is False
    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="pending", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is False
    assert record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="rejected", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )["execution_permitted"] is False


def test_record_section_approval_writes_artifact_and_updates_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_4_approval_artifact")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="safe_modify",
            approval_source="manual",
            approved_by="operator",
            approval_timestamp="2026-03-26T00:00:00Z",
            notes="Approved for safe modify after preview review.",
        ),
    )
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert approval_path.exists()
    assert approval == json.loads(approval_path.read_text(encoding="utf-8"))
    assert approval["section_id"] == "mission-board"
    assert approval["approval_status"] == "approved"
    assert approval["selected_mode"] == "safe_modify"
    assert approval["execution_permitted"] is True
    assert approval["input_path"] == ".dce/input/mission-board.json"
    assert approval["preview_path"] == ".dce/plans/mission-board.preview.json"
    assert approval["review_path"] == ".dce/reviews/mission-board.review.md"
    from aether.dgce.decompose import compute_review_artifact_fingerprint
    assert approval["input_fingerprint"] == compute_json_payload_fingerprint(
        json.loads((project_root / ".dce" / "input" / "mission-board.json").read_text(encoding="utf-8"))
    )
    assert approval["preview_fingerprint"] == json.loads(
        (project_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8")
    )["artifact_fingerprint"]
    assert approval["review_fingerprint"] == compute_review_artifact_fingerprint(
        (project_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")
    )
    assert approval["preview_outcome_class"] == "preview_create_only"
    assert approval["recommended_mode"] == "create_only"
    assert approval["approval_source"] == "manual"
    assert approval["approved_by"] == "operator"
    assert approval["approval_timestamp"] == "2026-03-26T00:00:00Z"
    assert approval["notes"] == "Approved for safe modify after preview review."
    assert isinstance(approval["artifact_fingerprint"], str)
    assert review_index["section_order"] == ["mission-board"]
    assert review_index["summary"] == {
        "sections_with_approval": 1,
        "sections_with_execution": 0,
        "sections_with_outputs": 0,
        "sections_with_review": 1,
        "total_sections_seen": 1,
    }
    assert review_index["sections"][0]["preview_path"] == ".dce/plans/mission-board.preview.json"
    assert review_index["sections"][0]["review_path"] == ".dce/reviews/mission-board.review.md"
    assert review_index["sections"][0]["approval_path"] == ".dce/approvals/mission-board.approval.json"
    assert review_index["sections"][0]["approval_status"] == "approved"
    assert review_index["sections"][0]["selected_mode"] == "safe_modify"
    assert review_index["sections"][0]["execution_permitted"] is True
    assert review_index["sections"][0]["approval_timestamp"] == "2026-03-26T00:00:00Z"
    assert review_index["sections"][0]["review_status"] == "review_available"
    assert review_index["sections"][0]["review_approval_summary"] == {
        "approval_status": "approved",
        "decision_source": "approval",
        "latest_decision": "safe_modify",
        "latest_decision_source": "approval",
        "review_status": "review_available",
    }
    assert review_index["sections"][0]["navigation_links"] == [
        {"link_role": "preview", "path": ".dce/plans/mission-board.preview.json"},
        {"link_role": "review", "path": ".dce/reviews/mission-board.review.md"},
        {"link_role": "approval", "path": ".dce/approvals/mission-board.approval.json"},
        {"link_role": "lifecycle_trace", "path": ".dce/lifecycle_trace.json"},
        {"link_role": "execution", "path": None},
        {"link_role": "outputs", "path": None},
    ]
    assert workspace_summary == {
        **_expected_artifact_metadata("workspace_summary"),
        "total_sections_seen": 1,
        "sections": [
            {
                "section_id": "mission-board",
                "latest_run_mode": None,
                "latest_run_outcome_class": None,
                "latest_status": None,
                "latest_validation_ok": None,
                "latest_advisory_type": None,
                "latest_advisory_explanation": None,
                "latest_written_files_count": 0,
                "latest_skipped_modify_count": 0,
                "latest_skipped_ignore_count": 0,
                "preview_path": ".dce/plans/mission-board.preview.json",
                "review_path": ".dce/reviews/mission-board.review.md",
                "preview_outcome_class": "preview_create_only",
                "recommended_mode": "create_only",
                "approval_path": ".dce/approvals/mission-board.approval.json",
                "approval_status": "approved",
                "selected_mode": "safe_modify",
                "execution_permitted": True,
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
                "execution_path": None,
                "execution_status": None,
                "approval_consumed": None,
                "approval_status_after": None,
                "decision_source": "approval",
                "review_status": "review_available",
                "latest_decision": "safe_modify",
                "latest_decision_source": "approval",
                "latest_stage": "approval",
                "latest_stage_status": "approved",
                "section_summary": _expected_section_summary(
                    section_id="mission-board",
                    approval_status="approved",
                    decision_source="approval",
                    latest_decision="safe_modify",
                    latest_stage="approval",
                    latest_stage_status="approved",
                    review_status="review_available",
                ),
            }
        ],
    }
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert (project_root / "api" / "missionboardservice.py").exists() is False


def test_record_section_approval_is_deterministic_with_fixed_inputs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_4_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_4_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    fixed_input = SectionApprovalInput(
        approval_status="approved",
        selected_mode="create_only",
        approval_source="system_seeded",
        approved_by="system",
        approval_timestamp="2026-03-26T00:00:00Z",
        notes="Seeded deterministic approval.",
    )

    run_section_with_workspace(_section(), first_root, incremental_mode="incremental_v2")
    run_section_with_workspace(_section(), second_root, incremental_mode="incremental_v2")
    record_section_approval(first_root, "mission-board", fixed_input)
    record_section_approval(second_root, "mission-board", fixed_input)

    assert (first_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "approvals" / "mission-board.approval.json"
    ).read_text(encoding="utf-8")


def test_persisted_artifacts_include_fingerprint_fields(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_artifact_fields")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    preflight = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    preview_payload = json.loads((project_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8"))
    approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
    preflight_payload = json.loads((project_root / ".dce" / "preflight" / "mission-board.preflight.json").read_text(encoding="utf-8"))
    review_text = (project_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")

    assert isinstance(preview_payload["artifact_fingerprint"], str)
    assert "- artifact_fingerprint: " in review_text
    assert isinstance(approval["artifact_fingerprint"], str)
    assert isinstance(preflight["artifact_fingerprint"], str)
    from aether.dgce.decompose import verify_artifact_fingerprint, verify_review_artifact_fingerprint
    assert verify_artifact_fingerprint(project_root / ".dce" / "plans" / "mission-board.preview.json") is True
    assert verify_artifact_fingerprint(project_root / ".dce" / "approvals" / "mission-board.approval.json") is True
    assert verify_artifact_fingerprint(project_root / ".dce" / "preflight" / "mission-board.preflight.json") is True
    assert verify_review_artifact_fingerprint(project_root / ".dce" / "reviews" / "mission-board.review.md") is True
    assert preview_payload["artifact_fingerprint"] == compute_preview_payload_fingerprint(preview_payload)
    assert approval_payload["artifact_fingerprint"] == compute_json_payload_fingerprint(approval_payload)
    assert preflight_payload["artifact_fingerprint"] == compute_json_payload_fingerprint(preflight_payload)


def test_record_section_approval_uses_same_preview_fingerprint_as_preview_artifact(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_preview_approval_fingerprint_match")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    preview_payload = json.loads((project_root / ".dce" / "plans" / "mission-board.preview.json").read_text(encoding="utf-8"))

    assert approval["preview_fingerprint"] == preview_payload["artifact_fingerprint"]


def test_record_section_approval_reads_latest_preview_artifact_from_disk(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_approval_reads_latest_preview")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
    preview_payload["preview_outcome_class"] = "preview_safe_modify_ready"
    preview_payload["recommended_mode"] = "safe_modify"
    preview_payload["artifact_fingerprint"] = compute_preview_payload_fingerprint(preview_payload)
    preview_path.write_text(json.dumps(preview_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    assert approval["preview_fingerprint"] == preview_payload["artifact_fingerprint"]
    assert approval["preview_outcome_class"] == "preview_safe_modify_ready"
    assert approval["recommended_mode"] == "safe_modify"


def test_record_section_stale_check_invalidates_on_preview_fingerprint_mismatch(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_stale_preview_fingerprint")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
    preview_payload["artifact_fingerprint"] = "mismatched-preview-fingerprint"
    preview_path.write_text(json.dumps(preview_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert stale["stale_status"] == "stale_invalidated"
    assert stale["stale_detected"] is True
    assert stale["stale_reason"] == "approval_preview_fingerprint_mismatch"


def test_record_section_stale_check_passes_when_fingerprints_match(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_stale_valid_fingerprint")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert stale["stale_status"] == "stale_valid"
    assert stale["stale_detected"] is False
    assert stale["approval_input_path"] == ".dce/input/mission-board.json"
    assert stale["current_input_path"] == ".dce/input/mission-board.json"
    assert stale["approval_input_fingerprint"] == stale["current_input_fingerprint"]
    assert stale["approval_review_fingerprint"] == stale["current_review_fingerprint"]


def test_record_section_stale_check_uses_stored_preview_artifact_fingerprint(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stale_uses_stored_preview_fingerprint")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
    stored_fingerprint = preview_payload["artifact_fingerprint"]
    preview_payload["summary"]["total_targets"] = 999
    preview_payload["artifact_fingerprint"] = stored_fingerprint
    preview_path.write_text(json.dumps(preview_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    assert stale["approval_preview_fingerprint"] == stored_fingerprint
    assert stale["current_preview_fingerprint"] == stored_fingerprint
    assert stale["stale_status"] == "stale_valid"
    assert stale["stale_detected"] is False


def test_record_section_stale_check_invalidates_on_review_fingerprint_mismatch(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stale_review_fingerprint_mismatch")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_fingerprint"] = "mismatched-review-fingerprint"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    assert stale["approval_review_fingerprint"] == "mismatched-review-fingerprint"
    assert stale["current_review_fingerprint"] == dgce_decompose.compute_review_artifact_fingerprint(
        (project_root / ".dce" / "reviews" / "mission-board.review.md").read_text(encoding="utf-8")
    )
    assert stale["stale_status"] == "stale_invalidated"
    assert stale["stale_detected"] is True
    assert stale["stale_reason"] == "approval_review_fingerprint_mismatch"


def test_record_section_stale_check_invalidates_on_input_fingerprint_mismatch(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_1_stale_input_fingerprint")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    input_path = project_root / ".dce" / "input" / "mission-board.json"
    input_payload = json.loads(input_path.read_text(encoding="utf-8"))
    input_payload["requirements"].append("require operator ack")
    input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert stale["stale_status"] == "stale_invalidated"
    assert stale["stale_detected"] is True
    assert stale["stale_reason"] == "approval_input_fingerprint_mismatch"
    assert stale["approval_input_path"] == ".dce/input/mission-board.json"
    assert stale["current_input_path"] == ".dce/input/mission-board.json"


def test_record_section_stale_check_is_backward_compatible_without_fingerprints(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_stale_backcompat")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    approval.pop("preview_fingerprint", None)
    approval.pop("review_fingerprint", None)
    approval.pop("input_fingerprint", None)
    (project_root / ".dce" / "approvals" / "mission-board.approval.json").write_text(
        json.dumps(approval, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    stale = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert stale["stale_status"] == "stale_valid"
    assert stale["stale_detected"] is False


def test_verify_artifact_fingerprint_fails_after_json_payload_mutation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_verify_invalidated")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    from aether.dgce.decompose import verify_artifact_fingerprint

    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))

    assert verify_artifact_fingerprint(preview_path) is True
    preview_payload["summary"]["total_targets"] = 42
    preview_path.write_text(json.dumps(preview_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    assert verify_artifact_fingerprint(preview_path) is False


def test_verify_review_artifact_fingerprint_fails_after_review_content_mutation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_review_verify_invalidated")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    from aether.dgce.decompose import verify_artifact_fingerprint, verify_review_artifact_fingerprint

    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"

    assert verify_review_artifact_fingerprint(review_path) is True
    assert verify_artifact_fingerprint(preview_path) is True
    review_path.write_text(review_path.read_text(encoding="utf-8").replace("- Total targets: 4", "- Total targets: 9"), encoding="utf-8")
    assert verify_review_artifact_fingerprint(review_path) is False
    assert verify_artifact_fingerprint(preview_path) is True


def test_run_dgce_section_ungoverned_behaves_unchanged(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_ungoverned")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    direct = run_section_with_workspace(_section(), project_root)
    second_root = _workspace_dir("dgce_productized_ungoverned_orchestrated")
    _write_section_input(second_root, _section())
    orchestrated = run_dgce_section("mission-board", second_root, governed=False)

    assert direct.run_outcome_class == "success_create_only"
    assert orchestrated.status == "success"
    assert orchestrated.reason == direct.run_outcome_class
    assert orchestrated.run_outcome_class == direct.run_outcome_class
    assert orchestrated.artifact_paths["output_path"] == ".dce/outputs/mission-board.json"


def test_run_dgce_section_governed_requires_approval(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_approval_required")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    result = run_dgce_section("mission-board", project_root, governed=True)

    assert result.status == "approval_required"
    assert result.reason == "missing_approval"
    assert result.artifact_paths["preview_path"] == ".dce/plans/mission-board.preview.json"
    assert result.artifact_paths["review_path"] == ".dce/reviews/mission-board.review.md"
    assert result.artifact_paths["approval_path"] is None
    assert result.artifact_paths["execution_path"] is None
    assert result.artifact_paths["output_path"] is None


def test_run_dgce_section_uses_explicit_section_id_not_title_for_cli_match(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_explicit_section_id_match")
    section = DGCESection(
        section_id="data-model",
        section_type="data_model",
        title="DGCE Core Data Model",
        description="Governed data model section.",
        requirements=["define section input artifacts"],
        constraints=["keep output deterministic"],
    )
    _write_section_input(project_root, section)

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    result = run_dgce_section("data-model", project_root, governed=False)

    assert result.status == "success"
    assert result.reason == result.run_outcome_class
    assert result.artifact_paths["input_path"] == ".dce/input/data-model.json"
    assert result.artifact_paths["output_path"] == ".dce/outputs/data-model.json"


def test_run_dgce_section_governed_valid_approval_succeeds(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_governed_success")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_dgce_section("mission-board", project_root, governed=True)

    assert result.status == "success"
    assert result.reason == "success_create_only"
    assert result.run_outcome_class == "success_create_only"
    assert result.artifact_paths["approval_path"] == ".dce/approvals/mission-board.approval.json"
    assert result.artifact_paths["stale_check_path"] == ".dce/preflight/mission-board.stale_check.json"
    assert result.artifact_paths["preflight_path"] == ".dce/preflight/mission-board.preflight.json"
    assert result.artifact_paths["execution_gate_path"] == ".dce/execution/gate/mission-board.execution_gate.json"
    assert result.artifact_paths["alignment_path"] == ".dce/execution/alignment/mission-board.alignment.json"
    assert result.artifact_paths["execution_path"] == ".dce/execution/mission-board.execution.json"
    assert result.artifact_paths["output_path"] == ".dce/outputs/mission-board.json"


def test_run_dgce_section_governed_stale_blocks(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_governed_stale")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_path"] = ".dce/reviews/other.review.md"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = run_dgce_section("mission-board", project_root, governed=True)

    assert result.status == "blocked"
    assert result.reason == "blocked_stale"
    assert result.run_outcome_class == "blocked_stale"
    assert result.artifact_paths["execution_path"] == ".dce/execution/mission-board.execution.json"


def test_run_dgce_section_governed_blocks_when_input_fingerprint_mismatches(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_governed_input_stale")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    input_path = project_root / ".dce" / "input" / "mission-board.json"
    input_payload = json.loads(input_path.read_text(encoding="utf-8"))
    input_payload["constraints"].append("operator input changed after approval")
    input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = run_dgce_section("mission-board", project_root, governed=True)
    stale_payload = json.loads((project_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8"))
    gate_payload = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").read_text(encoding="utf-8"))

    assert result.status == "blocked"
    assert result.reason == "blocked_stale"
    assert result.run_outcome_class == "blocked_stale"
    assert stale_payload["stale_reason"] == "approval_input_fingerprint_mismatch"
    assert gate_payload["gate_status"] == "gate_blocked_stale"


def test_run_dgce_section_governed_alignment_mismatch_blocks(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_governed_alignment")
    _write_section_input(project_root, _section())

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="no_changes", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_dgce_section("mission-board", project_root, governed=True)

    assert result.status == "success"
    assert result.reason == "success_create_only"
    assert result.run_outcome_class == "success_create_only"
    assert result.artifact_paths["alignment_path"] == ".dce/execution/alignment/mission-board.alignment.json"


def test_run_dgce_section_governed_safe_modify_succeeds(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_governed_safe_modify")
    _write_section_input(project_root, _section())
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {
                            "path": "api/missionboardservice.py",
                            "language": "python",
                            "purpose": "API surface",
                            "content": "stale-content",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_index(project_root, [{"path": "api/missionboardservice.py", "section_id": section_id}])
    _write_text(project_root / "api" / "missionboardservice.py", "old-content")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_dgce_section(section_id, project_root, governed=True)

    assert result.status == "success"
    assert result.reason == "success_safe_modify"
    assert result.run_outcome_class == "success_safe_modify"
    assert result.artifact_paths["execution_path"] == ".dce/execution/mission-board.execution.json"


def test_run_dgce_section_governed_system_breakdown_accepts_rich_contract_and_persists_enriched_fallback(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_productized_system_breakdown_rich_contract")
    section = DGCESection(
        section_type="system_breakdown",
        title="System Breakdown",
        description="Define the DGCE system as implementation-ready modules and a deterministic build graph.",
        requirements=[
            "Define implementation-ready module contracts",
            "Make build order explicit",
        ],
        constraints=[
            "Do not change DGCE governance",
            "Avoid narrative-only output",
        ],
        expected_targets=[
            "aether/dgce/decompose.py",
            "aether/dgce/incremental.py",
            "dce.py",
        ],
    )
    section_id = "system-breakdown"
    _write_section_input(project_root, section)
    _write_ownership_index(
        project_root,
        [{"path": path, "section_id": section_id} for path in section.expected_targets],
    )
    for path in section.expected_targets:
        _write_text(project_root / path, "existing governed target\n")

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "plan the system breakdown" in lowered:
            output = json.dumps(
                {
                    "modules": [
                        {
                            "name": "SectionInputHandler",
                            "layer": "application",
                            "responsibility": "accept, validate, fingerprint, and persist section input artifacts",
                            "inputs": [
                                {
                                    "name": "raw_section_input",
                                    "type": "SectionInputRequest",
                                    "schema_fields": [
                                        {"name": "section_id", "type": "string", "required": True},
                                        {"name": "title", "type": "string", "required": True},
                                        {"name": "acceptance_criteria", "type": "array", "items": "string", "required": False},
                                    ],
                                }
                            ],
                            "outputs": [
                                {"name": "section_input_record", "type": "SectionInput"},
                                {
                                    "name": "section_input_artifact",
                                    "type": "artifact",
                                    "artifact_path": ".dce/input/{section_id}.json",
                                },
                            ],
                            "dependencies": [],
                            "governance_touchpoints": ["input_capture"],
                            "failure_modes": ["input_validation_failed"],
                            "owned_paths": ["aether/dgce/decompose.py"],
                            "implementation_order": 1,
                        },
                        {
                            "name": "StaleCheckWriter",
                            "layer": "governance",
                            "responsibility": "evaluate stale state and persist a distinct stale-check artifact",
                            "inputs": [
                                {
                                    "name": "section_input_artifact",
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
                                {"name": "section_input_handler", "kind": "module", "reference": "aether/dgce/decompose.py"}
                            ],
                            "governance_touchpoints": ["stale_check"],
                            "failure_modes": ["stale_check_failed"],
                            "owned_paths": [
                                "aether/dgce/incremental.py",
                                ".dce/preflight/{section_id}.stale_check.json",
                            ],
                            "implementation_order": 2,
                        },
                        {
                            "name": "ExecutionCoordinator",
                            "layer": "application",
                            "responsibility": "coordinate governed execution and record execution artifacts",
                            "inputs": [
                                {
                                    "name": "approval_request",
                                    "type": "ApprovalRequest",
                                    "schema_fields": [
                                        {"name": "section_id", "type": "string", "required": True},
                                        {"name": "approval_status", "type": "string", "required": True},
                                    ],
                                },
                                {
                                    "name": "stale_check_artifact",
                                    "type": "artifact",
                                    "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
                                },
                            ],
                            "outputs": [
                                {"name": "execution_stamp", "type": "ExecutionStamp"},
                            ],
                            "dependencies": [
                                {"name": "stale_check_writer", "kind": "module", "reference": "aether/dgce/incremental.py"}
                            ],
                            "governance_touchpoints": ["approval", "execution"],
                            "failure_modes": ["execution_blocked"],
                            "owned_paths": ["dce.py"],
                            "implementation_order": 3,
                        },
                    ],
                    "build_graph": {
                        "type": "directed_acyclic_graph",
                        "edges": [
                            ["SectionInputHandler", "StaleCheckWriter"],
                            ["StaleCheckWriter", "ExecutionCoordinator"],
                        ],
                    },
                    "tests": [
                        {
                            "name": "stale_check_path_is_owned",
                            "purpose": "Verify stale-check ownership stays explicit.",
                            "targets": ["StaleCheckWriter"],
                        }
                    ],
                }
            )
        elif "implement a data model class" in lowered:
            output = json.dumps(
                {
                    "entities": ["SectionInput"],
                    "fields": ["section_id", "artifact_fingerprint"],
                    "relationships": ["section_input->execution_stamp"],
                    "validation_rules": ["section_id required"],
                }
            )
        elif "implement an api surface" in lowered:
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
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="safe_modify",
            approval_timestamp="2026-03-29T00:00:00Z",
        ),
    )

    result = run_section_with_workspace(
        section,
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        preflight_validation_timestamp="2026-03-29T00:00:00Z",
        gate_timestamp="2026-03-29T00:00:00Z",
        alignment_timestamp="2026-03-29T00:00:00Z",
        execution_timestamp="2026-03-29T00:00:00Z",
    )
    output_payload = json.loads((project_root / ".dce" / "outputs" / f"{section_id}.json").read_text(encoding="utf-8"))
    execution_payload = json.loads((project_root / ".dce" / "execution" / f"{section_id}.execution.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class != "validation_failure"
    assert output_payload["execution_outcome"]["validation_summary"] == {
        "ok": True,
        "error": None,
        "missing_keys": [],
    }
    assert output_payload["file_plan"]["files"] == [
        {
            "path": "aether/dgce/decompose.py",
            "purpose": "System-breakdown orchestration and contract rendering",
            "requirements": [
                "Define implementation-ready module contracts",
                "Make build order explicit",
                "Module contracts: SectionInputHandler, StaleCheckWriter, ExecutionCoordinator",
                "Build graph: SectionInputHandler->StaleCheckWriter, StaleCheckWriter->ExecutionCoordinator",
                "Verification: stale_check_path_is_owned",
            ],
            "source": "expected_targets",
        },
        {
            "path": "aether/dgce/incremental.py",
            "purpose": "System-breakdown target grounding and change planning",
            "requirements": [
                "Define implementation-ready module contracts",
                "Make build order explicit",
                "Module contracts: SectionInputHandler, StaleCheckWriter, ExecutionCoordinator",
                "Build graph: SectionInputHandler->StaleCheckWriter, StaleCheckWriter->ExecutionCoordinator",
                "Verification: stale_check_path_is_owned",
            ],
            "source": "expected_targets",
        },
        {
            "path": "dce.py",
            "purpose": "System-breakdown CLI orchestration entrypoint",
            "requirements": [
                "Define implementation-ready module contracts",
                "Make build order explicit",
                "Module contracts: SectionInputHandler, StaleCheckWriter, ExecutionCoordinator",
                "Build graph: SectionInputHandler->StaleCheckWriter, StaleCheckWriter->ExecutionCoordinator",
                "Verification: stale_check_path_is_owned",
            ],
            "source": "expected_targets",
        },
    ]
    assert execution_payload["run_outcome_class"] == result.run_outcome_class
    assert execution_payload["execution_status"] in {"execution_completed", "execution_completed_no_changes"}


def test_record_section_preflight_truth_table_and_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_5_preflight_truth_table")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    missing_approval = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_approval["preflight_status"] == "preflight_missing_approval"
    assert missing_approval["execution_allowed"] is False
    assert missing_approval["preflight_reason"] == "missing_approval"

    approval = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="safe_modify",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
    review_text = review_path.read_text(encoding="utf-8")
    review_path.unlink()
    missing_review = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_review["preflight_status"] == "preflight_missing_review"
    assert missing_review["preflight_reason"] == "missing_review"
    review_path.write_text(review_text, encoding="utf-8")

    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_text = preview_path.read_text(encoding="utf-8")
    preview_path.unlink()
    missing_preview = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_preview["preflight_status"] == "preflight_missing_preview"
    assert missing_preview["preflight_reason"] == "missing_preview"
    preview_path.write_text(preview_text, encoding="utf-8")

    invalid_linkage_payload = dict(approval)
    invalid_linkage_payload["preview_path"] = ".dce/plans/other.preview.json"
    (project_root / ".dce" / "approvals" / "mission-board.approval.json").write_text(
        json.dumps(invalid_linkage_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    invalid_linkage = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert invalid_linkage["preflight_status"] == "preflight_invalid_linkage"
    assert invalid_linkage["preflight_reason"] == "approval_preview_path_mismatch"
    _write_text(project_root / ".dce" / "approvals" / "mission-board.approval.json", json.dumps(approval, indent=2, sort_keys=True) + "\n")

    permitted_pass = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert permitted_pass["preflight_status"] == "preflight_pass"
    assert permitted_pass["execution_allowed"] is True
    assert permitted_pass["preflight_reason"] == "approved_and_linked"

    not_permitted = record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="review_required",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    execution_not_permitted = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert not_permitted["execution_permitted"] is False
    assert execution_not_permitted["preflight_status"] == "preflight_execution_not_permitted"
    assert execution_not_permitted["preflight_reason"] == "approval_not_permitted"

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="rejected",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    rejected = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert rejected["preflight_status"] == "preflight_rejected"
    assert rejected["preflight_reason"] == "approval_rejected"

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="superseded",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    superseded = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert superseded["preflight_status"] == "preflight_superseded"
    assert superseded["preflight_reason"] == "approval_superseded"
    assert review_index["sections"][0]["preflight_path"] == ".dce/preflight/mission-board.preflight.json"
    assert review_index["sections"][0]["preflight_status"] == "preflight_superseded"
    assert review_index["sections"][0]["execution_allowed"] is False
    assert workspace_summary["sections"][0]["preflight_path"] == ".dce/preflight/mission-board.preflight.json"
    assert workspace_summary["sections"][0]["preflight_status"] == "preflight_superseded"
    assert workspace_summary["sections"][0]["execution_allowed"] is False


def test_record_section_preflight_is_deterministic_with_fixed_inputs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_5_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_5_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    fixed_approval = SectionApprovalInput(
        approval_status="approved",
        selected_mode="create_only",
        approval_timestamp="2026-03-26T00:00:00Z",
    )
    fixed_preflight = SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z")

    run_section_with_workspace(_section(), first_root, incremental_mode="incremental_v2_2")
    run_section_with_workspace(_section(), second_root, incremental_mode="incremental_v2_2")
    record_section_approval(first_root, "mission-board", fixed_approval)
    record_section_approval(second_root, "mission-board", fixed_approval)
    record_section_preflight(first_root, "mission-board", fixed_preflight)
    record_section_preflight(second_root, "mission-board", fixed_preflight)

    assert (first_root / ".dce" / "preflight" / "mission-board.preflight.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "preflight" / "mission-board.preflight.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "reviews" / "index.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "workspace_summary.json"
    ).read_text(encoding="utf-8")

    first_gate = record_section_execution_gate(
        first_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=fixed_preflight,
    )
    second_gate = record_section_execution_gate(
        second_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=fixed_preflight,
    )

    assert first_gate == second_gate
    assert (first_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json"
    ).read_text(encoding="utf-8")


def test_record_section_preflight_emits_deterministic_structured_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_preflight_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    preflight = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    assert sorted(preflight.keys()) == [
        "approval_path",
        "approval_status",
        "artifact_fingerprint",
        "checked_artifacts",
        "checks",
        "execution_allowed",
        "execution_permitted",
        "findings",
        "preflight_reason",
        "preflight_status",
        "preview_outcome_class",
        "preview_path",
        "readiness_decision",
        "readiness_summary",
        "recommended_mode",
        "review_path",
        "section_id",
        "selected_mode",
        "validation_timestamp",
    ]
    assert [entry["artifact_role"] for entry in preflight["checked_artifacts"]] == ["approval", "preview", "review"]
    assert [entry["present"] for entry in preflight["checked_artifacts"]] == [True, True, True]
    assert [check["check_id"] for check in preflight["checks"]] == [
        "approval_artifact_present",
        "preview_artifact_present",
        "review_artifact_present",
        "approval_status_allows_execution",
        "approval_preview_linkage_valid",
        "approval_review_linkage_valid",
        "execution_permission_granted",
    ]
    assert [check["result"] for check in preflight["checks"]] == [
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
    ]
    assert all(
        sorted(check.keys()) == [
            "category",
            "check_id",
            "checked_artifact_path",
            "checked_artifact_role",
            "detail",
            "issue_code",
            "result",
        ]
        for check in preflight["checks"]
    )
    assert preflight["findings"] == []
    assert preflight["readiness_decision"] == "ready"
    assert preflight["readiness_summary"] == {
        "checked_artifact_count": 3,
        "failed_check_count": 0,
        "blocking_finding_count": 0,
        "not_evaluated_check_count": 0,
        "passed_check_count": 7,
        "readiness_decision": "ready",
        "readiness_reason": "approved_and_linked",
        "ready_for_gate": True,
        "total_check_count": 7,
    }


def test_record_section_preflight_normalizes_failed_findings_and_gate_handoff(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_preflight_failed_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="review_required",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    preflight = record_section_preflight(
        project_root,
        "mission-board",
        SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    assert preflight["preflight_status"] == "preflight_execution_not_permitted"
    assert preflight["readiness_decision"] == "blocked"
    assert [check["result"] for check in preflight["checks"]] == [
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
        "failed",
    ]
    assert preflight["findings"] == [
        {
            "finding_id": "07_execution_permission_granted",
            "category": "execution_permission",
            "severity": "error",
            "checked_artifact_path": ".dce/approvals/mission-board.approval.json",
            "checked_artifact_role": "approval",
            "issue_code": "approval_not_permitted",
            "message": "execution permission denied",
            "section_id": "mission-board",
        }
    ]
    assert preflight["readiness_summary"] == {
        "checked_artifact_count": 3,
        "failed_check_count": 1,
        "blocking_finding_count": 1,
        "not_evaluated_check_count": 0,
        "passed_check_count": 6,
        "readiness_decision": "blocked",
        "readiness_reason": "approval_not_permitted",
        "ready_for_gate": False,
        "total_check_count": 7,
    }
    assert gate["preflight_status"] == "preflight_execution_not_permitted"
    assert gate["gate_status"] == "gate_blocked_preflight_failed"
    assert gate["execution_blocked"] is True


def test_record_section_stale_check_truth_table_and_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_9_stale_truth_table")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    missing_approval = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_approval["stale_status"] == "stale_missing_approval"
    assert missing_approval["stale_detected"] is True
    assert missing_approval["stale_reason"] == "missing_approval"

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    preview_backup = preview_path.read_text(encoding="utf-8")
    preview_path.unlink()
    missing_preview = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_preview["stale_status"] == "stale_missing_preview"
    assert missing_preview["stale_detected"] is True
    assert missing_preview["stale_reason"] == "missing_preview"
    preview_path.write_text(preview_backup, encoding="utf-8")

    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
    review_backup = review_path.read_text(encoding="utf-8")
    review_path.unlink()
    missing_review = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_review["stale_status"] == "stale_missing_review"
    assert missing_review["stale_detected"] is True
    assert missing_review["stale_reason"] == "missing_review"
    review_path.write_text(review_backup, encoding="utf-8")

    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["preview_path"] = ".dce/plans/other.preview.json"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    invalidated = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert invalidated["stale_status"] == "stale_invalidated"
    assert invalidated["stale_detected"] is True
    assert invalidated["stale_reason"] == "approval_preview_path_mismatch"

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    valid = record_section_stale_check(
        project_root,
        "mission-board",
        SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert valid["stale_status"] == "stale_valid"
    assert valid["stale_detected"] is False
    assert valid["stale_reason"] == "approval_links_current"
    assert review_index["sections"][0]["stale_check_path"] == ".dce/preflight/mission-board.stale_check.json"
    assert review_index["sections"][0]["stale_status"] == "stale_valid"
    assert review_index["sections"][0]["stale_detected"] is False
    assert workspace_summary["sections"][0]["stale_check_path"] == ".dce/preflight/mission-board.stale_check.json"
    assert workspace_summary["sections"][0]["stale_status"] == "stale_valid"
    assert workspace_summary["sections"][0]["stale_detected"] is False


def test_record_section_stale_check_is_deterministic_with_fixed_inputs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_9_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_9_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    fixed_stale = SectionStaleCheckInput(validation_timestamp="2026-03-26T00:00:00Z")
    fixed_approval = SectionApprovalInput(
        approval_status="approved",
        selected_mode="create_only",
        approval_timestamp="2026-03-26T00:00:00Z",
    )

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(root, "mission-board", fixed_approval)
        record_section_stale_check(root, "mission-board", fixed_stale)

    assert (first_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "preflight" / "mission-board.stale_check.json"
    ).read_text(encoding="utf-8")


def test_run_section_with_workspace_repeat_runs_preserve_fingerprints(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_repeat_fingerprint_stability")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    input_path = project_root / ".dce" / "input" / "mission-board.json"
    preview_path = project_root / ".dce" / "plans" / "mission-board.preview.json"
    first_input_fingerprint = compute_json_payload_fingerprint(json.loads(input_path.read_text(encoding="utf-8")))
    first_preview_fingerprint = compute_preview_payload_fingerprint(json.loads(preview_path.read_text(encoding="utf-8")))

    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    second_input_fingerprint = compute_json_payload_fingerprint(json.loads(input_path.read_text(encoding="utf-8")))
    second_preview_fingerprint = compute_preview_payload_fingerprint(json.loads(preview_path.read_text(encoding="utf-8")))

    assert first_input_fingerprint == second_input_fingerprint
    assert first_preview_fingerprint == second_preview_fingerprint


def test_record_section_execution_gate_truth_table_and_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_gate_truth_table")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    not_required = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=False,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
    )
    assert not_required["gate_status"] == "gate_not_required"
    assert not_required["execution_attempted"] is False
    assert not_required["execution_blocked"] is False

    missing_approval = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
    )
    assert missing_approval["gate_status"] == "gate_blocked_stale"
    assert missing_approval["stale_status"] == "stale_missing_approval"
    assert missing_approval["execution_blocked"] is True

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="review_required", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    execution_not_allowed = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    assert execution_not_allowed["gate_status"] == "gate_blocked_preflight_failed"
    assert execution_not_allowed["execution_blocked"] is True

    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    gate_pass = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert gate_pass["gate_status"] == "gate_pass"
    assert gate_pass["execution_blocked"] is False
    assert gate_pass["preflight_status"] == "preflight_pass"
    assert gate_pass["stale_status"] == "stale_valid"
    assert review_index["sections"][0]["execution_gate_path"] == ".dce/execution/gate/mission-board.execution_gate.json"
    assert review_index["sections"][0]["gate_status"] == "gate_pass"
    assert review_index["sections"][0]["execution_blocked"] is False
    assert review_index["sections"][0]["stale_check_path"] == ".dce/preflight/mission-board.stale_check.json"
    assert review_index["sections"][0]["stale_status"] == "stale_valid"
    assert review_index["sections"][0]["stale_detected"] is False
    assert workspace_summary["sections"][0]["execution_gate_path"] == ".dce/execution/gate/mission-board.execution_gate.json"
    assert workspace_summary["sections"][0]["gate_status"] == "gate_pass"
    assert workspace_summary["sections"][0]["execution_blocked"] is False
    assert workspace_summary["sections"][0]["stale_check_path"] == ".dce/preflight/mission-board.stale_check.json"
    assert workspace_summary["sections"][0]["stale_status"] == "stale_valid"
    assert workspace_summary["sections"][0]["stale_detected"] is False


def test_record_section_execution_gate_writes_gate_input_artifact_without_code_graph(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_gate_input_absent")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    gate_input = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))

    assert gate["gate_input_path"] == ".dce/execution/gate/mission-board.gate_input.json"
    assert (project_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").exists()
    assert (project_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").exists()
    assert not (project_root / ".dce" / "preflight" / "mission-board.gate_input.json").exists()
    assert not (project_root / ".dce" / "preflight" / "mission-board.execution_gate.json").exists()
    assert gate["gate_input_fingerprint"] == gate_input["gate_input_fingerprint"]
    assert gate_input["code_graph_context"] == {
        "availability_status": "absent",
        "source_format": "conservative_default",
        "degradation_reason": "code_graph_context_absent",
    }
    assert [entry["path"] for entry in gate_input["approved_scope"]["approved_targets"]] == [
        preview["path"] for preview in gate_input["target_classifications"]
    ]


def test_record_section_execution_gate_uses_valid_code_graph_deterministically(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_6_gate_input_code_graph_a")
    second_root = _workspace_dir("dgce_incremental_v2_6_gate_input_code_graph_b")
    section = _section().model_copy(update={"code_graph_context": _valid_code_graph_context()})

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for root in (first_root, second_root):
        run_section_with_workspace(section, root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        record_section_execution_gate(
            root,
            "mission-board",
            require_preflight_pass=True,
            gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
            preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
        )

    first_gate_input = json.loads((first_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))
    second_gate_input = json.loads((second_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))
    service_classification = next(
        entry for entry in first_gate_input["target_classifications"] if entry["path"] == "mission_board/service.py"
    )

    assert first_gate_input == second_gate_input
    assert first_gate_input["code_graph_context"]["availability_status"] == "available"
    assert first_gate_input["code_graph_context"]["source_format"] == "dcg.facts.v1"
    assert service_classification["classification_source"] == "code_graph_validated"
    assert service_classification["symbol_name"] == "MissionBoardService"
    assert service_classification["blast_radius_estimate"] == {"files": 1, "symbols": 2}
    assert service_classification["classification_confidence"] == "high"


def test_record_section_execution_gate_invalid_code_graph_triggers_safe_fallback(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_gate_input_invalid_code_graph")
    section = _section().model_copy(update={"code_graph_context": _malformed_code_graph_context()})

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    gate_input = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))

    assert gate_input["code_graph_context"] == {
        "availability_status": "invalid",
        "source_format": "conservative_default",
        "degradation_reason": "facts_malformed",
    }
    assert all(entry["classification_source"] == "conservative_default" for entry in gate_input["target_classifications"])


def test_gate_input_target_classifications_stay_within_approved_scope_and_policy_free(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_gate_input_scope_binding")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    gate_input = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))
    approved_paths = {entry["path"] for entry in gate_input["approved_scope"]["approved_targets"]}
    classification_paths = {entry["path"] for entry in gate_input["target_classifications"]}
    allowed_classification_keys = {
        "target_id",
        "path",
        "operation",
        "symbol_name",
        "classification_source",
        "ownership_classes",
        "sensitive_surfaces",
        "existing_sensitive_symbol_modified",
        "new_external_boundary_detected",
        "env_access_detected",
        "credential_handling_detected",
        "blast_radius_estimate",
        "supporting_evidence",
        "classification_confidence",
    }

    assert classification_paths == approved_paths
    assert all(set(entry.keys()) == allowed_classification_keys for entry in gate_input["target_classifications"])
    assert all("decision" not in entry and "risk" not in entry for entry in gate_input["target_classifications"])
    assert all(
        not entry["ownership_classes"] or entry["sensitive_surfaces"]
        for entry in gate_input["target_classifications"]
    )


def test_record_section_execution_gate_hands_gate_input_to_guardrail_unchanged(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_gate_input_guardrail_handoff")
    captured: list[dict] = []

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_handoff(payload):
        captured.append(json.loads(json.dumps(payload, sort_keys=True)))
        return payload

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr("aether.dgce.decompose._pass_gate_input_to_guardrail", fake_handoff)
    run_section_with_workspace(_section().model_copy(update={"code_graph_context": _valid_code_graph_context()}), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    persisted_gate_input = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.gate_input.json").read_text(encoding="utf-8"))

    assert captured == [persisted_gate_input]


def test_run_section_with_workspace_require_preflight_pass_execution_behavior_unchanged_with_code_graph(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    baseline_root = _workspace_dir("dgce_incremental_v2_6_gate_input_execution_baseline")
    code_graph_root = _workspace_dir("dgce_incremental_v2_6_gate_input_execution_code_graph")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for root, section in (
        (baseline_root, _section()),
        (code_graph_root, _section().model_copy(update={"code_graph_context": _valid_code_graph_context()})),
    ):
        run_section_with_workspace(section, root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )

    baseline_result = run_section_with_workspace(
        _section(),
        baseline_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
    )
    code_graph_result = run_section_with_workspace(
        _section().model_copy(update={"code_graph_context": _valid_code_graph_context()}),
        code_graph_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
    )

    assert code_graph_result.run_outcome_class == baseline_result.run_outcome_class
    assert code_graph_result.written_files == baseline_result.written_files


def test_record_section_execution_gate_recomputes_stale_from_current_approval_review_fingerprint(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_gate_recompute_current_review_fingerprint")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    review_path = project_root / ".dce" / "reviews" / "mission-board.review.md"
    current_review_fingerprint = dgce_decompose.compute_review_artifact_fingerprint(review_path.read_text(encoding="utf-8"))

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_fingerprint"] = "stale-review-fingerprint"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    first_gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    stale_before = json.loads((project_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8"))

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_fingerprint"] = current_review_fingerprint
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    second_gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    stale_after = json.loads((project_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8"))

    assert first_gate["gate_status"] == "gate_blocked_stale"
    assert stale_before["stale_status"] == "stale_invalidated"
    assert stale_before["stale_reason"] == "approval_review_fingerprint_mismatch"
    assert second_gate["gate_status"] == "gate_pass"
    assert second_gate["execution_blocked"] is False
    assert second_gate["stale_status"] == "stale_valid"
    assert stale_after["stale_status"] == "stale_valid"
    assert stale_after["stale_detected"] is False
    assert stale_after["stale_reason"] == "approval_links_current"


def test_record_section_execution_gate_emits_deterministic_structured_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_gate_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    assert sorted(gate.keys()) == [
        "checked_artifacts",
        "checks",
        "decision_summary",
        "execution_allowed",
        "execution_attempted",
        "execution_blocked",
        "gate_input_fingerprint",
        "gate_input_path",
        "gate_reason",
        "gate_status",
        "gate_timestamp",
        "preflight_path",
        "preflight_status",
        "reasons",
        "require_preflight_pass",
        "section_id",
        "selected_mode",
        "stale_check_path",
        "stale_detected",
        "stale_status",
    ]
    assert [entry["artifact_role"] for entry in gate["checked_artifacts"]] == ["preflight", "stale_check"]
    assert [entry["present"] for entry in gate["checked_artifacts"]] == [True, True]
    assert [check["check_id"] for check in gate["checks"]] == [
        "preflight_required",
        "stale_check_clear",
        "preflight_artifact_present",
        "preflight_status_passed",
        "execution_permission_confirmed",
    ]
    assert [check["result"] for check in gate["checks"]] == [
        "passed",
        "passed",
        "passed",
        "passed",
        "passed",
    ]
    assert all(
        sorted(check.keys()) == [
            "category",
            "check_id",
            "checked_artifact_path",
            "checked_artifact_role",
            "detail",
            "issue_code",
            "result",
        ]
        for check in gate["checks"]
    )
    assert gate["reasons"] == []
    assert gate["decision_summary"] == {
        "allow_execution": True,
        "blocking_reason_count": 0,
        "checked_artifact_count": 2,
        "failed_check_count": 0,
        "gate_reason": "preflight_passed",
        "gate_status": "gate_pass",
        "not_evaluated_check_count": 0,
        "not_required_check_count": 0,
        "passed_check_count": 5,
        "total_check_count": 5,
    }


def test_record_section_execution_gate_normalizes_blocking_reasons(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_gate_blocking_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    gate = record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
    )

    assert gate["gate_status"] == "gate_blocked_stale"
    assert gate["execution_blocked"] is True
    assert [check["result"] for check in gate["checks"]] == [
        "passed",
        "failed",
        "passed",
        "failed",
        "failed",
    ]
    assert gate["reasons"] == [
        {
            "category": "stale_check",
            "checked_artifact_path": ".dce/preflight/mission-board.stale_check.json",
            "checked_artifact_role": "stale_check",
            "issue_code": "stale_detected",
            "message": "stale check blocked execution",
            "reason_id": "02_stale_check_clear",
            "section_id": "mission-board",
            "severity": "critical",
        },
        {
            "category": "preflight",
            "checked_artifact_path": ".dce/preflight/mission-board.preflight.json",
            "checked_artifact_role": "preflight",
            "issue_code": "preflight_failed",
            "message": "preflight status blocks execution: preflight_missing_approval",
            "reason_id": "04_preflight_status_passed",
            "section_id": "mission-board",
            "severity": "error",
        },
        {
            "category": "execution_permission",
            "checked_artifact_path": ".dce/preflight/mission-board.preflight.json",
            "checked_artifact_role": "preflight",
            "issue_code": "execution_not_allowed",
            "message": "execution permission blocked",
            "reason_id": "05_execution_permission_confirmed",
            "section_id": "mission-board",
            "severity": "error",
        },
    ]
    assert gate["decision_summary"] == {
        "allow_execution": False,
        "blocking_reason_count": 3,
        "checked_artifact_count": 2,
        "failed_check_count": 3,
        "gate_reason": "stale_detected",
        "gate_status": "gate_blocked_stale",
        "not_evaluated_check_count": 0,
        "not_required_check_count": 0,
        "passed_check_count": 2,
        "total_check_count": 5,
    }


def test_run_section_with_workspace_require_preflight_pass_blocks_without_writes(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_blocked_run")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
    )
    gate_artifact = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "blocked_stale"
    assert result.execution_outcome["status"] == "blocked"
    assert result.execution_outcome["stale_status"] == "stale_missing_approval"
    assert gate_artifact["execution_blocked"] is True
    assert gate_artifact["stale_status"] == "stale_missing_approval"
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert (project_root / "api" / "missionboardservice.py").exists() is False


def test_run_section_with_workspace_stale_gate_blocks_without_project_writes(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_9_blocked_stale")
    _write_text(project_root / "docs" / "readme.md", "keep-doc")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    approval_path = project_root / ".dce" / "approvals" / "mission-board.approval.json"
    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    approval_payload["review_path"] = ".dce/reviews/other.review.md"
    approval_path.write_text(json.dumps(approval_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    stale_payload = json.loads((project_root / ".dce" / "preflight" / "mission-board.stale_check.json").read_text(encoding="utf-8"))
    gate_payload = json.loads((project_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "blocked_stale"
    assert result.execution_outcome["status"] == "blocked"
    assert result.execution_outcome["stale_status"] == "stale_invalidated"
    assert stale_payload["stale_status"] == "stale_invalidated"
    assert stale_payload["stale_detected"] is True
    assert gate_payload["gate_status"] == "gate_blocked_stale"
    assert (project_root / "docs" / "readme.md").read_text(encoding="utf-8") == "keep-doc"
    assert (project_root / "api" / "missionboardservice.py").exists() is False


def test_run_section_with_workspace_require_preflight_pass_allows_normal_execution(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_6_passed_run")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
    )

    assert result.run_outcome_class == "success_create_only"
    assert sorted(result.written_files) == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]


def test_run_section_with_workspace_gate_outputs_are_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_6_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_6_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
        )

    assert (first_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "execution" / "gate" / "mission-board.execution_gate.json"
    ).read_text(encoding="utf-8")


def test_record_section_alignment_truth_table_and_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_alignment_truth_table")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    change_plan = load_change_plan(project_root / ".dce" / "plans" / "mission-board.change_plan.json")
    _, write_transparency = build_write_transparency(
        preview_result.file_plan,
        change_plan,
        project_root,
    )
    alignment = record_section_alignment(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        alignment=SectionAlignmentInput(alignment_timestamp="2026-03-26T00:00:00Z"),
        file_plan=preview_result.file_plan,
        change_plan=change_plan,
        write_transparency=write_transparency,
    )
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert alignment["alignment_status"] == "aligned"
    assert alignment["alignment_blocked"] is False
    assert alignment["drift_findings"] == []
    assert alignment["code_graph_used"] is False
    assert review_index["sections"][0]["alignment_path"] == ".dce/execution/alignment/mission-board.alignment.json"
    assert review_index["sections"][0]["alignment_status"] == "aligned"
    assert review_index["sections"][0]["alignment_blocked"] is False
    assert workspace_summary["sections"][0]["alignment_path"] == ".dce/execution/alignment/mission-board.alignment.json"
    assert workspace_summary["sections"][0]["alignment_status"] == "aligned"
    assert workspace_summary["sections"][0]["alignment_blocked"] is False


def test_record_section_alignment_emits_deterministic_structured_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_alignment_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    change_plan = load_change_plan(project_root / ".dce" / "plans" / "mission-board.change_plan.json")
    _, write_transparency = build_write_transparency(
        preview_result.file_plan,
        change_plan,
        project_root,
    )

    alignment = record_section_alignment(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        alignment=SectionAlignmentInput(alignment_timestamp="2026-03-26T00:00:00Z"),
        file_plan=preview_result.file_plan,
        change_plan=change_plan,
        write_transparency=write_transparency,
    )

    assert sorted(alignment.keys()) == [
        "alignment_blocked",
        "alignment_fingerprint",
        "alignment_reason",
        "alignment_status",
        "alignment_timestamp",
        "artifact_fingerprint",
        "artifact_type",
        "code_graph_used",
        "contract_version",
        "created_written_count",
        "drift_findings",
        "effective_execution_mode",
        "generated_by",
        "intent_alignment",
        "justification_alignment",
        "modify_written_count",
        "require_preflight_pass",
        "schema_version",
        "scope_alignment",
        "section_id",
        "strategy_alignment",
        "written_file_count",
    ]
    assert alignment["alignment_status"] == "aligned"
    assert alignment["scope_alignment"]["status"] == "aligned"
    assert alignment["intent_alignment"]["status"] == "aligned"
    assert alignment["strategy_alignment"]["status"] == "aligned"
    assert alignment["justification_alignment"]["status"] == "aligned"
    assert alignment["drift_findings"] == []
    assert alignment["artifact_type"] == "alignment_record"
    for forbidden_field in (
        "risk_score",
        "severity",
        "policy_violation",
        "requires_approval",
        "unauthorized",
    ):
        assert forbidden_field not in alignment


def test_record_section_alignment_normalizes_mismatches_and_remediation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_alignment_blocking_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(
            approval_status="approved",
            selected_mode="create_only",
            approval_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_execution_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )

    drifted_files = [dict(entry) for entry in preview_result.file_plan.files]
    drifted_files[2]["source"] = "api_surface"
    drifted_files[2]["purpose"] = "API surface for mission board orchestration"
    drifted_plan = FilePlan(project_name=preview_result.file_plan.project_name, files=drifted_files)
    change_plan = load_change_plan(project_root / ".dce" / "plans" / "mission-board.change_plan.json")
    _, write_transparency = build_write_transparency(
        drifted_plan,
        change_plan,
        project_root,
    )
    alignment = record_section_alignment(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        alignment=SectionAlignmentInput(alignment_timestamp="2026-03-26T00:00:00Z"),
        file_plan=drifted_plan,
        change_plan=change_plan,
        write_transparency=write_transparency,
    )

    assert alignment["alignment_status"] == "misaligned"
    assert alignment["alignment_blocked"] is True
    assert "intent_category_drift" in alignment["drift_findings"]


def test_record_section_alignment_unknown_selected_mode_does_not_emit_extra_execution_mismatch(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_8_alignment_unknown_mode_a")
    second_root = _workspace_dir("dgce_incremental_v2_8_alignment_unknown_mode_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        _write_text(root / "mission_board" / "service.py", "existing-service")
        preview_result = run_section_with_workspace(
            _section().model_copy(update={"code_graph_context": _valid_code_graph_context()}),
            root,
            incremental_mode="incremental_v2_2",
            allow_safe_modify=True,
        )
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(
                approval_status="approved",
                selected_mode="create_only",
                approval_timestamp="2026-03-26T00:00:00Z",
            ),
        )
        record_section_execution_gate(
            root,
            "mission-board",
            require_preflight_pass=True,
            gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
            preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
        )
        (root / ".dce" / "input" / "mission-board.json").write_text(
            json.dumps(_section().model_dump(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        change_plan = load_change_plan(root / ".dce" / "plans" / "mission-board.change_plan.json")
        owned_paths = {Path(str(entry["path"])).as_posix() for entry in preview_result.file_plan.files}
        _, write_transparency = build_write_transparency(
            preview_result.file_plan,
            change_plan,
            root,
            allow_modify_write=True,
            owned_paths=owned_paths,
        )
        alignment = record_section_alignment(
            root,
            "mission-board",
            require_preflight_pass=True,
            alignment=SectionAlignmentInput(alignment_timestamp="2026-03-26T00:00:00Z"),
            file_plan=preview_result.file_plan,
            change_plan=change_plan,
            write_transparency=write_transparency,
        )
        if root == first_root:
            first_alignment = alignment
        else:
            second_alignment = alignment

    assert first_alignment == second_alignment
    assert first_alignment["alignment_status"] == "misaligned"
    assert first_alignment["drift_findings"] == ["edit_strategy_drift"]
    assert (first_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json"
    ).read_text(encoding="utf-8")


def test_run_section_with_workspace_alignment_not_executed_when_preflight_not_required(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_alignment_not_required")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    result = run_section_with_workspace(_section(), project_root)
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_create_only"
    assert (project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").exists() is False
    assert workspace_summary["sections"][0]["alignment_path"] is None
    assert workspace_summary["sections"][0]["alignment_status"] is None
    assert workspace_summary["sections"][0]["alignment_blocked"] is None


def test_run_section_with_workspace_alignment_blocks_no_changes_without_project_writes(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_blocked_no_changes")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    drifted_files = [dict(entry) for entry in preview_result.file_plan.files]
    drifted_files.append({"path": "mission_board/drift.py", "purpose": "Unexpected drift target", "source": "game_system"})
    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        prepared_file_plan=FilePlan(project_name=preview_result.file_plan.project_name, files=drifted_files),
    )
    alignment_artifact = json.loads((project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "blocked_alignment"
    assert result.execution_outcome["status"] == "blocked"
    assert result.execution_outcome["alignment_status"] == "misaligned"
    assert "target_set_expanded" in alignment_artifact["drift_findings"]
    assert "approved_scope_mismatch" in alignment_artifact["drift_findings"]
    assert (project_root / "mission_board" / "drift.py").exists() is False


def test_run_section_with_workspace_alignment_blocks_create_only_when_modify_would_occur(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_blocked_create_only_modify")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {
                            "path": "api/missionboardservice.py",
                            "language": "python",
                            "purpose": "API surface",
                            "content": "stale-content",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_index(project_root, [{"path": "api/missionboardservice.py", "section_id": section_id}])
    _write_text(project_root / "api" / "missionboardservice.py", "old-content")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    constrained_section = _section().model_copy(
        update={"description": "create only", "constraints": ["create only"]}
    )
    run_section_with_workspace(
        constrained_section,
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
    )
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        constrained_section,
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
    )
    alignment_artifact = json.loads((project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "blocked_alignment"
    assert alignment_artifact["alignment_blocked"] is True
    assert alignment_artifact["alignment_status"] == "misaligned"
    assert "design_constraint_mismatch" in alignment_artifact["drift_findings"]
    assert (project_root / "api" / "missionboardservice.py").read_text(encoding="utf-8") == "old-content"
    assert (project_root / "mission_board" / "models.py").exists() is False


def test_record_section_alignment_reports_justification_missing_drift_for_modify_without_declared_justification(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_alignment_justification_missing")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {
                            "path": "api/missionboardservice.py",
                            "language": "python",
                            "purpose": "API surface",
                            "content": "stale-content",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_index(project_root, [{"path": "api/missionboardservice.py", "section_id": section_id}])
    _write_text(project_root / "api" / "missionboardservice.py", "old-content")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    preview_result = run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_execution_gate(
        project_root,
        section_id,
        require_preflight_pass=True,
        gate=SectionExecutionGateInput(gate_timestamp="2026-03-26T00:00:00Z"),
        preflight=SectionPreflightInput(validation_timestamp="2026-03-26T00:00:00Z"),
    )
    input_path = project_root / ".dce" / "input" / f"{section_id}.json"
    input_payload = json.loads(input_path.read_text(encoding="utf-8"))
    input_payload["description"] = ""
    input_path.write_text(json.dumps(input_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    change_plan = load_change_plan(project_root / ".dce" / "plans" / "mission-board.change_plan.json")
    _, write_transparency = build_write_transparency(
        preview_result.file_plan,
        change_plan,
        project_root,
        allow_modify_write=True,
    )
    alignment = record_section_alignment(
        project_root,
        section_id,
        require_preflight_pass=True,
        alignment=SectionAlignmentInput(alignment_timestamp="2026-03-26T00:00:00Z"),
        file_plan=preview_result.file_plan,
        change_plan=change_plan,
        write_transparency=write_transparency,
    )

    assert alignment["alignment_status"] == "misaligned"
    assert alignment["drift_findings"] == ["justification_missing_drift"]


def test_run_section_with_workspace_alignment_passes_safe_modify_and_executes(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_7_safe_modify_pass")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {
                            "path": "api/missionboardservice.py",
                            "language": "python",
                            "purpose": "API surface",
                            "content": "stale-content",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_index(project_root, [{"path": "api/missionboardservice.py", "section_id": section_id}])
    _write_text(project_root / "api" / "missionboardservice.py", "old-content")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
    )
    alignment_artifact = json.loads((project_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_safe_modify"
    assert sorted(result.written_files) == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert alignment_artifact["alignment_status"] == "aligned"
    assert alignment_artifact["alignment_blocked"] is False
    assert alignment_artifact["effective_execution_mode"] == "safe_modify"


def test_run_section_with_workspace_simulation_skip_path_proceeds(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_skip")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=False,
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    trigger_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").read_text(encoding="utf-8")
    )
    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_create_only"
    assert trigger_artifact["simulation_triggered"] is False
    assert trigger_artifact["simulation_stage_status"] == "simulation_skipped"
    assert trigger_artifact["trigger_reason_codes"] == []
    assert trigger_artifact["trigger_reason_summary"] is None
    assert not (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").exists()
    assert {"artifact_role": "simulation_trigger", "path": ".dce/execution/simulation/mission-board.simulation_trigger.json"} in workspace_index["sections"][0]["artifact_links"]


def test_run_section_with_workspace_simulation_required_blocks_without_valid_result(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_missing")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    trigger_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").read_text(encoding="utf-8")
    )

    assert result.run_outcome_class == "blocked_simulation"
    assert result.written_files == []
    assert result.execution_outcome["simulation_triggered"] is True
    assert result.execution_outcome["simulation_status"] == "indeterminate"
    assert trigger_artifact["simulation_triggered"] is True
    assert trigger_artifact["simulation_stage_status"] == "simulation_required"
    assert trigger_artifact["trigger_reason_codes"] == ["policy_required_simulation"]
    assert trigger_artifact["trigger_reason_summary"] == _expected_trigger_reason_summary("policy_required_simulation")
    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "simulation_provider_unresolved"


@pytest.mark.parametrize(
    ("simulation_status", "expected_outcome"),
    [
        ("pass", "success_create_only"),
        ("fail", "blocked_simulation"),
        ("indeterminate", "blocked_simulation"),
    ],
)
def test_run_section_with_workspace_simulation_result_contract_controls_gate(monkeypatch, simulation_status, expected_outcome):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir(f"dgce_incremental_v2_9_simulation_{simulation_status}")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status=simulation_status,
            findings=["deterministic_constraint_violation"] if simulation_status == "fail" else [],
            indeterminate_reason="simulation_result_missing" if simulation_status == "indeterminate" else None,
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )

    assert result.run_outcome_class == expected_outcome
    assert simulation_artifact["simulation_status"] == simulation_status
    if simulation_status == "pass":
        assert result.execution_outcome["status"] == "success"
    else:
        assert result.execution_outcome["status"] == "blocked"
        assert result.execution_outcome["simulation_status"] == simulation_status


def test_stage75_pass_artifact_has_normalized_reason_fields(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_normalized_pass")

    def passing_provider(_request):
        return {
            "simulation_status": "pass",
            "findings": [],
            "provider_debug_blob": {"should_not": "escape"},
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", passing_provider)
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_artifact["reason_code"] == "simulation_pass"
    assert simulation_artifact["reason_summary"] == "Simulation completed without blocking findings."
    assert simulation_artifact["findings"] == []
    assert simulation_artifact["indeterminate_reason"] is None
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact forced override applied"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "workspace_artifact",
        "advisory_provider": None,
        "composition_mode": "authoritative_only",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "not_run",
        "summary": "simulation not executed",
        "target": None,
    }
    assert "provider_debug_blob" not in simulation_artifact
    assert set(simulation_artifact.keys()) == {
        "artifact_type",
        "artifact_fingerprint",
        "contract_version",
        "findings",
        "generated_by",
        "indeterminate_reason",
        "provider_name",
        "provider_applicability",
        "provider_composition",
        "provider_execution_state",
        "provider_execution_summary",
        "provider_execution_target",
        "advisory_execution",
        "provider_selection_reason",
        "provider_selection_source",
        "reason_code",
        "reason_summary",
        "schema_version",
        "section_id",
        "simulation_fingerprint",
        "simulation_source",
        "simulation_status",
        "simulation_timestamp",
    }


def test_stage75_indeterminate_artifact_has_normalized_reason_fields():
    project_root = _workspace_dir("dgce_incremental_stage75_normalized_indeterminate")

    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "preview_artifact_missing"
    assert simulation_artifact["reason_summary"] == "Preview artifact required for simulation modeling was missing."
    assert simulation_artifact["findings"] == []


def test_stage75_fail_artifact_normalizes_factual_findings_with_stable_codes(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_normalized_fail")

    def failing_provider(_request):
        return {
            "simulation_status": "fail",
            "findings": [
                {
                    "code": "approved write set violates deterministic safe modify boundary",
                    "summary": "Approved write set violates deterministic safe modify boundary.",
                    "target": "deploy/docker-compose.yaml",
                }
            ],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", failing_provider)
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["reason_summary"] == "Simulation produced concrete blocking findings."
    assert simulation_artifact["findings"] == [
        {
            "code": "approved_write_set_violates_deterministic_safe_modify_boundary",
            "summary": "Approved write set violates deterministic safe modify boundary.",
            "target": "deploy/docker-compose.yaml",
        }
    ]


def test_execute_reserved_simulation_gate_preserves_skip_behavior_without_provider_execution(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_skip_provider")
    provider_calls: list[dict] = []

    def unexpected_provider(_request):
        provider_calls.append({"called": True})
        return {"simulation_status": "pass"}

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", unexpected_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=False,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    trigger_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").read_text(encoding="utf-8")
    )
    assert provider_calls == []
    assert simulation_gate["simulation_status"] == "skipped"
    assert simulation_gate["provider_resolution_status"] == "not_applicable"
    assert trigger_artifact["simulation_provider"] is None
    assert not (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").exists()


def test_execute_reserved_simulation_gate_writes_indeterminate_when_provider_unavailable():
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_unavailable")
    original_registry = dict(dgce_decompose._SIMULATION_PROVIDER_REGISTRY)
    dgce_decompose._SIMULATION_PROVIDER_REGISTRY.clear()
    try:
        simulation_gate = execute_reserved_simulation_gate(
            project_root,
            "mission-board",
            require_preflight_pass=True,
            simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
                simulation_triggered=True,
                simulation_provider="workspace_artifact",
                simulation_trigger_timestamp="2026-03-26T00:00:00Z",
            ),
        )
    finally:
        dgce_decompose._SIMULATION_PROVIDER_REGISTRY.clear()
        dgce_decompose._SIMULATION_PROVIDER_REGISTRY.update(original_registry)

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_gate["provider_selection_source"] == "explicit"
    assert simulation_gate["provider_selection_reason"] == "explicit_provider_unavailable"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "simulation_provider_unresolved"
    assert simulation_artifact["findings"] == []


def test_execute_reserved_simulation_gate_writes_indeterminate_when_provider_raises(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_exception")

    def raising_provider(_request):
        raise RuntimeError("boom")

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", raising_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "provider_exception"


def test_execute_reserved_simulation_gate_fail_closes_invalid_provider_response(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_invalid")

    def invalid_provider(_request):
        return {"simulation_status": "maybe", "findings": ["unclear"]}

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", invalid_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "invalid_provider_response"
    assert simulation_artifact["reason_code"] == "invalid_provider_response"
    assert simulation_artifact["reason_summary"] == (
        "Provider response could not be normalized into the sealed simulation evidence contract."
    )


def test_execute_reserved_simulation_gate_blocks_on_provider_fail_with_findings(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_fail")

    def failing_provider(_request):
        return {
            "simulation_status": "fail",
            "findings": ["approved write set violates deterministic safe modify boundary"],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", failing_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["reason_summary"] == "Simulation produced concrete blocking findings."
    assert simulation_artifact["findings"] == [
        {
            "code": "approved_write_set_violates_deterministic_safe_modify_boundary",
            "summary": "approved write set violates deterministic safe modify boundary",
            "target": None,
        }
    ]


def test_execute_reserved_simulation_gate_allows_stage_8_when_provider_passes(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_pass")

    def passing_provider(_request):
        return {
            "simulation_status": "pass",
            "findings": [],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", passing_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is False
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["reason_code"] == "simulation_pass"
    assert simulation_artifact["reason_summary"] == "Simulation completed without blocking findings."
    assert simulation_artifact["findings"] == []


def test_execute_reserved_simulation_gate_fail_closes_malformed_provider_findings(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_provider_bad_findings")

    def invalid_provider(_request):
        return {
            "simulation_status": "fail",
            "findings": [{"code": "bad_payload_without_summary"}],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", invalid_provider)

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "invalid_provider_response"
    assert simulation_artifact["reason_summary"] == (
        "Provider response could not be normalized into the sealed simulation evidence contract."
    )
    assert simulation_artifact["findings"] == []


def test_infra_dry_run_provider_passes_for_create_only_infra_candidate(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_infra_pass")
    prepared_file_plan = _infra_file_plan()

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert result.run_outcome_class == "success_create_only"
    assert result.execution_outcome["status"] == "success"
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["reason_code"] == "simulation_pass"
    assert simulation_artifact["findings"] == []


def test_infra_dry_run_provider_fails_for_modify_infra_candidate(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_infra_fail")
    prepared_file_plan = _infra_file_plan()
    deploy_path = project_root / "deploy" / "docker-compose.yaml"
    deploy_path.parent.mkdir(parents=True, exist_ok=True)
    deploy_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert result.run_outcome_class == "blocked_simulation"
    assert result.execution_outcome["status"] == "blocked"
    assert result.execution_outcome["simulation_status"] == "fail"
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["findings"] == [
        {
            "code": "infra_modify_candidate",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        }
    ]


def test_infra_dry_run_provider_returns_indeterminate_when_preview_input_missing():
    project_root = _workspace_dir("dgce_incremental_stage75_infra_indeterminate")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "preview_artifact_missing"
    assert simulation_artifact["reason_summary"] == "Preview artifact required for simulation modeling was missing."
    assert simulation_artifact["indeterminate_reason"] == "preview_artifact_missing"


def test_infra_dry_run_provider_non_triggered_path_is_unchanged():
    project_root = _workspace_dir("dgce_incremental_stage75_infra_not_triggered")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=False,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    trigger_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation_trigger.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is False
    assert simulation_gate["simulation_status"] == "skipped"
    assert simulation_gate["provider_resolution_status"] == "not_applicable"
    assert trigger_artifact["simulation_provider"] == "infra_dry_run"
    assert trigger_artifact["trigger_reason_codes"] == []
    assert trigger_artifact["trigger_reason_summary"] is None
    assert not (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").exists()


def test_external_dry_run_provider_passes_for_valid_docker_compose_config(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_pass")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **kwargs):
        assert command == ["docker", "compose", "-f", "deploy/docker-compose.yaml", "config"]
        assert kwargs["shell"] is False
        assert kwargs["cwd"] == str(project_root)
        assert kwargs["timeout"] == dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(command, 0, stdout="services:\n  app:\n    image: alpine:latest\n", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "external_dry_run"
    assert simulation_artifact["provider_name"] == "external_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["reason_code"] == "simulation_pass"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "docker compose config executed successfully"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_docker_compose_config(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_fail")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **kwargs):
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="services.app Additional property typo is not allowed\nservices.app.image must be a string\n",
        )

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "docker compose config executed with blocking findings"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"
    assert "stdout" not in simulation_artifact
    assert "stderr" not in simulation_artifact
    assert simulation_artifact["findings"] == [
        {
            "code": "external_compose_property_not_allowed",
            "summary": "Docker Compose validation reported an unsupported property.",
            "target": "deploy/docker-compose.yaml",
        },
        {
            "code": "external_compose_type_mismatch",
            "summary": "Docker Compose validation reported a field with an invalid type.",
            "target": "deploy/docker-compose.yaml",
        },
    ]


def test_external_dry_run_provider_returns_indeterminate_when_command_is_unavailable(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_unavailable")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(_command, **_kwargs):
        raise FileNotFoundError("docker not found")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_unavailable"
    assert simulation_artifact["provider_execution_state"] == "unavailable"
    assert simulation_artifact["provider_execution_summary"] == "external command unavailable"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"


def test_external_dry_run_provider_returns_indeterminate_when_command_times_out(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_timeout")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, timeout=dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_timeout"
    assert simulation_artifact["provider_execution_state"] == "timeout"
    assert simulation_artifact["provider_execution_summary"] == "external command timed out"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"


def test_external_dry_run_provider_returns_indeterminate_for_malformed_command_output(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_parse_error")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_parse_error"
    assert simulation_artifact["provider_execution_state"] == "input_invalid"
    assert simulation_artifact["provider_execution_summary"] == "external command input invalid"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"


def test_external_dry_run_provider_returns_indeterminate_for_unparseable_validation_surface(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_unparseable_surface")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="validation failed for unknown reason\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_parse_error"
    assert simulation_artifact["provider_execution_state"] == "input_invalid"
    assert simulation_artifact["provider_execution_summary"] == "external command input invalid"
    assert simulation_artifact["provider_execution_target"] == "deploy/docker-compose.yaml"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_supports_bounded_multifile_compose_inputs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_multifile")
    prepared_file_plan = _compose_file_plan("compose.yaml", "deploy/docker-compose.yaml")
    root_compose = project_root / "compose.yaml"
    deploy_compose = project_root / "deploy" / "docker-compose.yaml"
    root_compose.parent.mkdir(parents=True, exist_ok=True)
    deploy_compose.parent.mkdir(parents=True, exist_ok=True)
    root_compose.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")
    deploy_compose.write_text("services:\n  app:\n    environment:\n      TEST: 1\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **kwargs):
        assert command == ["docker", "compose", "-f", "compose.yaml", "-f", "deploy/docker-compose.yaml", "config"]
        assert kwargs["shell"] is False
        assert kwargs["cwd"] == str(project_root)
        assert kwargs["timeout"] == dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(command, 0, stdout="services:\n  app:\n    image: alpine:latest\n", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "external_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_returns_indeterminate_when_known_compose_input_file_is_missing(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_known_input_missing")
    prepared_file_plan = _compose_file_plan("compose.yaml", "deploy/docker-compose.yaml")
    root_compose = project_root / "compose.yaml"
    root_compose.parent.mkdir(parents=True, exist_ok=True)
    root_compose.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_input_missing"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_when_required_input_is_missing(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_input_missing")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_input_missing"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_for_unsupported_compose_input_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_unsupported")
    prepared_file_plan = _compose_file_plan("deploy/compose.yml")
    unsupported_compose = project_root / "deploy" / "compose.yml"
    unsupported_compose.parent.mkdir(parents=True, exist_ok=True)
    unsupported_compose.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_unsupported"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_passes_for_valid_dockerfile(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_dockerfile_pass")
    prepared_file_plan = _dockerfile_file_plan("Dockerfile")
    dockerfile_path = project_root / "Dockerfile"
    project_root.mkdir(parents=True, exist_ok=True)
    dockerfile_path.write_text("FROM alpine:latest\nRUN echo hello\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **kwargs):
        assert command == ["docker", "build", "--no-cache", "--progress=plain", "-f", "Dockerfile", "."]
        assert kwargs["shell"] is False
        assert kwargs["cwd"] == str(project_root)
        assert kwargs["timeout"] == dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(command, 0, stdout="build ok", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "external_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "docker build validation executed successfully"
    assert simulation_artifact["provider_execution_target"] == "Dockerfile"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_dockerfile(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_dockerfile_fail")
    prepared_file_plan = _dockerfile_file_plan("Dockerfile")
    dockerfile_path = project_root / "Dockerfile"
    project_root.mkdir(parents=True, exist_ok=True)
    dockerfile_path.write_text("FROOOM alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="unknown instruction: FROOOM\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_blocked"] is True
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "docker build validation executed with blocking findings"
    assert simulation_artifact["provider_execution_target"] == "Dockerfile"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_dockerfile_instruction_invalid",
            "summary": "Dockerfile validation reported an invalid or unknown instruction.",
            "target": "Dockerfile",
        }
    ]


def test_external_dry_run_provider_returns_indeterminate_for_unsupported_dockerfile_name(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_dockerfile_unsupported")
    prepared_file_plan = _dockerfile_file_plan("Dockerfile.dev")
    dockerfile_path = project_root / "Dockerfile.dev"
    project_root.mkdir(parents=True, exist_ok=True)
    dockerfile_path.write_text("FROM alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_unsupported"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_when_explicit_dockerfile_input_is_missing(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_dockerfile_input_missing")
    prepared_file_plan = _dockerfile_file_plan("Dockerfile")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_input_missing"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_for_unparseable_dockerfile_diagnostics(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_dockerfile_parse_error")
    prepared_file_plan = _dockerfile_file_plan("Dockerfile")
    dockerfile_path = project_root / "Dockerfile"
    project_root.mkdir(parents=True, exist_ok=True)
    dockerfile_path.write_text("FROM alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="validation failed for unknown dockerfile reason\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_parse_error"
    assert simulation_artifact["provider_execution_state"] == "input_invalid"
    assert simulation_artifact["provider_execution_summary"] == "external command input invalid"
    assert simulation_artifact["provider_execution_target"] == "Dockerfile"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_passes_for_valid_k8s_manifest(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_k8s_pass")
    prepared_file_plan = _k8s_file_plan("k8s/deployment.yaml")
    manifest_path = project_root / "k8s" / "deployment.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **kwargs):
        assert command == ["kubectl", "apply", "--dry-run=client", "-f", "k8s/deployment.yaml"]
        assert kwargs["shell"] is False
        assert kwargs["cwd"] == str(project_root)
        assert kwargs["timeout"] == dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(command, 0, stdout="deployment.apps/app created (dry run)", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "external_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "kubectl client dry-run executed successfully"
    assert simulation_artifact["provider_execution_target"] == "k8s/deployment.yaml"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_k8s_schema(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_k8s_schema_fail")
    prepared_file_plan = _k8s_file_plan("k8s/deployment.yaml")
    manifest_path = project_root / "k8s" / "deployment.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="error validating data: ValidationError(Deployment.spec): invalid value\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["provider_execution_summary"] == "kubectl client dry-run executed with blocking findings"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_k8s_schema_invalid",
            "summary": "Kubernetes validation reported a schema violation.",
            "target": "k8s/deployment.yaml",
        }
    ]


def test_external_dry_run_provider_fails_with_normalized_findings_for_missing_required_k8s_field(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_k8s_required_fail")
    prepared_file_plan = _k8s_file_plan("k8s/service.yaml")
    manifest_path = project_root / "k8s" / "service.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("apiVersion: v1\nkind: Service\nmetadata:\n  name: app\nspec: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="spec.ports: Required value\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_k8s_required_field_missing",
            "summary": "Kubernetes validation reported a missing required field.",
            "target": "k8s/service.yaml",
        }
    ]


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_k8s_kind(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_k8s_kind_fail")
    prepared_file_plan = _k8s_file_plan("kubernetes/resource.yaml")
    manifest_path = project_root / "kubernetes" / "resource.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("apiVersion: apps/v1\nkind: Deploymnt\nmetadata:\n  name: app\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="error: unable to recognize \"resource.yaml\": no matches for kind \"Deploymnt\"\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_k8s_resource_kind_invalid",
            "summary": "Kubernetes validation reported an invalid or unknown resource kind.",
            "target": "kubernetes/resource.yaml",
        }
    ]


def test_external_dry_run_provider_returns_indeterminate_for_unsupported_yaml_input(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_yaml_unsupported")
    prepared_file_plan = _k8s_file_plan("deploy/config.yaml")
    yaml_path = project_root / "deploy" / "config.yaml"
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text("key: value\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_unsupported"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_for_unparseable_k8s_diagnostics(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_k8s_parse_error")
    prepared_file_plan = _k8s_file_plan("k8s/deployment.yaml")
    manifest_path = project_root / "k8s" / "deployment.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="kubectl validation failed mysteriously\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_parse_error"
    assert simulation_artifact["provider_execution_state"] == "input_invalid"
    assert simulation_artifact["provider_execution_summary"] == "external command input invalid"
    assert simulation_artifact["provider_execution_target"] == "k8s/deployment.yaml"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_passes_for_valid_terraform_module(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_pass")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")
    tf_path = project_root / "infra" / "terraform" / "main.tf"
    tf_path.parent.mkdir(parents=True, exist_ok=True)
    tf_path.write_text('terraform {}\noutput "x" { value = "ok" }\n', encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    calls: list[tuple[list[str], str]] = []

    def fake_subprocess_run(command, **kwargs):
        calls.append((command, kwargs["cwd"]))
        assert kwargs["shell"] is False
        assert kwargs["timeout"] == dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert calls == [
        (["terraform", "init", "-backend=false"], str(project_root / "infra" / "terraform")),
        (["terraform", "validate"], str(project_root / "infra" / "terraform")),
    ]
    assert simulation_gate["provider_name"] == "external_dry_run"
    assert simulation_artifact["simulation_status"] == "pass"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "terraform validation executed successfully"
    assert simulation_artifact["provider_execution_target"] == "infra/terraform"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_terraform_block(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_schema_fail")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")
    tf_path = project_root / "infra" / "terraform" / "main.tf"
    tf_path.parent.mkdir(parents=True, exist_ok=True)
    tf_path.write_text("terraform {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    call_index = {"value": 0}

    def fake_subprocess_run(command, **_kwargs):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return subprocess.CompletedProcess(command, 0, stdout="init ok", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr='Unsupported argument; An argument named "bad" is not expected here.\n')

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["provider_execution_summary"] == "terraform validation executed with blocking findings"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_terraform_schema_invalid",
            "summary": "Terraform validation reported an invalid configuration block or attribute.",
            "target": "infra/terraform/main.tf",
        }
    ]


def test_external_dry_run_provider_fails_with_normalized_findings_for_missing_required_terraform_argument(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_required_fail")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")
    tf_path = project_root / "infra" / "terraform" / "main.tf"
    tf_path.parent.mkdir(parents=True, exist_ok=True)
    tf_path.write_text("terraform {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    call_index = {"value": 0}

    def fake_subprocess_run(command, **_kwargs):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return subprocess.CompletedProcess(command, 0, stdout="init ok", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr='Missing required argument; The argument "value" is required.\n')

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_terraform_required_field_missing",
            "summary": "Terraform validation reported a missing required argument or block.",
            "target": "infra/terraform/main.tf",
        }
    ]


def test_external_dry_run_provider_fails_with_normalized_findings_for_invalid_terraform_reference(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_reference_fail")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")
    tf_path = project_root / "infra" / "terraform" / "main.tf"
    tf_path.parent.mkdir(parents=True, exist_ok=True)
    tf_path.write_text("terraform {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    call_index = {"value": 0}

    def fake_subprocess_run(command, **_kwargs):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return subprocess.CompletedProcess(command, 0, stdout="init ok", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="Reference to undeclared resource\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "fail"
    assert simulation_artifact["findings"] == [
        {
            "code": "external_terraform_reference_invalid",
            "summary": "Terraform validation reported an invalid or unresolved reference.",
            "target": "infra/terraform/main.tf",
        }
    ]


def test_external_dry_run_provider_returns_indeterminate_for_unsupported_terraform_shape(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_unsupported")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf", "ops/terraform/main.tf")
    first_path = project_root / "infra" / "terraform" / "main.tf"
    second_path = project_root / "ops" / "terraform" / "main.tf"
    first_path.parent.mkdir(parents=True, exist_ok=True)
    second_path.parent.mkdir(parents=True, exist_ok=True)
    first_path.write_text("terraform {}\n", encoding="utf-8")
    second_path.write_text("terraform {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_unsupported"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_when_explicit_terraform_input_is_missing(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_input_missing")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_input_missing"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "external dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None


def test_external_dry_run_provider_returns_indeterminate_for_unparseable_terraform_diagnostics(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_terraform_parse_error")
    prepared_file_plan = _terraform_file_plan("infra/terraform/main.tf")
    tf_path = project_root / "infra" / "terraform" / "main.tf"
    tf_path.parent.mkdir(parents=True, exist_ok=True)
    tf_path.write_text("terraform {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    call_index = {"value": 0}

    def fake_subprocess_run(command, **_kwargs):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return subprocess.CompletedProcess(command, 0, stdout="init ok", stderr="")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="terraform validation failed mysteriously\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="external_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "external_command_parse_error"
    assert simulation_artifact["provider_execution_state"] == "input_invalid"
    assert simulation_artifact["provider_execution_summary"] == "external command input invalid"
    assert simulation_artifact["provider_execution_target"] == "infra/terraform"
    assert simulation_artifact["findings"] == []


def test_external_dry_run_provider_is_not_selected_when_not_applicable(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_external_not_applicable")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="test_seed",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["provider_applicability"]["applicable_providers"] == ["workspace_artifact"]


def test_external_dry_run_provider_does_not_change_inferred_infra_selection_when_both_are_applicable(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_external_infra_precedence")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("services:\n  app:\n    image: alpine:latest\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_gate["provider_selection_reason"] == "provider_precedence_resolved"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["external_dry_run", "infra_dry_run"],
        "resolution": "inferred",
        "selected_provider": "infra_dry_run",
    }


def test_stage75_trigger_record_adds_infra_and_deployment_reason_codes_for_deployment_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_trigger_reasons_deploy")
    prepared_file_plan = _infra_file_plan("deploy/docker-compose.yaml")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    trigger_artifact = dgce_decompose.record_section_simulation_trigger(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    assert trigger_artifact["trigger_reason_codes"] == [
        "policy_required_simulation",
        "infrastructure_touching",
        "deployment_artifact",
    ]
    assert trigger_artifact["trigger_reason_summary"] == _expected_trigger_reason_summary(
        "policy_required_simulation",
        "infrastructure_touching",
        "deployment_artifact",
    )


def test_stage75_trigger_record_adds_runtime_control_reason_codes_for_systemd_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_trigger_reasons_runtime")
    prepared_file_plan = _infra_file_plan("systemd/mission-board.service")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    trigger_artifact = dgce_decompose.record_section_simulation_trigger(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    assert trigger_artifact["trigger_reason_codes"] == [
        "policy_required_simulation",
        "infrastructure_touching",
        "runtime_control",
    ]
    assert trigger_artifact["trigger_reason_summary"] == _expected_trigger_reason_summary(
        "policy_required_simulation",
        "infrastructure_touching",
        "runtime_control",
    )


def test_stage75_trigger_record_sorts_multi_reason_codes_deterministically_for_modify_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_trigger_reasons_modify")
    prepared_file_plan = _infra_file_plan("deploy/docker-compose.yaml")
    deploy_path = project_root / "deploy" / "docker-compose.yaml"
    deploy_path.parent.mkdir(parents=True, exist_ok=True)
    deploy_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )

    trigger_artifact = dgce_decompose.record_section_simulation_trigger(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    assert trigger_artifact["trigger_reason_codes"] == [
        "policy_required_simulation",
        "infrastructure_touching",
        "deployment_artifact",
        "irreversible_operation",
    ]
    assert trigger_artifact["trigger_reason_summary"] == _expected_trigger_reason_summary(
        "policy_required_simulation",
        "infrastructure_touching",
        "deployment_artifact",
        "irreversible_operation",
    )


def test_stage75_selector_explicit_valid_provider_override_selects_requested_provider(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_selector_explicit_valid")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_gate["provider_selection_source"] == "explicit"
    assert simulation_gate["provider_selection_reason"] == "explicit_provider_selected"
    assert simulation_artifact["provider_name"] == "workspace_artifact"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["workspace_artifact"],
        "resolution": "explicit",
        "selected_provider": "workspace_artifact",
    }
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact evaluated successfully"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"
    assert simulation_artifact["provider_selection_source"] == "explicit"
    assert simulation_artifact["provider_selection_reason"] == "explicit_provider_selected"
    assert simulation_artifact["simulation_status"] == "pass"


def test_stage75_selector_explicit_override_uses_forced_override_when_provider_is_registered_but_not_applicable():
    project_root = _workspace_dir("dgce_incremental_stage75_selector_forced_override")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_gate["provider_selection_source"] == "explicit"
    assert simulation_gate["provider_selection_reason"] == "forced_override"
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": [],
        "resolution": "forced_override",
        "selected_provider": "infra_dry_run",
    }
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "infra dry-run forced override applied"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "preview_artifact_missing"


def test_workspace_artifact_provider_returns_fail_with_normalized_findings_on_valid_seed_artifact():
    project_root = _workspace_dir("dgce_incremental_stage75_workspace_provider_fail")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="fail",
            findings=["approved write set violates deterministic safe modify boundary"],
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact evaluated with blocking findings"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"
    assert simulation_artifact["findings"] == [
        {
            "code": "approved_write_set_violates_deterministic_safe_modify_boundary",
            "summary": "approved write set violates deterministic safe modify boundary",
            "target": None,
        }
    ]


def test_workspace_artifact_provider_returns_indeterminate_when_seed_artifact_missing():
    project_root = _workspace_dir("dgce_incremental_stage75_workspace_provider_missing")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "artifact_missing"
    assert simulation_artifact["reason_code"] == "artifact_missing"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact forced override applied"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"


def test_workspace_artifact_provider_returns_indeterminate_when_seed_artifact_is_malformed():
    project_root = _workspace_dir("dgce_incremental_stage75_workspace_provider_malformed")
    simulation_path = project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json"
    simulation_path.parent.mkdir(parents=True, exist_ok=True)
    simulation_path.write_text("{not valid json", encoding="utf-8")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(simulation_path.read_text(encoding="utf-8"))
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "artifact_invalid"
    assert simulation_artifact["reason_code"] == "artifact_invalid"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact forced override applied"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"


def test_workspace_artifact_provider_returns_indeterminate_when_seed_artifact_structure_is_invalid():
    project_root = _workspace_dir("dgce_incremental_stage75_workspace_provider_invalid_structure")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    simulation_path = project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json"
    payload = json.loads(simulation_path.read_text(encoding="utf-8"))
    payload["unsupported_provider_blob"] = {"leak": True}
    payload["artifact_fingerprint"] = dgce_decompose.compute_json_payload_fingerprint(payload)
    simulation_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(simulation_path.read_text(encoding="utf-8"))
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "artifact_invalid"
    assert simulation_artifact["reason_code"] == "artifact_invalid"
    assert simulation_artifact["provider_execution_state"] == "forced_override"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact forced override applied"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"


@pytest.mark.parametrize("seeded_status", ["pass", "fail", "indeterminate"])
def test_workspace_artifact_provider_always_emits_supported_statuses(seeded_status):
    project_root = _workspace_dir(f"dgce_incremental_stage75_workspace_provider_status_{seeded_status}")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status=seeded_status,
            findings=["approved write set violates deterministic safe modify boundary"] if seeded_status == "fail" else [],
            indeterminate_reason="simulation_result_missing" if seeded_status == "indeterminate" else None,
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    assert simulation_gate["simulation_status"] in {"pass", "fail", "indeterminate"}


def test_stage75_selector_explicit_invalid_provider_override_fails_closed():
    project_root = _workspace_dir("dgce_incremental_stage75_selector_explicit_invalid")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="not_a_real_provider",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_gate["provider_selection_source"] == "explicit"
    assert simulation_gate["provider_selection_reason"] == "explicit_provider_unavailable"
    assert simulation_artifact["provider_name"] is None
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": [],
        "resolution": "unresolved",
        "selected_provider": None,
    }
    assert simulation_artifact["provider_execution_state"] == "not_run"
    assert simulation_artifact["provider_execution_summary"] == "simulation not executed"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["provider_selection_source"] == "explicit"
    assert simulation_artifact["provider_selection_reason"] == "explicit_provider_unavailable"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "simulation_provider_unresolved"


def test_stage75_selector_infers_infra_dry_run_only_for_clear_infra_candidates(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_selector_infers_infra")
    prepared_file_plan = _infra_file_plan()

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_gate["provider_selection_source"] == "inferred"
    assert simulation_gate["provider_selection_reason"] == "infra_dry_run_applicable"
    assert simulation_artifact["provider_name"] == "infra_dry_run"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["infra_dry_run"],
        "resolution": "inferred",
        "selected_provider": "infra_dry_run",
    }
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "infra dry-run executed successfully"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["provider_selection_source"] == "inferred"
    assert simulation_artifact["provider_selection_reason"] == "infra_dry_run_applicable"


def test_stage75_selector_uses_workspace_artifact_when_existing_contract_is_applicable():
    project_root = _workspace_dir("dgce_incremental_stage75_selector_workspace_artifact")
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="test_seed",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_gate["provider_selection_source"] == "inferred"
    assert simulation_gate["provider_selection_reason"] == "workspace_artifact_available"
    assert simulation_artifact["provider_name"] == "workspace_artifact"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["workspace_artifact"],
        "resolution": "inferred",
        "selected_provider": "workspace_artifact",
    }
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact evaluated successfully"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"
    assert simulation_artifact["provider_selection_source"] == "inferred"
    assert simulation_artifact["provider_selection_reason"] == "workspace_artifact_available"
    assert simulation_artifact["simulation_status"] == "pass"


def test_stage75_selector_resolves_multiple_applicable_providers_via_deterministic_precedence(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_selector_precedence")
    prepared_file_plan = _infra_file_plan()

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_gate["provider_selection_source"] == "inferred"
    assert simulation_gate["provider_selection_reason"] == "provider_precedence_resolved"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["infra_dry_run", "workspace_artifact"],
        "resolution": "inferred",
        "selected_provider": "workspace_artifact",
    }
    assert simulation_artifact["provider_execution_state"] == "executed"
    assert simulation_artifact["provider_execution_summary"] == "workspace artifact evaluated successfully"
    assert simulation_artifact["provider_execution_target"] == ".dce/execution/simulation/mission-board.simulation.json"
    assert simulation_artifact["simulation_status"] == "pass"


def test_stage75_selector_fails_closed_on_multiple_applicable_providers_without_precedence(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_selector_conflict")
    prepared_file_plan = _infra_file_plan()

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose, "_SIMULATION_PROVIDER_PRECEDENCE", ())
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_gate["provider_selection_source"] == "unresolved"
    assert simulation_gate["provider_selection_reason"] == "simulation_provider_conflict"
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": ["infra_dry_run", "workspace_artifact"],
        "resolution": "conflict",
        "selected_provider": None,
    }
    assert simulation_artifact["provider_execution_state"] == "not_run"
    assert simulation_artifact["provider_execution_summary"] == "simulation not executed"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["reason_code"] == "simulation_provider_conflict"


def test_stage75_selector_returns_unresolved_when_no_provider_path_is_applicable(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_selector_unresolved")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_resolution_status"] == "unresolved"
    assert simulation_gate["provider_selection_source"] == "unresolved"
    assert simulation_gate["provider_selection_reason"] == "simulation_provider_unresolved"
    assert simulation_artifact["provider_name"] is None
    assert simulation_artifact["provider_applicability"] == {
        "applicable_providers": [],
        "resolution": "unresolved",
        "selected_provider": None,
    }
    assert simulation_artifact["provider_execution_state"] == "not_run"
    assert simulation_artifact["provider_execution_summary"] == "simulation not executed"
    assert simulation_artifact["provider_execution_target"] is None
    assert simulation_artifact["provider_selection_source"] == "unresolved"
    assert simulation_artifact["provider_selection_reason"] == "simulation_provider_unresolved"
    assert simulation_artifact["simulation_status"] == "indeterminate"
    assert simulation_artifact["indeterminate_reason"] == "simulation_provider_unresolved"


def test_stage75_authoritative_only_baseline_records_authoritative_only_composition(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_authoritative_only_composition")
    prepared_file_plan = _infra_file_plan()

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        prepared_file_plan=prepared_file_plan,
    )
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        simulation_triggered=True,
        simulation_provider="infra_dry_run",
        simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
        prepared_file_plan=prepared_file_plan,
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert result.run_outcome_class == "success_create_only"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": None,
        "composition_mode": "authoritative_only",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "not_run",
        "summary": "simulation not executed",
        "target": None,
    }


def test_stage75_composition_appends_advisory_findings_without_changing_authoritative_fail(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_composition_infra_external")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="services.app.image must be a string\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["reason_code"] == "simulation_fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": "external_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "executed",
        "summary": "docker compose config executed with blocking findings",
        "target": "deploy/docker-compose.yaml",
    }
    assert simulation_artifact["findings"] == [
        {
            "code": "external_compose_type_mismatch",
            "provider": "external_dry_run",
            "summary": "Docker Compose validation reported a field with an invalid type.",
            "target": "deploy/docker-compose.yaml",
        },
        {
            "code": "infra_modify_candidate",
            "provider": "infra_dry_run",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        },
    ]


def test_stage75_composition_ignores_advisory_failure_without_changing_authoritative_fail(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_composition_advisory_failure")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, timeout=dgce_decompose._EXTERNAL_DRY_RUN_TIMEOUT_SECONDS)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "infra_dry_run"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "infra_dry_run",
        "advisory_provider": "external_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["advisory_execution"] == {
        "state": "timeout",
        "summary": "external command timed out",
        "target": "deploy/docker-compose.yaml",
    }
    assert simulation_artifact["findings"] == [
        {
            "code": "infra_modify_candidate",
            "provider": "infra_dry_run",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        }
    ]


def test_stage75_composition_workspace_authoritative_with_external_advisory_is_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_composition_workspace_external")
    prepared_file_plan = _infra_file_plan()
    compose_path = project_root / "deploy" / "docker-compose.yaml"
    compose_path.parent.mkdir(parents=True, exist_ok=True)
    compose_path.write_text("version: '3'\nservices: {}\n", encoding="utf-8")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    def fake_subprocess_run(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="services.app Additional property typo is not allowed\n")

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    monkeypatch.setattr(dgce_decompose.subprocess, "run", fake_subprocess_run)
    run_section_with_workspace(
        _section(),
        project_root,
        incremental_mode="incremental_v2_2",
        allow_safe_modify=True,
        prepared_file_plan=prepared_file_plan,
    )
    record_section_simulation(
        project_root,
        "mission-board",
        simulation=SectionSimulationInput(
            simulation_status="fail",
            findings=["approved write set violates deterministic safe modify boundary"],
            provider_name="workspace_artifact",
            provider_selection_reason="seeded_workspace_artifact",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_gate = execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    simulation_artifact = json.loads(
        (project_root / ".dce" / "execution" / "simulation" / "mission-board.simulation.json").read_text(encoding="utf-8")
    )
    assert simulation_gate["provider_name"] == "workspace_artifact"
    assert simulation_artifact["simulation_status"] == "fail"
    assert simulation_artifact["provider_composition"] == {
        "authoritative_provider": "workspace_artifact",
        "advisory_provider": "infra_dry_run",
        "composition_mode": "authoritative_plus_advisory",
    }
    assert simulation_artifact["findings"] == [
        {
            "code": "approved_write_set_violates_deterministic_safe_modify_boundary",
            "provider": "workspace_artifact",
            "summary": "approved write set violates deterministic safe modify boundary",
            "target": None,
        },
        {
            "code": "infra_modify_candidate",
            "provider": "infra_dry_run",
            "summary": "Infrastructure dry-run detected a modify candidate.",
            "target": "deploy/docker-compose.yaml",
        },
    ]


def test_stage_7_5_remains_outside_canonical_lifecycle_order():
    assert dgce_decompose.DGCE_LIFECYCLE_ORDER == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]


def test_stage_7_5_artifact_exposure_remains_stable(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_v2_9_simulation_artifact_exposure")

    def passing_provider(_request):
        return {"simulation_status": "pass", "findings": []}

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", passing_provider)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    artifact_manifest = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    section_entry = next(entry for entry in workspace_index["sections"] if entry["section_id"] == "mission-board")
    artifact_roles = {entry["artifact_role"] for entry in section_entry["artifact_links"]}
    assert "simulation_trigger" in artifact_roles
    assert "simulation" in artifact_roles
    assert {
        "artifact_path": ".dce/execution/simulation/mission-board.simulation.json",
        "artifact_type": "simulation_record",
        "schema_version": "1.0",
        "scope": "section",
        "section_id": "mission-board",
    } in artifact_manifest["artifacts"]


def test_stage_7_5_projection_shows_triggered_pass_in_workspace_views(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_projection_pass")

    def passing_provider(_request):
        return {"simulation_status": "pass", "findings": []}

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", passing_provider)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    dashboard = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))
    section_summary = next(entry for entry in dashboard["sections"] if entry["section_id"] == "mission-board")["section_summary"]
    assert section_summary["simulation"] == {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 0,
        "finding_codes": [],
        "provider_execution_state": "forced_override",
        "provider_execution_summary": "workspace artifact forced override applied",
        "provider_execution_target": None,
        "provider_selection_source": "explicit",
        "provider_resolution": "forced_override",
        "reason_code": "simulation_pass",
        "reason_summary": "Simulation completed without blocking findings.",
        "selected_provider": "workspace_artifact",
        "simulation_provider": "workspace_artifact",
        "simulation_stage_applicable": True,
        "simulation_status": "pass",
        "simulation_triggered": True,
        "trigger_reason_codes": ["policy_required_simulation"],
        "trigger_reason_summary": _expected_trigger_reason_summary("policy_required_simulation"),
    }


def test_stage_7_5_projection_shows_triggered_fail_with_compact_findings(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_projection_fail")

    def failing_provider(_request):
        return {
            "simulation_status": "fail",
            "findings": [
                {"code": "second_code", "summary": "Second finding.", "target": "b.txt"},
                {"code": "first_code", "summary": "First finding.", "target": "a.txt"},
                {"code": "first_code", "summary": "First finding duplicate code.", "target": "c.txt"},
            ],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", failing_provider)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    section_summary = next(entry for entry in workspace_summary["sections"] if entry["section_id"] == "mission-board")["section_summary"]
    assert section_summary["simulation"] == {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 2,
        "finding_codes": ["first_code", "second_code"],
        "provider_execution_state": "forced_override",
        "provider_execution_summary": "workspace artifact forced override applied",
        "provider_execution_target": None,
        "provider_selection_source": "explicit",
        "provider_resolution": "forced_override",
        "reason_code": "simulation_fail",
        "reason_summary": "Simulation produced concrete blocking findings.",
        "selected_provider": "workspace_artifact",
        "simulation_provider": "workspace_artifact",
        "simulation_stage_applicable": True,
        "simulation_status": "fail",
        "simulation_triggered": True,
        "trigger_reason_codes": ["policy_required_simulation"],
        "trigger_reason_summary": _expected_trigger_reason_summary("policy_required_simulation"),
    }


def test_stage_7_5_projection_shows_triggered_indeterminate_reason_fields():
    project_root = _workspace_dir("dgce_incremental_stage75_projection_indeterminate")
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    section_summary = next(entry for entry in workspace_index["sections"] if entry["section_id"] == "mission-board")["section_summary"]
    assert section_summary["simulation"] == {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 0,
        "finding_codes": [],
        "provider_execution_state": "forced_override",
        "provider_execution_summary": "infra dry-run forced override applied",
        "provider_execution_target": None,
        "provider_selection_source": "explicit",
        "provider_resolution": "forced_override",
        "reason_code": "infra_candidate_absent",
        "reason_summary": "No actionable infrastructure dry-run candidate was present.",
        "selected_provider": "infra_dry_run",
        "simulation_provider": "infra_dry_run",
        "simulation_stage_applicable": True,
        "simulation_status": "indeterminate",
        "simulation_triggered": True,
        "trigger_reason_codes": ["policy_required_simulation"],
        "trigger_reason_summary": _expected_trigger_reason_summary("policy_required_simulation"),
    }


def test_stage_7_5_projection_keeps_non_triggered_case_explicit():
    project_root = _workspace_dir("dgce_incremental_stage75_projection_not_triggered")
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=False,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    section_summary = next(entry for entry in review_index["sections"] if entry["section_id"] == "mission-board")["section_summary"]
    assert section_summary["simulation"] == {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 0,
        "finding_codes": [],
        "provider_execution_state": "not_run",
        "provider_execution_summary": "simulation not executed",
        "provider_execution_target": None,
        "provider_selection_source": "not_applicable",
        "provider_resolution": None,
        "reason_code": None,
        "reason_summary": None,
        "selected_provider": None,
        "simulation_provider": "infra_dry_run",
        "simulation_stage_applicable": True,
        "simulation_status": "skipped",
        "simulation_triggered": False,
        "trigger_reason_codes": [],
        "trigger_reason_summary": None,
    }


def test_stage_7_5_trigger_reason_projection_remains_consistent_across_workspace_surfaces(monkeypatch):
    project_root = _workspace_dir("dgce_incremental_stage75_projection_trigger_reason_consistency")

    def failing_provider(_request):
        return {
            "simulation_status": "fail",
            "findings": [
                {"code": "infra_modify_candidate", "summary": "Infrastructure dry-run detected a modify candidate.", "target": "deploy/docker-compose.yaml"}
            ],
        }

    monkeypatch.setitem(dgce_decompose._SIMULATION_PROVIDER_REGISTRY, "workspace_artifact", failing_provider)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    execute_reserved_simulation_gate(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=True,
            simulation_provider="workspace_artifact",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    expected_projection = {
        "applicable_providers": [],
        "advisory_provider": None,
        "findings_count": 1,
        "finding_codes": ["infra_modify_candidate"],
        "provider_execution_state": "forced_override",
        "provider_execution_summary": "workspace artifact forced override applied",
        "provider_execution_target": None,
        "provider_selection_source": "explicit",
        "provider_resolution": "forced_override",
        "reason_code": "simulation_fail",
        "reason_summary": "Simulation produced concrete blocking findings.",
        "selected_provider": "workspace_artifact",
        "simulation_provider": "workspace_artifact",
        "simulation_stage_applicable": True,
        "simulation_status": "fail",
        "simulation_triggered": True,
        "trigger_reason_codes": ["policy_required_simulation"],
        "trigger_reason_summary": _expected_trigger_reason_summary("policy_required_simulation"),
    }

    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    dashboard = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))

    for surface in (review_index, workspace_summary, workspace_index, dashboard):
        section_summary = next(entry for entry in surface["sections"] if entry["section_id"] == "mission-board")["section_summary"]
        assert section_summary["simulation"] == expected_projection


def test_stage_7_5_projection_remains_consistent_across_workspace_surfaces_for_mixed_states(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_stage75_projection_mixed_consistency")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    sections = [
        ("alpha-section", "Alpha Section"),
        ("beta-section", "Beta Section"),
        ("gamma-section", "Gamma Section"),
        ("delta-section", "Delta Section"),
        ("epsilon-section", "Epsilon Section"),
    ]
    for _section_id, title in sections:
        run_section_with_workspace(_section_named(title), project_root, incremental_mode="incremental_v2_2")

    execute_reserved_simulation_gate(
        project_root,
        "beta-section",
        require_preflight_pass=True,
        simulation_trigger=dgce_decompose.SectionSimulationTriggerInput(
            simulation_triggered=False,
            simulation_provider="infra_dry_run",
            simulation_trigger_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_simulation(
        project_root,
        "gamma-section",
        simulation=SectionSimulationInput(
            simulation_status="pass",
            provider_name="workspace_artifact",
            provider_selection_reason="test_seed",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_simulation(
        project_root,
        "delta-section",
        simulation=SectionSimulationInput(
            simulation_status="fail",
            findings=[
                {"code": "duplicate_code", "summary": "Duplicate one.", "target": "a.txt"},
                {"code": "duplicate_code", "summary": "Duplicate two.", "target": "b.txt"},
                {"code": "distinct_code", "summary": "Distinct.", "target": "c.txt"},
            ],
            provider_name="workspace_artifact",
            provider_selection_reason="test_seed",
            provider_selection_source="explicit",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )
    record_section_simulation(
        project_root,
        "epsilon-section",
        simulation=SectionSimulationInput(
            simulation_status="indeterminate",
            indeterminate_reason="simulation_provider_unresolved",
            provider_selection_reason="simulation_provider_unresolved",
            provider_selection_source="unresolved",
            simulation_timestamp="2026-03-26T00:00:00Z",
        ),
    )

    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))
    workspace_index = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    dashboard = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))

    def _section_simulation(payload, section_id):
        return next(entry for entry in payload["sections"] if entry["section_id"] == section_id)["section_summary"]["simulation"]

    expected = {
        "alpha-section": {
            "applicable_providers": [],
            "advisory_provider": None,
            "findings_count": 0,
            "finding_codes": [],
            "provider_execution_state": "not_run",
            "provider_execution_summary": "simulation not executed",
            "provider_execution_target": None,
            "provider_selection_source": None,
            "provider_resolution": None,
            "reason_code": None,
            "reason_summary": None,
            "selected_provider": None,
            "simulation_provider": None,
            "simulation_stage_applicable": False,
            "simulation_status": None,
            "simulation_triggered": False,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
        "beta-section": {
            "applicable_providers": [],
            "advisory_provider": None,
            "findings_count": 0,
            "finding_codes": [],
            "provider_execution_state": "not_run",
            "provider_execution_summary": "simulation not executed",
            "provider_execution_target": None,
            "provider_selection_source": "not_applicable",
            "provider_resolution": None,
            "reason_code": None,
            "reason_summary": None,
            "selected_provider": None,
            "simulation_provider": "infra_dry_run",
            "simulation_stage_applicable": True,
            "simulation_status": "skipped",
            "simulation_triggered": False,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
        "gamma-section": {
            "applicable_providers": ["workspace_artifact"],
            "advisory_provider": None,
            "findings_count": 0,
            "finding_codes": [],
            "provider_execution_state": "executed",
            "provider_execution_summary": "workspace artifact evaluated successfully",
            "provider_execution_target": None,
            "provider_selection_source": "explicit",
            "provider_resolution": "explicit",
            "reason_code": "simulation_pass",
            "reason_summary": "Simulation completed without blocking findings.",
            "selected_provider": "workspace_artifact",
            "simulation_provider": "workspace_artifact",
            "simulation_stage_applicable": True,
            "simulation_status": "pass",
            "simulation_triggered": True,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
        "delta-section": {
            "applicable_providers": ["workspace_artifact"],
            "advisory_provider": None,
            "findings_count": 2,
            "finding_codes": ["duplicate_code", "distinct_code"],
            "provider_execution_state": "executed",
            "provider_execution_summary": "workspace artifact evaluated with blocking findings",
            "provider_execution_target": None,
            "provider_selection_source": "explicit",
            "provider_resolution": "explicit",
            "reason_code": "simulation_fail",
            "reason_summary": "Simulation produced concrete blocking findings.",
            "selected_provider": "workspace_artifact",
            "simulation_provider": "workspace_artifact",
            "simulation_stage_applicable": True,
            "simulation_status": "fail",
            "simulation_triggered": True,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
        "epsilon-section": {
            "applicable_providers": [],
            "advisory_provider": None,
            "findings_count": 0,
            "finding_codes": [],
            "provider_execution_state": "not_run",
            "provider_execution_summary": "simulation not executed",
            "provider_execution_target": None,
            "provider_selection_source": "unresolved",
            "provider_resolution": "unresolved",
            "reason_code": "simulation_provider_unresolved",
            "reason_summary": "No applicable simulation provider could be resolved.",
            "selected_provider": None,
            "simulation_provider": None,
            "simulation_stage_applicable": True,
            "simulation_status": "indeterminate",
            "simulation_triggered": True,
            "trigger_reason_codes": [],
            "trigger_reason_summary": None,
        },
    }
    for section_id, projection in expected.items():
        assert _section_simulation(review_index, section_id) == projection
        assert _section_simulation(workspace_summary, section_id) == projection
        assert _section_simulation(workspace_index, section_id) == projection
        assert _section_simulation(dashboard, section_id) == projection


def test_run_section_with_workspace_alignment_outputs_are_deterministic(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_7_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_7_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="no_changes", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
        )

    assert (first_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "execution" / "alignment" / "mission-board.alignment.json"
    ).read_text(encoding="utf-8")


def test_run_section_with_workspace_execution_stamp_written_under_dce_execution_only(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_execution_path")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    result = run_section_with_workspace(
        _section(),
        project_root,
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_path = project_root / ".dce" / "execution" / "mission-board.execution.json"
    payload = json.loads(execution_path.read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_create_only"
    assert execution_path.exists()
    assert sorted(path.relative_to(project_root).as_posix() for path in project_root.rglob("*.execution.json")) == [
        ".dce/execution/mission-board.execution.json"
    ]
    assert payload["execution_status"] == "execution_not_governed"
    assert payload["governed_execution"] is False
    assert payload["require_preflight_pass"] is False
    assert payload["written_file_count"] == 4
    assert payload["modify_written_count"] == 0
    assert payload["created_written_count"] == 4


def test_run_section_with_workspace_governed_execution_consumes_approval_and_updates_linkage(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_governed_success")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
    approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))
    review_index = json.loads((project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"))
    workspace_summary = json.loads((project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_create_only"
    assert execution_payload["execution_status"] == "execution_completed"
    assert execution_payload["governed_execution"] is True
    assert execution_payload["execution_blocked"] is False
    assert execution_payload["approval_status_before"] == "approved"
    assert execution_payload["approval_consumed"] is True
    assert execution_payload["approval_status_after"] == "superseded"
    assert execution_payload["selected_mode"] == "create_only"
    assert execution_payload["effective_execution_mode"] == "create_only"
    assert execution_payload["written_file_count"] == 4
    assert execution_payload["modify_written_count"] == 0
    assert execution_payload["created_written_count"] == 4
    assert approval_payload["approval_status"] == "superseded"
    assert approval_payload["execution_permitted"] is False
    assert review_index["sections"][0]["execution_path"] == ".dce/execution/mission-board.execution.json"
    assert review_index["sections"][0]["execution_status"] == "execution_completed"
    assert review_index["sections"][0]["approval_consumed"] is True
    assert review_index["sections"][0]["approval_status_after"] == "superseded"
    assert workspace_summary["sections"][0]["execution_path"] == ".dce/execution/mission-board.execution.json"
    assert workspace_summary["sections"][0]["execution_status"] == "execution_completed"
    assert workspace_summary["sections"][0]["approval_consumed"] is True
    assert workspace_summary["sections"][0]["approval_status_after"] == "superseded"
    assert workspace_summary["sections"][0]["approval_status"] == "superseded"


def test_run_section_with_workspace_execution_stamp_emits_deterministic_structured_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_0_execution_contract")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))

    assert sorted(execution_payload.keys()) == [
        "alignment_path",
        "approval_consumed",
        "approval_path",
        "approval_status_after",
        "approval_status_before",
        "artifact_results",
        "artifact_type",
        "created_written_count",
        "effective_execution_mode",
        "executed_units",
        "execution_blocked",
        "execution_gate_path",
        "execution_record_summary",
        "execution_status",
        "execution_timestamp",
        "failed_units",
        "generated_by",
        "governed_execution",
        "linked_artifacts",
        "modify_written_count",
        "outputs_path",
        "preflight_path",
        "require_preflight_pass",
        "run_outcome_class",
        "schema_version",
        "section_id",
        "selected_mode",
        "simulation_path",
        "simulation_status",
        "simulation_trigger_path",
        "simulation_triggered",
        "skipped_units",
        "unit_results",
        "written_file_count",
        "written_files",
    ]
    assert execution_payload["artifact_type"] == "execution_record"
    assert execution_payload["generated_by"] == "DGCE"
    assert execution_payload["schema_version"] == "1.0"
    assert [entry["artifact_role"] for entry in execution_payload["linked_artifacts"]] == [
        "approval",
        "preflight",
        "execution_gate",
        "alignment",
        "simulation_trigger",
        "simulation",
        "outputs",
    ]
    assert [entry["present"] for entry in execution_payload["linked_artifacts"]] == [True, True, True, True, True, False, True]
    assert [entry["path"] for entry in execution_payload["artifact_results"]] == [
        "api/missionboardservice.py",
        "mission_board/models.py",
        "mission_board/service.py",
        "models/mission.py",
    ]
    assert [entry["result_status"] for entry in execution_payload["artifact_results"]] == [
        "written",
        "written",
        "written",
        "written",
    ]
    assert all(
        sorted(entry.keys()) == [
            "artifact_id",
            "artifact_kind",
            "bytes_written",
            "implementation_unit",
            "path",
            "producer_ref",
            "result_status",
            "source",
            "write_decision",
            "write_reason",
        ]
        for entry in execution_payload["artifact_results"]
    )
    assert execution_payload["written_files"] == [
        {
            "path": "api/missionboardservice.py",
            "operation": "create",
            "bytes_written": execution_payload["artifact_results"][0]["bytes_written"],
        },
        {
            "path": "mission_board/models.py",
            "operation": "create",
            "bytes_written": execution_payload["artifact_results"][1]["bytes_written"],
        },
        {
            "path": "mission_board/service.py",
            "operation": "create",
            "bytes_written": execution_payload["artifact_results"][2]["bytes_written"],
        },
        {
            "path": "models/mission.py",
            "operation": "create",
            "bytes_written": execution_payload["artifact_results"][3]["bytes_written"],
        },
    ]
    assert [entry["unit_id"] for entry in execution_payload["unit_results"]] == [
        "generate_mission_model",
        "generate_missionboardservice_api",
        "implement_mission_board",
    ]
    assert [entry["unit_status"] for entry in execution_payload["unit_results"]] == [
        "executed",
        "executed",
        "executed",
    ]
    assert execution_payload["executed_units"] == [
        "generate_mission_model",
        "generate_missionboardservice_api",
        "implement_mission_board",
    ]
    assert execution_payload["skipped_units"] == []
    assert execution_payload["failed_units"] == []
    assert execution_payload["execution_record_summary"] == {
        "execution_blocked": False,
        "execution_status": "execution_completed",
        "executed_unit_count": 3,
        "linked_artifact_count": 7,
        "result_artifact_count": 4,
        "run_outcome_class": "success_create_only",
        "skipped_artifact_count": 0,
        "skipped_unit_count": 0,
        "written_artifact_count": 4,
    }


def test_run_section_with_workspace_governed_execution_completed_no_changes_consumes_approval(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_governed_no_changes")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "partial_skipped_identical"
    assert execution_payload["execution_status"] == "execution_completed_no_changes"
    assert execution_payload["approval_consumed"] is True
    assert execution_payload["approval_status_after"] == "superseded"
    assert execution_payload["effective_execution_mode"] == "no_changes"
    assert execution_payload["written_file_count"] == 0
    assert execution_payload["modify_written_count"] == 0
    assert execution_payload["created_written_count"] == 0


def test_run_section_with_workspace_governed_data_model_empty_output_no_changes_still_fails_validation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_governed_data_model_empty_output")
    section = DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE data model.",
        requirements=[
            "Define SectionInput and ExecutionStamp entities",
            "Keep the model deterministic and auditable",
        ],
        constraints=["Keep the model independent of .dce file paths"],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )
    section_id = "data-model"
    expected_files = [
        {
            "path": path,
            "purpose": "",
            "source": "expected_targets",
            "requirements": section.requirements,
        }
        for path in section.expected_targets
    ]

    for file_entry in expected_files:
        target_path = project_root / file_entry["path"]
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(render_file_entry_bytes(file_entry))
    _write_section_input(project_root, section)
    _write_ownership_index(project_root, [{"path": path, "section_id": section_id} for path in section.expected_targets])

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "implement a data model class" in lowered:
            output = ""
        elif "plan the system breakdown" in lowered:
            output = json.dumps(
                {
                    "module_name": "dgce_data_model",
                    "purpose": "describe governed dgce data entities",
                    "subcomponents": ["section_input", "execution_stamp"],
                    "dependencies": ["audit_log"],
                    "implementation_order": ["section_input", "execution_stamp"],
                }
            )
        elif "implement an api surface" in lowered:
            output = json.dumps(
                {
                    "interfaces": ["DGCEDataModelService"],
                    "methods": ["describe"],
                    "inputs": ["section_id"],
                    "outputs": ["artifact"],
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
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    run_section_with_workspace(section, project_root)
    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        section,
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / f"{section_id}.execution.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "validation_failure"
    assert result.execution_outcome["validation_summary"] == {
        "ok": False,
        "error": "invalid_json",
        "missing_keys": [],
    }
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 2
    assert execution_payload["execution_status"] == "execution_completed_no_changes"
    assert execution_payload["run_outcome_class"] == "validation_failure"


def test_run_section_with_workspace_governed_data_model_malformed_output_still_fails_validation(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_governed_data_model_malformed_output")
    section = DGCESection(
        section_type="data_model",
        title="Data Model",
        description="Define the governed DGCE data model.",
        requirements=[
            "Define SectionInput and ExecutionStamp entities",
            "Keep the model deterministic and auditable",
        ],
        constraints=["Keep the model independent of .dce file paths"],
        expected_targets=["aether/dgce/decompose.py", "aether/dgce/incremental.py"],
    )
    section_id = "data-model"
    expected_files = [
        {
            "path": path,
            "purpose": "",
            "source": "expected_targets",
            "requirements": section.requirements,
        }
        for path in section.expected_targets
    ]

    for file_entry in expected_files:
        target_path = project_root / file_entry["path"]
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(render_file_entry_bytes(file_entry))
    _write_section_input(project_root, section)
    _write_ownership_index(project_root, [{"path": path, "section_id": section_id} for path in section.expected_targets])

    def fake_run(self, executor_name, content):
        lowered = content.lower()
        if "implement a data model class" in lowered:
            output = "not valid json"
        elif "plan the system breakdown" in lowered:
            output = json.dumps(
                {
                    "module_name": "dgce_data_model",
                    "purpose": "describe governed dgce data entities",
                    "subcomponents": ["section_input", "execution_stamp"],
                    "dependencies": ["audit_log"],
                    "implementation_order": ["section_input", "execution_stamp"],
                }
            )
        elif "implement an api surface" in lowered:
            output = json.dumps(
                {
                    "interfaces": ["DGCEDataModelService"],
                    "methods": ["describe"],
                    "inputs": ["section_id"],
                    "outputs": ["artifact"],
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
    monkeypatch.setattr("aether.dgce.decompose.build_file_plan", lambda responses: FilePlan(project_name="DGCE", files=[]))

    run_section_with_workspace(section, project_root)
    run_section_with_workspace(section, project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        section,
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / f"{section_id}.execution.json").read_text(encoding="utf-8"))

    assert result.written_files == []
    assert result.run_outcome_class == "validation_failure"
    assert result.execution_outcome["validation_summary"] == {
        "ok": False,
        "error": "invalid_json",
        "missing_keys": [],
    }
    assert result.execution_outcome["execution_summary"]["skipped_identical_count"] == 2
    assert execution_payload["execution_status"] == "execution_completed_no_changes"
    assert execution_payload["run_outcome_class"] == "validation_failure"


def test_run_section_with_workspace_governed_blocked_execution_preserves_approval(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_governed_blocked")
    section_id = "mission-board"
    outputs_path = project_root / ".dce" / "outputs" / f"{section_id}.json"
    outputs_path.parent.mkdir(parents=True, exist_ok=True)
    outputs_path.write_text(
        json.dumps(
            {
                "section_id": section_id,
                "run_mode": "create_only",
                "run_outcome_class": "success_create_only",
                "file_plan": {
                    "project_name": "DGCE",
                    "files": [
                        {
                            "path": "api/missionboardservice.py",
                            "language": "python",
                            "purpose": "API surface",
                            "content": "stale-content",
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_index(project_root, [{"path": "api/missionboardservice.py", "section_id": section_id}])
    _write_text(project_root / "api" / "missionboardservice.py", "old-content")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        section_id,
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )

    result = run_section_with_workspace(
        _section(),
        project_root,
        allow_safe_modify=True,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    execution_payload = json.loads((project_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8"))
    approval_payload = json.loads((project_root / ".dce" / "approvals" / "mission-board.approval.json").read_text(encoding="utf-8"))

    assert result.run_outcome_class == "success_safe_modify"
    assert execution_payload["execution_status"] == "execution_completed"
    assert execution_payload["governed_execution"] is True
    assert execution_payload["execution_blocked"] is False
    assert execution_payload["approval_consumed"] is True
    assert execution_payload["approval_status_before"] == "approved"
    assert execution_payload["approval_status_after"] == "superseded"
    assert execution_payload["effective_execution_mode"] == "safe_modify"
    assert execution_payload["written_file_count"] == 4
    assert execution_payload["modify_written_count"] == 1
    assert execution_payload["created_written_count"] == 3
    assert approval_payload["approval_status"] == "superseded"


def test_record_section_execution_stamp_is_deterministic_with_fixed_inputs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v2_8_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v2_8_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    assert (first_root / ".dce" / "execution" / "mission-board.execution.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "execution" / "mission-board.execution.json"
    ).read_text(encoding="utf-8")


def test_record_section_execution_stamp_helper_derives_effective_mode_from_write_transparency(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v2_8_execution_helper")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)
    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2", allow_safe_modify=True)
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="safe_modify", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    stamp = record_section_execution_stamp(
        project_root,
        "mission-board",
        require_preflight_pass=True,
        execution=SectionExecutionStampInput(execution_timestamp="2026-03-26T00:00:00Z"),
        run_outcome_class="blocked_alignment",
        execution_blocked=True,
        write_transparency={"write_summary": {"written_count": 2, "modify_written_count": 1}},
    )

    assert stamp["execution_status"] == "execution_blocked"
    assert stamp["effective_execution_mode"] == "safe_modify"
    assert stamp["written_file_count"] == 0
    assert stamp["modify_written_count"] == 0
    assert stamp["created_written_count"] == 0
    assert [entry["artifact_role"] for entry in stamp["linked_artifacts"]] == [
        "approval",
        "preflight",
        "execution_gate",
        "alignment",
        "simulation_trigger",
        "simulation",
        "outputs",
    ]
    assert [entry["present"] for entry in stamp["linked_artifacts"]] == [True, False, False, False, False, False, False]
    assert stamp["artifact_results"] == []
    assert stamp["unit_results"] == []
    assert stamp["executed_units"] == []
    assert stamp["skipped_units"] == []
    assert stamp["failed_units"] == []
    assert stamp["execution_record_summary"] == {
        "execution_blocked": True,
        "execution_status": "execution_blocked",
        "executed_unit_count": 0,
        "linked_artifact_count": 7,
        "result_artifact_count": 0,
        "run_outcome_class": "blocked_alignment",
        "skipped_artifact_count": 0,
        "skipped_unit_count": 0,
        "written_artifact_count": 0,
    }


def test_run_section_with_workspace_lifecycle_trace_is_deterministic_and_governed(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v3_1_trace_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v3_1_trace_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_trace = json.loads((first_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"))
    second_trace = json.loads((second_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"))

    assert first_trace == second_trace
    assert (first_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "lifecycle_trace.json"
    ).read_text(encoding="utf-8")

    section_trace = first_trace["sections"][0]
    assert first_trace["lifecycle_order"] == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]
    assert [entry["stage"] for entry in section_trace["trace_entries"]] == [
        "preview",
        "review",
        "approval",
        "preflight",
        "gate",
        "alignment",
        "execution",
        "outputs",
    ]
    assert all(entry["artifact_present"] is True for entry in section_trace["trace_entries"])
    approval_entry = next(entry for entry in section_trace["trace_entries"] if entry["stage"] == "approval")
    execution_entry = next(entry for entry in section_trace["trace_entries"] if entry["stage"] == "execution")
    assert approval_entry["stage_status"] == "superseded"
    assert approval_entry["linkage"] == [
        {"ref_name": "preview_path", "ref_path": ".dce/plans/mission-board.preview.json"},
        {"ref_name": "review_path", "ref_path": ".dce/reviews/mission-board.review.md"},
        {"ref_name": "preflight_path", "ref_path": ".dce/preflight/mission-board.preflight.json"},
    ]
    assert execution_entry["stage_status"] == "execution_completed"
    assert execution_entry["linkage"] == [
        {"ref_name": "approval_path", "ref_path": ".dce/approvals/mission-board.approval.json"},
        {"ref_name": "preflight_path", "ref_path": ".dce/preflight/mission-board.preflight.json"},
        {"ref_name": "execution_gate_path", "ref_path": ".dce/execution/gate/mission-board.execution_gate.json"},
        {"ref_name": "alignment_path", "ref_path": ".dce/execution/alignment/mission-board.alignment.json"},
        {"ref_name": "simulation_trigger_path", "ref_path": ".dce/execution/simulation/mission-board.simulation_trigger.json"},
        {"ref_name": "simulation_path", "ref_path": None},
        {"ref_name": "output_path", "ref_path": ".dce/outputs/mission-board.json"},
    ]
    assert section_trace["trace_summary"] == {
        "available_artifact_count": 8,
        "approval_status": "superseded",
        "completed_stage_count": 8,
        "decision_source": "approval",
        "latest_decision": "create_only",
        "latest_decision_source": "approval",
        "latest_stage": "outputs",
        "latest_stage_status": "success_create_only",
        "review_status": "review_available",
        "section_id": "mission-board",
        "trace_entry_count": 8,
    }


def test_cross_artifact_section_summaries_converge_for_governed_run(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_convergence_governed_single")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )

    _assert_cross_artifact_section_consistency(project_root, "mission-board")


def test_dashboard_artifact_is_deterministic_for_repeated_governed_runs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_dashboard_repeat_a")
    second_root = _workspace_dir("dgce_dashboard_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_dashboard = json.loads((first_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))
    second_dashboard = json.loads((second_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))

    assert first_dashboard == second_dashboard
    assert (first_root / ".dce" / "dashboard.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "dashboard.json"
    ).read_text(encoding="utf-8")
    assert first_dashboard == {
        **_expected_artifact_metadata("dashboard"),
        "artifact_paths": {
            "lifecycle_trace_path": ".dce/lifecycle_trace.json",
            "review_index_path": ".dce/reviews/index.json",
            "workspace_index_path": ".dce/workspace_index.json",
        },
        "section_order": ["mission-board"],
        "sections": [
            {
                "approval_status": "superseded",
                "current_stage": "outputs",
                "decision_source": "approval",
                "entry_order": 1,
                "latest_decision": "create_only",
                "navigation_links": {
                    "approval": ".dce/approvals/mission-board.approval.json",
                    "execution": ".dce/execution/mission-board.execution.json",
                    "lifecycle_trace": ".dce/lifecycle_trace.json",
                    "outputs": ".dce/outputs/mission-board.json",
                    "review": ".dce/reviews/mission-board.review.md",
                },
                "progress": {
                    "available_artifact_count": 8,
                    "completed_stage_count": 8,
                    "lifecycle_stage_count": 8,
                    "trace_entry_count": 8,
                },
                "review_status": "review_available",
                "section_id": "mission-board",
                "section_summary": {
                    **_expected_section_summary(
                        section_id="mission-board",
                        approval_status="superseded",
                        decision_source="approval",
                        latest_decision="create_only",
                        latest_stage="outputs",
                        latest_stage_status="success_create_only",
                        review_status="review_available",
                    ),
                    "simulation": _explicit_non_triggered_simulation_projection(),
                    "summary_sources": {
                        **_expected_section_summary(
                            section_id="mission-board",
                            approval_status="superseded",
                            decision_source="approval",
                            latest_decision="create_only",
                            latest_stage="outputs",
                            latest_stage_status="success_create_only",
                            review_status="review_available",
                        )["summary_sources"],
                        "simulation": "simulation_trigger_record",
                    },
                },
                "stage_status": "success_create_only",
            }
        ],
        "summary": {
            "approval_status_counts": [{"section_count": 1, "value": "superseded"}],
            "current_stage_counts": [
                {"section_count": 0, "value": "preview"},
                {"section_count": 0, "value": "review"},
                {"section_count": 0, "value": "approval"},
                {"section_count": 0, "value": "preflight"},
                {"section_count": 0, "value": "gate"},
                {"section_count": 0, "value": "alignment"},
                {"section_count": 0, "value": "execution"},
                {"section_count": 1, "value": "outputs"},
            ],
            "review_status_counts": [{"section_count": 1, "value": "review_available"}],
            "stage_status_counts": [{"section_count": 1, "value": "success_create_only"}],
            "total_sections": 1,
        },
    }


def test_artifact_manifest_is_deterministic_for_repeated_governed_runs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_artifact_manifest_repeat_a")
    second_root = _workspace_dir("dgce_artifact_manifest_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_manifest = json.loads((first_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))
    second_manifest = json.loads((second_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))

    assert first_manifest == second_manifest
    assert (first_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "artifact_manifest.json"
    ).read_text(encoding="utf-8")


def test_consumer_contract_is_deterministic_for_repeated_governed_runs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_consumer_contract_repeat_a")
    second_root = _workspace_dir("dgce_consumer_contract_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_contract = _consumer_contract_payload(first_root)
    second_contract = _consumer_contract_payload(second_root)

    assert first_contract == second_contract
    assert (first_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "consumer_contract.json"
    ).read_text(encoding="utf-8")
    assert first_contract == {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": _expected_consumer_contract_supported_artifacts(),
    }
    _assert_exportable_contract_is_deterministic(first_root)
    _assert_exportable_contract_is_deterministic(second_root)


def test_consumer_contract_reference_is_deterministic_and_derived_from_consumer_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_consumer_contract_reference_repeat_a")
    second_root = _workspace_dir("dgce_consumer_contract_reference_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_contract = _consumer_contract_payload(first_root)
    first_reference = _consumer_contract_reference_text(first_root)
    second_reference = _consumer_contract_reference_text(second_root)

    assert first_reference == _expected_consumer_contract_reference(first_contract)
    assert first_reference == second_reference
    assert _consumer_contract_reference_text(first_root) == (second_root / ".dce" / "consumer_contract_reference.md").read_text(encoding="utf-8")
    _assert_reference_aligns_with_contract(first_root)
    _assert_reference_aligns_with_contract(second_root)


def test_export_contract_reference_is_deterministic_and_derived_from_export_contract(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_export_contract_reference_repeat_a")
    second_root = _workspace_dir("dgce_export_contract_reference_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), first_root)
    run_section_with_workspace(_section(), second_root)

    first_export = _export_contract_payload(first_root)
    second_export = _export_contract_payload(second_root)
    first_reference = _export_contract_reference_text(first_root)
    second_reference = _export_contract_reference_text(second_root)

    assert first_reference == _expected_export_contract_reference(first_export)
    assert second_reference == _expected_export_contract_reference(second_export)
    assert first_reference == second_reference
    assert _export_contract_reference_text(first_root) == (second_root / ".dce" / "export_contract_reference.md").read_text(encoding="utf-8")
    _assert_export_reference_aligns(first_root)
    _assert_export_reference_aligns(second_root)


def test_export_contract_is_deterministic_for_repeated_governed_runs(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_export_contract_repeat_a")
    second_root = _workspace_dir("dgce_export_contract_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_export = _export_contract_payload(first_root)
    second_export = _export_contract_payload(second_root)

    assert first_export == second_export
    assert (first_root / ".dce" / "export_contract.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "export_contract.json"
    ).read_text(encoding="utf-8")
    _assert_export_contract_aligns(first_root)
    _assert_export_contract_aligns(second_root)


def test_consumer_contract_schema_rejects_export_fields_outside_supported_fields():
    payload = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["sections[].section_id"],
            }
        ],
    }

    with pytest.raises(ValueError):
        dgce_decompose._validate_consumer_contract_schema(payload)


def test_export_contract_schema_rejects_non_external_scope_and_missing_export_fields():
    payload_missing = {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "contract_stability": "supported",
                "export_scope": "external",
            }
        ],
    }
    payload_internal = {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "contract_stability": "supported",
                "export_scope": "internal",
                "export_fields": ["section_order"],
            }
        ],
    }

    with pytest.raises(ValueError):
        dgce_decompose._validate_export_contract_schema(payload_missing)
    with pytest.raises(ValueError):
        dgce_decompose._validate_export_contract_schema(payload_internal)


def test_get_exportable_contract_filters_to_external_entries_and_preserves_order():
    payload = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order", "sections[].section_id"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            },
            {
                "artifact_type": "workspace_index",
                "schema_version": "1.0",
                "artifact_path": ".dce/workspace_index.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["sdk"],
                "export_scope": "internal",
            },
        ],
    }

    exportable = dgce_decompose._get_exportable_contract(payload)

    assert exportable == {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order", "sections[].section_id"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            }
        ],
    }


def test_build_export_contract_derives_strictly_from_exportable_contract():
    payload = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order", "sections[].section_id"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            },
            {
                "artifact_type": "workspace_index",
                "schema_version": "1.0",
                "artifact_path": ".dce/workspace_index.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["sdk"],
                "export_scope": "internal",
            },
        ],
    }

    assert dgce_decompose._build_export_contract(payload) == {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": dgce_decompose._get_exportable_contract(payload)["supported_artifacts"],
    }


def test_export_contract_full_convergence_rejects_missing_consumer_contract_entry():
    artifact_manifest = {
        **_expected_artifact_metadata("artifact_manifest"),
        "artifacts": [
            {
                "artifact_path": ".dce/dashboard.json",
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            }
        ],
    }
    consumer_contract = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [],
    }
    export_contract = {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "contract_stability": "supported",
                "export_scope": "external",
                "export_fields": ["section_order"],
            }
        ],
    }
    consumer_contract_reference = dgce_decompose._build_consumer_contract_reference(
        {
            **_expected_artifact_metadata("consumer_contract"),
            "supported_artifacts": [
                {
                    "artifact_type": "dashboard",
                    "schema_version": "1.0",
                    "artifact_path": ".dce/dashboard.json",
                    "supported_fields": ["section_order"],
                    "contract_stability": "supported",
                    "export_scope": "external",
                    "export_fields": ["section_order"],
                }
            ],
        }
    )

    with pytest.raises(ValueError, match="ordering must match consumer_contract.json export ordering exactly"):
        dgce_decompose._assert_export_contract_fully_converged(
            artifact_manifest,
            consumer_contract,
            consumer_contract_reference,
            export_contract,
        )


def test_export_contract_full_convergence_rejects_mismatched_schema_version():
    artifact_manifest = {
        **_expected_artifact_metadata("artifact_manifest"),
        "artifacts": [
            {
                "artifact_path": ".dce/dashboard.json",
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            }
        ],
    }
    consumer_contract = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            }
        ],
    }
    export_contract = dgce_decompose._build_export_contract(consumer_contract)
    export_contract["supported_artifacts"][0]["schema_version"] = "9.9"
    consumer_contract_reference = dgce_decompose._build_consumer_contract_reference(consumer_contract)

    with pytest.raises(ValueError, match="must match _get_exportable_contract|schema_version must match consumer_contract"):
        dgce_decompose._assert_export_contract_fully_converged(
            artifact_manifest,
            consumer_contract,
            consumer_contract_reference,
            export_contract,
        )


def test_export_contract_full_convergence_rejects_mismatched_export_fields():
    artifact_manifest = {
        **_expected_artifact_metadata("artifact_manifest"),
        "artifacts": [
            {
                "artifact_path": ".dce/dashboard.json",
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            }
        ],
    }
    consumer_contract = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order", "sections[].section_id"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            }
        ],
    }
    export_contract = dgce_decompose._build_export_contract(consumer_contract)
    export_contract["supported_artifacts"][0]["export_fields"] = ["sections[].section_id"]
    consumer_contract_reference = dgce_decompose._build_consumer_contract_reference(consumer_contract)

    with pytest.raises(ValueError, match="must match _get_exportable_contract|export_fields must match consumer_contract exportable fields"):
        dgce_decompose._assert_export_contract_fully_converged(
            artifact_manifest,
            consumer_contract,
            consumer_contract_reference,
            export_contract,
        )


def test_export_contract_matches_manifest_rejects_missing_manifest_entry():
    artifact_manifest = {
        **_expected_artifact_metadata("artifact_manifest"),
        "artifacts": [],
    }
    export_contract = {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "contract_stability": "supported",
                "export_scope": "external",
                "export_fields": ["section_order"],
            }
        ],
    }

    with pytest.raises(ValueError, match="must resolve to artifact_manifest"):
        dgce_decompose._assert_export_contract_matches_manifest(artifact_manifest, export_contract)


def test_export_contract_matches_consumer_contract_rejects_ordering_mismatch():
    consumer_contract = {
        **_expected_artifact_metadata("consumer_contract"),
        "supported_artifacts": [
            {
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "artifact_path": ".dce/dashboard.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["ui"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            },
            {
                "artifact_type": "workspace_index",
                "schema_version": "1.0",
                "artifact_path": ".dce/workspace_index.json",
                "supported_fields": ["section_order"],
                "contract_stability": "supported",
                "consumer_scopes": ["sdk"],
                "export_scope": "external",
                "export_fields": ["section_order"],
            },
        ],
    }
    export_contract = {
        **_expected_artifact_metadata("export_contract"),
        "supported_artifacts": list(reversed(dgce_decompose._get_exportable_contract(consumer_contract)["supported_artifacts"])),
    }

    with pytest.raises(ValueError, match="ordering must match consumer_contract.json export ordering exactly"):
        dgce_decompose._assert_export_contract_matches_consumer_contract(consumer_contract, export_contract)


def test_refresh_workspace_views_reuses_in_memory_builder_results_without_output_drift(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_refresh_efficiency")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section_named("Mission Board"),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    before_refresh = {
        "review_index": (project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"),
        "workspace_summary": (project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"),
        "lifecycle_trace": (project_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"),
        "workspace_index": (project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"),
        "dashboard": (project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"),
        "artifact_manifest": (project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"),
        "consumer_contract": (project_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8"),
        "export_contract": (project_root / ".dce" / "export_contract.json").read_text(encoding="utf-8"),
        "consumer_contract_reference": (project_root / ".dce" / "consumer_contract_reference.md").read_text(encoding="utf-8"),
        "export_contract_reference": (project_root / ".dce" / "export_contract_reference.md").read_text(encoding="utf-8"),
    }

    call_counts = {
        "review_index": 0,
        "lifecycle_trace": 0,
        "workspace_index": 0,
        "dashboard": 0,
        "artifact_manifest": 0,
        "consumer_contract": 0,
        "export_contract": 0,
        "consumer_contract_reference": 0,
        "export_contract_reference": 0,
    }
    original_build_review_index = dgce_decompose._build_review_index
    original_build_lifecycle_trace = dgce_decompose._build_lifecycle_trace
    original_build_workspace_index = dgce_decompose._build_workspace_index
    original_build_dashboard_view = dgce_decompose._build_dashboard_view
    original_build_artifact_manifest = dgce_decompose._build_artifact_manifest
    original_build_consumer_contract = dgce_decompose._build_consumer_contract
    original_build_export_contract = dgce_decompose._build_export_contract
    original_build_consumer_contract_reference = dgce_decompose._build_consumer_contract_reference
    original_build_export_contract_reference = dgce_decompose._build_export_contract_reference

    def counting_build_review_index(*args, **kwargs):
        call_counts["review_index"] += 1
        return original_build_review_index(*args, **kwargs)

    def counting_build_lifecycle_trace(*args, **kwargs):
        call_counts["lifecycle_trace"] += 1
        return original_build_lifecycle_trace(*args, **kwargs)

    def counting_build_workspace_index(*args, **kwargs):
        call_counts["workspace_index"] += 1
        return original_build_workspace_index(*args, **kwargs)

    def counting_build_dashboard_view(*args, **kwargs):
        call_counts["dashboard"] += 1
        return original_build_dashboard_view(*args, **kwargs)

    def counting_build_artifact_manifest(*args, **kwargs):
        call_counts["artifact_manifest"] += 1
        return original_build_artifact_manifest(*args, **kwargs)

    def counting_build_consumer_contract(*args, **kwargs):
        call_counts["consumer_contract"] += 1
        return original_build_consumer_contract(*args, **kwargs)

    def counting_build_export_contract(*args, **kwargs):
        call_counts["export_contract"] += 1
        return original_build_export_contract(*args, **kwargs)

    def counting_build_consumer_contract_reference(*args, **kwargs):
        call_counts["consumer_contract_reference"] += 1
        return original_build_consumer_contract_reference(*args, **kwargs)

    def counting_build_export_contract_reference(*args, **kwargs):
        call_counts["export_contract_reference"] += 1
        return original_build_export_contract_reference(*args, **kwargs)

    monkeypatch.setattr(dgce_decompose, "_build_review_index", counting_build_review_index)
    monkeypatch.setattr(dgce_decompose, "_build_lifecycle_trace", counting_build_lifecycle_trace)
    monkeypatch.setattr(dgce_decompose, "_build_workspace_index", counting_build_workspace_index)
    monkeypatch.setattr(dgce_decompose, "_build_dashboard_view", counting_build_dashboard_view)
    monkeypatch.setattr(dgce_decompose, "_build_artifact_manifest", counting_build_artifact_manifest)
    monkeypatch.setattr(dgce_decompose, "_build_consumer_contract", counting_build_consumer_contract)
    monkeypatch.setattr(dgce_decompose, "_build_export_contract", counting_build_export_contract)
    monkeypatch.setattr(dgce_decompose, "_build_consumer_contract_reference", counting_build_consumer_contract_reference)
    monkeypatch.setattr(dgce_decompose, "_build_export_contract_reference", counting_build_export_contract_reference)

    dgce_decompose._refresh_workspace_views(dgce_decompose._ensure_workspace(project_root))

    assert call_counts == {
        "review_index": 1,
        "lifecycle_trace": 1,
        "workspace_index": 1,
        "dashboard": 1,
        "artifact_manifest": 1,
        "consumer_contract": 1,
        "export_contract": 1,
        "consumer_contract_reference": 1,
        "export_contract_reference": 1,
    }
    assert before_refresh == {
        "review_index": (project_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8"),
        "workspace_summary": (project_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8"),
        "lifecycle_trace": (project_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8"),
        "workspace_index": (project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"),
        "dashboard": (project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"),
        "artifact_manifest": (project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"),
        "consumer_contract": (project_root / ".dce" / "consumer_contract.json").read_text(encoding="utf-8"),
        "export_contract": (project_root / ".dce" / "export_contract.json").read_text(encoding="utf-8"),
        "consumer_contract_reference": (project_root / ".dce" / "consumer_contract_reference.md").read_text(encoding="utf-8"),
        "export_contract_reference": (project_root / ".dce" / "export_contract_reference.md").read_text(encoding="utf-8"),
    }


def test_locked_artifact_schemas_accept_current_dgce_artifacts(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_schema_lock_success")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )

    for path in (
        project_root / ".dce" / "reviews" / "index.json",
        project_root / ".dce" / "workspace_summary.json",
        project_root / ".dce" / "lifecycle_trace.json",
        project_root / ".dce" / "workspace_index.json",
        project_root / ".dce" / "dashboard.json",
        project_root / ".dce" / "artifact_manifest.json",
        project_root / ".dce" / "consumer_contract.json",
        project_root / ".dce" / "export_contract.json",
        project_root / ".dce" / "outputs" / "mission-board.json",
        project_root / ".dce" / "execution" / "mission-board.execution.json",
    ):
        payload = json.loads(path.read_text(encoding="utf-8"))
        dgce_decompose._validate_locked_artifact_schema(path, payload)


def test_locked_artifact_schemas_reject_missing_required_fields(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_schema_lock_missing_field")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section(), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section(),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )

    artifact_cases = [
        (project_root / ".dce" / "reviews" / "index.json", ("sections", 0, "review_approval_summary")),
        (project_root / ".dce" / "workspace_summary.json", ("sections", 0, "section_summary")),
        (project_root / ".dce" / "lifecycle_trace.json", ("sections", 0, "trace_summary")),
        (project_root / ".dce" / "workspace_index.json", ("sections", 0, "section_summary")),
        (project_root / ".dce" / "dashboard.json", ("sections", 0, "progress")),
        (project_root / ".dce" / "artifact_manifest.json", ("artifacts", 0, "artifact_type")),
        (project_root / ".dce" / "consumer_contract.json", ("supported_artifacts", 0, "artifact_type")),
        (project_root / ".dce" / "export_contract.json", ("supported_artifacts", 0, "artifact_type")),
        (project_root / ".dce" / "outputs" / "mission-board.json", ("output_summary",)),
        (project_root / ".dce" / "execution" / "mission-board.execution.json", ("execution_record_summary",)),
    ]

    for path, field_path in artifact_cases:
        payload = json.loads(path.read_text(encoding="utf-8"))
        target = payload
        for segment in field_path[:-1]:
            target = target[segment]
        target.pop(field_path[-1])
        with pytest.raises(ValueError):
            dgce_decompose._validate_locked_artifact_schema(path, payload)


def test_locked_artifact_schema_dispatch_validates_only_exact_dgce_artifact_paths(monkeypatch):
    calls = {
        "review_index": 0,
        "workspace_summary": 0,
        "lifecycle_trace": 0,
        "workspace_index": 0,
        "dashboard": 0,
        "artifact_manifest": 0,
        "consumer_contract": 0,
        "export_contract": 0,
        "outputs": 0,
        "execution": 0,
    }

    monkeypatch.setattr(dgce_decompose, "_validate_review_index_schema", lambda payload: calls.__setitem__("review_index", calls["review_index"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_workspace_summary_schema", lambda payload: calls.__setitem__("workspace_summary", calls["workspace_summary"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_lifecycle_trace_schema", lambda payload: calls.__setitem__("lifecycle_trace", calls["lifecycle_trace"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_workspace_index_schema", lambda payload: calls.__setitem__("workspace_index", calls["workspace_index"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_dashboard_schema", lambda payload: calls.__setitem__("dashboard", calls["dashboard"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_artifact_manifest_schema", lambda payload: calls.__setitem__("artifact_manifest", calls["artifact_manifest"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_consumer_contract_schema", lambda payload: calls.__setitem__("consumer_contract", calls["consumer_contract"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_export_contract_schema", lambda payload: calls.__setitem__("export_contract", calls["export_contract"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_execution_output_schema", lambda payload: calls.__setitem__("outputs", calls["outputs"] + 1))
    monkeypatch.setattr(dgce_decompose, "_validate_execution_stamp_schema", lambda payload: calls.__setitem__("execution", calls["execution"] + 1))

    payload = {}
    valid_paths = [
        (Path("workspace/.dce/reviews/index.json"), "review_index"),
        (Path("workspace/.dce/workspace_summary.json"), "workspace_summary"),
        (Path("workspace/.dce/lifecycle_trace.json"), "lifecycle_trace"),
        (Path("workspace/.dce/workspace_index.json"), "workspace_index"),
        (Path("workspace/.dce/dashboard.json"), "dashboard"),
        (Path("workspace/.dce/artifact_manifest.json"), "artifact_manifest"),
        (Path("workspace/.dce/consumer_contract.json"), "consumer_contract"),
        (Path("workspace/.dce/export_contract.json"), "export_contract"),
        (Path("workspace/.dce/outputs/mission-board.json"), "outputs"),
        (Path("workspace/.dce/execution/mission-board.execution.json"), "execution"),
        (Path("workspace\\nested\\.dce\\outputs\\mission-board.json"), "outputs"),
        (Path("workspace\\nested\\.dce\\execution\\mission-board.execution.json"), "execution"),
    ]

    for path, expected_key in valid_paths:
        before = dict(calls)
        dgce_decompose._validate_locked_artifact_schema(path, payload)
        after = dict(calls)
        for key, value in after.items():
            expected_delta = 1 if key == expected_key else 0
            assert value - before[key] == expected_delta

    invalid_paths = [
        Path("workspace/.dce/outputs_nested/mission-board.json"),
        Path("workspace/.dce/not_outputs/mission-board.json"),
        Path("workspace/.dce/execution_nested/mission-board.execution.json"),
        Path("workspace/.dce/reviews/index.json.bak"),
        Path("workspace/.dce/dashboard.json.tmp"),
        Path("workspace/.dce/artifact_manifest.json.bak"),
        Path("workspace/.dce/consumer_contract.json.bak"),
        Path("workspace/.dce/export_contract.json.bak"),
        Path("workspace/.dce/outputs/mission-board.execution.json"),
        Path("workspace/.dce/execution/mission-board.json"),
        Path("workspace/similar/.dcex/outputs/mission-board.json"),
    ]

    before_invalid = dict(calls)
    for path in invalid_paths:
        dgce_decompose._validate_locked_artifact_schema(path, payload)
    assert calls == before_invalid


def test_run_section_with_workspace_workspace_index_is_deterministic_and_governed(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    first_root = _workspace_dir("dgce_incremental_v3_2_index_repeat_a")
    second_root = _workspace_dir("dgce_incremental_v3_2_index_repeat_b")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    for root in (first_root, second_root):
        run_section_with_workspace(_section(), root, incremental_mode="incremental_v2_2")
        record_section_approval(
            root,
            "mission-board",
            SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
        )
        run_section_with_workspace(
            _section(),
            root,
            require_preflight_pass=True,
            gate_timestamp="2026-03-26T00:00:00Z",
            preflight_validation_timestamp="2026-03-26T00:00:00Z",
            alignment_timestamp="2026-03-26T00:00:00Z",
            execution_timestamp="2026-03-26T00:00:00Z",
        )

    first_index = json.loads((first_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))
    second_index = json.loads((second_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))

    assert first_index == second_index
    assert (first_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "workspace_index.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "workspace_summary.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "workspace_summary.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "lifecycle_trace.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "lifecycle_trace.json"
    ).read_text(encoding="utf-8")
    assert (first_root / ".dce" / "reviews" / "index.json").read_text(encoding="utf-8") == (
        second_root / ".dce" / "reviews" / "index.json"
    ).read_text(encoding="utf-8")

    assert first_index["artifact_paths"] == {
        "lifecycle_trace_path": ".dce/lifecycle_trace.json",
        "review_index_path": ".dce/reviews/index.json",
        "workspace_summary_path": ".dce/workspace_summary.json",
    }
    assert first_index["section_order"] == ["mission-board"]
    assert first_index["summary"] == {
        "latest_stage_counts": [
            {"section_count": 0, "stage": "preview"},
            {"section_count": 0, "stage": "review"},
            {"section_count": 0, "stage": "approval"},
            {"section_count": 0, "stage": "preflight"},
            {"section_count": 0, "stage": "gate"},
            {"section_count": 0, "stage": "alignment"},
            {"section_count": 0, "stage": "execution"},
            {"section_count": 1, "stage": "outputs"},
        ],
        "sections_with_execution": 1,
        "sections_with_lifecycle_trace": 1,
        "sections_with_outputs": 1,
        "total_sections_seen": 1,
    }

    section_entry = first_index["sections"][0]
    assert section_entry["entry_order"] == 1
    assert section_entry["section_id"] == "mission-board"
    assert section_entry["lifecycle_trace_path"] == ".dce/lifecycle_trace.json"
    assert section_entry["execution_path"] == ".dce/execution/mission-board.execution.json"
    assert section_entry["output_path"] == ".dce/outputs/mission-board.json"
    assert section_entry["execution_status"] == "execution_completed"
    assert section_entry["approval_status"] == "superseded"
    assert section_entry["decision_source"] == "approval"
    assert section_entry["review_status"] == "review_available"
    assert section_entry["latest_decision"] == "create_only"
    assert section_entry["latest_decision_source"] == "approval"
    assert section_entry["latest_stage"] == "outputs"
    assert section_entry["latest_stage_status"] == "success_create_only"
    assert section_entry["latest_run_outcome_class"] == "success_create_only"
    assert section_entry["section_summary"] == {
        **_expected_section_summary(
            section_id="mission-board",
            approval_status="superseded",
            decision_source="approval",
            latest_decision="create_only",
            latest_stage="outputs",
            latest_stage_status="success_create_only",
            review_status="review_available",
        ),
        "simulation": _explicit_non_triggered_simulation_projection(),
        "summary_sources": {
            **_expected_section_summary(
                section_id="mission-board",
                approval_status="superseded",
                decision_source="approval",
                latest_decision="create_only",
                latest_stage="outputs",
                latest_stage_status="success_create_only",
                review_status="review_available",
            )["summary_sources"],
            "simulation": "simulation_trigger_record",
        },
    }
    assert [link["artifact_role"] for link in section_entry["artifact_links"]] == [
        "preview",
        "review",
        "approval",
        "preflight",
        "stale_check",
        "gate",
        "alignment",
        "simulation_trigger",
        "execution",
        "outputs",
    ]
    assert section_entry["artifact_links"] == [
        {"artifact_role": "preview", "path": ".dce/plans/mission-board.preview.json"},
        {"artifact_role": "review", "path": ".dce/reviews/mission-board.review.md"},
        {"artifact_role": "approval", "path": ".dce/approvals/mission-board.approval.json"},
        {"artifact_role": "preflight", "path": ".dce/preflight/mission-board.preflight.json"},
        {"artifact_role": "stale_check", "path": ".dce/preflight/mission-board.stale_check.json"},
        {"artifact_role": "gate", "path": ".dce/execution/gate/mission-board.execution_gate.json"},
        {"artifact_role": "alignment", "path": ".dce/execution/alignment/mission-board.alignment.json"},
        {"artifact_role": "simulation_trigger", "path": ".dce/execution/simulation/mission-board.simulation_trigger.json"},
        {"artifact_role": "execution", "path": ".dce/execution/mission-board.execution.json"},
        {"artifact_role": "outputs", "path": ".dce/outputs/mission-board.json"},
    ]
    assert section_entry["trace_summary"] == {
        "available_artifact_count": 8,
        "approval_status": "superseded",
        "completed_stage_count": 8,
        "decision_source": "approval",
        "latest_decision": "create_only",
        "latest_decision_source": "approval",
        "latest_stage": "outputs",
        "latest_stage_status": "success_create_only",
        "review_status": "review_available",
        "section_id": "mission-board",
        "trace_entry_count": 8,
    }


def test_workspace_index_is_sorted_and_isolated_across_sections(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_incremental_v3_2_index_multi_section")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section_named("Mission Board"),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    payload = json.loads((project_root / ".dce" / "workspace_index.json").read_text(encoding="utf-8"))

    assert payload["section_order"] == ["alpha-section", "mission-board"]
    assert [entry["section_id"] for entry in payload["sections"]] == ["alpha-section", "mission-board"]
    assert [entry["entry_order"] for entry in payload["sections"]] == [1, 2]
    assert payload["summary"] == {
        "latest_stage_counts": [
            {"section_count": 1, "stage": "preview"},
            {"section_count": 0, "stage": "review"},
            {"section_count": 0, "stage": "approval"},
            {"section_count": 0, "stage": "preflight"},
            {"section_count": 0, "stage": "gate"},
            {"section_count": 0, "stage": "alignment"},
            {"section_count": 0, "stage": "execution"},
            {"section_count": 1, "stage": "outputs"},
        ],
        "sections_with_execution": 1,
        "sections_with_lifecycle_trace": 2,
        "sections_with_outputs": 1,
        "total_sections_seen": 2,
    }

    alpha_entry = payload["sections"][0]
    mission_entry = payload["sections"][1]

    assert alpha_entry["artifact_links"] == [
        {"artifact_role": "preview", "path": ".dce/plans/alpha-section.preview.json"},
    ]
    assert mission_entry["artifact_links"] == [
        {"artifact_role": "preview", "path": ".dce/plans/mission-board.preview.json"},
        {"artifact_role": "review", "path": ".dce/reviews/mission-board.review.md"},
        {"artifact_role": "approval", "path": ".dce/approvals/mission-board.approval.json"},
        {"artifact_role": "preflight", "path": ".dce/preflight/mission-board.preflight.json"},
        {"artifact_role": "stale_check", "path": ".dce/preflight/mission-board.stale_check.json"},
        {"artifact_role": "gate", "path": ".dce/execution/gate/mission-board.execution_gate.json"},
        {"artifact_role": "alignment", "path": ".dce/execution/alignment/mission-board.alignment.json"},
        {"artifact_role": "simulation_trigger", "path": ".dce/execution/simulation/mission-board.simulation_trigger.json"},
        {"artifact_role": "execution", "path": ".dce/execution/mission-board.execution.json"},
        {"artifact_role": "outputs", "path": ".dce/outputs/mission-board.json"},
    ]
    assert [link["artifact_role"] for link in alpha_entry["artifact_links"]] == ["preview"]
    assert [link["artifact_role"] for link in mission_entry["artifact_links"]] == [
        "preview",
        "review",
        "approval",
        "preflight",
        "stale_check",
        "gate",
        "alignment",
        "simulation_trigger",
        "execution",
        "outputs",
    ]

    assert alpha_entry["execution_path"] is None
    assert alpha_entry["output_path"] is None
    assert alpha_entry["lifecycle_trace_path"] == ".dce/lifecycle_trace.json"
    assert alpha_entry["approval_status"] is None
    assert alpha_entry["decision_source"] == "preview_recommendation"
    assert alpha_entry["review_status"] is None
    assert alpha_entry["latest_decision"] == "review_required"
    assert alpha_entry["latest_decision_source"] == "preview_recommendation"
    assert alpha_entry["trace_summary"] == {
        "available_artifact_count": 1,
        "approval_status": None,
        "completed_stage_count": 1,
        "decision_source": "preview_recommendation",
        "latest_decision": "review_required",
        "latest_decision_source": "preview_recommendation",
        "latest_stage": "preview",
        "latest_stage_status": "preview_blocked_modify_disabled",
        "review_status": None,
        "section_id": "alpha-section",
        "trace_entry_count": 8,
    }

    assert mission_entry["execution_path"] == ".dce/execution/mission-board.execution.json"
    assert mission_entry["output_path"] == ".dce/outputs/mission-board.json"
    assert mission_entry["lifecycle_trace_path"] == ".dce/lifecycle_trace.json"
    assert mission_entry["approval_status"] == "superseded"
    assert mission_entry["decision_source"] == "approval"
    assert mission_entry["review_status"] == "review_available"
    assert mission_entry["latest_decision"] == "create_only"
    assert mission_entry["latest_decision_source"] == "approval"
    assert mission_entry["trace_summary"] == {
        "available_artifact_count": 8,
        "approval_status": "superseded",
        "completed_stage_count": 8,
        "decision_source": "approval",
        "latest_decision": "create_only",
        "latest_decision_source": "approval",
        "latest_stage": "outputs",
        "latest_stage_status": "success_create_only",
        "review_status": "review_available",
        "section_id": "mission-board",
        "trace_entry_count": 8,
    }

    assert all("mission-board" not in link["path"] for link in alpha_entry["artifact_links"])
    assert all("alpha-section" not in link["path"] for link in mission_entry["artifact_links"])


def test_cross_artifact_section_summaries_converge_across_mixed_section_states(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_convergence_multi_section")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section_named("Mission Board"),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    _assert_cross_artifact_section_consistency(project_root, "alpha-section")
    _assert_cross_artifact_section_consistency(project_root, "mission-board")
    _assert_dashboard_links_resolve_to_manifest(project_root)
    _assert_workspace_index_links_resolve_to_manifest(project_root)
    _assert_review_and_trace_links_resolve_to_manifest(project_root)
    _assert_consumer_contract_aligns_with_manifest(project_root)
    _assert_export_contract_aligns(project_root)
    _assert_reference_aligns_with_contract(project_root)
    _assert_export_reference_aligns(project_root)
    _assert_exportable_contract_is_deterministic(project_root)
    _assert_consumer_contract_has_no_cross_section_leakage(project_root, "alpha-section", "mission-board")


def test_dashboard_artifact_has_stable_multi_section_ordering_and_summary_counts(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_dashboard_multi_section")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section_named("Mission Board"),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    payload = json.loads((project_root / ".dce" / "dashboard.json").read_text(encoding="utf-8"))

    assert payload["section_order"] == ["alpha-section", "mission-board"]
    assert [entry["section_id"] for entry in payload["sections"]] == ["alpha-section", "mission-board"]
    assert [entry["entry_order"] for entry in payload["sections"]] == [1, 2]
    assert payload["summary"] == {
        "approval_status_counts": [
            {"section_count": 1, "value": "none"},
            {"section_count": 1, "value": "superseded"},
        ],
        "current_stage_counts": [
            {"section_count": 1, "value": "preview"},
            {"section_count": 0, "value": "review"},
            {"section_count": 0, "value": "approval"},
            {"section_count": 0, "value": "preflight"},
            {"section_count": 0, "value": "gate"},
            {"section_count": 0, "value": "alignment"},
            {"section_count": 0, "value": "execution"},
            {"section_count": 1, "value": "outputs"},
        ],
        "review_status_counts": [
            {"section_count": 1, "value": "none"},
            {"section_count": 1, "value": "review_available"},
        ],
        "stage_status_counts": [
            {"section_count": 1, "value": "preview_blocked_modify_disabled"},
            {"section_count": 1, "value": "success_create_only"},
        ],
        "total_sections": 2,
    }

    alpha_entry = payload["sections"][0]
    mission_entry = payload["sections"][1]

    assert alpha_entry["navigation_links"] == {
        "approval": None,
        "execution": None,
        "lifecycle_trace": ".dce/lifecycle_trace.json",
        "outputs": None,
        "review": None,
    }
    assert mission_entry["navigation_links"] == {
        "approval": ".dce/approvals/mission-board.approval.json",
        "execution": ".dce/execution/mission-board.execution.json",
        "lifecycle_trace": ".dce/lifecycle_trace.json",
        "outputs": ".dce/outputs/mission-board.json",
        "review": ".dce/reviews/mission-board.review.md",
    }
    assert alpha_entry["section_summary"] == _expected_section_summary(
        section_id="alpha-section",
        decision_source="preview_recommendation",
        latest_decision="review_required",
        latest_stage="preview",
        latest_stage_status="preview_blocked_modify_disabled",
    )
    assert mission_entry["section_summary"] == {
        **_expected_section_summary(
            section_id="mission-board",
            approval_status="superseded",
            decision_source="approval",
            latest_decision="create_only",
            latest_stage="outputs",
            latest_stage_status="success_create_only",
            review_status="review_available",
        ),
        "simulation": _explicit_non_triggered_simulation_projection(),
        "summary_sources": {
            **_expected_section_summary(
                section_id="mission-board",
                approval_status="superseded",
                decision_source="approval",
                latest_decision="create_only",
                latest_stage="outputs",
                latest_stage_status="success_create_only",
                review_status="review_available",
            )["summary_sources"],
            "simulation": "simulation_trigger_record",
        },
    }


def test_artifact_manifest_has_stable_multi_section_ordering_and_correct_entries(monkeypatch):
    monkeypatch.setattr("aether_core.config.OLLAMA_ENABLED", False)
    project_root = _workspace_dir("dgce_artifact_manifest_multi_section")

    def fake_run(self, executor_name, content):
        return _stub_executor_result(content)

    monkeypatch.setattr("aether_core.router.executors.StubExecutors.run", fake_run)

    run_section_with_workspace(_section_named("Mission Board"), project_root, incremental_mode="incremental_v2_2")
    record_section_approval(
        project_root,
        "mission-board",
        SectionApprovalInput(approval_status="approved", selected_mode="create_only", approval_timestamp="2026-03-26T00:00:00Z"),
    )
    run_section_with_workspace(
        _section_named("Mission Board"),
        project_root,
        require_preflight_pass=True,
        gate_timestamp="2026-03-26T00:00:00Z",
        preflight_validation_timestamp="2026-03-26T00:00:00Z",
        alignment_timestamp="2026-03-26T00:00:00Z",
        execution_timestamp="2026-03-26T00:00:00Z",
    )
    run_section_with_workspace(_section_named("Alpha Section"), project_root, incremental_mode="incremental_v2")

    payload = json.loads((project_root / ".dce" / "artifact_manifest.json").read_text(encoding="utf-8"))

    assert payload == {
        **_expected_artifact_metadata("artifact_manifest"),
        "artifacts": [
            {
                "artifact_path": ".dce/reviews/index.json",
                "artifact_type": "review_index",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/workspace_summary.json",
                "artifact_type": "workspace_summary",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/lifecycle_trace.json",
                "artifact_type": "lifecycle_trace",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/workspace_index.json",
                "artifact_type": "workspace_index",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/dashboard.json",
                "artifact_type": "dashboard",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/artifact_manifest.json",
                "artifact_type": "artifact_manifest",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/consumer_contract.json",
                "artifact_type": "consumer_contract",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/export_contract.json",
                "artifact_type": "export_contract",
                "schema_version": "1.0",
                "scope": "workspace",
                "section_id": None,
            },
            {
                "artifact_path": ".dce/plans/alpha-section.preview.json",
                "artifact_type": "preview_artifact",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "alpha-section",
            },
            {
                "artifact_path": ".dce/plans/mission-board.preview.json",
                "artifact_type": "preview_artifact",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/reviews/mission-board.review.md",
                "artifact_type": "review_artifact",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/approvals/mission-board.approval.json",
                "artifact_type": "approval_artifact",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/preflight/mission-board.preflight.json",
                "artifact_type": "preflight_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/preflight/mission-board.stale_check.json",
                "artifact_type": "stale_check_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/execution/gate/mission-board.execution_gate.json",
                "artifact_type": "execution_gate_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/execution/alignment/mission-board.alignment.json",
                "artifact_type": "alignment_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/execution/simulation/mission-board.simulation_trigger.json",
                "artifact_type": "simulation_trigger_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/execution/mission-board.execution.json",
                "artifact_type": "execution_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
            {
                "artifact_path": ".dce/outputs/mission-board.json",
                "artifact_type": "output_record",
                "schema_version": "1.0",
                "scope": "section",
                "section_id": "mission-board",
            },
        ],
    }


def test_build_run_outcome_class_treats_skipped_modify_zero_write_run_as_execution_no_changes():
    execution_outcome = {
        "status": "error",
        "validation_summary": {
            "ok": True,
            "error": None,
            "missing_keys": [],
        },
        "change_plan_summary": {
            "create_count": 0,
            "modify_count": 3,
            "ignore_count": 0,
        },
        "execution_summary": {
            "written_files_count": 0,
            "skipped_modify_count": 3,
            "skipped_ignore_count": 0,
            "skipped_identical_count": 0,
            "skipped_ownership_count": 0,
            "skipped_exists_fallback_count": 0,
        },
    }

    assert _build_run_outcome_class("create_only", execution_outcome) == "execution_no_changes"
