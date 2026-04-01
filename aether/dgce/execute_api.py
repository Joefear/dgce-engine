"""Governed DGCE section execution helpers for the HTTP transport layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aether.dgce import run_dgce_section
from aether.dgce.decompose import (
    SectionAlignmentInput,
    _build_alignment_artifact,
    _write_json,
    compute_json_payload_fingerprint,
)
from aether.dgce.file_plan import FilePlan
from aether.dgce.incremental import (
    build_incremental_change_plan,
    build_write_transparency,
    load_owned_paths,
    scan_workspace_file_paths,
)
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import (
    _compute_prepared_plan_approval_lineage,
    _compute_prepared_plan_binding,
    load_prepared_section_file_plan,
    load_prepared_section_plan_artifact,
    prepare_section_execution,
)


def _approval_payload(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = project_root / ".dce" / "approvals" / f"{section_id}.approval.json"
    if not approval_path.exists():
        return {}
    return json.loads(approval_path.read_text(encoding="utf-8"))


def _has_prior_execution_artifacts(project_root: Path, section_id: str) -> bool:
    return any(
        (
            project_root / ".dce" / directory / filename
        ).exists()
        for directory, filename in (
            ("execution", f"{section_id}.execution.json"),
            ("outputs", f"{section_id}.json"),
        )
    )


def _assert_rerun_is_safe(project_root: Path, section_id: str, file_plan: FilePlan) -> None:
    approval_payload = _approval_payload(project_root, section_id)
    selected_mode = str(approval_payload.get("selected_mode"))
    change_plan = build_incremental_change_plan(
        section_id,
        file_plan,
        scan_workspace_file_paths(project_root),
        project_root=project_root,
    )["changes"]
    _, write_transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=selected_mode == "safe_modify",
        owned_paths=load_owned_paths(project_root / ".dce" / "ownership_index.json"),
    )
    write_summary = dict(write_transparency.get("write_summary", {}))
    if int(write_summary.get("skipped_ownership_count", 0)) > 0:
        raise ValueError(f"Section rerun failed ownership validation: {section_id}")
    if int(write_summary.get("skipped_modify_count", 0)) > 0:
        raise ValueError(f"Section rerun requires safe_modify approval: {section_id}")

    alignment_artifact = _build_alignment_artifact(
        project_root / ".dce",
        section_id,
        require_preflight_pass=True,
        alignment_input=SectionAlignmentInput(),
        write_transparency=write_transparency,
    )
    if alignment_artifact.get("alignment_blocked") is True:
        raise ValueError(f"Section rerun failed safe modify validation: {section_id}")


def _assert_prepared_plan_binding_matches(project_root: Path, section_id: str) -> None:
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    current_binding = _compute_prepared_plan_binding(project_root, section_id)
    if prepared_plan.get("binding") != current_binding:
        raise ValueError(f"Prepared file plan binding mismatch: {section_id}")


def _assert_prepared_plan_approval_lineage_matches(project_root: Path, section_id: str) -> None:
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    current_lineage = _compute_prepared_plan_approval_lineage(project_root, section_id)
    if prepared_plan.get("approval_lineage") != current_lineage:
        raise ValueError(f"Prepared file plan approval lineage mismatch: {section_id}")


def _prepared_plan_relative_path(section_id: str) -> str:
    return f".dce/plans/{section_id}.prepared_plan.json"


def _execution_artifact_path(project_root: Path, section_id: str) -> Path:
    return project_root / ".dce" / "execution" / f"{section_id}.execution.json"


def _build_prepared_plan_audit_manifest(
    *,
    section_id: str,
    prepared_plan: dict[str, Any],
    execution_artifact: dict[str, Any],
) -> dict[str, Any]:
    written_files = execution_artifact.get("written_files")
    if not isinstance(written_files, list):
        raise ValueError(f"Execution audit manifest requires valid written_files: {section_id}")
    selected_mode = execution_artifact.get("selected_mode")
    if selected_mode is not None and not isinstance(selected_mode, str):
        raise ValueError(f"Execution audit manifest requires valid selected_mode: {section_id}")
    execution_status = execution_artifact.get("execution_status")
    if not isinstance(execution_status, str):
        raise ValueError(f"Execution audit manifest requires valid execution_status: {section_id}")
    return {
        "approval_lineage_fingerprint": prepared_plan["approval_lineage_fingerprint"],
        "binding_fingerprint": prepared_plan["binding_fingerprint"],
        "execution_permitted": bool(prepared_plan["binding"]["execution_permitted"]),
        "execution_status": execution_status,
        "prepared_plan_fingerprint": compute_json_payload_fingerprint(prepared_plan),
        "prepared_plan_path": _prepared_plan_relative_path(section_id),
        "section_id": section_id,
        "selected_mode": selected_mode,
        "written_files": written_files,
    }


def _build_prepared_plan_cross_link(
    *,
    section_id: str,
    prepared_plan: dict[str, Any],
    prepared_plan_audit_fingerprint: str,
) -> dict[str, Any]:
    return {
        "prepared_plan_audit_fingerprint": prepared_plan_audit_fingerprint,
        "prepared_plan_fingerprint": compute_json_payload_fingerprint(prepared_plan),
        "prepared_plan_path": _prepared_plan_relative_path(section_id),
        "section_id": section_id,
    }


def _persist_prepared_plan_audit_manifest(
    project_root: Path,
    section_id: str,
    prepared_plan: dict[str, Any],
) -> None:
    execution_path = _execution_artifact_path(project_root, section_id)
    execution_artifact = json.loads(execution_path.read_text(encoding="utf-8"))
    if not isinstance(execution_artifact, dict):
        raise ValueError(f"Execution audit manifest requires valid execution artifact: {section_id}")
    audit_manifest = _build_prepared_plan_audit_manifest(
        section_id=section_id,
        prepared_plan=prepared_plan,
        execution_artifact=execution_artifact,
    )
    execution_artifact["prepared_plan_audit_manifest"] = audit_manifest
    execution_artifact["prepared_plan_audit_fingerprint"] = compute_json_payload_fingerprint(audit_manifest)
    prepared_plan_cross_link = _build_prepared_plan_cross_link(
        section_id=section_id,
        prepared_plan=prepared_plan,
        prepared_plan_audit_fingerprint=execution_artifact["prepared_plan_audit_fingerprint"],
    )
    execution_artifact["prepared_plan_cross_link"] = prepared_plan_cross_link
    execution_artifact["prepared_plan_cross_link_fingerprint"] = compute_json_payload_fingerprint(prepared_plan_cross_link)
    _write_json(execution_path, execution_artifact)


def execute_prepared_section(workspace_path: str | Path, section_id: str, *, rerun: bool = False) -> dict[str, str | bool]:
    project_root = resolve_workspace_path(workspace_path)
    preparation = prepare_section_execution(project_root, section_id, persist_prepared_plan=False)
    if _has_prior_execution_artifacts(project_root, section_id) and rerun is not True:
        raise ValueError(f"Section has prior execution artifacts; rerun=true required: {section_id}")
    if preparation["eligible"] is not True:
        raise ValueError(f"Section is not eligible for execution: {section_id}")
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    _assert_prepared_plan_approval_lineage_matches(project_root, section_id)
    _assert_prepared_plan_binding_matches(project_root, section_id)
    prepared_file_plan = load_prepared_section_file_plan(project_root, section_id)
    if rerun is True and _has_prior_execution_artifacts(project_root, section_id):
        _assert_rerun_is_safe(project_root, section_id, prepared_file_plan)

    result = run_dgce_section(section_id, project_root, governed=True, prepared_file_plan=prepared_file_plan)
    if str(result.status) != "success":
        raise ValueError(f"Section execution blocked: {result.reason}")
    _persist_prepared_plan_audit_manifest(project_root, section_id, prepared_plan)

    return {
        "status": "ok",
        "section_id": section_id,
        "executed": True,
        "artifacts_updated": True,
    }
