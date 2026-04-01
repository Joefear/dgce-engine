"""Governed DGCE section execution helpers for the HTTP transport layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aether.dgce import run_dgce_section
from aether.dgce.decompose import SectionAlignmentInput, _build_alignment_artifact
from aether.dgce.file_plan import FilePlan
from aether.dgce.incremental import (
    build_incremental_change_plan,
    build_write_transparency,
    load_owned_paths,
    scan_workspace_file_paths,
)
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import (
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


def execute_prepared_section(workspace_path: str | Path, section_id: str, *, rerun: bool = False) -> dict[str, str | bool]:
    project_root = resolve_workspace_path(workspace_path)
    preparation = prepare_section_execution(project_root, section_id, persist_prepared_plan=False)
    if _has_prior_execution_artifacts(project_root, section_id) and rerun is not True:
        raise ValueError(f"Section has prior execution artifacts; rerun=true required: {section_id}")
    if preparation["eligible"] is not True:
        raise ValueError(f"Section is not eligible for execution: {section_id}")
    _assert_prepared_plan_binding_matches(project_root, section_id)
    prepared_file_plan = load_prepared_section_file_plan(project_root, section_id)
    if rerun is True and _has_prior_execution_artifacts(project_root, section_id):
        _assert_rerun_is_safe(project_root, section_id, prepared_file_plan)

    result = run_dgce_section(section_id, project_root, governed=True, prepared_file_plan=prepared_file_plan)
    if str(result.status) != "success":
        raise ValueError(f"Section execution blocked: {result.reason}")

    return {
        "status": "ok",
        "section_id": section_id,
        "executed": True,
        "artifacts_updated": True,
    }
