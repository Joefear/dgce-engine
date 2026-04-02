"""DGCE section orchestration endpoint for the local Aether API."""

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from aether.dgce import DGCESection, run_section
from aether.dgce.approve_api import approve_section_execution
from aether.dgce.execute_api import (
    execute_prepared_section,
    execute_prepared_section_bundle,
    get_bundle_index_records_by_input_fingerprint,
    get_bundle_operator_overview,
    get_bundle_operator_summary,
    get_bundle_index_records_for_section,
    get_section_operator_overview,
    get_section_operator_summary,
    get_section_provenance,
    load_bundle_execution_manifest,
    verify_bundle_artifact_chain,
    verify_section_artifact_chain,
)
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import prepare_section_execution
from aether.dgce.refresh_api import refresh_workspace_artifacts


router = APIRouter(prefix="/v1")


class WorkspacePathRequest(BaseModel):
    workspace_path: str
    rerun: bool = False


class SectionApprovalRequest(BaseModel):
    workspace_path: str
    approved_by: str = "operator"
    notes: str = ""
    selected_mode: str | None = None


class SectionBundleExecutionRequest(BaseModel):
    workspace_path: str
    section_ids: list[str]
    rerun: bool = False


@router.post("/dgce/section")
def run_dgce_section(section: DGCESection, request: Request) -> dict:
    """Run a DGCE section through the deterministic creation loop."""
    result = run_section(
        section,
        classification_service=request.app.state.classification_service,
        router_planner=request.app.state.router_planner,
    )
    return result.model_dump()


@router.post("/dgce/refresh")
def refresh_dgce_workspace(payload: WorkspacePathRequest) -> dict[str, str | bool]:
    try:
        project_root = refresh_workspace_artifacts(payload.workspace_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "status": "ok",
        "workspace": str(project_root),
        "artifacts_refreshed": True,
    }


@router.post("/dgce/sections/{section_id}/prepare")
def prepare_dgce_section(section_id: str, payload: WorkspacePathRequest) -> dict[str, str | bool | dict[str, bool]]:
    try:
        return prepare_section_execution(payload.workspace_path, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/dgce/sections/{section_id}/approve")
def approve_dgce_section(section_id: str, payload: SectionApprovalRequest) -> dict[str, str | bool]:
    try:
        return approve_section_execution(
            payload.workspace_path,
            section_id,
            approved_by=payload.approved_by,
            notes=payload.notes,
            selected_mode=payload.selected_mode,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/dgce/sections/{section_id}/execute")
def execute_dgce_section(section_id: str, payload: WorkspacePathRequest) -> dict[str, str | bool]:
    try:
        return execute_prepared_section(payload.workspace_path, section_id, rerun=payload.rerun)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/dgce/sections/execute-bundle")
def execute_dgce_section_bundle(payload: SectionBundleExecutionRequest):
    result, status_code = execute_prepared_section_bundle(
        payload.workspace_path,
        payload.section_ids,
        rerun=payload.rerun,
    )
    return JSONResponse(status_code=status_code, content=result)


@router.get("/dgce/bundles/{bundle_fingerprint}")
def get_dgce_bundle(bundle_fingerprint: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return load_bundle_execution_manifest(project_root, bundle_fingerprint)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dgce/bundles/by-input/{bundle_input_fingerprint}")
def get_dgce_bundles_by_input(bundle_input_fingerprint: str, workspace_path: str = Query(...)) -> list[dict]:
    try:
        project_root = resolve_workspace_path(workspace_path)
        records = get_bundle_index_records_by_input_fingerprint(project_root, bundle_input_fingerprint)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not records:
        raise HTTPException(status_code=404, detail=f"Bundle input fingerprint not found: {bundle_input_fingerprint}")
    return records


@router.get("/dgce/sections/{section_id}/bundles")
def get_dgce_section_bundles(section_id: str, workspace_path: str = Query(...)) -> list[dict]:
    try:
        project_root = resolve_workspace_path(workspace_path)
        records = get_bundle_index_records_for_section(project_root, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not records:
        raise HTTPException(status_code=404, detail=f"Section bundle participation not found: {section_id}")
    return records


@router.get("/dgce/sections/{section_id}/provenance")
def get_dgce_section_provenance(section_id: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return get_section_provenance(project_root, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dgce/sections/{section_id}/verify")
def verify_dgce_section(section_id: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return verify_section_artifact_chain(project_root, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dgce/sections/{section_id}/summary")
def get_dgce_section_summary(section_id: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return get_section_operator_summary(project_root, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dgce/sections/{section_id}/overview")
def get_dgce_section_overview(section_id: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return get_section_operator_overview(project_root, section_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dgce/bundles/{bundle_fingerprint}/verify")
def verify_dgce_bundle(bundle_fingerprint: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return verify_bundle_artifact_chain(project_root, bundle_fingerprint)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dgce/bundles/{bundle_fingerprint}/summary")
def get_dgce_bundle_summary(bundle_fingerprint: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return get_bundle_operator_summary(project_root, bundle_fingerprint)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dgce/bundles/{bundle_fingerprint}/overview")
def get_dgce_bundle_overview(bundle_fingerprint: str, workspace_path: str = Query(...)) -> dict:
    try:
        project_root = resolve_workspace_path(workspace_path)
        return get_bundle_operator_overview(project_root, bundle_fingerprint)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
