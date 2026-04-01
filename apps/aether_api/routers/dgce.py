"""DGCE section orchestration endpoint for the local Aether API."""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from aether.dgce import DGCESection, run_section
from aether.dgce.prepare_api import prepare_section_execution
from aether.dgce.refresh_api import refresh_workspace_artifacts


router = APIRouter(prefix="/v1")


class WorkspacePathRequest(BaseModel):
    workspace_path: str


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
def refresh_dgce_workspace(workspace_path: str = Query(...)) -> dict[str, str | bool]:
    try:
        project_root = refresh_workspace_artifacts(workspace_path)
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
