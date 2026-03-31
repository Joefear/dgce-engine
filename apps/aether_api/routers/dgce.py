"""DGCE section orchestration endpoint for the local Aether API."""

from fastapi import APIRouter, Request

from aether.dgce import DGCESection, run_section


router = APIRouter(prefix="/v1")


@router.post("/dgce/section")
def run_dgce_section(section: DGCESection, request: Request) -> dict:
    """Run a DGCE section through the deterministic creation loop."""
    result = run_section(
        section,
        classification_service=request.app.state.classification_service,
        router_planner=request.app.state.router_planner,
    )
    return result.model_dump()
