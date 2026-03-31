"""Telemetry endpoint for the local Aether API."""

from fastapi import APIRouter, Request


router = APIRouter(prefix="/v1")


@router.get("/telemetry/{request_id}")
def get_telemetry(request_id: str, request: Request) -> dict:
    """Return telemetry events for a request."""
    events = request.app.state.telemetry_logger.get_events(request_id)
    return {"request_id": request_id, "events": events}
