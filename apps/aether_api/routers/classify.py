"""Classification endpoint for the local Aether API."""

from fastapi import APIRouter, HTTPException, Request

from aether_core.models import ClassificationRequest


router = APIRouter(prefix="/v1")


def _build_request(payload: dict) -> ClassificationRequest:
    """Construct a ClassificationRequest from JSON payload."""
    try:
        return ClassificationRequest(
            content=payload["content"],
            request_id=payload["request_id"],
            preset=payload.get("preset"),
            project=payload.get("project"),
            task_type=payload.get("task_type"),
            priority=payload.get("priority"),
            user=payload.get("user"),
            reuse_scope=payload.get("reuse_scope"),
            metadata=payload.get("metadata"),
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"Missing field: {exc.args[0]}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/classify")
def classify_request(payload: dict, request: Request) -> dict:
    """Run classification only."""
    classification_request = _build_request(payload)
    telemetry = request.app.state.telemetry_logger
    telemetry.log_event(
        classification_request.request_id,
        "request_received",
        {"path": "/v1/classify"},
    )

    result = request.app.state.classification_service.classify(classification_request)

    telemetry.log_event(
        classification_request.request_id,
        "classification_completed",
        {
            "status": result.status.value,
            "explanation": result.explanation,
        },
    )
    telemetry.log_event(
        classification_request.request_id,
        "guardrail_decision",
        {"status": result.status.value},
    )
    telemetry.log_event(
        classification_request.request_id,
        "response_returned",
        {"path": "/v1/classify", "status": result.status.value},
    )

    return {
        "request_id": result.request_id,
        "status": result.status.value,
        "content": result.content,
        "output": result.output,
        "explanation": result.explanation,
        "processing_time_ms": result.processing_time_ms,
    }
