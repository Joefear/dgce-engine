"""Decision endpoint for the local Aether API."""

from fastapi import APIRouter, Request

from aether_core.classifier.rules import ClassifierRules
from apps.aether_api.routers.classify import _build_request


router = APIRouter(prefix="/v1")


@router.post("/decide")
def decide_request(payload: dict, request: Request) -> dict:
    """Return classifier and guardrail decision without execution."""
    classification_request = _build_request(payload)
    telemetry = request.app.state.telemetry_logger
    telemetry.log_event(
        classification_request.request_id,
        "request_received",
        {"path": "/v1/decide"},
    )

    classifier_result = ClassifierRules().classify(classification_request.content)
    classification_result = request.app.state.classification_service.classify(classification_request)

    telemetry.log_event(
        classification_request.request_id,
        "classification_completed",
        {
            "task_bucket": classifier_result["bucket"].value,
            "classifier_confidence": classifier_result["confidence"].value,
            "status": classification_result.status.value,
        },
    )
    telemetry.log_event(
        classification_request.request_id,
        "guardrail_decision",
        {
            "status": classification_result.status.value,
            "explanation": classification_result.explanation,
        },
    )
    telemetry.log_event(
        classification_request.request_id,
        "response_returned",
        {"path": "/v1/decide", "status": classification_result.status.value},
    )

    return {
        "request_id": classification_request.request_id,
        "task_bucket": classifier_result["bucket"].value,
        "classifier_confidence": classifier_result["confidence"].value,
        "status": classification_result.status.value,
        "explanation": classification_result.explanation,
    }
