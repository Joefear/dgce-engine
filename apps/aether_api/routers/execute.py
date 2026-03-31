"""Execution endpoint for the local Aether API."""

from fastapi import APIRouter, Request

from apps.aether_api.routers.classify import _build_request


router = APIRouter(prefix="/v1")


@router.post("/execute")
def execute_request(payload: dict, request: Request) -> dict:
    """Run the full local pipeline."""
    classification_request = _build_request(payload)
    request_context = classification_request.context_dict()
    telemetry = request.app.state.telemetry_logger
    telemetry.log_event(
        classification_request.request_id,
        "request_received",
        {**request_context, "path": "/v1/execute"},
    )

    classification_result = request.app.state.classification_service.classify(classification_request)
    route_result = request.app.state.router_planner.route(classification_request, classification_result)
    summary = _build_summary(route_result, classification_request)

    telemetry.log_event(
        classification_request.request_id,
        "classification_completed",
        {
            **request_context,
            "task_bucket": route_result.task_bucket,
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
        "reuse_hit" if route_result.reused else "reuse_miss",
        {"decision": route_result.decision, "status": route_result.status.value},
    )
    telemetry.log_event(
        classification_request.request_id,
        "execution_path_taken",
        {
            **request_context,
            **route_result.execution_metadata,
            "decision": route_result.decision,
            "reused": route_result.reused,
        },
    )
    telemetry.log_event(
        classification_request.request_id,
        "response_returned",
        {
            **request_context,
            "path": "/v1/execute",
            "status": route_result.status.value,
            "summary": summary,
        },
    )

    return {
        "request_id": classification_request.request_id,
        "classification": {
            "status": classification_result.status.value,
            "explanation": classification_result.explanation,
        },
        "route": {
            "decision": route_result.decision,
            "reused": route_result.reused,
            "task_bucket": route_result.task_bucket,
        },
        "status": route_result.status.value,
        "output": route_result.output,
        "summary": summary,
    }


def _build_summary(route_result, classification_request) -> dict:
    """Build a compact human-readable execution summary from existing routing data."""
    metadata = route_result.execution_metadata or {}
    return {
        "final_decision": route_result.decision,
        "reused": route_result.reused,
        "worth_running": metadata.get("worth_running", False),
        "inference_avoided": metadata.get("inference_avoided", False),
        "backend_used": metadata.get("backend_used", "unknown"),
        "estimated_tokens": metadata.get("estimated_tokens", 0),
        "estimated_cost": metadata.get("estimated_cost", 0),
        "artifact_status": route_result.status.value,
        "project": classification_request.project,
        "task_type": classification_request.task_type,
        "priority": classification_request.priority,
        "reuse_scope": classification_request.reuse_scope_value(),
        "short_reason": _short_reason(route_result),
    }


def _short_reason(route_result) -> str:
    """Return a concise explanation for the final execution path."""
    if route_result.decision == "BLOCKED":
        return "Blocked by policy - execution prevented"
    if route_result.reused:
        return "Reused approved artifact - inference avoided"
    return "No reusable artifact - execution required"
