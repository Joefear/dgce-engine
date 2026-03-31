"""Promotion endpoint for the local Aether API."""

from fastapi import APIRouter, HTTPException, Request

from aether_core.enums import ArtifactStatus


router = APIRouter(prefix="/v1")

_EXECUTION_METADATA_KEYS = {
    "real_model_called",
    "model_backend",
    "model_name",
    "estimated_tokens",
    "estimated_cost",
    "inference_avoided",
    "backend_used",
    "worth_running",
}


@router.post("/promote/{artifact_id}")
def promote_artifact(artifact_id: str, request: Request) -> dict:
    """Manually promote an artifact and seed exact-match reuse."""
    planner = request.app.state.router_planner
    telemetry = request.app.state.telemetry_logger

    try:
        record = planner.artifact_store.promote_to_approved(artifact_id)
    except ValueError as exc:
        telemetry.log_event(
            artifact_id,
            "promotion_failed",
            {
                "artifact_id": artifact_id,
                "reason": "artifact_not_found",
            },
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    cache_seeded = False
    try:
        stripped_context = {
            k: v
            for k, v in record.context.items()
            if k not in _EXECUTION_METADATA_KEYS
        }
        planner.cache.store(
            task_bucket=record.task_bucket,
            content=record.content,
            output=record.output,
            status=ArtifactStatus.APPROVED,
            context=planner.cache.scope_context(
                stripped_context,
                stripped_context.get("reuse_scope", "strict"),
            ),
        )
        cache_seeded = True
    except OSError:
        pass

    telemetry.log_event(
        artifact_id,
        "promotion_completed",
        {
            "artifact_id": artifact_id,
            "task_bucket": record.task_bucket,
            "seeded_exact_cache": cache_seeded,
        },
    )

    return {
        "artifact_id": artifact_id,
        "status": record.status,
        "seeded_exact_cache": cache_seeded,
    }
