"""Local FastAPI app for Aether v1."""

import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from starlette.requests import Request

from aether.dgce.config import get_config
from aether_core.classifier.service import ClassificationService
from aether_core.itera.artifact_store import ArtifactStore
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.router.planner import RouterPlanner
from aether_core.telemetry.logger import TelemetryLogger
from apps.aether_api.routers import health, classify, decide, execute, telemetry, promote, dgce, version
from aether.dgce.read_api_http import router as dgce_read_router

logger = logging.getLogger("aether.api")


def create_app(
    telemetry_path: Optional[Path] = None,
    cache_path: Optional[Path] = None,
    artifact_store_path: Optional[Path] = None,
) -> FastAPI:
    """Create the local Aether API app."""
    app = FastAPI(title="Aether API", version="1.0")

    @app.on_event("startup")
    async def load_runtime_config() -> None:
        app.state.dgce_config = get_config()
        logger.info(
            "Aether API startup complete",
            extra={"dgce_api_key_configured": app.state.dgce_config["api_key"] is not None},
        )

    @app.middleware("http")
    async def log_request_response(request: Request, call_next):
        response = await call_next(request)
        logger.info(
            "Aether API request complete",
            extra={
                "request_method": request.method,
                "request_path": request.url.path,
                "status_code": response.status_code,
            },
        )
        return response

    app.state.classification_service = ClassificationService()
    app.state.telemetry_logger = TelemetryLogger(telemetry_path)
    app.state.router_planner = RouterPlanner(
        cache=ExactMatchCache(cache_path),
        artifact_store=ArtifactStore(artifact_store_path),
    )

    app.include_router(health.router)
    app.include_router(version.router)
    app.include_router(classify.router)
    app.include_router(decide.router)
    app.include_router(execute.router)
    app.include_router(dgce.router)
    app.include_router(dgce_read_router)
    app.include_router(promote.router)
    app.include_router(telemetry.router)

    return app


app = create_app()
