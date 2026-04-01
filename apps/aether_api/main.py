"""Local FastAPI app for Aether v1."""

from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import Optional
from uuid import uuid4

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = get_config()
    app.state.dgce_config = config
    logger.info(
        "DGCE API startup",
        extra={"dgce_api_key_configured": config["api_key"] is not None},
    )
    yield


def create_app(
    telemetry_path: Optional[Path] = None,
    cache_path: Optional[Path] = None,
    artifact_store_path: Optional[Path] = None,
) -> FastAPI:
    """Create the local Aether API app."""
    app = FastAPI(title="Aether API", version="1.0", lifespan=lifespan)

    @app.middleware("http")
    async def log_request_response(request: Request, call_next):
        request_id = str(uuid4())
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "0"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["X-Request-ID"] = request_id
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
