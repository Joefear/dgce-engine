"""Health endpoint for the local Aether API."""

from fastapi import APIRouter


router = APIRouter()


@router.get("/health")
def health_check() -> dict:
    """Return a simple health response."""
    return {"status": "ok"}
