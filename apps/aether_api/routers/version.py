"""Version endpoint for the local Aether API."""

from fastapi import APIRouter


router = APIRouter()


@router.get("/version")
def version_info() -> dict[str, str]:
    return {
        "service": "aether-api",
        "dgce_version": "5.x",
        "api_version": "v1",
    }
