"""Thin HTTP transport for DGCE read-only artifact access."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from aether.dgce import read_api
from aether.dgce.config import get_config


ROUTE_POLICIES = {
    "/health": "public",
    "/version": "public",
    "/v1/dgce/": "read",
}


def resolve_scope(path: str) -> str:
    if path in ROUTE_POLICIES:
        return str(ROUTE_POLICIES[path])
    for route_path, scope in ROUTE_POLICIES.items():
        if path.startswith(route_path):
            return str(scope)
    return "read"


def _require_api_key(request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    expected_key = get_config()["api_key"]
    if expected_key is None:
        return
    scope = resolve_scope(request.url.path)
    if scope == "read" and x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


router = APIRouter(prefix="/v1/dgce", dependencies=[Depends(_require_api_key)])


def _read_artifact_over_http(
    reader: Callable[[str | Path], dict[str, Any]],
    workspace_path: str,
) -> dict[str, Any]:
    try:
        return reader(workspace_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _read_named_artifact_over_http(
    reader: Callable[[str | Path, str], dict[str, Any]],
    workspace_path: str,
    artifact_name: str,
) -> dict[str, Any]:
    try:
        return reader(workspace_path, artifact_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dashboard")
def get_dashboard(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_dashboard, workspace_path)


@router.get("/workspace-index")
def get_workspace_index(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_workspace_index, workspace_path)


@router.get("/lifecycle-trace")
def get_lifecycle_trace(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_lifecycle_trace, workspace_path)


@router.get("/consumer-contract")
def get_consumer_contract(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_consumer_contract, workspace_path)


@router.get("/export-contract")
def get_export_contract(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_export_contract, workspace_path)


@router.get("/artifact-manifest")
def get_artifact_manifest(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.get_artifact_manifest, workspace_path)


@router.get("/gce/stage0-artifacts")
def list_gce_stage0_artifacts(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.list_gce_stage0_artifacts, workspace_path)


@router.get("/gce/stage0-artifacts/{artifact_name}")
def get_gce_stage0_artifact(artifact_name: str, workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_named_artifact_over_http(read_api.get_gce_stage0_artifact, workspace_path, artifact_name)


@router.get("/game-adapter/stage2-preview-artifacts")
def list_game_adapter_stage2_preview_artifacts(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.list_game_adapter_stage2_preview_artifacts, workspace_path)


@router.get("/game-adapter/stage2-preview-artifacts/{artifact_name}")
def get_game_adapter_stage2_preview_artifact(artifact_name: str, workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_game_adapter_stage2_preview_artifact,
        workspace_path,
        artifact_name,
    )


@router.get("/game-adapter/unreal-project-structure-manifests")
def list_game_adapter_unreal_project_structure_manifests(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.list_game_adapter_unreal_project_structure_manifests, workspace_path)


@router.get("/game-adapter/unreal-project-structure-manifests/{artifact_name}")
def get_game_adapter_unreal_project_structure_manifest(
    artifact_name: str,
    workspace_path: str = Query(...),
) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_game_adapter_unreal_project_structure_manifest,
        workspace_path,
        artifact_name,
    )


@router.get("/game-adapter/unreal-symbol-candidate-indexes")
def list_game_adapter_unreal_symbol_candidate_indexes(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.list_game_adapter_unreal_symbol_candidate_indexes, workspace_path)


@router.get("/game-adapter/unreal-symbol-candidate-indexes/{artifact_name}")
def get_game_adapter_unreal_symbol_candidate_index(
    artifact_name: str,
    workspace_path: str = Query(...),
) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_game_adapter_unreal_symbol_candidate_index,
        workspace_path,
        artifact_name,
    )


@router.get("/game-adapter/unreal-symbol-resolutions")
def list_game_adapter_unreal_symbol_resolver_outputs(workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_artifact_over_http(read_api.list_game_adapter_unreal_symbol_resolver_outputs, workspace_path)


@router.get("/game-adapter/unreal-symbol-resolutions/{artifact_name}")
def get_game_adapter_unreal_symbol_resolver_output(
    artifact_name: str,
    workspace_path: str = Query(...),
) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_game_adapter_unreal_symbol_resolver_output,
        workspace_path,
        artifact_name,
    )


@router.get("/game-adapter/stage3-review-bundles/{section_id}")
def get_game_adapter_stage3_review_bundle_read_model(
    section_id: str,
    workspace_path: str = Query(...),
) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_game_adapter_stage3_review_bundle_read_model,
        workspace_path,
        section_id,
    )


@router.get("/stage7/alignment/{section_id}")
def get_stage7_alignment_read_model(section_id: str, workspace_path: str = Query(...)) -> dict[str, Any]:
    return _read_named_artifact_over_http(
        read_api.get_stage7_alignment_read_model,
        workspace_path,
        section_id,
    )
