"""Thin HTTP transport for DGCE read-only artifact access."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query

from aether.dgce import read_api


router = APIRouter(prefix="/v1/dgce")


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
