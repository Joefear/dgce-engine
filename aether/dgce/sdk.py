"""Thin Python SDK client for DGCE read-only HTTP endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class DGCEClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _get(self, endpoint: str, workspace_path: str | Path) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        request = Request(f"{self.base_url}{endpoint}?{query}", method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = self._read_error_detail(exc)
            if exc.code == 400:
                raise ValueError(detail) from exc
            if exc.code == 404:
                raise FileNotFoundError(detail) from exc
            raise RuntimeError(detail) from exc

    @staticmethod
    def _read_error_detail(exc: HTTPError) -> str:
        payload = json.loads(exc.read().decode("utf-8"))
        return str(payload["detail"])

    def get_dashboard(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/dashboard", workspace_path)

    def get_workspace_index(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/workspace-index", workspace_path)

    def get_lifecycle_trace(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/lifecycle-trace", workspace_path)

    def get_consumer_contract(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/consumer-contract", workspace_path)

    def get_export_contract(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/export-contract", workspace_path)

    def get_artifact_manifest(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/artifact-manifest", workspace_path)

    def list_available_artifacts(self, workspace_path: str | Path) -> dict[str, Any]:
        return self.get_artifact_manifest(workspace_path)
