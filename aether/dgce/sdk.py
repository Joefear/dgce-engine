"""Thin Python SDK client for DGCE read-only HTTP endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


class DGCEClient:
    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _get(self, endpoint: str, workspace_path: str | Path) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        headers = {"X-API-Key": self.api_key} if self.api_key is not None else {}
        request = Request(f"{self.base_url}{endpoint}?{query}", headers=headers, method="GET")
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

    def list_gce_stage0_artifacts(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/gce/stage0-artifacts", workspace_path)

    def get_gce_stage0_artifact(self, workspace_path: str | Path, artifact_name: str) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        headers = {"X-API-Key": self.api_key} if self.api_key is not None else {}
        request = Request(
            f"{self.base_url}/v1/dgce/gce/stage0-artifacts/{quote(artifact_name, safe='')}?{query}",
            headers=headers,
            method="GET",
        )
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

    def list_game_adapter_stage2_preview_artifacts(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/game-adapter/stage2-preview-artifacts", workspace_path)

    def get_game_adapter_stage2_preview_artifact(self, workspace_path: str | Path, artifact_name: str) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        headers = {"X-API-Key": self.api_key} if self.api_key is not None else {}
        request = Request(
            f"{self.base_url}/v1/dgce/game-adapter/stage2-preview-artifacts/{quote(artifact_name, safe='')}?{query}",
            headers=headers,
            method="GET",
        )
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

    def list_game_adapter_unreal_project_structure_manifests(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/game-adapter/unreal-project-structure-manifests", workspace_path)

    def get_game_adapter_unreal_project_structure_manifest(
        self,
        workspace_path: str | Path,
        artifact_name: str,
    ) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        headers = {"X-API-Key": self.api_key} if self.api_key is not None else {}
        request = Request(
            f"{self.base_url}/v1/dgce/game-adapter/unreal-project-structure-manifests/{quote(artifact_name, safe='')}?{query}",
            headers=headers,
            method="GET",
        )
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

    def list_game_adapter_unreal_symbol_candidate_indexes(self, workspace_path: str | Path) -> dict[str, Any]:
        return self._get("/v1/dgce/game-adapter/unreal-symbol-candidate-indexes", workspace_path)

    def get_game_adapter_unreal_symbol_candidate_index(
        self,
        workspace_path: str | Path,
        artifact_name: str,
    ) -> dict[str, Any]:
        query = urlencode({"workspace_path": str(workspace_path)})
        headers = {"X-API-Key": self.api_key} if self.api_key is not None else {}
        request = Request(
            f"{self.base_url}/v1/dgce/game-adapter/unreal-symbol-candidate-indexes/{quote(artifact_name, safe='')}?{query}",
            headers=headers,
            method="GET",
        )
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

    def list_available_artifacts(self, workspace_path: str | Path) -> dict[str, Any]:
        return self.get_artifact_manifest(workspace_path)
