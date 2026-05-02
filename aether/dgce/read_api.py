"""Read-only validated accessors for DGCE workspace artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aether.dgce.context_assembly import STAGE0_CONTRACT_NAME, STAGE0_CONTRACT_VERSION
from aether.dgce.decompose import _validate_locked_artifact_schema
from aether.dgce.decompose import verify_artifact_fingerprint
from aether.dgce.game_adapter_preview import (
    ADAPTER as GAME_ADAPTER_STAGE2_PREVIEW_ADAPTER,
    ARTIFACT_TYPE as GAME_ADAPTER_STAGE2_PREVIEW_ARTIFACT_TYPE,
    CONTRACT_NAME as GAME_ADAPTER_STAGE2_PREVIEW_CONTRACT_NAME,
    CONTRACT_VERSION as GAME_ADAPTER_STAGE2_PREVIEW_CONTRACT_VERSION,
    DOMAIN as GAME_ADAPTER_STAGE2_PREVIEW_DOMAIN,
    validate_game_adapter_stage2_preview_contract,
)
from aether.dgce.game_adapter_unreal_manifest import (
    ADAPTER as UNREAL_MANIFEST_ADAPTER,
    ARTIFACT_TYPE as UNREAL_MANIFEST_ARTIFACT_TYPE,
    CONTRACT_NAME as UNREAL_MANIFEST_CONTRACT_NAME,
    CONTRACT_VERSION as UNREAL_MANIFEST_CONTRACT_VERSION,
    DOMAIN as UNREAL_MANIFEST_DOMAIN,
    validate_unreal_project_structure_manifest,
)
from aether.dgce.game_adapter_unreal_symbol_candidates import (
    ARTIFACT_TYPE as UNREAL_SYMBOL_CANDIDATE_INDEX_ARTIFACT_TYPE,
    CONTRACT_NAME as UNREAL_SYMBOL_CANDIDATE_INDEX_CONTRACT_NAME,
    CONTRACT_VERSION as UNREAL_SYMBOL_CANDIDATE_INDEX_CONTRACT_VERSION,
    validate_unreal_symbol_candidate_index,
)
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import (
    CONTRACT_NAME as UNREAL_SYMBOL_RESOLVER_OUTPUT_CONTRACT_NAME,
    CONTRACT_VERSION as UNREAL_SYMBOL_RESOLVER_OUTPUT_CONTRACT_VERSION,
    OUTPUT_ARTIFACT_TYPE as UNREAL_SYMBOL_RESOLVER_OUTPUT_ARTIFACT_TYPE,
    validate_resolver_output_contract,
)
from aether.dgce.gce_ingestion import compute_gce_clarification_request_fingerprint
from aether.dgce.path_utils import resolve_workspace_path


GCE_STAGE0_READ_MODEL_CONTRACT_NAME = "GCEStage0ReadModel"
GCE_STAGE0_READ_MODEL_CONTRACT_VERSION = "gce.stage0.read_model.v1"
GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_NAME = "DGCEGameAdapterStage2PreviewReadModel"
GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_VERSION = "dgce.game_adapter.stage2.preview.read_model.v1"
UNREAL_MANIFEST_READ_MODEL_CONTRACT_NAME = "DGCEGameAdapterUnrealProjectStructureManifestReadModel"
UNREAL_MANIFEST_READ_MODEL_CONTRACT_VERSION = "dgce.game_adapter.unreal_project_structure_manifest.read_model.v1"
UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_NAME = "DGCEGameAdapterUnrealSymbolCandidateIndexReadModel"
UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_VERSION = "dgce.game_adapter.unreal_symbol_candidate_index.read_model.v1"
UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_NAME = "DGCEGameAdapterUnrealSymbolResolverOutputReadModel"
UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_VERSION = "dgce.game_adapter.unreal_symbol_resolver.output.read_model.v1"


def _workspace_root_path(workspace_path: str | Path) -> Path:
    return resolve_workspace_path(workspace_path)


def _artifact_file_path(workspace_path: str | Path, *parts: str) -> Path:
    return _workspace_root_path(workspace_path) / ".dce" / Path(*parts)


def _read_validated_json_artifact(workspace_path: str | Path, *parts: str) -> dict[str, Any]:
    artifact_path = _artifact_file_path(workspace_path, *parts)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{artifact_path.name} must contain a JSON object")
    _validate_locked_artifact_schema(artifact_path, payload)
    return payload


def get_dashboard(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "dashboard.json")


def get_workspace_index(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "workspace_index.json")


def get_lifecycle_trace(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "lifecycle_trace.json")


def get_consumer_contract(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "consumer_contract.json")


def get_export_contract(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "export_contract.json")


def get_artifact_manifest(workspace_path: str | Path) -> dict[str, Any]:
    return _read_validated_json_artifact(workspace_path, "artifact_manifest.json")


def list_available_artifacts(workspace_path: str | Path) -> dict[str, Any]:
    return get_artifact_manifest(workspace_path)


def list_gce_stage0_artifacts(workspace_path: str | Path) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_dir = workspace_root / ".dce" / "input" / "gce"
    artifacts = []
    if artifact_dir.is_dir():
        artifacts = [
            _read_gce_stage0_artifact_file(path, workspace_root=workspace_root)
            for path in sorted(artifact_dir.glob("*.stage0.json"), key=lambda candidate: candidate.name)
        ]
    return {
        "artifact_type": "gce_stage0_artifact_index",
        "adapter": "gce",
        "contract_name": GCE_STAGE0_READ_MODEL_CONTRACT_NAME,
        "contract_version": GCE_STAGE0_READ_MODEL_CONTRACT_VERSION,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_gce_stage0_artifact(workspace_path: str | Path, artifact_name: str) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_path = _gce_stage0_artifact_path(workspace_root, artifact_name)
    if artifact_path is None:
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=None,
            input_path=None,
            reason_code="artifact_name_invalid",
            source_artifact_fingerprint=None,
        )
    if not artifact_path.exists():
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=_artifact_path_for_read_model(artifact_path, workspace_root),
            input_path=None,
            reason_code="artifact_missing",
            source_artifact_fingerprint=None,
        )
    return _read_gce_stage0_artifact_file(artifact_path, workspace_root=workspace_root)


def list_game_adapter_stage2_preview_artifacts(workspace_path: str | Path) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_dir = workspace_root / ".dce" / "plans"
    artifacts = []
    if artifact_dir.is_dir():
        artifacts = [
            _read_game_adapter_stage2_preview_file(path, workspace_root=workspace_root)
            for path in sorted(artifact_dir.glob("game-adapter-stage2*.preview.json"), key=lambda candidate: candidate.name)
        ]
    return {
        "artifact_type": "game_adapter_stage2_preview_index",
        "adapter": GAME_ADAPTER_STAGE2_PREVIEW_ADAPTER,
        "domain": GAME_ADAPTER_STAGE2_PREVIEW_DOMAIN,
        "contract_name": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_NAME,
        "contract_version": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_VERSION,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_game_adapter_stage2_preview_artifact(workspace_path: str | Path, artifact_name: str) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_path = _game_adapter_stage2_preview_artifact_path(workspace_root, artifact_name)
    if artifact_path is None:
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=None,
            reason_code="artifact_name_invalid",
            source_artifact_fingerprint=None,
        )
    if not artifact_path.exists():
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=_artifact_path_for_read_model(artifact_path, workspace_root),
            reason_code="artifact_missing",
            source_artifact_fingerprint=None,
        )
    return _read_game_adapter_stage2_preview_file(artifact_path, workspace_root=workspace_root)


def list_game_adapter_unreal_project_structure_manifests(workspace_path: str | Path) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_dir = workspace_root / ".dce" / "plans"
    artifacts = []
    if artifact_dir.is_dir():
        artifacts = [
            _read_unreal_project_structure_manifest_file(path, workspace_root=workspace_root)
            for path in sorted(
                artifact_dir.glob("unreal-project-structure*.manifest.json"),
                key=lambda candidate: candidate.name,
            )
        ]
    return {
        "artifact_type": "game_adapter_unreal_project_structure_manifest_index",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_MANIFEST_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_MANIFEST_READ_MODEL_CONTRACT_VERSION,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_game_adapter_unreal_project_structure_manifest(
    workspace_path: str | Path,
    artifact_name: str,
) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_path = _unreal_project_structure_manifest_artifact_path(workspace_root, artifact_name)
    if artifact_path is None:
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=None,
            reason_code="artifact_name_invalid",
            source_artifact_fingerprint=None,
        )
    if not artifact_path.exists():
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=_artifact_path_for_read_model(artifact_path, workspace_root),
            reason_code="artifact_missing",
            source_artifact_fingerprint=None,
        )
    return _read_unreal_project_structure_manifest_file(artifact_path, workspace_root=workspace_root)


def list_game_adapter_unreal_symbol_candidate_indexes(workspace_path: str | Path) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_dir = workspace_root / ".dce" / "plans"
    artifacts = []
    if artifact_dir.is_dir():
        artifacts = [
            _read_unreal_symbol_candidate_index_file(path, workspace_root=workspace_root)
            for path in sorted(
                artifact_dir.glob("unreal-symbol-candidates*.index.json"),
                key=lambda candidate: candidate.name,
            )
        ]
    return {
        "artifact_type": "game_adapter_unreal_symbol_candidate_index_index",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_VERSION,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_game_adapter_unreal_symbol_candidate_index(
    workspace_path: str | Path,
    artifact_name: str,
) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_path = _unreal_symbol_candidate_index_artifact_path(workspace_root, artifact_name)
    if artifact_path is None:
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=None,
            reason_code="artifact_name_invalid",
            source_artifact_fingerprint=None,
        )
    if not artifact_path.exists():
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=_artifact_path_for_read_model(artifact_path, workspace_root),
            reason_code="artifact_missing",
            source_artifact_fingerprint=None,
        )
    return _read_unreal_symbol_candidate_index_file(artifact_path, workspace_root=workspace_root)


def list_game_adapter_unreal_symbol_resolver_outputs(workspace_path: str | Path) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_dir = workspace_root / ".dce" / "plans"
    artifacts = []
    if artifact_dir.is_dir():
        artifacts = [
            _read_unreal_symbol_resolver_output_file(path, workspace_root=workspace_root)
            for path in sorted(
                artifact_dir.glob("unreal-symbol-resolver*.resolution.json"),
                key=lambda candidate: candidate.name,
            )
        ]
    return {
        "artifact_type": "game_adapter_unreal_symbol_resolver_output_index",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_VERSION,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_game_adapter_unreal_symbol_resolver_output(
    workspace_path: str | Path,
    artifact_name: str,
) -> dict[str, Any]:
    workspace_root = _workspace_root_path(workspace_path)
    artifact_path = _unreal_symbol_resolver_output_artifact_path(workspace_root, artifact_name)
    if artifact_path is None:
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=None,
            reason_code="artifact_name_invalid",
            source_artifact_fingerprint=None,
        )
    if not artifact_path.exists():
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=_artifact_path_for_read_model(artifact_path, workspace_root),
            reason_code="artifact_missing",
            source_artifact_fingerprint=None,
        )
    return _read_unreal_symbol_resolver_output_file(artifact_path, workspace_root=workspace_root)


def _gce_stage0_artifact_path(workspace_root: Path, artifact_name: str) -> Path | None:
    name_path = Path(artifact_name)
    if name_path.name != artifact_name or not artifact_name.endswith(".stage0.json"):
        return None
    return workspace_root / ".dce" / "input" / "gce" / artifact_name


def _read_gce_stage0_artifact_file(path: Path, *, workspace_root: Path) -> dict[str, Any]:
    artifact_name = path.name
    artifact_path = _artifact_path_for_read_model(path, workspace_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            input_path=None,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )
    if not isinstance(payload, dict):
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            input_path=None,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )

    input_path = _string_or_none(payload.get("input_path"))
    artifact_fingerprint = _string_or_none(payload.get("artifact_fingerprint"))
    if artifact_fingerprint is None:
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            input_path=input_path,
            reason_code="artifact_fingerprint_missing",
            source_artifact_fingerprint=None,
        )
    if not verify_artifact_fingerprint(path):
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            input_path=input_path,
            reason_code="artifact_fingerprint_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )
    if not _gce_stage0_artifact_core_shape_is_valid(payload):
        return _gce_stage0_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            input_path=input_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=artifact_fingerprint,
        )

    return {
        "read_model_type": "gce_stage0_artifact_read_model",
        "adapter": "gce",
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "artifact_type": payload["artifact_type"],
        "contract_name": payload["contract_name"],
        "contract_version": payload["contract_version"],
        "input_path": payload["input_path"],
        "reason_code": _stage0_reason_code(payload),
        "stage_1_release": dict(payload["stage_1_release"]),
        "artifact_fingerprint": artifact_fingerprint,
        "normalized_session_intent_summary": _normalized_session_intent_summary(
            payload.get("normalized_session_intent")
        ),
        "clarification_request_summary": _clarification_request_summary(payload.get("clarification_request")),
    }


def _game_adapter_stage2_preview_artifact_path(workspace_root: Path, artifact_name: str) -> Path | None:
    name_path = Path(artifact_name)
    if (
        name_path.name != artifact_name
        or not artifact_name.startswith("game-adapter-stage2")
        or not artifact_name.endswith(".preview.json")
    ):
        return None
    return workspace_root / ".dce" / "plans" / artifact_name


def _read_game_adapter_stage2_preview_file(path: Path, *, workspace_root: Path) -> dict[str, Any]:
    artifact_name = path.name
    artifact_path = _artifact_path_for_read_model(path, workspace_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )
    if not isinstance(payload, dict):
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )

    artifact_fingerprint = _string_or_none(payload.get("artifact_fingerprint"))
    if artifact_fingerprint is None:
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_missing",
            source_artifact_fingerprint=None,
        )
    if not verify_artifact_fingerprint(path):
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )
    try:
        validate_game_adapter_stage2_preview_contract(payload)
    except ValueError:
        return _game_adapter_stage2_preview_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="contract_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )

    planned_changes = payload["planned_changes"]
    governance_context = payload["governance_context"]
    return {
        "read_model_type": "game_adapter_stage2_preview_read_model",
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "artifact_type": payload["artifact_type"],
        "contract_name": payload["contract_name"],
        "contract_version": payload["contract_version"],
        "read_model_contract_name": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_NAME,
        "read_model_contract_version": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_VERSION,
        "adapter": payload["adapter"],
        "domain": payload["domain"],
        "source_stage0_fingerprint": _string_or_none(payload.get("source_stage0_fingerprint")),
        "source_input_reference": _string_or_none(payload.get("source_input_reference")),
        "artifact_fingerprint": artifact_fingerprint,
        "planned_changes_summary": _game_adapter_stage2_planned_changes_summary(planned_changes),
        "governance_context_summary": _game_adapter_stage2_governance_summary(governance_context),
        "machine_view": payload["machine_view"],
        "human_view": payload["human_view"],
    }


def _unreal_project_structure_manifest_artifact_path(workspace_root: Path, artifact_name: str) -> Path | None:
    name_path = Path(artifact_name)
    if (
        name_path.name != artifact_name
        or not artifact_name.startswith("unreal-project-structure")
        or not artifact_name.endswith(".manifest.json")
    ):
        return None
    return workspace_root / ".dce" / "plans" / artifact_name


def _read_unreal_project_structure_manifest_file(path: Path, *, workspace_root: Path) -> dict[str, Any]:
    artifact_name = path.name
    artifact_path = _artifact_path_for_read_model(path, workspace_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )
    if not isinstance(payload, dict):
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )

    artifact_fingerprint = _string_or_none(payload.get("artifact_fingerprint"))
    if artifact_fingerprint is None:
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_missing",
            source_artifact_fingerprint=None,
        )
    if not verify_artifact_fingerprint(path):
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )
    try:
        validate_unreal_project_structure_manifest(payload)
    except ValueError:
        return _unreal_project_structure_manifest_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="contract_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )

    return {
        "read_model_type": "game_adapter_unreal_project_structure_manifest_read_model",
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "artifact_type": payload["artifact_type"],
        "contract_name": payload["contract_name"],
        "contract_version": payload["contract_version"],
        "read_model_contract_name": UNREAL_MANIFEST_READ_MODEL_CONTRACT_NAME,
        "read_model_contract_version": UNREAL_MANIFEST_READ_MODEL_CONTRACT_VERSION,
        "adapter": payload["adapter"],
        "domain": payload["domain"],
        "project_root_reference": payload["project_root_reference"],
        "structural_summary": payload["structural_summary"],
        "discovered_paths": payload["discovered_paths"],
        "artifact_fingerprint": artifact_fingerprint,
    }


def _unreal_symbol_candidate_index_artifact_path(workspace_root: Path, artifact_name: str) -> Path | None:
    name_path = Path(artifact_name)
    if (
        name_path.name != artifact_name
        or not artifact_name.startswith("unreal-symbol-candidates")
        or not artifact_name.endswith(".index.json")
    ):
        return None
    return workspace_root / ".dce" / "plans" / artifact_name


def _unreal_symbol_resolver_output_artifact_path(workspace_root: Path, artifact_name: str) -> Path | None:
    name_path = Path(artifact_name)
    if (
        name_path.name != artifact_name
        or not artifact_name.startswith("unreal-symbol-resolver")
        or not artifact_name.endswith(".resolution.json")
    ):
        return None
    return workspace_root / ".dce" / "plans" / artifact_name


def _read_unreal_symbol_candidate_index_file(path: Path, *, workspace_root: Path) -> dict[str, Any]:
    artifact_name = path.name
    artifact_path = _artifact_path_for_read_model(path, workspace_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )
    if not isinstance(payload, dict):
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )

    artifact_fingerprint = _string_or_none(payload.get("artifact_fingerprint"))
    if artifact_fingerprint is None:
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_missing",
            source_artifact_fingerprint=None,
        )
    if not verify_artifact_fingerprint(path):
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )
    try:
        validate_unreal_symbol_candidate_index(payload)
    except ValueError:
        return _unreal_symbol_candidate_index_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="contract_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )

    return {
        "read_model_type": "game_adapter_unreal_symbol_candidate_index_read_model",
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "artifact_type": payload["artifact_type"],
        "contract_name": payload["contract_name"],
        "contract_version": payload["contract_version"],
        "read_model_contract_name": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_NAME,
        "read_model_contract_version": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_VERSION,
        "adapter": payload["adapter"],
        "domain": payload["domain"],
        "source_manifest_fingerprint": payload["source_manifest_fingerprint"],
        "structural_summary": payload["structural_summary"],
        "candidates": payload["candidates"],
        "artifact_fingerprint": artifact_fingerprint,
    }


def _read_unreal_symbol_resolver_output_file(path: Path, *, workspace_root: Path) -> dict[str, Any]:
    artifact_name = path.name
    artifact_path = _artifact_path_for_read_model(path, workspace_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )
    if not isinstance(payload, dict):
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_malformed",
            source_artifact_fingerprint=None,
        )

    artifact_fingerprint = _string_or_none(payload.get("artifact_fingerprint"))
    if artifact_fingerprint is None:
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_missing",
            source_artifact_fingerprint=None,
        )
    if not verify_artifact_fingerprint(path):
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="artifact_fingerprint_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )
    try:
        validate_resolver_output_contract(payload)
    except ValueError:
        return _unreal_symbol_resolver_output_read_error(
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            reason_code="contract_invalid",
            source_artifact_fingerprint=artifact_fingerprint,
        )

    return {
        "read_model_type": "game_adapter_unreal_symbol_resolver_output_read_model",
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "artifact_type": payload["artifact_type"],
        "contract_name": payload["contract_name"],
        "contract_version": payload["contract_version"],
        "read_model_contract_name": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_NAME,
        "read_model_contract_version": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_VERSION,
        "adapter": payload["adapter"],
        "domain": payload["domain"],
        "source_input_fingerprint": payload["source_input_fingerprint"],
        "resolution_status": payload["resolution_status"],
        "resolved_symbols_summary": _unreal_symbol_resolution_summary(payload["resolved_symbols"]),
        "resolved_symbols": payload["resolved_symbols"],
        "unresolved_symbols_summary": _unreal_symbol_resolution_summary(payload["unresolved_symbols"]),
        "unresolved_symbols": payload["unresolved_symbols"],
        "integration_points": payload["integration_points"],
        "artifact_fingerprint": artifact_fingerprint,
    }


def _gce_stage0_artifact_core_shape_is_valid(payload: dict[str, Any]) -> bool:
    return (
        payload.get("artifact_type") == "stage0_input_package"
        and payload.get("contract_name") == STAGE0_CONTRACT_NAME
        and payload.get("contract_version") == STAGE0_CONTRACT_VERSION
        and payload.get("adapter") == "gce"
        and isinstance(payload.get("input_path"), str)
        and isinstance(payload.get("stage_1_release"), dict)
    )


def _normalized_session_intent_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    metadata = value.get("metadata")
    sections = value.get("sections")
    section_classifications = value.get("section_classifications")
    return {
        "contract_name": _string_or_none(value.get("contract_name")),
        "contract_version": _string_or_none(value.get("contract_version")),
        "source_input_path": _string_or_none(value.get("source_input_path")),
        "session_objective": _string_or_none(value.get("session_objective")),
        "metadata": _metadata_summary(metadata),
        "section_count": len(sections) if isinstance(sections, list) else 0,
        "section_ids": _section_ids(sections),
        "section_classifications": section_classifications if isinstance(section_classifications, dict) else {},
    }


def _clarification_request_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    questions = value.get("questions")
    return {
        "artifact_type": _string_or_none(value.get("artifact_type")),
        "contract_name": _string_or_none(value.get("contract_name")),
        "contract_version": _string_or_none(value.get("contract_version")),
        "source_input_path": _string_or_none(value.get("source_input_path")),
        "reason_code": _string_or_none(value.get("reason_code")),
        "stage_1_release_blocked": value.get("stage_1_release_blocked") is True,
        "question_count": len(questions) if isinstance(questions, list) else 0,
        "questions": _clarification_questions(questions),
        "clarification_request_fingerprint": compute_gce_clarification_request_fingerprint(value),
    }


def _metadata_summary(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    summary: dict[str, str] = {}
    for key in ("project_id", "project_name", "source_id"):
        field_value = value.get(key)
        if isinstance(field_value, str):
            summary[key] = field_value
    return summary


def _section_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    section_ids = []
    for section in value:
        if isinstance(section, dict) and isinstance(section.get("section_id"), str):
            section_ids.append(section["section_id"])
    return section_ids


def _clarification_questions(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    questions = []
    for question in value:
        if not isinstance(question, dict):
            continue
        questions.append(
            {
                "id": _string_or_none(question.get("id")),
                "field_path": _string_or_none(question.get("field_path")),
                "question": _string_or_none(question.get("question")),
                "blocking": question.get("blocking") is True,
            }
        )
    return questions


def _game_adapter_stage2_planned_changes_summary(planned_changes: Any) -> dict[str, Any]:
    if not isinstance(planned_changes, list):
        return {
            "change_count": 0,
            "operations": {},
            "domain_types": {},
            "strategies": {},
        }
    operations: dict[str, int] = {}
    domain_types: dict[str, int] = {}
    strategies: dict[str, int] = {}
    for change in planned_changes:
        if not isinstance(change, dict):
            continue
        _increment_count(operations, change.get("operation"))
        _increment_count(domain_types, change.get("domain_type"))
        _increment_count(strategies, change.get("strategy"))
    return {
        "change_count": len(planned_changes),
        "operations": operations,
        "domain_types": domain_types,
        "strategies": strategies,
    }


def _game_adapter_stage2_governance_summary(governance_context: Any) -> dict[str, Any] | None:
    if not isinstance(governance_context, dict):
        return None
    return {
        "policy_pack": _string_or_none(governance_context.get("policy_pack")),
        "guardrail_required": governance_context.get("guardrail_required") is True,
    }


def _unreal_symbol_resolution_summary(symbols: Any) -> dict[str, Any]:
    if not isinstance(symbols, list):
        return {
            "symbol_count": 0,
            "symbol_kinds": {},
            "confidence": {},
        }
    symbol_kinds: dict[str, int] = {}
    confidence: dict[str, int] = {}
    for symbol in symbols:
        if not isinstance(symbol, dict):
            continue
        _increment_count(symbol_kinds, symbol.get("symbol_kind"))
        _increment_count(confidence, symbol.get("confidence"))
    return {
        "symbol_count": len(symbols),
        "symbol_kinds": symbol_kinds,
        "confidence": confidence,
    }


def _increment_count(counts: dict[str, int], value: Any) -> None:
    if isinstance(value, str) and value:
        counts[value] = counts.get(value, 0) + 1


def _gce_stage0_read_error(
    *,
    artifact_name: str,
    artifact_path: str | None,
    input_path: str | None,
    reason_code: str,
    source_artifact_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "artifact_type": "gce_stage0_artifact_read_error",
        "adapter": "gce",
        "contract_name": GCE_STAGE0_READ_MODEL_CONTRACT_NAME,
        "contract_version": GCE_STAGE0_READ_MODEL_CONTRACT_VERSION,
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "input_path": input_path,
        "reason_code": reason_code,
        "artifact_fingerprint": source_artifact_fingerprint,
        "stage_1_release": {
            "blocked": True,
            "reason_code": reason_code,
        },
    }


def _game_adapter_stage2_preview_read_error(
    *,
    artifact_name: str,
    artifact_path: str | None,
    reason_code: str,
    source_artifact_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "read_model_type": "game_adapter_stage2_preview_read_error",
        "artifact_type": "game_adapter_stage2_preview_read_error",
        "adapter": GAME_ADAPTER_STAGE2_PREVIEW_ADAPTER,
        "domain": GAME_ADAPTER_STAGE2_PREVIEW_DOMAIN,
        "contract_name": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_NAME,
        "contract_version": GAME_ADAPTER_STAGE2_PREVIEW_READ_MODEL_CONTRACT_VERSION,
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "reason_code": reason_code,
        "artifact_fingerprint": source_artifact_fingerprint,
        "preview_artifact_type": GAME_ADAPTER_STAGE2_PREVIEW_ARTIFACT_TYPE,
        "preview_contract_name": GAME_ADAPTER_STAGE2_PREVIEW_CONTRACT_NAME,
        "preview_contract_version": GAME_ADAPTER_STAGE2_PREVIEW_CONTRACT_VERSION,
        "source_stage0_fingerprint": None,
        "source_input_reference": None,
        "planned_changes_summary": None,
        "governance_context_summary": None,
        "machine_view": None,
        "human_view": None,
    }


def _unreal_project_structure_manifest_read_error(
    *,
    artifact_name: str,
    artifact_path: str | None,
    reason_code: str,
    source_artifact_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "read_model_type": "game_adapter_unreal_project_structure_manifest_read_error",
        "artifact_type": "game_adapter_unreal_project_structure_manifest_read_error",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_MANIFEST_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_MANIFEST_READ_MODEL_CONTRACT_VERSION,
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "reason_code": reason_code,
        "artifact_fingerprint": source_artifact_fingerprint,
        "manifest_artifact_type": UNREAL_MANIFEST_ARTIFACT_TYPE,
        "manifest_contract_name": UNREAL_MANIFEST_CONTRACT_NAME,
        "manifest_contract_version": UNREAL_MANIFEST_CONTRACT_VERSION,
        "project_root_reference": None,
        "structural_summary": None,
        "discovered_paths": None,
    }


def _unreal_symbol_candidate_index_read_error(
    *,
    artifact_name: str,
    artifact_path: str | None,
    reason_code: str,
    source_artifact_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "read_model_type": "game_adapter_unreal_symbol_candidate_index_read_error",
        "artifact_type": "game_adapter_unreal_symbol_candidate_index_read_error",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_SYMBOL_CANDIDATE_INDEX_READ_MODEL_CONTRACT_VERSION,
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "reason_code": reason_code,
        "artifact_fingerprint": source_artifact_fingerprint,
        "candidate_index_artifact_type": UNREAL_SYMBOL_CANDIDATE_INDEX_ARTIFACT_TYPE,
        "candidate_index_contract_name": UNREAL_SYMBOL_CANDIDATE_INDEX_CONTRACT_NAME,
        "candidate_index_contract_version": UNREAL_SYMBOL_CANDIDATE_INDEX_CONTRACT_VERSION,
        "source_manifest_fingerprint": None,
        "structural_summary": None,
        "candidates": None,
    }


def _unreal_symbol_resolver_output_read_error(
    *,
    artifact_name: str,
    artifact_path: str | None,
    reason_code: str,
    source_artifact_fingerprint: str | None,
) -> dict[str, Any]:
    return {
        "read_model_type": "game_adapter_unreal_symbol_resolver_output_read_error",
        "artifact_type": "game_adapter_unreal_symbol_resolver_output_read_error",
        "adapter": UNREAL_MANIFEST_ADAPTER,
        "domain": UNREAL_MANIFEST_DOMAIN,
        "contract_name": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_NAME,
        "contract_version": UNREAL_SYMBOL_RESOLVER_OUTPUT_READ_MODEL_CONTRACT_VERSION,
        "artifact_name": artifact_name,
        "artifact_path": artifact_path,
        "reason_code": reason_code,
        "artifact_fingerprint": source_artifact_fingerprint,
        "resolver_output_artifact_type": UNREAL_SYMBOL_RESOLVER_OUTPUT_ARTIFACT_TYPE,
        "resolver_output_contract_name": UNREAL_SYMBOL_RESOLVER_OUTPUT_CONTRACT_NAME,
        "resolver_output_contract_version": UNREAL_SYMBOL_RESOLVER_OUTPUT_CONTRACT_VERSION,
        "source_input_fingerprint": None,
        "resolution_status": None,
        "resolved_symbols_summary": None,
        "resolved_symbols": None,
        "unresolved_symbols_summary": None,
        "unresolved_symbols": None,
        "integration_points": None,
    }


def _artifact_path_for_read_model(path: Path, workspace_root: Path) -> str:
    return path.resolve().relative_to(workspace_root.resolve()).as_posix()


def _stage0_reason_code(payload: dict[str, Any]) -> str | None:
    reason_code = _string_or_none(payload.get("reason_code"))
    if reason_code is not None:
        return reason_code
    stage_1_release = payload.get("stage_1_release")
    if isinstance(stage_1_release, dict):
        return _string_or_none(stage_1_release.get("reason_code"))
    return None


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
