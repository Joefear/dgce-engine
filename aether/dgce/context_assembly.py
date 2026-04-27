"""Stage 0 adapter-aware input assembly boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aether.dgce.decompose import (
    _write_json_with_artifact_fingerprint,
    compute_json_payload_fingerprint,
)
from aether.dgce.gce_ingestion import (
    INGESTION_CONTRACT_NAME,
    INGESTION_CONTRACT_VERSION,
    validate_gce_ingestion_input,
)


STAGE0_CONTRACT_NAME = "DGCEStage0InputPackage"
STAGE0_CONTRACT_VERSION = "dgce.stage0.input_package.v1"


@dataclass(frozen=True)
class Stage0InputBoundaryResult:
    """Result returned by the Stage 0 input assembly boundary."""

    ok: bool
    adapter: str
    stage_1_release_blocked: bool
    package: dict[str, Any]
    errors: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class Stage0PersistResult:
    """Result returned by Stage 0 artifact persistence."""

    boundary_result: Stage0InputBoundaryResult
    persisted: bool
    artifact_path: str | None = None
    artifact: dict[str, Any] | None = None


def assemble_stage0_input(raw_input: Any) -> Stage0InputBoundaryResult:
    """Assemble a Stage 0-ready package without changing software ingestion.

    GCE inputs opt into this boundary by declaring the GCE ingestion contract.
    Existing software-generation inputs are passed through for their existing
    DGCESection validation and lifecycle handling.
    """
    if _declares_gce_ingestion_contract(raw_input):
        return _assemble_gce_stage0_input(raw_input)
    if isinstance(raw_input, str):
        return _blocked_raw_natural_language_input()
    return _pass_through_software_input(raw_input)


def persist_stage0_input(workspace_path: str | Path, raw_input: Any) -> Stage0PersistResult:
    """Persist GCE Stage 0 packages while leaving software persistence unchanged."""
    boundary_result = assemble_stage0_input(raw_input)
    if boundary_result.adapter != "gce":
        return Stage0PersistResult(
            boundary_result=boundary_result,
            persisted=False,
            artifact_path=None,
            artifact=None,
        )

    project_root = _resolve_or_create_workspace_path(workspace_path)
    relative_path = _gce_stage0_artifact_relative_path(raw_input, boundary_result.package)
    artifact_path = project_root / relative_path
    artifact = _write_json_with_artifact_fingerprint(artifact_path, boundary_result.package)
    return Stage0PersistResult(
        boundary_result=Stage0InputBoundaryResult(
            ok=boundary_result.ok,
            adapter=boundary_result.adapter,
            stage_1_release_blocked=boundary_result.stage_1_release_blocked,
            package=artifact,
            errors=list(boundary_result.errors),
        ),
        persisted=True,
        artifact_path=relative_path.as_posix(),
        artifact=artifact,
    )


def _declares_gce_ingestion_contract(raw_input: Any) -> bool:
    return isinstance(raw_input, dict) and raw_input.get("contract_name") == INGESTION_CONTRACT_NAME


def _assemble_gce_stage0_input(raw_input: Any) -> Stage0InputBoundaryResult:
    validation = validate_gce_ingestion_input(raw_input)
    if validation.ok:
        input_path = str(validation.normalized_session_intent.get("source_input_path"))
        package = _stage0_package(
            adapter="gce",
            input_path=input_path,
            stage_1_release_blocked=False,
            normalized_session_intent=validation.normalized_session_intent,
            clarification_request=None,
            validation_report={
                "status": "PASS",
                "contract_version": INGESTION_CONTRACT_VERSION,
                "errors": [],
                "warnings": [],
                "stage_1_release_blocked": False,
            },
        )
        return Stage0InputBoundaryResult(
            ok=True,
            adapter="gce",
            stage_1_release_blocked=False,
            package=package,
            errors=[],
        )

    reason_code = "clarification_required" if validation.clarification_request is not None else "validation_failed"
    input_path = _declared_input_path(raw_input)
    package = _stage0_package(
        adapter="gce",
        input_path=input_path,
        stage_1_release_blocked=True,
        normalized_session_intent=None,
        clarification_request=validation.clarification_request,
        validation_report={
            "status": "FAIL",
            "contract_version": INGESTION_CONTRACT_VERSION,
            "errors": list(validation.errors),
            "warnings": [],
            "stage_1_release_blocked": True,
            "reason_code": reason_code,
        },
    )
    return Stage0InputBoundaryResult(
        ok=False,
        adapter="gce",
        stage_1_release_blocked=True,
        package=package,
        errors=list(validation.errors),
    )


def _blocked_raw_natural_language_input() -> Stage0InputBoundaryResult:
    errors = [
        {
            "field": "stage0_input",
            "condition": "raw_natural_language_input_not_supported",
            "severity": "HARD",
        }
    ]
    package = _stage0_package(
        adapter="unknown",
        input_path=None,
        stage_1_release_blocked=True,
        normalized_session_intent=None,
        clarification_request=None,
        validation_report={
            "status": "FAIL",
            "contract_version": None,
            "errors": errors,
            "warnings": [],
            "stage_1_release_blocked": True,
            "reason_code": "unsupported_input",
        },
    )
    return Stage0InputBoundaryResult(
        ok=False,
        adapter="unknown",
        stage_1_release_blocked=True,
        package=package,
        errors=errors,
    )


def _pass_through_software_input(raw_input: Any) -> Stage0InputBoundaryResult:
    package = _stage0_package(
        adapter="software",
        input_path=None,
        stage_1_release_blocked=False,
        normalized_session_intent=None,
        clarification_request=None,
        validation_report={
            "status": "PASS_THROUGH",
            "contract_version": None,
            "errors": [],
            "warnings": [],
            "stage_1_release_blocked": False,
        },
        source_input=raw_input,
    )
    return Stage0InputBoundaryResult(
        ok=True,
        adapter="software",
        stage_1_release_blocked=False,
        package=package,
        errors=[],
    )


def _stage0_package(
    *,
    adapter: str,
    input_path: str | None,
    stage_1_release_blocked: bool,
    normalized_session_intent: dict[str, Any] | None,
    clarification_request: dict[str, Any] | None,
    validation_report: dict[str, Any],
    source_input: Any = None,
) -> dict[str, Any]:
    package = {
        "contract_name": STAGE0_CONTRACT_NAME,
        "contract_version": STAGE0_CONTRACT_VERSION,
        "artifact_type": "stage0_input_package",
        "adapter": adapter,
        "input_path": input_path,
        "reason_code": validation_report.get("reason_code") if stage_1_release_blocked else None,
        "stage_1_release": {
            "blocked": stage_1_release_blocked,
            "reason_code": validation_report.get("reason_code") if stage_1_release_blocked else None,
        },
        "normalized_session_intent": normalized_session_intent,
        "clarification_request": clarification_request,
        "validation_report": validation_report,
    }
    if adapter == "software":
        package["source_input"] = source_input
    return package


def _declared_input_path(raw_input: Any) -> str | None:
    if not isinstance(raw_input, dict):
        return None
    input_path = raw_input.get("input_path")
    return input_path if isinstance(input_path, str) and input_path else None


def _gce_stage0_artifact_relative_path(raw_input: Any, package: dict[str, Any]) -> Path:
    input_path = _safe_path_token(str(package.get("input_path") or "unknown"))
    source_id = _declared_source_id(raw_input)
    if source_id is None:
        source_id = compute_json_payload_fingerprint(_hashable_raw_input(raw_input))[:16]
    return Path(".dce") / "input" / "gce" / f"{_safe_path_token(source_id)}.{input_path}.stage0.json"


def _declared_source_id(raw_input: Any) -> str | None:
    if not isinstance(raw_input, dict):
        return None
    metadata = raw_input.get("metadata")
    if not isinstance(metadata, dict):
        return None
    source_id = metadata.get("source_id")
    return source_id if isinstance(source_id, str) and source_id else None


def _hashable_raw_input(raw_input: Any) -> dict[str, Any]:
    return {
        "raw_input": raw_input
        if isinstance(raw_input, (dict, list, str, int, float, bool)) or raw_input is None
        else repr(raw_input)
    }


def _safe_path_token(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-") or "unknown"


def _resolve_or_create_workspace_path(workspace_path: str | Path) -> Path:
    raw_path = Path(workspace_path)
    base_root = Path.cwd().resolve()
    resolved_path = (base_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()

    if not raw_path.is_absolute():
        try:
            resolved_path.relative_to(base_root)
        except ValueError as exc:
            raise ValueError("workspace_path must remain within the current working directory") from exc

    resolved_path.mkdir(parents=True, exist_ok=True)
    return resolved_path


__all__ = [
    "STAGE0_CONTRACT_NAME",
    "STAGE0_CONTRACT_VERSION",
    "Stage0InputBoundaryResult",
    "Stage0PersistResult",
    "assemble_stage0_input",
    "persist_stage0_input",
]
