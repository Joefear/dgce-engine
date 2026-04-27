"""Stage 0 adapter-aware input assembly boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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


def assemble_stage0_input(raw_input: Any) -> Stage0InputBoundaryResult:
    """Assemble a Stage 0-ready package without changing software ingestion.

    GCE inputs opt into this boundary by declaring the GCE ingestion contract.
    Existing software-generation inputs are passed through for their existing
    DGCESection validation and lifecycle handling.
    """
    if _declares_gce_ingestion_contract(raw_input):
        return _assemble_gce_stage0_input(raw_input)
    return _pass_through_software_input(raw_input)


def _declares_gce_ingestion_contract(raw_input: Any) -> bool:
    return isinstance(raw_input, dict) and raw_input.get("contract_name") == INGESTION_CONTRACT_NAME


def _assemble_gce_stage0_input(raw_input: Any) -> Stage0InputBoundaryResult:
    validation = validate_gce_ingestion_input(raw_input)
    if validation.ok:
        package = _stage0_package(
            adapter="gce",
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
    package = _stage0_package(
        adapter="gce",
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


def _pass_through_software_input(raw_input: Any) -> Stage0InputBoundaryResult:
    package = _stage0_package(
        adapter="software",
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


__all__ = [
    "STAGE0_CONTRACT_NAME",
    "STAGE0_CONTRACT_VERSION",
    "Stage0InputBoundaryResult",
    "assemble_stage0_input",
]
