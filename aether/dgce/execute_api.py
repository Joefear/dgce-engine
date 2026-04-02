"""Governed DGCE section execution helpers for the HTTP transport layer."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from aether.dgce import run_dgce_section
from aether.dgce.decompose import (
    SectionAlignmentInput,
    _build_alignment_artifact,
    _write_json,
    compute_json_payload_fingerprint,
)
from aether.dgce.file_plan import FilePlan
from aether.dgce.incremental import (
    build_incremental_change_plan,
    build_write_transparency,
    load_owned_paths,
    scan_workspace_file_paths,
)
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.prepare_api import (
    _compute_prepared_plan_approval_lineage,
    _compute_prepared_plan_binding,
    _load_validated_json_artifact,
    _prepared_plan_relative_path as prepare_api_prepared_plan_relative_path,
    load_prepared_section_file_plan,
    load_prepared_section_plan_artifact,
    prepare_section_execution,
)


def _approval_payload(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = project_root / ".dce" / "approvals" / f"{section_id}.approval.json"
    if not approval_path.exists():
        return {}
    return json.loads(approval_path.read_text(encoding="utf-8"))


def _has_prior_execution_artifacts(project_root: Path, section_id: str) -> bool:
    return any(
        (
            project_root / ".dce" / directory / filename
        ).exists()
        for directory, filename in (
            ("execution", f"{section_id}.execution.json"),
            ("outputs", f"{section_id}.json"),
        )
    )


def _assert_rerun_is_safe(project_root: Path, section_id: str, file_plan: FilePlan) -> None:
    approval_payload = _approval_payload(project_root, section_id)
    selected_mode = str(approval_payload.get("selected_mode"))
    change_plan = build_incremental_change_plan(
        section_id,
        file_plan,
        scan_workspace_file_paths(project_root),
        project_root=project_root,
    )["changes"]
    _, write_transparency = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=selected_mode == "safe_modify",
        owned_paths=load_owned_paths(project_root / ".dce" / "ownership_index.json"),
    )
    write_summary = dict(write_transparency.get("write_summary", {}))
    if int(write_summary.get("skipped_ownership_count", 0)) > 0:
        raise ValueError(f"Section rerun failed ownership validation: {section_id}")
    if int(write_summary.get("skipped_modify_count", 0)) > 0:
        raise ValueError(f"Section rerun requires safe_modify approval: {section_id}")

    alignment_artifact = _build_alignment_artifact(
        project_root / ".dce",
        section_id,
        require_preflight_pass=True,
        alignment_input=SectionAlignmentInput(),
        write_transparency=write_transparency,
    )
    if alignment_artifact.get("alignment_blocked") is True:
        raise ValueError(f"Section rerun failed safe modify validation: {section_id}")


def _assert_prepared_plan_binding_matches(project_root: Path, section_id: str) -> None:
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    current_binding = _compute_prepared_plan_binding(project_root, section_id)
    if prepared_plan.get("binding") != current_binding:
        raise ValueError(f"Prepared file plan binding mismatch: {section_id}")


def _assert_prepared_plan_approval_lineage_matches(project_root: Path, section_id: str) -> None:
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    current_lineage = _compute_prepared_plan_approval_lineage(project_root, section_id)
    if prepared_plan.get("approval_lineage") != current_lineage:
        raise ValueError(f"Prepared file plan approval lineage mismatch: {section_id}")


def _prepared_plan_relative_path(section_id: str) -> str:
    return f".dce/plans/{section_id}.prepared_plan.json"


def _execution_artifact_path(project_root: Path, section_id: str) -> Path:
    return project_root / ".dce" / "execution" / f"{section_id}.execution.json"


def _execution_artifact_relative_path(section_id: str) -> str:
    return f".dce/execution/{section_id}.execution.json"


def _approval_artifact_relative_path(section_id: str) -> str:
    return f".dce/approvals/{section_id}.approval.json"


def load_section_approval_artifact(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = _approval_artifact_relative_path(section_id)
    approval_payload = _load_validated_json_artifact(project_root, approval_path)
    approval_artifact_fingerprint = approval_payload.get("artifact_fingerprint")
    if not isinstance(approval_artifact_fingerprint, str):
        raise ValueError(f"Approval artifact is malformed: {section_id}")
    return approval_payload


def load_section_execution_artifact(project_root: Path, section_id: str) -> dict[str, Any]:
    execution_path = _execution_artifact_relative_path(section_id)
    execution_payload = _load_validated_json_artifact(project_root, execution_path)
    execution_status = execution_payload.get("execution_status")
    if not isinstance(execution_status, str):
        raise ValueError(f"Execution artifact is malformed: {section_id}")
    written_files = execution_payload.get("written_files")
    if not isinstance(written_files, list):
        raise ValueError(f"Execution artifact is malformed: {section_id}")
    prepared_plan_audit_fingerprint = execution_payload.get("prepared_plan_audit_fingerprint")
    if prepared_plan_audit_fingerprint is not None and not isinstance(prepared_plan_audit_fingerprint, str):
        raise ValueError(f"Execution artifact is malformed: {section_id}")
    prepared_plan_cross_link_fingerprint = execution_payload.get("prepared_plan_cross_link_fingerprint")
    if prepared_plan_cross_link_fingerprint is not None and not isinstance(prepared_plan_cross_link_fingerprint, str):
        raise ValueError(f"Execution artifact is malformed: {section_id}")
    return execution_payload


def _bundle_input_fingerprint(section_ids: list[str]) -> str:
    ordered_payload = {"section_ids": section_ids}
    ordered_bytes = (json.dumps(ordered_payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return hashlib.sha256(ordered_bytes).hexdigest()


def _bundle_manifest_core_payload(
    *,
    section_ids: list[str],
    execution_status: str,
    stopped_early: bool,
    first_failing_section: str | None,
    sections: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "artifact_type": "bundle_execution_audit_manifest",
        "bundle_input_fingerprint": _bundle_input_fingerprint(section_ids),
        "execution_status": execution_status,
        "first_failing_section": first_failing_section,
        "generated_by": "DGCE",
        "schema_version": "1.0",
        "section_ids": section_ids,
        "sections": sections,
        "stopped_early": stopped_early,
    }


def _bundle_manifest_path(project_root: Path, bundle_fingerprint: str) -> Path:
    return project_root / ".dce" / "execution" / "bundles" / f"{bundle_fingerprint}.json"


def _bundle_index_path(project_root: Path) -> Path:
    return project_root / ".dce" / "execution" / "bundles" / "index.json"


def _validate_bundle_manifest_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Bundle audit manifest must contain a JSON object")
    for key, expected in (
        ("artifact_type", "bundle_execution_audit_manifest"),
        ("generated_by", "DGCE"),
        ("schema_version", "1.0"),
    ):
        if payload.get(key) != expected:
            raise ValueError("Bundle audit manifest is malformed")
    if any(not isinstance(payload.get(key), str) for key in ("bundle_fingerprint", "bundle_input_fingerprint", "execution_status")):
        raise ValueError("Bundle audit manifest is malformed")
    if not isinstance(payload.get("section_ids"), list) or any(not isinstance(item, str) for item in payload["section_ids"]):
        raise ValueError("Bundle audit manifest is malformed")
    if not isinstance(payload.get("stopped_early"), bool):
        raise ValueError("Bundle audit manifest is malformed")
    first_failing_section = payload.get("first_failing_section")
    if first_failing_section is not None and not isinstance(first_failing_section, str):
        raise ValueError("Bundle audit manifest is malformed")
    sections = payload.get("sections")
    if not isinstance(sections, list):
        raise ValueError("Bundle audit manifest is malformed")
    for entry in sections:
        if not isinstance(entry, dict):
            raise ValueError("Bundle audit manifest is malformed")
        if any(not isinstance(entry.get(key), str) for key in ("execution_artifact_path", "section_id", "status")):
            raise ValueError("Bundle audit manifest is malformed")
        for optional_key in (
            "approval_lineage_fingerprint",
            "binding_fingerprint",
            "prepared_plan_audit_fingerprint",
            "prepared_plan_fingerprint",
        ):
            optional_value = entry.get(optional_key)
            if optional_value is not None and not isinstance(optional_value, str):
                raise ValueError("Bundle audit manifest is malformed")
    core_payload = {
        key: value
        for key, value in payload.items()
        if key != "bundle_fingerprint"
    }
    if payload["bundle_fingerprint"] != compute_json_payload_fingerprint(core_payload):
        raise ValueError("Bundle audit manifest is malformed")
    return payload


def _validate_bundle_index_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Bundle index must contain a JSON object")
    for key, expected in (
        ("artifact_type", "bundle_execution_audit_index"),
        ("generated_by", "DGCE"),
        ("schema_version", "1.0"),
    ):
        if payload.get(key) != expected:
            raise ValueError("Bundle index is malformed")
    bundles = payload.get("bundles")
    if not isinstance(bundles, list):
        raise ValueError("Bundle index is malformed")
    by_section = payload.get("by_section")
    if not isinstance(by_section, dict):
        raise ValueError("Bundle index is malformed")
    for entry in bundles:
        if not isinstance(entry, dict):
            raise ValueError("Bundle index is malformed")
        if any(not isinstance(entry.get(key), str) for key in ("bundle_fingerprint", "bundle_input_fingerprint", "execution_status", "manifest_path")):
            raise ValueError("Bundle index is malformed")
        if not isinstance(entry.get("section_ids"), list) or any(not isinstance(item, str) for item in entry["section_ids"]):
            raise ValueError("Bundle index is malformed")
        if not isinstance(entry.get("stopped_early"), bool):
            raise ValueError("Bundle index is malformed")
        first_failing_section = entry.get("first_failing_section")
        if first_failing_section is not None and not isinstance(first_failing_section, str):
            raise ValueError("Bundle index is malformed")
    for section_id, bundle_fingerprints in by_section.items():
        if not isinstance(section_id, str):
            raise ValueError("Bundle index is malformed")
        if not isinstance(bundle_fingerprints, list) or any(not isinstance(item, str) for item in bundle_fingerprints):
            raise ValueError("Bundle index is malformed")
    return payload


def load_bundle_execution_index(project_root: Path) -> dict[str, Any]:
    index_path = _bundle_index_path(project_root)
    if not index_path.exists():
        return {
            "artifact_type": "bundle_execution_audit_index",
            "bundles": [],
            "by_section": {},
            "generated_by": "DGCE",
            "schema_version": "1.0",
        }
    return _validate_bundle_index_payload(json.loads(index_path.read_text(encoding="utf-8")))


def load_bundle_execution_manifest(project_root: Path, bundle_fingerprint: str) -> dict[str, Any]:
    manifest_path = _bundle_manifest_path(project_root, bundle_fingerprint)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Bundle not found: {bundle_fingerprint}")
    manifest = _validate_bundle_manifest_payload(json.loads(manifest_path.read_text(encoding="utf-8")))
    if manifest.get("bundle_fingerprint") != bundle_fingerprint:
        raise ValueError("Bundle audit manifest is malformed")
    return manifest


def get_bundle_index_record_by_fingerprint(project_root: Path, bundle_fingerprint: str) -> dict[str, Any] | None:
    for record in load_bundle_execution_index(project_root)["bundles"]:
        if record["bundle_fingerprint"] == bundle_fingerprint:
            return record
    return None


def get_bundle_index_records_by_input_fingerprint(project_root: Path, bundle_input_fingerprint: str) -> list[dict[str, Any]]:
    return [
        record
        for record in load_bundle_execution_index(project_root)["bundles"]
        if record["bundle_input_fingerprint"] == bundle_input_fingerprint
    ]


def get_bundle_fingerprints_for_section(project_root: Path, section_id: str) -> list[str]:
    return list(load_bundle_execution_index(project_root)["by_section"].get(section_id, []))


def get_bundle_index_records_for_section(project_root: Path, section_id: str) -> list[dict[str, Any]]:
    bundle_fingerprints = get_bundle_fingerprints_for_section(project_root, section_id)
    records_by_fingerprint = {
        record["bundle_fingerprint"]: record
        for record in load_bundle_execution_index(project_root)["bundles"]
    }
    return [
        records_by_fingerprint[bundle_fingerprint]
        for bundle_fingerprint in bundle_fingerprints
        if bundle_fingerprint in records_by_fingerprint
    ]


def get_section_provenance(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = project_root / _approval_artifact_relative_path(section_id)
    prepared_plan_path = project_root / prepare_api_prepared_plan_relative_path(section_id)
    execution_path = _execution_artifact_path(project_root, section_id)

    approval: dict[str, Any] | None = None
    approval_lineage: dict[str, Any] | None = None
    prepared_plan: dict[str, Any] | None = None
    execution: dict[str, Any] | None = None

    if approval_path.exists():
        approval = load_section_approval_artifact(project_root, section_id)
        approval_lineage = _compute_prepared_plan_approval_lineage(project_root, section_id)
    if prepared_plan_path.exists():
        prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    if execution_path.exists():
        execution = load_section_execution_artifact(project_root, section_id)

    bundle_references = get_bundle_index_records_for_section(project_root, section_id)
    if approval is None and prepared_plan is None and execution is None and not bundle_references:
        raise FileNotFoundError(f"Section provenance not found: {section_id}")

    prepared_plan_audit_manifest = execution.get("prepared_plan_audit_manifest") if execution is not None else None
    return {
        "section_id": section_id,
        "approval": {
            "approval_artifact_fingerprint": approval_lineage["approval_artifact_fingerprint"] if approval_lineage is not None else None,
            "approval_path": _approval_artifact_relative_path(section_id) if approval is not None else None,
            "approval_record_fingerprint": approval_lineage["approval_record_fingerprint"] if approval_lineage is not None else None,
            "approval_status": approval.get("approval_status") if approval is not None else None,
            "execution_permitted": approval.get("execution_permitted") if approval is not None else None,
            "selected_mode": approval.get("selected_mode") if approval is not None else None,
        },
        "prepared_plan": {
            "approval_lineage_fingerprint": prepared_plan.get("approval_lineage_fingerprint") if prepared_plan is not None else None,
            "binding_fingerprint": prepared_plan.get("binding_fingerprint") if prepared_plan is not None else None,
            "prepared_plan_fingerprint": compute_json_payload_fingerprint(prepared_plan) if prepared_plan is not None else None,
            "prepared_plan_path": _prepared_plan_relative_path(section_id) if prepared_plan is not None else None,
        },
        "execution": {
            "execution_artifact_path": _execution_artifact_relative_path(section_id) if execution is not None else None,
            "execution_status": execution.get("execution_status") if execution is not None else None,
            "prepared_plan_audit_fingerprint": execution.get("prepared_plan_audit_fingerprint") if execution is not None else None,
            "prepared_plan_cross_link_fingerprint": execution.get("prepared_plan_cross_link_fingerprint") if execution is not None else None,
            "written_files": prepared_plan_audit_manifest.get("written_files") if isinstance(prepared_plan_audit_manifest, dict) else None,
        },
        "bundle_references": bundle_references,
    }


def _verification_check(check_id: str, artifact: str, passed: bool, reason: str) -> dict[str, str]:
    return {
        "check_id": check_id,
        "status": "pass" if passed else "fail",
        "artifact": artifact,
        "reason": reason,
    }


def _verification_report(subject_type: str, subject_id: str, checks: list[dict[str, str]]) -> dict[str, Any]:
    failure_count = sum(1 for check in checks if check["status"] == "fail")
    return {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "verified": failure_count == 0,
        "checks": checks,
        "failure_count": failure_count,
    }


def verify_section_artifact_chain(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = project_root / _approval_artifact_relative_path(section_id)
    prepared_plan_path = project_root / prepare_api_prepared_plan_relative_path(section_id)
    execution_path = _execution_artifact_path(project_root, section_id)
    checks: list[dict[str, str]] = []

    bundle_references = get_bundle_index_records_for_section(project_root, section_id)
    grounded = approval_path.exists() or prepared_plan_path.exists() or execution_path.exists() or bool(bundle_references)
    if not grounded:
        raise FileNotFoundError(f"Section not found: {section_id}")

    approval_payload: dict[str, Any] | None = None
    prepared_plan_payload: dict[str, Any] | None = None
    execution_payload: dict[str, Any] | None = None

    if approval_path.exists():
        try:
            approval_payload = load_section_approval_artifact(project_root, section_id)
            approval_lineage = _compute_prepared_plan_approval_lineage(project_root, section_id)
            checks.append(_verification_check("approval.exists", "approval", True, "Approval artifact exists"))
            checks.append(_verification_check("approval.valid", "approval", True, "Approval artifact is valid"))
            checks.append(_verification_check("approval.identity", "approval", True, "Approval identity fields are present"))
        except Exception as exc:
            checks.append(_verification_check("approval.exists", "approval", True, "Approval artifact exists"))
            checks.append(_verification_check("approval.valid", "approval", False, str(exc)))
            checks.append(_verification_check("approval.identity", "approval", False, str(exc)))
            approval_lineage = None
    else:
        checks.append(_verification_check("approval.exists", "approval", False, "Approval artifact missing"))
        approval_lineage = None

    if prepared_plan_path.exists():
        try:
            prepared_plan_payload = load_prepared_section_plan_artifact(project_root, section_id)
            prepared_plan_fingerprint = compute_json_payload_fingerprint(prepared_plan_payload)
            checks.append(_verification_check("prepared_plan.exists", "prepared_plan", True, "Prepared plan artifact exists"))
            checks.append(_verification_check("prepared_plan.valid", "prepared_plan", True, "Prepared plan artifact is valid"))
            checks.append(_verification_check("prepared_plan.binding", "prepared_plan", True, "Prepared plan binding is valid"))
            checks.append(_verification_check("prepared_plan.lineage", "prepared_plan", True, "Prepared plan approval lineage is valid"))
            checks.append(_verification_check("prepared_plan.fingerprint", "prepared_plan", True, prepared_plan_fingerprint))
        except Exception as exc:
            checks.append(_verification_check("prepared_plan.exists", "prepared_plan", True, "Prepared plan artifact exists"))
            checks.append(_verification_check("prepared_plan.valid", "prepared_plan", False, str(exc)))
            checks.append(_verification_check("prepared_plan.binding", "prepared_plan", False, str(exc)))
            checks.append(_verification_check("prepared_plan.lineage", "prepared_plan", False, str(exc)))
            checks.append(_verification_check("prepared_plan.fingerprint", "prepared_plan", False, str(exc)))
            prepared_plan_fingerprint = None
    else:
        prepared_plan_fingerprint = None
        if execution_path.exists():
            checks.append(_verification_check("prepared_plan.exists", "prepared_plan", False, "Prepared plan artifact missing"))
        else:
            checks.append(_verification_check("prepared_plan.exists", "prepared_plan", True, "Prepared plan artifact not present"))

    if execution_path.exists():
        try:
            execution_payload = load_section_execution_artifact(project_root, section_id)
            checks.append(_verification_check("execution.exists", "execution", True, "Execution artifact exists"))
            checks.append(_verification_check("execution.valid", "execution", True, "Execution artifact is valid"))
            audit_manifest = execution_payload.get("prepared_plan_audit_manifest")
            audit_fingerprint = execution_payload.get("prepared_plan_audit_fingerprint")
            cross_link = execution_payload.get("prepared_plan_cross_link")
            cross_link_fingerprint = execution_payload.get("prepared_plan_cross_link_fingerprint")
            audit_ok = (
                isinstance(audit_manifest, dict)
                and isinstance(audit_fingerprint, str)
                and compute_json_payload_fingerprint(audit_manifest) == audit_fingerprint
            )
            checks.append(
                _verification_check(
                    "execution.audit",
                    "execution",
                    audit_ok,
                    "Prepared-plan audit manifest is consistent" if audit_ok else "Prepared-plan audit manifest fingerprint mismatch",
                )
            )
            cross_link_ok = (
                isinstance(cross_link, dict)
                and isinstance(cross_link_fingerprint, str)
                and compute_json_payload_fingerprint(cross_link) == cross_link_fingerprint
            )
            checks.append(
                _verification_check(
                    "execution.cross_link",
                    "execution",
                    cross_link_ok,
                    "Prepared-plan cross-link is consistent" if cross_link_ok else "Prepared-plan cross-link fingerprint mismatch",
                )
            )
            prepared_identity_ok = (
                prepared_plan_payload is not None
                and audit_ok
                and cross_link_ok
                and cross_link.get("prepared_plan_fingerprint") == prepared_plan_fingerprint
                and audit_manifest.get("prepared_plan_fingerprint") == prepared_plan_fingerprint
                and audit_manifest.get("binding_fingerprint") == prepared_plan_payload.get("binding_fingerprint")
                and audit_manifest.get("approval_lineage_fingerprint") == prepared_plan_payload.get("approval_lineage_fingerprint")
            )
            checks.append(
                _verification_check(
                    "execution.prepared_plan_identity",
                    "execution",
                    prepared_identity_ok,
                    "Execution artifact matches sealed prepared plan"
                    if prepared_identity_ok
                    else "Execution artifact does not match sealed prepared plan identity",
                )
            )
        except Exception as exc:
            checks.append(_verification_check("execution.exists", "execution", True, "Execution artifact exists"))
            checks.append(_verification_check("execution.valid", "execution", False, str(exc)))
            checks.append(_verification_check("execution.audit", "execution", False, str(exc)))
            checks.append(_verification_check("execution.cross_link", "execution", False, str(exc)))
            checks.append(_verification_check("execution.prepared_plan_identity", "execution", False, str(exc)))
    else:
        checks.append(_verification_check("execution.exists", "execution", True, "Execution artifact not present"))

    return _verification_report("section", section_id, checks)


def verify_bundle_artifact_chain(project_root: Path, bundle_fingerprint: str) -> dict[str, Any]:
    manifest_path = _bundle_manifest_path(project_root, bundle_fingerprint)
    checks: list[dict[str, str]] = []
    if not manifest_path.exists():
        raise FileNotFoundError(f"Bundle not found: {bundle_fingerprint}")

    manifest_payload: dict[str, Any] | None = None
    try:
        manifest_payload = load_bundle_execution_manifest(project_root, bundle_fingerprint)
        input_ok = manifest_payload["bundle_input_fingerprint"] == _bundle_input_fingerprint(list(manifest_payload["section_ids"]))
        checks.append(_verification_check("bundle_manifest.exists", "bundle_manifest", True, "Bundle manifest exists"))
        checks.append(_verification_check("bundle_manifest.valid", "bundle_manifest", True, "Bundle manifest is valid"))
        checks.append(
            _verification_check(
                "bundle_manifest.input",
                "bundle_manifest",
                input_ok,
                "Bundle input fingerprint matches ordered section_ids" if input_ok else "Bundle input fingerprint mismatch",
            )
        )
    except Exception as exc:
        checks.append(_verification_check("bundle_manifest.exists", "bundle_manifest", True, "Bundle manifest exists"))
        checks.append(_verification_check("bundle_manifest.valid", "bundle_manifest", False, str(exc)))
        checks.append(_verification_check("bundle_manifest.input", "bundle_manifest", False, str(exc)))

    try:
        index_record = get_bundle_index_record_by_fingerprint(project_root, bundle_fingerprint)
        index_loaded = True
    except Exception as exc:
        index_record = None
        index_loaded = False
        index_error = str(exc)
    if not index_loaded:
        checks.append(_verification_check("bundle_index.valid", "bundle_index", False, index_error))
        checks.append(_verification_check("bundle_index.record", "bundle_index", False, index_error))
    else:
        checks.append(_verification_check("bundle_index.valid", "bundle_index", True, "Bundle index is valid"))
        record_exists = index_record is not None
        checks.append(
            _verification_check(
                "bundle_index.record",
                "bundle_index",
                record_exists,
                "Bundle index entry exists" if record_exists else "Bundle index entry missing",
            )
        )
        if record_exists and manifest_payload is not None:
            record_matches = all(
                index_record.get(key) == expected
                for key, expected in (
                    ("bundle_input_fingerprint", manifest_payload["bundle_input_fingerprint"]),
                    ("manifest_path", _bundle_manifest_path(project_root, bundle_fingerprint).relative_to(project_root).as_posix()),
                    ("section_ids", manifest_payload["section_ids"]),
                    ("execution_status", manifest_payload["execution_status"]),
                    ("stopped_early", manifest_payload["stopped_early"]),
                    ("first_failing_section", manifest_payload["first_failing_section"]),
                )
            )
            checks.append(
                _verification_check(
                    "bundle_index.match",
                    "bundle_index",
                    record_matches,
                    "Bundle index entry matches manifest" if record_matches else "Bundle index entry does not match manifest",
                )
            )

    if manifest_payload is not None:
        sections = list(manifest_payload["sections"])
        fail_fast_ok = True
        if manifest_payload["execution_status"] == "failed" and manifest_payload["stopped_early"] is True:
            first_failing_section = manifest_payload.get("first_failing_section")
            if first_failing_section not in manifest_payload["section_ids"]:
                fail_fast_ok = False
            else:
                expected_prefix = manifest_payload["section_ids"][: manifest_payload["section_ids"].index(first_failing_section) + 1]
                fail_fast_ok = [entry["section_id"] for entry in sections] == expected_prefix
        checks.append(
            _verification_check(
                "bundle_manifest.fail_fast",
                "bundle_manifest",
                fail_fast_ok,
                "Bundle fail-fast semantics are coherent" if fail_fast_ok else "Bundle fail-fast semantics are incoherent",
            )
        )
        for section_record in sections:
            section_id = section_record["section_id"]
            artifact_label = f"section:{section_id}"
            execution_artifact_path = project_root / section_record["execution_artifact_path"]
            if not execution_artifact_path.exists():
                checks.append(_verification_check(f"bundle_section.{section_id}.execution", artifact_label, False, "Execution artifact missing"))
                continue
            try:
                execution_payload = load_section_execution_artifact(project_root, section_id)
                audit_manifest = execution_payload["prepared_plan_audit_manifest"]
                prepared_plan_payload = load_prepared_section_plan_artifact(project_root, section_id)
                prepared_plan_fingerprint = compute_json_payload_fingerprint(prepared_plan_payload)
                prepared_plan_match = section_record.get("prepared_plan_fingerprint") == prepared_plan_fingerprint == audit_manifest.get("prepared_plan_fingerprint")
                audit_match = section_record.get("prepared_plan_audit_fingerprint") == execution_payload.get("prepared_plan_audit_fingerprint")
                binding_match = section_record.get("binding_fingerprint") == audit_manifest.get("binding_fingerprint") == prepared_plan_payload.get("binding_fingerprint")
                lineage_match = section_record.get("approval_lineage_fingerprint") == audit_manifest.get("approval_lineage_fingerprint") == prepared_plan_payload.get("approval_lineage_fingerprint")
                checks.append(_verification_check(f"bundle_section.{section_id}.prepared_plan", artifact_label, prepared_plan_match, "Prepared plan fingerprint matches section linkage" if prepared_plan_match else "Prepared plan fingerprint mismatch"))
                checks.append(_verification_check(f"bundle_section.{section_id}.audit", artifact_label, audit_match, "Prepared-plan audit fingerprint matches section linkage" if audit_match else "Prepared-plan audit fingerprint mismatch"))
                checks.append(_verification_check(f"bundle_section.{section_id}.binding", artifact_label, binding_match, "Binding fingerprint matches section linkage" if binding_match else "Binding fingerprint mismatch"))
                checks.append(_verification_check(f"bundle_section.{section_id}.lineage", artifact_label, lineage_match, "Approval lineage fingerprint matches section linkage" if lineage_match else "Approval lineage fingerprint mismatch"))
            except Exception as exc:
                checks.append(_verification_check(f"bundle_section.{section_id}.prepared_plan", artifact_label, False, str(exc)))
                checks.append(_verification_check(f"bundle_section.{section_id}.audit", artifact_label, False, str(exc)))
                checks.append(_verification_check(f"bundle_section.{section_id}.binding", artifact_label, False, str(exc)))
                checks.append(_verification_check(f"bundle_section.{section_id}.lineage", artifact_label, False, str(exc)))

    return _verification_report("bundle", bundle_fingerprint, checks)


def _bundle_section_record(
    *,
    project_root: Path,
    section_id: str,
    status: str,
) -> dict[str, Any]:
    execution_artifact_path = _execution_artifact_relative_path(section_id)
    if status != "success":
        return {
            "approval_lineage_fingerprint": None,
            "binding_fingerprint": None,
            "execution_artifact_path": execution_artifact_path,
            "prepared_plan_audit_fingerprint": None,
            "prepared_plan_fingerprint": None,
            "section_id": section_id,
            "status": status,
        }

    execution_artifact = json.loads(_execution_artifact_path(project_root, section_id).read_text(encoding="utf-8"))
    if not isinstance(execution_artifact, dict):
        raise ValueError(f"Bundle audit manifest requires valid execution artifact: {section_id}")
    prepared_plan_audit_manifest = execution_artifact.get("prepared_plan_audit_manifest")
    if not isinstance(prepared_plan_audit_manifest, dict):
        raise ValueError(f"Bundle audit manifest requires valid prepared-plan audit manifest: {section_id}")
    return {
        "approval_lineage_fingerprint": prepared_plan_audit_manifest.get("approval_lineage_fingerprint"),
        "binding_fingerprint": prepared_plan_audit_manifest.get("binding_fingerprint"),
        "execution_artifact_path": execution_artifact_path,
        "prepared_plan_audit_fingerprint": execution_artifact.get("prepared_plan_audit_fingerprint"),
        "prepared_plan_fingerprint": prepared_plan_audit_manifest.get("prepared_plan_fingerprint"),
        "section_id": section_id,
        "status": status,
    }


def _persist_bundle_execution_audit_manifest(
    *,
    project_root: Path,
    section_ids: list[str],
    section_records: list[dict[str, Any]],
    execution_status: str,
    stopped_early: bool,
    first_failing_section: str | None,
) -> None:
    core_payload = _bundle_manifest_core_payload(
        section_ids=section_ids,
        execution_status=execution_status,
        stopped_early=stopped_early,
        first_failing_section=first_failing_section,
        sections=section_records,
    )
    bundle_fingerprint = compute_json_payload_fingerprint(core_payload)
    manifest_payload = {
        **core_payload,
        "bundle_fingerprint": bundle_fingerprint,
    }
    manifest_path = _bundle_manifest_path(project_root, bundle_fingerprint)
    _write_json(manifest_path, manifest_payload)
    _update_bundle_execution_index(project_root, manifest_payload)


def _update_bundle_execution_index(project_root: Path, manifest_payload: dict[str, Any]) -> None:
    index_payload = load_bundle_execution_index(project_root)
    record = {
        "bundle_fingerprint": manifest_payload["bundle_fingerprint"],
        "bundle_input_fingerprint": manifest_payload["bundle_input_fingerprint"],
        "execution_status": manifest_payload["execution_status"],
        "first_failing_section": manifest_payload["first_failing_section"],
        "manifest_path": _bundle_manifest_path(project_root, manifest_payload["bundle_fingerprint"]).relative_to(project_root).as_posix(),
        "section_ids": list(manifest_payload["section_ids"]),
        "stopped_early": manifest_payload["stopped_early"],
    }
    bundles_by_fingerprint = {
        entry["bundle_fingerprint"]: entry
        for entry in index_payload["bundles"]
    }
    bundles_by_fingerprint[record["bundle_fingerprint"]] = record
    bundles = sorted(
        bundles_by_fingerprint.values(),
        key=lambda entry: (
            entry["bundle_input_fingerprint"],
            entry["bundle_fingerprint"],
        ),
    )
    by_section: dict[str, list[str]] = {}
    for entry in bundles:
        for section_id in entry["section_ids"]:
            by_section.setdefault(section_id, [])
            if entry["bundle_fingerprint"] not in by_section[section_id]:
                by_section[section_id].append(entry["bundle_fingerprint"])
    index_artifact = {
        "artifact_type": "bundle_execution_audit_index",
        "bundles": bundles,
        "by_section": {section_id: sorted(bundle_fingerprints) for section_id, bundle_fingerprints in sorted(by_section.items())},
        "generated_by": "DGCE",
        "schema_version": "1.0",
    }
    _write_json(_bundle_index_path(project_root), index_artifact)


def _build_prepared_plan_audit_manifest(
    *,
    section_id: str,
    prepared_plan: dict[str, Any],
    execution_artifact: dict[str, Any],
) -> dict[str, Any]:
    written_files = execution_artifact.get("written_files")
    if not isinstance(written_files, list):
        raise ValueError(f"Execution audit manifest requires valid written_files: {section_id}")
    selected_mode = execution_artifact.get("selected_mode")
    if selected_mode is not None and not isinstance(selected_mode, str):
        raise ValueError(f"Execution audit manifest requires valid selected_mode: {section_id}")
    execution_status = execution_artifact.get("execution_status")
    if not isinstance(execution_status, str):
        raise ValueError(f"Execution audit manifest requires valid execution_status: {section_id}")
    return {
        "approval_lineage_fingerprint": prepared_plan["approval_lineage_fingerprint"],
        "binding_fingerprint": prepared_plan["binding_fingerprint"],
        "execution_permitted": bool(prepared_plan["binding"]["execution_permitted"]),
        "execution_status": execution_status,
        "prepared_plan_fingerprint": compute_json_payload_fingerprint(prepared_plan),
        "prepared_plan_path": _prepared_plan_relative_path(section_id),
        "section_id": section_id,
        "selected_mode": selected_mode,
        "written_files": written_files,
    }


def _build_prepared_plan_cross_link(
    *,
    section_id: str,
    prepared_plan: dict[str, Any],
    prepared_plan_audit_fingerprint: str,
) -> dict[str, Any]:
    return {
        "prepared_plan_audit_fingerprint": prepared_plan_audit_fingerprint,
        "prepared_plan_fingerprint": compute_json_payload_fingerprint(prepared_plan),
        "prepared_plan_path": _prepared_plan_relative_path(section_id),
        "section_id": section_id,
    }


def _persist_prepared_plan_audit_manifest(
    project_root: Path,
    section_id: str,
    prepared_plan: dict[str, Any],
) -> None:
    execution_path = _execution_artifact_path(project_root, section_id)
    execution_artifact = json.loads(execution_path.read_text(encoding="utf-8"))
    if not isinstance(execution_artifact, dict):
        raise ValueError(f"Execution audit manifest requires valid execution artifact: {section_id}")
    audit_manifest = _build_prepared_plan_audit_manifest(
        section_id=section_id,
        prepared_plan=prepared_plan,
        execution_artifact=execution_artifact,
    )
    execution_artifact["prepared_plan_audit_manifest"] = audit_manifest
    execution_artifact["prepared_plan_audit_fingerprint"] = compute_json_payload_fingerprint(audit_manifest)
    prepared_plan_cross_link = _build_prepared_plan_cross_link(
        section_id=section_id,
        prepared_plan=prepared_plan,
        prepared_plan_audit_fingerprint=execution_artifact["prepared_plan_audit_fingerprint"],
    )
    execution_artifact["prepared_plan_cross_link"] = prepared_plan_cross_link
    execution_artifact["prepared_plan_cross_link_fingerprint"] = compute_json_payload_fingerprint(prepared_plan_cross_link)
    _write_json(execution_path, execution_artifact)


def execute_prepared_section(workspace_path: str | Path, section_id: str, *, rerun: bool = False) -> dict[str, str | bool]:
    project_root = resolve_workspace_path(workspace_path)
    preparation = prepare_section_execution(project_root, section_id, persist_prepared_plan=False)
    if _has_prior_execution_artifacts(project_root, section_id) and rerun is not True:
        raise ValueError(f"Section has prior execution artifacts; rerun=true required: {section_id}")
    if preparation["eligible"] is not True:
        raise ValueError(f"Section is not eligible for execution: {section_id}")
    prepared_plan = load_prepared_section_plan_artifact(project_root, section_id)
    _assert_prepared_plan_approval_lineage_matches(project_root, section_id)
    _assert_prepared_plan_binding_matches(project_root, section_id)
    prepared_file_plan = load_prepared_section_file_plan(project_root, section_id)
    if rerun is True and _has_prior_execution_artifacts(project_root, section_id):
        _assert_rerun_is_safe(project_root, section_id, prepared_file_plan)

    result = run_dgce_section(section_id, project_root, governed=True, prepared_file_plan=prepared_file_plan)
    if str(result.status) != "success":
        raise ValueError(f"Section execution blocked: {result.reason}")
    _persist_prepared_plan_audit_manifest(project_root, section_id, prepared_plan)

    return {
        "status": "ok",
        "section_id": section_id,
        "executed": True,
        "artifacts_updated": True,
    }


def execute_prepared_section_bundle(
    workspace_path: str | Path,
    section_ids: list[str],
    *,
    rerun: bool = False,
) -> tuple[dict[str, Any], int]:
    project_root = resolve_workspace_path(workspace_path)
    if not section_ids:
        return (
            {
                "status": "failed",
                "section_results": [],
                "first_failing_section": None,
                "stopped_early": True,
                "detail": "Bundle requires at least one section_id",
            },
            400,
        )
    if any(not section_id.strip() for section_id in section_ids):
        return (
            {
                "status": "failed",
                "section_results": [],
                "first_failing_section": None,
                "stopped_early": True,
                "detail": "Bundle section_ids must be non-empty strings",
            },
            400,
        )
    if len(section_ids) != len(set(section_ids)):
        return (
            {
                "status": "failed",
                "section_results": [],
                "first_failing_section": None,
                "stopped_early": True,
                "detail": "Bundle section_ids must be unique",
            },
            400,
        )

    section_results: list[dict[str, Any]] = []
    section_records: list[dict[str, Any]] = []
    for section_id in section_ids:
        try:
            result = execute_prepared_section(workspace_path, section_id, rerun=rerun)
        except FileNotFoundError as exc:
            section_results.append(
                {
                    "section_id": section_id,
                    "status": "failed",
                    "detail": str(exc),
                }
            )
            section_records.append(_bundle_section_record(project_root=project_root, section_id=section_id, status="failed"))
            _persist_bundle_execution_audit_manifest(
                project_root=project_root,
                section_ids=section_ids,
                section_records=section_records,
                execution_status="failed",
                stopped_early=True,
                first_failing_section=section_id,
            )
            return (
                {
                    "status": "failed",
                    "section_results": section_results,
                    "first_failing_section": section_id,
                    "stopped_early": True,
                },
                404,
            )
        except ValueError as exc:
            section_results.append(
                {
                    "section_id": section_id,
                    "status": "failed",
                    "detail": str(exc),
                }
            )
            section_records.append(_bundle_section_record(project_root=project_root, section_id=section_id, status="failed"))
            _persist_bundle_execution_audit_manifest(
                project_root=project_root,
                section_ids=section_ids,
                section_records=section_records,
                execution_status="failed",
                stopped_early=True,
                first_failing_section=section_id,
            )
            return (
                {
                    "status": "failed",
                    "section_results": section_results,
                    "first_failing_section": section_id,
                    "stopped_early": True,
                },
                400,
            )
        section_results.append(result)
        section_records.append(_bundle_section_record(project_root=project_root, section_id=section_id, status="success"))

    _persist_bundle_execution_audit_manifest(
        project_root=project_root,
        section_ids=section_ids,
        section_records=section_records,
        execution_status="success",
        stopped_early=False,
        first_failing_section=None,
    )
    return (
        {
            "status": "ok",
            "section_results": section_results,
            "first_failing_section": None,
            "stopped_early": False,
        },
        200,
    )
