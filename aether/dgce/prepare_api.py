"""Read-only DGCE execution-eligibility checks for one section."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aether.dgce.decompose import (
    SectionExecutionGateInput,
    SectionPreflightInput,
    SectionStaleCheckInput,
    compute_governed_execution_file_plan,
    _build_gate_input_artifact,
    compute_json_file_fingerprint,
    compute_json_payload_fingerprint,
    compute_review_artifact_fingerprint,
    verify_artifact_fingerprint,
    verify_review_artifact_fingerprint,
    _load_section_from_workspace_input,
    _write_json,
    _write_json_with_artifact_fingerprint,
    _artifact_manifest_entries_by_path,
    _build_execution_gate_artifact,
    _build_preflight_artifact,
    _build_stale_check_artifact,
    stage6_execution_gate_allows_downstream,
    _normalize_artifact_path,
    _validate_locked_artifact_schema,
)
from aether.dgce.file_plan import FilePlan
from aether.dgce.path_utils import resolve_workspace_path
from aether.dgce.read_api import get_artifact_manifest, get_workspace_index


_SECTION_ARTIFACT_SPECS = (
    ("input", "input_path", "input_artifact", ".dce/input/{section_id}.json"),
    ("preview", "preview_path", "preview_artifact", ".dce/plans/{section_id}.preview.json"),
    ("review", "review_path", "review_artifact", ".dce/reviews/{section_id}.review.md"),
    ("approval", "approval_path", "approval_artifact", ".dce/approvals/{section_id}.approval.json"),
    ("preflight", "preflight_path", "preflight_record", ".dce/preflight/{section_id}.preflight.json"),
    ("stale_check", "stale_check_path", "stale_check_record", ".dce/preflight/{section_id}.stale_check.json"),
    ("gate", "execution_gate_path", "execution_gate_record", ".dce/execution/gate/{section_id}.execution_gate.json"),
    ("alignment", "alignment_path", "alignment_record", ".dce/execution/alignment/{section_id}.alignment.json"),
    ("simulation_trigger", "simulation_trigger_path", "simulation_trigger_record", ".dce/execution/simulation/{section_id}.simulation_trigger.json"),
    ("simulation", "simulation_path", "simulation_record", ".dce/execution/simulation/{section_id}.simulation.json"),
    ("execution", "execution_path", "execution_record", ".dce/execution/{section_id}.execution.json"),
    ("outputs", "output_path", "output_record", ".dce/outputs/{section_id}.json"),
)


def _artifact_relative_path(section_id: str, template: str) -> str:
    return template.format(section_id=section_id)


def _artifact_file_path(project_root: Path, relative_path: str) -> Path:
    normalized = _normalize_artifact_path(relative_path)
    if normalized is None:
        raise ValueError(f"Invalid artifact path: {relative_path}")
    return project_root / Path(normalized)


def _load_validated_json_artifact(project_root: Path, relative_path: str) -> dict[str, Any]:
    artifact_path = _artifact_file_path(project_root, relative_path)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{artifact_path.name} must contain a JSON object")
    _validate_locked_artifact_schema(artifact_path, payload)
    return payload


def _prepared_plan_relative_path(section_id: str) -> str:
    return f".dce/plans/{section_id}.prepared_plan.json"


def _prepared_plan_file_path(project_root: Path, section_id: str) -> Path:
    return _artifact_file_path(project_root, _prepared_plan_relative_path(section_id))


def _prepared_plan_artifact_relative_paths(section_id: str) -> dict[str, str]:
    return {
        "approval_path": _artifact_relative_path(section_id, ".dce/approvals/{section_id}.approval.json"),
        "execution_gate_path": _artifact_relative_path(section_id, ".dce/execution/gate/{section_id}.execution_gate.json"),
        "input_path": _artifact_relative_path(section_id, ".dce/input/{section_id}.json"),
        "preflight_path": _artifact_relative_path(section_id, ".dce/preflight/{section_id}.preflight.json"),
        "preview_path": _artifact_relative_path(section_id, ".dce/plans/{section_id}.preview.json"),
        "review_path": _artifact_relative_path(section_id, ".dce/reviews/{section_id}.review.md"),
        "stale_check_path": _artifact_relative_path(section_id, ".dce/preflight/{section_id}.stale_check.json"),
    }


def _compute_prepared_plan_binding(project_root: Path, section_id: str) -> dict[str, Any]:
    relative_paths = _prepared_plan_artifact_relative_paths(section_id)
    approval_path = _artifact_file_path(project_root, relative_paths["approval_path"])
    input_path = _artifact_file_path(project_root, relative_paths["input_path"])
    preflight_path = _artifact_file_path(project_root, relative_paths["preflight_path"])
    preview_path = _artifact_file_path(project_root, relative_paths["preview_path"])
    review_path = _artifact_file_path(project_root, relative_paths["review_path"])
    stale_check_path = _artifact_file_path(project_root, relative_paths["stale_check_path"])
    execution_gate_path = _artifact_file_path(project_root, relative_paths["execution_gate_path"])

    approval_payload = json.loads(approval_path.read_text(encoding="utf-8"))
    return {
        "artifact_paths": relative_paths,
        "execution_permitted": approval_payload.get("execution_permitted"),
        "fingerprints": {
            "approval": compute_json_file_fingerprint(approval_path),
            "execution_gate": compute_json_file_fingerprint(execution_gate_path),
            "input": compute_json_file_fingerprint(input_path),
            "preflight": compute_json_file_fingerprint(preflight_path),
            "preview": compute_json_file_fingerprint(preview_path),
            "review": compute_review_artifact_fingerprint(review_path.read_text(encoding="utf-8")),
            "stale_check": compute_json_file_fingerprint(stale_check_path),
        },
        "section_id": section_id,
        "selected_mode": approval_payload.get("selected_mode"),
    }


def _compute_prepared_plan_approval_lineage(project_root: Path, section_id: str) -> dict[str, Any]:
    approval_path = _artifact_relative_path(section_id, ".dce/approvals/{section_id}.approval.json")
    approval_file = _artifact_file_path(project_root, approval_path)
    approval_payload = _load_validated_json_artifact(project_root, approval_path)
    approval_artifact_fingerprint = approval_payload.get("artifact_fingerprint")
    if not isinstance(approval_artifact_fingerprint, str):
        raise ValueError(f"Approval artifact is malformed: {section_id}")
    return {
        "approval_artifact_fingerprint": approval_artifact_fingerprint,
        "approval_path": approval_path,
        "approval_record_fingerprint": compute_json_file_fingerprint(approval_file),
        "approval_status": approval_payload.get("approval_status"),
        "execution_permitted": approval_payload.get("execution_permitted"),
        "section_id": section_id,
        "selected_mode": approval_payload.get("selected_mode"),
    }


def _prepared_plan_payload(
    section_id: str,
    file_plan: dict[str, Any],
    binding: dict[str, Any],
    approval_lineage: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_type": "prepared_execution_plan",
        "approval_lineage": approval_lineage,
        "approval_lineage_fingerprint": compute_json_payload_fingerprint(approval_lineage),
        "binding": binding,
        "binding_fingerprint": compute_json_payload_fingerprint(binding),
        "file_plan": file_plan,
        "generated_by": "DGCE",
        "schema_version": "1.0",
        "section_id": section_id,
    }


def _normalize_prepared_plan_path(path_value: Any) -> str:
    path = Path(str(path_value))
    if path.is_absolute():
        raise ValueError("Prepared file plan artifact contains invalid path")
    normalized_parts: list[str] = []
    for part in path.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise ValueError("Prepared file plan artifact contains invalid path")
        normalized_parts.append(part)
    if not normalized_parts:
        raise ValueError("Prepared file plan artifact contains invalid path")
    return Path(*normalized_parts).as_posix()


def _preview_scope_paths(project_root: Path, section_id: str) -> set[str]:
    preview_relative_path = _artifact_relative_path(section_id, ".dce/plans/{section_id}.preview.json")
    preview_payload = _load_validated_json_artifact(project_root, preview_relative_path)
    preview_file = _artifact_file_path(project_root, preview_relative_path)
    if not verify_artifact_fingerprint(preview_file):
        raise ValueError(f"Preview artifact fingerprint mismatch: {section_id}")
    preview_paths: set[str] = set()
    for entry in preview_payload.get("previews", []):
        if not isinstance(entry, dict):
            continue
        if "path" not in entry:
            continue
        preview_paths.add(_normalize_prepared_plan_path(entry.get("path")))
    return preview_paths


def _validate_prepared_plan_file_plan(project_root: Path, section_id: str, file_plan_payload: dict[str, Any]) -> FilePlan:
    try:
        file_plan = FilePlan.model_validate(file_plan_payload)
    except Exception as exc:
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}") from exc

    preview_paths = _preview_scope_paths(project_root, section_id)
    normalized_paths: set[str] = set()
    for file_entry in file_plan.files:
        normalized_path = _normalize_prepared_plan_path(file_entry.get("path"))
        if normalized_path in normalized_paths:
            raise ValueError(f"Prepared file plan artifact contains duplicate path: {section_id}")
        if normalized_path not in preview_paths:
            raise ValueError(f"Prepared file plan artifact exceeds approved preview scope: {section_id}")
        normalized_paths.add(normalized_path)
    return file_plan


def load_prepared_section_plan_artifact(project_root: Path, section_id: str) -> dict[str, Any]:
    artifact_path = _prepared_plan_file_path(project_root, section_id)
    if not artifact_path.exists():
        raise ValueError(f"Section requires prepared file plan artifact: {section_id}")
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Prepared file plan artifact must contain a JSON object: {section_id}")
    artifact_fingerprint = payload.get("artifact_fingerprint")
    if not isinstance(artifact_fingerprint, str):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    if not verify_artifact_fingerprint(artifact_path):
        raise ValueError(f"Prepared file plan artifact fingerprint mismatch: {section_id}")
    if str(payload.get("section_id")) != section_id:
        raise ValueError(f"Prepared file plan artifact section mismatch: {section_id}")
    approval_lineage = payload.get("approval_lineage")
    if not isinstance(approval_lineage, dict):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    if approval_lineage.get("section_id") != section_id:
        raise ValueError(f"Prepared file plan artifact section mismatch: {section_id}")
    expected_approval_path = _artifact_relative_path(section_id, ".dce/approvals/{section_id}.approval.json")
    if approval_lineage.get("approval_path") != expected_approval_path:
        raise ValueError(f"Prepared file plan artifact approval lineage is malformed: {section_id}")
    for key in ("approval_artifact_fingerprint", "approval_record_fingerprint", "approval_status", "selected_mode"):
        if not isinstance(approval_lineage.get(key), str):
            raise ValueError(f"Prepared file plan artifact approval lineage is malformed: {section_id}")
    if not isinstance(approval_lineage.get("execution_permitted"), bool):
        raise ValueError(f"Prepared file plan artifact approval lineage is malformed: {section_id}")
    approval_lineage_fingerprint = payload.get("approval_lineage_fingerprint")
    if not isinstance(approval_lineage_fingerprint, str):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    if compute_json_payload_fingerprint(approval_lineage) != approval_lineage_fingerprint:
        raise ValueError(f"Prepared file plan artifact approval lineage is malformed: {section_id}")
    binding = payload.get("binding")
    if not isinstance(binding, dict):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    artifact_paths = binding.get("artifact_paths")
    if not isinstance(artifact_paths, dict):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    expected_paths = _prepared_plan_artifact_relative_paths(section_id)
    if artifact_paths != expected_paths:
        raise ValueError(f"Prepared file plan artifact binding is malformed: {section_id}")
    fingerprints = binding.get("fingerprints")
    if not isinstance(fingerprints, dict):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    required_fingerprints = (
        "approval",
        "execution_gate",
        "input",
        "preflight",
        "preview",
        "review",
        "stale_check",
    )
    if any(not isinstance(fingerprints.get(key), str) for key in required_fingerprints):
        raise ValueError(f"Prepared file plan artifact binding is malformed: {section_id}")
    if binding.get("section_id") != section_id:
        raise ValueError(f"Prepared file plan artifact section mismatch: {section_id}")
    if not isinstance(binding.get("execution_permitted"), bool):
        raise ValueError(f"Prepared file plan artifact binding is malformed: {section_id}")
    selected_mode = binding.get("selected_mode")
    if selected_mode is not None and not isinstance(selected_mode, str):
        raise ValueError(f"Prepared file plan artifact binding is malformed: {section_id}")
    binding_fingerprint = payload.get("binding_fingerprint")
    if not isinstance(binding_fingerprint, str):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    if compute_json_payload_fingerprint(binding) != binding_fingerprint:
        raise ValueError(f"Prepared file plan artifact binding is malformed: {section_id}")
    file_plan = payload.get("file_plan")
    if not isinstance(file_plan, dict):
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}")
    _validate_prepared_plan_file_plan(project_root, section_id, file_plan)
    return payload


def load_prepared_section_file_plan(project_root: Path, section_id: str) -> FilePlan:
    payload = load_prepared_section_plan_artifact(project_root, section_id)
    return _validate_prepared_plan_file_plan(project_root, section_id, payload["file_plan"])


def _seal_prepared_section_file_plan(project_root: Path, section_id: str) -> None:
    section = _load_section_from_workspace_input(project_root, section_id)
    file_plan = compute_governed_execution_file_plan(section)
    _validate_prepared_plan_file_plan(project_root, section_id, file_plan.model_dump())
    binding = _compute_prepared_plan_binding(project_root, section_id)
    approval_lineage = _compute_prepared_plan_approval_lineage(project_root, section_id)
    _write_json_with_artifact_fingerprint(
        _prepared_plan_file_path(project_root, section_id),
        _prepared_plan_payload(section_id, file_plan.model_dump(), binding, approval_lineage),
    )


def _validate_manifest_entry(
    manifest_entries: dict[str, dict[str, Any]],
    *,
    artifact_path: str,
    artifact_type: str,
    section_id: str | None,
) -> bool:
    normalized_path = _normalize_artifact_path(artifact_path)
    if normalized_path is None or normalized_path not in manifest_entries:
        return False
    entry = manifest_entries[normalized_path]
    return (
        str(entry.get("artifact_path")) == normalized_path
        and str(entry.get("artifact_type")) == artifact_type
        and str(entry.get("scope")) == ("section" if section_id is not None else "workspace")
        and entry.get("section_id") == section_id
    )


def _reference_path_is_valid(
    project_root: Path,
    manifest_entries: dict[str, dict[str, Any]],
    artifact_path: str | None,
) -> bool:
    normalized_path = _normalize_artifact_path(artifact_path)
    if normalized_path is None or normalized_path not in manifest_entries:
        return False
    return _artifact_file_path(project_root, normalized_path).exists()


def _section_exists(
    section_id: str,
    workspace_index: dict[str, Any],
    manifest_entries: dict[str, dict[str, Any]],
) -> bool:
    if section_id not in [str(entry) for entry in workspace_index.get("section_order", [])]:
        return False
    if not any(
        isinstance(entry, dict) and str(entry.get("section_id")) == section_id
        for entry in workspace_index.get("sections", [])
    ):
        return False
    return any(
        entry.get("section_id") == section_id and str(entry.get("scope")) == "section"
        for entry in manifest_entries.values()
    )


def _workspace_index_entry(workspace_index: dict[str, Any], section_id: str) -> dict[str, Any]:
    for entry in workspace_index.get("sections", []):
        if isinstance(entry, dict) and str(entry.get("section_id")) == section_id:
            return entry
    raise FileNotFoundError(f"Section not found: {section_id}")


def _section_artifacts_valid(
    project_root: Path,
    manifest_entries: dict[str, dict[str, Any]],
    workspace_index_entry: dict[str, Any],
    section_id: str,
) -> bool:
    input_relative_path = _artifact_relative_path(section_id, ".dce/input/{section_id}.json")
    if not _artifact_file_path(project_root, input_relative_path).exists():
        return False

    for _, _, artifact_type, template in _SECTION_ARTIFACT_SPECS[1:3]:
        relative_path = _artifact_relative_path(section_id, template)
        if not _validate_manifest_entry(
            manifest_entries,
            artifact_path=relative_path,
            artifact_type=artifact_type,
            section_id=section_id,
        ):
            return False
        if not _artifact_file_path(project_root, relative_path).exists():
            return False

    linked_paths: list[str] = []
    for field_name in ("lifecycle_trace_path", "execution_path", "output_path"):
        path_value = workspace_index_entry.get(field_name)
        if isinstance(path_value, str):
            linked_paths.append(path_value)
    for link in workspace_index_entry.get("artifact_links", []):
        if isinstance(link, dict) and isinstance(link.get("path"), str):
            linked_paths.append(str(link["path"]))

    for relative_path in linked_paths:
        if not _reference_path_is_valid(project_root, manifest_entries, relative_path):
            return False

    for _, field_name, artifact_type, template in _SECTION_ARTIFACT_SPECS[3:]:
        relative_path = _artifact_relative_path(section_id, template)
        artifact_file = _artifact_file_path(project_root, relative_path)
        manifest_present = _validate_manifest_entry(
            manifest_entries,
            artifact_path=relative_path,
            artifact_type=artifact_type,
            section_id=section_id,
        )
        file_present = artifact_file.exists()
        if manifest_present != file_present:
            return False

    approval_relative_path = _artifact_relative_path(section_id, ".dce/approvals/{section_id}.approval.json")
    if _artifact_file_path(project_root, approval_relative_path).exists():
        approval_payload = _load_validated_json_artifact(project_root, approval_relative_path)
        expected_paths = {
            "input_path": _artifact_relative_path(section_id, ".dce/input/{section_id}.json"),
            "preview_path": _artifact_relative_path(section_id, ".dce/plans/{section_id}.preview.json"),
            "review_path": _artifact_relative_path(section_id, ".dce/reviews/{section_id}.review.md"),
        }
        for field_name, expected_path in expected_paths.items():
            if approval_payload.get(field_name) != expected_path:
                return False
            if field_name == "input_path":
                if not _artifact_file_path(project_root, expected_path).exists():
                    return False
                continue
            if not _reference_path_is_valid(project_root, manifest_entries, expected_path):
                return False

    return True


def prepare_section_execution(
    workspace_path: str | Path,
    section_id: str,
    *,
    persist_prepared_plan: bool = True,
) -> dict[str, Any]:
    project_root = resolve_workspace_path(workspace_path)
    dce_root = project_root / ".dce"
    artifact_manifest = get_artifact_manifest(project_root)
    workspace_index = get_workspace_index(project_root)
    manifest_entries = _artifact_manifest_entries_by_path(artifact_manifest)

    section_exists = _section_exists(section_id, workspace_index, manifest_entries)
    if not section_exists:
        raise FileNotFoundError(f"Section not found: {section_id}")

    workspace_index_entry = _workspace_index_entry(workspace_index, section_id)
    artifacts_valid = _section_artifacts_valid(project_root, manifest_entries, workspace_index_entry, section_id)

    approval_relative_path = _artifact_relative_path(section_id, ".dce/approvals/{section_id}.approval.json")
    preflight_relative_path = _artifact_relative_path(section_id, ".dce/preflight/{section_id}.preflight.json")
    stale_relative_path = _artifact_relative_path(section_id, ".dce/preflight/{section_id}.stale_check.json")
    gate_relative_path = _artifact_relative_path(section_id, ".dce/execution/gate/{section_id}.execution_gate.json")
    gate_input_relative_path = _artifact_relative_path(section_id, ".dce/execution/gate/{section_id}.gate_input.json")
    preview_relative_path = _artifact_relative_path(section_id, ".dce/plans/{section_id}.preview.json")
    review_relative_path = _artifact_relative_path(section_id, ".dce/reviews/{section_id}.review.md")

    approval_payload = (
        _load_validated_json_artifact(project_root, approval_relative_path)
        if _artifact_file_path(project_root, approval_relative_path).exists()
        else None
    )
    persisted_preflight = (
        _load_validated_json_artifact(project_root, preflight_relative_path)
        if _artifact_file_path(project_root, preflight_relative_path).exists()
        else None
    )
    persisted_stale = (
        _load_validated_json_artifact(project_root, stale_relative_path)
        if _artifact_file_path(project_root, stale_relative_path).exists()
        else None
    )
    persisted_gate = (
        _load_validated_json_artifact(project_root, gate_relative_path)
        if _artifact_file_path(project_root, gate_relative_path).exists()
        else None
    )
    persisted_gate_input = (
        _load_validated_json_artifact(project_root, gate_input_relative_path)
        if _artifact_file_path(project_root, gate_input_relative_path).exists()
        else None
    )

    approval_file = _artifact_file_path(project_root, approval_relative_path)
    preview_file = _artifact_file_path(project_root, preview_relative_path)
    review_file = _artifact_file_path(project_root, review_relative_path)
    preflight_file = _artifact_file_path(project_root, preflight_relative_path)
    gate_input_file = _artifact_file_path(project_root, gate_input_relative_path)

    approval_ready = (
        approval_payload is not None
        and verify_artifact_fingerprint(approval_file)
        and verify_artifact_fingerprint(preview_file)
        and verify_review_artifact_fingerprint(review_file)
        and str(approval_payload.get("approval_status")) == "approved"
        and approval_payload.get("execution_permitted") is True
    )

    preflight_timestamp = (
        str(persisted_preflight.get("validation_timestamp"))
        if persisted_preflight is not None and isinstance(persisted_preflight.get("validation_timestamp"), str)
        else "1970-01-01T00:00:00Z"
    )
    stale_timestamp = (
        str(persisted_stale.get("validation_timestamp"))
        if persisted_stale is not None and isinstance(persisted_stale.get("validation_timestamp"), str)
        else "1970-01-01T00:00:00Z"
    )
    gate_timestamp = (
        str(persisted_gate.get("gate_timestamp"))
        if persisted_gate is not None and isinstance(persisted_gate.get("gate_timestamp"), str)
        else "1970-01-01T00:00:00Z"
    )

    recomputed_preflight = _build_preflight_artifact(
        dce_root,
        section_id,
        SectionPreflightInput(validation_timestamp=preflight_timestamp),
    )
    recomputed_preflight_with_fingerprint = {
        **recomputed_preflight,
        "artifact_fingerprint": compute_json_payload_fingerprint(recomputed_preflight),
    }
    preflight_ready = (
        persisted_preflight is not None
        and verify_artifact_fingerprint(preflight_file)
        and persisted_preflight == recomputed_preflight_with_fingerprint
        and str(recomputed_preflight.get("preflight_status")) == "preflight_pass"
        and recomputed_preflight.get("execution_allowed") is True
    )

    recomputed_stale = _build_stale_check_artifact(
        dce_root,
        section_id,
        SectionStaleCheckInput(validation_timestamp=stale_timestamp),
    )
    recomputed_gate_input = _build_gate_input_artifact(dce_root, section_id)
    recomputed_gate_input_with_fingerprint = {
        **recomputed_gate_input,
        "artifact_fingerprint": compute_json_payload_fingerprint(recomputed_gate_input),
    }
    recomputed_gate = _build_execution_gate_artifact(
        dce_root,
        section_id,
        require_preflight_pass=True,
        gate_input=SectionExecutionGateInput(gate_timestamp=gate_timestamp),
        gate_input_payload=recomputed_gate_input_with_fingerprint,
        preflight_payload=recomputed_preflight,
        stale_check_payload=recomputed_stale,
    )
    gate_input_ready = (
        persisted_gate_input is not None
        and verify_artifact_fingerprint(gate_input_file)
        and persisted_gate_input == recomputed_gate_input_with_fingerprint
        and persisted_gate is not None
        and persisted_gate.get("gate_input_path") == gate_input_relative_path
        and persisted_gate.get("gate_input_fingerprint") == persisted_gate_input.get("gate_input_fingerprint")
    )
    gate_ready = (
        persisted_stale is not None
        and persisted_gate is not None
        and persisted_stale == recomputed_stale
        and persisted_gate == recomputed_gate
        and gate_input_ready
        and stage6_execution_gate_allows_downstream(persisted_gate)
        and str(recomputed_stale.get("stale_status")) == "stale_valid"
        and recomputed_stale.get("stale_detected") is False
        and str(recomputed_gate.get("gate_status")) == "gate_pass"
        and recomputed_gate.get("execution_blocked") is False
    )

    checks = {
        "section_exists": section_exists,
        "artifacts_valid": artifacts_valid,
        "approval_ready": approval_ready,
        "preflight_ready": preflight_ready,
        "gate_ready": gate_ready,
    }
    if all(checks.values()) and persist_prepared_plan:
        _seal_prepared_section_file_plan(project_root, section_id)
    return {
        "status": "ok",
        "section_id": section_id,
        "eligible": all(checks.values()),
        "checks": checks,
    }
