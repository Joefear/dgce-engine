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
    compute_json_file_fingerprint,
    compute_json_payload_fingerprint,
    compute_review_artifact_fingerprint,
    _load_section_from_workspace_input,
    _write_json,
    _artifact_manifest_entries_by_path,
    _build_execution_gate_artifact,
    _build_preflight_artifact,
    _build_stale_check_artifact,
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
    ("gate", "execution_gate_path", "execution_gate_record", ".dce/preflight/{section_id}.execution_gate.json"),
    ("alignment", "alignment_path", "alignment_record", ".dce/preflight/{section_id}.alignment.json"),
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
        "execution_gate_path": _artifact_relative_path(section_id, ".dce/preflight/{section_id}.execution_gate.json"),
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


def _prepared_plan_payload(section_id: str, file_plan: dict[str, Any], binding: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_type": "prepared_execution_plan",
        "binding": binding,
        "binding_fingerprint": compute_json_payload_fingerprint(binding),
        "file_plan": file_plan,
        "generated_by": "DGCE",
        "schema_version": "1.0",
        "section_id": section_id,
    }


def load_prepared_section_plan_artifact(project_root: Path, section_id: str) -> dict[str, Any]:
    artifact_path = _prepared_plan_file_path(project_root, section_id)
    if not artifact_path.exists():
        raise ValueError(f"Section requires prepared file plan artifact: {section_id}")
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Prepared file plan artifact must contain a JSON object: {section_id}")
    if str(payload.get("section_id")) != section_id:
        raise ValueError(f"Prepared file plan artifact section mismatch: {section_id}")
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
    return payload


def load_prepared_section_file_plan(project_root: Path, section_id: str) -> FilePlan:
    payload = load_prepared_section_plan_artifact(project_root, section_id)
    try:
        return FilePlan.model_validate(payload["file_plan"])
    except Exception as exc:
        raise ValueError(f"Prepared file plan artifact is malformed: {section_id}") from exc


def _seal_prepared_section_file_plan(project_root: Path, section_id: str) -> None:
    section = _load_section_from_workspace_input(project_root, section_id)
    file_plan = compute_governed_execution_file_plan(section)
    binding = _compute_prepared_plan_binding(project_root, section_id)
    _write_json(
        _prepared_plan_file_path(project_root, section_id),
        _prepared_plan_payload(section_id, file_plan.model_dump(), binding),
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
    gate_relative_path = _artifact_relative_path(section_id, ".dce/preflight/{section_id}.execution_gate.json")

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

    approval_ready = (
        approval_payload is not None
        and str(approval_payload.get("approval_status")) == "approved"
        and approval_payload.get("execution_permitted") is True
    )

    recomputed_preflight = _build_preflight_artifact(dce_root, section_id, SectionPreflightInput())
    preflight_ready = (
        persisted_preflight is not None
        and str(recomputed_preflight.get("preflight_status")) == "preflight_pass"
        and recomputed_preflight.get("execution_allowed") is True
    )

    recomputed_stale = _build_stale_check_artifact(dce_root, section_id, SectionStaleCheckInput())
    recomputed_gate = _build_execution_gate_artifact(
        dce_root,
        section_id,
        require_preflight_pass=True,
        gate_input=SectionExecutionGateInput(),
        preflight_payload=recomputed_preflight,
        stale_check_payload=recomputed_stale,
    )
    gate_ready = (
        persisted_stale is not None
        and persisted_gate is not None
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
