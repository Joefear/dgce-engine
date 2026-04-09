"""Deterministic incremental workspace planning helpers for DGCE."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from aether.dgce.code_graph_context import parse_code_graph_context
from aether.dgce.file_plan import FilePlan
from aether.dgce.file_writer import render_file_entry_bytes

EXCLUDED_DIR_NAMES = {
    ".dce",
    ".git",
    ".next",
    ".pytest_cache",
    ".pytest_tmp",
    ".venv",
    "__pycache__",
    "Binaries",
    "DerivedDataCache",
    "Intermediate",
    "Saved",
    "build",
    "dist",
    "node_modules",
}
EXCLUDED_FILE_SUFFIXES = {".log", ".pyc", ".pyo"}


def scan_workspace_file_paths(project_root: Path) -> List[str]:
    """Return a deterministic inventory of safe relative file paths under project_root."""
    root = project_root.resolve()
    file_paths: List[str] = []

    for current_root, dir_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current_root)
        dir_names[:] = sorted(
            name
            for name in dir_names
            if name not in EXCLUDED_DIR_NAMES and not (current_path / name).is_symlink()
        )

        for file_name in sorted(file_names):
            file_path = current_path / file_name
            if file_path.is_symlink():
                continue
            if file_path.suffix.lower() in EXCLUDED_FILE_SUFFIXES:
                continue
            file_paths.append(_normalize_relative_path(file_path.relative_to(root)))

    return sorted(file_paths)


def scan_workspace_inventory(project_root: Path) -> List[Dict[str, Any]]:
    """Return a deterministic relative-path inventory for project files."""
    root = project_root.resolve()
    inventory: List[Dict[str, Any]] = []
    for relative_path in scan_workspace_file_paths(project_root):
        file_path = root / Path(relative_path)
        inventory.append(
            {
                "path": relative_path,
                "ext": Path(relative_path).suffix.lower(),
                "size": file_path.stat().st_size,
            }
        )

    return sorted(inventory, key=lambda entry: str(entry["path"]))


def classify_incremental_targets(
    target_paths: List[str],
    existing_paths: List[str],
    *,
    project_root: Path | None = None,
) -> List[Dict[str, str]]:
    """Classify deterministic section targets against the current workspace inventory."""
    existing = {_normalize_relative_path(Path(path)) for path in existing_paths}
    normalized_targets = sorted({_normalize_relative_path(Path(path)) for path in target_paths})

    changes: List[Dict[str, str]] = []
    for path in normalized_targets:
        resolved_existing_path = _resolve_existing_target_path(project_root, path) if project_root is not None else None
        action = "modify" if path in existing or resolved_existing_path is not None else "create"
        reason = "target_present_in_workspace" if action == "modify" else "target_missing_from_workspace"
        changes.append(
            {
                "path": path,
                "action": action,
                "reason": reason,
            }
        )
    return changes


def build_incremental_change_plan(
    section_id: str,
    file_plan: FilePlan,
    existing_paths: List[str],
    *,
    mode: str = "incremental_v1",
    project_root: Path | None = None,
) -> Dict[str, Any]:
    """Build the persisted incremental_v1 change-plan artifact for one section."""
    target_paths = [str(file_entry["path"]) for file_entry in file_plan.files]
    changes = classify_incremental_targets(target_paths, existing_paths, project_root=project_root)
    target_path_set = {_normalize_relative_path(Path(path)) for path in target_paths}
    ignored_existing_files = sorted(
        path for path in {_normalize_relative_path(Path(path)) for path in existing_paths} if path not in target_path_set
    )

    return {
        "section_id": section_id,
        "mode": mode,
        "summary": {
            "create_count": sum(1 for entry in changes if entry["action"] == "create"),
            "modify_count": sum(1 for entry in changes if entry["action"] == "modify"),
            "ignore_count": len(ignored_existing_files),
        },
        "changes": changes,
        "ignored_existing_files": ignored_existing_files,
    }


def load_expected_file_plan(outputs_path: Path) -> FilePlan:
    """Load the last persisted file plan metadata for incremental comparison."""
    if not outputs_path.exists():
        return FilePlan(project_name="DGCE", files=[])

    payload = outputs_path.read_text(encoding="utf-8").strip()
    if not payload:
        return FilePlan(project_name="DGCE", files=[])

    parsed = json.loads(payload)
    file_plan_data = parsed.get("file_plan", parsed)
    return FilePlan.model_validate(file_plan_data)


def load_change_plan(change_plan_path: Path) -> List[Dict[str, str]]:
    """Load persisted change-plan entries from disk."""
    if not change_plan_path.exists():
        return []

    payload = change_plan_path.read_text(encoding="utf-8").strip()
    if not payload:
        return []

    parsed = json.loads(payload)
    changes = parsed.get("changes", [])
    return [dict(entry) for entry in changes]


def load_owned_paths(ownership_index_path: Path) -> set[str]:
    """Load the currently DGCE-owned paths from the persisted ownership index."""
    if not ownership_index_path.exists():
        return set()

    payload = ownership_index_path.read_text(encoding="utf-8").strip()
    if not payload:
        return set()

    parsed = json.loads(payload)
    files = parsed.get("files", [])
    return {
        _normalize_relative_path(Path(str(entry["path"])))
        for entry in files
        if isinstance(entry, dict) and entry.get("path")
    }


def build_write_transparency(
    file_plan: FilePlan,
    change_plan: List[Dict[str, str]],
    project_root: Path,
    *,
    allow_modify_write: bool = False,
    owned_paths: set[str] | None = None,
) -> tuple[FilePlan, dict]:
    """Build the controlled write plan plus per-path transparency records."""
    actions_by_path = classify_section_targets(file_plan, change_plan, project_root)

    write_files: list[dict[str, Any]] = []
    write_decisions: list[dict[str, Any]] = []
    for file_entry in file_plan.files:
        normalized_path = _normalize_relative_path(Path(str(file_entry["path"])))
        decision, reason = _write_decision(
            file_entry,
            normalized_path,
            actions_by_path,
            project_root,
            allow_modify_write,
            owned_paths or set(),
        )
        if decision == "written":
            write_decision = {
                "path": normalized_path,
                "decision": decision,
                "reason": reason,
            }
            if reason == "modify":
                try:
                    existing_path = _resolve_existing_target_path(project_root, normalized_path)
                    existing_bytes = existing_path.read_bytes() if existing_path is not None else None
                except OSError:
                    existing_bytes = None
                if existing_bytes is not None:
                    new_bytes = render_file_entry_bytes(file_entry)
                    write_decision["diff_visibility"] = _build_diff_visibility(existing_bytes, new_bytes)
            write_files.append(file_entry)
            write_decisions.append(write_decision)
        else:
            write_decisions.append(
                {
                    "path": normalized_path,
                    "decision": decision,
                    "reason": reason,
                }
            )

    transparency = {
        "write_decisions": write_decisions,
        "write_summary": {
            "written_count": len([entry for entry in write_decisions if entry["decision"] == "written"]),
            "modify_written_count": len(
                [entry for entry in write_decisions if entry["decision"] == "written" and entry["reason"] == "modify"]
            ),
            "diff_visible_count": len([entry for entry in write_decisions if "diff_visibility" in entry]),
            "skipped_modify_count": len(
                [entry for entry in write_decisions if entry["decision"] == "skipped" and entry["reason"] == "modify"]
            ),
            "skipped_ignore_count": len(
                [entry for entry in write_decisions if entry["decision"] == "skipped" and entry["reason"] == "ignore"]
            ),
            "skipped_identical_count": len(
                [entry for entry in write_decisions if entry["decision"] == "skipped" and entry["reason"] == "identical"]
            ),
            "skipped_ownership_count": len(
                [entry for entry in write_decisions if entry["decision"] == "skipped" and entry["reason"] == "ownership"]
            ),
            "skipped_exists_fallback_count": len(
                [
                    entry
                    for entry in write_decisions
                    if entry["decision"] == "skipped" and entry["reason"] == "exists_fallback"
                ]
            ),
            "before_bytes_total": sum(
                int(entry["diff_visibility"]["before_bytes"])
                for entry in write_decisions
                if "diff_visibility" in entry
            ),
            "after_bytes_total": sum(
                int(entry["diff_visibility"]["after_bytes"])
                for entry in write_decisions
                if "diff_visibility" in entry
            ),
            "changed_lines_estimate_total": sum(
                int(entry["diff_visibility"]["changed_lines_estimate"])
                for entry in write_decisions
                if "diff_visibility" in entry
            ),
            "bytes_written_total": 0,
        },
    }
    return FilePlan(project_name=file_plan.project_name, files=write_files), transparency


def build_incremental_preview_artifact(
    section_id: str,
    file_plan: FilePlan,
    change_plan: List[Dict[str, str]],
    project_root: Path,
    *,
    allow_modify_write: bool = False,
    owned_paths: set[str] | None = None,
    mode: str = "incremental_v2",
    code_graph_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic metadata-only preview artifact for one section target set."""
    actions_by_path = classify_section_targets(file_plan, change_plan, project_root)
    code_graph_guidance = _build_preview_code_graph_guidance(code_graph_context)

    previews: list[dict[str, Any]] = []
    for file_entry in file_plan.files:
        normalized_path = _normalize_relative_path(Path(str(file_entry["path"])))
        existing_target_path = _resolve_existing_target_path(project_root, normalized_path)
        planned_action = actions_by_path.get(normalized_path, "ignore")
        decision, reason = _write_decision(
            file_entry,
            normalized_path,
            actions_by_path,
            project_root,
            allow_modify_write,
            owned_paths or set(),
        )
        existing_bytes = existing_target_path.read_bytes() if existing_target_path is not None else b""
        generated_bytes = render_file_entry_bytes(file_entry)
        previews.append(
            {
                "path": normalized_path,
                "section_id": section_id,
                "planned_action": planned_action,
                "eligibility": "eligible" if decision == "written" else "blocked",
                "preview_decision": "write" if decision == "written" else "skip",
                "preview_reason": reason,
                "identical_content": reason == "identical",
                "existing_bytes": len(existing_bytes),
                "generated_bytes": len(generated_bytes),
                "approximate_line_delta": _approximate_line_delta(existing_bytes, generated_bytes),
            }
        )
        if normalized_path in code_graph_guidance:
            previews[-1].update(code_graph_guidance[normalized_path])

    previews = sorted(previews, key=lambda entry: str(entry["path"]))
    summary, preview_outcome_class, recommended_mode = summarize_incremental_preview(previews)
    return {
        "section_id": section_id,
        "mode": mode,
        "summary": summary,
        "preview_outcome_class": preview_outcome_class,
        "recommended_mode": recommended_mode,
        "previews": previews,
    }


def summarize_incremental_preview(previews: List[Dict[str, Any]]) -> tuple[dict[str, int], str, str]:
    """Derive stable summary counters plus approval-ready preview classifications."""
    summary = {
        "total_targets": len(previews),
        "total_create": len([entry for entry in previews if entry["planned_action"] == "create"]),
        "total_modify": len([entry for entry in previews if entry["planned_action"] == "modify"]),
        "total_ignore": len([entry for entry in previews if entry["planned_action"] == "ignore"]),
        "total_write": len([entry for entry in previews if entry["preview_decision"] == "write"]),
        "total_skip": len([entry for entry in previews if entry["preview_decision"] == "skip"]),
        "total_eligible": len([entry for entry in previews if entry["eligibility"] == "eligible"]),
        "total_blocked": len([entry for entry in previews if entry["eligibility"] == "blocked"]),
        "total_identical": len([entry for entry in previews if entry["identical_content"]]),
        "total_blocked_ownership": len([entry for entry in previews if entry["preview_reason"] == "ownership"]),
        "total_blocked_modify_disabled": len(
            [
                entry
                for entry in previews
                if entry["preview_reason"] == "modify" and entry["preview_decision"] == "skip"
            ]
        ),
        "total_blocked_ignore": len([entry for entry in previews if entry["preview_reason"] == "ignore"]),
    }
    return summary, _preview_outcome_class(previews, summary), _recommended_mode(previews, summary)


def render_incremental_review_markdown(preview_artifact: dict[str, Any]) -> str:
    """Render a deterministic markdown review bundle from a preview artifact."""
    section_id = str(preview_artifact.get("section_id", ""))
    mode = str(preview_artifact.get("mode", ""))
    preview_outcome_class = str(preview_artifact.get("preview_outcome_class", ""))
    recommended_mode = str(preview_artifact.get("recommended_mode", ""))
    summary = dict(preview_artifact.get("summary", {}))
    previews = sorted(
        [dict(entry) for entry in preview_artifact.get("previews", []) if isinstance(entry, dict)],
        key=lambda entry: str(entry.get("path", "")),
    )

    groups = {
        "Create candidates": [
            entry for entry in previews if entry.get("preview_decision") == "write" and entry.get("preview_reason") == "create"
        ],
        "Modify-ready candidates": [
            entry for entry in previews if entry.get("preview_decision") == "write" and entry.get("preview_reason") == "modify"
        ],
        "Blocked candidates": [
            entry
            for entry in previews
            if entry.get("preview_decision") == "skip" and entry.get("preview_reason") in {"ownership", "modify"}
        ],
        "Identical / no-change candidates": [
            entry for entry in previews if entry.get("preview_decision") == "skip" and entry.get("preview_reason") == "identical"
        ],
        "Ignored candidates": [
            entry for entry in previews if entry.get("preview_decision") == "skip" and entry.get("preview_reason") == "ignore"
        ],
    }
    grouped_paths = {
        str(entry.get("path"))
        for entries in groups.values()
        for entry in entries
    }
    groups["Other"] = [entry for entry in previews if str(entry.get("path")) not in grouped_paths]

    lines = [
        f"# Section Review: {section_id}",
        "",
        f"- Mode: {mode}",
        f"- Preview outcome: {preview_outcome_class}",
        f"- Recommended mode: {recommended_mode}",
        "",
        "## Summary",
        f"- Total targets: {int(summary.get('total_targets', 0))}",
        f"- Create: {int(summary.get('total_create', 0))}",
        f"- Modify: {int(summary.get('total_modify', 0))}",
        f"- Ignore: {int(summary.get('total_ignore', 0))}",
        f"- Write: {int(summary.get('total_write', 0))}",
        f"- Skip: {int(summary.get('total_skip', 0))}",
        f"- Eligible: {int(summary.get('total_eligible', 0))}",
        f"- Blocked: {int(summary.get('total_blocked', 0))}",
        f"- Identical: {int(summary.get('total_identical', 0))}",
        f"- Blocked (ownership): {int(summary.get('total_blocked_ownership', 0))}",
        f"- Blocked (modify disabled): {int(summary.get('total_blocked_modify_disabled', 0))}",
        f"- Blocked (ignore): {int(summary.get('total_blocked_ignore', 0))}",
    ]

    for heading, entries in groups.items():
        if not entries and heading == "Other":
            continue
        lines.extend(["", f"## {heading}"])
        if not entries:
            lines.append("- none")
            continue
        for entry in entries:
            lines.append(
                "- "
                f"`{entry.get('path', '')}`"
                f" -- decision: {entry.get('preview_decision', '')}"
                f" / reason: {entry.get('preview_reason', '')}"
                f" / existing: {int(entry.get('existing_bytes', 0))}"
                f" / generated: {int(entry.get('generated_bytes', 0))}"
                f" / delta: {int(entry.get('approximate_line_delta', 0))}"
            )

    return "\n".join(lines) + "\n"


def finalize_write_transparency(
    transparency: dict,
    project_root: Path,
) -> dict:
    """Populate bytes-written fields for already-written transparency records."""
    bytes_written_total = 0
    finalized_decisions: list[dict[str, Any]] = []
    for entry in transparency["write_decisions"]:
        finalized = dict(entry)
        if finalized["decision"] == "written":
            bytes_written = (project_root / Path(str(finalized["path"]))).stat().st_size
            finalized["bytes_written"] = bytes_written
            bytes_written_total += bytes_written
        finalized_decisions.append(finalized)

    return {
        "write_decisions": finalized_decisions,
        "write_summary": {
            **transparency["write_summary"],
            "written_count": len([entry for entry in finalized_decisions if entry["decision"] == "written"]),
            "modify_written_count": len(
                [entry for entry in finalized_decisions if entry["decision"] == "written" and entry["reason"] == "modify"]
            ),
            "diff_visible_count": len([entry for entry in finalized_decisions if "diff_visibility" in entry]),
            "bytes_written_total": bytes_written_total,
        },
    }


def filter_file_plan_for_controlled_write(
    file_plan: FilePlan,
    change_plan: List[Dict[str, str]],
    project_root: Path,
    *,
    allow_modify_write: bool = False,
    owned_paths: set[str] | None = None,
) -> FilePlan:
    """Return only file-plan entries allowed to reach the writer under the current contract."""
    write_plan, _ = build_write_transparency(
        file_plan,
        change_plan,
        project_root,
        allow_modify_write=allow_modify_write,
        owned_paths=owned_paths,
    )
    return write_plan


def should_write_planned_file(
    normalized_path: str,
    actions_by_path: Dict[str, str],
    project_root: Path,
    *,
    allow_modify_write: bool = False,
) -> bool:
    """
    Perform a lightweight pre-check for whether a planned file may be written under the DGCE controlled write contract.

    Default v1 rules:

    - create -> always allowed
    - modify -> always skipped
    - ignore -> always skipped
    - path not in change plan:
        - written only if the file does not exist on disk
        - skipped if the file already exists (conservative fallback)

    Safe Modify v2:

    - modify -> allowed only when allow_modify_write=True

    Important:

    - this helper does NOT account for identical-content skipping in Safe Modify v2.5
    - this helper is a pre-check, not the final write authority
    - final per-path decisions are determined by _write_decision(...) and persisted in write_transparency

    This function is deterministic and does not modify state.
    """
    action = actions_by_path.get(normalized_path)
    if action == "create":
        return _resolve_existing_target_path(project_root, normalized_path) is None
    if action == "modify":
        return allow_modify_write
    if action == "ignore":
        return False
    return _resolve_existing_target_path(project_root, normalized_path) is None


def overwrite_paths_from_transparency(transparency: dict) -> set[str]:
    """Return the normalized paths that should be overwritten under Safe Modify."""
    return {
        str(entry["path"])
        for entry in transparency.get("write_decisions", [])
        if entry.get("decision") == "written" and entry.get("reason") == "modify"
    }


def _write_decision(
    file_entry: dict[str, Any],
    normalized_path: str,
    actions_by_path: Dict[str, str],
    project_root: Path,
    allow_modify_write: bool = False,
    owned_paths: set[str] | None = None,
) -> tuple[str, str]:
    """Return the controlled-write decision and reason for one planned file path."""
    existing_target_path = _resolve_existing_target_path(project_root, normalized_path)
    action = actions_by_path.get(normalized_path)
    if action == "create":
        if existing_target_path is not None:
            return "skipped", "exists_fallback"
        return "written", "create"
    if action == "modify":
        if normalized_path not in (owned_paths or set()):
            return "skipped", "ownership"
        if allow_modify_write:
            if _has_identical_existing_content(file_entry, normalized_path, project_root):
                return "skipped", "identical"
            return "written", "modify"
        return "skipped", "modify"
    if action == "ignore":
        return "skipped", "ignore"
    if existing_target_path is not None:
        return "skipped", "exists_fallback"
    return "written", "create"


def _has_identical_existing_content(
    file_entry: dict[str, Any],
    normalized_path: str,
    project_root: Path,
) -> bool:
    """Return True when an existing modify target already matches the intended bytes exactly."""
    target_path = _resolve_existing_target_path(project_root, normalized_path)
    if target_path is None:
        return False
    return target_path.read_bytes() == render_file_entry_bytes(file_entry)


def _build_diff_visibility(existing_bytes: bytes, new_bytes: bytes) -> dict[str, int]:
    """Build a lightweight deterministic diff-visibility summary for one modify write."""
    before_lines = existing_bytes.decode("utf-8", errors="replace").splitlines()
    after_lines = new_bytes.decode("utf-8", errors="replace").splitlines()
    changed_lines_estimate = 0
    max_lines = max(len(before_lines), len(after_lines))
    for index in range(max_lines):
        before_line = before_lines[index] if index < len(before_lines) else None
        after_line = after_lines[index] if index < len(after_lines) else None
        if before_line != after_line:
            changed_lines_estimate += 1

    return {
        "before_bytes": len(existing_bytes),
        "after_bytes": len(new_bytes),
        "changed_lines_estimate": changed_lines_estimate,
    }


def _approximate_line_delta(existing_bytes: bytes, generated_bytes: bytes) -> int:
    """Return a deterministic line-delta estimate without storing file contents."""
    if not existing_bytes:
        return len(generated_bytes.decode("utf-8", errors="replace").splitlines())
    return int(_build_diff_visibility(existing_bytes, generated_bytes)["changed_lines_estimate"])


def _preview_outcome_class(previews: List[Dict[str, Any]], summary: dict[str, int]) -> str:
    """Classify the preview artifact into one stable approval-ready outcome bucket."""
    if not previews:
        return "preview_empty"
    if all(entry["preview_reason"] == "ignore" for entry in previews):
        return "preview_ignore_only"
    if summary["total_blocked_ownership"] > 0:
        return "preview_blocked_ownership"
    if (
        summary["total_blocked_modify_disabled"] > 0
        and summary["total_write"] == 0
        and summary["total_create"] == 0
        and summary["total_ignore"] == 0
        and summary["total_identical"] == 0
    ):
        return "preview_blocked_modify_disabled"
    actionable = [entry for entry in previews if entry["planned_action"] in {"create", "modify"}]
    if actionable and all(entry["planned_action"] == "create" and entry["preview_decision"] == "write" for entry in actionable):
        return "preview_create_only"
    if summary["total_write"] == 0 and actionable and all(
        entry["planned_action"] == "modify" and entry["preview_reason"] == "identical" for entry in actionable
    ):
        return "preview_identical_only"
    if any(entry["preview_reason"] == "modify" and entry["preview_decision"] == "write" for entry in previews) and summary["total_blocked_ownership"] == 0:
        return "preview_safe_modify_ready"
    return "preview_mixed"


def _recommended_mode(previews: List[Dict[str, Any]], summary: dict[str, int]) -> str:
    """Return the stable recommended mode for later approval/review consumers."""
    if not previews or (
        summary["total_write"] == 0
        and summary["total_blocked_ownership"] == 0
        and summary["total_blocked_modify_disabled"] == 0
    ):
        return "no_changes"
    if summary["total_blocked_ownership"] > 0:
        return "review_required"
    if any(entry["preview_reason"] == "modify" and entry["preview_decision"] == "write" for entry in previews):
        return "safe_modify"
    if summary["total_write"] > 0 and summary["total_modify"] == 0:
        return "create_only"
    if summary["total_blocked_modify_disabled"] > 0:
        return "review_required"
    return "review_required"


def _build_preview_code_graph_guidance(raw_context: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Return per-path preview-only structural guidance from optional Code Graph facts."""
    if raw_context is None:
        return {}

    try:
        parsed_context = parse_code_graph_context(raw_context)
    except ValueError:
        return {}

    placement_facts = parsed_context.get("placement_facts")
    if placement_facts is None:
        return {}

    guidance_by_path: dict[str, dict[str, Any]] = {}
    insertion_candidates = placement_facts.get("insertion_candidates") or []
    for candidate in insertion_candidates:
        normalized_path = _normalize_code_graph_guidance_path(candidate.get("file_path"))
        if normalized_path is None:
            continue
        guidance_by_path.setdefault(normalized_path, {"insertion_candidates": []})["insertion_candidates"].append(candidate)

    relevant_paths: set[str] = set(guidance_by_path)
    target = parsed_context.get("target")
    if target is not None:
        normalized_target_path = _normalize_code_graph_guidance_path(target.get("file_path"))
        if normalized_target_path is not None:
            relevant_paths.add(normalized_target_path)

    patch_facts = parsed_context.get("patch_facts")
    if patch_facts is not None:
        for touched_file in patch_facts.get("touched_files") or []:
            normalized_touched_path = _normalize_code_graph_guidance_path(touched_file)
            if normalized_touched_path is not None:
                relevant_paths.add(normalized_touched_path)

    for normalized_path in sorted(relevant_paths):
        path_guidance = guidance_by_path.setdefault(normalized_path, {})
        if placement_facts.get("generation_collision_detected") is not None:
            path_guidance["generation_collision_detected"] = placement_facts.get("generation_collision_detected")
        if placement_facts.get("recommended_edit_strategy") is not None:
            path_guidance["recommended_edit_strategy"] = placement_facts.get("recommended_edit_strategy")
        if "insertion_candidates" in path_guidance:
            path_guidance["insertion_candidates"] = sorted(
                path_guidance["insertion_candidates"],
                key=_code_graph_insertion_candidate_sort_key,
            )

    return guidance_by_path


def _normalize_code_graph_guidance_path(path_value: Any) -> str | None:
    """Return a normalized relative guidance path or None when the facts path is unusable."""
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    try:
        return _normalize_relative_path(Path(path_value))
    except ValueError:
        return None


def _code_graph_insertion_candidate_sort_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    """Return a deterministic ordering key for preview-side insertion candidates."""
    span = candidate.get("span")
    return (
        str(candidate.get("file_path", "")),
        str(candidate.get("symbol_id") or ""),
        str(candidate.get("symbol_name") or ""),
        str(candidate.get("strategy") or ""),
        -1 if not isinstance(span, dict) or span.get("start_line") is None else int(span.get("start_line")),
        -1 if not isinstance(span, dict) or span.get("end_line") is None else int(span.get("end_line")),
    )


def build_change_plan(
    section_id: str,
    expected_file_plan: FilePlan,
    workspace_inventory: List[Dict[str, Any]],
    *,
    project_root: Path | None = None,
) -> List[Dict[str, str]]:
    """Build a deterministic create/modify/ignore change plan."""
    expected_paths = {
        _normalize_relative_path(Path(str(file_entry["path"])))
        for file_entry in expected_file_plan.files
    }
    inventory_paths = {
        _normalize_relative_path(Path(str(entry["path"])))
        for entry in workspace_inventory
    }

    changes: List[Dict[str, str]] = []
    for path in sorted(expected_paths):
        resolved_existing_path = _resolve_existing_target_path(project_root, path) if project_root is not None else None
        action = "modify" if path in inventory_paths or resolved_existing_path is not None else "create"
        reason = "expected_target_exists" if action == "modify" else "expected_target_missing"
        changes.append(
            {
                "section_id": section_id,
                "path": path,
                "action": action,
                "reason": reason,
            }
        )

    for path in sorted(inventory_paths - expected_paths):
        changes.append(
            {
                "section_id": section_id,
                "path": path,
                "action": "ignore",
                "reason": "not_in_expected_targets",
            }
        )

    return sorted(changes, key=lambda entry: str(entry["path"]))


def classify_section_targets(
    file_plan: FilePlan,
    change_plan: List[Dict[str, str]],
    project_root: Path,
) -> Dict[str, str]:
    """Build the authoritative per-path action map for the current file plan."""
    actions_by_path = {
        _normalize_relative_path(Path(str(entry["path"]))): str(entry.get("action", "ignore"))
        for entry in change_plan
    }
    for file_entry in file_plan.files:
        normalized_path = _normalize_relative_path(Path(str(file_entry["path"])))
        if normalized_path in actions_by_path:
            continue
        actions_by_path[normalized_path] = (
            "modify" if _resolve_existing_target_path(project_root, normalized_path) is not None else "create"
        )
    return actions_by_path


def _normalize_relative_path(path: Path) -> str:
    """Normalize and validate a safe relative path."""
    if path.is_absolute():
        raise ValueError(f"Absolute paths are not allowed: {path}")

    normalized_parts: List[str] = []
    for part in path.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise ValueError(f"Parent traversal is not allowed: {path}")
        normalized_parts.append(part)

    if not normalized_parts:
        raise ValueError("Path must not be empty")
    return Path(*normalized_parts).as_posix()


def _resolve_existing_target_path(project_root: Path | None, normalized_path: str) -> Path | None:
    """Resolve an already-existing target file from the current project root or its host repo root."""
    if project_root is None:
        return None

    relative_path = Path(normalized_path)
    root = project_root.resolve()
    candidates = [root / relative_path]
    parent_root = root.parent
    if parent_root != root:
        candidates.append(parent_root / relative_path)

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None
