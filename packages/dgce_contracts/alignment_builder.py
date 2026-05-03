"""Deterministic Stage 7 alignment_record.v1 builder.

This module builds only the locked alignment_record.v1 contract object. It does
not wire into DGCE lifecycle stages, execute Stage 8, read project files, call
LLMs, or treat resolver/code-graph systems as required authorities.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "dgce-contracts"
    / "schemas"
    / "alignment"
    / "alignment_record.v1.schema.json"
)


def build_alignment_record_v1(
    *,
    alignment_id: str,
    timestamp: str,
    input_fingerprint: str,
    approval_fingerprint: str,
    preview_fingerprint: str,
    approved_design_expectations: Sequence[Mapping[str, Any]],
    preview_proposed_targets: Sequence[Mapping[str, Any]],
    current_observed_targets: Sequence[Mapping[str, Any]] | None = None,
    resolver_context: Mapping[str, Any] | None = None,
    code_graph_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic schema-valid Stage 7 alignment record."""
    approved_targets = _normalize_targets(approved_design_expectations, "approved_design_expectations")
    preview_targets = _normalize_targets(preview_proposed_targets, "preview_proposed_targets")
    observed_targets = (
        _normalize_targets(current_observed_targets, "current_observed_targets")
        if current_observed_targets is not None
        else dict(preview_targets)
    )
    available_targets = {**preview_targets, **observed_targets}
    resolver_enrichment = _normalize_resolver_context(resolver_context)
    code_graph_enrichment = _normalize_code_graph_context(
        code_graph_context,
        approved_targets=approved_targets,
        preview_targets=preview_targets,
        observed_targets=observed_targets,
        available_targets=available_targets,
    )
    drift_items = _sort_drift_items(
        _deduplicate_drift_items(
            _build_drift_items(approved_targets, preview_targets, observed_targets, available_targets)
            + resolver_enrichment["drift_items"]
            + code_graph_enrichment["drift_items"]
        )
    )
    blocking_count = sum(1 for item in drift_items if item["severity"] == "blocking")
    informational_count = sum(1 for item in drift_items if item["severity"] == "informational")
    alignment_result = "misaligned" if blocking_count else "aligned"
    record = {
        "alignment_id": _required_string(alignment_id, "alignment_id"),
        "timestamp": _required_string(timestamp, "timestamp"),
        "input_fingerprint": _required_string(input_fingerprint, "input_fingerprint"),
        "approval_fingerprint": _required_string(approval_fingerprint, "approval_fingerprint"),
        "preview_fingerprint": _required_string(preview_fingerprint, "preview_fingerprint"),
        "alignment_result": alignment_result,
        "drift_detected": bool(blocking_count),
        "drift_items": drift_items,
        "evidence": _build_evidence(
            approved_targets,
            preview_targets,
            observed_targets,
            [*resolver_enrichment["evidence"], *code_graph_enrichment["evidence"]],
        ),
        "alignment_enrichment": {
            "code_graph_used": code_graph_enrichment["code_graph_used"],
            "resolver_used": resolver_enrichment["resolver_used"],
            "enrichment_status": _combined_enrichment_status(resolver_enrichment, code_graph_enrichment),
        },
        "execution_permitted": alignment_result == "aligned",
        "alignment_summary": {
            "primary_reason": _primary_reason(drift_items),
            "blocking_issues_count": blocking_count,
            "informational_issues_count": informational_count,
        },
    }
    validate_alignment_record_v1(record)
    return record


def validate_alignment_record_v1(record: Mapping[str, Any]) -> bool:
    """Validate a Stage 7 alignment record against the locked schema."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(dict(record)), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.path) or "<root>"
        raise ValueError(f"alignment_record invalid at {path}: {first.message}")
    return True


def _normalize_targets(targets: Sequence[Mapping[str, Any]] | None, field_name: str) -> dict[str, dict[str, Any]]:
    if targets is None:
        raise ValueError(f"{field_name} is required")
    if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
        raise ValueError(f"{field_name} must be an array of objects")
    normalized: dict[str, dict[str, Any]] = {}
    for index, target in enumerate(targets):
        if not isinstance(target, Mapping):
            raise ValueError(f"{field_name}[{index}] must be an object")
        allowed_keys = {"target", "reference", "structure"}
        extra = sorted(set(target) - allowed_keys)
        if extra:
            raise ValueError(f"{field_name}[{index}] contains unsupported fields: {', '.join(extra)}")
        target_name = _required_string(target.get("target"), f"{field_name}[{index}].target")
        if target_name in normalized:
            raise ValueError(f"{field_name}[{index}].target duplicates {target_name}")
        reference = target.get("reference", target_name)
        structure = target.get("structure", {})
        normalized[target_name] = {
            "target": target_name,
            "reference": _required_string(reference, f"{field_name}[{index}].reference"),
            "structure": _normalize_structure(structure, f"{field_name}[{index}].structure"),
        }
    return normalized


def _normalize_structure(value: Any, field_name: str) -> dict[str, str | int | bool]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    normalized: dict[str, str | int | bool] = {}
    for key, item in value.items():
        key_text = _required_string(key, f"{field_name}.key")
        if not isinstance(item, (str, int, bool)):
            raise ValueError(f"{field_name}.{key_text} must be a string, integer, or boolean")
        if isinstance(item, str):
            normalized[key_text] = _required_string(item, f"{field_name}.{key_text}")
        else:
            normalized[key_text] = item
    return dict(sorted(normalized.items()))


def _build_drift_items(
    approved_targets: Mapping[str, dict[str, Any]],
    preview_targets: Mapping[str, dict[str, Any]],
    observed_targets: Mapping[str, dict[str, Any]],
    available_targets: Mapping[str, dict[str, Any]],
) -> list[dict[str, str]]:
    drift_items: list[dict[str, str]] = []
    approved_names = set(approved_targets)
    available_names = set(available_targets)
    preview_names = set(preview_targets)
    observed_names = set(observed_targets)
    for target in sorted(approved_names - available_names):
        drift_items.append(
            _drift_item(
                code="missing_expected_artifact",
                summary="Approved expected artifact is missing from preview and observed targets.",
                target=target,
                severity="blocking",
            )
        )
    for target in sorted((preview_names | observed_names) - approved_names):
        drift_items.append(
            _drift_item(
                code="unexpected_artifact",
                summary="Preview or observed target is outside approved expectations.",
                target=target,
                severity="blocking",
            )
        )
    for target in sorted(approved_names & available_names):
        approved_structure = approved_targets[target]["structure"]
        if not approved_structure:
            continue
        candidate_structure = available_targets[target]["structure"]
        if candidate_structure and candidate_structure != approved_structure:
            drift_items.append(
                _drift_item(
                    code="structure_mismatch",
                    summary="Comparable structured metadata differs from approved expectations.",
                    target=target,
                    severity="informational",
                )
            )
    return _sort_drift_items(drift_items)


def _drift_item(*, code: str, summary: str, target: str, severity: str) -> dict[str, str]:
    return {
        "code": code,
        "summary": summary,
        "target": target,
        "severity": severity,
    }


def _sort_drift_items(drift_items: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    return sorted(
        [dict(item) for item in drift_items],
        key=lambda item: (item["severity"] != "blocking", item["code"], item["target"]),
    )


def _deduplicate_drift_items(drift_items: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    deduplicated: dict[tuple[str, str, str], dict[str, str]] = {}
    for item in drift_items:
        normalized = dict(item)
        key = (normalized["severity"], normalized["code"], normalized["target"])
        deduplicated.setdefault(key, normalized)
    return list(deduplicated.values())


def _build_evidence(
    approved_targets: Mapping[str, dict[str, Any]],
    preview_targets: Mapping[str, dict[str, Any]],
    observed_targets: Mapping[str, dict[str, Any]],
    enrichment_evidence: Sequence[Mapping[str, str]] | None = None,
) -> list[dict[str, str]]:
    evidence_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for source, targets in (
        ("approval", approved_targets),
        ("preview", preview_targets),
        ("runtime_state", observed_targets),
    ):
        for target_name in sorted(targets):
            reference = targets[target_name]["reference"]
            evidence_by_key[(source, reference)] = {
                "source": source,
                "reference": reference,
            }
    for evidence in enrichment_evidence or []:
        source = _required_string(evidence.get("source"), "enrichment_evidence.source")
        reference = _required_string(evidence.get("reference"), "enrichment_evidence.reference")
        snippet_hash = evidence.get("snippet_hash")
        normalized = {
            "source": source,
            "reference": reference,
        }
        if snippet_hash is not None:
            normalized["snippet_hash"] = _required_string(snippet_hash, "enrichment_evidence.snippet_hash")
        evidence_by_key[(source, reference)] = normalized
    return [evidence_by_key[key] for key in sorted(evidence_by_key)]


def _normalize_resolver_context(resolver_context: Mapping[str, Any] | None) -> dict[str, Any]:
    if resolver_context is None:
        return {
            "resolver_used": False,
            "enrichment_status": "not_used",
            "drift_items": [],
            "evidence": [],
        }
    if not isinstance(resolver_context, Mapping):
        raise ValueError("resolver_context must be an object")
    allowed_keys = {"resolved_symbols", "unresolved_symbols", "resolution_status"}
    extra = sorted(set(resolver_context) - allowed_keys)
    if extra:
        raise ValueError(f"resolver_context contains unsupported fields: {', '.join(extra)}")
    resolved_symbols = _normalize_resolver_symbols(
        resolver_context.get("resolved_symbols"),
        "resolver_context.resolved_symbols",
        resolved=True,
    )
    unresolved_symbols = _normalize_resolver_symbols(
        resolver_context.get("unresolved_symbols"),
        "resolver_context.unresolved_symbols",
        resolved=False,
    )
    if not resolved_symbols and not unresolved_symbols:
        return {
            "resolver_used": False,
            "enrichment_status": "not_used",
            "drift_items": [],
            "evidence": [],
        }
    drift_items: list[dict[str, str]] = []
    evidence: list[dict[str, str]] = []
    for symbol in resolved_symbols:
        symbol_name = symbol["symbol_name"]
        symbol_kind = symbol["symbol_kind"]
        confidence = symbol["confidence"]
        evidence.append(
            {
                "source": "resolver",
                "reference": f"resolver:resolved:{symbol_kind}:{symbol_name}",
            }
        )
        if confidence == "candidate_match":
            drift_items.append(
                _drift_item(
                    code="symbol_resolution_conflict",
                    summary="Resolver selected a non-exact candidate match from bounded path metadata.",
                    target=symbol_name,
                    severity="informational",
                )
            )
    for symbol in unresolved_symbols:
        symbol_name = symbol["symbol_name"]
        symbol_kind = symbol["symbol_kind"]
        evidence.append(
            {
                "source": "resolver",
                "reference": f"resolver:unresolved:{symbol_kind}:{symbol_name}",
            }
        )
        drift_items.append(
            _drift_item(
                code="symbol_resolution_conflict",
                summary="Resolver could not resolve requested symbol from bounded path metadata.",
                target=symbol_name,
                severity="blocking",
            )
        )
    return {
        "resolver_used": True,
        "enrichment_status": "full" if resolved_symbols and not drift_items else "partial",
        "drift_items": _sort_drift_items(drift_items),
        "evidence": sorted(evidence, key=lambda item: (item["source"], item["reference"])),
    }


def _normalize_code_graph_context(
    code_graph_context: Mapping[str, Any] | None,
    *,
    approved_targets: Mapping[str, dict[str, Any]],
    preview_targets: Mapping[str, dict[str, Any]],
    observed_targets: Mapping[str, dict[str, Any]],
    available_targets: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    facts = _parse_optional_code_graph_facts(code_graph_context)
    if facts is None or not _code_graph_has_usable_facts(facts):
        return {
            "code_graph_used": False,
            "enrichment_status": "not_used",
            "drift_items": [],
            "evidence": [],
        }

    drift_items: list[dict[str, str]] = []
    relevant_paths = _code_graph_relevant_paths(facts)
    target = _code_graph_primary_target(facts, relevant_paths)

    placement_facts = facts.get("placement_facts")
    if isinstance(placement_facts, Mapping):
        insertion_candidates = placement_facts.get("insertion_candidates")
        if placement_facts.get("generation_collision_detected") is True and not insertion_candidates:
            drift_items.append(
                _drift_item(
                    code="insertion_point_invalid",
                    summary="Code Graph placement facts indicate no bounded insertion candidate is available.",
                    target=target,
                    severity="blocking",
                )
            )

    patch_facts = facts.get("patch_facts")
    if isinstance(patch_facts, Mapping) and (
        patch_facts.get("claimed_intent_match") is False
        or patch_facts.get("structural_scope_expanded") is True
    ):
        drift_items.append(
            _drift_item(
                code="structure_mismatch",
                summary="Code Graph structural facts differ from the approved bounded target shape.",
                target=target,
                severity="informational",
            )
        )

    approved_names = set(approved_targets)
    available_names = set(available_targets)
    observed_names = set(observed_targets)
    preview_names = set(preview_targets)
    for path in relevant_paths:
        if path in approved_names and path not in available_names:
            drift_items.append(
                _drift_item(
                    code="missing_expected_artifact",
                    summary="Code Graph references an approved artifact absent from preview and observed targets.",
                    target=path,
                    severity="blocking",
                )
            )
        elif path not in approved_names and path not in preview_names and path not in observed_names:
            drift_items.append(
                _drift_item(
                    code="unexpected_artifact",
                    summary="Code Graph references a bounded artifact outside approved expectations.",
                    target=path,
                    severity="informational",
                )
            )

    impact_facts = facts.get("impact_facts")
    if isinstance(impact_facts, Mapping) and impact_facts.get("dependency_crossings"):
        drift_items.append(
            _drift_item(
                code="dependency_mismatch",
                summary="Code Graph dependency facts identify a bounded dependency crossing.",
                target=target,
                severity="informational",
            )
        )

    drift_items = _sort_drift_items(_deduplicate_drift_items(drift_items))
    return {
        "code_graph_used": True,
        "enrichment_status": "full" if not drift_items else "partial",
        "drift_items": drift_items,
        "evidence": [_code_graph_evidence(facts, target)],
    }


def _parse_optional_code_graph_facts(code_graph_context: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if code_graph_context is None or not isinstance(code_graph_context, Mapping):
        return None
    raw_context: Any
    if code_graph_context.get("availability_status") == "available":
        raw_context = code_graph_context.get("facts")
    elif "contract_version" in code_graph_context or "contract_name" in code_graph_context:
        raw_context = code_graph_context
    else:
        return None
    if not isinstance(raw_context, Mapping):
        return None
    try:
        from aether.dgce.code_graph_context import parse_code_graph_context

        return dict(parse_code_graph_context(dict(raw_context)))
    except Exception:
        return None


def _code_graph_has_usable_facts(facts: Mapping[str, Any]) -> bool:
    return any(
        isinstance(facts.get(key), Mapping)
        for key in ("target", "intent_facts", "patch_facts", "placement_facts", "impact_facts", "ownership_facts")
    )


def _code_graph_relevant_paths(facts: Mapping[str, Any]) -> list[str]:
    paths: set[str] = set()
    target = facts.get("target")
    if isinstance(target, Mapping):
        target_path = _bounded_path(target.get("file_path"))
        if target_path is not None:
            paths.add(target_path)
    patch_facts = facts.get("patch_facts")
    if isinstance(patch_facts, Mapping):
        for path in patch_facts.get("touched_files") or []:
            normalized = _bounded_path(path)
            if normalized is not None:
                paths.add(normalized)
    placement_facts = facts.get("placement_facts")
    if isinstance(placement_facts, Mapping):
        for candidate in placement_facts.get("insertion_candidates") or []:
            if not isinstance(candidate, Mapping):
                continue
            normalized = _bounded_path(candidate.get("file_path"))
            if normalized is not None:
                paths.add(normalized)
    return sorted(paths)


def _code_graph_primary_target(facts: Mapping[str, Any], relevant_paths: Sequence[str]) -> str:
    target = facts.get("target")
    if isinstance(target, Mapping):
        for key in ("file_path", "symbol_name", "symbol_id"):
            normalized = _bounded_reference_fragment(target.get(key))
            if normalized is not None:
                return normalized
    if relevant_paths:
        return relevant_paths[0]
    return "dcg.facts.v1"


def _code_graph_evidence(facts: Mapping[str, Any], target: str) -> dict[str, str]:
    graph_id = _required_string(facts.get("graph_id"), "code_graph_context.graph_id")
    graph_digest = hashlib.sha256(graph_id.encode("utf-8")).hexdigest()[:16]
    anchor = _bounded_reference_fragment(target) or "facts"
    reference = f"code_graph:{graph_digest}#{anchor}"
    bounded_hash_payload = {
        "graph_id": graph_id,
        "target": target,
        "paths": _code_graph_relevant_paths(facts),
        "placement": _code_graph_bounded_mapping(
            facts.get("placement_facts"),
            ("generation_collision_detected", "recommended_edit_strategy"),
        ),
        "patch": _code_graph_bounded_mapping(
            facts.get("patch_facts"),
            ("claimed_intent_match", "structural_scope_expanded"),
        ),
        "impact": {
            "dependency_crossing_count": len(facts.get("impact_facts", {}).get("dependency_crossings") or [])
            if isinstance(facts.get("impact_facts"), Mapping)
            else 0,
        },
    }
    return {
        "source": "code_graph",
        "reference": reference[:256],
        "snippet_hash": hashlib.sha256(
            json.dumps(bounded_hash_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _code_graph_bounded_mapping(value: Any, keys: Sequence[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {key: value.get(key) for key in keys}


def _bounded_path(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
        return None
    normalized = Path(value.strip()).as_posix()
    return normalized[:256] if normalized else None


def _bounded_reference_fragment(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = re.sub(r"[^A-Za-z0-9_.:/-]+", "-", value.strip()).strip("-")
    return normalized[:96] if normalized else None


def _combined_enrichment_status(
    resolver_enrichment: Mapping[str, Any],
    code_graph_enrichment: Mapping[str, Any],
) -> str:
    statuses: list[str] = []
    if resolver_enrichment.get("resolver_used") is True:
        statuses.append(str(resolver_enrichment.get("enrichment_status")))
    if code_graph_enrichment.get("code_graph_used") is True:
        statuses.append(str(code_graph_enrichment.get("enrichment_status")))
    if not statuses:
        return "not_used"
    return "full" if all(status == "full" for status in statuses) else "partial"


def _normalize_resolver_symbols(value: Any, field_name: str, *, resolved: bool) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field_name} must be an array")
    normalized: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise ValueError(f"{field_name}[{index}] must be an object")
        allowed_keys = {"symbol_name", "symbol_kind", "source_path", "resolution_method", "confidence"}
        extra = sorted(set(item) - allowed_keys)
        if extra:
            raise ValueError(f"{field_name}[{index}] contains unsupported fields: {', '.join(extra)}")
        symbol_name = _required_string(item.get("symbol_name"), f"{field_name}[{index}].symbol_name")
        symbol_kind = _required_string(item.get("symbol_kind"), f"{field_name}[{index}].symbol_kind")
        resolution_method = _required_string(item.get("resolution_method"), f"{field_name}[{index}].resolution_method")
        if resolution_method != "path_metadata":
            raise ValueError(f"{field_name}[{index}].resolution_method must be path_metadata")
        confidence = _required_string(item.get("confidence"), f"{field_name}[{index}].confidence")
        allowed_confidences = {"exact_path_match", "candidate_match"} if resolved else {"unresolved"}
        if confidence not in allowed_confidences:
            raise ValueError(f"{field_name}[{index}].confidence unsupported")
        if resolved:
            _required_string(item.get("source_path"), f"{field_name}[{index}].source_path")
        elif item.get("source_path") is not None:
            raise ValueError(f"{field_name}[{index}].source_path must be null for unresolved symbols")
        normalized.append(
            {
                "symbol_name": symbol_name,
                "symbol_kind": symbol_kind,
                "resolution_method": resolution_method,
                "confidence": confidence,
            }
        )
    return sorted(normalized, key=lambda entry: (entry["symbol_kind"], entry["symbol_name"], entry["confidence"]))


def _primary_reason(drift_items: Sequence[Mapping[str, str]]) -> str:
    blocking = [item for item in drift_items if item["severity"] == "blocking"]
    if not blocking:
        return "Approved expectations align with preview and observed targets."
    return str(blocking[0]["summary"])


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be single-line")
    return value


__all__ = ["build_alignment_record_v1", "validate_alignment_record_v1"]
