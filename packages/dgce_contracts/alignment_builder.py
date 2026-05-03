"""Deterministic Stage 7 alignment_record.v1 builder.

This module builds only the locked alignment_record.v1 contract object. It does
not wire into DGCE lifecycle stages, execute Stage 8, read project files, call
LLMs, or integrate resolver/code-graph systems.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from pathlib import Path
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
    drift_items = _sort_drift_items(
        _build_drift_items(approved_targets, preview_targets, observed_targets, available_targets)
        + resolver_enrichment["drift_items"]
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
            resolver_enrichment["evidence"],
        ),
        "alignment_enrichment": {
            "code_graph_used": False,
            "resolver_used": resolver_enrichment["resolver_used"],
            "enrichment_status": resolver_enrichment["enrichment_status"],
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


def _build_evidence(
    approved_targets: Mapping[str, dict[str, Any]],
    preview_targets: Mapping[str, dict[str, Any]],
    observed_targets: Mapping[str, dict[str, Any]],
    resolver_evidence: Sequence[Mapping[str, str]] | None = None,
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
    for evidence in resolver_evidence or []:
        source = _required_string(evidence.get("source"), "resolver_evidence.source")
        reference = _required_string(evidence.get("reference"), "resolver_evidence.reference")
        snippet_hash = evidence.get("snippet_hash")
        normalized = {
            "source": source,
            "reference": reference,
        }
        if snippet_hash is not None:
            normalized["snippet_hash"] = _required_string(snippet_hash, "resolver_evidence.snippet_hash")
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
