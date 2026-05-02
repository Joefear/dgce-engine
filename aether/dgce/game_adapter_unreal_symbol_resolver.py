"""Path-metadata Light Unreal Symbol Resolver v0.1 for Game Adapter.

This module consumes already-built manifest and candidate-index artifacts and
matches requested symbols against bounded path metadata only. It does not read
project files, parse Unreal content, inspect Blueprint graphs, or write any
runtime artifacts.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from aether.dgce.decompose import compute_json_payload_fingerprint
from aether.dgce.game_adapter_unreal_manifest import validate_unreal_project_structure_manifest
from aether.dgce.game_adapter_unreal_symbol_candidates import validate_unreal_symbol_candidate_index
from aether.dgce.game_adapter_unreal_symbol_resolver_contract import (
    ADAPTER,
    CONTRACT_NAME,
    CONTRACT_VERSION,
    DOMAIN,
    OUTPUT_ARTIFACT_TYPE,
    RESOLUTION_METHOD,
    UNRESOLVED_CONFIDENCE,
    validate_resolver_input_contract,
    validate_resolver_output_contract,
)


_CANDIDATE_KIND_ORDER = {
    "blueprint_asset": 0,
    "cpp_header": 1,
    "cpp_source": 2,
    "source_module": 3,
    "uproject": 4,
    "config_directory": 5,
}
_REQUEST_SEPARATOR_ALIASES = (".", "#", ":")


def resolve_unreal_symbols_from_path_metadata(
    resolver_input: Mapping[str, Any],
    manifest_payload: Mapping[str, Any],
    candidate_index_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve requested Unreal symbols from manifest/candidate path metadata only."""
    validate_resolver_input_contract(resolver_input)
    validate_unreal_project_structure_manifest(dict(manifest_payload))
    validate_unreal_symbol_candidate_index(dict(candidate_index_payload))
    _validate_source_fingerprints(resolver_input, manifest_payload, candidate_index_payload)

    allowed_symbol_kinds = tuple(resolver_input["allowed_symbol_kinds"])
    candidates = _candidate_records(candidate_index_payload, allowed_symbol_kinds)
    resolved_symbols: list[dict[str, Any]] = []
    unresolved_symbols: list[dict[str, Any]] = []

    if "requested_targets" in resolver_input:
        requests = _target_requests(resolver_input["requested_targets"])
    else:
        requests = _symbol_requests(resolver_input["requested_symbols"], allowed_symbol_kinds)

    for request in requests:
        match = _best_match(request, candidates)
        if match is None:
            unresolved_symbols.append(_unresolved_symbol(request))
        else:
            resolved_symbols.append(_resolved_symbol(request, match))

    resolved_symbols = sorted(
        resolved_symbols,
        key=lambda entry: (entry["symbol_kind"], entry["symbol_name"], entry["source_path"]),
    )
    unresolved_symbols = sorted(
        unresolved_symbols,
        key=lambda entry: (entry["symbol_kind"], entry["symbol_name"], ""),
    )
    resolution_status = _resolution_status(resolved_symbols, unresolved_symbols)
    payload = {
        "artifact_type": OUTPUT_ARTIFACT_TYPE,
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "adapter": ADAPTER,
        "domain": DOMAIN,
        "source_input_fingerprint": compute_json_payload_fingerprint(dict(resolver_input)),
        "resolved_symbols": resolved_symbols,
        "unresolved_symbols": unresolved_symbols,
        "resolution_status": resolution_status,
        "integration_points": _integration_points(
            resolver_input["stage_usage"],
            symbol_metadata_available=bool(resolved_symbols),
        ),
    }
    payload["artifact_fingerprint"] = compute_json_payload_fingerprint(payload)
    validate_resolver_output_contract(payload)
    return payload


def _validate_source_fingerprints(
    resolver_input: Mapping[str, Any],
    manifest_payload: Mapping[str, Any],
    candidate_index_payload: Mapping[str, Any],
) -> None:
    manifest_fingerprint = manifest_payload.get("artifact_fingerprint")
    candidate_index_fingerprint = candidate_index_payload.get("artifact_fingerprint")
    if resolver_input.get("source_manifest_fingerprint") != manifest_fingerprint:
        raise ValueError("source_manifest_fingerprint mismatch")
    if resolver_input.get("source_candidate_index_fingerprint") != candidate_index_fingerprint:
        raise ValueError("source_candidate_index_fingerprint mismatch")
    if candidate_index_payload.get("source_manifest_fingerprint") != manifest_fingerprint:
        raise ValueError("candidate index source_manifest_fingerprint mismatch")


def _candidate_records(candidate_index_payload: Mapping[str, Any], allowed_symbol_kinds: tuple[str, ...]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for candidate in candidate_index_payload["candidates"]:
        candidate_kinds = [
            symbol_kind
            for symbol_kind in _candidate_symbol_kinds(candidate)
            if symbol_kind in allowed_symbol_kinds
        ]
        if not candidate_kinds:
            continue
        records.append(
            {
                "candidate_name": candidate["candidate_name"],
                "candidate_kind": candidate["candidate_kind"],
                "source_path": candidate["source_path"],
                "symbol_kinds": tuple(candidate_kinds),
                "path_aliases": _path_aliases(candidate["source_path"]),
            }
        )
    return sorted(
        records,
        key=lambda record: (
            _CANDIDATE_KIND_ORDER.get(record["candidate_kind"], 99),
            record["source_path"],
            record["candidate_name"],
        ),
    )


def _candidate_symbol_kinds(candidate: Mapping[str, Any]) -> tuple[str, ...]:
    candidate_kind = candidate["candidate_kind"]
    candidate_name = candidate["candidate_name"]
    if candidate_kind == "blueprint_asset":
        return ("BlueprintClass", "Asset")
    if candidate_kind in {"cpp_header", "cpp_source"}:
        if candidate_name.endswith("Component"):
            return ("CppClass", "ActorComponent")
        return ("CppClass",)
    if candidate_kind in {"source_module", "uproject", "config_directory"}:
        return ("Asset",)
    return ()


def _symbol_requests(requested_symbols: list[str], allowed_symbol_kinds: tuple[str, ...]) -> list[dict[str, Any]]:
    fallback_kind = allowed_symbol_kinds[0]
    return [
        {
            "request_type": "symbol",
            "symbol_name": symbol_name,
            "symbol_kind": fallback_kind,
            "allowed_symbol_kinds": allowed_symbol_kinds,
            "name_aliases": {symbol_name},
            "path_aliases": set(),
        }
        for symbol_name in requested_symbols
    ]


def _target_requests(requested_targets: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    requests: list[dict[str, Any]] = []
    for target in requested_targets:
        target_kind = target["target_kind"]
        target_id = target["target_id"]
        target_path = target["target_path"]
        requests.append(
            {
                "request_type": "target",
                "symbol_name": _target_symbol_name(target_id, target_path),
                "symbol_kind": target_kind,
                "allowed_symbol_kinds": (target_kind,),
                "name_aliases": _target_name_aliases(target_id, target_path),
                "path_aliases": _path_aliases(target_path),
            }
        )
    return requests


def _best_match(request: Mapping[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    scored_matches = []
    for candidate in candidates:
        symbol_kind = _matched_symbol_kind(request, candidate)
        if symbol_kind is None:
            continue
        path_exact = bool(request["path_aliases"] & candidate["path_aliases"])
        name_match = candidate["candidate_name"] in request["name_aliases"]
        if not path_exact and not name_match:
            continue
        confidence = "exact_path_match" if path_exact else "candidate_match"
        scored_matches.append(
            (
                0 if path_exact else 1,
                _CANDIDATE_KIND_ORDER.get(candidate["candidate_kind"], 99),
                candidate["source_path"],
                candidate["candidate_name"],
                {
                    "candidate_name": candidate["candidate_name"],
                    "symbol_kind": symbol_kind,
                    "source_path": candidate["source_path"],
                    "confidence": confidence,
                },
            )
        )
    if not scored_matches:
        return None
    return sorted(scored_matches, key=lambda item: item[:4])[0][4]


def _matched_symbol_kind(request: Mapping[str, Any], candidate: Mapping[str, Any]) -> str | None:
    for symbol_kind in candidate["symbol_kinds"]:
        if symbol_kind in request["allowed_symbol_kinds"]:
            return symbol_kind
    return None


def _resolved_symbol(request: Mapping[str, Any], match: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol_name": match["candidate_name"],
        "symbol_kind": match["symbol_kind"],
        "source_path": match["source_path"],
        "resolution_method": RESOLUTION_METHOD,
        "confidence": match["confidence"],
    }


def _unresolved_symbol(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol_name": request["symbol_name"],
        "symbol_kind": request["symbol_kind"],
        "source_path": None,
        "resolution_method": RESOLUTION_METHOD,
        "confidence": UNRESOLVED_CONFIDENCE,
    }


def _resolution_status(resolved_symbols: list[dict[str, Any]], unresolved_symbols: list[dict[str, Any]]) -> str:
    if resolved_symbols and not unresolved_symbols:
        return "resolved"
    if resolved_symbols and unresolved_symbols:
        return "partially_resolved"
    return "unresolved"


def _integration_points(stage_usage: str, *, symbol_metadata_available: bool) -> dict[str, dict[str, Any]]:
    return {
        "stage2_preview_context": {
            "stage": "Stage2Preview",
            "context_kind": "resolver_metadata",
            "symbol_metadata_available": symbol_metadata_available and stage_usage in {"Stage2Preview", "both"},
        },
        "stage7_alignment_context": {
            "stage": "Stage7Alignment",
            "context_kind": "resolver_metadata",
            "symbol_metadata_available": symbol_metadata_available and stage_usage in {"Stage7Alignment", "both"},
        },
    }


def _target_symbol_name(target_id: str, target_path: str) -> str:
    aliases = _target_name_aliases(target_id, target_path)
    for alias in (target_id, *_split_aliases(target_id), Path(target_path).stem, Path(target_path).name):
        if alias in aliases:
            return alias
    return sorted(aliases)[0]


def _target_name_aliases(target_id: str, target_path: str) -> set[str]:
    aliases = {target_id}
    aliases.update(_split_aliases(target_id))
    path = Path(target_path)
    aliases.add(path.stem)
    aliases.add(path.name)
    return {alias for alias in aliases if alias}


def _split_aliases(value: str) -> set[str]:
    aliases = {value}
    for separator in _REQUEST_SEPARATOR_ALIASES:
        if separator in value:
            aliases.add(value.rsplit(separator, 1)[-1])
    return aliases


def _path_aliases(path_text: str) -> set[str]:
    path = Path(path_text)
    without_suffix = path.with_suffix("").as_posix() if path.suffix else path.as_posix()
    aliases = {path_text, without_suffix}
    if path_text.startswith("/"):
        aliases.add(path_text.rstrip("/"))
    else:
        aliases.add(f"/{without_suffix}")
    if path_text.startswith("Content/"):
        content_relative = Path(path_text.removeprefix("Content/"))
        game_path = content_relative.with_suffix("").as_posix() if content_relative.suffix else content_relative.as_posix()
        aliases.add(f"/Game/{game_path}")
    return {alias for alias in aliases if alias}


__all__ = ["resolve_unreal_symbols_from_path_metadata"]
