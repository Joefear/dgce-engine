"""Optional Defiant Code Graph facts contract for the DGCE function-stub slice."""

from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
import re
from typing import Any, TypedDict

CONTRACT_NAME = "DefiantCodeGraphFacts"
CONTRACT_VERSION = "dcg.facts.v1"
CONTRACT_SCHEMA_ID = "https://defiantinds.com/schemas/dcg-facts-v1.schema.json"
CONTRACT_ARTIFACT_DIR = Path(__file__).resolve().parents[2] / "contracts" / "dcg"
VENDORED_SCHEMA_PATH = CONTRACT_ARTIFACT_DIR / "dcg-facts-v1.schema.json"
VENDORED_CHECKSUM_PATH = CONTRACT_ARTIFACT_DIR / "dcg-facts-v1.sha256"
RFC3339_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


class CodeGraphSpan(TypedDict):
    start_line: int | None
    end_line: int | None


class CodeGraphTarget(TypedDict):
    file_path: str | None
    symbol_id: str | None
    symbol_name: str | None
    symbol_kind: str | None
    span: CodeGraphSpan | None


class CodeGraphIntentFacts(TypedDict):
    structural_scope: str | None
    module_boundary_crossed: bool | None
    trust_boundary_crossed: bool | None
    ownership_boundary_crossed: bool | None
    protected_region_overlap: bool | None
    governed_region_overlap: bool | None
    related_symbols: list[str] | None
    related_files: list[str] | None


class CodeGraphPatchFacts(TypedDict):
    touched_files: list[str] | None
    touched_symbols: list[str] | None
    structural_scope_expanded: bool | None
    module_boundary_crossed: bool | None
    trust_boundary_crossed: bool | None
    ownership_boundary_crossed: bool | None
    protected_region_overlap: bool | None
    governed_region_overlap: bool | None
    claimed_intent_match: bool | None


class CodeGraphInsertionCandidate(TypedDict):
    file_path: str
    symbol_id: str | None
    symbol_name: str | None
    strategy: str
    span: CodeGraphSpan | None


class CodeGraphPlacementFacts(TypedDict):
    insertion_candidates: list[CodeGraphInsertionCandidate]
    generation_collision_detected: bool | None
    recommended_edit_strategy: str


class CodeGraphBlastRadius(TypedDict):
    files: int | None
    symbols: int | None


class CodeGraphImpactFacts(TypedDict):
    blast_radius: CodeGraphBlastRadius | None
    dependency_crossings: list[str]
    dependent_symbols: list[str]


class CodeGraphOwnershipFacts(TypedDict):
    target_ownership: str | None
    touched_ownership_classes: list[str]


class CodeGraphMeta(TypedDict):
    parser_family: str | None
    snapshot_id: str | None
    notes: list[str]


class DefiantCodeGraphFacts(TypedDict):
    contract_name: str
    contract_version: str
    graph_id: str
    workspace_id: str | None
    repo_id: str | None
    language: str | None
    generated_at: str | None
    source: str | None
    target: CodeGraphTarget | None
    intent_facts: CodeGraphIntentFacts | None
    patch_facts: CodeGraphPatchFacts | None
    placement_facts: CodeGraphPlacementFacts | None
    impact_facts: CodeGraphImpactFacts | None
    ownership_facts: CodeGraphOwnershipFacts | None
    meta: CodeGraphMeta | None


def parse_code_graph_context(raw_context: Any) -> DefiantCodeGraphFacts:
    """Validate and normalize optional read-only Defiant Code Graph facts."""
    field_name = "function_stub.code_graph_context"
    payload = _expect_dict(raw_context, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"contract_name", "contract_version", "graph_id"},
        allowed_fields={
            "contract_name",
            "contract_version",
            "graph_id",
            "workspace_id",
            "repo_id",
            "language",
            "generated_at",
            "source",
            "target",
            "intent_facts",
            "patch_facts",
            "placement_facts",
            "impact_facts",
            "ownership_facts",
            "meta",
        },
    )
    return {
        "contract_name": _require_exact_string(payload.get("contract_name"), f"{field_name}.contract_name", CONTRACT_NAME),
        "contract_version": _require_exact_string(
            payload.get("contract_version"),
            f"{field_name}.contract_version",
            CONTRACT_VERSION,
        ),
        "graph_id": _require_min_length_string(payload.get("graph_id"), f"{field_name}.graph_id"),
        "workspace_id": _parse_nullable_string(payload.get("workspace_id"), f"{field_name}.workspace_id"),
        "repo_id": _parse_nullable_string(payload.get("repo_id"), f"{field_name}.repo_id"),
        "language": _parse_nullable_string(payload.get("language"), f"{field_name}.language"),
        "generated_at": _parse_generated_at(payload.get("generated_at"), f"{field_name}.generated_at"),
        "source": _parse_nullable_string(payload.get("source"), f"{field_name}.source"),
        "target": _parse_target(payload.get("target"), f"{field_name}.target"),
        "intent_facts": _parse_intent_facts(payload.get("intent_facts"), f"{field_name}.intent_facts"),
        "patch_facts": _parse_patch_facts(payload.get("patch_facts"), f"{field_name}.patch_facts"),
        "placement_facts": _parse_placement_facts(payload.get("placement_facts"), f"{field_name}.placement_facts"),
        "impact_facts": _parse_impact_facts(payload.get("impact_facts"), f"{field_name}.impact_facts"),
        "ownership_facts": _parse_ownership_facts(payload.get("ownership_facts"), f"{field_name}.ownership_facts"),
        "meta": _parse_meta(payload.get("meta"), f"{field_name}.meta"),
    }


def compute_schema_sha256(schema_path: Path) -> str:
    """Return SHA-256 over raw schema bytes with no normalization."""
    return hashlib.sha256(Path(schema_path).read_bytes()).hexdigest()


def verify_vendored_code_graph_schema_checksum(
    schema_path: Path = VENDORED_SCHEMA_PATH,
    checksum_path: Path = VENDORED_CHECKSUM_PATH,
) -> None:
    """Fail if the vendored schema bytes do not match the vendored checksum."""
    schema_bytes_path = Path(schema_path)
    checksum_bytes_path = Path(checksum_path)
    actual_checksum = compute_schema_sha256(schema_bytes_path)
    expected_checksum = checksum_bytes_path.read_text(encoding="utf-8").strip().split()[0]
    if actual_checksum != expected_checksum:
        raise ValueError("Vendored Defiant Code Graph schema checksum mismatch")


def _parse_target(raw_value: Any, field_name: str) -> CodeGraphTarget | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"file_path", "symbol_id", "symbol_name", "symbol_kind", "span"},
        allowed_fields={"file_path", "symbol_id", "symbol_name", "symbol_kind", "span"},
    )
    return {
        "file_path": _parse_nullable_string(payload.get("file_path"), f"{field_name}.file_path"),
        "symbol_id": _parse_nullable_string(payload.get("symbol_id"), f"{field_name}.symbol_id"),
        "symbol_name": _parse_nullable_string(payload.get("symbol_name"), f"{field_name}.symbol_name"),
        "symbol_kind": _parse_nullable_string(payload.get("symbol_kind"), f"{field_name}.symbol_kind"),
        "span": _parse_span(payload.get("span"), f"{field_name}.span"),
    }


def _parse_intent_facts(raw_value: Any, field_name: str) -> CodeGraphIntentFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={
            "structural_scope",
            "module_boundary_crossed",
            "trust_boundary_crossed",
            "ownership_boundary_crossed",
            "protected_region_overlap",
            "governed_region_overlap",
            "related_symbols",
            "related_files",
        },
        allowed_fields={
            "structural_scope",
            "module_boundary_crossed",
            "trust_boundary_crossed",
            "ownership_boundary_crossed",
            "protected_region_overlap",
            "governed_region_overlap",
            "related_symbols",
            "related_files",
        },
    )
    return {
        "structural_scope": _parse_nullable_enum(
            payload.get("structural_scope"),
            f"{field_name}.structural_scope",
            {"symbol", "file", "module", "package", "repo"},
        ),
        "module_boundary_crossed": _parse_nullable_bool(
            payload.get("module_boundary_crossed"),
            f"{field_name}.module_boundary_crossed",
        ),
        "trust_boundary_crossed": _parse_nullable_bool(
            payload.get("trust_boundary_crossed"),
            f"{field_name}.trust_boundary_crossed",
        ),
        "ownership_boundary_crossed": _parse_nullable_bool(
            payload.get("ownership_boundary_crossed"),
            f"{field_name}.ownership_boundary_crossed",
        ),
        "protected_region_overlap": _parse_nullable_bool(
            payload.get("protected_region_overlap"),
            f"{field_name}.protected_region_overlap",
        ),
        "governed_region_overlap": _parse_nullable_bool(
            payload.get("governed_region_overlap"),
            f"{field_name}.governed_region_overlap",
        ),
        "related_symbols": _parse_nullable_string_list(payload.get("related_symbols"), f"{field_name}.related_symbols"),
        "related_files": _parse_nullable_string_list(payload.get("related_files"), f"{field_name}.related_files"),
    }


def _parse_patch_facts(raw_value: Any, field_name: str) -> CodeGraphPatchFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={
            "touched_files",
            "touched_symbols",
            "structural_scope_expanded",
            "module_boundary_crossed",
            "trust_boundary_crossed",
            "ownership_boundary_crossed",
            "protected_region_overlap",
            "governed_region_overlap",
            "claimed_intent_match",
        },
        allowed_fields={
            "touched_files",
            "touched_symbols",
            "structural_scope_expanded",
            "module_boundary_crossed",
            "trust_boundary_crossed",
            "ownership_boundary_crossed",
            "protected_region_overlap",
            "governed_region_overlap",
            "claimed_intent_match",
        },
    )
    return {
        "touched_files": _parse_nullable_string_list(payload.get("touched_files"), f"{field_name}.touched_files"),
        "touched_symbols": _parse_nullable_string_list(payload.get("touched_symbols"), f"{field_name}.touched_symbols"),
        "structural_scope_expanded": _parse_nullable_bool(
            payload.get("structural_scope_expanded"),
            f"{field_name}.structural_scope_expanded",
        ),
        "module_boundary_crossed": _parse_nullable_bool(
            payload.get("module_boundary_crossed"),
            f"{field_name}.module_boundary_crossed",
        ),
        "trust_boundary_crossed": _parse_nullable_bool(
            payload.get("trust_boundary_crossed"),
            f"{field_name}.trust_boundary_crossed",
        ),
        "ownership_boundary_crossed": _parse_nullable_bool(
            payload.get("ownership_boundary_crossed"),
            f"{field_name}.ownership_boundary_crossed",
        ),
        "protected_region_overlap": _parse_nullable_bool(
            payload.get("protected_region_overlap"),
            f"{field_name}.protected_region_overlap",
        ),
        "governed_region_overlap": _parse_nullable_bool(
            payload.get("governed_region_overlap"),
            f"{field_name}.governed_region_overlap",
        ),
        "claimed_intent_match": _parse_nullable_bool(
            payload.get("claimed_intent_match"),
            f"{field_name}.claimed_intent_match",
        ),
    }


def _parse_placement_facts(raw_value: Any, field_name: str) -> CodeGraphPlacementFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"insertion_candidates", "generation_collision_detected", "recommended_edit_strategy"},
        allowed_fields={"insertion_candidates", "generation_collision_detected", "recommended_edit_strategy"},
    )
    return {
        "insertion_candidates": _parse_insertion_candidates(
            payload.get("insertion_candidates"),
            f"{field_name}.insertion_candidates",
        ),
        "generation_collision_detected": _parse_nullable_bool(
            payload.get("generation_collision_detected"),
            f"{field_name}.generation_collision_detected",
        ),
        "recommended_edit_strategy": _parse_required_enum(
            payload.get("recommended_edit_strategy"),
            f"{field_name}.recommended_edit_strategy",
            {"surgical_edit", "bounded_insert", "new_file", "rewrite_small_region", "unknown"},
        ),
    }


def _parse_impact_facts(raw_value: Any, field_name: str) -> CodeGraphImpactFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"blast_radius", "dependency_crossings", "dependent_symbols"},
        allowed_fields={"blast_radius", "dependency_crossings", "dependent_symbols"},
    )
    return {
        "blast_radius": _parse_blast_radius(payload.get("blast_radius"), f"{field_name}.blast_radius"),
        "dependency_crossings": _parse_required_string_list(
            payload.get("dependency_crossings"),
            f"{field_name}.dependency_crossings",
        ),
        "dependent_symbols": _parse_required_string_list(
            payload.get("dependent_symbols"),
            f"{field_name}.dependent_symbols",
        ),
    }


def _parse_ownership_facts(raw_value: Any, field_name: str) -> CodeGraphOwnershipFacts | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"target_ownership", "touched_ownership_classes"},
        allowed_fields={"target_ownership", "touched_ownership_classes"},
    )
    return {
        "target_ownership": _parse_nullable_enum(
            payload.get("target_ownership"),
            f"{field_name}.target_ownership",
            {"manual", "generated", "locked", "governed", "critical", "policy_sensitive", "unknown"},
        ),
        "touched_ownership_classes": _parse_required_enum_list(
            payload.get("touched_ownership_classes"),
            f"{field_name}.touched_ownership_classes",
            {"manual", "generated", "locked", "governed", "critical", "policy_sensitive", "unknown"},
        ),
    }


def _parse_meta(raw_value: Any, field_name: str) -> CodeGraphMeta | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"parser_family", "snapshot_id", "notes"},
        allowed_fields={"parser_family", "snapshot_id", "notes"},
    )
    return {
        "parser_family": _parse_nullable_string(payload.get("parser_family"), f"{field_name}.parser_family"),
        "snapshot_id": _parse_nullable_string(payload.get("snapshot_id"), f"{field_name}.snapshot_id"),
        "notes": _parse_required_string_list(payload.get("notes"), f"{field_name}.notes"),
    }


def _parse_insertion_candidates(raw_value: Any, field_name: str) -> list[CodeGraphInsertionCandidate]:
    items = _expect_list(raw_value, field_name)
    normalized: list[CodeGraphInsertionCandidate] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        payload = _expect_dict(item, item_field)
        _validate_fields(
            payload,
            item_field,
            required_fields={"file_path", "symbol_id", "symbol_name", "strategy", "span"},
            allowed_fields={"file_path", "symbol_id", "symbol_name", "strategy", "span"},
        )
        normalized.append(
            {
                "file_path": _require_min_length_string(payload.get("file_path"), f"{item_field}.file_path"),
                "symbol_id": _parse_nullable_string(payload.get("symbol_id"), f"{item_field}.symbol_id"),
                "symbol_name": _parse_nullable_string(payload.get("symbol_name"), f"{item_field}.symbol_name"),
                "strategy": _parse_required_enum(
                    payload.get("strategy"),
                    f"{item_field}.strategy",
                    {"append_after_symbol", "insert_before_symbol", "inside_symbol", "new_file", "unknown"},
                ),
                "span": _parse_span(payload.get("span"), f"{item_field}.span"),
            }
        )
    return normalized


def _parse_blast_radius(raw_value: Any, field_name: str) -> CodeGraphBlastRadius | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"files", "symbols"},
        allowed_fields={"files", "symbols"},
    )
    return {
        "files": _parse_nullable_non_negative_int(payload.get("files"), f"{field_name}.files"),
        "symbols": _parse_nullable_non_negative_int(payload.get("symbols"), f"{field_name}.symbols"),
    }


def _parse_span(raw_value: Any, field_name: str) -> CodeGraphSpan | None:
    if raw_value is None:
        return None
    payload = _expect_dict(raw_value, field_name)
    _validate_fields(
        payload,
        field_name,
        required_fields={"start_line", "end_line"},
        allowed_fields={"start_line", "end_line"},
    )
    start_line = _parse_nullable_positive_int(payload.get("start_line"), f"{field_name}.start_line")
    end_line = _parse_nullable_positive_int(payload.get("end_line"), f"{field_name}.end_line")
    if start_line is not None and end_line is not None and end_line < start_line:
        raise ValueError(f"{field_name}.end_line must be greater than or equal to {field_name}.start_line")
    return {
        "start_line": start_line,
        "end_line": end_line,
    }


def _parse_generated_at(raw_value: Any, field_name: str) -> str | None:
    if raw_value is None:
        return None
    normalized = _require_string(raw_value, field_name)
    if not RFC3339_PATTERN.match(normalized):
        raise ValueError(f"{field_name} must be RFC 3339 when provided")
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be RFC 3339 when provided") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must be RFC 3339 when provided")
    return normalized


def _expect_dict(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dict")
    return dict(value)


def _expect_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return list(value)


def _validate_fields(
    payload: dict[str, Any],
    field_name: str,
    *,
    required_fields: set[str],
    allowed_fields: set[str],
) -> None:
    missing_fields = sorted(field for field in required_fields if field not in payload)
    if missing_fields:
        raise ValueError(f"{field_name} must include all required contract fields")
    extra_fields = sorted(field for field in payload if field not in allowed_fields)
    if extra_fields:
        raise ValueError(f"{field_name} must not include extra fields")


def _require_exact_string(value: Any, field_name: str, expected: str) -> str:
    normalized = _require_string(value, field_name)
    if normalized != expected:
        raise ValueError(f"{field_name} must be {expected}")
    return normalized


def _require_min_length_string(value: Any, field_name: str) -> str:
    normalized = _require_string(value, field_name)
    if not normalized:
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _require_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    return value


def _parse_nullable_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field_name)


def _parse_nullable_bool(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a bool or null")
    return value


def _parse_nullable_enum(value: Any, field_name: str, allowed_values: set[str]) -> str | None:
    if value is None:
        return None
    normalized = _require_string(value, field_name)
    if normalized not in allowed_values:
        raise ValueError(f"{field_name} must be one of the allowed contract values")
    return normalized


def _parse_required_enum(value: Any, field_name: str, allowed_values: set[str]) -> str:
    normalized = _require_string(value, field_name)
    if normalized not in allowed_values:
        raise ValueError(f"{field_name} must be one of the allowed contract values")
    return normalized


def _parse_nullable_string_list(value: Any, field_name: str) -> list[str] | None:
    if value is None:
        return None
    return _parse_required_string_list(value, field_name)


def _parse_required_string_list(value: Any, field_name: str) -> list[str]:
    items = _expect_list(value, field_name)
    normalized: list[str] = []
    for index, item in enumerate(items):
        normalized.append(_require_string(item, f"{field_name}[{index}]"))
    return normalized


def _parse_required_enum_list(value: Any, field_name: str, allowed_values: set[str]) -> list[str]:
    items = _expect_list(value, field_name)
    normalized: list[str] = []
    for index, item in enumerate(items):
        item_field = f"{field_name}[{index}]"
        normalized_item = _require_string(item, item_field)
        if normalized_item not in allowed_values:
            raise ValueError(f"{item_field} must be one of the allowed contract values")
        normalized.append(normalized_item)
    return normalized


def _parse_nullable_positive_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer or null")
    if value < 1:
        raise ValueError(f"{field_name} must be greater than or equal to 1")
    return int(value)


def _parse_nullable_non_negative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer or null")
    if value < 0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return int(value)
