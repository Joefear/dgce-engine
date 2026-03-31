"""Deterministic DGCE file-plan generation from structured task outputs."""

import json
import re
from typing import TYPE_CHECKING, Any, Dict, List

from pydantic import BaseModel

if TYPE_CHECKING:
    from aether.dgce.decompose import ResponseEnvelope


class FilePlan(BaseModel):
    """A deterministic plan of project files derived from DGCE artifacts."""

    project_name: str
    files: List[Dict[str, Any]]


_MAX_DATA_MODEL_BASENAME_LENGTH = 64
_NAME_STOPWORDS = {
    "a",
    "an",
    "and",
    "by",
    "description",
    "field",
    "fields",
    "for",
    "from",
    "in",
    "json",
    "managing",
    "name",
    "names",
    "of",
    "or",
    "entities",
    "entity",
    "path",
    "required",
    "storage",
    "the",
    "to",
    "type",
    "types",
    "value",
    "values",
    "with",
    "dgce",
}
_ENTITY_SUFFIXES = ("artifact", "record", "input", "output", "gate", "stamp", "model", "entity")
_INTERFACE_SUFFIXES = ("service", "api", "gateway", "client", "interface")
_DGCE_CORE_API_INTERFACE_METHODS = {
    "PreviewService": ("generate_preview", "get_preview"),
    "ReviewService": ("submit_review", "get_review"),
    "ApprovalService": ("approve_section", "get_approval"),
    "PreflightService": ("run_preflight", "get_preflight"),
    "GateService": ("evaluate_gate",),
    "AlignmentService": ("validate_alignment",),
    "ExecutionService": ("execute_section", "get_execution"),
    "StatusService": ("get_section_status",),
}
_SEMANTIC_VOCABULARY = (
    "alignment",
    "api",
    "approval",
    "artifact",
    "client",
    "data",
    "entity",
    "execution",
    "gate",
    "governance",
    "input",
    "interface",
    "item",
    "method",
    "model",
    "output",
    "preflight",
    "preview",
    "record",
    "review",
    "section",
    "service",
    "stamp",
    "status",
)


def build_file_plan(responses: List["ResponseEnvelope"]) -> FilePlan:
    """Build a flat, deterministic file plan from structured DGCE responses."""
    files: list[dict[str, Any]] = []

    for response in responses:
        payload = _structured_payload(response)
        if payload is None:
            continue

        if response.task_type == "system_breakdown":
            files.extend(_system_breakdown_files(payload))
        elif response.task_type == "data_model":
            structured_payload = getattr(response, "structured_content", None)
            for entity in payload["entities"]:
                entity_label = _normalize_entity_label(_data_model_entity_label(entity))
                entity_name = _data_model_slug(entity_label)
                file_entry = {
                    "path": f"models/{entity_name}.py",
                    "purpose": f"Data model for {entity_label}",
                    "source": "data_model",
                }
                if isinstance(structured_payload, dict):
                    file_entry.update(_data_model_file_metadata(structured_payload, entity_label))
                files.append(
                    file_entry
                )
        elif response.task_type == "api_surface":
            structured_payload = getattr(response, "structured_content", None)
            for interface in payload["interfaces"]:
                interface_label = _normalize_interface_label(str(interface))
                interface_name = _slug(interface_label)
                file_entry = {
                    "path": f"api/{interface_name}.py",
                    "purpose": f"API interface for {interface_label}",
                    "source": "api_surface",
                }
                if isinstance(structured_payload, dict):
                    file_entry.update(_api_surface_file_metadata(structured_payload, interface_label))
                files.append(
                    file_entry
                )

    deduped = {
        (file_entry["path"], file_entry["source"]): file_entry
        for file_entry in files
    }
    ordered_files = sorted(deduped.values(), key=lambda entry: (entry["path"], entry["source"]))
    return FilePlan(project_name="DGCE", files=ordered_files)


def _structured_payload(response: "ResponseEnvelope") -> dict[str, Any] | None:
    """Parse and validate the structured JSON payload for one DGCE response."""
    if response.status == "error" or not response.output.strip():
        return None

    required_keys = _required_keys_for_task(response.task_type)
    if not required_keys:
        return None

    payload = getattr(response, "structured_content", None)
    if not isinstance(payload, dict):
        try:
            payload = json.loads(response.output)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    if not isinstance(payload, dict):
        return None
    if response.task_type == "system_breakdown":
        if not _is_supported_system_breakdown_payload(payload):
            return None
        return payload
    if any(key not in payload for key in required_keys):
        return None
    return payload


def _required_keys_for_task(task_type: str) -> list[str]:
    """Return the static required keys for DGCE build-oriented task types."""
    if task_type == "system_breakdown":
        return [
            "modules",
            "build_graph",
            "tests",
        ]
    if task_type == "data_model":
        return [
            "entities",
            "fields",
            "relationships",
            "validation_rules",
        ]
    if task_type == "api_surface":
        return [
            "interfaces",
            "methods",
            "inputs",
            "outputs",
            "error_cases",
        ]
    return []


def _slug(value: str) -> str:
    """Create a deterministic file-safe token."""
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "item"


def _data_model_slug(value: str) -> str:
    """Create a bounded Windows-safe token for data-model entity object names."""
    collapsed = _slug(value)
    return collapsed[:_MAX_DATA_MODEL_BASENAME_LENGTH].rstrip("_") or "item"


def _data_model_entity_label(entity: Any) -> str:
    """Return the stable entity name used for DGCE data-model file paths."""
    if isinstance(entity, dict):
        name = entity.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    if isinstance(entity, str) and entity.strip():
        return entity.strip()
    return "item"


def _data_model_file_metadata(payload: dict[str, Any], entity_label: str) -> dict[str, Any]:
    """Return implementation-ready model metadata for one validated data-model entity."""
    entity_payload = _data_model_entity_payload(payload, entity_label)
    if not entity_payload:
        return {}

    entity_names = [
        _normalize_entity_label(_data_model_entity_label(entity))
        for entity in payload.get("entities", [])
        if isinstance(entity, (dict, str))
    ]
    metadata = {
        "entity_schema": entity_payload,
        "entity_relationships": _entity_relationships(payload, entity_label),
        "known_entity_names": sorted({name for name in entity_names if name}),
    }
    return metadata


def _data_model_entity_payload(payload: dict[str, Any], entity_label: str) -> dict[str, Any] | None:
    """Return the normalized entity payload for one class-like entity name."""
    for entity in payload.get("entities", []):
        if not isinstance(entity, dict):
            continue
        normalized_name = _normalize_entity_label(_data_model_entity_label(entity))
        if normalized_name != entity_label:
            continue

        normalized_entity = dict(entity)
        normalized_entity["name"] = entity_label
        normalized_entity["identity_keys"] = _string_list(entity.get("identity_keys"))
        normalized_entity["invariants"] = _string_list(entity.get("invariants"))
        normalized_entity["fields"] = _normalize_entity_fields(entity.get("fields", []))
        return normalized_entity
    return None


def _normalize_entity_fields(fields: Any) -> list[dict[str, Any]]:
    """Return deterministically normalized entity fields for backend-ready model generation."""
    if not isinstance(fields, list):
        return []

    normalized_fields: list[dict[str, Any]] = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = field.get("name")
        if not isinstance(name, str) or not name.strip():
            continue

        normalized_field = dict(field)
        normalized_field["name"] = name.strip()
        normalized_field["type"] = str(field.get("type", "Any")).strip() or "Any"
        normalized_field["required"] = bool(field.get("required", False))
        normalized_fields.append(normalized_field)

    return sorted(
        normalized_fields,
        key=lambda item: (
            str(item.get("order", "")),
            str(item.get("name", "")),
            str(item.get("type", "")),
        ),
    )


def _entity_relationships(payload: dict[str, Any], entity_label: str) -> list[dict[str, str]]:
    """Return normalized outbound relationships for one entity."""
    relationships: list[dict[str, str]] = []
    for relationship in payload.get("relationships", []):
        parsed = _parse_relationship(relationship)
        if parsed is None or parsed["from_entity"] != entity_label:
            continue
        relationships.append(parsed)

    return sorted(
        relationships,
        key=lambda item: (
            item["to_entity"],
            item["relationship_type"],
        ),
    )


def _parse_relationship(value: Any) -> dict[str, str] | None:
    """Normalize one relationship entry into a deterministic object shape."""
    if isinstance(value, dict):
        raw_from_entity = value.get("from_entity")
        raw_to_entity = value.get("to_entity")
        if not isinstance(raw_from_entity, str) or not raw_from_entity.strip():
            return None
        if not isinstance(raw_to_entity, str) or not raw_to_entity.strip():
            return None
        from_entity = _normalize_entity_label(raw_from_entity.strip())
        to_entity = _normalize_entity_label(raw_to_entity.strip())
        if not from_entity or not to_entity:
            return None
        relationship_type = str(value.get("relationship_type", value.get("type", "references"))).strip() or "references"
        return {
            "from_entity": from_entity,
            "to_entity": to_entity,
            "relationship_type": relationship_type,
        }

    if isinstance(value, str):
        match = re.match(r"^\s*([A-Za-z0-9_]+)\s*->\s*([A-Za-z0-9_]+)\s*$", value)
        if not match:
            return None
        return {
            "from_entity": _normalize_entity_label(match.group(1)),
            "to_entity": _normalize_entity_label(match.group(2)),
            "relationship_type": "references",
        }

    return None


def _string_list(value: Any) -> list[str]:
    """Return a deterministic list of non-empty strings."""
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if isinstance(item, str) and item.strip()]


def _normalize_entity_label(value: str) -> str:
    """Return a concise class-like entity label derived only from the entity name."""
    if _is_clean_class_name(value, _ENTITY_SUFFIXES):
        return value.strip()
    return _normalize_class_like_label(value, suffix_mode="first", suffixes=_ENTITY_SUFFIXES, default="Item")


def _normalize_interface_label(value: str) -> str:
    """Return a concise class-like interface label derived only from the interface name."""
    if _is_clean_class_name(value, _INTERFACE_SUFFIXES):
        return value.strip()
    return _normalize_class_like_label(value, suffix_mode="last", suffixes=_INTERFACE_SUFFIXES, default="Interface")


def _api_surface_file_metadata(payload: dict[str, Any], interface_label: str) -> dict[str, Any]:
    """Return implementation-ready interface metadata for one validated api-surface interface."""
    method_names = _DGCE_CORE_API_INTERFACE_METHODS.get(interface_label, ())
    methods_payload = payload.get("methods", {})
    inputs_payload = payload.get("inputs", {})
    outputs_payload = payload.get("outputs", {})
    error_cases_payload = payload.get("error_cases", {})
    if not isinstance(methods_payload, dict):
        return {}

    interface_methods: list[dict[str, Any]] = []
    for method_name in method_names:
        method_payload = methods_payload.get(method_name)
        if not isinstance(method_payload, dict):
            continue
        interface_methods.append(
            {
                "name": method_name,
                "method": method_payload.get("method"),
                "path": method_payload.get("path"),
                "input": inputs_payload.get(method_name, method_payload.get("input", {})) if isinstance(inputs_payload, dict) else method_payload.get("input", {}),
                "output": outputs_payload.get(method_name, method_payload.get("output", {})) if isinstance(outputs_payload, dict) else method_payload.get("output", {}),
                "error_cases": error_cases_payload.get(method_name, method_payload.get("error_cases", [])) if isinstance(error_cases_payload, dict) else method_payload.get("error_cases", []),
            }
        )

    if not interface_methods:
        return {}

    return {
        "interface_schema": {
            "name": interface_label,
            "methods": sorted(interface_methods, key=lambda item: str(item.get("name", ""))),
        }
    }


def _normalize_class_like_label(value: str, *, suffix_mode: str, suffixes: tuple[str, ...], default: str) -> str:
    """Normalize noisy identifiers into short PascalCase labels."""
    tokens = _semantic_tokens(value)
    if not tokens:
        return default

    chosen_tokens = tokens
    suffix_token: str | None = None
    if suffix_mode == "last":
        for preferred_suffix in suffixes:
            if preferred_suffix in tokens:
                suffix_token = preferred_suffix
                suffix_index = max(index for index, token in enumerate(tokens) if token == preferred_suffix)
                chosen_tokens = tokens[max(0, suffix_index - 2) : suffix_index + 1]
                break
    else:
        for index, token in enumerate(tokens):
            if token in suffixes:
                suffix_token = token
                chosen_tokens = tokens[: index + 1]
                break

    base_tokens = [token for token in chosen_tokens if token not in suffixes]
    if suffix_token:
        base_tokens = base_tokens[:2]
        final_tokens = [*base_tokens, suffix_token]
    else:
        final_tokens = tokens[:3]

    return "".join(token.capitalize() for token in final_tokens) or default


def _semantic_tokens(value: str) -> list[str]:
    """Extract approximate semantic identifier tokens from a noisy string."""
    parts = _split_identifier_tokens(value)
    filtered = [
        part.lower()
        for part in parts
        if part and part.lower() not in _NAME_STOPWORDS and not part.isdigit()
    ]
    if not filtered:
        filtered = [part.lower() for part in parts if part and not part.isdigit()]

    deduped: list[str] = []
    for token in filtered:
        if not deduped or deduped[-1] != token:
            deduped.append(token)
    return deduped


def _split_identifier_tokens(value: str) -> list[str]:
    """Split mixed identifier text into rough semantic word tokens."""
    expanded = "".join(
        (
            f" {character}" if character.isupper() and index and value[index - 1].islower() else character
        )
        for index, character in enumerate(value)
    )
    tokens = [
        token
        for token in "".join(ch if ch.isalnum() else " " for ch in expanded).split()
        if token
    ]
    expanded_tokens: list[str] = []
    for token in tokens:
        expanded_tokens.extend(_split_compound_lower_token(token))
    return expanded_tokens


def _is_clean_class_name(value: str, suffixes: tuple[str, ...]) -> bool:
    """Return True when an identifier is already concise and class-like."""
    trimmed = value.strip()
    if not trimmed or not trimmed[0].isupper() or any(not ch.isalnum() for ch in trimmed):
        return False
    tokens = [token.lower() for token in _split_identifier_tokens(trimmed)]
    return sum(1 for token in tokens if token in suffixes) <= 1


def _split_compound_lower_token(token: str) -> list[str]:
    """Best-effort split for long lowercase compound tokens such as datamodelservice."""
    lowered = token.lower()
    if token != lowered or len(token) < 8:
        return [token]

    vocabulary = sorted(_SEMANTIC_VOCABULARY, key=len, reverse=True)
    result: list[str] = []
    index = 0
    while index < len(lowered):
        match = next((word for word in vocabulary if lowered.startswith(word, index)), None)
        if match is None:
            return [token]
        result.append(match)
        index += len(match)
    return result or [token]


def _is_supported_system_breakdown_payload(payload: dict[str, Any]) -> bool:
    """Accept both the current rich contract and the legacy single-module shape."""
    if all(key in payload for key in ("modules", "build_graph", "tests")):
        return isinstance(payload.get("modules"), list)
    return all(
        key in payload
        for key in (
            "module_name",
            "purpose",
            "subcomponents",
            "dependencies",
            "implementation_order",
        )
    )


def _system_breakdown_files(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Derive deterministic scaffold targets from supported system-breakdown payloads."""
    if isinstance(payload.get("modules"), list):
        files: list[dict[str, str]] = []
        for module in payload["modules"]:
            if not isinstance(module, dict):
                continue
            module_name = _slug(str(module.get("name", "")))
            responsibility = str(module.get("responsibility", "")).strip() or str(module.get("name", "")).strip() or "module"
            files.extend(
                [
                    {
                        "path": f"{module_name}/models.py",
                        "purpose": f"{responsibility} data structures",
                        "source": "system_breakdown",
                    },
                    {
                        "path": f"{module_name}/service.py",
                        "purpose": f"{responsibility} service orchestration",
                        "source": "system_breakdown",
                    },
                ]
            )
        return files

    module_name = _slug(str(payload["module_name"]))
    purpose = str(payload["purpose"])
    return [
        {
            "path": f"{module_name}/models.py",
            "purpose": f"{purpose} data structures",
            "source": "system_breakdown",
        },
        {
            "path": f"{module_name}/service.py",
            "purpose": f"{purpose} service orchestration",
            "source": "system_breakdown",
        },
    ]
