"""Minimal router planner with exact-match reuse for Aether Phase 1.5."""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from aether_core.contracts.validator import ValidationResult, validate_output
from aether_core.classifier.rules import ClassifierRules, TaskBucket
from aether_core.enums import ArtifactStatus
from aether_core.itera.artifact_store import ArtifactStore
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.models import ClassificationRequest, ClassificationResponse
from aether_core.router.executors import StubExecutors, ExecutionResult


@dataclass
class RouteResult:
    """Result of router planning and execution."""

    decision: str
    reused: bool
    output: str
    status: ArtifactStatus
    task_bucket: str
    execution_metadata: dict = field(default_factory=dict)
    structured_content: Optional[dict] = None


class RouterPlanner:
    """Route approved requests through exact reuse before stub execution."""

    _DGCE_CORE_DATA_MODEL_ENTITY_ORDER = (
        "SectionInput",
        "PreviewArtifact",
        "ReviewArtifact",
        "ApprovalArtifact",
        "PreflightRecord",
        "ExecutionGate",
        "AlignmentRecord",
        "ExecutionStamp",
        "OutputArtifact",
    )
    _DGCE_CORE_DATA_MODEL_ENTITY_SPECS = {
        "SectionInput": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/input/{section_id}.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "input_fingerprint", "type": "string", "required": False},
                {"name": "content", "type": "object", "required": False},
            ],
        },
        "PreviewArtifact": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/plans/{section_id}.preview.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "artifact_fingerprint", "type": "string", "required": True},
            ],
        },
        "ReviewArtifact": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/reviews/{section_id}.review.md",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "artifact_fingerprint", "type": "string", "required": True},
            ],
        },
        "ApprovalArtifact": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/approvals/{section_id}.approval.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "artifact_fingerprint", "type": "string", "required": True},
                {"name": "approval_status", "type": "string", "required": False},
            ],
        },
        "PreflightRecord": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/preflight/{section_id}.preflight.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "validation_timestamp", "type": "string", "required": True},
            ],
        },
        "ExecutionGate": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/preflight/{section_id}.execution_gate.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "gate_status", "type": "string", "required": True},
            ],
        },
        "AlignmentRecord": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/preflight/{section_id}.alignment.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "alignment_status", "type": "string", "required": True},
            ],
        },
        "ExecutionStamp": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/execution/{section_id}.execution.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "execution_timestamp", "type": "string", "required": True},
                {"name": "run_outcome_class", "type": "string", "required": False},
            ],
        },
        "OutputArtifact": {
            "identity_keys": ["section_id"],
            "storage_path": ".dce/outputs/{section_id}.json",
            "fields": [
                {"name": "section_id", "type": "string", "required": True},
                {"name": "artifact_fingerprint", "type": "string", "required": True},
                {"name": "run_outcome_class", "type": "string", "required": False},
            ],
        },
    }
    _DGCE_CORE_DATA_MODEL_RELATIONSHIPS = (
        "SectionInput->PreviewArtifact",
        "PreviewArtifact->ReviewArtifact",
        "ReviewArtifact->ApprovalArtifact",
        "ApprovalArtifact->PreflightRecord",
        "PreflightRecord->ExecutionGate",
        "ExecutionGate->AlignmentRecord",
        "AlignmentRecord->ExecutionStamp",
        "ExecutionStamp->OutputArtifact",
    )
    _ORDERED_SEMANTIC_STRING_LIST_KEYS = {"identity_keys", "required"}
    _ORDERED_SYSTEM_BREAKDOWN_STRING_LIST_KEYS = {
        "acceptance_criteria",
        "core_layers",
        "determinism_rules",
        "edges",
        "include",
        "subcomponents",
        "required_properties",
        "implementation_order",
    }
    _ORDERED_API_SURFACE_STRING_LIST_KEYS = {
        "acceptance_criteria",
        "data_fields",
        "determinism_rules",
        "include",
        "exclude",
        "lifecycle_stages",
        "next_action_examples",
        "optional_fields",
        "preconditions",
        "required",
        "required_error_codes",
        "required_fields",
        "required_properties",
        "side_effects",
    }
    _SEMANTIC_NAME_STOPWORDS = {
        "a",
        "an",
        "and",
        "by",
        "definition",
        "definitions",
        "describe",
        "describes",
        "description",
        "details",
        "for",
        "from",
        "generated",
        "in",
        "json",
        "managing",
        "name",
        "names",
        "of",
        "or",
        "schema",
        "the",
        "to",
        "with",
    }
    _SEMANTIC_METADATA_STOPWORDS = {
        "enum",
        "false",
        "field",
        "fields",
        "path",
        "paths",
        "required",
        "storage",
        "true",
        "type",
        "types",
        "value",
        "values",
    }
    _SEMANTIC_VOCABULARY = (
        "alignment",
        "api",
        "approval",
        "artifact",
        "board",
        "client",
        "data",
        "entity",
        "execution",
        "fingerprint",
        "gate",
        "governance",
        "input",
        "interface",
        "item",
        "lifecycle",
        "method",
        "mission",
        "model",
        "output",
        "preflight",
        "preview",
        "record",
        "review",
        "section",
        "service",
        "stamp",
        "state",
        "status",
        "surface",
    )

    def __init__(
        self,
        cache: Optional[ExactMatchCache] = None,
        artifact_store: Optional[ArtifactStore] = None,
        executors: Optional[StubExecutors] = None,
    ):
        self.cache = cache or ExactMatchCache()
        self.artifact_store = artifact_store or ArtifactStore()
        self.executors = executors or StubExecutors()
        self.task_classifier = ClassifierRules()

    def route(
        self,
        request: ClassificationRequest,
        classification: ClassificationResponse,
    ) -> RouteResult:
        """Attempt exact approved reuse before executing a placeholder model."""
        task_bucket = self.task_classifier.classify(request.content)["bucket"]
        # NOTE:
        # Cache key uses a deterministic reuse scope derived from request context.
        # - strict: full request context (including structured fields)
        # - project: project only, plus explicit reuse_scope marker
        # - prompt_profile: separates scaffold-shaped prompts from default prompts
        #
        # Full request context is still stored with artifacts even when
        # cache reuse is keyed more narrowly under project scope.
        context = request.context_dict()
        prompt_profile = request.prompt_profile_value()
        reuse_context = self.cache.scope_context(
            {**context, "prompt_profile": prompt_profile},
            request.reuse_scope_value(),
        )

        if classification.status == ArtifactStatus.BLOCKED:
            return RouteResult(
                decision="BLOCKED",
                reused=False,
                output="",
                status=ArtifactStatus.BLOCKED,
                task_bucket=task_bucket.value,
                execution_metadata={
                    "estimated_tokens": 0,
                    "estimated_cost": 0.0,
                    "inference_avoided": True,
                    "backend_used": "blocked",
                    "worth_running": False,
                },
            )

        if classification.status == ArtifactStatus.APPROVED:
            cache_result = self.cache.lookup(task_bucket, request.content, reuse_context)
            if cache_result.hit:
                reuse_metadata = self._reuse_metadata()
                structured_content = None
                if request.is_structured_output():
                    structure_metadata, structured_content = self._validate_structured_output(
                        request,
                        cache_result.content or "",
                    )
                    reuse_metadata.update(structure_metadata)
                self.artifact_store.store_artifact(
                    artifact_id=request.request_id,
                    task_bucket=task_bucket,
                    content=request.content,
                    output=cache_result.content or "",
                    status=ArtifactStatus.APPROVED,
                    context={**context, "prompt_profile": prompt_profile, **reuse_metadata},
                    structured_content=structured_content,
                )
                return RouteResult(
                    decision="REUSE",
                    reused=True,
                    output=cache_result.content or "",
                    status=ArtifactStatus.APPROVED,
                    task_bucket=task_bucket.value,
                    execution_metadata=reuse_metadata,
                    structured_content=structured_content,
                )

        executor_name = self._select_executor(task_bucket)
        execution = self.executors.run(executor_name, request.execution_prompt())
        structured_content = None
        if request.is_structured_output():
            structure_metadata, structured_content = self._validate_structured_output(
                request,
                execution.output,
            )
            execution.metadata.update(structure_metadata)
        artifact_context = {**context, "prompt_profile": prompt_profile, **execution.metadata}
        self.artifact_store.store_artifact(
            artifact_id=request.request_id,
            task_bucket=task_bucket,
            content=request.content,
            output=execution.output,
            status=execution.status,
            context=artifact_context,
            structured_content=structured_content,
        )

        return RouteResult(
            decision=executor_name,
            reused=False,
            output=execution.output,
            status=execution.status,
            task_bucket=task_bucket.value,
            execution_metadata=execution.metadata,
            structured_content=structured_content,
        )

    def _select_executor(self, task_bucket: TaskBucket) -> str:
        """Select a stub executor for the task bucket."""
        if task_bucket == TaskBucket.PLANNING:
            return "MID_MODEL"
        if task_bucket == TaskBucket.CODE_ROUTINE:
            return "LARGE_MODEL"
        return "SMALL_MODEL"

    def _reuse_metadata(self) -> dict:
        """Return deterministic metadata for exact-match reuse hits."""
        return {
            "estimated_tokens": 0,
            "estimated_cost": 0.0,
            "inference_avoided": True,
            "backend_used": "reuse",
            "worth_running": False,
        }

    def _strip_markdown_fences(self, text: str) -> str:
        """Remove outer markdown code fences without altering valid JSON content."""
        normalized = text.strip()
        if not normalized.startswith("```"):
            return normalized

        newline_index = normalized.find("\n")
        if newline_index != -1:
            normalized = normalized[newline_index + 1 :]
        else:
            normalized = normalized[3:]

        if normalized.endswith("```"):
            normalized = normalized[:-3]

        return normalized.strip()

    def _strip_json_line_comments(self, text: str) -> str:
        """Remove // comments that occur outside quoted JSON strings."""
        sanitized: list[str] = []
        in_string = False
        escape_next = False
        index = 0

        while index < len(text):
            char = text[index]

            if escape_next:
                sanitized.append(char)
                escape_next = False
                index += 1
                continue

            if char == "\\" and in_string:
                sanitized.append(char)
                escape_next = True
                index += 1
                continue

            if char == '"':
                in_string = not in_string
                sanitized.append(char)
                index += 1
                continue

            if not in_string and char == "/" and index + 1 < len(text) and text[index + 1] == "/":
                index += 2
                while index < len(text) and text[index] not in "\r\n":
                    index += 1
                continue

            sanitized.append(char)
            index += 1

        return "".join(sanitized)

    def _normalize_set_like_methods_blocks(self, text: str) -> str:
        """Rewrite malformed set-like methods blocks into valid JSON arrays."""

        def replace_match(match: re.Match[str]) -> str:
            body = match.group("body")
            if not self._is_string_only_collection(body):
                return match.group(0)
            return f'"methods": [{body}]'

        return re.sub(
            r'"methods"\s*:\s*\{(?P<body>[^{}]*)\}',
            replace_match,
            text,
        )

    def _extract_first_json_object_substring(self, text: str) -> str | None:
        """Return the first decodable top-level JSON object substring from noisy model output."""
        decoder = json.JSONDecoder()
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                parsed, end_index = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return text[index : index + end_index]
        return None

    def _parse_structured_json_output(self, output: str, *, allow_fallback_extraction: bool = True) -> dict | None:
        """Parse structured model output, optionally recovering from leading or trailing non-JSON text."""
        normalized_output = self._normalize_set_like_methods_blocks(
            self._strip_json_line_comments(
                self._strip_markdown_fences(output)
            )
        )
        if not normalized_output.strip():
            return None

        try:
            parsed = json.loads(normalized_output)
        except (TypeError, ValueError, json.JSONDecodeError):
            if not allow_fallback_extraction:
                return None
            extracted = self._extract_first_json_object_substring(normalized_output)
            if extracted is None:
                return None
            try:
                parsed = json.loads(extracted)
            except (TypeError, ValueError, json.JSONDecodeError):
                return None

        return parsed if isinstance(parsed, dict) else None

    def _is_string_only_collection(self, text: str) -> bool:
        """Return True when a brace body is a comma-separated collection of JSON strings only."""
        index = 0
        length = len(text)
        saw_string = False

        while index < length:
            while index < length and text[index] in " \t\r\n,":
                index += 1
            if index >= length:
                break
            if text[index] != '"':
                return False
            saw_string = True
            index += 1
            escape_next = False
            while index < length:
                char = text[index]
                if escape_next:
                    escape_next = False
                    index += 1
                    continue
                if char == "\\":
                    escape_next = True
                    index += 1
                    continue
                if char == '"':
                    index += 1
                    break
                index += 1
            else:
                return False

            while index < length and text[index] in " \t\r\n":
                index += 1
            if index < length and text[index] not in ",":
                return False

        return saw_string

    def _normalize_system_breakdown_payload(self, schema_name: str, payload: dict) -> dict:
        """Normalize rich system-breakdown contracts into the legacy top-level schema."""
        if schema_name != "dgce_system_breakdown_v1":
            return payload

        normalized = self._canonicalize_system_breakdown_value(dict(payload))
        modules = normalized.get("modules")
        if isinstance(modules, list) and modules:
            module_names = [module["name"] for module in modules if isinstance(module, dict) and "name" in module]
            if "module_name" not in normalized and module_names:
                normalized["module_name"] = str(module_names[0])
            if "purpose" not in normalized:
                normalized["purpose"] = str(
                    normalized.get("objective")
                    or next(
                        (
                            module.get("responsibility")
                            for module in modules
                            if isinstance(module, dict) and isinstance(module.get("responsibility"), str)
                        ),
                        "",
                    )
                )
            if "subcomponents" not in normalized and module_names:
                normalized["subcomponents"] = module_names
            if "dependencies" not in normalized:
                dependencies = sorted(
                    {
                        str(dependency.get("reference", dependency.get("name", "")))
                        for module in modules
                        if isinstance(module, dict)
                        for dependency in module.get("dependencies", [])
                        if (
                            isinstance(dependency, dict)
                            and isinstance(dependency.get("reference", dependency.get("name", "")), str)
                            and str(dependency.get("reference", dependency.get("name", "")))
                        )
                    }
                )
                normalized["dependencies"] = dependencies
            if "implementation_order" not in normalized and module_names:
                normalized["implementation_order"] = module_names

        return self._canonicalize_system_breakdown_value(normalized)

    def _repair_system_breakdown_payload(self, parsed: dict) -> dict:
        """
        Deterministically repair known required fields for system-breakdown.
        Only fills missing or empty owned_paths.
        Does NOT override valid data.
        """
        repaired = dict(parsed)
        modules = repaired.get("modules")
        if not isinstance(modules, list):
            return repaired

        default_owned_paths = {
            "SectionInputHandler": [".dce/input/"],
            "PreviewGenerator": [".dce/plans/"],
            "ReviewManager": [".dce/reviews/"],
            "ApprovalManager": [".dce/approvals/"],
            "StaleCheckEvaluator": [".dce/preflight/{section_id}.stale_check.json"],
            "PreflightChecker": [".dce/preflight/{section_id}.preflight.json"],
            "ExecutionGateEvaluator": [".dce/preflight/{section_id}.execution_gate.json"],
            "AlignmentValidator": [".dce/preflight/{section_id}.alignment.json"],
            "ExecutionEngine": [".dce/execution/stamps/", ".dce/outputs/"],
        }

        repaired_modules: list[object] = []
        for module in modules:
            if not isinstance(module, dict):
                repaired_modules.append(module)
                continue

            repaired_module = dict(module)
            owned_paths = repaired_module.get("owned_paths")
            if not isinstance(owned_paths, list) or not owned_paths:
                module_name = str(repaired_module.get("name", ""))
                repaired_module["owned_paths"] = list(default_owned_paths.get(module_name, [".dce/"]))
            repaired_modules.append(repaired_module)

        repaired["modules"] = repaired_modules
        return repaired

    def _canonicalize_system_breakdown_value(self, value: object, *, parent_key: str = "") -> object:
        """Return a deterministically ordered DGCE system-breakdown payload value."""
        if isinstance(value, dict):
            normalized = {
                key: self._canonicalize_system_breakdown_value(item, parent_key=str(key))
                for key, item in value.items()
            }
            return dict(sorted(normalized.items(), key=lambda entry: str(entry[0])))

        if isinstance(value, list):
            normalized_items = [
                self._canonicalize_system_breakdown_value(item, parent_key=parent_key)
                for item in value
            ]
            if all(isinstance(item, str) for item in normalized_items):
                return (
                    normalized_items
                    if parent_key in self._ORDERED_SYSTEM_BREAKDOWN_STRING_LIST_KEYS
                    else sorted(normalized_items)
                )
            if all(isinstance(item, dict) for item in normalized_items):
                return sorted(
                    normalized_items,
                    key=lambda item: self._system_breakdown_sort_key(parent_key, item),
                )
            if parent_key == "edges" and all(isinstance(item, list) for item in normalized_items):
                return sorted(
                    normalized_items,
                    key=lambda item: tuple(str(part) for part in item),
                )
            return normalized_items

        return value

    def _system_breakdown_sort_key(self, parent_key: str, item: dict) -> tuple[str, ...]:
        """Return the stable ordering key for one system-breakdown object."""
        order = str(item.get("order", item.get("implementation_order", "")))
        if parent_key == "modules":
            return (
                order,
                str(item.get("name", "")),
            )
        return (
            order,
            str(item.get("name", "")),
            json.dumps(item, sort_keys=True),
        )

    def _validate_dgce_system_breakdown_payload(self, payload: dict) -> Optional[ValidationResult]:
        """Accept nested module contracts for DGCE system-breakdown outputs."""
        legacy_keys = {
            "module_name",
            "purpose",
            "subcomponents",
            "dependencies",
            "implementation_order",
        }
        if legacy_keys.issubset(payload):
            return ValidationResult(ok=True, missing_keys=[], error=None)

        modules = payload.get("modules")
        build_graph = payload.get("build_graph")
        if not isinstance(modules, list) or not modules:
            return ValidationResult(ok=False, missing_keys=["modules"], error="missing_keys")
        if not isinstance(build_graph, dict):
            return ValidationResult(ok=False, missing_keys=["build_graph"], error="missing_keys")
        tests = payload.get("tests")
        if tests is None or not isinstance(tests, list):
            return ValidationResult(ok=False, missing_keys=["tests"], error="missing_keys")

        required_module_keys = [
            "name",
            "layer",
            "responsibility",
            "inputs",
            "outputs",
            "dependencies",
            "governance_touchpoints",
            "failure_modes",
            "owned_paths",
            "implementation_order",
        ]
        missing_keys: list[str] = []
        module_names: set[str] = set()
        implementation_orders: list[int] = []
        produced_artifact_paths: dict[str, str] = {}
        owned_paths: list[tuple[str, str]] = []
        required_edges: set[tuple[str, str]] = set()
        for module in modules:
            if not isinstance(module, dict):
                missing_keys.append("modules")
                continue
            for key in required_module_keys:
                if key not in module:
                    missing_keys.append(key)
            module_name = module.get("name")
            if isinstance(module_name, str) and module_name:
                module_names.add(module_name)
            else:
                missing_keys.append("name")
            order = module.get("implementation_order")
            if isinstance(order, int):
                implementation_orders.append(order)
            else:
                missing_keys.append("implementation_order")

            for owned_path in module.get("owned_paths", []):
                if isinstance(module_name, str) and isinstance(owned_path, str):
                    owned_paths.append((module_name, owned_path))

            inputs = module.get("inputs")
            if not isinstance(inputs, list):
                missing_keys.append("inputs")
                inputs = []
            outputs = module.get("outputs")
            if not isinstance(outputs, list):
                missing_keys.append("outputs")
                outputs = []
            dependencies = module.get("dependencies")
            if not isinstance(dependencies, list):
                missing_keys.append("dependencies")
                dependencies = []

            for dependency in dependencies:
                if not isinstance(dependency, dict):
                    missing_keys.append("dependencies")
                    continue
                if any(
                    not isinstance(dependency.get(key), str) or not str(dependency.get(key))
                    for key in ("name", "kind", "reference")
                ):
                    missing_keys.append("dependencies")

            for port in inputs:
                if not isinstance(port, dict):
                    missing_keys.append("inputs")
                    continue
                if any(
                    not isinstance(port.get(key), str) or not str(port.get(key))
                    for key in ("name", "type")
                ):
                    missing_keys.append("inputs")
                    continue
                port_type = str(port.get("type"))
                port_name = str(port.get("name"))
                if port_type == "artifact":
                    artifact_path = port.get("artifact_path")
                    if not isinstance(artifact_path, str) or not artifact_path:
                        missing_keys.append("inputs")
                    elif isinstance(module_name, str):
                        producer = produced_artifact_paths.get(artifact_path)
                        if producer and producer != module_name:
                            required_edges.add((producer, module_name))
                elif port_type.endswith("Request"):
                    schema_fields = port.get("schema_fields")
                    if not isinstance(schema_fields, list) or not schema_fields:
                        missing_keys.append("inputs")
                    else:
                        for field in schema_fields:
                            if isinstance(field, dict):
                                if not isinstance(field.get("name"), str) or not field.get("name"):
                                    missing_keys.append("inputs")
                                if not isinstance(field.get("type"), str) or not field.get("type"):
                                    missing_keys.append("inputs")
                                if not isinstance(field.get("required"), bool):
                                    missing_keys.append("inputs")
                                if field.get("type") == "array":
                                    if not isinstance(field.get("items"), str) or not field.get("items"):
                                        missing_keys.append("inputs")
                                elif "items" in field and field.get("items") is not None:
                                    missing_keys.append("inputs")
                            else:
                                missing_keys.append("inputs")
                    if port_name in {"review_request", "approval_request"} and not port.get("schema_fields"):
                        missing_keys.append("inputs")

            for port in outputs:
                if not isinstance(port, dict):
                    missing_keys.append("outputs")
                    continue
                if any(
                    not isinstance(port.get(key), str) or not str(port.get(key))
                    for key in ("name", "type")
                ):
                    missing_keys.append("outputs")
                    continue
                if str(port.get("type")) == "artifact":
                    artifact_path = port.get("artifact_path")
                    if not isinstance(artifact_path, str) or not artifact_path:
                        missing_keys.append("outputs")
                    elif isinstance(module_name, str):
                        produced_artifact_paths[artifact_path] = module_name

        edges = build_graph.get("edges")
        if not isinstance(edges, list):
            missing_keys.append("edges")
        else:
            actual_edges: set[tuple[str, str]] = set()
            for edge in edges:
                if not isinstance(edge, list) or len(edge) != 2:
                    missing_keys.append("edges")
                    continue
                if any(not isinstance(node, str) or node not in module_names for node in edge):
                    missing_keys.append("dependencies")
                    break
                actual_edges.add((str(edge[0]), str(edge[1])))
            if required_edges and not required_edges.issubset(actual_edges):
                missing_keys.append("dependencies")

        # Validate artifact-input edges after all producers are known.
        for module in modules:
            if not isinstance(module, dict) or not isinstance(module.get("name"), str):
                continue
            module_name = str(module["name"])
            for port in module.get("inputs", []):
                if (
                    isinstance(port, dict)
                    and port.get("type") == "artifact"
                    and isinstance(port.get("artifact_path"), str)
                ):
                    producer = produced_artifact_paths.get(str(port["artifact_path"]))
                    if producer and producer != module_name:
                        required_edges.add((producer, module_name))

        if isinstance(edges, list):
            actual_edges = {
                (str(edge[0]), str(edge[1]))
                for edge in edges
                if isinstance(edge, list)
                and len(edge) == 2
                and all(isinstance(node, str) for node in edge)
            }
            if required_edges and not required_edges.issubset(actual_edges):
                missing_keys.append("dependencies")

        stale_check_path = ".dce/preflight/{section_id}.stale_check.json"
        stale_check_module_present = any(
            isinstance(module, dict)
            and any(
                isinstance(port, dict)
                and port.get("type") == "artifact"
                and port.get("artifact_path") == stale_check_path
                for port in module.get("outputs", [])
            )
            and stale_check_path in module.get("owned_paths", [])
            for module in modules
        )
        if not stale_check_module_present:
            missing_keys.append("owned_paths")

        normalized_owned_paths = [
            (module_name, owned_path.rstrip("/"))
            for module_name, owned_path in owned_paths
        ]
        for index, (left_module, left_path) in enumerate(normalized_owned_paths):
            for right_module, right_path in normalized_owned_paths[index + 1 :]:
                if left_module == right_module:
                    continue
                same_path = left_path == right_path
                overlapping = left_path.startswith(f"{right_path}/") or right_path.startswith(f"{left_path}/")
                if same_path or overlapping:
                    missing_keys.append("owned_paths")
                    break
            if "owned_paths" in missing_keys:
                break

        if implementation_orders and len(set(implementation_orders)) != len(implementation_orders):
            missing_keys.append("implementation_order")

        if missing_keys:
            return ValidationResult(
                ok=False,
                missing_keys=sorted(set(missing_keys)),
                error="missing_keys",
            )
        return ValidationResult(ok=True, missing_keys=[], error=None)

    def _normalize_api_surface_payload(self, schema_name: str, payload: dict) -> dict:
        """Normalize nested api-surface method shapes into the existing top-level schema."""
        if schema_name != "dgce_api_surface_v1":
            return payload

        normalized = self._canonicalize_api_surface_value(dict(payload))
        endpoints = normalized.get("endpoints")

        if "interfaces" not in normalized and isinstance(endpoints, list) and endpoints:
            normalized["interfaces"] = ["DGCESectionGovernanceAPI"]

        if "methods" not in normalized and isinstance(endpoints, list):
            normalized["methods"] = self._normalize_api_surface_methods_from_endpoints(endpoints)

        methods = normalized.get("methods")
        if "methods" not in normalized:
            interfaces = normalized.get("interfaces")
            if isinstance(interfaces, dict):
                methods = {
                    interface_name: interface_payload.get("methods", [])
                    if isinstance(interface_payload, dict)
                    else []
                    for interface_name, interface_payload in interfaces.items()
                }
                normalized["methods"] = methods

        missing_keys = {"methods", "inputs", "outputs", "error_cases"} - normalized.keys()
        if not missing_keys:
            return self._canonicalize_api_surface_value(self._clean_api_surface_quality(normalized))

        methods = normalized.get("methods")
        if isinstance(methods, dict):
            if "inputs" in missing_keys:
                normalized["inputs"] = self._normalize_api_surface_field(methods, "input", {})
            if "outputs" in missing_keys:
                normalized["outputs"] = self._normalize_api_surface_field(methods, "output", {})
            if "error_cases" in missing_keys:
                normalized["error_cases"] = self._normalize_api_surface_field(methods, "error_cases", [])
        elif isinstance(methods, list):
            if "inputs" in missing_keys:
                normalized["inputs"] = self._normalize_api_surface_method_list_field(
                    methods,
                    "input",
                    {},
                )
            if "outputs" in missing_keys:
                normalized["outputs"] = self._normalize_api_surface_method_list_field(
                    methods,
                    "output",
                    {},
                )
            if "error_cases" in missing_keys:
                normalized["error_cases"] = self._normalize_api_surface_method_list_field(
                    methods,
                    "error_cases",
                    [],
                )

        normalized = self._clean_api_surface_quality(normalized)
        return self._canonicalize_api_surface_value(normalized)

    def _canonicalize_api_surface_value(self, value: object, *, parent_key: str = "") -> object:
        """Return a deterministically ordered DGCE api-surface payload value."""
        if isinstance(value, dict):
            normalized = {
                key: self._canonicalize_api_surface_value(item, parent_key=str(key))
                for key, item in value.items()
            }
            return dict(sorted(normalized.items(), key=lambda entry: str(entry[0])))

        if isinstance(value, list):
            normalized_items = [
                self._canonicalize_api_surface_value(item, parent_key=parent_key)
                for item in value
            ]
            if all(isinstance(item, str) for item in normalized_items):
                return (
                    normalized_items
                    if parent_key in self._ORDERED_API_SURFACE_STRING_LIST_KEYS
                    else sorted(normalized_items)
                )
            if all(isinstance(item, dict) for item in normalized_items):
                return sorted(
                    normalized_items,
                    key=lambda item: self._api_surface_sort_key(parent_key, item),
                )
            return normalized_items

        return value

    def _api_surface_sort_key(self, parent_key: str, item: dict) -> tuple[str, ...]:
        """Return the stable ordering key for one api-surface object."""
        order = str(item.get("order", ""))
        if parent_key == "endpoints":
            return (
                order,
                str(item.get("name", "")),
                str(item.get("path", "")),
                str(item.get("method", "")),
            )
        if parent_key == "error_responses":
            return (
                order,
                str(item.get("status_code", "")),
                str(item.get("error_code", "")),
            )
        if parent_key in {"common_request_headers", "common_response_headers"}:
            return (
                order,
                str(item.get("name", "")),
                str(item.get("value", "")),
                str(item.get("type", "")),
            )
        return (
            order,
            str(item.get("name", "")),
            json.dumps(item, sort_keys=True),
        )

    def _normalize_api_surface_methods_from_endpoints(self, endpoints: list) -> dict:
        """Build legacy api-surface method entries from explicit endpoint contracts."""
        normalized: dict = {}
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                continue

            endpoint_name = endpoint.get("name")
            if not isinstance(endpoint_name, str) or not endpoint_name:
                continue

            normalized[endpoint_name] = {
                "method": endpoint.get("method"),
                "path": endpoint.get("path"),
                "purpose": endpoint.get("purpose"),
                "input": endpoint.get("request_body"),
                "output": endpoint.get("success_response"),
                "error_cases": endpoint.get("error_responses", []),
                "preconditions": endpoint.get("preconditions", []),
                "idempotency": endpoint.get("idempotency"),
                "side_effects": endpoint.get("side_effects", []),
            }
        return normalized

    def _normalize_api_surface_field(
        self,
        methods: dict,
        field_name: str,
        default: dict | list,
    ) -> dict:
        """Build top-level api-surface field data from normalized methods."""
        alternate_field_name = f"{field_name}s" if field_name in {"input", "output"} else field_name
        normalized: dict = {}
        for method_name, method_payload in methods.items():
            if isinstance(method_payload, dict):
                normalized[method_name] = method_payload.get(
                    field_name,
                    method_payload.get(alternate_field_name, default),
                )
            elif isinstance(method_payload, list):
                normalized[method_name] = [
                    item.get(field_name, item.get(alternate_field_name, default))
                    if isinstance(item, dict)
                    else default
                    for item in method_payload
                ]
            else:
                normalized[method_name] = default
        return normalized

    def _normalize_api_surface_method_list_field(
        self,
        methods: list,
        field_name: str,
        default: dict | list,
    ) -> dict:
        """Build top-level api-surface field data from a top-level methods list."""
        alternate_field_name = f"{field_name}s" if field_name in {"input", "output"} else field_name
        normalized: dict = {}
        for method_payload in methods:
            if not isinstance(method_payload, dict):
                continue

            method_name = method_payload.get("name")
            if not isinstance(method_name, str) or not method_name:
                continue

            normalized[method_name] = method_payload.get(
                field_name,
                method_payload.get(alternate_field_name, default),
            )
        return normalized

    def _normalize_dgce_data_model_payload(self, schema_name: str, payload: dict) -> dict:
        """Normalize optional DGCE data-model fields into the existing top-level schema."""
        if schema_name != "dgce_data_model_v1":
            return payload

        parsed = dict(payload)
        normalized = {
            "modules": parsed.get("modules"),
            "entities": self._normalize_data_model_entities(parsed.get("entities", [])),
            "fields": self._dedupe_preserve_order(
                self._canonicalize_dgce_data_model_value(parsed.get("fields", []))
            ),
            "relationships": self._dedupe_preserve_order(
                self._canonicalize_dgce_data_model_value(parsed.get("relationships", []))
            ),
            "validation_rules": self._dedupe_preserve_order(
                self._canonicalize_dgce_data_model_value(parsed.get("validation_rules", []))
            ),
        }
        if "modules" not in normalized:
            normalized["modules"] = parsed.get("modules", [])
        if normalized["modules"] is None:
            normalized["modules"] = parsed.get("modules", [])
        return normalized

    def _repair_dgce_data_model_payload(self, parsed: dict) -> dict:
        """Deterministically synthesize a minimal modules array for DGCE data-model payloads when missing."""
        repaired = dict(parsed)
        normalized_entities = self._normalize_data_model_entities(repaired.get("entities", []))
        repaired["entities"] = normalized_entities
        repaired["modules"] = self._merge_non_core_dgce_modules(
            repaired.get("modules"),
            repaired["entities"],
            repaired.get("relationships"),
        )
        if "relationships" not in repaired:
            repaired["relationships"] = []
        if "validation_rules" not in repaired:
            repaired["validation_rules"] = []
        return repaired

    def _apply_dgce_core_data_model_contract(self, parsed: dict) -> dict:
        """Backfill deterministic DGCE core entities, fields, relationships, and modules."""
        repaired = dict(parsed)
        repaired["entities"] = self._merge_dgce_core_entity_contracts(
            self._normalize_data_model_entities(repaired.get("entities", []))
        )
        repaired["fields"] = self._merge_dgce_core_field_catalog(
            repaired.get("fields", []),
            repaired["entities"],
        )
        repaired["relationships"] = self._merge_dgce_core_relationships(repaired.get("relationships", []))
        repaired["modules"] = self._merge_dgce_core_modules(
            repaired.get("modules"),
            repaired["entities"],
            repaired["relationships"],
        )
        repaired["validation_rules"] = self._merge_dgce_core_validation_rules(
            repaired.get("validation_rules", []),
        )
        return repaired

    def _merge_dgce_core_entity_contracts(self, entities: object) -> list[dict]:
        """Backfill deterministic DGCE core entity minima into the normalized entity list."""
        entity_map: dict[str, dict] = {}
        if isinstance(entities, list):
            for entity in entities:
                if isinstance(entity, dict):
                    cleaned_name = self._clean_entity_name(entity.get("name"))
                    entity_map[cleaned_name] = self._merge_dgce_core_entity_payload(cleaned_name, dict(entity))
                elif isinstance(entity, str) and entity.strip():
                    cleaned_name = self._clean_entity_name(entity)
                    entity_map[cleaned_name] = self._merge_dgce_core_entity_payload(cleaned_name, {"name": cleaned_name})

        for entity_name in self._DGCE_CORE_DATA_MODEL_ENTITY_ORDER:
            entity_map[entity_name] = self._merge_dgce_core_entity_payload(
                entity_name,
                entity_map.get(entity_name, {"name": entity_name}),
            )

        ordered_entities = [entity_map[name] for name in self._DGCE_CORE_DATA_MODEL_ENTITY_ORDER]
        other_entities = [
            entity_map[name]
            for name in sorted(entity_map)
            if name not in self._DGCE_CORE_DATA_MODEL_ENTITY_ORDER
        ]
        merged_entities = ordered_entities + other_entities
        return self._canonicalize_dgce_data_model_value(merged_entities, parent_key="entities")

    def _merge_dgce_core_entity_payload(self, entity_name: str, entity_payload: dict) -> dict:
        """Backfill one DGCE core entity with deterministic minimum fields and relationships."""
        merged = dict(entity_payload)
        merged["name"] = entity_name

        entity_spec = self._DGCE_CORE_DATA_MODEL_ENTITY_SPECS.get(entity_name)
        if entity_spec is None:
            merged.setdefault("fields", [])
            merged.setdefault("description", "")
            return merged

        merged["identity_keys"] = self._merge_ordered_string_list(
            entity_spec.get("identity_keys", []),
            merged.get("identity_keys", []),
        )
        merged.setdefault("storage_path", entity_spec.get("storage_path", ""))
        merged.setdefault("description", "")
        merged["fields"] = self._merge_dgce_core_entity_fields(
            merged.get("fields", []),
            entity_spec.get("fields", []),
        )
        merged.setdefault("invariants", [])
        return merged

    def _merge_dgce_core_entity_fields(self, fields: object, required_fields: list[dict]) -> list[dict]:
        """Backfill minimum field definitions for one DGCE core entity."""
        field_map: dict[str, dict] = {}
        ordered_names: list[str] = []

        if isinstance(fields, list):
            for field in fields:
                if not isinstance(field, dict):
                    continue
                field_name = str(field.get("name", "")).strip()
                if not field_name:
                    continue
                if field_name not in ordered_names:
                    ordered_names.append(field_name)
                field_map[field_name] = dict(field)

        for required_field in required_fields:
            field_name = str(required_field.get("name", "")).strip()
            if not field_name:
                continue
            existing_field = field_map.get(field_name, {"name": field_name})
            existing_field.setdefault("type", required_field.get("type"))
            existing_field.setdefault("required", required_field.get("required", False))
            field_map[field_name] = existing_field
            if field_name not in ordered_names:
                ordered_names.append(field_name)

        merged_fields = [field_map[name] for name in ordered_names if name in field_map]
        return self._canonicalize_dgce_data_model_value(merged_fields, parent_key="fields")

    def _merge_dgce_core_field_catalog(self, fields: object, entities: list[dict]) -> list[str]:
        """Backfill the top-level field catalog from the deterministic entity minima."""
        merged_fields = self._merge_ordered_string_list([], fields)
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            for field in entity.get("fields", []):
                if not isinstance(field, dict):
                    continue
                field_name = str(field.get("name", "")).strip()
                if field_name and field_name not in merged_fields:
                    merged_fields.append(field_name)
        return self._canonicalize_dgce_data_model_value(merged_fields, parent_key="fields")

    def _merge_dgce_core_relationships(self, relationships: object) -> list[str]:
        """Backfill the deterministic DGCE core lifecycle relationship chain."""
        merged_relationships = self._merge_ordered_string_list(
            list(self._DGCE_CORE_DATA_MODEL_RELATIONSHIPS),
            relationships,
        )
        return self._dedupe_preserve_order(merged_relationships)

    def _merge_dgce_core_modules(self, modules: object, entities: list[dict], relationships: list[str]) -> list[dict]:
        """Backfill the DGCE core module envelope with deterministic entities and relationships."""
        repaired_modules: list[dict] = []
        dgce_module_found = False
        entity_names = [str(entity.get("name")) for entity in entities if isinstance(entity, dict) and entity.get("name")]

        if isinstance(modules, list):
            for module in modules:
                if not isinstance(module, dict):
                    continue
                repaired_module = dict(module)
                repaired_module.setdefault("required", [])
                repaired_module.setdefault("identity_keys", [])
                if str(repaired_module.get("name", "")) == "DGCEDataModel":
                    repaired_module["entities"] = self._merge_ordered_string_list(entity_names, repaired_module.get("entities", []))
                    repaired_module["relationships"] = self._merge_ordered_string_list(relationships, repaired_module.get("relationships", []))
                    dgce_module_found = True
                repaired_modules.append(repaired_module)

        if not dgce_module_found:
            repaired_modules.append(
                {
                    "name": "DGCEDataModel",
                    "entities": entity_names,
                    "relationships": list(relationships),
                    "required": [],
                    "identity_keys": [],
                }
            )

        return self._canonicalize_dgce_data_model_value(repaired_modules, parent_key="modules")

    def _merge_non_core_dgce_modules(self, modules: object, entities: list[object], relationships: object) -> list[dict]:
        """Preserve the existing non-core module behavior while backfilling required module keys."""
        if isinstance(modules, list) and modules:
            repaired_modules: list[dict] = []
            for module in modules:
                if not isinstance(module, dict):
                    continue
                repaired_module = dict(module)
                repaired_module.setdefault("required", [])
                repaired_module.setdefault("identity_keys", [])
                repaired_modules.append(repaired_module)
            return repaired_modules

        entity_names = [
            str(entity.get("name"))
            for entity in entities
            if isinstance(entity, dict) and isinstance(entity.get("name"), str) and entity.get("name")
        ]
        return [
            {
                "name": "DGCEDataModel",
                "entities": entity_names,
                "relationships": list(relationships) if isinstance(relationships, list) else [],
                "required": [],
                "identity_keys": [],
            }
        ]

    def _merge_dgce_core_validation_rules(self, validation_rules: object) -> list[str]:
        """Backfill stable validation rules for the DGCE core data-model contract."""
        merged_rules = self._merge_ordered_string_list(
            ["section_id required"],
            validation_rules,
        )
        return self._dedupe_preserve_order(merged_rules)

    def _merge_ordered_string_list(self, preferred_items: list[str], items: object) -> list[str]:
        """Merge preferred string items ahead of existing unique string values deterministically."""
        merged: list[str] = []
        for item in preferred_items:
            if isinstance(item, str) and item and item not in merged:
                merged.append(item)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, str) and item and item not in merged:
                    merged.append(item)
        return merged

    def _clean_api_surface_quality(self, payload: dict) -> dict:
        """Normalize api-surface identifiers into short, stable semantic names."""
        normalized = dict(payload)

        interfaces = normalized.get("interfaces")
        if isinstance(interfaces, list):
            normalized["interfaces"] = self._dedupe_preserve_order(
                [
                    self._clean_interface_name(interface)
                    for interface in interfaces
                    if isinstance(interface, str) and interface.strip()
                ]
            )

        methods = normalized.get("methods")
        if isinstance(methods, list):
            normalized["methods"] = self._dedupe_preserve_order(
                [
                    self._clean_method_name(method_name)
                    if isinstance(method_name, str)
                    else method_name
                    for method_name in methods
                ]
            )
        elif isinstance(methods, dict):
            name_map: dict[str, str] = {}
            cleaned_methods: dict[str, object] = {}
            for raw_name, method_payload in methods.items():
                cleaned_name = self._clean_method_name(raw_name)
                name_map[str(raw_name)] = cleaned_name
                if isinstance(method_payload, dict):
                    cleaned_payload = dict(method_payload)
                    if isinstance(cleaned_payload.get("name"), str):
                        cleaned_payload["name"] = self._clean_method_name(cleaned_payload["name"])
                    cleaned_methods[cleaned_name] = cleaned_payload
                else:
                    cleaned_methods[cleaned_name] = method_payload
            normalized["methods"] = cleaned_methods

            for field_name in ("inputs", "outputs", "error_cases"):
                field_value = normalized.get(field_name)
                if isinstance(field_value, dict):
                    normalized[field_name] = {
                        name_map.get(str(raw_name), self._clean_method_name(raw_name)): self._clean_api_surface_leaf_values(
                            value,
                            field_name,
                        )
                        for raw_name, value in field_value.items()
                    }

        for field_name in ("inputs", "outputs", "error_cases"):
            if field_name in normalized:
                normalized[field_name] = self._clean_api_surface_leaf_values(
                    normalized[field_name],
                    field_name,
                )

        return normalized

    def _clean_api_surface_leaf_values(self, value: object, field_name: str) -> object:
        """Normalize api-surface leaf identifier collections without changing their shape."""
        if isinstance(value, list):
            return self._dedupe_preserve_order(
                [
                    self._clean_api_surface_leaf_name(item, field_name)
                    if isinstance(item, str)
                    else item
                    for item in value
                ]
            )
        if isinstance(value, dict):
            return {
                self._clean_api_surface_leaf_name(key, field_name): (
                    self._clean_api_surface_leaf_values(nested, field_name)
                    if isinstance(nested, (list, dict))
                    else nested
                )
                for key, nested in value.items()
            }
        if isinstance(value, str):
            return self._clean_api_surface_leaf_name(value, field_name)
        return value

    def _clean_api_surface_leaf_name(self, value: str, field_name: str) -> str:
        """Normalize one api-surface field identifier."""
        if field_name == "error_cases":
            return self._clean_snake_name(value, default="error_case")
        return self._clean_snake_name(value, default="item")

    def _normalize_data_model_entities(self, entities: object) -> list:
        """Normalize DGCE entity names and drop redundant duplicates by canonical name."""
        if not isinstance(entities, list):
            return []

        cleaned_entities: list[object] = []
        seen_names: set[str] = set()
        for entity in entities:
            if isinstance(entity, dict):
                cleaned_entity = dict(entity)
                cleaned_name = self._clean_entity_name(entity.get("name"))
                cleaned_entity["name"] = cleaned_name
                cleaned_entity.setdefault("description", "")
                if cleaned_name in seen_names:
                    continue
                seen_names.add(cleaned_name)
                cleaned_entities.append(cleaned_entity)
            elif isinstance(entity, str) and entity.strip():
                cleaned_name = self._clean_entity_name(entity)
                if cleaned_name in seen_names:
                    continue
                seen_names.add(cleaned_name)
                cleaned_entities.append(cleaned_name)
            else:
                cleaned_entities.append(entity)
        return self._canonicalize_dgce_data_model_value(cleaned_entities)

    def _clean_entity_name(self, value: object) -> str:
        """Return a short class-like entity identifier."""
        if self._is_clean_class_name(value, ("artifact", "record", "input", "output", "gate", "stamp", "model", "entity")):
            return str(value)
        return self._clean_class_name(
            value,
            default="Item",
            suffix_mode="first",
            suffix_preferences=("artifact", "record", "input", "output", "gate", "stamp", "model", "entity"),
            max_words=3,
        )

    def _clean_interface_name(self, value: object) -> str:
        """Return a short class-like interface identifier."""
        if self._is_clean_class_name(value, ("service", "api", "gateway", "client", "interface")):
            return str(value)
        return self._clean_class_name(
            value,
            default="Interface",
            suffix_mode="last",
            suffix_preferences=("service", "api", "gateway", "client", "interface"),
            max_words=3,
        )

    def _clean_method_name(self, value: object) -> str:
        """Return a stable snake_case api method identifier."""
        return self._clean_snake_name(value, default="method")

    def _is_clean_class_name(self, value: object, suffix_preferences: tuple[str, ...]) -> bool:
        """Return True when an identifier is already a concise class-like name."""
        if not isinstance(value, str):
            return False
        trimmed = value.strip()
        if not trimmed or not re.fullmatch(r"[A-Z][A-Za-z0-9]*", trimmed):
            return False
        tokens = [token.lower() for token in self._split_identifier_tokens(trimmed)]
        suffix_count = sum(1 for token in tokens if token in suffix_preferences)
        return suffix_count <= 1

    def _clean_class_name(
        self,
        value: object,
        *,
        default: str,
        suffix_mode: str,
        suffix_preferences: tuple[str, ...],
        max_words: int,
    ) -> str:
        """Normalize noisy model identifiers into concise PascalCase names."""
        tokens = self._semantic_tokens(value)
        if not tokens:
            return default

        chosen_tokens = tokens
        suffix_token: str | None = None
        if suffix_mode == "last":
            for preferred_suffix in suffix_preferences:
                if preferred_suffix in tokens:
                    suffix_token = preferred_suffix
                    suffix_index = max(
                        index for index, token in enumerate(tokens) if token == preferred_suffix
                    )
                    chosen_tokens = tokens[max(0, suffix_index - (max_words - 1)) : suffix_index + 1]
                    break
        else:
            for index, token in enumerate(tokens):
                if token in suffix_preferences:
                    suffix_token = token
                    chosen_tokens = tokens[: index + 1]
                    break

        base_tokens = [token for token in chosen_tokens if token not in suffix_preferences]
        if not base_tokens and suffix_token is None:
            base_tokens = tokens[:max_words]
        max_base_words = max_words - (1 if suffix_token else 0)
        if max_base_words > 0 and len(base_tokens) > max_base_words:
            base_tokens = base_tokens[:max_base_words]

        final_tokens = list(base_tokens)
        if suffix_token:
            final_tokens.append(suffix_token)
        elif not final_tokens:
            final_tokens = tokens[:max_words]

        return "".join(token.capitalize() for token in final_tokens) or default

    def _clean_snake_name(self, value: object, *, default: str) -> str:
        """Normalize noisy model identifiers into concise snake_case names."""
        tokens = self._semantic_tokens(value)
        if not tokens:
            return default
        return "_".join(tokens[:5]) or default

    def _semantic_tokens(self, value: object) -> list[str]:
        """Extract stable semantic tokens from noisy identifier-like text."""
        if not isinstance(value, str):
            return []

        parts = self._split_identifier_tokens(value)
        filtered: list[str] = []
        for part in parts:
            lowered = part.lower()
            if (
                not lowered
                or lowered in self._SEMANTIC_NAME_STOPWORDS
                or lowered in self._SEMANTIC_METADATA_STOPWORDS
                or lowered.isdigit()
            ):
                continue
            filtered.append(lowered)

        if not filtered:
            filtered = [part.lower() for part in parts if part and not part.isdigit()]

        deduped: list[str] = []
        for token in filtered:
            if not deduped or deduped[-1] != token:
                deduped.append(token)
        return deduped

    def _split_identifier_tokens(self, value: str) -> list[str]:
        """Split mixed identifier text into approximate semantic words."""
        spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
        spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
        base_tokens = [token for token in re.split(r"[^A-Za-z0-9]+", spaced) if token]

        expanded: list[str] = []
        for token in base_tokens:
            expanded.extend(self._split_compound_lower_token(token))
        return expanded

    def _split_compound_lower_token(self, token: str) -> list[str]:
        """Best-effort split for long lowercase compound tokens such as datamodelservice."""
        lowered = token.lower()
        if token != lowered or len(token) < 8:
            return [token]

        result: list[str] = []
        index = 0
        vocabulary = sorted(self._SEMANTIC_VOCABULARY, key=len, reverse=True)
        while index < len(lowered):
            match = next((word for word in vocabulary if lowered.startswith(word, index)), None)
            if match is None:
                return [token]
            result.append(match)
            index += len(match)
        return result or [token]

    def _dedupe_preserve_order(self, items: object) -> object:
        """Remove duplicate list items while preserving their first occurrence."""
        if not isinstance(items, list):
            return items

        deduped: list[object] = []
        seen: set[str] = set()
        for item in items:
            key = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else repr(item)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _canonicalize_dgce_data_model_value(self, value: object, *, parent_key: str = "") -> object:
        """Return a deterministically ordered DGCE data-model payload value."""
        if isinstance(value, dict):
            normalized = {
                key: self._canonicalize_dgce_data_model_value(item, parent_key=str(key))
                for key, item in value.items()
            }
            return dict(sorted(normalized.items(), key=lambda entry: str(entry[0])))

        if isinstance(value, list):
            normalized_items = [
                self._canonicalize_dgce_data_model_value(item, parent_key=parent_key)
                for item in value
            ]
            if all(isinstance(item, str) for item in normalized_items):
                return (
                    normalized_items
                    if parent_key in self._ORDERED_SEMANTIC_STRING_LIST_KEYS
                    else sorted(normalized_items)
                )
            if all(isinstance(item, dict) for item in normalized_items):
                return sorted(
                    normalized_items,
                    key=lambda item: self._dgce_data_model_sort_key(parent_key, item),
                )
            return normalized_items

        return value

    def _dgce_data_model_sort_key(self, parent_key: str, item: dict) -> tuple[str, ...]:
        """Return the stable ordering key for one DGCE data-model object."""
        order = str(item.get("order", ""))
        if parent_key == "entities":
            return (
                order,
                str(item.get("name", "")),
                str(item.get("storage_path", "")),
            )
        if parent_key == "fields":
            return (
                order,
                str(item.get("name", "")),
                str(item.get("type", "")),
            )
        if parent_key == "relationships":
            return (
                order,
                str(item.get("from_entity", item.get("entity_1", ""))),
                str(item.get("to_entity", item.get("entity_2", ""))),
                str(item.get("relationship_type", item.get("type", ""))),
            )
        if parent_key in {"validation_rules", "determinism_rules", "status_model"}:
            return (
                order,
                str(item.get("name", item.get("status", ""))),
                str(item.get("rule", "")),
            )
        return (
            order,
            str(item.get("name", "")),
            json.dumps(item, sort_keys=True),
        )

    def _validate_dgce_data_model_payload(self, payload: dict) -> Optional[ValidationResult]:
        """Accept nested entity field definitions for DGCE data-model outputs."""
        assert "interfaces" not in {
            "modules",
            "entities",
            "fields",
            "relationships",
            "validation_rules",
        }
        modules = payload.get("modules")
        if not isinstance(modules, list) or not modules:
            return ValidationResult(ok=False, missing_keys=["modules"], error="missing_keys")

        module_missing_keys: list[str] = []
        for module in modules:
            if not isinstance(module, dict):
                return ValidationResult(ok=False, missing_keys=["modules"], error="missing_keys")
            for key in ("name", "entities", "relationships", "required", "identity_keys"):
                if key not in module:
                    module_missing_keys.append(key)
                    continue
                if key == "name":
                    if not isinstance(module.get(key), str) or not module.get(key):
                        module_missing_keys.append(key)
                elif not isinstance(module.get(key), list):
                    module_missing_keys.append(key)
        if module_missing_keys:
            return ValidationResult(
                ok=False,
                missing_keys=sorted(set(module_missing_keys)),
                error="missing_keys",
            )

        entities = payload.get("entities")
        if "entities" not in payload or not isinstance(entities, list):
            return None

        entity_dicts = [entity for entity in entities if isinstance(entity, dict)]
        if not entity_dicts:
            return None

        if len(entity_dicts) != len(entities):
            return ValidationResult(ok=False, missing_keys=["fields"], error="missing_keys")

        missing_keys: list[str] = []
        for entity in entity_dicts:
            if "name" not in entity:
                missing_keys.append("name")
            has_fields = "fields" in entity and isinstance(entity.get("fields"), list)
            has_methods = "methods" in entity and isinstance(entity.get("methods"), list)
            has_description = "description" in entity and isinstance(entity.get("description"), str)
            if not has_fields and not has_methods and not has_description:
                missing_keys.append("fields")
        if missing_keys:
            return ValidationResult(
                ok=False,
                missing_keys=sorted(set(missing_keys)),
                error="missing_keys",
            )

        missing_keys = [
            key
            for key in ("relationships", "validation_rules")
            if key not in payload
        ]
        if missing_keys:
            return ValidationResult(ok=False, missing_keys=missing_keys, error="missing_keys")

        return ValidationResult(ok=True, missing_keys=[], error=None)

    def _validate_dgce_api_surface_payload(self, payload: dict) -> Optional[ValidationResult]:
        """Accept DGCE api-surface payloads without crossing into other DGCE schema checks."""
        required_keys = ("interfaces", "methods", "inputs", "outputs", "error_cases")
        missing_keys = [key for key in required_keys if key not in payload]
        if missing_keys:
            return ValidationResult(ok=False, missing_keys=missing_keys, error="missing_keys")

        interfaces = payload.get("interfaces")
        if not isinstance(interfaces, list) or not interfaces:
            return ValidationResult(ok=False, missing_keys=["interfaces"], error="missing_keys")

        methods = payload.get("methods")
        if not isinstance(methods, (list, dict)):
            return ValidationResult(ok=False, missing_keys=["methods"], error="missing_keys")

        for key in ("inputs", "outputs", "error_cases"):
            if not isinstance(payload.get(key), (list, dict)):
                return ValidationResult(ok=False, missing_keys=[key], error="missing_keys")

        return ValidationResult(ok=True, missing_keys=[], error=None)

    def _validate_schema_specific_structured_payload(
        self,
        schema_name: str,
        parsed: dict,
    ) -> tuple[dict, ValidationResult | None]:
        """Normalize and validate DGCE schemas without cross-schema fallback."""
        if schema_name == "dgce_system_breakdown_v1":
            normalized = self._normalize_system_breakdown_payload(schema_name, parsed)
            return normalized, self._validate_dgce_system_breakdown_payload(normalized)

        if schema_name == "dgce_data_model_v1":
            normalized = self._normalize_dgce_data_model_payload(schema_name, parsed)
            normalized = self._repair_dgce_data_model_payload(normalized)
            validation = self._validate_dgce_data_model_payload(normalized)
            if validation is not None:
                assert "interfaces" not in validation.missing_keys
            return normalized, validation

        if schema_name == "dgce_api_surface_v1":
            normalized = self._normalize_api_surface_payload(schema_name, parsed)
            return normalized, self._validate_dgce_api_surface_payload(normalized)

        return parsed, None

    def _validate_structured_output(
        self,
        request: ClassificationRequest,
        output: str,
    ) -> tuple[dict, Optional[dict]]:
        """Validate structured executor output without interrupting the request path."""
        schema_name = request.output_contract.schema_name if request.output_contract else None
        if not schema_name:
            return ({}, None)

        sanitized_output = self._normalize_set_like_methods_blocks(
            self._strip_json_line_comments(
                self._strip_markdown_fences(output)
            )
        )
        if not sanitized_output.strip():
            empty_payload = self._empty_structured_payload(request, schema_name)
            if empty_payload is not None:
                return ({"structure_valid": True}, empty_payload)

        parsed = self._parse_structured_json_output(
            output,
            allow_fallback_extraction=schema_name != "dgce_data_model_v1",
        )
        if parsed is None:
            return (
                {
                    "structure_valid": False,
                    "structure_error": "invalid_json",
                },
                None,
            )

        if schema_name == "dgce_system_breakdown_v1":
            parsed = self._repair_system_breakdown_payload(parsed)

        validation = (
            self._validate_dgce_system_breakdown_payload(parsed)
            if schema_name == "dgce_system_breakdown_v1"
            else None
        )
        if validation is not None and not validation.ok:
            return (
                {
                    "structure_valid": False,
                    "structure_error": validation.error or "validation_failed",
                    "structure_missing_keys": validation.missing_keys,
                },
                None,
            )

        parsed, validation = self._validate_schema_specific_structured_payload(schema_name, parsed)
        metadata = request.metadata or {}
        if schema_name == "dgce_data_model_v1" and metadata.get("section_type") == "data_model":
            parsed = self._apply_dgce_core_data_model_contract(parsed)
            validation = self._validate_dgce_data_model_payload(parsed)
            if validation is not None:
                assert "interfaces" not in validation.missing_keys
        if validation is None:
            validation = validate_output(schema_name, parsed)
        if not validation.ok:
            return (
                {
                    "structure_valid": False,
                    "structure_error": validation.error or "validation_failed",
                    "structure_missing_keys": validation.missing_keys,
                },
                None,
            )

        return ({"structure_valid": True}, parsed)

    def _empty_structured_payload(
        self,
        request: ClassificationRequest,
        schema_name: str,
    ) -> Optional[dict]:
        """Allow intentionally empty structured output for select DGCE section contracts."""
        metadata = request.metadata or {}
        if (
            schema_name == "dgce_data_model_v1"
            and metadata.get("section_type") == "data_model"
            and not metadata.get("require_non_empty_structured_output")
        ):
            return {
                "modules": [],
                "entities": [],
                "fields": [],
                "relationships": [],
                "validation_rules": [],
            }
        return None
