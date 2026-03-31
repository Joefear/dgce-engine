"""Input request models for classification."""

from typing import Literal, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pydantic import BaseModel

from aether_core.contracts.schema_registry import SCHEMAS
from aether_core.presets.loader import get_preset

_DEFAULT_REUSE_SCOPE = "strict"


class OutputContract(BaseModel):
    """Optional structured-output contract resolved from a preset."""

    mode: Literal["freeform", "structured"] = "freeform"
    schema_name: Optional[str] = None


@dataclass
class ClassificationRequest:
    """Request to classify content against guardrails.
    
    Attributes:
        content: The text content to classify.
        request_id: Unique identifier for this request.
        timestamp: When the request was created.
        metadata: Optional additional context.
    """

    content: str
    request_id: str
    preset: Optional[str] = None
    project: Optional[str] = None
    task_type: Optional[str] = None
    priority: Optional[str] = None
    user: Optional[str] = None
    reuse_scope: Optional[str] = None
    system_hint: Optional[str] = None
    output_style: Optional[str] = None
    domain_hint: Optional[str] = None
    output_contract: Optional[OutputContract] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Optional[dict] = None

    def __post_init__(self):
        """Validate request fields."""
        self._apply_preset()
        if not self.content or not self.content.strip():
            raise ValueError("content cannot be empty")
        if not self.request_id or not self.request_id.strip():
            raise ValueError("request_id cannot be empty")

    def context_dict(self) -> dict:
        """Return merged request context including optional structured fields."""
        context = dict(self.metadata or {})
        if self.project is not None:
            context["project"] = self.project
        if self.task_type is not None:
            context["task_type"] = self.task_type
        if self.priority is not None:
            context["priority"] = self.priority
        if self.user is not None:
            context["user"] = self.user
        context["reuse_scope"] = self.reuse_scope_value()
        return context

    def reuse_scope_value(self) -> str:
        """Return a deterministic reuse scope."""
        if self.reuse_scope == "project":
            return "project"
        return _DEFAULT_REUSE_SCOPE

    def prompt_profile_value(self) -> str:
        """Return the deterministic prompt profile used for reuse separation."""
        if self.preset and self._has_prompt_scaffolds():
            return self.preset
        if self.is_structured_output():
            if self.output_contract and self.output_contract.schema_name:
                return f"structured:{self.output_contract.schema_name}"
            return "structured"
        return "default"

    def execution_prompt(self) -> str:
        """Return the deterministic prompt passed to executors."""
        hints: list[str] = []
        if self.domain_hint:
            hints.append(f"Domain hint: {self.domain_hint}")
        if self.output_style:
            hints.append(f"Output style: {self.output_style}")
        if self.system_hint:
            hints.append(f"System hint: {self.system_hint}")
        if self.is_structured_output():
            keys = ", ".join(self.required_output_keys())
            hints.append(
                f"You MUST return output in JSON format with the following top-level keys: {keys}"
            )
            hints.append("Do not include extra commentary outside the JSON.")

        if not hints:
            return self.content

        return "\n\n".join(
            [
                self.content,
                "Execution scaffolds:",
                *hints,
            ]
        )

    def _has_prompt_scaffolds(self) -> bool:
        """Return whether this request resolves any prompt-shaping inputs."""
        return any((self.domain_hint, self.output_style, self.system_hint)) or self.is_structured_output()

    def is_structured_output(self) -> bool:
        """Return whether this request asks the executor for structured JSON output."""
        return bool(self.output_contract and self.output_contract.mode == "structured")

    def required_output_keys(self) -> list[str]:
        """Return required top-level keys for structured output validation."""
        if not self.output_contract or not self.output_contract.schema_name:
            return []
        schema = SCHEMAS.get(self.output_contract.schema_name, {})
        keys = schema.get("required_keys", [])
        return list(keys) if isinstance(keys, list) else []

    def _apply_preset(self) -> None:
        """Apply preset defaults while allowing explicit request fields to win."""
        preset_values = get_preset(self.preset)
        for field_name, field_value in preset_values.items():
            if field_name == "output_contract" and self.output_contract is None:
                self.output_contract = OutputContract.model_validate(field_value)
                continue
            if hasattr(self, field_name) and getattr(self, field_name) is None:
                setattr(self, field_name, field_value)
