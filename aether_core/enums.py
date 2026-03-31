"""Enumerations for Aether core types."""

from enum import Enum


class ArtifactStatus(str, Enum):
    """Status of an artifact after classification."""

    APPROVED = "approved_artifact"
    EXPERIMENTAL = "experimental_output"
    BLOCKED = "blocked"


class GuardrailLevel(str, Enum):
    """Authority levels for guardrail enforcement."""

    CRITICAL = "critical"  # Non-overridable
    HIGH = "high"  # Non-overridable
    MEDIUM = "medium"  # Non-overridable in Phase 0/1
    LOW = "low"  # Non-overridable in Phase 0/1


class ClassifierType(str, Enum):
    """Classifier algorithm types."""

    RULE_BASED = "rule_based"  # Phase 0/1 only
    # MODEL_BASED would be Phase 2+


class TelemetryEventType(str, Enum):
    """Types of telemetry events."""

    CLASSIFICATION_REQUEST = "classification_request"
    CLASSIFICATION_RESULT = "classification_result"
    GUARDRAIL_APPLIED = "guardrail_applied"
    GUARDRAIL_BLOCKED = "guardrail_blocked"
