"""Output response models for classification."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from aether_core.enums import ArtifactStatus


@dataclass
class ClassificationResponse:
    """Response containing classification result and metadata.
    
    Attributes:
        request_id: Matched to original request.
        status: approved_artifact, experimental_output, or blocked.
        content: Original content that was classified.
        output: Processed output (empty if blocked).
        explanation: Why this status was assigned.
        timestamp: When classification occurred.
        processing_time_ms: Latency in milliseconds.
    """

    request_id: str
    status: ArtifactStatus
    content: str
    output: str  # Approved content or empty
    explanation: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time_ms: float = 0.0

    def is_approved(self) -> bool:
        """Check if artifact is approved."""
        return self.status == ArtifactStatus.APPROVED

    def is_experimental(self) -> bool:
        """Check if artifact is experimental."""
        return self.status == ArtifactStatus.EXPERIMENTAL

    def is_blocked(self) -> bool:
        """Check if artifact is blocked."""
        return self.status == ArtifactStatus.BLOCKED
