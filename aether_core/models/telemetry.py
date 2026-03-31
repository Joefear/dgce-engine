"""Local JSONL telemetry logging for Phase 0/1."""

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from aether_core.enums import TelemetryEventType


@dataclass
class TelemetryEvent:
    """A single telemetry event logged to JSONL.
    
    Attributes:
        event_type: Type of event (request, result, guardrail action).
        request_id: Associated request ID.
        timestamp: When event occurred.
        data: Event-specific data dict.
    """

    event_type: TelemetryEventType
    request_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: dict = field(default_factory=dict)

    def to_json_line(self) -> str:
        """Convert to JSON line for JSONL output."""
        event_dict = asdict(self)
        # Convert timestamp to ISO format string
        event_dict["timestamp"] = self.timestamp.isoformat()
        event_dict["event_type"] = self.event_type.value
        return json.dumps(event_dict)


class LocalJSONLTelemetry:
    """Phase 0/1 local JSONL telemetry logger (no remote transport).
    
    Writes events to local JSONL file for analysis.
    """

    def __init__(self, log_path: Optional[Path] = None):
        """Initialize JSONL logger.
        
        Args:
            log_path: Path to JSONL log file. Defaults to ./aether_telemetry.jsonl
        """
        self.log_path = log_path or Path("aether_telemetry.jsonl")

    def log_event(self, event: TelemetryEvent) -> None:
        """Write event to JSONL file (append mode).
        
        Args:
            event: TelemetryEvent to log.
        """
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(event.to_json_line() + "\n")

    def read_events(self) -> list[dict]:
        """Read all logged events from JSONL file.
        
        Returns:
            List of event dictionaries.
        """
        if not self.log_path.exists():
            return []

        events = []
        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    events.append(json.loads(line))
        return events

    def clear_logs(self) -> None:
        """Clear telemetry log file."""
        if self.log_path.exists():
            self.log_path.unlink()
