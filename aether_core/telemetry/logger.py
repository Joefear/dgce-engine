"""Resilient local JSONL telemetry logger for the Aether API."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class TelemetryLogger:
    """Best-effort JSONL telemetry logger that never raises to callers."""

    def __init__(self, log_path: Optional[Path] = None):
        self.log_path = log_path or Path("data/api_telemetry.jsonl")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_event(self, request_id: str, event_type: str, data: Optional[dict] = None) -> None:
        """Append a telemetry event, swallowing logging failures."""
        event = {
            "request_id": request_id,
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data or {},
        }

        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, sort_keys=True) + "\n")
        except (OSError, TypeError, ValueError):
            return

    def get_events(self, request_id: str) -> list[dict]:
        """Return all telemetry events for a specific request."""
        if not self.log_path.exists():
            return []

        events = []
        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    if event.get("request_id") == request_id:
                        events.append(event)
        except (OSError, json.JSONDecodeError):
            return []

        return events
