"""File-backed artifact store for Aether Phase 1.5."""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from aether_core.classifier.rules import TaskBucket
from aether_core.enums import ArtifactStatus


@dataclass
class StoredArtifact:
    """Stored artifact record."""

    artifact_id: str
    task_bucket: str
    content: str
    output: str
    status: str
    context: dict
    created_at: str
    structured_content: Optional[dict] = None


class ArtifactStore:
    """Append-only JSONL artifact store with manual promotion."""

    def __init__(self, store_path: Optional[Path] = None):
        self.store_path = store_path or Path("data/artifacts.jsonl")
        self.store_path.parent.mkdir(parents=True, exist_ok=True)

    def store_artifact(
        self,
        artifact_id: str,
        task_bucket: TaskBucket | str,
        content: str,
        output: str,
        status: ArtifactStatus,
        context: Optional[dict] = None,
        structured_content: Optional[dict] = None,
    ) -> StoredArtifact:
        """Store an artifact record."""
        record = StoredArtifact(
            artifact_id=artifact_id,
            task_bucket=task_bucket.value if isinstance(task_bucket, TaskBucket) else str(task_bucket),
            content=content,
            output=output,
            status=status.value,
            context=context or {},
            structured_content=structured_content,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        with open(self.store_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), sort_keys=True) + "\n")

        return record

    def promote_to_approved(self, artifact_id: str) -> StoredArtifact:
        """Promote an existing artifact to approved_artifact."""
        records = self.read_artifacts()

        for record in records:
            if record.artifact_id == artifact_id:
                record.status = ArtifactStatus.APPROVED.value
                self._rewrite(records)
                return record

        raise ValueError(f"Artifact not found: {artifact_id}")

    def read_artifacts(self) -> list[StoredArtifact]:
        """Read all stored artifacts."""
        if not self.store_path.exists():
            return []

        records = []
        with open(self.store_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                records.append(StoredArtifact(**json.loads(line)))
        return records

    def _rewrite(self, records: list[StoredArtifact]) -> None:
        """Rewrite the store file with updated records."""
        tmp = self.store_path.with_suffix(".tmp")

        with open(tmp, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(asdict(record), sort_keys=True) + "\n")

        tmp.replace(self.store_path)
