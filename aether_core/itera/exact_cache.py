"""Exact-match reuse cache for Aether Phase 1.5."""

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from aether_core.classifier.rules import TaskBucket
from aether_core.enums import ArtifactStatus


@dataclass
class ExactCacheResult:
    """Result of an exact cache lookup."""

    hit: bool
    content: Optional[str] = None


class ExactMatchCache:
    """File-backed exact-match cache for approved artifacts only."""

    def __init__(self, cache_path: Optional[Path] = None):
        self.cache_path = cache_path or Path("data/exact_cache.json")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

    def make_key(
        self,
        task_bucket: TaskBucket | str,
        content: str,
        context: Optional[dict] = None,
    ) -> str:
        """Build a stable exact-match key from bucket, content, and optional context."""
        bucket_value = task_bucket.value if isinstance(task_bucket, TaskBucket) else str(task_bucket)
        payload = {
            "task_bucket": bucket_value,
            "content": content,
            "context": self._key_context(context),
        }
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def scope_context(context: Optional[dict] = None, reuse_scope: str = "strict") -> dict:
        """Return the deterministic context used for exact-match keying."""
        base_context = ExactMatchCache._key_context(context)
        if reuse_scope == "project":
            project = base_context.get("project")
            if project is None:
                return base_context
            return {
                "project": project,
                "reuse_scope": "project",
                "prompt_profile": base_context.get("prompt_profile", "default"),
            }
        return base_context

    @staticmethod
    def _key_context(context: Optional[dict] = None) -> dict:
        """Normalize cache-key context fields with deterministic defaults."""
        key_context = dict(context or {})
        key_context.setdefault("prompt_profile", "default")
        return key_context

    def lookup(
        self,
        task_bucket: TaskBucket | str,
        content: str,
        context: Optional[dict] = None,
    ) -> ExactCacheResult:
        """Return a reuse hit only for approved artifacts."""
        key = self.make_key(task_bucket, content, context)
        cache_data = self._load_cache()
        record = cache_data.get(key)

        if not record:
            return ExactCacheResult(hit=False)

        if record.get("status") != ArtifactStatus.APPROVED.value:
            return ExactCacheResult(hit=False)

        return ExactCacheResult(hit=True, content=record.get("output"))

    def store(
        self,
        task_bucket: TaskBucket | str,
        content: str,
        output: str,
        status: ArtifactStatus,
        context: Optional[dict] = None,
    ) -> None:
        """Store an artifact result in the exact-match cache."""
        key = self.make_key(task_bucket, content, context)
        cache_data = self._load_cache()
        cache_data[key] = {
            "task_bucket": task_bucket.value if isinstance(task_bucket, TaskBucket) else str(task_bucket),
            "content": content,
            "context": context or {},
            "output": output,
            "status": status.value,
        }
        self._write_cache(cache_data)

    def _load_cache(self) -> dict:
        """Load cache contents from disk."""
        if not self.cache_path.exists():
            return {}

        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

        if not isinstance(data, dict):
            return {}

        return data

    def _write_cache(self, data: dict) -> None:
        """Persist cache contents to disk."""
        tmp = self.cache_path.with_suffix(".tmp")

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

        tmp.replace(self.cache_path)
