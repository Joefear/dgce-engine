"""Aether classifier components."""

from aether_core.classifier.rules import ClassifierRules, TaskBucket, ClassifierConfidence

__all__ = [
    "ClassifierRules",
    "TaskBucket",
    "ClassifierConfidence",
    "ClassificationService",
]


def __getattr__(name: str):
    """Lazily import service to avoid package-level circular imports."""
    if name == "ClassificationService":
        from aether_core.classifier.service import ClassificationService

        return ClassificationService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
