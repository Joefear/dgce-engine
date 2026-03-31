"""Rule definitions for Aether classifier."""

from dataclasses import dataclass
from typing import List, Dict, Any
from enum import Enum


class TaskBucket(str, Enum):
    """Task bucket classifications."""

    PLANNING = "planning"
    CODE_ROUTINE = "code_routine"
    GENERAL = "general"


class ClassifierConfidence(str, Enum):
    """Confidence levels for classification."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ClassificationRule:
    """A rule for classifying content into task buckets.

    Attributes:
        rule_id: Unique identifier for the rule.
        bucket: Task bucket this rule maps to.
        keywords: Keywords that trigger this rule.
        confidence: Confidence level of this rule.
        description: Human-readable description.
    """

    rule_id: str
    bucket: TaskBucket
    keywords: List[str]
    confidence: ClassifierConfidence
    description: str

    def matches(self, content: str) -> bool:
        """Check if content matches this rule (case-insensitive)."""
        content_lower = content.lower()
        return any(kw.lower() in content_lower for kw in self.keywords)


class ClassifierRules:
    """Collection of classification rules for task bucketing."""

    def __init__(self):
        """Initialize with default Phase 1 rules."""
        self.rules: Dict[str, ClassificationRule] = {}
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """Initialize default classification rules for Phase 1."""

        # Planning rules - high confidence
        self.add_rule(ClassificationRule(
            rule_id="planning_001",
            bucket=TaskBucket.PLANNING,
            keywords=["plan", "strategy", "roadmap", "design", "architecture"],
            confidence=ClassifierConfidence.HIGH,
            description="Content involves planning or strategic thinking",
        ))

        self.add_rule(ClassificationRule(
            rule_id="planning_002",
            bucket=TaskBucket.PLANNING,
            keywords=["break down", "step by step", "approach", "methodology"],
            confidence=ClassifierConfidence.MEDIUM,
            description="Content involves breaking down complex tasks",
        ))

        # Code routine rules - high confidence
        self.add_rule(ClassificationRule(
            rule_id="code_001",
            bucket=TaskBucket.CODE_ROUTINE,
            keywords=["implement", "code", "function", "class", "method"],
            confidence=ClassifierConfidence.HIGH,
            description="Content involves code implementation",
        ))

        self.add_rule(ClassificationRule(
            rule_id="code_002",
            bucket=TaskBucket.CODE_ROUTINE,
            keywords=["debug", "fix", "error", "bug", "test"],
            confidence=ClassifierConfidence.HIGH,
            description="Content involves debugging or fixing code",
        ))

        # General fallback rules - low confidence
        self.add_rule(ClassificationRule(
            rule_id="general_001",
            bucket=TaskBucket.GENERAL,
            keywords=["help", "how", "what", "explain"],
            confidence=ClassifierConfidence.LOW,
            description="General assistance requests",
        ))

    def add_rule(self, rule: ClassificationRule) -> None:
        """Add a classification rule."""
        self.rules[rule.rule_id] = rule

    def classify(self, content: str) -> Dict[str, Any]:
        """Classify content into task bucket with confidence.

        Returns:
            Dict with 'bucket', 'confidence', 'matched_rules', 'explanation'
        """
        matched_rules = []
        highest_confidence = None
        selected_bucket = None

        # Check rules in order (higher confidence rules first)
        for rule in self.rules.values():
            if rule.matches(content):
                matched_rules.append(rule.rule_id)

                # Select highest confidence match
                if (highest_confidence is None or
                    self._confidence_score(rule.confidence) > self._confidence_score(highest_confidence)):
                    highest_confidence = rule.confidence
                    selected_bucket = rule.bucket

        # Default to GENERAL if no matches
        if selected_bucket is None:
            selected_bucket = TaskBucket.GENERAL
            highest_confidence = ClassifierConfidence.LOW
            matched_rules = []

        return {
            "bucket": selected_bucket,
            "confidence": highest_confidence,
            "matched_rules": matched_rules,
            "explanation": f"Classified as {selected_bucket.value} with {highest_confidence.value} confidence",
        }

    @staticmethod
    def _confidence_score(confidence: ClassifierConfidence) -> int:
        """Get numeric score for confidence level."""
        scores = {
            ClassifierConfidence.HIGH: 3,
            ClassifierConfidence.MEDIUM: 2,
            ClassifierConfidence.LOW: 1,
        }
        return scores.get(confidence, 0)