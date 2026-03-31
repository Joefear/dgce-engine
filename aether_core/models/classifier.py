"""Rule-based classifier implementation for Phase 0/1."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime

from aether_core.enums import ArtifactStatus, ClassifierType, GuardrailLevel


@dataclass
class GuardrailRule:
    """A single rule in the rule-based classifier.
    
    Attributes:
        rule_id: Unique identifier for the rule.
        name: Human-readable rule name.
        keywords: List of keywords triggering this rule.
        level: Guardrail authority level (non-overridable in Phase 0/1).
        description: What this rule guards against.
    """

    rule_id: str
    name: str
    keywords: List[str]
    level: GuardrailLevel
    description: str

    def matches(self, content: str) -> bool:
        """Check if content matches this rule (case-insensitive)."""
        content_lower = content.lower()
        return any(kw.lower() in content_lower for kw in self.keywords)


@dataclass
class ClassificationResult:
    """Result of classification against all rules.
    
    Attributes:
        status: approved_artifact, experimental_output, or blocked.
        matched_rules: List of rule IDs that matched.
        highest_guardrail_level: Most severe guardrail level triggered.
        is_blocked: Whether output is blocked (non-overridable).
        explanation: Why content received this status.
    """

    status: ArtifactStatus
    matched_rules: List[str]
    highest_guardrail_level: GuardrailLevel
    is_blocked: bool
    explanation: str


class RuleBasedClassifier:
    """Phase 0/1 rule-based classifier. Non-overridable guardrails.
    
    Constraints:
    - Rule-based only (no ML)
    - Exact-match reuse of rules
    - Guardrail authority is non-overridable
    - approved_artifact and experimental_output remain distinct
    """

    def __init__(self):
        """Initialize classifier with default rules."""
        self.classifier_type = ClassifierType.RULE_BASED
        self.rules: Dict[str, GuardrailRule] = {}
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """Initialize Phase 0/1 default guardrail rules."""
        # Example minimal rules for Phase 0/1
        self.add_rule(
            GuardrailRule(
                rule_id="rule_001",
                name="Harmful Content",
                keywords=["harm", "violence", "illegal"],
                level=GuardrailLevel.CRITICAL,
                description="Blocks content promoting violence or illegal activity",
            )
        )
        self.add_rule(
            GuardrailRule(
                rule_id="rule_002",
                name="Personal Data",
                keywords=["ssn", "credit card", "password"],
                level=GuardrailLevel.HIGH,
                description="Blocks exposure of personal identifiable information",
            )
        )

    def add_rule(self, rule: GuardrailRule) -> None:
        """Add a new guardrail rule."""
        self.rules[rule.rule_id] = rule

    def classify(self, content: str) -> ClassificationResult:
        """Classify content against all rules (Phase 0/1 non-overridable).
        
        Args:
            content: Text to classify.
            
        Returns:
            ClassificationResult with status and matched rules.
        """
        matched_rule_ids = []
        highest_level = None

        # Check each rule in order
        for rule in self.rules.values():
            if rule.matches(content):
                matched_rule_ids.append(rule.rule_id)
                if highest_level is None or self._level_severity(rule.level) > self._level_severity(
                    highest_level
                ):
                    highest_level = rule.level

        # Determine final status (non-overridable in Phase 0/1)
        if highest_level is None:
            status = ArtifactStatus.APPROVED
            is_blocked = False
            explanation = "Content passed all guardrail rules."
        elif highest_level in (GuardrailLevel.CRITICAL, GuardrailLevel.HIGH):
            status = ArtifactStatus.BLOCKED
            is_blocked = True
            explanation = f"Content blocked by guardrail level: {highest_level.value}"
        else:
            # MEDIUM/LOW in Phase 0/1 still blocks (non-overridable)
            status = ArtifactStatus.EXPERIMENTAL
            is_blocked = False
            explanation = f"Content marked experimental due to guardrail level: {highest_level.value}"

        return ClassificationResult(
            status=status,
            matched_rules=matched_rule_ids,
            highest_guardrail_level=highest_level or GuardrailLevel.LOW,
            is_blocked=is_blocked,
            explanation=explanation,
        )

    @staticmethod
    def _level_severity(level: GuardrailLevel) -> int:
        """Get numeric severity for guardrail level."""
        severity_map = {
            GuardrailLevel.CRITICAL: 4,
            GuardrailLevel.HIGH: 3,
            GuardrailLevel.MEDIUM: 2,
            GuardrailLevel.LOW: 1,
        }
        return severity_map.get(level, 0)
