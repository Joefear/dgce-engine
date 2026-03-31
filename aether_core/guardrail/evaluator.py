"""Guardrail evaluator that combines policies and safe matching."""

from typing import Dict, List, Any, Optional
from pathlib import Path
from enum import Enum
from dataclasses import dataclass

from aether_core.guardrail.policy_loader import PolicyLoader, GuardrailPolicy
from aether_core.guardrail.matchers import ContentMatcher
from aether_core.classifier.rules import TaskBucket


class GuardrailDecision(str, Enum):
    """Guardrail evaluation decisions."""

    ALLOW = "allow"
    REVIEW = "review"
    BLOCK = "block"


@dataclass
class EvaluationResult:
    """Result of guardrail evaluation.

    Attributes:
        decision: ALLOW, REVIEW, or BLOCK
        policy_path: Path to policy that made the decision
        matched_rule_ids: List of rule IDs that matched
        explanation: Human-readable explanation
        confidence: Confidence in the decision (0.0-1.0)
    """

    decision: GuardrailDecision
    policy_path: str
    matched_rule_ids: List[str]
    explanation: str
    confidence: float


class GuardrailEvaluator:
    """Evaluates content against guardrail policies.

    Evaluation order:
    1. Global policies (highest priority first)
    2. Bucket-specific policies (if task bucket known)

    Decision is final - no overrides in Phase 1.
    """

    def __init__(self, policies_dir: Optional[Path] = None):
        """Initialize guardrail evaluator.

        Args:
            policies_dir: Directory containing policy files.
        """
        self.policy_loader = PolicyLoader(policies_dir)
        self.content_matcher = ContentMatcher()

    def evaluate(
        self,
        content: str,
        task_bucket: Optional[TaskBucket] = None,
        classifier_confidence: Optional[str] = None
    ) -> EvaluationResult:
        """Evaluate content against guardrail policies.

        Args:
            content: Content to evaluate.
            task_bucket: Task bucket classification (if known).
            classifier_confidence: Classifier confidence level.

        Returns:
            EvaluationResult with decision and details.
        """
        # Low confidence is a Phase 1 review gate and takes precedence.
        if classifier_confidence == "low":
            return EvaluationResult(
                decision=GuardrailDecision.REVIEW,
                policy_path="default/low_confidence",
                matched_rule_ids=[],
                explanation="Low classifier confidence requires review",
                confidence=0.6,
            )

        # 1. Check global policies first
        global_result = self._evaluate_global_policies(content)
        if global_result:
            return global_result

        # 2. Check bucket-specific policies if bucket known
        if task_bucket:
            bucket_result = self._evaluate_bucket_policies(content, task_bucket)
            if bucket_result:
                return bucket_result

        # 3. Apply Phase 1 default logic
        return self._apply_default_logic(content, task_bucket, classifier_confidence)

    def _evaluate_global_policies(self, content: str) -> Optional[EvaluationResult]:
        """Evaluate against global policies."""
        global_policies = self.policy_loader.get_global_policies()

        for policy in global_policies:
            result = self._evaluate_single_policy(content, policy)
            if result:
                return result

        return None

    def _evaluate_bucket_policies(self, content: str, bucket: TaskBucket) -> Optional[EvaluationResult]:
        """Evaluate against bucket-specific policies."""
        bucket_policies = self.policy_loader.get_bucket_policies(bucket.value)

        for policy in bucket_policies:
            result = self._evaluate_single_policy(content, policy)
            if result:
                return result

        return None

    def _evaluate_single_policy(self, content: str, policy: GuardrailPolicy) -> Optional[EvaluationResult]:
        """Evaluate content against a single policy."""
        match_result = self.content_matcher.evaluate_rules(content, policy.rules)

        if not match_result["matched"]:
            return None

        # Determine decision based on matched rules and their actions
        decision = self._determine_decision_from_matches(match_result, policy)

        return EvaluationResult(
            decision=decision,
            policy_path=f"policies/{policy.policy_id}.yaml",
            matched_rule_ids=match_result["matched_rules"],
            explanation=f"Matched policy '{policy.name}': {policy.description}",
            confidence=0.9,  # High confidence for explicit policy matches
        )

    def _determine_decision_from_matches(
        self,
        match_result: Dict[str, Any],
        policy: GuardrailPolicy
    ) -> GuardrailDecision:
        """Determine decision based on matched rules and their actions."""
        # Find the most severe action from matched rules
        most_severe_decision = GuardrailDecision.ALLOW

        for rule_id in match_result["matched_rules"]:
            # Find the rule in the policy
            for rule in policy.rules:
                if rule.get("id") == rule_id:
                    action = rule.get("action", "review")  # Default to review if no action
                    try:
                        decision = GuardrailDecision(action.lower())
                        # Update if this is more severe
                        if self._decision_severity(decision) > self._decision_severity(most_severe_decision):
                            most_severe_decision = decision
                    except ValueError:
                        # Invalid action, default to review
                        if self._decision_severity(GuardrailDecision.REVIEW) > self._decision_severity(most_severe_decision):
                            most_severe_decision = GuardrailDecision.REVIEW

        return most_severe_decision

    @staticmethod
    def _decision_severity(decision: GuardrailDecision) -> int:
        """Get numeric severity for guardrail decision."""
        severity_map = {
            GuardrailDecision.ALLOW: 0,
            GuardrailDecision.REVIEW: 1,
            GuardrailDecision.BLOCK: 2,
        }
        return severity_map.get(decision, 0)

    def _apply_default_logic(
        self,
        content: str,
        task_bucket: Optional[TaskBucket],
        classifier_confidence: Optional[str]
    ) -> EvaluationResult:
        """Apply Phase 1 default evaluation logic when no policies match."""

        # Required test case logic from constraints:
        # 1. High sensitivity content -> REVIEW (basic check)
        if self._check_high_sensitivity(content):
            return EvaluationResult(
                decision=GuardrailDecision.REVIEW,
                policy_path="default/high_sensitivity",
                matched_rule_ids=[],
                explanation="High sensitivity content requires review",
                confidence=0.8,
            )

        # 2. Destructive tool usage -> REVIEW before any allow logic
        if self._contains_destructive_tools(content):
            return EvaluationResult(
                decision=GuardrailDecision.REVIEW,
                policy_path="default/destructive_tools",
                matched_rule_ids=[],
                explanation="Destructive tool usage requires review",
                confidence=0.9,
            )

        # 3. Planning default -> ALLOW (MID_MODEL implied)
        if task_bucket == TaskBucket.PLANNING:
            return EvaluationResult(
                decision=GuardrailDecision.ALLOW,
                policy_path="default/planning_allow",
                matched_rule_ids=[],
                explanation="Planning tasks allowed by default",
                confidence=0.7,
            )

        # 4. Deterministic small planning -> ALLOW (RESOLVE_LOCAL implied)
        if self._is_deterministic_small_planning(content):
            return EvaluationResult(
                decision=GuardrailDecision.ALLOW,
                policy_path="default/small_planning",
                matched_rule_ids=[],
                explanation="Small deterministic planning tasks allowed",
                confidence=0.8,
            )

        # Default: ALLOW with low confidence
        return EvaluationResult(
            decision=GuardrailDecision.ALLOW,
            policy_path="default/allow",
            matched_rule_ids=[],
            explanation="Content allowed by default policy",
            confidence=0.5,
        )

    def _check_high_sensitivity(self, content: str) -> bool:
        """Check for high sensitivity content."""
        sensitive_keywords = [
            "password", "secret", "token", "key", "credential",
            "personal", "private", "confidential"
        ]
        content_lower = content.lower()
        return any(kw in content_lower for kw in sensitive_keywords)

    def _is_deterministic_small_planning(self, content: str) -> bool:
        """Check if content represents small deterministic planning."""
        small_indicators = [
            "simple", "basic", "small", "quick", "easy",
            "straightforward", "deterministic"
        ]
        planning_indicators = ["plan", "step", "approach", "method"]

        content_lower = content.lower()
        has_small = any(ind in content_lower for ind in small_indicators)
        has_planning = any(ind in content_lower for ind in planning_indicators)

        return has_small and has_planning

    def _contains_destructive_tools(self, content: str) -> bool:
        """Check for destructive tool usage."""
        destructive_keywords = [
            "delete", "remove", "destroy", "drop", "truncate",
            "format", "wipe", "erase", "kill", "terminate"
        ]
        content_lower = content.lower()
        return any(kw in content_lower for kw in destructive_keywords)
