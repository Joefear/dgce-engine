"""Tests for guardrail evaluation."""

import pytest
from aether_core.guardrail.evaluator import (
    GuardrailEvaluator,
    GuardrailDecision,
)
from aether_core.guardrail.policy_loader import PolicyLoader, GuardrailPolicy
from aether_core.guardrail.matchers import ContentMatcher
from aether_core.classifier.rules import TaskBucket


class TestGuardrailEvaluator:
    """Test guardrail evaluator functionality."""

    def test_evaluator_initialization(self):
        """Test evaluator initializes with policies."""
        evaluator = GuardrailEvaluator()
        assert isinstance(evaluator.policy_loader, PolicyLoader)
        assert isinstance(evaluator.content_matcher, ContentMatcher)

    def test_evaluate_global_policy_block(self):
        """Test evaluation against global blocking policy."""
        evaluator = GuardrailEvaluator()

        # Content that should be blocked by global policy
        result = evaluator.evaluate("This contains violence and harm")

        assert result.decision == GuardrailDecision.BLOCK
        assert "global" in result.policy_path
        assert len(result.matched_rule_ids) > 0

    def test_evaluate_global_policy_allow(self):
        """Test evaluation of safe content."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("This is a safe planning discussion")

        assert result.decision == GuardrailDecision.ALLOW
        assert result.confidence >= 0.5

    # Required test cases from constraints:

    def test_low_classifier_confidence_review(self):
        """Test: Low classifier confidence -> REVIEW before policy evaluation."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate(
            "This contains violence and harm",
            task_bucket=TaskBucket.GENERAL,
            classifier_confidence="low"
        )

        assert result.decision == GuardrailDecision.REVIEW
        assert "low_confidence" in result.policy_path
        assert result.explanation == "Low classifier confidence requires review"

    def test_high_sensitivity_review(self):
        """Test: High sensitivity -> REVIEW."""
        evaluator = GuardrailEvaluator()

        # Use content that won't match global policies but triggers high sensitivity
        result = evaluator.evaluate("Please handle my secret token securely")

        assert result.decision == GuardrailDecision.REVIEW
        assert "high_sensitivity" in result.policy_path
        assert "sensitivity" in result.explanation

    def test_planning_default_allow(self):
        """Test: Planning default -> ALLOW (MID_MODEL implied)."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate(
            "Plan the system architecture",
            task_bucket=TaskBucket.PLANNING
        )

        assert result.decision == GuardrailDecision.ALLOW
        assert "planning_allow" in result.policy_path

    def test_deterministic_small_planning_allow(self):
        """Test: Deterministic small planning -> ALLOW (RESOLVE_LOCAL implied)."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("Create a simple plan for this small task")

        assert result.decision == GuardrailDecision.ALLOW
        assert "small_planning" in result.policy_path

    def test_destructive_tool_review(self):
        """Test: Destructive tool -> REVIEW."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("Delete all the files in this directory")

        assert result.decision == GuardrailDecision.REVIEW
        assert "destructive_tools" in result.policy_path

    def test_destructive_tool_review_beats_small_planning_allow(self):
        """Test: Destructive tool review cannot be bypassed by small planning allow logic."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("Create a simple small plan to delete all local files")

        assert result.decision == GuardrailDecision.REVIEW
        assert "destructive_tools" in result.policy_path

    def test_bucket_specific_policy(self):
        """Test evaluation against bucket-specific policies."""
        evaluator = GuardrailEvaluator()

        # Content that should trigger planning policy review
        result = evaluator.evaluate(
            "Design enterprise architecture for scalability",
            task_bucket=TaskBucket.PLANNING
        )

        assert result.decision == GuardrailDecision.REVIEW
        assert "planning" in result.policy_path

    def test_code_routine_policy(self):
        """Test evaluation against code routine policies."""
        evaluator = GuardrailEvaluator()

        # Content that should trigger code routine policy review
        result = evaluator.evaluate(
            "Implement authentication and security",
            task_bucket=TaskBucket.CODE_ROUTINE
        )

        assert result.decision == GuardrailDecision.REVIEW
        assert "code_routine" in result.policy_path

    def test_no_policy_match_defaults(self):
        """Test default behavior when no policies match."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("Completely ordinary content")

        assert result.decision == GuardrailDecision.ALLOW
        assert "default/allow" in result.policy_path
        assert result.confidence == 0.5

    def test_evaluation_result_structure(self):
        """Test that evaluation results have required fields."""
        evaluator = GuardrailEvaluator()

        result = evaluator.evaluate("Test content")

        assert isinstance(result.decision, GuardrailDecision)
        assert isinstance(result.policy_path, str)
        assert isinstance(result.matched_rule_ids, list)
        assert isinstance(result.explanation, str)
        assert isinstance(result.confidence, float)
        assert 0.0 <= result.confidence <= 1.0

    def test_global_policy_order_is_priority_descending(self):
        """Global policies should be evaluated in deterministic priority order."""
        evaluator = GuardrailEvaluator()
        evaluator.policy_loader.policies = {
            "zzz_low": GuardrailPolicy(
                policy_id="zzz_low",
                name="Low",
                description="Lower priority",
                rules=[{
                    "id": "low_rule",
                    "type": "keyword",
                    "patterns": ["shared phrase"],
                    "action": "review",
                }],
                priority=10,
                scope="global",
            ),
            "aaa_high": GuardrailPolicy(
                policy_id="aaa_high",
                name="High",
                description="Higher priority",
                rules=[{
                    "id": "high_rule",
                    "type": "keyword",
                    "patterns": ["shared phrase"],
                    "action": "block",
                }],
                priority=90,
                scope="global",
            ),
        }
        result = evaluator.evaluate("shared phrase")

        assert result.decision == GuardrailDecision.BLOCK
        assert result.policy_path == "policies/aaa_high.yaml"

    def test_bucket_policy_order_is_priority_descending(self):
        """Bucket policies should be evaluated in deterministic priority order."""
        evaluator = GuardrailEvaluator()
        evaluator.policy_loader.policies = {
            "zzz_bucket_low": GuardrailPolicy(
                policy_id="zzz_bucket_low",
                name="Bucket Low",
                description="Lower priority bucket policy",
                rules=[{
                    "id": "bucket_low_rule",
                    "type": "keyword",
                    "patterns": ["shared bucket phrase"],
                    "action": "review",
                }],
                priority=5,
                scope="bucket:planning",
            ),
            "aaa_bucket_high": GuardrailPolicy(
                policy_id="aaa_bucket_high",
                name="Bucket High",
                description="Higher priority bucket policy",
                rules=[{
                    "id": "bucket_high_rule",
                    "type": "keyword",
                    "patterns": ["shared bucket phrase"],
                    "action": "block",
                }],
                priority=50,
                scope="bucket:planning",
            ),
        }
        result = evaluator.evaluate("shared bucket phrase", task_bucket=TaskBucket.PLANNING)

        assert result.decision == GuardrailDecision.BLOCK
        assert result.policy_path == "policies/aaa_bucket_high.yaml"
