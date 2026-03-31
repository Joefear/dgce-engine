"""Tests for the live Phase 1 classification service path."""

from aether_core.classifier.service import ClassificationService
from aether_core.classifier.rules import TaskBucket, ClassifierConfidence
from aether_core.guardrail.evaluator import EvaluationResult, GuardrailDecision
from aether_core.models import ClassificationRequest
from aether_core.enums import ArtifactStatus


class TestClassificationService:
    """Test service-level integration for Phase 1 classification."""

    def test_classify_uses_guardrail_evaluator(self):
        """GuardrailEvaluator should be used on the live classify path."""
        service = ClassificationService()
        request = ClassificationRequest(
            request_id="req_service_guardrail",
            content="safe content",
        )
        calls = {}

        def fake_task_classify(content: str):
            assert content == request.content
            return {
                "bucket": TaskBucket.CODE_ROUTINE,
                "confidence": ClassifierConfidence.HIGH,
                "matched_rules": ["code_001"],
                "explanation": "stub",
            }

        def fake_evaluate(content: str, task_bucket=None, classifier_confidence=None):
            calls["content"] = content
            calls["task_bucket"] = task_bucket
            calls["classifier_confidence"] = classifier_confidence
            return EvaluationResult(
                decision=GuardrailDecision.REVIEW,
                policy_path="test/policy",
                matched_rule_ids=["policy_rule"],
                explanation="forced review",
                confidence=0.9,
            )

        service.task_classifier.classify = fake_task_classify
        service.guardrail_evaluator.evaluate = fake_evaluate

        result = service.classify(request)

        assert calls == {
            "content": "safe content",
            "task_bucket": TaskBucket.CODE_ROUTINE,
            "classifier_confidence": "high",
        }
        assert result.status == ArtifactStatus.EXPERIMENTAL
        assert "Review via test/policy" in result.explanation
