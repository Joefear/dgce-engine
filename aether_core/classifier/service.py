"""Main classification service for Aether Phase 1."""

from typing import Dict, Any
from datetime import datetime, timezone

from aether_core.models import ClassificationRequest, ClassificationResponse
from aether_core.models.classifier import RuleBasedClassifier
from aether_core.classifier.rules import ClassifierRules
from aether_core.guardrail.evaluator import GuardrailEvaluator, EvaluationResult, GuardrailDecision
from aether_core.enums import ArtifactStatus


class ClassificationService:
    """Phase 1 classification service combining rule-based and task bucket classification.

    Integrates:
    - GuardrailEvaluator for Phase 1 guardrail checking
    - ClassifierRules for task bucket classification
    """

    def __init__(self):
        """Initialize classification service."""
        self.guardrail_classifier = RuleBasedClassifier()
        self.guardrail_evaluator = GuardrailEvaluator()
        self.task_classifier = ClassifierRules()

    def classify(self, request: ClassificationRequest) -> ClassificationResponse:
        """Classify content through guardrails and task bucketing.

        Args:
            request: ClassificationRequest with content to classify.

        Returns:
            ClassificationResponse with guardrail and task classification results.
        """
        start_time = datetime.now(timezone.utc)

        # Step 1: Phase 1 retains fail-closed blocking for critical guardrail matches.
        classifier_result = self.guardrail_classifier.classify(request.content)
        if classifier_result.is_blocked:
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            return ClassificationResponse(
                request_id=request.request_id,
                status=ArtifactStatus.BLOCKED,
                content=request.content,
                output="",
                explanation=f"Blocked by guardrail: {classifier_result.explanation}",
                processing_time_ms=processing_time,
            )

        # Step 2: Task bucket classification provides context for guardrail evaluation
        task_result = self.task_classifier.classify(request.content)

        # Step 3: Guardrail evaluation runs on the live classification path
        guardrail_result = self.guardrail_evaluator.evaluate(
            request.content,
            task_bucket=task_result["bucket"],
            classifier_confidence=task_result["confidence"].value,
        )

        # Step 4: Combine results into final response
        final_status, output, explanation = self._combine_results(
            guardrail_result, task_result, request.content
        )

        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

        return ClassificationResponse(
            request_id=request.request_id,
            status=final_status,
            content=request.content,
            output=output,
            explanation=explanation,
            processing_time_ms=processing_time,
        )

    def _combine_results(
        self,
        guardrail_result: EvaluationResult,
        task_result: Dict[str, Any],
        content: str
    ) -> tuple[ArtifactStatus, str, str]:
        """Combine guardrail and task classification results.

        Returns:
            Tuple of (status, output_content, explanation)
        """
        if guardrail_result.decision == GuardrailDecision.BLOCK:
            return (
                ArtifactStatus.BLOCKED,
                "",  # No output for blocked content
                f"Blocked by guardrail: {guardrail_result.explanation}"
            )

        bucket = task_result["bucket"]
        confidence = task_result["confidence"]
        explanation = (
            f"{guardrail_result.decision.value.title()} via {guardrail_result.policy_path}. "
            f"Classified as {bucket.value} with {confidence.value} confidence"
        )

        if guardrail_result.matched_rule_ids:
            explanation += f". Guardrail rules matched: {', '.join(guardrail_result.matched_rule_ids)}"

        if guardrail_result.decision == GuardrailDecision.REVIEW:
            return ArtifactStatus.EXPERIMENTAL, content, explanation

        return ArtifactStatus.APPROVED, content, explanation
