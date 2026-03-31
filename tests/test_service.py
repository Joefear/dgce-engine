from aether_core.classifier.service import ClassificationService
from aether_core.models import ClassificationRequest
from aether_core.enums import ArtifactStatus


class TestClassificationService:

    def test_blocked_content_produces_blocked_status_with_empty_output(self):
        svc = ClassificationService()
        req = ClassificationRequest(content="violence and harm", request_id="t001")
        resp = svc.classify(req)
        assert resp.status == ArtifactStatus.BLOCKED
        assert resp.output == ""

    def test_low_confidence_content_produces_experimental(self):
        svc = ClassificationService()
        req = ClassificationRequest(content="how does this work", request_id="t002")
        resp = svc.classify(req)
        assert resp.status == ArtifactStatus.EXPERIMENTAL

    def test_approved_planning_content(self):
        svc = ClassificationService()
        req = ClassificationRequest(content="plan the architecture", request_id="t003")
        resp = svc.classify(req)
        assert resp.status == ArtifactStatus.APPROVED

    def test_destructive_content_is_not_approved(self):
        svc = ClassificationService()
        req = ClassificationRequest(content="simple plan to delete all records", request_id="t004")
        resp = svc.classify(req)
        assert resp.status != ArtifactStatus.APPROVED
