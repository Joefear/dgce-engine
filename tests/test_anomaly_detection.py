import json
from pathlib import Path
import sys

import pytest


PROJECT_SRC = Path(__file__).resolve().parents[1] / "defiant-sky" / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from api.ingest import ingest_observation
from api.review import review_anomaly
from components.audit_trail_writer import AuditTrailWriter
from components.review_queue_manager import ReviewQueueManager
from models.anomaly_record import AnomalyRecord
from models.rsobservation import RSObservation


def _local_persistence_dir(name: str) -> Path:
    path = Path("tests/.tmp") / name
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_public_consumer_can_call_ingest_observation_with_dict_input() -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    result = ingest_observation(payload)

    assert isinstance(result, dict)
    assert "anomaly_type" in result
    assert "confidence" in result
    assert "deviation_score" in result
    assert "guardrail_cleared" in result
    assert "review_required" in result


def test_ingest_observation_accepts_rsobservation_instance_directly() -> None:
    observation = RSObservation(
        object_id="Rso-42",
        timestamp="2026-03-25T12:00:00Z",
        position_eci=(1.0, 2.0, 3.0),
        velocity_eci=(0.1, 0.2, 0.3),
        sensor_source="radar-alpha",
        data_quality=0.95,
    )
    result = ingest_observation(observation)
    assert isinstance(result, dict)
    assert "anomaly_type" in result
    assert "confidence" in result
    assert "deviation_score" in result
    assert "guardrail_cleared" in result
    assert "review_required" in result


def test_ingest_observation_returns_structured_pipeline_result() -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    result = ingest_observation(payload)

    assert isinstance(result, dict)
    assert "anomaly_type" in result
    assert "confidence" in result
    assert "deviation_score" in result


def test_ingest_observation_rejects_invalid_input_type() -> None:
    with pytest.raises(TypeError, match="payload must be a dict or RSObservation"):
        ingest_observation(42)


def test_ingest_observation_validates_dict_input_via_rsobservation() -> None:
    with pytest.raises(ValueError, match="object_id is required"):
        ingest_observation(
            {
                "object_id": "",
                "timestamp": "2026-03-25T12:00:00Z",
                "position_eci": (1.0, 2.0, 3.0),
                "velocity_eci": (0.1, 0.2, 0.3),
                "sensor_source": "radar-alpha",
                "data_quality": 0.95,
            }
        )


def test_ingest_observation_blocks_review_submission_for_low_confidence(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }
    submitted = {"called": False}

    monkeypatch.setattr("api.ingest.AnomalyClassifier.classify", lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.25})
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)

    def fake_submit(self, anomaly_record):
        submitted["called"] = True
        return {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True}

    monkeypatch.setattr("api.ingest.ReviewQueueManager.submit_for_review", fake_submit)

    result = ingest_observation(payload)

    assert result["review_required"] is True
    assert result["guardrail_cleared"] is True
    assert result["escalation_blocked"] is True
    assert result["review_ticket"] is None
    assert submitted["called"] is False


def test_ingest_observation_blocks_review_submission_for_degraded_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
        "degraded_mode": True,
    }
    submitted = {"called": False}

    monkeypatch.setattr("api.ingest.AnomalyClassifier.classify", lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9})
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)

    def fake_submit(self, anomaly_record):
        submitted["called"] = True
        return {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True}

    monkeypatch.setattr("api.ingest.ReviewQueueManager.submit_for_review", fake_submit)

    result = ingest_observation(payload)

    assert result["review_required"] is True
    assert result["guardrail_cleared"] is True
    assert result["escalation_blocked"] is True
    assert result["review_ticket"] is None
    assert submitted["called"] is False


def test_ingest_observation_submits_review_when_guardrail_allows(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr("api.ingest.AnomalyClassifier.classify", lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9})
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest.ReviewQueueManager.submit_for_review",
        lambda self, anomaly_record: {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True},
    )

    result = ingest_observation(payload)

    assert result["review_required"] is True
    assert result["guardrail_cleared"] is True
    assert result["escalation_blocked"] is False
    assert result["review_ticket"] == {"anomaly_id": "Rso-42:2026-03-25T12:00:00Z", "queued": True}


def test_ingest_observation_attaches_itera_advisory_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr(
        "api.ingest._build_itera_advisory",
        lambda anomaly_record, review_ticket: {
            "advisory_present": True,
            "advisory_type": "process_adjustment",
            "advisory_summary": "Review anomaly handling context",
            "advisory_context_key": anomaly_record.anomaly_id,
        },
    )

    result = ingest_observation(payload)

    assert result["advisory_present"] is True
    assert result["advisory_type"] == "process_adjustment"
    assert result["advisory_summary"] == "Review anomaly handling context"
    assert result["advisory_context_key"] == "Rso-42:2026-03-25T12:00:00Z"


def test_ingest_observation_swallows_itera_advisory_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    def fail_advisory(anomaly_record, review_ticket):
        raise RuntimeError("advisory unavailable")

    monkeypatch.setattr("api.ingest._build_itera_advisory", fail_advisory)

    result = ingest_observation(payload)

    assert result["advisory_present"] is False
    assert result["advisory_type"] is None
    assert result["advisory_summary"] is None
    assert result["advisory_context_key"] == "Rso-42:2026-03-25T12:00:00Z"


def test_itera_advisory_does_not_change_review_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr("api.ingest.AnomalyClassifier.classify", lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9})
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest.ReviewQueueManager.submit_for_review",
        lambda self, anomaly_record: {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True},
    )
    monkeypatch.setattr(
        "api.ingest._build_itera_advisory",
        lambda anomaly_record, review_ticket: {
            "advisory_present": True,
            "advisory_type": "policy_adjustment",
            "advisory_summary": "Advisory only",
            "advisory_context_key": anomaly_record.anomaly_id,
        },
    )

    result = ingest_observation(payload)

    assert result["review_ticket"] == {"anomaly_id": "Rso-42:2026-03-25T12:00:00Z", "queued": True}
    assert result["escalation_blocked"] is False
    assert result["advisory_present"] is True


def test_ingest_observation_end_to_end_allowed_path_response_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr(
        "api.ingest.AnomalyClassifier.classify",
        lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9},
    )
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest.ReviewQueueManager.submit_for_review",
        lambda self, anomaly_record: {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True},
    )
    monkeypatch.setattr(
        "api.ingest._build_itera_advisory",
        lambda anomaly_record, review_ticket: {
            "advisory_present": True,
            "advisory_type": "process_adjustment",
            "advisory_summary": "Review anomaly handling context",
            "advisory_context_key": anomaly_record.anomaly_id,
        },
    )

    result = ingest_observation(payload)

    assert result == {
        "anomaly_type": "RPO",
        "confidence": 0.9,
        "deviation_score": 0.0,
        "guardrail_cleared": True,
        "review_required": True,
        "escalation_blocked": False,
        "review_ticket": {"anomaly_id": "Rso-42:2026-03-25T12:00:00Z", "queued": True},
        "advisory_present": True,
        "advisory_type": "process_adjustment",
        "advisory_summary": "Review anomaly handling context",
        "advisory_context_key": "Rso-42:2026-03-25T12:00:00Z",
    }


def test_ingest_observation_end_to_end_low_confidence_blocked_response_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr(
        "api.ingest.AnomalyClassifier.classify",
        lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.25},
    )
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest._build_itera_advisory",
        lambda anomaly_record, review_ticket: {
            "advisory_present": True,
            "advisory_type": "process_adjustment",
            "advisory_summary": "Review anomaly handling context",
            "advisory_context_key": anomaly_record.anomaly_id,
        },
    )

    result = ingest_observation(payload)

    assert result["anomaly_type"] == "RPO"
    assert result["confidence"] == 0.25
    assert result["deviation_score"] == 0.0
    assert result["guardrail_cleared"] is True
    assert result["review_required"] is True
    assert result["escalation_blocked"] is True
    assert result["review_ticket"] is None
    assert result["advisory_present"] is True


def test_ingest_observation_end_to_end_degraded_mode_blocked_response_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
        "degraded_mode": True,
    }

    monkeypatch.setattr(
        "api.ingest.AnomalyClassifier.classify",
        lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9},
    )
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest._build_itera_advisory",
        lambda anomaly_record, review_ticket: {
            "advisory_present": True,
            "advisory_type": "process_adjustment",
            "advisory_summary": "Review anomaly handling context",
            "advisory_context_key": anomaly_record.anomaly_id,
        },
    )

    result = ingest_observation(payload)

    assert result["anomaly_type"] == "RPO"
    assert result["confidence"] == 0.9
    assert result["guardrail_cleared"] is True
    assert result["review_required"] is True
    assert result["escalation_blocked"] is True
    assert result["review_ticket"] is None
    assert result["advisory_present"] is True


def test_ingest_observation_end_to_end_advisory_failure_safe_response_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "object_id": "Rso-42",
        "timestamp": "2026-03-25T12:00:00Z",
        "position_eci": (1.0, 2.0, 3.0),
        "velocity_eci": (0.1, 0.2, 0.3),
        "sensor_source": "radar-alpha",
        "data_quality": 0.95,
    }

    monkeypatch.setattr(
        "api.ingest.AnomalyClassifier.classify",
        lambda self, observation, expected_state: {"anomaly_type": "RPO", "confidence": 0.9},
    )
    monkeypatch.setattr("api.ingest.GuardrailGateway.enforce_policy", lambda self, anomaly_record: True)
    monkeypatch.setattr(
        "api.ingest.ReviewQueueManager.submit_for_review",
        lambda self, anomaly_record: {"anomaly_id": anomaly_record.get("anomaly_id", ""), "queued": True},
    )

    def fail_advisory(anomaly_record, review_ticket):
        raise RuntimeError("advisory unavailable")

    monkeypatch.setattr("api.ingest._build_itera_advisory", fail_advisory)

    result = ingest_observation(payload)

    assert result["anomaly_type"] == "RPO"
    assert result["confidence"] == 0.9
    assert result["guardrail_cleared"] is True
    assert result["review_required"] is True
    assert result["escalation_blocked"] is False
    assert result["review_ticket"] == {"anomaly_id": "Rso-42:2026-03-25T12:00:00Z", "queued": True}
    assert result["advisory_present"] is False
    assert result["advisory_type"] is None
    assert result["advisory_summary"] is None
    assert result["advisory_context_key"] == "Rso-42:2026-03-25T12:00:00Z"


def test_review_anomaly_accepts_valid_review_status(monkeypatch: pytest.MonkeyPatch) -> None:
    persistence_dir = _local_persistence_dir("review_anomaly_accepted")
    monkeypatch.setenv("DEFIANT_SKY_REVIEW_QUEUE_PATH", str(persistence_dir / "review_queue.json"))
    monkeypatch.setenv("DEFIANT_SKY_AUDIT_TRAIL_PATH", str(persistence_dir / "audit_trail.jsonl"))

    anomaly_record = {
        "anomaly_id": "anom-001",
        "status": "pending",
    }

    result = review_anomaly(anomaly_record, "approved")

    assert isinstance(result, dict)
    assert result["anomaly_id"] == "anom-001"
    assert result["status"] == "approved"
    assert result["updated_record"]["status"] == "approved"


def test_review_anomaly_rejects_invalid_review_status() -> None:
    with pytest.raises(ValueError, match="status must be approved or rejected"):
        review_anomaly({"anomaly_id": "anom-002", "status": "pending"}, "pending")


def test_submit_for_review_persists_deterministic_record(monkeypatch: pytest.MonkeyPatch) -> None:
    queue_path = _local_persistence_dir("review_queue_persist") / "review_queue.json"
    monkeypatch.setenv("DEFIANT_SKY_REVIEW_QUEUE_PATH", str(queue_path))

    ticket = ReviewQueueManager().submit_for_review(
        {
            "anomaly_id": "anom-100",
            "object_id": "Rso-42",
            "status": "pending",
        }
    )

    payload = json.loads(queue_path.read_text(encoding="utf-8"))

    assert ticket == {
        "anomaly_id": "anom-100",
        "queued": True,
        "status": "pending",
    }
    assert payload["anom-100"]["anomaly_id"] == "anom-100"
    assert payload["anom-100"]["status"] == "pending"
    assert payload["anom-100"]["record"]["object_id"] == "Rso-42"


def test_review_anomaly_updates_persisted_status_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    persistence_dir = _local_persistence_dir("review_status_update")
    queue_path = persistence_dir / "review_queue.json"
    audit_path = persistence_dir / "audit_trail.jsonl"
    monkeypatch.setenv("DEFIANT_SKY_REVIEW_QUEUE_PATH", str(queue_path))
    monkeypatch.setenv("DEFIANT_SKY_AUDIT_TRAIL_PATH", str(audit_path))

    ReviewQueueManager().submit_for_review(
        {
            "anomaly_id": "anom-101",
            "object_id": "Rso-42",
            "status": "pending",
        }
    )

    result = review_anomaly(
        {
            "anomaly_id": "anom-101",
            "object_id": "Rso-42",
            "status": "pending",
        },
        "approved",
    )

    queue_payload = json.loads(queue_path.read_text(encoding="utf-8"))
    audit_lines = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert result["status"] == "approved"
    assert result["updated_record"]["status"] == "approved"
    assert queue_payload["anom-101"]["status"] == "approved"
    assert queue_payload["anom-101"]["record"]["status"] == "approved"
    assert audit_lines[-1]["anomaly_id"] == "anom-101"
    assert audit_lines[-1]["event_type"] == "review_status_updated"
    assert audit_lines[-1]["status"] == "approved"


def test_audit_trail_writer_writes_audit_data(monkeypatch: pytest.MonkeyPatch) -> None:
    audit_path = _local_persistence_dir("audit_writer") / "audit_trail.jsonl"
    monkeypatch.setenv("DEFIANT_SKY_AUDIT_TRAIL_PATH", str(audit_path))

    result = AuditTrailWriter().write_audit_event(
        {
            "anomaly_id": "anom-102",
            "event_type": "submitted_for_review",
            "status": "pending",
        }
    )

    lines = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert result == {
        "written": True,
        "anomaly_id": "anom-102",
        "event_type": "submitted_for_review",
    }
    assert lines[-1]["anomaly_id"] == "anom-102"
    assert lines[-1]["event_type"] == "submitted_for_review"
    assert lines[-1]["status"] == "pending"


@pytest.mark.parametrize(
    ("object_id", "timestamp", "message"),
    [
        ("", "2026-03-25T12:00:00Z", "object_id is required"),
        ("Rso-42", "", "timestamp is required"),
    ],
)
def test_rsobservation_validate_rejects_missing_required_fields(
    object_id: str,
    timestamp: str,
    message: str,
) -> None:
    observation = RSObservation(
        object_id=object_id,
        timestamp=timestamp,
        position_eci=(1.0, 2.0, 3.0),
        velocity_eci=(0.1, 0.2, 0.3),
        sensor_source="radar-alpha",
        data_quality=0.95,
    )

    with pytest.raises(ValueError, match=message):
        observation.validate()


def test_anomaly_record_validate_rejects_unsupported_status() -> None:
    record = AnomalyRecord(
        object_id="Rso-42",
        anomaly_id="anom-003",
        observation_id="obs-003",
        anomaly_type="unknown",
        deviation_score=0.0,
        confidence=0.0,
        status="reviewed",
    )

    with pytest.raises(ValueError, match="status must be a supported workflow value"):
        record.validate()


def test_anomaly_record_mark_reviewed_accepts_only_terminal_review_states() -> None:
    record = AnomalyRecord(
        object_id="Rso-42",
        anomaly_id="anom-004",
        observation_id="obs-004",
        anomaly_type="unknown",
        deviation_score=0.0,
        confidence=0.0,
        status="pending",
    )

    record.mark_reviewed("approved")
    assert record.status == "approved"

    with pytest.raises(ValueError, match="status must be approved or rejected"):
        record.mark_reviewed("pending")










































































