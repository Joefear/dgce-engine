from pathlib import Path
import importlib
import sys

import pytest


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from api.ingest import ingest_observation
from api.review import review_anomaly
from models.anomaly_record import AnomalyRecord
from models.rsobservation import RSObservation


def test_public_api_entrypoint_exports_ingest_observation() -> None:
    api_module = importlib.import_module("api")

    result = api_module.ingest_observation(
        {
            "object_id": "Rso-42",
            "timestamp": "2026-03-25T12:00:00Z",
            "position_eci": (1.0, 2.0, 3.0),
            "velocity_eci": (0.1, 0.2, 0.3),
            "sensor_source": "radar-alpha",
            "data_quality": 0.95,
        }
    )

    assert callable(api_module.ingest_observation)
    assert callable(api_module.review_anomaly)
    assert isinstance(result, dict)
    assert "anomaly_type" in result


@pytest.mark.parametrize(
    "payload",
    [
        {
            "object_id": "Rso-42",
            "timestamp": "2026-03-25T12:00:00Z",
            "position_eci": (1.0, 2.0, 3.0),
            "velocity_eci": (0.1, 0.2, 0.3),
            "sensor_source": "radar-alpha",
            "data_quality": 0.95,
        },
        RSObservation(
            object_id="Rso-42",
            timestamp="2026-03-25T12:00:00Z",
            position_eci=(1.0, 2.0, 3.0),
            velocity_eci=(0.1, 0.2, 0.3),
            sensor_source="radar-alpha",
            data_quality=0.95,
        ),
    ],
)
def test_public_consumer_can_call_ingest_observation_with_supported_inputs(payload: dict | RSObservation) -> None:
    result = ingest_observation(payload)

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


def test_ingest_observation_rejects_invalid_boundary_input_type() -> None:
    with pytest.raises(TypeError, match="payload must be a dict or RSObservation"):
        ingest_observation("not-an-observation")


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


def test_review_anomaly_accepts_valid_review_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DEFIANT_SKY_REVIEW_QUEUE_PATH", str(tmp_path / "review_queue.json"))
    monkeypatch.setenv("DEFIANT_SKY_AUDIT_TRAIL_PATH", str(tmp_path / "audit_trail.jsonl"))

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
