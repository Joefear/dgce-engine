"""Test suite for Aether core models."""

import pytest
from datetime import datetime
from pathlib import Path

import aether_core.presets.loader as preset_loader
import aether_core.models.request as request_module
from aether_core.itera.exact_cache import ExactMatchCache
from aether_core.models import (
    ClassificationRequest,
    ClassificationResponse,
    RuleBasedClassifier,
    ClassificationResult,
    TelemetryEvent,
    LocalJSONLTelemetry,
)
from aether_core.enums import (
    ArtifactStatus,
    GuardrailLevel,
    ClassifierType,
    TelemetryEventType,
)


def _test_tmp_path(name: str) -> Path:
    base = Path("tests/.tmp") / name
    if base.exists():
        for path in sorted(base.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        if base.exists():
            base.rmdir()
    base.mkdir(parents=True, exist_ok=True)
    return base


class TestClassificationRequest:
    """Test ClassificationRequest model."""

    def test_valid_request(self):
        """Test creating a valid request."""
        req = ClassificationRequest(
            content="This is safe content", request_id="req_001"
        )
        assert req.content == "This is safe content"
        assert req.request_id == "req_001"
        assert isinstance(req.timestamp, datetime)

    def test_empty_content_raises_error(self):
        """Test that empty content raises ValueError."""
        with pytest.raises(ValueError, match="content cannot be empty"):
            ClassificationRequest(content="", request_id="req_001")

    def test_empty_request_id_raises_error(self):
        """Test that empty request_id raises ValueError."""
        with pytest.raises(ValueError, match="request_id cannot be empty"):
            ClassificationRequest(content="Safe content", request_id="")

    def test_preset_applies_default_fields(self):
        """Preset should populate structured request defaults."""
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_preset_001",
            preset="dgce_planning",
        )

        assert req.project == "DGCE"
        assert req.task_type == "planning"
        assert req.priority == "high"
        assert req.reuse_scope == "project"
        assert req.domain_hint == "Defiant Game Creation Engine internal planning"
        assert req.output_style == "Return a practical implementation slice with milestones, dependencies, and next coding steps."
        assert req.system_hint == "Prefer engine-building, pipeline design, modular implementation, and builder-oriented recommendations."
        assert req.output_contract is not None
        assert req.output_contract.mode == "structured"
        assert req.output_contract.schema_name == "dgce_planning_v1"

    def test_explicit_fields_override_preset(self):
        """Explicit request fields should win over preset defaults."""
        req = ClassificationRequest(
            content="Analyze this system",
            request_id="req_preset_002",
            preset="defiant_sky_analysis",
            priority="low",
            reuse_scope="project",
        )

        assert req.project == "DefiantSky"
        assert req.task_type == "sensor_fusion_analysis"
        assert req.priority == "low"
        assert req.reuse_scope == "project"

    def test_missing_preset_registry_does_not_crash(self, monkeypatch):
        """Missing preset registry should behave like no preset."""
        preset_loader._PRESET_CACHE = None
        monkeypatch.setattr(preset_loader, "_preset_path", lambda: Path("missing-presets.yaml"))

        req = ClassificationRequest(
            content="Safe content",
            request_id="req_preset_missing",
            preset="dgce_planning",
        )

        assert req.project is None
        assert req.task_type is None
        assert req.priority is None
        assert req.reuse_scope is None

    def test_unknown_preset_key_is_ignored_safely(self, monkeypatch):
        """Unknown preset keys should be skipped without crashing."""
        monkeypatch.setattr(
            request_module,
            "get_preset",
            lambda name: {"project": "DGCE", "unknown_key": "ignored"},
        )

        req = ClassificationRequest(
            content="Safe content",
            request_id="req_preset_unknown_key",
            preset="dgce_planning",
        )

        assert req.project == "DGCE"
        assert not hasattr(req, "unknown_key")

    def test_unknown_preset_name_falls_back_cleanly(self):
        """Unknown preset names should behave like no preset."""
        req = ClassificationRequest(
            content="Safe content",
            request_id="req_preset_unknown_name",
            preset="not_a_real_preset",
        )

        assert req.project is None
        assert req.task_type is None
        assert req.priority is None
        assert req.reuse_scope is None

    def test_execution_prompt_appends_preset_hints_deterministically(self):
        """Preset prompt scaffolds should be appended in a stable order."""
        preset_loader._PRESET_CACHE = None
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_001",
            preset="dgce_planning",
        )

        assert req.execution_prompt() == "\n\n".join(
            [
                "Plan this system",
                "Execution scaffolds:",
                "Domain hint: Defiant Game Creation Engine internal planning",
                "Output style: Return a practical implementation slice with milestones, dependencies, and next coding steps.",
                "System hint: Prefer engine-building, pipeline design, modular implementation, and builder-oriented recommendations.",
                "You MUST return output in JSON format with the following top-level keys: systems, modules, dependencies, implementation_steps",
                "Do not include extra commentary outside the JSON.",
            ]
        )

    def test_execution_prompt_without_hints_returns_original_content(self):
        """Requests without scaffold hints should keep the original prompt."""
        preset_loader._PRESET_CACHE = None
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_002",
        )

        assert req.execution_prompt() == "Plan this system"

    def test_structured_output_request_gets_non_default_prompt_profile(self):
        """Structured output shaping should produce a non-default prompt profile."""
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_profile_001",
            output_contract=request_module.OutputContract(
                mode="structured",
                schema_name="dgce_system_breakdown_v1",
            ),
        )

        assert req.prompt_profile_value() == "structured:dgce_system_breakdown_v1"

    def test_freeform_equivalent_request_remains_default_prompt_profile(self):
        """Freeform requests without prompt shaping should stay in the default profile."""
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_profile_002",
        )

        assert req.prompt_profile_value() == "default"

    def test_structured_and_freeform_requests_produce_different_cache_identity(self):
        """Structured prompt shaping should partition exact-match cache identity."""
        cache = ExactMatchCache(Path("tests/.tmp/prompt_profile_identity_cache.json"))
        structured = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_profile_003",
            output_contract=request_module.OutputContract(
                mode="structured",
                schema_name="dgce_system_breakdown_v1",
            ),
        )
        freeform = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_profile_004",
        )

        structured_key = cache.make_key(
            "planning",
            structured.content,
            cache.scope_context(
                {
                    **structured.context_dict(),
                    "prompt_profile": structured.prompt_profile_value(),
                },
                structured.reuse_scope_value(),
            ),
        )
        freeform_key = cache.make_key(
            "planning",
            freeform.content,
            cache.scope_context(
                {
                    **freeform.context_dict(),
                    "prompt_profile": freeform.prompt_profile_value(),
                },
                freeform.reuse_scope_value(),
            ),
        )

        assert structured_key != freeform_key

    def test_existing_preset_prompt_profile_behavior_does_not_regress(self):
        """Existing preset-linked shaping should still resolve to the preset name."""
        preset_loader._PRESET_CACHE = None
        req = ClassificationRequest(
            content="Plan this system",
            request_id="req_prompt_profile_005",
            preset="dgce_planning",
        )

        assert req.prompt_profile_value() == "dgce_planning"


class TestRuleBasedClassifier:
    """Test RuleBasedClassifier implementation."""

    def test_classifier_initialization(self):
        """Test classifier initializes with default rules."""
        clf = RuleBasedClassifier()
        assert clf.classifier_type == ClassifierType.RULE_BASED
        assert len(clf.rules) > 0  # Has default rules

    def test_classify_approved_content(self):
        """Test classification of safe content."""
        clf = RuleBasedClassifier()
        result = clf.classify("Hello, this is a normal greeting.")
        assert result.status == ArtifactStatus.APPROVED
        assert not result.is_blocked
        assert len(result.matched_rules) == 0

    def test_classify_blocked_content_critical(self):
        """Test classification of content matching CRITICAL rule."""
        clf = RuleBasedClassifier()
        result = clf.classify("This content promotes violence and harm.")
        assert result.status == ArtifactStatus.BLOCKED
        assert result.is_blocked
        assert result.highest_guardrail_level == GuardrailLevel.CRITICAL
        assert len(result.matched_rules) > 0

    def test_classify_blocked_content_high(self):
        """Test classification of content matching HIGH rule."""
        clf = RuleBasedClassifier()
        result = clf.classify("Here is a credit card number 1234-5678-9012-3456")
        assert result.status == ArtifactStatus.BLOCKED
        assert result.is_blocked
        assert result.highest_guardrail_level == GuardrailLevel.HIGH

    def test_add_custom_rule(self):
        """Test adding a custom guardrail rule."""
        clf = RuleBasedClassifier()
        from aether_core.models.classifier import GuardrailRule

        new_rule = GuardrailRule(
            rule_id="custom_001",
            name="Custom Rule",
            keywords=["custom_keyword"],
            level=GuardrailLevel.MEDIUM,
            description="A custom test rule",
        )
        clf.add_rule(new_rule)
        assert "custom_001" in clf.rules
        result = clf.classify("This has custom_keyword in it.")
        assert "custom_001" in result.matched_rules

    def test_guardrail_authority_non_overridable(self):
        """Test that guardrail decisions are non-overridable in Phase 0/1."""
        clf = RuleBasedClassifier()
        result = clf.classify("violence and harm content")
        # No mechanism to override in Phase 0/1
        assert result.is_blocked is True
        assert result.status == ArtifactStatus.BLOCKED


class TestClassificationResponse:
    """Test ClassificationResponse model."""

    def test_approved_response(self):
        """Test creating an approved response."""
        response = ClassificationResponse(
            request_id="req_001",
            status=ArtifactStatus.APPROVED,
            content="Safe content",
            output="Safe content",
            explanation="Passed all checks",
        )
        assert response.is_approved()
        assert not response.is_blocked()
        assert not response.is_experimental()

    def test_blocked_response(self):
        """Test creating a blocked response."""
        response = ClassificationResponse(
            request_id="req_002",
            status=ArtifactStatus.BLOCKED,
            content="Unsafe content",
            output="",  # Empty output when blocked
            explanation="Matched critical guardrail",
        )
        assert response.is_blocked()
        assert not response.is_approved()

    def test_experimental_response(self):
        """Test creating an experimental response."""
        response = ClassificationResponse(
            request_id="req_003",
            status=ArtifactStatus.EXPERIMENTAL,
            content="Content",
            output="Content",
            explanation="Flagged for review",
        )
        assert response.is_experimental()
        assert not response.is_approved()
        assert not response.is_blocked()


class TestTelemetry:
    """Test telemetry logging."""

    def test_telemetry_event_creation(self):
        """Test creating a telemetry event."""
        event = TelemetryEvent(
            event_type=TelemetryEventType.CLASSIFICATION_REQUEST,
            request_id="req_001",
            data={"content_length": 100},
        )
        assert event.request_id == "req_001"
        assert event.event_type == TelemetryEventType.CLASSIFICATION_REQUEST

    def test_telemetry_event_to_json_line(self):
        """Test converting event to JSON line."""
        event = TelemetryEvent(
            event_type=TelemetryEventType.CLASSIFICATION_RESULT,
            request_id="req_001",
            data={"status": "approved"},
        )
        json_line = event.to_json_line()
        assert "classification_result" in json_line
        assert "req_001" in json_line

    def test_local_jsonl_telemetry_write_and_read(self):
        """Test writing and reading JSONL telemetry."""
        log_file = _test_tmp_path("telemetry_write_and_read") / "test_telemetry.jsonl"
        telemetry = LocalJSONLTelemetry(log_path=log_file)

        # Write events
        event1 = TelemetryEvent(
            event_type=TelemetryEventType.CLASSIFICATION_REQUEST,
            request_id="req_001",
            data={"content": "test"},
        )
        event2 = TelemetryEvent(
            event_type=TelemetryEventType.CLASSIFICATION_RESULT,
            request_id="req_001",
            data={"status": "approved"},
        )
        telemetry.log_event(event1)
        telemetry.log_event(event2)

        # Read events
        events = telemetry.read_events()
        assert len(events) == 2
        assert events[0]["request_id"] == "req_001"
        assert events[1]["data"]["status"] == "approved"

    def test_local_jsonl_telemetry_clear(self):
        """Test clearing telemetry logs."""
        log_file = _test_tmp_path("telemetry_clear") / "test_telemetry.jsonl"
        telemetry = LocalJSONLTelemetry(log_path=log_file)

        event = TelemetryEvent(
            event_type=TelemetryEventType.GUARDRAIL_APPLIED,
            request_id="req_001",
        )
        telemetry.log_event(event)
        assert log_file.exists()

        telemetry.clear_logs()
        assert not log_file.exists()
