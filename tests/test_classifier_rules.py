"""Tests for classifier rules."""

import pytest
from aether_core.classifier.rules import (
    ClassifierRules,
    TaskBucket,
    ClassifierConfidence,
    ClassificationRule,
)


class TestClassificationRule:
    """Test individual classification rules."""

    def test_rule_creation(self):
        """Test creating a classification rule."""
        rule = ClassificationRule(
            rule_id="test_001",
            bucket=TaskBucket.PLANNING,
            keywords=["plan", "strategy"],
            confidence=ClassifierConfidence.HIGH,
            description="Test rule",
        )
        assert rule.rule_id == "test_001"
        assert rule.bucket == TaskBucket.PLANNING
        assert "plan" in rule.keywords

    def test_rule_matching(self):
        """Test rule keyword matching."""
        rule = ClassificationRule(
            rule_id="test_001",
            bucket=TaskBucket.PLANNING,
            keywords=["plan", "strategy"],
            confidence=ClassifierConfidence.HIGH,
            description="Test rule",
        )

        assert rule.matches("I need to plan this project")
        assert rule.matches("STRATEGY is important")
        assert not rule.matches("This is just code")


class TestClassifierRules:
    """Test the classifier rules collection."""

    def test_initialization(self):
        """Test classifier rules initialization."""
        rules = ClassifierRules()
        assert len(rules.rules) > 0  # Has default rules

    def test_add_rule(self):
        """Test adding a custom rule."""
        rules = ClassifierRules()
        initial_count = len(rules.rules)

        new_rule = ClassificationRule(
            rule_id="custom_001",
            bucket=TaskBucket.CODE_ROUTINE,
            keywords=["custom"],
            confidence=ClassifierConfidence.MEDIUM,
            description="Custom rule",
        )
        rules.add_rule(new_rule)

        assert len(rules.rules) == initial_count + 1
        assert "custom_001" in rules.rules

    def test_classify_planning_content(self):
        """Test classification of planning content."""
        rules = ClassifierRules()
        result = rules.classify("I need to plan the system architecture")

        assert result["bucket"] == TaskBucket.PLANNING
        assert result["confidence"] in [ClassifierConfidence.HIGH, ClassifierConfidence.MEDIUM]
        assert len(result["matched_rules"]) > 0

    def test_classify_code_content(self):
        """Test classification of code content."""
        rules = ClassifierRules()
        result = rules.classify("Implement a function to calculate fibonacci")

        assert result["bucket"] == TaskBucket.CODE_ROUTINE
        assert result["confidence"] == ClassifierConfidence.HIGH
        assert len(result["matched_rules"]) > 0

    def test_classify_general_content(self):
        """Test classification of general content."""
        rules = ClassifierRules()
        result = rules.classify("How does Python work?")

        assert result["bucket"] == TaskBucket.GENERAL
        assert result["confidence"] == ClassifierConfidence.LOW

    def test_classify_no_matches(self):
        """Test classification when no rules match."""
        rules = ClassifierRules()
        result = rules.classify("xyzabc123")

        assert result["bucket"] == TaskBucket.GENERAL
        assert result["confidence"] == ClassifierConfidence.LOW
        assert len(result["matched_rules"]) == 0

    def test_confidence_precedence(self):
        """Test that higher confidence rules take precedence."""
        rules = ClassifierRules()

        # Add a low confidence rule that would match
        low_rule = ClassificationRule(
            rule_id="low_test",
            bucket=TaskBucket.GENERAL,
            keywords=["test"],
            confidence=ClassifierConfidence.LOW,
            description="Low confidence test",
        )
        rules.add_rule(low_rule)

        # Add a high confidence rule that also matches
        high_rule = ClassificationRule(
            rule_id="high_test",
            bucket=TaskBucket.PLANNING,
            keywords=["test"],
            confidence=ClassifierConfidence.HIGH,
            description="High confidence test",
        )
        rules.add_rule(high_rule)

        result = rules.classify("This is a test plan")
        assert result["confidence"] == ClassifierConfidence.HIGH
        assert result["bucket"] == TaskBucket.PLANNING