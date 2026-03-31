"""Safe content matchers for guardrail evaluation. No Python eval used."""

import re
from typing import List, Dict, Any, Optional, Set
from enum import Enum


class MatchType(str, Enum):
    """Types of content matching supported."""

    KEYWORD = "keyword"  # Exact keyword matching
    REGEX = "regex"      # Safe regex patterns
    PHRASE = "phrase"    # Exact phrase matching


class SafeMatcher:
    """Safe content matcher using controlled evaluation context.

    Supports:
    - Keyword matching (case-insensitive)
    - Safe regex patterns (pre-validated)
    - Exact phrase matching

    No arbitrary Python evaluation.
    """

    def __init__(self):
        """Initialize matcher with safe patterns."""
        self.allowed_regex_flags = re.IGNORECASE  # Only case-insensitive allowed

    def match_rule(self, content: str, rule_config: Dict[str, Any]) -> bool:
        """Match content against a rule configuration.

        Args:
            content: Text content to match against.
            rule_config: Rule configuration dict with 'type' and 'patterns'.

        Returns:
            True if content matches the rule.
        """
        match_type = rule_config.get("type", MatchType.KEYWORD)
        patterns = rule_config.get("patterns", [])

        if not patterns:
            return False

        if match_type == MatchType.KEYWORD:
            return self._match_keywords(content, patterns)
        elif match_type == MatchType.REGEX:
            return self._match_regex(content, patterns)
        elif match_type == MatchType.PHRASE:
            return self._match_phrases(content, patterns)
        else:
            # Unknown match type - no match
            return False

    def _match_keywords(self, content: str, keywords: List[str]) -> bool:
        """Match keywords (case-insensitive)."""
        content_lower = content.lower()
        return any(kw.lower() in content_lower for kw in keywords)

    def _match_regex(self, content: str, patterns: List[str]) -> bool:
        """Match safe regex patterns."""
        for pattern in patterns:
            try:
                # Only allow case-insensitive regex
                if re.search(pattern, content, self.allowed_regex_flags):
                    return True
            except re.error:
                # Invalid regex - skip this pattern
                continue
        return False

    def _match_phrases(self, content: str, phrases: List[str]) -> bool:
        """Match exact phrases (case-insensitive)."""
        content_lower = content.lower()
        return any(phrase.lower() in content_lower for phrase in phrases)

    def find_matches(self, content: str, rule_config: Dict[str, Any]) -> List[str]:
        """Find all matching patterns in content.

        Args:
            content: Text content to search.
            rule_config: Rule configuration.

        Returns:
            List of matched patterns.
        """
        match_type = rule_config.get("type", MatchType.KEYWORD)
        patterns = rule_config.get("patterns", [])
        matches = []

        if match_type == MatchType.KEYWORD:
            content_lower = content.lower()
            matches = [kw for kw in patterns if kw.lower() in content_lower]
        elif match_type == MatchType.PHRASE:
            content_lower = content.lower()
            matches = [phrase for phrase in patterns if phrase.lower() in content_lower]
        elif match_type == MatchType.REGEX:
            for pattern in patterns:
                try:
                    if re.search(pattern, content, self.allowed_regex_flags):
                        matches.append(pattern)
                except re.error:
                    continue

        return matches


class ContentMatcher:
    """High-level content matcher for guardrail rules."""

    def __init__(self):
        """Initialize content matcher."""
        self.safe_matcher = SafeMatcher()

    def evaluate_rules(self, content: str, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Evaluate content against a list of rules.

        Args:
            content: Content to evaluate.
            rules: List of rule configurations.

        Returns:
            Dict with 'matched', 'matched_rules', 'all_matches'
        """
        matched_rules = []
        all_matches = []

        for rule in rules:
            rule_id = rule.get("id", "unknown")
            if self.safe_matcher.match_rule(content, rule):
                matched_rules.append(rule_id)
                matches = self.safe_matcher.find_matches(content, rule)
                all_matches.extend(matches)

        return {
            "matched": len(matched_rules) > 0,
            "matched_rules": matched_rules,
            "all_matches": list(set(all_matches)),  # Deduplicate
        }