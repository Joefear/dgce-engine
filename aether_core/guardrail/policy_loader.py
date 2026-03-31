"""Policy loader for YAML-based guardrail policies."""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class GuardrailPolicy:
    """A guardrail policy loaded from YAML.

    Attributes:
        policy_id: Unique identifier for the policy.
        name: Human-readable policy name.
        description: Policy description.
        rules: List of rule dictionaries.
        priority: Priority level (higher = more important).
        scope: Policy scope (global, bucket-specific).
    """

    policy_id: str
    name: str
    description: str
    rules: List[Dict[str, Any]]
    priority: int
    scope: str


class PolicyLoader:
    """Loads and manages guardrail policies from YAML files."""

    def __init__(self, policies_dir: Optional[Path] = None):
        """Initialize policy loader.

        Args:
            policies_dir: Directory containing policy YAML files.
                         Defaults to ./policies
        """
        self.policies_dir = policies_dir or Path("policies")
        self.policies: Dict[str, GuardrailPolicy] = {}
        self._load_all_policies()

    def _load_all_policies(self) -> None:
        """Load all YAML policy files from policies directory."""
        if not self.policies_dir.exists():
            return

        load_errors = []

        for yaml_file in self.policies_dir.glob("*.yaml"):
            try:
                self._load_policy_file(yaml_file)
            except Exception as e:
                load_errors.append((yaml_file, e))

        if load_errors:
            details = "; ".join(f"{f}: {e}" for f, e in load_errors)
            raise RuntimeError(f"Failed to load guardrail policies: {details}")

    def _load_policy_file(self, file_path: Path) -> None:
        """Load a single policy YAML file."""
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # Validate policy structure
        if not isinstance(data, dict) or "policy" not in data:
            raise ValueError(f"Invalid policy structure in {file_path}")

        policy_data = data["policy"]

        policy = GuardrailPolicy(
            policy_id=policy_data["id"],
            name=policy_data["name"],
            description=policy_data.get("description", ""),
            rules=policy_data.get("rules", []),
            priority=policy_data.get("priority", 0),
            scope=policy_data.get("scope", "global"),
        )

        self.policies[policy.policy_id] = policy

    def get_policy(self, policy_id: str) -> Optional[GuardrailPolicy]:
        """Get a specific policy by ID."""
        return self.policies.get(policy_id)

    def get_policies_by_scope(self, scope: str) -> List[GuardrailPolicy]:
        """Get all policies for a specific scope."""
        return sorted(
            [p for p in self.policies.values() if p.scope == scope],
            key=lambda p: (-p.priority, p.policy_id),
        )

    def get_global_policies(self) -> List[GuardrailPolicy]:
        """Get all global policies."""
        return self.get_policies_by_scope("global")

    def get_bucket_policies(self, bucket: str) -> List[GuardrailPolicy]:
        """Get policies for a specific task bucket."""
        return self.get_policies_by_scope(f"bucket:{bucket}")

    def get_all_policies(self) -> List[GuardrailPolicy]:
        """Get all loaded policies sorted by priority (highest first)."""
        return sorted(self.policies.values(), key=lambda p: (-p.priority, p.policy_id))
