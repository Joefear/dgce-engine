"""Minimal YAML-backed preset loader for Aether request shaping."""

from pathlib import Path
from typing import Dict

_PRESET_CACHE: Dict[str, Dict[str, str]] | None = None


def load_presets() -> Dict[str, Dict[str, str]]:
    """Load request presets from the bundled YAML registry."""
    global _PRESET_CACHE
    if _PRESET_CACHE is None:
        path = _preset_path()
        if not path.exists():
            _PRESET_CACHE = {}
        else:
            try:
                _PRESET_CACHE = _parse_simple_yaml(path)
            except OSError:
                _PRESET_CACHE = {}
    return _PRESET_CACHE


def get_preset(name: str | None) -> Dict[str, str]:
    """Return a single preset mapping or an empty dict if missing."""
    if not name:
        return {}
    return dict(load_presets().get(name, {}))


def _preset_path() -> Path:
    """Resolve the bundled preset registry path."""
    return Path(__file__).resolve().parents[2] / "registries" / "presets.yaml"


def _parse_simple_yaml(path: Path) -> Dict[str, Dict[str, str]]:
    """Parse the small preset registry YAML subset used by Aether."""
    presets: Dict[str, Dict[str, str]] = {}
    current_name: str | None = None
    current_parent: str | None = None

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip()
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            indent = len(line) - len(line.lstrip(" "))

            if indent == 0:
                current_name = stripped[:-1]
                presets[current_name] = {}
                current_parent = None
                continue

            if current_name is None:
                continue

            key, value = stripped.split(":", 1)
            key = key.strip()
            value = _parse_scalar(value)

            if indent == 2:
                if value is None:
                    presets[current_name][key] = {}
                    current_parent = key
                else:
                    presets[current_name][key] = value
                    current_parent = None
                continue

            if indent == 4 and current_parent is not None:
                parent = presets[current_name].setdefault(current_parent, {})
                if isinstance(parent, dict):
                    parent[key] = value

    return presets


def _parse_scalar(value: str) -> str | None:
    """Parse the tiny scalar subset needed for preset YAML."""
    parsed = value.strip()
    if parsed == "":
        return None
    if (parsed.startswith('"') and parsed.endswith('"')) or (
        parsed.startswith("'") and parsed.endswith("'")
    ):
        return parsed[1:-1]
    if parsed.lower() == "null":
        return None
    return parsed
