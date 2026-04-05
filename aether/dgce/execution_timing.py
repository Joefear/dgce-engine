"""Bounded execution timing helpers for the DGCE function-stub slice."""

from __future__ import annotations

from typing import Any


def duration_ms(start_ns: int, end_ns: int) -> float:
    """Return non-negative elapsed milliseconds from two perf_counter_ns readings."""
    if not isinstance(start_ns, int) or not isinstance(end_ns, int):
        raise ValueError("timing points must be int nanoseconds")
    if end_ns < start_ns:
        raise ValueError("end_ns must be greater than or equal to start_ns")
    return (end_ns - start_ns) / 1_000_000.0


def build_execution_timing(
    *,
    provider_duration_ms: float | None = None,
    validation_duration_ms: float | None = None,
    total_model_path_duration_ms: float | None = None,
) -> dict[str, Any] | None:
    """Build bounded audit-safe execution timing metadata."""
    payload: dict[str, Any] = {}
    if provider_duration_ms is not None:
        payload["provider_duration_ms"] = _normalize_duration(provider_duration_ms, "provider_duration_ms")
    if validation_duration_ms is not None:
        payload["validation_duration_ms"] = _normalize_duration(validation_duration_ms, "validation_duration_ms")
    if total_model_path_duration_ms is not None:
        payload["total_model_path_duration_ms"] = _normalize_duration(
            total_model_path_duration_ms,
            "total_model_path_duration_ms",
        )
    return payload or None


def _normalize_duration(value: float, field_name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be a float")
    normalized = float(value)
    if normalized < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return normalized
