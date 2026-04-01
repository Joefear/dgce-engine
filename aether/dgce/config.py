"""Configuration helpers for DGCE deployment surfaces."""

from __future__ import annotations

import os


def get_config() -> dict[str, str | None]:
    return {
        "api_key": os.getenv("DGCE_API_KEY"),
    }
