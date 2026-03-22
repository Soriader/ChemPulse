from __future__ import annotations

from typing import Any


def is_valid_event(event: dict[str, Any]) -> bool:
    return event.get("quality_flag") == "OK"