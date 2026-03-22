from __future__ import annotations

from typing import Any

from chempulse_consumer.validation import is_valid_event


def route_event(topic: str, event: dict[str, Any]) -> str:
    topic_name = topic.replace(".", "_")

    if is_valid_event(event):
        return f"data/consumed/valid/{topic_name}.jsonl"

    return f"data/consumed/invalid/{topic_name}.jsonl"