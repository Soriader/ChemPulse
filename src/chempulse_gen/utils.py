from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, UTC
from pathlib import Path
from uuid import uuid4


def generate_event_id() -> str:
    return str(uuid4())


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def random_past_timestamp(hours_back: int = 72) -> str:
    now = datetime.now(UTC)
    delta = timedelta(
        hours=random.randint(0, hours_back),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (now - delta).isoformat()


def random_id(prefix: str, start: int = 1, end: int = 999) -> str:
    return f"{prefix}-{random.randint(start, end):03d}"


def write_jsonl(path: str | Path, records: list[dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")