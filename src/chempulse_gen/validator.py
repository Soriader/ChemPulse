from __future__ import annotations

import json
from pathlib import Path

from jsonschema import ValidationError, validate


SCHEMA_DIR = Path("schemas")


def load_schema(schema_file: str) -> dict:
    schema_path = SCHEMA_DIR / schema_file

    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_record(record: dict, schema_file: str) -> None:
    schema = load_schema(schema_file)
    validate(instance=record, schema=schema)


def validate_records(records: list[dict], schema_file: str) -> tuple[list[dict], list[dict]]:
    valid_records: list[dict] = []
    invalid_records: list[dict] = []

    schema = load_schema(schema_file)

    for record in records:
        try:
            validate(instance=record, schema=schema)
            valid_records.append(record)
        except ValidationError as e:
            invalid_records.append(
                {
                    "record": record,
                    "error": e.message,
                }
            )

    return valid_records, invalid_records