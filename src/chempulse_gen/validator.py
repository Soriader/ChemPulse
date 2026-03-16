from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = PROJECT_ROOT / "data_contracts"


def load_schema(schema_file: str) -> dict:
    schema_path = SCHEMA_DIR / schema_file

    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_records(records: list[dict], schema_file: str) -> tuple[list[dict], list[dict]]:
    valid_records: list[dict] = []
    invalid_records: list[dict] = []

    schema = load_schema(schema_file)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    for record in records:
        errors = sorted(validator.iter_errors(record), key=lambda e: list(e.path))

        if not errors:
            valid_records.append(record)
        else:
            invalid_records.append(
                {
                    "record": record,
                    "errors": [error.message for error in errors],
                }
            )

    return valid_records, invalid_records