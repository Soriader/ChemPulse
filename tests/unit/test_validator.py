from __future__ import annotations

from chempulse_gen.validator import validate_records


def build_valid_sensor_record() -> dict:
    return {
        "event_id": "evt-001",
        "event_ts": "2026-03-16T18:00:00+00:00",
        "ingestion_ts": "2026-03-16T18:01:00+00:00",
        "source_system": "scada",
        "equipment_id": "EQ-001",
        "sensor_id": "SNS-001",
        "batch_id": "BATCH-001",
        "metric_name": "temperature",
        "metric_value": 25.3,
        "metric_unit": "C",
        "quality_flag": "OK",
    }


def test_validate_records_accepts_valid_sensor_record() -> None:
    record = build_valid_sensor_record()

    valid_records, invalid_records = validate_records(
        [record],
        "chem.sensor_readings.v1.json",
    )

    assert len(valid_records) == 1
    assert len(invalid_records) == 0


def test_validate_records_detects_invalid_quality_flag() -> None:
    record = build_valid_sensor_record()
    record["quality_flag"] = "BROKEN"

    valid_records, invalid_records = validate_records(
        [record],
        "chem.sensor_readings.v1.json",
    )

    assert len(valid_records) == 0
    assert len(invalid_records) == 1


def test_validate_records_detects_missing_required_field() -> None:
    record = build_valid_sensor_record()
    record.pop("sensor_id")

    valid_records, invalid_records = validate_records(
        [record],
        "chem.sensor_readings.v1.json",
    )

    assert len(valid_records) == 0
    assert len(invalid_records) == 1