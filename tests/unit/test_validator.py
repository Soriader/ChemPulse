from __future__ import annotations

from chempulse_gen.validator import validate_records


def test_validate_records_detects_invalid_sensor_quality_flag() -> None:
    valid_record = {
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

    invalid_record = {
        "event_id": "evt-002",
        "event_ts": "2026-03-16T18:00:00+00:00",
        "ingestion_ts": "2026-03-16T18:01:00+00:00",
        "source_system": "scada",
        "equipment_id": "EQ-002",
        "sensor_id": "SNS-002",
        "batch_id": "BATCH-002",
        "metric_name": "temperature",
        "metric_value": 30.1,
        "metric_unit": "C",
        "quality_flag": "BROKEN",
    }

    valid_records, invalid_records = validate_records(
        [valid_record, invalid_record],
        "chem.sensor_readings.v1.json",
    )

    assert len(valid_records) == 1
    assert len(invalid_records) == 1
    assert valid_records[0]["event_id"] == "evt-001"
    assert invalid_records[0]["record"]["event_id"] == "evt-002"
    assert any("is not one of" in error for error in invalid_records[0]["errors"])