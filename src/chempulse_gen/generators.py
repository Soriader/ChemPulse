from __future__ import annotations

import random

from chempulse_gen.constants import (
    CHEMICALS,
    LAB_RESULT_STATUSES,
    LAB_TESTS,
    LOCATIONS,
    MATERIAL_TYPES,
    METRICS,
    MOVEMENT_STATUSES,
    MOVEMENT_TYPES,
    QUALITY_FLAGS,
    SOURCE_SYSTEMS,
)
from chempulse_gen.utils import generate_event_id, now_utc_iso, random_id, random_past_timestamp


def generate_sensor_reading() -> dict:
    metric_name, metric_unit = random.choice(METRICS)

    metric_ranges = {
        "temperature": (18.0, 95.0),
        "pressure": (0.8, 8.5),
        "ph": (1.0, 14.0),
        "conductivity": (50.0, 2500.0),
        "humidity": (20.0, 90.0),
    }

    min_val, max_val = metric_ranges[metric_name]
    metric_value = round(random.uniform(min_val, max_val), 2)

    quality_flag = random.choices(
        population=QUALITY_FLAGS,
        weights=[0.85, 0.10, 0.05],
        k=1,
    )[0]

    return {
        "event_id": generate_event_id(),
        "event_ts": random_past_timestamp(),
        "ingestion_ts": now_utc_iso(),
        "source_system": random.choice(["scada", "mes"]),
        "equipment_id": random_id("EQ"),
        "sensor_id": random_id("SNS"),
        "batch_id": random.choice([random_id("BATCH"), None]),
        "metric_name": metric_name,
        "metric_value": metric_value,
        "metric_unit": metric_unit,
        "quality_flag": quality_flag,
    }


def generate_lab_result() -> dict:
    test_code, result_unit, method_code = random.choice(LAB_TESTS)

    test_ranges = {
        "chloride_content": (1.0, 250.0),
        "sulfur_content": (5.0, 500.0),
        "density": (700.0, 1100.0),
        "viscosity": (1.0, 30.0),
        "ph": (2.0, 12.0),
    }

    min_val, max_val = test_ranges[test_code]
    result_value = round(random.uniform(min_val, max_val), 3)

    return {
        "event_id": generate_event_id(),
        "event_ts": random_past_timestamp(),
        "ingestion_ts": now_utc_iso(),
        "sample_id": random_id("SMP"),
        "batch_id": random.choice([random_id("BATCH"), None]),
        "lab_id": random_id("LAB", 1, 10),
        "test_code": test_code,
        "result_value": result_value,
        "result_unit": result_unit,
        "method_code": method_code,
        "analyst_id": random_id("ANL", 1, 50),
        "result_status": random.choices(
            population=LAB_RESULT_STATUSES,
            weights=[0.8, 0.05, 0.15],
            k=1,
        )[0],
        "quality_flag": random.choices(
            population=QUALITY_FLAGS,
            weights=[0.88, 0.08, 0.04],
            k=1,
        )[0],
    }


def generate_material_movement() -> dict:
    from_location = random.choice(LOCATIONS)
    to_location = random.choice([loc for loc in LOCATIONS if loc != from_location])

    quantity_unit = random.choice(["kg", "L", "pcs"])
    quantity = round(random.uniform(1.0, 500.0), 2)

    return {
        "event_id": generate_event_id(),
        "event_ts": random_past_timestamp(),
        "ingestion_ts": now_utc_iso(),
        "movement_id": random_id("MOV"),
        "material_id": random_id("MAT"),
        "material_type": random.choice(MATERIAL_TYPES),
        "batch_id": random.choice([random_id("BATCH"), None]),
        "from_location": from_location,
        "to_location": to_location,
        "quantity": quantity,
        "quantity_unit": quantity_unit,
        "movement_type": random.choice(MOVEMENT_TYPES),
        "operator_id": random_id("OP", 1, 40),
        "status": random.choices(
            population=MOVEMENT_STATUSES,
            weights=[0.85, 0.10, 0.05],
            k=1,
        )[0],
    }


def generate_chemical_mdm() -> dict:
    chemical_id, chemical_name, cas_number, hazard_class, default_unit, supplier_id = random.choice(CHEMICALS)

    return {
        "event_id": generate_event_id(),
        "event_ts": random_past_timestamp(hours_back=720),
        "ingestion_ts": now_utc_iso(),
        "chemical_id": chemical_id,
        "chemical_name": chemical_name,
        "cas_number": cas_number,
        "hazard_class": hazard_class,
        "default_unit": default_unit,
        "supplier_id": supplier_id,
        "is_active": random.choice([True, True, True, False]),
        "version": random.randint(1, 5),
    }


def generate_many(generator_func, n: int) -> list[dict]:
    return [generator_func() for _ in range(n)]