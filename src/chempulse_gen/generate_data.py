from __future__ import annotations

from chempulse_gen.generators import (
    generate_chemical_mdm,
    generate_lab_result,
    generate_many,
    generate_material_movement,
    generate_sensor_reading,
)
from chempulse_gen.utils import write_jsonl
from chempulse_gen.validator import validate_records


def main() -> None:
    sensor_events = generate_many(generate_sensor_reading, 200)
    lab_events = generate_many(generate_lab_result, 100)
    movement_events = generate_many(generate_material_movement, 80)
    chemical_events = generate_many(generate_chemical_mdm, 20)

    valid_sensor, invalid_sensor = validate_records(
        sensor_events,
        "chem.sensor_readings.v1.json",
    )
    valid_lab, invalid_lab = validate_records(
        lab_events,
        "chem.lab_results.v1.json",
    )
    valid_movement, invalid_movement = validate_records(
        movement_events,
        "chem.material_movements.v1.json",
    )
    valid_chemical, invalid_chemical = validate_records(
        chemical_events,
        "chem.chemical_mdm.v1.json",
    )

    write_jsonl("data/raw/chem.sensor_readings.v1.jsonl", valid_sensor)
    write_jsonl("data/raw/chem.lab_results.v1.jsonl", valid_lab)
    write_jsonl("data/raw/chem.material_movements.v1.jsonl", valid_movement)
    write_jsonl("data/raw/chem.chemical_mdm.v1.jsonl", valid_chemical)

    write_jsonl("data/invalid/chem.sensor_readings.v1.invalid.jsonl", invalid_sensor)
    write_jsonl("data/invalid/chem.lab_results.v1.invalid.jsonl", invalid_lab)
    write_jsonl("data/invalid/chem.material_movements.v1.invalid.jsonl", invalid_movement)
    write_jsonl("data/invalid/chem.chemical_mdm.v1.invalid.jsonl", invalid_chemical)

    print("Generated and validated ChemPulse event files:")
    print(f" - sensor_readings: valid={len(valid_sensor)}, invalid={len(invalid_sensor)}")
    print(f" - lab_results: valid={len(valid_lab)}, invalid={len(invalid_lab)}")
    print(f" - material_movements: valid={len(valid_movement)}, invalid={len(invalid_movement)}")
    print(f" - chemical_mdm: valid={len(valid_chemical)}, invalid={len(invalid_chemical)}")


if __name__ == "__main__":
    main()