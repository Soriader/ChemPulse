from __future__ import annotations

from chempulse_gen.generators import (
    generate_chemical_mdm,
    generate_lab_result,
    generate_many,
    generate_material_movement,
    generate_sensor_reading,
)
from chempulse_gen.utils import write_jsonl


def main() -> None:
    sensor_events = generate_many(generate_sensor_reading, 200)
    lab_events = generate_many(generate_lab_result, 100)
    movement_events = generate_many(generate_material_movement, 80)
    chemical_events = generate_many(generate_chemical_mdm, 20)

    write_jsonl("data/raw/chem.sensor_readings.v1.jsonl", sensor_events)
    write_jsonl("data/raw/chem.lab_results.v1.jsonl", lab_events)
    write_jsonl("data/raw/chem.material_movements.v1.jsonl", movement_events)
    write_jsonl("data/raw/chem.chemical_mdm.v1.jsonl", chemical_events)

    print("Generated ChemPulse raw event files:")
    print(f" - sensor_readings: {len(sensor_events)}")
    print(f" - lab_results: {len(lab_events)}")
    print(f" - material_movements: {len(movement_events)}")
    print(f" - chemical_mdm: {len(chemical_events)}")


if __name__ == "__main__":
    main()