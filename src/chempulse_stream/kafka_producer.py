from __future__ import annotations

import json
from typing import Callable

from kafka import KafkaProducer

from chempulse_gen.generators import (
    generate_chemical_mdm,
    generate_lab_result,
    generate_material_movement,
    generate_sensor_reading,
)
from chempulse_gen.validator import validate_records


TOPIC_CONFIG: dict[str, tuple[Callable[[], dict], str]] = {
    "chem.sensor_readings.v1": (
        generate_sensor_reading,
        "chem.sensor_readings.v1.json",
    ),
    "chem.lab_results.v1": (
        generate_lab_result,
        "chem.lab_results.v1.json",
    ),
    "chem.material_movements.v1": (
        generate_material_movement,
        "chem.material_movements.v1.json",
    ),
    "chem.chemical_mdm.v1": (
        generate_chemical_mdm,
        "chem.chemical_mdm.v1.json",
    ),
}


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers="localhost:29092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def publish_event(producer: KafkaProducer, topic: str, event: dict) -> None:
    future = producer.send(topic, value=event)
    metadata = future.get(timeout=10)

    print(
        f"Published event to topic={metadata.topic}, "
        f"partition={metadata.partition}, offset={metadata.offset}"
    )


def publish_one_valid_event(producer: KafkaProducer, topic: str) -> None:
    generator_func, schema_file = TOPIC_CONFIG[topic]
    event = generator_func()

    valid_records, invalid_records = validate_records([event], schema_file)

    if invalid_records:
        print(f"Event rejected for topic={topic}: {invalid_records[0]['errors']}")
        return

    publish_event(producer, topic, valid_records[0])


def main() -> None:
    producer = create_producer()

    try:
        for topic in TOPIC_CONFIG:
            publish_one_valid_event(producer, topic)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()