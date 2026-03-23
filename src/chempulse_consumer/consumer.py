from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer

from chempulse_consumer.routing import route_event
from chempulse_consumer.validation import is_valid_event
from chempulse_storage.sql_server_writer import insert_event

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple Kafka consumer for ChemPulse")

    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic to consume from, e.g. chem.sensor_readings.v1",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--group-id",
        default="chempulse-consumer-group",
        help="Kafka consumer group id",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read messages from the beginning of the topic",
    )
    parser.add_argument(
        "--save-to-file",
        action="store_true",
        help="Save events into valid/invalid JSONL files",
    )
    parser.add_argument(
        "--event-field",
        help="Field name used for filtering, e.g. quality_flag or metric_name",
    )
    parser.add_argument(
        "--event-value",
        help="Field value used for filtering, e.g. OK or humidity",
    )

    return parser.parse_args()


def create_consumer(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    from_beginning: bool,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest" if from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def matches_filter(
    event: dict[str, Any],
    event_field: str | None,
    event_value: str | None,
) -> bool:
    if not event_field or event_value is None:
        return True

    return str(event.get(event_field)) == event_value


def save_event_to_file(event: dict[str, Any], file_path: str) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()

    consumer = create_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
    )

    total_events = 0
    valid_events = 0
    invalid_events = 0

    print(f"Listening on topic: {args.topic}")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print(f"Group ID: {args.group_id}")
    print("Waiting for messages...\n")

    try:
        for message in consumer:
            event = message.value

            if not isinstance(event, dict):
                print("Skipped non-dict message:", event)
                continue

            if not matches_filter(event, args.event_field, args.event_value):
                continue

            total_events += 1

            print("=== NEW EVENT ===")
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(json.dumps(event, indent=2, ensure_ascii=False))
            print()

            if is_valid_event(message.topic, event):
                valid_events += 1

                inserted = insert_event(message.topic, event)
                if inserted:
                    print("Saved to SQL Server: dbo.sensor_readings\n")
                else:
                    print("Skipped duplicate event in SQL Server\n")
            else:
                invalid_events += 1

                if args.save_to_file:
                    output_path = route_event(message.topic, event)
                    save_event_to_file(event, output_path)
                    print(f"Saved to: {output_path}\n")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        consumer.close()
        print("\nSummary:")
        print(f"- total events processed: {total_events}")
        print(f"- valid events: {valid_events}")
        print(f"- invalid events: {invalid_events}")


if __name__ == "__main__":
    main()