from __future__ import annotations

import os
from typing import Any

import pyodbc
from dotenv import load_dotenv


TOPIC_TO_TABLE = {
    "chem.sensor_readings.v1": "dbo.sensor_readings",
    "chem.lab_results.v1": "dbo.lab_results",
    "chem.material_movements.v1": "dbo.material_movements",
    "chem.chemical_mdm.v1": "dbo.chemical_mdm",
}


TOPIC_TO_COLUMNS = {
    "chem.sensor_readings.v1": [
        "event_id",
        "event_ts",
        "ingestion_ts",
        "source_system",
        "equipment_id",
        "sensor_id",
        "batch_id",
        "metric_name",
        "metric_value",
        "metric_unit",
        "quality_flag",
    ],
    "chem.lab_results.v1": [
        "event_id",
        "event_ts",
        "ingestion_ts",
        "sample_id",
        "batch_id",
        "lab_id",
        "test_code",
        "result_value",
        "result_unit",
        "method_code",
        "analyst_id",
        "result_status",
        "quality_flag",
    ],
    "chem.material_movements.v1": [
        "event_id",
        "event_ts",
        "ingestion_ts",
        "movement_id",
        "material_id",
        "material_type",
        "batch_id",
        "from_location",
        "to_location",
        "quantity",
        "quantity_unit",
        "movement_type",
        "operator_id",
        "status",
    ],
    "chem.chemical_mdm.v1": [
        "event_id",
        "event_ts",
        "ingestion_ts",
        "chemical_id",
        "chemical_name",
        "cas_number",
        "hazard_class",
        "default_unit",
        "supplier_id",
        "is_active",
        "version",
    ],
}


def build_connection_string() -> str:
    load_dotenv()

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    trust_server_certificate = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "yes")

    if username and password:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate={trust_server_certificate};"
        )

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate={trust_server_certificate};"
    )


def insert_event(topic: str, event: dict[str, Any]) -> bool:
    if topic not in TOPIC_TO_TABLE:
        raise ValueError(f"Unsupported topic: {topic}")

    table_name = TOPIC_TO_TABLE[topic]
    columns = TOPIC_TO_COLUMNS[topic]

    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)

    query = f"""
    INSERT INTO {table_name} ({column_list})
    VALUES ({placeholders})
    """

    values = tuple(event.get(column) for column in columns)
    conn_str = build_connection_string()

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
        return True

    except pyodbc.IntegrityError as e:
        if "PRIMARY KEY" in str(e):
            return False
        raise