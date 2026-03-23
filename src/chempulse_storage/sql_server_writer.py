from __future__ import annotations

import os
from typing import Any

import pyodbc
from dotenv import load_dotenv


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


def insert_sensor_reading(event: dict[str, Any]) -> bool:
    conn_str = build_connection_string()

    query = """
    INSERT INTO dbo.sensor_readings (
        event_id,
        event_ts,
        ingestion_ts,
        source_system,
        equipment_id,
        sensor_id,
        batch_id,
        metric_name,
        metric_value,
        metric_unit,
        quality_flag
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        event.get("event_id"),
        event.get("event_ts"),
        event.get("ingestion_ts"),
        event.get("source_system"),
        event.get("equipment_id"),
        event.get("sensor_id"),
        event.get("batch_id"),
        event.get("metric_name"),
        event.get("metric_value"),
        event.get("metric_unit"),
        event.get("quality_flag"),
    )

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