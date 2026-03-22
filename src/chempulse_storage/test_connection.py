from __future__ import annotations

import os

from dotenv import load_dotenv
import pyodbc


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


def main() -> None:
    conn_str = build_connection_string()

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            print("Connection successful.")
            print(row[0])
    except Exception as e:
        print("Connection failed.")
        print(e)


if __name__ == "__main__":
    main()