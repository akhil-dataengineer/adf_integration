# tools/sql_tools.py

import os
import pyodbc


def get_sql_connection():
    """Create a connection to Azure SQL Database using env vars."""
    conn_str = os.getenv("SQL_CONN_STR")

    if not conn_str:
        raise ValueError("⚠️ Missing SQL_CONN_STR. Please set it in your environment.")

    return pyodbc.connect(conn_str)


def run_query(query: str):
    """Run a SQL query and return results as a list of dicts."""
    conn = get_sql_connection()
    cursor = conn.cursor()

    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()
    return result


def list_tables():
    """List all tables in the current database."""
    return run_query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;")


if __name__ == "__main__":
    # Example: run directly for testing
    print("📋 Available Tables:")
    for row in list_tables():
        print(f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}")

    print("\n🔍 Sample Query:")
    results = run_query("SELECT TOP 5 * FROM dev.semrush_geo_distribution;")
    for r in results:
        print(r)
