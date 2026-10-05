"""Lakebase connection pool for the app (native password role, same as the stream sink)."""
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
    conninfo=(f"host={os.environ['LAKEBASE_HOST']} port=5432 dbname=databricks_postgres "
              f"user=trip_intel_app password={os.environ['LAKEBASE_PASSWORD']} sslmode=require"),
    kwargs={"row_factory": dict_row, "autocommit": True},
    min_size=1,
    max_size=8,
    open=False,
)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params: tuple = ()) -> None:
    with pool.connection() as conn:
        conn.execute(sql, params)
