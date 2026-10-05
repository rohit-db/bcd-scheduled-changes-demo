"""Lakebase connection pool for the app (native password role, same as the stream sink).

Connections are health-checked on checkout and recycled when idle, so the first requests after an idle
night (or a Lakebase restart) don't fail with stale connections.
"""
import os

from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
    conninfo=make_conninfo(host=os.environ["LAKEBASE_HOST"], port=5432, dbname="databricks_postgres",
                           user="trip_intel_app", password=os.environ["LAKEBASE_PASSWORD"], sslmode="require",
                           connect_timeout=5),
    kwargs={"row_factory": dict_row, "autocommit": True},
    min_size=1,
    max_size=8,
    max_idle=300,
    check=ConnectionPool.check_connection,
    open=False,
)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params: tuple = ()) -> None:
    with pool.connection() as conn:
        conn.execute(sql, params)
