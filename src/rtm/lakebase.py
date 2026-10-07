"""Lakebase connection info and the active-rule loader used inside the stream."""
import json

import psycopg

from trip_intel.rules import Rule


def conninfo(host: str, password: str, user: str = "trip_intel_app", dbname: str = "databricks_postgres",
             schema: str | None = None) -> str:
    # A schema other than public keeps side-by-side runs (micro-batch comparison, load tests) in their own tables.
    extra = {"options": f"-c search_path={schema}"} if schema else {}
    return psycopg.conninfo.make_conninfo(host=host, port=5432, dbname=dbname, user=user, password=password,
                                          sslmode="require", **extra)


def load_active_rules(dsn: str) -> list[Rule]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute("SELECT rule_id, name, json_logic, action FROM rules WHERE status = 'active'").fetchall()
    return [Rule(r[0], r[1], r[2] if isinstance(r[2], dict) else json.loads(r[2]),
                 r[3] if isinstance(r[3], dict) else json.loads(r[3])) for r in rows]
