"""Hourly analytics lane: copy operational Lakebase tables into Delta for Genie / BI (serverless).

Same data the real-time lane wrote seconds ago — consumed here on an hourly schedule. Reads with
psycopg (the shared FEVM metastore does not allow registering Lakebase as a UC catalog).
"""
import argparse
import os
import sys

import psycopg
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(sys._getframe().f_code.co_filename)), "..")))
from rtm.lakebase import conninfo  # noqa: E402

TABLES = {
    "change_events_hourly": (
        "SELECT trip_id, from_version, to_version, tags, changes::text AS changes, "
        "schedule_changes::text AS schedule_changes, event_ts, detected_ts, written_ts "
        "FROM change_events WHERE written_ts > now() - interval '24 hours'",
        "trip_id string, from_version int, to_version int, tags array<string>, changes string, "
        "schedule_changes string, event_ts timestamp, detected_ts timestamp, written_ts timestamp",
    ),
    "rule_hits_hourly": (
        "SELECT rule_id, rule_name, trip_id, version, matched_ts FROM rule_hits",
        "rule_id string, rule_name string, trip_id string, version int, matched_ts timestamp",
    ),
    "trip_latest_hourly": (
        "SELECT trip_id, version, record_locator, snapshot::text AS snapshot, updated_ts FROM trip_latest",
        "trip_id string, version int, record_locator string, snapshot string, updated_ts timestamp",
    ),
    "action_outbox_hourly": (
        "SELECT id, type, trip_id, version, target, payload::text AS payload, status, created_ts, sent_ts FROM action_outbox",
        "id string, type string, trip_id string, version int, target string, payload string, status string, "
        "created_ts timestamp, sent_ts timestamp",
    ),
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--schema", required=True, help="catalog.schema for the Delta snapshot tables")
    p.add_argument("--lakebase-host", required=True)
    p.add_argument("--secret-scope", default="bcd-trip-intel")
    args = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    from databricks.sdk.runtime import dbutils

    dsn = conninfo(args.lakebase_host, dbutils.secrets.get(args.secret_scope, "lakebase-password"))
    with psycopg.connect(dsn) as conn:
        for table, (sql, schema) in TABLES.items():
            rows = conn.execute(sql).fetchall()
            spark.createDataFrame(rows, schema).write.mode("overwrite").option("overwriteSchema", "true") \
                .saveAsTable(f"{args.schema}.{table}")
            print(f"{table}: {len(rows)} rows")


if __name__ == "__main__":
    main()
