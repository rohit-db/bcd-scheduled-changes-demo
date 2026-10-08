"""SILVER: every booking version the real-time stream processed, read from Lakebase's change data feed.

The real-time lane diffs each version once and writes the result (tags, changes, schedule changes, snapshot) to
Lakebase. Lakebase CDF lands every insert as a Delta history table (lb_change_events_history), so this lane reads that
copy instead of ingesting the feed a second time: one ingest, one set of decisions for operations and analytics.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDF_SCHEMA = spark.conf.get("cdf_schema")

SNAPSHOT = (
    "struct<record_locator:string, "
    "segments:array<struct<type:string, segment_id:string, segment_number:int, status:string, carrier:string, "
    "flight_number:string, class_of_service:string, origin:string, destination:string, start:string, `end`:string, "
    "seat:string, hotel_name:string>>, "
    "traveler:struct<name:string, email:string, phone:string>, payment:struct<type:string, last4:string>>"
)


@dp.table(
    name="silver_change_events",
    comment="Every booking version with the real-time stream's change tags, from the Lakebase change data feed.",
)
@dp.expect_or_drop("valid_trip_id", "trip_id IS NOT NULL")
@dp.expect_or_drop("valid_version", "to_version IS NOT NULL AND to_version > 0")
@dp.expect("has_segments", "size(snapshot.segments) > 0")
def silver_change_events():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{CDF_SCHEMA}.lb_change_events_history")
        .where("_pg_change_type = 'insert'")
        .select(
            "trip_id",
            "from_version",
            "to_version",
            # Postgres text[] arrives as '{TAG_A,TAG_B}'.
            F.split(F.regexp_replace("tags", r"^\{|\}$", ""), ",").alias("tags"),
            "changes",
            "schedule_changes",
            F.from_json("snapshot", SNAPSHOT).alias("snapshot"),
            "event_ts",
            "detected_ts",
            "written_ts",
            F.col("_timestamp").cast("timestamp").alias("cdf_ts"),  # CDF writes timestamp_ntz
            F.current_timestamp().alias("ingested_at"),
        )
    )
