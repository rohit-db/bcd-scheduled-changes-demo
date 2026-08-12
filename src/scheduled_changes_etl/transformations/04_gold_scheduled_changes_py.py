# ============================================================================
# GOLD (Python): scheduled-change detection — the customer's PySpark notebook,
# now expressed as a Lakeflow Declarative Pipeline table.
# ----------------------------------------------------------------------------
# This is Bojidar's own logic, essentially verbatim, with two changes:
#   1. It reads from the `silver_segments` DLT table instead of a hard-coded
#      in-memory DataFrame, and returns a DataFrame that the pipeline
#      materializes — no SparkSession / .show() / main() plumbing.
#   2. BUG FIX: the original used Python `or` on Column objects, e.g.
#          df.filter(F.col("code") == "HX" or F.col("code") == "UN")
#      Python `or` short-circuits on truthiness and silently keeps ONLY "HX"
#      (and only "HK" on the NEW side) — dropping UN / KK / TK entirely.
#      The correct operator is `.isin(...)` (or `|` between conditions).
#
# The output is identical to gold_scheduled_changes_sql — same rows — which is
# the point to make in the room: SQL and PySpark run on the same engine.
# ============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

OLD_CODES = ["HX", "UN"]
NEW_CODES = ["HK", "KK", "TK"]
MATCH_WINDOW_SECONDS = 60 * 60  # 60 minutes


@dlt.table(
    name="gold_scheduled_changes_py",
    comment=(
        "Detected airline-initiated scheduled changes (unique 1:1 OLD->NEW "
        "segment matches within 60 minutes) — PySpark implementation. "
        "Identical output to gold_scheduled_changes_sql."
    ),
)
def gold_scheduled_changes_py():
    segments = dlt.read("silver_segments")

    # FIX: use .isin(...) — NOT `== "HX" or == "UN"`, which drops UN/KK/TK.
    old_df = segments.filter(F.col("code").isin(OLD_CODES)).alias("old")
    new_df = segments.filter(F.col("code").isin(NEW_CODES)).alias("new")

    time_difference_seconds = F.abs(
        F.col("old.departure_timestamp").cast("long")
        - F.col("new.departure_timestamp").cast("long")
    )

    candidate_pairs = old_df.join(
        new_df,
        on=(
            (F.col("old.origin") == F.col("new.origin"))
            & (F.col("old.destination") == F.col("new.destination"))
            & (F.col("old.departure_date") == F.col("new.departure_date"))
            & (time_difference_seconds <= MATCH_WINDOW_SECONDS)
        ),
        how="inner",
    ).select(
        F.col("old.record_id").alias("old_record_id"),
        F.col("new.record_id").alias("new_record_id"),
        F.coalesce(F.col("old.record_locator"), F.col("new.record_locator")).alias("record_locator"),
        F.col("old.origin").alias("origin"),
        F.col("old.destination").alias("destination"),
        F.col("old.departure_date").alias("departure_date"),
        F.col("old.departure_timestamp").alias("old_departure_timestamp"),
        F.col("new.departure_timestamp").alias("new_departure_timestamp"),
        (time_difference_seconds / 60).cast("int").alias("time_difference_minutes"),
        F.col("old.carrier").alias("carrier"),
    )

    # Keep only unique 1:1 matches — ambiguous many-to-one pairings are dropped.
    old_win = Window.partitionBy("old_record_id")
    new_win = Window.partitionBy("new_record_id")

    return (
        candidate_pairs
        .withColumn("old_match_count", F.count("*").over(old_win))
        .withColumn("new_match_count", F.count("*").over(new_win))
        .filter((F.col("old_match_count") == 1) & (F.col("new_match_count") == 1))
        .withColumn("is_scheduled_change", F.lit(True))
        .select(
            "record_locator", "origin", "destination", "departure_date",
            "old_departure_timestamp", "new_departure_timestamp",
            "time_difference_minutes", "carrier", "is_scheduled_change",
        )
    )
