# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode, live
# MAGIC
# MAGIC Every booking version is compared with the previous version of the same trip, tagged with what changed, checked
# MAGIC against business rules, and shown here **milliseconds after it happened**.
# MAGIC
# MAGIC - **No sink.** Results go straight to `display()` with a real-time trigger. Nothing is written anywhere.
# MAGIC - **Same code as the demo.** Change detection, schedule changes and rules come from the `trip_intel` library the
# MAGIC   production stream uses. Here it runs on its own, with two rules defined inline.
# MAGIC - **Source:** a generator producing booking versions in BCD's JSON shape, 15 a second (your normal rate). To read
# MAGIC   your Kinesis stream instead, swap the source in step 1. Nothing else changes.
# MAGIC
# MAGIC **Attach to the `[dev rohit_bhagwat] rtm-live-notebook` cluster**: single node, Runtime 18.2, Photon off, with
# MAGIC `spark.databricks.streaming.realTimeMode.enabled = true`.
# MAGIC Run the cells in order. Each `display` keeps running until you stop it (last cell).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup
# MAGIC Make the shared `trip_intel` library importable here and on the Python workers, and show one booking version.

# COMMAND ----------

import json
import os
import shutil
import sys
import tempfile
import time

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

assert spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false") == "true", \
    "Attach this notebook to a Real-Time Mode cluster (rtm-live-notebook)."

SRC = os.path.abspath("..")          # this notebook lives in src/notebooks; the library is src/trip_intel
sys.path.insert(0, SRC)
_staging = tempfile.mkdtemp()
shutil.copytree(os.path.join(SRC, "trip_intel"), os.path.join(_staging, "trip_intel"))
spark.sparkContext.addPyFile(shutil.make_archive(os.path.join(tempfile.mkdtemp(), "trip_intel"), "zip", _staging))

# Real-Time Mode runs every stage at once, so each query needs a task slot per source partition and shuffle
# partition: 1 + 2 = 3 slots per stateful query here, on an 8-core node.
spark.conf.set("spark.sql.shuffle.partitions", "2")

# This workspace requires explicit streaming checkpoints (no implicit temp ones), so each query gets a fresh folder.
CHECKPOINTS = f"/Volumes/bcd_api_uat/databricks_demo/trip_intel_checkpoints/notebook/{int(time.time())}"

from trip_intel.diff import EMPTY_CHANGE, diff
from trip_intel.generator import payload_for
from trip_intel.payload import parse
from trip_intel.rules import Rule, context, newly_matched

print(json.dumps(json.loads(payload_for(5)), indent=2)[:1800])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Booking versions arriving, live
# MAGIC Each row is one new version of a booking. `ms_after_event` is how long after the version was created this row
# MAGIC was processed. Watch it stay in the low hundreds of milliseconds.
# MAGIC
# MAGIC `realTime="5 minutes"` is how often Spark **checkpoints**, not how often results appear: rows show up as they
# MAGIC arrive.

# COMMAND ----------

@F.udf("string")
def booking_version(seq):
    return payload_for(seq)


# Evaluated per row (a plain current_timestamp() is fixed for the whole long-running batch).
now_ms = F.udf(lambda: int(time.time() * 1000), "long").asNondeterministic()

source = (
    spark.readStream.format("rate").option("rowsPerSecond", 15).option("numPartitions", 1).load()
    .select(booking_version("value").alias("payload"), F.col("timestamp").alias("event_ts"))
    # Your Kinesis stream instead, read-only (Runtime 19 for polling, or enhanced fan-out on 18.2):
    # spark.readStream.format("kinesis").option("streamName", "uat01-HermesTripsDS")
    #     .option("region", "us-east-1").option("serviceCredential", "kinesis_reader_cred_uat")
    #     .option("initialPosition", "latest").load()
    #     .select(F.col("data").cast("string").alias("payload"), F.col("approximateArrivalTimestamp").alias("event_ts"))
)

versions = source.select(
    F.get_json_object("payload", "$.identification.id").alias("trip_id"),
    F.get_json_object("payload", "$.identification.version").cast("int").alias("version"),
    "payload",
    "event_ts",
).where("trip_id IS NOT NULL AND version IS NOT NULL")

display(
    versions.select(
        "trip_id", "version", "event_ts",
        (now_ms() - F.unix_millis("event_ts")).alias("ms_after_event"),
        F.substring("payload", 1, 140).alias("payload (first 140 chars)"),
    ),
    realTime="5 minutes", outputMode="update", checkpointLocation=f"{CHECKPOINTS}/versions",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Every version compared with the one before it
# MAGIC `transformWithState` keeps the previous version of each trip. For each new version it:
# MAGIC 1. diffs it against the previous one and tags what changed (`SCHEDULE_CHANGE`, `SEAT_REMOVED`, ... or
# MAGIC    `NO_SIGNIFICANT_CHANGE`),
# MAGIC 2. evaluates the business rules (JSON Logic, the format BCD's service uses),
# MAGIC 3. records how many milliseconds after the event the decision was made.
# MAGIC
# MAGIC In production the rules come from Lakebase and reload every 5 seconds; here they're defined inline.

# COMMAND ----------

RULES = [
    Rule("seed-schedule-change", "Notify ops on schedule change",
         {"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]},
         {"type": "ALERT_OPS", "target": "ops-queue"}),
    Rule("seed-missing-seat", "Confirmed flight without a seat",
         {"some": [{"var": "trip.segments"},
                   {"and": [{"==": [{"var": "type"}, "Air"]},
                            {"in": [{"var": "status"}, ["HK", "KK", "TK"]]},
                            {"missing": "seat"}]}]},
         {"type": "INVOKE_SERVICE", "target": "seat-assignment-service"}),
]


def what_changed(change):
    if change["schedule_changes"]:
        old, new = change["schedule_changes"][0]["old"], change["schedule_changes"][0]["new"]
        return (f'{old["carrier"]}{old["flight_number"]} {old["origin"]}→{old["destination"]} moved '
                f'{old["start"][11:16]} → {new["start"][11:16]} on {new["carrier"]}{new["flight_number"]}')
    parts = []
    for c in change["changes"][:3]:
        if c["field"] == "*":
            parts.append(f'{c["segment"]} {"added" if c["new"] == "added" else "removed"}')
        else:
            parts.append(f'{c["segment"]} {c["field"]}: {c["old"]} → {c["new"]}')
    return "; ".join(parts)


DECISIONS = StructType([
    StructField("trip_id", StringType()),
    StructField("from_version", IntegerType()),
    StructField("to_version", IntegerType()),
    StructField("tags", StringType()),
    StructField("what_changed", StringType()),
    StructField("rules_fired", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("decided_ms_after_event", LongType()),
])


class ChangeDetector(StatefulProcessor):
    def init(self, handle):
        self.prev = handle.getValueState("prev", StructType([StructField("snapshot_json", StringType())]))

    def handleInputRows(self, key, rows, timerValues):
        prev = json.loads(self.prev.get()[0]) if self.prev.exists() else None
        for row in sorted(rows, key=lambda r: r.version):
            new = parse(row.payload)
            change = diff(prev, new)
            fired = []
            if "STALE_VERSION" not in change["tags"]:
                fired, _ = newly_matched(RULES, context(prev, EMPTY_CHANGE), context(new, change))
                prev = new
                self.prev.update((json.dumps(new),))
            yield Row(
                trip_id=key[0],
                from_version=change["from_version"],
                to_version=new["version"],
                tags=", ".join(change["tags"]),
                what_changed=what_changed(change),
                rules_fired=", ".join(r.name for r in fired),
                event_ts=row.event_ts,
                decided_ms_after_event=int(time.time() * 1000 - row.event_ts.timestamp() * 1000),
            )

    def close(self):
        pass


decisions = versions.groupBy("trip_id").transformWithState(
    statefulProcessor=ChangeDetector(), outputStructType=DECISIONS, outputMode="Update", timeMode="None")

display(decisions, realTime="5 minutes", outputMode="update", checkpointLocation=f"{CHECKPOINTS}/decisions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Only what matters
# MAGIC About 85% of versions are re-saves with no business change. Filter them out and this is the live feed the Ops
# MAGIC Console shows: schedule changes, lost seats, contact and payment changes, with the rules that fired.

# COMMAND ----------

display(
    decisions.where("tags NOT IN ('NO_SIGNIFICANT_CHANGE', 'NEW_TRIP')"),
    realTime="5 minutes", outputMode="update", checkpointLocation=f"{CHECKPOINTS}/significant",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spark's own latency numbers
# MAGIC Real-Time Mode reports end-to-end latency percentiles in each query's progress (`e2eLatencyMs`: record created
# MAGIC → output). Progress is reported once per checkpoint interval, so these appear about 5 minutes after a query
# MAGIC starts. The per-row `decided_ms_after_event` column above is the live view.

# COMMAND ----------

for q in spark.streams.active:
    p = q.lastProgress or {}
    lat = {k: {x: v[x] for x in ("P50", "P95", "P99") if x in v} for k, v in p.get("latencies", {}).items()}
    print(f"{q.name or q.id} | batch {p.get('batchId')} | rows {p.get('numInputRows')} | "
          f"{lat or 'latency percentiles not reported yet: see decided_ms_after_event in the tables above'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Stop everything
# MAGIC The cluster auto-terminates after 30 idle minutes; stopping the queries lets it idle.

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
print("stopped; active queries:", len(spark.streams.active))
