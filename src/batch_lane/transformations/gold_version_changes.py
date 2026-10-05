"""GOLD: every version of every trip diffed against the previous one — the same Python logic
(trip_intel.diff) the real-time stream runs, applied here in batch per trip."""
import pandas as pd
from pyspark import cloudpickle
from pyspark import pipelines as dp

import trip_intel
from trip_intel.batch import diff_trip_versions

# Ship trip_intel to the Python workers by value so they don't need it installed.
cloudpickle.register_pickle_by_value(trip_intel)

SCHEMA = ("trip_id string, from_version int, to_version int, tags array<string>, is_significant boolean, "
          "schedule_change_count int, changes string, carrier string, origin string, destination string, "
          "ingested_at timestamp")


def _diff_group(pdf: pd.DataFrame) -> pd.DataFrame:
    out = diff_trip_versions(pdf.to_dict("records"))
    return pd.DataFrame(out, columns=[c.split()[0] for c in SCHEMA.split(", ")])


@dp.materialized_view(
    name="gold_version_changes",
    comment="Per-version change tags for every trip (batch lane); same semantics as the real-time stream.",
)
def gold_version_changes():
    versions = spark.read.table("silver_pnr_versions").select("trip_id", "version", "payload", "ingested_at")
    return versions.groupBy("trip_id").applyInPandas(_diff_group, schema=SCHEMA)
