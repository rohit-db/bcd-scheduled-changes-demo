"""Entry point for the real-time trip-intelligence stream.

--source rate     : demo generator (FEVM), rate source + deterministic BCD-schema payload UDF
--source kinesis  : BCD's real stream (UAT finale), same options as EPAM's kinesis_reader.py
"""
import argparse
import os
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# The bundle syncs src/ as workspace files (no wheel): import from it on the driver, and ship it
# to Python workers as a zip so the UDF and the stateful processor can import trip_intel/rtm.
SRC = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(sys._getframe().f_code.co_filename)), ".."))
sys.path.insert(0, SRC)

from rtm.lakebase import conninfo  # noqa: E402
from rtm.processor import OUTPUT_SCHEMA, TripProcessor  # noqa: E402
from rtm.progress import latency_line  # noqa: E402
from rtm.sink import LakebaseSink  # noqa: E402
from trip_intel.generator import payload_for  # noqa: E402


def ship_packages(spark) -> None:
    staging = tempfile.mkdtemp()
    for pkg in ("trip_intel", "rtm"):
        shutil.copytree(os.path.join(SRC, pkg), os.path.join(staging, pkg))
    archive = shutil.make_archive(os.path.join(tempfile.mkdtemp(), "trip_intel_src"), "zip", staging)
    spark.sparkContext.addPyFile(archive)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["rate", "kinesis"], default="rate")
    p.add_argument("--rows-per-second", type=int, default=15)
    p.add_argument("--lakebase-host", required=True)
    p.add_argument("--secret-scope", default="bcd-trip-intel")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--kinesis-stream", default="uat01-HermesTripsDS")
    p.add_argument("--kinesis-region", default="us-east-1")
    p.add_argument("--kinesis-credential", default="kinesis_reader_cred_uat")
    p.add_argument("--trigger", choices=["realtime", "processing"], default="realtime")
    return p.parse_args()


def source_df(spark, args):
    if args.source == "rate":
        gen = F.udf(payload_for, "string")
        return (
            spark.readStream.format("rate").option("rowsPerSecond", args.rows_per_second).option("numPartitions", 1).load()
            .select(gen("value").alias("payload"), F.col("timestamp").alias("event_ts"))
        )
    return (
        spark.readStream.format("kinesis")
        .option("streamName", args.kinesis_stream)
        .option("region", args.kinesis_region)
        .option("serviceCredential", args.kinesis_credential)
        .option("initialPosition", "latest")
        .load()
        .select(F.col("data").cast("string").alias("payload"), F.col("approximateArrivalTimestamp").alias("event_ts"))
    )


class LatencyLogger(StreamingQueryListener):
    """Prints the built-in RTM latency percentiles (e2e / source queuing / processing) once per batch."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        line = latency_line(event.progress.json)
        if line:
            print(line, flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    ship_packages(spark)
    spark.streams.addListener(LatencyLogger())
    from databricks.sdk.runtime import dbutils

    dsn = conninfo(args.lakebase_host, dbutils.secrets.get(args.secret_scope, "lakebase-password"))
    versions = source_df(spark, args).select(
        F.get_json_object("payload", "$.identification.id").alias("trip_id"),
        F.get_json_object("payload", "$.identification.version").cast("int").alias("version"),
        "payload",
        "event_ts",
    ).where(F.col("trip_id").isNotNull() & F.col("version").isNotNull())
    enriched = versions.groupBy("trip_id").transformWithState(
        statefulProcessor=TripProcessor(dsn), outputStructType=OUTPUT_SCHEMA, outputMode="Update", timeMode="None"
    )
    writer = enriched.writeStream.foreach(LakebaseSink(dsn)).outputMode("update").option("checkpointLocation", args.checkpoint)
    writer = writer.trigger(realTime="5 minutes") if args.trigger == "realtime" else writer.trigger(processingTime="1 second")
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
