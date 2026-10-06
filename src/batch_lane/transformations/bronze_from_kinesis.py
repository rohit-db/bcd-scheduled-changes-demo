"""BRONZE flow from BCD's Kinesis stream — the same feed the real-time lane reads, landed once in Delta.

Defined only when the pipeline has a `kinesis_stream` setting (the UAT target). Read-only: a polling
consumer through the same UC service credential EPAM's reader uses.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

STREAM = spark.conf.get("kinesis_stream", "")

if STREAM:
    @dp.append_flow(target="bronze_pnr_versions", name="bronze_from_kinesis")
    def bronze_from_kinesis():
        return (
            spark.readStream.format("kinesis")
            .option("streamName", STREAM)
            .option("region", spark.conf.get("kinesis_region"))
            .option("serviceCredential", spark.conf.get("kinesis_credential"))
            .option("initialPosition", "trim_horizon")
            .load()
            .select(
                F.col("data").cast("string").alias("payload"),
                F.lit("kinesis").alias("source"),
                F.concat_ws(":", "shardId", "sequenceNumber").alias("source_ref"),
                F.col("approximateArrivalTimestamp").alias("landed_at"),
                F.current_timestamp().alias("ingested_at"),
            )
        )
