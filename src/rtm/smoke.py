"""RTM feasibility probe: rate source -> Python UDF -> transformWithState -> ForeachWriter (stdout).

Run on a single-node DBR 18 LTS dedicated cluster with
spark.databricks.streaming.realTimeMode.enabled=true. Watch the driver log for
'PROBE' lines; latency_ms should be well under 1000.
"""
import time
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

STATE = StructType([StructField("count", LongType())])
OUT = StructType(
    [StructField("key", StringType()), StructField("count", LongType()), StructField("event_ts", TimestampType())]
)


class CountProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.state = handle.getValueState("count", STATE)

    def handleInputRows(self, key, rows, timerValues):
        count = self.state.get()[0] if self.state.exists() else 0
        last_ts = None
        for row in rows:
            count += 1
            last_ts = row.event_ts
        self.state.update((count,))
        yield Row(key=key[0], count=count, event_ts=last_ts)

    def close(self) -> None:
        pass


class PrintSink:
    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        latency_ms = (datetime.now(timezone.utc) - row.event_ts.replace(tzinfo=timezone.utc)).total_seconds() * 1000
        print(f"PROBE key={row.key} count={row.count} latency_ms={latency_ms:.1f}", flush=True)

    def close(self, error):
        if error:
            print(f"PROBE sink error: {error}", flush=True)


@F.udf("string")
def fake_payload(value):
    return f"trip-{value % 5}"


def main():
    spark = SparkSession.builder.getOrCreate()
    src = (
        spark.readStream.format("rate").option("rowsPerSecond", 15).option("numPartitions", 1).load()
        .select(fake_payload("value").alias("key"), F.col("timestamp").alias("event_ts"))
    )
    out = src.groupBy("key").transformWithState(
        statefulProcessor=CountProcessor(), outputStructType=OUT, outputMode="Update", timeMode="None"
    )
    q = (
        out.writeStream.foreach(PrintSink()).outputMode("update")
        .option("checkpointLocation", f"/tmp/rtm_probe_{int(time.time())}")
        .trigger(realTime="5 minutes")
        .start()
    )
    q.awaitTermination(180)
    q.stop()


if __name__ == "__main__":
    main()
