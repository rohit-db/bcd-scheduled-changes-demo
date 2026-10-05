"""transformWithState processor: one trip_id's versions in, one enriched row per version out.

State holds the previous snapshot so every version is diffed against the one before it — the
per-version change tracking that EPAM's latest-only MERGE loses.
"""
import json
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from trip_intel.diff import EMPTY_CHANGE, diff
from trip_intel.outbox import build_actions
from trip_intel.payload import parse
from trip_intel.rules import RuleCache, context, newly_matched

STATE_SCHEMA = StructType([StructField("snapshot_json", StringType())])
OUTPUT_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("version", IntegerType()),
    StructField("event_ts", TimestampType()),
    StructField("processed_ts", TimestampType()),
    StructField("snapshot_json", StringType()),
    StructField("change_json", StringType()),
    StructField("rule_hits_json", StringType()),
    StructField("actions_json", StringType()),
])


class TripProcessor(StatefulProcessor):
    def __init__(self, dsn: str, rule_ttl_seconds: float = 5.0):
        self.dsn = dsn
        self.rule_ttl_seconds = rule_ttl_seconds

    def init(self, handle: StatefulProcessorHandle) -> None:
        from rtm.lakebase import load_active_rules

        self.prev_state = handle.getValueState("prev", STATE_SCHEMA)
        self.rules = RuleCache(lambda: load_active_rules(self.dsn), ttl_seconds=self.rule_ttl_seconds)

    def handleInputRows(self, key, rows, timerValues):
        prev = json.loads(self.prev_state.get()[0]) if self.prev_state.exists() else None
        for row in sorted(rows, key=lambda r: r.version or 0):
            new = parse(row.payload)
            change = diff(prev, new)
            fired, errors = [], []
            if "STALE_VERSION" not in change["tags"]:
                fired, errors = newly_matched(self.rules.get(), context(prev, EMPTY_CHANGE), context(new, change))
                prev = new
                self.prev_state.update((json.dumps(new),))
            for err in errors:
                print(f"RULE_ERROR {err}", flush=True)
            yield Row(
                trip_id=key[0],
                version=new["version"],
                event_ts=row.event_ts,
                processed_ts=datetime.now(timezone.utc),
                snapshot_json=json.dumps(new),
                change_json=json.dumps(change),
                rule_hits_json=json.dumps([{"rule_id": r.rule_id, "name": r.name} for r in fired]),
                actions_json=json.dumps(build_actions(new, change, fired) if fired or change["schedule_changes"] else []),
            )

    def close(self) -> None:
        pass
