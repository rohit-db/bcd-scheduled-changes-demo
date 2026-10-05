"""TripProcessor logic without a cluster: fake state handle, stubbed rule loader."""
import json
from datetime import datetime, timezone

from pyspark.sql import Row

import rtm.lakebase
from rtm.processor import OUTPUT_SCHEMA, TripProcessor
from trip_intel import generator as g
from trip_intel.rules import Rule
from trip_intel.sink_sql import statements

SCHED = Rule("r-sched", "Schedule change", {"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]},
             {"type": "ALERT_OPS", "target": "ops-queue"})


class FakeValueState:
    def __init__(self):
        self.value = None

    def exists(self):
        return self.value is not None

    def get(self):
        return self.value

    def update(self, value):
        self.value = value


class FakeHandle:
    def __init__(self):
        self.state = FakeValueState()

    def getValueState(self, name, schema):
        return self.state


def _processor(monkeypatch):
    monkeypatch.setattr(rtm.lakebase, "load_active_rules", lambda dsn: [SCHED])
    proc = TripProcessor("dsn")
    proc.init(FakeHandle())
    return proc


def _row(trip, version):
    return Row(trip_id=f"T{trip:07d}", version=version, payload=json.dumps(g.build_payload(trip, version)),
               event_ts=datetime(2026, 10, 5, 12, 0))


def test_versions_flow_through_state_and_fire_rules(monkeypatch):
    proc = _processor(monkeypatch)
    trip = g.SCENARIOS.index("schedule_change")
    outs = []
    for v in range(1, g.MUTATION_VERSION + 1):
        outs += list(proc.handleInputRows((f"T{trip:07d}",), iter([_row(trip, v)]), None))
    assert [o.version for o in outs] == list(range(1, g.MUTATION_VERSION + 1))
    assert json.loads(outs[0].change_json)["tags"] == ["NEW_TRIP"]
    last = outs[-1]
    assert "SCHEDULE_CHANGE" in json.loads(last.change_json)["tags"]
    assert [h["rule_id"] for h in json.loads(last.rule_hits_json)] == ["r-sched"]
    assert {a["type"] for a in json.loads(last.actions_json)} == {"NOTIFY_TRAVELER_SCHEDULE_CHANGE", "ALERT_OPS"}
    assert set(last.asDict()) == {f.name for f in OUTPUT_SCHEMA.fields}
    assert statements(last.asDict())  # sink can translate the row


def test_stale_version_does_not_regress_state(monkeypatch):
    proc = _processor(monkeypatch)
    key = (f"T{0:07d}",)
    list(proc.handleInputRows(key, iter([_row(0, 3)]), None))
    out = list(proc.handleInputRows(key, iter([_row(0, 2)]), None))[0]
    assert json.loads(out.change_json)["tags"] == ["STALE_VERSION"]
    assert json.loads(proc.prev_state.get()[0])["version"] == 3


def test_processed_ts_is_utc(monkeypatch):
    proc = _processor(monkeypatch)
    out = list(proc.handleInputRows(("T0000000",), iter([_row(0, 1)]), None))[0]
    assert out.processed_ts.tzinfo == timezone.utc


def test_malformed_record_is_skipped_and_stream_continues(monkeypatch):
    proc = _processor(monkeypatch)
    key = ("T0000000",)
    bad = Row(trip_id="T0000000", version=2, payload='{"identification": {"id": "T0000000", "version": 2}, "segments": ["oops"]}',
              event_ts=datetime(2026, 10, 5, 12, 0))
    assert list(proc.handleInputRows(key, iter([_row(0, 1), bad, _row(0, 3)]), None)) and True
    outs = list(proc.handleInputRows(key, iter([_row(0, 4)]), None))
    assert json.loads(proc.prev_state.get()[0])["version"] == 4
    assert outs[0].version == 4
