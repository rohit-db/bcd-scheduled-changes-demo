import json
from datetime import datetime, timezone

from trip_intel.sink_sql import statements

TS = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


def _out(tags, rule_hits=(), actions=()):
    return {
        "trip_id": "T1", "version": 6, "event_ts": TS, "processed_ts": TS,
        "snapshot_json": json.dumps({"trip_id": "T1", "record_locator": "R1", "version": 6}),
        "change_json": json.dumps({"from_version": 5, "to_version": 6, "tags": tags, "changes": [], "schedule_changes": []}),
        "rule_hits_json": json.dumps(list(rule_hits)),
        "actions_json": json.dumps(list(actions)),
    }


def test_normal_version_writes_latest_and_event():
    sqls = [s for s, _ in statements(_out(["SEAT_REMOVED"]))]
    assert any(s.startswith("INSERT INTO trip_latest") for s in sqls)
    assert any(s.startswith("INSERT INTO change_events") for s in sqls)


def test_stale_version_skips_latest():
    sqls = [s for s, _ in statements(_out(["STALE_VERSION"]))]
    assert not any("trip_latest" in s for s in sqls)
    assert any(s.startswith("INSERT INTO change_events") for s in sqls)


def test_statements_are_idempotent_on_replay():
    out = _out(["SCHEDULE_CHANGE"], rule_hits=[{"rule_id": "r1", "name": "x"}],
               actions=[{"id": "abc", "type": "T", "trip_id": "T1", "version": 6, "target": "t", "payload": {}}])
    for sql, _ in statements(out):
        assert "ON CONFLICT" in sql, sql


def test_latest_only_moves_forward():
    sql = [s for s, _ in statements(_out(["SEAT_REMOVED"])) if "trip_latest" in s][0]
    assert "WHERE trip_latest.version < EXCLUDED.version" in sql


def test_naive_timestamps_become_utc():
    naive = TS.replace(tzinfo=None)
    out = dict(_out(["SEAT_REMOVED"]), event_ts=naive, processed_ts=naive)
    params = [p for s, p in statements(out) if s.startswith("INSERT INTO change_events")][0]
    assert params[-2].tzinfo is timezone.utc and params[-1].tzinfo is timezone.utc


def test_rule_hits_and_actions_rows():
    out = _out(["SCHEDULE_CHANGE"], rule_hits=[{"rule_id": "r1", "name": "x"}, {"rule_id": "r2", "name": "y"}],
               actions=[{"id": "abc", "type": "T", "trip_id": "T1", "version": 6, "target": "t", "payload": {"a": 1}}])
    stmts = statements(out)
    assert sum(1 for s, _ in stmts if s.startswith("INSERT INTO rule_hits")) == 2
    outbox = [p for s, p in stmts if s.startswith("INSERT INTO action_outbox")]
    assert outbox[0][0] == "abc"
