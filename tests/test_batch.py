import json

from trip_intel import generator as g
from trip_intel.batch import diff_trip_versions


def _rows(trip, versions):
    return [{"version": v, "payload": json.dumps(g.build_payload(trip, v)), "ingested_at": f"t{v}"} for v in versions]


def test_batch_diff_matches_realtime_semantics():
    trip = g.SCENARIOS.index("schedule_change")
    out = diff_trip_versions(_rows(trip, range(1, g.VERSIONS_PER_TRIP + 1)))
    assert [o["to_version"] for o in out] == list(range(1, g.VERSIONS_PER_TRIP + 1))
    assert out[0]["tags"] == ["NEW_TRIP"]
    sched = [o for o in out if "SCHEDULE_CHANGE" in o["tags"]]
    assert len(sched) == 1 and sched[0]["schedule_change_count"] == 1
    assert sched[0]["to_version"] == g.mutation_version(trip)
    assert sum(o["is_significant"] for o in out) == 2          # NEW_TRIP + the schedule change
    assert sched[0]["carrier"] and sched[0]["origin"] and sched[0]["destination"]


def test_batch_diff_sorts_and_dedupes_versions():
    rows = _rows(0, [3, 1, 2, 2])
    out = diff_trip_versions(rows)
    assert [o["to_version"] for o in out] == [1, 2, 3]
    assert [o["from_version"] for o in out] == [None, 1, 2]


def test_batch_diff_skips_unparseable_payload():
    rows = _rows(0, [1, 2]) + [{"version": 3, "payload": "{not json", "ingested_at": "t3"}]
    assert [o["to_version"] for o in diff_trip_versions(rows)] == [1, 2]


def test_landing_batch_continues_from_cursor():
    from trip_intel.batch import landing_batch

    lines, nxt = landing_batch(start_seq=10, count=3)
    assert nxt == 13
    assert lines == [g.payload_for(10), g.payload_for(11), g.payload_for(12)]
