from trip_intel import generator as g
from trip_intel.diff import EMPTY_CHANGE, diff
from trip_intel.payload import to_snapshot
from trip_intel.rules import Rule, RuleCache, context, newly_matched

NO_SEAT = Rule("r-seat", "Air segment without seat", {
    "some": [{"var": "trip.segments"}, {"and": [
        {"==": [{"var": "type"}, "Air"]}, {"in": [{"var": "status"}, ["HK", "KK", "TK"]]}, {"missing": "seat"}]}]
}, {"type": "INVOKE_SERVICE", "target": "seat-assignment-service"})
SCHED = Rule("r-sched", "Schedule change", {"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]},
             {"type": "NOTIFY_TRAVELER", "target": "traveler-notifications"})


def _ctxs(scenario, v_prev, v_new):
    trip = g.SCENARIOS.index(scenario)
    prev = to_snapshot(g.build_payload(trip, v_prev))
    new = to_snapshot(g.build_payload(trip, v_new))
    return context(prev, EMPTY_CHANGE), context(new, diff(prev, new))


def test_seat_rule_fires_on_seat_removed():
    fired, errors = newly_matched([NO_SEAT], *_ctxs("seat_removed", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-seat"] and errors == []


def test_newly_matched_is_edge_triggered():
    fired, _ = newly_matched([NO_SEAT], *_ctxs("seat_removed", g.MUTATION_VERSION, g.MUTATION_VERSION + 1))
    assert fired == []


def test_change_rule_fires_each_time_condition_is_in_change():
    fired, _ = newly_matched([SCHED], *_ctxs("schedule_change", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-sched"]


def test_new_trip_counts_prev_as_false():
    snap = to_snapshot(g.build_payload(g.SCENARIOS.index("seat_removed"), g.MUTATION_VERSION))
    fired, _ = newly_matched([NO_SEAT], context(None, EMPTY_CHANGE), context(snap, diff(None, snap)))
    assert [r.rule_id for r in fired] == ["r-seat"]


def test_broken_rule_does_not_block_others():
    broken = Rule("r-bad", "bad", {"regex": ["x"]}, {})
    fired, errors = newly_matched([broken, SCHED], *_ctxs("schedule_change", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-sched"]
    assert errors[0]["rule_id"] == "r-bad"


def test_rule_cache_refreshes_and_survives_loader_errors():
    now = [0.0]
    calls = []

    def loader():
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("db down")
        return [SCHED] if len(calls) == 1 else [SCHED, NO_SEAT]

    cache = RuleCache(loader, ttl_seconds=5, clock=lambda: now[0])
    assert cache.get() == [SCHED]
    now[0] = 6
    assert cache.get() == [SCHED]          # loader failed -> keep last good
    now[0] = 12
    assert cache.get() == [SCHED, NO_SEAT]
