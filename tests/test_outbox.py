import json

from trip_intel import generator as g
from trip_intel.diff import diff
from trip_intel.outbox import build_actions, eventbridge_entry
from trip_intel.payload import to_snapshot
from trip_intel.rules import Rule

RULE = Rule("r-seat", "No seat", {"==": [1, 1]}, {"type": "INVOKE_SERVICE", "target": "seat-assignment-service"})


def _sched():
    trip = g.SCENARIOS.index("schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    return new, diff(prev, new)


def test_schedule_change_produces_traveler_notification():
    snap, change = _sched()
    actions = build_actions(snap, change, [])
    assert [a["type"] for a in actions] == ["NOTIFY_TRAVELER_SCHEDULE_CHANGE"]
    p = actions[0]["payload"]
    assert p["record_locator"] == snap["record_locator"]
    assert p["traveler_name"] == snap["traveler"]["name"]
    assert p["old_flight"]["status"] == "HX" and p["new_flight"]["status"] == "HK"
    assert p["delta_minutes"] == 45


def test_rule_action_and_stable_ids():
    snap, change = _sched()
    a1 = build_actions(snap, change, [RULE])
    a2 = build_actions(snap, change, [RULE])
    assert [a["id"] for a in a1] == [a["id"] for a in a2]
    assert len({a["id"] for a in a1}) == 2
    rule_action = [a for a in a1 if a["type"] == "INVOKE_SERVICE"][0]
    assert rule_action["target"] == "seat-assignment-service"
    assert rule_action["payload"]["rule_id"] == "r-seat"


def test_eventbridge_entry_shape():
    snap, change = _sched()
    action = build_actions(snap, change, [])[0]
    entry = eventbridge_entry(action, "bcd-trip-events")
    assert entry["Source"] == "bcd.trip-intel"
    assert entry["DetailType"] == "NOTIFY_TRAVELER_SCHEDULE_CHANGE"
    assert entry["EventBusName"] == "bcd-trip-events"
    assert json.loads(entry["Detail"])["idempotency_key"] == action["id"]
