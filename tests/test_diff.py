from pathlib import Path

from trip_intel import generator as g
from trip_intel.diff import diff
from trip_intel.payload import parse, to_snapshot

DATA = Path(__file__).parent / "data"


def _pair(scenario):
    trip = g.SCENARIOS.index(scenario)
    return (to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1)),
            to_snapshot(g.build_payload(trip, g.MUTATION_VERSION)))


def test_new_trip():
    snap = to_snapshot(g.build_payload(0, 1))
    result = diff(None, snap)
    assert result["tags"] == ["NEW_TRIP"]
    assert result["from_version"] is None and result["to_version"] == 1


def test_noise_version_is_not_significant():
    a = to_snapshot(g.build_payload(0, 2))
    b = to_snapshot(g.build_payload(0, 3))
    assert diff(a, b)["tags"] == ["NO_SIGNIFICANT_CHANGE"]


def test_stale_version_ignored():
    a = to_snapshot(g.build_payload(0, 5))
    b = to_snapshot(g.build_payload(0, 4))
    assert diff(a, b)["tags"] == ["STALE_VERSION"]


def test_stale_version_duplicate_ignored():
    a = to_snapshot(g.build_payload(0, 5))
    assert diff(a, a)["tags"] == ["STALE_VERSION"]


def test_schedule_change_tags():
    prev, new = _pair("schedule_change")
    result = diff(prev, new)
    assert {"SCHEDULE_CHANGE", "SEGMENT_ADDED", "SEGMENT_MODIFIED"} <= set(result["tags"])
    assert len(result["schedule_changes"]) == 1
    assert {"segment": prev["segments"][0]["segment_id"], "field": "status", "old": "HK", "new": "HX"} in result["changes"]


def test_seat_removed():
    prev, new = _pair("seat_removed")
    assert diff(prev, new)["tags"] == ["SEAT_REMOVED"]


def test_contact_and_payment():
    assert diff(*_pair("contact_changed"))["tags"] == ["CONTACT_CHANGED"]
    assert diff(*_pair("payment_changed"))["tags"] == ["PAYMENT_CHANGED"]


def test_hotel_added():
    assert diff(*_pair("hotel_added"))["tags"] == ["SEGMENT_ADDED"]


def test_real_uat_versions_diff():
    prev = parse((DATA / "uat_t1_v2.json").read_text())
    new = parse((DATA / "uat_t1_v3.json").read_text())
    result = diff(prev, new)
    # KK -> HX with no replacement and a later arrival: a modification, not a schedule change,
    # and no contact/payment noise from the missing extensions.
    assert result["tags"] == ["SEGMENT_MODIFIED"]
    fields = {c["field"] for c in result["changes"]}
    assert fields == {"status", "end"}
