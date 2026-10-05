import json

from trip_intel import generator as g
from trip_intel.payload import to_snapshot


def test_locate_interleaves_trips():
    assert g.locate(0) == (0, 1)
    assert g.locate(1) == (1, 1)
    assert g.locate(g.ACTIVE_TRIPS) == (0, 2)
    block = g.ACTIVE_TRIPS * g.VERSIONS_PER_TRIP
    assert g.locate(block) == (g.ACTIVE_TRIPS, 1)


def test_payload_matches_bcd_schema():
    p = json.loads(g.payload_for(0))
    assert set(p["identification"]) == {"id", "recordLocator", "version"}
    seg = p["segments"][0]
    for key in ["type", "segmentId", "segmentNumber", "status", "marketingFlight", "classOfService",
                "departureAirport", "arrivalAirport", "startDateTime", "endDateTime"]:
        assert key in seg
    assert seg["status"]["code"] == "HK"
    assert seg["departureAirport"]["airport"]["code"]


def test_deterministic():
    assert g.payload_for(12345) == g.payload_for(12345)


def test_every_scenario_appears_within_first_block():
    seen = {g.scenario_for(t) for t in range(g.ACTIVE_TRIPS)}
    assert seen == set(g.SCENARIOS)


def test_schedule_change_mutation_at_mutation_version():
    trip = g.SCENARIOS.index("schedule_change")
    before = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    after = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert [s["status"] for s in before["segments"]] == ["HK", "HK"]
    statuses = {s["segment_id"]: s["status"] for s in after["segments"]}
    assert statuses[f"T{trip:07d}-S1"] == "HX"
    assert statuses[f"T{trip:07d}-S1R"] == "HK"


def test_versions_after_mutation_only_touch_noise():
    trip = g.SCENARIOS.index("seat_removed")
    a = g.build_payload(trip, g.MUTATION_VERSION)
    b = g.build_payload(trip, g.MUTATION_VERSION + 1)
    assert a["remarks"] != b["remarks"]
    a.pop("remarks"); b.pop("remarks"); a["identification"].pop("version"); b["identification"].pop("version")
    assert a == b
