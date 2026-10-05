from trip_intel import generator as g
from trip_intel.payload import to_snapshot
from trip_intel.schedule_change import detect


def _seg(seg_id, status, start, origin="SFO", dest="LAX", type_="Air"):
    return {"type": type_, "segment_id": seg_id, "segment_number": 1, "status": status, "carrier": "DL",
            "flight_number": "1", "class_of_service": "Y", "origin": origin, "destination": dest,
            "start": start, "end": None, "seat": None, "hotel_name": None}


def _snap(*segs):
    return {"segments": list(segs)}


def test_simple_schedule_change():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:45:00+00:00"))
    pairs = detect(prev, new)
    assert len(pairs) == 1
    assert pairs[0]["old"]["segment_id"] == "A" and pairs[0]["new"]["segment_id"] == "B"
    assert pairs[0]["delta_minutes"] == 45


def test_un_and_kk_tk_codes_count():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    for old_code in ["HX", "UN"]:
        for new_code in ["HK", "KK", "TK"]:
            new = _snap(_seg("A", old_code, "2026-11-02T06:00:00+00:00"), _seg("B", new_code, "2026-11-02T06:10:00+00:00"))
            assert len(detect(prev, new)) == 1, (old_code, new_code)


def test_outside_window_ignored():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T07:01:00+00:00"))
    assert detect(prev, new) == []


def test_different_route_ignored():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:10:00+00:00", dest="SAN"))
    assert detect(prev, new) == []


def test_ambiguous_match_dropped():
    trip = g.SCENARIOS.index("ambiguous_schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert detect(prev, new) == []


def test_already_cancelled_not_reflagged():
    prev = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:10:00+00:00"))
    assert detect(prev, new) == []


def test_generator_schedule_change_detected():
    trip = g.SCENARIOS.index("schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert len(detect(prev, new)) == 1


def test_mixed_timezone_awareness_does_not_raise():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:30:00"))
    assert detect(prev, new) == []
