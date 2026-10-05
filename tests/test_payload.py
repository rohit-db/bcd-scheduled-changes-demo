import json
from pathlib import Path

from trip_intel.payload import parse, to_snapshot

DATA = Path(__file__).parent / "data"


def test_snapshot_from_real_uat_payload():
    snap = parse((DATA / "uat_t1_v2.json").read_text())
    assert snap["trip_id"] == "t1"
    assert snap["record_locator"] == "rl1"
    assert snap["version"] == 2
    seg = snap["segments"][0]
    assert seg == {
        "type": "Air", "segment_id": "manual-segment", "segment_number": 3, "status": "KK",
        "carrier": "delta", "flight_number": "342", "class_of_service": "H", "origin": "SFO",
        "destination": "LAX", "start": "2026-08-01T03:25:00+00:00", "end": "2026-08-01T07:43:00+00:00",
        "seat": None, "hotel_name": None,
    }
    # Real payloads have no extensions: sections exist but are all-None.
    assert snap["traveler"] == {"name": None, "email": None, "phone": None}
    assert snap["payment"] == {"type": None, "last4": None}


def test_snapshot_tolerates_empty_payload():
    snap = to_snapshot({})
    assert snap["trip_id"] is None
    assert snap["segments"] == []


def test_snapshot_reads_extensions():
    snap = to_snapshot({
        "identification": {"id": "T1", "recordLocator": "ABC123", "version": 1},
        "segments": [{"type": "Air", "segmentId": "S1", "seat": {"number": "12C"}}],
        "travelers": [{"name": "Ada", "email": "ada@x.com", "phone": "+1"}],
        "payment": {"type": "CC", "last4": "4242"},
    })
    assert snap["segments"][0]["seat"] == "12C"
    assert snap["traveler"]["email"] == "ada@x.com"
    assert snap["payment"]["last4"] == "4242"
