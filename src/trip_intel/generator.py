"""Deterministic generator of BCD-schema PNR versions for the demo stream.

seq (the rate source's monotonically increasing `value`) maps to (trip_no, version) so that
ACTIVE_TRIPS trips are interleaved and each receives VERSIONS_PER_TRIP versions. Most versions
are noise (only `remarks` changes, like GDS systems re-saving the record); at the trip's
mutation_version its scenario mutation lands and persists for the remaining versions. Mutation
versions vary per trip (3–9) so significant changes arrive continuously, not in one burst per block.
"""
import json
from datetime import datetime, timedelta, timezone

ACTIVE_TRIPS = 50
VERSIONS_PER_TRIP = 12
MUTATION_VERSION = 6
SCENARIOS = [
    "churn", "schedule_change", "seat_removed", "contact_changed",
    "churn", "payment_changed", "ambiguous_schedule_change", "hotel_added",
]
ROUTES = [("SFO", "LAX"), ("JFK", "YYZ"), ("ATL", "ORD"), ("BOS", "DCA"), ("SEA", "DEN"), ("LHR", "CDG")]
CARRIERS = ["DL", "UA", "AA", "AC", "BA"]
SYSTEMS = ["OBT", "MID_OFFICE", "TICKETING", "QC_ROBOT", "GDS_QUEUE"]
NAMES = ["Ada Lovelace", "Grace Hopper", "Alan Turing", "Katherine Johnson", "Linus Pauling", "Mae Jemison"]
BASE_DATE = datetime(2026, 11, 2, tzinfo=timezone.utc)


def locate(seq: int) -> tuple[int, int]:
    window, idx = divmod(seq, ACTIVE_TRIPS * VERSIONS_PER_TRIP)
    return window * ACTIVE_TRIPS + idx % ACTIVE_TRIPS, idx // ACTIVE_TRIPS + 1


def mutation_version(trip_no: int) -> int:
    # The first len(SCENARIOS) trips mutate at MUTATION_VERSION; later groups shift across 3..9.
    return 3 + ((trip_no // len(SCENARIOS)) % 7 + 3) % 7


def scenario_for(trip_no: int) -> str:
    return SCENARIOS[trip_no % len(SCENARIOS)]


def _air(seg_id, number, status, carrier, flight, origin, dest, dep, seat):
    seg = {
        "type": "Air",
        "segmentId": seg_id,
        "segmentNumber": number,
        "status": {"code": status},
        "marketingFlight": {"airlineCode": carrier, "flightNumber": flight},
        "classOfService": {"code": "Y"},
        "departureAirport": {"airport": {"code": origin}},
        "arrivalAirport": {"airport": {"code": dest}},
        "startDateTime": dep.isoformat(),
        "endDateTime": (dep + timedelta(hours=2, minutes=15)).isoformat(),
    }
    if seat:
        seg["seat"] = {"number": seat}
    return seg


def build_payload(trip_no: int, version: int) -> dict:
    tid = f"T{trip_no:07d}"
    origin, dest = ROUTES[trip_no % len(ROUTES)]
    carrier = CARRIERS[trip_no % len(CARRIERS)]
    dep = BASE_DATE + timedelta(days=trip_no % 30, hours=6 + trip_no % 10)
    flight = str(100 + trip_no % 900)
    segments = [
        _air(f"{tid}-S1", 1, "HK", carrier, flight, origin, dest, dep, f"{12 + trip_no % 20}C"),
        _air(f"{tid}-S2", 2, "HK", carrier, str(int(flight) + 1000), dest, origin, dep + timedelta(days=3), "14A"),
    ]
    name = NAMES[trip_no % len(NAMES)]
    traveler = {"name": name, "email": f"{name.split()[0].lower()}.{trip_no}@example.com", "phone": f"+1-555-{trip_no % 10000:04d}"}
    payment = {"type": "CC", "last4": f"{1000 + trip_no % 9000}"}

    if version >= mutation_version(trip_no):
        scenario = scenario_for(trip_no)
        if scenario == "schedule_change":
            segments[0]["status"] = {"code": "HX"}
            segments.append(_air(f"{tid}-S1R", 3, "HK", carrier, str(int(flight) + 1), origin, dest, dep + timedelta(minutes=45), None))
        elif scenario == "ambiguous_schedule_change":
            segments[0]["status"] = {"code": "HX"}
            segments.append(_air(f"{tid}-S1R", 3, "HK", carrier, str(int(flight) + 1), origin, dest, dep + timedelta(minutes=30), None))
            segments.append(_air(f"{tid}-S1Q", 4, "HK", carrier, str(int(flight) + 2), origin, dest, dep + timedelta(minutes=50), None))
        elif scenario == "seat_removed":
            segments[0].pop("seat")
        elif scenario == "contact_changed":
            traveler["email"] = f"{name.split()[0].lower()}.new.{trip_no}@example.com"
        elif scenario == "payment_changed":
            payment["last4"] = f"{9999 - trip_no % 9000}"
        elif scenario == "hotel_added":
            segments.append({
                "type": "Hotel", "segmentId": f"{tid}-H1", "segmentNumber": 3, "status": {"code": "HK"},
                "hotelName": "Grand Central Hotel", "startDateTime": dep.isoformat(),
                "endDateTime": (dep + timedelta(days=3)).isoformat(),
            })

    return {
        "identification": {"id": tid, "recordLocator": f"R{trip_no:05d}", "version": version},
        "segments": segments,
        "travelers": [traveler],
        "payment": payment,
        "remarks": f"saved by {SYSTEMS[version % len(SYSTEMS)]} v{version}",
    }


def payload_for(seq: int) -> str:
    trip_no, version = locate(seq)
    return json.dumps(build_payload(trip_no, version), separators=(",", ":"))
