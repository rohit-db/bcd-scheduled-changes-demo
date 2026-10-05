"""Turn a BCD trip payload (the Kinesis JSON) into a compact snapshot used for diffing and rules.

Only BCD's real fields are required; demo extensions (seat, travelers, payment, hotelName)
are optional and come back as None when absent.
"""
import json


def _get(d, *path):
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def _segment(s: dict) -> dict:
    return {
        "type": s.get("type"),
        "segment_id": s.get("segmentId"),
        "segment_number": s.get("segmentNumber"),
        "status": _get(s, "status", "code"),
        "carrier": _get(s, "marketingFlight", "airlineCode"),
        "flight_number": _get(s, "marketingFlight", "flightNumber"),
        "class_of_service": _get(s, "classOfService", "code"),
        "origin": _get(s, "departureAirport", "airport", "code"),
        "destination": _get(s, "arrivalAirport", "airport", "code"),
        "start": s.get("startDateTime"),
        "end": s.get("endDateTime"),
        "seat": _get(s, "seat", "number"),
        "hotel_name": s.get("hotelName"),
    }


def to_snapshot(payload: dict) -> dict:
    travelers = payload.get("travelers") or [{}]
    t0 = travelers[0] or {}
    payment = payload.get("payment") or {}
    return {
        "trip_id": _get(payload, "identification", "id"),
        "record_locator": _get(payload, "identification", "recordLocator"),
        "version": _get(payload, "identification", "version"),
        "segments": [_segment(s) for s in payload.get("segments") or []],
        "traveler": {"name": t0.get("name"), "email": t0.get("email"), "phone": t0.get("phone")},
        "payment": {"type": payment.get("type"), "last4": payment.get("last4")},
    }


def parse(raw: str) -> dict:
    return to_snapshot(json.loads(raw))
