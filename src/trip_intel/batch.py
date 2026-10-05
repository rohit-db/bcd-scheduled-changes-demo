"""Batch (analytics-lane) version of the per-version diff — the same logic the real-time stream runs.

Given every stored version of one trip, sort them, drop duplicate versions, and diff each against the
one before it. Used by the Lakeflow pipeline's gold table so batch and real-time agree by construction.
"""
import json

from trip_intel.diff import diff
from trip_intel.payload import parse

SIGNIFICANT_EXCLUDES = {"NO_SIGNIFICANT_CHANGE", "STALE_VERSION"}


def _first_air(snapshot: dict) -> dict:
    return next((s for s in snapshot["segments"] if s["type"] == "Air"), {})


def diff_trip_versions(rows: list[dict]) -> list[dict]:
    seen, ordered = set(), []
    for row in sorted(rows, key=lambda r: r["version"]):
        if row["version"] not in seen:
            seen.add(row["version"])
            ordered.append(row)
    out, prev = [], None
    for row in ordered:
        try:
            new = parse(row["payload"])
        except (ValueError, TypeError, AttributeError):
            continue  # unparseable payload: skip it, keep diffing the rest of the trip
        change = diff(prev, new)
        air = _first_air(new)
        out.append({
            "trip_id": new["trip_id"],
            "from_version": change["from_version"],
            "to_version": change["to_version"],
            "tags": change["tags"],
            "is_significant": not set(change["tags"]) <= SIGNIFICANT_EXCLUDES,
            "schedule_change_count": len(change["schedule_changes"]),
            "changes": json.dumps(change["changes"]),
            "carrier": air.get("carrier"),
            "origin": air.get("origin"),
            "destination": air.get("destination"),
            "ingested_at": row["ingested_at"],
        })
        prev = new
    return out


def landing_batch(start_seq: int, count: int) -> tuple[list[str], int]:
    """The next `count` generator versions as raw BCD-schema JSON lines, plus the next cursor."""
    from trip_intel.generator import payload_for

    return [payload_for(seq) for seq in range(start_seq, start_seq + count)], start_seq + count
