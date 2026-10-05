"""Airline-initiated scheduled-change detection between two versions of the same trip.

OLD: an Air segment that became HX/UN in this version. NEW: a confirmed (HK/KK/TK) Air segment
that did not exist in the previous version. A pair needs the same origin, destination and
departure date, departures within 60 minutes, and must be a unique 1:1 match — ambiguous
re-protections are dropped.
"""
from collections import Counter
from datetime import datetime

OLD_CODES = {"HX", "UN"}
NEW_CODES = {"HK", "KK", "TK"}
MATCH_WINDOW_SECONDS = 60 * 60


def _ts(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def detect(prev: dict, new: dict) -> list[dict]:
    prev_by_id = {s["segment_id"]: s for s in prev["segments"]}
    olds = [
        s for s in new["segments"]
        if s["type"] == "Air" and s["status"] in OLD_CODES
        and (prev_by_id.get(s["segment_id"]) or {}).get("status") not in OLD_CODES
    ]
    news = [
        s for s in new["segments"]
        if s["type"] == "Air" and s["status"] in NEW_CODES and s["segment_id"] not in prev_by_id
    ]
    candidates = []
    for o in olds:
        for n in news:
            o_ts, n_ts = _ts(o["start"]), _ts(n["start"])
            if o_ts is None or n_ts is None or (o_ts.tzinfo is None) != (n_ts.tzinfo is None):
                continue  # unparseable or mixed naive/aware timestamps: not comparable
            if (o["origin"], o["destination"], o["start"][:10]) != (n["origin"], n["destination"], n["start"][:10]):
                continue
            delta = (n_ts - o_ts).total_seconds()
            if abs(delta) <= MATCH_WINDOW_SECONDS:
                candidates.append((o, n, int(delta // 60)))
    old_counts = Counter(id(o) for o, _, _ in candidates)
    new_counts = Counter(id(n) for _, n, _ in candidates)
    return [
        {"old": o, "new": n, "delta_minutes": d}
        for o, n, d in candidates
        if old_counts[id(o)] == 1 and new_counts[id(n)] == 1
    ]
