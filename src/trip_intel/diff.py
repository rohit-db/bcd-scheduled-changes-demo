"""Diff one trip version against the previous one and tag what changed.

This is the per-version change tracking BCD runs today in an AWS Lambda: most GDS re-saves change
nothing that matters (NO_SIGNIFICANT_CHANGE); the tags tell each consumer whether to care.
"""
from trip_intel.schedule_change import detect

SEGMENT_FIELDS = ("status", "carrier", "flight_number", "class_of_service", "origin", "destination",
                  "start", "end", "hotel_name")
EMPTY_CHANGE = {"from_version": None, "to_version": None, "tags": [], "changes": [], "schedule_changes": []}


def _key(seg: dict) -> str:
    return seg["segment_id"] or f"{seg['type']}#{seg['segment_number']}"


def diff(prev: dict | None, new: dict) -> dict:
    result = {"from_version": prev["version"] if prev else None, "to_version": new["version"],
              "tags": [], "changes": [], "schedule_changes": []}
    if prev is None:
        result["tags"] = ["NEW_TRIP"]
        return result
    if prev["version"] is not None and new["version"] is not None and new["version"] <= prev["version"]:
        result["tags"] = ["STALE_VERSION"]
        return result

    tags, changes = set(), []
    prev_segs = {_key(s): s for s in prev["segments"]}
    new_segs = {_key(s): s for s in new["segments"]}
    for k in sorted(new_segs.keys() - prev_segs.keys()):
        tags.add("SEGMENT_ADDED")
        changes.append({"segment": k, "field": "*", "old": None, "new": "added"})
    for k in sorted(prev_segs.keys() - new_segs.keys()):
        tags.add("SEGMENT_REMOVED")
        changes.append({"segment": k, "field": "*", "old": "present", "new": None})
    for k in sorted(prev_segs.keys() & new_segs.keys()):
        old_seg, new_seg = prev_segs[k], new_segs[k]
        for field in SEGMENT_FIELDS:
            if old_seg.get(field) != new_seg.get(field):
                tags.add("SEGMENT_MODIFIED")
                changes.append({"segment": k, "field": field, "old": old_seg.get(field), "new": new_seg.get(field)})
        if old_seg.get("seat") != new_seg.get("seat"):
            tags.add("SEAT_REMOVED" if new_seg.get("seat") is None else "SEAT_CHANGED")
            changes.append({"segment": k, "field": "seat", "old": old_seg.get("seat"), "new": new_seg.get("seat")})
    for section, tag in (("traveler", "CONTACT_CHANGED"), ("payment", "PAYMENT_CHANGED")):
        for field, old_value in prev[section].items():
            new_value = new[section].get(field)
            if old_value != new_value:
                tags.add(tag)
                changes.append({"segment": None, "field": f"{section}.{field}", "old": old_value, "new": new_value})

    result["schedule_changes"] = detect(prev, new)
    if result["schedule_changes"]:
        tags.add("SCHEDULE_CHANGE")
    result["tags"] = sorted(tags) or ["NO_SIGNIFICANT_CHANGE"]
    result["changes"] = changes
    return result
