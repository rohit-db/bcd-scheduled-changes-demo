"""Actions produced by a trip version: traveler notifications for schedule changes and rule actions.

Action ids are deterministic (type + trip + version + rule/segment), so stream replays never
produce duplicate outbox rows — the id doubles as the EventBridge idempotency key.
"""
import hashlib
import json


def _id(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()


def _flight(seg: dict) -> dict:
    return {k: seg.get(k) for k in ("carrier", "flight_number", "origin", "destination", "start", "end", "status")}


def build_actions(snapshot: dict, change: dict, fired: list) -> list[dict]:
    trip_id, version = snapshot["trip_id"], snapshot["version"]
    actions = []
    for pair in change.get("schedule_changes", []):
        actions.append({
            "id": _id("NOTIFY_TRAVELER_SCHEDULE_CHANGE", trip_id, version, pair["old"]["segment_id"]),
            "type": "NOTIFY_TRAVELER_SCHEDULE_CHANGE",
            "trip_id": trip_id,
            "version": version,
            "target": "traveler-notifications",
            "payload": {
                "record_locator": snapshot["record_locator"],
                "traveler_name": snapshot["traveler"]["name"],
                "traveler_email": snapshot["traveler"]["email"],
                "old_flight": _flight(pair["old"]),
                "new_flight": _flight(pair["new"]),
                "delta_minutes": pair["delta_minutes"],
            },
        })
    for rule in fired:
        action_type = rule.action.get("type", "RULE_MATCHED")
        actions.append({
            "id": _id(action_type, trip_id, version, rule.rule_id),
            "type": action_type,
            "trip_id": trip_id,
            "version": version,
            "target": rule.action.get("target", "default"),
            "payload": {"rule_id": rule.rule_id, "rule_name": rule.name, "record_locator": snapshot["record_locator"],
                        "tags": change.get("tags", [])},
        })
    return actions


def eventbridge_entry(action: dict, bus_name: str) -> dict:
    detail = dict(action["payload"], trip_id=action["trip_id"], version=action["version"], idempotency_key=action["id"])
    return {"Source": "bcd.trip-intel", "DetailType": action["type"], "Detail": json.dumps(detail),
            "EventBusName": bus_name}
