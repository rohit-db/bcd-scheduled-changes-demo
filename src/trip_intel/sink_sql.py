"""Translate one processor output row into idempotent Postgres statements for Lakebase.

Every statement is safe to replay (RTM is at-least-once): trip_latest only moves forward,
everything else is keyed and uses ON CONFLICT DO NOTHING. Spark hands timestamps to Python as
naive UTC datetimes; they are made explicit UTC so Postgres never applies its session time zone.
"""
import json
from datetime import timezone


def _utc(ts):
    return ts.replace(tzinfo=timezone.utc) if ts is not None and ts.tzinfo is None else ts


def statements(out: dict) -> list[tuple[str, tuple]]:
    out = dict(out, event_ts=_utc(out["event_ts"]), processed_ts=_utc(out["processed_ts"]))
    change = json.loads(out["change_json"])
    snapshot = json.loads(out["snapshot_json"])
    stmts: list[tuple[str, tuple]] = []
    if "STALE_VERSION" not in change["tags"]:
        stmts.append((
            "INSERT INTO trip_latest (trip_id, version, record_locator, snapshot, updated_ts) "
            "VALUES (%s, %s, %s, %s::jsonb, %s) "
            "ON CONFLICT (trip_id) DO UPDATE SET version = EXCLUDED.version, record_locator = EXCLUDED.record_locator, "
            "snapshot = EXCLUDED.snapshot, updated_ts = EXCLUDED.updated_ts "
            "WHERE trip_latest.version < EXCLUDED.version",
            (out["trip_id"], out["version"], snapshot.get("record_locator"), out["snapshot_json"], out["processed_ts"]),
        ))
    stmts.append((
        "INSERT INTO change_events (trip_id, from_version, to_version, tags, changes, schedule_changes, snapshot, "
        "event_ts, detected_ts) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s) "
        "ON CONFLICT (trip_id, to_version) DO NOTHING",
        (out["trip_id"], change["from_version"], out["version"], change["tags"], json.dumps(change["changes"]),
         json.dumps(change["schedule_changes"]), out["snapshot_json"], out["event_ts"], out["processed_ts"]),
    ))
    for hit in json.loads(out["rule_hits_json"]):
        stmts.append((
            "INSERT INTO rule_hits (rule_id, rule_name, trip_id, version, matched_ts) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (rule_id, trip_id, version) DO NOTHING",
            (hit["rule_id"], hit["name"], out["trip_id"], out["version"], out["processed_ts"]),
        ))
    for action in json.loads(out["actions_json"]):
        stmts.append((
            "INSERT INTO action_outbox (id, type, trip_id, version, target, payload, status, created_ts) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'PENDING', %s) ON CONFLICT (id) DO NOTHING",
            (action["id"], action["type"], action["trip_id"], action["version"], action["target"],
             json.dumps(action["payload"]), out["processed_ts"]),
        ))
    return stmts
