"""Drain action_outbox to AWS EventBridge (PutEvents) using a UC service credential.

With no --bus-name the dispatcher runs in SIMULATED mode: it builds the exact PutEvents entry,
stores it on the row, and marks it SIMULATED — so the demo shows the payload without AWS rights.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg

# Bundle syncs the repo as workspace files; make src/ importable (no wheel on serverless).
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(sys._getframe().f_code.co_filename)), "..")))

from rtm.lakebase import conninfo  # noqa: E402
from trip_intel.outbox import eventbridge_entry  # noqa: E402

CLAIM = (
    "SELECT id, type, trip_id, version, target, payload FROM action_outbox "
    "WHERE status = 'PENDING' ORDER BY created_ts LIMIT 10 FOR UPDATE SKIP LOCKED"
)


def make_client(credential: str, region: str):
    import boto3
    from databricks.sdk.runtime import dbutils

    session = boto3.Session(botocore_session=dbutils.credentials.getServiceCredentialsProvider(credential), region_name=region)
    return session.client("events")


def drain_once(conn, bus_name: str, client) -> int:
    with conn.transaction():
        rows = conn.execute(CLAIM).fetchall()
        for row in rows:
            action = {"id": row[0], "type": row[1], "trip_id": row[2], "version": row[3], "target": row[4], "payload": row[5]}
            entry = eventbridge_entry(action, bus_name or "simulated-bus")
            status, error = "SIMULATED", None
            if client is not None:
                resp = client.put_events(Entries=[entry])
                status = "FAILED" if resp.get("FailedEntryCount") else "SENT"
                error = json.dumps(resp["Entries"][0]) if status == "FAILED" else None
            conn.execute(
                "UPDATE action_outbox SET status = %s, attempts = attempts + 1, last_error = %s, "
                "eventbridge_entry = %s::jsonb, sent_ts = %s WHERE id = %s",
                (status, error, json.dumps(entry), datetime.now(timezone.utc), action["id"]),
            )
    return len(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lakebase-host", required=True)
    p.add_argument("--secret-scope", default="bcd-trip-intel")
    p.add_argument("--bus-name", default="")
    p.add_argument("--service-credential", default="")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--run-minutes", type=float, default=55)
    args = p.parse_args()
    from databricks.sdk.runtime import dbutils

    dsn = conninfo(args.lakebase_host, dbutils.secrets.get(args.secret_scope, "lakebase-password"))
    client = make_client(args.service_credential, args.region) if args.bus_name else None
    deadline = time.monotonic() + args.run_minutes * 60
    with psycopg.connect(dsn) as conn:
        while time.monotonic() < deadline:
            if drain_once(conn, args.bus_name, client) == 0:
                time.sleep(0.5)


if __name__ == "__main__":
    main()
