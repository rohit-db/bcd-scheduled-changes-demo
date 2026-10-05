#!/usr/bin/env bash
# One-time Lakebase setup for the demo: instance, native-login role, secret, schema.
# Uses scripts/run_sql.py (psycopg) instead of psql.
set -euo pipefail
P=${P:-fevm-stable-71zsua}
PY=${PY:-.venv/bin/python}
INSTANCE=${INSTANCE:-bcd-trip-intel}
SCOPE=${SCOPE:-bcd-trip-intel}

databricks database get-database-instance $INSTANCE --profile $P >/dev/null 2>&1 || \
  databricks database create-database-instance $INSTANCE --capacity CU_1 --enable-pg-native-login --no-wait --profile $P
echo "waiting for instance..."
until [ "$(databricks database get-database-instance $INSTANCE --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["state"])')" = "AVAILABLE" ]; do sleep 15; done
HOST=$(databricks database get-database-instance $INSTANCE --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["read_write_dns"])')
ME=$(databricks current-user me --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["userName"])')
TOKEN=$(databricks database generate-database-credential --json "{\"instance_names\":[\"$INSTANCE\"],\"request_id\":\"setup\"}" --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

APP_PW=$(python3 -c 'import secrets;print(secrets.token_urlsafe(24))')
databricks secrets create-scope $SCOPE --profile $P 2>/dev/null || true
databricks secrets put-secret $SCOPE lakebase-password --string-value "$APP_PW" --profile $P

$PY scripts/run_sql.py --host "$HOST" --user "$ME" --password "$TOKEN" -c "
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trip_intel_app') THEN
    CREATE ROLE trip_intel_app LOGIN PASSWORD '$APP_PW';
  ELSE
    ALTER ROLE trip_intel_app PASSWORD '$APP_PW';
  END IF;
END \$\$;"
$PY scripts/run_sql.py --host "$HOST" --user "$ME" --password "$TOKEN" -f sql/lakebase_schema.sql
echo "LAKEBASE_HOST=$HOST"
