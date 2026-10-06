-- Unity Catalog views over the real-time lane's Lakebase tables, so SQL users and Genie can query what the
-- stream decided next to the batch lane's gold tables. Genie does not accept Lakebase catalog tables directly,
-- so it reads these views.
-- Prerequisite: the bundle's database_catalogs resource (e2demo: rohit_bcd_trip_intel_ops).
-- Replace the catalog/schema names for another target.

CREATE OR REPLACE VIEW users.rohit_bhagwat.rt_rule_hits
COMMENT 'Business rules fired by the real-time stream (live from Lakebase).'
AS SELECT rule_id, rule_name, trip_id, version, matched_ts
FROM rohit_bcd_trip_intel_ops.public.rule_hits;

CREATE OR REPLACE VIEW users.rohit_bhagwat.rt_actions
COMMENT 'Actions the real-time stream queued for AWS (live from the Lakebase outbox). rule_name is null for traveler notifications, which come straight from detected schedule changes.'
AS SELECT
  id,
  type,
  get_json_object(cast(payload AS string), '$.rule_id') AS rule_id,
  get_json_object(cast(payload AS string), '$.rule_name') AS rule_name,
  trip_id,
  version,
  target,
  status,
  created_ts,
  sent_ts
FROM rohit_bcd_trip_intel_ops.public.action_outbox;
