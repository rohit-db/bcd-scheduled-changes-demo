-- GOLD: every version of every trip with what changed. Same columns as the file-based lane's gold table, but no
-- re-diffing: the tags come from the real-time stream, so analytics and operations agree by construction.
CREATE OR REFRESH MATERIALIZED VIEW gold_version_changes
COMMENT 'Per-version change tags for every trip, as decided by the real-time stream (via Lakebase CDF).'
AS SELECT
  trip_id,
  from_version,
  to_version,
  tags,
  size(array_except(tags, array('NO_SIGNIFICANT_CHANGE', 'STALE_VERSION'))) > 0 AS is_significant,
  coalesce(json_array_length(schedule_changes), 0) AS schedule_change_count,
  changes,
  try_element_at(filter(snapshot.segments, s -> s.type = 'Air'), 1).carrier AS carrier,
  try_element_at(filter(snapshot.segments, s -> s.type = 'Air'), 1).origin AS origin,
  try_element_at(filter(snapshot.segments, s -> s.type = 'Air'), 1).destination AS destination,
  event_ts,
  ingested_at
FROM silver_change_events;
