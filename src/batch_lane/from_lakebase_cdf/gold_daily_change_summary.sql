-- GOLD: daily counts by change type and carrier — the "how many versions mattered?" view for BI / Genie.
-- Day is when the booking version happened (event time), not when the pipeline ran.
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_change_summary
COMMENT 'Daily version counts by change tag and carrier.'
AS SELECT
  date(event_ts) AS day,
  carrier,
  origin,
  destination,
  tag,
  count(*) AS versions,
  count(DISTINCT trip_id) AS trips
FROM gold_version_changes
LATERAL VIEW explode(tags) t AS tag
GROUP BY ALL;
