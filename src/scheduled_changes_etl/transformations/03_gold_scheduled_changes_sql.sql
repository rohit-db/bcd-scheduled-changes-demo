-- ============================================================================
-- GOLD (SQL): scheduled-change detection — the declarative version of the
-- customer's PySpark notebook.
-- ----------------------------------------------------------------------------
-- Logic (identical to the notebook, and to the Python transform alongside):
--   1. OLD segments (HX/UN) are candidate "before" states;
--      NEW segments (HK/KK/TK) are candidate "after" states.
--   2. Pair them on origin, destination, departure_date, and a departure-time
--      difference of <= 60 minutes.
--   3. Keep only UNIQUE 1:1 matches — if one OLD maps to multiple NEW (or vice
--      versa) the pairing is ambiguous, so we drop it.
--
-- Materialized view: full recompute over the current segment set. This is the
-- routing decision table — every row here is a detected scheduled change.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_scheduled_changes_sql (
  record_locator          COMMENT 'GDS PNR locator',
  origin                  COMMENT 'Origin airport',
  destination             COMMENT 'Destination airport',
  departure_date          COMMENT 'Scheduled departure date',
  old_departure_timestamp COMMENT 'Departure time on the cancelled/unable (OLD) segment',
  new_departure_timestamp COMMENT 'Departure time on the confirmed (NEW) segment',
  time_difference_minutes COMMENT 'Absolute minutes between old and new departure',
  carrier                 COMMENT 'Operating carrier',
  is_scheduled_change     COMMENT 'Always TRUE here — this table is the positive routing decision'
)
COMMENT 'Detected airline-initiated scheduled changes (unique 1:1 OLD->NEW segment matches within 60 minutes). This is the routing decision table.'
AS
WITH old_seg AS (
  SELECT record_id, record_locator, origin, destination, departure_date,
         departure_timestamp, carrier
  FROM silver_segments WHERE change_side = 'OLD'
),
new_seg AS (
  SELECT record_id, record_locator, origin, destination, departure_date,
         departure_timestamp, carrier
  FROM silver_segments WHERE change_side = 'NEW'
),
candidate_pairs AS (
  SELECT
    o.record_id AS old_record_id,
    n.record_id AS new_record_id,
    coalesce(o.record_locator, n.record_locator)              AS record_locator,
    o.origin, o.destination, o.departure_date,
    o.departure_timestamp                                     AS old_departure_timestamp,
    n.departure_timestamp                                     AS new_departure_timestamp,
    CAST(abs(unix_timestamp(o.departure_timestamp) - unix_timestamp(n.departure_timestamp)) / 60 AS INT)
                                                              AS time_difference_minutes,
    o.carrier
  FROM old_seg o
  JOIN new_seg n
    ON  o.origin = n.origin
    AND o.destination = n.destination
    AND o.departure_date = n.departure_date
    AND abs(unix_timestamp(o.departure_timestamp) - unix_timestamp(n.departure_timestamp)) <= 60 * 60
),
counted AS (
  SELECT *,
    count(*) OVER (PARTITION BY old_record_id) AS old_match_count,
    count(*) OVER (PARTITION BY new_record_id) AS new_match_count
  FROM candidate_pairs
)
SELECT
  record_locator, origin, destination, departure_date,
  old_departure_timestamp, new_departure_timestamp,
  time_difference_minutes, carrier,
  TRUE AS is_scheduled_change
FROM counted
WHERE old_match_count = 1 AND new_match_count = 1;
