-- ============================================================================
-- SILVER: cleaned, typed segments + data-quality expectations
-- ----------------------------------------------------------------------------
-- This is where we normalize the timestamp (date + time -> a single
-- departure_timestamp) and enforce quality. DLT EXPECTATIONS are the
-- declarative equivalent of the validation you'd otherwise hand-code:
--   - a well-formed timestamp,
--   - a recognized status code,
--   - a non-null O&D.
-- Bad rows are tracked in the pipeline's data-quality metrics (and dropped),
-- so downstream detection only ever sees clean segments.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE silver_segments (
  CONSTRAINT valid_timestamp   EXPECT (departure_timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_status_code EXPECT (code IN ('HX','UN','HK','KK','TK')) ON VIOLATION DROP ROW,
  CONSTRAINT has_od            EXPECT (origin IS NOT NULL AND destination IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned reservation segments with a normalized departure_timestamp and a change_side classification (OLD vs NEW).'
AS
SELECT
  record_id,
  record_locator,
  origin,
  destination,
  upper(code)                                                              AS code,
  to_date(departure_date, 'yyyy-MM-dd')                                    AS departure_date,
  to_timestamp(concat_ws(' ', departure_date, departure_time), 'yyyy-MM-dd HH:mm') AS departure_timestamp,
  carrier,
  -- Airline-cancel/unable statuses are the "old" side of a schedule change;
  -- confirmed/reconfirmed statuses are the "new" side.
  CASE
    WHEN upper(code) IN ('HX','UN')      THEN 'OLD'
    WHEN upper(code) IN ('HK','KK','TK') THEN 'NEW'
  END                                                                      AS change_side,
  _ingested_at,
  _source_file
FROM STREAM(bronze_segments);
