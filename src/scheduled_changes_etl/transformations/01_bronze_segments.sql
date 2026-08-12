-- ============================================================================
-- BRONZE: raw reservation segments landed from the feed (TD API + Kafka -> volume)
-- ----------------------------------------------------------------------------
-- Auto Loader (`read_files` over STREAM) picks up new JSON files as they land,
-- exactly like the real feed would drop PNR segment events into a landing zone.
-- Schema hints keep types stable across schema drift.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_segments
COMMENT 'Raw reservation segments ingested from the landing volume via Auto Loader. One row per segment event.'
AS
SELECT
  *,
  current_timestamp()               AS _ingested_at,
  _metadata.file_path               AS _source_file,
  _metadata.file_modification_time  AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/${volume}/segments',
  format => 'json',
  schemaHints => '
    record_id STRING,
    record_locator STRING,
    origin STRING,
    destination STRING,
    code STRING,
    departure_date STRING,
    departure_time STRING,
    carrier STRING
  '
);
