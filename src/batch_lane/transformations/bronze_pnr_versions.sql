-- BRONZE: every raw booking (PNR) version exactly as received — one row per version, untouched.
-- One table, fed by append flows: landed files (synthetic demo data) here, and BCD's Kinesis stream
-- (bronze_from_kinesis.py, only when the pipeline is configured with a stream). `source` says which.
CREATE OR REFRESH STREAMING TABLE bronze_pnr_versions
COMMENT 'Raw BCD trip/PNR versions as received (one JSON payload per row), from every source.';

CREATE FLOW bronze_from_landing
AS INSERT INTO bronze_pnr_versions BY NAME
SELECT
  value AS payload,
  'landing' AS source,
  _metadata.file_path AS source_ref,
  _metadata.file_modification_time AS landed_at,
  current_timestamp() AS ingested_at
FROM STREAM read_files('${landing_path}', format => 'text');
