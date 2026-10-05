-- BRONZE: every raw booking (PNR) version exactly as received — one row per version, untouched.
-- Demo source: JSON-lines files landed in a UC volume (stand-in for Kinesis Firehose -> S3).
-- Production: read the Kinesis stream directly (STREAM read_kinesis(...)) with the same columns.
CREATE OR REFRESH STREAMING TABLE bronze_pnr_versions
COMMENT 'Raw BCD trip/PNR versions as received (one JSON payload per row).'
AS SELECT
  value AS payload,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS landed_at,
  current_timestamp() AS ingested_at
FROM STREAM read_files('${landing_path}', format => 'text');
