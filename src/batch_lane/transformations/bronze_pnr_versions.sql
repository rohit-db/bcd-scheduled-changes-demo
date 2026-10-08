-- BRONZE: every raw booking (PNR) version exactly as received — one row per version, untouched.
-- Fed by an append flow from landed files (synthetic demo data); `source` records where each row came from.
-- The uat target doesn't use this lane: it builds silver/gold from Lakebase's change data feed instead.
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
