-- SILVER: parsed, typed booking versions with data-quality expectations.
-- Rows without a trip id or version are dropped (and counted in the pipeline's DQ metrics).
CREATE OR REFRESH STREAMING TABLE silver_pnr_versions (
  CONSTRAINT valid_trip_id EXPECT (trip_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_version EXPECT (version IS NOT NULL AND version > 0) ON VIOLATION DROP ROW,
  CONSTRAINT has_segments EXPECT (size(segments) > 0)
)
COMMENT 'Parsed BCD trip versions: identification, segments, traveler, payment.'
AS WITH parsed AS (
  SELECT from_json(payload, 'STRUCT<identification: STRUCT<id: STRING, recordLocator: STRING, version: BIGINT>,
    segments: ARRAY<STRUCT<type: STRING, segmentId: STRING, segmentNumber: BIGINT, status: STRUCT<code: STRING>,
      marketingFlight: STRUCT<airlineCode: STRING, flightNumber: STRING>, classOfService: STRUCT<code: STRING>,
      departureAirport: STRUCT<airport: STRUCT<code: STRING>>, arrivalAirport: STRUCT<airport: STRUCT<code: STRING>>,
      startDateTime: STRING, endDateTime: STRING, seat: STRUCT<number: STRING>, hotelName: STRING>>,
    travelers: ARRAY<STRUCT<name: STRING, email: STRING, phone: STRING>>,
    payment: STRUCT<type: STRING, last4: STRING>, remarks: STRING>') AS p,
    payload, landed_at, ingested_at
  FROM STREAM(bronze_pnr_versions)
)
SELECT
  p.identification.id AS trip_id,
  p.identification.recordLocator AS record_locator,
  CAST(p.identification.version AS INT) AS version,
  p.segments,
  p.travelers[0] AS traveler,
  p.payment,
  payload,
  landed_at,
  ingested_at
FROM parsed;
