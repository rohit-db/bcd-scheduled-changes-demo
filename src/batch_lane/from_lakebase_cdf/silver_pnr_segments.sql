-- SILVER: one row per segment per version — the flattened view analysts and Genie query.
CREATE OR REFRESH STREAMING TABLE silver_pnr_segments
COMMENT 'Flattened segments of every trip version (Air and Hotel), from the Lakebase change data feed.'
AS SELECT
  trip_id,
  snapshot.record_locator AS record_locator,
  to_version AS version,
  s.segment_id,
  s.type AS segment_type,
  s.status,
  s.carrier,
  s.flight_number,
  s.origin,
  s.destination,
  to_timestamp(s.start) AS departure_ts,
  s.seat,
  s.hotel_name,
  ingested_at
FROM STREAM(silver_change_events)
LATERAL VIEW explode(snapshot.segments) AS s;
