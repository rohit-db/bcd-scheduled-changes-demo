-- SILVER: one row per segment per version — the flattened view analysts and Genie query.
CREATE OR REFRESH STREAMING TABLE silver_pnr_segments
COMMENT 'Flattened segments of every trip version (Air and Hotel).'
AS SELECT
  trip_id,
  record_locator,
  version,
  s.segmentId AS segment_id,
  s.type AS segment_type,
  s.status.code AS status,
  s.marketingFlight.airlineCode AS carrier,
  s.marketingFlight.flightNumber AS flight_number,
  s.departureAirport.airport.code AS origin,
  s.arrivalAirport.airport.code AS destination,
  to_timestamp(s.startDateTime) AS departure_ts,
  s.seat.number AS seat,
  s.hotelName AS hotel_name,
  ingested_at
FROM STREAM(silver_pnr_versions)
LATERAL VIEW explode(segments) AS s;
