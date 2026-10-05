-- Lakebase (Postgres) schema for the BCD trip-intelligence demo.
-- Written by the RTM ForeachWriter and the Databricks App; all writes are idempotent.

CREATE TABLE IF NOT EXISTS trip_latest (
  trip_id        text PRIMARY KEY,
  version        int NOT NULL,
  record_locator text,
  snapshot       jsonb NOT NULL,
  updated_ts     timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS change_events (
  id               bigserial PRIMARY KEY,
  trip_id          text NOT NULL,
  from_version     int,
  to_version       int NOT NULL,
  tags             text[] NOT NULL,
  changes          jsonb NOT NULL,
  schedule_changes jsonb NOT NULL,
  snapshot         jsonb NOT NULL,
  event_ts         timestamptz NOT NULL,
  detected_ts      timestamptz NOT NULL,
  written_ts       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (trip_id, to_version)
);
CREATE INDEX IF NOT EXISTS change_events_trip ON change_events (trip_id, to_version);
CREATE INDEX IF NOT EXISTS change_events_written ON change_events (written_ts);

CREATE TABLE IF NOT EXISTS rules (
  rule_id      text PRIMARY KEY,
  name         text NOT NULL,
  nl_text      text,
  json_logic   jsonb NOT NULL,
  action       jsonb NOT NULL,
  status       text NOT NULL CHECK (status IN ('draft', 'active', 'disabled')),
  author       text,
  created_ts   timestamptz NOT NULL DEFAULT now(),
  activated_ts timestamptz
);

CREATE TABLE IF NOT EXISTS rule_hits (
  id         bigserial PRIMARY KEY,
  rule_id    text NOT NULL,
  rule_name  text NOT NULL,
  trip_id    text NOT NULL,
  version    int NOT NULL,
  matched_ts timestamptz NOT NULL,
  UNIQUE (rule_id, trip_id, version)
);
CREATE INDEX IF NOT EXISTS rule_hits_trip_version ON rule_hits (trip_id, version);

CREATE TABLE IF NOT EXISTS action_outbox (
  id          text PRIMARY KEY,
  type        text NOT NULL,
  trip_id     text NOT NULL,
  version     int NOT NULL,
  target      text NOT NULL,
  payload     jsonb NOT NULL,
  status      text NOT NULL CHECK (status IN ('PENDING', 'SENT', 'SIMULATED', 'FAILED')),
  attempts    int NOT NULL DEFAULT 0,
  last_error  text,
  eventbridge_entry jsonb,
  created_ts  timestamptz NOT NULL,
  sent_ts     timestamptz
);
CREATE INDEX IF NOT EXISTS action_outbox_pending ON action_outbox (status, created_ts);

CREATE TABLE IF NOT EXISTS traveler_responses (
  outbox_id   text PRIMARY KEY REFERENCES action_outbox (id),
  response    text NOT NULL CHECK (response IN ('ACCEPT', 'REBOOK')),
  response_ts timestamptz NOT NULL DEFAULT now()
);

-- Seed rules (active) so the stream has something to evaluate on first start.
INSERT INTO rules (rule_id, name, nl_text, json_logic, action, status, author, activated_ts) VALUES
('seed-schedule-change', 'Notify ops on schedule change',
 'When the airline changes the schedule, alert the operations team.',
 '{"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]}',
 '{"type": "ALERT_OPS", "target": "ops-queue"}', 'active', 'seed', now()),
('seed-missing-seat', 'Confirmed flight without a seat',
 'If a confirmed flight segment has no seat, ask the seat-assignment service to assign one.',
 '{"some": [{"var": "trip.segments"}, {"and": [{"==": [{"var": "type"}, "Air"]}, {"in": [{"var": "status"}, ["HK", "KK", "TK"]]}, {"missing": "seat"}]}]}',
 '{"type": "INVOKE_SERVICE", "target": "seat-assignment-service"}', 'active', 'seed', now())
ON CONFLICT (rule_id) DO NOTHING;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trip_intel_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trip_intel_app;
