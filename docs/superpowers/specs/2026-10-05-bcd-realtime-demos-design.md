# BCD Real-Time Trip Intelligence Demos — Design

**Date:** 2026-10-05 · **Demo:** Thu 2026-10-08, 2–3pm ET · **Audience:** BCD (Bojidar, Denis — principal architect) + EPAM
**Build workspace:** FEVM `fevm-serverless-stable-71zsua` (live until 2026-10-28) · **Finale (stretch):** BCD UAT `dbc-24f136f2-50df` (profile `bcd-api-uat`)

## 1. Why

EPAM's prototype (`bcd-hermes` bundle) runs hourly `availableNow` PySpark jobs on classic clusters, so BCD
still believes "batch vs real-time" and "cluster vs serverless" are open questions. Four needs from the
2026-10-01 call are unanswered:

| # | Need (customer words) | Demo answer |
|---|---|---|
| 1 | "Flight is delayed now — I cannot say after one hour" | RTM lane, ms latency, next to an hourly serverless batch lane |
| 2 | Track every PNR version vs the previous one (today: AWS Lambda) | `transformWithState` per `trip_id`, tagged diffs, no version loss |
| 3 | "Rules engine is the core of our system"; JSON-Logic rules, authored by BAs | Rule Studio: NL → JSON Logic → preview → approve → live in seconds |
| 4 | Publish to EventBridge / invoke Lambda (EPAM said impossible) | Action outbox → EventBridge `PutEvents` via UC service credential |

Story format: **standalone use-case demos (Option B)** on BCD's real data structures, plus apps.

## 2. Findings from EPAM's UAT workspace (read-only, 2026-10-05)

- Jobs: `jobs-orchestrator` (hourly cron, PAUSED) → `trips-pipeline` (ingest → parse → build_latest_versions)
  → `air-scheduler-change` (detect → optional Kinesis publish, disabled). Classic DBR 17.3, r5d.xlarge, 1 worker.
  Last full run 9/29: ~13 min for 4 records.
- Kinesis in: `uat01-HermesTripsDS` (us-east-1, credential `kinesis_reader_cred_uat`). Out: `uat01-HermesTripChanges`
  (`kinesis_writer_cred_uat`).
- Tables (`bcd_api_uat`): `bronze.raw_trips_stream`, `silver.trips_all_versions`, `silver.trips_latest_versions`
  (MERGE, CDF), `gold.air_scheduler_changes`. **Only 4 hand-made rows** (`t1`/`t2`, `segmentId: manual-segment`).
- `build_latest_versions` keeps `max_by` per batch → intermediate versions are never diffed.
- Detection SQL pairs prior `HK` → newer `TK/UN/HX`, no 1:1 check; its own header comment says the inverse.
  Gold has 0 rows. **Raise as a question, not a bug claim.**
- Payload schema (must match exactly):
  ```
  identification{id, recordLocator, version}
  segments[{type, segmentId, segmentNumber, status{code}, marketingFlight{airlineCode, flightNumber},
            classOfService{code}, departureAirport{airport{code}}, arrivalAirport{airport{code}},
            startDateTime, endDateTime}]
  ```

## 3. Platform constraints (verified)

- RTM: GA, **classic compute only** (no serverless), dedicated access mode, DBR 18.x LTS. Sources: Kafka, Kinesis,
  MSK, Event Hubs; sinks: Kafka, ForeachWriter. Update output mode only. `transformWithState` row-based only
  (no pandas variant, no event-time timers). Trigger `.trigger(realTime="5 minutes")` (= checkpoint interval).
- FE "How to Create a Realtime Demo" (Confluence 6309020037): **rate source + UDF** works in RTM with no broker;
  Lakebase writes via custom Python sink using a **Postgres password** (OAuth not supported in sink);
  app push via SSE. *Must be verified on day 1 — not in the public reference.*

## 4. Architecture

```
Generator (rate source + deterministic UDF, inside RTM job) ── BCD schema + extensions, 15/s (configurable to 100/s);
  │   50 interleaved trips × 12 versions; scenario mutation at v6, cycling all scenarios
  ▼
RTM lane — classic single-node, DBR 18 LTS
  parse → transformWithState(key=trip_id, ValueState=prev snapshot)
        → diff + change tags → schedule-change detection → JSON-Logic rule eval (rules hot-reloaded)
        → ForeachWriter → Lakebase
Batch lane — Lakebase registered as UC catalog → serverless hourly job snapshots into Delta → Genie
Dispatcher — outbox rows → EventBridge PutEvents (UC service credential); FEVM: SIMULATED status + payload
App (Databricks App, Lakebase resource) — Ops Console · Rule Studio · Traveler view
```

UAT finale: same RTM code with `source=kinesis` options pointed at `uat01-HermesTripsDS` (read-only).

## 5. Data

**Payload extensions** (clearly marked "illustrative extension" in the demo): `segments[].seat{number}`,
`travelers[{name, email, phone}]`, `payment{type, last4}`, Hotel segments (`type: "Hotel"`).

**Scenarios:** normal churn (5–15 versions/booking in minutes, mostly no-op fields), schedule change
(HK→HX + new HK same O&D/date ≤60 min), schedule change with ambiguous match (must be dropped),
seat removed, contact changed, payment changed, hotel added.

**Lakebase tables**
| Table | Purpose |
|---|---|
| `trip_latest` | latest snapshot per trip (traveler view, timeline header) |
| `change_events` | one row per version: trip_id, from_v, to_v, tags[], changes, schedule_changes, snapshot, event_ts, detected_ts |
| `rules` | id, name, nl_text, json_logic, action, status (draft/active/disabled), version, author |
| `rule_hits` | rule_id, trip_id, version, matched_ts |
| `action_outbox` | id (idempotency key), type, payload JSON, target, status (PENDING/SENT/SIMULATED/FAILED), attempts |
| `traveler_responses` | outbox id, response (ACCEPT/REBOOK), ts |

## 6. Logic

- **Diff:** compare prev vs new snapshot field-by-field over a fixed field list → tags
  `SEGMENT_ADDED/REMOVED/MODIFIED`, `SEAT_REMOVED`, `CONTACT_CHANGED`, `PAYMENT_CHANGED`, `SCHEDULE_CHANGE`.
  Out-of-order (version ≤ stored) → tag `STALE_VERSION`, no state update.
- **Schedule change:** OLD ∈ {HX, UN} in new version, NEW ∈ {HK, KK, TK} added; same origin, destination,
  departure date; |Δ departure| ≤ 60 min; keep only unique 1:1 pairs.
- **Rules:** vendored pure-Python JSON-Logic evaluator (subset: `var`, `==`, `!=`, `<`, `>`, `<=`, `>=`,
  `and`, `or`, `!`, `in`, `missing`, `some`, `all`, `none`, `if`). Evaluated against a *rule context*
  `{trip, change: {tags, diff}}`. Same module used by RTM job and app preview. Rules reloaded from Lakebase
  every 5 s inside the processor.
- **Rule Studio NL→rule:** FMAPI chat model with the rule-context schema + few-shot examples → JSON Logic +
  plain-English explanation; validated by parsing + dry-run on recent events before approve.

## 7. App (one Databricks App, three views)

- **Ops Console:** SSE live feed of change_events/rule_hits; per-trip version timeline with diffs;
  latency KPIs (event→detected→displayed, p50/p95); RTM-vs-batch freshness panel; noise-suppressed counter
  ("N versions received, M significant").
- **Rule Studio:** NL input → JSON Logic + explanation → preview matches over last N events → approve →
  shows first live hit.
- **Traveler view:** mobile-style card from outbox (`SCHEDULE_CHANGE`) with Accept/Rebook → writes
  `traveler_responses`.
- Stack: follow the `databricks-apps` skill's recommended scaffold; Lakebase as app resource.

## 8. Cost answer

Compute live from system tables / sizing tool: always-on single-node classic RTM vs serverless hourly
triggered, at 15/s normal, 100/s peak, 1–1.5M versions/day. Present as "per use case", not one winner.

## 9. Out of scope

Replacing EPAM's code; real EventBridge in FEVM (no AWS rights); writing to BCD's Kinesis; Genie app
embedding (Genie shown in workspace if time); production hardening.

## 10. Risks

| Risk | Mitigation |
|---|---|
| rate source unsupported in RTM | Day-1 spike; fallback: `ProcessingTime` trigger on classic (~1 s), say so honestly |
| Lakebase ForeachWriter auth | Postgres password per FE guide, stored in secret scope |
| FMAPI model availability in FEVM | Check endpoints day 1; fallback any available chat endpoint |
| UAT finale: no traffic, cluster perms | Stretch only; ask EPAM to push test PNRs; rehearse Thu morning |
| 3-day timeline | Cut order: batch-lane polish → traveler view → Rule Studio NL (keep manual JSON) |
