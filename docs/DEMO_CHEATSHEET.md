# BCD Trip Intelligence — demo cheat sheet (Thu 2026-10-15, 2:00–3:00pm ET)

**Audience:** BCD engineering (their architect and team), EPAM. **Goal:** show BCD's own change-tracking process running in
real time on Databricks, in *their* UAT workspace, and leave with the one question that decides the design: *which
consumers truly need sub-second?*
**Workspace:** BCD UAT, https://dbc-24f136f2-50df.cloud.databricks.com/?o=7474657914890252 (profile `bcd-api-uat`).
Commands below run from `bundles/trip_intel` in the repo. Background and reasoning: [`WHAT_WE_BUILT.md`](WHAT_WE_BUILT.md);
IDs and measurements: [`DEMO_RUNBOOK.md`](DEMO_RUNBOOK.md).

## Tabs to open, in order

| # | Tab | URL |
|---|---|---|
| 1 | Deck | http://localhost:8011/bcd-trip-intel.dc.html (start the server first, see T-15) |
| 2 | RTM live notebook | https://dbc-24f136f2-50df.cloud.databricks.com/?o=7474657914890252#notebook/3754832766807209 |
| 3 | Real-time job | https://dbc-24f136f2-50df.cloud.databricks.com/jobs/141818085214296?o=7474657914890252 |
| 4 | App | https://databricks-bcd-trip-intel-7474657914890252.aws.databricksapps.com |
| 5 | SQL editor (EventBridge entries) | query below, on warehouse `databricks-bcd-trip-intel-wh` |
| 6 | Change feed table | https://dbc-24f136f2-50df.cloud.databricks.com/explore/data/bcd_api_uat/databricks_demo/lb_change_events_history?o=7474657914890252 |
| 7 | Analytical pipeline | https://dbc-24f136f2-50df.cloud.databricks.com/pipelines/c7d5cc27-d8c0-4309-b35d-e18054d22bb1?o=7474657914890252 |
| 8 | Genie "BCD Trip Changes" | https://dbc-24f136f2-50df.cloud.databricks.com/genie/rooms/01f1c30ff44310c6b475edd6a2e6c34c?o=7474657914890252 |

## Already running (started 12:00 on 10/15)

Stream (run 241040172461397, fresh checkpoint, trips from T0300000), dispatcher, UAT app (commit `c0ba9dc`),
analytical lane refreshed at 12:04. Present the app in **dark mode** (sidebar → Dark mode): it matches the
Databricks reference demo.

## T-15 (1:45pm)

1. Refresh the analytical lane so gold is minutes old: `databricks bundle run trip_intel_batch -t uat --profile bcd-api-uat`
2. App (tab 4): green "Stream live"; change-type tiles show ▲/▼ deltas; Data quality: checks green.
3. Warm Genie once: Data quality → first suggested question (~20 s).
4. Deck server: `cd ~/Documents/github/decks/bcd-trip-intel && python3 -m http.server 8011`.
5. Notebook (tab 2): attach to `[dev rohit_bhagwat] rtm-live-notebook`, start it if terminated, run cell 0 only.

## Run of show (60 min)

| Time | Show | Say (one line) |
|---|---|---|
| 0:00–0:04 | Deck 1–3 (or skip) | Their problem in their numbers; "two stacks: Lambda/EventBridge for ops, S3 + hourly batch for analytics." |
| 0:04–0:08 | **App: Architecture** | "The Databricks platform" view: Lakeflow, Lakehouse, Lakebase on one open lake, one Unity Catalog; green dots = running in this demo. Then diagram 1 (reference architecture). Diagrams 2–6 on demand. |
| 0:08–0:14 | **Notebook** cells 1 → 3 | Real-Time Mode live with no sink: every version diffed, `decided_ms_after_event` in the low hundreds of ms. |
| 0:14–0:22 | **App: Live Ops Console** | KPIs + sparklines; change-type tiles (▲/▼ vs the previous 5 min); click a schedule change → timeline. "This is your track-changes Lambda as one stateful step." |
| 0:22–0:30 | **App: Analytics** | Live 5-second ticker; versions vs significant (the gap is noise that never reaches your systems); latency distribution; schedule changes by route; rules fired → actions. "Same Lakebase, no warehouse, no dashboard to build." |
| 0:30–0:38 | **App: Data quality** | Lineage strip (Kinesis → Lakebase → change feed → silver → gold, live counts); checks: expectations + every decision reconciled across layers; Ask Genie a steward question. "This is the governance and data-quality tool, on the same copy of the data, with no UI to build." |
| 0:38–0:46 | **App: Rule Studio → Traveler** | Their rule (or the email-change rule) live in ~5 s; traveler accepts; EventBridge entry (SQL tab). Deck 9: Lakebase is one sink, EventBridge can be another. |
| 0:46–0:52 | Architecture → diagram 2 (+ deck 10–12 if needed) | 73 ms at peak on one node; RTM vs micro-batch; cost. Databricks' own claim: sub-100 ms and >260k events/s at sub-100 ms p99. |
| 0:52–1:00 | Deck 17 | Four steps to production; **ask: which consumers truly need sub-second?** |

## Numbers to have ready

| | |
|---|---|
| Event → decision, 15/s | p50 185 ms · p95 254 ms |
| Event → decision, 100/s peak | p50 73 ms · p95 108 ms · p99 124 ms · 14% CPU |
| Same job as 1 s micro-batch | p50 987 ms |
| New rule live | ≈5 s after approval |
| Noise | ≈85% of versions |
| Cost | node 1.37 DBU/h ≈ $0.21/h list + EC2 (≈$150/month 24×7); pipeline ≈ $0.10/run |
| Their Kinesis | `uat01-HermesTripsDS`, 4 shards; read-only |

## If they ask

- **"Schedule changes is already live in batch; is streaming needed?"** Batch notifies the traveler up to an hour
  late. Real-time is ~0.2 s on one node at ≈$0.21/h, and it's the same code: one trigger line switches real-time vs
  micro-batch, so the latency decision is cheap to change later.
- **"This competes with our AWS investment."** It replaces the Lambda diff and the sync pipelines, and adds
  governance, data quality, analytics and Genie on the same data. Databricks released the same pattern this week
  ("Run Live Energy Trading, Risk, and Analytics in Databricks": Real-Time Mode → Lakebase → App + dashboards + Genie).
- **"Data governance and quality is the real priority."** That's the Data quality tab: expectations, cross-layer
  reconciliation, lineage, Unity Catalog permissions, Genie for stewards. Faster time to market because it's the
  same pipeline, lower ops cost because there's no second stack.

- **"Do we need two pipelines?"** Only if some consumer needs sub-second. Otherwise one serverless pipeline does it all.
  Either way the feed is read once and the logic exists once.
- **"Why not serverless for the stream?"** Standalone Real-Time Mode is classic-only. Real-time mode inside Lakeflow
  pipelines is in preview and today only writes to Kafka-protocol sinks; with a Lakebase sink (roadmap) the job could
  move in.
- **"Can it go straight to EventBridge / Kafka / DynamoDB?"** Yes: Kafka/MSK natively; anything else through a
  ForeachWriter. We used an outbox for replay-safe delivery; direct is one hop fewer.
- **"Is it reading our Kinesis?"** The demo stream uses a generator in your JSON shape. We connected to all 4 shards of
  `uat01-HermesTripsDS` read-only; for production, an enhanced fan-out consumer (an IAM change on your side), so we
  never compete with other readers.
- **"Trips change months apart. Won't state grow?"** Yes, today. Next step: keep each trip's latest state in Lakebase,
  indexed by trip ID, still in milliseconds (or a state TTL).
- **"How does analytics stay consistent?"** Gold uses the stream's own tags via Lakebase's change data feed. Nothing
  re-diffs.
- **"Governance?"** Everything in Unity Catalog: tables, Lakebase, volumes, service credentials. One bundle deploys it.
- **"What's a schedule change, exactly?"** An HX/UN segment paired with a new HK/KK/TK on the same route and day within
  60 minutes. *Ask:* is that what real GDS data does? (UAT test records flip status on the same segment.)

## Questions to ask them

1. Which consumers truly need sub-second, and which are fine with seconds or minutes?
2. Can we register an enhanced fan-out consumer on `HermesTripsDS` (IAM on your side)?
3. Can your architect share three real rules from the current service?
4. How does a schedule change appear in real GDS data?
5. Who owns the EventBridge bus and role for a UAT send test?

## If something breaks

| Problem | Do this |
|---|---|
| App won't load | Local copy on UAT data: from `src/`, run the app with the env in `DEMO_RUNBOOK.md` on port 8766 |
| Feed not moving / p50 high | Check the job run (tab 3); the stream may still be starting. Talk through slides 6–8 meanwhile. |
| Notebook display errors | Re-run cell 0, then the display cell; each run uses fresh checkpoints |
| Genie slow | It's warming the warehouse; show slide 15 and come back |
| Everything | Narrated video: `docs/video/bcd_trip_intel_demo.mp4` (local file) |

## After the call (same afternoon)

1. Notebook: run cell 5 (stops the displays); the cluster auto-terminates 30 min later.
2. Stop the stream and dispatcher (nothing should run overnight on BCD's bill):
   `databricks jobs list-runs --active-only --profile bcd-api-uat`, then
   `databricks jobs cancel-run <run_id> --profile bcd-api-uat` for each.
3. Decide what to do with the Lakebase instance (it keeps running), and send the follow-up links
   (`WHAT_WE_BUILT.md` → "Customer stories and links we can share").
