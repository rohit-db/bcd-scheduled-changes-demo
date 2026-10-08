# BCD Trip Intelligence — demo cheat sheet (Thu 2026-10-08, 2:00–3:00pm ET)

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

## T-60 (1:00pm)

1. **Stream running?** Job page (tab 3) should show one active run. If not:
   `databricks bundle run trip_intel_rtm --no-wait -t uat --profile bcd-api-uat` (cluster ~5 min, rows ~6 min).
2. **Start the dispatcher** (its runs stop after 55 min):
   `databricks bundle run trip_intel_dispatcher --no-wait -t uat --profile bcd-api-uat`
3. **Refresh the analytical lane** so it shows minutes, not hours (~1 min):
   `databricks bundle run trip_intel_batch -t uat --profile bcd-api-uat`
4. **App** (tab 4): log in; the "Stream live" dot is green, p50 ≈ 185 ms, the feed is moving, the analytical tier
   shows a few minutes. Rule Studio: no rehearsal rules left active (disable "Traveler email changed" if present).
5. **Genie** (tab 8): ask the warm-up question once so the warehouse is warm:
   "What share of booking versions today were significant vs noise?" (≈14.6%).

## T-15 (1:45pm)

1. **Deck server:** `cd ~/Documents/github/decks/bcd-trip-intel && python3 -m http.server 8011` (skip if already up).
2. **Notebook cluster:** in tab 2, attach to `[dev rohit_bhagwat] rtm-live-notebook` and **Start** it if it has
   auto-terminated (≈5 min). Run **cell 0 (setup) only**.
3. Run the SQL in tab 5 once, so it's warm:
   ```sql
   SELECT type, target, status, eventbridge_entry
   FROM databricks_demo_trip_intel_ops.public.action_outbox
   WHERE status = 'SIMULATED' ORDER BY created_ts DESC LIMIT 5;
   ```
4. Close everything else. Phone on silent.

## Run of show (60 min)

| Time | Show | Say (one line) |
|---|---|---|
| 0:00–0:03 | Deck slide 1 | "We built your change-tracking process on Databricks, in your UAT workspace, on your data structures." |
| 0:03–0:09 | Slides 2–4 | Problem in their numbers; "read the feed once, decide once"; walk the architecture left to right. |
| 0:09–0:17 | **Notebook** (tab 2), cells 1 → 2 → 3 | "No sink, just `display()`. Watch `decided_ms_after_event`: every version compared with the previous one, a few hundred ms after it happened." Cell 3: "85% is noise; this is what matters." |
| 0:17–0:21 | Job (tab 3) + slides 5–6 | "Same logic as one always-on job: one node, Runtime 18.2, Photon off. One trigger line picks real-time or micro-batch." |
| 0:21–0:29 | **App: Ops Console** | KPI cards (p50 ≈ 185 ms, % significant, two tiers). Click a red `Schedule change` row → timeline: "v1 created it, the next versions were noise, then the airline moved the flight. Both rules fired." Slide 7 if they want the code. |
| 0:29–0:39 | **App: Rule Studio** | Ask their architect for a rule. Fallback: "If a traveler's email address changes, notify the profile sync service." Draft → Preview → Approve → watch Hits (first hit in ~20 s). "No code, no deploy." Slide 13. |
| 0:39–0:44 | **App: Traveler** → tab 5 | Accept a card. Then the SQL: "the exact PutEvents entry, with an idempotency key; simulated because UAT has no bus." Slides 8–9: "Lakebase is one sink; the stream can publish straight to your EventBridge." |
| 0:44–0:48 | Slides 10–11 | 73 ms at 100/s on one node at 14% CPU; RTM 185 ms vs the same job as micro-batch ≈1 s; ≈$0.21/h node + EC2, pipeline ≈$0.10/run. |
| 0:48–0:54 | Tabs 6 → 7 → 8, slides 14–15 | Change feed table ("every decision, in Delta, seconds behind"), pipeline graph (DQ: 932,618 passed, 0 dropped), then Genie: "Which carriers had the most schedule changes today?" "We read your feed once." |
| 0:54–1:00 | Slide 16 | Four steps to production; **ask the deciding question**; agree next steps. |

Stop the notebook's displays (cell 5) when you leave the notebook, so they don't compete with anything else.

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
