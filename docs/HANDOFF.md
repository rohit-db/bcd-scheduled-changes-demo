# Handoff — state as of 2026-10-08 ~11:15 ET (demo with BCD today 2–3pm ET, in BCD's UAT workspace)

Start a new session by reading this file, then `README.md`, `docs/DEMO_RUNBOOK.md` (UAT section) and
`docs/WHAT_WE_BUILT.md`. Branch `feat/realtime-demos`; last commit `67e80d0` (not pushed).

## What's built (all validated in BCD UAT, profile `bcd-api-uat`, target `uat`)
- **Real-time lane:** `src/rtm/job.py`, Real-Time Mode on 1× m5d.2xlarge, DBR `18.2.x-scala2.13`, Photon off,
  on-demand. `transformWithState` (row-based Python) keeps previous version per trip → `trip_intel` diff/tags/rules →
  `LakebaseSink` ForeachWriter (pipelined, idempotent, reconnect+retry ×3) → Lakebase. Logs `RTM_LATENCY` per batch.
  Flags: `--source rate|kinesis`, `--rows-per-second`, `--seq-offset`, `--trigger realtime|processing`, `--pg-schema`,
  `--kinesis-*`, `--kinesis-initial-position`, `--debug-metrics`, `--checkpoint`.
- **Lakebase** instance/project `databricks-trip-intel` (host `ep-bitter-fog-d2w6wju5.database.us-east-1.cloud.databricks.com`),
  role `trip_intel_app`, secret scope `databricks-trip-intel` (lakebase-password, lakebase-host, analytics-schema).
  Postgres schemas: `public` (demo), `microbatch`, `load100` (side-by-side tests). Registered in UC as
  catalog `databricks_demo_trip_intel_ops`.
- **Dispatcher** (job 196400368671395): outbox → EventBridge, SIMULATED (no bus).
- **Batch lane** (job 608337505977714 + serverless pipeline): bronze (append flows: landed files + read-only Kinesis
  `uat01-HermesTripsDS`) → silver (DQ) → gold (same diff library) → `gold_daily_change_summary`.
- **UC views** `bcd_api_uat.databricks_demo.rt_rule_hits`, `rt_actions` over the Lakebase catalog.
- **Genie space** "BCD Trip Changes" `01f1c30ff44310c6b475edd6a2e6c34c` (gold + rt_ views; def `genie/bcd_trip_changes_uat.json`).
  Verified answers: 14.58% significant; rules hits = actions (27,970 / 9,324 / 85).
- **App** `databricks-bcd-trip-intel`, RUNNING, deployed from private repo `rohit-db/bcd-trip-intel-app` commit `78a43d8`
  (publish with `scripts/publish_app_repo.sh`). URL https://databricks-bcd-trip-intel-7474657914890252.aws.databricksapps.com
  — not yet visually verified in a browser. Fallback: local app on UAT data, port 8766.
- **Docs:** README (rewritten), WHAT_WE_BUILT, DEMO_RUNBOOK (UAT asset table + T-60), architecture diagrams
  (`docs/architecture/`, render with `scripts/render_diagrams.sh`), video `docs/video/bcd_trip_intel_demo.mp4` (UAT data).
- **e2-demo** (profile `e2-demo-field-eng`, target `e2demo`) has the same stack as a fallback.

## Measured (UAT, 2026-10-07)
RTM 15/s: detect p50/p95/p99 185/254/776 ms, in Lakebase p50 284 ms; Spark e2e p50 285. Micro-batch 1 s, same job:
p50 987 ms. RTM 100/s: 73/108/124 ms, 14% CPU. Cost: RTM node 1.37 DBU/h (≈$0.21/h list + EC2); pipeline ≈$0.10/run.
Kinesis: DBR 18.2 RTM rejects polling (needs EFO); DBR 19 polling works, 4 shards, ~2.5 GetRecords/s/shard (stopped to
avoid throttling EPAM). 4 shards + 4 shuffle partitions = 8 slots = exactly one node.

## Running right now (on BCD's bill)
- UAT RTM stream run **650479185230131** (bundle defaults: 15/s, checkpoint `rtm_demo_20261008`, `--seq-offset 2400000`,
  trips from T0200000). Dispatcher run **160827610670013**. Cancel both after the demo.
- Local app on port 8766 (UAT data), restarted on the freshness fix.

## Done 2026-10-08 (Uday's advice): ingest once via Lakebase change data feed — WORKING in UAT
- CDF config `public` (created from the UI after `REPLICA IDENTITY FULL` on all six tables; the earlier CLI config,
  created before that, skipped every table and was deleted). Delta history tables `lb_<table>_history` in
  `bcd_api_uat.databricks_demo`, seconds behind Lakebase.
- UAT analytical pipeline now reads `lb_change_events_history` (`src/batch_lane/from_lakebase_cdf/`):
  `silver_change_events` → `silver_pnr_segments`, `gold_version_changes` → `gold_daily_change_summary`. Same output
  names/columns, so app + Genie unchanged. Full refresh: 932,618 versions, 0 dropped by expectations. Incremental run:
  event → gold in seconds after a pipeline run. e2demo keeps the file-based lane (pipeline + batch job are per-target).
- UAT batch job = refresh-only, `max_concurrent_runs: 1`. Old `bronze_pnr_versions` / `silver_pnr_versions` are left
  (incl. 12 real Kinesis records) but are no longer in the pipeline.
- Genie UAT space: "today" now uses `event_ts` (column notes + example SQL); answered 14.69% for today.
- App fix: freshness cache could get stuck on a hung/failed warehouse query (silently showed 22 h); now logs errors and
  restarts a refresh stuck >180 s. Deployed to UAT app from Git commit `8538c46`.
- Docs + diagrams 01, 04, 07, 08 updated for the CDF lane.

## Changes still to consider (from Uday)
- **Unbounded state:** trips can change months apart, so `transformWithState` state grows without bound. Uday's
  alternative: keep state in Lakebase — on each version, MERGE/upsert by trip_id (indexed) in Postgres (stored proc or
  sink-side read-modify-write), still ms latency. Smallest change that keeps the shared Python library: stateless stream,
  sink reads `trip_latest ... FOR UPDATE`, diffs, writes (repartition by trip_id). Not started.
- Demo flow per Uday: show the Structured Streaming RTM job first, then say SDP real-time is in preview.
- Positioning decision (user): replacing BCD's Lambda is fine; don't reword to "augment".

## Demo-day T-60 (UAT) — see DEMO_RUNBOOK
Stream running (above) · run batch job once · start dispatcher if showing EventBridge · open app URL once ·
disable rehearsal rules · stop everything after the demo.

## Gotchas learned
- Reusing a `--seq-offset` writes nothing (idempotent sink skips known trips); trips so far reach ~T0150xxx.
- Bulk DELETE in BCD's Lakebase is blocked without explicit approval; use new offsets/schemas instead.
- UAT Apps must deploy from Git; the Git credential must be on the app (CLI caller credential must be a PAT).
- Git hook false-positive: don't put `grep -n` / `find -name` in the same command as `git commit`.
- Genie serialized_space: tables and column_configs must be sorted; Lakebase catalog tables need UC views for Genie.
