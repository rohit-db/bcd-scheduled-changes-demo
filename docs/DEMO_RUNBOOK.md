# BCD Real-Time Demos — Runbook (Thu 2026-10-08, 2–3pm ET)

The demo runs in one of two workspaces from the same bundle (`bundles/trip_intel`):

- **BCD UAT** (target `uat`, profile `bcd-api-uat`): their workspace, schema `bcd_api_uat.databricks_demo`, Lakebase
  `databricks-trip-intel`. This is where the latest measurements were taken. Use the "UAT (BCD's workspace) — T-60"
  section below.
- **Demo workspace** (target `e2demo`, profile `e2-demo-field-eng`): the fallback, with the Genie space over the
  stream's decisions. Table below.

FEVM serverless workspaces reject classic clusters (Real-Time Mode is classic-only), and FEVM's IP access
list blocks e2-demo from reaching FEVM Lakebase, which is why the demo build moved to e2-demo.

| Thing (demo workspace) | Where |
|---|---|
| App | https://rohit-bcd-trip-intel-1444828305810485.aws.databricksapps.com |
| RTM job | `[dev rohit_bhagwat] trip_intel_rtm` (job 598063876765561) |
| Lakebase | instance `rohit-bcd-trip-intel` · `ep-spring-rice-d1tg6e60.database.us-west-2.cloud.databricks.com`; registered in UC as `rohit_bcd_trip_intel_ops` |
| Batch pipeline | `[dev rohit_bhagwat] trip_intel_batch_pipeline` → `users.rohit_bhagwat.bronze_pnr_versions`, `silver_pnr_versions`, `silver_pnr_segments`, `gold_version_changes`, `gold_daily_change_summary` |
| Batch job | `[dev rohit_bhagwat] trip_intel_batch` (land versions → refresh pipeline), hourly |
| Genie space | "BCD Trip Changes" — `01f1c1041883164e9f82e7116000e7bb` (gold + `rt_rule_hits`, `rt_actions` views) |
| Secret | scope `rohit-bcd-trip-intel`, key `lakebase-password` (role `trip_intel_app`) |

Commands in the next section run from `bundles/trip_intel` with `--profile e2-demo-field-eng -t e2demo`.

## T-60 min (demo workspace)
- Start stream: `databricks bundle run trip_intel_rtm --no-wait` (cluster ~5 min; first rows ~6 min).
- Start dispatcher: `databricks bundle run trip_intel_dispatcher --no-wait`.
- Run the batch lane once: `databricks bundle run trip_intel_batch` (~2 min; lands 10,800 versions and refreshes the pipeline).
- Start app if stopped: `databricks bundle run trip_intel_app`.
- Open the app, the RTM job run page, and the spec (`docs/superpowers/specs/2026-10-05-bcd-realtime-demos-design.md`).
- Rule Studio → disable rules created during rehearsal.

## Arc (55 min)
1. **Real-time vs batch (10 min)** — Ops Console tiers: event→detected p50 ≈ 185 ms, p95 ≈ 250 ms at 15 versions/s
   (BCD UAT, 2026-10-07) vs the analytical tier's freshness. Then the measured comparison: the same job as a 1 s
   micro-batch is ≈ 1 s p50. "Ingest once; each consumer picks its latency. Which of yours need sub-second?"
   Show the RTM job: single-node classic `m5d.2xlarge` — standalone RTM with a custom Lakebase sink needs
   classic compute; RTM inside Lakeflow pipelines can run serverless (Public Preview) with Kafka-protocol sinks only.
   Lakebase is the demo's sink, not the only one: Kafka/MSK natively, or any database/API via ForeachWriter.
   Dispatcher, batch pipeline and app are serverless.
2. **Version-by-version change tracking (10 min)** — "N versions, M significant" (~85% are GDS re-saves with no
   business change). Click a schedule-change trip, walk v1→v12; noise is greyed. "This is your Lambda
   track-changes service as one stateful operator — and no version is skipped. A latest-only MERGE compares
   v1 to v15 and loses everything in between."
3. **Rules engine (15 min)** — Rule Studio live: ask their architect for a real rule; draft → preview on recent versions →
   approve → hits appear within seconds, no redeploy. Rules are JSON Logic (same format as their service).
   Edge-triggered: "no seat" fires once, not on every re-save.
4. **Actions to AWS (10 min)** — Traveler view card → Accept. Show `action_outbox.eventbridge_entry`
   (PutEvents shape, `idempotency_key`). "The dispatcher calls EventBridge through a UC service credential —
   you already use one for Kinesis. Here it runs in SIMULATED mode because this workspace has no AWS bus."
5. **Cost (5 min)** — table below. Per use case, not one winner.
6. **Analytical lane + Genie (5 min)** — show the job first, then the pipeline (per Uday): Catalog Explorer →
   `lb_change_events_history` ("Lakebase's change data feed: every decision the stream made, in Delta, seconds
   behind"), then the pipeline graph (`silver_change_events` → `silver_pnr_segments` / `gold_version_changes` →
   `gold_daily_change_summary`, DQ expectations: 932,618 passed / 0 dropped on 2026-10-08). "We read your feed once."
   Mention: real-time mode inside Lakeflow pipelines is in preview; with a Lakebase sink the real-time job can move
   into the pipeline. Then Genie: "What share of booking versions today were significant vs noise?" (14.59% / 85.41%).

## Cost
Fill Thursday morning from `system.billing.usage` (needs ≥1 h of runtime; billing lags a few hours):

```sql
select u.usage_metadata.job_id, u.sku_name, round(sum(u.usage_quantity),3) dbus,
       round(sum(u.usage_quantity * p.pricing.default),2) list_usd
from system.billing.usage u
join system.billing.list_prices p
  on u.sku_name = p.sku_name and u.usage_start_time >= p.price_start_time
 and (p.price_end_time is null or u.usage_start_time < p.price_end_time)
where u.usage_date >= current_date() - 1
  and u.usage_metadata.job_id in ('598063876765561', '<dispatcher job id>', '<analytics job id>')
group by all
```

Measured in BCD UAT (`system.billing`, list prices in that workspace):

| Lane | Compute | DBU/hr | Databricks $/hr (list) | Notes |
|---|---|---|---|---|
| Real-time (RTM) | 1× m5d.2xlarge classic, on-demand, 24×7 | 1.37 | $0.21 (Jobs Compute $0.15/DBU) | ≈ $150/month 24×7, **plus EC2** on BCD's AWS bill (m5d.2xlarge on-demand, ≈ $0.45/hr AWS list). Same node handles the 100/s peak at 14% avg CPU |
| Batch pipeline | Serverless pipeline | ≈ 0.28 DBU per run | ≈ $0.10 per run ($0.35/DBU serverless) | Demo volume (10,800 versions/run); hourly ≈ $70/month. Scales with data volume |
| Dispatcher | Serverless job | n/a yet | | Billing lags a few hours; check Thursday morning |

## Measured in BCD UAT (single m5d.2xlarge, DBR 18.2, 2026-10-07)
Same job, same node type, side by side; steady-state 4-minute windows; synthetic versions in BCD's JSON shape.

| Run | Rate | Event → detected p50 / p95 / p99 | Event → in Lakebase p50 / p99 | Spark built-in e2e p50 / p95 / p99 |
|---|---|---|---|---|
| Real-Time Mode | 15/s | 185 / 254 / 776 ms (182 / 227 / 274 with no checkpoint in window) | 284 / 1,070 ms | 285 / 343 / 423 ms |
| Micro-batch, 1 s trigger | 15/s | 987 / 1,436 / 1,580 ms | 1,058 / 1,642 ms | n/a (RTM-only metric) |
| Real-Time Mode | 100/s | 73 / 108 / 124 ms | 171 / 248 ms | |

- CPU (system.compute.node_timeline): 15/s avg 5%; 100/s avg 14%, p95 27% — below the docs' ~50% utilization target.
- Spark's built-in `e2eLatencyMs` (record written to source → written downstream) matches our event → in Lakebase.
- Latency is lower at 100/s than 15/s; the docs note Python RTM has extra latency at low throughput.

**Kinesis (real feed, read-only):**
- DBR 18.2 Real-Time Mode **rejects Kinesis polling** (`KINESIS_POLLING_MODE_UNSUPPORTED`); it needs enhanced fan-out (EFO), which registers a consumer on BCD's stream and needs IAM changes on their side.
- DBR 19.x polling works: connected to all **4 shards** and caught up. No records to read (yesterday's test records are past 24 h retention).
- Polling used ~2.5 GetRecords/s per shard, half of each shard's 5/s read limit shared with EPAM's reader. We stopped it after the test. For production: EFO.
- 4 shards + 4 shuffle partitions = 8 task slots = exactly one m5d.2xlarge; no headroom without a larger node or fewer partitions.

## Measured latency on e2-demo (single m5d.2xlarge, DBR 18.x, 2026-10-05)
| Rate | Event → detected p50 | p95 | p99 | Event → in Lakebase p50 |
|---|---|---|---|---|
| 15 versions/s (normal) | 190 ms | 270 ms | — | 294 ms |
| 100 versions/s (holiday peak) | 73 ms | 109 ms | 128 ms | 214 ms |

To rerun the peak test: cancel the RTM run, then
`databricks jobs run-now --json '{"job_id": 598063876765561, "python_params": ["--source","rate","--rows-per-second","100","--lakebase-host","<host>","--secret-scope","rohit-bcd-trip-intel","--checkpoint","/Volumes/users/rohit_bhagwat/trip_intel_checkpoints/rtm_load100b"]}'`

## Questions to ask
- Which consumers truly need sub-second results, and which are fine with seconds or minutes? (Decides one pipeline vs two.)
- How many shards does `uat01-HermesTripsDS` have? (One 8-core node fits up to 4 shards with 4 shuffle partitions.)
- Can we register an enhanced fan-out (EFO) consumer on the stream? The docs recommend EFO for Real-Time Mode; it needs IAM permissions on their side.
- In real GDS data, does a schedule change arrive as a new segment (HX + new HK) or an in-place status flip on the same segment? UAT test records do the latter.
- EPAM's detection pairs prior HK → newer TK/UN/HX; the definition we were given is HX/UN → HK/KK/TK. Which is intended?
- How many JSON-Logic rules exist in the current service, and can their architect share 3 real ones?
- Who owns the EventBridge bus / IAM role for a UAT test?

## UAT (BCD's workspace) — T-60 if demoing there

| Thing (UAT) | Where |
|---|---|
| App | https://databricks-bcd-trip-intel-7474657914890252.aws.databricksapps.com (deployed from private repo `rohit-db/bcd-trip-intel-app`, commit `78a43d8`) |
| RTM job | `[dev rohit_bhagwat] trip_intel_rtm` (job 141818085214296) |
| Batch job / dispatcher | jobs 608337505977714 / 196400368671395 |
| Lakebase | instance `databricks-trip-intel`; registered in UC as `databricks_demo_trip_intel_ops` |
| Change data feed | Lakebase CDF config `public` (Postgres `public` → `bcd_api_uat.databricks_demo`), tables `lb_<table>_history`; all six source tables have `REPLICA IDENTITY FULL` |
| Analytical pipeline | `[dev rohit_bhagwat] trip_intel_batch_pipeline` (`c7d5cc27-d8c0-4309-b35d-e18054d22bb1`), code `src/batch_lane/from_lakebase_cdf/` |
| Tables | `bcd_api_uat.databricks_demo.*`: `lb_*_history`, `silver_change_events`, `silver_pnr_segments`, `gold_version_changes`, `gold_daily_change_summary`, `rt_rule_hits` / `rt_actions` views. (`bronze_pnr_versions` / `silver_pnr_versions` are left from the earlier lane, incl. 12 real Kinesis records.) |
| Genie space | "BCD Trip Changes" — `01f1c30ff44310c6b475edd6a2e6c34c` (definition: `genie/bcd_trip_changes_uat.json`) |
| Warehouse | `databricks-bcd-trip-intel-wh` (`aa7bedb597c55ceb`) |
| RTM live notebook | `.bundle/bcd_trip_intel_rtm/uat/files/src/notebooks/rtm_live_demo` (https://dbc-24f136f2-50df.cloud.databricks.com/?o=7474657914890252#notebook/3754832766807209), cluster `[dev rohit_bhagwat] rtm-live-notebook` (`1008-152556-uxdnbhg0`, auto-terminates after 30 idle min). Real-Time Mode with `display()`, no sink; explicit checkpoints required in this workspace |

- Start stream: `databricks bundle run trip_intel_rtm --no-wait -t uat` (defaults: checkpoint `rtm_demo_20261008`,
  `--seq-offset 2400000`, i.e. trips from T0200000). To start completely fresh later, use a new checkpoint and a higher
  offset: reusing an offset writes nothing, because the idempotent sink skips trips it has already seen.
- Refresh the analytical lane once (`databricks bundle run trip_intel_batch -t uat`; it only refreshes the pipeline
  from the change feed, ~1 min) so the analytical tier shows minutes, not a day.
- Start the dispatcher (job 196400368671395) if showing EventBridge entries.
- App: already deployed and running from Git; open the URL above and log in once before the call. Fallback: a
  local app on UAT data (port 8766).
- Rule Studio: disable rehearsal rules ("Traveler email changed").
- Stop everything after the demo (nothing should run overnight on BCD's bill).

## UAT finale (original plan, superseded by the UAT section above)
- In `bcd-api-uat`: single-node DBR 18 LTS dedicated cluster with the same spark_conf as the bundle; run
  `src/rtm/job.py --source kinesis --kinesis-stream uat01-HermesTripsDS --kinesis-credential kinesis_reader_cred_uat`.
- It needs a Lakebase reachable from their workspace (an e2-demo Lakebase is likely blocked the same way FEVM was).
  Test Wednesday; if blocked, skip.
- Their stream is near-idle: ask EPAM to publish 3–5 test PNR versions (HK → HX + new HK) during the demo.

## Reset
The app role cannot TRUNCATE; use DELETE:
```bash
.venv/bin/python scripts/run_sql.py --host ep-spring-rice-d1tg6e60.database.us-west-2.cloud.databricks.com \
  --user trip_intel_app --password "$PW" \
  -c "DELETE FROM traveler_responses; DELETE FROM rule_hits; DELETE FROM change_events; DELETE FROM trip_latest; DELETE FROM action_outbox;"
```
Then cancel the RTM run, bump the checkpoint suffix (`rtm_v4` → `rtm_v5`) in `bundles/trip_intel/databricks.yml`,
redeploy, and rerun.
