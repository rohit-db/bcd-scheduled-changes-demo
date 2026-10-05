# BCD Real-Time Demos — Runbook (Thu 2026-10-08, 2–3pm ET)

Everything runs on **e2-demo-field-eng** (bundle `bundles/trip_intel`, target `e2demo`).
FEVM serverless workspaces reject classic clusters (Real-Time Mode is classic-only), and FEVM's IP access
list blocks e2-demo from reaching FEVM Lakebase — so stream, Lakebase, jobs and app live together on e2-demo.

| Thing | Where |
|---|---|
| App | https://rohit-bcd-trip-intel-1444828305810485.aws.databricksapps.com |
| RTM job | `[dev rohit_bhagwat] trip_intel_rtm` (job 598063876765561) |
| Lakebase | instance `rohit-bcd-trip-intel` · `ep-spring-rice-d1tg6e60.database.us-west-2.cloud.databricks.com` |
| Batch pipeline | `[dev rohit_bhagwat] trip_intel_batch_pipeline` → `users.rohit_bhagwat.bronze_pnr_versions`, `silver_pnr_versions`, `silver_pnr_segments`, `gold_version_changes`, `gold_daily_change_summary` |
| Batch job | `[dev rohit_bhagwat] trip_intel_batch` (land versions → refresh pipeline), hourly |
| Genie space | "BCD Trip Changes" — `01f1c1041883164e9f82e7116000e7bb` |
| Secret | scope `rohit-bcd-trip-intel`, key `lakebase-password` (role `trip_intel_app`) |

All commands below run from `bundles/trip_intel` with `--profile e2-demo-field-eng -t e2demo`.

## T-60 min
- Start stream: `databricks bundle run trip_intel_rtm --no-wait` (cluster ~5 min; first rows ~6 min).
- Start dispatcher: `databricks bundle run trip_intel_dispatcher --no-wait`.
- Run the batch lane once: `databricks bundle run trip_intel_batch` (~2 min; lands 10,800 versions and refreshes the pipeline).
- Start app if stopped: `databricks bundle run trip_intel_app`.
- Open the app, the RTM job run page, and the spec (`docs/superpowers/specs/2026-10-05-bcd-realtime-demos-design.md`).
- Rule Studio → disable rules created during rehearsal.

## Arc (55 min)
1. **Real-time vs batch (10 min)** — Ops Console KPIs: event→detected p50 ≈ 190 ms, p95 ≈ 270 ms (measured
   2026-10-05 at 15 versions/s) vs the hourly lane's freshness. "Ingest once; each consumer picks its latency."
   Show the RTM job: single-node classic `m5d.2xlarge` — standalone RTM with a custom Lakebase sink needs
   classic compute; RTM inside Lakeflow pipelines can run serverless (Public Preview) when the output is Kafka.
   Dispatcher, batch pipeline and app are serverless.
2. **Version-by-version change tracking (10 min)** — "N versions, M significant" (~85% are GDS re-saves with no
   business change). Click a schedule-change trip, walk v1→v12; noise is greyed. "This is your Lambda
   track-changes service as one stateful operator — and no version is skipped. A latest-only MERGE compares
   v1 to v15 and loses everything in between."
3. **Rules engine (15 min)** — Rule Studio live: ask Denis for a real rule; draft → preview on recent versions →
   approve → hits appear within seconds, no redeploy. Rules are JSON Logic (same format as their service).
   Edge-triggered: "no seat" fires once, not on every re-save.
4. **Actions to AWS (10 min)** — Traveler view card → Accept. Show `action_outbox.eventbridge_entry`
   (PutEvents shape, `idempotency_key`). "The dispatcher calls EventBridge through a UC service credential —
   you already use one for Kinesis. Here it runs in SIMULATED mode because this workspace has no AWS bus."
5. **Cost (5 min)** — table below. Per use case, not one winner.
6. **Batch pipeline + Genie (5 min)** — open the pipeline graph (bronze → silver → gold, DQ expectation metrics),
   then Genie: "What share of booking versions today were significant vs noise?" (answered 14.6% / 85.4% on 2026-10-05).

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

| Lane | Compute | DBU/hr | $/hr list | Notes |
|---|---|---|---|---|
| Real-time (RTM) | 1× m5d.2xlarge classic, 24×7 | | + EC2 | 15/s measured; 100/s peak not yet tested |
| Batch pipeline hourly | Serverless pipeline + job | | | ~2 min/run |
| Dispatcher | Serverless jobs | | | |

## Questions to ask
- EPAM's detection pairs prior HK → newer TK/UN/HX; the definition we were given is HX/UN → HK/KK/TK. Which is intended?
- How many JSON-Logic rules exist in the current service, and can Denis share 3 real ones?
- Who owns the EventBridge bus / IAM role for a UAT test?

## UAT finale (stretch)
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
