> **Legacy:** talk track for the earlier Lakeflow primer (root bundle). For the real-time demo, see
> [`DEMO_RUNBOOK.md`](DEMO_RUNBOOK.md) and [`WHAT_WE_BUILT.md`](WHAT_WE_BUILT.md).

# LDP Primer + Scheduled Changes Demo — Cheat Sheet

**Audience:** Bojidar Todorov + OMS team (BCD) · **Duration:** ~30 min
**Goal:** Overview of Lakeflow Declarative Pipelines (LDP), grounded in *their* scheduled-change use case.
**Workspace:** fevm-serverless-stable-71zsua · **Catalog/schema:** `serverless_stable_71zsua_catalog.scheduled_changes`

---

## Arc of the session

1. **Why LDP** (5 min) — the pitch, framed against hand-rolled Spark jobs.
2. **dbdemos reference** (5 min) — a canonical DLT pipeline, to anchor the concepts.
3. **Their use case, live** (15 min) — the scheduled-changes pipeline we built from Bojidar's notebook.
4. **Deployment with DABs** (5 min) — how it ships to prod / CI-CD.

---

## Act 1 — Why Lakeflow Declarative Pipelines (5 min)

Talk track:
> "You've written the detection logic in PySpark — it works. The question isn't *can* you do this; it's everything *around* the logic: ingestion, incremental processing, checkpoints, retries, data-quality checks, lineage, and getting it to prod repeatably. LDP is the declarative layer that gives you all of that for free, so your team writes the *transformation* and the platform runs the *pipeline*."

Four things LDP gives you that a raw job doesn't:
- **Declarative tables** — you declare the table + its query; the engine figures out dependencies, order, and incremental vs. full recompute.
- **Streaming + checkpointing managed** — Auto Loader + streaming tables, no manual checkpoint plumbing.
- **Data-quality expectations** — `EXPECT` constraints inline; violations tracked/dropped/failed, visible in the run.
- **Automatic lineage + the pipeline DAG** — every table's dependencies visualized and governed in Unity Catalog.

Reference: [LDP Use Case Battlecard](https://docs.google.com/presentation/d/1mrGr8NvTO_eTNAVYYAEp0jVl3MxzDLXjAsW6y2SpxfI/edit) (`go/pipelines/battle`).

---

## Act 2 — dbdemos reference pipeline (5 min)

> "Before your use case, here's the canonical example so the concepts are concrete."

- Open the installed dbdemos DLT pipeline (see repo README for the exact demo name once verified).
- Show: the **DAG**, a **streaming table** from Auto Loader, an **expectation**, and the **data-quality panel** on a run.
- Keep it quick — it's the reference, not the star.

---

## Act 3 — Their use case, live (15 min) ⭐

This is the payoff: **Bojidar's notebook, running as a governed pipeline.**

### 3a. The pipeline DAG (3 min)
Workspace → Pipelines → **scheduled_changes_etl**. Walk the graph:
`bronze_segments → silver_segments → { gold_scheduled_changes_sql, gold_scheduled_changes_py }`.

> "Same medallion shape you already use. Bronze is raw landed segments, silver cleans + classifies, gold is the detection — the routing decision."

### 3b. Bronze — ingestion (2 min)
Open `01_bronze_segments.sql`.
> "Auto Loader picks up new JSON as it lands in the volume — this is your TD API + Kafka feed dropping segment events. No Lambda to poll a queue; point it at the landing zone and it ingests incrementally."

### 3c. Silver — quality expectations (3 min)
Open `02_silver_segments.sql`. Highlight the `CONSTRAINT ... EXPECT` block.
> "This is the validation you'd otherwise hand-code: well-formed timestamp, known status code, non-null O&D. Bad rows are dropped and counted — you can see the DQ metrics on every run."

Live query — show clean, classified segments:
```sql
SELECT change_side, code, count(*) AS n
FROM serverless_stable_71zsua_catalog.scheduled_changes.silver_segments
GROUP BY change_side, code ORDER BY change_side, code;
```

### 3d. Gold — the detection, in SQL and PySpark (5 min)
Open `03_gold_scheduled_changes_sql.sql` and `04_gold_scheduled_changes_py.py` side by side.
> "Two implementations of *your* logic — the SQL version and your PySpark notebook, essentially verbatim, as a declarative table. Same self-join on O/D/date within 60 minutes, same unique-1:1 filter. They produce identical rows, because SQL and PySpark run on the same engine. Use whichever your team prefers."

Live query — the detected scheduled changes:
```sql
SELECT * FROM serverless_stable_71zsua_catalog.scheduled_changes.gold_scheduled_changes_sql
ORDER BY departure_date, origin LIMIT 20;
```

Prove SQL == PySpark (should return 0 rows of difference):
```sql
SELECT 'sql_only' AS side, * FROM (
  SELECT record_locator, origin, destination, departure_date FROM serverless_stable_71zsua_catalog.scheduled_changes.gold_scheduled_changes_sql
  EXCEPT
  SELECT record_locator, origin, destination, departure_date FROM serverless_stable_71zsua_catalog.scheduled_changes.gold_scheduled_changes_py
);
```

### 3e. The bug (2 min — credibility moment)
> "One thing while adapting your notebook — this line: `F.col('code') == 'HX' or F.col('code') == 'UN'`. Python's `or` short-circuits on Column objects and silently keeps only `HX`, dropping every `UN` (and only `HK` survives on the new side). The fix is `.isin('HX','UN')`. Small thing, but in production it would quietly miss a chunk of scheduled changes."

---

## Act 4 — Deployment with Databricks Asset Bundles (5 min)

Open `databricks.yml` + `resources/*.yml`.
> "Everything you just saw — the pipeline, the job, the schedule, the target workspace, the variables — is defined in YAML in this repo. One command deploys it; another runs it. Dev and prod are just different targets. This is how it goes into CI/CD."

```bash
databricks bundle validate   # check
databricks bundle deploy      # push pipeline + job to the workspace
databricks bundle run scheduled_changes_job   # generate data + refresh
```

> "No click-ops. The whole pipeline is version-controlled, reviewable, and reproducible across environments."

---

## Follow-ups to leave with them
- Academy course: **"Build Data Pipelines with Lakeflow Declarative Pipelines"** (free, self-paced).
- This repo (README has the deploy steps + how to reuse it).
- Offer: pair with their team to port the *real* feed into bronze and wire the routing sink.

## Open questions to revisit (from Aug 10)
- Production **watermarking / lookback** for streaming (when OLD and NEW arrive in different micro-batches).
- Downstream routing: back into **AWS Lambda** vs. a Databricks-native sink.
- A future **AI component** to complement the rule-based detection (ambiguous-tail classification).
