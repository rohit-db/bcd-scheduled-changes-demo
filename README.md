# BCD — Scheduled Changes Detection (Lakeflow Declarative Pipelines + DAB)

A reference **Lakeflow Declarative Pipeline**, deployed with a **Databricks Asset
Bundle**, that ingests reservation/PNR segments and detects **airline-initiated
scheduled changes** — then routes them to a gold decision table.

Built for **Bojidar Todorov's team @ BCD Travel** as the worked example behind the
Lakeflow Declarative Pipelines (LDP) primer. It takes the customer's own PySpark
detection notebook and shows it running as a governed, declarative pipeline.

---

## The detection logic

A **scheduled change** is airline-initiated: a segment is cancelled/unable
(`HX`, `UN`) and a confirmed replacement (`HK`, `KK`, `TK`) appears for the same
trip. We detect it by:

1. Pairing **OLD** (`HX`/`UN`) with **NEW** (`HK`/`KK`/`TK`) segments on
   **origin + destination + departure date**, within **≤ 60 minutes** of each other.
2. Keeping only **unique 1:1 matches** — if one OLD maps to several NEW (or vice
   versa), the pairing is ambiguous and dropped.

This is exactly the customer's notebook logic, re-expressed declaratively.

```
   Feed (TD API + Kafka)  ->  landing volume (JSON)
                                     │  Auto Loader
                                     ▼
   BRONZE  bronze_segments          raw segment events (streaming table)
                                     │
                                     ▼
   SILVER  silver_segments          typed + normalized timestamp + DQ expectations
                                     │        (classifies each row OLD / NEW)
                       ┌─────────────┴─────────────┐
                       ▼                           ▼
   GOLD  gold_scheduled_changes_sql     gold_scheduled_changes_py
         (SQL implementation)           (PySpark implementation — the customer's
                                         notebook, bug-fixed, as a @dlt table)
         ── identical output: the routing decision table ──
```

## Why both SQL and Python?

The pipeline computes the **same detection** two ways, in the same pipeline:
- `03_gold_scheduled_changes_sql.sql` — SQL materialized view.
- `04_gold_scheduled_changes_py.py` — the customer's PySpark, as a `@dlt.table`.

They produce identical rows — the point being that **SQL and PySpark run on the
same engine**; teams can use whichever they prefer.

> **Note on the customer notebook bug:** the original filtered with Python `or`
> on Column objects — `F.col("code") == "HX" or F.col("code") == "UN"` — which
> silently keeps only `HX` (and only `HK` on the new side), dropping `UN`/`KK`/`TK`.
> The Python transform here fixes it with `.isin(...)`. Worth raising with the team.

## Layout

```
databricks.yml                         # bundle + dev/prod targets + variables
pyproject.toml                         # project + dev deps (databricks-dlt, connect)
resources/
  scheduled_changes_etl.pipeline.yml   # the LDP pipeline (serverless)
  scheduled_changes.job.yml            # job: generate data -> refresh pipeline
src/
  generate_data/generate_segments.ipynb  # synthetic segment generator
  scheduled_changes_etl/transformations/
    01_bronze_segments.sql
    02_silver_segments.sql             # DQ expectations live here
    03_gold_scheduled_changes_sql.sql
    04_gold_scheduled_changes_py.py
docs/
  DEMO_CHEATSHEET.md                   # the 30-min talk track
```

## Deploy & run

```bash
# Validate
databricks bundle validate -p fevm-stable-71zsua

# Deploy (dev target is default)
databricks bundle deploy -p fevm-stable-71zsua

# Generate data + refresh the pipeline
databricks bundle run scheduled_changes_job -p fevm-stable-71zsua
```

Target workspace + catalog are set in `databricks.yml` (`dev` target uses
`serverless_stable_71zsua_catalog.scheduled_changes`). Change the `variables`
block to point at another workspace/catalog.

## Reusing for another account (e.g. Subaru)

The DAB skeleton, medallion structure, and demo flow are domain-agnostic. To
re-skin: swap the `src/scheduled_changes_etl/transformations/` domain logic and
the `generate_segments` notebook, update the `variables` in `databricks.yml`, and
adjust the talk track. Everything else (serverless pipeline, DQ expectations,
job orchestration, bundle deploy) carries over unchanged.
