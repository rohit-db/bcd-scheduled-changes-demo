"""Analytics, data-quality and Genie helpers for the app's Analytics and Data quality views."""

ANALYTICS_WINDOW_ROWS = 30000  # ≈ 33 minutes at 15 versions/s; selected by primary key, so it stays fast

_WINDOW = ("WITH w AS (SELECT * FROM change_events WHERE id > (SELECT max(id) FROM change_events) - %(n)s "
           "AND written_ts > (SELECT max(written_ts) FROM change_events) - interval '30 minutes')")
_SIG = "NOT (tags <@ ARRAY['NO_SIGNIFICANT_CHANGE','STALE_VERSION'])"
_LAT = "extract(epoch FROM detected_ts - event_ts) * 1000"

ANALYTICS_SQL = {
    "summary": f"""{_WINDOW}
        SELECT count(*) AS versions, count(*) FILTER (WHERE {_SIG}) AS significant,
               count(*) FILTER (WHERE 'SCHEDULE_CHANGE' = ANY(tags)) AS schedule_changes,
               count(DISTINCT trip_id) AS trips,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY {_LAT}) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY {_LAT}) AS p95_ms,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY {_LAT}) AS p99_ms,
               extract(epoch FROM max(written_ts) - min(written_ts)) / 60 AS window_minutes
        FROM w""",
    "per_minute": f"""{_WINDOW}
        SELECT date_trunc('minute', written_ts) AS minute, count(*) AS versions,
               count(*) FILTER (WHERE {_SIG}) AS significant,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY {_LAT}) AS p50_ms
        FROM w GROUP BY 1 ORDER BY 1""",
    "change_mix": f"""{_WINDOW}
        SELECT t AS tag, count(*) AS n FROM w, unnest(tags) AS t
        WHERE t NOT IN ('NO_SIGNIFICANT_CHANGE', 'STALE_VERSION', 'NEW_TRIP') GROUP BY 1 ORDER BY 2 DESC""",
    "tag_minutes": f"""{_WINDOW}
        SELECT date_trunc('minute', written_ts) AS minute, t AS tag, count(*) AS n FROM w, unnest(tags) AS t
        WHERE t NOT IN ('NO_SIGNIFICANT_CHANGE', 'STALE_VERSION', 'NEW_TRIP') GROUP BY 1, 2 ORDER BY 1""",
    "schedule_routes": f"""{_WINDOW}
        SELECT sc->'old'->>'carrier' AS carrier,
               (sc->'old'->>'origin') || ' → ' || (sc->'old'->>'destination') AS route,
               count(*) AS n,
               round(avg(extract(epoch FROM (sc->'new'->>'start')::timestamptz - (sc->'old'->>'start')::timestamptz) / 60))
                 AS avg_shift_min
        FROM w, jsonb_array_elements(schedule_changes) AS sc
        GROUP BY 1, 2 ORDER BY 3 DESC, 2 LIMIT 8""",
    "latency_hist": f"""{_WINDOW}
        SELECT least(floor({_LAT} / 50)::int, 20) AS bucket, count(*) AS n FROM w GROUP BY 1 ORDER BY 1""",
    "rules": f"""{_WINDOW}
        SELECT h.rule_name AS name, count(*) AS hits FROM rule_hits h
        WHERE h.matched_ts >= (SELECT min(written_ts) FROM w) GROUP BY 1 ORDER BY 2 DESC""",
    "actions": f"""{_WINDOW}
        SELECT type, status, count(*) AS n FROM action_outbox
        WHERE created_ts >= (SELECT min(written_ts) FROM w) GROUP BY 1, 2 ORDER BY 3 DESC""",
}


TICKER_SQL = f"""
    SELECT to_timestamp(floor(extract(epoch FROM written_ts) / 5) * 5) AS t,
           count(*) AS versions, count(*) FILTER (WHERE {_SIG}) AS significant
    FROM change_events WHERE written_ts > now() - interval '3 minutes' GROUP BY 1 ORDER BY 1"""


def quality_sql(schema: str) -> dict:
    """Warehouse statements for the Data quality view, against the analytical tables in `schema`."""
    return {
        "dq": f"""SELECT count(*) AS total,
                    count_if(trip_id IS NULL) AS missing_trip_id,
                    count_if(to_version IS NULL OR to_version <= 0) AS bad_version,
                    count_if(coalesce(json_array_length(get_json_object(snapshot, '$.segments')), 0) = 0) AS no_segments,
                    count(*) - count(DISTINCT trip_id, to_version) AS duplicates,
                    max(_timestamp) AS latest
                  FROM {schema}.lb_change_events_history WHERE _pg_change_type = 'insert'""",
        "layers": f"""SELECT 'silver_change_events' AS layer, count(*) AS n, max(ingested_at) AS latest
                        FROM {schema}.silver_change_events
                      UNION ALL SELECT 'silver_pnr_segments', count(*), max(ingested_at) FROM {schema}.silver_pnr_segments
                      UNION ALL SELECT 'gold_version_changes', count(*), max(ingested_at) FROM {schema}.gold_version_changes
                      UNION ALL SELECT 'gold_daily_change_summary', count(*), NULL FROM {schema}.gold_daily_change_summary""",
        "completeness": f"""SELECT round(100 * avg(CASE WHEN carrier IS NOT NULL THEN 1 ELSE 0 END), 2) AS carrier_pct,
                              round(100 * avg(CASE WHEN origin IS NOT NULL AND destination IS NOT NULL THEN 1 ELSE 0 END), 2)
                                AS route_pct
                            FROM {schema}.gold_version_changes""",
    }


def _status_for_gap(have: int, want: int, tolerance: float) -> str:
    if have == want:
        return "pass"
    if have < want and want and (want - have) / want <= tolerance:
        return "warn"
    return "fail"


def quality_checks(dq: dict, counts: dict) -> list[dict]:
    """Turn raw data-quality counts into steward-facing checks (pass / warn / fail) with plain-language detail."""
    total = int(dq.get("total") or 0)
    missing, bad, empty, dupes = (int(dq.get(k) or 0) for k in ("missing_trip_id", "bad_version", "no_segments", "duplicates"))
    lakebase, cdf, silver, gold = (int(counts.get(k) or 0) for k in ("lakebase", "cdf", "silver", "gold"))

    checks = [
        {"name": "Trip id present", "rule": "valid_trip_id · drop row",
         "status": "pass" if missing == 0 else "fail",
         "detail": "every version has a trip id" if missing == 0 else f"{missing:,} versions without a trip id were dropped"},
        {"name": "Valid version number", "rule": "valid_version · drop row",
         "status": "pass" if bad == 0 else "fail",
         "detail": "every version number is positive" if bad == 0 else f"{bad:,} versions with an invalid number were dropped"},
        {"name": "Every version has segments", "rule": "has_segments · warn",
         "status": "pass" if empty == 0 else "warn",
         "detail": "no empty bookings" if empty == 0 else f"{empty:,} versions with no segments flagged for review"},
        {"name": "No duplicate versions", "rule": "unique (trip_id, version)",
         "status": "pass" if dupes == 0 else "fail",
         "detail": "each version recorded exactly once" if dupes == 0 else f"{dupes:,} duplicate versions"},
    ]

    feed_status = _status_for_gap(cdf, lakebase, tolerance=0.02)
    checks.append({
        "name": "Lakebase → Delta (change data feed)", "rule": "row counts reconcile",
        "status": feed_status,
        "detail": (f"{cdf:,} of {lakebase:,} decisions in Delta" if feed_status != "pass"
                   else f"all {lakebase:,} decisions copied to Delta"),
    })

    expected_silver = total - missing - bad
    if silver == expected_silver and gold == silver:
        pipe_status, pipe_detail = "pass", f"{gold:,} versions in gold, matching silver"
    elif silver <= expected_silver and gold <= silver:
        waiting = expected_silver - gold
        pipe_status, pipe_detail = "warn", f"{waiting:,} versions waiting for the next pipeline refresh"
    else:
        pipe_status, pipe_detail = "fail", f"silver {silver:,} / gold {gold:,} don't reconcile with {expected_silver:,} expected"
    checks.append({"name": "Delta → silver → gold", "rule": "row counts reconcile", "status": pipe_status,
                   "detail": pipe_detail})
    return checks


def shape_genie(message: dict, result: dict | None, max_rows: int = 50) -> dict:
    """Reduce a Genie message (+ its query result) to what the UI shows: answer text, SQL, a small table."""
    text, sql, description = [], None, None
    for att in message.get("attachments") or []:
        if att.get("text", {}).get("content"):
            text.append(att["text"]["content"])
        if att.get("query", {}).get("query"):
            sql = att["query"]["query"]
            description = att["query"].get("description")
    columns, rows = [], []
    if result:
        resp = result.get("statement_response") or {}
        columns = [c["name"] for c in (resp.get("manifest", {}).get("schema", {}).get("columns") or [])]
        rows = (resp.get("result", {}).get("data_array") or [])[:max_rows]
    return {"text": "\n\n".join(text), "sql": sql, "description": description, "columns": columns, "rows": rows}
