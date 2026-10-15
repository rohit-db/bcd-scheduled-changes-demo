"""BCD Trip Intelligence app: Ops Console, Analytics, Data quality, Rule Studio, Traveler view over Lakebase."""
import asyncio
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import db
from app.feed import FeedBroadcaster, TTLValue
from app.freshness import BackgroundTTL
from app.insights import ANALYTICS_SQL, ANALYTICS_WINDOW_ROWS, TICKER_SQL, quality_checks, quality_sql, shape_genie
from app.rule_drafting import SYSTEM_PROMPT, parse_draft
from trip_intel.jsonlogic import apply, truthy, validate
from trip_intel.rules import context

STATIC = Path(__file__).parent / "static"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5-5")
EVENT_COLS = ("id, trip_id, from_version, to_version, tags, changes, schedule_changes, event_ts, detected_ts, written_ts, "
              "(select coalesce(json_agg(json_build_object('rule_id', h.rule_id, 'name', h.rule_name)), '[]') "
              " from rule_hits h where h.trip_id = e.trip_id and h.version = e.to_version) as rule_hits")
SIGNIFICANT = "NOT (tags <@ ARRAY['NO_SIGNIFICANT_CHANGE','STALE_VERSION'])"


def _fetch_feed(since_id: int) -> list[dict]:
    return db.query(f"SELECT {EVENT_COLS} FROM change_events e WHERE id > %s AND {SIGNIFICANT} ORDER BY id LIMIT 200",
                    (since_id,))


async def _run_feed(broadcaster: FeedBroadcaster):
    while True:
        try:
            await broadcaster.tick()
        except Exception as exc:  # keep the shared poller alive through transient DB errors
            print(f"FEED_ERROR {exc!r}", flush=True)
        await asyncio.sleep(0.25)


@asynccontextmanager
async def lifespan(app_):
    db.pool.open()
    start = (await asyncio.to_thread(db.query, "SELECT coalesce(max(id), 0) AS m FROM change_events"))[0]["m"]
    app_.state.feed = FeedBroadcaster(_fetch_feed, start=start)
    task = asyncio.create_task(_run_feed(app_.state.feed))
    _quality.get()  # start the warehouse checks now so the Data quality view is ready when opened
    yield
    task.cancel()
    db.pool.close()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/stream")
async def stream(request: Request):
    feed: FeedBroadcaster = request.app.state.feed

    async def events():
        backlog = await asyncio.to_thread(
            db.query, f"SELECT {EVENT_COLS} FROM change_events e WHERE {SIGNIFICANT} ORDER BY id DESC LIMIT 20")
        for row in reversed(backlog):
            yield f"data: {json.dumps(row, default=str)}\n\n"
        q = feed.subscribe()
        try:
            while True:
                try:
                    row = await asyncio.wait_for(q.get(), timeout=5)
                    yield f"data: {json.dumps(row, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            feed.unsubscribe(q)

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


def _compute_kpis():
    return db.query(f"""
        SELECT count(*) AS versions_5m,
               count(*) FILTER (WHERE {SIGNIFICANT}) AS significant_5m,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000) AS p95_ms,
               extract(epoch FROM now() - max(written_ts)) AS rtm_freshness_s
        FROM change_events WHERE written_ts > now() - interval '5 minutes'""")[0]


_kpis = TTLValue(_compute_kpis, ttl_seconds=1.5)


@app.get("/api/kpis")
def kpis():
    row = dict(_kpis.get())
    last = _batch_last_written.get()
    row["batch_freshness_s"] = (datetime.now(timezone.utc) - last).total_seconds() if last else None
    return row


def _fetch_batch_last_written():
    w = WorkspaceClient()
    res = w.statement_execution.execute_statement(
        warehouse_id=os.environ["WAREHOUSE_ID"], wait_timeout="50s",
        statement=f"SELECT max(ingested_at) FROM {os.environ['ANALYTICS_SCHEMA']}.gold_version_changes")
    value = res.result.data_array[0][0]
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


_batch_last_written = BackgroundTTL(_fetch_batch_last_written, ttl_seconds=60)


@app.get("/api/trips/{trip_id}")
def trip(trip_id: str):
    latest = db.query("SELECT snapshot FROM trip_latest WHERE trip_id = %s", (trip_id,))
    if not latest:
        raise HTTPException(404, "trip not found")
    versions = db.query(f"SELECT {EVENT_COLS} FROM change_events e WHERE trip_id = %s ORDER BY to_version", (trip_id,))
    return {"latest": latest[0]["snapshot"], "versions": versions}


class DraftIn(BaseModel):
    text: str


class LogicIn(BaseModel):
    json_logic: dict


class RuleIn(BaseModel):
    name: str
    nl_text: str = ""
    json_logic: dict
    action: dict = {"type": "RULE_MATCHED", "target": "ops-queue"}


class RespondIn(BaseModel):
    response: str


@app.get("/api/rules")
def list_rules():
    return db.query("""SELECT r.*, (SELECT count(*) FROM rule_hits h WHERE h.rule_id = r.rule_id) AS hits
                       FROM rules r ORDER BY created_ts DESC""")


@app.post("/api/rules/draft")
def draft_rule(body: DraftIn):
    try:
        resp = WorkspaceClient().serving_endpoints.query(
            name=LLM_ENDPOINT, max_tokens=800,
            messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
                      ChatMessage(role=ChatMessageRole.USER, content=body.text)])
    except Exception as exc:  # throttling, timeout, permissions: show the reason, not a bare 500
        raise HTTPException(502, f"model endpoint {LLM_ENDPOINT} failed: {exc}")
    try:
        return parse_draft(resp.choices[0].message.content)
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@app.post("/api/rules/preview")
def preview_rule(body: LogicIn):
    try:
        validate(body.json_logic)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    rows = db.query("""SELECT trip_id, to_version, tags, changes, schedule_changes, snapshot
                       FROM change_events WHERE NOT ('STALE_VERSION' = ANY(tags)) ORDER BY id DESC LIMIT 1000""")
    matched = []
    for r in rows:
        change = {"tags": r["tags"], "changes": r["changes"], "schedule_changes": r["schedule_changes"]}
        try:
            if truthy(apply(body.json_logic, context(r["snapshot"], change))):
                matched.append({"trip_id": r["trip_id"], "to_version": r["to_version"], "tags": r["tags"]})
        except Exception as exc:
            raise HTTPException(422, f"rule failed on {r['trip_id']} v{r['to_version']}: {exc}")
    return {"evaluated": len(rows), "matched": len(matched), "samples": matched[:10]}


@app.post("/api/rules")
def create_rule(body: RuleIn):
    try:
        validate(body.json_logic)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    rule_id = f"r-{uuid.uuid4().hex[:8]}"
    db.execute("""INSERT INTO rules (rule_id, name, nl_text, json_logic, action, status, author, activated_ts)
                  VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, 'active', 'rule-studio', now())""",
               (rule_id, body.name, body.nl_text, json.dumps(body.json_logic), json.dumps(body.action)))
    return db.query("SELECT * FROM rules WHERE rule_id = %s", (rule_id,))[0]


@app.post("/api/rules/{rule_id}/disable")
def disable_rule(rule_id: str):
    db.execute("UPDATE rules SET status = 'disabled' WHERE rule_id = %s", (rule_id,))
    return {"rule_id": rule_id, "status": "disabled"}


@app.get("/api/traveler/notifications")
def notifications(limit: int = 20):
    return db.query("""SELECT o.id, o.trip_id, o.version, o.payload, o.status, o.created_ts, r.response
                       FROM action_outbox o LEFT JOIN traveler_responses r ON r.outbox_id = o.id
                       WHERE o.type = 'NOTIFY_TRAVELER_SCHEDULE_CHANGE' ORDER BY o.created_ts DESC LIMIT %s""",
                    (min(limit, 100),))


@app.post("/api/traveler/notifications/{outbox_id}/respond")
def respond(outbox_id: str, body: RespondIn):
    if body.response not in ("ACCEPT", "REBOOK"):
        raise HTTPException(422, "response must be ACCEPT or REBOOK")
    db.execute("""INSERT INTO traveler_responses (outbox_id, response) VALUES (%s, %s)
                  ON CONFLICT (outbox_id) DO UPDATE SET response = EXCLUDED.response, response_ts = now()""",
               (outbox_id, body.response))
    return {"outbox_id": outbox_id, "response": body.response}


# ---- analytics (Lakebase, live) ----------------------------------------------------------------------------

def _compute_analytics():
    params = {"n": ANALYTICS_WINDOW_ROWS}
    out = {name: db.query(sql, params) for name, sql in ANALYTICS_SQL.items()}
    out["summary"] = out["summary"][0] if out["summary"] else {}
    return out


_analytics = TTLValue(_compute_analytics, ttl_seconds=5)


@app.get("/api/analytics")
def analytics():
    return _analytics.get()


_ticker = TTLValue(lambda: db.query(TICKER_SQL), ttl_seconds=1.5)


@app.get("/api/ticker")
def ticker():
    return _ticker.get()


# ---- data quality & governance (warehouse over the Delta tables) -------------------------------------------

def _sql_rows(w: WorkspaceClient, statement: str) -> list[dict]:
    res = w.statement_execution.execute_statement(warehouse_id=os.environ["WAREHOUSE_ID"], statement=statement,
                                                  wait_timeout="50s")
    if res.status and res.status.error:
        raise RuntimeError(res.status.error.message)
    cols = [c.name for c in res.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (res.result.data_array or [])] if res.result else []


def _compute_quality():
    w = WorkspaceClient()
    statements = quality_sql(os.environ["ANALYTICS_SCHEMA"])
    with ThreadPoolExecutor(max_workers=len(statements)) as pool:
        results = dict(zip(statements, pool.map(lambda s: _sql_rows(w, s), statements.values())))
    lake = db.query("SELECT count(*) AS n, max(written_ts) AS latest FROM change_events")[0]
    dq = results["dq"][0]
    layers = {r["layer"]: r for r in results["layers"]}
    counts = {"lakebase": lake["n"], "cdf": dq["total"],
              "silver": layers["silver_change_events"]["n"], "gold": layers["gold_version_changes"]["n"]}
    lineage = [
        {"name": "Kinesis → Real-Time Mode stream", "kind": "stream", "rows": None, "latest": None},
        {"name": "Lakebase: change_events", "kind": "operational", "rows": lake["n"], "latest": lake["latest"].isoformat() if lake["latest"] else None},
        {"name": "lb_change_events_history", "kind": "change data feed", "rows": dq["total"], "latest": dq["latest"]},
        *({"name": name, "kind": "silver" if name.startswith("silver") else "gold",
           "rows": layers[name]["n"], "latest": layers[name]["latest"]} for name in layers),
    ]
    return {"checks": quality_checks(dq, counts), "lineage": lineage, "completeness": results["completeness"][0],
            "schema": os.environ["ANALYTICS_SCHEMA"], "computed_at": datetime.now(timezone.utc).isoformat()}


_quality = BackgroundTTL(_compute_quality, ttl_seconds=60)


@app.get("/api/quality")
def quality():
    return _quality.get() or {"loading": True}


# ---- Ask Genie (natural-language questions for data stewards) ----------------------------------------------

class AskIn(BaseModel):
    question: str


@app.post("/api/genie")
def ask_genie(body: AskIn):
    space_id = os.environ.get("GENIE_SPACE_ID")
    if not space_id:
        raise HTTPException(503, "GENIE_SPACE_ID is not configured")
    w = WorkspaceClient()
    try:
        msg = w.genie.start_conversation_and_wait(space_id, body.question, timeout=timedelta(seconds=90))
        message = msg.as_dict()
        result = None
        for att in msg.attachments or []:
            if att.query:
                result = w.genie.get_message_attachment_query_result(
                    space_id, msg.conversation_id, msg.message_id, att.attachment_id).as_dict()
                break
    except Exception as exc:  # timeouts, permissions, throttling: show the reason
        raise HTTPException(502, f"Genie failed: {exc}")
    return shape_genie(message, result)
