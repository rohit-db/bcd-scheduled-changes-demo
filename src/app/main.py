"""BCD Trip Intelligence app: Ops Console, Rule Studio, Traveler view over Lakebase."""
import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import db
from app.freshness import BackgroundTTL
from app.rule_drafting import SYSTEM_PROMPT, parse_draft
from trip_intel.jsonlogic import apply, truthy, validate
from trip_intel.rules import context

STATIC = Path(__file__).parent / "static"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5-5")
EVENT_COLS = ("id, trip_id, from_version, to_version, tags, changes, schedule_changes, event_ts, detected_ts, written_ts, "
              "(select coalesce(json_agg(json_build_object('rule_id', h.rule_id, 'name', h.rule_name)), '[]') "
              " from rule_hits h where h.trip_id = e.trip_id and h.version = e.to_version) as rule_hits")
SIGNIFICANT = "NOT (tags <@ ARRAY['NO_SIGNIFICANT_CHANGE','STALE_VERSION'])"


@asynccontextmanager
async def lifespan(_):
    db.pool.open()
    yield
    db.pool.close()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/stream")
async def stream():
    async def events():
        last = (db.query("SELECT coalesce(max(id), 0) AS m FROM change_events")[0]["m"]) - 50
        while True:
            rows = await asyncio.to_thread(
                db.query, f"SELECT {EVENT_COLS} FROM change_events e WHERE id > %s AND {SIGNIFICANT} ORDER BY id LIMIT 200",
                (last,))
            for row in rows:
                last = row["id"]
                yield f"data: {json.dumps(row, default=str)}\n\n"
            if not rows:
                max_id = await asyncio.to_thread(db.query, "SELECT coalesce(max(id), 0) AS m FROM change_events")
                last = max(last, max_id[0]["m"])
                yield ": keepalive\n\n"
            await asyncio.sleep(0.2)

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.get("/api/kpis")
def kpis():
    row = db.query(f"""
        SELECT count(*) AS versions_5m,
               count(*) FILTER (WHERE {SIGNIFICANT}) AS significant_5m,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000) AS p95_ms,
               extract(epoch FROM now() - max(written_ts)) AS rtm_freshness_s
        FROM change_events WHERE written_ts > now() - interval '5 minutes'""")[0]
    last = _batch_last_written.get()
    row["batch_freshness_s"] = (datetime.now(timezone.utc) - last).total_seconds() if last else None
    return row


def _fetch_batch_last_written():
    w = WorkspaceClient()
    res = w.statement_execution.execute_statement(
        warehouse_id=os.environ["WAREHOUSE_ID"], wait_timeout="50s",
        statement=f"SELECT max(written_ts) FROM {os.environ['ANALYTICS_SCHEMA']}.change_events_hourly")
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
    w = WorkspaceClient()
    resp = w.serving_endpoints.query(
        name=LLM_ENDPOINT, max_tokens=800,
        messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
                  ChatMessage(role=ChatMessageRole.USER, content=body.text)])
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


@app.get("/api/rules/{rule_id}/hits")
def rule_hits(rule_id: str):
    return db.query("SELECT trip_id, version, matched_ts FROM rule_hits WHERE rule_id = %s ORDER BY id DESC LIMIT 20",
                    (rule_id,))


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
