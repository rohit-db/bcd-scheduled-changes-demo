# BCD Real-Time Trip Intelligence Demos — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working demo where BCD-schema PNR versions stream through Spark Real-Time Mode, get diffed per trip, detected for schedule changes, evaluated against business-authored JSON-Logic rules, and surfaced in a Databricks App (Ops Console, Rule Studio, Traveler view) — with an hourly serverless analytics lane for contrast.

**Architecture:** All business logic lives in a pure-Python package `trip_intel` (no Spark imports, fully unit-tested locally). A thin `rtm` package wires it into a Structured Streaming RTM job on a single-node classic cluster: rate source + deterministic generator UDF → `transformWithState` per `trip_id` → `ForeachWriter` into Lakebase Postgres. A FastAPI Databricks App reads/writes Lakebase and reuses `trip_intel` for rule preview so preview and production cannot disagree.

**Tech Stack:** Python 3.12, PySpark 4.1 (DBR `18.x-scala2.13`), Databricks Asset Bundles, Lakebase (provisioned, Postgres native login), psycopg 3, FastAPI + vanilla JS (SSE), Foundation Model API (`databricks-claude-sonnet-5-5`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-05-bcd-realtime-demos-design.md`

## Global Constraints

- Build workspace: profile `fevm-stable-71zsua`, catalog `serverless_stable_71zsua_catalog`, new schema `trip_intel`.
- BCD UAT (profile `bcd-api-uat`) is **read-only** for this work. Never write to their tables or Kinesis streams.
- Payload must match BCD's schema exactly: `identification{id, recordLocator, version}`, `segments[{type, segmentId, segmentNumber, status{code}, marketingFlight{airlineCode, flightNumber}, classOfService{code}, departureAirport{airport{code}}, arrivalAirport{airport{code}}, startDateTime, endDateTime}]`. Extensions (`segments[].seat`, `travelers`, `payment`, `hotelName`, `remarks`) are additive and optional.
- Schedule change = OLD ∈ {HX, UN} (newly so in this version), NEW ∈ {HK, KK, TK} (segment not in previous version), same origin + destination + departure date, |Δ departure| ≤ 60 min, unique 1:1 only.
- RTM: classic, dedicated (`SINGLE_USER`) access, DBR `18.x-scala2.13`, `spark.databricks.streaming.realTimeMode.enabled=true`, `.trigger(realTime="5 minutes")`, output mode `update`, `spark.sql.shuffle.partitions=2`.
- Lakebase auth from Spark sink and App: Postgres **native password** role `trip_intel_app`, password in secret scope `bcd-trip-intel` key `lakebase-password`.
- Code style: black line length 125 (from `pyproject.toml`); module docstrings like the existing transforms.
- Do not commit secrets. Do not modify `src/scheduled_changes_etl/` (existing demo stays intact).

## Review Focus

1. **Kinesis redelivery / out-of-order versions** (v5 arrives after v6, or v6 twice) → must not regress trip state or double-insert outbox rows. Pinned in Task 3 (`test_stale_version_*`) and Task 5 (`test_statements_are_idempotent_*`).
2. **Real BCD payloads lack the demo extensions** (no `travelers`, `payment`, `seat`) → parse, diff and rules must not crash; contact/payment tags must not fire on `None → None`. Pinned in Task 2 (`test_snapshot_from_real_uat_payload`) and Task 3 (`test_real_uat_versions_diff`); payloads with no id/version are filtered before state (Task 7 Step 4).
3. **A bad or LLM-generated rule** (unknown operator, `None < 5`, wrong shape) → that rule is skipped and reported; the stream keeps running and other rules still fire. Pinned in Task 4 (`test_broken_rule_does_not_block_others`, `test_comparisons_with_none_are_false`).
4. **Rule spam:** a "segment without seat" rule must fire once when the condition becomes true, not on every one of the next 10 versions. Pinned in Task 4 (`test_newly_matched_is_edge_triggered`).
5. **Ambiguous re-protection** (two candidate replacement flights) and **already-cancelled segments** in prior versions → no SCHEDULE_CHANGE. Pinned in Task 3 (`test_ambiguous_match_dropped`, `test_already_cancelled_not_reflagged`).

---

## File Structure

```
pyproject.toml                         # MODIFY: wheel packages, deps, pytest pythonpath
databricks.yml                         # MODIFY: artifacts (wheel), lakebase_host variable
resources/
  trip_intel_rtm.job.yml               # CREATE: RTM job on single-node classic cluster
  trip_intel_dispatcher.job.yml        # CREATE: serverless outbox dispatcher job
  trip_intel_analytics.job.yml         # CREATE: hourly serverless analytics snapshot job
  trip_intel.app.yml                   # CREATE: Databricks App resource
src/
  trip_intel/                          # CREATE: pure-Python domain logic (no Spark)
    __init__.py
    payload.py                         # BCD JSON -> compact snapshot
    generator.py                       # deterministic BCD-schema PNR version generator
    schedule_change.py                 # OLD->NEW 1:1 pairing
    diff.py                            # version-vs-previous diff + tags
    jsonlogic.py                       # JSON-Logic evaluator + validator
    rules.py                           # Rule, context building, edge-triggered evaluation, cache
    outbox.py                          # action records + EventBridge entry
    sink_sql.py                        # processor output -> idempotent SQL statements
  rtm/                                 # CREATE: Spark wiring (runs only on cluster)
    __init__.py
    lakebase.py                        # conninfo + rule loader (psycopg)
    processor.py                       # StatefulProcessor + output schema
    sink.py                            # ForeachWriter
    job.py                             # entry point: source=rate|kinesis
    smoke.py                           # Task 1 RTM feasibility probe
  dispatcher/
    dispatch.py                        # outbox -> EventBridge or SIMULATED
  analytics/
    snapshot.sql                       # Lakebase catalog -> Delta tables
  app.yaml                             # CREATE: Databricks App config (app root = src/)
  requirements.txt                     # CREATE: App deps
  app/
    __init__.py
    serve.py                           # uvicorn launcher (DATABRICKS_APP_PORT)
    db.py                              # psycopg pool
    rule_drafting.py                   # NL -> JSON Logic prompt + parser
    main.py                            # FastAPI routes
    static/index.html, app.js, style.css
sql/lakebase_schema.sql                # CREATE: Postgres DDL + grants
scripts/setup_lakebase.sh              # CREATE: instance, role, secret, schema
tests/
  data/uat_t1_v2.json, uat_t1_v3.json  # real UAT payloads (from bcd_api_uat.silver.trips_all_versions)
  test_payload.py, test_generator.py, test_schedule_change.py, test_diff.py,
  test_jsonlogic.py, test_rules.py, test_outbox.py, test_sink_sql.py, test_rule_drafting.py
docs/DEMO_RUNBOOK.md                   # CREATE: talk track, reset steps, UAT finale
```

---

### Task 1: RTM feasibility probe (de-risk before anything else)

The FE guide says the `rate` source works in RTM; the public reference does not list it. Prove rate + Python UDF + row-based `transformWithState` + Python `ForeachWriter` under `trigger(realTime=...)` on FEVM before building on it.

**Files:**
- Create: `src/rtm/__init__.py` (empty), `src/rtm/smoke.py`

**Interfaces:**
- Produces: a go/no-go decision recorded in the spec's Risks table; confirmed API names (`StatefulProcessor`, `handleInputRows(key, rows, timerValues)`, `transformWithState(..., outputMode="Update", timeMode="None")`).

- [ ] **Step 1: Write the probe**

```python
"""RTM feasibility probe: rate source -> Python UDF -> transformWithState -> ForeachWriter (stdout).

Run on a single-node DBR 18 LTS dedicated cluster with
spark.databricks.streaming.realTimeMode.enabled=true. Watch the driver log for
'PROBE' lines; latency_ms should be well under 1000.
"""
import time
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

STATE = StructType([StructField("count", LongType())])
OUT = StructType(
    [StructField("key", StringType()), StructField("count", LongType()), StructField("event_ts", TimestampType())]
)


class CountProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.state = handle.getValueState("count", STATE)

    def handleInputRows(self, key, rows, timerValues):
        count = self.state.get()[0] if self.state.exists() else 0
        last_ts = None
        for row in rows:
            count += 1
            last_ts = row.event_ts
        self.state.update((count,))
        yield Row(key=key[0], count=count, event_ts=last_ts)

    def close(self) -> None:
        pass


class PrintSink:
    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        latency_ms = (datetime.now(timezone.utc) - row.event_ts.replace(tzinfo=timezone.utc)).total_seconds() * 1000
        print(f"PROBE key={row.key} count={row.count} latency_ms={latency_ms:.1f}", flush=True)

    def close(self, error):
        if error:
            print(f"PROBE sink error: {error}", flush=True)


@F.udf("string")
def fake_payload(value):
    return f"trip-{value % 5}"


def main():
    spark = SparkSession.builder.getOrCreate()
    src = (
        spark.readStream.format("rate").option("rowsPerSecond", 15).option("numPartitions", 1).load()
        .select(fake_payload("value").alias("key"), F.col("timestamp").alias("event_ts"))
    )
    out = src.groupBy("key").transformWithState(
        statefulProcessor=CountProcessor(), outputStructType=OUT, outputMode="Update", timeMode="None"
    )
    q = (
        out.writeStream.foreach(PrintSink()).outputMode("update")
        .option("checkpointLocation", f"/tmp/rtm_probe_{int(time.time())}")
        .trigger(realTime="5 minutes")
        .start()
    )
    q.awaitTermination(180)
    q.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the probe on a one-off cluster**

Upload and submit a one-time run (no bundle yet):

```bash
P=fevm-stable-71zsua
ME=$(databricks current-user me --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["userName"])')
databricks workspace mkdirs /Workspace/Users/$ME/trip_intel_probe --profile $P
databricks workspace import /Workspace/Users/$ME/trip_intel_probe/smoke.py --file src/rtm/smoke.py --format AUTO --language PYTHON --overwrite --profile $P
cat > /tmp/probe_run.json <<EOF
{"run_name":"rtm_probe","tasks":[{"task_key":"probe",
 "spark_python_task":{"python_file":"/Workspace/Users/$ME/trip_intel_probe/smoke.py"},
 "new_cluster":{"spark_version":"18.x-scala2.13","node_type_id":"m5d.2xlarge","num_workers":0,
   "data_security_mode":"SINGLE_USER","single_user_name":"$ME",
   "spark_conf":{"spark.master":"local[*, 4]","spark.databricks.cluster.profile":"singleNode",
     "spark.databricks.streaming.realTimeMode.enabled":"true","spark.sql.shuffle.partitions":"2"},
   "custom_tags":{"ResourceClass":"SingleNode"}}}]}
EOF
databricks jobs submit --json @/tmp/probe_run.json --profile $P --timeout 30m
```

Expected: run SUCCESS after ~3 min of streaming. Then fetch driver output:

```bash
RUN_ID=<run_id printed above>; TASK_RUN=$(databricks jobs get-run $RUN_ID --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["tasks"][0]["run_id"])')
databricks jobs get-run-output $TASK_RUN --profile $P -o json | python3 -c 'import json,sys;d=json.load(sys.stdin);print((d.get("logs") or "")[-3000:]);print(d.get("error"))'
```

Expected: lines like `PROBE key=trip-3 count=412 latency_ms=35.2` (well under 1000 ms).

- [ ] **Step 3: Record the decision**

If PASS: in the spec Risks table, change the first row's mitigation to `Verified 2026-10-05: rate+TWS+ForeachWriter in RTM, p50 ≈ <N> ms`.
If FAIL with an unsupported-source/operator error: switch every later task's trigger from `realTime="5 minutes"` to `processingTime="1 second"` (same cluster, no RTM conf), record the error text and "~1 s latency fallback" in the spec. Everything else in the plan is unchanged.

- [ ] **Step 4: Commit**

```bash
git add src/rtm/__init__.py src/rtm/smoke.py docs/superpowers/specs/2026-10-05-bcd-realtime-demos-design.md
git commit -m "spike: verify RTM rate source + transformWithState + ForeachWriter on DBR 18 LTS

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Project packaging, snapshot parser and generator

**Files:**
- Modify: `pyproject.toml`
- Create: `src/trip_intel/__init__.py` (empty), `src/trip_intel/payload.py`, `src/trip_intel/generator.py`
- Create: `tests/data/uat_t1_v2.json`, `tests/data/uat_t1_v3.json`
- Test: `tests/test_payload.py`, `tests/test_generator.py`

**Interfaces:**
- Produces:
  - `payload.to_snapshot(payload: dict) -> dict` with keys `trip_id, record_locator, version, segments[list[dict]], traveler{name,email,phone}, payment{type,last4}`; each segment dict has `type, segment_id, segment_number, status, carrier, flight_number, class_of_service, origin, destination, start, end, seat, hotel_name`.
  - `payload.parse(raw: str) -> dict` (= `to_snapshot(json.loads(raw))`).
  - `generator.locate(seq: int) -> tuple[int, int]` (trip_no, version), `generator.scenario_for(trip_no: int) -> str`, `generator.build_payload(trip_no: int, version: int) -> dict`, `generator.payload_for(seq: int) -> str`.
  - Constants: `ACTIVE_TRIPS = 50`, `VERSIONS_PER_TRIP = 12`, `MUTATION_VERSION = 6`, `SCENARIOS` list.

- [ ] **Step 1: Update `pyproject.toml`**

Replace the `dependencies = [...]` block and append the tool sections:

```toml
dependencies = [
    "psycopg[binary]>=3.2",
]
```

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/trip_intel", "src/rtm"]

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
```

Then: `.venv/bin/pip install pytest "psycopg[binary]>=3.2" fastapi httpx` (local test deps; pyspark is not needed for unit tests).

- [ ] **Step 2: Save the real UAT payloads as fixtures**

`tests/data/uat_t1_v2.json`:
```json
{"identification":{"id":"t1","version":2,"recordLocator":"rl1"},"segments":[{"type":"Air","segmentId":"manual-segment","segmentNumber":3,"status":{"code":"KK"},"marketingFlight":{"airlineCode":"delta","flightNumber":"342"},"classOfService":{"code":"H"},"departureAirport":{"airport":{"code":"SFO"}},"arrivalAirport":{"airport":{"code":"LAX"}},"startDateTime":"2026-08-01T03:25:00+00:00","endDateTime":"2026-08-01T07:43:00+00:00"}]}
```
`tests/data/uat_t1_v3.json`:
```json
{"identification":{"id":"t1","version":3,"recordLocator":"rl1"},"segments":[{"type":"Air","segmentId":"manual-segment","segmentNumber":3,"status":{"code":"HX"},"marketingFlight":{"airlineCode":"delta","flightNumber":"342"},"classOfService":{"code":"H"},"departureAirport":{"airport":{"code":"SFO"}},"arrivalAirport":{"airport":{"code":"LAX"}},"startDateTime":"2026-08-01T03:25:00+00:00","endDateTime":"2026-08-01T09:43:00+00:00"}]}
```

- [ ] **Step 3: Write failing tests**

`tests/test_payload.py`:
```python
import json
from pathlib import Path

from trip_intel.payload import parse, to_snapshot

DATA = Path(__file__).parent / "data"


def test_snapshot_from_real_uat_payload():
    snap = parse((DATA / "uat_t1_v2.json").read_text())
    assert snap["trip_id"] == "t1"
    assert snap["record_locator"] == "rl1"
    assert snap["version"] == 2
    seg = snap["segments"][0]
    assert seg == {
        "type": "Air", "segment_id": "manual-segment", "segment_number": 3, "status": "KK",
        "carrier": "delta", "flight_number": "342", "class_of_service": "H", "origin": "SFO",
        "destination": "LAX", "start": "2026-08-01T03:25:00+00:00", "end": "2026-08-01T07:43:00+00:00",
        "seat": None, "hotel_name": None,
    }
    # Real payloads have no extensions: sections exist but are all-None.
    assert snap["traveler"] == {"name": None, "email": None, "phone": None}
    assert snap["payment"] == {"type": None, "last4": None}


def test_snapshot_tolerates_empty_payload():
    snap = to_snapshot({})
    assert snap["trip_id"] is None
    assert snap["segments"] == []


def test_snapshot_reads_extensions():
    snap = to_snapshot({
        "identification": {"id": "T1", "recordLocator": "ABC123", "version": 1},
        "segments": [{"type": "Air", "segmentId": "S1", "seat": {"number": "12C"}}],
        "travelers": [{"name": "Ada", "email": "ada@x.com", "phone": "+1"}],
        "payment": {"type": "CC", "last4": "4242"},
    })
    assert snap["segments"][0]["seat"] == "12C"
    assert snap["traveler"]["email"] == "ada@x.com"
    assert snap["payment"]["last4"] == "4242"
```

`tests/test_generator.py`:
```python
import json

from trip_intel import generator as g
from trip_intel.payload import to_snapshot


def test_locate_interleaves_trips():
    assert g.locate(0) == (0, 1)
    assert g.locate(1) == (1, 1)
    assert g.locate(g.ACTIVE_TRIPS) == (0, 2)
    block = g.ACTIVE_TRIPS * g.VERSIONS_PER_TRIP
    assert g.locate(block) == (g.ACTIVE_TRIPS, 1)


def test_payload_matches_bcd_schema():
    p = json.loads(g.payload_for(0))
    assert set(p["identification"]) == {"id", "recordLocator", "version"}
    seg = p["segments"][0]
    for key in ["type", "segmentId", "segmentNumber", "status", "marketingFlight", "classOfService",
                "departureAirport", "arrivalAirport", "startDateTime", "endDateTime"]:
        assert key in seg
    assert seg["status"]["code"] == "HK"
    assert seg["departureAirport"]["airport"]["code"]


def test_deterministic():
    assert g.payload_for(12345) == g.payload_for(12345)


def test_every_scenario_appears_within_first_block():
    seen = {g.scenario_for(t) for t in range(g.ACTIVE_TRIPS)}
    assert seen == set(g.SCENARIOS)


def test_schedule_change_mutation_at_mutation_version():
    trip = g.SCENARIOS.index("schedule_change")
    before = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    after = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert [s["status"] for s in before["segments"]] == ["HK", "HK"]
    statuses = {s["segment_id"]: s["status"] for s in after["segments"]}
    assert statuses[f"T{trip:07d}-S1"] == "HX"
    assert statuses[f"T{trip:07d}-S1R"] == "HK"


def test_versions_after_mutation_only_touch_noise():
    trip = g.SCENARIOS.index("seat_removed")
    a = g.build_payload(trip, g.MUTATION_VERSION)
    b = g.build_payload(trip, g.MUTATION_VERSION + 1)
    assert a["remarks"] != b["remarks"]
    a.pop("remarks"); b.pop("remarks"); a["identification"].pop("version"); b["identification"].pop("version")
    assert a == b
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_payload.py tests/test_generator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trip_intel'`.

- [ ] **Step 5: Implement `src/trip_intel/payload.py`**

```python
"""Turn a BCD trip payload (the Kinesis JSON) into a compact snapshot used for diffing and rules.

Only BCD's real fields are required; demo extensions (seat, travelers, payment, hotelName)
are optional and come back as None when absent.
"""
import json


def _get(d, *path):
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def _segment(s: dict) -> dict:
    return {
        "type": s.get("type"),
        "segment_id": s.get("segmentId"),
        "segment_number": s.get("segmentNumber"),
        "status": _get(s, "status", "code"),
        "carrier": _get(s, "marketingFlight", "airlineCode"),
        "flight_number": _get(s, "marketingFlight", "flightNumber"),
        "class_of_service": _get(s, "classOfService", "code"),
        "origin": _get(s, "departureAirport", "airport", "code"),
        "destination": _get(s, "arrivalAirport", "airport", "code"),
        "start": s.get("startDateTime"),
        "end": s.get("endDateTime"),
        "seat": _get(s, "seat", "number"),
        "hotel_name": s.get("hotelName"),
    }


def to_snapshot(payload: dict) -> dict:
    travelers = payload.get("travelers") or [{}]
    t0 = travelers[0] or {}
    payment = payload.get("payment") or {}
    return {
        "trip_id": _get(payload, "identification", "id"),
        "record_locator": _get(payload, "identification", "recordLocator"),
        "version": _get(payload, "identification", "version"),
        "segments": [_segment(s) for s in payload.get("segments") or []],
        "traveler": {"name": t0.get("name"), "email": t0.get("email"), "phone": t0.get("phone")},
        "payment": {"type": payment.get("type"), "last4": payment.get("last4")},
    }


def parse(raw: str) -> dict:
    return to_snapshot(json.loads(raw))
```

- [ ] **Step 6: Implement `src/trip_intel/generator.py`**

```python
"""Deterministic generator of BCD-schema PNR versions for the demo stream.

seq (the rate source's monotonically increasing `value`) maps to (trip_no, version) so that
ACTIVE_TRIPS trips are interleaved and each receives VERSIONS_PER_TRIP versions. Most versions
are noise (only `remarks` changes, like GDS systems re-saving the record); at MUTATION_VERSION
each trip's scenario mutation lands and persists for the remaining versions.
"""
import json
from datetime import datetime, timedelta, timezone

ACTIVE_TRIPS = 50
VERSIONS_PER_TRIP = 12
MUTATION_VERSION = 6
SCENARIOS = [
    "churn", "schedule_change", "seat_removed", "contact_changed",
    "churn", "payment_changed", "ambiguous_schedule_change", "hotel_added",
]
ROUTES = [("SFO", "LAX"), ("JFK", "YYZ"), ("ATL", "ORD"), ("BOS", "DCA"), ("SEA", "DEN"), ("LHR", "CDG")]
CARRIERS = ["DL", "UA", "AA", "AC", "BA"]
SYSTEMS = ["OBT", "MID_OFFICE", "TICKETING", "QC_ROBOT", "GDS_QUEUE"]
NAMES = ["Ada Lovelace", "Grace Hopper", "Alan Turing", "Katherine Johnson", "Linus Pauling", "Mae Jemison"]
BASE_DATE = datetime(2026, 11, 2, tzinfo=timezone.utc)


def locate(seq: int) -> tuple[int, int]:
    window, idx = divmod(seq, ACTIVE_TRIPS * VERSIONS_PER_TRIP)
    return window * ACTIVE_TRIPS + idx % ACTIVE_TRIPS, idx // ACTIVE_TRIPS + 1


def scenario_for(trip_no: int) -> str:
    return SCENARIOS[trip_no % len(SCENARIOS)]


def _air(seg_id, number, status, carrier, flight, origin, dest, dep, seat):
    seg = {
        "type": "Air",
        "segmentId": seg_id,
        "segmentNumber": number,
        "status": {"code": status},
        "marketingFlight": {"airlineCode": carrier, "flightNumber": flight},
        "classOfService": {"code": "Y"},
        "departureAirport": {"airport": {"code": origin}},
        "arrivalAirport": {"airport": {"code": dest}},
        "startDateTime": dep.isoformat(),
        "endDateTime": (dep + timedelta(hours=2, minutes=15)).isoformat(),
    }
    if seat:
        seg["seat"] = {"number": seat}
    return seg


def build_payload(trip_no: int, version: int) -> dict:
    tid = f"T{trip_no:07d}"
    origin, dest = ROUTES[trip_no % len(ROUTES)]
    carrier = CARRIERS[trip_no % len(CARRIERS)]
    dep = BASE_DATE + timedelta(days=trip_no % 30, hours=6 + trip_no % 10)
    flight = str(100 + trip_no % 900)
    segments = [
        _air(f"{tid}-S1", 1, "HK", carrier, flight, origin, dest, dep, f"{12 + trip_no % 20}C"),
        _air(f"{tid}-S2", 2, "HK", carrier, str(int(flight) + 1000), dest, origin, dep + timedelta(days=3), "14A"),
    ]
    name = NAMES[trip_no % len(NAMES)]
    traveler = {"name": name, "email": f"{name.split()[0].lower()}.{trip_no}@example.com", "phone": f"+1-555-{trip_no % 10000:04d}"}
    payment = {"type": "CC", "last4": f"{1000 + trip_no % 9000}"}

    if version >= MUTATION_VERSION:
        scenario = scenario_for(trip_no)
        if scenario == "schedule_change":
            segments[0]["status"] = {"code": "HX"}
            segments.append(_air(f"{tid}-S1R", 3, "HK", carrier, str(int(flight) + 1), origin, dest, dep + timedelta(minutes=45), None))
        elif scenario == "ambiguous_schedule_change":
            segments[0]["status"] = {"code": "HX"}
            segments.append(_air(f"{tid}-S1R", 3, "HK", carrier, str(int(flight) + 1), origin, dest, dep + timedelta(minutes=30), None))
            segments.append(_air(f"{tid}-S1Q", 4, "HK", carrier, str(int(flight) + 2), origin, dest, dep + timedelta(minutes=50), None))
        elif scenario == "seat_removed":
            segments[0].pop("seat")
        elif scenario == "contact_changed":
            traveler["email"] = f"{name.split()[0].lower()}.new.{trip_no}@example.com"
        elif scenario == "payment_changed":
            payment["last4"] = f"{9999 - trip_no % 9000}"
        elif scenario == "hotel_added":
            segments.append({
                "type": "Hotel", "segmentId": f"{tid}-H1", "segmentNumber": 3, "status": {"code": "HK"},
                "hotelName": "Grand Central Hotel", "startDateTime": dep.isoformat(),
                "endDateTime": (dep + timedelta(days=3)).isoformat(),
            })

    return {
        "identification": {"id": tid, "recordLocator": f"R{trip_no:05d}", "version": version},
        "segments": segments,
        "travelers": [traveler],
        "payment": payment,
        "remarks": f"saved by {SYSTEMS[version % len(SYSTEMS)]} v{version}",
    }


def payload_for(seq: int) -> str:
    trip_no, version = locate(seq)
    return json.dumps(build_payload(trip_no, version), separators=(",", ":"))
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_payload.py tests/test_generator.py -v`
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/trip_intel/__init__.py src/trip_intel/payload.py src/trip_intel/generator.py tests/
git commit -m "feat: trip snapshot parser and deterministic BCD-schema generator

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: Schedule-change detection and version diff

**Files:**
- Create: `src/trip_intel/schedule_change.py`, `src/trip_intel/diff.py`
- Test: `tests/test_schedule_change.py`, `tests/test_diff.py`

**Interfaces:**
- Consumes: snapshot dicts from `payload.to_snapshot`.
- Produces:
  - `schedule_change.detect(prev: dict, new: dict) -> list[dict]`; each item `{"old": segment_dict, "new": segment_dict, "delta_minutes": int}`.
  - `diff.diff(prev: dict | None, new: dict) -> dict` returning `{"from_version": int|None, "to_version": int|None, "tags": list[str] (sorted), "changes": list[{"segment","field","old","new"}], "schedule_changes": list}`.
  - `diff.EMPTY_CHANGE` = `{"from_version": None, "to_version": None, "tags": [], "changes": [], "schedule_changes": []}`.
  - Tag vocabulary: `NEW_TRIP, STALE_VERSION, SEGMENT_ADDED, SEGMENT_REMOVED, SEGMENT_MODIFIED, SEAT_REMOVED, SEAT_CHANGED, CONTACT_CHANGED, PAYMENT_CHANGED, SCHEDULE_CHANGE, NO_SIGNIFICANT_CHANGE`.

- [ ] **Step 1: Write failing tests**

`tests/test_schedule_change.py`:
```python
from trip_intel import generator as g
from trip_intel.payload import to_snapshot
from trip_intel.schedule_change import detect


def _seg(seg_id, status, start, origin="SFO", dest="LAX", type_="Air"):
    return {"type": type_, "segment_id": seg_id, "segment_number": 1, "status": status, "carrier": "DL",
            "flight_number": "1", "class_of_service": "Y", "origin": origin, "destination": dest,
            "start": start, "end": None, "seat": None, "hotel_name": None}


def _snap(*segs):
    return {"segments": list(segs)}


def test_simple_schedule_change():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:45:00+00:00"))
    pairs = detect(prev, new)
    assert len(pairs) == 1
    assert pairs[0]["old"]["segment_id"] == "A" and pairs[0]["new"]["segment_id"] == "B"
    assert pairs[0]["delta_minutes"] == 45


def test_un_and_kk_tk_codes_count():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    for old_code in ["HX", "UN"]:
        for new_code in ["HK", "KK", "TK"]:
            new = _snap(_seg("A", old_code, "2026-11-02T06:00:00+00:00"), _seg("B", new_code, "2026-11-02T06:10:00+00:00"))
            assert len(detect(prev, new)) == 1, (old_code, new_code)


def test_outside_window_ignored():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T07:01:00+00:00"))
    assert detect(prev, new) == []


def test_different_route_ignored():
    prev = _snap(_seg("A", "HK", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:10:00+00:00", dest="SAN"))
    assert detect(prev, new) == []


def test_ambiguous_match_dropped():
    trip = g.SCENARIOS.index("ambiguous_schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert detect(prev, new) == []


def test_already_cancelled_not_reflagged():
    prev = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"))
    new = _snap(_seg("A", "HX", "2026-11-02T06:00:00+00:00"), _seg("B", "HK", "2026-11-02T06:10:00+00:00"))
    assert detect(prev, new) == []


def test_generator_schedule_change_detected():
    trip = g.SCENARIOS.index("schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    assert len(detect(prev, new)) == 1
```

`tests/test_diff.py`:
```python
from pathlib import Path

from trip_intel import generator as g
from trip_intel.diff import diff
from trip_intel.payload import parse, to_snapshot

DATA = Path(__file__).parent / "data"


def _pair(scenario):
    trip = g.SCENARIOS.index(scenario)
    return (to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1)),
            to_snapshot(g.build_payload(trip, g.MUTATION_VERSION)))


def test_new_trip():
    snap = to_snapshot(g.build_payload(0, 1))
    result = diff(None, snap)
    assert result["tags"] == ["NEW_TRIP"]
    assert result["from_version"] is None and result["to_version"] == 1


def test_noise_version_is_not_significant():
    a = to_snapshot(g.build_payload(0, 2))
    b = to_snapshot(g.build_payload(0, 3))
    assert diff(a, b)["tags"] == ["NO_SIGNIFICANT_CHANGE"]


def test_stale_version_ignored():
    a = to_snapshot(g.build_payload(0, 5))
    b = to_snapshot(g.build_payload(0, 4))
    assert diff(a, b)["tags"] == ["STALE_VERSION"]


def test_stale_version_duplicate_ignored():
    a = to_snapshot(g.build_payload(0, 5))
    assert diff(a, a)["tags"] == ["STALE_VERSION"]


def test_schedule_change_tags():
    prev, new = _pair("schedule_change")
    result = diff(prev, new)
    assert {"SCHEDULE_CHANGE", "SEGMENT_ADDED", "SEGMENT_MODIFIED"} <= set(result["tags"])
    assert len(result["schedule_changes"]) == 1
    assert {"segment": prev["segments"][0]["segment_id"], "field": "status", "old": "HK", "new": "HX"} in result["changes"]


def test_seat_removed():
    prev, new = _pair("seat_removed")
    assert diff(prev, new)["tags"] == ["SEAT_REMOVED"]


def test_contact_and_payment():
    assert diff(*_pair("contact_changed"))["tags"] == ["CONTACT_CHANGED"]
    assert diff(*_pair("payment_changed"))["tags"] == ["PAYMENT_CHANGED"]


def test_hotel_added():
    assert diff(*_pair("hotel_added"))["tags"] == ["SEGMENT_ADDED"]


def test_real_uat_versions_diff():
    prev = parse((DATA / "uat_t1_v2.json").read_text())
    new = parse((DATA / "uat_t1_v3.json").read_text())
    result = diff(prev, new)
    # KK -> HX with no replacement and a later arrival: a modification, not a schedule change,
    # and no contact/payment noise from the missing extensions.
    assert result["tags"] == ["SEGMENT_MODIFIED"]
    fields = {c["field"] for c in result["changes"]}
    assert fields == {"status", "end"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_schedule_change.py tests/test_diff.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trip_intel.schedule_change'`.

- [ ] **Step 3: Implement `src/trip_intel/schedule_change.py`**

```python
"""Airline-initiated scheduled-change detection between two versions of the same trip.

OLD: an Air segment that became HX/UN in this version. NEW: a confirmed (HK/KK/TK) Air segment
that did not exist in the previous version. A pair needs the same origin, destination and
departure date, departures within 60 minutes, and must be a unique 1:1 match — ambiguous
re-protections are dropped.
"""
from collections import Counter
from datetime import datetime

OLD_CODES = {"HX", "UN"}
NEW_CODES = {"HK", "KK", "TK"}
MATCH_WINDOW_SECONDS = 60 * 60


def _ts(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def detect(prev: dict, new: dict) -> list[dict]:
    prev_by_id = {s["segment_id"]: s for s in prev["segments"]}
    olds = [
        s for s in new["segments"]
        if s["type"] == "Air" and s["status"] in OLD_CODES
        and (prev_by_id.get(s["segment_id"]) or {}).get("status") not in OLD_CODES
    ]
    news = [
        s for s in new["segments"]
        if s["type"] == "Air" and s["status"] in NEW_CODES and s["segment_id"] not in prev_by_id
    ]
    candidates = []
    for o in olds:
        for n in news:
            o_ts, n_ts = _ts(o["start"]), _ts(n["start"])
            if o_ts is None or n_ts is None:
                continue
            if (o["origin"], o["destination"], o["start"][:10]) != (n["origin"], n["destination"], n["start"][:10]):
                continue
            delta = (n_ts - o_ts).total_seconds()
            if abs(delta) <= MATCH_WINDOW_SECONDS:
                candidates.append((o, n, int(delta // 60)))
    old_counts = Counter(id(o) for o, _, _ in candidates)
    new_counts = Counter(id(n) for _, n, _ in candidates)
    return [
        {"old": o, "new": n, "delta_minutes": d}
        for o, n, d in candidates
        if old_counts[id(o)] == 1 and new_counts[id(n)] == 1
    ]
```

- [ ] **Step 4: Implement `src/trip_intel/diff.py`**

```python
"""Diff one trip version against the previous one and tag what changed.

This is the per-version change tracking BCD runs today in an AWS Lambda: most GDS re-saves change
nothing that matters (NO_SIGNIFICANT_CHANGE); the tags tell each consumer whether to care.
"""
from trip_intel.schedule_change import detect

SEGMENT_FIELDS = ("status", "carrier", "flight_number", "class_of_service", "origin", "destination",
                  "start", "end", "hotel_name")
EMPTY_CHANGE = {"from_version": None, "to_version": None, "tags": [], "changes": [], "schedule_changes": []}


def _key(seg: dict) -> str:
    return seg["segment_id"] or f"{seg['type']}#{seg['segment_number']}"


def diff(prev: dict | None, new: dict) -> dict:
    result = {"from_version": prev["version"] if prev else None, "to_version": new["version"],
              "tags": [], "changes": [], "schedule_changes": []}
    if prev is None:
        result["tags"] = ["NEW_TRIP"]
        return result
    if prev["version"] is not None and new["version"] is not None and new["version"] <= prev["version"]:
        result["tags"] = ["STALE_VERSION"]
        return result

    tags, changes = set(), []
    prev_segs = {_key(s): s for s in prev["segments"]}
    new_segs = {_key(s): s for s in new["segments"]}
    for k in sorted(new_segs.keys() - prev_segs.keys()):
        tags.add("SEGMENT_ADDED")
        changes.append({"segment": k, "field": "*", "old": None, "new": "added"})
    for k in sorted(prev_segs.keys() - new_segs.keys()):
        tags.add("SEGMENT_REMOVED")
        changes.append({"segment": k, "field": "*", "old": "present", "new": None})
    for k in sorted(prev_segs.keys() & new_segs.keys()):
        old_seg, new_seg = prev_segs[k], new_segs[k]
        for field in SEGMENT_FIELDS:
            if old_seg.get(field) != new_seg.get(field):
                tags.add("SEGMENT_MODIFIED")
                changes.append({"segment": k, "field": field, "old": old_seg.get(field), "new": new_seg.get(field)})
        if old_seg.get("seat") != new_seg.get("seat"):
            tags.add("SEAT_REMOVED" if new_seg.get("seat") is None else "SEAT_CHANGED")
            changes.append({"segment": k, "field": "seat", "old": old_seg.get("seat"), "new": new_seg.get("seat")})
    for section, tag in (("traveler", "CONTACT_CHANGED"), ("payment", "PAYMENT_CHANGED")):
        for field, old_value in prev[section].items():
            new_value = new[section].get(field)
            if old_value != new_value:
                tags.add(tag)
                changes.append({"segment": None, "field": f"{section}.{field}", "old": old_value, "new": new_value})

    result["schedule_changes"] = detect(prev, new)
    if result["schedule_changes"]:
        tags.add("SCHEDULE_CHANGE")
    result["tags"] = sorted(tags) or ["NO_SIGNIFICANT_CHANGE"]
    result["changes"] = changes
    return result
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_schedule_change.py tests/test_diff.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/trip_intel/schedule_change.py src/trip_intel/diff.py tests/test_schedule_change.py tests/test_diff.py
git commit -m "feat: per-version trip diff with change tags and 1:1 schedule-change detection

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: JSON-Logic evaluator and rule evaluation

**Files:**
- Create: `src/trip_intel/jsonlogic.py`, `src/trip_intel/rules.py`
- Test: `tests/test_jsonlogic.py`, `tests/test_rules.py`

**Interfaces:**
- Consumes: `diff.EMPTY_CHANGE`, change dicts from `diff.diff`, snapshots.
- Produces:
  - `jsonlogic.apply(logic, data)` → value; `jsonlogic.truthy(value) -> bool`; `jsonlogic.validate(logic) -> None` (raises `ValueError` on unknown op / bad shape); `jsonlogic.OPERATORS: frozenset[str]`.
  - `rules.Rule` frozen dataclass `(rule_id: str, name: str, logic: dict, action: dict)`.
  - `rules.context(snapshot: dict | None, change: dict) -> dict` = `{"trip": snapshot or {}, "change": change}`.
  - `rules.newly_matched(rules: list[Rule], prev_ctx: dict, new_ctx: dict) -> tuple[list[Rule], list[dict]]` → (fired, errors[{rule_id, error}]).
  - `rules.RuleCache(loader: Callable[[], list[Rule]], ttl_seconds: float = 5.0, clock=time.monotonic)` with `.get() -> list[Rule]` (keeps last good list if loader raises).

- [ ] **Step 1: Write failing tests**

`tests/test_jsonlogic.py`:
```python
import pytest

from trip_intel.jsonlogic import apply, validate


def test_var_and_equality():
    assert apply({"==": [{"var": "a.b"}, 1]}, {"a": {"b": 1}}) is True
    assert apply({"var": "segments.0.status"}, {"segments": [{"status": "HK"}]}) == "HK"
    assert apply({"var": ["missing.path", "dflt"]}, {}) == "dflt"


def test_logic_ops():
    assert apply({"and": [True, {"!": False}]}, {}) is True
    assert apply({"or": [False, 0, "x"]}, {}) == "x"
    assert apply({"if": [False, "a", True, "b", "c"]}, {}) == "b"


def test_in_and_missing():
    assert apply({"in": ["SCHEDULE_CHANGE", {"var": "tags"}]}, {"tags": ["SCHEDULE_CHANGE"]}) is True
    assert apply({"missing": ["seat", "status"]}, {"status": "HK", "seat": None}) == ["seat"]


def test_some_all_none():
    data = {"segs": [{"type": "Air", "seat": None}, {"type": "Hotel"}]}
    assert apply({"some": [{"var": "segs"}, {"==": [{"var": "type"}, "Air"]}]}, data) is True
    assert apply({"all": [{"var": "segs"}, {"==": [{"var": "type"}, "Air"]}]}, data) is False
    assert apply({"none": [{"var": "segs"}, {"==": [{"var": "type"}, "Rail"]}]}, data) is True
    assert apply({"all": [[], True]}, {}) is False


def test_comparisons_with_none_are_false():
    assert apply({"<": [{"var": "x"}, 5]}, {}) is False
    assert apply({"<=": [1, {"var": "x"}, 3]}, {"x": 2}) is True


def test_validate_rejects_unknown_operator():
    with pytest.raises(ValueError, match="unknown operator"):
        validate({"and": [{"regex": ["a", "b"]}]})


def test_validate_accepts_known():
    validate({"some": [{"var": "trip.segments"}, {"missing": "seat"}]})
```

`tests/test_rules.py`:
```python
from trip_intel import generator as g
from trip_intel.diff import EMPTY_CHANGE, diff
from trip_intel.payload import to_snapshot
from trip_intel.rules import Rule, RuleCache, context, newly_matched

NO_SEAT = Rule("r-seat", "Air segment without seat", {
    "some": [{"var": "trip.segments"}, {"and": [
        {"==": [{"var": "type"}, "Air"]}, {"in": [{"var": "status"}, ["HK", "KK", "TK"]]}, {"missing": "seat"}]}]
}, {"type": "INVOKE_SERVICE", "target": "seat-assignment-service"})
SCHED = Rule("r-sched", "Schedule change", {"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]},
             {"type": "NOTIFY_TRAVELER", "target": "traveler-notifications"})


def _ctxs(scenario, v_prev, v_new):
    trip = g.SCENARIOS.index(scenario)
    prev = to_snapshot(g.build_payload(trip, v_prev))
    new = to_snapshot(g.build_payload(trip, v_new))
    return context(prev, EMPTY_CHANGE), context(new, diff(prev, new))


def test_seat_rule_fires_on_seat_removed():
    fired, errors = newly_matched([NO_SEAT], *_ctxs("seat_removed", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-seat"] and errors == []


def test_newly_matched_is_edge_triggered():
    fired, _ = newly_matched([NO_SEAT], *_ctxs("seat_removed", g.MUTATION_VERSION, g.MUTATION_VERSION + 1))
    assert fired == []


def test_change_rule_fires_each_time_condition_is_in_change():
    fired, _ = newly_matched([SCHED], *_ctxs("schedule_change", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-sched"]


def test_new_trip_counts_prev_as_false():
    snap = to_snapshot(g.build_payload(g.SCENARIOS.index("seat_removed"), g.MUTATION_VERSION))
    fired, _ = newly_matched([NO_SEAT], context(None, EMPTY_CHANGE), context(snap, diff(None, snap)))
    assert [r.rule_id for r in fired] == ["r-seat"]


def test_broken_rule_does_not_block_others():
    broken = Rule("r-bad", "bad", {"regex": ["x"]}, {})
    fired, errors = newly_matched([broken, SCHED], *_ctxs("schedule_change", g.MUTATION_VERSION - 1, g.MUTATION_VERSION))
    assert [r.rule_id for r in fired] == ["r-sched"]
    assert errors[0]["rule_id"] == "r-bad"


def test_rule_cache_refreshes_and_survives_loader_errors():
    now = [0.0]
    calls = []

    def loader():
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("db down")
        return [SCHED] if len(calls) == 1 else [SCHED, NO_SEAT]

    cache = RuleCache(loader, ttl_seconds=5, clock=lambda: now[0])
    assert cache.get() == [SCHED]
    now[0] = 6
    assert cache.get() == [SCHED]          # loader failed -> keep last good
    now[0] = 12
    assert cache.get() == [SCHED, NO_SEAT]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_jsonlogic.py tests/test_rules.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `src/trip_intel/jsonlogic.py`**

```python
"""A small, dependency-free JSON-Logic evaluator (https://jsonlogic.com) for trip rules.

Supported: var, missing, ==, !=, ===, !==, <, >, <=, >=, !, !!, and, or, if, in, some, all, none, cat.
Differences from the reference: equality is Python equality (no type coercion) and any
comparison involving None is False instead of raising — rules must never crash the stream.
"""

OPERATORS = frozenset({"var", "missing", "==", "!=", "===", "!==", "<", ">", "<=", ">=", "!", "!!",
                       "and", "or", "if", "in", "some", "all", "none", "cat"})


def truthy(value) -> bool:
    return bool(value)


def _var(data, path, default=None):
    if path in ("", None):
        return data
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return default
        if current is None:
            return default
    return current


def _compare(op, values):
    if any(v is None for v in values):
        return False
    try:
        pairs = list(zip(values, values[1:]))
        if op == "<":
            return all(a < b for a, b in pairs)
        if op == ">":
            return all(a > b for a, b in pairs)
        if op == "<=":
            return all(a <= b for a, b in pairs)
        return all(a >= b for a, b in pairs)
    except TypeError:
        return False


def apply(logic, data):
    if isinstance(logic, list):
        return [apply(item, data) for item in logic]
    if not isinstance(logic, dict) or len(logic) != 1:
        return logic
    op, args = next(iter(logic.items()))
    if op not in OPERATORS:
        raise ValueError(f"unknown operator: {op}")
    if not isinstance(args, list):
        args = [args]

    if op in ("some", "all", "none"):
        items = apply(args[0], data) or []
        results = [truthy(apply(args[1], item)) for item in items]
        if op == "some":
            return any(results)
        if op == "all":
            return bool(results) and all(results)
        return not any(results)
    if op == "and":
        value = True
        for arg in args:
            value = apply(arg, data)
            if not truthy(value):
                return value
        return value
    if op == "or":
        value = False
        for arg in args:
            value = apply(arg, data)
            if truthy(value):
                return value
        return value
    if op == "if":
        for i in range(0, len(args) - 1, 2):
            if truthy(apply(args[i], data)):
                return apply(args[i + 1], data)
        return apply(args[-1], data) if len(args) % 2 else None

    values = [apply(arg, data) for arg in args]
    if op == "var":
        return _var(data, values[0] if values else "", values[1] if len(values) > 1 else None)
    if op == "missing":
        keys = values[0] if len(values) == 1 and isinstance(values[0], list) else values
        return [k for k in keys if _var(data, k) in (None, "")]
    if op in ("==", "==="):
        return values[0] == values[1]
    if op in ("!=", "!=="):
        return values[0] != values[1]
    if op in ("<", ">", "<=", ">="):
        return _compare(op, values)
    if op == "!":
        return not truthy(values[0])
    if op == "!!":
        return truthy(values[0])
    if op == "in":
        needle, haystack = values[0], values[1]
        return haystack is not None and needle in haystack
    return "".join("" if v is None else str(v) for v in values)  # cat


def validate(logic) -> None:
    if isinstance(logic, list):
        for item in logic:
            validate(item)
        return
    if isinstance(logic, dict):
        if len(logic) != 1:
            raise ValueError(f"operator objects must have exactly one key, got {sorted(logic)}")
        op, args = next(iter(logic.items()))
        if op not in OPERATORS:
            raise ValueError(f"unknown operator: {op}")
        validate(args)
```

- [ ] **Step 4: Implement `src/trip_intel/rules.py`**

```python
"""Business rules over trip versions: evaluation context, edge-triggered firing, hot-reload cache.

A rule fires when its JSON Logic is true for the new version and was false for the previous one,
so "segment without a seat" fires once — not on every subsequent GDS re-save.
"""
import time
from dataclasses import dataclass
from typing import Callable

from trip_intel.jsonlogic import apply, truthy


@dataclass(frozen=True)
class Rule:
    rule_id: str
    name: str
    logic: dict
    action: dict


def context(snapshot: dict | None, change: dict) -> dict:
    return {"trip": snapshot or {}, "change": change}


def newly_matched(rules: list[Rule], prev_ctx: dict, new_ctx: dict) -> tuple[list[Rule], list[dict]]:
    fired, errors = [], []
    for rule in rules:
        try:
            now_true = truthy(apply(rule.logic, new_ctx))
            was_true = truthy(apply(rule.logic, prev_ctx)) if prev_ctx["trip"] else False
        except Exception as exc:  # a bad rule must never stop the stream
            errors.append({"rule_id": rule.rule_id, "error": str(exc)})
            continue
        if now_true and not was_true:
            fired.append(rule)
    return fired, errors


class RuleCache:
    def __init__(self, loader: Callable[[], list[Rule]], ttl_seconds: float = 5.0, clock=time.monotonic):
        self._loader, self._ttl, self._clock = loader, ttl_seconds, clock
        self._rules: list[Rule] = []
        self._loaded_at: float | None = None

    def get(self) -> list[Rule]:
        now = self._clock()
        if self._loaded_at is None or now - self._loaded_at >= self._ttl:
            try:
                self._rules = self._loader()
            except Exception:
                pass  # keep serving the last good rule set
            self._loaded_at = now
        return self._rules
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_jsonlogic.py tests/test_rules.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/trip_intel/jsonlogic.py src/trip_intel/rules.py tests/test_jsonlogic.py tests/test_rules.py
git commit -m "feat: JSON-Logic evaluator and edge-triggered rule evaluation with hot-reload cache

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: Outbox actions and idempotent sink SQL

**Files:**
- Create: `src/trip_intel/outbox.py`, `src/trip_intel/sink_sql.py`
- Test: `tests/test_outbox.py`, `tests/test_sink_sql.py`

**Interfaces:**
- Consumes: `Rule`, change dicts, snapshots.
- Produces:
  - `outbox.build_actions(snapshot: dict, change: dict, fired: list[Rule]) -> list[dict]`; each `{"id": sha256 hex, "type": str, "trip_id", "version", "target": str, "payload": dict}`. Types: `NOTIFY_TRAVELER_SCHEDULE_CHANGE` (one per schedule-change pair) and the rule's `action["type"]` (one per fired rule, payload includes `rule_id`, `rule_name`).
  - `outbox.eventbridge_entry(action: dict, bus_name: str) -> dict` (`Source="bcd.trip-intel"`, `DetailType=action["type"]`, `Detail=json`, `EventBusName=bus_name`).
  - `sink_sql.statements(out: dict) -> list[tuple[str, tuple]]` where `out` has keys `trip_id, version, event_ts (datetime), processed_ts (datetime), snapshot_json, change_json, rule_hits_json, actions_json` (all `*_json` are JSON strings; `rule_hits_json` = list of `{"rule_id","name"}`).

- [ ] **Step 1: Write failing tests**

`tests/test_outbox.py`:
```python
import json

from trip_intel import generator as g
from trip_intel.diff import diff
from trip_intel.outbox import build_actions, eventbridge_entry
from trip_intel.payload import to_snapshot
from trip_intel.rules import Rule

RULE = Rule("r-seat", "No seat", {"==": [1, 1]}, {"type": "INVOKE_SERVICE", "target": "seat-assignment-service"})


def _sched():
    trip = g.SCENARIOS.index("schedule_change")
    prev = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION - 1))
    new = to_snapshot(g.build_payload(trip, g.MUTATION_VERSION))
    return new, diff(prev, new)


def test_schedule_change_produces_traveler_notification():
    snap, change = _sched()
    actions = build_actions(snap, change, [])
    assert [a["type"] for a in actions] == ["NOTIFY_TRAVELER_SCHEDULE_CHANGE"]
    p = actions[0]["payload"]
    assert p["record_locator"] == snap["record_locator"]
    assert p["traveler_name"] == snap["traveler"]["name"]
    assert p["old_flight"]["status"] == "HX" and p["new_flight"]["status"] == "HK"
    assert p["delta_minutes"] == 45


def test_rule_action_and_stable_ids():
    snap, change = _sched()
    a1 = build_actions(snap, change, [RULE])
    a2 = build_actions(snap, change, [RULE])
    assert [a["id"] for a in a1] == [a["id"] for a in a2]
    assert len({a["id"] for a in a1}) == 2
    rule_action = [a for a in a1 if a["type"] == "INVOKE_SERVICE"][0]
    assert rule_action["target"] == "seat-assignment-service"
    assert rule_action["payload"]["rule_id"] == "r-seat"


def test_eventbridge_entry_shape():
    snap, change = _sched()
    action = build_actions(snap, change, [])[0]
    entry = eventbridge_entry(action, "bcd-trip-events")
    assert entry["Source"] == "bcd.trip-intel"
    assert entry["DetailType"] == "NOTIFY_TRAVELER_SCHEDULE_CHANGE"
    assert entry["EventBusName"] == "bcd-trip-events"
    assert json.loads(entry["Detail"])["idempotency_key"] == action["id"]
```

`tests/test_sink_sql.py`:
```python
import json
from datetime import datetime, timezone

from trip_intel.sink_sql import statements

TS = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


def _out(tags, rule_hits=(), actions=()):
    return {
        "trip_id": "T1", "version": 6, "event_ts": TS, "processed_ts": TS,
        "snapshot_json": json.dumps({"trip_id": "T1", "record_locator": "R1", "version": 6}),
        "change_json": json.dumps({"from_version": 5, "to_version": 6, "tags": tags, "changes": [], "schedule_changes": []}),
        "rule_hits_json": json.dumps(list(rule_hits)),
        "actions_json": json.dumps(list(actions)),
    }


def test_normal_version_writes_latest_and_event():
    sqls = [s for s, _ in statements(_out(["SEAT_REMOVED"]))]
    assert any(s.startswith("INSERT INTO trip_latest") for s in sqls)
    assert any(s.startswith("INSERT INTO change_events") for s in sqls)


def test_stale_version_skips_latest():
    sqls = [s for s, _ in statements(_out(["STALE_VERSION"]))]
    assert not any("trip_latest" in s for s in sqls)
    assert any(s.startswith("INSERT INTO change_events") for s in sqls)


def test_statements_are_idempotent_on_replay():
    out = _out(["SCHEDULE_CHANGE"], rule_hits=[{"rule_id": "r1", "name": "x"}],
               actions=[{"id": "abc", "type": "T", "trip_id": "T1", "version": 6, "target": "t", "payload": {}}])
    for sql, _ in statements(out):
        assert "ON CONFLICT" in sql, sql


def test_latest_only_moves_forward():
    sql = [s for s, _ in statements(_out(["SEAT_REMOVED"])) if "trip_latest" in s][0]
    assert "WHERE trip_latest.version < EXCLUDED.version" in sql


def test_naive_timestamps_become_utc():
    naive = TS.replace(tzinfo=None)
    out = dict(_out(["SEAT_REMOVED"]), event_ts=naive, processed_ts=naive)
    params = [p for s, p in statements(out) if s.startswith("INSERT INTO change_events")][0]
    assert params[-2].tzinfo is timezone.utc and params[-1].tzinfo is timezone.utc


def test_rule_hits_and_actions_rows():
    out = _out(["SCHEDULE_CHANGE"], rule_hits=[{"rule_id": "r1", "name": "x"}, {"rule_id": "r2", "name": "y"}],
               actions=[{"id": "abc", "type": "T", "trip_id": "T1", "version": 6, "target": "t", "payload": {"a": 1}}])
    stmts = statements(out)
    assert sum(1 for s, _ in stmts if s.startswith("INSERT INTO rule_hits")) == 2
    outbox = [p for s, p in stmts if s.startswith("INSERT INTO action_outbox")]
    assert outbox[0][0] == "abc"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_outbox.py tests/test_sink_sql.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `src/trip_intel/outbox.py`**

```python
"""Actions produced by a trip version: traveler notifications for schedule changes and rule actions.

Action ids are deterministic (type + trip + version + rule/segment), so stream replays never
produce duplicate outbox rows — the id doubles as the EventBridge idempotency key.
"""
import hashlib
import json


def _id(*parts) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()


def _flight(seg: dict) -> dict:
    return {k: seg.get(k) for k in ("carrier", "flight_number", "origin", "destination", "start", "end", "status")}


def build_actions(snapshot: dict, change: dict, fired: list) -> list[dict]:
    trip_id, version = snapshot["trip_id"], snapshot["version"]
    actions = []
    for pair in change.get("schedule_changes", []):
        actions.append({
            "id": _id("NOTIFY_TRAVELER_SCHEDULE_CHANGE", trip_id, version, pair["old"]["segment_id"]),
            "type": "NOTIFY_TRAVELER_SCHEDULE_CHANGE",
            "trip_id": trip_id,
            "version": version,
            "target": "traveler-notifications",
            "payload": {
                "record_locator": snapshot["record_locator"],
                "traveler_name": snapshot["traveler"]["name"],
                "traveler_email": snapshot["traveler"]["email"],
                "old_flight": _flight(pair["old"]),
                "new_flight": _flight(pair["new"]),
                "delta_minutes": pair["delta_minutes"],
            },
        })
    for rule in fired:
        action_type = rule.action.get("type", "RULE_MATCHED")
        actions.append({
            "id": _id(action_type, trip_id, version, rule.rule_id),
            "type": action_type,
            "trip_id": trip_id,
            "version": version,
            "target": rule.action.get("target", "default"),
            "payload": {"rule_id": rule.rule_id, "rule_name": rule.name, "record_locator": snapshot["record_locator"],
                        "tags": change.get("tags", [])},
        })
    return actions


def eventbridge_entry(action: dict, bus_name: str) -> dict:
    detail = dict(action["payload"], trip_id=action["trip_id"], version=action["version"], idempotency_key=action["id"])
    return {"Source": "bcd.trip-intel", "DetailType": action["type"], "Detail": json.dumps(detail),
            "EventBusName": bus_name}
```

- [ ] **Step 4: Implement `src/trip_intel/sink_sql.py`**

```python
"""Translate one processor output row into idempotent Postgres statements for Lakebase.

Every statement is safe to replay (RTM is at-least-once): trip_latest only moves forward,
everything else is keyed and uses ON CONFLICT DO NOTHING. Spark hands timestamps to Python as
naive UTC datetimes; they are made explicit UTC so Postgres never applies its session time zone.
"""
import json
from datetime import timezone


def _utc(ts):
    return ts.replace(tzinfo=timezone.utc) if ts is not None and ts.tzinfo is None else ts


def statements(out: dict) -> list[tuple[str, tuple]]:
    out = dict(out, event_ts=_utc(out["event_ts"]), processed_ts=_utc(out["processed_ts"]))
    change = json.loads(out["change_json"])
    snapshot = json.loads(out["snapshot_json"])
    stmts: list[tuple[str, tuple]] = []
    if "STALE_VERSION" not in change["tags"]:
        stmts.append((
            "INSERT INTO trip_latest (trip_id, version, record_locator, snapshot, updated_ts) "
            "VALUES (%s, %s, %s, %s::jsonb, %s) "
            "ON CONFLICT (trip_id) DO UPDATE SET version = EXCLUDED.version, record_locator = EXCLUDED.record_locator, "
            "snapshot = EXCLUDED.snapshot, updated_ts = EXCLUDED.updated_ts "
            "WHERE trip_latest.version < EXCLUDED.version",
            (out["trip_id"], out["version"], snapshot.get("record_locator"), out["snapshot_json"], out["processed_ts"]),
        ))
    stmts.append((
        "INSERT INTO change_events (trip_id, from_version, to_version, tags, changes, schedule_changes, snapshot, "
        "event_ts, detected_ts) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s) "
        "ON CONFLICT (trip_id, to_version) DO NOTHING",
        (out["trip_id"], change["from_version"], out["version"], change["tags"], json.dumps(change["changes"]),
         json.dumps(change["schedule_changes"]), out["snapshot_json"], out["event_ts"], out["processed_ts"]),
    ))
    for hit in json.loads(out["rule_hits_json"]):
        stmts.append((
            "INSERT INTO rule_hits (rule_id, rule_name, trip_id, version, matched_ts) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (rule_id, trip_id, version) DO NOTHING",
            (hit["rule_id"], hit["name"], out["trip_id"], out["version"], out["processed_ts"]),
        ))
    for action in json.loads(out["actions_json"]):
        stmts.append((
            "INSERT INTO action_outbox (id, type, trip_id, version, target, payload, status, created_ts) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'PENDING', %s) ON CONFLICT (id) DO NOTHING",
            (action["id"], action["type"], action["trip_id"], action["version"], action["target"],
             json.dumps(action["payload"]), out["processed_ts"]),
        ))
    return stmts
```

- [ ] **Step 5: Run the full unit suite**

Run: `.venv/bin/python -m pytest -v`
Expected: all tests in `tests/` PASS.

- [ ] **Step 6: Commit**

```bash
git add src/trip_intel/outbox.py src/trip_intel/sink_sql.py tests/test_outbox.py tests/test_sink_sql.py
git commit -m "feat: deterministic outbox actions, EventBridge entry, idempotent Lakebase statements

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: Lakebase instance, schema, role and secret

Load the `databricks:databricks-lakebase` skill before running commands; if a CLI flag below differs in the installed CLI version, use the skill's form — the verification step is what must pass.

**Files:**
- Create: `sql/lakebase_schema.sql`, `scripts/setup_lakebase.sh`

**Interfaces:**
- Produces: Lakebase instance `bcd-trip-intel`, database `databricks_postgres`, role `trip_intel_app` (password in secret `bcd-trip-intel/lakebase-password`), tables `trip_latest, change_events, rules, rule_hits, action_outbox, traveler_responses`; host string recorded as bundle variable `lakebase_host`.

- [ ] **Step 1: Write `sql/lakebase_schema.sql`**

```sql
-- Lakebase (Postgres) schema for the BCD trip-intelligence demo.
-- Written by the RTM ForeachWriter and the Databricks App; all writes are idempotent.

CREATE TABLE IF NOT EXISTS trip_latest (
  trip_id        text PRIMARY KEY,
  version        int NOT NULL,
  record_locator text,
  snapshot       jsonb NOT NULL,
  updated_ts     timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS change_events (
  id               bigserial PRIMARY KEY,
  trip_id          text NOT NULL,
  from_version     int,
  to_version       int NOT NULL,
  tags             text[] NOT NULL,
  changes          jsonb NOT NULL,
  schedule_changes jsonb NOT NULL,
  snapshot         jsonb NOT NULL,
  event_ts         timestamptz NOT NULL,
  detected_ts      timestamptz NOT NULL,
  written_ts       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (trip_id, to_version)
);
CREATE INDEX IF NOT EXISTS change_events_trip ON change_events (trip_id, to_version);
CREATE INDEX IF NOT EXISTS change_events_written ON change_events (written_ts);

CREATE TABLE IF NOT EXISTS rules (
  rule_id      text PRIMARY KEY,
  name         text NOT NULL,
  nl_text      text,
  json_logic   jsonb NOT NULL,
  action       jsonb NOT NULL,
  status       text NOT NULL CHECK (status IN ('draft', 'active', 'disabled')),
  author       text,
  created_ts   timestamptz NOT NULL DEFAULT now(),
  activated_ts timestamptz
);

CREATE TABLE IF NOT EXISTS rule_hits (
  id         bigserial PRIMARY KEY,
  rule_id    text NOT NULL,
  rule_name  text NOT NULL,
  trip_id    text NOT NULL,
  version    int NOT NULL,
  matched_ts timestamptz NOT NULL,
  UNIQUE (rule_id, trip_id, version)
);

CREATE TABLE IF NOT EXISTS action_outbox (
  id          text PRIMARY KEY,
  type        text NOT NULL,
  trip_id     text NOT NULL,
  version     int NOT NULL,
  target      text NOT NULL,
  payload     jsonb NOT NULL,
  status      text NOT NULL CHECK (status IN ('PENDING', 'SENT', 'SIMULATED', 'FAILED')),
  attempts    int NOT NULL DEFAULT 0,
  last_error  text,
  eventbridge_entry jsonb,
  created_ts  timestamptz NOT NULL,
  sent_ts     timestamptz
);
CREATE INDEX IF NOT EXISTS action_outbox_pending ON action_outbox (status, created_ts);

CREATE TABLE IF NOT EXISTS traveler_responses (
  outbox_id   text PRIMARY KEY REFERENCES action_outbox (id),
  response    text NOT NULL CHECK (response IN ('ACCEPT', 'REBOOK')),
  response_ts timestamptz NOT NULL DEFAULT now()
);

-- Seed rules (active) so the stream has something to evaluate on first start.
INSERT INTO rules (rule_id, name, nl_text, json_logic, action, status, author, activated_ts) VALUES
('seed-schedule-change', 'Notify ops on schedule change',
 'When the airline changes the schedule, alert the operations team.',
 '{"in": ["SCHEDULE_CHANGE", {"var": "change.tags"}]}',
 '{"type": "ALERT_OPS", "target": "ops-queue"}', 'active', 'seed', now()),
('seed-missing-seat', 'Confirmed flight without a seat',
 'If a confirmed flight segment has no seat, ask the seat-assignment service to assign one.',
 '{"some": [{"var": "trip.segments"}, {"and": [{"==": [{"var": "type"}, "Air"]}, {"in": [{"var": "status"}, ["HK", "KK", "TK"]]}, {"missing": "seat"}]}]}',
 '{"type": "INVOKE_SERVICE", "target": "seat-assignment-service"}', 'active', 'seed', now())
ON CONFLICT (rule_id) DO NOTHING;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trip_intel_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trip_intel_app;
```

- [ ] **Step 2: Write `scripts/setup_lakebase.sh`**

```bash
#!/usr/bin/env bash
# One-time Lakebase setup for the demo: instance, native-login role, secret, schema.
set -euo pipefail
P=${P:-fevm-stable-71zsua}
INSTANCE=bcd-trip-intel
SCOPE=bcd-trip-intel

databricks database get-database-instance $INSTANCE --profile $P >/dev/null 2>&1 || \
  databricks database create-database-instance $INSTANCE --capacity CU_1 --enable-pg-native-login --profile $P
echo "waiting for instance..."
until [ "$(databricks database get-database-instance $INSTANCE --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["state"])')" = "AVAILABLE" ]; do sleep 15; done
HOST=$(databricks database get-database-instance $INSTANCE --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["read_write_dns"])')
ME=$(databricks current-user me --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["userName"])')
TOKEN=$(databricks database generate-database-credential --json "{\"instance_names\":[\"$INSTANCE\"],\"request_id\":\"setup\"}" --profile $P -o json | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

APP_PW=$(python3 -c 'import secrets;print(secrets.token_urlsafe(24))')
databricks secrets create-scope $SCOPE --profile $P 2>/dev/null || true
databricks secrets put-secret $SCOPE lakebase-password --string-value "$APP_PW" --profile $P

export PGPASSWORD=$TOKEN
psql "host=$HOST port=5432 dbname=databricks_postgres user=$ME sslmode=require" -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trip_intel_app') THEN
    CREATE ROLE trip_intel_app LOGIN PASSWORD '$APP_PW';
  ELSE
    ALTER ROLE trip_intel_app PASSWORD '$APP_PW';
  END IF;
END \$\$;
SQL
psql "host=$HOST port=5432 dbname=databricks_postgres user=$ME sslmode=require" -v ON_ERROR_STOP=1 -f sql/lakebase_schema.sql
echo "LAKEBASE_HOST=$HOST"
```

- [ ] **Step 3: Run it**

Run: `chmod +x scripts/setup_lakebase.sh && scripts/setup_lakebase.sh`
Expected: final line `LAKEBASE_HOST=instance-....database.cloud.databricks.com`. (Requires `psql`; if missing: `brew install libpq && brew link --force libpq`.)

- [ ] **Step 4: Verify the app role can log in and see seed rules**

```bash
HOST=<from step 3>; PW=$(databricks secrets get-secret bcd-trip-intel lakebase-password --profile fevm-stable-71zsua -o json | python3 -c 'import json,sys,base64;print(base64.b64decode(json.load(sys.stdin)["value"]).decode())')
PGPASSWORD=$PW psql "host=$HOST port=5432 dbname=databricks_postgres user=trip_intel_app sslmode=require" -c "select rule_id,status from rules"
```
Expected: two rows, `seed-schedule-change` and `seed-missing-seat`, both `active`.

- [ ] **Step 5: Add the host to `databricks.yml`**

Under top-level `variables:` add:
```yaml
  lakebase_host:
    description: Lakebase read/write DNS for instance bcd-trip-intel
```
Under `targets.dev.variables` add `lakebase_host: <HOST from step 3>`.

- [ ] **Step 6: Commit**

```bash
git add sql/lakebase_schema.sql scripts/setup_lakebase.sh databricks.yml
git commit -m "feat: Lakebase schema, native-login app role, seed rules

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: RTM job — processor, sink, entry point, bundle resource

**Files:**
- Create: `src/rtm/lakebase.py`, `src/rtm/processor.py`, `src/rtm/sink.py`, `src/rtm/job.py`, `resources/trip_intel_rtm.job.yml`
- Modify: `databricks.yml` (artifacts)

**Interfaces:**
- Consumes: `payload.parse`, `generator.payload_for`, `diff.diff`, `diff.EMPTY_CHANGE`, `rules.Rule/context/newly_matched/RuleCache`, `outbox.build_actions`, `sink_sql.statements`, Lakebase from Task 6.
- Produces: streaming job `trip_intel_rtm` that fills Lakebase tables; job parameters `--source rate|kinesis`, `--rows-per-second`, `--lakebase-host`, `--secret-scope`, `--checkpoint`, `--kinesis-stream`, `--kinesis-region`, `--kinesis-credential`.

- [ ] **Step 1: Implement `src/rtm/lakebase.py`**

```python
"""Lakebase connection info and the active-rule loader used inside the stream."""
import json

import psycopg

from trip_intel.rules import Rule


def conninfo(host: str, password: str, user: str = "trip_intel_app", dbname: str = "databricks_postgres") -> str:
    return psycopg.conninfo.make_conninfo(host=host, port=5432, dbname=dbname, user=user, password=password,
                                          sslmode="require")


def load_active_rules(dsn: str) -> list[Rule]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute("SELECT rule_id, name, json_logic, action FROM rules WHERE status = 'active'").fetchall()
    return [Rule(r[0], r[1], r[2] if isinstance(r[2], dict) else json.loads(r[2]),
                 r[3] if isinstance(r[3], dict) else json.loads(r[3])) for r in rows]
```

- [ ] **Step 2: Implement `src/rtm/processor.py`**

```python
"""transformWithState processor: one trip_id's versions in, one enriched row per version out.

State holds the previous snapshot so every version is diffed against the one before it — the
per-version change tracking that EPAM's latest-only MERGE loses.
"""
import json
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from trip_intel.diff import EMPTY_CHANGE, diff
from trip_intel.outbox import build_actions
from trip_intel.payload import parse
from trip_intel.rules import RuleCache, context, newly_matched

STATE_SCHEMA = StructType([StructField("snapshot_json", StringType())])
OUTPUT_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("version", IntegerType()),
    StructField("event_ts", TimestampType()),
    StructField("processed_ts", TimestampType()),
    StructField("snapshot_json", StringType()),
    StructField("change_json", StringType()),
    StructField("rule_hits_json", StringType()),
    StructField("actions_json", StringType()),
])


class TripProcessor(StatefulProcessor):
    def __init__(self, dsn: str, rule_ttl_seconds: float = 5.0):
        self.dsn = dsn
        self.rule_ttl_seconds = rule_ttl_seconds

    def init(self, handle: StatefulProcessorHandle) -> None:
        from rtm.lakebase import load_active_rules

        self.prev_state = handle.getValueState("prev", STATE_SCHEMA)
        self.rules = RuleCache(lambda: load_active_rules(self.dsn), ttl_seconds=self.rule_ttl_seconds)

    def handleInputRows(self, key, rows, timerValues):
        prev = json.loads(self.prev_state.get()[0]) if self.prev_state.exists() else None
        for row in sorted(rows, key=lambda r: r.version or 0):
            new = parse(row.payload)
            change = diff(prev, new)
            fired, errors = [], []
            if "STALE_VERSION" not in change["tags"]:
                fired, errors = newly_matched(self.rules.get(), context(prev, EMPTY_CHANGE), context(new, change))
                prev = new
                self.prev_state.update((json.dumps(new),))
            for err in errors:
                print(f"RULE_ERROR {err}", flush=True)
            yield Row(
                trip_id=key[0],
                version=new["version"],
                event_ts=row.event_ts,
                processed_ts=datetime.now(timezone.utc),
                snapshot_json=json.dumps(new),
                change_json=json.dumps(change),
                rule_hits_json=json.dumps([{"rule_id": r.rule_id, "name": r.name} for r in fired]),
                actions_json=json.dumps(build_actions(new, change, fired) if fired or change["schedule_changes"] else []),
            )

    def close(self) -> None:
        pass
```

- [ ] **Step 3: Implement `src/rtm/sink.py`**

```python
"""ForeachWriter that applies each processor output row to Lakebase (idempotent statements)."""
import psycopg

from trip_intel.sink_sql import statements


class LakebaseSink:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def open(self, partition_id, epoch_id):
        self.conn = psycopg.connect(self.dsn, autocommit=False)
        return True

    def process(self, row):
        out = row.asDict()
        with self.conn.cursor() as cur:
            for sql, params in statements(out):
                cur.execute(sql, params)
        self.conn.commit()

    def close(self, error):
        if self.conn is not None:
            if error:
                self.conn.rollback()
            self.conn.close()
        if error:
            raise error
```

- [ ] **Step 4: Implement `src/rtm/job.py`**

```python
"""Entry point for the real-time trip-intelligence stream.

--source rate     : demo generator (FEVM), rate source + deterministic BCD-schema payload UDF
--source kinesis  : BCD's real stream (UAT finale), same options as EPAM's kinesis_reader.py
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rtm.lakebase import conninfo
from rtm.processor import OUTPUT_SCHEMA, TripProcessor
from rtm.sink import LakebaseSink
from trip_intel.generator import payload_for


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["rate", "kinesis"], default="rate")
    p.add_argument("--rows-per-second", type=int, default=15)
    p.add_argument("--lakebase-host", required=True)
    p.add_argument("--secret-scope", default="bcd-trip-intel")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--kinesis-stream", default="uat01-HermesTripsDS")
    p.add_argument("--kinesis-region", default="us-east-1")
    p.add_argument("--kinesis-credential", default="kinesis_reader_cred_uat")
    p.add_argument("--trigger", choices=["realtime", "processing"], default="realtime")
    return p.parse_args()


def source_df(spark, args):
    if args.source == "rate":
        gen = F.udf(payload_for, "string")
        return (
            spark.readStream.format("rate").option("rowsPerSecond", args.rows_per_second).option("numPartitions", 1).load()
            .select(gen("value").alias("payload"), F.col("timestamp").alias("event_ts"))
        )
    return (
        spark.readStream.format("kinesis")
        .option("streamName", args.kinesis_stream)
        .option("region", args.kinesis_region)
        .option("serviceCredential", args.kinesis_credential)
        .option("initialPosition", "latest")
        .load()
        .select(F.col("data").cast("string").alias("payload"), F.col("approximateArrivalTimestamp").alias("event_ts"))
    )


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    from databricks.sdk.runtime import dbutils

    dsn = conninfo(args.lakebase_host, dbutils.secrets.get(args.secret_scope, "lakebase-password"))
    versions = source_df(spark, args).select(
        F.get_json_object("payload", "$.identification.id").alias("trip_id"),
        F.get_json_object("payload", "$.identification.version").cast("int").alias("version"),
        "payload",
        "event_ts",
    ).where(F.col("trip_id").isNotNull() & F.col("version").isNotNull())
    enriched = versions.groupBy("trip_id").transformWithState(
        statefulProcessor=TripProcessor(dsn), outputStructType=OUTPUT_SCHEMA, outputMode="Update", timeMode="None"
    )
    writer = enriched.writeStream.foreach(LakebaseSink(dsn)).outputMode("update").option("checkpointLocation", args.checkpoint)
    writer = writer.trigger(realTime="5 minutes") if args.trigger == "realtime" else writer.trigger(processingTime="1 second")
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Add wheel artifact to `databricks.yml`**

After the `include:` block add:
```yaml
artifacts:
  trip_intel:
    type: whl
    path: .
    build: uv build --wheel
```
Check `uv --version`; if not installed: `brew install uv`.

- [ ] **Step 6: Create `resources/trip_intel_rtm.job.yml`**

```yaml
# Real-time lane: Spark Structured Streaming Real-Time Mode on a single-node classic cluster.
# RTM is classic-only today; everything else in this demo is serverless.
resources:
  schemas:
    trip_intel:
      catalog_name: ${var.catalog}
      name: trip_intel
  volumes:
    trip_intel_checkpoints:
      catalog_name: ${var.catalog}
      schema_name: ${resources.schemas.trip_intel.name}
      name: checkpoints
  jobs:
    trip_intel_rtm:
      name: trip_intel_rtm
      max_concurrent_runs: 1
      tasks:
        - task_key: rtm
          job_cluster_key: rtm
          spark_python_task:
            python_file: ../src/rtm/job.py
            parameters:
              - --source
              - rate
              - --rows-per-second
              - "15"
              - --lakebase-host
              - ${var.lakebase_host}
              - --checkpoint
              - /Volumes/${var.catalog}/trip_intel/checkpoints/rtm_v1
          libraries:
            - whl: ../dist/*.whl
      job_clusters:
        - job_cluster_key: rtm
          new_cluster:
            spark_version: 18.x-scala2.13
            node_type_id: m5d.2xlarge
            num_workers: 0
            data_security_mode: SINGLE_USER
            single_user_name: ${workspace.current_user.userName}
            spark_conf:
              spark.master: "local[*, 4]"
              spark.databricks.cluster.profile: singleNode
              spark.databricks.streaming.realTimeMode.enabled: "true"
              spark.sql.shuffle.partitions: "2"
            custom_tags:
              ResourceClass: SingleNode
```

- [ ] **Step 7: Deploy and start**

```bash
databricks bundle validate --profile fevm-stable-71zsua
databricks bundle deploy --profile fevm-stable-71zsua
databricks bundle run trip_intel_rtm --profile fevm-stable-71zsua --no-wait
```
Expected: validate OK; deploy uploads the wheel; run starts (it runs until cancelled).

- [ ] **Step 8: Verify end-to-end after ~3 minutes**

```bash
PGPASSWORD=$PW psql "host=$HOST port=5432 dbname=databricks_postgres user=trip_intel_app sslmode=require" <<'SQL'
select count(*) events, count(*) filter (where 'SCHEDULE_CHANGE' = any(tags)) sched,
       count(*) filter (where tags = '{NO_SIGNIFICANT_CHANGE}') noise from change_events;
select percentile_cont(array[0.5,0.95]) within group (order by extract(epoch from detected_ts - event_ts)*1000) ms
  from change_events where written_ts > now() - interval '2 minutes';
select rule_id, count(*) from rule_hits group by 1;
select type, status, count(*) from action_outbox group by 1,2;
SQL
```
Expected: events grow ~15/s; `sched` > 0; `noise` is the majority; p50 well under 1000 ms (record the numbers for the runbook); hits for both seed rules; outbox rows `PENDING`.

If the run fails with an RTM-specific error, redeploy with `--trigger processing` appended to the task parameters and note it in the spec (per Task 1 fallback).

- [ ] **Step 9: Commit**

```bash
git add src/rtm resources/trip_intel_rtm.job.yml databricks.yml
git commit -m "feat: RTM trip-intelligence stream (transformWithState + Lakebase ForeachWriter)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Outbox dispatcher (EventBridge or SIMULATED)

**Files:**
- Create: `src/dispatcher/dispatch.py`, `resources/trip_intel_dispatcher.job.yml`

**Interfaces:**
- Consumes: `outbox.eventbridge_entry`, `rtm.lakebase.conninfo`, `action_outbox` table.
- Produces: outbox rows moved to `SENT` (real bus) or `SIMULATED` (no bus) with `eventbridge_entry` stored; parameters `--lakebase-host`, `--secret-scope`, `--bus-name` (empty = simulate), `--service-credential`, `--run-minutes`.

- [ ] **Step 1: Implement `src/dispatcher/dispatch.py`**

```python
"""Drain action_outbox to AWS EventBridge (PutEvents) using a UC service credential.

With no --bus-name the dispatcher runs in SIMULATED mode: it builds the exact PutEvents entry,
stores it on the row, and marks it SIMULATED — so the demo shows the payload without AWS rights.
"""
import argparse
import json
import time
from datetime import datetime, timezone

import psycopg

from rtm.lakebase import conninfo
from trip_intel.outbox import eventbridge_entry

CLAIM = (
    "SELECT id, type, trip_id, version, target, payload FROM action_outbox "
    "WHERE status = 'PENDING' ORDER BY created_ts LIMIT 10 FOR UPDATE SKIP LOCKED"
)


def make_client(credential: str, region: str):
    import boto3
    from databricks.sdk.runtime import dbutils

    session = boto3.Session(botocore_session=dbutils.credentials.getServiceCredentialsProvider(credential), region_name=region)
    return session.client("events")


def drain_once(conn, bus_name: str, client) -> int:
    with conn.transaction():
        rows = conn.execute(CLAIM).fetchall()
        for row in rows:
            action = {"id": row[0], "type": row[1], "trip_id": row[2], "version": row[3], "target": row[4], "payload": row[5]}
            entry = eventbridge_entry(action, bus_name or "simulated-bus")
            status, error = "SIMULATED", None
            if client is not None:
                resp = client.put_events(Entries=[entry])
                status = "FAILED" if resp.get("FailedEntryCount") else "SENT"
                error = json.dumps(resp["Entries"][0]) if status == "FAILED" else None
            conn.execute(
                "UPDATE action_outbox SET status = %s, attempts = attempts + 1, last_error = %s, "
                "eventbridge_entry = %s::jsonb, sent_ts = %s WHERE id = %s",
                (status, error, json.dumps(entry), datetime.now(timezone.utc), action["id"]),
            )
    return len(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lakebase-host", required=True)
    p.add_argument("--secret-scope", default="bcd-trip-intel")
    p.add_argument("--bus-name", default="")
    p.add_argument("--service-credential", default="")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--run-minutes", type=float, default=55)
    args = p.parse_args()
    from databricks.sdk.runtime import dbutils

    dsn = conninfo(args.lakebase_host, dbutils.secrets.get(args.secret_scope, "lakebase-password"))
    client = make_client(args.service_credential, args.region) if args.bus_name else None
    deadline = time.monotonic() + args.run_minutes * 60
    with psycopg.connect(dsn) as conn:
        while time.monotonic() < deadline:
            if drain_once(conn, args.bus_name, client) == 0:
                time.sleep(0.5)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create `resources/trip_intel_dispatcher.job.yml`**

```yaml
# Outbox dispatcher: serverless, restarted hourly; SIMULATED mode when no EventBridge bus is configured.
resources:
  jobs:
    trip_intel_dispatcher:
      name: trip_intel_dispatcher
      max_concurrent_runs: 1
      trigger:
        periodic:
          interval: 1
          unit: HOURS
      tasks:
        - task_key: dispatch
          environment_key: default
          spark_python_task:
            python_file: ../src/dispatcher/dispatch.py
            parameters: ["--lakebase-host", "${var.lakebase_host}", "--run-minutes", "55"]
      environments:
        - environment_key: default
          spec:
            environment_version: "4"
            dependencies:
              - ../dist/*.whl
              - boto3
```

- [ ] **Step 3: Deploy, run, verify**

```bash
databricks bundle deploy --profile fevm-stable-71zsua
databricks bundle run trip_intel_dispatcher --profile fevm-stable-71zsua --no-wait
sleep 120
PGPASSWORD=$PW psql "host=$HOST port=5432 dbname=databricks_postgres user=trip_intel_app sslmode=require" \
  -c "select status, count(*) from action_outbox group by 1" \
  -c "select eventbridge_entry from action_outbox where status='SIMULATED' limit 1"
```
Expected: `SIMULATED` count rising, `PENDING` near zero; entry JSON has `Source: bcd.trip-intel` and `Detail` with `idempotency_key`.

- [ ] **Step 4: Commit**

```bash
git add src/dispatcher resources/trip_intel_dispatcher.job.yml
git commit -m "feat: outbox dispatcher to EventBridge with SIMULATED mode

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Analytics lane, Genie space, cost numbers

**Files:**
- Create: `src/analytics/snapshot.sql`, `resources/trip_intel_analytics.job.yml`

**Interfaces:**
- Consumes: Lakebase instance `bcd-trip-intel`.
- Produces: UC catalog `bcd_trip_intel_pg` (Lakebase database catalog), Delta tables `${var.catalog}.trip_intel.change_events_hourly`, `trip_intel.rule_hits_hourly`, `trip_intel.trip_latest_hourly`; Genie space "BCD Trip Changes"; cost table for the runbook.

- [ ] **Step 1: Register Lakebase as a UC catalog**

```bash
databricks database create-database-catalog bcd_trip_intel_pg bcd-trip-intel databricks_postgres --profile fevm-stable-71zsua
```
Verify: `databricks experimental aitools tools query "select count(*) from bcd_trip_intel_pg.public.change_events" --profile fevm-stable-71zsua` returns a count matching psql (within a few seconds of drift).

- [ ] **Step 2: Write `src/analytics/snapshot.sql`**

```sql
-- Hourly analytics lane: snapshot operational Lakebase tables into Delta for Genie / BI.
-- Same data the real-time lane wrote seconds ago — consumed here on an hourly, serverless schedule.
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.trip_intel.change_events_hourly') AS
SELECT trip_id, from_version, to_version, tags, changes::string AS changes, schedule_changes::string AS schedule_changes,
       event_ts, detected_ts, written_ts
FROM bcd_trip_intel_pg.public.change_events;

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.trip_intel.rule_hits_hourly') AS
SELECT * FROM bcd_trip_intel_pg.public.rule_hits;

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.trip_intel.trip_latest_hourly') AS
SELECT trip_id, version, record_locator, snapshot::string AS snapshot, updated_ts
FROM bcd_trip_intel_pg.public.trip_latest;
```

- [ ] **Step 3: Create `resources/trip_intel_analytics.job.yml`**

```yaml
# Batch/analytics lane: serverless SQL, hourly — the "Ajay's team" consumer of the same data.
resources:
  jobs:
    trip_intel_analytics:
      name: trip_intel_analytics
      trigger:
        periodic:
          interval: 1
          unit: HOURS
      parameters:
        - name: catalog
          default: ${var.catalog}
      tasks:
        - task_key: snapshot
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../src/analytics/snapshot.sql
```
Add variable `warehouse_id` to `databricks.yml` (description "Serverless SQL warehouse for the analytics lane"); set it in `targets.dev.variables` from `databricks warehouses list --profile fevm-stable-71zsua` (pick the serverless one).

- [ ] **Step 4: Deploy, run once, verify**

```bash
databricks bundle deploy --profile fevm-stable-71zsua
databricks bundle run trip_intel_analytics --profile fevm-stable-71zsua
databricks experimental aitools tools query "select count(*), max(written_ts) from serverless_stable_71zsua_catalog.trip_intel.change_events_hourly" --profile fevm-stable-71zsua
```
Expected: SUCCESS; count > 0; `max(written_ts)` lags the live Lakebase table — this lag is the freshness contrast shown in the Ops Console.

- [ ] **Step 5: Create the Genie space**

Load the `databricks:databricks-genie-agents` skill and create a space "BCD Trip Changes" over the three `*_hourly` tables with instructions: "tags is an array of change tags; SCHEDULE_CHANGE means airline-initiated re-protection; NO_SIGNIFICANT_CHANGE versions are GDS re-saves with no business impact." Add sample questions: "How many versions were significant vs noise today?", "Which carriers had the most schedule changes?", "Which rules fired most in the last hour?". Verify one sample question returns a non-empty result. Record the space URL in `docs/DEMO_RUNBOOK.md` (Task 11).

- [ ] **Step 6: Pull cost numbers (after the RTM job has run ≥ 1 hour)**

```bash
databricks experimental aitools tools query "
select u.usage_metadata.job_id job_id, j.name, u.sku_name,
       round(sum(u.usage_quantity),3) dbus, round(sum(u.usage_quantity*p.pricing.default),2) list_usd,
       round(sum(u.usage_quantity)/ (timestampdiff(MINUTE, min(u.usage_start_time), max(u.usage_end_time))/60.0),3) dbu_per_hr
from system.billing.usage u
join system.billing.list_prices p on u.sku_name=p.sku_name and u.usage_start_time>=p.price_start_time and (p.price_end_time is null or u.usage_start_time<p.price_end_time)
left join system.lakeflow.jobs j on j.job_id=u.usage_metadata.job_id
where u.usage_date >= current_date() - 1 and j.name in ('[dev rohit_bhagwat] trip_intel_rtm','[dev rohit_bhagwat] trip_intel_analytics','[dev rohit_bhagwat] trip_intel_dispatcher')
group by all" --profile fevm-stable-71zsua
```
Record DBU/hr and $/hr per job in the runbook cost table (add EC2 for the classic node from the AWS on-demand price of `m5d.2xlarge` in us-east-1). Note: `system.billing` lags up to a few hours — if empty, re-run Thursday morning.

- [ ] **Step 7: Commit**

```bash
git add src/analytics resources/trip_intel_analytics.job.yml databricks.yml
git commit -m "feat: hourly serverless analytics lane over Lakebase catalog

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 10: App backend (FastAPI) and rule drafting

Load `databricks:databricks-apps` and `databricks:databricks-apps-python` skills first; follow them where they differ on app config details.

**Files:**
- Create: `src/app/__init__.py` (empty), `src/app/serve.py`, `src/app/db.py`, `src/app/rule_drafting.py`, `src/app/main.py`, `src/app.yaml`, `src/requirements.txt`, `resources/trip_intel.app.yml`
- Test: `tests/test_rule_drafting.py`

**Interfaces:**
- Consumes: Lakebase tables; `trip_intel.jsonlogic.validate/apply/truthy`, `trip_intel.rules.context`, `trip_intel.diff.EMPTY_CHANGE`.
- Produces (HTTP, all JSON):
  - `GET /api/stream` — SSE; each event `data:` = change_event row `{id, trip_id, from_version, to_version, tags, changes, schedule_changes, event_ts, detected_ts, written_ts, rule_hits:[...]}`; significant only (excludes `{NO_SIGNIFICANT_CHANGE}` and `{STALE_VERSION}`).
  - `GET /api/kpis` → `{"versions_5m", "significant_5m", "p50_ms", "p95_ms", "rtm_freshness_s", "batch_freshness_s"}`.
  - `GET /api/trips/{trip_id}` → `{"latest": snapshot, "versions": [change_event rows asc]}`.
  - `GET /api/rules`, `POST /api/rules/draft {"text"}` → `{"name","json_logic","explanation"}`, `POST /api/rules/preview {"json_logic"}` → `{"evaluated","matched","samples":[{trip_id,to_version,tags}]}`, `POST /api/rules {"name","nl_text","json_logic","action"}` → created rule (status active), `POST /api/rules/{rule_id}/disable`, `GET /api/rules/{rule_id}/hits`.
  - `GET /api/traveler/notifications?limit=20`, `POST /api/traveler/notifications/{outbox_id}/respond {"response": "ACCEPT"|"REBOOK"}`.
  - `rule_drafting.SYSTEM_PROMPT: str`, `rule_drafting.parse_draft(text: str) -> dict` (raises `ValueError`).

- [ ] **Step 1: Write failing test `tests/test_rule_drafting.py`**

```python
import pytest

from app.rule_drafting import parse_draft


def test_parses_fenced_json():
    text = 'Here you go:\n```json\n{"name": "No seat", "json_logic": {"missing": "seat"}, "explanation": "x"}\n```'
    d = parse_draft(text)
    assert d["name"] == "No seat" and d["json_logic"] == {"missing": "seat"}


def test_rejects_unknown_operator():
    with pytest.raises(ValueError, match="unknown operator"):
        parse_draft('{"name": "a", "json_logic": {"regex": ["a"]}, "explanation": "b"}')


def test_rejects_missing_keys():
    with pytest.raises(ValueError, match="missing"):
        parse_draft('{"name": "a"}')


def test_rejects_non_json():
    with pytest.raises(ValueError):
        parse_draft("I cannot do that")
```

Run: `.venv/bin/python -m pytest tests/test_rule_drafting.py -v` → FAIL (`ModuleNotFoundError: No module named 'app'`).

- [ ] **Step 2: Implement `src/app/rule_drafting.py`**

```python
"""Plain English -> JSON Logic rule drafting via a Foundation Model API chat endpoint."""
import json
import re

from trip_intel.jsonlogic import OPERATORS, validate

SYSTEM_PROMPT = f"""You convert a travel-operations business rule written in plain English into JSON Logic.

The rule is evaluated against this context for every new version of a trip (PNR):
{{
  "trip": {{
    "trip_id": str, "record_locator": str, "version": int,
    "segments": [{{"type": "Air"|"Hotel", "segment_id": str, "status": "HK"|"KK"|"TK"|"HX"|"UN",
                   "carrier": str, "flight_number": str, "class_of_service": str, "origin": str (IATA),
                   "destination": str (IATA), "start": ISO-8601 str, "end": ISO-8601 str, "seat": str|null,
                   "hotel_name": str|null}}],
    "traveler": {{"name": str, "email": str, "phone": str}},
    "payment": {{"type": str, "last4": str}}
  }},
  "change": {{"tags": [str], "changes": [{{"segment": str, "field": str, "old": any, "new": any}}],
             "schedule_changes": [{{"old": segment, "new": segment, "delta_minutes": int}}]}}
}}
Change tags: NEW_TRIP, SEGMENT_ADDED, SEGMENT_REMOVED, SEGMENT_MODIFIED, SEAT_REMOVED, SEAT_CHANGED,
CONTACT_CHANGED, PAYMENT_CHANGED, SCHEDULE_CHANGE, NO_SIGNIFICANT_CHANGE.
Status codes: HK/KK/TK = confirmed, HX/UN = cancelled/unable.
Inside "some"/"all"/"none", "var" paths are relative to the array item.
Allowed operators ONLY: {", ".join(sorted(OPERATORS))}.

Examples:
- "Alert when the airline changes the schedule" ->
  {{"in": ["SCHEDULE_CHANGE", {{"var": "change.tags"}}]}}
- "A confirmed flight has no seat" ->
  {{"some": [{{"var": "trip.segments"}}, {{"and": [{{"==": [{{"var": "type"}}, "Air"]}},
    {{"in": [{{"var": "status"}}, ["HK", "KK", "TK"]]}}, {{"missing": "seat"}}]}}]}}
- "Schedule moved by more than 30 minutes" ->
  {{"some": [{{"var": "change.schedule_changes"}}, {{"or": [{{">": [{{"var": "delta_minutes"}}, 30]}},
    {{"<": [{{"var": "delta_minutes"}}, -30]}}]}}]}}

Respond with ONLY a JSON object: {{"name": short title, "json_logic": object, "explanation": one sentence in plain English}}.
"""


def parse_draft(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("model did not return a JSON object")
    try:
        draft = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON from model: {exc}") from exc
    missing = [k for k in ("name", "json_logic", "explanation") if k not in draft]
    if missing:
        raise ValueError(f"missing keys: {missing}")
    validate(draft["json_logic"])
    return {"name": draft["name"], "json_logic": draft["json_logic"], "explanation": draft["explanation"]}
```

Run: `.venv/bin/python -m pytest tests/test_rule_drafting.py -v` → PASS.

- [ ] **Step 3: Implement `src/app/db.py` and `src/app/serve.py`**

`src/app/db.py`:
```python
"""Lakebase connection pool for the app (native password role, same as the stream sink)."""
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
    conninfo=(f"host={os.environ['LAKEBASE_HOST']} port=5432 dbname=databricks_postgres "
              f"user=trip_intel_app password={os.environ['LAKEBASE_PASSWORD']} sslmode=require"),
    kwargs={"row_factory": dict_row, "autocommit": True},
    min_size=1,
    max_size=8,
    open=False,
)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params: tuple = ()) -> None:
    with pool.connection() as conn:
        conn.execute(sql, params)
```

`src/app/serve.py`:
```python
"""Launch uvicorn on the port Databricks Apps assigns."""
import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
```

- [ ] **Step 4: Implement `src/app/main.py`**

```python
"""BCD Trip Intelligence app: Ops Console, Rule Studio, Traveler view over Lakebase."""
import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import db
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
    row["batch_freshness_s"] = _batch_freshness()
    return row


def _batch_freshness():
    try:
        w = WorkspaceClient()
        res = w.statement_execution.execute_statement(
            warehouse_id=os.environ["WAREHOUSE_ID"], wait_timeout="10s",
            statement=f"SELECT timestampdiff(SECOND, max(written_ts), current_timestamp()) "
                      f"FROM {os.environ['CATALOG']}.trip_intel.change_events_hourly")
        return float(res.result.data_array[0][0])
    except Exception:
        return None


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
```

- [ ] **Step 5: App config files**

`src/requirements.txt`:
```
fastapi>=0.115
uvicorn>=0.30
psycopg[binary]>=3.2
psycopg-pool>=3.2
databricks-sdk>=0.40
```

`src/app.yaml`:
```yaml
command: ["python", "-m", "app.serve"]
env:
  - name: LAKEBASE_HOST
    value: "<LAKEBASE_HOST from Task 6>"
  - name: LAKEBASE_PASSWORD
    valueFrom: lakebase-password
  - name: LLM_ENDPOINT
    valueFrom: llm
  - name: WAREHOUSE_ID
    valueFrom: warehouse
  - name: CATALOG
    value: serverless_stable_71zsua_catalog
```

`resources/trip_intel.app.yml`:
```yaml
# The demo app: Ops Console, Rule Studio, Traveler view. App root is src/ so it can import trip_intel.
resources:
  apps:
    trip_intel_app:
      name: bcd-trip-intel
      description: BCD real-time trip intelligence demo
      source_code_path: ../src
      resources:
        - name: lakebase-password
          secret:
            scope: bcd-trip-intel
            key: lakebase-password
            permission: READ
        - name: llm
          serving_endpoint:
            name: databricks-claude-sonnet-5-5
            permission: CAN_QUERY
        - name: warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
```
After first deploy, grant the app's service principal `USE CATALOG`/`USE SCHEMA`/`SELECT` on `serverless_stable_71zsua_catalog.trip_intel` (needed for `batch_freshness_s`): `databricks apps get bcd-trip-intel --profile fevm-stable-71zsua` → `service_principal_client_id`, then run the GRANTs via `databricks experimental aitools tools query`.

- [ ] **Step 6: Local smoke against real Lakebase**

```bash
cd src && LAKEBASE_HOST=$HOST LAKEBASE_PASSWORD=$PW DATABRICKS_CONFIG_PROFILE=fevm-stable-71zsua ../.venv/bin/python -m app.serve &
sleep 3
curl -s localhost:8000/api/kpis; echo
curl -s localhost:8000/api/rules | python3 -m json.tool | head -20
curl -s -X POST localhost:8000/api/rules/preview -H 'content-type: application/json' -d '{"json_logic":{"in":["SCHEDULE_CHANGE",{"var":"change.tags"}]}}'; echo
curl -s -X POST localhost:8000/api/rules/draft -H 'content-type: application/json' -d '{"text":"If a traveler changes their email address, notify the profile sync service"}'; echo
curl -s -N --max-time 3 localhost:8000/api/stream | head -5
kill %1; cd ..
```
(Install local deps first: `.venv/bin/pip install -r src/requirements.txt`.)
Expected: KPIs with p50_ms numeric; two seed rules; preview `matched` > 0; draft returns JSON Logic containing `CONTACT_CHANGED`; stream prints `data: {...}` lines.

- [ ] **Step 7: Commit**

```bash
git add src/app src/app.yaml src/requirements.txt resources/trip_intel.app.yml tests/test_rule_drafting.py
git commit -m "feat: trip-intel app backend (SSE feed, KPIs, rule studio API, traveler API)

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 11: App frontend (three views), deploy, browser check, runbook

Load the `dubois-design-language` skill for styling conventions; keep it vanilla JS (no build step).

**Files:**
- Create: `src/app/static/index.html`, `src/app/static/app.js`, `src/app/static/style.css`, `docs/DEMO_RUNBOOK.md`

**Interfaces:**
- Consumes: all `/api/*` endpoints from Task 10.

- [ ] **Step 1: `src/app/static/index.html`**

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>BCD Trip Intelligence</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <header>
    <h1>BCD Trip Intelligence <span class="sub">real-time on Databricks</span></h1>
    <nav>
      <button data-view="ops" class="active">Live Ops Console</button>
      <button data-view="rules">Rule Studio</button>
      <button data-view="traveler">Traveler</button>
    </nav>
  </header>

  <main>
    <section id="ops" class="view active">
      <div class="kpis">
        <div class="kpi"><label>Versions (5 min)</label><span id="k-versions">–</span></div>
        <div class="kpi"><label>Significant</label><span id="k-significant">–</span></div>
        <div class="kpi"><label>Event → detected p50</label><span id="k-p50">–</span></div>
        <div class="kpi"><label>p95</label><span id="k-p95">–</span></div>
        <div class="kpi"><label>Real-time lane freshness</label><span id="k-rtm">–</span></div>
        <div class="kpi"><label>Hourly batch lane freshness</label><span id="k-batch">–</span></div>
      </div>
      <div class="split">
        <div class="panel"><h2>Significant changes (live)</h2><ul id="feed"></ul></div>
        <div class="panel"><h2 id="trip-title">Select a trip</h2><div id="timeline"></div></div>
      </div>
    </section>

    <section id="rules" class="view">
      <div class="split">
        <div class="panel">
          <h2>New rule</h2>
          <textarea id="nl" rows="3" placeholder="e.g. If a confirmed flight moves by more than 30 minutes, alert the ops team"></textarea>
          <button id="draft">Draft with AI</button>
          <p id="explanation" class="muted"></p>
          <input id="rule-name" placeholder="Rule name" />
          <textarea id="logic" rows="10" spellcheck="false" placeholder="JSON Logic"></textarea>
          <input id="action-target" placeholder="Action target (e.g. seat-assignment-service)" value="ops-queue" />
          <div class="row"><button id="preview">Preview on recent versions</button><button id="activate" class="primary">Approve &amp; activate</button></div>
          <p id="preview-result"></p>
        </div>
        <div class="panel"><h2>Rules</h2><table id="rules-table"><thead><tr><th>Name</th><th>Status</th><th>Hits</th><th></th></tr></thead><tbody></tbody></table></div>
      </div>
    </section>

    <section id="traveler" class="view">
      <div class="phone"><div id="cards"></div></div>
    </section>
  </main>
  <script src="/static/app.js"></script>
</body>
</html>
```

- [ ] **Step 2: `src/app/static/app.js`**

```javascript
const $ = (sel) => document.querySelector(sel);
const api = async (path, opts = {}) => {
  const res = await fetch(path, { headers: { "content-type": "application/json" }, ...opts });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json();
};
const fmtMs = (v) => (v == null ? "–" : `${Math.round(v)} ms`);
const fmtAge = (s) => (s == null ? "–" : s < 120 ? `${Math.round(s)} s ago` : `${Math.round(s / 60)} min ago`);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// ---- navigation
document.querySelectorAll("nav button").forEach((b) =>
  b.addEventListener("click", () => {
    document.querySelectorAll("nav button, .view").forEach((el) => el.classList.remove("active"));
    b.classList.add("active");
    $(`#${b.dataset.view}`).classList.add("active");
    if (b.dataset.view === "rules") loadRules();
    if (b.dataset.view === "traveler") loadCards();
  })
);

// ---- ops console
async function refreshKpis() {
  try {
    const k = await api("/api/kpis");
    $("#k-versions").textContent = k.versions_5m;
    $("#k-significant").textContent = `${k.significant_5m} (${k.versions_5m ? Math.round((100 * k.significant_5m) / k.versions_5m) : 0}%)`;
    $("#k-p50").textContent = fmtMs(k.p50_ms);
    $("#k-p95").textContent = fmtMs(k.p95_ms);
    $("#k-rtm").textContent = fmtAge(k.rtm_freshness_s);
    $("#k-batch").textContent = fmtAge(k.batch_freshness_s);
  } catch (e) { console.error(e); }
}
setInterval(refreshKpis, 2000);
refreshKpis();

function addFeedItem(ev) {
  const li = document.createElement("li");
  const shownMs = Date.now() - new Date(ev.event_ts).getTime();
  const tags = ev.tags.map((t) => `<span class="tag ${t}">${t}</span>`).join("");
  const hits = ev.rule_hits.map((h) => `<span class="hit">⚑ ${esc(h.name)}</span>`).join("");
  li.innerHTML = `<b>${esc(ev.trip_id)}</b> v${ev.from_version ?? "–"}→v${ev.to_version} ${tags} ${hits}
    <span class="muted">${shownMs} ms event→screen</span>`;
  li.addEventListener("click", () => loadTrip(ev.trip_id));
  $("#feed").prepend(li);
  while ($("#feed").children.length > 60) $("#feed").lastChild.remove();
}
new EventSource("/api/stream").onmessage = (m) => addFeedItem(JSON.parse(m.data));

async function loadTrip(tripId) {
  const t = await api(`/api/trips/${encodeURIComponent(tripId)}`);
  $("#trip-title").textContent = `${tripId} · ${t.latest.record_locator} · ${t.latest.traveler.name ?? ""}`;
  $("#timeline").innerHTML = t.versions.map((v) => `
    <div class="ver ${v.tags.includes("NO_SIGNIFICANT_CHANGE") ? "noise" : ""}">
      <div><b>v${v.to_version}</b> ${v.tags.map((x) => `<span class="tag ${x}">${x}</span>`).join("")}
        ${v.rule_hits.map((h) => `<span class="hit">⚑ ${esc(h.name)}</span>`).join("")}</div>
      ${v.changes.map((c) => `<div class="chg">${esc(c.segment ?? "")} <code>${esc(c.field)}</code>: ${esc(c.old)} → ${esc(c.new)}</div>`).join("")}
    </div>`).join("");
}

// ---- rule studio
async function loadRules() {
  const rules = await api("/api/rules");
  $("#rules-table tbody").innerHTML = rules.map((r) => `
    <tr><td title="${esc(r.nl_text)}">${esc(r.name)}</td><td>${r.status}</td><td>${r.hits}</td>
    <td>${r.status === "active" ? `<button data-disable="${r.rule_id}">Disable</button>` : ""}</td></tr>`).join("");
  document.querySelectorAll("[data-disable]").forEach((b) =>
    b.addEventListener("click", async () => { await api(`/api/rules/${b.dataset.disable}/disable`, { method: "POST" }); loadRules(); }));
}
setInterval(() => { if ($("#rules").classList.contains("active")) loadRules(); }, 3000);

$("#draft").addEventListener("click", async () => {
  $("#explanation").textContent = "Drafting…";
  try {
    const d = await api("/api/rules/draft", { method: "POST", body: JSON.stringify({ text: $("#nl").value }) });
    $("#rule-name").value = d.name;
    $("#logic").value = JSON.stringify(d.json_logic, null, 2);
    $("#explanation").textContent = d.explanation;
  } catch (e) { $("#explanation").textContent = `Could not draft: ${e.message}`; }
});

const currentLogic = () => JSON.parse($("#logic").value);
$("#preview").addEventListener("click", async () => {
  try {
    const p = await api("/api/rules/preview", { method: "POST", body: JSON.stringify({ json_logic: currentLogic() }) });
    $("#preview-result").innerHTML = `Matched <b>${p.matched}</b> of ${p.evaluated} recent versions. ` +
      p.samples.map((s) => `<span class="tag">${esc(s.trip_id)} v${s.to_version}</span>`).join(" ");
  } catch (e) { $("#preview-result").textContent = `Invalid rule: ${e.message}`; }
});
$("#activate").addEventListener("click", async () => {
  try {
    await api("/api/rules", { method: "POST", body: JSON.stringify({
      name: $("#rule-name").value, nl_text: $("#nl").value, json_logic: currentLogic(),
      action: { type: "RULE_MATCHED", target: $("#action-target").value } }) });
    $("#preview-result").textContent = "Active — the stream picks it up within ~5 seconds. Watch the Hits column.";
    loadRules();
  } catch (e) { $("#preview-result").textContent = `Could not activate: ${e.message}`; }
});

// ---- traveler view
async function loadCards() {
  const items = await api("/api/traveler/notifications?limit=10");
  $("#cards").innerHTML = items.map((n) => {
    const p = n.payload, o = p.old_flight, f = p.new_flight;
    const t = (iso) => new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", timeZone: "UTC" });
    return `<div class="card">
      <div class="muted">Booking ${esc(p.record_locator)} · ${esc(p.traveler_name)}</div>
      <h3>Your flight ${esc(o.carrier)}${esc(o.flight_number)} ${esc(o.origin)} → ${esc(o.destination)} has changed</h3>
      <p><s>${t(o.start)}</s> → <b>${t(f.start)}</b> on ${esc(f.carrier)}${esc(f.flight_number)} (${p.delta_minutes > 0 ? "+" : ""}${p.delta_minutes} min)</p>
      ${n.response ? `<p class="done">You chose: ${n.response}</p>` :
        `<div class="row"><button class="primary" data-r="ACCEPT" data-id="${n.id}">Accept</button><button data-r="REBOOK" data-id="${n.id}">Rebook</button></div>`}
    </div>`;
  }).join("") || "<p class='muted'>No schedule changes yet.</p>";
  document.querySelectorAll("[data-r]").forEach((b) => b.addEventListener("click", async () => {
    await api(`/api/traveler/notifications/${b.dataset.id}/respond`, { method: "POST", body: JSON.stringify({ response: b.dataset.r }) });
    loadCards();
  }));
}
setInterval(() => { if ($("#traveler").classList.contains("active")) loadCards(); }, 3000);
```

- [ ] **Step 3: `src/app/static/style.css`**

```css
:root { --blue: #2272b4; --ink: #11171c; --muted: #5f7281; --line: #e8ecf0; --bg: #f6f7f9; --red: #c82d4c; --green: #277c43; --amber: #be501e; }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.45 -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--ink); background: var(--bg); }
header { display: flex; align-items: center; justify-content: space-between; padding: 12px 24px; background: #fff; border-bottom: 1px solid var(--line); }
h1 { font-size: 18px; margin: 0; } .sub { color: var(--muted); font-weight: 400; font-size: 14px; }
nav button { border: 0; background: none; padding: 8px 14px; font-weight: 600; color: var(--muted); cursor: pointer; border-bottom: 2px solid transparent; }
nav button.active { color: var(--blue); border-bottom-color: var(--blue); }
main { padding: 20px 24px; } .view { display: none; } .view.active { display: block; }
.kpis { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 16px; }
.kpi { background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 12px; }
.kpi label { display: block; color: var(--muted); font-size: 12px; } .kpi span { font-size: 22px; font-weight: 600; }
.split { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.panel { background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 16px; max-height: 70vh; overflow: auto; }
h2 { font-size: 15px; margin: 0 0 12px; }
#feed { list-style: none; margin: 0; padding: 0; } #feed li { padding: 8px 4px; border-bottom: 1px solid var(--line); cursor: pointer; }
#feed li:hover { background: var(--bg); }
.tag { display: inline-block; font-size: 11px; padding: 1px 6px; border-radius: 4px; background: #eef3f8; color: var(--blue); margin-right: 4px; }
.tag.SCHEDULE_CHANGE { background: #fde8ec; color: var(--red); } .tag.SEAT_REMOVED { background: #fdf0e8; color: var(--amber); }
.tag.NO_SIGNIFICANT_CHANGE { background: #f1f1f1; color: var(--muted); }
.hit { font-size: 11px; color: var(--green); font-weight: 600; margin-right: 6px; }
.muted { color: var(--muted); font-size: 12px; }
.ver { padding: 8px 0; border-bottom: 1px dashed var(--line); } .ver.noise { opacity: .45; } .chg { font-size: 12px; margin-left: 12px; }
textarea, input { width: 100%; margin: 6px 0; padding: 8px; border: 1px solid var(--line); border-radius: 6px; font: inherit; }
#logic { font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
button { padding: 7px 14px; border: 1px solid var(--line); border-radius: 6px; background: #fff; cursor: pointer; font-weight: 600; }
button.primary { background: var(--blue); color: #fff; border-color: var(--blue); }
.row { display: flex; gap: 8px; } table { width: 100%; border-collapse: collapse; } td, th { text-align: left; padding: 6px; border-bottom: 1px solid var(--line); }
.phone { width: 380px; margin: 0 auto; background: #fff; border: 10px solid var(--ink); border-radius: 32px; padding: 20px 16px; min-height: 640px; }
.card { border: 1px solid var(--line); border-radius: 12px; padding: 14px; margin-bottom: 12px; } .card h3 { font-size: 15px; margin: 4px 0; }
.done { color: var(--green); font-weight: 600; }
```

- [ ] **Step 4: Deploy the app**

Set `LAKEBASE_HOST` in `src/app.yaml` to the Task 6 host, then:
```bash
databricks bundle deploy --profile fevm-stable-71zsua
databricks bundle run trip_intel_app --profile fevm-stable-71zsua
databricks apps get bcd-trip-intel --profile fevm-stable-71zsua -o json | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["url"], d["app_status"]["state"], d["compute_status"]["state"])'
```
Expected: URL printed, state `RUNNING`/`ACTIVE`. Then run the SP grants from Task 10 Step 5.

- [ ] **Step 5: Browser verification**

Use the `fe-specialized-agents:web-devloop-tester` agent (or Chrome DevTools MCP) against the app URL and check, with screenshots:
1. Ops Console: KPIs populate within 5 s; feed shows new rows continuously with `SCHEDULE_CHANGE` / `SEAT_REMOVED` tags; clicking a row renders a timeline with greyed noise versions.
2. Rule Studio: type "If a traveler's email changes, notify the profile sync service" → Draft fills JSON Logic referencing `CONTACT_CHANGED`; Preview shows matched > 0; Approve → rule appears with Hits climbing within ~10 s.
3. Traveler: at least one schedule-change card; Accept → card shows "You chose: ACCEPT".
4. Browser console has no errors.

Fix any defects found, redeploy, re-check.

- [ ] **Step 6: Write `docs/DEMO_RUNBOOK.md`**

Content (fill bracketed values from Tasks 7–9 measurements):
```markdown
# BCD Real-Time Demos — Runbook (Thu 2026-10-08, 2–3pm ET)

## T-60 min
- Start stream: `databricks bundle run trip_intel_rtm --profile fevm-stable-71zsua --no-wait` (cluster ~5 min).
- Start dispatcher: `databricks bundle run trip_intel_dispatcher --profile fevm-stable-71zsua --no-wait`.
- Run analytics once: `databricks bundle run trip_intel_analytics --profile fevm-stable-71zsua`.
- Open app URL [<url>], Genie space [<url>], this repo's spec.
- Disable demo-created rules from rehearsal (Rule Studio → Disable).

## Arc (55 min)
1. **Real-time vs batch (10 min)** — Ops Console KPIs: p50 [<N> ms] event→detected vs hourly lane freshness. "Ingest once; each consumer picks its latency." Show the RTM job: single-node classic (RTM is classic-only today); analytics/dispatcher/app serverless.
2. **Version-by-version change tracking (10 min)** — "N versions, M significant": click a schedule-change trip, walk v1→v12 timeline; noise greyed. "This is your Lambda track-changes service, as 1 stateful operator — and no version is skipped (EPAM's latest-only MERGE compares v1 to v15)."
3. **Rules engine (15 min)** — Rule Studio live: Denis's own example (ask him for one), draft → preview → activate → hits in seconds, no redeploy. Show JSON Logic is the same format Reef uses. Mention the CEP/GoRules pattern for decision tables at scale.
4. **Actions to AWS (10 min)** — Traveler view card → Accept. Show `action_outbox.eventbridge_entry` payload (PutEvents shape, idempotency key). "Same code calls EventBridge through a UC service credential — you already use one for Kinesis."
5. **Cost (5 min)** — table below. Per use case, not one winner.
6. **Genie (5 min)** — "Which carriers had the most schedule changes today?"

## Cost (measured [date])
| Lane | Compute | DBU/hr | $/hr list | Notes |
|---|---|---|---|---|
| Real-time (RTM) | 1× m5d.2xlarge classic, 24×7 | [ ] | [ ] + EC2 [ ] | 15/s; same node handles 100/s peak [verify] |
| Analytics hourly | Serverless SQL | [ ] | [ ] | ~[ ] min/run |
| Dispatcher | Serverless jobs | [ ] | [ ] | |

## Questions to ask
- EPAM detection pairs prior HK → newer TK/UN/HX; definition we were given is HX/UN → HK/KK/TK. Which is intended?
- How many Reef/JSON-Logic rules exist, and can Denis share 3 real ones?
- Who owns the EventBridge bus/IAM role in AWS for a UAT test?

## UAT finale (stretch)
- In `bcd-api-uat`: create a single-node DBR 18 LTS dedicated cluster with RTM conf; run `src/rtm/job.py --source kinesis --kinesis-stream uat01-HermesTripsDS --kinesis-credential kinesis_reader_cred_uat --lakebase-host <FEVM host> --checkpoint /tmp/...`.
- Read-only on their side; outputs land in our Lakebase. Needs network egress from UAT to FEVM Lakebase — test Wed; if blocked, skip.
- Their stream is near-idle: ask EPAM to publish 3–5 test PNR versions (HK → HX + new HK) during the demo.

## Reset
- `TRUNCATE change_events, rule_hits, traveler_responses, action_outbox, trip_latest;` then bump checkpoint suffix (`rtm_v2`) in `resources/trip_intel_rtm.job.yml` and redeploy.
```

- [ ] **Step 7: Commit**

```bash
git add src/app/static docs/DEMO_RUNBOOK.md src/app.yaml
git commit -m "feat: trip-intel app UI (ops console, rule studio, traveler) and demo runbook

Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

## Cut order if time runs short (from spec §10)

1. Task 9 Step 5–6 polish (Genie + cost can be shown from workspace directly).
2. Task 8 dispatcher (show `eventbridge_entry` built inline in a notebook instead).
3. Traveler view (Task 11 card UI) — keep the API.
4. Rule Studio "Draft with AI" (keep manual JSON Logic + preview + activate).
