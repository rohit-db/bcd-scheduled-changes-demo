# What we built — plain-language guide

## The one-paragraph version

BCD gets a new copy of a booking (PNR) every time any system touches it — often 10–200 copies in the
first few minutes, most of them meaningless re-saves. Today they diff each copy against the previous one
in an AWS Lambda, decide whether anything important changed, and route it. EPAM's prototype does the
loading in hourly batches, so nothing is real-time yet. **We built the real-time version of BCD's own
process on Databricks:** every booking version is compared to the previous one within ~0.2 seconds,
tagged with what changed, run through business rules that analysts can write in plain English, and
turned into actions (alert ops, notify the traveler, call a service) — with a web app showing all of it
live.

## The pieces, in the order data flows

```
 Booking versions ──► 1. Real-time stream ──► 2. Operational database ──► 4. Web app (3 screens)
 (fake, in BCD's        compare to previous      (Lakebase = Postgres)        Ops · Rules · Traveler
  exact JSON shape)     tag · rules · actions          │        │
                                                       │
                                                       └──► 3a. Dispatcher → AWS EventBridge

 Same booking feed ──► 3b. Lakeflow pipeline (hourly, serverless): bronze → silver → gold ──► Genie
                          (raw JSON)   (parsed + DQ)  (same diff logic, daily metrics)
```
In production both lanes read the same Kinesis stream; in the demo both read the same generator.

| # | Piece | What it does, in plain words | Databricks product |
|---|---|---|---|
| 0 | **Data generator** | Pretends to be BCD's Kinesis feed: 50 bookings at a time, 12 versions each, 15 per second. Most versions are noise; some carry a real change (flight moved, seat lost, email changed, card changed, hotel added). Same JSON shape as BCD's real UAT records. | (Python inside the stream) |
| 1 | **Real-time stream** | Reads each version, remembers the previous version of that booking, works out exactly what changed, checks the business rules, and decides actions. ~190 ms from event to decision. | Spark Structured Streaming **Real-Time Mode** on one small classic cluster |
| 2 | **Operational database** | Where results land so apps can read them instantly: latest booking, every change, rule hits, pending actions. | **Lakebase** (managed Postgres) |
| 3a | **Dispatcher** | Takes pending actions and sends them to AWS EventBridge (the thing EPAM said was impossible). Runs in "simulated" mode because our demo workspace has no AWS bus — it still builds the exact message. | Serverless job |
| 3b | **Batch pipeline** | The analytics side, as a pipeline you can see: **bronze** keeps every raw booking version, **silver** parses it with data-quality checks and flattens segments, **gold** runs the *same* diff code as the real-time stream plus daily metrics. Runs hourly. | **Lakeflow Declarative Pipeline** (serverless) + Delta |
| 3c | **Genie space "BCD Trip Changes"** | Analysts ask questions in English ("which carriers had the most schedule changes today?") over the gold tables. | **Genie** |
| 4 | **Web app** | Three screens (below). | **Databricks App** + Foundation Model (Claude) |

### The three screens

1. **Live Ops Console** — a live feed of only the *significant* changes, how fast they were detected,
   and what % of versions were noise. Click a booking to see every version v1→v12 with the noise greyed
   out and the real change spelled out ("segment S1: HK → HX, new flight added").
2. **Rule Studio** — type a rule in English ("if a confirmed flight has no seat, call the seat service"),
   the AI turns it into a JSON-Logic rule (the format BCD already uses), you preview how many recent
   bookings it would have matched, click Approve, and it starts firing on live data within ~5 seconds.
   No code, no redeploy.
3. **Traveler view** — what the passenger would see on their phone: "Your flight UA501 moved from
   7:00 to 7:45 — Accept / Rebook". Clicking writes the answer back.

## Words you'll hear (and what to say)

| Term | Say this |
|---|---|
| **Real-Time Mode (RTM)** | "A setting on Spark streaming that processes each event as it arrives instead of in micro-batches — milliseconds instead of seconds." Standalone RTM (what we use, because it writes to Lakebase) runs on a classic cluster. RTM *inside a Lakeflow pipeline* can run serverless (Public Preview) but only writes to Kafka today. |
| **transformWithState** | "Spark remembers the last version of each booking, so every new version is compared to the one right before it." This replaces their Lambda diff. |
| **Lakebase** | "Postgres, managed by Databricks, sitting next to the lakehouse — so apps get millisecond reads." |
| **JSON Logic** | "The rule format BCD's own service already uses." |
| **Outbox** | "A to-do list of actions. Each one has a unique ID, so if anything retries, nobody gets two emails." |
| **Edge-triggered rule** | "A rule fires once when its condition becomes true — not again on every one of the next 10 re-saves." |
| **Serverless vs classic** | Real-time lane = one small always-on classic cluster. Everything else = serverless, pay per use. |

## Coverage check against the 2026-10-01 call

Legend: ✅ built and shown · 🟡 partly / talk track only · ❌ not built yet

| # | What came up in the call | Status | Where it's answered |
|---|---|---|---|
| 1 | Batch vs real-time — which to build? | ✅ | Ops Console: ~190 ms real-time lane vs hourly lane freshness, side by side |
| 2 | Cluster vs serverless | ✅ | RTM on 1 classic node; dispatcher, analytics, app serverless — "per use case" |
| 3 | Which is cheaper? | ❌ | Cost table in runbook is empty — needs ≥1 h of billing data, then a query |
| 4 | Data volume: 1–1.5M/day, 15/s normal, 50–100/s peak | ✅ | One small node: 15/s → p50 190 ms; 100/s → p50 73 ms, p99 128 ms |
| 5 | "Ingest once, many consumers" (your whiteboard: Kinesis → bronze → several pipelines) | ✅ | Bronze table keeps every raw version; the ms lane reads the source directly (RTM can't read Delta), the batch lane reads bronze |
| 6 | Read from their Kinesis stream | 🟡 | Code path exists (EPAM's exact options), **never run**; UAT finale is a stretch |
| 7 | Deeply nested JSON gets flattened | ✅ | Parser flattens the PNR into a snapshot (our payload is simpler than real PNRs) |
| 8 | Deduplication | ✅ | Duplicate/out-of-order versions tagged STALE and ignored; all writes idempotent |
| 9 | 10–200 versions per booking; track every version vs previous | ✅ | Per-version diff + timeline view; EPAM's job only compares latest-vs-latest |
| 10 | "Only send significant versions" (throttling idea) | ✅ | ~85% tagged NO_SIGNIFICANT_CHANGE and suppressed from the feed |
| 11 | Replace the Lambda "track changes" service | ✅ | Same logic as one stateful step in the stream |
| 12 | Different consumers care about different changes (finance → payment, etc.) | 🟡 | Tags (PAYMENT_CHANGED, CONTACT_CHANGED…) + rule targets; **no per-consumer subscription screen** |
| 13 | Rules engine is "the core" (Denis); multiple schemas, many rule sets | 🟡 | One schema (trips), unlimited rules; **no grouping into rule sets per consumer** |
| 14 | Business analysts write rules, not developers | ✅ | Rule Studio: English → JSON Logic → preview → approve |
| 15 | Example: segment has no seat → invoke a service | ✅ | Seed rule "Confirmed flight without a seat" → INVOKE_SERVICE seat-assignment-service |
| 16 | Trigger a Lambda / API; sync or async? | 🟡 | Async via outbox shown; **no live Lambda/API call, no sync example** |
| 17 | Publish to EventBridge (EPAM said impossible) | 🟡 | Exact PutEvents message built and stored; **simulated** — no AWS bus in our workspace |
| 18 | Consume *from* EventBridge | ❌ | Talk track only |
| 19 | Genie for analysts in natural language | ✅ | Genie space "BCD Trip Changes" over the gold tables, with sample questions |
| 20 | Genie pricing (tokens vs compute) | 🟡 | You answered on the call; repeat as talk track |
| 21 | Data-quality rules (DQT) | ✅ | Silver expectations: missing trip id / version dropped and counted; empty segments flagged — visible in the pipeline UI |
| 22 | Your promise: "combine these, show how I'd architect, low-latency RTM, external app consuming in real time" | ✅ | The whole demo |
| 23 | Your promise: send docs and customer stories | ❌ | Not sent — candidates: RTM GA blog, RTM fraud blog (Lakebase), MakeMyTrip |
| 24 | Do it in UAT, not production | 🟡 | Built on e2-demo with synthetic data in their schema; UAT untouched |
| 25 | Denis to send a real rule example | ❌ (theirs) | Ask in the demo; Rule Studio can take it live |
| 26 | EPAM's job design (one shared cluster, manual runs, redeploys) | 🟡 | Talk track: separate ingest from consumers; run ingest continuously |
| 27 | EPAM detection logic (HK → TK/UN/HX vs HX/UN → HK/KK/TK) | 🟡 | Ours follows the given definition; ask which is intended (in runbook) |

**Remaining gaps before Thursday:** cost numbers (#3) and the follow-up links (#23). Per-consumer rule sets (#12/#13) and a real Lambda call (#16) are larger and better
positioned as "next step with your team".

## Demo flow (55 min) — see `DEMO_RUNBOOK.md` for commands

1. **The problem in their words (3 min)** — "200 versions, 5 matter, and you need them now, not in an hour."
2. **Real-time vs batch (7 min)** — Ops Console KPIs; point at "0 s ago" vs "N min ago".
3. **One booking's story (10 min)** — click a SCHEDULE_CHANGE trip; walk the timeline; "this is your Lambda".
4. **Rules (15 min)** — ask Denis for a rule; type it; preview; approve; watch hits climb.
5. **Actions (8 min)** — Traveler card → Accept; show the EventBridge message.
6. **Cost + architecture (7 min)** — classic for RTM only, serverless elsewhere; cost table.
7. **Batch + Genie (5 min)** — show the pipeline graph (bronze → silver → gold, DQ metrics), then ask Genie
   "What share of booking versions today were significant vs noise?"
