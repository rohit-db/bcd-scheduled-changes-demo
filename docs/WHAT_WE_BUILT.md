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

Diagrams (overview, sequences, pipeline, deployment, target state): [`docs/architecture/`](architecture/README.md).

```
 Booking versions ──► 1. Real-time stream ──► 2. Operational database ──► 4. Web app (3 screens)
 (fake, in BCD's        compare to previous      (Lakebase = Postgres)        Ops · Rules · Traveler
  exact JSON shape)     tag · rules · actions          │        │
                                                       │
                                                       └──► 3a. Dispatcher → AWS EventBridge

 Same booking feed ──► 3b. Lakeflow pipeline (hourly, serverless): bronze → silver → gold ──► Genie
                          (raw JSON)   (parsed + DQ)  (same diff logic, daily metrics)
```
In production both lanes read the same Kinesis stream. In the demo both use the same generator, and in BCD UAT the
bronze table also reads their Kinesis stream (read-only), tagged `source = 'kinesis'`.

| # | Piece | What it does, in plain words | Databricks product |
|---|---|---|---|
| 0 | **Data generator** | Pretends to be BCD's Kinesis feed: 50 bookings at a time, 12 versions each, 15 per second. Most versions are noise; some carry a real change (flight moved, seat lost, email changed, card changed, hotel added). Same JSON shape as BCD's real UAT records. | (Python inside the stream) |
| 1 | **Real-time stream** | Reads each version, remembers the previous version of that booking, works out exactly what changed, checks the business rules, and decides actions. ~185 ms from event to decision at 15/s, ~73 ms at 100/s (measured in BCD UAT). The same job can run as a 1 s micro-batch (~1 s) for comparison. | Spark Structured Streaming **Real-Time Mode** on one small classic cluster |
| 2 | **Operational store** | Where results land so apps can read them instantly: latest booking, every change, rule hits, pending actions. Lakebase is our choice for the demo, not a requirement: the stream can equally publish to Kafka/MSK or write to any database or API (see "Where real-time results can go"). | **Lakebase** (managed Postgres) |
| 3a | **Dispatcher** | Takes pending actions and sends them to AWS EventBridge (the thing EPAM said was impossible). Runs in "simulated" mode because our demo workspace has no AWS bus — it still builds the exact message. | Serverless job |
| 3b | **Batch pipeline** | The analytics side, as a pipeline you can see: **bronze** keeps every raw booking version, **silver** parses it with data-quality checks and flattens segments, **gold** runs the *same* diff code as the real-time stream plus daily metrics. Runs hourly. | **Lakeflow Declarative Pipeline** (serverless) + Delta |
| 3c | **Genie space "BCD Trip Changes"** | Analysts ask questions in English ("which carriers had the most schedule changes today?") over the gold tables, and over the stream's own decisions (rule hits, actions) through Lakebase registered in Unity Catalog. | **Genie** |
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
| **Real-Time Mode (RTM)** | "A setting on Spark streaming that processes each event as it arrives instead of in micro-batches — milliseconds instead of seconds." Standalone RTM (what we use) runs on a classic cluster and can write to Kafka or, through a ForeachWriter, to any database or API. RTM *inside a Lakeflow pipeline* can run serverless (Public Preview) but only writes to Kafka-protocol sinks (Kafka, MSK, Event Hubs); it has no ForeachWriter, so it can't write to Lakebase directly today. |
| **transformWithState** | "Spark remembers the last version of each booking, so every new version is compared to the one right before it." This replaces their Lambda diff. |
| **Lakebase** | "Postgres, managed by Databricks, sitting next to the lakehouse — so apps get millisecond reads." |
| **JSON Logic** | "The rule format BCD's own service already uses." |
| **Outbox** | "A to-do list of actions. Each one has a unique ID, so if anything retries, nobody gets two emails." |
| **Edge-triggered rule** | "A rule fires once when its condition becomes true — not again on every one of the next 10 re-saves." |
| **Serverless vs classic** | Real-time lane = one small always-on classic cluster. Everything else = serverless, pay per use. |

## Operational vs analytical: why two lanes

Databricks' own framing for streaming splits workloads in two:

| | Operational | Analytical |
|---|---|---|
| Latency | sub-second (≈40–300 ms) | seconds to hours |
| Who acts on it | a machine, in the loop | a person or a model |
| Lands in | a message bus (Kafka) or an operational store (Lakebase, any OLTP database) | Delta tables |
| BCD examples | alert ops, notify the traveler, call the seat service | finance reporting, Genie, EPAM's pipelines |
| Engine | Structured Streaming, Real-Time Mode | Structured Streaming / Lakeflow pipelines, micro-batch |

Both lanes use the **same Spark engine, the same feed and the same change-detection code** (`trip_intel`).
That is the opposite of the "two engines" problem (e.g. Flink for real time + Spark for batch) where logic drifts.

### Do they need two pipelines?

Only if some consumer needs **sub-second** results. The deciding question for BCD is: *which consumers truly need
sub-second, and which are fine with seconds or minutes?*

- **Nobody needs sub-second** → **one** serverless Lakeflow pipeline: Kinesis → bronze → silver → change
  detection → Delta, plus a sink to the operational store. Latency is seconds (micro-batch). Simplest, all serverless.
- **Some consumers need sub-second** (e.g. traveler notifications, seat service) → two jobs reading the same stream:
  1. the **real-time job** (Real-Time Mode) for those consumers only, and
  2. a **bronze ingest** that lands every raw version once in Delta for everything else.

  Real-Time Mode can't write Delta, which is why the raw copy is its own small job. It is not a second copy of the
  business logic: both call the same library, so a schedule change means the same thing in both, and the
  stream's decisions are queryable next to gold through Unity Catalog.

What to avoid is the old "Lambda architecture" problem: two codebases that each implement the logic and drift apart.

## Where real-time results can go (sinks)

Lakebase is what the demo uses because apps need millisecond reads of the latest state. It isn't the only option.
Real-Time Mode supports:

- **Kafka / Amazon MSK / Event Hubs** topics, natively. Downstream services subscribe.
- **Any database or API through a ForeachWriter**: Lakebase/Postgres (this demo), DynamoDB, Redis, an HTTP service,
  or EventBridge directly. We send EventBridge events through an outbox so retries never create duplicates.
- **Not supported as Real-Time Mode sinks:** Delta tables, Kinesis.

A native Lakebase/Postgres sink is on the product roadmap. It would replace our custom ForeachWriter, and could let
the real-time lane run in a serverless Lakeflow pipeline. Present it as roadmap, with no dates.

## Customer stories and links we can share

- MakeMyTrip (travel): 44 ms p50, ~500 ms p99 (from over a minute), +7% click-through —
  https://www.databricks.com/blog/how-makemytrip-achieved-millisecond-personalization-scale-databricks
- Coinbase: fraud and ML features, <100 ms p99 — https://www.databricks.com/customers/coinbase/lakeflow
- Dutch Railways: train telemetry, one platform for real-time and analytics — https://www.databricks.com/customers/dutch-railways
- Real-Time Mode GA announcement — https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks
- Docs — https://docs.databricks.com/aws/en/structured-streaming/real-time
- Fraud features with Real-Time Mode + Lakebase (same sink pattern as ours) —
  https://community.databricks.com/t5/technical-blog/fraud-detection-feature-engineering-with-structured-streaming/ba-p/151308

## Open findings to raise with BCD

- **How a schedule change looks in real data.** Our rule pairs a cancelled segment with a new confirmed one; UAT test
  records flip the status on the same segment and move its time (tagged `SEGMENT_MODIFIED` today).
- **Kinesis for Real-Time Mode:** EFO on Runtime 18.2 (an AWS-side consumer and IAM change) or Runtime 19 polling,
  which shares each shard's read limit with EPAM's reader. The UAT stream has 4 shards.
- **State expiry.** Decide how long to remember a trip after its last version or travel date.

## Coverage check against the 2026-10-01 call

Legend: ✅ built and shown · 🟡 partly / talk track only · ❌ not built yet

| # | What came up in the call | Status | Where it's answered |
|---|---|---|---|
| 1 | Batch vs real-time — which to build? | ✅ | Ops Console shows operational vs analytical tiers side by side; measured in UAT: Real-Time Mode ≈0.19 s vs the same job as a 1 s micro-batch ≈1 s (p50) |
| 2 | Cluster vs serverless | ✅ | RTM on 1 classic node; dispatcher, analytics, app serverless — "per use case" |
| 3 | Which is cheaper? | ✅ | Measured in UAT: real-time node 1.37 DBU/h (≈ $0.21/h list + EC2), serverless pipeline ≈ $0.10/run. Micro-batch on the same node costs the same but is ~5× slower (≈1 s vs ≈0.19 s p50) |
| 4 | Data volume: 1–1.5M/day, 15/s normal, 50–100/s peak | ✅ | One small node in UAT: 15/s → p50 185 ms; 100/s → p50 73 ms, p99 124 ms at 14% avg CPU |
| 5 | "Ingest once, many consumers" (your whiteboard: Kinesis → bronze → several pipelines) | ✅ | Bronze table keeps every raw version; the ms lane reads the source directly (RTM can't read Delta), the batch lane reads bronze |
| 6 | Read from their Kinesis stream | 🟡 | Batch lane ✅ (12 real versions parsed). Real-time lane: connected to all 4 shards on DBR 19 polling; DBR 18.2 needs EFO. No live records to show yet — ask EPAM to publish test versions |
| 7 | Deeply nested JSON gets flattened | ✅ | Parser flattens the PNR into a snapshot (our payload is simpler than real PNRs) |
| 8 | Deduplication | ✅ | Duplicate/out-of-order versions tagged STALE and ignored; all writes idempotent |
| 9 | 10–200 versions per booking; track every version vs previous | ✅ | Per-version diff + timeline view; EPAM's job only compares latest-vs-latest |
| 10 | "Only send significant versions" (throttling idea) | ✅ | ~85% tagged NO_SIGNIFICANT_CHANGE and suppressed from the feed |
| 11 | Replace the Lambda "track changes" service | ✅ | Same logic as one stateful step in the stream |
| 12 | Different consumers care about different changes (finance → payment, etc.) | 🟡 | Tags (PAYMENT_CHANGED, CONTACT_CHANGED…) + rule targets; **no per-consumer subscription screen** |
| 13 | Rules engine is "the core" (their architect); multiple schemas, many rule sets | 🟡 | One schema (trips), unlimited rules; **no grouping into rule sets per consumer** |
| 14 | Business analysts write rules, not developers | ✅ | Rule Studio: English → JSON Logic → preview → approve |
| 15 | Example: segment has no seat → invoke a service | ✅ | Seed rule "Confirmed flight without a seat" → INVOKE_SERVICE seat-assignment-service |
| 16 | Trigger a Lambda / API; sync or async? | 🟡 | Async via outbox shown; **no live Lambda/API call, no sync example** |
| 17 | Publish to EventBridge (EPAM said impossible) | 🟡 | Exact PutEvents message built and stored; **simulated** — no AWS bus in our workspace |
| 18 | Consume *from* EventBridge | ❌ | Talk track only |
| 19 | Genie for analysts in natural language | ✅ | Genie space "BCD Trip Changes" over gold and the stream's decisions, with sample questions (demo workspace; UAT pending a catalog decision) |
| 20 | Genie pricing (tokens vs compute) | 🟡 | You answered on the call; repeat as talk track |
| 21 | Data-quality rules (DQT) | ✅ | Silver expectations: missing trip id / version dropped and counted; empty segments flagged — visible in the pipeline UI |
| 22 | Your promise: "combine these, show how I'd architect, low-latency RTM, external app consuming in real time" | ✅ | The whole demo |
| 23 | Your promise: send docs and customer stories | ❌ | Not sent — links collected under "Customer stories and links we can share" |
| 24 | Do it in UAT, not production | ✅ | Deployed in BCD UAT (`bcd_api_uat.databricks_demo`, own Lakebase); stream, micro-batch comparison, 100/s test, batch lane from their Kinesis all run there. App pending Git credential |
| 25 | Their architect to send a real rule example | ❌ (theirs) | Ask in the demo; Rule Studio can take it live |
| 26 | EPAM's job design (one shared cluster, manual runs, redeploys) | 🟡 | Talk track: separate ingest from consumers; run ingest continuously |
| 27 | EPAM detection logic (HK → TK/UN/HX vs HX/UN → HK/KK/TK) | 🟡 | Ours follows the given definition; ask which is intended (in runbook) |

**Remaining gaps before Thursday:** sending the follow-up links (#23), and live Kinesis records in the real-time lane
(#6, needs EFO or test publishes). Per-consumer rule sets (#12/#13) and a real Lambda call (#16) are larger and better
positioned as "next step with your team".

## Demo flow (55 min) — see `DEMO_RUNBOOK.md` for commands

1. **The problem in their words (3 min)** — "200 versions, 5 matter, and you need them now, not in an hour."
2. **Real-time vs batch (7 min)** — Ops Console tiers ("0 s ago" vs "N min ago"), then the measured comparison:
   same job, Real-Time Mode ≈0.19 s vs micro-batch ≈1 s. Ask: which consumers truly need sub-second?
3. **One booking's story (10 min)** — click a SCHEDULE_CHANGE trip; walk the timeline; "this is your Lambda".
4. **Rules (15 min)** — ask their architect for a rule; type it; preview; approve; watch hits climb (allow ~20 s for
   the first hit; it waits for a matching version).
5. **Actions (8 min)** — Traveler card → Accept; show the EventBridge message.
6. **Cost + architecture (7 min)** — classic for RTM only, serverless elsewhere; cost table from UAT billing; Kinesis
   needs EFO (or Runtime 19 polling) and 4 shards fill one node.
7. **Batch + Genie (5 min)** — show the pipeline graph (bronze → silver → gold, DQ metrics), then ask Genie
   "What share of booking versions today were significant vs noise?"
