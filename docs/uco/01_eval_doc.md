<!-- header BCD Travel × Databricks | Evaluation Document -->
<!-- footer Databricks | Data Intelligence Platform -->

# BCD Travel and Databricks Evaluation Document: Real-Time Booking Change Detection

## Key Contacts

| BCD Travel | Databricks |
|---|---|
| Bojidar Todorov, OMS (Reef) team | Donald Phelps, Account Executive |
| Denis, Principal Architect | Rohit Bhagwat, Solutions Architect |
| John Palomino, SVP Core Systems | [confirm] Sales Manager |
| EPAM (implementation partner) | |

## Evaluation Summary

BCD Travel is evaluating Databricks to replace its Lambda-based "track changes" service with a real-time change-detection pipeline for booking (PNR) versions, starting from the scheduled-changes use case. The evaluation started on 10/01/2026 (requirements call) and runs in BCD's UAT workspace (`bcd_api_uat.databricks_demo`); the working demo is complete and the results readout is being rescheduled. Target: an MVP before Thanksgiving 2026.

## Business Justification Framework

| Current State Challenges | Consequences |
|---|---|
| Every touch on a booking creates a new PNR version, often 10–200 per booking, and about 85% are re-saves with no business change | Downstream systems process and route noise; real changes are hard to find |
| The current prototype loads versions in hourly batches | A schedule change can wait up to an hour before ops or the traveler is told |
| Change detection lives in a Lambda, separate from analytics | Logic is duplicated, and operational and analytical views can disagree |
| Business rules are code in the current service | Every rule change needs engineering time and a deployment |

| Future State | Outcomes |
|---|---|
| Every version compared with the previous one in sub-second time, on one platform | Ops alerts, traveler notifications and service calls within ~0.2 s of the change |
| Business analysts write and approve rules in plain English | New rules live in seconds, without code or deploys |
| One read of the Kinesis feed feeds both operations and analytics | One copy of the logic; operational and analytical numbers always agree |
| Governed in Unity Catalog, deployed as one bundle | Repeatable promotion from UAT to production |

## Technical Architecture

### Current State

Kinesis (`HermesTripsDS`) → AWS Lambda (track-changes diff) → EventBridge → downstream Lambdas and services; S3 as the data lake; hourly batch loads into Databricks for analytics.

### Databricks: Future State (built in UAT)

- **Real-time lane:** Spark Structured Streaming in Real-Time Mode on one classic node. `transformWithState` keeps each trip's previous version, diffs it, tags the change (schedule change, seat removed, contact or payment change) and evaluates JSON Logic rules. Results are written idempotently to Lakebase (managed Postgres) in ~0.2 s.
- **Actions:** an outbox and dispatcher build EventBridge PutEvents entries with deterministic IDs, so retries never send duplicates.
- **Analytical lane, ingest once:** Lakebase's change data feed copies every decision to Delta within seconds; a serverless Lakeflow pipeline builds silver and gold tables with data-quality expectations; Genie answers questions in plain English.
- **App:** Live Ops Console, Rule Studio (English → JSON Logic, preview, approve) and a traveler notification view, as a Databricks App.
- **Sinks:** Lakebase is the demo's choice. Real-Time Mode also writes to Kafka/MSK natively and to any database or API (including EventBridge) through a ForeachWriter.

## Technical Requirements for a Successful Evaluation

| Task | Detail | Measure of Success | Databricks Measurement | Status |
|---|---|---|---|---|
| **Objective: real-time change detection** | | | | |
| Detect every version | Compare each PNR version with the previous version of the same trip | No version skipped; duplicates and out-of-order versions handled | Every version diffed; duplicates tagged stale and ignored; idempotent writes | Complete |
| Latency at normal load | 15 versions/s | Sub-second event → decision | p50 185 ms, p95 254 ms | Complete |
| Latency at holiday peak | 100 versions/s | Sub-second on modest compute | p50 73 ms, p95 108 ms, p99 124 ms on one node at 14% CPU | Complete |
| Schedule-change detection | HX/UN segment paired with a new HK/KK/TK, same route and day, within 60 min | Correctly tagged and explained | Tagged with old/new flight and time delta | Complete; confirm definition against real GDS data |
| Noise filtering | Suppress versions with no business change | Only significant changes reach consumers | ≈85% tagged no-significant-change and filtered | Complete |
| **Objective: business-owned rules and actions** | | | | |
| Rules in plain English | Analysts author rules without code | Rule live without a deploy | English → JSON Logic, preview, approve; live in ≈5 s | Complete |
| Trigger actions | Alert ops, notify traveler, invoke a service | Exactly-once delivery to AWS | PutEvents entries built with idempotency keys (simulated, no bus in UAT) | Partial: needs an EventBridge bus and role in UAT |
| **Objective: analytics and governance** | | | | |
| Ingest once | Analytics without a second read of Kinesis | One copy of the logic | Lakebase change data feed → Delta → serverless pipeline | Complete |
| Data quality | Expectations on parsed versions | Bad rows dropped and counted | 932,618 versions passed, 0 dropped | Complete |
| Natural-language analytics | Genie over gold and stream decisions | Correct answers to sample questions | e.g. 14.6% of today's versions significant | Complete |
| **Objective: integration with BCD's environment** | | | | |
| Read BCD's Kinesis | `uat01-HermesTripsDS`, read-only | Stream reads production-shaped data | Connected to all 4 shards (Runtime 19 polling); enhanced fan-out recommended | Partial: no live records yet; EFO needs IAM on BCD's side |
| Cost | Run the real-time lane 24×7 | Predictable, modest cost | 1.37 DBU/h (≈$0.21/h list) + EC2; pipeline ≈$0.10/run | Complete |

## Mutual Success Plan

### Pre-Evaluation

| Status | Action | Description / Goal | Target Date | Customer Owners | Databricks Owners |
|---|---|---|---|---|---|
| Completed | Requirements call | Review use cases and current architecture with OMS, the principal architect and EPAM | 10/01/2026 | Bojidar, Denis | Rohit, Donald |
| Completed | Architecture review | Design validated with Databricks Real-Time Mode specialists (sinks, state, cost) | 10/07/2026 | | Rohit |
| In Progress | Shared Evaluation Document | Agree measures of success (this document) | [TBD] | Bojidar | Rohit |
| Not Started | Execution alignment | Agree implementation model (EPAM / PS / self), resources and path to production | [TBD] | John Palomino | Donald |

### Evaluation

| Status | Action | Description / Goal | Target Date | Customer Owners | Databricks Owners |
|---|---|---|---|---|---|
| Completed | Build in BCD UAT | Stream, Lakebase, change feed, pipeline, Genie, app, live notebook | 10/08/2026 | | Rohit |
| Not Started | Results readout | Live walkthrough; confirm measures of success | [rescheduled, TBD] | Bojidar, Denis | Rohit, Donald |
| Not Started | Real data validation | Run on live `HermesTripsDS` records; validate schedule-change definition | [TBD] | Bojidar, EPAM | Rohit |
| Not Started | EventBridge send test | Real PutEvents to a UAT bus | [TBD] | [owner of bus/IAM] | Rohit |

### Post-Evaluation

| Status | Action | Description / Goal | Target Date | Customer Owners | Databricks Owners |
|---|---|---|---|---|---|
| Not Started | Usage scoping | Volumes, consumers needing sub-second, costs | [TBD] | | Donald, Rohit |
| Not Started | Onboarding plan | Align delivery team, architecture and milestones to an MVP before Thanksgiving | [TBD] | John Palomino, EPAM | Donald, Rohit |

## Open Questions for BCD

1. Which consumers truly need sub-second results, and which are fine with seconds or minutes?
2. Can we register an enhanced fan-out consumer on `HermesTripsDS` (IAM on BCD's side)?
3. Can the team share three real rules from the current service?
4. How does a schedule change appear in real GDS data (new segment vs. status change on the same segment)?
5. Who owns the EventBridge bus and IAM role for a UAT send test?

## Useful Resources

- Real-Time Mode docs: https://docs.databricks.com/aws/en/structured-streaming/real-time
- Real-Time Mode GA announcement: https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks
- Real-Time Mode with a Lakebase sink: https://community.databricks.com/t5/technical-blog/fraud-detection-feature-engineering-with-structured-streaming/ba-p/151308
- MakeMyTrip (travel), millisecond personalization: https://www.databricks.com/blog/how-makemytrip-achieved-millisecond-personalization-scale-databricks
