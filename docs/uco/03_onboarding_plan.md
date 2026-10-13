<!-- header BCD Travel × Databricks | Onboarding Plan -->
<!-- footer Databricks | Data Intelligence Platform -->

# BCD Travel and Databricks Onboarding Plan: Real-Time Booking Change Detection

**Use case:** real-time scheduled-change detection, replacing the Lambda "track changes" service. **Target:** MVP in production before Thanksgiving 2026. **Starting point:** the working build in BCD's UAT workspace (`bcd_api_uat.databricks_demo`), deployed as one Databricks Asset Bundle.

## U3/U4: Onboarding Plan

### WHO: Delivery Team (AE led)

| Role | Name | Responsibility |
|---|---|---|
| Executive sponsor | [confirm: John Palomino] | Priority, resources, sign-off |
| Technical owner | Bojidar Todorov (OMS / Reef) | Requirements, rules, schedule-change definition |
| Architect | Denis (Principal Architect) | Target architecture, consumer latency needs |
| Implementation partner | EPAM | Build, test and deploy to production |
| Databricks AE | Donald Phelps | Commercials, resourcing, exec cadence |
| Databricks SA | Rohit Bhagwat | Architecture, design reviews, UAT build hand-off |

- **Skills and enablement:** Structured Streaming (Real-Time Mode, `transformWithState`), Lakebase, Lakeflow Declarative Pipelines, Databricks Asset Bundles. Hand-off walkthrough of the UAT code for EPAM; [confirm certification or training plan].
- **Partner and PS support:** EPAM-led build with Databricks SA guidance; PS Assurance decision recorded separately by the Engagement Manager.
- **Resources and funding:** [confirm EPAM capacity and whether the free/resource-backed POC program applies].

### HOW: Technical Design and Architecture (SA led)

- **Real-time lane:** Real-Time Mode on a single classic node (Runtime 18.2, Photon off, on-demand, no autoscaling). Reads Kinesis; diffs each version against the previous one; tags changes; evaluates JSON Logic rules; writes idempotently to Lakebase.
- **Actions:** outbox and dispatcher to EventBridge (deterministic IDs). Alternative: publish directly from the stream with a ForeachWriter.
- **Analytical lane:** Lakebase change data feed → Delta history tables → serverless Lakeflow pipeline (silver/gold with expectations) → Genie.
- **Production design changes to make before go-live:**
  - Kinesis enhanced fan-out consumer instead of polling.
  - Trip state kept in Lakebase (indexed by trip ID) or a state TTL, because trips can change months apart.
  - Sizing: task slots must cover Kinesis shards plus shuffle partitions (4 shards + 4 partitions fill one 8-core node).
  - Project-level tags, cost monitoring and budget alerts on the real-time cluster and pipeline.
- **Validate architecture with BCD before development begins** (consumer latency needs, sink choices, rule ownership).

### WHEN: Onboarding Readiness (SA)

- **Technical dependencies:** EFO consumer on `HermesTripsDS` (IAM on BCD's side); EventBridge bus and role; Lakebase instance in production; three real rules from the current service.
- **Pre-development approvals:** [confirm security / enterprise-architecture review].
- **Production workspace:** classic compute allowed for the real-time job; UAT settings to carry over (Apps from Git; explicit streaming checkpoints).
- **Sign-offs:** project plan reviewed with BCD and EPAM; resources and dates agreed.

### RISKS: Path to Production (AE)

| Risk | Impact | Mitigation | Owner |
|---|---|---|---|
| Schedule-change definition differs in real GDS data | Missed or wrong detections | Validate on live UAT records first | Bojidar, Rohit |
| Unbounded per-trip state | Memory and checkpoint growth over months | Lakebase-backed state or TTL | EPAM, Rohit |
| Shared Kinesis read limits | Throttling other consumers | Enhanced fan-out | BCD platform |
| BCD bandwidth | Slipped MVP date | Single use case first; partner-led | Donald |
| Real-Time Mode in pipelines is preview | Can't consolidate into one serverless pipeline yet | Keep standalone job; revisit when a Lakebase sink is available | Rohit |

**Comms plan:** weekly delivery check-in (BCD, EPAM, Databricks); [confirm exec steering cadence].

| Milestone | Status | Start Date | Original Target | Current Target | Owner |
|---|---|---|---|---|---|
| UAT build complete | Complete | 10/01/2026 | 10/08/2026 | 10/08/2026 | Rohit |
| Evaluation readout | Not started | | 10/08/2026 | [rescheduled] | Donald, Rohit |
| Project plan finalized | Not started | | [TBD] | [TBD] | BCD, EPAM |
| Development started | Not started | | [TBD] | [TBD] | EPAM |
| Ready for UAT (live Kinesis data) | Not started | | [TBD] | [TBD] | EPAM |
| Ready for production (testing complete) | Not started | | [TBD] | [TBD] | EPAM, BCD |
| Initial production deployment (MVP) | Not started | | Before Thanksgiving 2026 | | BCD, EPAM |

## U5: Onboarding

### Ongoing Check-In: Delivery Success (AE led)

- Formal kick-off with the champion and delivery team.
- Track enablement against the plan above.
- Recurring delivery, champion and executive cadences.
- Refine the path to production: latency SLOs per consumer, alerting, runbooks, cost.
- After go-live: business-value readout (detection latency, noise removed, rules owned by the business) and next OMS use cases.
