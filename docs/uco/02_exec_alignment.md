<!-- header BCD Travel × Databricks | Executive Alignment -->
<!-- footer Databricks | Data Intelligence Platform -->

# Executive Alignment: Real-Time Booking Change Detection

**Use case:** replace BCD's Lambda "track changes" service with real-time change detection on Databricks, starting with scheduled changes.
**Status:** working demo complete in BCD's UAT workspace; readout being rescheduled. **Target:** MVP before Thanksgiving 2026.

## Executive Sponsor and Decision Objective

| | |
|---|---|
| Executive sponsor | [confirm: John Palomino, SVP Core Systems] |
| Technical owners | Bojidar Todorov (OMS / Reef), Denis (Principal Architect) |
| Implementation partner | EPAM |
| Databricks | Donald Phelps (AE), Rohit Bhagwat (SA) |
| Decision to make | Approve moving scheduled-change detection from the hourly batch prototype to a real-time pipeline on Databricks, and agree the delivery model and dates for an MVP before Thanksgiving |

## Why Now

- Schedule changes were chosen as the showcase use case on the 2026 roadmap (MVP before Thanksgiving), to prove value before adding more OMS use cases.
- Today's prototype loads hourly, so travelers and ops can hear about an airline schedule change up to an hour late.
- The same platform already runs BCD's analytics, so real-time detection can share data, governance and logic instead of adding another system.

## Business Outcomes

| Outcome | How it's measured | Evidence so far (UAT) |
|---|---|---|
| Faster response to airline schedule changes | Event → decision latency | p50 185 ms at normal load; 73 ms at 100/s peak |
| Less noise for downstream teams and systems | Share of versions forwarded | ≈85% of versions filtered as no-significant-change |
| Business-owned rules | Time from rule request to live | ≈5 s after approval; no code or deploy |
| One source of truth | Operational and analytical numbers match | Analytics reads the stream's own decisions (ingest once) |
| Predictable cost | Run-rate of the real-time lane | 1.37 DBU/h (≈$0.21/h list) + EC2; pipeline ≈$0.10/run |

## Agreed Success Criteria

Detailed in the Evaluation Document. In summary: every version compared with the previous one; sub-second detection at normal and peak load; schedule changes correctly identified; rules authored without code; actions delivered exactly once to AWS; analytics consistent with operations; governed in Unity Catalog.

## Implementation Strategy

| Option | Description | Recommendation |
|---|---|---|
| Partner-led (EPAM) with Databricks guidance | EPAM builds on the UAT design; Databricks SA reviews architecture and supports go-live | Recommended starting point [confirm with BCD and EPAM] |
| Databricks PS / FDE assisted | PS or FDE involvement for the real-time lane and state design | Consider if timelines are tight or production state design needs specialist support |
| Self-implementation | BCD engineering builds directly | Possible; slower without partner capacity |

## Dependencies

- **Kinesis access:** an enhanced fan-out consumer on `HermesTripsDS` (IAM change on BCD's side), so the stream never competes with other readers.
- **EventBridge:** a bus and IAM role in UAT for a real send test.
- **Business rules:** three real rules from the current service.
- **Data definition:** confirmation of how a schedule change appears in real GDS data.
- **Security and procurement:** [confirm any review required before production].

## Decision Process and Dates

| Step | Owner | Date |
|---|---|---|
| Evaluation readout (live demo, confirm success criteria) | Rohit, Donald | [rescheduled, TBD] |
| Decide which consumers need sub-second (one pipeline vs. two lanes) | BCD architecture | [TBD] |
| Agree delivery model, resources and funding | [Exec sponsor], Donald | [TBD] |
| Onboarding plan signed off | BCD, EPAM, Databricks | [TBD] |
| MVP in production | BCD, EPAM | Before Thanksgiving 2026 |

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Limited BCD engineering bandwidth | Start with one use case (scheduled changes); partner-led delivery |
| Schedule-change definition differs in real data | Validate on live UAT records before build-out |
| Per-trip state grows over months | Keep latest trip state in Lakebase (indexed by trip) or add a state TTL |
| Kinesis read limits shared with existing consumers | Enhanced fan-out consumer |

## Next Steps and Mutual Commitments

| Commitment | Owner | Due |
|---|---|---|
| Reschedule the results readout | Donald | [TBD] |
| Share three real rules and the GDS schedule-change pattern | Bojidar | [TBD] |
| Confirm EFO and EventBridge access in UAT | [BCD platform owner] | [TBD] |
| Draft onboarding plan with EPAM | Rohit, EPAM | [TBD] |
