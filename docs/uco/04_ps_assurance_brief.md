<!-- header INTERNAL | Databricks Confidential | Draft for EM review -->
<!-- footer Databricks | Data Intelligence Platform -->

# PS Assurance: Engagement Manager Review Brief

**Account:** BCD Travel | **Use case:** real-time booking change detection (scheduled changes) | **Prepared by:** Rohit Bhagwat (SA) | **Status:** draft for EM review. This is not an approval; the decision fields below are for the Engagement Manager.

## Decision (EM to complete)

| Field | Value |
|---|---|
| PS Assurance | ☐ Required ☐ Not required ☐ Declined |
| Reason | |
| Linked PS opportunity / SOW | |
| Approver (EM) | |
| Date | |

## Implementation Model

- **Delivery:** partner-led by EPAM (BCD's existing implementation partner), with Databricks SA architecture guidance. [Confirm with BCD and EPAM.]
- **Starting point:** working build in BCD's UAT workspace, deployed as one Databricks Asset Bundle: Real-Time Mode stream, Lakebase, change data feed, Lakeflow pipeline, Genie, Databricks App.
- **Target:** MVP in production before Thanksgiving 2026.

## Complexity and First-of-Kind Considerations

| Area | Assessment |
|---|---|
| Real-Time Mode (GA) | Standalone job on classic compute; newer to the field. Python `transformWithState` with a custom ForeachWriter sink. |
| State design | Per-trip state is unbounded today; production needs Lakebase-backed state or a TTL. Highest technical risk. |
| Kinesis integration | Needs an enhanced fan-out consumer (IAM on BCD's side); 4 shards size exactly to one node. |
| Lakebase change data feed | Beta; tables need `REPLICA IDENTITY FULL`. |
| Exactly-once delivery to AWS | Outbox with deterministic IDs; real EventBridge send not yet tested. |

## Partner Capability

- EPAM runs BCD's current batch prototype. [Confirm Structured Streaming / Real-Time Mode and Lakebase experience; identify training needs.]
- Proposed hand-off: walkthrough of the UAT code and design review before development starts.

## If PS Assurance Is Required: Proposed Scope

- Architecture and design review of the production real-time lane (state, sizing, sinks).
- Go-live readiness review (monitoring, alerting, runbooks, failure and replay testing).
- [Effort, cost and funding source: EM to estimate.]

## Expected Return

- Real-time lane run-rate measured in UAT: 1.37 DBU/h (≈$0.21/h list) + EC2; serverless pipeline ≈$0.10/run.
- Strategic: scheduled changes is BCD's showcase use case for further OMS use cases. [Add $DBU projection.]

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Unbounded state | Lakebase-backed state or TTL before production |
| Partner unfamiliar with Real-Time Mode | Hand-off session; SA design reviews; PS Assurance if needed |
| Schedule-change definition not yet validated on real data | Validate on live UAT records first |
| Preview/Beta dependencies (change data feed) | Fallback: earlier file-based analytical lane, kept in the code base |
