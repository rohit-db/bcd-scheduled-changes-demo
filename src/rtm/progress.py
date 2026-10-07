"""One log line per real-time batch with the built-in latency percentiles from StreamingQueryProgress.

e2eLatencyMs = written to the source -> written downstream; sourceQueuingLatencyMs = written to the source ->
read by the query; processingLatencyMs = read -> written downstream (Databricks Real-Time Mode docs).
These sit next to the app's own event -> detected measurement so both can be quoted.
"""
import json

METRICS = (("e2e", "e2eLatencyMs"), ("sourceQueuing", "sourceQueuingLatencyMs"), ("processing", "processingLatencyMs"))


def latency_line(progress_json: str) -> str | None:
    progress = json.loads(progress_json)
    latencies = progress.get("latencies")
    if not latencies:
        return None
    parts = []
    for label, key in METRICS:
        p = latencies.get(key)
        if p:
            parts.append(f"{label} p50={p.get('P50')} p95={p.get('P95')} p99={p.get('P99')}")
    return f"RTM_LATENCY batch={progress.get('batchId')} " + " | ".join(parts)
