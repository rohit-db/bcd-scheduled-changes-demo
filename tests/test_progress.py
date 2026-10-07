import json

from rtm.progress import latency_line

PROGRESS = {
    "batchId": 7,
    "latencies": {
        "processingLatencyMs": {"P0": 2, "P50": 11, "P90": 20, "P95": 25, "P99": 40},
        "sourceQueuingLatencyMs": {"P0": 1, "P50": 30, "P90": 50, "P95": 60, "P99": 90},
        "e2eLatencyMs": {"P0": 5, "P50": 45, "P90": 70, "P95": 85, "P99": 120},
    },
}


def test_latency_line_reports_built_in_percentiles():
    line = latency_line(json.dumps(PROGRESS))
    assert line == ("RTM_LATENCY batch=7 e2e p50=45 p95=85 p99=120 | sourceQueuing p50=30 p95=60 p99=90"
                    " | processing p50=11 p95=25 p99=40")


def test_latency_line_is_none_without_latencies():
    assert latency_line(json.dumps({"batchId": 3})) is None


def test_latency_line_skips_missing_metrics():
    progress = {"batchId": 1, "latencies": {"e2eLatencyMs": {"P50": 9, "P95": 10, "P99": 12}}}
    assert latency_line(json.dumps(progress)) == "RTM_LATENCY batch=1 e2e p50=9 p95=10 p99=12"
