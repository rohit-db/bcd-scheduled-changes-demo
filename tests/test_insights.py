from app.insights import quality_checks, shape_genie

MESSAGE = {
    "conversation_id": "c1",
    "message_id": "m1",
    "status": "COMPLETED",
    "attachments": [
        {"attachment_id": "a0", "text": {"content": "Here is the breakdown."}},
        {"attachment_id": "a1", "query": {"query": "SELECT carrier, count(*) FROM gold", "description": "Count by carrier"}},
    ],
}
RESULT = {
    "statement_response": {
        "manifest": {"schema": {"columns": [{"name": "carrier"}, {"name": "n"}]}},
        "result": {"data_array": [["UA", "12"], ["AC", "9"]]},
    }
}


def test_shape_genie_returns_text_sql_and_rows():
    out = shape_genie(MESSAGE, RESULT)
    assert out["text"] == "Here is the breakdown."
    assert out["sql"] == "SELECT carrier, count(*) FROM gold"
    assert out["description"] == "Count by carrier"
    assert out["columns"] == ["carrier", "n"]
    assert out["rows"] == [["UA", "12"], ["AC", "9"]]


def test_shape_genie_without_a_query_has_no_table():
    msg = {"attachments": [{"text": {"content": "I can only answer about trip changes."}}]}
    out = shape_genie(msg, None)
    assert out["text"].startswith("I can only")
    assert out["sql"] is None and out["columns"] == [] and out["rows"] == []


def test_shape_genie_caps_rows():
    result = {"statement_response": {"manifest": {"schema": {"columns": [{"name": "x"}]}},
                                     "result": {"data_array": [[str(i)] for i in range(500)]}}}
    assert len(shape_genie(MESSAGE, result)["rows"]) == 50


DQ = {"total": 1000, "missing_trip_id": 0, "bad_version": 0, "no_segments": 3, "duplicates": 0}
COUNTS = {"lakebase": 1000, "cdf": 1000, "silver": 1000, "gold": 1000}


def _by_name(checks):
    return {c["name"]: c for c in checks}


def test_quality_checks_pass_when_layers_reconcile():
    checks = _by_name(quality_checks(DQ, COUNTS))
    assert checks["Trip id present"]["status"] == "pass"
    assert checks["Valid version number"]["status"] == "pass"
    assert checks["No duplicate versions"]["status"] == "pass"
    assert checks["Lakebase → Delta (change data feed)"]["status"] == "pass"
    assert checks["Delta → silver → gold"]["status"] == "pass"


def test_quality_checks_warn_on_empty_segments_and_show_the_count():
    check = _by_name(quality_checks(DQ, COUNTS))["Every version has segments"]
    assert check["status"] == "warn"
    assert "3" in check["detail"]


def test_quality_checks_fail_on_dropped_rows():
    counts = dict(COUNTS, silver=990, gold=990)
    check = _by_name(quality_checks(dict(DQ, missing_trip_id=10), counts))["Trip id present"]
    assert check["status"] == "fail"
    assert "10" in check["detail"]


def test_quality_checks_warn_while_the_feed_is_catching_up():
    # Delta lags Lakebase by a few seconds: a small gap is expected, not a failure.
    check = _by_name(quality_checks(DQ, dict(COUNTS, cdf=995)))["Lakebase → Delta (change data feed)"]
    assert check["status"] == "warn"
