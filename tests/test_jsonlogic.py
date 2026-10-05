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
