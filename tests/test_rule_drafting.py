import pytest

from app.rule_drafting import parse_draft

FENCE = "`" * 3


def test_parses_fenced_json():
    text = f'Here you go:\n{FENCE}json\n{{"name": "No seat", "json_logic": {{"missing": "seat"}}, "explanation": "x"}}\n{FENCE}'
    d = parse_draft(text)
    assert d["name"] == "No seat" and d["json_logic"] == {"missing": "seat"}


def test_ignores_text_after_the_json_object():
    text = ('{"name": "Email changed", "json_logic": {"in": ["CONTACT_CHANGED", {"var": "change.tags"}]}, '
            '"explanation": "x"}\n\nNote: the context is {"trip": ..., "change": ...}.')
    d = parse_draft(text)
    assert d["json_logic"] == {"in": ["CONTACT_CHANGED", {"var": "change.tags"}]}


def test_rejects_unknown_operator():
    with pytest.raises(ValueError, match="unknown operator"):
        parse_draft('{"name": "a", "json_logic": {"regex": ["a"]}, "explanation": "b"}')


def test_rejects_missing_keys():
    with pytest.raises(ValueError, match="missing"):
        parse_draft('{"name": "a"}')


def test_rejects_non_json():
    with pytest.raises(ValueError):
        parse_draft("I cannot do that")


def test_accepts_content_block_list_from_chat_endpoint():
    blocks = [{"type": "reasoning", "summary": []},
              {"type": "text", "text": '{"name": "n", "json_logic": {"missing": "seat"}, "explanation": "e"}'}]
    assert parse_draft(blocks)["json_logic"] == {"missing": "seat"}
