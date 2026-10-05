"""Plain English -> JSON Logic rule drafting via a Foundation Model API chat endpoint."""
import json

from trip_intel.jsonlogic import OPERATORS, validate

SYSTEM_PROMPT = f"""You convert a travel-operations business rule written in plain English into JSON Logic.

The rule is evaluated against this context for every new version of a trip (PNR):
{{
  "trip": {{
    "trip_id": str, "record_locator": str, "version": int,
    "segments": [{{"type": "Air"|"Hotel", "segment_id": str, "status": "HK"|"KK"|"TK"|"HX"|"UN",
                   "carrier": str, "flight_number": str, "class_of_service": str, "origin": str (IATA),
                   "destination": str (IATA), "start": ISO-8601 str, "end": ISO-8601 str, "seat": str|null,
                   "hotel_name": str|null}}],
    "traveler": {{"name": str, "email": str, "phone": str}},
    "payment": {{"type": str, "last4": str}}
  }},
  "change": {{"tags": [str], "changes": [{{"segment": str, "field": str, "old": any, "new": any}}],
             "schedule_changes": [{{"old": segment, "new": segment, "delta_minutes": int}}]}}
}}
Change tags: NEW_TRIP, SEGMENT_ADDED, SEGMENT_REMOVED, SEGMENT_MODIFIED, SEAT_REMOVED, SEAT_CHANGED,
CONTACT_CHANGED, PAYMENT_CHANGED, SCHEDULE_CHANGE, NO_SIGNIFICANT_CHANGE.
Status codes: HK/KK/TK = confirmed, HX/UN = cancelled/unable.
Inside "some"/"all"/"none", "var" paths are relative to the array item.
Allowed operators ONLY: {", ".join(sorted(OPERATORS))}.

Examples:
- "Alert when the airline changes the schedule" ->
  {{"in": ["SCHEDULE_CHANGE", {{"var": "change.tags"}}]}}
- "A confirmed flight has no seat" ->
  {{"some": [{{"var": "trip.segments"}}, {{"and": [{{"==": [{{"var": "type"}}, "Air"]}},
    {{"in": [{{"var": "status"}}, ["HK", "KK", "TK"]]}}, {{"missing": "seat"}}]}}]}}
- "Schedule moved by more than 30 minutes" ->
  {{"some": [{{"var": "change.schedule_changes"}}, {{"or": [{{">": [{{"var": "delta_minutes"}}, 30]}},
    {{"<": [{{"var": "delta_minutes"}}, -30]}}]}}]}}

Respond with ONLY a JSON object: {{"name": short title, "json_logic": object, "explanation": one sentence in plain English}}.
"""


def _as_text(content) -> str:
    # Chat endpoints return either a string or a list of content blocks ({"type": "text", "text": ...}).
    if isinstance(content, list):
        return "".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    return content or ""


def parse_draft(content) -> dict:
    text = _as_text(content)
    start = text.find("{")
    if start < 0:
        raise ValueError("model did not return a JSON object")
    try:
        # raw_decode stops at the end of the first object, ignoring any commentary after it.
        draft, _ = json.JSONDecoder().raw_decode(text[start:])
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON from model: {exc}") from exc
    if not isinstance(draft, dict):
        raise ValueError("model did not return a JSON object")
    missing = [k for k in ("name", "json_logic", "explanation") if k not in draft]
    if missing:
        raise ValueError(f"missing keys: {missing}")
    validate(draft["json_logic"])
    return {"name": draft["name"], "json_logic": draft["json_logic"], "explanation": draft["explanation"]}
