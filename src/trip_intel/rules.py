"""Business rules over trip versions: evaluation context, edge-triggered firing, hot-reload cache.

A rule fires when its JSON Logic is true for the new version and was false for the previous one,
so "segment without a seat" fires once — not on every subsequent GDS re-save.
"""
import time
from dataclasses import dataclass
from typing import Callable

from trip_intel.jsonlogic import apply, truthy


@dataclass(frozen=True)
class Rule:
    rule_id: str
    name: str
    logic: dict
    action: dict


def context(snapshot: dict | None, change: dict) -> dict:
    return {"trip": snapshot or {}, "change": change}


def newly_matched(rules: list[Rule], prev_ctx: dict, new_ctx: dict) -> tuple[list[Rule], list[dict]]:
    fired, errors = [], []
    for rule in rules:
        try:
            now_true = truthy(apply(rule.logic, new_ctx))
            was_true = truthy(apply(rule.logic, prev_ctx)) if prev_ctx["trip"] else False
        except Exception as exc:  # a bad rule must never stop the stream
            errors.append({"rule_id": rule.rule_id, "error": str(exc)})
            continue
        if now_true and not was_true:
            fired.append(rule)
    return fired, errors


class RuleCache:
    def __init__(self, loader: Callable[[], list[Rule]], ttl_seconds: float = 5.0, clock=time.monotonic):
        self._loader, self._ttl, self._clock = loader, ttl_seconds, clock
        self._rules: list[Rule] = []
        self._loaded_at: float | None = None

    def get(self) -> list[Rule]:
        now = self._clock()
        if self._loaded_at is None or now - self._loaded_at >= self._ttl:
            try:
                self._rules = self._loader()
            except Exception:
                pass  # keep serving the last good rule set
            self._loaded_at = now
        return self._rules
