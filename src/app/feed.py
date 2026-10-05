"""Cursor for the live SSE feed over change_events.

Two sink writers can commit bigserial ids out of order (104 may become visible after 105), so the feed
re-reads a small overlap window behind its high-water mark and de-duplicates by id instead of trusting
`id > last`.
"""
from collections import deque


class FeedCursor:
    def __init__(self, start: int, overlap: int = 50, remember: int = 2000):
        self._last = start
        self._overlap = overlap
        self._seen: set[int] = set()
        self._order: deque[int] = deque()
        self._remember = remember

    def since(self) -> int:
        return self._last - self._overlap

    def accept(self, rows: list[dict]) -> list[dict]:
        fresh = []
        for row in rows:
            rid = row["id"]
            if rid in self._seen or rid <= self.since():
                continue
            fresh.append(row)
            self._seen.add(rid)
            self._order.append(rid)
            self._last = max(self._last, rid)
        while len(self._order) > self._remember:
            self._seen.discard(self._order.popleft())
        return fresh
