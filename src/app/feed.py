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


class FeedBroadcaster:
    """One database poll per tick shared by every open browser tab (fan-out to per-tab queues).

    Without this, each tab's SSE stream ran its own query five times a second and a few tabs could
    exhaust the connection pool.
    """

    def __init__(self, fetch, start: int, overlap: int = 50, queue_size: int = 500):
        self._fetch = fetch                      # fetch(since_id) -> list[row dict], runs in a thread
        self._cursor = FeedCursor(start=start, overlap=overlap)
        self._subscribers: set = set()
        self._queue_size = queue_size

    def subscribe(self):
        import asyncio

        q = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q) -> None:
        self._subscribers.discard(q)

    async def tick(self) -> None:
        import asyncio

        if not self._subscribers:
            return
        rows = await asyncio.to_thread(self._fetch, self._cursor.since())
        for row in self._cursor.accept(rows):
            for q in list(self._subscribers):
                if not q.full():                 # a stalled tab drops events instead of blocking others
                    q.put_nowait(row)


class TTLValue:
    """Compute a value at most once per ttl_seconds; concurrent callers share the cached result."""

    def __init__(self, compute, ttl_seconds: float = 1.5, clock=None):
        import threading
        import time

        self._compute, self._ttl = compute, ttl_seconds
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._value, self._at = None, None

    def get(self):
        with self._lock:
            now = self._clock()
            if self._at is None or now - self._at >= self._ttl:
                self._value, self._at = self._compute(), now
            return self._value
