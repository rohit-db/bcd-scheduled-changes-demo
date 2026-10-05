"""A value refreshed in the background at most every ttl_seconds; get() never blocks on the fetch.

Used for the batch-lane freshness KPI, which needs a SQL warehouse query that can take tens of
seconds on a cold warehouse — far longer than the 2-second KPI poll.
"""
import threading
import time


def _thread(fn):
    threading.Thread(target=fn, daemon=True).start()


class BackgroundTTL:
    def __init__(self, fetch, ttl_seconds: float = 60, clock=time.monotonic, spawn=_thread):
        self._fetch, self._ttl, self._clock, self._spawn = fetch, ttl_seconds, clock, spawn
        self._value = None
        self._fetched_at = None
        self._refreshing = False
        self._lock = threading.Lock()

    def _refresh(self):
        try:
            self._value = self._fetch()
        except Exception:
            pass  # keep the last good value
        finally:
            self._fetched_at = self._clock()
            self._refreshing = False

    def get(self):
        with self._lock:
            stale = self._fetched_at is None or self._clock() - self._fetched_at >= self._ttl
            start = stale and not self._refreshing
            if start:
                self._refreshing = True
        if start:
            self._spawn(self._refresh)
        return self._value
