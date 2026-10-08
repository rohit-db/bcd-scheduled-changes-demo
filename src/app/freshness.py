"""A value refreshed in the background at most every ttl_seconds; get() never blocks on the fetch.

Used for the batch-lane freshness KPI, which needs a SQL warehouse query that can take tens of
seconds on a cold warehouse — far longer than the 2-second KPI poll.
"""
import sys
import threading
import time


def _thread(fn):
    threading.Thread(target=fn, daemon=True).start()


class BackgroundTTL:
    def __init__(self, fetch, ttl_seconds: float = 60, clock=time.monotonic, spawn=_thread,
                 max_refresh_seconds: float = 180):
        self._fetch, self._ttl, self._clock, self._spawn = fetch, ttl_seconds, clock, spawn
        self._max_refresh = max_refresh_seconds
        self._value = None
        self._fetched_at = None
        self._refresh_started = None   # set while a refresh is running
        self._lock = threading.Lock()

    def _refresh(self):
        try:
            self._value = self._fetch()
        except Exception as e:  # keep the last good value, but say why it's stale
            print(f"BackgroundTTL refresh failed: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        finally:
            self._fetched_at = self._clock()
            self._refresh_started = None

    def get(self):
        with self._lock:
            now = self._clock()
            stale = self._fetched_at is None or now - self._fetched_at >= self._ttl
            # A refresh that never returns (hung network call) must not block refreshes forever.
            running = self._refresh_started is not None and now - self._refresh_started < self._max_refresh
            start = stale and not running
            if start:
                self._refresh_started = now
        if start:
            self._spawn(self._refresh)
        return self._value
