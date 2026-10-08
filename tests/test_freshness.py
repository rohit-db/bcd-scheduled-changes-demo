from app.freshness import BackgroundTTL


def test_returns_cached_value_and_refreshes_after_ttl():
    now, calls = [0.0], []

    def fetch():
        calls.append(1)
        return len(calls) * 10.0

    cache = BackgroundTTL(fetch, ttl_seconds=60, clock=lambda: now[0], spawn=lambda f: f())
    assert cache.get() == 10.0          # first call fetches (synchronously in this test)
    now[0] = 30
    assert cache.get() == 10.0 and len(calls) == 1
    now[0] = 61
    assert cache.get() == 20.0


def test_fetch_errors_keep_last_value():
    now, state = [0.0], {"fail": False}

    def fetch():
        if state["fail"]:
            raise RuntimeError("warehouse cold")
        return 5.0

    cache = BackgroundTTL(fetch, ttl_seconds=1, clock=lambda: now[0], spawn=lambda f: f())
    assert cache.get() == 5.0
    state["fail"], now[0] = True, 2
    assert cache.get() == 5.0


def test_get_does_not_block_while_refresh_is_pending():
    pending = []
    cache = BackgroundTTL(lambda: 1.0, ttl_seconds=60, spawn=pending.append)
    assert cache.get() is None          # nothing cached yet, refresh queued, no blocking
    assert cache.get() is None and len(pending) == 1   # no duplicate refreshes
    pending[0]()
    assert cache.get() == 1.0


def test_a_hung_refresh_does_not_block_refreshes_forever():
    now, pending = [0.0], []
    cache = BackgroundTTL(lambda: 7.0, ttl_seconds=60, clock=lambda: now[0], spawn=pending.append,
                          max_refresh_seconds=120)
    cache.get()                          # first refresh starts and never finishes (hung warehouse call)
    now[0] = 100
    cache.get()
    assert len(pending) == 1             # still within the allowed refresh time: no duplicate
    now[0] = 121
    cache.get()
    assert len(pending) == 2             # gave up on the hung refresh and started a new one
    pending[1]()
    assert cache.get() == 7.0


def test_fetch_errors_are_reported(capsys):
    def fetch():
        raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

    BackgroundTTL(fetch, ttl_seconds=60, spawn=lambda f: f()).get()
    assert "TABLE_OR_VIEW_NOT_FOUND" in capsys.readouterr().err
