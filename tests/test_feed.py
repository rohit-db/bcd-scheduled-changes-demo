from app.feed import FeedCursor


def test_late_committed_ids_inside_overlap_are_still_delivered():
    cur = FeedCursor(start=100, overlap=20)
    assert [r["id"] for r in cur.accept([{"id": 101}, {"id": 103}])] == [101, 103]
    assert cur.since() == 83                       # re-reads an overlap window behind the high-water mark
    # id 102 committed after 103 was read: it must still be delivered, 101/103 must not repeat.
    assert [r["id"] for r in cur.accept([{"id": 101}, {"id": 102}, {"id": 103}, {"id": 104}])] == [102, 104]


def test_cursor_never_moves_backwards_and_memory_is_bounded():
    cur = FeedCursor(start=0, overlap=5, remember=10)
    for i in range(1, 100):
        cur.accept([{"id": i}])
    assert cur.since() == 94
    assert len(cur._seen) <= 10


def test_broadcaster_queries_once_per_tick_for_all_subscribers():
    import asyncio

    from app.feed import FeedBroadcaster

    calls = []

    def fetch(since):
        calls.append(since)
        return [{"id": 11}, {"id": 12}]

    async def scenario():
        b = FeedBroadcaster(fetch, start=10, overlap=5)
        q1, q2 = b.subscribe(), b.subscribe()
        await b.tick()
        await b.tick()                      # same rows again: deduped, nothing new delivered
        b.unsubscribe(q2)
        return [q1.get_nowait()["id"], q1.get_nowait()["id"]], q1.qsize(), q2.qsize()

    got, q1_left, q2_left = asyncio.run(scenario())
    assert got == [11, 12] and q1_left == 0 and q2_left == 2
    assert len(calls) == 2                  # one query per tick, not one per subscriber


def test_broadcaster_skips_query_with_no_subscribers():
    import asyncio

    from app.feed import FeedBroadcaster

    calls = []
    b = FeedBroadcaster(lambda since: calls.append(since) or [], start=0)
    asyncio.run(b.tick())
    assert calls == []


def test_ttl_value_reuses_result_within_ttl():
    from app.feed import TTLValue

    now, calls = [0.0], []
    v = TTLValue(lambda: calls.append(1) or len(calls), ttl_seconds=1.5, clock=lambda: now[0])
    assert v.get() == 1 and v.get() == 1
    now[0] = 2.0
    assert v.get() == 2
