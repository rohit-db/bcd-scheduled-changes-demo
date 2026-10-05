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
