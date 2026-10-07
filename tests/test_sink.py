import psycopg
import pytest

from rtm.lakebase import conninfo
from rtm.sink import LakebaseSink

ROW = {"trip_id": "T1", "version": 2}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        if self.conn.fail_executes > 0:
            self.conn.fail_executes -= 1
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        self.conn.executed.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, fail_executes=0):
        self.fail_executes = fail_executes
        self.executed, self.commits, self.closed = [], 0, False

    def pipeline(self):
        return self

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeRow:
    def asDict(self):
        return ROW


def make_sink(conns, statements=lambda out: [("INSERT 1", ()), ("INSERT 2", ())]):
    pending = list(conns)
    sink = LakebaseSink("dsn", connect=lambda dsn, autocommit: pending.pop(0), sleep=lambda s: None,
                        statements=statements)
    sink.open(0, 0)
    return sink


def test_process_reconnects_and_retries_after_a_dropped_connection():
    first, second = FakeConn(fail_executes=1), FakeConn()
    sink = make_sink([first, second])
    sink.process(FakeRow())
    assert first.closed
    assert second.executed == ["INSERT 1", "INSERT 2"] and second.commits == 1


def test_process_gives_up_after_three_attempts():
    conns = [FakeConn(fail_executes=1) for _ in range(3)]
    sink = make_sink(conns)
    with pytest.raises(psycopg.OperationalError):
        sink.process(FakeRow())


def test_process_does_not_retry_sql_errors():
    class BadSql(FakeConn):
        def cursor(self):
            conn = self

            class C(FakeCursor):
                def execute(self, sql, params=None):
                    raise psycopg.errors.SyntaxError("bad sql")
            return C(conn)
    sink = make_sink([BadSql(), FakeConn()])
    with pytest.raises(psycopg.errors.SyntaxError):
        sink.process(FakeRow())


def test_conninfo_sets_search_path_for_a_schema():
    assert "search_path=microbatch" in conninfo("h", "pw", schema="microbatch")
    assert "search_path" not in conninfo("h", "pw")
