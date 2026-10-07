"""ForeachWriter that applies each processor output row to Lakebase (idempotent statements)."""
import time

import psycopg

from trip_intel.sink_sql import statements as sink_statements

ATTEMPTS = 3
BACKOFF_S = 0.2


class LakebaseSink:
    def __init__(self, dsn: str, connect=psycopg.connect, sleep=time.sleep, statements=sink_statements):
        self.dsn = dsn
        self.conn = None
        self._connect, self._sleep, self._statements = connect, sleep, statements

    def open(self, partition_id, epoch_id):
        self.conn = self._connect(self.dsn, autocommit=False)
        return True

    def process(self, row):
        out = row.asDict()
        # A dropped connection (Lakebase restart, network blip) reconnects and retries the row instead of failing
        # the query, which would restart the stream and replay up to a checkpoint interval. Every statement is
        # idempotent, so a retry after a partial write is safe. SQL errors are not retried.
        for attempt in range(ATTEMPTS):
            try:
                self._write(out)
                return
            except psycopg.OperationalError:
                if attempt == ATTEMPTS - 1:
                    raise
                self._reconnect()
                self._sleep(BACKOFF_S * 2 ** attempt)

    def _write(self, out):
        # Pipeline mode sends all of a row's statements in one network round trip instead of one each,
        # so a busy Lakebase slows the sink far less (one writer fell ~90 s behind without it at 100/s).
        with self.conn.pipeline(), self.conn.cursor() as cur:
            for sql, params in self._statements(out):
                cur.execute(sql, params)
        self.conn.commit()

    def _reconnect(self):
        try:
            self.conn.close()
        except psycopg.Error:
            pass
        self.conn = self._connect(self.dsn, autocommit=False)

    def close(self, error):
        if self.conn is not None:
            if error:
                self.conn.rollback()
            self.conn.close()
        if error:
            raise error
