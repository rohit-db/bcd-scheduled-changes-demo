"""ForeachWriter that applies each processor output row to Lakebase (idempotent statements)."""
import psycopg

from trip_intel.sink_sql import statements


class LakebaseSink:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def open(self, partition_id, epoch_id):
        self.conn = psycopg.connect(self.dsn, autocommit=False)
        return True

    def process(self, row):
        out = row.asDict()
        # Pipeline mode sends all of a row's statements in one network round trip instead of one each,
        # so a busy Lakebase slows the sink far less (one writer fell ~90 s behind without it at 100/s).
        with self.conn.pipeline(), self.conn.cursor() as cur:
            for sql, params in statements(out):
                cur.execute(sql, params)
        self.conn.commit()

    def close(self, error):
        if self.conn is not None:
            if error:
                self.conn.rollback()
            self.conn.close()
        if error:
            raise error
