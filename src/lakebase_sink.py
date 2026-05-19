# LakebaseSink — Structured Streaming ForeachWriter for Lakebase Postgres.
#
# Source: realtime-app skill (lakebase-python-sink.md).
#
# Buffers up to BUFFER_SIZE rows in process(), flushes on full or close().
# Uses INSERT ... ON CONFLICT for upsert semantics. One transaction per flush.
#
# Authenticates with Postgres password (NOT OAuth) — required for executor-side
# custom sinks; the SDK's OAuth path does not work in ForeachWriter context.

import psycopg


# PySpark's `foreach()` accepts any object that implements the
# `open(partition_id, epoch_id) -> bool`, `process(row)`, `close(error)`
# lifecycle. There is no exported base class to extend — the
# `pyspark.sql.streaming.ForeachWriter` symbol the skill references is a Scala
# class, not a Python one. Duck-typing is the documented Python pattern.
class LakebaseSink:
    def __init__(self, host: str, database: str, user: str, password: str,
                 table: str, port: int = 5432, key_col: str = "id",
                 buffer_size: int = 1):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.port = port
        self.key_col = key_col
        # buffer_size caps how many rows we hold before flushing. The default of
        # 1 (per-row commit) is the right choice for this *latency* demo: the
        # buffer no longer dominates end-to-end lag. Bump to 100+ for throughput
        # demos at >100 rows/sec.
        self.buffer_size = buffer_size

    def open(self, partition_id, epoch_id):
        # Lakebase requires SSL; sslmode is hard-coded rather than parameterised.
        self.conn = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            sslmode="require",
            autocommit=False,
        )
        self.buffer = []
        return True

    def process(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.buffer_size:
            self._flush()

    def close(self, error):
        try:
            if self.buffer:
                self._flush()
        finally:
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def _flush(self):
        if not self.buffer:
            return

        sample = self.buffer[0].asDict()
        cols = list(sample.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(f'"{c}"' for c in cols)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c != self.key_col
        )

        sql = (
            f'INSERT INTO "{self.table}" ({col_list}) '
            f'VALUES ({placeholders}) '
            f'ON CONFLICT ("{self.key_col}") DO UPDATE SET {update_set}'
        )

        try:
            with self.conn.cursor() as cur:
                rows = [tuple(r.asDict()[c] for c in cols) for r in self.buffer]
                cur.executemany(sql, rows)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.buffer.clear()
