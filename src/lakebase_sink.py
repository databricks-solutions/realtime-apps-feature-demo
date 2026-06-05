# LakebaseSink — Structured Streaming ForeachWriter for Lakebase Postgres.
#
# Source: realtime-app skill (lakebase-python-sink.md).
#
# Buffers up to BUFFER_SIZE rows in process(), flushes on full or close().
# Uses INSERT ... ON CONFLICT for upsert semantics. One transaction per flush.
#
# Authenticates with Postgres password (NOT OAuth) — required for executor-side
# custom sinks; the SDK's OAuth path does not work in ForeachWriter context.

import time

import psycopg


# PySpark's `foreach()` accepts any object that implements the
# `open(partition_id, epoch_id) -> bool`, `process(row)`, `close(error)`
# lifecycle. There is no exported base class to extend — the
# `pyspark.sql.streaming.ForeachWriter` symbol the skill references is a Scala
# class, not a Python one. Duck-typing is the documented Python pattern.
class LakebaseSink:
    def __init__(self, host: str, database: str, user: str, password: str,
                 table: str, port: int = 5432, key_col: str = "id",
                 max_dwell_ms: int = 10):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.port = port
        self.key_col = key_col
        # max_dwell_ms is the minimum time between commits. Each process(row)
        # call appends to the in-memory buffer and flushes only if at least
        # this many ms have passed since the last flush. The effective
        # Lakebase TX/s ceiling is 1000 / max_dwell_ms; at higher source
        # rates more rows accumulate per flush. Caveat: because the check
        # only fires on row arrival, at sustained low rates (rate <
        # 1000/max_dwell_ms) a row can wait up to one inter-arrival time
        # for the next row to trigger its flush — fine for steady sources.
        self.max_dwell_ms = max_dwell_ms

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
        self.last_flush_ms = self._now_ms()
        return True

    def process(self, row):
        self.buffer.append(row)
        if self._now_ms() - self.last_flush_ms >= self.max_dwell_ms:
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
            self.last_flush_ms = self._now_ms()

    @staticmethod
    def _now_ms():
        return int(time.monotonic() * 1000)
