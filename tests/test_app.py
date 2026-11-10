import asyncio
import importlib.util
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path


APP_DIR = Path(__file__).parents[1] / "src" / "app"
sys.path.insert(0, str(APP_DIR))
if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Pool = object
    asyncpg_stub.create_pool = None
    asyncpg_stub.PostgresError = type("PostgresError", (Exception,), {})
    asyncpg_stub.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["asyncpg"] = asyncpg_stub
spec = importlib.util.spec_from_file_location("realtime_app", APP_DIR / "main.py")
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


class FakeConnection:
    def __init__(self):
        self.query = None
        self.limit = None

    async def fetch(self, query, limit):
        self.query = query
        self.limit = limit
        return [{"id": 7, "timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc)}]


class FakePool:
    """Hands out `checkouts` in order; an exception entry simulates a pooled
    connection that fails its pre-ping on acquire."""

    def __init__(self, *checkouts):
        self.checkouts = list(checkouts)
        self.released = []

    async def acquire(self):
        conn = self.checkouts.pop(0)
        if isinstance(conn, Exception):
            raise conn
        return conn

    async def release(self, conn):
        self.released.append(conn)


class LatestEventsTest(unittest.TestCase):
    def test_uses_bounded_parameterized_query(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        result = asyncio.run(module.latest_events(limit=25, pool=pool))

        self.assertEqual(conn.limit, 25)
        self.assertIn("LIMIT $1", conn.query)
        self.assertIn("ORDER BY timestamp DESC, id DESC", conn.query)
        self.assertEqual(result["events"][0]["id"], 7)
        self.assertNotIn("metric", result["events"][0])
        self.assertTrue(result["events"][0]["timestamp"].endswith("+00:00"))
        self.assertTrue(result["server_ts"].endswith("+00:00"))
        self.assertEqual(pool.released, [conn])

    def test_retries_once_when_pooled_connection_is_dead(self):
        conn = FakeConnection()
        pool = FakePool(ConnectionResetError("connection reset by peer"), conn)
        result = asyncio.run(module.latest_events(limit=25, pool=pool))

        self.assertEqual(result["events"][0]["id"], 7)
        self.assertEqual(pool.released, [conn])
