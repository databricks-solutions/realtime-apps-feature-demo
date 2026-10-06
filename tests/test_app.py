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
        return [{"id": 7, "timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc), "metric": 0.75}]


class AcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class FakePool:
    def __init__(self):
        self.connection = FakeConnection()

    def acquire(self):
        return AcquireContext(self.connection)


class LatestEventsTest(unittest.TestCase):
    def test_uses_bounded_parameterized_query(self):
        pool = FakePool()
        result = asyncio.run(module.latest_events(limit=25, pool=pool))

        self.assertEqual(pool.connection.limit, 25)
        self.assertIn("LIMIT $1", pool.connection.query)
        self.assertEqual(result["events"][0]["id"], 7)
        self.assertEqual(result["events"][0]["metric"], 0.75)
        self.assertTrue(result["events"][0]["timestamp"].endswith("+00:00"))
        self.assertTrue(result["server_ts"].endswith("+00:00"))
