from __future__ import annotations

import gzip
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DEPLOY = ROOT / "deploy" / "market_collector"
for directory in (SCRIPTS, DEPLOY):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

import backup  # noqa: E402
from backup import BackupMirror  # noqa: E402
from healthcheck import heartbeat_is_fresh  # noqa: E402
from run_monthly_market_collector import (  # noqa: E402
    DAY_MS,
    CampaignWindow,
    GzipRecordStore,
    SupplementalWorker,
    WebSocketSideChannel,
)


class MonthlyMarketCollectorTests(unittest.TestCase):
    def test_campaign_window_is_stable_across_restarts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "campaign.json"
            first = CampaignWindow.load_or_create(
                path, 30, ["BTCUSDT", "ETHUSDT", "SOLUSDT"], now_ms=1_000
            )
            second = CampaignWindow.load_or_create(
                path, 30, ["BTCUSDT", "ETHUSDT", "SOLUSDT"], now_ms=999_000
            )
        self.assertEqual(first, second)
        self.assertEqual(first.ends_at_ms - first.started_at_ms, 30 * DAY_MS)

    def test_gzip_store_appends_readable_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GzipRecordStore(Path(directory))
            store.append("supplemental", "open_interest", "BTCUSDT", [{"value": 1}], 0)
            store.append("supplemental", "open_interest", "BTCUSDT", [{"value": 2}], 0)
            path = next(Path(directory).rglob("*.jsonl.gz"))
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                values = [json.loads(line)["value"] for line in handle]
        self.assertEqual(values, [1, 2])

    def test_backup_copies_with_checksum_and_does_not_delete_orphans(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            destination = Path(directory) / "backup"
            source.mkdir()
            value = source / "segment.jsonl.gz"
            value.write_bytes(b"market-data")
            mirror = BackupMirror(source, destination)
            result = mirror.run_once(now_ms=10_000)
            copied = destination / "mirror" / value.name
            self.assertEqual(result["copied_count"], 1)
            self.assertEqual(copied.read_bytes(), b"market-data")
            value.unlink()
            mirror.run_once(now_ms=20_000)
            self.assertTrue(copied.exists())

    def test_backup_skips_source_that_changes_while_hashing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            destination = Path(directory) / "backup"
            source.mkdir()
            value = source / "live.jsonl.gz"
            value.write_bytes(b"first")
            original_hash = backup._sha256

            def mutate_before_hash(path: Path) -> str:
                if path == value:
                    with path.open("ab") as handle:
                        handle.write(b"-second")
                return original_hash(path)

            mirror = BackupMirror(source, destination)
            with patch("backup._sha256", side_effect=mutate_before_hash):
                result = mirror.run_once(now_ms=10_000)

            self.assertEqual(result["copied_count"], 0)
            self.assertEqual(result["unchanged_or_live_count"], 1)
            self.assertFalse((destination / "mirror" / value.name).exists())

    def test_supplemental_worker_never_blocks_critical_loop(self) -> None:
        started = threading.Event()
        release = threading.Event()

        class SlowSupplemental:
            def __init__(self) -> None:
                self.calls: list[int] = []

            def poll_due(self, observed_at_ms: int) -> list[dict[str, object]]:
                self.calls.append(observed_at_ms)
                started.set()
                release.wait(2.0)
                return [{"observed_at_ms": observed_at_ms}]

        collector = SlowSupplemental()
        worker = SupplementalWorker(collector)  # type: ignore[arg-type]
        try:
            before = time.perf_counter()
            self.assertEqual(worker.poll(1_000), [])
            self.assertTrue(started.wait(1.0))
            self.assertEqual(worker.poll(2_000), [])
            self.assertLess(time.perf_counter() - before, 0.5)
            release.set()
            deadline = time.monotonic() + 1.0
            while worker.future is not None and not worker.future.done():
                if time.monotonic() >= deadline:
                    self.fail("Supplemental worker did not complete")
                time.sleep(0.01)
            self.assertEqual(worker.poll(3_000), [{"observed_at_ms": 1_000}])
            self.assertEqual(collector.calls, [1_000])
        finally:
            release.set()
            worker.close()

    def test_healthcheck_rejects_stale_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "status.json"
            path.write_text('{"heartbeat_ms": 1000}', encoding="utf-8")
            self.assertTrue(heartbeat_is_fresh(path, 2, now_ms=2_000))
            self.assertFalse(heartbeat_is_fresh(path, 2, now_ms=4_000))

    def test_websocket_event_identity_handles_liquidation(self) -> None:
        channel = WebSocketSideChannel(
            "wss://fstream.binance.com",
            ["BTCUSDT"],
            ["forceOrder"],
            GzipRecordStore(Path(tempfile.gettempdir())),
            1,
            1,
        )
        self.assertEqual(channel.url, "wss://fstream.binance.com/market/stream")
        stream, symbol, timestamp = WebSocketSideChannel._record_identity(
            {
                "received_at_ms": 123,
                "message": {
                    "stream": "btcusdt@forceOrder",
                    "data": {"o": {"s": "BTCUSDT"}},
                },
            }
        )
        self.assertEqual((stream, symbol, timestamp), ("btcusdt_forceOrder", "BTCUSDT", 123))


if __name__ == "__main__":
    unittest.main()
