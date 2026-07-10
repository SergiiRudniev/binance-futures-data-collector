from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DEPLOY = ROOT / "deploy" / "market_collector"
for directory in (SCRIPTS, DEPLOY):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

from backup import BackupMirror  # noqa: E402
from healthcheck import heartbeat_is_fresh  # noqa: E402
from run_monthly_market_collector import (  # noqa: E402
    DAY_MS,
    CampaignWindow,
    GzipRecordStore,
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
