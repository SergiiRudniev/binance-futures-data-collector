from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import gzip
import json
import os
import queue
import shutil
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

UTC = timezone.utc

try:
    import websocket
except ModuleNotFoundError:  # Installed by the collector image, optional for research-only installs.
    websocket = None

from collect_binance_microstructure_5s import (
    BinanceFuturesRestClient,
    MicrostructureCollector,
    _seconds_to_next_boundary,
)


SCHEMA_VERSION = "binance_futures_monthly_v1"
DAY_MS = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_date(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC).date().isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


class GzipRecordStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = threading.Lock()

    def append(
        self,
        category: str,
        dataset: str,
        symbol: str,
        records: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> None:
        if not records:
            return
        path = (
            self.root
            / category
            / dataset
            / _utc_date(timestamp_ms)
            / f"{symbol.upper()}.jsonl.gz"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, gzip.open(path, "at", encoding="utf-8", newline="") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")


@dataclass(frozen=True)
class CampaignWindow:
    started_at_ms: int
    ends_at_ms: int
    days: int

    @classmethod
    def load_or_create(
        cls,
        path: Path,
        days: int,
        symbols: list[str],
        now_ms: int | None = None,
    ) -> "CampaignWindow":
        if path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != SCHEMA_VERSION:
                raise RuntimeError("Campaign state schema mismatch")
            if payload.get("symbols") != symbols:
                raise RuntimeError("Campaign symbols differ from the existing state")
            return cls(
                int(payload["started_at_ms"]),
                int(payload["ends_at_ms"]),
                int(payload["days"]),
            )
        started = _now_ms() if now_ms is None else int(now_ms)
        window = cls(started, started + int(days) * DAY_MS, int(days))
        _atomic_json(
            path,
            {
                "schema_version": SCHEMA_VERSION,
                "symbols": symbols,
                "days": int(days),
                "started_at_ms": window.started_at_ms,
                "started_at_utc": datetime.fromtimestamp(started / 1000, tz=UTC).isoformat(),
                "ends_at_ms": window.ends_at_ms,
                "ends_at_utc": datetime.fromtimestamp(
                    window.ends_at_ms / 1000, tz=UTC
                ).isoformat(),
            },
        )
        return window


class SupplementalCollector:
    def __init__(
        self,
        base_url: str,
        symbols: list[str],
        store: GzipRecordStore,
        timeout_seconds: float,
        retries: int,
        cadences: dict[str, int],
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.symbols = symbols
        self.store = store
        self.timeout_seconds = float(timeout_seconds)
        self.retries = int(retries)
        self.cadences = cadences
        self.last_run: dict[str, float] = {}
        self.session = requests.Session()

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        error: Exception | None = None
        for attempt in range(self.retries):
            try:
                response = self.session.get(
                    f"{self.base_url}{path}",
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                error = exc
                if attempt + 1 < self.retries:
                    time.sleep(min(2.0, 0.25 * (2**attempt)))
        raise RuntimeError(f"Supplemental request failed: {path} {params}") from error

    @staticmethod
    def _specs(group: str) -> list[tuple[str, str, dict[str, Any]]]:
        if group == "fast":
            return [
                ("premium_index", "/fapi/v1/premiumIndex", {}),
                ("open_interest", "/fapi/v1/openInterest", {}),
                ("ticker_24h", "/fapi/v1/ticker/24hr", {}),
            ]
        if group == "minute":
            return [
                ("klines_1m", "/fapi/v1/klines", {"interval": "1m", "limit": 3}),
                (
                    "mark_price_klines_1m",
                    "/fapi/v1/markPriceKlines",
                    {"interval": "1m", "limit": 3},
                ),
                (
                    "premium_index_klines_1m",
                    "/fapi/v1/premiumIndexKlines",
                    {"interval": "1m", "limit": 3},
                ),
                (
                    "index_price_klines_1m",
                    "/fapi/v1/indexPriceKlines",
                    {"interval": "1m", "limit": 3},
                ),
                (
                    "continuous_klines_1m",
                    "/fapi/v1/continuousKlines",
                    {"contractType": "PERPETUAL", "interval": "1m", "limit": 3},
                ),
            ]
        if group == "slow":
            return [
                ("funding_rate", "/fapi/v1/fundingRate", {"limit": 20}),
                (
                    "open_interest_history",
                    "/futures/data/openInterestHist",
                    {"period": "5m", "limit": 3},
                ),
                (
                    "global_long_short_ratio",
                    "/futures/data/globalLongShortAccountRatio",
                    {"period": "5m", "limit": 3},
                ),
                (
                    "top_long_short_account_ratio",
                    "/futures/data/topLongShortAccountRatio",
                    {"period": "5m", "limit": 3},
                ),
                (
                    "top_long_short_position_ratio",
                    "/futures/data/topLongShortPositionRatio",
                    {"period": "5m", "limit": 3},
                ),
                (
                    "taker_long_short_ratio",
                    "/futures/data/takerlongshortRatio",
                    {"period": "5m", "limit": 3},
                ),
                (
                    "basis",
                    "/futures/data/basis",
                    {"contractType": "PERPETUAL", "period": "5m", "limit": 3},
                ),
            ]
        if group == "metadata":
            return [("exchange_info", "/fapi/v1/exchangeInfo", {})]
        raise ValueError(f"Unknown supplemental group: {group}")

    def poll_due(self, observed_at_ms: int) -> list[dict[str, Any]]:
        errors: list[dict[str, Any]] = []
        monotonic = time.monotonic()
        for group, cadence in self.cadences.items():
            if monotonic - self.last_run.get(group, -float("inf")) < cadence:
                continue
            self.last_run[group] = monotonic
            for dataset, path, common_params in self._specs(group):
                if group == "metadata":
                    targets: list[str | None] = [None]
                else:
                    targets = list(self.symbols)
                for symbol in targets:
                    params = dict(common_params)
                    if symbol is not None:
                        if dataset in {
                            "basis",
                            "index_price_klines_1m",
                            "continuous_klines_1m",
                        }:
                            params["pair"] = symbol
                        else:
                            params["symbol"] = symbol
                    try:
                        payload = self._get(path, params)
                        record = {
                            "observed_at_ms": int(observed_at_ms),
                            "endpoint": path,
                            "params": params,
                            "payload": payload,
                        }
                        self.store.append(
                            "supplemental",
                            dataset,
                            symbol or "ALL",
                            [record],
                            observed_at_ms,
                        )
                    except Exception as exc:
                        errors.append(
                            {
                                "group": group,
                                "dataset": dataset,
                                "symbol": symbol,
                                "error": repr(exc),
                            }
                        )
        return errors


class SupplementalWorker:
    """Keep slower supplemental REST calls off the critical 5s loop."""

    def __init__(self, collector: SupplementalCollector) -> None:
        self.collector = collector
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="supplemental"
        )
        self.future: Future[list[dict[str, Any]]] | None = None

    def poll(self, observed_at_ms: int) -> list[dict[str, Any]]:
        completed: list[dict[str, Any]] = []
        if self.future is not None:
            if not self.future.done():
                return completed
            try:
                completed = self.future.result()
            except Exception as exc:
                completed = [
                    {
                        "group": "worker",
                        "dataset": "supplemental",
                        "symbol": None,
                        "error": repr(exc),
                    }
                ]
        self.future = self.executor.submit(
            self.collector.poll_due, int(observed_at_ms)
        )
        return completed

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=False)


class WebSocketSideChannel:
    def __init__(
        self,
        base_url: str,
        symbols: list[str],
        stream_suffixes: list[str],
        store: GzipRecordStore,
        queue_size: int,
        flush_seconds: float,
    ) -> None:
        self.streams = [
            f"{symbol.lower()}@{suffix}"
            for symbol in symbols
            for suffix in stream_suffixes
        ]
        self.url = f"{base_url.rstrip('/')}/market/stream"
        self.store = store
        self.flush_seconds = float(flush_seconds)
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=int(queue_size))
        self.stop_event = threading.Event()
        self.reader: threading.Thread | None = None
        self.writer: threading.Thread | None = None
        self._lock = threading.Lock()
        self._stats = {
            "messages": 0,
            "dropped": 0,
            "reconnects": 0,
            "last_message_ms": None,
            "last_error": None,
            "connected": False,
        }

    def _update(self, **values: Any) -> None:
        with self._lock:
            self._stats.update(values)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._stats)

    def start(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for the side channel")
        self.reader = threading.Thread(target=self._reader_loop, name="market-ws", daemon=True)
        self.writer = threading.Thread(target=self._writer_loop, name="market-ws-writer", daemon=True)
        self.reader.start()
        self.writer.start()

    def stop(self) -> None:
        self.stop_event.set()
        for thread in (self.reader, self.writer):
            if thread is not None:
                thread.join(timeout=10)

    def _reader_loop(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            connection = None
            try:
                connection = websocket.create_connection(
                    self.url,
                    timeout=30,
                    enable_multithread=True,
                )
                connection.send(
                    json.dumps({"method": "SUBSCRIBE", "params": self.streams, "id": 1})
                )
                self._update(connected=True, last_error=None)
                backoff = 1.0
                while not self.stop_event.is_set():
                    message = connection.recv()
                    if not message:
                        continue
                    timestamp_ms = _now_ms()
                    payload = json.loads(message)
                    if payload.get("result") is None and "id" in payload:
                        continue
                    if "data" not in payload:
                        event = payload.get("e")
                        symbol = payload.get("s")
                        if event == "forceOrder":
                            symbol = payload.get("o", {}).get("s")
                        suffix = {
                            "markPriceUpdate": "markPrice@1s",
                            "forceOrder": "forceOrder",
                        }.get(event)
                        if not symbol or not suffix:
                            continue
                        payload = {
                            "stream": f"{str(symbol).lower()}@{suffix}",
                            "data": payload,
                        }
                    record = {
                        "received_at_ms": timestamp_ms,
                        "message": payload,
                    }
                    try:
                        self.queue.put_nowait(record)
                    except queue.Full:
                        with self._lock:
                            self._stats["dropped"] += 1
                    with self._lock:
                        self._stats["messages"] += 1
                        self._stats["last_message_ms"] = timestamp_ms
            except Exception as exc:
                with self._lock:
                    self._stats["connected"] = False
                    self._stats["last_error"] = repr(exc)
                    self._stats["reconnects"] += 1
                self.stop_event.wait(backoff)
                backoff = min(60.0, backoff * 2.0)
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass

    @staticmethod
    def _record_identity(record: dict[str, Any]) -> tuple[str, str, int]:
        wrapper = record["message"]
        stream = str(wrapper.get("stream", "unknown")).replace("@", "_")
        data = wrapper.get("data", {})
        symbol = str(data.get("s") or data.get("o", {}).get("s") or "UNKNOWN")
        return stream, symbol, int(record["received_at_ms"])

    def _writer_loop(self) -> None:
        while not self.stop_event.is_set() or not self.queue.empty():
            records: list[dict[str, Any]] = []
            try:
                records.append(self.queue.get(timeout=self.flush_seconds))
            except queue.Empty:
                continue
            deadline = time.monotonic() + self.flush_seconds
            while time.monotonic() < deadline and len(records) < 5000:
                try:
                    records.append(self.queue.get_nowait())
                except queue.Empty:
                    break
            groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
            timestamps: dict[tuple[str, str, str], int] = {}
            for record in records:
                stream, symbol, timestamp_ms = self._record_identity(record)
                key = (stream, symbol, _utc_date(timestamp_ms))
                groups.setdefault(key, []).append(record)
                timestamps[key] = timestamp_ms
            for (stream, symbol, _), rows in groups.items():
                self.store.append(
                    "websocket", stream, symbol, rows, timestamps[(stream, symbol, _)]
                )


def _status_payload(
    campaign: CampaignWindow,
    output_dir: Path,
    status: str,
    successful_buckets: int,
    failed_attempts: int,
    last_success_ms: int | None,
    consecutive_failures: int,
    last_error: str | None,
    websocket_stats: dict[str, Any] | None,
) -> dict[str, Any]:
    usage = shutil.disk_usage(output_dir)
    return {
        "schema_version": SCHEMA_VERSION,
        "heartbeat_ms": _now_ms(),
        "status": status,
        "campaign_started_at_ms": campaign.started_at_ms,
        "campaign_ends_at_ms": campaign.ends_at_ms,
        "successful_buckets": int(successful_buckets),
        "failed_attempts": int(failed_attempts),
        "consecutive_failures": int(consecutive_failures),
        "last_success_ms": last_success_ms,
        "last_error": last_error,
        "disk_free_bytes": int(usage.free),
        "disk_used_bytes": int(usage.used),
        "websocket": websocket_stats,
    }


def _load_config(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Monthly collector config must be a mapping")
    return payload


def run(config: dict[str, Any]) -> None:
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    symbols = [str(value).upper() for value in config["symbols"]]
    campaign = CampaignWindow.load_or_create(
        output_dir / "campaign.json",
        int(config["campaign_days"]),
        symbols,
    )
    store = GzipRecordStore(output_dir)
    client = BinanceFuturesRestClient(
        str(config["base_url"]),
        float(config["request_timeout_seconds"]),
        int(config["request_retries"]),
    )
    collector = MicrostructureCollector(
        client,
        symbols,
        int(config["interval_seconds"]),
        int(config["depth_limit"]),
        output_dir,
        write=True,
        raw_compression=str(config.get("raw_compression", "gzip")),
        min_free_disk_gb=float(config.get("min_free_disk_gb", 0.0)),
    )
    supplemental_config = config["supplemental"]
    supplemental = SupplementalCollector(
        str(config["base_url"]),
        symbols,
        store,
        float(config["request_timeout_seconds"]),
        int(config["request_retries"]),
        {
            "fast": int(supplemental_config["fast_seconds"]),
            "minute": int(supplemental_config["minute_seconds"]),
            "slow": int(supplemental_config["slow_seconds"]),
            "metadata": int(supplemental_config["metadata_seconds"]),
        },
    )
    supplemental_worker = SupplementalWorker(supplemental)
    websocket_config = config.get("websocket", {})
    side_channel = None
    if bool(websocket_config.get("enabled", True)):
        side_channel = WebSocketSideChannel(
            str(config["websocket_base_url"]),
            symbols,
            list(websocket_config["streams"]),
            store,
            int(websocket_config["queue_size"]),
            float(websocket_config["flush_seconds"]),
        )
        side_channel.start()

    stop_event = threading.Event()

    def request_stop(*_: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    status_path = output_dir / "status" / "collector.json"
    previous_status: dict[str, Any] = {}
    if status_path.is_file():
        try:
            previous_status = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            previous_status = {}
    successful_buckets = int(previous_status.get("successful_buckets", 0))
    failed_attempts = int(previous_status.get("failed_attempts", 0))
    consecutive_failures = 0
    last_success_ms = previous_status.get("last_success_ms")
    if last_success_ms is not None:
        last_success_ms = int(last_success_ms)
    last_error: str | None = None
    outage_started_ms: int | None = None
    try:
        while not stop_event.is_set():
            now_ms = _now_ms()
            if now_ms >= campaign.ends_at_ms:
                _atomic_json(
                    status_path,
                    _status_payload(
                        campaign,
                        output_dir,
                        "completed",
                        successful_buckets,
                        failed_attempts,
                        last_success_ms,
                        consecutive_failures,
                        last_error,
                        side_channel.stats() if side_channel else None,
                    ),
                )
                stop_event.wait(int(config.get("completed_heartbeat_seconds", 60)))
                continue
            try:
                rows = collector.collect_once(now_ms=now_ms)
                observed_ms = _now_ms()
                supplemental_errors = supplemental_worker.poll(observed_ms)
                successful_buckets += 1
                consecutive_failures = 0
                last_success_ms = observed_ms
                last_error = None
                if outage_started_ms is not None:
                    store.append(
                        "operations",
                        "connectivity",
                        "COLLECTOR",
                        [
                            {
                                "event": "recovered",
                                "started_at_ms": outage_started_ms,
                                "recovered_at_ms": observed_ms,
                                "duration_ms": observed_ms - outage_started_ms,
                            }
                        ],
                        observed_ms,
                    )
                    outage_started_ms = None
                if supplemental_errors:
                    store.append(
                        "operations",
                        "supplemental_errors",
                        "COLLECTOR",
                        supplemental_errors,
                        observed_ms,
                    )
                print(
                    json.dumps(
                        {
                            "bucket_end_ms": rows[0]["bucket_end_ms"] if rows else None,
                            "symbols": len(rows),
                            "supplemental_errors": len(supplemental_errors),
                        }
                    ),
                    flush=True,
                )
            except Exception as exc:
                failed_attempts += 1
                consecutive_failures += 1
                last_error = repr(exc)
                if outage_started_ms is None:
                    outage_started_ms = now_ms
                    store.append(
                        "operations",
                        "connectivity",
                        "COLLECTOR",
                        [{"event": "outage_started", "started_at_ms": now_ms, "error": last_error}],
                        now_ms,
                    )
                print(json.dumps({"collector_error": last_error}), flush=True)
            _atomic_json(
                status_path,
                _status_payload(
                    campaign,
                    output_dir,
                    "collecting" if consecutive_failures == 0 else "degraded",
                    successful_buckets,
                    failed_attempts,
                    last_success_ms,
                    consecutive_failures,
                    last_error,
                    side_channel.stats() if side_channel else None,
                ),
            )
            if consecutive_failures:
                stop_event.wait(min(60.0, float(2 ** min(consecutive_failures, 6))))
            else:
                interval = int(config["interval_seconds"])
                stop_event.wait(max(0.05, _seconds_to_next_boundary(time.time(), interval)))
    finally:
        supplemental_worker.close()
        if side_channel is not None:
            side_channel.stop()
        _atomic_json(
            status_path,
            _status_payload(
                campaign,
                output_dir,
                "stopped",
                successful_buckets,
                failed_attempts,
                last_success_ms,
                consecutive_failures,
                last_error,
                side_channel.stats() if side_channel else None,
            ),
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    run(_load_config(args.config))


if __name__ == "__main__":
    main()
