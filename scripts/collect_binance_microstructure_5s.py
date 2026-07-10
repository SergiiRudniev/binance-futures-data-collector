from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import requests
import yaml


SCHEMA_VERSION = "binance_futures_microstructure_5s_v3"
CSV_FIELDS = (
    "schema_version",
    "symbol",
    "bucket_start_ms",
    "bucket_end_ms",
    "collected_at_ms",
    "agg_trade_count",
    "first_agg_id",
    "last_agg_id",
    "requested_from_agg_id",
    "first_fetched_agg_id",
    "last_fetched_agg_id",
    "agg_id_discontinuity_count",
    "request_start_id_delta",
    "trade_time_gap_ms",
    "collector_recovery",
    "taker_buy_base_volume",
    "taker_sell_base_volume",
    "taker_buy_quote_volume",
    "taker_sell_quote_volume",
    "trade_flow_imbalance",
    "trade_vwap",
    "trade_first_price",
    "trade_last_price",
    "trade_high_price",
    "trade_low_price",
    "trade_price_velocity_bps_per_second",
    "best_bid",
    "best_ask",
    "best_bid_qty",
    "best_ask_qty",
    "mid_price",
    "microprice",
    "spread_bps",
    "bid_depth_5",
    "ask_depth_5",
    "bid_depth_20",
    "ask_depth_20",
    "bid_notional_5",
    "ask_notional_5",
    "book_imbalance_5",
    "book_imbalance_20",
    "book_slope_imbalance",
    "order_flow_imbalance_top",
    "mid_return_bps",
    "depth_last_update_id",
    "depth_non_monotonic",
    "request_latency_ms",
)


@dataclass
class SymbolState:
    last_agg_id: int | None = None
    last_trade_time_ms: int | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_qty: float | None = None
    best_ask_qty: float | None = None
    mid_price: float | None = None
    depth_last_update_id: int | None = None


class MarketDataClient(Protocol):
    def agg_trades(self, symbol: str, from_id: int | None) -> list[dict[str, Any]]: ...

    def book_ticker(self, symbol: str) -> dict[str, Any]: ...

    def depth(self, symbol: str, limit: int) -> dict[str, Any]: ...


class BinanceFuturesRestClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        retries: int,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self.retries = int(retries)
        self.session = session or requests.Session()

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
                    time.sleep(0.25 * (2**attempt))
        raise RuntimeError(f"Binance request failed: {path} {params}") from error

    def agg_trades(self, symbol: str, from_id: int | None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
        if from_id is not None:
            params["fromId"] = int(from_id)
        rows: list[dict[str, Any]] = []
        while True:
            page = self._get("/fapi/v1/aggTrades", params)
            if not isinstance(page, list):
                raise RuntimeError("Unexpected aggTrades response")
            rows.extend(page)
            if len(page) < 1000:
                break
            params["fromId"] = int(page[-1]["a"]) + 1
            if len(rows) >= 10_000:
                break
        return rows

    def book_ticker(self, symbol: str) -> dict[str, Any]:
        result = self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        if not isinstance(result, dict):
            raise RuntimeError("Unexpected bookTicker response")
        return result

    def depth(self, symbol: str, limit: int) -> dict[str, Any]:
        result = self._get(
            "/fapi/v1/depth", {"symbol": symbol, "limit": int(limit)}
        )
        if not isinstance(result, dict):
            raise RuntimeError("Unexpected depth response")
        return result


def _float(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Non-finite market value: {value}")
    return number


def _bucket_bounds(now_ms: int, interval_seconds: int) -> tuple[int, int]:
    width = int(interval_seconds) * 1000
    end = (int(now_ms) // width) * width
    return end - width, end


def _seconds_to_next_boundary(now_seconds: float, interval_seconds: int) -> float:
    interval = int(interval_seconds)
    return interval - (float(now_seconds) % interval) + 0.05


def _atomic_replace(source: Path, target: Path, retries: int = 5) -> None:
    for attempt in range(int(retries)):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt + 1 >= retries:
                raise
            time.sleep(0.05 * (2**attempt))


def _top_depth(levels: list[list[str]], count: int) -> tuple[float, float]:
    selected = levels[:count]
    base = sum(_float(level[1]) for level in selected)
    notional = sum(_float(level[0]) * _float(level[1]) for level in selected)
    return base, notional


def _imbalance(bid: float, ask: float) -> float:
    return (bid - ask) / max(bid + ask, 1e-12)


def _top_of_book_ofi(book: dict[str, Any], previous: SymbolState) -> float:
    if (
        previous.best_bid is None
        or previous.best_ask is None
        or previous.best_bid_qty is None
        or previous.best_ask_qty is None
    ):
        return 0.0
    bid = _float(book["bidPrice"])
    ask = _float(book["askPrice"])
    bid_qty = _float(book["bidQty"])
    ask_qty = _float(book["askQty"])
    bid_event = (
        (bid_qty if bid >= previous.best_bid else 0.0)
        - (previous.best_bid_qty if bid <= previous.best_bid else 0.0)
    )
    ask_event = (
        (ask_qty if ask <= previous.best_ask else 0.0)
        - (previous.best_ask_qty if ask >= previous.best_ask else 0.0)
    )
    return bid_event - ask_event


def aggregate_five_second_snapshot(
    symbol: str,
    bucket_start_ms: int,
    bucket_end_ms: int,
    collected_at_ms: int,
    trades: list[dict[str, Any]],
    book: dict[str, Any],
    depth: dict[str, Any],
    previous: SymbolState,
    request_latency_ms: float,
    requested_from_id: int | None = None,
    collector_recovery: bool = False,
) -> tuple[dict[str, Any], SymbolState]:
    fetched = sorted(trades, key=lambda row: (int(row["a"]), int(row["T"])))
    fetched_ids = [int(row["a"]) for row in fetched]
    discontinuities = sum(
        int(right != left + 1) for left, right in zip(fetched_ids, fetched_ids[1:])
    )
    request_start_delta = (
        fetched_ids[0] - int(requested_from_id)
        if fetched_ids and requested_from_id is not None
        else 0
    )
    trade_time_gap_ms = (
        int(fetched[0]["T"]) - int(previous.last_trade_time_ms)
        if fetched and previous.last_trade_time_ms is not None
        else 0
    )
    local = [
        row
        for row in trades
        if bucket_start_ms <= int(row["T"]) < bucket_end_ms
    ]
    local.sort(key=lambda row: (int(row["T"]), int(row["a"])))
    ids = [int(row["a"]) for row in local]
    buy_base = sum(_float(row["q"]) for row in local if not bool(row["m"]))
    sell_base = sum(_float(row["q"]) for row in local if bool(row["m"]))
    buy_quote = sum(
        _float(row["p"]) * _float(row["q"]) for row in local if not bool(row["m"])
    )
    sell_quote = sum(
        _float(row["p"]) * _float(row["q"]) for row in local if bool(row["m"])
    )
    total_base = buy_base + sell_base
    total_quote = buy_quote + sell_quote
    prices = [_float(row["p"]) for row in local]
    trade_vwap = total_quote / total_base if total_base > 0.0 else 0.0
    velocity = 0.0
    if len(local) >= 2:
        elapsed = max((int(local[-1]["T"]) - int(local[0]["T"])) / 1000.0, 1e-3)
        velocity = (
            math.log(max(prices[-1], 1e-12) / max(prices[0], 1e-12))
            * 10_000.0
            / elapsed
        )

    bid = _float(book["bidPrice"])
    ask = _float(book["askPrice"])
    bid_qty = _float(book["bidQty"])
    ask_qty = _float(book["askQty"])
    mid = 0.5 * (bid + ask)
    microprice = (ask * bid_qty + bid * ask_qty) / max(bid_qty + ask_qty, 1e-12)
    bids = depth.get("bids", [])
    asks = depth.get("asks", [])
    if not bids or not asks:
        raise RuntimeError(f"Depth snapshot is empty for {symbol}")
    bid_depth_5, bid_notional_5 = _top_depth(bids, 5)
    ask_depth_5, ask_notional_5 = _top_depth(asks, 5)
    bid_depth_20, _ = _top_depth(bids, 20)
    ask_depth_20, _ = _top_depth(asks, 20)
    bid_near = _imbalance(bid_depth_5, bid_depth_20 - bid_depth_5)
    ask_near = _imbalance(ask_depth_5, ask_depth_20 - ask_depth_5)
    depth_update = int(depth["lastUpdateId"])
    depth_non_monotonic = int(
        previous.depth_last_update_id is not None
        and depth_update <= previous.depth_last_update_id
    )
    row = {
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol,
        "bucket_start_ms": int(bucket_start_ms),
        "bucket_end_ms": int(bucket_end_ms),
        "collected_at_ms": int(collected_at_ms),
        "agg_trade_count": len(local),
        "first_agg_id": ids[0] if ids else "",
        "last_agg_id": ids[-1] if ids else "",
        "requested_from_agg_id": (
            int(requested_from_id) if requested_from_id is not None else ""
        ),
        "first_fetched_agg_id": fetched_ids[0] if fetched_ids else "",
        "last_fetched_agg_id": fetched_ids[-1] if fetched_ids else "",
        "agg_id_discontinuity_count": int(discontinuities),
        "request_start_id_delta": int(request_start_delta),
        "trade_time_gap_ms": int(trade_time_gap_ms),
        "collector_recovery": int(collector_recovery),
        "taker_buy_base_volume": buy_base,
        "taker_sell_base_volume": sell_base,
        "taker_buy_quote_volume": buy_quote,
        "taker_sell_quote_volume": sell_quote,
        "trade_flow_imbalance": _imbalance(buy_base, sell_base),
        "trade_vwap": trade_vwap,
        "trade_first_price": prices[0] if prices else 0.0,
        "trade_last_price": prices[-1] if prices else 0.0,
        "trade_high_price": max(prices) if prices else 0.0,
        "trade_low_price": min(prices) if prices else 0.0,
        "trade_price_velocity_bps_per_second": velocity,
        "best_bid": bid,
        "best_ask": ask,
        "best_bid_qty": bid_qty,
        "best_ask_qty": ask_qty,
        "mid_price": mid,
        "microprice": microprice,
        "spread_bps": (ask - bid) / max(mid, 1e-12) * 10_000.0,
        "bid_depth_5": bid_depth_5,
        "ask_depth_5": ask_depth_5,
        "bid_depth_20": bid_depth_20,
        "ask_depth_20": ask_depth_20,
        "bid_notional_5": bid_notional_5,
        "ask_notional_5": ask_notional_5,
        "book_imbalance_5": _imbalance(bid_depth_5, ask_depth_5),
        "book_imbalance_20": _imbalance(bid_depth_20, ask_depth_20),
        "book_slope_imbalance": bid_near - ask_near,
        "order_flow_imbalance_top": _top_of_book_ofi(book, previous),
        "mid_return_bps": (
            math.log(mid / previous.mid_price) * 10_000.0
            if previous.mid_price is not None and previous.mid_price > 0.0
            else 0.0
        ),
        "depth_last_update_id": depth_update,
        "depth_non_monotonic": depth_non_monotonic,
        "request_latency_ms": float(request_latency_ms),
    }
    state = SymbolState(
        last_agg_id=(fetched_ids[-1] if fetched_ids else previous.last_agg_id),
        last_trade_time_ms=(
            max(int(row["T"]) for row in fetched)
            if fetched
            else previous.last_trade_time_ms
        ),
        best_bid=bid,
        best_ask=ask,
        best_bid_qty=bid_qty,
        best_ask_qty=ask_qty,
        mid_price=mid,
        depth_last_update_id=depth_update,
    )
    return row, state


class MicrostructureCollector:
    def __init__(
        self,
        client: MarketDataClient,
        symbols: list[str],
        interval_seconds: int,
        depth_limit: int,
        output_dir: Path,
        write: bool = True,
        raw_compression: str = "none",
        min_free_disk_gb: float = 0.0,
    ) -> None:
        self.client = client
        self.symbols = [symbol.upper() for symbol in symbols]
        self.interval_seconds = int(interval_seconds)
        self.depth_limit = int(depth_limit)
        self.output_dir = output_dir
        self.write = bool(write)
        if raw_compression not in {"none", "gzip"}:
            raise ValueError("raw_compression must be 'none' or 'gzip'")
        if float(min_free_disk_gb) < 0.0:
            raise ValueError("min_free_disk_gb cannot be negative")
        self.raw_compression = raw_compression
        self.min_free_disk_bytes = int(float(min_free_disk_gb) * 1024**3)
        self.state_path = output_dir / "collector_state.json"
        self.states = self._load_state() if write else {
            symbol: SymbolState() for symbol in self.symbols
        }

    def _load_state(self) -> dict[str, SymbolState]:
        if not self.state_path.exists():
            return {symbol: SymbolState() for symbol in self.symbols}
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise RuntimeError("Microstructure collector state schema mismatch")
        return {
            symbol: SymbolState(**payload.get("symbols", {}).get(symbol, {}))
            for symbol in self.symbols
        }

    def _save_state(self) -> None:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "updated_at_ms": int(time.time() * 1000),
            "symbols": {symbol: asdict(state) for symbol, state in self.states.items()},
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        _atomic_replace(temporary, self.state_path)

    def _existing_storage_root(self) -> Path:
        current = self.output_dir.resolve()
        while not current.exists() and current != current.parent:
            current = current.parent
        return current

    def _ensure_disk_space(self) -> None:
        if self.min_free_disk_bytes <= 0:
            return
        free = shutil.disk_usage(self._existing_storage_root()).free
        if free < self.min_free_disk_bytes:
            raise RuntimeError(
                "Microstructure disk reserve reached: "
                f"free={free} required={self.min_free_disk_bytes}"
            )

    def _raw_path(self, directory: Path, stem: str) -> Path:
        suffix = ".jsonl.gz" if self.raw_compression == "gzip" else ".jsonl"
        return directory / f"{stem}{suffix}"

    def _append_jsonl_many(
        self, path: Path, payloads: list[dict[str, Any]]
    ) -> None:
        if not payloads:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.raw_compression == "gzip":
            handle = gzip.open(path, "at", encoding="utf-8", newline="")
        else:
            handle = path.open("a", encoding="utf-8", newline="")
        with handle:
            for payload in payloads:
                handle.write(json.dumps(payload, separators=(",", ":")) + "\n")

    def _append_csv(self, path: Path, row: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        exists = path.exists() and path.stat().st_size > 0
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            if not exists:
                writer.writeheader()
            writer.writerow({field: row[field] for field in CSV_FIELDS})

    def _collect_symbol(
        self,
        symbol: str,
        previous: SymbolState,
        bucket_start: int,
        bucket_end: int,
        collected_at: int,
    ) -> tuple[
        dict[str, Any],
        SymbolState,
        list[dict[str, Any]],
        dict[str, Any],
        dict[str, Any],
    ]:
        started = time.perf_counter()
        from_id = previous.last_agg_id + 1 if previous.last_agg_id is not None else None
        recovery = False
        try:
            trades = self.client.agg_trades(symbol, from_id)
        except RuntimeError:
            if from_id is None:
                raise
            trades = self.client.agg_trades(symbol, None)
            recovery = True
        book = self.client.book_ticker(symbol)
        depth = self.client.depth(symbol, self.depth_limit)
        latency_ms = (time.perf_counter() - started) * 1000.0
        row, state = aggregate_five_second_snapshot(
            symbol,
            bucket_start,
            bucket_end,
            collected_at,
            trades,
            book,
            depth,
            previous,
            latency_ms,
            requested_from_id=from_id,
            collector_recovery=recovery,
        )
        return row, state, trades, book, depth

    def collect_once(self, now_ms: int | None = None) -> list[dict[str, Any]]:
        if self.write:
            self._ensure_disk_space()
        collected_at = int(time.time() * 1000) if now_ms is None else int(now_ms)
        bucket_start, bucket_end = _bucket_bounds(
            collected_at, self.interval_seconds
        )
        date = datetime.fromtimestamp(bucket_start / 1000.0, tz=UTC).date().isoformat()
        with ThreadPoolExecutor(max_workers=len(self.symbols)) as executor:
            futures = {
                symbol: executor.submit(
                    self._collect_symbol,
                    symbol,
                    self.states[symbol],
                    bucket_start,
                    bucket_end,
                    collected_at,
                )
                for symbol in self.symbols
            }
            results = {symbol: futures[symbol].result() for symbol in self.symbols}
        rows = []
        for symbol in self.symbols:
            row, state, trades, book, depth = results[symbol]
            rows.append(row)
            self.states[symbol] = state
            if self.write:
                raw_dir = self.output_dir / "raw" / symbol / date
                self._append_jsonl_many(
                    self._raw_path(raw_dir, "aggTrades"), trades
                )
                self._append_jsonl_many(
                    self._raw_path(raw_dir, "bookSnapshots"),
                    [{
                        "collected_at_ms": collected_at,
                        "bookTicker": book,
                        "depth": depth,
                    }],
                )
                self._append_csv(
                    self.output_dir / "aggregated_5s" / date / f"{symbol}.csv",
                    row,
                )
        if self.write:
            self._save_state()
        return rows


def _load_config(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Microstructure config must be a mapping")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/microstructure_5s.yaml")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    if not args.once and args.duration_seconds <= 0:
        raise ValueError("Use --once or set a positive --duration-seconds")
    config = _load_config(Path(args.config))
    client = BinanceFuturesRestClient(
        str(config["base_url"]),
        float(config["request_timeout_seconds"]),
        int(config["request_retries"]),
    )
    collector = MicrostructureCollector(
        client,
        list(config["symbols"]),
        int(config["interval_seconds"]),
        int(config["depth_limit"]),
        Path(config["output_dir"]),
        write=not args.no_write,
        raw_compression=str(config.get("raw_compression", "none")),
        min_free_disk_gb=float(config.get("min_free_disk_gb", 0.0)),
    )
    deadline = time.monotonic() + max(args.duration_seconds, 0)
    if not args.once:
        time.sleep(_seconds_to_next_boundary(time.time(), int(config["interval_seconds"])))
    while True:
        rows = collector.collect_once()
        if not args.quiet:
            print(json.dumps(rows, separators=(",", ":")), flush=True)
        if args.once or time.monotonic() >= deadline:
            break
        interval = int(config["interval_seconds"])
        sleep_seconds = _seconds_to_next_boundary(time.time(), interval)
        time.sleep(max(sleep_seconds, 0.05))


if __name__ == "__main__":
    main()
