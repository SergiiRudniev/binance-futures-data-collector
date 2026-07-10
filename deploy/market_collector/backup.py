from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


UTC = timezone.utc


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def _free_bytes(path: Path) -> int:
    current = path.resolve()
    while not current.exists() and current != current.parent:
        current = current.parent
    return shutil.disk_usage(current).free


class BackupMirror:
    def __init__(self, source: Path, destination: Path, minimum_free_gb: float = 0.0) -> None:
        self.source = source.resolve()
        self.destination = destination.resolve()
        self.minimum_free_bytes = int(float(minimum_free_gb) * 1024**3)

    def _eligible_files(self) -> list[Path]:
        if not self.source.is_dir():
            return []
        return sorted(
            path
            for path in self.source.rglob("*")
            if path.is_file() and not path.name.endswith((".tmp", ".part"))
        )

    def _copy_stable(self, source: Path, destination: Path) -> dict[str, Any] | None:
        before = source.stat()
        if destination.is_file():
            current = destination.stat()
            if current.st_size == before.st_size and current.st_mtime_ns == before.st_mtime_ns:
                return None
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        shutil.copy2(source, temporary)
        after = source.stat()
        if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
            temporary.unlink(missing_ok=True)
            return None
        source_hash = _sha256(source)
        backup_hash = _sha256(temporary)
        if source_hash != backup_hash:
            temporary.unlink(missing_ok=True)
            raise RuntimeError(f"Backup checksum mismatch: {source}")
        os.replace(temporary, destination)
        return {
            "path": source.relative_to(self.source).as_posix(),
            "bytes": int(after.st_size),
            "sha256": source_hash,
        }

    def run_once(self, now_ms: int | None = None) -> dict[str, Any]:
        timestamp_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        self.destination.mkdir(parents=True, exist_ok=True)
        if _free_bytes(self.destination) < self.minimum_free_bytes:
            raise RuntimeError("Backup disk reserve reached")
        copied: list[dict[str, Any]] = []
        skipped_live = 0
        for source in self._eligible_files():
            relative = source.relative_to(self.source)
            result = self._copy_stable(source, self.destination / "mirror" / relative)
            if result is None:
                skipped_live += 1
            else:
                copied.append(result)
        audit = {
            "schema_version": 1,
            "completed_at_ms": timestamp_ms,
            "completed_at_utc": datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat(),
            "copied_files": copied,
            "copied_count": len(copied),
            "unchanged_or_live_count": skipped_live,
        }
        audit_path = self.destination / "audits" / f"{timestamp_ms}.json"
        _atomic_json(audit_path, audit)
        _atomic_json(
            self.destination / "backup_status.json",
            {
                "heartbeat_ms": timestamp_ms,
                "status": "ok",
                "last_audit": audit_path.relative_to(self.destination).as_posix(),
                "copied_count": len(copied),
            },
        )
        return audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.environ.get("BACKUP_INTERVAL_SECONDS", "3600")),
    )
    parser.add_argument(
        "--minimum-free-gb",
        type=float,
        default=float(os.environ.get("BACKUP_MIN_FREE_GB", "4")),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    mirror = BackupMirror(args.source, args.destination, args.minimum_free_gb)
    while True:
        try:
            result = mirror.run_once()
            print(json.dumps({"backup": result["copied_count"]}), flush=True)
        except Exception as exc:
            timestamp_ms = int(time.time() * 1000)
            _atomic_json(
                args.destination / "backup_status.json",
                {"heartbeat_ms": timestamp_ms, "status": "error", "error": repr(exc)},
            )
            print(json.dumps({"backup_error": repr(exc)}), flush=True)
        if args.once:
            break
        time.sleep(max(60, args.interval_seconds))


if __name__ == "__main__":
    main()
