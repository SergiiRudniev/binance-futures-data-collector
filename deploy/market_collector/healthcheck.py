from __future__ import annotations

import json
import sys
import time
from pathlib import Path


def heartbeat_is_fresh(path: Path, maximum_age_seconds: float, now_ms: int | None = None) -> bool:
    if not path.is_file():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        heartbeat_ms = int(payload["heartbeat_ms"])
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False
    current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    age_ms = max(0, current_ms - heartbeat_ms)
    return age_ms <= float(maximum_age_seconds) * 1000.0


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: healthcheck.py STATUS_PATH MAXIMUM_AGE_SECONDS")
    passed = heartbeat_is_fresh(Path(sys.argv[1]), float(sys.argv[2]))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
