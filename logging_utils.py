from __future__ import annotations
import csv
import os
from datetime import datetime
from typing import Optional, Dict, Any

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_PATH = os.path.join(LOG_DIR, "log.csv")


def ensure_log_setup() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "level", "source", "message", "error"])  # header


def log_event(level: str, source: str, message: str, *, error: Optional[str] = None) -> None:
    """Append a log line to logs/log.csv.

    level: INFO | WARNING | ERROR
    source: module or step name
    message: short description
    error: optional exception string
    """
    ensure_log_setup()
    ts = datetime.now().isoformat(timespec="seconds")
    row = [ts, level.upper(), source, message, error or ""]
    try:
        with open(LOG_PATH, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception:
        # Last resort: do not raise logging failures; best-effort only
        pass
