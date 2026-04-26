import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional


def safe_rename(src: Path, dst: Path, retries: int = 10, delay: float = 1.0) -> None:
    """Rename with retries to handle Windows AV file locks."""
    for attempt in range(retries):
        try:
            src.replace(dst)
            return
        except PermissionError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)


def load_jsonl(raw_dir: Path, subdirectory: str) -> list[dict]:
    source_dir = raw_dir / subdirectory
    records: list[dict] = []
    if not source_dir.exists():
        return records
    for path in sorted(source_dir.glob("*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def parse_date(s) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    for fmt, length in [("%Y%m%d", 8), ("%Y-%m-%d", 10), ("%m/%d/%Y", 10)]:
        try:
            return datetime.strptime(s[:length], fmt).date()
        except (ValueError, TypeError):
            pass
    return None


def clean(value, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()
