import json
from datetime import date, datetime
from pathlib import Path


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


def parse_date(s) -> date | None:
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
