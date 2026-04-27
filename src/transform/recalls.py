from pathlib import Path

import pandas as pd
from rich.console import Console

from src.utils import clean, load_jsonl, parse_date

console = Console()


def _class_num(classification: str) -> int | None:
    if "Class III" in classification:
        return 3
    if "Class II" in classification:
        return 2
    if "Class I" in classification:
        return 1
    return None


def _recall_type(vol: str) -> str:
    if "Voluntary" in vol:
        return "Voluntary"
    if "FDA" in vol or "Mandated" in vol:
        return "FDA Mandated"
    return vol or "Unknown"


def _extract(raw: dict) -> dict:
    classification = clean(raw.get("classification"))
    report_date = parse_date(raw.get("report_date"))
    initiation_date = parse_date(raw.get("recall_initiation_date"))

    return {
        "recall_number": clean(raw.get("recall_number")),
        "report_date": report_date,
        "recall_initiation_date": initiation_date,
        "year": report_date.year if report_date else None,
        "month": report_date.month if report_date else None,
        "classification": classification,
        "class_num": _class_num(classification),
        "status": clean(raw.get("status")) or "Unknown",
        "recall_type": _recall_type(clean(raw.get("voluntary_mandated", ""))),
        "recalling_firm": clean(raw.get("recalling_firm")) or "Unknown",
        "product_description": clean(raw.get("product_description")),
        "reason_for_recall": clean(raw.get("reason_for_recall")),
        "distribution_pattern": clean(raw.get("distribution_pattern")),
        "state": clean(raw.get("state")),
        "country": clean(raw.get("country")) or "US",
    }


def transform(raw_dir: Path) -> pd.DataFrame:
    console.print("  Loading recalls raw files...")
    raw_records = load_jsonl(raw_dir, "recalls")
    console.print(f"  Loaded {len(raw_records):,} raw records")

    rows = [_extract(r) for r in raw_records]
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    df["recall_initiation_date"] = pd.to_datetime(df["recall_initiation_date"], errors="coerce")

    # Remove rows without a recall number or report date
    df = df[df["recall_number"].str.len() > 0]
    df.drop_duplicates(subset=["recall_number"], keep="last", inplace=True)

    console.print(f"  [green]Transformed {len(df):,} recall records[/green]")
    return df
