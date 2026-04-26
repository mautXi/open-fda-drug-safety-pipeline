from pathlib import Path

import pandas as pd
from rich.console import Console

from src.utils import clean, load_jsonl, parse_date

console = Console()


def _extract(raw: dict) -> dict:
    routes = raw.get("route") or []
    route = "; ".join(str(r) for r in routes) if isinstance(routes, list) else clean(routes)

    openfda = raw.get("openfda") or {}
    pharm_classes = openfda.get("pharm_class") or []
    pharm_class = str(pharm_classes[0]) if pharm_classes else ""

    mkt_date = parse_date(raw.get("marketing_start_date"))

    return {
        "product_ndc": clean(raw.get("product_ndc")),
        "generic_name": clean(raw.get("generic_name")).lower(),
        "brand_name": clean(raw.get("brand_name")),
        "labeler_name": clean(raw.get("labeler_name")),
        "dosage_form": clean(raw.get("dosage_form")),
        "route": route,
        "pharm_class": pharm_class,
        "dea_schedule": clean(raw.get("dea_schedule")) or "Not Scheduled",
        "marketing_start_date": mkt_date,
        "product_type": clean(raw.get("product_type")),
        "application_number": clean(raw.get("application_number")),
    }


def transform(raw_dir: Path) -> pd.DataFrame:
    console.print("  Loading NDC raw files...")
    raw_records = load_jsonl(raw_dir, "ndc")
    console.print(f"  Loaded {len(raw_records):,} raw records")

    rows = [_extract(r) for r in raw_records]
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["marketing_start_date"] = pd.to_datetime(df["marketing_start_date"], errors="coerce")
    df = df[df["product_ndc"].str.len() > 0]
    df.drop_duplicates(subset=["product_ndc"], keep="last", inplace=True)

    console.print(f"  [green]Transformed {len(df):,} NDC records[/green]")
    return df
