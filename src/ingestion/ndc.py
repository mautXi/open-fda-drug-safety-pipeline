import calendar
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from functools import partial
from pathlib import Path

from rich.console import Console

from .client import OpenFDAClient

console = Console()
ENDPOINT = "/drug/ndc.json"
MAX_WORKERS = 10


def _fetch_chunk(client: OpenFDAClient, search: str) -> list[dict]:
    return [r for batch in client.paginate(ENDPOINT, search=search) for r in batch]


def _month_chunks(today: date) -> list[str]:
    chunks = []
    for year in range(1940, today.year + 1):
        for month in range(1, 13):
            if year == today.year and month > today.month:
                break
            last_day = calendar.monthrange(year, month)[1]
            chunks.append(
                f"finished:true AND marketing_start_date:[{year}{month:02d}01 TO {year}{month:02d}{last_day:02d}]"
            )
    return chunks


def ingest(client: OpenFDAClient, raw_dir: Path, force: bool = False, sample: bool = False) -> int:
    out_dir = raw_dir / "ndc"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "products.jsonl"

    if out_path.exists() and not force:
        with out_path.open() as f:
            n = sum(1 for _ in f)
        console.print(f"  [yellow]Skip NDC ({n:,} records cached)[/yellow]")
        return n

    if sample:
        console.print("  NDC: sampling 1,000 records")
        records, _ = client.fetch_page(ENDPOINT, "finished:true", limit=1000)
        with open(out_path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(r) + "\n" for r in records)
        count = len(records)
    else:
        chunks = _month_chunks(date.today())
        console.print(f"  NDC: {len(chunks)} month chunks, {MAX_WORKERS} threads")
        count = 0
        with open(out_path, "w", encoding="utf-8") as f:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                for i, records in enumerate(pool.map(partial(_fetch_chunk, client), chunks), 1):
                    f.writelines(json.dumps(r) + "\n" for r in records)
                    count += len(records)
                    print(f"    {i}/{len(chunks)} chunks, {count:,} records", end="\r")
        print()

    console.print(f"  [green]Saved {count:,} NDC products -> {out_path.name}[/green]")
    return count
