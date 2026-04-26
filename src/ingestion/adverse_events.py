import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from functools import partial
from pathlib import Path

from rich.console import Console

from .client import OpenFDAClient

console = Console()
ENDPOINT = "/drug/event.json"
MAX_WORKERS = 10


def _fetch_day(client: OpenFDAClient, date_str: str) -> list[dict]:
    search = f"receivedate:[{date_str} TO {date_str}]"
    return [r for batch in client.paginate(ENDPOINT, search=search) for r in batch]


def ingest(client: OpenFDAClient, raw_dir: Path, years_back: int = 3, force: bool = False, sample: bool = False) -> int:
    out_dir = raw_dir / "adverse_events"
    out_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    total_saved = 0
    fetched = 0
    offset = 0

    while fetched < years_back:
        year = today.year - offset
        offset += 1
        if offset > years_back + 5:
            break

        out_path = out_dir / f"{year}.jsonl"

        if out_path.exists() and not force:
            with out_path.open() as f:
                n = sum(1 for _ in f)
            console.print(f"  [yellow]Skip {year} ({n:,} records cached)[/yellow]")
            total_saved += n
            fetched += 1
            continue

        search = f"receivedate:[{year}0101 TO {year}1231]"
        _, year_total = client.fetch_page(ENDPOINT, search, limit=1)
        if year_total == 0:
            console.print(f"  [yellow]No data for {year}[/yellow]")
            continue

        if sample:
            console.print(f"  {year}: sampling 1,000 of {year_total:,} records")
            records, _ = client.fetch_page(ENDPOINT, search, limit=1000)
            with open(out_path, "w", encoding="utf-8") as f:
                f.writelines(json.dumps(r) + "\n" for r in records)
            count = len(records)
        else:
            year_start = date(year, 1, 1)
            year_end = min(date(year, 12, 31), today - timedelta(days=1))
            days = []
            current = year_start
            while current <= year_end:
                days.append(current.strftime("%Y%m%d"))
                current += timedelta(days=1)

            console.print(f"  {year}: {year_total:,} records, {len(days)} days, {MAX_WORKERS} threads")
            count = 0
            with open(out_path, "w", encoding="utf-8") as f:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    for i, records in enumerate(pool.map(partial(_fetch_day, client), days), 1):
                        f.writelines(json.dumps(r) + "\n" for r in records)
                        count += len(records)
                        print(f"    {i}/{len(days)} days, {count:,} records", end="\r")
            print()

        console.print(f"  [green]Saved {count:,} events -> {out_path.name}[/green]")
        total_saved += count
        fetched += 1

    return total_saved
