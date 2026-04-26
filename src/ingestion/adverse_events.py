import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console

from ..utils import safe_rename
from .client import OpenFDAClient

console = Console()
ENDPOINT = "/drug/event.json"
MAX_WORKERS = 10


def _fetch_day(client: OpenFDAClient, date_str: str) -> tuple[str, list[dict]]:
    search = f"receivedate:[{date_str} TO {date_str}]"
    records: list[dict] = []
    for batch in client.paginate(ENDPOINT, search=search):
        records.extend(batch)
    return date_str, records


def ingest(client: OpenFDAClient, raw_dir: Path, years_back: int = 3, force: bool = False) -> int:
    out_dir = raw_dir / "adverse_events"
    out_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    total_saved = 0

    for offset in range(years_back):
        year = today.year - offset
        out_path = out_dir / f"{year}.jsonl"

        if out_path.exists() and not force:
            with open(out_path) as f:
                n = sum(1 for _ in f)
            console.print(f"  [yellow]Skip {year} ({n:,} records cached)[/yellow]")
            total_saved += n
            continue

        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), today - timedelta(days=1))
        if year_start > year_end:
            console.print(f"  [yellow]No data for {year} yet[/yellow]")
            continue

        _, year_total = client.fetch_page(ENDPOINT, f"receivedate:[{year}0101 TO {year}1231]", limit=1)
        if year_total == 0:
            console.print(f"  [yellow]No data for {year}[/yellow]")
            continue

        days: list[str] = []
        current = year_start
        while current <= year_end:
            days.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)

        console.print(f"  {year}: {year_total:,} records across {len(days)} days, {MAX_WORKERS} threads")

        tmp_path = out_path.with_suffix(".tmp")
        count = 0
        capped = 0

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(_fetch_day, client, d): d for d in days}
                    done = 0
                    for future in as_completed(futures):
                        date_str, records = future.result()
                        done += 1
                        if len(records) >= 25_000:
                            capped += 1
                        for r in records:
                            f.write(json.dumps(r) + "\n")
                        count += len(records)
                        print(f"    {year}: {done}/{len(days)} days, {count:,} records", end="\r")
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            print()
            console.print(f"  [red]Error fetching {year}: {exc}[/red]")
            continue

        print()
        safe_rename(tmp_path, out_path)

        if count == 0:
            out_path.unlink(missing_ok=True)
            console.print(f"  [yellow]No data found for {year}[/yellow]")
        else:
            total_saved += count
            warn = f" ({capped} days hit 25K cap)" if capped else ""
            console.print(f"  [green]Saved {count:,} events -> {out_path.name}{warn}[/green]")

    return total_saved
