import calendar
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

from rich.console import Console

from ..utils import safe_rename
from .client import OpenFDAClient

console = Console()
ENDPOINT = "/drug/ndc.json"
MAX_WORKERS = 10


def _fetch_chunk(client: OpenFDAClient, search: str) -> list[dict]:
    records: list[dict] = []
    for batch in client.paginate(ENDPOINT, search=search):
        records.extend(batch)
    return records


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


def ingest(client: OpenFDAClient, raw_dir: Path, force: bool = False) -> int:
    out_dir = raw_dir / "ndc"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "products.jsonl"
    if out_path.exists() and not force:
        with open(out_path) as f:
            n = sum(1 for _ in f)
        console.print(f"  [yellow]Skip NDC ({n:,} records cached)[/yellow]")
        return 0

    today = date.today()
    chunks = _month_chunks(today)
    console.print(f"  Fetching NDC: {len(chunks)} month chunks with {MAX_WORKERS} threads...")

    tmp_path = out_path.with_suffix(".tmp")
    count = 0

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = [pool.submit(_fetch_chunk, client, s) for s in chunks]
                done = 0
                for future in as_completed(futures):
                    records = future.result()
                    done += 1
                    for r in records:
                        f.write(json.dumps(r) + "\n")
                    count += len(records)
                    print(f"    {done}/{len(chunks)} chunks, {count:,} records", end="\r")
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        print()
        console.print(f"  [red]Error fetching NDC: {exc}[/red]")
        return 0

    print()
    safe_rename(tmp_path, out_path)

    if count == 0:
        out_path.unlink(missing_ok=True)
        console.print("  [yellow]No NDC data returned[/yellow]")
    else:
        console.print(f"  [green]Saved {count:,} NDC products -> {out_path.name}[/green]")

    return count
