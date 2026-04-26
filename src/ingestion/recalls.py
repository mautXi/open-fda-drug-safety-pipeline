import json
from pathlib import Path

from rich.console import Console

from .client import OpenFDAClient

console = Console()
ENDPOINT = "/drug/enforcement.json"


def ingest(client: OpenFDAClient, raw_dir: Path, force: bool = False, sample: bool = False) -> int:
    out_dir = raw_dir / "recalls"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "all.jsonl"

    if out_path.exists() and not force:
        with out_path.open() as f:
            n = sum(1 for _ in f)
        console.print(f"  [yellow]Skip recalls ({n:,} records cached)[/yellow]")
        return n

    if sample:
        console.print("  Recalls: sampling 1,000 records")
        records, _ = client.fetch_page(ENDPOINT, limit=1000)
        with open(out_path, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(r) + "\n" for r in records)
        return len(records)

    console.print("  Fetching all recalls...")
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for batch in client.paginate(ENDPOINT):
            f.writelines(json.dumps(r) + "\n" for r in batch)
            count += len(batch)
            print(f"    {count:,} records", end="\r")
    print()

    console.print(f"  [green]Saved {count:,} recalls -> {out_path.name}[/green]")
    return count
