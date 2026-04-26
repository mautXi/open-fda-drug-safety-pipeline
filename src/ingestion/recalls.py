import json
from pathlib import Path

from rich.console import Console

from .client import OpenFDAClient
from ..utils import safe_rename

console = Console()
ENDPOINT = "/drug/enforcement.json"


def ingest(client: OpenFDAClient, raw_dir: Path, force: bool = False) -> int:
    out_dir = raw_dir / "recalls"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "all.jsonl"
    if out_path.exists() and not force:
        with open(out_path) as f:
            n = sum(1 for _ in f)
        console.print(f"  [yellow]Skip recalls ({n:,} records cached) -- use --force to re-fetch[/yellow]")
        return 0

    console.print("  Fetching drug recalls...")
    count = 0
    tmp_path = out_path.with_suffix(".tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for batch in client.paginate(ENDPOINT):
                for record in batch:
                    f.write(json.dumps(record) + "\n")
                count += len(batch)
                print(f"    {count:,} records", end="\r")
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        print()
        console.print(f"  [red]Error fetching recalls: {exc}[/red]")
        return 0

    print()
    safe_rename(tmp_path, out_path)

    if count == 0:
        out_path.unlink(missing_ok=True)
        console.print("  [yellow]No recalls data returned[/yellow]")
    else:
        console.print(f"  [green]Saved {count:,} recalls -> {out_path.name}[/green]")

    return count
