"""
Entry point for the openFDA data pipeline.

Usage:
    python pipeline.py ingest [--years N] [--endpoints adverse_events,recalls,ndc]
    python pipeline.py transform
    python pipeline.py run          # ingest + transform
    python pipeline.py dashboard    # launch Streamlit dashboard
"""

import argparse
import subprocess
import sys

from rich.console import Console
from rich.rule import Rule

from src import config
from src.ingestion.client import OpenFDAClient

console = Console()


def cmd_ingest(years: int, endpoints: list[str], force: bool = False, sample: bool = False) -> None:
    console.print(Rule("[bold blue]Ingestion"))
    if sample:
        console.print("[cyan]Sample mode: fetching 1,000 records per endpoint/year[/cyan]")
    if not config.OPENFDA_API_KEY:
        console.print(
            "[yellow]No OPENFDA_API_KEY set -- running at reduced rate (1,000 req/day).[/yellow]\n"
            "Copy .env.example to .env and add your key from https://open.fda.gov/apis/authentication/"
        )

    with OpenFDAClient(
        api_key=config.OPENFDA_API_KEY,
        requests_per_minute=config.REQUESTS_PER_MINUTE,
    ) as client:
        if "adverse_events" in endpoints:
            console.print("\n[bold]Adverse Events[/bold]")
            from src.ingestion import adverse_events
            n = adverse_events.ingest(client, config.RAW_DIR, years_back=years, force=force, sample=sample)
            console.print(f"  Total: {n:,} records")

        if "recalls" in endpoints:
            console.print("\n[bold]Recalls[/bold]")
            from src.ingestion import recalls
            recalls.ingest(client, config.RAW_DIR, force=force, sample=sample)

        if "ndc" in endpoints:
            console.print("\n[bold]NDC Drug Products[/bold]")
            from src.ingestion import ndc
            ndc.ingest(client, config.RAW_DIR, force=force, sample=sample)


def cmd_transform() -> None:
    console.print(Rule("[bold blue]Transform"))
    from src.storage import database as db
    from src.transform import adverse_events, recalls, ndc

    conn = db.init(config.DB_PATH)

    steps = [
        ("Adverse Events", adverse_events, "fact_adverse_events", "adverse_events"),
        ("Recalls",        recalls,        "fact_recalls",        "recalls"),
        ("NDC Drugs",      ndc,            "dim_drugs",           "ndc"),
    ]
    for label, module, table, key in steps:
        print(f"\n{label}")
        df = module.transform(config.RAW_DIR)
        loaded = db.upsert(conn, table, df)
        db.log_ingestion(conn, key, len(df), loaded)
        print(f"  Loaded {loaded:,} rows -> {table}")

    conn.close()
    print(f"\nDatabase written to {config.DB_PATH}")


def cmd_dashboard() -> None:
    console.print(Rule("[bold blue]Dashboard"))
    dashboard_app = config.BASE_DIR / "src" / "dashboard" / "app.py"
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(dashboard_app)],
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="openFDA analytics pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Fetch data from openFDA API")
    p_ingest.add_argument("--years", type=int, default=config.DEFAULT_YEARS_BACK)
    p_ingest.add_argument(
        "--endpoints",
        default="adverse_events,recalls,ndc",
        help="Comma-separated list of endpoints to ingest",
    )
    p_ingest.add_argument(
        "--force", action="store_true",
        help="Re-fetch even if the raw file already exists",
    )
    p_ingest.add_argument(
        "--sample", action="store_true",
        help="Fetch 1,000 records per endpoint/year for quick testing",
    )

    sub.add_parser("transform", help="Transform raw data into DuckDB analytics tables")

    p_run = sub.add_parser("run", help="Full pipeline: ingest then transform")
    p_run.add_argument("--years", type=int, default=config.DEFAULT_YEARS_BACK)
    p_run.add_argument("--endpoints", default="adverse_events,recalls,ndc")
    p_run.add_argument("--force", action="store_true", help="Re-fetch existing raw files")
    p_run.add_argument("--sample", action="store_true", help="Fetch 1,000 records per endpoint/year for quick testing")

    sub.add_parser("dashboard", help="Launch Streamlit dashboard")

    args = parser.parse_args()

    if args.command == "ingest":
        endpoints = [e.strip() for e in args.endpoints.split(",")]
        cmd_ingest(args.years, endpoints, force=args.force, sample=args.sample)

    elif args.command == "transform":
        cmd_transform()

    elif args.command == "run":
        endpoints = [e.strip() for e in args.endpoints.split(",")]
        cmd_ingest(args.years, endpoints, force=args.force, sample=args.sample)
        cmd_transform()

    elif args.command == "dashboard":
        cmd_dashboard()


if __name__ == "__main__":
    main()
