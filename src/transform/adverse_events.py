from pathlib import Path

import pandas as pd
from rich.console import Console

from src.utils import clean, load_jsonl, parse_date

console = Console()


def _drug_generic(drug: dict) -> str:
    openfda = drug.get("openfda") or {}
    names = openfda.get("generic_name") or []
    return str(names[0]).lower().strip() if names else clean(drug.get("medicinalproduct")).lower()


def _drug_brand(drug: dict) -> str:
    openfda = drug.get("openfda") or {}
    brands = openfda.get("brand_name") or []
    return str(brands[0]).strip() if brands else ""


def _parse_age_years(age_raw, age_unit_raw) -> float | None:
    if age_raw is None:
        return None
    try:
        age = float(age_raw)
        # 801=years, 802=months, 803=weeks, 804=days, 805=hours
        conversions = {"801": 1.0, "802": 1/12, "803": 1/52.18, "804": 1/365.25, "805": 1/8766}
        result = age * conversions.get(str(age_unit_raw or "801"), 1.0)
        return round(result, 1) if 0 < result < 130 else None
    except (ValueError, TypeError):
        return None


def _extract(raw: dict) -> dict | None:
    patient = raw.get("patient") or {}
    drugs = patient.get("drug") or []
    suspect_drugs = [d for d in drugs if clean(d.get("drugcharacterization")) == "1"]
    suspect_generics = [n for n in (_drug_generic(d) for d in suspect_drugs) if n]

    reactions = patient.get("reaction") or []
    reaction_names = [clean(r.get("reactionmeddrapt")) for r in reactions if r.get("reactionmeddrapt")]

    report_date = parse_date(raw.get("receivedate"))
    sex_map = {"1": "Male", "2": "Female", "0": "Unknown"}
    event_id = clean(raw.get("safetyreportid"))
    if not event_id:
        return None

    return {
        "event_id": event_id,
        "report_date": report_date,
        "year": report_date.year if report_date else None,
        "month": report_date.month if report_date else None,
        "serious": clean(raw.get("serious")) == "1",
        "death": clean(raw.get("seriousnessdeath")) == "1",
        "hospitalization": clean(raw.get("seriousnesshospitalization")) == "1",
        "life_threatening": clean(raw.get("seriousnesslifethreatening")) == "1",
        "country": clean(raw.get("occurcountry")) or "Unknown",
        "sex": sex_map.get(clean(patient.get("patientsex")), "Unknown"),
        "age_years": _parse_age_years(patient.get("patientonsetage"), patient.get("patientonsetageunit")),
        "suspect_drug": suspect_generics[0] if suspect_generics else "",
        "suspect_brand": _drug_brand(suspect_drugs[0]) if suspect_drugs else "",
        "all_suspect_drugs": "; ".join(suspect_generics[:5]),
        "reactions": "; ".join(reaction_names[:20]),
        "num_reactions": len(reaction_names),
        "num_suspect_drugs": len(suspect_generics),
    }


def transform(raw_dir: Path) -> pd.DataFrame:
    console.print("  Loading adverse event raw files...")
    raw_records = load_jsonl(raw_dir, "adverse_events")
    console.print(f"  Loaded {len(raw_records):,} raw records")

    rows = [r for raw in raw_records if (r := _extract(raw))]
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    df.drop_duplicates(subset=["event_id"], keep="last", inplace=True)
    console.print(f"  [green]Transformed {len(df):,} adverse event records[/green]")
    return df
