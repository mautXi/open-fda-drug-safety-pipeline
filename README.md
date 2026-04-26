# Open FDA - Drug Safety Analytics

A self-contained data engineering pipeline and analytics dashboard that ingests FDA drug safety data, transforms it into analytics-ready tables, and surfaces insights through an interactive Streamlit application.

> **Work in progress**_ this project is under active development. Features and APIs may change.

---

## What does this project do?

The FDA publishes three high-value drug safety datasets through its [openFDA API](https://open.fda.gov/apis/):

| Dataset | Source |
|---|---|
| FAERS adverse event reports | FDA Adverse Event Reporting System |
| Drug recalls | FDA Recall Enterprise System |
| NDC drug directory | National Drug Code directory |

This project pulls those datasets in full, flattens the deeply nested JSON, loads them into a local DuckDB database, and makes them queryable through a three-page dashboard.

**Dashboard pages:**

1. **Adverse Events Explorer** — Year-over-year trends, top drugs by report count (colored by serious rate), top MedDRA reactions, PRR safety signal detection, reporter demographics, geographic breakdown
2. **Recalls Monitor** — Timeline by hazard class (I/II/III), firm risk scoring (weighted by class severity), reason-for-recall categorization, active Class I recall table
3. **Drug Explorer** — Search any drug to see its full safety profile (adverse events over time, top reactions, linked recalls); Drug Safety Landscape scatter that positions all drugs by report volume vs. seriousness rate

---

## Business value

### Who benefits

| Audience | How they could use it |
|---|---|
| **Drug safety / pharmacovigilance teams** | Detect disproportional drug–reaction pairs (PRR signals) before they become regulatory issues; benchmark a drug's safety profile against similar compounds |
| **Regulatory affairs** | Monitor recall trends by class and recalling firm; track Class I (highest-risk) recalls in real time |
| **Medical affairs & KAMs** | Prepare evidence-based talking points on a drug's adverse event profile relative to therapeutic class competitors |
| **Hospital formulary committees** | Compare safety signals across drugs in the same class before adding to formulary |
| **Healthcare researchers** | Explore FAERS signal patterns and recall seasonality without writing a single API query |
| **Risk & compliance** | Identify manufacturers with poor safety track records before entering procurement or licensing agreements |

### What you can answer with this dashboard

- Which drugs have the most adverse event reports and what fraction are serious or fatal?
- For a specific drug, what are the most frequently reported reactions? Are there signals not yet labeled?
- Which companies have issued the most Class I recalls, and is the trend improving?
- What are the leading reasons drugs get recalled (contamination, labeling, GMP violations)?
- Where in the world are adverse events being reported from, and is there geographic clustering?

### PRR safety signal detection

The Adverse Events page includes a **Proportional Reporting Ratio (PRR)** calculator — the same statistical method used by regulators and pharmaceutical companies to detect safety signals in spontaneous reporting databases. A drug–reaction pair with PRR ≥ 2 and at least 3 reports is a conventional signal threshold.

---

## Architecture

```
openFDA REST API
      |
      v
data/raw/             <- raw JSONL files (one per year/endpoint)
  adverse_events/
    2023.jsonl
    2024.jsonl
    2025.jsonl
  recalls/
    all.jsonl
  ndc/
    products.jsonl
      |
      v  (Python transform -- flatten nested JSON)
      |
      v
data/db/openfda.duckdb
  fact_adverse_events   (one row per FAERS report)
  fact_recalls          (one row per enforcement recall)
  dim_drugs             (NDC reference -- joins on generic_name)
  ingestion_log
      |
      v
Streamlit dashboard
```

**Stack:** Python 3.12 · DuckDB · Streamlit · Plotly · httpx · pandas

---

## How ingestion works

The openFDA API enforces a hard cap of 25,000 records per paginated query. To retrieve the complete dataset the pipeline splits every request into chunks small enough to stay under that cap, then fetches all chunks in parallel:

| Endpoint | Chunking strategy | Threads |
|---|---|---|
| Adverse events | One query per calendar day | 10 |
| NDC products | One query per calendar month, 1940 to present (~1,000 chunks) | 10 |
| Recalls | Single paginated query (~18K records, fits under cap) | — |

Raw data is written to `data/raw/` and cached; rerunning skips files that already exist. Pass `--force` to overwrite.

Many FAERS reports carry a `receivedate` of January 1st due to pharmaceutical companies batch submitting at quarter boundaries. The pipeline fetches every day individually to capture these batches; the dashboard aggregates by year to reflect this correctly.

---

## Setup

### 1. Get a free API key

Register at **https://open.fda.gov/apis/authentication/**.It increases the rate limit from 1000 req/day to 240 req/min.

### 2. Clone the repository

```bash
git clone <repo-url>
cd open-fda-drug-safety-pipeline

```

### 3. Install dependencies

```bash
uv sync
```

### 4. Configure the API key

```bash
cp .env.example .env
```

Open `.env` and set your key:

```
OPENFDA_API_KEY=your_actual_key_here
```

### 5. Run the pipeline

```bash
# Quick test -- 1,000 records per endpoint
python pipeline.py run --years 2 --sample

# Full run -- fetches complete data for the last 2 years
python pipeline.py run --years 2
```

### 6. Launch the dashboard

```bash
python pipeline.py dashboard
```

Opens at `http://localhost:8501`.

---

## Pipeline reference

```bash
# Full pipeline (ingest + transform)
python pipeline.py run --years 2

# Sample run for testing
python pipeline.py run --years 2 --sample

# Ingest only
python pipeline.py ingest --years 2

# Single endpoint
python pipeline.py ingest --endpoints recalls

# Force re-fetch (overwrites cached files)
python pipeline.py ingest --years 2 --force

# Transform only (safe to rerun -- upserts on primary key)
python pipeline.py transform
```

---

## Data notes

**Incremental updates:** Delete a `.jsonl` file in `data/raw/` and rerun `ingest` to refresh that endpoint. The `transform` step is always safe to rerun.