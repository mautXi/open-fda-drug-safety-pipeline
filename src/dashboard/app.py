import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src import config
from src.dashboard.db import get_conn

st.set_page_config(
    page_title="openFDA Analytics",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def table_counts(conn) -> dict:
    counts = {}
    for table in ["fact_adverse_events", "fact_recalls", "dim_drugs"]:
        try:
            counts[table] = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        except Exception:
            counts[table] = 0
    return counts


def last_updated(conn) -> str:
    try:
        row = conn.execute(
            "SELECT max(ingested_at) FROM ingestion_log"
        ).fetchone()
        if row and row[0]:
            return str(row[0])[:19]
    except Exception:
        pass
    return "Unknown"


st.title("💊 openFDA Drug Safety Analytics")
st.markdown(
    "Explore FDA adverse event reports, drug recalls, and NDC drug metadata "
    "sourced from the [openFDA API](https://open.fda.gov/apis/)."
)

conn = get_conn()

if conn is None:
    st.error("No database found. Run the pipeline first:")
    st.code(
        "# 1. Copy .env.example → .env and add your API key\n"
        "# 2. Run the full pipeline:\n"
        "python pipeline.py run\n\n"
        "# Or step by step:\n"
        "python pipeline.py ingest --years 3\n"
        "python pipeline.py transform\n\n"
        "# Then launch the dashboard:\n"
        "python pipeline.py dashboard",
        language="bash",
    )
    st.stop()

counts = table_counts(conn)
updated = last_updated(conn)

st.markdown(f"**Last updated:** {updated}")

col1, col2, col3 = st.columns(3)
col1.metric("Adverse Event Reports", f"{counts['fact_adverse_events']:,}")
col2.metric("Drug Recalls", f"{counts['fact_recalls']:,}")
col3.metric("NDC Drug Products", f"{counts['dim_drugs']:,}")

st.divider()

st.subheader("Navigate")
st.markdown(
    """
| Page | What you can explore |
|---|---|
| **Adverse Events** | Drug safety signals, top reactions, temporal trends, geographic breakdown |
| **Recalls** | Class I/II/III recalls timeline, recalling firms, reasons for recall |
| **Drug Explorer** | Search any drug for its complete safety profile: adverse events + recalls |

Use the sidebar to switch between pages.
"""
)

# Show ingestion log
try:
    log_df = conn.execute(
        "SELECT endpoint, ingested_at, records_raw, records_loaded "
        "FROM ingestion_log ORDER BY ingested_at DESC LIMIT 10"
    ).df()
    if not log_df.empty:
        st.subheader("Ingestion Log")
        st.dataframe(log_df, width="stretch", hide_index=True)
except Exception:
    pass
