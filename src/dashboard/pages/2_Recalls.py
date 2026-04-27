"""
Recalls Monitor
- Timeline by hazard class (I/II/III)
- Recalling firm risk profiles
- Reason for recall analysis
- Recent high-risk recalls table
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src import config
from src.dashboard.db import get_conn

st.set_page_config(page_title="Recalls", page_icon="🚨", layout="wide")


@st.cache_data(ttl=300)
def load_recalls(_conn_id) -> pd.DataFrame:
    conn = get_conn()
    return conn.execute(
        """
        SELECT recall_number, report_date, recall_initiation_date, year, month,
               classification, class_num, status, recall_type,
               recalling_firm, product_description, reason_for_recall,
               distribution_pattern, state, country
        FROM fact_recalls
        WHERE report_date IS NOT NULL
        ORDER BY report_date
        """
    ).df()


conn = get_conn()
if conn is None:
    st.error("No database found. Run `python pipeline.py run` first.")
    st.stop()

st.title("🚨 Drug Recalls Monitor")

df_all = load_recalls(id(conn))

# ── Sidebar Filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    year_min = int(df_all["year"].min()) if not df_all.empty else 2004
    year_max = int(df_all["year"].max()) if not df_all.empty else 2024
    year_range = st.slider("Year range", year_min, year_max, (year_min, year_max))

    classes = st.multiselect(
        "Classification",
        ["Class I", "Class II", "Class III"],
        default=["Class I", "Class II", "Class III"],
    )
    statuses = ["All"] + sorted(df_all["status"].dropna().unique().tolist())
    status_filter = st.selectbox("Status", statuses)

    firm_list = ["All"] + sorted(
        df_all["recalling_firm"].value_counts().head(100).index.tolist()
    )
    firm_filter = st.selectbox("Recalling Firm (optional)", firm_list)

# Apply filters
df = df_all[
    (df_all["year"] >= year_range[0])
    & (df_all["year"] <= year_range[1])
    & (df_all["classification"].isin(classes))
]
if status_filter != "All":
    df = df[df["status"] == status_filter]
if firm_filter != "All":
    df = df[df["recalling_firm"] == firm_filter]

# ── KPI Metrics ──────────────────────────────────────────────────────────────
st.subheader("Overview")
if df.empty:
    st.warning("No records match current filters.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Recalls", f"{len(df):,}")
c2.metric("Class I (High Risk)", f"{(df['class_num'] == 1).sum():,}")
c3.metric("Class II", f"{(df['class_num'] == 2).sum():,}")
c4.metric("Class III", f"{(df['class_num'] == 3).sum():,}")
ongoing = (df["status"].str.lower() == "ongoing").sum()
c5.metric("Ongoing", f"{ongoing:,}")

st.divider()

# ── Timeline ─────────────────────────────────────────────────────────────────
st.subheader("Recalls Over Time by Classification")
CLASS_COLORS = {"Class I": "#e74c3c", "Class II": "#f39c12", "Class III": "#2ecc71"}

timeline = (
    df.assign(period=df["report_date"].dt.to_period("Q").dt.to_timestamp())
    .groupby(["period", "classification"])
    .size()
    .reset_index(name="count")
)
fig_timeline = px.bar(
    timeline,
    x="period",
    y="count",
    color="classification",
    color_discrete_map=CLASS_COLORS,
    barmode="stack",
    labels={"period": "Quarter", "count": "Recalls", "classification": "Class"},
)
fig_timeline.update_layout(height=320, legend_title_text="")
st.plotly_chart(fig_timeline, width="stretch")

# ── Firm Risk Profile & Class Distribution ───────────────────────────────────
col_firms, col_class = st.columns([2, 1])

with col_firms:
    st.subheader("Top Recalling Firms")
    top_n = st.slider("Show top N firms", 10, 30, 20, key="top_firms")
    firm_stats = (
        df.groupby("recalling_firm")
        .agg(
            total=("recall_number", "count"),
            class1=("class_num", lambda x: (x == 1).sum()),
            class2=("class_num", lambda x: (x == 2).sum()),
            class3=("class_num", lambda x: (x == 3).sum()),
        )
        .reset_index()
        .nlargest(top_n, "total")
        .sort_values("total")
    )
    # Risk score: Class I weighted 3x, Class II 2x, Class III 1x
    firm_stats["risk_score"] = firm_stats["class1"] * 3 + firm_stats["class2"] * 2 + firm_stats["class3"]

    fig_firms = px.bar(
        firm_stats,
        x="total",
        y="recalling_firm",
        orientation="h",
        color="risk_score",
        color_continuous_scale="Reds",
        labels={"total": "Total Recalls", "recalling_firm": "Firm", "risk_score": "Risk Score"},
    )
    fig_firms.update_layout(height=520)
    st.plotly_chart(fig_firms, width="stretch")
    st.caption("Risk Score = Class I × 3 + Class II × 2 + Class III × 1")

with col_class:
    st.subheader("Recall Status")
    st.caption("How many recalls are still active vs. resolved?")
    status_counts = df["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    status_counts["pct"] = (status_counts["count"] / status_counts["count"].sum() * 100).round(1)
    fig_status = px.bar(
        status_counts,
        x="count",
        y="status",
        orientation="h",
        text=status_counts["pct"].apply(lambda p: f"{p}%"),
        labels={"count": "Recalls", "status": ""},
        color="status",
        color_discrete_map={"Terminated": "#2ecc71", "Ongoing": "#e74c3c", "Completed": "#3498db"},
    )
    fig_status.update_traces(textposition="outside")
    fig_status.update_layout(height=280, showlegend=False)
    st.plotly_chart(fig_status, width="stretch")

st.divider()

# ── Top Reasons for Recall ────────────────────────────────────────────────────
st.subheader("Top Reasons for Recall")
st.caption("Extracted from free-text reason descriptions via keyword matching")

reason_keywords = {
    "Contamination": ["contamin", "mold", "microbial", "particulate", "foreign"],
    "Labeling Error": ["label", "misbranded", "incorrect label", "wrong label"],
    "Potency / Stability": ["potency", "stability", "out of spec", "degradation", "subpotent", "superpotent"],
    "Good Manufacturing Practice": ["GMP", "cGMP", "manufacturing", "steriliz"],
    "Packaging Defect": ["packag", "seal", "closure", "container"],
    "Undeclared Ingredient": ["undeclared", "allergen", "ingredient"],
}

rows = []
for category, keywords in reason_keywords.items():
    pattern = "|".join(keywords)
    matched = df["reason_for_recall"].str.contains(pattern, case=False, na=False)
    rows.append({"Reason Category": category, "Count": matched.sum()})

reason_df = pd.DataFrame(rows).sort_values("Count", ascending=True)
fig_reasons = px.bar(
    reason_df,
    x="Count",
    y="Reason Category",
    orientation="h",
    color="Count",
    color_continuous_scale="Oranges",
)
fig_reasons.update_layout(height=320, coloraxis_showscale=False)
st.plotly_chart(fig_reasons, width="stretch")

st.divider()

# ── High-Risk Recall Table ────────────────────────────────────────────────────
st.subheader("High-Risk Recalls (Class I, Most Recent)")
class1 = (
    df[df["class_num"] == 1]
    .sort_values("report_date", ascending=False)
    .head(50)[
        [
            "report_date",
            "recall_number",
            "recalling_firm",
            "product_description",
            "reason_for_recall",
            "status",
            "distribution_pattern",
        ]
    ]
)
st.dataframe(class1, width="stretch", hide_index=True)
