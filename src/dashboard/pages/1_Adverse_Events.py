"""
Adverse Events Explorer
- Timeline trends
- Top drugs / reactions
- Proportional Reporting Ratio (PRR) safety signal detection
- Geographic breakdown
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src import config

st.set_page_config(page_title="Adverse Events", page_icon="⚠️", layout="wide")


@st.cache_resource
def get_conn():
    if not config.DB_PATH.exists():
        return None
    return duckdb.connect(str(config.DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def load_events(_conn_id) -> pd.DataFrame:
    conn = get_conn()
    return conn.execute(
        """
        SELECT event_id, report_date, year, month, serious, death, hospitalization,
               life_threatening, country, sex, age_years,
               suspect_drug, suspect_brand, all_suspect_drugs,
               reactions, num_reactions, num_suspect_drugs
        FROM fact_adverse_events
        WHERE report_date IS NOT NULL
        ORDER BY report_date
        """
    ).df()


conn = get_conn()
if conn is None:
    st.error("No database found. Run `python pipeline.py run` first.")
    st.stop()

st.title("⚠️ Adverse Events Explorer")

df_all = load_events(id(conn))

# ── Sidebar Filters ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    year_min = int(df_all["year"].min()) if not df_all.empty else 2020
    year_max = int(df_all["year"].max()) if not df_all.empty else 2024
    year_range = st.slider("Year range", year_min, year_max, (year_min, year_max))

    serious_only = st.checkbox("Serious events only")
    deaths_only = st.checkbox("Fatal events only")

    top_drugs = (
        df_all[df_all["suspect_drug"] != ""]
        .groupby("suspect_drug")
        .size()
        .nlargest(100)
        .index.tolist()
    )
    drug_filter = st.selectbox("Drug (optional)", ["All"] + top_drugs)

    sex_filter = st.multiselect("Sex", ["Male", "Female", "Unknown"], default=["Male", "Female", "Unknown"])

# Apply filters
df = df_all[
    (df_all["year"] >= year_range[0])
    & (df_all["year"] <= year_range[1])
    & (df_all["sex"].isin(sex_filter))
]
if serious_only:
    df = df[df["serious"]]
if deaths_only:
    df = df[df["death"]]
if drug_filter != "All":
    df = df[df["all_suspect_drugs"].str.contains(drug_filter, case=False, na=False)]

# ── KPI Metrics ─────────────────────────────────────────────────────────────
st.subheader("Overview")
if df.empty:
    st.warning("No records match current filters.")
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Events", f"{len(df):,}")
c2.metric("Serious", f"{df['serious'].mean() * 100:.1f}%")
c3.metric("Fatal", f"{df['death'].mean() * 100:.1f}%")
c4.metric(
    "Hospitalization", f"{df['hospitalization'].mean() * 100:.1f}%"
)
c5.metric("Unique Drugs (suspect)", f"{df['suspect_drug'].nunique():,}")

st.divider()

# ── Timeline ─────────────────────────────────────────────────────────────────
# FAERS batch submissions assign rounded receive-dates (often Jan 1), so monthly
# granularity is meaningless. Annual view shows real year-over-year composition.
st.subheader("Serious vs. Non-Serious Events by Year")
st.caption(
    "Each bar represents ~25,000 sampled reports (openFDA pagination cap). "
    "The serious/non-serious split within each year is meaningful."
)
timeline = (
    df.groupby(["year", "serious"])
    .size()
    .reset_index(name="count")
)
timeline["serious_label"] = timeline["serious"].map({True: "Serious", False: "Non-Serious"})

fig_timeline = px.bar(
    timeline,
    x="year",
    y="count",
    color="serious_label",
    color_discrete_map={"Serious": "#e74c3c", "Non-Serious": "#3498db"},
    labels={"year": "Year", "count": "Reports", "serious_label": ""},
    barmode="stack",
    text_auto=True,
)
fig_timeline.update_layout(legend_title_text="", height=320)
st.plotly_chart(fig_timeline, width="stretch")

# ── Top Drugs & Reactions ────────────────────────────────────────────────────
col_drugs, col_reactions = st.columns(2)

with col_drugs:
    st.subheader("Top Drugs by Report Count")
    top_n = st.slider("Show top N drugs", 10, 30, 20, key="top_drugs")
    drug_counts = (
        df[df["suspect_drug"] != ""]
        .groupby("suspect_drug")
        .agg(
            total=("event_id", "count"),
            serious=("serious", "sum"),
            deaths=("death", "sum"),
        )
        .reset_index()
        .nlargest(top_n, "total")
    )
    drug_counts["serious_pct"] = (drug_counts["serious"] / drug_counts["total"] * 100).round(1)
    fig_drugs = px.bar(
        drug_counts.sort_values("total"),
        x="total",
        y="suspect_drug",
        orientation="h",
        color="serious_pct",
        color_continuous_scale="RdYlGn_r",
        labels={"total": "Reports", "suspect_drug": "Drug", "serious_pct": "Serious %"},
    )
    fig_drugs.update_layout(height=500, coloraxis_showscale=True)
    st.plotly_chart(fig_drugs, width="stretch")

with col_reactions:
    st.subheader("Top Reactions")
    top_n_rx = st.slider("Show top N reactions", 10, 30, 20, key="top_rx")
    rx_series = (
        df["reactions"]
        .dropna()
        .str.split("; ")
        .explode()
        .str.strip()
        .loc[lambda s: s != ""]
        .value_counts()
        .head(top_n_rx)
        .reset_index()
    )
    rx_series.columns = ["reaction", "count"]
    fig_rx = px.bar(
        rx_series.sort_values("count"),
        x="count",
        y="reaction",
        orientation="h",
        labels={"count": "Reports", "reaction": "Reaction (MedDRA)"},
        color_discrete_sequence=["#3498db"],
    )
    fig_rx.update_layout(height=500)
    st.plotly_chart(fig_rx, width="stretch")

st.divider()

# ── Safety Signal Detection (PRR) ────────────────────────────────────────────
st.subheader("Safety Signal Detection (Proportional Reporting Ratio)")
st.caption(
    "PRR identifies drug–reaction pairs reported more often than expected by chance. "
    "PRR ≥ 2 with ≥ 3 reports is a conventional signal threshold."
)

with st.expander("Compute PRR signals", expanded=False):
    min_cases = st.number_input("Minimum report count", min_value=2, max_value=20, value=3)
    min_prr = st.number_input("Minimum PRR", min_value=1.0, max_value=10.0, value=2.0, step=0.5)

    @st.cache_data(ttl=120)
    def compute_prr(drug_sel: str, min_n: int) -> pd.DataFrame:
        conn = get_conn()
        sql = """
        WITH exploded AS (
            SELECT event_id, suspect_drug,
                   TRIM(unnest(string_split(reactions, '; '))) AS reaction
            FROM fact_adverse_events
            WHERE suspect_drug <> '' AND reactions <> ''
              AND year BETWEEN ? AND ?
        ),
        total AS (SELECT count(DISTINCT event_id) AS N FROM exploded),
        pair AS (
            SELECT suspect_drug, reaction,
                   count(*) AS n_ab
            FROM exploded GROUP BY 1, 2
        ),
        drug_total AS (
            SELECT suspect_drug, count(*) AS n_a FROM exploded GROUP BY 1
        ),
        reaction_total AS (
            SELECT reaction, count(*) AS n_b FROM exploded GROUP BY 1
        )
        SELECT p.suspect_drug, p.reaction, p.n_ab,
               d.n_a AS drug_reports, r.n_b AS reaction_reports,
               ROUND((p.n_ab * 1.0 / d.n_a) / (r.n_b * 1.0 / t.N), 2) AS prr
        FROM pair p
        JOIN drug_total d USING (suspect_drug)
        JOIN reaction_total r USING (reaction)
        CROSS JOIN total t
        WHERE p.n_ab >= ?
        ORDER BY prr DESC
        LIMIT 200
        """
        return conn.execute(sql, [year_range[0], year_range[1], min_n]).df()

    prr_df = compute_prr(drug_filter, min_cases)
    if not prr_df.empty:
        prr_df = prr_df[prr_df["prr"] >= min_prr]
        if drug_filter != "All":
            prr_df = prr_df[prr_df["suspect_drug"] == drug_filter]

        st.dataframe(
            prr_df.style.background_gradient(subset=["prr"], cmap="Reds"),
            width="stretch",
            hide_index=True,
        )
        st.caption(f"{len(prr_df)} signals found")
    else:
        st.info("No signals found with current settings.")

st.divider()

# ── Demographics ─────────────────────────────────────────────────────────────
col_sex, col_age, col_geo = st.columns(3)

with col_sex:
    st.subheader("Reporter Sex")
    sex_counts = df["sex"].value_counts().reset_index()
    sex_counts.columns = ["sex", "count"]
    sex_counts["pct"] = (sex_counts["count"] / sex_counts["count"].sum() * 100).round(1)
    fig_sex = px.bar(
        sex_counts,
        x="count", y="sex", orientation="h",
        text=sex_counts["pct"].apply(lambda p: f"{p}%"),
        labels={"count": "Reports", "sex": ""},
        color="sex",
        color_discrete_map={"Female": "#e91e8c", "Male": "#1976d2", "Unknown": "#9e9e9e"},
    )
    fig_sex.update_traces(textposition="outside")
    fig_sex.update_layout(height=300, showlegend=False)
    st.plotly_chart(fig_sex, width="stretch")

with col_age:
    age_coverage = int(df["age_years"].notna().mean() * 100)
    st.subheader(f"Age Distribution ({age_coverage}% coverage)")
    age_data = df["age_years"].dropna()
    age_data = age_data[(age_data >= 0) & (age_data <= 100)]
    fig_age = px.histogram(
        age_data, nbins=20,
        labels={"value": "Age (years)", "count": "Reports"},
        color_discrete_sequence=["#9b59b6"],
    )
    fig_age.update_layout(height=300, showlegend=False)
    st.plotly_chart(fig_age, width="stretch")

with col_geo:
    st.subheader("Top Reporting Countries")
    geo = df["country"].value_counts().head(10).reset_index()
    geo.columns = ["country", "count"]
    fig_geo = px.bar(
        geo.sort_values("count"),
        x="count", y="country", orientation="h",
        labels={"count": "Reports", "country": ""},
        color_discrete_sequence=["#1abc9c"],
    )
    fig_geo.update_layout(height=300)
    st.plotly_chart(fig_geo, width="stretch")
