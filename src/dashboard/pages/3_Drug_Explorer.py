"""
Drug Explorer
- Search any drug name and see its complete safety profile
- Adverse events over time, top reactions, demographics
- Related recalls (product description match)
- Drug Safety Landscape: scatter of all drugs by AE count vs serious rate
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src import config
from src.dashboard.db import get_conn

st.set_page_config(page_title="Drug Explorer", page_icon="🔍", layout="wide")


@st.cache_data(ttl=300)
def drug_list(_conn_id) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT suspect_drug, count(*) AS n
        FROM fact_adverse_events
        WHERE suspect_drug <> ''
        GROUP BY 1
        HAVING count(*) >= 5
        ORDER BY n DESC
        LIMIT 2000
        """
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def drug_profile(_conn_id, drug: str) -> dict:
    conn = get_conn()

    ae = conn.execute(
        """
        SELECT event_id, report_date, year, month, serious, death,
               hospitalization, life_threatening, sex, age_years,
               reactions, num_reactions
        FROM fact_adverse_events
        WHERE all_suspect_drugs LIKE ? AND report_date IS NOT NULL
        ORDER BY report_date
        """,
        [f"%{drug}%"],
    ).df()

    recalls = conn.execute(
        """
        SELECT recall_number, report_date, classification, class_num,
               status, recalling_firm, product_description, reason_for_recall
        FROM fact_recalls
        WHERE lower(product_description) LIKE ?
           OR lower(product_description) LIKE ?
        ORDER BY report_date DESC
        LIMIT 100
        """,
        [f"%{drug}%", f"%{drug.replace(' ', '')}%"],
    ).df()

    ndc = conn.execute(
        """
        SELECT product_ndc, brand_name, labeler_name, dosage_form,
               route, pharm_class, dea_schedule, marketing_start_date
        FROM dim_drugs
        WHERE lower(generic_name) LIKE ?
        LIMIT 20
        """,
        [f"%{drug}%"],
    ).df()

    return {"ae": ae, "recalls": recalls, "ndc": ndc}


@st.cache_data(ttl=600)
def safety_landscape(_conn_id) -> pd.DataFrame:
    conn = get_conn()
    return conn.execute(
        """
        SELECT suspect_drug AS drug,
               count(*)                                    AS total_reports,
               round(avg(serious::int) * 100, 1)          AS serious_pct,
               round(avg(death::int) * 100, 2)            AS death_pct,
               round(avg(hospitalization::int) * 100, 1)  AS hosp_pct
        FROM fact_adverse_events
        WHERE suspect_drug <> ''
        GROUP BY 1
        HAVING count(*) >= 20
        ORDER BY total_reports DESC
        LIMIT 500
        """
    ).df()


conn = get_conn()
if conn is None:
    st.error("No database found. Run `python pipeline.py run` first.")
    st.stop()

st.title("🔍 Drug Explorer")

drugs = drug_list(id(conn))

tab_search, tab_landscape = st.tabs(["Drug Profile", "Safety Landscape"])

# ── Tab 1: Drug Profile ──────────────────────────────────────────────────────
with tab_search:
    drug = st.selectbox(
        "Select a drug",
        drugs,
        help="Ranked by adverse event report count. Only drugs with ≥5 reports shown.",
    )

    if drug:
        with st.spinner(f"Loading profile for **{drug}**..."):
            profile = drug_profile(id(conn), drug)

        ae = profile["ae"]
        recalls = profile["recalls"]
        ndc = profile["ndc"]

        # KPIs
        st.subheader(f"Safety Profile: {drug.title()}")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Adverse Event Reports", f"{len(ae):,}")
        c2.metric("Serious Rate", f"{ae['serious'].mean() * 100:.1f}%" if not ae.empty else "—")
        c3.metric("Fatality Rate", f"{ae['death'].mean() * 100:.2f}%" if not ae.empty else "—")
        c4.metric("Linked Recalls", f"{len(recalls):,}")

        # NDC metadata if available
        if not ndc.empty:
            with st.expander("NDC Drug Metadata"):
                st.dataframe(ndc, width="stretch", hide_index=True)

        if ae.empty:
            st.info("No adverse event records found for this drug.")
        else:
            col_time, col_rx = st.columns(2)

            with col_time:
                st.subheader("Reports Over Time")
                t = (
                    ae.assign(period=ae["report_date"].dt.to_period("Q").dt.to_timestamp())
                    .groupby(["period", "serious"])
                    .size()
                    .reset_index(name="n")
                )
                t["type"] = t["serious"].map({True: "Serious", False: "Non-Serious"})
                fig = px.bar(
                    t, x="period", y="n", color="type",
                    color_discrete_map={"Serious": "#e74c3c", "Non-Serious": "#3498db"},
                    barmode="stack",
                    labels={"period": "", "n": "Reports", "type": ""},
                )
                fig.update_layout(height=280, legend_title_text="")
                st.plotly_chart(fig, width="stretch")

            with col_rx:
                st.subheader("Top Reactions")
                rx = (
                    ae["reactions"]
                    .dropna()
                    .str.split("; ")
                    .explode()
                    .str.strip()
                    .loc[lambda s: s != ""]
                    .value_counts()
                    .head(15)
                    .reset_index()
                )
                rx.columns = ["reaction", "count"]
                fig_rx = px.bar(
                    rx.sort_values("count"),
                    x="count", y="reaction", orientation="h",
                    labels={"count": "Reports", "reaction": ""},
                    color="count", color_continuous_scale="Blues",
                )
                fig_rx.update_layout(height=280, coloraxis_showscale=False)
                st.plotly_chart(fig_rx, width="stretch")

            col_sex, col_age = st.columns(2)
            with col_sex:
                sex_c = ae["sex"].value_counts().reset_index()
                sex_c.columns = ["sex", "count"]
                sex_c["pct"] = (sex_c["count"] / sex_c["count"].sum() * 100).round(1)
                fig_s = px.bar(
                    sex_c,
                    x="count", y="sex", orientation="h",
                    title="Reporter Sex",
                    text=sex_c["pct"].apply(lambda p: f"{p}%"),
                    labels={"count": "Reports", "sex": ""},
                    color="sex",
                    color_discrete_map={"Female": "#e91e8c", "Male": "#1976d2", "Unknown": "#9e9e9e"},
                )
                fig_s.update_traces(textposition="outside")
                fig_s.update_layout(height=250, showlegend=False)
                st.plotly_chart(fig_s, width="stretch")

            with col_age:
                age_d = ae["age_years"].dropna()
                age_d = age_d[(age_d >= 0) & (age_d <= 100)]
                if not age_d.empty:
                    fig_a = px.histogram(
                        age_d, nbins=15, title="Age Distribution",
                        labels={"value": "Age (years)"},
                        color_discrete_sequence=["#9b59b6"],
                    )
                    fig_a.update_layout(height=250, showlegend=False)
                    st.plotly_chart(fig_a, width="stretch")

        if not recalls.empty:
            st.subheader("Linked Drug Recalls")
            st.dataframe(recalls, width="stretch", hide_index=True)
        else:
            st.info("No recalls found matching this drug name in product descriptions.")

# ── Tab 2: Safety Landscape ──────────────────────────────────────────────────
with tab_landscape:
    st.subheader("Drug Safety Landscape")
    st.caption(
        "Each point is a drug. X = total adverse event reports (log scale), "
        "Y = percentage of serious events. Size = report volume. "
        "Color = fatality rate. Drugs with ≥ 20 reports shown."
    )

    landscape = safety_landscape(id(conn))

    if landscape.empty:
        st.info("No data available yet.")
    else:
        min_reports = st.slider(
            "Minimum reports", 20, 500, 50,
            help="Filter out low-volume drugs for a cleaner chart",
        )
        land = landscape[landscape["total_reports"] >= min_reports]

        fig_land = px.scatter(
            land,
            x="total_reports",
            y="serious_pct",
            size="total_reports",
            color="death_pct",
            hover_name="drug",
            log_x=True,
            color_continuous_scale="RdYlGn_r",
            labels={
                "total_reports": "Total Reports (log scale)",
                "serious_pct": "Serious Event Rate (%)",
                "death_pct": "Fatality Rate (%)",
            },
            size_max=40,
        )
        fig_land.update_traces(
            hovertemplate="<b>%{hovertext}</b><br>"
                          "Reports: %{x:,}<br>"
                          "Serious: %{y:.1f}%<br>"
                          "Deaths: %{marker.color:.2f}%<extra></extra>"
        )
        fig_land.update_layout(height=600)
        st.plotly_chart(fig_land, width="stretch")

        st.subheader("High-Risk Drugs Table")
        st.caption("Sorted by fatality rate (minimum 50 reports)")
        high_risk = (
            landscape[landscape["total_reports"] >= 50]
            .sort_values("death_pct", ascending=False)
            .head(30)
        )
        st.dataframe(
            high_risk.style.background_gradient(subset=["death_pct", "serious_pct"], cmap="Reds"),
            width="stretch",
            hide_index=True,
        )
