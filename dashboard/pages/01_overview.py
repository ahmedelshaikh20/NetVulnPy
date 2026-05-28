"""Page 1 — Overview (landing page / reviewer's five minutes)"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import (
    SEV_COLOR_MAP,
    SEVERITY_ORDER,
    get_age_vs_findings,
    get_kpis,
    get_severity_counts,
    get_stars_vs_findings,
    get_top_rules,
)
from sidebar import render_sidebar

severities, min_stars = render_sidebar()

st.title("vulnscan-py — Overview")
st.caption(
    "Static security analysis of open-source Python repositories using Bandit. "
    "This page summarises answers to the four research questions."
)

# ── KPI Strip ────────────────────────────────────────────────────────────────
kpis = get_kpis(severities, min_stars)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Repos Scanned",      kpis["repos_scanned"])
c2.metric("Total Findings",     kpis["total_findings"])
c3.metric("% Repos with HIGH",  f'{kpis["pct_high"]}%')
c4.metric("Distinct Rules",     kpis["distinct_test_ids"])
c5.metric("Scan Success Rate",  f'{kpis["scanner_success_rate"]}%')

st.divider()

# ── RQ Charts ────────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

# RQ1 — Severity Distribution
with col_left:
    st.subheader("RQ1: Severity Distribution")
    sev_df = get_severity_counts(severities, min_stars)
    if sev_df.empty:
        st.info("No findings with current filters.")
    else:
        fig1 = px.bar(
            sev_df,
            x="issue_severity", y="count",
            color="issue_severity",
            color_discrete_map=SEV_COLOR_MAP,
            category_orders={"issue_severity": SEVERITY_ORDER},
            labels={"issue_severity": "Severity", "count": "Findings"},
            text_auto=True,
        )
        fig1.update_layout(showlegend=False, height=320, margin=dict(t=10))
        st.plotly_chart(fig1, use_container_width=True)
    st.caption("The majority of findings are LOW severity, suggesting common but lower-risk patterns.")

# RQ2 — Top-10 Rules
with col_right:
    st.subheader("RQ2: Top-10 Rules Triggered")
    rules_df = get_top_rules(severities, min_stars, n=10)
    if rules_df.empty:
        st.info("No findings with current filters.")
    else:
        fig2 = px.bar(
            rules_df,
            x="count", y="test_id",
            orientation="h",
            hover_data=["test_name"],
            labels={"count": "Findings", "test_id": "Rule ID"},
            color_discrete_sequence=["#60a5fa"],
        )
        fig2.update_layout(
            yaxis={"autorange": "reversed"},
            height=320,
            margin=dict(t=10),
        )
        st.plotly_chart(fig2, use_container_width=True)
    st.caption("Subprocess and import-related rules dominate, reflecting common scripting patterns.")

st.divider()

col_left2, col_right2 = st.columns(2)

# RQ3 — Stars vs Findings
with col_left2:
    st.subheader("RQ3: Popularity vs Findings")
    scatter_df = get_stars_vs_findings(severities, min_stars)
    valid = scatter_df.dropna(subset=["stars", "total_findings"])
    if len(valid) < 2:
        st.info("Scan more repositories to populate this chart.")
    else:
        fig3 = px.scatter(
            valid,
            x="stars", y="total_findings",
            hover_name="full_name",
            log_x=True,
            trendline="ols" if len(valid) >= 2 else None,
            labels={"stars": "Stars (log)", "total_findings": "Findings"},
            color_discrete_sequence=["#f97316"],
        )
        fig3.update_layout(height=320, margin=dict(t=10))
        st.plotly_chart(fig3, use_container_width=True)
    st.caption("No clear correlation between popularity and finding count suggests quality is independent of fame.")

# RQ4 — Age vs Findings
with col_right2:
    st.subheader("RQ4: Repository Age vs Findings")
    age_df = get_age_vs_findings(severities, min_stars)
    valid2 = age_df.dropna(subset=["repo_age_days", "total_findings"])
    if len(valid2) < 2:
        st.info("Scan more repositories to populate this chart.")
    else:
        fig4 = px.scatter(
            valid2,
            x="repo_age_days", y="total_findings",
            hover_name="full_name",
            trendline="ols" if len(valid2) >= 2 else None,
            labels={"repo_age_days": "Age (days)", "total_findings": "Findings"},
            color_discrete_sequence=["#fbbf24"],
        )
        fig4.update_layout(height=320, margin=dict(t=10))
        st.plotly_chart(fig4, use_container_width=True)
    st.caption("Older repositories do not necessarily accumulate more findings than newer ones.")

st.divider()

# ── Dataset Provenance ────────────────────────────────────────────────────────
from datetime import date
st.info(
    f"Dataset: top-starred Python repositories on GitHub — "
    f"last pipeline run analysed {kpis['repos_scanned']} repo(s) with Bandit. "
    f"Dashboard generated on {date.today().isoformat()}."
)
