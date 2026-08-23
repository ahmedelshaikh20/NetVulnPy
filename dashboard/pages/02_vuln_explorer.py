"""Page 2 — Vulnerability Explorer"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import (
    SEV_COLOR_MAP,
    SEVERITY_ORDER,
    get_findings_histogram,
    get_findings_per_kloc,
    get_severity_counts,
    get_top_rules,
)
from sidebar import render_sidebar

severities, min_stars, analyzer = render_sidebar()

st.title("Vulnerability Explorer")

# ── Top row: severity bar + histogram ────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Findings by Severity")
    sev_df = get_severity_counts(severities, min_stars, analyzer)
    if sev_df.empty:
        st.info("No findings with current filters.")
    else:
        fig = px.bar(
            sev_df,
            x="issue_severity", y="count",
            color="issue_severity",
            color_discrete_map=SEV_COLOR_MAP,
            category_orders={"issue_severity": SEVERITY_ORDER},
            labels={"issue_severity": "Severity", "count": "Findings"},
            text_auto=True,
        )
        fig.update_layout(showlegend=False, height=350, margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Total findings grouped by {analyzer} severity level.")

with col2:
    st.subheader("Findings per KLOC")
    kloc_df = get_findings_per_kloc(severities, min_stars, analyzer)
    if kloc_df.empty:
        st.info("No LOC data yet — run the analyzer on downloaded repos first.")
    else:
        fig2 = px.histogram(
            kloc_df,
            x="findings_per_kloc",
            nbins=max(5, len(kloc_df)),
            labels={"findings_per_kloc": "Findings / KLOC", "count": "Repositories"},
            color_discrete_sequence=["#60a5fa"],
        )
        fig2.update_layout(height=350, margin=dict(t=20))
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("Normalised finding rate per 1,000 lines of Python code.")

st.divider()

# ── Top-20 Rules Table ────────────────────────────────────────────────────────
st.subheader("Top-20 Rules Triggered")
rules_df = get_top_rules(severities, min_stars, n=20, analyzer=analyzer)
if rules_df.empty:
    st.info("No rules triggered with current filters.")
else:
    st.dataframe(
        rules_df.rename(columns={
            "test_id":   "Rule ID",
            "test_name": "Rule Name",
            "count":     "Count",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Count": st.column_config.ProgressColumn(
                "Count",
                format="%d",
                min_value=0,
                max_value=int(rules_df["count"].max()),
            )
        },
    )
    st.caption("Sorted by frequency. Reflects current severity and star filters.")

