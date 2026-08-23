"""Page 4 — Repository Explorer"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import (
    SEV_COLOR_MAP,
    SEVERITY_ORDER,
    get_repo_findings,
    get_repo_leaderboard,
    get_severity_counts_for_repo,
)
from sidebar import render_sidebar

severities, min_stars, analyzer = render_sidebar()

st.title("Repository Explorer")

# ── Leaderboard ───────────────────────────────────────────────────────────────
st.subheader("Leaderboard")

df = get_repo_leaderboard(severities, min_stars, analyzer)

query = st.text_input("Search repositories", "", placeholder="e.g. flask, django …")
if query:
    df = df[df["full_name"].str.contains(query, case=False, na=False)]

if df.empty:
    st.info("No repositories match the current filters.")
    st.stop()

event = st.dataframe(
    df[["full_name", "stars", "size_kb", "total_findings", "findings_per_kloc",
        "high", "medium", "low", "py_files_found", "status"]],
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "full_name":         st.column_config.TextColumn("Repository"),
        "stars":             st.column_config.NumberColumn("Stars", format="%d"),
        "size_kb":           st.column_config.NumberColumn("Size (KB)", format="%d"),
        "total_findings":    st.column_config.NumberColumn("Findings"),
        "findings_per_kloc": st.column_config.NumberColumn("/ KLOC", format="%.2f"),
        "high":              st.column_config.NumberColumn("HIGH"),
        "medium":            st.column_config.NumberColumn("MEDIUM"),
        "low":               st.column_config.NumberColumn("LOW"),
        "py_files_found":    st.column_config.NumberColumn("Python Files"),
        "status":            st.column_config.TextColumn("Scan Status"),
    },
)

# Persist selected repo in session_state
if event.selection.rows:
    st.session_state["selected_repo"] = df.iloc[event.selection.rows[0]]["full_name"]

# ── Repo Detail ───────────────────────────────────────────────────────────────
if "selected_repo" not in st.session_state:
    st.info("Click a row above to see repository details.")
    st.stop()

repo_name = st.session_state["selected_repo"]
st.divider()
st.subheader(f"Detail: {repo_name}")

col_meta, col_donut = st.columns([2, 1])

with col_meta:
    row = df[df["full_name"] == repo_name]
    if not row.empty:
        r = row.iloc[0]
        st.markdown(f"""
| Field | Value |
|---|---|
| Stars | {int(r.stars):,} |
| Forks | {int(r.forks):,} |
| Python Files | {int(r.py_files_found)} |
| Total Findings | {int(r.total_findings)} |
| Scan Status | `{r.status}` |
""")

with col_donut:
    sev_df = get_severity_counts_for_repo(repo_name, severities, analyzer)
    if not sev_df.empty:
        fig = px.pie(
            sev_df,
            names="issue_severity",
            values="count",
            hole=0.5,
            color="issue_severity",
            color_discrete_map=SEV_COLOR_MAP,
            title="Severity Breakdown",
            category_orders={"issue_severity": SEVERITY_ORDER},
        )
        fig.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=280)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No findings for this repository with the current severity filter.")

# Findings table
st.subheader("Findings")
findings_df = get_repo_findings(repo_name, severities, analyzer)

if findings_df.empty:
    st.info("No findings found with the current filters.")
else:
    display_cols = ["issue_severity", "test_id", "test_name", "filename",
                    "line_number", "issue_text", "github_link"]
    st.dataframe(
        findings_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "issue_severity": st.column_config.TextColumn("Severity"),
            "test_id":        st.column_config.TextColumn("Rule ID"),
            "test_name":      st.column_config.TextColumn("Rule"),
            "filename":       st.column_config.TextColumn("File"),
            "line_number":    st.column_config.NumberColumn("Line"),
            "issue_text":     st.column_config.TextColumn("Issue"),
            "github_link":    st.column_config.LinkColumn("Source", display_text="View on GitHub"),
        },
    )

    csv_bytes = findings_df.drop(columns=["github_link"], errors="ignore").to_csv(index=False).encode()
    st.download_button(
        "Download findings CSV",
        data=csv_bytes,
        file_name=f"{repo_name.replace('/', '_')}_findings.csv",
        mime="text/csv",
    )
