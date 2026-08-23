"""Page 3 — Correlation Analysis"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import SEV_COLOR_MAP, get_age_vs_findings, get_size_vs_findings, get_stars_vs_findings
from sidebar import render_sidebar

severities, min_stars, analyzer = render_sidebar()

st.title("Correlation Analysis")
st.caption("Exploring relationships between repository metadata and vulnerability counts.")

try:
    from scipy import stats
    SCIPY_OK = True
except ImportError:
    SCIPY_OK = False

# ── Stars vs Findings ─────────────────────────────────────────────────────────
st.subheader("RQ3: Repository Popularity vs Findings")

stars_df = get_stars_vs_findings(severities, min_stars, analyzer)
valid = stars_df.dropna(subset=["stars", "total_findings"])

if len(valid) < 2:
    st.warning("Not enough data points for correlation. Scan more repositories to populate this view.")
else:
    if SCIPY_OK and len(valid) >= 2:
        r_p, p_p = stats.pearsonr(valid["stars"], valid["total_findings"])
        r_s, p_s = stats.spearmanr(valid["stars"], valid["total_findings"])

        c1, c2 = st.columns(2)
        c1.metric("Pearson r", f"{r_p:.3f}", delta=f"p = {p_p:.3f}")
        c2.metric("Spearman ρ", f"{r_s:.3f}", delta=f"p = {p_s:.3f}")

        if len(valid) < 5:
            st.warning(f"Only {len(valid)} repos with scan data — correlations are not statistically reliable.")

    y_col = "findings_per_kloc" if valid["findings_per_kloc"].notna().any() else "total_findings"
    y_label = "Findings / KLOC" if y_col == "findings_per_kloc" else "Total Findings"
    fig = px.scatter(
        valid.dropna(subset=[y_col]),
        x="stars", y=y_col,
        hover_name="full_name",
        hover_data={"high": True, "medium": True, "low": True},
        log_x=True,
        trendline="ols" if len(valid) >= 2 else None,
        labels={"stars": "Stars (log scale)", y_col: y_label},
        title="Stars vs Findings per KLOC",
        color_discrete_sequence=["#60a5fa"],
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Each point is one repository. Y-axis is normalised by lines of code.")

st.divider()

# ── Size vs Findings ──────────────────────────────────────────────────────────
st.subheader("Repository Size (KB) vs Findings per KLOC")

size_df = get_size_vs_findings(severities, min_stars, analyzer)
valid_size = size_df.dropna(subset=["size_kb", "findings_per_kloc"])

if len(valid_size) < 2:
    st.info("Not enough data points with LOC information yet.")
else:
    if SCIPY_OK and len(valid_size) >= 2:
        r_p3, p_p3 = stats.pearsonr(valid_size["size_kb"], valid_size["findings_per_kloc"])
        r_s3, p_s3 = stats.spearmanr(valid_size["size_kb"], valid_size["findings_per_kloc"])
        c1, c2 = st.columns(2)
        c1.metric("Pearson r", f"{r_p3:.3f}", delta=f"p = {p_p3:.3f}")
        c2.metric("Spearman ρ", f"{r_s3:.3f}", delta=f"p = {p_s3:.3f}")

    fig_size = px.scatter(
        valid_size,
        x="size_kb", y="findings_per_kloc",
        hover_name="full_name",
        trendline="ols" if len(valid_size) >= 2 else None,
        labels={"size_kb": "Repository Size (KB)", "findings_per_kloc": "Findings / KLOC"},
        title="Repository Size vs Findings per KLOC",
        color_discrete_sequence=["#a78bfa"],
    )
    fig_size.update_layout(height=400)
    st.plotly_chart(fig_size, use_container_width=True)
    st.caption("Larger repositories do not necessarily have higher vulnerability density.")

st.divider()

# ── Repo Age Trend ─────────────────────────────────────────────────────────────
st.subheader("Repository Age vs Findings")

age_df = get_age_vs_findings(severities, min_stars, analyzer)
valid3 = age_df.dropna(subset=["repo_age_days", "total_findings"]).sort_values("repo_age_days")

if valid3.empty:
    st.info("No data available.")
else:
    fig3 = px.line(
        valid3,
        x="repo_age_days", y="total_findings",
        markers=True,
        hover_name="full_name",
        labels={"repo_age_days": "Repository Age (days)", "total_findings": "Total Findings"},
        title="Repository Age vs Findings",
        color_discrete_sequence=["#f97316"],
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, use_container_width=True)
    st.caption("Points connected in order of repository age.")
