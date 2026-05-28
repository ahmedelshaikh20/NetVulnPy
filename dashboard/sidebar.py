"""
sidebar.py — global filter widgets shared across all dashboard pages.
"""

import streamlit as st

SEVERITY_OPTIONS = ["LOW", "MEDIUM", "HIGH"]
SEV_COLOR_MAP = {
    "LOW":      "#60a5fa",
    "MEDIUM":   "#fbbf24",
    "HIGH":     "#f97316",
    "CRITICAL": "#dc2626",
}
SEVERITY_ORDER = ["LOW", "MEDIUM", "HIGH"]


def render_sidebar() -> tuple:
    """
    Renders sidebar filter widgets.
    Returns (severities: tuple[str,...], min_stars: int).
    """
    with st.sidebar:
        st.title("vulnscan-py")
        st.caption("Python Vulnerability Scanner Dashboard")
        st.divider()

        st.subheader("Filters")

        severities = st.multiselect(
            "Severity",
            options=SEVERITY_OPTIONS,
            default=SEVERITY_OPTIONS,
            key="filter_severities",
        )
        if not severities:
            severities = SEVERITY_OPTIONS

        min_stars = st.slider(
            "Minimum Stars",
            min_value=0,
            max_value=500_000,
            value=0,
            step=10_000,
            key="filter_min_stars",
        )

        if st.button("Reset Filters", use_container_width=True):
            st.session_state["filter_severities"] = SEVERITY_OPTIONS
            st.session_state["filter_min_stars"] = 0
            st.rerun()

        st.divider()
        st.caption("Filters apply to all pages except Methodology.")

    return tuple(sorted(severities)), min_stars
