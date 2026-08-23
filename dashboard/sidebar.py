"""
sidebar.py — global filter widgets shared across all dashboard pages.
"""

import sqlite3
from pathlib import Path

import streamlit as st

SEVERITY_OPTIONS = ["LOW", "MEDIUM", "HIGH"]
SEV_COLOR_MAP = {
    "LOW":      "#60a5fa",
    "MEDIUM":   "#fbbf24",
    "HIGH":     "#f97316",
    "CRITICAL": "#dc2626",
}
SEVERITY_ORDER = ["LOW", "MEDIUM", "HIGH"]

DB_PATH = Path(__file__).parent.parent / "findings.sqlite"


def _get_available_analyzers() -> list:
    """Query the DB for distinct analyzer values in scan_summary."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cur = conn.execute("SELECT DISTINCT analyzer FROM scan_summary ORDER BY analyzer")
        analyzers = [row[0] for row in cur.fetchall()]
        conn.close()
        return analyzers if analyzers else ["bandit"]
    except Exception:
        return ["bandit"]


def render_sidebar() -> tuple:
    """
    Renders sidebar filter widgets.
    Returns (severities: tuple[str,...], min_stars: int, analyzer: str).
    """
    with st.sidebar:
        st.title("vulnscan-py")
        st.caption("Python Vulnerability Scanner Dashboard")
        st.divider()

        st.subheader("Filters")

        available_analyzers = _get_available_analyzers()
        analyzer = st.selectbox(
            "Analyzer",
            options=available_analyzers,
            index=available_analyzers.index("llm") if "llm" in available_analyzers else 0,
            key="filter_analyzer",
        )

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
            st.session_state["filter_analyzer"] = available_analyzers[0]
            st.rerun()

        st.divider()
        st.caption("Filters apply to all pages except Methodology.")

    return tuple(sorted(severities)), min_stars, analyzer
