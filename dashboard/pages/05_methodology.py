"""Page 5 — Methodology & Reproducibility"""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from data import get_scan_log
from sidebar import render_sidebar

render_sidebar()  # render for consistency; filters not used on this page

st.title("Methodology & Reproducibility")

# ── Pipeline Diagram ────────────────────────────────────────────────────────
st.header("Pipeline")
st.graphviz_chart("""
digraph pipeline {
    rankdir=LR;
    node [shape=box, style="rounded,filled", fillcolor="#f0f4ff", fontname="sans-serif"];
    edge [fontname="sans-serif", fontsize=10];

    A [label="GitHub Search API\n(language:python, sort:stars)"];
    B [label="github_repo_harvester.py\nrepos.json"];
    C [label="repo_downloader.py\nZIP → .py files"];
    D [label="downloads/"];
    E [label="repo_analyzer.py\nBandit / Semgrep scanner"];
    F [label="results/\nscan_results.json\nscan_summary.csv"];
    G [label="db_loader.py\nfindings.sqlite"];
    H [label="Streamlit Dashboard\n(this app)"];

    A -> B -> C -> D -> E -> F -> G -> H;
}
""")

# ── Scanner Info ─────────────────────────────────────────────────────────────
st.header("Scanner Information")
try:
    import bandit
    bandit_version = bandit.__version__
except Exception:
    bandit_version = "unknown"

try:
    import platform
    py_version = platform.python_version()
except Exception:
    py_version = "unknown"

st.info(
    f"**Scanner:** Bandit {bandit_version}  \n"
    f"**Python:** {py_version}  \n"
    f"**Analysis type:** Static (AST-based)  \n"
    f"**Scope:** `.py` files only — test files, scripts, and source code"
)

# ── Scan Success Log ─────────────────────────────────────────────────────────
st.header("Scan Log")
scan_df = get_scan_log()
if scan_df.empty:
    st.warning("No scan data found. Run `python db_loader.py` first.")
else:
    st.dataframe(
        scan_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "full_name":        st.column_config.TextColumn("Repository"),
            "py_files_found":   st.column_config.NumberColumn("Python Files"),
            "total_issues":     st.column_config.NumberColumn("Total Issues"),
            "high":             st.column_config.NumberColumn("HIGH"),
            "medium":           st.column_config.NumberColumn("MEDIUM"),
            "low":              st.column_config.NumberColumn("LOW"),
            "errors":           st.column_config.NumberColumn("Errors"),
            "exit_code":        st.column_config.NumberColumn("Exit Code"),
            "analyzer":         st.column_config.TextColumn("Analyzer"),
            "status":           st.column_config.TextColumn("Status"),
        },
    )


