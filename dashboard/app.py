"""
app.py — vulnscan-py Streamlit entry point.

Run:
    streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="vulnscan-py",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page("pages/01_overview.py",       title="Overview",               icon="📊"),
    st.Page("pages/02_vuln_explorer.py",  title="Vulnerability Explorer", icon="🔎"),
    st.Page("pages/03_correlation.py",    title="Correlation Analysis",   icon="📈"),
    st.Page("pages/04_repo_explorer.py",  title="Repository Explorer",    icon="🗂️"),
    st.Page("pages/05_methodology.py",    title="Methodology",            icon="📋"),
]

pg = st.navigation(pages)
pg.run()
