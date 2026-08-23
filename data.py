"""
data.py

All cached query functions for the vulnscan-py dashboard.
Import this module from Streamlit page files.
"""

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent / "findings.sqlite"

SEV_COLOR_MAP = {
    "LOW":      "#60a5fa",
    "MEDIUM":   "#fbbf24",
    "HIGH":     "#f97316",
    "CRITICAL": "#dc2626",
}
SEVERITY_ORDER = ["LOW", "MEDIUM", "HIGH"]


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def _placeholders(seq):
    return ",".join("?" * len(seq))


def _analyzer_findings_filter(analyzer: str) -> str:
    """Return a SQL WHERE clause fragment to filter findings by analyzer.

    Since the findings table has no analyzer column, we use the scan_summary
    table to identify repos scanned by the given analyzer, and combine with
    test_id prefix matching for accuracy.
    """
    # Map analyzer to known test_id prefixes
    prefix_map = {
        "bandit":    "B",
        "llm":       "LLM-",
        "pip-audit": "CVE-",
        "skylos":    "SKY-",
    }
    prefix = prefix_map.get(analyzer)
    if prefix:
        return f"f.test_id LIKE '{prefix}%'"
    # For semgrep and others, fall back to scan_summary join
    return (
        f"EXISTS (SELECT 1 FROM scan_summary ss "
        f"WHERE ss.full_name = f.repo AND ss.analyzer = '{analyzer}')"
    )


# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_kpis(severities: tuple, min_stars: int, analyzer: str = "bandit") -> dict:
    ph = _placeholders(severities)
    conn = _conn()
    af = _analyzer_findings_filter(analyzer)

    repos_scanned = pd.read_sql_query(
        "SELECT COUNT(DISTINCT full_name) AS n FROM scan_summary WHERE status='ok' AND analyzer=?",
        conn, params=(analyzer,),
    ).iloc[0, 0]

    total_findings = pd.read_sql_query(
        f"""SELECT COUNT(*) AS n FROM findings f
            JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}""",
        conn, params=(*severities, min_stars),
    ).iloc[0, 0]

    high_repos = pd.read_sql_query(
        f"""SELECT COUNT(DISTINCT f.repo) AS n FROM findings f
            JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity = 'HIGH' AND r.stars >= ?
            AND {af}""",
        conn, params=(min_stars,),
    ).iloc[0, 0]

    pct_high = round(high_repos / max(repos_scanned, 1) * 100, 1)

    distinct_rules = pd.read_sql_query(
        f"""SELECT COUNT(DISTINCT test_id) AS n FROM findings f
            JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}""",
        conn, params=(*severities, min_stars),
    ).iloc[0, 0]

    total_scanned = pd.read_sql_query(
        "SELECT COUNT(*) AS n FROM scan_summary WHERE analyzer=?", conn, params=(analyzer,),
    ).iloc[0, 0]
    ok_scanned = pd.read_sql_query(
        "SELECT COUNT(*) AS n FROM scan_summary WHERE status='ok' AND analyzer=?", conn, params=(analyzer,),
    ).iloc[0, 0]
    success_rate = round(ok_scanned / max(total_scanned, 1) * 100, 1)

    conn.close()
    return {
        "repos_scanned":       int(repos_scanned),
        "total_findings":      int(total_findings),
        "pct_high":            pct_high,
        "distinct_test_ids":   int(distinct_rules),
        "scanner_success_rate": success_rate,
    }


# ---------------------------------------------------------------------------
# Severity distribution
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_severity_counts(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.issue_severity, COUNT(*) AS count
            FROM findings f JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}
            GROUP BY f.issue_severity""",
        conn, params=(*severities, min_stars),
    )
    conn.close()
    df["issue_severity"] = pd.Categorical(df["issue_severity"], categories=SEVERITY_ORDER, ordered=True)
    return df.sort_values("issue_severity")


# ---------------------------------------------------------------------------
# Top rules
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_top_rules(severities: tuple, min_stars: int, n: int = 20, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.test_id, f.test_name, COUNT(*) AS count
            FROM findings f JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}
            GROUP BY f.test_id, f.test_name
            ORDER BY count DESC
            LIMIT {int(n)}""",
        conn, params=(*severities, min_stars),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Stars vs findings (scatter)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_stars_vs_findings(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """SELECT r.full_name, r.stars, r.size_kb,
                  COALESCE(s.total_issues, 0) AS total_findings,
                  COALESCE(s.high, 0)         AS high,
                  COALESCE(s.medium, 0)       AS medium,
                  COALESCE(s.low, 0)          AS low,
                  COALESCE(s.loc, 0)          AS loc
           FROM repos r
           LEFT JOIN scan_summary s ON r.full_name = s.full_name AND s.analyzer = ?
           WHERE r.stars >= ?""",
        conn, params=(analyzer, min_stars),
    )
    conn.close()
    df["findings_per_kloc"] = df.apply(
        lambda row: round(row["total_findings"] / (row["loc"] / 1000), 2)
        if row["loc"] > 0 else None, axis=1
    )
    return df


@st.cache_data(ttl=None)
def get_size_vs_findings(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """SELECT r.full_name, r.size_kb,
                  COALESCE(s.total_issues, 0) AS total_findings,
                  COALESCE(s.loc, 0)          AS loc
           FROM repos r
           LEFT JOIN scan_summary s ON r.full_name = s.full_name AND s.analyzer = ?
           WHERE r.stars >= ? AND r.size_kb IS NOT NULL""",
        conn, params=(analyzer, min_stars),
    )
    conn.close()
    df["findings_per_kloc"] = df.apply(
        lambda row: round(row["total_findings"] / (row["loc"] / 1000), 2)
        if row["loc"] > 0 else None, axis=1
    )
    return df


# ---------------------------------------------------------------------------
# Age vs findings (scatter / line)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_age_vs_findings(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """SELECT r.full_name, r.stars, r.repo_age_days, r.days_since_update,
                  COALESCE(s.total_issues, 0) AS total_findings
           FROM repos r
           LEFT JOIN scan_summary s ON r.full_name = s.full_name AND s.analyzer = ?
           WHERE r.stars >= ?""",
        conn, params=(analyzer, min_stars),
    )
    conn.close()
    return df.dropna(subset=["repo_age_days"])


# ---------------------------------------------------------------------------
# Treemap (severity → rule)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_treemap_data(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.issue_severity, f.test_name, COUNT(*) AS count
            FROM findings f JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}
            GROUP BY f.issue_severity, f.test_name""",
        conn, params=(*severities, min_stars),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Findings histogram (distribution per repo)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_findings_per_kloc(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.repo, COUNT(*) AS finding_count, s.loc
            FROM findings f
            JOIN repos r ON f.repo = r.full_name
            JOIN scan_summary s ON f.repo = s.full_name AND s.analyzer = ?
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ? AND s.loc > 0
            AND {af}
            GROUP BY f.repo""",
        conn, params=(analyzer, *severities, min_stars),
    )
    conn.close()
    df["findings_per_kloc"] = df["finding_count"] / (df["loc"] / 1000)
    return df


@st.cache_data(ttl=None)
def get_findings_histogram(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.repo, COUNT(*) AS finding_count
            FROM findings f JOIN repos r ON f.repo = r.full_name
            WHERE f.issue_severity IN ({ph}) AND r.stars >= ?
            AND {af}
            GROUP BY f.repo""",
        conn, params=(*severities, min_stars),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Repository leaderboard
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_repo_leaderboard(severities: tuple, min_stars: int, analyzer: str = "bandit") -> pd.DataFrame:
    conn = _conn()
    df = pd.read_sql_query(
        """SELECT r.full_name, r.stars, r.forks, r.size_kb,
                  COALESCE(s.total_issues, 0)   AS total_findings,
                  COALESCE(s.high, 0)            AS high,
                  COALESCE(s.medium, 0)          AS medium,
                  COALESCE(s.low, 0)             AS low,
                  COALESCE(s.py_files_found, 0)  AS py_files_found,
                  COALESCE(s.loc, 0)             AS loc,
                  COALESCE(s.status, 'not scanned') AS status,
                  r.repo_age_days
           FROM repos r
           LEFT JOIN scan_summary s ON r.full_name = s.full_name AND s.analyzer = ?
           WHERE r.stars >= ?
           ORDER BY total_findings DESC, r.stars DESC""",
        conn, params=(analyzer, min_stars),
    )
    conn.close()
    df["findings_per_kloc"] = df.apply(
        lambda row: round(row["total_findings"] / (row["loc"] / 1000), 2)
        if row["loc"] > 0 else None, axis=1
    )
    return df


# ---------------------------------------------------------------------------
# Per-repo findings (detail view)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_repo_findings(repo_full_name: str, severities: tuple, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT f.filename, f.test_id, f.test_name,
                   f.issue_severity, f.issue_confidence,
                   f.line_number, f.issue_text, f.code,
                   r.html_url
            FROM findings f JOIN repos r ON f.repo = r.full_name
            WHERE f.repo = ? AND f.issue_severity IN ({ph})
            AND {af}
            ORDER BY f.issue_severity DESC, f.filename, f.line_number""",
        conn, params=(repo_full_name, *severities),
    )
    conn.close()
    if not df.empty:
        df["github_link"] = (
            df["html_url"] + "/blob/HEAD/" + df["filename"]
            + "#L" + df["line_number"].astype(str)
        )
        df = df.drop(columns=["html_url"])
    return df


# ---------------------------------------------------------------------------
# Per-repo severity counts (donut)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_severity_counts_for_repo(repo_full_name: str, severities: tuple, analyzer: str = "bandit") -> pd.DataFrame:
    ph = _placeholders(severities)
    af = _analyzer_findings_filter(analyzer)
    conn = _conn()
    df = pd.read_sql_query(
        f"""SELECT issue_severity, COUNT(*) AS count
            FROM findings f
            WHERE f.repo = ? AND issue_severity IN ({ph})
            AND {af}
            GROUP BY issue_severity""",
        conn, params=(repo_full_name, *severities),
    )
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Scan log (methodology page)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=None)
def get_scan_log(analyzer: str = None) -> pd.DataFrame:
    conn = _conn()
    if analyzer:
        df = pd.read_sql_query(
            "SELECT * FROM scan_summary WHERE analyzer=? ORDER BY full_name",
            conn, params=(analyzer,),
        )
    else:
        df = pd.read_sql_query("SELECT * FROM scan_summary ORDER BY full_name", conn)
    conn.close()
    return df
