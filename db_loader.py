"""
db_loader.py

ETL script: transforms pipeline output into findings.sqlite.

Sources:
  repos.json                  → repos table
  results/bandit_results.json → findings table
  results/bandit_summary.csv  → scan_summary table

Run:
  python db_loader.py [--repos repos.json]
                      [--results-dir results]
                      [--db findings.sqlite]
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timezone

DB_DEFAULT = "findings.sqlite"
REPOS_DEFAULT = "repos.json"
RESULTS_DIR_DEFAULT = "results"

DDL = """
DROP TABLE IF EXISTS findings;
DROP TABLE IF EXISTS scan_summary;
DROP TABLE IF EXISTS repos;

CREATE TABLE repos (
    full_name         TEXT PRIMARY KEY,
    name              TEXT,
    html_url          TEXT,
    description       TEXT,
    stars             INTEGER,
    forks             INTEGER,
    open_issues       INTEGER,
    language          TEXT,
    topics            TEXT,
    created_at        TEXT,
    updated_at        TEXT,
    license           TEXT,
    repo_age_days     INTEGER,
    days_since_update INTEGER
);

CREATE TABLE findings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    repo             TEXT NOT NULL,
    filename         TEXT,
    test_id          TEXT,
    test_name        TEXT,
    issue_severity   TEXT,
    issue_confidence TEXT,
    issue_text       TEXT,
    line_number      INTEGER,
    code             TEXT,
    FOREIGN KEY (repo) REFERENCES repos(full_name)
);
CREATE INDEX idx_findings_repo     ON findings(repo);
CREATE INDEX idx_findings_severity ON findings(issue_severity);
CREATE INDEX idx_findings_test_id  ON findings(test_id);

CREATE TABLE scan_summary (
    full_name        TEXT NOT NULL,
    py_files_found   INTEGER,
    total_issues     INTEGER,
    high             INTEGER,
    medium           INTEGER,
    low              INTEGER,
    errors           INTEGER,
    bandit_exit_code INTEGER,
    status           TEXT
);
"""


def _age_days(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return (date.today() - dt.date()).days
    except (ValueError, TypeError):
        return None


def load_repos(conn, repos_path):
    if not os.path.exists(repos_path):
        print(f"Warning: {repos_path} not found — repos table will be empty.", file=sys.stderr)
        return 0

    with open(repos_path, encoding="utf-8") as f:
        repos = json.load(f)

    rows = []
    for r in repos:
        rows.append((
            r.get("full_name"),
            r.get("name"),
            r.get("html_url"),
            r.get("description"),
            r.get("stars"),
            r.get("forks"),
            r.get("open_issues"),
            r.get("language"),
            r.get("topics"),
            r.get("created_at"),
            r.get("updated_at"),
            r.get("license"),
            _age_days(r.get("created_at")),
            _age_days(r.get("updated_at")),
        ))

    conn.executemany(
        """INSERT OR REPLACE INTO repos
           (full_name, name, html_url, description, stars, forks, open_issues,
            language, topics, created_at, updated_at, license,
            repo_age_days, days_since_update)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def load_findings(conn, results_dir):
    path = os.path.join(results_dir, "bandit_results.json")
    if not os.path.exists(path):
        print(f"Warning: {path} not found — findings table will be empty.", file=sys.stderr)
        return 0

    with open(path, encoding="utf-8") as f:
        findings = json.load(f)

    rows = []
    for f in findings:
        rows.append((
            f.get("repo"),
            f.get("filename"),
            f.get("test_id"),
            f.get("test_name"),
            f.get("issue_severity"),
            f.get("issue_confidence"),
            f.get("issue_text"),
            f.get("line_number"),
            f.get("code"),
        ))

    conn.executemany(
        """INSERT INTO findings
           (repo, filename, test_id, test_name, issue_severity,
            issue_confidence, issue_text, line_number, code)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def load_scan_summary(conn, results_dir):
    path = os.path.join(results_dir, "bandit_summary.csv")
    if not os.path.exists(path):
        print(f"Warning: {path} not found — scan_summary table will be empty.", file=sys.stderr)
        return 0

    # Deduplicate: prefer status='ok' row per full_name
    best = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            fn = row["full_name"]
            if fn not in best or row["status"] == "ok":
                best[fn] = row

    rows = []
    for row in best.values():
        rows.append((
            row["full_name"],
            int(row.get("py_files_found") or 0),
            int(row.get("total_issues") or 0),
            int(row.get("high") or 0),
            int(row.get("medium") or 0),
            int(row.get("low") or 0),
            int(row.get("errors") or 0),
            int(row.get("bandit_exit_code") or 0),
            row.get("status"),
        ))

    conn.executemany(
        """INSERT INTO scan_summary
           (full_name, py_files_found, total_issues, high, medium, low,
            errors, bandit_exit_code, status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Build findings.sqlite from pipeline output.")
    parser.add_argument("--repos", default=REPOS_DEFAULT)
    parser.add_argument("--results-dir", default=RESULTS_DIR_DEFAULT)
    parser.add_argument("--db", default=DB_DEFAULT)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.executescript(DDL)

    n_repos    = load_repos(conn, args.repos)
    n_findings = load_findings(conn, args.results_dir)
    n_summary  = load_scan_summary(conn, args.results_dir)

    conn.commit()
    conn.close()

    print(f"Built {args.db}:")
    print(f"  repos:        {n_repos}")
    print(f"  findings:     {n_findings}")
    print(f"  scan_summary: {n_summary}")


if __name__ == "__main__":
    main()
