"""
main.py — vulnscan-py full pipeline runner

Stages:
  1. Harvest   — query GitHub API for Python repos → repos.json
  2. Download  — download ZIPs and extract .py files → downloads/
  3. Analyze   — run Bandit or Semgrep on extracted files → results/
  4. Load DB   — import results into findings.sqlite

Usage:
  python main.py [--max-repos N] [--limit N] [--token TOKEN]
                 [--output-dir DIR] [--analyzer {bandit,semgrep}]
                 [--verbose] [--skip-harvest]
                 [--skip-download] [--skip-analyze] [--skip-db]

Examples:
  # Full pipeline (10 repos, verbose)
  python main.py --max-repos 10 --limit 10 --verbose

  # Re-run analysis + DB only (repos already downloaded)
  python main.py --skip-harvest --skip-download --verbose
"""

import argparse
import os
import sys
import types


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the full vulnscan-py pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Shared
    parser.add_argument("--token",      default=None,    help="GitHub personal access token.")
    parser.add_argument("--output-dir", default=".",     help="Root output directory.")
    parser.add_argument("--verbose",    action="store_true", help="Print progress to stderr.")
    parser.add_argument("--workers",    type=int, default=4,
                        help="Parallel workers for download/analyze stages.")

    # Stage 1 — Harvest
    parser.add_argument("--max-repos",  type=int, default=2000,
                        help="Max repos to harvest from GitHub.")
    parser.add_argument("--skip-harvest",   action="store_true", help="Skip the harvest stage.")

    # Stage 2 — Download
    parser.add_argument("--limit",      type=int, default=None,
                        help="Max repos to download/analyze (default: all harvested).")
    parser.add_argument("--keep-files", action="store_true",
                        help="Keep downloaded ZIPs and extracted files after analysis.")
    parser.add_argument("--skip-download",  action="store_true", help="Skip the download stage.")

    # Stage 3 — Analyze
    parser.add_argument("--analyzer",  choices=["bandit", "semgrep"], default="bandit",
                        help="Security analyzer to use.")
    parser.add_argument("--skip-analyze",   action="store_true", help="Skip the analysis stage.")

    # Stage 4 — DB
    parser.add_argument("--db",         default="findings.sqlite", help="SQLite output path.")
    parser.add_argument("--skip-db",    action="store_true", help="Skip the database load stage.")

    return parser.parse_args()


def _banner(msg):
    print(f"\n{'─' * 60}", flush=True)
    print(f"  {msg}", flush=True)
    print(f"{'─' * 60}", flush=True)


def stage_harvest(args):
    _banner("Stage 1 — Harvest: fetching Python repos from GitHub")
    from github_repo_harvester import run as _run
    harvest_args = types.SimpleNamespace(
        max_repos=args.max_repos,
        output_dir=args.output_dir,
        token=args.token,
        per_page=100,
        verbose=args.verbose,
    )
    _run(harvest_args)


def stage_download(args):
    _banner("Stage 2 — Download: fetching ZIPs and extracting .py files")
    from repo_downloader import run as _run
    dl_args = types.SimpleNamespace(
        input=os.path.join(args.output_dir, "repos.json") if args.output_dir != "." else "repos.json",
        output_dir=args.output_dir,
        token=args.token,
        limit=args.limit,
        keep_files=False,  # delete ZIPs after extraction; extracted dirs kept for analyzer
        verbose=args.verbose,
        workers=args.workers,
    )
    _run(dl_args)


def stage_analyze(args):
    _banner(f"Stage 3 — Analyze: running {args.analyzer} on extracted repos")
    from repo_analyzer import run as _run
    downloads_dir = os.path.join(args.output_dir, "downloads")
    results_dir   = os.path.join(args.output_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    # Clear previous results so this run starts fresh
    for fname in ("scan_results.json", "scan_summary.csv"):
        fpath = os.path.join(results_dir, fname)
        if os.path.exists(fpath):
            os.remove(fpath)

    analyze_args = types.SimpleNamespace(
        downloads_dir=downloads_dir,
        output_dir=results_dir,
        analyzer=args.analyzer,
        limit=args.limit,
        keep_files=args.keep_files,
        verbose=args.verbose,
        workers=args.workers,
    )
    _run(analyze_args)


def stage_load_db(args):
    _banner("Stage 4 — Load DB: importing results into findings.sqlite")
    from db_loader import load_repos, load_findings, load_scan_summary, DDL
    import sqlite3

    repos_path  = os.path.join(args.output_dir, "repos.json") if args.output_dir != "." else "repos.json"
    results_dir = os.path.join(args.output_dir, "results")
    db_path     = args.db

    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)
    n_repos    = load_repos(conn, repos_path)
    n_findings = load_findings(conn, results_dir)
    n_summary  = load_scan_summary(conn, results_dir)
    conn.commit()
    conn.close()

    print(f"\n  Built {db_path}:")
    print(f"    repos:        {n_repos}")
    print(f"    findings:     {n_findings}")
    print(f"    scan_summary: {n_summary}")


def main():
    args = parse_args()

    print("vulnscan-py pipeline starting …")

    if not args.skip_harvest:
        stage_harvest(args)
    else:
        print("Skipping Stage 1 (--skip-harvest)")

    if not args.skip_download:
        stage_download(args)
    else:
        print("Skipping Stage 2 (--skip-download)")

    if not args.skip_analyze:
        stage_analyze(args)
    else:
        print("Skipping Stage 3 (--skip-analyze)")

    if not args.skip_db:
        stage_load_db(args)
    else:
        print("Skipping Stage 4 (--skip-db)")

    _banner("Pipeline complete")
    print(f"  Dashboard: streamlit run dashboard/app.py")
    print(f"  Database:  {args.db}\n")


if __name__ == "__main__":
    main()
