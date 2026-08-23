"""
main.py — vulnscan-py full pipeline runner

Stages:
  1. Harvest   — query GitHub API for Python repos → repos.json
  2. Download  — download ZIPs and extract .py files → downloads/
  3. Analyze   — run Bandit, Semgrep, pip-audit, Skylos, or LLM on extracted files → results/
  4. Load DB   — import results into findings.sqlite
  5. Triage    — LLM-assisted false-positive classification (optional)

Usage:
  python main.py [--max-repos N] [--limit N] [--token TOKEN]
                 [--output-dir DIR]
                 [--analyzer {bandit,semgrep,pip-audit,skylos,llm}]
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
    parser.add_argument("--analyzer",  choices=["bandit", "semgrep", "skylos", "pip-audit", "llm"], default="bandit",
                        help="Security analyzer to use.")
    parser.add_argument("--skip-analyze",   action="store_true", help="Skip the analysis stage.")

    # LLM analyzer options
    parser.add_argument("--llm-api-key", default=None,
                        help="API key for LLM inference endpoint (or set LLM_API_KEY env var).")
    parser.add_argument("--llm-endpoint", default="https://llms.innkube.fim.uni-passau.de",
                        help="Base URL for OpenAI-compatible LLM endpoint.")
    parser.add_argument("--llm-model", default="qwen3-next-80b-a3b-instruct",
                        help="Model name for LLM analyzer.")

    # Stage 4 — DB
    parser.add_argument("--db",         default="findings.sqlite", help="SQLite output path.")
    parser.add_argument("--skip-db",    action="store_true", help="Skip the database load stage.")

    # Stage 5 — LLM Triage (optional)
    parser.add_argument("--triage",     action="store_true",
                        help="Run LLM-assisted false-positive triage on findings.")
    parser.add_argument("--triage-input", default=None,
                        help="Path to scan_results.json for triage (default: results/scan_results.json)")
    parser.add_argument("--triage-sample", type=int, default=100,
                        help="Findings to sample per severity level (default: 100)")
    parser.add_argument("--triage-model", default="claude-haiku-4-5-20251001",
                        help="Anthropic model for triage (default: claude-haiku-4-5-20251001)")

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
    all_files = getattr(args, "analyzer", "bandit") in ("skylos", "pip-audit")
    label = "fetching ZIPs and extracting all files" if all_files else "fetching ZIPs and extracting .py files"
    _banner(f"Stage 2 — Download: {label}")
    from repo_downloader import run as _run
    dl_args = types.SimpleNamespace(
        input=os.path.join(args.output_dir, "repos.json") if args.output_dir != "." else "repos.json",
        output_dir=args.output_dir,
        token=args.token,
        limit=args.limit,
        keep_files=True,   # keep extracted dirs for analyzer (and future re-runs)
        verbose=args.verbose,
        workers=args.workers,
        all_files=all_files,
    )
    _run(dl_args)


def stage_analyze(args):
    _banner(f"Stage 3 — Analyze: running {args.analyzer} on extracted repos")
    from repo_analyzer import run as _run
    downloads_dir = os.path.join(args.output_dir, "downloads")
    results_dir   = os.path.join(args.output_dir, "results", args.analyzer)
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
        llm_api_key=getattr(args, "llm_api_key", None),
        llm_endpoint=getattr(args, "llm_endpoint", None),
        llm_model=getattr(args, "llm_model", None),
    )
    _run(analyze_args)


def stage_triage(args):
    _banner("Stage 5 — Triage: LLM-assisted false-positive classification")
    from llm_triage import run_triage

    results_dir = os.path.join(args.output_dir, "results", args.analyzer)
    input_path = args.triage_input or os.path.join(results_dir, "scan_results.json")
    output_path = os.path.join(results_dir, "triage_results.json")

    run_triage(
        input_path=input_path,
        output_path=output_path,
        n_per_severity=args.triage_sample,
        model=args.triage_model,
        verbose=args.verbose,
    )


def stage_load_db(args):
    _banner("Stage 4 — Load DB: importing results into findings.sqlite")
    from db_loader import load_repos, load_findings, load_scan_summary, DDL
    import sqlite3

    repos_path  = os.path.join(args.output_dir, "repos.json") if args.output_dir != "." else "repos.json"
    results_root = os.path.join(args.output_dir, "results")
    db_path     = args.db

    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)
    n_repos = load_repos(conn, repos_path)

    # Load findings from all analyzer subdirs under results/
    total_findings = 0
    total_summary = 0
    if os.path.isdir(results_root):
        for entry in sorted(os.listdir(results_root)):
            subdir = os.path.join(results_root, entry)
            if os.path.isdir(subdir):
                nf = load_findings(conn, subdir)
                ns = load_scan_summary(conn, subdir)
                total_findings += nf
                total_summary += ns
                if args.verbose:
                    print(f"    {entry}: {nf} findings, {ns} summaries")

    conn.commit()
    conn.close()

    print(f"\n  Built {db_path}:")
    print(f"    repos:        {n_repos}")
    print(f"    findings:     {total_findings}")
    print(f"    scan_summary: {total_summary}")


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

    if args.triage:
        stage_triage(args)

    _banner("Pipeline complete")
    print(f"  Dashboard: streamlit run dashboard/app.py")
    print(f"  Database:  {args.db}\n")


if __name__ == "__main__":
    main()
