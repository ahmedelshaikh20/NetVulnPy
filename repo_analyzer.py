"""
repo_analyzer.py  —  Phase 2: Bandit Analysis

Scans the downloads/ directory produced by repo_downloader.py, runs Bandit
security analysis on each repo's .py files, and aggregates findings into:
  - bandit_results.json  : flat list of all individual findings
  - bandit_summary.csv   : one row per repo with severity counts

Usage:
    python repo_analyzer.py [--downloads-dir downloads] [--output-dir .]
                            [--limit N] [--verbose]
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys

SUMMARY_FIELDS = [
    "full_name",
    "py_files_found",
    "total_issues",
    "high",
    "medium",
    "low",
    "errors",
    "bandit_exit_code",
    "status",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Bandit on extracted repos and aggregate results."
    )
    parser.add_argument(
        "--downloads-dir",
        default="downloads",
        help="Directory containing extracted repo subdirectories (default: downloads).",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write bandit_results.json and bandit_summary.csv (default: .).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of repos to analyze (default: all).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress to stderr.",
    )
    return parser.parse_args()


def find_repo_dirs(downloads_dir, limit):
    """Return list of (full_name, dir_path) tuples for each repo subdirectory."""
    if not os.path.isdir(downloads_dir):
        print(f"Error: downloads directory '{downloads_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    entries = sorted(
        e for e in os.listdir(downloads_dir)
        if os.path.isdir(os.path.join(downloads_dir, e))
    )

    if limit is not None:
        entries = entries[:limit]

    repos = []
    for entry in entries:
        # entry is owner_repo — convert back to owner/repo for display
        full_name = entry.replace("_", "/", 1)
        repos.append((full_name, os.path.join(downloads_dir, entry)))

    return repos


def load_analyzed(results_path):
    """Return set of full_names already present in bandit_results.json."""
    if not os.path.exists(results_path):
        return set()
    with open(results_path, encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return set()
    return {r["repo"] for r in data if "repo" in r}


def count_py_files(repo_dir):
    count = 0
    for _, _, files in os.walk(repo_dir):
        count += sum(1 for f in files if f.endswith(".py"))
    return count


def run_bandit(repo_dir):
    """
    Run bandit -r on repo_dir.
    Returns (parsed_json | None, stderr_text, exit_code).
    """
    cmd = ["bandit", "-r", repo_dir, "-f", "json", "-q"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return None, "bandit not found — is it installed?", -1

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    rc = result.returncode

    # Exit code 0 = no issues, 1 = issues found — both produce valid JSON
    if rc in (0, 1):
        try:
            return json.loads(stdout), stderr, rc
        except json.JSONDecodeError:
            return None, f"JSON parse error. stdout: {stdout[:200]}", 2

    return None, stderr, rc


def parse_bandit_output(bandit_json, full_name, repo_dir):
    """
    Transform raw Bandit JSON into:
      - findings: list of flat finding dicts (for bandit_results.json)
      - summary: single dict (for bandit_summary.csv)
    """
    results = bandit_json.get("results", [])
    errors = bandit_json.get("errors", [])

    findings = []
    for r in results:
        # Strip the local repo_dir prefix from filename for portability
        filename = r.get("filename", "")
        if filename.startswith(repo_dir):
            filename = filename[len(repo_dir):].lstrip(os.sep)

        findings.append({
            "repo": full_name,
            "filename": filename,
            "test_id": r.get("test_id"),
            "test_name": r.get("test_name"),
            "issue_severity": r.get("issue_severity"),
            "issue_confidence": r.get("issue_confidence"),
            "issue_text": r.get("issue_text"),
            "line_number": r.get("line_number"),
            "line_range": r.get("line_range"),
            "code": r.get("code", "").strip(),
        })

    severities = [f["issue_severity"] for f in findings]
    summary = {
        "full_name": full_name,
        "py_files_found": count_py_files(repo_dir),
        "total_issues": len(findings),
        "high": severities.count("HIGH"),
        "medium": severities.count("MEDIUM"),
        "low": severities.count("LOW"),
        "errors": len(errors),
        "bandit_exit_code": 0 if not findings else 1,
        "status": "ok",
    }

    return findings, summary


def append_results(results_path, new_findings):
    """Atomically append findings to bandit_results.json."""
    if os.path.exists(results_path):
        with open(results_path, encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    else:
        existing = []

    existing.extend(new_findings)

    tmp_path = results_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, results_path)


def write_summary_row(summary_path, row):
    """Append one row to bandit_summary.csv; write header if file is new."""
    write_header = not os.path.exists(summary_path)
    with open(summary_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, lineterminator="\n")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def run(args):
    os.makedirs(args.output_dir, exist_ok=True)
    results_path = os.path.join(args.output_dir, "bandit_results.json")
    summary_path = os.path.join(args.output_dir, "bandit_summary.csv")

    repos = find_repo_dirs(args.downloads_dir, args.limit)
    already_done = load_analyzed(results_path)

    total = len(repos)
    analyzed = 0
    skipped = 0
    failed = 0

    if args.verbose:
        print(f"Found {total} repo(s) in '{args.downloads_dir}'.", file=sys.stderr)

    for i, (full_name, repo_dir) in enumerate(repos, 1):
        if full_name in already_done:
            if args.verbose:
                print(f"[{i}/{total}] Skipping {full_name} (already analyzed).", file=sys.stderr)
            skipped += 1
            continue

        py_count = count_py_files(repo_dir)
        if py_count == 0:
            if args.verbose:
                print(f"[{i}/{total}] {full_name} — no .py files, skipping.", file=sys.stderr)
            write_summary_row(summary_path, {
                "full_name": full_name,
                "py_files_found": 0,
                "total_issues": 0,
                "high": 0, "medium": 0, "low": 0,
                "errors": 0,
                "bandit_exit_code": 0,
                "status": "no_py_files",
            })
            skipped += 1
            continue

        if args.verbose:
            print(f"[{i}/{total}] Analyzing {full_name} ({py_count} .py files) ...", file=sys.stderr)

        bandit_json, stderr, rc = run_bandit(repo_dir)

        if bandit_json is None:
            if args.verbose:
                print(f"    Bandit error (exit {rc}): {stderr}", file=sys.stderr)
            write_summary_row(summary_path, {
                "full_name": full_name,
                "py_files_found": py_count,
                "total_issues": 0,
                "high": 0, "medium": 0, "low": 0,
                "errors": 1,
                "bandit_exit_code": rc,
                "status": "bandit_error",
            })
            failed += 1
            continue

        findings, summary = parse_bandit_output(bandit_json, full_name, repo_dir)
        append_results(results_path, findings)
        write_summary_row(summary_path, summary)

        if args.verbose:
            print(
                f"    Found {summary['total_issues']} issue(s) — "
                f"HIGH: {summary['high']}, MEDIUM: {summary['medium']}, LOW: {summary['low']}",
                file=sys.stderr,
            )

        analyzed += 1

    print(f"\nDone. Analyzed: {analyzed} | Skipped: {skipped} | Failed: {failed}")
    print(f"Results: {results_path}")
    print(f"Summary: {summary_path}")


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
