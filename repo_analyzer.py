"""
repo_analyzer.py  —  Phase 2: Security Analysis (Bandit, Semgrep, or Skylos)

Scans the downloads/ directory produced by repo_downloader.py, runs a
security analyzer on each repo's .py files, and aggregates findings into:
  - scan_results.json  : flat list of all individual findings
  - scan_summary.csv   : one row per repo with severity counts

Supported analyzers:
  - bandit  (default) — Python-focused SAST
  - semgrep           — multi-language SAST with community rules
  - skylos            — dead code, security, secrets & quality scanner
"""

import csv
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

SUMMARY_FIELDS = [
    "full_name",
    "py_files_found",
    "loc",
    "total_issues",
    "high",
    "medium",
    "low",
    "errors",
    "exit_code",
    "analyzer",
    "status",
]


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
    """Return set of full_names already present in scan_results.json."""
    if not os.path.exists(results_path):
        return set()
    with open(results_path, encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return set()
    return {r["repo"] for r in data if "repo" in r}


def count_loc(repo_dir):
    """Count non-blank, non-comment Python lines across all .py files."""
    loc = 0
    for root, _, files in os.walk(repo_dir):
        for f in files:
            if f.endswith(".py"):
                try:
                    with open(os.path.join(root, f), encoding="utf-8", errors="ignore") as fh:
                        loc += sum(
                            1 for line in fh
                            if line.strip() and not line.strip().startswith("#")
                        )
                except OSError:
                    pass
    return loc


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
      - findings: list of flat finding dicts (for scan_results.json)
      - summary: single dict (for scan_summary.csv)
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
        "loc": count_loc(repo_dir),
        "total_issues": len(findings),
        "high": severities.count("HIGH"),
        "medium": severities.count("MEDIUM"),
        "low": severities.count("LOW"),
        "errors": len(errors),
        "exit_code": 0 if not findings else 1,
        "analyzer": "bandit",
        "status": "ok",
    }

    return findings, summary


# ── Semgrep ──────────────────────────────────────────────────────────────

def run_semgrep(repo_dir):
    """
    Run semgrep on repo_dir with the default Python security rules.
    Returns (parsed_json | None, stderr_text, exit_code).
    """
    cmd = [
        "semgrep", "scan",
        "--config", "p/python",
        "--no-git-ignore",
        "--json",
        "--quiet",
        "--jobs", str(os.cpu_count() or 1),
        repo_dir,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return None, "semgrep not found — is it installed?", -1

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    rc = result.returncode

    if rc in (0, 1):
        try:
            return json.loads(stdout), stderr, rc
        except json.JSONDecodeError:
            return None, f"JSON parse error. stdout: {stdout[:200]}", 2

    return None, stderr, rc


_SEMGREP_SEVERITY_MAP = {
    "ERROR": "HIGH",
    "WARNING": "MEDIUM",
    "INFO": "LOW",
}


def parse_semgrep_output(semgrep_json, full_name, repo_dir):
    """
    Transform raw Semgrep JSON into the same schema used for Bandit:
      - findings: list of flat finding dicts
      - summary: single dict
    """
    results = semgrep_json.get("results", [])
    errors = semgrep_json.get("errors", [])

    findings = []
    for r in results:
        filename = r.get("path", "")
        if filename.startswith(repo_dir):
            filename = filename[len(repo_dir):].lstrip(os.sep)

        severity_raw = r.get("extra", {}).get("severity", "INFO").upper()
        severity = _SEMGREP_SEVERITY_MAP.get(severity_raw, "LOW")

        findings.append({
            "repo": full_name,
            "filename": filename,
            "test_id": r.get("check_id", ""),
            "test_name": r.get("check_id", "").rsplit(".", 1)[-1],
            "issue_severity": severity,
            "issue_confidence": r.get("extra", {}).get("metadata", {}).get("confidence", "MEDIUM").upper(),
            "issue_text": r.get("extra", {}).get("message", ""),
            "line_number": r.get("start", {}).get("line"),
            "line_range": [r.get("start", {}).get("line"), r.get("end", {}).get("line")],
            "code": r.get("extra", {}).get("lines", "").strip(),
        })

    severities = [f["issue_severity"] for f in findings]
    summary = {
        "full_name": full_name,
        "py_files_found": count_py_files(repo_dir),
        "loc": count_loc(repo_dir),
        "total_issues": len(findings),
        "high": severities.count("HIGH"),
        "medium": severities.count("MEDIUM"),
        "low": severities.count("LOW"),
        "errors": len(errors),
        "exit_code": 0 if not findings else 1,
        "analyzer": "semgrep",
        "status": "ok",
    }

    return findings, summary


# ── Skylos ──────────────────────────────────────────────────────────────

def run_skylos(repo_dir):
    """
    Run skylos with --danger on repo_dir.
    Returns (parsed_json | None, stderr_text, exit_code).
    """
    cmd = ["skylos", repo_dir, "--danger", "--json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        return None, "skylos not found — is it installed? (pip install skylos)", -1

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    rc = result.returncode

    if stdout:
        try:
            return json.loads(stdout), stderr, rc
        except json.JSONDecodeError:
            return None, f"JSON parse error. stdout: {stdout[:200]}", 2

    return None, stderr, rc


_SKYLOS_SEVERITY_MAP = {
    "HIGH": "HIGH",
    "MEDIUM": "MEDIUM",
    "LOW": "LOW",
    "INFO": "LOW",
    "CRITICAL": "HIGH",
}


def parse_skylos_output(skylos_json, full_name, repo_dir):
    """
    Transform raw Skylos JSON into the same schema used for Bandit:
      - findings: list of flat finding dicts
      - summary: single dict
    """
    results = skylos_json.get("findings", [])
    errors = skylos_json.get("errors", []) if isinstance(skylos_json.get("errors"), list) else []

    findings = []
    for r in results:
        file_range = r.get("range", {})
        filename = file_range.get("file", "")
        if filename.startswith(repo_dir):
            filename = filename[len(repo_dir):].lstrip(os.sep)

        severity_raw = r.get("severity", "LOW").upper()
        severity = _SKYLOS_SEVERITY_MAP.get(severity_raw, "LOW")

        start_line = file_range.get("start_line")
        end_line = file_range.get("end_line")

        findings.append({
            "repo": full_name,
            "filename": filename,
            "test_id": r.get("rule_id", ""),
            "test_name": r.get("category", r.get("rule_id", "")),
            "issue_severity": severity,
            "issue_confidence": str(r.get("confidence", "MEDIUM")).upper()
                if isinstance(r.get("confidence"), str)
                else f"{r.get('confidence', 'MEDIUM')}",
            "issue_text": r.get("message", ""),
            "line_number": start_line,
            "line_range": [start_line, end_line],
            "code": r.get("suggested_fix", "").strip(),
        })

    severities = [f["issue_severity"] for f in findings]
    summary = {
        "full_name": full_name,
        "py_files_found": count_py_files(repo_dir),
        "loc": count_loc(repo_dir),
        "total_issues": len(findings),
        "high": severities.count("HIGH"),
        "medium": severities.count("MEDIUM"),
        "low": severities.count("LOW"),
        "errors": len(errors),
        "exit_code": 0 if not findings else 1,
        "analyzer": "skylos",
        "status": "ok",
    }

    return findings, summary


def append_results(results_path, new_findings):
    """Atomically append findings to scan_results.json."""
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
    """Append one row to scan_summary.csv; write header if file is new."""
    write_header = not os.path.exists(summary_path)
    with open(summary_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, lineterminator="\n")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _batch_write_results(results_path, all_findings):
    """Write all findings at once to scan_results.json."""
    tmp_path = results_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(all_findings, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, results_path)


def _batch_write_summary(summary_path, all_summaries):
    """Write all summary rows at once to scan_summary.csv."""
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(all_summaries)


def _analyze_one(scan_fn, parse_fn, analyzer, full_name, repo_dir):
    """Analyze a single repo. Returns (full_name, findings|None, summary, status)."""
    py_count = count_py_files(repo_dir)
    if py_count == 0:
        summary = {
            "full_name": full_name,
            "py_files_found": 0, "loc": 0,
            "total_issues": 0, "high": 0, "medium": 0, "low": 0,
            "errors": 0, "exit_code": 0,
            "analyzer": analyzer, "status": "no_py_files",
        }
        return (full_name, None, summary, "no_py")

    scan_json, stderr, rc = scan_fn(repo_dir)

    if scan_json is None:
        summary = {
            "full_name": full_name,
            "py_files_found": py_count, "loc": count_loc(repo_dir),
            "total_issues": 0, "high": 0, "medium": 0, "low": 0,
            "errors": 1, "exit_code": rc,
            "analyzer": analyzer, "status": f"{analyzer}_error",
        }
        return (full_name, None, summary, "failed")

    findings, summary = parse_fn(scan_json, full_name, repo_dir)
    return (full_name, findings, summary, "analyzed")


def run(args):
    analyzer = getattr(args, "analyzer", "bandit")

    if analyzer == "bandit":
        scan_fn = run_bandit
        parse_fn = parse_bandit_output
    elif analyzer == "semgrep":
        scan_fn = run_semgrep
        parse_fn = parse_semgrep_output
    elif analyzer == "skylos":
        scan_fn = run_skylos
        parse_fn = parse_skylos_output
    else:
        print(f"Error: unknown analyzer '{analyzer}'.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    results_path = os.path.join(args.output_dir, "scan_results.json")
    summary_path = os.path.join(args.output_dir, "scan_summary.csv")

    repos = find_repo_dirs(args.downloads_dir, args.limit)
    already_done = load_analyzed(results_path)
    workers = getattr(args, "workers", 4)

    total = len(repos)
    analyzed = 0
    skipped = 0
    failed = 0

    if args.verbose:
        print(f"Using analyzer: {analyzer} with {workers} worker(s)", file=sys.stderr)
        print(f"Found {total} repo(s) in '{args.downloads_dir}'.", file=sys.stderr)

    # Filter out already-done repos
    work_items = []
    for full_name, repo_dir in repos:
        if full_name in already_done:
            skipped += 1
            if not args.keep_files:
                shutil.rmtree(repo_dir, ignore_errors=True)
        else:
            work_items.append((full_name, repo_dir))

    if args.verbose and skipped:
        print(f"Skipped {skipped} already-analyzed repo(s).", file=sys.stderr)

    # Analyze in parallel, collect results in memory
    all_findings = []
    all_summaries = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_analyze_one, scan_fn, parse_fn, analyzer, fn, rd): (fn, rd)
            for fn, rd in work_items
        }

        for fut in as_completed(futures):
            full_name, repo_dir = futures[fut]
            try:
                _, findings, summary, status = fut.result()
                all_summaries.append(summary)

                if status == "analyzed":
                    all_findings.extend(findings)
                    analyzed += 1
                    if args.verbose:
                        print(
                            f"  [{analyzed + skipped + failed}/{total}] {full_name} — "
                            f"{summary['total_issues']} issue(s)",
                            file=sys.stderr,
                        )
                elif status == "no_py":
                    skipped += 1
                else:
                    failed += 1

            except Exception as exc:
                if args.verbose:
                    print(f"  {full_name} — exception: {exc}", file=sys.stderr)
                failed += 1
            finally:
                if not args.keep_files:
                    shutil.rmtree(repo_dir, ignore_errors=True)

    # Batch write all results at once
    if all_findings:
        _batch_write_results(results_path, all_findings)
    if all_summaries:
        _batch_write_summary(summary_path, all_summaries)

    print(f"\nDone ({analyzer}). Analyzed: {analyzed} | Skipped: {skipped} | Failed: {failed}")
    print(f"Results: {results_path}")
    print(f"Summary: {summary_path}")


