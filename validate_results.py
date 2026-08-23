"""
validate_results.py — Validate and cross-check results from each analyzer.

Checks performed per analyzer:
  1. Schema validation: required fields, correct types, valid severity values
  2. Consistency: scan_summary totals match scan_results counts
  3. Coverage: repos in summary vs repos in findings
  4. Severity distribution sanity checks
  5. Cross-analyzer comparison on overlapping repos

Usage:
  python validate_results.py [--results-dir DIR] [--verbose]
"""

import argparse
import csv
import json
import os
import sys
from collections import Counter


# ── Schema Definitions ───────────────────────────────────────────────────────

REQUIRED_FINDING_FIELDS = [
    "repo", "filename", "test_id", "test_name",
    "issue_severity", "issue_confidence", "issue_text",
    "line_number", "line_range", "code",
]

VALID_SEVERITIES = {"HIGH", "MEDIUM", "LOW"}
VALID_CONFIDENCES = {"HIGH", "MEDIUM", "LOW"}

SUMMARY_FIELDS = [
    "full_name", "py_files_found", "loc", "total_issues",
    "high", "medium", "low", "errors", "exit_code", "analyzer", "status",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

class ValidationReport:
    """Collects validation results for one analyzer."""

    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.errors = []
        self.warnings = []
        self.stats = {}

    def error(self, msg):
        self.errors.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def stat(self, key, value):
        self.stats[key] = value

    @property
    def passed(self):
        return len(self.errors) == 0

    def print_report(self, verbose=False):
        status = "PASS" if self.passed else "FAIL"
        print(f"\n{'=' * 70}")
        print(f"  [{status}] {self.analyzer.upper()}")
        print(f"{'=' * 70}")

        # Stats
        if self.stats:
            print(f"\n  Statistics:")
            for k, v in self.stats.items():
                print(f"    {k}: {v}")

        # Errors
        if self.errors:
            print(f"\n  Errors ({len(self.errors)}):")
            for e in self.errors:
                print(f"    [ERROR] {e}")

        # Warnings
        if self.warnings:
            print(f"\n  Warnings ({len(self.warnings)}):")
            limit = None if verbose else 10
            for w in self.warnings[:limit]:
                print(f"    [WARN]  {w}")
            if not verbose and len(self.warnings) > 10:
                print(f"    ... and {len(self.warnings) - 10} more (use --verbose)")

        if self.passed and not self.warnings:
            print(f"\n  All checks passed.")


def load_findings(results_dir, analyzer):
    """Load scan_results.json for an analyzer."""
    path = os.path.join(results_dir, analyzer, "scan_results.json")
    if not os.path.exists(path):
        return None, path
    with open(path, encoding="utf-8") as f:
        return json.load(f), path


def load_summary(results_dir, analyzer):
    """Load scan_summary.csv for an analyzer."""
    path = os.path.join(results_dir, analyzer, "scan_summary.csv")
    if not os.path.exists(path):
        return None, path
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows, path


# ── Validation Functions ─────────────────────────────────────────────────────

def validate_schema(findings, report):
    """Check that all findings have required fields and valid values."""
    missing_fields = Counter()
    invalid_severity = Counter()
    invalid_confidence = Counter()
    null_repos = 0
    empty_test_ids = 0
    negative_lines = 0

    for i, f in enumerate(findings):
        for field in REQUIRED_FINDING_FIELDS:
            if field not in f:
                missing_fields[field] += 1

        sev = f.get("issue_severity", "")
        if sev not in VALID_SEVERITIES:
            invalid_severity[sev] += 1

        conf = f.get("issue_confidence", "")
        if conf not in VALID_CONFIDENCES:
            invalid_confidence[conf] += 1

        if not f.get("repo"):
            null_repos += 1

        if not f.get("test_id"):
            empty_test_ids += 1

        ln = f.get("line_number")
        if isinstance(ln, (int, float)) and ln < 0:
            negative_lines += 1

    if missing_fields:
        for field, count in missing_fields.items():
            report.error(f"Missing field '{field}' in {count} finding(s)")

    if invalid_severity:
        for val, count in invalid_severity.items():
            report.error(f"Invalid severity '{val}' in {count} finding(s)")

    if invalid_confidence:
        for val, count in invalid_confidence.items():
            report.warn(f"Invalid confidence '{val}' in {count} finding(s)")

    if null_repos:
        report.error(f"{null_repos} finding(s) have empty/null repo")

    if empty_test_ids:
        report.warn(f"{empty_test_ids} finding(s) have empty test_id")

    if negative_lines:
        report.warn(f"{negative_lines} finding(s) have negative line_number")


def validate_summary(summary_rows, report):
    """Check that summary rows have valid structure."""
    for i, row in enumerate(summary_rows):
        for field in SUMMARY_FIELDS:
            if field not in row:
                report.error(f"Summary row {i}: missing field '{field}'")

        # Check numeric fields are valid
        for num_field in ["py_files_found", "loc", "total_issues", "high", "medium", "low"]:
            val = row.get(num_field, "")
            if val != "":
                try:
                    n = int(val)
                    if n < 0:
                        report.warn(f"Summary row {i} ({row.get('full_name', '?')}): "
                                    f"negative {num_field}={n}")
                except ValueError:
                    report.error(f"Summary row {i} ({row.get('full_name', '?')}): "
                                 f"non-integer {num_field}='{val}'")


def validate_consistency(findings, summary_rows, report):
    """Cross-check findings against summary totals."""
    # Count findings per repo
    findings_per_repo = Counter()
    sev_per_repo = {}
    for f in findings:
        repo = f.get("repo", "")
        findings_per_repo[repo] += 1
        sev_per_repo.setdefault(repo, Counter())
        sev_per_repo[repo][f.get("issue_severity", "UNKNOWN")] += 1

    # Compare with summary
    mismatches = 0
    for row in summary_rows:
        repo = row.get("full_name", "")
        status = row.get("status", "")

        if status != "ok":
            continue

        summary_total = int(row.get("total_issues", 0))
        actual_total = findings_per_repo.get(repo, 0)

        if summary_total != actual_total:
            mismatches += 1
            if mismatches <= 5:
                report.warn(
                    f"Repo '{repo}': summary says {summary_total} issues, "
                    f"but findings has {actual_total}"
                )

        # Check severity breakdown
        summary_high = int(row.get("high", 0))
        summary_med = int(row.get("medium", 0))
        summary_low = int(row.get("low", 0))
        actual_sev = sev_per_repo.get(repo, Counter())

        if summary_high != actual_sev.get("HIGH", 0):
            mismatches += 1
        if summary_med != actual_sev.get("MEDIUM", 0):
            mismatches += 1
        if summary_low != actual_sev.get("LOW", 0):
            mismatches += 1

    if mismatches > 5:
        report.warn(f"... {mismatches - 5} more consistency mismatches")

    if mismatches > 0:
        report.stat("Consistency mismatches", mismatches)
    else:
        report.stat("Consistency mismatches", "0 (all match)")

    # Check coverage: repos in summary but not in findings
    summary_repos = {r["full_name"] for r in summary_rows if r.get("status") == "ok"}
    finding_repos = set(findings_per_repo.keys())

    summary_only = summary_repos - finding_repos
    # Repos with 0 issues in summary are expected to have no findings
    summary_with_issues = {
        r["full_name"] for r in summary_rows
        if r.get("status") == "ok" and int(r.get("total_issues", 0)) > 0
    }
    missing_findings = summary_with_issues - finding_repos
    if missing_findings:
        report.warn(f"{len(missing_findings)} repo(s) have issues in summary but no findings "
                    f"(e.g., {list(missing_findings)[:3]})")

    findings_only = finding_repos - summary_repos
    if findings_only:
        report.warn(f"{len(findings_only)} repo(s) have findings but no 'ok' summary row "
                    f"(e.g., {list(findings_only)[:3]})")


def validate_severity_distribution(findings, report):
    """Check for suspicious severity distributions."""
    sev_counts = Counter(f.get("issue_severity", "UNKNOWN") for f in findings)
    total = len(findings)

    report.stat("Severity distribution",
                {s: f"{c} ({c/total*100:.1f}%)" for s, c in sorted(sev_counts.items())})

    # Flag if a single severity is >98% (likely noise issue)
    for sev, count in sev_counts.items():
        if count / total > 0.98:
            report.warn(f"Severity '{sev}' accounts for {count/total*100:.1f}% of findings — "
                        f"possible noise domination")


def validate_duplicates(findings, report):
    """Check for exact duplicate findings."""
    seen = set()
    duplicates = 0
    for f in findings:
        key = (f.get("repo"), f.get("filename"), f.get("test_id"),
               f.get("line_number"), f.get("issue_text", "")[:100])
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    report.stat("Duplicate findings", duplicates)
    if duplicates > 0:
        pct = duplicates / len(findings) * 100
        if pct > 5:
            report.warn(f"{duplicates} duplicate findings ({pct:.1f}%)")


def validate_analyzer_specific(findings, analyzer, report):
    """Analyzer-specific validation rules."""
    if analyzer == "bandit":
        # All test_ids should start with B
        non_bandit = sum(1 for f in findings if not f.get("test_id", "").startswith("B"))
        if non_bandit:
            report.error(f"{non_bandit} findings have test_id not starting with 'B'")

        # Check for B101 dominance
        b101 = sum(1 for f in findings if f.get("test_id") == "B101")
        report.stat("B101 (assert_used) count", f"{b101} ({b101/len(findings)*100:.1f}%)")

    elif analyzer == "semgrep":
        # test_ids should be dotted rule paths
        short_ids = sum(1 for f in findings if "." not in f.get("test_id", "x.y"))
        if short_ids:
            report.warn(f"{short_ids} findings have non-dotted test_id (unusual for Semgrep)")

    elif analyzer == "llm":
        # All test_ids should start with LLM-
        non_llm = sum(1 for f in findings if not f.get("test_id", "").startswith("LLM-"))
        if non_llm:
            report.warn(f"{non_llm} findings have test_id not starting with 'LLM-'")

        # Check for very high rule diversity (expected for LLM)
        distinct_rules = len(set(f.get("test_id") for f in findings))
        report.stat("Distinct rule IDs", distinct_rules)
        if distinct_rules > 10000:
            report.warn(f"Very high rule diversity ({distinct_rules}) — "
                        f"LLM is generating inconsistent rule taxonomy")

        # Check for empty code snippets
        empty_code = sum(1 for f in findings if not f.get("code", "").strip())
        if empty_code:
            report.stat("Findings with empty code snippet",
                        f"{empty_code} ({empty_code/len(findings)*100:.1f}%)")

    elif analyzer == "pip-audit":
        # test_ids should be CVE or PYSEC identifiers
        non_cve = sum(1 for f in findings
                      if not f.get("test_id", "").startswith(("CVE-", "PYSEC-", "GHSA-")))
        if non_cve:
            report.warn(f"{non_cve} findings have non-CVE/PYSEC test_id")

        # All should be HIGH severity
        non_high = sum(1 for f in findings if f.get("issue_severity") != "HIGH")
        if non_high:
            report.warn(f"{non_high} pip-audit findings are not HIGH severity")


# ── Cross-Analyzer Comparison ────────────────────────────────────────────────

def cross_analyzer_comparison(all_results, results_dir):
    """Compare findings across analyzers on overlapping repos."""
    print(f"\n{'=' * 70}")
    print(f"  CROSS-ANALYZER COMPARISON")
    print(f"{'=' * 70}")

    # Collect repos per analyzer
    analyzer_repos = {}
    analyzer_findings = {}
    for analyzer, findings in all_results.items():
        repos = set(f["repo"] for f in findings)
        analyzer_repos[analyzer] = repos
        analyzer_findings[analyzer] = findings

    # Find common repos
    all_analyzers = list(analyzer_repos.keys())
    if len(all_analyzers) < 2:
        print("\n  Only one analyzer has results — skipping cross-comparison.")
        return

    common = set.intersection(*analyzer_repos.values())
    print(f"\n  Repos per analyzer:")
    for a, repos in sorted(analyzer_repos.items()):
        print(f"    {a:12s}: {len(repos)} repos")
    print(f"    {'common':12s}: {len(common)} repos")

    # Compare finding counts on common repos
    print(f"\n  Findings on {len(common)} common repos:")
    for analyzer in sorted(all_analyzers):
        common_findings = [f for f in analyzer_findings[analyzer] if f["repo"] in common]
        sev = Counter(f["issue_severity"] for f in common_findings)
        print(f"    {analyzer:12s}: {len(common_findings):>8} total | "
              f"HIGH: {sev.get('HIGH',0):>6} | MED: {sev.get('MEDIUM',0):>6} | "
              f"LOW: {sev.get('LOW',0):>6}")

    # Per-repo agreement: do analyzers agree on which repos are "clean"?
    print(f"\n  Clean repos (0 findings) per analyzer:")
    for analyzer in sorted(all_analyzers):
        clean = sum(1 for repo in common
                    if not any(f["repo"] == repo for f in analyzer_findings[analyzer]))
        print(f"    {analyzer:12s}: {clean} / {len(common)} repos clean")

    # Severity agreement on common repos
    print(f"\n  Repos with HIGH findings per analyzer:")
    for analyzer in sorted(all_analyzers):
        high_repos = set(f["repo"] for f in analyzer_findings[analyzer]
                         if f["repo"] in common and f["issue_severity"] == "HIGH")
        print(f"    {analyzer:12s}: {len(high_repos)} / {len(common)} repos")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate analyzer results.")
    parser.add_argument("--results-dir", default="results",
                        help="Root results directory (default: results)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show all warnings (default: first 10 per check)")
    args = parser.parse_args()

    results_dir = args.results_dir
    if not os.path.isdir(results_dir):
        print(f"Error: results directory '{results_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    # Discover available analyzers
    analyzers = sorted(
        d for d in os.listdir(results_dir)
        if os.path.isdir(os.path.join(results_dir, d))
        and os.path.exists(os.path.join(results_dir, d, "scan_results.json"))
    )

    if not analyzers:
        print("No analyzer results found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(analyzers)} analyzer(s): {', '.join(analyzers)}")

    all_results = {}
    reports = []

    for analyzer in analyzers:
        report = ValidationReport(analyzer)

        # Load data
        findings, findings_path = load_findings(results_dir, analyzer)
        summary, summary_path = load_summary(results_dir, analyzer)

        if findings is None:
            report.error(f"scan_results.json not found at {findings_path}")
            reports.append(report)
            continue

        report.stat("Total findings", len(findings))
        report.stat("Distinct repos", len(set(f.get("repo", "") for f in findings)))
        report.stat("Distinct rules", len(set(f.get("test_id", "") for f in findings)))

        # 1. Schema validation
        validate_schema(findings, report)

        # 2. Severity distribution
        validate_severity_distribution(findings, report)

        # 3. Duplicate check
        validate_duplicates(findings, report)

        # 4. Analyzer-specific checks
        validate_analyzer_specific(findings, analyzer, report)

        # 5. Summary validation & consistency
        if summary is not None:
            report.stat("Summary rows", len(summary))
            validate_summary(summary, report)
            validate_consistency(findings, summary, report)
        else:
            report.warn(f"scan_summary.csv not found at {summary_path}")

        reports.append(report)
        all_results[analyzer] = findings

    # Print all reports
    for report in reports:
        report.print_report(verbose=args.verbose)

    # Cross-analyzer comparison
    if len(all_results) >= 2:
        cross_analyzer_comparison(all_results, results_dir)

    # Final summary
    print(f"\n{'=' * 70}")
    print(f"  VALIDATION SUMMARY")
    print(f"{'=' * 70}")
    total_pass = sum(1 for r in reports if r.passed)
    total_fail = len(reports) - total_pass
    for r in reports:
        status = "PASS" if r.passed else "FAIL"
        print(f"    [{status}] {r.analyzer:12s} — "
              f"{len(r.errors)} error(s), {len(r.warnings)} warning(s)")
    print(f"\n  {total_pass} passed, {total_fail} failed out of {len(reports)} analyzer(s)")

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
