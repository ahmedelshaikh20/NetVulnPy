"""
repo_analyzer.py  —  Phase 2: Security Analysis (Bandit, Semgrep, Skylos, pip-audit, or LLM)

Scans the downloads/ directory produced by repo_downloader.py, runs a
security analyzer on each repo's .py files, and aggregates findings into:
  - scan_results.json  : flat list of all individual findings
  - scan_summary.csv   : one row per repo with severity counts

Supported analyzers:
  - bandit    (default) — Python-focused SAST
  - semgrep             — multi-language SAST with community rules
  - pip-audit           — dependency vulnerability scanner (known CVEs)
  - skylos              — dead code, security, secrets & quality scanner
  - llm                 — LLM-based security analysis via OpenAI-compatible endpoint
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


# ── pip-audit ─────────────────────────────────────────────────────────

def _find_requirements_files(repo_dir):
    """Find all requirements*.txt files in the repo root and common subdirs."""
    candidates = []
    for root, _, files in os.walk(repo_dir):
        for f in files:
            if f.startswith("requirements") and f.endswith(".txt"):
                candidates.append(os.path.join(root, f))
    return candidates


def run_pip_audit(repo_dir):
    """
    Run pip-audit on all requirements*.txt files in repo_dir.
    Returns (parsed_result | None, stderr_text, exit_code).
    """
    req_files = _find_requirements_files(repo_dir)
    if not req_files:
        return None, "no requirements files found", 0

    all_deps = []
    all_stderr = []

    for req_file in req_files:
        cmd = [
            "pip-audit", "-r", req_file,
            "-f", "json",
            "--no-deps",
            "--progress-spinner", "off",
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120,
            )
        except FileNotFoundError:
            return None, "pip-audit not found — pip install pip-audit", -1
        except subprocess.TimeoutExpired:
            all_stderr.append(f"timeout: {req_file}")
            continue

        stderr = result.stderr.strip()
        if stderr:
            all_stderr.append(stderr)

        stdout = result.stdout.strip()
        if stdout:
            try:
                parsed = json.loads(stdout)
                all_deps.extend(parsed.get("dependencies", []))
            except json.JSONDecodeError:
                all_stderr.append(f"JSON parse error for {req_file}")

    if not all_deps:
        return {"dependencies": []}, "\n".join(all_stderr), 0

    return {"dependencies": all_deps}, "\n".join(all_stderr), 0


def parse_pip_audit_output(audit_json, full_name, repo_dir):
    """
    Transform pip-audit JSON into the standard finding/summary schema.
    Each vulnerable dependency × vulnerability becomes one finding.
    All pip-audit findings are mapped to HIGH severity (confirmed CVEs).
    """
    dependencies = audit_json.get("dependencies", [])

    findings = []
    seen = set()  # deduplicate (dep_name, dep_version, vuln_id)
    for dep in dependencies:
        for vuln in dep.get("vulns", []):
            vuln_id = vuln.get("id", "")
            aliases = vuln.get("aliases", [])
            cve = next((a for a in aliases if a.startswith("CVE-")), vuln_id)
            dedup_key = (dep["name"], dep.get("version", "?"), cve)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            fix_versions = vuln.get("fix_versions", [])
            desc = vuln.get("description", "")
            # Truncate long descriptions
            if len(desc) > 300:
                desc = desc[:297] + "..."

            findings.append({
                "repo": full_name,
                "filename": f"requirements (dependency: {dep['name']}=={dep.get('version', '?')})",
                "test_id": cve,
                "test_name": vuln_id,
                "issue_severity": "HIGH",
                "issue_confidence": "HIGH",
                "issue_text": f"{dep['name']}=={dep.get('version', '?')}: {desc}",
                "line_number": 0,
                "line_range": [0, 0],
                "code": f"Fix available: {', '.join(fix_versions)}" if fix_versions else "",
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
        "errors": 0,
        "exit_code": 0 if not findings else 1,
        "analyzer": "pip-audit",
        "status": "ok" if audit_json.get("dependencies") is not None else "no_requirements",
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
            parsed = json.loads(stdout)
            return parsed, stderr, rc
        except json.JSONDecodeError:
            return None, f"JSON parse error. stdout: {stdout[:200]}", 2

    return None, stderr or "skylos produced no output", rc


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

    Skylos stores security findings under the "danger" key. Each entry has:
      rule_id, severity, message, file, line, col, symbol, category
    """
    results = skylos_json.get("danger", [])

    findings = []
    for r in results:
        filename = r.get("file", "")
        abs_repo = os.path.abspath(repo_dir)
        if filename.startswith(abs_repo):
            filename = filename[len(abs_repo):].lstrip(os.sep)
        elif filename.startswith(repo_dir):
            filename = filename[len(repo_dir):].lstrip(os.sep)

        severity_raw = r.get("severity", "LOW").upper()
        severity = _SKYLOS_SEVERITY_MAP.get(severity_raw, "LOW")

        line = r.get("line")

        findings.append({
            "repo": full_name,
            "filename": filename,
            "test_id": r.get("rule_id", ""),
            "test_name": r.get("symbol", r.get("rule_id", "")),
            "issue_severity": severity,
            "issue_confidence": "HIGH",
            "issue_text": r.get("message", ""),
            "line_number": line,
            "line_range": [line, line],
            "code": "",
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
        "errors": 0,
        "exit_code": 0 if not findings else 1,
        "analyzer": "skylos",
        "status": "ok",
    }

    return findings, summary


# ── LLM Analyzer ─────────────────────────────────────────────────────

_LLM_ANALYSIS_PROMPT = """\
You are a security expert analyzing Python source code for vulnerabilities.
Analyze the following Python file and identify any security issues.

**File**: {filename}
**Repository**: {repo}

```python
{code}
```

For each vulnerability found, respond with a JSON array of objects. Each object must have:
- "test_id": a short identifier (e.g., "LLM-SQL-INJECT", "LLM-XSS", "LLM-HARDCODED-SECRET")
- "test_name": human-readable name of the vulnerability type
- "issue_severity": "HIGH", "MEDIUM", or "LOW"
- "issue_confidence": "HIGH", "MEDIUM", or "LOW"
- "issue_text": brief description of the vulnerability
- "line_number": the approximate line number where the issue occurs
- "code": the relevant code snippet (1-3 lines)

If no vulnerabilities are found, respond with an empty array: []

Respond ONLY with the JSON array, no other text.
"""

# Module-level LLM client/config — set by run() before analysis begins
_llm_client = None
_llm_model = None


def _collect_py_files(repo_dir, max_files=50, max_file_size=30_000):
    """Collect Python files from repo_dir, with size/count limits."""
    py_files = []
    for root, _, files in os.walk(repo_dir):
        for f in files:
            if f.endswith(".py"):
                fpath = os.path.join(root, f)
                try:
                    size = os.path.getsize(fpath)
                    if size <= max_file_size:
                        py_files.append(fpath)
                except OSError:
                    pass
                if len(py_files) >= max_files:
                    return py_files
    return py_files


def run_llm(repo_dir):
    """
    Analyze Python files in repo_dir using an LLM via OpenAI-compatible API.
    Returns (list_of_file_results | None, stderr_text, exit_code).
    """
    global _llm_client, _llm_model
    if _llm_client is None:
        return None, "LLM client not initialized", -1

    py_files = _collect_py_files(repo_dir)
    if not py_files:
        return None, "no Python files found", 0

    all_results = []
    errors = []

    for fpath in py_files:
        try:
            with open(fpath, encoding="utf-8", errors="ignore") as fh:
                code = fh.read()
        except OSError as e:
            errors.append(f"read error: {fpath}: {e}")
            continue

        # Skip near-empty files
        if len(code.strip()) < 20:
            continue

        rel_path = os.path.relpath(fpath, repo_dir)

        prompt = _LLM_ANALYSIS_PROMPT.format(
            filename=rel_path,
            repo="",  # filled in by parse_llm_output
            code=code[:15_000],  # truncate very long files
        )

        try:
            response = _llm_client.chat.completions.create(
                model=_llm_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2048,
                temperature=0.1,
            )
            text = response.choices[0].message.content.strip()

            # Extract JSON from response (handle markdown code blocks)
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            findings = json.loads(text)
            if isinstance(findings, list):
                for f in findings:
                    f["_file"] = rel_path
                all_results.extend(findings)

        except json.JSONDecodeError:
            errors.append(f"JSON parse error for {rel_path}")
        except Exception as e:
            errors.append(f"LLM error for {rel_path}: {e}")

    return {"findings": all_results, "errors": errors}, "\n".join(errors), 0


_LLM_SEVERITY_MAP = {
    "HIGH": "HIGH",
    "MEDIUM": "MEDIUM",
    "LOW": "LOW",
    "CRITICAL": "HIGH",
    "INFO": "LOW",
}


def parse_llm_output(llm_json, full_name, repo_dir):
    """
    Transform LLM analysis results into the standard finding/summary schema.
    """
    raw_findings = llm_json.get("findings", [])
    errors = llm_json.get("errors", [])

    findings = []
    for r in raw_findings:
        severity = _LLM_SEVERITY_MAP.get(
            r.get("issue_severity", "LOW").upper(), "LOW"
        )
        line = r.get("line_number", 0)
        if not isinstance(line, int):
            try:
                line = int(line)
            except (ValueError, TypeError):
                line = 0

        findings.append({
            "repo": full_name,
            "filename": r.get("_file", r.get("filename", "")),
            "test_id": r.get("test_id", "LLM-UNKNOWN"),
            "test_name": r.get("test_name", ""),
            "issue_severity": severity,
            "issue_confidence": r.get("issue_confidence", "MEDIUM").upper(),
            "issue_text": r.get("issue_text", ""),
            "line_number": line,
            "line_range": [line, line],
            "code": r.get("code", ""),
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
        "analyzer": "llm",
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
    elif analyzer == "pip-audit":
        scan_fn = run_pip_audit
        parse_fn = parse_pip_audit_output
    elif analyzer == "skylos":
        scan_fn = run_skylos
        parse_fn = parse_skylos_output
    elif analyzer == "llm":
        global _llm_client, _llm_model
        try:
            import openai
        except ImportError:
            print("Error: pip install openai", file=sys.stderr)
            sys.exit(1)

        llm_api_key = getattr(args, "llm_api_key", None) or os.environ.get("LLM_API_KEY")
        llm_endpoint = getattr(args, "llm_endpoint", None) or "https://llms.innkube.fim.uni-passau.de"
        llm_model = getattr(args, "llm_model", None) or "qwen3-next-80b-a3b-instruct"

        if not llm_api_key:
            print("Error: --llm-api-key or LLM_API_KEY env var required for llm analyzer.", file=sys.stderr)
            sys.exit(1)

        _llm_client = openai.OpenAI(api_key=llm_api_key, base_url=llm_endpoint)
        _llm_model = llm_model

        scan_fn = run_llm
        parse_fn = parse_llm_output
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

    # Batch write all results at once
    if all_findings:
        _batch_write_results(results_path, all_findings)
    if all_summaries:
        _batch_write_summary(summary_path, all_summaries)

    print(f"\nDone ({analyzer}). Analyzed: {analyzed} | Skipped: {skipped} | Failed: {failed}")
    print(f"Results: {results_path}")
    print(f"Summary: {summary_path}")


