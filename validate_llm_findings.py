"""
validate_llm_findings.py — Ground-truth validation of LLM analyzer findings.

Samples findings from EACH analyzer (bandit, semgrep, llm, pip-audit),
reads the actual source code from downloads/, sends the code + finding
to an LLM for independent verification, and reports true-positive /
false-positive rates per analyzer.

This answers the question: "Are the vulnerabilities each tool reported
actually real issues in the code?"

Usage:
  python validate_llm_findings.py --sample 50 --verbose
  python validate_llm_findings.py --analyzer llm --sample 100
  python validate_llm_findings.py --all --sample 30

Requires:
  pip install openai
  export LLM_API_KEY=<your key>
"""

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import openai
except ImportError:
    print("Error: pip install openai", file=sys.stderr)
    sys.exit(1)


# ── Verification Prompt ──────────────────────────────────────────────────────

VERIFY_PROMPT = """\
You are a senior security auditor performing a code review. A static \
analysis tool flagged the following code as a potential vulnerability. \
Your task is to read the ACTUAL source code provided and determine \
whether this finding is a REAL vulnerability or a FALSE ALARM.

## Tool Finding
- **Tool**: {analyzer}
- **Rule**: {test_id} ({test_name})
- **Reported Severity**: {issue_severity}
- **Description**: {issue_text}
- **File**: {filename}
- **Reported Line**: {line_number}

## Reported Code Snippet
```python
{reported_code}
```

## Actual Source Code (full file)
```python
{actual_code}
```

## Instructions
1. Read the ACTUAL source code carefully — not just the snippet.
2. Determine if the flagged issue is a REAL security vulnerability \
   that could be exploited or cause harm in a production context.
3. Consider whether:
   - The code is test/example/demo code (lower risk)
   - The flagged pattern is used safely in context
   - There are mitigations elsewhere in the code
   - The vulnerability requires specific conditions to exploit

Respond in EXACTLY this JSON format:
{{
  "verdict": "TRUE_POSITIVE" or "FALSE_POSITIVE",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "actual_severity": "CRITICAL" or "HIGH" or "MEDIUM" or "LOW" or "NONE",
  "reasoning": "2-4 sentence explanation of why this is or is not a real issue"
}}
"""


# ── Data Loading ─────────────────────────────────────────────────────────────

def load_findings(results_dir, analyzer):
    """Load scan_results.json for an analyzer."""
    path = os.path.join(results_dir, analyzer, "scan_results.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_source_file(downloads_dir, repo, filename):
    """Read the actual source file from downloads/."""
    # repo format: owner/repo → directory: owner_repo
    repo_dir_name = repo.replace("/", "_", 1)
    filepath = os.path.join(downloads_dir, repo_dir_name, filename)

    if not os.path.exists(filepath):
        # Try without leading path components (semgrep sometimes includes downloads/ prefix)
        # Strip "downloads/owner_repo/" prefix if present
        clean = filename
        for prefix in [f"downloads/{repo_dir_name}/", f"downloads\\{repo_dir_name}\\"]:
            if clean.startswith(prefix):
                clean = clean[len(prefix):]
        filepath = os.path.join(downloads_dir, repo_dir_name, clean)

    if not os.path.exists(filepath):
        return None

    try:
        with open(filepath, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return None


def stratified_sample(findings, n_per_severity, seed=42):
    """Sample n findings per severity level."""
    random.seed(seed)
    by_severity = {"HIGH": [], "MEDIUM": [], "LOW": []}

    for f in findings:
        sev = f.get("issue_severity", "LOW").upper()
        if sev in by_severity:
            by_severity[sev].append(f)

    sample = []
    for sev, pool in by_severity.items():
        k = min(n_per_severity, len(pool))
        if k > 0:
            sample.extend(random.sample(pool, k))

    random.shuffle(sample)
    return sample


# ── LLM Verification ────────────────────────────────────────────────────────

def verify_finding(client, model, finding, actual_code, analyzer, max_retries=2):
    """Send one finding + actual code to LLM for verification."""
    # Truncate very long files but keep enough context
    if len(actual_code) > 20_000:
        # Try to keep the relevant lines in view
        lines = actual_code.split("\n")
        target_line = finding.get("line_number", 0)
        if isinstance(target_line, int) and target_line > 0:
            start = max(0, target_line - 50)
            end = min(len(lines), target_line + 150)
            actual_code = "\n".join(lines[start:end])
            actual_code = f"[... truncated, showing lines {start+1}-{end} ...]\n" + actual_code
        else:
            actual_code = actual_code[:20_000] + "\n[... truncated ...]"

    prompt = VERIFY_PROMPT.format(
        analyzer=analyzer,
        test_id=finding.get("test_id", ""),
        test_name=finding.get("test_name", ""),
        issue_severity=finding.get("issue_severity", ""),
        issue_text=finding.get("issue_text", ""),
        filename=finding.get("filename", ""),
        line_number=finding.get("line_number", ""),
        reported_code=finding.get("code", ""),
        actual_code=actual_code,
    )

    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=512,
                temperature=0.1,
            )
            text = response.choices[0].message.content.strip()

            # Extract JSON
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()

            result = json.loads(text)
            return {
                "verdict": result.get("verdict", "UNKNOWN"),
                "confidence": result.get("confidence", "UNKNOWN"),
                "actual_severity": result.get("actual_severity", "UNKNOWN"),
                "reasoning": result.get("reasoning", ""),
            }

        except json.JSONDecodeError:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return {
                "verdict": "ERROR",
                "confidence": "NONE",
                "actual_severity": "UNKNOWN",
                "reasoning": f"Failed to parse LLM response: {text[:200] if 'text' in dir() else 'no response'}",
            }
        except Exception as e:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return {
                "verdict": "ERROR",
                "confidence": "NONE",
                "actual_severity": "UNKNOWN",
                "reasoning": f"LLM API error: {e}",
            }


# ── Report Generation ────────────────────────────────────────────────────────

def print_analyzer_report(analyzer, results):
    """Print validation results for one analyzer."""
    total = len(results)
    if total == 0:
        print(f"\n  No results for {analyzer}.")
        return

    tp = sum(1 for r in results if r["verdict"] == "TRUE_POSITIVE")
    fp = sum(1 for r in results if r["verdict"] == "FALSE_POSITIVE")
    err = sum(1 for r in results if r["verdict"] == "ERROR")
    skipped = sum(1 for r in results if r["verdict"] == "SKIPPED")
    evaluated = total - err - skipped

    print(f"\n{'─' * 60}")
    print(f"  {analyzer.upper()} — Validation Results")
    print(f"{'─' * 60}")
    print(f"  Sampled:          {total}")
    print(f"  Evaluated:        {evaluated} (skipped: {skipped}, errors: {err})")

    if evaluated > 0:
        precision = tp / evaluated * 100
        fp_rate = fp / evaluated * 100
        print(f"  True Positives:   {tp} ({precision:.1f}%)")
        print(f"  False Positives:  {fp} ({fp_rate:.1f}%)")
        print(f"  Precision:        {precision:.1f}%")
        print(f"  False Positive Rate: {fp_rate:.1f}%")

    # Breakdown by reported severity
    print(f"\n  By reported severity:")
    for sev in ["HIGH", "MEDIUM", "LOW"]:
        sev_results = [r for r in results
                       if r["reported_severity"] == sev and r["verdict"] in ("TRUE_POSITIVE", "FALSE_POSITIVE")]
        if not sev_results:
            continue
        sev_tp = sum(1 for r in sev_results if r["verdict"] == "TRUE_POSITIVE")
        print(f"    {sev:6s}: {sev_tp}/{len(sev_results)} true positives "
              f"({sev_tp/len(sev_results)*100:.1f}%)")

    # Severity accuracy: how often does the tool's severity match the LLM's assessment?
    print(f"\n  Severity accuracy (tool vs verified):")
    sev_match = 0
    sev_over = 0
    sev_under = 0
    sev_order = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    for r in results:
        if r["verdict"] not in ("TRUE_POSITIVE", "FALSE_POSITIVE"):
            continue
        reported = sev_order.get(r["reported_severity"], -1)
        actual = sev_order.get(r.get("actual_severity", "UNKNOWN"), -1)
        if actual < 0:
            continue
        if r["verdict"] == "FALSE_POSITIVE":
            sev_over += 1
        elif reported == actual:
            sev_match += 1
        elif reported > actual:
            sev_over += 1
        else:
            sev_under += 1

    rated = sev_match + sev_over + sev_under
    if rated > 0:
        print(f"    Correct:        {sev_match}/{rated} ({sev_match/rated*100:.1f}%)")
        print(f"    Over-reported:  {sev_over}/{rated} ({sev_over/rated*100:.1f}%)")
        print(f"    Under-reported: {sev_under}/{rated} ({sev_under/rated*100:.1f}%)")

    # Show some example false positives
    fps = [r for r in results if r["verdict"] == "FALSE_POSITIVE"]
    if fps:
        print(f"\n  Example false positives:")
        for r in fps[:3]:
            print(f"    - [{r['reported_severity']}] {r['test_id']} in {r['repo']}")
            print(f"      {r['reasoning'][:120]}")

    # Show some example true positives
    tps = [r for r in results if r["verdict"] == "TRUE_POSITIVE"]
    if tps:
        print(f"\n  Example true positives:")
        for r in tps[:3]:
            print(f"    - [{r['reported_severity']}] {r['test_id']} in {r['repo']}")
            print(f"      {r['reasoning'][:120]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate analyzer findings against actual source code using LLM verification.",
    )
    parser.add_argument("--results-dir", default="results",
                        help="Root results directory (default: results)")
    parser.add_argument("--downloads-dir", default="downloads",
                        help="Downloads directory with source code (default: downloads)")
    parser.add_argument("--analyzer", default=None,
                        help="Validate only this analyzer (default: all)")
    parser.add_argument("--all", action="store_true",
                        help="Validate all analyzers")
    parser.add_argument("--sample", type=int, default=50,
                        help="Findings to sample per severity per analyzer (default: 50)")
    parser.add_argument("--api-key", default=None,
                        help="LLM API key (or set LLM_API_KEY env var)")
    parser.add_argument("--endpoint", default="https://llms.innkube.fim.uni-passau.de",
                        help="LLM endpoint URL")
    parser.add_argument("--model", default="qwen3-next-80b-a3b-instruct",
                        help="LLM model for verification")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel verification workers (default: 4)")
    parser.add_argument("--output", default=None,
                        help="Output JSON path (default: results/validation_<analyzer>.json)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for sampling (default: 42)")
    args = parser.parse_args()

    # Resolve API key
    api_key = args.api_key or os.environ.get("LLM_API_KEY")
    if not api_key:
        print("Error: --api-key or LLM_API_KEY env var required.", file=sys.stderr)
        sys.exit(1)

    client = openai.OpenAI(api_key=api_key, base_url=args.endpoint)

    # Determine which analyzers to validate
    if args.analyzer:
        analyzers = [args.analyzer]
    elif args.all:
        analyzers = sorted(
            d for d in os.listdir(args.results_dir)
            if os.path.isdir(os.path.join(args.results_dir, d))
            and os.path.exists(os.path.join(args.results_dir, d, "scan_results.json"))
        )
    else:
        # Default: just LLM
        analyzers = ["llm"]

    if not os.path.isdir(args.downloads_dir):
        print(f"Error: downloads directory '{args.downloads_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Validating {len(analyzers)} analyzer(s): {', '.join(analyzers)}")
    print(f"Sample: {args.sample} per severity | Model: {args.model}")
    print(f"Workers: {args.workers}")

    all_analyzer_results = {}

    for analyzer in analyzers:
        print(f"\n{'=' * 60}")
        print(f"  Validating: {analyzer.upper()}")
        print(f"{'=' * 60}")

        findings = load_findings(args.results_dir, analyzer)
        if findings is None:
            print(f"  No results found for {analyzer}.")
            continue

        # Filter to findings that have code snippets (meaningful to verify)
        verifiable = [f for f in findings if f.get("code", "").strip()]
        print(f"  Total findings: {len(findings)} | Verifiable (with code): {len(verifiable)}")

        if not verifiable:
            # For pip-audit, code field contains fix info, still verifiable
            verifiable = findings

        sample = stratified_sample(verifiable, args.sample, seed=args.seed)
        print(f"  Sampled: {len(sample)}")

        sev_counts = Counter(f["issue_severity"] for f in sample)
        print(f"  Sample breakdown: {dict(sev_counts)}")

        results = []

        def _verify_one(idx_finding):
            idx, finding = idx_finding
            repo = finding.get("repo", "")
            filename = finding.get("filename", "")

            # Read actual source code
            actual_code = read_source_file(args.downloads_dir, repo, filename)

            if actual_code is None:
                return {
                    "verdict": "SKIPPED",
                    "confidence": "NONE",
                    "actual_severity": "UNKNOWN",
                    "reasoning": f"Source file not found: {filename}",
                    "repo": repo,
                    "filename": filename,
                    "test_id": finding.get("test_id", ""),
                    "test_name": finding.get("test_name", ""),
                    "reported_severity": finding.get("issue_severity", ""),
                    "issue_text": finding.get("issue_text", "")[:200],
                    "line_number": finding.get("line_number"),
                }

            verification = verify_finding(
                client, args.model, finding, actual_code, analyzer
            )

            return {
                **verification,
                "repo": repo,
                "filename": filename,
                "test_id": finding.get("test_id", ""),
                "test_name": finding.get("test_name", ""),
                "reported_severity": finding.get("issue_severity", ""),
                "issue_text": finding.get("issue_text", "")[:200],
                "line_number": finding.get("line_number"),
            }

        # Run verification in parallel
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(_verify_one, (i, f)): i
                for i, f in enumerate(sample)
            }

            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    result = fut.result()
                    results.append(result)

                    if args.verbose:
                        v = result["verdict"]
                        sym = {"TRUE_POSITIVE": "TP", "FALSE_POSITIVE": "FP",
                               "ERROR": "ER", "SKIPPED": "SK"}.get(v, "??")
                        print(
                            f"  [{len(results)}/{len(sample)}] [{sym}] "
                            f"{result['test_id']} — {result['repo']}",
                            file=sys.stderr,
                        )
                except Exception as e:
                    print(f"  Exception on sample {idx}: {e}", file=sys.stderr)

        # Sort results for consistent output
        results.sort(key=lambda r: (r["reported_severity"], r["repo"]))

        all_analyzer_results[analyzer] = results

        # Save results
        output_path = args.output or os.path.join(
            args.results_dir, analyzer, f"validation_results.json"
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"  Results saved to: {output_path}")

        # Print report
        print_analyzer_report(analyzer, results)

    # ── Cross-Analyzer Precision Comparison ──────────────────────────────────
    if len(all_analyzer_results) >= 2:
        print(f"\n{'=' * 60}")
        print(f"  PRECISION COMPARISON ACROSS ANALYZERS")
        print(f"{'=' * 60}")
        print(f"  {'Analyzer':<12} {'Evaluated':>9} {'TP':>5} {'FP':>5} {'Precision':>10} {'FP Rate':>8}")
        print(f"  {'─'*12} {'─'*9} {'─'*5} {'─'*5} {'─'*10} {'─'*8}")

        for analyzer in analyzers:
            results = all_analyzer_results.get(analyzer, [])
            evaluated = [r for r in results if r["verdict"] in ("TRUE_POSITIVE", "FALSE_POSITIVE")]
            tp = sum(1 for r in evaluated if r["verdict"] == "TRUE_POSITIVE")
            fp = sum(1 for r in evaluated if r["verdict"] == "FALSE_POSITIVE")
            n = len(evaluated)
            if n > 0:
                prec = tp / n * 100
                fpr = fp / n * 100
                print(f"  {analyzer:<12} {n:>9} {tp:>5} {fp:>5} {prec:>9.1f}% {fpr:>7.1f}%")
            else:
                print(f"  {analyzer:<12} {'N/A':>9}")

        # Per-severity comparison
        for sev in ["HIGH", "MEDIUM", "LOW"]:
            has_data = False
            for results in all_analyzer_results.values():
                if any(r["reported_severity"] == sev and r["verdict"] in ("TRUE_POSITIVE", "FALSE_POSITIVE")
                       for r in results):
                    has_data = True
                    break
            if not has_data:
                continue

            print(f"\n  {sev} severity:")
            for analyzer in analyzers:
                results = all_analyzer_results.get(analyzer, [])
                sev_results = [r for r in results
                               if r["reported_severity"] == sev
                               and r["verdict"] in ("TRUE_POSITIVE", "FALSE_POSITIVE")]
                if sev_results:
                    tp = sum(1 for r in sev_results if r["verdict"] == "TRUE_POSITIVE")
                    print(f"    {analyzer:<12} {tp}/{len(sev_results)} TP "
                          f"({tp/len(sev_results)*100:.1f}%)")

    print(f"\n{'=' * 60}")
    print(f"  Validation complete.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
