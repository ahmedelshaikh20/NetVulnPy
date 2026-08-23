"""
sample_validation.py — Pick 100 random findings per analyzer, verify against actual source code.

For each analyzer (bandit, semgrep, llm, pip-audit):
  1. Randomly sample 100 findings (stratified by severity)
  2. Read the actual source file from downloads/
  3. Send to LLM for independent TRUE_POSITIVE / FALSE_POSITIVE verdict
  4. Report precision, false-positive rate, and severity breakdown
  5. Save detailed results to results/<analyzer>/sample_100_validation.json

Usage:
  python sample_validation.py
  python sample_validation.py --analyzer llm
  python sample_validation.py --model gemma4-31b-it
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


VERIFY_PROMPT = """\
You are a senior security auditor. A static analysis tool flagged the \
code below as a potential vulnerability. Read the FULL source file and \
decide whether this is a REAL vulnerability or a FALSE ALARM.

## Tool Finding
- Tool: {analyzer}
- Rule: {test_id} ({test_name})
- Severity: {issue_severity}
- Description: {issue_text}
- File: {filename}
- Line: {line_number}

## Reported Code Snippet
```python
{reported_code}
```

## Full Source File
```python
{actual_code}
```

## Instructions
Decide if this is a REAL exploitable security issue in a production context.
Consider: Is this test/demo code? Is the pattern used safely? Are there mitigations?

Respond ONLY with this JSON:
{{
  "verdict": "TRUE_POSITIVE" or "FALSE_POSITIVE",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "actual_severity": "CRITICAL" or "HIGH" or "MEDIUM" or "LOW" or "NONE",
  "reasoning": "2-3 sentence explanation"
}}
"""


def load_findings(results_dir, analyzer):
    path = os.path.join(results_dir, analyzer, "scan_results.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_source(downloads_dir, repo, filename):
    repo_dir = repo.replace("/", "_", 1)
    filepath = os.path.join(downloads_dir, repo_dir, filename)

    if not os.path.exists(filepath):
        # Strip downloads/owner_repo/ prefix if semgrep included it
        for prefix in [f"downloads/{repo_dir}/", f"downloads\\{repo_dir}\\"]:
            if filename.startswith(prefix):
                filepath = os.path.join(downloads_dir, repo_dir, filename[len(prefix):])
                break

    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, encoding="utf-8", errors="ignore") as f:
            return f.read()
    except OSError:
        return None


def sample_100(findings, seed=42):
    """Stratified sample: up to 100 findings, balanced across severities."""
    random.seed(seed)
    by_sev = {"HIGH": [], "MEDIUM": [], "LOW": []}
    for f in findings:
        sev = f.get("issue_severity", "LOW").upper()
        if sev in by_sev:
            by_sev[sev].append(f)

    # Distribute 100 across available severities
    present = {s: pool for s, pool in by_sev.items() if pool}
    per_sev = 100 // len(present) if present else 0
    remainder = 100 - per_sev * len(present)

    sample = []
    for i, (sev, pool) in enumerate(sorted(present.items())):
        n = per_sev + (1 if i < remainder else 0)
        n = min(n, len(pool))
        sample.extend(random.sample(pool, n))

    random.shuffle(sample)
    return sample[:100]


def verify_one(client, model, finding, actual_code, analyzer, retries=2):
    # Truncate long files, keep area around flagged line
    if len(actual_code) > 20_000:
        lines = actual_code.split("\n")
        target = finding.get("line_number", 0)
        if isinstance(target, int) and target > 0:
            start = max(0, target - 50)
            end = min(len(lines), target + 150)
            actual_code = f"[lines {start+1}-{end}]\n" + "\n".join(lines[start:end])
        else:
            actual_code = actual_code[:20_000] + "\n[truncated]"

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

    for attempt in range(retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=512,
                temperature=0.1,
            )
            text = resp.choices[0].message.content.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            r = json.loads(text)
            return {
                "verdict": r.get("verdict", "UNKNOWN"),
                "confidence": r.get("confidence", "UNKNOWN"),
                "actual_severity": r.get("actual_severity", "UNKNOWN"),
                "reasoning": r.get("reasoning", ""),
            }
        except (json.JSONDecodeError, Exception) as e:
            if attempt < retries:
                time.sleep(2)
            else:
                return {
                    "verdict": "ERROR",
                    "confidence": "NONE",
                    "actual_severity": "UNKNOWN",
                    "reasoning": str(e)[:200],
                }


def run_validation(analyzer, findings, client, model, downloads_dir, results_dir, verbose):
    print(f"\n{'=' * 60}")
    print(f"  {analyzer.upper()} — Sampling 100 findings")
    print(f"{'=' * 60}")

    # Filter to findings with code (verifiable)
    verifiable = [f for f in findings if f.get("code", "").strip()]
    if not verifiable:
        verifiable = findings  # pip-audit: code field = fix info

    sample = sample_100(verifiable)
    sev_dist = Counter(f["issue_severity"] for f in sample)
    print(f"  Total findings: {len(findings)}")
    print(f"  Sampled: {len(sample)}  ({dict(sev_dist)})")

    results = []

    def _process(idx_finding):
        idx, finding = idx_finding
        code = read_source(downloads_dir, finding["repo"], finding["filename"])
        if code is None:
            return {
                "idx": idx, "verdict": "SKIPPED", "confidence": "NONE",
                "actual_severity": "UNKNOWN",
                "reasoning": f"Source not found: {finding['filename']}",
                "repo": finding["repo"], "filename": finding["filename"],
                "test_id": finding.get("test_id", ""),
                "test_name": finding.get("test_name", ""),
                "reported_severity": finding.get("issue_severity", ""),
                "issue_text": finding.get("issue_text", "")[:200],
                "line_number": finding.get("line_number"),
            }
        v = verify_one(client, model, finding, code, analyzer)
        return {
            "idx": idx, **v,
            "repo": finding["repo"], "filename": finding["filename"],
            "test_id": finding.get("test_id", ""),
            "test_name": finding.get("test_name", ""),
            "reported_severity": finding.get("issue_severity", ""),
            "issue_text": finding.get("issue_text", "")[:200],
            "line_number": finding.get("line_number"),
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_process, (i, f)): i for i, f in enumerate(sample)}
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            if verbose:
                tag = {"TRUE_POSITIVE": "TP", "FALSE_POSITIVE": "FP",
                       "ERROR": "ER", "SKIPPED": "SK"}.get(r["verdict"], "??")
                print(f"  [{len(results):>3}/100] [{tag}] {r['test_id']:<30s} {r['repo']}")

    results.sort(key=lambda r: r["idx"])

    # Save
    out_path = os.path.join(results_dir, analyzer, "sample_100_validation.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {out_path}")

    # Report
    evaluated = [r for r in results if r["verdict"] in ("TRUE_POSITIVE", "FALSE_POSITIVE")]
    skipped = sum(1 for r in results if r["verdict"] == "SKIPPED")
    errors = sum(1 for r in results if r["verdict"] == "ERROR")
    tp = sum(1 for r in evaluated if r["verdict"] == "TRUE_POSITIVE")
    fp = sum(1 for r in evaluated if r["verdict"] == "FALSE_POSITIVE")
    n = len(evaluated)

    print(f"\n  {'─' * 50}")
    print(f"  Results: {n} evaluated, {skipped} skipped, {errors} errors")
    if n > 0:
        print(f"  True Positives:    {tp:>4}  ({tp/n*100:.1f}%)")
        print(f"  False Positives:   {fp:>4}  ({fp/n*100:.1f}%)")
        print(f"  Precision:         {tp/n*100:.1f}%")

        print(f"\n  By severity:")
        for sev in ["HIGH", "MEDIUM", "LOW"]:
            sr = [r for r in evaluated if r["reported_severity"] == sev]
            if sr:
                stp = sum(1 for r in sr if r["verdict"] == "TRUE_POSITIVE")
                print(f"    {sev:<6s}  {stp:>3}/{len(sr):<3} TP  ({stp/len(sr)*100:.1f}%)")

        # Example TPs and FPs
        tps = [r for r in results if r["verdict"] == "TRUE_POSITIVE"]
        fps = [r for r in results if r["verdict"] == "FALSE_POSITIVE"]
        if tps:
            print(f"\n  Example TRUE POSITIVES:")
            for r in tps[:3]:
                print(f"    [{r['reported_severity']}] {r['test_id']} — {r['repo']}")
                print(f"         {r['reasoning'][:100]}")
        if fps:
            print(f"\n  Example FALSE POSITIVES:")
            for r in fps[:3]:
                print(f"    [{r['reported_severity']}] {r['test_id']} — {r['repo']}")
                print(f"         {r['reasoning'][:100]}")

    return {"analyzer": analyzer, "total": n, "tp": tp, "fp": fp,
            "skipped": skipped, "errors": errors, "results": results}


def main():
    parser = argparse.ArgumentParser(description="Sample 100 findings per analyzer and validate.")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--downloads-dir", default="downloads")
    parser.add_argument("--analyzer", default=None, help="Single analyzer (default: all)")
    parser.add_argument("--api-key", default=None, help="LLM API key (or LLM_API_KEY env)")
    parser.add_argument("--endpoint", default="https://llms.innkube.fim.uni-passau.de")
    parser.add_argument("--model", default="qwen3-next-80b-a3b-instruct")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("LLM_API_KEY")
    if not api_key:
        print("Error: --api-key or LLM_API_KEY required.", file=sys.stderr)
        sys.exit(1)

    client = openai.OpenAI(api_key=api_key, base_url=args.endpoint)

    # Discover analyzers
    if args.analyzer:
        analyzers = [args.analyzer]
    else:
        analyzers = sorted(
            d for d in os.listdir(args.results_dir)
            if os.path.isdir(os.path.join(args.results_dir, d))
            and os.path.exists(os.path.join(args.results_dir, d, "scan_results.json"))
        )

    print(f"Analyzers: {', '.join(analyzers)}")
    print(f"Model: {args.model}")
    print(f"Sample: 100 per analyzer (stratified by severity)")

    summaries = []
    for analyzer in analyzers:
        findings = load_findings(args.results_dir, analyzer)
        if not findings:
            print(f"\n  {analyzer}: no findings found, skipping.")
            continue
        s = run_validation(analyzer, findings, client, args.model,
                           args.downloads_dir, args.results_dir, args.verbose)
        summaries.append(s)

    # Final comparison table
    if len(summaries) >= 2:
        print(f"\n{'=' * 60}")
        print(f"  PRECISION COMPARISON (100 samples each)")
        print(f"{'=' * 60}")
        print(f"  {'Analyzer':<12} {'Evaluated':>9} {'TP':>5} {'FP':>5} {'Precision':>10} {'FP Rate':>8}")
        print(f"  {'─'*12} {'─'*9} {'─'*5} {'─'*5} {'─'*10} {'─'*8}")
        for s in summaries:
            n = s["total"]
            if n > 0:
                print(f"  {s['analyzer']:<12} {n:>9} {s['tp']:>5} {s['fp']:>5} "
                      f"{s['tp']/n*100:>9.1f}% {s['fp']/n*100:>7.1f}%")
            else:
                print(f"  {s['analyzer']:<12} {'N/A':>9}")

        # Per-severity
        for sev in ["HIGH", "MEDIUM", "LOW"]:
            has = False
            for s in summaries:
                if any(r["reported_severity"] == sev and r["verdict"] in ("TRUE_POSITIVE","FALSE_POSITIVE")
                       for r in s["results"]):
                    has = True
            if not has:
                continue
            print(f"\n  {sev} severity:")
            for s in summaries:
                sr = [r for r in s["results"]
                      if r["reported_severity"] == sev and r["verdict"] in ("TRUE_POSITIVE","FALSE_POSITIVE")]
                if sr:
                    tp = sum(1 for r in sr if r["verdict"] == "TRUE_POSITIVE")
                    print(f"    {s['analyzer']:<12} {tp:>3}/{len(sr):<3} TP ({tp/len(sr)*100:.1f}%)")

    # Save summary
    summary_path = os.path.join(args.results_dir, "validation_summary.json")
    summary_out = []
    for s in summaries:
        summary_out.append({
            "analyzer": s["analyzer"],
            "evaluated": s["total"],
            "true_positives": s["tp"],
            "false_positives": s["fp"],
            "precision": round(s["tp"] / s["total"] * 100, 1) if s["total"] > 0 else None,
            "skipped": s["skipped"],
            "errors": s["errors"],
        })
    with open(summary_path, "w") as f:
        json.dump(summary_out, f, indent=2)
    print(f"\n  Summary saved: {summary_path}")

    print(f"\n{'=' * 60}")
    print(f"  Done.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
