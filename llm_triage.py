"""
llm_triage.py — LLM-assisted false-positive triage of SAST findings

Takes a scan_results.json (from Bandit or Semgrep runs), draws a
stratified random sample (N findings per severity per tool), sends each
finding to an LLM for classification, and writes results to a JSON file.

Usage:
  python llm_triage.py --input results/bandit_results.json \
                       --sample-per-severity 100 --verbose

Requires:
  pip install anthropic
  export ANTHROPIC_API_KEY=sk-...
"""

import argparse
import json
import os
import random
import sys
import time

try:
    import anthropic
except ImportError:
    print("Error: pip install anthropic", file=sys.stderr)
    sys.exit(1)


TRIAGE_PROMPT = """\
You are a security expert triaging static analysis findings.

A SAST tool flagged the following code as a potential vulnerability.
Analyze the code snippet and the tool's report, then classify it.

## Finding Details
- **Tool Rule**: {test_id} ({test_name})
- **Severity**: {issue_severity}
- **Description**: {issue_text}
- **File**: {filename}
- **Line**: {line_number}

## Code Snippet
```python
{code}
```

## Repository Context
- **Repository**: {repo}

## Your Task
Classify this finding as one of:
- **TRUE_POSITIVE**: The code genuinely contains a security vulnerability or bad practice that could be exploited or cause harm.
- **FALSE_POSITIVE**: The finding is a false alarm — the code is safe, or the flagged pattern is used in a benign/intentional way (e.g., test code, example, documentation, or the pattern is not actually exploitable in context).

Respond in exactly this JSON format:
{{
  "classification": "TRUE_POSITIVE" or "FALSE_POSITIVE",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "reasoning": "Brief explanation (1-3 sentences)"
}}
"""


def load_findings(input_path):
    """Load findings from a scan_results.json file."""
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    # Filter to only source-code findings (Bandit/Semgrep), skip pip-audit
    source_findings = [
        r for r in data
        if not r.get("test_id", "").startswith("CVE-")
        and not r.get("test_id", "").startswith("PYSEC-")
        and not r.get("test_id", "").startswith("SKY-")
        and r.get("code", "").strip()  # must have a code snippet
    ]
    return source_findings


def stratified_sample(findings, n_per_severity, seed=42):
    """
    Draw a stratified random sample: n_per_severity findings from each
    severity level (HIGH, MEDIUM, LOW). If a severity has fewer than
    n_per_severity findings, all are included.
    """
    random.seed(seed)
    by_severity = {"HIGH": [], "MEDIUM": [], "LOW": []}

    for f in findings:
        sev = f.get("issue_severity", "LOW").upper()
        if sev in by_severity:
            by_severity[sev].append(f)

    sample = []
    for sev, pool in by_severity.items():
        n = min(n_per_severity, len(pool))
        sample.extend(random.sample(pool, n))

    random.shuffle(sample)
    return sample


def triage_finding(client, finding, model="claude-haiku-4-5-20251001"):
    """Send one finding to the LLM and return the parsed classification."""
    prompt = TRIAGE_PROMPT.format(
        test_id=finding.get("test_id", ""),
        test_name=finding.get("test_name", ""),
        issue_severity=finding.get("issue_severity", ""),
        issue_text=finding.get("issue_text", ""),
        filename=finding.get("filename", ""),
        line_number=finding.get("line_number", ""),
        code=finding.get("code", ""),
        repo=finding.get("repo", ""),
    )

    try:
        response = client.messages.create(
            model=model,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Extract JSON from response (handle markdown code blocks)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        result = json.loads(text)
        return {
            "classification": result.get("classification", "UNKNOWN"),
            "confidence": result.get("confidence", "UNKNOWN"),
            "reasoning": result.get("reasoning", ""),
        }

    except (json.JSONDecodeError, IndexError, KeyError) as e:
        return {
            "classification": "ERROR",
            "confidence": "NONE",
            "reasoning": f"Failed to parse LLM response: {e}",
        }
    except anthropic.RateLimitError:
        time.sleep(10)
        return triage_finding(client, finding, model)


def run_triage(input_path, output_path, n_per_severity=100,
               model="claude-haiku-4-5-20251001", verbose=False):
    """Main triage pipeline."""
    if verbose:
        print(f"Loading findings from {input_path}...", file=sys.stderr)

    findings = load_findings(input_path)
    if not findings:
        print("No source-code findings with code snippets found.", file=sys.stderr)
        return

    if verbose:
        sev_counts = {}
        for f in findings:
            s = f.get("issue_severity", "?")
            sev_counts[s] = sev_counts.get(s, 0) + 1
        print(f"Total eligible findings: {len(findings)}", file=sys.stderr)
        print(f"  By severity: {sev_counts}", file=sys.stderr)

    sample = stratified_sample(findings, n_per_severity)
    if verbose:
        print(f"Sampled {len(sample)} findings ({n_per_severity} per severity)", file=sys.stderr)

    client = anthropic.Anthropic()
    results = []

    for i, finding in enumerate(sample, 1):
        if verbose:
            print(
                f"  [{i}/{len(sample)}] {finding['test_id']} "
                f"({finding['issue_severity']}) — {finding['repo']}",
                file=sys.stderr,
            )

        triage = triage_finding(client, finding, model=model)

        results.append({
            "repo": finding["repo"],
            "filename": finding["filename"],
            "test_id": finding["test_id"],
            "test_name": finding["test_name"],
            "issue_severity": finding["issue_severity"],
            "line_number": finding["line_number"],
            "classification": triage["classification"],
            "triage_confidence": triage["confidence"],
            "reasoning": triage["reasoning"],
            "model": model,
        })

    # Write results
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Print summary
    tp = sum(1 for r in results if r["classification"] == "TRUE_POSITIVE")
    fp = sum(1 for r in results if r["classification"] == "FALSE_POSITIVE")
    err = sum(1 for r in results if r["classification"] == "ERROR")
    total = len(results)

    print(f"\n{'='*50}")
    print(f"LLM Triage Summary ({model})")
    print(f"{'='*50}")
    print(f"Total triaged:    {total}")
    print(f"True positives:   {tp} ({tp/total*100:.1f}%)")
    print(f"False positives:  {fp} ({fp/total*100:.1f}%)")
    if err:
        print(f"Errors:           {err}")
    print(f"Precision (est.): {tp/total*100:.1f}%")
    print(f"\nResults saved to: {output_path}")

    # Breakdown by severity
    print(f"\nBy severity:")
    for sev in ["HIGH", "MEDIUM", "LOW"]:
        sev_results = [r for r in results if r["issue_severity"] == sev]
        if not sev_results:
            continue
        sev_tp = sum(1 for r in sev_results if r["classification"] == "TRUE_POSITIVE")
        print(f"  {sev:6s}: {sev_tp}/{len(sev_results)} true positives "
              f"({sev_tp/len(sev_results)*100:.1f}%)")

    # Breakdown by tool (infer from test_id prefix)
    print(f"\nBy tool:")
    tools = {}
    for r in results:
        tid = r["test_id"]
        tool = "bandit" if tid.startswith("B") else "semgrep"
        tools.setdefault(tool, []).append(r)
    for tool, tool_results in sorted(tools.items()):
        tool_tp = sum(1 for r in tool_results if r["classification"] == "TRUE_POSITIVE")
        print(f"  {tool:8s}: {tool_tp}/{len(tool_results)} true positives "
              f"({tool_tp/len(tool_results)*100:.1f}%)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="LLM-assisted triage of SAST findings.",
    )
    parser.add_argument("--input", required=True,
                        help="Path to scan_results.json (Bandit/Semgrep output)")
    parser.add_argument("--output", default="results/triage_results.json",
                        help="Output path for triage results (default: results/triage_results.json)")
    parser.add_argument("--sample-per-severity", type=int, default=100,
                        help="Number of findings to sample per severity level (default: 100)")
    parser.add_argument("--model", default="claude-haiku-4-5-20251001",
                        help="Anthropic model to use (default: claude-haiku-4-5-20251001)")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_triage(
        input_path=args.input,
        output_path=args.output,
        n_per_severity=args.sample_per_severity,
        model=args.model,
        verbose=args.verbose,
    )
