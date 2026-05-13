#!/usr/bin/env python3
"""
Aggregate Audit Report Generator

Reads all audit JSON files from RESULTS_ROOT/audit/ and produces AUDIT_REPORT.md
and aggregate_summary.json.

Input files (read whichever exist):
  - 01_existing_v4.json
  - 02_existing_validation.json
  - 03_d1_equity.json
  - 04_d2_determinism.json
  - 05_d3_cost.json
  - 06_d4_funding.json

Classification:
  - status == "FAIL" → CRITICAL issue
  - status == "WARN" → WARNING issue
  - status == "PASS" → INFO/passed
  - status == "SKIP" → INFO/skipped

Output:
  - RESULTS_ROOT/audit/AUDIT_REPORT.md (markdown report)
  - RESULTS_ROOT/audit/aggregate_summary.json (summary counts)

Exit code: 0 if CRITICAL=0, else 1
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone


def get_results_root():
    """Find RESULTS_ROOT by scanning up from script location."""
    script = Path(__file__).resolve()
    current = script.parent  # audit/
    while current != current.parent:
        # Check if we're in backtest/scripts/audit, then results is 2 levels up
        candidate = current.parent.parent / "results"
        if candidate.exists():
            return candidate
        current = current.parent
    raise RuntimeError("Cannot find RESULTS_ROOT")


RESULTS_ROOT = Path(get_results_root())
AUDIT_DIR = RESULTS_ROOT / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

# Expected audit files (in order)
AUDIT_FILES = [
    "01_existing_v4.json",
    "02_existing_validation.json",
    "03_d1_equity.json",
    "04_d2_determinism.json",
    "05_d3_cost.json",
    "06_d4_funding.json",
]


def load_audit_files():
    """
    Load all available audit JSON files.

    Returns:
      dict: {audit_id: {audit data}}
    """
    audits = {}

    for filename in AUDIT_FILES:
        filepath = AUDIT_DIR / filename
        if not filepath.exists():
            continue

        try:
            with open(filepath) as f:
                data = json.load(f)
                audit_id = data.get("audit_id", filename.replace(".json", ""))
                audits[audit_id] = data
                print(f"[aggregate] Loaded {audit_id} from {filename}", file=sys.stderr)
        except Exception as e:
            print(f"[aggregate] Warning: Could not load {filename}: {e}", file=sys.stderr)

    return audits


def classify_checks(audits):
    """
    Classify all checks into CRITICAL, WARNING, PASS, SKIP.

    Args:
      audits: dict of audit data

    Returns:
      dict: {
        "critical": [check dicts],
        "warning": [check dicts],
        "pass": [check dicts],
        "skip": [check dicts],
      }
    """
    classified = {
        "critical": [],
        "warning": [],
        "pass": [],
        "skip": [],
    }

    for audit_id, audit_data in audits.items():
        checks = audit_data.get("checks", [])

        for check in checks:
            status = check.get("status", "SKIP").upper()
            check_with_audit = {**check, "audit_id": audit_id}

            if status == "FAIL":
                classified["critical"].append(check_with_audit)
            elif status == "WARN":
                classified["warning"].append(check_with_audit)
            elif status == "PASS":
                classified["pass"].append(check_with_audit)
            else:  # SKIP or other
                classified["skip"].append(check_with_audit)

    return classified


def compute_audit_summary(audits):
    """
    Compute summary stats per audit.

    Args:
      audits: dict of audit data

    Returns:
      dict: {audit_id: {pass, warn, fail, skip}}
    """
    summary = {}

    for audit_id, audit_data in audits.items():
        audit_summary = audit_data.get("summary", {})
        summary[audit_id] = {
            "pass": audit_summary.get("pass", 0),
            "warn": audit_summary.get("warn", 0),
            "fail": audit_summary.get("fail", 0),
            "skip": audit_summary.get("skip", 0),
        }

    return summary


def generate_report_md(audits, classified, timestamp):
    """
    Generate markdown report.

    Args:
      audits: dict of audit data
      classified: dict of classified checks
      timestamp: ISO timestamp string

    Returns:
      str: markdown report
    """
    lines = []

    # Header
    lines.append("# Backtest Audit Report\n")
    lines.append(f"Generated: {timestamp}\n")
    lines.append(f"Audits run: {len(audits)} of {len(AUDIT_FILES)}\n\n")

    # Summary table
    total_checks = sum(len(v) for v in classified.values())
    critical_count = len(classified["critical"])
    warning_count = len(classified["warning"])
    pass_count = len(classified["pass"])
    skip_count = len(classified["skip"])

    lines.append("## Summary\n")
    lines.append("| Metric | Count |\n")
    lines.append("|--------|-------|\n")
    lines.append(f"| Total checks | {total_checks} |\n")
    lines.append(f"| CRITICAL (FAIL) | {critical_count} |\n")
    lines.append(f"| WARNING (WARN) | {warning_count} |\n")
    lines.append(f"| PASS | {pass_count} |\n")
    lines.append(f"| SKIP | {skip_count} |\n\n")

    # Critical issues section
    if critical_count > 0:
        lines.append("## CRITICAL Issues\n")
        for idx, check in enumerate(classified["critical"], 1):
            audit_id = check.get("audit_id", "unknown")
            check_id = check.get("id", "unknown")
            msg = check.get("msg", "No message")
            expected = check.get("expected", "N/A")
            actual = check.get("actual", "N/A")
            delta = check.get("delta", "N/A")

            lines.append(f"### [C{idx}] {msg}\n")
            lines.append(f"- **Source**: {audit_id} / {check_id}\n")
            lines.append(f"- **Expected**: {expected}\n")
            lines.append(f"- **Actual**: {actual}\n")
            if delta != "N/A":
                lines.append(f"- **Delta**: {delta}\n")
            lines.append("\n")
    else:
        lines.append("## CRITICAL Issues\n")
        lines.append("None.\n\n")

    # Warning issues section
    if warning_count > 0:
        lines.append("## WARNING Issues\n")
        for idx, check in enumerate(classified["warning"], 1):
            audit_id = check.get("audit_id", "unknown")
            check_id = check.get("id", "unknown")
            msg = check.get("msg", "No message")
            expected = check.get("expected", "N/A")
            actual = check.get("actual", "N/A")
            delta = check.get("delta", "N/A")

            lines.append(f"### [W{idx}] {msg}\n")
            lines.append(f"- **Source**: {audit_id} / {check_id}\n")
            lines.append(f"- **Expected**: {expected}\n")
            lines.append(f"- **Actual**: {actual}\n")
            if delta != "N/A":
                lines.append(f"- **Delta**: {delta}\n")
            lines.append("\n")
    else:
        lines.append("## WARNING Issues\n")
        lines.append("None.\n\n")

    # PASS / SKIP Summary
    lines.append("## Audit Details\n\n")
    audit_summary = compute_audit_summary(audits)
    for audit_id in sorted(audits.keys()):
        summary = audit_summary.get(audit_id, {})
        pass_c = summary.get("pass", 0)
        warn_c = summary.get("warn", 0)
        fail_c = summary.get("fail", 0)
        skip_c = summary.get("skip", 0)
        lines.append(
            f"- **{audit_id}**: "
            f"{pass_c} pass, {warn_c} warn, {fail_c} fail, {skip_c} skip\n"
        )

    if not audits:
        lines.append("- No audits loaded\n")

    return "".join(lines)


def main():
    print("[aggregate] Starting audit aggregation...", file=sys.stderr)

    # Load audits
    audits = load_audit_files()
    if not audits:
        print("[aggregate] WARNING: No audit files found", file=sys.stderr)

    # Classify checks
    classified = classify_checks(audits)
    critical_count = len(classified["critical"])
    warning_count = len(classified["warning"])
    pass_count = len(classified["pass"])
    skip_count = len(classified["skip"])

    print(
        f"[aggregate] Classified: {critical_count} critical, "
        f"{warning_count} warning, {pass_count} pass, {skip_count} skip",
        file=sys.stderr,
    )

    # Generate timestamp
    timestamp = datetime.now(timezone.utc).isoformat()

    # Generate markdown report
    report_md = generate_report_md(audits, classified, timestamp)

    # Write markdown report
    report_path = AUDIT_DIR / "AUDIT_REPORT.md"
    with open(report_path, "w") as f:
        f.write(report_md)
    print(f"[aggregate] Wrote {report_path}", file=sys.stderr)

    # Write aggregate summary JSON
    summary_data = {
        "ran_at": timestamp,
        "audits_run": len(audits),
        "audits_expected": len(AUDIT_FILES),
        "summary": {
            "total_checks": critical_count + warning_count + pass_count + skip_count,
            "critical": critical_count,
            "warning": warning_count,
            "pass": pass_count,
            "skip": skip_count,
        },
        "audit_details": compute_audit_summary(audits),
    }

    summary_path = AUDIT_DIR / "aggregate_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary_data, f, indent=2)
    print(f"[aggregate] Wrote {summary_path}", file=sys.stderr)

    # Print final summary
    print(
        f"AUDIT REPORT: CRITICAL={critical_count} WARNING={warning_count} "
        f"PASS={pass_count} → {report_path}",
        file=sys.stdout,
    )

    exit_code = 0 if critical_count == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
