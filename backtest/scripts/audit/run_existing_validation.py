#!/usr/bin/env python3
"""
Run Existing Validation Scripts

Wrapper that runs existing validation scripts and combines results into one JSON.

Scripts run:
  - validation/c3_pre21_sanity.py
  - validation/c2_canonical_results.py
  - validation/c1_p0_audit.py
  - validation/a1_hold_time_analysis.py

Output: results/audit/02_existing_validation.json
Exit code: 0 if all scripts exit 0, else 1
"""

import subprocess
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any


def run_script(script_path: Path, args: list[str] | None = None) -> tuple[int, str, str]:
    """
    Run a Python script via subprocess.

    Returns:
        (returncode, stdout, stderr)
    """
    if not script_path.exists():
        return -1, "", f"Script not found: {script_path}"

    cmd = [sys.executable, str(script_path)] + (args or [])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"Script timeout (600s): {script_path}"
    except Exception as e:
        return -1, "", f"Error running script: {e}"


def main():
    BT_ROOT = Path(__file__).parent.parent.parent
    VALIDATION_SCRIPTS_DIR = BT_ROOT / "scripts" / "validation"
    RESULTS_DIR = BT_ROOT / "results" / "audit"
    OUTPUT_FILE = RESULTS_DIR / "02_existing_validation.json"

    # Ensure output dir exists
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # List of validation scripts to run
    scripts_to_run = [
        ("c3_pre21_sanity.py", VALIDATION_SCRIPTS_DIR / "c3_pre21_sanity.py", []),
        ("c2_canonical_results.py", VALIDATION_SCRIPTS_DIR / "c2_canonical_results.py", []),
        ("c1_p0_audit.py", VALIDATION_SCRIPTS_DIR / "c1_p0_audit.py", []),
        ("a1_hold_time_analysis.py", VALIDATION_SCRIPTS_DIR / "a1_hold_time_analysis.py", []),
    ]

    print("Running validation scripts...", file=sys.stderr)

    results = {}
    failed_count = 0

    for script_name, script_path, args in scripts_to_run:
        print(f"  Running {script_name}...", file=sys.stderr)

        returncode, stdout, stderr = run_script(script_path, args)

        if returncode == -1:
            # Script not found or error
            status = "SKIP"
            if "not found" in stderr.lower():
                msg = stderr
            else:
                msg = stderr or stdout
        elif returncode == 0:
            status = "PASS"
            msg = stdout
            if not msg:
                msg = "Script completed successfully (no output)"
        else:
            status = "FAIL"
            msg = stdout or stderr
            failed_count += 1

        # Capture last 500 chars of stdout for summary
        stdout_tail = stdout[-500:] if stdout else "(no output)"

        results[script_name] = {
            "returncode": returncode,
            "stdout_tail": stdout_tail,
            "status": status,
            "full_stdout": stdout,
            "full_stderr": stderr
        }

        print(f"    → {status} (rc={returncode})", file=sys.stderr)

    # Tally results
    summary = {
        "total": len(scripts_to_run),
        "pass": sum(1 for r in results.values() if r["status"] == "PASS"),
        "fail": sum(1 for r in results.values() if r["status"] == "FAIL"),
        "warn": 0,
        "skip": sum(1 for r in results.values() if r["status"] == "SKIP")
    }

    # Build output JSON
    output = {
        "audit_id": "02_existing_validation",
        "name": "existing_validation_wrapper",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "scripts_run": [name for name, _, _ in scripts_to_run],
        "results": {
            name: {
                "returncode": r["returncode"],
                "stdout_tail": r["stdout_tail"],
                "status": r["status"]
            }
            for name, r in results.items()
        },
        "checks": [],
        "summary": summary
    }

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # Print summary
    print(f"\nSUMMARY: pass={summary['pass']} fail={summary['fail']} warn={summary['warn']} skip={summary['skip']} → {OUTPUT_FILE}", file=sys.stdout)

    # Exit code
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
