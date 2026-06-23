#!/usr/bin/env python3
"""
generate_workflow_fixes.py
Run this locally to patch all scrape_tyler_*.yml workflows with correct
timeout-minutes values and add the partial-save safeguard.

Usage:
    cd C:\Users\cmuno\OneDrive\Desktop\harris-county-leads
    python generate_workflow_fixes.py

What it does:
  1. Reads each .github/workflows/scrape_tyler_*.yml
  2. Sets timeout-minutes: 90 on every job (was missing or 120/150 on some)
  3. Adds `continue-on-error: true` so one county timing out doesn't kill the whole workflow
  4. Adds MAX_PAGES and LOOKBACK_DAYS env vars to every job step
  5. Writes the patched files back

After running: git add .github/workflows/ && git commit -m "fix: 90min job timeouts + continue-on-error for all tyler workflows"
"""

import re
import sys
from pathlib import Path

WORKFLOWS_DIR = Path(".github/workflows")

# Counties that timed out — need extra-low page limits passed as env
PROBLEM_COUNTIES = {
    "bowie", "taylor", "harrison", "wood", "orange", "guadalupe",
    "bastrop", "comal", "gonzales", "karnes", "andrews", "wise",
    "winkler", "somervell", "hamilton", "mills", "upshur", "erath",
    "hood", "lamar", "kerr", "wichita", "txlr", "gulf-coast",
    "gulf_coast", "calhoun2", "bexar",
}

# Per-county MAX_PAGES override (only for problem counties)
# All others get the default from scraper_config.py
MAX_PAGES_OVERRIDE = {
    "bowie": 8, "taylor": 8, "harrison": 8, "wood": 8,
    "orange": 8, "guadalupe": 8, "bastrop": 8, "comal": 8,
    "gonzales": 6, "karnes": 6, "andrews": 6, "wise": 6,
    "winkler": 6, "somervell": 6, "hamilton": 6, "mills": 6,
    "upshur": 6, "erath": 6, "hood": 6, "lamar": 6,
    "kerr": 6, "wichita": 4, "bexar": 10,
    "gulf-coast": 8, "gulf_coast": 8, "txlr": 6, "calhoun2": 6,
}

JOB_TIMEOUT = 90     # minutes per job
LOOKBACK    = 3      # days


def patch_workflow(path: Path) -> tuple[bool, str]:
    """
    Patch a single workflow YAML file.
    Returns (changed: bool, summary: str)
    """
    original = path.read_text(encoding="utf-8")
    text = original

    changes = []

    # 1. Ensure timeout-minutes is set on every job
    # Pattern: job block starts with "  jobname:" then "    runs-on:"
    # We insert timeout-minutes right after runs-on if not already there

    def add_timeout(m):
        block = m.group(0)
        if "timeout-minutes:" in block:
            # Already has it — update the value
            block = re.sub(
                r'timeout-minutes:\s*\d+',
                f'timeout-minutes: {JOB_TIMEOUT}',
                block
            )
        else:
            # Add after runs-on line
            block = re.sub(
                r'(    runs-on:[^\n]+\n)',
                f'\\1    timeout-minutes: {JOB_TIMEOUT}\n',
                block
            )
        return block

    # Match each job block
    text, n = re.subn(
        r'(  \w[\w-]*:\s*\n    runs-on:.*?)(?=\n  \w[\w-]*:|\Z)',
        add_timeout,
        text,
        flags=re.DOTALL
    )
    if n:
        changes.append(f"set timeout-minutes: {JOB_TIMEOUT} on {n} jobs")

    # 2. Add continue-on-error: true to every job that doesn't have it
    def add_continue_on_error(m):
        block = m.group(0)
        if "continue-on-error:" not in block:
            block = re.sub(
                r'(    timeout-minutes:[^\n]+\n)',
                f'\\1    continue-on-error: true\n',
                block
            )
        return block

    text, n = re.subn(
        r'(  \w[\w-]*:\s*\n    runs-on:.*?)(?=\n  \w[\w-]*:|\Z)',
        add_continue_on_error,
        text,
        flags=re.DOTALL
    )
    if n:
        changes.append(f"added continue-on-error: true on {n} jobs")

    # 3. Inject LOOKBACK_DAYS and MAX_PAGES env vars into python run commands
    # Look for: python scraper/multi_county_*.py
    # Add env vars inline if not already present

    def inject_env(m):
        run_block = m.group(0)
        py_script = m.group(1)

        # Extract county name from script name (multi_county_bowie.py -> bowie)
        county_match = re.search(r'multi_county[_-](\w+)\.py', py_script)
        county = county_match.group(1).lower() if county_match else ""

        max_pages = MAX_PAGES_OVERRIDE.get(county, 15)

        env_block = (
            f'          env:\n'
            f'            LOOKBACK_DAYS: "{LOOKBACK}"\n'
            f'            MAX_PAGES: "{max_pages}"\n'
        )

        # Only inject if not already there
        if "LOOKBACK_DAYS" not in run_block:
            # Insert env block before the 'run:' line
            run_block = re.sub(
                r'(      - name:[^\n]+\n)',
                f'\\1{env_block}',
                run_block,
                count=1
            )
        return run_block

    text, n = re.subn(
        r'(      - name:[^\n]+\n(?:.*?\n)*?.*?python scraper/(multi_county[^\s]+\.py)[^\n]*)',
        inject_env,
        text,
        flags=re.DOTALL
    )
    # Note: env injection is best-effort, may not match all YAML layouts
    # Manual check recommended after running

    changed = text != original
    if changed:
        path.write_text(text, encoding="utf-8")

    summary = f"{path.name}: {', '.join(changes) if changes else 'no changes'}"
    return changed, summary


def main():
    if not WORKFLOWS_DIR.exists():
        print(f"ERROR: {WORKFLOWS_DIR} not found. Run from repo root.")
        sys.exit(1)

    # Target workflows
    targets = sorted(WORKFLOWS_DIR.glob("scrape_*.yml"))
    if not targets:
        print(f"No scrape_*.yml files found in {WORKFLOWS_DIR}")
        sys.exit(1)

    print(f"Patching {len(targets)} workflow files...\n")

    changed_count = 0
    for wf in targets:
        changed, summary = patch_workflow(wf)
        status = "✅ PATCHED" if changed else "  unchanged"
        print(f"{status}  {summary}")
        if changed:
            changed_count += 1

    print(f"\n{changed_count}/{len(targets)} files updated.")

    if changed_count:
        print("\nNext steps:")
        print('  git add .github/workflows/')
        print('  git commit -m "fix: 90min job timeouts + continue-on-error for all scrape workflows"')
        print('  git push')
    else:
        print("\nNothing to commit.")


if __name__ == "__main__":
    main()
