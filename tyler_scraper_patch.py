#!/usr/bin/env python3
"""
tyler_scraper_patch.py
Patches every scraper/multi_county_*.py to:
  1. Import and use LOOKBACK_DAYS from scraper_config (instead of hardcoded 14/30)
  2. Import and use get_county_max_pages() for MAX_PAGES
  3. Add env-var overrides so GitHub Actions can pass LOOKBACK_DAYS / MAX_PAGES
     without modifying the Python file

Run from repo root:
    cd C:\Users\cmuno\OneDrive\Desktop\harris-county-leads
    python tyler_scraper_patch.py

After: git add scraper/ && git commit -m "fix: use scraper_config for lookback/max_pages across all scrapers"
"""

import re
import sys
from pathlib import Path

SCRAPER_DIR = Path("scraper")

# The import block we inject at the top of each scraper (after existing imports)
CONFIG_IMPORT = '''
# ── StackIQ scraper config (centralized timeouts + page limits) ────────────────
import os as _os
import sys as _sys
_SCRAPER_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _SCRAPER_DIR not in _sys.path:
    _sys.path.insert(0, _SCRAPER_DIR)
try:
    from scraper_config import (
        LOOKBACK_DAYS as _CFG_LOOKBACK,
        get_county_max_pages as _cfg_max_pages,
        COUNTY_HARD_TIMEOUT_SECONDS,
        PAGE_LOAD_TIMEOUT_MS,
        NETWORK_IDLE_TIMEOUT_MS,
        RESULTS_WAIT_MS,
    )
except ImportError:
    _CFG_LOOKBACK = 3
    COUNTY_HARD_TIMEOUT_SECONDS = 4800
    PAGE_LOAD_TIMEOUT_MS = 30000
    NETWORK_IDLE_TIMEOUT_MS = 15000
    RESULTS_WAIT_MS = 8000
    def _cfg_max_pages(c): return 10

# Allow GitHub Actions env vars to override config
LOOKBACK_DAYS = int(_os.environ.get("LOOKBACK_DAYS", _CFG_LOOKBACK))
# MAX_PAGES is set per-county by get_max_pages() below
_ENV_MAX_PAGES = _os.environ.get("MAX_PAGES")  # optional global override

def get_max_pages(county: str = "") -> int:
    if _ENV_MAX_PAGES:
        return int(_ENV_MAX_PAGES)
    return _cfg_max_pages(county)
# ──────────────────────────────────────────────────────────────────────────────
'''

# Patterns that indicate a hardcoded LOOKBACK_DAYS or MAX_PAGES in the scraper
LOOKBACK_PATTERN = re.compile(
    r'^(LOOKBACK_DAYS\s*=\s*)(\d+)',
    re.MULTILINE
)
MAXPAGES_PATTERN = re.compile(
    r'^(MAX_PAGES\s*=\s*)(\d+)',
    re.MULTILINE
)

# We replace hardcoded values with references to the config
LOOKBACK_REPLACEMENT = r'\g<1>int(os.environ.get("LOOKBACK_DAYS", 3))'
MAXPAGES_REPLACEMENT = r'\g<1>get_max_pages()'


def patch_scraper(path: Path) -> tuple[bool, list[str]]:
    """Patch a single scraper file. Returns (changed, [change_descriptions])."""
    original = path.read_text(encoding="utf-8-sig")
    text = original
    changes = []

    # Skip if already patched
    if "scraper_config" in text:
        return False, ["already patched"]

    # 1. Replace hardcoded LOOKBACK_DAYS = N
    old_lookback = LOOKBACK_PATTERN.search(text)
    if old_lookback:
        old_val = old_lookback.group(2)
        text = LOOKBACK_PATTERN.sub(
            lambda m: f'{m.group(1)}int(os.environ.get("LOOKBACK_DAYS", 3))',
            text
        )
        changes.append(f"LOOKBACK_DAYS: {old_val} → env/3")

    # 2. Replace hardcoded MAX_PAGES = N  
    old_maxpages = MAXPAGES_PATTERN.search(text)
    if old_maxpages:
        old_val = old_maxpages.group(2)
        text = MAXPAGES_PATTERN.sub(
            lambda m: f'{m.group(1)}get_max_pages()',
            text,
            count=1
        )
        changes.append(f"MAX_PAGES: {old_val} → get_max_pages()")

    # 3. Inject config import block after the last import line
    # Find the last "import X" or "from X import Y" line
    last_import_match = None
    for m in re.finditer(r'^(?:import |from )\S+.*$', text, re.MULTILINE):
        last_import_match = m

    if last_import_match:
        insert_pos = last_import_match.end()
        text = text[:insert_pos] + "\n" + CONFIG_IMPORT + text[insert_pos:]
        changes.append("injected scraper_config import")
    else:
        # No imports found — prepend
        text = CONFIG_IMPORT + "\n" + text
        changes.append("prepended scraper_config import (no imports found)")

    changed = text != original
    if changed:
        path.write_text(text, encoding="utf-8")

    return changed, changes


def main():
    if not SCRAPER_DIR.exists():
        print(f"ERROR: {SCRAPER_DIR} not found. Run from repo root.")
        sys.exit(1)

    scrapers = sorted(SCRAPER_DIR.glob("multi_county_*.py"))
    # Also patch pubsearch and eagleweb
    scrapers += sorted(SCRAPER_DIR.glob("tyler_universal.py"))
    scrapers += sorted(SCRAPER_DIR.glob("multi_county_pubsearch.py"))
    scrapers += sorted(SCRAPER_DIR.glob("multi_county_eagleweb.py"))
    scrapers += sorted(SCRAPER_DIR.glob("multi_county_portals.py"))

    if not scrapers:
        print(f"No multi_county_*.py files found in {SCRAPER_DIR}")
        sys.exit(1)

    print(f"Patching {len(scrapers)} scraper files...\n")

    patched = 0
    skipped = 0
    for path in scrapers:
        changed, desc = patch_scraper(path)
        if changed:
            print(f"✅ {path.name}: {', '.join(desc)}")
            patched += 1
        else:
            print(f"   {path.name}: {', '.join(desc)}")
            skipped += 1

    print(f"\n{patched} patched, {skipped} skipped.")

    if patched:
        print("\nNext steps:")
        print('  git add scraper/')
        print('  git commit -m "fix: centralize lookback/max_pages via scraper_config"')
        print('  git push')


if __name__ == "__main__":
    main()
