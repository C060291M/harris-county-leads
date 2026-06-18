"""
scraper_config.py — StackIQ centralized scraper settings
Import this in every multi_county_*.py scraper.

The single source of truth for:
  - LOOKBACK_DAYS (was defaulting to 14+ in many scrapers — that's the root cause)
  - MAX_PAGES per county tier
  - Per-page and per-county hard timeouts
  - County tier classification (fast / medium / slow / large)

GitHub Actions job timeout-minutes should be set to GITHUB_JOB_TIMEOUT_MINUTES.
Individual county scrapers should use COUNTY_HARD_TIMEOUT_SECONDS as a signal
to stop and save whatever they have rather than running forever.
"""

# ── Core scrape parameters ─────────────────────────────────────────────────────

LOOKBACK_DAYS = 3          # was 14 in many scrapers — 14 days × large county = hours of work
                           # 3 days catches everything new, reruns are cheap

# GitHub Actions free tier: 6 hour workflow limit, jobs default to 6h too.
# We set explicit per-job limits well under 6h so the workflow can cancel
# cleanly and push partial results rather than being killed mid-write.
GITHUB_JOB_TIMEOUT_MINUTES = 90   # hard ceiling per county job in the yml

# Per-county wall-clock limit enforced inside the scraper itself.
# When this is hit, the scraper saves whatever records it has and exits 0.
# This means timed-out counties produce PARTIAL results instead of NOTHING.
COUNTY_HARD_TIMEOUT_SECONDS = 80 * 60   # 80 minutes — leaves 10m buffer under job limit

# ── Page limits by county tier ─────────────────────────────────────────────────
# Tier is based on population / filing volume, not just county size.
# These are MAX_PAGES values — scraper stops after this many result pages.

MAX_PAGES_DEFAULT  = 10   # unknown / rural counties
MAX_PAGES_SMALL    = 8    # < 50k population
MAX_PAGES_MEDIUM   = 12   # 50k–200k population
MAX_PAGES_LARGE    = 15   # 200k–500k population
MAX_PAGES_METRO    = 20   # > 500k population (harris, dallas, tarrant, bexar)

# ── Per-page Playwright timeouts ───────────────────────────────────────────────
PAGE_LOAD_TIMEOUT_MS    = 30_000   # initial page load
NETWORK_IDLE_TIMEOUT_MS = 15_000   # wait_until='networkidle' timeout
RESULTS_WAIT_MS         = 8_000    # wait for results table to appear
NEXT_PAGE_WAIT_MS       = 5_000    # wait between pages

# ── County tier map ────────────────────────────────────────────────────────────
# Counties that repeatedly hit timeouts get lower MAX_PAGES and tighter budgets.
# Format: county_slug -> (max_pages, tier_label)

COUNTY_TIERS = {
    # ── Metro (500k+) ──────────────────────────────────────────────────────────
    "harris":     (20, "metro"),
    "dallas":     (20, "metro"),
    "tarrant":    (20, "metro"),
    "bexar":      (20, "metro"),
    "travis":     (18, "metro"),
    "collin":     (16, "large"),
    "denton":     (16, "large"),
    "hidalgo":    (15, "large"),
    "williamson": (14, "large"),
    "el_paso":    (14, "large"),
    "nueces":     (14, "large"),
    "montgomery": (14, "large"),

    # ── Large (200k–500k) ──────────────────────────────────────────────────────
    "jefferson":  (12, "large"),
    "cameron":    (12, "large"),
    "brazos":     (12, "large"),
    "galveston":  (12, "large"),
    "fort_bend":  (12, "large"),
    "smith":      (12, "large"),
    "lubbock":    (12, "large"),
    "mclennan":   (12, "large"),
    "webb":       (12, "large"),
    "midland":    (12, "large"),

    # ── Problem counties (repeatedly hitting 2h timeout) — reduce pages ────────
    # These timed out in the screenshots: set low until we know their volume
    "bowie":      (8, "medium"),
    "taylor":     (8, "medium"),
    "harrison":   (8, "medium"),
    "wood":       (8, "medium"),
    "orange":     (8, "medium"),
    "guadalupe":  (8, "medium"),
    "bastrop":    (8, "medium"),
    "comal":      (8, "medium"),
    "gonzales":   (6, "small"),
    "karnes":     (6, "small"),
    "andrews":    (6, "small"),
    "wise":       (6, "small"),
    "winkler":    (6, "small"),
    "somervell":  (6, "small"),
    "hamilton":   (6, "small"),
    "mills":      (6, "small"),
    "upshur":     (6, "small"),
    "erath":      (6, "small"),
    "hood":       (6, "small"),
    "lamar":      (6, "small"),
    "kerr":       (6, "small"),
    "wichita":    (6, "small"),   # also has disclaimer JS bug

    # ── Default for everything not listed ─────────────────────────────────────
    # handled by get_county_max_pages() returning MAX_PAGES_DEFAULT
}


def get_county_max_pages(county: str) -> int:
    """Return the max pages limit for a given county slug."""
    slug = county.lower().replace(" county", "").replace(" ", "_").strip()
    tier_entry = COUNTY_TIERS.get(slug)
    if tier_entry:
        return tier_entry[0]
    return MAX_PAGES_DEFAULT


def get_county_timeout(county: str) -> int:
    """Return per-county hard timeout in seconds."""
    # Metro counties get a bit more time since they have more legitimate pages
    slug = county.lower().replace(" county", "").replace(" ", "_").strip()
    tier_entry = COUNTY_TIERS.get(slug)
    if tier_entry and tier_entry[1] == "metro":
        return COUNTY_HARD_TIMEOUT_SECONDS  # full 80 min
    return int(COUNTY_HARD_TIMEOUT_SECONDS * 0.75)  # 60 min for non-metro
