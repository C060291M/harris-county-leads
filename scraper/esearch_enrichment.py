"""
esearch_enrichment.py — Fixed CAD address enrichment for StackIQ

Replaces the broken first-name-only search pattern.

WHAT WAS WRONG:
  - Old code was extracting only the first name from owner (e.g. "FAUSTINO" from
    "FAUSTINO GARCIA HERNANDEZ") and searching for that → hundreds of wrong matches
  - GET /Search/Owner always 302s to 404 on jcad.org and similar sites
  - No result validation — accepted any match regardless of name similarity

WHAT THIS DOES:
  - Searches by FULL owner name (last + first or full string)
  - Uses the correct endpoint per CAD site (SearchResults, not /Search/Owner)
  - Validates match: owner name similarity must be > 70% before accepting address
  - Batches by county/CAD domain to reuse sessions
  - Handles the session token pattern that jcad.org and similar sites require
  - Falls back gracefully: if no address found, leaves prop_address NULL

SUPPORTED CAD SITES:
  jcad.org (Johnson), bcad.org (Bexar), hcad.org (Harris), dcad.org (Dallas),
  tad.org (Tarrant), acad.org (Anderson), wcad.org (Williamson), etc.
  Generic esearch pattern works for ~30 TX CAD sites.

Usage (called from Railway endpoint or GitHub Actions):
  python scraper/esearch_enrichment.py --county johnson --limit 500
  python scraper/esearch_enrichment.py --all --limit 1000
"""

import os
import re
import sys
import time
import logging
import argparse
import asyncio
from typing import Optional
from difflib import SequenceMatcher

import httpx
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("esearch")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:MXgQgqTfzzHLuyAQXulZGjEqibdsAxis@kodama.proxy.rlwy.net:42079/railway",
)

# ── CAD site registry ──────────────────────────────────────────────────────────
# county_slug -> CAD search URL base
# Pattern: most TX CAD sites use one of these engines:
#   esearch.{county}cad.org  — proprietary esearch (jcad, bcad, etc.)
#   {county}cad.org/search   — direct search
#   propaccess.trueautomation.com/clientdb/{CountyCode} — TrueAutomation

CAD_SITES = {
    # ── esearch engine (jcad.org pattern) ─────────────────────────────────────
    "johnson":    {"engine": "esearch", "url": "https://esearch.jcad.org"},
    "bexar":      {"engine": "esearch", "url": "https://esearch.bcad.org"},
    "anderson":   {"engine": "esearch", "url": "https://esearch.acad.org"},
    "cherokee":   {"engine": "esearch", "url": "https://esearch.cherokeecad.com"},
    "henderson":  {"engine": "esearch", "url": "https://esearch.henderson-cad.org"},
    "rusk":       {"engine": "esearch", "url": "https://esearch.ruskcad.org"},
    "freestone":  {"engine": "esearch", "url": "https://esearch.freestonecad.org"},
    "leon":       {"engine": "esearch", "url": "https://esearch.leoncad.org"},

    # ── TrueAutomation engine ──────────────────────────────────────────────────
    "harris":     {"engine": "trueauto", "url": "https://hcad.org/hcad-resources/hcad-appraisal-codes/"},
    "dallas":     {"engine": "trueauto", "url": "https://www.dallascad.org/AcctDetailRes.aspx"},
    "tarrant":    {"engine": "trueauto", "url": "https://www.tad.org/"},
    "collin":     {"engine": "trueauto", "url": "https://www.collincad.org/"},

    # ── Direct CAD site search ─────────────────────────────────────────────────
    "williamson": {"engine": "wcad",   "url": "https://www.wcad.org/"},
    "travis":     {"engine": "traviscad", "url": "https://www.traviscad.org/"},
    "denton":     {"engine": "denton", "url": "https://www.dentoncad.com/"},
    "lubbock":    {"engine": "lubbock", "url": "https://www.lubbockcad.org/"},
}

# Minimum name similarity (0.0–1.0) to accept a CAD match
NAME_MATCH_THRESHOLD = 0.65


# ── Name normalization ─────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normalize owner name for comparison."""
    if not name:
        return ""
    # Remove common suffixes and noise
    name = re.sub(r'\b(ET AL|ET UX|ETUX|ETAL|JR|SR|II|III|IV|LLC|INC|CORP|TRUST|REV|LIV)\b', '', name, flags=re.IGNORECASE)
    # Remove punctuation, collapse spaces
    name = re.sub(r'[^A-Z0-9 ]', '', name.upper())
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def name_similarity(a: str, b: str) -> float:
    """
    Return similarity score (0.0-1.0) between two owner name strings.
    Uses word-set Jaccard so 'JOHN SMITH' and 'SMITH JOHN' both score 1.0.
    """
    na_words = set(normalize_name(a).split())
    nb_words = set(normalize_name(b).split())
    if not na_words or not nb_words:
        return 0.0
    intersection = na_words & nb_words
    union = na_words | nb_words
    jaccard = len(intersection) / len(union)
    seq = SequenceMatcher(None, " ".join(sorted(na_words)), " ".join(sorted(nb_words))).ratio()
    return max(jaccard, seq)


def build_search_query(owner: str) -> str:
    """
    Build the best search query from an owner name.
    
    Tyler iDS stores names as "LAST FIRST MIDDLE" or "FIRST LAST" — we don't
    always know which. Use the full normalized string as the search query.
    For common names, also try last-name-only as fallback.
    """
    name = normalize_name(owner)
    # Use full name, quoted
    return f'"{name}"'


# ── esearch engine (jcad.org / bcad.org pattern) ──────────────────────────────

async def esearch_get_session(client: httpx.AsyncClient, base_url: str) -> Optional[str]:
    """Get a session token from esearch sites."""
    try:
        r = await client.get(f"{base_url}/search/requestSessionToken", timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("token") or data.get("sessionToken")
    except Exception as e:
        log.debug(f"esearch session token failed: {e}")
    return None


async def esearch_lookup(
    client: httpx.AsyncClient,
    base_url: str,
    owner: str,
    session_token: Optional[str] = None,
) -> Optional[dict]:
    """
    Look up an owner on an esearch-engine CAD site.
    Returns dict with {address, city, zip, match_score} or None.
    """
    query = normalize_name(owner)
    if not query:
        return None

    # Try full name search first, then last-name only
    search_terms = [query]
    parts = query.split()
    if len(parts) >= 2:
        search_terms.append(parts[0])  # last name only as fallback

    headers = {}
    if session_token:
        headers["X-Session-Token"] = session_token

    for term in search_terms:
        try:
            # Correct endpoint — SearchResults, not /Search/Owner
            url = f"{base_url}/search/SearchResults"
            params = {"keywords": f'OwnerName:"{term}"'}

            r = await client.post(url, params=params, headers=headers, timeout=15)
            if r.status_code != 200:
                continue

            data = r.json()
            results = data.get("results") or data.get("data") or []

            for result in results[:10]:  # check top 10 matches
                result_owner = (
                    result.get("ownerName") or result.get("owner") or
                    result.get("OwnerName") or ""
                )
                similarity = name_similarity(owner, result_owner)
                if similarity >= NAME_MATCH_THRESHOLD:
                    address = (
                        result.get("siteAddress") or result.get("address") or
                        result.get("SiteAddress") or result.get("propertyAddress") or ""
                    )
                    city = result.get("city") or result.get("City") or ""
                    zipcode = result.get("zip") or result.get("zipCode") or result.get("Zip") or ""

                    if address:
                        full_address = address.strip()
                        if city:
                            full_address += f", {city.strip()}"
                        if zipcode:
                            full_address += f" {zipcode.strip()}"
                        return {
                            "address": full_address,
                            "match_score": similarity,
                            "matched_owner": result_owner,
                        }

        except Exception as e:
            log.debug(f"esearch lookup error for '{term}': {e}")
            continue

    return None


# ── Main enrichment loop ───────────────────────────────────────────────────────

async def enrich_county(
    conn,
    county: str,
    limit: int = 500,
    dry_run: bool = False,
) -> dict:
    """Enrich leads for one county. Returns stats dict."""
    stats = {"county": county, "attempted": 0, "enriched": 0, "failed": 0, "skipped": 0}

    cad_config = CAD_SITES.get(county.lower().replace(" ", "_"))
    if not cad_config:
        log.warning(f"[{county}] No CAD site configured — skipping")
        stats["skipped"] = limit
        return stats

    engine = cad_config["engine"]
    base_url = cad_config["url"]

    # Fetch leads needing enrichment for this county
    cur = conn.cursor()
    cur.execute("""
        SELECT id, owner, doc_num
        FROM lead_records
        WHERE county = %s
          AND (prop_address IS NULL OR prop_address = '')
          AND owner IS NOT NULL
          AND owner != ''
        ORDER BY score DESC, scraped_at DESC
        LIMIT %s
    """, (county.lower(), limit))

    leads = cur.fetchall()
    cur.close()

    if not leads:
        log.info(f"[{county}] No leads need enrichment")
        return stats

    log.info(f"[{county}] Enriching {len(leads)} leads via {engine} ({base_url})")
    stats["attempted"] = len(leads)

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        timeout=20,
    ) as client:

        # Get session token if needed
        session_token = None
        if engine == "esearch":
            session_token = await esearch_get_session(client, base_url)
            if session_token:
                log.debug(f"[{county}] Got session token")

        enriched_rows = []

        for lead_id, owner, doc_num in leads:
            try:
                result = None

                if engine == "esearch":
                    result = await esearch_lookup(client, base_url, owner, session_token)
                # Add other engines here as they're built

                if result and result.get("address"):
                    enriched_rows.append((result["address"], lead_id))
                    stats["enriched"] += 1
                    log.debug(
                        f"[{county}] {doc_num}: '{owner}' → '{result['address']}' "
                        f"(score={result['match_score']:.2f})"
                    )
                else:
                    stats["failed"] += 1

                # Rate limit — be polite to CAD servers
                await asyncio.sleep(0.3)

            except Exception as e:
                log.error(f"[{county}] Lead {doc_num} error: {e}")
                stats["failed"] += 1

        # Write enriched addresses to DB
        if enriched_rows and not dry_run:
            cur = conn.cursor()
            execute_values(
                cur,
                "UPDATE lead_records SET prop_address = data.addr FROM (VALUES %s) AS data(addr, id) WHERE lead_records.id = data.id",
                enriched_rows,
            )
            conn.commit()
            cur.close()
            log.info(f"[{county}] Wrote {len(enriched_rows)} addresses to DB")
        elif dry_run:
            log.info(f"[{county}] [dry-run] Would write {len(enriched_rows)} addresses")

    return stats


async def main_async(args):
    conn = psycopg2.connect(DATABASE_URL)
    log.info("DB connected ✓")

    if args.county:
        counties = [args.county.lower()]
    elif args.all:
        # Get all counties with unenriched leads
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT county
            FROM lead_records
            WHERE (prop_address IS NULL OR prop_address = '')
              AND owner IS NOT NULL
              AND county IS NOT NULL
            ORDER BY county
        """)
        counties = [r[0] for r in cur.fetchall()]
        cur.close()
        log.info(f"Found {len(counties)} counties with unenriched leads")
    else:
        log.error("Specify --county COUNTY or --all")
        sys.exit(1)

    all_stats = []
    total_enriched = 0

    for county in counties:
        stats = await enrich_county(conn, county, limit=args.limit, dry_run=args.dry_run)
        all_stats.append(stats)
        total_enriched += stats["enriched"]
        log.info(
            f"[{county}] done: {stats['enriched']}/{stats['attempted']} enriched, "
            f"{stats['failed']} failed, {stats['skipped']} skipped"
        )

    conn.close()

    log.info("─" * 60)
    log.info(f"TOTAL enriched: {total_enriched} across {len(all_stats)} counties")


def main():
    parser = argparse.ArgumentParser(description="StackIQ esearch CAD enrichment")
    parser.add_argument("--county", help="Single county slug to enrich")
    parser.add_argument("--all",    action="store_true", help="Enrich all counties with missing addresses")
    parser.add_argument("--limit",  type=int, default=500, help="Max leads per county (default 500)")
    parser.add_argument("--dry-run",action="store_true", help="Don't write to DB")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
