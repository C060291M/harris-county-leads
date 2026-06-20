"""
StackIQ — Universal Tyler iDS County Scraper
Replaces all 62 individual multi_county_*.py Tyler scrapers.

Usage (via GitHub Actions env vars):
  COUNTY=Rockwall          — county name (required)
  LOOKBACK_DAYS=3          — days to look back (default 3)
  MAX_PAGES=5              — max result pages (default 5)
  MAX_RECORDS=500          — stop after N records (default 500)
  WALL_MINUTES=75          — hard time limit in minutes (default 75)

All 59 Tyler county configs are embedded — no separate config file needed.
"""

import json, logging, re, os, asyncio, time
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("tyler")

# ── Runtime config ─────────────────────────────────────────────────────────────
COUNTY        = os.getenv("COUNTY", "").strip()
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "500"))
WALL_MINUTES  = int(os.getenv("WALL_MINUTES", "75"))

# ── County registry ────────────────────────────────────────────────────────────
# Format: "County": ("BASE_URL", "DOCSEARCH_ID")
COUNTY_REGISTRY = {
    "Andrews":     ("https://andrewscountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Aransas":     ("https://aransascountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Bastrop":     ("https://bastroptx-web.tylerhost.net/web",                    "DOCSEARCH144S1"),
    "Bowie":       ("https://bowiecountytx-web.tylerhost.net/web",                "DOCSEARCH149S1"),
    "Brazoria":    ("https://brazoriacountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Burnet":      ("https://burnetcountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Calhoun":     ("https://calhouncountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Calhoun2":    ("https://calhouncountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Carson":      ("https://carsoncountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Colorado":    ("https://coloradocountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Comal":       ("https://comalcountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Dallam":      ("https://dallamcountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Delta":       ("https://deltacountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Donley":      ("https://donleycountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Eastland":    ("https://eastlandcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Ector":       ("https://ectorcountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Fort Bend":   ("https://fortbendcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Gonzales":    ("https://gonzalescountytx-web.tylerhost.net/web",             "DOCSEARCH782S3"),
    "Gregg":       ("https://greggcountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Guadalupe":   ("https://guadalupecountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Hamilton":    ("https://hamiltoncountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Hardin":      ("https://hardincountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Harrison":    ("https://harrisoncountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Hays":        ("https://hayscountytx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Henderson":   ("https://hendersoncountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Hill":        ("https://hillcountytx-web.tylerhost.net/web",                 "DOCSEARCH100427S1"),
    "Hood":        ("https://hoodcountytx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Howard":      ("https://howardcountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Hunt":        ("https://huntcountytx-web.tylerhost.net/web",                 "DOCSEARCH149S1"),
    "Jasper":      ("https://jaspercountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Karnes":      ("https://karnescountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Kaufman":     ("https://kaufmancountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Kimble":      ("https://kimblecountytx-web.tylerhost.net/web",               "DOCSEARCH419S1"),
    "Lamar":       ("https://lamarcountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Lavaca":      ("https://lavacacountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Liberty":     ("https://libertycountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "McLennan":    ("https://mclennancountytx-web.tylerhost.net/web",             "DOCSEARCH402S1"),
    "Mills":       ("https://millscountytx-web.tylerhost.net/web",                "DOCSEARCH419S1"),
    "Montgomery":  ("https://montgomerycountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Navarro":     ("https://navarrocountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Orange":      ("https://orangecountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "PaloPinto":   ("https://palopintocountytx-selfservice.tylerhost.net/web",    "DOCSEARCH144S1"),
    "Panola":      ("https://panolacountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Pecos":       ("https://pecoscountytx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Polk":        ("https://polkcountytx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Potter":      ("https://pottercountytx-web.tylerhost.net/web",               "DOCSEARCH422S2"),
    "Randall":     ("https://randallcountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Rockwall":    ("https://rockwalltx-web.tylerhost.net/web",                   "DOCSEARCH144S1"),
    "Scurry":      ("https://scurrycountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Somervell":   ("https://somervellcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Taylor":      ("https://taylorcountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Upshur":      ("https://upshurcountytx-web.tylerhost.net/web",               "DOCSEARCH149S1"),
    "VanZandt":    ("https://vanzandtcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Waller":      ("https://wallercountytx-web.tylerhost.net/web",               "DOCSEARCH144S1"),
    "Washington":  ("https://washingtoncountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Wichita":     ("https://wichitacountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Williamson":  ("https://williamsoncountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Winkler":     ("https://winklercountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Wise":        ("https://wisecountytx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Wood":        ("https://woodcountytx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Yoakum":      ("https://yoakumcountytx-selfservice.tylerhost.net/web",       "DOCSEARCH144S1"),
}

KEEP_DOC_TYPES = {
    "LIS PENDENS", "TAX DEED", "ABSTRACT OF JUDGMENT", "MECHANIC LIEN",
    "FEDERAL TAX LIEN", "STATE TAX LIEN", "HOA LIEN", "NOTICE OF FORECLOSURE",
    "IRS TAX LIEN", "PROBATE", "DIVORCE", "FORECLOSURE", "JUDGMENT", "LIEN",
    "NOTICE OF TRUSTEE SALE", "HOSPITAL LIEN", "MECHANIC",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]


def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PENDENS" in dt:                    return ("LP",      "Lis Pendens")
    if "TAX DEED" in dt:                       return ("TAXDEED", "Tax Deed")
    if "ABSTRACT" in dt or "JUDGMENT" in dt:   return ("JUD",     "Abstract of Judgment")
    if "MECHANIC" in dt:                       return ("LNMECH",  "Mechanic Lien")
    if "FEDERAL" in dt or "IRS" in dt:         return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in dt:                      return ("LNSTATE", "State Tax Lien")
    if "HOA" in dt:                            return ("LNHOA",   "HOA Lien")
    if "FORECLOSURE" in dt or "TRUSTEE" in dt: return ("NOFC",    "Notice of Foreclosure")
    if "PROBATE" in dt:                        return ("PRO",     "Probate")
    if "DIVORCE" in dt:                        return ("DIV",     "Divorce")
    if "LIEN" in dt:                           return ("LN",      "Lien")
    return ("LN", doc_type)


def parse_items(soup, county_name, base_url, docsearch):
    """Parse Tyler iDS card layout items from BeautifulSoup."""
    from bs4 import BeautifulSoup
    records = []
    items = soup.find_all("li", attrs={"data-documentid": True})

    for item in items:
        h1 = item.find("h1")
        if not h1: continue
        h1_text  = h1.get_text(" ", strip=True)
        h1_clean = " ".join(h1_text.split())

        if not any(k in h1_clean.upper() for k in KEEP_DOC_TYPES):
            continue

        parts   = re.split(r"[\u2022\xa0\s]{2,}", h1_clean)
        parts   = [p.strip() for p in parts if p.strip()]
        doc_num = parts[0] if parts else ""
        # Clean doc_num — strip everything after bullet
        doc_num = re.split(r"[\u2022\xa0]", doc_num)[0].strip()
        doc_num = re.sub(r"\s+", " ", doc_num).strip()

        doc_type = parts[-1] if len(parts) > 1 else h1_clean
        # If doc_type looks like a doc number, use the raw h1
        if re.match(r"^\d{4}-\d+", doc_type):
            doc_type = h1_clean

        full_text = item.get_text(" ", strip=True)
        date_m    = re.search(r"(\d{2}/\d{2}/\d{4})", full_text)
        filed     = norm_date(date_m.group(1)) if date_m else ""
        grantor_m = re.search(r"Grantor\s+([A-Z][^\n]+?)(?:\s{2,}|Grantee|Recording)", full_text)
        owner     = grantor_m.group(1).strip() if grantor_m else ""
        amount_m  = re.search(r"\$[\d,]+\.?\d*", full_text)
        amount    = None
        if amount_m:
            try: amount = float(amount_m.group(0).replace("$","").replace(",",""))
            except: pass

        cat, cat_label = cat_from_doc_type(doc_type)

        records.append({
            "doc_num":    doc_num,
            "doc_type":   doc_type,
            "cat":        cat,
            "cat_label":  cat_label,
            "filed":      filed,
            "owner":      owner,
            "grantee":    "",
            "amount":     amount,
            "clerk_url":  f"{base_url}/search/{docsearch}",
            "county":     county_name,
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
            "score": 0, "flags": [],
        })

    return records


# ── Main scraper ───────────────────────────────────────────────────────────────

async def scrape_county(county_name: str, base_url: str, docsearch: str,
                        start_dt: datetime, end_dt: datetime) -> list:
    from bs4 import BeautifulSoup

    records  = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")
    wall_start = time.monotonic()
    wall_limit = WALL_MINUTES * 60

    log.info("[%s] Scraping %s to %s | max_pages=%d max_records=%d wall=%dmin",
             county_name, start_str, end_str, MAX_PAGES, MAX_RECORDS, WALL_MINUTES)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        # ── Accept disclaimer ──────────────────────────────────────────────────
        try:
            await page.goto(f"{base_url}/user/disclaimer",
                            wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    b.removeAttribute('disabled');
                    if (b.textContent.match(/accept|agree|continue/i)) { b.click(); return; }
                }
                if (btns[0]) btns[0].click();
            }""")
            await page.wait_for_timeout(1500)
        except Exception as e:
            log.warning("[%s] Disclaimer: %s", county_name, e)

        # ── Initial search — do this ONCE, then paginate ───────────────────────
        search_url = f"{base_url}/search/{docsearch}"
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)

            # Fill date fields
            await page.fill("input[name='field_RecordingDateID_DOT_StartDate']", start_str)
            await page.fill("input[name='field_RecordingDateID_DOT_EndDate']",   end_str)
            await page.wait_for_timeout(500)

            # Click search
            search_btn = (
                await page.query_selector("a[href*='searchResults']") or
                await page.query_selector("button[type='submit']") or
                await page.query_selector("input[type='submit']")
            )
            if search_btn:
                await search_btn.click()
                await page.wait_for_timeout(2500)
            else:
                log.warning("[%s] No search button found", county_name)
                await browser.close()
                return records

        except Exception as e:
            log.error("[%s] Search setup failed: %s", county_name, e)
            await browser.close()
            return records

        # ── Paginate through results ───────────────────────────────────────────
        for page_num in range(1, MAX_PAGES + 1):

            # Wall-clock guard
            elapsed = time.monotonic() - wall_start
            remaining = wall_limit - elapsed
            if remaining < 120:  # less than 2 min left — save and exit
                log.warning("[%s] Wall clock: %.0fs elapsed, stopping at page %d with %d records",
                            county_name, elapsed, page_num, len(records))
                break

            if len(records) >= MAX_RECORDS:
                log.info("[%s] Hit MAX_RECORDS=%d, stopping", county_name, MAX_RECORDS)
                break

            log.info("[%s] Page %d/%d (%.0fs elapsed, %d records so far)",
                     county_name, page_num, MAX_PAGES, elapsed, len(records))

            try:
                soup = BeautifulSoup(await page.content(), "lxml")
                page_records = parse_items(soup, county_name, base_url, docsearch)
                new_count = len(page_records)
                records.extend(page_records)
                log.info("[%s] Page %d: %d distress records", county_name, page_num, new_count)

                if new_count == 0:
                    log.info("[%s] Empty page, stopping pagination", county_name)
                    break

                # Try to go to next page — only click, don't reload
                next_btn = (
                    await page.query_selector("a:has-text('Next')") or
                    await page.query_selector("button:has-text('Next')") or
                    await page.query_selector("[aria-label='Next page']") or
                    await page.query_selector(".pagination-next") or
                    await page.query_selector("li.next a")
                )
                if not next_btn:
                    log.info("[%s] No next page button, done", county_name)
                    break

                await next_btn.click()
                await page.wait_for_timeout(2000)

            except Exception as e:
                log.warning("[%s] Page %d error: %s", county_name, page_num, e)
                break

        await browser.close()

    log.info("[%s] Done: %d total records in %.0fs",
             county_name, len(records), time.monotonic() - wall_start)
    return records


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    if not COUNTY:
        log.error("COUNTY env var not set. Example: COUNTY=Rockwall")
        raise SystemExit(1)

    config = COUNTY_REGISTRY.get(COUNTY)
    if not config:
        # Case-insensitive fallback
        match = next((v for k, v in COUNTY_REGISTRY.items()
                      if k.lower() == COUNTY.lower()), None)
        config = match
    if not config:
        log.error("County '%s' not found in registry. Available: %s",
                  COUNTY, sorted(COUNTY_REGISTRY.keys()))
        raise SystemExit(1)

    base_url, docsearch = config
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    records = await scrape_county(COUNTY, base_url, docsearch, cutoff, now)

    # Deduplicate by doc_num
    seen, deduped = set(), []
    for r in records:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)

    log.info("[%s] %d unique records after dedup (from %d raw)",
             COUNTY, len(deduped), len(records))

    # Write output
    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{COUNTY.lower().replace(' ','_')}_records.json")

    payload = {
        "fetched_at":  datetime.now().isoformat(),
        "source":      f"{COUNTY} County Clerk (Tyler iDS)",
        "date_range":  {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total":       len(deduped),
        "counties":    [COUNTY],
        "records":     deduped,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("[%s] Written to %s", COUNTY, out_file)
    return len(deduped)


if __name__ == "__main__":
    result = asyncio.run(main())
    raise SystemExit(0 if result >= 0 else 1)
