"""
StackIQ — Texas Land Records Scraper (Avenu/texaslandrecords.com)
Covers: Angelina, Cherokee, Cooke, Fannin, Rusk, San Jacinto, San Augustine,
        Scurry, Upton, Val Verde, Wilbarger, Live Oak
Free index search, no login needed
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("txlr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
BASE_URL = "https://www.texaslandrecords.com/txlr/TxlrApp/index.jsp"

# Counties to scrape - map county name to county code used by the system
COUNTIES = {
    "Angelina":    "angelina",
    "Cherokee":    "cherokee",
    "Cooke":       "cooke",
    "Fannin":      "fannin",
    "Rusk":        "rusk",
    "SanJacinto":  "sanjacinto",
    "SanAugustine":"sanaugustine",
    "Scurry":      "scurry",
    "Upton":       "upton",
    "ValVerde":    "valverde",
    "Wilbarger":   "wilbarger",
    "LiveOak":     "liveoak",
}

KEEP_DOC_TYPES = {
    "LIS PENDENS","FORECLOSURE","LIEN","ABSTRACT","JUDGMENT","MECHANIC",
    "FED TAX","IRS","HOA","STATE TAX","TAX DEED","PROBATE","NOTICE","DIVORCE",
    "RELEASE","ASSIGNMENT"
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PENDENS" in dt:  return ("LP", "Lis Pendens")
    if "FORECLOSURE" in dt:  return ("NOFC", "Notice of Foreclosure")
    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD", "Abstract of Judgment")
    if "FED" in dt or "IRS" in dt: return ("LNFED", "Federal Tax Lien")
    if "HOA" in dt:          return ("LNHOA", "HOA Lien")
    if "MECHANIC" in dt:     return ("LNMECH", "Mechanic Lien")
    if "TAX DEED" in dt:     return ("TAXDEED", "Tax Deed")
    if "PROBATE" in dt:      return ("PRO", "Probate")
    if "DIVORCE" in dt:      return ("DIV", "Divorce")
    if "LIEN" in dt:         return ("LN", "Lien")
    if "NOTICE" in dt:       return ("NOFC", "Notice")
    return ("LN", doc_type)

def compute_score(r):
    s, flags = 0, []
    cat = r.get("cat","")
    if cat == "TAXDEED":         flags.append("Tax Deed"); s += 50
    elif cat in ("LNFED",):      flags.append("Fed Lien"); s += 45
    elif cat == "JUD":           flags.append("Judgment"); s += 35
    elif cat in ("LNHOA","LNMECH"): flags.append("HOA/Mech"); s += 30
    elif cat == "PRO":           flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):   flags.append("Lis Pendens"); s += 20
    elif cat in ("LN",):         flags.append("Lien"); s += 20
    elif cat == "DIV":           flags.append("Divorce"); s += 15
    else:                        flags.append("Distress"); s += 10
    return min(s, 100), flags

async def scrape_county(page, county_name, county_code, start_dt, end_dt):
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str = end_dt.strftime("%m/%d/%Y")
    log.info("TxLR: scraping %s %s to %s", county_name, start_str, end_str)

    try:
        # Navigate to county search page
        url = f"https://www.texaslandrecords.com/txlr/TxlrApp/index.jsp?county={county_code}"
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(3000)

        # Try to find date range search fields
        # Fill start date
        start_inputs = await page.query_selector_all("input[name*='start'], input[name*='from'], input[placeholder*='Start'], input[id*='startDate'], input[id*='fromDate']")
        if start_inputs:
            await start_inputs[0].fill(start_str)

        end_inputs = await page.query_selector_all("input[name*='end'], input[name*='to'], input[placeholder*='End'], input[id*='endDate'], input[id*='toDate']")
        if end_inputs:
            await end_inputs[0].fill(end_str)

        # Click search
        search_btn = await page.query_selector("input[type='submit'], button[type='submit'], button:has-text('Search')")
        if search_btn:
            await search_btn.click()
            await page.wait_for_timeout(5000)

        # Parse results
        content = await page.content()
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', content, re.S | re.I)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S | re.I)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if len(cells) < 3: continue
            doc_type = cells[1] if len(cells) > 1 else ""
            if not any(k in doc_type.upper() for k in KEEP_DOC_TYPES):
                continue
            doc_num = cells[0] if cells else ""
            filed = norm_date(cells[2] if len(cells) > 2 else "")
            owner = cells[3] if len(cells) > 3 else ""
            cat, cat_label = cat_from_doc_type(doc_type)
            records.append({
                "doc_num": doc_num, "doc_type": doc_type,
                "cat": cat, "cat_label": cat_label,
                "filed": filed, "owner": owner, "grantee": "",
                "amount": None, "legal": "",
                "clerk_url": url,
                "county": county_name,
                "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",
                "score": 0, "flags": [],
            })
    except Exception as e:
        log.warning("TxLR %s error: %s", county_name, e)

    log.info("TxLR %s: %d records", county_name, len(records))
    return records

async def main_async():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Texas Land Records Scraper ===")
    log.info("Counties: %s", list(COUNTIES.keys()))

    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        for county_name, county_code in COUNTIES.items():
            recs = await scrape_county(page, county_name, county_code, cutoff, now)
            all_records.extend(recs)

        await browser.close()

    # Dedupe and score
    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['county']}|{r['doc_num']}|{r['filed']}"
        if key not in seen:
            seen.add(key)
            r["score"], r["flags"] = compute_score(r)
            deduped.append(r)

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)
    log.info("Total: %d unique records across %d counties", len(deduped), len(COUNTIES))

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Texas Land Records (texaslandrecords.com)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": list(COUNTIES.keys()),
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/txlr_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/txlr_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/txlr_records.json")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
