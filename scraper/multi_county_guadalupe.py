"""
StackIQ — Guadalupe County Scraper (Tyler iDS)
Portal: guadalupecountytx-web.tylerhost.net
Optimized: single date-range search, client-side doc type filtering
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("guadalupe")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "3"))
BASE_URL      = "https://guadalupecountytx-web.tylerhost.net/web"

KEEP_DOC_TYPES = {
    "LIS PENDENS","TAX DEED","ABSTRACT OF JUDGMENT","MECHANIC LIEN",
    "FEDERAL TAX LIEN","STATE TAX LIEN","HOA LIEN","NOTICE OF FORECLOSURE",
    "IRS TAX LIEN","PROBATE","DIVORCE","FORECLOSURE","JUDGMENT","LIEN"
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PENDENS" in dt:          return ("LP",      "Lis Pendens")
    if "TAX DEED" in dt:             return ("TAXDEED", "Tax Deed")
    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD","Abstract of Judgment")
    if "MECHANIC" in dt:             return ("LNMECH",  "Mechanic Lien")
    if "FEDERAL" in dt:              return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in dt:            return ("LNSTATE", "State Tax Lien")
    if "HOA" in dt:                  return ("LNHOA",   "HOA Lien")
    if "FORECLOSURE" in dt:          return ("NOFC",    "Notice of Foreclosure")
    if "IRS" in dt:                  return ("LNIRS",   "IRS Lien")
    if "PROBATE" in dt:              return ("PRO",     "Probate")
    if "DIVORCE" in dt:              return ("DIV",     "Divorce")
    if "LIEN" in dt:                 return ("LN",      "Lien")
    return ("LN", doc_type)

def should_keep(doc_type):
    dt = doc_type.upper()
    return any(k in dt for k in KEEP_DOC_TYPES)

def compute_score(r):
    s, flags = 0, []
    cat = r.get("cat","")
    if cat == "TAXDEED":              flags.append("Tax Deed"); s += 50
    elif cat in ("LNIRS","LNFED"):   flags.append("IRS/Fed Lien"); s += 45
    elif cat == "JUD":               flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"):  flags.append("HOA/Mech Lien"); s += 30
    elif cat == "PRO":               flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):       flags.append("Lis Pendens"); s += 20
    elif cat in ("LN","LNSTATE"):    flags.append("Lien"); s += 20
    elif cat == "DIV":               flags.append("Divorce"); s += 15
    else:                            flags.append("Distress signal"); s += 10
    filed_str = r.get("filed","")
    if filed_str:
        try:
            days_ago = (datetime.now() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days
            if days_ago <= 7:    flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    owner = (r.get("owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK","HOLDINGS"]):
        flags.append("LLC/Corp owner"); s += 10
    return min(s, 100), flags

async def scrape_guadalupe(start_dt, end_dt):
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        # Accept disclaimer once
        try:
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(1000)
            accept_btn = await page.query_selector("button:has-text('I Accept'), input[value='I Accept'], a:has-text('I Accept')")
            if accept_btn:
                await accept_btn.click()
                await page.wait_for_timeout(1000)
        except Exception as e:
            log.warning("Guadalupe: disclaimer error: %s", e)

        # Single search by date range only - no doc type filter
        for page_num in range(1, MAX_PAGES + 1):
            log.info("Guadalupe: page %d of %d (%s to %s)", page_num, MAX_PAGES, start_str, end_str)
            try:
                await page.goto(f"{BASE_URL}/search/official-records-search", wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(1000)

                # Fill date range
                inputs = await page.query_selector_all("input[placeholder='mm/dd/yyyy']")
                if len(inputs) >= 2:
                    await inputs[0].fill(start_str)
                    await page.wait_for_timeout(200)
                    await inputs[1].fill(end_str)
                    await page.wait_for_timeout(200)

                # Click search
                search_btn = await page.query_selector("button:has-text('Search'), input[value='Search']")
                if search_btn:
                    await search_btn.click()
                    await page.wait_for_timeout(2000)

                # Parse results
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(await page.content(), "lxml")
                rows = soup.select("table tbody tr, .search-result-row, tr.result-row")
                if not rows:
                    rows = soup.find_all("tr")

                page_records = 0
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 3: continue
                    doc_type = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""
                    if not doc_type or not should_keep(doc_type): continue
                    doc_num  = cells[0].get_text(" ", strip=True)
                    filed    = norm_date(cells[2].get_text(" ", strip=True) if len(cells) > 2 else "")
                    owner    = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
                    cat, cat_label = cat_from_doc_type(doc_type)
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "cat": cat, "cat_label": cat_label,
                        "filed": filed, "owner": owner, "grantee": "",
                        "amount": None, "legal": "",
                        "clerk_url": f"{BASE_URL}/search/official-records-search",
                        "county": "Guadalupe",
                        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                        "score": 0, "flags": [],
                    })
                    page_records += 1

                log.info("Guadalupe: page %d found %d distress records", page_num, page_records)
                if page_records == 0: break

                # Try next page
                next_btn = await page.query_selector("a:has-text('Next'), button:has-text('Next'), .next-page")
                if not next_btn: break
                await next_btn.click()
                await page.wait_for_timeout(2000)

            except Exception as e:
                log.warning("Guadalupe: page %d error: %s", page_num, e)
                break

        await browser.close()
    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Guadalupe County Scraper (optimized) ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = await scrape_guadalupe(cutoff, now)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['doc_num']}|{r['filed']}"
        if key not in seen:
            seen.add(key)
            r["score"], r["flags"] = compute_score(r)
            deduped.append(r)

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)
    log.info("Total unique: %d", len(deduped))

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Guadalupe County Clerk (Tyler iDS)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": ["Guadalupe"],
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/guadalupe_records.json","w") as f: json.dump(payload,f,indent=2,default=str)
    with open("data/guadalupe_records.json","w") as f: json.dump(payload,f,indent=2,default=str)
    log.info("Saved -> dashboard/guadalupe_records.json")

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
