"""
StackIQ — Kerr County Scraper (Fidlar AVA Web)
Portal: ava.fidlar.com/TXKerr/AvaWeb/#/search
System: Fidlar Technologies AVA Web
Free public access, no login, all results on one page
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("kerr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
BASE_URL      = "https://ava.fidlar.com/TXKerr/AvaWeb/#/search"

KEEP_DOC_TYPES = {
    "LIS PENDENS", "FORECLOSURE FILED", "LIEN AFFD", "CC DIVORCE",
    "ABSTRACT", "JUDGMENT", "MECHANIC", "FED LIEN", "FEDERAL",
    "IRS", "HOA", "STATE LIEN", "TAX DEED", "PROBATE", "NOTICE"
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except: pass
    try: return str(raw).strip()[:10]
    except: return ""

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PENDENS" in dt:                   return ("LP",      "Lis Pendens")
    if "FORECLOSURE" in dt:                   return ("NOFC",    "Notice of Foreclosure")
    if "ABSTRACT" in dt or "JUDGMENT" in dt:  return ("JUD",     "Abstract of Judgment")
    if "FED LIEN" in dt or "FEDERAL" in dt:   return ("LNFED",   "Federal Tax Lien")
    if "IRS" in dt:                            return ("LNIRS",   "IRS Lien")
    if "STATE LIEN" in dt:                     return ("LNSTATE", "State Tax Lien")
    if "HOA" in dt:                            return ("LNHOA",   "HOA Lien")
    if "MECHANIC" in dt:                       return ("LNMECH",  "Mechanic Lien")
    if "TAX DEED" in dt or "TRS DEED" in dt:  return ("TAXDEED", "Tax Deed")
    if "PROBATE" in dt:                        return ("PRO",     "Probate")
    if "DIVORCE" in dt:                        return ("DIV",     "Divorce")
    if "LIEN" in dt:                           return ("LN",      "Lien")
    if "NOTICE" in dt:                         return ("NOFC",    "Notice")
    return ("LN", doc_type)

def compute_score(r, cutoff):
    s, flags = 0, []
    cat = r.get("cat", "")
    if cat in ("TAXDEED",):        flags.append("Tax Deed"); s += 50
    elif cat in ("LNIRS","LNFED"): flags.append("IRS/Fed Lien"); s += 45
    elif cat == "JUD":             flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"): flags.append("HOA/Mech Lien"); s += 30
    elif cat == "PRO":             flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):     flags.append("Lis Pendens"); s += 20
    elif cat in ("LN","LNSTATE"):  flags.append("Lien"); s += 20
    elif cat == "DIV":             flags.append("Divorce"); s += 15
    else:                          flags.append("Distress signal"); s += 10
    filed_str = r.get("filed", "")
    if filed_str:
        try:
            days_ago = (datetime.now() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days
            if days_ago <= 7:    flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    owner = (r.get("owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK","HOLDINGS"]):
        flags.append("LLC/Corp owner"); s += 10
    if r.get("prop_address","").strip():
        flags.append("Has address"); s += 5
    return min(s, 100), flags

async def scrape_galveston(start_dt, end_dt):
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

        log.info("Galveston: loading search page...")
        await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(3000)

        # Fill start date
        try:
            start_input = await page.query_selector("input[placeholder='MM/DD/YYYY']:first-of-type")
            if not start_input:
                inputs = await page.query_selector_all("input[placeholder='MM/DD/YYYY']")
                start_input = inputs[0] if inputs else None
            if start_input:
                await start_input.click()
                await start_input.fill(start_str)
                await page.wait_for_timeout(300)
        except Exception as e:
            log.warning("Galveston: start date error: %s", e)

        # Fill end date
        try:
            inputs = await page.query_selector_all("input[placeholder='MM/DD/YYYY']")
            if len(inputs) >= 2:
                await inputs[1].click()
                await inputs[1].fill(end_str)
                await page.wait_for_timeout(300)
        except Exception as e:
            log.warning("Galveston: end date error: %s", e)

        # Click Search button
        try:
            search_btn = await page.query_selector("button:has-text('SEARCH'), button:has-text('Search')")
            if search_btn:
                await search_btn.click()
                log.info("Galveston: search submitted %s to %s", start_str, end_str)
                await page.wait_for_timeout(8000)  # Wait for SPA to load results
        except Exception as e:
            log.warning("Galveston: search button error: %s", e)

        # Parse results
        content = await page.content()
        records = parse_results(content)
        log.info("Galveston: %d total records found", len(records))

        await browser.close()
    return records

def parse_results(html):
    from bs4 import BeautifulSoup
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Find results table
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [td.get_text(" ", strip=True).lower() for td in rows[0].find_all(["th","td"])]
        if not any("document" in h for h in hdrs): continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4: continue
            try:
                doc_num   = cells[0].get_text(" ", strip=True) if len(cells) > 0 else ""
                doc_type  = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""
                recorded  = cells[2].get_text(" ", strip=True) if len(cells) > 2 else ""
                party1    = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
                party2    = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""
                legals    = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""

                if not doc_num or not doc_type: continue

                # Filter to distress doc types
                dt_upper = doc_type.upper()
                if not any(k in dt_upper for k in KEEP_DOC_TYPES):
                    continue

                cat, cat_label = cat_from_doc_type(doc_type)
                filed = norm_date(recorded)

                # Extract subdivision/address from legals
                prop_addr = ""
                if legals:
                    sub_match = re.search(r'Sub:\s*(.+)', legals)
                    if sub_match:
                        prop_addr = sub_match.group(1).strip()

                records.append({
                    "doc_num": doc_num,
                    "doc_type": doc_type,
                    "cat": cat, "cat_label": cat_label,
                    "filed": filed,
                    "owner": party1,
                    "grantee": party2,
                    "amount": None,
                    "legal": legals,
                    "clerk_url": f"https://ava.fidlar.com/TXKerr/AvaWeb/#/docdetail/{doc_num}",
                    "county": "Kerr",
                    "prop_address": prop_addr,
                    "prop_city": "",
                    "prop_state": "TX",
                    "prop_zip": "",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                    "score": 0, "flags": [],
                })
            except: continue
        if records: break

    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Kerr County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = await scrape_galveston(cutoff, now)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['doc_num']}|{r['filed']}"
        if key not in seen:
            seen.add(key); deduped.append(r)

    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)
    log.info("Total unique: %d", len(deduped))

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Kerr County Clerk (Fidlar AVA Web)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": ["Kerr"],
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/kerr_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/kerr_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/kerr_records.json")

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
