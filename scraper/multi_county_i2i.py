"""
StackIQ - USLandRecords (i2i) Adapter
Covers: Angelina, Bandera, Castro, Cherokee, Cochran, Cooke,
        Duval, Edwards, Falls, Hutchinson, Leon, Madison + others
Portal: https://i2i.uslandrecords.com/TX/{County}/D/
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("i2i")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "3"))

_ALL_COUNTIES = {
    "Angelina":   "https://i2i.uslandrecords.com/TX/Angelina/D/",
    "Bandera":    "https://i2i.uslandrecords.com/TX/Bandera/D/",
    "Castro":     "https://i2i.uslandrecords.com/TX/Castro/D/",
    "Cherokee":   "https://i2i.uslandrecords.com/TX/Cherokee/D/",
    "Cochran":    "https://i2i.uslandrecords.com/TX/Cochran/D/",
    "Cooke":      "https://i2i.uslandrecords.com/TX/Cooke/D/",
    "Duval":      "https://i2i.uslandrecords.com/TX/Duval/D/",
    "Edwards":    "https://i2i.uslandrecords.com/TX/Edwards/D/",
    "Falls":      "https://i2i.uslandrecords.com/TX/Falls/D/",
    "Hutchinson": "https://i2i.uslandrecords.com/TX/Hutchinson/D/",
    "Leon":       "https://i2i.uslandrecords.com/TX/Leon/D/",
    "Madison":    "https://i2i.uslandrecords.com/TX/Madison/D/",
}
# Allow workflow to pass COUNTIES=Angelina,Cherokee,... to run a subset
_env_counties = os.getenv("COUNTIES", "")
if _env_counties.strip():
    _filter = {c.strip() for c in _env_counties.split(",")}
    COUNTIES = {k: v for k, v in _ALL_COUNTIES.items() if k in _filter}
else:
    COUNTIES = _ALL_COUNTIES

KEEP_DOC_TYPES = {
    "LIS PENDENS","LIS PEN","TAX DEED","ABSTRACT OF JUDGMENT",
    "MECHANIC LIEN","FEDERAL TAX LIEN","STATE TAX LIEN","HOA LIEN",
    "NOTICE OF FORECLOSURE","IRS LIEN","PROBATE","DIVORCE","JUDGMENT","LIEN"
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PEN" in dt:              return ("LP",      "Lis Pendens")
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
    if cat == "TAXDEED":             flags.append("Tax Deed"); s += 50
    elif cat in ("LNIRS","LNFED"):   flags.append("IRS/Fed Lien"); s += 45
    elif cat == "JUD":               flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"):  flags.append("HOA/Mech Lien"); s += 30
    elif cat == "PRO":               flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):       flags.append("Lis Pendens"); s += 20
    elif cat in ("LN","LNSTATE"):    flags.append("Lien"); s += 20
    elif cat == "DIV":               flags.append("Divorce"); s += 15
    filed_str = r.get("filed","")
    if filed_str:
        try:
            days_ago = (datetime.now() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days
            if days_ago <= 7:    flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    owner = (r.get("owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK"]):
        flags.append("LLC/Corp owner"); s += 10
    return min(s, 100), flags

async def scrape_i2i_county(page, county_name, base_url, start_dt, end_dt):
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")
    log.info(f"{county_name}: searching {start_str} to {end_str}")

    try:
        await page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        # Accept disclaimer if present
        for selector in ["button:has-text('Accept')", "input[value='Accept']", "a:has-text('I Agree')", "#btnAccept"]:
            try:
                btn = await page.query_selector(selector)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(1000)
                    break
            except: pass

        # Navigate to search
        for selector in ["a:has-text('Search')", "#lnkSearch", "a[href*='search']"]:
            try:
                btn = await page.query_selector(selector)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(1000)
                    break
            except: pass

        for page_num in range(1, MAX_PAGES + 1):
            try:
                # Fill date fields
                for date_sel in ["#dateTo", "#txtEnd", "input[name='endDate']", "input[id*='End']"]:
                    try:
                        el = await page.query_selector(date_sel)
                        if el:
                            await el.fill(end_str)
                            break
                    except: pass

                for date_sel in ["#dateFrom", "#txtStart", "input[name='startDate']", "input[id*='Start']"]:
                    try:
                        el = await page.query_selector(date_sel)
                        if el:
                            await el.fill(start_str)
                            break
                    except: pass

                # Submit
                for sel in ["input[type='submit']", "button[type='submit']", "#btnSearch"]:
                    try:
                        btn = await page.query_selector(sel)
                        if btn:
                            await btn.click()
                            await page.wait_for_timeout(3000)
                            break
                    except: pass

                # Parse results table
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(await page.content(), "lxml")
                rows = soup.select("table tr") or soup.select(".results tr")

                page_records = 0
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 3: continue
                    text = [c.get_text(" ", strip=True) for c in cells]

                    # Try to find doc_type, date, owner from cells
                    doc_type = ""
                    filed    = ""
                    owner    = ""
                    doc_num  = ""

                    for i, t in enumerate(text):
                        if any(k in t.upper() for k in ["PENDENS","LIEN","DEED","JUDGMENT","PROBATE","DIVORCE","FORECLOSURE"]):
                            doc_type = t
                        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', t):
                            filed = norm_date(t)
                        if i == 0 and re.match(r'\d+', t):
                            doc_num = t

                    if not doc_type or not should_keep(doc_type): continue
                    if not owner and len(text) > 3:
                        owner = text[2] if len(text) > 2 else ""

                    cat, cat_label = cat_from_doc_type(doc_type)
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "cat": cat, "cat_label": cat_label,
                        "filed": filed, "owner": owner, "grantee": "",
                        "amount": None, "legal": "",
                        "clerk_url": base_url,
                        "county": county_name,
                        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                        "score": 0, "flags": [],
                    })
                    page_records += 1

                log.info(f"{county_name}: page {page_num} found {page_records} records")
                if page_records == 0: break

                # Next page
                next_btn = await page.query_selector("a:has-text('Next'), .next, #btnNext")
                if not next_btn: break
                await next_btn.click()
                await page.wait_for_timeout(2000)

            except Exception as e:
                log.warning(f"{county_name}: page {page_num} error: {e}")
                break

    except Exception as e:
        log.warning(f"{county_name}: error: {e}")

    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info(f"=== i2i USLandRecords Scraper ===")
    log.info(f"Date range: {cutoff.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}")
    log.info(f"Counties: {list(COUNTIES.keys())}")

    all_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        for county_name, base_url in COUNTIES.items():
            page = await context.new_page()
            records = await scrape_i2i_county(page, county_name, base_url, cutoff, now)
            all_records.extend(records)
            await page.close()
            log.info(f"{county_name}: {len(records)} records collected")

        await browser.close()

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['doc_num']}|{r['county']}|{r['filed']}"
        if key not in seen:
            seen.add(key)
            r["score"], r["flags"] = compute_score(r)
            deduped.append(r)

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)
    log.info(f"Total unique records: {len(deduped)}")

    payload = {
        "fetched_at": now.isoformat(),
        "source": "USLandRecords i2i",
        "total": len(deduped),
        "counties": list(COUNTIES.keys()),
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/i2i_records.json","w") as f: json.dump(payload,f,indent=2,default=str)
    with open("data/i2i_records.json","w") as f: json.dump(payload,f,indent=2,default=str)
    log.info("Saved -> dashboard/i2i_records.json")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()

