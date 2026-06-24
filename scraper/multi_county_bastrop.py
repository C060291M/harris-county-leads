import asyncio, os, re, json, logging
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bastrop")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
COUNTY        = "bastrop"
BASE_URL      = "https://cc.co.bastrop.tx.us"
SEARCH_URL    = f"{BASE_URL}/RealEstate/SearchEntry.aspx"

# Doc types from the search form - same Aumentum platform as Travis/Fort Bend
DISTRESS_DOC_TYPES = {
    "ABSTRACT OF JUDGMENT",
    "LIS PENDENS",
    "FEDERAL TAX LIEN",
    "STATE TAX LIEN",
    "MECHANIC LIEN",
    "JUDGMENT",
    "PROBATE",
    "DIVORCE",
    "FORECLOSURE",
    "LIEN",
    "HOA LIEN",
    "HOSPITAL LIEN",
}

def cat_from_doc_type(dt):
    dt = dt.upper()
    if "LIS PENDENS" in dt:      return ("LP",      "Lis Pendens")
    if "FEDERAL TAX" in dt:      return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in dt:        return ("LNSTATE", "State Tax Lien")
    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD", "Abstract of Judgment")
    if "PROBATE" in dt:          return ("PRO",     "Probate")
    if "DIVORCE" in dt:          return ("DIV",     "Divorce")
    if "MECHANIC" in dt:         return ("LNMECH",  "Mechanic Lien")
    if "FORECLOSURE" in dt:      return ("NOFC",    "Notice of Foreclosure")
    if "LIEN" in dt:             return ("LN",      "Lien")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def parse_results(soup):
    """Parse Bastrop Aumentum results - same structure as Travis"""
    import re
    records = []
    tables = soup.find_all("table")
    
    data_table = None
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) > 5:
            for row in rows[:3]:
                cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
                if cells and re.match(r"^\d+$", cells[0].strip()) and len(cells) > 8:
                    data_table = t
                    break
            if data_table: break
    
    if not data_table:
        return records
    
    rows = data_table.find_all("tr")
    for row in rows:
        cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        if not cells or not re.match(r"^\d+$", cells[0].strip()): continue
        if len(cells) < 20: continue
        
        doc_num  = cells[3].strip()  if len(cells) > 3  else ""
        filed    = norm_date(cells[8])  if len(cells) > 8  else ""
        doc_type = cells[9].strip()  if len(cells) > 9  else ""
        grantor  = re.sub(r"\[.\]|\(\+\)", "", cells[14]).strip() if len(cells) > 14 else ""
        grantee  = re.sub(r"\[.\]|\(\+\)", "", cells[18]).strip() if len(cells) > 18 else ""
        
        if not doc_num: continue
        dt_upper = doc_type.upper()
        if not any(k in dt_upper for k in DISTRESS_DOC_TYPES): continue
        
        cat, cat_label = cat_from_doc_type(doc_type)
        records.append({
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": cat_label,
            "filed": filed, "owner": grantor, "grantee": grantee,
            "amount": None, "county": COUNTY,
            "clerk_url": SEARCH_URL,
            "prop_address": "", "score": 0, "flags": [],
        })
    return records

async def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    log.info("[Bastrop] Scraping %s to %s", start_str, end_str)
    records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900}
        )
        
        # Accept disclaimer
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        link = await page.query_selector("a:has-text('Click here'), a:has-text('acknowledge')")
        if link:
            await link.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)
        
        # Go to Real Estate search
        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        
        # Fill dates - Aumentum uses dropdown date pickers
        start_inp = await page.query_selector("input[id*='DateFrom'], input[id*='dateFrom']")
        end_inp   = await page.query_selector("input[id*='DateTo'], input[id*='dateTo']")
        
        if not start_inp:
            # Try the calendar inputs
            date_inputs = await page.query_selector_all("input[placeholder='mm/dd/yyyy']")
            if len(date_inputs) >= 2:
                start_inp = date_inputs[0]
                end_inp   = date_inputs[1]
        
        if start_inp and end_inp:
            await start_inp.click(click_count=3)
        # Infragistics date picker - same as Travis/Fort Bend
        start_inp = await page.query_selector("#cphNoMargin_f_ddcDateFiledFrom input[type='text']")
        end_inp   = await page.query_selector("#cphNoMargin_f_ddcDateFiledTo input[type='text']")
        if start_inp and end_inp:
            await start_inp.click(click_count=3)
            await start_inp.type(start_str)
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(300)
            await end_inp.click(click_count=3)
            await end_inp.type(end_str)
            await page.keyboard.press("Tab")
            await page.wait_for_timeout(300)
            log.info("[Bastrop] Dates set: %s to %s", start_str, end_str)
        else:
            log.error("[Bastrop] Date inputs not found")
            await browser.close()
            return records
        
        # Click Search
        search_btn = await page.query_selector("input[value='Search'], button:has-text('Search')")
        if search_btn:
            await search_btn.evaluate("el => el.click()")
            await page.wait_for_timeout(5000)
        
        # Paginate
        for page_num in range(1, MAX_PAGES + 1):
            soup = BeautifulSoup(await page.content(), "lxml")
            page_recs = parse_results(soup)
            log.info("[Bastrop] Page %d: %d records", page_num, len(page_recs))
            records.extend(page_recs)
            if not page_recs: break
            
            next_btn = await page.query_selector("a:has-text('Next')")
            if not next_btn: break
            await next_btn.evaluate("el => el.click()")
            await page.wait_for_timeout(3000)
        
        await browser.close()

    seen, deduped = set(), []
    for r in records:
        k = r.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(r)

    log.info("[Bastrop] %d unique records", len(deduped))
    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "bastrop_records.json"), "w", encoding="utf-8") as f:
        json.dump({"fetched_at": datetime.now().isoformat(), "total": len(deduped),
                   "counties": [COUNTY], "records": deduped}, f, indent=2, default=str)
    return deduped

if __name__ == "__main__":
    asyncio.run(scrape())
