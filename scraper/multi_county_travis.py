import asyncio, os, re, json, logging
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("travis")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
FLOOR_DATE = "2025-01-01"  # Never pull records older than this
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
COUNTY        = "travis"

DISTRESS_DOC_TYPES = {
    1:"AJ", 23:"DIVOR", 24:"JUDGMT", 25:"PROB", 51:"FED TAX",
    60:"JDGMT", 62:"LIEN", 63:"LIS PEND", 65:"ML",
    72:"FORECLOSURE", 107:"ST TAX LIEN", 56:"HOSP LIEN",
}

def cat_from_doc_type(dt):
    dt = dt.upper()
    if "LIS PEND" in dt:    return ("LP",      "Lis Pendens")
    if "FED TAX" in dt:     return ("LNFED",   "Federal Tax Lien")
    if "ST TAX" in dt:      return ("LNSTATE", "State Tax Lien")
    if "JUDGMT" in dt or "JDGMT" in dt or dt=="AJ": return ("JUD","Abstract of Judgment")
    if "PROB" in dt:        return ("PRO",     "Probate")
    if "DIVOR" in dt:       return ("DIV",     "Divorce")
    if dt == "ML":          return ("LNMECH",  "Mechanic Lien")
    if "FORECLOSURE" in dt: return ("NOFC",    "Notice of Foreclosure")
    if "LIEN" in dt:        return ("LN",      "Lien")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def parse_name(raw):
    """Extract clean name from '[R] SMITH JOHN (+)' format"""
    if not raw: return ""
    raw = re.sub(r'\[.\]', '', raw).strip()
    raw = re.sub(r'\(\+\)', '', raw).strip()
    return re.sub(r'\s+', ' ', raw).strip()

def parse_table(soup):
    """Find and parse the results table"""
    records = []
    tables = soup.find_all("table")
    
    data_table = None
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) > 5:
            # Check if first data row has instrument number pattern
            for row in rows[:3]:
                cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
                if cells and re.match(r'^\d+$', cells[0].strip()) and len(cells) > 8:
                    data_table = t
                    break
            if data_table: break
    
    if not data_table:
        return records
    
    rows = data_table.find_all("tr")
    for row in rows:
        cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        if not cells or not re.match(r'^\d+$', cells[0].strip()): continue
        if len(cells) < 20: continue
        
        doc_num   = cells[3].strip()  if len(cells) > 3  else ""
        filed     = norm_date(cells[8]) if len(cells) > 8  else ""
        doc_type  = cells[9].strip()  if len(cells) > 9  else ""
        name_raw  = cells[11].strip() if len(cells) > 11 else ""
        grantor   = parse_name(cells[14]) if len(cells) > 14 else parse_name(name_raw)
        grantee   = parse_name(cells[18]) if len(cells) > 18 else ""
        legal     = cells[20].strip() if len(cells) > 20 else ""
        
        if not doc_num: continue
        cat, cat_label = cat_from_doc_type(doc_type)
        records.append({
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": cat_label,
            "filed": filed, "owner": grantor, "grantee": grantee,
            "amount": None, "county": COUNTY,
            "legal": legal,
            "clerk_url": "https://www.tccsearch.org/RealEstate/SearchEntry.aspx",
            "prop_address":"","score":0,"flags":[],
        })
    return records

async def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str   = now.strftime("%m/%d/%Y")
    log.info("[Travis] Scraping %s to %s", start_str, end_str)
    records = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900}
        )
        
        await page.goto("https://www.tccsearch.org", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        link = await page.query_selector("a:has-text('Click here')")
        if link:
            await link.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)
        
        await page.goto("https://www.tccsearch.org/RealEstate/SearchEntry.aspx",
                       wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        
        # Set dates - try multiple selectors for Infragistics date picker
        from_inp = (
            await page.query_selector("#cphNoMargin_f_ddcDateFiledFrom input[type='text']") or
            await page.query_selector("input[id*='DateFiledFrom']") or
            await page.query_selector("input[id*='DateFrom']")
        )
        to_inp = (
            await page.query_selector("#cphNoMargin_f_ddcDateFiledTo input[type='text']") or
            await page.query_selector("input[id*='DateFiledTo']") or
            await page.query_selector("input[id*='DateTo']")
        )
        if not from_inp or not to_inp:
            import logging
            logging.getLogger("travis").error("Date inputs not found on Travis portal")
            await browser.close()
            return []
        await from_inp.click(click_count=3)
        await from_inp.type(start_str)
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(500)
        await to_inp.click(click_count=3)
        await to_inp.type(end_str)
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(500)
        
        # Check distress doc types
        for idx in DISTRESS_DOC_TYPES:
            cb = await page.query_selector(f"#cphNoMargin_f_dclDocType_{idx}")
            if cb: await cb.check()
        
        await page.click("input[id='cphNoMargin_SearchButtons1_btnSearch']")
        await page.wait_for_timeout(6000)
        log.info("[Travis] Search submitted, URL: %s", page.url)
        
        for page_num in range(1, MAX_PAGES + 1):
            soup = BeautifulSoup(await page.content(), "lxml")
            page_recs = parse_table(soup)
            log.info("[Travis] Page %d: %d records", page_num, len(page_recs))
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
    
    log.info("[Travis] %d unique records", len(deduped))
    
    out_dir  = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "travis_records.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "fetched_at": datetime.now().isoformat(),
            "source": "Travis County Clerk (tccsearch.org)",
            "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
            "total": len(deduped), "counties": [COUNTY], "records": deduped
        }, f, indent=2, default=str)
    log.info("[Travis] Written to %s", out_file)
    return deduped

if __name__ == "__main__":
    asyncio.run(scrape())
