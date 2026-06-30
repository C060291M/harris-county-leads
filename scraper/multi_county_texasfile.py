"""
StackIQ TexasFile Scraper - All 254 TX Counties
Monthly Filings search - free HTML scraping
"""
import os, re, json, logging, time, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("texasfile")

TF_USER = os.getenv("TF_USER", "")
TF_PASS = os.getenv("TF_PASS", "")
COUNTY = os.getenv("COUNTY", "waller")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

BASE = "https://www.texasfile.com"

DISTRESS_TYPES = {
    "LIS PENDENS", "ABSTRACT OF JUDGEMENT", "ABSTRACT OF JUDGMENT",
    "FEDERAL TAX LIEN", "MECHANICS LIEN", "MECHANICS LIEN AFFIDAVIT",
    "STATE TAX LIEN", "HOSPITAL LIEN", "JUDGMENT", "JUDGEMENT",
    "NOTICE OF TRUSTEE SALE", "TRUSTEE SALE", "FORECLOSURE",
    "TAX LIEN STATE", "TAX LIEN FEDERAL", "TAX LIEN",
    "ABSTRACT OF JUDGMENT LIEN", "MECHANIC LIEN",
}

def norm_date(raw):
    raw = re.sub(r"View.*$","",str(raw)).strip()
    for fmt in ("%m/%d/%Y","%Y-%m-%d"):
        try: return datetime.strptime(raw[:10],fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def cat_from_type(t):
    t = t.upper()
    if "LIS PEN" in t: return ("LP","Lis Pendens")
    if "ABSTRACT" in t or "JUDGEMENT" in t or "JUDGMENT" in t: return ("JUD","Abstract of Judgment")
    if "FEDERAL" in t: return ("LNFED","Federal Tax Lien")
    if "STATE TAX" in t: return ("LNSTATE","State Tax Lien")
    if "MECHANIC" in t: return ("LNMECH","Mechanic Lien")
    if "HOSPITAL" in t: return ("LN","Hospital Lien")
    if "TRUSTEE" in t or "FORECLOS" in t: return ("NOFC","Notice of Foreclosure")
    return ("LN", t.title())

def compute_score(r):
    s,flags = 0,[]
    cat = r.get("cat","")
    if cat=="LNFED": flags.append("Fed Tax Lien"); s+=45
    elif cat=="JUD": flags.append("Judgment"); s+=35
    elif cat=="LNMECH": flags.append("Mech Lien"); s+=30
    elif cat in("LP","NOFC"): flags.append("Lis Pendens"); s+=20
    elif cat=="LNSTATE": flags.append("State Tax Lien"); s+=20
    elif cat=="LN": flags.append("Lien"); s+=15
    filed = r.get("filed","")
    if filed:
        try:
            days=(datetime.now()-datetime.strptime(filed[:10],"%Y-%m-%d")).days
            if days<=7: flags.append("New this week"); s+=10
            elif days<=30: flags.append("Filed this month"); s+=5
        except: pass
    return min(s,100),flags

def parse_table(html, county):
    soup = BeautifulSoup(html,"lxml")
    table = soup.find("table")
    if not table: return []
    records = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 8: continue
        date_raw = cells[1].get_text(" ",strip=True)
        doc_type = cells[2].get_text(strip=True).upper()
        doc_num = cells[3].get_text(strip=True)
        grantor = cells[7].get_text(" ",strip=True)
        grantee = cells[8].get_text(" ",strip=True) if len(cells)>8 else ""
        
        # Filter distress types only
        if not any(d in doc_type for d in DISTRESS_TYPES): continue
        
        filed = norm_date(date_raw)
        if filed and filed < "2025-01-01": continue
        if not doc_num or not re.search(r"\d{4,}", doc_num): continue
        
        cat, lbl = cat_from_type(doc_type)
        rec = {
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": lbl,
            "filed": filed, "owner": grantor, "grantee": grantee,
            "amount": None, "legal": "",
            "county": county.lower().replace("-"," "),
            "clerk_url": f"{BASE}/search/texas/{county}-county/county-clerk-records/",
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "score": 0, "flags": [],
        }
        rec["score"], rec["flags"] = compute_score(rec)
        records.append(rec)
    return records

async def main():
    now = datetime.now()
    county_slug = COUNTY.lower().replace(" ","-")
    
    # Get months to scrape
    months = set()
    for d in range(LOOKBACK_DAYS+32):
        dt = now - timedelta(days=d)
        months.add((dt.year, dt.month))
    months = sorted(months, reverse=True)
    
    all_records = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = await browser.new_page(viewport={"width":1280,"height":800})
        
        # Login
        await page.goto(f"{BASE}/")
        await page.wait_for_timeout(2000)
        await page.click("a.Nav-accentBtn")
        await page.wait_for_timeout(2000)
        await page.fill("input[name='username']", TF_USER)
        await page.fill("input[name='password']", TF_PASS)
        await page.click("button[type='submit']")
        await page.wait_for_timeout(3000)
        log.info("Login: %s", page.url)
        
        for year, month in months:
            log.info("Scraping %s %d-%02d", COUNTY, year, month)
            
            await page.goto(f"{BASE}/search/texas/{county_slug}-county/county-clerk-records/")
            await page.wait_for_timeout(2000)
            await page.click("text=Monthly Filings")
            await page.wait_for_timeout(500)
            
            # Set year and month by value
            year_selects = await page.query_selector_all("select")
            if len(year_selects) >= 2:
                await year_selects[0].select_option(str(year))
                await year_selects[1].select_option(str(month))
            await page.wait_for_timeout(500)
            
            await page.click("button.new-search-btn")
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except:
                pass
            await page.wait_for_timeout(3000)
            # If URL still has # or is same page, try waiting more
            if page.url.endswith("#") or "county-clerk-records/" == page.url.split("texasfile.com")[-1]:
                await page.wait_for_timeout(3000)
            
            log.info("Results URL: %s", page.url)
            try:
                html = await page.content()
            except Exception as e:
                log.warning("Could not get content: %s", e)
                continue
            recs = parse_table(html, county_slug)
            log.info("%s %d-%02d: %d distress records", COUNTY, year, month, len(recs))
            all_records.extend(recs)
            
            # Handle pagination using Playwright
            pg = 2
            while pg <= 30:
                try:
                    nxt = await page.query_selector("a:has-text('Next'), .next-page a, li.next a")
                    if not nxt:
                        break
                    await nxt.click()
                    try: await page.wait_for_load_state("networkidle", timeout=10000)
                    except: pass
                    await page.wait_for_timeout(1500)
                    html = await page.content()
                    recs = parse_table(html, county_slug)
                    log.info("  Page %d: %d records", pg, len(recs))
                    if not recs: break
                    all_records.extend(recs)
                    pg += 1
                except Exception as e:
                    log.warning("Pagination p%d: %s", pg, e)
                    break
    log.info("Saved -> dashboard/texasfile_%s_records.json", slug)

if __name__ == "__main__":
    asyncio.run(main())
