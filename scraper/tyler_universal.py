"""
Universal Tyler iDS scraper - dynamically finds DOCSEARCH URL via action group
Works for ANY countytx-web.tylerhost.net portal regardless of DOCSEARCH ID
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tyler_universal")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "3"))
COUNTY_NAME   = os.getenv("COUNTY_NAME", "")
BASE_URL      = os.getenv("BASE_URL", "")

KEEP_TYPES = ["LIS PENDENS","ABSTRACT OF JUDGMENT","FEDERAL TAX LIEN",
    "MECHANIC","STATE TAX LIEN","JUDGMENT","LIEN",
    "NOTICE OF TRUSTEE SALE","PROBATE","DIVORCE","HOSPITAL LIEN","FORECLOSURE"]

def norm_date(raw):
    if not raw: return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", str(raw).strip())
    if m: 
        try: return datetime(int(m.group(3)),int(m.group(1)),int(m.group(2))).strftime("%Y-%m-%d")
        except: pass
    return None

def cat_from_doc_type(dt):
    d = dt.upper()
    if "LIS PEN" in d: return ("LP","Lis Pendens")
    if "TRUSTEE" in d: return ("NOFC","Notice of Trustee Sale")
    if "TAX DEED" in d: return ("TAXDEED","Tax Deed")
    if "ABSTRACT" in d or "JUDGMENT" in d: return ("JUD","Judgment")
    if "FEDERAL" in d or "IRS" in d: return ("LNFED","Federal Tax Lien")
    if "STATE TAX" in d: return ("LNSTATE","State Tax Lien")
    if "MECHANIC" in d: return ("LNMECH","Mechanic Lien")
    if "HOSPITAL" in d: return ("LNHOA","Hospital Lien")
    if "PROBATE" in d: return ("PRO","Probate")
    if "DIVORCE" in d: return ("DIV","Divorce")
    if "FORECLOSURE" in d: return ("NOFC","Foreclosure")
    return ("LN", dt)

async def find_docsearch_url(page, base_url):
    """Navigate home page to find Official Records DOCSEARCH URL"""
    # Extract all link data at once using JS to avoid context destruction
    link_data = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]')).map(a => ({
            href: a.getAttribute('href') || '',
            text: a.innerText.trim()
        }));
    }""")
    
    # Check for direct DOCSEARCH links
    for l in link_data:
        if 'DOCSEARCH' in l['href'] and 'Plat' not in l['text']:
            href = l['href']
            return base_url.rstrip("/web") + href if href.startswith('/') else href
    
    # Find action group for Official Records
    action_url = None
    for l in link_data:
        if 'ACTIONGROUP' in l['href'] and ('Official' in l['text'] or ('Record' in l['text'] and 'Vital' not in l['text'])):
            href = l['href']
            action_url = base_url.rstrip("/web") + href if href.startswith('/') else href
            break
    
    if action_url:
        await page.goto(action_url, timeout=20000, wait_until='domcontentloaded')
        await page.wait_for_timeout(2000)
        # Extract links again
        link_data2 = await page.evaluate("""() => {
            return Array.from(document.querySelectorAll('a[href]')).map(a => ({
                href: a.getAttribute('href') || '',
                text: a.innerText.trim()
            }));
        }""")
        for l in link_data2:
            if 'DOCSEARCH' in l['href'] and 'Plat' not in l['text']:
                href = l['href']
                return base_url.rstrip("/web") + href if href.startswith('/') else href
    return None

async def scrape_county(county, base_url, start_str, end_str):
    records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")
        
        # Accept disclaimer
        try:
            await page.goto(base_url + "/user/disclaimer", timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            await page.evaluate("(() => { const b = document.querySelector('button'); if(b){ b.removeAttribute('disabled'); b.click(); } })()")
            await page.wait_for_timeout(2000)
            log.info(f"{county}: disclaimer accepted, at {page.url}")
        except Exception as e:
            log.warning(f"{county}: disclaimer error: {e}")
        
        # Find DOCSEARCH URL dynamically
        search_url = await find_docsearch_url(page, base_url)
        if not search_url:
            log.warning(f"{county}: could not find DOCSEARCH URL")
            await browser.close()
            return records
        
        log.info(f"{county}: using search URL {search_url}")
        
        # Determine date field names
        await page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        
        # Try both field name patterns
        start_field = await page.query_selector("input[name='field_RecordingDateID_DOT_StartDate']")
        if not start_field:
            start_field = await page.query_selector("input[name='field_RecDateID_DOT_StartDate']")
            date_start = "field_RecDateID_DOT_StartDate"
            date_end = "field_RecDateID_DOT_EndDate"
        else:
            date_start = "field_RecordingDateID_DOT_StartDate"
            date_end = "field_RecordingDateID_DOT_EndDate"
        
        log.info(f"{county}: date fields: {date_start}")
        
        for page_num in range(1, MAX_PAGES + 1):
            try:
                await page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)
                await page.fill(f"input[name='{date_start}']", start_str)
                await page.fill(f"input[name='{date_end}']", end_str)
                await page.wait_for_timeout(500)
                search_link = await page.query_selector("a[href*='searchResults']")
                if search_link:
                    await search_link.click()
                    await page.wait_for_timeout(6000)
                
                soup = BeautifulSoup(await page.content(), "lxml")
                items = soup.find_all("li", attrs={"data-documentid": True})
                log.info(f"{county}: page {page_num} - {len(items)} items")
                
                page_records = 0
                for item in items:
                    h1 = item.find("h1")
                    if not h1: continue
                    h1_clean = " ".join(h1.get_text(" ", strip=True).split())
                    if not any(k in h1_clean.upper() for k in KEEP_TYPES): continue
                    parts = re.split(r"[\u2022\xa0\s]{2,}", h1_clean)
                    parts = [p.strip() for p in parts if p.strip()]
                    doc_num = parts[0] if parts else ""
                    doc_type = parts[-1] if len(parts) > 1 else h1_clean
                    full_text = item.get_text(" ", strip=True)
                    date_m = re.search(r"(\d{2}/\d{2}/\d{4})", full_text)
                    filed = norm_date(date_m.group(1)) if date_m else ""
                    grantor_m = re.search(r"Grantor\s+([A-Z][^\n]+?)(?:\s{2,}|Grantee|Recording)", full_text)
                    owner = grantor_m.group(1).strip() if grantor_m else ""
                    cat, cat_label = cat_from_doc_type(doc_type)
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "cat": cat, "cat_label": cat_label,
                        "filed": filed, "owner": owner, "grantee": "",
                        "amount": None, "legal": "",
                        "clerk_url": search_url, "county": county,
                        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                        "score": 0, "flags": [],
                    })
                    page_records += 1
                
                log.info(f"{county}: page {page_num} found {page_records} distress records")
                if page_records == 0: break
                
            except Exception as e:
                log.warning(f"{county}: page {page_num} error: {e}")
                break
        
        await browser.close()
    return records

async def main():
    if not COUNTY_NAME or not BASE_URL:
        log.error("COUNTY_NAME and BASE_URL env vars required")
        return
    
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_str = cutoff.strftime("%m/%d/%Y")
    end_str = now.strftime("%m/%d/%Y")
    
    log.info(f"=== Tyler Universal Scraper: {COUNTY_NAME} ===")
    log.info(f"Date range: {start_str} to {end_str}")
    
    records = await scrape_county(COUNTY_NAME, BASE_URL, start_str, end_str)
    
    # Deduplicate
    seen, deduped = set(), []
    for r in records:
        if r["doc_num"] not in seen:
            seen.add(r["doc_num"])
            deduped.append(r)
    
    log.info(f"Total unique: {len(deduped)}")
    
    os.makedirs("dashboard", exist_ok=True)
    fname = COUNTY_NAME.lower().replace(" ","_")
    with open(f"dashboard/{fname}_records.json", "w") as f:
        json.dump({"records": deduped, "total": len(deduped), "county": COUNTY_NAME}, f, indent=2, default=str)
    log.info(f"Saved -> dashboard/{fname}_records.json")

if __name__ == "__main__":
    asyncio.run(main())
