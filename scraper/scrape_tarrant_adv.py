"""
StackIQ - Tarrant County PublicSearch Scraper
Uses advancedSearch with correct doc type codes discovered 2026-06-30
Portal: https://tarrant.tx.publicsearch.us
Codes: LP=15, J=656, L=672, FTL=157, STL=120, ML=20, MLAFF=184, CL=226, FL=11, PROB=58, DIV=28, AOH=260
"""
import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tarrant")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
COUNTY = "tarrant"
BASE = "https://tarrant.tx.publicsearch.us"
DEPT = "RP"

# All distress doc type codes for Tarrant PublicSearch (advancedSearch format)
DOC_CODES = {
    "LP":    ("LP",      "Lis Pendens"),
    "J":     ("JUD",     "Judgment"),
    "L":     ("LN",      "Lien"),
    "FTL":   ("LNFED",   "Federal Tax Lien"),
    "STL":   ("LNSTATE", "State Tax Lien"),
    "ML":    ("LNMECH",  "Mechanic Lien"),
    "MLAFF": ("LNMECH",  "Mechanic Lien Affidavit"),
    "CL":    ("LN",      "Child Support Lien"),
    "FL":    ("LNFED",   "Federal Lien"),
    "PROB":  ("PRO",     "Probate"),
    "DIV":   ("DIV",     "Divorce"),
    "AOH":   ("PRO",     "Affidavit of Heirship"),
    "SAJ":   ("JUD",     "State Abstract of Judgment"),
    "NTS":   ("NOFC",    "Notice of Trustee Sale"),
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def compute_score(cat):
    scores = {"TAXDEED":50,"LNFED":45,"JUD":35,"LNMECH":30,"PRO":25,"LP":20,"NOFC":20,"LNSTATE":20,"LN":15,"DIV":15}
    return scores.get(cat, 10)

async def scrape_tarrant():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start_s = cutoff.strftime("%Y-%m-%d")
    end_s = now.strftime("%Y-%m-%d")
    date_param = f"{start_s}%2C{end_s}"
    
    all_records = []
    seen = set()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(viewport={"width":1280,"height":800})
        
        for code, (cat, lbl) in DOC_CODES.items():
            try:
                url = f"{BASE}/results?department={DEPT}&docTypes={code}&recordedDateRange={date_param}&searchType=advancedSearch"
                await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                await page.wait_for_timeout(4000)
                
                text = await page.evaluate("document.body.innerText")
                if "No Results Found" in text or "Error with search" in text:
                    log.info(f"Tarrant {code}: 0 records")
                    continue
                
                soup = BeautifulSoup(await page.content(), "lxml")
                table = soup.find("table")
                if not table:
                    log.info(f"Tarrant {code}: no table")
                    continue
                
                rows = table.find_all("tr")
                count = 0
                
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 5: continue
                    ne = [c.get_text(" ", strip=True) for c in cells]
                    ne = [x for x in ne if x]
                    if len(ne) < 5: continue
                    
                    # Tarrant advancedSearch columns:
                    # 0=grantor, 1=grantee, 2=doc_type, 3=date, 4=doc_num, 5=book, 6=legal
                    grantor = ne[0].strip()
                    grantee = ne[1].strip()
                    doc_t   = ne[2].strip()
                    filed   = norm_date(ne[3].strip())
                    doc_num = ne[4].strip()
                    legal   = ne[6].strip() if len(ne) > 6 else ""
                    
                    if not doc_num or len(doc_num) < 5: continue
                    if not grantor or len(grantor) < 3: continue
                    if not filed or filed < "2025-01-01": continue
                    
                    key = f"{doc_num}|tarrant"
                    if key in seen: continue
                    seen.add(key)
                    
                    score = compute_score(cat)
                    all_records.append({
                        "doc_num": doc_num, "doc_type": doc_t or lbl,
                        "cat": cat, "cat_label": lbl,
                        "filed": filed, "owner": grantor, "grantee": grantee,
                        "amount": None, "legal": legal,
                        "clerk_url": url, "county": COUNTY,
                        "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                        "score": score, "flags": [lbl],
                    })
                    count += 1
                
                log.info(f"Tarrant {code} ({lbl}): {count} records")
                
                # Handle pagination - check for more pages
                page_text = await page.evaluate("document.body.innerText")
                total_match = re.search(r"([\d,]+)\s+result", page_text, re.I)
                total = int(total_match.group(1).replace(",","")) if total_match else 0
                
                if total > 50:
                    # Paginate
                    offset = 50
                    while offset < total and offset < 1000:
                        pg_url = url + f"&offset={offset}"
                        await page.goto(pg_url, timeout=30000, wait_until="domcontentloaded")
                        await page.wait_for_timeout(4000)
                        soup2 = BeautifulSoup(await page.content(), "lxml")
                        table2 = soup2.find("table")
                        if not table2: break
                        rows2 = table2.find_all("tr")
                        new_count = 0
                        for row in rows2:
                            cells = row.find_all("td")
                            if len(cells) < 5: continue
                            ne = [c.get_text(" ", strip=True) for c in cells]
                            ne = [x for x in ne if x]
                            if len(ne) < 5: continue
                            grantor = ne[0].strip()
                            grantee = ne[1].strip()
                            doc_t   = ne[2].strip()
                            filed   = norm_date(ne[3].strip())
                            doc_num = ne[4].strip()
                            legal   = ne[6].strip() if len(ne) > 6 else ""
                            if not doc_num or len(doc_num) < 5: continue
                            if not grantor or len(grantor) < 3: continue
                            if not filed or filed < "2025-01-01": continue
                            key = f"{doc_num}|tarrant"
                            if key in seen: continue
                            seen.add(key)
                            score = compute_score(cat)
                            all_records.append({
                                "doc_num": doc_num, "doc_type": doc_t or lbl,
                                "cat": cat, "cat_label": lbl,
                                "filed": filed, "owner": grantor, "grantee": grantee,
                                "amount": None, "legal": legal,
                                "clerk_url": pg_url, "county": COUNTY,
                                "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                                "score": score, "flags": [lbl],
                            })
                            new_count += 1
                        if new_count == 0: break
                        log.info(f"  page offset={offset}: {new_count} more")
                        offset += 50
                        
            except Exception as e:
                log.warning(f"Tarrant {code}: {e}")
        
        await browser.close()
    
    log.info(f"Tarrant total: {len(all_records)} unique records")
    
    out = {"county": COUNTY, "total": len(all_records), "records": all_records}
    with open(f"dashboard/tarrant_records.json", "w") as f:
        json.dump(out, f)
    log.info(f"Saved -> dashboard/tarrant_records.json")

asyncio.run(scrape_tarrant())
