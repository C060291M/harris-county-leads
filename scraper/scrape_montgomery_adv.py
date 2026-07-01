import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("montgomery")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
COUNTY = "montgomery"
BASE = "https://montgomery.tx.publicsearch.us"
DEPT = "RP"

DOC_CODES = {
    "L%2FP":            ("LP",      "Lis Pendens"),
    "A%2FJ":            ("JUD",     "Abstract of Judgment"),
    "MCTRA%20JUDGMENT": ("JUD",     "MCTRA Judgment"),
    "FTL":              ("LNFED",   "Federal Tax Lien"),
    "M%2FL":            ("LNMECH",  "Mechanic Lien"),
    "C%2FL":            ("LN",      "Child Support Lien"),
    "H%2FL":            ("LN",      "Hospital Lien"),
    "L%2FN":            ("LN",      "Lien Notice"),
    "AST":              ("NOFC",    "Appointment of Substitute Trustee"),
    "D%2FF":            ("NOFC",    "Deed in Lieu of Foreclosure"),
    "CCP":              ("PRO",     "Certified Copy of Probate"),
    "CDV":              ("DIV",     "Certified Copy Divorce"),
    "AFH":              ("PRO",     "Affidavit of Heirship"),
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def compute_score(cat):
    return {"TAXDEED":50,"LNFED":45,"JUD":35,"LNMECH":30,"PRO":25,"LP":20,"NOFC":20,"LNSTATE":20,"LN":15,"DIV":15}.get(cat, 10)

async def scrape():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    date_param = f"{cutoff.strftime('%Y%m%d')}%2C{now.strftime('%Y%m%d')}"
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
                    continue
                soup = BeautifulSoup(await page.content(), "lxml")
                table = soup.find("table")
                if not table: continue
                rows = table.find_all("tr")
                count = 0
                total_match = re.search(r"([\d,]+)\s+result", text, re.I)
                total = int(total_match.group(1).replace(",","")) if total_match else 0

                def parse_rows(rows):
                    recs = []
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) < 5: continue
                        ne = [c.get_text(" ", strip=True) for c in cells]
                        ne = [x for x in ne if x]
                        doc_num = ne[0].strip()
                        doc_t   = ne[2].strip() if len(ne) > 2 else ''
                        grantor = ne[4].strip() if len(ne) > 4 else ''
                        grantee = ne[5].strip() if len(ne) > 5 else ''
                        filed   = norm_date(ne[7].strip()) if len(ne) > 7 else ''
                        legal   = ne[6].strip() if len(ne) > 6 else ""
                        if not doc_num or len(doc_num) < 5: continue
                        if not grantor or len(grantor) < 3: continue
                        if not filed or filed < "2025-01-01": continue
                        key = f"{doc_num}|montgomery"
                        if key in seen: continue
                        seen.add(key)
                        recs.append({
                            "doc_num": doc_num, "doc_type": doc_t or lbl,
                            "cat": cat, "cat_label": lbl,
                            "filed": filed, "owner": grantor, "grantee": grantee,
                            "amount": None, "legal": legal,
                            "clerk_url": url, "county": COUNTY,
                            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                            "score": compute_score(cat), "flags": [lbl],
                        })
                    return recs

                new_recs = parse_rows(rows)
                all_records.extend(new_recs)
                count += len(new_recs)

                offset = 50
                prev_total = len(all_records)
                while offset < total and offset < 1000:
                    pg_url = url + f"&offset={offset}"
                    await page.goto(pg_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(3000)
                    soup2 = BeautifulSoup(await page.content(), "lxml")
                    t2 = soup2.find("table")
                    if not t2: break
                    pg_recs = parse_rows(t2.find_all("tr"))
                    if len(all_records) == prev_total: break
                    all_records.extend(pg_recs)
                    count += len(pg_recs)
                    if len(all_records) == prev_total: break
                    prev_total = len(all_records)
                    offset += 50

                log.info(f"Montgomery {code} ({lbl}): {count} records")
            except Exception as e:
                log.warning(f"Montgomery {code}: {e}")

        await browser.close()

    log.info(f"Montgomery total: {len(all_records)} unique records")
    out = {"county": COUNTY, "total": len(all_records), "records": all_records}
    with open("dashboard/montgomery_records.json", "w") as f:
        json.dump(out, f)
    log.info("Saved -> dashboard/montgomery_records.json")

asyncio.run(scrape())
