import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bexar_hidalgo")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
TARGET = os.getenv("COUNTY", "bexar").lower()

CONFIGS = {
    "bexar": {
        "base": "https://bexar.tx.publicsearch.us",
        "dept": "RP",
        "date_fmt": "%Y-%m-%d",
        "codes": {
            "LIS%20PEN":  ("LP",     "Lis Pendens"),
            "FTL":        ("LNFED",  "Federal Tax Lien"),
            "MECHLN":     ("LNMECH", "Mechanic Lien"),
            "NOTICE":     ("NOFC",   "Notice"),
            "CSUP%20LN":  ("LN",     "Child Support Lien"),
            "HOSP%20LN":  ("LN",     "Hospital Lien"),
            "PROBATE":    ("PRO",    "Probate"),
        }
    },
    "hidalgo": {
        "base": "https://hidalgo.tx.publicsearch.us",
        "dept": "RP",
        "date_fmt": "%Y-%m-%d",
        "codes": {
            "ABSTRACT%20JUDGEM":  ("JUD",    "Abstract of Judgment"),
            "AFFIDVT%20HEIRSHI":  ("PRO",    "Affidavit of Heirship"),
            "ML":                 ("LNMECH", "Mechanic Lien"),
            "AMD%20HOSPITAL%20LI":("LN",     "Hospital Lien"),
            "CSL":                ("LN",     "Child Support Lien"),
            "AMD%20DIVORCE":      ("DIV",    "Divorce"),
            "FTL":                ("LNFED",  "Federal Tax Lien"),
        }
    }
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def compute_score(cat):
    return {"TAXDEED":50,"LNFED":45,"JUD":35,"LNMECH":30,"PRO":25,"LP":20,"NOFC":20,"LNSTATE":20,"LN":15,"DIV":15}.get(cat, 10)

async def scrape(county, config):
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    date_param = f"{cutoff.strftime(config['date_fmt'])}%2C{now.strftime(config['date_fmt'])}"
    all_records = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(viewport={"width":1280,"height":800})

        for code, (cat, lbl) in config["codes"].items():
            try:
                url = f"{config['base']}/results?department={config['dept']}&docTypes={code}&recordedDateRange={date_param}&searchType=advancedSearch"
                await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                await page.wait_for_timeout(6000)
                text = await page.evaluate("document.body.innerText")
                if "No Results Found" in text or "Error with search" in text:
                    continue
                soup = BeautifulSoup(await page.content(), "lxml")
                table = soup.find("table")
                if not table: continue
                
                total_match = re.search(r"([\d,]+)\s+result", text, re.I)
                total = int(total_match.group(1).replace(",","")) if total_match else 0
                
                def parse_rows(rows):
                    recs = []
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) < 5: continue
                        ne = [c.get_text(" ", strip=True) for c in cells]
                        ne = [x for x in ne if x]
                        if len(ne) < 5: continue
                        grantor = ne[0].strip()
                        grantee = ne[1].strip() if len(ne) > 1 else ""
                        doc_t   = ne[2].strip() if len(ne) > 2 else ""
                        filed   = norm_date(ne[3].strip()) if len(ne) > 3 else ""
                        doc_num = ne[4].strip() if len(ne) > 4 else ""
                        if not doc_num or len(doc_num) < 5: continue
                        if not grantor or len(grantor) < 3: continue
                        if not filed or filed < "2025-01-01": continue
                        key = f"{doc_num}|{county}"
                        if key in seen: continue
                        seen.add(key)
                        recs.append({
                            "doc_num": doc_num, "doc_type": doc_t or lbl,
                            "cat": cat, "cat_label": lbl,
                            "filed": filed, "owner": grantor, "grantee": grantee,
                            "amount": None, "legal": "",
                            "clerk_url": url, "county": county,
                            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                            "score": compute_score(cat), "flags": [lbl],
                        })
                    return recs

                new_recs = parse_rows(table.find_all("tr"))
                all_records.extend(new_recs)
                count = len(new_recs)

                offset = 50
                prev_total = len(all_records)
                while offset < total and offset < 2000:
                    pg_url = url + f"&offset={offset}"
                    await page.goto(pg_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(3000)
                    soup2 = BeautifulSoup(await page.content(), "lxml")
                    t2 = soup2.find("table")
                    if not t2:
                        await page.wait_for_timeout(4000)
                        soup2 = BeautifulSoup(await page.content(), "lxml")
                        t2 = soup2.find("table")
                    if not t2: offset += 50; continue
                    pg_recs = parse_rows(t2.find_all("tr"))
                    all_records.extend(pg_recs)
                    count += len(pg_recs)
                    if len(all_records) == prev_total: break
                    prev_total = len(all_records)
                    offset += 50

                log.info(f"{county.title()} {code} ({lbl}): {count} records")
            except Exception as e:
                log.warning(f"{county} {code}: {e}")

        await browser.close()

    log.info(f"{county.title()} total: {len(all_records)} unique records")
    out = {"county": county, "total": len(all_records), "records": all_records}
    with open(f"dashboard/{county}_adv_records.json", "w") as f:
        json.dump(out, f)
    log.info(f"Saved -> dashboard/{county}_adv_records.json")

async def main():
    config = CONFIGS.get(TARGET)
    if not config:
        log.error(f"Unknown county: {TARGET}")
        return
    await scrape(TARGET, config)

asyncio.run(main())
