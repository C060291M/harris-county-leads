"""

StackIQ - Universal PublicSearch.us Scraper

Covers all TX counties on pubsearch.us using Quick Search URL approach

"""

import json, logging, re, os, asyncio

from datetime import datetime, timedelta

from playwright.async_api import async_playwright

from bs4 import BeautifulSoup

from urllib.parse import quote



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

log = logging.getLogger("pubsearch")



LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

MAX_PAGES     = int(os.getenv("MAX_PAGES", "3"))



# County -> (base_url, department_code)

COUNTIES = {

    "Tarrant":    ("https://tarrant.tx.publicsearch.us",    "RP"),
    "Smith":      ("https://smith.tx.publicsearch.us",       "RP"),

    "Montgomery": ("https://montgomery.tx.publicsearch.us", "RP"),

    "Bell":       ("https://bell.tx.publicsearch.us",       "RP"),

    "Medina":     ("https://medina.tx.publicsearch.us",     "RP"),

    "Coleman":    ("https://coleman.tx.publicsearch.us",    "RP"),

    "Brewster":   ("https://brewster.tx.publicsearch.us",   "RP"),

    "Hockley":    ("https://hockley.tx.publicsearch.us",    "RP"),

    "Jim Hogg":   ("https://jimhogg.tx.publicsearch.us",    "RP"),

    "Red River":  ("https://redriver.tx.publicsearch.us",   "RP"),

    "Refugio":    ("https://refugio.tx.publicsearch.us",    "RP"),

    "Jim Wells":  ("https://jimwells.tx.publicsearch.us",   "RP"),

}



DOC_TYPES = [

    "Lis Pendens", "Tax Deed", "Abstract of Judgment",

    "Federal Tax Lien", "State Tax Lien", "Mechanic Lien",

    "Probate", "Divorce", "Judgment", "Notice of Foreclosure",

]



def norm_date(raw):

    if not raw: return ""

    m = re.search(r"(\d{2}/\d{2}/\d{4})", str(raw))

    if m:

        try: return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")

        except: pass

    return str(raw).strip()[:10]



def cat_from_doc_type(dt):

    d = dt.upper()

    if "LIS PEN" in d: return ("LP", "Lis Pendens")

    if "TAX DEED" in d: return ("TAXDEED", "Tax Deed")

    if "ABSTRACT" in d or "JUDGMENT" in d: return ("JUD", "Abstract of Judgment")

    if "FEDERAL" in d: return ("LNFED", "Federal Tax Lien")

    if "STATE TAX" in d: return ("LNSTATE", "State Tax Lien")

    if "MECHANIC" in d: return ("LNMECH", "Mechanic Lien")

    if "PROBATE" in d: return ("PRO", "Probate")

    if "DIVORCE" in d: return ("DIV", "Divorce")

    if "FORECLOSURE" in d: return ("NOFC", "Notice of Foreclosure")

    return ("LN", dt)



def compute_score(r):

    s, flags = 0, []

    cat = r.get("cat", "")

    if cat == "TAXDEED": flags.append("Tax Deed"); s += 50

    elif cat in ("LNFED", "LNIRS"): flags.append("Fed/IRS Lien"); s += 45

    elif cat == "JUD": flags.append("Judgment"); s += 35

    elif cat in ("LNMECH", "LNHOA"): flags.append("Mech/HOA Lien"); s += 30

    elif cat == "PRO": flags.append("Probate"); s += 25

    elif cat in ("LP", "NOFC"): flags.append("Lis Pendens"); s += 20

    elif cat in ("LN", "LNSTATE"): flags.append("Lien"); s += 20

    elif cat == "DIV": flags.append("Divorce"); s += 15

    filed = r.get("filed", "")

    if filed:

        try:

            days = (datetime.now() - datetime.strptime(filed[:10], "%Y-%m-%d")).days

            if days <= 7: flags.append("New this week"); s += 10

            elif days <= 30: flags.append("Filed this month"); s += 5

        except: pass

    return min(s, 100), flags



async def scrape_county(page, county, base_url, dept, start_dt, end_dt):

    records = []

    date_param = f"L{LOOKBACK_DAYS}D" if LOOKBACK_DAYS <= 3 else "L1M" if LOOKBACK_DAYS <= 30 else "L3M"

    

    for doc_type in DOC_TYPES:

        try:

            url = f"{base_url}/results?department={dept}&keywordSearch=false&recordedDateRange={date_param}&searchOcrText=false&searchType=quickSearch&searchValue={quote(doc_type)}"

            await page.goto(url, timeout=30000, wait_until="networkidle")

            await page.wait_for_timeout(2000)

            

            text = await page.evaluate("document.body.innerText")

            if "Error with search" in text or "No Results Found" in text:

                continue

            

            # Parse results table

            soup = BeautifulSoup(await page.content(), "lxml")

            

            # Results are in a table with GRANTOR GRANTEE DOC TYPE RECORDED DATE INST NUMBER

            table = soup.find("table")

            if not table:

                continue

            

            rows = table.find_all("tr")

            log.info(f"{county} {doc_type}: {len(rows)} rows")

            

            for row in rows:

                cells = row.find_all("td")

                if len(cells) < 3: continue

                texts = [c.get_text(" ", strip=True) for c in cells]

                

                # Skip header row and empty rows

                # Table format: [empty,empty,empty, GRANTOR, GRANTEE, DOC TYPE, DATE, INST#, BOOK, LEGAL]

                non_empty = [t for t in texts if t.strip()]

                if len(non_empty) < 4: continue

                if non_empty[0] in ('Grantor', 'GRANTOR', 'Doc Type'): continue

                

                grantor  = non_empty[0]

                grantee  = non_empty[1] if len(non_empty) > 1 else ""

                doc_t    = non_empty[2] if len(non_empty) > 2 else doc_type

                filed    = norm_date(non_empty[3]) if len(non_empty) > 3 else ""

                doc_num  = non_empty[4] if len(non_empty) > 4 else ""

                legal    = non_empty[6] if len(non_empty) > 6 else ""

                

                if not grantor or len(grantor) < 3: continue

                

                cat, cat_label = cat_from_doc_type(doc_t or doc_type)

                rec = {

                    "doc_num": doc_num, "doc_type": doc_t or doc_type,

                    "cat": cat, "cat_label": cat_label,

                    "filed": filed, "owner": grantor, "grantee": grantee,

                    "amount": None, "legal": legal,

                    "clerk_url": url, "county": county,

                    "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",

                    "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",

                    "score": 0, "flags": [],

                }

                rec["score"], rec["flags"] = compute_score(rec)

                records.append(rec)

        

        except Exception as e:

            log.warning(f"{county} {doc_type}: {e}")

    

    log.info(f"{county}: {len(records)} total records")

    return records



async def main():

    now = datetime.now()

    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    

    # Filter counties based on env vars

    counties_env = os.getenv("COUNTIES", "")

    skip_env = os.getenv("SKIP_COUNTIES", "")

    

    active = dict(COUNTIES)

    if counties_env:

        only = [c.strip() for c in counties_env.split(",")]

        active = {k: v for k, v in active.items() if k in only}

    if skip_env:

        skip = [c.strip() for c in skip_env.split(",")]

        active = {k: v for k, v in active.items() if k not in skip}

    

    log.info(f"=== PublicSearch Universal Scraper ===")

    log.info(f"Counties: {list(active.keys())}")

    

    all_records = []

    

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])

        context = await browser.new_context(

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",

            viewport={"width": 1280, "height": 900}

        )

        page = await context.new_page()

        

        for county, (base_url, dept) in active.items():

            recs = await scrape_county(page, county, base_url, dept, cutoff, now)

            all_records.extend(recs)

        

        await browser.close()

    

    # Deduplicate

    seen, deduped = set(), []

    for r in all_records:

        key = f"{r['doc_num']}|{r['county']}"

        if key not in seen:

            seen.add(key)

            deduped.append(r)

    

    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    log.info(f"Total unique: {len(deduped)}")

    

    payload = {

        "fetched_at": now.isoformat(),

        "source": "PublicSearch.us Universal",

        "total": len(deduped),

        "counties": list(COUNTIES.keys()),

        "records": deduped,

    }

    

    os.makedirs("dashboard", exist_ok=True)

    os.makedirs("data", exist_ok=True)

    with open("dashboard/pubsearch_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    with open("data/pubsearch_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    log.info("Saved -> dashboard/pubsearch_records.json")



if __name__ == "__main__":

    asyncio.run(main())

