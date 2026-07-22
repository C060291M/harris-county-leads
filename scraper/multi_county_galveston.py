"""

StackIQ â€” Galveston County Scraper (Fidlar AVA Web)

Portal: ava.fidlar.com/TXGalveston/AvaWeb/#/search

System: Fidlar Technologies AVA Web

Free public access, no login, all results on one page

"""

import json, logging, re, os, asyncio

from datetime import datetime, timedelta

from playwright.async_api import async_playwright



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

log = logging.getLogger("galveston")



LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))

BASE_URL      = "https://ava.fidlar.com/TXGalveston/AvaWeb/#/search"



KEEP_DOC_TYPES = {
    "LIS PENDENS", "LIS P", "LIS PEN",
    "FORECLOSURE", "FORECLOSURE FILED",
    "LIEN", "LIEN AFFD", "HOA", "HOA LIEN",
    "DIVORCE", "CC DIVORCE",
    "ABSTRACT", "JUDGMENT", "A OF J", "JUDG",
    "MECHANIC", "MECH LIEN",
    "FED LIEN", "FEDERAL", "FED TAX", "IRS",
    "STATE LIEN", "ST TAX", "STATE TAX",
    "TAX DEED", "TAX LIEN",
    "PROBATE", "PROB",
    "NOTICE",
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

        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)

        await page.wait_for_timeout(3000)



        # Fill start date

        try:

            start_input = await page.query_selector("input[placeholder='MM/DD/YYYY']:first-of-type")

            if not start_input:

                inputs = await page.query_selector_all("input[placeholder='MM/DD/YYYY']")

                start_input = inputs[0] if inputs else None

            if start_input:

                await start_input.evaluate("el => el.click()")

                await start_input.fill(start_str)

                await page.wait_for_timeout(300)

        except Exception as e:

            log.warning("Galveston: start date error: %s", e)



        # Fill end date

        try:

            inputs = await page.query_selector_all("input[placeholder='MM/DD/YYYY']")

            if len(inputs) >= 2:

                await inputs[1].evaluate("el => el.click()")

                await inputs[1].fill(end_str)

                await page.wait_for_timeout(300)

        except Exception as e:

            log.warning("Galveston: end date error: %s", e)



        # Click Search button

        try:

            search_btn = await page.query_selector("button:has-text('SEARCH'), button:has-text('Search')")

            if search_btn:

                await search_btn.evaluate("el => el.click()")

                log.info("Galveston: search submitted %s to %s", start_str, end_str)

                await page.wait_for_timeout(6000)  # Wait for SPA to load results

        except Exception as e:

            log.warning("Galveston: search button error: %s", e)



        # Parse results

        content = await page.evaluate("document.body.innerText")

        records = parse_results(content)

        log.info("Galveston: %d total records found", len(records))



        await browser.close()

    return records



def parse_results(page_text):
    """Parse Galveston AvaWeb results from page innerText"""
    import re
    records = []
    
    # Doc number pattern: 2026XXXXXX
    # Format in text: doc_num\ndoc_type\ndate\n...
    lines = [l.strip() for l in page_text.split("\n") if l.strip()]
    
    i = 0
    while i < len(lines):
        # Look for document number (year + 6 digits)
        if re.match(r"^20\d{8}$", lines[i]):
            doc_num = lines[i]
            doc_type = lines[i+1] if i+1 < len(lines) else ""
            date_raw = lines[i+2] if i+2 < len(lines) else ""
            party1 = ""
            party2 = ""
            
            # Skip UNOFFICIAL markers
            j = i + 1
            while j < len(lines) and j < i + 10:
                if lines[j] == doc_num:
                    j += 1
                    continue
                if lines[j] == "UNOFFICIAL":
                    j += 1
                    continue
                if re.match(r"^\d{1,2}/\d{1,2}/\d{4}", lines[j]):
                    date_raw = lines[j]
                    j += 1
                    continue
                if lines[j] in ("Page Count:", "Parties", "Legals", "Additional"):
                    break
                if not doc_type or doc_type == doc_num:
                    doc_type = lines[j]
                elif not party1 and lines[j] not in ("Party 1:", "Party 2:"):
                    party1 = lines[j]
                elif not party2 and lines[j] not in ("Party 1:", "Party 2:"):
                    party2 = lines[j]
                j += 1
            
            # Filter to distress types
            dt_upper = doc_type.upper()
            # Guard against the parser misfiring and reusing a doc-type
            # label as the party name (e.g. owner ending up as "A OF J"
            # or "NOTICE") - reject anything that matches a known
            # doc-type token instead of saving it as a real name.
            _bad_owner_tokens = KEEP_DOC_TYPES | {"NOTICE/PUR", "FORECLOSURE NOTICE RECORDING", "UNOFFICIAL"}
            if party1 and party1.upper().strip() in _bad_owner_tokens:
                party1 = ""
            if party2 and party2.upper().strip() in _bad_owner_tokens:
                party2 = ""
            if any(k in dt_upper for k in KEEP_DOC_TYPES) and party1:
                cat, cat_label = cat_from_doc_type(doc_type)
                records.append({
                    "doc_num": doc_num,
                    "doc_type": doc_type,
                    "cat": cat,
                    "cat_label": cat_label,
                    "filed": norm_date(date_raw),
                    "owner": party1,
                    "grantee": party2,
                    "amount": None,
                    "county": "galveston",
                    "clerk_url": "https://ava.fidlar.com/TXGalveston/AvaWeb/",
                    "prop_address": "", "score": 0, "flags": [],
                })
            i = j
        else:
            i += 1
    
    return records

async def main_async():

    now    = datetime.now()

    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    log.info("=== Galveston County Scraper ===")

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

        "source": "Galveston County Clerk (Fidlar AVA Web)",

        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},

        "total": len(deduped),

        "counties": ["Galveston"],

        "records": deduped,

    }



    os.makedirs("dashboard", exist_ok=True)

    os.makedirs("data", exist_ok=True)

    with open("dashboard/galveston_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    with open("data/galveston_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    log.info("Saved -> dashboard/galveston_records.json")



    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)

    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)

    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)



def main():

    asyncio.run(main_async())



if __name__ == "__main__":

    main()


