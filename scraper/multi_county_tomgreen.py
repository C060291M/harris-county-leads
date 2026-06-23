"""

StackIQ — Tyler EagleWeb Scraper

Covers: Crane, Loving, and any other TX county on countygovernmentrecords.com

Free public login, date range search, all results on one page

"""

import json, logging, re, os, asyncio

from datetime import datetime, timedelta

from playwright.async_api import async_playwright



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

log = logging.getLogger("tomgreen")



LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))



COUNTIES = {

    "Tom Green": ("https://tomgreentx.countygovernmentrecords.com/TomGreenTXRecorder", "TomGreenTXRecorder"),

}



KEEP_DOC_TYPES = {

    "LIS PEN","LIS PENDENS","FORECLOSURE","LIEN","ABSTRACT","JUDGMENT",

    "MECHANIC","FED","IRS","HOA","STATE TAX","TAX DEED","PROBATE",

    "NOTICE","DIVORCE","RELEASE OF LIEN","RELLP"

}



def norm_date(raw):

    if not raw: return ""

    m = re.search(r'(\d{2}/\d{2}/\d{4})', str(raw))

    if m:

        try: return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")

        except: pass

    return str(raw).strip()[:10]



def cat_from_doc_type(doc_type):

    dt = doc_type.upper()

    if "LIS PEN" in dt:           return ("LP",      "Lis Pendens")

    if "FORECLOSURE" in dt:       return ("NOFC",    "Notice of Foreclosure")

    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD", "Abstract of Judgment")

    if "FED" in dt or "IRS" in dt: return ("LNFED",  "Federal Tax Lien")

    if "HOA" in dt:                return ("LNHOA",  "HOA Lien")

    if "MECHANIC" in dt:           return ("LNMECH", "Mechanic Lien")

    if "TAX DEED" in dt:           return ("TAXDEED","Tax Deed")

    if "PROBATE" in dt:            return ("PRO",    "Probate")

    if "DIVORCE" in dt:            return ("DIV",    "Divorce")

    if "LIEN" in dt:               return ("LN",     "Lien")

    if "NOTICE" in dt:             return ("NOFC",   "Notice")

    return ("LN", doc_type)



def compute_score(r):

    s, flags = 0, []

    cat = r.get("cat","")

    if cat == "TAXDEED":              flags.append("Tax Deed"); s += 50

    elif cat in ("LNFED",):          flags.append("Fed Lien"); s += 45

    elif cat == "JUD":               flags.append("Judgment"); s += 35

    elif cat in ("LNHOA","LNMECH"):  flags.append("HOA/Mech"); s += 30

    elif cat == "PRO":               flags.append("Probate"); s += 25

    elif cat in ("LP","NOFC"):       flags.append("Lis Pendens"); s += 20

    elif cat == "LN":                flags.append("Lien"); s += 20

    elif cat == "DIV":               flags.append("Divorce"); s += 15

    else:                            flags.append("Distress"); s += 10

    return min(s, 100), flags



async def scrape_eagleweb_county(page, county_name, base_url, path_prefix, start_dt, end_dt):

    records = []

    start_str = start_dt.strftime("%m/%d/%Y")

    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info("EagleWeb %s: %s to %s", county_name, start_str, end_str)



    try:

        # Step 1: Hit disclaimer page

        await page.goto(base_url, wait_until="domcontentloaded", timeout=30000)

        await page.wait_for_timeout(2000)



        # Step 2: Click I Acknowledge

        ack = await page.query_selector("input[value='I Acknowledge'], button:has-text('I Acknowledge')")

        if ack:

            await ack.evaluate("el => el.click()")

            await page.wait_for_timeout(2000)



        # Step 3: Click Public Login

        pub = await page.query_selector("input[value='Public Login'], button:has-text('Public Login')")

        if pub:

            await pub.evaluate("el => el.click()")

            await page.wait_for_timeout(2000)



        # Step 4: Set date range - clear start date and set our dates

        base = base_url.rstrip("/").replace("/web", "")

        search_url = f"{base}/{path_prefix}/eagleweb/docSearch.jsp"

        await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

        await page.wait_for_timeout(2000)



        # Fill start date

        start_input = await page.query_selector("input[name='startDate'], input[id='startDate']")

        if start_input:

            await start_input.triple_click()

            await start_input.fill(start_str)



        # Fill end date

        end_input = await page.query_selector("input[name='endDate'], input[id='endDate']")

        if end_input:

            await end_input.triple_click()

            await end_input.fill(end_str)



        # Make sure Search All Types is checked

        all_types = await page.query_selector("input[name='searchAllTypes']")

        if all_types:

            is_checked = await all_types.is_checked()

            if not is_checked:

                await all_types.evaluate("el => el.click()")



        # Click Search

        search_btn = await page.query_selector("input[value='Search'], button:has-text('Search')")

        if search_btn:

            await search_btn.evaluate("el => el.click()")

            await page.wait_for_timeout(3000)



        # Parse results

        content = await page.content()

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(content, "lxml")



        # Find result rows - each result is a table row with Description and Summary

        result_tables = soup.find_all("table")

        for tbl in result_tables:

            rows = tbl.find_all("tr")

            for row in rows:

                cells = row.find_all("td")

                if len(cells) < 2: continue

                first_cell = cells[0].get_text(" ", strip=True)

                second_cell = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""



                # Description cell contains doc type and doc number

                doc_type_match = re.match(r'^([A-Z][A-Z\s\&\/]+)', first_cell)

                if not doc_type_match: continue

                doc_type = doc_type_match.group(1).strip()

                if not any(k in doc_type.upper() for k in KEEP_DOC_TYPES):

                    continue



                doc_num_match = re.search(r'(\d{4,}[-\d]*)', first_cell)

                doc_num = doc_num_match.group(1) if doc_num_match else ""



                # Parse summary cell

                date_match = re.search(r'RecordingDate:\s*(\d{2}/\d{2}/\d{4})', second_cell)

                filed = norm_date(date_match.group(1)) if date_match else ""



                grantor_match = re.search(r'Grantor:\s*([^\n]+?)(?:Grantee:|Related:|Legal:|$)', second_cell)

                owner = grantor_match.group(1).strip() if grantor_match else ""



                grantee_match = re.search(r'Grantee:\s*([^\n]+?)(?:Related:|Legal:|$)', second_cell)

                grantee = grantee_match.group(1).strip() if grantee_match else ""



                legal_match = re.search(r'Legal:\s*(.+?)$', second_cell)

                legal = legal_match.group(1).strip() if legal_match else ""



                cat, cat_label = cat_from_doc_type(doc_type)

                records.append({

                    "doc_num": doc_num, "doc_type": doc_type,

                    "cat": cat, "cat_label": cat_label,

                    "filed": filed, "owner": owner, "grantee": grantee,

                    "amount": None, "legal": legal,

                    "clerk_url": search_url,

                    "county": county_name,

                    "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",

                    "mail_address": "", "mail_city": "", "mail_state": "TX", "mail_zip": "",

                    "score": 0, "flags": [],

                })



    except Exception as e:

        log.warning("EagleWeb %s error: %s", county_name, e)



    log.info("EagleWeb %s: %d records", county_name, len(records))

    return records



async def main_async():

    now    = datetime.now()

    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    log.info("=== Tyler EagleWeb Scraper ===")



    all_records = []

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])

        context = await browser.new_context(

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",

            viewport={"width": 1280, "height": 900}

        )

        page = await context.new_page()

        for county_name, (base_url, path_prefix) in COUNTIES.items():

            recs = await scrape_eagleweb_county(page, county_name, base_url, path_prefix, cutoff, now)

            all_records.extend(recs)

        await browser.close()



    seen, deduped = set(), []

    for r in all_records:

        key = f"{r['county']}|{r['doc_num']}|{r['filed']}"

        if key not in seen:

            seen.add(key)

            r["score"], r["flags"] = compute_score(r)

            deduped.append(r)



    deduped.sort(key=lambda x: x.get("score", 0), reverse=True)

    log.info("Total: %d records across %d counties", len(deduped), len(COUNTIES))



    payload = {

        "fetched_at": now.isoformat(),

        "source": "Tom Green County Clerk (EagleWeb)",

        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},

        "total": len(deduped),

        "counties": list(COUNTIES.keys()),

        "records": deduped,

    }



    os.makedirs("dashboard", exist_ok=True)

    os.makedirs("data", exist_ok=True)

    with open("dashboard/tomgreen_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    with open("data/tomgreen_records.json", "w") as f:

        json.dump(payload, f, indent=2, default=str)

    log.info("Saved -> dashboard/tomgreen_records.json")



def main():

    asyncio.run(main_async())



if __name__ == "__main__":

    main()

