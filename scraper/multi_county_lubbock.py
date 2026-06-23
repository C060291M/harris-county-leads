"""

StackIQ - Lubbock County EagleWeb Scraper

Portal: https://erecord.lubbockcounty.gov/recorder/web/

Requires Public Login, then searches by doc type

"""

import json, logging, re, os, asyncio

from datetime import datetime, timedelta

from playwright.async_api import async_playwright



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

log = logging.getLogger("lubbock")



LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

MAX_PAGES     = int(os.getenv("MAX_PAGES", "3"))



BASE_URL   = "https://erecord.lubbockcounty.gov/recorder"

LOGIN_URL  = f"{BASE_URL}/web/login.jsp"

SEARCH_URL = f"{BASE_URL}/eagleweb/docSearch.jsp"



# Doc type values from the __search_select dropdown

# Use full text names as shown in the dropdown

DISTRESS_DOC_TYPES = [

    "ABSTRACT OF JUDGMENT",

    "FEDERAL TAX LIEN",

    "JUDGMENT",

    "LIEN",

    "MECHANICS LIEN",

    "STATE TAX LIEN",

    "STATE ABSTRACT OF JUDGMENT",

    "DIVORCE",

    "PROBATE",

]



def norm_date(raw):

    if not raw: return ""

    m = re.search(r"(\d{2}/\d{2}/\d{4})", str(raw))

    if m:

        try: return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")

        except: pass

    return str(raw).strip()[:10]



def cat_from_doc_type(doc_type):

    dt = doc_type.upper()

    if "LIS PEN" in dt or dt == "LP":    return ("LP",     "Lis Pendens")

    if "FORECLOSURE" in dt:              return ("NOFC",   "Notice of Foreclosure")

    if "ABSTRACT" in dt or "JUDG" in dt: return ("JUD",   "Abstract of Judgment")

    if "FEDERAL" in dt or "FTL" in dt:  return ("LNFED",  "Federal Tax Lien")

    if "STATE TAX" in dt or "STL" in dt: return ("LNSTATE","State Tax Lien")

    if "MECHANIC" in dt or dt == "ML":  return ("LNMECH", "Mechanic Lien")

    if "PROBATE" in dt:                  return ("PRO",    "Probate")

    if "DIVORCE" in dt or dt == "DIV":  return ("DIV",    "Divorce")

    if "LIEN" in dt:                     return ("LN",     "Lien")

    return ("LN", doc_type)



def compute_score(r):

    s, flags = 0, []

    cat = r.get("cat","")

    if cat == "TAXDEED":             flags.append("Tax Deed"); s += 50

    elif cat in ("LNIRS","LNFED"):   flags.append("IRS/Fed Lien"); s += 45

    elif cat == "JUD":               flags.append("Judgment"); s += 35

    elif cat in ("LNHOA","LNMECH","LNSTATE"): flags.append("Lien"); s += 30

    elif cat == "PRO":               flags.append("Probate"); s += 25

    elif cat in ("LP","NOFC"):       flags.append("Lis Pendens"); s += 20

    elif cat in ("LN",):             flags.append("Lien"); s += 20

    elif cat == "DIV":               flags.append("Divorce"); s += 15

    filed_str = r.get("filed","")

    if filed_str:

        try:

            days_ago = (datetime.now() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days

            if days_ago <= 7:    flags.append("New this week"); s += 10

            elif days_ago <= 30: flags.append("Filed this month"); s += 5

        except: pass

    owner = (r.get("owner") or "").upper()

    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK"]):

        flags.append("LLC/Corp owner"); s += 10

    return min(s, 100), flags



async def scrape_lubbock(start_dt, end_dt):

    records = []

    start_str = start_dt.strftime("%m/%d/%Y")

    end_str   = end_dt.strftime("%m/%d/%Y")

    log.info("Lubbock: %s to %s", start_str, end_str)



    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])

        context = await browser.new_context(

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",

            viewport={"width": 1280, "height": 900}

        )

        page = await context.new_page()



        try:

            # Login as public

            await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

            await page.wait_for_timeout(1000)

            pub = await page.query_selector("input[value=\"Public Login\"]")

            if pub:

                await pub.evaluate("el => el.click()")

                await page.wait_for_timeout(2000)

                log.info("Lubbock: public login OK, at %s", page.url)



            # Search each doc type

            for doc_type in DISTRESS_DOC_TYPES:

                try:

                    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30000)

                    await page.wait_for_timeout(1000)



                    # Uncheck Search All Types

                    all_types = await page.query_selector("input[name=\"AllDocuments\"]")

                    if all_types and await all_types.is_checked():

                        await all_types.evaluate("el => el.click()")

                        await page.wait_for_timeout(300)

                        await page.evaluate("document.querySelector(\"select[name='__search_select']\"  ).style.display='block'")

                        await page.wait_for_timeout(500)



                    # Select doc type

                    select = await page.query_selector("select[name=\"__search_select\"]")

                    if select:

                        await select.select_option(label=doc_type)

                        await page.wait_for_timeout(300)



                    # Fill dates

                    await page.fill("input[name=\"RecDateIDStart\"]", start_str)

                    await page.fill("input[name=\"RecDateIDEnd\"]", end_str)

                    await page.wait_for_timeout(300)



                    # Search

                    await page.click("input[value=\"Search\"]")

                    await page.wait_for_timeout(3000)



                    # Parse results

                    from bs4 import BeautifulSoup

                    soup = BeautifulSoup(await page.content(), "lxml")



                    # Check result count

                    result_text = soup.get_text()

                    count_match = re.search(r"(\d+)\s+items? found", result_text)

                    total = int(count_match.group(1)) if count_match else 0

                    log.info("Lubbock %s: %d items", doc_type, total)

                    if total == 0:

                        continue



                    # Parse result rows

                    rows = soup.select("table tr")

                    for row in rows:

                        cells = row.find_all("td")

                        if len(cells) < 2: continue

                        desc = cells[0].get_text(" ", strip=True)

                        summary = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""



                        # Extract doc num from description

                        doc_num_match = re.search(r"(\d{4,}[-\d]*)", desc)

                        doc_num = doc_num_match.group(1) if doc_num_match else ""

                        if not doc_num: continue



                        # Extract date from summary

                        date_match = re.search(r"Filing Date:\s*([\d/]+)", summary)

                        filed = norm_date(date_match.group(1)) if date_match else ""



                        # Extract owner

                        party_match = re.search(r"Party One:\s*([^P\n]+)", summary)

                        owner = party_match.group(1).strip() if party_match else ""



                        cat, cat_label = cat_from_doc_type(doc_type)

                        records.append({

                            "doc_num": doc_num, "doc_type": doc_type,

                            "cat": cat, "cat_label": cat_label,

                            "filed": filed, "owner": owner, "grantee": "",

                            "amount": None, "legal": "",

                            "clerk_url": SEARCH_URL,

                            "county": "Lubbock",

                            "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",

                            "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",

                            "score": 0, "flags": [],

                        })



                except Exception as e:

                    log.warning("Lubbock %s: error %s", doc_type, e)



        except Exception as e:

            log.warning("Lubbock: fatal error: %s", e)

        finally:

            await browser.close()



    return records



def main():

    now    = datetime.now()

    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    log.info("=== Lubbock County Scraper ===")



    records = asyncio.run(scrape_lubbock(cutoff, now))



    seen, deduped = set(), []

    for r in records:

        key = f"{r['doc_num']}|Lubbock"

        if key not in seen:

            seen.add(key)

            r["score"], r["flags"] = compute_score(r)

            deduped.append(r)



    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    log.info("Lubbock: %d unique records", len(deduped))



    payload = {

        "fetched_at": now.isoformat(),

        "source": "Lubbock County Clerk (EagleWeb)",

        "total": len(deduped),

        "counties": ["Lubbock"],

        "records": deduped,

    }

    os.makedirs("dashboard", exist_ok=True)

    os.makedirs("data", exist_ok=True)

    with open("dashboard/lubbock_records.json","w") as f: json.dump(payload,f,indent=2,default=str)

    with open("data/lubbock_records.json","w") as f: json.dump(payload,f,indent=2,default=str)

    log.info("Saved -> dashboard/lubbock_records.json")



if __name__ == "__main__":

    main()

