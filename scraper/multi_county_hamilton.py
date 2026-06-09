"""
StackIQ — Hamilton County Scraper (Tyler iDS Self Service Web)
Portal: hamiltoncountytx-web.tylerhost.net
System: Tyler Technologies Self Service Web v2023.1.49
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("hamilton")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "8"))
BASE_URL      = "https://hamiltoncountytx-web.tylerhost.net/web"
SEARCH_URL    = f"{BASE_URL}/search/official-records-search"

DOC_TYPES = [
    ("LIS PENDENS",           "LP",      "Lis Pendens"),
    ("TAX DEED",              "TAXDEED", "Tax Deed"),
    ("ABSTRACT OF JUDGMENT",  "JUD",     "Abstract of Judgment"),
    ("MECHANIC LIEN",         "LNMECH",  "Mechanic Lien"),
    ("FEDERAL TAX LIEN",      "LNFED",   "Federal Tax Lien"),
    ("STATE TAX LIEN",        "LNSTATE", "State Tax Lien"),
    ("HOA LIEN",              "LNHOA",   "HOA Lien"),
    ("NOTICE OF FORECLOSURE", "NOFC",    "Notice of Foreclosure"),
    ("IRS TAX LIEN",          "LNIRS",   "IRS Lien"),
    ("PROBATE",               "PRO",     "Probate"),
    ("DIVORCE DECREE",        "DIV",     "Divorce"),
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt[:8]).strftime("%Y-%m-%d")
        except: pass
    # Handle "06/08/2026 01:08 PM" format
    try: return datetime.strptime(str(raw).strip(), "%m/%d/%Y %I:%M %p").strftime("%Y-%m-%d")
    except: pass
    return str(raw).strip()[:10]

def compute_score(r, cutoff):
    s, flags = 0, []
    cat = r.get("cat", "")
    if cat in ("TAXDEED","TAXLIEN"): flags.append("Tax Deed"); s += 50
    elif cat in ("LNIRS","LNFED"):   flags.append("IRS/Fed Lien"); s += 45
    elif cat == "JUD":               flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"):  flags.append("HOA/Mech Lien"); s += 30
    elif cat == "PRO":               flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):       flags.append("Lis Pendens"); s += 20
    elif cat in ("LN","LNSTATE"):    flags.append("Lien"); s += 20
    elif cat == "DIV":               flags.append("Divorce"); s += 15
    else:                            flags.append("Distress signal"); s += 10
    filed_str = r.get("filed", "")
    if filed_str:
        try:
            days_ago = (datetime.now() - datetime.strptime(filed_str[:10], "%Y-%m-%d")).days
            if days_ago <= 7:  flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    owner = (r.get("owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK","HOLDINGS"]):
        flags.append("LLC/Corp owner"); s += 10
    return min(s, 100), flags

async def scrape_mclennan(start_dt, end_dt):
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

        # Step 1: Accept disclaimer
        try:
            await page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(2000)
            accept_btn = await page.query_selector("button:has-text('I Accept'), input[value='I Accept'], a:has-text('I Accept')")
            if accept_btn:
                await accept_btn.click()
                await page.wait_for_timeout(2000)
                log.info("McLennan: accepted disclaimer")
        except Exception as e:
            log.warning("McLennan: disclaimer accept error: %s", e)

        for doc_type_name, cat, cat_label in DOC_TYPES:
            log.info("McLennan: searching %s", doc_type_name)
            try:
                # Navigate to search page
                await page.goto(f"{BASE_URL}/search/official-records-search", wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)

                # Fill date range
                date_start = await page.query_selector("input[placeholder='mm/dd/yyyy']:first-of-type, input[id*='start'], input[id*='Start'], input[id*='RecordingDateStart']")
                if not date_start:
                    inputs = await page.query_selector_all("input[placeholder='mm/dd/yyyy']")
                    if len(inputs) >= 2:
                        date_start = inputs[0]
                        date_end   = inputs[1]

                if date_start:
                    await date_start.fill(start_str)
                    await page.wait_for_timeout(300)

                inputs = await page.query_selector_all("input[placeholder='mm/dd/yyyy']")
                if len(inputs) >= 2:
                    await inputs[1].fill(end_str)
                    await page.wait_for_timeout(300)

                # Type doc type into Document Types filter box
                doc_type_input = await page.query_selector("input[placeholder*='ocument'], input[id*='docType'], .document-type-search input, input[aria-label*='Document']")
                if not doc_type_input:
                    # Try finding by label
                    doc_type_input = await page.query_selector("input[class*='filter'], .filter-input")

                if doc_type_input:
                    await doc_type_input.click()
                    await doc_type_input.fill(doc_type_name[:6])  # Type first 6 chars to filter
                    await page.wait_for_timeout(1500)

                    # Click matching option in dropdown
                    option = await page.query_selector(f"li:has-text('{doc_type_name}'), div[class*='option']:has-text('{doc_type_name}'), span:has-text('{doc_type_name}')")
                    if option:
                        await option.click()
                        await page.wait_for_timeout(500)
                        log.info("  Selected doc type: %s", doc_type_name)
                    else:
                        # Try clicking any visible option
                        options = await page.query_selector_all("li[class*='item'], div[class*='option'], .list-item")
                        for opt in options:
                            text = await opt.text_content()
                            if doc_type_name in (text or "").upper():
                                await opt.click()
                                await page.wait_for_timeout(500)
                                break

                # Click Search button
                search_btn = await page.query_selector("button:has-text('Search'), input[type='submit']")
                if search_btn:
                    await search_btn.click()
                    await page.wait_for_timeout(4000)

                # Parse results — handle pagination
                page_num = 0
                while page_num < MAX_PAGES:
                    page_num += 1
                    content = await page.content()
                    new_records = parse_results(content, cat, cat_label)
                    records.extend(new_records)
                    log.info("  McLennan %s page %d: %d records", doc_type_name, page_num, len(new_records))

                    # Check for next page button
                    try:
                        next_btn = await page.query_selector("button[aria-label='next page'], button:has-text('Next'), a:has-text('Next >')")
                        if next_btn:
                            is_disabled = await next_btn.get_attribute("disabled")
                            if is_disabled is not None:
                                break
                            await next_btn.click()
                            await page.wait_for_timeout(3000)
                        else:
                            break
                    except:
                        break

            except Exception as e:
                log.warning("McLennan %s error: %s", doc_type_name, e)
                continue

        await browser.close()
    return records

def parse_results(html, cat, cat_label):
    from bs4 import BeautifulSoup
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Tyler iDS results are in card-like divs, not tables
    # Each result has doc number, recording date, grantor, grantee
    # Pattern: "2026018210 • ABSTRACT OF JUDGMENT"
    result_items = soup.find_all(class_=re.compile(r"result|record|item|card|row", re.I))

    # Also try finding by the bullet pattern in text
    if not result_items:
        # Fall back to finding all elements containing doc numbers
        result_items = soup.find_all(lambda tag: tag.name in ["div","li","tr"] and
                                      re.search(r'\d{10}\s*[•·]\s*[A-Z]', tag.get_text()))

    for item in result_items:
        try:
            text = item.get_text(" ", strip=True)
            # Extract doc number (10 digit number)
            doc_match = re.search(r'(\d{7,10})\s*[•·]\s*([A-Z ]+)', text)
            if not doc_match: continue

            doc_num  = doc_match.group(1)
            doc_type = doc_match.group(2).strip()

            # Extract recording date
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
            filed = norm_date(date_match.group(1)) if date_match else ""

            # Extract grantor and grantee
            grantor_match = re.search(r'Grantor\s+([A-Z][A-Z &,.\-]+?)(?:Grantee|Legal|$)', text)
            grantee_match = re.search(r'Grantee\s+([A-Z][A-Z &,.\-]+?)(?:Legal|$)', text)

            owner   = grantor_match.group(1).strip() if grantor_match else ""
            grantee = grantee_match.group(1).strip() if grantee_match else ""

            if not doc_num: continue

            records.append({
                "doc_num": doc_num,
                "doc_type": doc_type or cat_label,
                "cat": cat, "cat_label": cat_label,
                "filed": filed,
                "owner": owner,
                "grantee": grantee,
                "amount": None,
                "legal": "",
                "clerk_url": f"{BASE_URL}/doc/{doc_num}",
                "county": "Hamilton",
                "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                "score": 0, "flags": [],
            })
        except: continue

    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Hamilton County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = await scrape_mclennan(cutoff, now)

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
        "source": "Hamilton County Clerk (Tyler iDS)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": ["Hamilton"],
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/hamilton_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/hamilton_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/hamilton_records.json")

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
