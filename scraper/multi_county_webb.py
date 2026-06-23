"""
StackIQ — Webb County Scraper (Kofile/GovOS countyfusion)
Portal: countyfusion13.govos.com/countyweb/
System: Kofile GovOS (Neumo-like interface)
Requires: Login as Public guest session
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("webb")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "8"))
BASE_URL      = "https://countyfusion13.govos.com/countyweb"
LOGIN_URL     = "https://webbcountytx.gov/CountyClerk/PropertyRecords/"

# Doc types to keep from "All Document Types" search
KEEP_DOC_TYPES = {
    "ABSTRACT", "FED LIEN", "LIEN", "NOTICE", "STATE LIEN",
    "ABSTRACT OF JUDGMENT", "LIS PENDENS", "MECHANIC LIEN",
    "FEDERAL TAX LIEN", "IRS LIEN", "HOA LIEN", "PROBATE",
    "TAX DEED", "NOTICE OF FORECLOSURE", "DIVORCE"
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "ABSTRACT" in dt or "JUDGMENT" in dt: return ("JUD", "Abstract of Judgment")
    if "FED LIEN" in dt or "FEDERAL" in dt:  return ("LNFED", "Federal Tax Lien")
    if "IRS" in dt:                           return ("LNIRS", "IRS Lien")
    if "STATE LIEN" in dt:                    return ("LNSTATE", "State Tax Lien")
    if "HOA" in dt:                           return ("LNHOA", "HOA Lien")
    if "MECHANIC" in dt:                      return ("LNMECH", "Mechanic Lien")
    if "LIS PENDENS" in dt:                   return ("LP", "Lis Pendens")
    if "FORECLOSURE" in dt:                   return ("NOFC", "Notice of Foreclosure")
    if "NOTICE" in dt:                        return ("NOFC", "Notice")
    if "TAX DEED" in dt:                      return ("TAXDEED", "Tax Deed")
    if "PROBATE" in dt:                       return ("PRO", "Probate")
    if "DIVORCE" in dt:                       return ("DIV", "Divorce")
    if "LIEN" in dt:                          return ("LN", "Lien")
    return ("LN", doc_type)

def compute_score(r, cutoff):
    s, flags = 0, []
    cat = r.get("cat", "")
    if cat in ("TAXDEED",):      flags.append("Tax Deed"); s += 50
    elif cat in ("LNIRS","LNFED"): flags.append("IRS/Fed Lien"); s += 45
    elif cat == "JUD":           flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"): flags.append("HOA/Mech Lien"); s += 30
    elif cat == "PRO":           flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"):   flags.append("Lis Pendens"); s += 20
    elif cat in ("LN","LNSTATE"): flags.append("Lien"); s += 20
    elif cat == "DIV":           flags.append("Divorce"); s += 15
    else:                        flags.append("Distress signal"); s += 10
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

async def scrape_webb(start_dt, end_dt):
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
        await page.set_viewport_size({"width": 1920, "height": 1080})

        # Step 1: Login as Public
        log.info("Webb: logging in as public...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        login_btn = await page.query_selector("input[value='Login as Public'], button:has-text('Login as Public')")
        if login_btn:
            await login_btn.evaluate("el => el.click()")
            await page.wait_for_timeout(3000)

        # Step 2: Accept disclaimer
        accept_btn = await page.query_selector("input[value='Accept'], button:has-text('Accept')")
        if accept_btn:
            await accept_btn.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)
            log.info("Webb: accepted disclaimer")

        # Step 3: Close notification popup
        try:
            close_btn = await page.query_selector("input[value='Close'], button:has-text('Close'), .close-btn")
            if close_btn:
                await close_btn.evaluate("el => el.click()")
                await page.wait_for_timeout(1000)
        except: pass

        # Step 4: Navigate to search
        search_link = await page.query_selector("a:has-text('Search Public Records')")
        if search_link:
            await search_link.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)

        # Step 5: Fill date range and search all doc types
        try:
            # Fill date from
            date_from = await page.query_selector("input[name*='dateFrom'], input[id*='dateFrom'], input[name*='DateFrom']")
            if date_from:
                await date_from.fill(start_str)
            else:
                # Try calendar icon approach - fill the text field near the calendar
                date_inputs = await page.query_selector_all("input[type='text'][size]")
                if len(date_inputs) >= 2:
                    await date_inputs[0].fill(start_str)
                    await date_inputs[1].fill(end_str)

            date_to = await page.query_selector("input[name*='dateTo'], input[id*='dateTo'], input[name*='DateTo']")
            if date_to:
                await date_to.fill(end_str)

            await page.wait_for_timeout(500)

            # Click Search
            search_btn = await page.query_selector("input[value='Search'], img[alt='Search'], a:has-text('Search')")
            if search_btn:
                await search_btn.evaluate("el => el.click()")
                await page.wait_for_timeout(2000)
                log.info("Webb: search submitted for %s to %s", start_str, end_str)

        except Exception as e:
            log.warning("Webb: search form error: %s", e)

        # Step 6: Parse pages
        page_num = 0
        while page_num < MAX_PAGES:
            page_num += 1
            content = await page.content()
            new_records = parse_results(content)
            records.extend(new_records)
            log.info("Webb page %d: %d records", page_num, len(new_records))

            # Try next page
            try:
                next_btn = await page.query_selector("img[alt='>|'], input[value='>'], a[title='Next Page']")
                if not next_btn:
                    # Try the arrow button
                    next_btn = await page.query_selector("input[type='image'][src*='next'], input[type='image'][src*='arrow']")
                if next_btn:
                    await next_btn.evaluate("el => el.click()")
                    await page.wait_for_timeout(3000)
            except:
                break

        await browser.close()
    return records

def parse_results(html):
    from bs4 import BeautifulSoup
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Find results table - look for table with Instrument # header
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [td.get_text(" ", strip=True).lower() for td in rows[0].find_all(["th","td"])]
        if not any("instrument" in h for h in hdrs): continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4: continue
            try:
                # Skip header/sub-header rows
                text = row.get_text(" ", strip=True)
                if not text or len(text) < 5: continue

                # Find instrument number link
                link = row.find("a")
                doc_num = link.get_text(strip=True) if link else ""
                if not doc_num or not re.match(r'\d+', doc_num): continue

                # Col layout: checkbox, Instrument#, Book, Page, DocType, Name(grantor), col, OtherName(grantee), Recorded, AssocDocs
                doc_type = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""
                name_cell = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""
                other_name = cells[7].get_text(" ", strip=True) if len(cells) > 7 else ""
                recorded = cells[8].get_text(" ", strip=True) if len(cells) > 8 else ""

                # Filter to distress doc types only
                dt_upper = doc_type.upper()
                if not any(k in dt_upper for k in ["ABSTRACT","LIEN","NOTICE","PROBATE","DEED","DIVORCE","LIS"]):
                    continue

                cat, cat_label = cat_from_doc_type(doc_type)
                filed = norm_date(recorded)

                # Get legal description from next row if present
                legal = ""
                next_row = row.find_next_sibling("tr")
                if next_row:
                    next_text = next_row.get_text(" ", strip=True)
                    if "Lot:" in next_text or "Prop Desc:" in next_text or "SDIV:" in next_text:
                        legal = next_text

                # Extract property address from legal if available
                prop_addr = ""
                addr_match = re.search(r'Prop Desc:([^L]+?)(?:Lot:|$)', legal)
                if addr_match:
                    prop_addr = addr_match.group(1).strip()

                records.append({
                    "doc_num": doc_num,
                    "doc_type": doc_type,
                    "cat": cat, "cat_label": cat_label,
                    "filed": filed,
                    "owner": name_cell,
                    "grantee": other_name,
                    "amount": None,
                    "legal": legal,
                    "clerk_url": f"{BASE_URL}/doc/{doc_num}",
                    "county": "Webb",
                    "prop_address": prop_addr,
                    "prop_city": "Laredo" if prop_addr else "",
                    "prop_state": "TX",
                    "prop_zip": "",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                    "score": 0, "flags": [],
                })
            except: continue
        if records: break
    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Webb County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = await scrape_webb(cutoff, now)

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
        "source": "Webb County Clerk (Kofile GovOS)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": ["Webb"],
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/webb_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/webb_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/webb_records.json")

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()

