"""
StackIQ — Fort Bend County Scraper (Aumentum Recorder)
Portal: ccweb.co.fort-bend.tx.us
System: Harris Recording Solutions / Aumentum
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fort_bend")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
BASE_URL      = "https://ccweb.co.fort-bend.tx.us"
SEARCH_URL    = f"{BASE_URL}/RealEstate/SearchEntry.aspx"

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
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()

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
    else:                            flags.append("Distress signal"); s += 10
    filed_str = r.get("filed", "")
    if filed_str:
        try:
            days_ago = (datetime.now() - datetime.strptime(filed_str, "%Y-%m-%d")).days
            if days_ago <= 7:  flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    owner = (r.get("owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","BANK","HOLDINGS"]):
        flags.append("LLC/Corp owner"); s += 10
    if r.get("prop_address","").strip():
        flags.append("Has address"); s += 5
    return min(s, 100), flags

async def scrape_fort_bend(start_dt, end_dt):
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

        for doc_type_name, cat, cat_label in DOC_TYPES:
            log.info("Fort Bend: searching %s", doc_type_name)
            try:
                await page.goto(SEARCH_URL, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)

                # Fill date range
                await page.fill("input[id*='DateFiledFrom'], input[name*='DateFiledFrom']", start_str)
                await page.fill("input[id*='DateFiledTo'], input[name*='DateFiledTo']", end_str)
                await page.wait_for_timeout(500)

                # Check the document type checkbox
                # Find checkbox by label text
                checked = False
                checkboxes = await page.query_selector_all("input[type='checkbox']")
                for cb in checkboxes:
                    try:
                        # Get the label/text near this checkbox
                        parent = await cb.evaluate_handle("el => el.parentElement")
                        text = await parent.evaluate("el => el.textContent")
                        if doc_type_name in text.upper():
                            await cb.check()
                            checked = True
                            log.info("  Checked: %s", doc_type_name)
                            break
                    except: continue

                if not checked:
                    log.warning("  Could not find checkbox for %s", doc_type_name)
                    continue

                await page.wait_for_timeout(500)

                # Click Search
                await page.click("input[value='Search'], input[type='submit'][value*='Search']", timeout=5000)
                await page.wait_for_timeout(3000)

                # Parse results pages
                page_num = 0
                while page_num < MAX_PAGES:
                    page_num += 1
                    content = await page.content()
                    new_records = parse_results_page(content, cat, cat_label)
                    records.extend(new_records)
                    log.info("  Fort Bend %s page %d: %d records", doc_type_name, page_num, len(new_records))

                    # Try next page
                    try:
                        next_btn = await page.query_selector("a[title='Go to next page'], img[alt='Next']")
                        if not next_btn:
                            # Try the page dropdown
                            sel = await page.query_selector("select[name*='Page']")
                            if sel:
                                opts = await sel.query_selector_all("option")
                                cur  = await sel.input_value()
                                cur_idx = next((i for i, o in enumerate(opts) if await o.get_attribute("value") == cur), -1)
                                if cur_idx >= 0 and cur_idx < len(opts) - 1:
                                    next_val = await opts[cur_idx + 1].get_attribute("value")
                                    await sel.select_option(next_val)
                                    await page.wait_for_timeout(3000)
                                    continue
                            break
                        await next_btn.click()
                        await page.wait_for_timeout(3000)
                    except:
                        break

            except Exception as e:
                log.warning("Fort Bend %s error: %s", doc_type_name, e)
                continue

        await browser.close()
    return records

def parse_results_page(html, cat, cat_label):
    from bs4 import BeautifulSoup
    records = []
    soup = BeautifulSoup(html, "lxml")

    # Find results table
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["th","td"])]
        if not any("instrument" in h or "document" in h for h in hdrs): continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4: continue
            try:
                # Col structure: #, Image, checkbox, Instrument#, Date Filed, Doc Type, Name/Assoc Name, Legal, Status
                doc_num  = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
                filed    = norm_date(cells[4].get_text(" ", strip=True)) if len(cells) > 4 else ""
                doc_type = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""
                name_cell = cells[6].get_text(" ", strip=True) if len(cells) > 6 else ""
                legal    = cells[7].get_text(" ", strip=True) if len(cells) > 7 else ""

                # Parse owner — [R] is grantor, [E] is grantee
                owner, grantee = "", ""
                for part in name_cell.split("\n"):
                    part = part.strip()
                    if part.startswith("[R]"):
                        owner = part.replace("[R]","").strip().rstrip("(+)").strip()
                    elif part.startswith("[E]"):
                        grantee = part.replace("[E]","").strip().rstrip("(+)").strip()

                if not doc_num or not filed: continue

                records.append({
                    "doc_num": doc_num, "doc_type": doc_type or cat_label,
                    "cat": cat, "cat_label": cat_label,
                    "filed": filed, "owner": owner, "grantee": grantee,
                    "amount": None, "legal": legal,
                    "clerk_url": f"{BASE_URL}/RealEstate/Details.aspx?doc={doc_num}",
                    "county": "Fort Bend",
                    "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
                    "score": 0, "flags": [],
                })
            except: continue
        if records: break
    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== Fort Bend County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = await scrape_fort_bend(cutoff, now)

    # Dedupe
    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['doc_num']}|{r['filed']}"
        if key not in seen:
            seen.add(key); deduped.append(r)

    # Score
    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)
    log.info("Total unique: %d", len(deduped))

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Fort Bend County Clerk (Aumentum)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": ["Fort Bend"],
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/fort_bend_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/fort_bend_records.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/fort_bend_records.json")

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
