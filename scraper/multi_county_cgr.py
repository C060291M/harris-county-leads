"""
StackIQ - County Government Records (CGR) Scraper
Portal: tx.countygovernmentrecords.com
Counties: Waller, Carson, Jasper, Karnes, Pecos, Van Zandt, 
          Caldwell, Calhoun, Hale, Morris, Ochiltree, Stephens, Titus
"""
import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("cgr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "10"))
CGR_USER      = os.getenv("CGR_USER", "")
CGR_PASS      = os.getenv("CGR_PASS", "")

BASE      = "https://tx.countygovernmentrecords.com"
LOGIN_URL = f"{BASE}/texas/web/loginPOST.jsp"
LIST_URL  = f"{BASE}/texas/landrecords/counties.jsp"
SEARCH_URL= f"{BASE}/texas/eagleweb/docSearch.jsp"

_ENV_COUNTIES = os.getenv("COUNTIES", "")
ALL_COUNTIES = [
    "Waller","Carson","Jasper","Karnes","Pecos","Van Zandt",
    "Caldwell","Calhoun","Hale","Morris","Ochiltree","Stephens","Titus"
]
COUNTIES = [c.strip() for c in _ENV_COUNTIES.split(",")] if _ENV_COUNTIES.strip() else ALL_COUNTIES

DISTRESS_DOC_TYPES = [
    "Lis Pendens Notice",
    "Abstract of Judgment",
    "Federal Tax Lien",
    "Mechanic's Lien Contract",
    "Mechanic's Lien with Assignment",
    "State Tax Lien",
    "Hospital Lien",
    "Judgment",
    "Notice of Foreclosure",
    "Probate",
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def cat_from_doc_type(dt):
    d = dt.upper()
    if "LIS PEN" in d: return ("LP", "Lis Pendens")
    if "ABSTRACT" in d or "JUDGMENT" in d: return ("JUD", "Abstract of Judgment")
    if "FEDERAL" in d: return ("LNFED", "Federal Tax Lien")
    if "STATE TAX" in d: return ("LNSTATE", "State Tax Lien")
    if "MECHANIC" in d: return ("LNMECH", "Mechanic Lien")
    if "HOSPITAL" in d: return ("LN", "Hospital Lien")
    if "PROBATE" in d: return ("PRO", "Probate")
    if "FORECLOSURE" in d: return ("NOFC", "Notice of Foreclosure")
    return ("LN", dt)

def compute_score(r):
    s, flags = 0, []
    cat = r.get("cat","")
    if cat == "LNFED": flags.append("Fed Tax Lien"); s += 45
    elif cat == "JUD": flags.append("Judgment"); s += 35
    elif cat == "LNMECH": flags.append("Mech Lien"); s += 30
    elif cat == "PRO": flags.append("Probate"); s += 25
    elif cat in ("LP","NOFC"): flags.append("Lis Pendens"); s += 20
    elif cat == "LNSTATE": flags.append("State Tax Lien"); s += 20
    elif cat == "LN": flags.append("Lien"); s += 15
    filed = r.get("filed","")
    if filed:
        try:
            days = (datetime.now() - datetime.strptime(filed[:10], "%Y-%m-%d")).days
            if days <= 7: flags.append("New this week"); s += 10
            elif days <= 30: flags.append("Filed this month"); s += 5
        except: pass
    return min(s, 100), flags

async def login(page):
    await page.goto(f"{BASE}/texas/web/login.jsp", timeout=30000)
    await page.wait_for_timeout(1000)
    await page.fill("input[name='userId']", CGR_USER)
    await page.fill("input[name='password']", CGR_PASS)
    await page.click("input[type='submit']")
    await page.wait_for_timeout(3000)
    log.info("Login: %s", page.url)
    # Navigate to county list to establish session
    await page.goto(LIST_URL, timeout=30000)
    await page.wait_for_timeout(3000)
    links = await page.query_selector_all("a")
    log.info("County list links: %d", len(links))
    return page

async def select_county(page, county):
    # Always navigate to county list
    await page.goto(LIST_URL, timeout=30000)
    await page.wait_for_timeout(3000)
    # Re-login if needed
    if "login" in page.url.lower():
        log.info("Re-logging in for %s", county)
        await page.fill("input[name='userId']", CGR_USER)
        await page.fill("input[name='password']", CGR_PASS)
        await page.click("input[type='submit']")
        await page.wait_for_timeout(3000)
        await page.goto(LIST_URL, timeout=30000)
        await page.wait_for_timeout(5000)
    # Wait for county table to render
    try:
        await page.wait_for_selector("table tr", timeout=10000)
        await page.wait_for_timeout(1000)
    except:
        await page.wait_for_timeout(5000)
    n_links = len(await page.query_selector_all("a"))
    log.info("County list: %d links at %s", n_links, page.url)
    # Get all links on page and find match
    all_links = await page.query_selector_all("a")
    link = None
    for a in all_links:
        txt = (await a.inner_text()).strip()
        if county.lower() in txt.lower():
            log.info("Found county link: %s", txt)
            link = a
            break
    if not link:
        all_texts = [(await a.inner_text()).strip() for a in all_links]
        log.warning("Available links: %s", all_texts[:20])
    if link:
        await link.click()
        await page.wait_for_timeout(2000)
        log.info("Selected county: %s -> %s", county, page.url)
        return True
    log.warning("County not found: %s", county)
    return False

async def scrape_county(page, county, start_dt, end_dt):
    records = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str = end_dt.strftime("%m/%d/%Y")

    if not await select_county(page, county):
        return records

    for doc_type in DISTRESS_DOC_TYPES:
        try:
            await page.goto(SEARCH_URL, timeout=30000)
            await page.wait_for_timeout(1000)

            # Uncheck "Search All Document Types"
            chk = await page.query_selector("input[type='checkbox']")
            if chk and await chk.is_checked():
                await chk.click()
                await page.wait_for_timeout(500)

            # Select doc type
            # Use evaluate to find option (avoids apostrophe CSS issues)
            selected = await page.evaluate(f"""() => {{
                const opts = Array.from(document.querySelectorAll('option'));
                const opt = opts.find(o => o.textContent.trim() === "{doc_type}");
                if (opt) {{ opt.selected = true; return true; }}
                return false;
            }}""")
            if not selected:
                continue

            # Fill dates
            await page.fill("input[name='RecDateIDStart']", start_str)
            await page.fill("input[name='RecDateIDEnd']", end_str)

            # Search
            await page.click("input[type='submit'][value='Search']")
            await page.wait_for_timeout(2000)

            # Parse results
            page_num = 1
            while page_num <= MAX_PAGES:
                rows = await page.query_selector_all("table.resultsTable tr, table tr")
                count = 0
                for row in rows:
                    cells = await row.query_selector_all("td")
                    if len(cells) < 3: continue
                    texts = [await c.inner_text() for c in cells]
                    texts = [t.strip() for t in texts]

                    # Find doc num (numeric)
                    doc_num = next((t for t in texts if re.match(r'^\d{6,}$', t)), "")
                    if not doc_num: continue

                    # Find date
                    filed = next((norm_date(t) for t in texts if re.match(r'\d{2}/\d{2}/\d{4}', t) and norm_date(t)), "")

                    # Find names - skip short/numeric/date cells
                    names = [t for t in texts if len(t) > 3 and not re.match(r'^\d+[/\-]?\d*$', t) and t != doc_num]
                    grantor = names[0] if names else ""
                    grantee = names[1] if len(names) > 1 else ""

                    if not grantor or len(grantor) < 3: continue
                    if not filed or filed < "2025-01-01": continue

                    cat, lbl = cat_from_doc_type(doc_type)
                    rec = {
                        "doc_num": doc_num, "doc_type": doc_type,
                        "cat": cat, "cat_label": lbl,
                        "filed": filed, "owner": grantor, "grantee": grantee,
                        "amount": None, "legal": "",
                        "county": county.lower(),
                        "clerk_url": SEARCH_URL,
                        "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                        "score": 0, "flags": [],
                    }
                    rec["score"], rec["flags"] = compute_score(rec)
                    records.append(rec)
                    count += 1

                log.info("%s %s: page %d - %d records", county, doc_type, page_num, count)
                if count == 0: break

                # Next page
                nxt = await page.query_selector("a:has-text('Next')")
                if not nxt: break
                await nxt.click()
                await page.wait_for_timeout(1500)
                page_num += 1

        except Exception as e:
            log.warning("%s %s: %s", county, doc_type, e)

    log.info("%s: %d total records", county, len(records))
    return records

async def main():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36")
        page = await context.new_page()

        await login(page)

        for county in COUNTIES:
            recs = await scrape_county(page, county, cutoff, now)
            all_records.extend(recs)

        await browser.close()

    # Deduplicate
    seen, deduped = set(), []
    for r in all_records:
        k = f"{r['doc_num']}|{r['county']}"
        if k not in seen:
            seen.add(k); deduped.append(r)

    log.info("Total unique: %d", len(deduped))

    os.makedirs("dashboard", exist_ok=True)
    fname = "cgr_records.json"
    with open(f"dashboard/{fname}", "w") as f:
        json.dump({
            "fetched_at": now.isoformat(),
            "source": "County Government Records",
            "total": len(deduped),
            "counties": COUNTIES,
            "records": deduped
        }, f, indent=2, default=str)
    log.info("Saved -> dashboard/%s", fname)

if __name__ == "__main__":
    asyncio.run(main())
