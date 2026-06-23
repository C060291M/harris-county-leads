import json, logging, re, os, asyncio, time
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("tyler")

COUNTY        = os.getenv("COUNTY", "").strip()
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "5"))
MAX_RECORDS   = int(os.getenv("MAX_RECORDS", "500"))
WALL_MINUTES  = int(os.getenv("WALL_MINUTES", "75"))

COUNTY_REGISTRY = {
    "Andrews":     ("https://andrewscountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Aransas":     ("https://aransascountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Bastrop":     ("https://bastroptx-web.tylerhost.net/web",                 "DOCSEARCH144S1"),
    "Bowie":       ("https://bowiecountytx-web.tylerhost.net/web",             "DOCSEARCH149S1"),
    "Brazoria":    ("https://brazoriacountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Burnet":      ("https://burnetcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Calhoun":     ("https://calhouncountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Calhoun2":    ("https://calhouncountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Carson":      ("https://carsoncountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Colorado":    ("https://coloradocountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Comal":       ("https://comalcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Dallam":      ("https://dallamcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Delta":       ("https://deltacountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Donley":      ("https://donleycountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Eastland":    ("https://eastlandcountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Erath":       ("https://erathcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Ector":       ("https://ectorcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Fort Bend":   ("https://fortbendcountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Gonzales":    ("https://gonzalescountytx-web.tylerhost.net/web",          "DOCSEARCH782S3"),
    "Gregg":       ("https://greggcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Guadalupe":   ("https://guadalupecountytx-web.tylerhost.net/web",         "DOCSEARCH144S1"),
    "Hamilton":    ("https://hamiltoncountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Hardin":      ("https://hardincountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Harrison":    ("https://harrisoncountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Hays":        ("https://hayscountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Henderson":   ("https://hendersoncountytx-web.tylerhost.net/web",         "DOCSEARCH144S1"),
    "Hill":        ("https://hillcountytx-web.tylerhost.net/web",              "DOCSEARCH100427S1"),
    "Hood":        ("https://hoodcountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Howard":      ("https://howardcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Hunt":        ("https://huntcountytx-web.tylerhost.net/web",              "DOCSEARCH149S1"),
    "Jasper":      ("https://jaspercountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Karnes":      ("https://karnescountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Kaufman":     ("https://kaufmancountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Kimble":      ("https://kimblecountytx-web.tylerhost.net/web",            "DOCSEARCH419S1"),
    "Lamar":       ("https://lamarcountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Lavaca":      ("https://lavacacountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Liberty":     ("https://libertycountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "McLennan":    ("https://mclennancountytx-web.tylerhost.net/web",          "DOCSEARCH402S1"),
    "Mills":       ("https://millscountytx-web.tylerhost.net/web",             "DOCSEARCH419S1"),
    "Montgomery":  ("https://montgomerycountytx-web.tylerhost.net/web",        "DOCSEARCH144S1"),
    "Navarro":     ("https://navarrocountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Orange":      ("https://orangecountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "PaloPinto":   ("https://palopintocountytx-selfservice.tylerhost.net/web", "DOCSEARCH144S1"),
    "Panola":      ("https://panolacountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Pecos":       ("https://pecoscountytx-web.tylerhost.net/web",             "DOCSEARCH144S1"),
    "Polk":        ("https://polkcountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Potter":      ("https://pottercountytx-web.tylerhost.net/web",            "DOCSEARCH422S2"),
    "Randall":     ("https://randallcountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Rockwall":    ("https://rockwalltx-web.tylerhost.net/web",                "DOCSEARCH144S1"),
    "Scurry":      ("https://scurrycountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Somervell":   ("https://somervellcountytx-web.tylerhost.net/web",         "DOCSEARCH144S1"),
    "Taylor":      ("https://taylorcountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Upshur":      ("https://upshurcountytx-web.tylerhost.net/web",            "DOCSEARCH149S1"),
    "VanZandt":    ("https://vanzandtcountytx-web.tylerhost.net/web",          "DOCSEARCH144S1"),
    "Waller":      ("https://wallercountytx-web.tylerhost.net/web",            "DOCSEARCH144S1"),
    "Washington":  ("https://washingtoncountytx-web.tylerhost.net/web",        "DOCSEARCH144S1"),
    "Wichita":     ("https://wichitacountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Williamson":  ("https://williamsoncountytx-web.tylerhost.net/williamsonweb",        "DOCSEARCH149S1"),
    "Winkler":     ("https://winklercountytx-web.tylerhost.net/web",           "DOCSEARCH144S1"),
    "Wise":        ("https://wisecountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Wood":        ("https://woodcountytx-web.tylerhost.net/web",              "DOCSEARCH144S1"),
    "Yoakum":      ("https://yoakumcountytx-selfservice.tylerhost.net/web",    "DOCSEARCH144S1"),
}

KEEP_DOC_TYPES = {
    "LIS PENDENS","TAX DEED","ABSTRACT OF JUDGMENT","MECHANIC LIEN",
    "FEDERAL TAX LIEN","STATE TAX LIEN","HOA LIEN","NOTICE OF FORECLOSURE",
    "IRS TAX LIEN","PROBATE","DIVORCE","FORECLOSURE","JUDGMENT","LIEN",
    "NOTICE OF TRUSTEE SALE","HOSPITAL LIEN","MECHANIC",
}

DATE_FIELD_VARIANTS = [
    ("field_RecordingDateID_DOT_StartDate", "field_RecordingDateID_DOT_EndDate"),
    ("field_RecDateID_DOT_StartDate",       "field_RecDateID_DOT_EndDate"),
    ("field_RecordedDateID_DOT_StartDate",  "field_RecordedDateID_DOT_EndDate"),
    ("field_FilingDateID_DOT_StartDate",    "field_FilingDateID_DOT_EndDate"),
    ("field_ClerkDateID_DOT_StartDate",     "field_ClerkDateID_DOT_EndDate"),
    ("field_InstrumentDateID_DOT_StartDate","field_InstrumentDateID_DOT_EndDate"),
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip()[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def cat_from_doc_type(doc_type):
    dt = doc_type.upper()
    if "LIS PENDENS" in dt:                    return ("LP",      "Lis Pendens")
    if "TAX DEED" in dt:                       return ("TAXDEED", "Tax Deed")
    if "ABSTRACT" in dt or "JUDGMENT" in dt:   return ("JUD",     "Abstract of Judgment")
    if "MECHANIC" in dt:                       return ("LNMECH",  "Mechanic Lien")
    if "FEDERAL" in dt or "IRS" in dt:         return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in dt:                      return ("LNSTATE", "State Tax Lien")
    if "HOA" in dt:                            return ("LNHOA",   "HOA Lien")
    if "FORECLOSURE" in dt or "TRUSTEE" in dt: return ("NOFC",    "Notice of Foreclosure")
    if "PROBATE" in dt:                        return ("PRO",     "Probate")
    if "DIVORCE" in dt:                        return ("DIV",     "Divorce")
    if "LIEN" in dt:                           return ("LN",      "Lien")
    return ("LN", doc_type)

def parse_items(soup, county_name, base_url, docsearch):
    records = []
    items = soup.find_all("li", attrs={"data-documentid": True})
    for item in items:
        h1 = item.find("h1")
        if not h1: continue
        h1_text  = h1.get_text(" ", strip=True)
        h1_clean = " ".join(h1_text.split())
        if not any(k in h1_clean.upper() for k in KEEP_DOC_TYPES): continue
        parts   = re.split(r"[\u2022\xa0\s]{2,}", h1_clean)
        parts   = [p.strip() for p in parts if p.strip()]
        doc_num = parts[0] if parts else ""
        doc_num = re.split(r"[\u2022\xa0]", doc_num)[0].strip()
        doc_num = re.sub(r"\s+", " ", doc_num).strip()
        doc_type = parts[-1] if len(parts) > 1 else h1_clean
        if re.match(r"^\d{4}-\d+", doc_type): doc_type = h1_clean
        full_text = item.get_text(" ", strip=True)
        date_m    = re.search(r"(\d{2}/\d{2}/\d{4})", full_text)
        filed     = norm_date(date_m.group(1)) if date_m else ""
        grantor_m = re.search(r"Grantor\s+([A-Z][^\n]+?)(?:\s{2,}|Grantee|Recording)", full_text)
        owner     = grantor_m.group(1).strip() if grantor_m else ""
        amount_m  = re.search(r"\$[\d,]+\.?\d*", full_text)
        amount    = None
        if amount_m:
            try: amount = float(amount_m.group(0).replace("$","").replace(",",""))
            except: pass
        cat, cat_label = cat_from_doc_type(doc_type)
        records.append({
            "doc_num": doc_num, "doc_type": doc_type, "cat": cat, "cat_label": cat_label,
            "filed": filed, "owner": owner, "grantee": "", "amount": amount,
            "clerk_url": f"{base_url}/search/{docsearch}", "county": county_name,
            "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
            "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
            "score":0,"flags":[],
        })
    return records

async def navigate_to_search(page, base_url, docsearch):
    """Navigate to search page, handling ACTIONGROUP redirects."""
    search_url = f"{base_url}/search/{docsearch}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)
    current = page.url
    if "DOCSEARCH" in current:
        return current.split("/")[-1]
    log.info("Redirected to %s — navigating via home page", current)
    # Go home first
    await page.goto(base_url + "/", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)
    # Find and click action group link
    links = await page.query_selector_all("a[href*='ACTIONGROUP']")
    for link in links:
        text = (await link.inner_text()).strip().lower()
        if "official" in text or "record" in text or "public" in text:
            await link.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)
            break
    # Now find DOCSEARCH link
    links2 = await page.query_selector_all("a[href*='DOCSEARCH']")
    for link in links2:
        text = (await link.inner_text()).strip().lower()
        if "official" in text or "real estate" in text or "record" in text or "search" in text:
            href = await link.get_attribute("href") or ""
            actual_id = href.split("/")[-1]
            await link.evaluate("el => el.click()")
            await page.wait_for_timeout(2000)
            log.info("Found search via navigation: %s", actual_id)
            return actual_id
    return docsearch

async def fill_dates(page, start_str, end_str):
    """Try multiple date field name variants."""
    for start_field, end_field in DATE_FIELD_VARIANTS:
        el = await page.query_selector(f"input[name='{start_field}']")
        if el:
            await page.fill(f"input[name='{start_field}']", start_str)
            await page.fill(f"input[name='{end_field}']",   end_str)
            log.info("Date fields: %s", start_field)
            return True
    # Fallback: any input with date in name
    inputs = await page.query_selector_all("input[type='text']")
    date_inputs = []
    for inp in inputs:
        name = await inp.get_attribute("name") or ""
        if "date" in name.lower():
            date_inputs.append(inp)
    if len(date_inputs) >= 2:
        await date_inputs[0].fill(start_str)
        await date_inputs[1].fill(end_str)
        log.info("Date fields: fallback")
        return True
    return False

async def scrape_county(county_name, base_url, docsearch, start_dt, end_dt):
    from bs4 import BeautifulSoup
    records   = []
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")
    wall_start = time.monotonic()
    wall_limit = WALL_MINUTES * 60
    log.info("[%s] Scraping %s to %s | max_pages=%d max_records=%d wall=%dmin",
             county_name, start_str, end_str, MAX_PAGES, MAX_RECORDS, WALL_MINUTES)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900})
        page = await context.new_page()

        # Disclaimer
        try:
            await page.goto(base_url + "/user/disclaimer", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(1500)
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    b.removeAttribute('disabled');
                    if (b.textContent.match(/accept|agree|continue/i)) { b.click(); return; }
                }
                if (btns[0]) btns[0].click();
            }""")
            await page.wait_for_timeout(2000)
        except Exception as e:
            log.warning("[%s] Disclaimer: %s", county_name, e)

        # Navigate to search
        try:
            actual_docsearch = await navigate_to_search(page, base_url, docsearch)
        except Exception as e:
            log.error("[%s] Navigation failed: %s", county_name, e)
            await browser.close()
            return records

        # Verify on search page
        if "DOCSEARCH" not in page.url:
            log.error("[%s] Not on search page: %s", county_name, page.url)
            await browser.close()
            return records

        # Fill dates
        try:
            ok = await fill_dates(page, start_str, end_str)
            if not ok:
                log.error("[%s] Could not find date fields", county_name)
                await browser.close()
                return records
            await page.wait_for_timeout(500)

            # Click search
            search_btn = (
                await page.query_selector("a[href*='searchResults']") or
                await page.query_selector("a[href*='SearchResults']") or
                await page.query_selector("button[type='submit']") or
                await page.query_selector("input[type='submit']")
            )
            if not search_btn:
                log.error("[%s] No search button", county_name)
                await browser.close()
                return records
            await search_btn.evaluate("el => el.click()")
            await page.wait_for_timeout(2500)
        except Exception as e:
            log.error("[%s] Search setup failed: %s", county_name, e)
            await browser.close()
            return records

        # Paginate
        for page_num in range(1, MAX_PAGES + 1):
            elapsed = time.monotonic() - wall_start
            if elapsed >= wall_limit - 120:
                log.warning("[%s] Wall clock limit, stopping at page %d with %d records",
                            county_name, page_num, len(records))
                break
            if len(records) >= MAX_RECORDS:
                log.info("[%s] MAX_RECORDS hit", county_name)
                break
            log.info("[%s] Page %d/%d (%.0fs, %d records)", county_name, page_num, MAX_PAGES, elapsed, len(records))
            try:
                soup = BeautifulSoup(await page.content(), "lxml")
                page_recs = parse_items(soup, county_name, base_url, actual_docsearch)
                records.extend(page_recs)
                log.info("[%s] Page %d: %d records", county_name, page_num, len(page_recs))
                if not page_recs: break
                next_btn = (
                    await page.query_selector("a:has-text('Next')") or
                    await page.query_selector("button:has-text('Next')") or
                    await page.query_selector(".pagination-next a") or
                    await page.query_selector("li.next a")
                )
                if not next_btn: break
                await next_btn.evaluate("el => el.click()")
                await page.wait_for_timeout(2000)
            except Exception as e:
                log.warning("[%s] Page %d error: %s", county_name, page_num, e)
                break

        await browser.close()

    log.info("[%s] Done: %d records in %.0fs", county_name, len(records), time.monotonic() - wall_start)
    return records

async def main():
    if not COUNTY:
        log.error("COUNTY env var not set")
        raise SystemExit(1)
    config = COUNTY_REGISTRY.get(COUNTY) or next(
        (v for k,v in COUNTY_REGISTRY.items() if k.lower()==COUNTY.lower()), None)
    if not config:
        log.error("County not found: %s", COUNTY)
        raise SystemExit(1)
    base_url, docsearch = config
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    records = await scrape_county(COUNTY, base_url, docsearch, cutoff, now)
    seen, deduped = set(), []
    for r in records:
        k = r.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(r)
    log.info("[%s] %d unique records", COUNTY, len(deduped))
    out_dir  = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{COUNTY.lower().replace(' ','_')}_records.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"fetched_at": datetime.now().isoformat(),
                   "source": f"{COUNTY} County Clerk (Tyler iDS)",
                   "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
                   "total": len(deduped), "counties": [COUNTY], "records": deduped}, f, indent=2, default=str)
    log.info("[%s] Written to %s", COUNTY, out_file)

if __name__ == "__main__":
    asyncio.run(main())





