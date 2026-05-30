"""
StackIQ Multi-County Texas Scraper — Playwright (Fixed)
Uses exact field IDs found via debug run.
"""
import json, logging, re, os, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("multi_county")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "50"))
API_URL = os.getenv("API_URL", "https://api.stackiq.org/leads/bulk-import")

COUNTIES = {
    "Hidalgo":  "hidalgo.tx.publicsearch.us",
    "El Paso":  "elpaso.tx.publicsearch.us",
    "Nueces":   "nueces.tx.publicsearch.us",
    "Jefferson":"jefferson.tx.publicsearch.us",
}

def date_slices(start, end, days=1):
    from datetime import timedelta
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=days), end)
        yield cur, nxt
        cur = nxt

DOC_TYPES = [
    ("Lis Pendens",           "LP",      "Lis Pendens"),
    ("Tax Deed",              "TAXDEED", "Tax Deed"),
    ("Abstract of Judgment",  "JUD",     "Abstract of Judgment"),
    ("Mechanic Lien",         "LNMECH",  "Mechanic Lien"),
    ("Federal Tax Lien",      "LNFED",   "Federal Tax Lien"),
    ("State Tax Lien",        "LNSTATE", "State Tax Lien"),
    ("HOA Lien",              "LNHOA",   "HOA Lien"),
    ("Notice of Foreclosure", "NOFC",    "Notice of Foreclosure"),
    ("IRS Lien",              "LNIRS",   "IRS Lien"),
    ("Probate",               "PRO",     "Probate"),
    ("Divorce Decree",        "DIV",     "Divorce"),
]

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
        try: return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()

def parse_amount(raw):
    if not raw: return None
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except: return None

def compute_score(r, cutoff):
    s, flags = 0, []
    cat = r.get("cat","")
    # Base score by category
    if cat in ("TAXDEED","TAXLIEN"): flags.append("Tax Deed / IRS / Corp Lien"); s += 50
    elif cat in ("LNIRS","LNFED"): flags.append("Tax Deed / IRS / Corp Lien"); s += 45
    elif cat in ("JUD",): flags.append("Judgment Lien"); s += 35
    elif cat in ("LNHOA","LNMECH"): flags.append("HOA / Mechanic Lien"); s += 30
    elif cat in ("PRO",): flags.append("Probate / Estate"); s += 25
    elif cat in ("LP","NOFC","NOFD"): flags.append("Lis Pendens / Pre-foreclosure"); s += 20
    elif cat in ("LN","LNSTATE"): flags.append("Lien on record"); s += 20
    elif cat in ("DIV","BK"): flags.append("Divorce / Bankruptcy"); s += 15
    else: flags.append("Distress signal"); s += 10
    # Amount bonus
    amt = r.get("amount")
    if amt and amt > 100000: flags.append("Amount > $100k"); s += 15
    elif amt and amt > 50000: flags.append("Amount > $50k"); s += 10
    # Recency bonus
    filed_str = r.get("filed","")
    if filed_str:
        try:
            filed_dt = datetime.strptime(filed_str,"%Y-%m-%d")
            days_ago = (datetime.now() - filed_dt).days
            if days_ago <= 7: flags.append("New this week"); s += 10
            elif days_ago <= 30: flags.append("Filed this month"); s += 5
        except: pass
    # Absentee owner
    mail_state = (r.get("mail_state") or "").upper().strip()
    if mail_state and mail_state != "TX":
        flags.append("Absentee owner (out of state)"); s += 15
    # LLC / corp owner
    owner = (r.get("owner") or r.get("primary_owner") or "").upper()
    if any(k in owner for k in ["LLC","INC","CORP","TRUST","LTD","HOLDINGS","PROPERTIES","INVESTMENTS","BANK"]):
        flags.append("LLC / corp owner"); s += 10
    # Has address
    if r.get("prop_address","").strip():
        flags.append("Has address"); s += 5
    # Address + lien combo
    if r.get("prop_address","").strip() and len(flags) >= 2:
        flags.append("Address + Lien combo"); s += 10
    # Multi-flag bonus
    if len(flags) >= 3:
        s += (len(flags) - 2) * 3
        flags.append("3+ distress signals")
    return min(s, 100), flags

def blank_rec(county, doc_num, doc_type, cat, cat_label, filed, owner,
              grantee="", amount=None, legal="", url=""):
    return {
        "doc_num": doc_num, "doc_type": doc_type, "cat": cat,
        "cat_label": cat_label, "filed": filed, "owner": owner,
        "grantee": grantee, "amount": amount, "legal": legal,
        "clerk_url": url, "county": county,
        "prop_address":"","prop_city":"","prop_state":"TX","prop_zip":"",
        "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":"",
        "score": 0, "flags": [],
    }

def parse_results(records, county, doc_type, cat, cat_label, base, api_responses, page_content):
    # Parse from intercepted API responses
    for resp in api_responses:
        body = resp.get("body", {})
        items = (body.get("results") or body.get("instruments") or
                 body.get("hits",{}).get("hits") or body.get("data") or [])
        if isinstance(items, dict): items = items.get("hits",[])
        for item in items:
            if "_source" in item: item = item["_source"]
            fn      = str(item.get("instrumentNumber", item.get("docNumber", item.get("id",""))))
            filed   = norm_date(item.get("recordedDate", item.get("fileDate","")))
            owner   = str(item.get("grantor", item.get("grantorName", item.get("owner","")))).strip()
            grantee = str(item.get("grantee", item.get("granteeName",""))).strip()
            amt     = parse_amount(item.get("amount", item.get("consideration","")))
            legal   = str(item.get("legalDescription", item.get("legal",""))).strip()
            url     = f"{base}/doc/{fn}" if fn else ""
            rec = blank_rec(county, fn, doc_type, cat, cat_label, filed, owner, grantee, amt, legal, url)
            for k in ["mailAddress","mailingAddress","grantorAddress"]:
                if item.get(k): rec["mail_address"] = str(item[k]).strip(); break
            records.append(rec)

    # HTML table fallback
    if not api_responses and page_content:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page_content, "lxml")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2: continue
            hdrs = [td.get_text(" ",strip=True).lower() for td in rows[0].find_all(["th","td"])]
            if not any(k in " ".join(hdrs) for k in ["instrument","grantor","document","name"]): continue
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells: continue
                d = {hdrs[i]:cells[i].get_text(" ",strip=True) for i in range(min(len(hdrs),len(cells)))}
                def f(*keys):
                    for k in keys:
                        for h in hdrs:
                            if k in h:
                                v = d.get(h,"").strip()
                                if v: return v
                    return ""
                fn    = f("instrument","number","document","file")
                filed = norm_date(f("date","recorded","filed"))
                owner = f("grantor","owner","name")
                amt   = parse_amount(f("amount","consideration"))
                link  = next((a["href"] for cell in cells for a in cell.find_all("a",href=True) if a.get("href")),"")
                if link and not link.startswith("http"): link = base + link
                if fn or owner:
                    records.append(blank_rec(county, fn, doc_type, cat, cat_label, filed, owner, amount=amt, url=link))

async def scrape_county(name, host, start_dt, end_dt):
    log.info("%s - scraping %s to %s", name, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    all_records = []
    for slice_start, slice_end in date_slices(start_dt, end_dt, days=1):
        day_records = await _scrape_day(name, host, slice_start, slice_end)
        all_records.extend(day_records)
        log.info("%s %s: %d records", name, slice_start.strftime("%m/%d"), len(day_records))
    return all_records

async def _scrape_day(name, host, start_dt, end_dt):
    records = []
    base = f"https://{host}"
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900}
        )
        page = await context.new_page()

        for doc_type, cat, cat_label in DOC_TYPES:
            api_responses = []

            async def on_response(resp):
                if resp.status == 200 and any(x in resp.url for x in ["/search","/results","/instruments","/api"]):
                    try:
                        ct = resp.headers.get("content-type","")
                        if "json" in ct:
                            body = await resp.json()
                            api_responses.append({"url": resp.url, "body": body})
                    except: pass

            page.on("response", on_response)

            try:
                # Load advanced search page fresh each time
                await page.goto(f"{base}/search/advanced", wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)

                # Step 1: Select department via JS evaluation
                try:
                    await page.click("#department", timeout=5000)
                    await page.wait_for_timeout(1000)
                    # Use JS to click first visible dropdown option
                    await page.evaluate("""
                        () => {
                            const opts = document.querySelectorAll('[class*="option"], [class*="menu-item"], li[role="option"]');
                            for (const o of opts) {
                                const t = o.textContent.trim();
                                if (t.includes("Property") || t.includes("Real") || t.includes("Record")) {
                                    o.click(); return;
                                }
                            }
                            if (opts.length > 0) opts[0].click();
                        }
                    """)
                    await page.wait_for_timeout(1000)
                except: pass

                # Step 2: Fill recorded date range using exact IDs
                await page.fill("#recordedDateRange-start", start_str)
                await page.wait_for_timeout(300)
                await page.fill("#recordedDateRange-end", end_str)
                await page.wait_for_timeout(300)

                # Step 3: Type document type into docTypes input
                await page.click("#docTypes-input")
                await page.wait_for_timeout(300)
                await page.type("#docTypes-input", doc_type, delay=50)
                await page.wait_for_timeout(1500)

                # Click the first matching option in dropdown
                try:
                    option = await page.wait_for_selector(
                        ".react-tokenized-select__option, [class*='option'], [role='option']",
                        timeout=3000
                    )
                    if option:
                        await option.click()
                        await page.wait_for_timeout(500)
                except: pass

                # Step 4: Click Search button
                await page.click("button:has-text('Search')", timeout=5000)
                await page.wait_for_timeout(4000)

                # Paginate through all results
                page_num = 1
                while True:
                    page_content = await page.content()
                    before = len(records)
                    parse_results(records, name, doc_type, cat, cat_label, base, api_responses, page_content)
                    after = len(records)
                    new_on_page = after - before
                    log.info("%s %s page %d: %d new records", name, doc_type, page_num, new_on_page)

                    # Try to click Next page button
                    try:
                        next_btn = await page.query_selector("[aria-label='next page']")
                        if next_btn:
                            is_disabled = await next_btn.get_attribute("disabled")
                            if is_disabled is not None:
                                break
                            await next_btn.click()
                            await page.wait_for_timeout(3000)
                            api_responses.clear()
                            page_num += 1
                            if page_num > MAX_PAGES:
                                break
                        else:
                            break
                    except:
                        break

                log.info("%s %s: %d total new records", name, doc_type, after - before)

            except Exception as e:
                log.warning("%s %s error: %s", name, doc_type, e)
            finally:
                page.remove_listener("response", on_response)

            await page.wait_for_timeout(1000)

        await browser.close()

    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== StackIQ Multi-County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    # Run all counties in parallel
    tasks = [scrape_county(name, host, cutoff, now) for name, host in COUNTIES.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_records = []
    for name, result in zip(COUNTIES.keys(), results):
        if isinstance(result, Exception):
            log.error("%s failed: %s", name, result)
        else:
            all_records.extend(result)

    seen, deduped = set(), []
    for r in all_records:
        key = f"{r['county']}|{r.get('doc_num')}|{r.get('filed')}|{r.get('owner')}"
        if key not in seen:
            seen.add(key); deduped.append(r)

    log.info("Total unique: %d", len(deduped))

    for r in deduped:
        try: r["score"], r["flags"] = compute_score(r, cutoff)
        except: r["score"] = 10; r["flags"] = []

    deduped.sort(key=lambda x: x.get("score",0), reverse=True)

    payload = {
        "fetched_at": now.isoformat(),
        "source": "Multi-County TX Clerk Portals (Playwright)",
        "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
        "total": len(deduped),
        "counties": list({r["county"] for r in deduped}),
        "records": deduped,
    }

    os.makedirs("dashboard", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    with open("dashboard/multi_county_3_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/multi_county_3_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/multi_county_3_records.json")

    try:
        r = requests.post(API_URL, json=payload, timeout=120)
        log.info("API push: %d %s", r.status_code, r.text[:100])
    except Exception as e:
        log.warning("API push failed: %s", e)

    hot  = sum(1 for r in deduped if r.get("score",0) >= 70)
    warm = sum(1 for r in deduped if 40 <= r.get("score",0) < 70)
    log.info("=== Summary: Total=%d Hot=%d Warm=%d ===", len(deduped), hot, warm)
    for county in sorted({r["county"] for r in deduped}):
        count = sum(1 for r in deduped if r["county"] == county)
        log.info("  %s: %d", county, count)

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
