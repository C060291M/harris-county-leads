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
API_URL = os.getenv("API_URL", "https://api.stackiq.org/leads/bulk-import")

COUNTIES = {
    "Dallas":  "dallas.tx.publicsearch.us",
    "Tarrant": "tarrant.tx.publicsearch.us",
    "Bexar":   "bexar.tx.publicsearch.us",
    "Collin":  "collin.tx.publicsearch.us",
}

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
    if cat in ("TAXDEED","TAXLIEN","LNIRS","LNFED"): flags.append("Tax Deed / IRS / Corp Lien"); s += 30
    if cat in ("LNHOA",): flags.append("HOA / Mechanic Lien"); s += 25
    if cat in ("PRO",): flags.append("Probate / Estate"); s += 20
    if cat in ("LN","LNMECH","LNSTATE"): flags.append("Lien on record"); s += 15
    if cat in ("LP","NOFC","NOFD"): flags.append("Lis Pendens / Pre-foreclosure"); s += 10
    if cat in ("JUD",): flags.append("Judgment Lien"); s += 10
    if cat in ("DIV","BK"): flags.append("Divorce / Bankruptcy"); s += 10
    amt = r.get("amount")
    if amt and amt > 100000: flags.append("Amount > $100k"); s += 15
    elif amt and amt > 50000: flags.append("Amount > $50k"); s += 10
    filed_str = r.get("filed","")
    if filed_str:
        try:
            if datetime.strptime(filed_str,"%Y-%m-%d") >= cutoff: flags.append("New this week"); s += 5
        except: pass
    if (r.get("mail_state") or "").upper().strip() not in ("","TX"):
        flags.append("Absentee owner (out of state)"); s += 15
    if len(flags) >= 3: s += 10
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
    log.info("%s - scraping %s to %s", name, start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y"))
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
                    await page.wait_for_timeout(1500)
                    opts_text = await page.evaluate("""
                        () => Array.from(document.querySelectorAll('[class*="option"], li[role="option"], [class*="item"]')).map(o => o.textContent.trim()).filter(t => t.length > 0)
                    """)
                    log.info("%s dept options: %s", name, opts_text[:6])
                    clicked = await page.evaluate("""
                        () => {
                            const opts = Array.from(document.querySelectorAll('[class*="option"], li[role="option"], [class*="item"]'));
                            // Skip container divs — find options with short text (single department name)
                            const clean = opts.filter(o => {
                                const t = o.textContent.trim();
                                return t.length > 0 && t.length < 40;
                            });
                            for (const kw of ["Real Property","Property Records","Land Records","Official Records"]) {
                                for (const o of clean) {
                                    if (o.textContent.trim() === kw) { o.click(); return o.textContent.trim(); }
                                }
                            }
                            // Fallback: click first short option
                            if (clean.length > 0) { clean[0].click(); return clean[0].textContent.trim(); }
                            return "none";
                        }
                    """)
                    log.info("%s dept clicked: %s", name, clicked)
                    await page.wait_for_timeout(1000)
                except Exception as e:
                    log.warning("%s dept error: %s", name, e)

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

                # Change results per page to 200
                try:
                    await page.click("[aria-label*='Results Per Page']", timeout=3000)
                    await page.wait_for_timeout(500)
                    await page.click("text=200", timeout=2000)
                    await page.wait_for_timeout(3000)
                except: pass

                # Paginate using exact aria-label
                page_num = 1
                while True:
                    page_content = await page.content()
                    before = len(records)
                    parse_results(records, name, doc_type, cat, cat_label, base, api_responses, page_content)
                    after = len(records)
                    log.info("%s %s page %d: %d new records", name, doc_type, page_num, after - before)

                    # Click next page using exact aria-label found in debug
                    try:
                        next_btn = await page.query_selector("[aria-label='next page']")
                        if not next_btn:
                            break
                        is_disabled = await next_btn.get_attribute("disabled")
                        if is_disabled is not None:
                            break
                        # Check if it looks disabled by class
                        cls = await next_btn.get_attribute("class") or ""
                        # On last page the prev/next use disabled class
                        await next_btn.click()
                        await page.wait_for_timeout(3000)
                        api_responses.clear()
                        page_num += 1
                        if page_num > 20:
                            break
                    except:
                        break

                log.info("%s %s: %d total records", name, doc_type, after - before)

            except Exception as e:
                log.warning("%s %s error: %s", name, doc_type, e)
            finally:
                page.remove_listener("response", on_response)

            await page.wait_for_timeout(1000)

        await browser.close()

    log.info("%s total: %d records", name, len(records))
    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== StackIQ Multi-County Scraper ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = []
    for name, host in COUNTIES.items():
        try:
            recs = await scrape_county(name, host, cutoff, now)
            all_records.extend(recs)
        except Exception as e:
            log.error("%s failed: %s", name, e)

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
    with open("dashboard/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    with open("data/multi_county_records.json","w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info("Saved -> dashboard/multi_county_records.json")

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
