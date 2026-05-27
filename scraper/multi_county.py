"""
StackIQ Multi-County Texas Scraper — Playwright Edition
Uses real browser to scrape publicsearch.us for clean, accurate data.
Counties: Dallas, Tarrant, Bexar, Travis, Collin
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
    "Travis":  "travis.tx.publicsearch.us",
    "Collin":  "collin.tx.publicsearch.us",
}

DOC_TYPES = [
    "Lis Pendens",
    "Tax Deed",
    "Abstract of Judgment",
    "Mechanic Lien",
    "Federal Tax Lien",
    "State Tax Lien",
    "HOA Lien",
    "Notice of Foreclosure",
    "IRS Lien",
    "Probate",
    "Divorce Decree",
]

CAT_MAP = {
    "Lis Pendens":           ("LP",      "Lis Pendens"),
    "Tax Deed":              ("TAXDEED", "Tax Deed"),
    "Abstract of Judgment":  ("JUD",     "Abstract of Judgment"),
    "Mechanic Lien":         ("LNMECH",  "Mechanic Lien"),
    "Federal Tax Lien":      ("LNFED",   "Federal Tax Lien"),
    "State Tax Lien":        ("LNSTATE", "State Tax Lien"),
    "HOA Lien":              ("LNHOA",   "HOA Lien"),
    "Notice of Foreclosure": ("NOFC",    "Notice of Foreclosure"),
    "IRS Lien":              ("LNIRS",   "IRS Lien"),
    "Probate":               ("PRO",     "Probate"),
    "Divorce Decree":        ("DIV",     "Divorce"),
}

def norm_date(raw):
    if not raw: return ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%Y/%m/%d","%d/%m/%Y"):
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

async def scrape_county_playwright(name, host, start_dt, end_dt):
    log.info("%s - browser scraping %s to %s", name, start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y"))
    records = []
    base = f"https://{host}"
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Intercept API responses
        api_responses = []
        async def handle_response(response):
            url = response.url
            if "api" in url and response.status == 200:
                try:
                    ct = response.headers.get("content-type","")
                    if "json" in ct:
                        body = await response.json()
                        api_responses.append({"url": url, "body": body})
                except: pass
        page.on("response", handle_response)

        for doc_type in DOC_TYPES:
            cat, cat_label = CAT_MAP.get(doc_type, (None, None))
            if not cat: continue
            try:
                api_responses.clear()
                # Go to advanced search
                await page.goto(f"{base}/search/advanced", wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)

                # Select document type
                try:
                    # Try dropdown/select
                    await page.select_option("select[name*='docType'], select[id*='docType'], select[placeholder*='type']", label=doc_type, timeout=3000)
                except:
                    try:
                        # Try typing in search box
                        dt_input = await page.query_selector("input[placeholder*='type'], input[placeholder*='Type'], input[name*='docType']")
                        if dt_input:
                            await dt_input.click()
                            await dt_input.fill(doc_type)
                            await page.wait_for_timeout(1000)
                            # Click first dropdown option
                            option = await page.query_selector(".dropdown-item, .suggestion, li[role='option']")
                            if option: await option.click()
                    except: pass

                # Fill date range
                try:
                    date_inputs = await page.query_selector_all("input[type='date'], input[placeholder*='date'], input[placeholder*='Date'], input[name*='date'], input[name*='Date']")
                    if len(date_inputs) >= 2:
                        await date_inputs[0].fill(start_dt.strftime("%Y-%m-%d"))
                        await date_inputs[1].fill(end_dt.strftime("%Y-%m-%d"))
                    elif len(date_inputs) == 1:
                        await date_inputs[0].fill(start_dt.strftime("%Y-%m-%d"))
                except: pass

                # Click search
                try:
                    await page.click("button[type='submit'], button:has-text('Search'), input[type='submit']", timeout=3000)
                    await page.wait_for_timeout(3000)
                except: pass

                # Parse API responses captured
                for resp in api_responses:
                    body = resp["body"]
                    items = (body.get("results") or body.get("instruments") or
                             body.get("hits",{}).get("hits") or body.get("data") or [])
                    if isinstance(items, dict): items = items.get("hits",[])
                    for item in items:
                        if "_source" in item: item = item["_source"]
                        fn      = str(item.get("instrumentNumber", item.get("docNumber", item.get("id",""))))
                        filed   = norm_date(item.get("recordedDate", item.get("fileDate", item.get("date",""))))
                        owner   = str(item.get("grantor", item.get("grantorName", item.get("owner","")))).strip()
                        grantee = str(item.get("grantee", item.get("granteeName",""))).strip()
                        amt     = parse_amount(item.get("amount", item.get("consideration","")))
                        legal   = str(item.get("legalDescription", item.get("legal",""))).strip()
                        url     = f"{base}/doc/{fn}" if fn else ""
                        rec = blank_rec(name, fn, doc_type, cat, cat_label, filed, owner, grantee, amt, legal, url)
                        for k in ["mailAddress","mailingAddress","grantorAddress","address"]:
                            if item.get(k): rec["mail_address"] = str(item[k]).strip(); break
                        records.append(rec)

                # Also parse HTML table results as fallback
                if not api_responses:
                    await page.wait_for_timeout(2000)
                    rows = await page.query_selector_all("table tbody tr, .result-row, .search-result")
                    for row in rows:
                        try:
                            cells = await row.query_selector_all("td, .cell")
                            texts = [await c.inner_text() for c in cells]
                            if len(texts) < 3: continue
                            links = await row.query_selector_all("a")
                            link_href = ""
                            if links:
                                link_href = await links[0].get_attribute("href") or ""
                                if link_href and not link_href.startswith("http"):
                                    link_href = base + link_href
                            # Best-guess column mapping
                            fn    = texts[0].strip() if texts else ""
                            filed = norm_date(texts[1].strip()) if len(texts) > 1 else ""
                            owner = texts[2].strip() if len(texts) > 2 else ""
                            amt   = parse_amount(texts[4]) if len(texts) > 4 else None
                            if fn or owner:
                                records.append(blank_rec(name, fn, doc_type, cat, cat_label, filed, owner, amount=amt, url=link_href))
                        except: pass

                log.info("%s %s: %d records so far", name, doc_type, len(records))
                await page.wait_for_timeout(1500)

            except Exception as e:
                log.warning("%s %s error: %s", name, doc_type, e)

        await browser.close()

    log.info("%s total: %d records", name, len(records))
    return records

async def main_async():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    log.info("=== StackIQ Multi-County Scraper (Playwright) ===")
    log.info("Date range: %s to %s", cutoff.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"))

    all_records = []
    for name, host in COUNTIES.items():
        try:
            recs = await scrape_county_playwright(name, host, cutoff, now)
            all_records.extend(recs)
            log.info("+ %s: %d records", name, len(recs))
        except Exception as e:
            log.error("x %s failed: %s", name, e)

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
