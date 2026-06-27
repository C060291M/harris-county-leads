import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("williamson")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "10"))
COUNTY        = "williamson"
BASE          = "https://williamsoncountytx-web.tylerhost.net/williamsonweb"

PROXY_USER = os.getenv("DECODO_USER", "")
PROXY_PASS = os.getenv("DECODO_PASS", "")

KEEP_TYPES = {"LIS PENDENS","ABSTRACT OF JUDGMENT","FEDERAL TAX LIEN",
              "MECHANICS LIEN","STATE TAX LIEN","JUDGMENT","PROBATE",
              "DEED IN LIEU OF FORECLOSURE","STATE OF TEXAS ABSTRACT OF JUDGMENT"}

def cat_from_type(dt):
    d = dt.upper()
    if "LIS PEN" in d:    return ("LP",      "Lis Pendens")
    if "FEDERAL" in d:    return ("LNFED",   "Federal Tax Lien")
    if "STATE TAX" in d:  return ("LNSTATE", "State Tax Lien")
    if "MECHANIC" in d:   return ("LNMECH",  "Mechanic Lien")
    if "PROBATE" in d:    return ("PRO",     "Probate")
    if "DEED IN LIEU" in d: return ("NOFC",  "Deed in Lieu")
    if "JUDGMENT" in d or "ABSTRACT" in d: return ("JUD", "Abstract of Judgment")
    return ("LN", dt)

def norm_date(raw):
    if not raw: return ""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", str(raw))
    if m:
        try: return datetime(int(m.group(3)),int(m.group(1)),int(m.group(2))).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

def parse_item(item):
    text = item.get_text(" ", strip=True)
    # Instrument number
    doc_num_m = re.search(r'\b(\d{10})\b', text)
    doc_num = doc_num_m.group(1) if doc_num_m else item.get("data-documentid","")
    # Date
    date_m = re.search(r'(\d{2}/\d{2}/\d{4})', text)
    filed = norm_date(date_m.group(1)) if date_m else ""
    # Doc type - look for known types
    doc_type = ""
    for t in sorted(KEEP_TYPES, key=len, reverse=True):
        if t in text.upper():
            doc_type = t
            break
    if not doc_type:
        # Try to get from heading element
        h = item.find(["h1","h2","h3","strong","b"])
        if h: doc_type = h.get_text(strip=True).upper()
    # Names
    grantor_m = re.search(r'Grantor\s+([A-Z][^\n•]+?)(?:\s{2,}|Grantee|Book)', text)
    grantee_m = re.search(r'Grantee(?:\s*\(\d+\))?\s+([A-Z][^\n•]+?)(?:\s{2,}|Legal|Book)', text)
    grantor = re.sub(r'\s+',' ', grantor_m.group(1)).strip() if grantor_m else ""
    grantee = re.sub(r'\s+',' ', grantee_m.group(1)).strip() if grantee_m else ""
    owner = grantee if grantee else grantor
    return doc_num, filed, doc_type, owner, grantor, grantee

async def scrape():
    now    = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    start  = f"{cutoff.month}/{cutoff.day}/{cutoff.year}"
    end    = f"{now.month}/{now.day}/{now.year}"
    log.info("[Williamson] Scraping %s to %s", start, end)

    launch_args = {"headless": True, "args": ["--no-sandbox","--disable-dev-shm-usage"]}
    if PROXY_USER:
        launch_args["proxy"] = {"server":"http://state.decodo.com:14001","username":PROXY_USER,"password":PROXY_PASS}
    WAIT = 3000 if PROXY_USER else 2000

    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_args)
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36")

        # Disclaimer
        await page.goto(f"{BASE}/user/disclaimer", timeout=60000)
        await page.wait_for_timeout(WAIT)
        await page.evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent.includes('Accept')).click()")
        await page.wait_for_timeout(WAIT)

        # Home -> Official Public Records
        await page.evaluate("Array.from(document.querySelectorAll('a')).find(l=>l.textContent.includes('Official Public Record')).click()")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(WAIT)
        log.info("[Williamson] Search page: %s", page.url)

        # Fill dates and search (no doc type filter - filter client side)
        await page.fill("input[name='field_RecDateID_DOT_StartDate']", start)
        await page.fill("input[name='field_RecDateID_DOT_EndDate']", end)
        await page.wait_for_timeout(500)
        await page.evaluate("Array.from(document.querySelectorAll('button,a')).find(b=>b.textContent.trim()==='Search').click()")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(WAIT)

        for page_num in range(1, MAX_PAGES + 1):
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            items = soup.find_all("li", attrs={"data-documentid": True})
            log.info("[Williamson] Page %d: %d items (url=%s)", page_num, len(items), page.url)

            for item in items:
                doc_num, filed, doc_type, owner, grantor, grantee = parse_item(item)
                if not doc_type or doc_type.upper() not in KEEP_TYPES: continue
                if not owner or len(owner) < 3: continue
                cat, cat_label = cat_from_type(doc_type)
                all_records.append({
                    "doc_num": doc_num, "doc_type": doc_type,
                    "cat": cat, "cat_label": cat_label,
                    "filed": filed, "owner": owner,
                    "grantee": grantee, "amount": None, "legal": "",
                    "county": COUNTY, "clerk_url": f"{BASE}/search/DOCSEARCH149S1",
                    "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                    "score": 0, "flags": [],
                })

            if not items: break
            # Next page
            next_btn = await page.query_selector("a:has-text('Next'), button:has-text('Next')")
            if not next_btn: break
            await next_btn.evaluate("el => el.click()")
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(WAIT)

        await browser.close()

    # Deduplicate
    seen, deduped = set(), []
    for r in all_records:
        k = r.get("doc_num","")
        if k and k not in seen:
            seen.add(k); deduped.append(r)

    log.info("[Williamson] %d unique distress records", len(deduped))
    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "williamson_records.json"), "w") as f:
        json.dump({
            "fetched_at": datetime.now().isoformat(),
            "source": "Williamson County Clerk",
            "date_range": {"start": cutoff.strftime("%Y-%m-%d"), "end": now.strftime("%Y-%m-%d")},
            "total": len(deduped), "counties": [COUNTY], "records": deduped
        }, f, indent=2, default=str)
    log.info("[Williamson] Done")
    return deduped

if __name__ == "__main__":
    asyncio.run(scrape())
