"""
StackIQ - County Government Records (CGR) Scraper - httpx version
Portal: tx.countygovernmentrecords.com (EagleWeb/Tyler Technologies)
"""
import os, re, json, logging, asyncio, httpx
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("cgr")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
CGR_USER = os.getenv("CGR_USER", "")
CGR_PASS = os.getenv("CGR_PASS", "")

BASE = "https://tx.countygovernmentrecords.com"
LOGIN_URL = f"{BASE}/texas/web/loginPOST.jsp"
LIST_URL  = f"{BASE}/texas/landrecords/counties.jsp"
SEARCH_URL = f"{BASE}/texas/eagleweb/docSearch.jsp"
RESULTS_URL = f"{BASE}/texas/eagleweb/docSearchResults.jsp"

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
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(str(raw).strip()[:20], fmt).strftime("%Y-%m-%d")
        except: pass
    return str(raw).strip()[:10]

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

def parse_results_page(html, county, doc_type):
    soup = BeautifulSoup(html, "lxml")
    records = []
    # Find results table
    table = soup.find("table", class_=re.compile("result", re.I)) or soup.find("table")
    if not table: return records
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2: continue
        text = row.get_text(" ", strip=True)
        # Find doc number (format: NNN-NNNNNNN)
        doc_m = re.search(r"\b(\d{2,4}-\d{4,8})\b", text)
        if not doc_m: continue
        doc_num = doc_m.group(1)
        # Find date
        date_m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
        filed = norm_date(date_m.group(1)) if date_m else ""
        if filed and filed < "2025-01-01": continue
        # Find grantor
        grantor_cell = row.find(string=re.compile("Grantor:", re.I))
        if grantor_cell:
            grantor_td = grantor_cell.find_parent("td") or grantor_cell.find_parent("div")
            owner = grantor_td.get_text(strip=True).replace("Grantor:", "").strip() if grantor_td else ""
        else:
            # Try to find name from cells
            names = [c.get_text(strip=True) for c in cells if len(c.get_text(strip=True)) > 5 
                     and not re.match(r"\d{2}/\d{2}/\d{4}", c.get_text(strip=True))
                     and not re.match(r"\d+-\d+", c.get_text(strip=True))]
            owner = names[0] if names else ""
        if not owner or len(owner) < 3: continue
        cat, lbl = cat_from_doc_type(doc_type)
        rec = {
            "doc_num": doc_num, "doc_type": doc_type,
            "cat": cat, "cat_label": lbl,
            "filed": filed, "owner": owner, "grantee": "",
            "amount": None, "legal": "", "county": county.lower(),
            "clerk_url": RESULTS_URL,
            "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
            "score": 0, "flags": [],
        }
        rec["score"], rec["flags"] = compute_score(rec)
        records.append(rec)
    return records

import asyncio as _asyncio

async def main_async():
    now = datetime.now()
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    all_records = []

    # Step 1: Login with httpx to get session cookie
    login_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36"}
    with httpx.Client(headers=login_headers, follow_redirects=True, timeout=30) as client:
        r0 = client.get(f"{BASE}/texas/web/login.jsp")
        soup0 = BeautifulSoup(r0.text, "lxml")
        form = soup0.find("form")
        action = form.get("action", "") if form else ""
        jsid = action.split("jsessionid=")[-1] if "jsessionid=" in action else ""
        login_url = f"{BASE}/texas/web/loginPOST.jsp" + (f";jsessionid={jsid}" if jsid else "")
        r1 = client.post(login_url, data={"userId": CGR_USER, "password": CGR_PASS, "submit": "Login"})
        log.info("Login: %s %s", r1.status_code, r1.url)
        cookies = dict(client.cookies)
        if jsid:
            cookies["JSESSIONID"] = jsid
        log.info("Cookies: %s", list(cookies.keys()))

    # Step 2: Use Playwright with injected cookies
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36")
        # Inject session cookies
        pw_cookies = [{"name": k, "value": v, "domain": "tx.countygovernmentrecords.com", "path": "/"} for k,v in cookies.items()]
        await context.add_cookies(pw_cookies)
        page = await context.new_page()

        # Verify session works
        await page.goto(LIST_URL, timeout=30000)
        await page.wait_for_timeout(3000)
        try:
            await page.wait_for_selector("table", timeout=8000)
        except:
            pass
        links = await page.query_selector_all("a")
        log.info("County list: %d links at %s", len(links), page.url)

        if len(links) == 0:
            log.error("Session not working - no links found")
            await browser.close()
            return []

        for county in COUNTIES:
            # Find county link
            all_links = await page.query_selector_all("a")
            link = None
            for a in all_links:
                txt = (await a.inner_text()).strip()
                if county.lower() in txt.lower():
                    link = a
                    break
            if not link:
                log.warning("County not found: %s", county)
                continue

            await link.click()
            await page.wait_for_timeout(2000)
            log.info("Selected %s -> %s", county, page.url)

            for doc_type in DISTRESS_DOC_TYPES:
                try:
                    await page.goto(SEARCH_URL, timeout=30000)
                    await page.wait_for_timeout(1000)
                    chk = await page.query_selector("input[type='checkbox']")
                    if chk and await chk.is_checked():
                        await chk.click()
                        await page.wait_for_timeout(300)
                    selected = await page.evaluate(f"""() => {{
                        const opts = Array.from(document.querySelectorAll('option'));
                        const opt = opts.find(o => o.textContent.trim() === "{doc_type}");
                        if (opt) {{ opt.selected = true; return true; }}
                        return false;
                    }}""")
                    if not selected:
                        continue
                    await page.fill("input[name='RecDateIDStart']", cutoff.strftime("%m/%d/%Y"))
                    await page.fill("input[name='RecDateIDEnd']", now.strftime("%m/%d/%Y"))
                    await page.click("input[type='submit'][value='Search']")
                    await page.wait_for_timeout(2000)

                    page_num = 1
                    while page_num <= 10:
                        html = await page.content()
                        recs = parse_results_page(html, county, doc_type)
                        log.info("%s %s p%d: %d records", county, doc_type, page_num, len(recs))
                        all_records.extend(recs)
                        if not recs: break
                        nxt = await page.query_selector("a:has-text('Next')")
                        if not nxt: break
                        await nxt.click()
                        await page.wait_for_timeout(1500)
                        page_num += 1
                except Exception as e:
                    log.warning("%s %s: %s", county, doc_type, e)

            # Go back to county list for next county
            await page.goto(LIST_URL, timeout=30000)
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_selector("table", timeout=5000)
            except:
                pass

        await browser.close()

    seen, deduped = set(), []
    for rec in all_records:
        k = f"{rec['doc_num']}|{rec['county']}"
        if k not in seen:
            seen.add(k); deduped.append(rec)

    log.info("Total unique: %d", len(deduped))
    os.makedirs("dashboard", exist_ok=True)
    with open("dashboard/cgr_records.json", "w") as f:
        json.dump({"fetched_at": now.isoformat(), "source": "CGR", "total": len(deduped),
                   "counties": COUNTIES, "records": deduped}, f, indent=2, default=str)
    log.info("Saved -> dashboard/cgr_records.json")
    return deduped

def main():
    _asyncio.run(main_async())


if __name__ == "__main__":
    main()
