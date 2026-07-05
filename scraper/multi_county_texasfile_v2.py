"""
StackIQ TexasFile Scraper v2 - rebuilt for TexasFile's 2026 UI
Uses Monthly Filings tab: county page -> Monthly Filings -> set year/month -> Search -> paginate
"""
import os, re, json, logging, asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("texasfile_v2")

TF_USER = os.getenv("TF_USER", "")
TF_PASS = os.getenv("TF_PASS", "")
COUNTY = os.getenv("COUNTY", "waller")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "35"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))

BASE = "https://www.texasfile.com"

DISTRESS_TYPES = {
    "LIS PENDENS", "ABSTRACT OF JUDGEMENT", "ABSTRACT OF JUDGMENT",
    "FEDERAL TAX LIEN", "MECHANICS LIEN", "MECHANICS LIEN AFFIDAVIT",
    "STATE TAX LIEN", "HOSPITAL LIEN", "JUDGMENT", "JUDGEMENT",
    "NOTICE OF TRUSTEE SALE", "TRUSTEE SALE", "FORECLOSURE",
    "TAX LIEN STATE", "TAX LIEN FEDERAL", "TAX LIEN",
}

def norm_date(raw):
    raw = re.sub(r"View.*$", "", str(raw)).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(raw[:10], fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def cat_from_type(t):
    t = t.upper()
    if "LIS PEN" in t: return ("LP", "Lis Pendens")
    if "ABSTRACT" in t or "JUDGEMENT" in t or "JUDGMENT" in t: return ("JUD", "Abstract of Judgment")
    if "FEDERAL" in t: return ("LNFED", "Federal Tax Lien")
    if "STATE TAX" in t: return ("LNSTATE", "State Tax Lien")
    if "MECHANIC" in t: return ("LNMECH", "Mechanic Lien")
    if "HOSPITAL" in t: return ("LN", "Hospital Lien")
    if "TRUSTEE" in t or "FORECLOS" in t: return ("NOFC", "Notice of Foreclosure")
    return ("LN", t.title())

def compute_score(r):
    s, flags = 0, []
    cat = r.get("cat", "")
    if cat == "LNFED": flags.append("Fed Tax Lien"); s += 45
    elif cat == "JUD": flags.append("Judgment"); s += 35
    elif cat == "LNMECH": flags.append("Mech Lien"); s += 30
    elif cat in ("LP", "NOFC"): flags.append("Lis Pendens"); s += 20
    elif cat == "LNSTATE": flags.append("State Tax Lien"); s += 20
    elif cat == "LN": flags.append("Lien"); s += 15
    filed = r.get("filed", "")
    if filed:
        try:
            days = (datetime.now() - datetime.strptime(filed[:10], "%Y-%m-%d")).days
            if days <= 7: flags.append("New this week"); s += 10
            elif days <= 30: flags.append("Filed this month"); s += 5
        except: pass
    return min(s, 100), flags

def parse_results_table(html, county):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []
    rows = table.find_all("tr")
    records = []
    i = 0
    while i < len(rows):
        cells = rows[i].find_all("td")
        texts = [c.get_text(" ", strip=True) for c in cells]
        # header row or malformed row check
        if len(texts) < 9 or texts[1] in ("Date Filed", ""):
            i += 1
            continue
        filed = norm_date(texts[1])
        doc_type = texts[2].upper()
        doc_num = texts[3].strip()
        grantor = texts[7].strip()
        grantee = texts[8].strip()

        legal = ""
        if i + 1 < len(rows):
            next_cells = rows[i+1].find_all("td")
            next_text = " ".join(c.get_text(" ", strip=True) for c in next_cells)
            m = re.search(r"Additional Information:\s*(.+)", next_text)
            if m:
                legal = m.group(1).strip()

        is_release = any(k in doc_type for k in ("RELEASE", "SATISFACTION", "DISCHARGE", "CANCELLATION", "WITHDRAWAL"))
        if any(d in doc_type for d in DISTRESS_TYPES) and not is_release:
            if filed and filed >= "2025-01-01" and doc_num and re.search(r"\d{4,}", doc_num):
                cat, lbl = cat_from_type(doc_type)
                rec = {
                    "doc_num": doc_num, "doc_type": doc_type,
                    "cat": cat, "cat_label": lbl,
                    "filed": filed, "owner": grantor, "grantee": grantee,
                    "amount": None, "legal": legal,
                    "county": county.lower().replace("-", " "),
                    "clerk_url": f"{BASE}/search/texas/{county}-county/county-clerk-records/",
                    "prop_address": "", "prop_city": "", "prop_state": "TX", "prop_zip": "",
                    "score": 0, "flags": [],
                }
                rec["score"], rec["flags"] = compute_score(rec)
                records.append(rec)
        i += 2  # skip the paired legal-description row
    return records

async def scrape_county_month(page, county_slug, year, month):
    url = f"{BASE}/search/texas/{county_slug}-county/county-clerk-records/"
    await page.goto(url, timeout=30000)
    await page.wait_for_timeout(2000)
    await page.click("text=Monthly Filings")
    await page.wait_for_timeout(1500)

    selects = await page.query_selector_all("select")
    visible_selects = []
    for s in selects:
        if await s.is_visible():
            visible_selects.append(s)
    if len(visible_selects) < 2:
        log.warning("[%s] Could not find year/month dropdowns", county_slug)
        return []
    await visible_selects[0].select_option(str(year))
    await visible_selects[1].select_option(str(month))
    await page.wait_for_timeout(500)

    buttons = await page.query_selector_all("button.new-search-btn")
    clicked = False
    for b in buttons:
        if await b.is_visible():
            await b.click()
            clicked = True
            break
    if not clicked:
        log.warning("[%s] Could not find Search button", county_slug)
        return []
    await page.wait_for_timeout(5000)

    all_records = []
    for page_num in range(1, MAX_PAGES + 1):
        html = await page.content()
        recs = parse_results_table(html, county_slug)
        all_records.extend(recs)
        log.info("[%s] %d-%02d page %d: %d distress records", county_slug, year, month, page_num, len(recs))

        next_arrow = await page.query_selector("li.Pagination-arrow:has-text('Next page')")
        if not next_arrow:
            break
        cls = await next_arrow.get_attribute("class") or ""
        if "is-disabled" in cls:
            break
        await next_arrow.click()
        await page.wait_for_timeout(3000)

    return all_records

async def main():
    if not TF_USER or not TF_PASS:
        log.error("TF_USER / TF_PASS not set")
        return

    county_slug = COUNTY.lower().replace(" ", "-")
    now = datetime.now()

    months = set()
    for d in range(0, LOOKBACK_DAYS + 32, 28):
        dt = now - timedelta(days=d)
        months.add((dt.year, dt.month))
    months = sorted(months, reverse=True)

    all_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = await browser.new_page(viewport={"width": 1280, "height": 900})

        await page.goto(f"{BASE}/")
        await page.wait_for_timeout(2000)
        await page.click("a.Nav-accentBtn")
        await page.wait_for_timeout(2000)
        await page.fill("input[name='username']", TF_USER)
        await page.fill("input[name='password']", TF_PASS)
        await page.click("button[type='submit']")
        await page.wait_for_timeout(3000)

        page_text = await page.evaluate("document.body.innerText")
        if "MY ACCOUNT" not in page_text.upper():
            log.error("[%s] Login failed - check TF_USER/TF_PASS", COUNTY)
            await browser.close()
            return
        log.info("[%s] Login OK", COUNTY)

        for year, month in months:
            try:
                recs = await scrape_county_month(page, county_slug, year, month)
                all_records.extend(recs)
            except Exception as e:
                log.warning("[%s] %d-%02d failed: %s", COUNTY, year, month, e)

        await browser.close()

    seen, deduped = set(), []
    for r in all_records:
        if r["doc_num"] not in seen:
            seen.add(r["doc_num"])
            deduped.append(r)

    out_dir = os.path.join(os.path.dirname(__file__), "..", "dashboard")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"texasfile_{county_slug}_records.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "fetched_at": now.isoformat(),
            "source": "TexasFile Monthly Filings v2",
            "county": county_slug,
            "total": len(deduped),
            "records": deduped,
        }, f, indent=2, default=str)
    log.info("[%s] Saved %d unique records -> %s", COUNTY, len(deduped), out_file)

if __name__ == "__main__":
    asyncio.run(main())