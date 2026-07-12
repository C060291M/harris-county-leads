import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import asyncio, psycopg2, re, logging
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.environ["DATABASE_URL"]
LIMIT = 100

# Counties confirmed live on this shared TaxNetUSA "Free Public Search" platform
COUNTIES = {
    "anderson":  "https://www.andersoncad.org",
    "hardin":    "https://www.hardincad.org",
    "kaufman":   "https://www.kaufmancad.org",
    "henderson": "https://www.hendersoncad.org",
    "hays":      "https://www.hayscad.org",
    "hood":      "https://www.hoodcad.org",
    "comal":     "https://www.comalcad.org",
    "wise":      "https://www.wisecad.org",
}

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

def last_name_from_owner(owner):
    parts = owner.strip().upper().split()
    for word in parts:
        if len(word) >= 4 and word.isalpha():
            return word
    return parts[0] if parts else owner

def is_real_address(addr):
    if not addr or len(addr) < 6:
        return False
    return bool(re.search(r"\d", addr))

async def search_taxnetusa(page, base, last_name):
    await page.goto(base + "/", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    field = await page.query_selector("#name-addr-acctno")
    if not field:
        return None
    await field.click()
    await field.fill("")
    await field.type(last_name, delay=60)
    await page.wait_for_timeout(300)
    await field.press("Enter")
    await page.wait_for_timeout(2500)

    soup = BeautifulSoup(await page.content(), "lxml")
    body_text = soup.get_text(" ", strip=True)
    if "more than 100 results" in body_text.lower():
        return None  # too broad, skip rather than guess wrong

    table = soup.find("table")
    if not table:
        return None
    rows = table.find_all("tr")
    if len(rows) < 2:
        return None
    headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]
    cells = rows[1].find_all(["td", "th"])
    try:
        addr_idx = headers.index("Street Address")
        addr_raw = cells[addr_idx].get_text(" ", strip=True)
        addr = re.sub(r"\s+", " ", addr_raw).strip() if addr_raw else None
        return addr if is_real_address(addr) else None
    except (ValueError, IndexError):
        return None

async def enrich_county(browser, cur, conn, county, base, leads):
    updated = 0
    context = await browser.new_context(viewport={"width": 1280, "height": 900})
    page = await context.new_page()

    for lead_id, owner in leads:
        try:
            last = last_name_from_owner(owner)
            if len(last) < 3:
                continue

            addr = await search_taxnetusa(page, base, last)

            if addr:
                cur.execute("""
                    UPDATE lead_records SET
                        prop_address=COALESCE(NULLIF(%s,''),prop_address),
                        cad_enriched_at=NOW()
                    WHERE id=%s
                """, (addr, lead_id))
                updated += 1
                if updated % 10 == 0:
                    conn.commit()
                    logger.info(f"{county}: {updated} enriched so far (committed)")

        except Exception as e:
            logger.warning(f"{county} lead {lead_id}: {e}")
            continue

    await page.close()
    await context.close()
    return updated

async def main():
    conn = get_conn(); cur = conn.cursor()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for county, base in COUNTIES.items():
            cur.execute("""
                SELECT id, owner FROM lead_records
                WHERE county=%s AND (prop_address IS NULL OR prop_address='')
                AND owner IS NOT NULL AND length(owner) > 5
                AND owner NOT ILIKE '%%LLC%%' AND owner NOT ILIKE '%%TRUST%%'
                AND owner NOT ILIKE '%%CORP%%' AND owner NOT ILIKE '%%BANK%%'
                AND owner NOT ILIKE '%%FEDERAL%%' AND owner NOT ILIKE '%%MORTGAGE%%'
                AND owner NOT ILIKE '%%CREDIT UNION%%'
                AND owner NOT ILIKE '%%U S OF AMERICA%%' AND owner NOT ILIKE '%%UNITED STATES%%'
                AND owner NOT ILIKE '%%HOSPITAL%%' AND owner NOT ILIKE '%%MEDICAL CENTER%%'
                AND owner NOT ILIKE '%%SCHOOL DISTRICT%%' AND owner NOT ILIKE '%%CHURCH%%'
                AND owner NOT ILIKE '%%UNIVERSITY%%' AND owner NOT ILIKE '%%COLLEGE%%'
                AND owner NOT ILIKE '%%CITY OF%%' AND owner NOT ILIKE '%%COUNTY OF%%'
                AND owner NOT ILIKE '%%STATE OF%%' AND owner NOT ILIKE '%% ISD%%'
                AND owner NOT ILIKE '%%INTERNAL REVENUE%%'
                ORDER BY score DESC LIMIT %s
            """, (county, LIMIT))
            leads = cur.fetchall()
            logger.info(f"{county}: {len(leads)} leads to enrich")
            updated = await enrich_county(browser, cur, conn, county, base, leads)
            conn.commit()
            logger.info(f"{county}: done - {updated}/{len(leads)} enriched")
        await browser.close()
    cur.close(); conn.close()

if __name__ == "__main__":
    asyncio.run(main())
