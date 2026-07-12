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
COUNTY = "williamson"
BASE = "https://search.wcad.org"

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

async def search_williamson(page, last_name):
    await page.goto(BASE + "/", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    field = await page.query_selector("#SearchText")
    if not field:
        return None
    await field.click()
    await field.fill("")
    await field.type(last_name, delay=60)
    await page.wait_for_timeout(300)
    await field.press("Enter")
    await page.wait_for_timeout(3000)

    soup = BeautifulSoup(await page.content(), "lxml")
    # Find a data row: first cell looks like a Property ID (letter + digits),
    # and the row has at least 6 columns so index 5 (Situs Address) exists.
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 6:
            continue
        first = cells[0].get_text(strip=True)
        if re.match(r"^R\d{5,}$", first):
            addr_raw = cells[5].get_text(" ", strip=True)
            addr = re.sub(r"\s+", " ", addr_raw).strip() if addr_raw else None
            return addr if is_real_address(addr) else None
    return None

async def main():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT id, owner FROM lead_records
        WHERE county=%s AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND length(owner) > 5
        AND owner NOT ILIKE '%%LLC%%' AND owner NOT ILIKE '%%TRUST%%'
        AND owner NOT ILIKE '%%CORP%%' AND owner NOT ILIKE '%%BANK%%'
        AND owner NOT ILIKE '%%FEDERAL%%' AND owner NOT ILIKE '%%MORTGAGE%%'
        AND owner NOT ILIKE '%%INTERNAL REVENUE%%' AND owner NOT ILIKE '%%CITY OF%%'
        ORDER BY score DESC LIMIT %s
    """, (COUNTY, LIMIT))
    leads = cur.fetchall()
    logger.info(f"{COUNTY}: {len(leads)} leads to enrich")

    updated = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        for lead_id, owner in leads:
            try:
                last = last_name_from_owner(owner)
                if len(last) < 3:
                    continue
                addr = await search_williamson(page, last)
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
                        logger.info(f"{COUNTY}: {updated} enriched so far (committed)")
            except Exception as e:
                logger.warning(f"{COUNTY} lead {lead_id}: {e}")
                continue
        await browser.close()

    conn.commit()
    logger.info(f"{COUNTY}: done - {updated}/{len(leads)} enriched")
    cur.close(); conn.close()

if __name__ == "__main__":
    asyncio.run(main())
