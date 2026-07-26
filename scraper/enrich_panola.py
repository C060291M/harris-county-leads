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
BASE = "http://www.panolacad.org/"

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

def last_name_from_owner(owner):
    parts = owner.strip().upper().split()
    for w in parts:
        if len(w) >= 4 and w.isalpha():
            return w
    return parts[0] if parts else owner

def is_real_address(addr):
    return bool(addr) and len(addr) >= 6 and bool(re.search(r"\d", addr))

async def search_panola(page, last_name):
    await page.goto(BASE, timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    field = await page.query_selector("#Keyword")
    if not field:
        return None
    await field.click()
    await field.fill("")
    await field.type(last_name, delay=40)
    await field.press("Enter")
    await page.wait_for_timeout(2500)
    soup = BeautifulSoup(await page.content(), "lxml")
    table = soup.find("table")
    if not table:
        return None
    rows = table.find_all("tr")
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 8:
            continue
        addr = re.sub(r"\s+", " ", cells[7].get_text(" ", strip=True)).strip()
        if is_real_address(addr):
            return addr
    return None

async def main():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT id, owner FROM lead_records
        WHERE county='panola' AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND length(owner) > 5
        AND owner NOT ILIKE '%%LLC%%' AND owner NOT ILIKE '%%TRUST%%'
        AND owner NOT ILIKE '%%CORP%%' AND owner NOT ILIKE '%%BANK%%'
        AND owner NOT ILIKE '%%FEDERAL%%' AND owner NOT ILIKE '%%MORTGAGE%%'
        AND owner NOT ILIKE '%%CREDIT UNION%%'
        AND owner NOT ILIKE '%%HOSPITAL%%' AND owner NOT ILIKE '%%SCHOOL DISTRICT%%'
        AND owner NOT ILIKE '%%CITY OF%%' AND owner NOT ILIKE '%%COUNTY OF%%'
        AND owner NOT ILIKE '%%STATE OF%%' AND owner NOT ILIKE '%% ISD%%'
        AND owner NOT ILIKE '%% INC%%' AND owner NOT ILIKE '%% LP%%' AND owner NOT ILIKE '%% LTD%%'
        AND owner NOT ILIKE '%%INTERNAL REVENUE%%'
        AND owner NOT ILIKE '%%ASSOCIATION%%' AND owner NOT ILIKE '%%DISTRICT%%'
        AND owner NOT ILIKE '%% COUNTY'
        AND owner !~ '^[0-9]{4}-[0-9]+$' AND owner !~ '^[0-9]{6,}$'
        ORDER BY score DESC LIMIT %s
    """, (LIMIT,))
    leads = cur.fetchall()
    logger.info(f"panola: {len(leads)} leads to enrich")

    updated = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        for lead_id, owner in leads:
            try:
                last = last_name_from_owner(owner)
                addr = await search_panola(page, last)
                if addr:
                    cur.execute("UPDATE lead_records SET prop_address=%s WHERE id=%s", (addr, lead_id))
                    updated += 1
                    if updated % 10 == 0:
                        conn.commit()
                        logger.info(f"panola: {updated} so far (committed)")
            except psycopg2.OperationalError as e:
                logger.warning(f"panola: DB dropped ({e}) - reconnecting")
                try: conn.close()
                except Exception: pass
                conn = get_conn(); cur = conn.cursor()
                continue
            except Exception as e:
                logger.warning(f"panola lead {lead_id}: {e}")
                try: conn.rollback()
                except Exception: pass
                continue
        await browser.close()
    conn.commit()
    logger.info(f"panola: done - {updated}/{len(leads)} enriched")
    cur.close(); conn.close()

if __name__ == "__main__":
    asyncio.run(main())
