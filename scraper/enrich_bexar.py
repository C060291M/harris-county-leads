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
LIMIT = 20
BASE = "https://bexar.trueautomation.com/clientdb/propertysearch.aspx?cid=110"

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

def last_name_from_owner(owner):
    parts = owner.strip().upper().split()
    for word in parts:
        if len(word) >= 3 and word.isalpha():
            return word
    return parts[0] if parts else owner

def is_real_address(addr):
    if not addr or len(addr) < 6:
        return False
    return bool(re.search(r"\d", addr))

async def search_bexar(page, last_name):
    await page.goto(BASE, timeout=45000, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    adv_btn = await page.query_selector("#propertySearchOptions_advanced")
    async with page.expect_navigation(timeout=15000):
        await adv_btn.click()
    await page.wait_for_timeout(1000)

    field = await page.query_selector("#propertySearchOptions_ownerName")
    await field.click()
    await field.type(last_name, delay=50)
    await page.wait_for_timeout(300)

    search_btn = await page.query_selector("#propertySearchOptions_searchAdv")
    async with page.expect_navigation(timeout=15000):
        await search_btn.click()
    await page.wait_for_timeout(2000)

    soup = BeautifulSoup(await page.content(), "lxml")
    table = soup.find("table")
    if not table:
        return None
    rows = table.find_all("tr")
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 7:
            continue
        addr_raw = cells[4].get_text(" ", strip=True)
        addr = re.sub(r"\s+", " ", addr_raw).strip()
        if is_real_address(addr):
            return addr
    return None

async def enrich_bexar(browser, cur, conn, leads):
    updated = 0
    page = await browser.new_page()
    for lead_id, owner in leads:
        try:
            last = last_name_from_owner(owner)
            if len(last) < 3:
                continue
            addr = await search_bexar(page, last)
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
                    logger.info(f"bexar: {updated} enriched so far (committed)")
        except psycopg2.OperationalError as e:
            logger.warning(f"bexar: DB connection dropped ({e}) - reconnecting")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            conn = get_conn()
            cur = conn.cursor()
            continue
        except Exception as e:
            logger.warning(f"bexar lead {lead_id}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            continue
    await page.close()
    return updated, conn, cur

async def main():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT id, owner FROM lead_records
        WHERE county='bexar' AND (prop_address IS NULL OR prop_address='')
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
        AND owner NOT ILIKE '%% INC%%' AND owner NOT ILIKE '%% LP%%' AND owner NOT ILIKE '%% LTD%%'
        AND owner NOT ILIKE '%%INTERNAL REVENUE%%' AND owner NOT ILIKE '%%JUDGMENT ENFORCEMENT%%'
        AND owner !~ '^[0-9]{4}-[0-9]+$'
        AND owner NOT ILIKE '%%CONSTRUCTION%%' AND owner NOT ILIKE '%%REPLAT%%'
        AND owner NOT ILIKE '%%ATTORNEY GENERAL%%'
        AND owner NOT ILIKE '%%ASSOCIATION%%' AND owner NOT ILIKE '%%DISTRICT%%'
        AND owner NOT ILIKE '%% ROA%%' AND owner NOT ILIKE '%% HOA%%'
        AND owner !~ '^[0-9]{6,}$'
        ORDER BY score DESC LIMIT %s
    """, (LIMIT,))
    leads = cur.fetchall()
    logger.info(f"bexar: {len(leads)} leads to enrich")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        updated, conn, cur = await enrich_bexar(browser, cur, conn, leads)
        await browser.close()

    conn.commit()
    logger.info(f"bexar: done - {updated}/{len(leads)} enriched")
    cur.close(); conn.close()

if __name__ == "__main__":
    asyncio.run(main())
