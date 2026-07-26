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
COUNTY = "chambers"
BASE = "https://www.chamberscad.org"

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

SUFFIX_WORDS = {"OF", "ESTATE", "DECEASED", "DECD", "ETAL", "ET", "AL", "TRUSTEE", "DTD", "AKA", "FKA", "NKA", "AND", "SR", "JR", "III"}

def strip_owner_suffixes(owner):
    words = owner.strip().upper().split()
    while words and words[-1] in SUFFIX_WORDS:
        words.pop()
    return " ".join(words) if words else owner.strip().upper()

def last_name_from_owner(owner):
    owner = strip_owner_suffixes(owner)
    parts = owner.strip().upper().split()
    for word in parts:
        if len(word) >= 4 and word.isalpha() and word not in SUFFIX_WORDS:
            return word
    return parts[0] if parts else owner

def is_real_address(addr):
    if not addr or len(addr) < 6:
        return False
    return bool(re.search(r"\d", addr))

async def search_chambers(page, last_name):
    await page.goto(f"{BASE}/Home/Search", timeout=30000, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)
    field = await page.query_selector("#Keyword")
    if not field:
        return None
    await field.click()
    await field.fill("")
    await field.type(last_name, delay=60)
    await page.wait_for_timeout(300)
    await field.press("Enter")
    await page.wait_for_timeout(2500)

    soup = BeautifulSoup(await page.content(), "lxml")
    tables = soup.find_all("table")
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]
        if "Property Address" not in headers:
            continue
        cells = rows[1].find_all(["td", "th"])
        try:
            addr_idx = headers.index("Property Address")
            addr_raw = cells[addr_idx].get_text(" ", strip=True)
            addr = re.sub(r"\s+", " ", addr_raw).strip() if addr_raw else None
            return addr if is_real_address(addr) else None
        except (ValueError, IndexError):
            return None
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
        AND owner NOT ILIKE '%%CREDIT UNION%%'
        AND owner NOT ILIKE '%%U S OF AMERICA%%' AND owner NOT ILIKE '%%UNITED STATES%%'
        AND owner NOT ILIKE '%%HOSPITAL%%' AND owner NOT ILIKE '%%MEDICAL CENTER%%'
        AND owner NOT ILIKE '%%SCHOOL DISTRICT%%' AND owner NOT ILIKE '%%CHURCH%%'
        AND owner NOT ILIKE '%%UNIVERSITY%%' AND owner NOT ILIKE '%%COLLEGE%%'
        AND owner NOT ILIKE '%%CITY OF%%' AND owner NOT ILIKE '%%COUNTY OF%%'
                AND owner NOT ILIKE '%% COUNTY'
        AND owner NOT ILIKE '%%STATE OF%%' AND owner NOT ILIKE '%% ISD%%'
        AND owner NOT ILIKE '%% INC%%' AND owner NOT ILIKE '%% LP%%' AND owner NOT ILIKE '%% LTD%%'
        AND owner NOT ILIKE '%%INTERNAL REVENUE%%' AND owner NOT ILIKE '%%JUDGMENT ENFORCEMENT%%'
        AND owner !~ '^[0-9]{4}-[0-9]+$'
        AND owner NOT ILIKE '%%CONSTRUCTION%%' AND owner NOT ILIKE '%%REPLAT%%'
        AND owner NOT ILIKE '%%ATTORNEY GENERAL%%'
        AND owner NOT ILIKE '%%ASSOCIATION%%' AND owner NOT ILIKE '%%DISTRICT%%'
        AND owner NOT ILIKE '%% ROA%%' AND owner NOT ILIKE '%% HOA%%'
        AND owner !~ '^[0-9]{6,}$'
        AND owner NOT ILIKE '%%INTERNAL REVENUE%%'
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
                addr = await search_chambers(page, last)
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
            except psycopg2.OperationalError as e:
                logger.warning(f"{COUNTY}: DB connection dropped ({e}) - reconnecting")
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_conn()
                cur = conn.cursor()
                continue
            except Exception as e:
                logger.warning(f"{COUNTY} lead {lead_id}: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue
        await browser.close()

    conn.commit()
    logger.info(f"{COUNTY}: done - {updated}/{len(leads)} enriched")
    cur.close(); conn.close()

if __name__ == "__main__":
    asyncio.run(main())
