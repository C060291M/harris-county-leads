import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import asyncio, psycopg2, re, logging
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.environ["DATABASE_URL"]
LIMIT = 100

COUNTIES = {
    "bell":     "https://esearch.bellcad.org",
    "rockwall": "https://esearch.rockwallcad.org",
}

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

async def search_owner(page, base, last_name, year=2025):
    """Proven flow: real mouse movement -> native value setter + input/change
    events -> call the page's Search() JS function -> wait for navigation to
    the results page with a valid searchSessionToken."""
    query = f"OwnerName:{last_name} Year:{year} "
    await page.evaluate("""(query) => {
        const el = document.querySelector('#keywords');
        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        setter.call(el, query);
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
    }""", query)

    async with page.expect_navigation(timeout=15000):
        await page.evaluate("Search()")
    await page.wait_for_timeout(2500)

async def enrich_county(browser, cur, conn, county, base, leads):
    updated = 0
    page = await browser.new_page()

    for lead_id, owner in leads:
        try:
            parts = owner.strip().upper().split()
            last = None
            for word in parts:
                if len(word) >= 4 and word.isalpha():
                    last = word
                    break
            if not last:
                last = parts[0] if parts else owner
            if len(last) < 3:
                continue

            await page.goto(f"{base}/search", timeout=30000)
            await page.wait_for_timeout(1200)

            has_field = await page.evaluate("() => document.querySelector('#keywords') !== null")
            if not has_field:
                logger.warning(f"{county}: #keywords field not found on this platform - skipping county entirely")
                break

            await page.mouse.move(100, 100)
            await page.mouse.move(300, 400)
            await page.wait_for_timeout(300)

            await search_owner(page, base, last)

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(await page.content(), "lxml")
            table = soup.find("table")
            addr = None
            if table:
                rows = table.find_all("tr")
                if len(rows) >= 2:
                    headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]
                    cells = rows[1].find_all(["td", "th"])
                    try:
                        addr_idx = headers.index("Situs Address")
                        addr_raw = cells[addr_idx].get_text(" ", strip=True)
                        if addr_raw:
                            addr = re.sub(r"\s+", " ", addr_raw).strip()
                    except (ValueError, IndexError):
                        pass

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
                ORDER BY score DESC LIMIT %s
            """, (county, LIMIT))
            leads = cur.fetchall()
            logger.info(f"{county}: {len(leads)} leads to enrich")
            updated = await enrich_county(browser, cur, conn, county, base, leads)
            conn.commit()
            logger.info(f"{county}: done - {updated}/{len(leads)} enriched")
        await browser.close()
    cur.close(); conn.close()

asyncio.run(main())
