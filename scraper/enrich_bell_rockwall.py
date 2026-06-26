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
LIMIT = 300

COUNTIES = {
    "bell":     "https://esearch.bellcad.org",
    "rockwall": "https://esearch.rockwallcad.org",
}

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

async def enrich_county(browser, cur, county, base, leads):
    updated = 0
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

            page = await browser.new_page()
            await page.goto(f"{base}/search", timeout=30000)
            await page.wait_for_timeout(1000)
            await page.click("a[href='#tab-search-adv']")
            await page.wait_for_timeout(300)
            owner_input = page.locator("#tab-search-adv input[name='query[owner][name]']")
            await owner_input.fill(last)
            submit = page.locator("#tab-search-adv button[type=submit]")
            await submit.evaluate("el => el.click()")
            await page.wait_for_timeout(1000)

            rows = await page.query_selector_all("table tr")
            if len(rows) >= 2:
                first_row = rows[1]
                text = await first_row.inner_text()
                parts_row = [p.strip() for p in text.split("\t") if p.strip()]
                addr = None
                for part in parts_row:
                    if re.search(r"\d+.*TX", part, re.IGNORECASE | re.DOTALL):
                        addr = re.sub(r"\s+", " ", part.replace("\n", " ")).strip()
                        addr = addr.split("  ")[0].strip() or addr
                        break
                if not addr:
                    full_text = " ".join(parts_row)
                    m = re.search(r"(\d+\s+[A-Z0-9 ]+(?:DR|ST|AVE|RD|LN|BLVD|CT|PL|WAY|CIR|TRL)[^\n]*(?:TX)[^\n]*)", full_text, re.IGNORECASE)
                    if m:
                        addr = re.sub(r"\s+", " ", m.group(1)).strip()

                if addr:
                    detail_links = await first_row.query_selector_all("a[href*='/parcels/']")
                    sqft = None; yr = None
                    if detail_links:
                        href = await detail_links[0].get_attribute("href")
                        detail_url = base + href if href.startswith("/") else href
                        dp = await browser.new_page()
                        await dp.goto(detail_url, timeout=20000)
                        await dp.wait_for_timeout(1000)
                        content = await dp.content()
                        sqft_m = re.search(r"Living Area[^\d]*(\d+)", content)
                        yr_m = re.search(r"Year Built[^\d]*(\d{4})", content)
                        if sqft_m: sqft = int(sqft_m.group(1))
                        if yr_m: yr = yr_m.group(1)
                        await dp.close()

                    cur.execute("""
                        UPDATE lead_records SET
                            prop_address=COALESCE(NULLIF(%s,''),prop_address),
                            sqft=COALESCE(%s,sqft),
                            yr_built=COALESCE(%s,yr_built),
                            cad_enriched_at=NOW()
                        WHERE id=%s
                    """, (addr, sqft, yr, lead_id))
                    updated += 1
                    if updated % 10 == 0:
                        logger.info(f"{county}: {updated} enriched so far")

            await page.close()
        except Exception as e:
            logger.warning(f"{county} lead {lead_id}: {e}")
            continue
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
            updated = await enrich_county(browser, cur, county, base, leads)
            conn.commit()
            logger.info(f"{county}: done - {updated}/{len(leads)} enriched")
        await browser.close()
    cur.close(); conn.close()

asyncio.run(main())
