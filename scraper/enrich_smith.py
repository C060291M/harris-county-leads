import asyncio, psycopg2, re, logging, os
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.environ["DATABASE_URL"]
BASE = "https://esearch.smithcad.org"
LIMIT = 50

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

async def enrich_smith():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT id, owner FROM lead_records
        WHERE county='Smith' AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND length(owner) > 5
        AND owner NOT ILIKE '%%LLC%%' AND owner NOT ILIKE '%%TRUST%%'
        AND owner NOT ILIKE '%%CORP%%' AND owner NOT ILIKE '%%INC %%'
        AND owner NOT ILIKE '%%BANK%%' AND owner NOT ILIKE '%%CREDIT%%'
        AND owner NOT ILIKE '%%FEDERAL%%' AND owner NOT ILIKE '%%MORTGAGE%%'
        AND owner NOT ILIKE '%%FINANCIAL%%' AND owner NOT ILIKE '%%LENDING%%'
        ORDER BY score DESC LIMIT %s
    """, (LIMIT,))
    leads = cur.fetchall()
    logger.info(f"Smith: {len(leads)} leads to enrich")
    updated = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for lead_id, owner in leads:
            try:
                parts = owner.strip().upper().split()
                # Use best word as last name - skip single letters/initials
                last = None
                for word in reversed(parts):
                    if len(word) >= 4 and word.isalpha():
                        last = word
                        break
                if not last:
                    last = parts[0] if parts else owner
                if len(last) < 3: continue
                page = await browser.new_page()
                await page.goto(f"{BASE}/search", timeout=30000)
                await page.wait_for_timeout(2000)
                await page.click("a[href='#tab-search-adv']")
                await page.wait_for_timeout(500)
                owner_input = page.locator("#tab-search-adv input[name='query[owner][name]']")
                await owner_input.fill(last)
                submit = page.locator("#tab-search-adv button[type=submit]")
                await submit.click()
                await page.wait_for_timeout(3000)
                rows = await page.query_selector_all("table tr")
                if len(rows) >= 2:
                    first_row = rows[1]
                    text = await first_row.inner_text()
                    parts_row = [p.strip() for p in text.split("\t") if p.strip()]
                    addr = None
                    for part in parts_row:
                        if re.search(r"\d+.*TX", part, re.IGNORECASE):
                            addr = part.replace("\n", ", ").strip()
                            break
                    if addr:
                        # Get sqft from detail page
                        detail_links = await first_row.query_selector_all("a[href*='/parcels/']")
                        sqft = None; yr = None
                        if detail_links:
                            href = await detail_links[0].get_attribute("href")
                            detail_url = BASE + href if href.startswith("/") else href
                            dp = await browser.new_page()
                            await dp.goto(detail_url, timeout=20000)
                            await dp.wait_for_timeout(1500)
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
                            conn.commit()
                            logger.info(f"Smith: {updated} enriched so far")
                await page.close()
            except Exception as e:
                logger.warning(f"Smith lead {lead_id}: {e}")
                continue
        await browser.close()

    conn.commit()
    cur.close(); conn.close()
    logger.info(f"Smith: done — {updated}/{len(leads)} enriched")

asyncio.run(enrich_smith())

