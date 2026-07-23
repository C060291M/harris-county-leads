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

COUNTIES = {
    "bell":      "https://esearch.bellcad.org",
    "rockwall":  "https://www.rockwallcad.com",
    "fort bend": "https://esearch.fbcad.org",
    "hunt":      "https://esearch.hctax.info",
    "kendall":   "https://esearch.kendallad.org",
    "walker":    "https://esearch.walkercad.org",
    "medina":    "https://esearch.medinacad.org",
    "starr":     "https://esearch.starrcad.org",
    "bee":       "https://esearch.beecad.org",
    "gillespie": "https://esearch.gillespiecad.org",
    "hockley":   "https://esearch.hockleycad.org",
    "jefferson": "https://esearch.jcad.org",
    "wilson":    "https://esearch.wilson-cad.org",
    "galveston": "https://esearch.galvestoncad.org",
    "nacogdoches": "https://esearch.nacocad.org",
    "taylor":     "https://esearch.taylor-cad.org",
    "hidalgo":   "https://hidalgo.prodigycad.com",
    "potter":    "https://www.prad.org",
    "randall":   "https://www.prad.org",
    "travis":    "https://travis.prodigycad.com",
}

# Counties sharing the same underlying vendor platform as Rockwall
# (React + ag-Grid, single #searchInput field, wide viewport needed)
ROCKWALL_PLATFORM_COUNTIES = {"rockwall", "potter", "randall", "travis", "hidalgo"}

# Counties sharing the same underlying vendor platform as Bell (structured
# OwnerName:X Year:Y query, #keywords field, Search() JS function)
BELL_PLATFORM_COUNTIES = {"bell", "fort bend", "hunt", "kendall", "walker", "medina", "starr", "bee", "gillespie", "hockley", "jefferson", "wilson", "galveston", "nacogdoches", "taylor"}

def get_conn():
    return psycopg2.connect(DB, connect_timeout=30)

def last_name_from_owner(owner):
    parts = owner.strip().upper().split()
    for word in parts:
        if len(word) >= 4 and word.isalpha():
            return word
    return parts[0] if parts else owner

async def search_owner_bell(page, base, last_name, year=2025):
    """Proven flow: real mouse movement -> native value setter + input/change
    events -> call the page's Search() JS function -> wait for navigation to
    the results page with a valid searchSessionToken."""
    await page.mouse.move(100, 100)
    await page.mouse.move(300, 400)
    await page.wait_for_timeout(300)

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

def is_real_address(addr):
    """Reject non-address account-type codes (e.g. 'LSE EQUIP' for leased
    equipment / business personal property accounts) that sometimes show up
    in the Situs Address column instead of a real street address."""
    if not addr or len(addr) < 6:
        return False
    return bool(re.search(r"\d", addr))

def parse_address_bell(html):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return None
    rows = table.find_all("tr")
    if len(rows) < 2:
        return None
    headers = [h.get_text(strip=True) for h in rows[0].find_all(["th", "td"])]
    cells = rows[1].find_all(["td", "th"])
    try:
        addr_idx = headers.index("Situs Address")
        addr_raw = cells[addr_idx].get_text(" ", strip=True)
        addr = re.sub(r"\s+", " ", addr_raw).strip() if addr_raw else None
        return addr if is_real_address(addr) else None
    except (ValueError, IndexError):
        return None

async def search_owner_rockwall(page, last_name):
    """Rockwall uses a completely different platform (rockwallcad.com,
    ag-Grid based) than Bell. Simple single-field search + Enter key."""
    search_input = await page.query_selector("#searchInput")
    if not search_input:
        return False
    await search_input.click()
    await search_input.fill("")
    await search_input.type(last_name, delay=60)
    await page.wait_for_timeout(300)
    await search_input.press("Enter")
    await page.wait_for_timeout(2500)
    return True

async def parse_address_rockwall(page):
    """ag-Grid virtualizes columns - only renders what's in view. We use a
    wide viewport (set at browser context level) so streetPrimary/city are
    already in the DOM without needing to scroll."""
    rows = await page.query_selector_all("[role='row']")
    for row in rows:
        cells = await row.query_selector_all("[role='gridcell']")
        street = None
        city = None
        for c in cells:
            col = await c.get_attribute("col-id")
            if col == "streetPrimary":
                street = (await c.inner_text()).strip()
            elif col == "city":
                city = (await c.inner_text()).strip()
        if street:
            return f"{street}, {city}" if city else street
    return None

async def enrich_county(browser, cur, conn, county, base, leads, get_conn_fn):
    updated = 0
    viewport = {"width": 3000, "height": 1000} if county in ROCKWALL_PLATFORM_COUNTIES else {"width": 1280, "height": 900}
    context = await browser.new_context(viewport=viewport)
    page = await context.new_page()

    for lead_id, owner in leads:
        try:
            last = last_name_from_owner(owner)
            if len(last) < 3:
                continue

            addr = None

            if county in BELL_PLATFORM_COUNTIES:
                await page.goto(f"{base}/search", timeout=30000)
                await page.wait_for_timeout(1200)
                has_field = await page.evaluate("() => document.querySelector('#keywords') !== null")
                if not has_field:
                    logger.warning(f"{county}: #keywords field not found - skipping county entirely")
                    break
                await search_owner_bell(page, base, last)
                addr = parse_address_bell(await page.content())

                if not addr:
                    parts = owner.strip().upper().split()
                    if len(parts) >= 2:
                        alt_term = parts[-1]
                        if alt_term.isalpha() and len(alt_term) >= 3 and alt_term != last:
                            await page.goto(f"{base}/search", timeout=30000)
                            await page.wait_for_timeout(1200)
                            await search_owner_bell(page, base, alt_term)
                            addr = parse_address_bell(await page.content())
                            if addr:
                                logger.info(f"{county}: matched via last-word fallback ('{alt_term}' instead of '{last}')")

            elif county in ROCKWALL_PLATFORM_COUNTIES:
                await page.goto(f"{base}/property-search", timeout=30000)
                await page.wait_for_timeout(2500)
                has_field = await page.evaluate("() => document.querySelector('#searchInput') !== null")
                if not has_field:
                    logger.warning(f"{county}: #searchInput field not found - skipping county entirely")
                    break
                ok = await search_owner_rockwall(page, last)
                if ok:
                    addr = await parse_address_rockwall(page)

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

        except psycopg2.OperationalError as e:
            logger.warning(f"{county} lead {lead_id}: DB connection dropped ({e}) - reconnecting")
            try:
                conn.close()
            except Exception:
                pass
            conn = get_conn_fn()
            cur = conn.cursor()
            continue
        except Exception as e:
            logger.warning(f"{county} lead {lead_id}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            continue

    await page.close()
    await context.close()
    return updated, conn, cur

async def main():
    conn = get_conn(); cur = conn.cursor()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for county, base in COUNTIES.items():
            try:
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
                    AND owner NOT ILIKE '%% INC%%' AND owner NOT ILIKE '%% LP%%'
                    AND owner NOT ILIKE '%%INTERNAL REVENUE%%' AND owner NOT ILIKE '%%JUDGMENT ENFORCEMENT%%'
                    AND owner !~ '^[0-9]{4}-[0-9]+$'
                    AND owner NOT ILIKE '%%CONSTRUCTION%%' AND owner NOT ILIKE '%%REPLAT%%'
                    AND owner NOT ILIKE '%%ATTORNEY GENERAL%%'
                    AND owner NOT ILIKE '%%ASSOCIATION%%' AND owner NOT ILIKE '%%DISTRICT%%'
                    AND owner NOT ILIKE '%% ROA%%' AND owner NOT ILIKE '%% HOA%%'
                    AND owner !~ '^[0-9]{6,}$'
                    ORDER BY score DESC LIMIT %s
                """, (county, LIMIT))
                leads = cur.fetchall()
                logger.info(f"{county}: {len(leads)} leads to enrich")
                updated, conn, cur = await enrich_county(browser, cur, conn, county, base, leads, get_conn)
                conn.commit()
                logger.info(f"{county}: done - {updated}/{len(leads)} enriched")
            except psycopg2.OperationalError as e:
                logger.warning(f"{county}: DB connection dropped ({e}) - reconnecting and continuing to next county")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_conn()
                cur = conn.cursor()
                continue
        await browser.close()
    try:
        cur.close(); conn.close()
    except Exception:
        pass

if __name__ == "__main__":
    asyncio.run(main())
