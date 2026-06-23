import asyncio, os, re, logging, psycopg2
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bexar_cad")

DB         = os.environ.get("DATABASE_URL", "")
BATCH      = int(os.getenv("BATCH_SIZE", "200"))

def clean_name(name):
    if not name: return ""
    words = re.sub(r"[^A-Z0-9 ]", "", name.upper()).split()
    return " ".join(words[:2]) if words else ""

def is_valid_address(addr):
    if not addr or len(addr) < 8: return False
    if not any(c.isdigit() for c in addr): return False
    bad = ["website","contact","phone","http","www","email","fax","(210)","suite only","po box"]
    return not any(b in addr.lower() for b in bad)

async def search_owner(page, owner_name):
    try:
        await page.goto("https://bexar.trueautomation.com/clientdb/?cid=110",
                       wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(1000)
        search_term = clean_name(owner_name)
        if not search_term: return []
        await page.fill("input[name='propertySearchOptions:searchText']", search_term)
        await page.click("input[name='propertySearchOptions:search']")
        await page.wait_for_timeout(3000)
        soup = BeautifulSoup(await page.content(), "lxml")
        rows = soup.find_all("tr")
        results = []
        for row in rows[1:]:
            cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            if len(cells) < 5: continue
            addr_cell  = cells[4] if len(cells) > 4 else ""
            owner_cell = cells[6] if len(cells) > 6 else ""
            addr = re.sub(r"\s+", " ", addr_cell).strip()
            if not is_valid_address(addr): continue
            if owner_name and owner_cell:
                name_words   = set(clean_name(owner_name).split())
                result_words = set(clean_name(owner_cell).split())
                if not (name_words & result_words): continue
            results.append((addr, owner_cell))
        return results
    except Exception as e:
        log.warning("Search error for %s: %s", owner_name, e)
        return []

async def main():
    if not DB:
        log.error("DATABASE_URL not set"); return
    
    conn = psycopg2.connect(DB)
    conn.autocommit = False
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, owner, doc_num FROM lead_records
        WHERE county = 'bexar'
        AND (prop_address IS NULL OR prop_address = '')
        AND owner IS NOT NULL AND owner != ''
        AND cat NOT IN ('RELEASE','SATISFACTION','DISCHARGE','NOC')
        AND score > 0
        ORDER BY score DESC
        LIMIT %s
    """, (BATCH,))
    leads = cur.fetchall()
    log.info("Found %d Bexar leads missing addresses", len(leads))
    
    updated = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
        )
        
        for i, (lead_id, owner, doc_num) in enumerate(leads):
            results = await search_owner(page, owner)
            if results:
                addr, matched_owner = results[0]
                try:
                    cur.execute("""
                        UPDATE lead_records 
                        SET prop_address = %s
                        WHERE id = %s AND (prop_address IS NULL OR prop_address = '')
                    """, (addr, lead_id))
                    conn.commit()
                    updated += 1
                    if updated % 10 == 0:
                        log.info("Updated %d/%d | Last: %s -> %s", updated, len(leads), owner[:30], addr[:40])
                except Exception as e:
                    log.warning("DB error at %d: %s — reconnecting", i, e)
                    try:
                        conn.rollback()
                        conn.close()
                    except: pass
                    conn = psycopg2.connect(DB)
                    conn.autocommit = False
                    cur = conn.cursor()
            
            if i % 30 == 29:
                await asyncio.sleep(2)
        
        await browser.close()
    
    try:
        cur.close()
        conn.close()
    except: pass
    log.info("Done: %d/%d addresses updated", updated, len(leads))

if __name__ == "__main__":
    asyncio.run(main())
