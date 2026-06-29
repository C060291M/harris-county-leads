import asyncpg, asyncio, os, logging, re, httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich_cad")

DB = os.environ["DATABASE_URL"]

COUNTY_TABLES = {
    "harris":     ("harris_cad",          "owner_name", "prop_address", "beds", "full_baths", "sqft", "yr_built", None),
    "dallas":     ("dallas_cad",          "owner_name", "prop_address", "beds", "full_baths", "sqft", "yr_built", "appraised_value"),
    "tarrant":    ("tarrant_cad",         "owner_name", "situs_address", "beds", "baths", "sqft", "yr_built", "total_value"),
    "denton":     ("denton_cad",          "owner_name", "situs_address", None, None, "sqft", "yr_built", "appraised_val"),
    "collin":     ("collin_cad",          "owner_name", "situs_address", None, None, "sqft", "yr_built", "appraised_val"),
    "montgomery": ("montgomery_cad",      "owner_name", "situs_address", None, None, "sqft", "yr_built", "appraised_val"),
    "grayson":    ("grayson_cad",         "owner_name", "situs_address", None, None, "living_area", "yr_built", "appraised_val"),
    "cameron":    ("cameron_cad",         "owner_name", "situs_address", None, None, "living_area", "yr_built", "appraised_val"),
    "nueces":     ("nueces_cad",          "owner_name", "situs_address", None, None, "living_area", "yr_built", "appraised_val"),
    "johnson":    ("johnson_cad",         "owner_name", "situs_address", None, None, "living_area", "yr_built", "appraised_val"),
    "midland":    ("midland_cad",         "owner_name", "situs_address", None, None, "living_area", "yr_built", "appraised_val"),
}

BIS_COUNTIES = {
    "bell":        "https://esearch.bellcad.org",
    "anderson":    "https://esearch.andersoncad.org",
    "walker":      "https://esearch.walkercad.org",
    "grimes":      "https://esearch.grimescad.org",
    "freestone":   "https://esearch.freestonecad.org",
    "blanco":      "https://esearch.blancocad.com",
    "burleson":    "https://esearch.burlesoncad.org",
    "kendall":     "https://esearch.kendallcad.org",
    "wood":        "https://esearch.woodcad.net",
    "andrews":     "https://esearch.andrewscad.org",
    "galveston":   "https://esearch.galvestoncad.org",
    "kaufman":     "https://esearch.kaufman-cad.org",
    "howard":      "https://esearch.howardcad.org",
    "taylor":      "https://esearch.taylorcad.org",
    "medina":      "https://esearch.medinacad.org",
    "goliad":      "https://esearch.goliadcad.org",
    "donley":      "https://esearch.donleycad.org",
    "smith":       "https://esearch.smithcad.org",
    "brazos":      "https://esearch.brazoscad.org",
    "rockwall":    "https://esearch.rockwallcad.com",
    "brazoria":    "https://esearch.brazoriacad.org",
    "victoria":    "https://esearch.victoriacad.org",
    "lubbock":     "https://esearch.lubbockcad.org",
    "hidalgo":     "https://esearch.hidalgoad.org",
    "williamson":  "https://esearch.wcad.org",
    "ector":       "https://esearch.ectorcad.org",
    "nacogdoches": "https://esearch.nacad.org",
    "jefferson":   "https://esearch.jeffersoncad.org",
    "hunt":        "https://esearch.huntcad.org",
    "wilson":      "https://esearch.wilsoncad.org",
    "potter":      "https://esearch.pottercad.org",
    "montgomery":  "https://esearch.mcad-tx.org",
    "bexar":       "https://esearch.bcad.org",
    "travis":      "https://esearch.traviscad.org",
}

CAD_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"}

async def enrich_county(conn, county, table, owner_col, addr_col, beds_col, baths_col, sqft_col, yr_col, val_col):
    leads = await conn.fetch("""
        SELECT id, owner FROM lead_records
        WHERE county=$1
        AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND owner != ''
        LIMIT 2000
    """, county)
    log.info(f"[{county}] {len(leads)} leads to enrich")
    if not leads:
        return 0
    updated = 0
    for lead in leads:
        owner = lead["owner"].strip().upper()
        words = [w for w in owner.split() if len(w) > 1]
        if not words:
            continue
        w1 = words[0]
        w2 = words[1] if len(words) > 1 else None
        try:
            if beds_col and baths_col:
                q = f"SELECT {addr_col}, {sqft_col}, {beds_col}, {baths_col}, {yr_col}, {val_col} FROM {table} WHERE {owner_col} ILIKE $1"
                args = [f"%{w1}%"]
                if w2:
                    q += f" AND ({owner_col} ILIKE $2 OR $2 = '')"
                    args.append(f"%{w2}%")
                q += f" AND {addr_col} IS NOT NULL AND {addr_col} != '' LIMIT 1"
            else:
                q = f"SELECT {addr_col}, {sqft_col}, NULL, NULL, {yr_col}, {val_col} FROM {table} WHERE {owner_col} ILIKE $1"
                args = [f"%{w1}%"]
                if w2:
                    q += f" AND ({owner_col} ILIKE $2 OR $2 = '')"
                    args.append(f"%{w2}%")
                q += f" AND {addr_col} IS NOT NULL AND {addr_col} != '' LIMIT 1"
            row = await conn.fetchrow(q, *args)
            if not row or not row[0]:
                continue
            addr = row[0].strip()
            sqft = int(row[1]) if row[1] and str(row[1]) not in ("0", "") else None
            beds = int(row[2]) if row[2] and str(row[2]) not in ("0", "") else None
            baths = int(row[3]) if row[3] and str(row[3]) not in ("0", "") else None
            yr = str(row[4]) if row[4] and str(row[4]) != "0" else None
            val = int(float(str(row[5]))) if row[5] and str(row[5]) not in ("0", "") else None
            await conn.execute("""
                UPDATE lead_records SET
                    prop_address = COALESCE(NULLIF(prop_address,''), $1),
                    sqft = COALESCE(sqft, $2),
                    beds = COALESCE(beds, $3),
                    full_baths = COALESCE(full_baths, $4),
                    yr_built = COALESCE(yr_built, $5),
                    appraised_value = COALESCE(appraised_value, $6),
                    cad_enriched_at = NOW()
                WHERE id = $7
            """, addr, sqft, beds, baths, yr, val, lead["id"])
            updated += 1
        except Exception as e:
            log.warning(f"[{county}] lead {lead['id']} error: {e}")
    log.info(f"[{county}] Updated {updated}/{len(leads)} leads")
    return updated

async def enrich_by_address(conn, county, table, addr_col, sqft_col, yr_col, val_col):
    leads = await conn.fetch("""
        SELECT id, prop_address FROM lead_records
        WHERE county=$1
        AND prop_address IS NOT NULL AND prop_address != ''
        AND (sqft IS NULL OR sqft = 0)
        LIMIT 2000
    """, county)
    log.info(f"[{county}] addr-enrich: {len(leads)} leads")
    updated = 0
    for lead in leads:
        addr = (lead["prop_address"] or "").strip().upper()
        m = re.match(r"(\d+)\s+(.+)", addr)
        if not m:
            continue
        snum, spart = m.group(1), m.group(2)[:8]
        try:
            row = await conn.fetchrow(f"""
                SELECT {sqft_col}, {yr_col}, {val_col} FROM {table}
                WHERE {addr_col} ILIKE $1 AND {addr_col} ILIKE $2
                AND {addr_col} IS NOT NULL LIMIT 1
            """, f"%{snum}%", f"%{spart}%")
            if not row:
                continue
            sqft = int(row[0]) if row[0] and str(row[0]) not in ("0", "") else None
            yr = str(row[1]) if row[1] and str(row[1]) not in ("0", "") else None
            val = int(float(str(row[2]))) if row[2] and str(row[2]) not in ("0", "") else None
            if sqft or yr:
                await conn.execute("""
                    UPDATE lead_records SET
                        sqft = COALESCE(sqft, $1),
                        yr_built = COALESCE(yr_built, $2),
                        appraised_value = COALESCE(appraised_value, $3),
                        cad_enriched_at = NOW()
                    WHERE id = $4
                """, sqft, yr, val, lead["id"])
                updated += 1
        except:
            pass
    log.info(f"[{county}] addr-enrich updated {updated}/{len(leads)}")
    return updated

def parse_bis_html(html):
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    def fv(labels):
        for label in labels:
            for cell in soup.find_all(["td", "th", "dt"]):
                if label.lower() in cell.get_text(strip=True).lower():
                    nxt = cell.find_next_sibling(["td", "th", "dd"])
                    if nxt:
                        val = nxt.get_text(strip=True)
                        if val:
                            return val
        return None
    addr = fv(["Property Address", "Situs Address", "Address"])
    if addr:
        data["address"] = addr
    sqft = fv(["Living Area", "Heated Area", "Building Area", "Sq Ft"])
    if sqft:
        c = re.sub(r"[^\d]", "", sqft.split()[0])
        if c:
            data["sqft"] = int(c)
    yr = fv(["Year Built", "Yr Built"])
    if yr:
        m2 = re.search(r"(19|20)\d{2}", yr)
        if m2:
            data["year_built"] = m2.group()
    val = fv(["Appraised Value", "Total Appraised", "Market Value"])
    if val:
        c = re.sub(r"[^\d]", "", val.split()[0])
        if c:
            data["appraised_value"] = int(c)
    return data

async def scrape_bis(base_url, owner_name):
    import json as _json
    import urllib.parse as _up
    async with httpx.AsyncClient(headers=CAD_HEADERS, timeout=20.0, follow_redirects=True, verify=False) as client:
        try:
            parts = owner_name.strip().upper().split()
            last = next((w for w in parts if len(w) >= 4 and w.isalpha()), parts[0] if parts else owner_name)
            r = await client.get(f"{base_url}/search/requestSessionToken")
            if r.status_code == 200:
                try:
                    session_token = r.json().get("searchSessionToken", "")
                except:
                    session_token = ""
                if session_token:
                    keywords = f'OwnerName:"{last}"'
                    r_home = await client.get(f"{base_url}/")
                    soup = BeautifulSoup(r_home.text, "html.parser")
                    meta = soup.find("meta", {"name": "search-token"})
                    search_token = meta["content"] if meta else ""
                    post = {"page": 1, "pageSize": 5, "isArb": False, "recaptchaToken": None, "searchToken": search_token}
                    r2 = await client.post(
                        f"{base_url}/search/SearchResults?keywords={_up.quote(keywords)}",
                        content=_json.dumps(post),
                        headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
                    )
                    if r2.status_code == 200:
                        results = r2.json().get("resultsList", [])
                        if results:
                            owner_words = set(w for w in owner_name.upper().split() if len(w) >= 3)
                            best, best_sim = None, 0.0
                            for prop in results:
                                cad_w = set(w for w in (prop.get("ownerName", "") or "").upper().split() if len(w) >= 3)
                                sim = len(owner_words & cad_w) / len(owner_words | cad_w) if (owner_words | cad_w) else 0
                                if sim > best_sim:
                                    best_sim, best = sim, prop
                            if best and best_sim >= 0.1:
                                addr = best.get("address", "")
                                result = {"address": addr} if addr else {}
                                pid = best.get("propertyId")
                                if pid:
                                    r3 = await client.get(f"{base_url}/Property/View/{pid}?year=2026")
                                    if r3.status_code == 200:
                                        result.update(parse_bis_html(r3.text))
                                if result.get("address"):
                                    return result
            r1 = await client.get(f"{base_url}/Search/Owner")
            if r1.status_code != 200:
                return None
            soup1 = BeautifulSoup(r1.text, "html.parser")
            ti = soup1.find("input", {"name": "__RequestVerificationToken"})
            if not ti:
                return None
            token = ti["value"]
            r2 = await client.post(f"{base_url}/Search/Owner",
                data={"__RequestVerificationToken": token, "OwnerName": last, "SearchType": "Name"},
                headers={"Referer": f"{base_url}/Search/Owner", "Content-Type": "application/x-www-form-urlencoded"})
            soup2 = BeautifulSoup(r2.text, "html.parser")
            row = soup2.find("table")
            if not row:
                return None
            link = row.find("a", href=re.compile(r"/Property/"))
            if not link:
                return None
            r3 = await client.get(base_url + link["href"])
            return parse_bis_html(r3.text)
        except:
            return None

async def enrich_bis_county(conn, county, base_url):
    leads = await conn.fetch("""
        SELECT id, owner FROM lead_records
        WHERE county=$1
        AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND owner != ''
        LIMIT 500
    """, county)
    log.info(f"[{county}] BIS: {len(leads)} leads to enrich")
    updated = 0
    for lead in leads:
        if not lead["owner"]:
            continue
        result = await scrape_bis(base_url, lead["owner"])
        if not result:
            continue
        await conn.execute("""
            UPDATE lead_records SET
                prop_address = COALESCE(NULLIF(prop_address,''), $1),
                sqft = COALESCE(sqft, $2),
                yr_built = COALESCE(yr_built, $3),
                appraised_value = COALESCE(appraised_value, $4),
                cad_enriched_at = NOW()
            WHERE id = $5
        """, result.get("address"), result.get("sqft"), result.get("year_built"), result.get("appraised_value"), lead["id"])
        updated += 1
        await asyncio.sleep(0.3)
    log.info(f"[{county}] BIS updated {updated}/{len(leads)}")
    return updated

async def main():
    county_filter = os.getenv("COUNTIES", "").split(",") if os.getenv("COUNTIES") else []
    county_filter = [c.strip().lower() for c in county_filter if c.strip()]

    conn = await asyncpg.connect(DB)
    total = 0

    for county, args in COUNTY_TABLES.items():
        if county_filter and county not in county_filter:
            continue
        n = await enrich_county(conn, county, *args)
        total += n

    for county, args in COUNTY_TABLES.items():
        if county_filter and county not in county_filter:
            continue
        table, _, addr_col, _, _, sqft_col, yr_col, val_col = args
        n = await enrich_by_address(conn, county, table, addr_col, sqft_col, yr_col, val_col)
        total += n

    for county, url in BIS_COUNTIES.items():
        if county_filter and county not in county_filter:
            continue
        n = await enrich_bis_county(conn, county, url)
        total += n

    await conn.close()
    log.info(f"Total updated: {total}")

asyncio.run(main())
