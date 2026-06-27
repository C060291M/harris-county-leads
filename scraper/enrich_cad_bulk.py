"""
StackIQ - Bulk CAD Enrichment via GitHub Actions
Runs directly against Railway PostgreSQL - no Railway API calls
"""
import asyncpg, asyncio, os, logging, re

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("enrich_cad")

DB = os.environ["DATABASE_URL"]

COUNTY_TABLES = {
    "dallas":     ("dallas_cad_accounts", "owner_name", "prop_address", None, None, "sqft", "yr_built", "appraised_value"),
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

async def enrich_county(conn, county, table, owner_col, addr_col, beds_col, baths_col, sqft_col, yr_col, val_col):
    # Get leads needing enrichment
    leads = await conn.fetch(f"""
        SELECT id, owner FROM lead_records
        WHERE county='{county}'
        AND (prop_address IS NULL OR prop_address='')
        AND owner IS NOT NULL AND owner != ''
        LIMIT 2000
    """)
    log.info(f"[{county}] {len(leads)} leads to enrich")
    if not leads: return 0

    updated = 0
    for lead in leads:
        owner = lead['owner'].strip().upper()
        words = [w for w in owner.split() if len(w) > 1]
        if not words: continue
        w1 = words[0]
        w2 = words[1] if len(words) > 1 else None

        # Build query
        if beds_col and baths_col:
            # Tarrant has beds/baths
            q = f"""
                SELECT {addr_col}, {sqft_col}, {beds_col}, {baths_col}, {yr_col}, {val_col}
                FROM {table}
                WHERE {owner_col} ILIKE $1
                {'AND ' + owner_col + ' ILIKE $2' if w2 else ''}
                AND {addr_col} IS NOT NULL AND {addr_col} != ''
                LIMIT 1
            """
        else:
            q = f"""
                SELECT {addr_col}, {sqft_col}, NULL, NULL, {yr_col}, {val_col}
                FROM {table}
                WHERE {owner_col} ILIKE $1
                {'AND ' + owner_col + ' ILIKE $2' if w2 else ''}
                AND {addr_col} IS NOT NULL AND {addr_col} != ''
                LIMIT 1
            """
        
        try:
            args = [f"%{w1}%"]
            if w2: args.append(f"%{w2}%")
            row = await conn.fetchrow(q, *args)
            if not row or not row[0]: continue

            addr = row[0].strip()
            sqft = int(row[1]) if row[1] and str(row[1]) not in ('0','') else None
            beds = int(row[2]) if row[2] and str(row[2]) not in ('0','') else None
            baths = int(row[3]) if row[3] and str(row[3]) not in ('0','') else None
            yr = str(row[4]) if row[4] and str(row[4]) != '0' else None
            val = int(float(str(row[5]))) if row[5] and str(row[5]) not in ('0','') else None

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
            """, addr, sqft, beds, baths, yr, val, lead['id'])
            updated += 1
        except Exception as e:
            log.warning(f"[{county}] lead {lead['id']} error: {e}")

    log.info(f"[{county}] Updated {updated}/{len(leads)} leads")
    return updated

async def main():
    county_filter = os.getenv("COUNTIES", "").split(",") if os.getenv("COUNTIES") else list(COUNTY_TABLES.keys())
    county_filter = [c.strip().lower() for c in county_filter]
    
    conn = await asyncpg.connect(DB)
    total = 0
    for county, args in COUNTY_TABLES.items():
        if county not in county_filter: continue
        n = await enrich_county(conn, county, *args)
        total += n
    await conn.close()
    log.info(f"Total updated: {total}")

asyncio.run(main())
