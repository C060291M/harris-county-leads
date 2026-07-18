import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.environ["DATABASE_URL"]

def main():
    conn = psycopg2.connect(DB, connect_timeout=30)
    cur = conn.cursor()

    cur.execute("""
        WITH cad_normalized AS (
            SELECT owner_name, situs_address, living_area, yr_built,
                UPPER(TRIM(split_part(owner_name, ',', 1))) AS cad_last,
                UPPER(TRIM(split_part(split_part(owner_name, ',', 2), ' ', 2))) AS cad_first
            FROM bell_cad
            WHERE situs_address IS NOT NULL AND situs_address != ''
        ),
        matched AS (
            SELECT DISTINCT ON (l.id) l.id, c.situs_address, c.living_area, c.yr_built
            FROM lead_records l
            JOIN cad_normalized c
                ON c.cad_last = UPPER(TRIM(split_part(l.owner, ' ', 1)))
                AND c.cad_first = UPPER(TRIM(split_part(l.owner, ' ', 2)))
            WHERE l.county='bell'
            AND l.owner NOT ILIKE '%%LLC%%' AND l.owner NOT ILIKE '%%BANK%%' AND l.owner NOT ILIKE '%%TRUST%%'
            AND l.owner NOT ILIKE '%%INC%%' AND l.owner NOT ILIKE '%%CORP%%'
            ORDER BY l.id, c.situs_address
        )
        SELECT id, situs_address, living_area, yr_built FROM matched
    """)
    matches = cur.fetchall()
    logger.info(f"Found {len(matches)} matches to apply")

    updated = 0
    failed = 0
    first_error_shown = False
    for lead_id, situs, sqft, yr in matches:
        try:
            sqft_val = int(float(sqft)) if sqft and sqft.replace(".","").isdigit() and float(sqft) > 0 else None
            yr_val = str(int(float(yr))) if yr and yr.replace(".","").isdigit() and float(yr) > 1800 else None
            cur.execute("""
                UPDATE lead_records SET
                    prop_address = COALESCE(NULLIF(prop_address,''), %s),
                    sqft = COALESCE(sqft, %s),
                    yr_built = COALESCE(yr_built, %s)
                WHERE id = %s
            """, (situs, sqft_val, yr_val, lead_id))
            updated += 1
            if updated % 50 == 0:
                conn.commit()
                logger.info(f"{updated}/{len(matches)} applied so far (committed)")
        except Exception as e:
            failed += 1
            if not first_error_shown:
                logger.error(f"FIRST REAL ERROR on lead {lead_id} (sqft={sqft!r}, yr={yr!r}): {e}")
                first_error_shown = True
            conn.rollback()
            continue

    conn.commit()
    logger.info(f"DONE: {updated}/{len(matches)} updated, {failed} failed")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
