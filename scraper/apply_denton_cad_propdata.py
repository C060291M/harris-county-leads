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
            SELECT owner_name, sqft, yr_built,
                CASE WHEN owner_name LIKE '%%,%%'
                     THEN UPPER(TRIM(split_part(owner_name, ',', 1)))
                     ELSE UPPER(TRIM(split_part(owner_name, ' ', 1)))
                END AS cad_last,
                CASE WHEN owner_name LIKE '%%,%%'
                     THEN UPPER(TRIM(split_part(split_part(owner_name, ',', 2), ' ', 2)))
                     ELSE UPPER(TRIM(split_part(owner_name, ' ', 2)))
                END AS cad_first
            FROM denton_cad
            WHERE (sqft IS NOT NULL OR yr_built IS NOT NULL)
        ),
        matched AS (
            SELECT DISTINCT ON (l.id) l.id, c.sqft, c.yr_built
            FROM lead_records l
            JOIN cad_normalized c
                ON c.cad_last = UPPER(TRIM(split_part(l.owner, ' ', 1)))
                AND c.cad_first = UPPER(TRIM(split_part(l.owner, ' ', 2)))
            WHERE l.county='denton'
            AND l.prop_address IS NOT NULL AND l.prop_address != ''
            AND l.sqft IS NULL
            AND l.owner IS NOT NULL AND length(l.owner) > 5
            AND l.owner NOT ILIKE '%%LLC%%' AND l.owner NOT ILIKE '%%TRUST%%'
            AND l.owner NOT ILIKE '%%CORP%%' AND l.owner NOT ILIKE '%%BANK%%'
            AND l.owner NOT ILIKE '%%FEDERAL%%' AND l.owner NOT ILIKE '%%MORTGAGE%%'
            AND l.owner NOT ILIKE '%%CREDIT UNION%%'
            AND l.owner NOT ILIKE '%%HOSPITAL%%' AND l.owner NOT ILIKE '%%SCHOOL DISTRICT%%'
            AND l.owner NOT ILIKE '%%ASSOCIATION%%' AND l.owner NOT ILIKE '%%DISTRICT%%'
            ORDER BY l.id
        )
        SELECT id, sqft, yr_built FROM matched
    """)
    matches = cur.fetchall()
    logger.info(f"Found {len(matches)} matches to apply (property data only)")

    updated = 0
    for lead_id, sqft, yr in matches:
        try:
            def clean_num(v, min_val=None):
                if not v:
                    return None
                try:
                    v_clean = str(v).replace(",", "")
                    f = float(v_clean)
                    if min_val is not None and f < min_val:
                        return None
                    return f
                except (ValueError, TypeError):
                    return None
            sqft_f = clean_num(sqft, 1)
            yr_f = clean_num(yr, 1800)
            sqft_val = int(sqft_f) if sqft_f else None
            yr_val = str(int(yr_f)) if yr_f else None
            if not sqft_val and not yr_val:
                continue
            cur.execute("""
                UPDATE lead_records SET
                    sqft = COALESCE(sqft, %s),
                    yr_built = COALESCE(yr_built, %s)
                WHERE id = %s
            """, (sqft_val, yr_val, lead_id))
            updated += 1
        except Exception as e:
            conn.rollback()
            continue

    conn.commit()
    logger.info(f"DONE: {updated}/{len(matches)} updated")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
