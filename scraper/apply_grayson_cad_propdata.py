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

    # Only apply sqft/yr_built - never touch prop_address, since grayson_cad's
    # situs_address is missing street numbers (0.07% coverage) and would be
    # misleading if used to "fill in" an address.
    cur.execute("""
        WITH cad_normalized AS (
            SELECT owner_name, yr_built, living_area,
                CASE WHEN owner_name LIKE '%%,%%'
                     THEN UPPER(TRIM(split_part(owner_name, ',', 1)))
                     ELSE UPPER(TRIM(split_part(owner_name, ' ', 1)))
                END AS cad_last,
                CASE WHEN owner_name LIKE '%%,%%'
                     THEN UPPER(TRIM(split_part(split_part(owner_name, ',', 2), ' ', 2)))
                     ELSE UPPER(TRIM(split_part(owner_name, ' ', 2)))
                END AS cad_first
            FROM grayson_cad
            WHERE (living_area IS NOT NULL OR yr_built IS NOT NULL)
        ),
        matched AS (
            SELECT DISTINCT ON (l.id) l.id, c.yr_built, c.living_area
            FROM lead_records l
            JOIN cad_normalized c
                ON c.cad_last = UPPER(TRIM(split_part(l.owner, ' ', 1)))
                AND c.cad_first = UPPER(TRIM(split_part(l.owner, ' ', 2)))
            WHERE l.county='grayson'
            AND l.prop_address IS NOT NULL AND l.prop_address != ''
            AND l.sqft IS NULL
            AND l.owner IS NOT NULL AND length(l.owner) > 5
            AND l.owner NOT ILIKE '%%LLC%%' AND l.owner NOT ILIKE '%%TRUST%%'
            AND l.owner NOT ILIKE '%%CORP%%' AND l.owner NOT ILIKE '%%BANK%%'
            AND l.owner NOT ILIKE '%%FEDERAL%%' AND l.owner NOT ILIKE '%%MORTGAGE%%'
            AND l.owner NOT ILIKE '%%CREDIT UNION%%'
            AND l.owner NOT ILIKE '%%U S OF AMERICA%%' AND l.owner NOT ILIKE '%%UNITED STATES%%'
            AND l.owner NOT ILIKE '%%HOSPITAL%%' AND l.owner NOT ILIKE '%%MEDICAL CENTER%%'
            AND l.owner NOT ILIKE '%%SCHOOL DISTRICT%%' AND l.owner NOT ILIKE '%%CHURCH%%'
            AND l.owner NOT ILIKE '%%UNIVERSITY%%' AND l.owner NOT ILIKE '%%COLLEGE%%'
            AND l.owner NOT ILIKE '%%CITY OF%%' AND l.owner NOT ILIKE '%%COUNTY OF%%'
            AND l.owner NOT ILIKE '%%STATE OF%%' AND l.owner NOT ILIKE '%% ISD%%'
            AND l.owner NOT ILIKE '%% INC%%' AND l.owner NOT ILIKE '%% LP%%'
            AND l.owner NOT ILIKE '%%INTERNAL REVENUE%%' AND l.owner NOT ILIKE '%%JUDGMENT ENFORCEMENT%%'
            AND l.owner !~ '^[0-9]{4}-[0-9]+$'
            AND l.owner NOT ILIKE '%%CONSTRUCTION%%' AND l.owner NOT ILIKE '%%REPLAT%%'
            AND l.owner NOT ILIKE '%%ATTORNEY GENERAL%%'
            AND l.owner !~ '^[0-9]{6,}$'
            AND l.owner NOT ILIKE '%%ASSOCIATION%%' AND l.owner NOT ILIKE '%%DISTRICT%%'
            AND l.owner NOT ILIKE '%% ROA%%' AND l.owner NOT ILIKE '%% HOA%%'
            ORDER BY l.id
        )
        SELECT id, yr_built, living_area FROM matched
    """)
    matches = cur.fetchall()
    logger.info(f"Found {len(matches)} matches to apply (property data only, no address)")

    updated = 0
    failed = 0
    for lead_id, yr, area in matches:
        try:
            def clean_num(v, min_val=None):
                if not v:
                    return None
                try:
                    f = float(v)
                    if min_val is not None and f < min_val:
                        return None
                    return f
                except (ValueError, TypeError):
                    return None

            yr_f = clean_num(yr, 1800)
            area_f = clean_num(area, 1)
            yr_val = str(int(yr_f)) if yr_f else None
            sqft_val = int(area_f) if area_f else None

            if not yr_val and not sqft_val:
                continue

            cur.execute("""
                UPDATE lead_records SET
                    sqft = COALESCE(sqft, %s),
                    yr_built = COALESCE(yr_built, %s)
                WHERE id = %s
            """, (sqft_val, yr_val, lead_id))
            updated += 1
        except Exception as e:
            failed += 1
            conn.rollback()
            continue

    conn.commit()
    logger.info(f"DONE: {updated}/{len(matches)} updated, {failed} failed")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
