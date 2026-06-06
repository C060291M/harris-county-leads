import psycopg2, os

DB = os.environ.get("DATABASE_URL", "postgresql://postgres:REDACTED_OLD2@kodama.proxy.rlwy.net:42079/railway")
LIMIT = 500

def normalize(s):
    if not s: return ""
    return " ".join(s.upper().strip().split())

conn = psycopg2.connect(DB, connect_timeout=30)
cur = conn.cursor()

# Load CAD into memory: owner_name -> (situs_address, yr_built, living_area)
print("Loading Midland CAD...")
cur.execute("SELECT owner_name, situs_address, situs_city, situs_zip, yr_built, living_area FROM midland_cad WHERE situs_address IS NOT NULL AND situs_address != ''")
cad = {}
for owner, addr, city, zip_, yr, sqft in cur.fetchall():
    key = normalize(owner)
    if key and key not in cad:
        full_addr = addr.strip()
        if city: full_addr += f", {city.strip()}"
        if zip_: full_addr += f" {zip_.strip()}"
        cad[key] = (full_addr, yr, sqft)
print(f"CAD loaded: {len(cad):,} unique owners")

# Get unaddressed Midland leads
cur.execute("""
    SELECT id, owner FROM lead_records
    WHERE county='Midland' AND (prop_address IS NULL OR prop_address='')
    AND owner IS NOT NULL
    AND owner NOT ILIKE '%%BANK%%' AND owner NOT ILIKE '%%CREDIT%%'
    AND owner NOT ILIKE '%%MORTGAGE%%' AND owner NOT ILIKE '%%FINANCIAL%%'
    AND owner NOT ILIKE '%%FEDERAL%%' AND owner NOT ILIKE '%%LENDING%%'
    ORDER BY score DESC LIMIT %s
""", (LIMIT,))
leads = cur.fetchall()
print(f"Unaddressed Midland leads to process: {len(leads)}")

matched = 0
for lead_id, owner in leads:
    key = normalize(owner)
    if not key: continue
    
    # Exact match first
    hit = cad.get(key)
    
    # If no exact match, try first word (last name) match
    if not hit:
        first_word = key.split()[0] if key.split() else ""
        if len(first_word) >= 4:
            for cad_key, val in cad.items():
                if cad_key.startswith(first_word):
                    hit = val
                    break
    
    if hit:
        addr, yr, sqft = hit
        cur.execute("""
            UPDATE lead_records SET prop_address=%s, yr_built=%s, sqft=%s
            WHERE id=%s AND (prop_address IS NULL OR prop_address='')
        """, (addr, yr, sqft, lead_id))
        matched += 1

conn.commit()
print(f"Midland enriched: {matched}/{len(leads)} leads updated")
conn.close()

