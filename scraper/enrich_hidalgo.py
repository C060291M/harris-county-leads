import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
DB = os.environ["DATABASE_URL"]
import psycopg2, requests, os, time


BASE = "https://prod-container.trueprodigyapi.com"
LIMIT = 500

def get_token():
    h = {"User-Agent":"Mozilla/5.0","Content-Type":"application/json","Referer":"https://hidalgoad.org/","Cache-Control":"no-cache"}
    r = requests.post(f"{BASE}/trueprodigy/cadpublic/auth/token", json={"office":"Hidalgo"}, headers=h, timeout=15)
    return r.json()["user"]["token"]

def search_owner(token, last_name):
    h = {"User-Agent":"Mozilla/5.0","Content-Type":"application/json","Referer":"https://hidalgoad.org/","Cache-Control":"no-cache","authorization":token}
    payload = {"pYear":{"operator":"=","value":"2026"},"fullTextSearch":{"operator":"match","value":last_name}}
    r = requests.post(f"{BASE}/public/property/searchfulltext?page=1&pageSize=50", json=payload, headers=h, timeout=20)
    if r.status_code != 200: return []
    return r.json().get("results", [])

def normalize(s):
    return " ".join((s or "").upper().strip().split())

conn = psycopg2.connect(DB, connect_timeout=30)
cur = conn.cursor()

cur.execute("""
    SELECT id, owner FROM lead_records
    WHERE county='Hidalgo' AND (prop_address IS NULL OR prop_address='')
    AND owner IS NOT NULL AND length(owner) > 3
    AND owner NOT ILIKE '%%BANK%%' AND owner NOT ILIKE '%%MORTGAGE%%'
    AND owner NOT ILIKE '%%CREDIT%%' AND owner NOT ILIKE '%%FEDERAL%%'
    AND owner NOT ILIKE '%%LLC%%' AND owner NOT ILIKE '%%TRUST%%'
    ORDER BY score DESC LIMIT %s
""", (LIMIT,))
leads = cur.fetchall()
print(f"Hidalgo leads to enrich: {len(leads)}")

token = get_token()
token_calls = 0
updated = 0

for lead_id, owner in leads:
    if not owner: continue
    token_calls += 1
    if token_calls % 50 == 0:
        token = get_token()
        print(f"  Token refreshed at call {token_calls}")

    parts = owner.strip().upper().split()
    last = next((w for w in parts if len(w) >= 4 and w.isalpha()), parts[0] if parts else "")
    if not last or len(last) < 3: continue

    try:
        results = search_owner(token, last)
    except Exception as e:
        print(f"  API error for {last}: {str(e)[:50]}, skipping")
        time.sleep(2)
        continue

    # Reconnect DB if needed
    try:
        cur.execute("SELECT 1")
    except Exception:
        conn = psycopg2.connect(DB, connect_timeout=30)
        cur = conn.cursor()
    best = None
    for res in results:
        r_name = normalize(res.get("name",""))
        if last in r_name:
            best = res
            break

    if not best: continue

    prop_addr = (best.get("fullSitus") or "").strip()
    mail_addr = (best.get("addrDeliveryLine") or "").strip()
    mail_city = (best.get("addrCity") or "").strip()
    mail_state= (best.get("addrState") or "TX").strip()
    mail_zip  = (best.get("addrZip") or "").strip()
    if not prop_addr: continue

    cur.execute("UPDATE lead_records SET prop_address=%s, prop_state='TX', mail_address=%s, mail_city=%s, mail_state=%s, mail_zip=%s WHERE id=%s AND (prop_address IS NULL OR prop_address='')",
        (prop_addr, mail_addr, mail_city, mail_state, mail_zip, lead_id))
    updated += 1
    if updated % 25 == 0:
        conn.commit()
        print(f"  Progress: {updated} updated...")
    time.sleep(0.3)

conn.commit()
print(f"\nDone: {updated}/{len(leads)} Hidalgo leads enriched")
cur.execute("SELECT COUNT(*), SUM(CASE WHEN prop_address IS NOT NULL AND prop_address!='' THEN 1 ELSE 0 END) FROM lead_records WHERE county='Hidalgo'")
total, addr = cur.fetchone()
print(f"Hidalgo: {addr}/{total} ({round(addr/total*100)}%) addresses")
conn.close()